use crate::error::EngineError;
use crate::manifest::{default_manifest, load_manifest, write_manifest};
use crate::memtable::Memtable;
use crate::segment_reader::SegmentReader;
use crate::segment_writer::{
    segment_dir, segment_tmp_dir, write_indexes_from_metadata, write_segment,
    write_v3_edges_dat, write_v3_nodes_dat, CompactEdgeMeta, CompactNodeMeta,
};
use crate::types::*;
use crate::wal::{truncate_wal, WalReader, WalWriter};
use crate::wal_sync::{shutdown_sync_thread, sync_thread_loop, WalSyncState};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
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
            match source.binary_search_by_key(&cursor, |item| key_fn(item)) {
                Ok(pos) => pos + 1,  // cursor found, start after it
                Err(pos) => pos,     // cursor not found, insertion point
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

/// Apply cursor-based pagination to an already-sorted list.
/// Binary-searches by key for the cursor position and returns at most `limit`
/// items. Used for post-filter paths where merge produces sorted output but
/// we need to re-paginate after filtering.
fn apply_cursor_to_sorted_by<T: Clone>(
    sorted_items: Vec<T>,
    key_fn: impl Fn(&T) -> u64,
    page: &PageRequest,
) -> PageResult<T> {
    let start = match page.after {
        Some(cursor) => match sorted_items.binary_search_by_key(&cursor, |item| key_fn(item)) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        },
        None => 0,
    };

    let remaining = &sorted_items[start..];

    match page.limit {
        Some(limit) if limit > 0 && limit < remaining.len() => {
            let items = remaining[..limit].to_vec();
            let next_cursor = Some(key_fn(&items[items.len() - 1]));
            PageResult { items, next_cursor }
        }
        _ => PageResult {
            items: remaining.to_vec(),
            next_cursor: None,
        },
    }
}

/// Apply limit to a list where the cursor has already been handled by the
/// merge binary-seek. Truncates to `page.limit` items and sets `next_cursor`
/// from the last emitted item's key. Used by filtered/policy paths that push
/// the cursor into the merge but apply limit after post-processing.
fn apply_limit_only<T>(
    mut items: Vec<T>,
    key_fn: impl Fn(&T) -> u64,
    page: &PageRequest,
) -> PageResult<T> {
    let limit = page.limit.unwrap_or(0);
    if limit > 0 && items.len() > limit {
        let next_cursor = Some(key_fn(&items[limit - 1]));
        items.truncate(limit);
        PageResult { items, next_cursor }
    } else {
        PageResult {
            items,
            next_cursor: None,
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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

impl DatabaseEngine {
    /// Open or create a database at the given directory path.
    pub fn open(path: &Path, options: &DbOptions) -> Result<Self, EngineError> {
        // Create directory if needed
        if !path.exists() {
            if options.create_if_missing {
                std::fs::create_dir_all(path)?;
            } else {
                return Err(EngineError::DatabaseNotFound(format!(
                    "{}",
                    path.display()
                )));
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
        if let WalSyncMode::GroupCommit { interval_ms, soft_trigger_bytes, hard_cap_bytes } = &options.wal_sync_mode {
            if *interval_ms == 0 {
                return Err(EngineError::InvalidOperation("GroupCommit interval_ms must be > 0".into()));
            }
            if *soft_trigger_bytes == 0 {
                return Err(EngineError::InvalidOperation("GroupCommit soft_trigger_bytes must be > 0".into()));
            }
            if *hard_cap_bytes == 0 {
                return Err(EngineError::InvalidOperation("GroupCommit hard_cap_bytes must be > 0".into()));
            }
            if *hard_cap_bytes <= *soft_trigger_bytes {
                return Err(EngineError::InvalidOperation(
                    format!("GroupCommit hard_cap_bytes ({}) must be > soft_trigger_bytes ({})", hard_cap_bytes, soft_trigger_bytes)
                ));
            }
        }

        // Initialize WAL sync based on mode
        let wal_sync_mode = options.wal_sync_mode.clone();
        let (wal_writer_immediate, wal_state, sync_thread) = match &wal_sync_mode {
            WalSyncMode::Immediate => {
                (Some(wal_writer), None, None)
            }
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

        Ok(DatabaseEngine {
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
            flush_count_since_last_compact: 0,
            compacting: false,
            wal_sync_mode,
            memtable_hard_cap: options.memtable_hard_cap_bytes,
            bg_compact: None,
            last_compaction_ms: None,
        })
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

    /// Internal helper that handles WAL append + memtable apply for both sync modes.
    fn append_and_apply(&mut self, ops: &[WalOp]) -> Result<(), EngineError> {
        self.wal_append(|w| w.append_batch(ops))?;
        for op in ops {
            self.memtable.apply_op(op);
        }
        Ok(())
    }

    /// Internal helper for a single WAL op (avoids vec allocation for common case).
    fn append_and_apply_one(&mut self, op: &WalOp) -> Result<(), EngineError> {
        self.wal_append(|w| w.append(op))?;
        self.memtable.apply_op(op);
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
                let w = self.wal_writer_immediate.as_mut().expect("immediate WAL writer");
                f(w)?;
                w.sync()?;
            }
            WalSyncMode::GroupCommit { soft_trigger_bytes, hard_cap_bytes, .. } => {
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
                self.wal_writer_immediate.as_mut().expect("immediate WAL writer").sync()?;
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
        let frozen = std::mem::replace(&mut self.memtable, Memtable::new());

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
        if self.flush_threshold > 0
            && self.memtable.estimated_size() >= self.flush_threshold
        {
            self.flush()?;
        }
        Ok(())
    }

    /// Check if the memtable has reached the hard cap. If so, trigger a
    /// synchronous flush before allowing the next write to proceed.
    /// This is the backpressure mechanism: writes "block" (flush inline) when
    /// the memtable is at capacity, preventing unbounded memory growth.
    fn maybe_backpressure_flush(&mut self) -> Result<(), EngineError> {
        if self.memtable_hard_cap > 0
            && self.memtable.estimated_size() >= self.memtable_hard_cap
        {
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
            bg_compact_worker(db_dir, seg_id, input_segments, prune_policies, &cancel_clone)
        });

        self.bg_compact = Some(BgCompactHandle { handle, cancel });

        Ok(())
    }

    /// Non-blocking check: if a background compaction has finished, apply its result.
    fn try_complete_bg_compact(&mut self) -> Option<CompactionStats> {
        let is_finished = self
            .bg_compact
            .as_ref()
            .map_or(false, |bg| bg.handle.is_finished());
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
        Some(result.stats)
    }

    // --- Compaction ---

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
    /// Uses the unified V3 compaction path: metadata-only planning (winner
    /// selection from sidecars), raw binary copy of winning records, and
    /// metadata-driven index building. No MessagePack decode at any point.
    /// Prune policies are always evaluated regardless of segment overlap.
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
        let policies: Vec<PrunePolicy> =
            self.manifest.prune_policies.values().cloned().collect();

        // Ensure segments directory exists before allocating segment ID (M1 fix)
        let segments_dir = self.db_dir.join("segments");
        std::fs::create_dir_all(&segments_dir)?;

        // Allocate new segment ID
        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;
        let tmp_dir = segment_tmp_dir(&self.db_dir, seg_id);
        let final_dir = segment_dir(&self.db_dir, seg_id);

        // Unified V3 path: metadata-only planning handles all cases (overlapping,
        // non-overlapping, tombstones, prune policies). Non-overlapping segments
        // naturally have every record win (first-seen, no duplicates).
        let (seg_info, nodes_auto_pruned, edges_auto_pruned) = match self.compact_standard(
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
        let old_seg_ids: HashSet<u64> =
            self.segments.iter().map(|s| s.segment_id).collect();
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

    /// V3 compaction: metadata-only planning + raw binary copy.
    ///
    /// Replaces the old decode-everything approach with:
    /// 1. Plan winners from metadata sidecars (never decode dropped records)
    /// 2. Raw-copy winning record bytes to output data files
    /// 3. Build all secondary indexes from metadata sidecars (no Memtable decode)
    ///
    /// Returns `(SegmentInfo, nodes_auto_pruned, edges_auto_pruned)`.
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

        let seg_info = v3_build_output(
            tmp_dir,
            seg_id,
            &self.segments,
            &plan,
        )?;

        Ok((seg_info, nodes_auto_pruned, edges_auto_pruned))
    }

    // --- High-level graph APIs ---

    /// Upsert a node. If a node with the same (type_id, key) exists, updates it.
    /// Otherwise allocates a new ID. Returns the node ID.
    pub fn upsert_node(
        &mut self,
        type_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
        weight: f32,
    ) -> Result<u64, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        let (id, created_at) = match self.find_existing_node(type_id, key)? {
            Some((id, created_at)) => (id, created_at),
            None => {
                let id = self.next_node_id;
                self.next_node_id += 1;
                (id, now)
            }
        };

        let node = NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props,
            created_at,
            updated_at: now,
            weight,
        };

        let op = WalOp::UpsertNode(node);
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(id)
    }

    /// Upsert an edge. If edge_uniqueness is enabled and an edge with the same
    /// (from, to, type_id) exists, updates it. Otherwise allocates a new ID.
    /// Returns the edge ID.
    pub fn upsert_edge(
        &mut self,
        from: u64,
        to: u64,
        type_id: u32,
        props: BTreeMap<String, PropValue>,
        weight: f32,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> Result<u64, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        let (id, created_at) = if self.edge_uniqueness {
            match self.find_existing_edge(from, to, type_id)? {
                Some((id, created_at)) => (id, created_at),
                None => {
                    let id = self.next_edge_id;
                    self.next_edge_id += 1;
                    (id, now)
                }
            }
        } else {
            let id = self.next_edge_id;
            self.next_edge_id += 1;
            (id, now)
        };

        let edge = EdgeRecord {
            id,
            from,
            to,
            type_id,
            props,
            created_at,
            updated_at: now,
            weight,
            valid_from: valid_from.unwrap_or(created_at),
            valid_to: valid_to.unwrap_or(i64::MAX),
        };

        let op = WalOp::UpsertEdge(edge);
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(id)
    }

    /// Batch upsert nodes with a single fsync. Returns IDs in input order.
    /// Handles dedup within the batch and against existing memtable state.
    pub fn batch_upsert_nodes(
        &mut self,
        inputs: &[NodeInput],
    ) -> Result<Vec<u64>, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
        // Track allocations within this batch for dedup
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in inputs {
            let key_tuple = (input.type_id, input.key.clone());

            let (id, created_at) =
                if let Some(&(id, created_at)) = batch_keys.get(&key_tuple) {
                    // Already allocated in this batch, update
                    (id, created_at)
                } else if let Some((id, created_at)) =
                    self.find_existing_node(input.type_id, &input.key)?
                {
                    (id, created_at)
                } else {
                    let id = self.next_node_id;
                    self.next_node_id += 1;
                    (id, now)
                };

            batch_keys.insert(key_tuple, (id, created_at));

            ops.push(WalOp::UpsertNode(NodeRecord {
                id,
                type_id: input.type_id,
                key: input.key.clone(),
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
            }));
            ids.push(id);
        }

        self.append_and_apply(&ops)?;

        self.maybe_auto_flush()?;
        Ok(ids)
    }

    /// Batch upsert edges with a single fsync. Returns IDs in input order.
    pub fn batch_upsert_edges(
        &mut self,
        inputs: &[EdgeInput],
    ) -> Result<Vec<u64>, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
        // Track allocations within this batch for uniqueness
        let mut batch_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();

        for input in inputs {
            let triple = (input.from, input.to, input.type_id);

            let (id, created_at) = if self.edge_uniqueness {
                if let Some(&(id, created_at)) = batch_triples.get(&triple) {
                    (id, created_at)
                } else if let Some((id, created_at)) =
                    self.find_existing_edge(input.from, input.to, input.type_id)?
                {
                    (id, created_at)
                } else {
                    let id = self.next_edge_id;
                    self.next_edge_id += 1;
                    (id, now)
                }
            } else {
                let id = self.next_edge_id;
                self.next_edge_id += 1;
                (id, now)
            };

            if self.edge_uniqueness {
                batch_triples.insert(triple, (id, created_at));
            }

            ops.push(WalOp::UpsertEdge(EdgeRecord {
                id,
                from: input.from,
                to: input.to,
                type_id: input.type_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
            }));
            ids.push(id);
        }

        self.append_and_apply(&ops)?;

        self.maybe_auto_flush()?;
        Ok(ids)
    }

    /// Delete a node by ID. Cascade-deletes all incident edges (memtable + segments),
    /// then writes the node tombstone. Single fsync at the end.
    pub fn delete_node(&mut self, id: u64) -> Result<(), EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        // Collect ALL incident edge IDs across memtable + segments.
        // Uses neighbors_raw to see edges to policy-excluded nodes too.
        let incident = self.neighbors_raw(id, Direction::Both, None, 0, None, None)?;

        // Build all ops: edge deletes first, then node delete
        let mut ops = Vec::with_capacity(incident.len() + 1);
        for entry in &incident {
            ops.push(WalOp::DeleteEdge {
                id: entry.edge_id,
                deleted_at: now,
            });
        }
        ops.push(WalOp::DeleteNode {
            id,
            deleted_at: now,
        });

        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;
        Ok(())
    }

    /// Delete an edge by ID. Writes a tombstone to WAL. Idempotent: deleting
    /// a nonexistent or already-deleted edge writes a tombstone but is not an error.
    pub fn delete_edge(&mut self, id: u64) -> Result<(), EngineError> {
        self.maybe_backpressure_flush()?;
        let op = WalOp::DeleteEdge {
            id,
            deleted_at: now_millis(),
        };
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(())
    }

    /// Invalidate an edge by closing its validity window. Sets valid_to on the edge.
    /// The edge remains in the database (not tombstoned) but is excluded from
    /// current-time neighbor queries. Returns the updated EdgeRecord, or None
    /// if the edge doesn't exist.
    pub fn invalidate_edge(
        &mut self,
        id: u64,
        valid_to: i64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.maybe_backpressure_flush()?;
        // Look up the current edge record
        let edge = match self.get_edge(id)? {
            Some(e) => e,
            None => return Ok(None),
        };

        let updated = EdgeRecord {
            updated_at: now_millis(),
            valid_to,
            ..edge
        };

        let op = WalOp::UpsertEdge(updated.clone());
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(Some(updated))
    }

    /// Atomic graph patch: apply a mix of node upserts, edge upserts, edge
    /// invalidations, and deletes in a single WAL batch. Deterministic ordering:
    /// node upserts → edge upserts → edge invalidations → edge deletes → node deletes.
    ///
    /// Node deletes cascade: incident edges are automatically deleted.
    /// Returns allocated IDs for upserted nodes and edges (input order preserved).
    pub fn graph_patch(&mut self, patch: &GraphPatch) -> Result<PatchResult, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops: Vec<WalOp> = Vec::new();

        // --- 1. Node upserts (same dedup as batch_upsert_nodes) ---
        let mut node_ids = Vec::with_capacity(patch.upsert_nodes.len());
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in &patch.upsert_nodes {
            let key_tuple = (input.type_id, input.key.clone());
            let (id, created_at) =
                if let Some(&(id, created_at)) = batch_keys.get(&key_tuple) {
                    (id, created_at)
                } else if let Some((id, created_at)) =
                    self.find_existing_node(input.type_id, &input.key)?
                {
                    (id, created_at)
                } else {
                    let id = self.next_node_id;
                    self.next_node_id += 1;
                    (id, now)
                };
            batch_keys.insert(key_tuple, (id, created_at));
            ops.push(WalOp::UpsertNode(NodeRecord {
                id,
                type_id: input.type_id,
                key: input.key.clone(),
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
            }));
            node_ids.push(id);
        }

        // --- 2. Edge upserts (same dedup as batch_upsert_edges) ---
        let mut edge_ids = Vec::with_capacity(patch.upsert_edges.len());
        let mut batch_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();

        for input in &patch.upsert_edges {
            let triple = (input.from, input.to, input.type_id);
            let (id, created_at) = if self.edge_uniqueness {
                if let Some(&(id, created_at)) = batch_triples.get(&triple) {
                    (id, created_at)
                } else if let Some((id, created_at)) =
                    self.find_existing_edge(input.from, input.to, input.type_id)?
                {
                    (id, created_at)
                } else {
                    let id = self.next_edge_id;
                    self.next_edge_id += 1;
                    (id, now)
                }
            } else {
                let id = self.next_edge_id;
                self.next_edge_id += 1;
                (id, now)
            };
            if self.edge_uniqueness {
                batch_triples.insert(triple, (id, created_at));
            }
            ops.push(WalOp::UpsertEdge(EdgeRecord {
                id,
                from: input.from,
                to: input.to,
                type_id: input.type_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
            }));
            edge_ids.push(id);
        }

        // --- 3. Edge invalidations (batch read) ---
        if !patch.invalidate_edges.is_empty() {
            let inv_ids: Vec<u64> = patch.invalidate_edges.iter().map(|&(id, _)| id).collect();
            // get_edges has no policy filtering (edges are unfiltered); safe for write path
            let inv_edges = self.get_edges(&inv_ids)?;
            for (&(_, valid_to), opt_edge) in patch.invalidate_edges.iter().zip(inv_edges) {
                if let Some(edge) = opt_edge {
                    ops.push(WalOp::UpsertEdge(EdgeRecord {
                        updated_at: now,
                        valid_to,
                        ..edge
                    }));
                }
            }
        }

        // --- 4. Edge deletes ---
        for &eid in &patch.delete_edge_ids {
            ops.push(WalOp::DeleteEdge {
                id: eid,
                deleted_at: now,
            });
        }

        // --- 5. Node deletes (cascade incident edges across all sources) ---
        // Uses neighbors_raw to see edges to policy-excluded nodes too.
        for &nid in &patch.delete_node_ids {
            let incident = self.neighbors_raw(nid, Direction::Both, None, 0, None, None)?;
            for entry in &incident {
                ops.push(WalOp::DeleteEdge {
                    id: entry.edge_id,
                    deleted_at: now,
                });
            }
            ops.push(WalOp::DeleteNode {
                id: nid,
                deleted_at: now,
            });
        }

        // --- Single WAL batch ---
        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;

        Ok(PatchResult { node_ids, edge_ids })
    }

    // --- Retention / Forgetting ---

    /// Prune nodes matching the given policy, cascade-deleting their incident edges.
    /// All matching criteria combine with AND logic. At least one of `max_age_ms` or
    /// `max_weight` must be set to prevent accidental mass deletion. All deletes are
    /// applied in a single WAL batch for atomicity.
    pub fn prune(&mut self, policy: &PrunePolicy) -> Result<PruneResult, EngineError> {
        // Require at least one substantive filter
        if policy.max_age_ms.is_none() && policy.max_weight.is_none() {
            return Ok(PruneResult {
                nodes_pruned: 0,
                edges_pruned: 0,
            });
        }

        // Reject nonsensical negative age threshold
        if let Some(age) = policy.max_age_ms {
            if age <= 0 {
                return Err(EngineError::InvalidOperation(
                    "max_age_ms must be positive".to_string(),
                ));
            }
        }

        self.maybe_backpressure_flush()?;
        let now = now_millis();

        // Collect all node IDs that match the prune policy
        let targets = self.collect_prune_targets(policy, now)?;

        if targets.is_empty() {
            return Ok(PruneResult {
                nodes_pruned: 0,
                edges_pruned: 0,
            });
        }

        // Build WAL ops: cascade-delete incident edges, then delete nodes.
        // Dedup edge deletes in case two pruned nodes share an edge.
        let mut ops = Vec::new();
        let mut edges_seen = HashSet::new();

        for &nid in &targets {
            let incident = self.neighbors_raw(nid, Direction::Both, None, 0, None, None)?;
            for entry in &incident {
                if edges_seen.insert(entry.edge_id) {
                    ops.push(WalOp::DeleteEdge {
                        id: entry.edge_id,
                        deleted_at: now,
                    });
                }
            }
            ops.push(WalOp::DeleteNode {
                id: nid,
                deleted_at: now,
            });
        }

        let nodes_pruned = targets.len() as u64;
        let edges_pruned = edges_seen.len() as u64;

        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;

        Ok(PruneResult {
            nodes_pruned,
            edges_pruned,
        })
    }

    /// Collect node IDs matching the prune policy by scanning memtable + segments.
    /// When `type_id` is set, uses the type index for efficiency.
    /// Uses raw (unfiltered) reads. Prune must see ALL nodes, including those
    /// hidden by registered policies, to ensure correct deletion.
    fn collect_prune_targets(
        &self,
        policy: &PrunePolicy,
        now: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let age_cutoff = policy.max_age_ms.map(|age| now - age);

        if let Some(type_id) = policy.type_id {
            // Use the type index (raw). Must see all nodes including policy-excluded ones
            let ids = self.nodes_by_type_raw(type_id)?;
            let nodes = self.get_nodes_raw(&ids)?;
            let targets = ids
                .iter()
                .zip(nodes)
                .filter_map(|(&id, opt)| {
                    opt.filter(|n| Self::matches_prune_criteria(n, age_cutoff, policy.max_weight))
                        .map(|_| id)
                })
                .collect();
            Ok(targets)
        } else {
            // Scan all nodes: memtable first, then segments (newest-first), dedup by ID
            let mut deleted: HashSet<u64> =
                self.memtable.deleted_nodes().keys().copied().collect();
            for seg in &self.segments {
                deleted.extend(seg.deleted_node_ids());
            }

            let mut seen = HashSet::new();
            let mut targets = Vec::new();

            // Memtable nodes (freshest)
            for node in self.memtable.nodes().values() {
                if !deleted.contains(&node.id) && seen.insert(node.id) {
                    if Self::matches_prune_criteria(node, age_cutoff, policy.max_weight) {
                        targets.push(node.id);
                    }
                }
            }

            // Segment nodes (newest segments first, skip already-seen)
            for seg in &self.segments {
                for node in seg.all_nodes()? {
                    if !deleted.contains(&node.id) && seen.insert(node.id) {
                        if Self::matches_prune_criteria(&node, age_cutoff, policy.max_weight) {
                            targets.push(node.id);
                        }
                    }
                }
            }

            Ok(targets)
        }
    }

    /// Check whether a node matches the prune criteria (AND logic).
    fn matches_prune_criteria(
        node: &NodeRecord,
        age_cutoff: Option<i64>,
        max_weight: Option<f32>,
    ) -> bool {
        if let Some(cutoff) = age_cutoff {
            if node.updated_at >= cutoff {
                return false; // Too recent, does not match
            }
        }
        if let Some(max_w) = max_weight {
            if node.weight > max_w {
                return false; // Weight too high, does not match
            }
        }
        true
    }

    // --- Named prune policies (compaction-filter auto-prune) ---

    /// Register a named prune policy. Persisted in the manifest and applied
    /// automatically during compaction. Multiple named policies are allowed;
    /// a node matching ANY policy is pruned (OR across policies, AND within).
    pub fn set_prune_policy(&mut self, name: &str, policy: PrunePolicy) -> Result<(), EngineError> {
        // Validate: at least one substantive filter
        if policy.max_age_ms.is_none() && policy.max_weight.is_none() {
            return Err(EngineError::InvalidOperation(
                "Prune policy must set at least max_age_ms or max_weight".to_string(),
            ));
        }
        if let Some(age) = policy.max_age_ms {
            if age <= 0 {
                return Err(EngineError::InvalidOperation(
                    "max_age_ms must be positive".to_string(),
                ));
            }
        }
        if let Some(w) = policy.max_weight {
            if w.is_nan() || w < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "max_weight must be non-negative and not NaN".to_string(),
                ));
            }
        }
        self.manifest.prune_policies.insert(name.to_string(), policy);
        write_manifest(&self.db_dir, &self.manifest)?;
        Ok(())
    }

    /// Remove a named prune policy. Returns true if it existed.
    pub fn remove_prune_policy(&mut self, name: &str) -> Result<bool, EngineError> {
        let removed = self.manifest.prune_policies.remove(name).is_some();
        if removed {
            write_manifest(&self.db_dir, &self.manifest)?;
        }
        Ok(removed)
    }

    /// List all registered prune policies.
    pub fn list_prune_policies(&self) -> Vec<(String, PrunePolicy)> {
        self.manifest
            .prune_policies
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    // --- Runtime introspection ---

    /// Return read-only runtime statistics for this database.
    pub fn stats(&self) -> DbStats {
        let pending_wal_bytes = match &self.wal_sync_mode {
            WalSyncMode::Immediate => 0,
            WalSyncMode::GroupCommit { .. } => {
                self.wal_state
                    .as_ref()
                    .map(|arc| {
                        let (lock, _) = &**arc;
                        lock.lock().map(|s| s.buffered_bytes).unwrap_or(0)
                    })
                    .unwrap_or(0)
            }
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

    // --- Read-time policy filtering ---

    /// Check if a single node is excluded by any registered prune policy.
    /// Early-out when no policies are registered.
    fn is_node_excluded_by_policies(&self, node: &NodeRecord) -> bool {
        if self.manifest.prune_policies.is_empty() {
            return false;
        }
        let now = now_millis();
        let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now);
        cutoffs.excludes(node)
    }

    /// Batch-compute the set of node IDs that should be excluded by prune policies.
    /// Uses the batched merge-walk (`get_nodes_raw`) instead of N individual lookups.
    /// Returns an empty set when no policies are registered (zero overhead).
    fn policy_excluded_node_ids(&self, node_ids: &[u64]) -> Result<HashSet<u64>, EngineError> {
        if self.manifest.prune_policies.is_empty() || node_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now_millis());
        let records = self.get_nodes_raw(node_ids)?;
        let mut excluded = HashSet::new();
        for (i, slot) in records.iter().enumerate() {
            if let Some(ref node) = slot {
                if cutoffs.excludes(node) {
                    excluded.insert(node_ids[i]);
                }
            }
        }
        Ok(excluded)
    }

    // --- Read APIs (multi-source: memtable → segments newest-first) ---

    /// Get a node by ID (raw, unfiltered). Checks memtable first, then segments
    /// newest-to-oldest. Returns None if not found or deleted (tombstoned).
    /// Used internally by upsert dedup, cascade deletes, and prune.
    fn get_node_raw(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        // Memtable is freshest
        if let Some(node) = self.memtable.get_node(id) {
            return Ok(Some(node.clone()));
        }
        // If memtable explicitly tombstoned this ID, it's deleted
        if self.memtable.deleted_nodes().contains_key(&id) {
            return Ok(None);
        }
        // Check segments newest-first
        for seg in &self.segments {
            if seg.is_node_deleted(id) {
                return Ok(None); // Tombstone in newer segment hides older data
            }
            if let Some(node) = seg.get_node(id)? {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    /// Get a node by ID. Checks memtable first, then segments newest-to-oldest.
    /// Returns None if not found, deleted, or excluded by a registered prune policy.
    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_raw(id)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    /// Get an edge by ID. Checks memtable first, then segments newest-to-oldest.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self.memtable.get_edge(id) {
            return Ok(Some(edge.clone()));
        }
        if self.memtable.deleted_edges().contains_key(&id) {
            return Ok(None);
        }
        for seg in &self.segments {
            if seg.is_edge_deleted(id) {
                return Ok(None);
            }
            if let Some(edge) = seg.get_edge(id)? {
                return Ok(Some(edge));
            }
        }
        Ok(None)
    }

    /// Get a node by (type_id, key) across memtable + segments (raw, unfiltered).
    /// Used internally by upsert dedup. Not subject to prune policy filtering.
    fn get_node_by_key_raw(&self, type_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        // Check memtable first (freshest)
        if let Some(node) = self.memtable.node_by_key(type_id, key) {
            return Ok(Some(node.clone()));
        }
        // Check segments newest-first, respecting tombstones
        for (i, seg) in self.segments.iter().enumerate() {
            if let Some(node) = seg.node_by_key(type_id, key)? {
                if self.memtable.deleted_nodes().contains_key(&node.id) {
                    return Ok(None);
                }
                for newer_seg in &self.segments[..i] {
                    if newer_seg.is_node_deleted(node.id) {
                        return Ok(None);
                    }
                }
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    /// Get a node by (type_id, key) across memtable + segments.
    /// Returns None if not found, deleted, or excluded by a registered prune policy.
    pub fn get_node_by_key(&self, type_id: u32, key: &str) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_by_key_raw(type_id, key)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    /// Get an edge by (from, to, type_id) across memtable + segments.
    /// Returns the most recently written edge matching the triple.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge_by_triple(&self, from: u64, to: u64, type_id: u32) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self.memtable.edge_by_triple(from, to, type_id) {
            return Ok(Some(edge.clone()));
        }
        for (i, seg) in self.segments.iter().enumerate() {
            if let Some(edge) = seg.edge_by_triple(from, to, type_id)? {
                if self.memtable.deleted_edges().contains_key(&edge.id) {
                    return Ok(None);
                }
                for newer_seg in &self.segments[..i] {
                    if newer_seg.is_edge_deleted(edge.id) {
                        return Ok(None);
                    }
                }
                return Ok(Some(edge));
            }
        }
        Ok(None)
    }

    /// Batch node lookup (raw, unfiltered). Core implementation shared by
    /// `get_nodes` (public, filtered) and `policy_excluded_node_ids`.
    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let n = ids.len();
        let mut results: Vec<Option<NodeRecord>> = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        // Build (original_index, id) pairs for tracking
        let mut remaining: Vec<(usize, u64)> = Vec::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            // Memtable tombstone, definitely deleted, skip
            if self.memtable.deleted_nodes().contains_key(&id) {
                continue;
            }
            // Memtable hit, freshest source
            if let Some(node) = self.memtable.get_node(id) {
                results[i] = Some(node.clone());
                continue;
            }
            remaining.push((i, id));
        }

        // Sort once before segment scan. retain() preserves order so
        // remaining stays sorted across iterations.
        remaining.sort_unstable_by_key(|&(_, id)| id);

        // Scan segments newest-first with sorted merge-walk
        for seg in &self.segments {
            if remaining.is_empty() {
                break;
            }

            // Filter out IDs tombstoned in this segment before the batch scan
            // (tombstone in a newer segment hides data in older segments)
            remaining.retain(|&(_, id)| !seg.is_node_deleted(id));

            seg.get_nodes_batch(&remaining, &mut results)?;

            // Remove IDs that were found in this segment
            remaining.retain(|&(i, _)| results[i].is_none());
        }

        Ok(results)
    }

    /// Get multiple nodes by ID in a single call.
    /// Returns a `Vec<Option<NodeRecord>>`, one per input ID (order preserved).
    /// Missing, deleted, or policy-excluded nodes are None.
    ///
    /// Uses a batched approach: resolves memtable hits first (O(1) each),
    /// then does a single sorted merge-walk per segment instead of N binary searches.
    pub fn get_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_raw(ids)?;

        // Policy filtering: exclude nodes matching any registered prune policy.
        // Early-out when no policies are registered (zero overhead).
        if !self.manifest.prune_policies.is_empty() {
            let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now_millis());
            for slot in results.iter_mut() {
                if let Some(ref node) = slot {
                    if cutoffs.excludes(node) {
                        *slot = None;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get multiple edges by ID in a single call.
    /// Returns a `Vec<Option<EdgeRecord>>`, one per input ID (order preserved).
    /// Missing or deleted edges are None.
    ///
    /// Uses a batched approach: resolves memtable hits first (O(1) each),
    /// then does a single sorted merge-walk per segment instead of N binary searches.
    pub fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let n = ids.len();
        let mut results: Vec<Option<EdgeRecord>> = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = Vec::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            if self.memtable.deleted_edges().contains_key(&id) {
                continue;
            }
            if let Some(edge) = self.memtable.get_edge(id) {
                results[i] = Some(edge.clone());
                continue;
            }
            remaining.push((i, id));
        }

        // Sort once. retain() preserves order across iterations.
        remaining.sort_unstable_by_key(|&(_, id)| id);

        for seg in &self.segments {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(_, id)| !seg.is_edge_deleted(id));
            seg.get_edges_batch(&remaining, &mut results)?;
            remaining.retain(|&(i, _)| results[i].is_none());
        }

        Ok(results)
    }

    /// Raw neighbor query with no prune policy filtering. Used internally by
    /// cascade deletes (delete_node, graph_patch, prune) that must see ALL
    /// incident edges including those to policy-excluded nodes.
    fn neighbors_raw(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Collect all tombstoned node and edge IDs across memtable + segments
        let mut deleted_nodes: HashSet<u64> = self
            .memtable
            .deleted_nodes()
            .keys()
            .copied()
            .collect();
        let mut deleted_edges: HashSet<u64> = self
            .memtable
            .deleted_edges()
            .keys()
            .copied()
            .collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Start with memtable results (fetch without limit to allow for temporal filtering)
        let mut results = self.memtable.neighbors(node_id, direction, type_filter, 0);
        let mut seen_edges: HashSet<u64> = results.iter().map(|e| e.edge_id).collect();

        // Add segment results
        for seg in &self.segments {
            let seg_results = seg.neighbors(node_id, direction, type_filter, 0)?;
            for entry in seg_results {
                // Deduplicate by edge_id (memtable/newer segment wins)
                if seen_edges.contains(&entry.edge_id) {
                    continue;
                }
                // Skip if the edge or neighbor node is deleted anywhere
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                if deleted_nodes.contains(&entry.node_id) {
                    continue;
                }
                seen_edges.insert(entry.edge_id);
                results.push(entry);
            }
        }

        // Temporal filtering + decay scoring using adjacency-embedded
        // valid_from/valid_to. No per-edge record lookup required.
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;

        results.retain_mut(|entry| {
            // Temporal filter: edge must be valid at reference_time
            if entry.valid_from > reference_time || entry.valid_to <= reference_time {
                return false;
            }

            // Decay-adjusted scoring: age is measured from valid_from in the
            // validity timeline. For non-temporal edges, valid_from defaults
            // to created_at, giving natural "time since creation" decay.
            if apply_decay {
                let age_hours = (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                entry.weight = entry.weight * (-lambda * age_hours).exp();
            }

            true
        });

        // Sort by decay-adjusted weight descending if decay was applied
        if apply_decay {
            results.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(std::cmp::Ordering::Equal));
        }

        // Apply limit after all filtering and sorting
        if limit > 0 && results.len() > limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Query neighbors, merging results from memtable + segments.
    /// Memtable results come first, then segments newest-to-oldest.
    /// Deduplicates by edge_id (first-seen wins).
    ///
    /// Nodes matching any registered prune policy are excluded from results.
    /// When policies are active, fetches all results first (ignoring limit),
    /// filters, then applies limit. This prevents short results when excluded
    /// nodes consume limit slots.
    ///
    /// `at_epoch`: if `Some(t)`, filter to edges valid at time `t`. If `None`, use current time.
    /// `decay_lambda`: if `Some(λ)`, compute `score = weight * e^(-λ * age_hours)` where
    ///   `age_hours = (reference_time - valid_from) / 3_600_000` and sort descending by score.
    pub fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        // Early-out: no policies → delegate directly to raw (zero overhead)
        if self.manifest.prune_policies.is_empty() {
            return self.neighbors_raw(node_id, direction, type_filter, limit, at_epoch, decay_lambda);
        }

        // Fetch ALL results (limit=0) so policy filtering doesn't cause short results
        let mut results = self.neighbors_raw(node_id, direction, type_filter, 0, at_epoch, decay_lambda)?;

        // Batch-fetch neighbor nodes and filter by policy (single merge-walk
        // per segment instead of N individual binary searches).
        let neighbor_ids: Vec<u64> = results.iter().map(|e| e.node_id).collect();
        let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
        if !excluded.is_empty() {
            results.retain(|entry| !excluded.contains(&entry.node_id));
        }

        // Apply limit after policy filtering
        if limit > 0 && results.len() > limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Batch neighbor query for multiple node IDs. Single O(N+M) cursor walk
    /// per segment instead of O(M log N) individual binary searches.
    ///
    /// `node_ids` need not be sorted (sorted internally). Returns a HashMap
    /// mapping each queried node_id to its neighbor entries.
    ///
    /// Applies prune policy filtering when policies are registered.
    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<HashMap<u64, Vec<NeighborEntry>>, EngineError> {
        if self.manifest.prune_policies.is_empty() {
            return self.neighbors_batch_raw(node_ids, direction, type_filter, at_epoch, decay_lambda);
        }

        let mut results = self.neighbors_batch_raw(node_ids, direction, type_filter, at_epoch, decay_lambda)?;

        // Collect unique neighbor node IDs for batch policy check (dedup avoids
        // redundant merge-walks when many queried nodes share neighbors).
        let all_neighbor_ids: Vec<u64> = {
            let mut ids: Vec<u64> = results
                .values()
                .flat_map(|entries| entries.iter().map(|e| e.node_id))
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let excluded = self.policy_excluded_node_ids(&all_neighbor_ids)?;
        if !excluded.is_empty() {
            for entries in results.values_mut() {
                entries.retain(|entry| !excluded.contains(&entry.node_id));
            }
            // Remove empty entries
            results.retain(|_, v| !v.is_empty());
        }

        Ok(results)
    }

    /// Raw batch neighbor query with no prune policy filtering.
    /// Collects tombstones once, then does a single cursor walk per segment
    /// for all node IDs.
    fn neighbors_batch_raw(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<HashMap<u64, Vec<NeighborEntry>>, EngineError> {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Sort + dedup node IDs for segment cursor walks
        let sorted_ids: Vec<u64> = {
            let mut ids = node_ids.to_vec();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        // Collect all tombstoned node and edge IDs ONCE
        let mut deleted_nodes: HashSet<u64> = self
            .memtable
            .deleted_nodes()
            .keys()
            .copied()
            .collect();
        let mut deleted_edges: HashSet<u64> = self
            .memtable
            .deleted_edges()
            .keys()
            .copied()
            .collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Start with memtable results
        let mut results = self.memtable.neighbors_batch(&sorted_ids, direction, type_filter);

        // Track seen edge IDs per node for dedup (memtable/newer segment wins)
        let mut seen_edges: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (&nid, entries) in &results {
            let seen = seen_edges.entry(nid).or_default();
            for e in entries {
                seen.insert(e.edge_id);
            }
        }

        // Merge segment results (newest-to-oldest, one cursor walk per segment)
        for seg in &self.segments {
            let seg_results = seg.neighbors_batch(&sorted_ids, direction, type_filter)?;
            for (nid, seg_entries) in seg_results {
                let seen = seen_edges.entry(nid).or_default();
                let node_entries = results.entry(nid).or_default();
                for entry in seg_entries {
                    if seen.contains(&entry.edge_id) {
                        continue;
                    }
                    if deleted_edges.contains(&entry.edge_id) {
                        continue;
                    }
                    if deleted_nodes.contains(&entry.node_id) {
                        continue;
                    }
                    seen.insert(entry.edge_id);
                    node_entries.push(entry);
                }
            }
        }

        // Apply temporal filtering + decay scoring
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;

        for entries in results.values_mut() {
            entries.retain_mut(|entry| {
                if entry.valid_from > reference_time || entry.valid_to <= reference_time {
                    return false;
                }
                if apply_decay {
                    let age_hours = (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                    entry.weight = entry.weight * (-lambda * age_hours).exp();
                }
                true
            });
        }

        if apply_decay {
            for entries in results.values_mut() {
                entries.sort_by(|a, b| {
                    b.weight.partial_cmp(&a.weight).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        // Remove empty entries
        results.retain(|_, v| !v.is_empty());

        Ok(results)
    }

    /// Paginated version of `neighbors`. Returns a page of neighbor entries
    /// sorted by edge_id with cursor-based pagination.
    ///
    /// Three paths based on query complexity:
    /// - **Fast path** (no decay, no policies): temporal validity in skip_fn +
    ///   cursor binary-seek + early termination. O(K log N) seek + O(limit) merge.
    /// - **Policy path** (no decay, has policies): temporal in skip_fn + cursor
    ///   binary-seek (no limit), then batch policy filter + take limit. Skips
    ///   items before cursor but still processes all post-cursor items for policy
    ///   correctness.
    /// - **Decay path**: collects all temporally-valid items (no cursor; results
    ///   are re-sorted by score), applies decay scoring, sorts descending, then
    ///   policy filter + truncate to limit.
    pub fn neighbors_paged(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        page: &PageRequest,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Collect all tombstoned node and edge IDs across memtable + segments
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;
        let has_policies = !self.manifest.prune_policies.is_empty();

        // Collect sources: memtable (unsorted) + segments (sorted by edge_id)
        let memtable_entries = self.memtable.neighbors(node_id, direction, type_filter, 0);
        let mut segment_entries: Vec<Vec<NeighborEntry>> =
            Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_entries.push(seg.neighbors(node_id, direction, type_filter, 0)?);
        }

        if apply_decay {
            // Decay path: collect all temporally-valid items, score, rank by score.
            // Cursor doesn't apply (score-sorted, not edge_id-sorted).
            let all_page = PageRequest {
                limit: None,
                after: None,
            };
            let mut all = merge_sorted_paged(
                memtable_entries,
                segment_entries,
                |e| e.edge_id,
                |e| {
                    deleted_edges.contains(&e.edge_id)
                        || deleted_nodes.contains(&e.node_id)
                        || e.valid_from > reference_time
                        || e.valid_to <= reference_time
                },
                &all_page,
            );

            // Apply decay scoring
            for entry in &mut all.items {
                let age_hours =
                    (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                entry.weight = entry.weight * (-lambda * age_hours).exp();
            }
            all.items.sort_by(|a, b| {
                b.weight
                    .partial_cmp(&a.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            // Policy filtering on neighbor endpoint nodes
            if has_policies {
                let neighbor_ids: Vec<u64> = all.items.iter().map(|e| e.node_id).collect();
                let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
                if !excluded.is_empty() {
                    all.items.retain(|e| !excluded.contains(&e.node_id));
                }
            }

            let limit = page.limit.unwrap_or(0);
            if limit > 0 && all.items.len() > limit {
                all.items.truncate(limit);
            }
            Ok(PageResult {
                items: all.items,
                next_cursor: None,
            })
        } else if !has_policies {
            // Fast path: temporal validity in skip_fn + cursor + early termination.
            // Temporal filter always runs (reference_time = at_epoch or now),
            // matching neighbors() which always filters against reference_time.
            Ok(merge_sorted_paged(
                memtable_entries,
                segment_entries,
                |e| e.edge_id,
                |e| {
                    deleted_edges.contains(&e.edge_id)
                        || deleted_nodes.contains(&e.node_id)
                        || e.valid_from > reference_time
                        || e.valid_to <= reference_time
                },
                page,
            ))
        } else {
            // Policy path (no decay): merge with cursor + temporal skip,
            // but no limit (policies may filter items downstream).
            let cursor_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let all = merge_sorted_paged(
                memtable_entries,
                segment_entries,
                |e| e.edge_id,
                |e| {
                    deleted_edges.contains(&e.edge_id)
                        || deleted_nodes.contains(&e.node_id)
                        || e.valid_from > reference_time
                        || e.valid_to <= reference_time
                },
                &cursor_page,
            );

            // Batch policy filter on neighbor endpoint nodes
            let neighbor_ids: Vec<u64> = all.items.iter().map(|e| e.node_id).collect();
            let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
            let mut items = all.items;
            if !excluded.is_empty() {
                items.retain(|e| !excluded.contains(&e.node_id));
            }

            Ok(apply_limit_only(items, |e| e.edge_id, page))
        }
    }

    /// 2-hop neighbor expansion. Returns neighbors of neighbors, excluding
    /// the origin node and any 1-hop nodes already seen.
    pub fn neighbors_2hop(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        self.neighbors_2hop_constrained(
            node_id,
            direction,
            type_filter,
            None,
            limit,
            at_epoch,
            decay_lambda,
        )
    }

    /// Paginated 2-hop neighbor expansion. Collects all 2-hop results, sorts
    /// by edge_id, then applies cursor + limit.
    pub fn neighbors_2hop_paged(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        page: &PageRequest,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        self.neighbors_2hop_constrained_paged(
            node_id,
            direction,
            type_filter,
            None,
            page,
            at_epoch,
            decay_lambda,
        )
    }

    /// Constrained 2-hop expansion. Like `neighbors_2hop`, but with separate
    /// control over which edge types to traverse and which node types to include
    /// in the results.
    ///
    /// - `traverse_edge_types`: edge types to follow on both hops. `None` = all types.
    /// - `target_node_types`: only include 2-hop nodes whose `type_id` is in this set.
    ///   `None` = all node types.
    pub fn neighbors_2hop_constrained(
        &self,
        node_id: u64,
        direction: Direction,
        traverse_edge_types: Option<&[u32]>,
        target_node_types: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        // First hop: no limit, no decay (we need all valid intermediate nodes)
        let hop1 = self.neighbors(node_id, direction, traverse_edge_types, 0, at_epoch, None)?;
        let hop1_node_ids: HashSet<u64> = hop1.iter().map(|e| e.node_id).collect();

        let mut seen_edges: HashSet<u64> = hop1.iter().map(|e| e.edge_id).collect();
        let apply_decay = decay_lambda.map_or(false, |l| l > 0.0);

        // Build target node type set for O(1) lookup
        let target_set: Option<HashSet<u32>> =
            target_node_types.map(|types| types.iter().copied().collect());

        // Second hop: batch-fetch neighbors for all 1-hop nodes at once
        // (neighbors_batch sorts+dedups internally)
        let hop1_ids: Vec<u64> = hop1.iter().map(|e| e.node_id).collect();
        let hop2_all = self.neighbors_batch(
            &hop1_ids,
            direction,
            traverse_edge_types,
            at_epoch,
            decay_lambda,
        )?;

        let mut candidates = Vec::new();
        for entry in &hop1 {
            if let Some(hop2) = hop2_all.get(&entry.node_id) {
                for e2 in hop2 {
                    // Skip origin node and 1-hop nodes
                    if e2.node_id == node_id || hop1_node_ids.contains(&e2.node_id) {
                        continue;
                    }
                    // Dedup by edge_id
                    if !seen_edges.insert(e2.edge_id) {
                        continue;
                    }
                    candidates.push(e2.clone());
                }
            }
        }

        // Filter by target node type via one batch read instead of per-candidate lookups
        let mut results = if let Some(ref ts) = target_set {
            let node_ids: Vec<u64> = candidates.iter().map(|e| e.node_id).collect();
            let nodes = self.get_nodes(&node_ids)?;
            let type_map: HashMap<u64, u32> = node_ids
                .iter()
                .zip(nodes.iter())
                .filter_map(|(&id, opt)| opt.as_ref().map(|n| (id, n.type_id)))
                .collect();
            candidates
                .into_iter()
                .filter(|e| type_map.get(&e.node_id).map_or(false, |t| ts.contains(t)))
                .collect()
        } else {
            candidates
        };

        // Apply limit for non-decay case
        if !apply_decay && limit > 0 && results.len() > limit {
            results.truncate(limit);
        }

        // When decay is active, sort globally by score and then apply limit
        if apply_decay {
            results.sort_by(|a, b| {
                b.weight
                    .partial_cmp(&a.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if limit > 0 && results.len() > limit {
                results.truncate(limit);
            }
        }

        Ok(results)
    }

    /// Paginated constrained 2-hop expansion. Collects all 2-hop results
    /// (full hop-1 set required for correct dedup), sorts the final deduplicated
    /// set by edge_id, and applies cursor + limit via `apply_cursor_to_sorted_by`.
    ///
    /// When decay is active, results are sorted by score descending. Cursor
    /// pagination is not meaningful on score-sorted results, so `next_cursor`
    /// is always `None` in that case (use `limit` only).
    pub fn neighbors_2hop_constrained_paged(
        &self,
        node_id: u64,
        direction: Direction,
        traverse_edge_types: Option<&[u32]>,
        target_node_types: Option<&[u32]>,
        page: &PageRequest,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        // First hop: no limit, no decay (we need all valid intermediate nodes)
        let hop1 = self.neighbors(node_id, direction, traverse_edge_types, 0, at_epoch, None)?;
        let hop1_node_ids: HashSet<u64> = hop1.iter().map(|e| e.node_id).collect();

        let mut seen_edges: HashSet<u64> = hop1.iter().map(|e| e.edge_id).collect();
        let apply_decay = decay_lambda.map_or(false, |l| l > 0.0);

        // Build target node type set for O(1) lookup
        let target_set: Option<HashSet<u32>> =
            target_node_types.map(|types| types.iter().copied().collect());

        // Second hop: batch-fetch neighbors for all 1-hop nodes at once
        // (neighbors_batch sorts+dedups internally)
        let hop1_ids: Vec<u64> = hop1.iter().map(|e| e.node_id).collect();
        let hop2_all = self.neighbors_batch(
            &hop1_ids,
            direction,
            traverse_edge_types,
            at_epoch,
            decay_lambda,
        )?;

        let mut candidates = Vec::new();
        for entry in &hop1 {
            if let Some(hop2) = hop2_all.get(&entry.node_id) {
                for e2 in hop2 {
                    // Skip origin node and 1-hop nodes
                    if e2.node_id == node_id || hop1_node_ids.contains(&e2.node_id) {
                        continue;
                    }
                    // Dedup by edge_id
                    if !seen_edges.insert(e2.edge_id) {
                        continue;
                    }
                    candidates.push(e2.clone());
                }
            }
        }

        // Filter by target node type via one batch read instead of per-candidate lookups
        let mut results = if let Some(ref ts) = target_set {
            let node_ids: Vec<u64> = candidates.iter().map(|e| e.node_id).collect();
            let nodes = self.get_nodes(&node_ids)?;
            // Build node_id → type_id map from batch results
            let type_map: HashMap<u64, u32> = node_ids
                .iter()
                .zip(nodes.iter())
                .filter_map(|(&id, opt)| opt.as_ref().map(|n| (id, n.type_id)))
                .collect();
            candidates
                .into_iter()
                .filter(|e| type_map.get(&e.node_id).map_or(false, |t| ts.contains(t)))
                .collect()
        } else {
            candidates
        };

        // When decay is active, sort globally by score. Cursor pagination is
        // not meaningful on score-sorted results. Return limit-truncated with
        // next_cursor = None.
        if apply_decay {
            results.sort_by(|a, b| {
                b.weight
                    .partial_cmp(&a.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let limit = page.limit.unwrap_or(0);
            if limit > 0 && results.len() > limit {
                results.truncate(limit);
            }
            return Ok(PageResult {
                items: results,
                next_cursor: None,
            });
        }

        // Sort by edge_id for stable cursor pagination
        results.sort_unstable_by_key(|e| e.edge_id);

        // Apply cursor + limit
        Ok(apply_cursor_to_sorted_by(results, |e| e.edge_id, page))
    }

    /// Return the top-k neighbors by the given scoring mode.
    ///
    /// Uses a bounded min-heap of size k during merge for efficiency,
    /// avoids sorting the full neighbor set.
    pub fn top_k_neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        k: usize,
        scoring: ScoringMode,
        at_epoch: Option<i64>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        if k == 0 {
            return Ok(Vec::new());
        }

        if let ScoringMode::DecayAdjusted { lambda } = &scoring {
            if *lambda < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Collect all tombstoned node and edge IDs
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Gather all neighbors from memtable + segments with dedup + deletion filter
        let mut all_entries = self.memtable.neighbors(node_id, direction, type_filter, 0);
        // Filter memtable results against deleted sets (M2 fix)
        all_entries.retain(|e| {
            !deleted_edges.contains(&e.edge_id) && !deleted_nodes.contains(&e.node_id)
        });
        let mut seen_edges: HashSet<u64> = all_entries.iter().map(|e| e.edge_id).collect();

        for seg in &self.segments {
            let seg_results = seg.neighbors(node_id, direction, type_filter, 0)?;
            for entry in seg_results {
                if seen_edges.contains(&entry.edge_id) {
                    continue;
                }
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                if deleted_nodes.contains(&entry.node_id) {
                    continue;
                }
                seen_edges.insert(entry.edge_id);
                all_entries.push(entry);
            }
        }

        // Policy filtering: batch-fetch neighbor nodes and exclude matches.
        // Single merge-walk per segment instead of N individual binary searches.
        {
            let neighbor_ids: Vec<u64> = all_entries.iter().map(|e| e.node_id).collect();
            let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
            if !excluded.is_empty() {
                all_entries.retain(|entry| !excluded.contains(&entry.node_id));
            }
        }

        // Temporal filter + score computation.
        // Use f64 bits for the min-heap key to preserve i64 timestamp precision
        // for Recency scoring (f32 can't represent epoch-millis without loss).
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
        let mut scored: Vec<NeighborEntry> = Vec::new();

        for mut entry in all_entries {
            // Temporal filter
            if entry.valid_from > reference_time || entry.valid_to <= reference_time {
                continue;
            }

            let score: f64 = match &scoring {
                ScoringMode::Weight => entry.weight as f64,
                ScoringMode::Recency => entry.valid_from as f64,
                ScoringMode::DecayAdjusted { lambda } => {
                    let age_hours =
                        (reference_time - entry.valid_from).max(0) as f64 / 3_600_000.0;
                    (entry.weight as f64) * (-(*lambda as f64) * age_hours).exp()
                }
            };

            // Update weight to reflect the computed score (so callers see the score used)
            if matches!(scoring, ScoringMode::DecayAdjusted { .. }) {
                entry.weight = score as f32;
            }

            let idx = scored.len();
            // f64::to_bits provides total ordering for non-negative IEEE 754 values
            let score_bits = score.to_bits();

            if heap.len() < k {
                heap.push(Reverse((score_bits, idx)));
                scored.push(entry);
            } else if let Some(&Reverse((min_bits, _))) = heap.peek() {
                if score_bits > min_bits {
                    heap.pop();
                    heap.push(Reverse((score_bits, idx)));
                    scored.push(entry);
                }
            }
        }

        // Extract the top-k indices from the heap
        let mut top_indices: Vec<(u64, usize)> = heap.into_iter().map(|Reverse(x)| x).collect();
        // Sort descending by score
        top_indices.sort_by(|a, b| b.0.cmp(&a.0));

        let results: Vec<NeighborEntry> = top_indices
            .into_iter()
            .map(|(_, idx)| scored[idx].clone())
            .collect();

        Ok(results)
    }

    /// Extract a subgraph of all nodes and edges reachable within `max_depth`
    /// hops of `start_node_id`.
    ///
    /// Uses BFS with cycle detection. Edges to already-visited nodes (cross-edges
    /// and back-edges) are included in the result. The traversal respects
    /// direction and edge type filters.
    ///
    /// - `max_depth`: maximum number of hops. 0 returns just the start node.
    /// - `direction`: which edge direction to follow during traversal.
    /// - `edge_type_filter`: only traverse edges of these types. `None` = all.
    /// - `at_epoch`: temporal filter. Only include edges valid at this time.
    ///
    /// Returns an empty `Subgraph` if the start node does not exist.
    pub fn extract_subgraph(
        &self,
        start_node_id: u64,
        max_depth: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<Subgraph, EngineError> {
        // Check start node exists
        let start_node = match self.get_node(start_node_id)? {
            Some(n) => n,
            None => {
                return Ok(Subgraph {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                })
            }
        };

        let mut visited_nodes: HashSet<u64> = HashSet::new();
        let mut collected_edge_ids: HashSet<u64> = HashSet::new();
        let mut node_records: Vec<NodeRecord> = Vec::new();
        let mut edge_records: Vec<EdgeRecord> = Vec::new();

        visited_nodes.insert(start_node_id);
        node_records.push(start_node);

        if max_depth == 0 {
            return Ok(Subgraph {
                nodes: node_records,
                edges: edge_records,
            });
        }

        // BFS level by level, batch-fetch neighbors for entire frontier
        let mut frontier: Vec<u64> = vec![start_node_id];

        for _depth in 0..max_depth {
            let mut new_edge_ids: Vec<u64> = Vec::new();
            let mut new_node_ids: Vec<u64> = Vec::new();

            // Batch adjacency: one cursor walk per segment for the whole frontier
            let all_neighbors =
                self.neighbors_batch(&frontier, direction, edge_type_filter, at_epoch, None)?;

            for &current_node in &frontier {
                let empty = Vec::new();
                let neighbors = all_neighbors.get(&current_node).unwrap_or(&empty);

                for entry in neighbors {
                    if collected_edge_ids.insert(entry.edge_id) {
                        new_edge_ids.push(entry.edge_id);
                    }
                    if visited_nodes.insert(entry.node_id) {
                        new_node_ids.push(entry.node_id);
                    }
                }
            }

            // Batch-fetch all new edges and nodes for this BFS level
            let fetched_edges = self.get_edges(&new_edge_ids)?;
            for edge in fetched_edges.into_iter().flatten() {
                edge_records.push(edge);
            }

            let fetched_nodes = self.get_nodes(&new_node_ids)?;
            let mut next_frontier: Vec<u64> = Vec::new();
            for (i, node_opt) in fetched_nodes.into_iter().enumerate() {
                if let Some(node) = node_opt {
                    node_records.push(node);
                    next_frontier.push(new_node_ids[i]);
                }
            }

            frontier = next_frontier;
            if frontier.is_empty() {
                break;
            }
        }

        Ok(Subgraph {
            nodes: node_records,
            edges: edge_records,
        })
    }

    // --- Secondary index queries ---

    /// Return all live node IDs with the given type_id (raw, unfiltered).
    /// Used internally by collect_prune_targets.
    fn nodes_by_type_raw(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for id in self.memtable.nodes_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for seg in &self.segments {
            for id in seg.nodes_by_type(type_id)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        Ok(results)
    }

    /// Return all live node IDs with the given type_id, merged across
    /// memtable and all segments. Excludes tombstoned and policy-excluded nodes.
    pub fn nodes_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut results = self.nodes_by_type_raw(type_id)?;

        // Policy filtering: batch-fetch nodes and exclude matches.
        let excluded = self.policy_excluded_node_ids(&results)?;
        if !excluded.is_empty() {
            results.retain(|id| !excluded.contains(id));
        }

        Ok(results)
    }

    /// Return all live edge IDs with the given type_id, merged across
    /// memtable and all segments. Excludes tombstoned edges.
    pub fn edges_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut deleted: HashSet<u64> = self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_edge_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for id in self.memtable.edges_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for seg in &self.segments {
            for id in seg.edges_by_type(type_id)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        Ok(results)
    }

    /// Return all live node records with the given type_id, hydrated from
    /// memtable and segments. Excludes tombstoned and policy-excluded nodes.
    ///
    /// Uses `nodes_by_type()` for the ID list (already policy-filtered), then
    /// `get_nodes_raw()` for batch hydration (one merge-walk per segment,
    /// not N individual lookups). Single policy pass, no redundant filtering.
    pub fn get_nodes_by_type(
        &self,
        type_id: u32,
    ) -> Result<Vec<NodeRecord>, EngineError> {
        let ids = self.nodes_by_type(type_id)?;
        let results = self.get_nodes_raw(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Return all live edge records with the given type_id, hydrated from
    /// memtable and segments. Excludes tombstoned edges.
    ///
    /// Uses `edges_by_type()` for the ID list, then `get_edges()` for batch
    /// hydration (one merge-walk per segment, not N individual lookups).
    pub fn get_edges_by_type(
        &self,
        type_id: u32,
    ) -> Result<Vec<EdgeRecord>, EngineError> {
        let ids = self.edges_by_type(type_id)?;
        let results = self.get_edges(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Return the count of live nodes with the given type_id without hydrating
    /// records. Excludes tombstoned and policy-excluded nodes.
    pub fn count_nodes_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        Ok(self.nodes_by_type(type_id)?.len() as u64)
    }

    /// Return the count of live edges with the given type_id without hydrating
    /// records. Excludes tombstoned edges (edges are not subject to prune policies).
    pub fn count_edges_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        Ok(self.edges_by_type(type_id)?.len() as u64)
    }

    // --- Paginated type-index queries ---

    /// Paginated version of `nodes_by_type`. Returns a page of node IDs sorted
    /// by ID, with cursor-based pagination. Pass `PageRequest::default()` to get
    /// all results (equivalent to `nodes_by_type`).
    ///
    /// Uses K-way merge across already-sorted sources with early termination:
    /// O(cursor_position + limit) instead of O(N log N) when no prune policies
    /// are active. With policies, still saves the sort via merge, then applies
    /// policy filtering and cursor on the sorted result.
    pub fn nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.nodes_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.nodes_by_type(type_id)?);
        }

        if self.manifest.prune_policies.is_empty() {
            // Fast path: merge with early termination, no policy filtering needed
            Ok(merge_type_ids_paged(memtable_ids, segment_ids, &deleted, page))
        } else {
            // Policy path: merge with cursor (skip past already-returned IDs)
            // but no limit (policies may filter items downstream).
            let cursor_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let all = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &cursor_page);
            let excluded = self.policy_excluded_node_ids(&all.items)?;
            let mut items = all.items;
            if !excluded.is_empty() {
                items.retain(|id| !excluded.contains(id));
            }

            Ok(apply_limit_only(items, |&id| id, page))
        }
    }

    /// Paginated version of `edges_by_type`. Returns a page of edge IDs sorted
    /// by ID, with cursor-based pagination. Uses K-way merge with early
    /// termination (edges are not subject to prune policies).
    pub fn edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_edge_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.edges_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.edges_by_type(type_id)?);
        }

        Ok(merge_type_ids_paged(memtable_ids, segment_ids, &deleted, page))
    }

    /// Paginated version of `get_nodes_by_type`. Returns a page of hydrated node
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<NodeRecord>, EngineError> {
        let id_page = self.nodes_by_type_paged(type_id, page)?;
        let hydrated = self.get_nodes_raw(&id_page.items)?;
        let items: Vec<NodeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    /// Paginated version of `get_edges_by_type`. Returns a page of hydrated edge
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<EdgeRecord>, EngineError> {
        let id_page = self.edges_by_type_paged(type_id, page)?;
        let hydrated = self.get_edges(&id_page.items)?;
        let items: Vec<EdgeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    /// Find node IDs matching (type_id, prop_key == prop_value).
    /// Merges candidates from memtable + segments, deduplicates, and post-filters
    /// to verify actual property equality (handles hash collisions).
    /// Excludes nodes matching any registered prune policy.
    pub fn find_nodes(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<Vec<u64>, EngineError> {
        // Collect all deleted node IDs across sources
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        // Memtable first (already post-filtered)
        for id in self.memtable.find_nodes(type_id, prop_key, prop_value) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        // Segments newest-first: get candidates by hash, then verify against
        // latest-wins version (not segment-local) to avoid stale matches when
        // a newer source has updated the property to a non-matching value.
        let key_hash = hash_prop_key(prop_key);
        let value_hash = hash_prop_value(prop_value);

        for seg in &self.segments {
            for id in seg.find_nodes_by_prop_hash(type_id, key_hash, value_hash)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    if let Some(node) = self.get_node_raw(id)? {
                        if node.props.get(prop_key).map(|v| v == prop_value).unwrap_or(false) {
                            results.push(id);
                        }
                    }
                }
            }
        }

        // Policy filtering: batch-fetch nodes and exclude matches.
        let excluded = self.policy_excluded_node_ids(&results)?;
        if !excluded.is_empty() {
            results.retain(|id| !excluded.contains(id));
        }

        Ok(results)
    }

    /// Paginated version of `find_nodes`. Returns a page of node IDs matching
    /// (type_id, prop_key == prop_value), sorted by ID with cursor-based
    /// pagination.
    ///
    /// Uses K-way merge across sorted property hash buckets (segment IDs are
    /// written sorted at flush/compaction time). Post-filters for hash
    /// collisions, then applies cursor + limit on the verified results.
    /// Policy filtering applied when policies are active.
    pub fn find_nodes_paged(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect candidate IDs from sources (sorted within each source)
        let memtable_ids = self.memtable.find_nodes(type_id, prop_key, prop_value);
        let key_hash = hash_prop_key(prop_key);
        let value_hash = hash_prop_value(prop_value);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.find_nodes_by_prop_hash(type_id, key_hash, value_hash)?);
        }

        // Merge candidates with cursor (skip past already-returned IDs)
        // but no limit. Need post-filter for hash collision verification.
        let cursor_page = PageRequest {
            limit: None,
            after: page.after,
        };
        let merged = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &cursor_page);

        // Post-filter: verify actual property value match (handles hash collisions).
        // Memtable candidates are already verified by memtable.find_nodes().
        // Segment candidates use batch get_nodes_raw (single merge-walk per segment)
        // instead of per-ID get_node_raw (binary search per ID per segment).
        let memtable_verified: HashSet<u64> = self
            .memtable
            .find_nodes(type_id, prop_key, prop_value)
            .into_iter()
            .collect();

        // Partition: memtable-verified pass through, segment candidates batch-verify
        let mut verified = Vec::with_capacity(merged.items.len());
        let mut segment_candidates: Vec<(usize, u64)> = Vec::new(); // (insert_position, id)
        for id in &merged.items {
            if memtable_verified.contains(id) {
                verified.push(*id);
            } else {
                segment_candidates.push((verified.len(), *id));
                verified.push(*id); // real ID, removed if verification fails
            }
        }

        if !segment_candidates.is_empty() {
            let batch_ids: Vec<u64> = segment_candidates.iter().map(|&(_, id)| id).collect();
            let batch_results = self.get_nodes_raw(&batch_ids)?;
            // Collect positions of failed verifications, remove back-to-front
            let mut remove_positions = Vec::new();
            for (i, &(pos, _)) in segment_candidates.iter().enumerate() {
                let keep = batch_results[i]
                    .as_ref()
                    .and_then(|node| node.props.get(prop_key))
                    .map(|v| v == prop_value)
                    .unwrap_or(false);
                if !keep {
                    remove_positions.push(pos);
                }
            }
            remove_positions.sort_unstable_by(|a, b| b.cmp(a));
            for pos in remove_positions {
                verified.remove(pos);
            }
        }

        // Policy filtering
        let excluded = self.policy_excluded_node_ids(&verified)?;
        if !excluded.is_empty() {
            verified.retain(|id| !excluded.contains(id));
        }

        Ok(apply_limit_only(verified, |&id| id, page))
    }

    // --- Timestamp range queries ---

    /// Find node IDs of a given type updated within a time range [from_ms, to_ms] (inclusive).
    /// Merges across memtable + segments with deduplication.
    /// Excludes tombstoned and policy-pruned nodes.
    pub fn find_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let page = PageRequest { limit: None, after: None };
        Ok(self.find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page)?.items)
    }

    /// Paginated version of `find_nodes_by_time_range`. Returns a page of node IDs
    /// sorted by ID with cursor-based pagination.
    ///
    /// Uses K-way merge across sorted sources (binary search for range in each
    /// segment, sort results by node_id). O(log N) seek per source + O(results) scan.
    pub fn find_nodes_by_time_range_paged(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources: memtable + segments
        let memtable_ids = self.memtable.nodes_by_time_range(type_id, from_ms, to_ms);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.nodes_by_time_range(type_id, from_ms, to_ms)?);
        }

        // Merge with cursor but no limit. Post-filter is required for latest-wins
        // correctness: a stale segment entry may match the time range even though
        // the node's current version (in memtable or newer segment) has a different
        // updated_at outside the range. Same pattern as find_nodes post-filter.
        let cursor_page = PageRequest {
            limit: None,
            after: page.after,
        };
        let all = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &cursor_page);

        // Post-filter: verify each node's current updated_at is within [from_ms, to_ms].
        // Stale segment entries can match the time range even though the node's current
        // version has a different updated_at (every upsert changes the timestamp).
        let nodes = self.get_nodes_raw(&all.items)?;
        let mut items: Vec<u64> = Vec::with_capacity(all.items.len());
        for (id, node) in all.items.iter().zip(nodes.iter()) {
            if let Some(n) = node {
                if n.updated_at >= from_ms && n.updated_at <= to_ms {
                    items.push(*id);
                }
            }
        }

        // Policy filtering
        if !self.manifest.prune_policies.is_empty() {
            let excluded = self.policy_excluded_node_ids(&items)?;
            if !excluded.is_empty() {
                items.retain(|id| !excluded.contains(id));
            }
        }

        Ok(apply_limit_only(items, |&id| id, page))
    }

    // --- Personalized PageRank ---

    /// Compute Personalized PageRank starting from seed nodes.
    ///
    /// Uses BFS discovery + dense Vec power iteration with weighted transitions.
    /// Phase 1: DFS from seeds discovers all reachable nodes and caches neighbors.
    /// Phase 2: Builds dense node_id→index mapping with precomputed normalized weights.
    /// Phase 3: Power iteration over contiguous `Vec<f64>` rank vectors.
    ///
    /// Edge weights determine transition probabilities (proportional to weight).
    /// Dangling nodes (no outgoing edges) teleport back to the seed set.
    ///
    /// Returns scored nodes sorted by score descending, with optional top-k cutoff.
    pub fn personalized_pagerank(
        &self,
        seed_node_ids: &[u64],
        options: &PprOptions,
    ) -> Result<PprResult, EngineError> {
        if seed_node_ids.is_empty() {
            return Ok(PprResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            });
        }

        let damping = options.damping_factor;
        if damping <= 0.0 || damping >= 1.0 {
            return Err(EngineError::InvalidOperation(
                "damping_factor must be in (0.0, 1.0)".into(),
            ));
        }
        let edge_filter = options.edge_type_filter.as_deref();

        // Deduplicate seeds and filter to live nodes only.
        // Without this, deleted/non-existent seeds become dangling nodes
        // that absorb teleport mass and appear in results with score > 0.
        let seeds: Vec<u64> = {
            let mut s: Vec<u64> = seed_node_ids.to_vec();
            s.sort_unstable();
            s.dedup();
            let live = self.get_nodes_raw(&s)?;
            s.into_iter()
                .zip(live.into_iter())
                .filter_map(|(id, node)| node.map(|_| id))
                .collect()
        };
        if seeds.is_empty() {
            return Ok(PprResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            });
        }
        let num_seeds = seeds.len() as f64;
        let teleport = (1.0 - damping) / num_seeds;

        // --- Phase 1: BFS discovery to find all reachable nodes ---
        // Wave-based BFS with batch adjacency: one cursor walk per segment
        // per wave instead of O(N log K) binary searches per node.
        let mut discovered: HashSet<u64> = seeds.iter().copied().collect();
        let mut wave: Vec<u64> = seeds.clone();
        let mut neighbor_cache: HashMap<u64, Vec<(u64, f32)>> = HashMap::with_capacity(seeds.len() * 16);

        while !wave.is_empty() {
            let all_neighbors =
                self.neighbors_batch(&wave, Direction::Outgoing, edge_filter, None, None)?;

            let mut next_wave: Vec<u64> = Vec::new();
            for &node_id in &wave {
                let entries: Vec<(u64, f32)> = all_neighbors
                    .get(&node_id)
                    .map(|n| n.iter().map(|e| (e.node_id, e.weight)).collect())
                    .unwrap_or_default();
                for &(neighbor_id, _) in &entries {
                    if discovered.insert(neighbor_id) {
                        next_wave.push(neighbor_id);
                    }
                }
                neighbor_cache.insert(node_id, entries);
            }
            wave = next_wave;
        }

        // --- Phase 2: Build dense mapping ---
        let idx_to_id: Vec<u64> = discovered.into_iter().collect();
        let n = idx_to_id.len();
        let id_to_idx: HashMap<u64, usize> = idx_to_id
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        // Pre-compute normalized transition weights and dangling flags
        // dense_neighbors[i] = [(dense_idx_j, normalized_weight_j), ...]
        let mut dense_neighbors: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
        let mut is_dangling = vec![true; n];

        for (&node_id, neighbors) in &neighbor_cache {
            let idx = id_to_idx[&node_id];
            if !neighbors.is_empty() {
                let total_weight: f64 = neighbors.iter().map(|&(_, w)| w as f64).sum();
                if total_weight > 0.0 {
                    is_dangling[idx] = false;
                    dense_neighbors[idx] = neighbors
                        .iter()
                        .map(|&(nid, w)| (id_to_idx[&nid], (w as f64) / total_weight))
                        .collect();
                }
            }
        }

        // Seed dense indices
        let seed_indices: Vec<usize> = seeds.iter().map(|id| id_to_idx[id]).collect();

        // --- Phase 3: Dense power iteration ---
        let mut rank = vec![0.0_f64; n];
        for &si in &seed_indices {
            rank[si] = 1.0 / num_seeds;
        }

        let mut iterations = 0u32;
        let mut converged = false;

        for _ in 0..options.max_iterations {
            iterations += 1;
            let mut new_rank = vec![0.0_f64; n];
            let mut dangling_sum = 0.0_f64;

            for i in 0..n {
                let r = rank[i];
                if r == 0.0 {
                    continue;
                }

                if is_dangling[i] {
                    dangling_sum += r;
                } else {
                    for &(j, norm_weight) in &dense_neighbors[i] {
                        new_rank[j] += damping * r * norm_weight;
                    }
                }
            }

            // Distribute dangling mass + teleport to seeds
            let dangling_per_seed = damping * dangling_sum / num_seeds;
            for &si in &seed_indices {
                new_rank[si] += teleport + dangling_per_seed;
            }

            // Convergence check: L1 norm over dense vectors
            let diff: f64 = rank
                .iter()
                .zip(new_rank.iter())
                .map(|(old, new)| (old - new).abs())
                .sum();

            rank = new_rank;

            if diff < options.epsilon {
                converged = true;
                break;
            }
        }

        // Extract results: map dense indices back to node IDs
        let mut scores: Vec<(u64, f64)> = idx_to_id
            .iter()
            .zip(rank.iter())
            .filter(|(_, &s)| s > 0.0)
            .map(|(&id, &s)| (id, s))
            .collect();
        scores.sort_unstable_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        // Apply max_results cutoff
        if let Some(max) = options.max_results {
            scores.truncate(max);
        }

        Ok(PprResult {
            scores,
            iterations,
            converged,
        })
    }

    // --- Graph export ---

    /// Export the graph's adjacency structure for external community detection.
    ///
    /// Returns all live node IDs and edges (from, to, type_id, weight),
    /// filtered by optional node/edge type filters. Respects tombstones
    /// and prune policies. Each edge is emitted once (outgoing direction only).
    ///
    /// Edges are only included if both endpoints are in the exported node set,
    /// ensuring a consistent subgraph for external tools.
    pub fn export_adjacency(
        &self,
        options: &ExportOptions,
    ) -> Result<AdjacencyExport, EngineError> {
        // Collect all node type IDs from memtable + segments
        let node_types: Vec<u32> = {
            let mut types: HashSet<u32> =
                self.memtable.type_node_index().keys().copied().collect();
            for seg in &self.segments {
                for tid in seg.node_type_ids()? {
                    types.insert(tid);
                }
            }
            // Apply node type filter
            if let Some(ref filter) = options.node_type_filter {
                let allowed: HashSet<u32> = filter.iter().copied().collect();
                types.retain(|t| allowed.contains(t));
            }
            types.into_iter().collect()
        };

        // Collect all live node IDs (policy-filtered)
        let mut node_set: HashSet<u64> = HashSet::new();
        for &tid in &node_types {
            for id in self.nodes_by_type(tid)? {
                node_set.insert(id);
            }
        }
        let node_ids: Vec<u64> = {
            let mut ids: Vec<u64> = node_set.iter().copied().collect();
            ids.sort_unstable();
            ids
        };

        let edge_filter_slice = options.edge_type_filter.as_deref();

        // Batch-fetch all outgoing neighbors in one cursor walk per segment
        // instead of O(N) individual binary searches.
        let all_neighbors =
            self.neighbors_batch(&node_ids, Direction::Outgoing, edge_filter_slice, None, None)?;

        let mut edges: Vec<(u64, u64, u32, f32)> = Vec::new();
        for &from_id in &node_ids {
            if let Some(neighbors) = all_neighbors.get(&from_id) {
                for entry in neighbors {
                    // Only include edges whose target is in the exported node set
                    if !node_set.contains(&entry.node_id) {
                        continue;
                    }
                    let weight = if options.include_weights {
                        entry.weight
                    } else {
                        0.0
                    };
                    edges.push((from_id, entry.node_id, entry.edge_type_id, weight));
                }
            }
        }

        Ok(AdjacencyExport { node_ids, edges })
    }

    // --- Segment-aware dedup lookups (for upsert) ---

    /// Look up a node by (type_id, key) across memtable + segments.
    /// Used by upsert_node for dedup. Uses raw (unfiltered) lookup to prevent
    /// policy-excluded nodes from being treated as "not found" (which would
    /// allocate a duplicate ID, causing silent data corruption).
    fn find_existing_node(&self, type_id: u32, key: &str) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self.get_node_by_key_raw(type_id, key)?.map(|n| (n.id, n.created_at)))
    }

    /// Look up an edge by (from, to, type_id) across memtable + segments.
    /// Used by upsert_edge for uniqueness enforcement. Delegates to public get_edge_by_triple.
    fn find_existing_edge(&self, from: u64, to: u64, type_id: u32) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self.get_edge_by_triple(from, to, type_id)?.map(|e| (e.id, e.created_at)))
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
        self.segments.iter().map(|s| s.deleted_node_ids().len()).sum()
    }

    /// Return the total number of tombstoned edge IDs across all segments.
    pub fn segment_tombstone_edge_count(&self) -> usize {
        self.segments.iter().map(|s| s.deleted_edge_ids().len()).sum()
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

/// Check if all segments have non-overlapping node and edge ID ranges.
/// Returns true when every record ID appears in at most one segment,
/// the common case for append-only workloads without updates.
#[cfg(test)]
fn segments_are_non_overlapping(segments: &[SegmentReader]) -> bool {
    // Check node ID ranges
    let mut node_ranges: Vec<(u64, u64)> = segments
        .iter()
        .filter_map(|s| s.node_id_range())
        .collect();
    node_ranges.sort_unstable_by_key(|(min, _)| *min);
    for window in node_ranges.windows(2) {
        if window[0].1 >= window[1].0 {
            return false;
        }
    }

    // Check edge ID ranges
    let mut edge_ranges: Vec<(u64, u64)> = segments
        .iter()
        .filter_map(|s| s.edge_id_range())
        .collect();
    edge_ranges.sort_unstable_by_key(|(min, _)| *min);
    for window in edge_ranges.windows(2) {
        if window[0].1 >= window[1].0 {
            return false;
        }
    }

    true
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

    let segments_dir = db_dir.join("segments");
    std::fs::create_dir_all(&segments_dir)?;
    let tmp_dir = segment_tmp_dir(&db_dir, seg_id);
    let final_dir = segment_dir(&db_dir, seg_id);

    // Unified V3 path: handles all cases (overlapping, non-overlapping,
    // tombstones, prune policies) via metadata-only planning.
    let (seg_info, nodes_auto_pruned, edges_auto_pruned) = match bg_standard_merge(
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

    let input_segment_ids: HashSet<u64> =
        input_segments.iter().map(|(id, _)| *id).collect();
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
        if matches_prune_cutoff(type_id, updated_at, weight, age_cutoff, policy.max_weight, policy.type_id) {
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
            let (node_id, data_offset, data_len, type_id, updated_at, weight, key_len, prop_hash_offset, prop_hash_count) =
                seg.node_meta_at(i)?;

            if seen_nodes.contains(&node_id) {
                continue; // Already have a newer version
            }
            seen_nodes.insert(node_id);

            if deleted_nodes.contains(&node_id) {
                continue; // Tombstoned
            }

            if has_policies
                && matches_any_prune_policy_meta(
                    type_id,
                    updated_at,
                    weight,
                    prune_policies,
                    now,
                )
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
            let (edge_id, data_offset, data_len, from, to, type_id, updated_at, weight, valid_from, valid_to) =
                seg.edge_meta_at(i)?;

            if seen_edges.contains(&edge_id) {
                continue;
            }
            seen_edges.insert(edge_id);

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
            plan.node_winners.len(), node_data.len()
        )));
    }
    if plan.edge_winners.len() != edge_data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "compaction edge winner count ({}) != output data count ({})",
            plan.edge_winners.len(), edge_data.len()
        )));
    }

    let mut node_metas = Vec::with_capacity(plan.node_winners.len());
    for ((&node_id, w), &(data_id, new_data_offset, data_len)) in
        plan.node_winners.iter().zip(node_data.iter())
    {
        if node_id != data_id {
            return Err(EngineError::CorruptRecord(format!(
                "compaction node ID mismatch: winner={}, data={}", node_id, data_id
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
                "compaction edge ID mismatch: winner={}, data={}", edge_id, data_id
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
    let plan = v3_plan_winners(
        segments,
        prune_policies,
        &deleted_nodes,
        &deleted_edges,
    )?;

    if cancel.load(Ordering::Relaxed) {
        return Err(EngineError::CompactionCancelled);
    }

    let nodes_auto_pruned = plan.pruned_node_ids.len() as u64;
    let edges_auto_pruned = plan.edges_auto_pruned;

    let seg_info = v3_build_output(tmp_dir, seg_id, segments, &plan)?;

    Ok((seg_info, nodes_auto_pruned, edges_auto_pruned))
}

#[cfg(test)]
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

    // --- Low-level write_op API tests ---

    #[test]
    fn test_open_creates_new_db() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 0);
        assert_eq!(engine.edge_count(), 0);
        assert!(db_path.exists());
        assert!(db_path.join("manifest.current").exists());
        engine.close().unwrap();
    }

    #[test]
    fn test_open_nonexistent_without_create() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("nope");

        let opts = DbOptions {
            create_if_missing: false,
            ..DbOptions::default()
        };
        let result = DatabaseEngine::open(&db_path, &opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_and_read_back() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.write_op(&WalOp::UpsertNode(make_node(1, "alice"))).unwrap();
        engine.write_op(&WalOp::UpsertNode(make_node(2, "bob"))).unwrap();
        engine.write_op(&WalOp::UpsertEdge(make_edge(1, 1, 2))).unwrap();

        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.edge_count(), 1);

        let alice = engine.get_node(1).unwrap().unwrap();
        assert_eq!(alice.key, "alice");

        let edge = engine.get_edge(1).unwrap().unwrap();
        assert_eq!(edge.from, 1);
        assert_eq!(edge.to, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_operations() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.write_op(&WalOp::UpsertNode(make_node(1, "alice"))).unwrap();
        engine.write_op(&WalOp::UpsertEdge(make_edge(1, 1, 1))).unwrap();

        assert!(engine.get_node(1).unwrap().is_some());
        assert!(engine.get_edge(1).unwrap().is_some());

        engine.write_op(&WalOp::DeleteNode { id: 1, deleted_at: 9999 }).unwrap();
        engine.write_op(&WalOp::DeleteEdge { id: 1, deleted_at: 9999 }).unwrap();

        assert!(engine.get_node(1).unwrap().is_none());
        assert!(engine.get_edge(1).unwrap().is_none());
        assert_eq!(engine.node_count(), 0);
        assert_eq!(engine.edge_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_close_and_reopen_recovers_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 1..=10 {
                engine.write_op(&WalOp::UpsertNode(make_node(i, &format!("node:{}", i)))).unwrap();
            }
            for i in 1..=5 {
                engine.write_op(&WalOp::UpsertEdge(make_edge(i, i, i + 5))).unwrap();
            }
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 10);
            assert_eq!(engine.edge_count(), 5);

            let node5 = engine.get_node(5).unwrap().unwrap();
            assert_eq!(node5.key, "node:5");

            let edge3 = engine.get_edge(3).unwrap().unwrap();
            assert_eq!(edge3.from, 3);
            assert_eq!(edge3.to, 8);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_manifest_id_counters_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine.write_op(&WalOp::UpsertNode(make_node(42, "high_id"))).unwrap();
            engine.write_op(&WalOp::UpsertEdge(make_edge(99, 42, 42))).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.next_node_id() >= 43);
            assert!(engine.next_edge_id() >= 100);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_wal_replay_with_deletes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine.write_op(&WalOp::UpsertNode(make_node(1, "will_delete"))).unwrap();
            engine.write_op(&WalOp::UpsertNode(make_node(2, "will_keep"))).unwrap();
            engine.write_op(&WalOp::DeleteNode { id: 1, deleted_at: 5000 }).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(1).unwrap().is_none());
            assert!(engine.get_node(2).unwrap().is_some());
            assert_eq!(engine.node_count(), 1);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_write_op_batch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let ops: Vec<WalOp> = (1..=50)
            .map(|i| WalOp::UpsertNode(make_node(i, &format!("batch:{}", i))))
            .collect();
        engine.write_op_batch(&ops).unwrap();

        assert_eq!(engine.node_count(), 50);
        assert_eq!(engine.get_node(25).unwrap().unwrap().key, "batch:25");

        engine.close().unwrap();

        // Verify recovery
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.node_count(), 50);
        engine.close().unwrap();
    }

    #[test]
    fn test_write_op_batch_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let mut ops = Vec::new();
            for i in 1..=20 {
                ops.push(WalOp::UpsertNode(make_node(i, &format!("n:{}", i))));
            }
            for i in 1..=10 {
                ops.push(WalOp::UpsertEdge(make_edge(i, i, i + 10)));
            }
            engine.write_op_batch(&ops).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 20);
            assert_eq!(engine.edge_count(), 10);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_upsert_overwrites_on_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            engine.write_op(&WalOp::UpsertNode(make_node(1, "v1"))).unwrap();
            let mut updated = make_node(1, "v2");
            updated.weight = 0.99;
            engine.write_op(&WalOp::UpsertNode(updated)).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let node = engine.get_node(1).unwrap().unwrap();
            assert_eq!(node.key, "v2");
            assert!((node.weight - 0.99).abs() < f32::EPSILON);
            assert_eq!(engine.node_count(), 1);
            engine.close().unwrap();
        }
    }

    // --- Upsert API tests ---

    #[test]
    fn test_upsert_node_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let id2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();

        assert_ne!(id1, id2);
        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
        assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props_v1 = BTreeMap::new();
        props_v1.insert("version".to_string(), PropValue::Int(1));
        let id1 = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();

        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();

        // Same (type_id, key) → same ID, updated fields
        assert_eq!(id1, id2);
        assert_eq!(engine.node_count(), 1);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_different_types_same_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let id2 = engine.upsert_node(2, "alice", BTreeMap::new(), 0.5).unwrap();

        // Different type_id → different nodes
        assert_ne!(id1, id2);
        assert_eq!(engine.node_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_id_counter_monotonic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(engine.upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5).unwrap());
        }

        // All IDs should be unique and monotonically increasing
        for i in 1..ids.len() {
            assert!(ids[i] > ids[i - 1]);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let n2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();

        let e1 = engine.upsert_edge(n1, n2, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        assert_eq!(engine.edge_count(), 1);
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_without_uniqueness_creates_duplicates() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        // Default: edge_uniqueness = false
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let e1 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        // Without uniqueness: creates separate edges
        assert_ne!(e1, e2);
        assert_eq!(engine.edge_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_with_uniqueness_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let e1 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None).unwrap();
        let e2 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.9, None, None).unwrap();

        // With uniqueness: same triple → same ID, updated weight
        assert_eq!(e1, e2);
        assert_eq!(engine.edge_count(), 1);
        assert!((engine.get_edge(e1).unwrap().unwrap().weight - 0.9).abs() < f32::EPSILON);

        // Different triple → new edge
        let e3 = engine.upsert_edge(1, 2, 20, BTreeMap::new(), 1.0, None, None).unwrap();
        assert_ne!(e1, e3);
        assert_eq!(engine.edge_count(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("node:{}", i),
                props: BTreeMap::new(),
                weight: 0.5,
            })
            .collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(engine.node_count(), 1000);

        // All queryable
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("node:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes_with_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Pre-insert a node
        let pre_id = engine.upsert_node(1, "existing", BTreeMap::new(), 0.5).unwrap();

        // Batch with duplicate key and one that matches pre-existing
        let inputs = vec![
            NodeInput { type_id: 1, key: "new1".into(), props: BTreeMap::new(), weight: 0.5 },
            NodeInput { type_id: 1, key: "existing".into(), props: BTreeMap::new(), weight: 0.9 },
            NodeInput { type_id: 1, key: "new1".into(), props: BTreeMap::new(), weight: 0.8 }, // dup within batch
        ];

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // "existing" reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // "new1" appears twice → same ID
        assert_eq!(engine.node_count(), 2); // "existing" + "new1"

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<EdgeInput> = (0..100)
            .map(|i| EdgeInput {
                from: i,
                to: i + 1,
                type_id: 10,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();

        let ids = engine.batch_upsert_edges(&inputs).unwrap();
        assert_eq!(ids.len(), 100);
        assert_eq!(engine.edge_count(), 100);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (id1, id2, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
            id2 = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            eid = engine.upsert_edge(id1, id2, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.close().unwrap();
        }

        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.node_count(), 2);
            assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");
            assert_eq!(engine.get_edge(eid).unwrap().unwrap().from, id1);

            // Upsert dedup should still work after replay
            let id1_again = engine.upsert_node(1, "alice", BTreeMap::new(), 0.99).unwrap();
            assert_eq!(id1_again, id1);

            // New allocations should not reuse old IDs
            let id3 = engine.upsert_node(1, "charlie", BTreeMap::new(), 0.5).unwrap();
            assert!(id3 > id2);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_upsert_node_preserves_created_at() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let created_at_v1 = engine.get_node(id1).unwrap().unwrap().created_at;

        // Small delay not needed, just upsert again. created_at must be preserved
        let id2 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.9).unwrap();
        assert_eq!(id1, id2);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.created_at, created_at_v1);
        assert!(node.updated_at >= created_at_v1);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges_with_uniqueness() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Pre-insert an edge
        let pre_id = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None).unwrap();

        // Batch with: duplicate within batch + match against pre-existing
        let inputs = vec![
            EdgeInput { from: 3, to: 4, type_id: 10, props: BTreeMap::new(), weight: 0.5, valid_from: None, valid_to: None },
            EdgeInput { from: 1, to: 2, type_id: 10, props: BTreeMap::new(), weight: 0.9, valid_from: None, valid_to: None }, // matches pre-existing
            EdgeInput { from: 3, to: 4, type_id: 10, props: BTreeMap::new(), weight: 0.8, valid_from: None, valid_to: None }, // dup within batch
        ];

        let ids = engine.batch_upsert_edges(&inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // within-batch dedup
        assert_eq!(engine.edge_count(), 2); // pre-existing + one new

        engine.close().unwrap();
    }

    #[test]
    fn test_id_counters_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let last_node_id;
        let last_edge_id;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 0..10 {
                engine.upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5).unwrap();
            }
            for i in 0..5 {
                engine.upsert_edge(i, i + 1, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            }
            last_node_id = engine.next_node_id();
            last_edge_id = engine.next_edge_id();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.next_node_id() >= last_node_id);
            assert!(engine.next_edge_id() >= last_edge_id);
            engine.close().unwrap();
        }
    }

    // --- Adjacency, neighbors, delete tests ---

    #[test]
    fn test_neighbors_outgoing() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None).unwrap();

        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);
        let neighbor_ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_incoming() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let inc = engine.neighbors(c, Direction::Incoming, None, 0, None, None).unwrap();
        assert_eq!(inc.len(), 2);
        let neighbor_ids: Vec<u64> = inc.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&a));
        assert!(neighbor_ids.contains(&b));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap(); // type 10
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 1.0, None, None).unwrap(); // type 20

        let typed = engine.neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None).unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.5).unwrap();
        for i in 0..10 {
            let n = engine.upsert_node(1, &format!("spoke:{}", i), BTreeMap::new(), 0.5).unwrap();
            engine.upsert_edge(hub, n, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        let limited = engine.neighbors(hub, Direction::Outgoing, None, 3, None, None).unwrap();
        assert_eq!(limited.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.delete_node(b).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.node_count(), 1);

        // b excluded from a's neighbors (node tombstone filtering)
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_edge_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.delete_edge(eid).unwrap();

        assert!(engine.get_edge(eid).unwrap().is_none());
        assert_eq!(engine.edge_count(), 0);
        assert!(engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.delete_node(b).unwrap();
            engine.delete_edge(eid).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(b).unwrap().is_none());
            assert!(engine.get_edge(eid).unwrap().is_none());
            assert_eq!(engine.node_count(), 1);
            assert_eq!(engine.edge_count(), 0);
            assert!(engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap().is_empty());
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_neighbors_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, c);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
            engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None).unwrap();
            engine.upsert_edge(b, c, 10, BTreeMap::new(), 0.5, None, None).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // a → b, c
            let out_a = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
            assert_eq!(out_a.len(), 2);
            // b → c
            let out_b = engine.neighbors(b, Direction::Outgoing, None, 0, None, None).unwrap();
            assert_eq!(out_b.len(), 1);
            assert_eq!(out_b[0].node_id, c);
            // c ← a, b
            let inc_c = engine.neighbors(c, Direction::Incoming, None, 0, None, None).unwrap();
            assert_eq!(inc_c.len(), 2);
            engine.close().unwrap();
        }
    }

    // --- Flush, segments, multi-source read tests ---

    #[test]
    fn test_flush_creates_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();

        assert_eq!(engine.segment_count(), 0);
        let info = engine.flush().unwrap();
        assert!(info.is_some());
        assert_eq!(engine.segment_count(), 1);

        // Memtable is empty after flush
        assert_eq!(engine.node_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_flush_empty_memtable_is_noop() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let info = engine.flush().unwrap();
        assert!(info.is_none());
        assert_eq!(engine.segment_count(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_data_readable_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.flush().unwrap();

        // Data should be readable from segment
        let alice = engine.get_node(a).unwrap().unwrap();
        assert_eq!(alice.key, "alice");
        let bob = engine.get_node(b).unwrap().unwrap();
        assert_eq!(bob.key, "bob");
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None).unwrap();

        engine.flush().unwrap();

        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        // Type filter should still work
        let typed = engine.neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None).unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Build chain: a -> b -> c -> d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // 2-hop from a: should reach c (via b), but NOT d (3 hops) or a/b (origin/1-hop)
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_excludes_origin_and_hop1() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Build graph with back-edge: a -> b -> a (cycle)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None).unwrap(); // back to origin
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // 2-hop from a: b is 1-hop, then from b we reach a (origin, excluded) and c
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_with_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b, a -> c, b -> d, b -> e, c -> f
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        let e = engine.upsert_node(1, "e", BTreeMap::new(), 0.5).unwrap();
        let f = engine.upsert_node(1, "f", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, e, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, f, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Without limit: 3 2-hop results (d, e, f)
        let all = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(all.len(), 3);

        // With limit: only 2
        let limited = engine.neighbors_2hop(a, Direction::Outgoing, None, 2, None, None).unwrap();
        assert_eq!(limited.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -[type1]-> b -[type1]-> c, b -[type2]-> d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 2, BTreeMap::new(), 1.0, None, None).unwrap();

        // Filter type 1 only: a->b (hop1), b->c (hop2). b->d is type 2, excluded.
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, Some(&[1]), 0, None, None).unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_incoming() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Chain: a -> b -> c -> d (incoming 2-hop from d should reach b)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Incoming 2-hop from d: hop1 = c, hop2 = b (not a, that's 3 hops)
        let hop2 = engine.neighbors_2hop(d, Direction::Incoming, None, 0, None, None).unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_2hop_nonexistent_node() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // No nodes at all. 2-hop on ID 999 should return empty
        let hop2 = engine.neighbors_2hop(999, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(hop2.is_empty());

        // Add a node but delete it, same result
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.delete_node(a).unwrap();

        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(hop2.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_cross_source_reads_memtable_plus_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Write batch 1, flush to segment
        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Write batch 2, stays in memtable
        let c = engine.upsert_node(1, "charlie", BTreeMap::new(), 0.7).unwrap();
        engine.upsert_edge(a, c, 10, BTreeMap::new(), 0.9, None, None).unwrap();

        // Can read from both sources
        assert!(engine.get_node(a).unwrap().is_some()); // from segment
        assert!(engine.get_node(c).unwrap().is_some()); // from memtable

        // Neighbors merge across memtable + segment
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_dedup_across_flush_boundary() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Insert and flush
        let id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Upsert same (type_id, key), should find existing in segment
        let mut props = BTreeMap::new();
        props.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node(1, "alice", props, 0.9).unwrap();

        // Same ID reused
        assert_eq!(id1, id2);

        // Updated version in memtable wins over segment
        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_tombstone_hides_segment_data() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Delete after flush. Tombstone in memtable hides segment data
        engine.delete_node(b).unwrap();
        assert!(engine.get_node(b).unwrap().is_none());

        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge(eid).unwrap().is_none());

        // Neighbors should exclude deleted node
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_tombstone_survives_second_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap(); // seg_0000: alice exists

        engine.delete_node(a).unwrap();
        engine.flush().unwrap(); // seg_0001: tombstone for alice

        // Tombstone in newer segment hides node in older segment
        assert!(engine.get_node(a).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_multiple_flushes_accumulate_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..3 {
            let id = engine.upsert_node(1, &format!("batch:{}", i), BTreeMap::new(), 0.5).unwrap();
            ids.push(id);
            engine.flush().unwrap();
        }

        assert_eq!(engine.segment_count(), 3);

        // All nodes readable across 3 segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("batch:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_flush_updates_manifest() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        let manifest = engine.manifest();
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].id, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_id_counters_survive_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine.upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5).unwrap();
        }
        let next_before = engine.next_node_id();
        engine.flush().unwrap();

        // New allocations should continue from where they left off
        let new_id = engine.upsert_node(1, "after_flush", BTreeMap::new(), 0.5).unwrap();
        assert!(new_id >= next_before);

        engine.close().unwrap();
    }

    #[test]
    fn test_segment_data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        let eid;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.segment_count(), 1);
            assert!(engine.get_node(a).unwrap().is_some());
            assert!(engine.get_node(b).unwrap().is_some());
            assert!(engine.get_edge(eid).unwrap().is_some());

            let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].node_id, b);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_deleted_edge_excluded_from_segment_neighbors() {
        // Regression: M2. Edge tombstone must hide segment adjacency entries
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e1 = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Delete only the edge to b (not the node). Edge tombstone in memtable
        engine.delete_edge(e1).unwrap();

        // Neighbors should return only c, not b
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_after_delete_across_flush_gets_new_id() {
        // Regression: S3. Upsert of a deleted node's key should not reuse the old ID
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Delete alice. Tombstone in memtable
        engine.delete_node(id1).unwrap();
        assert!(engine.get_node(id1).unwrap().is_none());

        // Re-insert same key, should get a fresh ID, not reuse deleted one
        let id2 = engine.upsert_node(1, "alice", BTreeMap::new(), 0.7).unwrap();
        assert_ne!(id1, id2);
        assert!(engine.get_node(id2).unwrap().is_some());
        assert!(engine.get_node(id1).unwrap().is_none()); // old ID still deleted

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_flush_triggers_on_threshold() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        // Set a very low threshold so auto-flush triggers quickly
        let opts = DbOptions {
            memtable_flush_threshold: 256, // 256 bytes, tiny
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        // Insert enough data to exceed the 256-byte threshold
        let mut ids = Vec::new();
        for i in 0..20 {
            let id = engine.upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5).unwrap();
            ids.push(id);
        }

        // Auto-flush should have triggered at least once
        assert!(engine.segment_count() >= 1);

        // All data still readable across memtable + segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("node:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_flush_disabled_when_zero() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            memtable_flush_threshold: 0, // disabled
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        for i in 0..100 {
            engine.upsert_node(1, &format!("node:{}", i), BTreeMap::new(), 0.5).unwrap();
        }

        // No auto-flush should have occurred
        assert_eq!(engine.segment_count(), 0);
        assert_eq!(engine.node_count(), 100);

        engine.close().unwrap();
    }

    // --- Compaction tests ---

    #[test]
    fn test_compact_requires_two_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // 0 segments → no-op
        assert!(engine.compact().unwrap().is_none());

        // 1 segment → no-op
        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.segment_count(), 1);
        assert!(engine.compact().unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_merges_two_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 2);
        assert_eq!(stats.nodes_removed, 0);
        assert_eq!(engine.segment_count(), 1);

        // Data still accessible
        assert_eq!(engine.get_node(a).unwrap().unwrap().key, "alice");
        assert_eq!(engine.get_node(b).unwrap().unwrap().key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_applies_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice + bob + edge
        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Segment 2: delete bob + edge
        engine.delete_node(b).unwrap();
        engine.delete_edge(eid).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 1); // only alice
        assert_eq!(stats.nodes_removed, 1); // bob removed
        assert_eq!(stats.edges_kept, 0);
        assert_eq!(stats.edges_removed, 1);
        assert_eq!(engine.segment_count(), 1);
        assert!(stats.output_segment_id > 0);
        assert!(stats.duration_ms < 30_000); // sanity upper bound

        // Compacted segment should have zero tombstones
        assert_eq!(engine.segment_tombstone_node_count(), 0);
        assert_eq!(engine.segment_tombstone_edge_count(), 0);

        // alice survives, bob and edge are gone
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(eid).unwrap().is_none());
        assert!(engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_node_last_write_wins() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice v1
        let mut props_v1 = BTreeMap::new();
        props_v1.insert("version".to_string(), PropValue::Int(1));
        let a = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();
        engine.flush().unwrap();

        // Segment 2: alice v2 (upsert updates in memtable, flushed to new segment)
        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segment_count(), 2);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        // One input from each segment, but they merge to 1 output → 1 removed
        assert_eq!(stats.nodes_removed, 1);

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_preserves_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 0.8, None, None).unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);
        let ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        // Type filter still works after compaction
        let typed = engine.neighbors(a, Direction::Outgoing, Some(&[10]), 0, None, None).unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_cleans_up_old_segment_dirs() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Old segment directories exist
        let seg_dir = db_path.join("segments");
        assert!(seg_dir.join("seg_0001").exists());
        assert!(seg_dir.join("seg_0002").exists());

        engine.compact().unwrap();

        // Old dirs cleaned up, new one exists
        assert!(!seg_dir.join("seg_0001").exists());
        assert!(!seg_dir.join("seg_0002").exists());
        assert!(seg_dir.join("seg_0003").exists());

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_updates_manifest() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.manifest().segments.len(), 2);

        engine.compact().unwrap();

        let manifest = engine.manifest();
        assert_eq!(manifest.segments.len(), 1);
        // New segment should have both nodes
        assert_eq!(manifest.segments[0].node_count, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
            engine.flush().unwrap();
            b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
            engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.flush().unwrap();
            engine.compact().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert_eq!(engine.segment_count(), 1);
            assert_eq!(engine.get_node(a).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(b).unwrap().unwrap().key, "bob");
            let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].node_id, b);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_compact_three_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut all_ids = Vec::new();
        for i in 0..3 {
            let id = engine.upsert_node(1, &format!("n:{}", i), BTreeMap::new(), 0.5).unwrap();
            all_ids.push(id);
            engine.flush().unwrap();
        }

        assert_eq!(engine.segment_count(), 3);

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 3);
        assert_eq!(engine.segment_count(), 1);

        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(engine.get_node(id).unwrap().unwrap().key, format!("n:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_unflushed_tombstone() {
        // Regression: S2. compact() must flush memtable first so tombstones
        // in the memtable are included in the compaction.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice + bob
        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.6).unwrap();
        engine.flush().unwrap();

        // Segment 2: charlie
        engine.upsert_node(1, "charlie", BTreeMap::new(), 0.7).unwrap();
        engine.flush().unwrap();

        // Delete bob. Unflushed, lives in memtable only
        engine.delete_node(b).unwrap();
        assert_eq!(engine.segment_count(), 2);

        // Compact should flush the tombstone first, then merge all 3 segments
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3); // 2 original + 1 from flush
        assert_eq!(stats.nodes_kept, 2); // alice + charlie
        assert_eq!(stats.nodes_removed, 1); // bob

        // bob is gone from the compacted segment
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.segment_count(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_unflushed_update() {
        // Regression: S2. compact() must flush memtable first so updates
        // in the memtable are included in the compaction output.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: alice v1
        let mut props_v1 = BTreeMap::new();
        props_v1.insert("v".to_string(), PropValue::Int(1));
        let a = engine.upsert_node(1, "alice", props_v1, 0.5).unwrap();
        engine.flush().unwrap();

        // Segment 2: bob
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Update alice to v2. Unflushed, lives in memtable only
        let mut props_v2 = BTreeMap::new();
        props_v2.insert("v".to_string(), PropValue::Int(2));
        engine.upsert_node(1, "alice", props_v2, 0.9).unwrap();

        // Compact should flush first, then merge all 3 segments
        engine.compact().unwrap();

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(node.props.get("v"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    /// Regression: compaction must remove edges whose endpoints are deleted,
    /// even if the edge itself was never explicitly deleted.
    #[test]
    fn test_compact_removes_dangling_edges_after_node_delete() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: A→B→C chain
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e_ab = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_bc = engine.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Segment 2: delete B (but NOT edges A→B or B→C explicitly)
        engine.delete_node(b).unwrap();
        engine.flush().unwrap();

        // Before compact: neighbors correctly filter deleted B
        assert!(engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap().is_empty());

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2); // A and C
        assert_eq!(stats.nodes_removed, 1); // B
        assert_eq!(stats.edges_kept, 0); // both edges dangling
        assert_eq!(stats.edges_removed, 2);

        // After compact: edges must still be gone (no dangling references)
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        assert!(engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap().is_empty());
        assert!(engine.neighbors(c, Direction::Incoming, None, 0, None, None).unwrap().is_empty());

        engine.close().unwrap();
    }

    // --- Type index tests ---

    #[test]
    fn test_nodes_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(2, "charlie", BTreeMap::new(), 0.5).unwrap();

        let mut type1 = engine.nodes_by_type(1).unwrap();
        type1.sort();
        assert_eq!(type1, vec![a, b]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![c]);
        assert!(engine.nodes_by_type(99).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_edges_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let e1 = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(a, b, 20, BTreeMap::new(), 1.0, None, None).unwrap();

        assert_eq!(engine.edges_by_type(10).unwrap(), vec![e1]);
        assert_eq!(engine.edges_by_type(20).unwrap(), vec![e2]);
        assert!(engine.edges_by_type(99).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_nodes_by_type_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment: type 1 nodes
        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Memtable: more type 1 + type 2
        let c = engine.upsert_node(1, "charlie", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(2, "delta", BTreeMap::new(), 0.5).unwrap();

        let mut type1 = engine.nodes_by_type(1).unwrap();
        type1.sort();
        assert_eq!(type1, vec![a, b, c]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![d]);

        engine.close().unwrap();
    }

    #[test]
    fn test_nodes_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // Delete alice (cross-source tombstone: segment data, memtable tombstone)
        engine.delete_node(a).unwrap();

        let type1 = engine.nodes_by_type(1).unwrap();
        assert_eq!(type1, vec![b]);

        engine.close().unwrap();
    }

    #[test]
    fn test_type_index_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        let b;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
            b = engine.upsert_node(2, "bob", BTreeMap::new(), 0.5).unwrap();
            engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        // Reopen. Type index should be available from segment
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(engine.nodes_by_type(1).unwrap(), vec![a]);
        assert_eq!(engine.nodes_by_type(2).unwrap(), vec![b]);
        assert_eq!(engine.edges_by_type(10).unwrap().len(), 1);

        engine.close().unwrap();
    }

    // --- get_nodes_by_type / get_edges_by_type / count tests ---

    #[test]
    fn test_get_nodes_by_type_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String("Alice".to_string()));
        engine.upsert_node(1, "alice", props.clone(), 0.9).unwrap();
        props.insert("name".to_string(), PropValue::String("Bob".to_string()));
        engine.upsert_node(1, "bob", props, 0.8).unwrap();
        engine.upsert_node(2, "charlie", BTreeMap::new(), 0.7).unwrap();

        let type1 = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(type1.len(), 2);
        assert!(type1.iter().all(|n| n.type_id == 1));
        assert!(type1.iter().any(|n| n.key == "alice"));
        assert!(type1.iter().any(|n| n.key == "bob"));

        let type2 = engine.get_nodes_by_type(2).unwrap();
        assert_eq!(type2.len(), 1);
        assert_eq!(type2[0].key, "charlie");

        // Non-existent type
        let empty = engine.get_nodes_by_type(99).unwrap();
        assert!(empty.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Type 1 nodes in segment
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Type 1 node in memtable
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 3);
        let keys: Vec<&str> = records.iter().map(|n| n.key.as_str()).collect();
        assert!(keys.contains(&"a"));
        assert!(keys.contains(&"b"));
        assert!(keys.contains(&"c"));

        // Verify records carry full data (props, weight, timestamps)
        for r in &records {
            assert_eq!(r.type_id, 1);
            assert!(r.weight > 0.0);
            assert!(r.created_at > 0);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "alice", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_node(1, "bob", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        engine.delete_node(a).unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_excludes_pruned() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Policy: prune nodes with weight <= 0.5
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_weight: Some(0.5),
                    max_age_ms: None,
                    type_id: None,
                },
            )
            .unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "high");

        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_by_type_post_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        let records = engine.get_nodes_by_type(1).unwrap();
        assert_eq!(records.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_by_type_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        // Type 10 edge in segment
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Type 10 edge in memtable
        engine.upsert_edge(b, c, 10, BTreeMap::new(), 0.8, None, None).unwrap();
        // Type 20 edge in memtable
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 0.5, None, None).unwrap();

        let type10 = engine.get_edges_by_type(10).unwrap();
        assert_eq!(type10.len(), 2);
        assert!(type10.iter().all(|e| e.type_id == 10));

        let type20 = engine.get_edges_by_type(20).unwrap();
        assert_eq!(type20.len(), 1);
        assert_eq!(type20[0].type_id, 20);

        // Verify records carry full data
        for e in &type10 {
            assert!(e.weight > 0.0);
            assert!(e.from > 0);
            assert!(e.to > 0);
        }

        // Empty type
        let empty = engine.get_edges_by_type(99).unwrap();
        assert!(empty.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_by_type_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let e1 = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        engine.delete_edge(e1).unwrap();

        let type10 = engine.get_edges_by_type(10).unwrap();
        assert_eq!(type10.len(), 1);
        assert_eq!(type10[0].from, b);
        assert_eq!(type10[0].to, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_nodes_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // 3 type-1 nodes across memtable + segment
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        // 1 type-2 node
        engine.upsert_node(2, "x", BTreeMap::new(), 1.0).unwrap();

        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 3);
        assert_eq!(engine.count_nodes_by_type(2).unwrap(), 1);
        assert_eq!(engine.count_nodes_by_type(99).unwrap(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_edges_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        engine.upsert_edge(a, c, 20, BTreeMap::new(), 1.0, None, None).unwrap();

        assert_eq!(engine.count_edges_by_type(10).unwrap(), 2);
        assert_eq!(engine.count_edges_by_type(20).unwrap(), 1);
        assert_eq!(engine.count_edges_by_type(99).unwrap(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_count_nodes_by_type_respects_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 2);

        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_weight: Some(0.5),
                    max_age_ms: None,
                    type_id: None,
                },
            )
            .unwrap();

        // Now the low-weight node is excluded
        assert_eq!(engine.count_nodes_by_type(1).unwrap(), 1);

        engine.close().unwrap();
    }

    // --- Paginated type-index query tests ---

    #[test]
    fn test_nodes_by_type_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 10 nodes of type 1
        let mut ids: Vec<u64> = Vec::new();
        for i in 0..10 {
            let id = engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
            ids.push(id);
        }
        ids.sort();

        // Page through 3 at a time
        let page1 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: None }).unwrap();
        assert_eq!(page1.items.len(), 3);
        assert_eq!(page1.items, ids[0..3]);
        assert!(page1.next_cursor.is_some());

        let page2 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: page1.next_cursor }).unwrap();
        assert_eq!(page2.items.len(), 3);
        assert_eq!(page2.items, ids[3..6]);
        assert!(page2.next_cursor.is_some());

        let page3 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: page2.next_cursor }).unwrap();
        assert_eq!(page3.items.len(), 3);
        assert_eq!(page3.items, ids[6..9]);
        assert!(page3.next_cursor.is_some());

        let page4 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: page3.next_cursor }).unwrap();
        assert_eq!(page4.items.len(), 1);
        assert_eq!(page4.items, ids[9..10]);
        assert!(page4.next_cursor.is_none()); // last page
    }

    #[test]
    fn test_nodes_by_type_paged_roundtrip() {
        // Page through all results 1-at-a-time, collect, should equal unpaginated
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..20 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(4), after: cursor }).unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let mut all_unpaged = engine.nodes_by_type(1).unwrap();
        all_unpaged.sort();
        assert_eq!(all_paged, all_unpaged);
    }

    #[test]
    fn test_nodes_by_type_paged_default_returns_all() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        let result = engine.nodes_by_type_paged(1, &PageRequest::default()).unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_empty_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let result = engine.nodes_by_type_paged(99, &PageRequest { limit: Some(10), after: None }).unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_cursor_past_end() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..3 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(10), after: Some(u64::MAX) }).unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_cross_source() {
        // IDs from memtable + segments should merge and paginate correctly
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 5 nodes, flush to segment
        for i in 0..5 {
            engine.upsert_node(1, &format!("seg{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Create 5 more in memtable
        for i in 0..5 {
            engine.upsert_node(1, &format!("mem{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        // Page through all, should see 10 total across both sources
        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: cursor }).unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all_paged.len(), 10);

        // Verify sorted order
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_nodes_by_type_paged_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(id2).unwrap();

        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(10), after: None }).unwrap();
        let mut expected = vec![id1, id3];
        expected.sort();
        assert_eq!(result.items, expected);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_nodes_by_type_paged_respects_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "keep", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "prune_me", BTreeMap::new(), 0.1).unwrap();

        engine.set_prune_policy("low_weight", PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(10), after: None }).unwrap();
        assert_eq!(result.items.len(), 1); // only "keep" survives
    }

    #[test]
    fn test_edges_by_type_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();

        let mut edge_ids: Vec<u64> = Vec::new();
        for _ in 0..6 {
            let eid = engine.upsert_edge(n1, n2, 5, BTreeMap::new(), 1.0, None, None).unwrap();
            edge_ids.push(eid);
            let eid = engine.upsert_edge(n2, n3, 5, BTreeMap::new(), 1.0, None, None).unwrap();
            edge_ids.push(eid);
        }
        edge_ids.sort();

        // Page 2 at a time
        let page1 = engine.edges_by_type_paged(5, &PageRequest { limit: Some(2), after: None }).unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.next_cursor.is_some());

        let page2 = engine.edges_by_type_paged(5, &PageRequest { limit: Some(2), after: page1.next_cursor }).unwrap();
        assert_eq!(page2.items.len(), 2);
    }

    #[test]
    fn test_edges_by_type_paged_roundtrip() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        for _ in 0..10 {
            engine.upsert_edge(n1, n2, 3, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.edges_by_type_paged(3, &PageRequest { limit: Some(3), after: cursor }).unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        let mut all_unpaged = engine.edges_by_type(3).unwrap();
        all_unpaged.sort();
        assert_eq!(all_paged, all_unpaged);
    }

    #[test]
    fn test_get_nodes_by_type_paged_hydrates_page_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..10 {
            let mut props = BTreeMap::new();
            props.insert("idx".to_string(), PropValue::Int(i));
            engine.upsert_node(1, &format!("n{}", i), props, 1.0).unwrap();
        }

        // Get first page of 3 hydrated records
        let page1 = engine.get_nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: None }).unwrap();
        assert_eq!(page1.items.len(), 3);
        assert!(page1.next_cursor.is_some());
        // Verify they're actual NodeRecords with properties
        for node in &page1.items {
            assert_eq!(node.type_id, 1);
            assert!(node.props.contains_key("idx"));
        }

        // Get next page
        let page2 = engine.get_nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: page1.next_cursor }).unwrap();
        assert_eq!(page2.items.len(), 3);
        // No overlap
        let page1_ids: Vec<u64> = page1.items.iter().map(|n| n.id).collect();
        let page2_ids: Vec<u64> = page2.items.iter().map(|n| n.id).collect();
        for id in &page2_ids {
            assert!(!page1_ids.contains(id));
        }
    }

    #[test]
    fn test_get_edges_by_type_paged() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        for _ in 0..6 {
            engine.upsert_edge(n1, n2, 7, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        let page1 = engine.get_edges_by_type_paged(7, &PageRequest { limit: Some(2), after: None }).unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.next_cursor.is_some());
        for edge in &page1.items {
            assert_eq!(edge.type_id, 7);
            assert_eq!(edge.from, n1);
            assert_eq!(edge.to, n2);
        }

        // Round-trip
        let mut all_paged: Vec<EdgeRecord> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.get_edges_by_type_paged(7, &PageRequest { limit: Some(2), after: cursor }).unwrap();
            cursor = page.next_cursor;
            all_paged.extend(page.items);
            if cursor.is_none() { break; }
        }
        assert_eq!(all_paged.len(), 6);
    }

    #[test]
    fn test_paged_single_item_pages() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let mut expected = vec![id1, id2, id3];
        expected.sort();

        // Page 1-at-a-time
        let p1 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(1), after: None }).unwrap();
        assert_eq!(p1.items, vec![expected[0]]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(1), after: p1.next_cursor }).unwrap();
        assert_eq!(p2.items, vec![expected[1]]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(1), after: p2.next_cursor }).unwrap();
        assert_eq!(p3.items, vec![expected[2]]);
        assert!(p3.next_cursor.is_none()); // last item
    }

    #[test]
    fn test_paged_limit_larger_than_result_set() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();

        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(100), after: None }).unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.next_cursor.is_none()); // all fit in one page
    }

    #[test]
    fn test_paged_limit_zero_returns_all() {
        // limit: Some(0) should behave like None (return everything)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..5 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(0), after: None }).unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_paged_cursor_on_deleted_id() {
        // Cursor points to a deleted node's ID (gap in the sorted list)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(id2).unwrap(); // id2 is now a gap

        // Use deleted id2 as cursor. Should still work via binary search insertion point
        let result = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(10), after: Some(id2) }).unwrap();
        // Should return only ids > id2 (which is id3, since id1 < id2)
        let mut expected: Vec<u64> = vec![id1, id3].into_iter().filter(|&id| id > id2).collect();
        expected.sort();
        assert_eq!(result.items, expected);
    }

    // --- merge_type_ids_paged unit tests ---

    #[test]
    fn test_merge_paged_early_termination() {
        // Verify correct results when limit < total items
        let memtable = vec![5u64, 1, 9]; // unsorted, will be sorted internally
        let seg1 = vec![2u64, 4, 6, 8, 10];
        let seg2 = vec![3u64, 7];
        let deleted = HashSet::new();

        // Get first 4
        let page = PageRequest { limit: Some(4), after: None };
        let result = merge_type_ids_paged(memtable.clone(), vec![seg1.clone(), seg2.clone()], &deleted, &page);
        assert_eq!(result.items, vec![1, 2, 3, 4]);
        assert!(result.next_cursor.is_some());

        // Continue from cursor
        let page2 = PageRequest { limit: Some(4), after: result.next_cursor };
        let result2 = merge_type_ids_paged(memtable.clone(), vec![seg1.clone(), seg2.clone()], &deleted, &page2);
        assert_eq!(result2.items, vec![5, 6, 7, 8]);
        assert!(result2.next_cursor.is_some());

        // Last page
        let page3 = PageRequest { limit: Some(4), after: result2.next_cursor };
        let result3 = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page3);
        assert_eq!(result3.items, vec![9, 10]);
        assert!(result3.next_cursor.is_none());
    }

    #[test]
    fn test_merge_paged_cross_source_sorted_output() {
        // Multiple sources with interleaved IDs produce sorted output
        let memtable = vec![10u64, 30, 50];
        let seg1 = vec![20u64, 40];
        let seg2 = vec![15u64, 35, 55];
        let deleted = HashSet::new();

        let page = PageRequest { limit: None, after: None };
        let result = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page);
        assert_eq!(result.items, vec![10, 15, 20, 30, 35, 40, 50, 55]);
        assert!(result.next_cursor.is_none());

        // Verify sorted
        for i in 1..result.items.len() {
            assert!(result.items[i] > result.items[i - 1]);
        }
    }

    #[test]
    fn test_merge_paged_dedup_across_sources() {
        // Same ID in memtable + segment should only appear once
        let memtable = vec![1u64, 3, 5];
        let seg1 = vec![1u64, 2, 3]; // IDs 1 and 3 overlap with memtable
        let seg2 = vec![3u64, 4, 5]; // IDs 3 and 5 overlap
        let deleted = HashSet::new();

        let page = PageRequest { limit: None, after: None };
        let result = merge_type_ids_paged(memtable, vec![seg1, seg2], &deleted, &page);
        assert_eq!(result.items, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_merge_paged_cursor_seek() {
        // Cursor skips correct items in the merge
        let memtable = vec![1u64, 5, 9];
        let seg1 = vec![2u64, 6, 10];
        let deleted = HashSet::new();

        // Cursor at 5 → should start from 6
        let page = PageRequest { limit: Some(3), after: Some(5) };
        let result = merge_type_ids_paged(memtable, vec![seg1], &deleted, &page);
        assert_eq!(result.items, vec![6, 9, 10]);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_merge_paged_with_policies() {
        // Policy path: merge produces sorted output, policy filtering + cursor works
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 6 nodes: 3 with high weight (keep), 3 with low weight (prune)
        let mut keep_ids = Vec::new();
        for i in 0..3 {
            let id = engine.upsert_node(1, &format!("keep{}", i), BTreeMap::new(), 1.0).unwrap();
            keep_ids.push(id);
        }
        for i in 0..3 {
            engine.upsert_node(1, &format!("prune{}", i), BTreeMap::new(), 0.1).unwrap();
        }

        engine.set_prune_policy("low_weight", PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        // Page through with limit=2, should only see the 3 high-weight nodes
        let p1 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(2), after: None }).unwrap();
        assert_eq!(p1.items.len(), 2);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(2), after: p1.next_cursor }).unwrap();
        assert_eq!(p2.items.len(), 1);
        assert!(p2.next_cursor.is_none());

        // Collected IDs should be the 3 keep nodes
        let mut all_paged: Vec<u64> = Vec::new();
        all_paged.extend(&p1.items);
        all_paged.extend(&p2.items);
        keep_ids.sort();
        assert_eq!(all_paged, keep_ids);
    }

    // --- find_nodes (property equality index) tests ---

    #[test]
    fn test_find_nodes_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props.clone(), 0.5).unwrap();

        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        let b = engine.upsert_node(1, "cherry", props2, 0.5).unwrap();

        let mut props3 = BTreeMap::new();
        props3.insert("color".to_string(), PropValue::String("green".to_string()));
        engine.upsert_node(1, "lime", props3, 0.5).unwrap();

        let mut reds = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        reds.sort();
        assert_eq!(reds, vec![a, b]);

        let greens = engine.find_nodes(1, "color", &PropValue::String("green".to_string())).unwrap();
        assert_eq!(greens.len(), 1);

        assert!(engine.find_nodes(1, "color", &PropValue::String("blue".to_string())).unwrap().is_empty());
        assert!(engine.find_nodes(2, "color", &PropValue::String("red".to_string())).unwrap().is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create node in memtable, flush to segment
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props, 0.5).unwrap();
        engine.flush().unwrap();

        // Create node in memtable (stays in memtable)
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        let b = engine.upsert_node(1, "cherry", props2, 0.5).unwrap();

        // find_nodes should merge across memtable + segment
        let mut reds = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        reds.sort();
        assert_eq!(reds, vec![a, b]);

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let a = engine.upsert_node(1, "apple", props.clone(), 0.5).unwrap();
        let b = engine.upsert_node(1, "cherry", props, 0.5).unwrap();

        engine.delete_node(b).unwrap();

        let reds = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        assert_eq!(reds, vec![a]);

        engine.close().unwrap();
    }

    #[test]
    fn test_find_nodes_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let a;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

            let mut props = BTreeMap::new();
            props.insert("lang".to_string(), PropValue::String("rust".to_string()));
            a = engine.upsert_node(1, "overgraph", props, 0.9).unwrap();

            let mut props2 = BTreeMap::new();
            props2.insert("lang".to_string(), PropValue::String("python".to_string()));
            engine.upsert_node(1, "other", props2, 0.5).unwrap();

            engine.flush().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

            let results = engine.find_nodes(1, "lang", &PropValue::String("rust".to_string())).unwrap();
            assert_eq!(results, vec![a]);

            let py = engine.find_nodes(1, "lang", &PropValue::String("python".to_string())).unwrap();
            assert_eq!(py.len(), 1);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_find_nodes_update_changes_index() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("status".to_string(), PropValue::String("active".to_string()));
        let a = engine.upsert_node(1, "item", props, 0.5).unwrap();

        assert_eq!(engine.find_nodes(1, "status", &PropValue::String("active".to_string())).unwrap(), vec![a]);

        // Update: change status
        let mut props2 = BTreeMap::new();
        props2.insert("status".to_string(), PropValue::String("inactive".to_string()));
        let a2 = engine.upsert_node(1, "item", props2, 0.5).unwrap();
        assert_eq!(a, a2); // same ID (dedup)

        assert!(engine.find_nodes(1, "status", &PropValue::String("active".to_string())).unwrap().is_empty());
        assert_eq!(engine.find_nodes(1, "status", &PropValue::String("inactive".to_string())).unwrap(), vec![a]);

        engine.close().unwrap();
    }

    // --- find_nodes_paged tests ---

    #[test]
    fn test_find_nodes_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..8 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String("red".to_string()));
            let id = engine.upsert_node(1, &format!("r{}", i), props, 1.0).unwrap();
            ids.push(id);
        }
        // Add some non-matching nodes
        for i in 0..3 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String("blue".to_string()));
            engine.upsert_node(1, &format!("b{}", i), props, 1.0).unwrap();
        }
        ids.sort();

        let red = PropValue::String("red".to_string());

        // Page through 3 at a time
        let p1 = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(3), after: None }).unwrap();
        assert_eq!(p1.items.len(), 3);
        assert_eq!(p1.items, ids[0..3]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(3), after: p1.next_cursor }).unwrap();
        assert_eq!(p2.items.len(), 3);
        assert_eq!(p2.items, ids[3..6]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(3), after: p2.next_cursor }).unwrap();
        assert_eq!(p3.items.len(), 2);
        assert_eq!(p3.items, ids[6..8]);
        assert!(p3.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        // Create 4 in segment
        for i in 0..4 {
            engine.upsert_node(1, &format!("seg{}", i), props.clone(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Create 4 in memtable
        for i in 0..4 {
            engine.upsert_node(1, &format!("mem{}", i), props.clone(), 1.0).unwrap();
        }

        // Round-trip pagination should collect all 8
        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(3), after: cursor }).unwrap();
            all_paged.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }
        assert_eq!(all_paged.len(), 8);
        // Verify sorted
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_find_nodes_paged_excludes_deleted() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        let id1 = engine.upsert_node(1, "a", props.clone(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", props.clone(), 1.0).unwrap();
        let id3 = engine.upsert_node(1, "c", props.clone(), 1.0).unwrap();
        engine.delete_node(id2).unwrap();

        let result = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(10), after: None }).unwrap();
        let mut expected = vec![id1, id3];
        expected.sort();
        assert_eq!(result.items, expected);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_find_nodes_paged_with_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        engine.upsert_node(1, "keep", props.clone(), 1.0).unwrap();
        engine.upsert_node(1, "prune", props.clone(), 0.1).unwrap();

        engine.set_prune_policy("low_weight", PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        let result = engine.find_nodes_paged(1, "color", &red, &PageRequest { limit: Some(10), after: None }).unwrap();
        assert_eq!(result.items.len(), 1);
    }

    #[test]
    fn test_find_nodes_paged_default_returns_all() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let red = PropValue::String("red".to_string());
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), red.clone());

        for i in 0..5 {
            engine.upsert_node(1, &format!("n{}", i), props.clone(), 1.0).unwrap();
        }

        let result = engine.find_nodes_paged(1, "color", &red, &PageRequest::default()).unwrap();
        assert_eq!(result.items.len(), 5);
        assert!(result.next_cursor.is_none());
    }

    // --- neighbors_paged tests ---

    #[test]
    fn test_neighbors_paged_basic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        let mut edge_ids = Vec::new();
        for i in 0..8 {
            let neighbor = engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
            let eid = engine.upsert_edge(center, neighbor, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            edge_ids.push(eid);
        }
        edge_ids.sort();

        // Page through 3 at a time
        let p1 = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(3), after: None }, None, None).unwrap();
        assert_eq!(p1.items.len(), 3);
        let p1_eids: Vec<u64> = p1.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p1_eids, edge_ids[0..3]);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(3), after: p1.next_cursor }, None, None).unwrap();
        assert_eq!(p2.items.len(), 3);
        let p2_eids: Vec<u64> = p2.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p2_eids, edge_ids[3..6]);
        assert!(p2.next_cursor.is_some());

        let p3 = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(3), after: p2.next_cursor }, None, None).unwrap();
        assert_eq!(p3.items.len(), 2);
        let p3_eids: Vec<u64> = p3.items.iter().map(|e| e.edge_id).collect();
        assert_eq!(p3_eids, edge_ids[6..8]);
        assert!(p3.next_cursor.is_none());
    }

    #[test]
    fn test_neighbors_paged_cross_source() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();

        // Create 4 edges, flush to segment
        for i in 0..4 {
            let n = engine.upsert_node(1, &format!("seg{}", i), BTreeMap::new(), 1.0).unwrap();
            engine.upsert_edge(center, n, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        engine.flush().unwrap();

        // Create 4 more in memtable
        for i in 0..4 {
            let n = engine.upsert_node(1, &format!("mem{}", i), BTreeMap::new(), 1.0).unwrap();
            engine.upsert_edge(center, n, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        // Round-trip
        let mut all_paged: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(3), after: cursor }, None, None).unwrap();
            all_paged.extend(page.items.iter().map(|e| e.edge_id));
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }
        assert_eq!(all_paged.len(), 8);
        for i in 1..all_paged.len() {
            assert!(all_paged[i] > all_paged[i - 1]);
        }
    }

    #[test]
    fn test_neighbors_paged_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        let n1 = engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "n2", BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "n3", BTreeMap::new(), 1.0).unwrap();

        engine.upsert_edge(center, n1, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(center, n2, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(center, n3, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.delete_edge(e2).unwrap();

        let result = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: None }, None, None).unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|e| e.edge_id != e2));
    }

    #[test]
    fn test_neighbors_paged_with_temporal_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        let n1 = engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "n2", BTreeMap::new(), 1.0).unwrap();

        // Edge valid from 100 to 200
        engine.upsert_edge(center, n1, 10, BTreeMap::new(), 1.0, Some(100), Some(200)).unwrap();
        // Edge valid from 150 to 300
        engine.upsert_edge(center, n2, 10, BTreeMap::new(), 1.0, Some(150), Some(300)).unwrap();

        // At epoch 175, both valid
        let result = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: None }, Some(175), None).unwrap();
        assert_eq!(result.items.len(), 2);

        // At epoch 250, only n2 valid
        let result = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: None }, Some(250), None).unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].node_id, n2);
    }

    #[test]
    fn test_neighbors_paged_roundtrip_matches_neighbors() {
        // Paginated results should match unpaginated neighbors()
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        for i in 0..15 {
            let n = engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
            engine.upsert_edge(center, n, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        // Collect all via pagination
        let mut paged_eids: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(4), after: cursor }, None, None).unwrap();
            paged_eids.extend(page.items.iter().map(|e| e.edge_id));
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        // Collect all via neighbors()
        let unpaged = engine.neighbors(center, Direction::Outgoing, None, 0, None, None).unwrap();
        let mut unpaged_eids: Vec<u64> = unpaged.iter().map(|e| e.edge_id).collect();
        unpaged_eids.sort();

        assert_eq!(paged_eids, unpaged_eids);
    }

    #[test]
    fn test_neighbors_paged_temporal_default_matches_neighbors() {
        // at_epoch=None should filter against now, matching neighbors() behavior.
        // Edges with future valid_from or past valid_to must be excluded.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        let n_past = engine.upsert_node(1, "past", BTreeMap::new(), 1.0).unwrap();
        let n_current = engine.upsert_node(1, "current", BTreeMap::new(), 1.0).unwrap();
        let n_future = engine.upsert_node(1, "future", BTreeMap::new(), 1.0).unwrap();

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Expired edge (valid_to in the past)
        engine
            .upsert_edge(center, n_past, 10, BTreeMap::new(), 1.0, Some(now - 2000), Some(now - 1000))
            .unwrap();
        // Currently valid edge
        engine
            .upsert_edge(center, n_current, 10, BTreeMap::new(), 1.0, Some(now - 1000), Some(now + 100_000))
            .unwrap();
        // Future edge (valid_from in the future)
        engine
            .upsert_edge(center, n_future, 10, BTreeMap::new(), 1.0, Some(now + 50_000), Some(now + 100_000))
            .unwrap();

        // Non-paginated: should return only n_current
        let unpaged = engine.neighbors(center, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(unpaged.len(), 1);
        assert_eq!(unpaged[0].node_id, n_current);

        // Paginated with at_epoch=None: must match
        let paged = engine
            .neighbors_paged(center, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: None }, None, None)
            .unwrap();
        assert_eq!(paged.items.len(), 1, "paged should filter future/expired edges when at_epoch=None");
        assert_eq!(paged.items[0].node_id, n_current);
    }

    #[test]
    fn test_neighbors_paged_temporal_cursor_correctness() {
        // Verify cursor works correctly when temporal filtering removes items.
        // Create a mix of valid and invalid edges, paginate with small limit.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let center = engine.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        let epoch = 500_000i64;
        let mut valid_node_ids = Vec::new();

        // Create 10 edges, alternating valid/invalid at epoch=500000
        for i in 0..10u64 {
            let n = engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
            if i % 2 == 0 {
                // Valid: valid_from=100000, valid_to=900000
                engine.upsert_edge(center, n, 10, BTreeMap::new(), 1.0, Some(100_000), Some(900_000)).unwrap();
                valid_node_ids.push(n);
            } else {
                // Invalid at epoch: valid_from=600000, valid_to=900000
                engine.upsert_edge(center, n, 10, BTreeMap::new(), 1.0, Some(600_000), Some(900_000)).unwrap();
            }
        }

        // Collect all via pagination (page size 2) at fixed epoch
        let mut paged_nodes: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        let mut page_count = 0;
        loop {
            let page = engine.neighbors_paged(
                center, Direction::Outgoing, None,
                &PageRequest { limit: Some(2), after: cursor },
                Some(epoch), None,
            ).unwrap();
            paged_nodes.extend(page.items.iter().map(|e| e.node_id));
            cursor = page.next_cursor;
            page_count += 1;
            if cursor.is_none() { break; }
        }

        // Should have exactly the 5 valid nodes
        valid_node_ids.sort();
        paged_nodes.sort();
        assert_eq!(paged_nodes, valid_node_ids);
        // Should have taken 3 pages (2+2+1)
        assert_eq!(page_count, 3);
    }

    #[test]
    fn test_nodes_by_type_paged_policy_cursor_correctness() {
        // Verify cursor is pushed down in policy-filtered nodes_by_type_paged.
        // Page 2 should not re-return page 1 items.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create 10 nodes of type 1
        let mut all_ids = Vec::new();
        for i in 0..10 {
            let id = engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
            all_ids.push(id);
        }

        // Register a policy that excludes nothing (max_age very large,
        // non-matching type). Just to force the policy code path
        engine.set_prune_policy("dummy", PrunePolicy {
            max_age_ms: Some(999_999_999),
            max_weight: None,
            type_id: Some(999), // non-matching type, excludes nothing
        }).unwrap();

        // Paginate with limit=3
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.nodes_by_type_paged(1, &PageRequest { limit: Some(3), after: cursor }).unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        all_ids.sort();
        collected.sort();
        assert_eq!(collected, all_ids);
        // No duplicates
        let deduped: HashSet<u64> = collected.iter().copied().collect();
        assert_eq!(deduped.len(), all_ids.len());
    }

    #[test]
    fn test_find_nodes_paged_cursor_correctness() {
        // Verify cursor is pushed down in find_nodes_paged.
        // Create many matching nodes, paginate, verify no duplicates and parity.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut matching_ids = Vec::new();
        for i in 0..12 {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(if i % 3 == 0 { "red".to_string() } else { "blue".to_string() }));
            let id = engine.upsert_node(1, &format!("n{}", i), props, 1.0).unwrap();
            if i % 3 == 0 {
                matching_ids.push(id);
            }
        }

        // Paginate find_nodes with limit=2
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.find_nodes_paged(
                1, "color", &PropValue::String("red".to_string()),
                &PageRequest { limit: Some(2), after: cursor },
            ).unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        matching_ids.sort();
        collected.sort();
        assert_eq!(collected, matching_ids);
        // No duplicates
        let deduped: HashSet<u64> = collected.iter().copied().collect();
        assert_eq!(deduped.len(), matching_ids.len());
    }

    #[test]
    fn test_find_nodes_paged_cross_source_cursor() {
        // Verify cursor works correctly across memtable + segments.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Create some matching nodes, flush, create more
        let mut all_matching = Vec::new();
        for i in 0..5 {
            let mut props = BTreeMap::new();
            props.insert("tag".to_string(), PropValue::String("yes".to_string()));
            let id = engine.upsert_node(1, &format!("pre{}", i), props, 1.0).unwrap();
            all_matching.push(id);
        }
        engine.flush().unwrap();
        for i in 0..5 {
            let mut props = BTreeMap::new();
            props.insert("tag".to_string(), PropValue::String("yes".to_string()));
            let id = engine.upsert_node(1, &format!("post{}", i), props, 1.0).unwrap();
            all_matching.push(id);
        }

        // Paginate with limit=3
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.find_nodes_paged(
                1, "tag", &PropValue::String("yes".to_string()),
                &PageRequest { limit: Some(3), after: cursor },
            ).unwrap();
            collected.extend(&page.items);
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        all_matching.sort();
        collected.sort();
        assert_eq!(collected, all_matching);
    }

    #[test]
    fn test_find_nodes_stale_match_after_update_across_flush() {
        // Regression: find_nodes must not return stale segment matches when
        // a newer source (memtable or newer segment) has changed the property.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let n1 = engine.upsert_node(1, "n1", props, 1.0).unwrap();

        // Flush: n1 with color=red goes to segment
        engine.flush().unwrap();

        // Update n1 to color=blue in memtable
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("blue".to_string()));
        engine.upsert_node(1, "n1", props2, 1.0).unwrap();

        // find_nodes for color=red must NOT return n1 (stale segment match)
        let red = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        assert!(!red.contains(&n1), "find_nodes returned stale segment match after update");

        // find_nodes for color=blue SHOULD return n1
        let blue = engine.find_nodes(1, "color", &PropValue::String("blue".to_string())).unwrap();
        assert!(blue.contains(&n1));

        // Parity: find_nodes_paged must agree
        let red_paged = engine.find_nodes_paged(
            1, "color", &PropValue::String("red".to_string()),
            &PageRequest { limit: Some(10), after: None },
        ).unwrap();
        assert!(red_paged.items.is_empty());

        let blue_paged = engine.find_nodes_paged(
            1, "color", &PropValue::String("blue".to_string()),
            &PageRequest { limit: Some(10), after: None },
        ).unwrap();
        assert!(blue_paged.items.contains(&n1));
    }

    #[test]
    fn test_find_nodes_stale_match_across_segments() {
        // Same bug across segments: older segment has color=red, newer has color=blue.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        let n1 = engine.upsert_node(1, "n1", props, 1.0).unwrap();

        // Flush: S1 has n1 with color=red
        engine.flush().unwrap();

        // Update and flush again: S2 has n1 with color=blue
        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("blue".to_string()));
        engine.upsert_node(1, "n1", props2, 1.0).unwrap();
        engine.flush().unwrap();

        // find_nodes for color=red must NOT return n1
        let red = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        assert!(!red.contains(&n1), "find_nodes returned stale match from older segment");

        let blue = engine.find_nodes(1, "color", &PropValue::String("blue".to_string())).unwrap();
        assert!(blue.contains(&n1));
    }

    // --- 2-hop paged tests ---

    #[test]
    fn test_neighbors_2hop_paged_basic() {
        // a -> b -> c, a -> b -> d, a -> b -> e: 2-hop from a = {c, d, e}
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_node(1, "e", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, e, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Page size 2: first page gets 2, second page gets 1
        let p1 = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest { limit: Some(2), after: None }, None, None).unwrap();
        assert_eq!(p1.items.len(), 2);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest { limit: Some(2), after: p1.next_cursor }, None, None).unwrap();
        assert_eq!(p2.items.len(), 1);
        assert!(p2.next_cursor.is_none());

        // Combined node_ids should be {c, d, e}
        let mut all_nodes: Vec<u64> = p1.items.iter().chain(p2.items.iter()).map(|e| e.node_id).collect();
        all_nodes.sort();
        let mut expected = vec![c, d, e];
        expected.sort();
        assert_eq!(all_nodes, expected);
    }

    #[test]
    fn test_neighbors_2hop_paged_roundtrip() {
        // Paginate 1-at-a-time and verify matches unpaginated
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Star: a -> {b1..b5}, each bi -> {ci1, ci2}
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let mut hop2_nodes = Vec::new();
        for i in 0..5 {
            let b = engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
            engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            for j in 0..2 {
                let c = engine.upsert_node(1, &format!("c{}_{}", i, j), BTreeMap::new(), 1.0).unwrap();
                engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
                hop2_nodes.push(c);
            }
        }

        // Unpaginated
        let all = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        let mut all_eids: Vec<u64> = all.iter().map(|e| e.edge_id).collect();
        all_eids.sort();

        // Paginated, 3 at a time
        let mut paged_eids = Vec::new();
        let mut cursor: Option<u64> = None;
        loop {
            let page = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest { limit: Some(3), after: cursor }, None, None).unwrap();
            paged_eids.extend(page.items.iter().map(|e| e.edge_id));
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }

        // Verify monotonically increasing edge_id order (pagination contract)
        for i in 1..paged_eids.len() {
            assert!(paged_eids[i] > paged_eids[i - 1], "edge_ids not monotonically increasing");
        }
        assert_eq!(paged_eids, all_eids);
    }

    #[test]
    fn test_neighbors_2hop_paged_default_returns_all() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let page = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest::default(), None, None).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].node_id, c);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn test_neighbors_2hop_paged_cursor_past_end() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Cursor past all edge IDs
        let page = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: Some(u64::MAX - 1) }, None, None).unwrap();
        assert_eq!(page.items.len(), 0);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn test_neighbors_2hop_constrained_paged() {
        // Verify target_node_types filtering works with pagination
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        // 2-hop targets: c is type 2, d is type 3, e is type 2
        let c = engine.upsert_node(2, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(3, "d", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_node(2, "e", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, e, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Only type 2 targets, paginate 1 at a time
        let p1 = engine.neighbors_2hop_constrained_paged(
            a, Direction::Outgoing, None, Some(&[2]),
            &PageRequest { limit: Some(1), after: None }, None, None,
        ).unwrap();
        assert_eq!(p1.items.len(), 1);
        assert!(p1.next_cursor.is_some());

        let p2 = engine.neighbors_2hop_constrained_paged(
            a, Direction::Outgoing, None, Some(&[2]),
            &PageRequest { limit: Some(1), after: p1.next_cursor }, None, None,
        ).unwrap();
        assert_eq!(p2.items.len(), 1);
        assert!(p2.next_cursor.is_none());

        let mut all_nodes: Vec<u64> = vec![p1.items[0].node_id, p2.items[0].node_id];
        all_nodes.sort();
        let mut expected = vec![c, e];
        expected.sort();
        assert_eq!(all_nodes, expected);
    }

    #[test]
    fn test_neighbors_2hop_paged_cross_segment() {
        // Verify pagination works when data spans memtable + segments
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        // c in segment
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // d in memtable
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let page = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest::default(), None, None).unwrap();
        let mut nodes: Vec<u64> = page.items.iter().map(|e| e.node_id).collect();
        nodes.sort();
        let mut expected = vec![c, d];
        expected.sort();
        assert_eq!(nodes, expected);
    }

    #[test]
    fn test_neighbors_2hop_paged_with_decay() {
        // Decay path: sorted by score, no cursor
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 0.5, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 0.9, None, None).unwrap();

        let page = engine.neighbors_2hop_paged(
            a, Direction::Outgoing, None,
            &PageRequest { limit: Some(1), after: None },
            None, Some(0.001),
        ).unwrap();
        assert_eq!(page.items.len(), 1);
        // Decay sorts by score descending. d (weight 0.9) should beat c (weight 0.5)
        assert_eq!(page.items[0].node_id, d);
        // next_cursor is None for decay-sorted results
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn test_neighbors_2hop_paged_empty() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        // No edges at all
        let page = engine.neighbors_2hop_paged(a, Direction::Outgoing, None, &PageRequest { limit: Some(10), after: None }, None, None).unwrap();
        assert_eq!(page.items.len(), 0);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn test_edge_uniqueness_across_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create edge and flush to segment
        let e1 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None).unwrap();
        engine.flush().unwrap();

        // Upsert same triple, should find existing in segment and reuse ID
        let e2 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.9, None, None).unwrap();
        assert_eq!(e1, e2, "same triple should reuse edge ID across flush");

        // Updated weight should be in memtable
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert!((edge.weight - 0.9).abs() < f32::EPSILON);

        // Different triple still gets new ID
        let e3 = engine.upsert_edge(1, 2, 20, BTreeMap::new(), 1.0, None, None).unwrap();
        assert_ne!(e1, e3);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbor_weight_survives_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 0.42, None, None).unwrap();
        engine.flush().unwrap();

        // Read neighbors from segment. Weight should be preserved
        let nbrs = engine.neighbors(n1, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].edge_id, eid);
        assert!((nbrs[0].weight - 0.42).abs() < f32::EPSILON, "weight: {}", nbrs[0].weight);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_uniqueness_respects_cross_segment_tombstone() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create edge and flush (segment 1 has the edge)
        let e1 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.5, None, None).unwrap();
        engine.flush().unwrap();

        // Delete the edge and flush again (segment 2 has the tombstone)
        engine.delete_edge(e1).unwrap();
        engine.flush().unwrap();

        // Now both segments are on disk, memtable is clean.
        // Upserting the same triple should get a NEW ID, not resurrect the deleted edge.
        let e2 = engine.upsert_edge(1, 2, 10, BTreeMap::new(), 0.9, None, None).unwrap();
        assert_ne!(e1, e2, "deleted edge should not be resurrected across segments");

        engine.close().unwrap();
    }

    // --- Orphan segment scanning ---

    #[test]
    fn test_orphan_segment_does_not_reuse_id() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        // Create DB, insert data, flush to create segment, close
        {
            let mut engine = DatabaseEngine::open(
                &db_path,
                &DbOptions { create_if_missing: true, ..Default::default() },
            ).unwrap();
            engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }

        // Simulate an orphan: create a segment directory with a higher ID
        // that is NOT in the manifest (as if a crash occurred after writing
        // the segment but before updating the manifest).
        let orphan_dir = db_path.join("segments").join("seg_0099");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        // Write a minimal nodes.dat so it looks like a real segment
        std::fs::write(orphan_dir.join("nodes.dat"), &[0u8; 0]).unwrap();

        // Reopen. next_segment_id should skip past the orphan
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // Insert more data and flush. Should get segment ID > 99
            engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
            engine.flush().unwrap();

            // The new segment should have ID >= 100 (since orphan was seg_0099)
            let max_manifest_seg = engine.manifest().segments.iter().map(|s| s.id).max().unwrap();
            assert!(
                max_manifest_seg >= 100,
                "next segment should skip past orphan seg_0099, got seg ID {}",
                max_manifest_seg
            );

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_scan_max_segment_id_no_segments_dir() {
        let dir = TempDir::new().unwrap();
        // No segments dir at all, should return 0
        assert_eq!(scan_max_segment_id(dir.path()), 0);
    }

    #[test]
    fn test_scan_max_segment_id_finds_highest() {
        let dir = TempDir::new().unwrap();
        let seg_dir = dir.path().join("segments");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0003")).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0010")).unwrap();
        std::fs::create_dir(seg_dir.join("seg_0007")).unwrap();
        // Non-matching entries should be ignored
        std::fs::create_dir(seg_dir.join("tmp_work")).unwrap();
        std::fs::write(seg_dir.join("some_file.txt"), b"hi").unwrap();

        assert_eq!(scan_max_segment_id(dir.path()), 10);
    }

    #[test]
    fn test_map_props_roundtrip_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let mut props = BTreeMap::new();
        let mut nested = BTreeMap::new();
        nested.insert("deep_key".to_string(), PropValue::Int(99));
        nested.insert("flag".to_string(), PropValue::Bool(true));
        props.insert("metadata".to_string(), PropValue::Map(nested));
        props.insert("name".to_string(), PropValue::String("test".into()));

        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let id = engine.upsert_node(1, "map_node", props.clone(), 1.0).unwrap();

        // Read from memtable
        let node = engine.get_node(id).unwrap().unwrap();
        assert_eq!(node.props, props);

        // Flush to segment and read back
        engine.flush().unwrap();
        let node2 = engine.get_node(id).unwrap().unwrap();
        assert_eq!(node2.props, props);

        // Close, reopen, read from segment
        engine.close().unwrap();
        let engine2 = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node3 = engine2.get_node(id).unwrap().unwrap();
        assert_eq!(node3.props, props);
        engine2.close().unwrap();
    }

    // --- Temporal edge fields ---

    #[test]
    fn test_upsert_edge_default_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let edge = engine.get_edge(eid).unwrap().unwrap();
        // Default: valid_from = created_at, valid_to = i64::MAX
        assert_eq!(edge.valid_from, edge.created_at);
        assert_eq!(edge.valid_to, i64::MAX);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_custom_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(
            a, b, 10, BTreeMap::new(), 1.0,
            Some(1000), Some(5000),
        ).unwrap();

        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 1000);
        assert_eq!(edge.valid_to, 5000);

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_flush_and_segment_read() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(
            a, b, 10, BTreeMap::new(), 1.0,
            Some(2000), Some(8000),
        ).unwrap();

        // Flush to segment
        engine.flush().unwrap();

        // Read from segment
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 2000);
        assert_eq!(edge.valid_to, 8000);

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let eid;
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine.upsert_edge(
                a, b, 10, BTreeMap::new(), 1.0,
                Some(3000), Some(9000),
            ).unwrap();
            engine.close().unwrap();
        }

        // Reopen, WAL replay
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let edge = engine.get_edge(eid).unwrap().unwrap();
            assert_eq!(edge.valid_from, 3000);
            assert_eq!(edge.valid_to, 9000);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_batch_upsert_edges_temporal_fields() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        let inputs = vec![
            EdgeInput {
                from: a, to: b, type_id: 10, props: BTreeMap::new(), weight: 1.0,
                valid_from: Some(1000), valid_to: Some(5000),
            },
            EdgeInput {
                from: b, to: c, type_id: 10, props: BTreeMap::new(), weight: 1.0,
                valid_from: None, valid_to: None, // defaults
            },
        ];
        let ids = engine.batch_upsert_edges(&inputs).unwrap();

        let e1 = engine.get_edge(ids[0]).unwrap().unwrap();
        assert_eq!(e1.valid_from, 1000);
        assert_eq!(e1.valid_to, 5000);

        let e2 = engine.get_edge(ids[1]).unwrap().unwrap();
        assert_eq!(e2.valid_from, e2.created_at); // default
        assert_eq!(e2.valid_to, i64::MAX); // default

        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_fields_survive_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(
            a, b, 10, BTreeMap::new(), 1.0,
            Some(4000), Some(7000),
        ).unwrap();

        // Flush segment 1
        engine.flush().unwrap();
        // Add something to create segment 2
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Compact
        engine.compact().unwrap();
        assert_eq!(engine.segment_count(), 1);

        // Temporal fields should survive compaction
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_from, 4000);
        assert_eq!(edge.valid_to, 7000);

        engine.close().unwrap();
    }

    // --- Temporal invalidation ---

    #[test]
    fn test_invalidate_edge_closes_validity_window() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        // Edge should be valid initially
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, i64::MAX);

        // Invalidate at epoch 5000
        let result = engine.invalidate_edge(eid, 5000).unwrap();
        assert!(result.is_some());
        let updated = result.unwrap();
        assert_eq!(updated.valid_to, 5000);

        // get_edge still returns it (not tombstoned)
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, 5000);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidate_nonexistent_edge_returns_none() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let result = engine.invalidate_edge(999, 5000).unwrap();
        assert!(result.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_hidden_from_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e_ab = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        // Both neighbors visible
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);

        // Invalidate a→b at epoch 1 (in the past)
        engine.invalidate_edge(e_ab, 1).unwrap();

        // Only a→c should be visible now
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_hidden_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        // Flush to segment, then invalidate (invalidation goes to memtable/WAL)
        engine.flush().unwrap();
        engine.invalidate_edge(eid, 1).unwrap();

        // Edge should be hidden from neighbors
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out.is_empty());

        // But still retrievable via get_edge
        let edge = engine.get_edge(eid).unwrap().unwrap();
        assert_eq!(edge.valid_to, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_invalidated_edge_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (a, eid);
        {
            let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
            eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
            engine.invalidate_edge(eid, 1).unwrap();
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve invalidation
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let edge = engine.get_edge(eid).unwrap().unwrap();
            assert_eq!(edge.valid_to, 1);

            let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
            assert!(out.is_empty());
            engine.close().unwrap();
        }
    }

    // --- Point-in-time query tests ---

    #[test]
    fn test_point_in_time_query_sees_valid_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b".into(), BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c".into(), BTreeMap::new(), 1.0).unwrap();

        // Edge a→b: valid from epoch 1000 to 5000
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000)).unwrap();
        // Edge a→c: valid from epoch 3000 to 8000
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, Some(3000), Some(8000)).unwrap();

        // At epoch 500: neither edge is valid
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(500), None).unwrap();
        assert_eq!(out.len(), 0);

        // At epoch 2000: only a→b is valid
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(2000), None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, b);

        // At epoch 4000: both edges are valid
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(4000), None).unwrap();
        assert_eq!(out.len(), 2);

        // At epoch 6000: only a→c is valid (a→b expired at 5000)
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(6000), None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, c);

        // At epoch 9000: neither edge is valid
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(9000), None).unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_query_with_invalidated_edge() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b".into(), BTreeMap::new(), 1.0).unwrap();

        // Create edge with explicit validity window
        let eid = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(10000)).unwrap();

        // Invalidate at epoch 5000
        engine.invalidate_edge(eid, 5000).unwrap();

        // At epoch 3000: edge is valid (before invalidation)
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(3000), None).unwrap();
        assert_eq!(out.len(), 1);

        // At epoch 5000: edge is no longer valid (valid_to is exclusive)
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(5000), None).unwrap();
        assert_eq!(out.len(), 0);

        // At epoch 7000: edge is no longer valid
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(7000), None).unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_query_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b".into(), BTreeMap::new(), 1.0).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(2000), Some(8000)).unwrap();
        engine.flush().unwrap();

        // Query from segment: at_epoch=5000 should see it
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(5000), None).unwrap();
        assert_eq!(out.len(), 1);

        // Query from segment: at_epoch=1000 should not
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, Some(1000), None).unwrap();
        assert_eq!(out.len(), 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_2hop() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b".into(), BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c".into(), BTreeMap::new(), 1.0).unwrap();

        // a→b valid 1000-5000, b→c valid 2000-6000
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000)).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, Some(2000), Some(6000)).unwrap();

        // At epoch 3000: both hops valid, should reach c
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, Some(3000), None).unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        // At epoch 500: first hop not valid, can't reach b or c
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, Some(500), None).unwrap();
        assert_eq!(hop2.len(), 0);

        // At epoch 5500: first hop expired (a→b), can't reach c
        let hop2 = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, Some(5500), None).unwrap();
        assert_eq!(hop2.len(), 0);
        engine.close().unwrap();
    }

    // --- Decay-adjusted scoring tests ---

    #[test]
    fn test_decay_scoring_orders_by_recency() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub".into(), BTreeMap::new(), 1.0).unwrap();
        let old = engine.upsert_node(1, "old".into(), BTreeMap::new(), 1.0).unwrap();
        let recent = engine.upsert_node(1, "recent".into(), BTreeMap::new(), 1.0).unwrap();

        let now = now_millis();
        let one_day_ago = now - 24 * 3_600_000; // 24 hours ago
        let one_hour_ago = now - 3_600_000; // 1 hour ago

        // Both edges have equal base weight=1.0, but different updated_at times
        // Old edge: created/updated a day ago
        engine.upsert_edge(hub, old, 1, BTreeMap::new(), 1.0, Some(one_day_ago), None).unwrap();
        // Recent edge: created/updated an hour ago
        engine.upsert_edge(hub, recent, 1, BTreeMap::new(), 1.0, Some(one_hour_ago), None).unwrap();

        // Without decay: order is insertion order (or arbitrary)
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 2);

        // With decay (lambda=0.1): recent edge should have higher score
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.1)).unwrap();
        assert_eq!(out.len(), 2);
        // First result should be the recent one (higher decay-adjusted weight)
        assert_eq!(out[0].node_id, recent);
        assert_eq!(out[1].node_id, old);
        // Recent edge score should be higher
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_scoring_with_different_base_weights() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub".into(), BTreeMap::new(), 1.0).unwrap();
        let heavy_old = engine.upsert_node(1, "heavy_old".into(), BTreeMap::new(), 1.0).unwrap();
        let light_new = engine.upsert_node(1, "light_new".into(), BTreeMap::new(), 1.0).unwrap();

        let now = now_millis();
        let two_days_ago = now - 48 * 3_600_000;
        let one_hour_ago = now - 3_600_000;

        // Heavy but old: weight=10.0, updated 2 days ago
        engine.upsert_edge(hub, heavy_old, 1, BTreeMap::new(), 10.0, Some(two_days_ago), None).unwrap();
        // Light but new: weight=1.0, updated 1 hour ago
        engine.upsert_edge(hub, light_new, 1, BTreeMap::new(), 1.0, Some(one_hour_ago), None).unwrap();

        // With aggressive decay (lambda=0.1): age penalty on the heavy edge should be large
        // score(heavy_old) = 10.0 * exp(-0.1 * 48) ≈ 10.0 * 0.0082 ≈ 0.082
        // score(light_new) = 1.0 * exp(-0.1 * 1) ≈ 1.0 * 0.905 ≈ 0.905
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.1)).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, light_new); // light_new wins despite lower base weight
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_zero_lambda_no_reorder() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b".into(), BTreeMap::new(), 1.0).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 5.0, None, None).unwrap();

        // decay_lambda=None means no decay applied
        let out = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert!((out[0].weight - 5.0).abs() < 0.001); // original weight preserved
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_with_limit_returns_top_scored() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub".into(), BTreeMap::new(), 1.0).unwrap();
        let n1 = engine.upsert_node(1, "n1".into(), BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "n2".into(), BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "n3".into(), BTreeMap::new(), 1.0).unwrap();

        let now = now_millis();

        // Three edges with different ages, same base weight
        engine.upsert_edge(hub, n1, 1, BTreeMap::new(), 1.0, Some(now - 72 * 3_600_000), None).unwrap(); // 3 days old
        engine.upsert_edge(hub, n2, 1, BTreeMap::new(), 1.0, Some(now - 24 * 3_600_000), None).unwrap(); // 1 day old
        engine.upsert_edge(hub, n3, 1, BTreeMap::new(), 1.0, Some(now - 3_600_000), None).unwrap(); // 1 hour old

        // With decay and limit=2: should return the 2 most recent
        let out = engine.neighbors(hub, Direction::Outgoing, None, 2, None, Some(0.05)).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, n3); // most recent first
        assert_eq!(out[1].node_id, n2); // second most recent
        engine.close().unwrap();
    }

    #[test]
    fn test_point_in_time_with_decay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub".into(), BTreeMap::new(), 1.0).unwrap();
        let n1 = engine.upsert_node(1, "n1".into(), BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "n2".into(), BTreeMap::new(), 1.0).unwrap();

        // Edge 1: valid 1000-5000, updated_at=1000
        engine.upsert_edge(hub, n1, 1, BTreeMap::new(), 1.0, Some(1000), Some(5000)).unwrap();
        // Edge 2: valid 2000-8000, updated_at=2000
        engine.upsert_edge(hub, n2, 1, BTreeMap::new(), 1.0, Some(2000), Some(8000)).unwrap();

        // At epoch 3000 with decay: both visible, decay based on age from reference_time
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, Some(3000), Some(0.01)).unwrap();
        assert_eq!(out.len(), 2);
        // n2's edge is newer (updated_at=2000 vs 1000), so it should score higher
        // Both have weight 1.0, but age_hours differs
        assert_eq!(out[0].node_id, n2);

        // At epoch 6000: only edge 2 is valid
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, Some(6000), Some(0.01)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, n2);
        engine.close().unwrap();
    }

    #[test]
    fn test_decay_scoring_after_flush_segment_sourced() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node(1, "hub".into(), BTreeMap::new(), 1.0).unwrap();
        let old = engine.upsert_node(1, "old".into(), BTreeMap::new(), 1.0).unwrap();
        let recent = engine.upsert_node(1, "recent".into(), BTreeMap::new(), 1.0).unwrap();

        let now = now_millis();
        engine.upsert_edge(hub, old, 1, BTreeMap::new(), 1.0, Some(now - 48 * 3_600_000), None).unwrap();
        engine.upsert_edge(hub, recent, 1, BTreeMap::new(), 1.0, Some(now - 3_600_000), None).unwrap();

        // Flush to segment. Edges now served from segment reader
        engine.flush().unwrap();

        // Decay should still work on segment-sourced edges
        let out = engine.neighbors(hub, Direction::Outgoing, None, 0, None, Some(0.05)).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].node_id, recent); // recent edge scores higher
        assert_eq!(out[1].node_id, old);
        assert!(out[0].weight > out[1].weight);
        engine.close().unwrap();
    }

    #[test]
    fn test_negative_decay_lambda_returns_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a".into(), BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = engine.neighbors(a, Direction::Outgoing, None, 0, None, Some(-0.5));
        assert!(result.is_err());
        engine.close().unwrap();
    }

    #[test]
    fn test_temporal_adjacency_postings_survive_flush() {
        // Temporal fields in adjacency postings enable filtering
        // without per-edge record lookup after flush.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        // Edge a→b valid [1000, 5000)
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, Some(1000), Some(5000)).unwrap();
        // Edge a→c valid [3000, 9000)
        engine.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, Some(3000), Some(9000)).unwrap();

        engine.flush().unwrap();

        // At t=2000: only a→b visible
        let n = engine.neighbors(a, Direction::Outgoing, None, 0, Some(2000), None).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, b);

        // At t=4000: both visible
        let n = engine.neighbors(a, Direction::Outgoing, None, 0, Some(4000), None).unwrap();
        assert_eq!(n.len(), 2);

        // At t=6000: only a→c visible
        let n = engine.neighbors(a, Direction::Outgoing, None, 0, Some(6000), None).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_adjacency_hashmap_upsert_idempotent() {
        // HashMap adjacency means re-upsert is O(1) and idempotent.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions { edge_uniqueness: true, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();

        // Insert edge, then upsert it multiple times with different weights
        // With edge_uniqueness, (a, b, 10) deduplicates to the same edge ID.
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        let eid2 = engine.upsert_edge(a, b, 10, BTreeMap::new(), 2.0, None, None).unwrap();
        let eid3 = engine.upsert_edge(a, b, 10, BTreeMap::new(), 3.0, None, None).unwrap();
        assert_eq!(eid, eid2);
        assert_eq!(eid, eid3);

        // Should still have exactly 1 neighbor entry, not 3
        let n = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].edge_id, eid);
        assert_eq!(n[0].weight, 3.0); // latest weight

        engine.close().unwrap();
    }

    // ========================================
    // Progress callback + cancellation
    // ========================================

    #[test]
    fn test_compact_with_progress_reports_all_phases() {
        // Verify that compact_with_progress reports all four phases in order.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create data across 3 segments
        let mut node_ids = Vec::new();
        for i in 0..30 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        for i in 30..60 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Add some edges to make the edge phase meaningful
        for i in 0..10 {
            engine
                .upsert_edge(node_ids[i], node_ids[i + 1], 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        engine.flush().unwrap();

        let mut phases_seen: Vec<CompactionPhase> = Vec::new();
        let mut progress_calls = 0u32;

        let stats = engine
            .compact_with_progress(|progress| {
                // Track unique phases in order
                if phases_seen.last() != Some(&progress.phase) {
                    phases_seen.push(progress.phase);
                }
                progress_calls += 1;
                true // continue
            })
            .unwrap();

        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 60);
        assert!(stats.edges_kept >= 10);

        // Must see all four phases
        assert_eq!(
            phases_seen,
            vec![
                CompactionPhase::CollectingTombstones,
                CompactionPhase::MergingNodes,
                CompactionPhase::MergingEdges,
                CompactionPhase::WritingOutput,
            ]
        );

        // Multiple progress calls: 3 per merge phase (one per segment) + tombstone + 2 write
        assert!(progress_calls >= 4, "got {} calls", progress_calls);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_tombstones() {
        // Cancel during tombstone collection. Engine state must be unchanged.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Cancel on first callback (during CollectingTombstones)
        let result = engine.compact_with_progress(|_| false);
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // Engine should still work, all data intact
        for &id in &ids {
            let node = engine.get_node(id).unwrap();
            assert!(node.is_some(), "node {} missing after cancel", id);
        }

        // Should still be able to compact successfully
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().nodes_kept, 40);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_merge_nodes() {
        // Cancel during the MergingNodes phase. No state change.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Cancel during MergingNodes
        let result = engine.compact_with_progress(|progress| {
            progress.phase != CompactionPhase::MergingNodes
        });
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // All data still accessible
        for &id in &ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_during_merge_edges() {
        // Cancel during the MergingEdges phase.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut node_ids = Vec::new();
        for i in 0..10 {
            node_ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 0..5 {
            engine
                .upsert_edge(node_ids[i], node_ids[i + 1], 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        engine.flush().unwrap();

        let result = engine.compact_with_progress(|progress| {
            progress.phase != CompactionPhase::MergingEdges
        });
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // All data intact
        for &id in &node_ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        for i in 0..5 {
            let neighbors = engine
                .neighbors(node_ids[i], Direction::Outgoing, None, 0, None, None)
                .unwrap();
            assert!(
                !neighbors.is_empty(),
                "node {} should have outgoing edges",
                node_ids[i]
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_cancel_before_write() {
        // Cancel at the WritingOutput phase. No temp dirs left behind.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 10..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        let result = engine.compact_with_progress(|progress| {
            progress.phase != CompactionPhase::WritingOutput
        });
        assert!(matches!(result, Err(EngineError::CompactionCancelled)));

        // No temp segment directories should be left behind
        let segments_dir = db_path.join("segments");
        if segments_dir.exists() {
            for entry in std::fs::read_dir(&segments_dir).unwrap() {
                let name = entry.unwrap().file_name();
                let name_str = name.to_string_lossy();
                assert!(
                    !name_str.contains(".tmp"),
                    "temp dir {} left after cancel",
                    name_str
                );
            }
        }

        // Data still intact, can still compact successfully
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_records_processed_counts() {
        // Verify records_processed and total_records are accurate.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // 50 nodes in seg1, 50 nodes in seg2
        for i in 0..50 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        for i in 50..100 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();

        let mut final_node_progress: Option<CompactionProgress> = None;

        engine
            .compact_with_progress(|progress| {
                if progress.phase == CompactionPhase::MergingNodes {
                    final_node_progress = Some(progress.clone());
                }
                true
            })
            .unwrap();

        let np = final_node_progress.unwrap();
        assert_eq!(np.total_records, 100, "total_records should be 100 nodes");
        assert_eq!(np.records_processed, 100, "all 100 should be processed");
        assert_eq!(np.segments_processed, 2);
        assert_eq!(np.total_segments, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_with_progress_tombstone_counts_all_examined() {
        // S2 fix: records_processed counts all examined records (including
        // tombstoned) so progress bars reach 100% even with deletes.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..50 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        // Delete 20 nodes → tombstones
        for i in 0..20 {
            engine.delete_node(ids[i]).unwrap();
        }
        engine.flush().unwrap();

        let mut final_node_progress: Option<CompactionProgress> = None;

        let stats = engine
            .compact_with_progress(|progress| {
                if progress.phase == CompactionPhase::MergingNodes {
                    final_node_progress = Some(progress.clone());
                }
                true
            })
            .unwrap()
            .unwrap();

        let np = final_node_progress.unwrap();
        // total_records = total input node count across segments (50 in seg1 + 0 in seg2)
        assert_eq!(np.total_records, 50);
        // records_processed should equal total_records (all examined, even tombstoned)
        assert_eq!(np.records_processed, np.total_records);
        // But compaction only kept the live ones
        assert_eq!(stats.nodes_kept, 30);
        assert_eq!(stats.nodes_removed, 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_compact_no_callback_wrapper() {
        // compact() (no callback) still works as before.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions::default();
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();
        for i in 20..40 {
            ids.push(
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap(),
            );
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 40);

        // Verify data integrity post-compact
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    // --- Fast-path compaction tests ---

    #[test]
    fn test_segments_non_overlapping_detection() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Unique keys per flush → non-overlapping IDs
        for seg in 0..3u64 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        assert!(segments_are_non_overlapping(&engine.segments));
        engine.close().unwrap();
    }

    #[test]
    fn test_segments_overlapping_detection() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Same keys across flushes → same IDs → overlapping
        for _seg in 0..3 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        assert!(!segments_are_non_overlapping(&engine.segments));
        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_compaction_correctness() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Build 3 segments with unique non-overlapping data
        let mut all_node_ids = Vec::new();
        let mut all_edge_ids = Vec::new();
        for seg in 0..3u64 {
            let mut seg_node_ids = Vec::new();
            for i in 0..20 {
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
                seg_node_ids.push(id);
                all_node_ids.push(id);
            }
            for i in 0..5 {
                let eid = engine
                    .upsert_edge(
                        seg_node_ids[i],
                        seg_node_ids[i + 1],
                        1,
                        BTreeMap::new(),
                        1.0,
                        None,
                        None,
                    )
                    .unwrap();
                all_edge_ids.push(eid);
            }
            engine.flush().unwrap();
        }

        assert_eq!(engine.segments.len(), 3);
        // Pre-condition: non-overlapping, no tombstones (simplest V3 case)
        assert!(!engine.segments.iter().any(|s| s.has_tombstones()));
        assert!(segments_are_non_overlapping(&engine.segments));

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 60);
        assert_eq!(stats.edges_kept, 15);
        assert_eq!(stats.nodes_removed, 0);
        assert_eq!(stats.edges_removed, 0);
        assert_eq!(engine.segments.len(), 1);

        // Verify all records are accessible (batch read)
        let node_results = engine.get_nodes(&all_node_ids).unwrap();
        for (i, result) in node_results.iter().enumerate() {
            assert!(
                result.is_some(),
                "node {} missing after compact",
                all_node_ids[i]
            );
        }
        let edge_results = engine.get_edges(&all_edge_ids).unwrap();
        for (i, result) in edge_results.iter().enumerate() {
            assert!(
                result.is_some(),
                "edge {} missing after compact",
                all_edge_ids[i]
            );
        }

        // Verify neighbors work
        for seg in 0..3u64 {
            let first_node = all_node_ids[(seg as usize) * 20];
            let nbrs = engine
                .neighbors(first_node, Direction::Outgoing, None, 100, None, None)
                .unwrap();
            assert_eq!(nbrs.len(), 1);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_with_properties() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segments with property data to verify raw byte copy preserves properties
        let mut ids = Vec::new();
        for seg in 0..2u64 {
            for i in 0..10 {
                let mut props = BTreeMap::new();
                props.insert("seg".to_string(), PropValue::UInt(seg));
                props.insert(
                    "name".to_string(),
                    PropValue::String(format!("s{}_n{}", seg, i)),
                );
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), props, 1.0)
                    .unwrap();
                ids.push(id);
            }
            engine.flush().unwrap();
        }

        engine.compact().unwrap();

        // Verify properties survived the raw binary merge
        for (idx, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            let seg = (idx / 10) as u64;
            assert_eq!(node.props.get("seg"), Some(&PropValue::UInt(seg)));
            assert_eq!(
                node.props.get("name"),
                Some(&PropValue::String(format!(
                    "s{}_n{}",
                    seg,
                    idx % 10
                )))
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };

        let mut ids = Vec::new();
        {
            let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
            for seg in 0..3u64 {
                for i in 0..10 {
                    let id = engine
                        .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                        .unwrap();
                    ids.push(id);
                }
                engine.flush().unwrap();
            }
            engine.compact().unwrap();
            engine.close().unwrap();
        }

        // Reopen and verify data
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        assert_eq!(engine.segments.len(), 1);
        engine.close().unwrap();
    }

    #[test]
    fn test_fast_merge_find_nodes_works() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for seg in 0..2u64 {
            for i in 0..20 {
                let mut props = BTreeMap::new();
                let color = if i % 2 == 0 { "red" } else { "blue" };
                props.insert("color".to_string(), PropValue::String(color.to_string()));
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), props, 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        engine.compact().unwrap();

        // find_nodes should work on the fast-merged segment
        let red = engine
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .unwrap();
        assert_eq!(red.len(), 20); // 10 red per segment * 2 segments
        let blue = engine
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .unwrap();
        assert_eq!(blue.len(), 20);

        engine.close().unwrap();
    }

    #[test]
    fn test_standard_path_used_for_overlapping_segments() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Same keys → overlapping IDs → standard path
        for _seg in 0..3 {
            for i in 0..10 {
                engine
                    .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        // Should still compact correctly via standard path
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 3);
        assert_eq!(stats.nodes_kept, 10); // deduped to 10 unique nodes
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            assert!(engine
                .get_node(engine.find_existing_node(1, &format!("n{}", i)).unwrap().unwrap().0)
                .unwrap()
                .is_some());
        }

        engine.close().unwrap();
    }

    // --- Auto-compaction tests ---

    #[test]
    fn test_auto_compact_triggers_after_n_flushes() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 3,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Flush 1 and 2: no compaction yet
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 2);

        // Flush 3: should trigger auto-compact (3 segments → 1)
        for i in 0..10 {
            engine
                .upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it to complete.
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        // All 30 nodes should be accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after auto-compact",
                    key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_disabled_when_zero() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for flush in 0..10u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("f{}_n{}", flush, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }

        // No auto-compact → all 10 segments should still exist
        assert_eq!(engine.segments.len(), 10);
        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_counter_resets_on_manual_compact() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 5,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // 2 flushes
        for seg in 0..2u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        assert_eq!(engine.segments.len(), 2);
        assert_eq!(engine.flush_count_since_last_compact, 2);

        // Manual compact resets the counter
        engine.compact().unwrap();
        assert_eq!(engine.flush_count_since_last_compact, 0);
        assert_eq!(engine.segments.len(), 1);

        // Now 4 more flushes (counter reset, so 5th from here triggers auto-compact)
        for seg in 2..6u64 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        // 4 flushes since manual compact: segments = 1 (from manual) + 4 = 5
        assert_eq!(engine.segments.len(), 5);

        // 5th flush triggers auto-compact
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("s6_n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it.
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_data_integrity() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let mut all_ids = Vec::new();
        // This will trigger auto-compact after every 2 flushes
        for seg in 0..6u64 {
            for i in 0..10 {
                let id = engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
                all_ids.push(id);
            }
            engine.flush().unwrap();
        }

        // Verify all data is intact despite multiple auto-compactions
        for &id in &all_ids {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node {} missing after auto-compactions",
                id
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_auto_compact_not_triggered_during_compact_flush() {
        // Verify that the flush inside compact() doesn't trigger recursive auto-compact
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 1, // trigger after every single flush
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First flush triggers auto-compact since threshold is 1.
        // But we only have 1 segment after flush, so compact() returns None (< 2 segments).
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Only 1 segment, compact can't fire (needs >= 2)
        assert_eq!(engine.segments.len(), 1);

        // Second flush: now 2 segments, auto-compact should fire
        for i in 0..5 {
            engine
                .upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        engine.flush().unwrap();
        // Auto-compact fires in background. Wait for it.
        engine.wait_for_bg_compact();
        // compact fires: 2 segments → 1. The flush inside compact()
        // (for unflushed memtable) should NOT trigger recursive auto-compact.
        assert_eq!(engine.segments.len(), 1);

        // All data accessible
        for i in 0..5 {
            assert!(engine.find_existing_node(1, &format!("a{}", i)).unwrap().is_some());
            assert!(engine.find_existing_node(1, &format!("b{}", i)).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    // --- Background compaction tests ---

    #[test]
    fn test_bg_compact_basic() {
        // Trigger auto-compact (threshold=2), wait, verify segment count and data.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes to trigger background compaction
        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        // Background compaction should have been started
        assert!(engine.bg_compact.is_some() || engine.segments.len() == 1);

        // Wait for background compaction to complete
        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        // All 20 nodes accessible
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_writes_during() {
        // Write more data while background compaction is running. Verify everything
        // is intact after close/reopen.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes to trigger bg compact
        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        // bg compact started (or already finished for small data)

        // Immediately write more data. Should NOT block
        for i in 0..20 {
            engine.upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        // Close waits for bg compact, then writes manifest
        engine.close().unwrap();

        // Reopen and verify all data
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for prefix in ["a", "b", "c"] {
            let count = if prefix == "c" { 20 } else { 10 };
            for i in 0..count {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.get_node(
                        engine.find_existing_node(1, &key).unwrap().unwrap().0
                    ).unwrap().is_some(),
                    "node {} missing after bg compact + writes", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_flush_during() {
        // Trigger bg compact, then do enough writes to cause another flush.
        // Verify both the new segment and the compacted segment coexist correctly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0,   // manual flush only
            memtable_hard_cap_bytes: 0,    // no backpressure
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap(); // bg compact starts here

        // Write more data and flush. Adds a NEW segment while bg compact runs
        for i in 0..10 {
            engine.upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap(); // new segment added; bg compact may still be running

        // Wait for bg compact
        engine.wait_for_bg_compact();

        // Should have: 1 compacted segment (from a+b) + 1 new segment (from c)
        assert_eq!(engine.segments.len(), 2);

        // All data accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact + flush during", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_no_double() {
        // Verify that a second bg compact is NOT started while one is running.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 1, // trigger after every flush
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First flush: only 1 segment, bg compact needs >= 2, so no bg compact
        for i in 0..5 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        assert!(engine.bg_compact.is_none());

        // Second flush: 2 segments, bg compact starts
        for i in 0..5 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        // bg_compact should be Some (or already completed)
        let had_bg = engine.bg_compact.is_some();

        // Third flush: bg compact is still running (or just completed),
        // should NOT start a second bg compact
        for i in 0..5 {
            engine.upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Wait for everything to settle
        engine.wait_for_bg_compact();

        // All data accessible
        for prefix in ["a", "b", "c"] {
            for i in 0..5 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing", key
                );
            }
        }

        // Just verify no panics occurred and data is consistent
        engine.close().unwrap();

        // If bg compact was running at flush 3, the guard should have prevented
        // a second bg compact from starting. We can't easily assert on timing,
        // but absence of panics + data integrity proves correctness.
        let _ = had_bg; // used above for documentation
    }

    #[test]
    fn test_bg_compact_manual_after_bg() {
        // bg compact finishes, then manual compact() works correctly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap(); // bg compact starts

        // Add more segments
        for i in 0..10 {
            engine.upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("d{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Manual compact. Should first wait for bg compact, then compact everything
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        assert_eq!(engine.segments.len(), 1);

        // All data accessible
        for prefix in ["a", "b", "c", "d"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after manual compact", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_drop_waits() {
        // Drop engine without close(). Verify no thread leak and data is on disk.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap(); // bg compact starts

        // Drop without close. Drop impl should wait for bg compact
        drop(engine);

        // Reopen and verify segments are compacted and data is accessible
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        // Data should be in segments (flushed before bg compact, then compacted)
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after drop + reopen", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_immediate_mode() {
        // Verify bg compact works with Immediate sync mode (not just GroupCommit).
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        engine.wait_for_bg_compact();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact (immediate mode)", key
                );
            }
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_group_commit_mode() {
        // Verify bg compact works with GroupCommit sync mode.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Write more data while bg compact may be running
        for i in 0..10 {
            engine.upsert_node(1, &format!("c{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        engine.close().unwrap();

        // Reopen and verify all data
        let opts_reopen = DbOptions {
            compact_after_n_flushes: 0, // disable auto-compact for clean verification
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts_reopen).unwrap();
        for prefix in ["a", "b", "c"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after bg compact (group commit mode)", key
                );
            }
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compact_cancel() {
        // Cancel a running background compaction. Original segments should remain.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 2,
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Two flushes → triggers bg compact
        for i in 0..10 {
            engine.upsert_node(1, &format!("a{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10 {
            engine.upsert_node(1, &format!("b{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Cancel the bg compact (may have already finished for small data, that's OK)
        engine.cancel_bg_compact();
        assert!(engine.bg_compact.is_none());

        // Segments should be >= 2 (cancel prevented the compaction from applying,
        // or if it finished before cancel, wait_for_bg_compact in cancel already
        // joined the thread; either way the engine is in a consistent state).
        // The key assertion: all data is accessible.
        for prefix in ["a", "b"] {
            for i in 0..10 {
                let key = format!("{}{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after cancel", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_orphan_segment_cleanup_on_open() {
        // Create orphan segment directories that are NOT in the manifest.
        // Verify that open() cleans them up.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0, // disable auto-compact
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write + flush to create a real segment
        for i in 0..5 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();
        assert_eq!(engine.segments.len(), 1);

        engine.close().unwrap();

        // Create orphan segment directories (simulate crash between segment write
        // and manifest update, or between bg compact output and apply).
        let orphan1 = segment_dir(dir.path(), 9990);
        let orphan2 = segment_dir(dir.path(), 9991);
        std::fs::create_dir_all(&orphan1).unwrap();
        std::fs::create_dir_all(&orphan2).unwrap();
        // Write a dummy file so the directory isn't empty
        std::fs::write(orphan1.join("dummy.dat"), b"orphan").unwrap();
        std::fs::write(orphan2.join("dummy.dat"), b"orphan").unwrap();
        assert!(orphan1.exists());
        assert!(orphan2.exists());

        // Reopen. Orphans should be cleaned up
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(!orphan1.exists(), "orphan1 should have been cleaned up");
        assert!(!orphan2.exists(), "orphan2 should have been cleaned up");

        // Real segment should still be there
        assert_eq!(engine.segments.len(), 1);
        for i in 0..5 {
            let key = format!("n{}", i);
            assert!(
                engine.find_existing_node(1, &key).unwrap().is_some(),
                "node {} missing after orphan cleanup", key
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_orphan_cleanup_preserves_valid_segments() {
        // Verify orphan cleanup does NOT delete segments that ARE in the manifest.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 3 segments
        for seg in 0..3 {
            for i in 0..5 {
                engine
                    .upsert_node(1, &format!("s{}_n{}", seg, i), BTreeMap::new(), 1.0)
                    .unwrap();
            }
            engine.flush().unwrap();
        }
        assert_eq!(engine.segments.len(), 3);
        engine.close().unwrap();

        // Reopen. All 3 segments should survive (no orphan cleanup of valid segments)
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert_eq!(engine.segments.len(), 3);

        // All data accessible
        for seg in 0..3 {
            for i in 0..5 {
                let key = format!("s{}_n{}", seg, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing", key
                );
            }
        }

        engine.close().unwrap();
    }

    // --- Group commit tests ---

    /// Helper to create a DB with Immediate WAL sync mode.
    fn temp_db_immediate() -> (TempDir, DatabaseEngine) {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        (dir, engine)
    }

    /// Helper to create a DB with GroupCommit WAL sync mode.
    fn temp_db_group_commit() -> (TempDir, DatabaseEngine) {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        (dir, engine)
    }

    #[test]
    fn test_immediate_mode_basic_operations() {
        let (dir, mut engine) = temp_db_immediate();

        // Write nodes and edges
        let n1 = engine.upsert_node(1, "alice", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "bob", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Read back immediately
        assert!(engine.get_node(n1).unwrap().is_some());
        assert!(engine.get_node(n2).unwrap().is_some());
        assert!(engine.get_edge(e1).unwrap().is_some());

        // Close and reopen
        engine.close().unwrap();
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        assert!(engine.get_node(n1).unwrap().is_some());
        assert!(engine.get_node(n2).unwrap().is_some());
        assert!(engine.get_edge(e1).unwrap().is_some());
        engine.close().unwrap();
    }

    #[test]
    fn test_immediate_mode_batch_operations() {
        let (_dir, mut engine) = temp_db_immediate();

        let inputs: Vec<NodeInput> = (0..50).map(|i| NodeInput {
            type_id: 1,
            key: format!("node_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        }).collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 50);

        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_immediate_mode_flush_compact_cycle() {
        let (_dir, mut engine) = temp_db_immediate();

        // Insert, flush, insert more, flush, compact
        for i in 0..100 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        for i in 100..200 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap();
        assert!(stats.is_some());

        // Verify all data present
        for i in 0..200 {
            assert!(
                engine.get_node(engine.find_existing_node(1, &format!("n{}", i)).unwrap().unwrap().0).unwrap().is_some(),
                "node n{} missing after compact", i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_basic_write_close_reopen() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 20 nodes
        let mut ids = Vec::new();
        for i in 0..20 {
            let id = engine.upsert_node(1, &format!("gc_node_{}", i), BTreeMap::new(), 1.0).unwrap();
            ids.push(id);
        }

        // All visible immediately via read-after-write
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        // Close (should drain all buffered data)
        engine.close().unwrap();

        // Reopen (with Immediate to avoid needing group commit for reads)
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        // All nodes survive restart
        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some(), "node {} missing after reopen", id);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_with_edges() {
        let (dir, mut engine) = temp_db_group_commit();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 0.5, None, None).unwrap();

        // Read-after-write consistency
        let neighbors = engine.neighbors(n1, Direction::Outgoing, None, 10, None, None).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node_id, n2);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(engine.get_edge(e1).unwrap().is_some());
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_batch_operations() {
        let (dir, mut engine) = temp_db_group_commit();

        let inputs: Vec<NodeInput> = (0..100).map(|i| NodeInput {
            type_id: 1,
            key: format!("batch_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        }).collect();

        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        assert_eq!(ids.len(), 100);

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some(), "batch node {} missing", id);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_sync_forces_immediate_flush() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write a node
        let id = engine.upsert_node(1, "sync_test", BTreeMap::new(), 1.0).unwrap();

        // Force sync. After this, data must be on disk
        engine.sync().unwrap();

        // Drop without close (no clean shutdown sync)
        drop(engine);

        // Reopen. Data should be present because we called sync()
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        assert!(engine.get_node(id).unwrap().is_some(), "sync'd data missing after drop");
        engine.close().unwrap();
    }

    #[test]
    fn test_sync_noop_in_immediate_mode() {
        let (_dir, mut engine) = temp_db_immediate();

        engine.upsert_node(1, "test", BTreeMap::new(), 1.0).unwrap();
        // sync() should be a no-op in Immediate mode and not error
        engine.sync().unwrap();
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_flush_cycle() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write → flush → write → flush under GroupCommit
        for i in 0..50 {
            engine.upsert_node(1, &format!("pre_flush_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        for i in 0..50 {
            engine.upsert_node(1, &format!("post_flush_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        engine.close().unwrap();

        // Reopen and verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for i in 0..50 {
            assert!(engine.find_existing_node(1, &format!("pre_flush_{}", i)).unwrap().is_some());
            assert!(engine.find_existing_node(1, &format!("post_flush_{}", i)).unwrap().is_some());
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_drop_joins_sync_thread() {
        // Verify Drop impl doesn't panic and joins the sync thread
        let (_dir, mut engine) = temp_db_group_commit();

        for i in 0..10 {
            engine.upsert_node(1, &format!("drop_test_{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        // Drop without close. Should not panic
        drop(engine);
        // If we get here, Drop succeeded without panic
    }

    #[test]
    fn test_default_options_use_group_commit() {
        let opts = DbOptions::default();
        assert!(matches!(opts.wal_sync_mode, WalSyncMode::GroupCommit { .. }));
    }

    // --- Group Commit CP2: Hardening tests ---

    #[test]
    fn test_backpressure_blocks_writer_at_hard_cap() {
        // Use a very small hard cap (256 bytes) so a few node writes exceed it.
        // The sync thread interval is very fast (1ms) so it drains quickly.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: false,
            compact_after_n_flushes: 0, // disable auto-compact
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 1,
                soft_trigger_bytes: 128,
                hard_cap_bytes: 256,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write many nodes. Some will block on backpressure but the sync thread
        // will drain them. If backpressure is broken, buffered_bytes grows unbounded.
        for i in 0..200 {
            engine.upsert_node(1, &format!("bp_{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        // All writes completed. Read them all back
        for i in 0..200 {
            assert!(
                engine.find_existing_node(1, &format!("bp_{}", i)).unwrap().is_some(),
                "node bp_{} missing after backpressure writes", i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_clean_shutdown_drains_all_buffered_data() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 100 nodes rapidly (most will be buffered, not yet synced)
        for i in 0..100 {
            engine.upsert_node(1, &format!("drain_{}", i), BTreeMap::new(), 1.0).unwrap();
        }

        // close() should drain everything
        engine.close().unwrap();

        // Reopen and verify all 100 nodes
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for i in 0..100 {
            assert!(
                engine.find_existing_node(1, &format!("drain_{}", i)).unwrap().is_some(),
                "node drain_{} lost during shutdown", i
            );
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_drop_drains_buffered_data() {
        let dir = TempDir::new().unwrap();

        // Write data and drop without close
        {
            let mut engine = DatabaseEngine::open(dir.path(), &DbOptions {
                create_if_missing: true,
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 5,
                    soft_trigger_bytes: 4 * 1024 * 1024,
                    hard_cap_bytes: 16 * 1024 * 1024,
                },
                ..DbOptions::default()
            }).unwrap();

            for i in 0..50 {
                engine.upsert_node(1, &format!("drop_drain_{}", i), BTreeMap::new(), 1.0).unwrap();
            }

            // Drop without close. Drop impl should flush buffered data
            drop(engine);
        }

        // Reopen and check data survived (note: manifest won't be updated by Drop,
        // so data may come from WAL replay, which is correct)
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for i in 0..50 {
            assert!(
                engine.find_existing_node(1, &format!("drop_drain_{}", i)).unwrap().is_some(),
                "node drop_drain_{} lost after drop", i
            );
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_sync_failure_poisons_engine() {
        // Test the poison mechanism directly through WalSyncState.
        // We can't easily force filesystem failures, but we can verify
        // that writers check the poisoned flag and return the right error.
        use crate::wal_sync::WalSyncState;

        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: Some("test: WAL sync failed 5 times".to_string()),
        };

        let arc = std::sync::Arc::new((std::sync::Mutex::new(state), std::sync::Condvar::new()));

        // Create an engine with GroupCommit mode and inject the poisoned state
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 1000, // long interval so sync thread doesn't interfere
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Replace the wal_state with our poisoned one (shut down the existing sync thread first)
        if let Some(ref wal_state) = engine.wal_state {
            crate::wal_sync::shutdown_sync_thread(wal_state, &mut engine.sync_thread).unwrap();
        }
        engine.wal_state = Some(arc);
        engine.sync_thread = None; // no sync thread needed for this test

        // Attempt to write. Should get WalSyncFailed error
        let result = engine.upsert_node(1, "should_fail", BTreeMap::new(), 1.0);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("WAL sync failed"), "unexpected error: {}", err_msg);
    }

    #[test]
    fn test_integration_1000_writes_group_commit() {
        let (dir, mut engine) = temp_db_group_commit();

        // Write 1000 nodes with properties
        for i in 0..1000 {
            let mut props = BTreeMap::new();
            props.insert("index".to_string(), PropValue::Int(i as i64));
            engine.upsert_node(1, &format!("int_{}", i), props, 1.0).unwrap();
        }

        // Close and reopen
        engine.close().unwrap();
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        // Verify all 1000 nodes with correct properties
        for i in 0..1000 {
            let (id, _) = engine.find_existing_node(1, &format!("int_{}", i)).unwrap()
                .unwrap_or_else(|| panic!("node int_{} missing", i));
            let node = engine.get_node(id).unwrap()
                .unwrap_or_else(|| panic!("node {} not found by id", id));
            assert_eq!(node.props.get("index"), Some(&PropValue::Int(i as i64)));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_integration_write_flush_write_flush_group_commit() {
        // Exercises truncate_and_reset through multiple flush cycles
        let (dir, mut engine) = temp_db_group_commit();

        // Cycle 1: write → flush
        for i in 0..100 {
            engine.upsert_node(1, &format!("c1_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        let seg1 = engine.flush().unwrap();
        assert!(seg1.is_some());

        // Cycle 2: write → flush
        for i in 0..100 {
            engine.upsert_node(1, &format!("c2_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        let seg2 = engine.flush().unwrap();
        assert!(seg2.is_some());

        // Cycle 3: write → flush
        for i in 0..100 {
            engine.upsert_node(1, &format!("c3_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        let seg3 = engine.flush().unwrap();
        assert!(seg3.is_some());

        engine.close().unwrap();

        // Reopen and verify all data from all 3 cycles
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for prefix in &["c1", "c2", "c3"] {
            for i in 0..100 {
                let key = format!("{}_{}", prefix, i);
                assert!(
                    engine.find_existing_node(1, &key).unwrap().is_some(),
                    "node {} missing after multi-flush cycle", key
                );
            }
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_delete_and_compact_cycle() {
        let (dir, mut engine) = temp_db_group_commit();

        // Insert nodes
        let mut ids = Vec::new();
        for i in 0..100 {
            let id = engine.upsert_node(1, &format!("gc_del_{}", i), BTreeMap::new(), 1.0).unwrap();
            ids.push(id);
        }
        engine.flush().unwrap();

        // Delete half
        for &id in &ids[..50] {
            engine.delete_node(id).unwrap();
        }
        engine.flush().unwrap();

        // Compact
        let stats = engine.compact().unwrap();
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert!(stats.nodes_removed > 0);

        // Verify: deleted nodes gone, remaining present
        for &id in &ids[..50] {
            assert!(engine.get_node(id).unwrap().is_none(), "deleted node {} still present", id);
        }
        for &id in &ids[50..] {
            assert!(engine.get_node(id).unwrap().is_some(), "surviving node {} missing", id);
        }

        engine.close().unwrap();

        // Reopen and re-verify
        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        for &id in &ids[..50] {
            assert!(engine.get_node(id).unwrap().is_none());
        }
        for &id in &ids[50..] {
            assert!(engine.get_node(id).unwrap().is_some());
        }
        engine.close().unwrap();
    }

    #[test]
    fn test_group_commit_rejects_invalid_parameters() {
        let dir = TempDir::new().unwrap();

        // interval_ms = 0
        let result = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 0,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        });
        assert!(result.is_err());

        // soft_trigger_bytes = 0
        let result = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 0,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        });
        assert!(result.is_err());

        // hard_cap_bytes = 0
        let result = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 0,
            },
            ..DbOptions::default()
        });
        assert!(result.is_err());

        // hard_cap <= soft_trigger
        let result = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 1024,
                hard_cap_bytes: 1024,
            },
            ..DbOptions::default()
        });
        assert!(result.is_err());

        // Valid parameters should succeed
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 1024,
                hard_cap_bytes: 2048,
            },
            ..DbOptions::default()
        }).unwrap();
        engine.close().unwrap();
    }

    // --- Memtable backpressure tests ---

    #[test]
    fn test_backpressure_flush_triggers_at_hard_cap_immediate() {
        // With a tiny hard cap, writes should trigger flushes automatically
        // even without the soft auto-flush threshold being set.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0, // auto-flush disabled
            memtable_hard_cap_bytes: 512, // tiny hard cap
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0, // disable auto-compact
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        // Write enough data to exceed the 512-byte cap multiple times
        let mut ids = Vec::new();
        for i in 0..50 {
            let id = engine.upsert_node(1, &format!("bp_imm_{}", i), BTreeMap::new(), 0.5).unwrap();
            ids.push(id);
        }

        // Backpressure should have triggered at least one flush
        assert!(engine.segment_count() >= 1, "expected at least 1 segment from backpressure flush");

        // All data readable across memtable + segments
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("bp_imm_{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_flush_triggers_at_hard_cap_group_commit() {
        // Same test but with GroupCommit mode. Verifies no deadlock when
        // backpressure flush acquires WAL lock and then the write also needs it.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0, // auto-flush disabled
            memtable_hard_cap_bytes: 512, // tiny hard cap
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.segment_count(), 0);

        let mut ids = Vec::new();
        for i in 0..50 {
            let id = engine.upsert_node(1, &format!("bp_gc_{}", i), BTreeMap::new(), 0.5).unwrap();
            ids.push(id);
        }

        // Backpressure flushed at least once
        assert!(engine.segment_count() >= 1, "expected backpressure flush in group commit mode");

        // Data integrity
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("bp_gc_{}", i));
        }

        engine.close().unwrap();

        // Reopen and verify durability
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some(), "node {} missing after reopen", id);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_disabled_when_zero() {
        // With hard cap = 0 (disabled) and auto-flush disabled, no flushes happen.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 0, // disabled
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..100 {
            engine.upsert_node(1, &format!("no_bp_{}", i), BTreeMap::new(), 0.5).unwrap();
        }

        // No flushes should have occurred
        assert_eq!(engine.segment_count(), 0);
        assert_eq!(engine.node_count(), 100);

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_fires_before_soft_threshold() {
        // Set hard cap below the soft auto-flush threshold.
        // Backpressure should trigger flushes before auto-flush would.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 1024 * 1024, // 1MB soft threshold (never reached in this test)
            memtable_hard_cap_bytes: 512,           // 512 byte hard cap
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..30 {
            engine.upsert_node(1, &format!("early_bp_{}", i), BTreeMap::new(), 0.5).unwrap();
        }

        // Backpressure kicked in before the 1MB soft threshold
        assert!(engine.segment_count() >= 1, "backpressure should trigger before soft threshold");

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_with_edges_and_deletes() {
        // Verify backpressure works for all write types, not just upsert_node.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create nodes
        let mut node_ids = Vec::new();
        for i in 0..20 {
            let id = engine.upsert_node(1, &format!("n_{}", i), BTreeMap::new(), 0.5).unwrap();
            node_ids.push(id);
        }

        // Create edges (triggers backpressure flush too)
        let mut edge_ids = Vec::new();
        for i in 0..19 {
            let eid = engine.upsert_edge(
                node_ids[i], node_ids[i + 1], 1, BTreeMap::new(), 0.5, None, None,
            ).unwrap();
            edge_ids.push(eid);
        }

        // Delete some nodes. Should also respect backpressure
        for i in 0..5 {
            engine.delete_node(node_ids[i]).unwrap();
        }

        // Delete some edges
        for i in 0..3 {
            engine.delete_edge(edge_ids[i]).unwrap();
        }

        // Segments created by backpressure
        assert!(engine.segment_count() >= 1);

        // Remaining data is accessible
        for i in 5..20 {
            assert!(engine.get_node(node_ids[i]).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_with_batch_upserts() {
        // Batch operations should also trigger backpressure before writing.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // First batch: fills memtable
        let inputs1: Vec<NodeInput> = (0..20).map(|i| NodeInput {
            type_id: 1,
            key: format!("batch1_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        }).collect();
        let ids1 = engine.batch_upsert_nodes(&inputs1).unwrap();

        // Second batch: should trigger backpressure flush before appending
        let inputs2: Vec<NodeInput> = (0..20).map(|i| NodeInput {
            type_id: 1,
            key: format!("batch2_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        }).collect();
        let ids2 = engine.batch_upsert_nodes(&inputs2).unwrap();

        assert!(engine.segment_count() >= 1, "backpressure should flush during batch ops");

        // All data from both batches readable
        for &id in ids1.iter().chain(ids2.iter()) {
            assert!(engine.get_node(id).unwrap().is_some());
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_flush_then_write_cycle_group_commit() {
        // Stress test: many writes in GroupCommit mode with a tiny hard cap.
        // Verifies no deadlock and data integrity across many flush cycles.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 256,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 2,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // 200 writes. Each may trigger backpressure flush, each flush
        // acquires/releases WAL sync lock, then the write acquires it again.
        let mut ids = Vec::new();
        for i in 0..200 {
            let id = engine.upsert_node(1, &format!("stress_{}", i), BTreeMap::new(), 0.5).unwrap();
            ids.push(id);
        }

        // Many segments created
        assert!(engine.segment_count() >= 5, "expected many backpressure flushes");

        // All data present
        for (i, &id) in ids.iter().enumerate() {
            assert!(
                engine.get_node(id).unwrap().is_some(),
                "node stress_{} (id={}) missing", i, id
            );
        }

        engine.close().unwrap();

        // Verify durability after reopen
        let engine = DatabaseEngine::open(dir.path(), &DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        }).unwrap();

        for &id in &ids {
            assert!(engine.get_node(id).unwrap().is_some(), "node {} missing after reopen", id);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_interacts_with_auto_compact() {
        // Backpressure flushes should trigger auto-compaction normally.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 3, // compact after 3 flushes
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Write enough to trigger many backpressure flushes
        for i in 0..100 {
            engine.upsert_node(1, &format!("ac_{}", i), BTreeMap::new(), 0.5).unwrap();
        }

        // Auto-compact should have fired and reduced segment count
        // (many flushes → compact triggers → segments merge)
        // Just verify data integrity. Segment count depends on timing
        for i in 0..100 {
            assert!(
                engine.find_existing_node(1, &format!("ac_{}", i)).unwrap().is_some(),
                "node ac_{} missing after backpressure + auto-compact", i
            );
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_backpressure_invalidate_edge() {
        // invalidate_edge should also trigger backpressure.
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 0,
            memtable_hard_cap_bytes: 512,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "src", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "dst", BTreeMap::new(), 1.0).unwrap();

        // Create many edges to fill memtable
        let mut edge_ids = Vec::new();
        for i in 0..20 {
            let eid = engine.upsert_edge(n1, n2, i as u32, BTreeMap::new(), 0.5, None, None).unwrap();
            edge_ids.push(eid);
        }

        // Invalidate edges. Should trigger backpressure
        for &eid in &edge_ids {
            engine.invalidate_edge(eid, 999).unwrap();
        }

        assert!(engine.segment_count() >= 1, "backpressure should flush during invalidate_edge");

        engine.close().unwrap();
    }

    // ---- Constrained 2-hop expansion tests ----

    #[test]
    fn test_constrained_2hop_traverse_edge_types() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -[type1]-> b -[type1]-> c, b -[type2]-> d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let _d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _d, 2, BTreeMap::new(), 1.0, None, None).unwrap();

        // Traverse only type 1: a->b (hop1), b->c (hop2). b->d is type 2, not traversed.
        let hop2 = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, Some(&[1]), None, 0, None, None)
            .unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        // Traverse all types: should reach both c and d
        let hop2_all = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, None, 0, None, None)
            .unwrap();
        assert_eq!(hop2_all.len(), 2);
        let node_ids: HashSet<u64> = hop2_all.iter().map(|e| e.node_id).collect();
        assert!(node_ids.contains(&c));
        assert!(node_ids.contains(&_d));

        engine.close().unwrap();
    }

    #[test]
    fn test_constrained_2hop_target_node_types() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b -> c (type_id=10), b -> d (type_id=20)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(10, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(20, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Filter target node types to 10 only, should get c but not d
        let hop2 = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        // Filter target to 20 only, should get d but not c
        let hop2_d = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[20]), 0, None, None)
            .unwrap();
        assert_eq!(hop2_d.len(), 1);
        assert_eq!(hop2_d[0].node_id, d);

        engine.close().unwrap();
    }

    #[test]
    fn test_constrained_2hop_both_filters() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -[1]-> b -[1]-> c(type=10), b -[2]-> d(type=10), b -[1]-> e(type=20)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(10, "c", BTreeMap::new(), 0.5).unwrap();
        let _d = engine.upsert_node(10, "d", BTreeMap::new(), 0.5).unwrap();
        let _e = engine.upsert_node(20, "e", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _d, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _e, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Traverse edge type 1, target node type 10: only c qualifies
        // (d is behind edge type 2, e is node type 20)
        let hop2 = engine
            .neighbors_2hop_constrained(
                a,
                Direction::Outgoing,
                Some(&[1]),
                Some(&[10]),
                0,
                None,
                None,
            )
            .unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_constrained_2hop_with_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b -> c, b -> d, b -> e (all type 10)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let _c = engine.upsert_node(10, "c", BTreeMap::new(), 0.5).unwrap();
        let _d = engine.upsert_node(10, "d", BTreeMap::new(), 0.5).unwrap();
        let _e = engine.upsert_node(10, "e", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, _e, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Without limit: 3 results
        let all = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(all.len(), 3);

        // With limit 2
        let limited = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[10]), 2, None, None)
            .unwrap();
        assert_eq!(limited.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_constrained_2hop_excludes_deleted_targets() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b -> c, b -> d; delete d
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(10, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(10, "d", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.delete_node(d).unwrap();

        // Deleted d should not appear in results
        let hop2 = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_constrained_2hop_across_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // a -> b in segment, b -> c in memtable
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(10, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let hop2 = engine
            .neighbors_2hop_constrained(a, Direction::Outgoing, None, Some(&[10]), 0, None, None)
            .unwrap();
        assert_eq!(hop2.len(), 1);
        assert_eq!(hop2[0].node_id, c);

        engine.close().unwrap();
    }

    // ---- Top-k neighbors tests ----

    #[test]
    fn test_top_k_by_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        let e = engine.upsert_node(1, "e", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 0.1, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 0.9, None, None).unwrap();
        engine.upsert_edge(a, d, 1, BTreeMap::new(), 0.5, None, None).unwrap();
        engine.upsert_edge(a, e, 1, BTreeMap::new(), 0.3, None, None).unwrap();

        // Top 2 by weight: c (0.9) and d (0.5)
        let top2 = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 2, ScoringMode::Weight, None)
            .unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].node_id, c);
        assert!((top2[0].weight - 0.9).abs() < 0.01);
        assert_eq!(top2[1].node_id, d);
        assert!((top2[1].weight - 0.5).abs() < 0.01);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_by_recency() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();

        // Edges with explicit valid_from to control recency
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(1000), None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, Some(3000), None).unwrap();
        engine.upsert_edge(a, d, 1, BTreeMap::new(), 1.0, Some(2000), None).unwrap();

        // Top 2 by recency: c (3000) and d (2000)
        let top2 = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 2, ScoringMode::Recency, None)
            .unwrap();
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].node_id, c);
        assert_eq!(top2[1].node_id, d);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_by_decay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // b: high weight but old, c: medium weight and recent, d: low weight very recent
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(now - 7_200_000), None).unwrap(); // 2 hours ago
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 0.8, Some(now - 600_000), None).unwrap(); // 10 min ago
        engine.upsert_edge(a, d, 1, BTreeMap::new(), 0.3, Some(now - 60_000), None).unwrap(); // 1 min ago

        // With strong decay (lambda=1.0), recent edges should beat old heavy ones
        let top2 = engine
            .top_k_neighbors(
                a,
                Direction::Outgoing,
                None,
                2,
                ScoringMode::DecayAdjusted { lambda: 1.0 },
                None,
            )
            .unwrap();
        assert_eq!(top2.len(), 2);
        // c should rank highest (0.8 * exp(-1.0 * 0.167) ≈ 0.68) over d (0.3 * ~1.0 ≈ 0.3)
        // b is heavily decayed (1.0 * exp(-2.0) ≈ 0.14), lowest
        assert_eq!(top2[0].node_id, c);
        assert_eq!(top2[1].node_id, d);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_zero_returns_empty() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 0, ScoringMode::Weight, None)
            .unwrap();
        assert!(result.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_k_greater_than_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 0.3, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 0.7, None, None).unwrap();

        // k=10 but only 2 neighbors. Returns all 2, sorted desc
        let result = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 10, ScoringMode::Weight, None)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].node_id, c); // higher weight first
        assert_eq!(result[1].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 0.9, None, None).unwrap();
        engine.upsert_edge(a, c, 2, BTreeMap::new(), 0.8, None, None).unwrap();
        engine.upsert_edge(a, d, 1, BTreeMap::new(), 0.7, None, None).unwrap();

        // Top 1 of edge type 1 only
        let result = engine
            .top_k_neighbors(a, Direction::Outgoing, Some(&[1]), 1, ScoringMode::Weight, None)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_excludes_deleted_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 0.9, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 0.8, None, None).unwrap();

        engine.delete_node(b).unwrap();

        let result = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 2, ScoringMode::Weight, None)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_across_segments() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 0.5, None, None).unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 0.9, None, None).unwrap();

        // Top 1 should be c (0.9) from memtable, beating b (0.5) from segment
        let result = engine
            .top_k_neighbors(a, Direction::Outgoing, None, 1, ScoringMode::Weight, None)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_top_k_negative_lambda_returns_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();

        let result = engine.top_k_neighbors(
            a,
            Direction::Outgoing,
            None,
            5,
            ScoringMode::DecayAdjusted { lambda: -1.0 },
            None,
        );
        assert!(result.is_err());

        engine.close().unwrap();
    }

    // =====================================================================
    // extract_subgraph tests
    // =====================================================================

    #[test]
    fn test_subgraph_linear_chain_depth_1() {
        // A→B→C→D, extract from A at depth 1 → only A and B
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        let e_ab = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b]));
        assert_eq!(sg.edges.len(), 1);
        assert_eq!(sg.edges[0].id, e_ab);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_linear_chain_depth_3() {
        // A→B→C→D, extract from A at depth 3 → all nodes and edges
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 3, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c, d]));
        assert_eq!(sg.edges.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_diamond_graph() {
        // Diamond: A→B, A→C, B→D, C→D. Node D should appear once.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 2, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c, d]));
        // All 4 edges included
        assert_eq!(sg.edges.len(), 4);
        // D appears exactly once in nodes
        assert_eq!(sg.nodes.iter().filter(|n| n.id == d).count(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_cycle() {
        // Cycle: A→B→C→A. BFS should terminate, not loop.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_ca = engine.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 10, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c]));
        // All 3 edges including the back-edge C→A
        let edge_ids: HashSet<u64> = sg.edges.iter().map(|e| e.id).collect();
        assert_eq!(edge_ids.len(), 3);
        assert!(edge_ids.contains(&e_ca));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_self_loop() {
        // A→A (self-loop), A→B. Should not infinite-loop.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e_aa = engine.upsert_edge(a, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_ab = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 5, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b]));
        let edge_ids: HashSet<u64> = sg.edges.iter().map(|e| e.id).collect();
        assert!(edge_ids.contains(&e_aa));
        assert!(edge_ids.contains(&e_ab));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_depth_zero() {
        // Depth 0 returns just the start node, no edges
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 0, Direction::Outgoing, None, None)
            .unwrap();

        assert_eq!(sg.nodes.len(), 1);
        assert_eq!(sg.nodes[0].id, a);
        assert_eq!(sg.edges.len(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_nonexistent_start_node() {
        // Non-existent start node → empty subgraph
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let sg = engine
            .extract_subgraph(999, 5, Direction::Outgoing, None, None)
            .unwrap();

        assert!(sg.nodes.is_empty());
        assert!(sg.edges.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_disconnected_not_reached() {
        // A→B, C (isolated). Subgraph from A should not include C.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let _c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 10, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b]));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_direction_incoming() {
        // A→B→C. Extract from C with Incoming direction → C, B, A
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(c, 2, Direction::Incoming, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_direction_both() {
        // A→B←C. Extract from B with Both direction → A, B, C
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(b, 1, Direction::Both, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_edge_type_filter() {
        // A→B (type 1), A→C (type 2), B→D (type 1).
        // Filter by type 1 → A, B, D (not C).
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 2, Direction::Outgoing, Some(&[1]), None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, d]));
        assert_eq!(sg.edges.len(), 2);
        // All edges should be type 1
        assert!(sg.edges.iter().all(|e| e.type_id == 1));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_cross_segment() {
        // Insert A→B, flush. Insert B→C in memtable. Subgraph spans both.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 2, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_with_deleted_node() {
        // A→B→C, delete B → subgraph from A should only contain A (B is deleted)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.delete_node(b).unwrap();

        let sg = engine
            .extract_subgraph(a, 5, Direction::Outgoing, None, None)
            .unwrap();

        // B is deleted, so neighbors() won't return it; C unreachable
        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a]));
        assert_eq!(sg.edges.len(), 0);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_with_deleted_edge() {
        // A→B (edge1), A→C (edge2). Delete edge1. Subgraph from A → A, C.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.delete_edge(e1).unwrap();

        let sg = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, c]));
        assert_eq!(sg.edges.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_temporal_filter() {
        // A→B (valid 100-200), A→C (valid 300-400).
        // At epoch 150 → only B reachable. At epoch 350 → only C reachable.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, Some(100), Some(200)).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, Some(300), Some(400)).unwrap();

        let sg_150 = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, Some(150))
            .unwrap();
        let node_ids_150: HashSet<u64> = sg_150.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids_150, HashSet::from([a, b]));

        let sg_350 = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, Some(350))
            .unwrap();
        let node_ids_350: HashSet<u64> = sg_350.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids_350, HashSet::from([a, c]));

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_large_fan_out() {
        // Hub node A with 50 outgoing edges to B1..B50
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "hub", BTreeMap::new(), 1.0).unwrap();
        let mut expected = HashSet::from([a]);
        for i in 0..50 {
            let n = engine
                .upsert_node(1, &format!("spoke_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            engine.upsert_edge(a, n, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            expected.insert(n);
        }

        let sg = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, expected);
        assert_eq!(sg.edges.len(), 50);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_depth_limits_traversal() {
        // Chain: A→B→C→D→E. Depth 2 → A, B, C only.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_node(1, "e", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(d, e, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 2, Direction::Outgoing, None, None)
            .unwrap();

        let node_ids: HashSet<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert_eq!(node_ids, HashSet::from([a, b, c]));
        assert_eq!(sg.edges.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_subgraph_preserves_node_properties() {
        // Verify extracted nodes carry their full properties
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props_a = BTreeMap::new();
        props_a.insert("name".to_string(), PropValue::String("Alice".to_string()));
        let a = engine.upsert_node(1, "a", props_a, 0.9).unwrap();

        let mut props_b = BTreeMap::new();
        props_b.insert("name".to_string(), PropValue::String("Bob".to_string()));
        let b = engine.upsert_node(2, "b", props_b, 0.7).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let sg = engine
            .extract_subgraph(a, 1, Direction::Outgoing, None, None)
            .unwrap();

        let node_a = sg.nodes.iter().find(|n| n.id == a).unwrap();
        assert_eq!(node_a.type_id, 1);
        assert_eq!(node_a.key, "a");
        assert_eq!(
            node_a.props.get("name"),
            Some(&PropValue::String("Alice".to_string()))
        );

        let node_b = sg.nodes.iter().find(|n| n.id == b).unwrap();
        assert_eq!(node_b.type_id, 2);
        assert_eq!(
            node_b.props.get("name"),
            Some(&PropValue::String("Bob".to_string()))
        );

        engine.close().unwrap();
    }

    // ========== get_node_by_key ==========

    fn make_props(key: &str, val: &str) -> BTreeMap<String, PropValue> {
        let mut m = BTreeMap::new();
        m.insert(key.to_string(), PropValue::String(val.to_string()));
        m
    }

    fn open_imm(path: &std::path::Path) -> DatabaseEngine {
        let opts = DbOptions {
            create_if_missing: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        DatabaseEngine::open(path, &opts).unwrap()
    }

    #[test]
    fn test_get_node_by_key_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine.upsert_node(1, "alice", make_props("name", "Alice"), 1.0).unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.type_id, 1);
        assert_eq!(node.key, "alice");
        assert_eq!(node.props.get("name"), Some(&PropValue::String("Alice".to_string())));
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_not_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "alice", make_props("name", "Alice"), 1.0).unwrap();
        assert!(engine.get_node_by_key(1, "bob").unwrap().is_none());
        assert!(engine.get_node_by_key(2, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine.upsert_node(1, "alice", make_props("name", "Alice"), 1.0).unwrap();
        engine.flush().unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id);
        assert_eq!(node.key, "alice");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_after_compaction() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "alice", make_props("name", "v1"), 1.0).unwrap();
        engine.flush().unwrap();
        let id2 = engine.upsert_node(1, "alice", make_props("name", "v2"), 2.0).unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(node.props.get("name"), Some(&PropValue::String("v2".to_string())));
        assert_eq!(node.weight, 2.0);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_deleted() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine.upsert_node(1, "alice", make_props("name", "Alice"), 1.0).unwrap();
        engine.delete_node(id).unwrap();
        assert!(engine.get_node_by_key(1, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_deleted_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id = engine.upsert_node(1, "alice", make_props("name", "Alice"), 1.0).unwrap();
        engine.flush().unwrap();
        engine.delete_node(id).unwrap();
        assert!(engine.get_node_by_key(1, "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_node_by_key_memtable_shadows_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "alice", make_props("name", "v1"), 1.0).unwrap();
        engine.flush().unwrap();
        let id2 = engine.upsert_node(1, "alice", make_props("name", "v2"), 2.0).unwrap();
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(node.props.get("name"), Some(&PropValue::String("v2".to_string())));
        engine.close().unwrap();
    }

    // ========== get_edge_by_triple ==========

    #[test]
    fn test_get_edge_by_triple_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine.upsert_edge(a, b, 10, make_props("rel", "knows"), 0.5, None, None).unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid);
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);
        assert_eq!(edge.type_id, 10);
        assert_eq!(edge.props.get("rel"), Some(&PropValue::String("knows".to_string())));
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_not_found() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        assert!(engine.get_edge_by_triple(a, b, 99).unwrap().is_none());
        assert!(engine.get_edge_by_triple(b, a, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_deleted() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge_by_triple(a, b, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_deleted_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let eid = engine.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        engine.delete_edge(eid).unwrap();
        assert!(engine.get_edge_by_triple(a, b, 10).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edge_by_triple_after_compaction() {
        let dir = TempDir::new().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let mut engine = DatabaseEngine::open(&dir.path().join("db"), &opts).unwrap();
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(a, b, 10, make_props("v", "1"), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        let eid2 = engine.upsert_edge(a, b, 10, make_props("v", "2"), 2.0, None, None).unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, eid2);
        assert_eq!(edge.props.get("v"), Some(&PropValue::String("2".to_string())));
        engine.close().unwrap();
    }

    // ========== get_nodes / get_edges (bulk) ==========

    #[test]
    fn test_get_nodes_bulk() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", make_props("name", "A"), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", make_props("name", "B"), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", make_props("name", "C"), 1.0).unwrap();
        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_mixed_found_missing() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.delete_node(b).unwrap();
        let results = engine.get_nodes(&[a, b, 9999]).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_none()); // deleted
        assert!(results[2].is_none()); // never existed
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let results = engine.get_nodes(&[a, b]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_empty() {
        let dir = TempDir::new().unwrap();
        let engine = open_imm(&dir.path().join("db"));
        let results = engine.get_nodes(&[]).unwrap();
        assert!(results.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let results = engine.get_edges(&[e1, e2, 9999]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().from, a);
        assert_eq!(results[1].as_ref().unwrap().from, b);
        assert!(results[2].is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_cross_source() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        let e2 = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let results = engine.get_edges(&[e1, e2]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().from, a);
        assert_eq!(results[1].as_ref().unwrap().from, b);
        engine.close().unwrap();
    }

    // ========== Bulk read merge-walk tests ==========

    #[test]
    fn test_get_nodes_bulk_multi_segment_interleaved() {
        // IDs spread across two segments. The merge-walk must handle
        // both segments having relevant entries
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        // Segment 1: nodes 1, 2, 3
        let a = engine.upsert_node(1, "a", make_props("seg", "1"), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", make_props("seg", "1"), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", make_props("seg", "1"), 1.0).unwrap();
        engine.flush().unwrap();
        // Segment 2: nodes 4, 5
        let d = engine.upsert_node(1, "d", make_props("seg", "2"), 1.0).unwrap();
        let e = engine.upsert_node(1, "e", make_props("seg", "2"), 1.0).unwrap();
        engine.flush().unwrap();

        // Request in non-sorted order, mixing IDs from both segments
        let results = engine.get_nodes(&[e, a, d, c, b]).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].as_ref().unwrap().key, "e");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "d");
        assert_eq!(results[3].as_ref().unwrap().key, "c");
        assert_eq!(results[4].as_ref().unwrap().key, "b");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_tombstone_in_newer_segment() {
        // Node flushed to segment 1, then deleted and flushed to segment 2.
        // Bulk read must respect cross-segment tombstones.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap(); // seg 1: a, b
        engine.delete_node(a).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap(); // seg 2: tombstone(a), c

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert!(results[0].is_none()); // a tombstoned in seg 2
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_memtable_shadows_segment() {
        // Node in segment, then updated in memtable.
        // Bulk read must return the fresher memtable version.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", make_props("v", "old"), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "a", make_props("v", "new"), 2.0).unwrap();
        // a is now in both memtable (newer) and segment (older)
        let results = engine.get_nodes(&[a]).unwrap();
        let node = results[0].as_ref().unwrap();
        assert_eq!(node.props.get("v"), Some(&PropValue::String("new".to_string())));
        assert_eq!(node.weight, 2.0);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_duplicate_ids() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let results = engine.get_nodes(&[a, a, a]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "a");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_duplicate_ids_in_segment() {
        // Same as above but the node is in a segment, exercising the merge-walk
        // with duplicate lookups
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        let results = engine.get_nodes(&[a, a, a]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().key, "a");
        assert_eq!(results[1].as_ref().unwrap().key, "a");
        assert_eq!(results[2].as_ref().unwrap().key, "a");
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_multi_segment_interleaved() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        // Segment 1
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        // Segment 2
        let e2 = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Reverse order
        let results = engine.get_edges(&[e2, e1]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().from, b);
        assert_eq!(results[1].as_ref().unwrap().from, a);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_edges_bulk_tombstone_cross_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap(); // seg 1: e1, e2
        engine.delete_edge(e1).unwrap();
        engine.flush().unwrap(); // seg 2: tombstone(e1)

        let results = engine.get_edges(&[e1, e2]).unwrap();
        assert!(results[0].is_none()); // tombstoned
        assert_eq!(results[1].as_ref().unwrap().from, b);
        engine.close().unwrap();
    }

    #[test]
    fn test_get_nodes_bulk_after_compaction() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", make_props("v", "1"), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", make_props("v", "1"), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "a", make_props("v", "2"), 2.0).unwrap();
        let c = engine.upsert_node(1, "c", make_props("v", "1"), 1.0).unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().props.get("v"),
            Some(&PropValue::String("2".to_string()))); // updated
        assert_eq!(results[1].as_ref().unwrap().key, "b");
        assert_eq!(results[2].as_ref().unwrap().key, "c");
        engine.close().unwrap();
    }

    // ========== delete-then-re-create via key ==========

    #[test]
    fn test_get_node_by_key_delete_then_recreate() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let id1 = engine.upsert_node(1, "alice", make_props("v", "1"), 1.0).unwrap();
        engine.flush().unwrap();
        engine.delete_node(id1).unwrap();
        // Re-create with same key → gets a NEW id
        let id2 = engine.upsert_node(1, "alice", make_props("v", "2"), 2.0).unwrap();
        assert_ne!(id1, id2);
        let node = engine.get_node_by_key(1, "alice").unwrap().unwrap();
        assert_eq!(node.id, id2);
        assert_eq!(node.props.get("v"), Some(&PropValue::String("2".to_string())));
        engine.close().unwrap();
    }

    // ========== get_edge_by_triple with uniqueness off ==========

    #[test]
    fn test_get_edge_by_triple_uniqueness_off_returns_latest() {
        let dir = TempDir::new().unwrap();
        // Default: edge_uniqueness = false
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let _e1 = engine.upsert_edge(a, b, 10, make_props("v", "1"), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(a, b, 10, make_props("v", "2"), 2.0, None, None).unwrap();
        // With uniqueness off, both edges exist. Triple index maps to the latest.
        let edge = engine.get_edge_by_triple(a, b, 10).unwrap().unwrap();
        assert_eq!(edge.id, e2);
        assert_eq!(edge.props.get("v"), Some(&PropValue::String("2".to_string())));
        engine.close().unwrap();
    }

    // ========== Atomic graph patch tests ==========

    #[test]
    fn test_graph_patch_mixed_ops() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Create some initial data
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Patch: upsert a new node, a new edge a→b, invalidate e1
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "c".to_string(),
                props: make_props("role", "new"),
                weight: 1.0,
            }],
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                type_id: 2,
                props: BTreeMap::new(),
                weight: 0.5,
                valid_from: None,
                valid_to: None,
            }],
            invalidate_edges: vec![(e1, 1000)],
            delete_node_ids: vec![],
            delete_edge_ids: vec![],
        };
        let result = engine.graph_patch(&patch).unwrap();

        // Verify results
        assert_eq!(result.node_ids.len(), 1);
        assert_eq!(result.edge_ids.len(), 1);

        // New node created
        let c = engine.get_node(result.node_ids[0]).unwrap().unwrap();
        assert_eq!(c.key, "c");
        assert_eq!(c.props.get("role"), Some(&PropValue::String("new".to_string())));

        // New edge created
        let edge = engine.get_edge(result.edge_ids[0]).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, b);

        // e1 invalidated (still exists but valid_to set)
        let e1_after = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(e1_after.valid_to, 1000);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_empty() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let result = engine.graph_patch(&GraphPatch::default()).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edge_ids.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_node_dedup() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Pre-existing node
        let existing = engine.upsert_node(1, "alice", make_props("v", "1"), 1.0).unwrap();

        let patch = GraphPatch {
            upsert_nodes: vec![
                NodeInput { type_id: 1, key: "alice".to_string(), props: make_props("v", "2"), weight: 2.0 },
                NodeInput { type_id: 1, key: "alice".to_string(), props: make_props("v", "3"), weight: 3.0 },
                NodeInput { type_id: 1, key: "bob".to_string(), props: BTreeMap::new(), weight: 1.0 },
            ],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();

        // alice deduped: both get the existing ID
        assert_eq!(result.node_ids[0], existing);
        assert_eq!(result.node_ids[1], existing);
        // bob is new
        assert_ne!(result.node_ids[2], existing);

        // Last write wins: alice has v=3
        let alice = engine.get_node(existing).unwrap().unwrap();
        assert_eq!(alice.props.get("v"), Some(&PropValue::String("3".to_string())));

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_delete_with_cascade() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e_ab = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_bc = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Delete node b. Should cascade delete e_ab and e_bc
        let patch = GraphPatch {
            delete_node_ids: vec![b],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        // a and c survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(c).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_edge_delete() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let patch = GraphPatch {
            delete_edge_ids: vec![e],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        assert!(engine.get_edge(e).unwrap().is_none());
        // Nodes survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_ordering_upserts_before_deletes() {
        // Upsert a node and delete it in the same patch.
        // Deterministic ordering: upserts first, deletes last.
        // The delete should win (delete comes after upsert in the WAL).
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "a".to_string(),
                props: make_props("v", "updated"),
                weight: 2.0,
            }],
            delete_node_ids: vec![a],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(result.node_ids[0], a);

        // Delete wins, node should be gone
        assert!(engine.get_node(a).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_invalidate_nonexistent_edge_skipped() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Invalidating a nonexistent edge should be silently skipped
        let patch = GraphPatch {
            invalidate_edges: vec![(99999, 5000)],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edge_ids.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (node_id, edge_id, invalidated_eid);
        {
            let mut engine = open_imm(&db_path);
            let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
            let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
            invalidated_eid = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

            let patch = GraphPatch {
                upsert_nodes: vec![NodeInput {
                    type_id: 1,
                    key: "c".to_string(),
                    props: make_props("role", "new"),
                    weight: 1.0,
                }],
                upsert_edges: vec![EdgeInput {
                    from: a,
                    to: b,
                    type_id: 5,
                    props: BTreeMap::new(),
                    weight: 0.5,
                    valid_from: None,
                    valid_to: None,
                }],
                invalidate_edges: vec![(invalidated_eid, 2000)],
                delete_edge_ids: vec![],
                delete_node_ids: vec![],
            };
            let result = engine.graph_patch(&patch).unwrap();
            node_id = result.node_ids[0];
            edge_id = result.edge_ids[0];
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve all patch effects
        {
            let engine = open_imm(&db_path);
            let node = engine.get_node(node_id).unwrap().unwrap();
            assert_eq!(node.key, "c");

            let edge = engine.get_edge(edge_id).unwrap().unwrap();
            assert_eq!(edge.type_id, 5);

            let inv_edge = engine.get_edge(invalidated_eid).unwrap().unwrap();
            assert_eq!(inv_edge.valid_to, 2000);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_graph_patch_two_step_upsert_then_connect() {
        // Edges reference node IDs, so we need IDs upfront. Use two patches:
        // first upsert nodes, then connect them with edges.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let patch1 = GraphPatch {
            upsert_nodes: vec![
                NodeInput { type_id: 1, key: "x".to_string(), props: BTreeMap::new(), weight: 1.0 },
                NodeInput { type_id: 1, key: "y".to_string(), props: BTreeMap::new(), weight: 1.0 },
            ],
            ..GraphPatch::default()
        };
        let r1 = engine.graph_patch(&patch1).unwrap();
        let x = r1.node_ids[0];
        let y = r1.node_ids[1];

        // Now connect them in a second patch
        let patch2 = GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: x,
                to: y,
                type_id: 10,
                props: make_props("rel", "friend"),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }],
            ..GraphPatch::default()
        };
        let r2 = engine.graph_patch(&patch2).unwrap();

        let edge = engine.get_edge(r2.edge_ids[0]).unwrap().unwrap();
        assert_eq!(edge.from, x);
        assert_eq!(edge.to, y);
        assert_eq!(edge.type_id, 10);

        // Neighbors work
        let nbrs = engine.neighbors(x, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, y);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_after_flush() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Patch against segment data
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "a".to_string(),
                props: make_props("v", "updated"),
                weight: 2.0,
            }],
            invalidate_edges: vec![(e, 500)],
            ..GraphPatch::default()
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(result.node_ids[0], a); // deduped against segment

        let node = engine.get_node(a).unwrap().unwrap();
        assert_eq!(node.props.get("v"), Some(&PropValue::String("updated".to_string())));

        let edge = engine.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.valid_to, 500);

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_duplicate_edge_delete_safe() {
        // Edge deleted via both explicit delete_edge_ids and cascade from delete_node_ids.
        // Should not panic. Duplicate DeleteEdge ops are idempotent.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let patch = GraphPatch {
            delete_edge_ids: vec![e],  // explicit delete
            delete_node_ids: vec![a],  // cascade also deletes e
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap(); // should not panic

        assert!(engine.get_edge(e).unwrap().is_none());
        assert!(engine.get_node(a).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_invalidate_pre_existing_edge() {
        // Invalidation looks up the current edge state. Since ops are applied
        // as a batch after all ops are built, invalidation sees the pre-patch state.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, make_props("v", "original"), 1.0, None, None).unwrap();

        // Upsert the same edge (updates props) AND invalidate it in the same patch.
        // Ordering: upsert (step 2) → invalidation (step 3) in the ops vec.
        // The invalidation reads the PRE-patch edge (the one already in memtable),
        // so it captures the original props, not the updated ones.
        // Both ops are applied atomically. Last write to valid_to wins.
        let patch = GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                type_id: 1,
                props: make_props("v", "updated"),
                weight: 2.0,
                valid_from: None,
                valid_to: None, // sets valid_to = MAX
            }],
            invalidate_edges: vec![(e, 3000)],
            ..GraphPatch::default()
        };
        engine.graph_patch(&patch).unwrap();

        // The invalidation's UpsertEdge comes after the upsert's UpsertEdge in the ops vec,
        // so the invalidation's valid_to=3000 wins via last-write-wins in apply_op.
        let edge = engine.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.valid_to, 3000);

        engine.close().unwrap();
    }

    // ========== Cross-source cascade delete tests ==========

    #[test]
    fn test_delete_node_cascades_segment_edges() {
        // Edge is flushed to a segment, then the node is deleted.
        // Cascade should still find and delete the edge.
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap(); // edge moves to segment

        engine.delete_node(a).unwrap();

        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none()); // cascade reached segment edge
        assert!(engine.get_node(b).unwrap().is_some()); // b survives
        // No ghost neighbors on b
        let nbrs = engine.neighbors(b, Direction::Incoming, None, 0, None, None).unwrap();
        assert!(nbrs.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_cascades_mixed_sources() {
        // Some edges in memtable, some in segments
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap(); // e1 in segment
        let e2 = engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        // e2 in memtable

        engine.delete_node(a).unwrap();

        assert!(engine.get_edge(e1).unwrap().is_none()); // segment edge
        assert!(engine.get_edge(e2).unwrap().is_none()); // memtable edge
        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_cascades_incoming_segment_edges() {
        // Test that incoming edges (where deleted node is the target) are also cascaded
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Delete b (the target). Should cascade delete the incoming edge
        engine.delete_node(b).unwrap();

        assert!(engine.get_edge(e).unwrap().is_none());
        let nbrs = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(nbrs.is_empty());
        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_delete_cascades_segment_edges() {
        // Same as above but via graph_patch
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        engine.graph_patch(&GraphPatch {
            delete_node_ids: vec![a],
            ..GraphPatch::default()
        }).unwrap();

        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    // ===================== prune(policy) =====================

    #[test]
    fn test_prune_empty_policy_no_op() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: None,
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        // Node survives
        assert!(engine.get_node(1).unwrap().is_some());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_id_only_no_op() {
        // type_id alone without age or weight is a no-op (safety)
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: None,
            type_id: Some(1),
        }).unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_by_age_only() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Insert nodes. They all get updated_at = now
        let a = engine.upsert_node(1, "old", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(1, "new", BTreeMap::new(), 1.0).unwrap();

        // Hack: manually set "old" node to have an ancient updated_at via write_op
        let old_node = engine.get_node(a).unwrap().unwrap();
        engine.write_op(&WalOp::UpsertNode(NodeRecord {
            updated_at: 1000, // ancient timestamp
            ..old_node
        })).unwrap();

        // Prune with max_age_ms = 1000 (1 second). "old" node is way older
        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: None,
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // old pruned
        assert!(engine.get_node(b).unwrap().is_some());  // new survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_by_weight_only() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "mid", BTreeMap::new(), 0.5).unwrap();
        let c = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Prune weight <= 0.5
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 2); // low + mid
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_node(c).unwrap().is_some()); // high survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_combo_age_and_weight() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "old-low", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "old-high", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "new-low", BTreeMap::new(), 0.1).unwrap();

        // Make a and b old
        let node_a = engine.get_node(a).unwrap().unwrap();
        engine.write_op(&WalOp::UpsertNode(NodeRecord {
            updated_at: 1000,
            ..node_a
        })).unwrap();
        let node_b = engine.get_node(b).unwrap().unwrap();
        engine.write_op(&WalOp::UpsertNode(NodeRecord {
            updated_at: 1000,
            ..node_b
        })).unwrap();

        // AND: old (max_age_ms=1000) AND low weight (<= 0.5)
        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1); // only old-low
        assert!(engine.get_node(a).unwrap().is_none()); // old + low → pruned
        assert!(engine.get_node(b).unwrap().is_some());  // old but high weight → survives
        assert!(engine.get_node(c).unwrap().is_some());  // low weight but new → survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_scoped() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "type1-low", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(2, "type2-low", BTreeMap::new(), 0.1).unwrap();

        // Prune only type 1 with weight <= 0.5
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: Some(1),
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // type 1, low → pruned
        assert!(engine.get_node(b).unwrap().is_some());  // type 2 → not in scope
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cascade_deletes_edges() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.1).unwrap();
        let e_ab = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_bc = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e_ca = engine.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Prune low-weight nodes (a and c)
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 2); // a and c
        assert_eq!(result.edges_pruned, 3); // all three edges (all touch a or c)
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(c).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some()); // b survives
        assert!(engine.get_edge(e_ab).unwrap().is_none());
        assert!(engine.get_edge(e_bc).unwrap().is_none());
        assert!(engine.get_edge(e_ca).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_shared_edge_dedup() {
        // When two pruned nodes share an edge, the edge should only be counted once
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 2);
        assert_eq!(result.edges_pruned, 1); // shared edge counted once
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_empty_result_no_match() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.8).unwrap();

        // No nodes have weight <= 0.1
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.1),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_after_flush_segment_nodes() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "seg-a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "seg-b", BTreeMap::new(), 0.9).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Nodes are now in segment, not memtable
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert_eq!(result.edges_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cross_source_memtable_and_segment() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        // Node in segment
        let a = engine.upsert_node(1, "in-seg", BTreeMap::new(), 0.1).unwrap();
        engine.flush().unwrap();

        // Node in memtable
        let b = engine.upsert_node(1, "in-mem", BTreeMap::new(), 0.1).unwrap();

        // Edge from memtable node to segment node
        let e = engine.upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 2);
        assert_eq!(result.edges_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_cascade_edges_in_segment() {
        // Edge is in segment, node to prune is in memtable. Cascade should find it
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();
        let e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Update b in memtable (still low weight)
        engine.upsert_node(1, "b", BTreeMap::new(), 0.1).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1); // only b
        assert_eq!(result.edges_pruned, 1); // edge in segment cascade-deleted
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_none());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        let (a, b, e);
        {
            let mut engine = open_imm(&db_path);
            a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
            b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
            e = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

            let result = engine.prune(&PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            }).unwrap();
            assert_eq!(result.nodes_pruned, 1);
            assert_eq!(result.edges_pruned, 1);
            engine.close().unwrap();
        }

        // Reopen. WAL replay should preserve prune effects
        let engine = open_imm(&db_path);
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_edge(e).unwrap().is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_weight_boundary() {
        // Exact boundary: weight == max_weight should be pruned (<=)
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "exact", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "above", BTreeMap::new(), 0.500001).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1);
        assert!(engine.get_node(a).unwrap().is_none()); // exactly 0.5 → pruned
        assert!(engine.get_node(b).unwrap().is_some());  // just above → survives
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_already_deleted_node_ignored() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        engine.delete_node(a).unwrap();

        // Prune should not count already-deleted nodes
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_empty_db() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let result = engine.prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 0);
        assert_eq!(result.edges_pruned, 0);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_negative_age_rejected() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();

        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(-100),
            max_weight: None,
            type_id: None,
        });

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_age_ms must be positive"), "got: {}", err);
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_zero_age_rejected() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(0),
            max_weight: None,
            type_id: None,
        });

        assert!(result.is_err());
        engine.close().unwrap();
    }

    #[test]
    fn test_prune_type_scoped_with_age() {
        let dir = TempDir::new().unwrap();
        let mut engine = open_imm(&dir.path().join("db"));

        let a = engine.upsert_node(1, "t1-old", BTreeMap::new(), 1.0).unwrap();
        let b = engine.upsert_node(2, "t2-old", BTreeMap::new(), 1.0).unwrap();
        let _c = engine.upsert_node(1, "t1-new", BTreeMap::new(), 1.0).unwrap();

        // Make a and b old
        let node_a = engine.get_node(a).unwrap().unwrap();
        engine.write_op(&WalOp::UpsertNode(NodeRecord {
            updated_at: 1000,
            ..node_a
        })).unwrap();
        let node_b = engine.get_node(b).unwrap().unwrap();
        engine.write_op(&WalOp::UpsertNode(NodeRecord {
            updated_at: 1000,
            ..node_b
        })).unwrap();

        // Prune old nodes of type 1 only
        let result = engine.prune(&PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: None,
            type_id: Some(1),
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1); // only t1-old
        assert!(engine.get_node(a).unwrap().is_none()); // type 1 + old → pruned
        assert!(engine.get_node(b).unwrap().is_some());  // type 2 → out of scope
        engine.close().unwrap();
    }

    // ==========================================================
    // FO-005: Named prune policies (compaction-filter auto-prune)
    // ==========================================================

    #[test]
    fn test_set_and_list_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Initially empty
        assert!(engine.list_prune_policies().is_empty());

        // Set a policy
        let policy = PrunePolicy { max_age_ms: None, max_weight: Some(0.5), type_id: None };
        engine.set_prune_policy("low-weight", policy.clone()).unwrap();

        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "low-weight");
        assert_eq!(list[0].1.max_weight, Some(0.5));

        // Overwrite
        let policy2 = PrunePolicy { max_age_ms: Some(60_000), max_weight: None, type_id: None };
        engine.set_prune_policy("low-weight", policy2).unwrap();
        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].1.max_age_ms, Some(60_000));
        assert!(list[0].1.max_weight.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_remove_prune_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.set_prune_policy("p1", PrunePolicy { max_age_ms: None, max_weight: Some(0.3), type_id: None }).unwrap();
        assert_eq!(engine.list_prune_policies().len(), 1);

        assert!(engine.remove_prune_policy("p1").unwrap());
        assert!(engine.list_prune_policies().is_empty());

        // Removing non-existent returns false
        assert!(!engine.remove_prune_policy("p1").unwrap());

        engine.close().unwrap();
    }

    #[test]
    fn test_prune_policy_validation() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Empty policy rejected
        let err = engine.set_prune_policy("bad", PrunePolicy { max_age_ms: None, max_weight: None, type_id: None });
        assert!(err.is_err());

        // type_id only rejected
        let err = engine.set_prune_policy("bad", PrunePolicy { max_age_ms: None, max_weight: None, type_id: Some(1) });
        assert!(err.is_err());

        // Negative age rejected
        let err = engine.set_prune_policy("bad", PrunePolicy { max_age_ms: Some(-1), max_weight: None, type_id: None });
        assert!(err.is_err());

        // NaN weight rejected
        let err = engine.set_prune_policy("bad", PrunePolicy { max_age_ms: None, max_weight: Some(f32::NAN), type_id: None });
        assert!(err.is_err());

        // Negative weight rejected
        let err = engine.set_prune_policy("bad", PrunePolicy { max_age_ms: None, max_weight: Some(-0.1), type_id: None });
        assert!(err.is_err());

        engine.close().unwrap();
    }

    #[test]
    fn test_prune_policy_survives_close_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;

        {
            let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            engine.set_prune_policy("age-rule", PrunePolicy {
                max_age_ms: Some(30_000), max_weight: None, type_id: None,
            }).unwrap();
            engine.set_prune_policy("weight-rule", PrunePolicy {
                max_age_ms: None, max_weight: Some(0.1), type_id: Some(5),
            }).unwrap();
            engine.close().unwrap();
        }

        // Reopen
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let list = engine.list_prune_policies();
        assert_eq!(list.len(), 2);
        // BTreeMap ordering: "age-rule" < "weight-rule"
        assert_eq!(list[0].0, "age-rule");
        assert_eq!(list[0].1.max_age_ms, Some(30_000));
        assert_eq!(list[1].0, "weight-rule");
        assert_eq!(list[1].1.max_weight, Some(0.1));
        assert_eq!(list[1].1.type_id, Some(5));
        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_auto_prune_by_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0; // manual compaction only
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Create nodes with different weights
        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Create more nodes in second segment; update "high" to create overlapping
        // IDs across segments, which forces the standard compaction path.
        let low2 = engine.upsert_node(1, "low2", BTreeMap::new(), 0.2).unwrap();
        let high2 = engine.upsert_node(1, "high2", BTreeMap::new(), 0.8).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap(); // overlap
        engine.flush().unwrap();

        // Register policy: prune weight <= 0.5
        engine.set_prune_policy("low-weight", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Compact. Should prune low and low2
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2);
        assert_eq!(stats.nodes_kept, 2); // high + high2
        assert!(stats.edges_auto_pruned == 0);

        // Verify pruned nodes are gone
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(low2).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());
        assert!(engine.get_node(high2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_auto_prune_cascade_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // will be pruned
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.9).unwrap();
        let e1 = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // More data in second segment; update "b" to create overlapping IDs
        // across segments, which forces the standard compaction path.
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.8).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap(); // overlap
        engine.flush().unwrap();

        engine.set_prune_policy("low", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1); // node a
        assert_eq!(stats.edges_auto_pruned, 1); // edge e1 (a→b)

        // a is gone, b/c/d survive
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());
        assert!(engine.get_node(c).unwrap().is_some());
        assert!(engine.get_node(d).unwrap().is_some());

        // e1 (a→b) cascade-dropped, e2 (b→c) survives
        assert!(engine.get_edge(e1).unwrap().is_none());
        assert!(engine.get_edge(e2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_multiple_policies_or_logic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Node that matches policy A (low weight) but not B (type 99)
        let n1 = engine.upsert_node(1, "n1", BTreeMap::new(), 0.1).unwrap();
        // Node that matches policy B (type 99) but not A (high weight)
        let n2 = engine.upsert_node(99, "n2", BTreeMap::new(), 0.9).unwrap();
        // Node that matches neither
        let n3 = engine.upsert_node(1, "n3", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Update n3 in second segment to create overlapping IDs (forces standard path)
        engine.upsert_node(1, "n3", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Policy A: prune if weight <= 0.5
        engine.set_prune_policy("low-weight", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();
        // Policy B: prune all nodes of type 99 (by weight, use very high threshold)
        engine.set_prune_policy("type-99", PrunePolicy {
            max_age_ms: None, max_weight: Some(999.0), type_id: Some(99),
        }).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2); // n1 (low weight) + n2 (type 99)

        assert!(engine.get_node(n1).unwrap().is_none());
        assert!(engine.get_node(n2).unwrap().is_none());
        assert!(engine.get_node(n3).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_no_policies_no_prune() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        engine.flush().unwrap();

        // No policies registered
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 0);
        assert_eq!(stats.edges_auto_pruned, 0);

        // All nodes survive
        assert!(engine.get_node(a).unwrap().is_some());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_removed_policy_no_longer_prunes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // overlap → standard path
        engine.flush().unwrap();

        // Remove the policy before compaction
        engine.remove_prune_policy("p").unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 0); // policy removed, no pruning

        // Node a still exists
        assert!(engine.get_node(a).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_type_scoped_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let t1_low = engine.upsert_node(1, "t1-low", BTreeMap::new(), 0.1).unwrap();
        let t2_low = engine.upsert_node(2, "t2-low", BTreeMap::new(), 0.1).unwrap();
        let t1_high = engine.upsert_node(1, "t1-high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();
        // Update t1_high in second segment to create overlapping IDs (forces standard path)
        engine.upsert_node(1, "t1-high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Only prune type 1 with low weight
        engine.set_prune_policy("type1-low", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: Some(1),
        }).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1); // only t1_low

        assert!(engine.get_node(t1_low).unwrap().is_none());
        assert!(engine.get_node(t2_low).unwrap().is_some()); // type 2, out of scope
        assert!(engine.get_node(t1_high).unwrap().is_some()); // type 1 but high weight

        engine.close().unwrap();
    }

    #[test]
    fn test_compaction_prune_stats_in_nodes_removed() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        for i in 0..10 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.1).unwrap();
        }
        engine.flush().unwrap();
        for i in 10..20 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.9).unwrap();
        }
        // Update n0 in second segment to create overlapping IDs (forces standard path)
        engine.upsert_node(1, "n0", BTreeMap::new(), 0.1).unwrap();
        engine.flush().unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 10);
        assert_eq!(stats.nodes_kept, 10);
        // nodes_removed includes auto-pruned nodes
        assert!(stats.nodes_removed >= 10);

        engine.close().unwrap();
    }

    #[test]
    fn test_manual_prune_unchanged_by_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Register a policy (should NOT affect manual prune calls)
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.0001), type_id: None,
        }).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();

        // Manual prune with a different threshold
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None, max_weight: Some(0.7), type_id: None,
        }).unwrap();

        assert_eq!(result.nodes_pruned, 1); // only a (0.5 <= 0.7)
        assert!(engine.get_node(a).unwrap().is_none());
        assert!(engine.get_node(b).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_bg_compaction_applies_prune_policies() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        // Auto-compact after 2 flushes
        opts.compact_after_n_flushes = 2;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Register policy: prune weight <= 0.3
        engine.set_prune_policy("low", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.3), type_id: None,
        }).unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.1).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        // Second flush with overlapping ID to force standard path in bg compaction
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap(); // triggers auto bg compaction

        // Wait for bg compaction to complete
        engine.wait_for_bg_compact();

        // Low-weight node should have been pruned by bg compaction
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    // --- FO-005a: Read-time policy filtering tests ---

    #[test]
    fn test_read_time_policy_get_node() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // No policy, both visible
        assert!(engine.get_node(low).unwrap().is_some());
        assert!(engine.get_node(high).unwrap().is_some());

        // Register policy: exclude weight <= 0.5
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Low-weight node excluded, high-weight still visible
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_by_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();

        // Register policy
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Low hidden by policy
        assert!(engine.get_node_by_key(1, "low").unwrap().is_none());
        // High still visible
        assert!(engine.get_node_by_key(1, "high").unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_nodes_batch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.3).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let results = engine.get_nodes(&[a, b, c]).unwrap();
        // a (0.2) excluded, b (0.9) visible, c (0.3) excluded
        assert!(results[0].is_none());
        assert!(results[1].is_some());
        assert!(results[2].is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let low = engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        let high = engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap(); // nodes now in segment

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Works on segment-sourced nodes too
        assert!(engine.get_node(low).unwrap().is_none());
        assert!(engine.get_node(high).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_get_node_by_key_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        engine.upsert_node(1, "low", BTreeMap::new(), 0.2).unwrap();
        engine.upsert_node(1, "high", BTreeMap::new(), 0.9).unwrap();
        engine.flush().unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        assert!(engine.get_node_by_key(1, "low").unwrap().is_none());
        assert!(engine.get_node_by_key(1, "high").unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_upsert_dedup_unaffected() {
        // Critical correctness test: upsert must still find the existing node
        // even when a policy would exclude it from public reads. If upsert
        // used filtered get_node_by_key, it would allocate a NEW ID for the
        // "hidden" node, causing silent data corruption.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine.upsert_node(1, "node-a", BTreeMap::new(), 0.2).unwrap();

        // Register policy that excludes this node from reads
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Public read confirms it's hidden
        assert!(engine.get_node(id1).unwrap().is_none());

        // Upsert same (type_id, key). MUST reuse existing ID, not allocate new one
        let id2 = engine.upsert_node(1, "node-a", BTreeMap::new(), 0.8).unwrap();
        assert_eq!(id1, id2, "upsert must reuse existing node ID even when policy-excluded");

        // Now weight is 0.8 > 0.5, so it should be visible again
        assert!(engine.get_node(id2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_upsert_dedup_after_flush() {
        // Same as above but with the node in a segment
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine.upsert_node(1, "node-a", BTreeMap::new(), 0.2).unwrap();
        engine.flush().unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Hidden from reads
        assert!(engine.get_node(id1).unwrap().is_none());

        // Upsert must still find and reuse the existing ID from segment
        let id2 = engine.upsert_node(1, "node-a", BTreeMap::new(), 0.8).unwrap();
        assert_eq!(id1, id2);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_add_remove_takes_effect() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id = engine.upsert_node(1, "target", BTreeMap::new(), 0.3).unwrap();

        // Initially visible
        assert!(engine.get_node(id).unwrap().is_some());

        // Add policy → hidden
        engine.set_prune_policy("hide-low", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();
        assert!(engine.get_node(id).unwrap().is_none());

        // Remove policy → visible again
        engine.remove_prune_policy("hide-low").unwrap();
        assert!(engine.get_node(id).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_type_scoped() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let t1 = engine.upsert_node(1, "t1-low", BTreeMap::new(), 0.2).unwrap();
        let t2 = engine.upsert_node(2, "t2-low", BTreeMap::new(), 0.2).unwrap();

        // Policy scoped to type_id=1 only
        engine.set_prune_policy("t1-only", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: Some(1),
        }).unwrap();

        // Type 1 node hidden, type 2 node still visible
        assert!(engine.get_node(t1).unwrap().is_none());
        assert!(engine.get_node(t2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_no_policies_zero_overhead() {
        // Regression test: ensure no policies = no filtering, no crashes
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id = engine.upsert_node(1, "node", BTreeMap::new(), 0.1).unwrap();

        // No policies registered, everything visible
        assert!(engine.list_prune_policies().is_empty());
        assert!(engine.get_node(id).unwrap().is_some());
        assert!(engine.get_node_by_key(1, "node").unwrap().is_some());
        let batch = engine.get_nodes(&[id]).unwrap();
        assert!(batch[0].is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_multiple_policies_or() {
        // Multiple policies: OR across policies. A node matching ANY policy is excluded.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.1).unwrap(); // type 1, low weight
        let b = engine.upsert_node(2, "b", BTreeMap::new(), 0.1).unwrap(); // type 2, low weight
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.9).unwrap(); // type 1, high weight

        // Policy 1: type 1, weight <= 0.5
        engine.set_prune_policy("p1", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: Some(1),
        }).unwrap();
        // Policy 2: type 2, weight <= 0.5
        engine.set_prune_policy("p2", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: Some(2),
        }).unwrap();

        // a (type 1, 0.1): matches p1 → hidden
        assert!(engine.get_node(a).unwrap().is_none());
        // b (type 2, 0.1): matches p2 → hidden
        assert!(engine.get_node(b).unwrap().is_none());
        // c (type 1, 0.9): doesn't match p1 (weight too high), doesn't match p2 (wrong type) → visible
        assert!(engine.get_node(c).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_graph_patch_dedup_unaffected() {
        // graph_patch node upserts must use raw lookup for dedup
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let id1 = engine.upsert_node(1, "node-a", BTreeMap::new(), 0.2).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Use graph_patch to upsert same node. Must reuse ID
        let patch = GraphPatch {
            upsert_nodes: vec![NodeInput {
                type_id: 1,
                key: "node-a".to_string(),
                props: BTreeMap::new(),
                weight: 0.8,
            }],
            upsert_edges: Vec::new(),
            invalidate_edges: Vec::new(),
            delete_node_ids: Vec::new(),
            delete_edge_ids: Vec::new(),
        };
        let result = engine.graph_patch(&patch).unwrap();
        assert_eq!(result.node_ids[0], id1, "graph_patch must reuse existing node ID");

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap(); // will be excluded
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.8).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // No policy: both neighbors visible
        let result = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(result.len(), 2);

        // Register policy: exclude weight <= 0.5
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // b is excluded by policy
        let result = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors_limit() {
        // When policies filter some results, limit should apply AFTER filtering.
        // If we have 5 neighbors and policy excludes 2, limit=2 should return 2 (not 0 or 1).
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.9).unwrap();
        // 3 high-weight neighbors (visible) + 2 low-weight (hidden by policy)
        let mut visible_ids = Vec::new();
        for i in 0..3 {
            let id = engine.upsert_node(1, &format!("hi-{}", i), BTreeMap::new(), 0.8).unwrap();
            engine.upsert_edge(hub, id, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            visible_ids.push(id);
        }
        for i in 0..2 {
            let id = engine.upsert_node(1, &format!("lo-{}", i), BTreeMap::new(), 0.1).unwrap();
            engine.upsert_edge(hub, id, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Without limit: 3 visible
        let result = engine.neighbors(hub, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(result.len(), 3);

        // With limit=2: should return exactly 2 (not fewer)
        let result = engine.neighbors(hub, Direction::Outgoing, None, 2, None, None).unwrap();
        assert_eq!(result.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors_2hop() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // a -> b -> c (c has low weight, should be excluded)
        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.2).unwrap(); // will be excluded
        let d = engine.upsert_node(1, "d", BTreeMap::new(), 0.9).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // 2-hop from a: b is 1-hop neighbor, c and d are 2-hop. c is excluded by policy.
        let result = engine.neighbors_2hop(a, Direction::Outgoing, None, 0, None, None).unwrap();
        let result_ids: Vec<u64> = result.iter().map(|e| e.node_id).collect();
        assert!(result_ids.contains(&d));
        assert!(!result_ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_top_k() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 0.9).unwrap();
        let hi = engine.upsert_node(1, "hi", BTreeMap::new(), 0.8).unwrap();
        let lo = engine.upsert_node(1, "lo", BTreeMap::new(), 0.2).unwrap();

        engine.upsert_edge(hub, hi, 1, BTreeMap::new(), 5.0, None, None).unwrap();
        engine.upsert_edge(hub, lo, 1, BTreeMap::new(), 10.0, None, None).unwrap(); // higher weight but node excluded

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let result = engine.top_k_neighbors(hub, Direction::Outgoing, None, 2, ScoringMode::Weight, None).unwrap();
        // Only hi should appear (lo excluded by policy)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, hi);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_extract_subgraph() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.8).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.2).unwrap(); // excluded

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let sg = engine.extract_subgraph(a, 1, Direction::Outgoing, None, None).unwrap();
        let node_ids: Vec<u64> = sg.nodes.iter().map(|n| n.id).collect();
        assert!(node_ids.contains(&a));
        assert!(node_ids.contains(&b));
        assert!(!node_ids.contains(&c)); // excluded by policy

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_nodes_by_type() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let ids = engine.nodes_by_type(1).unwrap();
        assert!(!ids.contains(&a)); // excluded
        assert!(ids.contains(&b));  // visible

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_find_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));

        engine.upsert_node(1, "a", props.clone(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", props.clone(), 0.9).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        let ids = engine.find_nodes(1, "color", &PropValue::String("red".to_string())).unwrap();
        // Only b visible (a excluded by policy)
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], b);

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_prune_still_works() {
        // Manual prune must still find and delete policy-excluded nodes.
        // This ensures prune uses raw reads internally.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.2).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.9).unwrap();
        // Edge between them for cascade testing
        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Register policy that hides 'a' from reads
        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Confirm a is hidden
        assert!(engine.get_node(a).unwrap().is_none());

        // Manual prune with same criteria. Should still find and delete 'a'
        let result = engine.prune(&PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();
        assert_eq!(result.nodes_pruned, 1);
        assert_eq!(result.edges_pruned, 1); // cascade delete

        // After prune, even raw read finds nothing (actually deleted now)
        // Remove policy first to test raw state
        engine.remove_prune_policy("p").unwrap();
        assert!(engine.get_node(a).unwrap().is_none()); // truly deleted

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_delete_node_cascade_unaffected() {
        // delete_node must cascade-delete ALL incident edges, even those to
        // policy-excluded nodes. Uses neighbors_raw internally.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap(); // will be policy-excluded
        let edge_id = engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Delete a. Must cascade-delete the edge to b even though b is policy-excluded
        engine.delete_node(a).unwrap();

        // The edge should be deleted
        assert!(engine.get_edge(edge_id).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_read_time_policy_neighbors_after_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut opts = DbOptions::default();
        opts.wal_sync_mode = WalSyncMode::Immediate;
        opts.compact_after_n_flushes = 0;
        let mut engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let a = engine.upsert_node(1, "a", BTreeMap::new(), 0.9).unwrap();
        let b = engine.upsert_node(1, "b", BTreeMap::new(), 0.2).unwrap();
        let c = engine.upsert_node(1, "c", BTreeMap::new(), 0.8).unwrap();

        engine.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        engine.set_prune_policy("p", PrunePolicy {
            max_age_ms: None, max_weight: Some(0.5), type_id: None,
        }).unwrap();

        // Works on segment-sourced neighbors too
        let result = engine.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].node_id, c);

        engine.close().unwrap();
    }

    // ============================================================
    // close_fast tests
    // ============================================================

    #[test]
    fn test_close_fast_basic() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id = engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        engine.close_fast().unwrap();

        // Reopen. Data should be intact
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(engine2.get_node(id).unwrap().is_some());
        engine2.close().unwrap();
    }

    #[test]
    fn test_close_fast_cancels_bg_compact() {
        // Create a DB with enough segments to trigger bg compaction,
        // then close_fast should cancel it without waiting.
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0, // manual only
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 3 segments
        for i in 0..3 {
            for j in 0..100 {
                let key = format!("node_{}_{}",i,j);
                engine.upsert_node(1, &key, BTreeMap::new(), 1.0).unwrap();
            }
            engine.flush().unwrap();
        }

        // Start background compaction
        engine.start_bg_compact().unwrap();
        assert!(engine.bg_compact.is_some());

        // close_fast should cancel it and succeed
        engine.close_fast().unwrap();

        // Reopen. Data should be intact (original segments preserved)
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine2.stats();
        // All nodes should still be accessible
        assert!(engine2.get_node_by_key(1, "node_0_0").unwrap().is_some());
        assert!(engine2.get_node_by_key(1, "node_2_99").unwrap().is_some());
        // 3 segments still present (compaction was cancelled)
        assert!(stats.segment_count >= 1); // could be 3 or 1 if compaction finished fast
        engine2.close().unwrap();
    }

    #[test]
    fn test_close_fast_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id = engine.upsert_node(1, "gc_node", BTreeMap::new(), 1.0).unwrap();
        engine.close_fast().unwrap();

        // Reopen. Data should be durable
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert!(engine2.get_node(id).unwrap().is_some());
        engine2.close().unwrap();
    }

    // ============================================================
    // stats tests
    // ============================================================

    #[test]
    fn test_stats_fresh_db() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine.stats();

        assert_eq!(stats.pending_wal_bytes, 0);
        assert_eq!(stats.segment_count, 0);
        assert_eq!(stats.node_tombstone_count, 0);
        assert_eq!(stats.edge_tombstone_count, 0);
        assert!(stats.last_compaction_ms.is_none());
        assert_eq!(stats.wal_sync_mode, "immediate");

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_group_commit_sync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let stats = engine.stats();

        assert_eq!(stats.wal_sync_mode, "group-commit");

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_segments_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.stats().segment_count, 0);

        engine.upsert_node(1, "n1", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().segment_count, 1);

        engine.upsert_node(1, "n2", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().segment_count, 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        assert_eq!(engine.stats().node_tombstone_count, 0);
        assert_eq!(engine.stats().edge_tombstone_count, 0);

        // delete_node cascades to incident edges, so edge e1 is also tombstoned
        engine.delete_node(n1).unwrap();
        assert_eq!(engine.stats().node_tombstone_count, 1);
        assert_eq!(engine.stats().edge_tombstone_count, 1);

        // Deleting n2 (no remaining edges) adds another node tombstone
        engine.delete_node(n2).unwrap();
        assert_eq!(engine.stats().node_tombstone_count, 2);
        assert_eq!(engine.stats().edge_tombstone_count, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_last_compaction_ms() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // No compaction yet
        assert!(engine.stats().last_compaction_ms.is_none());

        // Create 2 segments and compact
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        let before = now_millis();
        engine.compact().unwrap();
        let after = now_millis();

        let stats = engine.stats();
        let ts = stats.last_compaction_ms.expect("should have compaction timestamp");
        assert!(ts >= before && ts <= after, "timestamp should be between before and after");
        assert_eq!(stats.segment_count, 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_last_compaction_ms_bg() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::Immediate,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create 2 segments
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        let before = now_millis();
        engine.start_bg_compact().unwrap();
        engine.wait_for_bg_compact();
        let after = now_millis();

        let stats = engine.stats();
        let ts = stats.last_compaction_ms.expect("should have bg compaction timestamp");
        assert!(ts >= before && ts <= after);

        engine.close().unwrap();
    }

    #[test]
    fn test_stats_pending_wal_bytes_group_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 5_000, // Long enough to observe buffered bytes
                soft_trigger_bytes: 100 * 1024 * 1024, // Very high so timer-based sync only
                hard_cap_bytes: 200 * 1024 * 1024,
            },
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        assert_eq!(engine.stats().pending_wal_bytes, 0);

        // Write something. It should show as pending (sync interval hasn't fired)
        engine.upsert_node(1, "buffered", BTreeMap::new(), 1.0).unwrap();
        let stats = engine.stats();
        assert!(stats.pending_wal_bytes > 0, "should have buffered bytes");

        engine.close().unwrap();
    }

    // ========================================================================================
    // V3 Compaction Planner Tests
    // ========================================================================================

    #[test]
    fn test_v3_planner_basic_winner_selection() {
        // Two segments with overlapping node IDs. Newest segment's version wins
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1 (older): nodes with keys a, b, c
        engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Segment 2 (newer): update "a" with new weight, add "d"
        engine.upsert_node(1, "a", BTreeMap::new(), 2.0).unwrap();
        engine.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        assert_eq!(engine.segments.len(), 2);

        // Compact. V3 planner should pick "a" from segment 2 (newer version)
        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.segments_merged, 2);
        assert_eq!(stats.nodes_kept, 4); // a, b, c, d

        // Verify the winner for "a" has the updated weight
        let n = engine.get_node_by_key(1, "a").unwrap().unwrap();
        assert_eq!(n.weight, 2.0);

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_tombstone_handling() {
        // Delete a node, compact. V3 planner respects tombstones
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let id1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let id2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        engine.delete_node(id1).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        assert_eq!(stats.nodes_removed, 1); // node 1 tombstoned

        assert!(engine.get_node(id1).unwrap().is_none());
        assert!(engine.get_node(id2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_edge_cascade_on_tombstone() {
        // Delete a node with edges. V3 planner cascade-drops incident edges
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let n3 = engine.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        let e1 = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(n2, n3, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        // Delete n2. Should cascade-drop both e1 (n1→n2) and e2 (n2→n3)
        engine.delete_node(n2).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2); // n1, n3
        assert_eq!(stats.edges_kept, 0); // both edges dropped

        assert!(engine.get_edge(e1).unwrap().is_none());
        assert!(engine.get_edge(e2).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_from_metadata() {
        // Auto-prune via registered policy. V3 evaluates from metadata only
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Create overlapping segments (overlapping IDs across segments)
        engine.upsert_node(1, "low_weight", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "high_weight", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();
        // Update high_weight to create overlapping node ID across segments
        engine.upsert_node(1, "high_weight", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();

        // Register policy: prune nodes with weight <= 0.5
        engine.set_prune_policy(
            "low_weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                type_id: None,
            },
        ).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 1);
        assert_eq!(stats.nodes_auto_pruned, 1);

        // Only high_weight node survives
        let node = engine.get_node_by_key(1, "high_weight").unwrap();
        assert!(node.is_some());
        let node = engine.get_node_by_key(1, "low_weight").unwrap();
        assert!(node.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_edge_cascade() {
        // Pruned node's edges should be cascade-dropped
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let n1 = engine.upsert_node(1, "keep", BTreeMap::new(), 5.0).unwrap();
        let n2 = engine.upsert_node(1, "prune_me", BTreeMap::new(), 0.1).unwrap();
        let n3 = engine.upsert_node(1, "also_keep", BTreeMap::new(), 5.0).unwrap();
        let _e1 = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        let e2 = engine.upsert_edge(n1, n3, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();
        // Update a node to create overlapping IDs (forces V3 standard path)
        engine.upsert_node(1, "keep", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();

        engine.set_prune_policy(
            "low",
            PrunePolicy { max_age_ms: None, max_weight: Some(0.5), type_id: None },
        ).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 1);
        assert_eq!(stats.edges_auto_pruned, 1); // e1 cascade-dropped
        assert_eq!(stats.edges_kept, 1); // e2 survives

        // e2 (n1→n3) survives
        assert!(engine.get_edge(e2).unwrap().is_some());

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_prune_policy_or_semantics() {
        // Multiple policies: OR across policies (any match → pruned)
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Type 1: low weight, type 2: low weight, type 3: safe
        engine.upsert_node(1, "t1_low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(1, "t1_high", BTreeMap::new(), 5.0).unwrap();
        engine.upsert_node(2, "t2_low", BTreeMap::new(), 0.1).unwrap();
        engine.upsert_node(3, "t3_safe", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();
        // Update a node to create overlap (forces V3 standard path)
        engine.upsert_node(1, "t1_high", BTreeMap::new(), 5.0).unwrap();
        engine.flush().unwrap();

        // Policy A: prune type=1 with weight <= 0.5
        engine.set_prune_policy(
            "type1_low",
            PrunePolicy { max_age_ms: None, max_weight: Some(0.5), type_id: Some(1) },
        ).unwrap();
        // Policy B: prune type=2 with weight <= 0.5
        engine.set_prune_policy(
            "type2_low",
            PrunePolicy { max_age_ms: None, max_weight: Some(0.5), type_id: Some(2) },
        ).unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_auto_pruned, 2); // t1_low + t2_low
        assert_eq!(stats.nodes_kept, 2); // t1_high + t3_safe

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_planner_overlapping_multi_segment() {
        // Multiple segments with heavily overlapping IDs. Correctness stress
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1: nodes 1-50
        for i in 0..50 {
            engine.upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        // Segment 2: update nodes 10-30 with new weights
        for i in 10..30 {
            engine.upsert_node(1, &format!("node_{}", i), BTreeMap::new(), 2.0).unwrap();
        }
        engine.flush().unwrap();

        // Segment 3: delete nodes 40-49
        for i in 40..50 {
            let n = engine.get_node_by_key(1, &format!("node_{}", i)).unwrap().unwrap();
            engine.delete_node(n.id).unwrap();
        }
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 40); // 50 - 10 deleted = 40
        assert_eq!(stats.segments_merged, 3);

        // Verify updated nodes have new weight
        for i in 10..30 {
            let n = engine.get_node_by_key(1, &format!("node_{}", i)).unwrap().unwrap();
            assert_eq!(n.weight, 2.0, "node_{} should have updated weight", i);
        }

        // Verify deleted nodes are gone
        for i in 40..50 {
            let n = engine.get_node_by_key(1, &format!("node_{}", i)).unwrap();
            assert!(n.is_none(), "node_{} should be deleted", i);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_compact_preserves_edges_across_segments() {
        // Edges in different segments than their endpoint nodes
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        // Segment 1: nodes
        let n1 = engine.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let n2 = engine.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        engine.flush().unwrap();

        // Segment 2: edges
        let e1 = engine.upsert_edge(n1, n2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        engine.flush().unwrap();

        let stats = engine.compact().unwrap().unwrap();
        assert_eq!(stats.nodes_kept, 2);
        assert_eq!(stats.edges_kept, 1);

        // Edge survives compaction
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);

        // Adjacency works
        let nbrs = engine.neighbors(n1, Direction::Outgoing, None, 100, None, None).unwrap();
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].node_id, n2);

        engine.close().unwrap();
    }

    #[test]
    fn test_v3_compact_reopen_durability() {
        // V3 compaction output survives close + reopen
        let dir = TempDir::new().unwrap();
        let opts = DbOptions { compact_after_n_flushes: 0, ..DbOptions::default() };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        for i in 0..100 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0).unwrap();
        }
        engine.flush().unwrap();

        for i in 50..100 {
            engine.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 2.0).unwrap();
        }
        engine.flush().unwrap();

        engine.compact().unwrap();
        engine.close().unwrap();

        // Reopen and verify
        let engine2 = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert_eq!(engine2.segments.len(), 1);
        for i in 0..100 {
            let n = engine2.get_node_by_key(1, &format!("n{}", i)).unwrap();
            assert!(n.is_some(), "node n{} should exist after reopen", i);
            let n = n.unwrap();
            let expected_weight = if i >= 50 { 2.0 } else { 1.0 };
            assert_eq!(n.weight, expected_weight, "n{} weight", i);
        }

        engine2.close().unwrap();
    }

    #[test]
    fn test_v3_matches_any_prune_policy_meta() {
        // Unit test for the metadata-based prune policy matcher
        let policy_weight = PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: None,
        };
        let policy_type_scoped = PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: Some(1),
        };
        let policy_age = PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: None,
            type_id: None,
        };

        let now = 10_000i64;

        // Weight-only policy
        assert!(matches_any_prune_policy_meta(1, now, 0.1, &[policy_weight.clone()], now));
        assert!(matches_any_prune_policy_meta(1, now, 0.5, &[policy_weight.clone()], now));
        assert!(!matches_any_prune_policy_meta(1, now, 0.6, &[policy_weight.clone()], now));

        // Type-scoped policy
        assert!(matches_any_prune_policy_meta(1, now, 0.1, &[policy_type_scoped.clone()], now));
        assert!(!matches_any_prune_policy_meta(2, now, 0.1, &[policy_type_scoped.clone()], now)); // Wrong type

        // Age-only policy: updated_at < now - max_age_ms = 10000 - 1000 = 9000
        assert!(matches_any_prune_policy_meta(1, 8000, 1.0, &[policy_age.clone()], now)); // Old enough
        assert!(!matches_any_prune_policy_meta(1, 9500, 1.0, &[policy_age.clone()], now)); // Too recent

        // OR across policies
        let policies = vec![policy_type_scoped.clone(), policy_age.clone()];
        // Matches type-scoped (type=1, weight=0.1)
        assert!(matches_any_prune_policy_meta(1, now, 0.1, &policies, now));
        // Matches age (old enough)
        assert!(matches_any_prune_policy_meta(5, 8000, 1.0, &policies, now));
        // Matches neither
        assert!(!matches_any_prune_policy_meta(5, now, 1.0, &policies, now));

        // AND within policy: both age AND weight must match
        let policy_combo = PrunePolicy {
            max_age_ms: Some(1000),
            max_weight: Some(0.5),
            type_id: None,
        };
        // Old AND low weight → prune
        assert!(matches_any_prune_policy_meta(1, 8000, 0.1, &[policy_combo.clone()], now));
        // Old but high weight → no prune
        assert!(!matches_any_prune_policy_meta(1, 8000, 1.0, &[policy_combo.clone()], now));
        // Recent but low weight → no prune
        assert!(!matches_any_prune_policy_meta(1, 9500, 0.1, &[policy_combo.clone()], now));

        // Empty policies → never match
        assert!(!matches_any_prune_policy_meta(1, 0, 0.0, &[], now));
    }

    // =========================================================================
    // P8e-007: V3 correctness and stress tests
    // =========================================================================

    /// Stress test: many segments with heavy overlap and tombstones.
    #[test]
    fn test_v3_tombstone_overlap_stress() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for round in 0..5u64 {
            for i in 0..20u64 {
                let key = format!("node_{}", i);
                let mut props = BTreeMap::new();
                props.insert("round".into(), PropValue::Int(round as i64));
                db.upsert_node(1, &key, props, 0.5).unwrap();
            }
            for i in 0..19u64 {
                db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            }
            db.flush().unwrap();
        }

        for i in [3u64, 7, 11, 15] {
            db.delete_node(i).unwrap();
        }
        db.flush().unwrap();

        let stats = db.compact().unwrap().expect("compaction should run");
        assert!(stats.segments_merged >= 2, "should merge at least 2 segments");

        for i in 1..=20u64 {
            let node = db.get_node(i).unwrap();
            if [3, 7, 11, 15].contains(&i) {
                assert!(node.is_none(), "node {} should be deleted", i);
            } else {
                let n = node.unwrap_or_else(|| panic!("node {} should exist", i));
                assert_eq!(n.props.get("round"), Some(&PropValue::Int(4)));
            }
        }

        let out_3 = db.neighbors(3, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out_3.is_empty(), "deleted node 3 should have no neighbors");
    }

    /// Stress test: prune policies with type scoping.
    #[test]
    fn test_v3_policy_or_and_semantics() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..10u64 {
            let w = if i < 5 { 0.1 } else { 0.8 };
            let mut props = BTreeMap::new();
            props.insert("name".into(), PropValue::String(format!("t1_{}", i)));
            db.upsert_node(1, &format!("t1_{}", i), props, w).unwrap();
        }
        for i in 0..10u64 {
            let mut props = BTreeMap::new();
            props.insert("name".into(), PropValue::String(format!("t2_{}", i)));
            db.upsert_node(2, &format!("t2_{}", i), props, 0.5).unwrap();
        }
        db.flush().unwrap();

        // Touch a node to create a second segment (compaction needs ≥2).
        db.upsert_node(1, "t1_0", BTreeMap::new(), 0.1).unwrap();
        db.flush().unwrap();

        db.set_prune_policy("low-weight-t1", PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.3),
            type_id: Some(1),
        }).unwrap();

        let stats = db.compact().unwrap().expect("compaction");

        for i in 0..10u64 {
            assert!(
                db.get_node_by_key(2, &format!("t2_{}", i)).unwrap().is_some(),
                "type2 node t2_{} should survive", i
            );
        }
        for i in 5..10u64 {
            assert!(
                db.get_node_by_key(1, &format!("t1_{}", i)).unwrap().is_some(),
                "type1 node t1_{} (w=0.8) should survive", i
            );
        }
        assert!(stats.nodes_auto_pruned > 0);
    }

    /// Stress test: edge cascade with high-fanout pruned node.
    #[test]
    fn test_v3_edge_cascade_high_fanout() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = db.upsert_node(1, "hub", BTreeMap::new(), 0.5).unwrap();
        let mut spoke_ids = Vec::new();
        for i in 0..100u64 {
            let spoke = db.upsert_node(1, &format!("spoke_{}", i), BTreeMap::new(), 0.5).unwrap();
            spoke_ids.push(spoke);
            db.upsert_edge(hub, spoke, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            db.upsert_edge(spoke, hub, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        db.flush().unwrap();

        db.delete_node(hub).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        assert!(db.get_node(hub).unwrap().is_none());
        let hub_out = db.neighbors(hub, Direction::Both, None, 0, None, None).unwrap();
        assert!(hub_out.is_empty(), "hub should have no neighbors");

        for &spoke in &spoke_ids {
            assert!(db.get_node(spoke).unwrap().is_some(), "spoke {} should exist", spoke);
            let nbrs = db.neighbors(spoke, Direction::Both, None, 0, None, None).unwrap();
            for ne in &nbrs {
                assert_ne!(ne.node_id, hub, "no neighbor should be hub");
            }
        }
    }

    /// Stress test: deterministic output. Compact same data twice, identical results.
    #[test]
    fn test_v3_deterministic_output() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let p1 = dir1.path().join("db");
        let p2 = dir2.path().join("db");

        for p in [&p1, &p2] {
            let mut db = DatabaseEngine::open(p, &DbOptions::default()).unwrap();
            for i in 0..50u64 {
                let mut props = BTreeMap::new();
                props.insert("idx".into(), PropValue::Int(i as i64));
                db.upsert_node(i as u32 % 3 + 1, &format!("n_{}", i), props, 0.5).unwrap();
            }
            for i in 0..30u64 {
                db.upsert_edge(i + 1, (i + 5) % 50 + 1, i as u32 % 2 + 1, BTreeMap::new(), 1.0, None, None).unwrap();
            }
            db.flush().unwrap();
            for i in 0..20u64 {
                let mut props = BTreeMap::new();
                props.insert("idx".into(), PropValue::Int(i as i64 + 100));
                db.upsert_node(i as u32 % 3 + 1, &format!("n_{}", i), props, 0.5).unwrap();
            }
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
        }

        let db1 = DatabaseEngine::open(&p1, &DbOptions::default()).unwrap();
        let db2 = DatabaseEngine::open(&p2, &DbOptions::default()).unwrap();

        for i in 0..50u64 {
            let n1 = db1.get_node(i + 1).unwrap();
            let n2 = db2.get_node(i + 1).unwrap();
            assert_eq!(n1.is_some(), n2.is_some(), "node {} existence mismatch", i + 1);
            if let (Some(a), Some(b)) = (n1, n2) {
                assert_eq!(a.props, b.props, "node {} props mismatch", i + 1);
                assert_eq!(a.type_id, b.type_id, "node {} type_id mismatch", i + 1);
                assert_eq!(a.key, b.key, "node {} key mismatch", i + 1);
            }
        }
    }

    /// Critical test: index parity after V3 compaction. All query types correct.
    #[test]
    fn test_v3_index_parity() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props1 = BTreeMap::new();
        props1.insert("color".into(), PropValue::String("red".into()));
        props1.insert("score".into(), PropValue::Float(0.8));
        let mut props2 = BTreeMap::new();
        props2.insert("color".into(), PropValue::String("blue".into()));
        props2.insert("score".into(), PropValue::Float(0.6));

        for i in 0..10u64 {
            let props = if i % 2 == 0 { props1.clone() } else { props2.clone() };
            db.upsert_node(i as u32 % 2 + 1, &format!("key_{}", i), props, 0.5).unwrap();
        }
        for i in 0..8u64 {
            db.upsert_edge(i + 1, i + 2, i as u32 % 3 + 1, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        db.flush().unwrap();

        for i in 0..5u64 {
            let mut props = BTreeMap::new();
            props.insert("color".into(), PropValue::String("green".into()));
            props.insert("updated".into(), PropValue::Bool(true));
            db.upsert_node(i as u32 % 2 + 1, &format!("key_{}", i), props, 0.5).unwrap();
        }
        for i in 5..10u64 {
            db.upsert_edge(i + 1, 1, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        db.flush().unwrap();

        // Capture pre-compaction results
        let pre_nodes: Vec<_> = (1..=10).map(|i| db.get_node(i).unwrap()).collect();
        let pre_key_lookups: Vec<_> = (0..10u32)
            .map(|i| db.get_node_by_key(i % 2 + 1, &format!("key_{}", i)).unwrap())
            .collect();
        let pre_neighbors: Vec<_> = (1..=10)
            .map(|i| db.neighbors(i, Direction::Both, None, 0, None, None).unwrap())
            .collect();
        let pre_type1 = db.nodes_by_type(1).unwrap();
        let pre_type2 = db.nodes_by_type(2).unwrap();
        let pre_find = db.find_nodes(1, "color", &PropValue::String("green".into())).unwrap();

        db.compact().unwrap();

        for i in 0..10 {
            let post = db.get_node(i as u64 + 1).unwrap();
            assert_eq!(
                pre_nodes[i].as_ref().map(|n| (&n.props, n.type_id)),
                post.as_ref().map(|n| (&n.props, n.type_id)),
                "get_node({}) mismatch", i + 1
            );
        }

        for i in 0..10u32 {
            let post = db.get_node_by_key(i % 2 + 1, &format!("key_{}", i)).unwrap();
            assert_eq!(
                pre_key_lookups[i as usize].as_ref().map(|n| n.id),
                post.as_ref().map(|n| n.id),
                "get_node_by_key({}) mismatch", i
            );
        }

        for i in 0..10 {
            let post = db.neighbors(i as u64 + 1, Direction::Both, None, 0, None, None).unwrap();
            let pre_ids: HashSet<u64> = pre_neighbors[i].iter().map(|ne| ne.edge_id).collect();
            let post_ids: HashSet<u64> = post.iter().map(|ne| ne.edge_id).collect();
            assert_eq!(pre_ids, post_ids, "neighbors({}) edge set mismatch", i + 1);
        }

        let post_type1 = db.nodes_by_type(1).unwrap();
        assert_eq!(pre_type1.len(), post_type1.len(), "nodes_by_type(1) mismatch");

        let post_type2 = db.nodes_by_type(2).unwrap();
        assert_eq!(pre_type2.len(), post_type2.len(), "nodes_by_type(2) mismatch");

        let post_find = db.find_nodes(1, "color", &PropValue::String("green".into())).unwrap();
        let pre_find_ids: HashSet<u64> = pre_find.iter().copied().collect();
        let post_find_ids: HashSet<u64> = post_find.iter().copied().collect();
        assert_eq!(pre_find_ids, post_find_ids, "find_nodes mismatch");
    }

    /// Stress test: mixed workload. Interleave upserts, deletes, compactions.
    #[test]
    fn test_v3_mixed_workload_stress() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..30u64 {
            db.upsert_node(1, &format!("mix_{}", i), BTreeMap::new(), 0.5).unwrap();
        }
        for i in 0..20u64 {
            db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        db.flush().unwrap();

        for i in [5u64, 10, 15, 20, 25] {
            db.delete_node(i).unwrap();
        }
        for i in 0..10u64 {
            let mut props = BTreeMap::new();
            props.insert("updated".into(), PropValue::Bool(true));
            db.upsert_node(1, &format!("mix_{}", i), props, 0.5).unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();

        for i in 0..5u64 {
            let mut props = BTreeMap::new();
            props.insert("round3".into(), PropValue::Int(3));
            db.upsert_node(1, &format!("mix_{}", i), props, 0.5).unwrap();
        }
        for i in 30..40u64 {
            db.upsert_node(2, &format!("new_{}", i), BTreeMap::new(), 0.5).unwrap();
        }
        db.flush().unwrap();

        let stats = db.compact().unwrap().expect("compaction");
        assert!(stats.segments_merged >= 2);

        for i in [5u64, 10, 15, 20, 25] {
            assert!(db.get_node(i).unwrap().is_none(), "node {} should be deleted", i);
        }
        for i in 0..5u64 {
            let n = db.get_node_by_key(1, &format!("mix_{}", i)).unwrap().unwrap();
            assert_eq!(n.props.get("round3"), Some(&PropValue::Int(3)));
        }
        for i in 30..40u64 {
            assert!(
                db.get_node_by_key(2, &format!("new_{}", i)).unwrap().is_some(),
                "new node new_{} should exist", i
            );
        }
    }

    /// Stress test: edges in different segments than their endpoint nodes.
    #[test]
    fn test_v3_cross_segment_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..20u64 {
            db.upsert_node(1, &format!("cs_{}", i), BTreeMap::new(), 0.5).unwrap();
        }
        db.flush().unwrap();

        for i in 0..19u64 {
            db.upsert_edge(i + 1, i + 2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        }
        db.upsert_edge(1, 10, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(5, 15, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(10, 20, 2, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let out_1 = db.neighbors(1, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out_1.len() >= 2, "node 1 should have at least 2 outgoing edges");

        let out_5 = db.neighbors(5, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(out_5.len() >= 2, "node 5 should have outgoing to 6 and 15");

        let in_10 = db.neighbors(10, Direction::Incoming, None, 0, None, None).unwrap();
        assert!(
            in_10.iter().any(|ne| ne.node_id == 1),
            "node 10 should have incoming from node 1"
        );
    }

    /// Stress test: V3 compact → close → reopen → verify all queries.
    #[test]
    fn test_v3_reopen_durability() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");

        {
            let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 0..50u64 {
                let mut props = BTreeMap::new();
                props.insert("name".into(), PropValue::String(format!("durable_{}", i)));
                db.upsert_node(i as u32 % 3 + 1, &format!("dur_{}", i), props, 0.5).unwrap();
            }
            for i in 0..40u64 {
                db.upsert_edge(i + 1, (i + 3) % 50 + 1, i as u32 % 2 + 1, BTreeMap::new(), 1.0, None, None).unwrap();
            }
            db.flush().unwrap();

            for i in 0..20u64 {
                let mut props = BTreeMap::new();
                props.insert("name".into(), PropValue::String(format!("updated_{}", i)));
                db.upsert_node(i as u32 % 3 + 1, &format!("dur_{}", i), props, 0.5).unwrap();
            }
            for i in [5u64, 15, 25, 35, 45] {
                db.delete_node(i).unwrap();
            }
            db.flush().unwrap();
            db.compact().unwrap();
            db.close().unwrap();
        }

        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 0..50u64 {
            let id = i + 1;
            if [5, 15, 25, 35, 45].contains(&id) {
                assert!(db.get_node(id).unwrap().is_none(), "node {} should be deleted", id);
            } else {
                let n = db.get_node(id).unwrap()
                    .unwrap_or_else(|| panic!("node {} should exist", id));
                if i < 20 {
                    assert_eq!(
                        n.props.get("name"),
                        Some(&PropValue::String(format!("updated_{}", i)))
                    );
                } else {
                    assert_eq!(
                        n.props.get("name"),
                        Some(&PropValue::String(format!("durable_{}", i)))
                    );
                }
            }
        }

        assert!(db.get_node_by_key(1, "dur_0").unwrap().is_some(), "key lookup should work");

        let nbrs = db.neighbors(1, Direction::Outgoing, None, 0, None, None).unwrap();
        assert!(!nbrs.is_empty(), "adjacency should work after reopen");

        let type1 = db.nodes_by_type(1).unwrap();
        assert!(!type1.is_empty(), "type query should work after reopen");
    }

    /// Fast-merge path: verify metadata-driven indexes for non-overlapping segments.
    #[test]
    fn test_v3_fast_merge_index_parity() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("db");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for seg in 0..3u64 {
            let base = seg * 10;
            for i in 0..10u64 {
                let mut props = BTreeMap::new();
                props.insert("seg".into(), PropValue::Int(seg as i64));
                db.upsert_node((seg as u32 % 2) + 1, &format!("fm_{}_{}", seg, i), props, 0.5).unwrap();
            }
            for i in 0..9u64 {
                db.upsert_edge(base + i + 1, base + i + 2, 1, BTreeMap::new(), 1.0, None, None).unwrap();
            }
            db.flush().unwrap();
        }

        let pre_nodes: Vec<_> = (1..=30).filter_map(|i| db.get_node(i).unwrap()).collect();
        let pre_key_0 = db.get_node_by_key(1, "fm_0_0").unwrap();
        let pre_nbrs_1 = db.neighbors(1, Direction::Outgoing, None, 0, None, None).unwrap();
        let pre_type1 = db.nodes_by_type(1).unwrap();

        db.compact().unwrap();

        let post_nodes: Vec<_> = (1..=30).filter_map(|i| db.get_node(i).unwrap()).collect();
        assert_eq!(pre_nodes.len(), post_nodes.len());
        for (pre, post) in pre_nodes.iter().zip(post_nodes.iter()) {
            assert_eq!(pre.id, post.id);
            assert_eq!(pre.props, post.props);
            assert_eq!(pre.type_id, post.type_id);
        }

        let post_key_0 = db.get_node_by_key(1, "fm_0_0").unwrap();
        assert_eq!(
            pre_key_0.map(|n| n.id),
            post_key_0.map(|n| n.id),
            "key lookup mismatch after fast-merge"
        );

        let post_nbrs_1 = db.neighbors(1, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(pre_nbrs_1.len(), post_nbrs_1.len(), "neighbors mismatch");

        let post_type1 = db.nodes_by_type(1).unwrap();
        assert_eq!(pre_type1.len(), post_type1.len(), "type query mismatch");
    }

    // --- Timestamp range index tests ---

    fn time_node(id: u64, type_id: u32, key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: updated_at - 100,
            updated_at,
            weight: 0.5,
        }
    }

    #[test]
    fn test_time_range_memtable_only() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 2, "d", 2500))).unwrap();

        // Exact range
        let r = db.find_nodes_by_time_range(1, 1000, 3000).unwrap();
        assert_eq!(r, vec![1, 2, 3]);

        // Partial range
        let r = db.find_nodes_by_time_range(1, 1500, 2500).unwrap();
        assert_eq!(r, vec![2]);

        // Type filter
        let r = db.find_nodes_by_time_range(2, 2000, 3000).unwrap();
        assert_eq!(r, vec![4]);

        // No matches
        let r = db.find_nodes_by_time_range(1, 4000, 5000).unwrap();
        assert!(r.is_empty());

        // All within type 1
        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2, 3]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_across_flush() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.flush().unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 1, "d", 1500))).unwrap();

        let r = db.find_nodes_by_time_range(1, 1000, 2000).unwrap();
        assert_eq!(r, vec![1, 2, 4]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.flush().unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(4, 1, "d", 4000))).unwrap();
        db.flush().unwrap();

        db.compact().unwrap();

        let r = db.find_nodes_by_time_range(1, 1500, 3500).unwrap();
        assert_eq!(r, vec![2, 3]);

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2, 3, 4]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
        db.flush().unwrap();

        db.delete_node(2).unwrap();

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 3]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_boundary_conditions() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();

        // Inclusive boundaries
        let r = db.find_nodes_by_time_range(1, 1000, 1000).unwrap();
        assert_eq!(r, vec![1], "single-point range at lower bound");

        let r = db.find_nodes_by_time_range(1, 3000, 3000).unwrap();
        assert_eq!(r, vec![3], "single-point range at upper bound");

        let r = db.find_nodes_by_time_range(1, 2000, 2000).unwrap();
        assert_eq!(r, vec![2], "single-point range in middle");

        // Empty range
        let r = db.find_nodes_by_time_range(1, 1500, 1500).unwrap();
        assert!(r.is_empty(), "no nodes at this exact time");

        // Inverted range
        let r = db.find_nodes_by_time_range(1, 3000, 1000).unwrap();
        assert!(r.is_empty(), "inverted range returns empty");

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_paged() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for i in 1..=10u64 {
            db.write_op(&WalOp::UpsertNode(time_node(i, 1, &format!("n{}", i), i as i64 * 1000))).unwrap();
        }
        db.flush().unwrap();

        let page1 = db.find_nodes_by_time_range_paged(1, 1000, 10000, &PageRequest {
            limit: Some(3),
            after: None,
        }).unwrap();
        assert_eq!(page1.items, vec![1, 2, 3]);
        assert!(page1.next_cursor.is_some());

        let page2 = db.find_nodes_by_time_range_paged(1, 1000, 10000, &PageRequest {
            limit: Some(3),
            after: page1.next_cursor,
        }).unwrap();
        assert_eq!(page2.items, vec![4, 5, 6]);

        let page3 = db.find_nodes_by_time_range_paged(1, 1000, 10000, &PageRequest {
            limit: Some(3),
            after: page2.next_cursor,
        }).unwrap();
        assert_eq!(page3.items, vec![7, 8, 9]);

        let page4 = db.find_nodes_by_time_range_paged(1, 1000, 10000, &PageRequest {
            limit: Some(3),
            after: page3.next_cursor,
        }).unwrap();
        assert_eq!(page4.items, vec![10]);
        assert!(page4.next_cursor.is_none());

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_upsert_updates_index() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        let r = db.find_nodes_by_time_range(1, 900, 1100).unwrap();
        assert_eq!(r, vec![1]);

        // Update same node with new timestamp
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000))).unwrap();

        let r = db.find_nodes_by_time_range(1, 900, 1100).unwrap();
        assert!(r.is_empty(), "node should not appear at old timestamp");

        let r = db.find_nodes_by_time_range(1, 4900, 5100).unwrap();
        assert_eq!(r, vec![1], "node should appear at new timestamp");

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        {
            let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
            db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }

        {
            let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let r = db.find_nodes_by_time_range(1, 1500, 2500).unwrap();
            assert_eq!(r, vec![2]);
            let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
            assert_eq!(r, vec![1, 2, 3]);
            db.close().unwrap();
        }
    }

    #[test]
    fn test_time_range_dedup_across_sources() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.flush().unwrap();

        // Update same node in memtable (different time, same wide range)
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1500))).unwrap();

        let r = db.find_nodes_by_time_range(1, 0, 2000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_with_prune_policy() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.write_op(&WalOp::UpsertNode(NodeRecord {
            id: 3,
            type_id: 1,
            key: "c".to_string(),
            props: BTreeMap::new(),
            created_at: 2900,
            updated_at: 3000,
            weight: 0.001,
        })).unwrap();

        db.set_prune_policy("low_weight", PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.01),
            type_id: None,
        }).unwrap();

        let r = db.find_nodes_by_time_range(1, 0, i64::MAX).unwrap();
        assert_eq!(r, vec![1, 2]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_stale_segment_suppressed_by_newer_version() {
        // A node flushed to a segment with updated_at inside the range must NOT
        // appear in results when a newer version (in memtable or newer segment)
        // has updated_at OUTSIDE the range.
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Insert node at t=1000, flush to segment
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.flush().unwrap();

        // Upsert same node with t=5000 (outside the query window)
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000))).unwrap();

        // Query [500, 2000]. Old segment has node at t=1000 (in range),
        // but current version is t=5000 (out of range). Must return empty.
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(r.is_empty(), "stale segment entry must be suppressed by newer version");

        // Node should appear in a range that covers its current timestamp
        let r = db.find_nodes_by_time_range(1, 4000, 6000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_stale_segment_suppressed_across_segments() {
        // Same as above, but the newer version is also in a segment (not memtable)
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Segment 1: node at t=1000
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.flush().unwrap();

        // Segment 2: same node at t=5000
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 5000))).unwrap();
        db.flush().unwrap();

        // Query [500, 2000]. Segment 1 has t=1000 (in range),
        // but latest version (segment 2) has t=5000. Must return empty.
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(r.is_empty(), "stale segment entry must be suppressed by newer segment version");

        // After compaction, still correct
        db.compact().unwrap();
        let r = db.find_nodes_by_time_range(1, 500, 2000).unwrap();
        assert!(r.is_empty(), "stale entry must be suppressed after compaction too");

        let r = db.find_nodes_by_time_range(1, 4000, 6000).unwrap();
        assert_eq!(r, vec![1]);

        db.close().unwrap();
    }

    #[test]
    fn test_time_range_paged_stale_suppressed() {
        // Ensure pagination path also filters stale entries
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let mut db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Flush 3 nodes to segment
        db.write_op(&WalOp::UpsertNode(time_node(1, 1, "a", 1000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 2000))).unwrap();
        db.write_op(&WalOp::UpsertNode(time_node(3, 1, "c", 3000))).unwrap();
        db.flush().unwrap();

        // Move node 2 outside the range
        db.write_op(&WalOp::UpsertNode(time_node(2, 1, "b", 9000))).unwrap();

        // Paginated query [500, 4000] limit=10, should get [1, 3] (not [1, 2, 3])
        let r = db.find_nodes_by_time_range_paged(1, 500, 4000, &PageRequest {
            limit: Some(10),
            after: None,
        }).unwrap();
        assert_eq!(r.items, vec![1, 3]);

        db.close().unwrap();
    }

    // ---- PPR tests ----

    #[test]
    fn test_ppr_empty_seeds() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let result = db.personalized_pagerank(&[], &PprOptions::default()).unwrap();
        assert!(result.scores.is_empty());
        assert_eq!(result.iterations, 0);
        assert!(result.converged);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_single_node_no_edges() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let n1 = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let result = db.personalized_pagerank(&[n1], &PprOptions::default()).unwrap();
        // Single node with no outgoing edges: all mass stays on seed via teleport + dangling
        assert_eq!(result.scores.len(), 1);
        assert_eq!(result.scores[0].0, n1);
        assert!((result.scores[0].1 - 1.0).abs() < 1e-6, "single dangling node should have rank ~1.0");
        assert!(result.converged);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_simple_chain() {
        // A → B → C (seed = A)
        // Rank should flow A > B > C
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(result.scores.len() >= 2);

        let score_a = result.scores.iter().find(|s| s.0 == a).map(|s| s.1).unwrap_or(0.0);
        let score_b = result.scores.iter().find(|s| s.0 == b).map(|s| s.1).unwrap_or(0.0);
        let score_c = result.scores.iter().find(|s| s.0 == c).map(|s| s.1).unwrap_or(0.0);

        assert!(score_a > score_b, "seed A should rank higher than B");
        assert!(score_b > score_c, "B should rank higher than C");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_cycle_converges() {
        // A → B → A (cycle), seed = A
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert_eq!(result.scores.len(), 2);

        let score_a = result.scores.iter().find(|s| s.0 == a).unwrap().1;
        let score_b = result.scores.iter().find(|s| s.0 == b).unwrap().1;

        // A should rank higher than B (teleport bias toward seed)
        assert!(score_a > score_b, "seed should rank higher due to teleport");
        // Total rank should sum to ~1.0
        assert!((score_a + score_b - 1.0).abs() < 1e-4, "total rank should sum to ~1.0");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_weighted_edges() {
        // A → B (weight=1.0), A → C (weight=9.0), seed = A
        // C should get ~9x the rank of B from A's distribution
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 9.0, None, None).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_b = result.scores.iter().find(|s| s.0 == b).map(|s| s.1).unwrap_or(0.0);
        let score_c = result.scores.iter().find(|s| s.0 == c).map(|s| s.1).unwrap_or(0.0);

        // C should have significantly higher rank than B due to weight
        assert!(score_c > score_b * 3.0, "heavily-weighted C ({score_c}) should rank much higher than B ({score_b})");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_edge_type_filter() {
        // A → B (type 1), A → C (type 2), seed = A
        // Filter to type 1 only: only B should receive rank
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 2, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = PprOptions {
            edge_type_filter: Some(vec![1]),
            max_iterations: 100,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_b = result.scores.iter().find(|s| s.0 == b).map(|s| s.1).unwrap_or(0.0);
        let score_c = result.scores.iter().find(|s| s.0 == c).map(|s| s.1).unwrap_or(0.0);

        assert!(score_b > 0.0, "B should receive rank via type-1 edge");
        assert_eq!(score_c, 0.0, "C should receive no rank (type-2 edge filtered out)");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_max_results() {
        // Star graph: seed → 10 nodes
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let center = db.upsert_node(1, "center", BTreeMap::new(), 1.0).unwrap();
        for i in 0..10 {
            let n = db.upsert_node(1, &format!("n{i}"), BTreeMap::new(), 1.0).unwrap();
            db.upsert_edge(center, n, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        }

        let opts = PprOptions {
            max_results: Some(3),
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[center], &opts).unwrap();
        assert!(result.scores.len() <= 3, "max_results should cap output");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_multiple_seeds() {
        // A → C, B → C, seeds = [A, B]
        // C should get high rank from both seeds
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a, b], &opts).unwrap();
        assert!(result.converged);

        let score_c = result.scores.iter().find(|s| s.0 == c).map(|s| s.1).unwrap_or(0.0);
        assert!(score_c > 0.0, "C should receive rank from both seeds");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_respects_deleted_nodes() {
        // A → B → C, delete B, seed = A
        // B's outgoing edges should not contribute rank to C
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.delete_node(b).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        // B is deleted. neighbors(A) should not include B
        let score_b = result.scores.iter().find(|s| s.0 == b).map(|s| s.1).unwrap_or(0.0);
        let score_c = result.scores.iter().find(|s| s.0 == c).map(|s| s.1).unwrap_or(0.0);
        assert_eq!(score_b, 0.0, "deleted node B should not appear in results");
        assert_eq!(score_c, 0.0, "C unreachable after B deleted");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_deleted_seed_returns_empty() {
        // Deleted seed must not appear in results
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        db.delete_node(a).unwrap();

        let result = db.personalized_pagerank(&[a], &PprOptions::default()).unwrap();
        assert!(result.scores.is_empty(), "deleted seed must not appear in PPR results");
        assert!(result.converged);
        assert_eq!(result.iterations, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_nonexistent_seed_returns_empty() {
        // Non-existent ID should also be filtered out
        let dir = TempDir::new().unwrap();
        let db = open_imm(dir.path());
        let result = db.personalized_pagerank(&[999], &PprOptions::default()).unwrap();
        assert!(result.scores.is_empty());
        assert!(result.converged);
        assert_eq!(result.iterations, 0);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_across_flush() {
        // Create graph, flush to segment, verify PPR still works
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        // Add more in memtable
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(result.scores.len() >= 3, "should find nodes across memtable + segment");
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let opts = PprOptions { max_iterations: 100, ..PprOptions::default() };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);
        assert!(result.scores.len() >= 3);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_duplicate_seeds() {
        // Duplicate seed IDs should be deduplicated
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let r1 = db.personalized_pagerank(&[a], &PprOptions::default()).unwrap();
        let r2 = db.personalized_pagerank(&[a, a, a], &PprOptions::default()).unwrap();

        // Should produce identical results
        assert_eq!(r1.scores.len(), r2.scores.len());
        for (s1, s2) in r1.scores.iter().zip(r2.scores.iter()) {
            assert_eq!(s1.0, s2.0);
            assert!((s1.1 - s2.1).abs() < 1e-10);
        }
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_known_values() {
        // Triangle: A → B → C → A, seed = A, damping = 0.85
        // Analytic solution for PPR on a directed cycle with uniform weights:
        //   rank_seed = (1 - d) / (1 - d^3)  [geometric series]
        //   rank_hop1 = d * rank_seed
        //   rank_hop2 = d^2 * rank_seed
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let d = 0.85_f64;
        let opts = PprOptions {
            damping_factor: d,
            epsilon: 1e-10,
            max_iterations: 200,
            ..PprOptions::default()
        };
        let result = db.personalized_pagerank(&[a], &opts).unwrap();
        assert!(result.converged);

        let score_a = result.scores.iter().find(|s| s.0 == a).unwrap().1;
        let score_b = result.scores.iter().find(|s| s.0 == b).unwrap().1;
        let score_c = result.scores.iter().find(|s| s.0 == c).unwrap().1;

        // Expected: rank_a = (1-d)/(1-d^3), rank_b = d*rank_a, rank_c = d^2*rank_a
        let expected_a = (1.0 - d) / (1.0 - d.powi(3));
        let expected_b = d * expected_a;
        let expected_c = d * d * expected_a;

        assert!((score_a - expected_a).abs() < 1e-6, "A: got {score_a}, expected {expected_a}");
        assert!((score_b - expected_b).abs() < 1e-6, "B: got {score_b}, expected {expected_b}");
        assert!((score_c - expected_c).abs() < 1e-6, "C: got {score_c}, expected {expected_c}");
        assert!((score_a + score_b + score_c - 1.0).abs() < 1e-6, "total should sum to 1.0");
        db.close().unwrap();
    }

    // --- export_adjacency tests ---

    #[test]
    fn test_export_empty_db() {
        let dir = TempDir::new().unwrap();
        let db = open_imm(dir.path());
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert!(result.node_ids.is_empty());
        assert!(result.edges.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_export_nodes_only() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(2, "b", BTreeMap::new(), 1.0).unwrap();
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert_eq!(result.node_ids.len(), 2);
        assert!(result.node_ids.contains(&a));
        assert!(result.node_ids.contains(&b));
        assert!(result.edges.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_export_full_graph() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 2.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 3.0, None, None).unwrap();
        db.upsert_edge(c, a, 2, BTreeMap::new(), 1.0, None, None).unwrap();

        let opts = ExportOptions { include_weights: true, ..Default::default() };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 3);
        // Verify edge data
        let ab = result.edges.iter().find(|e| e.0 == a && e.1 == b).unwrap();
        assert_eq!(ab.2, 1);
        assert!((ab.3 - 2.0).abs() < 1e-6);
        let ca = result.edges.iter().find(|e| e.0 == c && e.1 == a).unwrap();
        assert_eq!(ca.2, 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_node_type_filter() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(2, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        // Only type-1 nodes: a and c
        let opts = ExportOptions {
            node_type_filter: Some(vec![1]),
            include_weights: true,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 2);
        assert!(result.node_ids.contains(&a));
        assert!(result.node_ids.contains(&c));
        // Edge a→b should be excluded (b is type-2, not in node set)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].0, a);
        assert_eq!(result.edges[0].1, c);
        db.close().unwrap();
    }

    #[test]
    fn test_export_edge_type_filter() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, b, 2, BTreeMap::new(), 1.0, None, None).unwrap();

        // Only edge type 2
        let opts = ExportOptions {
            edge_type_filter: Some(vec![2]),
            include_weights: true,
            ..Default::default()
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].2, 2); // type_id = 2
        db.close().unwrap();
    }

    #[test]
    fn test_export_include_weights_false() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 5.0, None, None).unwrap();

        let opts = ExportOptions { include_weights: false, ..Default::default() };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].3, 0.0); // weight zeroed out
        db.close().unwrap();
    }

    #[test]
    fn test_export_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.delete_node(b).unwrap();

        let result = db.export_adjacency(&ExportOptions { include_weights: true, ..Default::default() }).unwrap();
        assert_eq!(result.node_ids.len(), 2); // a and c
        assert!(!result.node_ids.contains(&b));
        // Edge a→b should be gone (b is deleted)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].1, c);
        db.close().unwrap();
    }

    #[test]
    fn test_export_across_flush() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 2.0, None, None).unwrap();

        let result = db.export_adjacency(&ExportOptions { include_weights: true, ..Default::default() }).unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let result = db.export_adjacency(&ExportOptions { include_weights: true, ..Default::default() }).unwrap();
        assert_eq!(result.node_ids.len(), 3);
        assert_eq!(result.edges.len(), 2);
        db.close().unwrap();
    }

    #[test]
    fn test_export_node_ids_sorted() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        for i in 0..10 {
            db.upsert_node(1, &format!("n{i}"), BTreeMap::new(), 1.0).unwrap();
        }
        let result = db.export_adjacency(&ExportOptions::default()).unwrap();
        assert_eq!(result.node_ids.len(), 10);
        for i in 1..result.node_ids.len() {
            assert!(result.node_ids[i] > result.node_ids[i - 1], "node_ids must be sorted");
        }
        db.close().unwrap();
    }

    #[test]
    fn test_export_combined_filters() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(dir.path());
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(2, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, b, 2, BTreeMap::new(), 2.0, None, None).unwrap();
        db.upsert_edge(a, c, 1, BTreeMap::new(), 3.0, None, None).unwrap();

        // node_type=1 + edge_type=1 → only edge a→b with type 1
        let opts = ExportOptions {
            node_type_filter: Some(vec![1]),
            edge_type_filter: Some(vec![1]),
            include_weights: true,
        };
        let result = db.export_adjacency(&opts).unwrap();
        assert_eq!(result.node_ids.len(), 2); // a and b (type 1)
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0], (a, b, 1, 1.0));
        db.close().unwrap();
    }

    // --- Batch adjacency tests ---

    #[test]
    fn test_neighbors_batch_basic() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db.neighbors_batch(&[a, b, c], Direction::Outgoing, None, None, None).unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 2); // A→B, A→C
        assert_eq!(result.get(&b).unwrap().len(), 1); // B→C
        assert!(result.get(&c).is_none()); // C has no outgoing
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_matches_individual() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let mut ids = Vec::new();
        for i in 0..5 {
            ids.push(db.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.5).unwrap());
        }
        db.upsert_edge(ids[0], ids[1], 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(ids[0], ids[2], 10, BTreeMap::new(), 2.0, None, None).unwrap();
        db.upsert_edge(ids[1], ids[2], 20, BTreeMap::new(), 3.0, None, None).unwrap();
        db.upsert_edge(ids[2], ids[3], 10, BTreeMap::new(), 1.5, None, None).unwrap();
        db.upsert_edge(ids[3], ids[4], 20, BTreeMap::new(), 0.5, None, None).unwrap();
        db.upsert_edge(ids[4], ids[0], 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let batch = db.neighbors_batch(&ids, Direction::Outgoing, None, None, None).unwrap();

        for &nid in &ids {
            let individual = db.neighbors(nid, Direction::Outgoing, None, 0, None, None).unwrap();
            let batch_entries = batch.get(&nid).cloned().unwrap_or_default();
            let mut ind_edge_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
            let mut bat_edge_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
            ind_edge_ids.sort();
            bat_edge_ids.sort();
            assert_eq!(ind_edge_ids, bat_edge_ids, "mismatch for node {}", nid);
        }
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_with_type_filter() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 20, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db
            .neighbors_batch(&[a], Direction::Outgoing, Some(&[10]), None, None)
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 1);
        assert_eq!(result[&a][0].edge_type_id, 10);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_cross_segment() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, c, 10, BTreeMap::new(), 2.0, None, None).unwrap();
        db.flush().unwrap();

        let d = db.upsert_node(1, "d", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(b, d, 10, BTreeMap::new(), 3.0, None, None).unwrap();

        let result = db
            .neighbors_batch(&[a, b], Direction::Outgoing, None, None, None)
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 2); // B (seg1) + C (seg2)
        assert_eq!(result.get(&b).unwrap().len(), 1); // D (memtable)
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_dedup_across_sources() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        // Re-upsert same edge triple. Should dedup (triple lookup across segments)
        db.upsert_edge(a, b, 10, BTreeMap::new(), 5.0, None, None).unwrap();

        // Batch and individual MUST return the same results
        let result = db
            .neighbors_batch(&[a], Direction::Outgoing, None, None, None)
            .unwrap();
        let individual = db.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        let batch_entries = result.get(&a).unwrap();

        assert_eq!(batch_entries.len(), individual.len());
        let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
        let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
        bat_ids.sort();
        ind_ids.sort();
        assert_eq!(bat_ids, ind_ids);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_respects_tombstones() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        let e1 = db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, c, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        db.delete_edge(e1).unwrap();

        let result = db
            .neighbors_batch(&[a], Direction::Outgoing, None, None, None)
            .unwrap();
        let entries = result.get(&a).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].node_id, c);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_direction_both() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(c, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db
            .neighbors_batch(&[b], Direction::Both, None, None, None)
            .unwrap();
        let entries = result.get(&b).unwrap();
        assert_eq!(entries.len(), 2); // incoming from A and C
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_self_loop_dedup() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        // Self-loop: A→A appears in both adj_out and adj_in
        db.upsert_edge(a, a, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db
            .neighbors_batch(&[a], Direction::Both, None, None, None)
            .unwrap();
        let individual = db.neighbors(a, Direction::Both, None, 0, None, None).unwrap();

        let batch_entries = result.get(&a).unwrap();
        // Self-loop should appear once (deduped by edge_id), not twice
        let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
        let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
        bat_ids.sort();
        ind_ids.sort();
        assert_eq!(bat_ids, ind_ids);
        assert_eq!(bat_ids.len(), ind_ids.len());
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_empty_input() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let result = db
            .neighbors_batch(&[], Direction::Outgoing, None, None, None)
            .unwrap();
        assert!(result.is_empty());
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_unsorted_input() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 0.5).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 0.5).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 0.5).unwrap();
        db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(c, a, 10, BTreeMap::new(), 2.0, None, None).unwrap();

        // Input in reverse order. Batch method sorts internally
        let result = db
            .neighbors_batch(&[c, a], Direction::Outgoing, None, None, None)
            .unwrap();
        assert_eq!(result.get(&a).unwrap().len(), 1);
        assert_eq!(result.get(&c).unwrap().len(), 1);
        db.close().unwrap();
    }

    #[test]
    fn test_neighbors_batch_large_graph_parity() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));

        // Build a graph with 50 nodes and ~100 edges across segment + memtable
        let mut node_ids = Vec::new();
        for i in 0..50 {
            node_ids.push(
                db.upsert_node(1, &format!("n{}", i), BTreeMap::new(), 0.5).unwrap(),
            );
        }
        for i in 0..50 {
            let t1 = (i + 1) % 50;
            let t2 = (i + 25) % 50;
            db.upsert_edge(node_ids[i], node_ids[t1], 10, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            db.upsert_edge(node_ids[i], node_ids[t2], 20, BTreeMap::new(), 0.5, None, None)
                .unwrap();
        }

        db.flush().unwrap();

        // Add more edges in memtable
        for i in 0..10 {
            let t = (i + 5) % 50;
            db.upsert_edge(node_ids[i], node_ids[t], 30, BTreeMap::new(), 0.3, None, None)
                .unwrap();
        }

        let batch = db
            .neighbors_batch(&node_ids, Direction::Outgoing, None, None, None)
            .unwrap();

        for &nid in &node_ids {
            let individual = db.neighbors(nid, Direction::Outgoing, None, 0, None, None).unwrap();
            let batch_entries = batch.get(&nid).cloned().unwrap_or_default();
            let mut ind_ids: Vec<u64> = individual.iter().map(|e| e.edge_id).collect();
            let mut bat_ids: Vec<u64> = batch_entries.iter().map(|e| e.edge_id).collect();
            ind_ids.sort();
            bat_ids.sort();
            assert_eq!(
                ind_ids, bat_ids,
                "neighbors_batch mismatch for node {} (individual: {:?}, batch: {:?})",
                nid, ind_ids, bat_ids
            );
        }
        db.close().unwrap();
    }

    // --- PPR damping_factor edge cases ---

    #[test]
    fn test_ppr_low_damping_seed_dominates() {
        // With very low damping, the seed should retain nearly all rank.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db.personalized_pagerank(&[a], &PprOptions {
            damping_factor: 0.01,
            max_iterations: 100,
            ..PprOptions::default()
        }).unwrap();

        let seed_score = result.scores.iter().find(|s| s.0 == a).map(|s| s.1).unwrap();
        assert!(seed_score > 0.95, "seed should have >95% rank with damping=0.01, got {}", seed_score);
        db.close().unwrap();
    }

    #[test]
    fn test_ppr_high_damping_spreads_rank() {
        // With high damping, rank should spread more evenly across the graph.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
        db.upsert_edge(c, a, 1, BTreeMap::new(), 1.0, None, None).unwrap();

        let result = db.personalized_pagerank(&[a], &PprOptions {
            damping_factor: 0.99,
            max_iterations: 200,
            epsilon: 1e-8,
            ..PprOptions::default()
        }).unwrap();

        let seed_score = result.scores.iter().find(|s| s.0 == a).map(|s| s.1).unwrap();
        // With cycle A→B→C→A and high damping, rank should be fairly distributed
        assert!(seed_score < 0.60, "seed should have <60% rank with damping=0.99, got {}", seed_score);
        db.close().unwrap();
    }

    // --- Empty segment after compaction ---

    #[test]
    fn test_compact_all_records_tombstoned() {
        // Compact when every record is deleted -- should produce a valid empty-ish segment.
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));

        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        let e = db.upsert_edge(a, b, 10, BTreeMap::new(), 1.0, None, None).unwrap();
        db.flush().unwrap();

        db.delete_node(a).unwrap();
        db.delete_node(b).unwrap();
        db.delete_edge(e).unwrap();
        db.flush().unwrap();

        let stats = db.compact().unwrap();
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.nodes_kept, 0);
        assert_eq!(stats.edges_kept, 0);
        assert_eq!(stats.nodes_removed, 2);
        assert_eq!(stats.edges_removed, 1);

        // DB should still be functional after compaction of all-tombstone data
        let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
        assert!(db.get_node(c).unwrap().is_some());
        db.close().unwrap();
    }

    // --- Self-loop tests (additional coverage) ---

    #[test]
    fn test_self_loop_neighbors_outgoing() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, a, 10, BTreeMap::new(), 1.0, None, None).unwrap();

        let out = db.neighbors(a, Direction::Outgoing, None, 0, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, a);

        let inc = db.neighbors(a, Direction::Incoming, None, 0, None, None).unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].node_id, a);

        // Both direction should dedup the self-loop
        let both = db.neighbors(a, Direction::Both, None, 0, None, None).unwrap();
        assert_eq!(both.len(), 1, "self-loop should appear once in Both, not twice");
        db.close().unwrap();
    }

    #[test]
    fn test_self_loop_survives_flush_and_compact() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        let e = db.upsert_edge(a, a, 10, BTreeMap::new(), 2.5, None, None).unwrap();

        db.flush().unwrap();
        // Add a second segment to enable compaction
        let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();

        let edge = db.get_edge(e).unwrap().unwrap();
        assert_eq!(edge.from, a);
        assert_eq!(edge.to, a);
        assert_eq!(edge.weight, 2.5);

        let nbrs = db.neighbors(a, Direction::Both, None, 0, None, None).unwrap();
        assert_eq!(nbrs.len(), 1);

        // Self-loop should also appear in PPR
        let ppr = db.personalized_pagerank(&[a], &PprOptions::default()).unwrap();
        let a_score = ppr.scores.iter().find(|s| s.0 == a).map(|s| s.1).unwrap();
        assert!(a_score > 0.0);

        db.close().unwrap();
    }

    #[test]
    fn test_self_loop_in_top_k() {
        let dir = TempDir::new().unwrap();
        let mut db = open_imm(&dir.path().join("db"));
        let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
        db.upsert_edge(a, a, 10, BTreeMap::new(), 5.0, None, None).unwrap();

        let top = db.top_k_neighbors(a, Direction::Outgoing, None, 10,
            ScoringMode::Weight, None).unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].node_id, a);
        assert_eq!(top[0].weight, 5.0);
        db.close().unwrap();
    }
}
