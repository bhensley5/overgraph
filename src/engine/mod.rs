use crate::degree_cache::{DegreeDelta, DegreeOverlayEdit, DegreeOverlaySnapshot};
use crate::dense_hnsw::exact_dense_search_above_cutoff;
use crate::edge_metadata::EdgeMetadataCandidate;
use crate::error::EngineError;
use crate::manifest::{default_manifest, load_manifest, load_manifest_readonly, write_manifest};
use crate::memtable::Memtable;
use crate::planner_stats::{
    planner_stats_declaration_fingerprint_for_entry,
    write_targeted_secondary_index_planner_stats_sidecar, DeclaredIndexRuntimeCoverage,
    EstimateConfidence, PlannerEstimateKind, PlannerStatsDeclaredIndexTarget,
    PlannerStatsDirection, PlannerStatsView, StalePostingRisk,
};
use crate::property_value_semantics::{
    compare_numeric_prop_values, hash_prop_equality_key, hash_semantic_equality_key_bytes,
    intersect_validated_numeric_ranges, numeric_range_sort_key_for_value,
    prop_value_within_validated_range, semantic_equality_key_bytes, semantic_property_eq,
    semantic_range_bound_key_bytes, structural_value_contains_float_zero,
    validate_numeric_range_bounds, NumericRangeSortKey, ValidatedNumericRange,
};
use crate::row_projection::{
    EdgeOutputProjection, EdgeProjectionField, EdgeSelectedFieldNeeds, EntityProjectionNeeds,
    NodeOutputProjection, NodeProjectionField, NodeSelectedFieldNeeds, PathSelectedFieldNeeds,
    ProjectedEdge, ProjectedNode, ProjectedRow, ProjectedRows, ProjectedValue, ProjectionColumn,
    ProjectionNeedClass, PropertySelection, RowProjectionPlan, VectorSelection, DIRECT_EDGE_ALIAS,
    DIRECT_NODE_ALIAS,
};
use crate::schema::{
    edge_schema_info_from_manifest, edge_schema_manifest_entry_from_public,
    node_schema_info_from_manifest, node_schema_manifest_entry_from_public,
    normalize_schema_manifest, validate_node_schema_dense_vector_config, EdgeSchema,
    EdgeSchemaInfo, GraphSchema, GraphSchemaCheckOptions, GraphSchemaCheckReport,
    GraphSchemaDropAction, GraphSchemaDropTargetResult, GraphSchemaOperation,
    GraphSchemaOperationKind, GraphSchemaPublishResult, GraphSchemaSetOptions,
    GraphSchemaValidationReportEntry, NodeSchema, NodeSchemaInfo, RuntimeSchemaCatalog,
    SchemaCheckOptions, SchemaSetOptions, SchemaTargetKind, SchemaValidationFailure,
    SchemaValidationReport, SchemaViolation, SchemaViolationTarget,
};
use crate::secondary_index_key::{
    compound_secondary_failure_message, compound_secondary_failure_message_from_str,
};
use crate::segment_components::{ComponentAvailability, SegmentComponentKind};
use crate::segment_reader::{SegmentLabelPosting, SegmentReader};
use crate::segment_writer::{
    build_edge_compound_entries_from_metadata, build_node_compound_entries_from_metadata,
    cleanup_orphan_optional_component_files, create_compaction_core_writer,
    finalize_compaction_segment, finish_compaction_core_writer,
    is_optional_component_publication_conflict,
    maintained_secondary_index_ids_from_segment_manifest, publish_compound_sidecar_component,
    publish_edge_prop_eq_sidecar_component, publish_edge_prop_range_sidecar_component,
    publish_node_prop_eq_sidecar_component, publish_node_prop_range_sidecar_component,
    remove_secondary_index_component_records, secondary_index_sidecar_paths_for_entry, segment_dir,
    segment_tmp_dir, write_compaction_source_components,
    write_indexes_from_metadata_with_secondary_indexes, write_merged_edges_dat,
    write_merged_nodes_dat, write_segment_with_degree_overlay_and_secondary_indexes,
    write_v3_edges_dat, write_v3_nodes_dat, CompactEdgeMeta, CompactNodeMeta, FastMergeCopyInfo,
    SecondaryIndexMaintenanceReport,
};
use crate::source_list::SourceList;
use crate::sparse_postings::sparse_dot_score;
use crate::types::*;
#[cfg(test)]
use crate::wal::wal_generation_path;
use crate::wal::{remove_wal_generation, truncate_wal_generation_to, WalReader, WalWriter};
use crate::wal_sync::{shutdown_sync_thread, sync_thread_loop, WalSyncState};
use arc_swap::ArcSwap;
use std::cmp::Reverse;
use std::collections::{
    hash_map::Entry, BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque,
};

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SecondaryIndexTargetDiscriminant {
    Node,
    Edge,
}

fn secondary_index_target_discriminant(
    target: &SecondaryIndexTarget,
) -> SecondaryIndexTargetDiscriminant {
    match target {
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::NodeFieldIndex { .. } => {
            SecondaryIndexTargetDiscriminant::Node
        }
        SecondaryIndexTarget::EdgeProperty { .. } | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            SecondaryIndexTargetDiscriminant::Edge
        }
    }
}

fn secondary_index_target_label_id(target: &SecondaryIndexTarget) -> u32 {
    target.label_id()
}

fn secondary_index_target_prop_key(target: &SecondaryIndexTarget) -> &str {
    target
        .single_property_key()
        .expect("single-property secondary index target")
}

fn secondary_index_target_requires_sidecar_build(target: &SecondaryIndexTarget) -> bool {
    matches!(
        target,
        SecondaryIndexTarget::NodeProperty { .. }
            | SecondaryIndexTarget::EdgeProperty { .. }
            | SecondaryIndexTarget::NodeFieldIndex { .. }
            | SecondaryIndexTarget::EdgeFieldIndex { .. }
    )
}

fn secondary_index_kind_rank(kind: &SecondaryIndexKind) -> (u8, u8) {
    match kind {
        SecondaryIndexKind::Equality => (0, 0),
        SecondaryIndexKind::Range => (1, 0),
    }
}

fn secondary_index_component_kind_for_recovery(
    entry: &SecondaryIndexManifestEntry,
) -> SegmentComponentKind {
    match (&entry.target, &entry.kind) {
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Equality) => {
            SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            }
        }
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Range) => {
            SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            }
        }
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
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            SegmentComponentKind::NodeCompoundEqualityIndex {
                index_id: entry.index_id,
            }
        }
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Range) => {
            SegmentComponentKind::NodeCompoundRangeIndex {
                index_id: entry.index_id,
            }
        }
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            SegmentComponentKind::EdgeCompoundEqualityIndex {
                index_id: entry.index_id,
            }
        }
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Range) => {
            SegmentComponentKind::EdgeCompoundRangeIndex {
                index_id: entry.index_id,
            }
        }
    }
}

fn secondary_index_fields_sort_key(target: &SecondaryIndexTarget) -> Vec<(u8, String)> {
    target
        .public_fields()
        .into_iter()
        .map(|field| match field {
            SecondaryIndexField::Property { key } => (0, key),
            SecondaryIndexField::NodeMetadata(field) => {
                (1, node_metadata_index_field_name(field).to_string())
            }
            SecondaryIndexField::EdgeMetadata(field) => {
                (2, edge_metadata_index_field_name(field).to_string())
            }
        })
        .collect()
}

#[derive(Default)]
struct SecondaryIndexCatalog {
    by_identity: HashMap<SecondaryIndexLookupKey, SecondaryIndexManifestEntry>,
    node_property: PropertyIndexCatalog,
    edge_property: PropertyIndexCatalog,
    node_field: FieldIndexCatalog,
    edge_field: FieldIndexCatalog,
}

#[derive(Default)]
struct PropertyIndexCatalog {
    equality: HashMap<u32, HashMap<String, SecondaryIndexManifestEntry>>,
    range: HashMap<u32, HashMap<String, SecondaryIndexManifestEntry>>,
}

#[derive(Default)]
struct FieldIndexCatalog {
    equality: HashMap<u32, Vec<SecondaryIndexManifestEntry>>,
    range: HashMap<u32, Vec<SecondaryIndexManifestEntry>>,
}

impl FieldIndexCatalog {
    fn insert(
        &mut self,
        label_id: u32,
        kind: &SecondaryIndexKind,
        entry: SecondaryIndexManifestEntry,
    ) {
        self.by_kind_mut(kind)
            .entry(label_id)
            .or_default()
            .push(entry);
    }

    fn get(&self, label_id: u32, kind: &SecondaryIndexKind) -> &[SecondaryIndexManifestEntry] {
        self.by_kind(kind).get(&label_id).map_or(&[], Vec::as_slice)
    }

    fn by_kind(
        &self,
        kind: &SecondaryIndexKind,
    ) -> &HashMap<u32, Vec<SecondaryIndexManifestEntry>> {
        match kind {
            SecondaryIndexKind::Equality => &self.equality,
            SecondaryIndexKind::Range => &self.range,
        }
    }

    fn by_kind_mut(
        &mut self,
        kind: &SecondaryIndexKind,
    ) -> &mut HashMap<u32, Vec<SecondaryIndexManifestEntry>> {
        match kind {
            SecondaryIndexKind::Equality => &mut self.equality,
            SecondaryIndexKind::Range => &mut self.range,
        }
    }
}

impl PropertyIndexCatalog {
    fn insert(
        &mut self,
        label_id: u32,
        prop_key: String,
        kind: &SecondaryIndexKind,
        entry: SecondaryIndexManifestEntry,
    ) {
        self.by_kind_mut(kind)
            .entry(label_id)
            .or_default()
            .insert(prop_key, entry);
    }

    fn get(
        &self,
        label_id: u32,
        prop_key: &str,
        kind: &SecondaryIndexKind,
    ) -> Option<SecondaryIndexManifestEntry> {
        self.by_kind(kind).get(&label_id)?.get(prop_key).cloned()
    }

    fn by_kind(
        &self,
        kind: &SecondaryIndexKind,
    ) -> &HashMap<u32, HashMap<String, SecondaryIndexManifestEntry>> {
        match kind {
            SecondaryIndexKind::Equality => &self.equality,
            SecondaryIndexKind::Range => &self.range,
        }
    }

    fn by_kind_mut(
        &mut self,
        kind: &SecondaryIndexKind,
    ) -> &mut HashMap<u32, HashMap<String, SecondaryIndexManifestEntry>> {
        match kind {
            SecondaryIndexKind::Equality => &mut self.equality,
            SecondaryIndexKind::Range => &mut self.range,
        }
    }
}

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
fn merge_record_ids_paged(
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
    SecondaryIndexLookupKey {
        discriminant: secondary_index_target_discriminant(&entry.target),
        target_label_id: secondary_index_target_label_id(&entry.target),
        fields: entry.target.public_fields(),
        kind: entry.kind.clone(),
    }
}

fn normalize_secondary_index_manifest(manifest: &mut ManifestState) -> Result<bool, EngineError> {
    let mut dirty = false;
    let mut seen_ids = HashSet::new();
    let mut seen_keys = HashSet::new();
    let mut max_index_id = 0u64;

    for entry in &mut manifest.secondary_indexes {
        validate_secondary_index_target(&entry.target)?;
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
    let mut catalog = SecondaryIndexCatalog {
        by_identity: HashMap::with_capacity(entries.len()),
        node_property: PropertyIndexCatalog::default(),
        edge_property: PropertyIndexCatalog::default(),
        node_field: FieldIndexCatalog::default(),
        edge_field: FieldIndexCatalog::default(),
    };
    for entry in entries {
        validate_secondary_index_target(&entry.target)?;
        let key = secondary_index_lookup_key(entry);
        if catalog.by_identity.insert(key, entry.clone()).is_some() {
            return Err(EngineError::ManifestError(format!(
                "duplicate secondary index declaration loaded from manifest: {:?}",
                entry.target
            )));
        }
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, prop_key } => {
                catalog.node_property.insert(
                    *label_id,
                    prop_key.clone(),
                    &entry.kind,
                    entry.clone(),
                );
            }
            SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => {
                catalog.edge_property.insert(
                    *label_id,
                    prop_key.clone(),
                    &entry.kind,
                    entry.clone(),
                );
            }
            SecondaryIndexTarget::NodeFieldIndex { label_id, .. } => {
                catalog
                    .node_field
                    .insert(*label_id, &entry.kind, entry.clone());
            }
            SecondaryIndexTarget::EdgeFieldIndex { label_id, .. } => {
                catalog
                    .edge_field
                    .insert(*label_id, &entry.kind, entry.clone());
            }
        }
    }
    for entries in catalog.node_field.equality.values_mut() {
        entries.sort_by_key(|entry| entry.index_id);
    }
    for entries in catalog.node_field.range.values_mut() {
        entries.sort_by_key(|entry| entry.index_id);
    }
    for entries in catalog.edge_field.equality.values_mut() {
        entries.sort_by_key(|entry| entry.index_id);
    }
    for entries in catalog.edge_field.range.values_mut() {
        entries.sort_by_key(|entry| entry.index_id);
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

#[derive(Debug, Clone)]
pub(crate) struct RuntimeLabelCatalog {
    pub node_label_to_id: BTreeMap<String, u32>,
    pub node_id_to_label: BTreeMap<u32, String>,
    node_label_wal_generation: BTreeMap<String, u64>,
    pub edge_label_to_id: BTreeMap<String, u32>,
    pub edge_id_to_label: BTreeMap<u32, String>,
    edge_label_wal_generation: BTreeMap<String, u64>,
    pub next_node_label_id: u32,
    pub next_edge_label_id: u32,
}

impl RuntimeLabelCatalog {
    fn from_manifest(manifest: &ManifestState) -> Result<Self, EngineError> {
        let mut catalog = Self {
            node_label_to_id: manifest.node_label_tokens.clone(),
            node_id_to_label: BTreeMap::new(),
            node_label_wal_generation: BTreeMap::new(),
            edge_label_to_id: manifest.edge_label_tokens.clone(),
            edge_id_to_label: BTreeMap::new(),
            edge_label_wal_generation: BTreeMap::new(),
            next_node_label_id: manifest.next_node_label_id,
            next_edge_label_id: manifest.next_edge_label_id,
        };
        catalog.rebuild_reverse_maps()?;
        Ok(catalog)
    }

    fn rebuild_reverse_maps(&mut self) -> Result<(), EngineError> {
        self.node_id_to_label.clear();
        for (label, &label_id) in &self.node_label_to_id {
            if let Some(existing) = self.node_id_to_label.insert(label_id, label.clone()) {
                return Err(EngineError::ManifestError(format!(
                    "node label token conflict: label_id {label_id} is assigned to both '{existing}' and '{label}'"
                )));
            }
        }
        self.edge_id_to_label.clear();
        for (label, &label_id) in &self.edge_label_to_id {
            if let Some(existing) = self.edge_id_to_label.insert(label_id, label.clone()) {
                return Err(EngineError::ManifestError(format!(
                    "edge-label token conflict: label_id {label_id} is assigned to both '{existing}' and '{label}'"
                )));
            }
        }
        Ok(())
    }

    fn apply_to_manifest(&self, manifest: &mut ManifestState) {
        manifest.label_token_schema_version = LABEL_TOKEN_SCHEMA_VERSION;
        manifest.node_label_tokens = self.node_label_to_id.clone();
        manifest.edge_label_tokens = self.edge_label_to_id.clone();
        manifest.next_node_label_id = self.next_node_label_id;
        manifest.next_edge_label_id = self.next_edge_label_id;
    }

    fn apply_checkpointed_to_manifest(
        &self,
        manifest: &mut ManifestState,
        max_wal_generation: Option<u64>,
    ) {
        manifest.label_token_schema_version = LABEL_TOKEN_SCHEMA_VERSION;
        for (label, &label_id) in &self.node_label_to_id {
            if manifest.node_label_tokens.get(label) == Some(&label_id)
                || self
                    .node_label_wal_generation
                    .get(label)
                    .is_some_and(|generation| {
                        max_wal_generation
                            .is_some_and(|max_generation| *generation <= max_generation)
                    })
            {
                manifest.node_label_tokens.insert(label.clone(), label_id);
            }
        }
        for (label, &label_id) in &self.edge_label_to_id {
            if manifest.edge_label_tokens.get(label) == Some(&label_id)
                || self
                    .edge_label_wal_generation
                    .get(label)
                    .is_some_and(|generation| {
                        max_wal_generation
                            .is_some_and(|max_generation| *generation <= max_generation)
                    })
            {
                manifest.edge_label_tokens.insert(label.clone(), label_id);
            }
        }
        manifest.next_node_label_id = manifest.next_node_label_id.max(
            manifest
                .node_label_tokens
                .values()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_add(1),
        );
        manifest.next_edge_label_id = manifest.next_edge_label_id.max(
            manifest
                .edge_label_tokens
                .values()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_add(1),
        );
    }

    fn reserve_node_label(&self, label: &str) -> Result<(u32, bool), EngineError> {
        if let Some(&label_id) = self.node_label_to_id.get(label) {
            return Ok((label_id, false));
        }
        validate_label_token_name(label)?;
        if self.next_node_label_id == u32::MAX {
            return Err(EngineError::InvalidOperation(
                "node label token ID space exhausted".to_string(),
            ));
        }
        Ok((self.next_node_label_id, true))
    }

    fn reserve_edge_label(&self, label: &str) -> Result<(u32, bool), EngineError> {
        if let Some(&label_id) = self.edge_label_to_id.get(label) {
            return Ok((label_id, false));
        }
        validate_label_token_name(label)?;
        if self.next_edge_label_id == u32::MAX {
            return Err(EngineError::InvalidOperation(
                "edge-label token ID space exhausted".to_string(),
            ));
        }
        Ok((self.next_edge_label_id, true))
    }

    fn apply_node_label(
        &mut self,
        label: String,
        label_id: u32,
        wal_generation: Option<u64>,
    ) -> Result<(), EngineError> {
        validate_label_token_name(&label)?;
        if label_id == 0 {
            return Err(EngineError::InvalidOperation(
                "node label token ID 0 is reserved".to_string(),
            ));
        }
        if let Some(existing_id) = self.node_label_to_id.get(&label) {
            if *existing_id == label_id {
                if let Some(wal_generation) = wal_generation {
                    self.node_label_wal_generation
                        .entry(label)
                        .and_modify(|existing| *existing = (*existing).min(wal_generation))
                        .or_insert(wal_generation);
                }
                return Ok(());
            }
            return Err(EngineError::CorruptWal(format!(
                "node label token conflict: label '{label}' is assigned to both label_id {existing_id} and {label_id}"
            )));
        }
        if let Some(existing_label) = self.node_id_to_label.get(&label_id) {
            return Err(EngineError::CorruptWal(format!(
                "node label token conflict: label_id {label_id} is assigned to both '{existing_label}' and '{label}'"
            )));
        }
        self.node_label_to_id.insert(label.clone(), label_id);
        self.node_id_to_label.insert(label_id, label);
        if let Some(wal_generation) = wal_generation {
            let stored_label = self
                .node_id_to_label
                .get(&label_id)
                .expect("node label reverse map was just inserted")
                .clone();
            self.node_label_wal_generation
                .insert(stored_label, wal_generation);
        }
        self.next_node_label_id = self.next_node_label_id.max(label_id.saturating_add(1));
        Ok(())
    }

    fn apply_edge_label(
        &mut self,
        label: String,
        label_id: u32,
        wal_generation: Option<u64>,
    ) -> Result<(), EngineError> {
        validate_label_token_name(&label)?;
        if label_id == 0 {
            return Err(EngineError::InvalidOperation(
                "edge-label token ID 0 is reserved".to_string(),
            ));
        }
        if let Some(existing_id) = self.edge_label_to_id.get(&label) {
            if *existing_id == label_id {
                if let Some(wal_generation) = wal_generation {
                    self.edge_label_wal_generation
                        .entry(label)
                        .and_modify(|existing| *existing = (*existing).min(wal_generation))
                        .or_insert(wal_generation);
                }
                return Ok(());
            }
            return Err(EngineError::CorruptWal(format!(
                "edge-label token conflict: edge label '{label}' is assigned to both label_id {existing_id} and {label_id}"
            )));
        }
        if let Some(existing_label) = self.edge_id_to_label.get(&label_id) {
            return Err(EngineError::CorruptWal(format!(
                "edge-label token conflict: label_id {label_id} is assigned to both '{existing_label}' and '{label}'"
            )));
        }
        self.edge_label_to_id.insert(label.clone(), label_id);
        self.edge_id_to_label.insert(label_id, label);
        if let Some(wal_generation) = wal_generation {
            let stored_label = self
                .edge_id_to_label
                .get(&label_id)
                .expect("edge-label reverse map was just inserted")
                .clone();
            self.edge_label_wal_generation
                .insert(stored_label, wal_generation);
        }
        self.next_edge_label_id = self.next_edge_label_id.max(label_id.saturating_add(1));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ReadLabelCatalogSnapshot {
    node_label_to_id: HashMap<String, u32>,
    node_id_to_label: ReadLabelNameLookup,
    edge_label_to_id: HashMap<String, u32>,
    edge_id_to_label: ReadLabelNameLookup,
}

#[derive(Debug, Clone)]
enum ReadLabelNameLookup {
    Dense(Box<[Option<Arc<str>>]>),
    Sparse(HashMap<u32, Arc<str>>),
}

impl ReadLabelNameLookup {
    const MAX_DENSE_LEN: usize = 1_000_000;

    fn from_runtime_map(names: &BTreeMap<u32, String>) -> Self {
        let Some(max_label_id) = names.keys().next_back().copied() else {
            return Self::Dense(Vec::new().into_boxed_slice());
        };
        let dense_len = usize::try_from(max_label_id)
            .ok()
            .and_then(|max| max.checked_add(1));
        let dense_threshold = names.len().saturating_mul(8).saturating_add(64);

        if let Some(len) =
            dense_len.filter(|&len| len <= Self::MAX_DENSE_LEN && len <= dense_threshold)
        {
            let mut dense = vec![None; len];
            for (&label_id, name) in names {
                dense[label_id as usize] = Some(Arc::<str>::from(name.as_str()));
            }
            return Self::Dense(dense.into_boxed_slice());
        }

        Self::Sparse(
            names
                .iter()
                .map(|(&label_id, name)| (label_id, Arc::<str>::from(name.as_str())))
                .collect(),
        )
    }

    fn get(&self, label_id: u32) -> Option<&str> {
        match self {
            Self::Dense(names) => usize::try_from(label_id)
                .ok()
                .and_then(|idx| names.get(idx))
                .and_then(Option::as_deref),
            Self::Sparse(names) => names.get(&label_id).map(AsRef::as_ref),
        }
    }
}

trait LabelCatalogLookup {
    fn node_label(&self, label_id: u32) -> Option<&str>;
    fn edge_label(&self, label_id: u32) -> Option<&str>;
}

impl LabelCatalogLookup for RuntimeLabelCatalog {
    fn node_label(&self, label_id: u32) -> Option<&str> {
        self.node_id_to_label.get(&label_id).map(String::as_str)
    }

    fn edge_label(&self, label_id: u32) -> Option<&str> {
        self.edge_id_to_label.get(&label_id).map(String::as_str)
    }
}

impl LabelCatalogLookup for ReadLabelCatalogSnapshot {
    fn node_label(&self, label_id: u32) -> Option<&str> {
        self.node_id_to_label.get(label_id)
    }

    fn edge_label(&self, label_id: u32) -> Option<&str> {
        self.edge_id_to_label.get(label_id)
    }
}

impl ReadLabelCatalogSnapshot {
    fn from_runtime(catalog: &RuntimeLabelCatalog) -> Self {
        Self {
            node_label_to_id: catalog.node_label_to_id.clone().into_iter().collect(),
            node_id_to_label: ReadLabelNameLookup::from_runtime_map(&catalog.node_id_to_label),
            edge_label_to_id: catalog.edge_label_to_id.clone().into_iter().collect(),
            edge_id_to_label: ReadLabelNameLookup::from_runtime_map(&catalog.edge_id_to_label),
        }
    }

    fn resolve_node_label_for_read(&self, label: &str) -> Result<Option<u32>, EngineError> {
        validate_label_token_name(label)?;
        Ok(self.node_label_to_id.get(label).copied())
    }

    fn resolve_edge_label_for_read(&self, label: &str) -> Result<Option<u32>, EngineError> {
        validate_label_token_name(label)?;
        Ok(self.edge_label_to_id.get(label).copied())
    }

    fn resolve_edge_label_filter(
        &self,
        edge_labels: Option<&[String]>,
    ) -> Result<(LabelFilterResolution, Vec<QueryPlanWarning>), EngineError> {
        let Some(edge_labels) = edge_labels else {
            return Ok((LabelFilterResolution::Unconstrained, Vec::new()));
        };
        if edge_labels.is_empty() {
            return Ok((LabelFilterResolution::Unconstrained, Vec::new()));
        }

        let mut known = Vec::new();
        let mut warnings = Vec::new();
        for label in edge_labels {
            match self.resolve_edge_label_for_read(label)? {
                Some(label_id) => known.push(label_id),
                None => push_query_warning(&mut warnings, QueryPlanWarning::UnknownEdgeLabel),
            }
        }

        if known.is_empty() {
            return Ok((LabelFilterResolution::EmptyConstraint, warnings));
        }
        known.sort_unstable();
        known.dedup();
        Ok((LabelFilterResolution::Known(known), warnings))
    }

    fn resolve_edge_label_filter_for_read(
        &self,
        edge_labels: Option<&[String]>,
    ) -> Result<LabelFilterResolution, EngineError> {
        Ok(self.resolve_edge_label_filter(edge_labels)?.0)
    }

    #[allow(dead_code)]
    fn resolve_node_label_filter_request(
        &self,
        filter: Option<&NodeLabelFilter>,
    ) -> Result<ResolvedNodeLabelFilter, EngineError> {
        let Some(filter) = filter else {
            return Ok(ResolvedNodeLabelFilter::Unconstrained);
        };
        validate_node_label_filter(filter)?;

        let mut known = Vec::with_capacity(filter.labels.len());
        let mut unknown_label_count = 0usize;
        for label in &filter.labels {
            match self.resolve_node_label_for_read(label)? {
                Some(label_id) => known.push(label_id),
                None => unknown_label_count += 1,
            }
        }

        if known.is_empty() {
            return Ok(ResolvedNodeLabelFilter::empty(
                filter.mode,
                unknown_label_count,
            ));
        }
        if filter.mode == LabelMatchMode::All && unknown_label_count > 0 {
            return Ok(ResolvedNodeLabelFilter::empty(
                filter.mode,
                unknown_label_count,
            ));
        }
        Ok(ResolvedNodeLabelFilter::known(
            filter.mode,
            NodeLabelSet::from_label_ids(known)?,
            unknown_label_count,
        ))
    }
}

fn node_view_from_record(
    record: NodeRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<NodeView, EngineError> {
    let labels = match record.label_ids.as_slice() {
        &[label_id] => vec![catalog
            .node_label(label_id)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "node record {} references missing node label_id {}",
                    record.id, label_id
                ))
            })?
            .to_string()],
        label_ids => {
            let mut labels = Vec::with_capacity(label_ids.len());
            for &label_id in label_ids {
                labels.push(
                    catalog
                        .node_label(label_id)
                        .ok_or_else(|| {
                            EngineError::InvalidOperation(format!(
                                "node record {} references missing node label_id {}",
                                record.id, label_id
                            ))
                        })?
                        .to_string(),
                );
            }
            labels
        }
    };

    Ok(NodeView {
        id: record.id,
        labels,
        key: record.key,
        props: record.props,
        created_at: record.created_at,
        updated_at: record.updated_at,
        weight: record.weight,
        dense_vector: record.dense_vector,
        sparse_vector: record.sparse_vector,
    })
}

fn node_view_from_record_with_resolved_label(
    record: NodeRecord,
    expected_label_id: u32,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<NodeView, EngineError> {
    if !record.label_ids.contains(expected_label_id) {
        return Err(EngineError::InvalidOperation(format!(
            "node record {} resolved by label_id {} but found {:?}",
            record.id, expected_label_id, record.label_ids
        )));
    }

    node_view_from_record(record, catalog)
}

fn edge_view_from_record(
    record: EdgeRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<EdgeView, EngineError> {
    let label = catalog
        .edge_label(record.label_id)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "edge record {} references missing edge-label label_id {}",
                record.id, record.label_id
            ))
        })?
        .to_string();

    Ok(EdgeView {
        id: record.id,
        from: record.from,
        to: record.to,
        label,
        props: record.props,
        created_at: record.created_at,
        updated_at: record.updated_at,
        weight: record.weight,
        valid_from: record.valid_from,
        valid_to: record.valid_to,
    })
}

fn edge_view_from_record_with_resolved_label(
    record: EdgeRecord,
    expected_label_id: u32,
    label: String,
) -> Result<EdgeView, EngineError> {
    if record.label_id != expected_label_id {
        return Err(EngineError::InvalidOperation(format!(
            "edge record {} resolved by edge label '{}' expected label_id {} but found {}",
            record.id, label, expected_label_id, record.label_id
        )));
    }

    Ok(EdgeView {
        id: record.id,
        from: record.from,
        to: record.to,
        label,
        props: record.props,
        created_at: record.created_at,
        updated_at: record.updated_at,
        weight: record.weight,
        valid_from: record.valid_from,
        valid_to: record.valid_to,
    })
}

fn neighbor_entry_from_record(
    record: NeighborRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<NeighborEntry, EngineError> {
    let label = catalog
        .edge_label(record.edge_label_id)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "neighbor edge {} references missing edge-label label_id {}",
                record.edge_id, record.edge_label_id
            ))
        })?
        .to_string();

    Ok(NeighborEntry {
        node_id: record.node_id,
        edge_id: record.edge_id,
        label,
        weight: record.weight,
        valid_from: record.valid_from,
        valid_to: record.valid_to,
    })
}

fn resolve_node_label_for_read(
    catalog: &RuntimeLabelCatalog,
    label: &str,
) -> Result<Option<u32>, EngineError> {
    validate_label_token_name(label)?;
    Ok(catalog.node_label_to_id.get(label).copied())
}

fn resolve_edge_label_for_read(
    catalog: &RuntimeLabelCatalog,
    label: &str,
) -> Result<Option<u32>, EngineError> {
    validate_label_token_name(label)?;
    Ok(catalog.edge_label_to_id.get(label).copied())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LabelFilterResolution {
    Unconstrained,
    Known(Vec<u32>),
    EmptyConstraint,
}

fn merge_runtime_label_catalog_into_manifest(
    manifest: &mut ManifestState,
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
) {
    label_catalog.read().unwrap().apply_to_manifest(manifest);
}

fn merge_checkpointed_label_catalog_into_manifest(
    manifest: &mut ManifestState,
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
    max_wal_generation: Option<u64>,
) {
    label_catalog
        .read()
        .unwrap()
        .apply_checkpointed_to_manifest(manifest, max_wal_generation);
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
    label_catalog: Option<&Arc<RwLock<RuntimeLabelCatalog>>>,
    max_token_checkpoint_wal_generation: Option<u64>,
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
    if let Some(label_catalog) = label_catalog {
        merge_checkpointed_label_catalog_into_manifest(
            &mut manifest,
            label_catalog,
            max_token_checkpoint_wal_generation,
        );
    }
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

fn manifestless_database_artifacts(path: &Path) -> Result<Vec<String>, EngineError> {
    let mut artifacts = Vec::new();
    if path.join("data.wal").exists() {
        artifacts.push("data.wal".to_string());
    }
    if path.join("segments").exists() {
        artifacts.push("segments/".to_string());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with("wal_") && name.ends_with(".wal") {
            artifacts.push(name.to_string());
        }
    }
    artifacts.sort();
    artifacts.dedup();
    Ok(artifacts)
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
            if matches!(entry.kind, SecondaryIndexKind::Range) {
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some(message.clone());
            }
        }
    }
}

fn reconcile_background_output_equality_declarations(
    manifest: &mut ManifestState,
    maintained_equality_index_ids: &NodeIdSet,
) -> Vec<u64> {
    let mut rebuild_index_ids = Vec::new();
    for entry in &mut manifest.secondary_indexes {
        if !matches!(entry.kind, SecondaryIndexKind::Equality)
            || maintained_equality_index_ids.contains(&entry.index_id)
            || !secondary_index_target_requires_sidecar_build(&entry.target)
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
        if !matches!(entry.kind, SecondaryIndexKind::Range)
            || maintained_range_index_ids.contains(&entry.index_id)
            || !secondary_index_target_requires_sidecar_build(&entry.target)
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
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
    index_id: u64,
    error: &EngineError,
) {
    let raw_message = error.to_string();
    let _ = update_secondary_index_manifest_runtime(
        db_dir,
        manifest_write_lock,
        catalog_lock,
        entries_lock,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
        Some(label_catalog),
        None,
        |manifest| {
            if let Some(entry) = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == index_id)
            {
                let message = secondary_index_failure_message_for_entry(entry, raw_message.clone());
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some(message);
            }
            Ok(())
        },
    );
}

fn build_secondary_eq_groups_for_segment(
    segment: &SegmentReader,
    target_label_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for index in 0..segment.node_meta_count() as usize {
        let meta = segment.node_meta_at(index)?;
        if !meta.label_ids.contains(target_label_id) {
            continue;
        }

        if let Some(value_hash) = segment
            .node_property_value_at_offset(meta.node_id, meta.data_offset, prop_key)?
            .map(|value| hash_prop_equality_key(&value))
        {
            groups.entry(value_hash).or_default().push(meta.node_id);
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
    target_label_id: u32,
    prop_key: &str,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let mut entries = Vec::new();

    for index in 0..segment.node_meta_count() as usize {
        let meta = segment.node_meta_at(index)?;
        if !meta.label_ids.contains(target_label_id) {
            continue;
        }

        let Some(value) =
            segment.node_property_value_at_offset(meta.node_id, meta.data_offset, prop_key)?
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

fn build_edge_secondary_eq_groups_for_segment(
    segment: &SegmentReader,
    label_id: u32,
    prop_key: &str,
) -> Result<BTreeMap<u64, Vec<u64>>, EngineError> {
    let mut groups: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for index in 0..segment.edge_meta_count() as usize {
        let (
            edge_id,
            data_offset,
            _data_len,
            _from,
            _to,
            edge_label_id,
            _updated_at,
            _weight,
            _valid_from,
            _valid_to,
            _last_write_seq,
        ) = segment.edge_meta_at(index)?;
        if edge_label_id != label_id {
            continue;
        }

        if let Some(value) =
            segment.edge_property_value_at_offset(edge_id, data_offset, prop_key)?
        {
            groups
                .entry(hash_prop_equality_key(&value))
                .or_default()
                .push(edge_id);
        }
    }

    for ids in groups.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    Ok(groups)
}

fn build_edge_secondary_range_entries_for_segment(
    segment: &SegmentReader,
    label_id: u32,
    prop_key: &str,
) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
    let mut entries = Vec::new();

    for index in 0..segment.edge_meta_count() as usize {
        let (
            edge_id,
            data_offset,
            _data_len,
            _from,
            _to,
            edge_label_id,
            _updated_at,
            _weight,
            _valid_from,
            _valid_to,
            _last_write_seq,
        ) = segment.edge_meta_at(index)?;
        if edge_label_id != label_id {
            continue;
        }

        let Some(value) = segment.edge_property_value_at_offset(edge_id, data_offset, prop_key)?
        else {
            continue;
        };
        let Some(encoded_value) = numeric_range_sort_key_for_value(&value) else {
            continue;
        };
        entries.push((encoded_value, edge_id));
    }

    entries.sort_unstable();
    entries.dedup();
    Ok(entries)
}

fn install_secondary_eq_sidecar(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    publish_node_prop_eq_sidecar_component(seg_dir, entry, groups)
}

fn install_secondary_range_sidecar(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    publish_node_prop_range_sidecar_component(seg_dir, entry, entries)
}

fn install_edge_secondary_eq_sidecar(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), EngineError> {
    publish_edge_prop_eq_sidecar_component(seg_dir, entry, groups)
}

fn install_edge_secondary_range_sidecar(
    seg_dir: &Path,
    entry: &SecondaryIndexManifestEntry,
    entries: &[(NumericRangeSortKey, u64)],
) -> Result<(), EngineError> {
    publish_edge_prop_range_sidecar_component(seg_dir, entry, entries)
}

#[derive(Clone)]
struct SecondaryEqBuildSnapshot {
    dense_config: Option<DenseVectorConfig>,
    target: SecondaryIndexTargetDiscriminant,
    target_label_id: u32,
    prop_key: String,
    segment_ids: Vec<u64>,
    segment_infos: Vec<SegmentInfo>,
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
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
    target: SecondaryIndexTargetDiscriminant,
    target_label_id: u32,
    prop_key: String,
    segment_ids: Vec<u64>,
    segment_infos: Vec<SegmentInfo>,
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
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

#[derive(Clone)]
struct CompoundSecondaryBuildSnapshot {
    dense_config: Option<DenseVectorConfig>,
    entry: SecondaryIndexManifestEntry,
    target_label_id: u32,
    segment_ids: Vec<u64>,
    segment_infos: Vec<SegmentInfo>,
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
}

enum CompoundSecondaryCoverageStatus {
    Covered,
    Incomplete,
    Failed(String),
    Cancelled,
}

enum CompoundSecondaryFinalizeOutcome {
    ReadyApplied(SecondaryIndexReadyApplied),
    Applied,
    Retry,
    Inactive,
}

fn segment_info_for_id(segment_infos: &[SegmentInfo], segment_id: u64) -> Option<&SegmentInfo> {
    segment_infos
        .iter()
        .find(|segment| segment.id == segment_id)
}

fn planner_stats_target_from_discriminant(
    target: SecondaryIndexTargetDiscriminant,
) -> PlannerStatsDeclaredIndexTarget {
    match target {
        SecondaryIndexTargetDiscriminant::Node => PlannerStatsDeclaredIndexTarget::NodeProperty,
        SecondaryIndexTargetDiscriminant::Edge => PlannerStatsDeclaredIndexTarget::EdgeProperty,
    }
}

#[derive(Clone, Debug)]
struct SecondaryIndexReadyApplied {
    index_id: u64,
    target: SecondaryIndexTarget,
    kind: SecondaryIndexKind,
    target_label_id: u32,
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
        let target_label_id = secondary_index_target_label_id(&entry.target);
        Some(Self {
            index_id: entry.index_id,
            target: entry.target.clone(),
            kind: entry.kind.clone(),
            target_label_id,
            declaration_fingerprint: planner_stats_declaration_fingerprint_for_entry(entry),
            snapshot_segment_ids,
        })
    }

    fn matches_entry(&self, entry: &SecondaryIndexManifestEntry) -> bool {
        if entry.state != SecondaryIndexState::Ready
            || entry.index_id != self.index_id
            || entry.target != self.target
            || entry.kind != self.kind
        {
            return false;
        }
        let target_label_id = secondary_index_target_label_id(&entry.target);
        target_label_id == self.target_label_id
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
    if !matches!(
        &entry.target,
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. }
    ) {
        return Ok(None);
    }

    let target = secondary_index_target_discriminant(&entry.target);
    let target_label_id = secondary_index_target_label_id(&entry.target);
    let prop_key = secondary_index_target_prop_key(&entry.target).to_string();
    let mut segment_ids: Vec<u64> = manifest.segments.iter().map(|segment| segment.id).collect();
    segment_ids.sort_unstable();
    let mut segment_infos = manifest.segments.clone();
    segment_infos.sort_by_key(|segment| segment.id);
    Ok(Some(SecondaryEqBuildSnapshot {
        dense_config: manifest.dense_vector.clone(),
        target,
        target_label_id,
        prop_key,
        segment_ids,
        segment_infos,
        secondary_indexes: manifest.secondary_indexes.clone(),
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
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            continue;
        };

        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => continue,
            Err(error) => return Err(error),
        };

        if snapshot.target == SecondaryIndexTargetDiscriminant::Edge {
            match segment.validate_secondary_eq_sidecar_for_target(
                index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
            ) {
                Ok(true) => continue,
                Ok(false) => {
                    let Some(entry) = snapshot
                        .secondary_indexes
                        .iter()
                        .find(|entry| entry.index_id == index_id)
                    else {
                        continue;
                    };
                    let groups = build_edge_secondary_eq_groups_for_segment(
                        &segment,
                        snapshot.target_label_id,
                        &snapshot.prop_key,
                    )?;
                    match install_edge_secondary_eq_sidecar(&seg_path, entry, &groups) {
                        Ok(()) => {}
                        Err(error) if is_not_found_io_error(&error) => {}
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if is_not_found_io_error(&error) => {}
                Err(_) => {
                    let Some(entry) = snapshot
                        .secondary_indexes
                        .iter()
                        .find(|entry| entry.index_id == index_id)
                    else {
                        continue;
                    };
                    let groups = build_edge_secondary_eq_groups_for_segment(
                        &segment,
                        snapshot.target_label_id,
                        &snapshot.prop_key,
                    )?;
                    match install_edge_secondary_eq_sidecar(&seg_path, entry, &groups) {
                        Ok(()) => {}
                        Err(error) if is_not_found_io_error(&error) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            continue;
        }

        match segment.validate_secondary_eq_sidecar_for_target(
            index_id,
            planner_stats_target_from_discriminant(snapshot.target),
        ) {
            Ok(true) => continue,
            Ok(false) => {
                let Some(entry) = snapshot
                    .secondary_indexes
                    .iter()
                    .find(|entry| entry.index_id == index_id)
                else {
                    continue;
                };
                let groups = build_secondary_eq_groups_for_segment(
                    &segment,
                    snapshot.target_label_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_eq_sidecar(&seg_path, entry, &groups) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_not_found_io_error(&error) => {}
            Err(_) => {
                let Some(entry) = snapshot
                    .secondary_indexes
                    .iter()
                    .find(|entry| entry.index_id == index_id)
                else {
                    continue;
                };
                let groups = build_secondary_eq_groups_for_segment(
                    &segment,
                    snapshot.target_label_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_eq_sidecar(&seg_path, entry, &groups) {
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
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            all_present = false;
            continue;
        };

        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => {
                all_present = false;
                continue;
            }
            Err(error) => return Err(error),
        };

        match segment.validate_secondary_eq_sidecar_for_target(
            index_id,
            planner_stats_target_from_discriminant(snapshot.target),
        ) {
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
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
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
        Some(label_catalog),
        None,
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

    if !matches!(&entry.kind, SecondaryIndexKind::Range) {
        return Ok(None);
    }
    if !matches!(
        &entry.target,
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. }
    ) {
        return Ok(None);
    }
    let target = secondary_index_target_discriminant(&entry.target);
    let target_label_id = secondary_index_target_label_id(&entry.target);
    let prop_key = secondary_index_target_prop_key(&entry.target).to_string();
    let mut segment_ids: Vec<u64> = manifest.segments.iter().map(|segment| segment.id).collect();
    segment_ids.sort_unstable();
    let mut segment_infos = manifest.segments.clone();
    segment_infos.sort_by_key(|segment| segment.id);
    Ok(Some(SecondaryRangeBuildSnapshot {
        dense_config: manifest.dense_vector.clone(),
        target,
        target_label_id,
        prop_key,
        segment_ids,
        segment_infos,
        secondary_indexes: manifest.secondary_indexes.clone(),
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
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            continue;
        };

        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => continue,
            Err(error) => return Err(error),
        };

        if snapshot.target == SecondaryIndexTargetDiscriminant::Edge {
            match segment.validate_secondary_range_sidecar_for_target(
                index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
            ) {
                Ok(true) => continue,
                Ok(false) => {
                    let Some(entry) = snapshot
                        .secondary_indexes
                        .iter()
                        .find(|entry| entry.index_id == index_id)
                    else {
                        continue;
                    };
                    let entries = build_edge_secondary_range_entries_for_segment(
                        &segment,
                        snapshot.target_label_id,
                        &snapshot.prop_key,
                    )?;
                    match install_edge_secondary_range_sidecar(&seg_path, entry, &entries) {
                        Ok(()) => {}
                        Err(error) if is_not_found_io_error(&error) => {}
                        Err(error) => return Err(error),
                    }
                }
                Err(error) if is_not_found_io_error(&error) => {}
                Err(_) => {
                    let Some(entry) = snapshot
                        .secondary_indexes
                        .iter()
                        .find(|entry| entry.index_id == index_id)
                    else {
                        continue;
                    };
                    let entries = build_edge_secondary_range_entries_for_segment(
                        &segment,
                        snapshot.target_label_id,
                        &snapshot.prop_key,
                    )?;
                    match install_edge_secondary_range_sidecar(&seg_path, entry, &entries) {
                        Ok(()) => {}
                        Err(error) if is_not_found_io_error(&error) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            continue;
        }

        match segment.validate_secondary_range_sidecar_for_target(
            index_id,
            planner_stats_target_from_discriminant(snapshot.target),
        ) {
            Ok(true) => continue,
            Ok(false) => {
                let Some(entry) = snapshot
                    .secondary_indexes
                    .iter()
                    .find(|entry| entry.index_id == index_id)
                else {
                    continue;
                };
                let entries = build_secondary_range_entries_for_segment(
                    &segment,
                    snapshot.target_label_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_range_sidecar(&seg_path, entry, &entries) {
                    Ok(()) => {}
                    Err(error) if is_not_found_io_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_not_found_io_error(&error) => {}
            Err(_) => {
                let Some(entry) = snapshot
                    .secondary_indexes
                    .iter()
                    .find(|entry| entry.index_id == index_id)
                else {
                    continue;
                };
                let entries = build_secondary_range_entries_for_segment(
                    &segment,
                    snapshot.target_label_id,
                    &snapshot.prop_key,
                )?;
                match install_secondary_range_sidecar(&seg_path, entry, &entries) {
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
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            all_present = false;
            continue;
        };

        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => {
                all_present = false;
                continue;
            }
            Err(error) => return Err(error),
        };

        match segment.validate_secondary_range_sidecar_for_target(
            index_id,
            planner_stats_target_from_discriminant(snapshot.target),
        ) {
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
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
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
        Some(label_catalog),
        None,
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
                || !matches!(entry.kind, SecondaryIndexKind::Range)
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

fn secondary_index_failure_message_for_entry(
    entry: &SecondaryIndexManifestEntry,
    message: String,
) -> String {
    if matches!(
        &entry.target,
        SecondaryIndexTarget::NodeFieldIndex { .. } | SecondaryIndexTarget::EdgeFieldIndex { .. }
    ) {
        compound_secondary_failure_message_from_str(&message)
    } else {
        message
    }
}

/// Materialize compact node metas for a compound sidecar build over a single
/// segment, scoped to the declaration's target label. Vector metadata is
/// never read on this path — compound tuples cannot reference vectors, so
/// those fields stay zeroed.
fn compound_node_metas_for_single_segment(
    segment: &SegmentReader,
    target_label_id: u32,
) -> Result<Vec<CompactNodeMeta>, EngineError> {
    let mut metas = Vec::new();
    for index in 0..segment.node_meta_count() as usize {
        let meta = segment.node_meta_at(index)?;
        if !meta.label_ids.contains(target_label_id) {
            continue;
        }
        metas.push(CompactNodeMeta {
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
        });
    }
    Ok(metas)
}

/// Edge counterpart of [`compound_node_metas_for_single_segment`].
fn compound_edge_metas_for_single_segment(
    segment: &SegmentReader,
    target_label_id: u32,
) -> Result<Vec<CompactEdgeMeta>, EngineError> {
    let mut metas = Vec::new();
    for index in 0..segment.edge_meta_count() as usize {
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
        ) = segment.edge_meta_at(index)?;
        if label_id != target_label_id {
            continue;
        }
        metas.push(CompactEdgeMeta {
            edge_id,
            new_data_offset: data_offset,
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
    Ok(metas)
}

fn load_compound_secondary_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    index_id: u64,
) -> Result<Option<CompoundSecondaryBuildSnapshot>, EngineError> {
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
        || !matches!(
            &entry.target,
            SecondaryIndexTarget::NodeFieldIndex { .. }
                | SecondaryIndexTarget::EdgeFieldIndex { .. }
        )
    {
        return Ok(None);
    }

    let target_label_id = secondary_index_target_label_id(&entry.target);
    let mut segment_ids: Vec<u64> = manifest.segments.iter().map(|segment| segment.id).collect();
    segment_ids.sort_unstable();
    let mut segment_infos = manifest.segments.clone();
    segment_infos.sort_by_key(|segment| segment.id);
    Ok(Some(CompoundSecondaryBuildSnapshot {
        dense_config: manifest.dense_vector.clone(),
        entry,
        target_label_id,
        segment_ids,
        segment_infos,
        secondary_indexes: manifest.secondary_indexes.clone(),
    }))
}

fn build_compound_sidecars_for_snapshot(
    db_dir: &Path,
    snapshot: &CompoundSecondaryBuildSnapshot,
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
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            continue;
        };
        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => Arc::new(segment),
            Err(error) if is_not_found_io_error(&error) => continue,
            Err(error) => return Err(error),
        };

        if let Ok(true) = segment.validate_compound_sidecar_for_entry(&snapshot.entry) {
            continue;
        }

        let source_segments = vec![Arc::clone(&segment)];
        let sidecar_entries = match &snapshot.entry.target {
            SecondaryIndexTarget::NodeFieldIndex { .. } => {
                let node_metas =
                    compound_node_metas_for_single_segment(&segment, snapshot.target_label_id)?;
                build_node_compound_entries_from_metadata(
                    &source_segments,
                    &node_metas,
                    &snapshot.entry,
                )?
            }
            SecondaryIndexTarget::EdgeFieldIndex { .. } => {
                let edge_metas =
                    compound_edge_metas_for_single_segment(&segment, snapshot.target_label_id)?;
                build_edge_compound_entries_from_metadata(
                    &source_segments,
                    &edge_metas,
                    &snapshot.entry,
                )?
            }
            SecondaryIndexTarget::NodeProperty { .. }
            | SecondaryIndexTarget::EdgeProperty { .. } => {
                continue;
            }
        };
        match publish_compound_sidecar_component(&seg_path, &snapshot.entry, &sidecar_entries) {
            Ok(()) => {}
            Err(error) if is_not_found_io_error(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn validate_compound_secondary_snapshot_coverage(
    db_dir: &Path,
    snapshot: &CompoundSecondaryBuildSnapshot,
    cancel: &AtomicBool,
) -> Result<CompoundSecondaryCoverageStatus, EngineError> {
    let mut all_present = true;
    for &segment_id in &snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            return Ok(CompoundSecondaryCoverageStatus::Cancelled);
        }
        let seg_path = segment_dir(db_dir, segment_id);
        if !seg_path.exists() {
            all_present = false;
            continue;
        }
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            all_present = false;
            continue;
        };
        let segment = match SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => {
                all_present = false;
                continue;
            }
            Err(error) => return Err(error),
        };
        match segment.validate_compound_sidecar_for_entry(&snapshot.entry) {
            Ok(true) => {}
            Ok(false) => all_present = false,
            Err(error) => {
                return Ok(CompoundSecondaryCoverageStatus::Failed(
                    compound_secondary_failure_message(&error),
                ));
            }
        }
    }
    Ok(if all_present {
        CompoundSecondaryCoverageStatus::Covered
    } else {
        CompoundSecondaryCoverageStatus::Incomplete
    })
}

#[allow(clippy::too_many_arguments)]
fn finalize_compound_secondary_build_snapshot(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    catalog_lock: &Arc<RwLock<SecondaryIndexCatalog>>,
    entries_lock: &Arc<RwLock<SecondaryIndexEntries>>,
    next_node_id_seen: &AtomicU64,
    next_edge_id_seen: &AtomicU64,
    engine_seq_seen: &AtomicU64,
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
    index_id: u64,
    snapshot: &CompoundSecondaryBuildSnapshot,
    coverage: &CompoundSecondaryCoverageStatus,
) -> Result<CompoundSecondaryFinalizeOutcome, EngineError> {
    let mut outcome = CompoundSecondaryFinalizeOutcome::Applied;
    update_secondary_index_manifest_runtime(
        db_dir,
        manifest_write_lock,
        catalog_lock,
        entries_lock,
        next_node_id_seen,
        next_edge_id_seen,
        engine_seq_seen,
        Some(label_catalog),
        None,
        |manifest| {
            let Some(entry_pos) = manifest
                .secondary_indexes
                .iter()
                .position(|entry| entry.index_id == index_id)
            else {
                outcome = CompoundSecondaryFinalizeOutcome::Inactive;
                return Ok(());
            };
            let mut current_segment_ids: Vec<u64> =
                manifest.segments.iter().map(|segment| segment.id).collect();
            current_segment_ids.sort_unstable();
            if current_segment_ids != snapshot.segment_ids {
                outcome = CompoundSecondaryFinalizeOutcome::Retry;
                return Ok(());
            }
            let entry = &mut manifest.secondary_indexes[entry_pos];
            if entry.state != SecondaryIndexState::Building
                || !matches!(
                    &entry.target,
                    SecondaryIndexTarget::NodeFieldIndex { .. }
                        | SecondaryIndexTarget::EdgeFieldIndex { .. }
                )
            {
                outcome = CompoundSecondaryFinalizeOutcome::Inactive;
                return Ok(());
            }
            match coverage {
                CompoundSecondaryCoverageStatus::Covered => {
                    entry.state = SecondaryIndexState::Ready;
                    entry.last_error = None;
                    let mut snapshot_segment_ids = snapshot.segment_ids.clone();
                    snapshot_segment_ids.sort_unstable();
                    if let Some(ready) =
                        SecondaryIndexReadyApplied::from_ready_entry(entry, snapshot_segment_ids)
                    {
                        outcome = CompoundSecondaryFinalizeOutcome::ReadyApplied(ready);
                    }
                }
                CompoundSecondaryCoverageStatus::Incomplete => {
                    entry.state = SecondaryIndexState::Building;
                    entry.last_error = None;
                }
                CompoundSecondaryCoverageStatus::Failed(message) => {
                    entry.state = SecondaryIndexState::Failed;
                    entry.last_error = Some(message.clone());
                }
                CompoundSecondaryCoverageStatus::Cancelled => {
                    outcome = CompoundSecondaryFinalizeOutcome::Inactive;
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
    label_catalog: &Arc<RwLock<RuntimeLabelCatalog>>,
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
                label_catalog,
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
                label_catalog,
                index_id,
                &snapshot,
                &coverage,
            )? {
                SecondaryRangeFinalizeOutcome::ReadyApplied(ready) => return Ok(Some(ready)),
                SecondaryRangeFinalizeOutcome::Applied
                | SecondaryRangeFinalizeOutcome::Inactive => return Ok(None),
                SecondaryRangeFinalizeOutcome::Retry => continue,
            }
        } else if let Some(snapshot) =
            load_compound_secondary_build_snapshot(db_dir, manifest_write_lock, index_id)?
        {
            build_compound_sidecars_for_snapshot(db_dir, &snapshot, cancel)?;
            let coverage =
                validate_compound_secondary_snapshot_coverage(db_dir, &snapshot, cancel)?;
            if matches!(coverage, CompoundSecondaryCoverageStatus::Cancelled) {
                return Ok(None);
            }
            match finalize_compound_secondary_build_snapshot(
                db_dir,
                manifest_write_lock,
                catalog_lock,
                entries_lock,
                next_node_id_seen,
                next_edge_id_seen,
                engine_seq_seen,
                label_catalog,
                index_id,
                &snapshot,
                &coverage,
            )? {
                CompoundSecondaryFinalizeOutcome::ReadyApplied(ready) => return Ok(Some(ready)),
                CompoundSecondaryFinalizeOutcome::Applied
                | CompoundSecondaryFinalizeOutcome::Inactive => return Ok(None),
                CompoundSecondaryFinalizeOutcome::Retry => continue,
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
    segment_infos: Vec<SegmentInfo>,
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
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
    let mut segment_infos: Vec<_> = manifest
        .segments
        .iter()
        .filter(|segment| snapshot_segment_ids.contains(&segment.id))
        .cloned()
        .collect();
    segment_infos.sort_by_key(|segment| segment.id);

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
        segment_infos,
        secondary_indexes: manifest.secondary_indexes.clone(),
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
    let target = match &ready.target {
        SecondaryIndexTarget::NodeProperty { .. } => PlannerStatsDeclaredIndexTarget::NodeProperty,
        SecondaryIndexTarget::EdgeProperty { .. } => PlannerStatsDeclaredIndexTarget::EdgeProperty,
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            let entry = SecondaryIndexManifestEntry {
                index_id: ready.index_id,
                target: ready.target.clone(),
                kind: ready.kind.clone(),
                state: SecondaryIndexState::Ready,
                last_error: None,
            };
            return segment.compound_sidecar_lightweight_available_for_entry(&entry);
        }
    };
    match ready.kind {
        SecondaryIndexKind::Equality => {
            segment.validate_secondary_eq_sidecar_for_target(ready.index_id, target)
        }
        SecondaryIndexKind::Range => {
            segment.validate_secondary_range_sidecar_for_target(ready.index_id, target)
        }
    }
}

fn refresh_ready_secondary_index_planner_stats(
    db_dir: &Path,
    manifest_write_lock: &Arc<Mutex<()>>,
    ready: &SecondaryIndexReadyApplied,
    cancel: &AtomicBool,
) -> Vec<(u64, Arc<SegmentReader>)> {
    const TARGETED_STATS_REFRESH_MAX_ATTEMPTS: usize = 4;

    let Some(initial_snapshot) =
        load_targeted_stats_refresh_snapshot(db_dir, manifest_write_lock, ready)
            .ok()
            .flatten()
    else {
        return Vec::new();
    };
    let mut refreshed = Vec::new();

    if matches!(
        ready.target,
        SecondaryIndexTarget::NodeFieldIndex { .. } | SecondaryIndexTarget::EdgeFieldIndex { .. }
    ) {
        for segment_id in initial_snapshot.segment_ids {
            if cancel.load(Ordering::Relaxed) {
                break;
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
            let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
                continue;
            };
            let seg_dir = segment_dir(db_dir, segment_id);
            let latest_segment = match SegmentReader::open_with_info(
                &seg_dir,
                seg_info,
                snapshot.dense_config.as_ref(),
                &snapshot.secondary_indexes,
            ) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => continue,
                Err(_) => continue,
            };
            if target_secondary_sidecar_is_valid(&latest_segment, ready).unwrap_or(false) {
                refreshed.push((segment_id, Arc::new(latest_segment)));
            }
        }
        return refreshed;
    }

    for segment_id in initial_snapshot.segment_ids {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        let seg_dir = segment_dir(db_dir, segment_id);
        let Some(initial_seg_info) =
            segment_info_for_id(&initial_snapshot.segment_infos, segment_id)
        else {
            continue;
        };
        let segment = match SegmentReader::open_with_info(
            &seg_dir,
            initial_seg_info,
            initial_snapshot.dense_config.as_ref(),
            &initial_snapshot.secondary_indexes,
        ) {
            Ok(segment) => segment,
            Err(error) if is_not_found_io_error(&error) => continue,
            Err(_) => continue,
        };
        match target_secondary_sidecar_is_valid(&segment, ready) {
            Ok(true) => {}
            Ok(false) | Err(_) => continue,
        }

        let mut latest_snapshot = None;
        for _ in 0..TARGETED_STATS_REFRESH_MAX_ATTEMPTS {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            let Some(snapshot) = targeted_refresh_snapshot_contains_segment(
                db_dir,
                manifest_write_lock,
                ready,
                segment_id,
            )
            .ok()
            .flatten() else {
                latest_snapshot = None;
                break;
            };
            let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
                latest_snapshot = None;
                break;
            };
            let latest_segment = match SegmentReader::open_with_info(
                &seg_dir,
                seg_info,
                snapshot.dense_config.as_ref(),
                &snapshot.secondary_indexes,
            ) {
                Ok(segment) => segment,
                Err(error) if is_not_found_io_error(&error) => {
                    latest_snapshot = None;
                    break;
                }
                Err(_) => {
                    latest_snapshot = None;
                    break;
                }
            };
            match target_secondary_sidecar_is_valid(&latest_segment, ready) {
                Ok(true) => {}
                Ok(false) | Err(_) => {
                    latest_snapshot = None;
                    break;
                }
            }
            match write_targeted_secondary_index_planner_stats_sidecar(
                &seg_dir,
                &latest_segment,
                &snapshot.target_entry,
                &snapshot.ready_indexes,
            ) {
                Ok(_) => {
                    latest_snapshot = Some(snapshot);
                    break;
                }
                Err(error) if is_optional_component_publication_conflict(&error) => {
                    latest_snapshot = Some(snapshot);
                    continue;
                }
                Err(_) => {
                    latest_snapshot = Some(snapshot);
                    break;
                }
            }
        }
        let Some(snapshot) = latest_snapshot else {
            continue;
        };
        let Some(seg_info) = segment_info_for_id(&snapshot.segment_infos, segment_id) else {
            continue;
        };

        let refreshed_reader = match SegmentReader::open_with_info(
            &seg_dir,
            seg_info,
            snapshot.dense_config.as_ref(),
            &snapshot.secondary_indexes,
        ) {
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
    entry: &SecondaryIndexManifestEntry,
    cancel: &AtomicBool,
) -> Result<(), EngineError> {
    let manifest = load_manifest_readonly(db_dir)?
        .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
    for segment_info in &manifest.segments {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        let seg_dir = segment_dir(db_dir, segment_info.id);
        let mut sidecar_paths = match remove_secondary_index_component_records(&seg_dir, entry) {
            Ok(paths) => paths,
            Err(error) if is_not_found_io_error(&error) => Vec::new(),
            Err(error) => return Err(error),
        };
        sidecar_paths.extend(secondary_index_sidecar_paths_for_entry(&seg_dir, entry));

        let mut seen_paths = HashSet::new();
        for sidecar_path in sidecar_paths {
            if !seen_paths.insert(sidecar_path.clone()) {
                continue;
            }
            if sidecar_path.exists() {
                let _ = std::fs::remove_file(&sidecar_path);
                if let Some(parent) = sidecar_path.parent() {
                    let _ = fsync_dir(parent);
                }
            }
        }
        cleanup_orphan_optional_component_files(&seg_dir);
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
    label_catalog: Arc<RwLock<RuntimeLabelCatalog>>,
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
                    &label_catalog,
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
                            &label_catalog,
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
            SecondaryIndexJob::DropCleanup { entry } => {
                let _ = process_secondary_index_drop_cleanup(&db_dir, &entry, &cancel);
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
        WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => {
            Err(EngineError::InvalidOperation(
                "WAL atomic batch markers are not write operations".into(),
            ))
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

fn validate_or_apply_replayed_label_token_op(
    catalog: &mut RuntimeLabelCatalog,
    op: &WalOp,
    wal_generation_id: u64,
) -> Result<(), EngineError> {
    match op {
        WalOp::EnsureNodeLabel { label, label_id } => {
            catalog.apply_node_label(label.clone(), *label_id, Some(wal_generation_id))
        }
        WalOp::EnsureEdgeLabel { label, label_id } => {
            catalog.apply_edge_label(label.clone(), *label_id, Some(wal_generation_id))
        }
        WalOp::UpsertNode(node) => {
            for &label_id in node.label_ids.as_slice() {
                if !catalog.node_id_to_label.contains_key(&label_id) {
                    return Err(EngineError::CorruptWal(format!(
                        "node record {} references missing node label label_id {}",
                        node.id, label_id
                    )));
                }
            }
            Ok(())
        }
        WalOp::UpsertEdge(edge) => {
            if catalog.edge_id_to_label.contains_key(&edge.label_id) {
                Ok(())
            } else {
                Err(EngineError::CorruptWal(format!(
                    "edge record {} references missing edge-label label_id {}",
                    edge.id, edge.label_id
                )))
            }
        }
        WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => Ok(()),
        WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => Err(
            EngineError::CorruptWal("WAL atomic batch marker reached normal replay apply".into()),
        ),
    }
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
    label_ids: &NodeLabelSet,
    updated_at: i64,
    weight: f32,
    policy_age_cutoff: Option<i64>,
    policy_max_weight: Option<f32>,
    policy_label_id: Option<u32>,
) -> bool {
    if let Some(label_id) = policy_label_id {
        if !label_ids.contains(label_id) {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedPrunePolicy {
    max_age_ms: Option<i64>,
    max_weight: Option<f32>,
    label_id: Option<u32>,
}

fn public_prune_policy_from_resolved(
    policy: &ResolvedPrunePolicy,
    catalog: &RuntimeLabelCatalog,
) -> Result<PrunePolicy, EngineError> {
    let label = policy
        .label_id
        .map(|label_id| {
            catalog
                .node_id_to_label
                .get(&label_id)
                .cloned()
                .ok_or_else(|| {
                    EngineError::ManifestError(format!(
                        "prune policy references missing node label_id {label_id}"
                    ))
                })
        })
        .transpose()?;
    Ok(PrunePolicy {
        max_age_ms: policy.max_age_ms,
        max_weight: policy.max_weight,
        label,
    })
}

fn resolve_manifest_prune_policy(
    policy: &PrunePolicy,
    catalog: &RuntimeLabelCatalog,
) -> Result<ResolvedPrunePolicy, EngineError> {
    let label_id = policy
        .label
        .as_deref()
        .map(|label| {
            validate_label_token_name(label)?;
            catalog.node_label_to_id.get(label).copied().ok_or_else(|| {
                EngineError::ManifestError(format!(
                    "prune policy references missing node label '{label}'"
                ))
            })
        })
        .transpose()?;
    Ok(ResolvedPrunePolicy {
        max_age_ms: policy.max_age_ms,
        max_weight: policy.max_weight,
        label_id,
    })
}

struct PrecomputedPruneCutoffs {
    /// (age_cutoff, max_weight, label_id) per policy.
    policies: Vec<(Option<i64>, Option<f32>, Option<u32>)>,
}

impl PrecomputedPruneCutoffs {
    fn from_policies(policies: &BTreeMap<String, ResolvedPrunePolicy>, now: i64) -> Self {
        let policies = policies
            .values()
            .map(|p| {
                let age_cutoff = p.max_age_ms.map(|age| now - age);
                (age_cutoff, p.max_weight, p.label_id)
            })
            .collect();
        Self { policies }
    }

    /// Returns true if the node matches ANY registered policy (should be excluded).
    fn excludes(&self, node: &NodeRecord) -> bool {
        self.excludes_fields(&node.label_ids, node.updated_at, node.weight)
    }

    fn excludes_fields(&self, label_ids: &NodeLabelSet, updated_at: i64, weight: f32) -> bool {
        for &(age_cutoff, max_weight, policy_label_id) in &self.policies {
            if matches_prune_cutoff(
                label_ids,
                updated_at,
                weight,
                age_cutoff,
                max_weight,
                policy_label_id,
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
    prune_policies: BTreeMap<String, ResolvedPrunePolicy>,
    dense_vector: Option<DenseVectorConfig>,
}

#[derive(Clone)]
struct PublishedSchemaCatalogSnapshot {
    next_schema_id: u64,
    node_schemas: Vec<NodeSchemaManifestEntry>,
    edge_schemas: Vec<EdgeSchemaManifestEntry>,
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
    node_record_hydration_reads: AtomicUsize,
    node_visibility_meta_reads: AtomicUsize,
    edge_record_hydration_reads: AtomicUsize,
    edge_record_hydration_calls: AtomicUsize,
    equality_materialization_record_reads: AtomicUsize,
    final_verifier_record_reads: AtomicUsize,
    edge_full_scan_pages: AtomicUsize,
    endpoint_adjacency_candidates: AtomicUsize,
    graph_row_query_calls: AtomicUsize,
    selected_field_reads: SelectedFieldReadCounters,
    public_node_query_calls: AtomicUsize,
    public_edge_query_calls: AtomicUsize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryExecutionCounterSnapshot {
    pub node_record_hydration_reads: usize,
    pub node_visibility_meta_reads: usize,
    pub edge_record_hydration_reads: usize,
    pub edge_record_hydration_calls: usize,
    pub equality_materialization_record_reads: usize,
    pub final_verifier_record_reads: usize,
    pub edge_full_scan_pages: usize,
    pub endpoint_adjacency_candidates: usize,
    pub graph_row_query_calls: usize,
    pub node_selected_field_batches: usize,
    pub node_selected_field_ids: usize,
    pub edge_selected_field_batches: usize,
    pub edge_selected_field_ids: usize,
    pub node_dense_vector_projection_reads: usize,
    pub node_sparse_vector_projection_reads: usize,
    pub public_node_query_calls: usize,
    pub public_edge_query_calls: usize,
}

/// Published read-visible snapshot for CP1 point/dedup reads.
#[derive(Clone)]
pub(crate) struct ReadView {
    sources: Arc<PublishedReadSources>,
    snapshot_seq: u64,
    active_degree_overlay: Arc<DegreeOverlaySnapshot>,
    label_catalog: Arc<ReadLabelCatalogSnapshot>,
}

pub(crate) type ReadViewImmutableEpoch = ImmutableEpoch;

struct PublishedReadState {
    view: Arc<ReadView>,
    label_catalog: Arc<ReadLabelCatalogSnapshot>,
    schema_catalog: Arc<PublishedSchemaCatalogSnapshot>,
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
    SnapshotWithLabelCatalog,
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
    #[cfg(test)]
    gql_mutation_before_commit_pause: Mutex<Option<RuntimeReadPauseHook>>,
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

#[allow(clippy::enum_variant_names)] // The suffix documents the queued repair trigger explicitly.
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
    CompoundEqualitySidecarFailure {
        index_id: u64,
        error: Option<EngineError>,
    },
    CompoundRangeSidecarFailure {
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
    CompoundEqualityBuilding { index_id: u64 },
    CompoundEqualityFailed { index_id: u64, message: String },
    CompoundRangeBuilding { index_id: u64 },
    CompoundRangeFailed { index_id: u64, message: String },
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
            SecondaryIndexReadFollowup::CompoundEqualitySidecarFailure { index_id, error } => {
                match error {
                    Some(error) if !is_not_found_io_error(error) => {
                        SecondaryIndexReadFollowupKey::CompoundEqualityFailed {
                            index_id: *index_id,
                            message: compound_secondary_failure_message(error),
                        }
                    }
                    _ => SecondaryIndexReadFollowupKey::CompoundEqualityBuilding {
                        index_id: *index_id,
                    },
                }
            }
            SecondaryIndexReadFollowup::CompoundRangeSidecarFailure { index_id, error } => {
                match error {
                    Some(error) if !is_not_found_io_error(error) => {
                        SecondaryIndexReadFollowupKey::CompoundRangeFailed {
                            index_id: *index_id,
                            message: compound_secondary_failure_message(error),
                        }
                    }
                    _ => SecondaryIndexReadFollowupKey::CompoundRangeBuilding {
                        index_id: *index_id,
                    },
                }
            }
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
    EnsureNodeLabel {
        label: String,
    },
    EnsureEdgeLabel {
        label: String,
    },
    UpsertNode {
        labels: Vec<String>,
        key: String,
        options: UpsertNodeOptions,
    },
    AddNodeLabel {
        id: u64,
        label: String,
    },
    RemoveNodeLabel {
        id: u64,
        label: String,
    },
    UpsertEdge {
        from: u64,
        to: u64,
        label: String,
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
        return_read_view: bool,
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
    SetNodeSchema {
        label: String,
        schema: NodeSchema,
        options: SchemaSetOptions,
    },
    DropNodeSchema {
        label: String,
    },
    SetEdgeSchema {
        label: String,
        schema: EdgeSchema,
        options: SchemaSetOptions,
    },
    DropEdgeSchema {
        label: String,
    },
    SetGraphSchema {
        schema: GraphSchema,
        options: GraphSchemaSetOptions,
    },
    AlterGraphSchema {
        operations: Vec<GraphSchemaOperation>,
        options: GraphSchemaSetOptions,
    },
    DropGraphSchema,
    EnsureNodePropertyIndex {
        label: String,
        spec: SecondaryIndexSpec,
    },
    DropNodePropertyIndex {
        label: String,
        spec: SecondaryIndexSpec,
    },
    EnsureEdgePropertyIndex {
        label: String,
        spec: SecondaryIndexSpec,
    },
    DropEdgePropertyIndex {
        label: String,
        spec: SecondaryIndexSpec,
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
    U32(u32),
    U64(u64),
    VecU64(Vec<u64>),
    Unit,
    OptionEdge(Option<EdgeRecord>),
    PatchResult(PatchResult),
    TxnCommitResult(TxnCommitResult),
    TxnCommitResultWithReadView(TxnCommitResult, Arc<ReadView>),
    PruneResult(PruneResult),
    Bool(bool),
    NodeSchemaInfo(NodeSchemaInfo),
    EdgeSchemaInfo(EdgeSchemaInfo),
    GraphSchemaPublishResult(GraphSchemaPublishResult),
    NodePropertyIndexInfo(NodePropertyIndexInfo),
    EdgePropertyIndexInfo(EdgePropertyIndexInfo),
    OptionSegmentInfo(Option<SegmentInfo>),
    OptionCompactionStats(Option<CompactionStats>),
}

struct CoreWritePlan {
    ops: Vec<WalOp>,
    reply: CoreWriteReply,
    auto_flush: bool,
    track_ids: bool,
    label_catalog_changed: bool,
}

#[derive(Clone, Copy)]
struct IdCounterSnapshot {
    next_node_id: u64,
    next_edge_id: u64,
    next_node_id_seen: u64,
    next_edge_id_seen: u64,
}

struct PlannedCoreWrite {
    plan: CoreWritePlan,
    id_counter_snapshot: IdCounterSnapshot,
}

struct QueuedCoreWrite {
    request: CoreWriteRequest,
    reply_tx: Option<std::sync::mpsc::SyncSender<Result<CoreWriteReply, EngineError>>>,
    followup_key: Option<SecondaryIndexReadFollowupKey>,
}

#[allow(clippy::large_enum_variant)]
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
            #[cfg(test)]
            gql_mutation_before_commit_pause: Mutex::new(None),
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
    fn pause_gql_mutation_before_commit_for_test(&self) {
        if let Some(hook) = self.gql_mutation_before_commit_pause.lock().unwrap().take() {
            let _ = hook.ready_tx.send(());
            let _ = hook.release_rx.recv();
        }
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
    ) -> Result<(), EngineError> {
        #[cfg(test)]
        match impact {
            PublishImpact::NoPublish => {
                self.publish_counters
                    .skipped
                    .fetch_add(1, Ordering::Relaxed);
            }
            PublishImpact::SnapshotOnly | PublishImpact::SnapshotWithLabelCatalog => {
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
            return Ok(());
        }

        #[cfg(test)]
        if _apply_test_pause {
            if let Some(hook) = self.write_publish_pause.lock().unwrap().take() {
                let _ = hook.ready_tx.send(());
                let _ = hook.release_rx.recv();
            }
        }

        if impact == PublishImpact::RebuildSources {
            core.rebuild_published_read_sources()?;
        }

        let label_catalog = match impact {
            PublishImpact::SnapshotOnly => Arc::clone(&self.published.load_full().label_catalog),
            PublishImpact::SnapshotWithLabelCatalog | PublishImpact::RebuildSources => {
                core.read_label_catalog_snapshot()
            }
            PublishImpact::NoPublish => unreachable!("NoPublish returned before publish rebuild"),
        };
        let published = Arc::new(core.published_read_state_with_catalog(label_catalog));
        // Publish before releasing the core mutex so later writers cannot overtake
        // this committed snapshot and install an older view afterward.
        self.published.store(published);
        core.retry_deferred_segment_cleanup();
        Ok(())
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

        let publish_result = self.publish_locked(core, PublishImpact::RebuildSources, true);
        drop(core_guard);
        publish_result?;
        result
    }

    fn republish_secondary_index_state_if_open(&self) {
        let mut core_guard = self.core.lock().unwrap();
        let Some(core) = core_guard.as_mut() else {
            return;
        };
        core.manifest.secondary_indexes = core.secondary_index_entries_snapshot();
        let _ = self.publish_locked(core, PublishImpact::RebuildSources, true);
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
                let Some(root_segment) = core
                    .manifest
                    .segments
                    .iter()
                    .find(|segment| segment.id == segment_id)
                else {
                    continue;
                };
                if root_segment.segment_data_id != reader.segment_data_id() {
                    continue;
                }
                if root_segment.node_count != reader.node_count()
                    || root_segment.edge_count != reader.edge_count()
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
                    && core.segments[position].segment_data_id() == reader.segment_data_id()
                    && core.segments[position].component_manifest_generation()
                        <= reader.component_manifest_generation()
                {
                    core.segments[position] = reader;
                }
            }
        }

        let _ = self.publish_locked(core, PublishImpact::RebuildSources, true);
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
                | CoreWriteRequest::AddNodeLabel { .. }
                | CoreWriteRequest::RemoveNodeLabel { .. }
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
                    if let Err(err) = self.publish_locked(core, publish_impact, true) {
                        drop(core_guard);
                        return QueuedWriteProgress::Complete {
                            command,
                            result: Err(err),
                        };
                    }
                    drop(core_guard);
                    return QueuedWriteProgress::WaitForLifecycle { command };
                }
                Err(err) => {
                    let _ = self.publish_locked(core, publish_impact, true);
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
            CoreWriteRequest::SetNodeSchema {
                label,
                schema,
                options,
            } => match core.set_node_schema(label, schema.clone(), options.clone()) {
                Ok((info, impact)) => (Ok(CoreWriteReply::NodeSchemaInfo(info)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::DropNodeSchema { label } => match core.drop_node_schema(label) {
                Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::SetEdgeSchema {
                label,
                schema,
                options,
            } => match core.set_edge_schema(label, schema.clone(), options.clone()) {
                Ok((info, impact)) => (Ok(CoreWriteReply::EdgeSchemaInfo(info)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::DropEdgeSchema { label } => match core.drop_edge_schema(label) {
                Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::SetGraphSchema { schema, options } => {
                match core.set_graph_schema(schema.clone(), options.clone()) {
                    Ok((result, impact)) => {
                        (Ok(CoreWriteReply::GraphSchemaPublishResult(result)), impact)
                    }
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::AlterGraphSchema {
                operations,
                options,
            } => match core.alter_graph_schema(operations.clone(), options.clone()) {
                Ok((result, impact)) => {
                    (Ok(CoreWriteReply::GraphSchemaPublishResult(result)), impact)
                }
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::DropGraphSchema => match core.drop_graph_schema() {
                Ok((result, impact)) => {
                    (Ok(CoreWriteReply::GraphSchemaPublishResult(result)), impact)
                }
                Err(err) => (Err(err), PublishImpact::NoPublish),
            },
            CoreWriteRequest::EnsureNodePropertyIndex { label, spec } => {
                match core.ensure_node_property_index(label, spec.clone()) {
                    Ok((info, impact)) => (Ok(CoreWriteReply::NodePropertyIndexInfo(info)), impact),
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::DropNodePropertyIndex { label, spec } => {
                match core.drop_node_property_index(label, spec.clone()) {
                    Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::EnsureEdgePropertyIndex { label, spec } => {
                match core.ensure_edge_property_index(label, spec.clone()) {
                    Ok((info, impact)) => (Ok(CoreWriteReply::EdgePropertyIndexInfo(info)), impact),
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::DropEdgePropertyIndex { label, spec } => {
                match core.drop_edge_property_index(label, spec.clone()) {
                    Ok((removed, impact)) => (Ok(CoreWriteReply::Bool(removed)), impact),
                    Err(err) => (Err(err), PublishImpact::NoPublish),
                }
            }
            CoreWriteRequest::ApplySecondaryIndexReadFollowup { followup } => {
                let impact = match followup {
                    SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id, error } => core
                        .degrade_ready_equality_index_after_sidecar_failure(
                            *index_id,
                            error.as_ref(),
                        ),
                    SecondaryIndexReadFollowup::RangeSidecarFailure { index_id, error } => core
                        .degrade_ready_range_index_after_sidecar_failure(*index_id, error.as_ref()),
                    SecondaryIndexReadFollowup::CompoundEqualitySidecarFailure {
                        index_id,
                        error,
                    } => core.degrade_ready_equality_index_after_sidecar_failure(
                        *index_id,
                        error.as_ref(),
                    ),
                    SecondaryIndexReadFollowup::CompoundRangeSidecarFailure { index_id, error } => {
                        core.degrade_ready_range_index_after_sidecar_failure(
                            *index_id,
                            error.as_ref(),
                        )
                    }
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
        let publish_result = self.publish_locked(core, publish_impact, true);
        let return_read_view = matches!(
            &command.request,
            CoreWriteRequest::TxnCommit {
                return_read_view: true,
                ..
            }
        );
        let read_view = if return_read_view && result.is_ok() && publish_result.is_ok() {
            Some(Arc::clone(&self.published.load_full().view))
        } else {
            None
        };
        drop(core_guard);
        let result = match (result, publish_result) {
            (Err(err), _) => Err(err),
            (Ok(CoreWriteReply::TxnCommitResult(result)), Ok(())) if return_read_view => {
                let view = read_view.expect("published read view captured for txn commit");
                Ok(CoreWriteReply::TxnCommitResultWithReadView(result, view))
            }
            (Ok(reply), Ok(())) => Ok(reply),
            (Ok(_), Err(err)) => Err(err),
        };
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
                        self.publish_locked(core, PublishImpact::RebuildSources, true)?;
                    }
                    let result = core.current_flush_pipeline_error().map_or(Ok(None), Err);
                    drop(core_guard);
                    return result;
                }

                core.ensure_bg_flush_worker();
                if let Err(error) = core.enqueue_all_non_in_flight() {
                    if mutated {
                        self.publish_locked(core, PublishImpact::RebuildSources, true)?;
                    }
                    drop(core_guard);
                    return Err(error);
                }
                target_epoch = core.immutable_epochs.first().map(|epoch| epoch.epoch_id);
                if mutated {
                    self.publish_locked(core, PublishImpact::RebuildSources, true)?;
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
        self.publish_locked(core, PublishImpact::NoPublish, true)?;
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
            self.publish_locked(core, publish_impact, true)?;
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
        let _ = self.publish_locked(core, publish_impact, false);
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
                self.publish_locked(core, PublishImpact::NoPublish, true)?;
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
        label_catalog: Arc<ReadLabelCatalogSnapshot>,
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
            label_catalog,
        }
    }

    fn sources(&self) -> SourceList<'_> {
        SourceList {
            active: self.memtable.as_ref(),
            immutable: &self.immutable_epochs,
            segments: &self.segments,
            snapshot_seq: self.snapshot_seq,
            #[cfg(test)]
            selected_field_read_counters: Some(&self.query_execution_counters.selected_field_reads),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
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
    fn note_node_record_hydration_reads(&self, count: usize) {
        self.query_execution_counters
            .node_record_hydration_reads
            .fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_node_visibility_meta_reads(&self, count: usize) {
        self.query_execution_counters
            .node_visibility_meta_reads
            .fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_edge_record_hydration_reads(&self, count: usize) {
        self.query_execution_counters
            .edge_record_hydration_reads
            .fetch_add(count, Ordering::Relaxed);
        self.query_execution_counters
            .edge_record_hydration_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_equality_materialization_record_reads(&self, count: usize) {
        self.query_execution_counters
            .equality_materialization_record_reads
            .fetch_add(count, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_edge_full_scan_page(&self) {
        self.query_execution_counters
            .edge_full_scan_pages
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn note_endpoint_adjacency_candidates(&self, count: usize) {
        self.query_execution_counters
            .endpoint_adjacency_candidates
            .fetch_add(count, Ordering::Relaxed);
    }

    fn node_property_index_entry(
        &self,
        label_id: u32,
        prop_key: &str,
        kind: &SecondaryIndexKind,
    ) -> Option<SecondaryIndexManifestEntry> {
        self.secondary_index_catalog
            .node_property
            .get(label_id, prop_key, kind)
    }

    fn edge_property_index_entry(
        &self,
        label_id: u32,
        prop_key: &str,
        kind: &SecondaryIndexKind,
    ) -> Option<SecondaryIndexManifestEntry> {
        self.secondary_index_catalog
            .edge_property
            .get(label_id, prop_key, kind)
    }

    fn node_field_index_entries(
        &self,
        label_id: u32,
        kind: &SecondaryIndexKind,
    ) -> &[SecondaryIndexManifestEntry] {
        self.secondary_index_catalog.node_field.get(label_id, kind)
    }

    fn edge_field_index_entries(
        &self,
        label_id: u32,
        kind: &SecondaryIndexKind,
    ) -> &[SecondaryIndexManifestEntry] {
        self.secondary_index_catalog.edge_field.get(label_id, kind)
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

    fn get_node_by_label_key_raw(
        &self,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node_by_label_key(label_id, key)
    }

    fn get_node_by_label_key(
        &self,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_by_label_key_raw(label_id, key)? {
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
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge_by_triple(from, to, label_id)
    }

    fn get_edges_by_triples_raw(
        &self,
        triples: &[(u64, u64, u32)],
    ) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        self.sources().find_edges_by_triples(triples)
    }

    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        #[cfg(test)]
        self.note_node_record_hydration_reads(ids.len());
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

    fn get_nodes_by_label_keys_raw(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        #[cfg(test)]
        self.note_node_record_hydration_reads(keys.len());
        self.sources().find_nodes_by_label_keys(keys)
    }

    fn get_nodes_by_label_keys(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_by_label_keys_raw(keys)?;
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
        #[cfg(test)]
        self.note_edge_record_hydration_reads(ids.len());
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
            #[cfg(test)]
            selected_field_read_counters: None,
        }
    }

    fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge(id)
    }

    fn get_node_by_label_key_raw(
        &self,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node_by_label_key(label_id, key)
    }

    fn get_nodes_by_label_keys_raw(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes_by_label_keys(keys)
    }

    fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge_by_triple(from, to, label_id)
    }

    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        self.sources().find_nodes(ids)
    }

    fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        self.read_view().get_edges(ids)
    }

    fn nodes_by_label_id_raw(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        self.read_view().nodes_by_label_id_raw(label_id)
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
        label_filter_ids: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
        tombstones: Option<(
            &HashSet<u64, NodeIdBuildHasher>,
            &HashSet<u64, NodeIdBuildHasher>,
        )>,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        self.read_view().neighbors_raw(
            node_id,
            direction,
            label_filter_ids,
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
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.map(|error| {
                secondary_index_failure_message_for_entry(&current_entry, error.to_string())
            })
        } else {
            None
        };
        let should_queue_build = next_state == SecondaryIndexState::Building
            && current_entry.state != SecondaryIndexState::Building;
        let should_queue_build = should_queue_build
            && secondary_index_target_requires_sidecar_build(&current_entry.target);
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
            Some(&self.label_catalog),
            self.checkpointable_wal_generation(),
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

        let Some(current_entry) = self
            .secondary_index_entries_snapshot()
            .into_iter()
            .find(|entry| entry.index_id == index_id)
        else {
            return PublishImpact::NoPublish;
        };
        if !matches!(current_entry.kind, SecondaryIndexKind::Range) {
            return PublishImpact::NoPublish;
        }
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.map(|error| {
                secondary_index_failure_message_for_entry(&current_entry, error.to_string())
            })
        } else {
            None
        };
        let should_queue_build = next_state == SecondaryIndexState::Building
            && current_entry.state != SecondaryIndexState::Building;
        let should_queue_build = should_queue_build
            && secondary_index_target_requires_sidecar_build(&current_entry.target);
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
            Some(&self.label_catalog),
            self.checkpointable_wal_generation(),
            |manifest| {
                if let Some(entry) = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == index_id)
                {
                    if matches!(entry.kind, SecondaryIndexKind::Range) {
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

    pub fn ensure_node_label(&self, label: &str) -> Result<u32, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EnsureNodeLabel {
                label: label.to_string(),
            })? {
            CoreWriteReply::U32(label_id) => Ok(label_id),
            _ => unreachable!("ensure_node_label must return a label id"),
        }
    }

    pub fn ensure_edge_label(&self, label: &str) -> Result<u32, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EnsureEdgeLabel {
                label: label.to_string(),
            })? {
            CoreWriteReply::U32(label_id) => Ok(label_id),
            _ => unreachable!("ensure_edge_label must return a label id"),
        }
    }

    pub fn get_node_label_id(&self, label: &str) -> Result<Option<u32>, EngineError> {
        validate_label_token_name(label)?;
        self.with_core_ref(|core| {
            Ok(core
                .label_catalog
                .read()
                .unwrap()
                .node_label_to_id
                .get(label)
                .copied())
        })
    }

    pub fn get_edge_label_id(&self, label: &str) -> Result<Option<u32>, EngineError> {
        validate_label_token_name(label)?;
        self.with_core_ref(|core| {
            Ok(core
                .label_catalog
                .read()
                .unwrap()
                .edge_label_to_id
                .get(label)
                .copied())
        })
    }

    pub fn get_node_label(&self, label_id: u32) -> Result<Option<String>, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .label_catalog
                .read()
                .unwrap()
                .node_id_to_label
                .get(&label_id)
                .cloned())
        })
    }

    pub fn get_edge_label(&self, label_id: u32) -> Result<Option<String>, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .label_catalog
                .read()
                .unwrap()
                .edge_id_to_label
                .get(&label_id)
                .cloned())
        })
    }

    pub fn list_node_labels(&self) -> Result<Vec<NodeLabelInfo>, EngineError> {
        self.with_core_ref(|core| {
            let mut labels: Vec<NodeLabelInfo> = core
                .label_catalog
                .read()
                .unwrap()
                .node_label_to_id
                .iter()
                .map(|(label, &label_id)| NodeLabelInfo {
                    label: label.clone(),
                    label_id,
                })
                .collect();
            labels.sort_by(|a, b| {
                a.label_id
                    .cmp(&b.label_id)
                    .then_with(|| a.label.cmp(&b.label))
            });
            Ok(labels)
        })
    }

    pub fn list_edge_labels(&self) -> Result<Vec<EdgeLabelInfo>, EngineError> {
        self.with_core_ref(|core| {
            let mut edge_labels: Vec<EdgeLabelInfo> = core
                .label_catalog
                .read()
                .unwrap()
                .edge_label_to_id
                .iter()
                .map(|(label, &label_id)| EdgeLabelInfo {
                    label: label.clone(),
                    label_id,
                })
                .collect();
            edge_labels.sort_by(|a, b| {
                a.label_id
                    .cmp(&b.label_id)
                    .then_with(|| a.label.cmp(&b.label))
            });
            Ok(edge_labels)
        })
    }

    pub fn upsert_node<L>(
        &self,
        labels: L,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<u64, EngineError>
    where
        L: IntoNodeLabels,
    {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::UpsertNode {
                labels: labels.into_node_labels(),
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
        label: &str,
        options: UpsertEdgeOptions,
    ) -> Result<u64, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::UpsertEdge {
                from,
                to,
                label: label.to_string(),
                options,
            })? {
            CoreWriteReply::U64(id) => Ok(id),
            _ => unreachable!("upsert_edge must return an edge id"),
        }
    }

    pub fn add_node_label(&self, id: u64, label: &str) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::AddNodeLabel {
                id,
                label: label.to_string(),
            })? {
            CoreWriteReply::Bool(changed) => Ok(changed),
            _ => unreachable!("add_node_label must return changed bool"),
        }
    }

    pub fn remove_node_label(&self, id: u64, label: &str) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::RemoveNodeLabel {
                id,
                label: label.to_string(),
            })? {
            CoreWriteReply::Bool(changed) => Ok(changed),
            _ => unreachable!("remove_node_label must return changed bool"),
        }
    }

    pub fn batch_upsert_nodes(&self, inputs: Vec<NodeInput>) -> Result<Vec<u64>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::BatchUpsertNodes { inputs })?
        {
            CoreWriteReply::VecU64(ids) => Ok(ids),
            _ => unreachable!("batch_upsert_nodes must return node ids"),
        }
    }

    pub fn batch_upsert_edges(&self, inputs: Vec<EdgeInput>) -> Result<Vec<u64>, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::BatchUpsertEdges { inputs })?
        {
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

    pub fn invalidate_edge(&self, id: u64, valid_to: i64) -> Result<Option<EdgeView>, EngineError> {
        let edge = match self
            .runtime
            .submit_core_write(CoreWriteRequest::InvalidateEdge { id, valid_to })?
        {
            CoreWriteReply::OptionEdge(edge) => edge,
            _ => unreachable!("invalidate_edge must return an optional edge"),
        };
        let (_guard, published) = self.runtime.published_snapshot()?;
        edge.map(|edge| edge_view_from_record(edge, published.label_catalog.as_ref()))
            .transpose()
    }

    pub fn graph_patch(&self, patch: GraphPatch) -> Result<PatchResult, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::GraphPatch { patch })?
        {
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

    pub fn list_prune_policies(&self) -> Result<Vec<PrunePolicyInfo>, EngineError> {
        self.with_core_ref(|core| core.list_prune_policies())
    }

    pub fn ensure_node_property_index(
        &self,
        label: &str,
        spec: SecondaryIndexSpec,
    ) -> Result<NodePropertyIndexInfo, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EnsureNodePropertyIndex {
                label: label.to_string(),
                spec,
            })? {
            CoreWriteReply::NodePropertyIndexInfo(info) => Ok(info),
            _ => unreachable!("ensure_node_property_index must return index info"),
        }
    }

    pub fn drop_node_property_index(
        &self,
        label: &str,
        spec: SecondaryIndexSpec,
    ) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropNodePropertyIndex {
                label: label.to_string(),
                spec,
            })? {
            CoreWriteReply::Bool(dropped) => Ok(dropped),
            _ => unreachable!("drop_node_property_index must return bool"),
        }
    }

    pub fn list_node_property_indexes(&self) -> Result<Vec<NodePropertyIndexInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let catalog = published.label_catalog.as_ref();
        let mut entries: Vec<&SecondaryIndexManifestEntry> = published
            .view
            .secondary_index_entries
            .iter()
            .filter(|e| {
                matches!(
                    &e.target,
                    SecondaryIndexTarget::NodeProperty { .. }
                        | SecondaryIndexTarget::NodeFieldIndex { .. }
                )
            })
            .collect();
        entries.sort_unstable_by(|left, right| {
            secondary_index_target_label_id(&left.target)
                .cmp(&secondary_index_target_label_id(&right.target))
                .then_with(|| {
                    secondary_index_fields_sort_key(&left.target)
                        .cmp(&secondary_index_fields_sort_key(&right.target))
                })
                .then_with(|| {
                    secondary_index_kind_rank(&left.kind)
                        .cmp(&secondary_index_kind_rank(&right.kind))
                })
                .then_with(|| left.index_id.cmp(&right.index_id))
        });
        entries
            .into_iter()
            .map(|entry| EngineCore::node_property_index_info(entry, catalog))
            .collect()
    }

    pub fn ensure_edge_property_index(
        &self,
        label: &str,
        spec: SecondaryIndexSpec,
    ) -> Result<EdgePropertyIndexInfo, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::EnsureEdgePropertyIndex {
                label: label.to_string(),
                spec,
            })? {
            CoreWriteReply::EdgePropertyIndexInfo(info) => Ok(info),
            _ => unreachable!("ensure_edge_property_index must return index info"),
        }
    }

    pub fn drop_edge_property_index(
        &self,
        label: &str,
        spec: SecondaryIndexSpec,
    ) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropEdgePropertyIndex {
                label: label.to_string(),
                spec,
            })? {
            CoreWriteReply::Bool(dropped) => Ok(dropped),
            _ => unreachable!("drop_edge_property_index must return bool"),
        }
    }

    pub fn list_edge_property_indexes(&self) -> Result<Vec<EdgePropertyIndexInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let catalog = published.label_catalog.as_ref();
        let mut entries: Vec<&SecondaryIndexManifestEntry> = published
            .view
            .secondary_index_entries
            .iter()
            .filter(|e| {
                matches!(
                    &e.target,
                    SecondaryIndexTarget::EdgeProperty { .. }
                        | SecondaryIndexTarget::EdgeFieldIndex { .. }
                )
            })
            .collect();
        entries.sort_unstable_by(|left, right| {
            secondary_index_target_label_id(&left.target)
                .cmp(&secondary_index_target_label_id(&right.target))
                .then_with(|| {
                    secondary_index_fields_sort_key(&left.target)
                        .cmp(&secondary_index_fields_sort_key(&right.target))
                })
                .then_with(|| {
                    secondary_index_kind_rank(&left.kind)
                        .cmp(&secondary_index_kind_rank(&right.kind))
                })
                .then_with(|| left.index_id.cmp(&right.index_id))
        });
        entries
            .into_iter()
            .map(|entry| EngineCore::edge_property_index_info(entry, catalog))
            .collect()
    }

    pub fn get_node(&self, id: u64) -> Result<Option<NodeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let node = published.view.get_node(id)?;
        node.map(|node| node_view_from_record(node, published.label_catalog.as_ref()))
            .transpose()
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let edge = published.view.get_edge(id)?;
        edge.map(|edge| edge_view_from_record(edge, published.label_catalog.as_ref()))
            .transpose()
    }

    pub fn get_node_by_key(&self, label: &str, key: &str) -> Result<Option<NodeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        validate_label_token_name(label)?;
        let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
            return Ok(None);
        };
        let node = published.view.get_node_by_label_key(label_id, key)?;
        node.map(|node| {
            node_view_from_record_with_resolved_label(
                node,
                label_id,
                published.label_catalog.as_ref(),
            )
        })
        .transpose()
    }

    pub fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        label: &str,
    ) -> Result<Option<EdgeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        validate_label_token_name(label)?;
        let Some(label_id) = published.label_catalog.resolve_edge_label_for_read(label)? else {
            return Ok(None);
        };
        let edge = published.view.get_edge_by_triple(from, to, label_id)?;
        edge.map(|edge| {
            edge_view_from_record_with_resolved_label(edge, label_id, label.to_string())
        })
        .transpose()
    }

    pub fn get_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeView>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let nodes = published.view.get_nodes(ids)?;
        nodes
            .into_iter()
            .map(|node| {
                node.map(|node| node_view_from_record(node, published.label_catalog.as_ref()))
                    .transpose()
            })
            .collect()
    }

    pub fn get_nodes_by_keys(
        &self,
        keys: &[NodeKeyQuery],
    ) -> Result<Vec<Option<NodeView>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut label_ids: BTreeMap<&str, Option<u32>> = BTreeMap::new();
        for query in keys {
            validate_label_token_name(&query.label)?;
            let label = query.label.as_str();
            if !label_ids.contains_key(label) {
                let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
                label_ids.insert(label, label_id);
            }
        }

        let mut resolved = Vec::new();
        let mut positions = Vec::new();
        for (idx, query) in keys.iter().enumerate() {
            if let Some(label_id) = label_ids
                .get(query.label.as_str())
                .copied()
                .expect("label was validated and resolved in first pass")
            {
                positions.push((idx, label_id));
                resolved.push((label_id, query.key.as_str()));
            }
        }
        let mut output = vec![None; keys.len()];
        if resolved.is_empty() {
            return Ok(output);
        }
        let nodes = published.view.get_nodes_by_label_keys(&resolved)?;
        for ((idx, label_id), node) in positions.into_iter().zip(nodes) {
            output[idx] = node
                .map(|node| {
                    node_view_from_record_with_resolved_label(
                        node,
                        label_id,
                        published.label_catalog.as_ref(),
                    )
                })
                .transpose()?;
        }
        Ok(output)
    }

    pub fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeView>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let edges = published.view.get_edges(ids)?;
        edges
            .into_iter()
            .map(|edge| {
                edge.map(|edge| edge_view_from_record(edge, published.label_catalog.as_ref()))
                    .transpose()
            })
            .collect()
    }

    pub fn vector_search(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.vector_search(request)
    }

    pub fn edges_by_label(&self, label: &str) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_edge_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(Vec::new());
        };
        published.view.edges_by_label_id(label_id)
    }

    pub fn get_edges_by_label(&self, label: &str) -> Result<Vec<EdgeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_edge_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(Vec::new());
        };
        let label = label.to_string();
        published
            .view
            .get_edges_by_label_id(label_id)?
            .into_iter()
            .map(|edge| edge_view_from_record_with_resolved_label(edge, label_id, label.clone()))
            .collect()
    }

    pub fn nodes_by_labels<L>(&self, labels: L) -> Result<Vec<u64>, EngineError>
    where
        L: IntoNodeLabels,
    {
        let labels = labels.into_node_labels();
        validate_public_node_label_list(labels.iter().map(String::as_str))?;
        if let [label] = labels.as_slice() {
            let (_guard, published) = self.runtime.published_snapshot()?;
            let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
                return Ok(Vec::new());
            };
            return published.view.nodes_by_label_id(label_id);
        }
        Ok(self
            .query_node_ids(&NodeQuery {
                label_filter: Some(NodeLabelFilter {
                    labels,
                    mode: LabelMatchMode::All,
                }),
                ..Default::default()
            })?
            .items)
    }

    pub fn get_nodes_by_labels<L>(&self, labels: L) -> Result<Vec<NodeView>, EngineError>
    where
        L: IntoNodeLabels,
    {
        let labels = labels.into_node_labels();
        validate_public_node_label_list(labels.iter().map(String::as_str))?;
        if let [label] = labels.as_slice() {
            let (_guard, published) = self.runtime.published_snapshot()?;
            let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
                return Ok(Vec::new());
            };
            return published
                .view
                .get_nodes_by_label_id(label_id)?
                .into_iter()
                .map(|node| {
                    node_view_from_record_with_resolved_label(
                        node,
                        label_id,
                        published.label_catalog.as_ref(),
                    )
                })
                .collect();
        }
        Ok(self
            .query_nodes(&NodeQuery {
                label_filter: Some(NodeLabelFilter {
                    labels,
                    mode: LabelMatchMode::All,
                }),
                ..Default::default()
            })?
            .items)
    }

    pub fn count_nodes_by_labels<L>(&self, labels: L) -> Result<u64, EngineError>
    where
        L: IntoNodeLabels,
    {
        let labels = labels.into_node_labels();
        let (_guard, published) = self.runtime.published_snapshot()?;
        let filter = NodeLabelFilter {
            labels,
            mode: LabelMatchMode::All,
        };
        let resolved = published
            .label_catalog
            .resolve_node_label_filter_request(Some(&filter))?;
        published
            .view
            .count_nodes_by_resolved_label_filter(&resolved)
    }

    pub fn count_edges_by_label(&self, label: &str) -> Result<u64, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_edge_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(0);
        };
        published.view.count_edges_by_label_id(label_id)
    }

    pub fn nodes_by_labels_paged<L>(
        &self,
        labels: L,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError>
    where
        L: IntoNodeLabels,
    {
        let labels = labels.into_node_labels();
        validate_public_node_label_list(labels.iter().map(String::as_str))?;
        if let [label] = labels.as_slice() {
            let (_guard, published) = self.runtime.published_snapshot()?;
            let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
                return Ok(PageResult {
                    items: Vec::new(),
                    next_cursor: None,
                });
            };
            return published.view.nodes_by_label_id_paged(label_id, page);
        }
        let result = self.query_node_ids(&NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels,
                mode: LabelMatchMode::All,
            }),
            page: page.clone(),
            ..Default::default()
        })?;
        Ok(PageResult {
            items: result.items,
            next_cursor: result.next_cursor,
        })
    }

    pub fn get_nodes_by_labels_paged<L>(
        &self,
        labels: L,
        page: &PageRequest,
    ) -> Result<PageResult<NodeView>, EngineError>
    where
        L: IntoNodeLabels,
    {
        let labels = labels.into_node_labels();
        validate_public_node_label_list(labels.iter().map(String::as_str))?;
        if let [label] = labels.as_slice() {
            let (_guard, published) = self.runtime.published_snapshot()?;
            let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
                return Ok(PageResult {
                    items: Vec::new(),
                    next_cursor: None,
                });
            };
            let page = published.view.get_nodes_by_label_id_paged(label_id, page)?;
            let items = page
                .items
                .into_iter()
                .map(|node| {
                    node_view_from_record_with_resolved_label(
                        node,
                        label_id,
                        published.label_catalog.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(PageResult {
                items,
                next_cursor: page.next_cursor,
            });
        }
        let result = self.query_nodes(&NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels,
                mode: LabelMatchMode::All,
            }),
            page: page.clone(),
            ..Default::default()
        })?;
        Ok(PageResult {
            items: result.items,
            next_cursor: result.next_cursor,
        })
    }

    pub fn edges_by_label_paged(
        &self,
        label: &str,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_edge_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(PageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        published.view.edges_by_label_id_paged(label_id, page)
    }

    pub fn get_edges_by_label_paged(
        &self,
        label: &str,
        page: &PageRequest,
    ) -> Result<PageResult<EdgeView>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_edge_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(PageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        let label = label.to_string();
        let page = published.view.get_edges_by_label_id_paged(label_id, page)?;
        let items = page
            .items
            .into_iter()
            .map(|edge| edge_view_from_record_with_resolved_label(edge, label_id, label.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PageResult {
            items,
            next_cursor: page.next_cursor,
        })
    }

    pub fn find_nodes(
        &self,
        label: &str,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(Vec::new());
        };
        let outcome = published
            .view
            .find_nodes_outcome(label_id, prop_key, prop_value)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_paged(
        &self,
        label: &str,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(PageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        let outcome = published
            .view
            .find_nodes_paged_outcome(label_id, prop_key, prop_value, page)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_range(
        &self,
        label: &str,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            ReadView::validate_property_range_bounds(lower, upper, None)?;
            return Ok(Vec::new());
        };
        let outcome = published.view.find_nodes_range_paged_outcome(
            label_id,
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
        label: &str,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        page: &PropertyRangePageRequest,
    ) -> Result<PropertyRangePageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            ReadView::validate_property_range_bounds(lower, upper, page.after.as_ref())?;
            return Ok(PropertyRangePageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        let outcome = published
            .view
            .find_nodes_range_paged_outcome(label_id, prop_key, lower, upper, page)?;
        self.runtime.record_property_query_route(outcome.route);
        if let Some(followup) = outcome.followup {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn find_nodes_by_time_range(
        &self,
        label: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(Vec::new());
        };
        published
            .view
            .find_nodes_by_time_range(label_id, from_ms, to_ms)
    }

    pub fn find_nodes_by_time_range_paged(
        &self,
        label: &str,
        from_ms: i64,
        to_ms: i64,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let label_id = published.label_catalog.resolve_node_label_for_read(label)?;
        let Some(label_id) = label_id else {
            return Ok(PageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        published
            .view
            .find_nodes_by_time_range_paged(label_id, from_ms, to_ms, page)
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
        let entries = published.view.neighbors(node_id, options)?;
        entries
            .into_iter()
            .map(|entry| neighbor_entry_from_record(entry, published.label_catalog.as_ref()))
            .collect()
    }

    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        options: &NeighborOptions,
    ) -> Result<NodeIdMap<Vec<NeighborEntry>>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let batch = published.view.neighbors_batch(node_ids, options)?;
        let mut output =
            NodeIdMap::with_capacity_and_hasher(batch.len(), NodeIdBuildHasher::default());
        for (node_id, entries) in batch {
            let entries = entries
                .into_iter()
                .map(|entry| neighbor_entry_from_record(entry, published.label_catalog.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            output.insert(node_id, entries);
        }
        Ok(output)
    }

    pub fn neighbors_paged(
        &self,
        node_id: u64,
        options: &NeighborOptions,
        page: &PageRequest,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let page = published.view.neighbors_paged(node_id, options, page)?;
        let items = page
            .items
            .into_iter()
            .map(|entry| neighbor_entry_from_record(entry, published.label_catalog.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PageResult {
            items,
            next_cursor: page.next_cursor,
        })
    }

    pub fn top_k_neighbors(
        &self,
        node_id: u64,
        k: usize,
        options: &TopKOptions,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let entries = published.view.top_k_neighbors(node_id, k, options)?;
        entries
            .into_iter()
            .map(|entry| neighbor_entry_from_record(entry, published.label_catalog.as_ref()))
            .collect()
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

    pub fn scrub(&self) -> Result<ScrubReport, EngineError> {
        let manifest = self.manifest()?;
        crate::scrub::scrub_database(self.path(), &manifest)
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

    /// Return a raw manifest snapshot for diagnostics.
    ///
    /// This is an explicit introspection exception: the returned manifest may
    /// contain internal numeric token IDs and storage metadata. Ordinary public
    /// graph APIs use node labels and edge-label names instead.
    pub fn manifest(&self) -> Result<ManifestState, EngineError> {
        self.with_core_ref(|core| {
            let mut manifest = core.manifest.clone();
            merge_runtime_manifest_counters_from_shared(
                &mut manifest,
                &core.next_node_id_seen,
                &core.next_edge_id_seen,
                &core.engine_seq_seen,
            );
            merge_runtime_label_catalog_into_manifest(&mut manifest, &core.label_catalog);
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

    #[cfg(test)]
    pub(crate) fn schema_validation_overlay_build_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .schema_validation_overlay_builds
                .load(Ordering::Acquire))
        })
    }

    #[cfg(test)]
    pub(crate) fn schema_validation_incident_scan_chunk_count(&self) -> Result<usize, EngineError> {
        self.with_core_ref(|core| {
            Ok(core
                .schema_validation_incident_scan_chunks
                .load(Ordering::Acquire))
        })
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
        label_id: u32,
        key: &str,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        Ok(published
            .view
            .get_node_by_label_key_raw(label_id, key)?
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

    #[cfg(test)]
    fn published_label_catalog_snapshot_for_test(&self) -> Arc<ReadLabelCatalogSnapshot> {
        Arc::clone(&self.published_state().label_catalog)
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
        let seg_info = core
            .manifest
            .segments
            .iter()
            .find(|segment| segment.id == segment_id)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "segment {} is not present in the root manifest",
                    segment_id
                ))
            })?;
        let reader = SegmentReader::open_with_info(
            &seg_path,
            seg_info,
            core.manifest.dense_vector.as_ref(),
            &core.manifest.secondary_indexes,
        )?;
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
            .publish_locked(core, PublishImpact::RebuildSources, false)?;
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
            node_record_hydration_reads: published
                .view
                .query_execution_counters
                .node_record_hydration_reads
                .load(Ordering::Relaxed),
            node_visibility_meta_reads: published
                .view
                .query_execution_counters
                .node_visibility_meta_reads
                .load(Ordering::Relaxed),
            edge_record_hydration_reads: published
                .view
                .query_execution_counters
                .edge_record_hydration_reads
                .load(Ordering::Relaxed),
            edge_record_hydration_calls: published
                .view
                .query_execution_counters
                .edge_record_hydration_calls
                .load(Ordering::Relaxed),
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
            edge_full_scan_pages: published
                .view
                .query_execution_counters
                .edge_full_scan_pages
                .load(Ordering::Relaxed),
            endpoint_adjacency_candidates: published
                .view
                .query_execution_counters
                .endpoint_adjacency_candidates
                .load(Ordering::Relaxed),
            graph_row_query_calls: published
                .view
                .query_execution_counters
                .graph_row_query_calls
                .load(Ordering::Relaxed),
            node_selected_field_batches: published
                .view
                .query_execution_counters
                .selected_field_reads
                .node_selected_field_batches(),
            node_selected_field_ids: published
                .view
                .query_execution_counters
                .selected_field_reads
                .node_selected_field_ids(),
            edge_selected_field_batches: published
                .view
                .query_execution_counters
                .selected_field_reads
                .edge_selected_field_batches(),
            edge_selected_field_ids: published
                .view
                .query_execution_counters
                .selected_field_reads
                .edge_selected_field_ids(),
            node_dense_vector_projection_reads: published
                .view
                .query_execution_counters
                .selected_field_reads
                .node_dense_vector_projection_reads(),
            node_sparse_vector_projection_reads: published
                .view
                .query_execution_counters
                .selected_field_reads
                .node_sparse_vector_projection_reads(),
            public_node_query_calls: published
                .view
                .query_execution_counters
                .public_node_query_calls
                .load(Ordering::Relaxed),
            public_edge_query_calls: published
                .view
                .query_execution_counters
                .public_edge_query_calls
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_query_execution_counters_for_test(&self) {
        let published = self.published_state();
        published
            .view
            .query_execution_counters
            .node_record_hydration_reads
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .node_visibility_meta_reads
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .edge_record_hydration_reads
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .edge_record_hydration_calls
            .store(0, Ordering::Relaxed);
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
        published
            .view
            .query_execution_counters
            .edge_full_scan_pages
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .endpoint_adjacency_candidates
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .graph_row_query_calls
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .selected_field_reads
            .reset();
        published
            .view
            .query_execution_counters
            .public_node_query_calls
            .store(0, Ordering::Relaxed);
        published
            .view
            .query_execution_counters
            .public_edge_query_calls
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

    pub(crate) fn set_schema_validation_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.with_core_ref(|core| Ok(core.set_schema_validation_pause()))
            .expect("set schema validation pause")
    }

    pub(crate) fn force_next_runtime_manifest_write_error(&self) {
        let _ = self.with_core_mut(|core| {
            core.force_next_runtime_manifest_write_error();
            Ok(())
        });
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

    pub(crate) fn set_gql_mutation_before_commit_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self
            .runtime
            .gql_mutation_before_commit_pause
            .lock()
            .unwrap() = Some(RuntimeReadPauseHook {
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
    ) -> Result<ValidatedNumericRange, EngineError> {
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
    /// Whether to enforce edge uniqueness on (from, to, label_id).
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
    #[cfg(test)]
    schema_validation_overlay_builds: Arc<AtomicUsize>,
    #[cfg(test)]
    schema_validation_incident_scan_chunks: Arc<AtomicUsize>,
    /// Monotonic shared view of the latest durable engine_seq.
    engine_seq_seen: Arc<AtomicU64>,
    /// Shared runtime node-label/edge-label catalog. Manifest writers merge this
    /// before checkpointing so token WAL generations are not retired early.
    label_catalog: Arc<RwLock<RuntimeLabelCatalog>>,
    /// Compiled schema catalog keyed by numeric label IDs. Schema management stores it but
    /// does not consume it from write admission yet.
    #[allow(dead_code)]
    runtime_schema_catalog: RuntimeSchemaCatalog,
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
    /// Runtime declaration catalog keyed by logical target kind, label, ordered fields, and kind.
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
    #[cfg(test)]
    schema_validation_pause: Mutex<Option<RuntimeReadPauseHook>>,
    /// One-shot failure injection flag (test only).
    #[cfg(test)]
    flush_force_error: bool,
    #[cfg(test)]
    runtime_manifest_write_force_error: bool,
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
    input_segment_snapshots: Vec<SegmentInfo>,
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
    discriminant: SecondaryIndexTargetDiscriminant,
    target_label_id: u32,
    fields: Vec<SecondaryIndexField>,
    kind: SecondaryIndexKind,
}

enum SecondaryIndexJob {
    Build { index_id: u64 },
    DropCleanup { entry: SecondaryIndexManifestEntry },
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
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
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
        if created_manifest {
            let artifacts = manifestless_database_artifacts(path)?;
            if !artifacts.is_empty() {
                return Err(EngineError::ManifestError(format!(
                    "database artifacts exist without a manifest label-token schema: {}; old numeric-only databases are not migrated",
                    artifacts.join(", ")
                )));
            }
        }
        let mut manifest = match loaded_manifest {
            Some(m) => m,
            None => default_manifest(),
        };
        let mut manifest_dirty = false;
        manifest_dirty |= reconcile_dense_vector_manifest(&mut manifest, options)?;
        manifest_dirty |= normalize_schema_manifest(&mut manifest)?;
        manifest_dirty |= normalize_secondary_index_manifest(&mut manifest)?;
        if created_manifest || manifest_dirty {
            write_manifest(path, &manifest)?;
        };

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
                let reader = SegmentReader::open_with_info(
                    &seg_path,
                    seg_info,
                    manifest.dense_vector.as_ref(),
                    &manifest.secondary_indexes,
                )?;
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

        let mut runtime_label_catalog = RuntimeLabelCatalog::from_manifest(&manifest)?;

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
            let (frozen_mt, degree_overlay, _) = replay_wal_generation_to_memtable_and_overlay(
                path,
                wal_gen_id,
                manifest.dense_vector.as_ref(),
                &mut runtime_label_catalog,
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
        let (memtable, active_degree_overlay, active_wal_durable_len) =
            replay_wal_generation_to_memtable_and_overlay(
                path,
                manifest.active_wal_generation_id,
                manifest.dense_vector.as_ref(),
                &mut runtime_label_catalog,
                &mut engine_seq,
                &immutable_epochs_on_open,
                &segments,
            )?;
        truncate_wal_generation_to(
            path,
            manifest.active_wal_generation_id,
            active_wal_durable_len,
        )?;
        runtime_label_catalog.apply_to_manifest(&mut manifest);
        let runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&manifest)?;

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
        cleanup_orphan_optional_refresh_files(path, &manifest);
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
        let label_catalog = Arc::new(RwLock::new(runtime_label_catalog));
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
            #[cfg(test)]
            schema_validation_overlay_builds: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            schema_validation_incident_scan_chunks: Arc::new(AtomicUsize::new(0)),
            engine_seq_seen,
            label_catalog,
            runtime_schema_catalog,
            manifest_write_lock,
            immutable_epochs: immutable_epochs_on_open,
            immutable_bytes_total: immutable_bytes_on_open,
            active_wal_generation_id,
            bg_flush: None,
            secondary_index_catalog: Arc::new(RwLock::new(SecondaryIndexCatalog::default())),
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
            schema_validation_pause: Mutex::new(None),
            #[cfg(test)]
            flush_force_error: false,
            #[cfg(test)]
            runtime_manifest_write_force_error: false,
        };

        engine.recover_secondary_index_states_on_open()?;
        engine.rebuild_secondary_index_catalog()?;
        engine.seed_secondary_indexes_from_manifest()?;
        engine.warm_declared_index_runtime_coverage_for_current_readers();
        engine.rebuild_published_read_sources()?;

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
        self.with_synced_runtime_manifest_write(|manifest| {
            manifest.active_wal_generation_id = active_wal_generation_id;
            Ok(())
        })
    }

    fn active_memtable(&self) -> &Memtable {
        &self.memtable
    }

    fn build_read_manifest_state(&self) -> Result<ReadManifestState, EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        let prune_policies = self
            .manifest
            .prune_policies
            .iter()
            .map(|(name, policy)| {
                resolve_manifest_prune_policy(policy, &catalog).map(|policy| (name.clone(), policy))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(ReadManifestState {
            prune_policies,
            dense_vector: self.manifest.dense_vector.clone(),
        })
    }

    fn resolved_manifest_prune_policies(&self) -> Result<Vec<ResolvedPrunePolicy>, EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        self.manifest
            .prune_policies
            .values()
            .map(|policy| resolve_manifest_prune_policy(policy, &catalog))
            .collect()
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

    fn build_published_read_sources(
        &self,
        generation: u64,
    ) -> Result<Arc<PublishedReadSources>, EngineError> {
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
            generation,
            &self.segments,
            &secondary_index_entries,
            declared_index_runtime_coverage.as_ref(),
        ));
        Ok(Arc::new(PublishedReadSources {
            manifest: self.build_read_manifest_state()?,
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
        }))
    }

    fn rebuild_published_read_sources(&mut self) -> Result<(), EngineError> {
        let generation = self.published_read_sources_generation.saturating_add(1);
        let sources = self.build_published_read_sources(generation)?;
        self.published_read_sources_generation = generation;
        self.published_read_sources = Some(sources);
        Ok(())
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
            self.read_label_catalog_snapshot(),
        )
    }

    fn published_read_state(&self) -> PublishedReadState {
        self.published_read_state_with_catalog(self.read_label_catalog_snapshot())
    }

    fn published_read_state_with_catalog(
        &self,
        label_catalog: Arc<ReadLabelCatalogSnapshot>,
    ) -> PublishedReadState {
        PublishedReadState {
            view: Arc::new(ReadView::from_published_sources(
                self.current_published_read_sources(),
                self.engine_seq,
                Arc::clone(&self.active_degree_overlay),
                Arc::clone(&label_catalog),
            )),
            label_catalog,
            schema_catalog: Arc::new(PublishedSchemaCatalogSnapshot {
                next_schema_id: self.manifest.next_schema_id,
                node_schemas: self.manifest.node_schemas.clone(),
                edge_schemas: self.manifest.edge_schemas.clone(),
            }),
            edge_uniqueness: self.edge_uniqueness,
            #[cfg(test)]
            engine_seq: self.engine_seq,
            #[cfg(test)]
            active_wal_generation_id: self.active_wal_generation_id,
        }
    }

    fn read_label_catalog_snapshot(&self) -> Arc<ReadLabelCatalogSnapshot> {
        let catalog = self.label_catalog.read().unwrap();
        Arc::new(ReadLabelCatalogSnapshot::from_runtime(&catalog))
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
                    SecondaryIndexKind::Equality => match &entry.target {
                        SecondaryIndexTarget::NodeProperty { .. } => segment
                            .secondary_eq_sidecar_lightweight_available_for_target(
                                entry.index_id,
                                PlannerStatsDeclaredIndexTarget::NodeProperty,
                            ),
                        SecondaryIndexTarget::EdgeProperty { .. } => segment
                            .secondary_eq_sidecar_lightweight_available_for_target(
                                entry.index_id,
                                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                            ),
                        SecondaryIndexTarget::NodeFieldIndex { .. }
                        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
                            segment.compound_sidecar_lightweight_available_for_entry(entry)
                        }
                    },
                    SecondaryIndexKind::Range => match &entry.target {
                        SecondaryIndexTarget::NodeProperty { .. } => segment
                            .secondary_range_sidecar_lightweight_available_for_target(
                                entry.index_id,
                                PlannerStatsDeclaredIndexTarget::NodeProperty,
                            ),
                        SecondaryIndexTarget::EdgeProperty { .. } => segment
                            .secondary_range_sidecar_lightweight_available_for_target(
                                entry.index_id,
                                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                            ),
                        SecondaryIndexTarget::NodeFieldIndex { .. }
                        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
                            segment.compound_sidecar_lightweight_available_for_entry(entry)
                        }
                    },
                };
                match validation {
                    Ok(true) => continue,
                    Ok(false) => {
                        let kind = secondary_index_component_kind_for_recovery(entry);
                        match segment.optional_component_availability(kind) {
                            ComponentAvailability::Missing | ComponentAvailability::Available => {
                                entry.state = SecondaryIndexState::Building;
                                entry.last_error = None;
                            }
                            ComponentAvailability::Incompatible { reason }
                            | ComponentAvailability::CorruptIdentity { reason }
                            | ComponentAvailability::Unsupported { reason } => {
                                entry.state = SecondaryIndexState::Failed;
                                entry.last_error =
                                    Some(secondary_index_failure_message_for_entry(entry, reason));
                            }
                        }
                        dirty = true;
                        break;
                    }
                    Err(error) => {
                        entry.state = SecondaryIndexState::Failed;
                        entry.last_error = Some(secondary_index_failure_message_for_entry(
                            entry,
                            error.to_string(),
                        ));
                        dirty = true;
                        break;
                    }
                }
            }
        }

        if dirty {
            let new_manifest = {
                let _guard = self.manifest_write_lock.lock().unwrap();
                let mut manifest = self.load_current_manifest_for_write()?;
                manifest.secondary_indexes = self.manifest.secondary_indexes.clone();
                self.merge_checkpointed_runtime_manifest_state(&mut manifest);
                write_manifest(&self.db_dir, &manifest)?;
                manifest
            };
            self.manifest = new_manifest;
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
        let label_catalog = Arc::clone(&self.label_catalog);
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
                label_catalog,
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
            .filter(|entry| {
                entry.state == SecondaryIndexState::Building
                    && secondary_index_target_requires_sidecar_build(&entry.target)
            })
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

    fn checkpointable_wal_generation(&self) -> Option<u64> {
        self.active_wal_generation_id.checked_sub(1)
    }

    fn merge_runtime_manifest_counters(&self, manifest: &mut ManifestState) {
        merge_runtime_manifest_counters_from_shared(
            manifest,
            &self.next_node_id_seen,
            &self.next_edge_id_seen,
            &self.engine_seq_seen,
        );
    }

    fn merge_checkpointed_runtime_manifest_state(&self, manifest: &mut ManifestState) {
        self.merge_runtime_manifest_counters(manifest);
        merge_checkpointed_label_catalog_into_manifest(
            manifest,
            &self.label_catalog,
            self.checkpointable_wal_generation(),
        );
    }

    fn merge_synced_runtime_manifest_state(&self, manifest: &mut ManifestState) {
        self.merge_runtime_manifest_counters(manifest);
        merge_runtime_label_catalog_into_manifest(manifest, &self.label_catalog);
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
        merge_checkpointed_label_catalog_into_manifest(
            &mut manifest,
            &self.label_catalog,
            self.checkpointable_wal_generation(),
        );
        Ok(manifest)
    }

    fn load_current_manifest_for_synced_write(&self) -> Result<ManifestState, EngineError> {
        let mut manifest = load_manifest_readonly(&self.db_dir)?
            .ok_or_else(|| EngineError::ManifestError("manifest missing".into()))?;
        manifest.next_wal_generation_id = manifest
            .next_wal_generation_id
            .max(self.manifest.next_wal_generation_id);
        manifest.active_wal_generation_id = manifest
            .active_wal_generation_id
            .max(self.manifest.active_wal_generation_id);
        merge_runtime_label_catalog_into_manifest(&mut manifest, &self.label_catalog);
        Ok(manifest)
    }

    fn with_runtime_manifest_write<T>(
        &mut self,
        mutate: impl FnOnce(&mut ManifestState) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let _guard = self.manifest_write_lock.lock().unwrap();
        let mut manifest = self.load_current_manifest_for_write()?;
        let result = mutate(&mut manifest)?;
        self.merge_checkpointed_runtime_manifest_state(&mut manifest);
        #[cfg(test)]
        if self.runtime_manifest_write_force_error {
            self.runtime_manifest_write_force_error = false;
            return Err(EngineError::ManifestError(
                "test forced runtime manifest write failure".to_string(),
            ));
        }
        write_manifest(&self.db_dir, &manifest)?;
        self.manifest = manifest;
        Ok(result)
    }

    fn with_synced_runtime_manifest_write<T>(
        &mut self,
        mutate: impl FnOnce(&mut ManifestState) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let _guard = self.manifest_write_lock.lock().unwrap();
        let mut manifest = self.load_current_manifest_for_synced_write()?;
        let result = mutate(&mut manifest)?;
        self.merge_synced_runtime_manifest_state(&mut manifest);
        write_manifest(&self.db_dir, &manifest)?;
        self.manifest = manifest;
        Ok(result)
    }

    // --- Write path helpers ---

    fn apply_label_token_op_after_wal_append(&mut self, op: &WalOp) -> Result<(), EngineError> {
        match op {
            WalOp::EnsureNodeLabel { label, label_id } => {
                {
                    let mut catalog = self.label_catalog.write().unwrap();
                    catalog.apply_node_label(
                        label.clone(),
                        *label_id,
                        Some(self.active_wal_generation_id),
                    )?;
                    catalog.apply_to_manifest(&mut self.manifest);
                }
                Ok(())
            }
            WalOp::EnsureEdgeLabel { label, label_id } => {
                {
                    let mut catalog = self.label_catalog.write().unwrap();
                    catalog.apply_edge_label(
                        label.clone(),
                        *label_id,
                        Some(self.active_wal_generation_id),
                    )?;
                    catalog.apply_to_manifest(&mut self.manifest);
                }
                Ok(())
            }
            WalOp::UpsertNode(node) => {
                let catalog = self.label_catalog.read().unwrap();
                for &label_id in node.label_ids.as_slice() {
                    if !catalog.node_id_to_label.contains_key(&label_id) {
                        return Err(EngineError::InvalidOperation(format!(
                            "node label_id {} does not exist in the node label catalog",
                            label_id
                        )));
                    }
                }
                Ok(())
            }
            WalOp::UpsertEdge(edge) => {
                if self
                    .label_catalog
                    .read()
                    .unwrap()
                    .edge_id_to_label
                    .contains_key(&edge.label_id)
                {
                    Ok(())
                } else {
                    Err(EngineError::InvalidOperation(format!(
                        "edge-label label_id {} does not exist in the edge-label catalog",
                        edge.label_id
                    )))
                }
            }
            WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => Ok(()),
            WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => {
                Err(EngineError::InvalidOperation(
                    "WAL atomic batch markers cannot be applied as normal write ops".into(),
                ))
            }
        }
    }

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
            WalOp::DeleteNode { .. }
            | WalOp::UpsertNode(_)
            | WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => {}
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
            WalOp::UpsertNode(_)
            | WalOp::DeleteNode { .. }
            | WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => None,
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
            WalOp::DeleteEdge { .. }
            | WalOp::DeleteNode { .. }
            | WalOp::UpsertNode(_)
            | WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => None,
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
            self.apply_label_token_op_after_wal_append(op)?;
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
        self.apply_label_token_op_after_wal_append(op)?;
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
        let new_wal_gen = self.with_synced_runtime_manifest_write(|manifest| {
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
        let label_catalog = Arc::clone(&self.label_catalog);
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
                label_catalog,
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
                        if matches!(entry.kind, SecondaryIndexKind::Range)
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

        // Snapshot current root SegmentInfo and paths for the background thread.
        let input_segments: Vec<(SegmentInfo, PathBuf)> = self
            .segments
            .iter()
            .filter_map(|s| {
                segment_info_for_id(&self.manifest.segments, s.segment_id)
                    .cloned()
                    .map(|info| (info, segment_dir(&self.db_dir, s.segment_id)))
            })
            .collect();
        if input_segments.len() != self.segments.len() {
            return Err(EngineError::ManifestError(
                "background compaction snapshot missing root segment info".into(),
            ));
        }
        // Allocate the output segment ID on the main thread.
        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Reset flush counter (same as synchronous compact).
        self.flush_count_since_last_compact = 0;

        let db_dir = self.db_dir.clone();
        let prune_policies = self.resolved_manifest_prune_policies()?;
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

            for input_info in &result.input_segment_snapshots {
                let Some(live_info) = segment_info_for_id(&manifest.segments, input_info.id) else {
                    let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
                    let _ = std::fs::remove_dir_all(output_dir);
                    return None;
                };
                if live_info.segment_data_id != input_info.segment_data_id {
                    let output_dir = segment_dir(&self.db_dir, result.stats.output_segment_id);
                    let _ = std::fs::remove_dir_all(output_dir);
                    return None;
                }
            }
            let input_segment_ids: NodeIdSet = result
                .input_segment_snapshots
                .iter()
                .map(|segment| segment.id)
                .collect();

            manifest
                .segments
                .retain(|s| !input_segment_ids.contains(&s.id));
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
            self.merge_checkpointed_runtime_manifest_state(&mut manifest);

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
        let input_segment_ids: NodeIdSet = result
            .input_segment_snapshots
            .iter()
            .map(|segment| segment.id)
            .collect();
        self.segments
            .retain(|s| !input_segment_ids.contains(&s.segment_id));
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
        let policies = self.resolved_manifest_prune_policies()?;
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
        if let Some(parent) = final_dir.parent() {
            if let Err(e) = fsync_dir(parent) {
                self.next_segment_id -= 1;
                let _ = std::fs::remove_dir_all(&final_dir);
                return Err(e);
            }
        }

        // Open new segment reader BEFORE modifying any state (M3 fix)
        let new_reader = match SegmentReader::open_with_info(
            &final_dir,
            &seg_info,
            self.manifest.dense_vector.as_ref(),
            &secondary_indexes,
        ) {
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
            self.merge_checkpointed_runtime_manifest_state(&mut manifest);
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
        prune_policies: &[ResolvedPrunePolicy],
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
            WalOp::EnsureNodeLabel { .. } | WalOp::EnsureEdgeLabel { .. } => {}
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

struct ReplayAtomicBatch {
    first_seq: u64,
    op_count: u32,
    ops: Vec<(u64, WalOp)>,
}

impl ReplayAtomicBatch {
    fn new(first_seq: u64, op_count: u32) -> Option<Self> {
        if first_seq == 0 || op_count < 2 {
            return None;
        }
        Some(Self {
            first_seq,
            op_count,
            ops: Vec::new(),
        })
    }

    fn push(&mut self, seq: u64, op: WalOp) -> bool {
        let Ok(op_count) = usize::try_from(self.op_count) else {
            return false;
        };
        if self.ops.len() >= op_count {
            return false;
        }
        self.ops.push((seq, op));
        true
    }

    fn matches_commit(&self, first_seq: u64, op_count: u32) -> bool {
        if self.first_seq != first_seq || self.op_count != op_count {
            return false;
        }
        if self.ops.len() != op_count as usize {
            return false;
        }
        self.ops.iter().enumerate().all(|(idx, (seq, _))| {
            self.first_seq
                .checked_add(idx as u64)
                .is_some_and(|expected| *seq == expected)
        })
    }
}

fn replay_apply_normalized_op(
    memtable: &Memtable,
    degree_deltas: &mut NodeIdMap<DegreeDelta>,
    engine_seq: &mut u64,
    seq: u64,
    op: &WalOp,
    old_edge: Option<OldEdgeInfo>,
) {
    *engine_seq = (*engine_seq).max(seq);
    memtable.apply_op(op, seq);
    EngineCore::collect_degree_delta_for_op(op, old_edge, degree_deltas);
}

fn capture_replay_batch_edge_states(
    memtable: &Memtable,
    immutable_epochs: &[ImmutableEpoch],
    segments: &[Arc<SegmentReader>],
    ops: &[(u64, WalOp)],
) -> Result<NodeIdMap<Option<EdgeCore>>, EngineError> {
    let mut states: NodeIdMap<Option<EdgeCore>> = NodeIdMap::default();
    for (_, op) in ops {
        let Some(edge_id) = EngineCore::edge_id_for_degree_op(op) else {
            continue;
        };
        if states.contains_key(&edge_id) {
            continue;
        }
        states.insert(
            edge_id,
            get_edge_core_from_sources(memtable, immutable_epochs, segments, edge_id)?,
        );
    }
    Ok(states)
}

#[allow(clippy::too_many_arguments)]
fn replay_apply_committed_atomic_batch(
    batch: ReplayAtomicBatch,
    memtable: &Memtable,
    degree_deltas: &mut NodeIdMap<DegreeDelta>,
    dense_config: Option<&DenseVectorConfig>,
    label_catalog: &mut RuntimeLabelCatalog,
    engine_seq: &mut u64,
    wal_generation_id: u64,
    immutable_epochs: &[ImmutableEpoch],
    segments: &[Arc<SegmentReader>],
) -> Result<(), EngineError> {
    let normalized_ops: Vec<(u64, WalOp)> = batch
        .ops
        .into_iter()
        .map(|(seq, op)| normalize_wal_op_for_replay(dense_config, op).map(|op| (seq, op)))
        .collect::<Result<_, _>>()?;

    let mut staged_catalog = label_catalog.clone();
    for (_, op) in &normalized_ops {
        validate_or_apply_replayed_label_token_op(&mut staged_catalog, op, wal_generation_id)?;
    }

    let mut edge_states =
        capture_replay_batch_edge_states(memtable, immutable_epochs, segments, &normalized_ops)?;
    *label_catalog = staged_catalog;

    for (seq, op) in normalized_ops {
        let edge_id = EngineCore::edge_id_for_degree_op(&op);
        let old_edge = edge_id
            .and_then(|id| edge_states.get(&id).copied().flatten())
            .map(OldEdgeInfo::from_core);
        replay_apply_normalized_op(memtable, degree_deltas, engine_seq, seq, &op, old_edge);
        if let Some(edge_id) = edge_id {
            edge_states.insert(edge_id, EngineCore::edge_core_after_op(&op));
        }
    }

    Ok(())
}

fn replay_wal_generation_to_memtable_and_overlay(
    db_dir: &Path,
    wal_generation_id: u64,
    dense_config: Option<&DenseVectorConfig>,
    label_catalog: &mut RuntimeLabelCatalog,
    engine_seq: &mut u64,
    immutable_epochs: &[ImmutableEpoch],
    segments: &[Arc<SegmentReader>],
) -> Result<(Memtable, Arc<DegreeOverlaySnapshot>, u64), EngineError> {
    let memtable = Memtable::new();
    let mut degree_deltas: NodeIdMap<DegreeDelta> = NodeIdMap::default();
    let mut open_batch: Option<ReplayAtomicBatch> = None;
    let read_result = WalReader::read_generation_recoverable(db_dir, wal_generation_id)?;
    let durable_len = read_result.durable_len;

    for (seq, op) in read_result.records {
        match op {
            WalOp::BeginAtomicBatch {
                first_seq,
                op_count,
            } => {
                if open_batch.is_some() {
                    break;
                }
                let Some(batch) = ReplayAtomicBatch::new(first_seq, op_count) else {
                    break;
                };
                open_batch = Some(batch);
            }
            WalOp::CommitAtomicBatch {
                first_seq,
                op_count,
            } => {
                let Some(batch) = open_batch.take() else {
                    break;
                };
                if !batch.matches_commit(first_seq, op_count) {
                    break;
                }
                replay_apply_committed_atomic_batch(
                    batch,
                    &memtable,
                    &mut degree_deltas,
                    dense_config,
                    label_catalog,
                    engine_seq,
                    wal_generation_id,
                    immutable_epochs,
                    segments,
                )?;
            }
            op => {
                if let Some(batch) = open_batch.as_mut() {
                    if !batch.push(seq, op) {
                        break;
                    }
                    continue;
                }

                let op = normalize_wal_op_for_replay(dense_config, op)?;
                validate_or_apply_replayed_label_token_op(label_catalog, &op, wal_generation_id)?;
                let old_edge = EngineCore::edge_id_for_degree_op(&op)
                    .map(|id| get_edge_core_from_sources(&memtable, immutable_epochs, segments, id))
                    .transpose()?
                    .flatten()
                    .map(OldEdgeInfo::from_core);
                replay_apply_normalized_op(
                    &memtable,
                    &mut degree_deltas,
                    engine_seq,
                    seq,
                    &op,
                    old_edge,
                );
            }
        }
    }

    Ok((
        memtable,
        DegreeOverlaySnapshot::from_flat(degree_deltas),
        durable_len,
    ))
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

fn cleanup_orphan_optional_refresh_files(db_dir: &Path, manifest: &ManifestState) {
    for segment in &manifest.segments {
        cleanup_orphan_optional_component_files(&segment_dir(db_dir, segment.id));
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

    #[cfg(test)]
    pub(crate) fn set_schema_validation_pause(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        *self.schema_validation_pause.lock().unwrap() = Some(RuntimeReadPauseHook {
            ready_tx,
            release_rx,
        });
        (ready_rx, release_tx)
    }

    #[cfg(test)]
    fn pause_schema_validation_for_test(&self) {
        if let Some(hook) = self.schema_validation_pause.lock().unwrap().take() {
            let _ = hook.ready_tx.send(());
            let _ = hook.release_rx.recv();
        }
    }

    #[cfg(test)]
    pub(crate) fn force_next_runtime_manifest_write_error(&mut self) {
        self.runtime_manifest_write_force_error = true;
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
                            let maintained_index_ids =
                                match maintained_secondary_index_ids_from_segment_manifest(
                                    &work.final_dir,
                                    &current_secondary_indexes,
                                ) {
                                    Ok(ids) => ids,
                                    Err(e) => {
                                        let _ = std::fs::remove_dir_all(&work.final_dir);
                                        send_bg_flush_event(
                                            &event_tx,
                                            &events_ready,
                                            &runtime,
                                            BgFlushEvent::Failed(FlushPipelineError {
                                                epoch_id: work.epoch_id,
                                                wal_generation_id: work.wal_gen_id,
                                                stage: FlushPipelineStage::Build,
                                                message: format!(
                                                    "segment maintained index scan failed: {}",
                                                    e
                                                ),
                                            }),
                                        );
                                        cancel.store(true, Ordering::Relaxed);
                                        break;
                                    }
                                };
                            BuiltFlushResult {
                                epoch_id: work.epoch_id,
                                wal_gen_to_retire: work.wal_gen_id,
                                seg_info,
                                seg_id: work.seg_id,
                                final_dir: work.final_dir,
                                dense_config: work.dense_config,
                                maintained_equality_index_ids: maintained_index_ids
                                    .equality_index_ids,
                                maintained_range_index_ids: maintained_index_ids.range_index_ids,
                                secondary_indexes: current_secondary_indexes,
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
    label_catalog: Arc<RwLock<RuntimeLabelCatalog>>,
    cancel: Arc<AtomicBool>,
    events_ready: Arc<AtomicUsize>,
    runtime: Option<std::sync::Weak<DbRuntime>>,
    #[cfg(test)] publish_pause: Arc<Mutex<Option<FlushPublishPauseHook>>>,
) {
    while let Ok(result) = rx.recv() {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        let reader = match SegmentReader::open_with_info(
            &result.final_dir,
            &result.seg_info,
            result.dense_config.as_ref(),
            &result.secondary_indexes,
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
            merge_checkpointed_label_catalog_into_manifest(
                &mut manifest,
                &label_catalog,
                Some(result.wal_gen_to_retire),
            );
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
    input_segments: Vec<(SegmentInfo, PathBuf)>,
    prune_policies: Vec<ResolvedPrunePolicy>,
    dense_vector: Option<DenseVectorConfig>,
    secondary_indexes: SecondaryIndexEntries,
    cancel: &AtomicBool,
    #[cfg(test)] compact_pause: &Arc<Mutex<Option<BgCompactPauseHook>>>,
) -> Result<BgCompactResult, EngineError> {
    let compact_start = std::time::Instant::now();

    #[cfg(test)]
    if let Some(hook) = compact_pause.lock().unwrap().take() {
        let _ = hook.ready_tx.send(());
        let _ = hook.release_rx.recv();
    }

    // Re-open input segments (independent mmap handles, safe to use concurrently
    // with the main thread's readers of the same files).
    let mut segments = Vec::with_capacity(input_segments.len());
    for (info, path) in &input_segments {
        segments.push(Arc::new(SegmentReader::open_with_info(
            path,
            info,
            dense_vector.as_ref(),
            &secondary_indexes,
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
    if let Some(parent) = final_dir.parent() {
        if let Err(e) = fsync_dir(parent) {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(e);
        }
    }
    let maintained_index_ids = match maintained_secondary_index_ids_from_segment_manifest(
        &final_dir,
        &secondary_indexes,
    ) {
        Ok(ids) => ids,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(error);
        }
    };

    // Open the output segment reader (will be sent back to the main thread).
    let reader = match SegmentReader::open_with_info(
        &final_dir,
        &seg_info,
        dense_vector.as_ref(),
        &secondary_indexes,
    ) {
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

    let input_segment_snapshots: Vec<SegmentInfo> = input_segments
        .iter()
        .map(|(info, _)| info.clone())
        .collect();
    let old_seg_dirs: Vec<PathBuf> = input_segments
        .iter()
        .map(|(info, _)| segment_dir(&db_dir, info.id))
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
        input_segment_snapshots,
        maintained_equality_index_ids: maintained_index_ids.equality_index_ids,
        maintained_range_index_ids: maintained_index_ids.range_index_ids,
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
    label_ids: NodeLabelSet,
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
    label_id: u32,
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
    label_ids: &NodeLabelSet,
    updated_at: i64,
    weight: f32,
    policies: &[ResolvedPrunePolicy],
    now: i64,
) -> bool {
    for policy in policies {
        let age_cutoff = policy.max_age_ms.map(|age| now - age);
        if matches_prune_cutoff(
            label_ids,
            updated_at,
            weight,
            age_cutoff,
            policy.max_weight,
            policy.label_id,
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
    prune_policies: &[ResolvedPrunePolicy],
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
            let meta = seg.node_meta_at(i)?;
            let (dense_vector_offset, dense_vector_len, sparse_vector_offset, sparse_vector_len) =
                seg.node_vector_meta_at(i)?;

            if seen_nodes.contains(&meta.node_id) {
                continue; // Already have a newer version
            }
            seen_nodes.insert(meta.node_id);

            if deleted_nodes.contains(&meta.node_id) {
                continue; // Tombstoned
            }

            if has_policies
                && matches_any_prune_policy_meta(
                    &meta.label_ids,
                    meta.updated_at,
                    meta.weight,
                    prune_policies,
                    now,
                )
            {
                pruned_node_ids.insert(meta.node_id);
                continue;
            }

            node_winners.insert(
                meta.node_id,
                NodeWinner {
                    seg_idx,
                    data_offset: meta.data_offset,
                    data_len: meta.data_len,
                    label_ids: meta.label_ids,
                    updated_at: meta.updated_at,
                    weight: meta.weight,
                    key_len: meta.key_len,
                    dense_vector_offset,
                    dense_vector_len,
                    sparse_vector_offset,
                    sparse_vector_len,
                    last_write_seq: meta.last_write_seq,
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
                label_id,
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
                    label_id,
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

    let mut core_writer = create_compaction_core_writer(tmp_dir, seg_id)?;

    // Raw-copy winning records to output data files
    let (node_record, node_data) =
        write_v3_nodes_dat(&mut core_writer, segments, &node_winner_list)?;
    let (edge_record, edge_data) =
        write_v3_edges_dat(&mut core_writer, segments, &edge_winner_list)?;

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
            label_ids: w.label_ids,
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
            label_id: w.label_id,
            updated_at: w.updated_at,
            weight: w.weight,
            valid_from: w.valid_from,
            valid_to: w.valid_to,
            src_seg_idx: w.seg_idx,
            src_data_offset: w.data_offset,
            last_write_seq: w.last_write_seq,
        });
    }

    let (source_groups, dense_points) = write_compaction_source_components(
        seg_id,
        &mut core_writer,
        segments,
        node_record,
        edge_record,
        &node_metas,
        &edge_metas,
    )?;

    // Build all secondary indexes and sidecars from metadata
    let component_output = write_indexes_from_metadata_with_secondary_indexes(
        seg_id,
        tmp_dir,
        &mut core_writer,
        segments,
        &node_metas,
        &edge_metas,
        dense_config,
        dense_points,
        write_degree_sidecar,
        secondary_indexes,
        source_groups,
    )?;
    let mut records = component_output.records;
    records.extend(finish_compaction_core_writer(core_writer)?);

    let node_count = plan.node_winners.len() as u64;
    let edge_count = plan.edge_winners.len() as u64;
    let seg_info = finalize_compaction_segment(tmp_dir, seg_id, node_count, edge_count, records)?;

    Ok((seg_info, component_output.report))
}

fn collect_fast_merge_node_metas(
    segments: &[Arc<SegmentReader>],
    copy_info: &[FastMergeCopyInfo],
) -> Result<Vec<CompactNodeMeta>, EngineError> {
    let mut metas = Vec::new();
    for (seg_idx, seg) in segments.iter().enumerate() {
        let info = &copy_info[seg_idx];
        for i in 0..seg.node_meta_count() as usize {
            let meta = seg.node_meta_at(i)?;
            let (dense_vector_offset, dense_vector_len, sparse_vector_offset, sparse_vector_len) =
                seg.node_vector_meta_at(i)?;
            let rebased_offset = info
                .new_data_base
                .checked_add(
                    meta.data_offset
                        .checked_sub(info.orig_data_start)
                        .ok_or_else(|| {
                            EngineError::CorruptRecord(format!(
                                "segment {} node {} data offset {} precedes data section {}",
                                seg.segment_id,
                                meta.node_id,
                                meta.data_offset,
                                info.orig_data_start
                            ))
                        })?,
                )
                .ok_or_else(|| {
                    EngineError::CorruptRecord(format!(
                        "segment {} node {} merged offset overflow",
                        seg.segment_id, meta.node_id
                    ))
                })?;
            metas.push(CompactNodeMeta {
                node_id: meta.node_id,
                new_data_offset: rebased_offset,
                data_len: meta.data_len,
                label_ids: meta.label_ids,
                updated_at: meta.updated_at,
                weight: meta.weight,
                key_len: meta.key_len,
                dense_vector_offset,
                dense_vector_len,
                sparse_vector_offset,
                sparse_vector_len,
                src_seg_idx: seg_idx,
                src_data_offset: meta.data_offset,
                last_write_seq: meta.last_write_seq,
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
                label_id,
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
                label_id,
                updated_at,
                weight,
                valid_from,
                valid_to,
                src_seg_idx: seg_idx,
                src_data_offset: data_offset,
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

    let mut core_writer = create_compaction_core_writer(tmp_dir, seg_id)?;
    let (node_record, node_copy_info) = write_merged_nodes_dat(&mut core_writer, segments)?;
    let (edge_record, edge_copy_info) = write_merged_edges_dat(&mut core_writer, segments)?;
    let node_metas = collect_fast_merge_node_metas(segments, &node_copy_info)?;
    let edge_metas = collect_fast_merge_edge_metas(segments, &edge_copy_info)?;

    let (source_groups, dense_points) = write_compaction_source_components(
        seg_id,
        &mut core_writer,
        segments,
        node_record,
        edge_record,
        &node_metas,
        &edge_metas,
    )?;

    let component_output = write_indexes_from_metadata_with_secondary_indexes(
        seg_id,
        tmp_dir,
        &mut core_writer,
        segments,
        &node_metas,
        &edge_metas,
        dense_config,
        dense_points,
        true,
        secondary_indexes,
        source_groups,
    )?;
    let mut records = component_output.records;
    records.extend(finish_compaction_core_writer(core_writer)?);
    let seg_info = finalize_compaction_segment(
        tmp_dir,
        seg_id,
        node_metas.len() as u64,
        edge_metas.len() as u64,
        records,
    )?;

    Ok((seg_info, component_output.report))
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
    prune_policies: &[ResolvedPrunePolicy],
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
include!("schema_management.rs");
include!("query_ir.rs");
include!("query_plan.rs");
include!("projection.rs");
include!("query_exec.rs");
include!("pipeline_ir.rs");
include!("pipeline_exec.rs");
include!("query.rs");

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn property_index_fields(prop_key: &str) -> Vec<SecondaryIndexField> {
        vec![SecondaryIndexField::property(prop_key)]
    }

    fn internal_node_record(
        engine: &DatabaseEngine,
        id: u64,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let (_guard, published) = engine.runtime.published_snapshot()?;
        published.view.get_node(id)
    }

    fn internal_edge_record(
        engine: &DatabaseEngine,
        id: u64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let (_guard, published) = engine.runtime.published_snapshot()?;
        published.view.get_edge(id)
    }

    fn internal_edge_records(
        engine: &DatabaseEngine,
        ids: &[u64],
    ) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let (_guard, published) = engine.runtime.published_snapshot()?;
        published.view.get_edges(ids)
    }

    fn seed_internal_wal_op_tokens(
        engine: &DatabaseEngine,
        ops: &[WalOp],
    ) -> Result<(), EngineError> {
        let node_label_for_label_id = |label_id| match label_id {
            1 => "Person".to_string(),
            2 => "Company".to_string(),
            3 => "Article".to_string(),
            4 => "Topic".to_string(),
            5 => "City".to_string(),
            6 => "Project".to_string(),
            7 => "Account".to_string(),
            8 => "Team".to_string(),
            9 => "User".to_string(),
            10 => "Document".to_string(),
            20 => "Group".to_string(),
            90 => "Metric".to_string(),
            99 => "MissingLabel".to_string(),
            110 => "SearchNode110".to_string(),
            117 => "SearchNode117".to_string(),
            120 => "SearchNode120".to_string(),
            831 => "SpecialNode831".to_string(),
            999 => "SpecialNode999".to_string(),
            1024 => "SpecialNode1024".to_string(),
            _ => format!("NodeLabel{label_id}"),
        };
        let edge_label_for_label_id = |label_id| match label_id {
            1 => "RELATES_TO".to_string(),
            2 => "WORKS_AT".to_string(),
            3 => "LIKES".to_string(),
            4 => "MENTIONS".to_string(),
            5 => "OWNS".to_string(),
            6 => "FOLLOWS".to_string(),
            7 => "FRIENDS_WITH".to_string(),
            8 => "COLLABORATES_WITH".to_string(),
            9 => "RELATED_TO".to_string(),
            10 => "KNOWS".to_string(),
            11 => "BLOCKS".to_string(),
            12 => "DEPENDS_ON".to_string(),
            13 => "ASSIGNED_TO".to_string(),
            14 => "REVIEWED_BY".to_string(),
            15 => "PUBLISHED_BY".to_string(),
            16 => "TAGGED_WITH".to_string(),
            20 => "REPORTS_TO".to_string(),
            30 => "RATES".to_string(),
            40 => "REFERENCES".to_string(),
            99 => "MISSING_EDGE_LABEL".to_string(),
            831 => "SPECIAL_EDGE_831".to_string(),
            999 => "SPECIAL_EDGE_999".to_string(),
            1024 => "SPECIAL_EDGE_1024".to_string(),
            _ => format!("EDGE_LABEL_{label_id}"),
        };
        engine.with_core_mut(|core| {
            let mut dirty = false;
            {
                let mut catalog = core.label_catalog.write().unwrap();
                for op in ops {
                    match op {
                        WalOp::UpsertNode(node) => {
                            for &label_id in node.label_ids.as_slice() {
                                let label = node_label_for_label_id(label_id);
                                if !catalog.node_id_to_label.contains_key(&label_id) {
                                    catalog.apply_node_label(label, label_id, None)?;
                                    dirty = true;
                                }
                            }
                        }
                        WalOp::UpsertEdge(edge) => {
                            let label = edge_label_for_label_id(edge.label_id);
                            if !catalog.edge_id_to_label.contains_key(&edge.label_id) {
                                catalog.apply_edge_label(label, edge.label_id, None)?;
                                dirty = true;
                            }
                        }
                        WalOp::EnsureNodeLabel { .. }
                        | WalOp::EnsureEdgeLabel { .. }
                        | WalOp::DeleteNode { .. }
                        | WalOp::DeleteEdge { .. }
                        | WalOp::BeginAtomicBatch { .. }
                        | WalOp::CommitAtomicBatch { .. } => {}
                    }
                }
                if dirty {
                    catalog.apply_to_manifest(&mut core.manifest);
                }
            }
            if dirty {
                write_manifest(&core.db_dir, &core.manifest)?;
            }
            Ok(())
        })
    }

    fn seed_internal_node_labels(
        engine: &DatabaseEngine,
        label_ids: &[u32],
    ) -> Result<(), EngineError> {
        let node_label_for_label_id = |label_id| match label_id {
            1 => "Person".to_string(),
            2 => "Company".to_string(),
            3 => "Article".to_string(),
            4 => "Topic".to_string(),
            5 => "City".to_string(),
            6 => "Project".to_string(),
            7 => "Account".to_string(),
            8 => "Team".to_string(),
            9 => "User".to_string(),
            10 => "Document".to_string(),
            20 => "Group".to_string(),
            90 => "Metric".to_string(),
            99 => "MissingLabel".to_string(),
            110 => "SearchNode110".to_string(),
            117 => "SearchNode117".to_string(),
            120 => "SearchNode120".to_string(),
            831 => "SpecialNode831".to_string(),
            999 => "SpecialNode999".to_string(),
            1024 => "SpecialNode1024".to_string(),
            _ => format!("NodeLabel{label_id}"),
        };
        engine.with_core_mut(|core| {
            let mut dirty = false;
            {
                let mut catalog = core.label_catalog.write().unwrap();
                for &label_id in label_ids {
                    if !catalog.node_id_to_label.contains_key(&label_id) {
                        catalog.apply_node_label(
                            node_label_for_label_id(label_id),
                            label_id,
                            None,
                        )?;
                        dirty = true;
                    }
                }
                if dirty {
                    catalog.apply_to_manifest(&mut core.manifest);
                }
            }
            if dirty {
                write_manifest(&core.db_dir, &core.manifest)?;
            }
            Ok(())
        })
    }

    fn write_internal_wal_op(engine: &DatabaseEngine, op: &WalOp) -> Result<(), EngineError> {
        seed_internal_wal_op_tokens(engine, std::slice::from_ref(op))?;
        engine.write_op(op)
    }

    fn write_internal_wal_op_batch(
        engine: &DatabaseEngine,
        ops: &[WalOp],
    ) -> Result<(), EngineError> {
        seed_internal_wal_op_tokens(engine, ops)?;
        engine.write_op_batch(ops)
    }

    fn make_node(id: u64, key: &str) -> NodeRecord {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(1).unwrap(),
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
            label_id: 10,
            props: BTreeMap::new(),
            created_at: 2000 * id as i64,
            updated_at: 2000 * id as i64 + 1,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    fn schema_manifest_for_open_tests() -> ManifestState {
        let mut manifest = default_manifest();
        manifest.node_label_tokens.insert("Person".to_string(), 1);
        manifest.node_label_tokens.insert("Team".to_string(), 2);
        manifest.next_node_label_id = 3;
        manifest.edge_label_tokens.insert("KNOWS".to_string(), 1);
        manifest.next_edge_label_id = 2;
        manifest
    }

    fn open_test_node_schema(schema_id: u64, label_id: u32) -> NodeSchemaManifestEntry {
        NodeSchemaManifestEntry {
            schema_id,
            revision: 1,
            label_id,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            key: None,
            label_constraints: None,
            weight: None,
            dense_vector: None,
            sparse_vector: None,
        }
    }

    fn open_test_edge_schema(schema_id: u64, label_id: u32) -> EdgeSchemaManifestEntry {
        EdgeSchemaManifestEntry {
            schema_id,
            revision: 1,
            label_id,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            from: None,
            to: None,
            allow_self_loops: true,
            weight: None,
            validity: None,
        }
    }

    #[test]
    fn open_old_manifest_missing_schema_fields_writes_normalized_fields() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 1,
  "next_edge_id": 1,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": [],
  "secondary_indexes": [],
  "next_secondary_index_id": 1
}"#;
        std::fs::write(dir.path().join("manifest.current"), legacy_json).unwrap();

        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        engine
            .with_core_ref(|core| {
                assert!(core.runtime_schema_catalog.is_empty());
                Ok(())
            })
            .unwrap();
        engine.close().unwrap();

        let raw_manifest = std::fs::read_to_string(dir.path().join("manifest.current")).unwrap();
        assert!(raw_manifest.contains("\"schema_catalog_version\": 1"));
        assert!(raw_manifest.contains("\"next_schema_id\": 1"));
        assert!(raw_manifest.contains("\"node_schemas\": []"));
        assert!(raw_manifest.contains("\"edge_schemas\": []"));
    }

    #[test]
    fn open_valid_schema_manifest_compiles_runtime_catalog() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        let mut manifest = schema_manifest_for_open_tests();
        let mut node_schema = open_test_node_schema(1, 1);
        node_schema.label_constraints = Some(NodeLabelConstraintManifestRule {
            all_of: vec![2],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        let mut edge_schema = open_test_edge_schema(2, 1);
        edge_schema.from = Some(EndpointLabelManifestRule {
            all_of: vec![1],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        manifest.node_schemas.push(node_schema);
        manifest.edge_schemas.push(edge_schema);
        manifest.next_schema_id = 3;
        write_manifest(dir.path(), &manifest).unwrap();

        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        engine
            .with_core_ref(|core| {
                let catalog = &core.runtime_schema_catalog;
                assert!(!catalog.is_empty());
                assert!(catalog.has_node_schemas);
                assert!(catalog.has_edge_schemas);
                assert!(catalog.has_node_label_constraints);
                assert!(catalog.has_edge_endpoint_constraints);
                assert!(catalog.node_has_applicable_schema(
                    &NodeLabelSet::from_canonical_ids(&[1, 2]).unwrap()
                ));
                assert!(catalog.edge_has_applicable_schema(1));
                assert!(catalog.endpoint_relevant_node_label_ids.contains(&1));
                Ok(())
            })
            .unwrap();
        engine.close().unwrap();
    }

    #[test]
    fn open_invalid_schema_manifest_fails_with_manifest_error() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        let mut manifest = schema_manifest_for_open_tests();
        manifest.node_schemas.push(open_test_node_schema(1, 1));
        manifest.node_schemas.push(open_test_node_schema(2, 1));
        manifest.next_schema_id = 3;
        write_manifest(dir.path(), &manifest).unwrap();

        let error = match DatabaseEngine::open(dir.path(), &DbOptions::default()) {
            Ok(_) => panic!("invalid schema manifest unexpectedly opened"),
            Err(error) => error,
        };
        assert!(matches!(error, EngineError::ManifestError(_)));
        assert!(error
            .to_string()
            .contains("duplicate node schema target label_id 1"));
    }

    fn is_atomic_batch_marker(op: &WalOp) -> bool {
        matches!(
            op,
            WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. }
        )
    }

    fn marker_free_wal_records(records: &[(u64, WalOp)]) -> Vec<(u64, WalOp)> {
        records
            .iter()
            .filter(|(_, op)| !is_atomic_batch_marker(op))
            .cloned()
            .collect()
    }

    fn assert_no_atomic_batch_markers(records: &[(u64, WalOp)]) {
        assert!(
            records.iter().all(|(_, op)| !is_atomic_batch_marker(op)),
            "expected marker-free WAL records, got {records:?}"
        );
    }

    fn split_top_level_args(call_args: &str) -> Vec<&str> {
        let mut args = Vec::new();
        let mut start = 0usize;
        let mut paren_depth = 0i32;
        let mut bracket_depth = 0i32;
        let mut brace_depth = 0i32;
        let mut in_string = false;
        let mut escaped = false;

        for (idx, ch) in call_args.char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }

            match ch {
                '"' => in_string = true,
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                '[' => bracket_depth += 1,
                ']' => bracket_depth -= 1,
                '{' => brace_depth += 1,
                '}' => brace_depth -= 1,
                ',' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                    args.push(call_args[start..idx].trim());
                    start = idx + ch.len_utf8();
                }
                _ => {}
            }
        }
        args.push(call_args[start..].trim());
        args
    }

    fn call_args_after<'a>(source: &'a str, needle: &str) -> Vec<&'a str> {
        let mut calls = Vec::new();
        let mut offset = 0usize;
        while let Some(pos) = source[offset..].find(needle) {
            let needle_start = offset + pos;
            let after_needle = needle_start + needle.len();
            let Some(open_rel) = source[after_needle..].find('(') else {
                break;
            };
            let open = after_needle + open_rel;
            if !source[after_needle..open].trim().is_empty() {
                offset = after_needle;
                continue;
            }

            let mut depth = 0i32;
            let mut in_string = false;
            let mut escaped = false;
            for (rel, ch) in source[open..].char_indices() {
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == '"' {
                        in_string = false;
                    }
                    continue;
                }

                match ch {
                    '"' => in_string = true,
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            let close = open + rel;
                            calls.push(&source[open + 1..close]);
                            offset = close + ch.len_utf8();
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if offset <= needle_start {
                offset = after_needle;
            }
        }
        calls
    }

    fn arg_starts_with_number(arg: &str) -> bool {
        arg.trim_start()
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_digit())
    }

    #[test]
    fn engine_tests_do_not_use_legacy_numeric_public_adapter_patterns() {
        let adapter_forbidden = [
            concat!("struct DatabaseEngine", "(super::DatabaseEngine)"),
            concat!("legacy", "_node_label"),
            concat!("legacy", "_edge", "_", "type"),
            concat!("seed", "_legacy_numeric_test_label_tokens"),
            concat!("test_node", "_label("),
            concat!("test_edge", "_", "type("),
            concat!("test_node", "_label_names("),
            concat!("test_edge", "_", "type_names("),
            concat!("test_node", "_label_option("),
            concat!("test_node", "_key_query("),
            concat!("test_node", "_key_queries("),
        ];
        let included_test_forbidden = [
            adapter_forbidden.as_slice(),
            &[
                concat!("node_type", "_filter:"),
                concat!("get_nodes_by_keys", "(&[("),
            ],
        ]
        .concat();

        let engine_mod_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/mod.rs");
        let test_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine/tests");
        let files = std::fs::read_dir(&test_dir).unwrap();
        let mut violations = Vec::new();

        let engine_mod_source = std::fs::read_to_string(&engine_mod_path).unwrap();
        let engine_mod_display = engine_mod_path
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap_or(&engine_mod_path);
        for forbidden in adapter_forbidden {
            if engine_mod_source.contains(forbidden) {
                violations.push(format!(
                    "{} contains `{}`",
                    engine_mod_display.display(),
                    forbidden
                ));
            }
        }

        for file in files {
            let path = file.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).unwrap();
            let display = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .unwrap_or(&path);

            for forbidden in &included_test_forbidden {
                if source.contains(*forbidden) {
                    violations.push(format!("{} contains `{}`", display.display(), forbidden));
                }
            }

            for (method, arg_index) in [
                ("upsert_node", 0usize),
                ("ensure_node_property_index", 0),
                ("ensure_edge_property_index", 0),
                ("get_node_by_key", 0),
                ("upsert_edge", 2),
                ("get_edge_by_triple", 2),
            ] {
                for call_args in call_args_after(&source, method) {
                    let args = split_top_level_args(call_args);
                    if args
                        .get(arg_index)
                        .is_some_and(|arg| arg_starts_with_number(arg))
                    {
                        violations.push(format!(
                            "{} has numeric public `{}` argument in `{}`",
                            display.display(),
                            method,
                            call_args.lines().next().unwrap_or(call_args).trim()
                        ));
                    }
                }
            }

            for (line_no, line) in source.lines().enumerate() {
                if line.contains("label_filter_ids:") && !line.contains("edge_label_filter:") {
                    violations.push(format!(
                        "{}:{} contains legacy `{}`",
                        display.display(),
                        line_no + 1,
                        line.trim()
                    ));
                }
            }
        }

        assert!(
            violations.is_empty(),
            "legacy numeric public API patterns remain:\n{}",
            violations.join("\n")
        );
    }

    include!("tests/label_catalog.rs");
    include!("tests/wal_atomic.rs");
    include!("tests/lifecycle.rs");
    include!("tests/txn.rs");
    include!("tests/write.rs");
    include!("tests/schema_management.rs");
    include!("tests/read.rs");
    include!("tests/graph_ops.rs");
    include!("tests/graph_rows.rs");
    include!("tests/query_planner.rs");
    include!("tests/projection.rs");
    include!("tests/gql_execution.rs");
}
