#[cfg(feature = "production-concurrency")]
mod graph;
#[cfg(feature = "production-concurrency")]
mod id_history;
#[cfg(feature = "production-concurrency")]
mod id_radix;
pub(crate) mod secondary;
#[cfg(feature = "production-concurrency")]
use graph::physical_direction;
pub(crate) mod primary;
use crate::edge_metadata::EdgeMetadataCandidate;
#[cfg(test)]
use crate::edge_metadata::{i64_matches_bounds, weight_matches_bounds, RangeBoundFlags};
use crate::error::EngineError;
use crate::property_value_semantics::{
    hash_prop_equality_key, numeric_range_sort_key_for_value, semantic_property_eq,
    NumericRangeSortKey,
};
use crate::row_projection::{EdgeSelectedFieldNeeds, NodeSelectedFieldNeeds, PropertySelection};
use crate::secondary_index_key::{
    encode_compound_tuple_key, CompoundFieldValue, CompoundLowerBound, CompoundPrefixBounds,
    CompoundRangeBounds, CompoundSidecarTargetKind, CompoundTupleContext,
};
use crate::types::*;
use primary::PrimaryRead;
#[cfg(feature = "production-concurrency")]
use primary::{PrimaryBinding, PrimaryMaps};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Bound, ControlFlow};
use std::sync::RwLock;

// Private vector transport: metadata/presence always survive source shadowing;
// only explicitly selected vectors are copied. Legacy callbacks keep borrowed data.
// Flat vector payloads have ordinary stack-safe destruction.
#[derive(Debug)]
pub(crate) struct NodeVectorRow<'a> {
    meta: NodeMetadataForQuery,
    pub(crate) has_dense: bool,
    pub(crate) has_sparse: bool,
    pub(crate) dense_vector: Option<std::borrow::Cow<'a, [f32]>>,
    pub(crate) sparse_vector: Option<std::borrow::Cow<'a, [(u32, f32)]>>,
}
impl std::ops::Deref for NodeVectorRow<'_> {
    type Target = NodeMetadataForQuery;
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}
impl<'a> NodeVectorRow<'a> {
    fn borrowed(node: &'a NodeRecord, vectors: crate::row_projection::VectorSelection) -> Self {
        Self {
            meta: NodeMetadataForQuery {
                id: node.id,
                label_ids: node.label_ids,
                updated_at: node.updated_at,
                weight: node.weight,
            },
            has_dense: node.dense_vector.is_some(),
            has_sparse: node.sparse_vector.is_some(),
            dense_vector: if vectors.needs_dense() {
                node.dense_vector.as_deref().map(std::borrow::Cow::Borrowed)
            } else {
                None
            },
            sparse_vector: if vectors.needs_sparse() {
                node.sparse_vector
                    .as_deref()
                    .map(std::borrow::Cow::Borrowed)
            } else {
                None
            },
        }
    }
    #[cfg(feature = "production-concurrency")]
    fn into_owned(self) -> NodeVectorRow<'static> {
        NodeVectorRow {
            meta: self.meta,
            has_dense: self.has_dense,
            has_sparse: self.has_sparse,
            dense_vector: self
                .dense_vector
                .map(|v| std::borrow::Cow::Owned(v.into_owned())),
            sparse_vector: self
                .sparse_vector
                .map(|v| std::borrow::Cow::Owned(v.into_owned())),
        }
    }
    #[cfg(feature = "production-concurrency")]
    fn copied_bytes(node: &NodeRecord, vectors: crate::row_projection::VectorSelection) -> usize {
        std::mem::size_of::<Self>().saturating_add(Self::vector_bytes(node, vectors))
    }
    #[cfg(feature = "production-concurrency")]
    fn vector_bytes(node: &NodeRecord, vectors: crate::row_projection::VectorSelection) -> usize {
        let dense = if vectors.needs_dense() {
            node.dense_vector
                .as_ref()
                .map_or(0, |v| v.len().saturating_mul(4))
        } else {
            0
        };
        let sparse = if vectors.needs_sparse() {
            node.sparse_vector
                .as_ref()
                .map_or(0, |v| v.len().saturating_mul(8))
        } else {
            0
        };
        dense.saturating_add(sparse)
    }
}

#[cfg(feature = "production-concurrency")]
struct VectorBatchRow {
    meta: NodeMetadataForQuery,
    has_dense: bool,
    has_sparse: bool,
    dense: Option<std::ops::Range<usize>>,
    sparse: Option<std::ops::Range<usize>>,
}

/// Bounded owned transport. Ranges survive backing growth; borrowed slices are
/// created only after the participant session ends, for one callback at a time.
#[cfg(feature = "production-concurrency")]
#[derive(Default)]
struct VectorBatch {
    rows: Vec<VectorBatchRow>,
    dense: DenseVector,
    sparse: SparseVector,
}
#[cfg(feature = "production-concurrency")]
impl VectorBatch {
    fn clear(&mut self) {
        self.rows.clear();
        self.dense.clear();
        self.sparse.clear();
    }

    fn copied_bytes(node: &NodeRecord, vectors: crate::row_projection::VectorSelection) -> usize {
        std::mem::size_of::<VectorBatchRow>()
            .saturating_add(NodeVectorRow::vector_bytes(node, vectors))
    }

    fn push(
        &mut self,
        node: &NodeRecord,
        vectors: crate::row_projection::VectorSelection,
        remaining: usize,
    ) {
        if self.rows.capacity() == 0 {
            let row_bytes = Self::copied_bytes(node, vectors).max(1);
            let rows = remaining.min(4096).min((1024 * 1024 / row_bytes).max(1));
            self.rows.reserve_exact(rows);
        }
        fn append<T: Copy>(
            buffer: &mut Vec<T>,
            value: Option<&[T]>,
            rows: usize,
        ) -> Option<std::ops::Range<usize>> {
            let value = value?;
            if buffer.capacity() == 0 && !value.is_empty() {
                let target = value
                    .len()
                    .saturating_mul(rows)
                    .min(1024 * 1024 / std::mem::size_of::<T>())
                    .max(value.len());
                buffer.reserve_exact(target);
            }
            let start = buffer.len();
            buffer.extend_from_slice(value);
            Some(start..buffer.len())
        }
        let dense = append(
            &mut self.dense,
            if vectors.needs_dense() {
                node.dense_vector.as_deref()
            } else {
                None
            },
            self.rows.capacity(),
        );
        let sparse = append(
            &mut self.sparse,
            if vectors.needs_sparse() {
                node.sparse_vector.as_deref()
            } else {
                None
            },
            self.rows.capacity(),
        );
        self.rows.push(VectorBatchRow {
            meta: NodeMetadataForQuery {
                id: node.id,
                label_ids: node.label_ids,
                updated_at: node.updated_at,
                weight: node.weight,
            },
            has_dense: node.dense_vector.is_some(),
            has_sparse: node.sparse_vector.is_some(),
            dense,
            sparse,
        });
    }

    fn row(&self, row: &VectorBatchRow) -> NodeVectorRow<'_> {
        NodeVectorRow {
            meta: row.meta,
            has_dense: row.has_dense,
            has_sparse: row.has_sparse,
            dense_vector: row
                .dense
                .clone()
                .map(|r| std::borrow::Cow::Borrowed(&self.dense[r])),
            sparse_vector: row
                .sparse
                .clone()
                .map(|r| std::borrow::Cow::Borrowed(&self.sparse[r])),
        }
    }
}

#[cfg(test)]
thread_local! {static ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST:Cell<usize>=const {Cell::new(0)};}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryScanWork {
    pub(crate) edge_count_refills: usize,
    pub(crate) edge_transport_records: usize,
    pub(crate) node_id_refills: usize,
    pub(crate) node_id_copies: usize,
    pub(crate) full_node_count_calls: usize,
    pub(crate) full_node_count_opinions: usize,
}

#[cfg(test)]
thread_local! {
    static QUERY_SCAN_WORK: Cell<QueryScanWork> = Cell::new(QueryScanWork::default());
}

#[cfg(test)]
pub(crate) fn take_query_scan_work_for_test() -> QueryScanWork {
    QUERY_SCAN_WORK.with(|work| work.replace(QueryScanWork::default()))
}

#[cfg(test)]
fn note_query_scan_work(update: impl FnOnce(&mut QueryScanWork)) {
    QUERY_SCAN_WORK.with(|work| {
        let mut value = work.get();
        update(&mut value);
        work.set(value);
    });
}

#[cfg(test)]
thread_local! {
    static VERSIONED_SLOT_LOOKUP_COMPARISONS_FOR_TEST: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_versioned_slot_lookup_comparisons_for_test() {
    VERSIONED_SLOT_LOOKUP_COMPARISONS_FOR_TEST.with(|count| count.set(0));
}

#[cfg(test)]
fn versioned_slot_lookup_comparisons_for_test() -> usize {
    VERSIONED_SLOT_LOOKUP_COMPARISONS_FOR_TEST.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_endpoint_cursor_entries_visited_for_test() {
    ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn endpoint_cursor_entries_visited_for_test() -> usize {
    ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.with(Cell::get)
}

fn selected_props_from_map(
    props: &BTreeMap<String, PropValue>,
    selection: &PropertySelection,
) -> BTreeMap<String, PropValue> {
    match selection {
        PropertySelection::None => BTreeMap::new(),
        PropertySelection::Keys(keys) => {
            let mut selected = BTreeMap::new();
            for key in keys {
                if let Some(value) = props.get(key) {
                    selected.insert(key.clone(), value.clone());
                }
            }
            selected
        }
        PropertySelection::All => props.clone(),
    }
}

/// An adjacency entry: one edge connecting to a neighbor.
#[derive(Debug, Clone)]
pub struct AdjEntry {
    pub edge_id: u64,
    pub label_id: u32,
    pub neighbor_id: u64,
    pub weight: f32,
    pub valid_from: i64,
    pub valid_to: i64,
}

#[derive(Debug, Clone)]
struct SlotVersion<T> {
    write_seq: u64,
    value: T,
}

#[derive(Debug, Clone)]
struct VersionedSlot<T> {
    head: SlotVersion<T>,
    history: Option<Vec<SlotVersion<T>>>,
}

impl<T: Clone> VersionedSlot<T> {
    fn new(write_seq: u64, value: T) -> Self {
        Self {
            head: SlotVersion { write_seq, value },
            history: None,
        }
    }

    fn replace(&mut self, write_seq: u64, value: T) {
        assert!(
            write_seq >= self.head.write_seq,
            "versioned slot write sequence must be monotonic"
        );
        if self.head.write_seq == write_seq {
            self.head.value = value;
            return;
        }
        let previous = std::mem::replace(&mut self.head, SlotVersion { write_seq, value });
        self.history.get_or_insert_with(Vec::new).push(previous);
    }

    fn current(&self) -> &T {
        &self.head.value
    }

    fn at(&self, snapshot_seq: u64) -> Option<&T> {
        if self.head.write_seq <= snapshot_seq {
            return Some(&self.head.value);
        }
        let history = self.history.as_ref()?;
        let mut low = 0usize;
        let mut high = history.len();
        while low < high {
            let mid = low + (high - low) / 2;
            #[cfg(test)]
            VERSIONED_SLOT_LOOKUP_COMPARISONS_FOR_TEST.with(|count| count.set(count.get() + 1));
            if history[mid].write_seq <= snapshot_seq {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low.checked_sub(1)
            .and_then(|index| history.get(index))
            .map(|version| &version.value)
    }
}

#[derive(Debug, Clone)]
enum RecordState<T> {
    Live(T),
    Tombstone(TombstoneEntry),
}

type VersionedRecordSlot<T> = VersionedSlot<RecordState<T>>;
type LookupSlot<V> = VersionedSlot<Option<V>>;
type MembershipSlot<T> = VersionedSlot<Option<T>>;
type SecondaryEqMemberState = HashMap<u64, NodeIdMap<MembershipSlot<()>>>;
type SecondaryEqState = HashMap<u64, SecondaryEqMemberState>;
type SecondaryRangeState = HashMap<u64, BTreeMap<(NumericRangeSortKey, u64), MembershipSlot<()>>>;
type CompoundSecondaryIndexState = HashMap<u64, BTreeMap<(Vec<u8>, u64), MembershipSlot<()>>>;
type CompoundAction = (u64, Vec<u8>, u64, bool);

#[derive(Debug, Clone)]
struct OrderedAdjacencyGroupState {
    current_count: u64,
    membership: MembershipSlot<()>,
}

#[derive(Debug)]
pub(crate) struct MemtableAdjacencyOwnerRefill {
    pub(crate) owner_head: Option<u64>,
    pub(crate) last_group_key_examined: Option<(u64, u32)>,
    pub(crate) examined: usize,
    pub(crate) exhausted: bool,
}

#[derive(Debug)]
pub(crate) struct MemtableAdjacencyPostingRefill {
    pub(crate) entries: Vec<AdjEntry>,
    pub(crate) last_edge_id_examined: Option<u64>,
    pub(crate) examined: usize,
    pub(crate) exhausted: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemtableEndpointCountEstimate {
    pub(crate) count: usize,
    pub(crate) exact: bool,
}

impl MemtableEndpointCountEstimate {
    fn exact_zero() -> Self {
        Self {
            count: 0,
            exact: true,
        }
    }

    fn add_assign(&mut self, other: Self) {
        self.count = self.count.saturating_add(other.count);
        self.exact &= other.exact;
    }
}

fn apply_size_delta(total: &mut usize, before: usize, after: usize) {
    if after >= before {
        *total += after - before;
    } else {
        *total -= before - after;
    }
}

fn estimate_node_record_slot(slot: &VersionedRecordSlot<NodeRecord>) -> usize {
    Memtable::estimate_slot(slot, |state| match state {
        RecordState::Live(node) => Memtable::estimate_node_record(node),
        RecordState::Tombstone(_) => 16,
    })
}

fn estimate_edge_record_slot(slot: &VersionedRecordSlot<EdgeRecord>) -> usize {
    Memtable::estimate_slot(slot, |state| match state {
        RecordState::Live(edge) => Memtable::estimate_edge_record(edge),
        RecordState::Tombstone(_) => 16,
    })
}

fn estimate_lookup_slot(slot: &LookupSlot<u64>) -> usize {
    Memtable::estimate_slot(slot, |_| 16)
}

fn estimate_membership_slot(slot: &MembershipSlot<()>) -> usize {
    Memtable::estimate_slot(slot, |_| 8)
}

fn estimate_adj_slot(slot: &MembershipSlot<AdjEntry>) -> usize {
    Memtable::estimate_slot(slot, |_| 48)
}

fn estimate_ordered_adj_group_state(state: &OrderedAdjacencyGroupState) -> usize {
    8 + 4 + 8 + estimate_membership_slot(&state.membership)
}

fn estimate_secondary_decl_entry(entry: &SecondaryIndexManifestEntry) -> usize {
    let field_bytes = match &entry.target {
        SecondaryIndexTarget::NodeProperty { prop_key, .. }
        | SecondaryIndexTarget::EdgeProperty { prop_key, .. } => prop_key.len(),
        SecondaryIndexTarget::NodeFieldIndex { fields, .. }
        | SecondaryIndexTarget::EdgeFieldIndex { fields, .. } => fields
            .iter()
            .map(|field| match field {
                SecondaryIndexFieldManifest::Property { key } => key.len(),
                SecondaryIndexFieldManifest::NodeMetadata { .. }
                | SecondaryIndexFieldManifest::EdgeMetadata { .. } => 16,
            })
            .sum(),
    };
    96 + field_bytes + entry.last_error.as_ref().map(|msg| msg.len()).unwrap_or(0)
}

fn estimate_secondary_eq_lookup_entry(prop_key: &str, index_ids: &[u64]) -> usize {
    48 + prop_key.len() + index_ids.len() * 8
}

fn estimate_secondary_range_lookup_entry(prop_key: &str, indexes: &[u64]) -> usize {
    48 + prop_key.len() + indexes.len() * 8
}

fn estimate_secondary_field_lookup_entry(index_ids: &[u64]) -> usize {
    48 + index_ids.len() * 8
}

fn estimate_secondary_eq_state_groups(groups: &SecondaryEqMemberState) -> usize {
    groups
        .values()
        .map(|members| {
            members
                .values()
                .map(estimate_membership_slot)
                .sum::<usize>()
        })
        .sum()
}

fn estimate_secondary_range_state_entries(
    entries: &BTreeMap<(NumericRangeSortKey, u64), MembershipSlot<()>>,
) -> usize {
    entries.values().map(estimate_membership_slot).sum()
}

fn estimate_compound_state_entries(
    entries: &BTreeMap<(Vec<u8>, u64), MembershipSlot<()>>,
) -> usize {
    entries
        .iter()
        .map(|((tuple_key, _), slot)| tuple_key.len() + 8 + estimate_membership_slot(slot))
        .sum()
}

#[derive(Clone, Default)]
struct MemtableState {
    #[cfg(feature = "production-concurrency")]
    sealed_export: Option<crate::segment_writer::SealedExport>,
    estimated_bytes: usize,
    live_node_count: usize,
    live_edge_count: usize,
    // Candidate-only current diagnostics; never historical read authority.
    #[cfg(feature = "production-concurrency")]
    candidate_tombstone_counts: (usize, usize),
    nodes: NodeIdMap<VersionedRecordSlot<NodeRecord>>,
    edges: NodeIdMap<VersionedRecordSlot<EdgeRecord>>,
    node_tombstones: NodeIdMap<MembershipSlot<()>>,
    edge_tombstones: NodeIdMap<MembershipSlot<()>>,
    node_key_index: HashMap<u32, HashMap<String, LookupSlot<u64>>>,
    edge_triple_index: HashMap<(u64, u64, u32), LookupSlot<u64>>,
    adj_out: NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
    adj_in: NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
    ordered_edge_ids: BTreeMap<u64, MembershipSlot<()>>,
    label_node_index: BTreeMap<(u32, u64), MembershipSlot<()>>,
    ordered_label_edge_index: BTreeMap<(u32, u64), MembershipSlot<()>>,
    ordered_adj_out: NodeIdMap<BTreeMap<u64, MembershipSlot<AdjEntry>>>,
    ordered_adj_in: NodeIdMap<BTreeMap<u64, MembershipSlot<AdjEntry>>>,
    ordered_adj_out_groups: BTreeMap<(u64, u32), OrderedAdjacencyGroupState>,
    ordered_adj_in_groups: BTreeMap<(u64, u32), OrderedAdjacencyGroupState>,
    retained_adj_out_posting_slots: u64,
    retained_adj_in_posting_slots: u64,
    label_edge_index: HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
    time_node_index: BTreeMap<(u32, i64, u64), MembershipSlot<()>>,
    catalog: secondary::SecondaryCatalog,
    secondary_eq_state: SecondaryEqState,
    secondary_range_state: SecondaryRangeState,
    secondary_compound_state: CompoundSecondaryIndexState,
}

// Only candidate facades share diagnostics. Default memtables retain their
// original direct lock and allocation/indirection behavior.
#[cfg(feature = "production-concurrency")]
type MemtableStateOwner = std::sync::Arc<RwLock<MemtableState>>;
#[cfg(not(feature = "production-concurrency"))]
type MemtableStateOwner = RwLock<MemtableState>;
fn new_memtable_state(value: MemtableState) -> MemtableStateOwner {
    #[cfg(feature = "production-concurrency")]
    {
        std::sync::Arc::new(RwLock::new(value))
    }
    #[cfg(not(feature = "production-concurrency"))]
    {
        RwLock::new(value)
    }
}

fn slot_option_current<T: Clone>(slot: &VersionedSlot<Option<T>>) -> Option<&T> {
    slot.current().as_ref()
}

fn slot_option_at<T: Clone>(slot: &VersionedSlot<Option<T>>, snapshot_seq: u64) -> Option<&T> {
    slot.at(snapshot_seq)?.as_ref()
}

fn slot_option_visible<T: Clone>(slot: &VersionedSlot<Option<T>>, snapshot_seq: u64) -> bool {
    slot.at(snapshot_seq).is_some_and(Option::is_some)
}

fn optional_semantic_property_eq(left: Option<&PropValue>, right: Option<&PropValue>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => semantic_property_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn node_compound_field_value<'a>(
    node: &'a NodeRecord,
    field: &'a SecondaryIndexFieldManifest,
) -> CompoundFieldValue<'a> {
    match field {
        SecondaryIndexFieldManifest::Property { key } => {
            CompoundFieldValue::Property(node.props.get(key))
        }
        SecondaryIndexFieldManifest::NodeMetadata { field } => match field {
            NodeMetadataIndexFieldManifest::Id => CompoundFieldValue::MetadataU64(node.id),
            NodeMetadataIndexFieldManifest::Key => CompoundFieldValue::MetadataString(&node.key),
            NodeMetadataIndexFieldManifest::Weight => {
                CompoundFieldValue::MetadataF64(node.weight as f64)
            }
            NodeMetadataIndexFieldManifest::CreatedAt => {
                CompoundFieldValue::MetadataI64(node.created_at)
            }
            NodeMetadataIndexFieldManifest::UpdatedAt => {
                CompoundFieldValue::MetadataI64(node.updated_at)
            }
        },
        SecondaryIndexFieldManifest::EdgeMetadata { .. } => {
            unreachable!("node field-index declaration contains edge metadata")
        }
    }
}

fn edge_compound_field_value<'a>(
    edge: &'a EdgeRecord,
    field: &'a SecondaryIndexFieldManifest,
) -> CompoundFieldValue<'a> {
    match field {
        SecondaryIndexFieldManifest::Property { key } => {
            CompoundFieldValue::Property(edge.props.get(key))
        }
        SecondaryIndexFieldManifest::EdgeMetadata { field } => match field {
            EdgeMetadataIndexFieldManifest::Id => CompoundFieldValue::MetadataU64(edge.id),
            EdgeMetadataIndexFieldManifest::From => CompoundFieldValue::MetadataU64(edge.from),
            EdgeMetadataIndexFieldManifest::To => CompoundFieldValue::MetadataU64(edge.to),
            EdgeMetadataIndexFieldManifest::Weight => {
                CompoundFieldValue::MetadataF64(edge.weight as f64)
            }
            EdgeMetadataIndexFieldManifest::CreatedAt => {
                CompoundFieldValue::MetadataI64(edge.created_at)
            }
            EdgeMetadataIndexFieldManifest::UpdatedAt => {
                CompoundFieldValue::MetadataI64(edge.updated_at)
            }
            EdgeMetadataIndexFieldManifest::ValidFrom => {
                CompoundFieldValue::MetadataI64(edge.valid_from)
            }
            EdgeMetadataIndexFieldManifest::ValidTo => {
                CompoundFieldValue::MetadataI64(edge.valid_to)
            }
        },
        SecondaryIndexFieldManifest::NodeMetadata { .. } => {
            unreachable!("edge field-index declaration contains node metadata")
        }
    }
}

fn encode_node_compound_tuple_key(
    label_id: u32,
    fields: &[SecondaryIndexFieldManifest],
    node: &NodeRecord,
) -> Vec<u8> {
    let context = CompoundTupleContext {
        target_kind: CompoundSidecarTargetKind::Node,
        target_label_id: label_id,
        fields,
    };
    let values = fields
        .iter()
        .map(|field| node_compound_field_value(node, field))
        .collect::<Vec<_>>();
    encode_compound_tuple_key(&context, &values)
        .expect("validated node field-index declaration must encode")
}

fn encode_edge_compound_tuple_key(
    label_id: u32,
    fields: &[SecondaryIndexFieldManifest],
    edge: &EdgeRecord,
) -> Vec<u8> {
    let context = CompoundTupleContext {
        target_kind: CompoundSidecarTargetKind::Edge,
        target_label_id: label_id,
        fields,
    };
    let values = fields
        .iter()
        .map(|field| edge_compound_field_value(edge, field))
        .collect::<Vec<_>>();
    encode_compound_tuple_key(&context, &values)
        .expect("validated edge field-index declaration must encode")
}

fn secondary_range_start_bound(
    lower: Option<(NumericRangeSortKey, bool)>,
    after: Option<(NumericRangeSortKey, u64)>,
) -> std::ops::Bound<(NumericRangeSortKey, u64)> {
    let mut start = lower.map(|(value, inclusive)| {
        if inclusive {
            ((value, 0), false)
        } else {
            ((value, u64::MAX), true)
        }
    });
    if let Some(cursor) = after {
        let cursor_start = (cursor, true);
        start = Some(match start {
            Some(existing) if existing.0 > cursor_start.0 => existing,
            Some(existing) if existing.0 < cursor_start.0 => cursor_start,
            Some(existing) => (existing.0, existing.1 || cursor_start.1),
            None => cursor_start,
        });
    }

    match start {
        Some((target, true)) => std::ops::Bound::Excluded(target),
        Some((target, false)) => std::ops::Bound::Included(target),
        None => std::ops::Bound::Unbounded,
    }
}

fn secondary_range_past_upper(
    encoded: NumericRangeSortKey,
    upper: Option<(NumericRangeSortKey, bool)>,
) -> bool {
    upper.is_some_and(|(upper_value, inclusive)| {
        encoded > upper_value || (!inclusive && encoded == upper_value)
    })
}

#[allow(dead_code)]
pub(crate) fn compound_start_bound(lower: Option<&CompoundLowerBound>) -> Bound<(Vec<u8>, u64)> {
    match lower {
        Some(lower) => Bound::Included((lower.key.clone(), 0)),
        None => Bound::Unbounded,
    }
}

#[allow(dead_code)]
fn collect_visible_compound_ids(
    entries: &BTreeMap<(Vec<u8>, u64), MembershipSlot<()>>,
    lower: Bound<(Vec<u8>, u64)>,
    upper_exclusive: &[u8],
    skip_exact_lower_component_prefix: Option<&[u8]>,
    snapshot_seq: u64,
) -> Vec<u64> {
    let upper = Bound::Excluded((upper_exclusive.to_vec(), 0));
    let mut ids = Vec::new();
    for ((tuple_key, record_id), slot) in entries.range((lower, upper)) {
        if skip_exact_lower_component_prefix.is_some_and(|prefix| tuple_key.starts_with(prefix)) {
            continue;
        }
        if slot_option_visible(slot, snapshot_seq) {
            ids.push(*record_id);
        }
    }
    ids
}

#[allow(dead_code)]
fn count_visible_compound_ids(
    entries: &BTreeMap<(Vec<u8>, u64), MembershipSlot<()>>,
    lower: Bound<(Vec<u8>, u64)>,
    upper_exclusive: &[u8],
    skip_exact_lower_component_prefix: Option<&[u8]>,
    snapshot_seq: u64,
) -> usize {
    let upper = Bound::Excluded((upper_exclusive.to_vec(), 0));
    entries
        .range((lower, upper))
        .filter(|((tuple_key, _), slot)| {
            !skip_exact_lower_component_prefix.is_some_and(|prefix| tuple_key.starts_with(prefix))
                && slot_option_visible(slot, snapshot_seq)
        })
        .count()
}

fn record_current<T: Clone>(slot: &VersionedRecordSlot<T>) -> Option<&T> {
    match slot.current() {
        RecordState::Live(value) => Some(value),
        RecordState::Tombstone(_) => None,
    }
}

fn record_at<T: Clone>(
    slot: &VersionedRecordSlot<T>,
    snapshot_seq: u64,
) -> Option<&RecordState<T>> {
    slot.at(snapshot_seq)
}

fn current_adj_map(
    source: &NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
) -> NodeIdMap<NodeIdMap<AdjEntry>> {
    let mut result = NodeIdMap::default();
    for (&owner_id, entries) in source {
        let mut visible = NodeIdMap::default();
        for (&member_id, slot) in entries {
            if let Some(entry) = slot_option_current(slot) {
                visible.insert(member_id, entry.clone());
            }
        }
        if !visible.is_empty() {
            result.insert(owner_id, visible);
        }
    }
    result
}

fn current_label_membership_index(
    source: &HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
) -> HashMap<u32, NodeIdSet> {
    let mut result = HashMap::new();
    for (&target_label_id, members) in source {
        let mut visible = NodeIdSet::default();
        for (&member_id, slot) in members {
            if slot_option_current(slot).is_some() {
                visible.insert(member_id);
            }
        }
        if !visible.is_empty() {
            result.insert(target_label_id, visible);
        }
    }
    result
}

fn current_secondary_eq_state(source: &SecondaryEqState) -> HashMap<u64, HashMap<u64, NodeIdSet>> {
    let mut result = HashMap::new();
    for (&index_id, groups) in source {
        let mut visible_groups = HashMap::new();
        for (&value_hash, members) in groups {
            let mut visible = NodeIdSet::default();
            for (&node_id, slot) in members {
                if slot_option_current(slot).is_some() {
                    visible.insert(node_id);
                }
            }
            if !visible.is_empty() {
                visible_groups.insert(value_hash, visible);
            }
        }
        if !visible_groups.is_empty() {
            result.insert(index_id, visible_groups);
        }
    }
    result
}

fn current_secondary_range_state(
    source: &SecondaryRangeState,
) -> HashMap<u64, BTreeSet<(NumericRangeSortKey, u64)>> {
    let mut result = HashMap::new();
    for (&index_id, entries) in source {
        let mut visible = BTreeSet::new();
        for (&key, slot) in entries {
            if slot_option_current(slot).is_some() {
                visible.insert(key);
            }
        }
        if !visible.is_empty() {
            result.insert(index_id, visible);
        }
    }
    result
}

#[cfg(test)]
type CompoundSecondaryTestState = HashMap<u64, BTreeSet<(Vec<u8>, u64)>>;

#[cfg(test)]
fn current_compound_secondary_state(
    source: &CompoundSecondaryIndexState,
) -> CompoundSecondaryTestState {
    let mut result = HashMap::new();
    for (&index_id, entries) in source {
        let mut visible = BTreeSet::new();
        for ((tuple_key, record_id), slot) in entries {
            if slot_option_current(slot).is_some() {
                visible.insert((tuple_key.clone(), *record_id));
            }
        }
        if !visible.is_empty() {
            result.insert(index_id, visible);
        }
    }
    result
}

fn current_compound_secondary_state_for_indexes(
    source: &CompoundSecondaryIndexState,
    index_ids: &[u64],
) -> HashMap<u64, Vec<(Vec<u8>, u64)>> {
    let mut requested = index_ids.to_vec();
    requested.sort_unstable();
    requested.dedup();

    let mut result = HashMap::new();
    for index_id in requested {
        let Some(entries) = source.get(&index_id) else {
            continue;
        };
        let visible: Vec<(Vec<u8>, u64)> = entries
            .iter()
            .filter_map(|((tuple_key, record_id), slot)| {
                slot_option_current(slot).map(|_| (tuple_key.clone(), *record_id))
            })
            .collect();
        if !visible.is_empty() {
            result.insert(index_id, visible);
        }
    }
    result
}

fn current_time_index(
    source: &BTreeMap<(u32, i64, u64), MembershipSlot<()>>,
) -> BTreeSet<(u32, i64, u64)> {
    source
        .iter()
        .filter_map(|(&key, slot)| slot_option_current(slot).map(|_| key))
        .collect()
}

impl MemtableState {
    fn set_node_state(&mut self, id: u64, state: RecordState<NodeRecord>, write_seq: u64) {
        match self.nodes.get_mut(&id) {
            Some(slot) => {
                let before = estimate_node_record_slot(slot);
                slot.replace(write_seq, state);
                let after = estimate_node_record_slot(slot);
                apply_size_delta(&mut self.estimated_bytes, before, after);
            }
            None => {
                let slot = VersionedSlot::new(write_seq, state);
                self.estimated_bytes += estimate_node_record_slot(&slot);
                self.nodes.insert(id, slot);
            }
        }
    }

    fn set_edge_state(&mut self, id: u64, state: RecordState<EdgeRecord>, write_seq: u64) {
        match self.edges.get_mut(&id) {
            Some(slot) => {
                let before = estimate_edge_record_slot(slot);
                slot.replace(write_seq, state);
                let after = estimate_edge_record_slot(slot);
                apply_size_delta(&mut self.estimated_bytes, before, after);
            }
            None => {
                let slot = VersionedSlot::new(write_seq, state);
                self.estimated_bytes += estimate_edge_record_slot(&slot);
                self.edges.insert(id, slot);
            }
        }
    }

    fn set_node_key(&mut self, label_id: u32, key: &str, value: Option<u64>, write_seq: u64) {
        let by_key = self.node_key_index.entry(label_id).or_default();
        if let Some(slot) = by_key.get_mut(key) {
            let before = estimate_lookup_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_lookup_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += key.len() + estimate_lookup_slot(&slot);
            by_key.insert(key.to_string(), slot);
        }
    }

    fn set_edge_triple(
        &mut self,
        from: u64,
        to: u64,
        label_id: u32,
        value: Option<u64>,
        write_seq: u64,
    ) {
        if let Some(slot) = self.edge_triple_index.get_mut(&(from, to, label_id)) {
            let before = estimate_lookup_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_lookup_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_lookup_slot(&slot);
            self.edge_triple_index.insert((from, to, label_id), slot);
        }
    }

    fn set_adj_slot(
        map: &mut NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
        owner_id: u64,
        member_id: u64,
        value: Option<AdjEntry>,
        write_seq: u64,
    ) -> (usize, usize) {
        let members = map.entry(owner_id).or_default();
        if let Some(slot) = members.get_mut(&member_id) {
            let before = estimate_adj_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_adj_slot(slot);
            (before, after)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = estimate_adj_slot(&slot);
            members.insert(member_id, slot);
            (0, after)
        }
    }

    fn set_ordered_adj_slot(
        map: &mut NodeIdMap<BTreeMap<u64, MembershipSlot<AdjEntry>>>,
        owner_id: u64,
        member_id: u64,
        value: Option<AdjEntry>,
        write_seq: u64,
    ) -> (usize, usize, bool) {
        let members = map.entry(owner_id).or_default();
        if let Some(slot) = members.get_mut(&member_id) {
            let before = estimate_adj_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_adj_slot(slot);
            (before, after, false)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = estimate_adj_slot(&slot);
            members.insert(member_id, slot);
            (0, after, true)
        }
    }

    fn change_ordered_adj_group(
        groups: &mut BTreeMap<(u64, u32), OrderedAdjacencyGroupState>,
        key: (u64, u32),
        increment: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        match groups.get_mut(&key) {
            Some(group) => {
                let before = estimate_ordered_adj_group_state(group);
                if increment {
                    let was_zero = group.current_count == 0;
                    group.current_count = group
                        .current_count
                        .checked_add(1)
                        .expect("ordered adjacency group count overflow");
                    if was_zero {
                        group.membership.replace(write_seq, Some(()));
                    }
                } else {
                    group.current_count = group
                        .current_count
                        .checked_sub(1)
                        .expect("ordered adjacency group count underflow");
                    if group.current_count == 0 {
                        group.membership.replace(write_seq, None);
                    }
                }
                let after = estimate_ordered_adj_group_state(group);
                (before, after)
            }
            None if increment => {
                let group = OrderedAdjacencyGroupState {
                    current_count: 1,
                    membership: VersionedSlot::new(write_seq, Some(())),
                };
                let after = estimate_ordered_adj_group_state(&group);
                groups.insert(key, group);
                (0, after)
            }
            None => panic!("ordered adjacency group count underflow"),
        }
    }

    fn change_ordered_adj_out_group(
        &mut self,
        owner_id: u64,
        label_id: u32,
        increment: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::change_ordered_adj_group(
            &mut self.ordered_adj_out_groups,
            (owner_id, label_id),
            increment,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn change_ordered_adj_in_group(
        &mut self,
        owner_id: u64,
        label_id: u32,
        increment: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::change_ordered_adj_group(
            &mut self.ordered_adj_in_groups,
            (owner_id, label_id),
            increment,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_label_membership_slot(
        map: &mut HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
        target_label_id: u32,
        member_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        let members = map.entry(target_label_id).or_default();
        if let Some(slot) = members.get_mut(&member_id) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            (before, after)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = estimate_membership_slot(&slot);
            members.insert(member_id, slot);
            (0, after)
        }
    }

    fn set_sparse_membership_slot(
        map: &mut NodeIdMap<MembershipSlot<()>>,
        member_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        match map.get_mut(&member_id) {
            Some(slot) => {
                let before = estimate_membership_slot(slot);
                slot.replace(write_seq, value);
                let after = estimate_membership_slot(slot);
                (before, after)
            }
            None => {
                if present {
                    let slot = VersionedSlot::new(write_seq, value);
                    let after = estimate_membership_slot(&slot);
                    map.insert(member_id, slot);
                    (0, after)
                } else {
                    (0, 0)
                }
            }
        }
    }

    fn set_time_slot(&mut self, key: (u32, i64, u64), present: bool, write_seq: u64) {
        let value = present.then_some(());
        if let Some(slot) = self.time_node_index.get_mut(&key) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_membership_slot(&slot);
            self.time_node_index.insert(key, slot);
        }
    }

    fn set_ordered_edge_slot(&mut self, edge_id: u64, present: bool, write_seq: u64) {
        let value = present.then_some(());
        if let Some(slot) = self.ordered_edge_ids.get_mut(&edge_id) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_membership_slot(&slot);
            self.ordered_edge_ids.insert(edge_id, slot);
        }
    }

    fn set_node_label_membership_slot(
        &mut self,
        label_id: u32,
        node_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let value = present.then_some(());
        let key = (label_id, node_id);
        if let Some(slot) = self.label_node_index.get_mut(&key) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_membership_slot(&slot);
            self.label_node_index.insert(key, slot);
        }
    }

    fn set_ordered_edge_label_slot(
        &mut self,
        label_id: u32,
        edge_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let value = present.then_some(());
        let key = (label_id, edge_id);
        if let Some(slot) = self.ordered_label_edge_index.get_mut(&key) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_membership_slot(&slot);
            self.ordered_label_edge_index.insert(key, slot);
        }
    }

    fn set_secondary_eq_slot_in(
        state: &mut SecondaryEqState,
        index_id: u64,
        value_hash: u64,
        node_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        let members = state
            .entry(index_id)
            .or_default()
            .entry(value_hash)
            .or_default();
        if let Some(slot) = members.get_mut(&node_id) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            (before, after)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = estimate_membership_slot(&slot);
            members.insert(node_id, slot);
            (0, after)
        }
    }

    fn set_secondary_range_slot_in(
        state: &mut SecondaryRangeState,
        index_id: u64,
        encoded: NumericRangeSortKey,
        node_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        let entries = state.entry(index_id).or_default();
        if let Some(slot) = entries.get_mut(&(encoded, node_id)) {
            let before = estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_membership_slot(slot);
            (before, after)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = estimate_membership_slot(&slot);
            entries.insert((encoded, node_id), slot);
            (0, after)
        }
    }

    fn set_compound_slot_in(
        state: &mut CompoundSecondaryIndexState,
        index_id: u64,
        tuple_key: Vec<u8>,
        record_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        let entries = state.entry(index_id).or_default();
        let key = (tuple_key, record_id);
        if let Some(slot) = entries.get_mut(&key) {
            let before = key.0.len() + 8 + estimate_membership_slot(slot);
            slot.replace(write_seq, value);
            let after = key.0.len() + 8 + estimate_membership_slot(slot);
            (before, after)
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            let after = key.0.len() + 8 + estimate_membership_slot(&slot);
            entries.insert(key, slot);
            (0, after)
        }
    }

    fn add_field_index_lookup(
        map: &mut HashMap<u32, Vec<u64>>,
        label_id: u32,
        index_id: u64,
    ) -> (usize, usize) {
        match map.get_mut(&label_id) {
            Some(index_ids) => {
                let before = estimate_secondary_field_lookup_entry(index_ids);
                index_ids.push(index_id);
                let after = estimate_secondary_field_lookup_entry(index_ids);
                (before, after)
            }
            None => {
                let index_ids = vec![index_id];
                let after = estimate_secondary_field_lookup_entry(&index_ids);
                map.insert(label_id, index_ids);
                (0, after)
            }
        }
    }

    fn remove_field_index_lookup(
        map: &mut HashMap<u32, Vec<u64>>,
        label_id: u32,
        index_id: u64,
    ) -> (usize, usize, bool) {
        let mut before = 0;
        let mut after = 0;
        let mut remove_label_entry = false;
        if let Some(index_ids) = map.get_mut(&label_id) {
            before = estimate_secondary_field_lookup_entry(index_ids);
            index_ids.retain(|&id| id != index_id);
            if index_ids.is_empty() {
                remove_label_entry = true;
            } else {
                after = estimate_secondary_field_lookup_entry(index_ids);
            }
        }
        (before, after, remove_label_entry)
    }

    fn set_node_label_slot(
        &mut self,
        label_id: u32,
        member_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        self.set_node_label_membership_slot(label_id, member_id, present, write_seq);
    }

    fn set_edge_label_slot(
        &mut self,
        label_id: u32,
        member_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_label_membership_slot(
            &mut self.label_edge_index,
            label_id,
            member_id,
            present,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_adj_out_slot(
        &mut self,
        owner_id: u64,
        member_id: u64,
        value: Option<AdjEntry>,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_adj_slot(
            &mut self.adj_out,
            owner_id,
            member_id,
            value.clone(),
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
        let (before, after, inserted) = Self::set_ordered_adj_slot(
            &mut self.ordered_adj_out,
            owner_id,
            member_id,
            value,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
        if inserted {
            self.retained_adj_out_posting_slots = self
                .retained_adj_out_posting_slots
                .checked_add(1)
                .expect("retained outgoing adjacency posting count overflow");
        }
    }

    fn set_adj_in_slot(
        &mut self,
        owner_id: u64,
        member_id: u64,
        value: Option<AdjEntry>,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_adj_slot(
            &mut self.adj_in,
            owner_id,
            member_id,
            value.clone(),
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
        let (before, after, inserted) = Self::set_ordered_adj_slot(
            &mut self.ordered_adj_in,
            owner_id,
            member_id,
            value,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
        if inserted {
            self.retained_adj_in_posting_slots = self
                .retained_adj_in_posting_slots
                .checked_add(1)
                .expect("retained incoming adjacency posting count overflow");
        }
    }

    fn sync_ordered_adj_groups_for_edge_transition(
        &mut self,
        old_edge: Option<&EdgeRecord>,
        new_edge: Option<&EdgeRecord>,
        write_seq: u64,
    ) {
        let old_out = old_edge.map(|edge| (edge.from, edge.label_id));
        let new_out = new_edge.map(|edge| (edge.from, edge.label_id));
        if old_out != new_out {
            if let Some((owner_id, label_id)) = old_out {
                self.change_ordered_adj_out_group(owner_id, label_id, false, write_seq);
            }
            if let Some((owner_id, label_id)) = new_out {
                self.change_ordered_adj_out_group(owner_id, label_id, true, write_seq);
            }
        }

        let old_in = old_edge.map(|edge| (edge.to, edge.label_id));
        let new_in = new_edge.map(|edge| (edge.to, edge.label_id));
        if old_in != new_in {
            if let Some((owner_id, label_id)) = old_in {
                self.change_ordered_adj_in_group(owner_id, label_id, false, write_seq);
            }
            if let Some((owner_id, label_id)) = new_in {
                self.change_ordered_adj_in_group(owner_id, label_id, true, write_seq);
            }
        }
    }

    fn set_node_tombstone_slot(&mut self, member_id: u64, present: bool, write_seq: u64) {
        let _ = Self::set_sparse_membership_slot(
            &mut self.node_tombstones,
            member_id,
            present,
            write_seq,
        );
    }

    fn set_edge_tombstone_slot(&mut self, member_id: u64, present: bool, write_seq: u64) {
        let _ = Self::set_sparse_membership_slot(
            &mut self.edge_tombstones,
            member_id,
            present,
            write_seq,
        );
    }

    fn set_secondary_eq_slot(
        &mut self,
        index_id: u64,
        value_hash: u64,
        node_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_secondary_eq_slot_in(
            &mut self.secondary_eq_state,
            index_id,
            value_hash,
            node_id,
            present,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_secondary_range_slot(
        &mut self,
        index_id: u64,
        encoded: NumericRangeSortKey,
        node_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_secondary_range_slot_in(
            &mut self.secondary_range_state,
            index_id,
            encoded,
            node_id,
            present,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_compound_slot(
        &mut self,
        index_id: u64,
        tuple_key: Vec<u8>,
        record_id: u64,
        present: bool,
        write_seq: u64,
    ) {
        let (before, after) = Self::set_compound_slot_in(
            &mut self.secondary_compound_state,
            index_id,
            tuple_key,
            record_id,
            present,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    #[cfg(test)]
    fn recompute_estimated_size(&self) -> usize {
        let node_size: usize = self.nodes.values().map(estimate_node_record_slot).sum();
        let edge_size: usize = self.edges.values().map(estimate_edge_record_slot).sum();
        let node_key_size: usize = self
            .node_key_index
            .values()
            .map(|keys| {
                keys.iter()
                    .map(|(key, slot)| key.len() + estimate_lookup_slot(slot))
                    .sum::<usize>()
            })
            .sum();
        let edge_triple_size: usize = self
            .edge_triple_index
            .values()
            .map(estimate_lookup_slot)
            .sum();
        let adj_out_size: usize = self
            .adj_out
            .values()
            .map(|entries| entries.values().map(estimate_adj_slot).sum::<usize>())
            .sum();
        let adj_in_size: usize = self
            .adj_in
            .values()
            .map(|entries| entries.values().map(estimate_adj_slot).sum::<usize>())
            .sum();
        let ordered_edge_size: usize = self
            .ordered_edge_ids
            .values()
            .map(estimate_membership_slot)
            .sum::<usize>()
            + self
                .label_node_index
                .values()
                .map(estimate_membership_slot)
                .sum::<usize>()
            + self
                .ordered_label_edge_index
                .values()
                .map(estimate_membership_slot)
                .sum::<usize>();
        let ordered_adj_size: usize = self
            .ordered_adj_out
            .values()
            .map(|entries| entries.values().map(estimate_adj_slot).sum::<usize>())
            .sum::<usize>()
            + self
                .ordered_adj_in
                .values()
                .map(|entries| entries.values().map(estimate_adj_slot).sum::<usize>())
                .sum::<usize>();
        let ordered_adj_group_size: usize = self
            .ordered_adj_out_groups
            .values()
            .map(estimate_ordered_adj_group_state)
            .sum::<usize>()
            + self
                .ordered_adj_in_groups
                .values()
                .map(estimate_ordered_adj_group_state)
                .sum::<usize>();
        let label_idx_size: usize = self
            .label_edge_index
            .values()
            .map(|members| {
                members
                    .values()
                    .map(estimate_membership_slot)
                    .sum::<usize>()
            })
            .sum::<usize>();
        let time_idx_size: usize = self
            .time_node_index
            .values()
            .map(estimate_membership_slot)
            .sum();
        let secondary_decl_size: usize = self
            .catalog
            .secondary_index_declarations
            .values()
            .map(estimate_secondary_decl_entry)
            .sum();
        let secondary_eq_lookup_size: usize = self
            .catalog
            .secondary_eq_by_prop
            .values()
            .map(|by_prop| {
                by_prop
                    .iter()
                    .map(|(prop_key, index_ids)| {
                        estimate_secondary_eq_lookup_entry(prop_key, index_ids)
                    })
                    .sum::<usize>()
            })
            .sum();
        let secondary_range_lookup_size: usize = self
            .catalog
            .secondary_range_by_prop
            .values()
            .map(|by_prop| {
                by_prop
                    .iter()
                    .map(|(prop_key, indexes)| {
                        estimate_secondary_range_lookup_entry(prop_key, indexes)
                    })
                    .sum::<usize>()
            })
            .sum();
        let secondary_edge_eq_lookup_size: usize = self
            .catalog
            .secondary_edge_eq_by_prop
            .values()
            .map(|by_prop| {
                by_prop
                    .iter()
                    .map(|(prop_key, index_ids)| {
                        estimate_secondary_eq_lookup_entry(prop_key, index_ids)
                    })
                    .sum::<usize>()
            })
            .sum();
        let secondary_edge_range_lookup_size: usize = self
            .catalog
            .secondary_edge_range_by_prop
            .values()
            .map(|by_prop| {
                by_prop
                    .iter()
                    .map(|(prop_key, indexes)| {
                        estimate_secondary_range_lookup_entry(prop_key, indexes)
                    })
                    .sum::<usize>()
            })
            .sum();
        let secondary_node_field_lookup_size: usize = self
            .catalog
            .secondary_node_field_indexes_by_label
            .values()
            .map(|index_ids| estimate_secondary_field_lookup_entry(index_ids))
            .sum();
        let secondary_edge_field_lookup_size: usize = self
            .catalog
            .secondary_edge_field_indexes_by_label
            .values()
            .map(|index_ids| estimate_secondary_field_lookup_entry(index_ids))
            .sum();
        let secondary_eq_state_size: usize = self
            .secondary_eq_state
            .values()
            .map(|groups| {
                groups
                    .values()
                    .map(|members| {
                        members
                            .values()
                            .map(estimate_membership_slot)
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum();
        let secondary_range_state_size: usize = self
            .secondary_range_state
            .values()
            .map(|entries| {
                entries
                    .values()
                    .map(estimate_membership_slot)
                    .sum::<usize>()
            })
            .sum();
        let secondary_compound_state_size: usize = self
            .secondary_compound_state
            .values()
            .map(estimate_compound_state_entries)
            .sum();

        node_size
            + edge_size
            + node_key_size
            + edge_triple_size
            + adj_out_size
            + adj_in_size
            + ordered_edge_size
            + ordered_adj_size
            + ordered_adj_group_size
            + label_idx_size
            + time_idx_size
            + secondary_decl_size
            + secondary_eq_lookup_size
            + secondary_range_lookup_size
            + secondary_edge_eq_lookup_size
            + secondary_edge_range_lookup_size
            + secondary_node_field_lookup_size
            + secondary_edge_field_lookup_size
            + secondary_eq_state_size
            + secondary_range_state_size
            + secondary_compound_state_size
    }

    fn current_node(&self, id: u64) -> Option<&NodeRecord> {
        record_current(self.nodes.get(&id)?)
    }

    fn current_edge(&self, id: u64) -> Option<&EdgeRecord> {
        record_current(self.edges.get(&id)?)
    }

    fn current_edge_triple_id(&self, from: u64, to: u64, label_id: u32) -> Option<u64> {
        self.edge_triple_index
            .get(&(from, to, label_id))
            .and_then(slot_option_current)
            .copied()
    }

    fn node_at(&self, id: u64, snapshot_seq: u64) -> Option<&NodeRecord> {
        match record_at(self.nodes.get(&id)?, snapshot_seq)? {
            RecordState::Live(node) => Some(node),
            RecordState::Tombstone(_) => None,
        }
    }

    fn edge_at(&self, id: u64, snapshot_seq: u64) -> Option<&EdgeRecord> {
        match record_at(self.edges.get(&id)?, snapshot_seq)? {
            RecordState::Live(edge) => Some(edge),
            RecordState::Tombstone(_) => None,
        }
    }

    fn node_deleted_at(&self, id: u64, snapshot_seq: u64) -> bool {
        self.node_tombstones
            .get(&id)
            .is_some_and(|slot| slot_option_visible(slot, snapshot_seq))
    }

    fn edge_deleted_at(&self, id: u64, snapshot_seq: u64) -> bool {
        self.edge_tombstones
            .get(&id)
            .is_some_and(|slot| slot_option_visible(slot, snapshot_seq))
    }

    fn add_secondary_index_entries_for_node(&mut self, node: &NodeRecord, write_seq: u64) {
        if self.catalog.secondary_eq_by_prop.is_empty()
            && self.catalog.secondary_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_node_field_indexes_by_label
                .is_empty()
        {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        for &label_id in node.label_ids.as_slice() {
            self.catalog.collect_secondary_index_entries_for_node_label(
                label_id,
                node.id,
                &node.props,
                true,
                &mut eq_actions,
                &mut range_actions,
            );
            self.catalog.collect_compound_index_entries_for_node_label(
                label_id,
                node,
                true,
                &mut compound_actions,
            );
        }
        for (index_id, value_hash, node_id, present) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node_id, present, write_seq);
        }
        for (index_id, encoded, node_id, present) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node_id, present, write_seq);
        }
        for (index_id, tuple_key, node_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, node_id, present, write_seq);
        }
    }

    fn remove_secondary_index_entries_for_node(&mut self, node: &NodeRecord, write_seq: u64) {
        if self.catalog.secondary_eq_by_prop.is_empty()
            && self.catalog.secondary_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_node_field_indexes_by_label
                .is_empty()
        {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        for &label_id in node.label_ids.as_slice() {
            self.catalog.collect_secondary_index_entries_for_node_label(
                label_id,
                node.id,
                &node.props,
                false,
                &mut eq_actions,
                &mut range_actions,
            );
            self.catalog.collect_compound_index_entries_for_node_label(
                label_id,
                node,
                false,
                &mut compound_actions,
            );
        }
        for (index_id, value_hash, node_id, present) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node_id, present, write_seq);
        }
        for (index_id, encoded, node_id, present) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node_id, present, write_seq);
        }
        for (index_id, tuple_key, node_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, node_id, present, write_seq);
        }
    }

    fn sync_secondary_index_entries_for_node_upsert(
        &mut self,
        old_node: Option<&NodeRecord>,
        new_node: &NodeRecord,
        write_seq: u64,
    ) {
        if self.catalog.secondary_eq_by_prop.is_empty()
            && self.catalog.secondary_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_node_field_indexes_by_label
                .is_empty()
        {
            return;
        }

        let Some(old_node) = old_node else {
            self.add_secondary_index_entries_for_node(new_node, write_seq);
            return;
        };

        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();

        for &old_label_id in old_node.label_ids.as_slice() {
            if !new_node.label_ids.contains(old_label_id) {
                self.catalog.collect_secondary_index_entries_for_node_label(
                    old_label_id,
                    old_node.id,
                    &old_node.props,
                    false,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.catalog.collect_compound_index_entries_for_node_label(
                    old_label_id,
                    old_node,
                    false,
                    &mut compound_actions,
                );
            }
        }

        for &new_label_id in new_node.label_ids.as_slice() {
            if old_node.label_ids.contains(new_label_id) {
                self.catalog.collect_secondary_index_updates_for_node_label(
                    new_label_id,
                    old_node,
                    new_node,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.catalog.collect_compound_index_updates_for_node_label(
                    new_label_id,
                    old_node,
                    new_node,
                    &mut compound_actions,
                );
            } else {
                self.catalog.collect_secondary_index_entries_for_node_label(
                    new_label_id,
                    new_node.id,
                    &new_node.props,
                    true,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.catalog.collect_compound_index_entries_for_node_label(
                    new_label_id,
                    new_node,
                    true,
                    &mut compound_actions,
                );
            }
        }
        for (index_id, value_hash, node_id, present) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node_id, present, write_seq);
        }
        for (index_id, encoded, node_id, present) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node_id, present, write_seq);
        }
        for (index_id, tuple_key, node_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, node_id, present, write_seq);
        }
    }

    fn add_secondary_index_entries_for_edge(&mut self, edge: &EdgeRecord, write_seq: u64) {
        if self.catalog.secondary_edge_eq_by_prop.is_empty()
            && self.catalog.secondary_edge_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_edge_field_indexes_by_label
                .is_empty()
        {
            return;
        }
        let eq_by_prop = self.catalog.secondary_edge_eq_by_prop.get(&edge.label_id);
        let range_by_prop = self
            .catalog
            .secondary_edge_range_by_prop
            .get(&edge.label_id);
        let has_compound = self
            .catalog
            .secondary_edge_field_indexes_by_label
            .contains_key(&edge.label_id);
        if eq_by_prop.is_none() && range_by_prop.is_none() && !has_compound {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        if eq_by_prop.is_some() || range_by_prop.is_some() {
            for (prop_key, prop_value) in &edge.props {
                if let Some(index_ids) =
                    eq_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str()))
                {
                    let value_hash = hash_prop_equality_key(prop_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash));
                    }
                }

                if let Some(indexes) =
                    range_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str()))
                {
                    for &index_id in indexes {
                        if let Some(encoded) = numeric_range_sort_key_for_value(prop_value) {
                            range_actions.push((index_id, encoded));
                        }
                    }
                }
            }
        }
        self.catalog.collect_compound_index_entries_for_edge_label(
            edge.label_id,
            edge,
            true,
            &mut compound_actions,
        );
        for (index_id, value_hash) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, edge.id, true, write_seq);
        }
        for (index_id, encoded) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, edge.id, true, write_seq);
        }
        for (index_id, tuple_key, edge_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, edge_id, present, write_seq);
        }
    }

    fn remove_secondary_index_entries_for_edge(&mut self, edge: &EdgeRecord, write_seq: u64) {
        if self.catalog.secondary_edge_eq_by_prop.is_empty()
            && self.catalog.secondary_edge_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_edge_field_indexes_by_label
                .is_empty()
        {
            return;
        }
        let eq_by_prop = self.catalog.secondary_edge_eq_by_prop.get(&edge.label_id);
        let range_by_prop = self
            .catalog
            .secondary_edge_range_by_prop
            .get(&edge.label_id);
        let has_compound = self
            .catalog
            .secondary_edge_field_indexes_by_label
            .contains_key(&edge.label_id);
        if eq_by_prop.is_none() && range_by_prop.is_none() && !has_compound {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        if eq_by_prop.is_some() || range_by_prop.is_some() {
            for (prop_key, prop_value) in &edge.props {
                if let Some(index_ids) =
                    eq_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str()))
                {
                    let value_hash = hash_prop_equality_key(prop_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash));
                    }
                }

                if let Some(indexes) =
                    range_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str()))
                {
                    for &index_id in indexes {
                        if let Some(encoded) = numeric_range_sort_key_for_value(prop_value) {
                            range_actions.push((index_id, encoded));
                        }
                    }
                }
            }
        }
        self.catalog.collect_compound_index_entries_for_edge_label(
            edge.label_id,
            edge,
            false,
            &mut compound_actions,
        );
        for (index_id, value_hash) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, edge.id, false, write_seq);
        }
        for (index_id, encoded) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, edge.id, false, write_seq);
        }
        for (index_id, tuple_key, edge_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, edge_id, present, write_seq);
        }
    }

    fn sync_secondary_index_entries_for_edge_upsert(
        &mut self,
        old_edge: Option<&EdgeRecord>,
        new_edge: &EdgeRecord,
        write_seq: u64,
    ) {
        if self.catalog.secondary_edge_eq_by_prop.is_empty()
            && self.catalog.secondary_edge_range_by_prop.is_empty()
            && self
                .catalog
                .secondary_edge_field_indexes_by_label
                .is_empty()
        {
            return;
        }

        let Some(old_edge) = old_edge else {
            self.add_secondary_index_entries_for_edge(new_edge, write_seq);
            return;
        };

        if old_edge.label_id != new_edge.label_id {
            self.remove_secondary_index_entries_for_edge(old_edge, write_seq);
            self.add_secondary_index_entries_for_edge(new_edge, write_seq);
            return;
        }

        let eq_by_prop = self
            .catalog
            .secondary_edge_eq_by_prop
            .get(&new_edge.label_id);
        let range_by_prop = self
            .catalog
            .secondary_edge_range_by_prop
            .get(&new_edge.label_id);
        let has_compound = self
            .catalog
            .secondary_edge_field_indexes_by_label
            .contains_key(&new_edge.label_id);
        if eq_by_prop.is_none() && range_by_prop.is_none() && !has_compound {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();

        if let Some(by_prop) = eq_by_prop {
            for (prop_key, index_ids) in by_prop {
                let old_value = old_edge.props.get(prop_key.as_str());
                let new_value = new_edge.props.get(prop_key.as_str());
                if optional_semantic_property_eq(old_value, new_value) {
                    continue;
                }
                if let Some(old_value) = old_value {
                    let value_hash = hash_prop_equality_key(old_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, old_edge.id, false));
                    }
                }
                if let Some(new_value) = new_value {
                    let value_hash = hash_prop_equality_key(new_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, new_edge.id, true));
                    }
                }
            }
        }

        if let Some(by_prop) = range_by_prop {
            for (prop_key, indexes) in by_prop {
                let old_value = old_edge.props.get(prop_key.as_str());
                let new_value = new_edge.props.get(prop_key.as_str());
                let old_encoded = old_value.and_then(numeric_range_sort_key_for_value);
                let new_encoded = new_value.and_then(numeric_range_sort_key_for_value);
                if old_encoded == new_encoded {
                    continue;
                }
                for &index_id in indexes {
                    if let Some(encoded) = old_encoded {
                        range_actions.push((index_id, encoded, old_edge.id, false));
                    }
                    if let Some(encoded) = new_encoded {
                        range_actions.push((index_id, encoded, new_edge.id, true));
                    }
                }
            }
        }
        self.catalog.collect_compound_index_updates_for_edge_label(
            new_edge.label_id,
            old_edge,
            new_edge,
            &mut compound_actions,
        );
        for (index_id, value_hash, edge_id, present) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, edge_id, present, write_seq);
        }
        for (index_id, encoded, edge_id, present) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, edge_id, present, write_seq);
        }
        for (index_id, tuple_key, edge_id, present) in compound_actions {
            self.set_compound_slot(index_id, tuple_key, edge_id, present, write_seq);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityOpinion<T = u64> {
    Missing,
    Removed,
    Live(T),
}
impl<T> From<Option<Option<T>>> for IdentityOpinion<T> {
    fn from(value: Option<Option<T>>) -> Self {
        match value {
            None => Self::Missing,
            Some(None) => Self::Removed,
            Some(Some(value)) => Self::Live(value),
        }
    }
}

pub(crate) type EdgeCoreFields = (u64, u64, i64, i64, f32, i64, i64);

pub(crate) enum PrimaryOpinion<T> {
    Missing,
    Removed(TombstoneEntry),
    Live(T),
}

/// Generation-owned MVCC state. Legacy histories use `state`; candidate primary,
/// graph and secondary append maps are authoritative and read without that lock.
pub struct Memtable {
    state: MemtableStateOwner,
    #[cfg(feature = "production-concurrency")]
    primary: Option<PrimaryBinding>,
    #[cfg(test)]
    tombstone_scan_work: std::sync::atomic::AtomicUsize,
}

impl Clone for Memtable {
    fn clone(&self) -> Self {
        #[cfg(feature = "production-concurrency")]
        assert!(
            self.primary.is_none(),
            "candidate generations clone their guarded owner"
        );
        Self {
            state: new_memtable_state(self.state.read().unwrap().clone()),
            #[cfg(feature = "production-concurrency")]
            primary: None,
            #[cfg(test)]
            tombstone_scan_work: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub(crate) fn key_opinions_at(
        &self,
        keys: &[(u32, &str)],
        seq: u64,
    ) -> Result<Vec<IdentityOpinion>, EngineError> {
        let state = self.primary_read()?;
        Ok(state.keys(keys, seq))
    }
    pub(crate) fn triple_opinions_at(
        &self,
        triples: &[(u64, u64, u32)],
        seq: u64,
    ) -> Result<Vec<IdentityOpinion>, EngineError> {
        let state = self.primary_read()?;
        Ok(triples
            .iter()
            .map(|&triple| IdentityOpinion::from(state.triple(triple, seq)))
            .collect())
    }

    #[cfg(feature = "production-concurrency")]
    pub(crate) fn seal_export(&self, seq: u64) {
        let facts = crate::segment_writer::SealedExport::capture(self, seq);
        self.state.write().unwrap().sealed_export = Some(facts);
    }
    #[cfg(feature = "production-concurrency")]
    pub(crate) fn sealed_export(&self) -> Result<crate::segment_writer::SealedExport, EngineError> {
        self.state.read().unwrap().sealed_export.ok_or_else(|| {
            EngineError::InvalidOperation(
                "candidate storage export requires a sealed generation".into(),
            )
        })
    }
    pub(crate) fn is_candidate(&self) -> bool {
        #[cfg(feature = "production-concurrency")]
        {
            self.primary.is_some()
        }
        #[cfg(not(feature = "production-concurrency"))]
        {
            false
        }
    }

    pub fn new() -> Self {
        Self {
            state: new_memtable_state(MemtableState::default()),
            #[cfg(feature = "production-concurrency")]
            primary: None,
            #[cfg(test)]
            tombstone_scan_work: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn node_sequence_opinion_at(
        &self,
        id: u64,
        seq: u64,
    ) -> Result<PrimaryOpinion<u64>, EngineError> {
        let p = self.primary_read()?;
        Ok(match p.node_state(id, seq) {
            None => PrimaryOpinion::Missing,
            Some(RecordState::Tombstone(t)) => PrimaryOpinion::Removed(*t),
            Some(RecordState::Live(n)) => PrimaryOpinion::Live(n.last_write_seq),
        })
    }
    pub(crate) fn edge_sequence_opinion_at(
        &self,
        id: u64,
        seq: u64,
    ) -> Result<PrimaryOpinion<u64>, EngineError> {
        let p = self.primary_read()?;
        Ok(match p.edge_state(id, seq) {
            None => PrimaryOpinion::Missing,
            Some(RecordState::Tombstone(t)) => PrimaryOpinion::Removed(*t),
            Some(RecordState::Live(n)) => PrimaryOpinion::Live(n.last_write_seq),
        })
    }
    pub(crate) fn node_opinion_at(
        &self,
        id: u64,
        seq: u64,
    ) -> Result<PrimaryOpinion<NodeRecord>, EngineError> {
        let p = self.primary_read()?;
        Ok(match p.node_state(id, seq) {
            None => PrimaryOpinion::Missing,
            Some(RecordState::Tombstone(t)) => PrimaryOpinion::Removed(*t),
            Some(RecordState::Live(v)) => PrimaryOpinion::Live(v.clone()),
        })
    }
    pub(crate) fn edge_opinion_at(
        &self,
        id: u64,
        seq: u64,
    ) -> Result<PrimaryOpinion<EdgeRecord>, EngineError> {
        let p = self.primary_read()?;
        Ok(match p.edge_state(id, seq) {
            None => PrimaryOpinion::Missing,
            Some(RecordState::Tombstone(t)) => PrimaryOpinion::Removed(*t),
            Some(RecordState::Live(v)) => PrimaryOpinion::Live(v.clone()),
        })
    }
    pub(crate) fn node_key_record_opinion_at(
        &self,
        label: u32,
        key: &str,
        seq: u64,
    ) -> Result<IdentityOpinion<NodeRecord>, EngineError> {
        let p = self.primary_read()?;
        Ok(match p.key(label, key, seq) {
            None => IdentityOpinion::Missing,
            Some(None) => IdentityOpinion::Removed,
            Some(Some(id)) => p
                .node_at(id, seq)
                .filter(|n| n.label_ids.contains(label) && n.key == key)
                .map_or(IdentityOpinion::Removed, |n| {
                    IdentityOpinion::Live(n.clone())
                }),
        })
    }

    fn primary_read(&self) -> Result<PrimaryRead<'_>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if let Some(maps) = &self.primary {
            let session = maps
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            #[cfg(test)]
            maps.pause_for_test(3)?;
            return Ok(PrimaryRead::Candidate(maps, session));
        }
        Ok(PrimaryRead::Legacy(self.state.read().unwrap()))
    }

    pub(crate) fn apply_op(&self, op: &WalOp, last_write_seq: u64) {
        #[cfg(feature = "production-concurrency")]
        assert!(
            self.primary.is_none(),
            "candidate install requires pre-WAL preparation/session"
        );
        self.apply_transition(op, last_write_seq, None, None, true);
    }

    // Legacy installation only. Candidate installation updates small diagnostics
    // directly after its append-only families, without maintaining these histories.
    fn apply_transition(
        &self,
        op: &WalOp,
        last_write_seq: u64,
        old_node: Option<&NodeRecord>,
        old_edge: Option<&EdgeRecord>,
        legacy: bool,
    ) {
        let mut state = self.state.write().unwrap();
        match op {
            WalOp::UpsertNode(node) => {
                let legacy_old = legacy.then(|| {
                    crate::engine::payload::Owned::new(state.current_node(node.id).cloned())
                });
                let old_node = legacy_old.as_ref().map_or(old_node, |owned| owned.as_ref());
                let was_live = old_node.is_some();
                let was_deleted = state.node_deleted_at(node.id, u64::MAX);
                if let Some(old) = old_node {
                    for &old_label_id in old.label_ids.as_slice() {
                        let label_removed = !node.label_ids.contains(old_label_id);
                        if legacy && (label_removed || old.key != node.key) {
                            state.set_node_key(old_label_id, &old.key, None, last_write_seq);
                        }
                        if label_removed && legacy {
                            state.set_node_label_slot(old_label_id, old.id, false, last_write_seq);
                        }
                        if (label_removed || old.updated_at != node.updated_at) && legacy {
                            state.set_time_slot(
                                (old_label_id, old.updated_at, old.id),
                                false,
                                last_write_seq,
                            );
                        }
                    }
                }

                let mut stored = legacy.then(|| crate::engine::payload::Owned::new(node.clone()));
                if let Some(stored) = &mut stored {
                    stored.last_write_seq = last_write_seq;
                }
                for &new_label_id in node.label_ids.as_slice() {
                    if legacy {
                        state.set_node_key(new_label_id, &node.key, Some(node.id), last_write_seq);
                    }
                    if legacy {
                        state.set_node_label_slot(new_label_id, node.id, true, last_write_seq);
                    }
                    if legacy {
                        state.set_time_slot(
                            (new_label_id, node.updated_at, node.id),
                            true,
                            last_write_seq,
                        );
                    }
                }
                if legacy {
                    state.sync_secondary_index_entries_for_node_upsert(
                        old_node,
                        stored.as_deref().unwrap_or(node),
                        last_write_seq,
                    );
                }
                if was_deleted {
                    state.set_node_tombstone_slot(node.id, false, last_write_seq);
                }
                if legacy {
                    state.set_node_state(
                        node.id,
                        RecordState::Live(stored.take().expect("legacy primary").into_inner()),
                        last_write_seq,
                    );
                }
                if !was_live {
                    state.live_node_count = state.live_node_count.saturating_add(1);
                }
            }
            WalOp::UpsertEdge(edge) => {
                let legacy_old = legacy.then(|| {
                    crate::engine::payload::Owned::new(state.current_edge(edge.id).cloned())
                });
                let old_edge = legacy_old.as_ref().map_or(old_edge, |owned| owned.as_ref());
                let was_live = old_edge.is_some();
                let was_deleted = state.edge_deleted_at(edge.id, u64::MAX);
                if legacy {
                    state.sync_ordered_adj_groups_for_edge_transition(
                        old_edge,
                        Some(edge),
                        last_write_seq,
                    );
                }
                if let Some(old) = old_edge {
                    if legacy
                        && (old.from != edge.from
                            || old.to != edge.to
                            || old.label_id != edge.label_id)
                        && state.current_edge_triple_id(old.from, old.to, old.label_id)
                            == Some(old.id)
                    {
                        state.set_edge_triple(old.from, old.to, old.label_id, None, last_write_seq);
                    }
                    if old.label_id != edge.label_id {
                        if legacy {
                            state.set_edge_label_slot(old.label_id, old.id, false, last_write_seq);
                        }
                        if legacy {
                            state.set_ordered_edge_label_slot(
                                old.label_id,
                                old.id,
                                false,
                                last_write_seq,
                            );
                        }
                    }
                    if old.from != edge.from || old.to != edge.to {
                        if legacy {
                            state.set_adj_out_slot(old.from, old.id, None, last_write_seq);
                        }
                        if legacy {
                            state.set_adj_in_slot(old.to, old.id, None, last_write_seq);
                        }
                    }
                }

                let adj_out_entry = AdjEntry {
                    edge_id: edge.id,
                    label_id: edge.label_id,
                    neighbor_id: edge.to,
                    weight: edge.weight,
                    valid_from: edge.valid_from,
                    valid_to: edge.valid_to,
                };
                let adj_in_entry = AdjEntry {
                    edge_id: edge.id,
                    label_id: edge.label_id,
                    neighbor_id: edge.from,
                    weight: edge.weight,
                    valid_from: edge.valid_from,
                    valid_to: edge.valid_to,
                };

                if legacy {
                    let current_triple_id =
                        state.current_edge_triple_id(edge.from, edge.to, edge.label_id);
                    let should_update_triple = match old_edge {
                        Some(old)
                            if old.from == edge.from
                                && old.to == edge.to
                                && old.label_id == edge.label_id =>
                        {
                            current_triple_id.is_none_or(|current_id| current_id == edge.id)
                        }
                        _ => true,
                    };
                    if should_update_triple {
                        state.set_edge_triple(
                            edge.from,
                            edge.to,
                            edge.label_id,
                            Some(edge.id),
                            last_write_seq,
                        );
                    }
                }
                if legacy {
                    state.set_adj_out_slot(edge.from, edge.id, Some(adj_out_entry), last_write_seq);
                }
                if legacy {
                    state.set_adj_in_slot(edge.to, edge.id, Some(adj_in_entry), last_write_seq);
                }
                if legacy {
                    state.set_edge_label_slot(edge.label_id, edge.id, true, last_write_seq);
                }
                if legacy {
                    state.set_ordered_edge_label_slot(edge.label_id, edge.id, true, last_write_seq);
                }
                if legacy {
                    state.set_ordered_edge_slot(edge.id, true, last_write_seq);
                }

                let mut stored = legacy.then(|| crate::engine::payload::Owned::new(edge.clone()));
                if let Some(stored) = &mut stored {
                    stored.last_write_seq = last_write_seq;
                }
                if legacy {
                    state.sync_secondary_index_entries_for_edge_upsert(
                        old_edge,
                        stored.as_deref().unwrap_or(edge),
                        last_write_seq,
                    );
                }
                if was_deleted {
                    state.set_edge_tombstone_slot(edge.id, false, last_write_seq);
                }
                if legacy {
                    state.set_edge_state(
                        edge.id,
                        RecordState::Live(stored.take().expect("legacy primary").into_inner()),
                        last_write_seq,
                    );
                }
                if !was_live {
                    state.live_edge_count = state.live_edge_count.saturating_add(1);
                }
            }
            WalOp::DeleteNode { id, deleted_at } => {
                let legacy_old = legacy
                    .then(|| crate::engine::payload::Owned::new(state.current_node(*id).cloned()));
                if let Some(node) = legacy_old.as_ref().map_or(old_node, |owned| owned.as_ref()) {
                    for &label_id in node.label_ids.as_slice() {
                        if legacy {
                            state.set_node_key(label_id, &node.key, None, last_write_seq);
                        }
                        if legacy {
                            state.set_node_label_slot(label_id, node.id, false, last_write_seq);
                        }
                        if legacy {
                            state.set_time_slot(
                                (label_id, node.updated_at, node.id),
                                false,
                                last_write_seq,
                            );
                        }
                    }
                    if legacy {
                        state.remove_secondary_index_entries_for_node(node, last_write_seq);
                    }
                    state.live_node_count = state.live_node_count.saturating_sub(1);
                }
                if legacy {
                    state.set_node_state(
                        *id,
                        RecordState::Tombstone(TombstoneEntry {
                            deleted_at: *deleted_at,
                            last_write_seq,
                        }),
                        last_write_seq,
                    );
                }
                state.set_node_tombstone_slot(*id, true, last_write_seq);
            }
            WalOp::DeleteEdge { id, deleted_at } => {
                let legacy_old = legacy
                    .then(|| crate::engine::payload::Owned::new(state.current_edge(*id).cloned()));
                if let Some(edge) = legacy_old.as_ref().map_or(old_edge, |owned| owned.as_ref()) {
                    if legacy {
                        state.sync_ordered_adj_groups_for_edge_transition(
                            Some(edge),
                            None,
                            last_write_seq,
                        );
                    }
                    if legacy
                        && state.current_edge_triple_id(edge.from, edge.to, edge.label_id)
                            == Some(*id)
                    {
                        state.set_edge_triple(
                            edge.from,
                            edge.to,
                            edge.label_id,
                            None,
                            last_write_seq,
                        );
                    }
                    if legacy {
                        state.set_edge_label_slot(edge.label_id, edge.id, false, last_write_seq);
                    }
                    if legacy {
                        state.set_ordered_edge_label_slot(
                            edge.label_id,
                            edge.id,
                            false,
                            last_write_seq,
                        );
                    }
                    if legacy {
                        state.set_adj_out_slot(edge.from, edge.id, None, last_write_seq);
                    }
                    if legacy {
                        state.set_adj_in_slot(edge.to, edge.id, None, last_write_seq);
                    }
                    if legacy {
                        state.set_ordered_edge_slot(edge.id, false, last_write_seq);
                    }
                    if legacy {
                        state.remove_secondary_index_entries_for_edge(edge, last_write_seq);
                    }
                    state.live_edge_count = state.live_edge_count.saturating_sub(1);
                }
                if legacy {
                    state.set_edge_state(
                        *id,
                        RecordState::Tombstone(TombstoneEntry {
                            deleted_at: *deleted_at,
                            last_write_seq,
                        }),
                        last_write_seq,
                    );
                }
                state.set_edge_tombstone_slot(*id, true, last_write_seq);
            }
            WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => {}
        }
    }

    pub fn register_secondary_index(
        &self,
        entry: &SecondaryIndexManifestEntry,
    ) -> Result<(), EngineError> {
        if self.is_candidate() {
            return Err(EngineError::InvalidOperation(
                "candidate indexes require an immutable replacement binding".into(),
            ));
        }
        let mut state = self.state.write().unwrap();
        if state
            .catalog
            .secondary_index_declarations
            .contains_key(&entry.index_id)
        {
            return Ok(());
        }
        state
            .catalog
            .secondary_index_declarations
            .insert(entry.index_id, entry.clone());
        state.estimated_bytes += estimate_secondary_decl_entry(entry);
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, prop_key } => match &entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state
                            .catalog
                            .secondary_eq_by_prop
                            .entry(*label_id)
                            .or_default();
                        match by_prop.get_mut(prop_key) {
                            Some(index_ids) => {
                                let before =
                                    estimate_secondary_eq_lookup_entry(prop_key, index_ids);
                                index_ids.push(entry.index_id);
                                let after = estimate_secondary_eq_lookup_entry(prop_key, index_ids);
                                (before, after)
                            }
                            None => {
                                let index_ids = vec![entry.index_id];
                                let after =
                                    estimate_secondary_eq_lookup_entry(prop_key, &index_ids);
                                by_prop.insert(prop_key.clone(), index_ids);
                                (0, after)
                            }
                        }
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    state.secondary_eq_state.entry(entry.index_id).or_default();
                    let mut seeded = Vec::new();
                    for node in state.nodes.values().filter_map(record_current) {
                        if !node.label_ids.contains(*label_id) {
                            continue;
                        }
                        let Some(prop_value) = node.props.get(prop_key) else {
                            continue;
                        };
                        seeded.push((
                            hash_prop_equality_key(prop_value),
                            node.id,
                            node.last_write_seq,
                        ));
                    }
                    for (value_hash, node_id, write_seq) in seeded {
                        state.set_secondary_eq_slot(
                            entry.index_id,
                            value_hash,
                            node_id,
                            true,
                            write_seq,
                        );
                    }
                }
                SecondaryIndexKind::Range => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state
                            .catalog
                            .secondary_range_by_prop
                            .entry(*label_id)
                            .or_default();
                        match by_prop.get_mut(prop_key) {
                            Some(indexes) => {
                                let before =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                indexes.push(entry.index_id);
                                let after =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                (before, after)
                            }
                            None => {
                                let indexes = vec![entry.index_id];
                                let after =
                                    estimate_secondary_range_lookup_entry(prop_key, &indexes);
                                by_prop.insert(prop_key.clone(), indexes);
                                (0, after)
                            }
                        }
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    state
                        .secondary_range_state
                        .entry(entry.index_id)
                        .or_default();
                    let mut seeded = Vec::new();
                    for node in state.nodes.values().filter_map(record_current) {
                        if !node.label_ids.contains(*label_id) {
                            continue;
                        }
                        let Some(prop_value) = node.props.get(prop_key) else {
                            continue;
                        };
                        let Some(encoded) = numeric_range_sort_key_for_value(prop_value) else {
                            continue;
                        };
                        seeded.push((encoded, node.id, node.last_write_seq));
                    }
                    for (encoded, node_id, write_seq) in seeded {
                        state.set_secondary_range_slot(
                            entry.index_id,
                            encoded,
                            node_id,
                            true,
                            write_seq,
                        );
                    }
                }
            },
            SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => match &entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state
                            .catalog
                            .secondary_edge_eq_by_prop
                            .entry(*label_id)
                            .or_default();
                        match by_prop.get_mut(prop_key) {
                            Some(index_ids) => {
                                let before =
                                    estimate_secondary_eq_lookup_entry(prop_key, index_ids);
                                index_ids.push(entry.index_id);
                                let after = estimate_secondary_eq_lookup_entry(prop_key, index_ids);
                                (before, after)
                            }
                            None => {
                                let index_ids = vec![entry.index_id];
                                let after =
                                    estimate_secondary_eq_lookup_entry(prop_key, &index_ids);
                                by_prop.insert(prop_key.clone(), index_ids);
                                (0, after)
                            }
                        }
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    state.secondary_eq_state.entry(entry.index_id).or_default();
                    let mut seeded = Vec::new();
                    for edge in state.edges.values().filter_map(record_current) {
                        if edge.label_id != *label_id {
                            continue;
                        }
                        let Some(prop_value) = edge.props.get(prop_key) else {
                            continue;
                        };
                        seeded.push((
                            hash_prop_equality_key(prop_value),
                            edge.id,
                            edge.last_write_seq,
                        ));
                    }
                    for (value_hash, edge_id, write_seq) in seeded {
                        state.set_secondary_eq_slot(
                            entry.index_id,
                            value_hash,
                            edge_id,
                            true,
                            write_seq,
                        );
                    }
                }
                SecondaryIndexKind::Range => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state
                            .catalog
                            .secondary_edge_range_by_prop
                            .entry(*label_id)
                            .or_default();
                        match by_prop.get_mut(prop_key) {
                            Some(indexes) => {
                                let before =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                indexes.push(entry.index_id);
                                let after =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                (before, after)
                            }
                            None => {
                                let indexes = vec![entry.index_id];
                                let after =
                                    estimate_secondary_range_lookup_entry(prop_key, &indexes);
                                by_prop.insert(prop_key.clone(), indexes);
                                (0, after)
                            }
                        }
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    state
                        .secondary_range_state
                        .entry(entry.index_id)
                        .or_default();
                    let mut seeded = Vec::new();
                    for edge in state.edges.values().filter_map(record_current) {
                        if edge.label_id != *label_id {
                            continue;
                        }
                        let Some(prop_value) = edge.props.get(prop_key) else {
                            continue;
                        };
                        let Some(encoded) = numeric_range_sort_key_for_value(prop_value) else {
                            continue;
                        };
                        seeded.push((encoded, edge.id, edge.last_write_seq));
                    }
                    for (encoded, edge_id, write_seq) in seeded {
                        state.set_secondary_range_slot(
                            entry.index_id,
                            encoded,
                            edge_id,
                            true,
                            write_seq,
                        );
                    }
                }
            },
            SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => {
                let (lookup_before, lookup_after) = MemtableState::add_field_index_lookup(
                    &mut state.catalog.secondary_node_field_indexes_by_label,
                    *label_id,
                    entry.index_id,
                );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                state
                    .secondary_compound_state
                    .entry(entry.index_id)
                    .or_default();
                let mut seeded = Vec::new();
                for node in state.nodes.values().filter_map(record_current) {
                    if !node.label_ids.contains(*label_id) {
                        continue;
                    }
                    seeded.push((
                        encode_node_compound_tuple_key(*label_id, fields, node),
                        node.id,
                        node.last_write_seq,
                    ));
                }
                for (tuple_key, node_id, write_seq) in seeded {
                    state.set_compound_slot(entry.index_id, tuple_key, node_id, true, write_seq);
                }
            }
            SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => {
                let (lookup_before, lookup_after) = MemtableState::add_field_index_lookup(
                    &mut state.catalog.secondary_edge_field_indexes_by_label,
                    *label_id,
                    entry.index_id,
                );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                state
                    .secondary_compound_state
                    .entry(entry.index_id)
                    .or_default();
                let mut seeded = Vec::new();
                for edge in state.edges.values().filter_map(record_current) {
                    if edge.label_id != *label_id {
                        continue;
                    }
                    seeded.push((
                        encode_edge_compound_tuple_key(*label_id, fields, edge),
                        edge.id,
                        edge.last_write_seq,
                    ));
                }
                for (tuple_key, edge_id, write_seq) in seeded {
                    state.set_compound_slot(entry.index_id, tuple_key, edge_id, true, write_seq);
                }
            }
        }
        Ok(())
    }

    pub fn unregister_secondary_index(&self, index_id: u64) -> bool {
        let mut state = self.state.write().unwrap();
        let Some(entry) = state.catalog.secondary_index_declarations.remove(&index_id) else {
            return false;
        };
        state.estimated_bytes = state
            .estimated_bytes
            .saturating_sub(estimate_secondary_decl_entry(&entry));
        match entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, prop_key } => match entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after, remove_label_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_label_entry = false;
                        if let Some(by_prop) = state.catalog.secondary_eq_by_prop.get_mut(&label_id)
                        {
                            let mut remove_prop_entry = false;
                            if let Some(index_ids) = by_prop.get_mut(&prop_key) {
                                delta.0 = estimate_secondary_eq_lookup_entry(&prop_key, index_ids);
                                index_ids.retain(|&id| id != index_id);
                                if index_ids.is_empty() {
                                    remove_prop_entry = true;
                                } else {
                                    delta.1 =
                                        estimate_secondary_eq_lookup_entry(&prop_key, index_ids);
                                }
                            }
                            if remove_prop_entry {
                                by_prop.remove(&prop_key);
                            }
                            remove_label_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_label_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_label_entry {
                        state.catalog.secondary_eq_by_prop.remove(&label_id);
                    }
                    if let Some(groups) = state.secondary_eq_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_eq_state_groups(&groups));
                    }
                }
                SecondaryIndexKind::Range => {
                    let (lookup_before, lookup_after, remove_label_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_label_entry = false;
                        if let Some(by_prop) =
                            state.catalog.secondary_range_by_prop.get_mut(&label_id)
                        {
                            let mut remove_prop_entry = false;
                            if let Some(indexes) = by_prop.get_mut(&prop_key) {
                                delta.0 = estimate_secondary_range_lookup_entry(&prop_key, indexes);
                                indexes.retain(|&id| id != index_id);
                                if indexes.is_empty() {
                                    remove_prop_entry = true;
                                } else {
                                    delta.1 =
                                        estimate_secondary_range_lookup_entry(&prop_key, indexes);
                                }
                            }
                            if remove_prop_entry {
                                by_prop.remove(&prop_key);
                            }
                            remove_label_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_label_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_label_entry {
                        state.catalog.secondary_range_by_prop.remove(&label_id);
                    }
                    if let Some(entries) = state.secondary_range_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_range_state_entries(&entries));
                    }
                }
            },
            SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => match entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after, remove_label_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_label_entry = false;
                        if let Some(by_prop) =
                            state.catalog.secondary_edge_eq_by_prop.get_mut(&label_id)
                        {
                            let mut remove_prop_entry = false;
                            if let Some(index_ids) = by_prop.get_mut(&prop_key) {
                                delta.0 = estimate_secondary_eq_lookup_entry(&prop_key, index_ids);
                                index_ids.retain(|&id| id != index_id);
                                if index_ids.is_empty() {
                                    remove_prop_entry = true;
                                } else {
                                    delta.1 =
                                        estimate_secondary_eq_lookup_entry(&prop_key, index_ids);
                                }
                            }
                            if remove_prop_entry {
                                by_prop.remove(&prop_key);
                            }
                            remove_label_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_label_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_label_entry {
                        state.catalog.secondary_edge_eq_by_prop.remove(&label_id);
                    }
                    if let Some(groups) = state.secondary_eq_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_eq_state_groups(&groups));
                    }
                }
                SecondaryIndexKind::Range => {
                    let (lookup_before, lookup_after, remove_label_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_label_entry = false;
                        if let Some(by_prop) = state
                            .catalog
                            .secondary_edge_range_by_prop
                            .get_mut(&label_id)
                        {
                            let mut remove_prop_entry = false;
                            if let Some(indexes) = by_prop.get_mut(&prop_key) {
                                delta.0 = estimate_secondary_range_lookup_entry(&prop_key, indexes);
                                indexes.retain(|&id| id != index_id);
                                if indexes.is_empty() {
                                    remove_prop_entry = true;
                                } else {
                                    delta.1 =
                                        estimate_secondary_range_lookup_entry(&prop_key, indexes);
                                }
                            }
                            if remove_prop_entry {
                                by_prop.remove(&prop_key);
                            }
                            remove_label_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_label_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_label_entry {
                        state.catalog.secondary_edge_range_by_prop.remove(&label_id);
                    }
                    if let Some(entries) = state.secondary_range_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_range_state_entries(&entries));
                    }
                }
            },
            SecondaryIndexTarget::NodeFieldIndex { label_id, .. } => {
                let (lookup_before, lookup_after, remove_label_entry) =
                    MemtableState::remove_field_index_lookup(
                        &mut state.catalog.secondary_node_field_indexes_by_label,
                        label_id,
                        index_id,
                    );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                if remove_label_entry {
                    state
                        .catalog
                        .secondary_node_field_indexes_by_label
                        .remove(&label_id);
                }
                if let Some(entries) = state.secondary_compound_state.remove(&index_id) {
                    state.estimated_bytes = state
                        .estimated_bytes
                        .saturating_sub(estimate_compound_state_entries(&entries));
                }
            }
            SecondaryIndexTarget::EdgeFieldIndex { label_id, .. } => {
                let (lookup_before, lookup_after, remove_label_entry) =
                    MemtableState::remove_field_index_lookup(
                        &mut state.catalog.secondary_edge_field_indexes_by_label,
                        label_id,
                        index_id,
                    );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                if remove_label_entry {
                    state
                        .catalog
                        .secondary_edge_field_indexes_by_label
                        .remove(&label_id);
                }
                if let Some(entries) = state.secondary_compound_state.remove(&index_id) {
                    state.estimated_bytes = state
                        .estimated_bytes
                        .saturating_sub(estimate_compound_state_entries(&entries));
                }
            }
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn get_node_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let state = self.primary_read()?;
        Ok(state.node_at(id, snapshot_seq).cloned())
    }

    pub(crate) fn visit_nodes_sorted_at<F>(
        &self,
        sorted_ids: &[u64],
        snapshot_seq: u64,
        vectors: crate::row_projection::VectorSelection,
        remaining: &mut Vec<u64>,
        callback: &mut F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(u64, &NodeVectorRow<'_>),
    {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            let mut position = 0;
            let mut batch = VectorBatch::default();
            while position < sorted_ids.len() {
                batch.clear();
                let state = self.primary_read()?;
                let PrimaryRead::Candidate(maps, session) = &state else {
                    unreachable!("candidate vector source");
                };
                let mut copied = 0usize;
                let mut remaining_requests = sorted_ids.len() - position;
                let consumed = maps.copy_nodes_sorted_at(
                    session,
                    &sorted_ids[position..],
                    snapshot_seq,
                    |id, value| {
                        match value {
                            Some(RecordState::Live(node)) => {
                                let bytes = VectorBatch::copied_bytes(node, vectors);
                                if !batch.rows.is_empty()
                                    && copied.saturating_add(bytes) > 1024 * 1024
                                {
                                    return ControlFlow::Break(false);
                                }
                                batch.push(node, vectors, remaining_requests);
                                #[cfg(test)]
                                self.primary.as_ref().unwrap().note_vector_copy_for_test(
                                    &NodeVectorRow::borrowed(node, vectors),
                                );
                                copied = copied.saturating_add(bytes);
                            }
                            Some(RecordState::Tombstone(_)) => {}
                            None => remaining.push(id),
                        }
                        remaining_requests -= 1;
                        if copied >= 1024 * 1024 {
                            ControlFlow::Break(true)
                        } else {
                            ControlFlow::Continue(())
                        }
                    },
                );
                debug_assert!(consumed > 0, "empty vector refill must advance");
                position += consumed;
                drop(state);
                for row in &batch.rows {
                    callback(row.meta.id, &batch.row(row));
                }
            }
            return Ok(());
        }
        let state = self.state.read().unwrap();
        for &id in sorted_ids {
            if state.node_deleted_at(id, snapshot_seq) {
                continue;
            }
            if let Some(node) = state.node_at(id, snapshot_seq) {
                callback(id, &NodeVectorRow::borrowed(node, vectors));
            } else {
                remaining.push(id);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_edge_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let state = self.primary_read()?;
        Ok(state.edge_at(id, snapshot_seq).cloned())
    }

    pub(crate) fn batch_get_node_selected_fields_at(
        &self,
        lookups: &[(usize, u64)],
        needs: &NodeSelectedFieldNeeds,
        snapshot_seq: u64,
        results: &mut [Option<SelectedNodeFields>],
        #[cfg(test)] selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            match cache.get(&id).copied() {
                Some(CachedLookup::Live(cached_idx)) => {
                    results[orig_idx] = results[cached_idx].clone();
                }
                Some(CachedLookup::Tombstone) => {}
                Some(CachedLookup::Miss) => remaining.push((orig_idx, id)),
                None => {
                    let outcome = match state.node_state(id, snapshot_seq) {
                        Some(RecordState::Live(node)) => {
                            #[cfg(test)]
                            let dense_vector = if needs.vectors.needs_dense() {
                                let vector = node.dense_vector.clone();
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
                            let dense_vector = needs
                                .vectors
                                .needs_dense()
                                .then(|| node.dense_vector.clone())
                                .flatten();
                            #[cfg(test)]
                            let sparse_vector = if needs.vectors.needs_sparse() {
                                let vector = node.sparse_vector.clone();
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
                            let sparse_vector = needs
                                .vectors
                                .needs_sparse()
                                .then(|| node.sparse_vector.clone())
                                .flatten();
                            results[orig_idx] = Some(SelectedNodeFields {
                                meta: NodeMetadataForQuery {
                                    id: node.id,
                                    label_ids: node.label_ids,
                                    updated_at: node.updated_at,
                                    weight: node.weight,
                                },
                                key: needs.key.then(|| node.key.clone()),
                                props: selected_props_from_map(&node.props, &needs.props),
                                created_at: needs.created_at.then_some(node.created_at),
                                dense_vector,
                                sparse_vector,
                            });
                            CachedLookup::Live(orig_idx)
                        }
                        Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }
        Ok(remaining)
    }

    pub(crate) fn batch_get_edge_selected_fields_at(
        &self,
        lookups: &[(usize, u64)],
        needs: &EdgeSelectedFieldNeeds,
        snapshot_seq: u64,
        results: &mut [Option<SelectedEdgeFields>],
        #[cfg(test)] _selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            match cache.get(&id).copied() {
                Some(CachedLookup::Live(cached_idx)) => {
                    results[orig_idx] = results[cached_idx].clone();
                }
                Some(CachedLookup::Tombstone) => {}
                Some(CachedLookup::Miss) => remaining.push((orig_idx, id)),
                None => {
                    let outcome = match state.edge_state(id, snapshot_seq) {
                        Some(RecordState::Live(edge)) => {
                            results[orig_idx] = Some(SelectedEdgeFields {
                                meta: EdgeMetadataForQuery::from(edge),
                                props: selected_props_from_map(&edge.props, &needs.props),
                                created_at: needs.created_at.then_some(edge.created_at),
                            });
                            CachedLookup::Live(orig_idx)
                        }
                        Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }
        Ok(remaining)
    }

    pub(crate) fn edge_core_opinion_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<PrimaryOpinion<EdgeCoreFields>, EngineError> {
        let state = self.primary_read()?;
        Ok(match state.edge_state(id, snapshot_seq) {
            None => PrimaryOpinion::Missing,
            Some(RecordState::Tombstone(t)) => PrimaryOpinion::Removed(*t),
            Some(RecordState::Live(edge)) => PrimaryOpinion::Live((
                edge.from,
                edge.to,
                edge.created_at,
                edge.updated_at,
                edge.weight,
                edge.valid_from,
                edge.valid_to,
            )),
        })
    }

    pub(crate) fn batch_get_edge_metadata_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [crate::edge_metadata::EdgeMetadataVisibilityState],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        use crate::edge_metadata::EdgeMetadataVisibilityState;

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            let visibility = match cache.get(&id).copied() {
                Some(visibility) => visibility,
                None => {
                    let visibility = match state.edge_state(id, snapshot_seq) {
                        Some(RecordState::Live(edge)) => EdgeMetadataVisibilityState::Live(
                            EdgeMetadataCandidate::from_edge(edge),
                        ),
                        Some(RecordState::Tombstone(_)) => EdgeMetadataVisibilityState::Deleted,
                        None if state.missing_edge_deleted_at(id, snapshot_seq) => {
                            EdgeMetadataVisibilityState::Deleted
                        }
                        None => EdgeMetadataVisibilityState::Missing,
                    };
                    cache.insert(id, visibility);
                    visibility
                }
            };
            results[orig_idx] = visibility;
            if visibility == EdgeMetadataVisibilityState::Missing {
                remaining.push((orig_idx, id));
            }
        }
        Ok(remaining)
    }

    pub(crate) fn batch_get_node_visibility_meta_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [NodeVisibilityState],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            let visibility = match cache.get(&id).copied() {
                Some(state) => state,
                None => {
                    let state = match state.node_state(id, snapshot_seq) {
                        Some(RecordState::Live(node)) => {
                            NodeVisibilityState::Live(NodeVisibilityMeta {
                                label_ids: node.label_ids,
                                updated_at: node.updated_at,
                                weight: node.weight,
                            })
                        }
                        Some(RecordState::Tombstone(_)) => NodeVisibilityState::Deleted,
                        None if state.missing_node_deleted_at(id, snapshot_seq) => {
                            NodeVisibilityState::Deleted
                        }
                        None => NodeVisibilityState::Missing,
                    };
                    cache.insert(id, state);
                    state
                }
            };

            match visibility {
                NodeVisibilityState::Live(_) | NodeVisibilityState::Deleted => {
                    results[orig_idx] = visibility;
                }
                NodeVisibilityState::Missing => remaining.push((orig_idx, id)),
            }
        }
        Ok(remaining)
    }

    pub(crate) fn edge_visibility_state_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<EdgeVisibilityState, EngineError> {
        let state = self.primary_read()?;
        Ok(match state.edge_state(id, snapshot_seq) {
            Some(RecordState::Live(_)) => EdgeVisibilityState::Live,
            Some(RecordState::Tombstone(_)) => EdgeVisibilityState::Deleted,
            None if state.missing_edge_deleted_at(id, snapshot_seq) => EdgeVisibilityState::Deleted,
            None => EdgeVisibilityState::Missing,
        })
    }

    pub(crate) fn batch_get_nodes_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [Option<NodeRecord>],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            match cache.get(&id).copied() {
                Some(CachedLookup::Live(cached_idx)) => {
                    results[orig_idx] = results[cached_idx].clone();
                }
                Some(CachedLookup::Tombstone) => {}
                Some(CachedLookup::Miss) => remaining.push((orig_idx, id)),
                None => {
                    let outcome = match state.node_state(id, snapshot_seq) {
                        Some(RecordState::Live(node)) => {
                            results[orig_idx] = Some(node.clone());
                            CachedLookup::Live(orig_idx)
                        }
                        Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }
        Ok(remaining)
    }

    pub(crate) fn batch_get_edges_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [Option<EdgeRecord>],
    ) -> Result<Vec<(usize, u64)>, EngineError> {
        let state = self.primary_read()?;
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            match cache.get(&id).copied() {
                Some(CachedLookup::Live(cached_idx)) => {
                    results[orig_idx] = results[cached_idx].clone();
                }
                Some(CachedLookup::Tombstone) => {}
                Some(CachedLookup::Miss) => remaining.push((orig_idx, id)),
                None => {
                    let outcome = match state.edge_state(id, snapshot_seq) {
                        Some(RecordState::Live(edge)) => {
                            results[orig_idx] = Some(edge.clone());
                            CachedLookup::Live(orig_idx)
                        }
                        Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }
        Ok(remaining)
    }

    pub(crate) fn is_node_deleted_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<bool, EngineError> {
        let state = self.primary_read()?;
        Ok(state.node_deleted_at(id, snapshot_seq))
    }

    pub(crate) fn is_edge_deleted_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<bool, EngineError> {
        let state = self.primary_read()?;
        Ok(state.edge_deleted_at(id, snapshot_seq))
    }

    #[cfg(test)]
    pub(crate) fn node_tombstone_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<Option<TombstoneEntry>, EngineError> {
        let state = self.primary_read()?;
        Ok(match state.node_state(id, snapshot_seq) {
            Some(RecordState::Tombstone(entry)) => Some(*entry),
            _ => None,
        })
    }

    #[cfg(all(test, feature = "production-concurrency"))]
    pub(crate) fn edge_tombstone_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<Option<TombstoneEntry>, EngineError> {
        let state = self.primary_read()?;
        Ok(match state.edge_state(id, snapshot_seq) {
            Some(RecordState::Tombstone(entry)) => Some(*entry),
            _ => None,
        })
    }

    #[cfg(test)]
    pub(crate) fn node_by_key_at(
        &self,
        label_id: u32,
        key: &str,
        snapshot_seq: u64,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let state = self.primary_read()?;
        Ok(state
            .key(label_id, key, snapshot_seq)
            .flatten()
            .and_then(|id| state.node_at(id, snapshot_seq))
            .cloned())
    }

    #[cfg(test)]
    pub(crate) fn edge_by_triple_at(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let state = self.primary_read()?;
        Ok(state
            .triple((from, to, label_id), snapshot_seq)
            .flatten()
            .and_then(|id| state.edge_at(id, snapshot_seq))
            .cloned())
    }

    pub(crate) fn for_each_visible_node_at<F>(
        &self,
        snapshot_seq: u64,
        vectors: crate::row_projection::VectorSelection,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(&NodeVectorRow<'_>) -> ControlFlow<()>,
    {
        #[cfg(feature = "production-concurrency")]
        if let Some(maps) = &self.primary {
            let upper = self.max_node_id();
            let mut after = None;
            loop {
                let (records, last, exhausted) =
                    maps.node_refill(snapshot_seq, (after, upper), vectors)?;
                for node in records.iter() {
                    if callback(node).is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                if exhausted {
                    return Ok(ControlFlow::Continue(()));
                }
                debug_assert!(last > after);
                after = last;
            }
        }
        let state = self.state.read().unwrap();
        for slot in state.nodes.values() {
            if let Some(RecordState::Live(node)) = record_at(slot, snapshot_seq) {
                if callback(&NodeVectorRow::borrowed(node, vectors)).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    pub(crate) fn refill_endpoint_ids_at(
        &self,
        owner: u64,
        outgoing: bool,
        labels: Option<&[u32]>,
        seq: u64,
        bound: Bound<u64>,
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_session(|m, s| {
                let r = m.graph.adj_refill(
                    s,
                    usize::from(!outgoing),
                    owner,
                    seq,
                    bound,
                    labels,
                    128,
                    Vec::new(),
                );
                #[cfg(test)]
                ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST
                    .with(|count| count.set(count.get() + r.examined));
                let ids = if m.graph_node_deleted(s, owner, seq) {
                    Vec::new()
                } else {
                    r.entries
                        .into_iter()
                        .filter(|e| !m.graph_node_deleted(s, e.neighbor_id, seq))
                        .map(|e| e.edge_id)
                        .collect()
                };
                (ids, r.last_edge_id_examined, r.exhausted)
            });
        }
        let r = self.refill_ordered_adj_postings_at(
            if outgoing {
                Direction::Outgoing
            } else {
                Direction::Incoming
            },
            owner,
            labels,
            seq,
            bound,
            128,
            Vec::new(),
        )?;
        #[cfg(test)]
        ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.with(|count| count.set(count.get() + r.examined));
        let state = self.state.read().unwrap();
        let ids = if state.node_deleted_at(owner, seq) {
            Vec::new()
        } else {
            r.entries
                .into_iter()
                .filter(|e| !state.node_deleted_at(e.neighbor_id, seq))
                .map(|e| e.edge_id)
                .collect()
        };
        Ok((ids, r.last_edge_id_examined, r.exhausted))
    }
    pub(crate) fn refill_label_ids_at(
        &self,
        edge: bool,
        label: u32,
        seq: u64,
        bound: Bound<u64>,
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_session(|m, s| {
                let (rows, w) = m.graph.label_refill(
                    s,
                    usize::from(edge),
                    Some(label),
                    seq,
                    bound.map(|id| (label, id)),
                    128,
                );
                (
                    rows.into_iter().map(|(_, id)| id).collect(),
                    w.last.map(|(_, id)| id),
                    w.exhausted,
                )
            });
        }
        let state = self.state.read().unwrap();
        let index = if edge {
            &state.ordered_label_edge_index
        } else {
            &state.label_node_index
        };
        let start = bound.map(|id| (label, id));
        let start = if matches!(start, Bound::Unbounded) {
            Bound::Included((label, 0))
        } else {
            start
        };
        let mut iter = index.range((start, Bound::Included((label, u64::MAX))));
        let mut rows = Vec::new();
        let mut last = None;
        for _ in 0..128 {
            let Some((&(_, id), slot)) = iter.next() else {
                return Ok((rows, last, true));
            };
            last = Some(id);
            if slot_option_visible(slot, seq) {
                rows.push(id);
            }
        }
        Ok((rows, last, false))
    }
    #[cfg(feature = "production-concurrency")]
    pub(crate) fn node_id_scan_upper_bound(&self) -> Option<u64> {
        self.primary
            .as_ref()
            .map(|maps| maps.max_node.load(std::sync::atomic::Ordering::Relaxed))
    }

    #[cfg(feature = "production-concurrency")]
    pub(crate) fn refill_node_ids_at(
        &self,
        seq: u64,
        bound: Bound<u64>,
        upper: u64,
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        let after = match bound {
            Bound::Included(id) => id.checked_sub(1),
            Bound::Excluded(id) => Some(id),
            Bound::Unbounded => None,
        };
        self.primary
            .as_ref()
            .expect("candidate node ID scan")
            .node_ids_refill(seq, (after, upper))
    }

    pub(crate) fn refill_edge_ids_at(
        &self,
        seq: u64,
        bound: Bound<u64>,
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_session(|m, s| {
                let mut ids = Vec::new();
                let after = match bound {
                    Bound::Included(id) => id.checked_sub(1),
                    Bound::Excluded(id) => Some(id),
                    Bound::Unbounded => None,
                };
                let (last, done) =
                    m.edges
                        .walk_versions_at(s, seq, (after, u64::MAX), 128, |id, value| {
                            if matches!(&**value, RecordState::Live(_)) {
                                ids.push(id);
                            }
                            true
                        });
                (ids, last, done)
            });
        }
        let state = self.state.read().unwrap();
        let mut iter = state.ordered_edge_ids.range((bound, Bound::Unbounded));
        let mut ids = Vec::new();
        let mut last = None;
        for _ in 0..128 {
            let Some((&id, slot)) = iter.next() else {
                return Ok((ids, last, true));
            };
            last = Some(id);
            if slot_option_visible(slot, seq) {
                ids.push(id);
            }
        }
        Ok((ids, last, false))
    }
    pub(crate) fn visible_nodes_by_label_id(
        &self,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return Ok(self
                .graph_labels(0, Some(label_id), snapshot_seq)?
                .into_iter()
                .map(|(_, id)| id)
                .collect());
        }
        Ok({
            let state = self.state.read().unwrap();
            let mut ids = Vec::new();
            for (&(_, node_id), slot) in state
                .label_node_index
                .range((label_id, 0)..=(label_id, u64::MAX))
            {
                if slot_option_visible(slot, snapshot_seq) {
                    ids.push(node_id);
                }
            }
            ids
        })
    }

    pub(crate) fn visible_node_ids_at(&self, snapshot_seq: u64) -> Result<Vec<u64>, EngineError> {
        let state = self.primary_read()?;
        let mut ids = Vec::new();
        state.nodes_at(snapshot_seq, |id, value| {
            if matches!(value, RecordState::Live(_)) {
                ids.push(id);
            }
        });
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn visible_node_count_at(&self, snapshot_seq: u64) -> Result<usize, EngineError> {
        let state = self.primary_read()?;
        let mut count = 0;
        #[cfg(test)]
        note_query_scan_work(|work| work.full_node_count_calls += 1);
        state.nodes_at(snapshot_seq, |_, value| {
            #[cfg(test)]
            note_query_scan_work(|work| work.full_node_count_opinions += 1);
            if matches!(value, RecordState::Live(_)) {
                count += 1;
            }
        });
        Ok(count)
    }

    pub(crate) fn visible_nodes_by_label_id_count(
        &self,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_label_count(label_id, snapshot_seq);
        }
        Ok({
            let state = self.state.read().unwrap();
            state
                .label_node_index
                .range((label_id, 0)..=(label_id, u64::MAX))
                .filter(|(_, slot)| slot_option_visible(slot, snapshot_seq))
                .count()
        })
    }

    pub(crate) fn visible_edges_by_label_id(
        &self,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return Ok(self
                .graph_labels(1, Some(label_id), snapshot_seq)?
                .into_iter()
                .map(|(_, id)| id)
                .collect());
        }
        Ok({
            let state = self.state.read().unwrap();
            let mut ids = Vec::new();
            if let Some(members) = state.label_edge_index.get(&label_id) {
                for (&edge_id, slot) in members {
                    if slot_option_visible(slot, snapshot_seq) {
                        ids.push(edge_id);
                    }
                }
            }
            ids.sort_unstable();
            ids
        })
    }

    pub(crate) fn edge_label_posting_count_upper_bound(
        &self,
        label_id: u32,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_session(|m, s| m.graph.label_bound(s, 1, label_id));
        }
        Ok({
            let state = self.state.read().unwrap();
            state
                .label_edge_index
                .get(&label_id)
                .map_or(0, NodeIdMap::len)
        })
    }

    #[cfg(test)]
    pub(crate) fn visible_edge_ids_at(&self, snapshot_seq: u64) -> Result<Vec<u64>, EngineError> {
        let state = self.primary_read()?;
        let mut ids = Vec::new();
        state.edges_at(snapshot_seq, |id, value| {
            if matches!(value, RecordState::Live(_)) {
                ids.push(id);
            }
        });
        ids.sort_unstable();
        Ok(ids)
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_node_by_label_id_after(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_label_next(
                0,
                label_id,
                snapshot_seq,
                after.map_or(Bound::Unbounded, Bound::Excluded),
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let start_key = (label_id, after.unwrap_or(0));
            let start = match after {
                Some(_) => Bound::Excluded(start_key),
                None => Bound::Included(start_key),
            };
            let end = Bound::Included((label_id, u64::MAX));
            for (&(_, node_id), slot) in state.label_node_index.range((start, end)) {
                if slot_option_visible(slot, snapshot_seq) {
                    return Some(node_id);
                }
            }
            None
        })())
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_node_by_label_id_ge(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        bound: u64,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_label_next(0, label_id, snapshot_seq, Bound::Included(bound));
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let start = Bound::Included((label_id, bound));
            let end = Bound::Included((label_id, u64::MAX));
            for (&(_, node_id), slot) in state.label_node_index.range((start, end)) {
                if slot_option_visible(slot, snapshot_seq) {
                    return Some(node_id);
                }
            }
            None
        })())
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_edge_by_label_id_after(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_label_next(
                1,
                label_id,
                snapshot_seq,
                after.map_or(Bound::Unbounded, Bound::Excluded),
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let start_key = (label_id, after.unwrap_or(0));
            let start = match after {
                Some(_) => Bound::Excluded(start_key),
                None => Bound::Included(start_key),
            };
            let end = Bound::Included((label_id, u64::MAX));
            for (&(_, edge_id), slot) in state.ordered_label_edge_index.range((start, end)) {
                if slot_option_visible(slot, snapshot_seq) {
                    return Some(edge_id);
                }
            }
            None
        })())
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_edge_by_label_id_ge(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        bound: u64,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_label_next(1, label_id, snapshot_seq, Bound::Included(bound));
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let start = Bound::Included((label_id, bound));
            let end = Bound::Included((label_id, u64::MAX));
            for (&(_, edge_id), slot) in state.ordered_label_edge_index.range((start, end)) {
                if slot_option_visible(slot, snapshot_seq) {
                    return Some(edge_id);
                }
            }
            None
        })())
    }

    pub(crate) fn edge_ids_by_triple_at(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        let mut ids = Vec::new();
        let mut bound = Bound::Unbounded;
        loop {
            let r = self.refill_ordered_adj_postings_at(
                Direction::Outgoing,
                from,
                Some(&[label_id]),
                snapshot_seq,
                bound,
                4096,
                Vec::new(),
            )?;
            ids.extend(
                r.entries
                    .into_iter()
                    .filter(|e| e.neighbor_id == to)
                    .map(|e| e.edge_id),
            );
            if r.exhausted {
                return Ok(ids);
            }
            bound = Bound::Excluded(
                r.last_edge_id_examined
                    .expect("nonterminal triple refill progresses"),
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn edge_metadata_scan_ids_at<F>(
        &self,
        snapshot_seq: u64,
        mut predicate: F,
    ) -> Result<Vec<u64>, EngineError>
    where
        F: FnMut(EdgeMetadataCandidate) -> bool,
    {
        let state = self.primary_read()?;
        let mut copies = Vec::new();
        state.edges_at(snapshot_seq, |id, value| {
            if let RecordState::Live(e) = value {
                copies.push((id, EdgeMetadataCandidate::from_edge(e)));
            }
        });
        drop(state);
        let mut ids = copies
            .into_iter()
            .filter_map(|(id, m)| predicate(m).then_some(id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn for_each_edge_metadata_at<F>(
        &self,
        snapshot_seq: u64,
        mut callback: F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(EdgeMetadataCandidate) -> ControlFlow<()>,
    {
        #[cfg(feature = "production-concurrency")]
        if let Some(maps) = &self.primary {
            let upper = self.max_edge_id();
            let mut after = None;
            loop {
                let (records, last, exhausted) =
                    maps.edge_metadata_refill(snapshot_seq, (after, upper))?;
                for meta in records {
                    if callback(meta).is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                if exhausted {
                    return Ok(ControlFlow::Continue(()));
                }
                debug_assert!(last > after);
                after = last;
            }
        }
        let state = self.state.read().unwrap();
        for slot in state.edges.values() {
            if let Some(RecordState::Live(edge)) = record_at(slot, snapshot_seq) {
                if callback(EdgeMetadataCandidate::from_edge(edge)).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Trusted planner predicate only: count borrowed metadata without result transport.
    pub(crate) fn count_edge_metadata_at(
        &self,
        snapshot_seq: u64,
        matches: impl Fn(EdgeMetadataCandidate) -> bool,
    ) -> Result<u64, EngineError> {
        let mut count = 0u64;
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            let upper = self.max_edge_id();
            let mut after = None;
            loop {
                let (last, exhausted) = self.graph_session(|maps, session| {
                    maps.edges.walk_versions_at(
                        session,
                        snapshot_seq,
                        (after, upper),
                        4096,
                        |_, value| {
                            if let RecordState::Live(edge) = &**value {
                                if matches(EdgeMetadataCandidate::from_edge(edge)) {
                                    count = count.saturating_add(1);
                                }
                            }
                            true
                        },
                    )
                })?;
                #[cfg(test)]
                note_query_scan_work(|work| work.edge_count_refills += 1);
                if exhausted {
                    return Ok(count);
                }
                debug_assert!(last > after);
                after = last;
            }
        }
        let state = self.state.read().unwrap();
        for slot in state.edges.values() {
            if let Some(RecordState::Live(edge)) = record_at(slot, snapshot_seq) {
                if matches(EdgeMetadataCandidate::from_edge(edge)) {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_weight_range_at(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.edge_metadata_scan_ids_at(snapshot_seq, |meta| {
            label_id.is_none_or(|target| meta.label_id == target)
                && weight_matches_bounds(meta.weight, bounds)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_updated_at_range_at(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.edge_metadata_scan_ids_at(snapshot_seq, |meta| {
            label_id.is_none_or(|target| meta.label_id == target)
                && i64_matches_bounds(meta.updated_at, bounds)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_valid_from_range_at(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.edge_metadata_scan_ids_at(snapshot_seq, |meta| {
            label_id.is_none_or(|target| meta.label_id == target)
                && i64_matches_bounds(meta.valid_from, bounds)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_valid_to_range_at(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.edge_metadata_scan_ids_at(snapshot_seq, |meta| {
            label_id.is_none_or(|target| meta.label_id == target)
                && i64_matches_bounds(meta.valid_to, bounds)
        })
        .unwrap()
    }

    pub(crate) fn visible_nodes_by_time_range(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                let mut ids = Vec::new();
                let _ =
                    self.graph_time_each(label_id, from_ms, to_ms, snapshot_seq, &mut |id| {
                        ids.push(id);
                        ControlFlow::Continue(())
                    })?;
                ids.sort_unstable();
                Ok(ids)
            };
        }
        Ok((|| {
            if from_ms > to_ms {
                return Vec::new();
            }
            let state = self.state.read().unwrap();
            use std::ops::Bound;
            let start = (label_id, from_ms, 0u64);
            let end = (label_id, to_ms, u64::MAX);
            let mut ids = state
                .time_node_index
                .range((Bound::Included(start), Bound::Included(end)))
                .filter_map(|(&(entry_label_id, _, node_id), slot)| {
                    (entry_label_id == label_id && slot_option_visible(slot, snapshot_seq))
                        .then_some(node_id)
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        })())
    }

    pub(crate) fn visible_node_time_range_count_at(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        let mut count = 0usize;
        let _ = self.for_each_visible_node_by_time_range_at(
            label_id,
            from_ms,
            to_ms,
            snapshot_seq,
            &mut |_| {
                count = count.saturating_add(1);
                ControlFlow::Continue(())
            },
        )?;
        Ok(count)
    }

    pub(crate) fn for_each_visible_node_by_time_range_at<F>(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_time_each(label_id, from_ms, to_ms, snapshot_seq, callback);
        }
        Ok((|| {
            if from_ms > to_ms {
                return ControlFlow::Continue(());
            }
            let state = self.state.read().unwrap();
            use std::ops::Bound;
            let start = (label_id, from_ms, 0u64);
            let end = (label_id, to_ms, u64::MAX);
            for (&(entry_label_id, _, node_id), slot) in state
                .time_node_index
                .range((Bound::Included(start), Bound::Included(end)))
            {
                if entry_label_id != label_id || !slot_option_visible(slot, snapshot_seq) {
                    continue;
                }
                if callback(node_id).is_break() {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        })())
    }

    pub(crate) fn neighbors_at(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
        snapshot_seq: u64,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_neighbors_one(
                node_id,
                direction,
                label_filter_ids,
                limit,
                snapshot_seq,
                true,
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            if state.node_deleted_at(node_id, snapshot_seq) {
                return Vec::new();
            }

            let mut results = Vec::new();
            let mut self_loop_edge_ids = NodeIdSet::default();
            let mut collect = |map: Option<&NodeIdMap<MembershipSlot<AdjEntry>>>,
                               dedupe_self_loops: bool,
                               results: &mut Vec<NeighborRecord>| {
                let Some(map) = map else {
                    return;
                };
                for slot in map.values() {
                    if limit > 0 && results.len() >= limit {
                        break;
                    }
                    let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                        continue;
                    };
                    if label_filter_ids
                        .is_some_and(|label_ids| !label_ids.contains(&entry.label_id))
                    {
                        continue;
                    }
                    if state.node_deleted_at(entry.neighbor_id, snapshot_seq) {
                        continue;
                    }
                    if dedupe_self_loops && entry.neighbor_id == node_id {
                        if !self_loop_edge_ids.insert(entry.edge_id) {
                            continue;
                        }
                    } else if entry.neighbor_id == node_id {
                        self_loop_edge_ids.insert(entry.edge_id);
                    }
                    results.push(NeighborRecord {
                        node_id: entry.neighbor_id,
                        edge_id: entry.edge_id,
                        edge_label_id: entry.label_id,
                        weight: entry.weight,
                        valid_from: entry.valid_from,
                        valid_to: entry.valid_to,
                    });
                }
            };

            match direction {
                Direction::Outgoing => {
                    collect(state.adj_out.get(&node_id), false, &mut results);
                }
                Direction::Incoming => {
                    collect(state.adj_in.get(&node_id), false, &mut results);
                }
                Direction::Both => {
                    collect(state.adj_out.get(&node_id), false, &mut results);
                    collect(state.adj_in.get(&node_id), true, &mut results);
                }
            }

            results
        })())
    }

    #[cfg(debug_assertions)]
    pub(crate) fn incident_edges_at(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_neighbors_one(
                node_id,
                direction,
                label_filter_ids,
                0,
                snapshot_seq,
                false,
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            if state.node_deleted_at(node_id, snapshot_seq) {
                return Vec::new();
            }

            let mut results = Vec::new();
            let mut self_loop_edge_ids = NodeIdSet::default();
            let mut collect = |map: Option<&NodeIdMap<MembershipSlot<AdjEntry>>>,
                               dedupe_self_loops: bool,
                               results: &mut Vec<NeighborRecord>| {
                let Some(map) = map else {
                    return;
                };
                for slot in map.values() {
                    let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                        continue;
                    };
                    if label_filter_ids
                        .is_some_and(|label_ids| !label_ids.contains(&entry.label_id))
                    {
                        continue;
                    }
                    if dedupe_self_loops && entry.neighbor_id == node_id {
                        if !self_loop_edge_ids.insert(entry.edge_id) {
                            continue;
                        }
                    } else if entry.neighbor_id == node_id {
                        self_loop_edge_ids.insert(entry.edge_id);
                    }
                    results.push(NeighborRecord {
                        node_id: entry.neighbor_id,
                        edge_id: entry.edge_id,
                        edge_label_id: entry.label_id,
                        weight: entry.weight,
                        valid_from: entry.valid_from,
                        valid_to: entry.valid_to,
                    });
                }
            };

            match direction {
                Direction::Outgoing => {
                    collect(state.adj_out.get(&node_id), false, &mut results);
                }
                Direction::Incoming => {
                    collect(state.adj_in.get(&node_id), false, &mut results);
                }
                Direction::Both => {
                    collect(state.adj_out.get(&node_id), false, &mut results);
                    collect(state.adj_in.get(&node_id), true, &mut results);
                }
            }

            results
        })())
    }

    pub(crate) fn adj_stats_at(
        &self,
        node_id: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        seq: u64,
        reference_time: i64,
    ) -> Result<(u64, f64), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_adj_stats(node_id, direction, labels, seq, reference_time);
        }
        let mut count = 0;
        let mut weight = 0.0;
        let _ = self.for_each_adj_entry_at(
            node_id,
            direction,
            labels,
            seq,
            &mut |_, _, w, from, to| {
                if from <= reference_time && reference_time < to {
                    count += 1;
                    weight += w as f64;
                }
                ControlFlow::Continue(())
            },
        )?;
        Ok((count, weight))
    }
    pub(crate) fn for_each_adj_entry_at<F>(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_adj_each(
                node_id,
                direction,
                label_filter_ids,
                snapshot_seq,
                callback,
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            if state.node_deleted_at(node_id, snapshot_seq) {
                return ControlFlow::Continue(());
            }

            let mut self_loop_edge_ids = NodeIdSet::default();
            let mut visit = |map: Option<&NodeIdMap<MembershipSlot<AdjEntry>>>,
                             dedupe_self_loops: bool| {
                let Some(map) = map else {
                    return ControlFlow::Continue(());
                };
                for slot in map.values() {
                    let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                        continue;
                    };
                    if label_filter_ids
                        .is_some_and(|label_ids| !label_ids.contains(&entry.label_id))
                    {
                        continue;
                    }
                    if state.node_deleted_at(entry.neighbor_id, snapshot_seq) {
                        continue;
                    }
                    if dedupe_self_loops && entry.neighbor_id == node_id {
                        if !self_loop_edge_ids.insert(entry.edge_id) {
                            continue;
                        }
                    } else if entry.neighbor_id == node_id {
                        self_loop_edge_ids.insert(entry.edge_id);
                    }
                    if callback(
                        entry.edge_id,
                        entry.neighbor_id,
                        entry.weight,
                        entry.valid_from,
                        entry.valid_to,
                    )
                    .is_break()
                    {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            };

            match direction {
                Direction::Outgoing => visit(state.adj_out.get(&node_id), false),
                Direction::Incoming => visit(state.adj_in.get(&node_id), false),
                Direction::Both => {
                    visit(state.adj_out.get(&node_id), false)?;
                    visit(state.adj_in.get(&node_id), true)
                }
            }
        })())
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    fn next_visible_adj_edge_after(
        state: &MemtableState,
        node_id: u64,
        outgoing: bool,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
        if state.node_deleted_at(node_id, snapshot_seq) {
            return None;
        }
        let source = if outgoing {
            &state.ordered_adj_out
        } else {
            &state.ordered_adj_in
        };
        let entries = source.get(&node_id)?;
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        for (&edge_id, slot) in entries.range((start, Bound::Unbounded)) {
            #[cfg(test)]
            ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.with(|count| count.set(count.get() + 1));
            let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                continue;
            };
            if label_filter_ids.is_some_and(|label_ids| !label_ids.contains(&entry.label_id)) {
                continue;
            }
            if state.node_deleted_at(entry.neighbor_id, snapshot_seq) {
                continue;
            }
            return Some(edge_id);
        }
        None
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    fn next_visible_adj_edge_ge_in_state(
        state: &MemtableState,
        node_id: u64,
        outgoing: bool,
        label_filter_id: Option<u32>,
        snapshot_seq: u64,
        bound: u64,
    ) -> Option<u64> {
        if state.node_deleted_at(node_id, snapshot_seq) {
            return None;
        }
        let source = if outgoing {
            &state.ordered_adj_out
        } else {
            &state.ordered_adj_in
        };
        let entries = source.get(&node_id)?;
        for (&edge_id, slot) in entries.range((Bound::Included(bound), Bound::Unbounded)) {
            #[cfg(test)]
            ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.with(|count| count.set(count.get() + 1));
            let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                continue;
            };
            if label_filter_id.is_some_and(|label_id| entry.label_id != label_id) {
                continue;
            }
            if state.node_deleted_at(entry.neighbor_id, snapshot_seq) {
                continue;
            }
            return Some(edge_id);
        }
        None
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_adj_edge_ge(
        &self,
        node_id: u64,
        outgoing: bool,
        label_filter_id: Option<u32>,
        snapshot_seq: u64,
        bound: u64,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_next(
                node_id,
                usize::from(!outgoing),
                label_filter_id.as_ref().map(std::slice::from_ref),
                snapshot_seq,
                Bound::Included(bound),
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::next_visible_adj_edge_ge_in_state(
                &state,
                node_id,
                outgoing,
                label_filter_id,
                snapshot_seq,
                bound,
            )
        })
    }

    fn visible_adj_edge_count_estimate(
        state: &MemtableState,
        node_id: u64,
        outgoing: bool,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        if label_filter_ids.is_some_and(<[u32]>::is_empty)
            || state.node_deleted_at(node_id, snapshot_seq)
        {
            return MemtableEndpointCountEstimate {
                count: 0,
                exact: true,
            };
        }
        let source = if outgoing {
            &state.ordered_adj_out
        } else {
            &state.ordered_adj_in
        };
        let Some(entries) = source.get(&node_id) else {
            return MemtableEndpointCountEstimate {
                count: 0,
                exact: true,
            };
        };
        MemtableEndpointCountEstimate {
            count: entries.len(),
            exact: entries.is_empty(),
        }
    }

    fn visible_adj_edges_count_estimate(
        state: &MemtableState,
        node_ids: &[u64],
        outgoing: bool,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        if node_ids.is_empty() || label_filter_ids.is_some_and(<[u32]>::is_empty) {
            return MemtableEndpointCountEstimate::exact_zero();
        }
        let mut estimate = MemtableEndpointCountEstimate::exact_zero();
        for &node_id in node_ids {
            estimate.add_assign(Self::visible_adj_edge_count_estimate(
                state,
                node_id,
                outgoing,
                label_filter_ids,
                snapshot_seq,
            ));
        }
        estimate
    }

    fn visible_endpoint_edges_count_estimate(
        state: &MemtableState,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        match direction {
            Direction::Outgoing => Self::visible_adj_edges_count_estimate(
                state,
                node_ids,
                true,
                label_filter_ids,
                snapshot_seq,
            ),
            Direction::Incoming => Self::visible_adj_edges_count_estimate(
                state,
                node_ids,
                false,
                label_filter_ids,
                snapshot_seq,
            ),
            Direction::Both => {
                let mut estimate = Self::visible_adj_edges_count_estimate(
                    state,
                    node_ids,
                    true,
                    label_filter_ids,
                    snapshot_seq,
                );
                estimate.add_assign(Self::visible_adj_edges_count_estimate(
                    state,
                    node_ids,
                    false,
                    label_filter_ids,
                    snapshot_seq,
                ));
                estimate
            }
        }
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_edge_from_endpoint_after(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_next(
                node_id,
                0,
                label_filter_ids,
                snapshot_seq,
                after.map_or(Bound::Unbounded, Bound::Excluded),
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::next_visible_adj_edge_after(
                &state,
                node_id,
                true,
                label_filter_ids,
                snapshot_seq,
                after,
            )
        })
    }

    #[allow(dead_code)] // Scalar compatibility helpers; production scans retain ID chunks.
    pub(crate) fn next_visible_edge_to_endpoint_after(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Result<Option<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_next(
                node_id,
                1,
                label_filter_ids,
                snapshot_seq,
                after.map_or(Bound::Unbounded, Bound::Excluded),
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::next_visible_adj_edge_after(
                &state,
                node_id,
                false,
                label_filter_ids,
                snapshot_seq,
                after,
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn visible_edges_from_endpoint_count_estimate(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Result<MemtableEndpointCountEstimate, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_estimate(
                &[node_id],
                Direction::Outgoing,
                label_filter_ids,
                snapshot_seq,
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::visible_adj_edge_count_estimate(
                &state,
                node_id,
                true,
                label_filter_ids,
                snapshot_seq,
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn visible_edges_to_endpoint_count_estimate(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Result<MemtableEndpointCountEstimate, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_estimate(
                &[node_id],
                Direction::Incoming,
                label_filter_ids,
                snapshot_seq,
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::visible_adj_edge_count_estimate(
                &state,
                node_id,
                false,
                label_filter_ids,
                snapshot_seq,
            )
        })
    }

    pub(crate) fn visible_endpoint_edges_count_estimate_at(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Result<MemtableEndpointCountEstimate, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_endpoint_estimate(
                node_ids,
                direction,
                label_filter_ids,
                snapshot_seq,
            );
        }
        Ok({
            let state = self.state.read().unwrap();
            Self::visible_endpoint_edges_count_estimate(
                &state,
                node_ids,
                direction,
                label_filter_ids,
                snapshot_seq,
            )
        })
    }

    pub(crate) fn visible_node_label_ids(
        &self,
        snapshot_seq: u64,
    ) -> Result<Vec<u32>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                let mut ids = self
                    .graph_labels(0, None, snapshot_seq)?
                    .into_iter()
                    .map(|(label, _)| label)
                    .collect::<Vec<_>>();
                ids.dedup();
                Ok(ids)
            };
        }
        Ok({
            let state = self.state.read().unwrap();
            let mut label_ids = Vec::new();
            let mut current_label_id = None;
            let mut current_label_visible = false;
            for (&(label_id, _), slot) in &state.label_node_index {
                if current_label_id != Some(label_id) {
                    if current_label_visible {
                        label_ids.push(current_label_id.expect("label group is present"));
                    }
                    current_label_id = Some(label_id);
                    current_label_visible = false;
                }
                current_label_visible |= slot_option_visible(slot, snapshot_seq);
            }
            if current_label_visible {
                label_ids.push(current_label_id.expect("label group is present"));
            }
            label_ids
        })
    }

    pub(crate) fn find_secondary_eq_nodes_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_secondary_eq_nodes_at_limited(index_id, prop_key, prop_value, snapshot_seq, None)
    }

    pub(crate) fn find_secondary_eq_nodes_at_limited(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_collect(
                index_id,
                hash_prop_equality_key(prop_value),
                snapshot_seq,
                max_ids,
                Some((false, prop_key, prop_value)),
            );
        }

        let state = self.state.read().unwrap();
        let value_hash = hash_prop_equality_key(prop_value);
        let mut ids = Vec::new();
        if let Some(groups) = state.secondary_eq_state.get(&index_id) {
            if let Some(group) = groups.get(&value_hash) {
                for (&node_id, slot) in group {
                    if !slot_option_visible(slot, snapshot_seq) {
                        continue;
                    }
                    let Some(node) = state.node_at(node_id, snapshot_seq) else {
                        continue;
                    };
                    if node
                        .props
                        .get(prop_key)
                        .is_some_and(|value| semantic_property_eq(value, prop_value))
                    {
                        ids.push(node_id);
                        if max_ids.is_some_and(|max_ids| ids.len() >= max_ids) {
                            break;
                        }
                    }
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }
    pub(crate) fn find_secondary_eq_nodes_by_hash_at_limited(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_collect(index_id, value_hash, snapshot_seq, max_ids, None);
        }

        let state = self.state.read().unwrap();
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Ok(Vec::new());
        };
        let Some(group) = groups.get(&value_hash) else {
            return Ok(Vec::new());
        };

        let mut ids = Vec::new();
        for (&node_id, slot) in group {
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            ids.push(node_id);
            if max_ids.is_some_and(|max_ids| ids.len() >= max_ids) {
                break;
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    // CP40.1 stream construction helper; CP40.2 wires it into query execution.
    #[allow(dead_code)]
    pub(crate) fn visible_secondary_eq_node_ids_sorted(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_secondary_eq_nodes_by_hash_at_limited(index_id, value_hash, snapshot_seq, None)
    }

    pub(crate) fn secondary_eq_node_count_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_count(
                index_id,
                hash_prop_equality_key(prop_value),
                snapshot_seq,
                Some((false, prop_key, prop_value)),
            );
        }

        let state = self.state.read().unwrap();
        let value_hash = hash_prop_equality_key(prop_value);
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Ok(0);
        };
        let Some(group) = groups.get(&value_hash) else {
            return Ok(0);
        };

        let mut count = 0;
        for (&node_id, slot) in group {
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            let Some(node) = state.node_at(node_id, snapshot_seq) else {
                continue;
            };
            if node
                .props
                .get(prop_key)
                .is_some_and(|value| semantic_property_eq(value, prop_value))
            {
                count += 1;
            }
        }
        Ok(count)
    }
    pub(crate) fn secondary_eq_node_hash_count_at(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_count(index_id, value_hash, snapshot_seq, None);
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let Some(groups) = state.secondary_eq_state.get(&index_id) else {
                return 0;
            };
            let Some(group) = groups.get(&value_hash) else {
                return 0;
            };
            group
                .values()
                .filter(|slot| slot_option_visible(slot, snapshot_seq))
                .count()
        })())
    }

    pub(crate) fn find_secondary_eq_edges_by_hash_at_limited(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Result<Vec<u64>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_collect(index_id, value_hash, snapshot_seq, max_ids, None);
        }

        let state = self.state.read().unwrap();
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Ok(Vec::new());
        };
        let Some(group) = groups.get(&value_hash) else {
            return Ok(Vec::new());
        };

        let mut ids = Vec::new();
        for (&edge_id, slot) in group {
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            ids.push(edge_id);
            if max_ids.is_some_and(|max_ids| ids.len() >= max_ids) {
                break;
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn visible_secondary_eq_edge_ids_sorted(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_secondary_eq_edges_by_hash_at_limited(index_id, value_hash, snapshot_seq, None)
    }

    pub(crate) fn secondary_eq_edge_count_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_count(
                index_id,
                hash_prop_equality_key(prop_value),
                snapshot_seq,
                Some((true, prop_key, prop_value)),
            );
        }

        let state = self.state.read().unwrap();
        let value_hash = hash_prop_equality_key(prop_value);
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Ok(0);
        };
        let Some(group) = groups.get(&value_hash) else {
            return Ok(0);
        };

        let mut count = 0;
        for (&edge_id, slot) in group {
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            let Some(edge) = state.edge_at(edge_id, snapshot_seq) else {
                continue;
            };
            if edge
                .props
                .get(prop_key)
                .is_some_and(|value| semantic_property_eq(value, prop_value))
            {
                count += 1;
            }
        }
        Ok(count)
    }
    pub(crate) fn secondary_eq_edge_hash_count_at(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_eq_count(index_id, value_hash, snapshot_seq, None);
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let Some(groups) = state.secondary_eq_state.get(&index_id) else {
                return 0;
            };
            let Some(group) = groups.get(&value_hash) else {
                return 0;
            };
            group
                .values()
                .filter(|slot| slot_option_visible(slot, snapshot_seq))
                .count()
        })())
    }

    #[cfg(test)]
    pub(crate) fn visible_secondary_range_entries(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
    ) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
        self.visible_secondary_range_entries_limited(
            index_id,
            lower,
            upper,
            after,
            snapshot_seq,
            None,
        )
    }

    pub(crate) fn visible_secondary_range_entries_limited(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(NumericRangeSortKey, u64)>, EngineError> {
        let mut rows = Vec::new();
        let mut after = after;
        if limit == Some(0) {
            return Ok(rows);
        }
        loop {
            let (chunk, last, done) = self.refill_secondary_range_at(
                index_id,
                lower,
                upper,
                after,
                snapshot_seq,
                limit.unwrap_or(usize::MAX).saturating_sub(rows.len()),
            )?;
            rows.extend(chunk);
            if done || limit.is_some_and(|max| rows.len() >= max) {
                return Ok(rows);
            }
            after = last.or(after);
        }
    }

    pub(crate) fn visible_secondary_range_entry_count(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_range_count(index_id, lower, upper, after, snapshot_seq);
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let Some(entries) = state.secondary_range_state.get(&index_id) else {
                return 0;
            };

            let start = secondary_range_start_bound(lower, after);
            entries
                .range((start, std::ops::Bound::Unbounded))
                .take_while(|&(&(encoded, _), _)| !secondary_range_past_upper(encoded, upper))
                .filter(|&(_, slot)| slot_option_visible(slot, snapshot_seq))
                .count()
        })())
    }

    pub(crate) fn for_each_visible_secondary_range_entry_at<F>(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut((NumericRangeSortKey, u64)) -> ControlFlow<()>,
    {
        let mut after = after;
        loop {
            let (chunk, last, done) =
                self.refill_secondary_range_at(index_id, lower, upper, after, snapshot_seq, 4096)?;
            for row in chunk {
                if callback(row).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            if done {
                return Ok(ControlFlow::Continue(()));
            }
            after = last.or(after);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn find_node_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_edge_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_node_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        self.count_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_edge_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        self.count_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_node_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_edge_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        self.find_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_node_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        self.count_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_edge_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        self.count_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    fn find_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        let mut start = Bound::Included((bounds.lower.clone(), 0));
        let mut ids = Vec::new();
        loop {
            let (rows, last, done) = self.refill_compound_at(
                index_id,
                start,
                &bounds.upper_exclusive,
                None,
                snapshot_seq,
                4096,
            )?;
            ids.extend(rows.into_iter().map(|(_, id)| id));
            if done {
                return Ok(ids);
            }
            start = Bound::Excluded(last.expect("compound progress"));
        }
    }

    #[allow(dead_code)]
    fn count_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_compound_count(
                index_id,
                Bound::Included((bounds.lower.clone(), 0)),
                &bounds.upper_exclusive,
                None,
                snapshot_seq,
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let Some(entries) = state.secondary_compound_state.get(&index_id) else {
                return 0;
            };
            count_visible_compound_ids(
                entries,
                Bound::Included((bounds.lower.clone(), 0)),
                &bounds.upper_exclusive,
                None,
                snapshot_seq,
            )
        })())
    }

    #[allow(dead_code)]
    fn find_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<Vec<u64>, EngineError> {
        let mut start = compound_start_bound(bounds.lower.as_ref());
        let mut ids = Vec::new();
        loop {
            let (rows, last, done) = self.refill_compound_at(
                index_id,
                start,
                &bounds.upper_exclusive,
                bounds
                    .lower
                    .as_ref()
                    .filter(|lower| lower.exclusive_component_prefix)
                    .map(|lower| lower.key.as_slice()),
                snapshot_seq,
                4096,
            )?;
            ids.extend(rows.into_iter().map(|(_, id)| id));
            if done {
                return Ok(ids);
            }
            start = Bound::Excluded(last.expect("compound progress"));
        }
    }

    #[allow(dead_code)]
    fn count_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Result<usize, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_compound_count(
                index_id,
                compound_start_bound(bounds.lower.as_ref()),
                &bounds.upper_exclusive,
                bounds
                    .lower
                    .as_ref()
                    .filter(|b| b.exclusive_component_prefix)
                    .map(|b| b.key.as_slice()),
                snapshot_seq,
            );
        }
        Ok((|| {
            let state = self.state.read().unwrap();
            let Some(entries) = state.secondary_compound_state.get(&index_id) else {
                return 0;
            };
            let skip_prefix = bounds
                .lower
                .as_ref()
                .filter(|lower| lower.exclusive_component_prefix)
                .map(|lower| lower.key.as_slice());
            count_visible_compound_ids(
                entries,
                compound_start_bound(bounds.lower.as_ref()),
                &bounds.upper_exclusive,
                skip_prefix,
                snapshot_seq,
            )
        })())
    }

    pub(crate) fn collect_deleted_nodes_at(
        &self,
        snapshot_seq: u64,
    ) -> Result<NodeIdSet, EngineError> {
        let state = self.primary_read()?;
        Ok(state.deleted_node_ids_at(snapshot_seq, || {
            #[cfg(test)]
            self.tombstone_scan_work
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }))
    }

    #[cfg(test)]
    pub(crate) fn take_tombstone_scan_work_for_test(&self) -> usize {
        self.tombstone_scan_work
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn collect_deleted_edges_at(
        &self,
        snapshot_seq: u64,
    ) -> Result<NodeIdSet, EngineError> {
        let state = self.primary_read()?;
        Ok(state.deleted_edge_ids_at(snapshot_seq, || {
            #[cfg(test)]
            self.tombstone_scan_work
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }))
    }

    #[allow(dead_code)]
    pub(crate) fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        self.neighbors_at(node_id, direction, label_filter_ids, limit, u64::MAX)
    }

    #[allow(dead_code)]
    pub(crate) fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        self.neighbors_batch_at(node_ids, direction, label_filter_ids, u64::MAX)
    }

    pub(crate) fn neighbors_batch_at(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_neighbors_batch(
                node_ids,
                direction,
                label_filter_ids,
                snapshot_seq,
                true,
            );
        }
        (|| -> Result<_, EngineError> {
            let mut results = NodeIdMap::default();
            for &nid in node_ids {
                let entries =
                    self.neighbors_at(nid, direction, label_filter_ids, 0, snapshot_seq)?;
                if !entries.is_empty() {
                    results.insert(nid, entries);
                }
            }
            Ok(results)
        })()
    }

    #[cfg(all(test, feature = "production-concurrency"))]
    pub(crate) fn locked_primary_slots_for_test(&self) -> [usize; 4] {
        let s = self.state.read().unwrap();
        [
            s.nodes.len(),
            s.edges.len(),
            s.node_key_index.len(),
            s.edge_triple_index.len(),
        ]
    }

    #[cfg(all(test, feature = "production-concurrency"))]
    pub(crate) fn locked_tombstone_slots_for_test(&self) -> [usize; 2] {
        let s = self.state.read().unwrap();
        [s.node_tombstones.len(), s.edge_tombstones.len()]
    }

    pub(crate) fn current_tombstone_counts(&self) -> (usize, usize) {
        let state = self.state.read().unwrap();
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return state.candidate_tombstone_counts;
        }
        (
            state
                .node_tombstones
                .values()
                .filter(|slot| slot.current().is_some())
                .count(),
            state
                .edge_tombstones
                .values()
                .filter(|slot| slot.current().is_some())
                .count(),
        )
    }

    pub fn node_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.live_node_count
    }

    pub fn edge_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.live_edge_count
    }

    pub fn retained_edge_slot_count(&self) -> usize {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.edge_ids.load(std::sync::atomic::Ordering::Relaxed);
        }

        let state = self.state.read().unwrap();
        state.edges.len()
    }

    pub(crate) fn retained_edge_record_keys_is_empty(&self) -> bool {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.edge_ids.load(std::sync::atomic::Ordering::Relaxed) == 0;
        }

        let state = self.state.read().unwrap();
        state.edges.is_empty()
    }

    pub(crate) fn retained_ordered_adj_outer_group_count(&self, direction: Direction) -> usize {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.graph.retained_groups(physical_direction(direction));
        }

        let state = self.state.read().unwrap();
        match direction {
            Direction::Outgoing => state.ordered_adj_out_groups.len(),
            Direction::Incoming => state.ordered_adj_in_groups.len(),
            Direction::Both => {
                panic!("ordered adjacency retained count requires a physical direction")
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn retained_ordered_adj_posting_slot_count(&self, direction: Direction) -> u64 {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.graph.retained_postings(physical_direction(direction));
        }

        let state = self.state.read().unwrap();
        match direction {
            Direction::Outgoing => state.retained_adj_out_posting_slots,
            Direction::Incoming => state.retained_adj_in_posting_slots,
            Direction::Both => {
                panic!("ordered adjacency retained count requires a physical direction")
            }
        }
    }

    pub(crate) fn refill_ordered_adj_owner_at(
        &self,
        direction: Direction,
        normalized_labels: Option<&[u32]>,
        snapshot_seq: u64,
        group_bound: Bound<(u64, u32)>,
        max_group_entries_examined: usize,
    ) -> Result<MemtableAdjacencyOwnerRefill, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                assert!(
                    max_group_entries_examined > 0,
                    "ordered adjacency owner refill maximum must be positive"
                );
                self.graph_session(|m, s| {
                    m.graph.owner_refill(
                        s,
                        physical_direction(direction),
                        snapshot_seq,
                        group_bound,
                        normalized_labels,
                        max_group_entries_examined,
                    )
                })
            };
        }
        Ok((|| {
            assert!(
                max_group_entries_examined > 0,
                "ordered adjacency owner refill maximum must be positive"
            );
            if normalized_labels.is_some_and(<[u32]>::is_empty) {
                return MemtableAdjacencyOwnerRefill {
                    owner_head: None,
                    last_group_key_examined: None,
                    examined: 0,
                    exhausted: true,
                };
            }

            let state = self.state.read().unwrap();
            let groups = match direction {
                Direction::Outgoing => &state.ordered_adj_out_groups,
                Direction::Incoming => &state.ordered_adj_in_groups,
                Direction::Both => {
                    panic!("ordered adjacency owner refill requires a physical direction")
                }
            };
            let mut range = groups.range((group_bound, Bound::Unbounded));
            let mut last_group_key_examined = None;
            let mut examined = 0usize;
            while examined < max_group_entries_examined {
                let Some((&key, group)) = range.next() else {
                    return MemtableAdjacencyOwnerRefill {
                        owner_head: None,
                        last_group_key_examined,
                        examined,
                        exhausted: true,
                    };
                };
                examined += 1;
                last_group_key_examined = Some(key);
                let label_matches =
                    normalized_labels.is_none_or(|labels| labels.binary_search(&key.1).is_ok());
                if label_matches && slot_option_visible(&group.membership, snapshot_seq) {
                    return MemtableAdjacencyOwnerRefill {
                        owner_head: Some(key.0),
                        last_group_key_examined,
                        examined,
                        exhausted: false,
                    };
                }
            }

            MemtableAdjacencyOwnerRefill {
                owner_head: None,
                last_group_key_examined,
                examined,
                exhausted: false,
            }
        })())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn refill_ordered_adj_postings_at(
        &self,
        direction: Direction,
        owner_id: u64,
        normalized_labels: Option<&[u32]>,
        snapshot_seq: u64,
        edge_bound: Bound<u64>,
        max_posting_slots_examined: usize,
        mut scratch: Vec<AdjEntry>,
    ) -> Result<MemtableAdjacencyPostingRefill, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                assert!(
                    max_posting_slots_examined > 0,
                    "ordered adjacency posting refill maximum must be positive"
                );
                self.graph_session(|m, s| {
                    m.graph.adj_refill(
                        s,
                        physical_direction(direction),
                        owner_id,
                        snapshot_seq,
                        edge_bound,
                        normalized_labels,
                        max_posting_slots_examined,
                        scratch,
                    )
                })
            };
        }
        Ok((|| {
            assert!(
                max_posting_slots_examined > 0,
                "ordered adjacency posting refill maximum must be positive"
            );
            scratch.clear();
            if normalized_labels.is_some_and(<[u32]>::is_empty) {
                return MemtableAdjacencyPostingRefill {
                    entries: scratch,
                    last_edge_id_examined: None,
                    examined: 0,
                    exhausted: true,
                };
            }
            let state = self.state.read().unwrap();
            let source = match direction {
                Direction::Outgoing => &state.ordered_adj_out,
                Direction::Incoming => &state.ordered_adj_in,
                Direction::Both => {
                    panic!("ordered adjacency posting refill requires a physical direction")
                }
            };
            let Some(entries) = source.get(&owner_id) else {
                return MemtableAdjacencyPostingRefill {
                    entries: scratch,
                    last_edge_id_examined: None,
                    examined: 0,
                    exhausted: true,
                };
            };
            let mut range = entries.range((edge_bound, Bound::Unbounded));
            let mut last_edge_id_examined = None;
            let mut examined = 0usize;
            while examined < max_posting_slots_examined {
                let Some((&edge_id, slot)) = range.next() else {
                    return MemtableAdjacencyPostingRefill {
                        entries: scratch,
                        last_edge_id_examined,
                        examined,
                        exhausted: true,
                    };
                };
                examined += 1;
                last_edge_id_examined = Some(edge_id);
                let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                    continue;
                };
                if normalized_labels
                    .is_some_and(|labels| labels.binary_search(&entry.label_id).is_err())
                {
                    continue;
                }
                scratch.push(entry.clone());
            }

            MemtableAdjacencyPostingRefill {
                entries: scratch,
                last_edge_id_examined,
                examined,
                exhausted: false,
            }
        })())
    }

    pub fn nodes(&self) -> Result<NodeIdMap<NodeRecord>, EngineError> {
        let state = self.primary_read()?;
        let mut rows = NodeIdMap::default();
        state.nodes_at(u64::MAX, |id, value| {
            if let RecordState::Live(value) = value {
                rows.insert(id, value.clone());
            }
        });
        Ok(rows)
    }

    pub fn edges(&self) -> Result<NodeIdMap<EdgeRecord>, EngineError> {
        let state = self.primary_read()?;
        let mut rows = NodeIdMap::default();
        state.edges_at(u64::MAX, |id, value| {
            if let RecordState::Live(value) = value {
                rows.insert(id, value.clone());
            }
        });
        Ok(rows)
    }

    pub fn deleted_nodes(&self) -> Result<NodeIdMap<TombstoneEntry>, EngineError> {
        let state = self.primary_read()?;
        let mut rows = NodeIdMap::default();
        state.nodes_at(u64::MAX, |id, value| {
            if let RecordState::Tombstone(value) = value {
                rows.insert(id, *value);
            }
        });
        Ok(rows)
    }

    pub fn deleted_edges(&self) -> Result<NodeIdMap<TombstoneEntry>, EngineError> {
        let state = self.primary_read()?;
        let mut rows = NodeIdMap::default();
        state.edges_at(u64::MAX, |id, value| {
            if let RecordState::Tombstone(value) = value {
                rows.insert(id, *value);
            }
        });
        Ok(rows)
    }

    pub fn adj_out(&self) -> Result<NodeIdMap<NodeIdMap<AdjEntry>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_export_adj(0);
        }
        Ok({
            let state = self.state.read().unwrap();
            current_adj_map(&state.adj_out)
        })
    }

    pub fn adj_in(&self) -> Result<NodeIdMap<NodeIdMap<AdjEntry>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_export_adj(1);
        }
        Ok({
            let state = self.state.read().unwrap();
            current_adj_map(&state.adj_in)
        })
    }

    pub(crate) fn node_label_posting_groups_current(
        &self,
    ) -> Result<Vec<(u32, Vec<u64>)>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                let mut groups: Vec<(u32, Vec<u64>)> = Vec::new();
                for (label, id) in self.graph_labels(0, None, u64::MAX)? {
                    if groups.last().is_none_or(|g| g.0 != label) {
                        groups.push((label, Vec::new()));
                    }
                    groups.last_mut().unwrap().1.push(id);
                }
                Ok(groups)
            };
        }
        Ok({
            let state = self.state.read().unwrap();
            let mut groups = Vec::new();
            let mut current_label_id = None;
            let mut current_ids = Vec::new();

            for (&(label_id, node_id), slot) in &state.label_node_index {
                if current_label_id != Some(label_id) {
                    if !current_ids.is_empty() {
                        groups.push((
                            current_label_id.expect("label group is present"),
                            std::mem::take(&mut current_ids),
                        ));
                    }
                    current_label_id = Some(label_id);
                }
                if slot_option_current(slot).is_some() {
                    current_ids.push(node_id);
                }
            }

            if !current_ids.is_empty() {
                groups.push((
                    current_label_id.expect("label group is present"),
                    current_ids,
                ));
            }
            groups
        })
    }

    pub fn label_edge_index(&self) -> Result<HashMap<u32, NodeIdSet>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return {
                let mut groups = HashMap::new();
                for (label, id) in self.graph_labels(1, None, u64::MAX)? {
                    groups
                        .entry(label)
                        .or_insert_with(NodeIdSet::default)
                        .insert(id);
                }
                Ok(groups)
            };
        }
        Ok({
            let state = self.state.read().unwrap();
            current_label_membership_index(&state.label_edge_index)
        })
    }

    pub fn secondary_index_declarations(&self) -> HashMap<u64, SecondaryIndexManifestEntry> {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m
                .secondary
                .catalog
                .dispatch
                .secondary_index_declarations
                .clone();
        }

        let state = self.state.read().unwrap();
        state.catalog.secondary_index_declarations.clone()
    }

    pub fn secondary_eq_state(&self) -> Result<HashMap<u64, HashMap<u64, NodeIdSet>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_export_eq();
        }
        Ok({
            let state = self.state.read().unwrap();
            current_secondary_eq_state(&state.secondary_eq_state)
        })
    }

    pub fn secondary_range_state(
        &self,
    ) -> Result<HashMap<u64, BTreeSet<(NumericRangeSortKey, u64)>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_export_range();
        }
        Ok({
            let state = self.state.read().unwrap();
            current_secondary_range_state(&state.secondary_range_state)
        })
    }

    #[cfg(test)]
    pub(crate) fn compound_secondary_state(
        &self,
    ) -> Result<CompoundSecondaryTestState, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_export_compound(None).map(|m| {
                m.into_iter()
                    .map(|(i, rows)| (i, rows.into_iter().collect()))
                    .collect()
            });
        }
        Ok({
            let state = self.state.read().unwrap();
            current_compound_secondary_state(&state.secondary_compound_state)
        })
    }

    pub(crate) fn compound_secondary_state_for_indexes(
        &self,
        index_ids: &[u64],
    ) -> Result<secondary::CompoundSecondaryExport, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.candidate_export_compound(Some(index_ids));
        }
        Ok({
            let state = self.state.read().unwrap();
            current_compound_secondary_state_for_indexes(&state.secondary_compound_state, index_ids)
        })
    }

    pub fn time_node_index(&self) -> Result<BTreeSet<(u32, i64, u64)>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if self.primary.is_some() {
            return self.graph_export_time();
        }
        let state = self.state.read().unwrap();
        Ok(current_time_index(&state.time_node_index))
    }

    fn estimate_node_record(node: &NodeRecord) -> usize {
        let dense_bytes = node
            .dense_vector
            .as_ref()
            .map(|values| values.len() * std::mem::size_of::<f32>())
            .unwrap_or(0);
        let sparse_bytes = node
            .sparse_vector
            .as_ref()
            .map(|values| values.len() * (std::mem::size_of::<u32>() + std::mem::size_of::<f32>()))
            .unwrap_or(0);
        120 + node.key.len() + node.props.len() * 80 + dense_bytes + sparse_bytes
    }

    fn estimate_edge_record(edge: &EdgeRecord) -> usize {
        100 + edge.props.len() * 80
    }

    fn estimate_slot<T>(slot: &VersionedSlot<T>, value_size: impl Fn(&T) -> usize) -> usize {
        let history_size = slot
            .history
            .as_ref()
            .map(|history| {
                history
                    .iter()
                    .map(|version| 8 + value_size(&version.value))
                    .sum::<usize>()
            })
            .unwrap_or(0);
        8 + value_size(&slot.head.value) + history_size
    }

    pub fn estimated_size(&self) -> usize {
        let state = self.state.read().unwrap();
        state.estimated_bytes + {
            #[cfg(feature = "production-concurrency")]
            {
                self.primary.as_ref().map_or(0, |m| {
                    m.estimated.load(std::sync::atomic::Ordering::Relaxed)
                        + m.graph.estimated()
                        + m.secondary.estimated()
                })
            }
            #[cfg(not(feature = "production-concurrency"))]
            {
                0
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn estimated_size_full_for_test(&self) -> usize {
        let state = self.state.read().unwrap();
        let derived = state.recompute_estimated_size();
        #[cfg(feature = "production-concurrency")]
        if let Some(primary) = &self.primary {
            let session = primary.admission.try_acquire().unwrap();
            return derived
                + primary.estimated_size_full_for_test()
                + primary.secondary.estimated_full(&session);
        }
        derived
    }

    pub fn is_empty(&self) -> bool {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.node_ids.load(std::sync::atomic::Ordering::Relaxed) == 0
                && m.edge_ids.load(std::sync::atomic::Ordering::Relaxed) == 0;
        }

        let state = self.state.read().unwrap();
        state.nodes.is_empty() && state.edges.is_empty()
    }

    pub fn max_node_id(&self) -> u64 {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.max_node.load(std::sync::atomic::Ordering::Relaxed);
        }

        let state = self.state.read().unwrap();
        state.nodes.keys().max().copied().unwrap_or(0)
    }

    pub fn max_edge_id(&self) -> u64 {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            return m.max_edge.load(std::sync::atomic::Ordering::Relaxed);
        }

        let state = self.state.read().unwrap();
        state.edges.keys().max().copied().unwrap_or(0)
    }
}

#[cfg(test)]
impl Memtable {
    fn label_node_index_key_count(&self) -> usize {
        self.node_label_posting_groups_current().unwrap().len()
    }

    fn node_key_index_key_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state
            .node_key_index
            .iter()
            .filter(|(_, by_key)| {
                by_key
                    .values()
                    .any(|slot| slot_option_current(slot).is_some())
            })
            .count()
    }

    fn time_node_index_len(&self) -> usize {
        self.time_node_index().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secondary_index_key::{
        compound_prefix_bounds, compound_range_bounds, encode_compound_field_component,
        encode_compound_tuple_prefix,
    };
    use std::collections::BTreeMap;

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

    fn make_node_at(id: u64, label_id: u32, key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            updated_at,
            ..make_node(id, label_id, key)
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

    fn make_edge_with_props(
        id: u64,
        from: u64,
        to: u64,
        label_id: u32,
        props: BTreeMap<String, PropValue>,
    ) -> EdgeRecord {
        EdgeRecord {
            props,
            ..make_edge(id, from, to, label_id)
        }
    }

    fn make_node_with_props(
        id: u64,
        label_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
    ) -> NodeRecord {
        NodeRecord {
            props,
            ..make_node(id, label_id, key)
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

    fn property_field(key: &str) -> SecondaryIndexFieldManifest {
        SecondaryIndexFieldManifest::Property {
            key: key.to_string(),
        }
    }

    fn node_meta_field(field: NodeMetadataIndexFieldManifest) -> SecondaryIndexFieldManifest {
        SecondaryIndexFieldManifest::NodeMetadata { field }
    }

    fn edge_meta_field(field: EdgeMetadataIndexFieldManifest) -> SecondaryIndexFieldManifest {
        SecondaryIndexFieldManifest::EdgeMetadata { field }
    }

    fn node_field_entry(
        index_id: u64,
        label_id: u32,
        fields: Vec<SecondaryIndexFieldManifest>,
        kind: SecondaryIndexKind,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex { label_id, fields },
            kind,
            state: SecondaryIndexState::Building,
            last_error: None,
        }
    }

    fn edge_field_entry(
        index_id: u64,
        label_id: u32,
        fields: Vec<SecondaryIndexFieldManifest>,
        kind: SecondaryIndexKind,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::EdgeFieldIndex { label_id, fields },
            kind,
            state: SecondaryIndexState::Building,
            last_error: None,
        }
    }

    fn prefix_bounds_for(
        entry: &SecondaryIndexManifestEntry,
        values: &[CompoundFieldValue<'_>],
    ) -> CompoundPrefixBounds {
        let context = CompoundTupleContext::from_manifest_entry(entry).unwrap();
        let prefix = encode_compound_tuple_prefix(&context, values).unwrap();
        compound_prefix_bounds(&prefix)
    }

    #[test]
    fn current_head_compatibility_still_works() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 2);

        assert_eq!(mt.get_node_at(1, u64::MAX).unwrap().unwrap().key, "alice");
        assert_eq!(mt.get_edge_at(1, u64::MAX).unwrap().unwrap().from, 1);
        assert_eq!(
            mt.node_by_key_at(1, "alice", u64::MAX).unwrap().unwrap().id,
            1
        );
        assert_eq!(
            mt.edge_by_triple_at(1, 2, 10, u64::MAX)
                .unwrap()
                .unwrap()
                .id,
            1
        );
    }

    #[test]
    fn current_live_counts_track_delete_and_resurrection_without_changing_snapshots() {
        let mt = Memtable::new();
        assert_eq!(mt.node_count(), 0);
        assert_eq!(mt.edge_count(), 0);
        assert!(mt.is_empty());

        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 3);
        assert_eq!(mt.node_count(), 2);
        assert_eq!(mt.edge_count(), 1);
        assert_eq!(mt.visible_node_count_at(2).unwrap(), 2);

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 4,
            },
            4,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 5,
            },
            5,
        );
        assert_eq!(mt.node_count(), 1);
        assert_eq!(mt.edge_count(), 0);
        assert_eq!(mt.visible_node_count_at(2).unwrap(), 2);
        assert_eq!(mt.visible_node_count_at(4).unwrap(), 1);

        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a2")), 6);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 7);
        assert_eq!(mt.node_count(), 2);
        assert_eq!(mt.edge_count(), 1);
        assert_eq!(mt.visible_node_count_at(4).unwrap(), 1);
        assert_eq!(mt.visible_node_count_at(6).unwrap(), 2);

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 8,
            },
            8,
        );
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 9,
            },
            9,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 10,
            },
            10,
        );
        assert_eq!(mt.node_count(), 0);
        assert_eq!(mt.edge_count(), 0);
        assert!(
            !mt.is_empty(),
            "tombstone-only memtables still carry flushable state"
        );
    }

    #[test]
    fn retained_edge_slot_count_tracks_ids_across_rewrites_deletes_and_resurrection() {
        let mt = Memtable::new();
        assert_eq!(mt.retained_edge_slot_count(), 0);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 1);
        assert_eq!(mt.retained_edge_slot_count(), 1);

        let mut rewritten = make_edge(10, 2, 3, 6);
        rewritten.weight = 2.0;
        mt.apply_op(&WalOp::UpsertEdge(rewritten), 2);
        assert_eq!(mt.retained_edge_slot_count(), 1);

        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 3,
            },
            3,
        );
        assert_eq!(mt.retained_edge_slot_count(), 1);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 3, 4, 7)), 4);
        assert_eq!(mt.retained_edge_slot_count(), 1);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 4, 5, 7)), 5);
        assert_eq!(mt.retained_edge_slot_count(), 2);
    }

    #[test]
    fn edge_label_posting_upper_bound_is_snapshot_safe_and_slot_stable() {
        let mt = Memtable::new();
        let visible_count = |label_id, snapshot_seq| {
            mt.visible_edges_by_label_id(label_id, snapshot_seq)
                .unwrap()
                .len()
        };

        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 0);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 1);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(visible_count(5, 1), 1);

        let mut same_label = make_edge(10, 1, 2, 5);
        same_label.weight = 2.0;
        mt.apply_op(&WalOp::UpsertEdge(same_label), 2);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(visible_count(5, 1), 1);
        assert_eq!(visible_count(5, 2), 1);

        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 3,
            },
            3,
        );
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(visible_count(5, 1), 1);
        assert_eq!(visible_count(5, 3), 0);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 4);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(visible_count(5, 3), 0);
        assert_eq!(visible_count(5, 4), 1);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 6)), 5);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(mt.edge_label_posting_count_upper_bound(6).unwrap(), 1);
        assert_eq!(visible_count(5, 4), 1);
        assert_eq!(visible_count(5, 5), 0);
        assert_eq!(visible_count(6, 5), 1);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 6);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 1);
        assert_eq!(mt.edge_label_posting_count_upper_bound(6).unwrap(), 1);
        assert_eq!(visible_count(5, 6), 1);
        assert_eq!(visible_count(6, 6), 0);

        mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 1, 3, 5)), 7);
        assert_eq!(mt.edge_label_posting_count_upper_bound(5).unwrap(), 2);
        for snapshot_seq in 0..=7 {
            assert!(
                mt.edge_label_posting_count_upper_bound(5).unwrap()
                    >= visible_count(5, snapshot_seq),
                "posting count must bound label 5 at snapshot {snapshot_seq}"
            );
            assert!(
                mt.edge_label_posting_count_upper_bound(6).unwrap()
                    >= visible_count(6, snapshot_seq),
                "posting count must bound label 6 at snapshot {snapshot_seq}"
            );
        }
    }

    #[test]
    fn edge_metadata_source_helpers_return_visible_ids() {
        let mt = Memtable::new();
        let mut edge_a = make_edge(10, 1, 2, 5);
        edge_a.weight = -0.0;
        edge_a.updated_at = 100;
        edge_a.valid_from = 10;
        edge_a.valid_to = 100;
        let mut edge_b = make_edge(11, 1, 3, 5);
        edge_b.weight = 0.0;
        edge_b.updated_at = 200;
        edge_b.valid_from = 20;
        edge_b.valid_to = 200;
        let mut edge_c = make_edge(12, 4, 1, 6);
        edge_c.weight = f32::NAN;
        edge_c.updated_at = 300;

        mt.apply_op(&WalOp::UpsertEdge(edge_a), 1);
        mt.apply_op(&WalOp::UpsertEdge(edge_b), 2);
        mt.apply_op(&WalOp::UpsertEdge(edge_c), 3);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 12,
                deleted_at: 400,
            },
            4,
        );

        assert_eq!(mt.visible_edge_ids_at(3).unwrap(), vec![10, 11, 12]);
        assert_eq!(mt.visible_edge_ids_at(4).unwrap(), vec![10, 11]);
        assert_eq!(mt.visible_edges_by_label_id(5, 4).unwrap(), vec![10, 11]);
        let mut outgoing_ids = mt
            .neighbors_batch_at(&[1], Direction::Outgoing, Some(&[5]), 4)
            .unwrap()
            .into_values()
            .flatten()
            .map(|entry| entry.edge_id)
            .collect::<Vec<_>>();
        outgoing_ids.sort_unstable();
        assert_eq!(outgoing_ids, vec![10, 11]);
        let mut both_ids = mt
            .neighbors_batch_at(&[1], Direction::Both, None, 4)
            .unwrap()
            .into_values()
            .flatten()
            .map(|entry| entry.edge_id)
            .collect::<Vec<_>>();
        both_ids.sort_unstable();
        both_ids.dedup();
        assert_eq!(both_ids, vec![10, 11]);
        assert_eq!(mt.edge_ids_by_triple_at(1, 2, 5, 4).unwrap(), vec![10]);
        assert_eq!(
            mt.edge_ids_by_weight_range_at(
                Some(5),
                RangeBoundFlags::inclusive(Some(0.0), Some(0.0)),
                4,
            ),
            vec![10, 11]
        );
        assert_eq!(
            mt.edge_ids_by_updated_at_range_at(
                Some(5),
                RangeBoundFlags::inclusive(Some(150), Some(250)),
                4,
            ),
            vec![11]
        );
        assert_eq!(
            mt.edge_ids_by_valid_from_range_at(
                None,
                RangeBoundFlags::inclusive(Some(0), Some(15)),
                4,
            ),
            vec![10]
        );
        assert_eq!(
            mt.edge_ids_by_valid_to_range_at(
                None,
                RangeBoundFlags {
                    lower: Some(100),
                    lower_inclusive: false,
                    upper: None,
                    upper_inclusive: true,
                },
                4,
            ),
            vec![11]
        );
    }

    #[test]
    fn snapshot_reads_keep_old_node_versions() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 10);
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice_v2")), 20);

        assert_eq!(mt.get_node_at(1, 10).unwrap().unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 19).unwrap().unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 20).unwrap().unwrap().key, "alice_v2");
        assert_eq!(
            mt.get_node_at(1, u64::MAX).unwrap().unwrap().key,
            "alice_v2"
        );
    }

    #[test]
    fn delete_preserves_older_visible_version() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 5);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 123,
            },
            6,
        );

        assert!(mt.get_node_at(1, u64::MAX).unwrap().is_none());
        assert_eq!(mt.get_node_at(1, 5).unwrap().unwrap().key, "alice");
        assert!(mt.get_node_at(1, 6).unwrap().is_none());
        assert!(mt.is_node_deleted_at(1, 6).unwrap());
        assert!(!mt.is_node_deleted_at(1, 5).unwrap());
        assert_eq!(
            mt.deleted_nodes().unwrap().get(&1).unwrap().last_write_seq,
            6
        );
    }

    #[test]
    fn deleted_node_membership_is_snapshot_correct_across_resurrection() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 20,
            },
            2,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice_v2")), 3);

        assert!(mt.collect_deleted_nodes_at(1).unwrap().is_empty());
        assert_eq!(
            mt.collect_deleted_nodes_at(2).unwrap(),
            NodeIdSet::from_iter([1])
        );
        assert!(mt.collect_deleted_nodes_at(3).unwrap().is_empty());
        assert!(mt.deleted_nodes().unwrap().is_empty());
        assert!(!mt.is_node_deleted_at(3, 3).unwrap());
        assert_eq!(mt.get_node_at(1, 1).unwrap().unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 3).unwrap().unwrap().key, "alice_v2");
    }

    #[test]
    fn deleted_edge_membership_is_snapshot_correct_across_resurrection() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(9, 1, 2, 7)), 1);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 9,
                deleted_at: 20,
            },
            2,
        );
        mt.apply_op(&WalOp::UpsertEdge(make_edge(9, 1, 3, 7)), 3);

        assert!(mt.collect_deleted_edges_at(1).unwrap().is_empty());
        assert_eq!(
            mt.collect_deleted_edges_at(2).unwrap(),
            NodeIdSet::from_iter([9])
        );
        assert!(mt.collect_deleted_edges_at(3).unwrap().is_empty());
        assert!(mt.deleted_edges().unwrap().is_empty());
        assert!(!mt.is_edge_deleted_at(9, 3).unwrap());
        assert_eq!(mt.get_edge_at(9, 3).unwrap().unwrap().to, 3);
    }

    #[test]
    fn key_reuse_is_snapshot_correct() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 100,
            },
            2,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "alice")), 3);

        assert_eq!(mt.node_by_key_at(1, "alice", 1).unwrap().unwrap().id, 1);
        assert!(mt.node_by_key_at(1, "alice", 2).unwrap().is_none());
        assert_eq!(mt.node_by_key_at(1, "alice", 3).unwrap().unwrap().id, 2);
        assert_eq!(
            mt.node_by_key_at(1, "alice", u64::MAX).unwrap().unwrap().id,
            2
        );
    }

    #[test]
    fn adjacency_and_label_memberships_are_snapshot_aware() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 3);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 200,
            },
            4,
        );

        assert_eq!(mt.visible_edges_by_label_id(10, 3).unwrap(), vec![1]);
        assert!(mt.visible_edges_by_label_id(10, 4).unwrap().is_empty());
        let before_delete = mt.neighbors_at(1, Direction::Outgoing, None, 0, 3).unwrap();
        assert_eq!(before_delete.len(), 1);
        assert_eq!(before_delete[0].node_id, 2);
        assert!(mt
            .neighbors_at(1, Direction::Outgoing, None, 0, 4)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn endpoint_batch_count_estimates_match_scalar_counts_across_snapshots() {
        fn scalar_endpoint_estimate(
            mt: &Memtable,
            node_ids: &[u64],
            direction: Direction,
            label_filter_ids: Option<&[u32]>,
            snapshot_seq: u64,
        ) -> MemtableEndpointCountEstimate {
            let mut estimate = MemtableEndpointCountEstimate::exact_zero();
            for &node_id in node_ids {
                match direction {
                    Direction::Outgoing => {
                        estimate.add_assign(
                            mt.visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                snapshot_seq,
                            )
                            .unwrap(),
                        );
                    }
                    Direction::Incoming => {
                        estimate.add_assign(
                            mt.visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                snapshot_seq,
                            )
                            .unwrap(),
                        );
                    }
                    Direction::Both => {
                        estimate.add_assign(
                            mt.visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                snapshot_seq,
                            )
                            .unwrap(),
                        );
                        estimate.add_assign(
                            mt.visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                snapshot_seq,
                            )
                            .unwrap(),
                        );
                    }
                }
            }
            estimate
        }

        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);
        mt.apply_op(&WalOp::UpsertNode(make_node(4, 1, "d")), 4);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 5);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 1, 3, 8)), 6);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(12, 4, 1, 7)), 7);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(13, 2, 1, 7)), 8);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 20,
            },
            9,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 13,
                deleted_at: 21,
            },
            10,
        );

        let node_ids = [1, 2, 4, 1, 99];
        let label_seven = [7];
        let empty_labels: [u32; 0] = [];
        let label_filters = [
            None,
            Some(label_seven.as_slice()),
            Some(empty_labels.as_slice()),
        ];

        for snapshot_seq in 0..=10 {
            for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
                for label_filter_ids in label_filters {
                    let batch = mt
                        .visible_endpoint_edges_count_estimate_at(
                            &node_ids,
                            direction,
                            label_filter_ids,
                            snapshot_seq,
                        )
                        .unwrap();
                    let scalar = scalar_endpoint_estimate(
                        &mt,
                        &node_ids,
                        direction,
                        label_filter_ids,
                        snapshot_seq,
                    );
                    assert_eq!(
                        batch, scalar,
                        "snapshot={snapshot_seq}; direction={direction:?}; label_filter_ids={label_filter_ids:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn next_visible_adj_edge_ge_is_inclusive_label_filtered_and_skips_deleted() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 4);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(12, 1, 3, 8)), 5);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(14, 1, 2, 7)), 6);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 700,
            },
            7,
        );
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 800,
            },
            8,
        );

        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, None, 6, 10).unwrap(),
            Some(10)
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, Some(7), 6, 10)
                .unwrap(),
            Some(10)
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, Some(8), 6, 10)
                .unwrap(),
            Some(12)
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, Some(7), 6, 11)
                .unwrap(),
            Some(14)
        );

        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, None, 8, 10).unwrap(),
            Some(14)
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, Some(8), 8, 10)
                .unwrap(),
            None
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(2, false, Some(7), 8, 0)
                .unwrap(),
            Some(14)
        );
        assert_eq!(
            mt.next_visible_adj_edge_ge(1, true, None, 8, u64::MAX)
                .unwrap(),
            None
        );
    }

    #[test]
    fn time_membership_history_is_snapshot_correct() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 200)), 2);

        assert_eq!(
            mt.visible_nodes_by_time_range(1, 50, 150, 1).unwrap(),
            vec![1]
        );
        assert!(mt
            .visible_nodes_by_time_range(1, 50, 150, 2)
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.visible_nodes_by_time_range(1, 150, 250, 2).unwrap(),
            vec![1]
        );
    }

    #[test]
    fn time_range_count_matches_visible_ids_across_snapshot_history() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node_at(2, 1, "b", 150)), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 300)), 3);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 400,
            },
            4,
        );

        for snapshot_seq in 1..=4 {
            for (from_ms, to_ms) in [(0, 500), (50, 175), (250, 350), (500, 0)] {
                assert_eq!(
                    mt.visible_node_time_range_count_at(1, from_ms, to_ms, snapshot_seq)
                        .unwrap(),
                    mt.visible_nodes_by_time_range(1, from_ms, to_ms, snapshot_seq)
                        .unwrap()
                        .len(),
                    "snapshot={snapshot_seq}, range={from_ms}..={to_ms}"
                );
            }
        }
    }

    #[test]
    fn multi_label_node_memberships_are_maintained_by_label() {
        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[1, 2, 5], "alice", 100)),
            1,
        );

        for label_id in [1, 2, 5] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice", 1)
                    .unwrap()
                    .map(|node| node.id),
                Some(1)
            );
            assert_eq!(mt.visible_nodes_by_label_id(label_id, 1).unwrap(), vec![1]);
            assert_eq!(
                mt.visible_nodes_by_time_range(label_id, 100, 100, 1)
                    .unwrap(),
                vec![1]
            );
        }
        assert!(mt.node_by_key_at(9, "alice", 1).unwrap().is_none());
        assert!(mt.visible_nodes_by_label_id(9, 1).unwrap().is_empty());

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[2, 5, 9], "alice", 200)),
            2,
        );

        assert_eq!(
            mt.node_by_key_at(1, "alice", 1)
                .unwrap()
                .map(|node| node.id),
            Some(1)
        );
        assert!(mt.node_by_key_at(1, "alice", 2).unwrap().is_none());
        assert!(mt.visible_nodes_by_label_id(1, 2).unwrap().is_empty());
        assert!(mt
            .visible_nodes_by_time_range(1, 100, 100, 2)
            .unwrap()
            .is_empty());

        for label_id in [2, 5, 9] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice", 2)
                    .unwrap()
                    .map(|node| node.id),
                Some(1)
            );
            assert_eq!(mt.visible_nodes_by_label_id(label_id, 2).unwrap(), vec![1]);
            assert!(mt
                .visible_nodes_by_time_range(label_id, 100, 100, 2)
                .unwrap()
                .is_empty());
            assert_eq!(
                mt.visible_nodes_by_time_range(label_id, 200, 200, 2)
                    .unwrap(),
                vec![1]
            );
        }

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 300,
            },
            3,
        );
        for label_id in [2, 5, 9] {
            assert!(mt.node_by_key_at(label_id, "alice", 3).unwrap().is_none());
            assert!(mt
                .visible_nodes_by_label_id(label_id, 3)
                .unwrap()
                .is_empty());
            assert!(mt
                .visible_nodes_by_time_range(label_id, 200, 200, 3)
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn ordered_node_label_memberships_drive_visible_cursors_and_flush_groups() {
        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[1, 3], "a", 100)),
            1,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "b")), 3);
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[2, 3], "a", 200)),
            4,
        );

        assert_eq!(mt.visible_nodes_by_label_id(1, 2).unwrap(), vec![1, 3]);
        assert_eq!(
            mt.next_visible_node_by_label_id_after(1, 2, None).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_after(1, 2, Some(1))
                .unwrap(),
            Some(3)
        );
        assert_eq!(mt.visible_nodes_by_label_id(1, 4).unwrap(), vec![3]);
        assert_eq!(mt.visible_nodes_by_label_id(2, 4).unwrap(), vec![1, 2]);
        assert_eq!(mt.visible_node_label_ids(4).unwrap(), vec![1, 2, 3]);
        assert_eq!(
            mt.node_label_posting_groups_current().unwrap(),
            vec![(1, vec![3]), (2, vec![1, 2]), (3, vec![1])]
        );

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 300,
            },
            5,
        );

        assert!(mt.visible_nodes_by_label_id(1, 5).unwrap().is_empty());
        assert_eq!(mt.visible_node_label_ids(5).unwrap(), vec![2, 3]);
        assert_eq!(
            mt.node_label_posting_groups_current().unwrap(),
            vec![(2, vec![1, 2]), (3, vec![1])]
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_after(2, 5, None).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_after(2, 5, Some(1))
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_after(2, 5, Some(2))
                .unwrap(),
            None
        );
    }

    #[test]
    fn next_visible_node_by_label_id_ge_is_snapshot_aware_and_inclusive() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 7, "a")), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 7, "b")), 2);
        mt.apply_op(&WalOp::UpsertNode(make_node(5, 7, "c")), 3);
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 8, "b")), 4);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 5,
                deleted_at: 500,
            },
            5,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 7, "d")), 6);

        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 0).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 1).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 2).unwrap(),
            Some(3)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 3).unwrap(),
            Some(3)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 4).unwrap(),
            Some(5)
        );
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 3, 5).unwrap(),
            Some(5)
        );
        assert_eq!(mt.next_visible_node_by_label_id_ge(7, 3, 6).unwrap(), None);

        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 4, 2).unwrap(),
            Some(5)
        );
        assert_eq!(mt.next_visible_node_by_label_id_ge(7, 5, 2).unwrap(), None);
        assert_eq!(
            mt.next_visible_node_by_label_id_ge(7, 6, 2).unwrap(),
            Some(2)
        );
    }

    #[test]
    fn next_visible_edge_by_label_id_ge_is_snapshot_aware_and_inclusive() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 7)), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 2, 7)), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(5, 1, 2, 7)), 3);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 2, 8)), 4);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 5,
                deleted_at: 500,
            },
            5,
        );
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 2, 7)), 6);

        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 0).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 1).unwrap(),
            Some(1)
        );
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 2).unwrap(),
            Some(3)
        );
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 3).unwrap(),
            Some(3)
        );
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 4).unwrap(),
            Some(5)
        );
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 3, 5).unwrap(),
            Some(5)
        );
        assert_eq!(mt.next_visible_edge_by_label_id_ge(7, 3, 6).unwrap(), None);

        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 4, 2).unwrap(),
            Some(5)
        );
        assert_eq!(mt.next_visible_edge_by_label_id_ge(7, 5, 2).unwrap(), None);
        assert_eq!(
            mt.next_visible_edge_by_label_id_ge(7, 6, 2).unwrap(),
            Some(2)
        );
    }

    #[test]
    fn multi_label_key_replacement_updates_all_label_memberships() {
        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[1, 2], "alice", 100)),
            1,
        );

        for label_id in [1, 2] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice", 1)
                    .unwrap()
                    .map(|node| node.id),
                Some(1)
            );
        }

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[2, 3], "alice2", 100)),
            2,
        );

        for label_id in [1, 2] {
            assert!(
                mt.node_by_key_at(label_id, "alice", 2).unwrap().is_none(),
                "old key remained visible for label {label_id}"
            );
        }
        assert!(
            mt.node_by_key_at(1, "alice2", 2).unwrap().is_none(),
            "new key became visible for removed label"
        );
        for label_id in [2, 3] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice2", 2)
                    .unwrap()
                    .map(|node| node.id),
                Some(1),
                "new key missing for label {label_id}"
            );
        }
    }

    #[test]
    fn secondary_eq_membership_history_is_snapshot_correct() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("name".into(), PropValue::String("alice".into()));
        let entry = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "name".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&entry).unwrap();
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", props)),
            1,
        );

        let mut next_props = BTreeMap::new();
        next_props.insert("name".into(), PropValue::String("bob".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", next_props)),
            2,
        );

        assert_eq!(
            mt.find_secondary_eq_nodes_at(10, "name", &PropValue::String("alice".into()), 1)
                .unwrap(),
            vec![1]
        );
        assert!(mt
            .find_secondary_eq_nodes_at(10, "name", &PropValue::String("alice".into()), 2)
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.find_secondary_eq_nodes_at(10, "name", &PropValue::String("bob".into()), 2)
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn visible_secondary_eq_node_ids_sorted_is_visible_sorted_deduped_and_snapshot_aware() {
        let mt = Memtable::new();
        let entry = SecondaryIndexManifestEntry {
            index_id: 12,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "status".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&entry).unwrap();

        let hot = PropValue::String("hot".into());
        let cold = PropValue::String("cold".into());
        let hot_hash = hash_prop_equality_key(&hot);
        let cold_hash = hash_prop_equality_key(&cold);

        let mut hot_props = BTreeMap::new();
        hot_props.insert("status".into(), hot.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(3, 1, "c", hot_props.clone())),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", hot_props.clone())),
            2,
        );

        let mut cold_props = BTreeMap::new();
        cold_props.insert("status".into(), cold.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(3, 1, "c", cold_props)),
            3,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(2, 1, "b", hot_props.clone())),
            4,
        );
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 500,
            },
            5,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", hot_props)),
            6,
        );

        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, hot_hash, 2)
                .unwrap(),
            vec![1, 3]
        );
        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, hot_hash, 3)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, cold_hash, 3)
                .unwrap(),
            vec![3]
        );
        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, hot_hash, 4)
                .unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, hot_hash, 5)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mt.visible_secondary_eq_node_ids_sorted(12, hot_hash, 6)
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn visible_secondary_eq_edge_ids_sorted_is_visible_sorted_deduped_and_snapshot_aware() {
        let mt = Memtable::new();
        let entry = SecondaryIndexManifestEntry {
            index_id: 13,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 7,
                prop_key: "status".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&entry).unwrap();

        let hot = PropValue::String("hot".into());
        let cold = PropValue::String("cold".into());
        let hot_hash = hash_prop_equality_key(&hot);
        let cold_hash = hash_prop_equality_key(&cold);

        let mut hot_props = BTreeMap::new();
        hot_props.insert("status".into(), hot.clone());
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(3, 1, 2, 7, hot_props.clone())),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(1, 1, 2, 7, hot_props.clone())),
            2,
        );

        let mut cold_props = BTreeMap::new();
        cold_props.insert("status".into(), cold.clone());
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(3, 1, 2, 7, cold_props)),
            3,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(2, 1, 2, 7, hot_props.clone())),
            4,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 2,
                deleted_at: 500,
            },
            5,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(1, 1, 2, 7, hot_props)),
            6,
        );

        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, hot_hash, 2)
                .unwrap(),
            vec![1, 3]
        );
        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, hot_hash, 3)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, cold_hash, 3)
                .unwrap(),
            vec![3]
        );
        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, hot_hash, 4)
                .unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, hot_hash, 5)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mt.visible_secondary_eq_edge_ids_sorted(13, hot_hash, 6)
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn secondary_eq_hash_counts_are_snapshot_aware_for_nodes_and_edges() {
        let mt = Memtable::new();
        let node_entry = SecondaryIndexManifestEntry {
            index_id: 18,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "status".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let edge_entry = SecondaryIndexManifestEntry {
            index_id: 19,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 7,
                prop_key: "status".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&node_entry).unwrap();
        mt.register_secondary_index(&edge_entry).unwrap();

        let hot = PropValue::String("hot".into());
        let cold = PropValue::String("cold".into());
        let hot_hash = hash_prop_equality_key(&hot);
        let cold_hash = hash_prop_equality_key(&cold);

        let mut hot_props = BTreeMap::new();
        hot_props.insert("status".into(), hot.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", hot_props.clone())),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 7, hot_props)),
            2,
        );

        let mut cold_props = BTreeMap::new();
        cold_props.insert("status".into(), cold.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", cold_props.clone())),
            3,
        );
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 7, cold_props)),
            4,
        );
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 5,
            },
            5,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 6,
            },
            6,
        );

        assert_eq!(
            mt.secondary_eq_node_hash_count_at(18, hot_hash, 1).unwrap(),
            1
        );
        assert_eq!(
            mt.secondary_eq_node_hash_count_at(18, hot_hash, 3).unwrap(),
            0
        );
        assert_eq!(
            mt.secondary_eq_node_hash_count_at(18, cold_hash, 3)
                .unwrap(),
            1
        );
        assert_eq!(
            mt.secondary_eq_node_hash_count_at(18, cold_hash, 5)
                .unwrap(),
            0
        );
        assert_eq!(
            mt.secondary_eq_edge_hash_count_at(19, hot_hash, 2).unwrap(),
            1
        );
        assert_eq!(
            mt.secondary_eq_edge_hash_count_at(19, hot_hash, 4).unwrap(),
            0
        );
        assert_eq!(
            mt.secondary_eq_edge_hash_count_at(19, cold_hash, 4)
                .unwrap(),
            1
        );
        assert_eq!(
            mt.secondary_eq_edge_hash_count_at(19, cold_hash, 6)
                .unwrap(),
            0
        );
    }

    #[test]
    fn secondary_eq_updates_raw_structural_signed_zero_values_for_nodes_and_edges() {
        let mt = Memtable::new();
        let node_entry = SecondaryIndexManifestEntry {
            index_id: 20,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "p".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let edge_entry = SecondaryIndexManifestEntry {
            index_id: 21,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id: 7,
                prop_key: "p".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&node_entry).unwrap();
        mt.register_secondary_index(&edge_entry).unwrap();

        let pos_array = PropValue::Array(vec![PropValue::Float(0.0)]);
        let neg_array = PropValue::Array(vec![PropValue::Float(-0.0)]);
        let mut node_props = BTreeMap::new();
        node_props.insert("p".into(), pos_array.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", node_props)),
            1,
        );
        let mut node_props = BTreeMap::new();
        node_props.insert("p".into(), neg_array.clone());
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", node_props)),
            2,
        );

        assert_eq!(
            mt.find_secondary_eq_nodes_at(20, "p", &neg_array, 2)
                .unwrap(),
            vec![1]
        );
        assert!(mt
            .find_secondary_eq_nodes_at(20, "p", &pos_array, 2)
            .unwrap()
            .is_empty());

        let mut pos_map = BTreeMap::new();
        pos_map.insert("zero".into(), PropValue::Float(0.0));
        let pos_map = PropValue::Map(pos_map);
        let mut neg_map = BTreeMap::new();
        neg_map.insert("zero".into(), PropValue::Float(-0.0));
        let neg_map = PropValue::Map(neg_map);
        let mut edge_props = BTreeMap::new();
        edge_props.insert("p".into(), pos_map.clone());
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 7, edge_props)),
            3,
        );
        let mut edge_props = BTreeMap::new();
        edge_props.insert("p".into(), neg_map.clone());
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 7, edge_props)),
            4,
        );

        assert_eq!(
            mt.secondary_eq_edge_count_at(21, "p", &neg_map, 4).unwrap(),
            1
        );
        assert_eq!(
            mt.secondary_eq_edge_count_at(21, "p", &pos_map, 4).unwrap(),
            0
        );
    }

    #[test]
    fn semantic_equivalent_numeric_updates_do_not_churn_secondary_membership() {
        let mt = Memtable::new();
        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 30,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 31,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry).unwrap();
        mt.register_secondary_index(&range_entry).unwrap();

        let mut props = BTreeMap::new();
        props.insert("score".into(), PropValue::Int(42));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", props)),
            1,
        );
        let mut props = BTreeMap::new();
        props.insert("score".into(), PropValue::Float(42.0));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", props)),
            2,
        );

        let state = mt.state.read().unwrap();
        let value_hash = hash_prop_equality_key(&PropValue::Int(42));
        let eq_slot = state
            .secondary_eq_state
            .get(&30)
            .and_then(|groups| groups.get(&value_hash))
            .and_then(|members| members.get(&1))
            .unwrap();
        assert!(eq_slot.history.is_none());
        assert_eq!(eq_slot.head.write_seq, 1);
        assert_eq!(slot_option_current(eq_slot), Some(&()));

        let encoded_score = numeric_range_sort_key_for_value(&PropValue::Int(42)).unwrap();
        let range_slot = state
            .secondary_range_state
            .get(&31)
            .and_then(|entries| entries.get(&(encoded_score, 1)))
            .unwrap();
        assert!(range_slot.history.is_none());
        assert_eq!(range_slot.head.write_seq, 1);
        assert_eq!(slot_option_current(range_slot), Some(&()));
    }

    #[test]
    fn secondary_range_entries_limited_chunks_and_resumes() {
        let mt = Memtable::new();
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 40,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "score".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&range_entry).unwrap();

        for id in 1..=5 {
            let mut props = BTreeMap::new();
            props.insert("score".into(), PropValue::Int((id * 10) as i64));
            mt.apply_op(
                &WalOp::UpsertNode(make_node_with_props(id, 1, &format!("n{id}"), props)),
                id,
            );
        }

        let encoded_10 = numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap();
        let encoded_20 = numeric_range_sort_key_for_value(&PropValue::Int(20)).unwrap();
        let encoded_30 = numeric_range_sort_key_for_value(&PropValue::Int(30)).unwrap();
        let encoded_40 = numeric_range_sort_key_for_value(&PropValue::Int(40)).unwrap();
        let encoded_50 = numeric_range_sort_key_for_value(&PropValue::Int(50)).unwrap();

        let first = mt
            .visible_secondary_range_entries_limited(
                40,
                Some((encoded_10, true)),
                Some((encoded_50, true)),
                None,
                5,
                Some(2),
            )
            .unwrap();
        assert_eq!(first, vec![(encoded_10, 1), (encoded_20, 2)]);

        let second = mt
            .visible_secondary_range_entries_limited(
                40,
                Some((encoded_10, true)),
                Some((encoded_50, true)),
                first.last().copied(),
                5,
                Some(2),
            )
            .unwrap();
        assert_eq!(second, vec![(encoded_30, 3), (encoded_40, 4)]);

        assert_eq!(
            mt.visible_secondary_range_entries_limited(
                40,
                Some((encoded_20, true)),
                Some((encoded_20, true)),
                None,
                5,
                Some(10),
            )
            .unwrap(),
            vec![(encoded_20, 2)]
        );
        assert!(mt
            .visible_secondary_range_entries_limited(
                40,
                Some((encoded_10, true)),
                Some((encoded_50, true)),
                None,
                5,
                Some(0),
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn multi_label_secondary_memberships_are_maintained_by_declared_label() {
        let mt = Memtable::new();
        let eq_label_1 = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "color".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let eq_label_2 = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 2,
                prop_key: "color".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_label_2 = SecondaryIndexManifestEntry {
            index_id: 12,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 2,
                prop_key: "score".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_label_3 = SecondaryIndexManifestEntry {
            index_id: 13,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 3,
                prop_key: "score".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        for entry in [&eq_label_1, &eq_label_2, &range_label_2, &range_label_3] {
            mt.register_secondary_index(entry).unwrap();
        }

        let mut props = BTreeMap::new();
        props.insert("color".into(), PropValue::String("red".into()));
        props.insert("score".into(), PropValue::Int(42));
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

        assert_eq!(
            mt.find_secondary_eq_nodes_at(10, "color", &PropValue::String("red".into()), 1)
                .unwrap(),
            vec![1]
        );
        assert_eq!(
            mt.find_secondary_eq_nodes_at(11, "color", &PropValue::String("red".into()), 1)
                .unwrap(),
            vec![1]
        );
        let encoded_42 = numeric_range_sort_key_for_value(&PropValue::Int(42)).unwrap();
        assert_eq!(
            mt.visible_secondary_range_entries(
                12,
                Some((encoded_42, true)),
                Some((encoded_42, true)),
                None,
                1
            )
            .unwrap(),
            vec![(encoded_42, 1)]
        );
        assert_eq!(
            mt.visible_secondary_range_entries(
                13,
                Some((encoded_42, true)),
                Some((encoded_42, true)),
                None,
                1
            )
            .unwrap(),
            vec![(encoded_42, 1)]
        );

        let mut updated_props = BTreeMap::new();
        updated_props.insert("color".into(), PropValue::String("blue".into()));
        updated_props.insert("score".into(), PropValue::Int(50));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[2],
                "item",
                updated_props,
                200,
            )),
            2,
        );

        assert!(mt
            .find_secondary_eq_nodes_at(10, "color", &PropValue::String("red".into()), 2)
            .unwrap()
            .is_empty());
        assert!(mt
            .find_secondary_eq_nodes_at(11, "color", &PropValue::String("red".into()), 2)
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.find_secondary_eq_nodes_at(11, "color", &PropValue::String("blue".into()), 2)
                .unwrap(),
            vec![1]
        );
        let encoded_50 = numeric_range_sort_key_for_value(&PropValue::Int(50)).unwrap();
        assert!(mt
            .visible_secondary_range_entries(
                13,
                Some((encoded_42, true)),
                Some((encoded_42, true)),
                None,
                2
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.visible_secondary_range_entries(
                12,
                Some((encoded_50, true)),
                Some((encoded_50, true)),
                None,
                2
            )
            .unwrap(),
            vec![(encoded_50, 1)]
        );
    }

    #[test]
    fn node_compound_insert_update_missing_complex_and_delete_are_visible() {
        let mt = Memtable::new();
        let entry = node_field_entry(
            70,
            1,
            vec![
                property_field("tenant"),
                property_field("profile"),
                node_meta_field(NodeMetadataIndexFieldManifest::UpdatedAt),
            ],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&entry).unwrap();

        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        props.insert(
            "profile".into(),
            PropValue::Array(vec![PropValue::Float(f64::NAN)]),
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props.clone())),
            1,
        );

        let tenant = PropValue::String("acme".into());
        let prefix = prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &prefix, 1).unwrap(),
            vec![1]
        );
        assert_eq!(mt.count_node_compound_prefix_at(70, &prefix, 1).unwrap(), 1);

        let complex = props.get("profile").unwrap().clone();
        let complex_prefix = prefix_bounds_for(
            &entry,
            &[
                CompoundFieldValue::Property(Some(&tenant)),
                CompoundFieldValue::Property(Some(&complex)),
            ],
        );
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &complex_prefix, 1)
                .unwrap(),
            vec![1]
        );

        let mut unrelated_props = props.clone();
        unrelated_props.insert("other".into(), PropValue::String("ignored".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", unrelated_props)),
            2,
        );
        {
            let state = mt.state.read().unwrap();
            let entries = state.secondary_compound_state.get(&70).unwrap();
            let live_slots = entries
                .values()
                .filter(|slot| slot_option_current(slot).is_some())
                .collect::<Vec<_>>();
            assert_eq!(live_slots.len(), 1);
            assert!(live_slots[0].history.is_none());
            assert_eq!(live_slots[0].head.write_seq, 1);
        }

        let mut changed_props = props.clone();
        changed_props.insert("tenant".into(), PropValue::String("globex".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", changed_props)),
            3,
        );
        assert!(mt
            .find_node_compound_prefix_at(70, &prefix, 3)
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &prefix, 1).unwrap(),
            vec![1],
            "old snapshot keeps the old tuple visible"
        );
        let globex = PropValue::String("globex".into());
        let globex_prefix =
            prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&globex))]);
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &globex_prefix, 3)
                .unwrap(),
            vec![1]
        );

        let missing_entry = node_field_entry(
            71,
            1,
            vec![property_field("tenant"), property_field("missing_suffix")],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&missing_entry).unwrap();
        let missing_prefix = prefix_bounds_for(
            &missing_entry,
            &[CompoundFieldValue::Property(Some(&globex))],
        );
        assert_eq!(
            mt.find_node_compound_prefix_at(71, &missing_prefix, 3)
                .unwrap(),
            vec![1],
            "missing suffix still emits a tuple for prefix completeness"
        );

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 4,
            },
            4,
        );
        assert!(mt
            .find_node_compound_prefix_at(70, &globex_prefix, 4)
            .unwrap()
            .is_empty());
        assert!(mt
            .find_node_compound_prefix_at(71, &missing_prefix, 4)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn node_compound_metadata_range_and_label_moves_are_maintained() {
        let mt = Memtable::new();
        let entry = node_field_entry(
            72,
            2,
            vec![
                property_field("tenant"),
                node_meta_field(NodeMetadataIndexFieldManifest::UpdatedAt),
            ],
            SecondaryIndexKind::Range,
        );
        mt.register_secondary_index(&entry).unwrap();

        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[1],
                "alice",
                props.clone(),
                100,
            )),
            1,
        );
        let tenant = PropValue::String("acme".into());
        let prefix = prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        assert!(mt
            .find_node_compound_prefix_at(72, &prefix, 1)
            .unwrap()
            .is_empty());

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[1, 2],
                "alice",
                props.clone(),
                100,
            )),
            2,
        );
        assert_eq!(
            mt.find_node_compound_prefix_at(72, &prefix, 2).unwrap(),
            vec![1]
        );

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[1, 2],
                "alice",
                props.clone(),
                250,
            )),
            3,
        );
        let context = CompoundTupleContext::from_manifest_entry(&entry).unwrap();
        let equality_prefix =
            encode_compound_tuple_prefix(&context, &[CompoundFieldValue::Property(Some(&tenant))])
                .unwrap();
        let lower =
            encode_compound_field_component(&context, 1, CompoundFieldValue::MetadataI64(200))
                .unwrap();
        let upper =
            encode_compound_field_component(&context, 1, CompoundFieldValue::MetadataI64(300))
                .unwrap();
        let range =
            compound_range_bounds(&equality_prefix, Some((&lower, true)), Some((&upper, true)))
                .unwrap();
        assert_eq!(
            mt.find_node_compound_range_at(72, &range, 3).unwrap(),
            vec![1]
        );
        assert_eq!(mt.count_node_compound_range_at(72, &range, 3).unwrap(), 1);

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels_and_props(
                1,
                &[1],
                "alice",
                props,
                250,
            )),
            4,
        );
        assert!(mt
            .find_node_compound_prefix_at(72, &prefix, 4)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn node_compound_unrelated_label_writes_do_not_mutate_tuple_state() {
        let mt = Memtable::new();
        let entry = node_field_entry(
            73,
            2,
            vec![
                property_field("tenant"),
                node_meta_field(NodeMetadataIndexFieldManifest::Id),
            ],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&entry).unwrap();
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let before_state = mt.compound_secondary_state().unwrap();
        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            1,
        );

        assert_eq!(mt.compound_secondary_state().unwrap(), before_state);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn edge_compound_insert_update_label_move_metadata_and_delete_are_maintained() {
        let mt = Memtable::new();
        let entry = edge_field_entry(
            80,
            7,
            vec![
                edge_meta_field(EdgeMetadataIndexFieldManifest::From),
                edge_meta_field(EdgeMetadataIndexFieldManifest::To),
                edge_meta_field(EdgeMetadataIndexFieldManifest::ValidFrom),
                edge_meta_field(EdgeMetadataIndexFieldManifest::ValidTo),
                property_field("status"),
            ],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&entry).unwrap();

        let mut props = BTreeMap::new();
        props.insert("status".into(), PropValue::String("open".into()));
        let mut edge = make_edge_with_props(10, 1, 2, 7, props.clone());
        edge.valid_from = 10;
        edge.valid_to = 100;
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 1);

        let range_entry = edge_field_entry(
            81,
            7,
            vec![
                property_field("status"),
                edge_meta_field(EdgeMetadataIndexFieldManifest::ValidTo),
            ],
            SecondaryIndexKind::Range,
        );
        mt.register_secondary_index(&range_entry).unwrap();
        let status = PropValue::String("open".into());
        let range_context = CompoundTupleContext::from_manifest_entry(&range_entry).unwrap();
        let equality_prefix = encode_compound_tuple_prefix(
            &range_context,
            &[CompoundFieldValue::Property(Some(&status))],
        )
        .unwrap();
        let lower =
            encode_compound_field_component(&range_context, 1, CompoundFieldValue::MetadataI64(90))
                .unwrap();
        let upper = encode_compound_field_component(
            &range_context,
            1,
            CompoundFieldValue::MetadataI64(110),
        )
        .unwrap();
        let edge_range =
            compound_range_bounds(&equality_prefix, Some((&lower, true)), Some((&upper, true)))
                .unwrap();
        assert_eq!(
            mt.find_edge_compound_range_at(81, &edge_range, 1).unwrap(),
            vec![10]
        );
        assert_eq!(
            mt.count_edge_compound_range_at(81, &edge_range, 1).unwrap(),
            1
        );

        let prefix = prefix_bounds_for(
            &entry,
            &[
                CompoundFieldValue::MetadataU64(1),
                CompoundFieldValue::MetadataU64(2),
                CompoundFieldValue::MetadataI64(10),
                CompoundFieldValue::MetadataI64(100),
            ],
        );
        assert_eq!(
            mt.find_edge_compound_prefix_at(80, &prefix, 1).unwrap(),
            vec![10]
        );
        assert_eq!(mt.count_edge_compound_prefix_at(80, &prefix, 1).unwrap(), 1);

        edge.valid_to = 200;
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 2);
        assert!(mt
            .find_edge_compound_prefix_at(80, &prefix, 2)
            .unwrap()
            .is_empty());
        assert_eq!(
            mt.find_edge_compound_prefix_at(80, &prefix, 1).unwrap(),
            vec![10]
        );

        let moved_prefix = prefix_bounds_for(
            &entry,
            &[
                CompoundFieldValue::MetadataU64(1),
                CompoundFieldValue::MetadataU64(2),
                CompoundFieldValue::MetadataI64(10),
                CompoundFieldValue::MetadataI64(200),
            ],
        );
        assert_eq!(
            mt.find_edge_compound_prefix_at(80, &moved_prefix, 2)
                .unwrap(),
            vec![10]
        );

        let mut other_label = edge.clone();
        other_label.label_id = 8;
        mt.apply_op(&WalOp::UpsertEdge(other_label), 3);
        assert!(mt
            .find_edge_compound_prefix_at(80, &moved_prefix, 3)
            .unwrap()
            .is_empty());

        mt.apply_op(&WalOp::UpsertEdge(edge), 4);
        assert_eq!(
            mt.find_edge_compound_prefix_at(80, &moved_prefix, 4)
                .unwrap(),
            vec![10]
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 5,
            },
            5,
        );
        assert!(mt
            .find_edge_compound_prefix_at(80, &moved_prefix, 5)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn edge_compound_unrelated_label_writes_do_not_mutate_tuple_state() {
        let mt = Memtable::new();
        let entry = edge_field_entry(
            82,
            7,
            vec![
                property_field("status"),
                edge_meta_field(EdgeMetadataIndexFieldManifest::From),
            ],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&entry).unwrap();
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let before_state = mt.compound_secondary_state().unwrap();
        let mut props = BTreeMap::new();
        props.insert("status".into(), PropValue::String("open".into()));
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 8, props)),
            1,
        );

        assert_eq!(mt.compound_secondary_state().unwrap(), before_state);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn unregister_compound_index_removes_lookup_and_tuple_state() {
        let mt = Memtable::new();
        let entry = node_field_entry(
            90,
            1,
            vec![
                property_field("tenant"),
                node_meta_field(NodeMetadataIndexFieldManifest::Id),
            ],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&entry).unwrap();
        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            1,
        );

        let tenant = PropValue::String("acme".into());
        let prefix = prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        assert_eq!(
            mt.find_node_compound_prefix_at(90, &prefix, 1).unwrap(),
            vec![1]
        );
        assert!(mt.compound_secondary_state().unwrap().contains_key(&90));
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        assert!(mt.unregister_secondary_index(90));
        assert!(mt
            .find_node_compound_prefix_at(90, &prefix, 1)
            .unwrap()
            .is_empty());
        assert!(!mt.compound_secondary_state().unwrap().contains_key(&90));
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn same_write_seq_replace_overwrites_head_in_place() {
        let mut slot = VersionedSlot::new(1, 10u64);
        slot.replace(2, 20);
        slot.replace(2, 30);

        assert_eq!(*slot.current(), 30);
        assert_eq!(slot.at(1), Some(&10));
        assert_eq!(slot.at(2), Some(&30));
        assert_eq!(slot.history.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn versioned_slot_binary_search_preserves_all_snapshot_boundaries() {
        let mut slot = VersionedSlot::new(10, 100u64);
        assert_eq!(slot.at(0), None);
        assert_eq!(slot.at(9), None);
        assert_eq!(slot.at(10), Some(&100));
        assert_eq!(slot.at(u64::MAX), Some(&100));

        slot.replace(20, 200);
        slot.replace(30, 300);
        slot.replace(40, 400);
        assert_eq!(slot.at(9), None);
        assert_eq!(slot.at(10), Some(&100));
        assert_eq!(slot.at(19), Some(&100));
        assert_eq!(slot.at(20), Some(&200));
        assert_eq!(slot.at(29), Some(&200));
        assert_eq!(slot.at(30), Some(&300));
        assert_eq!(slot.at(39), Some(&300));
        assert_eq!(slot.at(40), Some(&400));
        assert_eq!(slot.at(41), Some(&400));
    }

    #[test]
    fn versioned_slot_old_snapshot_lookup_is_logarithmic() {
        let mut slot = VersionedSlot::new(0, 0u64);
        for write_seq in 1..=4096 {
            slot.replace(write_seq, write_seq);
        }
        reset_versioned_slot_lookup_comparisons_for_test();
        assert_eq!(slot.at(2047), Some(&2047));
        assert!(
            versioned_slot_lookup_comparisons_for_test() <= 13,
            "upper-bound lookup over 4,096 historical versions must be logarithmic"
        );
    }

    #[test]
    #[should_panic(expected = "versioned slot write sequence must be monotonic")]
    fn versioned_slot_rejects_lower_write_sequence_in_every_build() {
        let mut slot = VersionedSlot::new(10, 1u64);
        slot.replace(9, 2);
    }

    fn ordered_group_snapshot(
        mt: &Memtable,
        direction: Direction,
        key: (u64, u32),
        snapshot_seq: u64,
    ) -> (u64, bool, usize, u64) {
        let state = mt.state.read().unwrap();
        let groups = match direction {
            Direction::Outgoing => &state.ordered_adj_out_groups,
            Direction::Incoming => &state.ordered_adj_in_groups,
            Direction::Both => unreachable!(),
        };
        let group = groups.get(&key).expect("ordered group must be retained");
        (
            group.current_count,
            slot_option_visible(&group.membership, snapshot_seq),
            group.membership.history.as_ref().map_or(0, Vec::len),
            group.membership.head.write_seq,
        )
    }

    #[test]
    fn ordered_adjacency_groups_track_moves_relabels_deletes_and_resurrection() {
        let mt = Memtable::new();
        assert_eq!(mt.estimated_size(), 0);
        assert!(mt.retained_edge_record_keys_is_empty());

        let edge = make_edge(10, 1, 2, 5);
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 1);
        assert!(!ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 0).1);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 1),
            (1, true, 0, 1)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (2, 5), 1),
            (1, true, 0, 1)
        );
        assert_eq!(
            mt.retained_ordered_adj_outer_group_count(Direction::Outgoing),
            1
        );
        assert_eq!(
            mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
            1
        );
        assert!(!mt.retained_edge_record_keys_is_empty());
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let mut metadata_only = edge.clone();
        metadata_only.weight = 9.0;
        mt.apply_op(&WalOp::UpsertEdge(metadata_only), 2);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 2),
            (1, true, 0, 1)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (2, 5), 2),
            (1, true, 0, 1)
        );

        let moved_to = make_edge(10, 1, 3, 5);
        mt.apply_op(&WalOp::UpsertEdge(moved_to), 3);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 3),
            (1, true, 0, 1)
        );
        assert!(ordered_group_snapshot(&mt, Direction::Incoming, (2, 5), 2).1);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (2, 5), 3),
            (0, false, 1, 3)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (3, 5), 3),
            (1, true, 0, 3)
        );

        let relabeled = make_edge(10, 1, 3, 6);
        mt.apply_op(&WalOp::UpsertEdge(relabeled), 4);
        assert!(ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 3).1);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 5), 4),
            (0, false, 1, 4)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 6), 4),
            (1, true, 0, 4)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (3, 5), 4),
            (0, false, 1, 4)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (3, 6), 4),
            (1, true, 0, 4)
        );

        let moved_and_relabeled = make_edge(10, 4, 5, 7);
        mt.apply_op(&WalOp::UpsertEdge(moved_and_relabeled.clone()), 5);
        assert!(!ordered_group_snapshot(&mt, Direction::Outgoing, (1, 6), 5).1);
        assert!(!ordered_group_snapshot(&mt, Direction::Incoming, (3, 6), 5).1);
        assert!(ordered_group_snapshot(&mt, Direction::Outgoing, (4, 7), 5).1);
        assert!(ordered_group_snapshot(&mt, Direction::Incoming, (5, 7), 5).1);

        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 6,
            },
            6,
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (4, 7), 6),
            (0, false, 1, 6)
        );
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Incoming, (5, 7), 6),
            (0, false, 1, 6)
        );
        let sizes_after_delete = (
            mt.retained_ordered_adj_outer_group_count(Direction::Outgoing),
            mt.retained_ordered_adj_outer_group_count(Direction::Incoming),
            mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
            mt.retained_ordered_adj_posting_slot_count(Direction::Incoming),
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 7,
            },
            7,
        );
        assert_eq!(
            sizes_after_delete,
            (
                mt.retained_ordered_adj_outer_group_count(Direction::Outgoing),
                mt.retained_ordered_adj_outer_group_count(Direction::Incoming),
                mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
                mt.retained_ordered_adj_posting_slot_count(Direction::Incoming),
            )
        );

        mt.apply_op(&WalOp::UpsertEdge(moved_and_relabeled), 8);
        assert!(!ordered_group_snapshot(&mt, Direction::Outgoing, (4, 7), 6).1);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (4, 7), 8),
            (1, true, 2, 8)
        );
        assert_eq!(
            mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
            2
        );
        assert_eq!(
            mt.retained_ordered_adj_posting_slot_count(Direction::Incoming),
            3
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn ordered_adjacency_same_sequence_crossings_collapse_to_final_membership() {
        let mt = Memtable::new();
        let edge = make_edge(1, 10, 20, 30);
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 5);
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 5,
            },
            5,
        );
        mt.apply_op(&WalOp::UpsertEdge(edge), 5);
        assert!(!ordered_group_snapshot(&mt, Direction::Outgoing, (10, 30), 4).1);
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (10, 30), 5),
            (1, true, 0, 5)
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn ordered_adjacency_churn_retains_physical_keys_and_versions_only_zero_crossings() {
        let mt = Memtable::new();
        for id in 0..1024u64 {
            mt.apply_op(
                &WalOp::UpsertEdge(make_edge(id, id, id + 10_000, 7)),
                id * 2 + 1,
            );
            mt.apply_op(
                &WalOp::DeleteEdge {
                    id,
                    deleted_at: (id * 2 + 2) as i64,
                },
                id * 2 + 2,
            );
        }
        assert_eq!(
            mt.retained_ordered_adj_outer_group_count(Direction::Outgoing),
            1024
        );
        assert_eq!(
            mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
            1024
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let sentinel = make_edge(10_000, 50_000, 60_000, 9);
        mt.apply_op(&WalOp::UpsertEdge(sentinel), 10_000);
        for offset in 1..=512u64 {
            let id = 10_000 + offset;
            mt.apply_op(
                &WalOp::UpsertEdge(make_edge(id, 50_000, 70_000 + offset, 9)),
                10_000 + offset * 2 - 1,
            );
            mt.apply_op(
                &WalOp::DeleteEdge {
                    id,
                    deleted_at: (10_000 + offset * 2) as i64,
                },
                10_000 + offset * 2,
            );
        }
        let group = ordered_group_snapshot(&mt, Direction::Outgoing, (50_000, 9), u64::MAX);
        assert_eq!(group, (1, true, 0, 10_000));
        let refill = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                50_000,
                Some(&[9]),
                u64::MAX,
                Bound::Unbounded,
                4096,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(refill.examined, 513);
        assert_eq!(refill.entries.len(), 1);
        assert!(refill.exhausted);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn ordered_adjacency_hot_group_history_tracks_only_zero_crossings() {
        let mt = Memtable::new();
        let edge = make_edge(1, 1, 2, 3);
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 1);
        for cycle in 0..64u64 {
            let delete_seq = cycle * 2 + 2;
            mt.apply_op(
                &WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: delete_seq as i64,
                },
                delete_seq,
            );
            mt.apply_op(&WalOp::UpsertEdge(edge.clone()), delete_seq + 1);
        }
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 3), u64::MAX),
            (1, true, 128, 129)
        );
        reset_versioned_slot_lookup_comparisons_for_test();
        assert_eq!(
            ordered_group_snapshot(&mt, Direction::Outgoing, (1, 3), 64),
            (1, false, 128, 129)
        );
        assert!(
            versioned_slot_lookup_comparisons_for_test() <= 8,
            "old-snapshot lookup over 128 zero-crossing versions must be logarithmic"
        );
        assert_eq!(
            mt.retained_ordered_adj_posting_slot_count(Direction::Outgoing),
            1
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    #[should_panic(expected = "ordered adjacency group count underflow")]
    fn ordered_adjacency_group_count_rejects_underflow() {
        MemtableState::change_ordered_adj_group(&mut BTreeMap::new(), (1, 2), false, 1);
    }

    #[test]
    #[should_panic(expected = "ordered adjacency group count overflow")]
    fn ordered_adjacency_group_count_rejects_overflow() {
        let mut groups = BTreeMap::from([(
            (1, 2),
            OrderedAdjacencyGroupState {
                current_count: u64::MAX,
                membership: VersionedSlot::new(1, Some(())),
            },
        )]);
        MemtableState::change_ordered_adj_group(&mut groups, (1, 2), true, 2);
    }

    #[test]
    fn ordered_adjacency_refills_are_bounded_owned_and_resume_exclusively() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(0, 0, 9, 0)), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 0, 8, u32::MAX)), 2);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 4, 7, 7)), 3);
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge(u64::MAX, u64::MAX, 6, u32::MAX)),
            4,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 5,
            },
            5,
        );

        let empty = mt
            .refill_ordered_adj_owner_at(Direction::Outgoing, Some(&[]), 5, Bound::Unbounded, 1)
            .unwrap();
        assert_eq!(
            (empty.owner_head, empty.examined, empty.exhausted),
            (None, 0, true)
        );

        let first = mt
            .refill_ordered_adj_owner_at(Direction::Outgoing, Some(&[7]), 5, Bound::Unbounded, 1)
            .unwrap();
        assert_eq!(first.owner_head, None);
        assert_eq!(first.last_group_key_examined, Some((0, 0)));
        assert_eq!(first.examined, 1);
        assert!(!first.exhausted);
        let second = mt
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                Some(&[7]),
                5,
                Bound::Excluded(first.last_group_key_examined.unwrap()),
                4096,
            )
            .unwrap();
        assert_eq!(second.owner_head, Some(4));
        assert_eq!(second.examined, 2);
        let after_owner_zero = mt
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                Some(&[0, 7, u32::MAX]),
                5,
                Bound::Excluded((0, u32::MAX)),
                4096,
            )
            .unwrap();
        assert_eq!(after_owner_zero.owner_head, Some(4));

        let scratch = Vec::with_capacity(8);
        let postings = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                None,
                5,
                Bound::Unbounded,
                1,
                scratch,
            )
            .unwrap();
        assert_eq!(postings.examined, 1);
        assert_eq!(postings.last_edge_id_examined, Some(0));
        assert_eq!(
            postings
                .entries
                .iter()
                .map(|entry| entry.edge_id)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert!(postings.entries.capacity() >= 8);
        assert!(!postings.exhausted);
        let postings = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                None,
                5,
                Bound::Excluded(postings.last_edge_id_examined.unwrap()),
                4096,
                postings.entries,
            )
            .unwrap();
        assert_eq!(postings.examined, 1, "deleted physical slot still counts");
        assert!(postings.entries.is_empty());
        assert!(postings.exhausted);

        let nonmatching = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                Some(&[7]),
                4,
                Bound::Unbounded,
                4096,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(nonmatching.examined, 2);
        assert!(nonmatching.entries.is_empty());
        let version_invisible = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                None,
                0,
                Bound::Unbounded,
                4096,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(version_invisible.examined, 2);
        assert!(version_invisible.entries.is_empty());
        let known_empty_postings = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                Some(&[]),
                5,
                Bound::Unbounded,
                4096,
                Vec::with_capacity(4),
            )
            .unwrap();
        assert_eq!(
            (
                known_empty_postings.examined,
                known_empty_postings.exhausted,
                known_empty_postings.entries.capacity(),
            ),
            (0, true, 4)
        );

        let max_owner = mt
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                Some(&[u32::MAX]),
                5,
                Bound::Included((u64::MAX, u32::MIN)),
                4096,
            )
            .unwrap();
        assert_eq!(max_owner.owner_head, Some(u64::MAX));
        let owned = mt
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                u64::MAX,
                Some(&[u32::MAX]),
                5,
                Bound::Unbounded,
                4096,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(owned.entries[0].edge_id, u64::MAX);
        let incoming = mt
            .refill_ordered_adj_owner_at(
                Direction::Incoming,
                Some(&[u32::MAX]),
                5,
                Bound::Included((6, u32::MIN)),
                4096,
            )
            .unwrap();
        assert_eq!(incoming.owner_head, Some(6));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(99, 100, 101, 9)), 6);
        assert_eq!(
            owned.entries[0].edge_id,
            u64::MAX,
            "refill result owns its entries"
        );
    }

    #[test]
    #[should_panic(expected = "ordered adjacency owner refill maximum must be positive")]
    fn ordered_adjacency_owner_refill_rejects_zero_maximum() {
        Memtable::new()
            .refill_ordered_adj_owner_at(Direction::Outgoing, None, 0, Bound::Unbounded, 0)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "ordered adjacency posting refill maximum must be positive")]
    fn ordered_adjacency_posting_refill_rejects_zero_maximum() {
        Memtable::new()
            .refill_ordered_adj_postings_at(
                Direction::Outgoing,
                0,
                None,
                0,
                Bound::Unbounded,
                0,
                Vec::new(),
            )
            .unwrap();
    }

    #[test]
    fn unchanged_indexed_props_do_not_accumulate_secondary_history() {
        let mt = Memtable::new();
        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "name".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "age".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        mt.register_secondary_index(&eq_entry).unwrap();
        mt.register_secondary_index(&range_entry).unwrap();

        let mut props = BTreeMap::new();
        props.insert("name".into(), PropValue::String("alice".into()));
        props.insert("age".into(), PropValue::Int(42));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", props.clone())),
            1,
        );
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "a", props)),
            2,
        );

        let state = mt.state.read().unwrap();
        let eq_slot = state
            .secondary_eq_state
            .get(&10)
            .and_then(|groups| {
                groups.get(&hash_prop_equality_key(&PropValue::String("alice".into())))
            })
            .and_then(|members| members.get(&1))
            .unwrap();
        assert!(eq_slot.history.is_none());
        assert_eq!(eq_slot.head.write_seq, 1);
        assert_eq!(slot_option_current(eq_slot), Some(&()));

        let encoded_age = numeric_range_sort_key_for_value(&PropValue::Int(42)).unwrap();
        let range_slot = state
            .secondary_range_state
            .get(&11)
            .and_then(|entries| entries.get(&(encoded_age, 1)))
            .unwrap();
        assert!(range_slot.history.is_none());
        assert_eq!(range_slot.head.write_seq, 1);
        assert_eq!(slot_option_current(range_slot), Some(&()));
    }

    #[test]
    fn estimated_size_grows_with_history() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        let initial = mt.estimated_size();
        assert_eq!(initial, mt.estimated_size_full_for_test());
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 2);
        let after_overwrite = mt.estimated_size();
        assert_eq!(after_overwrite, mt.estimated_size_full_for_test());

        assert!(after_overwrite > initial);
    }

    #[test]
    fn estimated_size_matches_full_recompute_after_mvcc_churn() {
        let mt = Memtable::new();

        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let mut props = BTreeMap::new();
        props.insert("name".into(), PropValue::String("alice".into()));
        props.insert("age".into(), PropValue::Int(42));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            2,
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")), 3);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 4);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 50,
            },
            5,
        );
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 60,
            },
            6,
        );
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn estimated_size_matches_full_recompute_after_secondary_index_registration() {
        let mt = Memtable::new();

        let mut props = BTreeMap::new();
        props.insert("name".into(), PropValue::String("alice".into()));
        props.insert("age".into(), PropValue::Int(42));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            1,
        );

        let eq_entry = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "name".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "age".into(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };

        mt.register_secondary_index(&eq_entry).unwrap();
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        mt.register_secondary_index(&range_entry).unwrap();
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        mt.unregister_secondary_index(10);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        mt.unregister_secondary_index(11);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }

    #[test]
    fn current_helpers_track_visible_key_counts() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 10,
            },
            2,
        );

        assert_eq!(mt.node_key_index_key_count(), 0);
        assert_eq!(mt.label_node_index_key_count(), 0);
        assert_eq!(mt.time_node_index_len(), 0);
        assert!(mt.get_node_at(1, u64::MAX).unwrap().is_none());
        assert_eq!(mt.max_node_id(), 1);
    }
    #[test]
    fn p45b1_legacy_tombstone_scan_ignores_unrelated_live_records() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "one")), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 1)), 2);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 3,
            },
            3,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 4,
            },
            4,
        );
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "resurrected")), 5);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 1)), 6);
        let check = || {
            mt.take_tombstone_scan_work_for_test();
            assert_eq!(
                mt.collect_deleted_nodes_at(4).unwrap(),
                NodeIdSet::from_iter([1])
            );
            assert_eq!(
                mt.collect_deleted_edges_at(4).unwrap(),
                NodeIdSet::from_iter([1])
            );
            assert_eq!(mt.take_tombstone_scan_work_for_test(), 2);
            assert!(mt.collect_deleted_nodes_at(u64::MAX).unwrap().is_empty());
            assert!(mt.collect_deleted_edges_at(u64::MAX).unwrap().is_empty());
            assert_eq!(mt.take_tombstone_scan_work_for_test(), 2);
        };
        check();
        for id in 2..1002 {
            mt.apply_op(
                &WalOp::UpsertNode(make_node(id, 1, &format!("live{id}"))),
                id * 2 + 10,
            );
            mt.apply_op(&WalOp::UpsertEdge(make_edge(id, 1, id + 1, 1)), id * 2 + 11);
        }
        check(); // Exactly two candidate IDs, despite 2,000 unrelated live rows.
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 9000,
            },
            9000,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 2,
                deleted_at: 9001,
            },
            9001,
        );
        assert_eq!(
            mt.collect_deleted_nodes_at(4).unwrap(),
            NodeIdSet::from_iter([1])
        );
        assert_eq!(
            mt.collect_deleted_edges_at(4).unwrap(),
            NodeIdSet::from_iter([1])
        );
        assert_eq!(mt.take_tombstone_scan_work_for_test(), 4);
        assert_eq!(
            mt.collect_deleted_nodes_at(u64::MAX).unwrap(),
            NodeIdSet::from_iter([2])
        );
        assert_eq!(
            mt.collect_deleted_edges_at(u64::MAX).unwrap(),
            NodeIdSet::from_iter([2])
        );
    }
}

#[cfg(test)]
mod post45_read_legacy_tests {
    use super::*;
    #[test]
    fn post45_read_legacy_separate_tombstones_survive_missing_records() {
        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 7,
                deleted_at: 10,
            },
            1,
        );
        mt.apply_op(
            &WalOp::DeleteEdge {
                id: 7,
                deleted_at: 10,
            },
            1,
        );
        // Exercise the independent legacy tombstone representation explicitly.
        {
            let mut state = mt.state.write().unwrap();
            state.nodes.remove(&7);
            state.edges.remove(&7);
        }
        for (seq, deleted) in [(0, false), (1, true)] {
            let mut nodes = vec![NodeVisibilityState::Missing; 2];
            let remaining = mt
                .batch_get_node_visibility_meta_at(&[(0, 7), (1, 7)], seq, &mut nodes)
                .unwrap();
            assert_eq!(
                nodes[0],
                if deleted {
                    NodeVisibilityState::Deleted
                } else {
                    NodeVisibilityState::Missing
                }
            );
            assert_eq!(remaining.is_empty(), deleted);
            use crate::edge_metadata::EdgeMetadataVisibilityState;
            let mut edges = vec![EdgeMetadataVisibilityState::Missing; 2];
            let remaining = mt
                .batch_get_edge_metadata_at(&[(0, 7), (1, 7)], seq, &mut edges)
                .unwrap();
            assert_eq!(
                edges[0],
                if deleted {
                    EdgeMetadataVisibilityState::Deleted
                } else {
                    EdgeMetadataVisibilityState::Missing
                }
            );
            assert_eq!(remaining.is_empty(), deleted);
            assert_eq!(
                mt.edge_visibility_state_at(7, seq).unwrap(),
                if deleted {
                    EdgeVisibilityState::Deleted
                } else {
                    EdgeVisibilityState::Missing
                }
            );
        }
    }
}
