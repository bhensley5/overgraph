use crate::edge_metadata::EdgeMetadataCandidate;
#[cfg(test)]
use crate::edge_metadata::{i64_matches_bounds, weight_matches_bounds, RangeBoundFlags};
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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Bound, ControlFlow};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

#[cfg(test)]
static ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_endpoint_cursor_entries_visited_for_test() {
    ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn endpoint_cursor_entries_visited_for_test() -> usize {
    ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.load(Ordering::Relaxed)
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
        if self.head.write_seq == write_seq {
            self.head.value = value;
            return;
        }
        self.history
            .get_or_insert_with(Vec::new)
            .push(self.head.clone());
        self.head = SlotVersion { write_seq, value };
    }

    fn current(&self) -> &T {
        &self.head.value
    }

    fn at(&self, snapshot_seq: u64) -> Option<&T> {
        if self.head.write_seq <= snapshot_seq {
            return Some(&self.head.value);
        }
        self.history
            .as_ref()?
            .iter()
            .rev()
            .find_map(|version| (version.write_seq <= snapshot_seq).then_some(&version.value))
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
    estimated_bytes: usize,
    live_node_count: usize,
    live_edge_count: usize,
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
    label_edge_index: HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
    time_node_index: BTreeMap<(u32, i64, u64), MembershipSlot<()>>,
    secondary_index_declarations: HashMap<u64, SecondaryIndexManifestEntry>,
    secondary_eq_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    secondary_range_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    secondary_edge_eq_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    secondary_edge_range_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    secondary_node_field_indexes_by_label: HashMap<u32, Vec<u64>>,
    secondary_edge_field_indexes_by_label: HashMap<u32, Vec<u64>>,
    secondary_eq_state: SecondaryEqState,
    secondary_range_state: SecondaryRangeState,
    secondary_compound_state: CompoundSecondaryIndexState,
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
fn compound_start_bound(lower: Option<&CompoundLowerBound>) -> Bound<(Vec<u8>, u64)> {
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
fn current_compound_secondary_state(
    source: &CompoundSecondaryIndexState,
) -> HashMap<u64, BTreeSet<(Vec<u8>, u64)>> {
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
        let (before, after) = Self::set_ordered_adj_slot(
            &mut self.ordered_adj_out,
            owner_id,
            member_id,
            value,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
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
        let (before, after) = Self::set_ordered_adj_slot(
            &mut self.ordered_adj_in,
            owner_id,
            member_id,
            value,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
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
            .secondary_index_declarations
            .values()
            .map(estimate_secondary_decl_entry)
            .sum();
        let secondary_eq_lookup_size: usize = self
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
            .secondary_node_field_indexes_by_label
            .values()
            .map(|index_ids| estimate_secondary_field_lookup_entry(index_ids))
            .sum();
        let secondary_edge_field_lookup_size: usize = self
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

    fn node_tombstone_current(&self, id: u64) -> Option<TombstoneEntry> {
        self.node_tombstones
            .get(&id)
            .and_then(slot_option_current)
            .and_then(|_| {
                match self
                    .nodes
                    .get(&id)
                    .and_then(|slot| record_at(slot, u64::MAX))
                {
                    Some(RecordState::Tombstone(entry)) => Some(*entry),
                    _ => None,
                }
            })
    }

    fn edge_tombstone_current(&self, id: u64) -> Option<TombstoneEntry> {
        self.edge_tombstones
            .get(&id)
            .and_then(slot_option_current)
            .and_then(|_| {
                match self
                    .edges
                    .get(&id)
                    .and_then(|slot| record_at(slot, u64::MAX))
                {
                    Some(RecordState::Tombstone(entry)) => Some(*entry),
                    _ => None,
                }
            })
    }

    fn collect_secondary_index_entries_for_node_label(
        &self,
        label_id: u32,
        node_id: u64,
        props: &BTreeMap<String, PropValue>,
        present: bool,
        eq_actions: &mut Vec<(u64, u64, u64, bool)>,
        range_actions: &mut Vec<(u64, NumericRangeSortKey, u64, bool)>,
    ) {
        let eq_by_prop = self.secondary_eq_by_prop.get(&label_id);
        let range_by_prop = self.secondary_range_by_prop.get(&label_id);
        if eq_by_prop.is_none() && range_by_prop.is_none() {
            return;
        }

        for (prop_key, prop_value) in props {
            if let Some(index_ids) = eq_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str())) {
                let value_hash = hash_prop_equality_key(prop_value);
                for &index_id in index_ids {
                    eq_actions.push((index_id, value_hash, node_id, present));
                }
            }

            if let Some(indexes) = range_by_prop.and_then(|by_prop| by_prop.get(prop_key.as_str()))
            {
                for &index_id in indexes {
                    if let Some(encoded) = numeric_range_sort_key_for_value(prop_value) {
                        range_actions.push((index_id, encoded, node_id, present));
                    }
                }
            }
        }
    }

    fn collect_secondary_index_updates_for_node_label(
        &self,
        label_id: u32,
        old_node: &NodeRecord,
        new_node: &NodeRecord,
        eq_actions: &mut Vec<(u64, u64, u64, bool)>,
        range_actions: &mut Vec<(u64, NumericRangeSortKey, u64, bool)>,
    ) {
        if let Some(by_prop) = self.secondary_eq_by_prop.get(&label_id) {
            for (prop_key, index_ids) in by_prop {
                let old_value = old_node.props.get(prop_key.as_str());
                let new_value = new_node.props.get(prop_key.as_str());
                if optional_semantic_property_eq(old_value, new_value) {
                    continue;
                }
                if let Some(old_value) = old_value {
                    let value_hash = hash_prop_equality_key(old_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, old_node.id, false));
                    }
                }
                if let Some(new_value) = new_value {
                    let value_hash = hash_prop_equality_key(new_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, new_node.id, true));
                    }
                }
            }
        }

        if let Some(by_prop) = self.secondary_range_by_prop.get(&label_id) {
            for (prop_key, indexes) in by_prop {
                let old_value = old_node.props.get(prop_key.as_str());
                let new_value = new_node.props.get(prop_key.as_str());
                let old_encoded = old_value.and_then(numeric_range_sort_key_for_value);
                let new_encoded = new_value.and_then(numeric_range_sort_key_for_value);
                if old_encoded == new_encoded {
                    continue;
                }
                for &index_id in indexes {
                    if let Some(encoded) = old_encoded {
                        range_actions.push((index_id, encoded, old_node.id, false));
                    }
                    if let Some(encoded) = new_encoded {
                        range_actions.push((index_id, encoded, new_node.id, true));
                    }
                }
            }
        }
    }

    fn collect_compound_index_entries_for_node_label(
        &self,
        label_id: u32,
        node: &NodeRecord,
        present: bool,
        actions: &mut Vec<CompoundAction>,
    ) {
        let Some(index_ids) = self.secondary_node_field_indexes_by_label.get(&label_id) else {
            return;
        };
        for &index_id in index_ids {
            let entry = self
                .secondary_index_declarations
                .get(&index_id)
                .expect("node field-index lookup points at a declaration");
            let SecondaryIndexTarget::NodeFieldIndex { fields, .. } = &entry.target else {
                unreachable!("node field-index lookup points at non-node-field target");
            };
            actions.push((
                index_id,
                encode_node_compound_tuple_key(label_id, fields, node),
                node.id,
                present,
            ));
        }
    }

    fn collect_compound_index_updates_for_node_label(
        &self,
        label_id: u32,
        old_node: &NodeRecord,
        new_node: &NodeRecord,
        actions: &mut Vec<CompoundAction>,
    ) {
        let Some(index_ids) = self.secondary_node_field_indexes_by_label.get(&label_id) else {
            return;
        };
        for &index_id in index_ids {
            let entry = self
                .secondary_index_declarations
                .get(&index_id)
                .expect("node field-index lookup points at a declaration");
            let SecondaryIndexTarget::NodeFieldIndex { fields, .. } = &entry.target else {
                unreachable!("node field-index lookup points at non-node-field target");
            };
            let old_key = encode_node_compound_tuple_key(label_id, fields, old_node);
            let new_key = encode_node_compound_tuple_key(label_id, fields, new_node);
            if old_key == new_key {
                continue;
            }
            actions.push((index_id, old_key, old_node.id, false));
            actions.push((index_id, new_key, new_node.id, true));
        }
    }

    fn add_secondary_index_entries_for_node(&mut self, node: &NodeRecord, write_seq: u64) {
        if self.secondary_eq_by_prop.is_empty()
            && self.secondary_range_by_prop.is_empty()
            && self.secondary_node_field_indexes_by_label.is_empty()
        {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        for &label_id in node.label_ids.as_slice() {
            self.collect_secondary_index_entries_for_node_label(
                label_id,
                node.id,
                &node.props,
                true,
                &mut eq_actions,
                &mut range_actions,
            );
            self.collect_compound_index_entries_for_node_label(
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
        if self.secondary_eq_by_prop.is_empty()
            && self.secondary_range_by_prop.is_empty()
            && self.secondary_node_field_indexes_by_label.is_empty()
        {
            return;
        }
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        let mut compound_actions = Vec::new();
        for &label_id in node.label_ids.as_slice() {
            self.collect_secondary_index_entries_for_node_label(
                label_id,
                node.id,
                &node.props,
                false,
                &mut eq_actions,
                &mut range_actions,
            );
            self.collect_compound_index_entries_for_node_label(
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
        if self.secondary_eq_by_prop.is_empty()
            && self.secondary_range_by_prop.is_empty()
            && self.secondary_node_field_indexes_by_label.is_empty()
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
                self.collect_secondary_index_entries_for_node_label(
                    old_label_id,
                    old_node.id,
                    &old_node.props,
                    false,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.collect_compound_index_entries_for_node_label(
                    old_label_id,
                    old_node,
                    false,
                    &mut compound_actions,
                );
            }
        }

        for &new_label_id in new_node.label_ids.as_slice() {
            if old_node.label_ids.contains(new_label_id) {
                self.collect_secondary_index_updates_for_node_label(
                    new_label_id,
                    old_node,
                    new_node,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.collect_compound_index_updates_for_node_label(
                    new_label_id,
                    old_node,
                    new_node,
                    &mut compound_actions,
                );
            } else {
                self.collect_secondary_index_entries_for_node_label(
                    new_label_id,
                    new_node.id,
                    &new_node.props,
                    true,
                    &mut eq_actions,
                    &mut range_actions,
                );
                self.collect_compound_index_entries_for_node_label(
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

    fn collect_compound_index_entries_for_edge_label(
        &self,
        label_id: u32,
        edge: &EdgeRecord,
        present: bool,
        actions: &mut Vec<CompoundAction>,
    ) {
        let Some(index_ids) = self.secondary_edge_field_indexes_by_label.get(&label_id) else {
            return;
        };
        for &index_id in index_ids {
            let entry = self
                .secondary_index_declarations
                .get(&index_id)
                .expect("edge field-index lookup points at a declaration");
            let SecondaryIndexTarget::EdgeFieldIndex { fields, .. } = &entry.target else {
                unreachable!("edge field-index lookup points at non-edge-field target");
            };
            actions.push((
                index_id,
                encode_edge_compound_tuple_key(label_id, fields, edge),
                edge.id,
                present,
            ));
        }
    }

    fn collect_compound_index_updates_for_edge_label(
        &self,
        label_id: u32,
        old_edge: &EdgeRecord,
        new_edge: &EdgeRecord,
        actions: &mut Vec<CompoundAction>,
    ) {
        let Some(index_ids) = self.secondary_edge_field_indexes_by_label.get(&label_id) else {
            return;
        };
        for &index_id in index_ids {
            let entry = self
                .secondary_index_declarations
                .get(&index_id)
                .expect("edge field-index lookup points at a declaration");
            let SecondaryIndexTarget::EdgeFieldIndex { fields, .. } = &entry.target else {
                unreachable!("edge field-index lookup points at non-edge-field target");
            };
            let old_key = encode_edge_compound_tuple_key(label_id, fields, old_edge);
            let new_key = encode_edge_compound_tuple_key(label_id, fields, new_edge);
            if old_key == new_key {
                continue;
            }
            actions.push((index_id, old_key, old_edge.id, false));
            actions.push((index_id, new_key, new_edge.id, true));
        }
    }

    fn add_secondary_index_entries_for_edge(&mut self, edge: &EdgeRecord, write_seq: u64) {
        if self.secondary_edge_eq_by_prop.is_empty()
            && self.secondary_edge_range_by_prop.is_empty()
            && self.secondary_edge_field_indexes_by_label.is_empty()
        {
            return;
        }
        let eq_by_prop = self.secondary_edge_eq_by_prop.get(&edge.label_id);
        let range_by_prop = self.secondary_edge_range_by_prop.get(&edge.label_id);
        let has_compound = self
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
        self.collect_compound_index_entries_for_edge_label(
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
        if self.secondary_edge_eq_by_prop.is_empty()
            && self.secondary_edge_range_by_prop.is_empty()
            && self.secondary_edge_field_indexes_by_label.is_empty()
        {
            return;
        }
        let eq_by_prop = self.secondary_edge_eq_by_prop.get(&edge.label_id);
        let range_by_prop = self.secondary_edge_range_by_prop.get(&edge.label_id);
        let has_compound = self
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
        self.collect_compound_index_entries_for_edge_label(
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
        if self.secondary_edge_eq_by_prop.is_empty()
            && self.secondary_edge_range_by_prop.is_empty()
            && self.secondary_edge_field_indexes_by_label.is_empty()
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

        let eq_by_prop = self.secondary_edge_eq_by_prop.get(&new_edge.label_id);
        let range_by_prop = self.secondary_edge_range_by_prop.get(&new_edge.label_id);
        let has_compound = self
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
        self.collect_compound_index_updates_for_edge_label(
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

/// In-memory graph state. The current head and optional per-entry history live
/// under one memtable-wide `RwLock`, so active and frozen epochs share the same
/// authoritative MVCC substrate.
pub struct Memtable {
    state: RwLock<MemtableState>,
}

impl Clone for Memtable {
    fn clone(&self) -> Self {
        Self {
            state: RwLock::new(self.state.read().unwrap().clone()),
        }
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MemtableState::default()),
        }
    }

    pub(crate) fn apply_op(&self, op: &WalOp, last_write_seq: u64) {
        let mut state = self.state.write().unwrap();
        match op {
            WalOp::UpsertNode(node) => {
                let old_node = state.current_node(node.id).cloned();
                let was_live = old_node.is_some();
                let was_deleted = state.node_deleted_at(node.id, u64::MAX);
                if let Some(old) = old_node.as_ref() {
                    for &old_label_id in old.label_ids.as_slice() {
                        let label_removed = !node.label_ids.contains(old_label_id);
                        if label_removed || old.key != node.key {
                            state.set_node_key(old_label_id, &old.key, None, last_write_seq);
                        }
                        if label_removed {
                            state.set_node_label_slot(old_label_id, old.id, false, last_write_seq);
                        }
                        if label_removed || old.updated_at != node.updated_at {
                            state.set_time_slot(
                                (old_label_id, old.updated_at, old.id),
                                false,
                                last_write_seq,
                            );
                        }
                    }
                }

                let mut stored = node.clone();
                stored.last_write_seq = last_write_seq;
                for &new_label_id in node.label_ids.as_slice() {
                    state.set_node_key(new_label_id, &node.key, Some(node.id), last_write_seq);
                    state.set_node_label_slot(new_label_id, node.id, true, last_write_seq);
                    state.set_time_slot(
                        (new_label_id, node.updated_at, node.id),
                        true,
                        last_write_seq,
                    );
                }
                state.sync_secondary_index_entries_for_node_upsert(
                    old_node.as_ref(),
                    &stored,
                    last_write_seq,
                );
                if was_deleted {
                    state.set_node_tombstone_slot(node.id, false, last_write_seq);
                }
                state.set_node_state(node.id, RecordState::Live(stored), last_write_seq);
                if !was_live {
                    state.live_node_count = state.live_node_count.saturating_add(1);
                }
            }
            WalOp::UpsertEdge(edge) => {
                let old_edge = state.current_edge(edge.id).cloned();
                let was_live = old_edge.is_some();
                let was_deleted = state.edge_deleted_at(edge.id, u64::MAX);
                if let Some(old) = old_edge.as_ref() {
                    if (old.from != edge.from || old.to != edge.to || old.label_id != edge.label_id)
                        && state.current_edge_triple_id(old.from, old.to, old.label_id)
                            == Some(old.id)
                    {
                        state.set_edge_triple(old.from, old.to, old.label_id, None, last_write_seq);
                    }
                    if old.label_id != edge.label_id {
                        state.set_edge_label_slot(old.label_id, old.id, false, last_write_seq);
                        state.set_ordered_edge_label_slot(
                            old.label_id,
                            old.id,
                            false,
                            last_write_seq,
                        );
                    }
                    if old.from != edge.from || old.to != edge.to {
                        state.set_adj_out_slot(old.from, old.id, None, last_write_seq);
                        state.set_adj_in_slot(old.to, old.id, None, last_write_seq);
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

                let current_triple_id =
                    state.current_edge_triple_id(edge.from, edge.to, edge.label_id);
                let should_update_triple = match old_edge.as_ref() {
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
                state.set_adj_out_slot(edge.from, edge.id, Some(adj_out_entry), last_write_seq);
                state.set_adj_in_slot(edge.to, edge.id, Some(adj_in_entry), last_write_seq);
                state.set_edge_label_slot(edge.label_id, edge.id, true, last_write_seq);
                state.set_ordered_edge_label_slot(edge.label_id, edge.id, true, last_write_seq);
                state.set_ordered_edge_slot(edge.id, true, last_write_seq);

                let mut stored = edge.clone();
                stored.last_write_seq = last_write_seq;
                state.sync_secondary_index_entries_for_edge_upsert(
                    old_edge.as_ref(),
                    &stored,
                    last_write_seq,
                );
                if was_deleted {
                    state.set_edge_tombstone_slot(edge.id, false, last_write_seq);
                }
                state.set_edge_state(edge.id, RecordState::Live(stored), last_write_seq);
                if !was_live {
                    state.live_edge_count = state.live_edge_count.saturating_add(1);
                }
            }
            WalOp::DeleteNode { id, deleted_at } => {
                if let Some(node) = state.current_node(*id).cloned() {
                    for &label_id in node.label_ids.as_slice() {
                        state.set_node_key(label_id, &node.key, None, last_write_seq);
                        state.set_node_label_slot(label_id, node.id, false, last_write_seq);
                        state.set_time_slot(
                            (label_id, node.updated_at, node.id),
                            false,
                            last_write_seq,
                        );
                    }
                    state.remove_secondary_index_entries_for_node(&node, last_write_seq);
                    state.live_node_count = state.live_node_count.saturating_sub(1);
                }
                state.set_node_state(
                    *id,
                    RecordState::Tombstone(TombstoneEntry {
                        deleted_at: *deleted_at,
                        last_write_seq,
                    }),
                    last_write_seq,
                );
                state.set_node_tombstone_slot(*id, true, last_write_seq);
            }
            WalOp::DeleteEdge { id, deleted_at } => {
                if let Some(edge) = state.current_edge(*id).cloned() {
                    if state.current_edge_triple_id(edge.from, edge.to, edge.label_id) == Some(*id)
                    {
                        state.set_edge_triple(
                            edge.from,
                            edge.to,
                            edge.label_id,
                            None,
                            last_write_seq,
                        );
                    }
                    state.set_edge_label_slot(edge.label_id, edge.id, false, last_write_seq);
                    state.set_ordered_edge_label_slot(
                        edge.label_id,
                        edge.id,
                        false,
                        last_write_seq,
                    );
                    state.set_adj_out_slot(edge.from, edge.id, None, last_write_seq);
                    state.set_adj_in_slot(edge.to, edge.id, None, last_write_seq);
                    state.set_ordered_edge_slot(edge.id, false, last_write_seq);
                    state.remove_secondary_index_entries_for_edge(&edge, last_write_seq);
                    state.live_edge_count = state.live_edge_count.saturating_sub(1);
                }
                state.set_edge_state(
                    *id,
                    RecordState::Tombstone(TombstoneEntry {
                        deleted_at: *deleted_at,
                        last_write_seq,
                    }),
                    last_write_seq,
                );
                state.set_edge_tombstone_slot(*id, true, last_write_seq);
            }
            WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => {}
        }
    }

    pub fn register_secondary_index(&self, entry: &SecondaryIndexManifestEntry) {
        let mut state = self.state.write().unwrap();
        if state
            .secondary_index_declarations
            .contains_key(&entry.index_id)
        {
            return;
        }
        state
            .secondary_index_declarations
            .insert(entry.index_id, entry.clone());
        state.estimated_bytes += estimate_secondary_decl_entry(entry);
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, prop_key } => match &entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state.secondary_eq_by_prop.entry(*label_id).or_default();
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
                    for slot in state.nodes.values() {
                        let Some(node) = record_current(slot) else {
                            continue;
                        };
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
                        let by_prop = state.secondary_range_by_prop.entry(*label_id).or_default();
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
                    for slot in state.nodes.values() {
                        let Some(node) = record_current(slot) else {
                            continue;
                        };
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
                    for slot in state.edges.values() {
                        let Some(edge) = record_current(slot) else {
                            continue;
                        };
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
                    for slot in state.edges.values() {
                        let Some(edge) = record_current(slot) else {
                            continue;
                        };
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
                    &mut state.secondary_node_field_indexes_by_label,
                    *label_id,
                    entry.index_id,
                );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                state
                    .secondary_compound_state
                    .entry(entry.index_id)
                    .or_default();
                let mut seeded = Vec::new();
                for slot in state.nodes.values() {
                    let Some(node) = record_current(slot) else {
                        continue;
                    };
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
                    &mut state.secondary_edge_field_indexes_by_label,
                    *label_id,
                    entry.index_id,
                );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                state
                    .secondary_compound_state
                    .entry(entry.index_id)
                    .or_default();
                let mut seeded = Vec::new();
                for slot in state.edges.values() {
                    let Some(edge) = record_current(slot) else {
                        continue;
                    };
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
    }

    pub fn unregister_secondary_index(&self, index_id: u64) -> bool {
        let mut state = self.state.write().unwrap();
        let Some(entry) = state.secondary_index_declarations.remove(&index_id) else {
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
                        if let Some(by_prop) = state.secondary_eq_by_prop.get_mut(&label_id) {
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
                        state.secondary_eq_by_prop.remove(&label_id);
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
                        if let Some(by_prop) = state.secondary_range_by_prop.get_mut(&label_id) {
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
                        state.secondary_range_by_prop.remove(&label_id);
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
                        if let Some(by_prop) = state.secondary_edge_eq_by_prop.get_mut(&label_id) {
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
                        state.secondary_edge_eq_by_prop.remove(&label_id);
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
                        if let Some(by_prop) = state.secondary_edge_range_by_prop.get_mut(&label_id)
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
                        state.secondary_edge_range_by_prop.remove(&label_id);
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
                        &mut state.secondary_node_field_indexes_by_label,
                        label_id,
                        index_id,
                    );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                if remove_label_entry {
                    state
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
                        &mut state.secondary_edge_field_indexes_by_label,
                        label_id,
                        index_id,
                    );
                apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                if remove_label_entry {
                    state
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

    pub(crate) fn get_node_at(&self, id: u64, snapshot_seq: u64) -> Option<NodeRecord> {
        let state = self.state.read().unwrap();
        state.node_at(id, snapshot_seq).cloned()
    }

    pub(crate) fn visit_nodes_sorted_at<F>(
        &self,
        sorted_ids: &[u64],
        snapshot_seq: u64,
        remaining: &mut Vec<u64>,
        callback: &mut F,
    ) where
        F: FnMut(u64, &NodeRecord),
    {
        let state = self.state.read().unwrap();
        for &id in sorted_ids {
            if state.node_deleted_at(id, snapshot_seq) {
                continue;
            }
            if let Some(node) = state.node_at(id, snapshot_seq) {
                callback(id, node);
            } else {
                remaining.push(id);
            }
        }
    }

    pub(crate) fn get_edge_at(&self, id: u64, snapshot_seq: u64) -> Option<EdgeRecord> {
        let state = self.state.read().unwrap();
        state.edge_at(id, snapshot_seq).cloned()
    }

    pub(crate) fn batch_get_node_selected_fields_at(
        &self,
        lookups: &[(usize, u64)],
        needs: &NodeSelectedFieldNeeds,
        snapshot_seq: u64,
        results: &mut [Option<SelectedNodeFields>],
        #[cfg(test)] selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Vec<(usize, u64)> {
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
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
                    let outcome = match state.nodes.get(&id) {
                        Some(slot) => match record_at(slot, snapshot_seq) {
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
                        },
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }

        remaining
    }

    pub(crate) fn batch_get_edge_selected_fields_at(
        &self,
        lookups: &[(usize, u64)],
        needs: &EdgeSelectedFieldNeeds,
        snapshot_seq: u64,
        results: &mut [Option<SelectedEdgeFields>],
        #[cfg(test)] _selected_field_read_counters: Option<&SelectedFieldReadCounters>,
    ) -> Vec<(usize, u64)> {
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
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
                    let outcome = match state.edges.get(&id) {
                        Some(slot) => match record_at(slot, snapshot_seq) {
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
                        },
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }

        remaining
    }

    pub(crate) fn get_edge_core_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Option<(u64, u64, i64, i64, f32, i64, i64)> {
        let state = self.state.read().unwrap();
        let edge = state.edge_at(id, snapshot_seq)?;
        Some((
            edge.from,
            edge.to,
            edge.created_at,
            edge.updated_at,
            edge.weight,
            edge.valid_from,
            edge.valid_to,
        ))
    }

    pub(crate) fn get_edge_metadata_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> Option<EdgeMetadataCandidate> {
        let state = self.state.read().unwrap();
        state
            .edge_at(id, snapshot_seq)
            .map(EdgeMetadataCandidate::from_edge)
    }

    pub(crate) fn batch_get_node_visibility_meta_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [NodeVisibilityState],
    ) -> Vec<(usize, u64)> {
        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
        let mut cache =
            NodeIdMap::with_capacity_and_hasher(lookups.len(), NodeIdBuildHasher::default());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, id) in lookups {
            let visibility = match cache.get(&id).copied() {
                Some(state) => state,
                None => {
                    let state = match state
                        .nodes
                        .get(&id)
                        .and_then(|slot| record_at(slot, snapshot_seq))
                    {
                        Some(RecordState::Live(node)) => {
                            NodeVisibilityState::Live(NodeVisibilityMeta {
                                label_ids: node.label_ids,
                                updated_at: node.updated_at,
                                weight: node.weight,
                            })
                        }
                        Some(RecordState::Tombstone(_)) => NodeVisibilityState::Deleted,
                        None if state.node_deleted_at(id, snapshot_seq) => {
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

        remaining
    }

    pub(crate) fn edge_visibility_state_at(
        &self,
        id: u64,
        snapshot_seq: u64,
    ) -> EdgeVisibilityState {
        let state = self.state.read().unwrap();
        match state
            .edges
            .get(&id)
            .and_then(|slot| record_at(slot, snapshot_seq))
        {
            Some(RecordState::Live(_)) => EdgeVisibilityState::Live,
            Some(RecordState::Tombstone(_)) => EdgeVisibilityState::Deleted,
            None if state.edge_deleted_at(id, snapshot_seq) => EdgeVisibilityState::Deleted,
            None => EdgeVisibilityState::Missing,
        }
    }

    pub(crate) fn batch_get_nodes_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [Option<NodeRecord>],
    ) -> Vec<(usize, u64)> {
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
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
                    let outcome = match state.nodes.get(&id) {
                        Some(slot) => match record_at(slot, snapshot_seq) {
                            Some(RecordState::Live(node)) => {
                                results[orig_idx] = Some(node.clone());
                                CachedLookup::Live(orig_idx)
                            }
                            Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                            None => CachedLookup::Miss,
                        },
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }

        remaining
    }

    pub(crate) fn batch_get_edges_at(
        &self,
        lookups: &[(usize, u64)],
        snapshot_seq: u64,
        results: &mut [Option<EdgeRecord>],
    ) -> Vec<(usize, u64)> {
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Tombstone,
            Miss,
        }

        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
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
                    let outcome = match state.edges.get(&id) {
                        Some(slot) => match record_at(slot, snapshot_seq) {
                            Some(RecordState::Live(edge)) => {
                                results[orig_idx] = Some(edge.clone());
                                CachedLookup::Live(orig_idx)
                            }
                            Some(RecordState::Tombstone(_)) => CachedLookup::Tombstone,
                            None => CachedLookup::Miss,
                        },
                        None => CachedLookup::Miss,
                    };
                    cache.insert(id, outcome);
                    if matches!(outcome, CachedLookup::Miss) {
                        remaining.push((orig_idx, id));
                    }
                }
            }
        }

        remaining
    }

    pub(crate) fn is_node_deleted_at(&self, id: u64, snapshot_seq: u64) -> bool {
        let state = self.state.read().unwrap();
        state.node_deleted_at(id, snapshot_seq)
    }

    pub(crate) fn is_edge_deleted_at(&self, id: u64, snapshot_seq: u64) -> bool {
        let state = self.state.read().unwrap();
        state.edge_deleted_at(id, snapshot_seq)
    }

    pub(crate) fn node_tombstone_at(&self, id: u64, snapshot_seq: u64) -> Option<TombstoneEntry> {
        let state = self.state.read().unwrap();
        state
            .nodes
            .get(&id)
            .and_then(|slot| match record_at(slot, snapshot_seq) {
                Some(RecordState::Tombstone(entry)) => Some(*entry),
                _ => None,
            })
    }

    pub(crate) fn edge_tombstone_at(&self, id: u64, snapshot_seq: u64) -> Option<TombstoneEntry> {
        let state = self.state.read().unwrap();
        state
            .edges
            .get(&id)
            .and_then(|slot| match record_at(slot, snapshot_seq) {
                Some(RecordState::Tombstone(entry)) => Some(*entry),
                _ => None,
            })
    }

    pub(crate) fn node_by_key_at(
        &self,
        label_id: u32,
        key: &str,
        snapshot_seq: u64,
    ) -> Option<NodeRecord> {
        let state = self.state.read().unwrap();
        let node_id =
            *slot_option_at(state.node_key_index.get(&label_id)?.get(key)?, snapshot_seq)?;
        state.node_at(node_id, snapshot_seq).cloned()
    }

    pub(crate) fn node_id_by_key_at(
        &self,
        label_id: u32,
        key: &str,
        snapshot_seq: u64,
    ) -> Option<u64> {
        let state = self.state.read().unwrap();
        let node_id =
            *slot_option_at(state.node_key_index.get(&label_id)?.get(key)?, snapshot_seq)?;
        let node = state.node_at(node_id, snapshot_seq)?;
        (node.label_ids.contains(label_id) && node.key == key).then_some(node_id)
    }

    pub(crate) fn edge_by_triple_at(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Option<EdgeRecord> {
        let state = self.state.read().unwrap();
        let edge_id = *slot_option_at(
            state.edge_triple_index.get(&(from, to, label_id))?,
            snapshot_seq,
        )?;
        state.edge_at(edge_id, snapshot_seq).cloned()
    }

    pub(crate) fn batch_edges_by_triples_at(
        &self,
        lookups: &[(usize, u64, u64, u32)],
        snapshot_seq: u64,
        results: &mut [Option<EdgeRecord>],
    ) -> Vec<(usize, u64, u64, u32)> {
        #[derive(Clone, Copy)]
        enum CachedLookup {
            Live(usize),
            Miss,
        }

        if lookups.is_empty() {
            return Vec::new();
        }

        let state = self.state.read().unwrap();
        let mut cache = HashMap::with_capacity(lookups.len());
        let mut remaining = Vec::with_capacity(lookups.len());

        for &(orig_idx, from, to, label_id) in lookups {
            let triple = (from, to, label_id);
            match cache.get(&triple).copied() {
                Some(CachedLookup::Live(cached_idx)) => {
                    results[orig_idx] = results[cached_idx].clone();
                }
                Some(CachedLookup::Miss) => remaining.push((orig_idx, from, to, label_id)),
                None => {
                    let outcome = match state
                        .edge_triple_index
                        .get(&triple)
                        .and_then(|slot| slot_option_at(slot, snapshot_seq))
                        .and_then(|edge_id| state.edge_at(*edge_id, snapshot_seq))
                    {
                        Some(edge) => {
                            results[orig_idx] = Some(edge.clone());
                            CachedLookup::Live(orig_idx)
                        }
                        None => {
                            remaining.push((orig_idx, from, to, label_id));
                            CachedLookup::Miss
                        }
                    };
                    cache.insert(triple, outcome);
                }
            }
        }

        remaining
    }

    pub(crate) fn for_each_visible_node_at<F>(
        &self,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(&NodeRecord) -> ControlFlow<()>,
    {
        let state = self.state.read().unwrap();
        for slot in state.nodes.values() {
            let Some(RecordState::Live(node)) = record_at(slot, snapshot_seq) else {
                continue;
            };
            if callback(node).is_break() {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    pub(crate) fn visible_nodes_by_label_id(&self, label_id: u32, snapshot_seq: u64) -> Vec<u64> {
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
    }

    pub(crate) fn visible_node_ids_at(&self, snapshot_seq: u64) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        for (&node_id, slot) in &state.nodes {
            if matches!(record_at(slot, snapshot_seq), Some(RecordState::Live(_))) {
                ids.push(node_id);
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn visible_node_count_at(&self, snapshot_seq: u64) -> usize {
        let state = self.state.read().unwrap();
        state
            .nodes
            .values()
            .filter(|slot| matches!(record_at(slot, snapshot_seq), Some(RecordState::Live(_))))
            .count()
    }

    pub(crate) fn visible_nodes_by_label_id_count(
        &self,
        label_id: u32,
        snapshot_seq: u64,
    ) -> usize {
        let state = self.state.read().unwrap();
        state
            .label_node_index
            .range((label_id, 0)..=(label_id, u64::MAX))
            .filter(|(_, slot)| slot_option_visible(slot, snapshot_seq))
            .count()
    }

    pub(crate) fn visible_edges_by_label_id(&self, label_id: u32, snapshot_seq: u64) -> Vec<u64> {
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
    }

    pub(crate) fn visible_edges_by_label_id_count(
        &self,
        label_id: u32,
        snapshot_seq: u64,
    ) -> usize {
        let state = self.state.read().unwrap();
        state
            .ordered_label_edge_index
            .range((label_id, 0)..=(label_id, u64::MAX))
            .filter(|(_, slot)| slot_option_visible(slot, snapshot_seq))
            .count()
    }

    #[cfg(test)]
    pub(crate) fn visible_edge_ids_at(&self, snapshot_seq: u64) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        for (&edge_id, slot) in &state.edges {
            if matches!(record_at(slot, snapshot_seq), Some(RecordState::Live(_))) {
                ids.push(edge_id);
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn next_visible_edge_id_after(
        &self,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
        let state = self.state.read().unwrap();
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        for (&edge_id, slot) in state.ordered_edge_ids.range((start, Bound::Unbounded)) {
            if slot_option_visible(slot, snapshot_seq) {
                return Some(edge_id);
            }
        }
        None
    }

    pub(crate) fn next_visible_node_by_label_id_after(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
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
    }

    pub(crate) fn next_visible_edge_by_label_id_after(
        &self,
        label_id: u32,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
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
    }

    pub(crate) fn edge_ids_by_triple_at(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        if let Some(entries) = state.adj_out.get(&from) {
            for (&edge_id, slot) in entries {
                let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                    continue;
                };
                if entry.neighbor_id == to && entry.label_id == label_id {
                    ids.push(edge_id);
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    #[cfg(test)]
    pub(crate) fn edge_metadata_scan_ids_at<F>(
        &self,
        snapshot_seq: u64,
        mut predicate: F,
    ) -> Vec<u64>
    where
        F: FnMut(EdgeMetadataCandidate) -> bool,
    {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        for (&edge_id, slot) in &state.edges {
            let Some(RecordState::Live(edge)) = record_at(slot, snapshot_seq) else {
                continue;
            };
            let meta = EdgeMetadataCandidate::from_edge(edge);
            if predicate(meta) {
                ids.push(edge_id);
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn for_each_edge_metadata_at<F>(
        &self,
        snapshot_seq: u64,
        mut callback: F,
    ) -> ControlFlow<()>
    where
        F: FnMut(EdgeMetadataCandidate) -> ControlFlow<()>,
    {
        let state = self.state.read().unwrap();
        for slot in state.edges.values() {
            let Some(RecordState::Live(edge)) = record_at(slot, snapshot_seq) else {
                continue;
            };
            if callback(EdgeMetadataCandidate::from_edge(edge)).is_break() {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
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
    }

    pub(crate) fn visible_nodes_by_time_range(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
    ) -> Vec<u64> {
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
    }

    pub(crate) fn visible_node_time_range_count_at(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
    ) -> usize {
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
        );
        count
    }

    pub(crate) fn for_each_visible_node_by_time_range_at<F>(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
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
    }

    pub(crate) fn neighbors_at(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
        snapshot_seq: u64,
    ) -> Vec<NeighborRecord> {
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
                if label_filter_ids.is_some_and(|label_ids| !label_ids.contains(&entry.label_id)) {
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
    }

    pub(crate) fn incident_edges_at(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Vec<NeighborRecord> {
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
                if label_filter_ids.is_some_and(|label_ids| !label_ids.contains(&entry.label_id)) {
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
    }

    pub(crate) fn for_each_adj_entry_at<F>(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
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
                if label_filter_ids.is_some_and(|label_ids| !label_ids.contains(&entry.label_id)) {
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
    }

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
            ENDPOINT_CURSOR_ENTRIES_VISITED_FOR_TEST.fetch_add(1, Ordering::Relaxed);
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

    pub(crate) fn next_visible_edge_from_endpoint_after(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
        let state = self.state.read().unwrap();
        Self::next_visible_adj_edge_after(
            &state,
            node_id,
            true,
            label_filter_ids,
            snapshot_seq,
            after,
        )
    }

    pub(crate) fn next_visible_edge_to_endpoint_after(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Option<u64> {
        let state = self.state.read().unwrap();
        Self::next_visible_adj_edge_after(
            &state,
            node_id,
            false,
            label_filter_ids,
            snapshot_seq,
            after,
        )
    }

    #[cfg(test)]
    pub(crate) fn visible_edges_from_endpoint_count_estimate(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        let state = self.state.read().unwrap();
        Self::visible_adj_edge_count_estimate(&state, node_id, true, label_filter_ids, snapshot_seq)
    }

    #[cfg(test)]
    pub(crate) fn visible_edges_to_endpoint_count_estimate(
        &self,
        node_id: u64,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        let state = self.state.read().unwrap();
        Self::visible_adj_edge_count_estimate(
            &state,
            node_id,
            false,
            label_filter_ids,
            snapshot_seq,
        )
    }

    pub(crate) fn visible_endpoint_edges_count_estimate_at(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> MemtableEndpointCountEstimate {
        let state = self.state.read().unwrap();
        Self::visible_endpoint_edges_count_estimate(
            &state,
            node_ids,
            direction,
            label_filter_ids,
            snapshot_seq,
        )
    }

    pub(crate) fn visible_node_label_ids(&self, snapshot_seq: u64) -> Vec<u32> {
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
    }

    pub(crate) fn find_secondary_eq_nodes_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.find_secondary_eq_nodes_at_limited(index_id, prop_key, prop_value, snapshot_seq, None)
    }

    pub(crate) fn find_secondary_eq_nodes_at_limited(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Vec<u64> {
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
        ids
    }

    pub(crate) fn find_secondary_eq_nodes_by_hash_at_limited(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Vec::new();
        };
        let Some(group) = groups.get(&value_hash) else {
            return Vec::new();
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
        ids
    }

    pub(crate) fn secondary_eq_node_count_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> usize {
        let state = self.state.read().unwrap();
        let value_hash = hash_prop_equality_key(prop_value);
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return 0;
        };
        let Some(group) = groups.get(&value_hash) else {
            return 0;
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
        count
    }

    pub(crate) fn secondary_eq_node_hash_count_at(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> usize {
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
    }

    pub(crate) fn find_secondary_eq_edges_by_hash_at_limited(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
        max_ids: Option<usize>,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return Vec::new();
        };
        let Some(group) = groups.get(&value_hash) else {
            return Vec::new();
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
        ids
    }

    pub(crate) fn secondary_eq_edge_count_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> usize {
        let state = self.state.read().unwrap();
        let value_hash = hash_prop_equality_key(prop_value);
        let Some(groups) = state.secondary_eq_state.get(&index_id) else {
            return 0;
        };
        let Some(group) = groups.get(&value_hash) else {
            return 0;
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
        count
    }

    pub(crate) fn secondary_eq_edge_hash_count_at(
        &self,
        index_id: u64,
        value_hash: u64,
        snapshot_seq: u64,
    ) -> usize {
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
    }

    #[cfg(test)]
    pub(crate) fn visible_secondary_range_entries(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
    ) -> Vec<(NumericRangeSortKey, u64)> {
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
    ) -> Vec<(NumericRangeSortKey, u64)> {
        if limit == Some(0) {
            return Vec::new();
        }
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_range_state.get(&index_id) else {
            return Vec::new();
        };

        let start = secondary_range_start_bound(lower, after);
        let mut result = Vec::new();
        for (&(encoded, node_id), slot) in entries.range((start, std::ops::Bound::Unbounded)) {
            if secondary_range_past_upper(encoded, upper) {
                break;
            }
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            result.push((encoded, node_id));
            if limit.is_some_and(|limit| result.len() >= limit) {
                break;
            }
        }
        result
    }

    pub(crate) fn visible_secondary_range_entry_count(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
    ) -> usize {
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
    }

    pub(crate) fn for_each_visible_secondary_range_entry_at<F>(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut((NumericRangeSortKey, u64)) -> ControlFlow<()>,
    {
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_range_state.get(&index_id) else {
            return ControlFlow::Continue(());
        };

        let start = secondary_range_start_bound(lower, after);
        for (&(encoded, node_id), slot) in entries.range((start, std::ops::Bound::Unbounded)) {
            if secondary_range_past_upper(encoded, upper) {
                break;
            }
            if !slot_option_visible(slot, snapshot_seq) {
                continue;
            }
            if callback((encoded, node_id)).is_break() {
                return ControlFlow::Break(());
            }
        }

        ControlFlow::Continue(())
    }

    #[allow(dead_code)]
    pub(crate) fn find_node_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.find_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_edge_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.find_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_node_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> usize {
        self.count_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_edge_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> usize {
        self.count_compound_prefix_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_node_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.find_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn find_edge_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        self.find_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_node_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> usize {
        self.count_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    pub(crate) fn count_edge_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> usize {
        self.count_compound_range_at(index_id, bounds, snapshot_seq)
    }

    #[allow(dead_code)]
    fn find_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_compound_state.get(&index_id) else {
            return Vec::new();
        };
        collect_visible_compound_ids(
            entries,
            Bound::Included((bounds.lower.clone(), 0)),
            &bounds.upper_exclusive,
            None,
            snapshot_seq,
        )
    }

    #[allow(dead_code)]
    fn count_compound_prefix_at(
        &self,
        index_id: u64,
        bounds: &CompoundPrefixBounds,
        snapshot_seq: u64,
    ) -> usize {
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
    }

    #[allow(dead_code)]
    fn find_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_compound_state.get(&index_id) else {
            return Vec::new();
        };
        let skip_prefix = bounds
            .lower
            .as_ref()
            .filter(|lower| lower.exclusive_component_prefix)
            .map(|lower| lower.key.as_slice());
        collect_visible_compound_ids(
            entries,
            compound_start_bound(bounds.lower.as_ref()),
            &bounds.upper_exclusive,
            skip_prefix,
            snapshot_seq,
        )
    }

    #[allow(dead_code)]
    fn count_compound_range_at(
        &self,
        index_id: u64,
        bounds: &CompoundRangeBounds,
        snapshot_seq: u64,
    ) -> usize {
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
    }

    pub(crate) fn collect_deleted_nodes_at(&self, snapshot_seq: u64) -> NodeIdSet {
        let state = self.state.read().unwrap();
        let mut deleted = NodeIdSet::default();
        for (&node_id, slot) in &state.node_tombstones {
            if slot_option_visible(slot, snapshot_seq) {
                deleted.insert(node_id);
            }
        }
        deleted
    }

    pub(crate) fn collect_deleted_edges_at(&self, snapshot_seq: u64) -> NodeIdSet {
        let state = self.state.read().unwrap();
        let mut deleted = NodeIdSet::default();
        for (&edge_id, slot) in &state.edge_tombstones {
            if slot_option_visible(slot, snapshot_seq) {
                deleted.insert(edge_id);
            }
        }
        deleted
    }

    #[allow(dead_code)]
    pub(crate) fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
    ) -> Vec<NeighborRecord> {
        self.neighbors_at(node_id, direction, label_filter_ids, limit, u64::MAX)
    }

    #[allow(dead_code)]
    pub(crate) fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> NodeIdMap<Vec<NeighborRecord>> {
        let mut results = NodeIdMap::default();
        for &nid in node_ids {
            let entries = self.neighbors(nid, direction, label_filter_ids, 0);
            if !entries.is_empty() {
                results.insert(nid, entries);
            }
        }
        results
    }

    pub(crate) fn neighbors_batch_at(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> NodeIdMap<Vec<NeighborRecord>> {
        let mut results = NodeIdMap::default();
        for &nid in node_ids {
            let entries = self.neighbors_at(nid, direction, label_filter_ids, 0, snapshot_seq);
            if !entries.is_empty() {
                results.insert(nid, entries);
            }
        }
        results
    }

    pub fn node_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.live_node_count
    }

    pub fn edge_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.live_edge_count
    }

    pub fn nodes(&self) -> NodeIdMap<NodeRecord> {
        let state = self.state.read().unwrap();
        state
            .nodes
            .iter()
            .filter_map(|(&id, slot)| record_current(slot).map(|node| (id, node.clone())))
            .collect()
    }

    pub fn edges(&self) -> NodeIdMap<EdgeRecord> {
        let state = self.state.read().unwrap();
        state
            .edges
            .iter()
            .filter_map(|(&id, slot)| record_current(slot).map(|edge| (id, edge.clone())))
            .collect()
    }

    pub fn deleted_nodes(&self) -> NodeIdMap<TombstoneEntry> {
        let state = self.state.read().unwrap();
        state
            .node_tombstones
            .iter()
            .filter_map(|(&id, _)| state.node_tombstone_current(id).map(|entry| (id, entry)))
            .collect()
    }

    pub fn deleted_edges(&self) -> NodeIdMap<TombstoneEntry> {
        let state = self.state.read().unwrap();
        state
            .edge_tombstones
            .iter()
            .filter_map(|(&id, _)| state.edge_tombstone_current(id).map(|entry| (id, entry)))
            .collect()
    }

    pub fn adj_out(&self) -> NodeIdMap<NodeIdMap<AdjEntry>> {
        let state = self.state.read().unwrap();
        current_adj_map(&state.adj_out)
    }

    pub fn adj_in(&self) -> NodeIdMap<NodeIdMap<AdjEntry>> {
        let state = self.state.read().unwrap();
        current_adj_map(&state.adj_in)
    }

    pub(crate) fn node_label_posting_groups_current(&self) -> Vec<(u32, Vec<u64>)> {
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
    }

    pub fn label_edge_index(&self) -> HashMap<u32, NodeIdSet> {
        let state = self.state.read().unwrap();
        current_label_membership_index(&state.label_edge_index)
    }

    pub fn secondary_index_declarations(&self) -> HashMap<u64, SecondaryIndexManifestEntry> {
        let state = self.state.read().unwrap();
        state.secondary_index_declarations.clone()
    }

    pub fn secondary_eq_state(&self) -> HashMap<u64, HashMap<u64, NodeIdSet>> {
        let state = self.state.read().unwrap();
        current_secondary_eq_state(&state.secondary_eq_state)
    }

    pub fn secondary_range_state(&self) -> HashMap<u64, BTreeSet<(NumericRangeSortKey, u64)>> {
        let state = self.state.read().unwrap();
        current_secondary_range_state(&state.secondary_range_state)
    }

    #[cfg(test)]
    pub(crate) fn compound_secondary_state(&self) -> HashMap<u64, BTreeSet<(Vec<u8>, u64)>> {
        let state = self.state.read().unwrap();
        current_compound_secondary_state(&state.secondary_compound_state)
    }

    pub(crate) fn compound_secondary_state_for_indexes(
        &self,
        index_ids: &[u64],
    ) -> HashMap<u64, Vec<(Vec<u8>, u64)>> {
        let state = self.state.read().unwrap();
        current_compound_secondary_state_for_indexes(&state.secondary_compound_state, index_ids)
    }

    pub fn time_node_index(&self) -> BTreeSet<(u32, i64, u64)> {
        let state = self.state.read().unwrap();
        current_time_index(&state.time_node_index)
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
        state.estimated_bytes
    }

    #[cfg(test)]
    fn estimated_size_full_for_test(&self) -> usize {
        let state = self.state.read().unwrap();
        state.recompute_estimated_size()
    }

    pub fn is_empty(&self) -> bool {
        let state = self.state.read().unwrap();
        state.nodes.is_empty() && state.edges.is_empty()
    }

    pub fn max_node_id(&self) -> u64 {
        let state = self.state.read().unwrap();
        state.nodes.keys().max().copied().unwrap_or(0)
    }

    pub fn max_edge_id(&self) -> u64 {
        let state = self.state.read().unwrap();
        state.edges.keys().max().copied().unwrap_or(0)
    }
}

#[cfg(test)]
impl Memtable {
    fn label_node_index_key_count(&self) -> usize {
        self.node_label_posting_groups_current().len()
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
        self.time_node_index().len()
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

        assert_eq!(mt.get_node_at(1, u64::MAX).unwrap().key, "alice");
        assert_eq!(mt.get_edge_at(1, u64::MAX).unwrap().from, 1);
        assert_eq!(mt.node_by_key_at(1, "alice", u64::MAX).unwrap().id, 1);
        assert_eq!(mt.edge_by_triple_at(1, 2, 10, u64::MAX).unwrap().id, 1);
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
        assert_eq!(mt.visible_node_count_at(2), 2);

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
        assert_eq!(mt.visible_node_count_at(2), 2);
        assert_eq!(mt.visible_node_count_at(4), 1);

        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a2")), 6);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 7);
        assert_eq!(mt.node_count(), 2);
        assert_eq!(mt.edge_count(), 1);
        assert_eq!(mt.visible_node_count_at(4), 1);
        assert_eq!(mt.visible_node_count_at(6), 2);

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

        assert_eq!(mt.visible_edge_ids_at(3), vec![10, 11, 12]);
        assert_eq!(mt.visible_edge_ids_at(4), vec![10, 11]);
        assert_eq!(mt.visible_edges_by_label_id(5, 4), vec![10, 11]);
        let mut outgoing_ids = mt
            .neighbors_batch_at(&[1], Direction::Outgoing, Some(&[5]), 4)
            .into_values()
            .flatten()
            .map(|entry| entry.edge_id)
            .collect::<Vec<_>>();
        outgoing_ids.sort_unstable();
        assert_eq!(outgoing_ids, vec![10, 11]);
        let mut both_ids = mt
            .neighbors_batch_at(&[1], Direction::Both, None, 4)
            .into_values()
            .flatten()
            .map(|entry| entry.edge_id)
            .collect::<Vec<_>>();
        both_ids.sort_unstable();
        both_ids.dedup();
        assert_eq!(both_ids, vec![10, 11]);
        assert_eq!(mt.edge_ids_by_triple_at(1, 2, 5, 4), vec![10]);
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

        assert_eq!(mt.get_node_at(1, 10).unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 19).unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 20).unwrap().key, "alice_v2");
        assert_eq!(mt.get_node_at(1, u64::MAX).unwrap().key, "alice_v2");
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

        assert!(mt.get_node_at(1, u64::MAX).is_none());
        assert_eq!(mt.get_node_at(1, 5).unwrap().key, "alice");
        assert!(mt.get_node_at(1, 6).is_none());
        assert!(mt.is_node_deleted_at(1, 6));
        assert!(!mt.is_node_deleted_at(1, 5));
        assert_eq!(mt.deleted_nodes().get(&1).unwrap().last_write_seq, 6);
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

        assert!(mt.collect_deleted_nodes_at(1).is_empty());
        assert_eq!(mt.collect_deleted_nodes_at(2), NodeIdSet::from_iter([1]));
        assert!(mt.collect_deleted_nodes_at(3).is_empty());
        assert!(mt.deleted_nodes().is_empty());
        assert!(!mt.is_node_deleted_at(3, 3));
        assert_eq!(mt.get_node_at(1, 1).unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 3).unwrap().key, "alice_v2");
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

        assert!(mt.collect_deleted_edges_at(1).is_empty());
        assert_eq!(mt.collect_deleted_edges_at(2), NodeIdSet::from_iter([9]));
        assert!(mt.collect_deleted_edges_at(3).is_empty());
        assert!(mt.deleted_edges().is_empty());
        assert!(!mt.is_edge_deleted_at(9, 3));
        assert_eq!(mt.get_edge_at(9, 3).unwrap().to, 3);
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

        assert_eq!(mt.node_by_key_at(1, "alice", 1).unwrap().id, 1);
        assert!(mt.node_by_key_at(1, "alice", 2).is_none());
        assert_eq!(mt.node_by_key_at(1, "alice", 3).unwrap().id, 2);
        assert_eq!(mt.node_by_key_at(1, "alice", u64::MAX).unwrap().id, 2);
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

        assert_eq!(mt.visible_edges_by_label_id(10, 3), vec![1]);
        assert!(mt.visible_edges_by_label_id(10, 4).is_empty());
        let before_delete = mt.neighbors_at(1, Direction::Outgoing, None, 0, 3);
        assert_eq!(before_delete.len(), 1);
        assert_eq!(before_delete[0].node_id, 2);
        assert!(mt
            .neighbors_at(1, Direction::Outgoing, None, 0, 4)
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
                        estimate.add_assign(mt.visible_edges_from_endpoint_count_estimate(
                            node_id,
                            label_filter_ids,
                            snapshot_seq,
                        ));
                    }
                    Direction::Incoming => {
                        estimate.add_assign(mt.visible_edges_to_endpoint_count_estimate(
                            node_id,
                            label_filter_ids,
                            snapshot_seq,
                        ));
                    }
                    Direction::Both => {
                        estimate.add_assign(mt.visible_edges_from_endpoint_count_estimate(
                            node_id,
                            label_filter_ids,
                            snapshot_seq,
                        ));
                        estimate.add_assign(mt.visible_edges_to_endpoint_count_estimate(
                            node_id,
                            label_filter_ids,
                            snapshot_seq,
                        ));
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
                    let batch = mt.visible_endpoint_edges_count_estimate_at(
                        &node_ids,
                        direction,
                        label_filter_ids,
                        snapshot_seq,
                    );
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
    fn time_membership_history_is_snapshot_correct() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)), 1);
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 200)), 2);

        assert_eq!(mt.visible_nodes_by_time_range(1, 50, 150, 1), vec![1]);
        assert!(mt.visible_nodes_by_time_range(1, 50, 150, 2).is_empty());
        assert_eq!(mt.visible_nodes_by_time_range(1, 150, 250, 2), vec![1]);
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
                    mt.visible_node_time_range_count_at(1, from_ms, to_ms, snapshot_seq),
                    mt.visible_nodes_by_time_range(1, from_ms, to_ms, snapshot_seq)
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
                mt.node_by_key_at(label_id, "alice", 1).map(|node| node.id),
                Some(1)
            );
            assert_eq!(mt.visible_nodes_by_label_id(label_id, 1), vec![1]);
            assert_eq!(
                mt.visible_nodes_by_time_range(label_id, 100, 100, 1),
                vec![1]
            );
        }
        assert!(mt.node_by_key_at(9, "alice", 1).is_none());
        assert!(mt.visible_nodes_by_label_id(9, 1).is_empty());

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[2, 5, 9], "alice", 200)),
            2,
        );

        assert_eq!(
            mt.node_by_key_at(1, "alice", 1).map(|node| node.id),
            Some(1)
        );
        assert!(mt.node_by_key_at(1, "alice", 2).is_none());
        assert!(mt.visible_nodes_by_label_id(1, 2).is_empty());
        assert!(mt.visible_nodes_by_time_range(1, 100, 100, 2).is_empty());

        for label_id in [2, 5, 9] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice", 2).map(|node| node.id),
                Some(1)
            );
            assert_eq!(mt.visible_nodes_by_label_id(label_id, 2), vec![1]);
            assert!(mt
                .visible_nodes_by_time_range(label_id, 100, 100, 2)
                .is_empty());
            assert_eq!(
                mt.visible_nodes_by_time_range(label_id, 200, 200, 2),
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
            assert!(mt.node_by_key_at(label_id, "alice", 3).is_none());
            assert!(mt.visible_nodes_by_label_id(label_id, 3).is_empty());
            assert!(mt
                .visible_nodes_by_time_range(label_id, 200, 200, 3)
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

        assert_eq!(mt.visible_nodes_by_label_id(1, 2), vec![1, 3]);
        assert_eq!(mt.next_visible_node_by_label_id_after(1, 2, None), Some(1));
        assert_eq!(
            mt.next_visible_node_by_label_id_after(1, 2, Some(1)),
            Some(3)
        );
        assert_eq!(mt.visible_nodes_by_label_id(1, 4), vec![3]);
        assert_eq!(mt.visible_nodes_by_label_id(2, 4), vec![1, 2]);
        assert_eq!(mt.visible_node_label_ids(4), vec![1, 2, 3]);
        assert_eq!(
            mt.node_label_posting_groups_current(),
            vec![(1, vec![3]), (2, vec![1, 2]), (3, vec![1])]
        );

        mt.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 300,
            },
            5,
        );

        assert!(mt.visible_nodes_by_label_id(1, 5).is_empty());
        assert_eq!(mt.visible_node_label_ids(5), vec![2, 3]);
        assert_eq!(
            mt.node_label_posting_groups_current(),
            vec![(2, vec![1, 2]), (3, vec![1])]
        );
        assert_eq!(mt.next_visible_node_by_label_id_after(2, 5, None), Some(1));
        assert_eq!(
            mt.next_visible_node_by_label_id_after(2, 5, Some(1)),
            Some(2)
        );
        assert_eq!(mt.next_visible_node_by_label_id_after(2, 5, Some(2)), None);
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
                mt.node_by_key_at(label_id, "alice", 1).map(|node| node.id),
                Some(1)
            );
        }

        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_labels(1, &[2, 3], "alice2", 100)),
            2,
        );

        for label_id in [1, 2] {
            assert!(
                mt.node_by_key_at(label_id, "alice", 2).is_none(),
                "old key remained visible for label {label_id}"
            );
        }
        assert!(
            mt.node_by_key_at(1, "alice2", 2).is_none(),
            "new key became visible for removed label"
        );
        for label_id in [2, 3] {
            assert_eq!(
                mt.node_by_key_at(label_id, "alice2", 2).map(|node| node.id),
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
        mt.register_secondary_index(&entry);
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
            mt.find_secondary_eq_nodes_at(10, "name", &PropValue::String("alice".into()), 1),
            vec![1]
        );
        assert!(mt
            .find_secondary_eq_nodes_at(10, "name", &PropValue::String("alice".into()), 2)
            .is_empty());
        assert_eq!(
            mt.find_secondary_eq_nodes_at(10, "name", &PropValue::String("bob".into()), 2),
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
        mt.register_secondary_index(&node_entry);
        mt.register_secondary_index(&edge_entry);

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

        assert_eq!(mt.secondary_eq_node_hash_count_at(18, hot_hash, 1), 1);
        assert_eq!(mt.secondary_eq_node_hash_count_at(18, hot_hash, 3), 0);
        assert_eq!(mt.secondary_eq_node_hash_count_at(18, cold_hash, 3), 1);
        assert_eq!(mt.secondary_eq_node_hash_count_at(18, cold_hash, 5), 0);
        assert_eq!(mt.secondary_eq_edge_hash_count_at(19, hot_hash, 2), 1);
        assert_eq!(mt.secondary_eq_edge_hash_count_at(19, hot_hash, 4), 0);
        assert_eq!(mt.secondary_eq_edge_hash_count_at(19, cold_hash, 4), 1);
        assert_eq!(mt.secondary_eq_edge_hash_count_at(19, cold_hash, 6), 0);
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
        mt.register_secondary_index(&node_entry);
        mt.register_secondary_index(&edge_entry);

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
            mt.find_secondary_eq_nodes_at(20, "p", &neg_array, 2),
            vec![1]
        );
        assert!(mt
            .find_secondary_eq_nodes_at(20, "p", &pos_array, 2)
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

        assert_eq!(mt.secondary_eq_edge_count_at(21, "p", &neg_map, 4), 1);
        assert_eq!(mt.secondary_eq_edge_count_at(21, "p", &pos_map, 4), 0);
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
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);

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
        mt.register_secondary_index(&range_entry);

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

        let first = mt.visible_secondary_range_entries_limited(
            40,
            Some((encoded_10, true)),
            Some((encoded_50, true)),
            None,
            5,
            Some(2),
        );
        assert_eq!(first, vec![(encoded_10, 1), (encoded_20, 2)]);

        let second = mt.visible_secondary_range_entries_limited(
            40,
            Some((encoded_10, true)),
            Some((encoded_50, true)),
            first.last().copied(),
            5,
            Some(2),
        );
        assert_eq!(second, vec![(encoded_30, 3), (encoded_40, 4)]);

        assert_eq!(
            mt.visible_secondary_range_entries_limited(
                40,
                Some((encoded_20, true)),
                Some((encoded_20, true)),
                None,
                5,
                Some(10),
            ),
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
            mt.register_secondary_index(entry);
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
            mt.find_secondary_eq_nodes_at(10, "color", &PropValue::String("red".into()), 1),
            vec![1]
        );
        assert_eq!(
            mt.find_secondary_eq_nodes_at(11, "color", &PropValue::String("red".into()), 1),
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
            ),
            vec![(encoded_42, 1)]
        );
        assert_eq!(
            mt.visible_secondary_range_entries(
                13,
                Some((encoded_42, true)),
                Some((encoded_42, true)),
                None,
                1
            ),
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
            .is_empty());
        assert!(mt
            .find_secondary_eq_nodes_at(11, "color", &PropValue::String("red".into()), 2)
            .is_empty());
        assert_eq!(
            mt.find_secondary_eq_nodes_at(11, "color", &PropValue::String("blue".into()), 2),
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
            .is_empty());
        assert_eq!(
            mt.visible_secondary_range_entries(
                12,
                Some((encoded_50, true)),
                Some((encoded_50, true)),
                None,
                2
            ),
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
        mt.register_secondary_index(&entry);

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
        assert_eq!(mt.find_node_compound_prefix_at(70, &prefix, 1), vec![1]);
        assert_eq!(mt.count_node_compound_prefix_at(70, &prefix, 1), 1);

        let complex = props.get("profile").unwrap().clone();
        let complex_prefix = prefix_bounds_for(
            &entry,
            &[
                CompoundFieldValue::Property(Some(&tenant)),
                CompoundFieldValue::Property(Some(&complex)),
            ],
        );
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &complex_prefix, 1),
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
        assert!(mt.find_node_compound_prefix_at(70, &prefix, 3).is_empty());
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &prefix, 1),
            vec![1],
            "old snapshot keeps the old tuple visible"
        );
        let globex = PropValue::String("globex".into());
        let globex_prefix =
            prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&globex))]);
        assert_eq!(
            mt.find_node_compound_prefix_at(70, &globex_prefix, 3),
            vec![1]
        );

        let missing_entry = node_field_entry(
            71,
            1,
            vec![property_field("tenant"), property_field("missing_suffix")],
            SecondaryIndexKind::Equality,
        );
        mt.register_secondary_index(&missing_entry);
        let missing_prefix = prefix_bounds_for(
            &missing_entry,
            &[CompoundFieldValue::Property(Some(&globex))],
        );
        assert_eq!(
            mt.find_node_compound_prefix_at(71, &missing_prefix, 3),
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
            .is_empty());
        assert!(mt
            .find_node_compound_prefix_at(71, &missing_prefix, 4)
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
        mt.register_secondary_index(&entry);

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
        assert!(mt.find_node_compound_prefix_at(72, &prefix, 1).is_empty());

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
        assert_eq!(mt.find_node_compound_prefix_at(72, &prefix, 2), vec![1]);

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
        assert_eq!(mt.find_node_compound_range_at(72, &range, 3), vec![1]);
        assert_eq!(mt.count_node_compound_range_at(72, &range, 3), 1);

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
        assert!(mt.find_node_compound_prefix_at(72, &prefix, 4).is_empty());
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
        mt.register_secondary_index(&entry);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let before_state = mt.compound_secondary_state();
        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            1,
        );

        assert_eq!(mt.compound_secondary_state(), before_state);
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
        mt.register_secondary_index(&entry);

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
        mt.register_secondary_index(&range_entry);
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
        assert_eq!(mt.find_edge_compound_range_at(81, &edge_range, 1), vec![10]);
        assert_eq!(mt.count_edge_compound_range_at(81, &edge_range, 1), 1);

        let prefix = prefix_bounds_for(
            &entry,
            &[
                CompoundFieldValue::MetadataU64(1),
                CompoundFieldValue::MetadataU64(2),
                CompoundFieldValue::MetadataI64(10),
                CompoundFieldValue::MetadataI64(100),
            ],
        );
        assert_eq!(mt.find_edge_compound_prefix_at(80, &prefix, 1), vec![10]);
        assert_eq!(mt.count_edge_compound_prefix_at(80, &prefix, 1), 1);

        edge.valid_to = 200;
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()), 2);
        assert!(mt.find_edge_compound_prefix_at(80, &prefix, 2).is_empty());
        assert_eq!(mt.find_edge_compound_prefix_at(80, &prefix, 1), vec![10]);

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
            mt.find_edge_compound_prefix_at(80, &moved_prefix, 2),
            vec![10]
        );

        let mut other_label = edge.clone();
        other_label.label_id = 8;
        mt.apply_op(&WalOp::UpsertEdge(other_label), 3);
        assert!(mt
            .find_edge_compound_prefix_at(80, &moved_prefix, 3)
            .is_empty());

        mt.apply_op(&WalOp::UpsertEdge(edge), 4);
        assert_eq!(
            mt.find_edge_compound_prefix_at(80, &moved_prefix, 4),
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
        mt.register_secondary_index(&entry);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        let before_state = mt.compound_secondary_state();
        let mut props = BTreeMap::new();
        props.insert("status".into(), PropValue::String("open".into()));
        mt.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 8, props)),
            1,
        );

        assert_eq!(mt.compound_secondary_state(), before_state);
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
        mt.register_secondary_index(&entry);
        let mut props = BTreeMap::new();
        props.insert("tenant".into(), PropValue::String("acme".into()));
        mt.apply_op(
            &WalOp::UpsertNode(make_node_with_props(1, 1, "alice", props)),
            1,
        );

        let tenant = PropValue::String("acme".into());
        let prefix = prefix_bounds_for(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        assert_eq!(mt.find_node_compound_prefix_at(90, &prefix, 1), vec![1]);
        assert!(mt.compound_secondary_state().contains_key(&90));
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());

        assert!(mt.unregister_secondary_index(90));
        assert!(mt.find_node_compound_prefix_at(90, &prefix, 1).is_empty());
        assert!(!mt.compound_secondary_state().contains_key(&90));
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
        mt.register_secondary_index(&eq_entry);
        mt.register_secondary_index(&range_entry);

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

        mt.register_secondary_index(&eq_entry);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        mt.register_secondary_index(&range_entry);
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
        assert!(mt.get_node_at(1, u64::MAX).is_none());
        assert_eq!(mt.max_node_id(), 1);
    }
}
