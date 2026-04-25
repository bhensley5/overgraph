use crate::types::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::sync::RwLock;

fn encode_signed_range_key(value: i64) -> u64 {
    (value as u64) ^ (1u64 << 63)
}

fn encode_float_range_key(value: f64) -> Option<u64> {
    if !value.is_finite() {
        return None;
    }
    let normalized = if value == 0.0 { 0.0 } else { value };
    let bits = normalized.to_bits();
    Some(if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    })
}

pub(crate) fn encode_range_prop_value(
    domain: SecondaryIndexRangeDomain,
    value: &PropValue,
) -> Option<u64> {
    match (domain, value) {
        (SecondaryIndexRangeDomain::Int, PropValue::Int(value)) => {
            Some(encode_signed_range_key(*value))
        }
        (SecondaryIndexRangeDomain::UInt, PropValue::UInt(value)) => Some(*value),
        (SecondaryIndexRangeDomain::Float, PropValue::Float(value)) => {
            encode_float_range_key(*value)
        }
        _ => None,
    }
}

/// An adjacency entry: one edge connecting to a neighbor.
#[derive(Debug, Clone)]
pub struct AdjEntry {
    pub edge_id: u64,
    pub type_id: u32,
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
type SecondaryRangeState = HashMap<u64, BTreeMap<(u64, u64), MembershipSlot<()>>>;

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
    let prop_key_len = match &entry.target {
        SecondaryIndexTarget::NodeProperty { prop_key, .. } => prop_key.len(),
    };
    96 + prop_key_len + entry.last_error.as_ref().map(|msg| msg.len()).unwrap_or(0)
}

fn estimate_secondary_eq_lookup_entry(prop_key: &str, index_ids: &[u64]) -> usize {
    48 + prop_key.len() + index_ids.len() * 8
}

fn estimate_secondary_range_lookup_entry(
    prop_key: &str,
    indexes: &[(u64, SecondaryIndexRangeDomain)],
) -> usize {
    48 + prop_key.len() + indexes.len() * 16
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
    entries: &BTreeMap<(u64, u64), MembershipSlot<()>>,
) -> usize {
    entries.values().map(estimate_membership_slot).sum()
}

#[derive(Clone, Default)]
struct MemtableState {
    estimated_bytes: usize,
    nodes: NodeIdMap<VersionedRecordSlot<NodeRecord>>,
    edges: NodeIdMap<VersionedRecordSlot<EdgeRecord>>,
    node_tombstones: NodeIdMap<MembershipSlot<()>>,
    edge_tombstones: NodeIdMap<MembershipSlot<()>>,
    node_key_index: HashMap<u32, HashMap<String, LookupSlot<u64>>>,
    edge_triple_index: HashMap<(u64, u64, u32), LookupSlot<u64>>,
    adj_out: NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
    adj_in: NodeIdMap<NodeIdMap<MembershipSlot<AdjEntry>>>,
    type_node_index: HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
    type_edge_index: HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
    time_node_index: BTreeMap<(u32, i64, u64), MembershipSlot<()>>,
    secondary_index_declarations: HashMap<u64, SecondaryIndexManifestEntry>,
    secondary_eq_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    secondary_range_by_prop: HashMap<u32, HashMap<String, Vec<(u64, SecondaryIndexRangeDomain)>>>,
    secondary_eq_state: SecondaryEqState,
    secondary_range_state: SecondaryRangeState,
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

fn current_type_index(
    source: &HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
) -> HashMap<u32, NodeIdSet> {
    let mut result = HashMap::new();
    for (&type_id, members) in source {
        let mut visible = NodeIdSet::default();
        for (&member_id, slot) in members {
            if slot_option_current(slot).is_some() {
                visible.insert(member_id);
            }
        }
        if !visible.is_empty() {
            result.insert(type_id, visible);
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
) -> HashMap<u64, BTreeSet<(u64, u64)>> {
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

    fn set_node_key(&mut self, type_id: u32, key: &str, value: Option<u64>, write_seq: u64) {
        let by_key = self.node_key_index.entry(type_id).or_default();
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
        type_id: u32,
        value: Option<u64>,
        write_seq: u64,
    ) {
        if let Some(slot) = self.edge_triple_index.get_mut(&(from, to, type_id)) {
            let before = estimate_lookup_slot(slot);
            slot.replace(write_seq, value);
            let after = estimate_lookup_slot(slot);
            apply_size_delta(&mut self.estimated_bytes, before, after);
        } else {
            let slot = VersionedSlot::new(write_seq, value);
            self.estimated_bytes += estimate_lookup_slot(&slot);
            self.edge_triple_index.insert((from, to, type_id), slot);
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

    fn set_type_slot(
        map: &mut HashMap<u32, NodeIdMap<MembershipSlot<()>>>,
        type_id: u32,
        member_id: u64,
        present: bool,
        write_seq: u64,
    ) -> (usize, usize) {
        let value = present.then_some(());
        let members = map.entry(type_id).or_default();
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
        encoded: u64,
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

    fn set_node_type_slot(&mut self, type_id: u32, member_id: u64, present: bool, write_seq: u64) {
        let (before, after) = Self::set_type_slot(
            &mut self.type_node_index,
            type_id,
            member_id,
            present,
            write_seq,
        );
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_edge_type_slot(&mut self, type_id: u32, member_id: u64, present: bool, write_seq: u64) {
        let (before, after) = Self::set_type_slot(
            &mut self.type_edge_index,
            type_id,
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
        let (before, after) =
            Self::set_adj_slot(&mut self.adj_out, owner_id, member_id, value, write_seq);
        apply_size_delta(&mut self.estimated_bytes, before, after);
    }

    fn set_adj_in_slot(
        &mut self,
        owner_id: u64,
        member_id: u64,
        value: Option<AdjEntry>,
        write_seq: u64,
    ) {
        let (before, after) =
            Self::set_adj_slot(&mut self.adj_in, owner_id, member_id, value, write_seq);
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
        encoded: u64,
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
        let type_idx_size: usize = self
            .type_node_index
            .values()
            .map(|members| {
                members
                    .values()
                    .map(estimate_membership_slot)
                    .sum::<usize>()
            })
            .sum::<usize>()
            + self
                .type_edge_index
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

        node_size
            + edge_size
            + node_key_size
            + edge_triple_size
            + adj_out_size
            + adj_in_size
            + type_idx_size
            + time_idx_size
            + secondary_decl_size
            + secondary_eq_lookup_size
            + secondary_range_lookup_size
            + secondary_eq_state_size
            + secondary_range_state_size
    }

    fn current_node(&self, id: u64) -> Option<&NodeRecord> {
        record_current(self.nodes.get(&id)?)
    }

    fn current_edge(&self, id: u64) -> Option<&EdgeRecord> {
        record_current(self.edges.get(&id)?)
    }

    fn current_edge_triple_id(&self, from: u64, to: u64, type_id: u32) -> Option<u64> {
        self.edge_triple_index
            .get(&(from, to, type_id))
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

    fn add_secondary_index_entries_for_node(&mut self, node: &NodeRecord, write_seq: u64) {
        let eq_by_prop = &self.secondary_eq_by_prop;
        let range_by_prop = &self.secondary_range_by_prop;
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        for (prop_key, prop_value) in &node.props {
            if let Some(index_ids) = eq_by_prop
                .get(&node.type_id)
                .and_then(|by_prop| by_prop.get(prop_key.as_str()))
            {
                let value_hash = hash_prop_value(prop_value);
                for &index_id in index_ids {
                    eq_actions.push((index_id, value_hash));
                }
            }

            if let Some(indexes) = range_by_prop
                .get(&node.type_id)
                .and_then(|by_prop| by_prop.get(prop_key.as_str()))
            {
                for &(index_id, domain) in indexes {
                    if let Some(encoded) = encode_range_prop_value(domain, prop_value) {
                        range_actions.push((index_id, encoded));
                    }
                }
            }
        }
        for (index_id, value_hash) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node.id, true, write_seq);
        }
        for (index_id, encoded) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node.id, true, write_seq);
        }
    }

    fn remove_secondary_index_entries_for_node(&mut self, node: &NodeRecord, write_seq: u64) {
        let eq_by_prop = &self.secondary_eq_by_prop;
        let range_by_prop = &self.secondary_range_by_prop;
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();
        for (prop_key, prop_value) in &node.props {
            if let Some(index_ids) = eq_by_prop
                .get(&node.type_id)
                .and_then(|by_prop| by_prop.get(prop_key.as_str()))
            {
                let value_hash = hash_prop_value(prop_value);
                for &index_id in index_ids {
                    eq_actions.push((index_id, value_hash));
                }
            }

            if let Some(indexes) = range_by_prop
                .get(&node.type_id)
                .and_then(|by_prop| by_prop.get(prop_key.as_str()))
            {
                for &(index_id, domain) in indexes {
                    if let Some(encoded) = encode_range_prop_value(domain, prop_value) {
                        range_actions.push((index_id, encoded));
                    }
                }
            }
        }
        for (index_id, value_hash) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node.id, false, write_seq);
        }
        for (index_id, encoded) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node.id, false, write_seq);
        }
    }

    fn sync_secondary_index_entries_for_node_upsert(
        &mut self,
        old_node: Option<&NodeRecord>,
        new_node: &NodeRecord,
        write_seq: u64,
    ) {
        let Some(old_node) = old_node else {
            self.add_secondary_index_entries_for_node(new_node, write_seq);
            return;
        };

        if old_node.type_id != new_node.type_id {
            self.remove_secondary_index_entries_for_node(old_node, write_seq);
            self.add_secondary_index_entries_for_node(new_node, write_seq);
            return;
        }

        let eq_by_prop = &self.secondary_eq_by_prop;
        let range_by_prop = &self.secondary_range_by_prop;
        let mut eq_actions = Vec::new();
        let mut range_actions = Vec::new();

        if let Some(by_prop) = eq_by_prop.get(&new_node.type_id) {
            for (prop_key, index_ids) in by_prop {
                let old_value = old_node.props.get(prop_key.as_str());
                let new_value = new_node.props.get(prop_key.as_str());
                if old_value == new_value {
                    continue;
                }
                if let Some(old_value) = old_value {
                    let value_hash = hash_prop_value(old_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, old_node.id, false));
                    }
                }
                if let Some(new_value) = new_value {
                    let value_hash = hash_prop_value(new_value);
                    for &index_id in index_ids {
                        eq_actions.push((index_id, value_hash, new_node.id, true));
                    }
                }
            }
        }

        if let Some(by_prop) = range_by_prop.get(&new_node.type_id) {
            for (prop_key, indexes) in by_prop {
                let old_value = old_node.props.get(prop_key.as_str());
                let new_value = new_node.props.get(prop_key.as_str());
                if old_value == new_value {
                    continue;
                }
                for &(index_id, domain) in indexes {
                    if let Some(encoded) =
                        old_value.and_then(|value| encode_range_prop_value(domain, value))
                    {
                        range_actions.push((index_id, encoded, old_node.id, false));
                    }
                    if let Some(encoded) =
                        new_value.and_then(|value| encode_range_prop_value(domain, value))
                    {
                        range_actions.push((index_id, encoded, new_node.id, true));
                    }
                }
            }
        }
        for (index_id, value_hash, node_id, present) in eq_actions {
            self.set_secondary_eq_slot(index_id, value_hash, node_id, present, write_seq);
        }
        for (index_id, encoded, node_id, present) in range_actions {
            self.set_secondary_range_slot(index_id, encoded, node_id, present, write_seq);
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
                let was_deleted = state.node_deleted_at(node.id, u64::MAX);
                if let Some(old) = old_node.as_ref() {
                    if old.type_id != node.type_id || old.key != node.key {
                        state.set_node_key(old.type_id, &old.key, None, last_write_seq);
                    }
                    if old.type_id != node.type_id {
                        state.set_node_type_slot(old.type_id, old.id, false, last_write_seq);
                    }
                    if old.type_id != node.type_id || old.updated_at != node.updated_at {
                        state.set_time_slot(
                            (old.type_id, old.updated_at, old.id),
                            false,
                            last_write_seq,
                        );
                    }
                }

                let mut stored = node.clone();
                stored.last_write_seq = last_write_seq;
                state.set_node_key(node.type_id, &node.key, Some(node.id), last_write_seq);
                state.set_node_type_slot(node.type_id, node.id, true, last_write_seq);
                state.set_time_slot(
                    (node.type_id, node.updated_at, node.id),
                    true,
                    last_write_seq,
                );
                state.sync_secondary_index_entries_for_node_upsert(
                    old_node.as_ref(),
                    &stored,
                    last_write_seq,
                );
                if was_deleted {
                    state.set_node_tombstone_slot(node.id, false, last_write_seq);
                }
                state.set_node_state(node.id, RecordState::Live(stored), last_write_seq);
            }
            WalOp::UpsertEdge(edge) => {
                let old_edge = state.current_edge(edge.id).cloned();
                let was_deleted = state.edge_deleted_at(edge.id, u64::MAX);
                if let Some(old) = old_edge.as_ref() {
                    if (old.from != edge.from || old.to != edge.to || old.type_id != edge.type_id)
                        && state.current_edge_triple_id(old.from, old.to, old.type_id)
                            == Some(old.id)
                    {
                        state.set_edge_triple(old.from, old.to, old.type_id, None, last_write_seq);
                    }
                    if old.type_id != edge.type_id {
                        state.set_edge_type_slot(old.type_id, old.id, false, last_write_seq);
                    }
                    if old.from != edge.from || old.to != edge.to {
                        state.set_adj_out_slot(old.from, old.id, None, last_write_seq);
                        state.set_adj_in_slot(old.to, old.id, None, last_write_seq);
                    }
                }

                let adj_out_entry = AdjEntry {
                    edge_id: edge.id,
                    type_id: edge.type_id,
                    neighbor_id: edge.to,
                    weight: edge.weight,
                    valid_from: edge.valid_from,
                    valid_to: edge.valid_to,
                };
                let adj_in_entry = AdjEntry {
                    edge_id: edge.id,
                    type_id: edge.type_id,
                    neighbor_id: edge.from,
                    weight: edge.weight,
                    valid_from: edge.valid_from,
                    valid_to: edge.valid_to,
                };

                let current_triple_id =
                    state.current_edge_triple_id(edge.from, edge.to, edge.type_id);
                let should_update_triple = match old_edge.as_ref() {
                    Some(old)
                        if old.from == edge.from
                            && old.to == edge.to
                            && old.type_id == edge.type_id =>
                    {
                        current_triple_id.is_none_or(|current_id| current_id == edge.id)
                    }
                    _ => true,
                };
                if should_update_triple {
                    state.set_edge_triple(
                        edge.from,
                        edge.to,
                        edge.type_id,
                        Some(edge.id),
                        last_write_seq,
                    );
                }
                state.set_adj_out_slot(edge.from, edge.id, Some(adj_out_entry), last_write_seq);
                state.set_adj_in_slot(edge.to, edge.id, Some(adj_in_entry), last_write_seq);
                state.set_edge_type_slot(edge.type_id, edge.id, true, last_write_seq);

                let mut stored = edge.clone();
                stored.last_write_seq = last_write_seq;
                if was_deleted {
                    state.set_edge_tombstone_slot(edge.id, false, last_write_seq);
                }
                state.set_edge_state(edge.id, RecordState::Live(stored), last_write_seq);
            }
            WalOp::DeleteNode { id, deleted_at } => {
                if let Some(node) = state.current_node(*id).cloned() {
                    state.set_node_key(node.type_id, &node.key, None, last_write_seq);
                    state.set_node_type_slot(node.type_id, node.id, false, last_write_seq);
                    state.set_time_slot(
                        (node.type_id, node.updated_at, node.id),
                        false,
                        last_write_seq,
                    );
                    state.remove_secondary_index_entries_for_node(&node, last_write_seq);
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
                    if state.current_edge_triple_id(edge.from, edge.to, edge.type_id) == Some(*id) {
                        state.set_edge_triple(
                            edge.from,
                            edge.to,
                            edge.type_id,
                            None,
                            last_write_seq,
                        );
                    }
                    state.set_edge_type_slot(edge.type_id, edge.id, false, last_write_seq);
                    state.set_adj_out_slot(edge.from, edge.id, None, last_write_seq);
                    state.set_adj_in_slot(edge.to, edge.id, None, last_write_seq);
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
            SecondaryIndexTarget::NodeProperty { type_id, prop_key } => match &entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state.secondary_eq_by_prop.entry(*type_id).or_default();
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
                        if node.type_id != *type_id {
                            continue;
                        }
                        let Some(prop_value) = node.props.get(prop_key) else {
                            continue;
                        };
                        seeded.push((hash_prop_value(prop_value), node.id, node.last_write_seq));
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
                SecondaryIndexKind::Range { domain } => {
                    let (lookup_before, lookup_after) = {
                        let by_prop = state.secondary_range_by_prop.entry(*type_id).or_default();
                        match by_prop.get_mut(prop_key) {
                            Some(indexes) => {
                                let before =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                indexes.push((entry.index_id, *domain));
                                let after =
                                    estimate_secondary_range_lookup_entry(prop_key, indexes);
                                (before, after)
                            }
                            None => {
                                let indexes = vec![(entry.index_id, *domain)];
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
                        if node.type_id != *type_id {
                            continue;
                        }
                        let Some(prop_value) = node.props.get(prop_key) else {
                            continue;
                        };
                        let Some(encoded) = encode_range_prop_value(*domain, prop_value) else {
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
            SecondaryIndexTarget::NodeProperty { type_id, prop_key } => match entry.kind {
                SecondaryIndexKind::Equality => {
                    let (lookup_before, lookup_after, remove_type_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_type_entry = false;
                        if let Some(by_prop) = state.secondary_eq_by_prop.get_mut(&type_id) {
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
                            remove_type_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_type_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_type_entry {
                        state.secondary_eq_by_prop.remove(&type_id);
                    }
                    if let Some(groups) = state.secondary_eq_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_eq_state_groups(&groups));
                    }
                }
                SecondaryIndexKind::Range { domain } => {
                    let (lookup_before, lookup_after, remove_type_entry) = {
                        let mut delta = (0, 0);
                        let mut remove_type_entry = false;
                        if let Some(by_prop) = state.secondary_range_by_prop.get_mut(&type_id) {
                            let mut remove_prop_entry = false;
                            if let Some(indexes) = by_prop.get_mut(&prop_key) {
                                delta.0 = estimate_secondary_range_lookup_entry(&prop_key, indexes);
                                indexes.retain(|&(id, existing_domain)| {
                                    id != index_id || existing_domain != domain
                                });
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
                            remove_type_entry = by_prop.is_empty();
                        }
                        (delta.0, delta.1, remove_type_entry)
                    };
                    apply_size_delta(&mut state.estimated_bytes, lookup_before, lookup_after);
                    if remove_type_entry {
                        state.secondary_range_by_prop.remove(&type_id);
                    }
                    if let Some(entries) = state.secondary_range_state.remove(&index_id) {
                        state.estimated_bytes = state
                            .estimated_bytes
                            .saturating_sub(estimate_secondary_range_state_entries(&entries));
                    }
                }
            },
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
        type_id: u32,
        key: &str,
        snapshot_seq: u64,
    ) -> Option<NodeRecord> {
        let state = self.state.read().unwrap();
        let node_id = *slot_option_at(state.node_key_index.get(&type_id)?.get(key)?, snapshot_seq)?;
        state.node_at(node_id, snapshot_seq).cloned()
    }

    pub(crate) fn edge_by_triple_at(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
        snapshot_seq: u64,
    ) -> Option<EdgeRecord> {
        let state = self.state.read().unwrap();
        let edge_id = *slot_option_at(
            state.edge_triple_index.get(&(from, to, type_id))?,
            snapshot_seq,
        )?;
        state.edge_at(edge_id, snapshot_seq).cloned()
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

    pub(crate) fn visible_nodes_by_type(&self, type_id: u32, snapshot_seq: u64) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        if let Some(members) = state.type_node_index.get(&type_id) {
            for (&node_id, slot) in members {
                if slot_option_visible(slot, snapshot_seq) {
                    ids.push(node_id);
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn visible_edges_by_type(&self, type_id: u32, snapshot_seq: u64) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        if let Some(members) = state.type_edge_index.get(&type_id) {
            for (&edge_id, slot) in members {
                if slot_option_visible(slot, snapshot_seq) {
                    ids.push(edge_id);
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn visible_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        if from_ms > to_ms {
            return Vec::new();
        }
        let state = self.state.read().unwrap();
        use std::ops::Bound;
        let start = (type_id, from_ms, 0u64);
        let end = (type_id, to_ms, u64::MAX);
        let mut ids = state
            .time_node_index
            .range((Bound::Included(start), Bound::Included(end)))
            .filter_map(|(&(entry_type, _, node_id), slot)| {
                (entry_type == type_id && slot_option_visible(slot, snapshot_seq))
                    .then_some(node_id)
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub(crate) fn neighbors_at(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        snapshot_seq: u64,
    ) -> Vec<NeighborEntry> {
        let state = self.state.read().unwrap();
        if state.node_deleted_at(node_id, snapshot_seq) {
            return Vec::new();
        }

        let mut results = Vec::new();
        let mut self_loop_edge_ids = NodeIdSet::default();
        let mut collect = |map: Option<&NodeIdMap<MembershipSlot<AdjEntry>>>,
                           dedupe_self_loops: bool,
                           results: &mut Vec<NeighborEntry>| {
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
                if type_filter.is_some_and(|types| !types.contains(&entry.type_id)) {
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
                results.push(NeighborEntry {
                    node_id: entry.neighbor_id,
                    edge_id: entry.edge_id,
                    edge_type_id: entry.type_id,
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
        type_filter: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> Vec<NeighborEntry> {
        let state = self.state.read().unwrap();
        if state.node_deleted_at(node_id, snapshot_seq) {
            return Vec::new();
        }

        let mut results = Vec::new();
        let mut self_loop_edge_ids = NodeIdSet::default();
        let mut collect = |map: Option<&NodeIdMap<MembershipSlot<AdjEntry>>>,
                           dedupe_self_loops: bool,
                           results: &mut Vec<NeighborEntry>| {
            let Some(map) = map else {
                return;
            };
            for slot in map.values() {
                let Some(entry) = slot_option_at(slot, snapshot_seq) else {
                    continue;
                };
                if type_filter.is_some_and(|types| !types.contains(&entry.type_id)) {
                    continue;
                }
                if dedupe_self_loops && entry.neighbor_id == node_id {
                    if !self_loop_edge_ids.insert(entry.edge_id) {
                        continue;
                    }
                } else if entry.neighbor_id == node_id {
                    self_loop_edge_ids.insert(entry.edge_id);
                }
                results.push(NeighborEntry {
                    node_id: entry.neighbor_id,
                    edge_id: entry.edge_id,
                    edge_type_id: entry.type_id,
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
        type_filter: Option<&[u32]>,
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
                if type_filter.is_some_and(|types| !types.contains(&entry.type_id)) {
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

    pub(crate) fn visible_types(&self, snapshot_seq: u64) -> Vec<u32> {
        let state = self.state.read().unwrap();
        let mut types = Vec::new();
        for (&type_id, members) in &state.type_node_index {
            if members
                .values()
                .any(|slot| slot_option_visible(slot, snapshot_seq))
            {
                types.push(type_id);
            }
        }
        types.sort_unstable();
        types
    }

    pub(crate) fn find_secondary_eq_nodes_at(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        snapshot_seq: u64,
    ) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let value_hash = hash_prop_value(prop_value);
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
                        .is_some_and(|value| value == prop_value)
                    {
                        ids.push(node_id);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    pub(crate) fn visible_secondary_range_entries(
        &self,
        index_id: u64,
        lower: Option<(u64, bool)>,
        upper: Option<(u64, bool)>,
        after: Option<(u64, u64)>,
        snapshot_seq: u64,
    ) -> Vec<(u64, u64)> {
        use std::ops::Bound;

        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_range_state.get(&index_id) else {
            return Vec::new();
        };

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

        let start = match start {
            Some((target, strict)) => {
                if strict {
                    Bound::Excluded(target)
                } else {
                    Bound::Included(target)
                }
            }
            None => Bound::Unbounded,
        };

        entries
            .range((start, Bound::Unbounded))
            .filter_map(|(&(encoded, node_id), slot)| {
                if upper.is_some_and(|(upper_value, inclusive)| {
                    encoded > upper_value || (!inclusive && encoded == upper_value)
                }) {
                    return None;
                }
                slot_option_visible(slot, snapshot_seq).then_some((encoded, node_id))
            })
            .collect()
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

    /// Current-head helpers below keep the existing memtable-facing API shape
    /// for writer planning, flush, stats, and tests.
    pub fn get_node(&self, id: u64) -> Option<NodeRecord> {
        self.get_node_at(id, u64::MAX)
    }

    pub fn get_edge(&self, id: u64) -> Option<EdgeRecord> {
        self.get_edge_at(id, u64::MAX)
    }

    pub fn node_by_key(&self, type_id: u32, key: &str) -> Option<NodeRecord> {
        self.node_by_key_at(type_id, key, u64::MAX)
    }

    pub fn edge_by_triple(&self, from: u64, to: u64, type_id: u32) -> Option<EdgeRecord> {
        self.edge_by_triple_at(from, to, type_id, u64::MAX)
    }

    pub fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
    ) -> Vec<NeighborEntry> {
        self.neighbors_at(node_id, direction, type_filter, limit, u64::MAX)
    }

    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
    ) -> NodeIdMap<Vec<NeighborEntry>> {
        let mut results = NodeIdMap::default();
        for &nid in node_ids {
            let entries = self.neighbors(nid, direction, type_filter, 0);
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
        type_filter: Option<&[u32]>,
        snapshot_seq: u64,
    ) -> NodeIdMap<Vec<NeighborEntry>> {
        let mut results = NodeIdMap::default();
        for &nid in node_ids {
            let entries = self.neighbors_at(nid, direction, type_filter, 0, snapshot_seq);
            if !entries.is_empty() {
                results.insert(nid, entries);
            }
        }
        results
    }

    pub fn for_each_adj_entry<F>(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        self.for_each_adj_entry_at(node_id, direction, type_filter, u64::MAX, callback)
    }

    pub fn incident_edge_ids(&self, node_id: u64) -> Vec<u64> {
        let state = self.state.read().unwrap();
        let mut ids = Vec::new();
        if let Some(map) = state.adj_out.get(&node_id) {
            for (&edge_id, slot) in map {
                if slot_option_current(slot).is_some() {
                    ids.push(edge_id);
                }
            }
        }
        if let Some(map) = state.adj_in.get(&node_id) {
            for (&edge_id, slot) in map {
                if slot_option_current(slot).is_some() {
                    ids.push(edge_id);
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn node_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state
            .nodes
            .values()
            .filter(|slot| record_current(slot).is_some())
            .count()
    }

    pub fn edge_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state
            .edges
            .values()
            .filter(|slot| record_current(slot).is_some())
            .count()
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

    pub fn nodes_by_type(&self, type_id: u32) -> Vec<u64> {
        self.visible_nodes_by_type(type_id, u64::MAX)
    }

    pub fn edges_by_type(&self, type_id: u32) -> Vec<u64> {
        self.visible_edges_by_type(type_id, u64::MAX)
    }

    pub fn type_node_index(&self) -> HashMap<u32, NodeIdSet> {
        let state = self.state.read().unwrap();
        current_type_index(&state.type_node_index)
    }

    pub fn type_edge_index(&self) -> HashMap<u32, NodeIdSet> {
        let state = self.state.read().unwrap();
        current_type_index(&state.type_edge_index)
    }

    pub fn secondary_index_declarations(&self) -> HashMap<u64, SecondaryIndexManifestEntry> {
        let state = self.state.read().unwrap();
        state.secondary_index_declarations.clone()
    }

    pub fn secondary_eq_state(&self) -> HashMap<u64, HashMap<u64, NodeIdSet>> {
        let state = self.state.read().unwrap();
        current_secondary_eq_state(&state.secondary_eq_state)
    }

    pub fn secondary_range_state(&self) -> HashMap<u64, BTreeSet<(u64, u64)>> {
        let state = self.state.read().unwrap();
        current_secondary_range_state(&state.secondary_range_state)
    }

    pub fn time_node_index(&self) -> BTreeSet<(u32, i64, u64)> {
        let state = self.state.read().unwrap();
        current_time_index(&state.time_node_index)
    }

    pub fn nodes_by_time_range(&self, type_id: u32, from_ms: i64, to_ms: i64) -> Vec<u64> {
        self.visible_nodes_by_time_range(type_id, from_ms, to_ms, u64::MAX)
    }

    pub fn find_nodes(&self, type_id: u32, prop_key: &str, prop_value: &PropValue) -> Vec<u64> {
        self.visible_nodes_by_type(type_id, u64::MAX)
            .into_iter()
            .filter(|id| {
                self.get_node(*id)
                    .and_then(|node| node.props.get(prop_key).cloned())
                    .is_some_and(|value| value == *prop_value)
            })
            .collect()
    }

    pub fn find_secondary_eq_nodes(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Vec<u64> {
        self.find_secondary_eq_nodes_at(index_id, prop_key, prop_value, u64::MAX)
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
    fn type_node_index_key_count(&self) -> usize {
        self.type_node_index().len()
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
    use std::collections::BTreeMap;

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

    fn make_node_at(id: u64, type_id: u32, key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            updated_at,
            ..make_node(id, type_id, key)
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

    fn make_node_with_props(
        id: u64,
        type_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
    ) -> NodeRecord {
        NodeRecord {
            props,
            ..make_node(id, type_id, key)
        }
    }

    #[test]
    fn current_head_compatibility_still_works() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 1);
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)), 2);

        assert_eq!(mt.get_node(1).unwrap().key, "alice");
        assert_eq!(mt.get_edge(1).unwrap().from, 1);
        assert_eq!(mt.node_by_key(1, "alice").unwrap().id, 1);
        assert_eq!(mt.edge_by_triple(1, 2, 10).unwrap().id, 1);
    }

    #[test]
    fn snapshot_reads_keep_old_node_versions() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")), 10);
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice_v2")), 20);

        assert_eq!(mt.get_node_at(1, 10).unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 19).unwrap().key, "alice");
        assert_eq!(mt.get_node_at(1, 20).unwrap().key, "alice_v2");
        assert_eq!(mt.get_node(1).unwrap().key, "alice_v2");
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

        assert!(mt.get_node(1).is_none());
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
        assert_eq!(mt.node_by_key(1, "alice").unwrap().id, 2);
    }

    #[test]
    fn adjacency_and_type_memberships_are_snapshot_aware() {
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

        assert_eq!(mt.visible_edges_by_type(10, 3), vec![1]);
        assert!(mt.visible_edges_by_type(10, 4).is_empty());
        let before_delete = mt.neighbors_at(1, Direction::Outgoing, None, 0, 3);
        assert_eq!(before_delete.len(), 1);
        assert_eq!(before_delete[0].node_id, 2);
        assert!(mt
            .neighbors_at(1, Direction::Outgoing, None, 0, 4)
            .is_empty());
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
    fn secondary_eq_membership_history_is_snapshot_correct() {
        let mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("name".into(), PropValue::String("alice".into()));
        let entry = SecondaryIndexManifestEntry {
            index_id: 10,
            target: SecondaryIndexTarget::NodeProperty {
                type_id: 1,
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
                type_id: 1,
                prop_key: "name".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                type_id: 1,
                prop_key: "age".into(),
            },
            kind: SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
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
            .and_then(|groups| groups.get(&hash_prop_value(&PropValue::String("alice".into()))))
            .and_then(|members| members.get(&1))
            .unwrap();
        assert!(eq_slot.history.is_none());
        assert_eq!(eq_slot.head.write_seq, 1);
        assert_eq!(slot_option_current(eq_slot), Some(&()));

        let encoded_age =
            encode_range_prop_value(SecondaryIndexRangeDomain::Int, &PropValue::Int(42)).unwrap();
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
                type_id: 1,
                prop_key: "name".into(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let range_entry = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeProperty {
                type_id: 1,
                prop_key: "age".into(),
            },
            kind: SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
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
        assert_eq!(mt.type_node_index_key_count(), 0);
        assert_eq!(mt.time_node_index_len(), 0);
        assert!(mt.get_node(1).is_none());
        assert_eq!(mt.max_node_id(), 1);
    }
}
