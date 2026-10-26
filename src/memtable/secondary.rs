//! Fixed secondary dispatch is shared by generations; legacy registration owns a mutable copy.
// This bounds a refill, not aggregate allocation. A legal oversized first key
// progresses once; later keys resume from the last consumed continuation.
#[cfg(feature = "production-concurrency")]
#[derive(Default)]
struct CopyQuantum {
    bytes: usize,
    groups: usize,
}
#[cfg(feature = "production-concurrency")]
impl CopyQuantum {
    fn admit(&mut self, bytes: usize) -> bool {
        if self.groups > 0 && self.bytes.saturating_add(bytes) > 1024 * 1024 {
            return false;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.groups += 1;
        true
    }
}
use super::*;
pub(crate) type SecondaryRefill<K> = (Vec<(K, u64)>, Option<(K, u64)>, bool);
pub(crate) type CompoundSecondaryExport = HashMap<u64, Vec<(Vec<u8>, u64)>>;

#[derive(Clone, Default)]
pub(crate) struct SecondaryCatalog {
    pub(super) secondary_index_declarations: HashMap<u64, SecondaryIndexManifestEntry>,
    pub(super) secondary_eq_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    pub(super) secondary_range_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    pub(super) secondary_edge_eq_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    pub(super) secondary_edge_range_by_prop: HashMap<u32, HashMap<String, Vec<u64>>>,
    pub(super) secondary_node_field_indexes_by_label: HashMap<u32, Vec<u64>>,
    pub(super) secondary_edge_field_indexes_by_label: HashMap<u32, Vec<u64>>,
}
impl SecondaryCatalog {
    #[cfg(feature = "production-concurrency")]
    pub(in crate::memtable) fn has_node_indexes(&self, labels: &NodeLabelSet) -> bool {
        labels.as_slice().iter().any(|label| {
            self.secondary_eq_by_prop
                .get(label)
                .is_some_and(|v| !v.is_empty())
                || self
                    .secondary_range_by_prop
                    .get(label)
                    .is_some_and(|v| !v.is_empty())
                || self
                    .secondary_node_field_indexes_by_label
                    .get(label)
                    .is_some_and(|v| !v.is_empty())
        })
    }

    #[cfg(feature = "production-concurrency")]
    pub(in crate::memtable) fn has_edge_indexes(&self, label: u32) -> bool {
        self.secondary_edge_eq_by_prop
            .get(&label)
            .is_some_and(|v| !v.is_empty())
            || self
                .secondary_edge_range_by_prop
                .get(&label)
                .is_some_and(|v| !v.is_empty())
            || self
                .secondary_edge_field_indexes_by_label
                .get(&label)
                .is_some_and(|v| !v.is_empty())
    }
    pub(in crate::memtable) fn collect_secondary_index_entries_for_node_label(
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
    pub(in crate::memtable) fn collect_secondary_index_updates_for_node_label(
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
    pub(in crate::memtable) fn collect_compound_index_entries_for_node_label(
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
    pub(in crate::memtable) fn collect_compound_index_updates_for_node_label(
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
    pub(in crate::memtable) fn collect_compound_index_entries_for_edge_label(
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
    pub(in crate::memtable) fn collect_compound_index_updates_for_edge_label(
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
}

#[cfg(feature = "production-concurrency")]
mod candidate {
    use super::*;
    use crate::engine::{
        participant::{AppendMap, Session, VersionWalk},
        shared::SharedArc,
    };
    use std::{
        cmp::Reverse,
        sync::atomic::{AtomicUsize, Ordering},
    };
    pub(crate) struct OpinionMap<K: Ord + Send + Sync + 'static> {
        data: AppendMap<(K, u64, Reverse<u64>), bool>,
        estimated: AtomicUsize,
    }
    impl<K: Ord + Send + Sync + 'static> std::ops::Deref for OpinionMap<K> {
        type Target = AppendMap<(K, u64, Reverse<u64>), bool>;
        fn deref(&self) -> &Self::Target {
            &self.data
        }
    }
    impl<K: Ord + Send + Sync + 'static> OpinionMap<K> {
        fn new() -> Self {
            Self {
                data: AppendMap::new(),
                estimated: AtomicUsize::new(0),
            }
        }
    }
    type MapRefill<K> = (Vec<(K, u64)>, VersionWalk<(K, u64)>);
    pub(crate) struct FixedCatalog {
        pub(crate) incarnations: HashMap<u64, u64>,
        pub(crate) dispatch: SecondaryCatalog,
        pub(crate) entries: Vec<SecondaryIndexManifestEntry>,
        first_building: Option<usize>,
        pub(crate) query: crate::engine::SecondaryIndexCatalog,
        estimated: usize,
    }
    impl FixedCatalog {
        pub(crate) fn first_building_entry(&self) -> Option<&SecondaryIndexManifestEntry> {
            self.first_building.map(|position| &self.entries[position])
        }
        pub(crate) fn new(entries: &[SecondaryIndexManifestEntry]) -> Result<Self, EngineError> {
            let mut dispatch = SecondaryCatalog::default();
            let mut first_building = None;
            for (position, entry) in entries.iter().enumerate() {
                if entry.state == SecondaryIndexState::Building {
                    first_building.get_or_insert(position);
                }
                if entry.state != SecondaryIndexState::Ready {
                    continue;
                }
                dispatch
                    .secondary_index_declarations
                    .insert(entry.index_id, entry.clone());
                match &entry.target {
                    SecondaryIndexTarget::NodeProperty { label_id, prop_key }
                    | SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => {
                        let edge =
                            matches!(entry.target, SecondaryIndexTarget::EdgeProperty { .. });
                        let by_label = match (edge, &entry.kind) {
                            (false, SecondaryIndexKind::Equality) => {
                                &mut dispatch.secondary_eq_by_prop
                            }
                            (false, SecondaryIndexKind::Range) => {
                                &mut dispatch.secondary_range_by_prop
                            }
                            (true, SecondaryIndexKind::Equality) => {
                                &mut dispatch.secondary_edge_eq_by_prop
                            }
                            (true, SecondaryIndexKind::Range) => {
                                &mut dispatch.secondary_edge_range_by_prop
                            }
                        };
                        by_label
                            .entry(*label_id)
                            .or_default()
                            .entry(prop_key.clone())
                            .or_default()
                            .push(entry.index_id);
                    }
                    SecondaryIndexTarget::NodeFieldIndex { label_id, .. } => dispatch
                        .secondary_node_field_indexes_by_label
                        .entry(*label_id)
                        .or_default()
                        .push(entry.index_id),
                    SecondaryIndexTarget::EdgeFieldIndex { label_id, .. } => dispatch
                        .secondary_edge_field_indexes_by_label
                        .entry(*label_id)
                        .or_default()
                        .push(entry.index_id),
                }
            }
            // Approximate catalog pressure, charged once to each owning generation's estimate.
            let estimated = entries
                .iter()
                .map(|e| estimate_secondary_decl_entry(e) + 128)
                .sum();
            Ok(Self {
                incarnations: entries.iter().map(|e| (e.index_id, 0)).collect(),
                dispatch,
                entries: entries.to_vec(),
                first_building,
                query: crate::engine::build_secondary_index_catalog(entries)?,
                estimated,
            })
        }
    }
    #[derive(Default)]
    pub(crate) struct SecondaryTransition {
        eq: BTreeMap<(u64, u64, u64), bool>,
        range: BTreeMap<(u64, NumericRangeSortKey, u64), bool>,
        compound: BTreeMap<(u64, Vec<u8>, u64), bool>,
    }
    impl SecondaryTransition {
        pub(crate) fn is_empty(&self) -> bool {
            self.eq.is_empty() && self.range.is_empty() && self.compound.is_empty()
        }
        // Logical transition encoding: index/key/id/presence, with a checked
        // length prefix for compound keys. No WAL/property-record duplication.
        pub(crate) fn encoded_bytes(&self) -> Result<usize, EngineError> {
            let fail = || EngineError::InvalidOperation("index suffix length overflow".into());
            let mut n = self.eq.len().checked_mul(25).ok_or_else(fail)?;
            n = n
                .checked_add(
                    self.range
                        .len()
                        .checked_mul(17 + crate::property_value_semantics::NUMERIC_RANGE_KEY_BYTES)
                        .ok_or_else(fail)?,
                )
                .ok_or_else(fail)?;
            for (_, k, _) in self.compound.keys() {
                n = n
                    .checked_add(k.len())
                    .and_then(|v| v.checked_add(21))
                    .ok_or_else(fail)?;
            }
            Ok(n)
        }
        pub(crate) fn base_node(catalog: &FixedCatalog, node: &NodeRecord) -> Self {
            Self::node(&catalog.dispatch, None, Some(node))
        }
        pub(crate) fn base_edge(catalog: &FixedCatalog, edge: &EdgeRecord) -> Self {
            Self::edge(&catalog.dispatch, None, Some(edge))
        }

        fn properties(
            &mut self,
            c: &SecondaryCatalog,
            edge: bool,
            label: u32,
            id: u64,
            old: Option<&BTreeMap<String, PropValue>>,
            new: Option<&BTreeMap<String, PropValue>>,
        ) {
            let (eq, range) = if edge {
                (
                    &c.secondary_edge_eq_by_prop,
                    &c.secondary_edge_range_by_prop,
                )
            } else {
                (&c.secondary_eq_by_prop, &c.secondary_range_by_prop)
            };
            if let Some(props) = eq.get(&label) {
                for (key, indexes) in props {
                    let old = old.and_then(|p| p.get(key));
                    let new = new.and_then(|p| p.get(key));
                    if optional_semantic_property_eq(old, new) {
                        continue;
                    }
                    if let Some(value) = old {
                        let hash = hash_prop_equality_key(value);
                        for &index in indexes {
                            self.eq.insert((index, hash, id), false);
                        }
                    }
                    if let Some(value) = new {
                        let hash = hash_prop_equality_key(value);
                        for &index in indexes {
                            self.eq.insert((index, hash, id), true);
                        }
                    }
                }
            }
            if let Some(props) = range.get(&label) {
                for (key, indexes) in props {
                    let old = old
                        .and_then(|p| p.get(key))
                        .and_then(numeric_range_sort_key_for_value);
                    let new = new
                        .and_then(|p| p.get(key))
                        .and_then(numeric_range_sort_key_for_value);
                    if old == new {
                        continue;
                    }
                    for &index in indexes {
                        if let Some(k) = old {
                            self.range.insert((index, k, id), false);
                        }
                        if let Some(k) = new {
                            self.range.insert((index, k, id), true);
                        }
                    }
                }
            }
        }
        fn extend(&mut self, new: Self) {
            self.eq.extend(new.eq);
            self.range.extend(new.range);
            self.compound.extend(new.compound);
        }
        pub(in crate::memtable) fn node_from_older(
            c: &SecondaryCatalog,
            old: Option<&NodeRecord>,
            new: &NodeRecord,
        ) -> Self {
            let mut t = Self::node(c, old, None);
            t.extend(Self::node(c, None, Some(new)));
            t
        }
        pub(in crate::memtable) fn edge_from_older(
            c: &SecondaryCatalog,
            old: Option<&EdgeRecord>,
            new: &EdgeRecord,
        ) -> Self {
            let mut t = Self::edge(c, old, None);
            t.extend(Self::edge(c, None, Some(new)));
            t
        }
        pub(in crate::memtable) fn node(
            c: &SecondaryCatalog,
            old: Option<&NodeRecord>,
            new: Option<&NodeRecord>,
        ) -> Self {
            let mut t = Self::default();
            let mut labels = BTreeSet::new();
            for n in [old, new].into_iter().flatten() {
                labels.extend(n.label_ids.as_slice());
            }
            let mut compound = Vec::new();
            for &label in labels {
                let old = old.filter(|n| n.label_ids.contains(label));
                let new = new.filter(|n| n.label_ids.contains(label));
                let id = new.map_or_else(|| old.unwrap().id, |n| n.id);
                t.properties(
                    c,
                    false,
                    label,
                    id,
                    old.map(|n| &n.props),
                    new.map(|n| &n.props),
                );
                match (old, new) {
                    (Some(o), Some(n)) => {
                        c.collect_compound_index_updates_for_node_label(label, o, n, &mut compound)
                    }
                    (Some(o), None) => c.collect_compound_index_entries_for_node_label(
                        label,
                        o,
                        false,
                        &mut compound,
                    ),
                    (None, Some(n)) => c.collect_compound_index_entries_for_node_label(
                        label,
                        n,
                        true,
                        &mut compound,
                    ),
                    _ => {}
                }
            }
            for (i, k, id, v) in compound {
                t.compound.insert((i, k, id), v);
            }
            t
        }
        pub(in crate::memtable) fn edge(
            c: &SecondaryCatalog,
            old: Option<&EdgeRecord>,
            new: Option<&EdgeRecord>,
        ) -> Self {
            let mut t = Self::default();
            let mut labels = BTreeSet::new();
            for e in [old, new].into_iter().flatten() {
                labels.insert(e.label_id);
            }
            let mut compound = Vec::new();
            for label in labels {
                let old = old.filter(|e| e.label_id == label);
                let new = new.filter(|e| e.label_id == label);
                let id = new.map_or_else(|| old.unwrap().id, |n| n.id);
                t.properties(
                    c,
                    true,
                    label,
                    id,
                    old.map(|n| &n.props),
                    new.map(|n| &n.props),
                );
                match (old, new) {
                    (Some(o), Some(n)) => {
                        c.collect_compound_index_updates_for_edge_label(label, o, n, &mut compound)
                    }
                    (Some(o), None) => c.collect_compound_index_entries_for_edge_label(
                        label,
                        o,
                        false,
                        &mut compound,
                    ),
                    (None, Some(n)) => c.collect_compound_index_entries_for_edge_label(
                        label,
                        n,
                        true,
                        &mut compound,
                    ),
                    _ => {}
                }
            }
            for (i, k, id, v) in compound {
                t.compound.insert((i, k, id), v);
            }
            t
        }
    }
    pub(crate) struct SecondaryMaps {
        pub(in crate::memtable) catalog: SharedArc<FixedCatalog>,
        pub(in crate::memtable) eq: HashMap<u64, SharedArc<OpinionMap<u64>>>,
        pub(in crate::memtable) range: HashMap<u64, SharedArc<OpinionMap<NumericRangeSortKey>>>,
        pub(in crate::memtable) compound: HashMap<u64, SharedArc<OpinionMap<Vec<u8>>>>,
    }
    impl SecondaryMaps {
        pub(crate) fn new(
            catalog: SharedArc<FixedCatalog>,
            domain: &crate::engine::reclamation::ReclaimDomain,
        ) -> Result<Self, EngineError> {
            Self {
                catalog: catalog.clone(),
                eq: HashMap::new(),
                range: HashMap::new(),
                compound: HashMap::new(),
            }
            .rebind(catalog, None, domain)
        }
        pub(crate) fn rebind(
            &self,
            catalog: SharedArc<FixedCatalog>,
            replacement: Option<&Self>,
            domain: &crate::engine::reclamation::ReclaimDomain,
        ) -> Result<Self, EngineError> {
            fn map<K: Ord + Send + Sync + 'static>(
                id: u64,
                old: &HashMap<u64, SharedArc<OpinionMap<K>>>,
                replacement: Option<&HashMap<u64, SharedArc<OpinionMap<K>>>>,
                domain: &crate::engine::reclamation::ReclaimDomain,
            ) -> Result<SharedArc<OpinionMap<K>>, EngineError> {
                if let Some(m) = replacement
                    .and_then(|maps| maps.get(&id))
                    .or_else(|| old.get(&id))
                {
                    return Ok(m.fork_owner());
                }
                let mut m = SharedArc::new(OpinionMap::new());
                m.guard(
                    domain,
                    crate::engine::reclamation::ResourceClass::Generation,
                )?;
                Ok(m)
            }
            let mut maps = Self {
                catalog,
                eq: HashMap::new(),
                range: HashMap::new(),
                compound: HashMap::new(),
            };
            for e in &maps.catalog.entries {
                if e.state != SecondaryIndexState::Ready {
                    continue;
                }
                match (&e.target, &e.kind) {
                    (
                        SecondaryIndexTarget::NodeProperty { .. }
                        | SecondaryIndexTarget::EdgeProperty { .. },
                        SecondaryIndexKind::Equality,
                    ) => {
                        maps.eq.insert(
                            e.index_id,
                            map(e.index_id, &self.eq, replacement.map(|r| &r.eq), domain)?,
                        );
                    }
                    (
                        SecondaryIndexTarget::NodeProperty { .. }
                        | SecondaryIndexTarget::EdgeProperty { .. },
                        SecondaryIndexKind::Range,
                    ) => {
                        maps.range.insert(
                            e.index_id,
                            map(
                                e.index_id,
                                &self.range,
                                replacement.map(|r| &r.range),
                                domain,
                            )?,
                        );
                    }
                    _ => {
                        maps.compound.insert(
                            e.index_id,
                            map(
                                e.index_id,
                                &self.compound,
                                replacement.map(|r| &r.compound),
                                domain,
                            )?,
                        );
                    }
                }
            }
            Ok(maps)
        }
        #[cfg(test)]
        pub(in crate::memtable) fn estimated_full(&self, s: &Session<'_>) -> usize {
            self.catalog.estimated
                + self
                    .eq
                    .values()
                    .map(|m| m.fold_all_for_test(s, 0, |n, _, _| n + 48))
                    .sum::<usize>()
                + self
                    .range
                    .values()
                    .map(|m| {
                        m.fold_all_for_test(s, 0, |n, _, _| {
                            n + std::mem::size_of::<NumericRangeSortKey>() + 40
                        })
                    })
                    .sum::<usize>()
                + self
                    .compound
                    .values()
                    .map(|m| m.fold_all_for_test(s, 0, |n, k, _| n + k.0.len() + 48))
                    .sum::<usize>()
        }
        pub(in crate::memtable) fn estimated(&self) -> usize {
            self.catalog.estimated
                + self
                    .eq
                    .values()
                    .map(|m| m.estimated.load(Ordering::Relaxed))
                    .sum::<usize>()
                + self
                    .range
                    .values()
                    .map(|m| m.estimated.load(Ordering::Relaxed))
                    .sum::<usize>()
                + self
                    .compound
                    .values()
                    .map(|m| m.estimated.load(Ordering::Relaxed))
                    .sum::<usize>()
        }
        pub(crate) fn apply_private(
            &self,
            s: &Session<'_>,
            seq: u64,
            t: SecondaryTransition,
        ) -> Result<(), EngineError> {
            self.install(s, seq, t, None)
        }
        pub(in crate::memtable) fn install(
            &self,
            s: &Session<'_>,
            seq: u64,
            t: SecondaryTransition,
            _primary: Option<&PrimaryMaps>,
        ) -> Result<(), EngineError> {
            for ((i, k, id), v) in t.eq {
                let map = &self.eq[&i];
                map.insert_unique(s, (k, id, Reverse(seq)), v);
                map.estimated.fetch_add(48, Ordering::Relaxed);
            }
            #[cfg(test)]
            if let Some(primary) = _primary {
                primary.pause_for_test(9)?;
            }
            for ((i, k, id), v) in t.range {
                let map = &self.range[&i];
                map.insert_unique(s, (k, id, Reverse(seq)), v);
                map.estimated.fetch_add(
                    std::mem::size_of::<NumericRangeSortKey>() + 40,
                    Ordering::Relaxed,
                );
            }
            #[cfg(test)]
            if let Some(primary) = _primary {
                primary.pause_for_test(10)?;
            }
            for ((i, k, id), v) in t.compound {
                let map = &self.compound[&i];
                map.estimated.fetch_add(k.len() + 48, Ordering::Relaxed);
                map.insert_unique(s, (k, id, Reverse(seq)), v);
            }
            #[cfg(test)]
            if let Some(primary) = _primary {
                primary.pause_for_test(11)?;
            }
            Ok(())
        }
        #[allow(clippy::too_many_arguments)]
        pub(in crate::memtable) fn eq_refill(
            &self,
            s: &Session<'_>,
            i: u64,
            hash: u64,
            seq: u64,
            start: Bound<u64>,
            max: usize,
            mut accept: impl FnMut(u64) -> bool,
        ) -> (Vec<u64>, Option<u64>, bool) {
            let Some(map) = self.eq.get(&i) else {
                return (Vec::new(), None, true);
            };
            let lower = start.map(|id| (hash, id));
            let mut ids = Vec::new();
            let walk = map.walk_secondary_versions(
                s,
                seq,
                (lower, Bound::Included((hash, u64::MAX))),
                4096,
                |_, id, value| {
                    if ids.len() >= max {
                        return false;
                    }
                    if value == Some(&true) && accept(id) {
                        ids.push(id);
                    }
                    true
                },
            );
            (ids, walk.last.map(|k| k.1), walk.exhausted)
        }
        pub(in crate::memtable) fn range_refill(
            &self,
            s: &Session<'_>,
            i: u64,
            seq: u64,
            start: Bound<(NumericRangeSortKey, u64)>,
            upper: Option<(NumericRangeSortKey, bool)>,
            max: usize,
        ) -> MapRefill<NumericRangeSortKey> {
            let Some(map) = self.range.get(&i) else {
                return (
                    Vec::new(),
                    VersionWalk {
                        last: None,
                        examined: 0,
                        exhausted: true,
                    },
                );
            };
            let end = match upper {
                Some((k, true)) => Bound::Included((k, u64::MAX)),
                Some((k, false)) => Bound::Excluded((k, 0)),
                None => Bound::Unbounded,
            };
            let mut rows = Vec::new();
            let walk =
                map.walk_secondary_versions(s, seq, (start, end), 4096, |encoded, id, value| {
                    if rows.len() >= max {
                        return false;
                    }
                    if value == Some(&true) {
                        rows.push((*encoded, id));
                    }
                    true
                });
            (rows, walk)
        }
        #[allow(clippy::too_many_arguments)]
        pub(in crate::memtable) fn compound_refill(
            &self,
            s: &Session<'_>,
            i: u64,
            seq: u64,
            start: Bound<(Vec<u8>, u64)>,
            upper: &[u8],
            skip: Option<&[u8]>,
            max: usize,
        ) -> MapRefill<Vec<u8>> {
            let Some(map) = self.compound.get(&i) else {
                return (
                    Vec::new(),
                    VersionWalk {
                        last: None,
                        examined: 0,
                        exhausted: true,
                    },
                );
            };
            let mut rows = Vec::new();
            let mut quantum = CopyQuantum::default();
            let walk = map.walk_secondary_versions(
                s,
                seq,
                (start, Bound::Excluded((upper.to_vec(), 0))),
                4096,
                |tuple, id, value| {
                    if rows.len() >= max {
                        return false;
                    }
                    if !quantum.admit(tuple.len() + 8) {
                        return false;
                    }
                    if value == Some(&true) && !skip.is_some_and(|p| tuple.starts_with(p)) {
                        rows.push((tuple.clone(), id));
                    }
                    true
                },
            );
            (rows, walk)
        }
    }
    impl Memtable {
        pub(crate) fn fixed_secondary_catalog(&self) -> Option<SharedArc<FixedCatalog>> {
            self.primary.as_ref().map(|m| m.secondary.catalog.clone())
        }
    }
}
#[cfg(feature = "production-concurrency")]
pub(crate) use candidate::FixedCatalog;
#[cfg(feature = "production-concurrency")]
pub(crate) use candidate::{SecondaryMaps, SecondaryTransition};

impl Memtable {
    // Every cursor retains owned continuation; an empty quantum can still advance.
    pub(crate) fn refill_secondary_eq_ids_at(
        &self,
        index: u64,
        hash: u64,
        seq: u64,
        start: Bound<u64>,
        max: usize,
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            let session = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            #[cfg(test)]
            m.pause_for_test(12)?;
            return Ok(m
                .secondary
                .eq_refill(&session, index, hash, seq, start, max, |_| true));
        }
        let state = self.state.read().unwrap();
        let Some(group) = state
            .secondary_eq_state
            .get(&index)
            .and_then(|g| g.get(&hash))
        else {
            return Ok((Vec::new(), None, true));
        };
        let mut rows = Vec::new();
        let mut last = None;
        let mut ordered = group.iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(id, _)| **id);
        let mut iter = ordered.into_iter().filter(|(id, _)| match &start {
            Bound::Included(min) => *id >= min,
            Bound::Excluded(min) => *id > min,
            Bound::Unbounded => true,
        });
        for _ in 0..4096 {
            let Some((&id, slot)) = iter.next() else {
                return Ok((rows, last, true));
            };
            if rows.len() >= max {
                return Ok((rows, last, false));
            }
            last = Some(id);
            if slot_option_visible(slot, seq) {
                rows.push(id);
            }
        }
        Ok((rows, last, iter.next().is_none()))
    }
    pub(crate) fn refill_secondary_range_at(
        &self,
        index: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        seq: u64,
        max: usize,
    ) -> Result<SecondaryRefill<NumericRangeSortKey>, EngineError> {
        let start = secondary_range_start_bound(lower, after);
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            let (rows, walk) = m.secondary.range_refill(&s, index, seq, start, upper, max);
            return Ok((rows, walk.last, walk.exhausted));
        }
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_range_state.get(&index) else {
            return Ok((Vec::new(), None, true));
        };
        let mut rows = Vec::new();
        let mut last = None;
        let mut iter = entries.range((start, Bound::Unbounded));
        for _ in 0..4096 {
            let Some((&key, slot)) = iter.next() else {
                return Ok((rows, last, true));
            };
            if secondary_range_past_upper(key.0, upper) {
                return Ok((rows, last, true));
            }
            if rows.len() >= max {
                return Ok((rows, last, false));
            }
            last = Some(key);
            if slot_option_visible(slot, seq) {
                rows.push(key);
            }
        }
        Ok((
            rows,
            last,
            iter.next()
                .is_none_or(|(key, _)| secondary_range_past_upper(key.0, upper)),
        ))
    }
    pub(crate) fn refill_compound_at(
        &self,
        index: u64,
        start: Bound<(Vec<u8>, u64)>,
        upper: &[u8],
        skip: Option<&[u8]>,
        seq: u64,
        max: usize,
    ) -> Result<SecondaryRefill<Vec<u8>>, EngineError> {
        #[cfg(feature = "production-concurrency")]
        if let Some(m) = &self.primary {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            let (rows, w) = m
                .secondary
                .compound_refill(&s, index, seq, start, upper, skip, max);
            return Ok((rows, w.last, w.exhausted));
        }
        let state = self.state.read().unwrap();
        let Some(entries) = state.secondary_compound_state.get(&index) else {
            return Ok((Vec::new(), None, true));
        };
        let mut rows = Vec::new();
        let mut last = None;
        let mut bytes = 0usize;
        let mut iter = entries.range((start, Bound::Excluded((upper.to_vec(), 0))));
        for _ in 0..4096 {
            let Some((key, slot)) = iter.next() else {
                return Ok((rows, last, true));
            };
            if rows.len() >= max {
                return Ok((rows, last, false));
            }
            if slot_option_visible(slot, seq) && !skip.is_some_and(|p| key.0.starts_with(p)) {
                let size = key.0.len() + 8;
                if !rows.is_empty() && bytes.saturating_add(size) > 1024 * 1024 {
                    return Ok((rows, last, false));
                }
                rows.push(key.clone());
                bytes = bytes.saturating_add(size);
            }
            last = Some(key.clone());
        }
        Ok((rows, last, iter.next().is_none()))
    }
    #[cfg(feature = "production-concurrency")]
    pub(super) fn candidate_eq_collect(
        &self,
        index: u64,
        hash: u64,
        seq: u64,
        max: Option<usize>,
        verify: Option<(bool, &str, &PropValue)>,
    ) -> Result<Vec<u64>, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let mut result = Vec::new();
        let mut start = Bound::Included(0);
        if max == Some(0) {
            return Ok(result);
        }
        loop {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            let (ids, last, done) = m.secondary.eq_refill(
                &s,
                index,
                hash,
                seq,
                start,
                max.unwrap_or(usize::MAX).saturating_sub(result.len()),
                |id| {
                    verify.is_none_or(|(edge, key, value)| {
                        if edge {
                            m.edge(&s, id, seq).and_then(|e| {
                                if let RecordState::Live(e) = e {
                                    e.props.get(key)
                                } else {
                                    None
                                }
                            })
                        } else {
                            m.node(&s, id, seq).and_then(|n| {
                                if let RecordState::Live(n) = n {
                                    n.props.get(key)
                                } else {
                                    None
                                }
                            })
                        }
                        .is_some_and(|v| semantic_property_eq(v, value))
                    })
                },
            );
            result.extend(ids);
            drop(s);
            if done || max.is_some_and(|n| result.len() >= n) {
                return Ok(result);
            }
            start = Bound::Excluded(last.expect("secondary progress"));
        }
    }
}

#[cfg(feature = "production-concurrency")]
impl Memtable {
    pub(super) fn candidate_eq_count(
        &self,
        index: u64,
        hash: u64,
        seq: u64,
        verify: Option<(bool, &str, &PropValue)>,
    ) -> Result<usize, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let mut count = 0;
        let mut start = Bound::Included((hash, 0));
        let Some(map) = m.secondary.eq.get(&index) else {
            return Ok(0);
        };
        loop {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            #[cfg(test)]
            m.pause_for_test(14)?;
            let w = map.walk_secondary_versions(
                &s,
                seq,
                (start, Bound::Included((hash, u64::MAX))),
                4096,
                |_, id, value| {
                    if value == Some(&true)
                        && verify.is_none_or(|(edge, prop, value)| {
                            if edge {
                                m.edge(&s, id, seq).and_then(|e| {
                                    if let RecordState::Live(e) = e {
                                        e.props.get(prop)
                                    } else {
                                        None
                                    }
                                })
                            } else {
                                m.node(&s, id, seq).and_then(|n| {
                                    if let RecordState::Live(n) = n {
                                        n.props.get(prop)
                                    } else {
                                        None
                                    }
                                })
                            }
                            .is_some_and(|v| semantic_property_eq(v, value))
                        })
                    {
                        count += 1;
                    }
                    true
                },
            );
            drop(s);
            if w.exhausted {
                return Ok(count);
            }
            start = Bound::Excluded(w.last.expect("count progress"));
        }
    }
    pub(super) fn candidate_range_count(
        &self,
        index: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        seq: u64,
    ) -> Result<usize, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let Some(map) = m.secondary.range.get(&index) else {
            return Ok(0);
        };
        let mut start = secondary_range_start_bound(lower, after);
        let end = match upper {
            Some((k, true)) => Bound::Included((k, u64::MAX)),
            Some((k, false)) => Bound::Excluded((k, 0)),
            None => Bound::Unbounded,
        };
        let mut count = 0;
        loop {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            let w = map.walk_secondary_versions(&s, seq, (start, end), 4096, |_, _, value| {
                count += usize::from(value == Some(&true));
                true
            });
            drop(s);
            if w.exhausted {
                return Ok(count);
            }
            start = Bound::Excluded(w.last.expect("range count progress"));
        }
    }
    pub(super) fn candidate_compound_count(
        &self,
        index: u64,
        mut start: Bound<(Vec<u8>, u64)>,
        upper: &[u8],
        skip: Option<&[u8]>,
        seq: u64,
    ) -> Result<usize, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let Some(map) = m.secondary.compound.get(&index) else {
            return Ok(0);
        };
        let mut count = 0;
        loop {
            let s = m
                .admission
                .try_acquire()
                .map_err(primary::participant_error)?;
            let mut quantum = CopyQuantum::default();
            let w = map.walk_secondary_versions(
                &s,
                seq,
                (start, Bound::Excluded((upper.to_vec(), 0))),
                4096,
                |tuple, _, value| {
                    if !quantum.admit(tuple.len() + 8) {
                        return false;
                    }
                    count += usize::from(
                        value == Some(&true) && !skip.is_some_and(|p| tuple.starts_with(p)),
                    );
                    true
                },
            );
            drop(s);
            if w.exhausted {
                return Ok(count);
            }
            start = Bound::Excluded(w.last.expect("compound count progress"));
        }
    }
    pub(super) fn candidate_export_eq(
        &self,
    ) -> Result<HashMap<u64, HashMap<u64, NodeIdSet>>, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let mut all = HashMap::new();
        for (&index, map) in &m.secondary.eq {
            let mut groups: HashMap<u64, NodeIdSet> = HashMap::new();
            let mut start = Bound::Unbounded;
            loop {
                let s = m
                    .admission
                    .try_acquire()
                    .map_err(primary::participant_error)?;
                let w = map.walk_secondary_versions(
                    &s,
                    u64::MAX,
                    (start, Bound::Unbounded),
                    4096,
                    |hash, id, value| {
                        if value == Some(&true) {
                            groups.entry(*hash).or_default().insert(id);
                        }
                        true
                    },
                );
                drop(s);
                if w.exhausted {
                    break;
                }
                start = Bound::Excluded(w.last.expect("export progress"));
            }
            all.insert(index, groups);
        }
        Ok(all)
    }
    pub(super) fn candidate_export_range(
        &self,
    ) -> Result<HashMap<u64, BTreeSet<(NumericRangeSortKey, u64)>>, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let mut all = HashMap::new();
        for &index in m.secondary.range.keys() {
            all.insert(
                index,
                self.visible_secondary_range_entries_limited(
                    index,
                    None,
                    None,
                    None,
                    u64::MAX,
                    None,
                )?
                .into_iter()
                .collect(),
            );
        }
        Ok(all)
    }
    pub(super) fn candidate_export_compound(
        &self,
        indexes: Option<&[u64]>,
    ) -> Result<CompoundSecondaryExport, EngineError> {
        let m = self.primary.as_ref().unwrap();
        let mut all = HashMap::new();
        for (&index, map) in &m.secondary.compound {
            if indexes.is_some_and(|ids| !ids.contains(&index)) {
                continue;
            }
            let mut rows = Vec::new();
            let mut start = Bound::Unbounded;
            loop {
                let s = m
                    .admission
                    .try_acquire()
                    .map_err(primary::participant_error)?;
                let mut quantum = CopyQuantum::default();
                let w = map.walk_secondary_versions(
                    &s,
                    u64::MAX,
                    (start, Bound::Unbounded),
                    4096,
                    |tuple, id, value| {
                        if !quantum.admit(tuple.len() + 8) {
                            return false;
                        }
                        if value == Some(&true) {
                            rows.push((tuple.clone(), id));
                        }
                        true
                    },
                );
                drop(s);
                if w.exhausted {
                    break;
                }
                start = Bound::Excluded(w.last.expect("compound export progress"));
            }
            all.insert(index, rows);
        }
        Ok(all)
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
impl Memtable {
    pub(crate) fn locked_secondary_slots_for_test(&self) -> [usize; 4] {
        let s = self.state.read().unwrap();
        [
            s.catalog.secondary_index_declarations.len(),
            s.secondary_eq_state.len(),
            s.secondary_range_state.len(),
            s.secondary_compound_state.len(),
        ]
    }
    pub(crate) fn secondary_opinions_for_test(&self) -> (usize, usize, usize) {
        let m = self.primary.as_ref().unwrap();
        let s = m.admission.try_acquire().unwrap();
        let counts = |sum: usize, _: &_, _: &bool| sum + 1;
        (
            m.secondary
                .eq
                .values()
                .map(|map| map.fold_all_for_test(&s, 0, counts))
                .sum(),
            m.secondary
                .range
                .values()
                .map(|map| map.fold_all_for_test(&s, 0, |n, _, _| n + 1))
                .sum(),
            m.secondary
                .compound
                .values()
                .map(|map| map.fold_all_for_test(&s, 0, |n, _, _| n + 1))
                .sum(),
        )
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
impl Memtable {
    pub(crate) fn assert_secondary_cursor_contracts_for_test(
        &self,
        eq: u64,
        range: u64,
        compound: u64,
    ) {
        use std::cmp::Reverse;
        let m = self.primary.as_ref().unwrap();
        let hash = 42;
        {
            let session = m.admission.try_acquire().unwrap();
            for id in 0..4200 {
                let (seq, present) = if id % 2 == 0 { (5, false) } else { (20, true) };
                m.secondary.eq[&eq].insert_unique(&session, (hash, id, Reverse(seq)), present);
                let encoded = numeric_range_sort_key_for_value(&PropValue::Int(id as i64)).unwrap();
                m.secondary.range[&range].insert_unique(
                    &session,
                    (encoded, id, Reverse(seq)),
                    present,
                );
                m.secondary.compound[&compound].insert_unique(
                    &session,
                    (format!("a{id:06}").into_bytes(), id, Reverse(seq)),
                    present,
                );
            }
            for seq in 1..=10_000 {
                m.secondary.eq[&eq].insert_unique(&session, (hash, 5000, Reverse(seq)), true);
            }
            for id in 6000..20_000 {
                m.secondary.eq[&eq].insert_unique(&session, (hash, id, Reverse(5)), true);
            }
            let encoded = numeric_range_sort_key_for_value(&PropValue::Int(5000)).unwrap();
            m.secondary.range[&range].insert_unique(&session, (encoded, 5000, Reverse(5)), true);
            m.secondary.compound[&compound].insert_unique(
                &session,
                (b"b".to_vec(), 5000, Reverse(5)),
                true,
            );
            for i in 0..3000u64 {
                let mut key = format!("c{i:06}").into_bytes();
                key.resize(520, b'x');
                m.secondary.compound[&compound].insert_unique(&session, (key, i, Reverse(5)), true);
            }
            m.secondary.compound[&compound].insert_unique(
                &session,
                (vec![0x7f], 90_000, Reverse(5)),
                true,
            );
            for prefix in [0x80, 0x81] {
                let mut key = vec![0; 1024 * 1024 + 10];
                key[0] = prefix;
                m.secondary.compound[&compound].insert_unique(
                    &session,
                    (key, prefix as u64, Reverse(5)),
                    true,
                );
            }
        }
        let (rows, last, done) = self
            .refill_secondary_eq_ids_at(eq, hash, 10, Bound::Included(0), 128)
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(last, Some(4095));
        assert!(!done);
        let (rows, _, done) = self
            .refill_secondary_eq_ids_at(eq, hash, 10, Bound::Excluded(last.unwrap()), 1)
            .unwrap();
        assert_eq!(rows, vec![5000]);
        assert!(!done);
        let (rows, last, done) = self
            .refill_secondary_range_at(range, None, None, None, 10, 128)
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(last.unwrap().1, 4095);
        assert!(!done);
        let (rows, _, done) = self
            .refill_secondary_range_at(range, None, None, last, 10, 128)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 5000);
        assert!(done);
        let (rows, last, done) = self
            .refill_compound_at(compound, Bound::Unbounded, &[0xff], None, 10, 128)
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(last.as_ref().unwrap().1, 4095);
        assert!(!done);
        let (rows, last, done) = self
            .refill_compound_at(
                compound,
                Bound::Excluded(last.unwrap()),
                b"c",
                None,
                10,
                128,
            )
            .unwrap();
        assert_eq!(rows, vec![(b"b".to_vec(), 5000)]);
        assert!(done);
        assert!(last.is_some());
        let (rows, last, done) = self
            .refill_compound_at(
                compound,
                Bound::Included((b"c".to_vec(), 0)),
                b"d",
                None,
                10,
                4096,
            )
            .unwrap();
        assert!(rows.len() < 3000 && rows.len() > 1);
        assert!(rows.iter().map(|(k, _)| k.len() + 8).sum::<usize>() <= 1024 * 1024);
        assert!(!done);
        let (rows2, _, done) = self
            .refill_compound_at(
                compound,
                Bound::Excluded(last.unwrap()),
                b"d",
                None,
                10,
                4096,
            )
            .unwrap();
        assert!(done);
        assert_eq!(rows.len() + rows2.len(), 3000);
        assert!(rows.last().unwrap() < rows2.first().unwrap());
        let (rows, last, done) = self
            .refill_compound_at(
                compound,
                Bound::Included((vec![0x7f], 0)),
                &[0xff],
                None,
                10,
                4096,
            )
            .unwrap();
        assert_eq!(rows, vec![(vec![0x7f], 90_000)]);
        assert!(!done);
        let (rows, last, done) = self
            .refill_compound_at(
                compound,
                Bound::Excluded(last.unwrap()),
                &[0xff],
                None,
                10,
                4096,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.len(), 1024 * 1024 + 10);
        assert_eq!(rows[0].1, 0x80);
        assert!(!done);
        let (rows, _, done) = self
            .refill_compound_at(
                compound,
                Bound::Excluded(last.unwrap()),
                &[0xff],
                None,
                10,
                4096,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 0x81);
        assert!(done);
        // Hash collisions are candidates only; absent/nonmatching primary rows never match semantically.
        assert!(self
            .find_secondary_eq_nodes_at(eq, "eq", &PropValue::Int(1), 10)
            .unwrap()
            .is_empty());
    }
}
