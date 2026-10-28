//! Generation-owned primary/identity and prepared graph/secondary installation.
use super::*;
#[cfg(feature = "production-concurrency")]
use crate::engine::participant::{AppendMap, ParticipantHandle, Session};
#[cfg(feature = "production-concurrency")]
use crate::engine::payload::{Drain, Owned};
#[cfg(feature = "production-concurrency")]
use std::cmp::Reverse;
#[cfg(feature = "production-concurrency")]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[cfg(feature = "production-concurrency")]
type KeyKey = (u32, String, Reverse<u64>);
#[cfg(feature = "production-concurrency")]
type TripleKey = (u64, u64, u32, Reverse<u64>);

// One authoritative directory for node/edge opinions and every ordered consumer.
#[cfg(feature = "production-concurrency")]
pub(super) struct PrimaryMap<T: Drain> {
    directory: super::id_radix::RadixDirectory<T>,
    #[cfg(test)]
    extra_structure: AtomicUsize,
}
#[cfg(feature = "production-concurrency")]
impl<T: Drain> PrimaryMap<T> {
    fn new() -> Self {
        Self {
            directory: super::id_radix::RadixDirectory::new(),
            #[cfg(test)]
            extra_structure: AtomicUsize::new(0),
        }
    }
    fn opinion<'a>(&'a self, session: &'a Session<'_>, id: u64, seq: u64) -> Option<&'a Owned<T>> {
        self.directory
            .get(session, &id)
            .and_then(|cell| cell.at(seq))
    }
    /// Existing serialized install/replay/fixture custody is required. Fresh-ID
    /// preparation facts avoid a second directory seek. Existing IDs pay one
    /// additional install seek; no writer-side handle cache is introduced.
    unsafe fn install(
        &self,
        session: &Session<'_>,
        id: u64,
        seq: u64,
        value: Owned<T>,
        fresh: bool,
    ) -> Result<usize, EngineError> {
        use super::id_history::IdCell;
        let structure = if fresh {
            // SAFETY: same sole generation writer custody; fresh-ID fact is
            // preserved, and the directory publishes stable leaves.
            let routing = unsafe {
                self.directory
                    .insert_unique(session, id, IdCell::new(seq, value))
            };
            // Replace the prior 32-byte directory approximation with all concrete
            // leaf/routing allocations, including retained replacements.
            IdCell::<T>::singleton_structure() - 32 + routing
        } else {
            let cell = self
                .directory
                .get(session, &id)
                .expect("prepared existing primary ID");
            // SAFETY: caller has sole generation writer custody. The directory
            // never moves exposed cells; its owning generation keeps the stable
            // cell and every descriptor alive for readers.
            unsafe { cell.append(seq, value) }.map_err(|()| {
                EngineError::InvalidOperation(
                    "primary nonmonotonic sequence or history overflow".into(),
                )
            })?
        };
        // Existing payload estimates include 32 bytes per entry. Replace that
        // term with cell/descriptor/chunk structure. This remains approximate
        // pressure, not heap accounting; preserve the layout boundary assertion.
        assert!(structure >= 32);
        #[cfg(test)]
        self.extra_structure
            .fetch_add(structure - 32, Ordering::Relaxed);
        Ok(structure)
    }
    pub(super) fn walk_versions_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        seq: u64,
        bounds: (Option<u64>, u64),
        max_groups: usize,
        mut copy: impl FnMut(u64, &'a Owned<T>) -> bool,
    ) -> (Option<u64>, bool) {
        self.directory
            .walk_id_cells(session, bounds, max_groups, |id, cell| {
                cell.at(seq).is_none_or(|value| copy(id, value))
            })
    }
    fn copy_sorted_versions_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        ids: &[u64],
        seq: u64,
        mut copy: impl FnMut(u64, Option<&'a Owned<T>>) -> ControlFlow<bool>,
    ) -> usize {
        let mut previous = None;
        let mut opinion = None;
        self.directory
            .copy_sorted_id_cells(session, ids, |id, cell| {
                if previous != Some(id) {
                    previous = Some(id);
                    opinion = cell.and_then(|cell| cell.at(seq));
                }
                copy(id, opinion)
            })
    }
    #[cfg(test)]
    fn fold_all_for_test<A>(
        &self,
        session: &Session<'_>,
        initial: A,
        mut fold: impl FnMut(A, &u64, &Owned<T>) -> A,
    ) -> A {
        self.directory
            .fold_all_for_test(session, initial, |sum, id, cell| {
                cell.fold(sum, |sum, value| fold(sum, id, value))
            })
    }
    #[cfg(test)]
    fn extra_structure_inventory(&self, session: &Session<'_>) -> usize {
        self.structure_inventory_for_test(session).extra()
    }
    #[cfg(test)]
    fn structure_inventory_for_test(
        &self,
        session: &Session<'_>,
    ) -> super::id_history::StructureInventory {
        let mut inventory = self.directory.fold_all_for_test(
            session,
            super::id_history::StructureInventory::default(),
            |inventory, _, cell| inventory.plus(cell.structure_inventory_for_test()),
        );
        inventory.bytes =
            inventory.bytes - 32 * inventory.cells + self.directory.routing_inventory_for_test().1;
        inventory
    }
    #[cfg(test)]
    pub(super) fn take_scan_groups_for_test(&self) -> usize {
        self.directory.take_scan_groups_for_test()
    }
    #[cfg(test)]
    fn take_sorted_walk_work_for_test(&self) -> [usize; 2] {
        self.directory.take_sorted_walk_work_for_test()
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
impl Memtable {
    /// Quiescent structural oracle. Return (cells, descriptors, chunks) per
    /// primary family after comparing independently inventoried bytes to charges.
    pub(crate) fn primary_structure_inventory_for_test(&self) -> [(usize, usize, usize); 2] {
        let maps = self.primary.as_ref().expect("production primary");
        let (inventories, charges) = {
            let session = maps.admission.try_acquire().unwrap();
            (
                [
                    maps.nodes.structure_inventory_for_test(&session),
                    maps.edges.structure_inventory_for_test(&session),
                ],
                [
                    maps.nodes.extra_structure.load(Ordering::Relaxed),
                    maps.edges.extra_structure.load(Ordering::Relaxed),
                ],
            )
        };
        // Assert after releasing the borrowed participant session, including on
        // failure. These charges are comparison inputs, never oracle inputs.
        for (inventory, charged) in inventories.iter().zip(charges) {
            assert_eq!(
                inventory.extra(),
                charged,
                "independent structure inventory"
            );
        }
        inventories.map(|inventory| (inventory.cells, inventory.descriptors, inventory.chunks))
    }
}

#[cfg(feature = "production-concurrency")]
impl<T: Drain> Drain for RecordState<T> {
    fn drain_payload(&mut self) {
        if let Self::Live(value) = self {
            value.drain_payload();
        }
    }
}

#[cfg(feature = "production-concurrency")]
pub(super) struct PrimaryMaps {
    pub(super) admission: ParticipantHandle,
    pub(super) graph: super::graph::GraphMaps,
    nodes: PrimaryMap<RecordState<NodeRecord>>,
    pub(super) edges: PrimaryMap<RecordState<EdgeRecord>>,
    keys: AppendMap<KeyKey, Option<u64>>,
    triples: AppendMap<TripleKey, Option<u64>>,
    // One ID marker per ever-deleted record, never a second primary history.
    // Future deletes/resurrection are resolved against primary at the view's S.
    deleted_nodes: AppendMap<u64, ()>,
    deleted_edges: AppendMap<u64, ()>,
    pub(super) estimated: AtomicUsize,
    pub(super) max_node: AtomicU64,
    pub(super) max_edge: AtomicU64,
    // Physical distinct ID counts are conservative planner bounds, not live counts.
    pub(super) node_ids: AtomicUsize,
    pub(super) edge_ids: AtomicUsize,
    #[cfg(all(test, feature = "production-concurrency"))]
    hooks: std::sync::Mutex<PrimaryHooks>,
    #[cfg(all(test, feature = "production-concurrency"))]
    node_seeks: AtomicUsize,
    #[cfg(test)]
    edge_seeks: AtomicUsize,
    #[cfg(test)]
    vector_copy_bytes: [AtomicUsize; 4],
    #[cfg(test)]
    older_preparation_ids: [AtomicUsize; 4],
    #[cfg(test)]
    preparation_seeks: [AtomicUsize; 2],
}
#[cfg(feature = "production-concurrency")]
pub(super) struct PrimaryBinding {
    data: crate::engine::shared::SharedArc<PrimaryMaps>,
    pub(super) secondary: super::secondary::SecondaryMaps,
}
#[cfg(feature = "production-concurrency")]
impl std::ops::Deref for PrimaryBinding {
    type Target = PrimaryMaps;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
#[cfg(feature = "production-concurrency")]
impl PrimaryMaps {
    pub(super) fn new(admission: ParticipantHandle) -> Self {
        Self {
            admission,
            graph: super::graph::GraphMaps::new(),
            nodes: PrimaryMap::new(),
            edges: PrimaryMap::new(),
            keys: AppendMap::new(),
            triples: AppendMap::new(),
            deleted_nodes: AppendMap::new(),
            deleted_edges: AppendMap::new(),
            estimated: AtomicUsize::new(0),
            max_node: AtomicU64::new(0),
            max_edge: AtomicU64::new(0),
            node_ids: AtomicUsize::new(0),
            edge_ids: AtomicUsize::new(0),
            #[cfg(all(test, feature = "production-concurrency"))]
            hooks: std::sync::Mutex::new(PrimaryHooks::default()),
            #[cfg(all(test, feature = "production-concurrency"))]
            node_seeks: AtomicUsize::new(0),
            #[cfg(test)]
            edge_seeks: AtomicUsize::new(0),
            #[cfg(test)]
            vector_copy_bytes: std::array::from_fn(|_| AtomicUsize::new(0)),
            #[cfg(test)]
            older_preparation_ids: std::array::from_fn(|_| AtomicUsize::new(0)),
            #[cfg(test)]
            preparation_seeks: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }
    pub(super) fn graph_node_deleted(&self, session: &Session<'_>, id: u64, seq: u64) -> bool {
        self.deleted_nodes
            .lower(session, Bound::Included(&id))
            .is_some_and(|(key, _)| *key == id)
            && matches!(self.node(session, id, seq), Some(RecordState::Tombstone(_)))
    }
    pub(super) fn node<'a>(
        &'a self,
        session: &'a Session<'_>,
        id: u64,
        seq: u64,
    ) -> Option<&'a RecordState<NodeRecord>> {
        #[cfg(all(test, feature = "production-concurrency"))]
        self.node_seeks.fetch_add(1, Ordering::Relaxed);
        self.nodes.opinion(session, id, seq).map(|v| &**v)
    }
    pub(super) fn copy_nodes_sorted_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        ids: &[u64],
        seq: u64,
        mut copy: impl FnMut(u64, Option<&'a RecordState<NodeRecord>>) -> ControlFlow<bool>,
    ) -> usize {
        self.nodes
            .copy_sorted_versions_at(session, ids, seq, |id, value| {
                copy(id, value.map(|value| &**value))
            })
    }

    pub(super) fn edge<'a>(
        &'a self,
        session: &'a Session<'_>,
        id: u64,
        seq: u64,
    ) -> Option<&'a RecordState<EdgeRecord>> {
        #[cfg(test)]
        self.edge_seeks.fetch_add(1, Ordering::Relaxed);
        self.edges.opinion(session, id, seq).map(|v| &**v)
    }
    pub(super) fn key(
        &self,
        session: &Session<'_>,
        label: u32,
        key: &str,
        seq: u64,
    ) -> Option<Option<u64>> {
        self.keys
            .lower(
                session,
                Bound::Included(&(label, key.to_owned(), Reverse(seq))),
            )
            .filter(|(k, _)| k.0 == label && k.1 == key)
            .map(|(_, v)| *v)
    }
    pub(super) fn triple(
        &self,
        session: &Session<'_>,
        triple: (u64, u64, u32),
        seq: u64,
    ) -> Option<Option<u64>> {
        self.triples
            .lower(
                session,
                Bound::Included(&(triple.0, triple.1, triple.2, Reverse(seq))),
            )
            .filter(|(k, _)| (k.0, k.1, k.2) == triple)
            .map(|(_, v)| *v)
    }
    // Internal copies only; no user callback runs inside this session.
    pub(super) fn nodes_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        seq: u64,
        mut copy: impl FnMut(u64, &'a RecordState<NodeRecord>),
    ) {
        self.nodes.walk_versions_at(
            session,
            seq,
            (None, self.max_node.load(Ordering::Relaxed)),
            usize::MAX,
            |id, value| {
                copy(id, value);
                true
            },
        );
    }
    pub(super) fn edges_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        seq: u64,
        mut copy: impl FnMut(u64, &'a RecordState<EdgeRecord>),
    ) {
        self.edges.walk_versions_at(
            session,
            seq,
            (None, self.max_edge.load(Ordering::Relaxed)),
            usize::MAX,
            |id, value| {
                copy(id, value);
                true
            },
        );
    }
}

#[cfg(feature = "production-concurrency")]
pub(crate) fn participant_error(
    error: crate::engine::participant::ParticipantLimit,
) -> crate::error::EngineError {
    crate::error::EngineError::ResourceLimit {
        resource: "participants",
        requested: error.requested as u64,
        limit: error.limit as u64,
        retryable: error.retryable,
    }
}

// A short operation-scoped read: no candidate derived lock and no per-row lease.
pub(super) enum PrimaryRead<'a> {
    Legacy(std::sync::RwLockReadGuard<'a, MemtableState>),
    #[cfg(feature = "production-concurrency")]
    Candidate(&'a PrimaryMaps, Session<'a>),
}
impl PrimaryRead<'_> {
    pub(super) fn deleted_node_ids_at(&self, seq: u64, mut examined: impl FnMut()) -> NodeIdSet {
        let mut ids = NodeIdSet::default();
        match self {
            Self::Legacy(state) => {
                for (&id, slot) in &state.node_tombstones {
                    examined();
                    if slot_option_visible(slot, seq) {
                        ids.insert(id);
                    }
                }
            }
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, session) => {
                m.deleted_nodes
                    .visit_ids(session, m.max_node.load(Ordering::Relaxed), |id| {
                        examined();
                        if matches!(m.node(session, id, seq), Some(RecordState::Tombstone(_))) {
                            ids.insert(id);
                        }
                    });
            }
        }
        ids
    }
    pub(super) fn deleted_edge_ids_at(&self, seq: u64, mut examined: impl FnMut()) -> NodeIdSet {
        let mut ids = NodeIdSet::default();
        match self {
            Self::Legacy(state) => {
                for (&id, slot) in &state.edge_tombstones {
                    examined();
                    if slot_option_visible(slot, seq) {
                        ids.insert(id);
                    }
                }
            }
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, session) => {
                m.deleted_edges
                    .visit_ids(session, m.max_edge.load(Ordering::Relaxed), |id| {
                        examined();
                        if matches!(m.edge(session, id, seq), Some(RecordState::Tombstone(_))) {
                            ids.insert(id);
                        }
                    });
            }
        }
        ids
    }
    pub(super) fn node_state(&self, id: u64, seq: u64) -> Option<&RecordState<NodeRecord>> {
        match self {
            Self::Legacy(s) => s.nodes.get(&id).and_then(|v| record_at(v, seq)),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.node(s, id, seq),
        }
    }
    pub(super) fn edge_state(&self, id: u64, seq: u64) -> Option<&RecordState<EdgeRecord>> {
        match self {
            Self::Legacy(s) => s.edges.get(&id).and_then(|v| record_at(v, seq)),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.edge(s, id, seq),
        }
    }
    pub(super) fn node_at(&self, id: u64, seq: u64) -> Option<&NodeRecord> {
        match self.node_state(id, seq)? {
            RecordState::Live(v) => Some(v),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn edge_at(&self, id: u64, seq: u64) -> Option<&EdgeRecord> {
        match self.edge_state(id, seq)? {
            RecordState::Live(value) => Some(value),
            _ => None,
        }
    }
    pub(super) fn node_deleted_at(&self, id: u64, seq: u64) -> bool {
        matches!(self.node_state(id, seq), Some(RecordState::Tombstone(_)))
    }
    pub(super) fn edge_deleted_at(&self, id: u64, seq: u64) -> bool {
        matches!(self.edge_state(id, seq), Some(RecordState::Tombstone(_)))
    }
    // A resolved candidate Missing has no separate tombstone opinion to seek.
    // The legacy representation retains its independent tombstone maps.
    pub(super) fn missing_node_deleted_at(&self, id: u64, seq: u64) -> bool {
        match self {
            Self::Legacy(state) => state.node_deleted_at(id, seq),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(..) => false,
        }
    }
    pub(super) fn missing_edge_deleted_at(&self, id: u64, seq: u64) -> bool {
        match self {
            Self::Legacy(state) => state.edge_deleted_at(id, seq),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(..) => false,
        }
    }
    pub(super) fn key(&self, label: u32, key: &str, seq: u64) -> Option<Option<u64>> {
        match self {
            Self::Legacy(s) => s.node_key_index.get(&label)?.get(key)?.at(seq).copied(),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.key(s, label, key, seq),
        }
    }
    pub(super) fn keys(&self, keys: &[(u32, &str)], seq: u64) -> Vec<IdentityOpinion> {
        match self {
            Self::Legacy(state) => keys
                .iter()
                .map(|&(label, key)| {
                    IdentityOpinion::from(
                        state
                            .node_key_index
                            .get(&label)
                            .and_then(|by_key| by_key.get(key))
                            .and_then(|slot| slot.at(seq))
                            .copied(),
                    )
                })
                .collect(),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, session) => {
                let mut seek = (0, String::new(), Reverse(seq));
                keys.iter()
                    .map(|&(label, key)| {
                        seek.0 = label;
                        seek.1.clear();
                        seek.1.push_str(key);
                        m.keys
                            .lower(session, Bound::Included(&seek))
                            .filter(|(found, _)| found.0 == label && found.1 == key)
                            .map(|(_, value)| *value)
                            .into()
                    })
                    .collect()
            }
        }
    }
    pub(super) fn triple(&self, triple: (u64, u64, u32), seq: u64) -> Option<Option<u64>> {
        match self {
            Self::Legacy(s) => s.edge_triple_index.get(&triple)?.at(seq).copied(),
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.triple(s, triple, seq),
        }
    }
    pub(super) fn nodes_at<'a>(
        &'a self,
        seq: u64,
        mut f: impl FnMut(u64, &'a RecordState<NodeRecord>),
    ) {
        match self {
            Self::Legacy(s) => {
                for (&id, slot) in &s.nodes {
                    if let Some(v) = record_at(slot, seq) {
                        f(id, v);
                    }
                }
            }
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.nodes_at(s, seq, f),
        }
    }
    pub(super) fn edges_at<'a>(
        &'a self,
        seq: u64,
        mut f: impl FnMut(u64, &'a RecordState<EdgeRecord>),
    ) {
        match self {
            Self::Legacy(s) => {
                for (&id, slot) in &s.edges {
                    if let Some(v) = record_at(slot, seq) {
                        f(id, v);
                    }
                }
            }
            #[cfg(feature = "production-concurrency")]
            Self::Candidate(m, s) => m.edges_at(s, seq, f),
        }
    }
}

// All payload/identity allocations and old-state resolution precede WAL. Each
// normalized op keeps its own sequence; only duplicate membership full keys within
// that op coalesce. Single-coordinator custody makes these transition inputs stable.
#[cfg(feature = "production-concurrency")]
pub(crate) struct PreparedPrimary {
    transitions: Vec<PreparedTransition>,
    graph_updates: super::graph::GraphUpdates,
    frontier: u64,
}
#[cfg(feature = "production-concurrency")]
impl PreparedPrimary {
    pub(crate) fn take_build_transitions(
        &mut self,
    ) -> Vec<(u64, super::secondary::SecondaryTransition)> {
        self.transitions
            .iter_mut()
            .filter_map(|t| {
                t.build_secondary
                    .take()
                    .filter(|v| !v.is_empty())
                    .map(|v| (t.seq, v))
            })
            .collect()
    }
}
#[cfg(feature = "production-concurrency")]
struct PreparedTransition {
    seq: u64,
    graph: super::graph::GraphTransition,
    secondary: super::secondary::SecondaryTransition,
    build_secondary: Option<super::secondary::SecondaryTransition>,
    old_node_live: bool,
    old_edge_live: bool,
    node: Option<(u64, Owned<RecordState<NodeRecord>>)>,
    edge: Option<(u64, Owned<RecordState<EdgeRecord>>)>,
    keys: Vec<(KeyKey, Option<u64>)>,
    triples: Vec<(TripleKey, Option<u64>)>,
    approximate: usize,
    new_node_id: bool,
    new_edge_id: bool,
}
#[cfg(feature = "production-concurrency")]
impl Memtable {
    pub(crate) fn participant_handle(&self) -> Option<ParticipantHandle> {
        self.primary.as_ref().map(|m| m.admission.clone())
    }

    pub(crate) fn new_candidate_with_catalog(
        admission: ParticipantHandle,
        catalog: crate::engine::shared::SharedArc<super::secondary::FixedCatalog>,
        domain: &crate::engine::reclamation::ReclaimDomain,
    ) -> Result<Self, EngineError> {
        use crate::engine::{reclamation::ResourceClass, shared::SharedArc};
        let mut data = SharedArc::new(PrimaryMaps::new(admission));
        data.guard(domain, ResourceClass::Generation)?;
        Ok(Self {
            state: new_memtable_state(MemtableState::default()),
            primary: Some(PrimaryBinding {
                data,
                secondary: super::secondary::SecondaryMaps::new(catalog, domain)?,
            }),
            #[cfg(test)]
            tombstone_scan_work: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    // A new view binding shares the generation's primary/graph/diagnostics and all
    // unchanged posting owners. The optional replacement was built privately.
    pub(crate) fn rebind_secondary(
        &self,
        catalog: crate::engine::shared::SharedArc<super::secondary::FixedCatalog>,
        replacement: Option<&super::secondary::SecondaryMaps>,
        domain: &crate::engine::reclamation::ReclaimDomain,
    ) -> Result<Self, EngineError> {
        let old = self.primary.as_ref().expect("candidate binding");
        Ok(Self {
            state: self.state.clone(),
            primary: Some(PrimaryBinding {
                data: old.data.fork_owner(),
                secondary: old.secondary.rebind(catalog, replacement, domain)?,
            }),
            #[cfg(test)]
            tombstone_scan_work: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    pub(crate) fn same_generation(&self, other: &Self) -> bool {
        match (&self.primary, &other.primary) {
            (Some(a), Some(b)) => crate::engine::shared::SharedArc::ptr_eq(&a.data, &b.data),
            _ => false,
        }
    }
    pub(crate) fn prepare_primary(
        &self,
        ops: &[WalOp],
        first_seq: u64,
        sources: Option<&crate::source_list::SourceList<'_>>,
        existing_node_limit: u64,
        existing_edge_limit: u64,
    ) -> Result<Option<PreparedPrimary>, crate::error::EngineError> {
        self.prepare_primary_with_build(
            ops,
            first_seq,
            sources,
            existing_node_limit,
            existing_edge_limit,
            None,
        )
    }
    pub(crate) fn prepare_primary_with_build(
        &self,
        ops: &[WalOp],
        first_seq: u64,
        sources: Option<&crate::source_list::SourceList<'_>>,
        existing_node_limit: u64,
        existing_edge_limit: u64,
        build_catalog: Option<&super::secondary::FixedCatalog>,
    ) -> Result<Option<PreparedPrimary>, crate::error::EngineError> {
        let Some(m) = &self.primary else {
            return Ok(None);
        };
        // Resolve older-source identities before borrowing the install collector.
        // New IDs skip this work; same-batch transitions use the local overlays below.
        let mut node_identities: NodeIdMap<Option<(NodeLabelSet, String, i64)>> =
            NodeIdMap::default();
        let mut edge_identities: NodeIdMap<Option<(u64, u64, u32)>> = NodeIdMap::default();
        let mut triple_owners = HashMap::new();
        let mut secondary_old_nodes: NodeIdMap<Owned<Option<NodeRecord>>> = NodeIdMap::default();
        let mut secondary_old_edges: NodeIdMap<Owned<Option<EdgeRecord>>> = NodeIdMap::default();
        let secondary_enabled = !m
            .secondary
            .catalog
            .dispatch
            .secondary_index_declarations
            .is_empty()
            || build_catalog.is_some();
        let older_sources = sources.filter(|s| !s.immutable.is_empty() || !s.segments.is_empty());
        let mut node_ids = NodeIdSet::default();
        let mut edge_ids = NodeIdSet::default();
        let mut triples = BTreeSet::new();
        let mut nodes: NodeIdMap<usize> = NodeIdMap::default();
        let mut edges: NodeIdMap<usize> = NodeIdMap::default();
        let mut transitions = Vec::with_capacity(ops.len());
        let mut preparation_session = Some(m.admission.try_acquire().map_err(participant_error)?);
        {
            let read = preparation_session.as_ref().expect("preparation session");
            for (i, op) in ops.iter().enumerate() {
                let previous = match op {
                    WalOp::UpsertNode(n) => nodes.insert(n.id, i),
                    WalOp::DeleteNode { id, .. } => nodes.insert(*id, i),
                    WalOp::UpsertEdge(e) => edges.insert(e.id, i),
                    WalOp::DeleteEdge { id, .. } => edges.insert(*id, i),
                    _ => None,
                };
                let (old_node, new_node_id) = match op {
                    WalOp::UpsertNode(n) => prepare_old_node(
                        m,
                        read,
                        ops,
                        previous,
                        n.id,
                        sources.map(|_| existing_node_limit),
                    ),
                    WalOp::DeleteNode { id, .. } => prepare_old_node(
                        m,
                        read,
                        ops,
                        previous,
                        *id,
                        sources.map(|_| existing_node_limit),
                    ),
                    _ => (None, false),
                };
                let (old_edge, new_edge_id) = match op {
                    WalOp::UpsertEdge(e) => prepare_old_edge(
                        m,
                        read,
                        ops,
                        previous,
                        e.id,
                        sources.map(|_| existing_edge_limit),
                    ),
                    WalOp::DeleteEdge { id, .. } => prepare_old_edge(
                        m,
                        read,
                        ops,
                        previous,
                        *id,
                        sources.map(|_| existing_edge_limit),
                    ),
                    _ => (None, false),
                };
                let node_id = match op {
                    WalOp::UpsertNode(n) => Some(n.id),
                    WalOp::DeleteNode { id, .. } => Some(*id),
                    _ => None,
                };
                if let Some(id) = node_id {
                    node_identities.entry(id).or_insert_with(|| {
                        old_node.map(|n| (n.label_ids, n.key.clone(), n.updated_at))
                    });
                    if older_sources.is_some() && new_node_id && id < existing_node_limit {
                        node_ids.insert(id);
                    }
                }
                let edge_id = match op {
                    WalOp::UpsertEdge(e) => {
                        if older_sources.is_some() {
                            triples.insert((e.from, e.to, e.label_id));
                        }
                        Some(e.id)
                    }
                    WalOp::DeleteEdge { id, .. } => Some(*id),
                    _ => None,
                };
                if let Some(id) = edge_id {
                    edge_identities
                        .entry(id)
                        .or_insert_with(|| old_edge.map(|e| (e.from, e.to, e.label_id)));
                    if older_sources.is_some() {
                        if let Some(e) = old_edge {
                            triples.insert((e.from, e.to, e.label_id));
                        }
                    }
                    if older_sources.is_some() && new_edge_id && id < existing_edge_limit {
                        edge_ids.insert(id);
                    }
                }
                let needs_older = older_sources.is_some()
                    && ((new_node_id && node_id.is_some_and(|id| id < existing_node_limit))
                        || (new_edge_id && edge_id.is_some_and(|id| id < existing_edge_limit)));
                let secondary = |catalog: &super::secondary::SecondaryCatalog| match op {
                    WalOp::UpsertNode(n) => {
                        super::secondary::SecondaryTransition::node(catalog, old_node, Some(n))
                    }
                    WalOp::DeleteNode { .. } => {
                        super::secondary::SecondaryTransition::node(catalog, old_node, None)
                    }
                    WalOp::UpsertEdge(e) => {
                        super::secondary::SecondaryTransition::edge(catalog, old_edge, Some(e))
                    }
                    WalOp::DeleteEdge { .. } => {
                        super::secondary::SecondaryTransition::edge(catalog, old_edge, None)
                    }
                    _ => super::secondary::SecondaryTransition::default(),
                };
                transitions.push(PreparedTransition {
                    seq: first_seq + i as u64,
                    graph: super::graph::GraphTransition::default(),
                    secondary: if secondary_enabled && !needs_older {
                        secondary(&m.secondary.catalog.dispatch)
                    } else {
                        Default::default()
                    },
                    build_secondary: build_catalog
                        .filter(|_| !needs_older)
                        .map(|c| secondary(&c.dispatch)),
                    old_node_live: old_node.is_some(),
                    old_edge_live: old_edge.is_some(),
                    node: None,
                    edge: None,
                    keys: Vec::new(),
                    triples: Vec::new(),
                    approximate: 0,
                    new_node_id,
                    new_edge_id,
                });
            }
        }
        drop((nodes, edges));
        // Only owned metadata and derived actions survive the first pass. End
        // the session before any older-source I/O; otherwise reuse it for graph
        // preparation. No record borrow spans a source lookup. This preparation
        // starts after rotation and uses this binding's fixed catalog throughout.
        if older_sources.is_some()
            && (!node_ids.is_empty() || !edge_ids.is_empty() || !triples.is_empty())
        {
            drop(preparation_session.take());
        }
        if let Some(sources) = older_sources {
            if !node_ids.is_empty() {
                let ids = node_ids.into_iter().collect::<Vec<_>>();
                #[cfg(test)]
                m.older_preparation_ids[0].fetch_add(ids.len(), Ordering::Relaxed);
                let fields = sources.find_node_projected_fields(
                    &ids,
                    &NodeSelectedFieldNeeds {
                        key: true,
                        ..Default::default()
                    },
                )?;
                // Metadata resolves the old labels before deciding whether any
                // maintained or registered-build posting needs old full fields.
                let secondary_node_ids = if secondary_enabled {
                    ids.iter()
                        .zip(&fields)
                        .filter_map(|(&id, row)| {
                            let labels = &row.as_ref()?.meta.label_ids;
                            (m.secondary.catalog.dispatch.has_node_indexes(labels)
                                || build_catalog
                                    .is_some_and(|c| c.dispatch.has_node_indexes(labels)))
                            .then_some(id)
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if !secondary_node_ids.is_empty() {
                    #[cfg(test)]
                    m.older_preparation_ids[2]
                        .fetch_add(secondary_node_ids.len(), Ordering::Relaxed);
                    let mut records = Owned::new(sources.find_nodes(&secondary_node_ids)?);
                    for &id in secondary_node_ids.iter().rev() {
                        secondary_old_nodes
                            .insert(id, Owned::new(records.pop().expect("batch rows")));
                    }
                }
                for (id, value) in ids.into_iter().zip(fields) {
                    node_identities.insert(
                        id,
                        value.map(|n| {
                            (
                                n.meta.label_ids,
                                n.key.expect("requested key"),
                                n.meta.updated_at,
                            )
                        }),
                    );
                }
            }
            if !edge_ids.is_empty() {
                let ids = edge_ids.into_iter().collect::<Vec<_>>();
                #[cfg(test)]
                m.older_preparation_ids[1].fetch_add(ids.len(), Ordering::Relaxed);
                let fields = sources.find_edge_metadata(&ids)?;
                let secondary_edge_ids = if secondary_enabled {
                    ids.iter()
                        .zip(&fields)
                        .filter_map(|(&id, row)| {
                            let label = row.as_ref()?.label_id;
                            (m.secondary.catalog.dispatch.has_edge_indexes(label)
                                || build_catalog
                                    .is_some_and(|c| c.dispatch.has_edge_indexes(label)))
                            .then_some(id)
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                if !secondary_edge_ids.is_empty() {
                    #[cfg(test)]
                    m.older_preparation_ids[3]
                        .fetch_add(secondary_edge_ids.len(), Ordering::Relaxed);
                    let mut records = Owned::new(sources.find_edges(&secondary_edge_ids)?);
                    for &id in secondary_edge_ids.iter().rev() {
                        secondary_old_edges
                            .insert(id, Owned::new(records.pop().expect("batch rows")));
                    }
                }
                for (id, value) in ids.into_iter().zip(fields) {
                    let triple = value.map(|e| (e.from, e.to, e.label_id));
                    if let Some(t) = triple {
                        triples.insert(t);
                    }
                    edge_identities.insert(id, triple);
                }
            }
            if !triples.is_empty() {
                let triples = triples.into_iter().collect::<Vec<_>>();
                let rows = sources.find_edge_ids_by_triples(&triples)?;
                for (&triple, value) in triples.iter().zip(rows.iter()) {
                    triple_owners.insert(triple, *value);
                }
            }
        }
        let session = match preparation_session {
            Some(session) => session,
            None => m.admission.try_acquire().map_err(participant_error)?,
        };
        let mut graph_preparation = m.graph.prepare();
        for (op, t) in ops.iter().zip(transitions.iter_mut()) {
            let seq = t.seq;
            let node_id = match op {
                WalOp::UpsertNode(n) => Some(n.id),
                WalOp::DeleteNode { id, .. } => Some(*id),
                _ => None,
            };
            let graph_old_node = node_id.and_then(|id| {
                node_identities
                    .get(&id)
                    .and_then(|v| v.as_ref())
                    .map(|(labels, _, time)| (*labels, *time))
            });
            let graph_old_edge = match op {
                WalOp::UpsertEdge(e) => edge_identities.get(&e.id).copied().flatten(),
                WalOp::DeleteEdge { id, .. } => edge_identities.get(id).copied().flatten(),
                _ => None,
            };
            let secondary_old_node = match op {
                WalOp::UpsertNode(n) => secondary_old_nodes.remove(&n.id),
                WalOp::DeleteNode { id, .. } => secondary_old_nodes.remove(id),
                _ => None,
            };
            let secondary_old_edge = match op {
                WalOp::UpsertEdge(e) => secondary_old_edges.remove(&e.id),
                WalOp::DeleteEdge { id, .. } => secondary_old_edges.remove(id),
                _ => None,
            };
            // The identity overlay advances in operation order, while the first
            // pass already derived secondary actions against each prior record.
            // Only metadata and active-live provenance are needed for graph work.
            let local_node = t.old_node_live.then_some(graph_old_node).flatten();
            let local_edge = t.old_edge_live.then_some(graph_old_edge).flatten();
            match op {
                WalOp::UpsertNode(node) => {
                    let id = node.id;
                    let mut keys = Vec::new();
                    let old_identity = node_identities
                        .get_mut(&id)
                        .expect("resolved node identity");
                    if let Some((labels, key, _)) = old_identity.as_ref() {
                        for &label in labels.as_slice() {
                            if !node.label_ids.contains(label) || *key != node.key {
                                keys.push(((label, key.clone(), Reverse(seq)), None));
                            }
                        }
                    }
                    *old_identity = Some((node.label_ids, node.key.clone(), node.updated_at));
                    for &label in node.label_ids.as_slice() {
                        keys.push(((label, node.key.clone(), Reverse(seq)), Some(id)));
                    }
                    // Canonical labels are unique; retracted and added full keys are disjoint.
                    t.keys = keys;
                    let mut stored = node.clone();
                    stored.last_write_seq = seq;
                    t.approximate = Memtable::estimate_node_record(&stored) + 32;
                    t.node = Some((id, Owned::new(RecordState::Live(stored))));
                }
                WalOp::DeleteNode { id, deleted_at } => {
                    let old_identity = node_identities.get_mut(id).expect("resolved node identity");
                    if let Some((labels, key, _)) = old_identity.take() {
                        for &label in labels.as_slice() {
                            t.keys.push(((label, key.clone(), Reverse(seq)), None));
                        }
                    }
                    t.node = Some((
                        *id,
                        Owned::new(RecordState::Tombstone(TombstoneEntry {
                            deleted_at: *deleted_at,
                            last_write_seq: seq,
                        })),
                    ));
                    t.approximate = 48;
                }
                WalOp::UpsertEdge(edge) => {
                    let id = edge.id;
                    let triple = (edge.from, edge.to, edge.label_id);
                    let mut triples = Vec::new();
                    let old_identity = edge_identities
                        .get_mut(&id)
                        .expect("resolved edge identity");
                    if let Some(ot) = *old_identity {
                        let owner = triple_owners
                            .entry(ot)
                            .or_insert_with(|| m.triple(&session, ot, u64::MAX).flatten());
                        if ot != triple && *owner == Some(id) {
                            *owner = None;
                            triples.push(((ot.0, ot.1, ot.2, Reverse(seq)), None));
                        }
                    }
                    let owner = triple_owners
                        .entry(triple)
                        .or_insert_with(|| m.triple(&session, triple, u64::MAX).flatten());
                    let same = *old_identity == Some(triple);
                    *old_identity = Some(triple);
                    if !same || owner.is_none_or(|own| own == id) {
                        *owner = Some(id);
                        triples.push(((triple.0, triple.1, triple.2, Reverse(seq)), Some(id)));
                    }
                    // Retraction requires a changed triple, so the two keys cannot
                    // collide. Keep the former tree's deterministic key order.
                    triples.sort_unstable_by_key(|(key, _)| *key);
                    t.triples = triples;
                    let mut stored = edge.clone();
                    stored.last_write_seq = seq;
                    t.approximate = Memtable::estimate_edge_record(&stored) + 32;
                    t.edge = Some((id, Owned::new(RecordState::Live(stored))));
                }
                WalOp::DeleteEdge { id, deleted_at } => {
                    let old_identity = edge_identities.get_mut(id).expect("resolved edge identity");
                    if let Some(triple) = old_identity.take() {
                        let owner = triple_owners
                            .entry(triple)
                            .or_insert_with(|| m.triple(&session, triple, u64::MAX).flatten());
                        if *owner == Some(*id) {
                            *owner = None;
                            t.triples
                                .push(((triple.0, triple.1, triple.2, Reverse(seq)), None));
                        }
                    }
                    t.edge = Some((
                        *id,
                        Owned::new(RecordState::Tombstone(TombstoneEntry {
                            deleted_at: *deleted_at,
                            last_write_seq: seq,
                        })),
                    ));
                    t.approximate = 48;
                }
                _ => {}
            }
            let secondary = |catalog: &super::secondary::SecondaryCatalog| match op {
                WalOp::UpsertNode(n) => super::secondary::SecondaryTransition::node_from_older(
                    catalog,
                    secondary_old_node.as_ref().and_then(|v| v.as_ref()),
                    n,
                ),
                WalOp::UpsertEdge(e) => super::secondary::SecondaryTransition::edge_from_older(
                    catalog,
                    secondary_old_edge.as_ref().and_then(|v| v.as_ref()),
                    e,
                ),
                WalOp::DeleteNode { .. } => super::secondary::SecondaryTransition::node(
                    catalog,
                    secondary_old_node.as_ref().and_then(|v| v.as_ref()),
                    None,
                ),
                WalOp::DeleteEdge { .. } => super::secondary::SecondaryTransition::edge(
                    catalog,
                    secondary_old_edge.as_ref().and_then(|v| v.as_ref()),
                    None,
                ),
                _ => super::secondary::SecondaryTransition::default(),
            };
            let needs_older = older_sources.is_some()
                && match op {
                    WalOp::UpsertNode(n) => t.new_node_id && n.id < existing_node_limit,
                    WalOp::DeleteNode { id, .. } => t.new_node_id && *id < existing_node_limit,
                    WalOp::UpsertEdge(e) => t.new_edge_id && e.id < existing_edge_limit,
                    WalOp::DeleteEdge { id, .. } => t.new_edge_id && *id < existing_edge_limit,
                    _ => false,
                };
            if needs_older {
                if secondary_enabled {
                    t.secondary = secondary(&m.secondary.catalog.dispatch);
                }
                t.build_secondary = build_catalog.map(|catalog| secondary(&catalog.dispatch));
            }
            t.graph = match op {
                WalOp::UpsertNode(n) => {
                    graph_preparation.node(&session, n.id, graph_old_node, local_node, Some(n))
                }
                WalOp::DeleteNode { id, .. } => {
                    graph_preparation.node(&session, *id, graph_old_node, local_node, None)
                }
                WalOp::UpsertEdge(e) => {
                    graph_preparation.edge(&session, e.id, graph_old_edge, local_edge, Some(e))?
                }
                WalOp::DeleteEdge { id, .. } => {
                    graph_preparation.edge(&session, *id, graph_old_edge, local_edge, None)?
                }
                _ => super::graph::GraphTransition::default(),
            };
            t.approximate += t
                .keys
                .iter()
                .map(|(key, _)| key.1.len() + 64)
                .sum::<usize>()
                + t.triples.len() * 72;
        }
        let graph_updates = graph_preparation.finish();
        drop(session);
        Ok(Some(PreparedPrimary {
            transitions,
            graph_updates,
            frontier: first_seq + ops.len().saturating_sub(1) as u64,
        }))
    }
    pub(crate) fn install_primary(
        &self,
        session: &Session<'_>,
        prepared: PreparedPrimary,
        ops: &[WalOp],
    ) -> Result<(), EngineError> {
        let maps = self.primary.as_ref().expect("candidate representation");
        assert_eq!(
            prepared.transitions.len(),
            ops.len(),
            "prepared operation custody"
        );
        maps.graph
            .install_bounds(session, prepared.frontier, &prepared.graph_updates);
        for (mut t, op) in prepared.transitions.into_iter().zip(ops) {
            if let Some((id, value)) = t.node.take() {
                if matches!(&*value, RecordState::Tombstone(_))
                    && maps.deleted_nodes.remember_id(session, id)
                {
                    t.approximate += 32;
                }
                maps.max_node.fetch_max(id, Ordering::Relaxed);
                maps.node_ids
                    .fetch_add(usize::from(t.new_node_id), Ordering::Relaxed);
                // SAFETY: install_primary is entered under the existing sole
                // coordinator/replay custody; the prepared same-ID overlay
                // preserves every operation and its sequence. No reader writes.
                t.approximate += unsafe {
                    maps.nodes
                        .install(session, id, t.seq, value, t.new_node_id)?
                } - 32;
            }
            if let Some((id, value)) = t.edge.take() {
                if matches!(&*value, RecordState::Tombstone(_))
                    && maps.deleted_edges.remember_id(session, id)
                {
                    t.approximate += 32;
                }
                maps.max_edge.fetch_max(id, Ordering::Relaxed);
                maps.edge_ids
                    .fetch_add(usize::from(t.new_edge_id), Ordering::Relaxed);
                // SAFETY: same serialized generation install contract as nodes.
                t.approximate += unsafe {
                    maps.edges
                        .install(session, id, t.seq, value, t.new_edge_id)?
                } - 32;
            }
            #[cfg(all(test, feature = "production-concurrency"))]
            maps.pause_for_test(1)?;
            for (key, value) in t.keys {
                maps.keys.insert_unique(session, key, value);
            }
            for (key, value) in t.triples {
                maps.triples.insert_unique(session, key, value);
            }
            maps.estimated.fetch_add(t.approximate, Ordering::Relaxed);
            maps.graph.install(session, t.seq, t.graph, maps)?;
            maps.secondary
                .install(session, t.seq, t.secondary, Some(maps))?;
            self.update_candidate_diagnostics(
                maps,
                op,
                t.old_node_live,
                t.old_edge_live,
                t.new_node_id,
                t.new_edge_id,
            );
        }
        maps.graph.finish_install(prepared.graph_updates);
        Ok(())
    }
    fn update_candidate_diagnostics(
        &self,
        maps: &PrimaryMaps,
        op: &WalOp,
        old_node_live: bool,
        old_edge_live: bool,
        new_node_id: bool,
        new_edge_id: bool,
    ) {
        // These are generation-current inspection counters, not snapshot counts.
        // A non-new local ID without a live old opinion is a local tombstone,
        // including repeated operations in this batch. Older sources do not count.
        let (old_live, new_id, upsert, node) = match op {
            WalOp::UpsertNode(_) => (old_node_live, new_node_id, true, true),
            WalOp::DeleteNode { .. } => (old_node_live, new_node_id, false, true),
            WalOp::UpsertEdge(_) => (old_edge_live, new_edge_id, true, false),
            WalOp::DeleteEdge { .. } => (old_edge_live, new_edge_id, false, false),
            _ => return,
        };
        let mut state = self.state.write().unwrap();
        #[cfg(test)]
        maps.pause_for_test(2)
            .expect("diagnostic pause cannot fail");
        #[cfg(not(test))]
        let _ = maps;
        let state = &mut *state;
        let (live, deleted) = if node {
            (
                &mut state.live_node_count,
                &mut state.candidate_tombstone_counts.0,
            )
        } else {
            (
                &mut state.live_edge_count,
                &mut state.candidate_tombstone_counts.1,
            )
        };
        if upsert {
            if !old_live {
                *live = live.saturating_add(1);
                if !new_id {
                    *deleted = deleted.saturating_sub(1);
                }
            }
        } else {
            if old_live {
                *live = live.saturating_sub(1);
            }
            if old_live || new_id {
                *deleted = deleted.saturating_add(1);
            }
        }
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
#[derive(Default)]
struct PrimaryHooks {
    pause: Option<(
        u8,
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    fail_install: bool,
    fail_stage: Option<u8>,
}
#[cfg(all(test, feature = "production-concurrency"))]
impl PrimaryMaps {
    pub(super) fn estimated_size_full_for_test(&self) -> usize {
        let session = self.admission.try_acquire().unwrap();
        let nodes = self.nodes.fold_all_for_test(&session, 0, |sum, _, value| {
            sum + match &**value {
                RecordState::Live(node) => Memtable::estimate_node_record(node) + 32,
                RecordState::Tombstone(_) => 48,
            }
        });
        let edges = self.edges.fold_all_for_test(&session, 0, |sum, _, value| {
            sum + match &**value {
                RecordState::Live(edge) => Memtable::estimate_edge_record(edge) + 32,
                RecordState::Tombstone(_) => 48,
            }
        });
        let keys = self
            .keys
            .fold_all_for_test(&session, 0, |sum, key, _| sum + key.1.len() + 64);
        let triples = self
            .triples
            .fold_all_for_test(&session, 0, |sum, _, _| sum + 72);
        let deleted = self
            .deleted_nodes
            .fold_all_for_test(&session, 0, |sum, _, _| sum + 32)
            + self
                .deleted_edges
                .fold_all_for_test(&session, 0, |sum, _, _| sum + 32);
        nodes
            + edges
            + keys
            + triples
            + deleted
            + self.graph.estimated_full(&session)
            + self.nodes.extra_structure_inventory(&session)
            + self.edges.extra_structure_inventory(&session)
    }

    pub(super) fn pause_for_test(&self, stage: u8) -> Result<(), EngineError> {
        let (pause, fail) = {
            let mut hooks = self.hooks.lock().unwrap();
            let pause = if hooks.pause.as_ref().is_some_and(|p| p.0 == stage) {
                hooks.pause.take()
            } else {
                None
            };
            let fail = (stage == 1 && std::mem::take(&mut hooks.fail_install))
                || if hooks.fail_stage == Some(stage) {
                    hooks.fail_stage.take();
                    true
                } else {
                    false
                };
            (pause, fail)
        };
        if let Some((_, ready, release)) = pause {
            let _ = ready.send(());
            let _ = release.recv();
        }
        if fail {
            return Err(EngineError::InvalidOperation(
                "injected primary install error".into(),
            ));
        }
        Ok(())
    }
}
#[cfg(all(test, feature = "production-concurrency"))]
impl Memtable {
    pub(crate) fn primary_pause_for_test(
        &self,
        stage: u8,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        self.primary
            .as_ref()
            .expect("candidate map")
            .hooks
            .lock()
            .unwrap()
            .pause = Some((stage, ready_tx, release_rx));
        (ready_rx, release_tx)
    }
    pub(crate) fn fail_graph_install_for_test(&self, stage: u8) {
        self.primary
            .as_ref()
            .unwrap()
            .hooks
            .lock()
            .unwrap()
            .fail_stage = Some(stage);
    }
    pub(crate) fn fail_primary_install_for_test(&self) {
        self.primary
            .as_ref()
            .expect("candidate map")
            .hooks
            .lock()
            .unwrap()
            .fail_install = true;
    }
    pub(crate) fn primary_node_seeks_for_test(&self, reset: bool) -> usize {
        let m = self.primary.as_ref().unwrap();
        if reset {
            m.node_seeks.swap(0, Ordering::Relaxed)
        } else {
            m.node_seeks.load(Ordering::Relaxed)
        }
    }
    pub(crate) fn take_node_scan_groups_for_test(&self) -> usize {
        self.primary
            .as_ref()
            .unwrap()
            .nodes
            .take_scan_groups_for_test()
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
impl PrimaryMaps {
    pub(super) fn note_vector_copy_for_test(&self, row: &NodeVectorRow<'_>) {
        let bytes = [
            0,
            0,
            row.dense_vector.as_ref().map_or(0, |v| v.len() * 4),
            row.sparse_vector.as_ref().map_or(0, |v| v.len() * 8),
        ];
        for (counter, bytes) in self.vector_copy_bytes.iter().zip(bytes) {
            counter.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}
#[cfg(all(test, feature = "production-concurrency"))]
impl Memtable {
    pub(crate) fn take_vector_copy_bytes_for_test(&self) -> [usize; 4] {
        let m = self.primary.as_ref().unwrap();
        std::array::from_fn(|i| m.vector_copy_bytes[i].swap(0, Ordering::Relaxed))
    }
    pub(crate) fn take_node_sorted_walk_work_for_test(&self) -> [usize; 2] {
        self.primary
            .as_ref()
            .unwrap()
            .nodes
            .take_sorted_walk_work_for_test()
    }

    pub(crate) fn take_edge_seeks_for_test(&self) -> usize {
        self.primary
            .as_ref()
            .unwrap()
            .edge_seeks
            .swap(0, Ordering::Relaxed)
    }
    pub(crate) fn take_preparation_seeks_for_test(&self) -> [usize; 2] {
        let m = self.primary.as_ref().expect("candidate binding");
        std::array::from_fn(|i| m.preparation_seeks[i].swap(0, Ordering::Relaxed))
    }
    pub(crate) fn primary_distinct_ids_for_test(&self) -> [usize; 2] {
        let m = self.primary.as_ref().expect("candidate binding");
        [
            m.node_ids.load(Ordering::Relaxed),
            m.edge_ids.load(Ordering::Relaxed),
        ]
    }
    pub(crate) fn take_older_preparation_ids_for_test(&self) -> [usize; 4] {
        let m = self.primary.as_ref().unwrap();
        std::array::from_fn(|i| m.older_preparation_ids[i].swap(0, Ordering::Relaxed))
    }
}

#[cfg(feature = "production-concurrency")]
pub(super) fn copied_node_bytes(node: &NodeRecord) -> usize {
    // A logical copied-payload target, not physical allocation accounting.
    let mut bytes = std::mem::size_of::<NodeRecord>()
        + node.key.len()
        + node.dense_vector.as_ref().map_or(0, |v| v.len() * 4)
        + node.sparse_vector.as_ref().map_or(0, |v| v.len() * 8);
    bytes = bytes.saturating_add(copied_properties_bytes(&node.props));
    bytes
}
#[cfg(feature = "production-concurrency")]
fn copied_properties_bytes(props: &BTreeMap<String, PropValue>) -> usize {
    let mut bytes = 0usize;
    let mut pending = Vec::new();
    for (key, value) in props {
        bytes = bytes.saturating_add(key.len());
        pending.push(value);
    }
    while let Some(value) = pending.pop() {
        bytes = bytes.saturating_add(std::mem::size_of::<PropValue>());
        match value {
            PropValue::String(v) => bytes = bytes.saturating_add(v.len()),
            PropValue::Bytes(v) => bytes = bytes.saturating_add(v.len()),
            PropValue::Array(v) => pending.extend(v),
            PropValue::Map(v) => {
                for (k, v) in v {
                    bytes = bytes.saturating_add(k.len());
                    pending.push(v);
                }
            }
            _ => {}
        }
    }
    bytes
}
#[cfg(feature = "production-concurrency")]
type PrimaryNodeRefill = (Vec<NodeVectorRow<'static>>, Option<u64>, bool);
#[cfg(feature = "production-concurrency")]
impl PrimaryMaps {
    pub(super) fn node_refill(
        &self,
        seq: u64,
        bounds: (Option<u64>, u64),
        vectors: crate::row_projection::VectorSelection,
    ) -> Result<PrimaryNodeRefill, EngineError> {
        let session = self.admission.try_acquire().map_err(participant_error)?;
        let mut records = Vec::new();
        let mut copied = 0usize;
        let (last, done) = self
            .nodes
            .walk_versions_at(&session, seq, bounds, 4096, |_, value| {
                if let RecordState::Live(node) = &**value {
                    let bytes = NodeVectorRow::copied_bytes(node, vectors);
                    if !records.is_empty() && copied.saturating_add(bytes) > 1024 * 1024 {
                        return false;
                    }
                    let row = NodeVectorRow::borrowed(node, vectors).into_owned();
                    #[cfg(test)]
                    self.note_vector_copy_for_test(&row);
                    records.push(row);
                    copied = copied.saturating_add(bytes);
                }
                true
            });
        Ok((records, last, done))
    }

    pub(super) fn edge_metadata_refill(
        &self,
        seq: u64,
        bounds: (Option<u64>, u64),
    ) -> Result<(Vec<EdgeMetadataCandidate>, Option<u64>, bool), EngineError> {
        let session = self.admission.try_acquire().map_err(participant_error)?;
        let mut records = Vec::new();
        let (last, done) = self
            .edges
            .walk_versions_at(&session, seq, bounds, 4096, |_, value| {
                if let RecordState::Live(edge) = &**value {
                    records.push(EdgeMetadataCandidate::from_edge(edge));
                    #[cfg(test)]
                    note_query_scan_work(|work| work.edge_transport_records += 1);
                }
                true
            });
        Ok((records, last, done))
    }

    pub(super) fn node_ids_refill(
        &self,
        seq: u64,
        bounds: (Option<u64>, u64),
    ) -> Result<(Vec<u64>, Option<u64>, bool), EngineError> {
        let session = self.admission.try_acquire().map_err(participant_error)?;
        let mut ids = Vec::with_capacity(128);
        let (last, done) = self
            .nodes
            .walk_versions_at(&session, seq, bounds, 4096, |id, value| {
                if ids.len() == 128 {
                    return false;
                }
                if matches!(&**value, RecordState::Live(_)) {
                    ids.push(id);
                }
                true
            });
        #[cfg(test)]
        note_query_scan_work(|work| {
            work.node_id_refills += 1;
            work.node_id_copies += ids.len();
        });
        Ok((ids, last, done))
    }
}

#[cfg(feature = "production-concurrency")]
fn prepare_old_node<'a>(
    m: &'a PrimaryMaps,
    session: &'a Session<'_>,
    ops: &'a [WalOp],
    previous: Option<usize>,
    id: u64,
    existing_limit: Option<u64>,
) -> (Option<&'a NodeRecord>, bool) {
    if let Some(index) = previous {
        return (
            match &ops[index] {
                WalOp::UpsertNode(n) => Some(n),
                WalOp::DeleteNode { .. } => None,
                _ => unreachable!("node transition index"),
            },
            false,
        );
    }
    // Allocation counters do not advance for deletes of unallocated IDs. The
    // generation maximum includes tombstones too, so only IDs above it prove
    // absence. Preparation is coordinator-serialized; same-batch IDs use the
    // overlay above.
    if existing_limit.is_some_and(|limit| limit != u64::MAX && id >= limit)
        && id > m.max_node.load(Ordering::Relaxed)
    {
        return (None, true);
    }
    #[cfg(test)]
    m.preparation_seeks[0].fetch_add(1, Ordering::Relaxed);
    let opinion = m.node(session, id, u64::MAX);
    (
        match opinion {
            Some(RecordState::Live(n)) => Some(n),
            _ => None,
        },
        opinion.is_none(),
    )
}
#[cfg(feature = "production-concurrency")]
fn prepare_old_edge<'a>(
    m: &'a PrimaryMaps,
    session: &'a Session<'_>,
    ops: &'a [WalOp],
    previous: Option<usize>,
    id: u64,
    existing_limit: Option<u64>,
) -> (Option<&'a EdgeRecord>, bool) {
    if let Some(index) = previous {
        return (
            match &ops[index] {
                WalOp::UpsertEdge(e) => Some(e),
                WalOp::DeleteEdge { .. } => None,
                _ => unreachable!("edge transition index"),
            },
            false,
        );
    }
    // A prior delete can occupy a future allocation ID in this generation.
    if existing_limit.is_some_and(|limit| limit != u64::MAX && id >= limit)
        && id > m.max_edge.load(Ordering::Relaxed)
    {
        return (None, true);
    }
    #[cfg(test)]
    m.preparation_seeks[1].fetch_add(1, Ordering::Relaxed);
    let opinion = m.edge(session, id, u64::MAX);
    (
        match opinion {
            Some(RecordState::Live(e)) => Some(e),
            _ => None,
        },
        opinion.is_none(),
    )
}

#[cfg(test)]
impl Memtable {
    /// Explicit private fixture construction, never a production mutation route.
    /// Large query fixtures retain their real candidate representation without
    /// pretending to issue public WAL writes or changing their query assertions.
    pub(crate) fn apply_op_for_test(&self, op: &WalOp, seq: u64) -> Result<(), EngineError> {
        #[cfg(feature = "production-concurrency")]
        if let Some(prepared) = self.prepare_primary(std::slice::from_ref(op), seq, None, 0, 0)? {
            let handle = self.participant_handle().expect("candidate fixture");
            let session = handle.try_acquire().map_err(participant_error)?;
            return self.install_primary(&session, prepared, std::slice::from_ref(op));
        }
        self.apply_op(op, seq);
        Ok(())
    }
}

// Sealed storage export copies one opinion per ID. No dependency session escapes
// a refill, including negative-only yields and an indivisible oversized record.
#[cfg(feature = "production-concurrency")]
pub(crate) struct ExportChunk<T: Drain> {
    pub records: Owned<Vec<T>>,
    pub tombstones: Vec<(u64, TombstoneEntry)>,
    pub last: Option<u64>,
    pub exhausted: bool,
    pub copied_bytes: usize,
}
#[cfg(feature = "production-concurrency")]
macro_rules! storage_export {
    ($method:ident, $map:ident, $record:ty, $bytes:expr) => {
        impl Memtable {
            pub(crate) fn $method(
                &self,
                seq: u64,
                bounds: (Option<u64>, u64),
            ) -> Result<ExportChunk<$record>, EngineError> {
                let maps = self.primary.as_ref().expect("candidate export");
                let session = maps.admission.try_acquire().map_err(participant_error)?;
                let mut records = Owned::new(Vec::new());
                let mut tombstones = Vec::new();
                let mut copied = 0usize;
                let (last, exhausted) =
                    maps.$map
                        .walk_versions_at(&session, seq, bounds, 4096, |id, value| {
                            let bytes = match &**value {
                                RecordState::Live(row) => ($bytes)(row),
                                RecordState::Tombstone(_) => {
                                    std::mem::size_of::<(u64, TombstoneEntry)>()
                                }
                            };
                            if (!records.is_empty() || !tombstones.is_empty())
                                && copied.saturating_add(bytes) > 1024 * 1024
                            {
                                return false;
                            }
                            match &**value {
                                RecordState::Live(row) => records.push(row.clone()),
                                RecordState::Tombstone(t) => tombstones.push((id, *t)),
                            }
                            copied = copied.saturating_add(bytes);
                            true
                        });
                Ok(ExportChunk {
                    records,
                    tombstones,
                    last,
                    exhausted,
                    copied_bytes: copied,
                })
            }
        }
    };
}
#[cfg(feature = "production-concurrency")]
storage_export!(export_node_chunk, nodes, NodeRecord, copied_node_bytes);
#[cfg(feature = "production-concurrency")]
storage_export!(export_edge_chunk, edges, EdgeRecord, |edge: &EdgeRecord| {
    // Reuse the checked iterative property-size walk; no payload clone.
    copied_properties_bytes(&edge.props).saturating_add(std::mem::size_of::<EdgeRecord>())
});
