#[derive(Clone)]
pub(crate) struct TxnCommitRequest {
    snapshot: Arc<ReadView>,
    snapshot_seq: u64,
    entries: Vec<StagedTxnIntent>,
    record_replacements: Vec<TxnRecordReplacement>,
    gql_return_read_set: TxnReturnReadSet,
    graph_op_budget: Option<TxnGraphOpBudget>,
}

#[derive(Clone)]
pub(crate) struct StagedTxnIntent {
    intent: TxnIntent,
    produced_node: Option<TxnLocalRef>,
    produced_edge: Option<TxnLocalRef>,
}

#[derive(Clone)]
pub(crate) enum TxnRecordReplacement {
    Node(TxnNodeRecordReplacement),
    Edge(TxnEdgeRecordReplacement),
}

#[derive(Clone)]
pub(crate) struct TxnNodeRecordReplacement {
    pub(crate) id: u64,
    pub(crate) labels: Vec<String>,
    pub(crate) key: String,
    pub(crate) props: BTreeMap<String, PropValue>,
    pub(crate) created_at: i64,
    pub(crate) weight: f32,
    pub(crate) dense_vector: Option<DenseVector>,
    pub(crate) sparse_vector: Option<SparseVector>,
}

#[derive(Clone)]
pub(crate) struct TxnEdgeRecordReplacement {
    pub(crate) id: u64,
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) label: String,
    pub(crate) props: BTreeMap<String, PropValue>,
    pub(crate) created_at: i64,
    pub(crate) weight: f32,
    pub(crate) valid_from: i64,
    pub(crate) valid_to: i64,
}

#[derive(Clone, Default)]
pub(crate) struct TxnReturnReadSet {
    pub(crate) node_ids: BTreeSet<u64>,
    pub(crate) edge_ids: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TxnMergeLocalNodeRef(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TxnMergeLocalEdgeRef(pub(crate) usize);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TxnMergeEndpointKey {
    Id(u64),
    Local(TxnLocalRef),
    Key(String, String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TxnKeyedNodeMergeRowOutcome {
    Existing(u64),
    MatchedLocal(TxnMergeLocalNodeRef),
    Create(TxnMergeLocalNodeRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TxnKeyedNodeMergeBatchOutcome {
    pub(crate) rows: Vec<TxnKeyedNodeMergeRowOutcome>,
    pub(crate) existing_ids: BTreeSet<u64>,
    pub(crate) snapshot_lookup_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TxnUniqueEdgeMergeInput {
    pub(crate) from: TxnNodeRef,
    pub(crate) to: TxnNodeRef,
    pub(crate) label: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TxnUniqueEdgeMergeRowOutcome {
    SkippedNull,
    Existing(u64),
    MatchedLocal(TxnMergeLocalEdgeRef),
    Create {
        local: TxnMergeLocalEdgeRef,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TxnUniqueEdgeMergeBatchOutcome {
    pub(crate) rows: Vec<TxnUniqueEdgeMergeRowOutcome>,
    pub(crate) existing_ids: BTreeSet<u64>,
    pub(crate) snapshot_lookup_count: usize,
    pub(crate) missing_committed_triples: BTreeSet<(u64, u64, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TxnMergeNodeTarget {
    Existing(u64),
    Created(TxnMergeLocalNodeRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TxnMergeEdgeTarget {
    Existing(u64),
    Created(TxnMergeLocalEdgeRef),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TxnMergeOverlay {
    node_keys: BTreeMap<(String, String), TxnMergeNodeTarget>,
    edge_triples: BTreeMap<(TxnMergeEndpointKey, TxnMergeEndpointKey, String), TxnMergeEdgeTarget>,
    next_node: usize,
    next_edge: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct TxnGraphOpBudget {
    scope: &'static str,
    name: &'static str,
    max_ops: usize,
}

#[derive(Clone, Copy)]
struct TxnGraphOpCounter {
    budget: Option<TxnGraphOpBudget>,
    ops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TxnEndpointKey {
    Id(u64),
    Local(TxnLocalRef),
    Key(String, String),
}

#[derive(Debug, Clone)]
enum NodeOverlayOpinion {
    Live(TxnNodeView),
    Deleted(Option<TxnNodeView>),
    RemovedLabel,
}

#[derive(Debug, Clone)]
enum EdgeOverlayOpinion {
    Live(TxnEdgeView),
    Deleted(Option<TxnEdgeView>),
}

#[derive(Clone, Default)]
struct TxnOverlay {
    edge_uniqueness: bool,
    node_aliases: HashSet<String>,
    edge_aliases: HashSet<String>,
    nodes_by_local: HashMap<TxnLocalRef, NodeOverlayOpinion>,
    node_key_locals: HashMap<(String, String), Vec<TxnLocalRef>>,
    nodes_by_id: NodeIdMap<NodeOverlayOpinion>,
    nodes_by_key: HashMap<(String, String), NodeOverlayOpinion>,
    deleted_node_ids_seen: NodeIdSet,
    edges_by_local: HashMap<TxnLocalRef, EdgeOverlayOpinion>,
    edge_triple_locals: HashMap<(TxnEndpointKey, TxnEndpointKey, String), Vec<TxnLocalRef>>,
    edges_by_id: NodeIdMap<EdgeOverlayOpinion>,
    edges_by_triple: HashMap<(TxnEndpointKey, TxnEndpointKey, String), EdgeOverlayOpinion>,
}

/// Explicit write transaction handle.
///
/// A transaction stages logical graph intents locally. Staging, rollback, and bounded
/// reads do not append WAL records or mutate live engine state.
pub struct WriteTxn {
    runtime: Arc<DbRuntime>,
    snapshot: Arc<ReadView>,
    snapshot_seq: u64,
    entries: Vec<StagedTxnIntent>,
    record_replacements: Vec<TxnRecordReplacement>,
    gql_return_read_set: TxnReturnReadSet,
    graph_op_budget: Option<TxnGraphOpBudget>,
    overlay: TxnOverlay,
    edge_uniqueness: bool,
    closed: bool,
    next_slot: u32,
}

impl DatabaseEngine {
    pub fn begin_write_txn(&self) -> Result<WriteTxn, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        Ok(WriteTxn {
            runtime: Arc::clone(&self.runtime),
            snapshot: Arc::clone(&published.view),
            snapshot_seq: published.view.snapshot_seq,
            entries: Vec::new(),
            record_replacements: Vec::new(),
            gql_return_read_set: TxnReturnReadSet::default(),
            graph_op_budget: None,
            overlay: TxnOverlay::new(published.edge_uniqueness),
            edge_uniqueness: published.edge_uniqueness,
            closed: false,
            next_slot: 0,
        })
    }
}

impl ReadView {
    fn txn_delete_incident_edge_ids_limited(
        &self,
        node_ids: &[u64],
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        self.sources()
            .edge_ids_by_endpoints_limited(node_ids, Direction::Both, None, limit)
    }
}

impl TxnGraphOpCounter {
    fn new(budget: Option<TxnGraphOpBudget>) -> Self {
        Self { budget, ops: 0 }
    }

    fn reserve(&mut self, count: usize) -> Result<(), EngineError> {
        if count == 0 {
            return Ok(());
        }
        let next = self.ops.saturating_add(count);
        if let Some(budget) = self.budget {
            if next > budget.max_ops {
                return Err(txn_graph_op_cap_error(budget, next));
            }
        }
        self.ops = next;
        Ok(())
    }

    fn limited_scan_len(&self, already_counted_matches: usize) -> usize {
        match self.budget {
            Some(budget) => budget
                .max_ops
                .saturating_sub(self.ops)
                .saturating_add(already_counted_matches)
                .saturating_add(1),
            None => usize::MAX,
        }
    }

    fn reject_if_limited_scan_filled(
        &self,
        returned: usize,
        limit: usize,
    ) -> Result<(), EngineError> {
        let Some(budget) = self.budget else {
            return Ok(());
        };
        if limit != usize::MAX && returned >= limit {
            return Err(txn_graph_op_cap_error(
                budget,
                budget.max_ops.saturating_add(1),
            ));
        }
        Ok(())
    }
}

fn txn_graph_op_cap_error(budget: TxnGraphOpBudget, actual: usize) -> EngineError {
    EngineError::InvalidOperation(format!(
        "{} {} exceeded: attempted {}, cap {}",
        budget.scope, budget.name, actual, budget.max_ops
    ))
}

impl TxnMergeOverlay {
    fn allocate_node(&mut self) -> TxnMergeLocalNodeRef {
        let local = TxnMergeLocalNodeRef(self.next_node);
        self.next_node = self.next_node.saturating_add(1);
        local
    }

    fn allocate_edge(&mut self) -> TxnMergeLocalEdgeRef {
        let local = TxnMergeLocalEdgeRef(self.next_edge);
        self.next_edge = self.next_edge.saturating_add(1);
        local
    }
}

fn txn_merge_endpoint_key(node: &TxnNodeRef) -> TxnMergeEndpointKey {
    match node {
        TxnNodeRef::Id(id) => TxnMergeEndpointKey::Id(*id),
        TxnNodeRef::Key { label, key } => TxnMergeEndpointKey::Key(label.clone(), key.clone()),
        TxnNodeRef::Local(local) => TxnMergeEndpointKey::Local(local.clone()),
    }
}

fn txn_committed_edge_merge_triple(
    from: &TxnNodeRef,
    to: &TxnNodeRef,
    label: &str,
) -> Option<(u64, u64, String)> {
    match (from, to) {
        (TxnNodeRef::Id(from), TxnNodeRef::Id(to)) => Some((*from, *to, label.to_string())),
        _ => None,
    }
}

impl WriteTxn {
    #[allow(dead_code)]
    pub(crate) fn gql_snapshot(&self) -> Result<Arc<ReadView>, EngineError> {
        self.ensure_open()?;
        Ok(Arc::clone(&self.snapshot))
    }

    #[allow(dead_code)]
    pub(crate) fn gql_snapshot_seq(&self) -> Result<u64, EngineError> {
        self.ensure_open()?;
        Ok(self.snapshot_seq)
    }

    pub(crate) fn gql_edge_uniqueness(&self) -> Result<bool, EngineError> {
        self.ensure_open()?;
        Ok(self.edge_uniqueness)
    }

    pub(crate) fn plan_keyed_node_merge_batch(
        &self,
        overlay: &mut TxnMergeOverlay,
        keys: &[(String, String)],
    ) -> Result<TxnKeyedNodeMergeBatchOutcome, EngineError> {
        self.ensure_open()?;
        let lookup_keys = keys
            .iter()
            .filter(|key| !overlay.node_keys.contains_key(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        let snapshot_matches = self.gql_lookup_node_merge_keys(&lookup_keys)?;
        let existing_ids = snapshot_matches.values().copied().collect::<BTreeSet<_>>();
        let mut existing_ids = existing_ids;
        let mut rows = Vec::with_capacity(keys.len());

        for merge_key in keys {
            if let Some(target) = overlay.node_keys.get(merge_key).cloned() {
                rows.push(match target {
                    TxnMergeNodeTarget::Existing(id) => {
                        existing_ids.insert(id);
                        TxnKeyedNodeMergeRowOutcome::Existing(id)
                    }
                    TxnMergeNodeTarget::Created(local) => {
                        TxnKeyedNodeMergeRowOutcome::MatchedLocal(local)
                    }
                });
                continue;
            }

            if let Some(&id) = snapshot_matches.get(merge_key) {
                overlay
                    .node_keys
                    .insert(merge_key.clone(), TxnMergeNodeTarget::Existing(id));
                rows.push(TxnKeyedNodeMergeRowOutcome::Existing(id));
                continue;
            }

            let local = overlay.allocate_node();
            overlay
                .node_keys
                .insert(merge_key.clone(), TxnMergeNodeTarget::Created(local));
            rows.push(TxnKeyedNodeMergeRowOutcome::Create(local));
        }

        Ok(TxnKeyedNodeMergeBatchOutcome {
            rows,
            existing_ids,
            snapshot_lookup_count: lookup_keys.len(),
        })
    }

    pub(crate) fn plan_unique_edge_merge_batch(
        &self,
        overlay: &mut TxnMergeOverlay,
        inputs: &[Option<TxnUniqueEdgeMergeInput>],
    ) -> Result<TxnUniqueEdgeMergeBatchOutcome, EngineError> {
        self.ensure_open()?;
        if !self.edge_uniqueness {
            return Err(EngineError::InvalidOperation(
                "GQL relationship MERGE requires edge_uniqueness=true".to_string(),
            ));
        }
        for input in inputs.iter().flatten() {
            if matches!(&input.from, TxnNodeRef::Key { .. })
                || matches!(&input.to, TxnNodeRef::Key { .. })
            {
                return Err(EngineError::InvalidOperation(
                    "transaction relationship MERGE planner requires resolved node IDs or local refs"
                        .to_string(),
                ));
            }
        }

        let committed_triples = inputs
            .iter()
            .filter_map(|input| {
                let input = input.as_ref()?;
                let merge_key = (
                    txn_merge_endpoint_key(&input.from),
                    txn_merge_endpoint_key(&input.to),
                    input.label.clone(),
                );
                if overlay.edge_triples.contains_key(&merge_key) {
                    return None;
                }
                txn_committed_edge_merge_triple(&input.from, &input.to, &input.label)
            })
            .collect::<BTreeSet<_>>();
        let snapshot_matches = self.gql_lookup_edge_merge_triples(&committed_triples)?;
        let existing_ids = snapshot_matches.values().copied().collect::<BTreeSet<_>>();
        let mut existing_ids = existing_ids;
        let mut missing_committed_triples = BTreeSet::new();
        let mut rows = Vec::with_capacity(inputs.len());

        for input in inputs {
            let Some(input) = input else {
                rows.push(TxnUniqueEdgeMergeRowOutcome::SkippedNull);
                continue;
            };
            let merge_key = (
                txn_merge_endpoint_key(&input.from),
                txn_merge_endpoint_key(&input.to),
                input.label.clone(),
            );

            if let Some(target) = overlay.edge_triples.get(&merge_key).cloned() {
                rows.push(match target {
                    TxnMergeEdgeTarget::Existing(id) => {
                        existing_ids.insert(id);
                        TxnUniqueEdgeMergeRowOutcome::Existing(id)
                    }
                    TxnMergeEdgeTarget::Created(local) => {
                        TxnUniqueEdgeMergeRowOutcome::MatchedLocal(local)
                    }
                });
                continue;
            }

            if let Some(triple) =
                txn_committed_edge_merge_triple(&input.from, &input.to, &input.label)
            {
                if let Some(&id) = snapshot_matches.get(&triple) {
                    overlay
                        .edge_triples
                        .insert(merge_key, TxnMergeEdgeTarget::Existing(id));
                    rows.push(TxnUniqueEdgeMergeRowOutcome::Existing(id));
                    continue;
                }
                missing_committed_triples.insert(triple);
            }

            let local = overlay.allocate_edge();
            overlay
                .edge_triples
                .insert(merge_key, TxnMergeEdgeTarget::Created(local));
            rows.push(TxnUniqueEdgeMergeRowOutcome::Create {
                local,
                from: input.from.clone(),
                to: input.to.clone(),
                label: input.label.clone(),
            });
        }

        Ok(TxnUniqueEdgeMergeBatchOutcome {
            rows,
            existing_ids,
            snapshot_lookup_count: committed_triples.len(),
            missing_committed_triples,
        })
    }

    pub(crate) fn gql_first_existing_node_key(
        &self,
        keys: &BTreeSet<(String, String)>,
    ) -> Result<Option<(String, String)>, EngineError> {
        self.ensure_open()?;
        let mut resolved = Vec::with_capacity(keys.len());
        for (label, key) in keys {
            let Some(label_id) = self.snapshot.label_catalog.resolve_node_label_for_read(label)?
            else {
                continue;
            };
            resolved.push((label.clone(), key.clone(), label_id));
        }
        if resolved.is_empty() {
            return Ok(None);
        }
        let key_refs: Vec<(u32, &str)> = resolved
            .iter()
            .map(|(_, key, label_id)| (*label_id, key.as_str()))
            .collect();
        let nodes = self.snapshot.get_nodes_by_label_keys_raw(&key_refs)?;
        for ((label, key, _), node) in resolved.into_iter().zip(nodes) {
            if node.is_some() {
                return Ok(Some((label, key)));
            }
        }
        Ok(None)
    }

    pub(crate) fn gql_lookup_node_merge_keys(
        &self,
        keys: &BTreeSet<(String, String)>,
    ) -> Result<BTreeMap<(String, String), u64>, EngineError> {
        self.ensure_open()?;
        let mut resolved = Vec::with_capacity(keys.len());
        for (label, key) in keys {
            let Some(label_id) = self.snapshot.label_catalog.resolve_node_label_for_read(label)?
            else {
                continue;
            };
            resolved.push((label.clone(), key.clone(), label_id));
        }
        if resolved.is_empty() {
            return Ok(BTreeMap::new());
        }
        let key_refs: Vec<(u32, &str)> = resolved
            .iter()
            .map(|(_, key, label_id)| (*label_id, key.as_str()))
            .collect();
        let nodes = self.snapshot.get_nodes_by_label_keys_raw(&key_refs)?;
        let mut out = BTreeMap::new();
        for ((label, key, _), node) in resolved.into_iter().zip(nodes) {
            if let Some(node) = node {
                out.insert((label, key), node.id);
            }
        }
        Ok(out)
    }

    pub(crate) fn gql_first_existing_edge_triple(
        &self,
        triples: &BTreeSet<(u64, u64, String)>,
    ) -> Result<Option<(u64, u64, String)>, EngineError> {
        self.ensure_open()?;
        let mut resolved = Vec::with_capacity(triples.len());
        for (from, to, label) in triples {
            let Some(label_id) = self.snapshot.label_catalog.resolve_edge_label_for_read(label)?
            else {
                continue;
            };
            resolved.push((*from, *to, label.clone(), label_id));
        }
        if resolved.is_empty() {
            return Ok(None);
        }
        let triple_refs: Vec<(u64, u64, u32)> = resolved
            .iter()
            .map(|(from, to, _, label_id)| (*from, *to, *label_id))
            .collect();
        let edges = self.snapshot.get_edges_by_triples_raw(&triple_refs)?;
        for ((from, to, label, _), edge) in resolved.into_iter().zip(edges) {
            if edge.is_some() {
                return Ok(Some((from, to, label)));
            }
        }
        Ok(None)
    }

    pub(crate) fn gql_lookup_edge_merge_triples(
        &self,
        triples: &BTreeSet<(u64, u64, String)>,
    ) -> Result<BTreeMap<(u64, u64, String), u64>, EngineError> {
        self.ensure_open()?;
        let mut resolved = Vec::with_capacity(triples.len());
        for (from, to, label) in triples {
            let Some(label_id) = self.snapshot.label_catalog.resolve_edge_label_for_read(label)?
            else {
                continue;
            };
            resolved.push((*from, *to, label.clone(), label_id));
        }
        if resolved.is_empty() {
            return Ok(BTreeMap::new());
        }
        let triple_refs: Vec<(u64, u64, u32)> = resolved
            .iter()
            .map(|(from, to, _, label_id)| (*from, *to, *label_id))
            .collect();
        let edges = self.snapshot.get_edges_by_triples_raw(&triple_refs)?;
        let mut out = BTreeMap::new();
        for ((from, to, label, _), edge) in resolved.into_iter().zip(edges) {
            if let Some(edge) = edge {
                out.insert((from, to, label), edge.id);
            }
        }
        Ok(out)
    }

    pub fn upsert_node<L>(
        &mut self,
        labels: L,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<TxnNodeRef, EngineError>
    where
        L: IntoNodeLabels,
    {
        let local = self.next_slot_ref()?;
        let intent = TxnIntent::UpsertNode {
            alias: None,
            labels: labels.into_node_labels(),
            key: key.to_string(),
            options,
        };
        self.append_entry(intent, Some(local.clone()), None)?;
        self.advance_next_slot()?;
        Ok(TxnNodeRef::Local(local))
    }

    pub fn upsert_node_as<L>(
        &mut self,
        alias: &str,
        labels: L,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<TxnNodeRef, EngineError>
    where
        L: IntoNodeLabels,
    {
        let local = TxnLocalRef::Alias(alias.to_string());
        let intent = TxnIntent::UpsertNode {
            alias: Some(alias.to_string()),
            labels: labels.into_node_labels(),
            key: key.to_string(),
            options,
        };
        self.append_entry(intent, Some(local.clone()), None)?;
        Ok(TxnNodeRef::Local(local))
    }

    pub fn add_node_label(&mut self, target: TxnNodeRef, label: &str) -> Result<bool, EngineError> {
        self.ensure_open()?;
        validate_label_token_name(label)?;
        let Some(view) = self.get_node(target)? else {
            return Err(EngineError::InvalidOperation(
                "transaction node target does not exist".to_string(),
            ));
        };
        if view.labels.iter().any(|existing| existing == label) {
            return Ok(false);
        }
        let mut labels = view.labels.clone();
        labels.push(label.to_string());
        let intent = TxnIntent::UpsertNode {
            alias: None,
            labels,
            key: view.key.clone(),
            options: UpsertNodeOptions {
                props: view.props,
                weight: view.weight,
                dense_vector: view.dense_vector,
                sparse_vector: view.sparse_vector,
            },
        };
        self.append_entry(intent, view.local.clone(), None)?;
        Ok(true)
    }

    pub fn remove_node_label(
        &mut self,
        target: TxnNodeRef,
        label: &str,
    ) -> Result<bool, EngineError> {
        self.ensure_open()?;
        validate_label_token_name(label)?;
        let Some(view) = self.get_node(target)? else {
            return Err(EngineError::InvalidOperation(
                "transaction node target does not exist".to_string(),
            ));
        };
        if !view.labels.iter().any(|existing| existing == label) {
            return Ok(false);
        }
        if view.labels.len() == 1 {
            return Err(EngineError::InvalidOperation(
                "cannot remove the last node label".to_string(),
            ));
        }
        let labels = view
            .labels
            .iter()
            .filter(|existing| existing.as_str() != label)
            .cloned()
            .collect();
        let intent = TxnIntent::UpsertNode {
            alias: None,
            labels,
            key: view.key.clone(),
            options: UpsertNodeOptions {
                props: view.props,
                weight: view.weight,
                dense_vector: view.dense_vector,
                sparse_vector: view.sparse_vector,
            },
        };
        self.append_entry(intent, view.local.clone(), None)?;
        Ok(true)
    }

    pub fn upsert_edge(
        &mut self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: &str,
        options: UpsertEdgeOptions,
    ) -> Result<TxnEdgeRef, EngineError> {
        let local = self.next_slot_ref()?;
        let intent = TxnIntent::UpsertEdge {
            alias: None,
            from,
            to,
            label: label.to_string(),
            options,
        };
        self.append_entry(intent, None, Some(local.clone()))?;
        self.advance_next_slot()?;
        Ok(TxnEdgeRef::Local(local))
    }

    pub fn upsert_edge_as(
        &mut self,
        alias: &str,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: &str,
        options: UpsertEdgeOptions,
    ) -> Result<TxnEdgeRef, EngineError> {
        let local = TxnLocalRef::Alias(alias.to_string());
        let intent = TxnIntent::UpsertEdge {
            alias: Some(alias.to_string()),
            from,
            to,
            label: label.to_string(),
            options,
        };
        self.append_entry(intent, None, Some(local.clone()))?;
        Ok(TxnEdgeRef::Local(local))
    }

    pub fn delete_node(&mut self, target: TxnNodeRef) -> Result<(), EngineError> {
        self.append_entry(TxnIntent::DeleteNode { target }, None, None)
    }

    pub fn delete_edge(&mut self, target: TxnEdgeRef) -> Result<(), EngineError> {
        self.append_entry(TxnIntent::DeleteEdge { target }, None, None)
    }

    pub fn invalidate_edge(
        &mut self,
        target: TxnEdgeRef,
        valid_to: i64,
    ) -> Result<(), EngineError> {
        self.append_entry(TxnIntent::InvalidateEdge { target, valid_to }, None, None)
    }

    pub fn stage_intents(&mut self, intents: Vec<TxnIntent>) -> Result<(), EngineError> {
        self.ensure_open()?;
        let original_next_slot = self.next_slot;
        let mut next_slot = self.next_slot;
        let mut entries = Vec::with_capacity(intents.len());
        for intent in intents {
            let produced_node = match &intent {
                TxnIntent::UpsertNode {
                    alias: Some(alias), ..
                } => Some(TxnLocalRef::Alias(alias.clone())),
                TxnIntent::UpsertNode { alias: None, .. } => {
                    let slot = next_slot;
                    let Some(next) = next_slot.checked_add(1) else {
                        self.next_slot = original_next_slot;
                        self.rebuild_overlay_from_entries()?;
                        return Err(EngineError::InvalidOperation(
                            "transaction local slots exhausted".into(),
                        ));
                    };
                    next_slot = next;
                    Some(TxnLocalRef::Slot(slot))
                }
                _ => None,
            };
            let produced_edge = match &intent {
                TxnIntent::UpsertEdge {
                    alias: Some(alias), ..
                } => Some(TxnLocalRef::Alias(alias.clone())),
                TxnIntent::UpsertEdge { alias: None, .. } => {
                    let slot = next_slot;
                    let Some(next) = next_slot.checked_add(1) else {
                        self.next_slot = original_next_slot;
                        self.rebuild_overlay_from_entries()?;
                        return Err(EngineError::InvalidOperation(
                            "transaction local slots exhausted".into(),
                        ));
                    };
                    next_slot = next;
                    Some(TxnLocalRef::Slot(slot))
                }
                _ => None,
            };
            if let Err(err) =
                self.overlay
                    .apply(&self.snapshot, &intent, produced_node.clone(), produced_edge.clone())
            {
                self.next_slot = original_next_slot;
                self.rebuild_overlay_from_entries()?;
                return Err(err);
            }
            entries.push(StagedTxnIntent {
                intent,
                produced_node,
                produced_edge,
            });
        }
        self.next_slot = next_slot;
        self.entries.extend(entries);
        Ok(())
    }

    pub(crate) fn stage_record_replacements(
        &mut self,
        replacements: Vec<TxnRecordReplacement>,
    ) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.record_replacements.extend(replacements);
        Ok(())
    }

    pub(crate) fn gql_validate_return_read_set(
        &mut self,
        read_set: TxnReturnReadSet,
    ) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.gql_return_read_set.node_ids.extend(read_set.node_ids);
        self.gql_return_read_set.edge_ids.extend(read_set.edge_ids);
        Ok(())
    }

    pub(crate) fn gql_apply_mutation_op_budget(
        &mut self,
        max_ops: usize,
    ) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.graph_op_budget = Some(TxnGraphOpBudget {
            scope: "GQL mutation",
            name: "max_mutation_ops",
            max_ops,
        });
        Ok(())
    }

    pub fn get_node(&self, target: TxnNodeRef) -> Result<Option<TxnNodeView>, EngineError> {
        self.ensure_open()?;
        self.overlay.get_node(&self.snapshot, &target)
    }

    pub fn get_edge(&self, target: TxnEdgeRef) -> Result<Option<TxnEdgeView>, EngineError> {
        self.ensure_open()?;
        self.overlay.get_edge(&self.snapshot, &target)
    }

    pub fn get_node_by_key(
        &self,
        label: &str,
        key: &str,
    ) -> Result<Option<TxnNodeView>, EngineError> {
        self.ensure_open()?;
        self.overlay
            .get_node(&self.snapshot, &TxnNodeRef::Key {
                label: label.to_string(),
                key: key.to_string(),
            })
    }

    pub fn get_edge_by_triple(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: &str,
    ) -> Result<Option<TxnEdgeView>, EngineError> {
        self.ensure_open()?;
        self.overlay.get_edge(
            &self.snapshot,
            &TxnEdgeRef::Triple {
                from,
                to,
                label: label.to_string(),
            },
        )
    }

    pub fn commit(&mut self) -> Result<TxnCommitResult, EngineError> {
        self.ensure_open()?;
        self.closed = true;
        let request = TxnCommitRequest {
            snapshot: Arc::clone(&self.snapshot),
            snapshot_seq: self.snapshot_seq,
            entries: std::mem::take(&mut self.entries),
            record_replacements: std::mem::take(&mut self.record_replacements),
            gql_return_read_set: std::mem::take(&mut self.gql_return_read_set),
            graph_op_budget: self.graph_op_budget.take(),
        };
        self.overlay = TxnOverlay::new(self.edge_uniqueness);
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::TxnCommit {
                request,
                return_read_view: false,
            })?
        {
            CoreWriteReply::TxnCommitResult(result) => Ok(result),
            _ => unreachable!("txn commit must return transaction commit results"),
        }
    }

    pub(crate) fn commit_with_gql_return_view(
        &mut self,
    ) -> Result<(TxnCommitResult, Arc<ReadView>), EngineError> {
        self.ensure_open()?;
        self.closed = true;
        let request = TxnCommitRequest {
            snapshot: Arc::clone(&self.snapshot),
            snapshot_seq: self.snapshot_seq,
            entries: std::mem::take(&mut self.entries),
            record_replacements: std::mem::take(&mut self.record_replacements),
            gql_return_read_set: std::mem::take(&mut self.gql_return_read_set),
            graph_op_budget: self.graph_op_budget.take(),
        };
        self.overlay = TxnOverlay::new(self.edge_uniqueness);
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::TxnCommit {
                request,
                return_read_view: true,
            })?
        {
            CoreWriteReply::TxnCommitResultWithReadView(result, view) => Ok((result, view)),
            _ => unreachable!("txn commit with read view must return transaction commit results"),
        }
    }

    pub fn rollback(&mut self) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.closed = true;
        self.entries.clear();
        self.record_replacements.clear();
        self.gql_return_read_set = TxnReturnReadSet::default();
        self.graph_op_budget = None;
        self.overlay = TxnOverlay::new(self.edge_uniqueness);
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), EngineError> {
        if self.closed {
            Err(EngineError::TxnClosed)
        } else {
            Ok(())
        }
    }

    fn next_slot_ref(&mut self) -> Result<TxnLocalRef, EngineError> {
        self.ensure_open()?;
        self.next_slot
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidOperation("transaction local slots exhausted".into()))?;
        Ok(TxnLocalRef::Slot(self.next_slot))
    }

    fn advance_next_slot(&mut self) -> Result<(), EngineError> {
        self.next_slot = self
            .next_slot
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidOperation("transaction local slots exhausted".into()))?;
        Ok(())
    }

    fn append_entry(
        &mut self,
        intent: TxnIntent,
        produced_node: Option<TxnLocalRef>,
        produced_edge: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.overlay.apply(
            &self.snapshot,
            &intent,
            produced_node.clone(),
            produced_edge.clone(),
        )?;
        self.entries.push(StagedTxnIntent {
            intent,
            produced_node,
            produced_edge,
        });
        Ok(())
    }

    fn rebuild_overlay_from_entries(&mut self) -> Result<(), EngineError> {
        let mut overlay = TxnOverlay::new(self.edge_uniqueness);
        for entry in &self.entries {
            overlay.apply(
                &self.snapshot,
                &entry.intent,
                entry.produced_node.clone(),
                entry.produced_edge.clone(),
            )?;
        }
        self.overlay = overlay;
        Ok(())
    }
}

impl TxnOverlay {
    fn new(edge_uniqueness: bool) -> Self {
        Self {
            edge_uniqueness,
            ..Self::default()
        }
    }

    fn apply(
        &mut self,
        snapshot: &ReadView,
        intent: &TxnIntent,
        produced_node: Option<TxnLocalRef>,
        produced_edge: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        match intent {
            TxnIntent::UpsertNode {
                alias,
                labels,
                key,
                options,
            } => self.apply_upsert_node(snapshot, alias, labels, key, options, produced_node),
            TxnIntent::UpsertEdge {
                alias,
                from,
                to,
                label,
                options,
            } => self.apply_upsert_edge(
                snapshot,
                alias,
                from,
                to,
                label,
                options,
                produced_edge,
            ),
            TxnIntent::DeleteNode { target } => self.apply_delete_node(snapshot, target),
            TxnIntent::DeleteEdge { target } => self.apply_delete_edge(snapshot, target),
            TxnIntent::InvalidateEdge { target, valid_to } => {
                self.apply_invalidate_edge(snapshot, target, *valid_to)
            }
        }
    }

    fn apply_upsert_node(
        &mut self,
        snapshot: &ReadView,
        alias: &Option<String>,
        labels: &[String],
        key: &str,
        options: &UpsertNodeOptions,
        produced: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        let validated_labels = ValidatedNodeLabelList::new(labels.iter().map(String::as_str))?;
        validate_node_key_for_write(key)?;
        if let Some(alias) = alias {
            if self.node_aliases.contains(alias) {
                return Err(EngineError::InvalidOperation(format!(
                    "duplicate transaction node alias '{}'",
                    alias
                )));
            }
        }

        let mut existing: Option<TxnNodeView> = None;
        for &label in validated_labels.as_slice() {
            let node_key = (label.to_string(), key.to_string());
            let candidate = match self.nodes_by_key.get(&node_key) {
                Some(NodeOverlayOpinion::Live(view)) => Some(view.clone()),
                Some(NodeOverlayOpinion::Deleted(view)) => view.clone(),
                Some(NodeOverlayOpinion::RemovedLabel) => None,
                None => match snapshot.label_catalog.resolve_node_label_for_read(label)? {
                    Some(label_id) => snapshot
                        .get_node_by_label_key(label_id, key)?
                        .map(|node| {
                            node_to_txn_view_with_resolved_label(
                                node,
                                label_id,
                                label.to_string(),
                                snapshot.label_catalog.as_ref(),
                            )
                        })
                        .transpose()?,
                    None => None,
                },
            };
            let Some(candidate) = candidate else {
                continue;
            };
            match existing.as_ref() {
                Some(winner) if !txn_node_views_match(winner, &candidate) => {
                    return Err(EngineError::InvalidOperation(format!(
                        "node key conflict for key '{}': requested label memberships resolve to different transaction nodes",
                        key
                    )));
                }
                None => existing = Some(candidate),
                _ => {}
            }
        }

        let labels: Vec<String> = validated_labels
            .as_slice()
            .iter()
            .map(|label| (*label).to_string())
            .collect();
        let related_locals = existing
            .as_ref()
            .map(|view| self.node_locals_for_view(view))
            .unwrap_or_default();
        if let Some(existing_view) = existing.as_ref() {
            let removed_labels: Vec<String> = existing_view
                .labels
                .iter()
                .filter(|label| !labels.iter().any(|new_label| new_label == *label))
                .cloned()
                .collect();
            for old_label in removed_labels {
                let key = (old_label, key.to_string());
                self.nodes_by_key
                    .insert(key.clone(), NodeOverlayOpinion::RemovedLabel);
                self.node_key_locals.remove(&key);
            }
        }

        let view = TxnNodeView {
            id: existing.as_ref().and_then(|node| node.id),
            local: produced.clone(),
            labels: labels.clone(),
            key: key.to_string(),
            props: options.props.clone(),
            created_at: existing.and_then(|node| node.created_at),
            updated_at: None,
            weight: options.weight,
            dense_vector: options.dense_vector.clone(),
            sparse_vector: options.sparse_vector.clone(),
        };
        if let Some(alias) = alias {
            self.node_aliases.insert(alias.clone());
        }
        self.insert_node_live_with_locals(view, produced, related_locals);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_upsert_edge(
        &mut self,
        snapshot: &ReadView,
        alias: &Option<String>,
        from: &TxnNodeRef,
        to: &TxnNodeRef,
        label: &str,
        options: &UpsertEdgeOptions,
        produced: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        validate_label_token_name(label)?;
        if let Some(alias) = alias {
            if self.edge_aliases.contains(alias) {
                return Err(EngineError::InvalidOperation(format!(
                    "duplicate transaction edge alias '{}'",
                    alias
                )));
            }
        }

        let from_key = self.endpoint_key(snapshot, from)?;
        let to_key = self.endpoint_key(snapshot, to)?;
        let triple_key = (from_key, to_key, label.to_string());
        let from_ref = self.canonical_node_ref(snapshot, from)?;
        let to_ref = self.canonical_node_ref(snapshot, to)?;
        let existing = if self.edge_uniqueness {
            match self.edges_by_triple.get(&triple_key) {
                Some(EdgeOverlayOpinion::Live(view)) => Some(view.clone()),
                Some(EdgeOverlayOpinion::Deleted(view)) => view.clone(),
                None => {
                    let from_id = self.committed_node_id(snapshot, from)?;
                    let to_id = self.committed_node_id(snapshot, to)?;
                    match (from_id, to_id) {
                        (Some(from_id), Some(to_id)) => {
                            match snapshot.label_catalog.resolve_edge_label_for_read(label)? {
                                Some(label_id) => snapshot
                                    .get_edge_by_triple(from_id, to_id, label_id)?
                                    .map(|edge| {
                                        edge_to_txn_view_with_resolved_label(
                                            edge,
                                            label_id,
                                            label.to_string(),
                                        )
                                    })
                                    .transpose()?,
                                None => None,
                            }
                        }
                        _ => None,
                    }
                }
            }
        } else {
            None
        };
        let created_at = existing.as_ref().and_then(|edge| edge.created_at);
        let valid_from = options.valid_from.or(created_at);
        let view = TxnEdgeView {
            id: existing.as_ref().and_then(|edge| edge.id),
            local: produced.clone(),
            from: from_ref,
            to: to_ref,
            label: label.to_string(),
            props: options.props.clone(),
            created_at,
            updated_at: None,
            weight: options.weight,
            valid_from,
            valid_to: options.valid_to.or(Some(i64::MAX)),
        };
        if let Some(alias) = alias {
            self.edge_aliases.insert(alias.clone());
        }
        self.insert_edge_live_with_key(view, produced, triple_key);
        Ok(())
    }

    fn apply_delete_node(
        &mut self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<(), EngineError> {
        let existing = self.get_node(snapshot, target)?;
        self.mark_incident_staged_edges_deleted(snapshot, target, existing.as_ref())?;
        self.mark_node_deleted(target, existing);
        Ok(())
    }

    fn apply_delete_edge(
        &mut self,
        snapshot: &ReadView,
        target: &TxnEdgeRef,
    ) -> Result<(), EngineError> {
        let existing = self.get_edge(snapshot, target)?;
        self.mark_edge_deleted(snapshot, target, existing)?;
        Ok(())
    }

    fn apply_invalidate_edge(
        &mut self,
        snapshot: &ReadView,
        target: &TxnEdgeRef,
        valid_to: i64,
    ) -> Result<(), EngineError> {
        if let Some(mut view) = self.get_edge(snapshot, target)? {
            view.updated_at = None;
            view.valid_to = Some(valid_to);
            let local = view.local.clone();
            self.update_edge_live(snapshot, view, local)?;
        }
        Ok(())
    }

    fn get_node(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<Option<TxnNodeView>, EngineError> {
        match target {
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view)) => Ok(Some(view.clone())),
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                Some(NodeOverlayOpinion::RemovedLabel) => Ok(None),
                None => Err(EngineError::InvalidOperation(format!(
                    "unknown transaction node local ref {:?}",
                    local
                ))),
            },
            TxnNodeRef::Id(id) => match self.nodes_by_id.get(id) {
                Some(NodeOverlayOpinion::Live(view)) => Ok(Some(view.clone())),
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                Some(NodeOverlayOpinion::RemovedLabel) => Ok(None),
                None => {
                    snapshot
                        .get_node(*id)?
                        .map(|node| node_to_txn_view(node, snapshot.label_catalog.as_ref()))
                        .transpose()
                }
            },
            TxnNodeRef::Key { label, key } => {
                validate_label_token_name(label)?;
                match self.nodes_by_key.get(&(label.clone(), key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => Ok(Some(view.clone())),
                    Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                    Some(NodeOverlayOpinion::RemovedLabel) => Ok(None),
                    None => {
                        let Some(label_id) =
                            snapshot.label_catalog.resolve_node_label_for_read(label)?
                        else {
                            return Ok(None);
                        };
                        snapshot
                            .get_node_by_label_key(label_id, key)?
                            .map(|node| {
                                node_to_txn_view_with_resolved_label(
                                    node,
                                    label_id,
                                    label.clone(),
                                    snapshot.label_catalog.as_ref(),
                                )
                            })
                            .transpose()
                    }
                }
            }
        }
    }

    fn get_edge(
        &self,
        snapshot: &ReadView,
        target: &TxnEdgeRef,
    ) -> Result<Option<TxnEdgeView>, EngineError> {
        let overlay = match target {
            TxnEdgeRef::Local(local) => match self.edges_by_local.get(local) {
                Some(EdgeOverlayOpinion::Live(view)) => {
                    return self.hide_if_endpoint_deleted(snapshot, view)
                }
                Some(EdgeOverlayOpinion::Deleted(_)) => return Ok(None),
                None => {
                    return Err(EngineError::InvalidOperation(format!(
                        "unknown transaction edge local ref {:?}",
                        local
                    )))
                }
            },
            TxnEdgeRef::Id(id) => self.edges_by_id.get(id),
            TxnEdgeRef::Triple {
                from,
                to,
                label,
            } => {
                validate_label_token_name(label)?;
                let Some(from_key) = self.read_endpoint_key(snapshot, from)? else {
                    return Ok(None);
                };
                let Some(to_key) = self.read_endpoint_key(snapshot, to)? else {
                    return Ok(None);
                };
                self.edges_by_triple
                    .get(&(from_key, to_key, label.clone()))
            }
        };
        match overlay {
            Some(EdgeOverlayOpinion::Live(view)) => self.hide_if_endpoint_deleted(snapshot, view),
            Some(EdgeOverlayOpinion::Deleted(_)) => Ok(None),
            None => self.snapshot_edge(snapshot, target),
        }
    }

    fn snapshot_edge(
        &self,
        snapshot: &ReadView,
        target: &TxnEdgeRef,
    ) -> Result<Option<TxnEdgeView>, EngineError> {
        let edge = match target {
            TxnEdgeRef::Local(_) => return Ok(None),
            TxnEdgeRef::Id(id) => snapshot.get_edge(*id)?,
            TxnEdgeRef::Triple {
                from,
                to,
                label,
            } => {
                validate_label_token_name(label)?;
                let Some(from_id) = self.committed_node_id(snapshot, from)? else {
                    return Ok(None);
                };
                let Some(to_id) = self.committed_node_id(snapshot, to)? else {
                    return Ok(None);
                };
                let Some(label_id) = snapshot
                    .label_catalog
                    .resolve_edge_label_for_read(label)?
                else {
                    return Ok(None);
                };
                snapshot.get_edge_by_triple(from_id, to_id, label_id)?
            }
        };
        match edge {
            Some(edge) if !self.committed_edge_endpoint_deleted(&edge) => {
                Ok(Some(edge_to_txn_view(
                    edge,
                    snapshot.label_catalog.as_ref(),
                )?))
            }
            _ => Ok(None),
        }
    }

    fn hide_if_endpoint_deleted(
        &self,
        snapshot: &ReadView,
        view: &TxnEdgeView,
    ) -> Result<Option<TxnEdgeView>, EngineError> {
        if self.node_ref_deleted(snapshot, &view.from)? || self.node_ref_deleted(snapshot, &view.to)? {
            Ok(None)
        } else {
            Ok(Some(view.clone()))
        }
    }

    fn node_ref_deleted(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<bool, EngineError> {
        match self.get_node(snapshot, target) {
            Ok(Some(_)) => Ok(false),
            Ok(None) => Ok(true),
            Err(EngineError::InvalidOperation(_)) => Ok(true),
            Err(err) => Err(err),
        }
    }

    fn committed_edge_endpoint_deleted(&self, edge: &EdgeRecord) -> bool {
        self.deleted_node_ids_seen.contains(&edge.from)
            || self.deleted_node_ids_seen.contains(&edge.to)
            || matches!(
                self.nodes_by_id.get(&edge.from),
                Some(NodeOverlayOpinion::Deleted(_))
            )
            || matches!(
                self.nodes_by_id.get(&edge.to),
                Some(NodeOverlayOpinion::Deleted(_))
            )
    }

    fn read_endpoint_key(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<Option<TxnEndpointKey>, EngineError> {
        match target {
            TxnNodeRef::Id(id) => match self.nodes_by_id.get(id) {
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                _ => Ok(Some(TxnEndpointKey::Id(*id))),
            },
            TxnNodeRef::Key { label, key } => {
                validate_label_token_name(label)?;
                match self.nodes_by_key.get(&(label.clone(), key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => {
                        if let Some(id) = view.id {
                            Ok(Some(TxnEndpointKey::Id(id)))
                        } else {
                            Ok(Some(self.uncommitted_endpoint_key(view, label, key)))
                        }
                    }
                    Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                    Some(NodeOverlayOpinion::RemovedLabel) => {
                        Ok(Some(TxnEndpointKey::Key(label.clone(), key.clone())))
                    }
                    None => {
                        match snapshot.label_catalog.resolve_node_label_for_read(label)? {
                            Some(label_id) => match snapshot.get_node_by_label_key(label_id, key)? {
                                Some(node) => Ok(Some(TxnEndpointKey::Id(node.id))),
                                None => Ok(Some(TxnEndpointKey::Key(
                                    label.clone(),
                                    key.clone(),
                                ))),
                            },
                            None => Ok(Some(TxnEndpointKey::Key(label.clone(), key.clone()))),
                        }
                    }
                }
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view)) => {
                    if let Some(id) = view.id {
                        Ok(Some(TxnEndpointKey::Id(id)))
                    } else {
                        Ok(Some(self.uncommitted_endpoint_key(
                            view,
                            txn_node_view_fallback_label(view)?,
                            &view.key,
                        )))
                    }
                }
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                Some(NodeOverlayOpinion::RemovedLabel) => Ok(None),
                None => Err(EngineError::InvalidOperation(format!(
                    "unknown transaction node local ref {:?}",
                    local
                ))),
            },
        }
    }

    fn endpoint_key(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<TxnEndpointKey, EngineError> {
        match target {
            TxnNodeRef::Id(id) => match self.nodes_by_id.get(id) {
                Some(NodeOverlayOpinion::Deleted(_)) => Err(EngineError::InvalidOperation(format!(
                    "transaction node id {} is deleted",
                    id
                ))),
                _ => Ok(TxnEndpointKey::Id(*id)),
            },
            TxnNodeRef::Key { label, key } => {
                validate_label_token_name(label)?;
                match self.nodes_by_key.get(&(label.clone(), key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => {
                        if let Some(id) = view.id {
                            Ok(TxnEndpointKey::Id(id))
                        } else {
                            Ok(self.uncommitted_endpoint_key(view, label, key))
                        }
                    }
                    Some(NodeOverlayOpinion::Deleted(_)) => Err(EngineError::InvalidOperation(
                        format!("transaction node key ({}, {}) is deleted", label, key),
                    )),
                    Some(NodeOverlayOpinion::RemovedLabel) => {
                        Ok(TxnEndpointKey::Key(label.clone(), key.clone()))
                    }
                    None => {
                        match snapshot.label_catalog.resolve_node_label_for_read(label)? {
                            Some(label_id) => match snapshot.get_node_by_label_key(label_id, key)? {
                                Some(node) => Ok(TxnEndpointKey::Id(node.id)),
                                None => Ok(TxnEndpointKey::Key(label.clone(), key.clone())),
                            },
                            None => Ok(TxnEndpointKey::Key(label.clone(), key.clone())),
                        }
                    }
                }
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view)) => {
                    if let Some(id) = view.id {
                        Ok(TxnEndpointKey::Id(id))
                    } else {
                        Ok(self.uncommitted_endpoint_key(
                            view,
                            txn_node_view_fallback_label(view)?,
                            &view.key,
                        ))
                    }
                }
                Some(NodeOverlayOpinion::Deleted(_)) => Err(EngineError::InvalidOperation(format!(
                    "transaction node local ref {:?} is deleted",
                    local
                ))),
                Some(NodeOverlayOpinion::RemovedLabel) => Err(EngineError::InvalidOperation(
                    format!("transaction node local ref {:?} is deleted", local),
                )),
                None => Err(EngineError::InvalidOperation(format!(
                    "unknown transaction node local ref {:?}",
                    local
                ))),
            },
        }
    }

    fn committed_node_id(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<Option<u64>, EngineError> {
        match self.get_node(snapshot, target)? {
            Some(view) => Ok(view.id),
            None => Ok(None),
        }
    }

    fn insert_node_live_with_locals(
        &mut self,
        view: TxnNodeView,
        local: Option<TxnLocalRef>,
        related_locals: Vec<TxnLocalRef>,
    ) {
        let mut locals_to_track = related_locals;
        if let Some(local) = local.as_ref() {
            push_distinct_txn_local(&mut locals_to_track, local.clone());
        }
        for label in &view.labels {
            let key = (label.clone(), view.key.clone());
            for local in &locals_to_track {
                let locals = self.node_key_locals.entry(key.clone()).or_default();
                if !locals.contains(local) {
                    locals.push(local.clone());
                }
            }

            let opinion = NodeOverlayOpinion::Live(view.clone());
            self.nodes_by_key.insert(key.clone(), opinion.clone());
            self.set_node_locals_for_key(&key, opinion);
        }
        let opinion = NodeOverlayOpinion::Live(view.clone());
        if let Some(id) = view.id {
            self.nodes_by_id.insert(id, opinion.clone());
        }
        for local in locals_to_track {
            self.nodes_by_local
                .insert(local.clone(), node_opinion_for_local(&opinion, &local));
        }
    }

    fn node_locals_for_view(&self, view: &TxnNodeView) -> Vec<TxnLocalRef> {
        let mut locals = Vec::new();
        if let Some(local) = &view.local {
            push_distinct_txn_local(&mut locals, local.clone());
        }
        for label in &view.labels {
            let key = (label.clone(), view.key.clone());
            if let Some(known_locals) = self.node_key_locals.get(&key) {
                for local in known_locals {
                    push_distinct_txn_local(&mut locals, local.clone());
                }
            }
        }
        locals
    }

    fn uncommitted_endpoint_key(
        &self,
        view: &TxnNodeView,
        fallback_label: &str,
        fallback_key: &str,
    ) -> TxnEndpointKey {
        if let Some(local) = self.canonical_uncommitted_node_local(view) {
            TxnEndpointKey::Local(local)
        } else {
            TxnEndpointKey::Key(fallback_label.to_string(), fallback_key.to_string())
        }
    }

    fn canonical_uncommitted_node_local(&self, view: &TxnNodeView) -> Option<TxnLocalRef> {
        for label in &view.labels {
            let key = (label.clone(), view.key.clone());
            let Some(locals) = self.node_key_locals.get(&key) else {
                continue;
            };
            for local in locals {
                let Some(NodeOverlayOpinion::Live(local_view)) = self.nodes_by_local.get(local)
                else {
                    continue;
                };
                if local_view.id.is_none()
                    && local_view.key == view.key
                    && txn_label_sets_equal(&local_view.labels, &view.labels)
                {
                    return Some(local.clone());
                }
            }
        }
        view.local.clone()
    }

    fn canonical_node_ref(
        &self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
    ) -> Result<TxnNodeRef, EngineError> {
        let Some(view) = self.get_node(snapshot, target)? else {
            return Ok(target.clone());
        };
        if let Some(id) = view.id {
            Ok(TxnNodeRef::Id(id))
        } else if let Some(local) = self.canonical_uncommitted_node_local(&view) {
            Ok(TxnNodeRef::Local(local))
        } else {
            Ok(target.clone())
        }
    }

    fn insert_edge_live_with_key(
        &mut self,
        view: TxnEdgeView,
        local: Option<TxnLocalRef>,
        triple_key: (TxnEndpointKey, TxnEndpointKey, String),
    ) {
        if self.edge_uniqueness {
            if let Some(local) = local.as_ref() {
                let locals = self.edge_triple_locals.entry(triple_key.clone()).or_default();
                if !locals.contains(local) {
                    locals.push(local.clone());
                }
            }
        }

        let opinion = EdgeOverlayOpinion::Live(view.clone());
        self.edges_by_triple.insert(triple_key.clone(), opinion.clone());
        if let Some(id) = view.id {
            self.edges_by_id.insert(id, opinion.clone());
        }
        if self.edge_uniqueness {
            self.set_edge_locals_for_triple(&triple_key, EdgeOverlayOpinion::Live(view));
        } else if let Some(local) = local {
            self.edges_by_local
                .insert(local.clone(), edge_opinion_for_local(&opinion, &local));
        }
    }

    fn update_edge_live(
        &mut self,
        snapshot: &ReadView,
        view: TxnEdgeView,
        local: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        let from_key = self.endpoint_key(snapshot, &view.from)?;
        let to_key = self.endpoint_key(snapshot, &view.to)?;
        let label = view.label.clone();
        let triple_key = (from_key, to_key, label);
        let opinion = EdgeOverlayOpinion::Live(view.clone());

        if self.edge_uniqueness {
            self.edges_by_triple
                .insert(triple_key.clone(), opinion.clone());
            self.set_edge_locals_for_triple(&triple_key, opinion);
            return Ok(());
        }

        if self.edge_triple_matches_target(&triple_key, local.as_ref(), view.id) {
            self.edges_by_triple.insert(triple_key, opinion.clone());
        }
        if let Some(id) = view.id {
            self.edges_by_id.insert(id, opinion.clone());
        }
        if let Some(local) = local {
            self.edges_by_local
                .insert(local.clone(), edge_opinion_for_local(&opinion, &local));
        }
        Ok(())
    }

    fn insert_edge_triple_delete_if_current(
        &mut self,
        triple_key: (TxnEndpointKey, TxnEndpointKey, String),
        opinion: EdgeOverlayOpinion,
        deleted_local: Option<&TxnLocalRef>,
        deleted_id: Option<u64>,
    ) {
        if self.edge_uniqueness {
            self.edges_by_triple
                .insert(triple_key.clone(), opinion.clone());
            self.set_edge_locals_for_triple(&triple_key, opinion);
            return;
        }

        if self.edge_triple_matches_target(&triple_key, deleted_local, deleted_id) {
            self.edges_by_triple.insert(triple_key, opinion);
        }
    }

    fn edge_triple_matches_target(
        &self,
        triple_key: &(TxnEndpointKey, TxnEndpointKey, String),
        target_local: Option<&TxnLocalRef>,
        target_id: Option<u64>,
    ) -> bool {
        let Some(current) = self.edges_by_triple.get(triple_key) else {
            return true;
        };
        edge_opinion_matches_target(current, target_local, target_id)
    }

    fn track_edge_local_for_triple(
        &mut self,
        triple_key: (TxnEndpointKey, TxnEndpointKey, String),
        local: &TxnLocalRef,
    ) {
        if self.edge_uniqueness {
            let locals = self.edge_triple_locals.entry(triple_key.clone()).or_default();
            if !locals.contains(local) {
                locals.push(local.clone());
            }
        }
    }

    fn mark_node_deleted(&mut self, target: &TxnNodeRef, existing: Option<TxnNodeView>) {
        let opinion = NodeOverlayOpinion::Deleted(existing.clone());
        let deleted_id = existing.as_ref().and_then(|view| view.id).or(match target {
            TxnNodeRef::Id(id) => Some(*id),
            _ => None,
        });
        match target {
            TxnNodeRef::Local(local) => {
                self.nodes_by_local.insert(local.clone(), opinion.clone());
            }
            TxnNodeRef::Id(id) => {
                self.nodes_by_id.insert(*id, opinion.clone());
            }
            TxnNodeRef::Key { label, key } => {
                self.nodes_by_key
                    .insert((label.clone(), key.clone()), opinion.clone());
            }
        }
        if let Some(id) = deleted_id {
            self.deleted_node_ids_seen.insert(id);
        }
        if let Some(view) = existing.as_ref() {
            let keys: Vec<(String, String)> = view
                .labels
                .iter()
                .map(|label| (label.clone(), view.key.clone()))
                .collect();
            for key in &keys {
                self.nodes_by_key.insert(key.clone(), opinion.clone());
            }
            if let Some(id) = view.id {
                self.nodes_by_id.insert(id, opinion.clone());
            }
            if let Some(local) = &view.local {
                for key in &keys {
                    let locals = self.node_key_locals.entry(key.clone()).or_default();
                    if !locals.contains(local) {
                        locals.push(local.clone());
                    }
                }
            }
            for key in keys {
                self.set_node_locals_for_key(&key, opinion.clone());
            }
        }
    }

    fn mark_incident_staged_edges_deleted(
        &mut self,
        snapshot: &ReadView,
        target: &TxnNodeRef,
        existing: Option<&TxnNodeView>,
    ) -> Result<(), EngineError> {
        let mut incident = Vec::new();
        for opinion in self
            .edges_by_local
            .values()
            .chain(self.edges_by_id.values())
            .chain(self.edges_by_triple.values())
        {
            let EdgeOverlayOpinion::Live(view) = opinion else {
                continue;
            };
            if self.edge_view_incident_to_node(snapshot, view, target, existing)? {
                incident.push(view.clone());
            }
        }

        for view in incident {
            let target = match (&view.local, view.id) {
                (Some(local), _) => TxnEdgeRef::Local(local.clone()),
                (None, Some(id)) => TxnEdgeRef::Id(id),
                (None, None) => TxnEdgeRef::Triple {
                    from: view.from.clone(),
                    to: view.to.clone(),
                    label: view.label.clone(),
                },
            };
            self.mark_edge_deleted(snapshot, &target, Some(view))?;
        }
        Ok(())
    }

    fn edge_view_incident_to_node(
        &self,
        snapshot: &ReadView,
        edge: &TxnEdgeView,
        target: &TxnNodeRef,
        existing: Option<&TxnNodeView>,
    ) -> Result<bool, EngineError> {
        Ok(self.node_ref_matches_deleted_node(snapshot, &edge.from, target, existing)?
            || self.node_ref_matches_deleted_node(snapshot, &edge.to, target, existing)?)
    }

    fn node_ref_matches_deleted_node(
        &self,
        snapshot: &ReadView,
        candidate: &TxnNodeRef,
        target: &TxnNodeRef,
        existing: Option<&TxnNodeView>,
    ) -> Result<bool, EngineError> {
        if candidate == target {
            return Ok(true);
        }

        let deleted_id = existing.as_ref().and_then(|view| view.id).or(match target {
            TxnNodeRef::Id(id) => Some(*id),
            _ => None,
        });
        let deleted_keys = deleted_node_keys(existing, target);

        match candidate {
            TxnNodeRef::Id(id) => Ok(deleted_id == Some(*id)),
            TxnNodeRef::Key { label, key } => {
                validate_label_token_name(label)?;
                if deleted_keys
                    .iter()
                    .any(|(deleted_label, deleted_key)| {
                        deleted_label == label && deleted_key == key
                    })
                {
                    return Ok(true);
                }
                let Some(id) = deleted_id else {
                    return Ok(false);
                };
                let Some(label_id) = snapshot.label_catalog.resolve_node_label_for_read(label)? else {
                    return Ok(false);
                };
                Ok(snapshot
                    .get_node_by_label_key(label_id, key)?
                    .is_some_and(|node| node.id == id))
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view))
                | Some(NodeOverlayOpinion::Deleted(Some(view))) => {
                    Ok(view.id.is_some_and(|id| deleted_id == Some(id))
                        || deleted_keys.iter().any(|(label, key)| {
                            view.key == *key
                                && view.labels.iter().any(|existing| existing == label)
                        }))
                }
                Some(NodeOverlayOpinion::Deleted(None)) => Ok(false),
                Some(NodeOverlayOpinion::RemovedLabel) => Ok(false),
                None => Err(EngineError::InvalidOperation(format!(
                    "unknown transaction node local ref {:?}",
                    local
                ))),
            },
        }
    }

    fn mark_edge_deleted(
        &mut self,
        snapshot: &ReadView,
        target: &TxnEdgeRef,
        existing: Option<TxnEdgeView>,
    ) -> Result<(), EngineError> {
        let opinion = EdgeOverlayOpinion::Deleted(existing.clone());
        match target {
            TxnEdgeRef::Local(local) => {
                self.edges_by_local.insert(local.clone(), opinion.clone());
            }
            TxnEdgeRef::Id(id) => {
                self.edges_by_id.insert(*id, opinion.clone());
            }
            TxnEdgeRef::Triple {
                from,
                to,
                label,
            } => {
                validate_label_token_name(label)?;
                if let (Some(from_key), Some(to_key)) = (
                    self.read_endpoint_key(snapshot, from)?,
                    self.read_endpoint_key(snapshot, to)?,
                ) {
                    self.insert_edge_triple_delete_if_current(
                        (from_key, to_key, label.clone()),
                        opinion.clone(),
                        None,
                        None,
                    );
                }
            }
        }
        if let Some(view) = existing.as_ref() {
            if let Some(id) = view.id {
                self.edges_by_id.insert(id, opinion.clone());
            }
            let from_key = self.endpoint_key(snapshot, &view.from)?;
            let to_key = self.endpoint_key(snapshot, &view.to)?;
            let triple_key = (from_key, to_key, view.label.clone());
            if let Some(local) = &view.local {
                self.edges_by_local
                    .insert(local.clone(), edge_opinion_for_local(&opinion, local));
                self.track_edge_local_for_triple(triple_key.clone(), local);
            }
            self.insert_edge_triple_delete_if_current(
                triple_key,
                opinion,
                view.local.as_ref(),
                view.id,
            );
        }
        Ok(())
    }

    fn set_node_locals_for_key(
        &mut self,
        key: &(String, String),
        opinion: NodeOverlayOpinion,
    ) {
        let Some(locals) = self.node_key_locals.get(key).cloned() else {
            return;
        };
        for local in locals {
            self.nodes_by_local
                .insert(local.clone(), node_opinion_for_local(&opinion, &local));
        }
    }

    fn set_edge_locals_for_triple(
        &mut self,
        triple_key: &(TxnEndpointKey, TxnEndpointKey, String),
        opinion: EdgeOverlayOpinion,
    ) {
        let Some(locals) = self.edge_triple_locals.get(triple_key).cloned() else {
            return;
        };
        for local in locals {
            self.edges_by_local
                .insert(local.clone(), edge_opinion_for_local(&opinion, &local));
        }
    }
}

fn node_opinion_for_local(
    opinion: &NodeOverlayOpinion,
    local: &TxnLocalRef,
) -> NodeOverlayOpinion {
    match opinion {
        NodeOverlayOpinion::Live(view) => {
            let mut view = view.clone();
            view.local = Some(local.clone());
            NodeOverlayOpinion::Live(view)
        }
        NodeOverlayOpinion::Deleted(view) => NodeOverlayOpinion::Deleted(view.as_ref().map(|view| {
            let mut view = view.clone();
            view.local = Some(local.clone());
            view
        })),
        NodeOverlayOpinion::RemovedLabel => NodeOverlayOpinion::RemovedLabel,
    }
}

fn edge_opinion_for_local(
    opinion: &EdgeOverlayOpinion,
    local: &TxnLocalRef,
) -> EdgeOverlayOpinion {
    match opinion {
        EdgeOverlayOpinion::Live(view) => {
            let mut view = view.clone();
            view.local = Some(local.clone());
            EdgeOverlayOpinion::Live(view)
        }
        EdgeOverlayOpinion::Deleted(view) => EdgeOverlayOpinion::Deleted(view.as_ref().map(|view| {
            let mut view = view.clone();
            view.local = Some(local.clone());
            view
        })),
    }
}

fn edge_opinion_matches_target(
    opinion: &EdgeOverlayOpinion,
    target_local: Option<&TxnLocalRef>,
    target_id: Option<u64>,
) -> bool {
    match opinion {
        EdgeOverlayOpinion::Live(view) | EdgeOverlayOpinion::Deleted(Some(view)) => {
            target_id.is_some_and(|id| view.id == Some(id))
                || target_local.is_some_and(|local| view.local.as_ref() == Some(local))
        }
        EdgeOverlayOpinion::Deleted(None) => target_local.is_none() && target_id.is_none(),
    }
}

fn push_distinct_txn_local(locals: &mut Vec<TxnLocalRef>, local: TxnLocalRef) {
    if !locals.contains(&local) {
        locals.push(local);
    }
}

fn txn_label_sets_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|label| right.iter().any(|other| other == label))
}

fn txn_node_view_fallback_label(view: &TxnNodeView) -> Result<&str, EngineError> {
    view.labels.first().map(String::as_str).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "transaction node view for key '{}' has no labels",
            view.key
        ))
    })
}

fn deleted_node_keys(
    existing: Option<&TxnNodeView>,
    target: &TxnNodeRef,
) -> Vec<(String, String)> {
    if let Some(view) = existing {
        return view
            .labels
            .iter()
            .map(|label| (label.clone(), view.key.clone()))
            .collect();
    }
    match target {
        TxnNodeRef::Key { label, key } => vec![(label.clone(), key.clone())],
        _ => Vec::new(),
    }
}

fn push_distinct_txn_name<'a>(
    name: &'a str,
    names: &mut Vec<&'a str>,
    seen: &mut HashSet<&'a str>,
) {
    if seen.insert(name) {
        names.push(name);
    }
}

fn collect_txn_intent_read_label_names<'a>(
    intent: &'a TxnIntent,
    node_labels: &mut Vec<&'a str>,
    seen_node_labels: &mut HashSet<&'a str>,
    edge_labels: &mut Vec<&'a str>,
    seen_edge_labels: &mut HashSet<&'a str>,
) {
    match intent {
        TxnIntent::UpsertNode { .. } => {}
        TxnIntent::UpsertEdge { from, to, .. } => {
            collect_txn_node_ref_read_label_names(from, node_labels, seen_node_labels);
            collect_txn_node_ref_read_label_names(to, node_labels, seen_node_labels);
        }
        TxnIntent::DeleteNode { target } => {
            collect_txn_node_ref_read_label_names(target, node_labels, seen_node_labels);
        }
        TxnIntent::DeleteEdge { target } | TxnIntent::InvalidateEdge { target, .. } => {
            collect_txn_edge_ref_read_label_names(
                target,
                node_labels,
                seen_node_labels,
                edge_labels,
                seen_edge_labels,
            );
        }
    }
}

fn collect_txn_replacement_read_label_names<'a>(
    replacement: &'a TxnRecordReplacement,
    node_labels: &mut Vec<&'a str>,
    seen_node_labels: &mut HashSet<&'a str>,
    edge_labels: &mut Vec<&'a str>,
    seen_edge_labels: &mut HashSet<&'a str>,
) {
    match replacement {
        TxnRecordReplacement::Node(node) => {
            for label in &node.labels {
                push_distinct_txn_name(label, node_labels, seen_node_labels);
            }
        }
        TxnRecordReplacement::Edge(edge) => {
            push_distinct_txn_name(&edge.label, edge_labels, seen_edge_labels);
        }
    }
}

fn collect_txn_node_ref_read_label_names<'a>(
    target: &'a TxnNodeRef,
    node_labels: &mut Vec<&'a str>,
    seen_node_labels: &mut HashSet<&'a str>,
) {
    match target {
        TxnNodeRef::Key { label, .. } => {
            push_distinct_txn_name(label, node_labels, seen_node_labels);
        }
        TxnNodeRef::Id(_) | TxnNodeRef::Local(_) => {}
    }
}

fn collect_txn_edge_ref_read_label_names<'a>(
    target: &'a TxnEdgeRef,
    node_labels: &mut Vec<&'a str>,
    seen_node_labels: &mut HashSet<&'a str>,
    edge_labels: &mut Vec<&'a str>,
    seen_edge_labels: &mut HashSet<&'a str>,
) {
    match target {
        TxnEdgeRef::Triple {
            from,
            to,
            label,
        } => {
            collect_txn_node_ref_read_label_names(from, node_labels, seen_node_labels);
            collect_txn_node_ref_read_label_names(to, node_labels, seen_node_labels);
            push_distinct_txn_name(label, edge_labels, seen_edge_labels);
        }
        TxnEdgeRef::Id(_) | TxnEdgeRef::Local(_) => {}
    }
}

fn collect_txn_intent_cache_targets(
    intent: &TxnIntent,
    label_resolution: &TxnLabelResolution,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
    edge_ids: &mut NodeIdSet,
) {
    match intent {
        TxnIntent::UpsertNode { labels, key, .. } => {
            for label in labels {
                if let Some(label_id) = label_resolution.node_label_id(label) {
                    node_keys.insert((label_id, key.clone()));
                }
            }
        }
        TxnIntent::UpsertEdge { from, to, .. } => {
            collect_txn_node_ref_cache_targets(from, label_resolution, node_keys, node_ids);
            collect_txn_node_ref_cache_targets(to, label_resolution, node_keys, node_ids);
        }
        TxnIntent::DeleteNode { target } => {
            collect_txn_node_ref_cache_targets(target, label_resolution, node_keys, node_ids);
        }
        TxnIntent::DeleteEdge { target } => {
            collect_txn_edge_ref_cache_targets(target, label_resolution, node_keys, node_ids, edge_ids);
        }
        TxnIntent::InvalidateEdge { target, .. } => {
            collect_txn_edge_ref_cache_targets(target, label_resolution, node_keys, node_ids, edge_ids);
        }
    }
}

fn collect_txn_replacement_cache_targets(
    replacement: &TxnRecordReplacement,
    label_resolution: &TxnLabelResolution,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
    edge_ids: &mut NodeIdSet,
) {
    match replacement {
        TxnRecordReplacement::Node(node) => {
            node_ids.insert(node.id);
            for label in &node.labels {
                if let Some(label_id) = label_resolution.node_label_id(label) {
                    node_keys.insert((label_id, node.key.clone()));
                }
            }
        }
        TxnRecordReplacement::Edge(edge) => {
            edge_ids.insert(edge.id);
            node_ids.insert(edge.from);
            node_ids.insert(edge.to);
        }
    }
}

fn collect_txn_node_ref_cache_targets(
    target: &TxnNodeRef,
    label_resolution: &TxnLabelResolution,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
) {
    match target {
        TxnNodeRef::Id(id) => {
            node_ids.insert(*id);
        }
        TxnNodeRef::Key { label, key } => {
            if let Some(label_id) = label_resolution.node_label_id(label) {
                node_keys.insert((label_id, key.clone()));
            }
        }
        TxnNodeRef::Local(_) => {}
    }
}

fn collect_txn_edge_ref_cache_targets(
    target: &TxnEdgeRef,
    label_resolution: &TxnLabelResolution,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
    edge_ids: &mut NodeIdSet,
) {
    match target {
        TxnEdgeRef::Id(id) => {
            edge_ids.insert(*id);
        }
        TxnEdgeRef::Triple { from, to, .. } => {
            collect_txn_node_ref_cache_targets(from, label_resolution, node_keys, node_ids);
            collect_txn_node_ref_cache_targets(to, label_resolution, node_keys, node_ids);
        }
        TxnEdgeRef::Local(_) => {}
    }
}

fn node_to_txn_view(
    node: NodeRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<TxnNodeView, EngineError> {
    let labels = txn_labels_from_record(&node, catalog)?;
    Ok(TxnNodeView {
        id: Some(node.id),
        local: None,
        labels,
        key: node.key,
        props: node.props,
        created_at: Some(node.created_at),
        updated_at: Some(node.updated_at),
        weight: node.weight,
        dense_vector: node.dense_vector,
        sparse_vector: node.sparse_vector,
    })
}

fn node_to_txn_view_with_resolved_label(
    node: NodeRecord,
    expected_label_id: u32,
    label: String,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<TxnNodeView, EngineError> {
    if !node.label_ids.contains(expected_label_id) {
        return Err(EngineError::InvalidOperation(format!(
            "node record {} resolved by label '{}' expected label_id {} but found {:?}",
            node.id, label, expected_label_id, node.label_ids
        )));
    }
    let labels = txn_labels_from_record(&node, catalog)?;
    Ok(TxnNodeView {
        id: Some(node.id),
        local: None,
        labels,
        key: node.key,
        props: node.props,
        created_at: Some(node.created_at),
        updated_at: Some(node.updated_at),
        weight: node.weight,
        dense_vector: node.dense_vector,
        sparse_vector: node.sparse_vector,
    })
}

fn txn_labels_from_record(
    node: &NodeRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<Vec<String>, EngineError> {
    node.label_ids
        .as_slice()
        .iter()
        .map(|&label_id| {
            catalog.node_label(label_id).map(str::to_string).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "node record {} references missing node label_id {}",
                    node.id, label_id
                ))
            })
        })
        .collect()
}

fn txn_node_views_match(left: &TxnNodeView, right: &TxnNodeView) -> bool {
    match (left.id, right.id) {
        (Some(left), Some(right)) => left == right,
        (None, None) => left.local.is_some() && left.local == right.local,
        _ => false,
    }
}

fn edge_to_txn_view(
    edge: EdgeRecord,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<TxnEdgeView, EngineError> {
    let label = catalog
        .edge_label(edge.label_id)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "edge record {} references missing edge-label label_id {}",
                edge.id, edge.label_id
            ))
        })?
        .to_string();
    Ok(TxnEdgeView {
        id: Some(edge.id),
        local: None,
        from: TxnNodeRef::Id(edge.from),
        to: TxnNodeRef::Id(edge.to),
        label,
        props: edge.props,
        created_at: Some(edge.created_at),
        updated_at: Some(edge.updated_at),
        weight: edge.weight,
        valid_from: Some(edge.valid_from),
        valid_to: Some(edge.valid_to),
    })
}

fn edge_to_txn_view_with_resolved_label(
    edge: EdgeRecord,
    expected_label_id: u32,
    label: String,
) -> Result<TxnEdgeView, EngineError> {
    if edge.label_id != expected_label_id {
        return Err(EngineError::InvalidOperation(format!(
            "edge record {} resolved by edge label '{}' expected label_id {} but found {}",
            edge.id, label, expected_label_id, edge.label_id
        )));
    }
    Ok(TxnEdgeView {
        id: Some(edge.id),
        local: None,
        from: TxnNodeRef::Id(edge.from),
        to: TxnNodeRef::Id(edge.to),
        label,
        props: edge.props,
        created_at: Some(edge.created_at),
        updated_at: Some(edge.updated_at),
        weight: edge.weight,
        valid_from: Some(edge.valid_from),
        valid_to: Some(edge.valid_to),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetOpinion {
    Absent,
    Live { last_write_seq: u64 },
    Tombstone { last_write_seq: u64 },
}

impl TargetOpinion {
    fn last_write_seq(self) -> Option<u64> {
        match self {
            TargetOpinion::Absent => None,
            TargetOpinion::Live { last_write_seq }
            | TargetOpinion::Tombstone { last_write_seq } => Some(last_write_seq),
        }
    }
}

#[derive(Default)]
struct PlannedTxnState {
    node_ids: Vec<u64>,
    edge_ids: Vec<u64>,
    local_node_ids: BTreeMap<TxnLocalRef, u64>,
    local_edge_ids: BTreeMap<TxnLocalRef, u64>,
    nodes_by_key: HashMap<(u32, String), (u64, i64)>,
    removed_node_keys: HashSet<(u32, String)>,
    node_records_by_id: NodeIdMap<NodeRecord>,
    edges_by_triple: HashMap<(u64, u64, u32), (u64, i64)>,
    edge_id_to_triple: NodeIdMap<(u64, u64, u32)>,
    edge_records_by_id: NodeIdMap<EdgeRecord>,
    deleted_node_ids: NodeIdSet,
    deleted_edge_ids: NodeIdSet,
}

fn remove_planned_edge_triple_if_current(
    state: &mut PlannedTxnState,
    triple: (u64, u64, u32),
    edge_id: u64,
) {
    if state
        .edges_by_triple
        .get(&triple)
        .is_some_and(|&(current_id, _)| current_id == edge_id)
    {
        state.edges_by_triple.remove(&triple);
    }
}

fn set_planned_edge_triple_if_current_or_absent(
    state: &mut PlannedTxnState,
    triple: (u64, u64, u32),
    edge_id: u64,
    created_at: i64,
) {
    if state
        .edges_by_triple
        .get(&triple)
        .is_none_or(|&(current_id, _)| current_id == edge_id)
    {
        state.edges_by_triple.insert(triple, (edge_id, created_at));
    }
}

fn resolve_txn_node_replacement_label_ids(
    label_resolution: &TxnLabelResolution,
    replacement: &TxnNodeRecordReplacement,
) -> Result<NodeLabelSet, EngineError> {
    let validated_labels =
        ValidatedNodeLabelList::new(replacement.labels.iter().map(String::as_str))?;
    let mut label_ids = [0u32; MAX_NODE_LABELS_PER_NODE];
    for (idx, &label) in validated_labels.as_slice().iter().enumerate() {
        label_ids[idx] = label_resolution.node_label_id(label).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "transaction node label '{}' was not resolved for commit",
                label
            ))
        })?;
    }
    NodeLabelSet::from_label_ids(label_ids[..validated_labels.len()].iter().copied())
}

#[derive(Default)]
struct TxnPlanningCache {
    begin_nodes_by_id: NodeIdMap<Option<NodeRecord>>,
    begin_edges_by_id: NodeIdMap<Option<EdgeRecord>>,
    begin_node_keys: HashMap<(u32, String), Option<NodeRecord>>,
    current_node_keys: HashMap<(u32, String), Option<NodeRecord>>,
    begin_edge_triples: HashMap<(u64, u64, u32), Option<EdgeRecord>>,
    current_edge_triples: HashMap<(u64, u64, u32), Option<EdgeRecord>>,
    current_edges_by_id: NodeIdMap<Option<EdgeRecord>>,
    node_opinions_by_id: NodeIdMap<TargetOpinion>,
    edge_opinions_by_id: NodeIdMap<TargetOpinion>,
}

#[derive(Default)]
struct TxnLabelResolution {
    node_labels: HashMap<String, Option<u32>>,
    edge_labels: HashMap<String, Option<u32>>,
}

impl TxnLabelResolution {
    fn node_label_id(&self, label: &str) -> Option<u32> {
        self.node_labels.get(label).and_then(|id| *id)
    }

    fn edge_label_id(&self, label: &str) -> Option<u32> {
        self.edge_labels.get(label).and_then(|id| *id)
    }

    fn insert_node_label(&mut self, label: String, label_id: Option<u32>) {
        self.node_labels.entry(label).or_insert(label_id);
    }

    fn insert_edge_label(&mut self, label: String, label_id: Option<u32>) {
        self.edge_labels.entry(label).or_insert(label_id);
    }
}

impl EngineCore {
    fn plan_txn_commit(&mut self, request: &TxnCommitRequest) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let (label_resolution, mut ops, label_catalog_changed) = self.resolve_txn_label_names(request)?;
        let mut state = PlannedTxnState::default();
        let mut cache = self.build_txn_planning_cache(request, &label_resolution)?;
        let mut next_node_id = self.next_node_id;
        let mut next_edge_id = self.next_edge_id;
        let mut graph_ops = TxnGraphOpCounter::new(request.graph_op_budget);

        for entry in &request.entries {
            match &entry.intent {
                TxnIntent::UpsertNode {
                    labels,
                    key,
                    options,
                    ..
                } => {
                    validate_node_key_for_write(key)?;
                    let validated_labels =
                        ValidatedNodeLabelList::new(labels.iter().map(String::as_str))?;
                    let mut label_ids = [0u32; MAX_NODE_LABELS_PER_NODE];
                    for (idx, &label) in validated_labels.as_slice().iter().enumerate() {
                        label_ids[idx] = label_resolution.node_label_id(label).ok_or_else(|| {
                            EngineError::InvalidOperation(format!(
                                "transaction node label '{}' was not resolved for commit",
                                label
                            ))
                        })?;
                    }
                    let label_ids =
                        NodeLabelSet::from_label_ids(label_ids[..validated_labels.len()].iter().copied())?;

                    let mut winner: Option<(u64, i64, Option<NodeRecord>)> = None;
                    for &label_id in label_ids.as_slice() {
                        let key_tuple = (label_id, key.clone());
                        let candidate = if let Some(&(id, created_at)) =
                            state.nodes_by_key.get(&key_tuple)
                        {
                            Some((id, created_at, state.node_records_by_id.get(&id).cloned()))
                        } else if state.removed_node_keys.contains(&key_tuple) {
                            None
                        } else {
                            self.validate_node_key_conflict(request, &mut cache, label_id, key)?;
                            let current = self.cached_current_node_key(&mut cache, label_id, key)?;
                            let node = match current {
                                Some(node) => Some(node),
                                None => self.cached_begin_node_key(
                                    request,
                                    &mut cache,
                                    label_id,
                                    key,
                                )?,
                            };
                            node.map(|node| (node.id, node.created_at, Some(node)))
                        };
                        let Some(candidate) = candidate else {
                            continue;
                        };
                        match winner.as_ref() {
                            Some((winner_id, _, _)) if *winner_id != candidate.0 => {
                                return Err(node_key_conflict_error(key, *winner_id, candidate.0));
                            }
                            None => winner = Some(candidate),
                            _ => {}
                        }
                    }

                    let (id, created_at, previous_record) = match winner {
                        Some(existing) => existing,
                        None => {
                            let id = next_node_id;
                            next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
                                EngineError::InvalidOperation("node id counter overflow".into())
                            })?;
                            (id, now, None)
                        }
                    };

                    let previous_labels = state
                        .node_records_by_id
                        .get(&id)
                        .map(|node| node.label_ids)
                        .or_else(|| previous_record.as_ref().map(|node| node.label_ids));
                    if let Some(previous_labels) = previous_labels {
                        for &old_label_id in previous_labels.as_slice() {
                            if !label_ids.contains(old_label_id) {
                                let key_tuple = (old_label_id, key.clone());
                                state.nodes_by_key.remove(&key_tuple);
                                state.removed_node_keys.insert(key_tuple);
                            }
                        }
                    }

                    for &label_id in label_ids.as_slice() {
                        let key_tuple = (label_id, key.clone());
                        state.removed_node_keys.remove(&key_tuple);
                        state.nodes_by_key.insert(key_tuple, (id, created_at));
                    }
                    state.deleted_node_ids.remove(&id);
                    if let Some(local) = &entry.produced_node {
                        state.local_node_ids.insert(local.clone(), id);
                    }
                    let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
                        self.manifest.dense_vector.as_ref(),
                        options.dense_vector.as_ref(),
                        options.sparse_vector.as_ref(),
                    )?;
                    let node = NodeRecord {
                        id,
                        label_ids,
                        key: key.clone(),
                        props: options.props.clone(),
                        created_at,
                        updated_at: now,
                        weight: options.weight,
                        dense_vector,
                        sparse_vector,
                        last_write_seq: 0,
                    };
                    state.node_records_by_id.insert(id, node.clone());
                    graph_ops.reserve(1)?;
                    ops.push(WalOp::UpsertNode(node));
                    state.node_ids.push(id);
                }
                TxnIntent::UpsertEdge {
                    from,
                    to,
                    label,
                    options,
                    ..
                } => {
                    let label_id = label_resolution.edge_label_id(label).ok_or_else(|| {
                        EngineError::InvalidOperation(format!(
                            "transaction edge label '{}' was not resolved for commit",
                            label
                        ))
                    })?;
                    let from_id =
                        self.resolve_node_ref_required(from, &state, request, &label_resolution, &mut cache)?;
                    let to_id =
                        self.resolve_node_ref_required(to, &state, request, &label_resolution, &mut cache)?;
                    self.validate_node_id_conflict(&mut cache, from_id, request.snapshot_seq)?;
                    self.validate_node_id_conflict(&mut cache, to_id, request.snapshot_seq)?;
                    let triple = (from_id, to_id, label_id);
                    let (id, created_at) = if self.edge_uniqueness {
                        if let Some(&(id, created_at)) = state.edges_by_triple.get(&triple) {
                            (id, created_at)
                        } else {
                            self.validate_edge_triple_conflict(request, &mut cache, from_id, to_id, label_id)?;
                            match self.cached_current_edge_triple(&mut cache, from_id, to_id, label_id)? {
                                Some(edge) => (edge.id, edge.created_at),
                                None => {
                                    let id = next_edge_id;
                                    next_edge_id = next_edge_id.checked_add(1).ok_or_else(|| {
                                        EngineError::InvalidOperation(
                                            "edge id counter overflow".into(),
                                        )
                                    })?;
                                    (id, now)
                                }
                            }
                        }
                    } else {
                        self.validate_edge_triple_conflict(request, &mut cache, from_id, to_id, label_id)?;
                        let id = next_edge_id;
                        next_edge_id = next_edge_id.checked_add(1).ok_or_else(|| {
                            EngineError::InvalidOperation("edge id counter overflow".into())
                        })?;
                        (id, now)
                    };
                    state.edges_by_triple.insert(triple, (id, created_at));
                    state.deleted_edge_ids.remove(&id);
                    if let Some(local) = &entry.produced_edge {
                        state.local_edge_ids.insert(local.clone(), id);
                    }
                    let edge = EdgeRecord {
                        id,
                        from: from_id,
                        to: to_id,
                        label_id,
                        props: options.props.clone(),
                        created_at,
                        updated_at: now,
                        weight: options.weight,
                        valid_from: options.valid_from.unwrap_or(created_at),
                        valid_to: options.valid_to.unwrap_or(i64::MAX),
                        last_write_seq: 0,
                    };
                    state.edge_id_to_triple.insert(id, triple);
                    state.edge_records_by_id.insert(id, edge.clone());
                    graph_ops.reserve(1)?;
                    ops.push(WalOp::UpsertEdge(edge));
                    state.edge_ids.push(id);
                }
                TxnIntent::DeleteNode { target } => {
                    let Some(id) =
                        self.resolve_node_ref_optional(target, &state, request, &label_resolution, &mut cache)?
                    else {
                        continue;
                    };
                    self.validate_node_id_conflict(&mut cache, id, request.snapshot_seq)?;
                    graph_ops.reserve(1)?;
                    let incident_scan_limit =
                        graph_ops.limited_scan_len(state.deleted_edge_ids.len());
                    let snapshot_incident = request
                        .snapshot
                        .txn_delete_incident_edge_ids_limited(&[id], incident_scan_limit)?;
                    graph_ops
                        .reject_if_limited_scan_filled(snapshot_incident.len(), incident_scan_limit)?;
                    for edge_id in snapshot_incident {
                        self.validate_edge_id_conflict(&mut cache, edge_id, request.snapshot_seq)?;
                    }
                    let incident =
                        self.incident_edge_ids_for_txn_delete_limited(id, incident_scan_limit)?;
                    graph_ops.reject_if_limited_scan_filled(incident.len(), incident_scan_limit)?;
                    for edge_id in incident {
                        self.validate_edge_id_conflict(&mut cache, edge_id, request.snapshot_seq)?;
                        if state.deleted_edge_ids.insert(edge_id) {
                            graph_ops.reserve(1)?;
                            ops.push(WalOp::DeleteEdge {
                                id: edge_id,
                                deleted_at: now,
                            });
                        }
                    }
                    let planned_incident: Vec<u64> = state
                        .edge_records_by_id
                        .values()
                        .filter(|edge| edge.from == id || edge.to == id)
                        .map(|edge| edge.id)
                        .collect();
                    for edge_id in planned_incident {
                        if state.deleted_edge_ids.insert(edge_id) {
                            graph_ops.reserve(1)?;
                            if let Some(triple) = state.edge_id_to_triple.remove(&edge_id) {
                                remove_planned_edge_triple_if_current(&mut state, triple, edge_id);
                            }
                            state.edge_records_by_id.remove(&edge_id);
                            ops.push(WalOp::DeleteEdge {
                                id: edge_id,
                                deleted_at: now,
                            });
                        }
                    }
                    state.deleted_node_ids.insert(id);
                    ops.push(WalOp::DeleteNode {
                        id,
                        deleted_at: now,
                    });
                }
                TxnIntent::DeleteEdge { target } => {
                    let Some(id) =
                        self.resolve_edge_ref_optional(target, &state, request, &label_resolution, &mut cache)?
                    else {
                        continue;
                    };
                    self.validate_edge_id_conflict(&mut cache, id, request.snapshot_seq)?;
                    if state.deleted_edge_ids.insert(id) {
                        graph_ops.reserve(1)?;
                        if let Some(triple) = state.edge_id_to_triple.remove(&id) {
                            remove_planned_edge_triple_if_current(&mut state, triple, id);
                        }
                        state.edge_records_by_id.remove(&id);
                        ops.push(WalOp::DeleteEdge {
                            id,
                            deleted_at: now,
                        });
                    }
                }
                TxnIntent::InvalidateEdge { target, valid_to } => {
                    let Some(id) =
                        self.resolve_edge_ref_optional(target, &state, request, &label_resolution, &mut cache)?
                    else {
                        continue;
                    };
                    if state.deleted_edge_ids.contains(&id) {
                        continue;
                    }
                    if let Some(edge) = state.edge_records_by_id.get(&id).cloned() {
                        let updated = EdgeRecord {
                            updated_at: now,
                            valid_to: *valid_to,
                            ..edge
                            };
                            let triple = (updated.from, updated.to, updated.label_id);
                            set_planned_edge_triple_if_current_or_absent(
                                &mut state,
                            triple,
                            id,
                            updated.created_at,
                            );
                            state.edge_id_to_triple.insert(id, triple);
                            state.edge_records_by_id.insert(id, updated.clone());
                            graph_ops.reserve(1)?;
                            ops.push(WalOp::UpsertEdge(updated));
                        } else {
                            self.validate_edge_id_conflict(&mut cache, id, request.snapshot_seq)?;
                        if let Some(edge) = self.cached_current_edge(&mut cache, id)? {
                            let updated = EdgeRecord {
                                updated_at: now,
                                valid_to: *valid_to,
                                ..edge
                            };
                            let triple = (updated.from, updated.to, updated.label_id);
                            let current_triple =
                                self.cached_current_edge_triple(&mut cache, updated.from, updated.to, updated.label_id)?;
                            if current_triple.as_ref().is_none_or(|edge| edge.id == id) {
                                state
                                    .edges_by_triple
                                    .insert(triple, (id, updated.created_at));
                            }
                            state.edge_id_to_triple.insert(id, triple);
                            state.edge_records_by_id.insert(id, updated.clone());
                            graph_ops.reserve(1)?;
                            ops.push(WalOp::UpsertEdge(updated));
                        }
                    }
                }
            }
        }

        self.prepare_txn_node_replacement_key_state(
            request,
            &label_resolution,
            &mut cache,
            &mut state,
        )?;
        let replacement_order =
            self.txn_replacement_planning_order(request, &label_resolution, &mut cache, &state)?;

        for replacement_index in replacement_order {
            let replacement = &request.record_replacements[replacement_index];
            match replacement {
                TxnRecordReplacement::Node(node) => self.plan_txn_node_record_replacement(
                    request,
                    &label_resolution,
                    &mut cache,
                    &mut state,
                    &mut graph_ops,
                    &mut ops,
                    node,
                    now,
                )?,
                TxnRecordReplacement::Edge(edge) => self.plan_txn_edge_record_replacement(
                    request,
                    &label_resolution,
                    &mut cache,
                    &mut state,
                    &mut graph_ops,
                    &mut ops,
                    edge,
                    now,
                )?,
            }
        }

        self.validate_gql_return_read_set(request, &state)?;

        self.next_node_id = next_node_id;
        self.next_edge_id = next_edge_id;

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::TxnCommitResult(TxnCommitResult {
                node_ids: state.node_ids,
                edge_ids: state.edge_ids,
                local_node_ids: state.local_node_ids,
                local_edge_ids: state.local_edge_ids,
            }),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn prepare_txn_node_replacement_key_state(
        &self,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
        state: &mut PlannedTxnState,
    ) -> Result<(), EngineError> {
        let mut final_node_key_owners: HashMap<(u32, String), u64> = HashMap::new();
        for replacement in &request.record_replacements {
            let TxnRecordReplacement::Node(replacement) = replacement else {
                continue;
            };
            if state.deleted_node_ids.contains(&replacement.id) {
                return Err(EngineError::InvalidOperation(format!(
                    "transaction node {} was deleted earlier in the transaction",
                    replacement.id
                )));
            }
            let begin = self
                .cached_begin_node(request, cache, replacement.id)?
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "transaction node {} does not exist in the transaction snapshot",
                        replacement.id
                    ))
                })?;
            self.validate_node_id_conflict(cache, replacement.id, request.snapshot_seq)?;
            if begin.key != replacement.key {
                return Err(EngineError::InvalidOperation(format!(
                    "transaction node {} replacement cannot change key",
                    replacement.id
                )));
            }
            if begin.created_at != replacement.created_at {
                return Err(EngineError::InvalidOperation(format!(
                    "transaction node {} replacement cannot change created_at",
                    replacement.id
                )));
            }

            let label_ids = resolve_txn_node_replacement_label_ids(label_resolution, replacement)?;
            let previous_labels = state
                .node_records_by_id
                .get(&replacement.id)
                .map(|node| node.label_ids)
                .unwrap_or(begin.label_ids);
            for &old_label_id in previous_labels.as_slice() {
                if !label_ids.contains(old_label_id) {
                    let key_tuple = (old_label_id, replacement.key.clone());
                    if state
                        .nodes_by_key
                        .get(&key_tuple)
                        .is_some_and(|(id, _)| *id == replacement.id)
                    {
                        state.nodes_by_key.remove(&key_tuple);
                    }
                    state.removed_node_keys.insert(key_tuple);
                }
            }
            for &label_id in label_ids.as_slice() {
                let key_tuple = (label_id, replacement.key.clone());
                if let Some(other_id) =
                    final_node_key_owners.insert(key_tuple, replacement.id)
                {
                    if other_id != replacement.id {
                        return Err(node_key_conflict_error(
                            &replacement.key,
                            other_id,
                            replacement.id,
                        ));
                    }
                }
            }
        }

        for ((label_id, key), replacement_id) in final_node_key_owners {
            let key_tuple = (label_id, key.clone());
            if let Some(&(other_id, _)) = state.nodes_by_key.get(&key_tuple) {
                if other_id != replacement_id {
                    return Err(node_key_conflict_error(&key, other_id, replacement_id));
                }
            } else if !state.removed_node_keys.contains(&key_tuple) {
                self.validate_node_key_conflict(request, cache, label_id, &key)?;
                if let Some(current) = self.cached_current_node_key(cache, label_id, &key)? {
                    if current.id != replacement_id {
                        return Err(node_key_conflict_error(
                            &key,
                            current.id,
                            replacement_id,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn txn_replacement_planning_order(
        &self,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
        state: &PlannedTxnState,
    ) -> Result<Vec<usize>, EngineError> {
        let mut added_keys = vec![HashSet::<(u32, String)>::new(); request.record_replacements.len()];
        let mut key_removers: HashMap<(u32, String), Vec<usize>> = HashMap::new();
        for (idx, replacement) in request.record_replacements.iter().enumerate() {
            let TxnRecordReplacement::Node(replacement) = replacement else {
                continue;
            };
            let begin = self
                .cached_begin_node(request, cache, replacement.id)?
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "transaction node {} does not exist in the transaction snapshot",
                        replacement.id
                    ))
                })?;
            let label_ids = resolve_txn_node_replacement_label_ids(label_resolution, replacement)?;
            let previous_labels = state
                .node_records_by_id
                .get(&replacement.id)
                .map(|node| node.label_ids)
                .unwrap_or(begin.label_ids);
            for &old_label_id in previous_labels.as_slice() {
                if !label_ids.contains(old_label_id) {
                    key_removers
                        .entry((old_label_id, replacement.key.clone()))
                        .or_default()
                        .push(idx);
                }
            }
            for &new_label_id in label_ids.as_slice() {
                if !previous_labels.contains(new_label_id) {
                    added_keys[idx].insert((new_label_id, replacement.key.clone()));
                }
            }
        }

        let mut prerequisites = vec![HashSet::<usize>::new(); request.record_replacements.len()];
        for (added_idx, keys) in added_keys.iter().enumerate() {
            for key in keys {
                let Some(removers) = key_removers.get(key) else {
                    continue;
                };
                for &removed_idx in removers {
                    if removed_idx != added_idx {
                        prerequisites[added_idx].insert(removed_idx);
                    }
                }
            }
        }

        let mut emitted = vec![false; request.record_replacements.len()];
        let mut order = Vec::with_capacity(request.record_replacements.len());
        while order.len() < request.record_replacements.len() {
            let mut progressed = false;
            for idx in 0..request.record_replacements.len() {
                if emitted[idx] {
                    continue;
                }
                if prerequisites[idx].iter().all(|&dependency| emitted[dependency]) {
                    emitted[idx] = true;
                    order.push(idx);
                    progressed = true;
                    break;
                }
            }
            if !progressed {
                return Err(EngineError::InvalidOperation(
                    "cyclic node label/key replacements cannot be planned safely in one transaction"
                        .to_string(),
                ));
            }
        }
        Ok(order)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_txn_node_record_replacement(
        &self,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
        state: &mut PlannedTxnState,
        graph_ops: &mut TxnGraphOpCounter,
        ops: &mut Vec<WalOp>,
        replacement: &TxnNodeRecordReplacement,
        now: i64,
    ) -> Result<(), EngineError> {
        if state.deleted_node_ids.contains(&replacement.id) {
            return Err(EngineError::InvalidOperation(format!(
                "transaction node {} was deleted earlier in the transaction",
                replacement.id
            )));
        }
        let begin = self
            .cached_begin_node(request, cache, replacement.id)?
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "transaction node {} does not exist in the transaction snapshot",
                    replacement.id
                ))
            })?;
        self.validate_node_id_conflict(cache, replacement.id, request.snapshot_seq)?;
        if begin.key != replacement.key {
            return Err(EngineError::InvalidOperation(format!(
                "transaction node {} replacement cannot change key",
                replacement.id
            )));
        }
        if begin.created_at != replacement.created_at {
            return Err(EngineError::InvalidOperation(format!(
                "transaction node {} replacement cannot change created_at",
                replacement.id
            )));
        }

        let label_ids = resolve_txn_node_replacement_label_ids(label_resolution, replacement)?;

        let previous_labels = state
            .node_records_by_id
            .get(&replacement.id)
            .map(|node| node.label_ids)
            .unwrap_or(begin.label_ids);
        for &old_label_id in previous_labels.as_slice() {
            if !label_ids.contains(old_label_id) {
                let key_tuple = (old_label_id, replacement.key.clone());
                if state
                    .nodes_by_key
                    .get(&key_tuple)
                    .is_some_and(|(id, _)| *id == replacement.id)
                {
                    state.nodes_by_key.remove(&key_tuple);
                }
                state.removed_node_keys.insert(key_tuple);
            }
        }

        for &label_id in label_ids.as_slice() {
            let key_tuple = (label_id, replacement.key.clone());
            if let Some(&(other_id, _)) = state.nodes_by_key.get(&key_tuple) {
                if other_id != replacement.id {
                    return Err(node_key_conflict_error(
                        &replacement.key,
                        other_id,
                        replacement.id,
                    ));
                }
            } else if !state.removed_node_keys.contains(&key_tuple) {
                self.validate_node_key_conflict(
                    request,
                    cache,
                    label_id,
                    &replacement.key,
                )?;
                if let Some(current) =
                    self.cached_current_node_key(cache, label_id, &replacement.key)?
                {
                    if current.id != replacement.id {
                        return Err(node_key_conflict_error(
                            &replacement.key,
                            current.id,
                            replacement.id,
                        ));
                    }
                }
            }
            state.removed_node_keys.remove(&key_tuple);
            state
                .nodes_by_key
                .insert(key_tuple, (replacement.id, replacement.created_at));
        }

        let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
            self.manifest.dense_vector.as_ref(),
            replacement.dense_vector.as_ref(),
            replacement.sparse_vector.as_ref(),
        )?;
        let node = NodeRecord {
            id: replacement.id,
            label_ids,
            key: replacement.key.clone(),
            props: replacement.props.clone(),
            created_at: replacement.created_at,
            updated_at: now,
            weight: replacement.weight,
            dense_vector,
            sparse_vector,
            last_write_seq: 0,
        };
        state.deleted_node_ids.remove(&replacement.id);
        state.node_records_by_id.insert(replacement.id, node.clone());
        graph_ops.reserve(1)?;
        ops.push(WalOp::UpsertNode(node));
        state.node_ids.push(replacement.id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_txn_edge_record_replacement(
        &self,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
        state: &mut PlannedTxnState,
        graph_ops: &mut TxnGraphOpCounter,
        ops: &mut Vec<WalOp>,
        replacement: &TxnEdgeRecordReplacement,
        now: i64,
    ) -> Result<(), EngineError> {
        if replacement.valid_from >= replacement.valid_to {
            return Err(EngineError::InvalidOperation(
                "transaction edge replacement requires valid_from < valid_to".to_string(),
            ));
        }
        if state.deleted_edge_ids.contains(&replacement.id) {
            return Err(EngineError::InvalidOperation(format!(
                "transaction edge {} was deleted earlier in the transaction",
                replacement.id
            )));
        }
        let begin = self
            .cached_begin_edge(request, cache, replacement.id)?
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "transaction edge {} does not exist in the transaction snapshot",
                    replacement.id
                ))
            })?;
        self.validate_edge_id_conflict(cache, replacement.id, request.snapshot_seq)?;
        let label_id = label_resolution.edge_label_id(&replacement.label).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "transaction edge label '{}' was not resolved for commit",
                replacement.label
            ))
        })?;
        if begin.from != replacement.from
            || begin.to != replacement.to
            || begin.label_id != label_id
        {
            return Err(EngineError::InvalidOperation(format!(
                "transaction edge {} replacement cannot change endpoints or label",
                replacement.id
            )));
        }
        if begin.created_at != replacement.created_at {
            return Err(EngineError::InvalidOperation(format!(
                "transaction edge {} replacement cannot change created_at",
                replacement.id
            )));
        }
        self.validate_node_id_conflict(cache, replacement.from, request.snapshot_seq)?;
        self.validate_node_id_conflict(cache, replacement.to, request.snapshot_seq)?;

        let triple = (replacement.from, replacement.to, label_id);
        if let Some(&(other_id, _)) = state.edges_by_triple.get(&triple) {
            if self.edge_uniqueness && other_id != replacement.id {
                return Err(EngineError::InvalidOperation(format!(
                    "edge triple ({}, {}, {}) conflicts with staged edge {}",
                    replacement.from, replacement.to, label_id, other_id
                )));
            }
        }
        set_planned_edge_triple_if_current_or_absent(
            state,
            triple,
            replacement.id,
            replacement.created_at,
        );
        state.deleted_edge_ids.remove(&replacement.id);
        let edge = EdgeRecord {
            id: replacement.id,
            from: replacement.from,
            to: replacement.to,
            label_id,
            props: replacement.props.clone(),
            created_at: replacement.created_at,
            updated_at: now,
            weight: replacement.weight,
            valid_from: replacement.valid_from,
            valid_to: replacement.valid_to,
            last_write_seq: 0,
        };
        state.edge_id_to_triple.insert(replacement.id, triple);
        state.edge_records_by_id.insert(replacement.id, edge.clone());
        graph_ops.reserve(1)?;
        ops.push(WalOp::UpsertEdge(edge));
        state.edge_ids.push(replacement.id);
        Ok(())
    }

    fn resolve_txn_label_names(
        &self,
        request: &TxnCommitRequest,
    ) -> Result<(TxnLabelResolution, Vec<WalOp>, bool), EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let mut resolution = TxnLabelResolution::default();

        let mut write_node_labels = Vec::new();
        let mut seen_write_node_labels = HashSet::new();
        let mut write_edge_labels = Vec::new();
        let mut seen_write_edge_labels = HashSet::new();

        for entry in &request.entries {
            match &entry.intent {
                TxnIntent::UpsertNode { labels, key, .. } => {
                    validate_node_key_for_write(key)?;
                    let labels = ValidatedNodeLabelList::new(labels.iter().map(String::as_str))?;
                    for &label in labels.as_slice() {
                        if seen_write_node_labels.insert(label) {
                            write_node_labels.push(label);
                        }
                    }
                }
                TxnIntent::UpsertEdge { label, .. } => {
                    if seen_write_edge_labels.insert(label.as_str()) {
                        write_edge_labels.push(label.as_str());
                    }
                }
                TxnIntent::DeleteNode { .. }
                | TxnIntent::DeleteEdge { .. }
                | TxnIntent::InvalidateEdge { .. } => {}
            }
        }
        for replacement in &request.record_replacements {
            if let TxnRecordReplacement::Node(node) = replacement {
                validate_node_key_for_write(&node.key)?;
                let labels =
                    ValidatedNodeLabelList::new(node.labels.iter().map(String::as_str))?;
                for &label in labels.as_slice() {
                    if seen_write_node_labels.insert(label) {
                        write_node_labels.push(label);
                    }
                }
            }
        }

        for label in write_node_labels {
            let label_id = label_plan.resolve_node_label_for_write(label)?;
            resolution.insert_node_label(label.to_string(), Some(label_id));
        }
        for label in write_edge_labels {
            let label_id = label_plan.resolve_edge_label_for_write(label)?;
            resolution.insert_edge_label(label.to_string(), Some(label_id));
        }

        let mut read_node_labels = Vec::new();
        let mut seen_read_node_labels = HashSet::new();
        let mut read_edge_labels = Vec::new();
        let mut seen_read_edge_labels = HashSet::new();
        for entry in &request.entries {
            collect_txn_intent_read_label_names(
                &entry.intent,
                &mut read_node_labels,
                &mut seen_read_node_labels,
                &mut read_edge_labels,
                &mut seen_read_edge_labels,
            );
        }
        for replacement in &request.record_replacements {
            collect_txn_replacement_read_label_names(
                replacement,
                &mut read_node_labels,
                &mut seen_read_node_labels,
                &mut read_edge_labels,
                &mut seen_read_edge_labels,
            );
        }

        for label in read_node_labels {
            if resolution.node_labels.contains_key(label) {
                continue;
            }
            let label_id = resolve_node_label_for_read(&catalog, label)?;
            resolution.insert_node_label(label.to_string(), label_id);
        }
        for label in read_edge_labels {
            if resolution.edge_labels.contains_key(label) {
                continue;
            }
            let label_id = resolve_edge_label_for_read(&catalog, label)?;
            resolution.insert_edge_label(label.to_string(), label_id);
        }

        let token_op_count = label_plan.token_op_count();
        let mut ops = Vec::with_capacity(token_op_count + request.entries.len());
        label_plan.push_token_ops(&mut ops);
        Ok((resolution, ops, token_op_count > 0))
    }

    fn build_txn_planning_cache(
        &self,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
    ) -> Result<TxnPlanningCache, EngineError> {
        let mut node_keys = HashSet::new();
        let mut node_ids = NodeIdSet::default();
        let mut edge_ids = NodeIdSet::default();
        for entry in &request.entries {
            collect_txn_intent_cache_targets(
                &entry.intent,
                label_resolution,
                &mut node_keys,
                &mut node_ids,
                &mut edge_ids,
            );
        }
        for replacement in &request.record_replacements {
            collect_txn_replacement_cache_targets(
                replacement,
                label_resolution,
                &mut node_keys,
                &mut node_ids,
                &mut edge_ids,
            );
        }

        let mut cache = TxnPlanningCache::default();
        let node_keys: Vec<(u32, String)> = node_keys.into_iter().collect();
        if !node_keys.is_empty() {
            let key_refs: Vec<(u32, &str)> = node_keys
                .iter()
                .map(|(label_id, key)| (*label_id, key.as_str()))
                .collect();
            let begin_nodes = request.snapshot.get_nodes_by_label_keys_raw(&key_refs)?;
            let current_nodes = self.get_nodes_by_label_keys_raw(&key_refs)?;
            for ((label_id, key), node) in node_keys.iter().cloned().zip(begin_nodes) {
                cache.begin_node_keys.insert((label_id, key), node);
            }
            for ((label_id, key), node) in node_keys.into_iter().zip(current_nodes) {
                cache.current_node_keys.insert((label_id, key), node);
            }
        }

        let node_ids: Vec<u64> = node_ids.into_iter().collect();
        if !node_ids.is_empty() {
            let begin_nodes = request.snapshot.get_nodes_raw(&node_ids)?;
            for (id, node) in node_ids.iter().copied().zip(begin_nodes) {
                cache.begin_nodes_by_id.insert(id, node);
            }
            for id in node_ids {
                let opinion = self.node_id_opinion(id)?;
                cache.node_opinions_by_id.insert(id, opinion);
            }
        }

        let edge_ids: Vec<u64> = edge_ids.into_iter().collect();
        if !edge_ids.is_empty() {
            let begin_edges = request.snapshot.get_edges(&edge_ids)?;
            for (id, edge) in edge_ids.iter().copied().zip(begin_edges) {
                cache.begin_edges_by_id.insert(id, edge);
            }
            let current_edges = self.get_edges(&edge_ids)?;
            for (id, edge) in edge_ids.into_iter().zip(current_edges) {
                cache.current_edges_by_id.insert(id, edge);
            }
        }

        Ok(cache)
    }

    fn cached_begin_node(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        id: u64,
    ) -> Result<Option<NodeRecord>, EngineError> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.begin_nodes_by_id.entry(id)
        {
            let mut nodes = request.snapshot.get_nodes_raw(&[id])?;
            entry.insert(nodes.pop().unwrap_or(None));
        }
        Ok(cache.begin_nodes_by_id.get(&id).cloned().flatten())
    }

    fn cached_begin_edge(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        id: u64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.begin_edges_by_id.entry(id)
        {
            let mut edges = request.snapshot.get_edges(&[id])?;
            entry.insert(edges.pop().unwrap_or(None));
        }
        Ok(cache.begin_edges_by_id.get(&id).cloned().flatten())
    }

    fn cached_begin_node_key(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let cache_key = (label_id, key.to_string());
        if !cache.begin_node_keys.contains_key(&cache_key) {
            let node = request.snapshot.get_node_by_label_key_raw(label_id, key)?;
            cache.begin_node_keys.insert(cache_key.clone(), node);
        }
        Ok(cache.begin_node_keys.get(&cache_key).cloned().flatten())
    }

    fn cached_current_node_key(
        &self,
        cache: &mut TxnPlanningCache,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let cache_key = (label_id, key.to_string());
        if !cache.current_node_keys.contains_key(&cache_key) {
            let node = self.get_node_by_label_key_raw(label_id, key)?;
            cache.current_node_keys.insert(cache_key.clone(), node);
        }
        Ok(cache.current_node_keys.get(&cache_key).cloned().flatten())
    }

    fn cached_begin_edge_triple(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let key = (from, to, label_id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.begin_edge_triples.entry(key)
        {
            let edge = request.snapshot.get_edge_by_triple(from, to, label_id)?;
            entry.insert(edge);
        }
        Ok(cache.begin_edge_triples.get(&key).cloned().flatten())
    }

    fn cached_current_edge_triple(
        &self,
        cache: &mut TxnPlanningCache,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let key = (from, to, label_id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.current_edge_triples.entry(key)
        {
            let edge = self.get_edge_by_triple(from, to, label_id)?;
            entry.insert(edge);
        }
        Ok(cache.current_edge_triples.get(&key).cloned().flatten())
    }

    fn cached_current_edge(
        &self,
        cache: &mut TxnPlanningCache,
        id: u64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.current_edges_by_id.entry(id)
        {
            let mut edges = self.get_edges(&[id])?;
            let edge = edges.pop().unwrap_or(None);
            entry.insert(edge);
        }
        Ok(cache.current_edges_by_id.get(&id).cloned().flatten())
    }

    fn cached_node_id_opinion(
        &self,
        cache: &mut TxnPlanningCache,
        id: u64,
    ) -> Result<TargetOpinion, EngineError> {
        if let Some(opinion) = cache.node_opinions_by_id.get(&id).copied() {
            return Ok(opinion);
        }
        let opinion = self.node_id_opinion(id)?;
        cache.node_opinions_by_id.insert(id, opinion);
        Ok(opinion)
    }

    fn cached_edge_id_opinion(
        &self,
        cache: &mut TxnPlanningCache,
        id: u64,
    ) -> Result<TargetOpinion, EngineError> {
        if let Some(opinion) = cache.edge_opinions_by_id.get(&id).copied() {
            return Ok(opinion);
        }
        let opinion = self.edge_id_opinion(id)?;
        cache.edge_opinions_by_id.insert(id, opinion);
        Ok(opinion)
    }

    fn validate_node_key_conflict(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        label_id: u32,
        key: &str,
    ) -> Result<(), EngineError> {
        let current = self.cached_current_node_key(cache, label_id, key)?;
        if let Some(current) = current.as_ref() {
            if current.last_write_seq <= request.snapshot_seq {
                return Ok(());
            }
        }

        let begin = self.cached_begin_node_key(request, cache, label_id, key)?;
        match (begin, current) {
            (Some(begin), Some(current)) if begin.id == current.id => {
                self.validate_node_id_conflict(cache, begin.id, request.snapshot_seq)
            }
            (Some(begin), _) => {
                self.validate_node_id_conflict(cache, begin.id, request.snapshot_seq)?;
                Err(EngineError::TxnConflict(format!(
                    "node key ({}, {}) changed after transaction begin",
                    label_id, key
                )))
            }
            (None, Some(current)) => {
                if current.last_write_seq > request.snapshot_seq {
                    Err(EngineError::TxnConflict(format!(
                        "node key ({}, {}) appeared after transaction begin",
                        label_id, key
                    )))
                } else {
                    Ok(())
                }
            }
            (None, None) => Ok(()),
        }
    }

    fn incident_edge_ids_for_txn_delete_limited(
        &self,
        node_id: u64,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let ids = self
            .sources()
            .edge_ids_by_endpoints_limited(&[node_id], Direction::Both, None, limit)?;
        #[cfg(debug_assertions)]
        {
            if limit == usize::MAX {
                let active_edge_ids = self
                    .memtable
                    .incident_edges_at(node_id, Direction::Both, None, self.engine_seq)
                    .into_iter()
                    .map(|entry| entry.edge_id)
                    .collect::<IdSet>();
                debug_assert!(
                    active_edge_ids.iter().all(|edge_id| ids.contains(edge_id)),
                    "source-list transaction delete helper must include active memtable incident edges"
                );
            }
        }
        Ok(ids)
    }

    fn validate_edge_triple_conflict(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<(), EngineError> {
        let current = self.cached_current_edge_triple(cache, from, to, label_id)?;
        if let Some(current) = current.as_ref() {
            if current.last_write_seq <= request.snapshot_seq {
                return Ok(());
            }
        }

        let begin = self.cached_begin_edge_triple(request, cache, from, to, label_id)?;
        match (begin, current) {
            (Some(begin), Some(current)) if begin.id == current.id => {
                self.validate_edge_id_conflict(cache, begin.id, request.snapshot_seq)
            }
            (Some(begin), _) => {
                self.validate_edge_id_conflict(cache, begin.id, request.snapshot_seq)?;
                Err(EngineError::TxnConflict(format!(
                    "edge triple ({}, {}, {}) changed after transaction begin",
                    from, to, label_id
                )))
            }
            (None, Some(current)) => {
                if current.last_write_seq > request.snapshot_seq {
                    Err(EngineError::TxnConflict(format!(
                        "edge triple ({}, {}, {}) appeared after transaction begin",
                        from, to, label_id
                    )))
                } else {
                    Ok(())
                }
            }
            (None, None) => Ok(()),
        }
    }

    fn validate_node_id_conflict(
        &self,
        cache: &mut TxnPlanningCache,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<(), EngineError> {
        if self
            .cached_node_id_opinion(cache, id)?
            .last_write_seq()
            .is_some_and(|seq| seq > snapshot_seq)
        {
            Err(EngineError::TxnConflict(format!(
                "node {} changed after transaction begin",
                id
            )))
        } else {
            Ok(())
        }
    }

    fn validate_edge_id_conflict(
        &self,
        cache: &mut TxnPlanningCache,
        id: u64,
        snapshot_seq: u64,
    ) -> Result<(), EngineError> {
        if self
            .cached_edge_id_opinion(cache, id)?
            .last_write_seq()
            .is_some_and(|seq| seq > snapshot_seq)
        {
            Err(EngineError::TxnConflict(format!(
                "edge {} changed after transaction begin",
                id
            )))
        } else {
            Ok(())
        }
    }

    fn validate_gql_return_read_set(
        &self,
        request: &TxnCommitRequest,
        state: &PlannedTxnState,
    ) -> Result<(), EngineError> {
        let node_ids = request
            .gql_return_read_set
            .node_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let current_nodes = self.get_nodes_raw(&node_ids)?;
        for (id, node) in node_ids.into_iter().zip(current_nodes) {
            if state.deleted_node_ids.contains(&id) {
                return Err(EngineError::TxnConflict(format!(
                    "node {id} was deleted before GQL mutation RETURN projection"
                )));
            }
            let Some(node) = node else {
                return Err(EngineError::TxnConflict(format!(
                    "node {id} was deleted before GQL mutation RETURN projection"
                )));
            };
            if node.last_write_seq > request.snapshot_seq {
                return Err(EngineError::TxnConflict(format!(
                    "node {id} changed after transaction begin"
                )));
            }
        }

        let edge_ids = request
            .gql_return_read_set
            .edge_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let current_edges = self.get_edges(&edge_ids)?;
        for (id, edge) in edge_ids.into_iter().zip(current_edges) {
            if state.deleted_edge_ids.contains(&id) {
                return Err(EngineError::TxnConflict(format!(
                    "edge {id} was deleted before GQL mutation RETURN projection"
                )));
            }
            let Some(edge) = edge else {
                return Err(EngineError::TxnConflict(format!(
                    "edge {id} was deleted before GQL mutation RETURN projection"
                )));
            };
            if edge.last_write_seq > request.snapshot_seq {
                return Err(EngineError::TxnConflict(format!(
                    "edge {id} changed after transaction begin"
                )));
            }
        }
        Ok(())
    }

    fn resolve_node_ref_required(
        &self,
        target: &TxnNodeRef,
        state: &PlannedTxnState,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
    ) -> Result<u64, EngineError> {
        let id = self
            .resolve_node_ref_optional(target, state, request, label_resolution, cache)?
            .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "transaction node ref {:?} does not resolve to an existing or staged node",
                target
            ))
        })?;
        if state.deleted_node_ids.contains(&id) {
            return Err(EngineError::InvalidOperation(format!(
                "transaction node ref {:?} resolves to a node deleted earlier in the transaction",
                target
            )));
        }
        Ok(id)
    }

    fn resolve_node_ref_optional(
        &self,
        target: &TxnNodeRef,
        state: &PlannedTxnState,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
    ) -> Result<Option<u64>, EngineError> {
        match target {
            TxnNodeRef::Id(id) => Ok(Some(*id)),
            TxnNodeRef::Local(local) => state
                .local_node_ids
                .get(local)
                .copied()
                .map(Some)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "unknown transaction node local ref {:?}",
                        local
                    ))
            }),
            TxnNodeRef::Key { label, key } => {
                let Some(label_id) = label_resolution.node_label_id(label) else {
                    return Ok(None);
                };
                let key_tuple = (label_id, key.clone());
                if let Some(&(id, _)) = state.nodes_by_key.get(&key_tuple) {
                    Ok(Some(id))
                } else if state.removed_node_keys.contains(&key_tuple) {
                    Ok(None)
                } else {
                    let current = self
                        .cached_current_node_key(cache, label_id, key)?
                        .map(|node| node.id);
                    if current.is_some() {
                        Ok(current)
                    } else {
                        Ok(self
                            .cached_begin_node_key(request, cache, label_id, key)?
                            .map(|node| node.id))
                    }
                }
            }
        }
    }

    fn resolve_edge_ref_optional(
        &self,
        target: &TxnEdgeRef,
        state: &PlannedTxnState,
        request: &TxnCommitRequest,
        label_resolution: &TxnLabelResolution,
        cache: &mut TxnPlanningCache,
    ) -> Result<Option<u64>, EngineError> {
        match target {
            TxnEdgeRef::Id(id) => Ok(Some(*id)),
            TxnEdgeRef::Local(local) => state
                .local_edge_ids
                .get(local)
                .copied()
                .map(Some)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "unknown transaction edge local ref {:?}",
                        local
                    ))
                }),
            TxnEdgeRef::Triple {
                from,
                to,
                label,
            } => {
                let Some(label_id) = label_resolution.edge_label_id(label) else {
                    return Ok(None);
                };
                let Some(from_id) =
                    self.resolve_node_ref_optional(from, state, request, label_resolution, cache)?
                else {
                    return Ok(None);
                };
                let Some(to_id) =
                    self.resolve_node_ref_optional(to, state, request, label_resolution, cache)?
                else {
                    return Ok(None);
                };
                if let Some(&(id, _)) = state.edges_by_triple.get(&(from_id, to_id, label_id)) {
                    Ok(Some(id))
                } else {
                    let current = self
                        .cached_current_edge_triple(cache, from_id, to_id, label_id)?
                        .map(|edge| edge.id);
                    if current.is_some() {
                        Ok(current)
                    } else {
                        Ok(self
                            .cached_begin_edge_triple(request, cache, from_id, to_id, label_id)?
                            .map(|edge| edge.id))
                    }
                }
            }
        }
    }

    fn node_id_opinion(&self, id: u64) -> Result<TargetOpinion, EngineError> {
        let snapshot_seq = self.engine_seq;
        if let Some(node) = self.memtable.get_node_at(id, snapshot_seq) {
            return Ok(TargetOpinion::Live {
                last_write_seq: node.last_write_seq,
            });
        }
        if let Some(tombstone) = self.memtable.node_tombstone_at(id, snapshot_seq) {
            return Ok(TargetOpinion::Tombstone {
                last_write_seq: tombstone.last_write_seq,
            });
        }
        for epoch in &self.immutable_epochs {
            if let Some(node) = epoch.memtable.get_node_at(id, snapshot_seq) {
                return Ok(TargetOpinion::Live {
                    last_write_seq: node.last_write_seq,
                });
            }
            if let Some(tombstone) = epoch.memtable.node_tombstone_at(id, snapshot_seq) {
                return Ok(TargetOpinion::Tombstone {
                    last_write_seq: tombstone.last_write_seq,
                });
            }
        }
        for seg in &self.segments {
            if let Some(tombstone) = seg.deleted_node_tombstones().get(&id) {
                return Ok(TargetOpinion::Tombstone {
                    last_write_seq: tombstone.last_write_seq,
                });
            }
            if let Some(node) = seg.get_node(id)? {
                return Ok(TargetOpinion::Live {
                    last_write_seq: node.last_write_seq,
                });
            }
        }
        Ok(TargetOpinion::Absent)
    }

    fn edge_id_opinion(&self, id: u64) -> Result<TargetOpinion, EngineError> {
        let snapshot_seq = self.engine_seq;
        if let Some(edge) = self.memtable.get_edge_at(id, snapshot_seq) {
            return Ok(TargetOpinion::Live {
                last_write_seq: edge.last_write_seq,
            });
        }
        if let Some(tombstone) = self.memtable.edge_tombstone_at(id, snapshot_seq) {
            return Ok(TargetOpinion::Tombstone {
                last_write_seq: tombstone.last_write_seq,
            });
        }
        for epoch in &self.immutable_epochs {
            if let Some(edge) = epoch.memtable.get_edge_at(id, snapshot_seq) {
                return Ok(TargetOpinion::Live {
                    last_write_seq: edge.last_write_seq,
                });
            }
            if let Some(tombstone) = epoch.memtable.edge_tombstone_at(id, snapshot_seq) {
                return Ok(TargetOpinion::Tombstone {
                    last_write_seq: tombstone.last_write_seq,
                });
            }
        }
        for seg in &self.segments {
            if let Some(tombstone) = seg.deleted_edge_tombstones().get(&id) {
                return Ok(TargetOpinion::Tombstone {
                    last_write_seq: tombstone.last_write_seq,
                });
            }
            if let Some(edge) = seg.get_edge(id)? {
                return Ok(TargetOpinion::Live {
                    last_write_seq: edge.last_write_seq,
                });
            }
        }
        Ok(TargetOpinion::Absent)
    }
}
