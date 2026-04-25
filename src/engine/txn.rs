#[derive(Clone)]
pub(crate) struct TxnCommitRequest {
    snapshot: Arc<ReadView>,
    snapshot_seq: u64,
    entries: Vec<StagedTxnIntent>,
}

#[derive(Clone)]
pub(crate) struct StagedTxnIntent {
    intent: TxnIntent,
    produced_node: Option<TxnLocalRef>,
    produced_edge: Option<TxnLocalRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TxnEndpointKey {
    Id(u64),
    Key(u32, String),
}

#[derive(Debug, Clone)]
enum NodeOverlayOpinion {
    Live(TxnNodeView),
    Deleted(Option<TxnNodeView>),
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
    node_key_locals: HashMap<(u32, String), Vec<TxnLocalRef>>,
    nodes_by_id: NodeIdMap<NodeOverlayOpinion>,
    nodes_by_key: HashMap<(u32, String), NodeOverlayOpinion>,
    deleted_node_ids_seen: NodeIdSet,
    edges_by_local: HashMap<TxnLocalRef, EdgeOverlayOpinion>,
    edge_triple_locals: HashMap<(TxnEndpointKey, TxnEndpointKey, u32), Vec<TxnLocalRef>>,
    edges_by_id: NodeIdMap<EdgeOverlayOpinion>,
    edges_by_triple: HashMap<(TxnEndpointKey, TxnEndpointKey, u32), EdgeOverlayOpinion>,
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
            overlay: TxnOverlay::new(published.edge_uniqueness),
            edge_uniqueness: published.edge_uniqueness,
            closed: false,
            next_slot: 0,
        })
    }
}

impl WriteTxn {
    pub fn upsert_node(
        &mut self,
        type_id: u32,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<TxnNodeRef, EngineError> {
        let local = self.next_slot_ref()?;
        let intent = TxnIntent::UpsertNode {
            alias: None,
            type_id,
            key: key.to_string(),
            options,
        };
        self.append_entry(intent, Some(local.clone()), None)?;
        self.advance_next_slot()?;
        Ok(TxnNodeRef::Local(local))
    }

    pub fn upsert_node_as(
        &mut self,
        alias: &str,
        type_id: u32,
        key: &str,
        options: UpsertNodeOptions,
    ) -> Result<TxnNodeRef, EngineError> {
        let local = TxnLocalRef::Alias(alias.to_string());
        let intent = TxnIntent::UpsertNode {
            alias: Some(alias.to_string()),
            type_id,
            key: key.to_string(),
            options,
        };
        self.append_entry(intent, Some(local.clone()), None)?;
        Ok(TxnNodeRef::Local(local))
    }

    pub fn upsert_edge(
        &mut self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        type_id: u32,
        options: UpsertEdgeOptions,
    ) -> Result<TxnEdgeRef, EngineError> {
        let local = self.next_slot_ref()?;
        let intent = TxnIntent::UpsertEdge {
            alias: None,
            from,
            to,
            type_id,
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
        type_id: u32,
        options: UpsertEdgeOptions,
    ) -> Result<TxnEdgeRef, EngineError> {
        let local = TxnLocalRef::Alias(alias.to_string());
        let intent = TxnIntent::UpsertEdge {
            alias: Some(alias.to_string()),
            from,
            to,
            type_id,
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
        type_id: u32,
        key: &str,
    ) -> Result<Option<TxnNodeView>, EngineError> {
        self.ensure_open()?;
        self.overlay
            .get_node(&self.snapshot, &TxnNodeRef::Key {
                type_id,
                key: key.to_string(),
            })
    }

    pub fn get_edge_by_triple(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        type_id: u32,
    ) -> Result<Option<TxnEdgeView>, EngineError> {
        self.ensure_open()?;
        self.overlay.get_edge(
            &self.snapshot,
            &TxnEdgeRef::Triple { from, to, type_id },
        )
    }

    pub fn commit(&mut self) -> Result<TxnCommitResult, EngineError> {
        self.ensure_open()?;
        self.closed = true;
        let request = TxnCommitRequest {
            snapshot: Arc::clone(&self.snapshot),
            snapshot_seq: self.snapshot_seq,
            entries: std::mem::take(&mut self.entries),
        };
        self.overlay = TxnOverlay::new(self.edge_uniqueness);
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::TxnCommit { request })?
        {
            CoreWriteReply::TxnCommitResult(result) => Ok(result),
            _ => unreachable!("txn commit must return transaction commit results"),
        }
    }

    pub fn rollback(&mut self) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.closed = true;
        self.entries.clear();
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
                type_id,
                key,
                options,
            } => self.apply_upsert_node(snapshot, alias, *type_id, key, options, produced_node),
            TxnIntent::UpsertEdge {
                alias,
                from,
                to,
                type_id,
                options,
            } => self.apply_upsert_edge(
                snapshot,
                alias,
                from,
                to,
                *type_id,
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
        type_id: u32,
        key: &str,
        options: &UpsertNodeOptions,
        produced: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
        if let Some(alias) = alias {
            if self.node_aliases.contains(alias) {
                return Err(EngineError::InvalidOperation(format!(
                    "duplicate transaction node alias '{}'",
                    alias
                )));
            }
        }

        let existing = match self.nodes_by_key.get(&(type_id, key.to_string())) {
            Some(NodeOverlayOpinion::Live(view)) => Some(view.clone()),
            Some(NodeOverlayOpinion::Deleted(view)) => view.clone(),
            None => snapshot.get_node_by_key(type_id, key)?.map(node_to_txn_view),
        };
        let view = TxnNodeView {
            id: existing.as_ref().and_then(|node| node.id),
            local: produced.clone(),
            type_id,
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
        self.insert_node_live(view, produced);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_upsert_edge(
        &mut self,
        snapshot: &ReadView,
        alias: &Option<String>,
        from: &TxnNodeRef,
        to: &TxnNodeRef,
        type_id: u32,
        options: &UpsertEdgeOptions,
        produced: Option<TxnLocalRef>,
    ) -> Result<(), EngineError> {
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
        let triple_key = (from_key, to_key, type_id);
        let existing = if self.edge_uniqueness {
            match self.edges_by_triple.get(&triple_key) {
                Some(EdgeOverlayOpinion::Live(view)) => Some(view.clone()),
                Some(EdgeOverlayOpinion::Deleted(view)) => view.clone(),
                None => {
                    let from_id = self.committed_node_id(snapshot, from)?;
                    let to_id = self.committed_node_id(snapshot, to)?;
                    match (from_id, to_id) {
                        (Some(from_id), Some(to_id)) => snapshot
                            .get_edge_by_triple(from_id, to_id, type_id)?
                            .map(edge_to_txn_view),
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
            from: from.clone(),
            to: to.clone(),
            type_id,
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
                None => Err(EngineError::InvalidOperation(format!(
                    "unknown transaction node local ref {:?}",
                    local
                ))),
            },
            TxnNodeRef::Id(id) => match self.nodes_by_id.get(id) {
                Some(NodeOverlayOpinion::Live(view)) => Ok(Some(view.clone())),
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                None => Ok(snapshot.get_node(*id)?.map(node_to_txn_view)),
            },
            TxnNodeRef::Key { type_id, key } => {
                match self.nodes_by_key.get(&(*type_id, key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => Ok(Some(view.clone())),
                    Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                    None => Ok(snapshot
                        .get_node_by_key(*type_id, key)?
                        .map(node_to_txn_view)),
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
            TxnEdgeRef::Triple { from, to, type_id } => {
                let Some(from_key) = self.read_endpoint_key(snapshot, from)? else {
                    return Ok(None);
                };
                let Some(to_key) = self.read_endpoint_key(snapshot, to)? else {
                    return Ok(None);
                };
                self.edges_by_triple.get(&(from_key, to_key, *type_id))
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
            TxnEdgeRef::Triple { from, to, type_id } => {
                let Some(from_id) = self.committed_node_id(snapshot, from)? else {
                    return Ok(None);
                };
                let Some(to_id) = self.committed_node_id(snapshot, to)? else {
                    return Ok(None);
                };
                snapshot.get_edge_by_triple(from_id, to_id, *type_id)?
            }
        };
        match edge {
            Some(edge) if !self.committed_edge_endpoint_deleted(&edge) => {
                Ok(Some(edge_to_txn_view(edge)))
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
            TxnNodeRef::Key { type_id, key } => {
                match self.nodes_by_key.get(&(*type_id, key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => {
                        if let Some(id) = view.id {
                            Ok(Some(TxnEndpointKey::Id(id)))
                        } else {
                            Ok(Some(TxnEndpointKey::Key(*type_id, key.clone())))
                        }
                    }
                    Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
                    None => match snapshot.get_node_by_key(*type_id, key)? {
                        Some(node) => Ok(Some(TxnEndpointKey::Id(node.id))),
                        None => Ok(Some(TxnEndpointKey::Key(*type_id, key.clone()))),
                    },
                }
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view)) => {
                    if let Some(id) = view.id {
                        Ok(Some(TxnEndpointKey::Id(id)))
                    } else {
                        Ok(Some(TxnEndpointKey::Key(view.type_id, view.key.clone())))
                    }
                }
                Some(NodeOverlayOpinion::Deleted(_)) => Ok(None),
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
            TxnNodeRef::Key { type_id, key } => {
                match self.nodes_by_key.get(&(*type_id, key.clone())) {
                    Some(NodeOverlayOpinion::Live(view)) => {
                        if let Some(id) = view.id {
                            Ok(TxnEndpointKey::Id(id))
                        } else {
                            Ok(TxnEndpointKey::Key(*type_id, key.clone()))
                        }
                    }
                    Some(NodeOverlayOpinion::Deleted(_)) => Err(EngineError::InvalidOperation(
                        format!("transaction node key ({}, {}) is deleted", type_id, key),
                    )),
                    None => match snapshot.get_node_by_key(*type_id, key)? {
                        Some(node) => Ok(TxnEndpointKey::Id(node.id)),
                        None => Ok(TxnEndpointKey::Key(*type_id, key.clone())),
                    },
                }
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view)) => {
                    if let Some(id) = view.id {
                        Ok(TxnEndpointKey::Id(id))
                    } else {
                        Ok(TxnEndpointKey::Key(view.type_id, view.key.clone()))
                    }
                }
                Some(NodeOverlayOpinion::Deleted(_)) => Err(EngineError::InvalidOperation(format!(
                    "transaction node local ref {:?} is deleted",
                    local
                ))),
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

    fn insert_node_live(&mut self, view: TxnNodeView, local: Option<TxnLocalRef>) {
        let key = (view.type_id, view.key.clone());
        if let Some(local) = local {
            let locals = self.node_key_locals.entry(key.clone()).or_default();
            if !locals.contains(&local) {
                locals.push(local);
            }
        }

        let opinion = NodeOverlayOpinion::Live(view.clone());
        self.nodes_by_key.insert(key.clone(), opinion.clone());
        if let Some(id) = view.id {
            self.nodes_by_id.insert(id, opinion);
        }
        self.set_node_locals_for_key(&key, NodeOverlayOpinion::Live(view));
    }

    fn insert_edge_live_with_key(
        &mut self,
        view: TxnEdgeView,
        local: Option<TxnLocalRef>,
        triple_key: (TxnEndpointKey, TxnEndpointKey, u32),
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
        let type_id = view.type_id;
        let triple_key = (from_key, to_key, type_id);
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
        triple_key: (TxnEndpointKey, TxnEndpointKey, u32),
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
        triple_key: &(TxnEndpointKey, TxnEndpointKey, u32),
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
        triple_key: (TxnEndpointKey, TxnEndpointKey, u32),
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
            TxnNodeRef::Key { type_id, key } => {
                self.nodes_by_key
                    .insert((*type_id, key.clone()), opinion.clone());
            }
        }
        if let Some(id) = deleted_id {
            self.deleted_node_ids_seen.insert(id);
        }
        if let Some(view) = existing.as_ref() {
            let key = (view.type_id, view.key.clone());
            self.nodes_by_key.insert(key.clone(), opinion.clone());
            if let Some(id) = view.id {
                self.nodes_by_id.insert(id, opinion.clone());
            }
            if let Some(local) = &view.local {
                let locals = self.node_key_locals.entry(key.clone()).or_default();
                if !locals.contains(local) {
                    locals.push(local.clone());
                }
            }
            self.set_node_locals_for_key(&key, opinion);
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
                    type_id: view.type_id,
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
        let deleted_key = existing
            .map(|view| (view.type_id, view.key.as_str()))
            .or(match target {
                TxnNodeRef::Key { type_id, key } => Some((*type_id, key.as_str())),
                _ => None,
            });

        match candidate {
            TxnNodeRef::Id(id) => Ok(deleted_id == Some(*id)),
            TxnNodeRef::Key { type_id, key } => {
                if deleted_key == Some((*type_id, key.as_str())) {
                    return Ok(true);
                }
                let Some(id) = deleted_id else {
                    return Ok(false);
                };
                Ok(snapshot
                    .get_node_by_key(*type_id, key)?
                    .is_some_and(|node| node.id == id))
            }
            TxnNodeRef::Local(local) => match self.nodes_by_local.get(local) {
                Some(NodeOverlayOpinion::Live(view))
                | Some(NodeOverlayOpinion::Deleted(Some(view))) => {
                    Ok(view.id.is_some_and(|id| deleted_id == Some(id))
                        || deleted_key == Some((view.type_id, view.key.as_str())))
                }
                Some(NodeOverlayOpinion::Deleted(None)) => Ok(false),
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
            TxnEdgeRef::Triple { from, to, type_id } => {
                if let (Some(from_key), Some(to_key)) = (
                    self.read_endpoint_key(snapshot, from)?,
                    self.read_endpoint_key(snapshot, to)?,
                ) {
                    self.insert_edge_triple_delete_if_current(
                        (from_key, to_key, *type_id),
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
            let triple_key = (from_key, to_key, view.type_id);
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
        key: &(u32, String),
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
        triple_key: &(TxnEndpointKey, TxnEndpointKey, u32),
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

fn collect_txn_intent_cache_targets(
    intent: &TxnIntent,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
    edge_ids: &mut NodeIdSet,
) {
    match intent {
        TxnIntent::UpsertNode { type_id, key, .. } => {
            node_keys.insert((*type_id, key.clone()));
        }
        TxnIntent::UpsertEdge { from, to, .. } => {
            collect_txn_node_ref_cache_targets(from, node_keys, node_ids);
            collect_txn_node_ref_cache_targets(to, node_keys, node_ids);
        }
        TxnIntent::DeleteNode { target } => {
            collect_txn_node_ref_cache_targets(target, node_keys, node_ids);
        }
        TxnIntent::DeleteEdge { target } => {
            collect_txn_edge_ref_cache_targets(target, node_keys, node_ids, edge_ids);
        }
        TxnIntent::InvalidateEdge { target, .. } => {
            collect_txn_edge_ref_cache_targets(target, node_keys, node_ids, edge_ids);
        }
    }
}

fn collect_txn_node_ref_cache_targets(
    target: &TxnNodeRef,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
) {
    match target {
        TxnNodeRef::Id(id) => {
            node_ids.insert(*id);
        }
        TxnNodeRef::Key { type_id, key } => {
            node_keys.insert((*type_id, key.clone()));
        }
        TxnNodeRef::Local(_) => {}
    }
}

fn collect_txn_edge_ref_cache_targets(
    target: &TxnEdgeRef,
    node_keys: &mut HashSet<(u32, String)>,
    node_ids: &mut NodeIdSet,
    edge_ids: &mut NodeIdSet,
) {
    match target {
        TxnEdgeRef::Id(id) => {
            edge_ids.insert(*id);
        }
        TxnEdgeRef::Triple { from, to, .. } => {
            collect_txn_node_ref_cache_targets(from, node_keys, node_ids);
            collect_txn_node_ref_cache_targets(to, node_keys, node_ids);
        }
        TxnEdgeRef::Local(_) => {}
    }
}

fn node_to_txn_view(node: NodeRecord) -> TxnNodeView {
    TxnNodeView {
        id: Some(node.id),
        local: None,
        type_id: node.type_id,
        key: node.key,
        props: node.props,
        created_at: Some(node.created_at),
        updated_at: Some(node.updated_at),
        weight: node.weight,
        dense_vector: node.dense_vector,
        sparse_vector: node.sparse_vector,
    }
}

fn edge_to_txn_view(edge: EdgeRecord) -> TxnEdgeView {
    TxnEdgeView {
        id: Some(edge.id),
        local: None,
        from: TxnNodeRef::Id(edge.from),
        to: TxnNodeRef::Id(edge.to),
        type_id: edge.type_id,
        props: edge.props,
        created_at: Some(edge.created_at),
        updated_at: Some(edge.updated_at),
        weight: edge.weight,
        valid_from: Some(edge.valid_from),
        valid_to: Some(edge.valid_to),
    }
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

#[derive(Default)]
struct TxnPlanningCache {
    begin_node_keys: HashMap<(u32, String), Option<NodeRecord>>,
    current_node_keys: HashMap<(u32, String), Option<NodeRecord>>,
    begin_edge_triples: HashMap<(u64, u64, u32), Option<EdgeRecord>>,
    current_edge_triples: HashMap<(u64, u64, u32), Option<EdgeRecord>>,
    current_edges_by_id: NodeIdMap<Option<EdgeRecord>>,
    node_opinions_by_id: NodeIdMap<TargetOpinion>,
    edge_opinions_by_id: NodeIdMap<TargetOpinion>,
}

impl EngineCore {
    fn plan_txn_commit(&mut self, request: &TxnCommitRequest) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let mut ops = Vec::new();
        let mut state = PlannedTxnState::default();
        let mut cache = self.build_txn_planning_cache(request)?;
        let mut next_node_id = self.next_node_id;
        let mut next_edge_id = self.next_edge_id;

        for entry in &request.entries {
            match &entry.intent {
                TxnIntent::UpsertNode {
                    type_id,
                    key,
                    options,
                    ..
                } => {
                    let (id, created_at) = if let Some(&(id, created_at)) =
                        state.nodes_by_key.get(&(*type_id, key.clone()))
                    {
                        (id, created_at)
                    } else {
                        self.validate_node_key_conflict(request, &mut cache, *type_id, key)?;
                        match self.cached_current_node_key(&mut cache, *type_id, key)? {
                            Some(node) => (node.id, node.created_at),
                            None => {
                                let id = next_node_id;
                                next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
                                    EngineError::InvalidOperation("node id counter overflow".into())
                                })?;
                                (id, now)
                            }
                        }
                    };
                    state
                        .nodes_by_key
                        .insert((*type_id, key.clone()), (id, created_at));
                    state.deleted_node_ids.remove(&id);
                    if let Some(local) = &entry.produced_node {
                        state.local_node_ids.insert(local.clone(), id);
                    }
                    let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
                        self.manifest.dense_vector.as_ref(),
                        options.dense_vector.as_ref(),
                        options.sparse_vector.as_ref(),
                    )?;
                    ops.push(WalOp::UpsertNode(NodeRecord {
                        id,
                        type_id: *type_id,
                        key: key.clone(),
                        props: options.props.clone(),
                        created_at,
                        updated_at: now,
                        weight: options.weight,
                        dense_vector,
                        sparse_vector,
                        last_write_seq: 0,
                    }));
                    state.node_ids.push(id);
                }
                TxnIntent::UpsertEdge {
                    from,
                    to,
                    type_id,
                    options,
                    ..
                } => {
                    let from_id =
                        self.resolve_node_ref_required(from, &state, request, &mut cache)?;
                    let to_id =
                        self.resolve_node_ref_required(to, &state, request, &mut cache)?;
                    self.validate_node_id_conflict(&mut cache, from_id, request.snapshot_seq)?;
                    self.validate_node_id_conflict(&mut cache, to_id, request.snapshot_seq)?;
                    let triple = (from_id, to_id, *type_id);
                    let (id, created_at) = if self.edge_uniqueness {
                        if let Some(&(id, created_at)) = state.edges_by_triple.get(&triple) {
                            (id, created_at)
                        } else {
                            self.validate_edge_triple_conflict(request, &mut cache, from_id, to_id, *type_id)?;
                            match self.cached_current_edge_triple(&mut cache, from_id, to_id, *type_id)? {
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
                        self.validate_edge_triple_conflict(request, &mut cache, from_id, to_id, *type_id)?;
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
                        type_id: *type_id,
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
                    ops.push(WalOp::UpsertEdge(edge));
                    state.edge_ids.push(id);
                }
                TxnIntent::DeleteNode { target } => {
                    let Some(id) =
                        self.resolve_node_ref_optional(target, &state, request, &mut cache)?
                    else {
                        continue;
                    };
                    self.validate_node_id_conflict(&mut cache, id, request.snapshot_seq)?;
                    let incident = self.incident_edges_for_txn_delete(id)?;
                    for entry in &incident {
                        self.validate_edge_id_conflict(&mut cache, entry.edge_id, request.snapshot_seq)?;
                        if state.deleted_edge_ids.insert(entry.edge_id) {
                            ops.push(WalOp::DeleteEdge {
                                id: entry.edge_id,
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
                        self.resolve_edge_ref_optional(target, &state, request, &mut cache)?
                    else {
                        continue;
                    };
                    self.validate_edge_id_conflict(&mut cache, id, request.snapshot_seq)?;
                    if state.deleted_edge_ids.insert(id) {
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
                        self.resolve_edge_ref_optional(target, &state, request, &mut cache)?
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
                        let triple = (updated.from, updated.to, updated.type_id);
                        set_planned_edge_triple_if_current_or_absent(
                            &mut state,
                            triple,
                            id,
                            updated.created_at,
                        );
                        state.edge_id_to_triple.insert(id, triple);
                        state.edge_records_by_id.insert(id, updated.clone());
                        ops.push(WalOp::UpsertEdge(updated));
                    } else {
                        self.validate_edge_id_conflict(&mut cache, id, request.snapshot_seq)?;
                        if let Some(edge) = self.cached_current_edge(&mut cache, id)? {
                            let updated = EdgeRecord {
                                updated_at: now,
                                valid_to: *valid_to,
                                ..edge
                            };
                            let triple = (updated.from, updated.to, updated.type_id);
                            let current_triple =
                                self.cached_current_edge_triple(&mut cache, updated.from, updated.to, updated.type_id)?;
                            if current_triple.as_ref().is_none_or(|edge| edge.id == id) {
                                state
                                    .edges_by_triple
                                    .insert(triple, (id, updated.created_at));
                            }
                            state.edge_id_to_triple.insert(id, triple);
                            state.edge_records_by_id.insert(id, updated.clone());
                            ops.push(WalOp::UpsertEdge(updated));
                        }
                    }
                }
            }
        }

        self.next_node_id = next_node_id;
        self.next_edge_id = next_edge_id;
        self.update_next_node_id_seen();
        self.update_next_edge_id_seen();

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
        })
    }

    fn build_txn_planning_cache(
        &self,
        request: &TxnCommitRequest,
    ) -> Result<TxnPlanningCache, EngineError> {
        let mut node_keys = HashSet::new();
        let mut node_ids = NodeIdSet::default();
        let mut edge_ids = NodeIdSet::default();
        for entry in &request.entries {
            collect_txn_intent_cache_targets(
                &entry.intent,
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
                .map(|(type_id, key)| (*type_id, key.as_str()))
                .collect();
            let begin_nodes = request.snapshot.get_nodes_by_keys_raw(&key_refs)?;
            let current_nodes = self.get_nodes_by_keys_raw(&key_refs)?;
            for ((type_id, key), node) in node_keys.iter().cloned().zip(begin_nodes) {
                cache.begin_node_keys.insert((type_id, key), node);
            }
            for ((type_id, key), node) in node_keys.into_iter().zip(current_nodes) {
                cache.current_node_keys.insert((type_id, key), node);
            }
        }

        let edge_ids: Vec<u64> = edge_ids.into_iter().collect();
        if !edge_ids.is_empty() {
            let current_edges = self.get_edges(&edge_ids)?;
            for (id, edge) in edge_ids.into_iter().zip(current_edges) {
                cache.current_edges_by_id.insert(id, edge);
            }
        }
        for id in node_ids {
            let opinion = self.node_id_opinion(id)?;
            cache.node_opinions_by_id.insert(id, opinion);
        }

        Ok(cache)
    }

    fn cached_begin_node_key(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let cache_key = (type_id, key.to_string());
        if !cache.begin_node_keys.contains_key(&cache_key) {
            let node = request.snapshot.get_node_by_key_raw(type_id, key)?;
            cache.begin_node_keys.insert(cache_key.clone(), node);
        }
        Ok(cache.begin_node_keys.get(&cache_key).cloned().flatten())
    }

    fn cached_current_node_key(
        &self,
        cache: &mut TxnPlanningCache,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let cache_key = (type_id, key.to_string());
        if !cache.current_node_keys.contains_key(&cache_key) {
            let node = self.get_node_by_key_raw(type_id, key)?;
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
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let key = (from, to, type_id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.begin_edge_triples.entry(key)
        {
            let edge = request.snapshot.get_edge_by_triple(from, to, type_id)?;
            entry.insert(edge);
        }
        Ok(cache.begin_edge_triples.get(&key).cloned().flatten())
    }

    fn cached_current_edge_triple(
        &self,
        cache: &mut TxnPlanningCache,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        let key = (from, to, type_id);
        if let std::collections::hash_map::Entry::Vacant(entry) =
            cache.current_edge_triples.entry(key)
        {
            let edge = self.get_edge_by_triple(from, to, type_id)?;
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
        type_id: u32,
        key: &str,
    ) -> Result<(), EngineError> {
        let current = self.cached_current_node_key(cache, type_id, key)?;
        if let Some(current) = current.as_ref() {
            if current.last_write_seq <= request.snapshot_seq {
                return Ok(());
            }
        }

        let begin = self.cached_begin_node_key(request, cache, type_id, key)?;
        match (begin, current) {
            (Some(begin), Some(current)) if begin.id == current.id => {
                self.validate_node_id_conflict(cache, begin.id, request.snapshot_seq)
            }
            (Some(begin), _) => {
                self.validate_node_id_conflict(cache, begin.id, request.snapshot_seq)?;
                Err(EngineError::TxnConflict(format!(
                    "node key ({}, {}) changed after transaction begin",
                    type_id, key
                )))
            }
            (None, Some(current)) => {
                if current.last_write_seq > request.snapshot_seq {
                    Err(EngineError::TxnConflict(format!(
                        "node key ({}, {}) appeared after transaction begin",
                        type_id, key
                    )))
                } else {
                    Ok(())
                }
            }
            (None, None) => Ok(()),
        }
    }

    fn incident_edges_for_txn_delete(
        &self,
        node_id: u64,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        let tombstones = self.collect_tombstones();
        let deleted_edges = &tombstones.1;
        let mut results = self
            .memtable
            .incident_edges_at(node_id, Direction::Both, None, self.engine_seq);
        let mut seen_edges: IdSet = results.iter().map(|entry| entry.edge_id).collect();

        for epoch in &self.immutable_epochs {
            for entry in epoch
                .memtable
                .incident_edges_at(node_id, Direction::Both, None, self.engine_seq)
            {
                if !seen_edges.insert(entry.edge_id) {
                    continue;
                }
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                results.push(entry);
            }
        }

        for seg in &self.segments {
            for entry in seg.neighbors(node_id, Direction::Both, None, 0)? {
                if !seen_edges.insert(entry.edge_id) {
                    continue;
                }
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                results.push(entry);
            }
        }

        Ok(results)
    }

    fn validate_edge_triple_conflict(
        &self,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<(), EngineError> {
        let current = self.cached_current_edge_triple(cache, from, to, type_id)?;
        if let Some(current) = current.as_ref() {
            if current.last_write_seq <= request.snapshot_seq {
                return Ok(());
            }
        }

        let begin = self.cached_begin_edge_triple(request, cache, from, to, type_id)?;
        match (begin, current) {
            (Some(begin), Some(current)) if begin.id == current.id => {
                self.validate_edge_id_conflict(cache, begin.id, request.snapshot_seq)
            }
            (Some(begin), _) => {
                self.validate_edge_id_conflict(cache, begin.id, request.snapshot_seq)?;
                Err(EngineError::TxnConflict(format!(
                    "edge triple ({}, {}, {}) changed after transaction begin",
                    from, to, type_id
                )))
            }
            (None, Some(current)) => {
                if current.last_write_seq > request.snapshot_seq {
                    Err(EngineError::TxnConflict(format!(
                        "edge triple ({}, {}, {}) appeared after transaction begin",
                        from, to, type_id
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

    fn resolve_node_ref_required(
        &self,
        target: &TxnNodeRef,
        state: &PlannedTxnState,
        request: &TxnCommitRequest,
        cache: &mut TxnPlanningCache,
    ) -> Result<u64, EngineError> {
        let id = self
            .resolve_node_ref_optional(target, state, request, cache)?
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
            TxnNodeRef::Key { type_id, key } => {
                if let Some(&(id, _)) = state.nodes_by_key.get(&(*type_id, key.clone())) {
                    Ok(Some(id))
                } else {
                    let current = self
                        .cached_current_node_key(cache, *type_id, key)?
                        .map(|node| node.id);
                    if current.is_some() {
                        Ok(current)
                    } else {
                        Ok(self
                            .cached_begin_node_key(request, cache, *type_id, key)?
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
            TxnEdgeRef::Triple { from, to, type_id } => {
                let Some(from_id) = self.resolve_node_ref_optional(from, state, request, cache)?
                else {
                    return Ok(None);
                };
                let Some(to_id) = self.resolve_node_ref_optional(to, state, request, cache)? else {
                    return Ok(None);
                };
                if let Some(&(id, _)) = state.edges_by_triple.get(&(from_id, to_id, *type_id)) {
                    Ok(Some(id))
                } else {
                    let current = self
                        .cached_current_edge_triple(cache, from_id, to_id, *type_id)?
                        .map(|edge| edge.id);
                    if current.is_some() {
                        Ok(current)
                    } else {
                        Ok(self
                            .cached_begin_edge_triple(request, cache, from_id, to_id, *type_id)?
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
