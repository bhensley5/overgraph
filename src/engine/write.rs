// Write operations: upsert, delete, batch, patch, prune.
// This file is include!()'d into mod.rs. All items share the engine module scope.

type TokenCreationList = Vec<(String, u32)>;
type TokenCreationSet = (TokenCreationList, TokenCreationList);

struct LabelResolutionPlan<'a> {
    catalog: &'a RuntimeLabelCatalog,
    node_labels_to_create: Vec<(String, u32)>,
    edge_labels_to_create: Vec<(String, u32)>,
    new_node_label_to_id: BTreeMap<String, u32>,
    new_edge_label_to_id: BTreeMap<String, u32>,
    next_node_label_id: u32,
    next_edge_label_id: u32,
    #[cfg(test)]
    node_label_resolve_calls: usize,
    #[cfg(test)]
    edge_label_resolve_calls: usize,
}

impl<'a> LabelResolutionPlan<'a> {
    fn from_catalog(catalog: &'a RuntimeLabelCatalog) -> Self {
        Self {
            catalog,
            node_labels_to_create: Vec::new(),
            edge_labels_to_create: Vec::new(),
            new_node_label_to_id: BTreeMap::new(),
            new_edge_label_to_id: BTreeMap::new(),
            next_node_label_id: catalog.next_node_label_id,
            next_edge_label_id: catalog.next_edge_label_id,
            #[cfg(test)]
            node_label_resolve_calls: 0,
            #[cfg(test)]
            edge_label_resolve_calls: 0,
        }
    }

    fn resolve_node_label_for_write(&mut self, label: &str) -> Result<u32, EngineError> {
        #[cfg(test)]
        {
            self.node_label_resolve_calls += 1;
        }
        if let Some(&label_id) = self.catalog.node_label_to_id.get(label) {
            return Ok(label_id);
        }
        if let Some(&label_id) = self.new_node_label_to_id.get(label) {
            return Ok(label_id);
        }
        validate_label_token_name(label)?;
        if self.next_node_label_id == u32::MAX {
            return Err(EngineError::InvalidOperation(
                "node label token ID space exhausted".to_string(),
            ));
        }
        let label_id = self.next_node_label_id;
        self.next_node_label_id += 1;
        let label = label.to_string();
        self.new_node_label_to_id.insert(label.clone(), label_id);
        self.node_labels_to_create.push((label, label_id));
        Ok(label_id)
    }

    fn resolve_edge_label_for_write(&mut self, label: &str) -> Result<u32, EngineError> {
        #[cfg(test)]
        {
            self.edge_label_resolve_calls += 1;
        }
        if let Some(&label_id) = self.catalog.edge_label_to_id.get(label) {
            return Ok(label_id);
        }
        if let Some(&label_id) = self.new_edge_label_to_id.get(label) {
            return Ok(label_id);
        }
        validate_label_token_name(label)?;
        if self.next_edge_label_id == u32::MAX {
            return Err(EngineError::InvalidOperation(
                "edge-label token ID space exhausted".to_string(),
            ));
        }
        let label_id = self.next_edge_label_id;
        self.next_edge_label_id += 1;
        let label = label.to_string();
        self.new_edge_label_to_id.insert(label.clone(), label_id);
        self.edge_labels_to_create.push((label, label_id));
        Ok(label_id)
    }

    #[cfg(test)]
    fn resolve_node_label_ids_for_request<'request, I>(
        &mut self,
        labels: I,
    ) -> Result<Vec<u32>, EngineError>
    where
        I: IntoIterator<Item = &'request str>,
    {
        let labels = labels.into_iter();
        let (min_len, _) = labels.size_hint();
        let mut resolved = HashMap::with_capacity(min_len);
        let mut ids = Vec::with_capacity(min_len);
        for label in labels {
            let label_id = if let Some(&label_id) = resolved.get(label) {
                label_id
            } else {
                let label_id = self.resolve_node_label_for_write(label)?;
                resolved.insert(label, label_id);
                label_id
            };
            ids.push(label_id);
        }
        Ok(ids)
    }

    fn resolve_validated_node_label_set_for_write(
        &mut self,
        labels: &ValidatedNodeLabelList<'_>,
    ) -> Result<NodeLabelSet, EngineError> {
        let mut ids = [0u32; MAX_NODE_LABELS_PER_NODE];
        for (idx, &label) in labels.as_slice().iter().enumerate() {
            ids[idx] = self.resolve_node_label_for_write(label)?;
        }
        NodeLabelSet::from_label_ids(ids[..labels.len()].iter().copied())
    }

    fn resolve_validated_node_label_sets_for_request(
        &mut self,
        requests: &[ValidatedNodeLabelList<'_>],
    ) -> Result<Vec<NodeLabelSet>, EngineError> {
        let mut resolved = HashMap::new();
        let mut label_sets = Vec::with_capacity(requests.len());
        for labels in requests {
            let mut ids = [0u32; MAX_NODE_LABELS_PER_NODE];
            for (idx, &label) in labels.as_slice().iter().enumerate() {
                let label_id = match resolved.entry(label) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        let label_id = self.resolve_node_label_for_write(label)?;
                        entry.insert(label_id);
                        label_id
                    }
                };
                ids[idx] = label_id;
            }
            label_sets.push(NodeLabelSet::from_label_ids(
                ids[..labels.len()].iter().copied(),
            )?);
        }
        Ok(label_sets)
    }

    fn resolve_edge_label_ids_for_request<'request, I>(
        &mut self,
        edge_labels: I,
    ) -> Result<Vec<u32>, EngineError>
    where
        I: IntoIterator<Item = &'request str>,
    {
        let edge_labels = edge_labels.into_iter();
        let (min_len, _) = edge_labels.size_hint();
        let mut resolved = HashMap::with_capacity(min_len);
        let mut ids = Vec::with_capacity(min_len);
        for label in edge_labels {
            let label_id = if let Some(&label_id) = resolved.get(label) {
                label_id
            } else {
                let label_id = self.resolve_edge_label_for_write(label)?;
                resolved.insert(label, label_id);
                label_id
            };
            ids.push(label_id);
        }
        Ok(ids)
    }

    fn token_op_count(&self) -> usize {
        self.node_labels_to_create.len() + self.edge_labels_to_create.len()
    }

    fn push_token_ops(&self, ops: &mut Vec<WalOp>) {
        for (label, label_id) in &self.node_labels_to_create {
            ops.push(WalOp::EnsureNodeLabel {
                label: label.clone(),
                label_id: *label_id,
            });
        }
        for (label, label_id) in &self.edge_labels_to_create {
            ops.push(WalOp::EnsureEdgeLabel {
                label: label.clone(),
                label_id: *label_id,
            });
        }
    }

    fn token_creations(&self) -> TokenCreationSet {
        (
            self.node_labels_to_create.clone(),
            self.edge_labels_to_create.clone(),
        )
    }
}

fn validate_prune_policy(policy: &PrunePolicy) -> Result<(), EngineError> {
    if let Some(label) = policy.label.as_deref() {
        validate_label_token_name(label)?;
    }
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
    Ok(())
}

fn validate_node_key_for_write(key: &str) -> Result<(), EngineError> {
    if key.len() > u16::MAX as usize {
        return Err(EngineError::InvalidOperation(format!(
            "node key must be at most {} UTF-8 bytes, got {}",
            u16::MAX,
            key.len()
        )));
    }
    Ok(())
}

fn node_key_conflict_error(key: &str, existing: u64, conflicting: u64) -> EngineError {
    EngineError::InvalidOperation(format!(
        "node key conflict for key '{key}': requested label memberships resolve to node IDs {existing} and {conflicting}"
    ))
}

fn stage_node_label_token_in_manifest(
    manifest: &mut ManifestState,
    label: &str,
    label_id: u32,
) -> Result<(), EngineError> {
    validate_label_token_name(label)?;
    if let Some(existing_id) = manifest.node_label_tokens.get(label) {
        if *existing_id != label_id {
            return Err(EngineError::ManifestError(format!(
                "node label token conflict: label '{label}' already has label_id {existing_id}, not {label_id}"
            )));
        }
        return Ok(());
    }
    if let Some((existing_label, _)) = manifest
        .node_label_tokens
        .iter()
        .find(|(_, existing_id)| **existing_id == label_id)
    {
        return Err(EngineError::ManifestError(format!(
            "node label token conflict: label_id {label_id} is assigned to both '{existing_label}' and '{label}'"
        )));
    }
    manifest
        .node_label_tokens
        .insert(label.to_string(), label_id);
    manifest.next_node_label_id = manifest
        .next_node_label_id
        .max(label_id.saturating_add(1));
    Ok(())
}

fn stage_edge_label_token_in_manifest(
    manifest: &mut ManifestState,
    label: &str,
    label_id: u32,
) -> Result<(), EngineError> {
    validate_label_token_name(label)?;
    if let Some(existing_id) = manifest.edge_label_tokens.get(label) {
        if *existing_id != label_id {
            return Err(EngineError::ManifestError(format!(
                "edge-label token conflict: edge label '{label}' already has label_id {existing_id}, not {label_id}"
            )));
        }
        return Ok(());
    }
    if let Some((existing_edge_label, _)) = manifest
        .edge_label_tokens
        .iter()
        .find(|(_, existing_id)| **existing_id == label_id)
    {
        return Err(EngineError::ManifestError(format!(
            "edge-label token conflict: label_id {label_id} is assigned to both '{existing_edge_label}' and '{label}'"
        )));
    }
    manifest
        .edge_label_tokens
        .insert(label.to_string(), label_id);
    manifest.next_edge_label_id = manifest
        .next_edge_label_id
        .max(label_id.saturating_add(1));
    Ok(())
}

fn stage_label_tokens_in_manifest(
    manifest: &mut ManifestState,
    node_labels: &[(String, u32)],
    edge_labels: &[(String, u32)],
) -> Result<(), EngineError> {
    for (label, label_id) in node_labels {
        stage_node_label_token_in_manifest(manifest, label, *label_id)?;
    }
    for (label, label_id) in edge_labels {
        stage_edge_label_token_in_manifest(manifest, label, *label_id)?;
    }
    Ok(())
}

struct SchemaWriteOverlay {
    final_nodes: NodeIdMap<NodeRecord>,
    final_edges: NodeIdMap<EdgeRecord>,
    deleted_nodes: NodeIdSet,
    deleted_edges: NodeIdSet,
    final_upserted_edges: NodeIdSet,
    node_order: Vec<u64>,
    edge_order: Vec<u64>,
}

impl SchemaWriteOverlay {
    fn new() -> Self {
        Self {
            final_nodes: NodeIdMap::default(),
            final_edges: NodeIdMap::default(),
            deleted_nodes: NodeIdSet::default(),
            deleted_edges: NodeIdSet::default(),
            final_upserted_edges: NodeIdSet::default(),
            node_order: Vec::new(),
            edge_order: Vec::new(),
        }
    }
}

#[cfg(not(test))]
const SCHEMA_ENDPOINT_VALIDATION_CHUNK_SIZE: usize = 4096;
#[cfg(test)]
const SCHEMA_ENDPOINT_VALIDATION_CHUNK_SIZE: usize = 8;

#[derive(Clone, Copy)]
enum SchemaEndpointNodeState {
    Live(NodeLabelSet),
    Missing,
}

#[derive(Default)]
struct SchemaEndpointLabelCache {
    states: NodeIdMap<SchemaEndpointNodeState>,
}

impl SchemaEndpointLabelCache {
    fn hydrate_node_ids(
        &mut self,
        core: &EngineCore,
        overlay: &SchemaWriteOverlay,
        node_ids: &[u64],
    ) -> Result<(), EngineError> {
        if node_ids.is_empty() {
            return Ok(());
        }

        let mut unique = node_ids.to_vec();
        unique.sort_unstable();
        unique.dedup();

        let mut hydrate_current = Vec::new();
        for node_id in unique {
            if self.states.contains_key(&node_id) {
                continue;
            }
            if let Some(node) = overlay.final_nodes.get(&node_id) {
                self.states
                    .insert(node_id, SchemaEndpointNodeState::Live(node.label_ids));
            } else if overlay.deleted_nodes.contains(&node_id) {
                self.states.insert(node_id, SchemaEndpointNodeState::Missing);
            } else {
                hydrate_current.push(node_id);
            }
        }

        for chunk in hydrate_current.chunks(SCHEMA_ENDPOINT_VALIDATION_CHUNK_SIZE) {
            let visibility = core.sources().find_node_visibility_meta(chunk)?;
            for (&node_id, state) in chunk.iter().zip(visibility.iter()) {
                let endpoint_state = match state {
                    NodeVisibilityState::Live(meta) => {
                        SchemaEndpointNodeState::Live(meta.label_ids)
                    }
                    NodeVisibilityState::Deleted | NodeVisibilityState::Missing => {
                        SchemaEndpointNodeState::Missing
                    }
                };
                self.states.insert(node_id, endpoint_state);
            }
        }

        Ok(())
    }

    fn labels_for(&self, node_id: u64) -> Option<NodeLabelSet> {
        debug_assert!(
            self.states.contains_key(&node_id),
            "endpoint node labels must be hydrated before validation"
        );
        match self.states.get(&node_id).copied() {
            Some(SchemaEndpointNodeState::Live(labels)) => Some(labels),
            Some(SchemaEndpointNodeState::Missing) | None => None,
        }
    }
}

impl IdCounterSnapshot {
    fn capture(core: &EngineCore) -> Self {
        Self {
            next_node_id: core.next_node_id,
            next_edge_id: core.next_edge_id,
            next_node_id_seen: core.next_node_id_seen.load(Ordering::Acquire),
            next_edge_id_seen: core.next_edge_id_seen.load(Ordering::Acquire),
        }
    }

    fn restore(self, core: &mut EngineCore) {
        core.next_node_id = self.next_node_id;
        core.next_edge_id = self.next_edge_id;
        core.next_node_id_seen
            .store(self.next_node_id_seen, Ordering::Release);
        core.next_edge_id_seen
            .store(self.next_edge_id_seen, Ordering::Release);
    }
}

fn is_schema_violation_error(error: &EngineError) -> bool {
    matches!(
        error,
        EngineError::InvalidOperation(message) if message.starts_with("schema violation:")
    )
}

fn schema_validation_failure_error(failure: SchemaValidationFailure) -> EngineError {
    EngineError::InvalidOperation(failure.message)
}

impl EngineCore {
    fn apply_manifest_token_creations(
        &mut self,
        node_labels: &[(String, u32)],
        edge_labels: &[(String, u32)],
    ) -> Result<(), EngineError> {
        if node_labels.is_empty() && edge_labels.is_empty() {
            return Ok(());
        }
        {
            let mut catalog = self.label_catalog.write().unwrap();
            for (label, label_id) in node_labels {
                catalog.apply_node_label(label.clone(), *label_id, None)?;
            }
            for (label, label_id) in edge_labels {
                catalog.apply_edge_label(label.clone(), *label_id, None)?;
            }
            catalog.apply_to_manifest(&mut self.manifest);
        }
        Ok(())
    }

    fn commit_core_write_plan(
        &mut self,
        planned: PlannedCoreWrite,
    ) -> (Result<CoreWriteReply, EngineError>, PublishImpact) {
        let PlannedCoreWrite {
            plan,
            id_counter_snapshot,
        } = planned;
        let mut publish_impact = PublishImpact::NoPublish;

        let result = (|| -> Result<CoreWriteReply, EngineError> {
            if let Err(error) = self.validate_schema_for_wal_ops(&plan.ops) {
                if is_schema_violation_error(&error) {
                    id_counter_snapshot.restore(self);
                }
                return Err(error);
            }

            match plan.ops.as_slice() {
                [] => {}
                [op] => self.append_and_apply_one_normalized(op)?,
                _ => self.append_and_apply_normalized(&plan.ops)?,
            }
            if !plan.ops.is_empty() {
                publish_impact = if plan.label_catalog_changed {
                    PublishImpact::SnapshotWithLabelCatalog
                } else {
                    PublishImpact::SnapshotOnly
                };
            }

            if plan.track_ids {
                for op in &plan.ops {
                    self.track_id(op);
                }
            }
            if self.next_node_id != id_counter_snapshot.next_node_id {
                self.update_next_node_id_seen();
            }
            if self.next_edge_id != id_counter_snapshot.next_edge_id {
                self.update_next_edge_id_seen();
            }

            if plan.auto_flush {
                let (auto_flush_result, auto_flush_impact) = self.maybe_auto_flush();
                publish_impact = publish_impact.combine(auto_flush_impact);
                auto_flush_result?;
            }

            Ok(plan.reply)
        })();

        (result, publish_impact)
    }

    fn plan_core_write(
        &mut self,
        request: &CoreWriteRequest,
    ) -> Result<PlannedCoreWrite, EngineError> {
        let id_counter_snapshot = IdCounterSnapshot::capture(self);
        let plan = match request {
            CoreWriteRequest::EnsureNodeLabel { label } => self.plan_ensure_node_label(label),
            CoreWriteRequest::EnsureEdgeLabel { label } => self.plan_ensure_edge_label(label),
            CoreWriteRequest::UpsertNode {
                labels,
                key,
                options,
            } => self.plan_upsert_node(labels, key, options),
            CoreWriteRequest::AddNodeLabel { id, label } => self.plan_add_node_label(*id, label),
            CoreWriteRequest::RemoveNodeLabel { id, label } => {
                self.plan_remove_node_label(*id, label)
            }
            CoreWriteRequest::UpsertEdge {
                from,
                to,
                label,
                options,
            } => self.plan_upsert_edge(*from, *to, label, options),
            CoreWriteRequest::BatchUpsertNodes { inputs } => self.plan_batch_upsert_nodes(inputs),
            CoreWriteRequest::BatchUpsertEdges { inputs } => self.plan_batch_upsert_edges(inputs),
            CoreWriteRequest::DeleteNode { id } => self.plan_delete_node(*id),
            CoreWriteRequest::DeleteEdge { id } => self.plan_delete_edge(*id),
            CoreWriteRequest::InvalidateEdge { id, valid_to } => {
                self.plan_invalidate_edge(*id, *valid_to)
            }
            #[cfg(test)]
            CoreWriteRequest::WriteOp { op } => self.plan_write_op(op),
            #[cfg(test)]
            CoreWriteRequest::WriteOpBatch { ops } => self.plan_write_op_batch(ops),
            CoreWriteRequest::GraphPatch { patch } => self.plan_graph_patch(patch),
            CoreWriteRequest::TxnCommit { request, .. } => self.plan_txn_commit(request),
            CoreWriteRequest::Prune { policy } => self.plan_prune(policy),
            CoreWriteRequest::SetPrunePolicy { .. }
            | CoreWriteRequest::RemovePrunePolicy { .. }
            | CoreWriteRequest::SetNodeSchema { .. }
            | CoreWriteRequest::DropNodeSchema { .. }
            | CoreWriteRequest::SetEdgeSchema { .. }
            | CoreWriteRequest::DropEdgeSchema { .. }
            | CoreWriteRequest::SetGraphSchema { .. }
            | CoreWriteRequest::AlterGraphSchema { .. }
            | CoreWriteRequest::DropGraphSchema
            | CoreWriteRequest::EnsureNodePropertyIndex { .. }
            | CoreWriteRequest::DropNodePropertyIndex { .. }
            | CoreWriteRequest::EnsureEdgePropertyIndex { .. }
            | CoreWriteRequest::DropEdgePropertyIndex { .. }
            | CoreWriteRequest::ApplySecondaryIndexReadFollowup { .. }
            | CoreWriteRequest::Sync
            | CoreWriteRequest::Flush
            | CoreWriteRequest::IngestMode
            | CoreWriteRequest::EndIngest
            | CoreWriteRequest::Compact => Err(EngineError::InvalidOperation(
                "request does not use the planner write path".to_string(),
            )),
        }?;
        Ok(PlannedCoreWrite {
            plan,
            id_counter_snapshot,
        })
    }

    fn validate_schema_for_wal_ops(&self, ops: &[WalOp]) -> Result<(), EngineError> {
        let catalog = &self.runtime_schema_catalog;
        if catalog.is_empty() {
            return Ok(());
        }
        if !self.schema_wal_ops_may_need_validation(catalog, ops) {
            return Ok(());
        }

        #[cfg(test)]
        self.schema_validation_overlay_builds
            .fetch_add(1, Ordering::Relaxed);

        let overlay = self.build_schema_write_overlay(catalog, ops);
        self.validate_schema_write_overlay(catalog, &overlay)
    }

    fn schema_wal_ops_may_need_validation(
        &self,
        catalog: &RuntimeSchemaCatalog,
        ops: &[WalOp],
    ) -> bool {
        ops.iter().any(|op| match op {
            WalOp::UpsertNode(node) => {
                catalog.node_has_applicable_schema(&node.label_ids)
                    || catalog.has_edge_endpoint_constraints
            }
            WalOp::UpsertEdge(edge) => catalog.edge_has_wal_validation_rules(edge.label_id),
            WalOp::DeleteNode { .. } => catalog.has_edge_endpoint_constraints,
            WalOp::DeleteEdge { .. }
            | WalOp::EnsureNodeLabel { .. }
            | WalOp::EnsureEdgeLabel { .. }
            | WalOp::BeginAtomicBatch { .. }
            | WalOp::CommitAtomicBatch { .. } => false,
        })
    }

    fn build_schema_write_overlay(
        &self,
        catalog: &RuntimeSchemaCatalog,
        ops: &[WalOp],
    ) -> SchemaWriteOverlay {
        let mut overlay = SchemaWriteOverlay::new();
        for op in ops {
            match op {
                WalOp::UpsertNode(node) => {
                    overlay.deleted_nodes.remove(&node.id);
                    if catalog.has_edge_endpoint_constraints
                        || catalog.node_has_applicable_schema(&node.label_ids)
                    {
                        overlay.final_nodes.insert(node.id, node.clone());
                        overlay.node_order.push(node.id);
                    } else {
                        overlay.final_nodes.remove(&node.id);
                    }
                }
                WalOp::UpsertEdge(edge) => {
                    overlay.deleted_edges.remove(&edge.id);
                    overlay.final_upserted_edges.insert(edge.id);
                    if catalog.edge_has_wal_validation_rules(edge.label_id) {
                        overlay.final_edges.insert(edge.id, edge.clone());
                        overlay.edge_order.push(edge.id);
                    } else {
                        overlay.final_edges.remove(&edge.id);
                    }
                }
                WalOp::DeleteNode { id, .. } => {
                    overlay.deleted_nodes.insert(*id);
                    overlay.final_nodes.remove(id);
                }
                WalOp::DeleteEdge { id, .. } => {
                    overlay.deleted_edges.insert(*id);
                    overlay.final_edges.remove(id);
                    overlay.final_upserted_edges.remove(id);
                }
                WalOp::EnsureNodeLabel { .. }
                | WalOp::EnsureEdgeLabel { .. }
                | WalOp::BeginAtomicBatch { .. }
                | WalOp::CommitAtomicBatch { .. } => {}
            }
        }
        overlay
    }

    fn validate_schema_write_overlay(
        &self,
        catalog: &RuntimeSchemaCatalog,
        overlay: &SchemaWriteOverlay,
    ) -> Result<(), EngineError> {
        let _deleted_record_count = overlay.deleted_nodes.len() + overlay.deleted_edges.len();
        let mut visited_nodes = NodeIdSet::default();
        for node_id in &overlay.node_order {
            if !visited_nodes.insert(*node_id) {
                continue;
            }
            let Some(node) = overlay.final_nodes.get(node_id) else {
                continue;
            };
            catalog
                .validate_node_record_detailed(node, self.manifest.dense_vector.as_ref())
                .map_err(schema_validation_failure_error)?;
        }

        let mut visited_edges = NodeIdSet::default();
        let mut endpoint_edge_order = Vec::new();
        let mut endpoint_node_ids = Vec::new();
        for edge_id in &overlay.edge_order {
            if !visited_edges.insert(*edge_id) {
                continue;
            }
            let Some(edge) = overlay.final_edges.get(edge_id) else {
                continue;
            };
            catalog
                .validate_edge_record_detailed(edge)
                .map_err(schema_validation_failure_error)?;
            if catalog.edge_schema_has_endpoint_rules(edge.label_id) {
                endpoint_edge_order.push(*edge_id);
                endpoint_node_ids.push(edge.from);
                endpoint_node_ids.push(edge.to);
            }
        }

        let mut endpoint_cache = SchemaEndpointLabelCache::default();
        endpoint_cache.hydrate_node_ids(self, overlay, &endpoint_node_ids)?;
        for edge_id in endpoint_edge_order {
            let Some(edge) = overlay.final_edges.get(&edge_id) else {
                continue;
            };
            let from_labels = endpoint_cache.labels_for(edge.from);
            let to_labels = endpoint_cache.labels_for(edge.to);
            catalog
                .validate_edge_endpoint_labels_detailed(
                    edge,
                    from_labels.as_ref(),
                    to_labels.as_ref(),
                )
                .map_err(schema_validation_failure_error)?;
        }

        self.validate_endpoint_incident_edges(catalog, overlay, &mut endpoint_cache)?;
        Ok(())
    }

    fn validate_endpoint_incident_edges(
        &self,
        catalog: &RuntimeSchemaCatalog,
        overlay: &SchemaWriteOverlay,
        endpoint_cache: &mut SchemaEndpointLabelCache,
    ) -> Result<(), EngineError> {
        if !catalog.has_edge_endpoint_constraints {
            return Ok(());
        }
        let label_filter_ids = catalog.endpoint_constrained_edge_label_ids.as_slice();
        if label_filter_ids.is_empty() {
            return Ok(());
        }

        let mut candidate_node_ids = Vec::new();
        if !overlay.final_nodes.is_empty() {
            let mut final_node_ids: Vec<u64> = overlay.final_nodes.keys().copied().collect();
            final_node_ids.sort_unstable();
            let current_states = self.sources().find_node_visibility_meta(&final_node_ids)?;
            for (&node_id, current_state) in final_node_ids.iter().zip(current_states.iter()) {
                let final_labels = overlay
                    .final_nodes
                    .get(&node_id)
                    .map(|node| node.label_ids)
                    .expect("final node id came from final_nodes map");
                let old_labels = match current_state {
                    NodeVisibilityState::Live(meta) => Some(meta.label_ids),
                    NodeVisibilityState::Deleted | NodeVisibilityState::Missing => None,
                };
                if catalog.label_change_may_affect_endpoint_rules(
                    old_labels.as_ref(),
                    Some(&final_labels),
                    false,
                ) {
                    candidate_node_ids.push(node_id);
                }
            }
        }

        candidate_node_ids.extend(overlay.deleted_nodes.iter().copied());
        if candidate_node_ids.is_empty() {
            return Ok(());
        }
        candidate_node_ids.sort_unstable();
        candidate_node_ids.dedup();

        self.sources().scan_edge_ids_by_endpoints(
            &candidate_node_ids,
            Direction::Both,
            Some(label_filter_ids),
            SCHEMA_ENDPOINT_VALIDATION_CHUNK_SIZE,
            |chunk| {
                #[cfg(test)]
                self.schema_validation_incident_scan_chunks
                    .fetch_add(1, Ordering::Relaxed);

                let mut edge_ids = Vec::with_capacity(chunk.len());
                for &edge_id in chunk {
                    if overlay.deleted_edges.contains(&edge_id)
                        || overlay.final_upserted_edges.contains(&edge_id)
                    {
                        continue;
                    }
                    edge_ids.push(edge_id);
                }
                if edge_ids.is_empty() {
                    return Ok(ControlFlow::Continue(()));
                }

                let hydrated = self.sources().find_edges(&edge_ids)?;
                let mut endpoint_node_ids = Vec::new();
                let mut edges = Vec::new();
                for edge in hydrated.into_iter().flatten() {
                    if !catalog.edge_schema_has_endpoint_rules(edge.label_id) {
                        continue;
                    }
                    endpoint_node_ids.push(edge.from);
                    endpoint_node_ids.push(edge.to);
                    edges.push(edge);
                }
                if edges.is_empty() {
                    return Ok(ControlFlow::Continue(()));
                }

                endpoint_cache.hydrate_node_ids(self, overlay, &endpoint_node_ids)?;
                for edge in &edges {
                    let from_labels = endpoint_cache.labels_for(edge.from);
                    let to_labels = endpoint_cache.labels_for(edge.to);
                    catalog
                        .validate_edge_endpoint_labels_detailed(
                            edge,
                            from_labels.as_ref(),
                            to_labels.as_ref(),
                        )
                        .map_err(schema_validation_failure_error)?;
                }
                Ok(ControlFlow::Continue(()))
            },
        )
    }

    fn plan_ensure_node_label(&mut self, label: &str) -> Result<CoreWritePlan, EngineError> {
        let (label_id, should_create) = self
            .label_catalog
            .read()
            .unwrap()
            .reserve_node_label(label)?;
        let ops = if should_create {
            vec![WalOp::EnsureNodeLabel {
                label: label.to_string(),
                label_id,
            }]
        } else {
            Vec::new()
        };
        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::U32(label_id),
            auto_flush: false,
            track_ids: false,
            label_catalog_changed: should_create,
        })
    }

    fn plan_ensure_edge_label(&mut self, label: &str) -> Result<CoreWritePlan, EngineError> {
        let (label_id, should_create) = self
            .label_catalog
            .read()
            .unwrap()
            .reserve_edge_label(label)?;
        let ops = if should_create {
            vec![WalOp::EnsureEdgeLabel {
                label: label.to_string(),
                label_id,
            }]
        } else {
            Vec::new()
        };
        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::U32(label_id),
            auto_flush: false,
            track_ids: false,
            label_catalog_changed: should_create,
        })
    }

    fn plan_upsert_node(
        &mut self,
        labels: &[String],
        key: &str,
        options: &UpsertNodeOptions,
    ) -> Result<CoreWritePlan, EngineError> {
        let validated_labels = ValidatedNodeLabelList::new(labels.iter().map(String::as_str))?;
        validate_node_key_for_write(key)?;
        let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
            self.manifest.dense_vector.as_ref(),
            options.dense_vector.as_ref(),
            options.sparse_vector.as_ref(),
        )?;
        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_ids = label_plan.resolve_validated_node_label_set_for_write(&validated_labels)?;
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;
        let now = now_millis();

        let (id, created_at) = match self.find_existing_node_for_label_set(label_ids, key)? {
            Some((id, created_at)) => (id, created_at),
            None => {
                let id = self.next_node_id;
                self.next_node_id += 1;
                (id, now)
            }
        };

        let node = NodeRecord {
            id,
            label_ids,
            key: key.to_string(),
            props: options.props.clone(),
            created_at,
            updated_at: now,
            weight: options.weight,
            dense_vector,
            sparse_vector,
            last_write_seq: 0,
        };

        let mut ops = Vec::with_capacity(token_op_count + 1);
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);
        ops.push(WalOp::UpsertNode(node));

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::U64(id),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn plan_node_upsert_records(
        &mut self,
        inputs: &[NodeInput],
        label_sets: &[NodeLabelSet],
        normalized_vectors: Vec<(Option<DenseVector>, Option<SparseVector>)>,
        now: i64,
    ) -> Result<(Vec<NodeRecord>, Vec<u64>, u64), EngineError> {
        let mut committed_keys: HashMap<(u32, String), NodeRecord> = HashMap::new();
        let mut committed_nodes_by_id: NodeIdMap<NodeRecord> = NodeIdMap::default();
        if !inputs.is_empty() {
            let mut distinct_keys = Vec::new();
            let mut seen_keys = HashSet::new();
            for (input, label_set) in inputs.iter().zip(label_sets.iter().copied()) {
                for &label_id in label_set.as_slice() {
                    if seen_keys.insert((label_id, input.key.as_str())) {
                        distinct_keys.push((label_id, input.key.as_str()));
                    }
                }
            }
            let existing_nodes = self.get_nodes_by_label_keys_raw(&distinct_keys)?;
            for ((label_id, key), existing) in distinct_keys.into_iter().zip(existing_nodes) {
                if let Some(node) = existing {
                    committed_nodes_by_id
                        .entry(node.id)
                        .or_insert_with(|| node.clone());
                    committed_keys.insert((label_id, key.to_string()), node);
                }
            }
        }

        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();
        let mut removed_keys: HashSet<(u32, String)> = HashSet::new();
        let mut staged_label_sets: NodeIdMap<NodeLabelSet> = NodeIdMap::default();
        let mut next_node_id = self.next_node_id;
        let mut records = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());

        for ((input, &label_set), (dense_vector, sparse_vector)) in inputs
            .iter()
            .zip(label_sets.iter())
            .zip(normalized_vectors)
        {
            let mut winner: Option<(u64, i64)> = None;
            for &label_id in label_set.as_slice() {
                let key_tuple = (label_id, input.key.clone());
                let membership = batch_keys
                    .get(&key_tuple)
                    .copied()
                    .or_else(|| {
                        if removed_keys.contains(&key_tuple) {
                            None
                        } else {
                            committed_keys
                                .get(&key_tuple)
                                .map(|node| (node.id, node.created_at))
                        }
                    });
                if let Some((id, created_at)) = membership {
                    match winner {
                        Some((winner_id, _)) if winner_id != id => {
                            return Err(node_key_conflict_error(&input.key, winner_id, id));
                        }
                        None => winner = Some((id, created_at)),
                        _ => {}
                    }
                }
            }

            let (id, created_at) = match winner {
                Some(existing) => existing,
                None => {
                    let id = next_node_id;
                    next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
                        EngineError::InvalidOperation("node id counter overflow".into())
                    })?;
                    (id, now)
                }
            };

            let previous_labels = staged_label_sets
                .get(&id)
                .copied()
                .or_else(|| committed_nodes_by_id.get(&id).map(|node| node.label_ids));
            if let Some(previous_labels) = previous_labels {
                for &old_label_id in previous_labels.as_slice() {
                    if !label_set.contains(old_label_id) {
                        let key_tuple = (old_label_id, input.key.clone());
                        batch_keys.remove(&key_tuple);
                        removed_keys.insert(key_tuple);
                    }
                }
            }
            for &new_label_id in label_set.as_slice() {
                let key_tuple = (new_label_id, input.key.clone());
                removed_keys.remove(&key_tuple);
                batch_keys.insert(key_tuple, (id, created_at));
            }
            staged_label_sets.insert(id, label_set);

            records.push(NodeRecord {
                id,
                label_ids: label_set,
                key: input.key.clone(),
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                dense_vector,
                sparse_vector,
                last_write_seq: 0,
            });
            ids.push(id);
        }

        Ok((records, ids, next_node_id))
    }

    fn plan_add_node_label(
        &mut self,
        id: u64,
        label: &str,
    ) -> Result<CoreWritePlan, EngineError> {
        validate_label_token_name(label)?;
        let current = self.get_nodes_raw(&[id])?.into_iter().next().flatten().ok_or_else(|| {
            EngineError::InvalidOperation(format!("node {id} does not exist"))
        })?;

        let catalog = self.label_catalog.read().unwrap();
        let existing_label_id = catalog.node_label_to_id.get(label).copied();
        if existing_label_id.is_some_and(|label_id| current.label_ids.contains(label_id)) {
            return Ok(CoreWritePlan {
                ops: Vec::new(),
                reply: CoreWriteReply::Bool(false),
                auto_flush: false,
                track_ids: false,
                label_catalog_changed: false,
            });
        }
        if current.label_ids.len() == MAX_NODE_LABELS_PER_NODE {
            return Err(EngineError::InvalidOperation(format!(
                "node label set must contain at most {} labels",
                MAX_NODE_LABELS_PER_NODE
            )));
        }

        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_id = match existing_label_id {
            Some(label_id) => label_id,
            None => label_plan.resolve_node_label_for_write(label)?,
        };
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;

        if let Some((existing_id, _)) = self.find_existing_node(label_id, &current.key)? {
            if existing_id != current.id {
                return Err(node_key_conflict_error(&current.key, current.id, existing_id));
            }
        }

        let mut next_labels = current.label_ids.as_slice().to_vec();
        next_labels.push(label_id);
        let label_ids = NodeLabelSet::from_label_ids(next_labels)?;
        let mut ops = Vec::with_capacity(token_op_count + 1);
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);
        ops.push(WalOp::UpsertNode(NodeRecord {
            updated_at: now_millis(),
            label_ids,
            ..current
        }));

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::Bool(true),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn plan_remove_node_label(
        &mut self,
        id: u64,
        label: &str,
    ) -> Result<CoreWritePlan, EngineError> {
        validate_label_token_name(label)?;
        let current = self.get_nodes_raw(&[id])?.into_iter().next().flatten().ok_or_else(|| {
            EngineError::InvalidOperation(format!("node {id} does not exist"))
        })?;

        let label_id = {
            let catalog = self.label_catalog.read().unwrap();
            match resolve_node_label_for_read(&catalog, label)? {
                Some(label_id) => label_id,
                None => {
                    return Ok(CoreWritePlan {
                        ops: Vec::new(),
                        reply: CoreWriteReply::Bool(false),
                        auto_flush: false,
                        track_ids: false,
                        label_catalog_changed: false,
                    });
                }
            }
        };

        if !current.label_ids.contains(label_id) {
            return Ok(CoreWritePlan {
                ops: Vec::new(),
                reply: CoreWriteReply::Bool(false),
                auto_flush: false,
                track_ids: false,
                label_catalog_changed: false,
            });
        }
        if current.label_ids.len() == 1 {
            return Err(EngineError::InvalidOperation(
                "cannot remove the last node label".to_string(),
            ));
        }

        let next_labels = current
            .label_ids
            .as_slice()
            .iter()
            .copied()
            .filter(|&existing| existing != label_id);
        let label_ids = NodeLabelSet::from_label_ids(next_labels)?;
        Ok(CoreWritePlan {
            ops: vec![WalOp::UpsertNode(NodeRecord {
                updated_at: now_millis(),
                label_ids,
                ..current
            })],
            reply: CoreWriteReply::Bool(true),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed: false,
        })
    }

    fn plan_upsert_edge(
        &mut self,
        from: u64,
        to: u64,
        label: &str,
        options: &UpsertEdgeOptions,
    ) -> Result<CoreWritePlan, EngineError> {
        let (label_id, should_create_token) = self
            .label_catalog
            .read()
            .unwrap()
            .reserve_edge_label(label)?;
        let now = now_millis();

        let (id, created_at) = if self.edge_uniqueness {
            match self.find_existing_edge(from, to, label_id)? {
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
            label_id,
            props: options.props.clone(),
            created_at,
            updated_at: now,
            weight: options.weight,
            valid_from: options.valid_from.unwrap_or(created_at),
            valid_to: options.valid_to.unwrap_or(i64::MAX),
            last_write_seq: 0,
        };

        let mut ops = Vec::with_capacity(1 + usize::from(should_create_token));
        if should_create_token {
            ops.push(WalOp::EnsureEdgeLabel {
                label: label.to_string(),
                label_id,
            });
        }
        ops.push(WalOp::UpsertEdge(edge));

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::U64(id),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed: should_create_token,
        })
    }

    fn plan_batch_upsert_nodes(
        &mut self,
        inputs: &[NodeInput],
    ) -> Result<CoreWritePlan, EngineError> {
        let mut validated_labels = Vec::with_capacity(inputs.len());
        let mut normalized_vectors = Vec::with_capacity(inputs.len());
        for input in inputs {
            validated_labels.push(ValidatedNodeLabelList::new(
                input.labels.iter().map(String::as_str),
            )?);
            validate_node_key_for_write(&input.key)?;
            normalized_vectors.push(normalize_node_vectors_for_write(
                self.manifest.dense_vector.as_ref(),
                input.dense_vector.as_ref(),
                input.sparse_vector.as_ref(),
            )?);
        }

        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_sets =
            label_plan.resolve_validated_node_label_sets_for_request(&validated_labels)?;
        let now = now_millis();
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;
        let mut ops = Vec::with_capacity(token_op_count + inputs.len());
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);
        let (records, ids, next_node_id) =
            self.plan_node_upsert_records(inputs, &label_sets, normalized_vectors, now)?;
        if next_node_id != self.next_node_id {
            self.next_node_id = next_node_id;
        }
        ops.extend(records.into_iter().map(WalOp::UpsertNode));

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::VecU64(ids),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn plan_batch_upsert_edges(
        &mut self,
        inputs: &[EdgeInput],
    ) -> Result<CoreWritePlan, EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_ids = label_plan.resolve_edge_label_ids_for_request(
            inputs.iter().map(|input| input.label.as_str()),
        )?;
        let now = now_millis();
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;
        let mut ops = Vec::with_capacity(token_op_count + inputs.len());
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);
        let mut ids = Vec::with_capacity(inputs.len());
        let mut committed_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();
        if self.edge_uniqueness && !inputs.is_empty() {
            let mut distinct_triples = Vec::new();
            let mut seen_triples = HashSet::new();
            for (input, &label_id) in inputs.iter().zip(label_ids.iter()) {
                let triple = (input.from, input.to, label_id);
                if seen_triples.insert(triple) {
                    distinct_triples.push(triple);
                }
            }
            let existing_edges = self.find_existing_edges_batch(&distinct_triples)?;
            for (triple, existing) in distinct_triples.into_iter().zip(existing_edges) {
                if let Some((id, created_at)) = existing {
                    committed_triples.insert(triple, (id, created_at));
                }
            }
        }
        let mut batch_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();

        for (input, label_id) in inputs.iter().zip(label_ids.iter().copied()) {
            let triple = (input.from, input.to, label_id);

            let (id, created_at) = if self.edge_uniqueness {
                if let Some(&(id, created_at)) = batch_triples.get(&triple) {
                    (id, created_at)
                } else if let Some(&(id, created_at)) = committed_triples.get(&triple) {
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
                label_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
                last_write_seq: 0,
            }));
            ids.push(id);
        }

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::VecU64(ids),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn plan_delete_node(&mut self, id: u64) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let incident = self.neighbors_raw(id, Direction::Both, None, 0, None, None, None)?;
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

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::Unit,
            auto_flush: true,
            track_ids: false,
            label_catalog_changed: false,
        })
    }

    fn plan_delete_edge(&mut self, id: u64) -> Result<CoreWritePlan, EngineError> {
        Ok(CoreWritePlan {
            ops: vec![WalOp::DeleteEdge {
                id,
                deleted_at: now_millis(),
            }],
            reply: CoreWriteReply::Unit,
            auto_flush: true,
            track_ids: false,
            label_catalog_changed: false,
        })
    }

    fn plan_invalidate_edge(
        &mut self,
        id: u64,
        valid_to: i64,
    ) -> Result<CoreWritePlan, EngineError> {
        let edge = match self.get_edge(id)? {
            Some(edge) => edge,
            None => {
                return Ok(CoreWritePlan {
                    ops: Vec::new(),
                    reply: CoreWriteReply::OptionEdge(None),
                    auto_flush: true,
                    track_ids: false,
                    label_catalog_changed: false,
                });
            }
        };

        let updated = EdgeRecord {
            updated_at: now_millis(),
            valid_to,
            ..edge
        };

        Ok(CoreWritePlan {
            ops: vec![WalOp::UpsertEdge(updated.clone())],
            reply: CoreWriteReply::OptionEdge(Some(updated)),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed: false,
        })
    }

    #[cfg(test)]
    fn plan_write_op(&mut self, op: &WalOp) -> Result<CoreWritePlan, EngineError> {
        let normalized = normalize_wal_op_for_write(self.manifest.dense_vector.as_ref(), op)?;
        self.validate_wal_op_label_tokens_for_write(&normalized)?;
        Ok(CoreWritePlan {
            ops: vec![normalized],
            reply: CoreWriteReply::Unit,
            auto_flush: false,
            track_ids: true,
            label_catalog_changed: matches!(
                op,
                WalOp::EnsureNodeLabel { .. } | WalOp::EnsureEdgeLabel { .. }
            ),
        })
    }

    #[cfg(test)]
    fn plan_write_op_batch(&mut self, ops: &[WalOp]) -> Result<CoreWritePlan, EngineError> {
        let normalized_ops: Vec<WalOp> = ops
            .iter()
            .map(|op| normalize_wal_op_for_write(self.manifest.dense_vector.as_ref(), op))
            .collect::<Result<_, _>>()?;
        for op in &normalized_ops {
            self.validate_wal_op_label_tokens_for_write(op)?;
        }
        Ok(CoreWritePlan {
            ops: normalized_ops,
            reply: CoreWriteReply::Unit,
            auto_flush: false,
            track_ids: true,
            label_catalog_changed: ops.iter().any(|op| {
                matches!(
                    op,
                    WalOp::EnsureNodeLabel { .. } | WalOp::EnsureEdgeLabel { .. }
                )
            }),
        })
    }

    fn plan_graph_patch(&mut self, patch: &GraphPatch) -> Result<CoreWritePlan, EngineError> {
        let mut validated_node_labels = Vec::with_capacity(patch.upsert_nodes.len());
        let mut normalized_node_vectors = Vec::with_capacity(patch.upsert_nodes.len());
        for input in &patch.upsert_nodes {
            validated_node_labels.push(ValidatedNodeLabelList::new(
                input.labels.iter().map(String::as_str),
            )?);
            validate_node_key_for_write(&input.key)?;
            normalized_node_vectors.push(normalize_node_vectors_for_write(
                self.manifest.dense_vector.as_ref(),
                input.dense_vector.as_ref(),
                input.sparse_vector.as_ref(),
            )?);
        }

        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let node_label_sets =
            label_plan.resolve_validated_node_label_sets_for_request(&validated_node_labels)?;
        let edge_label_ids = label_plan.resolve_edge_label_ids_for_request(
            patch
                .upsert_edges
                .iter()
                .map(|input| input.label.as_str()),
        )?;
        let now = now_millis();
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;
        let mut ops: Vec<WalOp> = Vec::with_capacity(
            token_op_count
                + patch.upsert_nodes.len()
                + patch.upsert_edges.len()
                + patch.invalidate_edges.len()
                + patch.delete_edge_ids.len()
                + patch.delete_node_ids.len(),
        );
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);

        let (node_records, node_ids, next_node_id) = self.plan_node_upsert_records(
            &patch.upsert_nodes,
            &node_label_sets,
            normalized_node_vectors,
            now,
        )?;
        ops.extend(node_records.into_iter().map(WalOp::UpsertNode));

        let mut edge_ids = Vec::with_capacity(patch.upsert_edges.len());
        let mut committed_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();
        if self.edge_uniqueness && !patch.upsert_edges.is_empty() {
            let mut distinct_triples = Vec::new();
            let mut seen_triples = HashSet::new();
            for (input, &label_id) in patch.upsert_edges.iter().zip(edge_label_ids.iter()) {
                let triple = (input.from, input.to, label_id);
                if seen_triples.insert(triple) {
                    distinct_triples.push(triple);
                }
            }
            let existing_edges = self.find_existing_edges_batch(&distinct_triples)?;
            for (triple, existing) in distinct_triples.into_iter().zip(existing_edges) {
                if let Some((id, created_at)) = existing {
                    committed_triples.insert(triple, (id, created_at));
                }
            }
        }
        let mut batch_triples: HashMap<(u64, u64, u32), (u64, i64)> = HashMap::new();
        let mut staged_edge_op_idx: HashMap<u64, usize> = HashMap::new();
        let mut staged_incident_edges: HashMap<u64, Vec<u64>> = HashMap::new();

        for (input, label_id) in patch.upsert_edges.iter().zip(edge_label_ids.iter().copied()) {
            let triple = (input.from, input.to, label_id);
            let (id, created_at) = if self.edge_uniqueness {
                if let Some(&(id, created_at)) = batch_triples.get(&triple) {
                    (id, created_at)
                } else if let Some(&(id, created_at)) = committed_triples.get(&triple) {
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
            let edge = EdgeRecord {
                id,
                from: input.from,
                to: input.to,
                label_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
                last_write_seq: 0,
            };
            staged_incident_edges.entry(edge.from).or_default().push(id);
            if edge.to != edge.from {
                staged_incident_edges.entry(edge.to).or_default().push(id);
            }
            let op_idx = ops.len();
            ops.push(WalOp::UpsertEdge(edge));
            staged_edge_op_idx.insert(id, op_idx);
            edge_ids.push(id);
        }

        if !patch.invalidate_edges.is_empty() {
            let mut inv_lookup_ids = Vec::new();
            let mut inv_lookup_positions = HashMap::new();
            for &(id, _) in &patch.invalidate_edges {
                if !staged_edge_op_idx.contains_key(&id)
                    && !inv_lookup_positions.contains_key(&id)
                {
                    inv_lookup_positions.insert(id, inv_lookup_ids.len());
                    inv_lookup_ids.push(id);
                }
            }
            let committed_inv_edges = self.get_edges(&inv_lookup_ids)?;

            for &(id, valid_to) in &patch.invalidate_edges {
                if let Some(&op_idx) = staged_edge_op_idx.get(&id) {
                    let mut edge = match ops.get(op_idx) {
                        Some(WalOp::UpsertEdge(edge)) => edge.clone(),
                        _ => {
                            return Err(EngineError::InvalidOperation(
                                "staged edge overlay pointed at a non-edge WAL op".into(),
                            ));
                        }
                    };
                    edge.updated_at = now;
                    edge.valid_to = valid_to;
                    let updated_op_idx = ops.len();
                    ops.push(WalOp::UpsertEdge(edge));
                    staged_edge_op_idx.insert(id, updated_op_idx);
                    continue;
                }

                if let Some(&idx) = inv_lookup_positions.get(&id) {
                    if let Some(edge) = committed_inv_edges[idx].as_ref() {
                        let updated = EdgeRecord {
                            updated_at: now,
                            valid_to,
                            ..edge.clone()
                        };
                        staged_incident_edges
                            .entry(updated.from)
                            .or_default()
                            .push(updated.id);
                        if updated.to != updated.from {
                            staged_incident_edges
                                .entry(updated.to)
                                .or_default()
                                .push(updated.id);
                        }
                        let op_idx = ops.len();
                        ops.push(WalOp::UpsertEdge(updated));
                        staged_edge_op_idx.insert(id, op_idx);
                    }
                }
            }
        }

        let mut deleted_edge_ids = HashSet::new();
        for &eid in &patch.delete_edge_ids {
            if deleted_edge_ids.insert(eid) {
                ops.push(WalOp::DeleteEdge {
                    id: eid,
                    deleted_at: now,
                });
            }
        }

        let patch_tombstones = if patch.delete_node_ids.is_empty() {
            None
        } else {
            Some(self.collect_tombstones())
        };
        for &nid in &patch.delete_node_ids {
            let ts = patch_tombstones.as_ref().map(|(dn, de)| (dn, de));
            let incident = self.neighbors_raw(nid, Direction::Both, None, 0, None, None, ts)?;
            for entry in &incident {
                if deleted_edge_ids.insert(entry.edge_id) {
                    ops.push(WalOp::DeleteEdge {
                        id: entry.edge_id,
                        deleted_at: now,
                    });
                }
            }
            if let Some(staged_incident) = staged_incident_edges.get(&nid) {
                for &eid in staged_incident {
                    if deleted_edge_ids.insert(eid) {
                        ops.push(WalOp::DeleteEdge {
                            id: eid,
                            deleted_at: now,
                        });
                    }
                }
            }
            ops.push(WalOp::DeleteNode {
                id: nid,
                deleted_at: now,
            });
        }

        if next_node_id != self.next_node_id {
            self.next_node_id = next_node_id;
        }

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::PatchResult(PatchResult { node_ids, edge_ids }),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    fn plan_prune(&mut self, policy: &PrunePolicy) -> Result<CoreWritePlan, EngineError> {
        validate_prune_policy(policy)?;
        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_id = policy
            .label
            .as_deref()
            .map(|label| label_plan.resolve_node_label_for_write(label))
            .transpose()?;
        let token_op_count = label_plan.token_op_count();
        let label_catalog_changed = token_op_count > 0;
        let mut ops = Vec::with_capacity(token_op_count);
        label_plan.push_token_ops(&mut ops);
        drop(label_plan);
        drop(catalog);
        let resolved_policy = ResolvedPrunePolicy {
            max_age_ms: policy.max_age_ms,
            max_weight: policy.max_weight,
            label_id,
        };

        let now = now_millis();
        let targets = self.collect_prune_targets(&resolved_policy, now)?;
        if targets.is_empty() {
            return Ok(CoreWritePlan {
                ops,
                reply: CoreWriteReply::PruneResult(PruneResult {
                    nodes_pruned: 0,
                    edges_pruned: 0,
                }),
                auto_flush: true,
                track_ids: false,
                label_catalog_changed,
            });
        }

        let mut edges_seen = NodeIdSet::default();
        let prune_tombstones = self.collect_tombstones();

        for &nid in &targets {
            let incident = self.neighbors_raw(
                nid,
                Direction::Both,
                None,
                0,
                None,
                None,
                Some((&prune_tombstones.0, &prune_tombstones.1)),
            )?;
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

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::PruneResult(PruneResult {
                nodes_pruned: targets.len() as u64,
                edges_pruned: edges_seen.len() as u64,
            }),
            auto_flush: true,
            track_ids: false,
            label_catalog_changed,
        })
    }

    /// Collect node IDs matching the prune policy by scanning memtable + segments.
    /// When `label_id` is set, uses the label posting index for efficiency.
    /// Uses raw (unfiltered) reads. Prune must see ALL nodes, including those
    /// hidden by registered policies, to ensure correct deletion.
    fn collect_prune_targets(
        &self,
        policy: &ResolvedPrunePolicy,
        now: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let age_cutoff = policy.max_age_ms.map(|age| now - age);

        if let Some(label_id) = policy.label_id {
            // Use the label posting index (raw). Must see all nodes including policy-excluded ones.
            // Latest visibility is verified from metadata below.
            let ids = self.nodes_by_label_id_raw(label_id)?;
            self.collect_prune_targets_from_candidates(ids, policy, age_cutoff)
        } else {
            // Scan all sources for candidate IDs without hydrating NodeRecords, then
            // latest-verify each candidate through SourceList visibility metadata.
            let mut seen = NodeIdSet::default();
            let mut candidates = Vec::new();

            // Active memtable nodes (freshest)
            for node_id in self.memtable.visible_node_ids_at(self.engine_seq) {
                if seen.insert(node_id) {
                    candidates.push(node_id);
                }
            }

            // Immutable memtable nodes (newest-first)
            for epoch in &self.immutable_epochs {
                for node_id in epoch.memtable.visible_node_ids_at(self.engine_seq) {
                    if seen.insert(node_id) {
                        candidates.push(node_id);
                    }
                }
            }

            // Segment node metadata (newest segments first, skip already-seen)
            for seg in &self.segments {
                for index in 0..seg.node_meta_count() as usize {
                    let meta = seg.node_meta_at(index)?;
                    if seen.insert(meta.node_id) {
                        candidates.push(meta.node_id);
                    }
                }
            }

            self.collect_prune_targets_from_candidates(candidates, policy, age_cutoff)
        }
    }

    fn collect_prune_targets_from_candidates(
        &self,
        candidates: Vec<u64>,
        policy: &ResolvedPrunePolicy,
        age_cutoff: Option<i64>,
    ) -> Result<Vec<u64>, EngineError> {
        let visibility = self.sources().find_node_visibility_meta(&candidates)?;
        let mut targets = Vec::new();
        for (&node_id, state) in candidates.iter().zip(visibility.iter()) {
            if let NodeVisibilityState::Live(meta) = state {
                if matches_prune_cutoff(
                    &meta.label_ids,
                    meta.updated_at,
                    meta.weight,
                    age_cutoff,
                    policy.max_weight,
                    policy.label_id,
                ) {
                    targets.push(node_id);
                }
            }
        }
        Ok(targets)
    }

    // --- Named prune policies (compaction-filter auto-prune) ---

    /// Register a named prune policy. Persisted in the manifest and applied
    /// automatically during compaction. Multiple named policies are allowed;
    /// a node matching ANY policy is pruned (OR across policies, AND within).
    pub fn set_prune_policy(
        &mut self,
        name: &str,
        policy: PrunePolicy,
    ) -> Result<PublishImpact, EngineError> {
        validate_prune_policy(&policy)?;

        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let mut node_labels_to_stage = Vec::new();
        if let Some(label) = policy.label.as_deref() {
        let label_id = label_plan.resolve_node_label_for_write(label)?;
        node_labels_to_stage.push((label.to_string(), label_id));
        }
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog);

        if self
            .manifest
            .prune_policies
            .get(name)
            .is_some_and(|existing| existing == &policy)
        {
            return Ok(PublishImpact::NoPublish);
        }

        let name = name.to_string();
        self.with_runtime_manifest_write(|manifest| {
            stage_label_tokens_in_manifest(manifest, &node_labels_to_stage, &[])?;
            manifest.prune_policies.insert(name, policy);
            Ok(())
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;
        Ok(PublishImpact::RebuildSources)
    }

    /// Remove a named prune policy. Returns true if it existed.
    pub fn remove_prune_policy(
        &mut self,
        name: &str,
    ) -> Result<(bool, PublishImpact), EngineError> {
        if !self.manifest.prune_policies.contains_key(name) {
            return Ok((false, PublishImpact::NoPublish));
        }
        let name = name.to_string();
        let removed = self.with_runtime_manifest_write(|manifest| {
            Ok(manifest.prune_policies.remove(&name).is_some())
        })?;
        Ok((
            removed,
            if removed {
                PublishImpact::RebuildSources
            } else {
                PublishImpact::NoPublish
            },
        ))
    }

    /// List all registered prune policies.
    pub fn list_prune_policies(&self) -> Result<Vec<PrunePolicyInfo>, EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        self.manifest
            .prune_policies
            .iter()
            .map(|(name, policy)| {
                let resolved = resolve_manifest_prune_policy(policy, &catalog)?;
                let policy = public_prune_policy_from_resolved(&resolved, &catalog)?;
                Ok(PrunePolicyInfo {
                    name: name.clone(),
                    policy,
                })
            })
            .collect()
    }

    fn node_property_index_info(
        entry: &SecondaryIndexManifestEntry,
        catalog: &impl LabelCatalogLookup,
    ) -> Result<NodePropertyIndexInfo, EngineError> {
        Ok(match &entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, prop_key } => NodePropertyIndexInfo {
                index_id: entry.index_id,
                label: catalog
                    .node_label(*label_id)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        EngineError::ManifestError(format!(
                            "node property index {} references missing node label label_id {}",
                            entry.index_id, label_id
                        ))
                    })?,
                prop_key: prop_key.clone(),
                kind: entry.kind.clone(),
                state: entry.state,
                last_error: entry.last_error.clone(),
            },
            SecondaryIndexTarget::EdgeProperty { .. } => {
                unreachable!("node_property_index_info called with EdgeProperty target")
            }
        })
    }

    fn edge_property_index_info(
        entry: &SecondaryIndexManifestEntry,
        catalog: &impl LabelCatalogLookup,
    ) -> Result<EdgePropertyIndexInfo, EngineError> {
        Ok(match &entry.target {
            SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => EdgePropertyIndexInfo {
                index_id: entry.index_id,
                label: catalog
                    .edge_label(*label_id)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        EngineError::ManifestError(format!(
                            "edge property index {} references missing edge-label label_id {}",
                            entry.index_id, label_id
                        ))
                    })?,
                prop_key: prop_key.clone(),
                kind: entry.kind.clone(),
                state: entry.state,
                last_error: entry.last_error.clone(),
            },
            SecondaryIndexTarget::NodeProperty { .. } => {
                unreachable!("edge_property_index_info called with NodeProperty target")
            }
        })
    }

    pub fn ensure_node_property_index(
        &mut self,
        label: &str,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(NodePropertyIndexInfo, PublishImpact), EngineError> {
        enum EnsureOutcome {
            Existing,
            New,
            Retry,
        }

        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_id = label_plan.resolve_node_label_for_write(label)?;
        let node_labels_to_stage = vec![(label.to_string(), label_id)];
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog);
        let prop_key = prop_key.to_string();
        let (entry, outcome) = self.with_runtime_manifest_write(|manifest| {
            stage_label_tokens_in_manifest(manifest, &node_labels_to_stage, &[])?;
            if let Some(existing) = manifest.secondary_indexes.iter_mut().find(|entry| {
                entry.target
                    == SecondaryIndexTarget::NodeProperty {
                        label_id,
                        prop_key: prop_key.clone(),
                    }
                    && entry.kind == kind
            }) {
                if existing.state == SecondaryIndexState::Failed {
                    existing.state = SecondaryIndexState::Building;
                    existing.last_error = None;
                    return Ok((existing.clone(), EnsureOutcome::Retry));
                }
                return Ok((existing.clone(), EnsureOutcome::Existing));
            }

            let entry = SecondaryIndexManifestEntry {
                index_id: manifest.next_secondary_index_id,
                target: SecondaryIndexTarget::NodeProperty {
                    label_id,
                    prop_key: prop_key.clone(),
                },
                kind: kind.clone(),
                state: SecondaryIndexState::Building,
                last_error: None,
            };
            manifest.next_secondary_index_id = manifest.next_secondary_index_id.saturating_add(1);
            manifest.secondary_indexes.push(entry.clone());
            Ok((entry, EnsureOutcome::New))
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;

        let publish_impact = match outcome {
            EnsureOutcome::Existing => PublishImpact::NoPublish,
            EnsureOutcome::New => {
                self.rebuild_secondary_index_catalog()?;
                self.seed_secondary_index_entry(&entry)?;
                self.enqueue_secondary_index_job(SecondaryIndexJob::Build {
                    index_id: entry.index_id,
                });
                PublishImpact::RebuildSources
            }
            EnsureOutcome::Retry => {
                self.rebuild_secondary_index_catalog()?;
                self.remove_secondary_index_entry_from_memtables(entry.index_id)?;
                self.seed_secondary_index_entry(&entry)?;
                self.enqueue_secondary_index_job(SecondaryIndexJob::Build {
                    index_id: entry.index_id,
                });
                PublishImpact::RebuildSources
            }
        };

        let catalog = self.label_catalog.read().unwrap();
        Ok((
            Self::node_property_index_info(&entry, &*catalog)?,
            publish_impact,
        ))
    }

    pub fn drop_node_property_index(
        &mut self,
        label: &str,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(bool, PublishImpact), EngineError> {
        let label_id = {
            let catalog = self.label_catalog.read().unwrap();
            let Some(label_id) = resolve_node_label_for_read(&catalog, label)? else {
                return Ok((false, PublishImpact::NoPublish));
            };
            label_id
        };
        let prop_key = prop_key.to_string();
        let removed = self.with_runtime_manifest_write(|manifest| {
            let idx = manifest.secondary_indexes.iter().position(|entry| {
                entry.target
                    == SecondaryIndexTarget::NodeProperty {
                        label_id,
                        prop_key: prop_key.clone(),
                    }
                    && entry.kind == kind
            });
            Ok(idx.map(|idx| manifest.secondary_indexes.remove(idx)))
        })?;

        let Some(entry) = removed else {
            return Ok((false, PublishImpact::NoPublish));
        };

        self.rebuild_secondary_index_catalog()?;
        self.remove_secondary_index_entry_from_memtables(entry.index_id)?;
        self.enqueue_secondary_index_job(SecondaryIndexJob::DropCleanup { entry });
        Ok((true, PublishImpact::RebuildSources))
    }

    pub fn ensure_edge_property_index(
        &mut self,
        label: &str,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(EdgePropertyIndexInfo, PublishImpact), EngineError> {
        enum EnsureOutcome {
            Existing,
            New,
            Retry,
        }

        let catalog = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog);
        let label_id = label_plan.resolve_edge_label_for_write(label)?;
        let edge_labels_to_stage = vec![(label.to_string(), label_id)];
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog);
        let prop_key = prop_key.to_string();
        let (entry, outcome) = self.with_runtime_manifest_write(|manifest| {
            stage_label_tokens_in_manifest(manifest, &[], &edge_labels_to_stage)?;
            if let Some(existing) = manifest.secondary_indexes.iter_mut().find(|entry| {
                entry.target
                    == SecondaryIndexTarget::EdgeProperty {
                        label_id,
                        prop_key: prop_key.clone(),
                    }
                    && entry.kind == kind
            }) {
                if existing.state == SecondaryIndexState::Failed {
                    existing.state = SecondaryIndexState::Building;
                    existing.last_error = None;
                    return Ok((existing.clone(), EnsureOutcome::Retry));
                }
                return Ok((existing.clone(), EnsureOutcome::Existing));
            }

            let entry = SecondaryIndexManifestEntry {
                index_id: manifest.next_secondary_index_id,
                target: SecondaryIndexTarget::EdgeProperty {
                    label_id,
                    prop_key: prop_key.clone(),
                },
                kind: kind.clone(),
                state: SecondaryIndexState::Building,
                last_error: None,
            };
            manifest.next_secondary_index_id = manifest.next_secondary_index_id.saturating_add(1);
            manifest.secondary_indexes.push(entry.clone());
            Ok((entry, EnsureOutcome::New))
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;

        let publish_impact = match outcome {
            EnsureOutcome::Existing => PublishImpact::NoPublish,
            EnsureOutcome::New => {
                self.rebuild_secondary_index_catalog()?;
                self.seed_secondary_index_entry(&entry)?;
                self.enqueue_secondary_index_job(SecondaryIndexJob::Build {
                    index_id: entry.index_id,
                });
                PublishImpact::RebuildSources
            }
            EnsureOutcome::Retry => {
                self.rebuild_secondary_index_catalog()?;
                self.remove_secondary_index_entry_from_memtables(entry.index_id)?;
                self.seed_secondary_index_entry(&entry)?;
                self.enqueue_secondary_index_job(SecondaryIndexJob::Build {
                    index_id: entry.index_id,
                });
                PublishImpact::RebuildSources
            }
        };

        let catalog = self.label_catalog.read().unwrap();
        Ok((
            Self::edge_property_index_info(&entry, &*catalog)?,
            publish_impact,
        ))
    }

    pub fn drop_edge_property_index(
        &mut self,
        label: &str,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(bool, PublishImpact), EngineError> {
        let label_id = {
            let catalog = self.label_catalog.read().unwrap();
            let Some(label_id) = resolve_edge_label_for_read(&catalog, label)? else {
                return Ok((false, PublishImpact::NoPublish));
            };
            label_id
        };
        let prop_key = prop_key.to_string();
        let removed = self.with_runtime_manifest_write(|manifest| {
            let idx = manifest.secondary_indexes.iter().position(|entry| {
                entry.target
                    == SecondaryIndexTarget::EdgeProperty {
                        label_id,
                        prop_key: prop_key.clone(),
                    }
                    && entry.kind == kind
            });
            Ok(idx.map(|idx| manifest.secondary_indexes.remove(idx)))
        })?;

        let Some(entry) = removed else {
            return Ok((false, PublishImpact::NoPublish));
        };

        self.rebuild_secondary_index_catalog()?;
        self.remove_secondary_index_entry_from_memtables(entry.index_id)?;
        self.enqueue_secondary_index_job(SecondaryIndexJob::DropCleanup { entry });
        Ok((true, PublishImpact::RebuildSources))
    }

    fn require_node_label_token_for_numeric_stub(&self, label_id: u32) -> Result<(), EngineError> {
        if self
            .label_catalog
            .read()
            .unwrap()
            .node_id_to_label
            .contains_key(&label_id)
        {
            Ok(())
        } else {
            Err(EngineError::InvalidOperation(format!(
                "numeric node label_id {label_id} is not present in the node label catalog; call ensure_node_label first"
            )))
        }
    }

    fn require_edge_label_token_for_numeric_stub(&self, label_id: u32) -> Result<(), EngineError> {
        if self
            .label_catalog
            .read()
            .unwrap()
            .edge_id_to_label
            .contains_key(&label_id)
        {
            Ok(())
        } else {
            Err(EngineError::InvalidOperation(format!(
                "numeric edge-label label_id {label_id} is not present in the edge-label catalog; call ensure_edge_label first"
            )))
        }
    }

    #[allow(dead_code)]
    fn validate_wal_op_label_tokens_for_write(&self, op: &WalOp) -> Result<(), EngineError> {
        match op {
            WalOp::UpsertNode(node) => {
                for &label_id in node.label_ids.as_slice() {
                    self.require_node_label_token_for_numeric_stub(label_id)?;
                }
                Ok(())
            }
            WalOp::UpsertEdge(edge) => self.require_edge_label_token_for_numeric_stub(edge.label_id),
            WalOp::EnsureNodeLabel { label, label_id } => {
                validate_label_token_name(label)?;
                let catalog = self.label_catalog.read().unwrap();
                if let Some(existing_id) = catalog.node_label_to_id.get(label) {
                    if *existing_id != *label_id {
                        return Err(EngineError::InvalidOperation(format!(
                            "node label token conflict: label '{label}' already has label_id {existing_id}, not {label_id}"
                        )));
                    }
                    return Ok(());
                }
                if catalog.node_id_to_label.contains_key(label_id) {
                    return Err(EngineError::InvalidOperation(format!(
                        "node label token conflict: label_id {label_id} is already assigned"
                    )));
                }
                Ok(())
            }
            WalOp::EnsureEdgeLabel { label, label_id } => {
                validate_label_token_name(label)?;
                let catalog = self.label_catalog.read().unwrap();
                if let Some(existing_id) = catalog.edge_label_to_id.get(label) {
                    if *existing_id != *label_id {
                        return Err(EngineError::InvalidOperation(format!(
                            "edge-label token conflict: edge label '{label}' already has label_id {existing_id}, not {label_id}"
                        )));
                    }
                    return Ok(());
                }
                if catalog.edge_id_to_label.contains_key(label_id) {
                    return Err(EngineError::InvalidOperation(format!(
                        "edge-label token conflict: label_id {label_id} is already assigned"
                    )));
                }
                Ok(())
            }
            WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => Ok(()),
            WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => Err(
                EngineError::InvalidOperation(
                    "WAL atomic batch markers cannot be submitted as write ops".into(),
                ),
            ),
        }
    }

    // --- Segment-aware dedup lookups (for upsert) ---

    /// Look up a node by (label_id, key) across memtable + segments.
    /// Used by upsert_node for dedup. Uses raw (unfiltered) lookup to prevent
    /// policy-excluded nodes from being treated as "not found" (which would
    /// allocate a duplicate ID, causing silent data corruption).
    fn find_existing_node(
        &self,
        label_id: u32,
        key: &str,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self
            .get_node_by_label_key_raw(label_id, key)?
            .map(|n| (n.id, n.created_at)))
    }

    fn find_existing_node_for_label_set(
        &self,
        label_ids: NodeLabelSet,
        key: &str,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        let key_lookups: Vec<(u32, &str)> = label_ids
            .as_slice()
            .iter()
            .map(|&label_id| (label_id, key))
            .collect();
        let existing = self.find_existing_nodes_batch(&key_lookups)?;
        let mut winner: Option<(u64, i64)> = None;
        for node in existing.into_iter().flatten() {
            match winner {
                Some((winner_id, _)) if winner_id != node.0 => {
                    return Err(node_key_conflict_error(key, winner_id, node.0));
                }
                None => winner = Some(node),
                _ => {}
            }
        }
        Ok(winner)
    }

    fn find_existing_nodes_batch(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<(u64, i64)>>, EngineError> {
        Ok(self
            .get_nodes_by_label_keys_raw(keys)?
            .into_iter()
            .map(|node| node.map(|n| (n.id, n.created_at)))
            .collect())
    }

    /// Look up an edge by (from, to, label_id) across memtable + segments.
    /// Used by upsert_edge for uniqueness enforcement. Delegates to public get_edge_by_triple.
    fn find_existing_edge(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self
            .get_edge_by_triple(from, to, label_id)?
            .map(|e| (e.id, e.created_at)))
    }

    fn find_existing_edges_batch(
        &self,
        triples: &[(u64, u64, u32)],
    ) -> Result<Vec<Option<(u64, i64)>>, EngineError> {
        Ok(self
            .sources()
            .find_edges_by_triples(triples)?
            .into_iter()
            .map(|edge| edge.map(|e| (e.id, e.created_at)))
            .collect())
    }
}
