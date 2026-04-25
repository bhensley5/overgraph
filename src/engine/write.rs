// Write operations: upsert, delete, batch, patch, prune.
// This file is include!()'d into mod.rs. All items share the engine module scope.

impl EngineCore {
    fn commit_core_write_plan(
        &mut self,
        plan: CoreWritePlan,
    ) -> (Result<CoreWriteReply, EngineError>, PublishImpact) {
        let mut publish_impact = PublishImpact::NoPublish;

        let result = (|| -> Result<CoreWriteReply, EngineError> {
            match plan.ops.as_slice() {
                [] => {}
                [op] => self.append_and_apply_one_normalized(op)?,
                _ => self.append_and_apply_normalized(&plan.ops)?,
            }
            if !plan.ops.is_empty() {
                publish_impact = PublishImpact::SnapshotOnly;
            }

            if plan.track_ids {
                for op in &plan.ops {
                    self.track_id(op);
                }
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

    fn plan_core_write(&mut self, request: &CoreWriteRequest) -> Result<CoreWritePlan, EngineError> {
        match request {
            CoreWriteRequest::UpsertNode {
                type_id,
                key,
                options,
            } => self.plan_upsert_node(*type_id, key, options),
            CoreWriteRequest::UpsertEdge {
                from,
                to,
                type_id,
                options,
            } => self.plan_upsert_edge(*from, *to, *type_id, options),
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
            CoreWriteRequest::TxnCommit { request } => self.plan_txn_commit(request),
            CoreWriteRequest::Prune { policy } => self.plan_prune(policy),
            CoreWriteRequest::SetPrunePolicy { .. }
            | CoreWriteRequest::RemovePrunePolicy { .. }
            | CoreWriteRequest::EnsureNodePropertyIndex { .. }
            | CoreWriteRequest::DropNodePropertyIndex { .. }
            | CoreWriteRequest::ApplySecondaryIndexReadFollowup { .. }
            | CoreWriteRequest::Sync
            | CoreWriteRequest::Flush
            | CoreWriteRequest::IngestMode
            | CoreWriteRequest::EndIngest
            | CoreWriteRequest::Compact => Err(EngineError::InvalidOperation(
                "request does not use the planner write path".to_string(),
            )),
        }
    }

    fn plan_upsert_node(
        &mut self,
        type_id: u32,
        key: &str,
        options: &UpsertNodeOptions,
    ) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
            self.manifest.dense_vector.as_ref(),
            options.dense_vector.as_ref(),
            options.sparse_vector.as_ref(),
        )?;

        let (id, created_at) = match self.find_existing_node(type_id, key)? {
            Some((id, created_at)) => (id, created_at),
            None => {
                let id = self.next_node_id;
                self.next_node_id += 1;
                self.update_next_node_id_seen();
                (id, now)
            }
        };

        let node = NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props: options.props.clone(),
            created_at,
            updated_at: now,
            weight: options.weight,
            dense_vector,
            sparse_vector,
            last_write_seq: 0,
        };

        Ok(CoreWritePlan {
            ops: vec![WalOp::UpsertNode(node)],
            reply: CoreWriteReply::U64(id),
            auto_flush: true,
            track_ids: false,
        })
    }

    fn plan_upsert_edge(
        &mut self,
        from: u64,
        to: u64,
        type_id: u32,
        options: &UpsertEdgeOptions,
    ) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();

        let (id, created_at) = if self.edge_uniqueness {
            match self.find_existing_edge(from, to, type_id)? {
                Some((id, created_at)) => (id, created_at),
                None => {
                    let id = self.next_edge_id;
                    self.next_edge_id += 1;
                    self.update_next_edge_id_seen();
                    (id, now)
                }
            }
        } else {
            let id = self.next_edge_id;
            self.next_edge_id += 1;
            self.update_next_edge_id_seen();
            (id, now)
        };

        let edge = EdgeRecord {
            id,
            from,
            to,
            type_id,
            props: options.props.clone(),
            created_at,
            updated_at: now,
            weight: options.weight,
            valid_from: options.valid_from.unwrap_or(created_at),
            valid_to: options.valid_to.unwrap_or(i64::MAX),
            last_write_seq: 0,
        };

        Ok(CoreWritePlan {
            ops: vec![WalOp::UpsertEdge(edge)],
            reply: CoreWriteReply::U64(id),
            auto_flush: true,
            track_ids: false,
        })
    }

    fn plan_batch_upsert_nodes(&mut self, inputs: &[NodeInput]) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in inputs {
            let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
                self.manifest.dense_vector.as_ref(),
                input.dense_vector.as_ref(),
                input.sparse_vector.as_ref(),
            )?;
            let key_tuple = (input.type_id, input.key.clone());

            let (id, created_at) = if let Some(&(id, created_at)) = batch_keys.get(&key_tuple) {
                (id, created_at)
            } else if let Some((id, created_at)) =
                self.find_existing_node(input.type_id, &input.key)?
            {
                (id, created_at)
            } else {
                let id = self.next_node_id;
                self.next_node_id += 1;
                self.update_next_node_id_seen();
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
                dense_vector,
                sparse_vector,
                last_write_seq: 0,
            }));
            ids.push(id);
        }

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::VecU64(ids),
            auto_flush: true,
            track_ids: false,
        })
    }

    fn plan_batch_upsert_edges(&mut self, inputs: &[EdgeInput]) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
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
                    self.update_next_edge_id_seen();
                    (id, now)
                }
            } else {
                let id = self.next_edge_id;
                self.next_edge_id += 1;
                self.update_next_edge_id_seen();
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
                last_write_seq: 0,
            }));
            ids.push(id);
        }

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::VecU64(ids),
            auto_flush: true,
            track_ids: false,
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
        })
    }

    fn plan_invalidate_edge(&mut self, id: u64, valid_to: i64) -> Result<CoreWritePlan, EngineError> {
        let edge = match self.get_edge(id)? {
            Some(edge) => edge,
            None => {
                return Ok(CoreWritePlan {
                    ops: Vec::new(),
                    reply: CoreWriteReply::OptionEdge(None),
                    auto_flush: true,
                    track_ids: false,
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
        })
    }

    #[cfg(test)]
    fn plan_write_op(&mut self, op: &WalOp) -> Result<CoreWritePlan, EngineError> {
        let normalized = normalize_wal_op_for_write(self.manifest.dense_vector.as_ref(), op)?;
        Ok(CoreWritePlan {
            ops: vec![normalized],
            reply: CoreWriteReply::Unit,
            auto_flush: false,
            track_ids: true,
        })
    }

    #[cfg(test)]
    fn plan_write_op_batch(&mut self, ops: &[WalOp]) -> Result<CoreWritePlan, EngineError> {
        let normalized_ops: Vec<WalOp> = ops
            .iter()
            .map(|op| normalize_wal_op_for_write(self.manifest.dense_vector.as_ref(), op))
            .collect::<Result<_, _>>()?;
        Ok(CoreWritePlan {
            ops: normalized_ops,
            reply: CoreWriteReply::Unit,
            auto_flush: false,
            track_ids: true,
        })
    }

    fn plan_graph_patch(&mut self, patch: &GraphPatch) -> Result<CoreWritePlan, EngineError> {
        let now = now_millis();
        let mut ops: Vec<WalOp> = Vec::new();

        let mut node_ids = Vec::with_capacity(patch.upsert_nodes.len());
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in &patch.upsert_nodes {
            let (dense_vector, sparse_vector) = normalize_node_vectors_for_write(
                self.manifest.dense_vector.as_ref(),
                input.dense_vector.as_ref(),
                input.sparse_vector.as_ref(),
            )?;
            let key_tuple = (input.type_id, input.key.clone());
            let (id, created_at) = if let Some(&(id, created_at)) = batch_keys.get(&key_tuple) {
                (id, created_at)
            } else if let Some((id, created_at)) =
                self.find_existing_node(input.type_id, &input.key)?
            {
                (id, created_at)
            } else {
                let id = self.next_node_id;
                self.next_node_id += 1;
                self.update_next_node_id_seen();
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
                dense_vector,
                sparse_vector,
                last_write_seq: 0,
            }));
            node_ids.push(id);
        }

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
                    self.update_next_edge_id_seen();
                    (id, now)
                }
            } else {
                let id = self.next_edge_id;
                self.next_edge_id += 1;
                self.update_next_edge_id_seen();
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
                last_write_seq: 0,
            }));
            edge_ids.push(id);
        }

        if !patch.invalidate_edges.is_empty() {
            let inv_ids: Vec<u64> = patch.invalidate_edges.iter().map(|&(id, _)| id).collect();
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

        for &eid in &patch.delete_edge_ids {
            ops.push(WalOp::DeleteEdge {
                id: eid,
                deleted_at: now,
            });
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

        Ok(CoreWritePlan {
            ops,
            reply: CoreWriteReply::PatchResult(PatchResult { node_ids, edge_ids }),
            auto_flush: true,
            track_ids: false,
        })
    }

    fn plan_prune(&mut self, policy: &PrunePolicy) -> Result<CoreWritePlan, EngineError> {
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

        let now = now_millis();
        let targets = self.collect_prune_targets(policy, now)?;
        if targets.is_empty() {
            return Ok(CoreWritePlan {
                ops: Vec::new(),
                reply: CoreWriteReply::PruneResult(PruneResult {
                    nodes_pruned: 0,
                    edges_pruned: 0,
                }),
                auto_flush: true,
                track_ids: false,
            });
        }

        let mut ops = Vec::new();
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
            // Scan all nodes: active memtable first, then immutable memtables
            // (newest-first), then segments (newest-first), dedup by ID.
            // Flat tombstone set is safe here: monotonic ID allocation guarantees
            // tombstoned IDs are never re-upserted, so source precedence doesn't matter.
            let mut deleted = self.memtable.collect_deleted_nodes_at(u64::MAX);
            for epoch in &self.immutable_epochs {
                deleted.extend(epoch.memtable.collect_deleted_nodes_at(u64::MAX));
            }
            for seg in &self.segments {
                deleted.extend(seg.deleted_node_ids());
            }

            let mut seen = NodeIdSet::default();
            let mut targets = Vec::new();

            // Active memtable nodes (freshest)
            let _ = self.memtable.for_each_visible_node_at(u64::MAX, &mut |node| {
                if !deleted.contains(&node.id)
                    && seen.insert(node.id)
                    && Self::matches_prune_criteria(node, age_cutoff, policy.max_weight)
                {
                    targets.push(node.id);
                }
                ControlFlow::Continue(())
            });

            // Immutable memtable nodes (newest-first)
            for epoch in &self.immutable_epochs {
                let _ = epoch.memtable.for_each_visible_node_at(u64::MAX, &mut |node| {
                    if !deleted.contains(&node.id)
                        && seen.insert(node.id)
                        && Self::matches_prune_criteria(node, age_cutoff, policy.max_weight)
                    {
                        targets.push(node.id);
                    }
                    ControlFlow::Continue(())
                });
            }

            // Segment nodes (newest segments first, skip already-seen)
            for seg in &self.segments {
                for node in seg.all_nodes()? {
                    if !deleted.contains(&node.id) && seen.insert(node.id)
                        && Self::matches_prune_criteria(&node, age_cutoff, policy.max_weight) {
                            targets.push(node.id);
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
    pub fn set_prune_policy(
        &mut self,
        name: &str,
        policy: PrunePolicy,
    ) -> Result<PublishImpact, EngineError> {
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

        if self
            .manifest
            .prune_policies
            .get(name)
            .is_some_and(|existing| {
                existing.max_age_ms == policy.max_age_ms
                    && existing.max_weight == policy.max_weight
                    && existing.type_id == policy.type_id
            })
        {
            return Ok(PublishImpact::NoPublish);
        }

        let name = name.to_string();
        self.with_runtime_manifest_write(|manifest| {
            manifest.prune_policies.insert(name, policy);
            Ok(())
        })?;
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
    pub fn list_prune_policies(&self) -> Vec<(String, PrunePolicy)> {
        self.manifest
            .prune_policies
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn node_property_index_info(entry: &SecondaryIndexManifestEntry) -> NodePropertyIndexInfo {
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { type_id, prop_key } => NodePropertyIndexInfo {
                index_id: entry.index_id,
                type_id: *type_id,
                prop_key: prop_key.clone(),
                kind: entry.kind.clone(),
                state: entry.state,
                last_error: entry.last_error.clone(),
            },
        }
    }

    pub fn ensure_node_property_index(
        &mut self,
        type_id: u32,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(NodePropertyIndexInfo, PublishImpact), EngineError> {
        enum EnsureOutcome {
            Existing,
            New,
            Retry,
        }

        let prop_key = prop_key.to_string();
        let (entry, outcome) = self.with_runtime_manifest_write(|manifest| {
            if matches!(&kind, SecondaryIndexKind::Range { .. }) {
                for existing in &manifest.secondary_indexes {
                    let SecondaryIndexTarget::NodeProperty {
                        type_id: existing_type_id,
                        prop_key: existing_prop_key,
                    } = &existing.target;
                    if *existing_type_id == type_id
                        && existing_prop_key == &prop_key
                        && matches!(existing.kind, SecondaryIndexKind::Range { .. })
                        && existing.kind != kind
                    {
                        return Err(EngineError::InvalidOperation(format!(
                            "property index ({}, {}) already has a range declaration with a different domain",
                            type_id, prop_key
                        )));
                    }
                }
            }

            if let Some(existing) = manifest.secondary_indexes.iter_mut().find(|entry| {
                entry.target
                    == SecondaryIndexTarget::NodeProperty {
                        type_id,
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
                    type_id,
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

        Ok((Self::node_property_index_info(&entry), publish_impact))
    }

    pub fn drop_node_property_index(
        &mut self,
        type_id: u32,
        prop_key: &str,
        kind: SecondaryIndexKind,
    ) -> Result<(bool, PublishImpact), EngineError> {
        let prop_key = prop_key.to_string();
        let removed = self.with_runtime_manifest_write(|manifest| {
            let idx = manifest.secondary_indexes.iter().position(|entry| {
                entry.target
                    == SecondaryIndexTarget::NodeProperty {
                        type_id,
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
        self.enqueue_secondary_index_job(SecondaryIndexJob::DropCleanup {
            index_id: entry.index_id,
        });
        Ok((true, PublishImpact::RebuildSources))
    }

    pub fn list_node_property_indexes(&self) -> Vec<NodePropertyIndexInfo> {
        let mut indexes: Vec<NodePropertyIndexInfo> = self
            .secondary_index_entries_snapshot()
            .iter()
            .map(Self::node_property_index_info)
            .collect();
        indexes.sort_unstable_by(|left, right| {
            left.type_id
                .cmp(&right.type_id)
                .then_with(|| left.prop_key.cmp(&right.prop_key))
                .then_with(|| format!("{:?}", left.kind).cmp(&format!("{:?}", right.kind)))
                .then_with(|| left.index_id.cmp(&right.index_id))
        });
        indexes
    }

    // --- Segment-aware dedup lookups (for upsert) ---

    /// Look up a node by (type_id, key) across memtable + segments.
    /// Used by upsert_node for dedup. Uses raw (unfiltered) lookup to prevent
    /// policy-excluded nodes from being treated as "not found" (which would
    /// allocate a duplicate ID, causing silent data corruption).
    fn find_existing_node(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self
            .get_node_by_key_raw(type_id, key)?
            .map(|n| (n.id, n.created_at)))
    }

    /// Look up an edge by (from, to, type_id) across memtable + segments.
    /// Used by upsert_edge for uniqueness enforcement. Delegates to public get_edge_by_triple.
    fn find_existing_edge(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<(u64, i64)>, EngineError> {
        Ok(self
            .get_edge_by_triple(from, to, type_id)?
            .map(|e| (e.id, e.created_at)))
    }
}
