// Write operations: upsert, delete, batch, patch, prune.
// This file is include!()'d into mod.rs — all items share the engine module scope.

impl DatabaseEngine {
    // --- High-level graph APIs ---

    /// Upsert a node. If a node with the same (type_id, key) exists, updates it.
    /// Otherwise allocates a new ID. Returns the node ID.
    pub fn upsert_node(
        &mut self,
        type_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
        weight: f32,
    ) -> Result<u64, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        let (id, created_at) = match self.find_existing_node(type_id, key)? {
            Some((id, created_at)) => (id, created_at),
            None => {
                let id = self.next_node_id;
                self.next_node_id += 1;
                (id, now)
            }
        };

        let node = NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props,
            created_at,
            updated_at: now,
            weight,
        };

        let op = WalOp::UpsertNode(node);
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(id)
    }

    /// Upsert an edge. If edge_uniqueness is enabled and an edge with the same
    /// (from, to, type_id) exists, updates it. Otherwise allocates a new ID.
    /// Returns the edge ID.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_edge(
        &mut self,
        from: u64,
        to: u64,
        type_id: u32,
        props: BTreeMap<String, PropValue>,
        weight: f32,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> Result<u64, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        let (id, created_at) = if self.edge_uniqueness {
            match self.find_existing_edge(from, to, type_id)? {
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
            type_id,
            props,
            created_at,
            updated_at: now,
            weight,
            valid_from: valid_from.unwrap_or(created_at),
            valid_to: valid_to.unwrap_or(i64::MAX),
        };

        let op = WalOp::UpsertEdge(edge);
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(id)
    }

    /// Batch upsert nodes with a single fsync. Returns IDs in input order.
    /// Handles dedup within the batch and against existing memtable state.
    pub fn batch_upsert_nodes(&mut self, inputs: &[NodeInput]) -> Result<Vec<u64>, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
        // Track allocations within this batch for dedup
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in inputs {
            let key_tuple = (input.type_id, input.key.clone());

            let (id, created_at) = if let Some(&(id, created_at)) = batch_keys.get(&key_tuple) {
                // Already allocated in this batch, update
                (id, created_at)
            } else if let Some((id, created_at)) =
                self.find_existing_node(input.type_id, &input.key)?
            {
                (id, created_at)
            } else {
                let id = self.next_node_id;
                self.next_node_id += 1;
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
            }));
            ids.push(id);
        }

        self.append_and_apply(&ops)?;

        self.maybe_auto_flush()?;
        Ok(ids)
    }

    /// Batch upsert edges with a single fsync. Returns IDs in input order.
    pub fn batch_upsert_edges(&mut self, inputs: &[EdgeInput]) -> Result<Vec<u64>, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops = Vec::with_capacity(inputs.len());
        let mut ids = Vec::with_capacity(inputs.len());
        // Track allocations within this batch for uniqueness
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
                type_id: input.type_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
            }));
            ids.push(id);
        }

        self.append_and_apply(&ops)?;

        self.maybe_auto_flush()?;
        Ok(ids)
    }

    /// Delete a node by ID. Cascade-deletes all incident edges (memtable + segments),
    /// then writes the node tombstone. Single fsync at the end.
    pub fn delete_node(&mut self, id: u64) -> Result<(), EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();

        // Collect ALL incident edge IDs across memtable + segments.
        // Uses neighbors_raw to see edges to policy-excluded nodes too.
        let incident = self.neighbors_raw(id, Direction::Both, None, 0, None, None, None)?;

        // Build all ops: edge deletes first, then node delete
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

        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;
        Ok(())
    }

    /// Delete an edge by ID. Writes a tombstone to WAL. Idempotent: deleting
    /// a nonexistent or already-deleted edge writes a tombstone but is not an error.
    pub fn delete_edge(&mut self, id: u64) -> Result<(), EngineError> {
        self.maybe_backpressure_flush()?;
        let op = WalOp::DeleteEdge {
            id,
            deleted_at: now_millis(),
        };
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(())
    }

    /// Invalidate an edge by closing its validity window. Sets valid_to on the edge.
    /// The edge remains in the database (not tombstoned) but is excluded from
    /// current-time neighbor queries. Returns the updated EdgeRecord, or None
    /// if the edge doesn't exist.
    pub fn invalidate_edge(
        &mut self,
        id: u64,
        valid_to: i64,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.maybe_backpressure_flush()?;
        // Look up the current edge record
        let edge = match self.get_edge(id)? {
            Some(e) => e,
            None => return Ok(None),
        };

        let updated = EdgeRecord {
            updated_at: now_millis(),
            valid_to,
            ..edge
        };

        let op = WalOp::UpsertEdge(updated.clone());
        self.append_and_apply_one(&op)?;
        self.maybe_auto_flush()?;
        Ok(Some(updated))
    }

    /// Atomic graph patch: apply a mix of node upserts, edge upserts, edge
    /// invalidations, and deletes in a single WAL batch. Deterministic ordering:
    /// node upserts → edge upserts → edge invalidations → edge deletes → node deletes.
    ///
    /// Node deletes cascade: incident edges are automatically deleted.
    /// Returns allocated IDs for upserted nodes and edges (input order preserved).
    pub fn graph_patch(&mut self, patch: &GraphPatch) -> Result<PatchResult, EngineError> {
        self.maybe_backpressure_flush()?;
        let now = now_millis();
        let mut ops: Vec<WalOp> = Vec::new();

        // --- 1. Node upserts (same dedup as batch_upsert_nodes) ---
        let mut node_ids = Vec::with_capacity(patch.upsert_nodes.len());
        let mut batch_keys: HashMap<(u32, String), (u64, i64)> = HashMap::new();

        for input in &patch.upsert_nodes {
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
            }));
            node_ids.push(id);
        }

        // --- 2. Edge upserts (same dedup as batch_upsert_edges) ---
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
                type_id: input.type_id,
                props: input.props.clone(),
                created_at,
                updated_at: now,
                weight: input.weight,
                valid_from: input.valid_from.unwrap_or(created_at),
                valid_to: input.valid_to.unwrap_or(i64::MAX),
            }));
            edge_ids.push(id);
        }

        // --- 3. Edge invalidations (batch read) ---
        if !patch.invalidate_edges.is_empty() {
            let inv_ids: Vec<u64> = patch.invalidate_edges.iter().map(|&(id, _)| id).collect();
            // get_edges has no policy filtering (edges are unfiltered); safe for write path
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

        // --- 4. Edge deletes ---
        for &eid in &patch.delete_edge_ids {
            ops.push(WalOp::DeleteEdge {
                id: eid,
                deleted_at: now,
            });
        }

        // --- 5. Node deletes (cascade incident edges across all sources) ---
        // Uses neighbors_raw to see edges to policy-excluded nodes too.
        // Pre-compute tombstones once for the entire loop.
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

        // --- Single WAL batch ---
        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;

        Ok(PatchResult { node_ids, edge_ids })
    }

    // --- Retention / Forgetting ---

    /// Prune nodes matching the given policy, cascade-deleting their incident edges.
    /// All matching criteria combine with AND logic. At least one of `max_age_ms` or
    /// `max_weight` must be set to prevent accidental mass deletion. All deletes are
    /// applied in a single WAL batch for atomicity.
    pub fn prune(&mut self, policy: &PrunePolicy) -> Result<PruneResult, EngineError> {
        // Require at least one substantive filter
        if policy.max_age_ms.is_none() && policy.max_weight.is_none() {
            return Ok(PruneResult {
                nodes_pruned: 0,
                edges_pruned: 0,
            });
        }

        // Reject nonsensical negative age threshold
        if let Some(age) = policy.max_age_ms {
            if age <= 0 {
                return Err(EngineError::InvalidOperation(
                    "max_age_ms must be positive".to_string(),
                ));
            }
        }

        self.maybe_backpressure_flush()?;
        let now = now_millis();

        // Collect all node IDs that match the prune policy
        let targets = self.collect_prune_targets(policy, now)?;

        if targets.is_empty() {
            return Ok(PruneResult {
                nodes_pruned: 0,
                edges_pruned: 0,
            });
        }

        // Build WAL ops: cascade-delete incident edges, then delete nodes.
        // Dedup edge deletes in case two pruned nodes share an edge.
        let mut ops = Vec::new();
        let mut edges_seen = HashSet::new();
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

        let nodes_pruned = targets.len() as u64;
        let edges_pruned = edges_seen.len() as u64;

        self.append_and_apply(&ops)?;
        self.maybe_auto_flush()?;

        Ok(PruneResult {
            nodes_pruned,
            edges_pruned,
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
            // Scan all nodes: memtable first, then segments (newest-first), dedup by ID
            let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
            for seg in &self.segments {
                deleted.extend(seg.deleted_node_ids());
            }

            let mut seen = HashSet::new();
            let mut targets = Vec::new();

            // Memtable nodes (freshest)
            for node in self.memtable.nodes().values() {
                if !deleted.contains(&node.id) && seen.insert(node.id)
                    && Self::matches_prune_criteria(node, age_cutoff, policy.max_weight) {
                        targets.push(node.id);
                    }
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
    pub fn set_prune_policy(&mut self, name: &str, policy: PrunePolicy) -> Result<(), EngineError> {
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
        self.manifest
            .prune_policies
            .insert(name.to_string(), policy);
        write_manifest(&self.db_dir, &self.manifest)?;
        Ok(())
    }

    /// Remove a named prune policy. Returns true if it existed.
    pub fn remove_prune_policy(&mut self, name: &str) -> Result<bool, EngineError> {
        let removed = self.manifest.prune_policies.remove(name).is_some();
        if removed {
            write_manifest(&self.db_dir, &self.manifest)?;
        }
        Ok(removed)
    }

    /// List all registered prune policies.
    pub fn list_prune_policies(&self) -> Vec<(String, PrunePolicy)> {
        self.manifest
            .prune_policies
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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
