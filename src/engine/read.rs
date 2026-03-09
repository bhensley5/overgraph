// Read operations: get, find, query, pagination, export.
// This file is include!()'d into mod.rs — all items share the engine module scope.

impl DatabaseEngine {
    // --- Read-time policy filtering ---

    /// Check if a single node is excluded by any registered prune policy.
    /// Early-out when no policies are registered.
    fn is_node_excluded_by_policies(&self, node: &NodeRecord) -> bool {
        if self.manifest.prune_policies.is_empty() {
            return false;
        }
        let now = now_millis();
        let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now);
        cutoffs.excludes(node)
    }

    /// Batch-compute the set of node IDs that should be excluded by prune policies.
    /// Uses the batched merge-walk (`get_nodes_raw`) instead of N individual lookups.
    /// Returns an empty set when no policies are registered (zero overhead).
    fn policy_excluded_node_ids(&self, node_ids: &[u64]) -> Result<HashSet<u64>, EngineError> {
        if self.manifest.prune_policies.is_empty() || node_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now_millis());
        let records = self.get_nodes_raw(node_ids)?;
        let mut excluded = HashSet::new();
        for (i, slot) in records.iter().enumerate() {
            if let Some(ref node) = slot {
                if cutoffs.excludes(node) {
                    excluded.insert(node_ids[i]);
                }
            }
        }
        Ok(excluded)
    }

    /// Precompute prune cutoffs for a single read query. Returns `None` when no
    /// read-time prune policies are registered.
    fn query_policy_cutoffs(&self) -> Option<PrecomputedPruneCutoffs> {
        if self.manifest.prune_policies.is_empty() {
            None
        } else {
            Some(PrecomputedPruneCutoffs::from_manifest(
                &self.manifest,
                now_millis(),
            ))
        }
    }

    /// Return the subset of `node_ids` visible to public read APIs: existing,
    /// not tombstoned, and not excluded by any read-time prune policy.
    fn visible_node_ids(
        &self,
        node_ids: &[u64],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<HashSet<u64>, EngineError> {
        if node_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let records = self.get_nodes_raw(node_ids)?;
        let mut visible = HashSet::with_capacity(node_ids.len());
        for (&node_id, slot) in node_ids.iter().zip(records.iter()) {
            if let Some(node) = slot {
                if policy_cutoffs.is_none_or(|cutoffs| !cutoffs.excludes(node)) {
                    visible.insert(node_id);
                }
            }
        }
        Ok(visible)
    }

    /// Validate that both path endpoints are visible to public read APIs.
    fn path_endpoints_visible(
        &self,
        from: u64,
        to: u64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<bool, EngineError> {
        let visible = self.visible_node_ids(&[from, to], policy_cutoffs)?;
        Ok(visible.contains(&from) && visible.contains(&to))
    }

    // --- Read APIs (multi-source: memtable → segments newest-first) ---

    /// Get a node by ID (raw, unfiltered). Checks memtable first, then segments
    /// newest-to-oldest. Returns None if not found or deleted (tombstoned).
    /// Used internally by upsert dedup, cascade deletes, and prune.
    fn get_node_raw(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        // Memtable is freshest
        if let Some(node) = self.memtable.get_node(id) {
            return Ok(Some(node.clone()));
        }
        // If memtable explicitly tombstoned this ID, it's deleted
        if self.memtable.deleted_nodes().contains_key(&id) {
            return Ok(None);
        }
        // Check segments newest-first
        for seg in &self.segments {
            if seg.is_node_deleted(id) {
                return Ok(None); // Tombstone in newer segment hides older data
            }
            if let Some(node) = seg.get_node(id)? {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    /// Get a node by ID. Checks memtable first, then segments newest-to-oldest.
    /// Returns None if not found, deleted, or excluded by a registered prune policy.
    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_raw(id)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    /// Get an edge by ID. Checks memtable first, then segments newest-to-oldest.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self.memtable.get_edge(id) {
            return Ok(Some(edge.clone()));
        }
        if self.memtable.deleted_edges().contains_key(&id) {
            return Ok(None);
        }
        for seg in &self.segments {
            if seg.is_edge_deleted(id) {
                return Ok(None);
            }
            if let Some(edge) = seg.get_edge(id)? {
                return Ok(Some(edge));
            }
        }
        Ok(None)
    }

    /// Get a node by (type_id, key) across memtable + segments (raw, unfiltered).
    /// Used internally by upsert dedup. Not subject to prune policy filtering.
    fn get_node_by_key_raw(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        // Check memtable first (freshest)
        if let Some(node) = self.memtable.node_by_key(type_id, key) {
            return Ok(Some(node.clone()));
        }
        // Check segments newest-first, respecting tombstones
        for (i, seg) in self.segments.iter().enumerate() {
            if let Some(node) = seg.node_by_key(type_id, key)? {
                if self.memtable.deleted_nodes().contains_key(&node.id) {
                    return Ok(None);
                }
                for newer_seg in &self.segments[..i] {
                    if newer_seg.is_node_deleted(node.id) {
                        return Ok(None);
                    }
                }
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    /// Get a node by (type_id, key) across memtable + segments.
    /// Returns None if not found, deleted, or excluded by a registered prune policy.
    pub fn get_node_by_key(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        let node = match self.get_node_by_key_raw(type_id, key)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if self.is_node_excluded_by_policies(&node) {
            return Ok(None);
        }
        Ok(Some(node))
    }

    /// Get an edge by (from, to, type_id) across memtable + segments.
    /// Returns the most recently written edge matching the triple.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self.memtable.edge_by_triple(from, to, type_id) {
            return Ok(Some(edge.clone()));
        }
        for (i, seg) in self.segments.iter().enumerate() {
            if let Some(edge) = seg.edge_by_triple(from, to, type_id)? {
                if self.memtable.deleted_edges().contains_key(&edge.id) {
                    return Ok(None);
                }
                for newer_seg in &self.segments[..i] {
                    if newer_seg.is_edge_deleted(edge.id) {
                        return Ok(None);
                    }
                }
                return Ok(Some(edge));
            }
        }
        Ok(None)
    }

    /// Batch node lookup (raw, unfiltered). Core implementation shared by
    /// `get_nodes` (public, filtered) and `policy_excluded_node_ids`.
    fn get_nodes_raw(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let n = ids.len();
        let mut results: Vec<Option<NodeRecord>> = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        // Build (original_index, id) pairs for tracking
        let mut remaining: Vec<(usize, u64)> = Vec::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            // Memtable tombstone, definitely deleted, skip
            if self.memtable.deleted_nodes().contains_key(&id) {
                continue;
            }
            // Memtable hit, freshest source
            if let Some(node) = self.memtable.get_node(id) {
                results[i] = Some(node.clone());
                continue;
            }
            remaining.push((i, id));
        }

        // Sort once before segment scan. retain() preserves order so
        // remaining stays sorted across iterations.
        remaining.sort_unstable_by_key(|&(_, id)| id);

        // Scan segments newest-first with sorted merge-walk
        for seg in &self.segments {
            if remaining.is_empty() {
                break;
            }

            // Filter out IDs tombstoned in this segment before the batch scan
            // (tombstone in a newer segment hides data in older segments)
            remaining.retain(|&(_, id)| !seg.is_node_deleted(id));

            seg.get_nodes_batch(&remaining, &mut results)?;

            // Remove IDs that were found in this segment
            remaining.retain(|&(i, _)| results[i].is_none());
        }

        Ok(results)
    }

    /// Get multiple nodes by ID in a single call.
    /// Returns a `Vec<Option<NodeRecord>>`, one per input ID (order preserved).
    /// Missing, deleted, or policy-excluded nodes are None.
    ///
    /// Uses a batched approach: resolves memtable hits first (O(1) each),
    /// then does a single sorted merge-walk per segment instead of N binary searches.
    pub fn get_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_raw(ids)?;

        // Policy filtering: exclude nodes matching any registered prune policy.
        // Early-out when no policies are registered (zero overhead).
        if !self.manifest.prune_policies.is_empty() {
            let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now_millis());
            for slot in results.iter_mut() {
                if let Some(ref node) = slot {
                    if cutoffs.excludes(node) {
                        *slot = None;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get multiple edges by ID in a single call.
    /// Returns a `Vec<Option<EdgeRecord>>`, one per input ID (order preserved).
    /// Missing or deleted edges are None.
    ///
    /// Uses a batched approach: resolves memtable hits first (O(1) each),
    /// then does a single sorted merge-walk per segment instead of N binary searches.
    pub fn get_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let n = ids.len();
        let mut results: Vec<Option<EdgeRecord>> = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = Vec::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            if self.memtable.deleted_edges().contains_key(&id) {
                continue;
            }
            if let Some(edge) = self.memtable.get_edge(id) {
                results[i] = Some(edge.clone());
                continue;
            }
            remaining.push((i, id));
        }

        // Sort once. retain() preserves order across iterations.
        remaining.sort_unstable_by_key(|&(_, id)| id);

        for seg in &self.segments {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(_, id)| !seg.is_edge_deleted(id));
            seg.get_edges_batch(&remaining, &mut results)?;
            remaining.retain(|&(i, _)| results[i].is_none());
        }

        Ok(results)
    }


    // --- Secondary index queries ---

    /// Return all live node IDs with the given type_id (raw, unfiltered).
    /// Used internally by collect_prune_targets.
    fn nodes_by_type_raw(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for id in self.memtable.nodes_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for seg in &self.segments {
            for id in seg.nodes_by_type(type_id)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        Ok(results)
    }

    /// Return all live node IDs with the given type_id, merged across
    /// memtable and all segments. Excludes tombstoned and policy-excluded nodes.
    pub fn nodes_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut results = self.nodes_by_type_raw(type_id)?;

        // Policy filtering: batch-fetch nodes and exclude matches.
        let excluded = self.policy_excluded_node_ids(&results)?;
        if !excluded.is_empty() {
            results.retain(|id| !excluded.contains(id));
        }

        Ok(results)
    }

    /// Return all live edge IDs with the given type_id, merged across
    /// memtable and all segments. Excludes tombstoned edges.
    pub fn edges_by_type(&self, type_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut deleted: HashSet<u64> = self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_edge_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for id in self.memtable.edges_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for seg in &self.segments {
            for id in seg.edges_by_type(type_id)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        Ok(results)
    }

    /// Return all live node records with the given type_id, hydrated from
    /// memtable and segments. Excludes tombstoned and policy-excluded nodes.
    ///
    /// Uses `nodes_by_type()` for the ID list (already policy-filtered), then
    /// `get_nodes_raw()` for batch hydration (one merge-walk per segment,
    /// not N individual lookups). Single policy pass, no redundant filtering.
    pub fn get_nodes_by_type(&self, type_id: u32) -> Result<Vec<NodeRecord>, EngineError> {
        let ids = self.nodes_by_type(type_id)?;
        let results = self.get_nodes_raw(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Return all live edge records with the given type_id, hydrated from
    /// memtable and segments. Excludes tombstoned edges.
    ///
    /// Uses `edges_by_type()` for the ID list, then `get_edges()` for batch
    /// hydration (one merge-walk per segment, not N individual lookups).
    pub fn get_edges_by_type(&self, type_id: u32) -> Result<Vec<EdgeRecord>, EngineError> {
        let ids = self.edges_by_type(type_id)?;
        let results = self.get_edges(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Return the count of live nodes with the given type_id without hydrating
    /// records. Excludes tombstoned and policy-excluded nodes.
    pub fn count_nodes_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        Ok(self.nodes_by_type(type_id)?.len() as u64)
    }

    /// Return the count of live edges with the given type_id without hydrating
    /// records. Excludes tombstoned edges (edges are not subject to prune policies).
    pub fn count_edges_by_type(&self, type_id: u32) -> Result<u64, EngineError> {
        Ok(self.edges_by_type(type_id)?.len() as u64)
    }

    // --- Paginated type-index queries ---

    /// Paginated version of `nodes_by_type`. Returns a page of node IDs sorted
    /// by ID, with cursor-based pagination. Pass `PageRequest::default()` to get
    /// all results (equivalent to `nodes_by_type`).
    ///
    /// Uses K-way merge across already-sorted sources with early termination:
    /// O(cursor_position + limit) instead of O(N log N) when no prune policies
    /// are active. With policies, still saves the sort via merge, then applies
    /// policy filtering and cursor on the sorted result.
    pub fn nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.nodes_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.nodes_by_type(type_id)?);
        }

        if self.manifest.prune_policies.is_empty() {
            // Fast path: merge with early termination, no policy filtering needed
            Ok(merge_type_ids_paged(
                memtable_ids,
                segment_ids,
                &deleted,
                page,
            ))
        } else {
            let limit = page.limit.unwrap_or(0);
            if limit == 0 {
                let all_page = PageRequest {
                    limit: None,
                    after: page.after,
                };
                let all = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
                let excluded = self.policy_excluded_node_ids(&all.items)?;
                let mut items = all.items;
                if !excluded.is_empty() {
                    items.retain(|id| !excluded.contains(id));
                }
                Ok(PageResult {
                    items,
                    next_cursor: None,
                })
            } else {
                let chunk_limit = limit.saturating_mul(4).max(limit);
                let mut collected = Vec::with_capacity(limit);
                let mut cursor = page.after;

                loop {
                    let chunk_page = PageRequest {
                        limit: Some(chunk_limit),
                        after: cursor,
                    };
                    let chunk =
                        merge_type_ids_paged(memtable_ids.clone(), segment_ids.clone(), &deleted, &chunk_page);
                    if chunk.items.is_empty() {
                        return Ok(PageResult {
                            items: collected,
                            next_cursor: None,
                        });
                    }

                    let excluded = self.policy_excluded_node_ids(&chunk.items)?;
                    for id in chunk.items {
                        if !excluded.contains(&id) {
                            collected.push(id);
                            if collected.len() >= limit {
                                return Ok(PageResult {
                                    items: collected,
                                    next_cursor: Some(id),
                                });
                            }
                        }
                        cursor = Some(id);
                    }

                    if chunk.next_cursor.is_none() {
                        return Ok(PageResult {
                            items: collected,
                            next_cursor: None,
                        });
                    }
                }
            }
        }
    }

    /// Paginated version of `edges_by_type`. Returns a page of edge IDs sorted
    /// by ID, with cursor-based pagination. Uses K-way merge with early
    /// termination (edges are not subject to prune policies).
    pub fn edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_edge_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.edges_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.edges_by_type(type_id)?);
        }

        Ok(merge_type_ids_paged(
            memtable_ids,
            segment_ids,
            &deleted,
            page,
        ))
    }

    /// Paginated version of `get_nodes_by_type`. Returns a page of hydrated node
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_nodes_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<NodeRecord>, EngineError> {
        let id_page = self.nodes_by_type_paged(type_id, page)?;
        let hydrated = self.get_nodes_raw(&id_page.items)?;
        let items: Vec<NodeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    /// Paginated version of `get_edges_by_type`. Returns a page of hydrated edge
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_edges_by_type_paged(
        &self,
        type_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<EdgeRecord>, EngineError> {
        let id_page = self.edges_by_type_paged(type_id, page)?;
        let hydrated = self.get_edges(&id_page.items)?;
        let items: Vec<EdgeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    /// Find node IDs matching (type_id, prop_key == prop_value).
    /// Merges candidates from memtable + segments, deduplicates, and post-filters
    /// to verify actual property equality (handles hash collisions).
    /// Excludes nodes matching any registered prune policy.
    pub fn find_nodes(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<Vec<u64>, EngineError> {
        // Collect all deleted node IDs across sources
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        // Memtable first (already post-filtered)
        for id in self.memtable.find_nodes(type_id, prop_key, prop_value) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        // Segments newest-first: get candidates by hash, then verify against
        // latest-wins version (not segment-local) to avoid stale matches when
        // a newer source has updated the property to a non-matching value.
        let key_hash = hash_prop_key(prop_key);
        let value_hash = hash_prop_value(prop_value);

        for seg in &self.segments {
            for id in seg.find_nodes_by_prop_hash(type_id, key_hash, value_hash)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    if let Some(node) = self.get_node_raw(id)? {
                        if node
                            .props
                            .get(prop_key)
                            .map(|v| v == prop_value)
                            .unwrap_or(false)
                        {
                            results.push(id);
                        }
                    }
                }
            }
        }

        // Policy filtering: batch-fetch nodes and exclude matches.
        let excluded = self.policy_excluded_node_ids(&results)?;
        if !excluded.is_empty() {
            results.retain(|id| !excluded.contains(id));
        }

        Ok(results)
    }

    /// Paginated version of `find_nodes`. Returns a page of node IDs matching
    /// (type_id, prop_key == prop_value), sorted by ID with cursor-based
    /// pagination.
    ///
    /// Uses K-way merge across sorted property hash buckets (segment IDs are
    /// written sorted at flush/compaction time). Post-filters for hash
    /// collisions, then applies cursor + limit on the verified results.
    /// Policy filtering applied when policies are active.
    pub fn find_nodes_paged(
        &self,
        type_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect candidate IDs from sources (sorted within each source)
        let memtable_ids = self.memtable.find_nodes(type_id, prop_key, prop_value);
        let key_hash = hash_prop_key(prop_key);
        let value_hash = hash_prop_value(prop_value);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.find_nodes_by_prop_hash(type_id, key_hash, value_hash)?);
        }

        let memtable_verified: HashSet<u64> = self
            .memtable
            .find_nodes(type_id, prop_key, prop_value)
            .into_iter()
            .collect();
        let limit = page.limit.unwrap_or(0);
        if limit == 0 {
            let all_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let merged = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
            let mut visible: HashSet<u64> = HashSet::with_capacity(merged.items.len());
            let segment_candidates: Vec<u64> = merged
                .items
                .iter()
                .copied()
                .filter(|id| !memtable_verified.contains(id))
                .collect();
            let batch_results = self.get_nodes_raw(&segment_candidates)?;
            for id in merged.items.iter().copied() {
                if memtable_verified.contains(&id) {
                    visible.insert(id);
                }
            }
            for (id, node) in segment_candidates.into_iter().zip(batch_results.into_iter()) {
                let keep = node
                    .as_ref()
                    .and_then(|n| n.props.get(prop_key))
                    .map(|v| v == prop_value)
                    .unwrap_or(false);
                if keep {
                    visible.insert(id);
                }
            }

            let excluded = self.policy_excluded_node_ids(&merged.items)?;
            let items = merged
                .items
                .into_iter()
                .filter(|id| visible.contains(id) && !excluded.contains(id))
                .collect();
            Ok(PageResult {
                items,
                next_cursor: None,
            })
        } else {
            let chunk_limit = limit.saturating_mul(4).max(limit);
            let mut collected = Vec::with_capacity(limit);
            let mut cursor = page.after;

            loop {
                let chunk_page = PageRequest {
                    limit: Some(chunk_limit),
                    after: cursor,
                };
                let merged = merge_type_ids_paged(
                    memtable_ids.clone(),
                    segment_ids.clone(),
                    &deleted,
                    &chunk_page,
                );
                if merged.items.is_empty() {
                    return Ok(PageResult {
                        items: collected,
                        next_cursor: None,
                    });
                }

                let segment_candidates: Vec<u64> = merged
                    .items
                    .iter()
                    .copied()
                    .filter(|id| !memtable_verified.contains(id))
                    .collect();
                let batch_results = self.get_nodes_raw(&segment_candidates)?;
                let mut visible: HashSet<u64> = HashSet::with_capacity(merged.items.len());
                for id in merged.items.iter().copied() {
                    if memtable_verified.contains(&id) {
                        visible.insert(id);
                    }
                }
                for (id, node) in segment_candidates.into_iter().zip(batch_results.into_iter()) {
                    let keep = node
                        .as_ref()
                        .and_then(|n| n.props.get(prop_key))
                        .map(|v| v == prop_value)
                        .unwrap_or(false);
                    if keep {
                        visible.insert(id);
                    }
                }

                let excluded = self.policy_excluded_node_ids(&merged.items)?;
                for id in merged.items {
                    if visible.contains(&id) && !excluded.contains(&id) {
                        collected.push(id);
                        if collected.len() >= limit {
                            return Ok(PageResult {
                                items: collected,
                                next_cursor: Some(id),
                            });
                        }
                    }
                    cursor = Some(id);
                }

                if merged.next_cursor.is_none() {
                    return Ok(PageResult {
                        items: collected,
                        next_cursor: None,
                    });
                }
            }
        }
    }

    // --- Timestamp range queries ---

    /// Find node IDs of a given type updated within a time range [from_ms, to_ms] (inclusive).
    /// Merges across memtable + segments with deduplication.
    /// Excludes tombstoned and policy-pruned nodes.
    pub fn find_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let page = PageRequest {
            limit: None,
            after: None,
        };
        Ok(self
            .find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page)?
            .items)
    }

    /// Paginated version of `find_nodes_by_time_range`. Returns a page of node IDs
    /// sorted by ID with cursor-based pagination.
    ///
    /// Uses K-way merge across sorted sources (binary search for range in each
    /// segment, sort results by node_id). O(log N) seek per source + O(results) scan.
    pub fn find_nodes_by_time_range_paged(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        // Build global deleted set (cross-source tombstones)
        let mut deleted: HashSet<u64> = self.memtable.deleted_nodes().keys().copied().collect();
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources: memtable + segments
        let memtable_ids = self.memtable.nodes_by_time_range(type_id, from_ms, to_ms);
        let mut segment_ids: Vec<Vec<u64>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_ids.push(seg.nodes_by_time_range(type_id, from_ms, to_ms)?);
        }

        let limit = page.limit.unwrap_or(0);
        if limit == 0 {
            let all_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let all = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
            let nodes = self.get_nodes_raw(&all.items)?;
            let mut items: Vec<u64> = Vec::with_capacity(all.items.len());
            for (id, node) in all.items.iter().zip(nodes.iter()) {
                if let Some(n) = node {
                    if n.updated_at >= from_ms && n.updated_at <= to_ms {
                        items.push(*id);
                    }
                }
            }

            if !self.manifest.prune_policies.is_empty() {
                let excluded = self.policy_excluded_node_ids(&items)?;
                if !excluded.is_empty() {
                    items.retain(|id| !excluded.contains(id));
                }
            }

            Ok(PageResult {
                items,
                next_cursor: None,
            })
        } else {
            let chunk_limit = limit.saturating_mul(4).max(limit);
            let mut collected = Vec::with_capacity(limit);
            let mut cursor = page.after;

            loop {
                let chunk_page = PageRequest {
                    limit: Some(chunk_limit),
                    after: cursor,
                };
                let chunk = merge_type_ids_paged(
                    memtable_ids.clone(),
                    segment_ids.clone(),
                    &deleted,
                    &chunk_page,
                );
                if chunk.items.is_empty() {
                    return Ok(PageResult {
                        items: collected,
                        next_cursor: None,
                    });
                }

                let nodes = self.get_nodes_raw(&chunk.items)?;
                let mut visible: HashSet<u64> = HashSet::with_capacity(chunk.items.len());
                for (id, node) in chunk.items.iter().zip(nodes.iter()) {
                    if let Some(n) = node {
                        if n.updated_at >= from_ms && n.updated_at <= to_ms {
                            visible.insert(*id);
                        }
                    }
                }

                let excluded = if self.manifest.prune_policies.is_empty() {
                    HashSet::new()
                } else {
                    self.policy_excluded_node_ids(&chunk.items)?
                };

                for id in chunk.items {
                    if visible.contains(&id) && !excluded.contains(&id) {
                        collected.push(id);
                        if collected.len() >= limit {
                            return Ok(PageResult {
                                items: collected,
                                next_cursor: Some(id),
                            });
                        }
                    }
                    cursor = Some(id);
                }

                if chunk.next_cursor.is_none() {
                    return Ok(PageResult {
                        items: collected,
                        next_cursor: None,
                    });
                }
            }
        }
    }

    // --- Personalized PageRank ---

    /// Compute Personalized PageRank starting from seed nodes.
    ///
    /// Uses BFS discovery + dense Vec power iteration with weighted transitions.
    /// Phase 1: DFS from seeds discovers all reachable nodes and caches neighbors.
    /// Phase 2: Builds dense node_id→index mapping with precomputed normalized weights.
    /// Phase 3: Power iteration over contiguous `Vec<f64>` rank vectors.
    ///
    /// Edge weights determine transition probabilities (proportional to weight).
    /// Dangling nodes (no outgoing edges) teleport back to the seed set.
    ///
    /// Returns scored nodes sorted by score descending, with optional top-k cutoff.
    pub fn personalized_pagerank(
        &self,
        seed_node_ids: &[u64],
        options: &PprOptions,
    ) -> Result<PprResult, EngineError> {
        if seed_node_ids.is_empty() {
            return Ok(PprResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            });
        }

        let damping = options.damping_factor;
        if damping <= 0.0 || damping >= 1.0 {
            return Err(EngineError::InvalidOperation(
                "damping_factor must be in (0.0, 1.0)".into(),
            ));
        }
        let edge_filter = options.edge_type_filter.as_deref();

        // Deduplicate seeds and filter to live nodes only.
        // Without this, deleted/non-existent seeds become dangling nodes
        // that absorb teleport mass and appear in results with score > 0.
        let seeds: Vec<u64> = {
            let mut s: Vec<u64> = seed_node_ids.to_vec();
            s.sort_unstable();
            s.dedup();
            let live = self.get_nodes_raw(&s)?;
            s.into_iter()
                .zip(live)
                .filter_map(|(id, node)| node.map(|_| id))
                .collect()
        };
        if seeds.is_empty() {
            return Ok(PprResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            });
        }
        let num_seeds = seeds.len() as f64;
        let teleport = (1.0 - damping) / num_seeds;

        // --- Phase 1: BFS discovery to find all reachable nodes ---
        // Wave-based BFS with batch adjacency: one cursor walk per segment
        // per wave instead of O(N log K) binary searches per node.
        let mut discovered: HashSet<u64> = seeds.iter().copied().collect();
        let mut wave: Vec<u64> = seeds.clone();
        let mut neighbor_cache: HashMap<u64, Vec<(u64, f32)>> =
            HashMap::with_capacity(seeds.len() * 16);

        while !wave.is_empty() {
            let all_neighbors =
                self.neighbors_batch(&wave, Direction::Outgoing, edge_filter, None, None)?;

            let mut next_wave: Vec<u64> = Vec::new();
            for &node_id in &wave {
                let entries: Vec<(u64, f32)> = all_neighbors
                    .get(&node_id)
                    .map(|n| n.iter().map(|e| (e.node_id, e.weight)).collect())
                    .unwrap_or_default();
                for &(neighbor_id, _) in &entries {
                    if discovered.insert(neighbor_id) {
                        next_wave.push(neighbor_id);
                    }
                }
                neighbor_cache.insert(node_id, entries);
            }
            wave = next_wave;
        }

        // --- Phase 2: Build dense mapping ---
        let idx_to_id: Vec<u64> = discovered.into_iter().collect();
        let n = idx_to_id.len();
        let id_to_idx: HashMap<u64, usize> = idx_to_id
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        // Pre-compute normalized transition weights and dangling flags
        // dense_neighbors[i] = [(dense_idx_j, normalized_weight_j), ...]
        let mut dense_neighbors: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
        let mut is_dangling = vec![true; n];

        for (&node_id, neighbors) in &neighbor_cache {
            let idx = id_to_idx[&node_id];
            if !neighbors.is_empty() {
                let total_weight: f64 = neighbors.iter().map(|&(_, w)| w as f64).sum();
                if total_weight > 0.0 {
                    is_dangling[idx] = false;
                    dense_neighbors[idx] = neighbors
                        .iter()
                        .map(|&(nid, w)| (id_to_idx[&nid], (w as f64) / total_weight))
                        .collect();
                }
            }
        }

        // Seed dense indices
        let seed_indices: Vec<usize> = seeds.iter().map(|id| id_to_idx[id]).collect();

        // --- Phase 3: Dense power iteration ---
        let mut rank = vec![0.0_f64; n];
        for &si in &seed_indices {
            rank[si] = 1.0 / num_seeds;
        }

        let mut iterations = 0u32;
        let mut converged = false;

        for _ in 0..options.max_iterations {
            iterations += 1;
            let mut new_rank = vec![0.0_f64; n];
            let mut dangling_sum = 0.0_f64;

            for i in 0..n {
                let r = rank[i];
                if r == 0.0 {
                    continue;
                }

                if is_dangling[i] {
                    dangling_sum += r;
                } else {
                    for &(j, norm_weight) in &dense_neighbors[i] {
                        new_rank[j] += damping * r * norm_weight;
                    }
                }
            }

            // Distribute dangling mass + teleport to seeds
            let dangling_per_seed = damping * dangling_sum / num_seeds;
            for &si in &seed_indices {
                new_rank[si] += teleport + dangling_per_seed;
            }

            // Convergence check: L1 norm over dense vectors
            let diff: f64 = rank
                .iter()
                .zip(new_rank.iter())
                .map(|(old, new)| (old - new).abs())
                .sum();

            rank = new_rank;

            if diff < options.epsilon {
                converged = true;
                break;
            }
        }

        // Extract results: map dense indices back to node IDs
        let mut scores: Vec<(u64, f64)> = idx_to_id
            .iter()
            .zip(rank.iter())
            .filter(|(_, &s)| s > 0.0)
            .map(|(&id, &s)| (id, s))
            .collect();
        scores.sort_unstable_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        // Apply max_results cutoff
        if let Some(max) = options.max_results {
            scores.truncate(max);
        }

        Ok(PprResult {
            scores,
            iterations,
            converged,
        })
    }

    // --- Graph export ---

    /// Export the graph's adjacency structure for external community detection.
    ///
    /// Returns all live node IDs and edges (from, to, type_id, weight),
    /// filtered by optional node/edge type filters. Respects tombstones
    /// and prune policies. Each edge is emitted once (outgoing direction only).
    ///
    /// Edges are only included if both endpoints are in the exported node set,
    /// ensuring a consistent subgraph for external tools.
    pub fn export_adjacency(
        &self,
        options: &ExportOptions,
    ) -> Result<AdjacencyExport, EngineError> {
        // Collect all node type IDs from memtable + segments
        let node_types: Vec<u32> = {
            let mut types: HashSet<u32> = self.memtable.type_node_index().keys().copied().collect();
            for seg in &self.segments {
                for tid in seg.node_type_ids()? {
                    types.insert(tid);
                }
            }
            // Apply node type filter
            if let Some(ref filter) = options.node_type_filter {
                let allowed: HashSet<u32> = filter.iter().copied().collect();
                types.retain(|t| allowed.contains(t));
            }
            types.into_iter().collect()
        };

        // Collect all live node IDs (policy-filtered)
        let mut node_set: HashSet<u64> = HashSet::new();
        for &tid in &node_types {
            for id in self.nodes_by_type(tid)? {
                node_set.insert(id);
            }
        }
        let node_ids: Vec<u64> = {
            let mut ids: Vec<u64> = node_set.iter().copied().collect();
            ids.sort_unstable();
            ids
        };

        let edge_filter_slice = options.edge_type_filter.as_deref();

        // Batch-fetch all outgoing neighbors in one cursor walk per segment
        // instead of O(N) individual binary searches.
        let all_neighbors = self.neighbors_batch(
            &node_ids,
            Direction::Outgoing,
            edge_filter_slice,
            None,
            None,
        )?;

        let mut edges: Vec<(u64, u64, u32, f32)> = Vec::new();
        for &from_id in &node_ids {
            if let Some(neighbors) = all_neighbors.get(&from_id) {
                for entry in neighbors {
                    // Only include edges whose target is in the exported node set
                    if !node_set.contains(&entry.node_id) {
                        continue;
                    }
                    let weight = if options.include_weights {
                        entry.weight
                    } else {
                        0.0
                    };
                    edges.push((from_id, entry.node_id, entry.edge_type_id, weight));
                }
            }
        }

        Ok(AdjacencyExport { node_ids, edges })
    }
}
