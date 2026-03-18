// Read operations: get, find, query, pagination, export.
// This file is include!()'d into mod.rs. All items share the engine module scope.

#[derive(Clone, Copy, Debug, PartialEq)]
struct SparseTopKEntry {
    node_id: u64,
    score: f32,
}

impl Eq for SparseTopKEntry {}

impl Ord for SparseTopKEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for SparseTopKEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// --- Hybrid fusion constants and functions ---

/// Smoothing constant for reciprocal rank fusion (Cormack et al., 2009).
const RRF_K: f32 = 60.0;

/// Over-fetch multiplier: each sub-search fetches this many times `k` candidates
/// before fusion trims to the final `k`.
const HYBRID_OVERFETCH_FACTOR: usize = 2;

/// Weighted reciprocal rank fusion.
/// `score(d) = w_dense / (RRF_K + rank_dense) + w_sparse / (RRF_K + rank_sparse)`
fn fuse_weighted_rank(
    dense_hits: &[VectorHit],
    sparse_hits: &[VectorHit],
    dense_weight: f32,
    sparse_weight: f32,
    k: usize,
) -> Vec<VectorHit> {
    let mut scores: NodeIdMap<f32> = NodeIdMap::with_capacity_and_hasher(
        dense_hits.len() + sparse_hits.len(),
        NodeIdBuildHasher::default(),
    );
    for (rank_0, hit) in dense_hits.iter().enumerate() {
        let rank = (rank_0 + 1) as f32;
        *scores.entry(hit.node_id).or_insert(0.0) += dense_weight / (RRF_K + rank);
    }
    for (rank_0, hit) in sparse_hits.iter().enumerate() {
        let rank = (rank_0 + 1) as f32;
        *scores.entry(hit.node_id).or_insert(0.0) += sparse_weight / (RRF_K + rank);
    }
    collect_top_k_from_scores(scores, k)
}

/// Unweighted reciprocal rank fusion (ignores dense_weight/sparse_weight).
fn fuse_reciprocal_rank(
    dense_hits: &[VectorHit],
    sparse_hits: &[VectorHit],
    k: usize,
) -> Vec<VectorHit> {
    fuse_weighted_rank(dense_hits, sparse_hits, 1.0, 1.0, k)
}

/// Min-max normalize scores to [0, 1]. All-equal scores normalize to 1.0.
fn min_max_normalize(hits: &[VectorHit]) -> Vec<f32> {
    if hits.is_empty() {
        return Vec::new();
    }
    let mut min_score = f32::INFINITY;
    let mut max_score = f32::NEG_INFINITY;
    for hit in hits {
        if hit.score < min_score {
            min_score = hit.score;
        }
        if hit.score > max_score {
            max_score = hit.score;
        }
    }
    let range = max_score - min_score;
    if range == 0.0 {
        return vec![1.0; hits.len()];
    }
    hits.iter()
        .map(|hit| (hit.score - min_score) / range)
        .collect()
}

/// Weighted score fusion with min-max normalization per modality.
/// `score(d) = w_dense * norm(dense_score) + w_sparse * norm(sparse_score)`
fn fuse_weighted_score(
    dense_hits: &[VectorHit],
    sparse_hits: &[VectorHit],
    dense_weight: f32,
    sparse_weight: f32,
    k: usize,
) -> Vec<VectorHit> {
    let dense_norm = min_max_normalize(dense_hits);
    let sparse_norm = min_max_normalize(sparse_hits);

    let mut scores: NodeIdMap<f32> = NodeIdMap::with_capacity_and_hasher(
        dense_hits.len() + sparse_hits.len(),
        NodeIdBuildHasher::default(),
    );
    for (hit, &norm) in dense_hits.iter().zip(dense_norm.iter()) {
        *scores.entry(hit.node_id).or_insert(0.0) += dense_weight * norm;
    }
    for (hit, &norm) in sparse_hits.iter().zip(sparse_norm.iter()) {
        *scores.entry(hit.node_id).or_insert(0.0) += sparse_weight * norm;
    }
    collect_top_k_from_scores(scores, k)
}

/// Collect fused scores into a sorted, truncated `Vec<VectorHit>`.
fn collect_top_k_from_scores(scores: NodeIdMap<f32>, k: usize) -> Vec<VectorHit> {
    let mut hits: Vec<VectorHit> = scores
        .into_iter()
        .map(|(node_id, score)| VectorHit { node_id, score })
        .collect();
    hits.sort_unstable_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    hits.truncate(k);
    hits
}


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
    fn policy_excluded_node_ids(&self, node_ids: &[u64]) -> Result<NodeIdSet, EngineError> {
        if self.manifest.prune_policies.is_empty() || node_ids.is_empty() {
            return Ok(NodeIdSet::default());
        }
        let cutoffs = PrecomputedPruneCutoffs::from_manifest(&self.manifest, now_millis());
        let records = self.get_nodes_raw(node_ids)?;
        let mut excluded = NodeIdSet::default();
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
    ) -> Result<NodeIdSet, EngineError> {
        if node_ids.is_empty() {
            return Ok(NodeIdSet::default());
        }

        let records = self.get_nodes_raw(node_ids)?;
        let mut visible = NodeIdSet::with_capacity_and_hasher(node_ids.len(), Default::default());
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

    /// Get a node by ID (raw, unfiltered). Checks all sources in precedence
    /// order: active memtable → immutable memtables → segments newest-first.
    /// Returns None if not found or deleted (tombstoned).
    /// Used internally by upsert dedup, cascade deletes, and prune.
    fn get_node_raw(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node(id)
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

    /// Get an edge by ID. Checks all sources in precedence order.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge(id)
    }

    /// Get a node by (type_id, key) across all sources (raw, unfiltered).
    /// Used internally by upsert dedup. Not subject to prune policy filtering.
    fn get_node_by_key_raw(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        self.sources().find_node_by_key(type_id, key)
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

    /// Get an edge by (from, to, type_id) across all sources.
    /// Returns the most recently written edge matching the triple.
    /// Returns None if not found or deleted (tombstoned).
    pub fn get_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        self.sources().find_edge_by_triple(from, to, type_id)
    }

    fn score_dense_candidate_ids(
        &self,
        candidate_ids: &NodeIdSet,
        query: &[f32],
        metric: DenseMetric,
        type_filter: Option<&[u32]>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut ids: Vec<u64> = candidate_ids.iter().copied().collect();
        ids.sort_unstable();
        let query_norm = crate::dense_hnsw::dense_query_norm(metric, query);
        let mut hits = Vec::with_capacity(ids.len());
        let mut remaining = Vec::with_capacity(ids.len());

        for &node_id in &ids {
            if self.memtable.deleted_nodes().contains_key(&node_id) {
                continue;
            }
            if let Some(node) = self.memtable.get_node(node_id) {
                if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                    continue;
                }
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    continue;
                }
                let Some(dense_vector) = node.dense_vector.as_ref() else {
                    continue;
                };
                hits.push(VectorHit {
                    node_id,
                    score: crate::dense_hnsw::dense_score_with_query_norm(
                        metric,
                        query,
                        query_norm,
                        dense_vector,
                    ),
                });
            } else {
                remaining.push(node_id);
            }
        }

        // Check immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&node_id| !epoch.memtable.deleted_nodes().contains_key(&node_id));
            let mut next = Vec::with_capacity(remaining.len());
            for &node_id in &remaining {
                if let Some(node) = epoch.memtable.get_node(node_id) {
                    if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                        continue;
                    }
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        continue;
                    }
                    if let Some(dense_vector) = node.dense_vector.as_ref() {
                        hits.push(VectorHit {
                            node_id,
                            score: crate::dense_hnsw::dense_score_with_query_norm(
                                metric,
                                query,
                                query_norm,
                                dense_vector,
                            ),
                        });
                    }
                } else {
                    next.push(node_id);
                }
            }
            remaining = next;
        }

        let mut next_remaining = Vec::with_capacity(remaining.len());
        for segment in &self.segments {
            if remaining.is_empty() {
                break;
            }
            next_remaining.clear();
            segment.score_dense_candidates_sorted(
                &remaining,
                query,
                metric,
                query_norm,
                |type_id, updated_at, weight| {
                    type_filter.is_none_or(|types| types.contains(&type_id))
                        && policy_cutoffs.is_none_or(|cutoffs| {
                            !cutoffs.excludes_fields(type_id, updated_at, weight)
                        })
                },
                &mut hits,
                &mut next_remaining,
            )?;
            std::mem::swap(&mut remaining, &mut next_remaining);
        }

        hits.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        Ok(hits)
    }

    fn sort_vector_hits(hits: &mut [VectorHit]) {
        hits.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
    }

    fn push_sparse_top_k(
        heap: &mut BinaryHeap<SparseTopKEntry>,
        k: usize,
        node_id: u64,
        score: f32,
    ) {
        let entry = SparseTopKEntry { node_id, score };
        if heap.len() < k {
            heap.push(entry);
            return;
        }

        let should_replace = heap.peek().is_some_and(|worst| {
            entry.score > worst.score
                || (entry.score.total_cmp(&worst.score) == std::cmp::Ordering::Equal
                    && entry.node_id < worst.node_id)
        });
        if should_replace {
            heap.pop();
            heap.push(entry);
        }
    }

    fn sparse_top_k_hits(heap: BinaryHeap<SparseTopKEntry>) -> Vec<VectorHit> {
        heap.into_sorted_vec()
            .into_iter()
            .map(|entry| VectorHit {
                node_id: entry.node_id,
                score: entry.score,
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)] // Dense tail scan needs all context inline for hot-path efficiency
    fn exact_dense_tail_candidates(
        &self,
        segments: &[(&SegmentReader, usize)],
        exhausted: &[bool],
        query: &[f32],
        metric: DenseMetric,
        cutoff: &VectorHit,
        candidate_ids: &mut NodeIdSet,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<NodeIdSet, EngineError> {
        let mut new_candidate_ids =
            NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
        for ((segment, _), segment_exhausted) in segments.iter().zip(exhausted.iter()) {
            if *segment_exhausted {
                continue;
            }
            for (node_id, _) in exact_dense_search_above_cutoff(
                segment.raw_dense_hnsw_meta_mmap(),
                segment.raw_node_dense_vectors_mmap(),
                query,
                metric,
                cutoff.score,
                cutoff.node_id,
            )? {
                if scope_ids.is_some_and(|s| !s.contains(&node_id)) {
                    continue;
                }
                if candidate_ids.insert(node_id) {
                    new_candidate_ids.insert(node_id);
                }
            }
        }
        Ok(new_candidate_ids)
    }

    /// Resolve a `VectorSearchScope` into a set of reachable node IDs using
    /// the existing traversal substrate. The start node (depth 0) is included.
    fn resolve_scope_ids(
        &self,
        scope: &VectorSearchScope,
    ) -> Result<NodeIdSet, EngineError> {
        let traverse_opts = TraverseOptions {
            min_depth: 0,
            direction: scope.direction,
            edge_type_filter: scope.edge_type_filter.clone(),
            node_type_filter: None,
            at_epoch: scope.at_epoch,
            decay_lambda: None,
            limit: None,
            cursor: None,
        };
        let result = self.traverse(
            scope.start_node_id,
            scope.max_depth,
            &traverse_opts,
        )?;
        let mut ids = NodeIdSet::with_capacity_and_hasher(
            result.items.len(),
            NodeIdBuildHasher::default(),
        );
        for hit in &result.items {
            ids.insert(hit.node_id);
        }
        Ok(ids)
    }

    fn vector_search_dense(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let scope_ids = match &request.scope {
            Some(scope) => Some(self.resolve_scope_ids(scope)?),
            None => None,
        };
        self.vector_search_dense_with_scope(request, request.k, scope_ids.as_ref())
    }

    fn vector_search_dense_with_scope(
        &self,
        request: &VectorSearchRequest,
        k: usize,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let Some(query) = request.dense_query.as_ref() else {
            return Err(EngineError::InvalidOperation(
                "vector_search(mode=\"dense\") requires dense_query".into(),
            ));
        };
        if k == 0 {
            return Ok(Vec::new());
        }

        let Some(config) = self.manifest.dense_vector.as_ref() else {
            return Ok(Vec::new());
        };
        validate_dense_vector(query, config)?;

        if let Some(ef_search) = request.ef_search {
            if ef_search == 0 {
                return Err(EngineError::InvalidOperation(
                    "vector_search ef_search must be > 0".into(),
                ));
            }
        }

        let type_filter = request.type_filter.as_deref();
        let policy_cutoffs = self.query_policy_cutoffs();

        let searchable_segments: Vec<(&SegmentReader, usize)> = self
            .segments
            .iter()
            .filter_map(|segment| {
                segment
                    .dense_hnsw_header()
                    .map(|header| (segment, header.point_count as usize))
            })
            .collect();

        // --- Small-scope fast path: skip HNSW, exact-score scope IDs only ---
        if let Some(scope) = scope_ids {
            let total_dense_points: usize = searchable_segments
                .iter()
                .map(|(_, pc)| *pc)
                .sum::<usize>()
                + self
                    .memtable
                    .nodes()
                    .values()
                    .filter(|n| n.dense_vector.is_some())
                    .count()
                + self
                    .immutable_epochs
                    .iter()
                    .flat_map(|epoch| epoch.memtable.nodes().values())
                    .filter(|n| n.dense_vector.is_some())
                    .count();
            if scope.len() <= total_dense_points / 20 || scope.len() <= 2048 {
                let mut hits = self.score_dense_candidate_ids(
                    scope,
                    query,
                    config.metric,
                    type_filter,
                    policy_cutoffs.as_ref(),
                )?;
                hits.truncate(k);
                return Ok(hits);
            }
        }

        // --- Standard path (no scope) or large-scope path (HNSW + ACORN) ---
        let mut candidate_ids = NodeIdSet::with_capacity_and_hasher(
            self.memtable.nodes().len(),
            NodeIdBuildHasher::default(),
        );
        candidate_ids.extend(
            self.memtable
                .nodes()
                .values()
                .filter(|node| {
                    node.dense_vector.is_some()
                        && type_filter.is_none_or(|types| types.contains(&node.type_id))
                        && scope_ids.is_none_or(|s| s.contains(&node.id))
                })
                .map(|node| node.id),
        );
        // Also collect dense vector candidates from immutable memtables.
        // Note: candidate_ids is a NodeIdSet, so duplicates across immutable memtables
        // are harmless; score_dense_candidate_ids does a proper newest-first lookup.
        for epoch in &self.immutable_epochs {
            candidate_ids.extend(
                epoch.memtable.nodes()
                    .values()
                    .filter(|node| {
                        node.dense_vector.is_some()
                            && !self.memtable.deleted_nodes().contains_key(&node.id)
                            && !self.memtable.nodes().contains_key(&node.id)
                            && type_filter.is_none_or(|types| types.contains(&node.type_id))
                            && scope_ids.is_none_or(|s| s.contains(&node.id))
                    })
                    .map(|node| node.id),
            );
        }

        if candidate_ids.is_empty() && searchable_segments.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits = self.score_dense_candidate_ids(
            &candidate_ids,
            query,
            config.metric,
            type_filter,
            policy_cutoffs.as_ref(),
        )?;

        let mut fetch_limit = request
            .ef_search
            .unwrap_or(crate::types::DEFAULT_DENSE_EF_SEARCH)
            .max(k)
            .max(8);

        loop {
            let mut exhausted_segments = true;
            let mut segment_exhausted = Vec::with_capacity(searchable_segments.len());
            let mut new_candidate_ids =
                NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
            for (segment, point_count) in &searchable_segments {
                let limit = fetch_limit.min(*point_count);
                if limit == 0 {
                    segment_exhausted.push(true);
                    continue;
                }
                let is_exhausted = limit >= *point_count;
                if !is_exhausted {
                    exhausted_segments = false;
                }
                segment_exhausted.push(is_exhausted);
                let ef_search = request
                    .ef_search
                    .unwrap_or(fetch_limit)
                    .max(limit)
                    .min(*point_count);

                let segment_hits = if let Some(scope) = scope_ids {
                    segment.search_dense_hnsw_scoped(query, ef_search, limit, scope)?
                } else {
                    segment.search_dense_hnsw(query, ef_search, limit)?
                };
                for (node_id, _) in segment_hits {
                    if candidate_ids.insert(node_id) {
                        new_candidate_ids.insert(node_id);
                    }
                }
            }

            if !new_candidate_ids.is_empty() {
                hits.extend(self.score_dense_candidate_ids(
                    &new_candidate_ids,
                    query,
                    config.metric,
                    type_filter,
                    policy_cutoffs.as_ref(),
                )?);
                Self::sort_vector_hits(&mut hits);
            }

            if hits.len() >= k && !exhausted_segments {
                let cutoff = hits[k - 1].clone();
                let tail_candidate_ids = self.exact_dense_tail_candidates(
                    &searchable_segments,
                    &segment_exhausted,
                    query,
                    config.metric,
                    &cutoff,
                    &mut candidate_ids,
                    scope_ids,
                )?;
                if !tail_candidate_ids.is_empty() {
                    hits.extend(self.score_dense_candidate_ids(
                        &tail_candidate_ids,
                        query,
                        config.metric,
                        type_filter,
                        policy_cutoffs.as_ref(),
                    )?);
                    Self::sort_vector_hits(&mut hits);
                }
                hits.truncate(k);
                return Ok(hits);
            }

            if exhausted_segments {
                hits.truncate(k);
                return Ok(hits);
            }

            let next_limit = fetch_limit.saturating_mul(2);
            if next_limit == fetch_limit {
                hits.truncate(k);
                return Ok(hits);
            }
            fetch_limit = next_limit;
        }
    }

    fn vector_search_sparse_with_scope(
        &self,
        request: &VectorSearchRequest,
        k: usize,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let Some(query) = request.sparse_query.as_ref() else {
            return Err(EngineError::InvalidOperation(
                "vector_search(mode=\"sparse\") requires sparse_query".into(),
            ));
        };
        if k == 0 {
            return Ok(Vec::new());
        }

        let Some(query) = canonicalize_sparse_vector(query)? else {
            return Ok(Vec::new());
        };

        let type_filter = request.type_filter.as_deref();
        let policy_cutoffs = self.query_policy_cutoffs();

        // Small-scope fast path: score scope nodes directly instead of
        // walking full posting lists across the segment.
        if let Some(scope) = scope_ids {
            if scope.len() <= 2048 {
                return self.vector_search_sparse_exact_scope(
                    k,
                    &query,
                    scope,
                    type_filter,
                    policy_cutoffs.as_ref(),
                );
            }
        }

        let mut top_hits = BinaryHeap::with_capacity(k);
        let mut hidden_ids: NodeIdSet = NodeIdSet::with_capacity_and_hasher(
            self.memtable.deleted_nodes().len(),
            NodeIdBuildHasher::default(),
        );
        hidden_ids.extend(self.memtable.deleted_nodes().keys().copied());
        // Include immutable memtable tombstones in hidden_ids
        for epoch in &self.immutable_epochs {
            hidden_ids.extend(epoch.memtable.deleted_nodes().keys().copied());
        }
        hidden_ids.reserve(
            self.memtable.nodes().len()
                + self
                    .immutable_epochs
                    .iter()
                    .map(|epoch| epoch.memtable.nodes().len())
                    .sum::<usize>()
                + self
                    .segments
                    .iter()
                    .take(self.segments.len().saturating_sub(1))
                    .map(|segment| segment.deleted_node_count())
                    .sum::<usize>(),
        );

        for node in self.memtable.nodes().values() {
            hidden_ids.insert(node.id);
            if scope_ids.is_some_and(|s| !s.contains(&node.id)) {
                continue;
            }
            if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                continue;
            }
            if policy_cutoffs
                .as_ref()
                .is_some_and(|cutoffs| cutoffs.excludes(node))
            {
                continue;
            }
            let Some(sparse_vector) = node.sparse_vector.as_ref() else {
                continue;
            };
            let score = sparse_dot_score(&query, sparse_vector);
            if score > 0.0 {
                Self::push_sparse_top_k(&mut top_hits, k, node.id, score);
            }
        }

        // Score nodes in immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            for node in epoch.memtable.nodes().values() {
                hidden_ids.insert(node.id);
                if scope_ids.is_some_and(|s| !s.contains(&node.id)) {
                    continue;
                }
                if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                    continue;
                }
                if policy_cutoffs
                    .as_ref()
                    .is_some_and(|cutoffs| cutoffs.excludes(node))
                {
                    continue;
                }
                let Some(sparse_vector) = node.sparse_vector.as_ref() else {
                    continue;
                };
                let score = sparse_dot_score(&query, sparse_vector);
                if score > 0.0 {
                    Self::push_sparse_top_k(&mut top_hits, k, node.id, score);
                }
            }
        }

        for (segment_index, segment) in self.segments.iter().enumerate() {
            let has_older_segments = segment_index + 1 < self.segments.len();
            let mut scores = NodeIdMap::default();
            accumulate_sparse_posting_scores(
                segment.raw_sparse_posting_index_mmap(),
                segment.raw_sparse_postings_mmap(),
                &query,
                &mut scores,
            )?;
            let mut candidates = Vec::with_capacity(scores.len());
            for (node_id, score) in scores {
                if score > 0.0
                    && !hidden_ids.contains(&node_id)
                    && scope_ids.is_none_or(|s| s.contains(&node_id))
                {
                    candidates.push((node_id, score));
                }
            }
            if candidates.is_empty() {
                if has_older_segments {
                    hidden_ids.extend(segment.deleted_node_ids().iter().copied());
                    hidden_ids.extend(segment.node_ids()?.iter().copied());
                }
                continue;
            }

            candidates.sort_unstable_by_key(|&(node_id, _)| node_id);
            let mut meta_results = vec![None; candidates.len()];
            let mut remaining: Vec<(usize, u64)> = candidates
                .iter()
                .enumerate()
                .map(|(index, &(node_id, _))| (index, node_id))
                .collect();
            remaining.retain(|&(_, node_id)| !hidden_ids.contains(&node_id));
            if remaining.is_empty() {
                if has_older_segments {
                    hidden_ids.extend(segment.deleted_node_ids().iter().copied());
                    hidden_ids.extend(segment.node_ids()?.iter().copied());
                }
                continue;
            }

            remaining.retain(|&(_, node_id)| {
                if segment.is_node_deleted(node_id) {
                    hidden_ids.insert(node_id);
                    false
                } else {
                    true
                }
            });
            if remaining.is_empty() {
                if has_older_segments {
                    hidden_ids.extend(segment.deleted_node_ids().iter().copied());
                    hidden_ids.extend(segment.node_ids()?.iter().copied());
                }
                continue;
            }

            meta_results.fill(None);
            segment.get_node_meta_batch(&remaining, &mut meta_results)?;
            for (index, (node_id, score)) in candidates.into_iter().enumerate() {
                let Some((type_id, updated_at, weight)) = meta_results[index] else {
                    continue;
                };
                if type_filter.is_some_and(|types| !types.contains(&type_id)) {
                    continue;
                }
                if policy_cutoffs
                    .as_ref()
                    .is_some_and(|cutoffs| cutoffs.excludes_fields(type_id, updated_at, weight))
                {
                    continue;
                }
                Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
            }
            if has_older_segments {
                hidden_ids.extend(segment.deleted_node_ids().iter().copied());
                hidden_ids.extend(segment.node_ids()?.iter().copied());
            }
        }

        Ok(Self::sparse_top_k_hits(top_hits))
    }

    /// Small-scope sparse search: look up each scope node's sparse vector
    /// directly and score against the query. Avoids full posting-list walks.
    fn vector_search_sparse_exact_scope(
        &self,
        k: usize,
        query: &[(u32, f32)],
        scope_ids: &NodeIdSet,
        type_filter: Option<&[u32]>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let mut top_hits = BinaryHeap::with_capacity(k);

        // Sort scope IDs for segment merge walks.
        let mut sorted_ids: Vec<u64> = scope_ids.iter().copied().collect();
        sorted_ids.sort_unstable();

        // Track IDs we still need to find across segments (memtable first).
        let mut remaining = Vec::with_capacity(sorted_ids.len());
        for &node_id in &sorted_ids {
            if self.memtable.deleted_nodes().contains_key(&node_id) {
                continue;
            }
            if let Some(node) = self.memtable.get_node(node_id) {
                if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                    continue;
                }
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    continue;
                }
                if let Some(sparse_vector) = node.sparse_vector.as_ref() {
                    let score = sparse_dot_score(query, sparse_vector);
                    if score > 0.0 {
                        Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
                    }
                }
                continue;
            }
            remaining.push(node_id);
        }

        // Check immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&id| !epoch.memtable.deleted_nodes().contains_key(&id));
            let mut next = Vec::with_capacity(remaining.len());
            for &node_id in &remaining {
                if let Some(node) = epoch.memtable.get_node(node_id) {
                    if type_filter.is_some_and(|types| !types.contains(&node.type_id)) {
                        continue;
                    }
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        continue;
                    }
                    if let Some(sparse_vector) = node.sparse_vector.as_ref() {
                        let score = sparse_dot_score(query, sparse_vector);
                        if score > 0.0 {
                            Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
                        }
                    }
                    continue;
                }
                next.push(node_id);
            }
            remaining = next;
        }

        // Walk segments newest-first with sorted merge.
        let mut next_remaining = Vec::with_capacity(remaining.len());
        let mut segment_hits = Vec::new();
        for segment in &self.segments {
            if remaining.is_empty() {
                break;
            }
            // Filter out tombstoned IDs in this segment.
            remaining.retain(|&id| !segment.is_node_deleted(id));
            if remaining.is_empty() {
                break;
            }

            segment_hits.clear();
            next_remaining.clear();
            segment.score_sparse_candidates_sorted(
                &remaining,
                query,
                |type_id, updated_at, weight| {
                    type_filter.is_none_or(|types| types.contains(&type_id))
                        && policy_cutoffs.is_none_or(|cutoffs| {
                            !cutoffs.excludes_fields(type_id, updated_at, weight)
                        })
                },
                &mut segment_hits,
                &mut next_remaining,
            )?;
            for &(node_id, score) in &segment_hits {
                Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
            }
            // IDs not found in this segment continue to older segments.
            remaining = std::mem::take(&mut next_remaining);
        }

        Ok(Self::sparse_top_k_hits(top_hits))
    }

    /// Hybrid vector search: run dense and sparse sub-searches, then fuse
    /// results using the selected fusion mode.
    fn vector_search_hybrid(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let has_dense = request.dense_query.is_some();
        let has_sparse = request.sparse_query.is_some();

        if !has_dense && !has_sparse {
            return Err(EngineError::InvalidOperation(
                "vector_search(mode=\"hybrid\") requires at least one of dense_query or sparse_query".into(),
            ));
        }

        // Degenerate: single-modality fast paths (use original k, no fusion).
        if has_dense && !has_sparse {
            return self.vector_search_dense(request);
        }
        if has_sparse && !has_dense {
            let scope_ids = match &request.scope {
                Some(scope) => Some(self.resolve_scope_ids(scope)?),
                None => None,
            };
            return self.vector_search_sparse_with_scope(request, request.k, scope_ids.as_ref());
        }

        // True hybrid: both modalities present.
        if request.k == 0 {
            return Ok(Vec::new());
        }

        // Resolve scope once, shared by both sub-searches.
        let scope_ids = match &request.scope {
            Some(scope) => Some(self.resolve_scope_ids(scope)?),
            None => None,
        };

        // Over-fetch from each modality for better fusion quality.
        let sub_k = request.k.saturating_mul(HYBRID_OVERFETCH_FACTOR).max(request.k);

        let (dense_result, sparse_result) = std::thread::scope(|s| {
            let dense = s.spawn(|| {
                self.vector_search_dense_with_scope(request, sub_k, scope_ids.as_ref())
            });
            let sparse = s.spawn(|| {
                self.vector_search_sparse_with_scope(request, sub_k, scope_ids.as_ref())
            });
            (dense.join().unwrap(), sparse.join().unwrap())
        });
        let dense_hits = dense_result?;
        let sparse_hits = sparse_result?;

        let fusion_mode = request.fusion_mode.unwrap_or_default();
        let dense_weight = request.dense_weight.unwrap_or(1.0);
        let sparse_weight = request.sparse_weight.unwrap_or(1.0);

        let fused = match fusion_mode {
            FusionMode::WeightedRankFusion => {
                fuse_weighted_rank(&dense_hits, &sparse_hits, dense_weight, sparse_weight, request.k)
            }
            FusionMode::ReciprocalRankFusion => {
                fuse_reciprocal_rank(&dense_hits, &sparse_hits, request.k)
            }
            FusionMode::WeightedScoreFusion => {
                fuse_weighted_score(&dense_hits, &sparse_hits, dense_weight, sparse_weight, request.k)
            }
        };

        Ok(fused)
    }

    /// Search node vectors and return scored node IDs.
    ///
    /// Supports `mode="dense"`, `mode="sparse"`, and `mode="hybrid"`.
    /// All modes support optional graph-scoped filtering via `scope`.
    pub fn vector_search(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        match request.mode {
            VectorSearchMode::Dense => self.vector_search_dense(request),
            VectorSearchMode::Sparse => {
                let scope_ids = match &request.scope {
                    Some(scope) => Some(self.resolve_scope_ids(scope)?),
                    None => None,
                };
                self.vector_search_sparse_with_scope(request, request.k, scope_ids.as_ref())
            }
            VectorSearchMode::Hybrid => self.vector_search_hybrid(request),
        }
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

        // Check immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, id)| {
                if epoch.memtable.deleted_nodes().contains_key(&id) {
                    return false; // tombstoned, stop looking
                }
                if let Some(node) = epoch.memtable.get_node(id) {
                    results[i] = Some(node.clone());
                    return false; // found, stop looking
                }
                true // keep searching
            });
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

    /// Batch-resolve `(type_id, key)` pairs to node records across all sources.
    /// Returns `Vec<Option<NodeRecord>>` with the same length and order as input.
    /// Duplicates get duplicate result slots.
    ///
    /// Walks memtable (O(1) per key) → immutable memtables → segments (batch
    /// merge-walk or per-key seek, cost-model selected). A key resolved in a
    /// newer source is authoritative even if tombstoned. It does NOT fall
    /// through to older segments.
    fn get_nodes_by_keys_raw(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let n = keys.len();
        let mut results: Vec<Option<NodeRecord>> = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        // Phase 1: Active memtable, O(1) HashMap per key
        let mut remaining: Vec<(usize, u32, &str)> = Vec::with_capacity(n);
        for (i, &(type_id, key)) in keys.iter().enumerate() {
            if let Some(node) = self.memtable.node_by_key(type_id, key) {
                results[i] = Some(node.clone());
            } else {
                remaining.push((i, type_id, key));
            }
        }

        // Phase 2: Immutable memtables (newest-first)
        for (epoch_idx, epoch) in self.immutable_epochs.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, type_id, key)| {
                if let Some(node) = epoch.memtable.node_by_key(type_id, key) {
                    // Check higher-precedence sources: active memtable +
                    // newer immutable epochs (mirrors is_node_tombstoned_above_immutable)
                    let tombstoned = self.memtable.deleted_nodes().contains_key(&node.id)
                        || self.immutable_epochs[..epoch_idx]
                            .iter()
                            .any(|e| e.memtable.deleted_nodes().contains_key(&node.id));
                    if tombstoned {
                        return false; // tombstoned, stop searching this key
                    }
                    results[i] = Some(node.clone());
                    return false;
                }
                true
            });
        }

        // Phase 3: Sort remaining by (type_id, key) for segment batch ops
        remaining.sort_unstable_by(|a, b| (a.1, a.2).cmp(&(b.1, b.2)));

        // Pre-collect deleted node IDs from memtable + all immutable epochs
        // so the per-found-node tombstone check inside the segment loop is O(1)
        // instead of O(F × I).
        let mut deleted_above: crate::types::NodeIdSet = self
            .memtable
            .deleted_nodes()
            .keys()
            .copied()
            .collect();
        for e in &self.immutable_epochs {
            deleted_above.extend(e.memtable.deleted_nodes().keys().copied());
        }

        // Phase 4: Segments (newest-first)
        for (seg_idx, seg) in self.segments.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }

            // Resolve keys → records; returns which orig_idxs were found in key index
            let found = seg.resolve_keys_batch(&remaining, &mut results)?;

            // Tombstone-filter found results: check higher-precedence sources
            for &orig_idx in &found {
                if let Some(ref node) = results[orig_idx] {
                    let tombstoned = deleted_above.contains(&node.id)
                        || self.segments[..seg_idx]
                            .iter()
                            .any(|s| s.is_node_deleted(node.id));
                    if tombstoned {
                        results[orig_idx] = None;
                    }
                }
            }

            // Remove ALL found keys from remaining (even if tombstoned,
            // the newest source containing a key is authoritative).
            if !found.is_empty() {
                let mut found_mask = vec![false; n];
                for &idx in &found {
                    found_mask[idx] = true;
                }
                remaining.retain(|&(i, _, _)| !found_mask[i]);
            }
        }

        Ok(results)
    }

    /// Get multiple nodes by `(type_id, key)` in a single call.
    /// Returns `Vec<Option<NodeRecord>>`, one per input key (order preserved).
    /// Missing, deleted, or policy-excluded nodes are None.
    ///
    /// Uses a batched approach: resolves memtable hits first (O(1) each),
    /// then does a single sorted merge-walk per segment instead of N binary searches.
    pub fn get_nodes_by_keys(
        &self,
        keys: &[(u32, &str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = self.get_nodes_by_keys_raw(keys)?;

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

        // Check immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, id)| {
                if epoch.memtable.deleted_edges().contains_key(&id) {
                    return false;
                }
                if let Some(edge) = epoch.memtable.get_edge(id) {
                    results[i] = Some(edge.clone());
                    return false;
                }
                true
            });
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
        let (deleted, _) = self.collect_tombstones();

        let mut seen = NodeIdSet::default();
        let mut results = Vec::new();

        for id in self.memtable.nodes_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for epoch in &self.immutable_epochs {
            for id in epoch.memtable.nodes_by_type(type_id) {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
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
        let (_, deleted) = self.collect_tombstones();

        let mut seen = NodeIdSet::default();
        let mut results = Vec::new();

        for id in self.memtable.edges_by_type(type_id) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for epoch in &self.immutable_epochs {
            for id in epoch.memtable.edges_by_type(type_id) {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
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
        let mut deleted: NodeIdSet = self.memtable.deleted_nodes().keys().copied().collect();
        for epoch in &self.immutable_epochs {
            deleted.extend(epoch.memtable.deleted_nodes().keys().copied());
        }
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.nodes_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.nodes_by_type(type_id));
        }
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
        let mut deleted: NodeIdSet = self.memtable.deleted_edges().keys().copied().collect();
        for epoch in &self.immutable_epochs {
            deleted.extend(epoch.memtable.deleted_edges().keys().copied());
        }
        for seg in &self.segments {
            deleted.extend(seg.deleted_edge_ids());
        }

        // Collect sources
        let memtable_ids = self.memtable.edges_by_type(type_id);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.edges_by_type(type_id));
        }
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
        let mut deleted: NodeIdSet = self.memtable.deleted_nodes().keys().copied().collect();
        for epoch in &self.immutable_epochs {
            deleted.extend(epoch.memtable.deleted_nodes().keys().copied());
        }
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        let mut seen = NodeIdSet::default();
        let mut results = Vec::new();

        // Memtable first (already post-filtered)
        for id in self.memtable.find_nodes(type_id, prop_key, prop_value) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        // Immutable memtables (newest-first, already post-filtered)
        for epoch in &self.immutable_epochs {
            for id in epoch.memtable.find_nodes(type_id, prop_key, prop_value) {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
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
        let mut deleted: NodeIdSet = self.memtable.deleted_nodes().keys().copied().collect();
        for epoch in &self.immutable_epochs {
            deleted.extend(epoch.memtable.deleted_nodes().keys().copied());
        }
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect candidate IDs from sources (sorted within each source)
        let memtable_ids = self.memtable.find_nodes(type_id, prop_key, prop_value);
        let key_hash = hash_prop_key(prop_key);
        let value_hash = hash_prop_value(prop_value);
        // Immutable memtable find_nodes results go into segment_ids
        // (prepended before actual segments since they are newer)
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.find_nodes(type_id, prop_key, prop_value));
        }
        for seg in &self.segments {
            segment_ids.push(seg.find_nodes_by_prop_hash(type_id, key_hash, value_hash)?);
        }

        // Reuse the already-computed memtable_ids + immutable find_nodes results
        // instead of calling find_nodes() a second time on each source.
        let mut memtable_verified: NodeIdSet = memtable_ids.iter().copied().collect();
        for ids in &segment_ids[..self.immutable_epochs.len()] {
            memtable_verified.extend(ids.iter().copied());
        }
        let limit = page.limit.unwrap_or(0);
        if limit == 0 {
            let all_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let merged = merge_type_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
            let mut visible: NodeIdSet = NodeIdSet::with_capacity_and_hasher(merged.items.len(), Default::default());
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
            for (id, node) in segment_candidates
                .into_iter()
                .zip(batch_results.into_iter())
            {
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
                let mut visible: NodeIdSet = NodeIdSet::with_capacity_and_hasher(merged.items.len(), Default::default());
                for id in merged.items.iter().copied() {
                    if memtable_verified.contains(&id) {
                        visible.insert(id);
                    }
                }
                for (id, node) in segment_candidates
                    .into_iter()
                    .zip(batch_results.into_iter())
                {
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
        let mut deleted: NodeIdSet = self.memtable.deleted_nodes().keys().copied().collect();
        for epoch in &self.immutable_epochs {
            deleted.extend(epoch.memtable.deleted_nodes().keys().copied());
        }
        for seg in &self.segments {
            deleted.extend(seg.deleted_node_ids());
        }

        // Collect sources: memtable + immutable memtables + segments
        let memtable_ids = self.memtable.nodes_by_time_range(type_id, from_ms, to_ms);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.nodes_by_time_range(type_id, from_ms, to_ms));
        }
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
                let mut visible: NodeIdSet = NodeIdSet::with_capacity_and_hasher(chunk.items.len(), Default::default());
                for (id, node) in chunk.items.iter().zip(nodes.iter()) {
                    if let Some(n) = node {
                        if n.updated_at >= from_ms && n.updated_at <= to_ms {
                            visible.insert(*id);
                        }
                    }
                }

                let excluded = if self.manifest.prune_policies.is_empty() {
                    NodeIdSet::default()
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
        let mut discovered: NodeIdSet = seeds.iter().copied().collect();
        let mut wave: Vec<u64> = seeds.clone();
        let mut neighbor_cache: NodeIdMap<Vec<(u64, f32)>> =
            NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());

        while !wave.is_empty() {
            let batch_opts = NeighborOptions {
                    direction: Direction::Outgoing,
                    type_filter: edge_filter.map(|s| s.to_vec()),
                    limit: None,
                    at_epoch: None,
                    decay_lambda: None,
                };
            let all_neighbors = self.neighbors_batch(&wave, &batch_opts)?;

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
        let id_to_idx: NodeIdMap<usize> = idx_to_id
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
        // Collect all node type IDs from memtable + immutable memtables + segments
        let node_types: Vec<u32> = {
            let mut types: HashSet<u32> = self.memtable.type_node_index().keys().copied().collect();
            for epoch in &self.immutable_epochs {
                types.extend(epoch.memtable.type_node_index().keys().copied());
            }
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
        let mut node_set: NodeIdSet = NodeIdSet::default();
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
        let batch_opts = NeighborOptions {
            direction: Direction::Outgoing,
            type_filter: edge_filter_slice.map(|s| s.to_vec()),
            limit: None,
            at_epoch: None,
            decay_lambda: None,
        };
        let all_neighbors = self.neighbors_batch(&node_ids, &batch_opts)?;

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
