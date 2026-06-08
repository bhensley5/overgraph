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

struct ResolvedVectorSearchScope {
    start_node_id: u64,
    max_depth: u32,
    direction: Direction,
    edge_label_filter: Option<Vec<u32>>,
    at_epoch: Option<i64>,
}

struct ResolvedVectorSearchRequest<'a> {
    mode: VectorSearchMode,
    dense_query: Option<&'a DenseVector>,
    sparse_query: Option<&'a SparseVector>,
    k: usize,
    label_filter: ResolvedNodeLabelFilter,
    ef_search: Option<usize>,
    scope: Option<ResolvedVectorSearchScope>,
    dense_weight: Option<f32>,
    sparse_weight: Option<f32>,
    fusion_mode: Option<FusionMode>,
}

fn finish_verified_id_page(mut items: Vec<u64>, limit: usize) -> PageResult<u64> {
    let next_cursor = if limit > 0 && items.len() > limit {
        items.truncate(limit);
        items.last().copied()
    } else {
        None
    };
    PageResult { items, next_cursor }
}

enum NodeLabelScanSource<'a> {
    Memtable {
        memtable: &'a Memtable,
        label_id: u32,
        snapshot_seq: u64,
    },
    Segment {
        segment: &'a SegmentReader,
        posting: SegmentLabelPosting,
    },
}

impl NodeLabelScanSource<'_> {
    fn next_after(&self, after: Option<u64>) -> Result<Option<u64>, EngineError> {
        match self {
            NodeLabelScanSource::Memtable {
                memtable,
                label_id,
                snapshot_seq,
            } => Ok(memtable.next_visible_node_by_label_id_after(
                *label_id,
                *snapshot_seq,
                after,
            )),
            NodeLabelScanSource::Segment { segment, posting } => {
                let index = match after {
                    Some(after) => segment.node_label_posting_lower_bound(*posting, after)?,
                    None => 0,
                };
                segment.node_id_at_label_posting(*posting, index)
            }
        }
    }
}

enum EdgeLabelScanSource<'a> {
    Memtable {
        memtable: &'a Memtable,
        label_id: u32,
        snapshot_seq: u64,
    },
    Segment {
        segment: &'a SegmentReader,
        posting: SegmentLabelPosting,
    },
}

impl EdgeLabelScanSource<'_> {
    fn next_after(&self, after: Option<u64>) -> Result<Option<u64>, EngineError> {
        match self {
            EdgeLabelScanSource::Memtable {
                memtable,
                label_id,
                snapshot_seq,
            } => Ok(memtable.next_visible_edge_by_label_id_after(
                *label_id,
                *snapshot_seq,
                after,
            )),
            EdgeLabelScanSource::Segment { segment, posting } => {
                let index = match after {
                    Some(after) => segment.edge_label_id_lower_bound_posting(*posting, after)?,
                    None => 0,
                };
                segment.edge_label_id_at_posting(*posting, index)
            }
        }
    }
}

struct EqualityRawPostingBudget {
    cap: usize,
    consumed: usize,
}

enum EqualitySegmentPostingScan {
    #[cfg(test)]
    AcceptedCapReached,
    RawCapExceeded,
    Exhausted,
}

impl EqualityRawPostingBudget {
    fn new(cap: usize) -> Self {
        Self { cap, consumed: 0 }
    }

    fn next_chunk_limit(&self) -> Option<usize> {
        if self.consumed > self.cap {
            None
        } else {
            Some(
                self.cap
                    .saturating_add(1)
                    .saturating_sub(self.consumed)
                    .min(QUERY_VERIFY_CHUNK),
            )
        }
    }

    fn consume(&mut self, count: usize) -> bool {
        self.consumed = self.consumed.saturating_add(count);
        self.consumed > self.cap
    }
}

fn compare_range_values(left: &PropValue, right: &PropValue) -> Option<std::cmp::Ordering> {
    compare_numeric_prop_values(left, right)
}

fn compare_range_cursor_to_candidate(
    cursor: &PropertyRangeCursor,
    value: &PropValue,
    node_id: u64,
) -> Option<std::cmp::Ordering> {
    compare_range_values(&cursor.value, value).map(|ordering| {
        if ordering == std::cmp::Ordering::Equal {
            cursor.node_id.cmp(&node_id)
        } else {
            ordering
        }
    })
}

fn range_value_within_bounds(
    value: &PropValue,
    lower: Option<&PropertyRangeBound>,
    upper: Option<&PropertyRangeBound>,
) -> Option<bool> {
    crate::property_value_semantics::numeric_range_value_within_bounds(value, lower, upper)
}

fn memtable_secondary_eq_nodes_for_filter(
    memtable: &Memtable,
    index_id: u64,
    prop_key: &str,
    prop_value: &PropValue,
    snapshot_seq: u64,
) -> Vec<u64> {
    memtable.find_secondary_eq_nodes_at(index_id, prop_key, prop_value, snapshot_seq)
}

fn memtable_secondary_eq_raw_nodes_for_filter(
    memtable: &Memtable,
    index_id: u64,
    prop_value: &PropValue,
    snapshot_seq: u64,
    max_ids: Option<usize>,
) -> Vec<u64> {
    let mut ids = Vec::new();
    for value_hash in equality_probe_value_hashes(prop_value) {
        let remaining = max_ids.map(|max_ids| max_ids.saturating_sub(ids.len()));
        if remaining == Some(0) {
            break;
        }
        ids.extend(memtable.find_secondary_eq_nodes_by_hash_at_limited(
            index_id,
            value_hash,
            snapshot_seq,
            remaining,
        ));
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn memtable_secondary_eq_count_for_filter(
    memtable: &Memtable,
    index_id: u64,
    prop_key: &str,
    prop_value: &PropValue,
    snapshot_seq: u64,
) -> usize {
    memtable.secondary_eq_node_count_at(index_id, prop_key, prop_value, snapshot_seq)
}

fn segment_secondary_eq_ids_for_filter(
    segment: &SegmentReader,
    index_id: u64,
    prop_value: &PropValue,
) -> Result<Option<Vec<u64>>, EngineError> {
    let mut ids = Vec::new();
    for value_hash in equality_probe_value_hashes(prop_value) {
        let Some(mut probe_ids) = segment
            .find_nodes_by_secondary_eq_index_if_present(index_id, value_hash)?
        else {
            return Ok(None);
        };
        ids.append(&mut probe_ids);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(Some(ids))
}

fn segment_secondary_eq_posting_count_for_filter(
    segment: &SegmentReader,
    index_id: u64,
    prop_value: &PropValue,
) -> Result<Option<usize>, EngineError> {
    let mut count = 0usize;
    for value_hash in equality_probe_value_hashes(prop_value) {
        let Some(probe_count) =
            segment.secondary_eq_posting_count_if_present(index_id, value_hash)?
        else {
            return Ok(None);
        };
        count = count.saturating_add(probe_count);
    }
    Ok(Some(count))
}

#[derive(Clone, Debug)]
struct RangeScanMatch {
    encoded_value: NumericRangeSortKey,
    value: PropValue,
    node_id: u64,
}

impl PartialEq for RangeScanMatch {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_value == other.encoded_value && self.node_id == other.node_id
    }
}

impl Eq for RangeScanMatch {}

impl Ord for RangeScanMatch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded_value
            .cmp(&other.encoded_value)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for RangeScanMatch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

enum ReadyRangeSourceStep {
    Entry((NumericRangeSortKey, u64)),
    Exhausted,
    MissingSidecar,
}

enum ReadyRangeSourceKind<'a> {
    Memtable {
        memtable: &'a Memtable,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
        buffer: Vec<(NumericRangeSortKey, u64)>,
        offset: usize,
        chunk_size: usize,
        exhausted: bool,
    },
    Segment {
        segment: &'a SegmentReader,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        buffer: Vec<(NumericRangeSortKey, u64)>,
        offset: usize,
        chunk_size: usize,
    },
}

struct ReadyRangeSource<'a> {
    kind: ReadyRangeSourceKind<'a>,
}

type ReadyEqualitySourceIds = (Vec<u64>, Vec<Vec<u64>>, NodeIdSet);

fn push_unique_candidate_id_limited(
    ids: &mut Vec<u64>,
    seen: &mut NodeIdSet,
    node_id: u64,
    max_ids: usize,
) -> ControlFlow<()> {
    if max_ids == 0 {
        return ControlFlow::Break(());
    }
    if seen.insert(node_id) {
        ids.push(node_id);
        if ids.len() >= max_ids {
            return ControlFlow::Break(());
        }
    }
    ControlFlow::Continue(())
}

impl ReadyRangeSource<'_> {
    fn next_entry(&mut self) -> Result<ReadyRangeSourceStep, EngineError> {
        match &mut self.kind {
            ReadyRangeSourceKind::Memtable {
                memtable,
                index_id,
                lower,
                upper,
                after,
                snapshot_seq,
                buffer,
                offset,
                chunk_size,
                exhausted,
            } => loop {
                if *offset < buffer.len() {
                    let entry = buffer[*offset];
                    *offset += 1;
                    return Ok(ReadyRangeSourceStep::Entry(entry));
                }
                if *exhausted {
                    return Ok(ReadyRangeSourceStep::Exhausted);
                }

                let entries = memtable.visible_secondary_range_entries_limited(
                    *index_id,
                    *lower,
                    *upper,
                    *after,
                    *snapshot_seq,
                    Some(*chunk_size),
                );
                if entries.is_empty() {
                    *exhausted = true;
                    return Ok(ReadyRangeSourceStep::Exhausted);
                }

                *after = entries.last().copied();
                *buffer = entries;
                *offset = 0;
            },
            ReadyRangeSourceKind::Segment {
                segment,
                index_id,
                lower,
                upper,
                after,
                buffer,
                offset,
                chunk_size,
            } => loop {
                if *offset < buffer.len() {
                    let entry = buffer[*offset];
                    *offset += 1;
                    return Ok(ReadyRangeSourceStep::Entry(entry));
                }

                let Some(entries) = segment.find_nodes_by_secondary_range_index_if_present_limited(
                    *index_id,
                    *lower,
                    *upper,
                    *after,
                    Some(*chunk_size),
                )?
                else {
                    return Ok(ReadyRangeSourceStep::MissingSidecar);
                };
                if entries.is_empty() {
                    return Ok(ReadyRangeSourceStep::Exhausted);
                }

                *after = entries.last().copied();
                *buffer = entries;
                *offset = 0;
            },
        }
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

impl ReadView {
    // --- Read-time policy filtering ---

    /// Check if a single node is excluded by any registered prune policy.
    /// Early-out when no policies are registered.
    fn is_node_excluded_by_policies(&self, node: &NodeRecord) -> bool {
        if self.manifest.prune_policies.is_empty() {
            return false;
        }
        let now = now_millis();
        let cutoffs = PrecomputedPruneCutoffs::from_policies(&self.manifest.prune_policies, now);
        cutoffs.excludes(node)
    }

    /// Batch-compute the set of node IDs that should be excluded by prune policies.
    /// Uses latest-source visibility metadata instead of hydrating full records.
    /// Returns an empty set when no policies are registered (zero overhead).
    fn policy_excluded_node_ids(&self, node_ids: &[u64]) -> Result<NodeIdSet, EngineError> {
        if self.manifest.prune_policies.is_empty() || node_ids.is_empty() {
            return Ok(NodeIdSet::default());
        }
        let cutoffs =
            PrecomputedPruneCutoffs::from_policies(&self.manifest.prune_policies, now_millis());
        let visibility = self.sources().find_node_visibility_meta(node_ids)?;
        let mut excluded = NodeIdSet::default();
        for (&node_id, state) in node_ids.iter().zip(visibility.iter()) {
            if let NodeVisibilityState::Live(meta) = state {
                if cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight) {
                    excluded.insert(node_id);
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
            Some(PrecomputedPruneCutoffs::from_policies(
                &self.manifest.prune_policies,
                now_millis(),
            ))
        }
    }

    fn ready_equality_node_matches(
        node: Option<&NodeRecord>,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> bool {
        let Some(node) = node else {
            return false;
        };
        if !node.label_ids.contains(label_id)
            || !node
                .props
                .get(prop_key)
                .map(|value| prop_values_equal_for_filter(value, prop_value))
                .unwrap_or(false)
        {
            return false;
        }
        true
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

        let visibility = self.sources().find_node_visibility_meta(node_ids)?;
        let mut visible = NodeIdSet::with_capacity_and_hasher(node_ids.len(), Default::default());
        for (&node_id, state) in node_ids.iter().zip(visibility.iter()) {
            if let NodeVisibilityState::Live(meta) = state {
                if policy_cutoffs
                    .is_none_or(|cutoffs| !cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight))
                {
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

    fn equality_sidecar_failure_followup(
        &self,
        index_id: u64,
        error: Option<EngineError>,
    ) -> Option<SecondaryIndexReadFollowup> {
        let next_state = if error
            .as_ref()
            .is_some_and(|error| !is_not_found_io_error(error))
        {
            SecondaryIndexState::Failed
        } else {
            SecondaryIndexState::Building
        };
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.as_ref().map(ToString::to_string)
        } else {
            None
        };

        let current_entry = self
            .secondary_index_entries
            .iter()
            .find(|entry| entry.index_id == index_id)?;
        if !matches!(current_entry.kind, SecondaryIndexKind::Equality) {
            return None;
        }
        if current_entry.state == next_state && current_entry.last_error == next_last_error {
            return None;
        }

        Some(SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id, error })
    }

    fn ready_equality_verified_node_ids(
        &self,
        merged_ids: &[u64],
        memtable_verified: &NodeIdSet,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<NodeIdSet, EngineError> {
        let mut visible =
            NodeIdSet::with_capacity_and_hasher(merged_ids.len(), Default::default());
        let segment_candidates: Vec<u64> = merged_ids
            .iter()
            .copied()
            .filter(|id| !memtable_verified.contains(id))
            .collect();
        for &id in merged_ids {
            if memtable_verified.contains(&id) {
                visible.insert(id);
            }
        }
        if segment_candidates.is_empty() {
            return Ok(visible);
        }

        let segment_candidates =
            self.filter_node_ids_by_current_label(segment_candidates, label_id)?;
        let batch_results = self.get_nodes_raw(&segment_candidates)?;
        for (id, node) in segment_candidates.into_iter().zip(batch_results) {
            if Self::ready_equality_node_matches(node.as_ref(), label_id, prop_key, prop_value) {
                visible.insert(id);
            }
        }
        Ok(visible)
    }

    fn ready_equality_source_ids(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<
        (
            Option<ReadyEqualitySourceIds>,
            Option<SecondaryIndexReadFollowup>,
        ),
        EngineError,
    > {
        let memtable_ids = memtable_secondary_eq_nodes_for_filter(
            &self.memtable,
            index_id,
            prop_key,
            prop_value,
            self.snapshot_seq,
        );
        let memtable_verified: NodeIdSet = memtable_ids.iter().copied().collect();
        let mut deleted_above = self.memtable.collect_deleted_nodes_at(self.snapshot_seq);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());

        for epoch in &self.immutable_epochs {
            let ids: Vec<u64> = memtable_secondary_eq_nodes_for_filter(
                &epoch.memtable,
                index_id,
                prop_key,
                prop_value,
                self.snapshot_seq,
            )
                .into_iter()
                .filter(|id| !deleted_above.contains(id))
                .collect();
            segment_ids.push(ids);
            deleted_above.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        }

        for seg in &self.segments {
            let ids = match segment_secondary_eq_ids_for_filter(seg, index_id, prop_value) {
                Ok(Some(ids)) => ids,
                Ok(None) => {
                    return Ok((None, self.equality_sidecar_failure_followup(index_id, None)));
                }
                Err(error) => {
                    return Ok((
                        None,
                        self.equality_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            };
            let ids: Vec<u64> = ids
                .into_iter()
                .filter(|id| !deleted_above.contains(id))
                .collect();
            segment_ids.push(ids);
            deleted_above.extend(seg.deleted_node_ids());
        }

        Ok((Some((memtable_ids, segment_ids, memtable_verified)), None))
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn extend_verified_equality_candidates_from_segment_postings(
        &self,
        segment: &SegmentReader,
        index_id: u64,
        value_hash: u64,
        prop_key: &str,
        prop_value: &PropValue,
        max_ids: usize,
        mut raw_posting_budget: Option<&mut EqualityRawPostingBudget>,
        accepted: &mut NodeIdSet,
    ) -> Result<Option<EqualitySegmentPostingScan>, EngineError> {
        let mut posting_offset = 0usize;
        loop {
            let raw_limit = if let Some(budget) = raw_posting_budget.as_ref() {
                match budget.next_chunk_limit() {
                    Some(limit) if limit > 0 => limit,
                    _ => return Ok(Some(EqualitySegmentPostingScan::RawCapExceeded)),
                }
            } else {
                QUERY_VERIFY_CHUNK
            };
            let chunk_start = posting_offset;
            let Some(chunk) = segment.secondary_eq_posting_chunk_if_present(
                index_id,
                value_hash,
                posting_offset,
                raw_limit,
            )?
            else {
                return Ok(None);
            };
            posting_offset = chunk.next_offset;
            if let Some(budget) = raw_posting_budget.as_mut() {
                if budget.consume(posting_offset.saturating_sub(chunk_start)) {
                    return Ok(Some(EqualitySegmentPostingScan::RawCapExceeded));
                }
            }
            if self.extend_verified_equality_candidates(
                chunk.ids, prop_key, prop_value, max_ids, accepted,
            )? {
                return Ok(Some(EqualitySegmentPostingScan::AcceptedCapReached));
            }
            if chunk.exhausted {
                return Ok(Some(EqualitySegmentPostingScan::Exhausted));
            }
        }
    }

    fn extend_raw_equality_candidates_from_segment_postings(
        &self,
        segment: &SegmentReader,
        index_id: u64,
        value_hash: u64,
        raw_posting_budget: &mut EqualityRawPostingBudget,
        deleted_above: &NodeIdSet,
        accepted: &mut NodeIdSet,
    ) -> Result<Option<EqualitySegmentPostingScan>, EngineError> {
        let mut posting_offset = 0usize;
        loop {
            let raw_limit = match raw_posting_budget.next_chunk_limit() {
                Some(limit) if limit > 0 => limit,
                _ => return Ok(Some(EqualitySegmentPostingScan::RawCapExceeded)),
            };
            let chunk_start = posting_offset;
            let Some(chunk) = segment.secondary_eq_posting_chunk_if_present(
                index_id,
                value_hash,
                posting_offset,
                raw_limit,
            )?
            else {
                return Ok(None);
            };
            posting_offset = chunk.next_offset;
            if raw_posting_budget.consume(posting_offset.saturating_sub(chunk_start)) {
                return Ok(Some(EqualitySegmentPostingScan::RawCapExceeded));
            }
            accepted.extend(chunk.ids.into_iter().filter(|id| !deleted_above.contains(id)));
            if chunk.exhausted {
                return Ok(Some(EqualitySegmentPostingScan::Exhausted));
            }
        }
    }

    #[cfg(test)]
    fn ready_equality_candidate_ids_limited(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        max_ids: Option<usize>,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        self.ready_equality_candidate_ids_limited_internal(
            index_id, prop_key, prop_value, max_ids, None,
        )
    }

    #[cfg(test)]
    fn ready_equality_candidate_ids_limited_by_raw_postings(
        &self,
        index_id: u64,
        prop_value: &PropValue,
        raw_posting_cap: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        self.ready_equality_candidate_ids_raw_limited(
            index_id,
            prop_value,
            raw_posting_cap,
        )
    }

    fn ready_equality_candidate_ids_raw_limited(
        &self,
        index_id: u64,
        prop_value: &PropValue,
        raw_posting_cap: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let mut raw_posting_budget = EqualityRawPostingBudget::new(raw_posting_cap);
        let mut accepted = NodeIdSet::default();

        let raw_limit = match raw_posting_budget.next_chunk_limit() {
            Some(limit) if limit > 0 => limit,
            _ => return Ok((None, None)),
        };
        let ids = memtable_secondary_eq_raw_nodes_for_filter(
            &self.memtable,
            index_id,
            prop_value,
            self.snapshot_seq,
            Some(raw_limit),
        );
        if raw_posting_budget.consume(ids.len()) {
            return Ok((None, None));
        }
        accepted.extend(ids);

        let mut deleted_above = self.memtable.collect_deleted_nodes_at(self.snapshot_seq);
        for epoch in &self.immutable_epochs {
            let raw_limit = match raw_posting_budget.next_chunk_limit() {
                Some(limit) if limit > 0 => limit,
                _ => return Ok((None, None)),
            };
            let ids = memtable_secondary_eq_raw_nodes_for_filter(
                &epoch.memtable,
                index_id,
                prop_value,
                self.snapshot_seq,
                Some(raw_limit),
            );
            if raw_posting_budget.consume(ids.len()) {
                return Ok((None, None));
            }
            accepted.extend(ids.into_iter().filter(|id| !deleted_above.contains(id)));
            deleted_above.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        }

        for segment in &self.segments {
            for value_hash in equality_probe_value_hashes(prop_value) {
                match self.extend_raw_equality_candidates_from_segment_postings(
                    segment,
                    index_id,
                    value_hash,
                    &mut raw_posting_budget,
                    &deleted_above,
                    &mut accepted,
                ) {
                    Ok(Some(EqualitySegmentPostingScan::RawCapExceeded)) => {
                        return Ok((None, None));
                    }
                    Ok(Some(EqualitySegmentPostingScan::Exhausted)) => {}
                    #[cfg(test)]
                    Ok(Some(EqualitySegmentPostingScan::AcceptedCapReached)) => {}
                    Ok(None) => {
                        return Ok((
                            None,
                            self.equality_sidecar_failure_followup(index_id, None),
                        ));
                    }
                    Err(error) => {
                        return Ok((
                            None,
                            self.equality_sidecar_failure_followup(index_id, Some(error)),
                        ));
                    }
                }
            }
            deleted_above.extend(segment.deleted_node_ids());
        }

        let mut ids: Vec<u64> = accepted.into_iter().collect();
        ids.sort_unstable();
        Ok((Some(ids), None))
    }

    #[cfg(test)]
    fn ready_equality_candidate_ids_limited_internal(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        max_ids: Option<usize>,
        raw_posting_cap: Option<usize>,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let Some(max_ids) = max_ids else {
            let (sources, followup) =
                self.ready_equality_source_ids(index_id, prop_key, prop_value)?;
            let Some((memtable_ids, segment_ids, _memtable_verified)) = sources else {
                return Ok((None, followup));
            };

            let deleted = NodeIdSet::default();
            let page = PageRequest::default();
            let merged = merge_record_ids_paged(memtable_ids, segment_ids, &deleted, &page);
            return Ok((Some(merged.items), followup));
        };

        let mut raw_posting_budget = raw_posting_cap.map(EqualityRawPostingBudget::new);
        let mut accepted = NodeIdSet::default();
        for node_id in memtable_secondary_eq_nodes_for_filter(
            &self.memtable,
            index_id,
            prop_key,
            prop_value,
            self.snapshot_seq,
        ) {
            accepted.insert(node_id);
            if accepted.len() >= max_ids {
                break;
            }
        }
        if accepted.len() >= max_ids {
            let mut ids: Vec<u64> = accepted.into_iter().collect();
            ids.sort_unstable();
            return Ok((Some(ids), None));
        }

        for epoch in &self.immutable_epochs {
            let ids = memtable_secondary_eq_nodes_for_filter(
                &epoch.memtable,
                index_id,
                prop_key,
                prop_value,
                self.snapshot_seq,
            );
            if self.extend_verified_equality_candidates(
                ids,
                prop_key,
                prop_value,
                max_ids,
                &mut accepted,
            )? {
                let mut ids: Vec<u64> = accepted.into_iter().collect();
                ids.sort_unstable();
                return Ok((Some(ids), None));
            }
        }

        for seg in &self.segments {
            for value_hash in equality_probe_value_hashes(prop_value) {
                match self.extend_verified_equality_candidates_from_segment_postings(
                    seg,
                    index_id,
                    value_hash,
                    prop_key,
                    prop_value,
                    max_ids,
                    raw_posting_budget.as_mut(),
                    &mut accepted,
                ) {
                    Ok(Some(EqualitySegmentPostingScan::AcceptedCapReached)) => {
                        let mut ids: Vec<u64> = accepted.into_iter().collect();
                        ids.sort_unstable();
                        return Ok((Some(ids), None));
                    }
                    Ok(Some(EqualitySegmentPostingScan::RawCapExceeded)) => {
                        return Ok((None, None));
                    }
                    Ok(Some(EqualitySegmentPostingScan::Exhausted)) => {}
                    Ok(None) => {
                        return Ok((None, self.equality_sidecar_failure_followup(index_id, None)));
                    }
                    Err(error) => {
                        return Ok((
                            None,
                            self.equality_sidecar_failure_followup(index_id, Some(error)),
                        ));
                    }
                }
            }
        }

        let mut ids: Vec<u64> = accepted.into_iter().collect();
        ids.sort_unstable();
        Ok((Some(ids), None))
    }

    fn ready_equality_candidate_ids_from_postings(
        &self,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        limit: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let (sources, followup) =
            self.ready_equality_source_ids(index_id, prop_key, prop_value)?;
        let Some((memtable_ids, segment_ids, _memtable_verified)) = sources else {
            return Ok((None, followup));
        };

        let deleted = NodeIdSet::default();
        let page = PageRequest {
            limit: Some(limit),
            after: None,
        };
        let merged = merge_record_ids_paged(memtable_ids, segment_ids, &deleted, &page);
        Ok((Some(merged.items), followup))
    }

    #[cfg(test)]
    fn extend_verified_equality_candidates(
        &self,
        candidate_ids: Vec<u64>,
        prop_key: &str,
        prop_value: &PropValue,
        max_ids: usize,
        accepted: &mut NodeIdSet,
    ) -> Result<bool, EngineError> {
        if candidate_ids.is_empty() {
            return Ok(false);
        }

        let mut ids: Vec<u64> = candidate_ids
            .into_iter()
            .filter(|id| !accepted.contains(id))
            .collect();
        if ids.is_empty() {
            return Ok(false);
        }
        ids.sort_unstable();
        ids.dedup();

        #[cfg(test)]
        self.note_equality_materialization_record_reads(ids.len());
        let nodes = self.get_nodes_raw(&ids)?;
        for (&node_id, node) in ids.iter().zip(nodes.iter()) {
            let Some(node) = node.as_ref() else {
                continue;
            };
            if node
                .props
                .get(prop_key)
                .is_some_and(|candidate| prop_values_equal_for_filter(candidate, prop_value))
            {
                accepted.insert(node_id);
                if accepted.len() >= max_ids {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn range_sidecar_failure_followup(
        &self,
        index_id: u64,
        error: Option<EngineError>,
    ) -> Option<SecondaryIndexReadFollowup> {
        let next_state = if error
            .as_ref()
            .is_some_and(|error| !is_not_found_io_error(error))
        {
            SecondaryIndexState::Failed
        } else {
            SecondaryIndexState::Building
        };
        let next_last_error = if next_state == SecondaryIndexState::Failed {
            error.as_ref().map(ToString::to_string)
        } else {
            None
        };

        let current_entry = self
            .secondary_index_entries
            .iter()
            .find(|entry| entry.index_id == index_id)?;
        if !matches!(current_entry.kind, SecondaryIndexKind::Range) {
            return None;
        }
        if current_entry.state == next_state && current_entry.last_error == next_last_error {
            return None;
        }

        Some(SecondaryIndexReadFollowup::RangeSidecarFailure { index_id, error })
    }

    fn nodes_by_single_label_id_paged_unfiltered(
        &self,
        single_label_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let limit = page.limit.unwrap_or(0);
        let target = page_verify_target(limit);
        let chunk_limit = match page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut collected = Vec::with_capacity(if limit > 0 { limit } else { 0 });

        self.scan_raw_node_label_candidates(&[single_label_id], page.after, chunk_limit, |chunk| {
            #[cfg(test)]
            self.note_node_visibility_meta_reads(chunk.len());
            let visibility = self.sources().find_node_visibility_meta(chunk)?;
            for (&node_id, state) in chunk.iter().zip(visibility.iter()) {
                if matches!(
                    state,
                    NodeVisibilityState::Live(meta) if meta.label_ids.contains(single_label_id)
                ) {
                    collected.push(node_id);
                    if collected.len() >= target {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;

        let next_cursor = if limit > 0 && collected.len() > limit {
            collected.truncate(limit);
            collected.last().copied()
        } else {
            None
        };
        Ok(PageResult {
            items: collected,
            next_cursor,
        })
    }

    fn single_label_node_sources(
        &self,
        single_label_id: u32,
    ) -> Result<Vec<NodeLabelScanSource<'_>>, EngineError> {
        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(NodeLabelScanSource::Memtable {
            memtable: self.memtable.as_ref(),
            label_id: single_label_id,
            snapshot_seq: self.snapshot_seq,
        });
        for epoch in &self.immutable_epochs {
            sources.push(NodeLabelScanSource::Memtable {
                memtable: epoch.memtable.as_ref(),
                label_id: single_label_id,
                snapshot_seq: self.snapshot_seq,
            });
        }
        for segment in &self.segments {
            if let Some(posting) = segment.node_label_posting(single_label_id)? {
                sources.push(NodeLabelScanSource::Segment {
                    segment: segment.as_ref(),
                    posting,
                });
            }
        }
        Ok(sources)
    }

    fn scan_raw_node_label_candidates<F>(
        &self,
        label_ids: &[u32],
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let chunk_limit = chunk_limit.max(1);
        let mut sources = Vec::new();
        for &label_id in label_ids {
            sources.extend(self.single_label_node_sources(label_id)?);
        }

        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter().enumerate() {
            if let Some(node_id) = source.next_after(start_after)? {
                heap.push(Reverse((node_id, source_index)));
            }
        }

        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((node_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_after(Some(node_id))? {
                heap.push(Reverse((next_id, source_index)));
            }

            if last_seen == Some(node_id) {
                continue;
            }
            last_seen = Some(node_id);
            chunk.push(node_id);
            if chunk.len() >= chunk_limit {
                if visitor(&chunk)?.is_break() {
                    return Ok(());
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let _ = visitor(&chunk)?;
        }
        Ok(())
    }

    fn single_label_edge_sources(
        &self,
        label_id: u32,
    ) -> Result<Vec<EdgeLabelScanSource<'_>>, EngineError> {
        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(EdgeLabelScanSource::Memtable {
            memtable: self.memtable.as_ref(),
            label_id,
            snapshot_seq: self.snapshot_seq,
        });
        for epoch in &self.immutable_epochs {
            sources.push(EdgeLabelScanSource::Memtable {
                memtable: epoch.memtable.as_ref(),
                label_id,
                snapshot_seq: self.snapshot_seq,
            });
        }
        for segment in &self.segments {
            if let Some(posting) = segment.edge_label_posting(label_id)? {
                sources.push(EdgeLabelScanSource::Segment {
                    segment: segment.as_ref(),
                    posting,
                });
            }
        }
        Ok(sources)
    }

    fn scan_raw_edge_label_candidates<F>(
        &self,
        label_id: u32,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let chunk_limit = chunk_limit.max(1);
        let sources = self.single_label_edge_sources(label_id)?;

        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter().enumerate() {
            if let Some(edge_id) = source.next_after(start_after)? {
                heap.push(Reverse((edge_id, source_index)));
            }
        }

        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((edge_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_after(Some(edge_id))? {
                heap.push(Reverse((next_id, source_index)));
            }

            if last_seen == Some(edge_id) {
                continue;
            }
            last_seen = Some(edge_id);
            chunk.push(edge_id);
            if chunk.len() >= chunk_limit {
                if visitor(&chunk)?.is_break() {
                    return Ok(());
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let _ = visitor(&chunk)?;
        }
        Ok(())
    }

    fn scan_nodes_by_single_label_id_filtered<F>(
        &self,
        single_label_id: u32,
        start_after: Option<u64>,
        chunk_limit: usize,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(u64, &NodeRecord) -> Result<ControlFlow<()>, EngineError>,
    {
        let chunk_limit = chunk_limit.max(1);
        let sources = self.single_label_node_sources(single_label_id)?;
        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter().enumerate() {
            if let Some(node_id) = source.next_after(start_after)? {
                heap.push(Reverse((node_id, source_index)));
            }
        }

        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((node_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_after(Some(node_id))? {
                heap.push(Reverse((next_id, source_index)));
            }

            if last_seen == Some(node_id) {
                continue;
            }
            last_seen = Some(node_id);
            chunk.push(node_id);
            if chunk.len() >= chunk_limit {
                if self
                    .visit_single_label_scan_chunk(
                        single_label_id,
                        &chunk,
                        policy_cutoffs,
                        &mut visitor,
                    )?
                    .is_break()
                {
                    return Ok(());
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let _ = self.visit_single_label_scan_chunk(
                single_label_id,
                &chunk,
                policy_cutoffs,
                &mut visitor,
            )?;
        }
        Ok(())
    }

    fn visit_single_label_scan_chunk<F>(
        &self,
        single_label_id: u32,
        chunk: &[u64],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        visitor: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, &NodeRecord) -> Result<ControlFlow<()>, EngineError>,
    {
        let nodes = self.get_nodes_raw(chunk)?;
        for (&node_id, node) in chunk.iter().zip(nodes.iter()) {
            let Some(node) = node.as_ref() else {
                continue;
            };
            if !node.label_ids.contains(single_label_id) {
                continue;
            }
            if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                continue;
            }
            if visitor(node_id, node)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn validate_property_range_bounds(
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        cursor: Option<&PropertyRangeCursor>,
    ) -> Result<ValidatedNumericRange, EngineError> {
        validate_numeric_range_bounds(lower, upper, cursor)
    }

    fn find_nodes_scan_fallback(
        &self,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<Vec<u64>, EngineError> {
        let policy_cutoffs = self.query_policy_cutoffs();
        let mut results = Vec::new();
        self.scan_nodes_by_single_label_id_filtered(
            label_id,
            None,
            256,
            policy_cutoffs.as_ref(),
            |node_id, node| {
                if node
                    .props
                    .get(prop_key)
                    .is_some_and(|value| prop_values_equal_for_filter(value, prop_value))
                {
                    results.push(node_id);
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(results)
    }

    fn find_nodes_paged_scan_fallback(
        &self,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let limit = page.limit.unwrap_or(0);
        let chunk_limit = match page.limit {
            Some(limit) if limit > 0 => limit.saturating_mul(4).max(limit),
            _ => 256,
        };
        let policy_cutoffs = self.query_policy_cutoffs();

        if limit == 0 {
            let mut items = Vec::new();
            self.scan_nodes_by_single_label_id_filtered(
                label_id,
                page.after,
                chunk_limit,
                policy_cutoffs.as_ref(),
                |node_id, node| {
                    if node
                        .props
                        .get(prop_key)
                        .is_some_and(|value| prop_values_equal_for_filter(value, prop_value))
                    {
                        items.push(node_id);
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            return Ok(PageResult {
                items,
                next_cursor: None,
            });
        }

        let target = page_verify_target(limit);
        let mut items = Vec::with_capacity(limit);
        self.scan_nodes_by_single_label_id_filtered(
            label_id,
            page.after,
            chunk_limit,
            policy_cutoffs.as_ref(),
            |node_id, node| {
                if node
                    .props
                    .get(prop_key)
                    .is_some_and(|value| prop_values_equal_for_filter(value, prop_value))
                {
                    items.push(node_id);
                    if items.len() >= target {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(finish_verified_id_page(items, limit))
    }

    fn find_nodes_ready_equality_index(
        &self,
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let (page, followup) = self.find_nodes_paged_ready_equality_index(
            label_id,
            index_id,
            prop_key,
            prop_value,
            &PageRequest::default(),
        )?;
        Ok((page.map(|page| page.items), followup))
    }

    fn find_nodes_paged_ready_equality_index(
        &self,
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<
        (
            Option<PageResult<u64>>,
            Option<SecondaryIndexReadFollowup>,
        ),
        EngineError,
    > {
        let (sources, followup) = self.ready_equality_source_ids(index_id, prop_key, prop_value)?;
        let Some((memtable_ids, segment_ids, memtable_verified)) = sources else {
            return Ok((None, followup));
        };

        let deleted = NodeIdSet::default();
        let limit = page.limit.unwrap_or(0);
        if limit == 0 {
            let all_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let merged = merge_record_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
            let visible = self.ready_equality_verified_node_ids(
                &merged.items,
                &memtable_verified,
                label_id,
                prop_key,
                prop_value,
            )?;
            let excluded = self.policy_excluded_node_ids(&merged.items)?;
            let items: Vec<u64> = merged
                .items
                .into_iter()
                .filter(|id| visible.contains(id) && !excluded.contains(id))
                .collect();
            Ok((
                Some(PageResult {
                    items,
                    next_cursor: None,
                }),
                followup,
            ))
        } else {
            let target = page_verify_target(limit);
            let chunk_limit = limit
                .saturating_add(1)
                .saturating_mul(4)
                .max(limit.saturating_add(1));
            let mut collected = Vec::with_capacity(limit);
            let mut cursor = page.after;

            loop {
                let chunk_page = PageRequest {
                    limit: Some(chunk_limit),
                    after: cursor,
                };
                let merged = merge_record_ids_paged(
                    memtable_ids.clone(),
                    segment_ids.clone(),
                    &deleted,
                    &chunk_page,
                );
                if merged.items.is_empty() {
                    return Ok((
                        Some(PageResult {
                            items: collected,
                            next_cursor: None,
                        }),
                        followup,
                    ));
                }

                let visible = self.ready_equality_verified_node_ids(
                    &merged.items,
                    &memtable_verified,
                    label_id,
                    prop_key,
                    prop_value,
                )?;
                let excluded = self.policy_excluded_node_ids(&merged.items)?;
                for id in merged.items {
                    if visible.contains(&id) && !excluded.contains(&id) {
                        collected.push(id);
                        if collected.len() >= target {
                            let page = finish_verified_id_page(collected, limit);
                            return Ok((
                                Some(page),
                                followup,
                            ));
                        }
                    }
                    cursor = Some(id);
                }

                if merged.next_cursor.is_none() {
                    return Ok((
                        Some(PageResult {
                            items: collected,
                            next_cursor: None,
                        }),
                        followup,
                    ));
                }
            }
        }
    }

    fn ready_range_node_value(
        node: Option<&NodeRecord>,
        label_id: u32,
        prop_key: &str,
        candidate_key: NumericRangeSortKey,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Option<PropValue> {
        let node = node?;
        if !node.label_ids.contains(label_id)
            || policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node))
        {
            return None;
        }

        let value = node.props.get(prop_key)?;
        if numeric_range_sort_key_for_value(value) != Some(candidate_key) {
            return None;
        }
        if range_value_within_bounds(value, lower, upper) != Some(true) {
            return None;
        }

        Some(value.clone())
    }

    fn encode_property_range_bound(
        bound: Option<&PropertyRangeBound>,
    ) -> Option<(NumericRangeSortKey, bool)> {
        bound.map(|bound| {
            (
                numeric_range_sort_key_for_value(bound.value())
                    .expect("validated property range bounds must encode"),
                bound.is_inclusive(),
            )
        })
    }

    fn encode_property_range_cursor(
        cursor: Option<&PropertyRangeCursor>,
    ) -> Option<(NumericRangeSortKey, u64)> {
        cursor.map(|cursor| {
            (
                numeric_range_sort_key_for_value(&cursor.value)
                    .expect("validated property range cursor must encode"),
                cursor.node_id,
            )
        })
    }

    fn ready_range_chunk_limit(limit: usize) -> usize {
        if limit == 0 {
            512
        } else {
            limit.saturating_mul(4).max(limit).max(64)
        }
    }

    fn ready_range_memtable_source<'a>(
        memtable: &'a Memtable,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        snapshot_seq: u64,
        chunk_size: usize,
    ) -> ReadyRangeSource<'a> {
        ReadyRangeSource {
            kind: ReadyRangeSourceKind::Memtable {
                memtable,
                index_id,
                lower,
                upper,
                after,
                snapshot_seq,
                buffer: Vec::new(),
                offset: 0,
                chunk_size: chunk_size.max(1),
                exhausted: false,
            },
        }
    }

    fn ready_range_segment_source<'a>(
        segment: &'a SegmentReader,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        after: Option<(NumericRangeSortKey, u64)>,
        chunk_size: usize,
    ) -> ReadyRangeSource<'a> {
        ReadyRangeSource {
            kind: ReadyRangeSourceKind::Segment {
                segment,
                index_id,
                lower,
                upper,
                after,
                buffer: Vec::new(),
                offset: 0,
                chunk_size: chunk_size.max(1),
            },
        }
    }

    fn ready_range_candidate_ids(
        &self,
        index_id: u64,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        max_ids: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let lower_key = Self::encode_property_range_bound(lower);
        let upper_key = Self::encode_property_range_bound(upper);
        let mut ids = Vec::with_capacity(max_ids.min(4096));
        let mut seen = NodeIdSet::default();

        for seg in &self.segments {
            match seg.find_nodes_by_secondary_range_index_if_present_limited(
                index_id,
                lower_key,
                upper_key,
                None,
                Some(1),
            ) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Ok((None, self.range_sidecar_failure_followup(index_id, None)));
                }
                Err(error) => {
                    return Ok((
                        None,
                        self.range_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            }
        }

        let flow = self.memtable.for_each_visible_secondary_range_entry_at(
            index_id,
            lower_key,
            upper_key,
            None,
            self.snapshot_seq,
            &mut |(_, node_id)| {
                push_unique_candidate_id_limited(&mut ids, &mut seen, node_id, max_ids)
            },
        );
        if flow.is_break() {
            ids.sort_unstable();
            return Ok((Some(ids), None));
        }

        for epoch in &self.immutable_epochs {
            let flow = epoch.memtable.for_each_visible_secondary_range_entry_at(
                index_id,
                lower_key,
                upper_key,
                None,
                self.snapshot_seq,
                &mut |(_, node_id)| {
                    push_unique_candidate_id_limited(&mut ids, &mut seen, node_id, max_ids)
                },
            );
            if flow.is_break() {
                ids.sort_unstable();
                return Ok((Some(ids), None));
            }
        }

        let chunk_size = max_ids.clamp(1, 512);
        for seg in &self.segments {
            let mut source = Self::ready_range_segment_source(
                seg,
                index_id,
                lower_key,
                upper_key,
                None,
                chunk_size,
            );
            loop {
                match source.next_entry() {
                    Ok(ReadyRangeSourceStep::Entry((_, node_id))) => {
                        if push_unique_candidate_id_limited(
                            &mut ids,
                            &mut seen,
                            node_id,
                            max_ids,
                        )
                        .is_break()
                        {
                            ids.sort_unstable();
                            return Ok((Some(ids), None));
                        }
                    }
                    Ok(ReadyRangeSourceStep::Exhausted) => break,
                    Ok(ReadyRangeSourceStep::MissingSidecar) => {
                        return Ok((None, self.range_sidecar_failure_followup(index_id, None)));
                    }
                    Err(error) => {
                        return Ok((
                            None,
                            self.range_sidecar_failure_followup(index_id, Some(error)),
                        ));
                    }
                }
            }
        }

        ids.sort_unstable();
        Ok((Some(ids), None))
    }

    #[allow(clippy::too_many_arguments)]
    fn find_nodes_paged_ready_range_index(
        &self,
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        page: &PropertyRangePageRequest,
    ) -> Result<
        (
            Option<PropertyRangePageResult<u64>>,
            Option<SecondaryIndexReadFollowup>,
        ),
        EngineError,
    > {
        let lower_key = Self::encode_property_range_bound(lower);
        let upper_key = Self::encode_property_range_bound(upper);
        let after_encoded = Self::encode_property_range_cursor(page.after.as_ref());
        let policy_cutoffs = self.query_policy_cutoffs();
        let limit = page.limit.unwrap_or(0);
        let chunk_limit = Self::ready_range_chunk_limit(limit);
        let target_visible = if limit == 0 {
            usize::MAX
        } else {
            limit.saturating_add(1)
        };

        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(Self::ready_range_memtable_source(
            &self.memtable,
            index_id,
            lower_key,
            upper_key,
            after_encoded,
            self.snapshot_seq,
            chunk_limit,
        ));
        for epoch in &self.immutable_epochs {
            sources.push(Self::ready_range_memtable_source(
                epoch.memtable.as_ref(),
                index_id,
                lower_key,
                upper_key,
                after_encoded,
                self.snapshot_seq,
                chunk_limit,
            ));
        }
        for seg in &self.segments {
            sources.push(Self::ready_range_segment_source(
                seg,
                index_id,
                lower_key,
                upper_key,
                after_encoded,
                chunk_limit,
            ));
        }

        let mut heap: BinaryHeap<Reverse<((NumericRangeSortKey, u64), usize)>> =
            BinaryHeap::with_capacity(sources.len());
        for (source_idx, source) in sources.iter_mut().enumerate() {
            match source.next_entry() {
                Ok(ReadyRangeSourceStep::Entry(entry)) => {
                    heap.push(Reverse((entry, source_idx)));
                }
                Ok(ReadyRangeSourceStep::Exhausted) => {}
                Ok(ReadyRangeSourceStep::MissingSidecar) => {
                    return Ok((None, self.range_sidecar_failure_followup(index_id, None)));
                }
                Err(error) => {
                    return Ok((
                        None,
                        self.range_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            }
        }

        let mut visible = Vec::new();
        let mut seen = NodeIdSet::default();
        let mut pending = Vec::with_capacity(chunk_limit);
        let hydrate_pending = |pending: &mut Vec<(NumericRangeSortKey, u64)>,
                               visible: &mut Vec<(u64, PropertyRangeCursor)>,
                               seen: &mut NodeIdSet|
         -> Result<(), EngineError> {
            if pending.is_empty() {
                return Ok(());
            }

            let node_ids: Vec<u64> = pending.iter().map(|&(_, node_id)| node_id).collect();
            let hydrated = self.get_nodes_raw(&node_ids)?;
            for ((candidate_key, node_id), node) in pending.iter().zip(hydrated) {
                if visible.len() >= target_visible {
                    break;
                }
                let Some(value) = Self::ready_range_node_value(
                    node.as_ref(),
                    label_id,
                    prop_key,
                    *candidate_key,
                    lower,
                    upper,
                    policy_cutoffs.as_ref(),
                ) else {
                    continue;
                };
                if !seen.insert(*node_id) {
                    continue;
                }
                visible.push((
                    *node_id,
                    PropertyRangeCursor {
                        value,
                        node_id: *node_id,
                    },
                ));
            }
            pending.clear();
            Ok(())
        };

        while visible.len() < target_visible {
            let Some(Reverse((entry, source_idx))) = heap.pop() else {
                break;
            };

            match sources[source_idx].next_entry() {
                Ok(ReadyRangeSourceStep::Entry(next_entry)) => {
                    heap.push(Reverse((next_entry, source_idx)));
                }
                Ok(ReadyRangeSourceStep::Exhausted) => {}
                Ok(ReadyRangeSourceStep::MissingSidecar) => {
                    return Ok((None, self.range_sidecar_failure_followup(index_id, None)));
                }
                Err(error) => {
                    return Ok((
                        None,
                        self.range_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            }

            if seen.contains(&entry.1) {
                continue;
            }

            pending.push(entry);
            if pending.len() >= chunk_limit {
                hydrate_pending(&mut pending, &mut visible, &mut seen)?;
            }
        }
        if visible.len() < target_visible {
            hydrate_pending(&mut pending, &mut visible, &mut seen)?;
        }

        if limit == 0 {
            return Ok((
                Some(PropertyRangePageResult {
                    items: visible.into_iter().map(|(node_id, _)| node_id).collect(),
                    next_cursor: None,
                }),
                None,
            ));
        }

        let has_more = visible.len() > limit;
        let selected = &visible[..visible.len().min(limit)];
        Ok((
            Some(PropertyRangePageResult {
                items: selected.iter().map(|(node_id, _)| *node_id).collect(),
                next_cursor: if has_more {
                    selected.last().map(|(_, cursor)| cursor.clone())
                } else {
                    None
                },
            }),
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_property_range_scan_matches(
        &self,
        label_id: u32,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        after: Option<&PropertyRangeCursor>,
        max_results: Option<usize>,
    ) -> Result<Vec<RangeScanMatch>, EngineError> {
        let policy_cutoffs = self.query_policy_cutoffs();
        let mut matches = Vec::new();
        let mut bounded: Option<BinaryHeap<RangeScanMatch>> =
            max_results.map(|_| BinaryHeap::new());
        self.scan_nodes_by_single_label_id_filtered(
            label_id,
            None,
            256,
            policy_cutoffs.as_ref(),
            |node_id, node| {
                let Some(value) = node.props.get(prop_key) else {
                    return Ok(ControlFlow::Continue(()));
                };
                if range_value_within_bounds(value, lower, upper) != Some(true) {
                    return Ok(ControlFlow::Continue(()));
                }
                if after.is_some_and(|cursor| {
                    compare_range_cursor_to_candidate(cursor, value, node_id)
                        != Some(std::cmp::Ordering::Less)
                }) {
                    return Ok(ControlFlow::Continue(()));
                }
                let Some(encoded_value) = numeric_range_sort_key_for_value(value) else {
                    return Ok(ControlFlow::Continue(()));
                };
                let entry = RangeScanMatch {
                    encoded_value,
                    value: value.clone(),
                    node_id,
                };
                if let Some(heap) = bounded.as_mut() {
                    heap.push(entry);
                    if heap.len() > max_results.unwrap_or(usize::MAX) {
                        heap.pop();
                    }
                } else {
                    matches.push(entry);
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if let Some(heap) = bounded {
            matches = heap.into_vec();
        }
        matches.sort_unstable();
        Ok(matches)
    }

    fn score_dense_candidate_ids(
        &self,
        candidate_ids: &NodeIdSet,
        query: &[f32],
        metric: DenseMetric,
        label_filter: &ResolvedNodeLabelFilter,
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
        self.memtable.visit_nodes_sorted_at(
            &ids,
            self.snapshot_seq,
            &mut remaining,
            &mut |node_id, node| {
                if !node_label_filter_matches(label_filter, &node.label_ids) {
                    return;
                }
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    return;
                }
                let Some(dense_vector) = node.dense_vector.as_ref() else {
                    return;
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
            },
        );

        // Check immutable memtables (newest-first), reusing a single buffer.
        let mut next_remaining_imm = Vec::with_capacity(remaining.len());
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            next_remaining_imm.clear();
            epoch.memtable.visit_nodes_sorted_at(
                &remaining,
                self.snapshot_seq,
                &mut next_remaining_imm,
                &mut |node_id, node| {
                    if !node_label_filter_matches(label_filter, &node.label_ids) {
                        return;
                    }
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        return;
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
                },
            );
            std::mem::swap(&mut remaining, &mut next_remaining_imm);
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
                |label_ids, updated_at, weight| {
                    node_label_filter_matches(label_filter, &label_ids)
                        && policy_cutoffs.is_none_or(|cutoffs| {
                            !cutoffs.excludes_fields(&label_ids, updated_at, weight)
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

    fn hidden_node_ids_before_segment(
        &self,
        segment_index: usize,
        initial_hidden_ids: &NodeIdSet,
    ) -> Result<NodeIdSet, EngineError> {
        let mut hidden_ids = initial_hidden_ids.clone();
        for segment in self.segments.iter().take(segment_index) {
            hidden_ids.extend(segment.deleted_node_id_iter());
            hidden_ids.extend(segment.node_ids()?.iter().copied());
        }
        Ok(hidden_ids)
    }

    #[allow(clippy::too_many_arguments)]
    fn score_exact_dense_segment(
        &self,
        segment_index: usize,
        query: &[f32],
        metric: DenseMetric,
        query_norm: Option<f32>,
        scope_ids: Option<&NodeIdSet>,
        initial_hidden_ids: &NodeIdSet,
        label_filter: &ResolvedNodeLabelFilter,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        candidate_ids: &mut NodeIdSet,
        hits: &mut Vec<VectorHit>,
    ) -> Result<(), EngineError> {
        let segment = &self.segments[segment_index];
        if segment.dense_vector_count() == 0 {
            return Ok(());
        }

        let hidden_ids = self.hidden_node_ids_before_segment(segment_index, initial_hidden_ids)?;
        let mut segment_hits = Vec::new();
        segment.exact_dense_vector_search(
            query,
            metric,
            query_norm,
            scope_ids,
            &hidden_ids,
            |label_ids, updated_at, weight| {
                node_label_filter_matches(label_filter, &label_ids)
                    && policy_cutoffs.is_none_or(|cutoffs| {
                        !cutoffs.excludes_fields(&label_ids, updated_at, weight)
                    })
            },
            &mut segment_hits,
        )?;
        for hit in segment_hits {
            if candidate_ids.insert(hit.node_id) {
                hits.push(hit);
            }
        }
        Ok(())
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

    /// Reduce a single segment's sparse scores into the top-k heap, maintaining
    /// newest-first visibility via hidden_ids. Shared by both serial and parallel paths.
    ///
    /// `candidates`, `meta_results`, and `remaining` are caller-owned reusable buffers
    /// that avoid per-segment allocation when called in a loop.
    #[allow(clippy::too_many_arguments)]
    fn sparse_reduce_segment_scores(
        segment: &SegmentReader,
        scores: NodeIdMap<f32>,
        hidden_ids: &mut NodeIdSet,
        scope_ids: Option<&NodeIdSet>,
        label_filter: &ResolvedNodeLabelFilter,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        top_hits: &mut BinaryHeap<SparseTopKEntry>,
        k: usize,
        has_older_segments: bool,
        candidates: &mut Vec<(u64, f32)>,
        meta_results: &mut Vec<Option<(NodeLabelSet, i64, f32)>>,
        remaining: &mut Vec<(usize, u64)>,
    ) -> Result<(), EngineError> {
        // Early exit: no scores at all from this segment.
        if scores.is_empty() {
            if has_older_segments {
                hidden_ids.extend(segment.deleted_node_id_iter());
                hidden_ids.extend(segment.node_ids()?.iter().copied());
            }
            return Ok(());
        }

        candidates.clear();
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
                hidden_ids.extend(segment.deleted_node_id_iter());
                hidden_ids.extend(segment.node_ids()?.iter().copied());
            }
            return Ok(());
        }

        candidates.sort_unstable_by_key(|&(node_id, _)| node_id);
        meta_results.clear();
        meta_results.resize(candidates.len(), None);
        remaining.clear();
        remaining.extend(
            candidates
                .iter()
                .enumerate()
                .map(|(index, &(node_id, _))| (index, node_id)),
        );
        // Redundant hidden_ids check removed: candidates were already filtered
        // by !hidden_ids.contains() above, and hidden_ids is not mutated between.
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
                hidden_ids.extend(segment.deleted_node_id_iter());
                hidden_ids.extend(segment.node_ids()?.iter().copied());
            }
            return Ok(());
        }

        segment.get_node_meta_batch(remaining, meta_results)?;
        for (index, &(node_id, score)) in candidates.iter().enumerate() {
            let Some((label_ids, updated_at, weight)) = meta_results[index] else {
                continue;
            };
            if !node_label_filter_matches(label_filter, &label_ids) {
                continue;
            }
            if policy_cutoffs
                .is_some_and(|cutoffs| cutoffs.excludes_fields(&label_ids, updated_at, weight))
            {
                continue;
            }
            Self::push_sparse_top_k(top_hits, k, node_id, score);
        }
        if has_older_segments {
            hidden_ids.extend(segment.deleted_node_id_iter());
            hidden_ids.extend(segment.node_ids()?.iter().copied());
        }
        Ok(())
    }

    fn hide_segment_nodes_for_older_segments(
        segment: &SegmentReader,
        hidden_ids: &mut NodeIdSet,
    ) -> Result<(), EngineError> {
        hidden_ids.extend(segment.deleted_node_id_iter());
        hidden_ids.extend(segment.node_ids()?.iter().copied());
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn sparse_reduce_exact_segment_scores(
        segment: &SegmentReader,
        query: &[(u32, f32)],
        hidden_ids: &mut NodeIdSet,
        scope_ids: Option<&NodeIdSet>,
        label_filter: &ResolvedNodeLabelFilter,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        top_hits: &mut BinaryHeap<SparseTopKEntry>,
        k: usize,
        has_older_segments: bool,
        exact_hits: &mut Vec<(u64, f32)>,
    ) -> Result<(), EngineError> {
        exact_hits.clear();
        segment.exact_sparse_vector_scores(
            query,
            scope_ids,
            hidden_ids,
            |label_ids, updated_at, weight| {
                node_label_filter_matches(label_filter, &label_ids)
                    && policy_cutoffs.is_none_or(|cutoffs| {
                        !cutoffs.excludes_fields(&label_ids, updated_at, weight)
                    })
            },
            exact_hits,
        )?;
        for &(node_id, score) in exact_hits.iter() {
            Self::push_sparse_top_k(top_hits, k, node_id, score);
        }
        if has_older_segments {
            Self::hide_segment_nodes_for_older_segments(segment, hidden_ids)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Dense tail scan needs all context inline for hot-path efficiency
    fn exact_dense_tail_candidates(
        &self,
        segments: &[(usize, &SegmentReader, usize)],
        exhausted: &[bool],
        query: &[f32],
        metric: DenseMetric,
        cutoff: &VectorHit,
        candidate_ids: &mut NodeIdSet,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<NodeIdSet, EngineError> {
        let work_items: Vec<&SegmentReader> = segments
            .iter()
            .zip(exhausted.iter())
            .filter(|(_, ex)| !**ex)
            .map(|((_, seg, _), _)| *seg)
            .collect();

        if work_items.is_empty() {
            return Ok(NodeIdSet::with_capacity_and_hasher(
                0,
                NodeIdBuildHasher::default(),
            ));
        }

        let cutoff_score = cutoff.score;
        let cutoff_node_id = cutoff.node_id;

        let per_segment_hits: Vec<Result<Vec<(u64, f32)>, EngineError>> = if work_items.len() <= 1 {
            // Single non-exhausted segment: skip rayon overhead.
            work_items
                .iter()
                .map(|segment| {
                    let mut hits = exact_dense_search_above_cutoff(
                        segment.raw_dense_hnsw_meta_mmap(),
                        segment.raw_node_dense_vectors_mmap(),
                        query,
                        metric,
                        cutoff_score,
                        cutoff_node_id,
                    )?;
                    if let Some(scope) = scope_ids {
                        hits.retain(|(node_id, _)| scope.contains(node_id));
                    }
                    Ok(hits)
                })
                .collect()
        } else {
            crate::parallel::engine_cpu_install(|| {
                use rayon::prelude::*;
                work_items
                    .par_iter()
                    .map(|segment| {
                        let mut hits = exact_dense_search_above_cutoff(
                            segment.raw_dense_hnsw_meta_mmap(),
                            segment.raw_node_dense_vectors_mmap(),
                            query,
                            metric,
                            cutoff_score,
                            cutoff_node_id,
                        )?;
                        if let Some(scope) = scope_ids {
                            hits.retain(|(node_id, _)| scope.contains(node_id));
                        }
                        Ok(hits)
                    })
                    .collect()
            })
        };

        let total_tail_hits: usize = per_segment_hits
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|hits| hits.len())
            .sum();
        let mut new_candidate_ids =
            NodeIdSet::with_capacity_and_hasher(total_tail_hits, NodeIdBuildHasher::default());
        for result in per_segment_hits {
            for (node_id, _) in result? {
                if candidate_ids.insert(node_id) {
                    new_candidate_ids.insert(node_id);
                }
            }
        }
        Ok(new_candidate_ids)
    }

    /// Resolve a `VectorSearchScope` into a set of reachable node IDs using
    /// the existing traversal substrate. The start node (depth 0) is included.
    fn resolve_scope_ids(&self, scope: &ResolvedVectorSearchScope) -> Result<NodeIdSet, EngineError> {
        let traverse_opts = TraverseOptions {
            min_depth: 0,
            direction: scope.direction,
            edge_label_filter: None,
            emit_node_label_filter: None,
            at_epoch: scope.at_epoch,
            decay_lambda: None,
            limit: None,
            cursor: None,
        };
        let result = if scope.edge_label_filter.is_none() {
            self.traverse(scope.start_node_id, scope.max_depth, &traverse_opts)?
        } else {
            self.traverse_resolved(
                scope.start_node_id,
                scope.max_depth,
                &traverse_opts,
                scope.edge_label_filter.as_deref(),
                &ResolvedNodeLabelFilter::Unconstrained,
            )?
        };
        let mut ids =
            NodeIdSet::with_capacity_and_hasher(result.items.len(), NodeIdBuildHasher::default());
        for hit in &result.items {
            ids.insert(hit.node_id);
        }
        Ok(ids)
    }

    fn vector_search_dense(
        &self,
        request: &ResolvedVectorSearchRequest<'_>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let scope_ids = match &request.scope {
            Some(scope) => Some(self.resolve_scope_ids(scope)?),
            None => None,
        };
        self.vector_search_dense_with_scope(request, request.k, scope_ids.as_ref())
    }

    fn vector_search_dense_with_scope(
        &self,
        request: &ResolvedVectorSearchRequest<'_>,
        k: usize,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let Some(query) = request.dense_query else {
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

        let label_filter = &request.label_filter;
        let policy_cutoffs = self.query_policy_cutoffs();

        let searchable_segments: Vec<(usize, &SegmentReader, usize)> = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(segment_index, segment)| {
                segment
                    .dense_hnsw_header()
                    .map(|header| (segment_index, segment.as_ref(), header.point_count as usize))
            })
            .collect();

        // --- Small-scope fast path: skip HNSW, exact-score scope IDs only ---
        if let Some(scope) = scope_ids {
            let mut active_dense_points = 0usize;
            let _ = self
                .memtable
                .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                    if node.dense_vector.is_some() {
                        active_dense_points += 1;
                    }
                    ControlFlow::Continue(())
                });
            let mut immutable_dense_points = 0usize;
            for epoch in &self.immutable_epochs {
                let _ = epoch
                    .memtable
                    .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                        if node.dense_vector.is_some() {
                            immutable_dense_points += 1;
                        }
                        ControlFlow::Continue(())
                    });
            }
            let total_dense_points: usize = self
                .segments
                .iter()
                .map(|segment| segment.dense_vector_count())
                .sum::<usize>()
                + active_dense_points
                + immutable_dense_points;
            if scope.len() <= total_dense_points / 20 || scope.len() <= 2048 {
                let mut hits = self.score_dense_candidate_ids(
                    scope,
                    query,
                    config.metric,
                    label_filter,
                    policy_cutoffs.as_ref(),
                )?;
                hits.truncate(k);
                return Ok(hits);
            }
        }

        // --- Standard path (no scope) or large-scope path (HNSW + ACORN) ---
        let active_deleted = self.memtable.collect_deleted_nodes_at(self.snapshot_seq);
        let mut active_node_ids =
            NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
        let mut candidate_ids = NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
        let mut initial_hidden_ids =
            NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
        initial_hidden_ids.extend(active_deleted.iter().copied());
        let _ = self
            .memtable
            .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                active_node_ids.insert(node.id);
                initial_hidden_ids.insert(node.id);
                if node.dense_vector.is_some()
                    && node_label_filter_matches(label_filter, &node.label_ids)
                    && scope_ids.is_none_or(|scope| scope.contains(&node.id))
                {
                    candidate_ids.insert(node.id);
                }
                ControlFlow::Continue(())
            });
        // Also collect dense vector candidates from immutable memtables.
        // Note: candidate_ids is a NodeIdSet, so duplicates across immutable memtables
        // are harmless; score_dense_candidate_ids does a proper newest-first lookup.
        for epoch in &self.immutable_epochs {
            initial_hidden_ids.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
            let _ = epoch
                .memtable
                .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                    initial_hidden_ids.insert(node.id);
                    if node.dense_vector.is_some()
                        && !active_deleted.contains(&node.id)
                        && !active_node_ids.contains(&node.id)
                        && node_label_filter_matches(label_filter, &node.label_ids)
                        && scope_ids.is_none_or(|scope| scope.contains(&node.id))
                    {
                        candidate_ids.insert(node.id);
                    }
                    ControlFlow::Continue(())
                });
        }

        let mut hits = self.score_dense_candidate_ids(
            &candidate_ids,
            query,
            config.metric,
            label_filter,
            policy_cutoffs.as_ref(),
        )?;
        let query_norm = crate::dense_hnsw::dense_query_norm(config.metric, query);
        let mut segment_hidden_ids = initial_hidden_ids.clone();
        for (segment_index, segment) in self.segments.iter().enumerate() {
            if segment.dense_vector_count() > 0 && segment.dense_hnsw_header().is_none() {
                let mut segment_hits = Vec::new();
                segment.exact_dense_vector_search(
                    query,
                    config.metric,
                    query_norm,
                    scope_ids,
                    &segment_hidden_ids,
                    |label_ids, updated_at, weight| {
                        node_label_filter_matches(label_filter, &label_ids)
                            && policy_cutoffs.as_ref().is_none_or(|cutoffs| {
                                !cutoffs.excludes_fields(&label_ids, updated_at, weight)
                            })
                    },
                    &mut segment_hits,
                )?;
                for hit in segment_hits {
                    if candidate_ids.insert(hit.node_id) {
                        hits.push(hit);
                    }
                }
            }
            if segment_index + 1 < self.segments.len() {
                Self::hide_segment_nodes_for_older_segments(segment, &mut segment_hidden_ids)?;
            }
        }
        Self::sort_vector_hits(&mut hits);

        if candidate_ids.is_empty() && searchable_segments.is_empty() {
            hits.truncate(k);
            return Ok(hits);
        }

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
            let mut direct_exact_hits_added = false;

            // Threshold on total searchable segments (not just non-trivial ones)
            // because the serial path must still push into segment_exhausted for limit==0 entries.
            if searchable_segments.len() <= 1 {
                for (segment_index, segment, point_count) in &searchable_segments {
                    let limit = fetch_limit.min(*point_count);
                    if limit == 0 {
                        segment_exhausted.push(true);
                        continue;
                    }
                    let is_exhausted = limit >= *point_count;
                    let ef_search = request
                        .ef_search
                        .unwrap_or(fetch_limit)
                        .max(limit)
                        .min(*point_count);

                    let segment_hits = if let Some(scope) = scope_ids {
                        segment.search_dense_hnsw_scoped(query, ef_search, limit, scope)
                    } else {
                        segment.search_dense_hnsw(query, ef_search, limit)
                    };
                    match segment_hits {
                        Ok(segment_hits) => {
                            if !is_exhausted {
                                exhausted_segments = false;
                            }
                            segment_exhausted.push(is_exhausted);
                            for (node_id, _) in segment_hits {
                                if candidate_ids.insert(node_id) {
                                    new_candidate_ids.insert(node_id);
                                }
                            }
                        }
                        Err(_) => {
                            segment_exhausted.push(true);
                            self.score_exact_dense_segment(
                                *segment_index,
                                query,
                                config.metric,
                                query_norm,
                                scope_ids,
                                &initial_hidden_ids,
                                label_filter,
                                policy_cutoffs.as_ref(),
                                &mut candidate_ids,
                                &mut hits,
                            )?;
                            direct_exact_hits_added = true;
                        }
                    }
                }
            } else {
                // Multi-segment parallel path: collect per-segment results then reduce.
                let user_ef = request.ef_search;
                #[allow(clippy::type_complexity)]
                let per_segment_results: Vec<(
                    usize,
                    Result<(Vec<(u64, f32)>, bool), EngineError>,
                )> = crate::parallel::engine_cpu_install(|| {
                    use rayon::prelude::*;
                    searchable_segments
                        .par_iter()
                        .map(|(segment_index, segment, point_count)| {
                            let limit = fetch_limit.min(*point_count);
                            if limit == 0 {
                                return (*segment_index, Ok((Vec::new(), true)));
                            }
                            let is_exhausted = limit >= *point_count;
                            let ef_search =
                                user_ef.unwrap_or(fetch_limit).max(limit).min(*point_count);
                            let result = if let Some(scope) = scope_ids {
                                segment.search_dense_hnsw_scoped(query, ef_search, limit, scope)
                            } else {
                                segment.search_dense_hnsw(query, ef_search, limit)
                            };
                            (*segment_index, result.map(|hits| (hits, is_exhausted)))
                        })
                        .collect()
                });

                // Serial reduce: merge per-segment results preserving input order.
                let total_hits: usize = per_segment_results
                    .iter()
                    .filter_map(|(_, r)| r.as_ref().ok())
                    .map(|(hits, _)| hits.len())
                    .sum();
                new_candidate_ids.reserve(total_hits);
                for (segment_index, result) in per_segment_results {
                    match result {
                        Ok((hits, is_exhausted)) => {
                            if !is_exhausted {
                                exhausted_segments = false;
                            }
                            segment_exhausted.push(is_exhausted);
                            for (node_id, _) in hits {
                                if candidate_ids.insert(node_id) {
                                    new_candidate_ids.insert(node_id);
                                }
                            }
                        }
                        Err(_) => {
                            segment_exhausted.push(true);
                            self.score_exact_dense_segment(
                                segment_index,
                                query,
                                config.metric,
                                query_norm,
                                scope_ids,
                                &initial_hidden_ids,
                                label_filter,
                                policy_cutoffs.as_ref(),
                                &mut candidate_ids,
                                &mut hits,
                            )?;
                            direct_exact_hits_added = true;
                        }
                    }
                }
            }

            if !new_candidate_ids.is_empty() {
                hits.extend(self.score_dense_candidate_ids(
                    &new_candidate_ids,
                    query,
                    config.metric,
                    label_filter,
                    policy_cutoffs.as_ref(),
                )?);
            }
            if !new_candidate_ids.is_empty() || direct_exact_hits_added {
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
                        label_filter,
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
        request: &ResolvedVectorSearchRequest<'_>,
        k: usize,
        scope_ids: Option<&NodeIdSet>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let Some(query) = request.sparse_query else {
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

        let label_filter = &request.label_filter;
        let policy_cutoffs = self.query_policy_cutoffs();

        // Small-scope fast path: score scope nodes directly instead of
        // walking full posting lists across the segment.
        if let Some(scope) = scope_ids {
            if scope.len() <= 2048 {
                return self.vector_search_sparse_exact_scope(
                    k,
                    &query,
                    scope,
                    label_filter,
                    policy_cutoffs.as_ref(),
                );
            }
        }

        let mut top_hits = BinaryHeap::with_capacity(k);
        let mut hidden_ids: NodeIdSet =
            NodeIdSet::with_capacity_and_hasher(0, NodeIdBuildHasher::default());
        hidden_ids.extend(self.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        for epoch in &self.immutable_epochs {
            hidden_ids.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        }
        let _ = self
            .memtable
            .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                hidden_ids.insert(node.id);
                if scope_ids.is_some_and(|scope| !scope.contains(&node.id)) {
                    return ControlFlow::Continue(());
                }
                if !node_label_filter_matches(label_filter, &node.label_ids) {
                    return ControlFlow::Continue(());
                }
                if policy_cutoffs
                    .as_ref()
                    .is_some_and(|cutoffs| cutoffs.excludes(node))
                {
                    return ControlFlow::Continue(());
                }
                let Some(sparse_vector) = node.sparse_vector.as_ref() else {
                    return ControlFlow::Continue(());
                };
                let score = sparse_dot_score(&query, sparse_vector);
                if score > 0.0 {
                    Self::push_sparse_top_k(&mut top_hits, k, node.id, score);
                }
                ControlFlow::Continue(())
            });

        // Score nodes in immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            let _ = epoch
                .memtable
                .for_each_visible_node_at(self.snapshot_seq, &mut |node| {
                    hidden_ids.insert(node.id);
                    if scope_ids.is_some_and(|scope| !scope.contains(&node.id)) {
                        return ControlFlow::Continue(());
                    }
                    if !node_label_filter_matches(label_filter, &node.label_ids) {
                        return ControlFlow::Continue(());
                    }
                    if policy_cutoffs
                        .as_ref()
                        .is_some_and(|cutoffs| cutoffs.excludes(node))
                    {
                        return ControlFlow::Continue(());
                    }
                    let Some(sparse_vector) = node.sparse_vector.as_ref() else {
                        return ControlFlow::Continue(());
                    };
                    let score = sparse_dot_score(&query, sparse_vector);
                    if score > 0.0 {
                        Self::push_sparse_top_k(&mut top_hits, k, node.id, score);
                    }
                    ControlFlow::Continue(())
                });
        }

        // Count segments with sparse posting data to decide parallel vs serial.
        let sparse_segment_count = self
            .segments
            .iter()
            .filter(|seg| seg.sparse_postings_available())
            .count();
        let has_sparse_fallback_segments = self
            .segments
            .iter()
            .any(|seg| seg.sparse_vector_count() > 0 && !seg.sparse_postings_available());

        // Reusable buffers for the reduce loop — allocated once, cleared per iteration.
        let mut candidates: Vec<(u64, f32)> = Vec::new();
        let mut meta_results: Vec<Option<(NodeLabelSet, i64, f32)>> = Vec::new();
        let mut remaining: Vec<(usize, u64)> = Vec::new();
        let mut exact_hits: Vec<(u64, f32)> = Vec::new();

        if sparse_segment_count <= 1 || has_sparse_fallback_segments {
            // Single-segment / fallback path: keep newest-to-oldest visibility
            // state local so unavailable or failing postings can exact-scan
            // their segment from vector source truth.
            for (segment_index, segment) in self.segments.iter().enumerate() {
                let has_older_segments = segment_index + 1 < self.segments.len();
                if segment.sparse_postings_available() {
                    let mut scores = NodeIdMap::default();
                    match segment.accumulate_sparse_posting_scores(&query, &mut scores) {
                        Ok(()) => Self::sparse_reduce_segment_scores(
                            segment,
                            scores,
                            &mut hidden_ids,
                            scope_ids,
                            label_filter,
                            policy_cutoffs.as_ref(),
                            &mut top_hits,
                            k,
                            has_older_segments,
                            &mut candidates,
                            &mut meta_results,
                            &mut remaining,
                        )?,
                        Err(_) => Self::sparse_reduce_exact_segment_scores(
                            segment,
                            &query,
                            &mut hidden_ids,
                            scope_ids,
                            label_filter,
                            policy_cutoffs.as_ref(),
                            &mut top_hits,
                            k,
                            has_older_segments,
                            &mut exact_hits,
                        )?,
                    }
                } else if segment.sparse_vector_count() > 0 {
                    Self::sparse_reduce_exact_segment_scores(
                        segment,
                        &query,
                        &mut hidden_ids,
                        scope_ids,
                        label_filter,
                        policy_cutoffs.as_ref(),
                        &mut top_hits,
                        k,
                        has_older_segments,
                        &mut exact_hits,
                    )?;
                } else if has_older_segments {
                    Self::hide_segment_nodes_for_older_segments(segment, &mut hidden_ids)?;
                }
            }
        } else {
            // Multi-segment parallel path: parallel score, serial reduce.
            let per_segment_scores: Vec<Result<NodeIdMap<f32>, EngineError>> =
                crate::parallel::engine_cpu_install(|| {
                    use rayon::prelude::*;
                    self.segments
                        .par_iter()
                        .map(|segment| {
                            let cap = (segment.node_count() as usize / 4).max(16);
                            let mut scores = NodeIdMap::with_capacity_and_hasher(
                                cap,
                                NodeIdBuildHasher::default(),
                            );
                            segment.accumulate_sparse_posting_scores(&query, &mut scores)?;
                            Ok(scores)
                        })
                        .collect()
                });

            // Serial ordered reduce (newest-to-oldest) for visibility correctness.
            for (segment_index, (segment, scores_result)) in
                self.segments.iter().zip(per_segment_scores).enumerate()
            {
                let has_older_segments = segment_index + 1 < self.segments.len();
                match scores_result {
                    Ok(scores) => Self::sparse_reduce_segment_scores(
                        segment,
                        scores,
                        &mut hidden_ids,
                        scope_ids,
                        label_filter,
                        policy_cutoffs.as_ref(),
                        &mut top_hits,
                        k,
                        has_older_segments,
                        &mut candidates,
                        &mut meta_results,
                        &mut remaining,
                    )?,
                    Err(_) => Self::sparse_reduce_exact_segment_scores(
                        segment,
                        &query,
                        &mut hidden_ids,
                        scope_ids,
                        label_filter,
                        policy_cutoffs.as_ref(),
                        &mut top_hits,
                        k,
                        has_older_segments,
                        &mut exact_hits,
                    )?,
                }
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
        label_filter: &ResolvedNodeLabelFilter,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let mut top_hits = BinaryHeap::with_capacity(k);

        // Sort scope IDs for segment merge walks.
        let mut sorted_ids: Vec<u64> = scope_ids.iter().copied().collect();
        sorted_ids.sort_unstable();

        // Track IDs we still need to find across segments (memtable first).
        let mut remaining = Vec::with_capacity(sorted_ids.len());
        self.memtable.visit_nodes_sorted_at(
            &sorted_ids,
            self.snapshot_seq,
            &mut remaining,
            &mut |node_id, node| {
                if !node_label_filter_matches(label_filter, &node.label_ids) {
                    return;
                }
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    return;
                }
                if let Some(sparse_vector) = node.sparse_vector.as_ref() {
                    let score = sparse_dot_score(query, sparse_vector);
                    if score > 0.0 {
                        Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
                    }
                }
            },
        );

        // Check immutable memtables (newest-first)
        for epoch in &self.immutable_epochs {
            if remaining.is_empty() {
                break;
            }
            let mut next = Vec::with_capacity(remaining.len());
            epoch.memtable.visit_nodes_sorted_at(
                &remaining,
                self.snapshot_seq,
                &mut next,
                &mut |node_id, node| {
                    if !node_label_filter_matches(label_filter, &node.label_ids) {
                        return;
                    }
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        return;
                    }
                    if let Some(sparse_vector) = node.sparse_vector.as_ref() {
                        let score = sparse_dot_score(query, sparse_vector);
                        if score > 0.0 {
                            Self::push_sparse_top_k(&mut top_hits, k, node_id, score);
                        }
                    }
                },
            );
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
                |label_ids, updated_at, weight| {
                    node_label_filter_matches(label_filter, &label_ids)
                        && policy_cutoffs.is_none_or(|cutoffs| {
                            !cutoffs.excludes_fields(&label_ids, updated_at, weight)
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
        request: &ResolvedVectorSearchRequest<'_>,
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
        let sub_k = request
            .k
            .saturating_mul(HYBRID_OVERFETCH_FACTOR)
            .max(request.k);

        let (dense_result, sparse_result) = crate::parallel::engine_cpu_join(
            || self.vector_search_dense_with_scope(request, sub_k, scope_ids.as_ref()),
            || self.vector_search_sparse_with_scope(request, sub_k, scope_ids.as_ref()),
        );
        let dense_hits = dense_result?;
        let sparse_hits = sparse_result?;

        let fusion_mode = request.fusion_mode.unwrap_or_default();
        let dense_weight = request.dense_weight.unwrap_or(1.0);
        let sparse_weight = request.sparse_weight.unwrap_or(1.0);

        let fused = match fusion_mode {
            FusionMode::WeightedRankFusion => fuse_weighted_rank(
                &dense_hits,
                &sparse_hits,
                dense_weight,
                sparse_weight,
                request.k,
            ),
            FusionMode::ReciprocalRankFusion => {
                fuse_reciprocal_rank(&dense_hits, &sparse_hits, request.k)
            }
            FusionMode::WeightedScoreFusion => fuse_weighted_score(
                &dense_hits,
                &sparse_hits,
                dense_weight,
                sparse_weight,
                request.k,
            ),
        };

        Ok(fused)
    }

    fn validate_vector_search_dense_shape(
        &self,
        query: Option<&DenseVector>,
        k: usize,
        ef_search: Option<usize>,
    ) -> Result<(), EngineError> {
        let Some(query) = query else {
            return Err(EngineError::InvalidOperation(
                "vector_search(mode=\"dense\") requires dense_query".into(),
            ));
        };
        if k == 0 {
            return Ok(());
        }

        let Some(config) = self.manifest.dense_vector.as_ref() else {
            return Ok(());
        };
        validate_dense_vector(query, config)?;

        if ef_search == Some(0) {
            return Err(EngineError::InvalidOperation(
                "vector_search ef_search must be > 0".into(),
            ));
        }
        Ok(())
    }

    fn validate_vector_search_sparse_shape(
        &self,
        query: Option<&SparseVector>,
        k: usize,
    ) -> Result<(), EngineError> {
        let Some(query) = query else {
            return Err(EngineError::InvalidOperation(
                "vector_search(mode=\"sparse\") requires sparse_query".into(),
            ));
        };
        if k == 0 {
            return Ok(());
        }
        let _ = canonicalize_sparse_vector(query)?;
        Ok(())
    }

    fn validate_vector_search_shape(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<(), EngineError> {
        match request.mode {
            VectorSearchMode::Dense => self.validate_vector_search_dense_shape(
                request.dense_query.as_ref(),
                request.k,
                request.ef_search,
            ),
            VectorSearchMode::Sparse => {
                self.validate_vector_search_sparse_shape(request.sparse_query.as_ref(), request.k)
            }
            VectorSearchMode::Hybrid => {
                let has_dense = request.dense_query.is_some();
                let has_sparse = request.sparse_query.is_some();

                if !has_dense && !has_sparse {
                    return Err(EngineError::InvalidOperation(
                        "vector_search(mode=\"hybrid\") requires at least one of dense_query or sparse_query".into(),
                    ));
                }

                if has_dense && !has_sparse {
                    return self.validate_vector_search_dense_shape(
                        request.dense_query.as_ref(),
                        request.k,
                        request.ef_search,
                    );
                }
                if has_sparse && !has_dense {
                    return self
                        .validate_vector_search_sparse_shape(request.sparse_query.as_ref(), request.k);
                }
                if request.k == 0 {
                    return Ok(());
                }

                self.validate_vector_search_dense_shape(
                    request.dense_query.as_ref(),
                    request.k,
                    request.ef_search,
                )?;
                self.validate_vector_search_sparse_shape(request.sparse_query.as_ref(), request.k)
            }
        }
    }

    /// Search node vectors and return scored node IDs.
    ///
    /// Supports `mode="dense"`, `mode="sparse"`, and `mode="hybrid"`.
    /// All modes support optional graph-scoped filtering via `scope`.
    pub fn vector_search(
        &self,
        request: &VectorSearchRequest,
    ) -> Result<Vec<VectorHit>, EngineError> {
        let label_filter = self
            .label_catalog
            .resolve_node_label_filter_request(request.label_filter.as_ref())?;
        let scope = request
            .scope
            .as_ref()
            .map(|scope| {
                let edge_label_filter = match self
                    .label_catalog
                    .resolve_edge_label_filter_for_read(scope.edge_label_filter.as_deref())?
                {
                    LabelFilterResolution::Unconstrained => None,
                    LabelFilterResolution::Known(label_ids) => Some(label_ids),
                    LabelFilterResolution::EmptyConstraint => Some(Vec::new()),
                };
                Ok::<ResolvedVectorSearchScope, EngineError>(ResolvedVectorSearchScope {
                    start_node_id: scope.start_node_id,
                    max_depth: scope.max_depth,
                    direction: scope.direction,
                    edge_label_filter,
                    at_epoch: scope.at_epoch,
                })
            })
            .transpose()?;
        if label_filter.is_empty_constraint() {
            self.validate_vector_search_shape(request)?;
            return Ok(Vec::new());
        };
        let resolved = ResolvedVectorSearchRequest {
            mode: request.mode,
            dense_query: request.dense_query.as_ref(),
            sparse_query: request.sparse_query.as_ref(),
            k: request.k,
            label_filter,
            ef_search: request.ef_search,
            scope,
            dense_weight: request.dense_weight,
            sparse_weight: request.sparse_weight,
            fusion_mode: request.fusion_mode,
        };

        match resolved.mode {
            VectorSearchMode::Dense => self.vector_search_dense(&resolved),
            VectorSearchMode::Sparse => {
                let scope_ids = match &resolved.scope {
                    Some(scope) => Some(self.resolve_scope_ids(scope)?),
                    None => None,
                };
                self.vector_search_sparse_with_scope(&resolved, resolved.k, scope_ids.as_ref())
            }
            VectorSearchMode::Hybrid => self.vector_search_hybrid(&resolved),
        }
    }

    // --- Secondary index queries ---

    fn filter_node_ids_by_current_label(
        &self,
        mut ids: Vec<u64>,
        label_id: u32,
    ) -> Result<Vec<u64>, EngineError> {
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(ids);
        }
        if self.immutable_epochs.is_empty() && self.segments.is_empty() {
            return Ok(ids);
        }

        let visibility = self.sources().find_node_visibility_meta(&ids)?;
        let mut filtered = Vec::with_capacity(ids.len());
        for (node_id, state) in ids.into_iter().zip(visibility) {
            if matches!(
                state,
                NodeVisibilityState::Live(meta) if meta.label_ids.contains(label_id)
            ) {
                filtered.push(node_id);
            }
        }
        Ok(filtered)
    }

    fn filter_node_ids_by_current_label_and_time(
        &self,
        mut ids: Vec<u64>,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(ids);
        }

        let visibility = self.sources().find_node_visibility_meta(&ids)?;
        let mut filtered = Vec::with_capacity(ids.len());
        for (node_id, state) in ids.into_iter().zip(visibility) {
            if matches!(
                state,
                NodeVisibilityState::Live(meta)
                    if meta.label_ids.contains(label_id)
                        && meta.updated_at >= from_ms
                        && meta.updated_at <= to_ms
            ) {
                filtered.push(node_id);
            }
        }
        Ok(filtered)
    }

    /// Return all live node IDs with the given label_id (raw, unfiltered).
    /// Used internally by collect_prune_targets.
    fn nodes_by_label_id_raw(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut candidates = Vec::new();

        for id in self.memtable.visible_nodes_by_label_id(label_id, self.snapshot_seq) {
            candidates.push(id);
        }

        for epoch in &self.immutable_epochs {
            for id in epoch.memtable.visible_nodes_by_label_id(label_id, self.snapshot_seq) {
                candidates.push(id);
            }
        }

        for seg in &self.segments {
            for id in seg.nodes_by_label_id(label_id)? {
                candidates.push(id);
            }
        }

        self.filter_node_ids_by_current_label(candidates, label_id)
    }

    /// Return all live node IDs with the given label_id, merged across
    /// memtable and all segments. Excludes tombstoned and policy-excluded nodes.
    pub fn nodes_by_label_id(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut results = self.nodes_by_label_id_raw(label_id)?;

        // Policy filtering: batch-fetch nodes and exclude matches.
        let excluded = self.policy_excluded_node_ids(&results)?;
        if !excluded.is_empty() {
            results.retain(|id| !excluded.contains(id));
        }

        Ok(results)
    }

    /// Return all live edge IDs with the given label_id, merged across
    /// memtable and all segments. Excludes tombstoned edges.
    pub fn edges_by_label_id(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        let deleted = self.sources().collect_deleted_edges();

        let mut seen = NodeIdSet::default();
        let mut results = Vec::new();

        for id in self.memtable.visible_edges_by_label_id(label_id, self.snapshot_seq) {
            if !deleted.contains(&id) && seen.insert(id) {
                results.push(id);
            }
        }

        for epoch in &self.immutable_epochs {
            for id in epoch.memtable.visible_edges_by_label_id(label_id, self.snapshot_seq) {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        for seg in &self.segments {
            for id in seg.edges_by_label_id(label_id)? {
                if !deleted.contains(&id) && seen.insert(id) {
                    results.push(id);
                }
            }
        }

        Ok(results)
    }

    /// Return all live node records with the given label_id, hydrated from
    /// memtable and segments. Excludes tombstoned and policy-excluded nodes.
    ///
    /// Uses `nodes_by_label_id()` for the ID list (already policy-filtered), then
    /// `get_nodes_raw()` for batch hydration (one merge-walk per segment,
    /// not N individual lookups). Single policy pass, no redundant filtering.
    pub fn get_nodes_by_label_id(&self, label_id: u32) -> Result<Vec<NodeRecord>, EngineError> {
        let ids = self.nodes_by_label_id(label_id)?;
        let results = self.get_nodes_raw(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Return all live edge records with the given label_id, hydrated from
    /// memtable and segments. Excludes tombstoned edges.
    ///
    /// Uses `edges_by_label_id()` for the ID list, then `get_edges()` for batch
    /// hydration (one merge-walk per segment, not N individual lookups).
    pub fn get_edges_by_label_id(&self, label_id: u32) -> Result<Vec<EdgeRecord>, EngineError> {
        let ids = self.edges_by_label_id(label_id)?;
        let results = self.get_edges(&ids)?;
        Ok(results.into_iter().flatten().collect())
    }

    fn count_nodes_by_resolved_label_filter(
        &self,
        filter: &ResolvedNodeLabelFilter,
    ) -> Result<u64, EngineError> {
        let ResolvedNodeLabelFilter::LabelSet {
            mode,
            label_ids,
            ..
        } = filter
        else {
            return match filter {
                ResolvedNodeLabelFilter::Empty { .. } => Ok(0),
                ResolvedNodeLabelFilter::Unconstrained => {
                    let policy_cutoffs = self.query_policy_cutoffs();
                    Ok(self
                        .collect_node_ids_for_resolved_label_filter(
                            filter,
                            policy_cutoffs.as_ref(),
                        )?
                        .len() as u64)
                }
                ResolvedNodeLabelFilter::LabelSet { .. } => unreachable!(),
            };
        };

        let scan_labels: Vec<u32> = match mode {
            LabelMatchMode::Any => label_ids.as_slice().to_vec(),
            LabelMatchMode::All => {
                if label_ids.len() == 1 {
                    label_ids.as_slice().to_vec()
                } else {
                    let driver = self
                        .node_label_filter_estimate(label_ids, LabelMatchMode::All)?
                        .driver_label_id
                        .unwrap_or_else(|| label_ids.as_slice()[0]);
                    vec![driver]
                }
            }
        };
        let policy_cutoffs = self.query_policy_cutoffs();
        let mut count = 0u64;
        self.scan_raw_node_label_candidates(&scan_labels, None, QUERY_VERIFY_CHUNK, |chunk| {
            #[cfg(test)]
            self.note_node_visibility_meta_reads(chunk.len());
            let visibility = self.sources().find_node_visibility_meta(chunk)?;
            for state in visibility {
                let NodeVisibilityState::Live(meta) = state else {
                    continue;
                };
                if policy_cutoffs.as_ref().is_some_and(|cutoffs| {
                    cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight)
                }) {
                    continue;
                }
                if node_label_filter_matches(filter, &meta.label_ids) {
                    count += 1;
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(count)
    }

    /// Return the count of live edges with the given label_id without hydrating
    /// records. Excludes tombstoned edges (edges are not subject to prune policies).
    pub fn count_edges_by_label_id(&self, label_id: u32) -> Result<u64, EngineError> {
        Ok(self.edges_by_label_id(label_id)?.len() as u64)
    }

    // --- Paginated label-index queries ---

    /// Paginated version of `nodes_by_label_id`. Returns a page of node IDs sorted
    /// by ID, with cursor-based pagination. Pass `PageRequest::default()` to get
    /// all results (equivalent to `nodes_by_label_id`).
    ///
    /// Uses K-way merge across already-sorted sources with early termination:
    /// O(cursor_position + limit) instead of O(N log N) when no prune policies
    /// are active. With policies, still saves the sort via merge, then applies
    /// policy filtering and cursor on the sorted result.
    pub fn nodes_by_label_id_paged(
        &self,
        label_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let unfiltered = self.nodes_by_single_label_id_paged_unfiltered(label_id, page)?;
        if self.manifest.prune_policies.is_empty() {
            // Fast path: merge with early termination, no policy filtering needed
            Ok(unfiltered)
        } else {
            let limit = page.limit.unwrap_or(0);
            if limit == 0 {
                let excluded = self.policy_excluded_node_ids(&unfiltered.items)?;
                let mut items = unfiltered.items;
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
                        self.nodes_by_single_label_id_paged_unfiltered(label_id, &chunk_page)?;
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

    /// Paginated version of `edges_by_label_id`. Returns a page of edge IDs sorted
    /// by ID, with cursor-based pagination. Uses K-way merge with early
    /// termination (edges are not subject to prune policies).
    pub fn edges_by_label_id_paged(
        &self,
        label_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let deleted = self.sources().collect_deleted_edges();

        // Collect sources
        let memtable_ids = self.memtable.visible_edges_by_label_id(label_id, self.snapshot_seq);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.visible_edges_by_label_id(label_id, self.snapshot_seq));
        }
        for seg in &self.segments {
            segment_ids.push(seg.edges_by_label_id(label_id)?);
        }

        Ok(merge_record_ids_paged(
            memtable_ids,
            segment_ids,
            &deleted,
            page,
        ))
    }

    /// Paginated version of `get_nodes_by_label_id`. Returns a page of hydrated node
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_nodes_by_label_id_paged(
        &self,
        label_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<NodeRecord>, EngineError> {
        let id_page = self.nodes_by_label_id_paged(label_id, page)?;
        let hydrated = self.get_nodes_raw(&id_page.items)?;
        let items: Vec<NodeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    /// Paginated version of `get_edges_by_label_id`. Returns a page of hydrated edge
    /// records. Only hydrates records in the requested page (not all then slice).
    /// In rare cases (data inconsistency), the page may contain fewer items than
    /// `limit` even when `next_cursor` is `Some`.
    pub fn get_edges_by_label_id_paged(
        &self,
        label_id: u32,
        page: &PageRequest,
    ) -> Result<PageResult<EdgeRecord>, EngineError> {
        let id_page = self.edges_by_label_id_paged(label_id, page)?;
        let hydrated = self.get_edges(&id_page.items)?;
        let items: Vec<EdgeRecord> = hydrated.into_iter().flatten().collect();
        Ok(PageResult {
            items,
            next_cursor: id_page.next_cursor,
        })
    }

    fn timestamp_candidate_ids(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        max_ids: usize,
    ) -> Result<Vec<u64>, EngineError> {
        if from_ms > to_ms || max_ids == 0 {
            return Ok(Vec::new());
        }

        let mut ids = Vec::with_capacity(max_ids.min(4096));
        let mut seen = NodeIdSet::default();
        let flow = self.memtable.for_each_visible_node_by_time_range_at(
            label_id,
            from_ms,
            to_ms,
            self.snapshot_seq,
            &mut |node_id| {
                push_unique_candidate_id_limited(&mut ids, &mut seen, node_id, max_ids)
            },
        );
        if flow.is_break() {
            ids.sort_unstable();
            return Ok(ids);
        }

        for epoch in &self.immutable_epochs {
            let flow = epoch.memtable.for_each_visible_node_by_time_range_at(
                label_id,
                from_ms,
                to_ms,
                self.snapshot_seq,
                &mut |node_id| {
                    push_unique_candidate_id_limited(&mut ids, &mut seen, node_id, max_ids)
                },
            );
            if flow.is_break() {
                ids.sort_unstable();
                return Ok(ids);
            }
        }

        for seg in &self.segments {
            let flow = seg.for_each_node_by_time_range(label_id, from_ms, to_ms, |node_id| {
                push_unique_candidate_id_limited(&mut ids, &mut seen, node_id, max_ids)
            })?;
            if flow.is_break() {
                ids.sort_unstable();
                return Ok(ids);
            }
        }

        self.filter_node_ids_by_current_label_and_time(ids, label_id, from_ms, to_ms)
    }

    fn find_nodes_outcome(
        &self,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
    ) -> Result<PropertyQueryOutcome<Vec<u64>>, EngineError> {
        let ready_entry =
            self.node_property_index_entry(label_id, prop_key, &SecondaryIndexKind::Equality);
        if let Some(entry) = ready_entry.filter(|entry| entry.state == SecondaryIndexState::Ready) {
            let (results, followup) =
                self.find_nodes_ready_equality_index(label_id, entry.index_id, prop_key, prop_value)?;
            if let Some(results) = results
            {
                Ok(PropertyQueryOutcome {
                    value: results,
                    route: PropertyQueryRouteKind::EqualityIndexLookup,
                    followup,
                })
            } else {
                Ok(PropertyQueryOutcome {
                    value: self.find_nodes_scan_fallback(label_id, prop_key, prop_value)?,
                    route: PropertyQueryRouteKind::EqualityScanFallback,
                    followup,
                })
            }
        } else {
            Ok(PropertyQueryOutcome {
                value: self.find_nodes_scan_fallback(label_id, prop_key, prop_value)?,
                route: PropertyQueryRouteKind::EqualityScanFallback,
                followup: None,
            })
        }
    }

    fn find_nodes_paged_outcome(
        &self,
        label_id: u32,
        prop_key: &str,
        prop_value: &PropValue,
        page: &PageRequest,
    ) -> Result<PropertyQueryOutcome<PageResult<u64>>, EngineError> {
        let ready_entry =
            self.node_property_index_entry(label_id, prop_key, &SecondaryIndexKind::Equality);
        if let Some(entry) = ready_entry.filter(|entry| entry.state == SecondaryIndexState::Ready) {
            let (result, followup) = self.find_nodes_paged_ready_equality_index(
                label_id,
                entry.index_id,
                prop_key,
                prop_value,
                page,
            )?;
            if let Some(result) = result {
                Ok(PropertyQueryOutcome {
                    value: result,
                    route: PropertyQueryRouteKind::EqualityIndexLookup,
                    followup,
                })
            } else {
                Ok(PropertyQueryOutcome {
                    value: self.find_nodes_paged_scan_fallback(
                        label_id, prop_key, prop_value, page,
                    )?,
                    route: PropertyQueryRouteKind::EqualityScanFallback,
                    followup,
                })
            }
        } else {
            Ok(PropertyQueryOutcome {
                value: self.find_nodes_paged_scan_fallback(label_id, prop_key, prop_value, page)?,
                route: PropertyQueryRouteKind::EqualityScanFallback,
                followup: None,
            })
        }
    }

    fn find_nodes_range_paged_outcome(
        &self,
        label_id: u32,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        page: &PropertyRangePageRequest,
    ) -> Result<PropertyQueryOutcome<PropertyRangePageResult<u64>>, EngineError> {
        let validated = Self::validate_property_range_bounds(lower, upper, page.after.as_ref())?;
        if validated.is_empty {
            return Ok(PropertyQueryOutcome {
                value: PropertyRangePageResult {
                    items: Vec::new(),
                    next_cursor: None,
                },
                route: PropertyQueryRouteKind::RangeScanFallback,
                followup: None,
            });
        }

        if let Some(entry) = self.node_property_index_entry(
            label_id,
            prop_key,
            &SecondaryIndexKind::Range,
        ) {
            if entry.state == SecondaryIndexState::Ready {
                let (ready_value, followup) = self.find_nodes_paged_ready_range_index(
                    label_id,
                    entry.index_id,
                    prop_key,
                    lower,
                    upper,
                    page,
                )?;
                if let Some(value) = ready_value {
                    return Ok(PropertyQueryOutcome {
                        value,
                        route: PropertyQueryRouteKind::RangeIndexLookup,
                        followup,
                    });
                }
                if let Some(followup) = followup {
                    let value = self.find_nodes_range_scan_page(
                        label_id,
                        prop_key,
                        lower,
                        upper,
                        page,
                    )?;
                    return Ok(PropertyQueryOutcome {
                        value,
                        route: PropertyQueryRouteKind::RangeScanFallback,
                        followup: Some(followup),
                    });
                }
            }
        }

        let value = self.find_nodes_range_scan_page(label_id, prop_key, lower, upper, page)?;
        Ok(PropertyQueryOutcome {
            value,
            route: PropertyQueryRouteKind::RangeScanFallback,
            followup: None,
        })
    }

    fn find_nodes_range_scan_page(
        &self,
        label_id: u32,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        page: &PropertyRangePageRequest,
    ) -> Result<PropertyRangePageResult<u64>, EngineError> {
        let value = match page.limit {
            Some(limit) if limit > 0 => {
                let matches = self.collect_property_range_scan_matches(
                    label_id,
                    prop_key,
                    lower,
                    upper,
                    page.after.as_ref(),
                    Some(limit.saturating_add(1)),
                )?;
                let has_more = matches.len() > limit;
                let selected = &matches[..matches.len().min(limit)];
                let items = selected.iter().map(|entry| entry.node_id).collect();
                let next_cursor = if has_more {
                    selected.last().map(|entry| PropertyRangeCursor {
                        value: entry.value.clone(),
                        node_id: entry.node_id,
                    })
                } else {
                    None
                };
                PropertyRangePageResult { items, next_cursor }
            }
            _ => {
                let matches = self.collect_property_range_scan_matches(
                    label_id,
                    prop_key,
                    lower,
                    upper,
                    page.after.as_ref(),
                    None,
                )?;
                PropertyRangePageResult {
                    items: matches.into_iter().map(|entry| entry.node_id).collect(),
                    next_cursor: None,
                }
            }
        };
        Ok(value)
    }

    // --- Timestamp range queries ---

    /// Find node IDs of a given label updated within a time range [from_ms, to_ms] (inclusive).
    /// Merges across memtable + segments with deduplication.
    /// Excludes tombstoned and policy-pruned nodes.
    pub fn find_nodes_by_time_range(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<u64>, EngineError> {
        let page = PageRequest {
            limit: None,
            after: None,
        };
        Ok(self
            .find_nodes_by_time_range_paged(label_id, from_ms, to_ms, &page)?
            .items)
    }

    /// Paginated version of `find_nodes_by_time_range`. Returns a page of node IDs
    /// sorted by ID with cursor-based pagination.
    ///
    /// Uses K-way merge across sorted sources (binary search for range in each
    /// segment, sort results by node_id). O(log N) seek per source + O(results) scan.
    pub fn find_nodes_by_time_range_paged(
        &self,
        label_id: u32,
        from_ms: i64,
        to_ms: i64,
        page: &PageRequest,
    ) -> Result<PageResult<u64>, EngineError> {
        let deleted = self.sources().collect_deleted_nodes();
        let memtable_ids =
            self.memtable
                .visible_nodes_by_time_range(label_id, from_ms, to_ms, self.snapshot_seq);
        let mut segment_ids: Vec<Vec<u64>> =
            Vec::with_capacity(self.immutable_epochs.len() + self.segments.len());
        for epoch in &self.immutable_epochs {
            segment_ids.push(epoch.memtable.visible_nodes_by_time_range(
                label_id,
                from_ms,
                to_ms,
                self.snapshot_seq,
            ));
        }
        for seg in &self.segments {
            segment_ids.push(seg.nodes_by_time_range(label_id, from_ms, to_ms)?);
        }

        let limit = page.limit.unwrap_or(0);
        if limit == 0 {
            let all_page = PageRequest {
                limit: None,
                after: page.after,
            };
            let merged = merge_record_ids_paged(memtable_ids, segment_ids, &deleted, &all_page);
            let mut items =
                self.filter_node_ids_by_current_label_and_time(merged.items, label_id, from_ms, to_ms)?;
            if !self.manifest.prune_policies.is_empty() {
                let excluded = self.policy_excluded_node_ids(&items)?;
                if !excluded.is_empty() {
                    items.retain(|id| !excluded.contains(id));
                }
            }
            return Ok(PageResult {
                items,
                next_cursor: None,
            });
        }

        let target = page_verify_target(limit);
        let chunk_limit = limit
            .saturating_add(1)
            .saturating_mul(4)
            .max(limit.saturating_add(1));
        let mut collected = Vec::with_capacity(limit);
        let mut cursor = page.after;
        loop {
            let chunk_page = PageRequest {
                limit: Some(chunk_limit),
                after: cursor,
            };
            let chunk = merge_record_ids_paged(
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

            let visible = self.filter_node_ids_by_current_label_and_time(
                chunk.items.clone(),
                label_id,
                from_ms,
                to_ms,
            )?;
            let excluded = if self.manifest.prune_policies.is_empty() {
                NodeIdSet::default()
            } else {
                self.policy_excluded_node_ids(&visible)?
            };
            let visible: NodeIdSet = visible.into_iter().collect();
            for id in chunk.items.iter().copied() {
                if visible.contains(&id) && !excluded.contains(&id) {
                    collected.push(id);
                    if collected.len() >= target {
                        return Ok(finish_verified_id_page(collected, limit));
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
        let empty_approx = || {
            if options.algorithm == PprAlgorithm::ApproxForwardPush {
                Some(PprApproxMeta {
                    residual_tolerance: options.approx_residual_tolerance,
                    pushes: 0,
                    max_remaining_residual: 0.0,
                })
            } else {
                None
            }
        };

        if seed_node_ids.is_empty() {
            return Ok(PprResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
                algorithm: options.algorithm,
                approx: empty_approx(),
            });
        }

        let damping = options.damping_factor;
        if damping <= 0.0 || damping >= 1.0 {
            return Err(EngineError::InvalidOperation(
                "damping_factor must be in (0.0, 1.0)".into(),
            ));
        }
        if options.algorithm == PprAlgorithm::ApproxForwardPush
            && options.approx_residual_tolerance <= 0.0
        {
            return Err(EngineError::InvalidOperation(
                "approx_residual_tolerance must be > 0.0".into(),
            ));
        }
        let resolved_edge_filter =
            self.resolve_edge_label_filter_for_graph(options.edge_label_filter.as_deref())?;
        let edge_filter = match resolved_edge_filter {
            LabelFilterResolution::Unconstrained => None,
            LabelFilterResolution::Known(label_ids) => Some(label_ids),
            LabelFilterResolution::EmptyConstraint => Some(Vec::new()),
        };

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
                algorithm: options.algorithm,
                approx: empty_approx(),
            });
        }
        let num_seeds = seeds.len() as f64;
        let teleport = (1.0 - damping) / num_seeds;

        if options.algorithm == PprAlgorithm::ApproxForwardPush {
            let tolerance = options.approx_residual_tolerance;
            let mut reserve: NodeIdMap<f64> =
                NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());
            let mut residual: NodeIdMap<f64> =
                NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());
            let mut pending: NodeIdSet =
                NodeIdSet::with_capacity_and_hasher(seeds.len() * 16, Default::default());
            let mut neighbor_cache: NodeIdMap<Vec<(u64, f64)>> =
                NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());
            let mut out_weight_sums: NodeIdMap<f64> =
                NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());
            let mut pushes = 0u64;

            for &seed in &seeds {
                *residual.entry(seed).or_insert(0.0) += 1.0 / num_seeds;
                pending.insert(seed);
            }

            while !pending.is_empty() {
                let mut wave: Vec<u64> = pending.drain().collect();
                wave.sort_unstable();
                let uncached: Vec<u64> = wave
                    .iter()
                    .copied()
                    .filter(|id| !neighbor_cache.contains_key(id))
                    .collect();

                if !uncached.is_empty() {
                    let all_neighbors = self.neighbors_batch_resolved(
                        &uncached,
                        Direction::Outgoing,
                        edge_filter.as_deref(),
                        None,
                        None,
                    )?;
                    for &node_id in &uncached {
                        let raw_neighbors: Vec<(u64, f32)> = all_neighbors
                            .get(&node_id)
                            .map(|n| n.iter().map(|e| (e.node_id, e.weight)).collect())
                            .unwrap_or_default();
                        let total_weight: f64 = raw_neighbors.iter().map(|&(_, w)| w as f64).sum();
                        if total_weight > 0.0 {
                            out_weight_sums.insert(node_id, total_weight);
                            neighbor_cache.insert(
                                node_id,
                                raw_neighbors
                                    .into_iter()
                                    .map(|(nid, w)| (nid, (w as f64) / total_weight))
                                    .collect(),
                            );
                        } else {
                            out_weight_sums.insert(node_id, 0.0);
                            neighbor_cache.insert(node_id, Vec::new());
                        }
                    }
                }

                let mut wave_updates: NodeIdMap<f64> =
                    NodeIdMap::with_capacity_and_hasher(wave.len() * 8, Default::default());
                let mut dangling_seed_add = 0.0_f64;

                for node_id in wave {
                    let res = *residual.get(&node_id).unwrap_or(&0.0);
                    if res == 0.0 {
                        continue;
                    }

                    let out_weight_sum = *out_weight_sums.get(&node_id).unwrap_or(&0.0);
                    let activation = if out_weight_sum > 0.0 {
                        res / out_weight_sum
                    } else {
                        res
                    };
                    if activation <= tolerance {
                        continue;
                    }

                    residual.insert(node_id, 0.0);
                    *reserve.entry(node_id).or_insert(0.0) += (1.0 - damping) * res;
                    let pushed_mass = damping * res;
                    pushes += 1;

                    let neighbors = neighbor_cache
                        .get(&node_id)
                        .expect("approx PPR neighbor cache should be populated");
                    if neighbors.is_empty() {
                        dangling_seed_add += pushed_mass / num_seeds;
                    } else {
                        for &(neighbor_id, norm_weight) in neighbors {
                            *wave_updates.entry(neighbor_id).or_insert(0.0) +=
                                pushed_mass * norm_weight;
                        }
                    }
                }

                if dangling_seed_add != 0.0 {
                    for &seed in &seeds {
                        *wave_updates.entry(seed).or_insert(0.0) += dangling_seed_add;
                    }
                }

                for (node_id, delta) in wave_updates {
                    let next_residual = {
                        let entry = residual.entry(node_id).or_insert(0.0);
                        *entry += delta;
                        *entry
                    };
                    if next_residual == 0.0 {
                        continue;
                    }
                    match out_weight_sums.get(&node_id).copied() {
                        Some(out_weight_sum) => {
                            let activation = if out_weight_sum > 0.0 {
                                next_residual / out_weight_sum
                            } else {
                                next_residual
                            };
                            if activation > tolerance {
                                pending.insert(node_id);
                            }
                        }
                        None => {
                            pending.insert(node_id);
                        }
                    }
                }
            }

            let mut scores: Vec<(u64, f64)> = reserve
                .into_iter()
                .filter(|(_, score)| *score > 0.0)
                .collect();
            scores.sort_unstable_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            if let Some(max) = options.max_results {
                scores.truncate(max);
            }

            let max_remaining_residual = residual.values().copied().fold(0.0, f64::max);
            return Ok(PprResult {
                scores,
                iterations: 0,
                converged: true,
                algorithm: PprAlgorithm::ApproxForwardPush,
                approx: Some(PprApproxMeta {
                    residual_tolerance: tolerance,
                    pushes,
                    max_remaining_residual,
                }),
            });
        }

        // --- Phase 1: BFS discovery to find all reachable nodes ---
        // Wave-based BFS with batch adjacency: one cursor walk per segment
        // per wave instead of O(N log K) binary searches per node.
        let mut discovered: NodeIdSet = seeds.iter().copied().collect();
        let mut wave: Vec<u64> = seeds.clone();
        let mut neighbor_cache: NodeIdMap<Vec<(u64, f32)>> =
            NodeIdMap::with_capacity_and_hasher(seeds.len() * 16, Default::default());

        while !wave.is_empty() {
            let all_neighbors = self.neighbors_batch_resolved(
                &wave,
                Direction::Outgoing,
                edge_filter.as_deref(),
                None,
                None,
            )?;

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
        let mut idx_to_id: Vec<u64> = discovered.into_iter().collect();
        idx_to_id.sort_unstable();
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
            algorithm: PprAlgorithm::ExactPowerIteration,
            approx: None,
        })
    }

    // --- Graph export ---

    /// Export the graph's adjacency structure for external community detection.
    ///
    /// Returns all live node IDs, export-local edge-label names, and edges,
    /// filtered by optional node-label and edge-label filters. Respects tombstones
    /// and prune policies. Each edge is emitted once (outgoing direction only).
    ///
    /// Edges are only included if both endpoints are in the exported node set,
    /// ensuring a consistent subgraph for external tools.
    pub fn export_adjacency(
        &self,
        options: &ExportOptions,
    ) -> Result<AdjacencyExport, EngineError> {
        let resolved_node_filter =
            self.resolve_node_label_filter_request_for_graph(options.node_label_filter.as_ref())?;
        let resolved_edge_filter =
            self.resolve_edge_label_filter_for_graph(options.edge_label_filter.as_deref())?;
        let edge_label_filter = match resolved_edge_filter {
            LabelFilterResolution::Unconstrained => None,
            LabelFilterResolution::Known(label_ids) => Some(label_ids),
            LabelFilterResolution::EmptyConstraint => Some(Vec::new()),
        };
        if resolved_node_filter.is_empty_constraint() {
            return Ok(AdjacencyExport {
                node_ids: Vec::new(),
                node_labels: Vec::new(),
                node_label_indexes: Vec::new(),
                edge_labels: Vec::new(),
                edges: Vec::new(),
            });
        }

        let policy_cutoffs = self.query_policy_cutoffs();
        let node_ids = self.collect_node_ids_for_resolved_label_filter(
            &resolved_node_filter,
            policy_cutoffs.as_ref(),
        )?;
        let node_set: NodeIdSet = node_ids.iter().copied().collect();

        let node_visibility = self.sources().find_node_visibility_meta(&node_ids)?;
        let mut node_label_sets = Vec::with_capacity(node_ids.len());
        let mut node_label_index_by_label_id: BTreeMap<u32, u32> = BTreeMap::new();
        for (&node_id, state) in node_ids.iter().zip(node_visibility.iter()) {
            let NodeVisibilityState::Live(meta) = state else {
                return Err(EngineError::InvalidOperation(format!(
                    "export node {node_id} is not live in latest visibility metadata"
                )));
            };
            node_label_sets.push(meta.label_ids);
            for &label_id in meta.label_ids.as_slice() {
                node_label_index_by_label_id.entry(label_id).or_insert(0);
            }
        }

        let mut node_labels = Vec::with_capacity(node_label_index_by_label_id.len());
        for (&label_id, index_slot) in node_label_index_by_label_id.iter_mut() {
            let label = self
                .label_catalog
                .node_label(label_id)
                .map(str::to_string)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "export node label side table references missing node label_id {label_id}"
                    ))
                })?;
            *index_slot = node_labels.len() as u32;
            node_labels.push(label);
        }
        let node_label_indexes: Vec<Vec<u32>> = node_label_sets
            .iter()
            .map(|label_ids| {
                label_ids
                    .as_slice()
                    .iter()
                    .map(|label_id| node_label_index_by_label_id[label_id])
                    .collect()
            })
            .collect();

        // Batch-fetch all outgoing neighbors in one cursor walk per segment
        // instead of O(N) individual binary searches.
        let all_neighbors = self.neighbors_batch_resolved(
            &node_ids,
            Direction::Outgoing,
            edge_label_filter.as_deref(),
            None,
            None,
        )?;

        let mut edge_label_indexes: BTreeMap<u32, u32> = BTreeMap::new();
        let mut edge_labels: Vec<String> = Vec::new();
        let mut edges: Vec<ExportEdge> = Vec::new();
        for &from_id in &node_ids {
            if let Some(neighbors) = all_neighbors.get(&from_id) {
                for entry in neighbors {
                    // Only include edges whose target is in the exported node set
                    if !node_set.contains(&entry.node_id) {
                        continue;
                    }
                    let edge_label_index = match edge_label_indexes.get(&entry.edge_label_id) {
                        Some(index) => *index,
                        None => {
                            let label = self
                                .label_catalog
                                .edge_label(entry.edge_label_id)
                                .map(str::to_string)
                                .ok_or_else(|| {
                                    EngineError::InvalidOperation(format!(
                                        "export edge {} references missing edge-label label_id {}",
                                        entry.edge_id, entry.edge_label_id
                                    ))
                                })?;
                            let index = edge_labels.len() as u32;
                            edge_labels.push(label);
                            edge_label_indexes.insert(entry.edge_label_id, index);
                            index
                        }
                    };
                    edges.push(ExportEdge {
                        from: from_id,
                        to: entry.node_id,
                        edge_label_index,
                        weight: options.include_weights.then_some(entry.weight),
                    });
                }
            }
        }

        Ok(AdjacencyExport {
            node_ids,
            node_labels,
            node_label_indexes,
            edge_labels,
            edges,
        })
    }
}
