const QUERY_VERIFY_CHUNK: usize = 256;
const PROPERTY_IN_LINEAR_VERIFY_THRESHOLD: usize = 16;
const PATTERN_FRONTIER_BUDGET: usize = 65_536;

struct QueryExecutionOutcome<T> {
    value: T,
    followups: Vec<SecondaryIndexReadFollowup>,
}

struct VerifiedNodePage {
    ids: Vec<u64>,
    nodes: Vec<NodeRecord>,
    next_cursor: Option<u64>,
}

enum CandidateMaterializationResult {
    Ready {
        ids: Vec<u64>,
        followups: Vec<SecondaryIndexReadFollowup>,
    },
    TooBroad {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
}

fn materialization_followups(
    followup: Option<SecondaryIndexReadFollowup>,
) -> Vec<SecondaryIndexReadFollowup> {
    followup.into_iter().collect()
}

enum FullScanNodeSource<'a> {
    Owned(Vec<u64>),
    Segment(&'a SegmentReader),
}

impl FullScanNodeSource<'_> {
    fn get_id(&self, index: usize) -> Result<Option<u64>, EngineError> {
        match self {
            FullScanNodeSource::Owned(ids) => Ok(ids.get(index).copied()),
            FullScanNodeSource::Segment(segment) => segment.node_id_at_index(index),
        }
    }

    fn seek_after(&self, after: Option<u64>) -> Result<usize, EngineError> {
        let Some(after) = after else {
            return Ok(0);
        };
        match self {
            FullScanNodeSource::Owned(ids) => match ids.binary_search(&after) {
                Ok(index) => Ok(index + 1),
                Err(index) => Ok(index),
            },
            FullScanNodeSource::Segment(segment) => segment.node_id_lower_bound(after),
        }
    }
}

fn first_candidate_after(candidate_ids: &[u64], after: Option<u64>) -> usize {
    let Some(after) = after else {
        return 0;
    };
    match candidate_ids.binary_search(&after) {
        Ok(index) => index + 1,
        Err(index) => index,
    }
}

fn page_limit(page: &PageRequest) -> usize {
    page.limit.unwrap_or(0)
}

fn page_verify_target(limit: usize) -> usize {
    if limit == 0 {
        usize::MAX
    } else {
        limit.saturating_add(1)
    }
}

fn finalize_verified_page(
    mut ids: Vec<u64>,
    mut nodes: Vec<NodeRecord>,
    limit: usize,
) -> VerifiedNodePage {
    let next_cursor = if limit > 0 && ids.len() > limit {
        ids.truncate(limit);
        if !nodes.is_empty() {
            nodes.truncate(limit);
        }
        ids.last().copied()
    } else {
        None
    };

    VerifiedNodePage {
        ids,
        nodes,
        next_cursor,
    }
}

fn intersect_sorted_unique(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut intersection = Vec::with_capacity(left.len().min(right.len()));
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                intersection.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    intersection
}

fn intersect_candidate_sets(candidate_sets: &[Vec<u64>]) -> Vec<u64> {
    let mut iter = candidate_sets.iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut current = first.clone();
    for source in iter {
        current = intersect_sorted_unique(&current, source);
        if current.is_empty() {
            break;
        }
    }
    current
}

fn union_sorted_unique(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut union = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                union.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                union.push(right[right_index]);
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                union.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    union.extend_from_slice(&left[left_index..]);
    union.extend_from_slice(&right[right_index..]);
    union
}

fn union_candidate_sets(candidate_sets: &[Vec<u64>]) -> Vec<u64> {
    let mut iter = candidate_sets.iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut current = first.clone();
    for source in iter {
        current = union_sorted_unique(&current, source);
    }
    current
}

fn prop_value_contains_zero_float(value: &PropValue) -> bool {
    match value {
        PropValue::Float(value) => *value == 0.0,
        PropValue::Array(values) => values.iter().any(prop_value_contains_zero_float),
        PropValue::Map(values) => values.values().any(prop_value_contains_zero_float),
        _ => false,
    }
}

fn property_in_filter_matches(
    candidate: &PropValue,
    values: &[PropValue],
    value_keys: &[Vec<u8>],
) -> bool {
    if values.len() <= PROPERTY_IN_LINEAR_VERIFY_THRESHOLD || values.len() != value_keys.len() {
        return values
            .iter()
            .any(|value| prop_values_equal_for_filter(candidate, value));
    }

    let candidate_key = prop_value_canonical_bytes(candidate);
    match value_keys.binary_search(&candidate_key) {
        Ok(index) => prop_values_equal_for_filter(candidate, &values[index]),
        Err(_) if prop_value_contains_zero_float(candidate) => values
            .iter()
            .any(|value| prop_values_equal_for_filter(candidate, value)),
        Err(_) => false,
    }
}

fn node_filter_matches(filter: &NormalizedNodeFilter, node: &NodeRecord) -> bool {
    match filter {
        NormalizedNodeFilter::AlwaysTrue => true,
        NormalizedNodeFilter::AlwaysFalse => false,
        NormalizedNodeFilter::PropertyEquals { key, value } => {
            node.props
                .get(key)
                .is_some_and(|candidate| prop_values_equal_for_filter(candidate, value))
        }
        NormalizedNodeFilter::PropertyIn {
            key,
            values,
            value_keys,
        } => node
            .props
            .get(key)
            .is_some_and(|candidate| property_in_filter_matches(candidate, values, value_keys)),
        NormalizedNodeFilter::PropertyRange { key, lower, upper } => node
            .props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
        NormalizedNodeFilter::PropertyExists { key } => node.props.contains_key(key),
        NormalizedNodeFilter::PropertyMissing { key } => !node.props.contains_key(key),
        NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            node.updated_at >= *lower_ms && node.updated_at <= *upper_ms
        }
        NormalizedNodeFilter::And(children) => {
            children.iter().all(|child| node_filter_matches(child, node))
        }
        NormalizedNodeFilter::Or(children) => {
            children.iter().any(|child| node_filter_matches(child, node))
        }
        NormalizedNodeFilter::Not(child) => !node_filter_matches(child, node),
    }
}

fn query_node_matches(query: &NormalizedNodeQuery, node: &NodeRecord) -> bool {
    if query.type_id.is_some_and(|type_id| node.type_id != type_id) {
        return false;
    }
    if !query.ids.is_empty() && query.ids.binary_search(&node.id).is_err() {
        return false;
    }
    if !query.keys.is_empty() && query.keys.binary_search(&node.key).is_err() {
        return false;
    }

    node_filter_matches(&query.filter, node)
}

#[derive(Clone)]
struct PatternExecutionState {
    nodes: Vec<Option<u64>>,
    edges: Vec<Option<u64>>,
}

#[derive(Clone, Copy)]
struct PatternEdgeContext {
    source_id: u64,
    target_index: usize,
    target_id: Option<u64>,
    direction: Direction,
}

struct PatternUnnamedAccumulator {
    seen_edges: NodeIdSet,
    best_by_target: NodeIdMap<NeighborEntry>,
    best_bound_entry: Option<NeighborEntry>,
    exceeded: bool,
}

impl PatternUnnamedAccumulator {
    fn new() -> Self {
        Self {
            seen_edges: NodeIdSet::default(),
            best_by_target: NodeIdMap::default(),
            best_bound_entry: None,
            exceeded: false,
        }
    }

    fn is_done(&self) -> bool {
        self.best_bound_entry.is_some() || self.exceeded
    }
}

#[derive(Clone)]
struct PatternPendingEntry {
    state_index: usize,
    entry: NeighborEntry,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PatternMatchSortKey {
    anchor_node_id: u64,
    node_ids: Vec<u64>,
    edge_ids: Vec<u64>,
}

fn reverse_pattern_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn edge_post_filter_matches(
    predicates: &[EdgePostFilterPredicate],
    edge: &EdgeRecord,
) -> bool {
    predicates.iter().all(|predicate| match predicate {
        EdgePostFilterPredicate::PropertyEquals { key, value } => {
            edge.props.get(key).is_some_and(|candidate| candidate == value)
        }
        EdgePostFilterPredicate::PropertyRange { key, lower, upper } => edge
            .props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
    })
}

fn pattern_distinct_node_binding_ok(
    state: &PatternExecutionState,
    target_index: usize,
    candidate_id: u64,
) -> bool {
    state
        .nodes
        .iter()
        .enumerate()
        .all(|(index, bound)| index == target_index || *bound != Some(candidate_id))
}

fn pattern_match_from_state(
    query: &NormalizedGraphPatternQuery,
    state: &PatternExecutionState,
) -> QueryMatch {
    let mut nodes = BTreeMap::new();
    for (index, pattern) in query.nodes.iter().enumerate() {
        nodes.insert(
            pattern.alias.clone(),
            state.nodes[index].expect("complete pattern match must bind every node alias"),
        );
    }

    let mut edges = BTreeMap::new();
    for (index, pattern) in query.edges.iter().enumerate() {
        if let Some(alias) = pattern.alias.as_ref() {
            edges.insert(
                alias.clone(),
                state.edges[index].expect("named edge alias must be bound in a complete match"),
            );
        }
    }

    QueryMatch { nodes, edges }
}

fn pattern_match_sort_key(match_: &QueryMatch, anchor_alias: &str) -> PatternMatchSortKey {
    PatternMatchSortKey {
        anchor_node_id: *match_
            .nodes
            .get(anchor_alias)
            .expect("pattern match must contain anchor alias"),
        node_ids: match_.nodes.values().copied().collect(),
        edge_ids: match_.edges.values().copied().collect(),
    }
}

fn sort_pattern_matches(matches: &mut [QueryMatch], anchor_alias: &str) {
    matches.sort_by(|left, right| {
        pattern_match_sort_key(left, anchor_alias).cmp(&pattern_match_sort_key(right, anchor_alias))
    });
}

fn insert_bounded_pattern_match(
    matches: &mut Vec<QueryMatch>,
    candidate: QueryMatch,
    limit: usize,
    anchor_alias: &str,
) {
    if matches.contains(&candidate) {
        return;
    }
    matches.push(candidate);
    sort_pattern_matches(matches, anchor_alias);
    let keep = limit.saturating_add(1);
    if matches.len() > keep {
        matches.truncate(keep);
    }
}

#[allow(clippy::too_many_arguments)]
fn record_pattern_unnamed_entry(
    state: &PatternExecutionState,
    context: &PatternEdgeContext,
    reference_time: i64,
    deleted_nodes: &NodeIdSet,
    deleted_edges: &NodeIdSet,
    seen_edges: &mut NodeIdSet,
    best_by_target: &mut NodeIdMap<NeighborEntry>,
    best_bound_entry: &mut Option<NeighborEntry>,
    exceeded: &mut bool,
    edge_id: u64,
    neighbor_id: u64,
    edge_type_id: u32,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
) -> ControlFlow<()> {
    if !seen_edges.insert(edge_id) {
        return ControlFlow::Continue(());
    }
    if deleted_edges.contains(&edge_id) || deleted_nodes.contains(&neighbor_id) {
        return ControlFlow::Continue(());
    }
    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
        return ControlFlow::Continue(());
    }
    if let Some(target_id) = context.target_id {
        if neighbor_id != target_id {
            return ControlFlow::Continue(());
        }
        *best_bound_entry = Some(NeighborEntry {
            node_id: neighbor_id,
            edge_id,
            edge_type_id,
            weight,
            valid_from,
            valid_to,
        });
        return ControlFlow::Break(());
    }
    if !pattern_distinct_node_binding_ok(state, context.target_index, neighbor_id) {
        return ControlFlow::Continue(());
    }
    let entry = NeighborEntry {
        node_id: neighbor_id,
        edge_id,
        edge_type_id,
        weight,
        valid_from,
        valid_to,
    };
    match best_by_target.get_mut(&neighbor_id) {
        Some(existing) if entry.edge_id < existing.edge_id => *existing = entry,
        Some(_) => {}
        None => {
            best_by_target.insert(neighbor_id, entry);
            if best_by_target.len() > PATTERN_FRONTIER_BUDGET {
                *exceeded = true;
                return ControlFlow::Break(());
            }
        }
    }
    ControlFlow::Continue(())
}

impl ReadView {
    fn query_node_page_from_type_id_index(
        &self,
        query: &NormalizedNodeQuery,
        type_id: u32,
    ) -> Result<VerifiedNodePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });

        if query.ids.is_empty() {
            self.scan_type_ids_unfiltered(type_id, query.page.after, |node_id| {
                ids.push(node_id);
                if ids.len() >= target {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })?;
            return Ok(finalize_verified_page(ids, Vec::new(), limit));
        }

        self.scan_type_ids_unfiltered(type_id, query.page.after, |node_id| {
            if query.ids.binary_search(&node_id).is_ok() {
                ids.push(node_id);
                if ids.len() >= target {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        })?;

        Ok(finalize_verified_page(ids, Vec::new(), limit))
    }

    fn query_node_page_from_candidates(
        &self,
        candidate_ids: &[u64],
        query: &NormalizedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let start = first_candidate_after(candidate_ids, query.page.after);
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut nodes = Vec::with_capacity(if hydrate && limit > 0 { limit } else { 0 });

        for chunk in candidate_ids[start..].chunks(QUERY_VERIFY_CHUNK) {
            #[cfg(test)]
            self.note_final_verifier_record_reads(chunk.len());
            let chunk_nodes = self.get_nodes_raw(chunk)?;
            for (&node_id, node) in chunk.iter().zip(chunk_nodes.into_iter()) {
                let Some(node) = node.as_ref() else {
                    continue;
                };
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    continue;
                }
                if !query_node_matches(query, node) {
                    continue;
                }
                ids.push(node_id);
                if hydrate {
                    nodes.push(node.clone());
                }
                if ids.len() >= target {
                    return Ok(finalize_verified_page(ids, nodes, limit));
                }
            }
        }

        Ok(finalize_verified_page(ids, nodes, limit))
    }

    fn query_node_page_from_type_scan(
        &self,
        query: &NormalizedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
        let type_id = query
            .type_id
            .expect("normalized type-scan query must have type_id");
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut nodes = Vec::with_capacity(if hydrate && limit > 0 { limit } else { 0 });

        if !hydrate
            && policy_cutoffs.is_none()
            && query.filter.is_always_true()
            && query.keys.is_empty()
        {
            return self.query_node_page_from_type_id_index(query, type_id);
        }

        self.scan_nodes_by_type_filtered(
            type_id,
            query.page.after,
            chunk_limit,
            policy_cutoffs,
            |node_id, node| {
                if query_node_matches(query, node) {
                    ids.push(node_id);
                    if hydrate {
                        nodes.push(node.clone());
                    }
                    if ids.len() >= target {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;

        Ok(finalize_verified_page(ids, nodes, limit))
    }

    fn full_scan_source_node_ids(&self) -> Result<Vec<FullScanNodeSource<'_>>, EngineError> {
        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(FullScanNodeSource::Owned(
            self.memtable.visible_node_ids_at(self.snapshot_seq),
        ));
        for epoch in &self.immutable_epochs {
            sources.push(FullScanNodeSource::Owned(
                epoch.memtable.visible_node_ids_at(self.snapshot_seq),
            ));
        }
        for segment in &self.segments {
            sources.push(FullScanNodeSource::Segment(segment.as_ref()));
        }
        Ok(sources)
    }

    fn scan_full_node_ids_filtered<F>(
        &self,
        start_after: Option<u64>,
        chunk_limit: usize,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(u64, &NodeRecord) -> Result<ControlFlow<()>, EngineError>,
    {
        let sources = self.full_scan_source_node_ids()?;
        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter().enumerate() {
            let start = source.seek_after(start_after)?;
            if let Some(node_id) = source.get_id(start)? {
                heap.push(Reverse((node_id, source_index, start)));
            }
        }

        let mut chunk = Vec::with_capacity(chunk_limit.max(1));
        let mut last_seen = None;
        while let Some(Reverse((node_id, source_index, offset))) = heap.pop() {
            let next_offset = offset + 1;
            if let Some(next_id) = sources[source_index].get_id(next_offset)? {
                heap.push(Reverse((next_id, source_index, next_offset)));
            }

            if last_seen == Some(node_id) {
                continue;
            }
            last_seen = Some(node_id);
            chunk.push(node_id);
            if chunk.len() >= chunk_limit.max(1) {
                if self.visit_full_scan_chunk(&chunk, policy_cutoffs, &mut visitor)?.is_break() {
                    return Ok(());
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let _ = self.visit_full_scan_chunk(&chunk, policy_cutoffs, &mut visitor)?;
        }
        Ok(())
    }

    fn visit_full_scan_chunk<F>(
        &self,
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
            if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                continue;
            }
            if visitor(node_id, node)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    fn query_node_page_from_full_scan(
        &self,
        query: &NormalizedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut nodes = Vec::with_capacity(if hydrate && limit > 0 { limit } else { 0 });

        self.scan_full_node_ids_filtered(
            query.page.after,
            chunk_limit,
            policy_cutoffs,
            |node_id, node| {
                if query_node_matches(query, node) {
                    ids.push(node_id);
                    if hydrate {
                        nodes.push(node.clone());
                    }
                    if ids.len() >= target {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;

        Ok(finalize_verified_page(ids, nodes, limit))
    }

    fn materialize_node_candidate_source(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        source: &PlannedNodeCandidateSource,
    ) -> Result<CandidateMaterializationResult, EngineError> {
        let eager_cap = cap_context.source_cap(source.kind, query.page.limit, source.estimate);
        match &source.materialization {
            NodeCandidateMaterialization::Precomputed(ids) => {
                Ok(CandidateMaterializationResult::Ready {
                    ids: ids.clone(),
                    followups: Vec::new(),
                })
            }
            NodeCandidateMaterialization::KeyLookup => Ok(CandidateMaterializationResult::Ready {
                ids: self.key_lookup_candidate_ids(query)?,
                followups: Vec::new(),
            }),
            NodeCandidateMaterialization::PropertyEqualityIndex {
                index_id,
                key,
                value,
            } => {
                let (ids, followup) = if source
                    .estimate
                    .known_upper_bound()
                    .is_some_and(|count| count <= eager_cap as u64)
                    && source.estimate.can_use_uncapped_equality_materialization()
                {
                    self.ready_equality_candidate_ids_from_postings(
                        *index_id,
                        key,
                        value,
                        eager_cap + 1,
                    )?
                } else {
                    self.ready_equality_candidate_ids_raw_limited(
                        *index_id,
                        value,
                        eager_cap,
                    )?
                };
                let followups = materialization_followups(followup);
                let Some(ids) = ids else {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                };
                if ids.len() > eager_cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready { ids, followups })
                }
            }
            NodeCandidateMaterialization::PropertyRangeIndex {
                index_id,
                domain,
                lower,
                upper,
            } => {
                let (ids, followup) = self.ready_range_candidate_ids(
                    *index_id,
                    *domain,
                    lower.as_ref(),
                    upper.as_ref(),
                    eager_cap + 1,
                )?;
                let followups = materialization_followups(followup);
                let Some(ids) = ids else {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                };
                if ids.len() > eager_cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready { ids, followups })
                }
            }
            NodeCandidateMaterialization::TimestampIndex {
                type_id,
                lower_ms,
                upper_ms,
            } => {
                let ids = self.timestamp_candidate_ids(
                    *type_id,
                    *lower_ms,
                    *upper_ms,
                    eager_cap + 1,
                )?;
                if ids.len() > eager_cap {
                    Ok(CandidateMaterializationResult::TooBroad {
                        followups: Vec::new(),
                    })
                } else {
                    Ok(CandidateMaterializationResult::Ready {
                        ids,
                        followups: Vec::new(),
                    })
                }
            }
            NodeCandidateMaterialization::NodeTypeIndex
            | NodeCandidateMaterialization::FallbackTypeScan
            | NodeCandidateMaterialization::FallbackFullNodeScan => {
                Ok(CandidateMaterializationResult::TooBroad {
                    followups: Vec::new(),
                })
            }
        }
    }

    fn materialize_node_physical_plan(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        plan: &NodePhysicalPlan,
    ) -> Result<CandidateMaterializationResult, EngineError> {
        match plan {
            NodePhysicalPlan::Empty => Ok(CandidateMaterializationResult::Ready {
                ids: Vec::new(),
                followups: Vec::new(),
            }),
            NodePhysicalPlan::Source(source) => {
                self.materialize_node_candidate_source(query, cap_context, source)
            }
            NodePhysicalPlan::Intersect(inputs) => {
                let mut materialized = Vec::with_capacity(inputs.len());
                let mut followups = Vec::new();
                for input in inputs {
                    match self.materialize_node_physical_plan(query, cap_context, input)? {
                        CandidateMaterializationResult::Ready {
                            ids,
                            followups: mut input_followups,
                        } => {
                            materialized.push(ids);
                            followups.append(&mut input_followups);
                        }
                        CandidateMaterializationResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                Ok(CandidateMaterializationResult::Ready {
                    ids: intersect_candidate_sets(&materialized),
                    followups,
                })
            }
            NodePhysicalPlan::Union(inputs) => {
                let mut materialized = Vec::with_capacity(inputs.len());
                let mut followups = Vec::new();
                let mut total_len = 0usize;
                let union_cap = cap_context.union_total_cap(query.page.limit, plan.estimate());
                for input in inputs {
                    match self.materialize_node_physical_plan(query, cap_context, input)? {
                        CandidateMaterializationResult::Ready {
                            ids,
                            followups: mut input_followups,
                        } => {
                            total_len = total_len.saturating_add(ids.len());
                            followups.append(&mut input_followups);
                            if total_len > union_cap {
                                return Ok(CandidateMaterializationResult::TooBroad { followups });
                            }
                            materialized.push(ids);
                        }
                        CandidateMaterializationResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let union = union_candidate_sets(&materialized);
                if union.len() > union_cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready {
                        ids: union,
                        followups,
                    })
                }
            }
        }
    }

    fn query_node_page_from_source_driver(
        &self,
        source: &PlannedNodeCandidateSource,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedNodePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        match source.kind {
            NodeQueryCandidateSourceKind::NodeTypeIndex
            | NodeQueryCandidateSourceKind::FallbackTypeScan => {
                Ok((
                    self.query_node_page_from_type_scan(query, hydrate, policy_cutoffs)?,
                    Vec::new(),
                ))
            }
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => {
                Ok((
                    self.query_node_page_from_full_scan(query, hydrate, policy_cutoffs)?,
                    Vec::new(),
                ))
            }
            _ => {
                match self.materialize_node_candidate_source(query, cap_context, source)? {
                    CandidateMaterializationResult::Ready { ids, followups } => Ok((
                        self.query_node_page_from_candidates(
                            &ids,
                            query,
                            hydrate,
                            policy_cutoffs,
                        )?,
                        followups,
                    )),
                    CandidateMaterializationResult::TooBroad {
                        followups: mut materialization_followups,
                    } => {
                        let (page, mut fallback_followups) = self.query_node_page_from_legal_universe(
                            query,
                            cap_context,
                            hydrate,
                            policy_cutoffs,
                        )?;
                        materialization_followups.append(&mut fallback_followups);
                        Ok((page, materialization_followups))
                    }
                }
            }
        }
    }

    fn query_node_page_from_legal_universe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedNodePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        let mut plans = self.legal_universe_plans(query, true)?;
        if plans.is_empty() {
            return Err(EngineError::InvalidOperation(
                "node query requires type_id, ids, keys, or allow_full_scan".into(),
            ));
        }
        self.sort_physical_plans_by_selectivity(&mut plans);
        match plans.first().expect("legal universe plans must be non-empty") {
            NodePhysicalPlan::Source(source) => {
                self.query_node_page_from_source_driver(
                    source,
                    query,
                    cap_context,
                    hydrate,
                    policy_cutoffs,
                )
            }
            _ => unreachable!("legal universe plans are source drivers"),
        }
    }

    fn query_node_page_planned(
        &self,
        query: &NormalizedNodeQuery,
        planned: &PlannedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedNodePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        match &planned.driver {
            NodePhysicalPlan::Empty => Ok((
                VerifiedNodePage {
                    ids: Vec::new(),
                    nodes: Vec::new(),
                    next_cursor: None,
                },
                Vec::new(),
            )),
            NodePhysicalPlan::Source(source) => {
                self.query_node_page_from_source_driver(
                    source,
                    query,
                    planned.cap_context,
                    hydrate,
                    policy_cutoffs,
                )
            }
            plan => {
                match self.materialize_node_physical_plan(query, planned.cap_context, plan)? {
                    CandidateMaterializationResult::Ready { ids, followups } => Ok((
                        self.query_node_page_from_candidates(
                            &ids,
                            query,
                            hydrate,
                            policy_cutoffs,
                        )?,
                        followups,
                    )),
                    CandidateMaterializationResult::TooBroad {
                        followups: mut materialization_followups,
                    } => {
                        let (page, mut fallback_followups) = self.query_node_page_from_legal_universe(
                            query,
                            planned.cap_context,
                            hydrate,
                            policy_cutoffs,
                        )?;
                        materialization_followups.append(&mut fallback_followups);
                        Ok((page, materialization_followups))
                    }
                }
            }
        }
    }

    fn query_node_ids_outcome(
        &self,
        query: &NodeQuery,
    ) -> Result<QueryExecutionOutcome<QueryNodeIdsResult>, EngineError> {
        let normalized = self.normalize_node_query(query)?;
        let planned = self.plan_normalized_node_query(&normalized)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let (page, followups) =
            self.query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())?;
        let value = QueryNodeIdsResult {
            items: page.ids,
            next_cursor: page.next_cursor,
        };
        Ok(QueryExecutionOutcome { value, followups })
    }

    fn query_nodes_outcome(
        &self,
        query: &NodeQuery,
    ) -> Result<QueryExecutionOutcome<QueryNodesResult>, EngineError> {
        let normalized = self.normalize_node_query(query)?;
        let planned = self.plan_normalized_node_query(&normalized)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let (page, followups) =
            self.query_node_page_planned(&normalized, &planned, true, policy_cutoffs.as_ref())?;
        let value = QueryNodesResult {
            items: page.nodes,
            next_cursor: page.next_cursor,
        };
        Ok(QueryExecutionOutcome { value, followups })
    }

    fn pattern_edge_context(
        edge: &NormalizedEdgePattern,
        state: &PatternExecutionState,
    ) -> Result<PatternEdgeContext, EngineError> {
        let from_bound = state.nodes[edge.from_index].is_some();
        let to_bound = state.nodes[edge.to_index].is_some();
        let (source_index, target_index, direction) = if from_bound {
            (edge.from_index, edge.to_index, edge.direction)
        } else if to_bound {
            (
                edge.to_index,
                edge.from_index,
                reverse_pattern_direction(edge.direction),
            )
        } else {
            return Err(EngineError::InvalidOperation(
                "pattern expansion encountered an unbound edge".into(),
            ));
        };

        Ok(PatternEdgeContext {
            source_id: state.nodes[source_index]
                .expect("pattern expansion source alias must already be bound"),
            target_index,
            target_id: state.nodes[target_index],
            direction,
        })
    }

    fn pattern_verified_target_ids_by_index(
        &self,
        query: &NormalizedGraphPatternQuery,
        mut candidate_ids_by_index: Vec<Vec<u64>>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<NodeIdSet>, EngineError> {
        let mut verified_by_index = vec![NodeIdSet::default(); query.nodes.len()];
        for (target_index, candidate_ids) in candidate_ids_by_index.iter_mut().enumerate() {
            if candidate_ids.is_empty() {
                continue;
            }
            candidate_ids.sort_unstable();
            candidate_ids.dedup();
            let nodes = self.get_nodes_raw(candidate_ids)?;
            for (&node_id, node) in candidate_ids.iter().zip(nodes.iter()) {
                if let Some(node) = node.as_ref() {
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        continue;
                    }
                    if query_node_matches(&query.nodes[target_index].query, node) {
                        verified_by_index[target_index].insert(node_id);
                    }
                }
            }
        }
        Ok(verified_by_index)
    }

    #[allow(clippy::too_many_arguments)]
    fn flush_pattern_pending_entries(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        edge: &NormalizedEdgePattern,
        states: &[PatternExecutionState],
        contexts: &[PatternEdgeContext],
        pending: &mut Vec<PatternPendingEntry>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        unnamed_emitted_targets: &mut [NodeIdSet],
        unnamed_bound_satisfied: &mut [bool],
        on_next: &mut impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<ControlFlow<()>, EngineError> {
        if pending.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }

        let matching_edges = if edge.property_predicates.is_empty() {
            None
        } else {
            let mut edge_ids: Vec<u64> = pending
                .iter()
                .map(|pending| pending.entry.edge_id)
                .collect();
            edge_ids.sort_unstable();
            edge_ids.dedup();
            let edges = self.get_edges(&edge_ids)?;
            let mut matching_edges = NodeIdSet::default();
            for (&edge_id, edge_record) in edge_ids.iter().zip(edges.iter()) {
                if edge_record
                    .as_ref()
                    .is_some_and(|edge_record| edge_post_filter_matches(&edge.property_predicates, edge_record))
                {
                    matching_edges.insert(edge_id);
                }
            }
            Some(matching_edges)
        };

        let mut candidate_ids_by_index = vec![Vec::new(); query.nodes.len()];
        for pending_entry in pending.iter() {
            if matching_edges
                .as_ref()
                .is_some_and(|matching| !matching.contains(&pending_entry.entry.edge_id))
            {
                continue;
            }
            let context = contexts[pending_entry.state_index];
            if context.target_id.is_none() {
                candidate_ids_by_index[context.target_index].push(pending_entry.entry.node_id);
            }
        }
        let verified_targets = self.pattern_verified_target_ids_by_index(
            query,
            candidate_ids_by_index,
            policy_cutoffs,
        )?;

        for pending_entry in pending.drain(..) {
            if matching_edges
                .as_ref()
                .is_some_and(|matching| !matching.contains(&pending_entry.entry.edge_id))
            {
                continue;
            }

            let context = contexts[pending_entry.state_index];
            if context.target_id.is_none()
                && !verified_targets[context.target_index].contains(&pending_entry.entry.node_id)
            {
                continue;
            }

            if edge.alias.is_none() {
                if context.target_id.is_some() {
                    if unnamed_bound_satisfied[pending_entry.state_index] {
                        continue;
                    }
                    unnamed_bound_satisfied[pending_entry.state_index] = true;
                } else if !unnamed_emitted_targets[pending_entry.state_index]
                    .insert(pending_entry.entry.node_id)
                {
                    continue;
                }
            }

            let state = &states[pending_entry.state_index];
            let mut next = state.clone();
            if next.nodes[context.target_index].is_none() {
                next.nodes[context.target_index] = Some(pending_entry.entry.node_id);
            }
            next.edges[edge_index] = Some(pending_entry.entry.edge_id);
            if on_next(next)?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    #[allow(clippy::too_many_arguments)]
    fn record_pattern_general_pending_entry(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        edge: &NormalizedEdgePattern,
        states: &[PatternExecutionState],
        contexts: &[PatternEdgeContext],
        seen_edges: &mut [NodeIdSet],
        pending: &mut Vec<PatternPendingEntry>,
        unnamed_emitted_targets: &mut [NodeIdSet],
        unnamed_bound_satisfied: &mut [bool],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        on_next: &mut impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
        state_index: usize,
        edge_id: u64,
        neighbor_id: u64,
        edge_type_id: u32,
        weight: f32,
        valid_from: i64,
        valid_to: i64,
        reference_time: i64,
        deleted_nodes: &NodeIdSet,
        deleted_edges: &NodeIdSet,
    ) -> Result<ControlFlow<()>, EngineError> {
        if edge.alias.is_none() && unnamed_bound_satisfied[state_index] {
            return Ok(ControlFlow::Continue(()));
        }
        if !seen_edges[state_index].insert(edge_id) {
            return Ok(ControlFlow::Continue(()));
        }
        if deleted_edges.contains(&edge_id) || deleted_nodes.contains(&neighbor_id) {
            return Ok(ControlFlow::Continue(()));
        }
        if !is_edge_valid_at(valid_from, valid_to, reference_time) {
            return Ok(ControlFlow::Continue(()));
        }

        let state = &states[state_index];
        let context = contexts[state_index];
        if let Some(target_id) = context.target_id {
            if neighbor_id != target_id {
                return Ok(ControlFlow::Continue(()));
            }
        } else if !pattern_distinct_node_binding_ok(state, context.target_index, neighbor_id) {
            return Ok(ControlFlow::Continue(()));
        }

        pending.push(PatternPendingEntry {
            state_index,
            entry: NeighborEntry {
                node_id: neighbor_id,
                edge_id,
                edge_type_id,
                weight,
                valid_from,
                valid_to,
            },
        });

        let flush_now = pending.len() >= QUERY_VERIFY_CHUNK
            || (edge.alias.is_none() && context.target_id.is_some());
        if flush_now {
            return self.flush_pattern_pending_entries(
                query,
                edge_index,
                edge,
                states,
                contexts,
                pending,
                policy_cutoffs,
                unnamed_emitted_targets,
                unnamed_bound_satisfied,
                on_next,
            );
        }

        Ok(ControlFlow::Continue(()))
    }

    fn execute_pattern_unnamed_constraint_frontier_for_each(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        states: Vec<PatternExecutionState>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        mut on_next: impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<(), EngineError> {
        let edge = &query.edges[edge_index];
        let (deleted_nodes, deleted_edges) = self.collect_tombstones();
        let type_filter = edge.type_filter.as_deref();
        let mut contexts = Vec::with_capacity(states.len());

        for state in &states {
            contexts.push(Self::pattern_edge_context(edge, state)?);
        }

        let mut accumulators: Vec<PatternUnnamedAccumulator> = states
            .iter()
            .map(|_| PatternUnnamedAccumulator::new())
            .collect();

        for (state_index, state) in states.iter().enumerate() {
            let context = contexts[state_index];
            let accumulator = &mut accumulators[state_index];
            let flow = self.memtable.for_each_adj_entry_at(
                context.source_id,
                context.direction,
                type_filter,
                self.snapshot_seq,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    record_pattern_unnamed_entry(
                        state,
                        &context,
                        reference_time,
                        &deleted_nodes,
                        &deleted_edges,
                        &mut accumulator.seen_edges,
                        &mut accumulator.best_by_target,
                        &mut accumulator.best_bound_entry,
                        &mut accumulator.exceeded,
                        edge_id,
                        neighbor_id,
                        0,
                        weight,
                        valid_from,
                        valid_to,
                    )
                },
            );
            if accumulator.exceeded {
                return Err(EngineError::InvalidOperation(format!(
                    "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                )));
            }
            if flow.is_break() && accumulator.best_bound_entry.is_some() {
                continue;
            }
        }

        for epoch in &self.immutable_epochs {
            for (state_index, state) in states.iter().enumerate() {
                if accumulators[state_index].is_done() {
                    continue;
                }
                let context = contexts[state_index];
                let accumulator = &mut accumulators[state_index];
                let flow = epoch.memtable.for_each_adj_entry_at(
                    context.source_id,
                    context.direction,
                    type_filter,
                    self.snapshot_seq,
                    &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                        record_pattern_unnamed_entry(
                            state,
                            &context,
                            reference_time,
                            &deleted_nodes,
                            &deleted_edges,
                            &mut accumulator.seen_edges,
                            &mut accumulator.best_by_target,
                            &mut accumulator.best_bound_entry,
                            &mut accumulator.exceeded,
                            edge_id,
                            neighbor_id,
                            0,
                            weight,
                            valid_from,
                            valid_to,
                        )
                    },
                );
                if accumulator.exceeded {
                    return Err(EngineError::InvalidOperation(format!(
                        "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                    )));
                }
                if flow.is_break() && accumulator.best_bound_entry.is_some() {
                    continue;
                }
            }
        }

        for segment in &self.segments {
            for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
                let mut source_ids = Vec::new();
                let mut context_indices_by_source: NodeIdMap<Vec<usize>> = NodeIdMap::default();
                for (state_index, context) in contexts.iter().enumerate() {
                    if context.direction != direction || accumulators[state_index].is_done() {
                        continue;
                    }
                    context_indices_by_source
                        .entry(context.source_id)
                        .or_default()
                        .push(state_index);
                    source_ids.push(context.source_id);
                }
                if source_ids.is_empty() {
                    continue;
                }
                source_ids.sort_unstable();
                source_ids.dedup();
                let mut remaining_contexts = context_indices_by_source
                    .values()
                    .map(Vec::len)
                    .sum::<usize>();

                let flow = segment.for_each_adj_posting_batch(
                    &source_ids,
                    direction,
                    type_filter,
                    &mut |queried_node_id, edge_id, neighbor_id, weight, valid_from, valid_to| {
                        let Some(state_indices) = context_indices_by_source.get(&queried_node_id)
                        else {
                            return ControlFlow::Continue(());
                        };
                        for &state_index in state_indices {
                            if accumulators[state_index].is_done() {
                                continue;
                            }
                            let context = contexts[state_index];
                            let accumulator = &mut accumulators[state_index];
                            let was_done = accumulator.is_done();
                            let flow = record_pattern_unnamed_entry(
                                &states[state_index],
                                &context,
                                reference_time,
                                &deleted_nodes,
                                &deleted_edges,
                                &mut accumulator.seen_edges,
                                &mut accumulator.best_by_target,
                                &mut accumulator.best_bound_entry,
                                &mut accumulator.exceeded,
                                edge_id,
                                neighbor_id,
                                0,
                                weight,
                                valid_from,
                                valid_to,
                            );
                            if !was_done && accumulator.is_done() {
                                remaining_contexts = remaining_contexts.saturating_sub(1);
                            }
                            if accumulator.exceeded {
                                return ControlFlow::Break(());
                            }
                            if flow.is_break() && remaining_contexts == 0 {
                                return ControlFlow::Break(());
                            }
                        }
                        if remaining_contexts == 0 {
                            return ControlFlow::Break(());
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if flow.is_break() && accumulators.iter().any(|accumulator| accumulator.exceeded)
                {
                    return Err(EngineError::InvalidOperation(format!(
                        "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                    )));
                }
            }
        }

        let mut pending_entries = 0usize;
        let mut candidate_ids_by_index = vec![Vec::new(); query.nodes.len()];
        for (context, accumulator) in contexts.iter().zip(accumulators.iter()) {
            if accumulator.exceeded {
                return Err(EngineError::InvalidOperation(format!(
                    "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                )));
            }
            if accumulator.best_bound_entry.is_some() {
                pending_entries = pending_entries.saturating_add(1);
            } else {
                pending_entries = pending_entries.saturating_add(accumulator.best_by_target.len());
                if context.target_id.is_none() {
                    candidate_ids_by_index[context.target_index]
                        .extend(accumulator.best_by_target.keys().copied());
                }
            }
            if pending_entries > PATTERN_FRONTIER_BUDGET {
                return Err(EngineError::InvalidOperation(format!(
                    "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                )));
            }
        }

        let verified_targets =
            self.pattern_verified_target_ids_by_index(query, candidate_ids_by_index, policy_cutoffs)?;

        for ((state, context), accumulator) in states
            .into_iter()
            .zip(contexts.into_iter())
            .zip(accumulators.into_iter())
        {
            let mut entries: Vec<NeighborEntry> =
                if let Some(entry) = accumulator.best_bound_entry {
                    vec![entry]
                } else {
                    accumulator.best_by_target.into_values().collect()
                };
            entries.sort_by_key(|entry| (entry.node_id, entry.edge_id));
            for entry in entries {
                if context.target_id.is_none()
                    && !verified_targets[context.target_index].contains(&entry.node_id)
                {
                    continue;
                }
                let mut next = state.clone();
                if next.nodes[context.target_index].is_none() {
                    next.nodes[context.target_index] = Some(entry.node_id);
                }
                next.edges[edge_index] = Some(entry.edge_id);
                if on_next(next)?.is_break() {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn execute_pattern_general_edge_frontier_for_each(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        states: Vec<PatternExecutionState>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        mut on_next: impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<(), EngineError> {
        let edge = &query.edges[edge_index];
        let (deleted_nodes, deleted_edges) = self.collect_tombstones();
        let type_filter = edge.type_filter.as_deref();
        let mut contexts = Vec::with_capacity(states.len());
        for state in &states {
            contexts.push(Self::pattern_edge_context(edge, state)?);
        }

        let mut seen_edges: Vec<NodeIdSet> =
            states.iter().map(|_| NodeIdSet::default()).collect();
        let mut pending = Vec::with_capacity(QUERY_VERIFY_CHUNK);
        let mut unnamed_emitted_targets: Vec<NodeIdSet> =
            states.iter().map(|_| NodeIdSet::default()).collect();
        let mut unnamed_bound_satisfied = vec![false; states.len()];

        for (state_index, context) in contexts.iter().copied().enumerate() {
            if edge.alias.is_none() && unnamed_bound_satisfied[state_index] {
                continue;
            }
            let mut stream_error = None;
            let mut satisfied_this_walk = false;
            let flow = self.memtable.for_each_adj_entry_at(
                context.source_id,
                context.direction,
                type_filter,
                self.snapshot_seq,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    let was_satisfied = unnamed_bound_satisfied[state_index];
                    match self.record_pattern_general_pending_entry(
                        query,
                        edge_index,
                        edge,
                        &states,
                        &contexts,
                        &mut seen_edges,
                        &mut pending,
                        &mut unnamed_emitted_targets,
                        &mut unnamed_bound_satisfied,
                        policy_cutoffs,
                        &mut on_next,
                        state_index,
                        edge_id,
                        neighbor_id,
                        0,
                        weight,
                        valid_from,
                        valid_to,
                        reference_time,
                        &deleted_nodes,
                        &deleted_edges,
                    ) {
                        Ok(flow) => {
                            if flow.is_break() {
                                return flow;
                            }
                            if edge.alias.is_none()
                                && context.target_id.is_some()
                                && !was_satisfied
                                && unnamed_bound_satisfied[state_index]
                            {
                                satisfied_this_walk = true;
                                return ControlFlow::Break(());
                            }
                            ControlFlow::Continue(())
                        }
                        Err(error) => {
                            stream_error = Some(error);
                            ControlFlow::Break(())
                        }
                    }
                },
            );
            if let Some(error) = stream_error {
                return Err(error);
            }
            if flow.is_break() && !satisfied_this_walk {
                return Ok(());
            }
        }

        for epoch in &self.immutable_epochs {
            for (state_index, context) in contexts.iter().copied().enumerate() {
                if edge.alias.is_none() && unnamed_bound_satisfied[state_index] {
                    continue;
                }
                let mut stream_error = None;
                let mut satisfied_this_walk = false;
                let flow = epoch.memtable.for_each_adj_entry_at(
                    context.source_id,
                    context.direction,
                    type_filter,
                    self.snapshot_seq,
                    &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                        let was_satisfied = unnamed_bound_satisfied[state_index];
                        match self.record_pattern_general_pending_entry(
                            query,
                            edge_index,
                            edge,
                            &states,
                            &contexts,
                            &mut seen_edges,
                            &mut pending,
                            &mut unnamed_emitted_targets,
                            &mut unnamed_bound_satisfied,
                            policy_cutoffs,
                            &mut on_next,
                            state_index,
                            edge_id,
                            neighbor_id,
                            0,
                            weight,
                            valid_from,
                            valid_to,
                            reference_time,
                            &deleted_nodes,
                            &deleted_edges,
                        ) {
                            Ok(flow) => {
                                if flow.is_break() {
                                    return flow;
                                }
                                if edge.alias.is_none()
                                    && context.target_id.is_some()
                                    && !was_satisfied
                                    && unnamed_bound_satisfied[state_index]
                                {
                                    satisfied_this_walk = true;
                                    return ControlFlow::Break(());
                                }
                                ControlFlow::Continue(())
                            }
                            Err(error) => {
                                stream_error = Some(error);
                                ControlFlow::Break(())
                            }
                        }
                    },
                );
                if let Some(error) = stream_error {
                    return Err(error);
                }
                if flow.is_break() && !satisfied_this_walk {
                    return Ok(());
                }
            }
        }

        for segment in &self.segments {
            for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
                let mut source_ids = Vec::new();
                let mut context_indices_by_source: NodeIdMap<Vec<usize>> = NodeIdMap::default();
                for (state_index, context) in contexts.iter().enumerate() {
                    if context.direction != direction {
                        continue;
                    }
                    if edge.alias.is_none() && unnamed_bound_satisfied[state_index] {
                        continue;
                    }
                    context_indices_by_source
                        .entry(context.source_id)
                        .or_default()
                        .push(state_index);
                    source_ids.push(context.source_id);
                }
                if source_ids.is_empty() {
                    continue;
                }
                source_ids.sort_unstable();
                source_ids.dedup();
                let mut remaining_target_bound = if edge.alias.is_none() {
                    contexts
                        .iter()
                        .enumerate()
                        .filter(|(state_index, context)| {
                            context.direction == direction
                                && context.target_id.is_some()
                                && !unnamed_bound_satisfied[*state_index]
                        })
                        .count()
                } else {
                    0
                };
                let stop_when_target_bound = remaining_target_bound > 0;

                let mut stream_error = None;
                let flow = segment.for_each_adj_posting_batch(
                    &source_ids,
                    direction,
                    type_filter,
                    &mut |queried_node_id, edge_id, neighbor_id, weight, valid_from, valid_to| {
                        let Some(state_indices) = context_indices_by_source.get(&queried_node_id)
                        else {
                            return ControlFlow::Continue(());
                        };
                        for &state_index in state_indices {
                            if edge.alias.is_none() && unnamed_bound_satisfied[state_index] {
                                continue;
                            }
                            let was_satisfied = unnamed_bound_satisfied[state_index];
                            match self.record_pattern_general_pending_entry(
                                query,
                                edge_index,
                                edge,
                                &states,
                                &contexts,
                                &mut seen_edges,
                                &mut pending,
                                &mut unnamed_emitted_targets,
                                &mut unnamed_bound_satisfied,
                                policy_cutoffs,
                                &mut on_next,
                                state_index,
                                edge_id,
                                neighbor_id,
                                0,
                                weight,
                                valid_from,
                                valid_to,
                                reference_time,
                                &deleted_nodes,
                                &deleted_edges,
                            ) {
                                Ok(ControlFlow::Continue(())) => {
                                    if edge.alias.is_none()
                                        && contexts[state_index].target_id.is_some()
                                        && !was_satisfied
                                        && unnamed_bound_satisfied[state_index]
                                    {
                                        remaining_target_bound =
                                            remaining_target_bound.saturating_sub(1);
                                    }
                                }
                                Ok(ControlFlow::Break(())) => return ControlFlow::Break(()),
                                Err(error) => {
                                    stream_error = Some(error);
                                    return ControlFlow::Break(());
                                }
                            }
                        }
                        if stop_when_target_bound && remaining_target_bound == 0 {
                            return ControlFlow::Break(());
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(error) = stream_error {
                    return Err(error);
                }
                if flow.is_break() {
                    return Ok(());
                }
            }
        }

        if self
            .flush_pattern_pending_entries(
                query,
                edge_index,
                edge,
                &states,
                &contexts,
                &mut pending,
                policy_cutoffs,
                &mut unnamed_emitted_targets,
                &mut unnamed_bound_satisfied,
                &mut on_next,
            )?
            .is_break()
        {
            return Ok(());
        }

        Ok(())
    }

    fn execute_pattern_edge_frontier_for_each(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        states: Vec<PatternExecutionState>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        on_next: impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<(), EngineError> {
        if states.is_empty() {
            return Ok(());
        }

        let edge = &query.edges[edge_index];
        if edge.alias.is_none() && edge.property_predicates.is_empty() {
            return self.execute_pattern_unnamed_constraint_frontier_for_each(
                query,
                edge_index,
                states,
                reference_time,
                policy_cutoffs,
                on_next,
            );
        }

        self.execute_pattern_general_edge_frontier_for_each(
            query,
            edge_index,
            states,
            reference_time,
            policy_cutoffs,
            on_next,
        )
    }

    fn execute_pattern_edge_frontier(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        states: Vec<PatternExecutionState>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<PatternExecutionState>, EngineError> {
        let mut next_states = Vec::new();
        let mut exceeded = false;
        self.execute_pattern_edge_frontier_for_each(
            query,
            edge_index,
            states,
            reference_time,
            policy_cutoffs,
            |next| {
                if next_states.len() >= PATTERN_FRONTIER_BUDGET {
                    exceeded = true;
                    return Ok(ControlFlow::Break(()));
                }
                next_states.push(next);
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if exceeded {
            return Err(EngineError::InvalidOperation(format!(
                "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
            )));
        }
        Ok(next_states)
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_pattern_matches_from_final_edge(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        states: Vec<PatternExecutionState>,
        matches: &mut Vec<QueryMatch>,
        anchor_alias: &str,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(), EngineError> {
        self.execute_pattern_edge_frontier_for_each(
            query,
            edge_index,
            states,
            reference_time,
            policy_cutoffs,
            |next| {
                insert_bounded_pattern_match(
                    matches,
                    pattern_match_from_state(query, &next),
                    query.limit,
                    anchor_alias,
                );
                Ok(ControlFlow::Continue(()))
            },
        )
    }

    fn query_pattern_planned(
        &self,
        query: &NormalizedGraphPatternQuery,
        planned: &PlannedPatternQuery,
    ) -> Result<QueryExecutionOutcome<QueryPatternResult>, EngineError> {
        match query.order {
            PatternOrder::AnchorThenAliasesAsc => {}
        }

        let anchor = &query.nodes[planned.anchor_index];
        let policy_cutoffs = self.query_policy_cutoffs();
        let pattern_reference_time = query.at_epoch.unwrap_or_else(now_millis);
        let mut anchor_query = anchor.query.clone();
        anchor_query.page.limit = Some(PATTERN_FRONTIER_BUDGET);
        let (anchor_page, followups) = self.query_node_page_planned(
            &anchor_query,
            &planned.anchor_plan,
            false,
            policy_cutoffs.as_ref(),
        )?;
        if anchor_page.next_cursor.is_some() {
            return Err(EngineError::InvalidOperation(format!(
                "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
            )));
        }
        let anchor_ids = anchor_page.ids;
        let mut frontier = Vec::with_capacity(anchor_ids.len());
        for anchor_id in anchor_ids {
            let mut state = PatternExecutionState {
                nodes: vec![None; query.nodes.len()],
                edges: vec![None; query.edges.len()],
            };
            state.nodes[planned.anchor_index] = Some(anchor_id);
            frontier.push(state);
            if frontier.len() > PATTERN_FRONTIER_BUDGET {
                return Err(EngineError::InvalidOperation(format!(
                    "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                )));
            }
        }

        let sort_anchor_alias = &planned.sort_anchor_alias;
        let mut matches = Vec::with_capacity(query.limit.saturating_add(1));
        let Some((&final_edge_index, prefix_order)) = planned.expansion_order.split_last() else {
            for state in frontier {
                insert_bounded_pattern_match(
                    &mut matches,
                    pattern_match_from_state(query, &state),
                    query.limit,
                    sort_anchor_alias,
                );
            }
            sort_pattern_matches(&mut matches, sort_anchor_alias);
            let truncated = matches.len() > query.limit;
            matches.truncate(query.limit);
            return Ok(QueryExecutionOutcome {
                value: QueryPatternResult { matches, truncated },
                followups,
            });
        };

        for &edge_index in prefix_order {
            frontier = self.execute_pattern_edge_frontier(
                query,
                edge_index,
                frontier,
                pattern_reference_time,
                policy_cutoffs.as_ref(),
            )?;
            if frontier.is_empty() {
                break;
            }
        }

        if !frontier.is_empty() {
            self.collect_pattern_matches_from_final_edge(
                query,
                final_edge_index,
                frontier,
                &mut matches,
                sort_anchor_alias,
                pattern_reference_time,
                policy_cutoffs.as_ref(),
            )?;
        }

        sort_pattern_matches(&mut matches, sort_anchor_alias);
        let truncated = matches.len() > query.limit;
        matches.truncate(query.limit);
        Ok(QueryExecutionOutcome {
            value: QueryPatternResult { matches, truncated },
            followups,
        })
    }

    fn query_pattern_outcome(
        &self,
        query: &GraphPatternQuery,
    ) -> Result<QueryExecutionOutcome<QueryPatternResult>, EngineError> {
        let normalized = self.normalize_pattern_query(query)?;
        let planned = self.plan_normalized_pattern_query(&normalized)?;
        self.query_pattern_planned(&normalized, &planned)
    }
}
