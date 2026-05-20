const QUERY_VERIFY_CHUNK: usize = 256;
const PROPERTY_IN_LINEAR_VERIFY_THRESHOLD: usize = 16;
const PATTERN_FRONTIER_BUDGET: usize = 65_536;
const EDGE_INTERSECTION_TINY_SET: usize = 64;

struct QueryExecutionOutcome<T> {
    value: T,
    followups: Vec<SecondaryIndexReadFollowup>,
}

struct VerifiedNodePage {
    ids: Vec<u64>,
    nodes: Vec<NodeRecord>,
    next_cursor: Option<u64>,
}

struct VerifiedEdgePage {
    ids: Vec<u64>,
    edges: Vec<EdgeRecord>,
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

enum FullScanEdgeSource<'a> {
    Memtable {
        memtable: &'a Memtable,
        snapshot_seq: u64,
        next_after: Option<u64>,
    },
    Segment {
        segment: &'a SegmentReader,
        next: usize,
    },
}

enum LabelEdgeSource<'a> {
    Memtable {
        memtable: &'a Memtable,
        snapshot_seq: u64,
        label_id: u32,
        next_after: Option<u64>,
    },
    Segment {
        segment: &'a SegmentReader,
        posting: SegmentLabelPosting,
        next: usize,
    },
}

enum EndpointEdgeSource<'a> {
    Memtable(MemtableEndpointEdgeSource<'a>),
    Segment(SegmentEndpointEdgeSource<'a>),
}

#[derive(Clone, Copy)]
enum MemtableEndpointDirection {
    Outgoing,
    Incoming,
}

struct MemtableEndpointCursor<'a> {
    memtable: &'a Memtable,
    node_id: u64,
    direction: MemtableEndpointDirection,
    label_filter_ids: Option<&'a [u32]>,
    snapshot_seq: u64,
    next_after: Option<u64>,
}

struct MemtableEndpointEdgeSource<'a> {
    cursors: Vec<MemtableEndpointCursor<'a>>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    last_seen: Option<u64>,
}

struct SegmentEndpointEdgeSource<'a> {
    segment: &'a SegmentReader,
    cursors: Vec<SegmentAdjPostingCursor>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    last_seen: Option<u64>,
}

#[derive(Default)]
struct EdgeEndpointVisibilityCache {
    visible: NodeIdMap<bool>,
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

impl<'a> FullScanEdgeSource<'a> {
    fn memtable(
        memtable: &'a Memtable,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> FullScanEdgeSource<'a> {
        FullScanEdgeSource::Memtable {
            memtable,
            snapshot_seq,
            next_after: after,
        }
    }

    fn segment(
        segment: &'a SegmentReader,
        after: Option<u64>,
    ) -> Result<FullScanEdgeSource<'a>, EngineError> {
        let next = match after {
            Some(after) => {
                let mut lo = 0usize;
                let mut hi = segment.edge_meta_count() as usize;
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let (edge_id, ..) = segment.edge_meta_at(mid)?;
                    if edge_id <= after {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo
            }
            None => 0,
        };
        Ok(FullScanEdgeSource::Segment { segment, next })
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        match self {
            FullScanEdgeSource::Memtable {
                memtable,
                snapshot_seq,
                next_after,
            } => {
                let edge_id = memtable.next_visible_edge_id_after(*snapshot_seq, *next_after);
                if let Some(edge_id) = edge_id {
                    *next_after = Some(edge_id);
                }
                Ok(edge_id)
            }
            FullScanEdgeSource::Segment { segment, next } => {
                if *next >= segment.edge_meta_count() as usize {
                    return Ok(None);
                }
                let (edge_id, ..) = segment.edge_meta_at(*next)?;
                *next += 1;
                Ok(Some(edge_id))
            }
        }
    }
}

impl<'a> LabelEdgeSource<'a> {
    fn memtable(
        memtable: &'a Memtable,
        snapshot_seq: u64,
        label_id: u32,
        after: Option<u64>,
    ) -> LabelEdgeSource<'a> {
        LabelEdgeSource::Memtable {
            memtable,
            snapshot_seq,
            label_id,
            next_after: after,
        }
    }

    fn segment(
        segment: &'a SegmentReader,
        posting: SegmentLabelPosting,
        after: Option<u64>,
    ) -> Result<LabelEdgeSource<'a>, EngineError> {
        let next = match after {
            Some(after) => segment.edge_label_id_lower_bound_posting(posting, after)?,
            None => 0,
        };
        Ok(LabelEdgeSource::Segment {
            segment,
            posting,
            next,
        })
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        match self {
            LabelEdgeSource::Memtable {
                memtable,
                snapshot_seq,
                label_id,
                next_after,
            } => {
                let edge_id =
                    memtable.next_visible_edge_by_label_id_after(*label_id, *snapshot_seq, *next_after);
                if let Some(edge_id) = edge_id {
                    *next_after = Some(edge_id);
                }
                Ok(edge_id)
            }
            LabelEdgeSource::Segment {
                segment,
                posting,
                next,
            } => {
                let edge_id = segment.edge_label_id_at_posting(*posting, *next)?;
                if edge_id.is_some() {
                    *next += 1;
                }
                Ok(edge_id)
            }
        }
    }
}

impl<'a> EndpointEdgeSource<'a> {
    fn memtable(
        memtable: &'a Memtable,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'a [u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Self {
        Self::Memtable(MemtableEndpointEdgeSource::new(
            memtable,
            node_ids,
            direction,
            label_filter_ids,
            snapshot_seq,
            after,
        ))
    }

    fn segment(
        segment: &'a SegmentReader,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        after: Option<u64>,
    ) -> Result<Self, EngineError> {
        Ok(Self::Segment(SegmentEndpointEdgeSource::new(
            segment,
            node_ids,
            direction,
            label_filter_ids,
            after,
        )?))
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        match self {
            Self::Memtable(source) => Ok(source.next_id()),
            Self::Segment(source) => source.next_id(),
        }
    }
}

impl<'a> MemtableEndpointEdgeSource<'a> {
    fn new(
        memtable: &'a Memtable,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'a [u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Self {
        let mut cursors = Vec::new();
        for &node_id in node_ids {
            match direction {
                Direction::Outgoing => cursors.push(MemtableEndpointCursor {
                    memtable,
                    node_id,
                    direction: MemtableEndpointDirection::Outgoing,
                    label_filter_ids,
                    snapshot_seq,
                    next_after: after,
                }),
                Direction::Incoming => cursors.push(MemtableEndpointCursor {
                    memtable,
                    node_id,
                    direction: MemtableEndpointDirection::Incoming,
                    label_filter_ids,
                    snapshot_seq,
                    next_after: after,
                }),
                Direction::Both => {
                    cursors.push(MemtableEndpointCursor {
                        memtable,
                        node_id,
                        direction: MemtableEndpointDirection::Outgoing,
                        label_filter_ids,
                        snapshot_seq,
                        next_after: after,
                    });
                    cursors.push(MemtableEndpointCursor {
                        memtable,
                        node_id,
                        direction: MemtableEndpointDirection::Incoming,
                        label_filter_ids,
                        snapshot_seq,
                        next_after: after,
                    });
                }
            }
        }

        let mut source = Self {
            cursors,
            heap: BinaryHeap::new(),
            last_seen: None,
        };
        for cursor_index in 0..source.cursors.len() {
            if let Some(edge_id) = source.next_cursor_id(cursor_index) {
                source.heap.push(Reverse((edge_id, cursor_index)));
            }
        }
        source
    }

    fn next_cursor_id(&mut self, cursor_index: usize) -> Option<u64> {
        let cursor = &mut self.cursors[cursor_index];
        let edge_id = match cursor.direction {
            MemtableEndpointDirection::Outgoing => cursor.memtable.next_visible_edge_from_endpoint_after(
                cursor.node_id,
                cursor.label_filter_ids,
                cursor.snapshot_seq,
                cursor.next_after,
            ),
            MemtableEndpointDirection::Incoming => cursor.memtable.next_visible_edge_to_endpoint_after(
                cursor.node_id,
                cursor.label_filter_ids,
                cursor.snapshot_seq,
                cursor.next_after,
            ),
        };
        if let Some(edge_id) = edge_id {
            cursor.next_after = Some(edge_id);
        }
        edge_id
    }

    fn next_id(&mut self) -> Option<u64> {
        while let Some(Reverse((edge_id, cursor_index))) = self.heap.pop() {
            if let Some(next_id) = self.next_cursor_id(cursor_index) {
                self.heap.push(Reverse((next_id, cursor_index)));
            }
            if self.last_seen == Some(edge_id) {
                continue;
            }
            self.last_seen = Some(edge_id);
            return Some(edge_id);
        }
        None
    }
}

impl<'a> SegmentEndpointEdgeSource<'a> {
    fn new(
        segment: &'a SegmentReader,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        after: Option<u64>,
    ) -> Result<Self, EngineError> {
        let mut cursors = segment.endpoint_adj_posting_cursors(node_ids, direction, label_filter_ids)?;
        let mut heap = BinaryHeap::new();
        for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
            while let Some(edge_id) = segment.next_adj_posting_edge_id(cursor)? {
                if after.is_none_or(|after| edge_id > after) {
                    heap.push(Reverse((edge_id, cursor_index)));
                    break;
                }
            }
        }
        Ok(Self {
            segment,
            cursors,
            heap,
            last_seen: None,
        })
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        while let Some(Reverse((edge_id, cursor_index))) = self.heap.pop() {
            if let Some(next_id) =
                self.segment
                    .next_adj_posting_edge_id(&mut self.cursors[cursor_index])?
            {
                self.heap.push(Reverse((next_id, cursor_index)));
            }
            if self.last_seen == Some(edge_id) {
                continue;
            }
            self.last_seen = Some(edge_id);
            return Ok(Some(edge_id));
        }
        Ok(None)
    }
}

impl EdgeEndpointVisibilityCache {
    fn ensure_endpoint_ids(
        &mut self,
        sources: &SourceList<'_>,
        endpoint_ids: &[u64],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(), EngineError> {
        let mut missing = Vec::new();
        for &endpoint_id in endpoint_ids {
            if !self.visible.contains_key(&endpoint_id) {
                missing.push(endpoint_id);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        missing.sort_unstable();
        missing.dedup();
        let states = sources.find_node_visibility_meta(&missing)?;
        for (&endpoint_id, state) in missing.iter().zip(states.iter()) {
            let visible = match state {
                NodeVisibilityState::Live(meta) => policy_cutoffs.is_none_or(|cutoffs| {
                    !cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight)
                }),
                NodeVisibilityState::Deleted | NodeVisibilityState::Missing => false,
            };
            self.visible.insert(endpoint_id, visible);
        }
        Ok(())
    }

    fn ensure_edge_endpoints(
        &mut self,
        sources: &SourceList<'_>,
        metas: &[EdgeMetadataCandidate],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(), EngineError> {
        let mut endpoint_ids = Vec::with_capacity(metas.len().saturating_mul(2));
        for meta in metas {
            endpoint_ids.push(meta.from);
            endpoint_ids.push(meta.to);
        }
        self.ensure_endpoint_ids(sources, &endpoint_ids, policy_cutoffs)
    }

    fn edge_endpoints_visible(&self, meta: EdgeMetadataCandidate) -> bool {
        self.visible.get(&meta.from).copied().unwrap_or(false)
            && self.visible.get(&meta.to).copied().unwrap_or(false)
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

fn edge_plan_is_filter_source(plan: &EdgePhysicalPlan) -> bool {
    match plan {
        EdgePhysicalPlan::Source(source) => matches!(
            source.kind,
            EdgeQueryCandidateSourceKind::EdgeWeightIndex
                | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
                | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
                | EdgeQueryCandidateSourceKind::EdgeValidToIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
                | EdgeQueryCandidateSourceKind::EdgeMetadataScan
        ),
        EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
            inputs.iter().all(edge_plan_is_filter_source)
        }
        EdgePhysicalPlan::Empty => false,
    }
}

fn edge_materialization_uses_limited_probe(materialization: &EdgeCandidateMaterialization) -> bool {
    matches!(
        materialization,
        EdgeCandidateMaterialization::EdgeWeightIndex { .. }
            | EdgeCandidateMaterialization::EdgeUpdatedAtIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidFromIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidToIndex { .. }
            | EdgeCandidateMaterialization::EdgePropertyEqualityIndex { .. }
            | EdgeCandidateMaterialization::EdgePropertyRangeIndex { .. }
    )
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

fn finalize_verified_edge_page(
    mut ids: Vec<u64>,
    mut edges: Vec<EdgeRecord>,
    limit: usize,
) -> VerifiedEdgePage {
    let next_cursor = if limit > 0 && ids.len() > limit {
        ids.truncate(limit);
        if !edges.is_empty() {
            edges.truncate(limit);
        }
        ids.last().copied()
    } else {
        None
    };

    VerifiedEdgePage {
        ids,
        edges,
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

fn node_filter_visibility_meta_matches(
    filter: &NormalizedNodeFilter,
    meta: &NodeVisibilityMeta,
) -> Option<bool> {
    match filter {
        NormalizedNodeFilter::AlwaysTrue => Some(true),
        NormalizedNodeFilter::AlwaysFalse => Some(false),
        NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            Some(meta.updated_at >= *lower_ms && meta.updated_at <= *upper_ms)
        }
        NormalizedNodeFilter::And(children) => {
            for child in children {
                match node_filter_visibility_meta_matches(child, meta) {
                    Some(true) => {}
                    Some(false) => return Some(false),
                    None => return None,
                }
            }
            Some(true)
        }
        NormalizedNodeFilter::Or(children) => {
            let mut needs_record = false;
            for child in children {
                match node_filter_visibility_meta_matches(child, meta) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => needs_record = true,
                }
            }
            if needs_record {
                None
            } else {
                Some(false)
            }
        }
        NormalizedNodeFilter::Not(child) => {
            node_filter_visibility_meta_matches(child, meta).map(|matched| !matched)
        }
        NormalizedNodeFilter::PropertyEquals { .. }
        | NormalizedNodeFilter::PropertyIn { .. }
        | NormalizedNodeFilter::PropertyRange { .. }
        | NormalizedNodeFilter::PropertyExists { .. }
        | NormalizedNodeFilter::PropertyMissing { .. } => None,
    }
}

fn node_filter_visibility_meta_compatible(filter: &NormalizedNodeFilter) -> bool {
    match filter {
        NormalizedNodeFilter::AlwaysTrue
        | NormalizedNodeFilter::AlwaysFalse
        | NormalizedNodeFilter::UpdatedAtRange { .. } => true,
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            children.iter().all(node_filter_visibility_meta_compatible)
        }
        NormalizedNodeFilter::Not(child) => node_filter_visibility_meta_compatible(child),
        NormalizedNodeFilter::PropertyEquals { .. }
        | NormalizedNodeFilter::PropertyIn { .. }
        | NormalizedNodeFilter::PropertyRange { .. }
        | NormalizedNodeFilter::PropertyExists { .. }
        | NormalizedNodeFilter::PropertyMissing { .. } => false,
    }
}

fn node_label_filter_matches(filter: &ResolvedNodeLabelFilter, labels: &NodeLabelSet) -> bool {
    match filter {
        ResolvedNodeLabelFilter::Unconstrained => true,
        ResolvedNodeLabelFilter::Empty { .. } => false,
        ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::Any,
            label_ids,
            ..
        } => label_ids.as_slice().iter().any(|&label_id| labels.contains(label_id)),
        ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::All,
            label_ids,
            ..
        } => label_ids.as_slice().iter().all(|&label_id| labels.contains(label_id)),
    }
}

fn query_node_matches(query: &NormalizedNodeQuery, node: &NodeRecord) -> bool {
    if !node_label_filter_matches(&query.label_filter, &node.label_ids) {
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

fn query_node_visibility_meta_matches(
    query: &NormalizedNodeQuery,
    node_id: u64,
    meta: &NodeVisibilityMeta,
    policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
) -> bool {
    if !node_label_filter_matches(&query.label_filter, &meta.label_ids) {
        return false;
    }
    if !query.ids.is_empty() && query.ids.binary_search(&node_id).is_err() {
        return false;
    }
    if policy_cutoffs.is_some_and(|cutoffs| {
        cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight)
    }) {
        return false;
    }
    if !node_filter_visibility_meta_matches(&query.filter, meta).unwrap_or(false) {
        return false;
    }
    true
}

#[derive(Clone, Copy, Debug)]
struct EdgeMetadataForQuery {
    id: u64,
    from: u64,
    to: u64,
    label_id: u32,
    updated_at: i64,
    weight: f32,
    valid_from: i64,
    valid_to: i64,
}

impl From<&EdgeRecord> for EdgeMetadataForQuery {
    fn from(edge: &EdgeRecord) -> Self {
        Self {
            id: edge.id,
            from: edge.from,
            to: edge.to,
            label_id: edge.label_id,
            updated_at: edge.updated_at,
            weight: edge.weight,
            valid_from: edge.valid_from,
            valid_to: edge.valid_to,
        }
    }
}

impl From<crate::edge_metadata::EdgeMetadataCandidate> for EdgeMetadataForQuery {
    fn from(meta: crate::edge_metadata::EdgeMetadataCandidate) -> Self {
        Self {
            id: meta.edge_id,
            from: meta.from,
            to: meta.to,
            label_id: meta.label_id,
            updated_at: meta.updated_at,
            weight: meta.weight,
            valid_from: meta.valid_from,
            valid_to: meta.valid_to,
        }
    }
}

fn i64_range_matches(value: i64, lower: i64, upper: i64) -> bool {
    value >= lower && value <= upper
}

fn edge_weight_range_matches(value: f32, lower: Option<f32>, upper: Option<f32>) -> bool {
    if value.is_nan() {
        return false;
    }
    if lower.is_some_and(|lower| value < lower) {
        return false;
    }
    if upper.is_some_and(|upper| value > upper) {
        return false;
    }
    true
}

fn edge_filter_requires_hydration(filter: &NormalizedEdgeFilter) -> bool {
    match filter {
        NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. } => true,
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().any(edge_filter_requires_hydration)
        }
        NormalizedEdgeFilter::Not(child) => edge_filter_requires_hydration(child),
        _ => false,
    }
}

fn edge_filter_needs_full_metadata(filter: &NormalizedEdgeFilter) -> bool {
    match filter {
        NormalizedEdgeFilter::UpdatedAtRange { .. } => true,
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().any(edge_filter_needs_full_metadata)
        }
        NormalizedEdgeFilter::Not(child) => edge_filter_needs_full_metadata(child),
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PatternEdgeFilterMode {
    AlwaysTrue,
    PostingMetadataOnly,
    BatchMetadataNeeded,
    PropertyVerifier,
}

fn pattern_edge_filter_mode(filter: &NormalizedEdgeFilter) -> PatternEdgeFilterMode {
    if filter.is_always_true() {
        PatternEdgeFilterMode::AlwaysTrue
    } else if edge_filter_requires_hydration(filter) {
        PatternEdgeFilterMode::PropertyVerifier
    } else if edge_filter_needs_full_metadata(filter) {
        PatternEdgeFilterMode::BatchMetadataNeeded
    } else {
        PatternEdgeFilterMode::PostingMetadataOnly
    }
}

#[derive(Clone, Copy, Debug)]
struct EdgePostingMetadataForQuery {
    weight: f32,
    valid_from: i64,
    valid_to: i64,
}

fn edge_filter_posting_metadata_outcome(
    filter: &NormalizedEdgeFilter,
    meta: &EdgePostingMetadataForQuery,
) -> Option<bool> {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => Some(true),
        NormalizedEdgeFilter::AlwaysFalse => Some(false),
        NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. }
        | NormalizedEdgeFilter::UpdatedAtRange { .. } => None,
        NormalizedEdgeFilter::WeightRange { lower, upper } => {
            Some(edge_weight_range_matches(meta.weight, *lower, *upper))
        }
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            Some(meta.valid_from <= *epoch_ms && *epoch_ms < meta.valid_to)
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
            Some(i64_range_matches(meta.valid_from, *lower_ms, *upper_ms))
        }
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
            Some(i64_range_matches(meta.valid_to, *lower_ms, *upper_ms))
        }
        NormalizedEdgeFilter::And(children) => {
            let mut unknown = false;
            for child in children {
                match edge_filter_posting_metadata_outcome(child, meta) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(true) }
        }
        NormalizedEdgeFilter::Or(children) => {
            let mut unknown = false;
            for child in children {
                match edge_filter_posting_metadata_outcome(child, meta) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(false) }
        }
        NormalizedEdgeFilter::Not(child) => {
            edge_filter_posting_metadata_outcome(child, meta).map(|matched| !matched)
        }
    }
}

fn edge_filter_posting_metadata_maybe_matches(
    filter: &NormalizedEdgeFilter,
    meta: &EdgePostingMetadataForQuery,
) -> bool {
    edge_filter_posting_metadata_outcome(filter, meta).unwrap_or(true)
}

fn edge_filter_metadata_outcome(
    filter: &NormalizedEdgeFilter,
    meta: &EdgeMetadataForQuery,
) -> Option<bool> {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => Some(true),
        NormalizedEdgeFilter::AlwaysFalse => Some(false),
        NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. } => None,
        NormalizedEdgeFilter::WeightRange { lower, upper } => {
            Some(edge_weight_range_matches(meta.weight, *lower, *upper))
        }
        NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            Some(i64_range_matches(meta.updated_at, *lower_ms, *upper_ms))
        }
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            Some(meta.valid_from <= *epoch_ms && *epoch_ms < meta.valid_to)
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
            Some(i64_range_matches(meta.valid_from, *lower_ms, *upper_ms))
        }
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
            Some(i64_range_matches(meta.valid_to, *lower_ms, *upper_ms))
        }
        NormalizedEdgeFilter::And(children) => {
            let mut unknown = false;
            for child in children {
                match edge_filter_metadata_outcome(child, meta) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(true) }
        }
        NormalizedEdgeFilter::Or(children) => {
            let mut unknown = false;
            for child in children {
                match edge_filter_metadata_outcome(child, meta) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(false) }
        }
        NormalizedEdgeFilter::Not(child) => {
            edge_filter_metadata_outcome(child, meta).map(|matched| !matched)
        }
    }
}

#[cfg(test)]
fn edge_filter_metadata_maybe_matches(
    filter: &NormalizedEdgeFilter,
    meta: &EdgeMetadataForQuery,
) -> bool {
    edge_filter_metadata_outcome(filter, meta).unwrap_or(true)
}

#[cfg(test)]
fn edge_filter_matches(filter: &NormalizedEdgeFilter, edge: &EdgeRecord) -> bool {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => true,
        NormalizedEdgeFilter::AlwaysFalse => false,
        NormalizedEdgeFilter::PropertyEquals { key, value } => edge
            .props
            .get(key)
            .is_some_and(|candidate| prop_values_equal_for_filter(candidate, value)),
        NormalizedEdgeFilter::PropertyIn {
            key,
            values,
            value_keys,
        } => edge
            .props
            .get(key)
            .is_some_and(|candidate| property_in_filter_matches(candidate, values, value_keys)),
        NormalizedEdgeFilter::PropertyRange { key, lower, upper } => edge
            .props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
        NormalizedEdgeFilter::PropertyExists { key } => edge.props.contains_key(key),
        NormalizedEdgeFilter::PropertyMissing { key } => !edge.props.contains_key(key),
        NormalizedEdgeFilter::WeightRange { lower, upper } => {
            edge_weight_range_matches(edge.weight, *lower, *upper)
        }
        NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            i64_range_matches(edge.updated_at, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            edge.valid_from <= *epoch_ms && *epoch_ms < edge.valid_to
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
            i64_range_matches(edge.valid_from, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
            i64_range_matches(edge.valid_to, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::And(children) => {
            children.iter().all(|child| edge_filter_matches(child, edge))
        }
        NormalizedEdgeFilter::Or(children) => {
            children.iter().any(|child| edge_filter_matches(child, edge))
        }
        NormalizedEdgeFilter::Not(child) => !edge_filter_matches(child, edge),
    }
}

fn collect_edge_filter_property_keys(filter: &NormalizedEdgeFilter, keys: &mut Vec<String>) {
    match filter {
        NormalizedEdgeFilter::PropertyEquals { key, .. }
        | NormalizedEdgeFilter::PropertyIn { key, .. }
        | NormalizedEdgeFilter::PropertyRange { key, .. }
        | NormalizedEdgeFilter::PropertyExists { key }
        | NormalizedEdgeFilter::PropertyMissing { key } => {
            if !keys.iter().any(|existing| existing == key) {
                keys.push(key.clone());
            }
        }
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            for child in children {
                collect_edge_filter_property_keys(child, keys);
            }
        }
        NormalizedEdgeFilter::Not(child) => collect_edge_filter_property_keys(child, keys),
        NormalizedEdgeFilter::AlwaysTrue
        | NormalizedEdgeFilter::AlwaysFalse
        | NormalizedEdgeFilter::WeightRange { .. }
        | NormalizedEdgeFilter::UpdatedAtRange { .. }
        | NormalizedEdgeFilter::ValidAt { .. }
        | NormalizedEdgeFilter::ValidFromRange { .. }
        | NormalizedEdgeFilter::ValidToRange { .. } => {}
    }
}

fn edge_filter_projected_matches(
    filter: &NormalizedEdgeFilter,
    meta: &EdgeMetadataForQuery,
    props: &BTreeMap<String, PropValue>,
) -> bool {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => true,
        NormalizedEdgeFilter::AlwaysFalse => false,
        NormalizedEdgeFilter::PropertyEquals { key, value } => props
            .get(key)
            .is_some_and(|candidate| prop_values_equal_for_filter(candidate, value)),
        NormalizedEdgeFilter::PropertyIn {
            key,
            values,
            value_keys,
        } => props
            .get(key)
            .is_some_and(|candidate| property_in_filter_matches(candidate, values, value_keys)),
        NormalizedEdgeFilter::PropertyRange { key, lower, upper } => props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
        NormalizedEdgeFilter::PropertyExists { key } => props.contains_key(key),
        NormalizedEdgeFilter::PropertyMissing { key } => !props.contains_key(key),
        NormalizedEdgeFilter::WeightRange { lower, upper } => {
            edge_weight_range_matches(meta.weight, *lower, *upper)
        }
        NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            i64_range_matches(meta.updated_at, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            meta.valid_from <= *epoch_ms && *epoch_ms < meta.valid_to
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
            i64_range_matches(meta.valid_from, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
            i64_range_matches(meta.valid_to, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::And(children) => children
            .iter()
            .all(|child| edge_filter_projected_matches(child, meta, props)),
        NormalizedEdgeFilter::Or(children) => children
            .iter()
            .any(|child| edge_filter_projected_matches(child, meta, props)),
        NormalizedEdgeFilter::Not(child) => !edge_filter_projected_matches(child, meta, props),
    }
}

#[cfg(test)]
fn edge_query_metadata_matches(
    query: &NormalizedEdgeQuery,
    meta: &EdgeMetadataForQuery,
) -> bool {
    if !edge_query_metadata_constraints_match(query, meta) {
        return false;
    }

    edge_filter_metadata_maybe_matches(&query.filter, meta)
}

fn edge_query_metadata_constraints_match(
    query: &NormalizedEdgeQuery,
    meta: &EdgeMetadataForQuery,
) -> bool {
    if query.label_id.is_some_and(|label_id| meta.label_id != label_id) {
        return false;
    }
    if !query.ids.is_empty() && query.ids.binary_search(&meta.id).is_err() {
        return false;
    }
    if !query.from_ids.is_empty() && query.from_ids.binary_search(&meta.from).is_err() {
        return false;
    }
    if !query.to_ids.is_empty() && query.to_ids.binary_search(&meta.to).is_err() {
        return false;
    }
    if !query.endpoint_ids.is_empty()
        && query.endpoint_ids.binary_search(&meta.from).is_err()
        && query.endpoint_ids.binary_search(&meta.to).is_err()
    {
        return false;
    }

    true
}

#[cfg(test)]
fn edge_query_matches(query: &NormalizedEdgeQuery, edge: &EdgeRecord) -> bool {
    let meta = EdgeMetadataForQuery::from(edge);
    if !edge_query_metadata_matches(query, &meta) {
        return false;
    }
    edge_filter_matches(&query.filter, edge)
}

#[derive(Clone)]
struct PatternExecutionState {
    nodes: Vec<Option<u64>>,
    edges: Vec<Option<u64>>,
}

#[derive(Clone, Copy)]
struct VerifiedPatternEdgeAnchor {
    meta: EdgeMetadataCandidate,
}

enum PatternAnchorFrontier {
    Ready {
        states: Vec<PatternExecutionState>,
        followups: Vec<SecondaryIndexReadFollowup>,
        expansion_order: Vec<usize>,
        sort_anchor_alias: String,
    },
    TooBroad {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
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
    best_by_target: NodeIdMap<NeighborRecord>,
    best_bound_entry: Option<NeighborRecord>,
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
    entry: NeighborRecord,
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
    edge_filter: &NormalizedEdgeFilter,
    reference_time: i64,
    deleted_nodes: &NodeIdSet,
    deleted_edges: &NodeIdSet,
    seen_edges: &mut NodeIdSet,
    best_by_target: &mut NodeIdMap<NeighborRecord>,
    best_bound_entry: &mut Option<NeighborRecord>,
    exceeded: &mut bool,
    edge_id: u64,
    neighbor_id: u64,
    edge_label_id: u32,
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
    let posting_meta = EdgePostingMetadataForQuery {
        weight,
        valid_from,
        valid_to,
    };
    if !edge_filter_posting_metadata_maybe_matches(edge_filter, &posting_meta) {
        return ControlFlow::Continue(());
    }
    if let Some(target_id) = context.target_id {
        if neighbor_id != target_id {
            return ControlFlow::Continue(());
        }
        *best_bound_entry = Some(NeighborRecord {
            node_id: neighbor_id,
            edge_id,
            edge_label_id,
            weight,
            valid_from,
            valid_to,
        });
        return ControlFlow::Break(());
    }
    if !pattern_distinct_node_binding_ok(state, context.target_index, neighbor_id) {
        return ControlFlow::Continue(());
    }
    let entry = NeighborRecord {
        node_id: neighbor_id,
        edge_id,
        edge_label_id,
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
    fn query_node_page_from_candidates(
        &self,
        candidate_ids: &[u64],
        query: &NormalizedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
        if query.filter.is_always_true() && query.keys.is_empty() {
            return self.query_node_page_from_metadata_candidates(
                candidate_ids,
                query,
                hydrate,
                policy_cutoffs,
            );
        }

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

    fn query_node_page_from_metadata_candidates(
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

        for chunk in candidate_ids[start..].chunks(QUERY_VERIFY_CHUNK) {
            #[cfg(test)]
            self.note_node_visibility_meta_reads(chunk.len());
            let visibility = self.sources().find_node_visibility_meta(chunk)?;
            for (&node_id, state) in chunk.iter().zip(visibility.iter()) {
                let NodeVisibilityState::Live(meta) = state else {
                    continue;
                };
                if !query_node_visibility_meta_matches(query, node_id, meta, policy_cutoffs) {
                    continue;
                }
                ids.push(node_id);
                if ids.len() >= target {
                    let mut page = finalize_verified_page(ids, Vec::new(), limit);
                    if hydrate {
                        let nodes = self.get_nodes_raw(&page.ids)?;
                        page.nodes = nodes.into_iter().flatten().collect();
                    }
                    return Ok(page);
                }
            }
        }

        let mut page = finalize_verified_page(ids, Vec::new(), limit);
        if hydrate {
            let nodes = self.get_nodes_raw(&page.ids)?;
            page.nodes = nodes.into_iter().flatten().collect();
        }
        Ok(page)
    }

    fn query_node_page_from_label_scan(
        &self,
        query: &NormalizedNodeQuery,
        label_ids: &[u32],
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let metadata_only = query.filter.is_always_true() && query.keys.is_empty();
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut nodes = Vec::with_capacity(if hydrate && limit > 0 { limit } else { 0 });

        self.scan_raw_node_label_candidates(
            label_ids,
            query.page.after,
            chunk_limit,
            |chunk| {
                if metadata_only {
                    #[cfg(test)]
                    self.note_node_visibility_meta_reads(chunk.len());
                    let visibility = self.sources().find_node_visibility_meta(chunk)?;
                    for (&node_id, state) in chunk.iter().zip(visibility.iter()) {
                        let NodeVisibilityState::Live(meta) = state else {
                            continue;
                        };
                        if !query_node_visibility_meta_matches(
                            query,
                            node_id,
                            meta,
                            policy_cutoffs,
                        ) {
                            continue;
                        }
                        ids.push(node_id);
                        if ids.len() >= target {
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                    return Ok(ControlFlow::Continue(()));
                }

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
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;

        if metadata_only {
            let mut page = finalize_verified_page(ids, Vec::new(), limit);
            if hydrate {
                let nodes = self.get_nodes_raw(&page.ids)?;
                page.nodes = nodes.into_iter().flatten().collect();
            }
            Ok(page)
        } else {
            Ok(finalize_verified_page(ids, nodes, limit))
        }
    }

    fn query_node_page_from_single_label_scan(
        &self,
        query: &NormalizedNodeQuery,
        single_label_id: u32,
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

        if !hydrate
            && policy_cutoffs.is_none()
            && query.filter.is_always_true()
            && query.keys.is_empty()
            && query.ids.is_empty()
            && single_resolved_label_id(&query.label_filter) == Some(single_label_id)
        {
            let id_page =
                self.nodes_by_single_label_id_paged_unfiltered(single_label_id, &query.page)?;
            return Ok(VerifiedNodePage {
                ids: id_page.items,
                nodes: Vec::new(),
                next_cursor: id_page.next_cursor,
            });
        }

        if query.filter.is_always_true() && query.keys.is_empty() {
            return self.query_node_page_from_label_scan(
                query,
                &[single_label_id],
                hydrate,
                policy_cutoffs,
            );
        }

        self.scan_nodes_by_single_label_id_filtered(
            single_label_id,
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
                label_id,
                lower_ms,
                upper_ms,
            } => {
                let ids = self.timestamp_candidate_ids(
                    *label_id,
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
            NodeCandidateMaterialization::NodeLabelAny { .. }
            | NodeCandidateMaterialization::NodeLabelIndex { .. }
            | NodeCandidateMaterialization::FallbackNodeLabelScan { .. }
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
            NodeQueryCandidateSourceKind::NodeLabelIndex
            | NodeQueryCandidateSourceKind::FallbackNodeLabelScan => {
                let label_id = match &source.materialization {
                    NodeCandidateMaterialization::NodeLabelIndex { label_id }
                    | NodeCandidateMaterialization::FallbackNodeLabelScan { label_id } => *label_id,
                    NodeCandidateMaterialization::NodeLabelAny { label_ids } => {
                        return Ok((
                            self.query_node_page_from_label_scan(
                                query,
                                label_ids.as_slice(),
                                hydrate,
                                policy_cutoffs,
                            )?,
                            Vec::new(),
                        ));
                    }
                    _ => unreachable!("node label source must carry label materialization"),
                };
                Ok((
                    self.query_node_page_from_single_label_scan(
                        query,
                        label_id,
                        hydrate,
                        policy_cutoffs,
                    )?,
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
                "node query requires label_filter, ids, keys, or allow_full_scan".into(),
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
        let items = page
            .nodes
            .into_iter()
            .map(|node| node_view_from_record(node, self.label_catalog.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let value = QueryNodesResult {
            items,
            next_cursor: page.next_cursor,
        };
        Ok(QueryExecutionOutcome { value, followups })
    }

    fn ready_edge_equality_candidate_ids_raw_limited(
        &self,
        index_id: u64,
        value_hashes: &[u64],
        raw_posting_cap: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        match self.sources().edge_ids_by_secondary_eq_hashes_limited_read(
            index_id,
            value_hashes,
            raw_posting_cap,
        ) {
            Ok(crate::source_list::LimitedEdgeIndexRead::Ready(ids)) => Ok((Some(ids), None)),
            Ok(crate::source_list::LimitedEdgeIndexRead::TooBroad) => Ok((None, None)),
            Ok(crate::source_list::LimitedEdgeIndexRead::MissingSidecar) => {
                Ok((None, self.equality_sidecar_failure_followup(index_id, None)))
            }
            Err(error) => Ok((
                None,
                self.equality_sidecar_failure_followup(index_id, Some(error)),
            )),
        }
    }

    fn materialize_edge_candidate_source(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        source: &PlannedEdgeCandidateSource,
    ) -> Result<CandidateMaterializationResult, EngineError> {
        let cap = cap_context.source_cap(source.kind, query.page.limit, source.estimate);
        if !matches!(
            source.materialization,
            EdgeCandidateMaterialization::Precomputed(_)
                | EdgeCandidateMaterialization::FallbackFullEdgeScan
        ) && !edge_materialization_uses_limited_probe(&source.materialization)
            && source
            .estimate
            .known_upper_bound()
            .is_some_and(|count| count > cap as u64)
        {
            return Ok(CandidateMaterializationResult::TooBroad {
                followups: Vec::new(),
            });
        }

        if matches!(
            source.materialization,
            EdgeCandidateMaterialization::FallbackFullEdgeScan
        ) {
            return Ok(CandidateMaterializationResult::TooBroad {
                followups: Vec::new(),
            });
        }

        let sources = self.sources();
        let ids = match &source.materialization {
            EdgeCandidateMaterialization::Precomputed(ids) => {
                return Ok(CandidateMaterializationResult::Ready {
                    ids: ids.clone(),
                    followups: Vec::new(),
                });
            }
            EdgeCandidateMaterialization::EdgeLabelIndex { label_id } => sources.edge_ids_by_label_id(*label_id),
            EdgeCandidateMaterialization::EdgeTripleIndex { from, to, label_id } => {
                sources.edge_ids_by_triple(*from, *to, *label_id)
            }
            EdgeCandidateMaterialization::FromEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => self.edge_ids_by_endpoint_sources(
                node_ids,
                Direction::Outgoing,
                label_filter_ids.as_deref(),
                cap.saturating_add(1),
            ),
            EdgeCandidateMaterialization::ToEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => self.edge_ids_by_endpoint_sources(
                node_ids,
                Direction::Incoming,
                label_filter_ids.as_deref(),
                cap.saturating_add(1),
            ),
            EdgeCandidateMaterialization::AnyEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => self.edge_ids_by_endpoint_sources(
                node_ids,
                Direction::Both,
                label_filter_ids.as_deref(),
                cap.saturating_add(1),
            ),
            EdgeCandidateMaterialization::EdgeWeightIndex { label_id, bounds } => {
                sources.edge_ids_by_weight_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )
            }
            EdgeCandidateMaterialization::EdgeUpdatedAtIndex { label_id, bounds } => {
                sources.edge_ids_by_updated_at_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )
            }
            EdgeCandidateMaterialization::EdgeValidFromIndex { label_id, bounds } => {
                sources.edge_ids_by_valid_from_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )
            }
            EdgeCandidateMaterialization::EdgeValidToIndex { label_id, bounds } => {
                sources.edge_ids_by_valid_to_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )
            }
            EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
                index_id,
                label_id,
                prop_key,
                value,
                value_hashes,
            } => {
                let _ = (label_id, prop_key, value);
                let (ids, followup) = self.ready_edge_equality_candidate_ids_raw_limited(
                    *index_id,
                    value_hashes,
                    cap.saturating_add(1),
                )?;
                let followups = materialization_followups(followup);
                let Some(ids) = ids else {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                };
                if ids.len() > cap {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                }
                return Ok(CandidateMaterializationResult::Ready { ids, followups });
            }
            EdgeCandidateMaterialization::EdgePropertyRangeIndex {
                index_id,
                label_id,
                prop_key,
                domain,
                lower,
                upper,
            } => {
                let _ = (label_id, prop_key);
                let (ids, followup) = self.ready_edge_range_candidate_ids(
                    *index_id,
                    *domain,
                    lower.as_ref(),
                    upper.as_ref(),
                    cap.saturating_add(1),
                )?;
                let followups = materialization_followups(followup);
                let Some(ids) = ids else {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                };
                if ids.len() > cap {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                }
                return Ok(CandidateMaterializationResult::Ready { ids, followups });
            }
            EdgeCandidateMaterialization::FallbackFullEdgeScan => unreachable!("handled above"),
        }?;
        if ids.len() > cap {
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

    fn edge_ids_by_endpoint_sources(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        self.sources()
            .edge_ids_by_endpoints_limited(node_ids, direction, label_filter_ids, limit)
    }

    fn materialize_edge_physical_plan(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        plan: &EdgePhysicalPlan,
    ) -> Result<CandidateMaterializationResult, EngineError> {
        match plan {
            EdgePhysicalPlan::Empty => Ok(CandidateMaterializationResult::Ready {
                ids: Vec::new(),
                followups: Vec::new(),
            }),
            EdgePhysicalPlan::Source(source) => {
                self.materialize_edge_candidate_source(query, cap_context, source)
            }
            EdgePhysicalPlan::Intersect(inputs) => {
                let mut sets = Vec::with_capacity(inputs.len());
                let mut followups = Vec::new();
                for input in inputs {
                    if !sets.is_empty()
                        && edge_plan_is_filter_source(input)
                        && sets
                            .iter()
                            .map(Vec::len)
                            .min()
                            .is_some_and(|len| len <= EDGE_INTERSECTION_TINY_SET)
                    {
                        continue;
                    }
                    match self.materialize_edge_physical_plan(query, cap_context, input)? {
                        CandidateMaterializationResult::Ready {
                            ids,
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            if ids.is_empty() {
                                return Ok(CandidateMaterializationResult::Ready {
                                    ids: Vec::new(),
                                    followups,
                                });
                            }
                            sets.push(ids);
                        }
                        CandidateMaterializationResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            if !sets.is_empty() {
                                continue;
                            }
                        }
                    }
                }
                if sets.is_empty() {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready {
                        ids: intersect_candidate_sets(&sets),
                        followups,
                    })
                }
            }
            EdgePhysicalPlan::Union(inputs) => {
                let mut sets = Vec::with_capacity(inputs.len());
                let mut followups = Vec::new();
                let mut total_len = 0usize;
                let cap = cap_context.union_total_cap(query.page.limit, plan.estimate());
                for input in inputs {
                    match self.materialize_edge_physical_plan(query, cap_context, input)? {
                        CandidateMaterializationResult::Ready {
                            ids,
                            followups: mut input_followups,
                        } => {
                            total_len = total_len.saturating_add(ids.len());
                            followups.append(&mut input_followups);
                            if total_len > cap {
                                return Ok(CandidateMaterializationResult::TooBroad { followups });
                            }
                            sets.push(ids);
                        }
                        CandidateMaterializationResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let ids = union_candidate_sets(&sets);
                if ids.len() > cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready { ids, followups })
                }
            }
        }
    }

    fn full_scan_edge_sources(
        &self,
        start_after: Option<u64>,
    ) -> Result<Vec<FullScanEdgeSource<'_>>, EngineError> {
        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(FullScanEdgeSource::memtable(
            &self.memtable,
            self.snapshot_seq,
            start_after,
        ));
        for epoch in &self.immutable_epochs {
            sources.push(FullScanEdgeSource::memtable(
                &epoch.memtable,
                self.snapshot_seq,
                start_after,
            ));
        }
        for segment in &self.segments {
            sources.push(FullScanEdgeSource::segment(segment.as_ref(), start_after)?);
        }
        Ok(sources)
    }

    fn label_edge_sources(
        &self,
        label_id: u32,
        start_after: Option<u64>,
    ) -> Result<Vec<LabelEdgeSource<'_>>, EngineError> {
        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(LabelEdgeSource::memtable(
            &self.memtable,
            self.snapshot_seq,
            label_id,
            start_after,
        ));
        for epoch in &self.immutable_epochs {
            sources.push(LabelEdgeSource::memtable(
                &epoch.memtable,
                self.snapshot_seq,
                label_id,
                start_after,
            ));
        }
        for segment in &self.segments {
            if let Some(posting) = segment.edge_label_posting(label_id)? {
                sources.push(LabelEdgeSource::segment(
                    segment.as_ref(),
                    posting,
                    start_after,
                )?);
            }
        }
        Ok(sources)
    }

    fn endpoint_edge_sources<'a>(
        &'a self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'a [u32]>,
        start_after: Option<u64>,
    ) -> Result<Vec<EndpointEdgeSource<'a>>, EngineError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut sorted_node_ids = node_ids.to_vec();
        sorted_node_ids.sort_unstable();
        sorted_node_ids.dedup();

        let mut sources = Vec::with_capacity(1 + self.immutable_epochs.len() + self.segments.len());
        sources.push(EndpointEdgeSource::memtable(
            &self.memtable,
            &sorted_node_ids,
            direction,
            label_filter_ids,
            self.snapshot_seq,
            start_after,
        ));
        for epoch in &self.immutable_epochs {
            sources.push(EndpointEdgeSource::memtable(
                &epoch.memtable,
                &sorted_node_ids,
                direction,
                label_filter_ids,
                self.snapshot_seq,
                start_after,
            ));
        }
        for segment in &self.segments {
            sources.push(EndpointEdgeSource::segment(
                segment.as_ref(),
                &sorted_node_ids,
                direction,
                label_filter_ids,
                start_after,
            )?);
        }
        Ok(sources)
    }

    fn scan_full_edge_id_chunks<F>(
        &self,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let mut sources = self.full_scan_edge_sources(start_after)?;
        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter_mut().enumerate() {
            if let Some(edge_id) = source.next_id()? {
                heap.push(Reverse((edge_id, source_index)));
            }
        }

        let chunk_limit = chunk_limit.max(1);
        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((edge_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_id()? {
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

    fn scan_label_edge_id_chunks<F>(
        &self,
        label_id: u32,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let mut sources = self.label_edge_sources(label_id, start_after)?;
        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter_mut().enumerate() {
            if let Some(edge_id) = source.next_id()? {
                heap.push(Reverse((edge_id, source_index)));
            }
        }

        let chunk_limit = chunk_limit.max(1);
        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((edge_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_id()? {
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

    fn scan_endpoint_edge_id_chunks<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let mut sources =
            self.endpoint_edge_sources(node_ids, direction, label_filter_ids, start_after)?;
        let mut heap = BinaryHeap::new();
        for (source_index, source) in sources.iter_mut().enumerate() {
            if let Some(edge_id) = source.next_id()? {
                heap.push(Reverse((edge_id, source_index)));
            }
        }

        let chunk_limit = chunk_limit.max(1);
        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((edge_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].next_id()? {
                heap.push(Reverse((next_id, source_index)));
            }

            if last_seen == Some(edge_id) {
                continue;
            }
            last_seen = Some(edge_id);
            #[cfg(test)]
            self.note_endpoint_adjacency_candidates(1);
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

    fn populate_verified_edge_records(
        &self,
        page: &mut VerifiedEdgePage,
        hydrated_records: &mut NodeIdMap<EdgeRecord>,
    ) -> Result<(), EngineError> {
        if page.ids.is_empty() {
            return Ok(());
        }

        let mut slots = vec![None; page.ids.len()];
        let mut missing_positions = Vec::new();
        let mut missing_ids = Vec::new();
        for (index, &edge_id) in page.ids.iter().enumerate() {
            if let Some(edge) = hydrated_records.remove(&edge_id) {
                slots[index] = Some(edge);
            } else {
                missing_positions.push(index);
                missing_ids.push(edge_id);
            }
        }

        if !missing_ids.is_empty() {
            let records = self.get_edges(&missing_ids)?;
            for (index, record) in missing_positions.into_iter().zip(records.into_iter()) {
                slots[index] = record;
            }
        }

        page.edges = slots.into_iter().flatten().collect();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_edge_candidate_chunk(
        &self,
        chunk: &[u64],
        query: &NormalizedEdgeQuery,
        _hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        endpoint_cache: &mut EdgeEndpointVisibilityCache,
        ids: &mut Vec<u64>,
        _hydrated_records: &mut NodeIdMap<EdgeRecord>,
        target: usize,
    ) -> Result<ControlFlow<()>, EngineError> {
        let metadata = self.sources().find_edge_metadata(chunk)?;
        let metas = metadata.iter().filter_map(|meta| *meta).collect::<Vec<_>>();
        {
            let sources = self.sources();
            endpoint_cache.ensure_edge_endpoints(&sources, &metas, policy_cutoffs)?;
        }

        let mut decisions = Vec::new();
        let mut property_candidate_ids = Vec::new();
        let mut property_keys = Vec::new();
        collect_edge_filter_property_keys(&query.filter, &mut property_keys);

        for meta in metas {
            if !endpoint_cache.edge_endpoints_visible(meta) {
                continue;
            }
            let query_meta = EdgeMetadataForQuery::from(meta);
            if !edge_query_metadata_constraints_match(query, &query_meta) {
                continue;
            }
            match edge_filter_metadata_outcome(&query.filter, &query_meta) {
                Some(false) => continue,
                Some(true) => {
                    decisions.push((meta.edge_id, query_meta, false));
                }
                None => {
                    decisions.push((meta.edge_id, query_meta, true));
                    property_candidate_ids.push(meta.edge_id);
                }
            }
        }

        let mut property_matches = NodeIdSet::default();
        if !property_candidate_ids.is_empty() {
            let mut metadata_by_property_candidate = NodeIdMap::with_capacity_and_hasher(
                property_candidate_ids.len(),
                Default::default(),
            );
            for (edge_id, query_meta, needs_properties) in &decisions {
                if *needs_properties {
                    metadata_by_property_candidate.insert(*edge_id, *query_meta);
                }
            }
            let projected = self
                .sources()
                .find_edge_properties(&property_candidate_ids, &property_keys)?;
            for (&edge_id, props) in property_candidate_ids.iter().zip(projected.iter()) {
                let Some(props) = props else {
                    continue;
                };
                let Some(query_meta) = metadata_by_property_candidate.get(&edge_id) else {
                    continue;
                };
                if edge_filter_projected_matches(&query.filter, query_meta, props) {
                    property_matches.insert(edge_id);
                }
            }
        }

        for (edge_id, _, needs_properties) in decisions {
            if needs_properties && !property_matches.contains(&edge_id) {
                continue;
            }
            ids.push(edge_id);
            if ids.len() >= target {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    fn query_edge_page_from_candidates(
        &self,
        candidate_ids: &[u64],
        query: &NormalizedEdgeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedEdgePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let mut ids = Vec::new();
        let mut hydrated_records = NodeIdMap::default();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();
        let start = first_candidate_after(candidate_ids, query.page.after);
        let mut cursor = start;

        while cursor < candidate_ids.len() && ids.len() < target {
            let end = (cursor + QUERY_VERIFY_CHUNK).min(candidate_ids.len());
            let chunk = &candidate_ids[cursor..end];
            if self
                .verify_edge_candidate_chunk(
                    chunk,
                    query,
                    hydrate,
                policy_cutoffs,
                &mut endpoint_cache,
                &mut ids,
                &mut hydrated_records,
                target,
            )?
            .is_break()
            {
                break;
            }

            cursor = end;
        }

        let mut page = finalize_verified_edge_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_edge_records(&mut page, &mut hydrated_records)?;
        }
        Ok(page)
    }

    fn query_edge_page_from_label_scan(
        &self,
        label_id: u32,
        query: &NormalizedEdgeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedEdgePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut ids = Vec::new();
        let mut hydrated_records = NodeIdMap::default();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();

        self.scan_label_edge_id_chunks(label_id, query.page.after, chunk_limit, |chunk| {
            self.verify_edge_candidate_chunk(
                chunk,
                query,
                hydrate,
                policy_cutoffs,
                &mut endpoint_cache,
                &mut ids,
                &mut hydrated_records,
                target,
            )
        })?;

        let mut page = finalize_verified_edge_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_edge_records(&mut page, &mut hydrated_records)?;
        }
        Ok(page)
    }

    fn query_edge_page_from_endpoint_scan(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        query: &NormalizedEdgeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedEdgePage, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut ids = Vec::new();
        let mut hydrated_records = NodeIdMap::default();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();

        self.scan_endpoint_edge_id_chunks(
            node_ids,
            direction,
            label_filter_ids,
            query.page.after,
            chunk_limit,
            |chunk| {
                self.verify_edge_candidate_chunk(
                    chunk,
                    query,
                    hydrate,
                    policy_cutoffs,
                    &mut endpoint_cache,
                    &mut ids,
                    &mut hydrated_records,
                    target,
                )
            },
        )?;

        let mut page = finalize_verified_edge_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_edge_records(&mut page, &mut hydrated_records)?;
        }
        Ok(page)
    }

    fn query_edge_page_from_full_scan(
        &self,
        query: &NormalizedEdgeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedEdgePage, EngineError> {
        #[cfg(test)]
        self.note_edge_full_scan_page();

        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let chunk_limit = match query.page.limit {
            Some(limit) if limit > 0 => limit.saturating_add(1).saturating_mul(4).max(limit + 1),
            _ => QUERY_VERIFY_CHUNK,
        };
        let mut ids = Vec::new();
        let mut hydrated_records = NodeIdMap::default();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();

        self.scan_full_edge_id_chunks(query.page.after, chunk_limit, |chunk| {
            self.verify_edge_candidate_chunk(
                chunk,
                query,
                hydrate,
                policy_cutoffs,
                &mut endpoint_cache,
                &mut ids,
                &mut hydrated_records,
                target,
            )
        })?;

        let mut page = finalize_verified_edge_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_edge_records(&mut page, &mut hydrated_records)?;
        }
        Ok(page)
    }

    fn query_edge_page_from_source_driver(
        &self,
        source: &PlannedEdgeCandidateSource,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedEdgePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        match &source.materialization {
            EdgeCandidateMaterialization::EdgeLabelIndex { label_id } => {
                Ok((
                    self.query_edge_page_from_label_scan(*label_id, query, hydrate, policy_cutoffs)?,
                    Vec::new(),
                ))
            }
            EdgeCandidateMaterialization::FromEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => Ok((
                self.query_edge_page_from_endpoint_scan(
                    node_ids,
                    Direction::Outgoing,
                    label_filter_ids.as_deref(),
                    query,
                    hydrate,
                    policy_cutoffs,
                )?,
                Vec::new(),
            )),
            EdgeCandidateMaterialization::ToEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => Ok((
                self.query_edge_page_from_endpoint_scan(
                    node_ids,
                    Direction::Incoming,
                    label_filter_ids.as_deref(),
                    query,
                    hydrate,
                    policy_cutoffs,
                )?,
                Vec::new(),
            )),
            EdgeCandidateMaterialization::AnyEndpointAdjacency {
                node_ids,
                label_filter_ids,
            } => Ok((
                self.query_edge_page_from_endpoint_scan(
                    node_ids,
                    Direction::Both,
                    label_filter_ids.as_deref(),
                    query,
                    hydrate,
                    policy_cutoffs,
                )?,
                Vec::new(),
            )),
            EdgeCandidateMaterialization::FallbackFullEdgeScan => {
                Ok((
                    self.query_edge_page_from_full_scan(query, hydrate, policy_cutoffs)?,
                    Vec::new(),
                ))
            }
            _ => match self.materialize_edge_candidate_source(query, cap_context, source)? {
                CandidateMaterializationResult::Ready { ids, followups } => Ok((
                    self.query_edge_page_from_candidates(&ids, query, hydrate, policy_cutoffs)?,
                    followups,
                )),
                CandidateMaterializationResult::TooBroad {
                    followups: mut materialization_followups,
                } => {
                    let (page, mut fallback_followups) = self.query_edge_page_from_legal_universe(
                        query,
                        cap_context,
                        hydrate,
                        policy_cutoffs,
                    )?;
                    materialization_followups.append(&mut fallback_followups);
                    Ok((page, materialization_followups))
                }
            },
        }
    }

    fn query_edge_page_from_legal_universe(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedEdgePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        let mut sources = self.edge_legal_universe_sources(query);
        if sources.is_empty() {
            return Err(EngineError::InvalidOperation(
                "edge query requires label, ids, from_ids, to_ids, endpoint_ids, or allow_full_scan"
                    .into(),
            ));
        }
        sources.sort_by_key(|source| EdgePhysicalPlan::source(source.clone()).plan_cost());
        let source = sources
            .first()
            .expect("legal edge universe sources must be non-empty");
        self.query_edge_page_from_source_driver(
            source,
            query,
            cap_context,
            hydrate,
            policy_cutoffs,
        )
    }

    fn query_edge_page_planned(
        &self,
        query: &NormalizedEdgeQuery,
        planned: PlannedEdgeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedEdgePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        let PlannedEdgeQuery {
            driver,
            cap_context,
            warnings: _,
            mut followups,
        } = planned;

        if let EdgePhysicalPlan::Source(source) = &driver {
            let (page, mut source_followups) = self.query_edge_page_from_source_driver(
                source,
                query,
                cap_context,
                hydrate,
                policy_cutoffs,
            )?;
            followups.append(&mut source_followups);
            return Ok((page, followups));
        }

        match self.materialize_edge_physical_plan(query, cap_context, &driver)? {
            CandidateMaterializationResult::Ready {
                ids,
                followups: mut materialization_followups,
            } => {
                let page = self.query_edge_page_from_candidates(
                    &ids,
                    query,
                    hydrate,
                    policy_cutoffs,
                )?;
                followups.append(&mut materialization_followups);
                Ok((page, followups))
            }
            CandidateMaterializationResult::TooBroad {
                followups: mut materialization_followups,
            } => {
                let (page, mut fallback_followups) = self.query_edge_page_from_legal_universe(
                    query,
                    cap_context,
                    hydrate,
                    policy_cutoffs,
                )?;
                followups.append(&mut materialization_followups);
                followups.append(&mut fallback_followups);
                Ok((page, followups))
            }
        }
    }

    fn query_edge_ids_outcome(
        &self,
        query: &EdgeQuery,
    ) -> Result<QueryExecutionOutcome<QueryEdgeIdsResult>, EngineError> {
        let normalized = self.normalize_edge_query(query)?;
        let planned = self.plan_normalized_edge_query(&normalized)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let (page, followups) =
            self.query_edge_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())?;
        Ok(QueryExecutionOutcome {
            value: QueryEdgeIdsResult {
                edge_ids: page.ids,
                next_cursor: page.next_cursor,
            },
            followups,
        })
    }

    fn query_edges_outcome(
        &self,
        query: &EdgeQuery,
    ) -> Result<QueryExecutionOutcome<QueryEdgesResult>, EngineError> {
        let normalized = self.normalize_edge_query(query)?;
        let planned = self.plan_normalized_edge_query(&normalized)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let (page, followups) =
            self.query_edge_page_planned(&normalized, planned, true, policy_cutoffs.as_ref())?;
        let edges = page
            .edges
            .into_iter()
            .map(|edge| edge_view_from_record(edge, self.label_catalog.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(QueryExecutionOutcome {
            value: QueryEdgesResult {
                edges,
                next_cursor: page.next_cursor,
            },
            followups,
        })
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
            let target_query = &query.nodes[target_index].query;
            if target_query.keys.is_empty()
                && node_filter_visibility_meta_compatible(&target_query.filter)
            {
                #[cfg(test)]
                self.note_node_visibility_meta_reads(candidate_ids.len());
                let visibility = self.sources().find_node_visibility_meta(candidate_ids)?;
                for (&node_id, state) in candidate_ids.iter().zip(visibility.iter()) {
                    let NodeVisibilityState::Live(meta) = state else {
                        continue;
                    };
                    if query_node_visibility_meta_matches(
                        target_query,
                        node_id,
                        meta,
                        policy_cutoffs,
                    ) {
                        verified_by_index[target_index].insert(node_id);
                    }
                }
            } else {
                let nodes = self.get_nodes_raw(candidate_ids)?;
                for (&node_id, node) in candidate_ids.iter().zip(nodes.iter()) {
                    let Some(node) = node.as_ref() else {
                        continue;
                    };
                    if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                        continue;
                    }
                    if query_node_matches(target_query, node) {
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
        edge_filter_match_cache: &mut NodeIdMap<bool>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        unnamed_emitted_targets: &mut [NodeIdSet],
        unnamed_bound_satisfied: &mut [bool],
        on_next: &mut impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<ControlFlow<()>, EngineError> {
        if pending.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }

        let mode = pattern_edge_filter_mode(&edge.filter);
        let matching_edges = match mode {
            PatternEdgeFilterMode::AlwaysTrue | PatternEdgeFilterMode::PostingMetadataOnly => None,
            PatternEdgeFilterMode::BatchMetadataNeeded
            | PatternEdgeFilterMode::PropertyVerifier => {
                let mut edge_ids: Vec<u64> = pending
                    .iter()
                    .map(|pending| pending.entry.edge_id)
                    .collect();
                edge_ids.sort_unstable();
                edge_ids.dedup();
                let mut matching_edges = NodeIdSet::default();
                let mut unresolved_edge_ids = Vec::new();
                for edge_id in edge_ids {
                    match edge_filter_match_cache.get(&edge_id).copied() {
                        Some(true) => {
                            matching_edges.insert(edge_id);
                        }
                        Some(false) => {}
                        None => unresolved_edge_ids.push(edge_id),
                    }
                }

                let metadata = self.sources().find_edge_metadata(&unresolved_edge_ids)?;
                let requires_properties = mode == PatternEdgeFilterMode::PropertyVerifier;
                let mut property_candidate_ids = Vec::new();
                let mut metadata_by_property_candidate =
                    NodeIdMap::with_capacity_and_hasher(unresolved_edge_ids.len(), Default::default());
                for (&edge_id, meta) in unresolved_edge_ids.iter().zip(metadata.iter()) {
                    let Some(meta) = meta else {
                        edge_filter_match_cache.insert(edge_id, false);
                        continue;
                    };
                    let query_meta = EdgeMetadataForQuery::from(*meta);
                    match edge_filter_metadata_outcome(&edge.filter, &query_meta) {
                        Some(false) => {
                            edge_filter_match_cache.insert(edge_id, false);
                        }
                        Some(true) => {
                            edge_filter_match_cache.insert(edge_id, true);
                            matching_edges.insert(edge_id);
                        }
                        None if requires_properties => {
                            property_candidate_ids.push(edge_id);
                            metadata_by_property_candidate.insert(edge_id, query_meta);
                        }
                        None => {
                            edge_filter_match_cache.insert(edge_id, true);
                            matching_edges.insert(edge_id);
                        }
                    }
                }

                if requires_properties && !property_candidate_ids.is_empty() {
                    let mut property_keys = Vec::new();
                    collect_edge_filter_property_keys(&edge.filter, &mut property_keys);
                    let projected = self
                        .sources()
                        .find_edge_properties(&property_candidate_ids, &property_keys)?;
                    for (&edge_id, props) in property_candidate_ids.iter().zip(projected.iter()) {
                        let matched = props.as_ref().is_some_and(|props| {
                            metadata_by_property_candidate
                                .get(&edge_id)
                                .is_some_and(|query_meta| {
                                    edge_filter_projected_matches(&edge.filter, query_meta, props)
                                })
                        });
                        edge_filter_match_cache.insert(edge_id, matched);
                        if matched {
                            matching_edges.insert(edge_id);
                        }
                    }
                }
                Some(matching_edges)
            }
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
        edge_filter_match_cache: &mut NodeIdMap<bool>,
        unnamed_emitted_targets: &mut [NodeIdSet],
        unnamed_bound_satisfied: &mut [bool],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        on_next: &mut impl FnMut(PatternExecutionState) -> Result<ControlFlow<()>, EngineError>,
        state_index: usize,
        edge_id: u64,
        neighbor_id: u64,
        edge_label_id: u32,
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
        let posting_meta = EdgePostingMetadataForQuery {
            weight,
            valid_from,
            valid_to,
        };
        if !edge_filter_posting_metadata_maybe_matches(&edge.filter, &posting_meta) {
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

        #[cfg(test)]
        self.note_pattern_edge_pending_entry();
        pending.push(PatternPendingEntry {
            state_index,
            entry: NeighborRecord {
                node_id: neighbor_id,
                edge_id,
                edge_label_id,
                weight,
                valid_from,
                valid_to,
            },
        });

        let flush_now = pending.len() >= QUERY_VERIFY_CHUNK;
        if flush_now {
            return self.flush_pattern_pending_entries(
                query,
                edge_index,
                edge,
                states,
                contexts,
                pending,
                edge_filter_match_cache,
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
        let label_filter_ids = edge.label_filter_ids.as_deref();
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
                label_filter_ids,
                self.snapshot_seq,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    record_pattern_unnamed_entry(
                        state,
                        &context,
                        &edge.filter,
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
                    label_filter_ids,
                    self.snapshot_seq,
                    &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                        record_pattern_unnamed_entry(
                            state,
                            &context,
                            &edge.filter,
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
                    label_filter_ids,
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
                                &edge.filter,
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
            let mut entries: Vec<NeighborRecord> =
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
        let label_filter_ids = edge.label_filter_ids.as_deref();
        let mut contexts = Vec::with_capacity(states.len());
        for state in &states {
            contexts.push(Self::pattern_edge_context(edge, state)?);
        }

        let mut seen_edges: Vec<NodeIdSet> =
            states.iter().map(|_| NodeIdSet::default()).collect();
        let mut pending = Vec::with_capacity(QUERY_VERIFY_CHUNK);
        let mut edge_filter_match_cache = NodeIdMap::default();
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
                label_filter_ids,
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
                        &mut edge_filter_match_cache,
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
            if edge.alias.is_none() && context.target_id.is_some() {
                if self
                    .flush_pattern_pending_entries(
                        query,
                        edge_index,
                        edge,
                        &states,
                        &contexts,
                        &mut pending,
                        &mut edge_filter_match_cache,
                        policy_cutoffs,
                        &mut unnamed_emitted_targets,
                        &mut unnamed_bound_satisfied,
                        &mut on_next,
                    )?
                    .is_break()
                {
                    return Ok(());
                }
                if !flow.is_break() && unnamed_bound_satisfied[state_index] {
                    satisfied_this_walk = true;
                }
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
                    label_filter_ids,
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
                            &mut edge_filter_match_cache,
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
                if edge.alias.is_none() && context.target_id.is_some() {
                    if self
                        .flush_pattern_pending_entries(
                            query,
                            edge_index,
                            edge,
                            &states,
                            &contexts,
                            &mut pending,
                            &mut edge_filter_match_cache,
                            policy_cutoffs,
                            &mut unnamed_emitted_targets,
                            &mut unnamed_bound_satisfied,
                            &mut on_next,
                        )?
                        .is_break()
                    {
                        return Ok(());
                    }
                    if !flow.is_break() && unnamed_bound_satisfied[state_index] {
                        satisfied_this_walk = true;
                    }
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
                    label_filter_ids,
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
                                &mut edge_filter_match_cache,
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
                if stop_when_target_bound {
                    if self
                        .flush_pattern_pending_entries(
                            query,
                            edge_index,
                            edge,
                            &states,
                            &contexts,
                            &mut pending,
                            &mut edge_filter_match_cache,
                            policy_cutoffs,
                            &mut unnamed_emitted_targets,
                            &mut unnamed_bound_satisfied,
                            &mut on_next,
                        )?
                        .is_break()
                    {
                        return Ok(());
                    }
                    remaining_target_bound = contexts
                        .iter()
                        .enumerate()
                        .filter(|(state_index, context)| {
                            context.direction == direction
                                && context.target_id.is_some()
                                && !unnamed_bound_satisfied[*state_index]
                        })
                        .count();
                }
                if flow.is_break() {
                    return Ok(());
                }
                if stop_when_target_bound && remaining_target_bound == 0 {
                    continue;
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
                &mut edge_filter_match_cache,
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
        let mode = pattern_edge_filter_mode(&edge.filter);
        if edge.alias.is_none()
            && matches!(
                mode,
                PatternEdgeFilterMode::AlwaysTrue | PatternEdgeFilterMode::PostingMetadataOnly
            )
        {
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

    #[allow(clippy::too_many_arguments)]
    fn verify_pattern_edge_anchor_candidate_chunk(
        &self,
        candidate_ids: &[u64],
        edge: &NormalizedEdgePattern,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        endpoint_cache: &mut EdgeEndpointVisibilityCache,
        property_keys: &[String],
        verified: &mut Vec<VerifiedPatternEdgeAnchor>,
    ) -> Result<(), EngineError> {
        let metadata = self.sources().find_edge_metadata(candidate_ids)?;
        let mut metas = Vec::new();
        for meta in metadata.into_iter().flatten() {
            if edge
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.binary_search(&meta.label_id).is_err())
            {
                continue;
            }
            if !is_edge_valid_at(meta.valid_from, meta.valid_to, reference_time) {
                continue;
            }
            metas.push(meta);
        }

        {
            let sources = self.sources();
            endpoint_cache.ensure_edge_endpoints(&sources, &metas, policy_cutoffs)?;
        }

        let mut decisions = Vec::new();
        let mut property_candidate_ids = Vec::new();

        for meta in metas {
            if !endpoint_cache.edge_endpoints_visible(meta) {
                continue;
            }
            let query_meta = EdgeMetadataForQuery::from(meta);
            match edge_filter_metadata_outcome(&edge.filter, &query_meta) {
                Some(false) => continue,
                Some(true) => decisions.push((meta, false)),
                None => {
                    property_candidate_ids.push(meta.edge_id);
                    decisions.push((meta, true));
                }
            }
        }

        let mut property_matches = NodeIdSet::default();
        if !property_candidate_ids.is_empty() {
            let mut metadata_by_edge_id =
                NodeIdMap::with_capacity_and_hasher(property_candidate_ids.len(), Default::default());
            for (meta, needs_properties) in &decisions {
                if *needs_properties {
                    metadata_by_edge_id.insert(meta.edge_id, EdgeMetadataForQuery::from(*meta));
                }
            }
            let projected = self
                .sources()
                .find_edge_properties(&property_candidate_ids, property_keys)?;
            for (&edge_id, props) in property_candidate_ids.iter().zip(projected.iter()) {
                let Some(props) = props else {
                    continue;
                };
                let Some(query_meta) = metadata_by_edge_id.get(&edge_id) else {
                    continue;
                };
                if edge_filter_projected_matches(&edge.filter, query_meta, props) {
                    property_matches.insert(edge_id);
                }
            }
        }

        for (meta, needs_properties) in decisions {
            if needs_properties && !property_matches.contains(&meta.edge_id) {
                continue;
            }
            verified.push(VerifiedPatternEdgeAnchor { meta });
        }
        Ok(())
    }

    fn verify_pattern_edge_anchor_candidates(
        &self,
        candidate_ids: &[u64],
        edge: &NormalizedEdgePattern,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<VerifiedPatternEdgeAnchor>, EngineError> {
        let mut verified = Vec::new();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();
        let mut property_keys = Vec::new();
        collect_edge_filter_property_keys(&edge.filter, &mut property_keys);

        for chunk in candidate_ids.chunks(QUERY_VERIFY_CHUNK) {
            self.verify_pattern_edge_anchor_candidate_chunk(
                chunk,
                edge,
                reference_time,
                policy_cutoffs,
                &mut endpoint_cache,
                &property_keys,
                &mut verified,
            )?;
        }
        Ok(verified)
    }

    fn pattern_edge_anchor_orientation_bindings(
        edge: &NormalizedEdgePattern,
        meta: EdgeMetadataCandidate,
    ) -> Vec<(u64, u64)> {
        match edge.direction {
            Direction::Outgoing => vec![(meta.from, meta.to)],
            Direction::Incoming => vec![(meta.to, meta.from)],
            Direction::Both if meta.from == meta.to => vec![(meta.from, meta.to)],
            Direction::Both => vec![(meta.from, meta.to), (meta.to, meta.from)],
        }
    }

    fn pattern_edge_anchor_states_from_verified(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        verified_edges: &[VerifiedPatternEdgeAnchor],
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<PatternExecutionState>, EngineError> {
        let edge = &query.edges[edge_index];
        let mut provisional = Vec::new();
        let mut candidate_ids_by_index = vec![Vec::new(); query.nodes.len()];
        let mut seen = HashSet::new();

        for verified in verified_edges {
            for (from_id, to_id) in Self::pattern_edge_anchor_orientation_bindings(edge, verified.meta) {
                let mut state = PatternExecutionState {
                    nodes: vec![None; query.nodes.len()],
                    edges: vec![None; query.edges.len()],
                };
                if edge.from_index == edge.to_index {
                    if from_id != to_id {
                        continue;
                    }
                    state.nodes[edge.from_index] = Some(from_id);
                    candidate_ids_by_index[edge.from_index].push(from_id);
                } else {
                    if from_id == to_id {
                        continue;
                    }
                    state.nodes[edge.from_index] = Some(from_id);
                    state.nodes[edge.to_index] = Some(to_id);
                    candidate_ids_by_index[edge.from_index].push(from_id);
                    candidate_ids_by_index[edge.to_index].push(to_id);
                }
                state.edges[edge_index] = Some(verified.meta.edge_id);

                let mut edge_key = state.edges.clone();
                if edge.alias.is_none() {
                    edge_key[edge_index] = None;
                }
                if seen.insert((state.nodes.clone(), edge_key)) {
                    provisional.push(state);
                    if provisional.len() > PATTERN_FRONTIER_BUDGET {
                        return Err(EngineError::InvalidOperation(format!(
                            "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                        )));
                    }
                }
            }
        }

        let verified_targets = self.pattern_verified_target_ids_by_index(
            query,
            candidate_ids_by_index,
            policy_cutoffs,
        )?;
        provisional.retain(|state| {
            [edge.from_index, edge.to_index].into_iter().all(|node_index| {
                state.nodes[node_index].is_some_and(|node_id| {
                    verified_targets[node_index].contains(&node_id)
                })
            })
        });
        Ok(provisional)
    }

    fn pattern_node_anchor_frontier(
        &self,
        query: &NormalizedGraphPatternQuery,
        node_index: usize,
        anchor_plan: &PlannedNodeQuery,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<PatternAnchorFrontier, EngineError> {
        let anchor = &query.nodes[node_index];
        let mut anchor_query = anchor.query.clone();
        anchor_query.page.limit = Some(PATTERN_FRONTIER_BUDGET);
        let (anchor_page, followups) =
            self.query_node_page_planned(&anchor_query, anchor_plan, false, policy_cutoffs)?;
        if anchor_page.next_cursor.is_some() {
            return Err(EngineError::InvalidOperation(format!(
                "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
            )));
        }
        let mut states = Vec::with_capacity(anchor_page.ids.len());
        for anchor_id in anchor_page.ids {
            let mut state = PatternExecutionState {
                nodes: vec![None; query.nodes.len()],
                edges: vec![None; query.edges.len()],
            };
            state.nodes[node_index] = Some(anchor_id);
            states.push(state);
            if states.len() > PATTERN_FRONTIER_BUDGET {
                return Err(EngineError::InvalidOperation(format!(
                    "pattern intermediate frontier exceeded {PATTERN_FRONTIER_BUDGET} states"
                )));
            }
        }
        Ok(PatternAnchorFrontier::Ready {
            states,
            followups,
            expansion_order: Vec::new(),
            sort_anchor_alias: String::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn pattern_edge_anchor_frontier(
        &self,
        query: &NormalizedGraphPatternQuery,
        edge_index: usize,
        edge_query: &NormalizedEdgeQuery,
        edge_plan: PlannedEdgeQuery,
        edge_fallback_plans: Vec<PlannedEdgeQuery>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<PatternAnchorFrontier, EngineError> {
        let mut deferred_followups = Vec::new();
        for plan in std::iter::once(edge_plan).chain(edge_fallback_plans.into_iter()) {
            let PlannedEdgeQuery {
                driver,
                cap_context,
                warnings: _,
                mut followups,
            } = plan;
            let materialized = self.materialize_edge_physical_plan(edge_query, cap_context, &driver)?;
            let candidate_ids = match materialized {
                CandidateMaterializationResult::Ready {
                    ids,
                    followups: mut materialization_followups,
                } => {
                    followups.append(&mut materialization_followups);
                    ids
                }
                CandidateMaterializationResult::TooBroad {
                    followups: mut materialization_followups,
                } => {
                    followups.append(&mut materialization_followups);
                    deferred_followups.append(&mut followups);
                    continue;
                }
            };
            let verified = self.verify_pattern_edge_anchor_candidates(
                &candidate_ids,
                &query.edges[edge_index],
                reference_time,
                policy_cutoffs,
            )?;
            let states = self.pattern_edge_anchor_states_from_verified(
                query,
                edge_index,
                &verified,
                policy_cutoffs,
            )?;
            deferred_followups.append(&mut followups);
            return Ok(PatternAnchorFrontier::Ready {
                states,
                followups: deferred_followups,
                expansion_order: Vec::new(),
                sort_anchor_alias: String::new(),
            });
            }
        Ok(PatternAnchorFrontier::TooBroad {
            followups: deferred_followups,
        })
    }

    fn pattern_anchor_frontier(
        &self,
        query: &NormalizedGraphPatternQuery,
        anchor: PatternAnchorPlan,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<PatternAnchorFrontier, EngineError> {
        match anchor {
            PatternAnchorPlan::Node {
                node_index,
                anchor_plan,
                expansion_order,
                sort_anchor_alias,
                ..
            } => match self.pattern_node_anchor_frontier(query, node_index, &anchor_plan, policy_cutoffs)? {
                PatternAnchorFrontier::Ready { states, followups, .. } => {
                    Ok(PatternAnchorFrontier::Ready {
                        states,
                        followups,
                        expansion_order,
                        sort_anchor_alias,
                    })
                }
                PatternAnchorFrontier::TooBroad { followups } => {
                    Ok(PatternAnchorFrontier::TooBroad { followups })
                }
            },
            PatternAnchorPlan::Edge {
                edge_index,
                edge_query,
                edge_plan,
                edge_fallback_plans,
                expansion_order,
                sort_anchor_alias,
                from_index,
                to_index,
                orientation_policy,
                ..
            } => {
                let edge = &query.edges[edge_index];
                debug_assert_eq!(from_index, edge.from_index);
                debug_assert_eq!(to_index, edge.to_index);
                debug_assert_eq!(
                    orientation_policy,
                    EdgeAnchorOrientationPolicy::from_direction(edge.direction)
                );
                match self.pattern_edge_anchor_frontier(
                    query,
                    edge_index,
                    &edge_query,
                    edge_plan,
                    edge_fallback_plans,
                    reference_time,
                    policy_cutoffs,
                )? {
                PatternAnchorFrontier::Ready { states, followups, .. } => {
                    Ok(PatternAnchorFrontier::Ready {
                        states,
                        followups,
                        expansion_order,
                        sort_anchor_alias,
                    })
                }
                PatternAnchorFrontier::TooBroad { followups } => {
                    Ok(PatternAnchorFrontier::TooBroad { followups })
                }
            }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_pattern_from_frontier(
        &self,
        query: &NormalizedGraphPatternQuery,
        expansion_order: &[usize],
        mut frontier: Vec<PatternExecutionState>,
        sort_anchor_alias: &str,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: Vec<SecondaryIndexReadFollowup>,
    ) -> Result<QueryExecutionOutcome<QueryPatternResult>, EngineError> {
        let mut matches = Vec::with_capacity(query.limit.saturating_add(1));
        let Some((&final_edge_index, prefix_order)) = expansion_order.split_last() else {
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
                reference_time,
                policy_cutoffs,
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
                reference_time,
                policy_cutoffs,
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

    fn query_pattern_planned(
        &self,
        query: &NormalizedGraphPatternQuery,
        planned: PlannedPatternQuery,
    ) -> Result<QueryExecutionOutcome<QueryPatternResult>, EngineError> {
        match query.order {
            PatternOrder::AnchorThenAliasesAsc => {}
        }

        let policy_cutoffs = self.query_policy_cutoffs();
        let pattern_reference_time = query.at_epoch.unwrap_or_else(now_millis);
        let mut deferred_followups = Vec::new();
        for anchor in std::iter::once(planned.anchor).chain(planned.fallback_anchors.into_iter()) {
            match self.pattern_anchor_frontier(
                query,
                anchor,
                pattern_reference_time,
                policy_cutoffs.as_ref(),
            )? {
                PatternAnchorFrontier::Ready {
                    states,
                    mut followups,
                    expansion_order,
                    sort_anchor_alias,
                } => {
                    deferred_followups.append(&mut followups);
                    return self.finish_pattern_from_frontier(
                        query,
                        &expansion_order,
                        states,
                        &sort_anchor_alias,
                        pattern_reference_time,
                        policy_cutoffs.as_ref(),
                        deferred_followups,
                    );
                }
                PatternAnchorFrontier::TooBroad { mut followups } => {
                    deferred_followups.append(&mut followups);
                }
            }
        }

        Err(EngineError::InvalidOperation(
            "pattern edge anchor source exceeded candidate cap".into(),
        ))
    }

    fn query_pattern_outcome(
        &self,
        query: &GraphPatternQuery,
    ) -> Result<QueryExecutionOutcome<QueryPatternResult>, EngineError> {
        let normalized = self.normalize_pattern_query(query)?;
        let planned = self.plan_normalized_pattern_query(&normalized)?;
        self.query_pattern_planned(&normalized, planned)
    }
}
