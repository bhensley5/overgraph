const QUERY_VERIFY_CHUNK: usize = 256;
const PROPERTY_IN_LINEAR_VERIFY_THRESHOLD: usize = 16;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowRuntimeGoal {
    AllRows,
    ExistsOne,
}

impl GraphRowRuntimeGoal {
    fn is_exists_one(self) -> bool {
        matches!(self, Self::ExistsOne)
    }

    fn reached(self, rows: usize) -> bool {
        self.is_exists_one() && rows > 0
    }
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
            EdgeQueryCandidateSourceKind::EdgeLabelIndex
                | EdgeQueryCandidateSourceKind::EdgeTripleIndex
                | EdgeQueryCandidateSourceKind::FromEndpointAdjacency
                | EdgeQueryCandidateSourceKind::ToEndpointAdjacency
                | EdgeQueryCandidateSourceKind::AnyEndpointAdjacency
                | EdgeQueryCandidateSourceKind::EdgeWeightIndex
                | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
                | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
                | EdgeQueryCandidateSourceKind::EdgeValidToIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
                | EdgeQueryCandidateSourceKind::CompoundEqualityIndex
                | EdgeQueryCandidateSourceKind::CompoundRangeIndex
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
            | EdgeCandidateMaterialization::CompoundPrefixIndex { .. }
            | EdgeCandidateMaterialization::CompoundRangeIndex { .. }
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

fn followup_is_compound_sidecar_failure(followup: &SecondaryIndexReadFollowup) -> bool {
    matches!(
        followup,
        SecondaryIndexReadFollowup::CompoundEqualitySidecarFailure { .. }
            | SecondaryIndexReadFollowup::CompoundRangeSidecarFailure { .. }
    )
}

fn edge_physical_plan_contains_compound(plan: &EdgePhysicalPlan) -> bool {
    match plan {
        EdgePhysicalPlan::Empty => false,
        EdgePhysicalPlan::Source(source) => matches!(
            source.materialization,
            EdgeCandidateMaterialization::CompoundPrefixIndex { .. }
                | EdgeCandidateMaterialization::CompoundRangeIndex { .. }
        ),
        EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
            inputs.iter().any(edge_physical_plan_contains_compound)
        }
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

fn property_in_filter_matches(
    candidate: &PropValue,
    values: &[PropValue],
    value_keys: &[Vec<u8>],
) -> bool {
    if values.len() <= PROPERTY_IN_LINEAR_VERIFY_THRESHOLD
        || values.len() != value_keys.len()
        || structural_value_contains_float_zero(candidate)
    {
        return values
            .iter()
            .any(|value| prop_values_equal_for_filter(candidate, value));
    }

    let candidate_key = prop_value_canonical_bytes(candidate);
    match value_keys.binary_search(&candidate_key) {
        Ok(index) => prop_values_equal_for_filter(candidate, &values[index]),
        Err(_) => false,
    }
}

fn node_query_meta_from_visibility(node_id: u64, meta: &NodeVisibilityMeta) -> NodeMetadataForQuery {
    NodeMetadataForQuery {
        id: node_id,
        label_ids: meta.label_ids,
        updated_at: meta.updated_at,
        weight: meta.weight,
    }
}

fn collect_node_filter_property_keys(filter: &NormalizedNodeFilter, keys: &mut Vec<String>) {
    match filter {
        NormalizedNodeFilter::PropertyEquals { key, .. }
        | NormalizedNodeFilter::PropertyIn { key, .. }
        | NormalizedNodeFilter::PropertyRange { key, .. }
        | NormalizedNodeFilter::PropertyExists { key }
        | NormalizedNodeFilter::PropertyMissing { key } => {
            if !keys.iter().any(|existing| existing == key) {
                keys.push(key.clone());
            }
        }
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            for child in children {
                collect_node_filter_property_keys(child, keys);
            }
        }
        NormalizedNodeFilter::Not(child) => collect_node_filter_property_keys(child, keys),
        NormalizedNodeFilter::AlwaysTrue
        | NormalizedNodeFilter::AlwaysFalse
        | NormalizedNodeFilter::IdRange { .. }
        | NormalizedNodeFilter::KeyEquals(_)
        | NormalizedNodeFilter::KeyIn { .. }
        | NormalizedNodeFilter::WeightRange { .. }
        | NormalizedNodeFilter::CreatedAtRange { .. }
        | NormalizedNodeFilter::UpdatedAtRange { .. } => {}
    }
}

fn u64_flexible_range_matches(
    value: u64,
    lower: Option<u64>,
    upper: Option<u64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> bool {
    if let Some(lower) = lower {
        if value < lower || (value == lower && !lower_inclusive) {
            return false;
        }
    }
    if let Some(upper) = upper {
        if value > upper || (value == upper && !upper_inclusive) {
            return false;
        }
    }
    true
}

fn i64_flexible_range_matches(
    value: i64,
    lower: Option<i64>,
    upper: Option<i64>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> bool {
    if let Some(lower) = lower {
        if value < lower || (value == lower && !lower_inclusive) {
            return false;
        }
    }
    if let Some(upper) = upper {
        if value > upper || (value == upper && !upper_inclusive) {
            return false;
        }
    }
    true
}

fn f32_flexible_range_matches(
    value: f32,
    lower: Option<f32>,
    upper: Option<f32>,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> bool {
    if value.is_nan() {
        return false;
    }
    if let Some(lower) = lower {
        if value < lower || (value == lower && !lower_inclusive) {
            return false;
        }
    }
    if let Some(upper) = upper {
        if value > upper || (value == upper && !upper_inclusive) {
            return false;
        }
    }
    true
}

fn node_filter_needs_key(filter: &NormalizedNodeFilter) -> bool {
    match filter {
        NormalizedNodeFilter::KeyEquals(_) | NormalizedNodeFilter::KeyIn { .. } => true,
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            children.iter().any(node_filter_needs_key)
        }
        NormalizedNodeFilter::Not(child) => node_filter_needs_key(child),
        _ => false,
    }
}

fn node_filter_needs_created_at(filter: &NormalizedNodeFilter) -> bool {
    match filter {
        NormalizedNodeFilter::CreatedAtRange { .. } => true,
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            children.iter().any(node_filter_needs_created_at)
        }
        NormalizedNodeFilter::Not(child) => node_filter_needs_created_at(child),
        _ => false,
    }
}

fn node_filter_metadata_outcome(
    filter: &NormalizedNodeFilter,
    meta: &NodeMetadataForQuery,
) -> Option<bool> {
    match filter {
        NormalizedNodeFilter::AlwaysTrue => Some(true),
        NormalizedNodeFilter::AlwaysFalse => Some(false),
        NormalizedNodeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => Some(u64_flexible_range_matches(
            meta.id,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        )),
        NormalizedNodeFilter::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => Some(f32_flexible_range_matches(
            meta.weight,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        )),
        NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            Some(meta.updated_at >= *lower_ms && meta.updated_at <= *upper_ms)
        }
        NormalizedNodeFilter::And(children) => {
            let mut unknown = false;
            for child in children {
                match node_filter_metadata_outcome(child, meta) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(true) }
        }
        NormalizedNodeFilter::Or(children) => {
            let mut unknown = false;
            for child in children {
                match node_filter_metadata_outcome(child, meta) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(false) }
        }
        NormalizedNodeFilter::Not(child) => {
            node_filter_metadata_outcome(child, meta).map(|matched| !matched)
        }
        NormalizedNodeFilter::PropertyEquals { .. }
        | NormalizedNodeFilter::PropertyIn { .. }
        | NormalizedNodeFilter::PropertyRange { .. }
        | NormalizedNodeFilter::PropertyExists { .. }
        | NormalizedNodeFilter::PropertyMissing { .. }
        | NormalizedNodeFilter::KeyEquals(_)
        | NormalizedNodeFilter::KeyIn { .. }
        | NormalizedNodeFilter::CreatedAtRange { .. } => None,
    }
}

fn node_filter_projected_matches(
    filter: &NormalizedNodeFilter,
    selected: &SelectedNodeFields,
) -> bool {
    match filter {
        NormalizedNodeFilter::AlwaysTrue => true,
        NormalizedNodeFilter::AlwaysFalse => false,
        NormalizedNodeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => u64_flexible_range_matches(
            selected.meta.id,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
        NormalizedNodeFilter::KeyEquals(value) => {
            selected.key.as_deref() == Some(value.as_str())
        }
        NormalizedNodeFilter::KeyIn { values } => selected
            .key
            .as_ref()
            .is_some_and(|key| values.binary_search(key).is_ok()),
        NormalizedNodeFilter::PropertyEquals { key, value } => selected
            .props
            .get(key)
            .is_some_and(|candidate| prop_values_equal_for_filter(candidate, value)),
        NormalizedNodeFilter::PropertyIn {
            key,
            values,
            value_keys,
        } => selected
            .props
            .get(key)
            .is_some_and(|candidate| property_in_filter_matches(candidate, values, value_keys)),
        NormalizedNodeFilter::PropertyRange { key, lower, upper } => selected
            .props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
        NormalizedNodeFilter::PropertyExists { key } => selected.props.contains_key(key),
        NormalizedNodeFilter::PropertyMissing { key } => !selected.props.contains_key(key),
        NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            selected.meta.updated_at >= *lower_ms && selected.meta.updated_at <= *upper_ms
        }
        NormalizedNodeFilter::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => f32_flexible_range_matches(
            selected.meta.weight,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
        NormalizedNodeFilter::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => selected.created_at.is_some_and(|created_at| {
            i64_flexible_range_matches(
                created_at,
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
            )
        }),
        NormalizedNodeFilter::And(children) => children
            .iter()
            .all(|child| node_filter_projected_matches(child, selected)),
        NormalizedNodeFilter::Or(children) => children
            .iter()
            .any(|child| node_filter_projected_matches(child, selected)),
        NormalizedNodeFilter::Not(child) => !node_filter_projected_matches(child, selected),
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

fn query_node_metadata_constraints_match(
    query: &NormalizedNodeQuery,
    meta: &NodeMetadataForQuery,
    policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
) -> bool {
    if !node_label_filter_matches(&query.label_filter, &meta.label_ids) {
        return false;
    }
    if !query.ids.is_empty() && query.ids.binary_search(&meta.id).is_err() {
        return false;
    }
    if policy_cutoffs.is_some_and(|cutoffs| {
        cutoffs.excludes_fields(&meta.label_ids, meta.updated_at, meta.weight)
    }) {
        return false;
    }
    true
}

fn query_node_selected_fields_match(query: &NormalizedNodeQuery, selected: &SelectedNodeFields) -> bool {
    if !query.keys.is_empty()
        && selected
            .key
            .as_ref()
            .is_none_or(|key| query.keys.binary_search(key).is_err())
    {
        return false;
    }
    node_filter_projected_matches(&query.filter, selected)
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
        | NormalizedEdgeFilter::PropertyMissing { .. }
        | NormalizedEdgeFilter::CreatedAtRange { .. } => true,
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().any(edge_filter_requires_hydration)
        }
        NormalizedEdgeFilter::Not(child) => edge_filter_requires_hydration(child),
        _ => false,
    }
}

fn edge_filter_metadata_outcome(
    filter: &NormalizedEdgeFilter,
    meta: &EdgeMetadataForQuery,
) -> Option<bool> {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => Some(true),
        NormalizedEdgeFilter::AlwaysFalse => Some(false),
        NormalizedEdgeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => Some(u64_flexible_range_matches(
            meta.id,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        )),
        NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. }
        | NormalizedEdgeFilter::CreatedAtRange { .. } => None,
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
        NormalizedEdgeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => u64_flexible_range_matches(
            edge.id,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
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
        NormalizedEdgeFilter::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => i64_flexible_range_matches(
            edge.created_at,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
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
        | NormalizedEdgeFilter::IdRange { .. }
        | NormalizedEdgeFilter::WeightRange { .. }
        | NormalizedEdgeFilter::CreatedAtRange { .. }
        | NormalizedEdgeFilter::UpdatedAtRange { .. }
        | NormalizedEdgeFilter::ValidAt { .. }
        | NormalizedEdgeFilter::ValidFromRange { .. }
        | NormalizedEdgeFilter::ValidToRange { .. } => {}
    }
}

fn edge_filter_needs_created_at(filter: &NormalizedEdgeFilter) -> bool {
    match filter {
        NormalizedEdgeFilter::CreatedAtRange { .. } => true,
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().any(edge_filter_needs_created_at)
        }
        NormalizedEdgeFilter::Not(child) => edge_filter_needs_created_at(child),
        _ => false,
    }
}

fn edge_filter_projected_matches(
    filter: &NormalizedEdgeFilter,
    selected: &SelectedEdgeFields,
) -> bool {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue => true,
        NormalizedEdgeFilter::AlwaysFalse => false,
        NormalizedEdgeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => u64_flexible_range_matches(
            selected.meta.id,
            *lower,
            *upper,
            *lower_inclusive,
            *upper_inclusive,
        ),
        NormalizedEdgeFilter::PropertyEquals { key, value } => selected
            .props
            .get(key)
            .is_some_and(|candidate| prop_values_equal_for_filter(candidate, value)),
        NormalizedEdgeFilter::PropertyIn {
            key,
            values,
            value_keys,
        } => selected
            .props
            .get(key)
            .is_some_and(|candidate| property_in_filter_matches(candidate, values, value_keys)),
        NormalizedEdgeFilter::PropertyRange { key, lower, upper } => selected
            .props
            .get(key)
            .and_then(|value| range_value_within_bounds(value, lower.as_ref(), upper.as_ref()))
            == Some(true),
        NormalizedEdgeFilter::PropertyExists { key } => selected.props.contains_key(key),
        NormalizedEdgeFilter::PropertyMissing { key } => !selected.props.contains_key(key),
        NormalizedEdgeFilter::WeightRange { lower, upper } => {
            edge_weight_range_matches(selected.meta.weight, *lower, *upper)
        }
        NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
            i64_range_matches(selected.meta.updated_at, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => selected.created_at.is_some_and(|created_at| {
            i64_flexible_range_matches(
                created_at,
                *lower,
                *upper,
                *lower_inclusive,
                *upper_inclusive,
            )
        }),
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            selected.meta.valid_from <= *epoch_ms && *epoch_ms < selected.meta.valid_to
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
            i64_range_matches(selected.meta.valid_from, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
            i64_range_matches(selected.meta.valid_to, *lower_ms, *upper_ms)
        }
        NormalizedEdgeFilter::And(children) => children
            .iter()
            .all(|child| edge_filter_projected_matches(child, selected)),
        NormalizedEdgeFilter::Or(children) => children
            .iter()
            .any(|child| edge_filter_projected_matches(child, selected)),
        NormalizedEdgeFilter::Not(child) => !edge_filter_projected_matches(child, selected),
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
struct GraphRowRuntimeNode {
    alias: String,
    slot: crate::graph_row::GraphBindingSlotRef,
    query: NormalizedNodeQuery,
}

#[derive(Clone)]
struct GraphRowRuntimeEdge {
    alias: Option<String>,
    edge_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    hidden_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    from_alias: String,
    to_alias: String,
    from_slot: crate::graph_row::GraphBindingSlotRef,
    to_slot: crate::graph_row::GraphBindingSlotRef,
    direction: Direction,
    candidate_edge_ids: Vec<u64>,
    label_filter_ids: Option<Vec<u32>>,
    filter: NormalizedEdgeFilter,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone)]
struct GraphRowRuntimeVariableLength {
    piece_index: usize,
    path_alias: Option<String>,
    edge_alias: Option<String>,
    path_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    edge_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    hidden_slot: Option<crate::graph_row::GraphBindingSlotRef>,
    from_alias: String,
    to_alias: String,
    from_slot: crate::graph_row::GraphBindingSlotRef,
    to_slot: crate::graph_row::GraphBindingSlotRef,
    direction: Direction,
    candidate_edge_ids: Vec<u64>,
    label_filter_ids: Option<Vec<u32>>,
    filter: NormalizedEdgeFilter,
    min_hops: u8,
    max_hops: u8,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone)]
struct GraphRowRuntimeFixedPath {
    alias: String,
    path_slot: crate::graph_row::GraphBindingSlotRef,
    node_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    edge_slots: Vec<GraphRowRuntimeFixedPathEdgeSlot>,
}

#[derive(Clone, Copy)]
enum GraphRowRuntimeFixedPathEdgeSlot {
    Edge(crate::graph_row::GraphBindingSlotRef),
    Hidden(crate::graph_row::GraphBindingSlotRef),
}

struct GraphRowRuntimePlan {
    nodes: Vec<GraphRowRuntimeNode>,
    node_by_alias: BTreeMap<String, usize>,
    edges: Vec<GraphRowRuntimeEdge>,
    required_segments: Vec<GraphRowRequiredSegment>,
    steps: Vec<GraphRowRuntimeStep>,
    warnings: Vec<QueryPlanWarning>,
}

enum GraphRowRuntimeStep {
    RequiredSegment(usize),
    FixedPath(GraphRowRuntimeFixedPath),
    Optional(GraphRowRuntimeOptionalGroup),
    VariableLength(GraphRowRuntimeVariableLength),
}

struct GraphRowRuntimeOptionalGroup {
    piece_index: usize,
    pieces_len: usize,
    runtime: Box<GraphRowRuntimePlan>,
    introduced_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    dependency_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    left_slots: Vec<crate::graph_row::GraphBindingSlotRef>,
    where_expr: Option<crate::graph_row::BoundGraphExpr>,
    where_needs: EntityProjectionNeeds,
    where_present: bool,
}

struct GraphRowOptionalLeftGroup {
    key: Vec<crate::graph_row::GraphSortAtom>,
    representative: crate::graph_row::GraphBindingRow,
    rows: Vec<crate::graph_row::GraphBindingRow>,
}

struct GraphRowPathSearchSeed {
    node_id: u64,
    reverse: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct GraphRowVlpSearchKey {
    bound_from: Option<u64>,
    bound_to: Option<u64>,
}

struct GraphRowVlpSearchResult {
    seed_count: usize,
    paths: Vec<GraphPath>,
}

struct GraphRowPartialPath {
    current: u64,
    nodes: Vec<u64>,
    edges: Vec<u64>,
}

#[derive(Clone, Copy)]
struct GraphRowVlpStepEdge {
    edge_id: u64,
    next_node: u64,
}

#[derive(Clone, Debug)]
struct GraphRowRequiredSegment {
    edge_indices: Vec<usize>,
    barriers_before: Vec<GraphRowPlanBarrier>,
}

#[derive(Clone, Debug)]
struct GraphRowPlanBarrier {
    kind: GraphRowPlanBarrierKind,
    piece_index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowPlanBarrierKind {
    Optional,
    VariableLength,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowEdgeCandidateSourceChoice {
    ExplicitIds,
    EndpointAdjacency,
    EdgeCandidateSource,
    MixedEndpointAndEdgeSource,
    EmptyResult,
    SkippedEmptyFrontier,
}

enum GraphRowEdgeSourceRead {
    Ready {
        ids: Vec<u64>,
        followups: Vec<SecondaryIndexReadFollowup>,
        materialized_source: String,
        subset_source: Option<String>,
    },
    TooBroad {
        followups: Vec<SecondaryIndexReadFollowup>,
        planned_source: String,
    },
    NoLegalSource,
}

#[derive(Clone, Copy)]
struct GraphRowOrientedEdge {
    meta: EdgeMetadataForQuery,
    logical_from: u64,
    logical_to: u64,
}

struct GraphRowEdgeCandidateBuckets {
    by_pair: HashMap<(u64, u64), Vec<usize>>,
    by_from: NodeIdMap<Vec<usize>>,
    by_to: NodeIdMap<Vec<usize>>,
}

fn graph_row_cap_error(name: &str, cap: usize) -> EngineError {
    EngineError::InvalidOperation(format!("graph row {name} exceeded configured cap {cap}"))
}

fn graph_row_vlp_cap_error(
    name: &str,
    cap: usize,
    path: &GraphRowRuntimeVariableLength,
) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row {name} exceeded configured cap {cap}; path={}; piece_index={}",
        graph_row_vlp_context(path),
        path.piece_index
    ))
}

fn graph_row_vlp_context(path: &GraphRowRuntimeVariableLength) -> String {
    path.path_alias
        .as_deref()
        .or(path.edge_alias.as_deref())
        .map(str::to_string)
        .unwrap_or_else(|| format!("variable_length_piece_{}", path.piece_index))
}

fn graph_row_record_frontier_cap_peak(
    peak: &mut usize,
    len: usize,
    cap: usize,
    vlp_context: Option<&GraphRowRuntimeVariableLength>,
) -> Result<(), EngineError> {
    *peak = (*peak).max(len);
    if len > cap {
        return Err(match vlp_context {
            Some(path) => graph_row_vlp_cap_error("max_frontier", cap, path),
            None => graph_row_cap_error("max_frontier", cap),
        });
    }
    Ok(())
}

fn graph_row_reverse_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn graph_row_materialize_partial_path(partial: &GraphRowPartialPath, reverse: bool) -> GraphPath {
    if reverse {
        let mut nodes = partial.nodes.clone();
        let mut edges = partial.edges.clone();
        nodes.reverse();
        edges.reverse();
        GraphPath { nodes, edges }
    } else {
        GraphPath {
            nodes: partial.nodes.clone(),
            edges: partial.edges.clone(),
        }
    }
}

fn graph_row_runtime_node<'a>(
    runtime: &'a GraphRowRuntimePlan,
    alias: &str,
) -> Result<&'a GraphRowRuntimeNode, EngineError> {
    let index = runtime.node_by_alias.get(alias).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "graph row runtime is missing node alias '{alias}'"
        ))
    })?;
    runtime.nodes.get(*index).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "graph row runtime node index {index} for alias '{alias}' is out of bounds"
        ))
    })
}

fn graph_row_record_cap_peak(
    peak: &mut usize,
    len: usize,
    name: &str,
    cap: usize,
) -> Result<(), EngineError> {
    *peak = (*peak).max(len);
    if len > cap {
        return Err(graph_row_cap_error(name, cap));
    }
    Ok(())
}

fn graph_row_push_optional_joined_row(
    query: &NormalizedGraphRowQuery,
    rows: &mut Vec<crate::graph_row::GraphBindingRow>,
    row: crate::graph_row::GraphBindingRow,
) -> Result<(), EngineError> {
    rows.push(row);
    if rows.len() > query.options.max_intermediate_bindings {
        return Err(graph_row_cap_error(
            "max_intermediate_bindings",
            query.options.max_intermediate_bindings,
        ));
    }
    Ok(())
}

fn graph_row_any_slot_null(
    row: &crate::graph_row::GraphBindingRow,
    slots: &[crate::graph_row::GraphBindingSlotRef],
) -> Result<bool, EngineError> {
    for slot in slots {
        if row.slot_is_null(*slot)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn graph_row_slot_list_detail(
    query: &NormalizedGraphRowQuery,
    slots: &[crate::graph_row::GraphBindingSlotRef],
) -> String {
    if slots.is_empty() {
        return "none".to_string();
    }
    slots
        .iter()
        .map(|slot| {
            query
                .binding_schema
                .slot(*slot)
                .map(|slot| format!("{}:{:?}", slot.name, slot.kind))
                .unwrap_or_else(|| format!("{:?}:{}", slot.kind, slot.index))
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn graph_row_node_query_has_anchor(query: &NormalizedNodeQuery) -> bool {
    !query.ids.is_empty()
        || matches!(
            query.label_filter,
            ResolvedNodeLabelFilter::LabelSet { .. } | ResolvedNodeLabelFilter::Empty { .. }
        )
        || !query.filter.is_always_true()
        || query.filter.is_always_false()
}

fn graph_row_collect_endpoint_sources(
    pattern_direction: Direction,
    is_from_alias: bool,
    node_id: Option<u64>,
    outgoing: &mut Vec<u64>,
    incoming: &mut Vec<u64>,
    both: &mut Vec<u64>,
) {
    let Some(node_id) = node_id else {
        return;
    };
    match (pattern_direction, is_from_alias) {
        (Direction::Outgoing, true) | (Direction::Incoming, false) => outgoing.push(node_id),
        (Direction::Outgoing, false) | (Direction::Incoming, true) => incoming.push(node_id),
        (Direction::Both, _) => both.push(node_id),
    }
}

fn graph_row_push_required_segment(
    segments: &mut Vec<GraphRowRequiredSegment>,
    current_edges: &mut Vec<usize>,
    pending_barriers: &mut Vec<GraphRowPlanBarrier>,
) -> Option<usize> {
    if current_edges.is_empty() {
        return None;
    }
    let segment_index = segments.len();
    segments.push(GraphRowRequiredSegment {
        edge_indices: std::mem::take(current_edges),
        barriers_before: std::mem::take(pending_barriers),
    });
    Some(segment_index)
}

fn graph_row_fixed_paths_for_scope<'a>(
    query: &'a NormalizedGraphRowQuery,
    scope: &[usize],
) -> Vec<&'a GraphFixedPathBinding> {
    let mut fixed_paths = query
        .fixed_paths
        .iter()
        .filter(|path| path.scope.as_slice() == scope)
        .collect::<Vec<_>>();
    fixed_paths.sort_by(|left, right| {
        left.after_piece_index
            .cmp(&right.after_piece_index)
            .then_with(|| left.alias.cmp(&right.alias))
    });
    fixed_paths
}

#[allow(clippy::too_many_arguments)]
fn graph_row_push_fixed_path_steps_before_piece(
    query: &NormalizedGraphRowQuery,
    fixed_paths: &[&GraphFixedPathBinding],
    piece_index: usize,
    edge_by_piece: &BTreeMap<usize, usize>,
    nodes: &[GraphRowRuntimeNode],
    node_by_alias: &BTreeMap<String, usize>,
    edges: &[GraphRowRuntimeEdge],
    next_fixed_path: &mut usize,
    steps: &mut Vec<GraphRowRuntimeStep>,
    bound_slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<(), EngineError> {
    while fixed_paths
        .get(*next_fixed_path)
        .is_some_and(|path| path.after_piece_index < piece_index)
    {
        let runtime = graph_row_runtime_fixed_path(
            query,
            fixed_paths[*next_fixed_path],
            edge_by_piece,
            nodes,
            node_by_alias,
            edges,
        )?;
        bound_slots.insert(runtime.path_slot);
        steps.push(GraphRowRuntimeStep::FixedPath(runtime));
        *next_fixed_path += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn graph_row_push_remaining_fixed_path_steps(
    query: &NormalizedGraphRowQuery,
    fixed_paths: &[&GraphFixedPathBinding],
    edge_by_piece: &BTreeMap<usize, usize>,
    nodes: &[GraphRowRuntimeNode],
    node_by_alias: &BTreeMap<String, usize>,
    edges: &[GraphRowRuntimeEdge],
    next_fixed_path: &mut usize,
    steps: &mut Vec<GraphRowRuntimeStep>,
    bound_slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<(), EngineError> {
    while let Some(fixed_path) = fixed_paths.get(*next_fixed_path) {
        let runtime = graph_row_runtime_fixed_path(
            query,
            fixed_path,
            edge_by_piece,
            nodes,
            node_by_alias,
            edges,
        )?;
        bound_slots.insert(runtime.path_slot);
        steps.push(GraphRowRuntimeStep::FixedPath(runtime));
        *next_fixed_path += 1;
    }
    Ok(())
}

fn graph_row_runtime_fixed_path(
    query: &NormalizedGraphRowQuery,
    fixed_path: &GraphFixedPathBinding,
    edge_by_piece: &BTreeMap<usize, usize>,
    nodes: &[GraphRowRuntimeNode],
    node_by_alias: &BTreeMap<String, usize>,
    edges: &[GraphRowRuntimeEdge],
) -> Result<GraphRowRuntimeFixedPath, EngineError> {
    let path_slot = query
        .binding_schema
        .slot_for_alias(&fixed_path.alias)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row fixed path alias '{}' is missing from binding schema",
                fixed_path.alias
            ))
        })?;
    let node_slots = fixed_path
        .node_aliases
        .iter()
        .map(|alias| {
            let index = node_by_alias.get(alias).copied().ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row fixed path '{}' references missing node alias '{}'",
                    fixed_path.alias, alias
                ))
            })?;
            Ok(nodes[index].slot)
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let edge_slots = fixed_path
        .edge_piece_indices
        .iter()
        .map(|piece_index| {
            let edge_index = edge_by_piece.get(piece_index).copied().ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row fixed path '{}' references missing fixed edge piece {}",
                    fixed_path.alias, piece_index
                ))
            })?;
            let edge = edges.get(edge_index).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row fixed path '{}' references missing runtime edge {}",
                    fixed_path.alias, edge_index
                ))
            })?;
            if let Some(slot) = edge.edge_slot {
                Ok(GraphRowRuntimeFixedPathEdgeSlot::Edge(slot))
            } else if let Some(slot) = edge.hidden_slot {
                Ok(GraphRowRuntimeFixedPathEdgeSlot::Hidden(slot))
            } else {
                Err(EngineError::InvalidOperation(format!(
                    "graph row fixed path '{}' edge piece {} has no edge occurrence slot",
                    fixed_path.alias, piece_index
                )))
            }
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(GraphRowRuntimeFixedPath {
        alias: fixed_path.alias.clone(),
        path_slot,
        node_slots,
        edge_slots,
    })
}

fn graph_row_optional_dependency_slots(
    group: &GraphOptionalGroup,
    schema: &crate::graph_row::GraphBindingSchema,
    prior_slots: &BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<Vec<crate::graph_row::GraphBindingSlotRef>, EngineError> {
    let mut slots = BTreeSet::new();
    for piece in &group.pieces {
        graph_row_collect_piece_dependency_slots(piece, schema, prior_slots, &mut slots)?;
    }
    if let Some(expr) = group.where_.as_ref() {
        graph_row_collect_expr_dependency_slots(expr, schema, prior_slots, &mut slots)?;
    }
    Ok(slots.into_iter().collect())
}

fn graph_row_collect_piece_dependency_slots(
    piece: &GraphPatternPiece,
    schema: &crate::graph_row::GraphBindingSchema,
    prior_slots: &BTreeSet<crate::graph_row::GraphBindingSlotRef>,
    slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<(), EngineError> {
    match piece {
        GraphPatternPiece::Edge(edge) => {
            graph_row_maybe_collect_dependency_alias(
                &edge.from_alias,
                schema,
                prior_slots,
                slots,
            )?;
            graph_row_maybe_collect_dependency_alias(&edge.to_alias, schema, prior_slots, slots)?;
        }
        GraphPatternPiece::Optional(group) => {
            for child in &group.pieces {
                graph_row_collect_piece_dependency_slots(child, schema, prior_slots, slots)?;
            }
            if let Some(expr) = group.where_.as_ref() {
                graph_row_collect_expr_dependency_slots(expr, schema, prior_slots, slots)?;
            }
        }
        GraphPatternPiece::VariableLength(path) => {
            graph_row_maybe_collect_dependency_alias(
                &path.from_alias,
                schema,
                prior_slots,
                slots,
            )?;
            graph_row_maybe_collect_dependency_alias(&path.to_alias, schema, prior_slots, slots)?;
        }
    }
    Ok(())
}

fn graph_row_collect_expr_dependency_slots(
    expr: &GraphExpr,
    schema: &crate::graph_row::GraphBindingSchema,
    prior_slots: &BTreeSet<crate::graph_row::GraphBindingSlotRef>,
    slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<(), EngineError> {
    match expr {
        GraphExpr::Binding(alias)
        | GraphExpr::Property { alias, .. }
        | GraphExpr::NodeField { alias, .. }
        | GraphExpr::EdgeField { alias, .. }
        | GraphExpr::PathField { alias, .. } => {
            graph_row_maybe_collect_dependency_alias(alias, schema, prior_slots, slots)?;
        }
        GraphExpr::List(items) => {
            for item in items {
                graph_row_collect_expr_dependency_slots(item, schema, prior_slots, slots)?;
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                graph_row_collect_expr_dependency_slots(item, schema, prior_slots, slots)?;
            }
        }
        GraphExpr::Function { args, .. } => {
            for arg in args {
                graph_row_collect_expr_dependency_slots(arg, schema, prior_slots, slots)?;
            }
        }
        GraphExpr::AggregateCall { arg, .. } => {
            if let Some(arg) = arg {
                graph_row_collect_expr_dependency_slots(arg, schema, prior_slots, slots)?;
            }
        }
        GraphExpr::ExistsSubquery(_) => {}
        GraphExpr::Unary { expr, .. }
        | GraphExpr::IsNull(expr)
        | GraphExpr::IsNotNull(expr) => {
            graph_row_collect_expr_dependency_slots(expr, schema, prior_slots, slots)?;
        }
        GraphExpr::Binary { left, right, .. } => {
            graph_row_collect_expr_dependency_slots(left, schema, prior_slots, slots)?;
            graph_row_collect_expr_dependency_slots(right, schema, prior_slots, slots)?;
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                graph_row_collect_expr_dependency_slots(operand, schema, prior_slots, slots)?;
            }
            for branch in branches {
                graph_row_collect_expr_dependency_slots(
                    &branch.when,
                    schema,
                    prior_slots,
                    slots,
                )?;
                graph_row_collect_expr_dependency_slots(
                    &branch.then,
                    schema,
                    prior_slots,
                    slots,
                )?;
            }
            if let Some(else_expr) = else_expr {
                graph_row_collect_expr_dependency_slots(else_expr, schema, prior_slots, slots)?;
            }
        }
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_)
        | GraphExpr::Param(_) => {}
    }
    Ok(())
}

fn graph_row_maybe_collect_dependency_alias(
    alias: &str,
    schema: &crate::graph_row::GraphBindingSchema,
    prior_slots: &BTreeSet<crate::graph_row::GraphBindingSlotRef>,
    slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
) -> Result<(), EngineError> {
    let slot = schema.slot_for_alias(alias).ok_or_else(|| {
        EngineError::InvalidOperation(format!("graph row references unknown binding '{alias}'"))
    })?;
    if prior_slots.contains(&slot) {
        slots.insert(slot);
    }
    Ok(())
}

fn graph_row_source_choice_label(choice: GraphRowEdgeCandidateSourceChoice) -> &'static str {
    match choice {
        GraphRowEdgeCandidateSourceChoice::ExplicitIds => "ExplicitEdgeIds",
        GraphRowEdgeCandidateSourceChoice::EndpointAdjacency => "EndpointAdjacency",
        GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource => "EdgeCandidateSource",
        GraphRowEdgeCandidateSourceChoice::MixedEndpointAndEdgeSource => {
            "MixedEndpointAndEdgeSource"
        }
        GraphRowEdgeCandidateSourceChoice::EmptyResult => "EmptyResult",
        GraphRowEdgeCandidateSourceChoice::SkippedEmptyFrontier => "SkippedEmptyFrontier",
    }
}

fn graph_row_edge_orientations(
    direction: Direction,
    meta: EdgeMetadataForQuery,
) -> Vec<(u64, u64)> {
    match direction {
        Direction::Outgoing => vec![(meta.from, meta.to)],
        Direction::Incoming => vec![(meta.to, meta.from)],
        Direction::Both if meta.from == meta.to => vec![(meta.from, meta.to)],
        Direction::Both => vec![(meta.from, meta.to), (meta.to, meta.from)],
    }
}

impl GraphRowEdgeCandidateBuckets {
    fn new(candidates: &[GraphRowOrientedEdge]) -> Self {
        let mut by_pair: HashMap<(u64, u64), Vec<usize>> = HashMap::new();
        let mut by_from: NodeIdMap<Vec<usize>> = NodeIdMap::default();
        let mut by_to: NodeIdMap<Vec<usize>> = NodeIdMap::default();
        for (index, candidate) in candidates.iter().enumerate() {
            by_pair
                .entry((candidate.logical_from, candidate.logical_to))
                .or_default()
                .push(index);
            by_from
                .entry(candidate.logical_from)
                .or_default()
                .push(index);
            by_to.entry(candidate.logical_to).or_default().push(index);
        }
        Self {
            by_pair,
            by_from,
            by_to,
        }
    }

    fn indices_for(&self, from: Option<u64>, to: Option<u64>) -> Option<&[usize]> {
        match (from, to) {
            (Some(from), Some(to)) => self
                .by_pair
                .get(&(from, to))
                .map(|indices| indices.as_slice()),
            (Some(from), None) => self.by_from.get(&from).map(|indices| indices.as_slice()),
            (None, Some(to)) => self.by_to.get(&to).map(|indices| indices.as_slice()),
            (None, None) => None,
        }
    }
}

fn compare_graph_logical_keys(
    left: &[crate::graph_row::GraphSortAtom],
    right: &[crate::graph_row::GraphSortAtom],
) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let ordering = crate::graph_row::compare_graph_sort_atoms(left, right);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

#[derive(Clone)]
struct GraphRowPageCandidate {
    sort_key: Vec<crate::graph_row::GraphSortAtom>,
    logical_key: Vec<crate::graph_row::GraphSortAtom>,
    row: crate::graph_row::GraphBindingRow,
}

#[derive(Clone, Debug)]
struct GraphRowCursorPayload {
    effective_at_epoch: i64,
    original_skip: u64,
    page_sequence: u64,
    rows_emitted_after_skip: u64,
    query_fingerprint: u128,
    order_fingerprint: u128,
    output_fingerprint: u128,
    params_fingerprint: u128,
    last_sort_key: Vec<crate::graph_row::GraphSortAtom>,
    last_logical_row_key: Vec<crate::graph_row::GraphSortAtom>,
}

#[derive(Clone)]
struct GraphRowCursorState {
    decoded: Option<GraphRowCursorPayload>,
    effective_at_epoch: i64,
    original_skip: u64,
    rows_emitted_after_skip: u64,
}

impl GraphRowCursorState {
    fn is_cursor_page(&self) -> bool {
        self.decoded.is_some()
    }
}

#[derive(Clone, Copy)]
struct GraphRowCursorFingerprints {
    query: u128,
    order: u128,
    output: u128,
    params: u128,
}

const GRAPH_ROW_CURSOR_PREFIX: &str = "ogr32c1_";
const GRAPH_ROW_CURSOR_MAGIC: &[u8; 8] = b"OGR32CUR";
const GRAPH_ROW_CURSOR_VERSION: u8 = 1;
const GRAPH_ROW_CURSOR_SEMANTIC_VERSION: u16 = 2;
const GRAPH_ROW_CURSOR_FLAGS: u16 = 0;
const FNV128_OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
const FNV128_PRIME: u128 = 0x0000000001000000000000000000013b;

fn graph_row_explicit_sort_key(
    query: &NormalizedGraphRowQuery,
    row: &crate::graph_row::GraphBindingRow,
) -> Result<Vec<crate::graph_row::GraphSortAtom>, EngineError> {
    if query.bound_order_by.is_empty() {
        return Ok(Vec::new());
    }
    let context = crate::graph_row::BoundGraphEvalContext { row };
    query
        .bound_order_by
        .iter()
        .map(|item| {
            let value = crate::graph_row::eval_bound_graph_expr(&item.expr, &context)?;
            crate::graph_row::graph_sort_atom_for_value(&value)
        })
        .collect()
}

#[derive(Clone, Eq, PartialEq)]
struct GraphRowFinalSortKey {
    sort_key: Vec<crate::graph_row::GraphSortAtom>,
    logical_key: Vec<crate::graph_row::GraphSortAtom>,
    directions: Arc<[GraphOrderDirection]>,
}

impl Ord for GraphRowFinalSortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_graph_final_keys_by_directions(
            &self.sort_key,
            &self.logical_key,
            &other.sort_key,
            &other.logical_key,
            &self.directions,
        )
    }
}

impl PartialOrd for GraphRowFinalSortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
struct GraphRowHeapCandidate {
    key: GraphRowFinalSortKey,
    candidate: GraphRowPageCandidate,
}

impl Eq for GraphRowHeapCandidate {}

impl PartialEq for GraphRowHeapCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for GraphRowHeapCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for GraphRowHeapCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn graph_row_insert_bounded_candidate(
    selected: &mut BinaryHeap<GraphRowHeapCandidate>,
    candidate: GraphRowPageCandidate,
    capacity: usize,
    directions: &Arc<[GraphOrderDirection]>,
) {
    if capacity == 0 {
        return;
    }
    let key = GraphRowFinalSortKey {
        sort_key: candidate.sort_key.clone(),
        logical_key: candidate.logical_key.clone(),
        directions: Arc::clone(directions),
    };
    let heap_candidate = GraphRowHeapCandidate { key, candidate };
    if selected.len() < capacity {
        selected.push(heap_candidate);
        return;
    }
    if selected
        .peek()
        .is_some_and(|worst| heap_candidate.key < worst.key)
    {
        let _ = selected.pop();
        selected.push(heap_candidate);
    }
}

fn graph_row_order_directions(
    order_by: &[crate::graph_row::BoundGraphOrderItem],
) -> Arc<[GraphOrderDirection]> {
    order_by
        .iter()
        .map(|item| item.direction)
        .collect::<Vec<_>>()
        .into()
}

fn compare_graph_final_keys_by_directions(
    left_sort: &[crate::graph_row::GraphSortAtom],
    left_logical: &[crate::graph_row::GraphSortAtom],
    right_sort: &[crate::graph_row::GraphSortAtom],
    right_logical: &[crate::graph_row::GraphSortAtom],
    directions: &[GraphOrderDirection],
) -> std::cmp::Ordering {
    for (index, direction) in directions.iter().enumerate() {
        let Some(left) = left_sort.get(index) else {
            return left_sort.len().cmp(&right_sort.len());
        };
        let Some(right) = right_sort.get(index) else {
            return left_sort.len().cmp(&right_sort.len());
        };
        let mut ordering = crate::graph_row::compare_graph_sort_atoms(left, right);
        if *direction == GraphOrderDirection::Desc
            && !matches!(left, crate::graph_row::GraphSortAtom::Null)
            && !matches!(right, crate::graph_row::GraphSortAtom::Null)
        {
            ordering = ordering.reverse();
        }
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    compare_graph_logical_keys(left_logical, right_logical)
}

fn graph_row_decode_request_cursor(
    page: &GraphPageRequest,
    options: &GraphQueryOptions,
) -> Result<Option<GraphRowCursorPayload>, EngineError> {
    validate_graph_row_page(page, options)?;
    page.cursor
        .as_ref()
        .map(|cursor| graph_row_decode_cursor(cursor, options.max_cursor_bytes))
        .transpose()
}

fn graph_row_cursor_state_from_decoded(
    decoded_cursor: Option<GraphRowCursorPayload>,
    page: &GraphPageRequest,
    at_epoch: Option<i64>,
) -> Result<GraphRowCursorState, EngineError> {
    let effective_at_epoch = match (decoded_cursor.as_ref(), at_epoch) {
        (None, Some(epoch)) => epoch,
        (None, None) => now_millis(),
        (Some(cursor), None) => cursor.effective_at_epoch,
        (Some(cursor), Some(epoch)) if epoch == cursor.effective_at_epoch => epoch,
        (Some(cursor), Some(epoch)) => {
            return Err(invalid_graph_row_cursor(format!(
                "explicit at_epoch {epoch} does not match cursor epoch {}",
                cursor.effective_at_epoch
            )));
        }
    };
    let original_skip = match decoded_cursor.as_ref() {
        Some(cursor) => {
            let current_skip = page.skip as u64;
            if current_skip != 0 && current_skip != cursor.original_skip {
                return Err(invalid_graph_row_cursor(format!(
                    "cursor page skip {current_skip} does not match original skip {}",
                    cursor.original_skip
                )));
            }
            cursor.original_skip
        }
        None => page.skip as u64,
    };
    let rows_emitted_after_skip = decoded_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.rows_emitted_after_skip);
    Ok(GraphRowCursorState {
        decoded: decoded_cursor,
        effective_at_epoch,
        original_skip,
        rows_emitted_after_skip,
    })
}

fn graph_row_prepare_cursor_state(
    page: &GraphPageRequest,
    at_epoch: Option<i64>,
    options: &GraphQueryOptions,
) -> Result<GraphRowCursorState, EngineError> {
    let decoded = graph_row_decode_request_cursor(page, options)?;
    graph_row_cursor_state_from_decoded(decoded, page, at_epoch)
}

fn graph_row_validate_cursor_fingerprints(
    cursor: &GraphRowCursorPayload,
    fingerprints: &GraphRowCursorFingerprints,
) -> Result<(), EngineError> {
    if cursor.query_fingerprint != fingerprints.query {
        return Err(invalid_graph_row_cursor("query fingerprint mismatch"));
    }
    if cursor.order_fingerprint != fingerprints.order {
        return Err(invalid_graph_row_cursor("order fingerprint mismatch"));
    }
    if cursor.output_fingerprint != fingerprints.output {
        return Err(invalid_graph_row_cursor("output fingerprint mismatch"));
    }
    if cursor.params_fingerprint != fingerprints.params {
        return Err(invalid_graph_row_cursor("params fingerprint mismatch"));
    }
    Ok(())
}

fn graph_row_validate_cursor_shape(
    query: &NormalizedGraphRowQuery,
    cursor: &GraphRowCursorPayload,
) -> Result<(), EngineError> {
    if let Some(limit) = query.logical_limit {
        if cursor.rows_emitted_after_skip >= limit as u64 {
            return Err(invalid_graph_row_cursor(
                "cursor has already exhausted the logical row limit",
            ));
        }
    }
    if cursor.last_sort_key.len() != query.bound_order_by.len() {
        return Err(invalid_graph_row_cursor(format!(
            "cursor sort key has {} atom(s), expected {}",
            cursor.last_sort_key.len(),
            query.bound_order_by.len()
        )));
    }
    if cursor.last_logical_row_key.len() != query.binding_schema.slots().len() {
        return Err(invalid_graph_row_cursor(format!(
            "cursor logical row key has {} atom(s), expected {}",
            cursor.last_logical_row_key.len(),
            query.binding_schema.slots().len()
        )));
    }
    for (slot, atom) in query
        .binding_schema
        .slots()
        .iter()
        .zip(cursor.last_logical_row_key.iter())
    {
        if !graph_row_cursor_atom_matches_slot(atom, slot) {
            return Err(invalid_graph_row_cursor(format!(
                "cursor logical row key atom does not match slot '{}'",
                slot.name
            )));
        }
    }
    for (index, (item, atom)) in query
        .bound_order_by
        .iter()
        .zip(cursor.last_sort_key.iter())
        .enumerate()
    {
        let expectation =
            graph_row_cursor_order_atom_expectation(&item.expr, &query.binding_schema)?;
        if !graph_row_cursor_atom_matches_expectation(atom, expectation) {
            return Err(invalid_graph_row_cursor(format!(
                "cursor order key atom {} does not match order expression result kind",
                index + 1
            )));
        }
        if let crate::graph_row::GraphSortAtom::Path {
            hop_count,
            nodes,
            edges,
        } = atom
        {
            graph_row_validate_cursor_path_atom(*hop_count, nodes, edges)?;
        }
    }
    for atom in &cursor.last_logical_row_key {
        if let crate::graph_row::GraphSortAtom::Path {
            hop_count,
            nodes,
            edges,
        } = atom
        {
            graph_row_validate_cursor_path_atom(*hop_count, nodes, edges)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowCursorAtomExpectation {
    AnyOrderable,
    Unsupported,
    Scalar,
    Bool,
    Number,
    String,
    Bytes,
    Node,
    Slot(crate::graph_row::GraphBindingSlotKind, bool),
}

fn graph_row_cursor_order_atom_expectation(
    expr: &crate::graph_row::BoundGraphExpr,
    schema: &crate::graph_row::GraphBindingSchema,
) -> Result<GraphRowCursorAtomExpectation, EngineError> {
    use crate::graph_row::BoundGraphExpr;
    Ok(match expr {
        BoundGraphExpr::Null => GraphRowCursorAtomExpectation::AnyOrderable,
        BoundGraphExpr::Bool(_) => GraphRowCursorAtomExpectation::Bool,
        BoundGraphExpr::Int(_) | BoundGraphExpr::UInt(_) | BoundGraphExpr::Float(_) => {
            GraphRowCursorAtomExpectation::Number
        }
        BoundGraphExpr::String(_) => GraphRowCursorAtomExpectation::String,
        BoundGraphExpr::Bytes(_) => GraphRowCursorAtomExpectation::Bytes,
        BoundGraphExpr::List(_) | BoundGraphExpr::Map(_) => GraphRowCursorAtomExpectation::Unsupported,
        BoundGraphExpr::Binding(slot) => {
            let slot = schema
                .slot(*slot)
                .ok_or_else(|| invalid_graph_row_cursor("cursor order binding slot is not in schema"))?;
            GraphRowCursorAtomExpectation::Slot(slot.kind, slot.nullable)
        }
        BoundGraphExpr::Property { .. } => GraphRowCursorAtomExpectation::Scalar,
        BoundGraphExpr::NodeField { field, .. } => match field {
            GraphNodeField::Id
            | GraphNodeField::Weight
            | GraphNodeField::CreatedAt
            | GraphNodeField::UpdatedAt => GraphRowCursorAtomExpectation::Number,
            GraphNodeField::Key => GraphRowCursorAtomExpectation::String,
            GraphNodeField::Labels => GraphRowCursorAtomExpectation::Unsupported,
        },
        BoundGraphExpr::EdgeField { field, .. } => match field {
            GraphEdgeField::Id
            | GraphEdgeField::From
            | GraphEdgeField::To
            | GraphEdgeField::Weight
            | GraphEdgeField::CreatedAt
            | GraphEdgeField::UpdatedAt
            | GraphEdgeField::ValidFrom
            | GraphEdgeField::ValidTo => GraphRowCursorAtomExpectation::Number,
            GraphEdgeField::Label => GraphRowCursorAtomExpectation::String,
        },
        BoundGraphExpr::PathField { field, .. } => match field {
            GraphPathField::Length => GraphRowCursorAtomExpectation::Number,
            GraphPathField::NodeIds | GraphPathField::EdgeIds => {
                GraphRowCursorAtomExpectation::Unsupported
            }
        },
        BoundGraphExpr::Function { name, .. } => match name {
            GraphFunction::Id | GraphFunction::Length => GraphRowCursorAtomExpectation::Number,
            GraphFunction::Type => GraphRowCursorAtomExpectation::String,
            GraphFunction::StartNode | GraphFunction::EndNode => GraphRowCursorAtomExpectation::Node,
            GraphFunction::Labels | GraphFunction::Nodes | GraphFunction::Relationships => {
                GraphRowCursorAtomExpectation::Unsupported
            }
            GraphFunction::ToString
            | GraphFunction::Lower
            | GraphFunction::Upper
            | GraphFunction::Trim
            | GraphFunction::Substring => GraphRowCursorAtomExpectation::String,
            GraphFunction::ToInteger
            | GraphFunction::ToFloat
            | GraphFunction::Abs
            | GraphFunction::Floor
            | GraphFunction::Ceil
            | GraphFunction::Round
            | GraphFunction::Size => GraphRowCursorAtomExpectation::Number,
            GraphFunction::Coalesce | GraphFunction::Head | GraphFunction::Last => {
                GraphRowCursorAtomExpectation::AnyOrderable
            }
        },
        BoundGraphExpr::Unary { op, .. } => match op {
            GraphUnaryOp::Not => GraphRowCursorAtomExpectation::Bool,
            GraphUnaryOp::Neg => GraphRowCursorAtomExpectation::Number,
        },
        BoundGraphExpr::Binary { op, .. } => match op {
            GraphBinaryOp::Add | GraphBinaryOp::Sub | GraphBinaryOp::Mul | GraphBinaryOp::Div => {
                GraphRowCursorAtomExpectation::Number
            }
            GraphBinaryOp::And
            | GraphBinaryOp::Or
            | GraphBinaryOp::Eq
            | GraphBinaryOp::Neq
            | GraphBinaryOp::Lt
            | GraphBinaryOp::Le
            | GraphBinaryOp::Gt
            | GraphBinaryOp::Ge
            | GraphBinaryOp::In
            | GraphBinaryOp::StartsWith
            | GraphBinaryOp::EndsWith
            | GraphBinaryOp::Contains => GraphRowCursorAtomExpectation::Bool,
        },
        BoundGraphExpr::Case { .. } => GraphRowCursorAtomExpectation::AnyOrderable,
        BoundGraphExpr::IsNull(_) | BoundGraphExpr::IsNotNull(_) => GraphRowCursorAtomExpectation::Bool,
    })
}

fn graph_row_cursor_atom_matches_expectation(
    atom: &crate::graph_row::GraphSortAtom,
    expectation: GraphRowCursorAtomExpectation,
) -> bool {
    match expectation {
        GraphRowCursorAtomExpectation::AnyOrderable => true,
        GraphRowCursorAtomExpectation::Unsupported => matches!(atom, crate::graph_row::GraphSortAtom::Null),
        GraphRowCursorAtomExpectation::Scalar => matches!(
            atom,
            crate::graph_row::GraphSortAtom::Null
                | crate::graph_row::GraphSortAtom::Bool(_)
                | crate::graph_row::GraphSortAtom::Number(_)
                | crate::graph_row::GraphSortAtom::String(_)
                | crate::graph_row::GraphSortAtom::Bytes(_)
        ),
        GraphRowCursorAtomExpectation::Bool => {
            matches!(
                atom,
                crate::graph_row::GraphSortAtom::Null | crate::graph_row::GraphSortAtom::Bool(_)
            )
        }
        GraphRowCursorAtomExpectation::Number => {
            matches!(
                atom,
                crate::graph_row::GraphSortAtom::Null | crate::graph_row::GraphSortAtom::Number(_)
            )
        }
        GraphRowCursorAtomExpectation::String => {
            matches!(
                atom,
                crate::graph_row::GraphSortAtom::Null | crate::graph_row::GraphSortAtom::String(_)
            )
        }
        GraphRowCursorAtomExpectation::Bytes => {
            matches!(
                atom,
                crate::graph_row::GraphSortAtom::Null | crate::graph_row::GraphSortAtom::Bytes(_)
            )
        }
        GraphRowCursorAtomExpectation::Node => {
            matches!(
                atom,
                crate::graph_row::GraphSortAtom::Null | crate::graph_row::GraphSortAtom::Node(_)
            )
        }
        GraphRowCursorAtomExpectation::Slot(slot_kind, nullable) => {
            graph_row_cursor_atom_matches_slot_kind(atom, slot_kind, nullable)
        }
    }
}

fn graph_row_cursor_atom_matches_slot(
    atom: &crate::graph_row::GraphSortAtom,
    slot: &crate::graph_row::GraphBindingSlot,
) -> bool {
    graph_row_cursor_atom_matches_slot_kind(atom, slot.kind, slot.nullable)
}

fn graph_row_cursor_atom_matches_slot_kind(
    atom: &crate::graph_row::GraphSortAtom,
    slot_kind: crate::graph_row::GraphBindingSlotKind,
    nullable: bool,
) -> bool {
    match atom {
        crate::graph_row::GraphSortAtom::Null => nullable,
        crate::graph_row::GraphSortAtom::Node(_) => {
            slot_kind == crate::graph_row::GraphBindingSlotKind::Node
        }
        crate::graph_row::GraphSortAtom::Edge(_) => {
            matches!(
                slot_kind,
                crate::graph_row::GraphBindingSlotKind::Edge
                    | crate::graph_row::GraphBindingSlotKind::HiddenOccurrence
            )
        }
        crate::graph_row::GraphSortAtom::Path { .. } => {
            matches!(
                slot_kind,
                crate::graph_row::GraphBindingSlotKind::Path
                    | crate::graph_row::GraphBindingSlotKind::HiddenOccurrence
            )
        }
        crate::graph_row::GraphSortAtom::Bool(_)
        | crate::graph_row::GraphSortAtom::Number(_)
        | crate::graph_row::GraphSortAtom::String(_)
        | crate::graph_row::GraphSortAtom::Bytes(_)
        | crate::graph_row::GraphSortAtom::List(_)
        | crate::graph_row::GraphSortAtom::Map(_) => {
            slot_kind == crate::graph_row::GraphBindingSlotKind::Scalar
        }
    }
}

fn graph_row_validate_cursor_path_atom(
    hop_count: usize,
    nodes: &[u64],
    edges: &[u64],
) -> Result<(), EngineError> {
    if hop_count != edges.len() {
        return Err(invalid_graph_row_cursor(
            "cursor path sort atom hop count does not match edge count",
        ));
    }
    if nodes.len() != edges.len().saturating_add(1) {
        return Err(invalid_graph_row_cursor(
            "cursor path sort atom node count must equal edge count plus one",
        ));
    }
    Ok(())
}

fn graph_row_cursor_fingerprints(
    query: &NormalizedGraphRowQuery,
    effective_at_epoch: i64,
    original_skip: u64,
) -> GraphRowCursorFingerprints {
    let mut query_writer = GraphRowFingerprintWriter::new("query");
    query_writer.u16(GRAPH_ROW_CURSOR_SEMANTIC_VERSION);
    query_writer.i64(effective_at_epoch);
    query_writer.u64(original_skip);
    match query.logical_limit {
        Some(limit) => {
            query_writer.tag(1);
            query_writer.u64(limit as u64);
        }
        None => query_writer.tag(0),
    }
    graph_row_fingerprint_node_patterns(&mut query_writer, &query.nodes);
    graph_row_fingerprint_pattern_pieces(&mut query_writer, &query.pieces);
    graph_row_fingerprint_fixed_paths(&mut query_writer, &query.fixed_paths);
    graph_row_fingerprint_edge_id_constraints(&mut query_writer, &query.edge_id_constraints);
    graph_row_fingerprint_option_expr(&mut query_writer, query.fingerprint_where.as_ref());

    let mut order_writer = GraphRowFingerprintWriter::new("order");
    graph_row_fingerprint_order_items(&mut order_writer, &query.fingerprint_order_by);

    let mut output_writer = GraphRowFingerprintWriter::new("output");
    graph_row_fingerprint_option_return_items(
        &mut output_writer,
        query.fingerprint_return_items.as_ref(),
    );
    graph_row_fingerprint_string_vec(&mut output_writer, &query.columns);
    graph_row_fingerprint_output_options(&mut output_writer, &query.output);

    let mut params_writer = GraphRowFingerprintWriter::new("params");
    params_writer.len(query.referenced_params.len());
    for (name, value) in &query.referenced_params {
        params_writer.str(name);
        graph_row_fingerprint_param_value(&mut params_writer, value);
    }

    GraphRowCursorFingerprints {
        query: query_writer.finish(),
        order: order_writer.finish(),
        output: output_writer.finish(),
        params: params_writer.finish(),
    }
}

struct GraphRowFingerprintWriter {
    hash: u128,
}

impl GraphRowFingerprintWriter {
    fn new(namespace: &str) -> Self {
        let mut writer = Self {
            hash: FNV128_OFFSET,
        };
        writer.str(namespace);
        writer
    }

    fn finish(self) -> u128 {
        self.hash
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.u64(bytes.len() as u64);
        self.raw_bytes(bytes);
    }

    fn raw_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= *byte as u128;
            self.hash = self.hash.wrapping_mul(FNV128_PRIME);
        }
    }

    fn tag(&mut self, tag: u8) {
        self.raw_bytes(&[tag]);
    }

    fn bool(&mut self, value: bool) {
        self.tag(u8::from(value));
    }

    fn u8(&mut self, value: u8) {
        self.raw_bytes(&[value]);
    }

    fn u16(&mut self, value: u16) {
        self.raw_bytes(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw_bytes(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.raw_bytes(&value.to_be_bytes());
    }

    fn usize(&mut self, value: usize) {
        self.u64(value as u64);
    }

    fn len(&mut self, value: usize) {
        self.usize(value);
    }

    fn f32(&mut self, value: f32) {
        self.raw_bytes(&value.to_bits().to_be_bytes());
    }

    fn f64(&mut self, value: f64) {
        self.raw_bytes(&value.to_bits().to_be_bytes());
    }

    fn str(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }
}

fn graph_row_fingerprint_string_vec(writer: &mut GraphRowFingerprintWriter, values: &[String]) {
    writer.len(values.len());
    for value in values {
        writer.str(value);
    }
}

fn graph_row_fingerprint_node_patterns(
    writer: &mut GraphRowFingerprintWriter,
    nodes: &[GraphNodePattern],
) {
    writer.len(nodes.len());
    for node in nodes {
        writer.str(&node.alias);
        graph_row_fingerprint_node_label_filter(writer, node.label_filter.as_ref());
        writer.len(node.ids.len());
        for id in &node.ids {
            writer.u64(*id);
        }
        writer.len(node.keys.len());
        for key in &node.keys {
            writer.str(&key.label);
            writer.str(&key.key);
        }
        graph_row_fingerprint_node_filter(writer, node.filter.as_ref());
    }
}

fn graph_row_fingerprint_pattern_pieces(
    writer: &mut GraphRowFingerprintWriter,
    pieces: &[GraphPatternPiece],
) {
    writer.len(pieces.len());
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                writer.tag(0);
                graph_row_fingerprint_edge_pattern(writer, edge);
            }
            GraphPatternPiece::Optional(group) => {
                writer.tag(1);
                graph_row_fingerprint_pattern_pieces(writer, &group.pieces);
                graph_row_fingerprint_option_expr(writer, group.where_.as_ref());
            }
            GraphPatternPiece::VariableLength(pattern) => {
                writer.tag(2);
                graph_row_fingerprint_opt_str(writer, pattern.path_alias.as_deref());
                graph_row_fingerprint_opt_str(writer, pattern.edge_alias.as_deref());
                writer.str(&pattern.from_alias);
                writer.str(&pattern.to_alias);
                graph_row_fingerprint_direction(writer, pattern.direction);
                graph_row_fingerprint_string_vec(writer, &pattern.label_filter);
                graph_row_fingerprint_edge_filter(writer, pattern.filter.as_ref());
                writer.u8(pattern.min_hops);
                writer.u8(pattern.max_hops);
            }
        }
    }
}

fn graph_row_fingerprint_fixed_paths(
    writer: &mut GraphRowFingerprintWriter,
    fixed_paths: &[GraphFixedPathBinding],
) {
    writer.len(fixed_paths.len());
    for fixed_path in fixed_paths {
        writer.len(fixed_path.scope.len());
        for piece_index in &fixed_path.scope {
            writer.u64(*piece_index as u64);
        }
        writer.str(&fixed_path.alias);
        graph_row_fingerprint_string_vec(writer, &fixed_path.node_aliases);
        writer.len(fixed_path.edge_piece_indices.len());
        for piece_index in &fixed_path.edge_piece_indices {
            writer.u64(*piece_index as u64);
        }
        writer.u64(fixed_path.after_piece_index as u64);
    }
}

fn graph_row_fingerprint_edge_pattern(
    writer: &mut GraphRowFingerprintWriter,
    edge: &GraphEdgePattern,
) {
    graph_row_fingerprint_opt_str(writer, edge.alias.as_deref());
    writer.str(&edge.from_alias);
    writer.str(&edge.to_alias);
    graph_row_fingerprint_direction(writer, edge.direction);
    graph_row_fingerprint_string_vec(writer, &edge.label_filter);
    graph_row_fingerprint_edge_filter(writer, edge.filter.as_ref());
}

fn graph_row_fingerprint_direction(writer: &mut GraphRowFingerprintWriter, direction: Direction) {
    writer.tag(match direction {
        Direction::Outgoing => 0,
        Direction::Incoming => 1,
        Direction::Both => 2,
    });
}

fn graph_row_fingerprint_node_label_filter(
    writer: &mut GraphRowFingerprintWriter,
    filter: Option<&NodeLabelFilter>,
) {
    match filter {
        Some(filter) => {
            writer.tag(1);
            writer.tag(match filter.mode {
                LabelMatchMode::Any => 0,
                LabelMatchMode::All => 1,
            });
            graph_row_fingerprint_string_vec(writer, &filter.labels);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_edge_id_constraints(
    writer: &mut GraphRowFingerprintWriter,
    constraints: &BTreeMap<String, Vec<u64>>,
) {
    writer.len(constraints.len());
    for (alias, ids) in constraints {
        writer.str(alias);
        writer.len(ids.len());
        for id in ids {
            writer.u64(*id);
        }
    }
}

fn graph_row_fingerprint_option_expr(
    writer: &mut GraphRowFingerprintWriter,
    expr: Option<&GraphExpr>,
) {
    match expr {
        Some(expr) => {
            writer.tag(1);
            graph_row_fingerprint_expr(writer, expr);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_expr(writer: &mut GraphRowFingerprintWriter, expr: &GraphExpr) {
    match expr {
        GraphExpr::Null => writer.tag(0),
        GraphExpr::Bool(value) => {
            writer.tag(1);
            writer.bool(*value);
        }
        GraphExpr::Int(value) => {
            writer.tag(2);
            writer.i64(*value);
        }
        GraphExpr::UInt(value) => {
            writer.tag(3);
            writer.u64(*value);
        }
        GraphExpr::Float(value) => {
            writer.tag(4);
            writer.f64(*value);
        }
        GraphExpr::String(value) => {
            writer.tag(5);
            writer.str(value);
        }
        GraphExpr::Bytes(value) => {
            writer.tag(6);
            writer.bytes(value);
        }
        GraphExpr::List(items) => {
            writer.tag(7);
            writer.len(items.len());
            for item in items {
                graph_row_fingerprint_expr(writer, item);
            }
        }
        GraphExpr::Map(items) => {
            writer.tag(8);
            writer.len(items.len());
            for (key, value) in items {
                writer.str(key);
                graph_row_fingerprint_expr(writer, value);
            }
        }
        GraphExpr::Param(name) => {
            writer.tag(9);
            writer.str(name);
        }
        GraphExpr::Binding(alias) => {
            writer.tag(10);
            writer.str(alias);
        }
        GraphExpr::Property { alias, key } => {
            writer.tag(11);
            writer.str(alias);
            writer.str(key);
        }
        GraphExpr::NodeField { alias, field } => {
            writer.tag(12);
            writer.str(alias);
            writer.tag(*field as u8);
        }
        GraphExpr::EdgeField { alias, field } => {
            writer.tag(13);
            writer.str(alias);
            writer.tag(*field as u8);
        }
        GraphExpr::PathField { alias, field } => {
            writer.tag(14);
            writer.str(alias);
            writer.tag(*field as u8);
        }
        GraphExpr::Function { name, args } => {
            writer.tag(15);
            writer.tag(*name as u8);
            writer.len(args.len());
            for arg in args {
                graph_row_fingerprint_expr(writer, arg);
            }
        }
        GraphExpr::AggregateCall {
            function,
            distinct,
            arg,
        } => {
            writer.tag(21);
            writer.tag(*function as u8);
            writer.bool(*distinct);
            graph_row_fingerprint_option_expr(writer, arg.as_deref());
        }
        GraphExpr::ExistsSubquery(stage) => {
            writer.tag(22);
            graph_row_fingerprint_string_vec(writer, &stage.import_aliases);
            graph_pipeline_fingerprint_stages(writer, &stage.query.stages);
            graph_row_fingerprint_string_vec(
                writer,
                &graph_pipeline_declared_branch_columns(&stage.query),
            );
        }
        GraphExpr::Unary { op, expr } => {
            writer.tag(16);
            writer.tag(*op as u8);
            graph_row_fingerprint_expr(writer, expr);
        }
        GraphExpr::Binary { left, op, right } => {
            writer.tag(17);
            writer.tag(*op as u8);
            graph_row_fingerprint_expr(writer, left);
            graph_row_fingerprint_expr(writer, right);
        }
        GraphExpr::IsNull(expr) => {
            writer.tag(18);
            graph_row_fingerprint_expr(writer, expr);
        }
        GraphExpr::IsNotNull(expr) => {
            writer.tag(19);
            graph_row_fingerprint_expr(writer, expr);
        }
        GraphExpr::Case {
            operand,
            branches,
            else_expr,
        } => {
            writer.tag(20);
            graph_row_fingerprint_option_expr(writer, operand.as_deref());
            writer.len(branches.len());
            for branch in branches {
                graph_row_fingerprint_expr(writer, &branch.when);
                graph_row_fingerprint_expr(writer, &branch.then);
            }
            graph_row_fingerprint_option_expr(writer, else_expr.as_deref());
        }
    }
}

fn graph_row_fingerprint_order_items(
    writer: &mut GraphRowFingerprintWriter,
    items: &[GraphOrderItem],
) {
    writer.len(items.len());
    for item in items {
        graph_row_fingerprint_expr(writer, &item.expr);
        writer.tag(match item.direction {
            GraphOrderDirection::Asc => 0,
            GraphOrderDirection::Desc => 1,
        });
    }
}

fn graph_row_fingerprint_option_return_items(
    writer: &mut GraphRowFingerprintWriter,
    items: Option<&Vec<GraphReturnItem>>,
) {
    match items {
        Some(items) => {
            writer.tag(1);
            graph_row_fingerprint_return_items(writer, items);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_return_items(
    writer: &mut GraphRowFingerprintWriter,
    items: &[GraphReturnItem],
) {
    writer.len(items.len());
    for item in items {
        graph_row_fingerprint_expr(writer, &item.expr);
        graph_row_fingerprint_opt_str(writer, item.alias.as_deref());
        graph_row_fingerprint_return_projection(writer, &item.projection);
    }
}

fn graph_row_fingerprint_return_projection(
    writer: &mut GraphRowFingerprintWriter,
    projection: &GraphReturnProjection,
) {
    match projection {
        GraphReturnProjection::Auto => writer.tag(0),
        GraphReturnProjection::IdOnly => writer.tag(1),
        GraphReturnProjection::Element(element) => {
            writer.tag(2);
            writer.tag(match element {
                GraphElementProjection::IdOnly => 0,
                GraphElementProjection::Compact => 1,
                GraphElementProjection::Full => 2,
            });
        }
        GraphReturnProjection::Selected(selected) => {
            writer.tag(3);
            graph_row_fingerprint_selected_projection(writer, selected);
        }
    }
}

fn graph_row_fingerprint_selected_projection(
    writer: &mut GraphRowFingerprintWriter,
    selected: &GraphSelectedProjection,
) {
    match selected {
        GraphSelectedProjection::Node(node) => {
            writer.tag(0);
            writer.bool(node.id);
            writer.bool(node.labels);
            writer.bool(node.key);
            graph_row_fingerprint_graph_property_selection(writer, &node.props);
            writer.bool(node.weight);
            writer.bool(node.created_at);
            writer.bool(node.updated_at);
            graph_row_fingerprint_graph_vector_selection(writer, node.vectors);
        }
        GraphSelectedProjection::Edge(edge) => {
            writer.tag(1);
            writer.bool(edge.id);
            writer.bool(edge.from);
            writer.bool(edge.to);
            writer.bool(edge.label);
            graph_row_fingerprint_graph_property_selection(writer, &edge.props);
            writer.bool(edge.weight);
            writer.bool(edge.created_at);
            writer.bool(edge.updated_at);
            writer.bool(edge.valid_from);
            writer.bool(edge.valid_to);
        }
        GraphSelectedProjection::Path(path) => {
            writer.tag(2);
            writer.bool(path.node_ids);
            writer.bool(path.edge_ids);
            graph_row_fingerprint_opt_selected_node(writer, path.nodes.as_ref());
            graph_row_fingerprint_opt_selected_edge(writer, path.edges.as_ref());
        }
    }
}

fn graph_row_fingerprint_opt_selected_node(
    writer: &mut GraphRowFingerprintWriter,
    node: Option<&GraphSelectedNodeProjection>,
) {
    match node {
        Some(node) => {
            writer.tag(1);
            graph_row_fingerprint_selected_projection(
                writer,
                &GraphSelectedProjection::Node(node.clone()),
            );
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_opt_selected_edge(
    writer: &mut GraphRowFingerprintWriter,
    edge: Option<&GraphSelectedEdgeProjection>,
) {
    match edge {
        Some(edge) => {
            writer.tag(1);
            graph_row_fingerprint_selected_projection(
                writer,
                &GraphSelectedProjection::Edge(edge.clone()),
            );
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_graph_property_selection(
    writer: &mut GraphRowFingerprintWriter,
    selection: &GraphPropertySelection,
) {
    match selection {
        GraphPropertySelection::None => writer.tag(0),
        GraphPropertySelection::Keys(keys) => {
            writer.tag(1);
            graph_row_fingerprint_string_vec(writer, keys);
        }
        GraphPropertySelection::All => writer.tag(2),
    }
}

fn graph_row_fingerprint_graph_vector_selection(
    writer: &mut GraphRowFingerprintWriter,
    selection: GraphVectorSelection,
) {
    writer.tag(match selection {
        GraphVectorSelection::None => 0,
        GraphVectorSelection::Dense => 1,
        GraphVectorSelection::Sparse => 2,
        GraphVectorSelection::Both => 3,
    });
}

fn graph_row_fingerprint_output_options(
    writer: &mut GraphRowFingerprintWriter,
    output: &GraphOutputOptions,
) {
    writer.tag(match output.mode {
        GraphOutputMode::Ids => 0,
        GraphOutputMode::Elements => 1,
        GraphOutputMode::Projected => 2,
    });
    writer.bool(output.include_vectors);
}

fn graph_row_fingerprint_node_filter(
    writer: &mut GraphRowFingerprintWriter,
    filter: Option<&NodeFilterExpr>,
) {
    match filter {
        Some(filter) => {
            writer.tag(1);
            graph_row_fingerprint_node_filter_inner(writer, filter);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_node_filter_inner(
    writer: &mut GraphRowFingerprintWriter,
    filter: &NodeFilterExpr,
) {
    match filter {
        NodeFilterExpr::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            writer.tag(13);
            graph_row_fingerprint_opt_u64(writer, *lower);
            graph_row_fingerprint_opt_u64(writer, *upper);
            writer.bool(*lower_inclusive);
            writer.bool(*upper_inclusive);
        }
        NodeFilterExpr::KeyEquals(key) => {
            writer.tag(14);
            writer.str(key);
        }
        NodeFilterExpr::KeyIn(keys) => {
            writer.tag(15);
            writer.len(keys.len());
            for key in keys {
                writer.str(key);
            }
        }
        NodeFilterExpr::PropertyEquals { key, value } => {
            writer.tag(0);
            writer.str(key);
            graph_row_fingerprint_prop_value(writer, value);
        }
        NodeFilterExpr::PropertyIn { key, values } => {
            writer.tag(1);
            writer.str(key);
            writer.len(values.len());
            for value in values {
                graph_row_fingerprint_prop_value(writer, value);
            }
        }
        NodeFilterExpr::PropertyRange { key, lower, upper } => {
            writer.tag(2);
            writer.str(key);
            graph_row_fingerprint_range_bound(writer, lower.as_ref());
            graph_row_fingerprint_range_bound(writer, upper.as_ref());
        }
        NodeFilterExpr::PropertyExists { key } => {
            writer.tag(3);
            writer.str(key);
        }
        NodeFilterExpr::PropertyMissing { key } => {
            writer.tag(4);
            writer.str(key);
        }
        NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            writer.tag(5);
            graph_row_fingerprint_opt_i64(writer, *lower_ms);
            graph_row_fingerprint_opt_i64(writer, *upper_ms);
        }
        NodeFilterExpr::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            writer.tag(16);
            graph_row_fingerprint_opt_f32(writer, *lower);
            graph_row_fingerprint_opt_f32(writer, *upper);
            writer.bool(*lower_inclusive);
            writer.bool(*upper_inclusive);
        }
        NodeFilterExpr::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            writer.tag(17);
            graph_row_fingerprint_opt_i64(writer, *lower);
            graph_row_fingerprint_opt_i64(writer, *upper);
            writer.bool(*lower_inclusive);
            writer.bool(*upper_inclusive);
        }
        NodeFilterExpr::And(filters) => {
            writer.tag(6);
            writer.len(filters.len());
            for filter in filters {
                graph_row_fingerprint_node_filter_inner(writer, filter);
            }
        }
        NodeFilterExpr::Or(filters) => {
            writer.tag(7);
            writer.len(filters.len());
            for filter in filters {
                graph_row_fingerprint_node_filter_inner(writer, filter);
            }
        }
        NodeFilterExpr::Not(filter) => {
            writer.tag(8);
            graph_row_fingerprint_node_filter_inner(writer, filter);
        }
    }
}

fn graph_row_fingerprint_edge_filter(
    writer: &mut GraphRowFingerprintWriter,
    filter: Option<&EdgeFilterExpr>,
) {
    match filter {
        Some(filter) => {
            writer.tag(1);
            graph_row_fingerprint_edge_filter_inner(writer, filter);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_edge_filter_inner(
    writer: &mut GraphRowFingerprintWriter,
    filter: &EdgeFilterExpr,
) {
    match filter {
        EdgeFilterExpr::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            writer.tag(13);
            graph_row_fingerprint_opt_u64(writer, *lower);
            graph_row_fingerprint_opt_u64(writer, *upper);
            writer.bool(*lower_inclusive);
            writer.bool(*upper_inclusive);
        }
        EdgeFilterExpr::PropertyEquals { key, value } => {
            writer.tag(0);
            writer.str(key);
            graph_row_fingerprint_prop_value(writer, value);
        }
        EdgeFilterExpr::PropertyIn { key, values } => {
            writer.tag(1);
            writer.str(key);
            writer.len(values.len());
            for value in values {
                graph_row_fingerprint_prop_value(writer, value);
            }
        }
        EdgeFilterExpr::PropertyRange { key, lower, upper } => {
            writer.tag(2);
            writer.str(key);
            graph_row_fingerprint_range_bound(writer, lower.as_ref());
            graph_row_fingerprint_range_bound(writer, upper.as_ref());
        }
        EdgeFilterExpr::PropertyExists { key } => {
            writer.tag(3);
            writer.str(key);
        }
        EdgeFilterExpr::PropertyMissing { key } => {
            writer.tag(4);
            writer.str(key);
        }
        EdgeFilterExpr::WeightRange { lower, upper } => {
            writer.tag(5);
            graph_row_fingerprint_opt_f32(writer, *lower);
            graph_row_fingerprint_opt_f32(writer, *upper);
        }
        EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            writer.tag(6);
            graph_row_fingerprint_opt_i64(writer, *lower_ms);
            graph_row_fingerprint_opt_i64(writer, *upper_ms);
        }
        EdgeFilterExpr::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            writer.tag(14);
            graph_row_fingerprint_opt_i64(writer, *lower);
            graph_row_fingerprint_opt_i64(writer, *upper);
            writer.bool(*lower_inclusive);
            writer.bool(*upper_inclusive);
        }
        EdgeFilterExpr::ValidAt { epoch_ms } => {
            writer.tag(7);
            writer.i64(*epoch_ms);
        }
        EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms } => {
            writer.tag(8);
            graph_row_fingerprint_opt_i64(writer, *lower_ms);
            graph_row_fingerprint_opt_i64(writer, *upper_ms);
        }
        EdgeFilterExpr::ValidToRange { lower_ms, upper_ms } => {
            writer.tag(9);
            graph_row_fingerprint_opt_i64(writer, *lower_ms);
            graph_row_fingerprint_opt_i64(writer, *upper_ms);
        }
        EdgeFilterExpr::And(filters) => {
            writer.tag(10);
            writer.len(filters.len());
            for filter in filters {
                graph_row_fingerprint_edge_filter_inner(writer, filter);
            }
        }
        EdgeFilterExpr::Or(filters) => {
            writer.tag(11);
            writer.len(filters.len());
            for filter in filters {
                graph_row_fingerprint_edge_filter_inner(writer, filter);
            }
        }
        EdgeFilterExpr::Not(filter) => {
            writer.tag(12);
            graph_row_fingerprint_edge_filter_inner(writer, filter);
        }
    }
}

fn graph_row_fingerprint_range_bound(
    writer: &mut GraphRowFingerprintWriter,
    bound: Option<&PropertyRangeBound>,
) {
    match bound {
        Some(PropertyRangeBound::Included(value)) => {
            writer.tag(1);
            graph_row_fingerprint_prop_value(writer, value);
        }
        Some(PropertyRangeBound::Excluded(value)) => {
            writer.tag(2);
            graph_row_fingerprint_prop_value(writer, value);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_prop_value(writer: &mut GraphRowFingerprintWriter, value: &PropValue) {
    match value {
        PropValue::Null => writer.tag(0),
        PropValue::Bool(value) => {
            writer.tag(1);
            writer.bool(*value);
        }
        PropValue::Int(value) => {
            writer.tag(2);
            writer.i64(*value);
        }
        PropValue::UInt(value) => {
            writer.tag(3);
            writer.u64(*value);
        }
        PropValue::Float(value) => {
            writer.tag(4);
            writer.f64(*value);
        }
        PropValue::String(value) => {
            writer.tag(5);
            writer.str(value);
        }
        PropValue::Bytes(value) => {
            writer.tag(6);
            writer.bytes(value);
        }
        PropValue::Array(values) => {
            writer.tag(7);
            writer.len(values.len());
            for value in values {
                graph_row_fingerprint_prop_value(writer, value);
            }
        }
        PropValue::Map(values) => {
            writer.tag(8);
            writer.len(values.len());
            for (key, value) in values {
                writer.str(key);
                graph_row_fingerprint_prop_value(writer, value);
            }
        }
    }
}

fn graph_row_fingerprint_param_value(
    writer: &mut GraphRowFingerprintWriter,
    value: &GraphParamValue,
) {
    match value {
        GraphParamValue::Null => writer.tag(0),
        GraphParamValue::Bool(value) => {
            writer.tag(1);
            writer.bool(*value);
        }
        GraphParamValue::Int(value) => {
            writer.tag(2);
            writer.i64(*value);
        }
        GraphParamValue::UInt(value) => {
            writer.tag(3);
            writer.u64(*value);
        }
        GraphParamValue::Float(value) => {
            writer.tag(4);
            writer.f64(*value);
        }
        GraphParamValue::String(value) => {
            writer.tag(5);
            writer.str(value);
        }
        GraphParamValue::Bytes(value) => {
            writer.tag(6);
            writer.bytes(value);
        }
        GraphParamValue::List(values) => {
            writer.tag(7);
            writer.len(values.len());
            for value in values {
                graph_row_fingerprint_param_value(writer, value);
            }
        }
        GraphParamValue::Map(values) => {
            writer.tag(8);
            writer.len(values.len());
            for (key, value) in values {
                writer.str(key);
                graph_row_fingerprint_param_value(writer, value);
            }
        }
    }
}

fn graph_row_fingerprint_opt_str(writer: &mut GraphRowFingerprintWriter, value: Option<&str>) {
    match value {
        Some(value) => {
            writer.tag(1);
            writer.str(value);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_opt_i64(writer: &mut GraphRowFingerprintWriter, value: Option<i64>) {
    match value {
        Some(value) => {
            writer.tag(1);
            writer.i64(value);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_opt_u64(writer: &mut GraphRowFingerprintWriter, value: Option<u64>) {
    match value {
        Some(value) => {
            writer.tag(1);
            writer.u64(value);
        }
        None => writer.tag(0),
    }
}

fn graph_row_fingerprint_opt_f32(writer: &mut GraphRowFingerprintWriter, value: Option<f32>) {
    match value {
        Some(value) => {
            writer.tag(1);
            writer.f32(value);
        }
        None => writer.tag(0),
    }
}

fn graph_row_encode_cursor(
    cursor: &GraphRowCursorPayload,
    max_cursor_bytes: usize,
) -> Result<String, EngineError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(GRAPH_ROW_CURSOR_MAGIC);
    push_u8(&mut bytes, GRAPH_ROW_CURSOR_VERSION);
    push_u16(&mut bytes, GRAPH_ROW_CURSOR_SEMANTIC_VERSION);
    push_u16(&mut bytes, GRAPH_ROW_CURSOR_FLAGS);
    push_i64(&mut bytes, cursor.effective_at_epoch);
    push_u64(&mut bytes, cursor.original_skip);
    push_u64(&mut bytes, cursor.page_sequence);
    push_u64(&mut bytes, cursor.rows_emitted_after_skip);
    push_u128(&mut bytes, cursor.query_fingerprint);
    push_u128(&mut bytes, cursor.order_fingerprint);
    push_u128(&mut bytes, cursor.output_fingerprint);
    push_u128(&mut bytes, cursor.params_fingerprint);
    encode_graph_sort_atoms(&mut bytes, &cursor.last_sort_key)?;
    encode_graph_sort_atoms(&mut bytes, &cursor.last_logical_row_key)?;
    let checksum = crate::types::fnv1a(&bytes);
    push_u64(&mut bytes, checksum);
    if bytes.len() > max_cursor_bytes {
        return Err(invalid_graph_row_cursor(format!(
            "emitted graph row cursor payload is {} bytes, exceeding max_cursor_bytes {}",
            bytes.len(),
            max_cursor_bytes
        )));
    }
    let encoded = format!(
        "{GRAPH_ROW_CURSOR_PREFIX}{}",
        base64url_no_pad_encode(&bytes)
    );
    Ok(encoded)
}

fn graph_row_decode_cursor(
    cursor: &str,
    max_cursor_bytes: usize,
) -> Result<GraphRowCursorPayload, EngineError> {
    let Some(encoded) = cursor.strip_prefix(GRAPH_ROW_CURSOR_PREFIX) else {
        return Err(invalid_graph_row_cursor("invalid graph row cursor prefix"));
    };
    let bytes = base64url_no_pad_decode(encoded)?;
    if bytes.len() > max_cursor_bytes {
        return Err(invalid_graph_row_cursor(format!(
            "decoded graph row cursor is {} bytes, exceeding max_cursor_bytes {}",
            bytes.len(),
            max_cursor_bytes
        )));
    }
    if bytes.len() < GRAPH_ROW_CURSOR_MAGIC.len() + 1 + 2 + 2 + 8 {
        return Err(invalid_graph_row_cursor("cursor payload is too short"));
    }
    if bytes.len() < 8 {
        return Err(invalid_graph_row_cursor("cursor payload is missing checksum"));
    }
    let checksum_offset = bytes.len() - 8;
    let expected_checksum = crate::types::fnv1a(&bytes[..checksum_offset]);
    let stored_checksum = u64::from_be_bytes(
        bytes[checksum_offset..]
            .try_into()
            .map_err(|_| invalid_graph_row_cursor("cursor checksum is malformed"))?,
    );
    if stored_checksum != expected_checksum {
        return Err(invalid_graph_row_cursor("cursor checksum mismatch"));
    }

    let mut reader = CursorPayloadReader::new(&bytes[..checksum_offset]);
    let magic = reader.take(GRAPH_ROW_CURSOR_MAGIC.len())?;
    if magic != GRAPH_ROW_CURSOR_MAGIC {
        return Err(invalid_graph_row_cursor("cursor magic mismatch"));
    }
    let version = reader.read_u8()?;
    if version != GRAPH_ROW_CURSOR_VERSION {
        return Err(invalid_graph_row_cursor(format!(
            "unsupported cursor version {version}"
        )));
    }
    let semantic_version = reader.read_u16()?;
    if semantic_version != GRAPH_ROW_CURSOR_SEMANTIC_VERSION {
        return Err(invalid_graph_row_cursor(format!(
            "unsupported cursor semantic version {semantic_version}"
        )));
    }
    let flags = reader.read_u16()?;
    if flags != GRAPH_ROW_CURSOR_FLAGS {
        return Err(invalid_graph_row_cursor(format!(
            "unsupported cursor flags {flags}"
        )));
    }
    let payload = GraphRowCursorPayload {
        effective_at_epoch: reader.read_i64()?,
        original_skip: reader.read_u64()?,
        page_sequence: reader.read_u64()?,
        rows_emitted_after_skip: reader.read_u64()?,
        query_fingerprint: reader.read_u128()?,
        order_fingerprint: reader.read_u128()?,
        output_fingerprint: reader.read_u128()?,
        params_fingerprint: reader.read_u128()?,
        last_sort_key: decode_graph_sort_atoms(&mut reader)?,
        last_logical_row_key: decode_graph_sort_atoms(&mut reader)?,
    };
    if !reader.is_finished() {
        return Err(invalid_graph_row_cursor("cursor payload has trailing bytes"));
    }
    Ok(payload)
}

fn encode_graph_sort_atoms(
    bytes: &mut Vec<u8>,
    atoms: &[crate::graph_row::GraphSortAtom],
) -> Result<(), EngineError> {
    push_u32(bytes, atoms.len().try_into().map_err(|_| {
        EngineError::InvalidOperation("graph row cursor sort key is too large".to_string())
    })?);
    for atom in atoms {
        match atom {
            crate::graph_row::GraphSortAtom::Null => push_u8(bytes, 0),
            crate::graph_row::GraphSortAtom::Bool(value) => {
                push_u8(bytes, 1);
                push_u8(bytes, u8::from(*value));
            }
            crate::graph_row::GraphSortAtom::Number(value) => {
                push_u8(bytes, 2);
                bytes.extend_from_slice(&value.as_bytes());
            }
            crate::graph_row::GraphSortAtom::String(value) => {
                push_u8(bytes, 3);
                push_bytes(bytes, value)?;
            }
            crate::graph_row::GraphSortAtom::Bytes(value) => {
                push_u8(bytes, 4);
                push_bytes(bytes, value)?;
            }
            crate::graph_row::GraphSortAtom::Node(value) => {
                push_u8(bytes, 5);
                push_u64(bytes, *value);
            }
            crate::graph_row::GraphSortAtom::Edge(value) => {
                push_u8(bytes, 6);
                push_u64(bytes, *value);
            }
            crate::graph_row::GraphSortAtom::Path {
                hop_count,
                nodes,
                edges,
            } => {
                push_u8(bytes, 7);
                push_u64(bytes, (*hop_count).try_into().map_err(|_| {
                    EngineError::InvalidOperation(
                        "graph row cursor path hop count is too large".to_string(),
                    )
                })?);
                push_u64_vec(bytes, nodes)?;
                push_u64_vec(bytes, edges)?;
            }
            crate::graph_row::GraphSortAtom::List(values) => {
                push_u8(bytes, 8);
                encode_graph_sort_atoms(bytes, values)?;
            }
            crate::graph_row::GraphSortAtom::Map(values) => {
                push_u8(bytes, 9);
                push_u32(bytes, values.len().try_into().map_err(|_| {
                    EngineError::InvalidOperation(
                        "graph row cursor map sort atom is too large".to_string(),
                    )
                })?);
                for (key, value) in values {
                    push_bytes(bytes, key.as_bytes())?;
                    encode_graph_sort_atoms(bytes, std::slice::from_ref(value))?;
                }
            }
        }
    }
    Ok(())
}

fn decode_graph_sort_atoms(
    reader: &mut CursorPayloadReader<'_>,
) -> Result<Vec<crate::graph_row::GraphSortAtom>, EngineError> {
    let len = reader.read_u32()? as usize;
    if len > reader.remaining() {
        return Err(invalid_graph_row_cursor(
            "cursor sort atom count exceeds remaining payload",
        ));
    }
    let mut atoms = Vec::with_capacity(len);
    for _ in 0..len {
        let tag = reader.read_u8()?;
        let atom = match tag {
            0 => crate::graph_row::GraphSortAtom::Null,
            1 => match reader.read_u8()? {
                0 => crate::graph_row::GraphSortAtom::Bool(false),
                1 => crate::graph_row::GraphSortAtom::Bool(true),
                value => {
                    return Err(invalid_graph_row_cursor(format!(
                        "invalid bool sort atom value {value}"
                    )));
                }
            },
            2 => {
                let bytes: [u8; crate::property_value_semantics::NUMERIC_RANGE_KEY_BYTES] = reader
                    .take(crate::property_value_semantics::NUMERIC_RANGE_KEY_BYTES)?
                    .try_into()
                    .map_err(|_| invalid_graph_row_cursor("malformed numeric sort atom"))?;
                crate::graph_row::GraphSortAtom::Number(
                    NumericRangeSortKey::from_sidecar_bytes(bytes)
                        .map_err(|_| invalid_graph_row_cursor("invalid numeric sort atom"))?,
                )
            }
            3 => crate::graph_row::GraphSortAtom::String(reader.read_bytes()?.to_vec()),
            4 => crate::graph_row::GraphSortAtom::Bytes(reader.read_bytes()?.to_vec()),
            5 => crate::graph_row::GraphSortAtom::Node(reader.read_u64()?),
            6 => crate::graph_row::GraphSortAtom::Edge(reader.read_u64()?),
            7 => {
                let hop_count: usize = reader.read_u64()?.try_into().map_err(|_| {
                    invalid_graph_row_cursor("path hop count does not fit usize")
                })?;
                let nodes = reader.read_u64_vec()?;
                let edges = reader.read_u64_vec()?;
                graph_row_validate_cursor_path_atom(hop_count, &nodes, &edges)?;
                crate::graph_row::GraphSortAtom::Path {
                    hop_count,
                    nodes,
                    edges,
                }
            }
            8 => crate::graph_row::GraphSortAtom::List(decode_graph_sort_atoms(reader)?),
            9 => {
                let len = reader.read_u32()? as usize;
                if len > reader.remaining() {
                    return Err(invalid_graph_row_cursor(
                        "cursor map sort atom count exceeds remaining payload",
                    ));
                }
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let key = std::str::from_utf8(reader.read_bytes()?)
                        .map_err(|_| invalid_graph_row_cursor("cursor map key is not UTF-8"))?
                        .to_string();
                    let mut value = decode_graph_sort_atoms(reader)?;
                    if value.len() != 1 {
                        return Err(invalid_graph_row_cursor(
                            "cursor map value did not contain exactly one sort atom",
                        ));
                    }
                    values.push((key, value.remove(0)));
                }
                crate::graph_row::GraphSortAtom::Map(values)
            }
            value => {
                return Err(invalid_graph_row_cursor(format!(
                    "invalid sort atom tag {value}"
                )));
            }
        };
        atoms.push(atom);
    }
    Ok(atoms)
}

struct CursorPayloadReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CursorPayloadReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], EngineError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_graph_row_cursor("cursor payload offset overflow"))?;
        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(invalid_graph_row_cursor("truncated cursor payload"));
        };
        self.offset = end;
        Ok(slice)
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u8(&mut self) -> Result<u8, EngineError> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, EngineError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| invalid_graph_row_cursor("malformed u16"))?,
        ))
    }

    fn read_u32(&mut self) -> Result<u32, EngineError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| invalid_graph_row_cursor("malformed u32"))?,
        ))
    }

    fn read_u64(&mut self) -> Result<u64, EngineError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid_graph_row_cursor("malformed u64"))?,
        ))
    }

    fn read_i64(&mut self) -> Result<i64, EngineError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid_graph_row_cursor("malformed i64"))?,
        ))
    }

    fn read_u128(&mut self) -> Result<u128, EngineError> {
        Ok(u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| invalid_graph_row_cursor("malformed u128"))?,
        ))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], EngineError> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    fn read_u64_vec(&mut self) -> Result<Vec<u64>, EngineError> {
        let len = self.read_u32()? as usize;
        let required = len
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| invalid_graph_row_cursor("cursor u64 vector length overflow"))?;
        if required > self.remaining() {
            return Err(invalid_graph_row_cursor(
                "cursor u64 vector length exceeds remaining payload",
            ));
        }
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.read_u64()?);
        }
        Ok(values)
    }
}

fn push_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u128(bytes: &mut Vec<u8>, value: u128) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), EngineError> {
    push_u32(bytes, value.len().try_into().map_err(|_| {
        EngineError::InvalidOperation("graph row cursor byte field is too large".to_string())
    })?);
    bytes.extend_from_slice(value);
    Ok(())
}

fn push_u64_vec(bytes: &mut Vec<u8>, values: &[u64]) -> Result<(), EngineError> {
    push_u32(bytes, values.len().try_into().map_err(|_| {
        EngineError::InvalidOperation("graph row cursor id vector is too large".to_string())
    })?);
    for value in values {
        push_u64(bytes, *value);
    }
    Ok(())
}

fn base64url_no_pad_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity((bytes.len() * 4).div_ceil(3));
    let mut index = 0usize;
    while index + 3 <= bytes.len() {
        let chunk = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | bytes[index + 2] as u32;
        output.push(ALPHABET[((chunk >> 18) & 0x3f) as usize] as char);
        output.push(ALPHABET[((chunk >> 12) & 0x3f) as usize] as char);
        output.push(ALPHABET[((chunk >> 6) & 0x3f) as usize] as char);
        output.push(ALPHABET[(chunk & 0x3f) as usize] as char);
        index += 3;
    }
    match bytes.len() - index {
        1 => {
            let chunk = (bytes[index] as u32) << 16;
            output.push(ALPHABET[((chunk >> 18) & 0x3f) as usize] as char);
            output.push(ALPHABET[((chunk >> 12) & 0x3f) as usize] as char);
        }
        2 => {
            let chunk = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
            output.push(ALPHABET[((chunk >> 18) & 0x3f) as usize] as char);
            output.push(ALPHABET[((chunk >> 12) & 0x3f) as usize] as char);
            output.push(ALPHABET[((chunk >> 6) & 0x3f) as usize] as char);
        }
        _ => {}
    }
    output
}

fn base64url_no_pad_decode(encoded: &str) -> Result<Vec<u8>, EngineError> {
    if encoded.len() % 4 == 1 {
        return Err(invalid_graph_row_cursor("malformed base64url cursor"));
    }
    let mut output = Vec::with_capacity(encoded.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in encoded.bytes() {
        if byte == b'=' {
            return Err(invalid_graph_row_cursor(
                "padded base64 is not valid for graph row cursors",
            ));
        }
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(invalid_graph_row_cursor("invalid base64url cursor byte")),
        } as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        while bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    if bits > 0 && buffer != 0 {
        return Err(invalid_graph_row_cursor("non-zero trailing base64 cursor bits"));
    }
    Ok(output)
}

fn graph_row_encoded_cursor_transport_limit(max_decoded_bytes: usize) -> usize {
    let tail = match max_decoded_bytes % 3 {
        0 => 0,
        1 => 2,
        _ => 3,
    };
    let encoded = (max_decoded_bytes / 3)
        .checked_mul(4)
        .and_then(|value| value.checked_add(tail))
        .unwrap_or(usize::MAX);
    GRAPH_ROW_CURSOR_PREFIX.len().saturating_add(encoded)
}

fn invalid_graph_row_cursor(message: impl Into<String>) -> EngineError {
    EngineError::InvalidCursor {
        message: message.into(),
    }
}

fn graph_row_collect_node_ids(
    rows: &[crate::graph_row::GraphBindingRow],
    slot: crate::graph_row::GraphBindingSlotRef,
) -> Result<Vec<u64>, EngineError> {
    let mut ids = Vec::new();
    for row in rows {
        if let Some(id) = row.node_id_for_slot_if_bound(slot)? {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn graph_row_collect_edge_ids(
    rows: &[crate::graph_row::GraphBindingRow],
    slot: crate::graph_row::GraphBindingSlotRef,
) -> Result<Vec<u64>, EngineError> {
    let mut ids = Vec::new();
    for row in rows {
        if let Some(id) = row.edge_id_for_slot_if_bound(slot)? {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn graph_row_remaining_output_needs(
    output: &EntityProjectionNeeds,
    loaded: &EntityProjectionNeeds,
) -> EntityProjectionNeeds {
    let mut remaining = EntityProjectionNeeds::default();
    for (alias, needs) in &output.nodes {
        match loaded.nodes.get(alias) {
            Some(loaded_needs) => {
                if let Some(needs) = graph_row_remaining_node_needs(needs, loaded_needs) {
                    remaining.nodes.insert(alias.clone(), needs);
                }
            }
            None => {
                remaining.nodes.insert(alias.clone(), needs.clone());
            }
        }
    }
    for (alias, needs) in &output.edges {
        match loaded.edges.get(alias) {
            Some(loaded_needs) => {
                if let Some(needs) = graph_row_remaining_edge_needs(needs, loaded_needs) {
                    remaining.edges.insert(alias.clone(), needs);
                }
            }
            None => {
                remaining.edges.insert(alias.clone(), needs.clone());
            }
        }
    }
    for (alias, needs) in &output.paths {
        match loaded.paths.get(alias) {
            Some(loaded_needs) => {
                if let Some(needs) = graph_row_remaining_path_needs(needs, loaded_needs) {
                    remaining.paths.insert(alias.clone(), needs);
                }
            }
            None => {
                if graph_row_path_needs_require_selected_field_reads(needs) {
                    remaining.paths.insert(alias.clone(), needs.clone());
                }
            }
        }
    }
    for (slot, needs) in &output.hidden_edges {
        match loaded.hidden_edges.get(slot) {
            Some(loaded_needs) => {
                if let Some(needs) = graph_row_remaining_edge_needs(needs, loaded_needs) {
                    remaining.hidden_edges.insert(*slot, needs);
                }
            }
            None => {
                remaining.hidden_edges.insert(*slot, needs.clone());
            }
        }
    }
    for (slot, needs) in &output.hidden_paths {
        match loaded.hidden_paths.get(slot) {
            Some(loaded_needs) => {
                if let Some(needs) = graph_row_remaining_path_needs(needs, loaded_needs) {
                    remaining.hidden_paths.insert(*slot, needs);
                }
            }
            None => {
                if graph_row_path_needs_require_selected_field_reads(needs) {
                    remaining.hidden_paths.insert(*slot, needs.clone());
                }
            }
        }
    }
    remaining
}

fn graph_row_remaining_node_needs(
    needed: &NodeSelectedFieldNeeds,
    loaded: &NodeSelectedFieldNeeds,
) -> Option<NodeSelectedFieldNeeds> {
    let remaining = NodeSelectedFieldNeeds {
        key: needed.key && !loaded.key,
        created_at: needed.created_at && !loaded.created_at,
        props: graph_row_remaining_props(&needed.props, &loaded.props),
        vectors: graph_row_remaining_vectors(needed.vectors, loaded.vectors),
    };
    graph_row_node_needs_has_source_fields(&remaining).then_some(remaining)
}

fn graph_row_remaining_edge_needs(
    needed: &EdgeSelectedFieldNeeds,
    loaded: &EdgeSelectedFieldNeeds,
) -> Option<EdgeSelectedFieldNeeds> {
    let remaining = EdgeSelectedFieldNeeds {
        created_at: needed.created_at && !loaded.created_at,
        props: graph_row_remaining_props(&needed.props, &loaded.props),
    };
    graph_row_edge_needs_has_source_fields(&remaining).then_some(remaining)
}

fn graph_row_remaining_path_needs(
    needed: &PathSelectedFieldNeeds,
    loaded: &PathSelectedFieldNeeds,
) -> Option<PathSelectedFieldNeeds> {
    let remaining = PathSelectedFieldNeeds {
        node_ids: false,
        edge_ids: false,
        start_node: match (&needed.start_node, &loaded.start_node) {
            (Some(needed), Some(loaded)) => graph_row_remaining_node_needs(needed, loaded),
            (Some(needed), None) => {
                graph_row_node_needs_has_source_fields(needed).then_some(needed.clone())
            }
            (None, _) => None,
        },
        end_node: match (&needed.end_node, &loaded.end_node) {
            (Some(needed), Some(loaded)) => graph_row_remaining_node_needs(needed, loaded),
            (Some(needed), None) => {
                graph_row_node_needs_has_source_fields(needed).then_some(needed.clone())
            }
            (None, _) => None,
        },
        nodes: match (&needed.nodes, &loaded.nodes) {
            (Some(needed), Some(loaded)) => graph_row_remaining_node_needs(needed, loaded),
            (Some(needed), None) => {
                graph_row_node_needs_has_source_fields(needed).then_some(needed.clone())
            }
            (None, _) => None,
        },
        edges: match (&needed.edges, &loaded.edges) {
            (Some(needed), Some(loaded)) => graph_row_remaining_edge_needs(needed, loaded),
            (Some(needed), None) => {
                graph_row_edge_needs_has_source_fields(needed).then_some(needed.clone())
            }
            (None, _) => None,
        },
    };
    graph_row_path_needs_require_selected_field_reads(&remaining).then_some(remaining)
}

fn graph_row_node_needs_has_source_fields(needs: &NodeSelectedFieldNeeds) -> bool {
    needs.key
        || needs.created_at
        || !matches!(needs.props, PropertySelection::None)
        || !matches!(needs.vectors, VectorSelection::None)
}

fn graph_row_edge_needs_has_source_fields(needs: &EdgeSelectedFieldNeeds) -> bool {
    needs.created_at || !matches!(needs.props, PropertySelection::None)
}

fn graph_row_entity_needs_require_selected_field_reads(needs: &EntityProjectionNeeds) -> bool {
    !needs.nodes.is_empty()
        || !needs.edges.is_empty()
        || !needs.hidden_edges.is_empty()
        || needs
            .paths
            .values()
            .any(graph_row_path_needs_require_selected_field_reads)
        || needs
            .hidden_paths
            .values()
            .any(graph_row_path_needs_require_selected_field_reads)
}

fn graph_row_path_needs_require_selected_field_reads(needs: &PathSelectedFieldNeeds) -> bool {
    needs.start_node.is_some()
        || needs.end_node.is_some()
        || needs.nodes.is_some()
        || needs.edges.is_some()
}

fn graph_row_merge_node_selected_needs(
    target: &mut NodeSelectedFieldNeeds,
    incoming: &NodeSelectedFieldNeeds,
) -> Result<(), EngineError> {
    target.key |= incoming.key;
    target.created_at |= incoming.created_at;
    target.props.merge_from(&incoming.props, ProjectionNeedClass::Output)?;
    target.vectors = target.vectors.union(incoming.vectors);
    Ok(())
}

fn graph_row_merge_edge_selected_needs(
    target: &mut EdgeSelectedFieldNeeds,
    incoming: &EdgeSelectedFieldNeeds,
) -> Result<(), EngineError> {
    target.created_at |= incoming.created_at;
    target.props.merge_from(&incoming.props, ProjectionNeedClass::Output)?;
    Ok(())
}

fn graph_row_path_node_hydration_needs(
    needs: &PathSelectedFieldNeeds,
) -> Result<Option<NodeSelectedFieldNeeds>, EngineError> {
    let mut merged = NodeSelectedFieldNeeds::default();
    let mut any = false;
    for node_needs in [
        needs.start_node.as_ref(),
        needs.end_node.as_ref(),
        needs.nodes.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        graph_row_merge_node_selected_needs(&mut merged, node_needs)?;
        any = true;
    }
    Ok(any.then_some(merged))
}

fn graph_row_path_edge_hydration_needs(
    needs: &PathSelectedFieldNeeds,
) -> Result<Option<EdgeSelectedFieldNeeds>, EngineError> {
    let Some(edge_needs) = needs.edges.as_ref() else {
        return Ok(None);
    };
    let mut merged = EdgeSelectedFieldNeeds::default();
    graph_row_merge_edge_selected_needs(&mut merged, edge_needs)?;
    Ok(Some(merged))
}

fn graph_row_collect_path_node_ids(
    rows: &[crate::graph_row::GraphBindingRow],
    slot: crate::graph_row::GraphBindingSlotRef,
    needs: &PathSelectedFieldNeeds,
) -> Result<Vec<u64>, EngineError> {
    let mut ids = Vec::new();
    for row in rows {
        let Some(path) = row.path_for_slot_if_bound(slot)? else {
            continue;
        };
        if needs.nodes.is_some() {
            ids.extend(path.path.nodes.iter().copied());
        } else {
            if needs.start_node.is_some() {
                ids.extend(path.path.nodes.first().copied());
            }
            if needs.end_node.is_some() {
                ids.extend(path.path.nodes.last().copied());
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn graph_row_collect_path_edge_ids(
    rows: &[crate::graph_row::GraphBindingRow],
    slot: crate::graph_row::GraphBindingSlotRef,
    needs: &PathSelectedFieldNeeds,
) -> Result<Vec<u64>, EngineError> {
    if needs.edges.is_none() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for row in rows {
        let Some(path) = row.path_for_slot_if_bound(slot)? else {
            continue;
        };
        ids.extend(path.path.edges.iter().copied());
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn graph_row_remaining_props(
    needed: &PropertySelection,
    loaded: &PropertySelection,
) -> PropertySelection {
    match (needed, loaded) {
        (PropertySelection::None, _) | (_, PropertySelection::All) => PropertySelection::None,
        (PropertySelection::All, _) => PropertySelection::All,
        (PropertySelection::Keys(keys), PropertySelection::Keys(loaded_keys)) => {
            let remaining = keys
                .iter()
                .filter(|key| !loaded_keys.contains(*key))
                .cloned()
                .collect::<Vec<_>>();
            if remaining.is_empty() {
                PropertySelection::None
            } else {
                PropertySelection::Keys(remaining)
            }
        }
        (PropertySelection::Keys(keys), PropertySelection::None) => {
            PropertySelection::Keys(keys.clone())
        }
    }
}

fn graph_row_remaining_vectors(
    needed: VectorSelection,
    loaded: VectorSelection,
) -> VectorSelection {
    match (
        needed.needs_dense() && !loaded.needs_dense(),
        needed.needs_sparse() && !loaded.needs_sparse(),
    ) {
        (false, false) => VectorSelection::None,
        (true, false) => VectorSelection::Dense,
        (false, true) => VectorSelection::Sparse,
        (true, true) => VectorSelection::Both,
    }
}

fn graph_node_value_from_selected(
    node_id: u64,
    fields: &SelectedNodeFields,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<GraphNodeValue, EngineError> {
    Ok(GraphNodeValue {
        id: Some(node_id),
        labels: Some(graph_row_node_label_names(node_id, fields.meta.label_ids, catalog)?),
        key: fields.key.clone(),
        props: Some(graph_row_props_to_values(&fields.props)?),
        weight: Some(fields.meta.weight),
        created_at: fields.created_at,
        updated_at: Some(fields.meta.updated_at),
        dense_vector: fields.dense_vector.clone(),
        sparse_vector: fields.sparse_vector.clone(),
    })
}

fn graph_edge_value_from_selected(
    edge_id: u64,
    fields: &SelectedEdgeFields,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<GraphEdgeValue, EngineError> {
    Ok(GraphEdgeValue {
        id: Some(edge_id),
        from: Some(fields.meta.from),
        to: Some(fields.meta.to),
        label: Some(
            catalog
                .edge_label(fields.meta.label_id)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "edge record {} references missing edge-label label_id {}",
                        edge_id, fields.meta.label_id
                    ))
                })?
                .to_string(),
        ),
        props: Some(graph_row_props_to_values(&fields.props)?),
        weight: Some(fields.meta.weight),
        created_at: fields.created_at,
        updated_at: Some(fields.meta.updated_at),
        valid_from: Some(fields.meta.valid_from),
        valid_to: Some(fields.meta.valid_to),
    })
}

fn graph_row_node_label_names(
    node_id: u64,
    label_ids: NodeLabelSet,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<Vec<String>, EngineError> {
    let mut labels = Vec::with_capacity(label_ids.len());
    for &label_id in label_ids.as_slice() {
        labels.push(
            catalog
                .node_label(label_id)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "node record {} references missing node label_id {}",
                        node_id, label_id
                    ))
                })?
                .to_string(),
        );
    }
    Ok(labels)
}

fn graph_row_props_to_values(
    props: &BTreeMap<String, PropValue>,
) -> Result<BTreeMap<String, GraphValue>, EngineError> {
    props.iter()
        .map(|(key, value)| Ok((key.clone(), graph_row_prop_to_value(value)?)))
        .collect()
}

fn graph_row_prop_to_value(value: &PropValue) -> Result<GraphValue, EngineError> {
    Ok(match value {
        PropValue::Null => GraphValue::Null,
        PropValue::Bool(value) => GraphValue::Bool(*value),
        PropValue::Int(value) => GraphValue::Int(*value),
        PropValue::UInt(value) => GraphValue::UInt(*value),
        PropValue::Float(value) => GraphValue::Float(*value),
        PropValue::String(value) => GraphValue::String(value.clone()),
        PropValue::Bytes(value) => GraphValue::Bytes(value.clone()),
        PropValue::Array(values) => GraphValue::List(
            values
                .iter()
                .map(graph_row_prop_to_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PropValue::Map(values) => GraphValue::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), graph_row_prop_to_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
    })
}

impl ReadView {
    fn populate_verified_node_records(
        &self,
        page: &mut VerifiedNodePage,
    ) -> Result<(), EngineError> {
        let nodes = self.get_nodes_raw(&page.ids)?;
        page.nodes = nodes.into_iter().flatten().collect();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_node_candidate_chunk(
        &self,
        chunk: &[u64],
        query: &NormalizedNodeQuery,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        include_key: bool,
        include_created_at: bool,
        property_keys: &[String],
        ids: &mut Vec<u64>,
        target: usize,
    ) -> Result<ControlFlow<()>, EngineError> {
        #[cfg(test)]
        self.note_node_visibility_meta_reads(chunk.len());
        let visibility = self.sources().find_node_visibility_meta(chunk)?;
        let mut decisions = Vec::new();
        let mut projected_candidate_ids = Vec::new();

        for (&node_id, state) in chunk.iter().zip(visibility.iter()) {
            let NodeVisibilityState::Live(meta) = state else {
                continue;
            };
            let query_meta = node_query_meta_from_visibility(node_id, meta);
            if !query_node_metadata_constraints_match(query, &query_meta, policy_cutoffs) {
                continue;
            }
            match node_filter_metadata_outcome(&query.filter, &query_meta) {
                Some(false) => continue,
                Some(true) if !include_key => decisions.push((node_id, false)),
                Some(true) | None => {
                    decisions.push((node_id, true));
                    projected_candidate_ids.push(node_id);
                }
            }
        }

        let mut projected_matches = NodeIdSet::default();
        if !projected_candidate_ids.is_empty() {
            let projected = self.sources().find_node_projected_fields(
                &projected_candidate_ids,
                &NodeSelectedFieldNeeds {
                    key: include_key,
                    created_at: include_created_at,
                    props: PropertySelection::Keys(property_keys.to_vec()),
                    ..NodeSelectedFieldNeeds::default()
                },
            )?;
            for (&node_id, selected) in projected_candidate_ids.iter().zip(projected.iter()) {
                let Some(selected) = selected else {
                    continue;
                };
                if query_node_selected_fields_match(query, selected) {
                    projected_matches.insert(node_id);
                }
            }
        }

        for (node_id, needs_projection) in decisions {
            if needs_projection && !projected_matches.contains(&node_id) {
                continue;
            }
            ids.push(node_id);
            if ids.len() >= target {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
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
        let include_key = !query.keys.is_empty() || node_filter_needs_key(&query.filter);
        let include_created_at = node_filter_needs_created_at(&query.filter);
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&query.filter, &mut property_keys);

        for chunk in candidate_ids[start..].chunks(QUERY_VERIFY_CHUNK) {
            if self
                .verify_node_candidate_chunk(
                    chunk,
                    query,
                    policy_cutoffs,
                    include_key,
                    include_created_at,
                    &property_keys,
                    &mut ids,
                    target,
                )?
                .is_break()
            {
                break;
            }
        }

        let mut page = finalize_verified_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_node_records(&mut page)?;
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
        let mut ids = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let include_key = !query.keys.is_empty() || node_filter_needs_key(&query.filter);
        let include_created_at = node_filter_needs_created_at(&query.filter);
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&query.filter, &mut property_keys);

        self.scan_raw_node_label_candidates(
            label_ids,
            query.page.after,
            chunk_limit,
            |chunk| {
                self.verify_node_candidate_chunk(
                    chunk,
                    query,
                    policy_cutoffs,
                    include_key,
                    include_created_at,
                    &property_keys,
                    &mut ids,
                    target,
                )
            },
        )?;

        let mut page = finalize_verified_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_node_records(&mut page)?;
        }
        Ok(page)
    }

    fn query_node_page_from_single_label_scan(
        &self,
        query: &NormalizedNodeQuery,
        single_label_id: u32,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedNodePage, EngineError> {
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

        self.query_node_page_from_label_scan(query, &[single_label_id], hydrate, policy_cutoffs)
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

    fn scan_full_node_id_chunks<F>(
        &self,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
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
        let include_key = !query.keys.is_empty() || node_filter_needs_key(&query.filter);
        let include_created_at = node_filter_needs_created_at(&query.filter);
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&query.filter, &mut property_keys);

        self.scan_full_node_id_chunks(query.page.after, chunk_limit, |chunk| {
            self.verify_node_candidate_chunk(
                chunk,
                query,
                policy_cutoffs,
                include_key,
                include_created_at,
                &property_keys,
                &mut ids,
                target,
            )
        })?;

        let mut page = finalize_verified_page(ids, Vec::new(), limit);
        if hydrate {
            self.populate_verified_node_records(&mut page)?;
        }
        Ok(page)
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
                    ids: ids.as_ref().clone(),
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
                lower,
                upper,
            } => {
                let (ids, followup) = self.ready_range_candidate_ids(
                    *index_id,
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
            NodeCandidateMaterialization::CompoundPrefixIndex { entry, bounds, .. } => {
                let mut sets = Vec::with_capacity(bounds.len());
                let mut followups = Vec::new();
                for bound in bounds {
                    match self.sources().node_ids_by_compound_prefix_limited(
                        entry,
                        bound,
                        eager_cap.saturating_add(1),
                    ) {
                        Ok(crate::source_list::LimitedCompoundIndexRead::Ready(ids)) => {
                            sets.push(ids);
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::TooBroad) => {
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::MissingSidecar) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, None),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Err(error) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, Some(error)),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let ids = union_candidate_sets(&sets);
                if ids.len() > eager_cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready { ids, followups })
                }
            }
            NodeCandidateMaterialization::CompoundRangeIndex { entry, bounds, .. } => {
                let mut sets = Vec::with_capacity(bounds.len());
                let mut followups = Vec::new();
                for bound in bounds {
                    match self.sources().node_ids_by_compound_range_limited(
                        entry,
                        bound,
                        eager_cap.saturating_add(1),
                    ) {
                        Ok(crate::source_list::LimitedCompoundIndexRead::Ready(ids)) => {
                            sets.push(ids);
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::TooBroad) => {
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::MissingSidecar) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, None),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Err(error) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, Some(error)),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let ids = union_candidate_sets(&sets);
                if ids.len() > eager_cap {
                    Ok(CandidateMaterializationResult::TooBroad { followups })
                } else {
                    Ok(CandidateMaterializationResult::Ready { ids, followups })
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
                let union_cap = cap_context.union_total_cap(
                    plan.members_are_eager_index_sources(),
                    query.page.limit,
                    plan.estimate(),
                );
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
        legal_universe_fallback: Option<&PlannedNodeCandidateSource>,
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
                        let (page, mut fallback_followups) =
                            if let Some(fallback_source) = legal_universe_fallback {
                                self.query_node_page_from_source_driver(
                                    fallback_source,
                                    query,
                                    cap_context,
                                    None,
                                    hydrate,
                                    policy_cutoffs,
                                )?
                            } else {
                                self.query_node_page_from_legal_universe(
                                    query,
                                    cap_context,
                                    hydrate,
                                    policy_cutoffs,
                                )?
                            };
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
                    None,
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
        planned: PlannedNodeQuery,
        hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<(VerifiedNodePage, Vec<SecondaryIndexReadFollowup>), EngineError> {
        let PlannedNodeQuery {
            driver,
            cap_context,
            legal_universe_fallback,
            warnings: _,
            mut followups,
        } = planned;

        match &driver {
            NodePhysicalPlan::Empty => Ok((
                VerifiedNodePage {
                    ids: Vec::new(),
                    nodes: Vec::new(),
                    next_cursor: None,
                },
                followups,
            )),
            NodePhysicalPlan::Source(source) => {
                let (page, mut source_followups) = self.query_node_page_from_source_driver(
                    source,
                    query,
                    cap_context,
                    legal_universe_fallback.as_ref(),
                    hydrate,
                    policy_cutoffs,
                )?;
                followups.append(&mut source_followups);
                Ok((page, followups))
            }
            plan => {
                match self.materialize_node_physical_plan(query, cap_context, plan)? {
                    CandidateMaterializationResult::Ready {
                        ids,
                        followups: mut materialization_followups,
                    } => {
                        let page = self.query_node_page_from_candidates(
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
                        let (page, mut fallback_followups) =
                            if let Some(fallback_source) = legal_universe_fallback.as_ref() {
                                self.query_node_page_from_source_driver(
                                    fallback_source,
                                    query,
                                    cap_context,
                                    None,
                                    hydrate,
                                    policy_cutoffs,
                                )?
                            } else {
                                self.query_node_page_from_legal_universe(
                                    query,
                                    cap_context,
                                    hydrate,
                                    policy_cutoffs,
                                )?
                            };
                        followups.append(&mut materialization_followups);
                        followups.append(&mut fallback_followups);
                        Ok((page, followups))
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
            self.query_node_page_planned(&normalized, planned, false, policy_cutoffs.as_ref())?;
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
            self.query_node_page_planned(&normalized, planned, true, policy_cutoffs.as_ref())?;
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
                    ids: ids.as_ref().clone(),
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
                lower,
                upper,
            } => {
                let _ = (label_id, prop_key);
                let (ids, followup) = self.ready_edge_range_candidate_ids(
                    *index_id,
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
            EdgeCandidateMaterialization::CompoundPrefixIndex { entry, bounds, .. } => {
                let mut sets = Vec::with_capacity(bounds.len());
                let mut followups = Vec::new();
                for bound in bounds {
                    match sources.edge_ids_by_compound_prefix_limited(
                        entry,
                        bound,
                        cap.saturating_add(1),
                    ) {
                        Ok(crate::source_list::LimitedCompoundIndexRead::Ready(ids)) => {
                            sets.push(ids);
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::TooBroad) => {
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::MissingSidecar) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, None),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Err(error) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, Some(error)),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let ids = union_candidate_sets(&sets);
                if ids.len() > cap {
                    return Ok(CandidateMaterializationResult::TooBroad { followups });
                }
                return Ok(CandidateMaterializationResult::Ready { ids, followups });
            }
            EdgeCandidateMaterialization::CompoundRangeIndex { entry, bounds, .. } => {
                let mut sets = Vec::with_capacity(bounds.len());
                let mut followups = Vec::new();
                for bound in bounds {
                    match sources.edge_ids_by_compound_range_limited(
                        entry,
                        bound,
                        cap.saturating_add(1),
                    ) {
                        Ok(crate::source_list::LimitedCompoundIndexRead::Ready(ids)) => {
                            sets.push(ids);
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::TooBroad) => {
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Ok(crate::source_list::LimitedCompoundIndexRead::MissingSidecar) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, None),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                        Err(error) => {
                            followups.extend(materialization_followups(
                                self.compound_sidecar_failure_followup(entry, Some(error)),
                            ));
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
                let ids = union_candidate_sets(&sets);
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
                let cap = cap_context.union_total_cap(
                    plan.members_are_eager_index_sources(),
                    query.page.limit,
                    plan.estimate(),
                );
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
        self.sources().scan_edge_ids_by_endpoints_after(
            node_ids,
            direction,
            label_filter_ids,
            start_after,
            chunk_limit,
            |chunk| {
                #[cfg(test)]
                {
                    self.note_endpoint_adjacency_candidates(chunk.len());
                }
                visitor(chunk)
            },
        )
    }

    #[allow(dead_code)]
    pub(crate) fn scan_edge_ids_by_endpoints<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        chunk_limit: usize,
        visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        self.scan_endpoint_edge_id_chunks(
            node_ids,
            direction,
            label_filter_ids,
            None,
            chunk_limit,
            visitor,
        )
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
            for (index, record) in missing_positions.into_iter().zip(records) {
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
        let include_created_at = edge_filter_needs_created_at(&query.filter);

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
            let projected = self.sources().find_edge_projected_fields(
                &property_candidate_ids,
                &EdgeSelectedFieldNeeds {
                    created_at: include_created_at,
                    props: PropertySelection::Keys(property_keys.clone()),
                },
            )?;
            for (&edge_id, selected) in property_candidate_ids.iter().zip(projected) {
                let Some(selected) = selected else {
                    continue;
                };
                let Some(query_meta) = metadata_by_property_candidate.get(&edge_id) else {
                    continue;
                };
                let mut selected = selected;
                selected.meta = *query_meta;
                if edge_filter_projected_matches(&query.filter, &selected) {
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
        legal_universe_fallback: Option<&PlannedEdgeCandidateSource>,
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
                    let (page, mut fallback_followups) =
                        if let Some(fallback_source) = legal_universe_fallback {
                            self.query_edge_page_from_source_driver(
                                fallback_source,
                                query,
                                cap_context,
                                None,
                                hydrate,
                                policy_cutoffs,
                            )?
                        } else {
                            self.query_edge_page_from_legal_universe(
                                query,
                                cap_context,
                                hydrate,
                                policy_cutoffs,
                            )?
                        };
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
        sources.sort_by_cached_key(|source| EdgePhysicalPlan::source(source.clone()).plan_cost());
        let source = sources
            .first()
            .expect("legal edge universe sources must be non-empty");
        self.query_edge_page_from_source_driver(
            source,
            query,
            cap_context,
            None,
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
            legal_universe_fallback,
            warnings: _,
            mut followups,
        } = planned;

        if let EdgePhysicalPlan::Source(source) = &driver {
            let (page, mut source_followups) = self.query_edge_page_from_source_driver(
                source,
                query,
                cap_context,
                legal_universe_fallback.as_ref(),
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
                let (page, mut fallback_followups) =
                    if let Some(fallback_source) = legal_universe_fallback.as_ref() {
                        self.query_edge_page_from_source_driver(
                            fallback_source,
                            query,
                            cap_context,
                            None,
                            hydrate,
                            policy_cutoffs,
                        )?
                    } else {
                        self.query_edge_page_from_legal_universe(
                            query,
                            cap_context,
                            hydrate,
                            policy_cutoffs,
                        )?
                    };
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

    fn normalize_graph_row_runtime_plan(
        &self,
        query: &NormalizedGraphRowQuery,
    ) -> Result<GraphRowRuntimePlan, EngineError> {
        let mut nodes = Vec::with_capacity(query.nodes.len());
        let mut node_by_alias = BTreeMap::new();
        let mut warnings = Vec::new();
        for node in &query.nodes {
            let runtime = self.normalize_graph_row_runtime_node(node, query)?;
            for warning in &runtime.query.warnings {
                push_query_warning(&mut warnings, *warning);
            }
            node_by_alias.insert(runtime.alias.clone(), nodes.len());
            nodes.push(runtime);
        }

        let mut bound_slots = query
            .initial_bound_slots
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut next_hidden_id = 0usize;
        let mut runtime = self.normalize_graph_row_runtime_piece_plan(
            query,
            &query.pieces,
            &[],
            nodes,
            node_by_alias,
            &mut next_hidden_id,
            &mut bound_slots,
        )?;
        for warning in warnings {
            push_query_warning(&mut runtime.warnings, warning);
        }
        Ok(runtime)
    }

    #[allow(clippy::too_many_arguments)]
    fn normalize_graph_row_runtime_piece_plan(
        &self,
        query: &NormalizedGraphRowQuery,
        pieces: &[GraphPatternPiece],
        scope: &[usize],
        nodes: Vec<GraphRowRuntimeNode>,
        node_by_alias: BTreeMap<String, usize>,
        next_hidden_id: &mut usize,
        bound_slots: &mut BTreeSet<crate::graph_row::GraphBindingSlotRef>,
    ) -> Result<GraphRowRuntimePlan, EngineError> {
        let mut edges = Vec::new();
        let mut required_segments = Vec::new();
        let mut steps = Vec::new();
        let mut warnings = Vec::new();
        let mut current_segment_edges = Vec::new();
        let mut pending_barriers = Vec::new();
        let mut edge_by_piece = BTreeMap::new();
        let fixed_paths = graph_row_fixed_paths_for_scope(query, scope);
        let mut next_fixed_path = 0usize;
        for (piece_index, piece) in pieces.iter().enumerate() {
            match piece {
                GraphPatternPiece::Edge(edge) => {
                    let runtime =
                        self.normalize_graph_row_runtime_edge(edge, query, next_hidden_id)?;
                    for warning in &runtime.warnings {
                        push_query_warning(&mut warnings, *warning);
                    }
                    current_segment_edges.push(edges.len());
                    bound_slots.insert(runtime.from_slot);
                    bound_slots.insert(runtime.to_slot);
                    if let Some(edge_slot) = runtime.edge_slot {
                        bound_slots.insert(edge_slot);
                    }
                    if let Some(hidden_slot) = runtime.hidden_slot {
                        bound_slots.insert(hidden_slot);
                    }
                    edge_by_piece.insert(piece_index, edges.len());
                    edges.push(runtime);
                }
                GraphPatternPiece::Optional(group) => {
                    if let Some(segment_index) = graph_row_push_required_segment(
                        &mut required_segments,
                        &mut current_segment_edges,
                        &mut pending_barriers,
                    ) {
                        steps.push(GraphRowRuntimeStep::RequiredSegment(segment_index));
                    }
                    graph_row_push_fixed_path_steps_before_piece(
                        query,
                        &fixed_paths,
                        piece_index,
                        &edge_by_piece,
                        &nodes,
                        &node_by_alias,
                        &edges,
                        &mut next_fixed_path,
                        &mut steps,
                        bound_slots,
                    )?;
                    let left_slots = bound_slots.iter().copied().collect::<Vec<_>>();
                    let dependency_slots = graph_row_optional_dependency_slots(
                        group,
                        &query.binding_schema,
                        bound_slots,
                    )?;
                    let before_group = bound_slots.clone();
                    let mut group_bound_slots = before_group.clone();
                    let mut group_scope = scope.to_vec();
                    group_scope.push(piece_index);
                    let group_runtime = self.normalize_graph_row_runtime_piece_plan(
                        query,
                        &group.pieces,
                        &group_scope,
                        nodes.clone(),
                        node_by_alias.clone(),
                        next_hidden_id,
                        &mut group_bound_slots,
                    )?;
                    let mut introduced_slots = group_bound_slots
                        .difference(&before_group)
                        .copied()
                        .collect::<Vec<_>>();
                    introduced_slots.sort_unstable();
                    for warning in &group_runtime.warnings {
                        push_query_warning(&mut warnings, *warning);
                    }
                    let where_expr = group
                        .where_
                        .as_ref()
                        .map(|expr| crate::graph_row::bind_graph_expr(&query.binding_schema, expr))
                        .transpose()?;
                    let where_needs = group
                        .where_
                        .as_ref()
                        .map(|expr| {
                            crate::graph_row::collect_graph_expr_projection_needs(
                                &query.binding_schema,
                                expr,
                                ProjectionNeedClass::Residual,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let where_present = where_expr.is_some();
                    steps.push(GraphRowRuntimeStep::Optional(GraphRowRuntimeOptionalGroup {
                        piece_index,
                        pieces_len: group.pieces.len(),
                        runtime: Box::new(group_runtime),
                        introduced_slots: introduced_slots.clone(),
                        dependency_slots,
                        left_slots,
                        where_expr,
                        where_needs,
                        where_present,
                    }));
                    bound_slots.extend(introduced_slots);
                    pending_barriers.push(GraphRowPlanBarrier {
                        kind: GraphRowPlanBarrierKind::Optional,
                        piece_index,
                    });
                }
                GraphPatternPiece::VariableLength(vlp) => {
                    if let Some(segment_index) = graph_row_push_required_segment(
                        &mut required_segments,
                        &mut current_segment_edges,
                        &mut pending_barriers,
                    ) {
                        steps.push(GraphRowRuntimeStep::RequiredSegment(segment_index));
                    }
                    graph_row_push_fixed_path_steps_before_piece(
                        query,
                        &fixed_paths,
                        piece_index,
                        &edge_by_piece,
                        &nodes,
                        &node_by_alias,
                        &edges,
                        &mut next_fixed_path,
                        &mut steps,
                        bound_slots,
                    )?;
                    let runtime =
                        self.normalize_graph_row_runtime_vlp(piece_index, vlp, query, next_hidden_id)?;
                    for warning in &runtime.warnings {
                        push_query_warning(&mut warnings, *warning);
                    }
                    bound_slots.insert(runtime.from_slot);
                    bound_slots.insert(runtime.to_slot);
                    if let Some(edge_slot) = runtime.edge_slot {
                        bound_slots.insert(edge_slot);
                    }
                    if let Some(path_slot) = runtime.path_slot {
                        bound_slots.insert(path_slot);
                    }
                    if let Some(hidden_slot) = runtime.hidden_slot {
                        bound_slots.insert(hidden_slot);
                    }
                    steps.push(GraphRowRuntimeStep::VariableLength(runtime));
                    pending_barriers.push(GraphRowPlanBarrier {
                        kind: GraphRowPlanBarrierKind::VariableLength,
                        piece_index,
                    });
                }
            }
        }
        if let Some(segment_index) = graph_row_push_required_segment(
            &mut required_segments,
            &mut current_segment_edges,
            &mut pending_barriers,
        ) {
            steps.push(GraphRowRuntimeStep::RequiredSegment(segment_index));
        }
        graph_row_push_remaining_fixed_path_steps(
            query,
            &fixed_paths,
            &edge_by_piece,
            &nodes,
            &node_by_alias,
            &edges,
            &mut next_fixed_path,
            &mut steps,
            bound_slots,
        )?;

        Ok(GraphRowRuntimePlan {
            nodes,
            node_by_alias,
            edges,
            required_segments,
            steps,
            warnings,
        })
    }

    fn normalize_graph_row_runtime_node(
        &self,
        node: &GraphNodePattern,
        query: &NormalizedGraphRowQuery,
    ) -> Result<GraphRowRuntimeNode, EngineError> {
        let slot = query
            .binding_schema
            .slot_for_alias(&node.alias)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row node alias '{}' is missing from binding schema",
                    node.alias
                ))
            })?;
        let slot_info = query.binding_schema.slot(slot).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row node alias '{}' is missing from binding schema",
                node.alias
            ))
        })?;
        if slot_info.kind != crate::graph_row::GraphBindingSlotKind::Node {
            return Err(EngineError::InvalidOperation(format!(
                "graph row node alias '{}' resolved to a non-node binding slot",
                node.alias
            )));
        }
        let (label_filter, single_label_id, warnings) =
            self.resolve_node_query_label_filter(node.label_filter.as_ref())?;
        let mut filter = normalize_optional_node_filter(node.filter.as_ref())?;
        if label_filter.is_empty_constraint() {
            filter = NormalizedNodeFilter::AlwaysFalse;
        }

        let mut ids = node.ids.clone();
        if !node.keys.is_empty() {
            let mut key_refs = Vec::with_capacity(node.keys.len());
            for key in &node.keys {
                match self.label_catalog.resolve_node_label_for_read(&key.label)? {
                    Some(label_id) => key_refs.push((label_id, key.key.as_str())),
                    None => {
                        filter = NormalizedNodeFilter::AlwaysFalse;
                    }
                }
            }
            if !key_refs.is_empty() {
                ids.extend(
                    self.sources()
                        .find_node_ids_by_label_keys(&key_refs)?
                        .into_iter()
                        .flatten(),
                );
            }
        }
        ids.sort_unstable();
        ids.dedup();

        Ok(GraphRowRuntimeNode {
            alias: node.alias.clone(),
            slot,
            query: NormalizedNodeQuery {
                single_label_id,
                label_filter,
                ids,
                keys: Vec::new(),
                filter,
                allow_full_scan: query.options.allow_full_scan,
                page: PageRequest::default(),
                warnings,
            },
        })
    }

    fn normalize_graph_row_runtime_edge(
        &self,
        edge: &GraphEdgePattern,
        query: &NormalizedGraphRowQuery,
        next_hidden_id: &mut usize,
    ) -> Result<GraphRowRuntimeEdge, EngineError> {
        let from_slot = query
            .binding_schema
            .slot_for_alias(&edge.from_alias)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row edge references unknown from alias '{}'",
                    edge.from_alias
                ))
            })?;
        let to_slot = query
            .binding_schema
            .slot_for_alias(&edge.to_alias)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row edge references unknown to alias '{}'",
                    edge.to_alias
                ))
            })?;
        let edge_slot = edge
            .alias
            .as_ref()
            .map(|alias| {
                query.binding_schema.slot_for_alias(alias).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row edge alias '{alias}' is missing from binding schema"
                    ))
                })
            })
            .transpose()?;
        let hidden_slot = if edge.alias.is_none() {
            let slot = crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::HiddenOccurrence,
                index: *next_hidden_id,
            };
            query.binding_schema.slot(slot).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row hidden edge occurrence slot {} is missing",
                    *next_hidden_id
                ))
            })?;
            *next_hidden_id += 1;
            Some(slot)
        } else {
            None
        };

        let (label_resolution, warnings) = self
            .label_catalog
            .resolve_edge_label_filter(Some(&edge.label_filter))?;
        let label_filter_ids = match label_resolution {
            LabelFilterResolution::Unconstrained => None,
            LabelFilterResolution::Known(label_ids) => Some(label_ids),
            LabelFilterResolution::EmptyConstraint => Some(Vec::new()),
        };
        let mut candidate_edge_ids = edge
            .alias
            .as_ref()
            .and_then(|alias| query.edge_id_constraints.get(alias))
            .cloned()
            .unwrap_or_default();
        candidate_edge_ids.sort_unstable();
        candidate_edge_ids.dedup();

        Ok(GraphRowRuntimeEdge {
            alias: edge.alias.clone(),
            edge_slot,
            hidden_slot,
            from_alias: edge.from_alias.clone(),
            to_alias: edge.to_alias.clone(),
            from_slot,
            to_slot,
            direction: edge.direction,
            candidate_edge_ids,
            label_filter_ids,
            filter: normalize_optional_edge_filter(edge.filter.as_ref())?,
            warnings,
        })
    }

    fn normalize_graph_row_runtime_vlp(
        &self,
        piece_index: usize,
        path: &GraphVariableLengthPattern,
        query: &NormalizedGraphRowQuery,
        next_hidden_id: &mut usize,
    ) -> Result<GraphRowRuntimeVariableLength, EngineError> {
        let from_slot = query
            .binding_schema
            .slot_for_alias(&path.from_alias)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row variable-length pattern references unknown from alias '{}'",
                    path.from_alias
                ))
            })?;
        let to_slot = query
            .binding_schema
            .slot_for_alias(&path.to_alias)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row variable-length pattern references unknown to alias '{}'",
                    path.to_alias
                ))
            })?;
        let path_slot = path
            .path_alias
            .as_ref()
            .map(|alias| {
                query.binding_schema.slot_for_alias(alias).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row path alias '{alias}' is missing from binding schema"
                    ))
                })
            })
            .transpose()?;
        let edge_slot = path
            .edge_alias
            .as_ref()
            .map(|alias| {
                query.binding_schema.slot_for_alias(alias).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row variable-length edge alias '{alias}' is missing from binding schema"
                    ))
                })
            })
            .transpose()?;
        let hidden_slot = if path.edge_alias.is_none() && path.path_alias.is_none() {
            let slot = crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::HiddenOccurrence,
                index: *next_hidden_id,
            };
            query.binding_schema.slot(slot).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row hidden path occurrence slot {} is missing",
                    *next_hidden_id
                ))
            })?;
            *next_hidden_id += 1;
            Some(slot)
        } else {
            None
        };

        let (label_resolution, warnings) = self
            .label_catalog
            .resolve_edge_label_filter(Some(&path.label_filter))?;
        let label_filter_ids = match label_resolution {
            LabelFilterResolution::Unconstrained => None,
            LabelFilterResolution::Known(label_ids) => Some(label_ids),
            LabelFilterResolution::EmptyConstraint => Some(Vec::new()),
        };
        let mut candidate_edge_ids = path
            .edge_alias
            .as_ref()
            .and_then(|alias| query.edge_id_constraints.get(alias))
            .cloned()
            .unwrap_or_default();
        candidate_edge_ids.sort_unstable();
        candidate_edge_ids.dedup();

        Ok(GraphRowRuntimeVariableLength {
            piece_index,
            path_alias: path.path_alias.clone(),
            edge_alias: path.edge_alias.clone(),
            path_slot,
            edge_slot,
            hidden_slot,
            from_alias: path.from_alias.clone(),
            to_alias: path.to_alias.clone(),
            from_slot,
            to_slot,
            direction: path.direction,
            candidate_edge_ids,
            label_filter_ids,
            filter: normalize_optional_edge_filter(path.filter.as_ref())?,
            min_hops: path.min_hops,
            max_hops: path.max_hops,
            warnings,
        })
    }

    fn query_graph_rows_outcome(
        &self,
        query: &NormalizedGraphRowQuery,
        cursor_state: GraphRowCursorState,
    ) -> Result<QueryExecutionOutcome<GraphRowResult>, EngineError> {
        let started_at = std::time::Instant::now();
        #[cfg(test)]
        self.query_execution_counters
            .graph_row_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let effective_at_epoch = cursor_state.effective_at_epoch;
        let original_skip = cursor_state.original_skip;
        let fingerprints = graph_row_cursor_fingerprints(query, effective_at_epoch, original_skip);
        if let Some(cursor) = cursor_state.decoded.as_ref() {
            graph_row_validate_cursor_fingerprints(cursor, &fingerprints)?;
            graph_row_validate_cursor_shape(query, cursor)?;
        }
        let page_start = if cursor_state.is_cursor_page() {
            0
        } else {
            query.page.skip
        };
        let selection_capacity = graph_row_selection_capacity(query, &cursor_state)?;

        let runtime = self.normalize_graph_row_runtime_plan(query)?;
        let physical_plan =
            self.plan_graph_row_physical(query, &runtime, query.options.include_plan)?;
        let policy_cutoffs = self.query_policy_cutoffs();
        let mut explain_trace = if query.options.include_plan {
            let mut trace = GraphRowExplainTrace::default();
            self.populate_graph_row_explain_trace_from_runtime(
                query,
                &cursor_state,
                &runtime,
                &physical_plan,
                &mut trace,
            )?;
            Some(trace)
        } else {
            None
        };
        if graph_row_node_only_default_order_fast_path(query, &runtime) {
            if let Some(outcome) = self.query_graph_rows_node_only_default_order_outcome(
                query,
                &runtime,
                &cursor_state,
                &fingerprints,
                effective_at_epoch,
                original_skip,
                selection_capacity,
                started_at,
                policy_cutoffs.as_ref(),
                explain_trace.take(),
            )? {
                return Ok(outcome);
            }
        }
        let mut followups = Vec::new();
        let mut intermediate_peak = 0;
        let mut frontier_peak = 0;
        let mut paths_enumerated = 0;
        let mut rows = self.graph_row_execute_runtime_plan(
            query,
            &runtime,
            &physical_plan,
            None,
            GraphRowRuntimeGoal::AllRows,
            effective_at_epoch,
            policy_cutoffs.as_ref(),
            &mut followups,
            &mut frontier_peak,
            &mut intermediate_peak,
            &mut paths_enumerated,
            explain_trace.as_mut(),
        )?;

        let residual_needs = query.projection_needs.residual.clone();
        if rows.len() > query.options.max_order_materialization
            && graph_row_entity_needs_require_selected_field_reads(&residual_needs)
        {
            return Err(graph_row_cap_error(
                "max_order_materialization",
                query.options.max_order_materialization,
            ));
        }
        let mut pre_page_needs = residual_needs.clone();
        let order_loaded_before_filter = rows.len() <= query.options.max_order_materialization;
        if order_loaded_before_filter {
            pre_page_needs.merge_from(&query.projection_needs.order, ProjectionNeedClass::Order)?;
        }
        self.hydrate_graph_rows_for_needs(&mut rows, &query.binding_schema, &pre_page_needs)?;
        let mut filtered = Vec::with_capacity(rows.len().min(query.options.max_order_materialization));
        for row in rows {
            if let Some(where_expr) = query.bound_where.as_ref() {
                let context = crate::graph_row::BoundGraphEvalContext { row: &row };
                if !crate::graph_row::eval_bound_graph_predicate(where_expr, &context)? {
                    continue;
                }
            }
            if filtered.len() >= query.options.max_order_materialization {
                return Err(graph_row_cap_error(
                    "max_order_materialization",
                    query.options.max_order_materialization,
                ));
            }
            filtered.push(row);
        }
        let rows_after_filter = filtered.len();
        if !order_loaded_before_filter {
            let order_needs =
                graph_row_remaining_output_needs(&query.projection_needs.order, &pre_page_needs);
            self.hydrate_graph_rows_for_needs(&mut filtered, &query.binding_schema, &order_needs)?;
            pre_page_needs.merge_from(&query.projection_needs.order, ProjectionNeedClass::Order)?;
        }

        let mut selected = BinaryHeap::new();
        let mut rows_seen_for_page = 0usize;
        let order_directions = graph_row_order_directions(&query.bound_order_by);
        for row in filtered {
            let logical_key = row.logical_sort_key(&query.binding_schema)?;
            let sort_key = graph_row_explicit_sort_key(query, &row)?;
            if let Some(cursor) = cursor_state.decoded.as_ref() {
                let ordering = compare_graph_final_keys_by_directions(
                    &sort_key,
                    &logical_key,
                    &cursor.last_sort_key,
                    &cursor.last_logical_row_key,
                    &order_directions,
                );
                if ordering != std::cmp::Ordering::Greater {
                    continue;
                }
            }
            rows_seen_for_page = rows_seen_for_page.saturating_add(1);
            graph_row_insert_bounded_candidate(
                &mut selected,
                GraphRowPageCandidate {
                    sort_key,
                    logical_key,
                    row,
                },
                selection_capacity,
                &order_directions,
            );
        }
        let mut selected = selected
            .into_iter()
            .map(|candidate| candidate.candidate)
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            compare_graph_final_keys_by_directions(
                &left.sort_key,
                &left.logical_key,
                &right.sort_key,
                &right.logical_key,
                &order_directions,
            )
        });

        let effective_page_limit = graph_row_effective_page_limit(query, &cursor_state);
        let page_end = page_start
            .saturating_add(effective_page_limit)
            .min(selected.len());
        let mut page_candidates = if page_start >= selected.len() {
            Vec::new()
        } else {
            selected[page_start..page_end].to_vec()
        };
        let rows_emitted_after_page =
            graph_row_rows_emitted_after_page(&cursor_state, page_candidates.len())?;
        let has_more = !graph_row_logical_limit_exhausted(query, rows_emitted_after_page)
            && selected.len() > page_end;
        let next_cursor = if has_more {
            page_candidates.last().map(|last| {
                graph_row_encode_cursor(&GraphRowCursorPayload {
                    effective_at_epoch,
                    original_skip,
                    page_sequence: cursor_state
                        .decoded
                        .as_ref()
                        .map(|cursor| cursor.page_sequence.saturating_add(1))
                        .unwrap_or(1),
                    rows_emitted_after_skip: rows_emitted_after_page,
                    query_fingerprint: fingerprints.query,
                    order_fingerprint: fingerprints.order,
                    output_fingerprint: fingerprints.output,
                    params_fingerprint: fingerprints.params,
                    last_sort_key: last.sort_key.clone(),
                    last_logical_row_key: last.logical_key.clone(),
                }, query.options.max_cursor_bytes)
            })
        } else {
            None
        }
        .transpose()?;

        let mut page_rows = page_candidates
            .drain(..)
            .map(|candidate| candidate.row)
            .collect::<Vec<_>>();
        let output_needs =
            graph_row_remaining_output_needs(&query.projection_needs.output, &pre_page_needs);
        self.hydrate_graph_rows_for_needs(&mut page_rows, &query.binding_schema, &output_needs)?;

        let mut result_rows = Vec::with_capacity(page_rows.len());
        for row in &page_rows {
            result_rows.push(GraphRow {
                values: crate::graph_row::project_bound_graph_row_values(
                    row,
                    &query.bound_return_items,
                    &query.output,
                )?,
            });
        }

        let mut warnings = runtime
            .warnings
            .iter()
            .map(|warning| format!("{warning:?}"))
            .collect::<Vec<_>>();
        warnings.sort();
        warnings.dedup();
        let rows_returned = result_rows.len();
        let runtime_stats = GraphRowExplainRuntimeStats {
            rows_returned,
            rows_after_filter,
            rows_seen_for_page,
            intermediate_bindings_peak: intermediate_peak,
            frontier_peak,
            paths_enumerated,
            next_cursor: next_cursor.is_some(),
        };
        let plan = explain_trace.map(|trace| {
            build_graph_row_explain(
                query,
                Some(effective_at_epoch),
                &cursor_state,
                Some(trace),
                Some(runtime_stats),
            )
        });
        let result = GraphRowResult {
            columns: query.columns.clone(),
            rows: result_rows,
            next_cursor,
            stats: GraphRowStats {
                rows_returned,
                rows_after_filter,
                rows_seen_for_page,
                intermediate_bindings_peak: intermediate_peak,
                frontier_peak,
                paths_enumerated,
                db_hits: 0,
                elapsed_us: query
                    .options
                    .profile
                    .then(|| started_at.elapsed().as_micros() as u64),
                effective_at_epoch,
                warnings,
            },
            plan,
        };
        Ok(QueryExecutionOutcome {
            value: result,
            followups,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn query_graph_rows_node_only_default_order_outcome(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        cursor_state: &GraphRowCursorState,
        fingerprints: &GraphRowCursorFingerprints,
        effective_at_epoch: i64,
        original_skip: u64,
        selection_capacity: usize,
        started_at: std::time::Instant,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        explain_trace: Option<GraphRowExplainTrace>,
    ) -> Result<Option<QueryExecutionOutcome<GraphRowResult>>, EngineError> {
        if !query.pieces.is_empty()
            || runtime.nodes.len() != 1
            || !query.bound_order_by.is_empty()
            || query.bound_where.is_some()
            || !query.edge_id_constraints.is_empty()
        {
            return Ok(None);
        }

        let anchor = &runtime.nodes[0];
        let cursor_after = match cursor_state.decoded.as_ref() {
            Some(cursor) => match cursor.last_logical_row_key.as_slice() {
                [crate::graph_row::GraphSortAtom::Node(id)] => Some(*id),
                [crate::graph_row::GraphSortAtom::Null] => {
                    return Err(invalid_graph_row_cursor(
                        "node-only cursor logical row key cannot be null",
                    ));
                }
                _ => return Ok(None),
            },
            None => None,
        };

        let mut anchor_query = anchor.query.clone();
        anchor_query.page.after = cursor_after;
        anchor_query.page.limit = Some(selection_capacity);
        let planned = self.plan_normalized_node_query(&anchor_query)?;
        let (page, followups) =
            self.query_node_page_planned(&anchor_query, planned, false, policy_cutoffs)?;

        let page_start = if cursor_state.is_cursor_page() {
            0
        } else {
            query.page.skip
        };
        let rows_seen_for_page = page.ids.len().saturating_sub(page_start);
        let mut page_ids = page
            .ids
            .iter()
            .copied()
            .skip(page_start)
            .collect::<Vec<_>>();
        let effective_page_limit = graph_row_effective_page_limit(query, cursor_state);
        let page_ids_had_extra = page_ids.len() > effective_page_limit;
        if page_ids_had_extra {
            page_ids.truncate(effective_page_limit);
        }
        let rows_emitted_after_page =
            graph_row_rows_emitted_after_page(cursor_state, page_ids.len())?;
        let has_more = !graph_row_logical_limit_exhausted(query, rows_emitted_after_page)
            && (page.next_cursor.is_some() || page_ids_had_extra);

        let next_cursor = if has_more {
            page_ids.last().map(|last_id| {
                graph_row_encode_cursor(&GraphRowCursorPayload {
                    effective_at_epoch,
                    original_skip,
                    page_sequence: cursor_state
                        .decoded
                        .as_ref()
                        .map(|cursor| cursor.page_sequence.saturating_add(1))
                        .unwrap_or(1),
                    rows_emitted_after_skip: rows_emitted_after_page,
                    query_fingerprint: fingerprints.query,
                    order_fingerprint: fingerprints.order,
                    output_fingerprint: fingerprints.output,
                    params_fingerprint: fingerprints.params,
                    last_sort_key: Vec::new(),
                    last_logical_row_key: vec![crate::graph_row::GraphSortAtom::Node(*last_id)],
                }, query.options.max_cursor_bytes)
            })
        } else {
            None
        }
        .transpose()?;

        let mut page_rows = Vec::with_capacity(page_ids.len());
        for node_id in page_ids {
            let mut row = query.binding_schema.empty_row();
            row.bind_node(
                anchor.slot,
                crate::graph_row::GraphBoundNode::id_only(node_id),
            )?;
            page_rows.push(row);
        }
        self.hydrate_graph_rows_for_needs(
            &mut page_rows,
            &query.binding_schema,
            &query.projection_needs.output,
        )?;

        let mut result_rows = Vec::with_capacity(page_rows.len());
        for row in &page_rows {
            result_rows.push(GraphRow {
                values: crate::graph_row::project_bound_graph_row_values(
                    row,
                    &query.bound_return_items,
                    &query.output,
                )?,
            });
        }

        let mut warnings = runtime
            .warnings
            .iter()
            .map(|warning| format!("{warning:?}"))
            .collect::<Vec<_>>();
        warnings.sort();
        warnings.dedup();
        let rows_returned = result_rows.len();
        let rows_after_filter = page.ids.len();
        let runtime_stats = GraphRowExplainRuntimeStats {
            rows_returned,
            rows_after_filter,
            rows_seen_for_page,
            intermediate_bindings_peak: rows_after_filter,
            frontier_peak: 0,
            paths_enumerated: 0,
            next_cursor: next_cursor.is_some(),
        };
        let plan = explain_trace.map(|trace| {
            build_graph_row_explain(
                query,
                Some(effective_at_epoch),
                cursor_state,
                Some(trace),
                Some(runtime_stats),
            )
        });
        Ok(Some(QueryExecutionOutcome {
            value: GraphRowResult {
                columns: query.columns.clone(),
                rows: result_rows,
                next_cursor,
                stats: GraphRowStats {
                    rows_returned,
                    rows_after_filter,
                    rows_seen_for_page,
                    intermediate_bindings_peak: rows_after_filter,
                    frontier_peak: 0,
                    paths_enumerated: 0,
                    db_hits: 0,
                    elapsed_us: query
                        .options
                        .profile
                        .then(|| started_at.elapsed().as_micros() as u64),
                    effective_at_epoch,
                    warnings,
                },
                plan,
            },
            followups,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_runtime_plan(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        initial_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
        goal: GraphRowRuntimeGoal,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        intermediate_peak: &mut usize,
        paths_enumerated: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        if runtime.steps.is_empty() {
            let fallback_node_driver;
            let initial_driver = match &physical_plan.initial_driver {
                GraphRowInitialDriver::Empty { .. } if !runtime.nodes.is_empty() => {
                    fallback_node_driver = GraphRowInitialDriver::Node {
                        node_index: 0,
                        alias: runtime.nodes[0].alias.clone(),
                    };
                    &fallback_node_driver
                }
                driver => driver,
            };
            let rows = match initial_rows {
                Some(rows) => self.graph_row_seed_required_segment_rows(
                    query,
                    runtime,
                    initial_driver,
                    rows,
                    GraphRowRuntimeGoal::AllRows,
                    policy_cutoffs,
                    followups,
                )?,
                None => self.graph_row_initial_rows(
                    query,
                    runtime,
                    initial_driver,
                    goal,
                    policy_cutoffs,
                    followups,
                )?,
            };
            graph_row_record_cap_peak(
                intermediate_peak,
                rows.len(),
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
            )?;
            return Ok(rows);
        }

        let mut rows = initial_rows;
        let step_count = runtime.steps.len();
        for (step_index, step) in runtime.steps.iter().enumerate() {
            let step_goal = if step_index + 1 == step_count {
                goal
            } else {
                GraphRowRuntimeGoal::AllRows
            };
            let next_rows = match step {
                GraphRowRuntimeStep::RequiredSegment(segment_index) => {
                    let segment_plan = physical_plan
                        .segments
                        .iter()
                        .find(|segment| segment.segment_index == *segment_index)
                        .ok_or_else(|| {
                            EngineError::InvalidOperation(format!(
                                "graph row physical plan is missing required segment {segment_index}"
                            ))
                        })?;
                    self.graph_row_execute_required_segment(
                        query,
                        runtime,
                        physical_plan,
                        segment_plan,
                        rows.take(),
                        step_goal,
                        effective_at_epoch,
                        policy_cutoffs,
                        followups,
                        frontier_peak,
                        explain_trace.as_deref_mut(),
                    )?
                }
                GraphRowRuntimeStep::FixedPath(path) => {
                    let left_rows = rows
                        .take()
                        .unwrap_or_else(|| vec![query.binding_schema.empty_row()]);
                    self.graph_row_compose_fixed_path_rows(
                        path,
                        left_rows,
                        step_goal,
                        explain_trace.as_deref_mut(),
                    )?
                }
                GraphRowRuntimeStep::Optional(group) => {
                    let left_rows = rows
                        .take()
                        .unwrap_or_else(|| vec![query.binding_schema.empty_row()]);
                    self.graph_row_execute_optional_group(
                        query,
                        group,
                        left_rows,
                        effective_at_epoch,
                        policy_cutoffs,
                        followups,
                        frontier_peak,
                        intermediate_peak,
                        paths_enumerated,
                        explain_trace.as_deref_mut(),
                    )?
                }
                GraphRowRuntimeStep::VariableLength(path) => {
                    let left_rows = rows
                        .take()
                        .unwrap_or_else(|| vec![query.binding_schema.empty_row()]);
                    self.graph_row_execute_variable_length(
                        query,
                        runtime,
                        path,
                        left_rows,
                        effective_at_epoch,
                        policy_cutoffs,
                        followups,
                        frontier_peak,
                        paths_enumerated,
                        step_goal,
                        explain_trace.as_deref_mut(),
                    )?
                }
            };
            graph_row_record_cap_peak(
                intermediate_peak,
                next_rows.len(),
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
            )?;
            rows = Some(next_rows);
        }

        Ok(rows.unwrap_or_else(|| vec![query.binding_schema.empty_row()]))
    }

    fn graph_row_compose_fixed_path_rows(
        &self,
        fixed_path: &GraphRowRuntimeFixedPath,
        mut rows: Vec<crate::graph_row::GraphBindingRow>,
        goal: GraphRowRuntimeGoal,
        explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        for row in &mut rows {
            let mut node_ids = Vec::with_capacity(fixed_path.node_slots.len());
            for slot in &fixed_path.node_slots {
                let Some(node_id) = row.node_id_for_slot_if_bound(*slot)? else {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph row fixed path '{}' cannot compose from an unbound node slot",
                        fixed_path.alias
                    )));
                };
                node_ids.push(node_id);
            }
            let mut edge_ids = Vec::with_capacity(fixed_path.edge_slots.len());
            for slot in &fixed_path.edge_slots {
                let edge_id = match slot {
                    GraphRowRuntimeFixedPathEdgeSlot::Edge(slot) => {
                        row.edge_id_for_slot_if_bound(*slot)?
                    }
                    GraphRowRuntimeFixedPathEdgeSlot::Hidden(slot) => {
                        row.hidden_edge_id_for_slot_if_bound(*slot)?
                    }
                }
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row fixed path '{}' cannot compose from an unbound edge slot",
                        fixed_path.alias
                    ))
                })?;
                edge_ids.push(edge_id);
            }
            row.bind_path(
                fixed_path.path_slot,
                crate::graph_row::GraphBoundPath::id_only(GraphPath {
                    nodes: node_ids,
                    edges: edge_ids,
                })?,
            )?;
        }
        if let Some(trace) = explain_trace {
            trace.record_plan(
                "FixedPathComposeRuntime",
                format!(
                    "path={}; rows={}; edges_per_path={}; no_new_index_scans=true; hydration_deferred=true",
                    fixed_path.alias,
                    rows.len(),
                    fixed_path.edge_slots.len()
                ),
            );
        }
        if goal.reached(rows.len()) {
            rows.truncate(1);
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_required_segment(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        segment_plan: &GraphRowPhysicalSegment,
        current_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
        goal: GraphRowRuntimeGoal,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let mut rows = match current_rows {
            Some(rows) => self.graph_row_seed_required_segment_rows(
                query,
                runtime,
                &segment_plan.initial_driver,
                rows,
                GraphRowRuntimeGoal::AllRows,
                policy_cutoffs,
                followups,
            )?,
            None => self.graph_row_initial_rows(
                query,
                runtime,
                &segment_plan.initial_driver,
                GraphRowRuntimeGoal::AllRows,
                policy_cutoffs,
                followups,
            )?,
        };

        let edge_count = segment_plan.edge_order.len();
        for (position, &edge_index) in segment_plan.edge_order.iter().enumerate() {
            let edge = &runtime.edges[edge_index];
            let planned_source_choice = physical_plan
                .edge_source_choices
                .get(edge_index)
                .and_then(|choice| *choice);
            let edge_goal = if position + 1 == edge_count {
                goal
            } else {
                GraphRowRuntimeGoal::AllRows
            };
            rows = self.graph_row_expand_fixed_edge(
                query,
                runtime,
                edge,
                planned_source_choice,
                rows,
                edge_goal,
                effective_at_epoch,
                policy_cutoffs,
                followups,
                frontier_peak,
                explain_trace.as_deref_mut(),
            )?;
        }
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_seed_required_segment_rows(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        initial_driver: &GraphRowInitialDriver,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        goal: GraphRowRuntimeGoal,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let GraphRowInitialDriver::Node { node_index, .. } = initial_driver else {
            return Ok(rows);
        };
        if rows.is_empty() {
            return Ok(rows);
        }
        let anchor = runtime.nodes.get(*node_index).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row physical plan references missing node index {node_index}"
            ))
        })?;

        let mut output = Vec::with_capacity(rows.len());
        let mut anchor_ids: Option<Vec<u64>> = None;
        for row in rows {
            if row.slot_is_null(anchor.slot)? {
                continue;
            }
            if row.node_id_for_slot_if_bound(anchor.slot)?.is_some() {
                output.push(row);
                continue;
            }
            if anchor_ids.is_none() {
                anchor_ids = Some(self.graph_row_initial_node_ids(
                    query,
                    anchor,
                    GraphRowRuntimeGoal::AllRows,
                    policy_cutoffs,
                    followups,
                )?);
            }
            for node_id in anchor_ids.as_deref().unwrap_or_default() {
                let mut next = row.clone();
                next.bind_node(
                    anchor.slot,
                    crate::graph_row::GraphBoundNode::id_only(*node_id),
                )?;
                output.push(next);
                if output.len() > query.options.max_intermediate_bindings {
                    return Err(graph_row_cap_error(
                        "max_intermediate_bindings",
                        query.options.max_intermediate_bindings,
                    ));
                }
                if goal.reached(output.len()) {
                    return Ok(output);
                }
            }
        }
        Ok(output)
    }

    fn graph_row_partition_initial_bound_node_constraint_rows(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<
        (
            Vec<crate::graph_row::GraphBindingRow>,
            Vec<crate::graph_row::GraphBindingRow>,
        ),
        EngineError,
    > {
        if rows.is_empty() || query.initial_bound_slots.is_empty() {
            return Ok((rows, Vec::new()));
        }

        let initial_slots = query
            .initial_bound_slots
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let constrained_nodes = runtime
            .nodes
            .iter()
            .filter(|node| {
                initial_slots.contains(&node.slot) && graph_row_node_query_has_anchor(&node.query)
            })
            .collect::<Vec<_>>();
        if constrained_nodes.is_empty() {
            return Ok((rows, Vec::new()));
        }

        let mut verified_by_slot = BTreeMap::new();
        for node in constrained_nodes {
            let candidate_ids = graph_row_collect_node_ids(&rows, node.slot)?;
            let verified =
                self.graph_row_verified_bound_anchor_ids(&candidate_ids, node, policy_cutoffs)?;
            verified_by_slot.insert(node.slot, verified);
        }

        let mut valid = Vec::with_capacity(rows.len());
        let mut invalid = Vec::new();
        'rows: for row in rows {
            for (slot, verified_ids) in &verified_by_slot {
                let Some(node_id) = row.node_id_for_slot_if_bound(*slot)? else {
                    invalid.push(row);
                    continue 'rows;
                };
                if !verified_ids.contains(&node_id) {
                    invalid.push(row);
                    continue 'rows;
                }
            }
            valid.push(row);
        }
        Ok((valid, invalid))
    }

    fn graph_row_null_extend_initial_optional_miss_row(
        &self,
        query: &NormalizedGraphRowQuery,
        mut row: crate::graph_row::GraphBindingRow,
    ) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
        let initial_slots = query
            .initial_bound_slots
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for slot in query.binding_schema.slots() {
            let slot_ref = crate::graph_row::GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            };
            if slot.user_alias.is_some() && !initial_slots.contains(&slot_ref) {
                row.set_null(&query.binding_schema, slot_ref)?;
            }
        }
        Ok(row)
    }

    fn graph_row_verified_bound_anchor_ids(
        &self,
        candidate_ids: &[u64],
        anchor: &GraphRowRuntimeNode,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<NodeIdSet, EngineError> {
        let mut unique = candidate_ids.to_vec();
        unique.sort_unstable();
        unique.dedup();
        if unique.is_empty() {
            return Ok(NodeIdSet::default());
        }
        let include_key =
            !anchor.query.keys.is_empty() || node_filter_needs_key(&anchor.query.filter);
        let include_created_at = node_filter_needs_created_at(&anchor.query.filter);
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&anchor.query.filter, &mut property_keys);
        property_keys.sort();
        property_keys.dedup();
        let mut verified = Vec::with_capacity(unique.len());
        for chunk in unique.chunks(QUERY_VERIFY_CHUNK) {
            let _ = self.verify_node_candidate_chunk(
                chunk,
                &anchor.query,
                policy_cutoffs,
                include_key,
                include_created_at,
                &property_keys,
                &mut verified,
                usize::MAX,
            )?;
        }
        Ok(verified.into_iter().collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_optional_group(
        &self,
        query: &NormalizedGraphRowQuery,
        group: &GraphRowRuntimeOptionalGroup,
        left_rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        intermediate_peak: &mut usize,
        paths_enumerated: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        if left_rows.is_empty() {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_plan(
                    "OptionalApplyRuntime",
                    format!(
                        "piece_index={}; left_rows=0; hits=0; misses=0; output_rows=0; skipped_due_to_empty_frontier=true",
                        group.piece_index
                    ),
                );
            }
            return Ok(left_rows);
        }

        let group_physical_plan =
            self.plan_graph_row_physical(query, &group.runtime, query.options.include_plan)?;
        if group.dependency_slots.is_empty() {
            return self.graph_row_execute_uncorrelated_optional_group(
                query,
                group,
                &group_physical_plan,
                left_rows,
                effective_at_epoch,
                policy_cutoffs,
                followups,
                frontier_peak,
                intermediate_peak,
                paths_enumerated,
                explain_trace.as_deref_mut(),
            );
        }

        self.graph_row_execute_correlated_optional_group(
            query,
            group,
            &group_physical_plan,
            left_rows,
            effective_at_epoch,
            policy_cutoffs,
            followups,
            frontier_peak,
            intermediate_peak,
            paths_enumerated,
            explain_trace,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_uncorrelated_optional_group(
        &self,
        query: &NormalizedGraphRowQuery,
        group: &GraphRowRuntimeOptionalGroup,
        group_physical_plan: &GraphRowPhysicalPlan,
        left_rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        intermediate_peak: &mut usize,
        paths_enumerated: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let left_count = left_rows.len();
        let group_rows = self.graph_row_execute_runtime_plan(
            query,
            &group.runtime,
            group_physical_plan,
            Some(vec![query.binding_schema.empty_row()]),
            GraphRowRuntimeGoal::AllRows,
            effective_at_epoch,
            policy_cutoffs,
            followups,
            frontier_peak,
            intermediate_peak,
            paths_enumerated,
            explain_trace.as_deref_mut(),
        )?;
        let group_rows = self.graph_row_apply_optional_where(query, group, group_rows)?;
        let mut output = Vec::new();
        if group_rows.is_empty() {
            for row in left_rows {
                graph_row_push_optional_joined_row(
                    query,
                    &mut output,
                    self.graph_row_null_extend_optional_row(query, group, row)?,
                )?;
            }
        } else {
            for left in left_rows {
                for group_row in &group_rows {
                    let mut next = left.clone();
                    next.copy_slots_from(group_row, &group.introduced_slots)?;
                    graph_row_push_optional_joined_row(query, &mut output, next)?;
                }
            }
        }
        let hit_rows = if group_rows.is_empty() {
            0
        } else {
            left_count.saturating_mul(group_rows.len())
        };
        let miss_rows = if group_rows.is_empty() { left_count } else { 0 };
        if let Some(trace) = explain_trace {
            trace.record_plan(
                "OptionalApplyRuntime",
                format!(
                    "piece_index={}; correlated=false; left_rows={}; reusable_subplan_rows={}; hit_rows={}; miss_rows={}; output_rows={}; left_outer=true; full_scan_per_left_row=false; where_present={}; introduced_slots={}; dependency_slots={}; left_slots={}",
                    group.piece_index,
                    left_count,
                    group_rows.len(),
                    hit_rows,
                    miss_rows,
                    output.len(),
                    group.where_present,
                    graph_row_slot_list_detail(query, &group.introduced_slots),
                    graph_row_slot_list_detail(query, &group.dependency_slots),
                    graph_row_slot_list_detail(query, &group.left_slots)
                ),
            );
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_correlated_optional_group(
        &self,
        query: &NormalizedGraphRowQuery,
        group: &GraphRowRuntimeOptionalGroup,
        group_physical_plan: &GraphRowPhysicalPlan,
        left_rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        intermediate_peak: &mut usize,
        paths_enumerated: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let left_count = left_rows.len();
        let mut dependency_null_misses = Vec::new();
        let mut left_groups = Vec::<GraphRowOptionalLeftGroup>::new();
        let mut left_group_by_key: BTreeMap<Vec<crate::graph_row::GraphSortAtom>, usize> =
            BTreeMap::new();

        for row in left_rows {
            if graph_row_any_slot_null(&row, &group.dependency_slots)? {
                dependency_null_misses.push(row);
                continue;
            }
            let key = row.logical_sort_key_for_slots(&query.binding_schema, &group.dependency_slots)?;
            match left_group_by_key.get(&key).copied() {
                Some(index) => left_groups[index].rows.push(row),
                None => {
                    let index = left_groups.len();
                    left_group_by_key.insert(key.clone(), index);
                    left_groups.push(GraphRowOptionalLeftGroup {
                        key,
                        representative: row.clone(),
                        rows: vec![row],
                    });
                }
            }
        }

        let representatives = left_groups
            .iter()
            .map(|group| group.representative.clone())
            .collect::<Vec<_>>();
        let mut group_rows = if representatives.is_empty() {
            Vec::new()
        } else {
            self.graph_row_execute_runtime_plan(
                query,
                &group.runtime,
                group_physical_plan,
                Some(representatives),
                GraphRowRuntimeGoal::AllRows,
                effective_at_epoch,
                policy_cutoffs,
                followups,
                frontier_peak,
                intermediate_peak,
                paths_enumerated,
                explain_trace.as_deref_mut(),
            )?
        };
        group_rows = self.graph_row_apply_optional_where(query, group, group_rows)?;

        let mut hits_by_key: BTreeMap<Vec<crate::graph_row::GraphSortAtom>, Vec<crate::graph_row::GraphBindingRow>> =
            BTreeMap::new();
        for row in group_rows {
            let key = row.logical_sort_key_for_slots(&query.binding_schema, &group.dependency_slots)?;
            hits_by_key.entry(key).or_default().push(row);
        }

        let mut output = Vec::new();
        let mut hit_groups = 0usize;
        let mut missed_groups = 0usize;
        for left_group in left_groups {
            match hits_by_key.get(&left_group.key) {
                Some(hits) if !hits.is_empty() => {
                    hit_groups = hit_groups.saturating_add(1);
                    for left in left_group.rows {
                        for hit in hits {
                            let mut next = left.clone();
                            next.copy_slots_from(hit, &group.introduced_slots)?;
                            graph_row_push_optional_joined_row(query, &mut output, next)?;
                        }
                    }
                }
                _ => {
                    missed_groups = missed_groups.saturating_add(1);
                    for left in left_group.rows {
                        graph_row_push_optional_joined_row(
                            query,
                            &mut output,
                            self.graph_row_null_extend_optional_row(query, group, left)?,
                        )?;
                    }
                }
            }
        }
        let dependency_null_miss_rows = dependency_null_misses.len();
        for row in dependency_null_misses {
            missed_groups = missed_groups.saturating_add(1);
            graph_row_push_optional_joined_row(
                query,
                &mut output,
                self.graph_row_null_extend_optional_row(query, group, row)?,
            )?;
        }

        if let Some(trace) = explain_trace {
            trace.record_plan(
                "OptionalApplyRuntime",
                format!(
                    "piece_index={}; correlated=true; left_rows={left_count}; distinct_dependency_bindings={}; dependency_null_misses={}; hit_groups={hit_groups}; missed_groups={missed_groups}; output_rows={}; left_outer=true; batched_by_dependency_bindings=true; where_present={}; introduced_slots={}; dependency_slots={}; left_slots={}",
                    group.piece_index,
                    left_group_by_key.len(),
                    dependency_null_miss_rows,
                    output.len(),
                    group.where_present,
                    graph_row_slot_list_detail(query, &group.introduced_slots),
                    graph_row_slot_list_detail(query, &group.dependency_slots),
                    graph_row_slot_list_detail(query, &group.left_slots)
                ),
            );
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_variable_length(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        path: &GraphRowRuntimeVariableLength,
        left_rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        paths_enumerated: &mut usize,
        goal: GraphRowRuntimeGoal,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        if left_rows.is_empty() {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_plan(
                    "VariableLengthPathRuntime",
                    format!(
                        "piece_index={}; path={}; left_rows=0; output_rows=0; skipped_due_to_empty_frontier=true",
                        path.piece_index,
                        graph_row_vlp_context(path)
                    ),
                );
            }
            return Ok(left_rows);
        }

        let left_count = left_rows.len();
        let rows = left_rows
            .into_iter()
            .filter_map(|row| {
                match (
                    row.slot_is_null(path.from_slot),
                    row.slot_is_null(path.to_slot),
                ) {
                    (Ok(false), Ok(false)) => Some(Ok(row)),
                    (Ok(_), Ok(_)) => None,
                    (Err(err), _) | (_, Err(err)) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        if rows.is_empty() {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_plan(
                    "VariableLengthPathRuntime",
                    format!(
                        "piece_index={}; path={}; output_rows=0; skipped_due_to_null_required_endpoint=true",
                        path.piece_index,
                        graph_row_vlp_context(path)
                    ),
                );
            }
            return Ok(rows);
        }

        if path.min_hops == 1 && path.max_hops == 1 {
            return self.graph_row_execute_one_hop_variable_length(
                query,
                runtime,
                path,
                rows,
                effective_at_epoch,
                policy_cutoffs,
                followups,
                frontier_peak,
                paths_enumerated,
                goal,
                explain_trace.as_deref_mut(),
            );
        }

        let from_node = graph_row_runtime_node(runtime, &path.from_alias)?;
        let to_node = graph_row_runtime_node(runtime, &path.to_alias)?;
        let mut output = Vec::new();
        let mut starts_considered = 0usize;
        let mut search_cache: BTreeMap<GraphRowVlpSearchKey, GraphRowVlpSearchResult> =
            BTreeMap::new();
        let mut search_cache_hits = 0usize;

        for row in rows {
            let key = GraphRowVlpSearchKey {
                bound_from: row.node_id_for_slot_if_bound(path.from_slot)?,
                bound_to: row.node_id_for_slot_if_bound(path.to_slot)?,
            };
            if let std::collections::btree_map::Entry::Vacant(e) = search_cache.entry(key) {
                let mut per_start_counts: NodeIdMap<usize> = NodeIdMap::default();
                let seeds = self.graph_row_vlp_seeds(
                    query,
                    path,
                    from_node,
                    to_node,
                    &row,
                    policy_cutoffs,
                    followups,
                    frontier_peak,
                )?;
                let seed_count = seeds.len();
                let mut paths = Vec::new();
                for seed in seeds {
                    let seed_paths = self.graph_row_enumerate_vlp_paths_for_seed(
                        query,
                        path,
                        from_node,
                        to_node,
                        &row,
                        seed,
                        effective_at_epoch,
                        policy_cutoffs,
                        frontier_peak,
                        &mut per_start_counts,
                    )?;
                    paths.extend(seed_paths);
                }
                *paths_enumerated = paths_enumerated.saturating_add(paths.len());
                e.insert(GraphRowVlpSearchResult { seed_count, paths });
            } else {
                search_cache_hits = search_cache_hits.saturating_add(1);
            }

            let Some(search_result) = search_cache.get(&key) else {
                continue;
            };
            starts_considered = starts_considered.saturating_add(search_result.seed_count);
            let mut per_start_counts: NodeIdMap<usize> = NodeIdMap::default();
            for graph_path in &search_result.paths {
                let Some(&logical_start) = graph_path.nodes.first() else {
                    continue;
                };
                let count = per_start_counts.entry(logical_start).or_default();
                if *count >= query.options.max_paths_per_start {
                    return Err(graph_row_vlp_cap_error(
                        "max_paths_per_start",
                        query.options.max_paths_per_start,
                        path,
                    ));
                }
                *count += 1;
                self.graph_row_push_vlp_path_row(
                    query,
                    path,
                    &row,
                    graph_path.clone(),
                    &mut output,
                )?;
                if goal.reached(output.len()) {
                    break;
                }
            }
            if goal.reached(output.len()) {
                break;
            }
        }

        if let Some(trace) = explain_trace {
            trace.record_plan(
                "VariableLengthPathRuntime",
                format!(
                    "piece_index={}; path={}; left_rows={}; starts_considered={}; distinct_search_groups={}; search_cache_hits={}; output_rows={}; min_hops={}; max_hops={}; direction={:?}; relationship_simple=true; max_frontier={}; max_paths_per_start={}; source_verification=latest_visible_edges_and_endpoints",
                    path.piece_index,
                    graph_row_vlp_context(path),
                    left_count,
                    starts_considered,
                    search_cache.len(),
                    search_cache_hits,
                    output.len(),
                    path.min_hops,
                    path.max_hops,
                    path.direction,
                    query.options.max_frontier,
                    query.options.max_paths_per_start
                ),
            );
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_one_hop_variable_length(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        path: &GraphRowRuntimeVariableLength,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        paths_enumerated: &mut usize,
        goal: GraphRowRuntimeGoal,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let temp_edge = GraphRowRuntimeEdge {
            alias: path.edge_alias.clone(),
            edge_slot: path.edge_slot,
            hidden_slot: None,
            from_alias: path.from_alias.clone(),
            to_alias: path.to_alias.clone(),
            from_slot: path.from_slot,
            to_slot: path.to_slot,
            direction: path.direction,
            candidate_edge_ids: path.candidate_edge_ids.clone(),
            label_filter_ids: path.label_filter_ids.clone(),
            filter: path.filter.clone(),
            warnings: path.warnings.clone(),
        };
        let candidates = self.graph_row_fixed_edge_candidates(
            query,
            &temp_edge,
            None,
            &rows,
            effective_at_epoch,
            policy_cutoffs,
            followups,
            frontier_peak,
            None,
            Some(path),
        )?;
        if candidates.is_empty() {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_plan(
                    "VariableLengthPathRuntime",
                    format!(
                        "piece_index={}; path={}; one_hop_fixed_equivalent=true; output_rows=0",
                        path.piece_index,
                        graph_row_vlp_context(path)
                    ),
                );
            }
            return Ok(Vec::new());
        }

        let from_node = graph_row_runtime_node(runtime, &path.from_alias)?;
        let to_node = graph_row_runtime_node(runtime, &path.to_alias)?;
        let mut from_candidates = Vec::with_capacity(candidates.len());
        let mut to_candidates = Vec::with_capacity(candidates.len());
        for candidate in &candidates {
            from_candidates.push(candidate.logical_from);
            to_candidates.push(candidate.logical_to);
        }
        let verified_from =
            self.graph_row_verified_node_ids(from_node, from_candidates, policy_cutoffs)?;
        let verified_to = if from_node.alias == to_node.alias {
            verified_from.clone()
        } else {
            self.graph_row_verified_node_ids(to_node, to_candidates, policy_cutoffs)?
        };

        let buckets = GraphRowEdgeCandidateBuckets::new(&candidates);
        let mut output = Vec::new();
        for row in rows {
            let mut per_start_counts: NodeIdMap<usize> = NodeIdMap::default();
            let bound_from = row.node_id_for_slot_if_bound(path.from_slot)?;
            let bound_to = row.node_id_for_slot_if_bound(path.to_slot)?;
            let all_indices;
            let indices = match buckets.indices_for(bound_from, bound_to) {
                Some(indices) => indices,
                None if bound_from.is_none() && bound_to.is_none() => {
                    all_indices = (0..candidates.len()).collect::<Vec<_>>();
                    all_indices.as_slice()
                }
                None => continue,
            };
            for &candidate_index in indices {
                let candidate = &candidates[candidate_index];
                if bound_from.is_some_and(|node_id| node_id != candidate.logical_from)
                    || bound_to.is_some_and(|node_id| node_id != candidate.logical_to)
                {
                    continue;
                }
                if bound_from.is_none() && !verified_from.contains(&candidate.logical_from) {
                    continue;
                }
                if bound_to.is_none() && !verified_to.contains(&candidate.logical_to) {
                    continue;
                }
                let count = per_start_counts.entry(candidate.logical_from).or_default();
                if *count >= query.options.max_paths_per_start {
                    return Err(graph_row_vlp_cap_error(
                        "max_paths_per_start",
                        query.options.max_paths_per_start,
                        path,
                    ));
                }
                *count += 1;
                let graph_path = GraphPath {
                    nodes: vec![candidate.logical_from, candidate.logical_to],
                    edges: vec![candidate.meta.id],
                };
                self.graph_row_push_vlp_path_row(query, path, &row, graph_path, &mut output)?;
                if goal.reached(output.len()) {
                    break;
                }
            }
            if goal.reached(output.len()) {
                break;
            }
        }
        *paths_enumerated = paths_enumerated.saturating_add(output.len());
        if let Some(trace) = explain_trace {
            trace.record_plan(
                "VariableLengthPathRuntime",
                format!(
                    "piece_index={}; path={}; one_hop_fixed_equivalent=true; output_rows={}; edge_alias={}; path_alias={}; direction={:?}; source_verification=latest_visible_edges_and_endpoints",
                    path.piece_index,
                    graph_row_vlp_context(path),
                    output.len(),
                    path.edge_alias.is_some(),
                    path.path_alias.is_some(),
                    path.direction
                ),
            );
        }
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_vlp_seeds(
        &self,
        query: &NormalizedGraphRowQuery,
        path: &GraphRowRuntimeVariableLength,
        from_node: &GraphRowRuntimeNode,
        to_node: &GraphRowRuntimeNode,
        row: &crate::graph_row::GraphBindingRow,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
    ) -> Result<Vec<GraphRowPathSearchSeed>, EngineError> {
        let bound_from = row.node_id_for_slot_if_bound(path.from_slot)?;
        let bound_to = row.node_id_for_slot_if_bound(path.to_slot)?;
        if let Some(node_id) = bound_from {
            return Ok(vec![GraphRowPathSearchSeed {
                node_id,
                reverse: false,
            }]);
        }
        if let Some(node_id) = bound_to {
            return Ok(vec![GraphRowPathSearchSeed {
                node_id,
                reverse: true,
            }]);
        }

        let from_legal = graph_row_node_query_has_anchor(&from_node.query)
            || query.options.allow_full_scan;
        let to_legal =
            graph_row_node_query_has_anchor(&to_node.query) || query.options.allow_full_scan;

        let from_ids = if from_legal {
            Some(self.graph_row_vlp_node_candidate_ids(
                query,
                path,
                from_node,
                policy_cutoffs,
                followups,
                frontier_peak,
            ))
        } else {
            None
        };
        let to_ids = if to_legal {
            Some(self.graph_row_vlp_node_candidate_ids(
                query,
                path,
                to_node,
                policy_cutoffs,
                followups,
                frontier_peak,
            ))
        } else {
            None
        };

        match (from_ids, to_ids) {
            (Some(Ok(from_ids)), Some(Ok(to_ids))) if to_ids.len() < from_ids.len() => {
                Ok(to_ids
                    .into_iter()
                    .map(|node_id| GraphRowPathSearchSeed {
                        node_id,
                        reverse: true,
                    })
                    .collect())
            }
            (Some(Ok(from_ids)), _) => Ok(from_ids
                .into_iter()
                .map(|node_id| GraphRowPathSearchSeed {
                    node_id,
                    reverse: false,
                })
                .collect()),
            (Some(Err(_)), Some(Ok(to_ids))) => Ok(to_ids
                .into_iter()
                .map(|node_id| GraphRowPathSearchSeed {
                    node_id,
                    reverse: true,
                })
                .collect()),
            (Some(Err(err)), Some(Err(_))) | (Some(Err(err)), None) => Err(err),
            (None, Some(Ok(to_ids))) => Ok(to_ids
                .into_iter()
                .map(|node_id| GraphRowPathSearchSeed {
                    node_id,
                    reverse: true,
                })
                .collect()),
            (None, Some(Err(err))) => Err(err),
            (None, None) => Err(EngineError::InvalidOperation(
                "graph row variable-length pattern requires an anchor or allow_full_scan=true"
                    .to_string(),
            )),
        }
    }

    fn graph_row_vlp_node_candidate_ids(
        &self,
        query: &NormalizedGraphRowQuery,
        path: &GraphRowRuntimeVariableLength,
        node: &GraphRowRuntimeNode,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
    ) -> Result<Vec<u64>, EngineError> {
        let cap = query.options.max_frontier;
        let mut node_query = node.query.clone();
        node_query.page.limit = Some(cap.saturating_add(1));
        let planned = self.plan_normalized_node_query(&node_query)?;
        let (page, mut node_followups) =
            self.query_node_page_planned(&node_query, planned, false, policy_cutoffs)?;
        followups.append(&mut node_followups);
        if page.next_cursor.is_some() || page.ids.len() > cap {
            return Err(graph_row_vlp_cap_error("max_frontier", cap, path));
        }
        graph_row_record_frontier_cap_peak(frontier_peak, page.ids.len(), cap, Some(path))?;
        Ok(page.ids)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_enumerate_vlp_paths_for_seed(
        &self,
        query: &NormalizedGraphRowQuery,
        path: &GraphRowRuntimeVariableLength,
        from_node: &GraphRowRuntimeNode,
        to_node: &GraphRowRuntimeNode,
        row: &crate::graph_row::GraphBindingRow,
        seed: GraphRowPathSearchSeed,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        frontier_peak: &mut usize,
        per_start_counts: &mut NodeIdMap<usize>,
    ) -> Result<Vec<GraphPath>, EngineError> {
        let target_bound = if seed.reverse {
            row.node_id_for_slot_if_bound(path.from_slot)?
        } else {
            row.node_id_for_slot_if_bound(path.to_slot)?
        };
        let endpoint_node = if seed.reverse { from_node } else { to_node };
        let mut frontier = vec![GraphRowPartialPath {
            current: seed.node_id,
            nodes: vec![seed.node_id],
            edges: Vec::new(),
        }];
        graph_row_record_frontier_cap_peak(
            frontier_peak,
            frontier.len(),
            query.options.max_frontier,
            Some(path),
        )?;

        let mut accepted = Vec::new();
        for depth in 0..=path.max_hops {
            if depth >= path.min_hops {
                let mut candidates = Vec::new();
                for partial in &frontier {
                    if target_bound.is_some_and(|target| target != partial.current) {
                        continue;
                    }
                    candidates.push(graph_row_materialize_partial_path(partial, seed.reverse));
                }
                let candidates = self.graph_row_filter_vlp_endpoint_paths(
                    candidates,
                    endpoint_node,
                    seed.reverse,
                    policy_cutoffs,
                )?;
                for graph_path in candidates {
                    let Some(&logical_start) = graph_path.nodes.first() else {
                        continue;
                    };
                    let count = per_start_counts.entry(logical_start).or_default();
                    if *count >= query.options.max_paths_per_start {
                        return Err(graph_row_vlp_cap_error(
                            "max_paths_per_start",
                            query.options.max_paths_per_start,
                            path,
                        ));
                    }
                    *count += 1;
                    accepted.push(graph_path);
                }
            }
            if depth == path.max_hops {
                break;
            }

            let frontier_nodes = frontier
                .iter()
                .map(|partial| partial.current)
                .collect::<Vec<_>>();
            let step_edges = self.graph_row_vlp_step_edges(
                query,
                path,
                seed.reverse,
                frontier_nodes,
                effective_at_epoch,
                policy_cutoffs,
                frontier_peak,
            )?;
            if step_edges.is_empty() {
                break;
            }
            let mut next_frontier = Vec::new();
            for partial in &frontier {
                let Some(edges) = step_edges.get(&partial.current) else {
                    continue;
                };
                for edge in edges {
                    if partial.edges.contains(&edge.edge_id) {
                        continue;
                    }
                    if next_frontier.len() >= query.options.max_frontier {
                        return Err(graph_row_vlp_cap_error(
                            "max_frontier",
                            query.options.max_frontier,
                            path,
                        ));
                    }
                    let mut next_nodes = partial.nodes.clone();
                    next_nodes.push(edge.next_node);
                    let mut next_edges = partial.edges.clone();
                    next_edges.push(edge.edge_id);
                    next_frontier.push(GraphRowPartialPath {
                        current: edge.next_node,
                        nodes: next_nodes,
                        edges: next_edges,
                    });
                }
            }
            next_frontier.sort_by(|left, right| {
                left.nodes
                    .cmp(&right.nodes)
                    .then_with(|| left.edges.cmp(&right.edges))
            });
            graph_row_record_frontier_cap_peak(
                frontier_peak,
                next_frontier.len(),
                query.options.max_frontier,
                Some(path),
            )?;
            frontier = next_frontier;
        }

        accepted.sort_by(|left, right| {
            left.edges
                .len()
                .cmp(&right.edges.len())
                .then_with(|| left.nodes.cmp(&right.nodes))
                .then_with(|| left.edges.cmp(&right.edges))
        });
        Ok(accepted)
    }

    fn graph_row_filter_vlp_endpoint_paths(
        &self,
        paths: Vec<GraphPath>,
        endpoint_node: &GraphRowRuntimeNode,
        use_start_node: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<GraphPath>, EngineError> {
        if paths.is_empty() {
            return Ok(paths);
        }
        let candidate_ids = paths
            .iter()
            .filter_map(|path| {
                if use_start_node {
                    path.nodes.first().copied()
                } else {
                    path.nodes.last().copied()
                }
            })
            .collect::<Vec<_>>();
        let verified =
            self.graph_row_verified_node_ids(endpoint_node, candidate_ids, policy_cutoffs)?;
        Ok(paths
            .into_iter()
            .filter(|path| {
                let endpoint = if use_start_node {
                    path.nodes.first()
                } else {
                    path.nodes.last()
                };
                endpoint.is_some_and(|node_id| verified.contains(node_id))
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_vlp_step_edges(
        &self,
        query: &NormalizedGraphRowQuery,
        path: &GraphRowRuntimeVariableLength,
        reverse: bool,
        mut frontier_nodes: Vec<u64>,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        frontier_peak: &mut usize,
    ) -> Result<NodeIdMap<Vec<GraphRowVlpStepEdge>>, EngineError> {
        frontier_nodes.sort_unstable();
        frontier_nodes.dedup();
        graph_row_record_frontier_cap_peak(
            frontier_peak,
            frontier_nodes.len(),
            query.options.max_frontier,
            Some(path),
        )?;
        if frontier_nodes.is_empty()
            || path.filter.is_always_false()
            || path
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.is_empty())
        {
            return Ok(NodeIdMap::default());
        }

        let direction = if reverse {
            graph_row_reverse_direction(path.direction)
        } else {
            path.direction
        };
        let mut candidate_ids = path.candidate_edge_ids.clone();
        if candidate_ids.is_empty() {
            self.graph_row_collect_endpoint_edge_ids(
                &mut candidate_ids,
                match direction {
                    Direction::Outgoing => frontier_nodes.clone(),
                    _ => Vec::new(),
                },
                match direction {
                    Direction::Incoming => frontier_nodes.clone(),
                    _ => Vec::new(),
                },
                match direction {
                    Direction::Both => frontier_nodes.clone(),
                    _ => Vec::new(),
                },
                path.label_filter_ids.as_deref(),
                query.options.max_frontier,
                frontier_peak,
                Some(path),
            )?;
        } else {
            candidate_ids.sort_unstable();
            candidate_ids.dedup();
            graph_row_record_frontier_cap_peak(
                frontier_peak,
                candidate_ids.len(),
                query.options.max_frontier,
                Some(path),
            )?;
        }

        let verified = self.graph_row_verify_edge_candidate_ids(
            &candidate_ids,
            path.label_filter_ids.as_deref(),
            &path.filter,
            effective_at_epoch,
            policy_cutoffs,
        )?;
        let frontier_set: NodeIdSet = frontier_nodes.into_iter().collect();
        let mut by_source: NodeIdMap<Vec<GraphRowVlpStepEdge>> = NodeIdMap::default();
        for meta in verified {
            match direction {
                Direction::Outgoing => {
                    if frontier_set.contains(&meta.from) {
                        by_source.entry(meta.from).or_default().push(GraphRowVlpStepEdge {
                            edge_id: meta.id,
                            next_node: meta.to,
                        });
                    }
                }
                Direction::Incoming => {
                    if frontier_set.contains(&meta.to) {
                        by_source.entry(meta.to).or_default().push(GraphRowVlpStepEdge {
                            edge_id: meta.id,
                            next_node: meta.from,
                        });
                    }
                }
                Direction::Both => {
                    if frontier_set.contains(&meta.from) {
                        by_source.entry(meta.from).or_default().push(GraphRowVlpStepEdge {
                            edge_id: meta.id,
                            next_node: meta.to,
                        });
                    }
                    if meta.to != meta.from && frontier_set.contains(&meta.to) {
                        by_source.entry(meta.to).or_default().push(GraphRowVlpStepEdge {
                            edge_id: meta.id,
                            next_node: meta.from,
                        });
                    }
                }
            }
        }
        for edges in by_source.values_mut() {
            edges.sort_by_key(|edge| (edge.next_node, edge.edge_id));
        }
        Ok(by_source)
    }

    fn graph_row_push_vlp_path_row(
        &self,
        query: &NormalizedGraphRowQuery,
        path: &GraphRowRuntimeVariableLength,
        row: &crate::graph_row::GraphBindingRow,
        graph_path: GraphPath,
        output: &mut Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<(), EngineError> {
        if output.len() >= query.options.max_intermediate_bindings {
            return Err(graph_row_vlp_cap_error(
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
                path,
            ));
        }
        let Some(&logical_from) = graph_path.nodes.first() else {
            return Ok(());
        };
        let Some(&logical_to) = graph_path.nodes.last() else {
            return Ok(());
        };
        let mut next = row.clone();
        if next
            .bind_node(
                path.from_slot,
                crate::graph_row::GraphBoundNode::id_only(logical_from),
            )
            .is_err()
        {
            return Ok(());
        }
        if next
            .bind_node(
                path.to_slot,
                crate::graph_row::GraphBoundNode::id_only(logical_to),
            )
            .is_err()
        {
            return Ok(());
        }
        if let Some(edge_slot) = path.edge_slot {
            let Some(&edge_id) = graph_path.edges.first() else {
                return Ok(());
            };
            next.bind_edge(edge_slot, crate::graph_row::GraphBoundEdge::id_only(edge_id))?;
        }
        if let Some(path_slot) = path.path_slot {
            next.bind_path(
                path_slot,
                crate::graph_row::GraphBoundPath::id_only(graph_path.clone())?,
            )?;
        }
        if let Some(hidden_slot) = path.hidden_slot {
            next.bind_hidden(
                hidden_slot,
                crate::graph_row::GraphHiddenOccurrence::Path(graph_path),
            )?;
        }
        output.push(next);
        Ok(())
    }

    fn graph_row_apply_optional_where(
        &self,
        query: &NormalizedGraphRowQuery,
        group: &GraphRowRuntimeOptionalGroup,
        mut rows: Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let Some(where_expr) = group.where_expr.as_ref() else {
            return Ok(rows);
        };
        self.hydrate_graph_rows_for_needs(
            &mut rows,
            &query.binding_schema,
            &group.where_needs,
        )?;
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows {
            let context = crate::graph_row::BoundGraphEvalContext { row: &row };
            if crate::graph_row::eval_bound_graph_predicate(where_expr, &context)? {
                filtered.push(row);
            }
        }
        Ok(filtered)
    }

    fn graph_row_null_extend_optional_row(
        &self,
        query: &NormalizedGraphRowQuery,
        group: &GraphRowRuntimeOptionalGroup,
        mut row: crate::graph_row::GraphBindingRow,
    ) -> Result<crate::graph_row::GraphBindingRow, EngineError> {
        for slot in &group.introduced_slots {
            row.set_null(&query.binding_schema, *slot)?;
        }
        Ok(row)
    }

    fn graph_row_initial_rows(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        initial_driver: &GraphRowInitialDriver,
        goal: GraphRowRuntimeGoal,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        let GraphRowInitialDriver::Node { node_index, .. } = initial_driver else {
            return Ok(vec![query.binding_schema.empty_row()]);
        };
        let anchor = runtime.nodes.get(*node_index).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row physical plan references missing node index {node_index}"
            ))
        })?;
        let ids = self.graph_row_initial_node_ids(query, anchor, goal, policy_cutoffs, followups)?;

        let mut rows = Vec::with_capacity(ids.len());
        for node_id in ids {
            let mut row = query.binding_schema.empty_row();
            row.bind_node(anchor.slot, crate::graph_row::GraphBoundNode::id_only(node_id))?;
            rows.push(row);
            if goal.reached(rows.len()) {
                break;
            }
        }
        Ok(rows)
    }

    fn graph_row_initial_node_ids(
        &self,
        query: &NormalizedGraphRowQuery,
        anchor: &GraphRowRuntimeNode,
        goal: GraphRowRuntimeGoal,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<Vec<u64>, EngineError> {
        let mut anchor_query = anchor.query.clone();
        anchor_query.page.limit = Some(if goal.is_exists_one() {
            1
        } else {
            query.options.max_intermediate_bindings.saturating_add(1)
        });
        let planned = self.plan_normalized_node_query(&anchor_query)?;
        let (page, mut node_followups) =
            self.query_node_page_planned(&anchor_query, planned, false, policy_cutoffs)?;
        followups.append(&mut node_followups);
        if !goal.is_exists_one()
            && (page.next_cursor.is_some() || page.ids.len() > query.options.max_intermediate_bindings)
        {
            return Err(graph_row_cap_error(
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
            ));
        }
        Ok(page.ids)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_expand_fixed_edge(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        edge: &GraphRowRuntimeEdge,
        planned_source_choice: Option<GraphRowEdgeCandidateSourceChoice>,
        rows: Vec<crate::graph_row::GraphBindingRow>,
        goal: GraphRowRuntimeGoal,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<Vec<crate::graph_row::GraphBindingRow>, EngineError> {
        if rows.is_empty() {
            if let Some(trace) = explain_trace {
                let planned_driver =
                    graph_row_source_choice_label(planned_source_choice.unwrap_or_else(|| {
                        graph_row_deterministic_fallback_edge_source_choice(edge, None)
                    }));
                trace.record_runtime_edge_source(
                    edge,
                    GraphRowEdgeCandidateSourceChoice::SkippedEmptyFrontier,
                    format!("planned_driver={planned_driver}; materialized_source=none; fallback_source=none; skipped_due_to_empty_frontier=true; subset_intersection_source_materialized=none"),
                    0,
                );
            }
            return Ok(rows);
        }
        let rows = rows
            .into_iter()
            .filter_map(|row| {
                match (
                    row.slot_is_null(edge.from_slot),
                    row.slot_is_null(edge.to_slot),
                ) {
                    (Ok(false), Ok(false)) => Some(Ok(row)),
                    (Ok(_), Ok(_)) => None,
                    (Err(err), _) | (_, Err(err)) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        if rows.is_empty() {
            if let Some(trace) = explain_trace {
                let planned_driver =
                    graph_row_source_choice_label(planned_source_choice.unwrap_or_else(|| {
                        graph_row_deterministic_fallback_edge_source_choice(edge, None)
                    }));
                trace.record_runtime_edge_source(
                    edge,
                    GraphRowEdgeCandidateSourceChoice::SkippedEmptyFrontier,
                    format!("planned_driver={planned_driver}; materialized_source=none; fallback_source=none; skipped_due_to_null_required_endpoint=true; subset_intersection_source_materialized=none"),
                    0,
                );
            }
            return Ok(rows);
        }

        let candidates = self.graph_row_fixed_edge_candidates(
            query,
            edge,
            planned_source_choice,
            &rows,
            effective_at_epoch,
            policy_cutoffs,
            followups,
            frontier_peak,
            explain_trace,
            None,
        )?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let from_node = &runtime.nodes[*runtime.node_by_alias.get(&edge.from_alias).ok_or_else(
            || {
                EngineError::InvalidOperation(format!(
                    "graph row edge references missing node alias '{}'",
                    edge.from_alias
                ))
            },
        )?];
        let to_node = &runtime.nodes[*runtime.node_by_alias.get(&edge.to_alias).ok_or_else(
            || {
                EngineError::InvalidOperation(format!(
                    "graph row edge references missing node alias '{}'",
                    edge.to_alias
                ))
            },
        )?];

        let mut from_candidates = Vec::new();
        let mut to_candidates = Vec::new();
        for candidate in &candidates {
            from_candidates.push(candidate.logical_from);
            to_candidates.push(candidate.logical_to);
        }
        let verified_from =
            self.graph_row_verified_node_ids(from_node, from_candidates, policy_cutoffs)?;
        let verified_to = if from_node.alias == to_node.alias {
            verified_from.clone()
        } else {
            self.graph_row_verified_node_ids(to_node, to_candidates, policy_cutoffs)?
        };

        let buckets = GraphRowEdgeCandidateBuckets::new(&candidates);
        let mut next_rows = Vec::new();
        for row in rows {
            let bound_from = row.node_id_for_slot_if_bound(edge.from_slot)?;
            let bound_to = row.node_id_for_slot_if_bound(edge.to_slot)?;
            if bound_from.is_some() || bound_to.is_some() {
                let Some(indices) = buckets.indices_for(bound_from, bound_to) else {
                    continue;
                };
                for &candidate_index in indices {
                    let candidate = &candidates[candidate_index];
                    self.graph_row_push_edge_candidate_row(
                        query,
                        edge,
                        &verified_from,
                        &verified_to,
                        bound_from,
                        bound_to,
                        &row,
                        candidate,
                        &mut next_rows,
                    )?;
                    if goal.reached(next_rows.len()) {
                        return Ok(next_rows);
                    }
                }
            } else {
                for candidate in &candidates {
                    self.graph_row_push_edge_candidate_row(
                        query,
                        edge,
                        &verified_from,
                        &verified_to,
                        bound_from,
                        bound_to,
                        &row,
                        candidate,
                        &mut next_rows,
                    )?;
                    if goal.reached(next_rows.len()) {
                        return Ok(next_rows);
                    }
                }
            }
        }
        Ok(next_rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_push_edge_candidate_row(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        verified_from: &NodeIdSet,
        verified_to: &NodeIdSet,
        bound_from: Option<u64>,
        bound_to: Option<u64>,
        row: &crate::graph_row::GraphBindingRow,
        candidate: &GraphRowOrientedEdge,
        next_rows: &mut Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<(), EngineError> {
        if bound_from.is_some_and(|node_id| node_id != candidate.logical_from)
            || bound_to.is_some_and(|node_id| node_id != candidate.logical_to)
        {
            return Ok(());
        }
        if bound_from.is_none() && !verified_from.contains(&candidate.logical_from) {
            return Ok(());
        }
        if bound_to.is_none() && !verified_to.contains(&candidate.logical_to) {
            return Ok(());
        }

        let mut next = row.clone();
        if next
            .bind_node(
                edge.from_slot,
                crate::graph_row::GraphBoundNode::id_only(candidate.logical_from),
            )
            .is_err()
        {
            return Ok(());
        }
        if next
            .bind_node(
                edge.to_slot,
                crate::graph_row::GraphBoundNode::id_only(candidate.logical_to),
            )
            .is_err()
        {
            return Ok(());
        }
        if let Some(edge_slot) = edge.edge_slot {
            next.bind_edge(
                edge_slot,
                crate::graph_row::GraphBoundEdge::id_only(candidate.meta.id),
            )?;
        }
        if let Some(hidden_slot) = edge.hidden_slot {
            next.bind_hidden(
                hidden_slot,
                crate::graph_row::GraphHiddenOccurrence::Edge(candidate.meta.id),
            )?;
        }
        next_rows.push(next);
        if next_rows.len() > query.options.max_intermediate_bindings {
            return Err(graph_row_cap_error(
                "max_intermediate_bindings",
                query.options.max_intermediate_bindings,
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_fixed_edge_candidates(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        planned_source_choice: Option<GraphRowEdgeCandidateSourceChoice>,
        rows: &[crate::graph_row::GraphBindingRow],
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
        frontier_peak: &mut usize,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
        vlp_cap_context: Option<&GraphRowRuntimeVariableLength>,
    ) -> Result<Vec<GraphRowOrientedEdge>, EngineError> {
        let mut has_bound_endpoint = false;
        let mut has_unbound_endpoint_pair = false;
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        let mut both = Vec::new();

        for row in rows {
            let from = row.node_id_for_slot_if_bound(edge.from_slot)?;
            let to = row.node_id_for_slot_if_bound(edge.to_slot)?;
            match (from, to) {
                (None, None) => has_unbound_endpoint_pair = true,
                _ => has_bound_endpoint = true,
            }
            graph_row_collect_endpoint_sources(
                edge.direction,
                true,
                from,
                &mut outgoing,
                &mut incoming,
                &mut both,
            );
            graph_row_collect_endpoint_sources(
                edge.direction,
                false,
                to,
                &mut outgoing,
                &mut incoming,
                &mut both,
            );
        }

        let mut candidate_ids = Vec::new();
        let planned_driver =
            graph_row_source_choice_label(planned_source_choice.unwrap_or_else(|| {
                graph_row_deterministic_fallback_edge_source_choice(edge, None)
            }));
        if !edge.candidate_edge_ids.is_empty() {
            candidate_ids.extend_from_slice(&edge.candidate_edge_ids);
            candidate_ids.sort_unstable();
            candidate_ids.dedup();
            graph_row_record_frontier_cap_peak(
                frontier_peak,
                candidate_ids.len(),
                query.options.max_frontier,
                vlp_cap_context,
            )?;
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_runtime_edge_source(
                    edge,
                    GraphRowEdgeCandidateSourceChoice::ExplicitIds,
                    format!("planned_driver={planned_driver}; materialized_source=ExplicitEdgeIds; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none"),
                    candidate_ids.len(),
                );
            }
        } else if edge.filter.is_always_false()
            || edge
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.is_empty())
        {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.record_runtime_edge_source(
                    edge,
                    GraphRowEdgeCandidateSourceChoice::EmptyResult,
                    format!("planned_driver={planned_driver}; materialized_source=EmptyResult; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none"),
                    0,
                );
            }
            return Ok(Vec::new());
        } else if has_bound_endpoint && !has_unbound_endpoint_pair {
            let choice = self.graph_row_choose_bound_edge_source(
                query,
                edge,
                &outgoing,
                &incoming,
                &both,
            )?;
            match choice {
                GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource => {
                    match self.graph_row_materialize_edge_candidate_source(
                        query,
                        edge,
                        policy_cutoffs,
                        frontier_peak,
                        vlp_cap_context,
                    )? {
                        GraphRowEdgeSourceRead::Ready {
                            ids,
                            followups: mut source_followups,
                            materialized_source,
                            subset_source,
                        } => {
                            followups.append(&mut source_followups);
                            candidate_ids = ids;
                            if let Some(trace) = explain_trace.as_deref_mut() {
                                trace.record_runtime_edge_source(
                                    edge,
                                    choice,
                                    format!(
                                        "planned_driver={planned_driver}; materialized_source={materialized_source}; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized={}",
                                        subset_source.as_deref().unwrap_or("none")
                                    ),
                                    candidate_ids.len(),
                                );
                            }
                        }
                        GraphRowEdgeSourceRead::TooBroad {
                            followups: mut source_followups,
                            planned_source,
                        } => {
                            followups.append(&mut source_followups);
                            self.graph_row_collect_endpoint_edge_ids(
                                &mut candidate_ids,
                                outgoing,
                                incoming,
                                both,
                                edge.label_filter_ids.as_deref(),
                                query.options.max_frontier,
                                frontier_peak,
                                vlp_cap_context,
                            )?;
                            if let Some(trace) = explain_trace.as_deref_mut() {
                                trace.record_runtime_edge_source(
                                    edge,
                                    choice,
                                    format!(
                                        "planned_driver={planned_driver}; materialized_source={planned_source}; fallback_source=EndpointAdjacency; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=too_broad"
                                    ),
                                    candidate_ids.len(),
                                );
                            }
                        }
                        GraphRowEdgeSourceRead::NoLegalSource => {
                            self.graph_row_collect_endpoint_edge_ids(
                                &mut candidate_ids,
                                outgoing,
                                incoming,
                                both,
                                edge.label_filter_ids.as_deref(),
                                query.options.max_frontier,
                                frontier_peak,
                                vlp_cap_context,
                            )?;
                            if let Some(trace) = explain_trace.as_deref_mut() {
                                trace.record_runtime_edge_source(
                                    edge,
                                    choice,
                                    format!("planned_driver={planned_driver}; materialized_source=none; fallback_source=EndpointAdjacency; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none"),
                                    candidate_ids.len(),
                                );
                            }
                        }
                    }
                }
                _ => {
                    let empty_frontier = outgoing.is_empty() && incoming.is_empty() && both.is_empty();
                    self.graph_row_collect_endpoint_edge_ids(
                        &mut candidate_ids,
                        outgoing,
                        incoming,
                        both,
                        edge.label_filter_ids.as_deref(),
                        query.options.max_frontier,
                        frontier_peak,
                        vlp_cap_context,
                    )?;
                    if let Some(trace) = explain_trace.as_deref_mut() {
                        trace.record_runtime_edge_source(
                            edge,
                            GraphRowEdgeCandidateSourceChoice::EndpointAdjacency,
                            format!(
                                "planned_driver={planned_driver}; materialized_source=EndpointAdjacency; fallback_source=none; skipped_due_to_empty_frontier={empty_frontier}; subset_intersection_source_materialized=none"
                            ),
                            candidate_ids.len(),
                        );
                    }
                }
            }
        } else {
            if has_bound_endpoint {
                self.graph_row_collect_endpoint_edge_ids(
                    &mut candidate_ids,
                    outgoing,
                    incoming,
                    both,
                    edge.label_filter_ids.as_deref(),
                    query.options.max_frontier,
                    frontier_peak,
                    vlp_cap_context,
                )?;
            }
            if has_unbound_endpoint_pair {
                match self.graph_row_materialize_edge_candidate_source(
                    query,
                    edge,
                    policy_cutoffs,
                    frontier_peak,
                    vlp_cap_context,
                )? {
                    GraphRowEdgeSourceRead::Ready {
                        mut ids,
                        followups: mut source_followups,
                        materialized_source,
                        subset_source,
                    } => {
                        followups.append(&mut source_followups);
                        candidate_ids.append(&mut ids);
                        let runtime_choice = if has_bound_endpoint {
                            GraphRowEdgeCandidateSourceChoice::MixedEndpointAndEdgeSource
                        } else {
                            GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource
                        };
                        let runtime_planned_driver = if has_bound_endpoint {
                            "MixedEndpointAndEdgeSource"
                        } else {
                            "EdgeCandidateSource"
                        };
                        let planned_driver = planned_source_choice
                            .map(graph_row_source_choice_label)
                            .unwrap_or(runtime_planned_driver);
                        if let Some(trace) = explain_trace {
                            trace.record_runtime_edge_source(
                                edge,
                                runtime_choice,
                                format!(
                                    "planned_driver={planned_driver}; materialized_source={materialized_source}; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized={}",
                                    subset_source.as_deref().unwrap_or("none")
                                ),
                                candidate_ids.len(),
                            );
                        }
                    }
                    GraphRowEdgeSourceRead::TooBroad { planned_source, .. } => {
                        if let Some(path) = vlp_cap_context {
                            return Err(graph_row_vlp_cap_error(
                                "max_frontier",
                                query.options.max_frontier,
                                path,
                            ));
                        }
                        return Err(EngineError::InvalidOperation(format!(
                            "graph row max_frontier exceeded configured cap {}; source=EdgeCandidateSource edge={} planned_source={planned_source}",
                            query.options.max_frontier,
                            edge.explain_name()
                        )));
                    }
                    GraphRowEdgeSourceRead::NoLegalSource => {
                        return Err(EngineError::InvalidOperation(
                            "graph row required edge pattern requires an anchor or allow_full_scan=true"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        candidate_ids.sort_unstable();
        candidate_ids.dedup();
        graph_row_record_frontier_cap_peak(
            frontier_peak,
            candidate_ids.len(),
            query.options.max_frontier,
            vlp_cap_context,
        )?;

        let verified = self.graph_row_verify_edge_candidates(
            &candidate_ids,
            edge,
            effective_at_epoch,
            policy_cutoffs,
        )?;
        let mut oriented = Vec::new();
        for meta in verified {
            for (logical_from, logical_to) in graph_row_edge_orientations(edge.direction, meta) {
                oriented.push(GraphRowOrientedEdge {
                    meta,
                    logical_from,
                    logical_to,
                });
            }
        }
        oriented.sort_by_key(|candidate| {
            (
                candidate.logical_from,
                candidate.logical_to,
                candidate.meta.id,
            )
        });
        graph_row_record_frontier_cap_peak(
            frontier_peak,
            oriented.len(),
            query.options.max_frontier,
            vlp_cap_context,
        )?;
        Ok(oriented)
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_collect_endpoint_edge_ids(
        &self,
        target: &mut Vec<u64>,
        outgoing: Vec<u64>,
        incoming: Vec<u64>,
        both: Vec<u64>,
        label_filter_ids: Option<&[u32]>,
        cap: usize,
        frontier_peak: &mut usize,
        vlp_cap_context: Option<&GraphRowRuntimeVariableLength>,
    ) -> Result<(), EngineError> {
        self.graph_row_extend_endpoint_edge_ids(
            target,
            Direction::Outgoing,
            outgoing,
            label_filter_ids,
            cap,
            frontier_peak,
            vlp_cap_context,
        )?;
        self.graph_row_extend_endpoint_edge_ids(
            target,
            Direction::Incoming,
            incoming,
            label_filter_ids,
            cap,
            frontier_peak,
            vlp_cap_context,
        )?;
        self.graph_row_extend_endpoint_edge_ids(
            target,
            Direction::Both,
            both,
            label_filter_ids,
            cap,
            frontier_peak,
            vlp_cap_context,
        )
    }

    fn graph_row_choose_bound_edge_source(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        outgoing: &[u64],
        incoming: &[u64],
        both: &[u64],
    ) -> Result<GraphRowEdgeCandidateSourceChoice, EngineError> {
        let endpoint_cost = self.graph_row_endpoint_source_cost(
            outgoing,
            incoming,
            both,
            edge.label_filter_ids.as_deref(),
        );
        let Some(edge_source_cost) = self.graph_row_edge_source_plan_cost(query, edge, false)?
        else {
            return Ok(GraphRowEdgeCandidateSourceChoice::EndpointAdjacency);
        };
        let edge_cost = edge_source_cost.cost;
        let Some(edge_candidates) = edge_cost.estimated_candidates else {
            return Ok(GraphRowEdgeCandidateSourceChoice::EndpointAdjacency);
        };
        let Some(endpoint_candidates) = endpoint_cost.estimated_candidates else {
            return Ok(GraphRowEdgeCandidateSourceChoice::EndpointAdjacency);
        };
        let edge_work = edge_cost
            .estimated_work
            .saturating_add(edge_candidates.saturating_mul(2));
        let endpoint_work = endpoint_cost
            .estimated_work
            .saturating_add(endpoint_candidates.saturating_mul(2));
        if edge_work < endpoint_work {
            Ok(GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource)
        } else {
            Ok(GraphRowEdgeCandidateSourceChoice::EndpointAdjacency)
        }
    }

    fn graph_row_endpoint_source_cost(
        &self,
        outgoing: &[u64],
        incoming: &[u64],
        both: &[u64],
        label_filter_ids: Option<&[u32]>,
    ) -> PlanCost {
        let mut count = 0u64;
        let mut confidence = EstimateConfidence::Exact;
        let mut exact = true;
        for (node_ids, direction) in [
            (outgoing, Direction::Outgoing),
            (incoming, Direction::Incoming),
            (both, Direction::Both),
        ] {
            if node_ids.is_empty() {
                continue;
            }
            let estimate = self.edge_endpoint_estimate(node_ids, direction, label_filter_ids);
            match estimate.known_upper_bound() {
                Some(estimate_count) => count = count.saturating_add(estimate_count),
                None => exact = false,
            }
            confidence = weaker_confidence(confidence, estimate.confidence);
        }
        let estimate = if exact {
            PlannerEstimate::upper_bound_with_confidence(count, confidence)
        } else {
            PlannerEstimate::unknown()
        };
        let source = PlannedEdgeCandidateSource::endpoint_adjacency(
            EdgeQueryCandidateSourceKind::AnyEndpointAdjacency,
            Arc::new(Vec::new()),
            label_filter_ids.map(Vec::from),
            estimate,
        );
        source.plan_cost()
    }

    fn graph_row_materialize_edge_candidate_source(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        frontier_peak: &mut usize,
        vlp_cap_context: Option<&GraphRowRuntimeVariableLength>,
    ) -> Result<GraphRowEdgeSourceRead, EngineError> {
        let cap = query.options.max_frontier;
        if edge.filter.is_always_false()
            || edge
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.is_empty())
        {
            return Ok(GraphRowEdgeSourceRead::Ready {
                ids: Vec::new(),
                followups: Vec::new(),
                materialized_source: "EmptyResult".to_string(),
                subset_source: None,
            });
        }

        let label_branches: Vec<Option<u32>> = match edge.label_filter_ids.as_deref() {
            Some(label_ids) => label_ids.iter().copied().map(Some).collect(),
            None => vec![None],
        };
        let mut all_ids = Vec::new();
        let mut followups = Vec::new();
        let mut sources = Vec::new();
        let mut subset_sources = Vec::new();

        for label_id in label_branches {
            let normalized = NormalizedEdgeQuery {
                label_id,
                ids: edge.candidate_edge_ids.clone(),
                from_ids: Vec::new(),
                to_ids: Vec::new(),
                endpoint_ids: Vec::new(),
                filter: edge.filter.clone(),
                allow_full_scan: query.options.allow_full_scan,
                page: PageRequest {
                    limit: Some(cap.saturating_add(1)),
                    after: None,
                },
                warnings: Vec::new(),
            };
            let planned = match self.plan_normalized_edge_query(&normalized) {
                Ok(planned) => planned,
                Err(EngineError::InvalidOperation(_)) => return Ok(GraphRowEdgeSourceRead::NoLegalSource),
                Err(error) => return Err(error),
            };
            let planned_source = format!("{:?}", planned.driver.plan_node());
            let subset_source = if matches!(planned.driver, EdgePhysicalPlan::Intersect(_)) {
                Some(planned_source.clone())
            } else {
                None
            };
            if matches!(
                &planned.driver,
                EdgePhysicalPlan::Source(source)
                    if matches!(
                        source.materialization,
                        EdgeCandidateMaterialization::FallbackFullEdgeScan
                    )
            ) && planned
                .estimated_candidate_count()
                .is_some_and(|count| count <= cap as u64)
            {
                let (page, mut source_followups) =
                    self.query_edge_page_planned(&normalized, planned, false, policy_cutoffs)?;
                followups.append(&mut source_followups);
                all_ids.extend(page.ids);
                sources.push(planned_source.clone());
                all_ids.sort_unstable();
                all_ids.dedup();
                if all_ids.len() > cap {
                    return Ok(GraphRowEdgeSourceRead::TooBroad {
                        followups,
                        planned_source,
                    });
                }
                graph_row_record_frontier_cap_peak(
                    frontier_peak,
                    all_ids.len(),
                    cap,
                    vlp_cap_context,
                )?;
                continue;
            }
            let mut materialized = self.materialize_edge_physical_plan(
                &normalized,
                planned.cap_context,
                &planned.driver,
            )?;
            let mut effective_source = planned_source;
            let mut effective_subset_source = subset_source;
            let compound_sidecar_failed = match &materialized {
                CandidateMaterializationResult::Ready { followups, .. }
                | CandidateMaterializationResult::TooBroad { followups } => followups
                    .iter()
                    .any(followup_is_compound_sidecar_failure),
            };
            let compound_too_broad = matches!(
                &materialized,
                CandidateMaterializationResult::TooBroad { .. }
            ) && edge_physical_plan_contains_compound(&planned.driver);
            if compound_sidecar_failed || compound_too_broad {
                // The selected compound source failed (missing/corrupt
                // sidecar) or returned more raw candidates than the cap
                // (stale postings or a genuinely broad tuple scan). Neither
                // is frontier breadth: the materialized result is TooBroad or
                // built from broad unverified fallback candidates. Replan
                // without compound candidates and stream the verified page
                // through the remaining legal edge sources so the frontier
                // cap applies to verified matches, not to the raw size of
                // the compound or fallback scan. Failure followups are kept
                // so lifecycle reconciliation still runs.
                let mut failure_followups = match materialized {
                    CandidateMaterializationResult::Ready { followups, .. }
                    | CandidateMaterializationResult::TooBroad { followups } => followups,
                };
                let replanned =
                    self.plan_normalized_edge_query_excluding_compound(&normalized)?;
                effective_source = format!("{:?}", replanned.driver.plan_node());
                effective_subset_source = None;
                let (page, mut retry_followups) =
                    self.query_edge_page_planned(&normalized, replanned, false, policy_cutoffs)?;
                failure_followups.append(&mut retry_followups);
                materialized = if page.ids.len() > cap {
                    CandidateMaterializationResult::TooBroad {
                        followups: failure_followups,
                    }
                } else {
                    CandidateMaterializationResult::Ready {
                        ids: page.ids,
                        followups: failure_followups,
                    }
                };
            }
            match materialized {
                CandidateMaterializationResult::Ready {
                    mut ids,
                    followups: mut source_followups,
                } => {
                    followups.append(&mut source_followups);
                    all_ids.append(&mut ids);
                    sources.push(effective_source);
                    if let Some(subset_source) = effective_subset_source {
                        subset_sources.push(subset_source);
                    }
                    all_ids.sort_unstable();
                    all_ids.dedup();
                    if all_ids.len() > cap {
                        return Ok(GraphRowEdgeSourceRead::TooBroad {
                            followups,
                            planned_source: sources
                                .last()
                                .cloned()
                                .unwrap_or_else(|| "EdgeCandidateSource".to_string()),
                        });
                    }
                    graph_row_record_frontier_cap_peak(
                        frontier_peak,
                        all_ids.len(),
                        cap,
                        vlp_cap_context,
                    )?;
                }
                CandidateMaterializationResult::TooBroad {
                    followups: mut source_followups,
                } => {
                    followups.append(&mut source_followups);
                    return Ok(GraphRowEdgeSourceRead::TooBroad {
                        followups,
                        planned_source: effective_source,
                    });
                }
            }
        }

        all_ids.sort_unstable();
        all_ids.dedup();
        if all_ids.len() > cap {
            return Ok(GraphRowEdgeSourceRead::TooBroad {
                followups,
                planned_source: sources.join("|"),
            });
        }
        graph_row_record_frontier_cap_peak(frontier_peak, all_ids.len(), cap, vlp_cap_context)?;
        Ok(GraphRowEdgeSourceRead::Ready {
            ids: all_ids,
            followups,
            materialized_source: if sources.is_empty() {
                "none".to_string()
            } else {
                sources.join("|")
            },
            subset_source: if subset_sources.is_empty() {
                None
            } else {
                Some(subset_sources.join("|"))
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_extend_endpoint_edge_ids(
        &self,
        target: &mut Vec<u64>,
        direction: Direction,
        mut node_ids: Vec<u64>,
        label_filter_ids: Option<&[u32]>,
        cap: usize,
        frontier_peak: &mut usize,
        vlp_cap_context: Option<&GraphRowRuntimeVariableLength>,
    ) -> Result<(), EngineError> {
        if node_ids.is_empty() {
            return Ok(());
        }
        node_ids.sort_unstable();
        node_ids.dedup();
        graph_row_record_frontier_cap_peak(frontier_peak, node_ids.len(), cap, vlp_cap_context)?;
        let mut ids = self.sources().edge_ids_by_endpoints_limited(
            &node_ids,
            direction,
            label_filter_ids,
            cap.saturating_add(1),
        )?;
        graph_row_record_frontier_cap_peak(frontier_peak, ids.len(), cap, vlp_cap_context)?;
        target.append(&mut ids);
        target.sort_unstable();
        target.dedup();
        graph_row_record_frontier_cap_peak(frontier_peak, target.len(), cap, vlp_cap_context)?;
        Ok(())
    }

    fn graph_row_verify_edge_candidates(
        &self,
        candidate_ids: &[u64],
        edge: &GraphRowRuntimeEdge,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<EdgeMetadataForQuery>, EngineError> {
        self.graph_row_verify_edge_candidate_ids(
            candidate_ids,
            edge.label_filter_ids.as_deref(),
            &edge.filter,
            effective_at_epoch,
            policy_cutoffs,
        )
    }

    fn graph_row_verify_edge_candidate_ids(
        &self,
        candidate_ids: &[u64],
        label_filter_ids: Option<&[u32]>,
        filter: &NormalizedEdgeFilter,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<EdgeMetadataForQuery>, EngineError> {
        let mut verified = Vec::new();
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();
        let mut property_keys = Vec::new();
        collect_edge_filter_property_keys(filter, &mut property_keys);

        for chunk in candidate_ids.chunks(QUERY_VERIFY_CHUNK) {
            let metadata = self.sources().find_edge_metadata(chunk)?;
            let mut metas = Vec::new();
            for meta in metadata.into_iter().flatten() {
                if label_filter_ids
                    .is_some_and(|label_ids| label_ids.binary_search(&meta.label_id).is_err())
                {
                    continue;
                }
                if !is_edge_valid_at(meta.valid_from, meta.valid_to, effective_at_epoch) {
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
                match edge_filter_metadata_outcome(filter, &query_meta) {
                    Some(false) => continue,
                    Some(true) => decisions.push((query_meta, false)),
                    None => {
                        property_candidate_ids.push(query_meta.id);
                        decisions.push((query_meta, true));
                    }
                }
            }

            let mut property_matches = NodeIdSet::default();
            if !property_candidate_ids.is_empty() {
                let include_created_at = edge_filter_needs_created_at(filter);
                let mut metadata_by_edge_id = NodeIdMap::with_capacity_and_hasher(
                    property_candidate_ids.len(),
                    Default::default(),
                );
                for (meta, needs_properties) in &decisions {
                    if *needs_properties {
                        metadata_by_edge_id.insert(meta.id, *meta);
                    }
                }
                let projected = self.sources().find_edge_projected_fields(
                    &property_candidate_ids,
                    &EdgeSelectedFieldNeeds {
                        created_at: include_created_at,
                        props: PropertySelection::Keys(property_keys.clone()),
                    },
                )?;
                for (&edge_id, selected) in property_candidate_ids.iter().zip(projected) {
                    let Some(selected) = selected else {
                        continue;
                    };
                    let Some(query_meta) = metadata_by_edge_id.get(&edge_id) else {
                        continue;
                    };
                    let mut selected = selected;
                    selected.meta = *query_meta;
                    if edge_filter_projected_matches(filter, &selected) {
                        property_matches.insert(edge_id);
                    }
                }
            }

            for (meta, needs_properties) in decisions {
                if needs_properties && !property_matches.contains(&meta.id) {
                    continue;
                }
                verified.push(meta);
            }
        }
        Ok(verified)
    }

    fn graph_row_verified_node_ids(
        &self,
        node: &GraphRowRuntimeNode,
        mut candidate_ids: Vec<u64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<NodeIdSet, EngineError> {
        candidate_ids.sort_unstable();
        candidate_ids.dedup();
        let mut verified = Vec::new();
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&node.query.filter, &mut property_keys);
        let include_key = !node.query.keys.is_empty() || node_filter_needs_key(&node.query.filter);
        let include_created_at = node_filter_needs_created_at(&node.query.filter);
        for chunk in candidate_ids.chunks(QUERY_VERIFY_CHUNK) {
            let _ = self.verify_node_candidate_chunk(
                chunk,
                &node.query,
                policy_cutoffs,
                include_key,
                include_created_at,
                &property_keys,
                &mut verified,
                usize::MAX,
            )?;
        }
        Ok(verified.into_iter().collect())
    }

    fn hydrate_graph_rows_for_needs(
        &self,
        rows: &mut [crate::graph_row::GraphBindingRow],
        schema: &crate::graph_row::GraphBindingSchema,
        needs: &EntityProjectionNeeds,
    ) -> Result<(), EngineError> {
        for (alias, node_needs) in &needs.nodes {
            let Some(slot) = schema.slot_for_alias(alias) else {
                continue;
            };
            let ids = graph_row_collect_node_ids(rows, slot)?;
            if ids.is_empty() {
                continue;
            }
            let selected = self.sources().find_node_projected_fields(&ids, node_needs)?;
            let mut by_id = NodeIdMap::with_capacity_and_hasher(ids.len(), Default::default());
            for (node_id, fields) in ids.into_iter().zip(selected) {
                if let Some(fields) = fields {
                    by_id.insert(node_id, fields);
                }
            }
            for row in rows.iter_mut() {
                let Some(node_id) = row.node_id_for_slot_if_bound(slot)? else {
                    continue;
                };
                let Some(fields) = by_id.get(&node_id) else {
                    continue;
                };
                row.bind_node(
                    slot,
                    crate::graph_row::GraphBoundNode::with_element(
                        node_id,
                        graph_node_value_from_selected(node_id, fields, &self.label_catalog)?,
                    ),
                )?;
            }
        }

        for (alias, edge_needs) in &needs.edges {
            let Some(slot) = schema.slot_for_alias(alias) else {
                continue;
            };
            let ids = graph_row_collect_edge_ids(rows, slot)?;
            if ids.is_empty() {
                continue;
            }
            let selected = self.sources().find_edge_projected_fields(&ids, edge_needs)?;
            let mut by_id = NodeIdMap::with_capacity_and_hasher(ids.len(), Default::default());
            for (edge_id, fields) in ids.into_iter().zip(selected) {
                if let Some(fields) = fields {
                    by_id.insert(edge_id, fields);
                }
            }
            for row in rows.iter_mut() {
                let Some(edge_id) = row.edge_id_for_slot_if_bound(slot)? else {
                    continue;
                };
                let Some(fields) = by_id.get(&edge_id) else {
                    continue;
                };
                row.bind_edge(
                    slot,
                    crate::graph_row::GraphBoundEdge::with_element(
                        edge_id,
                        graph_edge_value_from_selected(edge_id, fields, &self.label_catalog)?,
                    ),
                )?;
            }
        }

        for (alias, path_needs) in &needs.paths {
            if !graph_row_path_needs_require_selected_field_reads(path_needs) {
                continue;
            }
            let Some(slot) = schema.slot_for_alias(alias) else {
                continue;
            };
            self.hydrate_graph_path_rows_for_needs(rows, slot, path_needs)?;
        }
        Ok(())
    }

    fn hydrate_graph_path_rows_for_needs(
        &self,
        rows: &mut [crate::graph_row::GraphBindingRow],
        slot: crate::graph_row::GraphBindingSlotRef,
        needs: &PathSelectedFieldNeeds,
    ) -> Result<(), EngineError> {
        let node_needs = graph_row_path_node_hydration_needs(needs)?;
        let edge_needs = graph_row_path_edge_hydration_needs(needs)?;

        let mut nodes_by_id = NodeIdMap::default();
        if let Some(node_needs) = node_needs.as_ref() {
            let ids = graph_row_collect_path_node_ids(rows, slot, needs)?;
            if !ids.is_empty() {
                let selected = self.sources().find_node_projected_fields(&ids, node_needs)?;
                nodes_by_id = NodeIdMap::with_capacity_and_hasher(ids.len(), Default::default());
                for (node_id, fields) in ids.into_iter().zip(selected) {
                    if let Some(fields) = fields {
                        nodes_by_id.insert(
                            node_id,
                            graph_node_value_from_selected(node_id, &fields, &self.label_catalog)?,
                        );
                    }
                }
            }
        }

        let mut edges_by_id = NodeIdMap::default();
        if let Some(edge_needs) = edge_needs.as_ref() {
            let ids = graph_row_collect_path_edge_ids(rows, slot, needs)?;
            if !ids.is_empty() {
                let selected = self.sources().find_edge_projected_fields(&ids, edge_needs)?;
                edges_by_id = NodeIdMap::with_capacity_and_hasher(ids.len(), Default::default());
                for (edge_id, fields) in ids.into_iter().zip(selected) {
                    if let Some(fields) = fields {
                        edges_by_id.insert(
                            edge_id,
                            graph_edge_value_from_selected(edge_id, &fields, &self.label_catalog)?,
                        );
                    }
                }
            }
        }

        for row in rows.iter_mut() {
            let Some(path) = row.path_for_slot_if_bound(slot)?.cloned() else {
                continue;
            };
            let nodes = path
                .path
                .nodes
                .iter()
                .copied()
                .map(|node_id| {
                    if let Some(value) = nodes_by_id.get(&node_id) {
                        crate::graph_row::GraphBoundNode::with_element(node_id, value.clone())
                    } else {
                        crate::graph_row::GraphBoundNode::id_only(node_id)
                    }
                })
                .collect::<Vec<_>>();
            let edges = path
                .path
                .edges
                .iter()
                .copied()
                .map(|edge_id| {
                    if let Some(value) = edges_by_id.get(&edge_id) {
                        crate::graph_row::GraphBoundEdge::with_element(edge_id, value.clone())
                    } else {
                        crate::graph_row::GraphBoundEdge::id_only(edge_id)
                    }
                })
                .collect::<Vec<_>>();
            row.bind_path(
                slot,
                crate::graph_row::GraphBoundPath::with_values(path.path, nodes, edges)?,
            )?;
        }
        Ok(())
    }


}
