const QUERY_RANGE_CANDIDATE_CAP: usize =
    crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP;
const QUERY_BROAD_SOURCE_FACTOR: u64 = 4;
const MAX_BOOLEAN_PLANNING_PROBE_IDS: usize = 16_384;
const MAX_BOOLEAN_UNION_INPUTS: usize = 256;
const TINY_EXPLICIT_ANCHOR_MAX: u64 = 16;
const PLAN_COST_UNKNOWN_WORK: u64 = u64::MAX;
const GRAPH_ROW_FANOUT_UNKNOWN_WORK: u64 = u64::MAX / 4;
const GRAPH_ROW_FRONTIER_BUDGET: usize = crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP;
const GRAPH_ROW_HUB_HIGH_RATIO: u64 = 8;
const GRAPH_ROW_HUB_MEDIUM_RATIO: u64 = 4;
const GRAPH_ROW_CONFIDENCE_DOWNGRADE_STEP: u8 = 1;

struct PlannedNodeQuery {
    driver: NodePhysicalPlan,
    cap_context: QueryCapContext,
    warnings: Vec<QueryPlanWarning>,
    followups: Vec<SecondaryIndexReadFollowup>,
}

#[derive(Clone)]
enum NodePhysicalPlan {
    Empty,
    Source(PlannedNodeCandidateSource),
    Intersect(Vec<NodePhysicalPlan>),
    Union(Vec<NodePhysicalPlan>),
}

#[allow(clippy::large_enum_variant)]
enum BooleanPlanClassification {
    AlwaysFalse,
    VerifyOnly,
    Bounded {
        plan: NodePhysicalPlan,
        estimate: PlannerEstimate,
        structural_key: Vec<u8>,
        complete: bool,
    },
}

struct BooleanPlanResult {
    classification: BooleanPlanClassification,
    has_verify_only: bool,
}

struct BooleanPlanningBudget {
    remaining_probe_ids: usize,
}

#[derive(Clone)]
struct PlannedNodeCandidateSource {
    kind: NodeQueryCandidateSourceKind,
    canonical_key: String,
    estimate: PlannerEstimate,
    materialization: NodeCandidateMaterialization,
}

struct PlannedEdgeQuery {
    driver: EdgePhysicalPlan,
    cap_context: EdgeQueryCapContext,
    warnings: Vec<QueryPlanWarning>,
    followups: Vec<SecondaryIndexReadFollowup>,
}

#[derive(Clone, Debug)]
struct GraphRowPhysicalPlan {
    initial_driver: GraphRowInitialDriver,
    edge_order: Vec<usize>,
    segments: Vec<GraphRowPhysicalSegment>,
    edge_source_choices: Vec<Option<GraphRowEdgeCandidateSourceChoice>>,
    alternatives: Vec<GraphRowPlanAlternative>,
    notes: Vec<String>,
}

#[derive(Clone, Debug)]
enum GraphRowInitialDriver {
    Empty {
        reason: String,
    },
    Node {
        node_index: usize,
        alias: String,
    },
    Edge {
        edge_index: usize,
        edge_name: String,
    },
}

#[derive(Clone, Debug)]
struct GraphRowPlanAlternative {
    chosen: bool,
    kind: String,
    detail: String,
    decision: Option<String>,
    cost: Option<GraphRowPlanCost>,
}

type GraphRowEdgeSourcePlanCost = Option<(PlanCost, String, Vec<QueryPlanWarning>)>;
type GraphRowFrontierPlan = (
    Vec<usize>,
    Vec<(usize, GraphRowEdgeCandidateSourceChoice)>,
    GraphRowPlanCost,
);

const GRAPH_ROW_EDGE_INTERSECTION_TINY_SET: u64 = 64;

#[derive(Clone, Debug)]
struct GraphRowPhysicalSegment {
    segment_index: usize,
    barriers_before: Vec<GraphRowPlanBarrier>,
    initial_driver: GraphRowInitialDriver,
    edge_order: Vec<usize>,
}

struct GraphRowPhysicalSegmentPlan {
    segment: GraphRowPhysicalSegment,
    source_choices: Vec<(usize, GraphRowEdgeCandidateSourceChoice)>,
    alternatives: Vec<GraphRowPlanAlternative>,
}

struct GraphRowExpansionChoice {
    bound_rank: u8,
    complete: bool,
    estimated_expansion: u64,
    next_frontier: u64,
    confidence_rank: u8,
    hub_risk_rank: u8,
    coverage_rank: u8,
    source_rank: usize,
    canonical_key: String,
    edge_index: usize,
    source_choice: GraphRowEdgeCandidateSourceChoice,
    fanout: Option<GraphRowFanoutEstimate>,
}

#[derive(Clone, Copy)]
struct EdgeMetadataSidecarAvailability {
    weight: bool,
    updated_at: bool,
    valid_from: bool,
    valid_to: bool,
}

#[derive(Clone)]
enum EdgePhysicalPlan {
    Empty,
    Source(PlannedEdgeCandidateSource),
    Intersect(Vec<EdgePhysicalPlan>),
    Union(Vec<EdgePhysicalPlan>),
}

#[derive(Clone)]
struct PlannedEdgeCandidateSource {
    kind: EdgeQueryCandidateSourceKind,
    canonical_key: String,
    estimate: PlannerEstimate,
    materialization: EdgeCandidateMaterialization,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeQueryCandidateSourceKind {
    ExplicitEdgeIds,
    EdgeLabelIndex,
    EdgeTripleIndex,
    FromEndpointAdjacency,
    ToEndpointAdjacency,
    AnyEndpointAdjacency,
    EdgeWeightIndex,
    EdgeUpdatedAtIndex,
    EdgeValidFromIndex,
    EdgeValidToIndex,
    EdgeMetadataScan,
    EdgePropertyEqualityIndex,
    EdgePropertyRangeIndex,
    FallbackFullEdgeScan,
}

impl EdgeQueryCandidateSourceKind {
    fn plan_node(self) -> QueryPlanNode {
        match self {
            Self::ExplicitEdgeIds => QueryPlanNode::ExplicitEdgeIds,
            Self::EdgeLabelIndex => QueryPlanNode::EdgeLabelIndex,
            Self::EdgeTripleIndex => QueryPlanNode::EdgeTripleIndex,
            Self::FromEndpointAdjacency
            | Self::ToEndpointAdjacency
            | Self::AnyEndpointAdjacency => QueryPlanNode::EdgeEndpointAdjacency,
            Self::EdgeWeightIndex => QueryPlanNode::EdgeWeightIndex,
            Self::EdgeUpdatedAtIndex => QueryPlanNode::EdgeUpdatedAtIndex,
            Self::EdgeValidFromIndex | Self::EdgeValidToIndex => QueryPlanNode::EdgeValidityIndex,
            Self::EdgeMetadataScan => QueryPlanNode::EdgeMetadataScan,
            Self::EdgePropertyEqualityIndex => QueryPlanNode::EdgePropertyEqualityIndex,
            Self::EdgePropertyRangeIndex => QueryPlanNode::EdgePropertyRangeIndex,
            Self::FallbackFullEdgeScan => QueryPlanNode::FallbackFullEdgeScan,
        }
    }

    fn source_rank(self) -> usize {
        match self {
            Self::ExplicitEdgeIds => 0,
            Self::EdgeTripleIndex => 1,
            Self::FromEndpointAdjacency | Self::ToEndpointAdjacency => 2,
            Self::AnyEndpointAdjacency => 3,
            Self::EdgeWeightIndex
            | Self::EdgeUpdatedAtIndex
            | Self::EdgeValidFromIndex
            | Self::EdgeValidToIndex
            | Self::EdgePropertyEqualityIndex
            | Self::EdgePropertyRangeIndex => 4,
            Self::EdgeLabelIndex => 5,
            Self::EdgeMetadataScan => 6,
            Self::FallbackFullEdgeScan => 7,
        }
    }
}

#[derive(Clone)]
enum EdgeCandidateMaterialization {
    Precomputed(Vec<u64>),
    EdgeLabelIndex {
        label_id: u32,
    },
    EdgeTripleIndex {
        from: u64,
        to: u64,
        label_id: u32,
    },
    FromEndpointAdjacency {
        node_ids: Vec<u64>,
        label_filter_ids: Option<Vec<u32>>,
    },
    ToEndpointAdjacency {
        node_ids: Vec<u64>,
        label_filter_ids: Option<Vec<u32>>,
    },
    AnyEndpointAdjacency {
        node_ids: Vec<u64>,
        label_filter_ids: Option<Vec<u32>>,
    },
    EdgeWeightIndex {
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<f32>,
    },
    EdgeUpdatedAtIndex {
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
    },
    EdgeValidFromIndex {
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
    },
    EdgeValidToIndex {
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
    },
    EdgePropertyEqualityIndex {
        index_id: u64,
        label_id: u32,
        prop_key: String,
        value: PropValue,
        value_hashes: Vec<u64>,
    },
    EdgePropertyRangeIndex {
        index_id: u64,
        label_id: u32,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    FallbackFullEdgeScan,
}

struct CandidateProbe {
    source: Option<PlannedNodeCandidateSource>,
    warning: Option<QueryPlanWarning>,
    followup: Option<SecondaryIndexReadFollowup>,
}

struct EdgeCandidateProbe {
    source: Option<PlannedEdgeCandidateSource>,
    warning: Option<QueryPlanWarning>,
    followup: Option<SecondaryIndexReadFollowup>,
}

#[allow(clippy::large_enum_variant)]
enum EdgeBooleanPlanClassification {
    AlwaysFalse,
    VerifyOnly,
    Bounded {
        plan: EdgePhysicalPlan,
        estimate: PlannerEstimate,
        structural_key: Vec<u8>,
        complete: bool,
    },
}

struct EdgeBooleanPlanResult {
    classification: EdgeBooleanPlanClassification,
    has_verify_only: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct InProbeValue {
    value: PropValue,
    value_hash: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlanCost {
    estimated_work: u64,
    estimated_candidates: Option<u64>,
    estimate_kind_rank: u8,
    confidence_rank: u8,
    stale_risk_rank: u8,
    materialization_rank: u8,
    source_rank: usize,
    canonical_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GraphRowPlanCost {
    anchor_cost: PlanCost,
    estimated_work: u64,
    simulated_frontier: u64,
    fanout_complete: bool,
    confidence_rank: u8,
    stale_risk_rank: u8,
    hub_risk_rank: u8,
    frontier_capped: bool,
    source_rank: usize,
    canonical_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowFanoutCoverage {
    Complete,
    GlobalFallback,
    Missing,
}

impl GraphRowFanoutCoverage {
    fn complete(self) -> bool {
        matches!(self, GraphRowFanoutCoverage::Complete)
    }

    fn rank(self) -> u8 {
        match self {
            GraphRowFanoutCoverage::Complete => 0,
            GraphRowFanoutCoverage::GlobalFallback => 1,
            GraphRowFanoutCoverage::Missing => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowHubRisk {
    Low,
    Medium,
    High,
    Unknown,
}

impl GraphRowHubRisk {
    fn rank(self) -> u8 {
        match self {
            GraphRowHubRisk::Low => 0,
            GraphRowHubRisk::Medium => 1,
            GraphRowHubRisk::High => 2,
            GraphRowHubRisk::Unknown => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GraphRowFanoutEstimate {
    avg_upper_fanout: u64,
    p99_fanout: u64,
    max_fanout: u64,
    hub_risk: GraphRowHubRisk,
    confidence: EstimateConfidence,
    coverage: GraphRowFanoutCoverage,
    canonical_key: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryCapContext {
    cheapest_legal_universe: Option<PlannerEstimate>,
}

#[derive(Clone, Copy, Debug, Default)]
struct EdgeQueryCapContext {
    cheapest_legal_universe: Option<PlannerEstimate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlanMaterializationClass {
    Precomputed,
    KeyLookup,
    EagerIndex,
    StreamingLegalUniverse,
    Compound,
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PlannerEstimate {
    count: Option<u64>,
    kind: PlannerEstimateKind,
    confidence: EstimateConfidence,
    stale_risk: StalePostingRisk,
    proves_empty: bool,
    current_posting_bound: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeLabelMembershipEstimate {
    estimate: PlannerEstimate,
    driver_label_id: Option<u32>,
}

#[derive(Clone)]
enum NodeCandidateMaterialization {
    Precomputed(Vec<u64>),
    KeyLookup,
    NodeLabelIndex {
        label_id: u32,
    },
    NodeLabelAny {
        label_ids: NodeLabelSet,
    },
    PropertyEqualityIndex {
        index_id: u64,
        key: String,
        value: PropValue,
    },
    PropertyRangeIndex {
        index_id: u64,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    TimestampIndex {
        label_id: u32,
        lower_ms: i64,
        upper_ms: i64,
    },
    FallbackNodeLabelScan {
        label_id: u32,
    },
    FallbackFullNodeScan,
}

fn normalize_candidate_ids(mut ids: Vec<u64>) -> Vec<u64> {
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn reverse_graph_row_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn add_plan_warning(warnings: &mut Vec<QueryPlanWarning>, warning: QueryPlanWarning) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

fn plan_warning_rank(warning: QueryPlanWarning) -> usize {
    match warning {
        QueryPlanWarning::MissingReadyIndex => 0,
        QueryPlanWarning::UsingFallbackScan => 1,
        QueryPlanWarning::FullScanRequiresOptIn => 2,
        QueryPlanWarning::FullScanExplicitlyAllowed => 3,
        QueryPlanWarning::EdgePropertyPostFilter => 4,
        QueryPlanWarning::IndexSkippedAsBroad => 5,
        QueryPlanWarning::CandidateCapExceeded => 6,
        QueryPlanWarning::RangeCandidateCapExceeded => 7,
        QueryPlanWarning::TimestampCandidateCapExceeded => 8,
        QueryPlanWarning::VerifyOnlyFilter => 9,
        QueryPlanWarning::BooleanBranchFallback => 10,
        QueryPlanWarning::PlanningProbeBudgetExceeded => 11,
        QueryPlanWarning::UnknownNodeLabel => 12,
        QueryPlanWarning::UnknownEdgeLabel => 13,
    }
}

fn finalize_plan_warnings(warnings: &mut Vec<QueryPlanWarning>) {
    warnings.sort_by_key(|warning| plan_warning_rank(*warning));
    warnings.dedup();
}

fn explicit_anchor_universe_count(query: &NormalizedNodeQuery) -> Option<u64> {
    let mut count = None;
    if !query.ids.is_empty() {
        count = Some(query.ids.len() as u64);
    }
    if !query.keys.is_empty() {
        let key_count = query.keys.len() as u64;
        count = Some(count.map_or(key_count, |existing: u64| existing.min(key_count)));
    }
    count
}

fn node_index_candidate_labels(query: &NormalizedNodeQuery) -> Option<NodeLabelSet> {
    if let Some(single_label_id) = query.single_label_id {
        return NodeLabelSet::single(single_label_id).ok();
    }

    match query.label_filter {
        ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::All,
            label_ids,
            ..
        } => Some(label_ids),
        ResolvedNodeLabelFilter::Unconstrained
        | ResolvedNodeLabelFilter::Empty { .. }
        | ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::Any,
            ..
        } => None,
    }
}

fn should_skip_filter_planning_for_explicit_anchor(query: &NormalizedNodeQuery) -> bool {
    let Some(count) = explicit_anchor_universe_count(query) else {
        return false;
    };
    node_index_candidate_labels(query).is_none() || count <= TINY_EXPLICIT_ANCHOR_MAX
}

fn filter_has_intrinsic_verify_only(filter: &NormalizedNodeFilter) -> bool {
    match filter {
        NormalizedNodeFilter::PropertyExists { .. }
        | NormalizedNodeFilter::PropertyMissing { .. }
        | NormalizedNodeFilter::Not(_) => true,
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            children.iter().any(filter_has_intrinsic_verify_only)
        }
        NormalizedNodeFilter::AlwaysTrue
        | NormalizedNodeFilter::AlwaysFalse
        | NormalizedNodeFilter::PropertyEquals { .. }
        | NormalizedNodeFilter::PropertyIn { .. }
        | NormalizedNodeFilter::PropertyRange { .. }
        | NormalizedNodeFilter::UpdatedAtRange { .. } => false,
    }
}

fn equality_probe_value_hashes(value: &PropValue) -> Vec<u64> {
    vec![hash_prop_equality_key(value)]
}

fn unique_in_probe_values(values: &[PropValue]) -> Vec<InProbeValue> {
    unique_in_probe_values_with_hash(values, hash_semantic_equality_key_bytes)
}

fn unique_in_probe_values_with_hash(
    values: &[PropValue],
    mut hash_fn: impl FnMut(&[u8]) -> u64,
) -> Vec<InProbeValue> {
    let mut by_canonical: BTreeMap<Vec<u8>, InProbeValue> = BTreeMap::new();
    for value in values {
        let canonical_key = semantic_equality_key_bytes(value);
        match by_canonical.entry(canonical_key) {
            std::collections::btree_map::Entry::Occupied(_) => {}
            std::collections::btree_map::Entry::Vacant(entry) => {
                let value_hash = hash_fn(entry.key());
                entry.insert(InProbeValue {
                    value: value.clone(),
                    value_hash,
                });
            }
        }
    }
    by_canonical.into_values().collect()
}

fn estimate_confidence_from_rank(rank: u8) -> EstimateConfidence {
    match rank {
        0 => EstimateConfidence::Exact,
        1 => EstimateConfidence::High,
        2 => EstimateConfidence::Medium,
        3 => EstimateConfidence::Low,
        _ => EstimateConfidence::Unknown,
    }
}

fn downgrade_confidence(confidence: EstimateConfidence, steps: u8) -> EstimateConfidence {
    estimate_confidence_from_rank(confidence.rank().saturating_add(steps).min(4))
}

fn weaker_confidence(left: EstimateConfidence, right: EstimateConfidence) -> EstimateConfidence {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn higher_stale_posting_risk(left: StalePostingRisk, right: StalePostingRisk) -> StalePostingRisk {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn higher_graph_row_hub_risk(left: GraphRowHubRisk, right: GraphRowHubRisk) -> GraphRowHubRisk {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn worse_graph_row_fanout_coverage(
    left: GraphRowFanoutCoverage,
    right: GraphRowFanoutCoverage,
) -> GraphRowFanoutCoverage {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

impl BooleanPlanningBudget {
    fn new() -> Self {
        Self {
            remaining_probe_ids: MAX_BOOLEAN_PLANNING_PROBE_IDS,
        }
    }

    fn probe_limit(&self) -> usize {
        self.remaining_probe_ids.min(QUERY_RANGE_CANDIDATE_CAP)
    }

    fn consume_probe_ids(&mut self, count: usize) {
        self.remaining_probe_ids = self.remaining_probe_ids.saturating_sub(count);
    }
}

fn adaptive_candidate_cap(
    source_kind: NodeQueryCandidateSourceKind,
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    source_estimate: PlannerEstimate,
) -> usize {
    let default_cap = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP;
    let hard_cap = crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP;
    let Some(source_count) = source_estimate.known_upper_bound() else {
        return default_cap.min(hard_cap);
    };

    let legal_count = cheapest_legal_universe.and_then(PlannerEstimate::known_upper_bound);
    if legal_count.is_some_and(|legal_count| source_count >= legal_count) {
        return default_cap.min(hard_cap);
    }

    let high_confidence = matches!(
        source_estimate.confidence,
        EstimateConfidence::Exact | EstimateConfidence::High
    ) && !matches!(source_estimate.stale_risk, StalePostingRisk::High);

    let mut cap = default_cap.min(hard_cap);
    if high_confidence {
        if let Some(limit) = query_limit.filter(|limit| *limit > 0) {
            let proof_cap = limit
                .saturating_add(1)
                .saturating_mul(64)
                .max(default_cap);
            cap = cap.max(proof_cap.min(hard_cap));
        }
        if matches!(
            source_kind,
            NodeQueryCandidateSourceKind::PropertyRangeIndex
                | NodeQueryCandidateSourceKind::TimestampIndex
                | NodeQueryCandidateSourceKind::PropertyEqualityIndex
        ) {
            cap = cap.max((source_count.min(hard_cap as u64)) as usize);
        }
    }
    cap.min(hard_cap)
}

fn adaptive_union_total_cap(
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    union_estimate: PlannerEstimate,
) -> usize {
    adaptive_candidate_cap(
        NodeQueryCandidateSourceKind::PropertyEqualityIndex,
        query_limit,
        cheapest_legal_universe,
        union_estimate,
    )
    .saturating_mul(2)
    .min(crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP)
}

fn adaptive_edge_candidate_cap(
    source_kind: EdgeQueryCandidateSourceKind,
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    source_estimate: PlannerEstimate,
) -> usize {
    let default_cap = crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP;
    let hard_cap = crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP;
    let Some(source_count) = source_estimate.known_upper_bound() else {
        return default_cap.min(hard_cap);
    };

    let legal_count = cheapest_legal_universe.and_then(PlannerEstimate::known_upper_bound);
    if legal_count.is_some_and(|legal_count| source_count >= legal_count) {
        return default_cap.min(hard_cap);
    }

    let high_confidence = matches!(
        source_estimate.confidence,
        EstimateConfidence::Exact | EstimateConfidence::High
    ) && !matches!(source_estimate.stale_risk, StalePostingRisk::High);

    let mut cap = default_cap.min(hard_cap);
    if high_confidence {
        if let Some(limit) = query_limit.filter(|limit| *limit > 0) {
            let proof_cap = limit
                .saturating_add(1)
                .saturating_mul(64)
                .max(default_cap);
            cap = cap.max(proof_cap.min(hard_cap));
        }
        if matches!(
            source_kind,
            EdgeQueryCandidateSourceKind::EdgeWeightIndex
                | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
                | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
                | EdgeQueryCandidateSourceKind::EdgeValidToIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
        ) {
            cap = cap.max((source_count.min(hard_cap as u64)) as usize);
        }
    }
    cap.min(hard_cap)
}

fn adaptive_edge_union_total_cap(
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    union_estimate: PlannerEstimate,
) -> usize {
    adaptive_edge_candidate_cap(
        EdgeQueryCandidateSourceKind::EdgeWeightIndex,
        query_limit,
        cheapest_legal_universe,
        union_estimate,
    )
    .saturating_mul(2)
    .min(crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP)
}

impl QueryCapContext {
    fn source_cap(
        self,
        source_kind: NodeQueryCandidateSourceKind,
        query_limit: Option<usize>,
        source_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_candidate_cap(
            source_kind,
            query_limit,
            self.cheapest_legal_universe,
            source_estimate,
        )
    }

    fn source_estimate_exceeds_cap(
        self,
        source_kind: NodeQueryCandidateSourceKind,
        query_limit: Option<usize>,
        source_estimate: PlannerEstimate,
    ) -> bool {
        let Some(count) = source_estimate.known_upper_bound() else {
            return true;
        };
        count > self.source_cap(source_kind, query_limit, source_estimate) as u64
    }

    fn union_total_cap(
        self,
        query_limit: Option<usize>,
        union_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_union_total_cap(query_limit, self.cheapest_legal_universe, union_estimate)
    }

    fn cheapest_legal_count(self) -> Option<u64> {
        self.cheapest_legal_universe
            .and_then(PlannerEstimate::known_upper_bound)
    }
}

impl EdgeQueryCapContext {
    fn source_cap(
        self,
        source_kind: EdgeQueryCandidateSourceKind,
        query_limit: Option<usize>,
        source_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_edge_candidate_cap(
            source_kind,
            query_limit,
            self.cheapest_legal_universe,
            source_estimate,
        )
    }

    fn source_estimate_exceeds_cap(
        self,
        source_kind: EdgeQueryCandidateSourceKind,
        query_limit: Option<usize>,
        source_estimate: PlannerEstimate,
    ) -> bool {
        let Some(count) = source_estimate.known_upper_bound() else {
            return true;
        };
        count > self.source_cap(source_kind, query_limit, source_estimate) as u64
    }

    fn union_total_cap(
        self,
        query_limit: Option<usize>,
        union_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_edge_union_total_cap(
            query_limit,
            self.cheapest_legal_universe,
            union_estimate,
        )
    }

    fn cheapest_legal_count(self) -> Option<u64> {
        self.cheapest_legal_universe
            .and_then(PlannerEstimate::known_upper_bound)
    }
}

fn cap_warning_for_source(kind: NodeQueryCandidateSourceKind) -> QueryPlanWarning {
    match kind {
        NodeQueryCandidateSourceKind::PropertyRangeIndex => {
            QueryPlanWarning::RangeCandidateCapExceeded
        }
        NodeQueryCandidateSourceKind::TimestampIndex => QueryPlanWarning::TimestampCandidateCapExceeded,
        _ => QueryPlanWarning::CandidateCapExceeded,
    }
}

fn edge_cap_warning_for_source(kind: EdgeQueryCandidateSourceKind) -> QueryPlanWarning {
    match kind {
        EdgeQueryCandidateSourceKind::EdgeWeightIndex
        | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex => {
            QueryPlanWarning::RangeCandidateCapExceeded
        }
        EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
        | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
        | EdgeQueryCandidateSourceKind::EdgeValidToIndex => {
            QueryPlanWarning::TimestampCandidateCapExceeded
        }
        _ => QueryPlanWarning::CandidateCapExceeded,
    }
}

fn memtable_secondary_eq_edge_count_for_filter(
    memtable: &Memtable,
    index_id: u64,
    prop_key: &str,
    prop_value: &PropValue,
    snapshot_seq: u64,
) -> usize {
    memtable.secondary_eq_edge_count_at(index_id, prop_key, prop_value, snapshot_seq)
}

fn segment_edge_secondary_eq_posting_count_for_filter(
    segment: &SegmentReader,
    index_id: u64,
    prop_value: &PropValue,
) -> Result<Option<usize>, EngineError> {
    let mut count = 0usize;
    for value_hash in equality_probe_value_hashes(prop_value) {
        let Some(probe_count) =
            segment.edge_secondary_eq_posting_count_if_present(index_id, value_hash)?
        else {
            return Ok(None);
        };
        count = count.saturating_add(probe_count);
    }
    Ok(Some(count))
}

impl NodeQueryCandidateSourceKind {
    fn plan_node(self) -> QueryPlanNode {
        match self {
            NodeQueryCandidateSourceKind::ExplicitIds => QueryPlanNode::ExplicitIds,
            NodeQueryCandidateSourceKind::KeyLookup => QueryPlanNode::KeyLookup,
            NodeQueryCandidateSourceKind::NodeLabelIndex => QueryPlanNode::NodeLabelIndex,
            NodeQueryCandidateSourceKind::PropertyEqualityIndex => {
                QueryPlanNode::PropertyEqualityIndex
            }
            NodeQueryCandidateSourceKind::PropertyRangeIndex => QueryPlanNode::PropertyRangeIndex,
            NodeQueryCandidateSourceKind::TimestampIndex => QueryPlanNode::TimestampIndex,
            NodeQueryCandidateSourceKind::FallbackNodeLabelScan => QueryPlanNode::FallbackNodeLabelScan,
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => QueryPlanNode::FallbackFullNodeScan,
        }
    }

    fn selectivity_rank(self) -> usize {
        match self {
            NodeQueryCandidateSourceKind::ExplicitIds => 0,
            NodeQueryCandidateSourceKind::KeyLookup => 1,
            NodeQueryCandidateSourceKind::PropertyEqualityIndex => 2,
            NodeQueryCandidateSourceKind::PropertyRangeIndex => 3,
            NodeQueryCandidateSourceKind::TimestampIndex => 4,
            NodeQueryCandidateSourceKind::NodeLabelIndex => 5,
            NodeQueryCandidateSourceKind::FallbackNodeLabelScan => 6,
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => 7,
        }
    }
}

impl PlannedNodeCandidateSource {
    fn with_ids(kind: NodeQueryCandidateSourceKind, canonical_key: String, ids: Vec<u64>) -> Self {
        let ids = normalize_candidate_ids(ids);
        let estimate = PlannerEstimate::exact_cheap(ids.len() as u64);
        Self {
            kind,
            canonical_key,
            estimate,
            materialization: NodeCandidateMaterialization::Precomputed(ids),
        }
    }

    fn key_lookup(key_count: usize) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::KeyLookup,
            canonical_key: "keys".to_string(),
            estimate: PlannerEstimate::upper_bound(key_count as u64),
            materialization: NodeCandidateMaterialization::KeyLookup,
        }
    }

    fn node_label_index(label_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::NodeLabelIndex,
            canonical_key: format!("label:{label_id}"),
            estimate,
            materialization: NodeCandidateMaterialization::NodeLabelIndex { label_id },
        }
    }

    fn node_label_any_index(label_ids: NodeLabelSet, estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::NodeLabelIndex,
            canonical_key: format!("label_any:{:?}", label_ids.as_slice()),
            estimate,
            materialization: NodeCandidateMaterialization::NodeLabelAny { label_ids },
        }
    }

    fn fallback_node_label_scan(label_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::FallbackNodeLabelScan,
            canonical_key: format!("fallback_label:{label_id}"),
            estimate,
            materialization: NodeCandidateMaterialization::FallbackNodeLabelScan { label_id },
        }
    }

    fn fallback_full_scan(estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::FallbackFullNodeScan,
            canonical_key: "fallback_full".to_string(),
            estimate,
            materialization: NodeCandidateMaterialization::FallbackFullNodeScan,
        }
    }

    fn property_equality_index(
        label_id: u32,
        index_id: u64,
        key: &str,
        value: &PropValue,
        estimate: PlannerEstimate,
    ) -> Self {
        Self::property_equality_index_with_hash(
            label_id,
            index_id,
            key,
            value,
            hash_prop_equality_key(value),
            estimate,
        )
    }

    fn property_equality_index_with_hash(
        label_id: u32,
        index_id: u64,
        key: &str,
        value: &PropValue,
        value_hash: u64,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::PropertyEqualityIndex,
            canonical_key: format!("eq:{label_id}:{key}:{value_hash}"),
            estimate,
            materialization: NodeCandidateMaterialization::PropertyEqualityIndex {
                index_id,
                key: key.to_string(),
                value: value.clone(),
            },
        }
    }

    fn property_range_index(
        index_id: u64,
        key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::PropertyRangeIndex,
            canonical_key: format!("range:{index_id}:{key}:{lower:?}:{upper:?}"),
            estimate,
            materialization: NodeCandidateMaterialization::PropertyRangeIndex {
                index_id,
                lower: lower.cloned(),
                upper: upper.cloned(),
            },
        }
    }

    fn timestamp_index(
        label_id: u32,
        lower_ms: i64,
        upper_ms: i64,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::TimestampIndex,
            canonical_key: format!("time:{label_id}:{lower_ms}:{upper_ms}"),
            estimate,
            materialization: NodeCandidateMaterialization::TimestampIndex {
                label_id,
                lower_ms,
                upper_ms,
            },
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        match self.materialization {
            NodeCandidateMaterialization::NodeLabelAny { .. } => QueryPlanNode::NodeLabelAnyIndex,
            _ => self.kind.plan_node(),
        }
    }

    fn broad_skip_warnable(&self) -> bool {
        matches!(
            self.kind,
            NodeQueryCandidateSourceKind::PropertyEqualityIndex
                | NodeQueryCandidateSourceKind::PropertyRangeIndex
                | NodeQueryCandidateSourceKind::TimestampIndex
        )
    }
}

impl PlannedEdgeCandidateSource {
    fn with_ids(kind: EdgeQueryCandidateSourceKind, canonical_key: String, ids: Vec<u64>) -> Self {
        let ids = normalize_candidate_ids(ids);
        let estimate = PlannerEstimate::exact_cheap(ids.len() as u64);
        Self {
            kind,
            canonical_key,
            estimate,
            materialization: EdgeCandidateMaterialization::Precomputed(ids),
        }
    }

    fn edge_label_index(label_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgeLabelIndex,
            canonical_key: format!("label:{label_id}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeLabelIndex { label_id },
        }
    }

    fn edge_triple_index(from: u64, to: u64, label_id: u32) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgeTripleIndex,
            canonical_key: format!("edge_triple:{from}:{to}:{label_id}"),
            estimate: PlannerEstimate::unknown(),
            materialization: EdgeCandidateMaterialization::EdgeTripleIndex { from, to, label_id },
        }
    }

    fn endpoint_adjacency(
        kind: EdgeQueryCandidateSourceKind,
        node_ids: Vec<u64>,
        label_filter_ids: Option<Vec<u32>>,
        estimate: PlannerEstimate,
    ) -> Self {
        let canonical_key = format!("{kind:?}:{node_ids:?}:{label_filter_ids:?}");
        let materialization = match kind {
            EdgeQueryCandidateSourceKind::FromEndpointAdjacency => {
                EdgeCandidateMaterialization::FromEndpointAdjacency {
                    node_ids,
                    label_filter_ids,
                }
            }
            EdgeQueryCandidateSourceKind::ToEndpointAdjacency => {
                EdgeCandidateMaterialization::ToEndpointAdjacency {
                    node_ids,
                    label_filter_ids,
                }
            }
            EdgeQueryCandidateSourceKind::AnyEndpointAdjacency => {
                EdgeCandidateMaterialization::AnyEndpointAdjacency {
                    node_ids,
                    label_filter_ids,
                }
            }
            _ => unreachable!("endpoint source kind required"),
        };
        Self {
            kind,
            canonical_key,
            estimate,
            materialization,
        }
    }

    fn edge_weight_index(
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<f32>,
        indexed: bool,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: if indexed {
                EdgeQueryCandidateSourceKind::EdgeWeightIndex
            } else {
                EdgeQueryCandidateSourceKind::EdgeMetadataScan
            },
            canonical_key: format!("edge_weight:{label_id:?}:{bounds:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeWeightIndex { label_id, bounds },
        }
    }

    fn edge_updated_at_index(
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
        indexed: bool,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: if indexed {
                EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
            } else {
                EdgeQueryCandidateSourceKind::EdgeMetadataScan
            },
            canonical_key: format!("edge_updated_at:{label_id:?}:{bounds:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeUpdatedAtIndex { label_id, bounds },
        }
    }

    fn edge_valid_from_index(
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
        indexed: bool,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: if indexed {
                EdgeQueryCandidateSourceKind::EdgeValidFromIndex
            } else {
                EdgeQueryCandidateSourceKind::EdgeMetadataScan
            },
            canonical_key: format!("edge_valid_from:{label_id:?}:{bounds:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeValidFromIndex { label_id, bounds },
        }
    }

    fn edge_valid_to_index(
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
        indexed: bool,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: if indexed {
                EdgeQueryCandidateSourceKind::EdgeValidToIndex
            } else {
                EdgeQueryCandidateSourceKind::EdgeMetadataScan
            },
            canonical_key: format!("edge_valid_to:{label_id:?}:{bounds:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeValidToIndex { label_id, bounds },
        }
    }

    fn edge_property_equality_index(
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        value: &PropValue,
        estimate: PlannerEstimate,
    ) -> Self {
        let value_hashes = equality_probe_value_hashes(value);
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
            canonical_key: format!("edge_prop_eq:{label_id}:{prop_key}:{value_hashes:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
                index_id,
                label_id,
                prop_key: prop_key.to_string(),
                value: value.clone(),
                value_hashes,
            },
        }
    }

    fn edge_property_equality_index_with_hash(
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        value: &PropValue,
        value_hash: u64,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
            canonical_key: format!("edge_prop_eq:{label_id}:{prop_key}:{value_hash}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
                index_id,
                label_id,
                prop_key: prop_key.to_string(),
                value: value.clone(),
                value_hashes: vec![value_hash],
            },
        }
    }

    fn edge_property_range_index(
        label_id: u32,
        index_id: u64,
        prop_key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
            canonical_key: format!("edge_prop_range:{label_id}:{index_id}:{prop_key}:{lower:?}:{upper:?}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgePropertyRangeIndex {
                index_id,
                label_id,
                prop_key: prop_key.to_string(),
                lower: lower.cloned(),
                upper: upper.cloned(),
            },
        }
    }

    fn fallback_full_scan(estimate: PlannerEstimate) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::FallbackFullEdgeScan,
            canonical_key: "fallback_full_edge_scan".to_string(),
            estimate,
            materialization: EdgeCandidateMaterialization::FallbackFullEdgeScan,
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        self.kind.plan_node()
    }

    fn broad_skip_warnable(&self) -> bool {
        matches!(
            self.kind,
            EdgeQueryCandidateSourceKind::EdgeLabelIndex
                | EdgeQueryCandidateSourceKind::FromEndpointAdjacency
                | EdgeQueryCandidateSourceKind::ToEndpointAdjacency
                | EdgeQueryCandidateSourceKind::AnyEndpointAdjacency
                | EdgeQueryCandidateSourceKind::EdgeWeightIndex
                | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
                | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
                | EdgeQueryCandidateSourceKind::EdgeValidToIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
                | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
                | EdgeQueryCandidateSourceKind::EdgeMetadataScan
        )
    }

    fn estimated_work(&self) -> u64 {
        let count = self.estimate.known_upper_bound().unwrap_or(u64::MAX);
        let (setup, candidate_weight) = match self.kind {
            EdgeQueryCandidateSourceKind::ExplicitEdgeIds => (1u64, 1u64),
            EdgeQueryCandidateSourceKind::EdgeTripleIndex => (2, 1),
            EdgeQueryCandidateSourceKind::FromEndpointAdjacency
            | EdgeQueryCandidateSourceKind::ToEndpointAdjacency
            | EdgeQueryCandidateSourceKind::AnyEndpointAdjacency => (8, 2),
            EdgeQueryCandidateSourceKind::EdgeWeightIndex
            | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
            | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
            | EdgeQueryCandidateSourceKind::EdgeValidToIndex
            | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex => (12, 2),
            EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex => (8, 2),
            EdgeQueryCandidateSourceKind::EdgeLabelIndex
            | EdgeQueryCandidateSourceKind::EdgeMetadataScan => (24, 3),
            EdgeQueryCandidateSourceKind::FallbackFullEdgeScan => (64, 4),
        };
        setup.saturating_add(count.saturating_mul(candidate_weight))
    }
}

impl NodePhysicalPlan {
    fn source(source: PlannedNodeCandidateSource) -> Self {
        Self::Source(source)
    }

    fn intersect(inputs: Vec<NodePhysicalPlan>) -> Self {
        let mut flattened = Vec::new();
        for input in inputs {
            match input {
                NodePhysicalPlan::Empty => return NodePhysicalPlan::Empty,
                NodePhysicalPlan::Intersect(children) => flattened.extend(children),
                plan => flattened.push(plan),
            }
        }
        match flattened.len() {
            0 => NodePhysicalPlan::Empty,
            1 => flattened.into_iter().next().unwrap(),
            _ => NodePhysicalPlan::Intersect(flattened),
        }
    }

    fn union(inputs: Vec<NodePhysicalPlan>) -> Self {
        let mut flattened = Vec::new();
        for input in inputs {
            match input {
                NodePhysicalPlan::Empty => {}
                NodePhysicalPlan::Union(children) => flattened.extend(children),
                plan => flattened.push(plan),
            }
        }
        match flattened.len() {
            0 => NodePhysicalPlan::Empty,
            1 => flattened.into_iter().next().unwrap(),
            _ => NodePhysicalPlan::Union(flattened),
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        match self {
            NodePhysicalPlan::Empty => QueryPlanNode::EmptyResult,
            NodePhysicalPlan::Source(source) => source.plan_node(),
            NodePhysicalPlan::Intersect(inputs) => QueryPlanNode::Intersect {
                inputs: inputs.iter().map(NodePhysicalPlan::plan_node).collect(),
            },
            NodePhysicalPlan::Union(inputs) => QueryPlanNode::Union {
                inputs: inputs.iter().map(NodePhysicalPlan::plan_node).collect(),
            },
        }
    }

    fn estimate(&self) -> PlannerEstimate {
        match self {
            NodePhysicalPlan::Empty => PlannerEstimate::exact_cheap(0),
            NodePhysicalPlan::Source(source) => source.estimate,
            NodePhysicalPlan::Intersect(inputs) => inputs
                .iter()
                .map(NodePhysicalPlan::estimate)
                .filter_map(PlannerEstimate::known_upper_bound)
                .min()
                .map(PlannerEstimate::upper_bound)
                .unwrap_or_else(PlannerEstimate::unknown),
            NodePhysicalPlan::Union(inputs) => {
                let mut total = 0u64;
                for input in inputs {
                    let Some(count) = input.estimate().known_upper_bound() else {
                        return PlannerEstimate::unknown();
                    };
                    total = total.saturating_add(count);
                }
                PlannerEstimate::upper_bound(total)
            }
        }
    }

    fn selectivity_rank(&self) -> usize {
        match self {
            NodePhysicalPlan::Empty => 0,
            NodePhysicalPlan::Source(source) => source.kind.selectivity_rank(),
            NodePhysicalPlan::Intersect(inputs) => inputs
                .iter()
                .map(NodePhysicalPlan::selectivity_rank)
                .min()
                .unwrap_or(usize::MAX),
            NodePhysicalPlan::Union(_) => 4,
        }
    }

    fn materialization_class(&self) -> PlanMaterializationClass {
        match self {
            NodePhysicalPlan::Empty => PlanMaterializationClass::Empty,
            NodePhysicalPlan::Source(source) => source.materialization.materialization_class(),
            NodePhysicalPlan::Intersect(_) | NodePhysicalPlan::Union(_) => {
                PlanMaterializationClass::Compound
            }
        }
    }

    fn plan_cost(&self) -> PlanCost {
        let estimate = self.estimate();
        let estimated_candidates = estimate.known_upper_bound();
        let base_work = match self {
            NodePhysicalPlan::Empty => 0,
            NodePhysicalPlan::Source(source) => source.estimated_work(),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                inputs.iter().fold(16u64, |total, input| {
                    total.saturating_add(
                        input
                            .estimate()
                            .known_upper_bound()
                            .unwrap_or(u64::MAX / 4)
                            .saturating_mul(2),
                    )
                })
            }
        };
        let estimated_work = estimate.apply_cost_penalties(base_work);
        PlanCost {
            estimated_work,
            estimated_candidates,
            estimate_kind_rank: estimate.kind.rank(),
            confidence_rank: estimate.confidence.rank(),
            stale_risk_rank: estimate.stale_risk.rank(),
            materialization_rank: self.materialization_class().rank(),
            source_rank: self.selectivity_rank(),
            canonical_key: self.canonical_key(),
        }
    }

    fn canonical_key(&self) -> String {
        match self {
            NodePhysicalPlan::Empty => "empty".to_string(),
            NodePhysicalPlan::Source(source) => source.canonical_key.clone(),
            NodePhysicalPlan::Intersect(inputs) => {
                let mut key = String::from("and:");
                for input in inputs {
                    key.push_str(&input.canonical_key());
                    key.push('|');
                }
                key
            }
            NodePhysicalPlan::Union(inputs) => {
                let mut key = String::from("or:");
                for input in inputs {
                    key.push_str(&input.canonical_key());
                    key.push('|');
                }
                key
            }
        }
    }

    fn broad_skip_warnable(&self) -> bool {
        match self {
            NodePhysicalPlan::Empty => false,
            NodePhysicalPlan::Source(source) => source.broad_skip_warnable(),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                inputs.iter().any(NodePhysicalPlan::broad_skip_warnable)
            }
        }
    }

    fn uses_label_postings(&self) -> bool {
        match self {
            NodePhysicalPlan::Source(source) => matches!(
                source.materialization,
                NodeCandidateMaterialization::NodeLabelIndex { .. }
                    | NodeCandidateMaterialization::NodeLabelAny { .. }
                    | NodeCandidateMaterialization::FallbackNodeLabelScan { .. }
            ),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                inputs.iter().any(NodePhysicalPlan::uses_label_postings)
            }
            NodePhysicalPlan::Empty => false,
        }
    }

    fn uses_label_any_union(&self) -> bool {
        match self {
            NodePhysicalPlan::Source(source) => matches!(
                source.materialization,
                NodeCandidateMaterialization::NodeLabelAny { .. }
            ),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                inputs.iter().any(NodePhysicalPlan::uses_label_any_union)
            }
            NodePhysicalPlan::Empty => false,
        }
    }
}

impl EdgePhysicalPlan {
    fn source(source: PlannedEdgeCandidateSource) -> Self {
        Self::Source(source)
    }

    fn intersect(inputs: Vec<EdgePhysicalPlan>) -> Self {
        let mut flattened = Vec::new();
        for input in inputs {
            match input {
                EdgePhysicalPlan::Empty => return EdgePhysicalPlan::Empty,
                EdgePhysicalPlan::Intersect(children) => flattened.extend(children),
                plan => flattened.push(plan),
            }
        }
        match flattened.len() {
            0 => EdgePhysicalPlan::Empty,
            1 => flattened.into_iter().next().unwrap(),
            _ => EdgePhysicalPlan::Intersect(flattened),
        }
    }

    fn union(inputs: Vec<EdgePhysicalPlan>) -> Self {
        let mut flattened = Vec::new();
        for input in inputs {
            match input {
                EdgePhysicalPlan::Empty => {}
                EdgePhysicalPlan::Union(children) => flattened.extend(children),
                plan => flattened.push(plan),
            }
        }
        match flattened.len() {
            0 => EdgePhysicalPlan::Empty,
            1 => flattened.into_iter().next().unwrap(),
            _ => EdgePhysicalPlan::Union(flattened),
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        match self {
            EdgePhysicalPlan::Empty => QueryPlanNode::EmptyResult,
            EdgePhysicalPlan::Source(source) => source.plan_node(),
            EdgePhysicalPlan::Intersect(inputs) => QueryPlanNode::Intersect {
                inputs: inputs.iter().map(EdgePhysicalPlan::plan_node).collect(),
            },
            EdgePhysicalPlan::Union(inputs) => QueryPlanNode::Union {
                inputs: inputs.iter().map(EdgePhysicalPlan::plan_node).collect(),
            },
        }
    }

    fn estimate(&self) -> PlannerEstimate {
        match self {
            EdgePhysicalPlan::Empty => PlannerEstimate::exact_cheap(0),
            EdgePhysicalPlan::Source(source) => source.estimate,
            EdgePhysicalPlan::Intersect(inputs) => inputs
                .iter()
                .map(EdgePhysicalPlan::estimate)
                .filter_map(PlannerEstimate::known_upper_bound)
                .min()
                .map(PlannerEstimate::upper_bound)
                .unwrap_or_else(PlannerEstimate::unknown),
            EdgePhysicalPlan::Union(inputs) => {
                let mut total = 0u64;
                for input in inputs {
                    let Some(count) = input.estimate().known_upper_bound() else {
                        return PlannerEstimate::unknown();
                    };
                    total = total.saturating_add(count);
                }
                PlannerEstimate::upper_bound(total)
            }
        }
    }

    fn cap_source_kind(&self) -> EdgeQueryCandidateSourceKind {
        match self {
            EdgePhysicalPlan::Empty => EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
            EdgePhysicalPlan::Source(source) => source.kind,
            EdgePhysicalPlan::Intersect(inputs) => inputs
                .iter()
                .min_by(|left, right| left.plan_cost().cmp(&right.plan_cost()))
                .map(EdgePhysicalPlan::cap_source_kind)
                .unwrap_or(EdgeQueryCandidateSourceKind::EdgeMetadataScan),
            EdgePhysicalPlan::Union(inputs) => {
                let mut kinds = inputs.iter().map(EdgePhysicalPlan::cap_source_kind);
                let Some(first) = kinds.next() else {
                    return EdgeQueryCandidateSourceKind::EdgeMetadataScan;
                };
                if kinds.all(|kind| kind == first) {
                    first
                } else {
                    EdgeQueryCandidateSourceKind::EdgeMetadataScan
                }
            }
        }
    }

    fn materialization_cap(
        &self,
        cap_context: EdgeQueryCapContext,
        query_limit: Option<usize>,
    ) -> usize {
        let estimate = self.estimate();
        match self {
            EdgePhysicalPlan::Union(_) => cap_context.union_total_cap(query_limit, estimate),
            _ => cap_context.source_cap(self.cap_source_kind(), query_limit, estimate),
        }
    }

    #[cfg(test)]
    fn estimate_exceeds_cap(
        &self,
        cap_context: EdgeQueryCapContext,
        query_limit: Option<usize>,
    ) -> bool {
        let estimate = self.estimate();
        let Some(count) = estimate.known_upper_bound() else {
            return true;
        };
        count > self.materialization_cap(cap_context, query_limit) as u64
    }

    fn canonical_key(&self) -> String {
        match self {
            EdgePhysicalPlan::Empty => "edge_empty".to_string(),
            EdgePhysicalPlan::Source(source) => source.canonical_key.clone(),
            EdgePhysicalPlan::Intersect(inputs) => {
                let mut key = String::from("edge_and:");
                for input in inputs {
                    key.push_str(&input.canonical_key());
                    key.push('|');
                }
                key
            }
            EdgePhysicalPlan::Union(inputs) => {
                let mut key = String::from("edge_or:");
                for input in inputs {
                    key.push_str(&input.canonical_key());
                    key.push('|');
                }
                key
            }
        }
    }

    fn source_rank(&self) -> usize {
        match self {
            EdgePhysicalPlan::Empty => 0,
            EdgePhysicalPlan::Source(source) => source.kind.source_rank(),
            EdgePhysicalPlan::Intersect(inputs) => inputs
                .iter()
                .map(EdgePhysicalPlan::source_rank)
                .min()
                .unwrap_or(usize::MAX),
            EdgePhysicalPlan::Union(_) => 4,
        }
    }

    fn materialization_class(&self) -> PlanMaterializationClass {
        match self {
            EdgePhysicalPlan::Empty => PlanMaterializationClass::Empty,
            EdgePhysicalPlan::Source(source) => source.materialization.materialization_class(),
            EdgePhysicalPlan::Intersect(_) | EdgePhysicalPlan::Union(_) => {
                PlanMaterializationClass::Compound
            }
        }
    }

    fn plan_cost(&self) -> PlanCost {
        let estimate = self.estimate();
        let estimated_candidates = estimate.known_upper_bound();
        let base_work = match self {
            EdgePhysicalPlan::Empty => 0,
            EdgePhysicalPlan::Source(source) => source.estimated_work(),
            EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
                inputs.iter().fold(16u64, |total, input| {
                    total.saturating_add(
                        input
                            .estimate()
                            .known_upper_bound()
                            .unwrap_or(u64::MAX / 4)
                            .saturating_mul(2),
                    )
                })
            }
        };
        PlanCost {
            estimated_work: estimate.apply_cost_penalties(base_work),
            estimated_candidates,
            estimate_kind_rank: estimate.kind.rank(),
            confidence_rank: estimate.confidence.rank(),
            stale_risk_rank: estimate.stale_risk.rank(),
            materialization_rank: self.materialization_class().rank(),
            source_rank: self.source_rank(),
            canonical_key: self.canonical_key(),
        }
    }

    fn broad_skip_warnable(&self) -> bool {
        match self {
            EdgePhysicalPlan::Empty => false,
            EdgePhysicalPlan::Source(source) => source.broad_skip_warnable(),
            EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
                inputs.iter().any(EdgePhysicalPlan::broad_skip_warnable)
            }
        }
    }
}

impl Ord for PlanCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.estimated_work
            .cmp(&other.estimated_work)
            .then_with(|| {
                self.estimated_candidates
                    .unwrap_or(u64::MAX)
                    .cmp(&other.estimated_candidates.unwrap_or(u64::MAX))
            })
            .then_with(|| self.estimate_kind_rank.cmp(&other.estimate_kind_rank))
            .then_with(|| self.confidence_rank.cmp(&other.confidence_rank))
            .then_with(|| self.stale_risk_rank.cmp(&other.stale_risk_rank))
            .then_with(|| self.materialization_rank.cmp(&other.materialization_rank))
            .then_with(|| self.source_rank.cmp(&other.source_rank))
            .then_with(|| self.canonical_key.cmp(&other.canonical_key))
    }
}

impl PartialOrd for PlanCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GraphRowPlanCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let primary = if self.fanout_complete && other.fanout_complete {
            self.estimated_work
                .cmp(&other.estimated_work)
                .then_with(|| self.simulated_frontier.cmp(&other.simulated_frontier))
                .then_with(|| self.confidence_rank.cmp(&other.confidence_rank))
                .then_with(|| self.stale_risk_rank.cmp(&other.stale_risk_rank))
                .then_with(|| self.hub_risk_rank.cmp(&other.hub_risk_rank))
                .then_with(|| self.frontier_capped.cmp(&other.frontier_capped))
                .then_with(|| self.source_rank.cmp(&other.source_rank))
        } else {
            self.anchor_cost
                .cmp(&other.anchor_cost)
                .then_with(|| self.source_rank.cmp(&other.source_rank))
        };
        primary.then_with(|| self.canonical_key.cmp(&other.canonical_key))
    }
}

impl PartialOrd for GraphRowPlanCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn graph_row_initial_edge_source_choice(
    edge: &GraphRowRuntimeEdge,
) -> GraphRowEdgeCandidateSourceChoice {
    if !edge.candidate_edge_ids.is_empty() {
        GraphRowEdgeCandidateSourceChoice::ExplicitIds
    } else if edge.filter.is_always_false()
        || edge
            .label_filter_ids
            .as_ref()
            .is_some_and(|label_ids| label_ids.is_empty())
    {
        GraphRowEdgeCandidateSourceChoice::EmptyResult
    } else {
        GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource
    }
}

fn graph_row_deterministic_fallback_edge_source_choice(
    edge: &GraphRowRuntimeEdge,
    edge_source_cost: Option<&(PlanCost, String, Vec<QueryPlanWarning>)>,
) -> GraphRowEdgeCandidateSourceChoice {
    if !edge.candidate_edge_ids.is_empty() {
        GraphRowEdgeCandidateSourceChoice::ExplicitIds
    } else if edge.filter.is_always_false()
        || edge
            .label_filter_ids
            .as_ref()
            .is_some_and(|label_ids| label_ids.is_empty())
    {
        GraphRowEdgeCandidateSourceChoice::EmptyResult
    } else if edge_source_cost.is_some() {
        GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource
    } else {
        GraphRowEdgeCandidateSourceChoice::EndpointAdjacency
    }
}

fn graph_row_plan_cost_rejection_reason(
    rejected: &GraphRowPlanCost,
    winner: &GraphRowPlanCost,
) -> &'static str {
    if rejected.fanout_complete && winner.fanout_complete {
        if rejected.estimated_work != winner.estimated_work {
            "estimated_work"
        } else if rejected.simulated_frontier != winner.simulated_frontier {
            "simulated_frontier"
        } else if rejected.confidence_rank != winner.confidence_rank {
            "confidence"
        } else if rejected.stale_risk_rank != winner.stale_risk_rank {
            "staleness"
        } else if rejected.hub_risk_rank != winner.hub_risk_rank {
            "hub_risk"
        } else if rejected.frontier_capped != winner.frontier_capped {
            "frontier_cap"
        } else if rejected.source_rank != winner.source_rank {
            "source_rank"
        } else {
            "canonical_key_tie_breaker"
        }
    } else if rejected.anchor_cost != winner.anchor_cost {
        graph_row_anchor_cost_rejection_reason(&rejected.anchor_cost, &winner.anchor_cost)
    } else if rejected.source_rank != winner.source_rank {
        "source_rank"
    } else {
        "canonical_key_tie_breaker"
    }
}

fn graph_row_anchor_cost_rejection_reason(
    rejected: &PlanCost,
    winner: &PlanCost,
) -> &'static str {
    if rejected.estimated_work != winner.estimated_work {
        "anchor_estimated_work"
    } else if rejected.estimated_candidates != winner.estimated_candidates {
        "anchor_estimated_candidates"
    } else if rejected.estimate_kind_rank != winner.estimate_kind_rank {
        "anchor_estimate_kind"
    } else if rejected.confidence_rank != winner.confidence_rank {
        "anchor_confidence"
    } else if rejected.stale_risk_rank != winner.stale_risk_rank {
        "anchor_staleness"
    } else if rejected.materialization_rank != winner.materialization_rank {
        "anchor_materialization"
    } else if rejected.source_rank != winner.source_rank {
        "anchor_source_rank"
    } else {
        "canonical_key_tie_breaker"
    }
}

fn graph_row_edge_plan_is_filter_source(plan: &EdgePhysicalPlan) -> bool {
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
                | EdgeQueryCandidateSourceKind::EdgeMetadataScan
        ),
        EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
            inputs.iter().all(graph_row_edge_plan_is_filter_source)
        }
        EdgePhysicalPlan::Empty => false,
    }
}

fn graph_row_edge_source_materialization_work(plan: &EdgePhysicalPlan) -> u64 {
    match plan {
        EdgePhysicalPlan::Empty | EdgePhysicalPlan::Source(_) | EdgePhysicalPlan::Union(_) => {
            plan.plan_cost().estimated_work
        }
        EdgePhysicalPlan::Intersect(inputs) => {
            let mut work = 16u64;
            let mut smallest_ready_set: Option<u64> = None;
            let mut materialized_any = false;
            for input in inputs {
                if smallest_ready_set
                    .is_some_and(|len| len <= GRAPH_ROW_EDGE_INTERSECTION_TINY_SET)
                    && graph_row_edge_plan_is_filter_source(input)
                {
                    continue;
                }
                materialized_any = true;
                work = work.saturating_add(graph_row_edge_source_materialization_work(input));
                if let Some(count) = input.estimate().known_upper_bound() {
                    smallest_ready_set = Some(
                        smallest_ready_set
                            .map(|current| current.min(count))
                            .unwrap_or(count),
                    );
                }
            }
            if materialized_any {
                work.min(plan.plan_cost().estimated_work)
            } else {
                plan.plan_cost().estimated_work
            }
        }
    }
}

impl PlanMaterializationClass {
    fn rank(self) -> u8 {
        match self {
            PlanMaterializationClass::Empty => 0,
            PlanMaterializationClass::Precomputed => 1,
            PlanMaterializationClass::KeyLookup => 2,
            PlanMaterializationClass::EagerIndex => 3,
            PlanMaterializationClass::Compound => 4,
            PlanMaterializationClass::StreamingLegalUniverse => 5,
        }
    }
}

impl NodeCandidateMaterialization {
    fn materialization_class(&self) -> PlanMaterializationClass {
        match self {
            NodeCandidateMaterialization::Precomputed(_) => PlanMaterializationClass::Precomputed,
            NodeCandidateMaterialization::KeyLookup => PlanMaterializationClass::KeyLookup,
            NodeCandidateMaterialization::PropertyEqualityIndex { .. }
            | NodeCandidateMaterialization::PropertyRangeIndex { .. }
            | NodeCandidateMaterialization::TimestampIndex { .. } => {
                PlanMaterializationClass::EagerIndex
            }
            NodeCandidateMaterialization::NodeLabelIndex { .. }
            | NodeCandidateMaterialization::NodeLabelAny { .. }
            | NodeCandidateMaterialization::FallbackNodeLabelScan { .. }
            | NodeCandidateMaterialization::FallbackFullNodeScan => {
                PlanMaterializationClass::StreamingLegalUniverse
            }
        }
    }
}

impl EdgeCandidateMaterialization {
    fn materialization_class(&self) -> PlanMaterializationClass {
        match self {
            EdgeCandidateMaterialization::Precomputed(_) => PlanMaterializationClass::Precomputed,
            EdgeCandidateMaterialization::EdgeTripleIndex { .. } => {
                PlanMaterializationClass::KeyLookup
            }
            EdgeCandidateMaterialization::EdgeWeightIndex { .. }
            | EdgeCandidateMaterialization::EdgeUpdatedAtIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidFromIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidToIndex { .. }
            | EdgeCandidateMaterialization::EdgePropertyEqualityIndex { .. }
            | EdgeCandidateMaterialization::EdgePropertyRangeIndex { .. } => {
                PlanMaterializationClass::EagerIndex
            }
            EdgeCandidateMaterialization::EdgeLabelIndex { .. }
            | EdgeCandidateMaterialization::FromEndpointAdjacency { .. }
            | EdgeCandidateMaterialization::ToEndpointAdjacency { .. }
            | EdgeCandidateMaterialization::AnyEndpointAdjacency { .. }
            | EdgeCandidateMaterialization::FallbackFullEdgeScan => {
                PlanMaterializationClass::StreamingLegalUniverse
            }
        }
    }
}

impl PlannerEstimate {
    fn exact_cheap(count: u64) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::ExactCheap,
            confidence: EstimateConfidence::Exact,
            stale_risk: StalePostingRisk::Low,
            proves_empty: count == 0,
            current_posting_bound: false,
        }
    }

    fn stats_exact(count: u64) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::StatsExact,
            confidence: EstimateConfidence::Exact,
            stale_risk: StalePostingRisk::Low,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn stats_estimated(count: u64, confidence: EstimateConfidence, stale_risk: StalePostingRisk) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::StatsEstimated,
            confidence,
            stale_risk,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn upper_bound(count: u64) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::UpperBound,
            confidence: EstimateConfidence::Medium,
            stale_risk: StalePostingRisk::Unknown,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn upper_bound_with_confidence(count: u64, confidence: EstimateConfidence) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::UpperBound,
            confidence,
            stale_risk: StalePostingRisk::Unknown,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn upper_bound_with_quality(
        count: u64,
        confidence: EstimateConfidence,
        stale_risk: StalePostingRisk,
    ) -> Self {
        Self {
            count: Some(count),
            kind: PlannerEstimateKind::UpperBound,
            confidence,
            stale_risk,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn unknown() -> Self {
        Self {
            count: None,
            kind: PlannerEstimateKind::Unknown,
            confidence: EstimateConfidence::Unknown,
            stale_risk: StalePostingRisk::Unknown,
            proves_empty: false,
            current_posting_bound: false,
        }
    }

    fn known_upper_bound(self) -> Option<u64> {
        self.count
    }

    fn proves_empty(self) -> bool {
        self.proves_empty
    }

    fn with_current_posting_bound(mut self) -> Self {
        self.current_posting_bound = true;
        self
    }

    fn can_use_uncapped_equality_materialization(self) -> bool {
        self.current_posting_bound
    }

    fn apply_cost_penalties(self, base_work: u64) -> u64 {
        if self.count.is_none() {
            return PLAN_COST_UNKNOWN_WORK;
        }
        base_work
    }
}

impl PlannedNodeCandidateSource {
    fn estimated_work(&self) -> u64 {
        let count = self.estimate.known_upper_bound().unwrap_or(u64::MAX);
        let (setup, candidate_weight) = match self.kind {
            NodeQueryCandidateSourceKind::ExplicitIds => (1u64, 1u64),
            NodeQueryCandidateSourceKind::KeyLookup => (2, 1),
            NodeQueryCandidateSourceKind::PropertyEqualityIndex => (8, 2),
            NodeQueryCandidateSourceKind::PropertyRangeIndex
            | NodeQueryCandidateSourceKind::TimestampIndex => (12, 2),
            NodeQueryCandidateSourceKind::NodeLabelIndex
            | NodeQueryCandidateSourceKind::FallbackNodeLabelScan => (24, 3),
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => (64, 4),
        };
        setup.saturating_add(count.saturating_mul(candidate_weight))
    }
}

impl PlannedNodeQuery {
    fn estimated_candidate_count(&self) -> Option<u64> {
        self.driver.estimate().known_upper_bound()
    }

    fn explain_plan(&self, public_inputs: QueryPlanPublicInputs) -> QueryPlan {
        QueryPlan {
            kind: QueryPlanKind::NodeQuery,
            root: QueryPlanNode::VerifyNodeFilter {
                input: Box::new(self.driver.plan_node()),
            },
            estimated_candidates: self.estimated_candidate_count(),
            warnings: self.warnings.clone(),
            notes: Vec::new(),
            public_inputs,
        }
    }
}

impl PlannedEdgeQuery {
    fn estimated_candidate_count(&self) -> Option<u64> {
        self.driver.estimate().known_upper_bound()
    }

    fn explain_plan(&self, public_inputs: QueryPlanPublicInputs) -> QueryPlan {
        let input = self.driver.plan_node();
        QueryPlan {
            kind: QueryPlanKind::EdgeQuery,
            root: QueryPlanNode::VerifyEdgeFilter {
                input: Box::new(input),
            },
            estimated_candidates: self.estimated_candidate_count(),
            warnings: self.warnings.clone(),
            notes: Vec::new(),
            public_inputs,
        }
    }
}

impl GraphRowFanoutEstimate {
    fn zero(canonical_key: String) -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: GraphRowHubRisk::Low,
            confidence: EstimateConfidence::Exact,
            coverage: GraphRowFanoutCoverage::Complete,
            canonical_key,
        }
    }

    fn unknown(canonical_key: String) -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: GraphRowHubRisk::Unknown,
            confidence: EstimateConfidence::Unknown,
            coverage: GraphRowFanoutCoverage::Missing,
            canonical_key,
        }
    }

    fn from_rollup(
        rollup: &crate::planner_stats::AdjacencyRollupStats,
        canonical_key: String,
        coverage: GraphRowFanoutCoverage,
        confidence: EstimateConfidence,
        known_source_ids: Option<&[u64]>,
    ) -> Self {
        let avg_upper_fanout = if rollup.source_node_count == 0 {
            0
        } else {
            rollup.total_edges.div_ceil(rollup.source_node_count)
        };
        let known_hub_fanout = known_source_ids.and_then(|ids| {
            rollup
                .top_hubs
                .iter()
                .filter(|hub| ids.binary_search(&hub.node_id).is_ok())
                .map(|hub| hub.count as u64)
                .max()
        });
        let hub_risk = if known_hub_fanout.is_some() {
            GraphRowHubRisk::High
        } else {
            let baseline = avg_upper_fanout.max(1);
            if (rollup.max_fanout as u64) >= baseline.saturating_mul(GRAPH_ROW_HUB_HIGH_RATIO) {
                GraphRowHubRisk::High
            } else if (rollup.p99_fanout as u64)
                >= baseline.saturating_mul(GRAPH_ROW_HUB_MEDIUM_RATIO)
            {
                GraphRowHubRisk::Medium
            } else {
                GraphRowHubRisk::Low
            }
        };
        let max_fanout = known_hub_fanout
            .unwrap_or(rollup.max_fanout as u64)
            .max(rollup.max_fanout as u64);
        Self {
            avg_upper_fanout,
            p99_fanout: rollup.p99_fanout as u64,
            max_fanout,
            hub_risk,
            confidence,
            coverage,
            canonical_key,
        }
    }

    fn combine_sum(self, other: Self, canonical_key: String) -> Self {
        Self {
            avg_upper_fanout: self
                .avg_upper_fanout
                .saturating_add(other.avg_upper_fanout),
            p99_fanout: self.p99_fanout.saturating_add(other.p99_fanout),
            max_fanout: self.max_fanout.saturating_add(other.max_fanout),
            hub_risk: higher_graph_row_hub_risk(self.hub_risk, other.hub_risk),
            confidence: weaker_confidence(self.confidence, other.confidence),
            coverage: worse_graph_row_fanout_coverage(self.coverage, other.coverage),
            canonical_key,
        }
    }

    fn cost_fanout(&self) -> u64 {
        if self.avg_upper_fanout == 0 && self.p99_fanout == 0 && self.max_fanout == 0 {
            return 0;
        }
        match self.hub_risk {
            GraphRowHubRisk::High => self
                .avg_upper_fanout
                .max(self.p99_fanout)
                .saturating_add(self.max_fanout / 2),
            GraphRowHubRisk::Medium => self
                .avg_upper_fanout
                .max(self.p99_fanout)
                .saturating_add(self.max_fanout / 4),
            GraphRowHubRisk::Low => self.avg_upper_fanout.max(1),
            GraphRowHubRisk::Unknown => GRAPH_ROW_FANOUT_UNKNOWN_WORK,
        }
    }

    fn complete(&self) -> bool {
        self.coverage.complete()
    }
}

impl ReadView {
    fn public_inputs_for_node_query(
        &self,
        query: &NodeQuery,
    ) -> Result<QueryPlanPublicInputs, EngineError> {
        let mut public_inputs = QueryPlanPublicInputs::default();
        if let Some(filter) = query.label_filter.as_ref() {
            for label in &filter.labels {
                let known = self
                    .label_catalog
                    .resolve_node_label_for_read(label)?
                    .is_some();
                public_inputs.node_labels.push(QueryPlanPublicName {
                    alias: None,
                    name: label.clone(),
                    known,
                    mode: Some(filter.mode),
                });
            }
        }
        Ok(public_inputs)
    }

    fn public_inputs_for_edge_query(
        &self,
        query: &EdgeQuery,
    ) -> Result<QueryPlanPublicInputs, EngineError> {
        let mut public_inputs = QueryPlanPublicInputs::default();
        if let Some(label) = query.label.as_ref() {
            let known = self
                .label_catalog
                .resolve_edge_label_for_read(label)?
                .is_some();
            public_inputs.edge_labels.push(QueryPlanPublicName {
                alias: None,
                name: label.clone(),
                known,
                mode: None,
            });
        }
        Ok(public_inputs)
    }

    fn add_node_label_filter_notes(
        notes: &mut Vec<QueryPlanNote>,
        filter: &ResolvedNodeLabelFilter,
        source_plan: Option<&NodePhysicalPlan>,
    ) {
        let ResolvedNodeLabelFilter::LabelSet {
            mode, label_ids, ..
        } = filter
        else {
            return;
        };
        let uses_label_postings = source_plan.is_some_and(NodePhysicalPlan::uses_label_postings);
        let uses_label_any_union = source_plan.is_some_and(NodePhysicalPlan::uses_label_any_union);
        match mode {
            LabelMatchMode::Any if label_ids.len() > 1 => {
                if uses_label_any_union {
                    notes.push(QueryPlanNote::NodeLabelAnyDedupeBeforePagination);
                }
                notes.push(QueryPlanNote::NodeLabelAnyFinalVerification);
                if uses_label_postings {
                    notes.push(QueryPlanNote::StaleNodeLabelMembershipVerification);
                }
            }
            LabelMatchMode::All if label_ids.len() > 1 => {
                notes.push(QueryPlanNote::NodeLabelAllSupersetVerification);
                if uses_label_postings {
                    notes.push(QueryPlanNote::StaleNodeLabelMembershipVerification);
                }
            }
            _ => {
                if uses_label_postings {
                    notes.push(QueryPlanNote::StaleNodeLabelMembershipVerification);
                }
            }
        }
    }

    fn node_query_explain_notes(
        query: &NormalizedNodeQuery,
        driver: &NodePhysicalPlan,
    ) -> Vec<QueryPlanNote> {
        let mut notes = Vec::new();
        Self::add_node_label_filter_notes(&mut notes, &query.label_filter, Some(driver));
        notes.sort_by_key(|note| match note {
            QueryPlanNote::NodeLabelAnyDedupeBeforePagination => 0,
            QueryPlanNote::NodeLabelAnyFinalVerification => 1,
            QueryPlanNote::NodeLabelAllSupersetVerification => 2,
            QueryPlanNote::StaleNodeLabelMembershipVerification => 3,
        });
        notes.dedup();
        notes
    }

    fn key_lookup_candidate_ids(
        &self,
        query: &NormalizedNodeQuery,
    ) -> Result<Vec<u64>, EngineError> {
        let label_id = query
            .single_label_id
            .expect("normalized key query must have single_label_id");
        let key_refs: Vec<(u32, &str)> = query
            .keys
            .iter()
            .map(|key| (label_id, key.as_str()))
            .collect();
        let ids = self
            .sources()
            .find_node_ids_by_label_keys(&key_refs)?
            .into_iter()
            .flatten()
            .collect();
        Ok(normalize_candidate_ids(ids))
    }

    fn equality_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        label_id: u32,
        key: &str,
        value: &PropValue,
    ) -> Result<CandidateProbe, EngineError> {
        let Some(entry) =
            self.node_property_index_entry(label_id, key, &SecondaryIndexKind::Equality)
        else {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        }

        let (estimate, followup) =
            self.equality_candidate_estimate(entry.index_id, key, value)?;
        let Some(estimate) = estimate else {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup,
            });
        };
        if cap_context.source_estimate_exceeds_cap(
            NodeQueryCandidateSourceKind::PropertyEqualityIndex,
            query.page.limit,
            estimate,
        ) {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::CandidateCapExceeded),
                followup,
            });
        }

        Ok(CandidateProbe {
            source: Some(PlannedNodeCandidateSource::property_equality_index(
                label_id,
                entry.index_id,
                key,
                value,
                estimate,
            )),
            warning: None,
            followup,
        })
    }

    fn range_candidate_estimate(
        &self,
        index_id: u64,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let lower_key = Self::encode_property_range_bound(lower);
        let upper_key = Self::encode_property_range_bound(upper);
        let mut count = self
            .memtable
            .visible_secondary_range_entry_count(
                index_id,
                lower_key,
                upper_key,
                None,
                self.snapshot_seq,
            ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                epoch
                    .memtable
                    .visible_secondary_range_entry_count(
                        index_id,
                        lower_key,
                        upper_key,
                        None,
                        self.snapshot_seq,
                    ) as u64,
            );
        }

        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.range_segment_estimate(
                index_id,
                segment.segment_id,
                lower_key,
                upper_key,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment.count_nodes_by_secondary_range_index_if_present(
                index_id,
                lower_key,
                upper_key,
            ) {
                Ok(Some(entries)) => count = count.saturating_add(entries as u64),
                Ok(None) => return Ok((None, self.range_sidecar_failure_followup(index_id, None))),
                Err(error) => {
                    return Ok((
                        None,
                        self.range_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            }
        }
        #[cfg(test)]
        if used_fallback {
            self.note_range_planning_probe();
        }

        let mut estimate = self.planner_stats_estimate_from_rollup(
            count,
            used_stats,
            used_fallback,
            stats_values_exact,
        );
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    #[allow(clippy::too_many_arguments)]
    fn range_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        label_id: u32,
        key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        budget: &mut BooleanPlanningBudget,
    ) -> Result<CandidateProbe, EngineError> {
        let validated = Self::validate_property_range_bounds(lower, upper, None)?;
        if validated.is_empty {
            return Ok(CandidateProbe {
                source: Some(PlannedNodeCandidateSource::with_ids(
                    NodeQueryCandidateSourceKind::PropertyRangeIndex,
                    format!("range_empty:{label_id}:{key}"),
                    Vec::new(),
                )),
                warning: None,
                followup: None,
            });
        }
        let Some(entry) =
            self.node_property_index_entry(label_id, key, &SecondaryIndexKind::Range)
        else {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        }

        let (estimate, followup) = self.range_candidate_estimate(entry.index_id, lower, upper)?;
        let Some(estimate) = estimate else {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup,
            });
        };
        if cap_context.source_estimate_exceeds_cap(
            NodeQueryCandidateSourceKind::PropertyRangeIndex,
            query.page.limit,
            estimate,
        ) {
            let probe_limit = budget.probe_limit();
            if probe_limit == 0 {
                return Ok(CandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::PlanningProbeBudgetExceeded),
                    followup: None,
                });
            }
            let (candidate_ids, followup) = self.ready_range_candidate_ids(
                entry.index_id,
                lower,
                upper,
                probe_limit.saturating_add(1),
            )?;
            let Some(candidate_ids) = candidate_ids else {
                return Ok(CandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::MissingReadyIndex),
                    followup,
                });
            };
            budget.consume_probe_ids(candidate_ids.len().min(probe_limit));
            if candidate_ids.len() <= probe_limit {
                let estimate = PlannerEstimate::exact_cheap(candidate_ids.len() as u64);
                return Ok(CandidateProbe {
                    source: Some(PlannedNodeCandidateSource::property_range_index(
                        entry.index_id,
                        key,
                        lower,
                        upper,
                        estimate,
                    )),
                    warning: None,
                    followup,
                });
            }
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::RangeCandidateCapExceeded),
                followup,
            });
        }
        Ok(CandidateProbe {
            source: Some(PlannedNodeCandidateSource::property_range_index(
                entry.index_id,
                key,
                lower,
                upper,
                estimate,
            )),
            warning: None,
            followup,
        })
    }

    fn timestamp_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        label_id: u32,
        lower_ms: i64,
        upper_ms: i64,
        budget: &mut BooleanPlanningBudget,
    ) -> Result<CandidateProbe, EngineError> {
        if let Some(stats_estimate) =
            self.planner_stats.timestamp_estimate(label_id, lower_ms, upper_ms)
        {
            let mut count = self
                .memtable
                .visible_nodes_by_time_range(label_id, lower_ms, upper_ms, self.snapshot_seq)
                .len() as u64;
            for epoch in &self.immutable_epochs {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .visible_nodes_by_time_range(
                            label_id,
                            lower_ms,
                            upper_ms,
                            self.snapshot_seq,
                        )
                        .len() as u64,
                );
            }
            count = count.saturating_add(stats_estimate.count);

            let mut used_fallback = false;
            let uncovered_segments: Vec<&SegmentReader> = self
                .segments
                .iter()
                .filter(|segment| {
                    !self
                        .planner_stats
                        .timestamp_covers_segment(label_id, segment.segment_id)
                })
                .map(|segment| segment.as_ref())
                .collect();
            if !uncovered_segments.is_empty() {
                used_fallback = true;
                let probe_limit = budget.probe_limit();
                if probe_limit == 0 {
                    return Ok(CandidateProbe {
                        source: None,
                        warning: Some(QueryPlanWarning::PlanningProbeBudgetExceeded),
                        followup: None,
                    });
                }

                #[cfg(test)]
                self.note_timestamp_planning_probe();

                let mut fallback_ids = 0usize;
                let total_read_limit = probe_limit.saturating_add(1);
                for segment in uncovered_segments {
                    if fallback_ids >= total_read_limit {
                        break;
                    }
                    let flow = segment.for_each_node_by_time_range(
                        label_id,
                        lower_ms,
                        upper_ms,
                        |_| {
                            fallback_ids = fallback_ids.saturating_add(1);
                            if fallback_ids >= total_read_limit {
                                ControlFlow::Break(())
                            } else {
                                ControlFlow::Continue(())
                            }
                        },
                    )?;
                    if flow.is_break() {
                        break;
                    }
                }
                budget.consume_probe_ids(fallback_ids);
                if fallback_ids > probe_limit {
                    return Ok(CandidateProbe {
                        source: None,
                        warning: Some(if probe_limit < QUERY_RANGE_CANDIDATE_CAP {
                            QueryPlanWarning::PlanningProbeBudgetExceeded
                        } else {
                            QueryPlanWarning::TimestampCandidateCapExceeded
                        }),
                        followup: None,
                    });
                }
                count = count.saturating_add(fallback_ids as u64);
            }

            let estimate = if self.active_memtable_only_exact_estimates() {
                PlannerEstimate::exact_cheap(count)
            } else {
                self.planner_stats_estimate_from_rollup(
                    count,
                    true,
                    used_fallback,
                    stats_estimate.exact,
                )
            };
            if cap_context.source_estimate_exceeds_cap(
                NodeQueryCandidateSourceKind::TimestampIndex,
                query.page.limit,
                estimate,
            ) {
                return Ok(CandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::TimestampCandidateCapExceeded),
                    followup: None,
                });
            }

            return Ok(CandidateProbe {
                source: Some(PlannedNodeCandidateSource::timestamp_index(
                    label_id, lower_ms, upper_ms, estimate,
                )),
                warning: None,
                followup: None,
            });
        }

        #[cfg(test)]
        self.note_timestamp_planning_probe();
        let probe_limit = budget.probe_limit();
        if probe_limit == 0 {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::PlanningProbeBudgetExceeded),
                followup: None,
            });
        }
        let ids = self.timestamp_candidate_ids(
            label_id,
            lower_ms,
            upper_ms,
            probe_limit + 1,
        )?;
        if ids.len() <= probe_limit {
            budget.consume_probe_ids(ids.len());
            Ok(CandidateProbe {
                source: Some(PlannedNodeCandidateSource::timestamp_index(
                    label_id,
                    lower_ms,
                    upper_ms,
                    PlannerEstimate::exact_cheap(ids.len() as u64),
                )),
                warning: None,
                followup: None,
            })
        } else {
            budget.consume_probe_ids(ids.len().min(probe_limit + 1));
            Ok(CandidateProbe {
                source: None,
                warning: Some(if probe_limit < QUERY_RANGE_CANDIDATE_CAP {
                    QueryPlanWarning::PlanningProbeBudgetExceeded
                } else {
                    QueryPlanWarning::TimestampCandidateCapExceeded
                }),
                followup: None,
            })
        }
    }

    fn planner_stats_exact_safe_for_single_segment(&self) -> bool {
        self.memtable.is_empty()
            && self.immutable_epochs.is_empty()
            && self.segments.len() == 1
            && !self.segments[0].has_tombstones()
            && self.manifest.prune_policies.is_empty()
    }

    fn active_memtable_only_exact_estimates(&self) -> bool {
        self.immutable_epochs.is_empty()
            && self.segments.is_empty()
            && self.manifest.prune_policies.is_empty()
    }

    fn planner_stats_estimate_from_rollup(
        &self,
        count: u64,
        used_stats: bool,
        used_fallback: bool,
        stats_values_exact: bool,
    ) -> PlannerEstimate {
        if used_stats && !used_fallback {
            if stats_values_exact && self.planner_stats_exact_safe_for_single_segment() {
                return PlannerEstimate::stats_exact(count);
            }
            let has_mutable_sources = !self.memtable.is_empty() || !self.immutable_epochs.is_empty();
            let confidence = if !self.manifest.prune_policies.is_empty() {
                EstimateConfidence::Low
            } else if has_mutable_sources {
                EstimateConfidence::Medium
            } else {
                EstimateConfidence::High
            };
            let stale_risk = if !self.manifest.prune_policies.is_empty() {
                StalePostingRisk::High
            } else if has_mutable_sources {
                StalePostingRisk::Medium
            } else {
                self.planner_stats.max_segment_stale_risk()
            };
            return PlannerEstimate::stats_estimated(count, confidence, stale_risk);
        }
        PlannerEstimate::upper_bound(count)
    }

    fn node_label_estimate(&self, label_id: u32) -> Result<PlannerEstimate, EngineError> {
        let mut count = self
            .memtable
            .visible_nodes_by_label_id_count(label_id, self.snapshot_seq) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok(PlannerEstimate::exact_cheap(count));
        }
        for epoch in &self.immutable_epochs {
            count += epoch
                .memtable
                .visible_nodes_by_label_id_count(label_id, self.snapshot_seq) as u64;
        }
        count = count.saturating_add(self.planner_stats.node_label_count(label_id));
        let mut used_fallback = self.planner_stats.node_label_coverage.has_uncovered();
        for segment in &self.segments {
            if self.planner_stats.node_label_coverage.covers(segment.segment_id) {
                continue;
            }
            used_fallback = true;
            count = count.saturating_add(segment.node_label_posting_count(label_id)? as u64);
        }
        Ok(self.planner_stats_estimate_from_rollup(
            count,
            self.planner_stats.node_label_coverage.covered_count() > 0,
            used_fallback,
            true,
        ))
    }

    #[allow(dead_code)]
    fn node_label_filter_estimate(
        &self,
        label_ids: &NodeLabelSet,
        mode: LabelMatchMode,
    ) -> Result<NodeLabelMembershipEstimate, EngineError> {
        if label_ids.len() == 1 {
            let label_id = label_ids.single_label_id();
            return Ok(NodeLabelMembershipEstimate {
                estimate: self.node_label_estimate(label_id)?,
                driver_label_id: Some(label_id),
            });
        }

        match mode {
            LabelMatchMode::Any => {
                let mut count = 0u64;
                let mut confidence = EstimateConfidence::Exact;
                let mut stale_risk = StalePostingRisk::Low;
                for &label_id in label_ids.as_slice() {
                    let estimate = self.node_label_estimate(label_id)?;
                    let Some(label_count) = estimate.known_upper_bound() else {
                        return Ok(NodeLabelMembershipEstimate {
                            estimate: PlannerEstimate::unknown(),
                            driver_label_id: None,
                        });
                    };
                    count = count.saturating_add(label_count);
                    confidence = weaker_confidence(confidence, estimate.confidence);
                    stale_risk = higher_stale_posting_risk(stale_risk, estimate.stale_risk);
                }
                Ok(NodeLabelMembershipEstimate {
                    estimate: PlannerEstimate::upper_bound_with_quality(
                        count,
                        confidence,
                        stale_risk,
                    ),
                    driver_label_id: None,
                })
            }
            LabelMatchMode::All => {
                let mut best: Option<(u64, u32)> = None;
                let mut confidence = EstimateConfidence::Exact;
                let mut stale_risk = StalePostingRisk::Low;
                for &label_id in label_ids.as_slice() {
                    let estimate = self.node_label_estimate(label_id)?;
                    confidence = weaker_confidence(confidence, estimate.confidence);
                    stale_risk = higher_stale_posting_risk(stale_risk, estimate.stale_risk);
                    let Some(label_count) = estimate.known_upper_bound() else {
                        continue;
                    };
                    let better = match best {
                        Some((best_count, best_label_id)) => {
                            label_count < best_count
                                || (label_count == best_count && label_id < best_label_id)
                        }
                        None => true,
                    };
                    if better {
                        best = Some((label_count, label_id));
                    }
                }
                let Some((count, driver_label_id)) = best else {
                    return Ok(NodeLabelMembershipEstimate {
                        estimate: PlannerEstimate::unknown(),
                        driver_label_id: None,
                    });
                };
                Ok(NodeLabelMembershipEstimate {
                    estimate: PlannerEstimate::upper_bound_with_quality(
                        count,
                        confidence,
                        stale_risk,
                    ),
                    driver_label_id: Some(driver_label_id),
                })
            }
        }
    }

    fn full_scan_estimate(&self) -> PlannerEstimate {
        let mut count = self.memtable.visible_node_count_at(self.snapshot_seq) as u64;
        if self.active_memtable_only_exact_estimates() {
            return PlannerEstimate::exact_cheap(count);
        }
        for epoch in &self.immutable_epochs {
            count = count
                .saturating_add(epoch.memtable.visible_node_count_at(self.snapshot_seq) as u64);
        }
        count = count.saturating_add(self.planner_stats.full_rollup.node_count);
        let mut used_fallback = self.planner_stats.full_rollup.coverage.has_uncovered();
        for segment in &self.segments {
            if self
                .planner_stats
                .full_rollup
                .coverage
                .covers(segment.segment_id)
            {
                continue;
            }
            used_fallback = true;
            count = count.saturating_add(segment.node_count());
        }
        self.planner_stats_estimate_from_rollup(
            count,
            self.planner_stats.full_rollup.coverage.covered_count() > 0,
            used_fallback,
            true,
        )
    }

    fn edge_full_scan_estimate(&self) -> PlannerEstimate {
        let mut count = self.memtable.edge_count() as u64;
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(epoch.memtable.edge_count() as u64);
        }
        for segment in &self.segments {
            count = count.saturating_add(segment.edge_count());
        }
        PlannerEstimate::upper_bound(count)
    }

    fn edge_label_estimate(&self, label_id: u32) -> PlannerEstimate {
        let mut count =
            self.memtable.visible_edges_by_label_id_count(label_id, self.snapshot_seq) as u64;
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                epoch
                    .memtable
                    .visible_edges_by_label_id_count(label_id, self.snapshot_seq)
                    as u64,
            );
        }
        for segment in &self.segments {
            let Ok(segment_count) = segment.edge_label_posting_count(label_id) else {
                return self.edge_full_scan_estimate();
            };
            count = count.saturating_add(segment_count as u64);
        }
        PlannerEstimate::upper_bound(count)
    }

    fn edge_metadata_source_estimate(&self, label_id: Option<u32>) -> PlannerEstimate {
        label_id
            .map(|label_id| self.edge_label_estimate(label_id))
            .unwrap_or_else(|| self.edge_full_scan_estimate())
    }

    fn edge_weight_range_estimate(
        &self,
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<f32>,
        indexed: bool,
    ) -> PlannerEstimate {
        if !indexed {
            return self.edge_metadata_source_estimate(label_id);
        }
        let mut count = 0u64;
        let add_memtable = |memtable: &Memtable| {
            let mut local = 0u64;
            let _ = memtable.for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::weight_matches_bounds(meta.weight, bounds)
                {
                    local = local.saturating_add(1);
                }
                ControlFlow::Continue(())
            });
            local
        };
        count = count.saturating_add(add_memtable(&self.memtable));
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(add_memtable(&epoch.memtable));
        }
        for segment in &self.segments {
            let Some(segment_count) = segment.edge_weight_range_count(label_id, bounds) else {
                return self.edge_metadata_source_estimate(label_id);
            };
            count = count.saturating_add(segment_count as u64);
        }
        PlannerEstimate::upper_bound(count)
    }

    fn edge_i64_metadata_range_estimate(
        &self,
        label_id: Option<u32>,
        bounds: crate::edge_metadata::RangeBoundFlags<i64>,
        indexed: bool,
        memtable_value: impl Fn(EdgeMetadataCandidate) -> i64,
        segment_count: impl Fn(&SegmentReader) -> Option<usize>,
    ) -> PlannerEstimate {
        if !indexed {
            return self.edge_metadata_source_estimate(label_id);
        }
        let mut count = 0u64;
        let add_memtable = |memtable: &Memtable| {
            let mut local = 0u64;
            let _ = memtable.for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::i64_matches_bounds(memtable_value(meta), bounds)
                {
                    local = local.saturating_add(1);
                }
                ControlFlow::Continue(())
            });
            local
        };
        count = count.saturating_add(add_memtable(&self.memtable));
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(add_memtable(&epoch.memtable));
        }
        for segment in &self.segments {
            let Some(segment_count) = segment_count(segment) else {
                return self.edge_metadata_source_estimate(label_id);
            };
            count = count.saturating_add(segment_count as u64);
        }
        PlannerEstimate::upper_bound(count)
    }

    fn edge_endpoint_estimate(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
    ) -> PlannerEstimate {
        if node_ids.is_empty() {
            return PlannerEstimate::exact_cheap(0);
        }
        let mut sorted_node_ids = node_ids.to_vec();
        sorted_node_ids.sort_unstable();
        sorted_node_ids.dedup();

        let mut count = 0u64;
        let mut confidence = EstimateConfidence::Medium;
        let mut add_memtable_estimate =
            |estimate: crate::memtable::MemtableEndpointCountEstimate| {
                count = count.saturating_add(estimate.count as u64);
                if !estimate.exact {
                    confidence = weaker_confidence(confidence, EstimateConfidence::Low);
                }
            };
        for &node_id in &sorted_node_ids {
            match direction {
                Direction::Outgoing => {
                    add_memtable_estimate(
                        self.memtable
                            .visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                    );
                    for epoch in &self.immutable_epochs {
                        add_memtable_estimate(
                            epoch.memtable.visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                        );
                    }
                }
                Direction::Incoming => {
                    add_memtable_estimate(
                        self.memtable
                            .visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                    );
                    for epoch in &self.immutable_epochs {
                        add_memtable_estimate(
                            epoch.memtable.visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                        );
                    }
                }
                Direction::Both => {
                    add_memtable_estimate(
                        self.memtable
                            .visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                    );
                    add_memtable_estimate(
                        self.memtable
                            .visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                    );
                    for epoch in &self.immutable_epochs {
                        add_memtable_estimate(
                            epoch.memtable.visible_edges_from_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                        );
                        add_memtable_estimate(
                            epoch.memtable.visible_edges_to_endpoint_count_estimate(
                                node_id,
                                label_filter_ids,
                                self.snapshot_seq,
                            ),
                        );
                    }
                }
            }
        }

        for segment in &self.segments {
            match segment.endpoint_adj_posting_count(&sorted_node_ids, direction, label_filter_ids) {
                Ok(segment_count) => count = count.saturating_add(segment_count as u64),
                Err(_) => return self.edge_full_scan_estimate(),
            }
        }

        PlannerEstimate::upper_bound_with_confidence(count, confidence)
    }

    fn edge_metadata_sidecar_availability(&self) -> EdgeMetadataSidecarAvailability {
        let mut has_nonempty_segment = false;
        let mut weight = true;
        let mut updated_at = true;
        let mut valid_from = true;
        let mut valid_to = true;

        for segment in &self.segments {
            if segment.edge_count() == 0 {
                continue;
            }
            has_nonempty_segment = true;
            weight &= segment.edge_weight_index_available();
            updated_at &= segment.edge_updated_at_index_available();
            valid_from &= segment.edge_valid_from_index_available();
            valid_to &= segment.edge_valid_to_index_available();
        }

        EdgeMetadataSidecarAvailability {
            weight: has_nonempty_segment && weight,
            updated_at: has_nonempty_segment && updated_at,
            valid_from: has_nonempty_segment && valid_from,
            valid_to: has_nonempty_segment && valid_to,
        }
    }

    fn graph_row_known_node_ids_for_fanout(
        &self,
        node: &GraphRowRuntimeNode,
    ) -> Option<Vec<u64>> {
        if node.query.ids.is_empty() {
            None
        } else {
            Some(node.query.ids.clone())
        }
    }

    fn graph_row_has_unrolled_memtable_edges(&self) -> bool {
        self.memtable.edge_count() > 0
            || self
                .immutable_epochs
                .iter()
                .any(|epoch| epoch.memtable.edge_count() > 0)
    }

    fn graph_row_planner_directions(direction: Direction) -> &'static [PlannerStatsDirection] {
        match direction {
            Direction::Outgoing => &[PlannerStatsDirection::Outgoing],
            Direction::Incoming => &[PlannerStatsDirection::Incoming],
            Direction::Both => &[
                PlannerStatsDirection::Outgoing,
                PlannerStatsDirection::Incoming,
            ],
        }
    }

    fn graph_row_fanout_rollup_estimate(
        &self,
        direction: PlannerStatsDirection,
        edge_label_id: Option<u32>,
        coverage: GraphRowFanoutCoverage,
        confidence: EstimateConfidence,
        canonical_key: String,
        known_source_ids: Option<&[u64]>,
    ) -> Option<GraphRowFanoutEstimate> {
        let rollup = self
            .planner_stats
            .adjacency_rollups
            .get(&(direction, edge_label_id))?;
        let coverage = if self.graph_row_has_unrolled_memtable_edges()
            || rollup.coverage.has_uncovered()
        {
            GraphRowFanoutCoverage::GlobalFallback
        } else {
            coverage
        };
        let mut confidence = confidence;
        if self.graph_row_has_unrolled_memtable_edges() {
            confidence = downgrade_confidence(confidence, GRAPH_ROW_CONFIDENCE_DOWNGRADE_STEP);
        }
        Some(GraphRowFanoutEstimate::from_rollup(
            rollup,
            canonical_key,
            coverage,
            confidence,
            known_source_ids,
        ))
    }

    fn graph_row_single_direction_fanout_estimate(
        &self,
        direction: PlannerStatsDirection,
        label_filter_ids: Option<&[u32]>,
        known_source_ids: Option<&[u64]>,
    ) -> GraphRowFanoutEstimate {
        let direction_key = match direction {
            PlannerStatsDirection::Outgoing => "out",
            PlannerStatsDirection::Incoming => "in",
        };
        let Some(label_ids) = label_filter_ids else {
            return self
                .graph_row_fanout_rollup_estimate(
                    direction,
                    None,
                    GraphRowFanoutCoverage::Complete,
                    EstimateConfidence::High,
                    format!("{direction_key}:global"),
                    known_source_ids,
                )
                .unwrap_or_else(|| {
                    GraphRowFanoutEstimate::unknown(format!("{direction_key}:global"))
                });
        };

        if label_ids.is_empty() {
            return GraphRowFanoutEstimate::zero(format!("{direction_key}:empty-label-filter"));
        }

        if label_ids.len() == 1 {
            let label_id = label_ids[0];
            if let Some(estimate) = self.graph_row_fanout_rollup_estimate(
                direction,
                Some(label_id),
                GraphRowFanoutCoverage::Complete,
                EstimateConfidence::High,
                format!("{direction_key}:label:{label_id}"),
                known_source_ids,
            ) {
                return estimate;
            }
            if !self.graph_row_has_unrolled_memtable_edges()
                && self
                    .planner_stats
                    .adjacency_rollups
                    .get(&(direction, None))
                    .is_some_and(|rollup| !rollup.coverage.has_uncovered())
            {
                return GraphRowFanoutEstimate::zero(format!(
                    "{direction_key}:label:{label_id}:absent"
                ));
            }
            return self
                .graph_row_fanout_rollup_estimate(
                    direction,
                    None,
                    GraphRowFanoutCoverage::GlobalFallback,
                    EstimateConfidence::Low,
                    format!("{direction_key}:global_fallback:{label_id}"),
                    known_source_ids,
                )
                .unwrap_or_else(|| {
                    GraphRowFanoutEstimate::unknown(format!("{direction_key}:missing:{label_id}"))
                });
        }

        let mut combined: Option<GraphRowFanoutEstimate> = None;
        for label_id in label_ids {
            let Some(estimate) = self.graph_row_fanout_rollup_estimate(
                direction,
                Some(*label_id),
                GraphRowFanoutCoverage::Complete,
                EstimateConfidence::High,
                format!("{direction_key}:label:{label_id}"),
                known_source_ids,
            ) else {
                return self
                    .graph_row_fanout_rollup_estimate(
                        direction,
                        None,
                        GraphRowFanoutCoverage::GlobalFallback,
                        EstimateConfidence::Low,
                        format!("{direction_key}:global_fallback:{label_ids:?}"),
                        known_source_ids,
                    )
                    .unwrap_or_else(|| {
                        GraphRowFanoutEstimate::unknown(format!(
                            "{direction_key}:missing:{label_ids:?}"
                        ))
                    });
            };
            combined = Some(match combined {
                Some(current) => current.combine_sum(
                    estimate,
                    format!("{direction_key}:labels:{label_ids:?}"),
                ),
                None => estimate,
            });
        }

        combined.unwrap_or_else(|| {
            GraphRowFanoutEstimate::unknown(format!("{direction_key}:empty"))
        })
    }

    fn graph_row_edge_fanout_estimate(
        &self,
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        known_source_ids: Option<&[u64]>,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
    ) -> GraphRowFanoutEstimate {
        let mut combined: Option<GraphRowFanoutEstimate> = None;
        for planner_direction in Self::graph_row_planner_directions(direction) {
            let estimate = self.graph_row_single_direction_fanout_estimate(
                *planner_direction,
                label_filter_ids,
                known_source_ids,
            );
            combined = Some(match combined {
                Some(current) => current.combine_sum(
                    estimate,
                    format!("{direction:?}:{:?}", label_filter_ids.unwrap_or(&[])),
                ),
                None => estimate,
            });
        }
        let mut estimate = combined
            .unwrap_or_else(|| GraphRowFanoutEstimate::unknown(format!("{direction:?}:missing")));
        let mut downgrade_steps = 0u8;
        if query.at_epoch.is_some() {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if !self.manifest.prune_policies.is_empty() {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if edge_filter_requires_hydration(&edge.filter) {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if downgrade_steps > 0 {
            estimate.confidence = downgrade_confidence(estimate.confidence, downgrade_steps);
        }
        estimate
    }

    fn graph_row_target_selectivity(
        &self,
        node: &GraphRowRuntimeNode,
    ) -> Option<(u64, u64)> {
        if !graph_row_node_query_has_anchor(&node.query) || node.query.filter.is_always_false() {
            return None;
        }
        let planned = self.plan_normalized_node_query(&node.query).ok()?;
        let target_count = planned.estimated_candidate_count()?;
        let universe_count = self.full_scan_estimate().known_upper_bound()?;
        if universe_count == 0 || target_count >= universe_count {
            None
        } else {
            Some((target_count, universe_count))
        }
    }

    fn apply_graph_row_target_selectivity(
        raw_expansion: u64,
        target_selectivity: Option<(u64, u64)>,
    ) -> u64 {
        let Some((target_count, universe_count)) = target_selectivity else {
            return raw_expansion;
        };
        if universe_count == 0 {
            return raw_expansion;
        }
        raw_expansion
            .saturating_mul(target_count)
            .div_ceil(universe_count)
            .max(1)
            .min(raw_expansion)
    }

    fn graph_row_edge_source_plan_cost(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
    ) -> Result<Option<(PlanCost, String, Vec<QueryPlanWarning>)>, EngineError> {
        if edge.filter.is_always_false()
            || edge
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.is_empty())
        {
            let cost = EdgePhysicalPlan::Empty.plan_cost();
            return Ok(Some((
                cost,
                "source=EmptyResult; reason=always_false_or_empty_label_filter".to_string(),
                Vec::new(),
            )));
        }

        let label_branches: Vec<Option<u32>> = match edge.label_filter_ids.as_deref() {
            Some(label_ids) => label_ids.iter().copied().map(Some).collect(),
            None => vec![None],
        };
        let mut drivers = Vec::new();
        let mut warnings = Vec::new();
        let mut details = Vec::new();
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
                    limit: Some(query.options.max_frontier.saturating_add(1)),
                    after: None,
                },
                warnings: Vec::new(),
            };
            let planned = match self.plan_normalized_edge_query(&normalized) {
                Ok(planned) => planned,
                Err(EngineError::InvalidOperation(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            for warning in &planned.warnings {
                push_query_warning(&mut warnings, *warning);
            }
            details.push(format!(
                "label_id={label_id:?}; source={:?}; estimated_candidates={:?}",
                planned.driver.plan_node(),
                planned.estimated_candidate_count()
            ));
            drivers.push(planned.driver);
        }

        if drivers.is_empty() {
            return Ok(None);
        }
        let driver = EdgePhysicalPlan::union(drivers);
        let mut cost = driver.plan_cost();
        cost.estimated_work = graph_row_edge_source_materialization_work(&driver);
        finalize_plan_warnings(&mut warnings);
        Ok(Some((
            cost,
            format!("source=EdgeCandidateSource; {}", details.join(" | ")),
            warnings,
        )))
    }

    fn plan_graph_row_physical(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
    ) -> Result<GraphRowPhysicalPlan, EngineError> {
        let known_node_ids = runtime
            .nodes
            .iter()
            .map(|node| self.graph_row_known_node_ids_for_fanout(node))
            .collect::<Vec<_>>();
        let target_selectivities = runtime
            .nodes
            .iter()
            .map(|node| self.graph_row_target_selectivity(node))
            .collect::<Vec<_>>();
        let edge_source_costs = runtime
            .edges
            .iter()
            .map(|edge| self.graph_row_edge_source_plan_cost(query, edge))
            .collect::<Result<Vec<_>, _>>()?;

        if runtime.edges.is_empty() {
            let initial_driver = if runtime.nodes.len() == 1 {
                GraphRowInitialDriver::Node {
                    node_index: 0,
                    alias: runtime.nodes[0].alias.clone(),
                }
            } else {
                GraphRowInitialDriver::Empty {
                    reason: "no required fixed edge segment".to_string(),
                }
            };
            return Ok(GraphRowPhysicalPlan {
                initial_driver,
                edge_order: Vec::new(),
                segments: Vec::new(),
                edge_source_choices: Vec::new(),
                alternatives: Vec::new(),
                notes: self.graph_row_physical_plan_notes(query),
            });
        }

        let fallback_segment;
        let required_segments = if runtime.required_segments.is_empty() {
            fallback_segment = GraphRowRequiredSegment {
                edge_indices: (0..runtime.edges.len()).collect(),
                barriers_before: Vec::new(),
            };
            std::slice::from_ref(&fallback_segment)
        } else {
            runtime.required_segments.as_slice()
        };

        let mut initial_driver = GraphRowInitialDriver::Empty {
            reason: "no required fixed edge segment".to_string(),
        };
        let mut edge_order = Vec::with_capacity(runtime.edges.len());
        let mut segments = Vec::with_capacity(required_segments.len());
        let mut edge_source_choices = vec![None; runtime.edges.len()];
        let mut alternatives = Vec::new();

        for (segment_index, segment) in required_segments.iter().enumerate() {
            let planned = self.plan_graph_row_physical_segment(
                query,
                runtime,
                segment,
                segment_index,
                &edge_source_costs,
                &known_node_ids,
                &target_selectivities,
            )?;
            if segment_index == 0 {
                initial_driver = planned.segment.initial_driver.clone();
            }
            for (edge_index, choice) in &planned.source_choices {
                if let Some(slot) = edge_source_choices.get_mut(*edge_index) {
                    *slot = Some(*choice);
                }
            }
            edge_order.extend(planned.segment.edge_order.iter().copied());
            alternatives.extend(planned.alternatives);
            segments.push(planned.segment);
        }

        Ok(GraphRowPhysicalPlan {
            initial_driver,
            edge_order,
            segments,
            edge_source_choices,
            alternatives,
            notes: self.graph_row_physical_plan_notes(query),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_graph_row_physical_segment(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        segment: &GraphRowRequiredSegment,
        segment_index: usize,
        edge_source_costs: &[GraphRowEdgeSourcePlanCost],
        known_node_ids: &[Option<Vec<u64>>],
        target_selectivities: &[Option<(u64, u64)>],
    ) -> Result<GraphRowPhysicalSegmentPlan, EngineError> {
        let mut alternatives = Vec::new();
        let mut candidates = Vec::new();
        let mut segment_node_aliases = BTreeSet::new();
        for &edge_index in &segment.edge_indices {
            let edge = &runtime.edges[edge_index];
            segment_node_aliases.insert(edge.from_alias.as_str());
            segment_node_aliases.insert(edge.to_alias.as_str());
        }

        for (node_index, node) in runtime.nodes.iter().enumerate() {
            if !segment_node_aliases.contains(node.alias.as_str()) {
                continue;
            }
            if !graph_row_node_query_has_anchor(&node.query) {
                alternatives.push(GraphRowPlanAlternative {
                    chosen: false,
                    kind: "RejectedNodeAnchor".to_string(),
                    detail: format!(
                        "segment={segment_index}; alias={}; reason=no legal initial candidate source",
                        node.alias
                    ),
                    decision: None,
                    cost: None,
                });
                continue;
            }
            let mut anchor_query = node.query.clone();
            anchor_query.page.limit =
                Some(query.options.max_intermediate_bindings.saturating_add(1));
            match self.plan_normalized_node_query(&anchor_query) {
                Ok(planned) => {
                    let mut bound = BTreeSet::new();
                    bound.insert(node.alias.clone());
                    let visited = vec![false; runtime.edges.len()];
                    let anchor_cost = planned.driver.plan_cost();
                    let initial_frontier = planned.estimated_candidate_count().unwrap_or(1).max(1);
                    let (edge_order, source_choices, cost) = self.graph_row_plan_from_frontier(
                        query,
                        runtime,
                        &segment.edge_indices,
                        bound,
                        visited,
                        anchor_cost,
                        initial_frontier,
                        edge_source_costs,
                        known_node_ids,
                        target_selectivities,
                        format!("node:{}:{node_index}", node.alias),
                    )?;
                    candidates.push((
                        GraphRowInitialDriver::Node {
                            node_index,
                            alias: node.alias.clone(),
                        },
                        edge_order,
                        source_choices,
                        cost,
                        "NodeAnchor".to_string(),
                        format!(
                            "segment={segment_index}; alias={}; source={:?}; estimated_candidates={:?}; warnings={:?}",
                            node.alias,
                            planned.driver.plan_node(),
                            planned.estimated_candidate_count(),
                            planned.warnings
                        ),
                    ));
                }
                Err(error) => alternatives.push(GraphRowPlanAlternative {
                    chosen: false,
                    kind: "RejectedNodeAnchor".to_string(),
                    detail: format!("segment={segment_index}; alias={}; reason={error}", node.alias),
                    decision: None,
                    cost: None,
                }),
            }
        }

        for &edge_index in &segment.edge_indices {
            let edge = &runtime.edges[edge_index];
            let Some((anchor_cost, source_detail, warnings)) =
                edge_source_costs[edge_index].clone()
            else {
                alternatives.push(GraphRowPlanAlternative {
                    chosen: false,
                    kind: "RejectedEdgeAnchor".to_string(),
                    detail: format!(
                        "segment={segment_index}; edge={}; reason=no legal unbound edge candidate source without full scan opt-in",
                        edge.explain_name()
                    ),
                    decision: None,
                    cost: None,
                });
                continue;
            };
            let mut bound = BTreeSet::new();
            bound.insert(edge.from_alias.clone());
            bound.insert(edge.to_alias.clone());
            let mut visited = vec![false; runtime.edges.len()];
            visited[edge_index] = true;
            let initial_frontier = anchor_cost.estimated_candidates.unwrap_or(1).max(1);
            let (mut edge_order, mut source_choices, cost) = self.graph_row_plan_from_frontier(
                query,
                runtime,
                &segment.edge_indices,
                bound,
                visited,
                anchor_cost,
                initial_frontier,
                edge_source_costs,
                known_node_ids,
                target_selectivities,
                format!("edge:{}:{edge_index}", edge.explain_name()),
            )?;
            edge_order.insert(0, edge_index);
            source_choices.insert(0, (edge_index, graph_row_initial_edge_source_choice(edge)));
            candidates.push((
                GraphRowInitialDriver::Edge {
                    edge_index,
                    edge_name: edge.explain_name(),
                },
                edge_order,
                source_choices,
                cost,
                "EdgeAnchor".to_string(),
                format!(
                    "segment={segment_index}; edge={}; {source_detail}; warnings={warnings:?}",
                    edge.explain_name()
                ),
            ));
        }

        if candidates.is_empty() {
            let edge_order = segment.edge_indices.clone();
            let source_choices = edge_order
                .iter()
                .map(|edge_index| {
                    (
                        *edge_index,
                        graph_row_deterministic_fallback_edge_source_choice(
                            &runtime.edges[*edge_index],
                            edge_source_costs[*edge_index].as_ref(),
                        ),
                    )
                })
                .collect::<Vec<_>>();
            alternatives.push(GraphRowPlanAlternative {
                chosen: true,
                kind: "DeterministicFallback".to_string(),
                detail: format!("segment={segment_index}; no legal early node or edge anchor; execution keeps query-order required edges and will enforce normal full-scan/cap rules"),
                decision: Some("decision=chosen_deterministic_fallback".to_string()),
                cost: None,
            });
            return Ok(GraphRowPhysicalSegmentPlan {
                segment: GraphRowPhysicalSegment {
                    segment_index,
                    barriers_before: segment.barriers_before.clone(),
                    initial_driver: GraphRowInitialDriver::Empty {
                        reason: "deterministic query-order fallback".to_string(),
                    },
                    edge_order,
                },
                source_choices,
                alternatives,
            });
        }

        candidates.sort_by(|left, right| left.3.cmp(&right.3));
        let chosen_cost = candidates
            .first()
            .map(|candidate| candidate.3.clone())
            .expect("candidates must be non-empty");
        let (initial_driver, edge_order, source_choices, _, _, _) =
            candidates.first().cloned().expect("candidates must be non-empty");
        for (candidate_index, (_, _, _, cost, kind, detail)) in candidates.into_iter().enumerate() {
            let chosen = candidate_index == 0 && cost == chosen_cost;
            let decision = if chosen {
                "decision=chosen_lowest_cost_or_deterministic_tie_breaker".to_string()
            } else {
                format!(
                    "decision=rejected_by={}",
                    graph_row_plan_cost_rejection_reason(&cost, &chosen_cost)
                )
            };
            alternatives.push(GraphRowPlanAlternative {
                chosen,
                kind,
                detail,
                decision: Some(decision),
                cost: Some(cost),
            });
        }

        Ok(GraphRowPhysicalSegmentPlan {
            segment: GraphRowPhysicalSegment {
                segment_index,
                barriers_before: segment.barriers_before.clone(),
                initial_driver,
                edge_order,
            },
            source_choices,
            alternatives,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_plan_from_frontier(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        segment_edge_indices: &[usize],
        mut bound: BTreeSet<String>,
        mut visited_edges: Vec<bool>,
        anchor_cost: PlanCost,
        initial_frontier: u64,
        edge_source_costs: &[GraphRowEdgeSourcePlanCost],
        known_node_ids: &[Option<Vec<u64>>],
        target_selectivities: &[Option<(u64, u64)>],
        canonical_key: String,
    ) -> Result<GraphRowFrontierPlan, EngineError> {
        let target_order_len = segment_edge_indices
            .iter()
            .filter(|edge_index| !visited_edges[**edge_index])
            .count();
        let mut order = Vec::with_capacity(target_order_len);
        let mut source_choices = Vec::with_capacity(target_order_len);
        let mut total_work = anchor_cost.estimated_work;
        let mut frontier = initial_frontier.max(1);
        let mut fanout_complete = true;
        let mut confidence = EstimateConfidence::Exact;
        let mut stale_risk = StalePostingRisk::Low;
        let mut hub_risk = GraphRowHubRisk::Low;
        let mut frontier_capped = frontier > GRAPH_ROW_FRONTIER_BUDGET as u64;
        frontier = frontier.min(GRAPH_ROW_FRONTIER_BUDGET as u64 + 1);

        while order.len() < target_order_len {
            let mut choices = Vec::new();
            for &edge_index in segment_edge_indices {
                let edge = &runtime.edges[edge_index];
                if visited_edges[edge_index] {
                    continue;
                }
                let from_bound = bound.contains(&edge.from_alias);
                let to_bound = bound.contains(&edge.to_alias);
                if !from_bound && !to_bound {
                    if bound.is_empty() {
                        if let Some((cost, detail, _)) = edge_source_costs[edge_index].as_ref() {
                            let estimated_expansion = cost
                                .estimated_candidates
                                .unwrap_or(GRAPH_ROW_FANOUT_UNKNOWN_WORK)
                                .max(1);
                            choices.push(GraphRowExpansionChoice {
                                bound_rank: 2,
                                complete: cost.estimated_candidates.is_some(),
                                estimated_expansion,
                                next_frontier: estimated_expansion
                                    .min(GRAPH_ROW_FRONTIER_BUDGET as u64 + 1),
                                confidence_rank: cost.confidence_rank,
                                hub_risk_rank: GraphRowHubRisk::Unknown.rank(),
                                coverage_rank: GraphRowFanoutCoverage::Missing.rank(),
                                source_rank: cost.source_rank,
                                canonical_key: format!(
                                    "edge-source:{}:{edge_index}:{detail}",
                                    edge.explain_name()
                                ),
                                edge_index,
                                source_choice: GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource,
                                fanout: None,
                            });
                        }
                    }
                    continue;
                }

                let from_index = *runtime.node_by_alias.get(&edge.from_alias).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row edge references missing node alias '{}'",
                        edge.from_alias
                    ))
                })?;
                let to_index = *runtime.node_by_alias.get(&edge.to_alias).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row edge references missing node alias '{}'",
                        edge.to_alias
                    ))
                })?;
                let bound_rank = if from_bound && to_bound { 0 } else { 1 };
                let (source_index, target_index, direction, source_alias, target_alias) =
                    if from_bound {
                        (
                            from_index,
                            to_index,
                            edge.direction,
                            edge.from_alias.as_str(),
                            edge.to_alias.as_str(),
                        )
                    } else {
                        (
                            to_index,
                            from_index,
                            reverse_graph_row_direction(edge.direction),
                            edge.to_alias.as_str(),
                            edge.from_alias.as_str(),
                        )
                    };
                let fanout = self.graph_row_edge_fanout_estimate(
                    direction,
                    edge.label_filter_ids.as_deref(),
                    known_node_ids[source_index].as_deref(),
                    query,
                    edge,
                );
                let raw_expansion = frontier.saturating_mul(fanout.cost_fanout());
                let target_selectivity = if bound.contains(target_alias) {
                    None
                } else {
                    target_selectivities[target_index]
                };
                let mut estimated_expansion =
                    Self::apply_graph_row_target_selectivity(raw_expansion, target_selectivity);
                if edge_filter_requires_hydration(&edge.filter) {
                    estimated_expansion = estimated_expansion.saturating_add(raw_expansion);
                }
                let next_frontier = if bound.contains(target_alias) {
                    frontier
                } else {
                    estimated_expansion.min(GRAPH_ROW_FRONTIER_BUDGET as u64 + 1)
                };
                choices.push(GraphRowExpansionChoice {
                    bound_rank,
                    complete: fanout.complete(),
                    estimated_expansion,
                    next_frontier,
                    confidence_rank: fanout.confidence.rank(),
                    hub_risk_rank: fanout.hub_risk.rank(),
                    coverage_rank: fanout.coverage.rank(),
                    source_rank: 2,
                    canonical_key: format!(
                        "adjacency:{}:{}:{}:{}:{edge_index}",
                        edge.explain_name(),
                        source_alias,
                        target_alias,
                        fanout.canonical_key
                    ),
                    edge_index,
                    source_choice: GraphRowEdgeCandidateSourceChoice::EndpointAdjacency,
                    fanout: Some(fanout),
                });
                if let Some((edge_source_cost, edge_source_detail, _)) =
                    edge_source_costs[edge_index].as_ref()
                {
                    let edge_candidates = edge_source_cost
                        .estimated_candidates
                        .unwrap_or(GRAPH_ROW_FANOUT_UNKNOWN_WORK)
                        .max(1);
                    let edge_source_work = edge_source_cost
                        .estimated_work
                        .saturating_add(edge_candidates.saturating_mul(2));
                    let edge_source_next_frontier = if bound.contains(target_alias) {
                        frontier
                    } else {
                        edge_candidates.min(GRAPH_ROW_FRONTIER_BUDGET as u64 + 1)
                    };
                    choices.push(GraphRowExpansionChoice {
                        bound_rank,
                        complete: edge_source_cost.estimated_candidates.is_some(),
                        estimated_expansion: edge_source_work,
                        next_frontier: edge_source_next_frontier,
                        confidence_rank: edge_source_cost.confidence_rank,
                        hub_risk_rank: GraphRowHubRisk::Low.rank(),
                        coverage_rank: GraphRowFanoutCoverage::Missing.rank(),
                        source_rank: edge_source_cost.source_rank,
                        canonical_key: format!(
                            "bound-edge-source:{}:{edge_index}:{edge_source_detail}",
                            edge.explain_name()
                        ),
                        edge_index,
                        source_choice: GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource,
                        fanout: None,
                    });
                }
            }

            choices.sort_by(|left, right| {
                left.bound_rank.cmp(&right.bound_rank).then_with(|| {
                    if left.complete && right.complete {
                        left.estimated_expansion
                            .cmp(&right.estimated_expansion)
                            .then_with(|| left.next_frontier.cmp(&right.next_frontier))
                            .then_with(|| left.confidence_rank.cmp(&right.confidence_rank))
                            .then_with(|| left.hub_risk_rank.cmp(&right.hub_risk_rank))
                            .then_with(|| left.coverage_rank.cmp(&right.coverage_rank))
                            .then_with(|| left.source_rank.cmp(&right.source_rank))
                            .then_with(|| left.canonical_key.cmp(&right.canonical_key))
                            .then_with(|| left.edge_index.cmp(&right.edge_index))
                    } else {
                        left.edge_index
                            .cmp(&right.edge_index)
                            .then_with(|| left.canonical_key.cmp(&right.canonical_key))
                            .then_with(|| left.source_rank.cmp(&right.source_rank))
                    }
                })
            });

            let Some(choice) = choices.into_iter().next() else {
                break;
            };
            let edge = &runtime.edges[choice.edge_index];
            visited_edges[choice.edge_index] = true;
            bound.insert(edge.from_alias.clone());
            bound.insert(edge.to_alias.clone());
            order.push(choice.edge_index);
            source_choices.push((choice.edge_index, choice.source_choice));
            fanout_complete &= choice.complete;
            total_work = total_work.saturating_add(choice.estimated_expansion);
            frontier_capped |= choice.next_frontier > GRAPH_ROW_FRONTIER_BUDGET as u64;
            frontier = choice
                .next_frontier
                .min(GRAPH_ROW_FRONTIER_BUDGET as u64 + 1);
            if let Some(fanout) = choice.fanout {
                confidence = weaker_confidence(confidence, fanout.confidence);
                hub_risk = higher_graph_row_hub_risk(hub_risk, fanout.hub_risk);
                if matches!(fanout.hub_risk, GraphRowHubRisk::High) {
                    stale_risk = StalePostingRisk::Medium;
                }
            }
        }

        if order.len() != target_order_len {
            fanout_complete = false;
            total_work = anchor_cost.estimated_work;
            for &edge_index in segment_edge_indices {
                if !visited_edges[edge_index] {
                    order.push(edge_index);
                    source_choices.push((
                        edge_index,
                        graph_row_deterministic_fallback_edge_source_choice(
                            &runtime.edges[edge_index],
                            edge_source_costs[edge_index].as_ref(),
                        ),
                    ));
                }
            }
        }

        Ok((
            order,
            source_choices,
            GraphRowPlanCost {
                anchor_cost: anchor_cost.clone(),
                estimated_work: total_work,
                simulated_frontier: frontier,
                fanout_complete,
                confidence_rank: confidence.rank(),
                stale_risk_rank: stale_risk.rank(),
                hub_risk_rank: hub_risk.rank(),
                frontier_capped,
                source_rank: anchor_cost.source_rank,
                canonical_key,
            },
        ))
    }

    fn graph_row_physical_plan_notes(&self, query: &NormalizedGraphRowQuery) -> Vec<String> {
        let mut notes = Vec::new();
        if self.graph_row_has_unrolled_memtable_edges() {
            notes.push(
                "fanout confidence downgraded because active/immutable memtables are not represented by immutable adjacency rollups".to_string(),
            );
        }
        if self.planner_stats.adjacency_rollups.is_empty() {
            notes.push(
                "missing fanout stats; deterministic legal-source tie-breakers preserve query correctness and logical order".to_string(),
            );
        }
        if self
            .planner_stats
            .adjacency_rollups
            .values()
            .any(|rollup| rollup.coverage.has_uncovered())
        {
            notes.push(
                "stale or partial adjacency stats coverage; fanout estimates are advisory and downgraded".to_string(),
            );
        }
        if query.at_epoch.is_some() || !self.manifest.prune_policies.is_empty() {
            notes.push(
                "temporal/prune active state downgrades fanout confidence; final visibility verification remains authoritative".to_string(),
            );
        }
        notes
    }

    fn edge_metadata_filter_candidate_plan(
        &self,
        filter: &NormalizedEdgeFilter,
        label_id: Option<u32>,
        availability: EdgeMetadataSidecarAvailability,
    ) -> Option<EdgePhysicalPlan> {
        match filter {
            NormalizedEdgeFilter::AlwaysTrue => None,
            NormalizedEdgeFilter::AlwaysFalse => Some(EdgePhysicalPlan::Empty),
            NormalizedEdgeFilter::WeightRange { lower, upper } => {
                let bounds = crate::edge_metadata::RangeBoundFlags::inclusive(*lower, *upper);
                Some(EdgePhysicalPlan::source(PlannedEdgeCandidateSource::edge_weight_index(
                    label_id,
                    bounds,
                    availability.weight,
                    self.edge_weight_range_estimate(label_id, bounds, availability.weight),
                )))
            }
            NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                let bounds =
                    crate::edge_metadata::RangeBoundFlags::inclusive(Some(*lower_ms), Some(*upper_ms));
                Some(EdgePhysicalPlan::source(PlannedEdgeCandidateSource::edge_updated_at_index(
                    label_id,
                    bounds,
                    availability.updated_at,
                    self.edge_i64_metadata_range_estimate(
                        label_id,
                        bounds,
                        availability.updated_at,
                        |meta| meta.updated_at,
                        |segment| segment.edge_updated_at_range_count(label_id, bounds),
                    ),
                )))
            }
            NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => {
                let bounds =
                    crate::edge_metadata::RangeBoundFlags::inclusive(Some(*lower_ms), Some(*upper_ms));
                Some(EdgePhysicalPlan::source(PlannedEdgeCandidateSource::edge_valid_from_index(
                    label_id,
                    bounds,
                    availability.valid_from,
                    self.edge_i64_metadata_range_estimate(
                        label_id,
                        bounds,
                        availability.valid_from,
                        |meta| meta.valid_from,
                        |segment| segment.edge_valid_from_range_count(label_id, bounds),
                    ),
                )))
            }
            NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => {
                let bounds =
                    crate::edge_metadata::RangeBoundFlags::inclusive(Some(*lower_ms), Some(*upper_ms));
                Some(EdgePhysicalPlan::source(PlannedEdgeCandidateSource::edge_valid_to_index(
                    label_id,
                    bounds,
                    availability.valid_to,
                    self.edge_i64_metadata_range_estimate(
                        label_id,
                        bounds,
                        availability.valid_to,
                        |meta| meta.valid_to,
                        |segment| segment.edge_valid_to_range_count(label_id, bounds),
                    ),
                )))
            }
            NormalizedEdgeFilter::ValidAt { epoch_ms } => {
                let valid_from_bounds =
                    crate::edge_metadata::RangeBoundFlags::inclusive(None, Some(*epoch_ms));
                let valid_from = EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::edge_valid_from_index(
                        label_id,
                        valid_from_bounds,
                        availability.valid_from,
                        self.edge_i64_metadata_range_estimate(
                            label_id,
                            valid_from_bounds,
                            availability.valid_from,
                            |meta| meta.valid_from,
                            |segment| {
                                segment.edge_valid_from_range_count(label_id, valid_from_bounds)
                            },
                        ),
                    ),
                );
                let valid_to_bounds = crate::edge_metadata::RangeBoundFlags {
                    lower: Some(*epoch_ms),
                    lower_inclusive: false,
                    upper: None,
                    upper_inclusive: true,
                };
                let valid_to = EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::edge_valid_to_index(
                        label_id,
                        valid_to_bounds,
                        availability.valid_to,
                        self.edge_i64_metadata_range_estimate(
                            label_id,
                            valid_to_bounds,
                            availability.valid_to,
                            |meta| meta.valid_to,
                            |segment| segment.edge_valid_to_range_count(label_id, valid_to_bounds),
                        ),
                    ),
                );
                Some(EdgePhysicalPlan::intersect(vec![valid_from, valid_to]))
            }
            NormalizedEdgeFilter::And(children) => {
                let plans = children
                    .iter()
                    .filter_map(|child| {
                        self.edge_metadata_filter_candidate_plan(child, label_id, availability)
                    })
                    .collect::<Vec<_>>();
                (!plans.is_empty()).then(|| EdgePhysicalPlan::intersect(plans))
            }
            NormalizedEdgeFilter::Or(children) => {
                let mut plans = Vec::with_capacity(children.len());
                for child in children {
                    let plan =
                        self.edge_metadata_filter_candidate_plan(child, label_id, availability)?;
                    plans.push(plan);
                }
                Some(EdgePhysicalPlan::union(plans))
            }
            NormalizedEdgeFilter::Not(_)
            | NormalizedEdgeFilter::PropertyEquals { .. }
            | NormalizedEdgeFilter::PropertyIn { .. }
            | NormalizedEdgeFilter::PropertyRange { .. }
            | NormalizedEdgeFilter::PropertyExists { .. }
            | NormalizedEdgeFilter::PropertyMissing { .. } => None,
        }
    }

    fn edge_equality_candidate_estimate(
        &self,
        index_id: u64,
        key: &str,
        value: &PropValue,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let mut count = memtable_secondary_eq_edge_count_for_filter(
            &self.memtable,
            index_id,
            key,
            value,
            self.snapshot_seq,
        ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(memtable_secondary_eq_edge_count_for_filter(
                &epoch.memtable,
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64);
        }

        let value_hashes = equality_probe_value_hashes(value);
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.equality_segment_estimate(
                index_id,
                segment.segment_id,
                &value_hashes,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment_edge_secondary_eq_posting_count_for_filter(segment, index_id, value) {
                Ok(Some(posting_count)) => count = count.saturating_add(posting_count as u64),
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

        let mut estimate =
            self.planner_stats_estimate_from_rollup(count, used_stats, used_fallback, stats_values_exact);
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    fn edge_equality_candidate_estimate_for_hash(
        &self,
        index_id: u64,
        key: &str,
        value: &PropValue,
        value_hash: u64,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let mut count = self.memtable.secondary_eq_edge_count_at(
            index_id,
            key,
            value,
            self.snapshot_seq,
        ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(epoch.memtable.secondary_eq_edge_count_at(
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64);
        }

        let value_hashes = [value_hash];
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.equality_segment_estimate(
                index_id,
                segment.segment_id,
                &value_hashes,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment.edge_secondary_eq_posting_count_if_present(index_id, value_hash) {
                Ok(Some(posting_count)) => count = count.saturating_add(posting_count as u64),
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

        let mut estimate =
            self.planner_stats_estimate_from_rollup(count, used_stats, used_fallback, stats_values_exact);
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    fn edge_equality_candidate_probe(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        label_id: u32,
        key: &str,
        value: &PropValue,
    ) -> Result<EdgeCandidateProbe, EngineError> {
        let Some(entry) =
            self.edge_property_index_entry(label_id, key, &SecondaryIndexKind::Equality)
        else {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        }

        let (estimate, followup) =
            self.edge_equality_candidate_estimate(entry.index_id, key, value)?;
        let Some(estimate) = estimate else {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup,
            });
        };
        if cap_context.source_estimate_exceeds_cap(
            EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
            query.page.limit,
            estimate,
        ) {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::CandidateCapExceeded),
                followup,
            });
        }

        Ok(EdgeCandidateProbe {
            source: Some(PlannedEdgeCandidateSource::edge_property_equality_index(
                label_id,
                entry.index_id,
                key,
                value,
                estimate,
            )),
            warning: None,
            followup,
        })
    }

    fn ready_edge_range_candidate_ids(
        &self,
        index_id: u64,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        max_ids: usize,
    ) -> Result<(Option<Vec<u64>>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let lower_key = Self::encode_property_range_bound(lower);
        let upper_key = Self::encode_property_range_bound(upper);
        match self.sources().edge_ids_by_secondary_range_index_limited(
            index_id,
            lower_key,
            upper_key,
            max_ids,
        ) {
            Ok(Some(ids)) => Ok((Some(ids), None)),
            Ok(None) => Ok((None, self.range_sidecar_failure_followup(index_id, None))),
            Err(error) => Ok((None, self.range_sidecar_failure_followup(index_id, Some(error)))),
        }
    }

    fn edge_range_candidate_estimate(
        &self,
        index_id: u64,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let lower_key = Self::encode_property_range_bound(lower);
        let upper_key = Self::encode_property_range_bound(upper);
        let mut count = self
            .memtable
            .visible_secondary_range_entry_count(
                index_id,
                lower_key,
                upper_key,
                None,
                self.snapshot_seq,
            ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                epoch
                    .memtable
                    .visible_secondary_range_entry_count(
                        index_id,
                        lower_key,
                        upper_key,
                        None,
                        self.snapshot_seq,
                    ) as u64,
            );
        }

        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.range_segment_estimate(
                index_id,
                segment.segment_id,
                lower_key,
                upper_key,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment.count_edges_by_secondary_range_index_if_present(
                index_id,
                lower_key,
                upper_key,
            ) {
                Ok(Some(entries)) => count = count.saturating_add(entries as u64),
                Ok(None) => return Ok((None, self.range_sidecar_failure_followup(index_id, None))),
                Err(error) => {
                    return Ok((
                        None,
                        self.range_sidecar_failure_followup(index_id, Some(error)),
                    ));
                }
            }
        }
        #[cfg(test)]
        if used_fallback {
            self.note_range_planning_probe();
        }

        let mut estimate = self.planner_stats_estimate_from_rollup(
            count,
            used_stats,
            used_fallback,
            stats_values_exact,
        );
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    #[allow(clippy::too_many_arguments)]
    fn edge_range_candidate_probe(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        label_id: u32,
        key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        budget: &mut BooleanPlanningBudget,
    ) -> Result<EdgeCandidateProbe, EngineError> {
        let validated = Self::validate_property_range_bounds(lower, upper, None)?;
        if validated.is_empty {
            return Ok(EdgeCandidateProbe {
                source: Some(PlannedEdgeCandidateSource::with_ids(
                    EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
                    format!("edge_range_empty:{label_id}:{key}"),
                    Vec::new(),
                )),
                warning: None,
                followup: None,
            });
        }
        let Some(entry) =
            self.edge_property_index_entry(label_id, key, &SecondaryIndexKind::Range)
        else {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup: None,
            });
        }

        let (estimate, followup) = self.edge_range_candidate_estimate(entry.index_id, lower, upper)?;
        let Some(estimate) = estimate else {
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup,
            });
        };
        if cap_context.source_estimate_exceeds_cap(
            EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
            query.page.limit,
            estimate,
        ) {
            let probe_limit = budget.probe_limit();
            if probe_limit == 0 {
                return Ok(EdgeCandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::PlanningProbeBudgetExceeded),
                    followup: None,
                });
            }
            let (candidate_ids, followup) = self.ready_edge_range_candidate_ids(
                entry.index_id,
                lower,
                upper,
                probe_limit.saturating_add(1),
            )?;
            let Some(candidate_ids) = candidate_ids else {
                return Ok(EdgeCandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::MissingReadyIndex),
                    followup,
                });
            };
            budget.consume_probe_ids(candidate_ids.len().min(probe_limit));
            if candidate_ids.len() <= probe_limit {
                let estimate = PlannerEstimate::exact_cheap(candidate_ids.len() as u64);
                return Ok(EdgeCandidateProbe {
                    source: Some(PlannedEdgeCandidateSource::edge_property_range_index(
                        label_id,
                        entry.index_id,
                        key,
                        lower,
                        upper,
                        estimate,
                    )),
                    warning: None,
                    followup,
                });
            }
            return Ok(EdgeCandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::RangeCandidateCapExceeded),
                followup,
            });
        }
        Ok(EdgeCandidateProbe {
            source: Some(PlannedEdgeCandidateSource::edge_property_range_index(
                label_id,
                entry.index_id,
                key,
                lower,
                upper,
                estimate,
            )),
            warning: None,
            followup,
        })
    }

    fn classification_from_edge_probe(
        &self,
        probe: EdgeCandidateProbe,
        structural_key: Vec<u8>,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> EdgeBooleanPlanResult {
        if let Some(warning) = probe.warning {
            add_plan_warning(warnings, warning);
        }
        if let Some(followup) = probe.followup {
            followups.push(followup);
        }
        match probe.source {
            Some(source) if source.estimate.proves_empty() => EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            },
            Some(source) => EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::Bounded {
                    estimate: source.estimate,
                    structural_key,
                    complete: true,
                    plan: EdgePhysicalPlan::source(source),
                },
                has_verify_only: false,
            },
            None => EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            },
        }
    }

    fn sort_edge_physical_plans_by_selectivity(&self, plans: &mut [EdgePhysicalPlan]) {
        plans.sort_by_key(|plan| plan.plan_cost());
    }

    fn select_bounded_edge_and_plans(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        mut plans: Vec<EdgePhysicalPlan>,
        warnings: &mut Vec<QueryPlanWarning>,
    ) -> (Vec<EdgePhysicalPlan>, bool) {
        self.sort_edge_physical_plans_by_selectivity(&mut plans);
        let Some(first) = plans.first() else {
            return (Vec::new(), false);
        };
        let smallest_cost = first.plan_cost();
        let mut selected = Vec::new();
        let mut skipped_to_verifier = false;

        for plan in plans {
            if selected.is_empty() {
                selected.push(plan);
                continue;
            }
            let plan_cost = plan.plan_cost();
            let estimate = plan.estimate();
            let cap = plan.materialization_cap(cap_context, query.page.limit);
            let within_input_cap = estimate
                .known_upper_bound()
                .is_some_and(|count| count <= cap as u64);
            let include = within_input_cap
                && plan_cost.estimated_work
                    <= smallest_cost
                        .estimated_work
                        .saturating_mul(QUERY_BROAD_SOURCE_FACTOR);
            if include {
                selected.push(plan);
            } else {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
            }
        }

        (selected, skipped_to_verifier)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_edge_property_in_filter(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        key: &str,
        values: &[PropValue],
        structural_key: Vec<u8>,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<EdgeBooleanPlanResult, EngineError> {
        let Some(label_id) = query.label_id else {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        };
        if values.len() == 1 {
            let probe =
                self.edge_equality_candidate_probe(query, cap_context, label_id, key, &values[0])?;
            return Ok(self.classification_from_edge_probe(
                probe,
                structural_key,
                warnings,
                followups,
            ));
        }

        let unique_values = unique_in_probe_values(values);
        if unique_values.len() > MAX_BOOLEAN_UNION_INPUTS {
            add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        let Some(entry) =
            self.edge_property_index_entry(label_id, key, &SecondaryIndexKind::Equality)
        else {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        let mut plans = Vec::new();
        let mut estimated_total = 0u64;
        for probe in &unique_values {
            let (estimate, followup) = self.edge_equality_candidate_estimate_for_hash(
                entry.index_id,
                key,
                &probe.value,
                probe.value_hash,
            )?;
            if let Some(followup) = followup {
                followups.push(followup);
            }
            let Some(estimate) = estimate else {
                add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                return Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            };
            let Some(count) = estimate.known_upper_bound() else {
                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                return Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            };
            if cap_context.source_estimate_exceeds_cap(
                EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
                query.page.limit,
                estimate,
            ) {
                add_plan_warning(
                    warnings,
                    edge_cap_warning_for_source(
                        EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
                    ),
                );
                return Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            }
            estimated_total = estimated_total.saturating_add(count);
            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
            let union_cap = cap_context.union_total_cap(query.page.limit, union_estimate);
            if estimated_total > union_cap as u64 {
                add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
                return Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            }
            if count == 0 && estimate.proves_empty() {
                continue;
            }
            plans.push(EdgePhysicalPlan::source(
                PlannedEdgeCandidateSource::edge_property_equality_index_with_hash(
                    label_id,
                    entry.index_id,
                    key,
                    &probe.value,
                    probe.value_hash,
                    estimate,
                ),
            ));
        }

        if plans.is_empty() {
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            });
        }

        if cap_context
            .cheapest_legal_count()
            .is_some_and(|legal| legal <= estimated_total)
        {
            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
            return Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        Ok(EdgeBooleanPlanResult {
            classification: EdgeBooleanPlanClassification::Bounded {
                plan: EdgePhysicalPlan::union(plans),
                estimate: PlannerEstimate::upper_bound(estimated_total),
                structural_key,
                complete: true,
            },
            has_verify_only: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_edge_filter_subtree(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        filter: &NormalizedEdgeFilter,
        availability: EdgeMetadataSidecarAvailability,
        budget: &mut BooleanPlanningBudget,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<EdgeBooleanPlanResult, EngineError> {
        let structural_key = filter.structural_key();
        match filter {
            NormalizedEdgeFilter::AlwaysFalse => Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            }),
            NormalizedEdgeFilter::AlwaysTrue => Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }),
            NormalizedEdgeFilter::PropertyEquals { key, value } => {
                let Some(label_id) = query.label_id else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe =
                    self.edge_equality_candidate_probe(query, cap_context, label_id, key, value)?;
                Ok(self.classification_from_edge_probe(
                    probe,
                    structural_key,
                    warnings,
                    followups,
                ))
            }
            NormalizedEdgeFilter::PropertyIn { key, values, .. } => self
                .plan_edge_property_in_filter(
                    query,
                    cap_context,
                    key,
                    values,
                    structural_key,
                    warnings,
                    followups,
                ),
            NormalizedEdgeFilter::PropertyRange { key, lower, upper } => {
                let Some(label_id) = query.label_id else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.edge_range_candidate_probe(
                    query,
                    cap_context,
                    label_id,
                    key,
                    lower.as_ref(),
                    upper.as_ref(),
                    budget,
                )?;
                Ok(self.classification_from_edge_probe(
                    probe,
                    structural_key,
                    warnings,
                    followups,
                ))
            }
            NormalizedEdgeFilter::PropertyExists { .. }
            | NormalizedEdgeFilter::PropertyMissing { .. }
            | NormalizedEdgeFilter::Not(_) => Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            }),
            NormalizedEdgeFilter::WeightRange { .. }
            | NormalizedEdgeFilter::UpdatedAtRange { .. }
            | NormalizedEdgeFilter::ValidAt { .. }
            | NormalizedEdgeFilter::ValidFromRange { .. }
            | NormalizedEdgeFilter::ValidToRange { .. } => {
                match self.edge_metadata_filter_candidate_plan(filter, query.label_id, availability)
                {
                    Some(EdgePhysicalPlan::Empty) => Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::AlwaysFalse,
                        has_verify_only: false,
                    }),
                    Some(plan) => Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::Bounded {
                            estimate: plan.estimate(),
                            structural_key,
                            complete: true,
                            plan,
                        },
                        has_verify_only: false,
                    }),
                    None => Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only: false,
                    }),
                }
            }
            NormalizedEdgeFilter::And(children) => {
                let mut plans = Vec::new();
                let mut has_verify_only = false;
                for child in children {
                    let planned = self.plan_edge_filter_subtree(
                        query,
                        cap_context,
                        child,
                        availability,
                        budget,
                        warnings,
                        followups,
                    )?;
                    has_verify_only |= planned.has_verify_only;
                    match planned.classification {
                        EdgeBooleanPlanClassification::AlwaysFalse => {
                            return Ok(EdgeBooleanPlanResult {
                                classification: EdgeBooleanPlanClassification::AlwaysFalse,
                                has_verify_only,
                            });
                        }
                        EdgeBooleanPlanClassification::VerifyOnly => {}
                        EdgeBooleanPlanClassification::Bounded { plan, complete, .. }
                            if complete =>
                        {
                            plans.push(plan)
                        }
                        EdgeBooleanPlanClassification::Bounded { .. } => {
                            has_verify_only = true;
                        }
                    }
                }

                let (selected, skipped_to_verifier) =
                    self.select_bounded_edge_and_plans(query, cap_context, plans, warnings);
                has_verify_only |= skipped_to_verifier;
                if selected.is_empty() {
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only,
                    });
                }
                let plan = EdgePhysicalPlan::intersect(selected);
                Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::Bounded {
                        estimate: plan.estimate(),
                        structural_key,
                        complete: true,
                        plan,
                    },
                    has_verify_only,
                })
            }
            NormalizedEdgeFilter::Or(children) => {
                if children.len() > MAX_BOOLEAN_UNION_INPUTS {
                    add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
                    add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                }

                let mut plan_entries = Vec::new();
                let mut estimated_total = 0u64;
                let mut has_verify_only = false;
                for child in children {
                    let planned = self.plan_edge_filter_subtree(
                        query,
                        cap_context,
                        child,
                        availability,
                        budget,
                        warnings,
                        followups,
                    )?;
                    has_verify_only |= planned.has_verify_only;
                    match planned.classification {
                        EdgeBooleanPlanClassification::AlwaysFalse => {}
                        EdgeBooleanPlanClassification::VerifyOnly => {
                            add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                            return Ok(EdgeBooleanPlanResult {
                                classification: EdgeBooleanPlanClassification::VerifyOnly,
                                has_verify_only: true,
                            });
                        }
                        EdgeBooleanPlanClassification::Bounded {
                            plan,
                            estimate,
                            structural_key,
                            complete,
                        } if complete => {
                            let Some(count) = estimate.known_upper_bound() else {
                                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                                add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                                return Ok(EdgeBooleanPlanResult {
                                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                                    has_verify_only: true,
                                });
                            };
                            estimated_total = estimated_total.saturating_add(count);
                            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
                            let union_cap =
                                cap_context.union_total_cap(query.page.limit, union_estimate);
                            if estimated_total > union_cap as u64 {
                                add_plan_warning(
                                    warnings,
                                    QueryPlanWarning::PlanningProbeBudgetExceeded,
                                );
                                add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                                return Ok(EdgeBooleanPlanResult {
                                    classification: EdgeBooleanPlanClassification::VerifyOnly,
                                    has_verify_only: true,
                                });
                            }
                            plan_entries.push((structural_key, plan));
                        }
                        EdgeBooleanPlanClassification::Bounded { .. } => {
                            add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                            return Ok(EdgeBooleanPlanResult {
                                classification: EdgeBooleanPlanClassification::VerifyOnly,
                                has_verify_only: true,
                            });
                        }
                    }
                }

                if plan_entries.is_empty() {
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::AlwaysFalse,
                        has_verify_only,
                    });
                }
                if cap_context
                    .cheapest_legal_count()
                    .is_some_and(|legal| legal <= estimated_total)
                {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                    add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                    return Ok(EdgeBooleanPlanResult {
                        classification: EdgeBooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                }

                plan_entries.sort_by(|left, right| left.0.cmp(&right.0));
                let plan = EdgePhysicalPlan::union(
                    plan_entries
                        .into_iter()
                        .map(|(_, plan)| plan)
                        .collect(),
                );
                Ok(EdgeBooleanPlanResult {
                    classification: EdgeBooleanPlanClassification::Bounded {
                        estimate: plan.estimate(),
                        structural_key,
                        complete: true,
                        plan,
                    },
                    has_verify_only,
                })
            }
        }
    }

    fn equality_candidate_estimate(
        &self,
        index_id: u64,
        key: &str,
        value: &PropValue,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let mut count =
            memtable_secondary_eq_count_for_filter(
                &self.memtable,
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(memtable_secondary_eq_count_for_filter(
                &epoch.memtable,
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64);
        }

        let value_hashes = equality_probe_value_hashes(value);
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.equality_segment_estimate(
                index_id,
                segment.segment_id,
                &value_hashes,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment_secondary_eq_posting_count_for_filter(segment, index_id, value) {
                Ok(Some(posting_count)) => count = count.saturating_add(posting_count as u64),
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

        let mut estimate =
            self.planner_stats_estimate_from_rollup(count, used_stats, used_fallback, stats_values_exact);
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    fn equality_candidate_estimate_for_hash(
        &self,
        index_id: u64,
        key: &str,
        value: &PropValue,
        value_hash: u64,
    ) -> Result<(Option<PlannerEstimate>, Option<SecondaryIndexReadFollowup>), EngineError> {
        let mut count =
            self.memtable.secondary_eq_node_count_at(
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(epoch.memtable.secondary_eq_node_count_at(
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64);
        }

        let value_hashes = [value_hash];
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_values_exact = true;
        for segment in &self.segments {
            if let Some(segment_estimate) = self.planner_stats.equality_segment_estimate(
                index_id,
                segment.segment_id,
                &value_hashes,
            ) {
                used_stats = true;
                stats_values_exact &= segment_estimate.exact;
                count = count.saturating_add(segment_estimate.count);
                continue;
            }
            used_fallback = true;
            match segment.secondary_eq_posting_count_if_present(index_id, value_hash) {
                Ok(Some(posting_count)) => count = count.saturating_add(posting_count as u64),
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

        let mut estimate =
            self.planner_stats_estimate_from_rollup(count, used_stats, used_fallback, stats_values_exact);
        if !used_stats {
            estimate = estimate.with_current_posting_bound();
        }
        Ok((Some(estimate), None))
    }

    fn sort_physical_plans_by_selectivity(&self, plans: &mut [NodePhysicalPlan]) {
        plans.sort_by(|left, right| {
            left.plan_cost().cmp(&right.plan_cost())
        });
    }

    fn cheapest_legal_universe_estimate(
        &self,
        query: &NormalizedNodeQuery,
    ) -> Result<Option<PlannerEstimate>, EngineError> {
        let mut candidates = self.legal_universe_plans(query, true)?;
        self.sort_physical_plans_by_selectivity(&mut candidates);
        Ok(candidates.first().map(NodePhysicalPlan::estimate))
    }

    fn query_cap_context(&self, query: &NormalizedNodeQuery) -> Result<QueryCapContext, EngineError> {
        Ok(QueryCapContext {
            cheapest_legal_universe: self.cheapest_legal_universe_estimate(query)?,
        })
    }

    fn legal_universe_plans(
        &self,
        query: &NormalizedNodeQuery,
        filter_driver: bool,
    ) -> Result<Vec<NodePhysicalPlan>, EngineError> {
        let mut plans = Vec::new();
        if !query.ids.is_empty() {
            plans.push(NodePhysicalPlan::source(PlannedNodeCandidateSource::with_ids(
                NodeQueryCandidateSourceKind::ExplicitIds,
                "ids".to_string(),
                query.ids.clone(),
            )));
        }
        if !query.keys.is_empty() {
            plans.push(NodePhysicalPlan::source(
                PlannedNodeCandidateSource::key_lookup(query.keys.len()),
            ));
        }
        if let ResolvedNodeLabelFilter::LabelSet {
            mode, label_ids, ..
        } = query.label_filter
        {
            let membership_estimate = self.node_label_filter_estimate(&label_ids, mode)?;
            match (mode, label_ids.as_slice()) {
                (_, [single_label_id]) => {
                    let source = if filter_driver {
                        PlannedNodeCandidateSource::fallback_node_label_scan(
                            *single_label_id,
                            membership_estimate.estimate,
                        )
                    } else {
                        PlannedNodeCandidateSource::node_label_index(
                            *single_label_id,
                            membership_estimate.estimate,
                        )
                    };
                    plans.push(NodePhysicalPlan::source(source));
                }
                (LabelMatchMode::Any, _) => {
                    plans.push(NodePhysicalPlan::source(
                        PlannedNodeCandidateSource::node_label_any_index(
                            label_ids,
                            membership_estimate.estimate,
                        ),
                    ));
                }
                (LabelMatchMode::All, _) => {
                    if let Some(driver_label_id) = membership_estimate.driver_label_id {
                        let source = if filter_driver {
                            PlannedNodeCandidateSource::fallback_node_label_scan(
                                driver_label_id,
                                membership_estimate.estimate,
                            )
                        } else {
                            PlannedNodeCandidateSource::node_label_index(
                                driver_label_id,
                                membership_estimate.estimate,
                            )
                        };
                        plans.push(NodePhysicalPlan::source(source));
                    }
                }
            }
        }
        if query.allow_full_scan {
            plans.push(NodePhysicalPlan::source(
                PlannedNodeCandidateSource::fallback_full_scan(self.full_scan_estimate()),
            ));
        }
        Ok(plans)
    }

    fn select_bounded_and_plans(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        mut plans: Vec<NodePhysicalPlan>,
        warnings: &mut Vec<QueryPlanWarning>,
    ) -> (Vec<NodePhysicalPlan>, bool) {
        self.sort_physical_plans_by_selectivity(&mut plans);
        let Some(first) = plans.first() else {
            return (Vec::new(), false);
        };
        let smallest_cost = first.plan_cost();
        let mut selected = Vec::new();
        let mut skipped_to_verifier = false;

        for plan in plans {
            if selected.is_empty() {
                selected.push(plan);
                continue;
            }
            let plan_cost = plan.plan_cost();
            let estimate = plan.estimate();
            let cap = cap_context.source_cap(
                NodeQueryCandidateSourceKind::PropertyEqualityIndex,
                query.page.limit,
                estimate,
            );
            let within_input_cap = estimate
                .known_upper_bound()
                .is_some_and(|count| count <= cap as u64);
            let include = within_input_cap
                && plan_cost.estimated_work
                    <= smallest_cost
                        .estimated_work
                        .saturating_mul(QUERY_BROAD_SOURCE_FACTOR);
            if include {
                selected.push(plan);
            } else {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
            }
        }

        (selected, skipped_to_verifier)
    }

    fn classification_from_probe(
        &self,
        probe: CandidateProbe,
        structural_key: Vec<u8>,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> BooleanPlanResult {
        if let Some(warning) = probe.warning {
            add_plan_warning(warnings, warning);
        }
        if let Some(followup) = probe.followup {
            followups.push(followup);
        }
        match probe.source {
            Some(source) if source.estimate.proves_empty() => BooleanPlanResult {
                classification: BooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            },
            Some(source) => BooleanPlanResult {
                classification: BooleanPlanClassification::Bounded {
                    estimate: source.estimate,
                    structural_key,
                    complete: true,
                    plan: NodePhysicalPlan::source(source),
                },
                has_verify_only: false,
            },
            None => BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            },
        }
    }

    fn candidate_probe_warning_precedence(warning: QueryPlanWarning) -> usize {
        match warning {
            QueryPlanWarning::PlanningProbeBudgetExceeded => 0,
            QueryPlanWarning::CandidateCapExceeded
            | QueryPlanWarning::RangeCandidateCapExceeded
            | QueryPlanWarning::TimestampCandidateCapExceeded
            | QueryPlanWarning::IndexSkippedAsBroad => 1,
            QueryPlanWarning::MissingReadyIndex => 2,
            _ => 3,
        }
    }

    fn remember_candidate_probe_warning(
        current: &mut Option<(QueryPlanWarning, Option<SecondaryIndexReadFollowup>)>,
        warning: QueryPlanWarning,
        followup: Option<SecondaryIndexReadFollowup>,
    ) {
        let replace = current.as_ref().is_none_or(|(current_warning, _)| {
            Self::candidate_probe_warning_precedence(warning)
                < Self::candidate_probe_warning_precedence(*current_warning)
        });
        if replace {
            *current = Some((warning, followup));
        }
    }

    fn best_candidate_probe_for_labels(
        &self,
        label_ids: NodeLabelSet,
        mut probe_label: impl FnMut(u32) -> Result<CandidateProbe, EngineError>,
    ) -> Result<CandidateProbe, EngineError> {
        let labels = label_ids.as_slice();
        if let [label_id] = labels {
            return probe_label(*label_id);
        }

        let mut best_source: Option<(
            PlanCost,
            PlannedNodeCandidateSource,
            Option<SecondaryIndexReadFollowup>,
        )> = None;
        let mut best_warning = None;

        for &label_id in labels {
            let probe = probe_label(label_id)?;
            if let Some(source) = probe.source {
                let cost = NodePhysicalPlan::source(source.clone()).plan_cost();
                if best_source
                    .as_ref()
                    .is_none_or(|(best_cost, _, _)| cost < *best_cost)
                {
                    best_source = Some((cost, source, probe.followup));
                }
                continue;
            }

            if let Some(warning) = probe.warning {
                Self::remember_candidate_probe_warning(
                    &mut best_warning,
                    warning,
                    probe.followup,
                );
            }
        }

        if let Some((_, source, followup)) = best_source {
            return Ok(CandidateProbe {
                source: Some(source),
                warning: None,
                followup,
            });
        }

        let (warning, followup) =
            best_warning.unwrap_or((QueryPlanWarning::MissingReadyIndex, None));
        Ok(CandidateProbe {
            source: None,
            warning: Some(warning),
            followup,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_property_in_filter_for_label(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        label_id: u32,
        key: &str,
        values: &[PropValue],
        structural_key: Vec<u8>,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<BooleanPlanResult, EngineError> {
        if values.len() == 1 {
            let probe =
                self.equality_candidate_probe(query, cap_context, label_id, key, &values[0])?;
            return Ok(self.classification_from_probe(probe, structural_key, warnings, followups));
        }
        let unique_values = unique_in_probe_values(values);
        if unique_values.len() > MAX_BOOLEAN_UNION_INPUTS {
            add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        let Some(entry) =
            self.node_property_index_entry(label_id, key, &SecondaryIndexKind::Equality)
        else {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        };
        if entry.state != SecondaryIndexState::Ready {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        let mut plans = Vec::new();
        let mut estimated_total = 0u64;
        for probe in &unique_values {
            let (estimate, followup) = self.equality_candidate_estimate_for_hash(
                entry.index_id,
                key,
                &probe.value,
                probe.value_hash,
            )?;
            if let Some(followup) = followup {
                followups.push(followup);
            }
            let Some(estimate) = estimate else {
                add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                return Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            };
            let Some(count) = estimate.known_upper_bound() else {
                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                return Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            };
            if cap_context.source_estimate_exceeds_cap(
                NodeQueryCandidateSourceKind::PropertyEqualityIndex,
                query.page.limit,
                estimate,
            ) {
                add_plan_warning(warnings, cap_warning_for_source(NodeQueryCandidateSourceKind::PropertyEqualityIndex));
                return Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            }
            estimated_total = estimated_total.saturating_add(count);
            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
            let union_cap = cap_context.union_total_cap(query.page.limit, union_estimate);
            if estimated_total > union_cap as u64 {
                add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
                return Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::VerifyOnly,
                    has_verify_only: true,
                });
            }
            if count == 0 && estimate.proves_empty() {
                continue;
            }
            plans.push(NodePhysicalPlan::source(
                PlannedNodeCandidateSource::property_equality_index_with_hash(
                    label_id,
                    entry.index_id,
                    key,
                    &probe.value,
                    probe.value_hash,
                    estimate,
                ),
            ));
        }

        if plans.is_empty() {
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            });
        }

        if cap_context
            .cheapest_legal_count()
            .is_some_and(|legal| legal <= estimated_total)
        {
            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        }

        Ok(BooleanPlanResult {
            classification: BooleanPlanClassification::Bounded {
                plan: NodePhysicalPlan::union(plans),
                estimate: PlannerEstimate::upper_bound(estimated_total),
                structural_key,
                complete: true,
            },
            has_verify_only: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_property_in_filter(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        key: &str,
        values: &[PropValue],
        structural_key: Vec<u8>,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<BooleanPlanResult, EngineError> {
        let Some(label_ids) = node_index_candidate_labels(query) else {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        };
        let labels = label_ids.as_slice();
        if let [label_id] = labels {
            return self.plan_property_in_filter_for_label(
                query,
                cap_context,
                *label_id,
                key,
                values,
                structural_key,
                warnings,
                followups,
            );
        }

        let mut best_plan: Option<(
            PlanCost,
            BooleanPlanResult,
            Vec<QueryPlanWarning>,
            Vec<SecondaryIndexReadFollowup>,
        )> = None;
        let mut fallback_warnings = Vec::new();
        let mut fallback_followups = Vec::new();

        for &label_id in labels {
            let mut label_warnings = Vec::new();
            let mut label_followups = Vec::new();
            let planned = self.plan_property_in_filter_for_label(
                query,
                cap_context,
                label_id,
                key,
                values,
                structural_key.clone(),
                &mut label_warnings,
                &mut label_followups,
            )?;

            match &planned.classification {
                BooleanPlanClassification::AlwaysFalse => {
                    for warning in label_warnings {
                        add_plan_warning(warnings, warning);
                    }
                    followups.append(&mut label_followups);
                    return Ok(planned);
                }
                BooleanPlanClassification::Bounded { plan, .. } => {
                    let cost = plan.plan_cost();
                    if best_plan
                        .as_ref()
                        .is_none_or(|(best_cost, _, _, _)| cost < *best_cost)
                    {
                        best_plan = Some((cost, planned, label_warnings, label_followups));
                    }
                }
                BooleanPlanClassification::VerifyOnly => {
                    for warning in label_warnings {
                        add_plan_warning(&mut fallback_warnings, warning);
                    }
                    fallback_followups.append(&mut label_followups);
                }
            }
        }

        if let Some((_, planned, selected_warnings, mut selected_followups)) = best_plan {
            for warning in selected_warnings {
                add_plan_warning(warnings, warning);
            }
            followups.append(&mut selected_followups);
            return Ok(planned);
        }

        if fallback_warnings.is_empty() {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
        } else {
            for warning in fallback_warnings {
                add_plan_warning(warnings, warning);
            }
        }
        followups.append(&mut fallback_followups);
        Ok(BooleanPlanResult {
            classification: BooleanPlanClassification::VerifyOnly,
            has_verify_only: true,
        })
    }

    fn plan_filter_subtree(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        filter: &NormalizedNodeFilter,
        budget: &mut BooleanPlanningBudget,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<BooleanPlanResult, EngineError> {
        let structural_key = filter.structural_key();
        match filter {
            NormalizedNodeFilter::AlwaysFalse => Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::AlwaysFalse,
                has_verify_only: false,
            }),
            NormalizedNodeFilter::AlwaysTrue => Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }),
            NormalizedNodeFilter::PropertyEquals { key, value } => {
                let Some(label_ids) = node_index_candidate_labels(query) else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.best_candidate_probe_for_labels(label_ids, |label_id| {
                    self.equality_candidate_probe(query, cap_context, label_id, key, value)
                })?;
                Ok(self.classification_from_probe(probe, structural_key, warnings, followups))
            }
            NormalizedNodeFilter::PropertyIn { key, values, .. } => self
                .plan_property_in_filter(
                    query,
                    cap_context,
                    key,
                    values,
                    structural_key,
                    warnings,
                    followups,
                ),
            NormalizedNodeFilter::PropertyRange { key, lower, upper } => {
                let Some(label_ids) = node_index_candidate_labels(query) else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.best_candidate_probe_for_labels(label_ids, |label_id| {
                    self.range_candidate_probe(
                        query,
                        cap_context,
                        label_id,
                        key,
                        lower.as_ref(),
                        upper.as_ref(),
                        budget,
                    )
                })?;
                Ok(self.classification_from_probe(probe, structural_key, warnings, followups))
            }
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                let Some(label_ids) = node_index_candidate_labels(query) else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.best_candidate_probe_for_labels(label_ids, |label_id| {
                    self.timestamp_candidate_probe(
                        query,
                        cap_context,
                        label_id,
                        *lower_ms,
                        *upper_ms,
                        budget,
                    )
                })?;
                Ok(self.classification_from_probe(probe, structural_key, warnings, followups))
            }
            NormalizedNodeFilter::PropertyExists { .. }
            | NormalizedNodeFilter::PropertyMissing { .. }
            | NormalizedNodeFilter::Not(_) => Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            }),
            NormalizedNodeFilter::And(children) => {
                let mut plans = Vec::new();
                let mut has_verify_only = false;
                for child in children {
                    let planned =
                        self.plan_filter_subtree(
                            query,
                            cap_context,
                            child,
                            budget,
                            warnings,
                            followups,
                        )?;
                    has_verify_only |= planned.has_verify_only;
                    match planned.classification {
                        BooleanPlanClassification::AlwaysFalse => {
                            return Ok(BooleanPlanResult {
                                classification: BooleanPlanClassification::AlwaysFalse,
                                has_verify_only,
                            });
                        }
                        BooleanPlanClassification::VerifyOnly => {}
                        BooleanPlanClassification::Bounded {
                            plan, complete, ..
                        } if complete => plans.push(plan),
                        BooleanPlanClassification::Bounded { .. } => {
                            has_verify_only = true;
                        }
                    }
                }

                let (selected, skipped_to_verifier) =
                    self.select_bounded_and_plans(query, cap_context, plans, warnings);
                has_verify_only |= skipped_to_verifier;
                if selected.is_empty() {
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only,
                    });
                }
                let plan = NodePhysicalPlan::intersect(selected);
                Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::Bounded {
                        estimate: plan.estimate(),
                        structural_key,
                        complete: true,
                        plan,
                    },
                    has_verify_only,
                })
            }
            NormalizedNodeFilter::Or(children) => {
                if children.len() > MAX_BOOLEAN_UNION_INPUTS {
                    add_plan_warning(warnings, QueryPlanWarning::PlanningProbeBudgetExceeded);
                    add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                }

                let mut plan_entries = Vec::new();
                let mut estimated_total = 0u64;
                let mut has_verify_only = false;
                for child in children {
                    let planned =
                        self.plan_filter_subtree(
                            query,
                            cap_context,
                            child,
                            budget,
                            warnings,
                            followups,
                        )?;
                    has_verify_only |= planned.has_verify_only;
                    match planned.classification {
                        BooleanPlanClassification::AlwaysFalse => {}
                        BooleanPlanClassification::VerifyOnly => {
                            add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                            return Ok(BooleanPlanResult {
                                classification: BooleanPlanClassification::VerifyOnly,
                                has_verify_only: true,
                            });
                        }
                        BooleanPlanClassification::Bounded {
                            plan,
                            estimate,
                            structural_key,
                            complete,
                        } if complete => {
                            let Some(count) = estimate.known_upper_bound() else {
                                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                                add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                                return Ok(BooleanPlanResult {
                                    classification: BooleanPlanClassification::VerifyOnly,
                                    has_verify_only: true,
                                });
                            };
                            estimated_total = estimated_total.saturating_add(count);
                            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
                            let union_cap =
                                cap_context.union_total_cap(query.page.limit, union_estimate);
                            if estimated_total > union_cap as u64 {
                                add_plan_warning(
                                    warnings,
                                    QueryPlanWarning::PlanningProbeBudgetExceeded,
                                );
                                add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                                return Ok(BooleanPlanResult {
                                    classification: BooleanPlanClassification::VerifyOnly,
                                    has_verify_only: true,
                                });
                            }
                            plan_entries.push((structural_key, plan));
                        }
                        BooleanPlanClassification::Bounded { .. } => {
                            add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                            return Ok(BooleanPlanResult {
                                classification: BooleanPlanClassification::VerifyOnly,
                                has_verify_only: true,
                            });
                        }
                    }
                }

                if plan_entries.is_empty() {
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::AlwaysFalse,
                        has_verify_only,
                    });
                }
                if cap_context
                    .cheapest_legal_count()
                    .is_some_and(|legal| legal <= estimated_total)
                {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                    add_plan_warning(warnings, QueryPlanWarning::BooleanBranchFallback);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                }

                plan_entries.sort_by(|left, right| left.0.cmp(&right.0));
                let plan = NodePhysicalPlan::union(
                    plan_entries
                        .into_iter()
                        .map(|(_, plan)| plan)
                        .collect(),
                );
                Ok(BooleanPlanResult {
                    classification: BooleanPlanClassification::Bounded {
                        estimate: plan.estimate(),
                        structural_key,
                        complete: true,
                        plan,
                    },
                    has_verify_only,
                })
            }
        }
    }

    fn plan_normalized_node_query(
        &self,
        query: &NormalizedNodeQuery,
    ) -> Result<PlannedNodeQuery, EngineError> {
        let mut warnings = query.warnings.clone();
        if query.filter.is_always_false() {
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedNodeQuery {
                driver: NodePhysicalPlan::Empty,
                cap_context: QueryCapContext::default(),
                warnings,
                followups: Vec::new(),
            });
        }

        let cap_context = self.query_cap_context(query)?;
        let has_filter = !query.filter.is_always_true();
        let mut budget = BooleanPlanningBudget::new();
        let mut filter_followups = Vec::new();
        let skip_filter_planning = should_skip_filter_planning_for_explicit_anchor(query);
        let filter_plan = if !has_filter {
            BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }
        } else if skip_filter_planning {
            BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: filter_has_intrinsic_verify_only(&query.filter),
            }
        } else {
            self.plan_filter_subtree(
                query,
                cap_context,
                &query.filter,
                &mut budget,
                &mut warnings,
                &mut filter_followups,
            )?
        };
        if has_filter && filter_plan.has_verify_only {
            add_plan_warning(&mut warnings, QueryPlanWarning::VerifyOnlyFilter);
        }

        if matches!(
            filter_plan.classification,
            BooleanPlanClassification::AlwaysFalse
        ) {
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedNodeQuery {
                driver: NodePhysicalPlan::Empty,
                cap_context,
                warnings,
                followups: filter_followups,
            });
        }

        let mut driver_candidates = self.legal_universe_plans(query, has_filter)?;
        let mut bounded_filter_plan = None;
        if let BooleanPlanClassification::Bounded { plan, .. } = filter_plan.classification {
            bounded_filter_plan = Some(plan.clone());
            driver_candidates.push(plan);
        }

        if driver_candidates.is_empty() {
            return Err(EngineError::InvalidOperation(
                "node query requires label_filter, ids, keys, or allow_full_scan".into(),
            ));
        }

        self.sort_physical_plans_by_selectivity(&mut driver_candidates);
        let driver = driver_candidates
            .first()
            .cloned()
            .expect("driver candidates must be non-empty");

        if let Some(bounded_plan) = bounded_filter_plan.as_ref() {
            if bounded_plan.canonical_key() != driver.canonical_key() {
                if let (Some(selected), Some(bounded)) = (
                    driver.estimate().known_upper_bound(),
                    bounded_plan.estimate().known_upper_bound(),
                ) {
                    if bounded > selected.saturating_mul(QUERY_BROAD_SOURCE_FACTOR)
                        && bounded_plan.broad_skip_warnable()
                    {
                        add_plan_warning(&mut warnings, QueryPlanWarning::IndexSkippedAsBroad);
                    }
                }
            }
        }

        match &driver {
            NodePhysicalPlan::Source(source)
                if source.kind == NodeQueryCandidateSourceKind::FallbackNodeLabelScan =>
            {
                add_plan_warning(&mut warnings, QueryPlanWarning::UsingFallbackScan);
            }
            NodePhysicalPlan::Source(source)
                if source.kind == NodeQueryCandidateSourceKind::FallbackFullNodeScan =>
            {
                add_plan_warning(&mut warnings, QueryPlanWarning::FullScanExplicitlyAllowed);
            }
            _ => {}
        }

        finalize_plan_warnings(&mut warnings);
        Ok(PlannedNodeQuery {
            driver,
            cap_context,
            warnings,
            followups: filter_followups,
        })
    }

    fn explain_node_query(&self, query: &NodeQuery) -> Result<QueryPlan, EngineError> {
        let normalized = self.normalize_node_query(query)?;
        let public_inputs = self.public_inputs_for_node_query(query)?;
        let planned = self.plan_normalized_node_query(&normalized)?;
        let mut plan = planned.explain_plan(public_inputs);
        plan.notes = Self::node_query_explain_notes(&normalized, &planned.driver);
        Ok(plan)
    }

    fn edge_legal_universe_sources(
        &self,
        query: &NormalizedEdgeQuery,
    ) -> Vec<PlannedEdgeCandidateSource> {
        let mut sources = Vec::new();
        if !query.ids.is_empty() {
            sources.push(PlannedEdgeCandidateSource::with_ids(
                EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
                "edge_ids".to_string(),
                query.ids.clone(),
            ));
        }

        let label_filter_ids = query.label_id.map(|label_id| vec![label_id]);
        if let Some(label_id) = query.label_id {
            sources.push(PlannedEdgeCandidateSource::edge_label_index(
                label_id,
                self.edge_label_estimate(label_id),
            ));
        }
        if !query.from_ids.is_empty() {
            let estimate = self.edge_endpoint_estimate(
                &query.from_ids,
                Direction::Outgoing,
                label_filter_ids.as_deref(),
            );
            sources.push(PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
                query.from_ids.clone(),
                label_filter_ids.clone(),
                estimate,
            ));
        }
        if !query.to_ids.is_empty() {
            let estimate =
                self.edge_endpoint_estimate(&query.to_ids, Direction::Incoming, label_filter_ids.as_deref());
            sources.push(PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::ToEndpointAdjacency,
                query.to_ids.clone(),
                label_filter_ids.clone(),
                estimate,
            ));
        }
        if !query.endpoint_ids.is_empty() {
            let estimate =
                self.edge_endpoint_estimate(&query.endpoint_ids, Direction::Both, label_filter_ids.as_deref());
            sources.push(PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::AnyEndpointAdjacency,
                query.endpoint_ids.clone(),
                label_filter_ids,
                estimate,
            ));
        }
        if query.allow_full_scan {
            sources.push(PlannedEdgeCandidateSource::fallback_full_scan(
                self.edge_full_scan_estimate(),
            ));
        }
        sources
    }

    fn edge_query_cap_context(&self, query: &NormalizedEdgeQuery) -> EdgeQueryCapContext {
        let cheapest_legal_universe = self
            .edge_legal_universe_sources(query)
            .into_iter()
            .map(|source| source.estimate)
            .filter_map(|estimate| estimate.known_upper_bound().map(|count| (count, estimate)))
            .min_by_key(|(count, _)| *count)
            .map(|(_, estimate)| estimate);
        EdgeQueryCapContext {
            cheapest_legal_universe,
        }
    }

    fn plan_normalized_edge_query(
        &self,
        query: &NormalizedEdgeQuery,
    ) -> Result<PlannedEdgeQuery, EngineError> {
        let mut warnings = query.warnings.clone();
        let cap_context = self.edge_query_cap_context(query);
        if query.filter.is_always_false() {
            return Ok(PlannedEdgeQuery {
                driver: EdgePhysicalPlan::Empty,
                cap_context,
                warnings,
                followups: Vec::new(),
            });
        }

        let mut inputs = Vec::new();
        if !query.ids.is_empty() {
            inputs.push(EdgePhysicalPlan::source(
                PlannedEdgeCandidateSource::with_ids(
                    EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
                    "edge_ids".to_string(),
                    query.ids.clone(),
                ),
            ));
        }

        let triple_source_used = if let (Some(label_id), [from], [to]) =
            (query.label_id, query.from_ids.as_slice(), query.to_ids.as_slice())
        {
            inputs.push(EdgePhysicalPlan::source(
                PlannedEdgeCandidateSource::edge_triple_index(*from, *to, label_id),
            ));
            true
        } else {
            false
        };

        let label_filter_ids = query.label_id.map(|label_id| vec![label_id]);
        if !triple_source_used {
            if let Some(label_id) = query.label_id {
                inputs.push(EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::edge_label_index(
                        label_id,
                        self.edge_label_estimate(label_id),
                    ),
                ));
            }
            if !query.from_ids.is_empty() {
                let estimate = self.edge_endpoint_estimate(
                    &query.from_ids,
                    Direction::Outgoing,
                    label_filter_ids.as_deref(),
                );
                inputs.push(EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::endpoint_adjacency(
                        EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
                        query.from_ids.clone(),
                        label_filter_ids.clone(),
                        estimate,
                    ),
                ));
            }
            if !query.to_ids.is_empty() {
                let estimate = self.edge_endpoint_estimate(
                    &query.to_ids,
                    Direction::Incoming,
                    label_filter_ids.as_deref(),
                );
                inputs.push(EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::endpoint_adjacency(
                        EdgeQueryCandidateSourceKind::ToEndpointAdjacency,
                        query.to_ids.clone(),
                        label_filter_ids.clone(),
                        estimate,
                    ),
                ));
            }
        }
        if !query.endpoint_ids.is_empty() {
            let estimate = self.edge_endpoint_estimate(
                &query.endpoint_ids,
                Direction::Both,
                label_filter_ids.as_deref(),
            );
            inputs.push(EdgePhysicalPlan::source(
                PlannedEdgeCandidateSource::endpoint_adjacency(
                    EdgeQueryCandidateSourceKind::AnyEndpointAdjacency,
                    query.endpoint_ids.clone(),
                    label_filter_ids,
                    estimate,
                ),
            ));
        }

        let has_filter = !query.filter.is_always_true();
        let mut budget = BooleanPlanningBudget::new();
        let mut filter_followups = Vec::new();
        let filter_plan = if has_filter {
            self.plan_edge_filter_subtree(
                query,
                cap_context,
                &query.filter,
                self.edge_metadata_sidecar_availability(),
                &mut budget,
                &mut warnings,
                &mut filter_followups,
            )?
        } else {
            EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }
        };
        if matches!(
            &filter_plan.classification,
            EdgeBooleanPlanClassification::AlwaysFalse
        ) {
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedEdgeQuery {
                driver: EdgePhysicalPlan::Empty,
                cap_context,
                warnings,
                followups: filter_followups,
            });
        }
        if let EdgeBooleanPlanClassification::Bounded { plan, .. } = &filter_plan.classification {
            inputs.push(plan.clone());
        }
        if has_filter && filter_plan.has_verify_only && edge_filter_requires_hydration(&query.filter) {
            add_plan_warning(&mut warnings, QueryPlanWarning::EdgePropertyPostFilter);
            add_plan_warning(&mut warnings, QueryPlanWarning::VerifyOnlyFilter);
        }

        if inputs.is_empty() {
            if query.allow_full_scan {
                add_plan_warning(&mut warnings, QueryPlanWarning::FullScanExplicitlyAllowed);
                inputs.push(EdgePhysicalPlan::source(
                    PlannedEdgeCandidateSource::fallback_full_scan(
                        self.edge_full_scan_estimate(),
                    ),
                ));
            } else {
                return Err(EngineError::InvalidOperation(
                    "edge query requires label, ids, from_ids, to_ids, endpoint_ids, or allow_full_scan".into(),
                ));
            }
        } else if query.allow_full_scan
            && query.label_id.is_none()
            && query.ids.is_empty()
            && query.from_ids.is_empty()
            && query.to_ids.is_empty()
            && query.endpoint_ids.is_empty()
        {
            add_plan_warning(&mut warnings, QueryPlanWarning::FullScanExplicitlyAllowed);
        }

        for input in &inputs {
            if let EdgePhysicalPlan::Source(source) = input {
                if source.kind == EdgeQueryCandidateSourceKind::EdgeMetadataScan {
                    add_plan_warning(&mut warnings, QueryPlanWarning::UsingFallbackScan);
                }
                if source.broad_skip_warnable()
                    && cap_context.source_estimate_exceeds_cap(
                        source.kind,
                        query.page.limit,
                        source.estimate,
                    )
                {
                    add_plan_warning(&mut warnings, edge_cap_warning_for_source(source.kind));
                }
            }
        }

        inputs.sort_by_key(EdgePhysicalPlan::plan_cost);
        if let Some(driver) = inputs.first() {
            if let Some(driver_count) = driver.estimate().known_upper_bound() {
                for skipped in inputs.iter().skip(1) {
                    if let Some(skipped_count) = skipped.estimate().known_upper_bound() {
                        if skipped_count > driver_count.saturating_mul(QUERY_BROAD_SOURCE_FACTOR)
                            && skipped.broad_skip_warnable()
                        {
                            add_plan_warning(&mut warnings, QueryPlanWarning::IndexSkippedAsBroad);
                        }
                    }
                }
            }
        }
        finalize_plan_warnings(&mut warnings);
        Ok(PlannedEdgeQuery {
            driver: EdgePhysicalPlan::intersect(inputs),
            cap_context,
            warnings,
            followups: filter_followups,
        })
    }

    fn explain_edge_query(&self, query: &EdgeQuery) -> Result<QueryPlan, EngineError> {
        let normalized = self.normalize_edge_query(query)?;
        let public_inputs = self.public_inputs_for_edge_query(query)?;
        let planned = self.plan_normalized_edge_query(&normalized)?;
        Ok(planned.explain_plan(public_inputs))
    }


}

#[cfg(test)]
mod query_plan_unit_tests {
    use super::*;

    fn plan_cost(
        estimated_work: u64,
        estimated_candidates: Option<u64>,
        estimate_kind: PlannerEstimateKind,
        confidence: EstimateConfidence,
        stale_risk: StalePostingRisk,
        source_rank: usize,
    ) -> PlanCost {
        PlanCost {
            estimated_work,
            estimated_candidates,
            estimate_kind_rank: estimate_kind.rank(),
            confidence_rank: confidence.rank(),
            stale_risk_rank: stale_risk.rank(),
            materialization_rank: PlanMaterializationClass::EagerIndex.rank(),
            source_rank,
            canonical_key: "test".to_string(),
        }
    }

    fn edge_source_plan(kind: EdgeQueryCandidateSourceKind, count: u64) -> EdgePhysicalPlan {
        EdgePhysicalPlan::source(PlannedEdgeCandidateSource {
            kind,
            canonical_key: format!("test-edge-source:{kind:?}:{count}"),
            estimate: PlannerEstimate::stats_exact(count),
            materialization: EdgeCandidateMaterialization::Precomputed(Vec::new()),
        })
    }

    #[test]
    fn plan_cost_lower_work_beats_source_rank() {
        let lower_work_worse_rank = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::UpperBound,
            EstimateConfidence::Medium,
            StalePostingRisk::Unknown,
            99,
        );
        let higher_work_better_rank = plan_cost(
            11,
            Some(1),
            PlannerEstimateKind::ExactCheap,
            EstimateConfidence::Exact,
            StalePostingRisk::Low,
            0,
        );

        assert!(lower_work_worse_rank < higher_work_better_rank);
    }

    #[test]
    fn plan_cost_confidence_and_stale_risk_break_late_ties() {
        let lower_work_low_confidence = plan_cost(
            9,
            Some(10),
            PlannerEstimateKind::UpperBound,
            EstimateConfidence::Low,
            StalePostingRisk::High,
            3,
        );
        let higher_work_exact = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::ExactCheap,
            EstimateConfidence::Exact,
            StalePostingRisk::Low,
            0,
        );
        assert!(lower_work_low_confidence < higher_work_exact);

        let lower_count_low_confidence = plan_cost(
            10,
            Some(9),
            PlannerEstimateKind::UpperBound,
            EstimateConfidence::Low,
            StalePostingRisk::High,
            3,
        );
        let higher_count_exact = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::ExactCheap,
            EstimateConfidence::Exact,
            StalePostingRisk::Low,
            0,
        );
        assert!(lower_count_low_confidence < higher_count_exact);

        let high_confidence = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::StatsEstimated,
            EstimateConfidence::High,
            StalePostingRisk::Medium,
            3,
        );
        let low_confidence = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::StatsEstimated,
            EstimateConfidence::Low,
            StalePostingRisk::Low,
            0,
        );
        assert!(high_confidence < low_confidence);

        let low_stale_risk = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::StatsEstimated,
            EstimateConfidence::High,
            StalePostingRisk::Low,
            3,
        );
        let high_stale_risk = plan_cost(
            10,
            Some(10),
            PlannerEstimateKind::StatsEstimated,
            EstimateConfidence::High,
            StalePostingRisk::High,
            0,
        );
        assert!(low_stale_risk < high_stale_risk);
    }

    #[test]
    fn plan_cost_unknown_estimates_lose_to_known_bounds_without_proving_empty() {
        let known = plan_cost(
            1_000,
            Some(1_000),
            PlannerEstimateKind::UpperBound,
            EstimateConfidence::Medium,
            StalePostingRisk::Unknown,
            9,
        );
        let unknown = plan_cost(
            PLAN_COST_UNKNOWN_WORK,
            None,
            PlannerEstimateKind::Unknown,
            EstimateConfidence::Unknown,
            StalePostingRisk::Unknown,
            0,
        );

        assert!(known < unknown);
        assert!(!PlannerEstimate::unknown().proves_empty());
    }

    #[test]
    fn edge_plan_cap_uses_source_kind_for_broad_label_and_metadata_sources() {
        let cap_context = EdgeQueryCapContext {
            cheapest_legal_universe: Some(PlannerEstimate::upper_bound(100_000)),
        };
        let broad_count = QUERY_RANGE_CANDIDATE_CAP as u64 + 1;

        let label_plan = edge_source_plan(EdgeQueryCandidateSourceKind::EdgeLabelIndex, broad_count);
        assert!(label_plan.estimate_exceeds_cap(cap_context, Some(16)));

        let metadata_plan =
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgeMetadataScan, broad_count);
        assert!(metadata_plan.estimate_exceeds_cap(cap_context, Some(16)));

        let equality_plan =
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex, broad_count);
        assert!(!equality_plan.estimate_exceeds_cap(cap_context, Some(16)));
    }

    #[test]
    fn edge_plan_cap_allows_selective_property_range_and_metadata_sources() {
        let cap_context = EdgeQueryCapContext {
            cheapest_legal_universe: Some(PlannerEstimate::upper_bound(100_000)),
        };

        let selective_metadata =
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgeMetadataScan, 128);
        assert!(!selective_metadata.estimate_exceeds_cap(cap_context, Some(16)));

        let range_count = QUERY_RANGE_CANDIDATE_CAP as u64 + 256;
        let range_plan =
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex, range_count);
        assert!(!range_plan.estimate_exceeds_cap(cap_context, Some(16)));

        let union_count = QUERY_RANGE_CANDIDATE_CAP as u64 * 2 + 1;
        let broad_union = EdgePhysicalPlan::union(vec![
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgeMetadataScan, union_count / 2),
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgeMetadataScan, union_count.div_ceil(2)),
        ]);
        assert!(broad_union.estimate_exceeds_cap(cap_context, Some(16)));
    }

    #[test]
    fn unique_in_probe_values_preserves_distinct_values_with_same_hash() {
        let values = vec![
            PropValue::String("a".to_string()),
            PropValue::String("b".to_string()),
        ];
        let probes = unique_in_probe_values_with_hash(&values, |_| 42);

        assert_eq!(probes.len(), 2);
        assert!(probes.iter().all(|probe| probe.value_hash == 42));
        assert!(probes
            .iter()
            .any(|probe| probe.value == PropValue::String("a".to_string())));
        assert!(probes
            .iter()
            .any(|probe| probe.value == PropValue::String("b".to_string())));

        let canonical_keys: Vec<Vec<u8>> = probes
            .iter()
            .map(|probe| semantic_equality_key_bytes(&probe.value))
            .collect();
        assert_ne!(canonical_keys[0], canonical_keys[1]);
    }

    #[test]
    fn unique_in_probe_values_dedupes_exact_duplicates_by_semantic_value() {
        let values = vec![
            PropValue::String("a".to_string()),
            PropValue::String("a".to_string()),
        ];
        let probes = unique_in_probe_values_with_hash(&values, hash_semantic_equality_key_bytes);

        assert_eq!(probes.len(), 1);
        assert_eq!(probes[0].value, PropValue::String("a".to_string()));
        assert_eq!(probes[0].value_hash, hash_prop_equality_key(&probes[0].value));
    }

    #[test]
    fn unique_in_probe_values_dedupes_semantic_zero_to_one_probe() {
        let probes = unique_in_probe_values(&[PropValue::Float(0.0)]);

        assert_eq!(probes.len(), 1);
        assert_eq!(probes[0].value_hash, hash_prop_equality_key(&PropValue::Int(0)));
    }

    #[test]
    fn unique_in_probe_values_dedupes_one_across_numeric_variants() {
        let probes = unique_in_probe_values(&[
            PropValue::Int(1),
            PropValue::UInt(1),
            PropValue::Float(1.0),
        ]);

        assert_eq!(probes.len(), 1);
        assert_eq!(probes[0].value_hash, hash_prop_equality_key(&PropValue::UInt(1)));
    }
}
