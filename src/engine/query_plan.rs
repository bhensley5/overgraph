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
const COMPOUND_INDEX_IN_EXPANSION_CAP: usize = 64;

struct PlannedNodeQuery {
    driver: NodePhysicalPlan,
    cap_context: QueryCapContext,
    legal_universe_fallback: Option<PlannedNodeCandidateSource>,
    warnings: Vec<QueryPlanWarning>,
    followups: Vec<SecondaryIndexReadFollowup>,
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // Planner source payloads stay inline to avoid per-source boxing.
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

#[derive(Clone)]
enum NodeLegalUniverseSource {
    ExplicitIds(Arc<Vec<u64>>),
    KeyLookup(usize),
    Label {
        label_id: u32,
        estimate: PlannerEstimate,
    },
    LabelAny {
        label_ids: NodeLabelSet,
        estimate: PlannerEstimate,
    },
    FullScan {
        estimate: PlannerEstimate,
    },
}

struct PlannedEdgeQuery {
    driver: EdgePhysicalPlan,
    cap_context: EdgeQueryCapContext,
    legal_universe_fallback: Option<PlannedEdgeCandidateSource>,
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

#[derive(Clone)]
struct GraphRowNodeAnchorExplain {
    plan_node: QueryPlanNode,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone)]
struct GraphRowNodeAnchorPlan {
    driver: NodePhysicalPlan,
    estimated_candidates: Option<u64>,
    explain: Option<GraphRowNodeAnchorExplain>,
}

#[derive(Clone)]
struct GraphRowEdgeSourceCost {
    cost: PlanCost,
    detail: Option<String>,
    warnings: Vec<QueryPlanWarning>,
}

type GraphRowEdgeSourcePlanCost = Option<GraphRowEdgeSourceCost>;
type GraphRowFrontierPlan = (
    Vec<usize>,
    Vec<(usize, GraphRowEdgeCandidateSourceChoice)>,
    GraphRowPlanCost,
);

struct GraphRowEdgeSourceCostMemo {
    bound_costs: Vec<[Option<GraphRowEdgeSourcePlanCost>; 3]>,
}

impl GraphRowEdgeSourceCostMemo {
    fn new(edge_count: usize) -> Self {
        Self {
            bound_costs: (0..edge_count)
                .map(|_| std::array::from_fn(|_| None))
                .collect(),
        }
    }

    fn bound_state_index(from_bound: bool, to_bound: bool) -> Option<usize> {
        match (from_bound, to_bound) {
            (true, false) => Some(0),
            (false, true) => Some(1),
            (true, true) => Some(2),
            (false, false) => None,
        }
    }
}

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
    tie_kind_rank: u8,
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
#[allow(clippy::large_enum_variant)] // Planner source payloads stay inline to avoid per-source boxing.
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
    CompoundEqualityIndex,
    CompoundRangeIndex,
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
            Self::CompoundEqualityIndex | Self::CompoundRangeIndex => {
                unreachable!("compound edge plan nodes require materialization details")
            }
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
            | Self::EdgePropertyRangeIndex
            | Self::CompoundEqualityIndex
            | Self::CompoundRangeIndex => 4,
            Self::EdgeLabelIndex => 5,
            Self::EdgeMetadataScan => 6,
            Self::FallbackFullEdgeScan => 7,
        }
    }
}

#[derive(Clone)]
enum EdgeCandidateMaterialization {
    Precomputed(Arc<Vec<u64>>),
    EdgeLabelIndex {
        label_id: u32,
    },
    EdgeTripleIndex {
        from: u64,
        to: u64,
        label_id: u32,
    },
    FromEndpointAdjacency {
        node_ids: Arc<Vec<u64>>,
        label_filter_ids: Option<Vec<u32>>,
    },
    ToEndpointAdjacency {
        node_ids: Arc<Vec<u64>>,
        label_filter_ids: Option<Vec<u32>>,
    },
    AnyEndpointAdjacency {
        node_ids: Arc<Vec<u64>>,
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
    CompoundPrefixIndex {
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        details: CompoundIndexPlanDetails,
    },
    CompoundRangeIndex {
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundRangeBounds>,
        details: CompoundIndexPlanDetails,
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

#[derive(Clone, Debug, PartialEq)]
enum CompoundOwnedValue {
    Property(PropValue),
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
}

impl CompoundOwnedValue {
    fn as_field_value(&self) -> crate::secondary_index_key::CompoundFieldValue<'_> {
        match self {
            CompoundOwnedValue::Property(value) => {
                crate::secondary_index_key::CompoundFieldValue::Property(Some(value))
            }
            CompoundOwnedValue::String(value) => {
                crate::secondary_index_key::CompoundFieldValue::MetadataString(value)
            }
            CompoundOwnedValue::I64(value) => {
                crate::secondary_index_key::CompoundFieldValue::MetadataI64(*value)
            }
            CompoundOwnedValue::U64(value) => {
                crate::secondary_index_key::CompoundFieldValue::MetadataU64(*value)
            }
            CompoundOwnedValue::F64(value) => {
                crate::secondary_index_key::CompoundFieldValue::MetadataF64(*value)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct CompoundRangeConstraint {
    lower: Option<(CompoundOwnedValue, bool)>,
    upper: Option<(CompoundOwnedValue, bool)>,
}

#[derive(Clone, Debug, Default)]
struct CompoundFieldConstraints {
    equalities: BTreeMap<SecondaryIndexField, Vec<CompoundOwnedValue>>,
    ranges: BTreeMap<SecondaryIndexField, CompoundRangeConstraint>,
}

#[derive(Clone)]
struct CompoundNodeCandidatePlan {
    source: PlannedNodeCandidateSource,
    score: CompoundCandidateScore,
}

/// Selected compound node sources for a query: one source for single-label
/// and multi-label `All` shapes, or one source per label for a multi-label
/// `Any` union (every `Any` label must be covered or the selection is
/// abandoned entirely).
struct CompoundNodeCandidateSelection {
    sources: Vec<PlannedNodeCandidateSource>,
}

impl CompoundNodeCandidateSelection {
    fn into_plan(self) -> NodePhysicalPlan {
        let mut plans: Vec<NodePhysicalPlan> = self
            .sources
            .into_iter()
            .map(NodePhysicalPlan::source)
            .collect();
        if plans.len() == 1 {
            plans.pop().expect("compound selection plans non-empty")
        } else {
            NodePhysicalPlan::union(plans)
        }
    }
}

#[derive(Clone)]
struct CompoundEdgeCandidatePlan {
    source: PlannedEdgeCandidateSource,
    score: CompoundCandidateScore,
}

enum CompoundEncodedBounds {
    Prefix {
        bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        matched_prefix_len: usize,
    },
    Range {
        bounds: Vec<crate::secondary_index_key::CompoundRangeBounds>,
        /// Equality-prefix bounds matching `bounds` one-to-one, used for
        /// prefix-capped range costing.
        prefix_bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        matched_prefix_len: usize,
        range_field: SecondaryIndexField,
        /// Decoded numeric range-bound keys in sidecar key encoding, used for
        /// bound-fraction range costing against compound range stats.
        numeric_lower: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        numeric_upper: Option<(crate::planner_stats::RangeStatsKey, bool)>,
    },
}

/// Why a tuple-capable declaration produced no usable bounds. The variants
/// drive warning selection: only `PrefixNotSatisfied` may surface the
/// `CompoundIndexPrefixNotSatisfied` warning, and `InExpansionCapExceeded`
/// surfaces `IndexSkippedAsBroad` per the IN-expansion cap contract.
enum CompoundBoundsOutcome {
    Bounds(CompoundEncodedBounds),
    InExpansionCapExceeded,
    PrefixNotSatisfied,
    Ineligible,
}

enum CompoundPrefixExpansion {
    Expanded(Vec<Vec<CompoundOwnedValue>>),
    CapExceeded,
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CompoundCandidateScore {
    estimated_candidates: u64,
    matched_prefix_len: usize,
    has_range: bool,
    in_expansions: usize,
    coverage_rank: u8,
    index_id: u64,
}

impl Ord for CompoundCandidateScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.estimated_candidates
            .cmp(&other.estimated_candidates)
            .then_with(|| other.matched_prefix_len.cmp(&self.matched_prefix_len))
            .then_with(|| other.has_range.cmp(&self.has_range))
            .then_with(|| self.in_expansions.cmp(&other.in_expansions))
            .then_with(|| self.coverage_rank.cmp(&other.coverage_rank))
            .then_with(|| self.index_id.cmp(&other.index_id))
    }
}

impl PartialOrd for CompoundCandidateScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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
    Precomputed(Arc<Vec<u64>>),
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
    CompoundPrefixIndex {
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        details: CompoundIndexPlanDetails,
    },
    CompoundRangeIndex {
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundRangeBounds>,
        details: CompoundIndexPlanDetails,
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

// Deterministic FNV-1a-style content hash so large ID lists contribute a
// fixed-size component to canonical plan keys instead of a Debug dump of
// every ID. Mixes whole IDs per step (not bytes): the serial multiply chain
// is 8x shorter, and the list length alongside the hash in the key keeps
// practical collision exposure unchanged.
fn candidate_ids_canonical_hash(ids: &[u64]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for id in ids {
        hash ^= *id;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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
        QueryPlanWarning::CompoundIndexPrefixNotSatisfied => 12,
        QueryPlanWarning::UnknownNodeLabel => 13,
        QueryPlanWarning::UnknownEdgeLabel => 14,
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
        | NormalizedNodeFilter::IdRange { .. }
        | NormalizedNodeFilter::KeyEquals(_)
        | NormalizedNodeFilter::KeyIn { .. }
        | NormalizedNodeFilter::PropertyEquals { .. }
        | NormalizedNodeFilter::PropertyIn { .. }
        | NormalizedNodeFilter::PropertyRange { .. }
        | NormalizedNodeFilter::WeightRange { .. }
        | NormalizedNodeFilter::CreatedAtRange { .. }
        | NormalizedNodeFilter::UpdatedAtRange { .. } => false,
    }
}

// Edge analog of should_skip_filter_planning_for_explicit_anchor: a tiny
// explicit-edge-ids anchor always wins the driver sort and execution verifies
// every hydrated edge anyway, so per-leaf index probes, compound candidate
// search, and plan-cost sorting are provably wasted planning work.
fn should_skip_filter_planning_for_explicit_edge_anchor(query: &NormalizedEdgeQuery) -> bool {
    !query.ids.is_empty() && query.ids.len() as u64 <= TINY_EXPLICIT_ANCHOR_MAX
}

fn edge_filter_has_intrinsic_verify_only(filter: &NormalizedEdgeFilter) -> bool {
    match filter {
        NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. }
        | NormalizedEdgeFilter::Not(_) => true,
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().any(edge_filter_has_intrinsic_verify_only)
        }
        NormalizedEdgeFilter::AlwaysTrue
        | NormalizedEdgeFilter::AlwaysFalse
        | NormalizedEdgeFilter::IdRange { .. }
        | NormalizedEdgeFilter::PropertyEquals { .. }
        | NormalizedEdgeFilter::PropertyIn { .. }
        | NormalizedEdgeFilter::PropertyRange { .. }
        | NormalizedEdgeFilter::WeightRange { .. }
        | NormalizedEdgeFilter::UpdatedAtRange { .. }
        | NormalizedEdgeFilter::CreatedAtRange { .. }
        | NormalizedEdgeFilter::ValidAt { .. }
        | NormalizedEdgeFilter::ValidFromRange { .. }
        | NormalizedEdgeFilter::ValidToRange { .. } => false,
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

fn dedup_compound_values(values: &mut Vec<CompoundOwnedValue>) {
    if values.len() <= 1 {
        return;
    }
    // Pairwise structural dedup is quadratic, and past the IN-expansion cap
    // the list can be a graph-row endpoint set thousands of IDs long. Switch
    // to canonical byte keys there: semantically equal values encode to
    // byte-identical tuple components, so the coarser key can only drop
    // values that would have produced duplicate bounds.
    if values.len() <= COMPOUND_INDEX_IN_EXPANSION_CAP {
        let mut unique = Vec::with_capacity(values.len());
        for value in values.drain(..) {
            if !unique.iter().any(|existing| existing == &value) {
                unique.push(value);
            }
        }
        *values = unique;
        return;
    }
    let mut seen = BTreeSet::new();
    values.retain(|value| seen.insert(compound_value_dedup_key(value)));
}

fn compound_value_dedup_key(value: &CompoundOwnedValue) -> Vec<u8> {
    let (tag, payload): (u8, Vec<u8>) = match value {
        CompoundOwnedValue::Property(value) => (0, semantic_equality_key_bytes(value)),
        CompoundOwnedValue::String(value) => (1, value.as_bytes().to_vec()),
        CompoundOwnedValue::I64(value) => (2, value.to_be_bytes().to_vec()),
        CompoundOwnedValue::U64(value) => (3, value.to_be_bytes().to_vec()),
        CompoundOwnedValue::F64(value) => {
            // -0.0 and 0.0 encode identically in tuple keys and match
            // identically in the verifier, so they must share one key.
            let normalized = if *value == 0.0 { 0.0f64 } else { *value };
            (4, normalized.to_bits().to_be_bytes().to_vec())
        }
    };
    let mut key = Vec::with_capacity(1 + payload.len());
    key.push(tag);
    key.extend_from_slice(&payload);
    key
}

fn add_compound_equality_values(
    constraints: &mut CompoundFieldConstraints,
    field: SecondaryIndexField,
    mut values: Vec<CompoundOwnedValue>,
) {
    if values.is_empty() {
        return;
    }
    dedup_compound_values(&mut values);
    match constraints.equalities.entry(field) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(values);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            // Intersect on semantic keys, not structural equality: the index
            // encoding and the runtime verifier both treat Int(2)/UInt(2)/
            // Float(2.0) as equal, and an under-inclusive intersection drives
            // a compound scan that drops matching rows verification cannot
            // recover.
            let incoming: BTreeSet<Vec<u8>> =
                values.iter().map(compound_value_dedup_key).collect();
            entry
                .get_mut()
                .retain(|existing| incoming.contains(&compound_value_dedup_key(existing)));
        }
    }
}

fn add_compound_range(
    constraints: &mut CompoundFieldConstraints,
    field: SecondaryIndexField,
    range: CompoundRangeConstraint,
) {
    constraints.ranges.entry(field).or_insert(range);
}

fn prop_bound_to_compound(bound: &PropertyRangeBound) -> (CompoundOwnedValue, bool) {
    (
        CompoundOwnedValue::Property(bound.value().clone()),
        bound.is_inclusive(),
    )
}

fn node_id_field() -> SecondaryIndexField {
    SecondaryIndexField::NodeMetadata(NodeMetadataIndexField::Id)
}

fn node_key_field() -> SecondaryIndexField {
    SecondaryIndexField::NodeMetadata(NodeMetadataIndexField::Key)
}

fn node_weight_field() -> SecondaryIndexField {
    SecondaryIndexField::NodeMetadata(NodeMetadataIndexField::Weight)
}

fn node_created_at_field() -> SecondaryIndexField {
    SecondaryIndexField::NodeMetadata(NodeMetadataIndexField::CreatedAt)
}

fn node_updated_at_field() -> SecondaryIndexField {
    SecondaryIndexField::NodeMetadata(NodeMetadataIndexField::UpdatedAt)
}

fn edge_id_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::Id)
}

fn edge_from_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::From)
}

fn edge_to_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::To)
}

fn edge_weight_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::Weight)
}

fn edge_created_at_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::CreatedAt)
}

fn edge_updated_at_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::UpdatedAt)
}

fn edge_valid_from_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::ValidFrom)
}

fn edge_valid_to_field() -> SecondaryIndexField {
    SecondaryIndexField::EdgeMetadata(EdgeMetadataIndexField::ValidTo)
}

fn collect_node_compound_filter_constraints(
    filter: &NormalizedNodeFilter,
    constraints: &mut CompoundFieldConstraints,
) {
    match filter {
        NormalizedNodeFilter::AlwaysTrue | NormalizedNodeFilter::AlwaysFalse => {}
        NormalizedNodeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => add_compound_range(
            constraints,
            node_id_field(),
            CompoundRangeConstraint {
                lower: lower.map(|value| (CompoundOwnedValue::U64(value), *lower_inclusive)),
                upper: upper.map(|value| (CompoundOwnedValue::U64(value), *upper_inclusive)),
            },
        ),
        NormalizedNodeFilter::KeyEquals(value) => add_compound_equality_values(
            constraints,
            node_key_field(),
            vec![CompoundOwnedValue::String(value.clone())],
        ),
        NormalizedNodeFilter::KeyIn { values } => add_compound_equality_values(
            constraints,
            node_key_field(),
            values
                .iter()
                .cloned()
                .map(CompoundOwnedValue::String)
                .collect(),
        ),
        NormalizedNodeFilter::PropertyEquals { key, value } => add_compound_equality_values(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            vec![CompoundOwnedValue::Property(value.clone())],
        ),
        NormalizedNodeFilter::PropertyIn { key, values, .. } => add_compound_equality_values(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            values
                .iter()
                .cloned()
                .map(CompoundOwnedValue::Property)
                .collect(),
        ),
        NormalizedNodeFilter::PropertyRange { key, lower, upper } => add_compound_range(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            CompoundRangeConstraint {
                lower: lower.as_ref().map(prop_bound_to_compound),
                upper: upper.as_ref().map(prop_bound_to_compound),
            },
        ),
        NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => add_compound_range(
            constraints,
            node_updated_at_field(),
            CompoundRangeConstraint {
                lower: Some((CompoundOwnedValue::I64(*lower_ms), true)),
                upper: Some((CompoundOwnedValue::I64(*upper_ms), true)),
            },
        ),
        NormalizedNodeFilter::WeightRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => add_compound_range(
            constraints,
            node_weight_field(),
            CompoundRangeConstraint {
                lower: lower
                    .map(|value| (CompoundOwnedValue::F64(value as f64), *lower_inclusive)),
                upper: upper
                    .map(|value| (CompoundOwnedValue::F64(value as f64), *upper_inclusive)),
            },
        ),
        NormalizedNodeFilter::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => add_compound_range(
            constraints,
            node_created_at_field(),
            CompoundRangeConstraint {
                lower: lower.map(|value| (CompoundOwnedValue::I64(value), *lower_inclusive)),
                upper: upper.map(|value| (CompoundOwnedValue::I64(value), *upper_inclusive)),
            },
        ),
        NormalizedNodeFilter::And(children) => {
            for child in children {
                collect_node_compound_filter_constraints(child, constraints);
            }
        }
        NormalizedNodeFilter::Or(_)
        | NormalizedNodeFilter::Not(_)
        | NormalizedNodeFilter::PropertyExists { .. }
        | NormalizedNodeFilter::PropertyMissing { .. } => {}
    }
}

fn collect_edge_compound_filter_constraints(
    filter: &NormalizedEdgeFilter,
    constraints: &mut CompoundFieldConstraints,
) {
    match filter {
        NormalizedEdgeFilter::AlwaysTrue | NormalizedEdgeFilter::AlwaysFalse => {}
        NormalizedEdgeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => add_compound_range(
            constraints,
            edge_id_field(),
            CompoundRangeConstraint {
                lower: lower.map(|value| (CompoundOwnedValue::U64(value), *lower_inclusive)),
                upper: upper.map(|value| (CompoundOwnedValue::U64(value), *upper_inclusive)),
            },
        ),
        NormalizedEdgeFilter::PropertyEquals { key, value } => add_compound_equality_values(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            vec![CompoundOwnedValue::Property(value.clone())],
        ),
        NormalizedEdgeFilter::PropertyIn { key, values, .. } => add_compound_equality_values(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            values
                .iter()
                .cloned()
                .map(CompoundOwnedValue::Property)
                .collect(),
        ),
        NormalizedEdgeFilter::PropertyRange { key, lower, upper } => add_compound_range(
            constraints,
            SecondaryIndexField::Property { key: key.clone() },
            CompoundRangeConstraint {
                lower: lower.as_ref().map(prop_bound_to_compound),
                upper: upper.as_ref().map(prop_bound_to_compound),
            },
        ),
        NormalizedEdgeFilter::WeightRange { lower, upper } => add_compound_range(
            constraints,
            edge_weight_field(),
            CompoundRangeConstraint {
                lower: lower.map(|value| (CompoundOwnedValue::F64(value as f64), true)),
                upper: upper.map(|value| (CompoundOwnedValue::F64(value as f64), true)),
            },
        ),
        NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms } => add_compound_range(
            constraints,
            edge_updated_at_field(),
            CompoundRangeConstraint {
                lower: Some((CompoundOwnedValue::I64(*lower_ms), true)),
                upper: Some((CompoundOwnedValue::I64(*upper_ms), true)),
            },
        ),
        NormalizedEdgeFilter::CreatedAtRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => add_compound_range(
            constraints,
            edge_created_at_field(),
            CompoundRangeConstraint {
                lower: lower.map(|value| (CompoundOwnedValue::I64(value), *lower_inclusive)),
                upper: upper.map(|value| (CompoundOwnedValue::I64(value), *upper_inclusive)),
            },
        ),
        NormalizedEdgeFilter::ValidAt { epoch_ms } => {
            add_compound_range(
                constraints,
                edge_valid_from_field(),
                CompoundRangeConstraint {
                    lower: None,
                    upper: Some((CompoundOwnedValue::I64(*epoch_ms), true)),
                },
            );
            add_compound_range(
                constraints,
                edge_valid_to_field(),
                CompoundRangeConstraint {
                    lower: Some((CompoundOwnedValue::I64(*epoch_ms), false)),
                    upper: None,
                },
            );
        }
        NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms } => add_compound_range(
            constraints,
            edge_valid_from_field(),
            CompoundRangeConstraint {
                lower: Some((CompoundOwnedValue::I64(*lower_ms), true)),
                upper: Some((CompoundOwnedValue::I64(*upper_ms), true)),
            },
        ),
        NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms } => add_compound_range(
            constraints,
            edge_valid_to_field(),
            CompoundRangeConstraint {
                lower: Some((CompoundOwnedValue::I64(*lower_ms), true)),
                upper: Some((CompoundOwnedValue::I64(*upper_ms), true)),
            },
        ),
        NormalizedEdgeFilter::And(children) => {
            for child in children {
                collect_edge_compound_filter_constraints(child, constraints);
            }
        }
        NormalizedEdgeFilter::Or(_)
        | NormalizedEdgeFilter::Not(_)
        | NormalizedEdgeFilter::PropertyExists { .. }
        | NormalizedEdgeFilter::PropertyMissing { .. } => {}
    }
}

fn node_compound_constraints(
    query: &NormalizedNodeQuery,
    filter: &NormalizedNodeFilter,
) -> CompoundFieldConstraints {
    let mut constraints = CompoundFieldConstraints::default();
    if !query.ids.is_empty() {
        add_compound_equality_values(
            &mut constraints,
            node_id_field(),
            query
                .ids
                .iter()
                .copied()
                .map(CompoundOwnedValue::U64)
                .collect(),
        );
    }
    if !query.keys.is_empty() {
        add_compound_equality_values(
            &mut constraints,
            node_key_field(),
            query
                .keys
                .iter()
                .cloned()
                .map(CompoundOwnedValue::String)
                .collect(),
        );
    }
    collect_node_compound_filter_constraints(filter, &mut constraints);
    constraints
}

fn edge_compound_constraints(
    query: &NormalizedEdgeQuery,
    filter: &NormalizedEdgeFilter,
) -> CompoundFieldConstraints {
    let mut constraints = CompoundFieldConstraints::default();
    if !query.ids.is_empty() {
        add_compound_equality_values(
            &mut constraints,
            edge_id_field(),
            query
                .ids
                .iter()
                .copied()
                .map(CompoundOwnedValue::U64)
                .collect(),
        );
    }
    if !query.from_ids.is_empty() {
        add_compound_equality_values(
            &mut constraints,
            edge_from_field(),
            query
                .from_ids
                .iter()
                .copied()
                .map(CompoundOwnedValue::U64)
                .collect(),
        );
    }
    if !query.to_ids.is_empty() {
        add_compound_equality_values(
            &mut constraints,
            edge_to_field(),
            query
                .to_ids
                .iter()
                .copied()
                .map(CompoundOwnedValue::U64)
                .collect(),
        );
    }
    collect_edge_compound_filter_constraints(filter, &mut constraints);
    constraints
}

/// Leaf filter shapes that contribute compound constraints (directly or via
/// query-level anchors) and may therefore drive a compound candidate when
/// they are the entire filter.
fn node_filter_is_compound_constraining_leaf(filter: &NormalizedNodeFilter) -> bool {
    matches!(
        filter,
        NormalizedNodeFilter::PropertyEquals { .. }
            | NormalizedNodeFilter::PropertyIn { .. }
            | NormalizedNodeFilter::PropertyRange { .. }
            | NormalizedNodeFilter::KeyEquals(_)
            | NormalizedNodeFilter::KeyIn { .. }
            | NormalizedNodeFilter::IdRange { .. }
            | NormalizedNodeFilter::WeightRange { .. }
            | NormalizedNodeFilter::CreatedAtRange { .. }
            | NormalizedNodeFilter::UpdatedAtRange { .. }
    )
}

fn edge_filter_is_compound_constraining_leaf(filter: &NormalizedEdgeFilter) -> bool {
    matches!(
        filter,
        NormalizedEdgeFilter::PropertyEquals { .. }
            | NormalizedEdgeFilter::PropertyIn { .. }
            | NormalizedEdgeFilter::PropertyRange { .. }
            | NormalizedEdgeFilter::IdRange { .. }
            | NormalizedEdgeFilter::WeightRange { .. }
            | NormalizedEdgeFilter::CreatedAtRange { .. }
            | NormalizedEdgeFilter::UpdatedAtRange { .. }
            | NormalizedEdgeFilter::ValidAt { .. }
            | NormalizedEdgeFilter::ValidFromRange { .. }
            | NormalizedEdgeFilter::ValidToRange { .. }
    )
}

fn node_filter_leaf_count(filter: &NormalizedNodeFilter) -> usize {
    match filter {
        NormalizedNodeFilter::And(children) | NormalizedNodeFilter::Or(children) => {
            children.iter().map(node_filter_leaf_count).sum()
        }
        NormalizedNodeFilter::AlwaysTrue | NormalizedNodeFilter::AlwaysFalse => 0,
        NormalizedNodeFilter::Not(child) => node_filter_leaf_count(child).max(1),
        _ => 1,
    }
}

fn edge_filter_leaf_count(filter: &NormalizedEdgeFilter) -> usize {
    match filter {
        NormalizedEdgeFilter::And(children) | NormalizedEdgeFilter::Or(children) => {
            children.iter().map(edge_filter_leaf_count).sum()
        }
        NormalizedEdgeFilter::AlwaysTrue | NormalizedEdgeFilter::AlwaysFalse => 0,
        NormalizedEdgeFilter::Not(child) => edge_filter_leaf_count(child).max(1),
        _ => 1,
    }
}

/// Stable explain string for compound candidates whose costing was downgraded
/// to conservative fallback estimates because segment stats coverage is
/// incomplete. Skip reasons surface through plan warnings instead because a
/// skipped candidate emits no details node.
const COMPOUND_FALLBACK_REASON_INCOMPLETE_COVERAGE: &str =
    "compound planner stats coverage incomplete; conservative fallback estimate included";

fn compound_coverage_fallback_reason(coverage_rank: u8) -> Option<String> {
    (coverage_rank >= 1).then(|| COMPOUND_FALLBACK_REASON_INCOMPLETE_COVERAGE.to_string())
}

fn consumed_contains_property(consumed: &[SecondaryIndexField], key: &str) -> bool {
    consumed
        .iter()
        .any(|field| matches!(field, SecondaryIndexField::Property { key: field_key } if field_key == key))
}

/// Counts filter leaves NOT enforced by the selected compound scan. Equality
/// leaves on consumed prefix fields are fully enforced (constraint collection
/// intersects all equality values per field). Range constraints use
/// first-insertion-wins collection, so only the first matching range leaf in
/// the same DFS order is enforced; later range leaves on the same field stay
/// residual. Or/Not subtrees are never consumed by constraint collection.
fn node_compound_residual_predicates(
    filter: &NormalizedNodeFilter,
    consumed_equality: &[SecondaryIndexField],
    consumed_range: Option<&SecondaryIndexField>,
) -> usize {
    let mut range_consumed = false;
    node_compound_residual_walk(filter, consumed_equality, consumed_range, &mut range_consumed)
}

fn node_compound_residual_walk(
    filter: &NormalizedNodeFilter,
    consumed_equality: &[SecondaryIndexField],
    consumed_range: Option<&SecondaryIndexField>,
    range_consumed: &mut bool,
) -> usize {
    let mut consume_range = |field: &SecondaryIndexField| -> bool {
        if consumed_range == Some(field) && !*range_consumed {
            *range_consumed = true;
            true
        } else {
            false
        }
    };
    match filter {
        NormalizedNodeFilter::And(children) => children
            .iter()
            .map(|child| {
                node_compound_residual_walk(
                    child,
                    consumed_equality,
                    consumed_range,
                    range_consumed,
                )
            })
            .sum(),
        NormalizedNodeFilter::AlwaysTrue | NormalizedNodeFilter::AlwaysFalse => 0,
        NormalizedNodeFilter::KeyEquals(_) | NormalizedNodeFilter::KeyIn { .. } => {
            usize::from(!consumed_equality.contains(&node_key_field()))
        }
        NormalizedNodeFilter::PropertyEquals { key, .. }
        | NormalizedNodeFilter::PropertyIn { key, .. } => {
            usize::from(!consumed_contains_property(consumed_equality, key))
        }
        NormalizedNodeFilter::IdRange { .. } => usize::from(!consume_range(&node_id_field())),
        NormalizedNodeFilter::PropertyRange { key, .. } => usize::from(
            !consume_range(&SecondaryIndexField::Property { key: key.clone() }),
        ),
        NormalizedNodeFilter::UpdatedAtRange { .. } => {
            usize::from(!consume_range(&node_updated_at_field()))
        }
        NormalizedNodeFilter::CreatedAtRange { .. } => {
            usize::from(!consume_range(&node_created_at_field()))
        }
        NormalizedNodeFilter::WeightRange { .. } => {
            usize::from(!consume_range(&node_weight_field()))
        }
        other => node_filter_leaf_count(other),
    }
}

/// Edge analog of `node_compound_residual_predicates`. `ValidAt` always stays
/// residual: it can contribute one range bound to a `valid_from` / `valid_to`
/// declaration, but the other validity half still requires verification.
fn edge_compound_residual_predicates(
    filter: &NormalizedEdgeFilter,
    consumed_equality: &[SecondaryIndexField],
    consumed_range: Option<&SecondaryIndexField>,
) -> usize {
    let mut range_consumed = false;
    edge_compound_residual_walk(filter, consumed_equality, consumed_range, &mut range_consumed)
}

fn edge_compound_residual_walk(
    filter: &NormalizedEdgeFilter,
    consumed_equality: &[SecondaryIndexField],
    consumed_range: Option<&SecondaryIndexField>,
    range_consumed: &mut bool,
) -> usize {
    let mut consume_range = |field: &SecondaryIndexField| -> bool {
        if consumed_range == Some(field) && !*range_consumed {
            *range_consumed = true;
            true
        } else {
            false
        }
    };
    match filter {
        NormalizedEdgeFilter::And(children) => children
            .iter()
            .map(|child| {
                edge_compound_residual_walk(
                    child,
                    consumed_equality,
                    consumed_range,
                    range_consumed,
                )
            })
            .sum(),
        NormalizedEdgeFilter::AlwaysTrue | NormalizedEdgeFilter::AlwaysFalse => 0,
        NormalizedEdgeFilter::PropertyEquals { key, .. }
        | NormalizedEdgeFilter::PropertyIn { key, .. } => {
            usize::from(!consumed_contains_property(consumed_equality, key))
        }
        NormalizedEdgeFilter::IdRange { .. } => usize::from(!consume_range(&edge_id_field())),
        NormalizedEdgeFilter::PropertyRange { key, .. } => usize::from(
            !consume_range(&SecondaryIndexField::Property { key: key.clone() }),
        ),
        NormalizedEdgeFilter::WeightRange { .. } => {
            usize::from(!consume_range(&edge_weight_field()))
        }
        NormalizedEdgeFilter::UpdatedAtRange { .. } => {
            usize::from(!consume_range(&edge_updated_at_field()))
        }
        NormalizedEdgeFilter::CreatedAtRange { .. } => {
            usize::from(!consume_range(&edge_created_at_field()))
        }
        NormalizedEdgeFilter::ValidFromRange { .. } => {
            usize::from(!consume_range(&edge_valid_from_field()))
        }
        NormalizedEdgeFilter::ValidToRange { .. } => {
            usize::from(!consume_range(&edge_valid_to_field()))
        }
        NormalizedEdgeFilter::ValidAt { .. } => {
            // The scan may enforce one half via the consumed range field, but
            // the leaf still needs residual verification of the other half.
            if consumed_range == Some(&edge_valid_from_field())
                || consumed_range == Some(&edge_valid_to_field())
            {
                *range_consumed = true;
            }
            1
        }
        other => edge_filter_leaf_count(other),
    }
}

fn append_compound_prefix_values(
    prefixes: Vec<Vec<CompoundOwnedValue>>,
    values: &[CompoundOwnedValue],
) -> CompoundPrefixExpansion {
    if values.is_empty() {
        return CompoundPrefixExpansion::Empty;
    }
    let Some(total) = prefixes.len().checked_mul(values.len()) else {
        return CompoundPrefixExpansion::CapExceeded;
    };
    if total > COMPOUND_INDEX_IN_EXPANSION_CAP {
        return CompoundPrefixExpansion::CapExceeded;
    }
    let mut expanded = Vec::with_capacity(total);
    for prefix in prefixes {
        for value in values {
            let mut next = prefix.clone();
            next.push(value.clone());
            expanded.push(next);
        }
    }
    CompoundPrefixExpansion::Expanded(expanded)
}

/// Extracts the sidecar numeric range key payload from an encoded range-bound
/// component (3-byte component header followed by the numeric key bytes).
fn compound_component_stats_key(component: &[u8]) -> Option<crate::planner_stats::RangeStatsKey> {
    component
        .get(3..)
        .and_then(|payload| crate::planner_stats::RangeStatsKey::try_from(payload).ok())
}

fn encode_compound_bounds_for_entry(
    entry: &SecondaryIndexManifestEntry,
    constraints: &CompoundFieldConstraints,
) -> CompoundBoundsOutcome {
    if entry.state != SecondaryIndexState::Ready {
        return CompoundBoundsOutcome::Ineligible;
    }
    // One-field metadata declarations are legal tuple-capable targets; only an
    // empty field list (never produced by validated manifests) is rejected.
    let public_fields = entry.target.public_fields();
    if public_fields.is_empty() {
        return CompoundBoundsOutcome::Ineligible;
    }
    let Ok(context) = crate::secondary_index_key::CompoundTupleContext::from_manifest_entry(entry)
    else {
        return CompoundBoundsOutcome::Ineligible;
    };
    let mut prefixes = vec![Vec::<CompoundOwnedValue>::new()];
    let mut matched_prefix_len = 0usize;

    for (ordinal, field) in public_fields.iter().enumerate() {
        if let Some(values) = constraints.equalities.get(field) {
            match append_compound_prefix_values(prefixes, values) {
                CompoundPrefixExpansion::Expanded(expanded) => {
                    prefixes = expanded;
                    matched_prefix_len += 1;
                    continue;
                }
                CompoundPrefixExpansion::CapExceeded => {
                    return CompoundBoundsOutcome::InExpansionCapExceeded;
                }
                CompoundPrefixExpansion::Empty => return CompoundBoundsOutcome::Ineligible,
            }
        }

        if let Some(range) = constraints.ranges.get(field) {
            if matched_prefix_len == 0 {
                return CompoundBoundsOutcome::PrefixNotSatisfied;
            }
            if entry.kind != SecondaryIndexKind::Range {
                break;
            }
            // Weight tuples store non-finite values as EqualityHash-class
            // components that Numeric-class range scans never visit, while the
            // public weight-range verifier matches +/-infinity records. Keep
            // the equality-prefix candidate and verify the weight range
            // residually so plan choice can never change results.
            if *field == node_weight_field() || *field == edge_weight_field() {
                break;
            }
            let lower_component = match range.lower.as_ref() {
                Some((value, inclusive)) => {
                    let Ok(encoded) = crate::secondary_index_key::encode_compound_field_component(
                        &context,
                        ordinal,
                        value.as_field_value(),
                    ) else {
                        return CompoundBoundsOutcome::Ineligible;
                    };
                    Some((encoded, *inclusive))
                }
                None => None,
            };
            let upper_component = match range.upper.as_ref() {
                Some((value, inclusive)) => {
                    let Ok(encoded) = crate::secondary_index_key::encode_compound_field_component(
                        &context,
                        ordinal,
                        value.as_field_value(),
                    ) else {
                        return CompoundBoundsOutcome::Ineligible;
                    };
                    Some((encoded, *inclusive))
                }
                None => None,
            };
            // A bound that does not encode as a Numeric-class component (for
            // example a non-finite float) cannot bound a Numeric-class tuple
            // scan; demote the range to a residual verifier predicate instead
            // of dropping the compound candidate entirely.
            let bound_is_numeric = |component: &Option<(Vec<u8>, bool)>| {
                component.as_ref().is_none_or(|(encoded, _)| {
                    encoded.first()
                        == Some(&crate::secondary_index_key::COMPOUND_COMPONENT_CLASS_NUMERIC)
                })
            };
            if !bound_is_numeric(&lower_component) || !bound_is_numeric(&upper_component) {
                break;
            }
            let numeric_lower = lower_component
                .as_ref()
                .and_then(|(encoded, inclusive)| {
                    compound_component_stats_key(encoded).map(|key| (key, *inclusive))
                });
            let numeric_upper = upper_component
                .as_ref()
                .and_then(|(encoded, inclusive)| {
                    compound_component_stats_key(encoded).map(|key| (key, *inclusive))
                });
            let lower_ref = lower_component
                .as_ref()
                .map(|(encoded, inclusive)| (encoded.as_slice(), *inclusive));
            let upper_ref = upper_component
                .as_ref()
                .map(|(encoded, inclusive)| (encoded.as_slice(), *inclusive));
            let mut bounds = Vec::with_capacity(prefixes.len());
            let mut prefix_bounds = Vec::with_capacity(prefixes.len());
            for prefix in &prefixes {
                let equality_prefix_values = prefix
                    .iter()
                    .map(CompoundOwnedValue::as_field_value)
                    .collect::<Vec<_>>();
                let Ok(equality_prefix) = crate::secondary_index_key::encode_compound_tuple_prefix(
                    &context,
                    &equality_prefix_values,
                ) else {
                    return CompoundBoundsOutcome::Ineligible;
                };
                let Ok(range_bounds) = crate::secondary_index_key::compound_range_bounds(
                    &equality_prefix,
                    lower_ref,
                    upper_ref,
                ) else {
                    return CompoundBoundsOutcome::Ineligible;
                };
                bounds.push(range_bounds);
                prefix_bounds.push(crate::secondary_index_key::compound_prefix_bounds(
                    &equality_prefix,
                ));
            }
            return CompoundBoundsOutcome::Bounds(CompoundEncodedBounds::Range {
                bounds,
                prefix_bounds,
                matched_prefix_len,
                range_field: field.clone(),
                numeric_lower,
                numeric_upper,
            });
        }
        break;
    }

    if matched_prefix_len == 0 {
        return CompoundBoundsOutcome::PrefixNotSatisfied;
    }
    let mut bounds = Vec::with_capacity(prefixes.len());
    for prefix in &prefixes {
        let values = prefix
            .iter()
            .map(CompoundOwnedValue::as_field_value)
            .collect::<Vec<_>>();
        let Ok(encoded) =
            crate::secondary_index_key::encode_compound_tuple_prefix(&context, &values)
        else {
            return CompoundBoundsOutcome::Ineligible;
        };
        bounds.push(crate::secondary_index_key::compound_prefix_bounds(&encoded));
    }
    CompoundBoundsOutcome::Bounds(CompoundEncodedBounds::Prefix {
        bounds,
        matched_prefix_len,
    })
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

fn min_known_planner_estimate(left: PlannerEstimate, right: PlannerEstimate) -> PlannerEstimate {
    match (left.known_upper_bound(), right.known_upper_bound()) {
        (Some(left_count), Some(right_count)) => {
            if left_count <= right_count {
                left
            } else {
                right
            }
        }
        (Some(_), None) => left,
        (None, _) => right,
    }
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

// Eager index kinds materialize bounded candidate sets through limited index
// reads whose estimates are genuine raw-posting upper bounds (exact memtable
// counts plus immutable per-segment posting counts). Streaming scans,
// adjacency fallbacks, and unindexed metadata scans are not eager kinds.
fn node_source_kind_is_eager_index(source_kind: NodeQueryCandidateSourceKind) -> bool {
    matches!(
        source_kind,
        NodeQueryCandidateSourceKind::PropertyRangeIndex
            | NodeQueryCandidateSourceKind::TimestampIndex
            | NodeQueryCandidateSourceKind::PropertyEqualityIndex
            | NodeQueryCandidateSourceKind::CompoundEqualityIndex
            | NodeQueryCandidateSourceKind::CompoundRangeIndex
    )
}

fn edge_source_kind_is_eager_index(source_kind: EdgeQueryCandidateSourceKind) -> bool {
    matches!(
        source_kind,
        EdgeQueryCandidateSourceKind::EdgeWeightIndex
            | EdgeQueryCandidateSourceKind::EdgeUpdatedAtIndex
            | EdgeQueryCandidateSourceKind::EdgeValidFromIndex
            | EdgeQueryCandidateSourceKind::EdgeValidToIndex
            | EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
            | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
            | EdgeQueryCandidateSourceKind::CompoundEqualityIndex
            | EdgeQueryCandidateSourceKind::CompoundRangeIndex
    )
}

fn adaptive_cap_for_estimate(
    eager_index_kind: bool,
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

    // The eager-kind uncap must not be gated on confidence class: any
    // unflushed write downgrades confidence database-wide, and capping at the
    // default trades a bounded <=hard_cap posting read for a full label scan.
    // High stale-posting risk stays excluded as a cost heuristic for
    // prune-heavy posting lists.
    let trusted_upper_bound = !matches!(source_estimate.stale_risk, StalePostingRisk::High);
    let high_confidence = matches!(
        source_estimate.confidence,
        EstimateConfidence::Exact | EstimateConfidence::High
    ) && trusted_upper_bound;

    let mut cap = default_cap.min(hard_cap);
    if high_confidence {
        if let Some(limit) = query_limit.filter(|limit| *limit > 0) {
            let proof_cap = limit
                .saturating_add(1)
                .saturating_mul(64)
                .max(default_cap);
            cap = cap.max(proof_cap.min(hard_cap));
        }
    }
    if eager_index_kind && trusted_upper_bound {
        cap = cap.max((source_count.min(hard_cap as u64)) as usize);
    }
    cap.min(hard_cap)
}

fn adaptive_candidate_cap(
    source_kind: NodeQueryCandidateSourceKind,
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    source_estimate: PlannerEstimate,
) -> usize {
    adaptive_cap_for_estimate(
        node_source_kind_is_eager_index(source_kind),
        query_limit,
        cheapest_legal_universe,
        source_estimate,
    )
}

fn adaptive_edge_candidate_cap(
    source_kind: EdgeQueryCandidateSourceKind,
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    source_estimate: PlannerEstimate,
) -> usize {
    adaptive_cap_for_estimate(
        edge_source_kind_is_eager_index(source_kind),
        query_limit,
        cheapest_legal_universe,
        source_estimate,
    )
}

// A union uncaps only when every member is an eager index kind: the union
// total then stays a trusted posting upper bound. One scan-backed member
// makes the whole union scan-priced and the default cap applies.
fn adaptive_union_total_cap(
    members_eager: bool,
    query_limit: Option<usize>,
    cheapest_legal_universe: Option<PlannerEstimate>,
    union_estimate: PlannerEstimate,
) -> usize {
    adaptive_cap_for_estimate(
        members_eager,
        query_limit,
        cheapest_legal_universe,
        union_estimate,
    )
    .saturating_mul(2)
    .min(crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP)
}

fn compound_union_estimate_exceeds_materialization_cap(
    cap_context: QueryCapContext,
    query_limit: Option<usize>,
    union_estimate: u64,
) -> bool {
    let estimate = PlannerEstimate::upper_bound(union_estimate);
    union_estimate > cap_context.union_total_cap(true, query_limit, estimate) as u64
}

fn remaining_compound_count_cap(cap: u64, total: u64, local_count: u64) -> Option<u64> {
    let used = total.saturating_add(local_count);
    if used < cap {
        Some(cap - used)
    } else {
        None
    }
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
        members_eager: bool,
        query_limit: Option<usize>,
        union_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_union_total_cap(
            members_eager,
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
        members_eager: bool,
        query_limit: Option<usize>,
        union_estimate: PlannerEstimate,
    ) -> usize {
        adaptive_union_total_cap(
            members_eager,
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
            NodeQueryCandidateSourceKind::CompoundEqualityIndex
            | NodeQueryCandidateSourceKind::CompoundRangeIndex => {
                unreachable!("compound node plan nodes require materialization details")
            }
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
            NodeQueryCandidateSourceKind::CompoundEqualityIndex
            | NodeQueryCandidateSourceKind::CompoundRangeIndex => 3,
            NodeQueryCandidateSourceKind::PropertyRangeIndex => 4,
            NodeQueryCandidateSourceKind::TimestampIndex => 5,
            NodeQueryCandidateSourceKind::NodeLabelIndex => 6,
            NodeQueryCandidateSourceKind::FallbackNodeLabelScan => 7,
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => 8,
        }
    }
}

impl PlannedNodeCandidateSource {
    fn with_ids(kind: NodeQueryCandidateSourceKind, canonical_key: String, ids: Vec<u64>) -> Self {
        Self::with_normalized_ids(kind, canonical_key, Arc::new(normalize_candidate_ids(ids)))
    }

    /// `ids` must already be sorted and deduplicated (normalized query ID
    /// lists are); skips the re-normalization pass and shares the list.
    fn with_normalized_ids(
        kind: NodeQueryCandidateSourceKind,
        canonical_key: String,
        ids: Arc<Vec<u64>>,
    ) -> Self {
        debug_assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
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

    fn compound_prefix_index(
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        estimate: PlannerEstimate,
        details: CompoundIndexPlanDetails,
    ) -> Self {
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => NodeQueryCandidateSourceKind::CompoundEqualityIndex,
            SecondaryIndexKind::Range => NodeQueryCandidateSourceKind::CompoundRangeIndex,
        };
        Self {
            kind,
            canonical_key: format!(
                "compound_prefix:{}:{}:{}",
                entry.index_id,
                details.matched_prefix_len,
                details.in_expansions
            ),
            estimate,
            materialization: NodeCandidateMaterialization::CompoundPrefixIndex {
                entry,
                bounds,
                details,
            },
        }
    }

    fn compound_range_index(
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundRangeBounds>,
        estimate: PlannerEstimate,
        details: CompoundIndexPlanDetails,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::CompoundRangeIndex,
            canonical_key: format!(
                "compound_range:{}:{}:{}",
                entry.index_id,
                details.matched_prefix_len,
                details.in_expansions
            ),
            estimate,
            materialization: NodeCandidateMaterialization::CompoundRangeIndex {
                entry,
                bounds,
                details,
            },
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        match self.materialization {
            NodeCandidateMaterialization::NodeLabelAny { .. } => QueryPlanNode::NodeLabelAnyIndex,
            NodeCandidateMaterialization::CompoundPrefixIndex { ref details, .. } => {
                match self.kind {
                    NodeQueryCandidateSourceKind::CompoundEqualityIndex => {
                        QueryPlanNode::CompoundEqualityIndex {
                            details: details.clone(),
                        }
                    }
                    NodeQueryCandidateSourceKind::CompoundRangeIndex => {
                        QueryPlanNode::CompoundRangeIndex {
                            details: details.clone(),
                        }
                    }
                    _ => self.kind.plan_node(),
                }
            }
            NodeCandidateMaterialization::CompoundRangeIndex { ref details, .. } => {
                QueryPlanNode::CompoundRangeIndex {
                    details: details.clone(),
                }
            }
            _ => self.kind.plan_node(),
        }
    }

    fn broad_skip_warnable(&self) -> bool {
        matches!(
            self.kind,
            NodeQueryCandidateSourceKind::PropertyEqualityIndex
                | NodeQueryCandidateSourceKind::PropertyRangeIndex
                | NodeQueryCandidateSourceKind::CompoundEqualityIndex
                | NodeQueryCandidateSourceKind::CompoundRangeIndex
                | NodeQueryCandidateSourceKind::TimestampIndex
        )
    }
}

impl NodeLegalUniverseSource {
    fn source(&self, filter_driver: bool) -> PlannedNodeCandidateSource {
        match self {
            NodeLegalUniverseSource::ExplicitIds(ids) => {
                PlannedNodeCandidateSource::with_normalized_ids(
                    NodeQueryCandidateSourceKind::ExplicitIds,
                    "ids".to_string(),
                    Arc::clone(ids),
                )
            }
            NodeLegalUniverseSource::KeyLookup(key_count) => {
                PlannedNodeCandidateSource::key_lookup(*key_count)
            }
            NodeLegalUniverseSource::Label { label_id, estimate } => {
                if filter_driver {
                    PlannedNodeCandidateSource::fallback_node_label_scan(*label_id, *estimate)
                } else {
                    PlannedNodeCandidateSource::node_label_index(*label_id, *estimate)
                }
            }
            NodeLegalUniverseSource::LabelAny {
                label_ids,
                estimate,
            } => PlannedNodeCandidateSource::node_label_any_index(*label_ids, *estimate),
            NodeLegalUniverseSource::FullScan { estimate } => {
                PlannedNodeCandidateSource::fallback_full_scan(*estimate)
            }
        }
    }

    fn plan(&self, filter_driver: bool) -> NodePhysicalPlan {
        NodePhysicalPlan::source(self.source(filter_driver))
    }
}

impl PlannedEdgeCandidateSource {
    fn with_ids(kind: EdgeQueryCandidateSourceKind, canonical_key: String, ids: Vec<u64>) -> Self {
        Self::with_normalized_ids(kind, canonical_key, Arc::new(normalize_candidate_ids(ids)))
    }

    /// `ids` must already be sorted and deduplicated (normalized query ID
    /// lists are); skips the re-normalization pass and shares the list.
    fn with_normalized_ids(
        kind: EdgeQueryCandidateSourceKind,
        canonical_key: String,
        ids: Arc<Vec<u64>>,
    ) -> Self {
        debug_assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
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

    fn edge_triple_index(from: u64, to: u64, label_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgeTripleIndex,
            canonical_key: format!("edge_triple:{from}:{to}:{label_id}"),
            estimate,
            materialization: EdgeCandidateMaterialization::EdgeTripleIndex { from, to, label_id },
        }
    }

    fn endpoint_adjacency(
        kind: EdgeQueryCandidateSourceKind,
        node_ids: Arc<Vec<u64>>,
        label_filter_ids: Option<Vec<u32>>,
        estimate: PlannerEstimate,
    ) -> Self {
        // Fixed-size content key: Debug-dumping every endpoint ID made the
        // canonical key O(ids) to build, clone, and compare on large anchors.
        let canonical_key = format!(
            "{kind:?}:{}:{:016x}:{label_filter_ids:?}",
            node_ids.len(),
            candidate_ids_canonical_hash(&node_ids),
        );
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

    fn edge_compound_prefix_index(
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundPrefixBounds>,
        estimate: PlannerEstimate,
        details: CompoundIndexPlanDetails,
    ) -> Self {
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => EdgeQueryCandidateSourceKind::CompoundEqualityIndex,
            SecondaryIndexKind::Range => EdgeQueryCandidateSourceKind::CompoundRangeIndex,
        };
        Self {
            kind,
            canonical_key: format!(
                "edge_compound_prefix:{}:{}:{}",
                entry.index_id,
                details.matched_prefix_len,
                details.in_expansions
            ),
            estimate,
            materialization: EdgeCandidateMaterialization::CompoundPrefixIndex {
                entry,
                bounds,
                details,
            },
        }
    }

    fn edge_compound_range_index(
        entry: SecondaryIndexManifestEntry,
        bounds: Vec<crate::secondary_index_key::CompoundRangeBounds>,
        estimate: PlannerEstimate,
        details: CompoundIndexPlanDetails,
    ) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::CompoundRangeIndex,
            canonical_key: format!(
                "edge_compound_range:{}:{}:{}",
                entry.index_id,
                details.matched_prefix_len,
                details.in_expansions
            ),
            estimate,
            materialization: EdgeCandidateMaterialization::CompoundRangeIndex {
                entry,
                bounds,
                details,
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

    /// Complete scan-and-verify source for queries whose only anchor is a
    /// metadata filter (e.g. `IdRange` / `CreatedAtRange` without label,
    /// ids, or endpoints). The metadata anchor makes the scan legal without
    /// `allow_full_scan`; the full normalized filter is verified per record.
    fn metadata_filter_scan(estimate: PlannerEstimate) -> Self {
        Self {
            kind: EdgeQueryCandidateSourceKind::EdgeMetadataScan,
            canonical_key: "edge_metadata_filter_scan".to_string(),
            estimate,
            materialization: EdgeCandidateMaterialization::FallbackFullEdgeScan,
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        match self.materialization {
            EdgeCandidateMaterialization::CompoundPrefixIndex { ref details, .. } => {
                match self.kind {
                    EdgeQueryCandidateSourceKind::CompoundEqualityIndex => {
                        QueryPlanNode::CompoundEqualityIndex {
                            details: details.clone(),
                        }
                    }
                    EdgeQueryCandidateSourceKind::CompoundRangeIndex => {
                        QueryPlanNode::CompoundRangeIndex {
                            details: details.clone(),
                        }
                    }
                    _ => self.kind.plan_node(),
                }
            }
            EdgeCandidateMaterialization::CompoundRangeIndex { ref details, .. } => {
                QueryPlanNode::CompoundRangeIndex {
                    details: details.clone(),
                }
            }
            _ => self.kind.plan_node(),
        }
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
                | EdgeQueryCandidateSourceKind::CompoundEqualityIndex
                | EdgeQueryCandidateSourceKind::CompoundRangeIndex
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
            | EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex
            | EdgeQueryCandidateSourceKind::CompoundRangeIndex => (12, 2),
            EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex
            | EdgeQueryCandidateSourceKind::CompoundEqualityIndex => (8, 2),
            EdgeQueryCandidateSourceKind::EdgeLabelIndex
            | EdgeQueryCandidateSourceKind::EdgeMetadataScan => (24, 3),
            EdgeQueryCandidateSourceKind::FallbackFullEdgeScan => (64, 4),
        };
        setup.saturating_add(count.saturating_mul(candidate_weight))
    }

    // Identical to EdgePhysicalPlan::plan_cost on a Source plan (which
    // delegates here) without constructing or cloning the plan/source.
    fn plan_cost(&self) -> PlanCost {
        PlanCost {
            estimated_work: self.estimate.apply_cost_penalties(self.estimated_work()),
            estimated_candidates: self.estimate.known_upper_bound(),
            estimate_kind_rank: self.estimate.kind.rank(),
            confidence_rank: self.estimate.confidence.rank(),
            stale_risk_rank: self.estimate.stale_risk.rank(),
            materialization_rank: self.materialization.materialization_class().rank(),
            source_rank: self.kind.source_rank(),
            canonical_key: self.canonical_key.clone(),
        }
    }
}

impl NodePhysicalPlan {
    fn source(source: PlannedNodeCandidateSource) -> Self {
        Self::Source(source)
    }

    // True when every reachable source is an eager index kind, so summed
    // estimates remain trusted posting upper bounds for union caps.
    fn members_are_eager_index_sources(&self) -> bool {
        match self {
            NodePhysicalPlan::Empty => false,
            NodePhysicalPlan::Source(source) => node_source_kind_is_eager_index(source.kind),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                !inputs.is_empty()
                    && inputs
                        .iter()
                        .all(NodePhysicalPlan::members_are_eager_index_sources)
            }
        }
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
        // Source plans early-return before any shared work is computed.
        let base_work = match self {
            NodePhysicalPlan::Empty => 0,
            NodePhysicalPlan::Source(source) => return source.plan_cost(),
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
        let estimate = self.estimate();
        PlanCost {
            estimated_work: estimate.apply_cost_penalties(base_work),
            estimated_candidates: estimate.known_upper_bound(),
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

    fn contains_compound_source(&self) -> bool {
        match self {
            NodePhysicalPlan::Source(source) => matches!(
                source.kind,
                NodeQueryCandidateSourceKind::CompoundEqualityIndex
                    | NodeQueryCandidateSourceKind::CompoundRangeIndex
            ),
            NodePhysicalPlan::Intersect(inputs) | NodePhysicalPlan::Union(inputs) => {
                inputs.iter().any(NodePhysicalPlan::contains_compound_source)
            }
            NodePhysicalPlan::Empty => false,
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

    // See NodePhysicalPlan::members_are_eager_index_sources.
    fn members_are_eager_index_sources(&self) -> bool {
        match self {
            EdgePhysicalPlan::Empty => false,
            EdgePhysicalPlan::Source(source) => edge_source_kind_is_eager_index(source.kind),
            EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
                !inputs.is_empty()
                    && inputs
                        .iter()
                        .all(EdgePhysicalPlan::members_are_eager_index_sources)
            }
        }
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
                .min_by_key(|plan| plan.plan_cost())
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
            EdgePhysicalPlan::Union(_) => cap_context.union_total_cap(
                self.members_are_eager_index_sources(),
                query_limit,
                estimate,
            ),
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
        // Source plans early-return before any shared work is computed.
        let base_work = match self {
            EdgePhysicalPlan::Empty => 0,
            EdgePhysicalPlan::Source(source) => return source.plan_cost(),
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
        let estimate = self.estimate();
        PlanCost {
            estimated_work: estimate.apply_cost_penalties(base_work),
            estimated_candidates: estimate.known_upper_bound(),
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

    fn contains_compound_source(&self) -> bool {
        match self {
            EdgePhysicalPlan::Source(source) => matches!(
                source.kind,
                EdgeQueryCandidateSourceKind::CompoundEqualityIndex
                    | EdgeQueryCandidateSourceKind::CompoundRangeIndex
            ),
            EdgePhysicalPlan::Intersect(inputs) | EdgePhysicalPlan::Union(inputs) => {
                inputs.iter().any(EdgePhysicalPlan::contains_compound_source)
            }
            EdgePhysicalPlan::Empty => false,
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
    // One comparison rule for every pair: branching the rule on pair state
    // (complete vs incomplete) makes the order intransitive, which both
    // violates Ord's contract (sort_by may panic) and discards estimates
    // whenever coverage is mixed. Incomplete plans carry their accumulated
    // work plus a per-unbound-edge unknown-work penalty in estimated_work,
    // so complete plans still win against them on the first key.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.estimated_work
            .cmp(&other.estimated_work)
            .then_with(|| self.simulated_frontier.cmp(&other.simulated_frontier))
            .then_with(|| (!self.fanout_complete).cmp(&!other.fanout_complete))
            .then_with(|| self.confidence_rank.cmp(&other.confidence_rank))
            .then_with(|| self.stale_risk_rank.cmp(&other.stale_risk_rank))
            .then_with(|| self.hub_risk_rank.cmp(&other.hub_risk_rank))
            .then_with(|| self.frontier_capped.cmp(&other.frontier_capped))
            .then_with(|| self.anchor_cost.cmp(&other.anchor_cost))
            .then_with(|| self.source_rank.cmp(&other.source_rank))
            .then_with(|| self.canonical_key.cmp(&other.canonical_key))
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
    edge_source_cost: Option<&GraphRowEdgeSourceCost>,
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

// Must mirror GraphRowPlanCost::cmp key-for-key.
fn graph_row_plan_cost_rejection_reason(
    rejected: &GraphRowPlanCost,
    winner: &GraphRowPlanCost,
) -> &'static str {
    if rejected.estimated_work != winner.estimated_work {
        "estimated_work"
    } else if rejected.simulated_frontier != winner.simulated_frontier {
        "simulated_frontier"
    } else if rejected.fanout_complete != winner.fanout_complete {
        "fanout_coverage"
    } else if rejected.confidence_rank != winner.confidence_rank {
        "confidence"
    } else if rejected.stale_risk_rank != winner.stale_risk_rank {
        "staleness"
    } else if rejected.hub_risk_rank != winner.hub_risk_rank {
        "hub_risk"
    } else if rejected.frontier_capped != winner.frontier_capped {
        "frontier_cap"
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
                | EdgeQueryCandidateSourceKind::CompoundEqualityIndex
                | EdgeQueryCandidateSourceKind::CompoundRangeIndex
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
            | NodeCandidateMaterialization::TimestampIndex { .. }
            | NodeCandidateMaterialization::CompoundPrefixIndex { .. }
            | NodeCandidateMaterialization::CompoundRangeIndex { .. } => {
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
            | EdgeCandidateMaterialization::EdgePropertyRangeIndex { .. }
            | EdgeCandidateMaterialization::CompoundPrefixIndex { .. }
            | EdgeCandidateMaterialization::CompoundRangeIndex { .. } => {
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
            NodeQueryCandidateSourceKind::PropertyEqualityIndex
            | NodeQueryCandidateSourceKind::CompoundEqualityIndex => (8, 2),
            NodeQueryCandidateSourceKind::PropertyRangeIndex
            | NodeQueryCandidateSourceKind::CompoundRangeIndex
            | NodeQueryCandidateSourceKind::TimestampIndex => (12, 2),
            NodeQueryCandidateSourceKind::NodeLabelIndex
            | NodeQueryCandidateSourceKind::FallbackNodeLabelScan => (24, 3),
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => (64, 4),
        };
        setup.saturating_add(count.saturating_mul(candidate_weight))
    }

    // Identical to NodePhysicalPlan::plan_cost on a Source plan (which
    // delegates here) without constructing or cloning the plan/source.
    fn plan_cost(&self) -> PlanCost {
        PlanCost {
            estimated_work: self.estimate.apply_cost_penalties(self.estimated_work()),
            estimated_candidates: self.estimate.known_upper_bound(),
            estimate_kind_rank: self.estimate.kind.rank(),
            confidence_rank: self.estimate.confidence.rank(),
            stale_risk_rank: self.estimate.stale_risk.rank(),
            materialization_rank: self.materialization.materialization_class().rank(),
            source_rank: self.kind.selectivity_rank(),
            canonical_key: self.canonical_key.clone(),
        }
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
    fn zero() -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: GraphRowHubRisk::Low,
            confidence: EstimateConfidence::Exact,
            coverage: GraphRowFanoutCoverage::Complete,
        }
    }

    fn unknown() -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: GraphRowHubRisk::Unknown,
            confidence: EstimateConfidence::Unknown,
            coverage: GraphRowFanoutCoverage::Missing,
        }
    }

    fn from_rollup(
        rollup: &crate::planner_stats::AdjacencyRollupStats,
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
        }
    }

    fn combine_sum(self, other: Self) -> Self {
        Self {
            avg_upper_fanout: self
                .avg_upper_fanout
                .saturating_add(other.avg_upper_fanout),
            p99_fanout: self.p99_fanout.saturating_add(other.p99_fanout),
            max_fanout: self.max_fanout.saturating_add(other.max_fanout),
            hub_risk: higher_graph_row_hub_risk(self.hub_risk, other.hub_risk),
            confidence: weaker_confidence(self.confidence, other.confidence),
            coverage: worse_graph_row_fanout_coverage(self.coverage, other.coverage),
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
                .visible_node_time_range_count_at(label_id, lower_ms, upper_ms, self.snapshot_seq)
                as u64;
            for epoch in &self.immutable_epochs {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .visible_node_time_range_count_at(
                            label_id,
                            lower_ms,
                            upper_ms,
                            self.snapshot_seq,
                        ) as u64,
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

    fn compound_coverage_rank_and_label(used_stats: bool, used_fallback: bool) -> (u8, String) {
        match (used_stats, used_fallback) {
            (true, false) => (0, "complete".to_string()),
            (true, true) => (1, "partial".to_string()),
            (false, true) => (2, "fallback".to_string()),
            (false, false) => (0, "memtable-only".to_string()),
        }
    }

    fn compound_range_rollup_estimate(
        rollup: &crate::planner_stats::CompoundIndexRollupStats,
        prefix_bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
        range_field_ordinal: usize,
        numeric_lower: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        numeric_upper: Option<(crate::planner_stats::RangeStatsKey, bool)>,
    ) -> (u64, bool) {
        let Some(range_stats) = rollup.range_stats.iter().find(|stats| {
            stats.equality_prefix_len as usize == matched_prefix_len
                && stats.range_field_ordinal as usize == range_field_ordinal
        }) else {
            return (rollup.total_postings, false);
        };
        if range_stats.total_numeric_entries == 0 {
            return (0, false);
        }
        // Range estimates use the numeric bound fraction, capped by the
        // matching equality-prefix estimate. The histogram spans every
        // equality prefix in the block, so its in-range fraction scales the
        // per-prefix posting estimate under an independence assumption.
        let in_range = range_stats.estimate_range_postings(numeric_lower, numeric_upper);
        let (prefix_estimate, _) =
            Self::compound_prefix_rollup_estimate(rollup, prefix_bounds, matched_prefix_len);
        let scaled = ((prefix_estimate as u128).saturating_mul(in_range.count as u128)
            / range_stats.total_numeric_entries as u128) as u64;
        let estimate = scaled
            .max(u64::from(in_range.count > 0 && prefix_estimate > 0))
            .min(prefix_estimate)
            .min(range_stats.total_numeric_entries);
        // Only an exact empty range is truly exact; any scaled product is an
        // estimate even when both inputs are exact.
        (estimate, in_range.exact && in_range.count == 0)
    }

    fn compound_prefix_rollup_estimate(
        rollup: &crate::planner_stats::CompoundIndexRollupStats,
        bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
    ) -> (u64, bool) {
        let Some(prefix_stats) = rollup
            .prefix_stats
            .iter()
            .find(|stats| stats.prefix_len as usize == matched_prefix_len)
        else {
            return (rollup.total_postings, false);
        };
        let mut total = 0u64;
        let mut exact = true;
        let average = if prefix_stats.distinct_prefixes == 0 {
            0
        } else {
            rollup
                .total_postings
                .div_ceil(prefix_stats.distinct_prefixes)
                .max(1)
        };
        let fallback = prefix_stats.max_postings_per_prefix.max(average);
        for bound in bounds {
            // The exact-prefix list is sorted by encoded prefix on both the
            // per-segment build and rollup-merge paths.
            match prefix_stats.exact_prefix_postings.binary_search_by(|stat| {
                stat.encoded_prefix.as_slice().cmp(bound.lower.as_slice())
            }) {
                Ok(found) => {
                    total = total
                        .saturating_add(prefix_stats.exact_prefix_postings[found].postings);
                }
                Err(_) => {
                    exact = false;
                    total = total.saturating_add(fallback);
                }
            }
        }
        (total, exact)
    }

    fn compound_segment_is_stats_uncovered(
        coverage: Option<&crate::planner_stats::PlannerStatsFamilyCoverage>,
        segment_id: u64,
    ) -> bool {
        match coverage {
            Some(coverage) => {
                coverage.uncovered_segment_ids.contains(&segment_id)
                    || coverage.mismatched_segment_ids.contains(&segment_id)
            }
            None => true,
        }
    }

    // Planner review P4: stats-uncovered segments previously charged the
    // whole label cardinality, so compound candidates systematically lost
    // (or were skipped as broad) during stats gaps. Count real postings from
    // the compound sidecar key table instead — no posting decode, early exit
    // past the hard candidate cap where every planner decision is already
    // made. A missing or corrupt sidecar falls back to the label charge for
    // that segment only.
    fn compound_uncovered_prefix_count(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        coverage: Option<&crate::planner_stats::PlannerStatsFamilyCoverage>,
        target_label_count: u64,
    ) -> u64 {
        let cap =
            (crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP as u64).saturating_add(1);
        let mut total = 0u64;
        for segment in &self.segments {
            if !Self::compound_segment_is_stats_uncovered(coverage, segment.segment_id) {
                continue;
            }
            let mut segment_count = 0u64;
            let mut fallback = false;
            for bound in bounds {
                let Some(remaining_cap) =
                    remaining_compound_count_cap(cap, total, segment_count)
                else {
                    break;
                };
                match segment.compound_prefix_posting_count_if_present(
                    entry,
                    bound,
                    remaining_cap,
                ) {
                    Ok(Some(count)) => {
                        segment_count = segment_count.saturating_add(count);
                    }
                    Ok(None) | Err(_) => {
                        fallback = true;
                        break;
                    }
                }
            }
            total = total.saturating_add(if fallback {
                target_label_count
            } else {
                segment_count
            });
            if total >= cap {
                break;
            }
        }
        total
    }

    fn compound_uncovered_range_count(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundRangeBounds],
        coverage: Option<&crate::planner_stats::PlannerStatsFamilyCoverage>,
        target_label_count: u64,
    ) -> u64 {
        let cap =
            (crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP as u64).saturating_add(1);
        let mut total = 0u64;
        for segment in &self.segments {
            if !Self::compound_segment_is_stats_uncovered(coverage, segment.segment_id) {
                continue;
            }
            let mut segment_count = 0u64;
            let mut fallback = false;
            for bound in bounds {
                let Some(remaining_cap) =
                    remaining_compound_count_cap(cap, total, segment_count)
                else {
                    break;
                };
                match segment.compound_range_posting_count_if_present(
                    entry,
                    bound,
                    remaining_cap,
                ) {
                    Ok(Some(count)) => {
                        segment_count = segment_count.saturating_add(count);
                    }
                    Ok(None) | Err(_) => {
                        fallback = true;
                        break;
                    }
                }
            }
            total = total.saturating_add(if fallback {
                target_label_count
            } else {
                segment_count
            });
            if total >= cap {
                break;
            }
        }
        total
    }

    fn node_compound_prefix_estimate(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
        target_label_count: u64,
    ) -> (PlannerEstimate, u8, String) {
        let mut count = bounds
            .iter()
            .map(|bound| {
                self.memtable
                    .count_node_compound_prefix_at(entry.index_id, bound, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                bounds
                    .iter()
                    .map(|bound| {
                        epoch.memtable.count_node_compound_prefix_at(
                            entry.index_id,
                            bound,
                            self.snapshot_seq,
                        ) as u64
                    })
                    .sum::<u64>(),
            );
        }
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_exact = true;
        if let Some(rollup) = self.planner_stats.compound_index_rollups.get(&entry.index_id) {
            let (stats_count, exact) =
                Self::compound_prefix_rollup_estimate(rollup, bounds, matched_prefix_len);
            used_stats = rollup.coverage.covered_count() > 0;
            stats_exact = exact;
            count = count.saturating_add(stats_count);
            if rollup.coverage.has_uncovered() {
                used_fallback = true;
                count = count.saturating_add(self.compound_uncovered_prefix_count(
                    entry,
                    bounds,
                    Some(&rollup.coverage),
                    target_label_count,
                ));
            }
        } else if !self.segments.is_empty() {
            used_fallback = true;
            count = count.saturating_add(self.compound_uncovered_prefix_count(
                entry,
                bounds,
                None,
                target_label_count,
            ));
        }
        let (coverage_rank, coverage) = Self::compound_coverage_rank_and_label(used_stats, used_fallback);
        (
            self.planner_stats_estimate_from_rollup(
                count,
                used_stats,
                used_fallback,
                stats_exact,
            ),
            coverage_rank,
            coverage,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn node_compound_range_estimate(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundRangeBounds],
        prefix_bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
        range_field_ordinal: usize,
        numeric_lower: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        numeric_upper: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        target_label_count: u64,
    ) -> (PlannerEstimate, u8, String) {
        let mut count = bounds
            .iter()
            .map(|bound| {
                self.memtable
                    .count_node_compound_range_at(entry.index_id, bound, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                bounds
                    .iter()
                    .map(|bound| {
                        epoch.memtable.count_node_compound_range_at(
                            entry.index_id,
                            bound,
                            self.snapshot_seq,
                        ) as u64
                    })
                    .sum::<u64>(),
            );
        }
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_exact = false;
        if let Some(rollup) = self.planner_stats.compound_index_rollups.get(&entry.index_id) {
            let (stats_count, exact) = Self::compound_range_rollup_estimate(
                rollup,
                prefix_bounds,
                matched_prefix_len,
                range_field_ordinal,
                numeric_lower,
                numeric_upper,
            );
            used_stats = rollup.coverage.covered_count() > 0;
            stats_exact = exact;
            count = count.saturating_add(stats_count);
            if rollup.coverage.has_uncovered() {
                used_fallback = true;
                count = count.saturating_add(self.compound_uncovered_range_count(
                    entry,
                    bounds,
                    Some(&rollup.coverage),
                    target_label_count,
                ));
            }
        } else if !self.segments.is_empty() {
            used_fallback = true;
            count = count.saturating_add(self.compound_uncovered_range_count(
                entry,
                bounds,
                None,
                target_label_count,
            ));
        }
        let (coverage_rank, coverage) = Self::compound_coverage_rank_and_label(used_stats, used_fallback);
        (
            self.planner_stats_estimate_from_rollup(
                count,
                used_stats,
                used_fallback,
                stats_exact,
            ),
            coverage_rank,
            coverage,
        )
    }

    fn edge_compound_prefix_estimate(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
        target_label_count: u64,
    ) -> (PlannerEstimate, u8, String) {
        let mut count = bounds
            .iter()
            .map(|bound| {
                self.memtable
                    .count_edge_compound_prefix_at(entry.index_id, bound, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                bounds
                    .iter()
                    .map(|bound| {
                        epoch.memtable.count_edge_compound_prefix_at(
                            entry.index_id,
                            bound,
                            self.snapshot_seq,
                        ) as u64
                    })
                    .sum::<u64>(),
            );
        }
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_exact = true;
        if let Some(rollup) = self.planner_stats.compound_index_rollups.get(&entry.index_id) {
            let (stats_count, exact) =
                Self::compound_prefix_rollup_estimate(rollup, bounds, matched_prefix_len);
            used_stats = rollup.coverage.covered_count() > 0;
            stats_exact = exact;
            count = count.saturating_add(stats_count);
            if rollup.coverage.has_uncovered() {
                used_fallback = true;
                count = count.saturating_add(self.compound_uncovered_prefix_count(
                    entry,
                    bounds,
                    Some(&rollup.coverage),
                    target_label_count,
                ));
            }
        } else if !self.segments.is_empty() {
            used_fallback = true;
            count = count.saturating_add(self.compound_uncovered_prefix_count(
                entry,
                bounds,
                None,
                target_label_count,
            ));
        }
        let (coverage_rank, coverage) = Self::compound_coverage_rank_and_label(used_stats, used_fallback);
        (
            self.planner_stats_estimate_from_rollup(
                count,
                used_stats,
                used_fallback,
                stats_exact,
            ),
            coverage_rank,
            coverage,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn edge_compound_range_estimate(
        &self,
        entry: &SecondaryIndexManifestEntry,
        bounds: &[crate::secondary_index_key::CompoundRangeBounds],
        prefix_bounds: &[crate::secondary_index_key::CompoundPrefixBounds],
        matched_prefix_len: usize,
        range_field_ordinal: usize,
        numeric_lower: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        numeric_upper: Option<(crate::planner_stats::RangeStatsKey, bool)>,
        target_label_count: u64,
    ) -> (PlannerEstimate, u8, String) {
        let mut count = bounds
            .iter()
            .map(|bound| {
                self.memtable
                    .count_edge_compound_range_at(entry.index_id, bound, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                bounds
                    .iter()
                    .map(|bound| {
                        epoch.memtable.count_edge_compound_range_at(
                            entry.index_id,
                            bound,
                            self.snapshot_seq,
                        ) as u64
                    })
                    .sum::<u64>(),
            );
        }
        let mut used_stats = false;
        let mut used_fallback = false;
        let mut stats_exact = false;
        if let Some(rollup) = self.planner_stats.compound_index_rollups.get(&entry.index_id) {
            let (stats_count, exact) = Self::compound_range_rollup_estimate(
                rollup,
                prefix_bounds,
                matched_prefix_len,
                range_field_ordinal,
                numeric_lower,
                numeric_upper,
            );
            used_stats = rollup.coverage.covered_count() > 0;
            stats_exact = exact;
            count = count.saturating_add(stats_count);
            if rollup.coverage.has_uncovered() {
                used_fallback = true;
                count = count.saturating_add(self.compound_uncovered_range_count(
                    entry,
                    bounds,
                    Some(&rollup.coverage),
                    target_label_count,
                ));
            }
        } else if !self.segments.is_empty() {
            used_fallback = true;
            count = count.saturating_add(self.compound_uncovered_range_count(
                entry,
                bounds,
                None,
                target_label_count,
            ));
        }
        let (coverage_rank, coverage) = Self::compound_coverage_rank_and_label(used_stats, used_fallback);
        (
            self.planner_stats_estimate_from_rollup(
                count,
                used_stats,
                used_fallback,
                stats_exact,
            ),
            coverage_rank,
            coverage,
        )
    }

    fn compound_estimate_skips_as_broad(
        estimate: PlannerEstimate,
        cheapest_legal_count: Option<u64>,
    ) -> bool {
        let Some(count) = estimate.known_upper_bound() else {
            return true;
        };
        // Execution materializes at most PLANNER_STATS_HARD_CANDIDATE_CAP ids
        // from a compound source, so keeping a broader candidate guarantees a
        // wasted TooBroad probe before the legal-universe fallback re-drives
        // the query. Only keep such a candidate when no legal fallback exists.
        count > crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP as u64
            && cheapest_legal_count.is_some()
    }

    fn node_compound_label_name(&self, label_id: u32) -> Option<String> {
        self.label_catalog
            .node_label(label_id)
            .map(ToString::to_string)
    }

    fn edge_compound_label_name(&self, label_id: u32) -> Option<String> {
        self.label_catalog
            .edge_label(label_id)
            .map(ToString::to_string)
    }

    fn best_node_compound_candidate(
        &self,
        query: &NormalizedNodeQuery,
        filter: &NormalizedNodeFilter,
        cap_context: QueryCapContext,
        warnings: &mut Vec<QueryPlanWarning>,
    ) -> Result<Option<CompoundNodeCandidateSelection>, EngineError> {
        // Constraint-set construction clones property values and ID lists, so
        // bail out before building it when no candidate label has any
        // compound declaration — the common case for multi-predicate queries
        // in databases without compound indexes.
        let label_has_declarations = |label_id: u32| {
            !self
                .node_field_index_entries(label_id, &SecondaryIndexKind::Equality)
                .is_empty()
                || !self
                    .node_field_index_entries(label_id, &SecondaryIndexKind::Range)
                    .is_empty()
        };
        let any_label_declared = if let Some(label_ids) = node_index_candidate_labels(query) {
            label_ids
                .as_slice()
                .iter()
                .any(|&label_id| label_has_declarations(label_id))
        } else if let ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::Any,
            ref label_ids,
            ..
        } = query.label_filter
        {
            label_ids
                .as_slice()
                .iter()
                .any(|&label_id| label_has_declarations(label_id))
        } else {
            false
        };
        if !any_label_declared {
            return Ok(None);
        }

        let constraints = node_compound_constraints(query, filter);
        let has_constraints =
            !constraints.equalities.is_empty() || !constraints.ranges.is_empty();
        let mut prefix_unsatisfied = false;

        if let Some(label_ids) = node_index_candidate_labels(query) {
            let mut best: Option<CompoundNodeCandidatePlan> = None;
            for &label_id in label_ids.as_slice() {
                if let Some(candidate) = self.best_node_compound_candidate_for_label(
                    label_id,
                    &constraints,
                    filter,
                    cap_context,
                    warnings,
                    &mut prefix_unsatisfied,
                )? {
                    if best
                        .as_ref()
                        .is_none_or(|current| candidate.score < current.score)
                    {
                        best = Some(candidate);
                    }
                }
            }
            if best.is_none() && prefix_unsatisfied && has_constraints {
                add_plan_warning(warnings, QueryPlanWarning::CompoundIndexPrefixNotSatisfied);
            }
            return Ok(best.map(|candidate| CompoundNodeCandidateSelection {
                sources: vec![candidate.source],
            }));
        }

        // Multi-label `Any` can use compound declarations only as a union
        // with one Ready branch per label; a partial union would drop rows,
        // so any uncovered label abandons the union to the existing
        // label/scan path.
        let ResolvedNodeLabelFilter::LabelSet {
            mode: LabelMatchMode::Any,
            label_ids,
            ..
        } = query.label_filter
        else {
            return Ok(None);
        };
        if !has_constraints {
            return Ok(None);
        }
        let mut sources = Vec::with_capacity(label_ids.as_slice().len());
        let mut union_estimate = 0u64;
        let mut union_complete = true;
        for &label_id in label_ids.as_slice() {
            let Some(candidate) = self.best_node_compound_candidate_for_label(
                label_id,
                &constraints,
                filter,
                cap_context,
                warnings,
                &mut prefix_unsatisfied,
            )?
            else {
                union_complete = false;
                break;
            };
            union_estimate =
                union_estimate.saturating_add(candidate.score.estimated_candidates);
            sources.push(candidate.source);
        }
        if !union_complete {
            if prefix_unsatisfied && has_constraints {
                add_plan_warning(warnings, QueryPlanWarning::CompoundIndexPrefixNotSatisfied);
            }
            return Ok(None);
        }
        // The union competes against the shared `Any` fallback scan. A
        // strictly worse union is abandoned here; ties are offered so the
        // downstream cost model (which also weighs confidence and fanout)
        // makes the final choice, matching how single-label compound
        // candidates compete.
        let fallback_estimate = self
            .node_label_filter_estimate(&label_ids, LabelMatchMode::Any)?
            .estimate;
        if fallback_estimate
            .known_upper_bound()
            .is_some_and(|scan_count| union_estimate > scan_count)
        {
            return Ok(None);
        }
        if compound_union_estimate_exceeds_materialization_cap(
            cap_context,
            query.page.limit,
            union_estimate,
        ) {
            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
            return Ok(None);
        }
        if cap_context
            .cheapest_legal_count()
            .is_some_and(|legal| legal <= union_estimate)
        {
            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
            return Ok(None);
        }
        Ok(Some(CompoundNodeCandidateSelection { sources }))
    }

    #[allow(clippy::too_many_arguments)]
    fn best_node_compound_candidate_for_label(
        &self,
        label_id: u32,
        constraints: &CompoundFieldConstraints,
        filter: &NormalizedNodeFilter,
        cap_context: QueryCapContext,
        warnings: &mut Vec<QueryPlanWarning>,
        prefix_unsatisfied: &mut bool,
    ) -> Result<Option<CompoundNodeCandidatePlan>, EngineError> {
        let mut best: Option<CompoundNodeCandidatePlan> = None;
        {
            let target_label_estimate = self.node_label_estimate(label_id)?;
            let target_label_count = target_label_estimate.known_upper_bound().unwrap_or(u64::MAX / 4);
            for kind in [SecondaryIndexKind::Equality, SecondaryIndexKind::Range] {
                for entry in self.node_field_index_entries(label_id, &kind) {
                    let encoded = match encode_compound_bounds_for_entry(entry, constraints) {
                        CompoundBoundsOutcome::Bounds(encoded) => encoded,
                        CompoundBoundsOutcome::InExpansionCapExceeded => {
                            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                            continue;
                        }
                        CompoundBoundsOutcome::PrefixNotSatisfied => {
                            *prefix_unsatisfied = true;
                            continue;
                        }
                        CompoundBoundsOutcome::Ineligible => continue,
                    };
                    let fields = entry.target.public_fields();
                    let (source, score) = match encoded {
                        CompoundEncodedBounds::Prefix {
                            bounds,
                            matched_prefix_len,
                        } => {
                            let in_expansions = bounds.len();
                            let (estimate, coverage_rank, coverage) =
                                self.node_compound_prefix_estimate(
                                    entry,
                                    &bounds,
                                    matched_prefix_len,
                                    target_label_count,
                                );
                            if Self::compound_estimate_skips_as_broad(
                                estimate,
                                cap_context.cheapest_legal_count(),
                            ) {
                                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                                continue;
                            }
                            let residual_predicates = node_compound_residual_predicates(
                                filter,
                                &fields[..matched_prefix_len],
                                None,
                            );
                            let details = CompoundIndexPlanDetails {
                                index_id: entry.index_id,
                                target_kind: QueryPlanCompoundTargetKind::Node,
                                label: self.node_compound_label_name(label_id),
                                kind: entry.kind.clone(),
                                fields,
                                compound: true,
                                matched_prefix_len,
                                range_field: None,
                                in_expansions,
                                estimated_candidates: estimate.known_upper_bound(),
                                coverage,
                                residual_predicates,
                                final_verification: true,
                                fallback_reason: compound_coverage_fallback_reason(coverage_rank),
                            };
                            let score = CompoundCandidateScore {
                                estimated_candidates: estimate.known_upper_bound().unwrap_or(u64::MAX),
                                matched_prefix_len,
                                has_range: false,
                                in_expansions,
                                coverage_rank,
                                index_id: entry.index_id,
                            };
                            (
                                PlannedNodeCandidateSource::compound_prefix_index(
                                    entry.clone(), bounds, estimate, details,
                                ),
                                score,
                            )
                        }
                        CompoundEncodedBounds::Range {
                            bounds,
                            prefix_bounds,
                            matched_prefix_len,
                            range_field,
                            numeric_lower,
                            numeric_upper,
                        } => {
                            let in_expansions = bounds.len();
                            let (estimate, coverage_rank, coverage) =
                                self.node_compound_range_estimate(
                                    entry,
                                    &bounds,
                                    &prefix_bounds,
                                    matched_prefix_len,
                                    matched_prefix_len,
                                    numeric_lower,
                                    numeric_upper,
                                    target_label_count,
                                );
                            if Self::compound_estimate_skips_as_broad(
                                estimate,
                                cap_context.cheapest_legal_count(),
                            ) {
                                add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                                continue;
                            }
                            let residual_predicates = node_compound_residual_predicates(
                                filter,
                                &fields[..matched_prefix_len],
                                Some(&range_field),
                            );
                            let details = CompoundIndexPlanDetails {
                                index_id: entry.index_id,
                                target_kind: QueryPlanCompoundTargetKind::Node,
                                label: self.node_compound_label_name(label_id),
                                kind: entry.kind.clone(),
                                fields,
                                compound: true,
                                matched_prefix_len,
                                range_field: Some(range_field),
                                in_expansions,
                                estimated_candidates: estimate.known_upper_bound(),
                                coverage,
                                residual_predicates,
                                final_verification: true,
                                fallback_reason: compound_coverage_fallback_reason(coverage_rank),
                            };
                            let score = CompoundCandidateScore {
                                estimated_candidates: estimate.known_upper_bound().unwrap_or(u64::MAX),
                                matched_prefix_len,
                                has_range: true,
                                in_expansions,
                                coverage_rank,
                                index_id: entry.index_id,
                            };
                            (
                                PlannedNodeCandidateSource::compound_range_index(
                                    entry.clone(), bounds, estimate, details,
                                ),
                                score,
                            )
                        }
                    };
                    if best
                        .as_ref()
                        .is_none_or(|current| score < current.score)
                    {
                        best = Some(CompoundNodeCandidatePlan { source, score });
                    }
                }
            }
        }
        Ok(best)
    }

    fn best_edge_compound_candidate(
        &self,
        query: &NormalizedEdgeQuery,
        filter: &NormalizedEdgeFilter,
        cap_context: EdgeQueryCapContext,
        warnings: &mut Vec<QueryPlanWarning>,
    ) -> Option<CompoundEdgeCandidatePlan> {
        let label_id = query.label_id?;
        // Skip constraint-set construction (value clones plus endpoint-ID
        // vector clones, which graph-row planning can pass by the thousands)
        // when the label has no compound declarations.
        if self
            .edge_field_index_entries(label_id, &SecondaryIndexKind::Equality)
            .is_empty()
            && self
                .edge_field_index_entries(label_id, &SecondaryIndexKind::Range)
                .is_empty()
        {
            return None;
        }
        let constraints = edge_compound_constraints(query, filter);
        let target_label_estimate = self.edge_label_estimate(label_id);
        let target_label_count = target_label_estimate.known_upper_bound().unwrap_or(u64::MAX / 4);
        let mut best: Option<CompoundEdgeCandidatePlan> = None;
        let mut prefix_unsatisfied = false;
        for kind in [SecondaryIndexKind::Equality, SecondaryIndexKind::Range] {
            for entry in self.edge_field_index_entries(label_id, &kind) {
                let encoded = match encode_compound_bounds_for_entry(entry, &constraints) {
                    CompoundBoundsOutcome::Bounds(encoded) => encoded,
                    CompoundBoundsOutcome::InExpansionCapExceeded => {
                        add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                        continue;
                    }
                    CompoundBoundsOutcome::PrefixNotSatisfied => {
                        prefix_unsatisfied = true;
                        continue;
                    }
                    CompoundBoundsOutcome::Ineligible => continue,
                };
                let fields = entry.target.public_fields();
                let (source, score) = match encoded {
                    CompoundEncodedBounds::Prefix {
                        bounds,
                        matched_prefix_len,
                    } => {
                        let in_expansions = bounds.len();
                        let (estimate, coverage_rank, coverage) =
                            self.edge_compound_prefix_estimate(
                                entry,
                                &bounds,
                                matched_prefix_len,
                                target_label_count,
                            );
                        if Self::compound_estimate_skips_as_broad(
                            estimate,
                            cap_context.cheapest_legal_count(),
                        ) {
                            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                            continue;
                        }
                        let residual_predicates = edge_compound_residual_predicates(
                            filter,
                            &fields[..matched_prefix_len],
                            None,
                        );
                        let details = CompoundIndexPlanDetails {
                            index_id: entry.index_id,
                            target_kind: QueryPlanCompoundTargetKind::Edge,
                            label: self.edge_compound_label_name(label_id),
                            kind: entry.kind.clone(),
                            fields,
                            compound: true,
                            matched_prefix_len,
                            range_field: None,
                            in_expansions,
                            estimated_candidates: estimate.known_upper_bound(),
                            coverage,
                            residual_predicates,
                            final_verification: true,
                            fallback_reason: compound_coverage_fallback_reason(coverage_rank),
                        };
                        let score = CompoundCandidateScore {
                            estimated_candidates: estimate.known_upper_bound().unwrap_or(u64::MAX),
                            matched_prefix_len,
                            has_range: false,
                            in_expansions,
                            coverage_rank,
                            index_id: entry.index_id,
                        };
                        (
                            PlannedEdgeCandidateSource::edge_compound_prefix_index(
                                entry.clone(), bounds, estimate, details,
                            ),
                            score,
                        )
                    }
                    CompoundEncodedBounds::Range {
                        bounds,
                        prefix_bounds,
                        matched_prefix_len,
                        range_field,
                        numeric_lower,
                        numeric_upper,
                    } => {
                        let in_expansions = bounds.len();
                        let (estimate, coverage_rank, coverage) =
                            self.edge_compound_range_estimate(
                                entry,
                                &bounds,
                                &prefix_bounds,
                                matched_prefix_len,
                                matched_prefix_len,
                                numeric_lower,
                                numeric_upper,
                                target_label_count,
                            );
                        if Self::compound_estimate_skips_as_broad(
                            estimate,
                            cap_context.cheapest_legal_count(),
                        ) {
                            add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                            continue;
                        }
                        let residual_predicates = edge_compound_residual_predicates(
                            filter,
                            &fields[..matched_prefix_len],
                            Some(&range_field),
                        );
                        let details = CompoundIndexPlanDetails {
                            index_id: entry.index_id,
                            target_kind: QueryPlanCompoundTargetKind::Edge,
                            label: self.edge_compound_label_name(label_id),
                            kind: entry.kind.clone(),
                            fields,
                            compound: true,
                            matched_prefix_len,
                            range_field: Some(range_field),
                            in_expansions,
                            estimated_candidates: estimate.known_upper_bound(),
                            coverage,
                            residual_predicates,
                            final_verification: true,
                            fallback_reason: compound_coverage_fallback_reason(coverage_rank),
                        };
                        let score = CompoundCandidateScore {
                            estimated_candidates: estimate.known_upper_bound().unwrap_or(u64::MAX),
                            matched_prefix_len,
                            has_range: true,
                            in_expansions,
                            coverage_rank,
                            index_id: entry.index_id,
                        };
                        (
                            PlannedEdgeCandidateSource::edge_compound_range_index(
                                entry.clone(), bounds, estimate, details,
                            ),
                            score,
                        )
                    }
                };
                if best
                    .as_ref()
                    .is_none_or(|current| score < current.score)
                {
                    best = Some(CompoundEdgeCandidatePlan { source, score });
                }
            }
        }
        if best.is_none()
            && prefix_unsatisfied
            && (!constraints.equalities.is_empty() || !constraints.ranges.is_empty())
        {
            add_plan_warning(warnings, QueryPlanWarning::CompoundIndexPrefixNotSatisfied);
        }
        best
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
        add_memtable_estimate(self.memtable.visible_endpoint_edges_count_estimate_at(
            &sorted_node_ids,
            direction,
            label_filter_ids,
            self.snapshot_seq,
        ));
        for epoch in &self.immutable_epochs {
            add_memtable_estimate(epoch.memtable.visible_endpoint_edges_count_estimate_at(
                &sorted_node_ids,
                direction,
                label_filter_ids,
                self.snapshot_seq,
            ));
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
        let Some(label_ids) = label_filter_ids else {
            return self
                .graph_row_fanout_rollup_estimate(
                    direction,
                    None,
                    GraphRowFanoutCoverage::Complete,
                    EstimateConfidence::High,
                    known_source_ids,
                )
                .unwrap_or_else(GraphRowFanoutEstimate::unknown);
        };

        if label_ids.is_empty() {
            return GraphRowFanoutEstimate::zero();
        }

        if label_ids.len() == 1 {
            let label_id = label_ids[0];
            if let Some(estimate) = self.graph_row_fanout_rollup_estimate(
                direction,
                Some(label_id),
                GraphRowFanoutCoverage::Complete,
                EstimateConfidence::High,
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
                return GraphRowFanoutEstimate::zero();
            }
            return self
                .graph_row_fanout_rollup_estimate(
                    direction,
                    None,
                    GraphRowFanoutCoverage::GlobalFallback,
                    EstimateConfidence::Low,
                    known_source_ids,
                )
                .unwrap_or_else(GraphRowFanoutEstimate::unknown);
        }

        let mut combined: Option<GraphRowFanoutEstimate> = None;
        for label_id in label_ids {
            let Some(estimate) = self.graph_row_fanout_rollup_estimate(
                direction,
                Some(*label_id),
                GraphRowFanoutCoverage::Complete,
                EstimateConfidence::High,
                known_source_ids,
            ) else {
                return self
                    .graph_row_fanout_rollup_estimate(
                        direction,
                        None,
                        GraphRowFanoutCoverage::GlobalFallback,
                        EstimateConfidence::Low,
                        known_source_ids,
                    )
                    .unwrap_or_else(GraphRowFanoutEstimate::unknown);
            };
            combined = Some(match combined {
                Some(current) => current.combine_sum(estimate),
                None => estimate,
            });
        }

        combined.unwrap_or_else(GraphRowFanoutEstimate::unknown)
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
                Some(current) => current.combine_sum(estimate),
                None => estimate,
            });
        }
        let mut estimate = combined.unwrap_or_else(GraphRowFanoutEstimate::unknown);
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

    fn graph_row_node_anchor_plan(
        &self,
        node: &GraphRowRuntimeNode,
        limit: usize,
        build_explain: bool,
    ) -> Option<Result<GraphRowNodeAnchorPlan, EngineError>> {
        if !graph_row_node_query_has_anchor(&node.query) {
            return None;
        }
        let mut anchor_query = node.query.clone();
        anchor_query.page.limit = Some(limit);
        Some(
            self.plan_normalized_node_query(&anchor_query)
                .map(|planned| {
                    let estimated_candidates = planned.estimated_candidate_count();
                    let explain = build_explain.then(|| GraphRowNodeAnchorExplain {
                        plan_node: planned.driver.plan_node(),
                        warnings: planned.warnings.clone(),
                    });
                    GraphRowNodeAnchorPlan {
                        driver: planned.driver,
                        estimated_candidates,
                        explain,
                    }
                }),
        )
    }

    fn graph_row_target_selectivity_from_anchor(
        node: &GraphRowRuntimeNode,
        anchor_plan: Option<&Result<GraphRowNodeAnchorPlan, EngineError>>,
        universe_count: Option<u64>,
    ) -> Option<(u64, u64)> {
        if node.query.filter.is_always_false() {
            return None;
        }
        let target_count = anchor_plan?.as_ref().ok()?.estimated_candidates?;
        let universe_count = universe_count?;
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
        build_explain: bool,
    ) -> Result<GraphRowEdgeSourcePlanCost, EngineError> {
        self.graph_row_edge_source_plan_cost_with_endpoints(query, edge, &[], &[], build_explain)
    }

    fn graph_row_edge_source_plan_cost_with_endpoints(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        from_ids: &[u64],
        to_ids: &[u64],
        build_explain: bool,
    ) -> Result<GraphRowEdgeSourcePlanCost, EngineError> {
        if edge.filter.is_always_false()
            || edge
                .label_filter_ids
                .as_ref()
                .is_some_and(|label_ids| label_ids.is_empty())
        {
            let cost = EdgePhysicalPlan::Empty.plan_cost();
            return Ok(Some(GraphRowEdgeSourceCost {
                cost,
                detail: build_explain.then(|| {
                    "source=EmptyResult; reason=always_false_or_empty_label_filter".to_string()
                }),
                warnings: Vec::new(),
            }));
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
                from_ids: from_ids.to_vec(),
                to_ids: to_ids.to_vec(),
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
            if build_explain {
                details.push(format!(
                    "label_id={label_id:?}; from_bound={}; to_bound={}; source={:?}; estimated_candidates={:?}",
                    !from_ids.is_empty(),
                    !to_ids.is_empty(),
                    planned.driver.plan_node(),
                    planned.estimated_candidate_count()
                ));
            }
            drivers.push(planned.driver);
        }

        if drivers.is_empty() {
            return Ok(None);
        }
        let driver = EdgePhysicalPlan::union(drivers);
        let mut cost = driver.plan_cost();
        cost.estimated_work = graph_row_edge_source_materialization_work(&driver);
        finalize_plan_warnings(&mut warnings);
        Ok(Some(GraphRowEdgeSourceCost {
            cost,
            detail: build_explain.then(|| {
                format!("source=EdgeCandidateSource; {}", details.join(" | "))
            }),
            warnings,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_bound_edge_source_plan_cost<'memo>(
        &self,
        query: &NormalizedGraphRowQuery,
        edge: &GraphRowRuntimeEdge,
        edge_index: usize,
        from_bound: bool,
        to_bound: bool,
        from_ids: &[u64],
        to_ids: &[u64],
        build_explain: bool,
        memo: &'memo mut GraphRowEdgeSourceCostMemo,
    ) -> Result<&'memo GraphRowEdgeSourcePlanCost, EngineError> {
        let Some(state_index) = GraphRowEdgeSourceCostMemo::bound_state_index(from_bound, to_bound)
        else {
            return Err(EngineError::InvalidOperation(
                "graph row bound edge-source cost requested without a bound endpoint".into(),
            ));
        };
        if memo.bound_costs[edge_index][state_index].is_none() {
            let cost = self.graph_row_edge_source_plan_cost_with_endpoints(
                query,
                edge,
                if from_bound { from_ids } else { &[] },
                if to_bound { to_ids } else { &[] },
                build_explain,
            )?;
            memo.bound_costs[edge_index][state_index] = Some(cost);
        }
        Ok(memo.bound_costs[edge_index][state_index]
            .as_ref()
            .expect("graph row edge-source cost memo entry must be initialized"))
    }

    fn plan_graph_row_physical(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        build_explain: bool,
    ) -> Result<GraphRowPhysicalPlan, EngineError> {
        let known_node_ids = runtime
            .nodes
            .iter()
            .map(|node| self.graph_row_known_node_ids_for_fanout(node))
            .collect::<Vec<_>>();
        let node_anchor_limit = query.options.max_intermediate_bindings.saturating_add(1);
        let node_anchor_plans = runtime
            .nodes
            .iter()
            .map(|node| self.graph_row_node_anchor_plan(node, node_anchor_limit, build_explain))
            .collect::<Vec<_>>();
        let full_scan_count = if node_anchor_plans
            .iter()
            .any(|plan| matches!(plan, Some(Ok(plan)) if plan.estimated_candidates.is_some()))
        {
            self.full_scan_estimate().known_upper_bound()
        } else {
            None
        };
        let target_selectivities = runtime
            .nodes
            .iter()
            .enumerate()
            .map(|(node_index, node)| {
                Self::graph_row_target_selectivity_from_anchor(
                    node,
                    node_anchor_plans[node_index].as_ref(),
                    full_scan_count,
                )
            })
            .collect::<Vec<_>>();
        let edge_source_costs = runtime
            .edges
            .iter()
            .map(|edge| self.graph_row_edge_source_plan_cost(query, edge, build_explain))
            .collect::<Result<Vec<_>, _>>()?;
        let mut edge_source_cost_memo = GraphRowEdgeSourceCostMemo::new(runtime.edges.len());

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
                &mut edge_source_cost_memo,
                &node_anchor_plans,
                &known_node_ids,
                &target_selectivities,
                build_explain,
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
            notes: if build_explain {
                self.graph_row_physical_plan_notes(query)
            } else {
                Vec::new()
            },
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
        edge_source_cost_memo: &mut GraphRowEdgeSourceCostMemo,
        node_anchor_plans: &[Option<Result<GraphRowNodeAnchorPlan, EngineError>>],
        known_node_ids: &[Option<Vec<u64>>],
        target_selectivities: &[Option<(u64, u64)>],
        build_explain: bool,
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
                if build_explain {
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
                }
                continue;
            }
            match node_anchor_plans.get(node_index).and_then(Option::as_ref) {
                Some(Ok(planned)) => {
                    let mut bound = BTreeSet::new();
                    bound.insert(node.alias.clone());
                    let visited = vec![false; runtime.edges.len()];
                    let anchor_cost = planned.driver.plan_cost();
                    let initial_frontier = planned.estimated_candidates.unwrap_or(1).max(1);
                    let (edge_order, source_choices, cost) = self.graph_row_plan_from_frontier(
                        query,
                        runtime,
                        &segment.edge_indices,
                        bound,
                        visited,
                        anchor_cost,
                        initial_frontier,
                        edge_source_costs,
                        edge_source_cost_memo,
                        known_node_ids,
                        target_selectivities,
                        format!("node:{}:{node_index}", node.alias),
                        build_explain,
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
                        build_explain.then(|| {
                            let Some(explain) = planned.explain.as_ref() else {
                                return format!(
                                    "segment={segment_index}; alias={}; source=<none>; estimated_candidates={:?}; warnings=[]",
                                    node.alias, planned.estimated_candidates
                                );
                            };
                            format!(
                                "segment={segment_index}; alias={}; source={:?}; estimated_candidates={:?}; warnings={:?}",
                                node.alias,
                                explain.plan_node,
                                planned.estimated_candidates,
                                explain.warnings
                            )
                        }),
                    ));
                }
                Some(Err(error)) => {
                    if build_explain {
                        alternatives.push(GraphRowPlanAlternative {
                            chosen: false,
                            kind: "RejectedNodeAnchor".to_string(),
                            detail: format!(
                                "segment={segment_index}; alias={}; reason={error}",
                                node.alias
                            ),
                            decision: None,
                            cost: None,
                        });
                    }
                }
                None => {
                    if build_explain {
                        alternatives.push(GraphRowPlanAlternative {
                            chosen: false,
                            kind: "RejectedNodeAnchor".to_string(),
                            detail: format!(
                                "segment={segment_index}; alias={}; reason=no cached node anchor plan",
                                node.alias
                            ),
                            decision: None,
                            cost: None,
                        });
                    }
                }
            }
        }

        for &edge_index in &segment.edge_indices {
            let edge = &runtime.edges[edge_index];
            let Some(edge_source_cost) = edge_source_costs[edge_index].clone() else {
                if build_explain {
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
                }
                continue;
            };
            let anchor_cost = edge_source_cost.cost.clone();
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
                edge_source_cost_memo,
                known_node_ids,
                target_selectivities,
                format!("edge:{}:{edge_index}", edge.explain_name()),
                build_explain,
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
                build_explain.then(|| {
                    format!(
                        "segment={segment_index}; edge={}; {}; warnings={:?}",
                        edge.explain_name(),
                        edge_source_cost.detail.unwrap_or_default(),
                        edge_source_cost.warnings
                    )
                }),
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
            if build_explain {
                alternatives.push(GraphRowPlanAlternative {
                    chosen: true,
                    kind: "DeterministicFallback".to_string(),
                    detail: format!("segment={segment_index}; no legal early node or edge anchor; execution keeps query-order required edges and will enforce normal full-scan/cap rules"),
                    decision: Some("decision=chosen_deterministic_fallback".to_string()),
                    cost: None,
                });
            }
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
            if build_explain {
                alternatives.push(GraphRowPlanAlternative {
                    chosen,
                    kind,
                    detail: detail.unwrap_or_default(),
                    decision: Some(decision),
                    cost: Some(cost),
                });
            }
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
        edge_source_cost_memo: &mut GraphRowEdgeSourceCostMemo,
        known_node_ids: &[Option<Vec<u64>>],
        target_selectivities: &[Option<(u64, u64)>],
        canonical_key: String,
        build_explain: bool,
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
                        if let Some(edge_source_cost) = edge_source_costs[edge_index].as_ref() {
                            let cost = &edge_source_cost.cost;
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
                                tie_kind_rank: 2,
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
                let (source_index, target_index, direction, target_alias) = if from_bound {
                    (
                        from_index,
                        to_index,
                        edge.direction,
                        edge.to_alias.as_str(),
                    )
                } else {
                    (
                        to_index,
                        from_index,
                        reverse_graph_row_direction(edge.direction),
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
                    tie_kind_rank: 0,
                    edge_index,
                    source_choice: GraphRowEdgeCandidateSourceChoice::EndpointAdjacency,
                    fanout: Some(fanout),
                });
                let bound_edge_source_cost = self.graph_row_bound_edge_source_plan_cost(
                    query,
                    edge,
                    edge_index,
                    from_bound,
                    to_bound,
                    known_node_ids[from_index].as_deref().unwrap_or(&[]),
                    known_node_ids[to_index].as_deref().unwrap_or(&[]),
                    build_explain,
                    edge_source_cost_memo,
                )?;
                if let Some(edge_source_cost) = bound_edge_source_cost
                    .as_ref()
                    .or(edge_source_costs[edge_index].as_ref())
                {
                    let edge_source_cost = &edge_source_cost.cost;
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
                        tie_kind_rank: 1,
                        edge_index,
                        source_choice: GraphRowEdgeCandidateSourceChoice::EdgeCandidateSource,
                        fanout: None,
                    });
                }
            }

            // One comparison rule for every pair (see GraphRowPlanCost::cmp):
            // estimates stay decisive when completeness is mixed — an
            // incomplete choice still carries a usable fallback expansion
            // estimate (or the unknown-work sentinel), and one unflushed edge
            // write downgrades every adjacency choice to incomplete, so the
            // old query-order fallback was the common case on warm databases.
            choices.sort_by(|left, right| {
                left.bound_rank
                    .cmp(&right.bound_rank)
                    .then_with(|| left.estimated_expansion.cmp(&right.estimated_expansion))
                    .then_with(|| (!left.complete).cmp(&!right.complete))
                    .then_with(|| left.next_frontier.cmp(&right.next_frontier))
                    .then_with(|| left.confidence_rank.cmp(&right.confidence_rank))
                    .then_with(|| left.hub_risk_rank.cmp(&right.hub_risk_rank))
                    .then_with(|| left.coverage_rank.cmp(&right.coverage_rank))
                    .then_with(|| left.source_rank.cmp(&right.source_rank))
                    .then_with(|| left.edge_index.cmp(&right.edge_index))
                    .then_with(|| left.tie_kind_rank.cmp(&right.tie_kind_rank))
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
            // Keep the accumulated traversal work and penalize each unbound
            // edge instead of resetting to the anchor cost: a reset made
            // incomplete fallback plans look spuriously anchor-cheap against
            // fully-estimated plans.
            for &edge_index in segment_edge_indices {
                if !visited_edges[edge_index] {
                    total_work = total_work.saturating_add(GRAPH_ROW_FANOUT_UNKNOWN_WORK);
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
            NormalizedEdgeFilter::IdRange { .. }
            | NormalizedEdgeFilter::CreatedAtRange { .. } => None,
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
        if self.active_memtable_only_exact_estimates() {
            let count = memtable_secondary_eq_edge_count_for_filter(
                &self.memtable,
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        let value_hashes = equality_probe_value_hashes(value);
        let mut count = value_hashes
            .iter()
            .map(|value_hash| {
                self.memtable
                    .secondary_eq_edge_hash_count_at(index_id, *value_hash, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            for value_hash in &value_hashes {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .secondary_eq_edge_hash_count_at(index_id, *value_hash, self.snapshot_seq)
                        as u64,
                );
            }
        }

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
        if self.active_memtable_only_exact_estimates() {
            let count = self.memtable.secondary_eq_edge_count_at(
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        let mut count = self
            .memtable
            .secondary_eq_edge_hash_count_at(index_id, value_hash, self.snapshot_seq)
            as u64;
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                epoch
                    .memtable
                    .secondary_eq_edge_hash_count_at(index_id, value_hash, self.snapshot_seq)
                    as u64,
            );
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
        plans.sort_by_cached_key(EdgePhysicalPlan::plan_cost);
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
            if selected
                .first()
                .is_some_and(EdgePhysicalPlan::contains_compound_source)
            {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
                continue;
            }
            // A compound source that is not the cheapest input would re-scan
            // its tuple sidecar alongside the cheaper driver — the repeated
            // dual lookup the spec prohibits. Its predicates are re-checked
            // by the verifier instead.
            if plan.contains_compound_source() {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
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
            // IN-expansion members are all equality-index probes (eager).
            let union_cap = cap_context.union_total_cap(true, query.page.limit, union_estimate);
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
    #[allow(clippy::too_many_arguments)]
    /// Edge twin of `plan_root_filter`: a single constraint-bearing leaf at
    /// the filter root also consults compound declarations, so anchors like
    /// `from_ids` plus one predicate can drive a compound prefix scan.
    #[allow(clippy::too_many_arguments)]
    fn plan_root_edge_filter(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        filter: &NormalizedEdgeFilter,
        availability: EdgeMetadataSidecarAvailability,
        allow_compound: bool,
        budget: &mut BooleanPlanningBudget,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<EdgeBooleanPlanResult, EngineError> {
        let planned = self.plan_edge_filter_subtree(
            query,
            cap_context,
            filter,
            availability,
            allow_compound,
            budget,
            warnings,
            followups,
        )?;
        if !allow_compound
            || !edge_filter_is_compound_constraining_leaf(filter)
            || matches!(
                planned.classification,
                EdgeBooleanPlanClassification::AlwaysFalse
            )
        {
            return Ok(planned);
        }
        let Some(compound) = self.best_edge_compound_candidate(query, filter, cap_context, warnings)
        else {
            return Ok(planned);
        };
        let mut plans = vec![EdgePhysicalPlan::source(compound.source)];
        let mut has_verify_only = planned.has_verify_only;
        match planned.classification {
            EdgeBooleanPlanClassification::Bounded {
                plan, complete, ..
            } if complete => plans.push(plan),
            EdgeBooleanPlanClassification::Bounded { .. } => has_verify_only = true,
            EdgeBooleanPlanClassification::VerifyOnly => {}
            EdgeBooleanPlanClassification::AlwaysFalse => unreachable!("handled above"),
        }
        let (selected, _skipped_to_verifier) =
            self.select_bounded_edge_and_plans(query, cap_context, plans, warnings);
        // A skipped plan here is redundant coverage of the same single leaf,
        // not a lost predicate, so it does not mark the filter verify-only.
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
                structural_key: filter.structural_key(),
                complete: true,
                plan,
            },
            has_verify_only,
        })
    }

    #[allow(clippy::too_many_arguments)] // Edge filter planning needs shared query context plus mutable planning outputs.
    fn plan_edge_filter_subtree(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        filter: &NormalizedEdgeFilter,
        availability: EdgeMetadataSidecarAvailability,
        allow_compound: bool,
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
            NormalizedEdgeFilter::IdRange { .. }
            | NormalizedEdgeFilter::CreatedAtRange { .. } => Ok(EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
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
                if allow_compound {
                    if let Some(compound) =
                        self.best_edge_compound_candidate(query, filter, cap_context, warnings)
                    {
                        plans.push(EdgePhysicalPlan::source(compound.source));
                    }
                }
                for child in children {
                    let planned = self.plan_edge_filter_subtree(
                        query,
                        cap_context,
                        child,
                        availability,
                        allow_compound,
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
                let mut members_eager = true;
                for child in children {
                    let planned = self.plan_edge_filter_subtree(
                        query,
                        cap_context,
                        child,
                        availability,
                        allow_compound,
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
                            members_eager &= plan.members_are_eager_index_sources();
                            estimated_total = estimated_total.saturating_add(count);
                            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
                            let union_cap = cap_context.union_total_cap(
                                members_eager,
                                query.page.limit,
                                union_estimate,
                            );
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
        if self.active_memtable_only_exact_estimates() {
            let count = memtable_secondary_eq_count_for_filter(
                &self.memtable,
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        let value_hashes = equality_probe_value_hashes(value);
        let mut count = value_hashes
            .iter()
            .map(|value_hash| {
                self.memtable
                    .secondary_eq_node_hash_count_at(index_id, *value_hash, self.snapshot_seq)
                    as u64
            })
            .sum::<u64>();
        for epoch in &self.immutable_epochs {
            for value_hash in &value_hashes {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .secondary_eq_node_hash_count_at(index_id, *value_hash, self.snapshot_seq)
                        as u64,
                );
            }
        }

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
        if self.active_memtable_only_exact_estimates() {
            let count = self.memtable.secondary_eq_node_count_at(
                index_id,
                key,
                value,
                self.snapshot_seq,
            ) as u64;
            return Ok((Some(PlannerEstimate::exact_cheap(count)), None));
        }
        let mut count = self
            .memtable
            .secondary_eq_node_hash_count_at(index_id, value_hash, self.snapshot_seq)
            as u64;
        for epoch in &self.immutable_epochs {
            count = count.saturating_add(
                epoch
                    .memtable
                    .secondary_eq_node_hash_count_at(index_id, value_hash, self.snapshot_seq)
                    as u64,
            );
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
        plans.sort_by_cached_key(NodePhysicalPlan::plan_cost);
    }

    #[cfg(test)]
    fn query_cap_context(&self, query: &NormalizedNodeQuery) -> Result<QueryCapContext, EngineError> {
        let sources = self.legal_universe_sources(query)?;
        Ok(Self::node_query_cap_context_from_legal_sources(&sources))
    }

    fn legal_universe_sources(
        &self,
        query: &NormalizedNodeQuery,
    ) -> Result<Vec<NodeLegalUniverseSource>, EngineError> {
        let mut sources = Vec::new();
        if !query.ids.is_empty() {
            // Normalized query IDs are already sorted/deduped; share one copy
            // across every downstream source/plan instead of recloning.
            sources.push(NodeLegalUniverseSource::ExplicitIds(Arc::new(
                query.ids.clone(),
            )));
        }
        if !query.keys.is_empty() {
            sources.push(NodeLegalUniverseSource::KeyLookup(query.keys.len()));
        }
        if let ResolvedNodeLabelFilter::LabelSet {
            mode, label_ids, ..
        } = query.label_filter
        {
            let membership_estimate = self.node_label_filter_estimate(&label_ids, mode)?;
            match (mode, label_ids.as_slice()) {
                (_, [single_label_id]) => {
                    sources.push(NodeLegalUniverseSource::Label {
                        label_id: *single_label_id,
                        estimate: membership_estimate.estimate,
                    });
                }
                (LabelMatchMode::Any, _) => {
                    sources.push(NodeLegalUniverseSource::LabelAny {
                        label_ids,
                        estimate: membership_estimate.estimate,
                    });
                }
                (LabelMatchMode::All, _) => {
                    if let Some(driver_label_id) = membership_estimate.driver_label_id {
                        sources.push(NodeLegalUniverseSource::Label {
                            label_id: driver_label_id,
                            estimate: membership_estimate.estimate,
                        });
                    }
                }
            }
        }
        if query.allow_full_scan {
            sources.push(NodeLegalUniverseSource::FullScan {
                estimate: self.full_scan_estimate(),
            });
        }
        Ok(sources)
    }

    fn legal_universe_plans(
        &self,
        query: &NormalizedNodeQuery,
        filter_driver: bool,
    ) -> Result<Vec<NodePhysicalPlan>, EngineError> {
        Ok(Self::node_legal_universe_plans_from_sources(
            &self.legal_universe_sources(query)?,
            filter_driver,
        ))
    }

    fn node_legal_universe_plans_from_sources(
        sources: &[NodeLegalUniverseSource],
        filter_driver: bool,
    ) -> Vec<NodePhysicalPlan> {
        sources
            .iter()
            .map(|source| source.plan(filter_driver))
            .collect()
    }

    fn cheapest_node_legal_universe_source(
        sources: &[NodeLegalUniverseSource],
    ) -> Option<PlannedNodeCandidateSource> {
        sources
            .iter()
            .map(|source| source.source(true))
            .min_by_key(PlannedNodeCandidateSource::plan_cost)
    }

    #[cfg(test)]
    fn node_query_cap_context_from_legal_sources(
        sources: &[NodeLegalUniverseSource],
    ) -> QueryCapContext {
        QueryCapContext {
            cheapest_legal_universe: Self::cheapest_node_legal_universe_source(sources)
                .map(|source| source.estimate),
        }
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
            if selected
                .first()
                .is_some_and(NodePhysicalPlan::contains_compound_source)
            {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
                continue;
            }
            // A compound source that is not the cheapest input would re-scan
            // its tuple sidecar alongside the cheaper driver — the repeated
            // dual lookup the spec prohibits. Its predicates are re-checked
            // by the verifier instead.
            if plan.contains_compound_source() {
                skipped_to_verifier = true;
                if plan.estimate().known_upper_bound().is_some() && plan.broad_skip_warnable() {
                    add_plan_warning(warnings, QueryPlanWarning::IndexSkippedAsBroad);
                }
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
                let cost = source.plan_cost();
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
            // IN-expansion members are all equality-index probes (eager).
            let union_cap = cap_context.union_total_cap(true, query.page.limit, union_estimate);
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

    /// Plans the root filter. Single constraint-bearing leaves additionally
    /// consult compound declarations (CP37.5 review S2 / planner review P1,
    /// user-ratified): a single-predicate query like `tenant = 'acme'` may
    /// drive a `(tenant, score)` prefix scan. Applied at the root only —
    /// `And` arms already extract over the whole conjunction, and re-probing
    /// per nested child would only duplicate that work.
    fn plan_root_filter(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        filter: &NormalizedNodeFilter,
        budget: &mut BooleanPlanningBudget,
        warnings: &mut Vec<QueryPlanWarning>,
        followups: &mut Vec<SecondaryIndexReadFollowup>,
    ) -> Result<BooleanPlanResult, EngineError> {
        let planned =
            self.plan_filter_subtree(query, cap_context, filter, budget, warnings, followups)?;
        if !node_filter_is_compound_constraining_leaf(filter)
            || matches!(
                planned.classification,
                BooleanPlanClassification::AlwaysFalse
            )
        {
            return Ok(planned);
        }
        let Some(compound) =
            self.best_node_compound_candidate(query, filter, cap_context, warnings)?
        else {
            return Ok(planned);
        };
        let mut plans = vec![compound.into_plan()];
        let mut has_verify_only = planned.has_verify_only;
        match planned.classification {
            BooleanPlanClassification::Bounded {
                plan, complete, ..
            } if complete => plans.push(plan),
            BooleanPlanClassification::Bounded { .. } => has_verify_only = true,
            BooleanPlanClassification::VerifyOnly => {}
            BooleanPlanClassification::AlwaysFalse => unreachable!("handled above"),
        }
        let (selected, _skipped_to_verifier) =
            self.select_bounded_and_plans(query, cap_context, plans, warnings);
        // A skipped plan here is redundant coverage of the same single leaf,
        // not a lost predicate, so it does not mark the filter verify-only.
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
                structural_key: filter.structural_key(),
                complete: true,
                plan,
            },
            has_verify_only,
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
            NormalizedNodeFilter::IdRange { .. }
            | NormalizedNodeFilter::KeyEquals(_)
            | NormalizedNodeFilter::KeyIn { .. }
            | NormalizedNodeFilter::WeightRange { .. }
            | NormalizedNodeFilter::CreatedAtRange { .. } => Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }),
            NormalizedNodeFilter::PropertyExists { .. }
            | NormalizedNodeFilter::PropertyMissing { .. }
            | NormalizedNodeFilter::Not(_) => Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            }),
            NormalizedNodeFilter::And(children) => {
                let mut plans = Vec::new();
                let mut has_verify_only = false;
                if let Some(compound) =
                    self.best_node_compound_candidate(query, filter, cap_context, warnings)?
                {
                    plans.push(compound.into_plan());
                }
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
                let mut members_eager = true;
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
                            members_eager &= plan.members_are_eager_index_sources();
                            estimated_total = estimated_total.saturating_add(count);
                            let union_estimate = PlannerEstimate::upper_bound(estimated_total);
                            let union_cap = cap_context.union_total_cap(
                                members_eager,
                                query.page.limit,
                                union_estimate,
                            );
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
                legal_universe_fallback: None,
                warnings,
                followups: Vec::new(),
            });
        }

        let legal_universe_sources = self.legal_universe_sources(query)?;
        let legal_universe_fallback =
            Self::cheapest_node_legal_universe_source(&legal_universe_sources);
        let cap_context = QueryCapContext {
            cheapest_legal_universe: legal_universe_fallback
                .as_ref()
                .map(|source| source.estimate),
        };
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
            self.plan_root_filter(
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
                legal_universe_fallback,
                warnings,
                followups: filter_followups,
            });
        }

        let mut driver_candidates =
            Self::node_legal_universe_plans_from_sources(&legal_universe_sources, has_filter);
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
            legal_universe_fallback,
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
            sources.push(PlannedEdgeCandidateSource::with_normalized_ids(
                EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
                "edge_ids".to_string(),
                Arc::new(query.ids.clone()),
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
                Arc::new(query.from_ids.clone()),
                label_filter_ids.clone(),
                estimate,
            ));
        }
        if !query.to_ids.is_empty() {
            let estimate =
                self.edge_endpoint_estimate(&query.to_ids, Direction::Incoming, label_filter_ids.as_deref());
            sources.push(PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::ToEndpointAdjacency,
                Arc::new(query.to_ids.clone()),
                label_filter_ids.clone(),
                estimate,
            ));
        }
        if !query.endpoint_ids.is_empty() {
            let estimate =
                self.edge_endpoint_estimate(&query.endpoint_ids, Direction::Both, label_filter_ids.as_deref());
            sources.push(PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::AnyEndpointAdjacency,
                Arc::new(query.endpoint_ids.clone()),
                label_filter_ids,
                estimate,
            ));
        }
        if query.allow_full_scan {
            sources.push(PlannedEdgeCandidateSource::fallback_full_scan(
                self.edge_full_scan_estimate(),
            ));
        } else if query.filter.has_metadata_anchor() {
            // A metadata-anchored filter authorizes a complete scan-and-verify
            // metadata source, so fallback from a failed/too-broad candidate
            // source never leaves a filter-only query without a legal universe.
            sources.push(PlannedEdgeCandidateSource::metadata_filter_scan(
                self.edge_full_scan_estimate(),
            ));
        }
        sources
    }

    fn edge_query_cap_context_from_legal_sources(
        sources: &[PlannedEdgeCandidateSource],
    ) -> EdgeQueryCapContext {
        let cheapest_legal_universe = sources
            .iter()
            .map(|source| source.estimate)
            .filter_map(|estimate| estimate.known_upper_bound().map(|count| (count, estimate)))
            .min_by_key(|(count, _)| *count)
            .map(|(_, estimate)| estimate);
        EdgeQueryCapContext {
            cheapest_legal_universe,
        }
    }

    fn cheapest_edge_legal_universe_source(
        sources: &[PlannedEdgeCandidateSource],
    ) -> Option<PlannedEdgeCandidateSource> {
        sources
            .iter()
            .min_by_key(|source| source.plan_cost())
            .cloned()
    }

    fn edge_legal_source_by_kind(
        sources: &[PlannedEdgeCandidateSource],
        kind: EdgeQueryCandidateSourceKind,
    ) -> Option<PlannedEdgeCandidateSource> {
        sources.iter().find(|source| source.kind == kind).cloned()
    }

    fn plan_normalized_edge_query(
        &self,
        query: &NormalizedEdgeQuery,
    ) -> Result<PlannedEdgeQuery, EngineError> {
        self.plan_normalized_edge_query_with_compound(query, true)
    }

    /// Replan entry point for execution-time fallback after a selected
    /// compound source failed on a missing/corrupt sidecar: compound
    /// candidates are excluded so the plan uses only non-compound legal
    /// sources.
    fn plan_normalized_edge_query_excluding_compound(
        &self,
        query: &NormalizedEdgeQuery,
    ) -> Result<PlannedEdgeQuery, EngineError> {
        self.plan_normalized_edge_query_with_compound(query, false)
    }

    fn plan_normalized_edge_query_with_compound(
        &self,
        query: &NormalizedEdgeQuery,
        allow_compound: bool,
    ) -> Result<PlannedEdgeQuery, EngineError> {
        let mut warnings = query.warnings.clone();
        let legal_universe_sources = self.edge_legal_universe_sources(query);
        let cap_context = Self::edge_query_cap_context_from_legal_sources(&legal_universe_sources);
        let legal_universe_fallback =
            Self::cheapest_edge_legal_universe_source(&legal_universe_sources);
        if query.filter.is_always_false() {
            return Ok(PlannedEdgeQuery {
                driver: EdgePhysicalPlan::Empty,
                cap_context,
                legal_universe_fallback: None,
                warnings,
                followups: Vec::new(),
            });
        }

        let mut inputs = Vec::new();
        if !query.ids.is_empty() {
            let source = Self::edge_legal_source_by_kind(
                &legal_universe_sources,
                EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
            )
            .unwrap_or_else(|| {
                PlannedEdgeCandidateSource::with_normalized_ids(
                    EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
                    "edge_ids".to_string(),
                    Arc::new(query.ids.clone()),
                )
            });
            inputs.push(EdgePhysicalPlan::source(source));
        }

        let label_filter_ids = query.label_id.map(|label_id| vec![label_id]);
        let triple_source_used = if let (Some(label_id), [from], [to]) =
            (query.label_id, query.from_ids.as_slice(), query.to_ids.as_slice())
        {
            // The (from, to, label) triple result is a subset of both endpoint
            // adjacency lists, so the cheaper side is a valid upper bound; an
            // unknown estimate would rank the cheapest edge source dead last.
            let from_estimate = Self::edge_legal_source_by_kind(
                &legal_universe_sources,
                EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
            )
            .map(|source| source.estimate)
            .unwrap_or_else(|| {
                self.edge_endpoint_estimate(
                    std::slice::from_ref(from),
                    Direction::Outgoing,
                    label_filter_ids.as_deref(),
                )
            });
            let to_estimate = Self::edge_legal_source_by_kind(
                &legal_universe_sources,
                EdgeQueryCandidateSourceKind::ToEndpointAdjacency,
            )
            .map(|source| source.estimate)
            .unwrap_or_else(|| {
                self.edge_endpoint_estimate(
                    std::slice::from_ref(to),
                    Direction::Incoming,
                    label_filter_ids.as_deref(),
                )
            });
            let estimate = min_known_planner_estimate(from_estimate, to_estimate);
            inputs.push(EdgePhysicalPlan::source(
                PlannedEdgeCandidateSource::edge_triple_index(*from, *to, label_id, estimate),
            ));
            true
        } else {
            false
        };
        if !triple_source_used {
            if query.label_id.is_some() {
                let source = Self::edge_legal_source_by_kind(
                    &legal_universe_sources,
                    EdgeQueryCandidateSourceKind::EdgeLabelIndex,
                )
                .expect("edge label legal source must exist when label_id is present");
                inputs.push(EdgePhysicalPlan::source(source));
            }
            if !query.from_ids.is_empty() {
                let source = Self::edge_legal_source_by_kind(
                    &legal_universe_sources,
                    EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
                )
                .expect("from-endpoint legal source must exist when from_ids is non-empty");
                inputs.push(EdgePhysicalPlan::source(source));
            }
            if !query.to_ids.is_empty() {
                let source = Self::edge_legal_source_by_kind(
                    &legal_universe_sources,
                    EdgeQueryCandidateSourceKind::ToEndpointAdjacency,
                )
                .expect("to-endpoint legal source must exist when to_ids is non-empty");
                inputs.push(EdgePhysicalPlan::source(source));
            }
        }
        if !query.endpoint_ids.is_empty() {
            let source = Self::edge_legal_source_by_kind(
                &legal_universe_sources,
                EdgeQueryCandidateSourceKind::AnyEndpointAdjacency,
            )
            .expect("any-endpoint legal source must exist when endpoint_ids is non-empty");
            inputs.push(EdgePhysicalPlan::source(source));
        }

        let has_filter = !query.filter.is_always_true();
        let mut budget = BooleanPlanningBudget::new();
        let mut filter_followups = Vec::new();
        let filter_plan = if !has_filter {
            EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: false,
            }
        } else if should_skip_filter_planning_for_explicit_edge_anchor(query) {
            EdgeBooleanPlanResult {
                classification: EdgeBooleanPlanClassification::VerifyOnly,
                has_verify_only: edge_filter_has_intrinsic_verify_only(&query.filter),
            }
        } else {
            self.plan_root_edge_filter(
                query,
                cap_context,
                &query.filter,
                self.edge_metadata_sidecar_availability(),
                allow_compound,
                &mut budget,
                &mut warnings,
                &mut filter_followups,
            )?
        };
        if matches!(
            &filter_plan.classification,
            EdgeBooleanPlanClassification::AlwaysFalse
        ) {
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedEdgeQuery {
                driver: EdgePhysicalPlan::Empty,
                cap_context,
                legal_universe_fallback,
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
                let source = Self::edge_legal_source_by_kind(
                    &legal_universe_sources,
                    EdgeQueryCandidateSourceKind::FallbackFullEdgeScan,
                )
                .expect("full-scan legal source must exist when allow_full_scan is set");
                inputs.push(EdgePhysicalPlan::source(source));
            } else if query.filter.has_metadata_anchor() {
                // Metadata-anchored filters (including `IdRange` and
                // `CreatedAtRange`, which have no bounded candidate plan) are
                // legal anchors per normalization; serve them through a
                // scan-and-verify metadata source instead of failing.
                let source = Self::edge_legal_source_by_kind(
                    &legal_universe_sources,
                    EdgeQueryCandidateSourceKind::EdgeMetadataScan,
                )
                .expect("metadata legal source must exist for metadata-anchored filters");
                inputs.push(EdgePhysicalPlan::source(source));
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

        inputs.sort_by_cached_key(EdgePhysicalPlan::plan_cost);
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
            legal_universe_fallback,
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
            materialization: EdgeCandidateMaterialization::Precomputed(Arc::new(Vec::new())),
        })
    }

    fn node_source_plan(kind: NodeQueryCandidateSourceKind, count: u64) -> NodePhysicalPlan {
        NodePhysicalPlan::source(PlannedNodeCandidateSource {
            kind,
            canonical_key: format!("test-node-source:{kind:?}:{count}"),
            estimate: PlannerEstimate::stats_exact(count),
            materialization: NodeCandidateMaterialization::Precomputed(Arc::new(Vec::new())),
        })
    }

    #[test]
    fn source_plan_cost_matches_source_plan_plan_cost() {
        // Large-anchor cleanup pin: cheapest-legal-universe and multi-label
        // probe selection now cost sources by reference; the source-level
        // plan_cost must stay identical to wrapping the source in a physical
        // plan or plan choice silently diverges between the two paths.
        let node_sources = [
            PlannedNodeCandidateSource::with_ids(
                NodeQueryCandidateSourceKind::ExplicitIds,
                "ids".to_string(),
                vec![9, 3, 3, 7],
            ),
            PlannedNodeCandidateSource::node_label_index(4, PlannerEstimate::upper_bound(123)),
            PlannedNodeCandidateSource::fallback_full_scan(PlannerEstimate::unknown()),
        ];
        for source in node_sources {
            assert_eq!(
                source.plan_cost(),
                NodePhysicalPlan::source(source.clone()).plan_cost(),
                "node source {:?} cost diverged",
                source.kind
            );
        }
        let edge_sources = [
            PlannedEdgeCandidateSource::with_ids(
                EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
                "edge_ids".to_string(),
                vec![5, 1, 5],
            ),
            PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
                Arc::new(vec![1, 2, 3]),
                Some(vec![7]),
                PlannerEstimate::upper_bound(3),
            ),
            PlannedEdgeCandidateSource::fallback_full_scan(PlannerEstimate::unknown()),
        ];
        for source in edge_sources {
            assert_eq!(
                source.plan_cost(),
                EdgePhysicalPlan::source(source.clone()).plan_cost(),
                "edge source {:?} cost diverged",
                source.kind
            );
        }
    }

    #[test]
    fn endpoint_adjacency_canonical_key_is_fixed_size_and_content_keyed() {
        let adjacency = |ids: Vec<u64>| {
            PlannedEdgeCandidateSource::endpoint_adjacency(
                EdgeQueryCandidateSourceKind::FromEndpointAdjacency,
                Arc::new(ids),
                None,
                PlannerEstimate::upper_bound(3),
            )
        };
        // Deterministic and content-keyed: same list → same key, any
        // different list → different key.
        assert_eq!(
            adjacency(vec![1, 2, 3]).canonical_key,
            adjacency(vec![1, 2, 3]).canonical_key
        );
        assert_ne!(
            adjacency(vec![1, 2, 3]).canonical_key,
            adjacency(vec![1, 2, 4]).canonical_key
        );
        // Fixed-size: the key must not scale with the endpoint list (it is
        // cloned into every PlanCost and compared in sort tie-breaks).
        let large = adjacency((0..100_000).collect());
        assert!(
            large.canonical_key.len() < 128,
            "endpoint adjacency canonical key grew with the ID list: {} bytes",
            large.canonical_key.len()
        );
    }

    #[test]
    fn adaptive_cap_uncaps_eager_kinds_on_medium_confidence_upper_bounds() {
        // Planner review P3: any unflushed write downgrades stats confidence
        // to Medium database-wide; eager index kinds must still uncap to
        // their trusted posting upper bound instead of falling back to the
        // default cap (and from there to a full label scan).
        let estimate = PlannerEstimate::upper_bound(10_000);
        assert!(matches!(estimate.confidence, EstimateConfidence::Medium));
        let legal = Some(PlannerEstimate::upper_bound(1_000_000));
        for kind in [
            NodeQueryCandidateSourceKind::PropertyEqualityIndex,
            NodeQueryCandidateSourceKind::PropertyRangeIndex,
            NodeQueryCandidateSourceKind::TimestampIndex,
            NodeQueryCandidateSourceKind::CompoundEqualityIndex,
            NodeQueryCandidateSourceKind::CompoundRangeIndex,
        ] {
            assert_eq!(
                adaptive_candidate_cap(kind, None, legal, estimate),
                10_000,
                "node kind {kind:?} should uncap to its upper bound"
            );
        }
        for kind in [
            EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
            EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
            EdgeQueryCandidateSourceKind::EdgeWeightIndex,
            EdgeQueryCandidateSourceKind::CompoundEqualityIndex,
            EdgeQueryCandidateSourceKind::CompoundRangeIndex,
        ] {
            assert_eq!(
                adaptive_edge_candidate_cap(kind, None, legal, estimate),
                10_000,
                "edge kind {kind:?} should uncap to its upper bound"
            );
        }
        // Scan-backed kinds keep the default cap.
        assert_eq!(
            adaptive_candidate_cap(
                NodeQueryCandidateSourceKind::NodeLabelIndex,
                None,
                legal,
                estimate
            ),
            crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP,
        );
        assert_eq!(
            adaptive_edge_candidate_cap(
                EdgeQueryCandidateSourceKind::EdgeMetadataScan,
                None,
                legal,
                estimate
            ),
            crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP,
        );
        // High stale-posting risk still keeps the default cap.
        let stale = PlannerEstimate::upper_bound_with_quality(
            10_000,
            EstimateConfidence::Medium,
            StalePostingRisk::High,
        );
        assert_eq!(
            adaptive_candidate_cap(
                NodeQueryCandidateSourceKind::PropertyEqualityIndex,
                None,
                legal,
                stale
            ),
            crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP,
        );
    }

    #[test]
    fn compound_skips_as_broad_only_above_hard_candidate_cap() {
        // Planner review P2: the planner must not keep a compound candidate
        // that execution is guaranteed to TooBroad at the hard cap.
        let hard_cap = crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP as u64;
        let legal = Some(1_000_000u64);
        assert!(!ReadView::compound_estimate_skips_as_broad(
            PlannerEstimate::upper_bound(hard_cap),
            legal,
        ));
        assert!(ReadView::compound_estimate_skips_as_broad(
            PlannerEstimate::upper_bound(hard_cap + 1),
            legal,
        ));
        // Without a legal fallback the candidate is kept regardless.
        assert!(!ReadView::compound_estimate_skips_as_broad(
            PlannerEstimate::upper_bound(hard_cap + 1),
            None,
        ));
        assert!(ReadView::compound_estimate_skips_as_broad(
            PlannerEstimate::unknown(),
            legal,
        ));
    }

    #[test]
    fn compound_any_union_estimate_is_clamped_to_execution_cap() {
        // Multi-label Any compound unions are materialized as one eager union,
        // so plan-time admission must use the same hard cap as execution.
        let hard_cap = crate::planner_stats::PLANNER_STATS_HARD_CANDIDATE_CAP as u64;
        let cap_context = QueryCapContext {
            cheapest_legal_universe: Some(PlannerEstimate::upper_bound(1_000_000)),
        };

        assert!(!compound_union_estimate_exceeds_materialization_cap(
            cap_context,
            None,
            hard_cap,
        ));
        assert!(compound_union_estimate_exceeds_materialization_cap(
            cap_context,
            None,
            hard_cap + 1,
        ));
    }

    #[test]
    fn compound_source_detection_recurses_into_unions() {
        let compound_node_union = NodePhysicalPlan::union(vec![
            node_source_plan(NodeQueryCandidateSourceKind::CompoundEqualityIndex, 10),
            node_source_plan(NodeQueryCandidateSourceKind::CompoundRangeIndex, 12),
        ]);
        assert!(compound_node_union.contains_compound_source());

        let plain_node_union = NodePhysicalPlan::union(vec![
            node_source_plan(NodeQueryCandidateSourceKind::PropertyEqualityIndex, 10),
            node_source_plan(NodeQueryCandidateSourceKind::TimestampIndex, 12),
        ]);
        assert!(!plain_node_union.contains_compound_source());

        let compound_edge_union = EdgePhysicalPlan::union(vec![
            edge_source_plan(EdgeQueryCandidateSourceKind::CompoundEqualityIndex, 10),
            edge_source_plan(EdgeQueryCandidateSourceKind::CompoundRangeIndex, 12),
        ]);
        assert!(compound_edge_union.contains_compound_source());

        let plain_edge_union = EdgePhysicalPlan::union(vec![
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex, 10),
            edge_source_plan(EdgeQueryCandidateSourceKind::EdgeWeightIndex, 12),
        ]);
        assert!(!plain_edge_union.contains_compound_source());
    }

    #[test]
    fn compound_count_remaining_cap_tracks_global_and_local_totals() {
        assert_eq!(remaining_compound_count_cap(100, 25, 30), Some(45));
        assert_eq!(remaining_compound_count_cap(100, 25, 75), None);
        assert_eq!(remaining_compound_count_cap(100, 100, 0), None);
        assert_eq!(
            remaining_compound_count_cap(100, u64::MAX - 5, 10),
            None
        );
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

    fn node_field_entry(
        fields: &[SecondaryIndexField],
        kind: SecondaryIndexKind,
        state: SecondaryIndexState,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: fields
                    .iter()
                    .map(|field| {
                        crate::types::SecondaryIndexFieldManifest::from_public(field).unwrap()
                    })
                    .collect(),
            },
            kind,
            state,
            last_error: None,
        }
    }

    #[test]
    fn compound_bounds_one_field_metadata_equality_is_eligible() {
        let entry = node_field_entry(
            &[SecondaryIndexField::node_meta(NodeMetadataIndexField::Key)],
            SecondaryIndexKind::Equality,
            SecondaryIndexState::Ready,
        );
        let mut constraints = CompoundFieldConstraints::default();
        add_compound_equality_values(
            &mut constraints,
            node_key_field(),
            vec![CompoundOwnedValue::String("alpha".to_string())],
        );
        let outcome = encode_compound_bounds_for_entry(&entry, &constraints);
        match outcome {
            CompoundBoundsOutcome::Bounds(CompoundEncodedBounds::Prefix {
                bounds,
                matched_prefix_len,
            }) => {
                assert_eq!(matched_prefix_len, 1);
                assert_eq!(bounds.len(), 1);
            }
            _ => panic!("expected one-field metadata equality to produce prefix bounds"),
        }
    }

    #[test]
    fn compound_bounds_bare_range_on_one_field_range_declaration_is_prefix_unsatisfied() {
        // Range scans need a non-empty equality prefix; a one-field range
        // declaration with only a range predicate remains ineligible.
        let entry = node_field_entry(
            &[SecondaryIndexField::node_meta(
                NodeMetadataIndexField::CreatedAt,
            )],
            SecondaryIndexKind::Range,
            SecondaryIndexState::Ready,
        );
        let mut constraints = CompoundFieldConstraints::default();
        add_compound_range(
            &mut constraints,
            node_created_at_field(),
            CompoundRangeConstraint {
                lower: Some((CompoundOwnedValue::I64(5), true)),
                upper: None,
            },
        );
        assert!(matches!(
            encode_compound_bounds_for_entry(&entry, &constraints),
            CompoundBoundsOutcome::PrefixNotSatisfied
        ));
    }

    #[test]
    fn compound_bounds_in_expansion_cap_exceeded_is_distinct_outcome() {
        let entry = node_field_entry(
            &[
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ],
            SecondaryIndexKind::Equality,
            SecondaryIndexState::Ready,
        );
        let mut constraints = CompoundFieldConstraints::default();
        add_compound_equality_values(
            &mut constraints,
            SecondaryIndexField::property("tenant"),
            (0..(COMPOUND_INDEX_IN_EXPANSION_CAP as i64 + 1))
                .map(|value| CompoundOwnedValue::Property(PropValue::Int(value)))
                .collect(),
        );
        assert!(matches!(
            encode_compound_bounds_for_entry(&entry, &constraints),
            CompoundBoundsOutcome::InExpansionCapExceeded
        ));
    }

    #[test]
    fn compound_bounds_non_prefix_predicate_is_prefix_unsatisfied() {
        let entry = node_field_entry(
            &[
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ],
            SecondaryIndexKind::Equality,
            SecondaryIndexState::Ready,
        );
        let mut constraints = CompoundFieldConstraints::default();
        add_compound_equality_values(
            &mut constraints,
            SecondaryIndexField::property("score"),
            vec![CompoundOwnedValue::Property(PropValue::Int(5))],
        );
        assert!(matches!(
            encode_compound_bounds_for_entry(&entry, &constraints),
            CompoundBoundsOutcome::PrefixNotSatisfied
        ));
    }

    #[test]
    fn compound_bounds_building_declaration_is_ineligible() {
        let entry = node_field_entry(
            &[SecondaryIndexField::node_meta(NodeMetadataIndexField::Key)],
            SecondaryIndexKind::Equality,
            SecondaryIndexState::Building,
        );
        let mut constraints = CompoundFieldConstraints::default();
        add_compound_equality_values(
            &mut constraints,
            node_key_field(),
            vec![CompoundOwnedValue::String("alpha".to_string())],
        );
        assert!(matches!(
            encode_compound_bounds_for_entry(&entry, &constraints),
            CompoundBoundsOutcome::Ineligible
        ));
    }

    fn range_stats_key(first_byte: u8) -> crate::planner_stats::RangeStatsKey {
        let mut key = [0u8; crate::property_value_semantics::NUMERIC_RANGE_KEY_BYTES];
        key[0] = first_byte;
        key
    }

    fn range_costing_rollup() -> crate::planner_stats::CompoundIndexRollupStats {
        let mut rollup = crate::planner_stats::CompoundIndexRollupStats::default();
        rollup.index_id = 7;
        rollup.total_postings = 1_000;
        rollup.field_count = 2;
        rollup.prefix_stats = vec![crate::planner_stats::CompoundPrefixStats {
            prefix_len: 1,
            distinct_prefixes: 10,
            max_postings_per_prefix: 30,
            exact_prefix_postings: Vec::new(),
        }];
        rollup.range_stats = vec![crate::planner_stats::CompoundRangeStats {
            equality_prefix_len: 1,
            range_field_ordinal: 1,
            total_numeric_entries: 100,
            min_key: Some(range_stats_key(10)),
            max_key: Some(range_stats_key(50)),
            buckets: vec![
                crate::planner_stats::RangeBucket {
                    upper_key: range_stats_key(20),
                    count: 25,
                },
                crate::planner_stats::RangeBucket {
                    upper_key: range_stats_key(35),
                    count: 50,
                },
                crate::planner_stats::RangeBucket {
                    upper_key: range_stats_key(50),
                    count: 25,
                },
            ],
        }];
        rollup
    }

    fn range_costing_prefix_bounds() -> Vec<crate::secondary_index_key::CompoundPrefixBounds> {
        vec![crate::secondary_index_key::CompoundPrefixBounds {
            lower: vec![1, 2, 3],
            upper_exclusive: vec![1, 2, 4],
        }]
    }

    #[test]
    fn compound_range_costing_scales_with_actual_bounds() {
        let rollup = range_costing_rollup();
        let prefix_bounds = range_costing_prefix_bounds();
        // Prefix fallback estimate: max(30, 1000 / 10) = 100 postings.
        let (narrow, _) = ReadView::compound_range_rollup_estimate(
            &rollup,
            &prefix_bounds,
            1,
            1,
            Some((range_stats_key(11), true)),
            Some((range_stats_key(15), true)),
        );
        let (wide, _) = ReadView::compound_range_rollup_estimate(
            &rollup,
            &prefix_bounds,
            1,
            1,
            Some((range_stats_key(10), true)),
            Some((range_stats_key(50), true)),
        );
        assert!(narrow < wide, "narrow {narrow} should be < wide {wide}");
        // The wide (full-domain) range is capped by the prefix estimate.
        assert_eq!(wide, 100);
        // The narrow range covers only the first histogram bucket.
        assert_eq!(narrow, 25);
    }

    #[test]
    fn compound_range_costing_out_of_domain_estimates_zero() {
        let rollup = range_costing_rollup();
        let prefix_bounds = range_costing_prefix_bounds();
        let (estimate, exact) = ReadView::compound_range_rollup_estimate(
            &rollup,
            &prefix_bounds,
            1,
            1,
            Some((range_stats_key(60), true)),
            None,
        );
        assert_eq!(estimate, 0);
        assert!(exact);
    }

    #[test]
    fn compound_range_costing_missing_stats_falls_back_to_total_postings() {
        let mut rollup = range_costing_rollup();
        rollup.range_stats.clear();
        let prefix_bounds = range_costing_prefix_bounds();
        let (estimate, exact) = ReadView::compound_range_rollup_estimate(
            &rollup,
            &prefix_bounds,
            1,
            1,
            None,
            None,
        );
        assert_eq!(estimate, rollup.total_postings);
        assert!(!exact);
    }
}
