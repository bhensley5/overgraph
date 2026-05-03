const QUERY_RANGE_CANDIDATE_CAP: usize =
    crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP;
const QUERY_BROAD_SOURCE_FACTOR: u64 = 4;
const MAX_BOOLEAN_PLANNING_PROBE_IDS: usize = 16_384;
const MAX_BOOLEAN_UNION_INPUTS: usize = 256;
const TINY_EXPLICIT_ANCHOR_MAX: u64 = 16;
const PLAN_COST_UNKNOWN_WORK: u64 = u64::MAX;
const FANOUT_COST_UNKNOWN_WORK: u64 = u64::MAX / 4;
const FANOUT_HUB_HIGH_RATIO: u64 = 8;
const FANOUT_HUB_MEDIUM_RATIO: u64 = 4;
const FANOUT_CONFIDENCE_DOWNGRADE_STEP: u8 = 1;

struct PlannedNodeQuery {
    driver: NodePhysicalPlan,
    cap_context: QueryCapContext,
    warnings: Vec<QueryPlanWarning>,
}

struct PlannedPatternQuery {
    anchor_index: usize,
    anchor_alias: String,
    sort_anchor_alias: String,
    anchor_plan: PlannedNodeQuery,
    expansion_order: Vec<usize>,
    warnings: Vec<QueryPlanWarning>,
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

struct CandidateProbe {
    source: Option<PlannedNodeCandidateSource>,
    warning: Option<QueryPlanWarning>,
    followup: Option<SecondaryIndexReadFollowup>,
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

#[derive(Clone, Copy, Debug, Default)]
struct QueryCapContext {
    cheapest_legal_universe: Option<PlannerEstimate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FanoutCoverage {
    Complete,
    GlobalFallback,
    Missing,
}

impl FanoutCoverage {
    fn complete(self) -> bool {
        matches!(self, FanoutCoverage::Complete)
    }

    fn rank(self) -> u8 {
        match self {
            FanoutCoverage::Complete => 0,
            FanoutCoverage::GlobalFallback => 1,
            FanoutCoverage::Missing => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FanoutHubRisk {
    Low,
    Medium,
    High,
    Unknown,
}

impl FanoutHubRisk {
    fn rank(self) -> u8 {
        match self {
            FanoutHubRisk::Low => 0,
            FanoutHubRisk::Medium => 1,
            FanoutHubRisk::High => 2,
            FanoutHubRisk::Unknown => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FanoutEstimate {
    avg_upper_fanout: u64,
    p99_fanout: u64,
    max_fanout: u64,
    hub_risk: FanoutHubRisk,
    confidence: EstimateConfidence,
    coverage: FanoutCoverage,
    canonical_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PatternPlanCost {
    anchor_cost: PlanCost,
    estimated_work: u64,
    simulated_frontier: u64,
    fanout_complete: bool,
    confidence_rank: u8,
    stale_risk_rank: u8,
    frontier_capped: bool,
    canonical_key: String,
}

impl Ord for PatternPlanCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let primary = if self.fanout_complete && other.fanout_complete {
            self.estimated_work
                .cmp(&other.estimated_work)
                .then_with(|| self.simulated_frontier.cmp(&other.simulated_frontier))
                .then_with(|| self.confidence_rank.cmp(&other.confidence_rank))
                .then_with(|| self.stale_risk_rank.cmp(&other.stale_risk_rank))
                .then_with(|| self.frontier_capped.cmp(&other.frontier_capped))
        } else {
            self.anchor_cost.cmp(&other.anchor_cost)
        };
        primary.then_with(|| self.canonical_key.cmp(&other.canonical_key))
    }
}

impl PartialOrd for PatternPlanCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

#[derive(Clone)]
enum NodeCandidateMaterialization {
    Precomputed(Vec<u64>),
    KeyLookup,
    NodeTypeIndex,
    PropertyEqualityIndex {
        index_id: u64,
        key: String,
        value: PropValue,
    },
    PropertyRangeIndex {
        index_id: u64,
        domain: SecondaryIndexRangeDomain,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    TimestampIndex {
        type_id: u32,
        lower_ms: i64,
        upper_ms: i64,
    },
    FallbackTypeScan,
    FallbackFullNodeScan,
}

fn normalize_candidate_ids(mut ids: Vec<u64>) -> Vec<u64> {
    ids.sort_unstable();
    ids.dedup();
    ids
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
        QueryPlanWarning::UnboundedPatternRejected => 4,
        QueryPlanWarning::EdgePropertyPostFilter => 5,
        QueryPlanWarning::IndexSkippedAsBroad => 6,
        QueryPlanWarning::CandidateCapExceeded => 7,
        QueryPlanWarning::RangeCandidateCapExceeded => 8,
        QueryPlanWarning::TimestampCandidateCapExceeded => 9,
        QueryPlanWarning::VerifyOnlyFilter => 10,
        QueryPlanWarning::BooleanBranchFallback => 11,
        QueryPlanWarning::PlanningProbeBudgetExceeded => 12,
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

fn should_skip_filter_planning_for_explicit_anchor(query: &NormalizedNodeQuery) -> bool {
    let Some(count) = explicit_anchor_universe_count(query) else {
        return false;
    };
    query.type_id.is_none() || count <= TINY_EXPLICIT_ANCHOR_MAX
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
    let mut hashes = Vec::new();
    for probe_value in equality_probe_values(value) {
        let hash = hash_prop_value(&probe_value);
        if !hashes.contains(&hash) {
            hashes.push(hash);
        }
    }
    hashes
}

fn equality_probe_values(value: &PropValue) -> Vec<PropValue> {
    if let Some(probe_values) = signed_zero_equality_probe_values(value) {
        probe_values.into_iter().collect()
    } else {
        vec![value.clone()]
    }
}

fn unique_in_probe_values(values: &[PropValue]) -> Vec<InProbeValue> {
    unique_in_probe_values_with_hash(values, hash_prop_value)
}

fn unique_in_probe_values_with_hash(
    values: &[PropValue],
    mut hash_fn: impl FnMut(&PropValue) -> u64,
) -> Vec<InProbeValue> {
    let mut by_canonical: BTreeMap<Vec<u8>, InProbeValue> = BTreeMap::new();
    for value in values {
        for probe_value in equality_probe_values(value) {
            let canonical_key = prop_value_canonical_bytes(&probe_value);
            match by_canonical.entry(canonical_key) {
                std::collections::btree_map::Entry::Occupied(_) => {}
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let value_hash = hash_fn(&probe_value);
                    entry.insert(InProbeValue {
                        value: probe_value,
                        value_hash,
                    });
                }
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

fn higher_hub_risk(left: FanoutHubRisk, right: FanoutHubRisk) -> FanoutHubRisk {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn worse_fanout_coverage(left: FanoutCoverage, right: FanoutCoverage) -> FanoutCoverage {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

impl FanoutEstimate {
    fn zero(canonical_key: String) -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: FanoutHubRisk::Low,
            confidence: EstimateConfidence::Exact,
            coverage: FanoutCoverage::Complete,
            canonical_key,
        }
    }

    fn unknown(canonical_key: String) -> Self {
        Self {
            avg_upper_fanout: 0,
            p99_fanout: 0,
            max_fanout: 0,
            hub_risk: FanoutHubRisk::Unknown,
            confidence: EstimateConfidence::Unknown,
            coverage: FanoutCoverage::Missing,
            canonical_key,
        }
    }

    fn from_rollup(
        rollup: &crate::planner_stats::AdjacencyRollupStats,
        canonical_key: String,
        coverage: FanoutCoverage,
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
            FanoutHubRisk::High
        } else {
            let baseline = avg_upper_fanout.max(1);
            if (rollup.max_fanout as u64) >= baseline.saturating_mul(FANOUT_HUB_HIGH_RATIO) {
                FanoutHubRisk::High
            } else if (rollup.p99_fanout as u64)
                >= baseline.saturating_mul(FANOUT_HUB_MEDIUM_RATIO)
            {
                FanoutHubRisk::Medium
            } else {
                FanoutHubRisk::Low
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
            hub_risk: higher_hub_risk(self.hub_risk, other.hub_risk),
            confidence: weaker_confidence(self.confidence, other.confidence),
            coverage: worse_fanout_coverage(self.coverage, other.coverage),
            canonical_key,
        }
    }

    fn cost_fanout(&self) -> u64 {
        if self.avg_upper_fanout == 0 && self.p99_fanout == 0 && self.max_fanout == 0 {
            return 0;
        }
        match self.hub_risk {
            FanoutHubRisk::High => self
                .avg_upper_fanout
                .max(self.p99_fanout)
                .saturating_add(self.max_fanout / 2),
            FanoutHubRisk::Medium => self
                .avg_upper_fanout
                .max(self.p99_fanout)
                .saturating_add(self.max_fanout / 4),
            FanoutHubRisk::Low => self.avg_upper_fanout.max(1),
            FanoutHubRisk::Unknown => FANOUT_COST_UNKNOWN_WORK,
        }
    }

    fn complete(&self) -> bool {
        self.coverage.complete()
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

fn cap_warning_for_source(kind: NodeQueryCandidateSourceKind) -> QueryPlanWarning {
    match kind {
        NodeQueryCandidateSourceKind::PropertyRangeIndex => {
            QueryPlanWarning::RangeCandidateCapExceeded
        }
        NodeQueryCandidateSourceKind::TimestampIndex => QueryPlanWarning::TimestampCandidateCapExceeded,
        _ => QueryPlanWarning::CandidateCapExceeded,
    }
}

impl NodeQueryCandidateSourceKind {
    fn plan_node(self) -> QueryPlanNode {
        match self {
            NodeQueryCandidateSourceKind::ExplicitIds => QueryPlanNode::ExplicitIds,
            NodeQueryCandidateSourceKind::KeyLookup => QueryPlanNode::KeyLookup,
            NodeQueryCandidateSourceKind::NodeTypeIndex => QueryPlanNode::NodeTypeIndex,
            NodeQueryCandidateSourceKind::PropertyEqualityIndex => {
                QueryPlanNode::PropertyEqualityIndex
            }
            NodeQueryCandidateSourceKind::PropertyRangeIndex => QueryPlanNode::PropertyRangeIndex,
            NodeQueryCandidateSourceKind::TimestampIndex => QueryPlanNode::TimestampIndex,
            NodeQueryCandidateSourceKind::FallbackTypeScan => QueryPlanNode::FallbackTypeScan,
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
            NodeQueryCandidateSourceKind::NodeTypeIndex => 5,
            NodeQueryCandidateSourceKind::FallbackTypeScan => 6,
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

    fn node_type_index(type_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::NodeTypeIndex,
            canonical_key: format!("type:{type_id}"),
            estimate,
            materialization: NodeCandidateMaterialization::NodeTypeIndex,
        }
    }

    fn fallback_type_scan(type_id: u32, estimate: PlannerEstimate) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::FallbackTypeScan,
            canonical_key: format!("fallback_type:{type_id}"),
            estimate,
            materialization: NodeCandidateMaterialization::FallbackTypeScan,
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
        type_id: u32,
        index_id: u64,
        key: &str,
        value: &PropValue,
        estimate: PlannerEstimate,
    ) -> Self {
        Self::property_equality_index_with_hash(
            type_id,
            index_id,
            key,
            value,
            hash_prop_value(value),
            estimate,
        )
    }

    fn property_equality_index_with_hash(
        type_id: u32,
        index_id: u64,
        key: &str,
        value: &PropValue,
        value_hash: u64,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::PropertyEqualityIndex,
            canonical_key: format!("eq:{type_id}:{key}:{value_hash}"),
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
        domain: SecondaryIndexRangeDomain,
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
                domain,
                lower: lower.cloned(),
                upper: upper.cloned(),
            },
        }
    }

    fn timestamp_index(
        type_id: u32,
        lower_ms: i64,
        upper_ms: i64,
        estimate: PlannerEstimate,
    ) -> Self {
        Self {
            kind: NodeQueryCandidateSourceKind::TimestampIndex,
            canonical_key: format!("time:{type_id}:{lower_ms}:{upper_ms}"),
            estimate,
            materialization: NodeCandidateMaterialization::TimestampIndex {
                type_id,
                lower_ms,
                upper_ms,
            },
        }
    }

    fn plan_node(&self) -> QueryPlanNode {
        self.kind.plan_node()
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
            NodeCandidateMaterialization::NodeTypeIndex
            | NodeCandidateMaterialization::FallbackTypeScan
            | NodeCandidateMaterialization::FallbackFullNodeScan => {
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
            NodeQueryCandidateSourceKind::NodeTypeIndex
            | NodeQueryCandidateSourceKind::FallbackTypeScan => (24, 3),
            NodeQueryCandidateSourceKind::FallbackFullNodeScan => (64, 4),
        };
        setup.saturating_add(count.saturating_mul(candidate_weight))
    }
}

impl PlannedNodeQuery {
    fn estimated_candidate_count(&self) -> Option<u64> {
        self.driver.estimate().known_upper_bound()
    }

    fn explain_plan(&self) -> QueryPlan {
        QueryPlan {
            kind: QueryPlanKind::NodeQuery,
            root: QueryPlanNode::VerifyNodeFilter {
                input: Box::new(self.driver.plan_node()),
            },
            estimated_candidates: self.estimated_candidate_count(),
            warnings: self.warnings.clone(),
        }
    }
}

impl PlannedPatternQuery {
    fn explain_plan(&self) -> QueryPlan {
        let anchor_input = QueryPlanNode::VerifyNodeFilter {
            input: Box::new(self.anchor_plan.driver.plan_node()),
        };
        let pattern = QueryPlanNode::PatternExpand {
            anchor_alias: self.anchor_alias.clone(),
            input: Box::new(anchor_input),
        };
        let root = if self
            .warnings
            .contains(&QueryPlanWarning::EdgePropertyPostFilter)
        {
            QueryPlanNode::VerifyEdgePredicates {
                input: Box::new(pattern),
            }
        } else {
            pattern
        };
        QueryPlan {
            kind: QueryPlanKind::PatternQuery,
            root,
            estimated_candidates: self.anchor_plan.estimated_candidate_count(),
            warnings: self.warnings.clone(),
        }
    }
}

impl ReadView {
    fn key_lookup_candidate_ids(
        &self,
        query: &NormalizedNodeQuery,
    ) -> Result<Vec<u64>, EngineError> {
        let type_id = query
            .type_id
            .expect("normalized key query must have type_id");
        let key_refs: Vec<(u32, &str)> = query
            .keys
            .iter()
            .map(|key| (type_id, key.as_str()))
            .collect();
        let ids = self
            .get_nodes_by_keys_raw(&key_refs)?
            .into_iter()
            .flatten()
            .map(|node| node.id)
            .collect();
        Ok(normalize_candidate_ids(ids))
    }

    fn equality_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        type_id: u32,
        key: &str,
        value: &PropValue,
    ) -> Result<CandidateProbe, EngineError> {
        let Some(entry) =
            self.node_property_index_entry(type_id, key, &SecondaryIndexKind::Equality)
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
                type_id,
                entry.index_id,
                key,
                value,
                estimate,
            )),
            warning: None,
            followup,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn range_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        type_id: u32,
        key: &str,
        lower: Option<&PropertyRangeBound>,
        upper: Option<&PropertyRangeBound>,
        budget: &mut BooleanPlanningBudget,
    ) -> Result<CandidateProbe, EngineError> {
        let domain = Self::validate_property_range_bounds(lower, upper, None)?;
        let Some(entry) = self.node_property_index_entry(
            type_id,
            key,
            &SecondaryIndexKind::Range { domain },
        ) else {
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

        let lower_encoded = Self::encode_property_range_bound(domain, lower);
        let upper_encoded = Self::encode_property_range_bound(domain, upper);
        if let Some(stats_estimate) =
            self.planner_stats
                .range_index_estimate(entry.index_id, lower_encoded, upper_encoded)
        {
            let mut count = self
                .memtable
                .visible_secondary_range_entries(
                    entry.index_id,
                    lower_encoded,
                    upper_encoded,
                    None,
                    self.snapshot_seq,
                )
                .len() as u64;
            for epoch in &self.immutable_epochs {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .visible_secondary_range_entries(
                            entry.index_id,
                            lower_encoded,
                            upper_encoded,
                            None,
                            self.snapshot_seq,
                        )
                        .len() as u64,
                );
            }
            count = count.saturating_add(stats_estimate.count);

            let mut used_fallback = false;
            let coverage = &self
                .planner_stats
                .range_index_rollups
                .get(&entry.index_id)
                .expect("range estimate must have matching rollup")
                .coverage;
            let uncovered_segments: Vec<&SegmentReader> = self
                .segments
                .iter()
                .filter(|segment| !coverage.covers(segment.segment_id))
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
                self.note_range_planning_probe();

                let mut fallback_ids = 0usize;
                let total_read_limit = probe_limit.saturating_add(1);
                for segment in uncovered_segments {
                    let remaining_read_limit = total_read_limit.saturating_sub(fallback_ids);
                    if remaining_read_limit == 0 {
                        break;
                    }
                    match segment.find_nodes_by_secondary_range_index_if_present_limited(
                        entry.index_id,
                        lower_encoded,
                        upper_encoded,
                        None,
                        Some(remaining_read_limit),
                    ) {
                        Ok(Some(ids)) => {
                            fallback_ids = fallback_ids.saturating_add(ids.len());
                            if fallback_ids > probe_limit {
                                break;
                            }
                        }
                        Ok(None) => {
                            return Ok(CandidateProbe {
                                source: None,
                                warning: Some(QueryPlanWarning::MissingReadyIndex),
                                followup: self.range_sidecar_failure_followup(entry.index_id, None),
                            });
                        }
                        Err(error) => {
                            return Ok(CandidateProbe {
                                source: None,
                                warning: Some(QueryPlanWarning::MissingReadyIndex),
                                followup: self
                                    .range_sidecar_failure_followup(entry.index_id, Some(error)),
                            });
                        }
                    }
                }
                budget.consume_probe_ids(fallback_ids);
                if fallback_ids > probe_limit {
                    return Ok(CandidateProbe {
                        source: None,
                        warning: Some(if probe_limit < QUERY_RANGE_CANDIDATE_CAP {
                            QueryPlanWarning::PlanningProbeBudgetExceeded
                        } else {
                            QueryPlanWarning::RangeCandidateCapExceeded
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
                NodeQueryCandidateSourceKind::PropertyRangeIndex,
                query.page.limit,
                estimate,
            ) {
                return Ok(CandidateProbe {
                    source: None,
                    warning: Some(QueryPlanWarning::RangeCandidateCapExceeded),
                    followup: None,
                });
            }

            return Ok(CandidateProbe {
                source: Some(PlannedNodeCandidateSource::property_range_index(
                    entry.index_id,
                    key,
                    domain,
                    lower,
                    upper,
                    estimate,
                )),
                warning: None,
                followup: None,
            });
        }

        #[cfg(test)]
        self.note_range_planning_probe();
        let probe_limit = budget.probe_limit();
        if probe_limit == 0 {
            return Ok(CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::PlanningProbeBudgetExceeded),
                followup: None,
            });
        }

        let (ids, followup) = self.ready_range_candidate_ids(
            entry.index_id,
            domain,
            lower,
            upper,
            probe_limit + 1,
        )?;
        Ok(match ids {
            Some(ids) if ids.len() <= probe_limit => {
                budget.consume_probe_ids(ids.len());
                CandidateProbe {
                    source: Some(PlannedNodeCandidateSource::property_range_index(
                        entry.index_id,
                        key,
                        domain,
                        lower,
                        upper,
                        PlannerEstimate::exact_cheap(ids.len() as u64),
                    )),
                    warning: None,
                    followup,
                }
            }
            Some(ids) => {
                budget.consume_probe_ids(ids.len().min(probe_limit + 1));
                CandidateProbe {
                    source: None,
                    warning: Some(if probe_limit < QUERY_RANGE_CANDIDATE_CAP {
                        QueryPlanWarning::PlanningProbeBudgetExceeded
                    } else {
                        QueryPlanWarning::RangeCandidateCapExceeded
                    }),
                    followup,
                }
            }
            None => CandidateProbe {
                source: None,
                warning: Some(QueryPlanWarning::MissingReadyIndex),
                followup,
            },
        })
    }

    fn timestamp_candidate_probe(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        type_id: u32,
        lower_ms: i64,
        upper_ms: i64,
        budget: &mut BooleanPlanningBudget,
    ) -> Result<CandidateProbe, EngineError> {
        if let Some(stats_estimate) =
            self.planner_stats.timestamp_estimate(type_id, lower_ms, upper_ms)
        {
            let mut count = self
                .memtable
                .visible_nodes_by_time_range(type_id, lower_ms, upper_ms, self.snapshot_seq)
                .len() as u64;
            for epoch in &self.immutable_epochs {
                count = count.saturating_add(
                    epoch
                        .memtable
                        .visible_nodes_by_time_range(
                            type_id,
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
                        .timestamp_covers_segment(type_id, segment.segment_id)
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
                        type_id,
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
                    type_id, lower_ms, upper_ms, estimate,
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
            type_id,
            lower_ms,
            upper_ms,
            probe_limit + 1,
        )?;
        if ids.len() <= probe_limit {
            budget.consume_probe_ids(ids.len());
            Ok(CandidateProbe {
                source: Some(PlannedNodeCandidateSource::timestamp_index(
                    type_id,
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

    fn node_type_estimate(&self, type_id: u32) -> Result<PlannerEstimate, EngineError> {
        let mut count = self
            .memtable
            .visible_nodes_by_type_count(type_id, self.snapshot_seq) as u64;
        if self.active_memtable_only_exact_estimates() {
            return Ok(PlannerEstimate::exact_cheap(count));
        }
        for epoch in &self.immutable_epochs {
            count += epoch
                .memtable
                .visible_nodes_by_type_count(type_id, self.snapshot_seq) as u64;
        }
        count = count.saturating_add(self.planner_stats.type_node_count(type_id));
        let mut used_fallback = self.planner_stats.type_coverage.has_uncovered();
        for segment in &self.segments {
            if self.planner_stats.type_coverage.covers(segment.segment_id) {
                continue;
            }
            used_fallback = true;
            count = count.saturating_add(segment.node_type_posting_count(type_id)? as u64);
        }
        Ok(self.planner_stats_estimate_from_rollup(
            count,
            self.planner_stats.type_coverage.covered_count() > 0,
            used_fallback,
            true,
        ))
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
        if let Some(type_id) = query.type_id {
            let estimate = self.node_type_estimate(type_id)?;
            let source = if filter_driver {
                PlannedNodeCandidateSource::fallback_type_scan(type_id, estimate)
            } else {
                PlannedNodeCandidateSource::node_type_index(type_id, estimate)
            };
            plans.push(NodePhysicalPlan::source(source));
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
        let Some(type_id) = query.type_id else {
            add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
            return Ok(BooleanPlanResult {
                classification: BooleanPlanClassification::VerifyOnly,
                has_verify_only: true,
            });
        };
        if values.len() == 1 {
            let probe = self.equality_candidate_probe(query, cap_context, type_id, key, &values[0])?;
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
            self.node_property_index_entry(type_id, key, &SecondaryIndexKind::Equality)
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
            let (estimate, followup) =
                self.equality_candidate_estimate(entry.index_id, key, &probe.value)?;
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
                    type_id,
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
                let Some(type_id) = query.type_id else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.equality_candidate_probe(query, cap_context, type_id, key, value)?;
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
                let Some(type_id) = query.type_id else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.range_candidate_probe(
                    query,
                    cap_context,
                    type_id,
                    key,
                    lower.as_ref(),
                    upper.as_ref(),
                    budget,
                )?;
                Ok(self.classification_from_probe(probe, structural_key, warnings, followups))
            }
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                let Some(type_id) = query.type_id else {
                    add_plan_warning(warnings, QueryPlanWarning::MissingReadyIndex);
                    return Ok(BooleanPlanResult {
                        classification: BooleanPlanClassification::VerifyOnly,
                        has_verify_only: true,
                    });
                };
                let probe = self.timestamp_candidate_probe(
                    query,
                    cap_context,
                    type_id,
                    *lower_ms,
                    *upper_ms,
                    budget,
                )?;
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
        let mut warnings = Vec::new();
        if query.filter.is_always_false() {
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedNodeQuery {
                driver: NodePhysicalPlan::Empty,
                cap_context: QueryCapContext::default(),
                warnings,
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
                "node query requires type_id, ids, keys, or allow_full_scan".into(),
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
                if source.kind == NodeQueryCandidateSourceKind::FallbackTypeScan =>
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
        })
    }

    fn explain_node_query(&self, query: &NodeQuery) -> Result<QueryPlan, EngineError> {
        let normalized = self.normalize_node_query(query)?;
        let planned = self.plan_normalized_node_query(&normalized)?;
        Ok(planned.explain_plan())
    }

    fn normalized_node_pattern_has_initial_universe(pattern: &NormalizedNodePattern) -> bool {
        !pattern.query.ids.is_empty()
            || !pattern.query.keys.is_empty()
            || pattern.query.type_id.is_some()
            || pattern.query.filter.is_always_false()
    }

    fn pattern_known_node_ids_for_fanout(
        &self,
        pattern: &NormalizedNodePattern,
    ) -> Option<Vec<u64>> {
        if !pattern.query.ids.is_empty() {
            return Some(pattern.query.ids.clone());
        }
        if pattern.query.keys.is_empty()
            || pattern.query.type_id.is_none()
            || pattern.query.keys.len() as u64 > TINY_EXPLICIT_ANCHOR_MAX
        {
            return None;
        }
        self.key_lookup_candidate_ids(&pattern.query)
            .ok()
            .map(normalize_candidate_ids)
    }

    fn has_unrolled_memtable_edges(&self) -> bool {
        self.memtable.edge_count() > 0
            || self
                .immutable_epochs
                .iter()
                .any(|epoch| epoch.memtable.edge_count() > 0)
    }

    fn planner_direction(direction: Direction) -> &'static [PlannerStatsDirection] {
        match direction {
            Direction::Outgoing => &[PlannerStatsDirection::Outgoing],
            Direction::Incoming => &[PlannerStatsDirection::Incoming],
            Direction::Both => &[
                PlannerStatsDirection::Outgoing,
                PlannerStatsDirection::Incoming,
            ],
        }
    }

    fn fanout_rollup_estimate(
        &self,
        direction: PlannerStatsDirection,
        edge_type_id: Option<u32>,
        coverage: FanoutCoverage,
        confidence: EstimateConfidence,
        canonical_key: String,
        known_source_ids: Option<&[u64]>,
    ) -> Option<FanoutEstimate> {
        let rollup = self
            .planner_stats
            .adjacency_rollups
            .get(&(direction, edge_type_id))?;
        let coverage = if self.has_unrolled_memtable_edges() || rollup.coverage.has_uncovered() {
            FanoutCoverage::GlobalFallback
        } else {
            coverage
        };
        let mut confidence = confidence;
        if self.has_unrolled_memtable_edges() {
            confidence = downgrade_confidence(confidence, FANOUT_CONFIDENCE_DOWNGRADE_STEP);
        }
        Some(FanoutEstimate::from_rollup(
            rollup,
            canonical_key,
            coverage,
            confidence,
            known_source_ids,
        ))
    }

    fn single_direction_fanout_estimate(
        &self,
        direction: PlannerStatsDirection,
        type_filter: Option<&[u32]>,
        known_source_ids: Option<&[u64]>,
    ) -> FanoutEstimate {
        let direction_key = match direction {
            PlannerStatsDirection::Outgoing => "out",
            PlannerStatsDirection::Incoming => "in",
        };
        let Some(types) = type_filter else {
            return self
                .fanout_rollup_estimate(
                    direction,
                    None,
                    FanoutCoverage::Complete,
                    EstimateConfidence::High,
                    format!("{direction_key}:global"),
                    known_source_ids,
                )
                .unwrap_or_else(|| FanoutEstimate::unknown(format!("{direction_key}:global")));
        };

        if types.len() == 1 {
            let type_id = types[0];
            if let Some(estimate) = self.fanout_rollup_estimate(
                direction,
                Some(type_id),
                FanoutCoverage::Complete,
                EstimateConfidence::High,
                format!("{direction_key}:type:{type_id}"),
                known_source_ids,
            ) {
                return estimate;
            }
            if !self.has_unrolled_memtable_edges()
                && self
                    .planner_stats
                    .adjacency_rollups
                    .get(&(direction, None))
                    .is_some_and(|rollup| !rollup.coverage.has_uncovered())
            {
                return FanoutEstimate::zero(format!("{direction_key}:type:{type_id}:absent"));
            }
            return self
                .fanout_rollup_estimate(
                    direction,
                    None,
                    FanoutCoverage::GlobalFallback,
                    EstimateConfidence::Low,
                    format!("{direction_key}:global_fallback:{type_id}"),
                    known_source_ids,
                )
                .unwrap_or_else(|| {
                    FanoutEstimate::unknown(format!("{direction_key}:missing:{type_id}"))
                });
        }

        let mut combined: Option<FanoutEstimate> = None;
        for type_id in types {
            let Some(estimate) = self.fanout_rollup_estimate(
                direction,
                Some(*type_id),
                FanoutCoverage::Complete,
                EstimateConfidence::High,
                format!("{direction_key}:type:{type_id}"),
                known_source_ids,
            ) else {
                return self
                    .fanout_rollup_estimate(
                        direction,
                        None,
                        FanoutCoverage::GlobalFallback,
                        EstimateConfidence::Low,
                        format!("{direction_key}:global_fallback:{types:?}"),
                        known_source_ids,
                    )
                    .unwrap_or_else(|| {
                        FanoutEstimate::unknown(format!("{direction_key}:missing:{types:?}"))
                    });
            };
            combined = Some(match combined {
                Some(current) => current.combine_sum(
                    estimate,
                    format!("{direction_key}:types:{types:?}"),
                ),
                None => estimate,
            });
        }

        combined.unwrap_or_else(|| FanoutEstimate::unknown(format!("{direction_key}:empty")))
    }

    fn pattern_edge_fanout_estimate(
        &self,
        direction: Direction,
        type_filter: Option<&[u32]>,
        known_source_ids: Option<&[u64]>,
        query: &NormalizedGraphPatternQuery,
        edge: &NormalizedEdgePattern,
    ) -> FanoutEstimate {
        let mut combined: Option<FanoutEstimate> = None;
        for planner_direction in Self::planner_direction(direction) {
            let estimate = self.single_direction_fanout_estimate(
                *planner_direction,
                type_filter,
                known_source_ids,
            );
            combined = Some(match combined {
                Some(current) => current.combine_sum(
                    estimate,
                    format!("{direction:?}:{:?}", type_filter.unwrap_or(&[])),
                ),
                None => estimate,
            });
        }
        let mut estimate =
            combined.unwrap_or_else(|| FanoutEstimate::unknown(format!("{direction:?}:missing")));
        let mut downgrade_steps = 0u8;
        if query.at_epoch.is_some() {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if !self.manifest.prune_policies.is_empty() {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if !edge.property_predicates.is_empty() {
            downgrade_steps = downgrade_steps.saturating_add(1);
        }
        if downgrade_steps > 0 {
            estimate.confidence = downgrade_confidence(estimate.confidence, downgrade_steps);
        }
        estimate
    }

    fn pattern_target_selectivity(
        &self,
        pattern: &NormalizedNodePattern,
    ) -> Option<(u64, u64)> {
        if !Self::normalized_node_pattern_has_initial_universe(pattern)
            || pattern.query.filter.is_always_false()
        {
            return None;
        }
        let planned = self.plan_normalized_node_query(&pattern.query).ok()?;
        let target_count = planned.estimated_candidate_count()?;
        let universe_count = self.full_scan_estimate().known_upper_bound()?;
        if universe_count == 0 || target_count >= universe_count {
            None
        } else {
            Some((target_count, universe_count))
        }
    }

    fn apply_pattern_target_selectivity(
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

    fn plan_normalized_node_pattern_anchor(
        &self,
        pattern: &NormalizedNodePattern,
    ) -> Result<Option<PlannedNodeQuery>, EngineError> {
        if !Self::normalized_node_pattern_has_initial_universe(pattern) {
            return Ok(None);
        }

        let mut anchor_query = pattern.query.clone();
        anchor_query.allow_full_scan = false;
        let planned = self.plan_normalized_node_query(&anchor_query)?;
        if matches!(
            &planned.driver,
            NodePhysicalPlan::Source(source)
                if source.kind == NodeQueryCandidateSourceKind::FallbackFullNodeScan
        ) {
            return Err(EngineError::InvalidOperation(
                "pattern query cannot use a full node scan anchor".into(),
            ));
        }
        Ok(Some(planned))
    }

    fn planned_pattern_expansion_order_legacy(
        &self,
        query: &NormalizedGraphPatternQuery,
        anchor_index: usize,
    ) -> Vec<usize> {
        let mut bound = vec![false; query.nodes.len()];
        bound[anchor_index] = true;
        let mut visited_edges = vec![false; query.edges.len()];
        let mut order = Vec::with_capacity(query.edges.len());

        while order.len() < query.edges.len() {
            let mut candidates = Vec::new();
            for (edge_index, edge) in query.edges.iter().enumerate() {
                if visited_edges[edge_index] {
                    continue;
                }
                let from_bound = bound[edge.from_index];
                let to_bound = bound[edge.to_index];
                if !from_bound && !to_bound {
                    continue;
                }
                let both_bound_rank = if from_bound && to_bound { 0 } else { 1 };
                let alias_key = edge.alias.as_deref().unwrap_or("");
                candidates.push((
                    both_bound_rank,
                    alias_key,
                    query.nodes[edge.from_index].alias.as_str(),
                    query.nodes[edge.to_index].alias.as_str(),
                    edge_index,
                ));
            }
            candidates.sort_unstable();
            let Some((_, _, _, _, edge_index)) = candidates.first().copied() else {
                break;
            };
            visited_edges[edge_index] = true;
            let edge = &query.edges[edge_index];
            bound[edge.from_index] = true;
            bound[edge.to_index] = true;
            order.push(edge_index);
        }

        order
    }

    fn planned_pattern_expansion_order(
        &self,
        query: &NormalizedGraphPatternQuery,
        anchor_index: usize,
        anchor_plan: &PlannedNodeQuery,
        known_node_ids: &[Option<Vec<u64>>],
        target_selectivities: &[Option<(u64, u64)>],
    ) -> (Vec<usize>, PatternPlanCost) {
        let mut bound = vec![false; query.nodes.len()];
        bound[anchor_index] = true;
        let mut visited_edges = vec![false; query.edges.len()];
        let mut order = Vec::with_capacity(query.edges.len());
        let anchor_cost = anchor_plan.driver.plan_cost();
        let mut total_work = anchor_cost.estimated_work;
        let mut frontier = anchor_plan.estimated_candidate_count().unwrap_or(1).max(1);
        let mut fanout_complete = true;
        let mut confidence = EstimateConfidence::Exact;
        let mut stale_risk = StalePostingRisk::Low;
        let mut frontier_capped = frontier > PATTERN_FRONTIER_BUDGET as u64;
        frontier = frontier.min(PATTERN_FRONTIER_BUDGET as u64 + 1);

        while order.len() < query.edges.len() {
            let mut choices = Vec::new();
            for (edge_index, edge) in query.edges.iter().enumerate() {
                if visited_edges[edge_index] {
                    continue;
                }
                let from_bound = bound[edge.from_index];
                let to_bound = bound[edge.to_index];
                if !from_bound && !to_bound {
                    continue;
                }
                let both_bound_rank = if from_bound && to_bound { 0 } else { 1 };
                let (source_index, target_index, direction) = if from_bound {
                    (edge.from_index, edge.to_index, edge.direction)
                } else {
                    (
                        edge.to_index,
                        edge.from_index,
                        reverse_pattern_direction(edge.direction),
                    )
                };
                let fanout = self.pattern_edge_fanout_estimate(
                    direction,
                    edge.type_filter.as_deref(),
                    known_node_ids[source_index].as_deref(),
                    query,
                    edge,
                );
                let raw_expansion = frontier.saturating_mul(fanout.cost_fanout());
                let target_selectivity = if bound[target_index] {
                    None
                } else {
                    target_selectivities[target_index]
                };
                let mut estimated_expansion =
                    Self::apply_pattern_target_selectivity(raw_expansion, target_selectivity);
                if !edge.property_predicates.is_empty() {
                    estimated_expansion = estimated_expansion.saturating_add(raw_expansion);
                }
                let next_frontier = if bound[target_index] {
                    frontier
                } else {
                    estimated_expansion.min(PATTERN_FRONTIER_BUDGET as u64 + 1)
                };
                let alias_key = edge.alias.as_deref().unwrap_or("");
                choices.push((
                    both_bound_rank,
                    fanout.complete(),
                    estimated_expansion,
                    next_frontier,
                    fanout.confidence.rank(),
                    fanout.hub_risk.rank(),
                    fanout.coverage.rank(),
                    alias_key,
                    query.nodes[edge.from_index].alias.as_str(),
                    query.nodes[edge.to_index].alias.as_str(),
                    edge_index,
                    fanout,
                ));
            }
            choices.sort_by(|left, right| {
                left.0.cmp(&right.0).then_with(|| {
                    if left.1 && right.1 {
                        left.2
                            .cmp(&right.2)
                            .then_with(|| left.3.cmp(&right.3))
                            .then_with(|| left.4.cmp(&right.4))
                            .then_with(|| left.5.cmp(&right.5))
                            .then_with(|| left.6.cmp(&right.6))
                            .then_with(|| left.7.cmp(right.7))
                            .then_with(|| left.8.cmp(right.8))
                            .then_with(|| left.9.cmp(right.9))
                            .then_with(|| left.10.cmp(&right.10))
                    } else {
                        left.7
                            .cmp(right.7)
                            .then_with(|| left.8.cmp(right.8))
                            .then_with(|| left.9.cmp(right.9))
                            .then_with(|| left.10.cmp(&right.10))
                    }
                })
            });
            let Some((
                _,
                complete,
                estimated_expansion,
                next_frontier,
                _,
                _,
                _,
                _,
                _,
                _,
                edge_index,
                fanout,
            )) = choices.into_iter().next()
            else {
                break;
            };
            visited_edges[edge_index] = true;
            let edge = &query.edges[edge_index];
            bound[edge.from_index] = true;
            bound[edge.to_index] = true;
            order.push(edge_index);
            fanout_complete &= complete;
            confidence = weaker_confidence(confidence, fanout.confidence);
            if fanout.hub_risk == FanoutHubRisk::High {
                stale_risk = StalePostingRisk::Medium;
            }
            total_work = total_work.saturating_add(estimated_expansion);
            frontier_capped |= next_frontier > PATTERN_FRONTIER_BUDGET as u64;
            frontier = next_frontier.min(PATTERN_FRONTIER_BUDGET as u64 + 1);
        }

        if order.len() != query.edges.len() {
            let legacy = self.planned_pattern_expansion_order_legacy(query, anchor_index);
            order = legacy;
            fanout_complete = false;
            total_work = anchor_cost.estimated_work;
        }

        let cost = PatternPlanCost {
            anchor_cost,
            estimated_work: total_work,
            simulated_frontier: frontier,
            fanout_complete,
            confidence_rank: confidence.rank(),
            stale_risk_rank: stale_risk.rank(),
            frontier_capped,
            canonical_key: format!("{}:{anchor_index}", query.nodes[anchor_index].alias),
        };
        (order, cost)
    }

    fn plan_normalized_pattern_query(
        &self,
        query: &NormalizedGraphPatternQuery,
    ) -> Result<PlannedPatternQuery, EngineError> {
        if let Some((node_index, pattern)) = query
            .nodes
            .iter()
            .enumerate()
            .find(|(_, pattern)| pattern.query.filter.is_always_false())
        {
            let anchor_plan = self
                .plan_normalized_node_pattern_anchor(pattern)?
                .expect("AlwaysFalse pattern must be an anchorable empty universe");
            let mut warnings = anchor_plan.warnings.clone();
            finalize_plan_warnings(&mut warnings);
            return Ok(PlannedPatternQuery {
                anchor_index: node_index,
                anchor_alias: pattern.alias.clone(),
                sort_anchor_alias: pattern.alias.clone(),
                expansion_order: Vec::new(),
                warnings,
                anchor_plan,
            });
        }

        let mut anchor_candidates = Vec::new();
        let known_node_ids: Vec<_> = query
            .nodes
            .iter()
            .map(|pattern| self.pattern_known_node_ids_for_fanout(pattern))
            .collect();
        let target_selectivities: Vec<_> = query
            .nodes
            .iter()
            .map(|pattern| self.pattern_target_selectivity(pattern))
            .collect();
        for (node_index, pattern) in query.nodes.iter().enumerate() {
            let Some(planned) = self.plan_normalized_node_pattern_anchor(pattern)? else {
                continue;
            };
            let sort_count = planned.estimated_candidate_count().unwrap_or(u64::MAX);
            let sort_rank = planned.driver.selectivity_rank();
            let (expansion_order, pattern_cost) = self.planned_pattern_expansion_order(
                query,
                node_index,
                &planned,
                &known_node_ids,
                &target_selectivities,
            );
            anchor_candidates.push((
                pattern_cost,
                pattern.alias.as_str(),
                sort_count,
                sort_rank,
                node_index,
                expansion_order,
                planned,
            ));
        }

        anchor_candidates.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(right.1))
        });

        let sort_anchor_alias = anchor_candidates
            .iter()
            .min_by(|left, right| {
                left.2
                    .cmp(&right.2)
                    .then_with(|| left.3.cmp(&right.3))
                    .then_with(|| left.1.cmp(right.1))
            })
            .map(|candidate| candidate.1.to_string());

        let Some((_, _, _, _, anchor_index, expansion_order, anchor_plan)) =
            anchor_candidates.into_iter().next()
        else {
            return Err(EngineError::InvalidOperation(
                "pattern query requires an anchorable node pattern".into(),
            ));
        };

        if expansion_order.len() != query.edges.len() {
            return Err(EngineError::InvalidOperation(
                "pattern query must be one connected component".into(),
            ));
        }

        let mut warnings = anchor_plan.warnings.clone();
        if query
            .edges
            .iter()
            .any(|edge| !edge.property_predicates.is_empty())
        {
            add_plan_warning(&mut warnings, QueryPlanWarning::EdgePropertyPostFilter);
        }
        finalize_plan_warnings(&mut warnings);

        Ok(PlannedPatternQuery {
            anchor_index,
            anchor_alias: query.nodes[anchor_index].alias.clone(),
            sort_anchor_alias: sort_anchor_alias.unwrap_or_else(|| query.nodes[anchor_index].alias.clone()),
            expansion_order,
            warnings,
            anchor_plan,
        })
    }

    fn explain_pattern_query(&self, query: &GraphPatternQuery) -> Result<QueryPlan, EngineError> {
        let normalized = self.normalize_pattern_query(query)?;
        let planned = self.plan_normalized_pattern_query(&normalized)?;
        Ok(planned.explain_plan())
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
            .map(|probe| prop_value_canonical_bytes(&probe.value))
            .collect();
        assert_ne!(canonical_keys[0], canonical_keys[1]);
    }

    #[test]
    fn unique_in_probe_values_dedupes_exact_duplicates_by_canonical_value() {
        let values = vec![
            PropValue::String("a".to_string()),
            PropValue::String("a".to_string()),
        ];
        let probes = unique_in_probe_values_with_hash(&values, hash_prop_value);

        assert_eq!(probes.len(), 1);
        assert_eq!(probes[0].value, PropValue::String("a".to_string()));
        assert_eq!(probes[0].value_hash, hash_prop_value(&probes[0].value));
    }

    #[test]
    fn unique_in_probe_values_keeps_signed_zero_probe_expansion() {
        let probes = unique_in_probe_values(&[PropValue::Float(0.0)]);

        let mut actual_bits: Vec<u64> = probes
            .iter()
            .filter_map(|probe| match probe.value {
                PropValue::Float(value) => Some(value.to_bits()),
                _ => None,
            })
            .collect();
        actual_bits.sort_unstable();

        let mut expected_bits = vec![0.0f64.to_bits(), (-0.0f64).to_bits()];
        expected_bits.sort_unstable();
        assert_eq!(actual_bits, expected_bits);
    }
}
