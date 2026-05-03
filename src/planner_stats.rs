use crate::error::EngineError;
use crate::memtable::encode_range_prop_value;
use crate::segment_reader::SegmentReader;
use crate::segment_writer::{CompactEdgeMeta, CompactNodeMeta};
use crate::types::{
    hash_prop_value, EdgeRecord, NodeIdMap, NodeRecord, PropValue, SecondaryIndexKind,
    SecondaryIndexManifestEntry, SecondaryIndexRangeDomain, SecondaryIndexState,
    SecondaryIndexTarget,
};
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

pub(crate) const PLANNER_STATS_FILENAME: &str = "planner_stats.dat";
const PLANNER_STATS_TMP_FILENAME: &str = "planner_stats.tmp";
const PLANNER_STATS_MAGIC: [u8; 8] = *b"OGPST01\0";
pub(crate) const PLANNER_STATS_FORMAT_VERSION: u32 = 1;
const PLANNER_STATS_ENVELOPE_LEN: usize = 8 + 4 + 8 + 4 + 4;

pub(crate) const PLANNER_STATS_MAX_PROPERTY_KEYS_PER_TYPE: usize = 256;
const PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_TYPE: usize = 1024;
pub(crate) const PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY: usize = 32;
pub(crate) const PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES: usize = 4096;
pub(crate) const PLANNER_STATS_RANGE_BUCKETS: usize = 64;
pub(crate) const PLANNER_STATS_TIMESTAMP_BUCKETS: usize = 64;
pub(crate) const PLANNER_STATS_NODE_ID_SAMPLE_SIZE: usize = 1024;
pub(crate) const PLANNER_STATS_TOP_HUBS_PER_EDGE_TYPE: usize = 32;
pub(crate) const PLANNER_STATS_SOFT_SIDECAR_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const PLANNER_STATS_HARD_SIDECAR_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const PLANNER_STATS_HARD_CANDIDATE_CAP: usize = 65_536;
pub(crate) const PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP: usize = 4096;
pub(crate) const PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_NODES: usize = 1024;
pub(crate) const PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const PLANNER_STATS_REFRESH_GENERAL_PROP_DECODE_BUDGET_NODES: usize = 0;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PlannerStatsAvailability {
    Available(Box<SegmentPlannerStatsV1>),
    Missing,
    Unavailable { reason: String },
}

impl PlannerStatsAvailability {
    #[cfg(test)]
    pub(crate) fn stats(&self) -> Option<&SegmentPlannerStatsV1> {
        match self {
            PlannerStatsAvailability::Available(stats) => Some(stats.as_ref()),
            PlannerStatsAvailability::Missing | PlannerStatsAvailability::Unavailable { .. } => {
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn is_available(&self) -> bool {
        matches!(self, PlannerStatsAvailability::Available(_))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PlannerStatsBuildKind {
    Flush,
    Compaction,
    SecondaryIndexRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlannerStatsBuildMode {
    Flush,
    Compaction,
    TargetedSecondaryIndexRefresh { index_id: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlannerStatsBuildPolicy {
    pub mode: PlannerStatsBuildMode,
    pub general_property_decode_budget_nodes: usize,
    pub general_property_decode_budget_bytes: usize,
    pub declared_index_decode_budget_nodes: usize,
    pub allow_general_property_decode: bool,
}

impl PlannerStatsBuildPolicy {
    fn flush() -> Self {
        Self {
            mode: PlannerStatsBuildMode::Flush,
            general_property_decode_budget_nodes: usize::MAX,
            general_property_decode_budget_bytes: usize::MAX,
            declared_index_decode_budget_nodes: usize::MAX,
            allow_general_property_decode: true,
        }
    }

    fn compaction() -> Self {
        Self {
            mode: PlannerStatsBuildMode::Compaction,
            general_property_decode_budget_nodes:
                PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_NODES,
            general_property_decode_budget_bytes:
                PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_BYTES,
            declared_index_decode_budget_nodes: 0,
            allow_general_property_decode: true,
        }
    }

    fn targeted_secondary_index_refresh(index_id: u64) -> Self {
        Self {
            mode: PlannerStatsBuildMode::TargetedSecondaryIndexRefresh { index_id },
            general_property_decode_budget_nodes:
                PLANNER_STATS_REFRESH_GENERAL_PROP_DECODE_BUDGET_NODES,
            general_property_decode_budget_bytes: 0,
            declared_index_decode_budget_nodes: 0,
            allow_general_property_decode: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum PlannerStatsDeclaredIndexKind {
    Equality,
    Range,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeclaredIndexRuntimeCoverageState {
    Available,
    Missing,
    Corrupt,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeclaredIndexRuntimeCoverageKey {
    pub segment_id: u64,
    pub index_id: u64,
    pub kind: PlannerStatsDeclaredIndexKind,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DeclaredIndexRuntimeCoverage {
    states: BTreeMap<DeclaredIndexRuntimeCoverageKey, DeclaredIndexRuntimeCoverageState>,
}

impl DeclaredIndexRuntimeCoverage {
    pub(crate) fn from_readers(
        segments: &[Arc<SegmentReader>],
        secondary_indexes: &[SecondaryIndexManifestEntry],
    ) -> Self {
        let mut coverage = Self::default();
        for entry in secondary_indexes {
            if entry.state != SecondaryIndexState::Ready {
                continue;
            }
            if ready_node_property_target(entry).is_none() {
                continue;
            }
            let kind = match entry.kind {
                SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
                SecondaryIndexKind::Range { .. } => PlannerStatsDeclaredIndexKind::Range,
            };
            for segment in segments {
                coverage.insert(
                    segment.segment_id,
                    entry.index_id,
                    kind,
                    segment.declared_index_runtime_coverage_state(entry.index_id, kind),
                );
            }
        }
        coverage
    }

    pub(crate) fn insert(
        &mut self,
        segment_id: u64,
        index_id: u64,
        kind: PlannerStatsDeclaredIndexKind,
        state: DeclaredIndexRuntimeCoverageState,
    ) {
        self.states.insert(
            DeclaredIndexRuntimeCoverageKey {
                segment_id,
                index_id,
                kind,
            },
            state,
        );
    }

    pub(crate) fn state(
        &self,
        segment_id: u64,
        index_id: u64,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> DeclaredIndexRuntimeCoverageState {
        self.states
            .get(&DeclaredIndexRuntimeCoverageKey {
                segment_id,
                index_id,
                kind,
            })
            .copied()
            .unwrap_or(DeclaredIndexRuntimeCoverageState::Unknown)
    }

    pub(crate) fn is_available(
        &self,
        segment_id: u64,
        index_id: u64,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> bool {
        self.state(segment_id, index_id, kind) == DeclaredIndexRuntimeCoverageState::Available
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.states.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeclaredIndexStatsFingerprint {
    pub index_id: u64,
    pub kind: PlannerStatsDeclaredIndexKind,
    pub type_id: u32,
    pub prop_key: String,
    pub range_domain: Option<SecondaryIndexRangeDomain>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum PropertyStatsTrackedReason {
    DeclaredEquality,
    DeclaredRange,
    DeclaredEqualityAndRange,
    GeneralTopProperty,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum PlannerStatsDirection {
    #[default]
    Outgoing,
    Incoming,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct SegmentPlannerStatsV1 {
    pub format_version: u32,
    pub segment_id: u64,
    pub build_kind: PlannerStatsBuildKind,
    pub built_at_ms: i64,
    pub declaration_fingerprint: u64,
    pub declared_indexes: Vec<DeclaredIndexStatsFingerprint>,
    pub node_count: u64,
    pub edge_count: u64,
    pub truncated: bool,
    pub general_property_stats_complete: bool,
    pub general_property_sampled_node_count: u64,
    pub general_property_sampled_raw_bytes: u64,
    pub general_property_budget_exhausted: bool,
    pub type_stats: Vec<TypePlannerStats>,
    pub timestamp_stats: Vec<TimestampPlannerStats>,
    pub property_stats: Vec<PropertyPlannerStats>,
    pub equality_index_stats: Vec<EqualityIndexPlannerStats>,
    pub range_index_stats: Vec<RangeIndexPlannerStats>,
    pub adjacency_stats: Vec<AdjacencyPlannerStats>,
    pub node_id_sample: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TypePlannerStats {
    pub type_id: u32,
    pub node_count: u64,
    pub min_node_id: Option<u64>,
    pub max_node_id: Option<u64>,
    pub min_updated_at_ms: Option<i64>,
    pub max_updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TimestampPlannerStats {
    pub type_id: u32,
    pub count: u64,
    pub min_ms: i64,
    pub max_ms: i64,
    pub buckets: Vec<TimestampBucket>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TimestampBucket {
    pub upper_ms: i64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PropertyPlannerStats {
    pub type_id: u32,
    pub prop_key: String,
    pub tracked_reason: PropertyStatsTrackedReason,
    pub present_count: u64,
    pub null_count: u64,
    pub value_kind_counts: ValueKindCounts,
    pub exact_distinct_count: Option<u64>,
    pub distinct_lower_bound: Option<u64>,
    pub top_values: Vec<ValueFrequency>,
    pub numeric_summaries: Vec<RangeValueSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValueKindCounts {
    pub null_count: u64,
    pub bool_count: u64,
    pub int_count: u64,
    pub uint_count: u64,
    pub float_count: u64,
    pub string_count: u64,
    pub bytes_count: u64,
    pub array_count: u64,
    pub map_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RangeValueSummary {
    pub domain: SecondaryIndexRangeDomain,
    pub count: u64,
    pub min_encoded: Option<u64>,
    pub max_encoded: Option<u64>,
    pub buckets: Vec<RangeBucket>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct EqualityIndexPlannerStats {
    pub index_id: u64,
    pub type_id: u32,
    pub prop_key: String,
    pub total_postings: u64,
    pub value_group_count: u64,
    pub max_group_postings: u64,
    pub top_value_hashes: Vec<ValueFrequency>,
    pub sidecar_present_at_build: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RangeIndexPlannerStats {
    pub index_id: u64,
    pub type_id: u32,
    pub prop_key: String,
    pub domain: SecondaryIndexRangeDomain,
    pub total_entries: u64,
    pub min_encoded: Option<u64>,
    pub max_encoded: Option<u64>,
    pub buckets: Vec<RangeBucket>,
    pub sidecar_present_at_build: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RangeBucket {
    pub upper_encoded: u64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AdjacencyPlannerStats {
    pub direction: PlannerStatsDirection,
    pub edge_type_id: Option<u32>,
    pub source_node_count: u64,
    pub total_edges: u64,
    pub min_fanout: u32,
    pub max_fanout: u32,
    pub p50_fanout: u32,
    pub p90_fanout: u32,
    pub p99_fanout: u32,
    pub top_hubs: Vec<NodeFanoutFrequency>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValueFrequency {
    pub value_hash: u64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NodeFanoutFrequency {
    pub node_id: u64,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PlannerStatsWriteOutcome {
    Written,
    SkippedOversize,
    SkippedTargetUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlannerEstimateKind {
    ExactCheap,
    StatsExact,
    StatsEstimated,
    UpperBound,
    Unknown,
}

impl PlannerEstimateKind {
    pub(crate) fn rank(self) -> u8 {
        match self {
            PlannerEstimateKind::ExactCheap => 0,
            PlannerEstimateKind::StatsExact => 1,
            PlannerEstimateKind::StatsEstimated => 2,
            PlannerEstimateKind::UpperBound => 3,
            PlannerEstimateKind::Unknown => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EstimateConfidence {
    Exact,
    High,
    Medium,
    Low,
    Unknown,
}

impl EstimateConfidence {
    pub(crate) fn rank(self) -> u8 {
        match self {
            EstimateConfidence::Exact => 0,
            EstimateConfidence::High => 1,
            EstimateConfidence::Medium => 2,
            EstimateConfidence::Low => 3,
            EstimateConfidence::Unknown => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StalePostingRisk {
    Low,
    Medium,
    High,
    Unknown,
}

impl StalePostingRisk {
    pub(crate) fn rank(self) -> u8 {
        match self {
            StalePostingRisk::Low => 0,
            StalePostingRisk::Medium => 1,
            StalePostingRisk::High => 2,
            StalePostingRisk::Unknown => 3,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PlannerStatsFamilyCoverage {
    pub covered_segment_ids: Vec<u64>,
    pub uncovered_segment_ids: Vec<u64>,
    pub mismatched_segment_ids: Vec<u64>,
}

impl PlannerStatsFamilyCoverage {
    pub(crate) fn covers(&self, segment_id: u64) -> bool {
        self.covered_segment_ids.binary_search(&segment_id).is_ok()
    }

    pub(crate) fn covered_count(&self) -> usize {
        self.covered_segment_ids.len()
    }

    pub(crate) fn has_uncovered(&self) -> bool {
        !self.uncovered_segment_ids.is_empty() || !self.mismatched_segment_ids.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannerStatsValueEstimate {
    pub count: u64,
    pub exact: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PlannerStatsView {
    pub generation: u64,
    pub segment_count: usize,
    pub available_segment_stats: usize,
    pub missing_segment_stats: usize,
    pub unavailable_segment_stats: usize,
    pub full_rollup: FullRollupStats,
    pub type_coverage: PlannerStatsFamilyCoverage,
    pub timestamp_coverage: PlannerStatsFamilyCoverage,
    pub property_rollups: BTreeMap<(u32, String), PropertyRollupStats>,
    pub type_rollups: BTreeMap<u32, TypeRollupStats>,
    pub timestamp_rollups: BTreeMap<u32, TimestampRollupStats>,
    pub equality_index_rollups: BTreeMap<u64, EqualityIndexRollupStats>,
    pub range_index_rollups: BTreeMap<u64, RangeIndexRollupStats>,
    pub adjacency_rollups: BTreeMap<(PlannerStatsDirection, Option<u32>), AdjacencyRollupStats>,
    pub segment_stale_risks: BTreeMap<u64, StalePostingRisk>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FullRollupStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub coverage: PlannerStatsFamilyCoverage,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TypeRollupStats {
    pub type_id: u32,
    pub node_count: u64,
    pub min_node_id: Option<u64>,
    pub max_node_id: Option<u64>,
    pub min_updated_at_ms: Option<i64>,
    pub max_updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TimestampRollupStats {
    pub type_id: u32,
    pub count: u64,
    pub min_ms: Option<i64>,
    pub max_ms: Option<i64>,
    pub coverage: PlannerStatsFamilyCoverage,
    segment_rollups: BTreeMap<u64, TimestampSegmentRollupStats>,
}

#[derive(Clone, Debug, Default)]
struct TimestampSegmentRollupStats {
    count: u64,
    min_ms: Option<i64>,
    max_ms: Option<i64>,
    buckets: Vec<TimestampBucket>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PropertyRollupStats {
    pub type_id: u32,
    pub prop_key: String,
    pub present_count: u64,
    pub null_count: u64,
    pub top_values: BTreeMap<u64, u64>,
    pub coverage: PlannerStatsFamilyCoverage,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EqualityIndexRollupStats {
    pub index_id: u64,
    pub type_id: u32,
    pub prop_key: String,
    pub total_postings: u64,
    pub value_group_count: u64,
    pub max_group_postings: u64,
    pub top_value_hashes: BTreeMap<u64, u64>,
    pub coverage: PlannerStatsFamilyCoverage,
    segment_rollups: BTreeMap<u64, EqualitySegmentRollupStats>,
}

#[derive(Clone, Debug, Default)]
struct EqualitySegmentRollupStats {
    total_postings: u64,
    value_group_count: u64,
    top_value_hashes: BTreeMap<u64, u64>,
    top_value_total: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RangeIndexRollupStats {
    pub index_id: u64,
    pub type_id: u32,
    pub prop_key: String,
    pub domain: SecondaryIndexRangeDomain,
    pub total_entries: u64,
    pub min_encoded: Option<u64>,
    pub max_encoded: Option<u64>,
    pub coverage: PlannerStatsFamilyCoverage,
    segment_rollups: BTreeMap<u64, RangeIndexSegmentRollupStats>,
}

#[derive(Clone, Debug, Default)]
struct RangeIndexSegmentRollupStats {
    total_entries: u64,
    min_encoded: Option<u64>,
    max_encoded: Option<u64>,
    buckets: Vec<RangeBucket>,
}

impl Default for RangeIndexRollupStats {
    fn default() -> Self {
        Self {
            index_id: 0,
            type_id: 0,
            prop_key: String::new(),
            domain: SecondaryIndexRangeDomain::Int,
            total_entries: 0,
            min_encoded: None,
            max_encoded: None,
            coverage: PlannerStatsFamilyCoverage::default(),
            segment_rollups: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AdjacencyRollupStats {
    pub direction: PlannerStatsDirection,
    pub edge_type_id: Option<u32>,
    pub source_node_count: u64,
    pub total_edges: u64,
    pub max_fanout: u32,
    pub p99_fanout: u32,
    pub top_hubs: Vec<NodeFanoutFrequency>,
    pub coverage: PlannerStatsFamilyCoverage,
}

impl PlannerStatsView {
    pub(crate) fn build_from_readers(
        generation: u64,
        segments: &[Arc<SegmentReader>],
        secondary_indexes: &[SecondaryIndexManifestEntry],
        runtime_coverage: &DeclaredIndexRuntimeCoverage,
    ) -> Self {
        let segment_stale_risks = segment_stale_risks_from_readers(segments);
        let snapshots: Vec<_> = segments
            .iter()
            .map(|segment| PlannerStatsSegmentSnapshot {
                segment_id: segment.segment_id,
                node_count: segment.node_count(),
                edge_count: segment.edge_count(),
                availability: segment.planner_stats_availability(),
            })
            .collect();
        let mut view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
            generation,
            &snapshots,
            secondary_indexes,
            runtime_coverage,
        );
        view.segment_stale_risks = segment_stale_risks;
        view
    }

    pub(crate) fn type_node_count(&self, type_id: u32) -> u64 {
        self.type_rollups
            .get(&type_id)
            .map_or(0, |rollup| rollup.node_count)
    }

    pub(crate) fn equality_segment_estimate(
        &self,
        index_id: u64,
        segment_id: u64,
        value_hashes: &[u64],
    ) -> Option<PlannerStatsValueEstimate> {
        self.equality_index_rollups
            .get(&index_id)?
            .estimate_segment_hashes(segment_id, value_hashes)
    }

    pub(crate) fn range_index_estimate(
        &self,
        index_id: u64,
        lower: Option<(u64, bool)>,
        upper: Option<(u64, bool)>,
    ) -> Option<PlannerStatsValueEstimate> {
        let rollup = self.range_index_rollups.get(&index_id)?;
        if rollup.segment_rollups.is_empty() {
            return None;
        }
        let mut count = 0u64;
        let mut exact = !rollup.coverage.has_uncovered();
        for segment in rollup.segment_rollups.values() {
            let estimate = estimate_u64_histogram(
                segment.total_entries,
                segment.min_encoded,
                segment.max_encoded,
                segment
                    .buckets
                    .iter()
                    .map(|bucket| (bucket.upper_encoded, bucket.count)),
                lower,
                upper,
            );
            count = count.saturating_add(estimate.count);
            exact &= estimate.exact;
        }
        Some(PlannerStatsValueEstimate { count, exact })
    }

    pub(crate) fn timestamp_estimate(
        &self,
        type_id: u32,
        lower_ms: i64,
        upper_ms: i64,
    ) -> Option<PlannerStatsValueEstimate> {
        let Some(rollup) = self.timestamp_rollups.get(&type_id) else {
            if self.type_node_count(type_id) == 0 && self.type_coverage.covered_count() > 0 {
                return Some(PlannerStatsValueEstimate {
                    count: 0,
                    exact: !self.type_coverage.has_uncovered(),
                });
            }
            return None;
        };
        if rollup.segment_rollups.is_empty() {
            return None;
        }
        let mut count = 0u64;
        let mut exact = !rollup.coverage.has_uncovered();
        for segment in rollup.segment_rollups.values() {
            let estimate = estimate_i64_histogram(
                segment.count,
                segment.min_ms,
                segment.max_ms,
                segment
                    .buckets
                    .iter()
                    .map(|bucket| (bucket.upper_ms, bucket.count)),
                lower_ms,
                upper_ms,
            );
            count = count.saturating_add(estimate.count);
            exact &= estimate.exact;
        }
        Some(PlannerStatsValueEstimate { count, exact })
    }

    pub(crate) fn timestamp_covers_segment(&self, type_id: u32, segment_id: u64) -> bool {
        self.timestamp_rollups.get(&type_id).map_or_else(
            || self.type_node_count(type_id) == 0 && self.type_coverage.covers(segment_id),
            |rollup| rollup.coverage.covers(segment_id),
        )
    }

    pub(crate) fn max_segment_stale_risk(&self) -> StalePostingRisk {
        self.segment_stale_risks
            .values()
            .copied()
            .max_by_key(|risk| risk.rank())
            .unwrap_or(StalePostingRisk::Unknown)
    }

    fn validate_rollup_shape(&self) {
        let _generation = self.generation;
        debug_assert_eq!(
            self.segment_count,
            self.available_segment_stats
                .saturating_add(self.missing_segment_stats)
                .saturating_add(self.unavailable_segment_stats)
        );
        debug_assert!(
            self.full_rollup.coverage.covered_count() <= self.segment_count,
            "full stats coverage exceeds segment count"
        );
        debug_assert!(
            self.type_coverage.covered_count() <= self.segment_count,
            "type stats coverage exceeds segment count"
        );
        debug_assert!(
            self.timestamp_coverage.covered_count() <= self.segment_count,
            "timestamp stats coverage exceeds segment count"
        );
        for (type_id, rollup) in &self.type_rollups {
            debug_assert_eq!(*type_id, rollup.type_id);
        }
        for (type_id, rollup) in &self.timestamp_rollups {
            debug_assert_eq!(*type_id, rollup.type_id);
        }
        for ((type_id, prop_key), rollup) in &self.property_rollups {
            debug_assert_eq!(*type_id, rollup.type_id);
            debug_assert_eq!(prop_key, &rollup.prop_key);
        }
        for (index_id, rollup) in &self.equality_index_rollups {
            debug_assert_eq!(*index_id, rollup.index_id);
            debug_assert!(
                !rollup.prop_key.is_empty(),
                "equality rollup for type {} must have property key",
                rollup.type_id
            );
        }
        for (index_id, rollup) in &self.range_index_rollups {
            debug_assert_eq!(*index_id, rollup.index_id);
            debug_assert!(
                !rollup.prop_key.is_empty(),
                "range rollup for type {} must have property key",
                rollup.type_id
            );
            match rollup.domain {
                SecondaryIndexRangeDomain::Int
                | SecondaryIndexRangeDomain::UInt
                | SecondaryIndexRangeDomain::Float => {}
            }
        }
        for ((direction, edge_type_id), rollup) in &self.adjacency_rollups {
            debug_assert_eq!(*direction, rollup.direction);
            debug_assert_eq!(*edge_type_id, rollup.edge_type_id);
        }
    }
}

impl EqualityIndexRollupStats {
    pub(crate) fn estimate_segment_hashes(
        &self,
        segment_id: u64,
        value_hashes: &[u64],
    ) -> Option<PlannerStatsValueEstimate> {
        let segment = self.segment_rollups.get(&segment_id)?;
        Some(segment.estimate_hashes(value_hashes))
    }
}

impl EqualitySegmentRollupStats {
    fn from_stats(stats: &EqualityIndexPlannerStats) -> Self {
        let mut top_value_hashes = BTreeMap::new();
        let mut top_value_total = 0u64;
        for frequency in &stats.top_value_hashes {
            top_value_total = top_value_total.saturating_add(frequency.count);
            top_value_hashes.insert(frequency.value_hash, frequency.count);
        }
        Self {
            total_postings: stats.total_postings,
            value_group_count: stats.value_group_count,
            top_value_hashes,
            top_value_total,
        }
    }

    fn estimate_hashes(&self, value_hashes: &[u64]) -> PlannerStatsValueEstimate {
        let mut seen = BTreeSet::new();
        let mut total = 0u64;
        let mut exact = true;
        let mut residual_probe_count = 0u64;
        for value_hash in value_hashes {
            if !seen.insert(*value_hash) {
                continue;
            }
            if let Some(count) = self.top_value_hashes.get(value_hash) {
                total = total.saturating_add(*count);
            } else if self.residual_group_count() > 0 {
                residual_probe_count = residual_probe_count.saturating_add(1);
                exact = false;
            }
        }

        if residual_probe_count > 0 {
            let residual_postings = self.residual_postings();
            let residual_estimate = self
                .residual_group_estimate()
                .saturating_mul(residual_probe_count)
                .min(residual_postings);
            total = total.saturating_add(residual_estimate);
        }

        PlannerStatsValueEstimate {
            count: total,
            exact,
        }
    }

    fn residual_postings(&self) -> u64 {
        self.total_postings.saturating_sub(self.top_value_total)
    }

    fn residual_group_count(&self) -> u64 {
        self.value_group_count
            .saturating_sub(self.top_value_hashes.len() as u64)
    }

    fn residual_group_estimate(&self) -> u64 {
        let residual_groups = self.residual_group_count();
        if residual_groups == 0 {
            return 0;
        }

        let residual_postings = self.residual_postings();
        let mut estimate = residual_postings / residual_groups;
        if !residual_postings.is_multiple_of(residual_groups) {
            estimate = estimate.saturating_add(1);
        }
        estimate
    }
}

struct PlannerStatsSegmentSnapshot<'a> {
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
    availability: &'a PlannerStatsAvailability,
}

#[derive(Clone)]
struct CoverageBuilder {
    all_segment_ids: Arc<[u64]>,
    covered: BTreeSet<u64>,
    mismatched: BTreeSet<u64>,
}

impl CoverageBuilder {
    fn new(all_segment_ids: Arc<[u64]>) -> Self {
        Self {
            all_segment_ids,
            covered: BTreeSet::new(),
            mismatched: BTreeSet::new(),
        }
    }

    fn mark_covered(&mut self, segment_id: u64) {
        self.mismatched.remove(&segment_id);
        self.covered.insert(segment_id);
    }

    fn mark_mismatched(&mut self, segment_id: u64) {
        self.covered.remove(&segment_id);
        self.mismatched.insert(segment_id);
    }

    fn finish(self) -> PlannerStatsFamilyCoverage {
        let uncovered = self
            .all_segment_ids
            .iter()
            .copied()
            .filter(|segment_id| {
                !self.covered.contains(segment_id) && !self.mismatched.contains(segment_id)
            })
            .collect();
        PlannerStatsFamilyCoverage {
            covered_segment_ids: self.covered.into_iter().collect(),
            uncovered_segment_ids: uncovered,
            mismatched_segment_ids: self.mismatched.into_iter().collect(),
        }
    }
}

struct PropertyRollupBuilder {
    stats: PropertyRollupStats,
    coverage: CoverageBuilder,
}

struct EqualityRollupBuilder {
    stats: EqualityIndexRollupStats,
    coverage: CoverageBuilder,
}

struct RangeRollupBuilder {
    stats: RangeIndexRollupStats,
    coverage: CoverageBuilder,
}

struct AdjacencyRollupBuilder {
    stats: AdjacencyRollupStats,
    coverage: CoverageBuilder,
}

#[derive(Clone)]
struct EqualityIndexDeclaration {
    type_id: u32,
    prop_key: String,
}

#[derive(Clone)]
struct RangeIndexDeclaration {
    type_id: u32,
    prop_key: String,
    domain: SecondaryIndexRangeDomain,
}

type DeclaredIndexFingerprintSet = BTreeSet<(u64, u8, u32, String, u8)>;

#[cfg(test)]
fn build_planner_stats_view_from_snapshots(
    generation: u64,
    segments: &[PlannerStatsSegmentSnapshot<'_>],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> PlannerStatsView {
    let runtime_coverage =
        all_available_runtime_coverage_for_snapshots(segments, secondary_indexes);
    build_planner_stats_view_from_snapshots_with_runtime_coverage(
        generation,
        segments,
        secondary_indexes,
        &runtime_coverage,
    )
}

#[cfg(test)]
fn all_available_runtime_coverage_for_snapshots(
    segments: &[PlannerStatsSegmentSnapshot<'_>],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> DeclaredIndexRuntimeCoverage {
    let mut coverage = DeclaredIndexRuntimeCoverage::default();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready || ready_node_property_target(entry).is_none()
        {
            continue;
        }
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
            SecondaryIndexKind::Range { .. } => PlannerStatsDeclaredIndexKind::Range,
        };
        for segment in segments {
            coverage.insert(
                segment.segment_id,
                entry.index_id,
                kind,
                DeclaredIndexRuntimeCoverageState::Available,
            );
        }
    }
    coverage
}

fn build_planner_stats_view_from_snapshots_with_runtime_coverage(
    generation: u64,
    segments: &[PlannerStatsSegmentSnapshot<'_>],
    secondary_indexes: &[SecondaryIndexManifestEntry],
    runtime_coverage: &DeclaredIndexRuntimeCoverage,
) -> PlannerStatsView {
    let all_segment_ids: Arc<[u64]> = segments
        .iter()
        .map(|segment| segment.segment_id)
        .collect::<Vec<_>>()
        .into();
    let mut full_coverage = CoverageBuilder::new(all_segment_ids.clone());
    let mut type_coverage = CoverageBuilder::new(all_segment_ids.clone());
    let mut timestamp_coverage = CoverageBuilder::new(all_segment_ids.clone());
    let mut full_rollup = FullRollupStats::default();
    let mut type_rollups: BTreeMap<u32, TypeRollupStats> = BTreeMap::new();
    let mut timestamp_rollups: BTreeMap<u32, TimestampRollupStats> = BTreeMap::new();
    let mut segment_type_ids: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    let mut segment_timestamp_type_ids: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    let mut property_builders: BTreeMap<(u32, String), PropertyRollupBuilder> = BTreeMap::new();
    let equality_declarations = ready_equality_declarations(secondary_indexes);
    let range_declarations = ready_range_declarations(secondary_indexes);
    let mut equality_builders =
        equality_rollup_builders(&equality_declarations, all_segment_ids.clone());
    let mut range_builders = range_rollup_builders(&range_declarations, all_segment_ids.clone());
    let mut adjacency_builders: BTreeMap<
        (PlannerStatsDirection, Option<u32>),
        AdjacencyRollupBuilder,
    > = BTreeMap::new();
    let mut available_segment_stats = 0usize;
    let mut missing_segment_stats = 0usize;
    let mut unavailable_segment_stats = 0usize;
    let mut complete_property_segment_ids = BTreeSet::new();

    for segment in segments {
        match segment.availability {
            PlannerStatsAvailability::Available(stats) => {
                available_segment_stats += 1;
                full_coverage.mark_covered(segment.segment_id);
                type_coverage.mark_covered(segment.segment_id);
                timestamp_coverage.mark_covered(segment.segment_id);
                full_rollup.node_count = full_rollup.node_count.saturating_add(stats.node_count);
                full_rollup.edge_count = full_rollup.edge_count.saturating_add(stats.edge_count);
                if stats.general_property_stats_complete {
                    complete_property_segment_ids.insert(segment.segment_id);
                }
                segment_type_ids.insert(
                    segment.segment_id,
                    stats
                        .type_stats
                        .iter()
                        .map(|type_stats| type_stats.type_id)
                        .collect(),
                );
                segment_timestamp_type_ids.insert(
                    segment.segment_id,
                    stats
                        .timestamp_stats
                        .iter()
                        .map(|timestamp| timestamp.type_id)
                        .collect(),
                );
                add_type_rollups(&mut type_rollups, stats);
                add_timestamp_rollups(&mut timestamp_rollups, segment.segment_id, stats);
                add_property_rollups(
                    &mut property_builders,
                    all_segment_ids.clone(),
                    segment.segment_id,
                    stats,
                );
                let declared_fingerprints = declared_index_fingerprint_set(stats);
                add_equality_rollups(
                    &mut equality_builders,
                    segment.segment_id,
                    stats,
                    &equality_declarations,
                    &declared_fingerprints,
                    runtime_coverage,
                );
                add_range_rollups(
                    &mut range_builders,
                    segment.segment_id,
                    stats,
                    &range_declarations,
                    &declared_fingerprints,
                    runtime_coverage,
                );
                add_adjacency_rollups(
                    &mut adjacency_builders,
                    all_segment_ids.clone(),
                    segment.segment_id,
                    stats,
                );
            }
            PlannerStatsAvailability::Missing => {
                missing_segment_stats += 1;
                let _ = (segment.node_count, segment.edge_count);
            }
            PlannerStatsAvailability::Unavailable { .. } => {
                unavailable_segment_stats += 1;
                let _ = (segment.node_count, segment.edge_count);
            }
        }
    }

    full_rollup.coverage = full_coverage.finish();
    let type_coverage = type_coverage.finish();
    let timestamp_coverage = timestamp_coverage.finish();
    finalize_timestamp_rollup_coverage(
        &mut timestamp_rollups,
        &type_rollups,
        all_segment_ids.clone(),
        &segment_type_ids,
        &segment_timestamp_type_ids,
    );

    let property_rollups = property_builders
        .into_iter()
        .map(|(key, mut builder)| {
            for segment_id in &complete_property_segment_ids {
                builder.coverage.mark_covered(*segment_id);
            }
            builder.stats.coverage = builder.coverage.finish();
            (key, builder.stats)
        })
        .collect();

    let equality_index_rollups = equality_builders
        .into_iter()
        .map(|(index_id, mut builder)| {
            builder.stats.coverage = builder.coverage.finish();
            (index_id, builder.stats)
        })
        .collect();

    let range_index_rollups = range_builders
        .into_iter()
        .map(|(index_id, mut builder)| {
            builder.stats.coverage = builder.coverage.finish();
            (index_id, builder.stats)
        })
        .collect();

    let adjacency_rollups = adjacency_builders
        .into_iter()
        .map(|(key, mut builder)| {
            builder.stats.coverage = builder.coverage.finish();
            (key, builder.stats)
        })
        .collect();

    let view = PlannerStatsView {
        generation,
        segment_count: segments.len(),
        available_segment_stats,
        missing_segment_stats,
        unavailable_segment_stats,
        full_rollup,
        type_coverage,
        timestamp_coverage,
        property_rollups,
        type_rollups,
        timestamp_rollups,
        equality_index_rollups,
        range_index_rollups,
        adjacency_rollups,
        segment_stale_risks: BTreeMap::new(),
    };
    view.validate_rollup_shape();
    view
}

const STALE_RISK_MIN_SAMPLE_SIZE: usize = 8;
const STALE_RISK_HIGH_OVERLAP_PERCENT: usize = 25;
const STALE_RISK_MEDIUM_OVERLAP_PERCENT: usize = 5;
const STALE_RISK_HIGH_RAW_TO_VISIBLE_BPS: u128 = 13_334;
const STALE_RISK_MEDIUM_RAW_TO_VISIBLE_BPS: u128 = 10_526;
const STALE_RISK_MEDIUM_ESTIMATED_STALE_NODES: u64 = 1024;

fn segment_stale_risks_from_readers(
    segments: &[Arc<SegmentReader>],
) -> BTreeMap<u64, StalePostingRisk> {
    let mut risks = BTreeMap::new();
    let mut newer_sample_ids = BTreeSet::new();
    let mut newer_sample_tombstone_ids = BTreeSet::new();
    let mut unknown_newer_source = false;

    for segment in segments {
        let risk = match segment.planner_stats_availability() {
            PlannerStatsAvailability::Available(stats) => {
                let sample_overlap = stats
                    .node_id_sample
                    .iter()
                    .filter(|node_id| newer_sample_ids.contains(*node_id))
                    .count();
                let newer_tombstone_hits = stats
                    .node_id_sample
                    .iter()
                    .filter(|node_id| newer_sample_tombstone_ids.contains(*node_id))
                    .count();
                classify_sample_stale_risk(
                    stats.node_count,
                    stats.node_id_sample.len(),
                    sample_overlap,
                    newer_tombstone_hits,
                    unknown_newer_source,
                )
            }
            PlannerStatsAvailability::Missing | PlannerStatsAvailability::Unavailable { .. } => {
                StalePostingRisk::Unknown
            }
        };
        risks.insert(segment.segment_id, risk);

        match segment.planner_stats_availability() {
            PlannerStatsAvailability::Available(stats) => {
                newer_sample_ids.extend(stats.node_id_sample.iter().copied());
            }
            PlannerStatsAvailability::Missing | PlannerStatsAvailability::Unavailable { .. } => {
                unknown_newer_source = true;
            }
        }
        newer_sample_tombstone_ids.extend(segment.deleted_node_id_iter());
    }

    risks
}

fn classify_sample_stale_risk(
    node_count: u64,
    sample_len: usize,
    sample_overlap: usize,
    newer_tombstone_hits: usize,
    unknown_newer_source: bool,
) -> StalePostingRisk {
    if node_count == 0 {
        return StalePostingRisk::Low;
    }
    let required_sample =
        STALE_RISK_MIN_SAMPLE_SIZE.min(usize::try_from(node_count).unwrap_or(usize::MAX));
    if sample_len < required_sample {
        return StalePostingRisk::Unknown;
    }
    let stale_hits = sample_overlap.saturating_add(newer_tombstone_hits);
    if stale_hits == 0 {
        return if unknown_newer_source {
            StalePostingRisk::Unknown
        } else {
            StalePostingRisk::Low
        };
    }
    let stale_percent = stale_hits.saturating_mul(100) / sample_len.max(1);
    let survivor_ratio_risk = stale_risk_from_survivor_ratio(node_count, sample_len, stale_hits);
    if stale_percent >= STALE_RISK_HIGH_OVERLAP_PERCENT
        || survivor_ratio_risk == StalePostingRisk::High
    {
        StalePostingRisk::High
    } else if stale_percent >= STALE_RISK_MEDIUM_OVERLAP_PERCENT
        || survivor_ratio_risk == StalePostingRisk::Medium
        || newer_tombstone_hits > 0
    {
        StalePostingRisk::Medium
    } else {
        StalePostingRisk::Low
    }
}

fn stale_risk_from_survivor_ratio(
    node_count: u64,
    sample_len: usize,
    stale_hits: usize,
) -> StalePostingRisk {
    if node_count == 0 || stale_hits == 0 || sample_len == 0 {
        return StalePostingRisk::Low;
    }
    let estimated_stale = (node_count as u128)
        .saturating_mul(stale_hits as u128)
        .div_ceil(sample_len as u128)
        .min(node_count as u128) as u64;
    let estimated_visible = node_count.saturating_sub(estimated_stale);
    if estimated_visible == 0 {
        return StalePostingRisk::High;
    }
    let raw_to_visible_bps = (node_count as u128)
        .saturating_mul(10_000)
        .div_ceil(estimated_visible as u128);
    if raw_to_visible_bps >= STALE_RISK_HIGH_RAW_TO_VISIBLE_BPS {
        StalePostingRisk::High
    } else if raw_to_visible_bps >= STALE_RISK_MEDIUM_RAW_TO_VISIBLE_BPS
        || estimated_stale >= STALE_RISK_MEDIUM_ESTIMATED_STALE_NODES
    {
        StalePostingRisk::Medium
    } else {
        StalePostingRisk::Low
    }
}

fn ready_equality_declarations(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> BTreeMap<u64, EqualityIndexDeclaration> {
    let mut declarations = BTreeMap::new();
    for entry in secondary_indexes {
        let Some((type_id, prop_key)) = ready_node_property_target(entry) else {
            continue;
        };
        if !matches!(entry.kind, SecondaryIndexKind::Equality) {
            continue;
        }
        declarations.insert(
            entry.index_id,
            EqualityIndexDeclaration { type_id, prop_key },
        );
    }
    declarations
}

fn ready_range_declarations(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> BTreeMap<u64, RangeIndexDeclaration> {
    let mut declarations = BTreeMap::new();
    for entry in secondary_indexes {
        let Some((type_id, prop_key)) = ready_node_property_target(entry) else {
            continue;
        };
        let SecondaryIndexKind::Range { domain } = entry.kind else {
            continue;
        };
        declarations.insert(
            entry.index_id,
            RangeIndexDeclaration {
                type_id,
                prop_key,
                domain,
            },
        );
    }
    declarations
}

fn equality_rollup_builders(
    declarations: &BTreeMap<u64, EqualityIndexDeclaration>,
    all_segment_ids: Arc<[u64]>,
) -> BTreeMap<u64, EqualityRollupBuilder> {
    let mut builders = BTreeMap::new();
    for (index_id, declaration) in declarations {
        builders.insert(
            *index_id,
            EqualityRollupBuilder {
                stats: EqualityIndexRollupStats {
                    index_id: *index_id,
                    type_id: declaration.type_id,
                    prop_key: declaration.prop_key.clone(),
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            },
        );
    }
    builders
}

fn range_rollup_builders(
    declarations: &BTreeMap<u64, RangeIndexDeclaration>,
    all_segment_ids: Arc<[u64]>,
) -> BTreeMap<u64, RangeRollupBuilder> {
    let mut builders = BTreeMap::new();
    for (index_id, declaration) in declarations {
        builders.insert(
            *index_id,
            RangeRollupBuilder {
                stats: RangeIndexRollupStats {
                    index_id: *index_id,
                    type_id: declaration.type_id,
                    prop_key: declaration.prop_key.clone(),
                    domain: declaration.domain,
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            },
        );
    }
    builders
}

fn ready_node_property_target(entry: &SecondaryIndexManifestEntry) -> Option<(u32, String)> {
    if entry.state != crate::types::SecondaryIndexState::Ready {
        return None;
    }
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { type_id, prop_key } => {
            Some((*type_id, prop_key.clone()))
        }
    }
}

fn add_type_rollups(
    type_rollups: &mut BTreeMap<u32, TypeRollupStats>,
    stats: &SegmentPlannerStatsV1,
) {
    for type_stats in &stats.type_stats {
        let rollup = type_rollups
            .entry(type_stats.type_id)
            .or_insert_with(|| TypeRollupStats {
                type_id: type_stats.type_id,
                ..Default::default()
            });
        rollup.node_count = rollup.node_count.saturating_add(type_stats.node_count);
        rollup.min_node_id = min_option(rollup.min_node_id, type_stats.min_node_id);
        rollup.max_node_id = max_option(rollup.max_node_id, type_stats.max_node_id);
        rollup.min_updated_at_ms =
            min_option(rollup.min_updated_at_ms, type_stats.min_updated_at_ms);
        rollup.max_updated_at_ms =
            max_option(rollup.max_updated_at_ms, type_stats.max_updated_at_ms);
    }
}

fn add_timestamp_rollups(
    timestamp_rollups: &mut BTreeMap<u32, TimestampRollupStats>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
) {
    for timestamp in &stats.timestamp_stats {
        let rollup = timestamp_rollups
            .entry(timestamp.type_id)
            .or_insert_with(|| TimestampRollupStats {
                type_id: timestamp.type_id,
                ..Default::default()
            });
        rollup.count = rollup.count.saturating_add(timestamp.count);
        rollup.min_ms = min_option(rollup.min_ms, Some(timestamp.min_ms));
        rollup.max_ms = max_option(rollup.max_ms, Some(timestamp.max_ms));
        rollup.segment_rollups.insert(
            segment_id,
            TimestampSegmentRollupStats {
                count: timestamp.count,
                min_ms: Some(timestamp.min_ms),
                max_ms: Some(timestamp.max_ms),
                buckets: timestamp.buckets.clone(),
            },
        );
    }
}

fn finalize_timestamp_rollup_coverage(
    timestamp_rollups: &mut BTreeMap<u32, TimestampRollupStats>,
    type_rollups: &BTreeMap<u32, TypeRollupStats>,
    all_segment_ids: Arc<[u64]>,
    segment_type_ids: &BTreeMap<u64, BTreeSet<u32>>,
    segment_timestamp_type_ids: &BTreeMap<u64, BTreeSet<u32>>,
) {
    let timestamp_type_ids: Vec<u32> = type_rollups
        .keys()
        .chain(timestamp_rollups.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for type_id in timestamp_type_ids {
        let mut coverage = CoverageBuilder::new(all_segment_ids.clone());
        for segment_id in all_segment_ids.iter().copied() {
            let Some(segment_types) = segment_type_ids.get(&segment_id) else {
                continue;
            };
            if !segment_types.contains(&type_id) {
                coverage.mark_covered(segment_id);
                continue;
            }
            if segment_timestamp_type_ids
                .get(&segment_id)
                .is_some_and(|timestamps| timestamps.contains(&type_id))
            {
                coverage.mark_covered(segment_id);
            } else {
                coverage.mark_mismatched(segment_id);
            }
        }
        let rollup = timestamp_rollups
            .entry(type_id)
            .or_insert_with(|| TimestampRollupStats {
                type_id,
                ..Default::default()
            });
        rollup.coverage = coverage.finish();
    }
}

fn add_property_rollups(
    property_builders: &mut BTreeMap<(u32, String), PropertyRollupBuilder>,
    all_segment_ids: Arc<[u64]>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
) {
    for property in &stats.property_stats {
        let key = (property.type_id, property.prop_key.clone());
        let builder = property_builders
            .entry(key)
            .or_insert_with(|| PropertyRollupBuilder {
                stats: PropertyRollupStats {
                    type_id: property.type_id,
                    prop_key: property.prop_key.clone(),
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            });
        builder.coverage.mark_covered(segment_id);
        builder.stats.present_count = builder
            .stats
            .present_count
            .saturating_add(property.present_count);
        builder.stats.null_count = builder.stats.null_count.saturating_add(property.null_count);
        for frequency in &property.top_values {
            saturating_add_map_value(
                &mut builder.stats.top_values,
                frequency.value_hash,
                frequency.count,
            );
        }
    }
}

fn add_equality_rollups(
    equality_builders: &mut BTreeMap<u64, EqualityRollupBuilder>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
    declarations: &BTreeMap<u64, EqualityIndexDeclaration>,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
    runtime_coverage: &DeclaredIndexRuntimeCoverage,
) {
    let mut seen = BTreeSet::new();
    for index_stats in &stats.equality_index_stats {
        if !seen.insert(index_stats.index_id) {
            continue;
        };
        let Some(builder) = equality_builders.get_mut(&index_stats.index_id) else {
            continue;
        };
        let Some(declaration) = declarations.get(&index_stats.index_id) else {
            continue;
        };
        if !index_stats.sidecar_present_at_build
            || !declared_equality_block_matches(index_stats, declaration, declared_fingerprints)
            || !runtime_coverage.is_available(
                segment_id,
                index_stats.index_id,
                PlannerStatsDeclaredIndexKind::Equality,
            )
        {
            builder.coverage.mark_mismatched(segment_id);
            continue;
        }
        builder.coverage.mark_covered(segment_id);
        builder.stats.total_postings = builder
            .stats
            .total_postings
            .saturating_add(index_stats.total_postings);
        builder.stats.value_group_count = builder
            .stats
            .value_group_count
            .saturating_add(index_stats.value_group_count);
        builder.stats.max_group_postings = builder
            .stats
            .max_group_postings
            .max(index_stats.max_group_postings);
        for frequency in &index_stats.top_value_hashes {
            saturating_add_map_value(
                &mut builder.stats.top_value_hashes,
                frequency.value_hash,
                frequency.count,
            );
        }
        builder.stats.segment_rollups.insert(
            segment_id,
            EqualitySegmentRollupStats::from_stats(index_stats),
        );
    }
}

fn add_range_rollups(
    range_builders: &mut BTreeMap<u64, RangeRollupBuilder>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
    declarations: &BTreeMap<u64, RangeIndexDeclaration>,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
    runtime_coverage: &DeclaredIndexRuntimeCoverage,
) {
    let mut seen = BTreeSet::new();
    for index_stats in &stats.range_index_stats {
        if !seen.insert(index_stats.index_id) {
            continue;
        };
        let Some(builder) = range_builders.get_mut(&index_stats.index_id) else {
            continue;
        };
        let Some(declaration) = declarations.get(&index_stats.index_id) else {
            continue;
        };
        if !index_stats.sidecar_present_at_build
            || !declared_range_block_matches(index_stats, declaration, declared_fingerprints)
            || !runtime_coverage.is_available(
                segment_id,
                index_stats.index_id,
                PlannerStatsDeclaredIndexKind::Range,
            )
        {
            builder.coverage.mark_mismatched(segment_id);
            continue;
        }
        builder.coverage.mark_covered(segment_id);
        builder.stats.total_entries = builder
            .stats
            .total_entries
            .saturating_add(index_stats.total_entries);
        builder.stats.min_encoded = min_option(builder.stats.min_encoded, index_stats.min_encoded);
        builder.stats.max_encoded = max_option(builder.stats.max_encoded, index_stats.max_encoded);
        builder.stats.segment_rollups.insert(
            segment_id,
            RangeIndexSegmentRollupStats {
                total_entries: index_stats.total_entries,
                min_encoded: index_stats.min_encoded,
                max_encoded: index_stats.max_encoded,
                buckets: index_stats.buckets.clone(),
            },
        );
    }
}

fn add_adjacency_rollups(
    adjacency_builders: &mut BTreeMap<(PlannerStatsDirection, Option<u32>), AdjacencyRollupBuilder>,
    all_segment_ids: Arc<[u64]>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
) {
    for adjacency in &stats.adjacency_stats {
        let key = (adjacency.direction, adjacency.edge_type_id);
        let builder = adjacency_builders
            .entry(key)
            .or_insert_with(|| AdjacencyRollupBuilder {
                stats: AdjacencyRollupStats {
                    direction: adjacency.direction,
                    edge_type_id: adjacency.edge_type_id,
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            });
        builder.coverage.mark_covered(segment_id);
        builder.stats.source_node_count = builder
            .stats
            .source_node_count
            .saturating_add(adjacency.source_node_count);
        builder.stats.total_edges = builder
            .stats
            .total_edges
            .saturating_add(adjacency.total_edges);
        builder.stats.max_fanout = builder.stats.max_fanout.max(adjacency.max_fanout);
        builder.stats.p99_fanout = builder.stats.p99_fanout.max(adjacency.p99_fanout);
        merge_adjacency_top_hubs(&mut builder.stats.top_hubs, &adjacency.top_hubs);
    }
}

fn merge_adjacency_top_hubs(
    current: &mut Vec<NodeFanoutFrequency>,
    incoming: &[NodeFanoutFrequency],
) {
    if incoming.is_empty() {
        return;
    }
    let mut counts = BTreeMap::<u64, u32>::new();
    for hub in current.iter().chain(incoming.iter()) {
        let entry = counts.entry(hub.node_id).or_default();
        *entry = entry.saturating_add(hub.count);
    }
    let mut merged: Vec<_> = counts
        .into_iter()
        .map(|(node_id, count)| NodeFanoutFrequency { node_id, count })
        .collect();
    merged.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    merged.truncate(PLANNER_STATS_TOP_HUBS_PER_EDGE_TYPE);
    *current = merged;
}

fn declared_equality_block_matches(
    block: &EqualityIndexPlannerStats,
    declaration: &EqualityIndexDeclaration,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
) -> bool {
    if block.type_id != declaration.type_id || block.prop_key != declaration.prop_key {
        return false;
    }
    declared_fingerprints.contains(&declared_index_key(
        block.index_id,
        PlannerStatsDeclaredIndexKind::Equality,
        declaration.type_id,
        &declaration.prop_key,
        None,
    ))
}

fn declared_range_block_matches(
    block: &RangeIndexPlannerStats,
    declaration: &RangeIndexDeclaration,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
) -> bool {
    if block.type_id != declaration.type_id
        || block.prop_key != declaration.prop_key
        || block.domain != declaration.domain
    {
        return false;
    }
    declared_fingerprints.contains(&declared_index_key(
        block.index_id,
        PlannerStatsDeclaredIndexKind::Range,
        declaration.type_id,
        &declaration.prop_key,
        Some(declaration.domain),
    ))
}

fn declared_index_fingerprint_set(stats: &SegmentPlannerStatsV1) -> DeclaredIndexFingerprintSet {
    stats
        .declared_indexes
        .iter()
        .map(|declared| {
            declared_index_key(
                declared.index_id,
                declared.kind,
                declared.type_id,
                &declared.prop_key,
                declared.range_domain,
            )
        })
        .collect()
}

fn declared_index_key(
    index_id: u64,
    kind: PlannerStatsDeclaredIndexKind,
    type_id: u32,
    prop_key: &str,
    range_domain: Option<SecondaryIndexRangeDomain>,
) -> (u64, u8, u32, String, u8) {
    (
        index_id,
        declared_index_kind_rank(kind),
        type_id,
        prop_key.to_string(),
        range_domain_rank(range_domain),
    )
}

fn min_option<T: Ord + Copy>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_option<T: Ord + Copy>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn saturating_add_map_value(map: &mut BTreeMap<u64, u64>, key: u64, value: u64) {
    map.entry(key)
        .and_modify(|count| *count = count.saturating_add(value))
        .or_insert(value);
}

fn invalid_u64_bounds(lower: Option<(u64, bool)>, upper: Option<(u64, bool)>) -> bool {
    let (Some((lower_value, lower_inclusive)), Some((upper_value, upper_inclusive))) =
        (lower, upper)
    else {
        return false;
    };
    lower_value > upper_value
        || (lower_value == upper_value && (!lower_inclusive || !upper_inclusive))
}

fn u64_bucket_below_lower(bucket_upper: u64, lower: Option<(u64, bool)>) -> bool {
    lower.is_some_and(|(lower_value, inclusive)| {
        bucket_upper < lower_value || (!inclusive && bucket_upper <= lower_value)
    })
}

fn u64_bucket_above_upper(bucket_lower_floor: Option<u64>, upper: Option<(u64, bool)>) -> bool {
    let (Some(bucket_lower_floor), Some((upper_value, inclusive))) = (bucket_lower_floor, upper)
    else {
        return false;
    };
    bucket_lower_floor > upper_value || (!inclusive && bucket_lower_floor >= upper_value)
}

fn u64_bucket_fully_inside(
    bucket_lower_floor: Option<u64>,
    bucket_upper: Option<u64>,
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> bool {
    let lower_inside = match (bucket_lower_floor, lower) {
        (_, None) => true,
        (Some(bucket_lower_floor), Some((lower_value, inclusive))) => {
            bucket_lower_floor > lower_value || (inclusive && bucket_lower_floor >= lower_value)
        }
        (None, Some(_)) => false,
    };
    let upper_inside = match (bucket_upper, upper) {
        (_, None) => true,
        (Some(bucket_upper), Some((upper_value, inclusive))) => {
            bucket_upper < upper_value || (inclusive && bucket_upper <= upper_value)
        }
        (None, Some(_)) => false,
    };
    lower_inside && upper_inside
}

fn estimate_u64_histogram(
    total_count: u64,
    min_value: Option<u64>,
    max_value: Option<u64>,
    buckets: impl Iterator<Item = (u64, u64)>,
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> PlannerStatsValueEstimate {
    if total_count == 0 || invalid_u64_bounds(lower, upper) {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if max_value.is_some_and(|max_value| u64_bucket_below_lower(max_value, lower))
        || min_value.is_some_and(|min_value| u64_bucket_above_upper(Some(min_value), upper))
    {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if min_value.is_some()
        && max_value.is_some()
        && u64_bucket_fully_inside(min_value, max_value, lower, upper)
    {
        return PlannerStatsValueEstimate {
            count: total_count,
            exact: true,
        };
    }

    let mut estimated = 0u64;
    let mut exact = true;
    let mut previous_upper = min_value;
    let mut saw_bucket = false;
    for (bucket_upper, bucket_count) in buckets {
        saw_bucket = true;
        if u64_bucket_below_lower(bucket_upper, lower)
            || u64_bucket_above_upper(previous_upper, upper)
        {
            previous_upper = Some(bucket_upper);
            continue;
        }
        estimated = estimated.saturating_add(bucket_count);
        if !u64_bucket_fully_inside(previous_upper, Some(bucket_upper), lower, upper) {
            exact = false;
        }
        previous_upper = Some(bucket_upper);
    }

    if !saw_bucket {
        return PlannerStatsValueEstimate {
            count: total_count,
            exact: false,
        };
    }

    PlannerStatsValueEstimate {
        count: estimated.min(total_count),
        exact,
    }
}

fn estimate_i64_histogram(
    total_count: u64,
    min_value: Option<i64>,
    max_value: Option<i64>,
    buckets: impl Iterator<Item = (i64, u64)>,
    lower: i64,
    upper: i64,
) -> PlannerStatsValueEstimate {
    if total_count == 0 || lower > upper {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if max_value.is_some_and(|max_value| max_value < lower)
        || min_value.is_some_and(|min_value| min_value > upper)
    {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if min_value.is_some_and(|min_value| min_value >= lower)
        && max_value.is_some_and(|max_value| max_value <= upper)
    {
        return PlannerStatsValueEstimate {
            count: total_count,
            exact: true,
        };
    }

    let mut estimated = 0u64;
    let mut exact = true;
    let mut previous_upper = min_value;
    let mut saw_bucket = false;
    for (bucket_upper, bucket_count) in buckets {
        saw_bucket = true;
        if bucket_upper < lower
            || previous_upper.is_some_and(|previous_upper| previous_upper > upper)
        {
            previous_upper = Some(bucket_upper);
            continue;
        }
        estimated = estimated.saturating_add(bucket_count);
        if !(previous_upper.is_some_and(|previous_upper| previous_upper >= lower)
            && bucket_upper <= upper)
        {
            exact = false;
        }
        previous_upper = Some(bucket_upper);
    }

    if !saw_bucket {
        return PlannerStatsValueEstimate {
            count: total_count,
            exact: false,
        };
    }

    PlannerStatsValueEstimate {
        count: estimated.min(total_count),
        exact,
    }
}

#[derive(Default)]
struct TypeAccumulator {
    node_count: u64,
    min_node_id: Option<u64>,
    max_node_id: Option<u64>,
    min_updated_at_ms: Option<i64>,
    max_updated_at_ms: Option<i64>,
    updated_values: Vec<i64>,
}

#[derive(Clone)]
struct PropertyAccumulator {
    type_id: u32,
    prop_key: String,
    tracked_reason: PropertyStatsTrackedReason,
    present_count: u64,
    null_count: u64,
    value_kind_counts: ValueKindCounts,
    value_counts: BTreeMap<u64, u64>,
    distinct_overflow: bool,
    int_values: Vec<u64>,
    uint_values: Vec<u64>,
    float_values: Vec<u64>,
}

impl PropertyAccumulator {
    fn new(type_id: u32, prop_key: String, tracked_reason: PropertyStatsTrackedReason) -> Self {
        Self {
            type_id,
            prop_key,
            tracked_reason,
            present_count: 0,
            null_count: 0,
            value_kind_counts: ValueKindCounts::default(),
            value_counts: BTreeMap::new(),
            distinct_overflow: false,
            int_values: Vec::new(),
            uint_values: Vec::new(),
            float_values: Vec::new(),
        }
    }

    fn observe(&mut self, value: &PropValue) {
        self.present_count += 1;
        self.value_kind_counts.observe(value);
        if matches!(value, PropValue::Null) {
            self.null_count += 1;
        }

        let value_hash = hash_prop_value(value);
        if let Some(count) = self.value_counts.get_mut(&value_hash) {
            *count += 1;
        } else if self.value_counts.len() < PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES {
            self.value_counts.insert(value_hash, 1);
        } else {
            self.distinct_overflow = true;
        }

        match value {
            PropValue::Int(_) => {
                if let Some(encoded) =
                    encode_range_prop_value(SecondaryIndexRangeDomain::Int, value)
                {
                    push_capped_numeric(&mut self.int_values, encoded);
                }
            }
            PropValue::UInt(_) => {
                if let Some(encoded) =
                    encode_range_prop_value(SecondaryIndexRangeDomain::UInt, value)
                {
                    push_capped_numeric(&mut self.uint_values, encoded);
                }
            }
            PropValue::Float(_) => {
                if let Some(encoded) =
                    encode_range_prop_value(SecondaryIndexRangeDomain::Float, value)
                {
                    push_capped_numeric(&mut self.float_values, encoded);
                }
            }
            PropValue::Null
            | PropValue::Bool(_)
            | PropValue::String(_)
            | PropValue::Bytes(_)
            | PropValue::Array(_)
            | PropValue::Map(_) => {}
        }
    }

    fn into_stats(mut self) -> PropertyPlannerStats {
        self.int_values.sort_unstable();
        self.uint_values.sort_unstable();
        self.float_values.sort_unstable();
        let mut numeric_summaries = Vec::new();
        if !self.int_values.is_empty() {
            numeric_summaries.push(range_summary_from_values(
                SecondaryIndexRangeDomain::Int,
                &self.int_values,
            ));
        }
        if !self.uint_values.is_empty() {
            numeric_summaries.push(range_summary_from_values(
                SecondaryIndexRangeDomain::UInt,
                &self.uint_values,
            ));
        }
        if !self.float_values.is_empty() {
            numeric_summaries.push(range_summary_from_values(
                SecondaryIndexRangeDomain::Float,
                &self.float_values,
            ));
        }

        let exact_distinct_count =
            (!self.distinct_overflow).then_some(self.value_counts.len() as u64);
        let distinct_lower_bound = self
            .distinct_overflow
            .then_some(self.value_counts.len() as u64);
        let top_values =
            top_value_frequencies(self.value_counts, PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY);
        PropertyPlannerStats {
            type_id: self.type_id,
            prop_key: self.prop_key,
            tracked_reason: self.tracked_reason,
            present_count: self.present_count,
            null_count: self.null_count,
            value_kind_counts: self.value_kind_counts,
            exact_distinct_count,
            distinct_lower_bound,
            top_values,
            numeric_summaries,
        }
    }
}

struct PropertyKeyCandidateTracker {
    cap: usize,
    estimated_counts: BTreeMap<String, u64>,
}

impl PropertyKeyCandidateTracker {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            estimated_counts: BTreeMap::new(),
        }
    }

    fn observe(&mut self, key: &str) {
        if self.cap == 0 {
            return;
        }
        if let Some(count) = self.estimated_counts.get_mut(key) {
            *count = count.saturating_add(1);
            return;
        }
        if self.estimated_counts.len() < self.cap {
            self.estimated_counts.insert(key.to_string(), 1);
            return;
        }
        let Some((evicted_key, evicted_count)) = self.weakest_candidate() else {
            return;
        };
        self.estimated_counts.remove(&evicted_key);
        self.estimated_counts
            .insert(key.to_string(), evicted_count.saturating_add(1));
    }

    fn weakest_candidate(&self) -> Option<(String, u64)> {
        self.estimated_counts
            .iter()
            .min_by(|(left_key, left_count), (right_key, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_key.as_bytes().cmp(left_key.as_bytes()))
            })
            .map(|(key, count)| (key.clone(), *count))
    }

    fn into_keys(self) -> impl Iterator<Item = String> {
        self.estimated_counts.into_keys()
    }
}

impl ValueKindCounts {
    fn observe(&mut self, value: &PropValue) {
        match value {
            PropValue::Null => self.null_count += 1,
            PropValue::Bool(_) => self.bool_count += 1,
            PropValue::Int(_) => self.int_count += 1,
            PropValue::UInt(_) => self.uint_count += 1,
            PropValue::Float(_) => self.float_count += 1,
            PropValue::String(_) => self.string_count += 1,
            PropValue::Bytes(_) => self.bytes_count += 1,
            PropValue::Array(_) => self.array_count += 1,
            PropValue::Map(_) => self.map_count += 1,
        }
    }
}

pub(crate) fn read_planner_stats_sidecar(
    seg_dir: &Path,
    expected_segment_id: u64,
    expected_node_count: u64,
    expected_edge_count: u64,
) -> PlannerStatsAvailability {
    let path = seg_dir.join(PLANNER_STATS_FILENAME);
    match read_planner_stats_file(
        &path,
        expected_segment_id,
        expected_node_count,
        expected_edge_count,
    ) {
        Ok(stats) => PlannerStatsAvailability::Available(Box::new(stats)),
        Err(PlannerStatsReadFailure::Missing) => PlannerStatsAvailability::Missing,
        Err(PlannerStatsReadFailure::Unavailable(reason)) => {
            PlannerStatsAvailability::Unavailable { reason }
        }
    }
}

pub(crate) fn write_flush_planner_stats_sidecar_best_effort(
    seg_dir: &Path,
    segment_id: u64,
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) {
    let result = build_flush_stats(segment_id, seg_dir, nodes, edges, secondary_indexes)
        .and_then(|stats| write_planner_stats_sidecar_atomic(seg_dir, stats).map(|_| ()));
    if result.is_err() {
        cleanup_stats_tmp(seg_dir);
    }
}

pub(crate) fn write_compaction_planner_stats_sidecar_best_effort(
    seg_dir: &Path,
    segment_id: u64,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) {
    let result = build_compaction_stats(
        segment_id,
        seg_dir,
        segments,
        node_metas,
        edge_metas,
        secondary_indexes,
        PlannerStatsBuildPolicy::compaction(),
    )
    .and_then(|stats| write_planner_stats_sidecar_atomic(seg_dir, stats).map(|_| ()));
    if result.is_err() {
        cleanup_stats_tmp(seg_dir);
    }
}

pub(crate) fn write_targeted_secondary_index_planner_stats_sidecar(
    seg_dir: &Path,
    segment: &SegmentReader,
    target_index: &SecondaryIndexManifestEntry,
    ready_secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<PlannerStatsWriteOutcome, EngineError> {
    let policy = PlannerStatsBuildPolicy::targeted_secondary_index_refresh(target_index.index_id);
    if !matches!(
        policy.mode,
        PlannerStatsBuildMode::TargetedSecondaryIndexRefresh { .. }
    ) || policy.allow_general_property_decode
        || policy.general_property_decode_budget_nodes != 0
    {
        return Err(EngineError::InvalidOperation(
            "invalid targeted planner stats refresh policy".into(),
        ));
    }

    if target_index.state != SecondaryIndexState::Ready {
        return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
    }
    let ready_indexes = ready_planner_stats_indexes(ready_secondary_indexes);
    if !ready_indexes
        .iter()
        .any(|entry| planner_stats_declaration_matches(entry, target_index))
    {
        return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
    }

    let target_equality_stats = if matches!(target_index.kind, SecondaryIndexKind::Equality) {
        let mut stats =
            build_equality_index_stats_from_sidecars(seg_dir, std::slice::from_ref(target_index))?;
        let Some(stats) = stats.pop() else {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        };
        if !stats.sidecar_present_at_build {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        }
        Some(stats)
    } else {
        None
    };
    let target_range_stats = if matches!(target_index.kind, SecondaryIndexKind::Range { .. }) {
        let mut stats =
            build_range_index_stats_from_sidecars(seg_dir, std::slice::from_ref(target_index))?;
        let Some(stats) = stats.pop() else {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        };
        if !stats.sidecar_present_at_build {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        }
        Some(stats)
    } else {
        None
    };

    let declared = declared_index_fingerprints(&ready_indexes);
    let declaration_fingerprint = declaration_fingerprint(&declared);
    let mut stats = match read_planner_stats_sidecar(
        seg_dir,
        segment.segment_id,
        segment.node_count(),
        segment.edge_count(),
    ) {
        PlannerStatsAvailability::Available(stats) => {
            let mut stats = *stats;
            retain_current_declared_index_stats(&mut stats, &ready_indexes, target_index.index_id);
            stats
        }
        PlannerStatsAvailability::Missing | PlannerStatsAvailability::Unavailable { .. } => {
            build_minimal_targeted_refresh_stats(segment)?
        }
    };

    stats.build_kind = PlannerStatsBuildKind::SecondaryIndexRefresh;
    stats.built_at_ms = 0;
    stats.declared_indexes = declared;
    stats.declaration_fingerprint = declaration_fingerprint;
    stats.truncated |= !stats.general_property_stats_complete;

    if let Some(equality) = target_equality_stats {
        stats.equality_index_stats.push(equality);
        stats
            .equality_index_stats
            .sort_by_key(|index_stats| index_stats.index_id);
    }
    if let Some(range) = target_range_stats {
        stats.range_index_stats.push(range);
        stats
            .range_index_stats
            .sort_by_key(|index_stats| index_stats.index_id);
    }

    write_planner_stats_sidecar_atomic_cleanup_on_error(seg_dir, stats)
}

pub(crate) fn planner_stats_declaration_fingerprint_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> u64 {
    declaration_fingerprint(&declared_index_fingerprints(std::slice::from_ref(entry)))
}

pub(crate) fn build_flush_stats(
    segment_id: u64,
    seg_dir: &Path,
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let policy = PlannerStatsBuildPolicy::flush();
    let declared = declared_index_fingerprints(secondary_indexes);
    let declared_property_reasons = declared_property_reasons(secondary_indexes);
    let mut type_accs = BTreeMap::new();
    let mut property_candidates = BTreeMap::new();

    let mut sorted_nodes: Vec<&NodeRecord> = nodes.values().collect();
    sorted_nodes.sort_unstable_by_key(|node| node.id);
    for node in &sorted_nodes {
        observe_type(&mut type_accs, node.id, node.type_id, node.updated_at);
        if policy.allow_general_property_decode {
            observe_general_property_candidates(
                &mut property_candidates,
                &declared_property_reasons,
                node.type_id,
                &node.props,
            );
        }
    }
    let mut property_accs =
        seed_property_accumulators(&declared_property_reasons, property_candidates, &type_accs);
    if policy.allow_general_property_decode {
        for node in &sorted_nodes {
            observe_selected_node_properties(&mut property_accs, node.type_id, &node.props);
        }
    }

    let mut sorted_edges: Vec<&EdgeRecord> = edges.values().collect();
    sorted_edges.sort_unstable_by_key(|edge| edge.id);
    let stats = SegmentPlannerStatsV1 {
        format_version: PLANNER_STATS_FORMAT_VERSION,
        segment_id,
        build_kind: PlannerStatsBuildKind::Flush,
        built_at_ms: 0,
        declaration_fingerprint: declaration_fingerprint(&declared),
        declared_indexes: declared,
        node_count: sorted_nodes.len() as u64,
        edge_count: sorted_edges.len() as u64,
        truncated: false,
        general_property_stats_complete: true,
        general_property_sampled_node_count: sorted_nodes.len() as u64,
        general_property_sampled_raw_bytes: 0,
        general_property_budget_exhausted: false,
        type_stats: finalize_type_stats(type_accs),
        timestamp_stats: finalize_timestamp_stats(&sorted_nodes),
        property_stats: finalize_property_stats(property_accs),
        equality_index_stats: build_equality_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        range_index_stats: build_range_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        adjacency_stats: build_adjacency_stats_from_edges(sorted_edges.iter().copied()),
        node_id_sample: node_id_sample(sorted_nodes.iter().map(|node| node.id)),
    };
    Ok(stats)
}

fn build_compaction_stats(
    segment_id: u64,
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
    policy: PlannerStatsBuildPolicy,
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let declared = declared_index_fingerprints(secondary_indexes);
    let declared_property_reasons = declared_property_reasons(secondary_indexes);
    let mut type_accs = BTreeMap::new();
    for meta in node_metas {
        observe_type(&mut type_accs, meta.node_id, meta.type_id, meta.updated_at);
    }

    let mut property_candidates = BTreeMap::new();
    let mut sampled_props = Vec::new();
    let mut sampled_node_count = 0u64;
    let mut sampled_raw_bytes = 0u64;
    let mut budget_exhausted = false;
    if policy.allow_general_property_decode {
        for meta in node_metas {
            if sampled_node_count as usize >= policy.general_property_decode_budget_nodes {
                budget_exhausted = (sampled_node_count as usize) < node_metas.len();
                break;
            }
            let next_bytes = sampled_raw_bytes.saturating_add(meta.data_len as u64);
            if next_bytes as usize > policy.general_property_decode_budget_bytes {
                budget_exhausted = true;
                break;
            }
            let props = decode_node_props_at(
                segments[meta.src_seg_idx].raw_nodes_mmap(),
                meta.src_data_offset,
                meta.node_id,
            )?;
            observe_general_property_candidates(
                &mut property_candidates,
                &declared_property_reasons,
                meta.type_id,
                &props,
            );
            sampled_props.push((meta.type_id, props));
            sampled_node_count += 1;
            sampled_raw_bytes = next_bytes;
        }
    }
    let mut property_accs =
        seed_property_accumulators(&declared_property_reasons, property_candidates, &type_accs);
    for (type_id, props) in &sampled_props {
        observe_selected_node_properties(&mut property_accs, *type_id, props);
    }
    let general_property_stats_complete =
        sampled_node_count == node_metas.len() as u64 && !budget_exhausted;

    let edge_refs = edge_metas.iter().map(EdgeMetaRef::from);
    Ok(SegmentPlannerStatsV1 {
        format_version: PLANNER_STATS_FORMAT_VERSION,
        segment_id,
        build_kind: PlannerStatsBuildKind::Compaction,
        built_at_ms: 0,
        declaration_fingerprint: declaration_fingerprint(&declared),
        declared_indexes: declared,
        node_count: node_metas.len() as u64,
        edge_count: edge_metas.len() as u64,
        truncated: !general_property_stats_complete,
        general_property_stats_complete,
        general_property_sampled_node_count: sampled_node_count,
        general_property_sampled_raw_bytes: sampled_raw_bytes,
        general_property_budget_exhausted: budget_exhausted,
        type_stats: finalize_type_stats(type_accs),
        timestamp_stats: finalize_timestamp_stats_from_meta(node_metas),
        property_stats: finalize_property_stats(property_accs),
        equality_index_stats: build_equality_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        range_index_stats: build_range_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        adjacency_stats: build_adjacency_stats_from_edge_meta(edge_refs),
        node_id_sample: node_id_sample(node_metas.iter().map(|meta| meta.node_id)),
    })
}

fn build_equality_index_stats_from_sidecars(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<EqualityIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if !matches!(entry.kind, SecondaryIndexKind::Equality) {
            continue;
        }
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        let path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_eq_{}.dat", entry.index_id));
        let groups = read_secondary_eq_group_counts(&path)?;
        let sidecar_present_at_build = groups.is_some();
        let groups = groups.unwrap_or_default();
        let mut value_counts = BTreeMap::new();
        let mut total_postings = 0u64;
        let mut max_group_postings = 0u64;
        for (&value_hash, &count) in &groups {
            total_postings += count;
            max_group_postings = max_group_postings.max(count);
            value_counts.insert(value_hash, count);
        }
        result.push(EqualityIndexPlannerStats {
            index_id: entry.index_id,
            type_id: *type_id,
            prop_key: prop_key.clone(),
            total_postings,
            value_group_count: groups.len() as u64,
            max_group_postings,
            top_value_hashes: top_value_frequencies(
                value_counts,
                PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY,
            ),
            sidecar_present_at_build,
        });
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

fn build_range_index_stats_from_sidecars(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<RangeIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        let SecondaryIndexKind::Range { domain } = entry.kind else {
            continue;
        };
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        let path = seg_dir
            .join("secondary_indexes")
            .join(format!("node_prop_range_{}.dat", entry.index_id));
        let encoded_values = read_secondary_range_encoded_values(&path)?;
        let sidecar_present_at_build = encoded_values.is_some();
        let mut encoded_values = encoded_values.unwrap_or_default();
        encoded_values.sort_unstable();
        let min_encoded = encoded_values.first().copied();
        let max_encoded = encoded_values.last().copied();
        let buckets = range_buckets(&encoded_values, PLANNER_STATS_RANGE_BUCKETS);
        result.push(RangeIndexPlannerStats {
            index_id: entry.index_id,
            type_id: *type_id,
            prop_key: prop_key.clone(),
            domain,
            total_entries: encoded_values.len() as u64,
            min_encoded,
            max_encoded,
            buckets,
            sidecar_present_at_build,
        });
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

fn ready_planner_stats_indexes(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Vec<SecondaryIndexManifestEntry> {
    let mut indexes: Vec<_> = secondary_indexes
        .iter()
        .filter(|entry| entry.state == SecondaryIndexState::Ready)
        .cloned()
        .collect();
    indexes.sort_by_key(|entry| entry.index_id);
    indexes
}

fn planner_stats_declaration_matches(
    left: &SecondaryIndexManifestEntry,
    right: &SecondaryIndexManifestEntry,
) -> bool {
    left.index_id == right.index_id
        && left.kind == right.kind
        && left.target == right.target
        && left.state == SecondaryIndexState::Ready
        && right.state == SecondaryIndexState::Ready
}

fn retain_current_declared_index_stats(
    stats: &mut SegmentPlannerStatsV1,
    ready_indexes: &[SecondaryIndexManifestEntry],
    target_index_id: u64,
) {
    stats.equality_index_stats.retain(|block| {
        block.index_id != target_index_id
            && block.sidecar_present_at_build
            && ready_indexes.iter().any(|entry| {
                matches!(entry.kind, SecondaryIndexKind::Equality)
                    && entry.index_id == block.index_id
                    && matches!(
                        &entry.target,
                        SecondaryIndexTarget::NodeProperty { type_id, prop_key }
                            if *type_id == block.type_id && prop_key == &block.prop_key
                    )
            })
    });
    stats.range_index_stats.retain(|block| {
        block.index_id != target_index_id
            && block.sidecar_present_at_build
            && ready_indexes.iter().any(|entry| {
                matches!(entry.kind, SecondaryIndexKind::Range { domain } if domain == block.domain)
                    && entry.index_id == block.index_id
                    && matches!(
                        &entry.target,
                        SecondaryIndexTarget::NodeProperty { type_id, prop_key }
                            if *type_id == block.type_id && prop_key == &block.prop_key
                    )
            })
    });
}

fn build_minimal_targeted_refresh_stats(
    segment: &SegmentReader,
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let mut type_accs = BTreeMap::new();
    let mut timestamp_groups: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    let mut node_ids = Vec::with_capacity(segment.node_meta_count() as usize);
    for index in 0..segment.node_meta_count() as usize {
        let (
            node_id,
            _data_offset,
            _data_len,
            type_id,
            updated_at,
            _weight,
            _key_len,
            _prop_hash_offset,
            _prop_hash_count,
            _last_write_seq,
        ) = segment.node_meta_at(index)?;
        observe_type(&mut type_accs, node_id, type_id, updated_at);
        timestamp_groups
            .entry(type_id)
            .or_default()
            .push(updated_at);
        node_ids.push(node_id);
    }

    let mut edge_refs = Vec::with_capacity(segment.edge_meta_count() as usize);
    for index in 0..segment.edge_meta_count() as usize {
        let (
            _edge_id,
            _data_offset,
            _data_len,
            from,
            to,
            type_id,
            _updated_at,
            _weight,
            _valid_from,
            _valid_to,
            _last_write_seq,
        ) = segment.edge_meta_at(index)?;
        edge_refs.push(EdgeMetaRef { type_id, from, to });
    }

    Ok(SegmentPlannerStatsV1 {
        format_version: PLANNER_STATS_FORMAT_VERSION,
        segment_id: segment.segment_id,
        build_kind: PlannerStatsBuildKind::SecondaryIndexRefresh,
        built_at_ms: 0,
        declaration_fingerprint: 0,
        declared_indexes: Vec::new(),
        node_count: segment.node_count(),
        edge_count: segment.edge_count(),
        truncated: segment.node_count() > 0,
        general_property_stats_complete: false,
        general_property_sampled_node_count: 0,
        general_property_sampled_raw_bytes: 0,
        general_property_budget_exhausted: segment.node_count() > 0,
        type_stats: finalize_type_stats(type_accs),
        timestamp_stats: finalize_timestamp_groups(timestamp_groups),
        property_stats: Vec::new(),
        equality_index_stats: Vec::new(),
        range_index_stats: Vec::new(),
        adjacency_stats: build_adjacency_stats_from_edge_meta(edge_refs.into_iter()),
        node_id_sample: node_id_sample(node_ids.into_iter()),
    })
}

pub(crate) fn write_planner_stats_sidecar_atomic(
    seg_dir: &Path,
    stats: SegmentPlannerStatsV1,
) -> Result<PlannerStatsWriteOutcome, EngineError> {
    let Some(payload) = serialize_stats_with_limits(
        stats,
        PLANNER_STATS_SOFT_SIDECAR_BYTES,
        PLANNER_STATS_HARD_SIDECAR_BYTES,
    )?
    else {
        cleanup_stats_tmp(seg_dir);
        return Ok(PlannerStatsWriteOutcome::SkippedOversize);
    };

    let tmp_path = seg_dir.join(PLANNER_STATS_TMP_FILENAME);
    let final_path = seg_dir.join(PLANNER_STATS_FILENAME);
    let mut file = File::create(&tmp_path)?;
    file.write_all(&payload)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, &final_path)?;
    fsync_dir(seg_dir)?;
    Ok(PlannerStatsWriteOutcome::Written)
}

fn write_planner_stats_sidecar_atomic_cleanup_on_error(
    seg_dir: &Path,
    stats: SegmentPlannerStatsV1,
) -> Result<PlannerStatsWriteOutcome, EngineError> {
    let result = write_planner_stats_sidecar_atomic(seg_dir, stats);
    if result.is_err() {
        cleanup_stats_tmp(seg_dir);
    }
    result
}

fn serialize_stats_with_limits(
    mut stats: SegmentPlannerStatsV1,
    soft_limit: usize,
    hard_limit: usize,
) -> Result<Option<Vec<u8>>, EngineError> {
    let mut payload = encode_enveloped_stats(&stats)?;
    if payload.len() <= soft_limit {
        return Ok(Some(payload));
    }

    let mut reductions = [
        ReductionStep::GeneralProperties,
        ReductionStep::AdjacencyHubSamples,
        ReductionStep::AdjacencyStats,
        ReductionStep::DeclaredEqualityHeavyHitters,
        ReductionStep::DeclaredRangeBuckets,
    ]
    .into_iter();

    while payload.len() > soft_limit {
        let Some(step) = reductions.next() else {
            break;
        };
        if apply_reduction_step(&mut stats, step) {
            stats.truncated = true;
            payload = encode_enveloped_stats(&stats)?;
        }
    }

    if payload.len() > hard_limit {
        Ok(None)
    } else {
        Ok(Some(payload))
    }
}

#[derive(Clone, Copy)]
enum ReductionStep {
    GeneralProperties,
    AdjacencyHubSamples,
    AdjacencyStats,
    DeclaredEqualityHeavyHitters,
    DeclaredRangeBuckets,
}

fn apply_reduction_step(stats: &mut SegmentPlannerStatsV1, step: ReductionStep) -> bool {
    match step {
        ReductionStep::GeneralProperties => {
            let before = stats.property_stats.len();
            stats.property_stats.retain(|prop| {
                prop.tracked_reason != PropertyStatsTrackedReason::GeneralTopProperty
            });
            before != stats.property_stats.len()
        }
        ReductionStep::AdjacencyHubSamples => {
            let mut changed = false;
            for adjacency in &mut stats.adjacency_stats {
                if !adjacency.top_hubs.is_empty() {
                    adjacency.top_hubs.clear();
                    changed = true;
                }
            }
            changed
        }
        ReductionStep::AdjacencyStats => {
            let changed = !stats.adjacency_stats.is_empty();
            stats.adjacency_stats.clear();
            changed
        }
        ReductionStep::DeclaredEqualityHeavyHitters => {
            let mut changed = false;
            for equality in &mut stats.equality_index_stats {
                if !equality.top_value_hashes.is_empty() {
                    equality.top_value_hashes.clear();
                    changed = true;
                }
            }
            for prop in &mut stats.property_stats {
                if !prop.top_values.is_empty() {
                    prop.top_values.clear();
                    changed = true;
                }
            }
            changed
        }
        ReductionStep::DeclaredRangeBuckets => {
            let mut changed = false;
            for range in &mut stats.range_index_stats {
                if !range.buckets.is_empty() {
                    range.buckets.clear();
                    changed = true;
                }
            }
            for prop in &mut stats.property_stats {
                for summary in &mut prop.numeric_summaries {
                    if !summary.buckets.is_empty() {
                        summary.buckets.clear();
                        changed = true;
                    }
                }
            }
            changed
        }
    }
}

fn encode_enveloped_stats(stats: &SegmentPlannerStatsV1) -> Result<Vec<u8>, EngineError> {
    let payload = rmp_serde::to_vec(stats)
        .map_err(|error| EngineError::SerializationError(error.to_string()))?;
    let mut crc = Crc32Hasher::new();
    crc.update(&payload);
    let checksum = crc.finalize();
    let mut data = Vec::with_capacity(PLANNER_STATS_ENVELOPE_LEN + payload.len());
    data.extend_from_slice(&PLANNER_STATS_MAGIC);
    data.extend_from_slice(&PLANNER_STATS_FORMAT_VERSION.to_le_bytes());
    data.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    data.extend_from_slice(&checksum.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());
    data.extend_from_slice(&payload);
    Ok(data)
}

enum PlannerStatsReadFailure {
    Missing,
    Unavailable(String),
}

fn read_planner_stats_file(
    path: &Path,
    expected_segment_id: u64,
    expected_node_count: u64,
    expected_edge_count: u64,
) -> Result<SegmentPlannerStatsV1, PlannerStatsReadFailure> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(PlannerStatsReadFailure::Missing);
        }
        Err(error) => return Err(PlannerStatsReadFailure::Unavailable(error.to_string())),
    };
    let file_len = file
        .metadata()
        .map_err(|error| PlannerStatsReadFailure::Unavailable(error.to_string()))?
        .len();
    if file_len > PLANNER_STATS_HARD_SIDECAR_BYTES as u64 {
        return Err(PlannerStatsReadFailure::Unavailable(format!(
            "planner stats sidecar exceeds hard cap: {} bytes",
            file_len
        )));
    }
    let mut data = Vec::with_capacity(file_len as usize);
    file.take((PLANNER_STATS_HARD_SIDECAR_BYTES + 1) as u64)
        .read_to_end(&mut data)
        .map_err(|error| PlannerStatsReadFailure::Unavailable(error.to_string()))?;
    if data.len() > PLANNER_STATS_HARD_SIDECAR_BYTES {
        return Err(PlannerStatsReadFailure::Unavailable(format!(
            "planner stats sidecar exceeds hard cap: {} bytes",
            data.len()
        )));
    }
    decode_planner_stats_envelope(
        &data,
        expected_segment_id,
        expected_node_count,
        expected_edge_count,
    )
    .map_err(PlannerStatsReadFailure::Unavailable)
}

fn decode_planner_stats_envelope(
    data: &[u8],
    expected_segment_id: u64,
    expected_node_count: u64,
    expected_edge_count: u64,
) -> Result<SegmentPlannerStatsV1, String> {
    if data.len() < PLANNER_STATS_ENVELOPE_LEN {
        return Err("planner stats sidecar is shorter than envelope".to_string());
    }
    if data[0..8] != PLANNER_STATS_MAGIC {
        return Err("planner stats sidecar has bad magic".to_string());
    }
    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if version != PLANNER_STATS_FORMAT_VERSION {
        return Err(format!("unsupported planner stats version {}", version));
    }
    let payload_len = u64::from_le_bytes(data[12..20].try_into().unwrap()) as usize;
    let expected_payload_len = data.len() - PLANNER_STATS_ENVELOPE_LEN;
    if payload_len != expected_payload_len {
        return Err(format!(
            "planner stats payload length mismatch: header={}, actual={}",
            payload_len, expected_payload_len
        ));
    }
    let expected_crc = u32::from_le_bytes(data[20..24].try_into().unwrap());
    let reserved = u32::from_le_bytes(data[24..28].try_into().unwrap());
    if reserved != 0 {
        return Err("planner stats sidecar reserved field is nonzero".to_string());
    }
    let payload = &data[PLANNER_STATS_ENVELOPE_LEN..];
    let mut crc = Crc32Hasher::new();
    crc.update(payload);
    let actual_crc = crc.finalize();
    if expected_crc != actual_crc {
        return Err("planner stats payload crc mismatch".to_string());
    }
    let stats: SegmentPlannerStatsV1 =
        rmp_serde::from_slice(payload).map_err(|error| error.to_string())?;
    validate_stats_payload(
        &stats,
        expected_segment_id,
        expected_node_count,
        expected_edge_count,
    )?;
    Ok(stats)
}

fn validate_stats_payload(
    stats: &SegmentPlannerStatsV1,
    expected_segment_id: u64,
    expected_node_count: u64,
    expected_edge_count: u64,
) -> Result<(), String> {
    if stats.format_version != PLANNER_STATS_FORMAT_VERSION {
        return Err(format!(
            "planner stats payload version mismatch: {}",
            stats.format_version
        ));
    }
    if stats.segment_id != expected_segment_id {
        return Err(format!(
            "planner stats segment id mismatch: expected {}, got {}",
            expected_segment_id, stats.segment_id
        ));
    }
    if stats.node_count != expected_node_count {
        return Err(format!(
            "planner stats node count mismatch: expected {}, got {}",
            expected_node_count, stats.node_count
        ));
    }
    if stats.edge_count != expected_edge_count {
        return Err(format!(
            "planner stats edge count mismatch: expected {}, got {}",
            expected_edge_count, stats.edge_count
        ));
    }
    if stats.general_property_sampled_node_count > stats.node_count {
        return Err("planner stats sampled node count exceeds node count".to_string());
    }
    if stats.general_property_stats_complete
        && stats.general_property_sampled_node_count != stats.node_count
    {
        return Err("planner stats complete property section has partial sample count".to_string());
    }
    if stats.node_id_sample.len() > PLANNER_STATS_NODE_ID_SAMPLE_SIZE
        || stats.node_id_sample.len() as u64 > stats.node_count
    {
        return Err("planner stats node id sample exceeds allowed size".to_string());
    }
    if !stats
        .node_id_sample
        .windows(2)
        .all(|pair| pair[0] <= pair[1])
    {
        return Err("planner stats node id sample is not sorted".to_string());
    }
    let type_counts = validate_type_stats(stats)?;
    validate_timestamp_stats(stats, &type_counts)?;
    validate_property_stats(stats, &type_counts)?;
    validate_declared_index_stats(stats, &type_counts)?;
    validate_adjacency_stats(stats)?;
    Ok(())
}

fn validate_type_stats(stats: &SegmentPlannerStatsV1) -> Result<BTreeMap<u32, u64>, String> {
    let mut type_counts = BTreeMap::new();
    let mut total = 0u64;
    for type_stat in &stats.type_stats {
        if type_stat.node_count == 0 {
            return Err(format!(
                "planner stats type {} has zero node count",
                type_stat.type_id
            ));
        }
        if type_counts
            .insert(type_stat.type_id, type_stat.node_count)
            .is_some()
        {
            return Err(format!(
                "planner stats type {} appears more than once",
                type_stat.type_id
            ));
        }
        total = checked_add_count(total, type_stat.node_count, "type node counts")?;
        validate_ordered_option_pair(
            type_stat.min_node_id,
            type_stat.max_node_id,
            "type node id bounds",
        )?;
        validate_ordered_option_pair(
            type_stat.min_updated_at_ms,
            type_stat.max_updated_at_ms,
            "type updated-at bounds",
        )?;
    }
    if total != stats.node_count {
        return Err(format!(
            "planner stats type counts sum to {}, expected {}",
            total, stats.node_count
        ));
    }
    Ok(type_counts)
}

fn validate_timestamp_stats(
    stats: &SegmentPlannerStatsV1,
    type_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let mut seen = BTreeMap::new();
    for timestamp in &stats.timestamp_stats {
        let Some(type_count) = type_counts.get(&timestamp.type_id) else {
            return Err(format!(
                "planner stats timestamp section references unknown type {}",
                timestamp.type_id
            ));
        };
        if seen.insert(timestamp.type_id, ()).is_some() {
            return Err(format!(
                "planner stats timestamp section repeats type {}",
                timestamp.type_id
            ));
        }
        if timestamp.count != *type_count {
            return Err(format!(
                "planner stats timestamp count for type {} is {}, expected {}",
                timestamp.type_id, timestamp.count, type_count
            ));
        }
        if timestamp.min_ms > timestamp.max_ms {
            return Err("planner stats timestamp bounds are reversed".to_string());
        }
        validate_timestamp_buckets(timestamp.count, &timestamp.buckets)?;
    }
    Ok(())
}

fn validate_property_stats(
    stats: &SegmentPlannerStatsV1,
    type_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let mut seen = BTreeMap::new();
    for prop in &stats.property_stats {
        let Some(type_count) = type_counts.get(&prop.type_id) else {
            return Err(format!(
                "planner stats property {} references unknown type {}",
                prop.prop_key, prop.type_id
            ));
        };
        let key = (prop.type_id, prop.prop_key.as_str());
        if seen.insert(key, ()).is_some() {
            return Err(format!(
                "planner stats property {} for type {} appears more than once",
                prop.prop_key, prop.type_id
            ));
        }
        if prop.present_count > *type_count {
            return Err(format!(
                "planner stats property {} present count exceeds type count",
                prop.prop_key
            ));
        }
        if prop.null_count > prop.present_count {
            return Err(format!(
                "planner stats property {} null count exceeds present count",
                prop.prop_key
            ));
        }
        if prop.value_kind_counts.null_count != prop.null_count {
            return Err(format!(
                "planner stats property {} null kind count mismatch",
                prop.prop_key
            ));
        }
        let kind_total = value_kind_total(&prop.value_kind_counts)?;
        if kind_total != prop.present_count {
            return Err(format!(
                "planner stats property {} value-kind counts sum to {}, expected {}",
                prop.prop_key, kind_total, prop.present_count
            ));
        }
        if let Some(exact) = prop.exact_distinct_count {
            if exact > prop.present_count {
                return Err(format!(
                    "planner stats property {} exact distinct count exceeds present count",
                    prop.prop_key
                ));
            }
        }
        if let Some(lower_bound) = prop.distinct_lower_bound {
            if lower_bound > prop.present_count {
                return Err(format!(
                    "planner stats property {} distinct lower bound exceeds present count",
                    prop.prop_key
                ));
            }
        }
        validate_value_frequencies(&prop.top_values, prop.present_count, "property top values")?;
        for summary in &prop.numeric_summaries {
            if summary.count > prop.present_count {
                return Err(format!(
                    "planner stats property {} numeric summary exceeds present count",
                    prop.prop_key
                ));
            }
            validate_ordered_option_pair(
                summary.min_encoded,
                summary.max_encoded,
                "property numeric bounds",
            )?;
            validate_range_buckets(summary.count, &summary.buckets)?;
        }
    }
    Ok(())
}

fn validate_declared_index_stats(
    stats: &SegmentPlannerStatsV1,
    type_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let declared = declared_index_map(stats)?;
    let mut equality_seen = BTreeMap::new();
    for equality in &stats.equality_index_stats {
        let Some(declared_index) = declared.get(&equality.index_id) else {
            return Err(format!(
                "planner stats equality index {} has no declaration",
                equality.index_id
            ));
        };
        if declared_index.kind != PlannerStatsDeclaredIndexKind::Equality
            || declared_index.type_id != equality.type_id
            || declared_index.prop_key != equality.prop_key
        {
            return Err(format!(
                "planner stats equality index {} declaration mismatch",
                equality.index_id
            ));
        }
        if equality_seen.insert(equality.index_id, ()).is_some() {
            return Err(format!(
                "planner stats equality index {} appears more than once",
                equality.index_id
            ));
        }
        let type_count = *type_counts.get(&equality.type_id).unwrap_or(&0);
        if equality.total_postings > type_count {
            return Err(format!(
                "planner stats equality index {} postings exceed type count",
                equality.index_id
            ));
        }
        if equality.value_group_count > equality.total_postings {
            return Err(format!(
                "planner stats equality index {} group count exceeds postings",
                equality.index_id
            ));
        }
        if equality.max_group_postings > equality.total_postings {
            return Err(format!(
                "planner stats equality index {} max group exceeds postings",
                equality.index_id
            ));
        }
        validate_value_frequencies(
            &equality.top_value_hashes,
            equality.total_postings,
            "equality heavy hitters",
        )?;
    }

    let mut range_seen = BTreeMap::new();
    for range in &stats.range_index_stats {
        let Some(declared_index) = declared.get(&range.index_id) else {
            return Err(format!(
                "planner stats range index {} has no declaration",
                range.index_id
            ));
        };
        if declared_index.kind != PlannerStatsDeclaredIndexKind::Range
            || declared_index.type_id != range.type_id
            || declared_index.prop_key != range.prop_key
            || declared_index.range_domain != Some(range.domain)
        {
            return Err(format!(
                "planner stats range index {} declaration mismatch",
                range.index_id
            ));
        }
        if range_seen.insert(range.index_id, ()).is_some() {
            return Err(format!(
                "planner stats range index {} appears more than once",
                range.index_id
            ));
        }
        let type_count = *type_counts.get(&range.type_id).unwrap_or(&0);
        if range.total_entries > type_count {
            return Err(format!(
                "planner stats range index {} entries exceed type count",
                range.index_id
            ));
        }
        validate_ordered_option_pair(range.min_encoded, range.max_encoded, "range index bounds")?;
        validate_range_buckets(range.total_entries, &range.buckets)?;
    }
    Ok(())
}

fn declared_index_map(
    stats: &SegmentPlannerStatsV1,
) -> Result<BTreeMap<u64, &DeclaredIndexStatsFingerprint>, String> {
    let mut declared = BTreeMap::new();
    for entry in &stats.declared_indexes {
        if declared.insert(entry.index_id, entry).is_some() {
            return Err(format!(
                "planner stats declaration {} appears more than once",
                entry.index_id
            ));
        }
    }
    Ok(declared)
}

fn validate_adjacency_stats(stats: &SegmentPlannerStatsV1) -> Result<(), String> {
    let mut seen = BTreeMap::new();
    for adjacency in &stats.adjacency_stats {
        let key = (adjacency.direction, adjacency.edge_type_id);
        if seen.insert(key, ()).is_some() {
            return Err("planner stats adjacency section repeats a direction/type".to_string());
        }
        if adjacency.source_node_count == 0 || adjacency.total_edges == 0 {
            return Err("planner stats adjacency section has empty counts".to_string());
        }
        if adjacency.source_node_count > adjacency.total_edges {
            return Err(
                "planner stats adjacency source count exceeds total edge count".to_string(),
            );
        }
        if adjacency.total_edges > stats.edge_count {
            return Err("planner stats adjacency total exceeds segment edge count".to_string());
        }
        if adjacency.edge_type_id.is_none() && adjacency.total_edges != stats.edge_count {
            return Err(
                "planner stats global adjacency total does not match edge count".to_string(),
            );
        }
        if adjacency.min_fanout == 0
            || adjacency.min_fanout > adjacency.max_fanout
            || adjacency.p50_fanout < adjacency.min_fanout
            || adjacency.p50_fanout > adjacency.max_fanout
            || adjacency.p90_fanout < adjacency.min_fanout
            || adjacency.p90_fanout > adjacency.max_fanout
            || adjacency.p99_fanout < adjacency.min_fanout
            || adjacency.p99_fanout > adjacency.max_fanout
        {
            return Err("planner stats adjacency fanout summary is inconsistent".to_string());
        }
        if adjacency.top_hubs.len() as u64 > adjacency.source_node_count {
            return Err("planner stats adjacency hub sample exceeds source count".to_string());
        }
        for hub in &adjacency.top_hubs {
            if hub.count == 0 || hub.count > adjacency.max_fanout {
                return Err("planner stats adjacency hub sample is inconsistent".to_string());
            }
        }
    }
    Ok(())
}

fn validate_timestamp_buckets(
    expected_count: u64,
    buckets: &[TimestampBucket],
) -> Result<(), String> {
    if expected_count == 0 {
        if buckets.is_empty() {
            return Ok(());
        }
        return Err("planner stats timestamp buckets exist for empty summary".to_string());
    }
    if buckets.is_empty() {
        return Err("planner stats timestamp summary has no buckets".to_string());
    }
    if !buckets
        .windows(2)
        .all(|pair| pair[0].upper_ms <= pair[1].upper_ms)
    {
        return Err("planner stats timestamp buckets are not sorted".to_string());
    }
    let sum = checked_count_sum(
        buckets.iter().map(|bucket| bucket.count),
        "timestamp bucket counts",
    )?;
    if sum != expected_count {
        return Err(format!(
            "planner stats timestamp buckets sum to {}, expected {}",
            sum, expected_count
        ));
    }
    Ok(())
}

fn validate_range_buckets(expected_count: u64, buckets: &[RangeBucket]) -> Result<(), String> {
    if buckets.is_empty() {
        return Ok(());
    }
    if !buckets
        .windows(2)
        .all(|pair| pair[0].upper_encoded <= pair[1].upper_encoded)
    {
        return Err("planner stats range buckets are not sorted".to_string());
    }
    let sum = checked_count_sum(
        buckets.iter().map(|bucket| bucket.count),
        "range bucket counts",
    )?;
    if sum != expected_count {
        return Err(format!(
            "planner stats range buckets sum to {}, expected {}",
            sum, expected_count
        ));
    }
    Ok(())
}

fn validate_value_frequencies(
    values: &[ValueFrequency],
    max_total: u64,
    label: &str,
) -> Result<(), String> {
    if values.len() > PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY {
        return Err(format!("planner stats {} exceed cap", label));
    }
    if !values.windows(2).all(|pair| {
        pair[0].count > pair[1].count
            || (pair[0].count == pair[1].count && pair[0].value_hash <= pair[1].value_hash)
    }) {
        return Err(format!(
            "planner stats {} are not deterministically sorted",
            label
        ));
    }
    let sum = checked_count_sum(values.iter().map(|value| value.count), label)?;
    if sum > max_total {
        return Err(format!(
            "planner stats {} sum to {}, exceeds {}",
            label, sum, max_total
        ));
    }
    if values.iter().any(|value| value.count > max_total) {
        return Err(format!("planner stats {} entry exceeds total", label));
    }
    Ok(())
}

fn value_kind_total(counts: &ValueKindCounts) -> Result<u64, String> {
    let mut total = 0u64;
    for count in [
        counts.null_count,
        counts.bool_count,
        counts.int_count,
        counts.uint_count,
        counts.float_count,
        counts.string_count,
        counts.bytes_count,
        counts.array_count,
        counts.map_count,
    ] {
        total = checked_add_count(total, count, "value kind counts")?;
    }
    Ok(total)
}

fn checked_count_sum(counts: impl Iterator<Item = u64>, label: &str) -> Result<u64, String> {
    let mut total = 0u64;
    for count in counts {
        total = checked_add_count(total, count, label)?;
    }
    Ok(total)
}

fn checked_add_count(left: u64, right: u64, label: &str) -> Result<u64, String> {
    left.checked_add(right)
        .ok_or_else(|| format!("planner stats {} overflow", label))
}

fn validate_ordered_option_pair<T: Ord>(
    min: Option<T>,
    max: Option<T>,
    label: &str,
) -> Result<(), String> {
    match (min, max) {
        (Some(min), Some(max)) if min <= max => Ok(()),
        (None, None) => Ok(()),
        _ => Err(format!("planner stats {} are inconsistent", label)),
    }
}

fn observe_type(
    type_accs: &mut BTreeMap<u32, TypeAccumulator>,
    node_id: u64,
    type_id: u32,
    updated_at_ms: i64,
) {
    let acc = type_accs.entry(type_id).or_default();
    acc.node_count += 1;
    acc.min_node_id = Some(acc.min_node_id.map_or(node_id, |value| value.min(node_id)));
    acc.max_node_id = Some(acc.max_node_id.map_or(node_id, |value| value.max(node_id)));
    acc.min_updated_at_ms = Some(
        acc.min_updated_at_ms
            .map_or(updated_at_ms, |value| value.min(updated_at_ms)),
    );
    acc.max_updated_at_ms = Some(
        acc.max_updated_at_ms
            .map_or(updated_at_ms, |value| value.max(updated_at_ms)),
    );
    acc.updated_values.push(updated_at_ms);
}

fn finalize_type_stats(type_accs: BTreeMap<u32, TypeAccumulator>) -> Vec<TypePlannerStats> {
    type_accs
        .into_iter()
        .map(|(type_id, acc)| TypePlannerStats {
            type_id,
            node_count: acc.node_count,
            min_node_id: acc.min_node_id,
            max_node_id: acc.max_node_id,
            min_updated_at_ms: acc.min_updated_at_ms,
            max_updated_at_ms: acc.max_updated_at_ms,
        })
        .collect()
}

fn finalize_timestamp_stats(nodes: &[&NodeRecord]) -> Vec<TimestampPlannerStats> {
    let mut by_type: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    for node in nodes {
        by_type
            .entry(node.type_id)
            .or_default()
            .push(node.updated_at);
    }
    finalize_timestamp_groups(by_type)
}

fn finalize_timestamp_stats_from_meta(
    node_metas: &[CompactNodeMeta],
) -> Vec<TimestampPlannerStats> {
    let mut by_type: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    for meta in node_metas {
        by_type
            .entry(meta.type_id)
            .or_default()
            .push(meta.updated_at);
    }
    finalize_timestamp_groups(by_type)
}

fn finalize_timestamp_groups(by_type: BTreeMap<u32, Vec<i64>>) -> Vec<TimestampPlannerStats> {
    by_type
        .into_iter()
        .filter_map(|(type_id, mut values)| {
            if values.is_empty() {
                return None;
            }
            values.sort_unstable();
            let min_ms = *values.first().unwrap();
            let max_ms = *values.last().unwrap();
            let buckets = timestamp_buckets(&values, PLANNER_STATS_TIMESTAMP_BUCKETS);
            Some(TimestampPlannerStats {
                type_id,
                count: values.len() as u64,
                min_ms,
                max_ms,
                buckets,
            })
        })
        .collect()
}

fn declared_property_reasons(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> BTreeMap<(u32, String), PropertyStatsTrackedReason> {
    let mut reasons: BTreeMap<(u32, String), PropertyStatsTrackedReason> = BTreeMap::new();
    for entry in secondary_indexes {
        let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
        let new_reason = match entry.kind {
            SecondaryIndexKind::Equality => PropertyStatsTrackedReason::DeclaredEquality,
            SecondaryIndexKind::Range { .. } => PropertyStatsTrackedReason::DeclaredRange,
        };
        reasons
            .entry((*type_id, prop_key.clone()))
            .and_modify(|reason| *reason = combine_property_reason(*reason, new_reason))
            .or_insert(new_reason);
    }
    reasons
}

fn seed_property_accumulators(
    declared_reasons: &BTreeMap<(u32, String), PropertyStatsTrackedReason>,
    property_candidates: BTreeMap<u32, PropertyKeyCandidateTracker>,
    type_accs: &BTreeMap<u32, TypeAccumulator>,
) -> BTreeMap<(u32, String), PropertyAccumulator> {
    let mut accs = BTreeMap::new();
    for ((type_id, prop_key), reason) in declared_reasons {
        if !type_accs.contains_key(type_id) {
            continue;
        }
        accs.insert(
            (*type_id, prop_key.clone()),
            PropertyAccumulator::new(*type_id, prop_key.clone(), *reason),
        );
    }
    for (type_id, tracker) in property_candidates {
        for prop_key in tracker.into_keys() {
            accs.entry((type_id, prop_key.clone())).or_insert_with(|| {
                PropertyAccumulator::new(
                    type_id,
                    prop_key,
                    PropertyStatsTrackedReason::GeneralTopProperty,
                )
            });
        }
    }
    accs
}

fn combine_property_reason(
    existing: PropertyStatsTrackedReason,
    new_reason: PropertyStatsTrackedReason,
) -> PropertyStatsTrackedReason {
    match (existing, new_reason) {
        (
            PropertyStatsTrackedReason::DeclaredEquality,
            PropertyStatsTrackedReason::DeclaredRange,
        )
        | (
            PropertyStatsTrackedReason::DeclaredRange,
            PropertyStatsTrackedReason::DeclaredEquality,
        )
        | (PropertyStatsTrackedReason::DeclaredEqualityAndRange, _)
        | (_, PropertyStatsTrackedReason::DeclaredEqualityAndRange) => {
            PropertyStatsTrackedReason::DeclaredEqualityAndRange
        }
        (reason, _) => reason,
    }
}

fn observe_general_property_candidates(
    candidates: &mut BTreeMap<u32, PropertyKeyCandidateTracker>,
    declared_reasons: &BTreeMap<(u32, String), PropertyStatsTrackedReason>,
    type_id: u32,
    props: &BTreeMap<String, PropValue>,
) {
    let tracker = candidates.entry(type_id).or_insert_with(|| {
        PropertyKeyCandidateTracker::new(PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_TYPE)
    });
    for key in props.keys() {
        if declared_reasons.contains_key(&(type_id, key.clone())) {
            continue;
        }
        tracker.observe(key);
    }
}

fn observe_selected_node_properties(
    accs: &mut BTreeMap<(u32, String), PropertyAccumulator>,
    type_id: u32,
    props: &BTreeMap<String, PropValue>,
) {
    for (key, value) in props {
        if let Some(acc) = accs.get_mut(&(type_id, key.clone())) {
            acc.observe(value);
        }
    }
}

fn finalize_property_stats(
    accs: BTreeMap<(u32, String), PropertyAccumulator>,
) -> Vec<PropertyPlannerStats> {
    let mut by_type: BTreeMap<u32, Vec<PropertyAccumulator>> = BTreeMap::new();
    for acc in accs.into_values() {
        by_type.entry(acc.type_id).or_default().push(acc);
    }

    let mut stats = Vec::new();
    for (_type_id, mut props) in by_type {
        let mut declared = Vec::new();
        let mut general = Vec::new();
        for acc in props.drain(..) {
            if acc.tracked_reason == PropertyStatsTrackedReason::GeneralTopProperty {
                general.push(acc);
            } else {
                declared.push(acc);
            }
        }
        declared.sort_by(|a, b| {
            a.tracked_reason
                .cmp(&b.tracked_reason)
                .then_with(|| a.prop_key.cmp(&b.prop_key))
        });
        general.sort_by(|a, b| {
            b.present_count
                .cmp(&a.present_count)
                .then_with(|| a.prop_key.as_bytes().cmp(b.prop_key.as_bytes()))
        });
        stats.extend(declared.into_iter().map(PropertyAccumulator::into_stats));
        stats.extend(
            general
                .into_iter()
                .take(PLANNER_STATS_MAX_PROPERTY_KEYS_PER_TYPE)
                .map(PropertyAccumulator::into_stats),
        );
    }
    stats.sort_by(|a, b| {
        a.type_id
            .cmp(&b.type_id)
            .then_with(|| a.tracked_reason.cmp(&b.tracked_reason))
            .then_with(|| a.prop_key.cmp(&b.prop_key))
    });
    stats
}

fn declared_index_fingerprints(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Vec<DeclaredIndexStatsFingerprint> {
    let mut declared: Vec<_> = secondary_indexes
        .iter()
        .map(|entry| {
            let SecondaryIndexTarget::NodeProperty { type_id, prop_key } = &entry.target;
            let (kind, range_domain) = match entry.kind {
                SecondaryIndexKind::Equality => (PlannerStatsDeclaredIndexKind::Equality, None),
                SecondaryIndexKind::Range { domain } => {
                    (PlannerStatsDeclaredIndexKind::Range, Some(domain))
                }
            };
            DeclaredIndexStatsFingerprint {
                index_id: entry.index_id,
                kind,
                type_id: *type_id,
                prop_key: prop_key.clone(),
                range_domain,
            }
        })
        .collect();
    declared.sort_by(|a, b| {
        a.index_id
            .cmp(&b.index_id)
            .then_with(|| declared_index_kind_rank(a.kind).cmp(&declared_index_kind_rank(b.kind)))
            .then_with(|| a.type_id.cmp(&b.type_id))
            .then_with(|| a.prop_key.cmp(&b.prop_key))
            .then_with(|| range_domain_rank(a.range_domain).cmp(&range_domain_rank(b.range_domain)))
    });
    declared
}

fn declaration_fingerprint(declared: &[DeclaredIndexStatsFingerprint]) -> u64 {
    let mut hash = FNV_OFFSET;
    for entry in declared {
        hash = fnv_update_u64(hash, entry.index_id);
        hash = fnv_update_u8(
            hash,
            match entry.kind {
                PlannerStatsDeclaredIndexKind::Equality => 1,
                PlannerStatsDeclaredIndexKind::Range => 2,
            },
        );
        hash = fnv_update_u32(hash, entry.type_id);
        hash = fnv_update_bytes(hash, entry.prop_key.as_bytes());
        hash = fnv_update_u8(
            hash,
            match entry.range_domain {
                None => 0,
                Some(SecondaryIndexRangeDomain::Int) => 1,
                Some(SecondaryIndexRangeDomain::UInt) => 2,
                Some(SecondaryIndexRangeDomain::Float) => 3,
            },
        );
    }
    hash
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv_update_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn fnv_update_u8(hash: u64, value: u8) -> u64 {
    fnv_update_bytes(hash, &[value])
}

fn fnv_update_u32(hash: u64, value: u32) -> u64 {
    fnv_update_bytes(hash, &value.to_le_bytes())
}

fn fnv_update_u64(hash: u64, value: u64) -> u64 {
    fnv_update_bytes(hash, &value.to_le_bytes())
}

fn declared_index_kind_rank(kind: PlannerStatsDeclaredIndexKind) -> u8 {
    match kind {
        PlannerStatsDeclaredIndexKind::Equality => 0,
        PlannerStatsDeclaredIndexKind::Range => 1,
    }
}

fn range_domain_rank(domain: Option<SecondaryIndexRangeDomain>) -> u8 {
    match domain {
        None => 0,
        Some(SecondaryIndexRangeDomain::Int) => 1,
        Some(SecondaryIndexRangeDomain::UInt) => 2,
        Some(SecondaryIndexRangeDomain::Float) => 3,
    }
}

fn push_capped_numeric(values: &mut Vec<u64>, encoded: u64) {
    if values.len() < PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES {
        values.push(encoded);
    }
}

fn range_summary_from_values(
    domain: SecondaryIndexRangeDomain,
    values: &[u64],
) -> RangeValueSummary {
    RangeValueSummary {
        domain,
        count: values.len() as u64,
        min_encoded: values.first().copied(),
        max_encoded: values.last().copied(),
        buckets: range_buckets(values, PLANNER_STATS_RANGE_BUCKETS),
    }
}

fn top_value_frequencies(counts: BTreeMap<u64, u64>, cap: usize) -> Vec<ValueFrequency> {
    let mut values: Vec<_> = counts
        .into_iter()
        .map(|(value_hash, count)| ValueFrequency { value_hash, count })
        .collect();
    values.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.value_hash.cmp(&b.value_hash))
    });
    values.truncate(cap);
    values
}

fn timestamp_buckets(values: &[i64], cap: usize) -> Vec<TimestampBucket> {
    if values.is_empty() || cap == 0 {
        return Vec::new();
    }
    let bucket_count = values.len().min(cap);
    let mut buckets = Vec::with_capacity(bucket_count);
    let mut start = 0usize;
    for bucket_idx in 0..bucket_count {
        let end = ((bucket_idx + 1) * values.len()).div_ceil(bucket_count);
        if end > start {
            buckets.push(TimestampBucket {
                upper_ms: values[end - 1],
                count: (end - start) as u64,
            });
        }
        start = end;
    }
    buckets
}

fn range_buckets(values: &[u64], cap: usize) -> Vec<RangeBucket> {
    if values.is_empty() || cap == 0 {
        return Vec::new();
    }
    let bucket_count = values.len().min(cap);
    let mut buckets = Vec::with_capacity(bucket_count);
    let mut start = 0usize;
    for bucket_idx in 0..bucket_count {
        let end = ((bucket_idx + 1) * values.len()).div_ceil(bucket_count);
        if end > start {
            buckets.push(RangeBucket {
                upper_encoded: values[end - 1],
                count: (end - start) as u64,
            });
        }
        start = end;
    }
    buckets
}

fn node_id_sample(ids: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut sorted: Vec<u64> = ids.collect();
    sorted.sort_unstable();
    if sorted.len() <= PLANNER_STATS_NODE_ID_SAMPLE_SIZE {
        return sorted;
    }
    let last = sorted.len() - 1;
    (0..PLANNER_STATS_NODE_ID_SAMPLE_SIZE)
        .map(|i| {
            let idx = i * last / (PLANNER_STATS_NODE_ID_SAMPLE_SIZE - 1);
            sorted[idx]
        })
        .collect()
}

trait EdgeLike {
    fn edge_type_id(&self) -> u32;
    fn source_node_id(&self) -> u64;
    fn target_node_id(&self) -> u64;
}

impl EdgeLike for EdgeRecord {
    fn edge_type_id(&self) -> u32 {
        self.type_id
    }
    fn source_node_id(&self) -> u64 {
        self.from
    }
    fn target_node_id(&self) -> u64 {
        self.to
    }
}

impl<T: EdgeLike + ?Sized> EdgeLike for &T {
    fn edge_type_id(&self) -> u32 {
        (*self).edge_type_id()
    }

    fn source_node_id(&self) -> u64 {
        (*self).source_node_id()
    }

    fn target_node_id(&self) -> u64 {
        (*self).target_node_id()
    }
}

#[derive(Clone, Copy)]
struct EdgeMetaRef {
    type_id: u32,
    from: u64,
    to: u64,
}

impl From<&CompactEdgeMeta> for EdgeMetaRef {
    fn from(meta: &CompactEdgeMeta) -> Self {
        Self {
            type_id: meta.type_id,
            from: meta.from,
            to: meta.to,
        }
    }
}

impl EdgeLike for EdgeMetaRef {
    fn edge_type_id(&self) -> u32 {
        self.type_id
    }
    fn source_node_id(&self) -> u64 {
        self.from
    }
    fn target_node_id(&self) -> u64 {
        self.to
    }
}

fn build_adjacency_stats_from_edges<'a>(
    edges: impl Iterator<Item = &'a EdgeRecord>,
) -> Vec<AdjacencyPlannerStats> {
    build_adjacency_stats(edges)
}

fn build_adjacency_stats_from_edge_meta(
    edges: impl Iterator<Item = EdgeMetaRef>,
) -> Vec<AdjacencyPlannerStats> {
    build_adjacency_stats(edges)
}

fn build_adjacency_stats<E: EdgeLike>(
    edges: impl Iterator<Item = E>,
) -> Vec<AdjacencyPlannerStats> {
    let mut groups: BTreeMap<(PlannerStatsDirection, Option<u32>), BTreeMap<u64, u32>> =
        BTreeMap::new();
    for edge in edges {
        let type_id = edge.edge_type_id();
        for edge_type_id in [None, Some(type_id)] {
            *groups
                .entry((PlannerStatsDirection::Outgoing, edge_type_id))
                .or_default()
                .entry(edge.source_node_id())
                .or_default() += 1;
            *groups
                .entry((PlannerStatsDirection::Incoming, edge_type_id))
                .or_default()
                .entry(edge.target_node_id())
                .or_default() += 1;
        }
    }

    groups
        .into_iter()
        .filter_map(|((direction, edge_type_id), fanouts)| {
            if fanouts.is_empty() {
                return None;
            }
            Some(adjacency_stats_from_fanouts(
                direction,
                edge_type_id,
                fanouts,
            ))
        })
        .collect()
}

fn adjacency_stats_from_fanouts(
    direction: PlannerStatsDirection,
    edge_type_id: Option<u32>,
    fanouts: BTreeMap<u64, u32>,
) -> AdjacencyPlannerStats {
    let mut counts: Vec<u32> = fanouts.values().copied().collect();
    counts.sort_unstable();
    let total_edges = counts.iter().map(|count| *count as u64).sum();
    let min_fanout = *counts.first().unwrap_or(&0);
    let max_fanout = *counts.last().unwrap_or(&0);
    let p50_fanout = percentile_nearest_rank(&counts, 50);
    let p90_fanout = percentile_nearest_rank(&counts, 90);
    let p99_fanout = percentile_nearest_rank(&counts, 99);
    let mut top_hubs: Vec<_> = fanouts
        .into_iter()
        .map(|(node_id, count)| NodeFanoutFrequency { node_id, count })
        .collect();
    top_hubs.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    top_hubs.truncate(PLANNER_STATS_TOP_HUBS_PER_EDGE_TYPE);
    AdjacencyPlannerStats {
        direction,
        edge_type_id,
        source_node_count: counts.len() as u64,
        total_edges,
        min_fanout,
        max_fanout,
        p50_fanout,
        p90_fanout,
        p99_fanout,
        top_hubs,
    }
}

fn percentile_nearest_rank(sorted_counts: &[u32], percentile: usize) -> u32 {
    if sorted_counts.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted_counts.len()).div_ceil(100);
    sorted_counts[rank.saturating_sub(1).min(sorted_counts.len() - 1)]
}

fn decode_node_props_at(
    data: &[u8],
    data_offset: u64,
    node_id: u64,
) -> Result<BTreeMap<String, PropValue>, EngineError> {
    let start = data_offset as usize;
    let key_len_start = start.checked_add(4).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} props offset overflow", node_id))
    })?;
    let key_len_end = key_len_start.checked_add(2).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} props key len overflow", node_id))
    })?;
    let key_len_bytes = data.get(key_len_start..key_len_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} record too short for key length", node_id))
    })?;
    let key_len = u16::from_le_bytes(key_len_bytes.try_into().unwrap()) as usize;
    let props_len_start = key_len_end
        .checked_add(key_len)
        .and_then(|offset| offset.checked_add(8 + 8 + 4))
        .ok_or_else(|| {
            EngineError::CorruptRecord(format!("node {} props offset overflow", node_id))
        })?;
    let props_len_end = props_len_start.checked_add(4).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} props length offset overflow", node_id))
    })?;
    let props_len_bytes = data.get(props_len_start..props_len_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "node {} record too short for props length",
            node_id
        ))
    })?;
    let props_len = u32::from_le_bytes(props_len_bytes.try_into().unwrap()) as usize;
    let props_start = props_len_end;
    let props_end = props_start.checked_add(props_len).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} props length overflow", node_id))
    })?;
    let props_bytes = data.get(props_start..props_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} props range exceeds record data", node_id))
    })?;
    rmp_serde::from_slice(props_bytes).map_err(|error| {
        EngineError::SerializationError(format!("decode node {} props: {}", node_id, error))
    })
}

fn read_secondary_eq_group_counts(path: &Path) -> Result<Option<BTreeMap<u64, u64>>, EngineError> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "secondary equality sidecar {} missing header",
            path.display()
        )));
    }
    let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let index_len = count
        .checked_mul(20)
        .and_then(|len| len.checked_add(8))
        .ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality index length overflow".into())
        })?;
    if index_len > data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "secondary equality sidecar {} index exceeds file length",
            path.display()
        )));
    }
    let mut groups = BTreeMap::new();
    for i in 0..count {
        let off = 8 + i * 20;
        let value_hash = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let data_offset = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap()) as usize;
        let id_count = u32::from_le_bytes(data[off + 16..off + 20].try_into().unwrap()) as usize;
        let bytes = id_count.checked_mul(8).ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality group overflow".into())
        })?;
        let end = data_offset.checked_add(bytes).ok_or_else(|| {
            EngineError::CorruptRecord("secondary equality group overflow".into())
        })?;
        if end > data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "secondary equality sidecar {} group exceeds file length",
                path.display()
            )));
        }
        groups.insert(value_hash, id_count as u64);
    }
    Ok(Some(groups))
}

fn read_secondary_range_encoded_values(path: &Path) -> Result<Option<Vec<u64>>, EngineError> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "secondary range sidecar {} missing header",
            path.display()
        )));
    }
    let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let expected_len = count
        .checked_mul(16)
        .and_then(|len| len.checked_add(8))
        .ok_or_else(|| EngineError::CorruptRecord("secondary range length overflow".into()))?;
    if expected_len != data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "secondary range sidecar {} length mismatch",
            path.display()
        )));
    }
    let mut encoded_values = Vec::with_capacity(count);
    for i in 0..count {
        let off = 8 + i * 16;
        let encoded = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        encoded_values.push(encoded);
    }
    Ok(Some(encoded_values))
}

fn cleanup_stats_tmp(seg_dir: &Path) {
    let _ = fs::remove_file(seg_dir.join(PLANNER_STATS_TMP_FILENAME));
}

fn fsync_dir(dir: &Path) -> Result<(), EngineError> {
    #[cfg(not(target_os = "windows"))]
    {
        let d = File::open(dir)?;
        d.sync_all()?;
    }
    #[cfg(target_os = "windows")]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SecondaryIndexState;

    fn minimal_stats(segment_id: u64) -> SegmentPlannerStatsV1 {
        SegmentPlannerStatsV1 {
            format_version: PLANNER_STATS_FORMAT_VERSION,
            segment_id,
            build_kind: PlannerStatsBuildKind::Flush,
            built_at_ms: 0,
            declaration_fingerprint: 0,
            declared_indexes: Vec::new(),
            node_count: 1,
            edge_count: 0,
            truncated: false,
            general_property_stats_complete: true,
            general_property_sampled_node_count: 1,
            general_property_sampled_raw_bytes: 0,
            general_property_budget_exhausted: false,
            type_stats: vec![TypePlannerStats {
                type_id: 7,
                node_count: 1,
                min_node_id: Some(42),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1000),
                max_updated_at_ms: Some(1000),
            }],
            timestamp_stats: Vec::new(),
            property_stats: Vec::new(),
            equality_index_stats: Vec::new(),
            range_index_stats: Vec::new(),
            adjacency_stats: Vec::new(),
            node_id_sample: vec![42],
        }
    }

    #[test]
    fn stale_risk_classification_uses_bounded_sample_signals() {
        assert_eq!(
            classify_sample_stale_risk(100, 16, 0, 0, false),
            StalePostingRisk::Low
        );
        assert_eq!(
            classify_sample_stale_risk(100, 16, 5, 0, false),
            StalePostingRisk::High
        );
        assert_eq!(
            classify_sample_stale_risk(100, 16, 0, 1, false),
            StalePostingRisk::Medium
        );
        assert_eq!(
            classify_sample_stale_risk(100, 4, 0, 0, false),
            StalePostingRisk::Unknown
        );
        assert_eq!(
            classify_sample_stale_risk(100, 16, 0, 0, true),
            StalePostingRisk::Unknown
        );
        assert_eq!(
            classify_sample_stale_risk(2_000_000, 1024, 1, 0, false),
            StalePostingRisk::Medium
        );
    }

    #[test]
    fn adjacency_rollup_retains_capped_top_hubs() {
        let mut stats = minimal_stats(1);
        stats.edge_count = 4;
        stats.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_type_id: Some(10),
            source_node_count: 2,
            total_edges: 4,
            min_fanout: 1,
            max_fanout: 3,
            p50_fanout: 1,
            p90_fanout: 3,
            p99_fanout: 3,
            top_hubs: vec![
                NodeFanoutFrequency {
                    node_id: 7,
                    count: 3,
                },
                NodeFanoutFrequency {
                    node_id: 8,
                    count: 1,
                },
            ],
        });
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 1,
            edge_count: 4,
            availability: &available,
        }];
        let view = build_planner_stats_view_from_snapshots(1, &segments, &[]);
        let rollup = view
            .adjacency_rollups
            .get(&(PlannerStatsDirection::Outgoing, Some(10)))
            .unwrap();
        assert_eq!(
            rollup.top_hubs,
            vec![
                NodeFanoutFrequency {
                    node_id: 7,
                    count: 3,
                },
                NodeFanoutFrequency {
                    node_id: 8,
                    count: 1,
                },
            ]
        );
    }

    fn ready_eq_entry(index_id: u64, type_id: u32, prop_key: &str) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                type_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn ready_range_entry(
        index_id: u64,
        type_id: u32,
        prop_key: &str,
        domain: SecondaryIndexRangeDomain,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                type_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Range { domain },
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn add_eq_stats(
        stats: &mut SegmentPlannerStatsV1,
        index_id: u64,
        type_id: u32,
        prop_key: &str,
        total_postings: u64,
        value_group_count: u64,
        top_value_hashes: Vec<ValueFrequency>,
    ) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            index_id,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            type_id,
            prop_key: prop_key.to_string(),
            range_domain: None,
        });
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id,
            type_id,
            prop_key: prop_key.to_string(),
            total_postings,
            value_group_count,
            max_group_postings: top_value_hashes
                .iter()
                .map(|frequency| frequency.count)
                .max()
                .unwrap_or(0),
            top_value_hashes,
            sidecar_present_at_build: true,
        });
    }

    fn add_range_stats(
        stats: &mut SegmentPlannerStatsV1,
        index_id: u64,
        type_id: u32,
        prop_key: &str,
        domain: SecondaryIndexRangeDomain,
        total_entries: u64,
    ) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            index_id,
            kind: PlannerStatsDeclaredIndexKind::Range,
            type_id,
            prop_key: prop_key.to_string(),
            range_domain: Some(domain),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id,
            type_id,
            prop_key: prop_key.to_string(),
            domain,
            total_entries,
            min_encoded: Some(10),
            max_encoded: Some(20),
            buckets: vec![RangeBucket {
                upper_encoded: 20,
                count: total_entries,
            }],
            sidecar_present_at_build: true,
        });
    }

    fn stats_with_timestamp_histogram(
        segment_id: u64,
        type_id: u32,
        count: u64,
        min_ms: i64,
        max_ms: i64,
    ) -> SegmentPlannerStatsV1 {
        let mut stats = minimal_stats(segment_id);
        stats.node_count = count;
        stats.general_property_sampled_node_count = stats
            .general_property_sampled_node_count
            .min(stats.node_count);
        stats.type_stats = vec![TypePlannerStats {
            type_id,
            node_count: count,
            min_node_id: Some(segment_id.saturating_mul(1_000)),
            max_node_id: Some(segment_id.saturating_mul(1_000).saturating_add(count)),
            min_updated_at_ms: Some(min_ms),
            max_updated_at_ms: Some(max_ms),
        }];
        stats.timestamp_stats = vec![TimestampPlannerStats {
            type_id,
            count,
            min_ms,
            max_ms,
            buckets: vec![TimestampBucket {
                upper_ms: max_ms,
                count,
            }],
        }];
        stats
    }

    struct RangeHistogramInput {
        index_id: u64,
        type_id: u32,
        prop_key: &'static str,
        domain: SecondaryIndexRangeDomain,
        count: u64,
        min_encoded: u64,
        max_encoded: u64,
    }

    fn add_range_histogram_stats(stats: &mut SegmentPlannerStatsV1, input: RangeHistogramInput) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            index_id: input.index_id,
            kind: PlannerStatsDeclaredIndexKind::Range,
            type_id: input.type_id,
            prop_key: input.prop_key.to_string(),
            range_domain: Some(input.domain),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: input.index_id,
            type_id: input.type_id,
            prop_key: input.prop_key.to_string(),
            domain: input.domain,
            total_entries: input.count,
            min_encoded: Some(input.min_encoded),
            max_encoded: Some(input.max_encoded),
            buckets: vec![RangeBucket {
                upper_encoded: input.max_encoded,
                count: input.count,
            }],
            sidecar_present_at_build: true,
        });
    }

    #[test]
    fn envelope_round_trip() {
        let stats = minimal_stats(9);
        let data = encode_enveloped_stats(&stats).unwrap();
        let decoded = decode_planner_stats_envelope(&data, 9, 1, 0).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    #[cfg(unix)]
    fn atomic_write_cleanup_wrapper_removes_stale_tmp_on_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let tmp_path = dir.path().join(PLANNER_STATS_TMP_FILENAME);
        fs::write(&tmp_path, b"stale").unwrap();
        let mut perms = fs::metadata(&tmp_path).unwrap().permissions();
        perms.set_mode(0o444);
        fs::set_permissions(&tmp_path, perms).unwrap();

        let result =
            write_planner_stats_sidecar_atomic_cleanup_on_error(dir.path(), minimal_stats(9));
        assert!(result.is_err());
        assert!(!tmp_path.exists());
        assert!(!dir.path().join(PLANNER_STATS_FILENAME).exists());
    }

    #[test]
    fn envelope_rejects_bad_magic_version_crc_len_reserved_and_mismatch() {
        let stats = minimal_stats(9);
        let mut data = encode_enveloped_stats(&stats).unwrap();
        data[0] = b'X';
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("bad magic"));

        let mut data = encode_enveloped_stats(&stats).unwrap();
        data[8..12].copy_from_slice(&99u32.to_le_bytes());
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("unsupported"));

        let mut data = encode_enveloped_stats(&stats).unwrap();
        data[12..20].copy_from_slice(&1u64.to_le_bytes());
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("payload length"));

        let mut data = encode_enveloped_stats(&stats).unwrap();
        data[20] ^= 0xFF;
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("crc"));

        let mut data = encode_enveloped_stats(&stats).unwrap();
        data[24..28].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("reserved"));

        let data = encode_enveloped_stats(&stats).unwrap();
        assert!(decode_planner_stats_envelope(&data, 10, 1, 0)
            .unwrap_err()
            .contains("segment id"));

        let mut invalid_payload = Vec::new();
        invalid_payload.extend_from_slice(&PLANNER_STATS_MAGIC);
        invalid_payload.extend_from_slice(&PLANNER_STATS_FORMAT_VERSION.to_le_bytes());
        invalid_payload.extend_from_slice(&1u64.to_le_bytes());
        let mut crc = Crc32Hasher::new();
        crc.update(&[0xC1]);
        invalid_payload.extend_from_slice(&crc.finalize().to_le_bytes());
        invalid_payload.extend_from_slice(&0u32.to_le_bytes());
        invalid_payload.push(0xC1);
        assert!(decode_planner_stats_envelope(&invalid_payload, 9, 1, 0).is_err());
    }

    #[test]
    fn envelope_rejects_invalid_count_sanity() {
        let mut stats = minimal_stats(9);
        stats.general_property_sampled_node_count = 2;
        let data = encode_enveloped_stats(&stats).unwrap();
        assert!(decode_planner_stats_envelope(&data, 9, 1, 0)
            .unwrap_err()
            .contains("sampled node count"));
    }

    #[test]
    fn read_rejects_oversized_sidecar_before_decode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(PLANNER_STATS_FILENAME);
        let file = File::create(&path).unwrap();
        file.set_len((PLANNER_STATS_HARD_SIDECAR_BYTES + 1) as u64)
            .unwrap();
        drop(file);

        let availability = read_planner_stats_sidecar(dir.path(), 1, 0, 0);
        assert!(matches!(
            availability,
            PlannerStatsAvailability::Unavailable { reason } if reason.contains("hard cap")
        ));
    }

    #[test]
    fn envelope_rejects_internal_count_sanity_failures() {
        let mut bad_type_count = minimal_stats(9);
        bad_type_count.type_stats[0].node_count = 2;
        assert_decode_err_contains(bad_type_count, 1, 0, "type counts sum");

        let mut bad_property_count = minimal_stats(9);
        bad_property_count
            .property_stats
            .push(PropertyPlannerStats {
                type_id: 7,
                prop_key: "score".to_string(),
                tracked_reason: PropertyStatsTrackedReason::GeneralTopProperty,
                present_count: 2,
                null_count: 0,
                value_kind_counts: ValueKindCounts {
                    int_count: 2,
                    ..Default::default()
                },
                exact_distinct_count: Some(1),
                distinct_lower_bound: None,
                top_values: Vec::new(),
                numeric_summaries: Vec::new(),
            });
        assert_decode_err_contains(bad_property_count, 1, 0, "present count exceeds");

        let mut duplicate_property = minimal_stats(9);
        for tracked_reason in [
            PropertyStatsTrackedReason::DeclaredEquality,
            PropertyStatsTrackedReason::DeclaredRange,
        ] {
            duplicate_property
                .property_stats
                .push(PropertyPlannerStats {
                    type_id: 7,
                    prop_key: "score".to_string(),
                    tracked_reason,
                    present_count: 1,
                    null_count: 0,
                    value_kind_counts: ValueKindCounts {
                        int_count: 1,
                        ..Default::default()
                    },
                    exact_distinct_count: Some(1),
                    distinct_lower_bound: None,
                    top_values: Vec::new(),
                    numeric_summaries: Vec::new(),
                });
        }
        assert_decode_err_contains(duplicate_property, 1, 0, "appears more than once");

        let mut bad_timestamp_bucket = minimal_stats(9);
        bad_timestamp_bucket
            .timestamp_stats
            .push(TimestampPlannerStats {
                type_id: 7,
                count: 1,
                min_ms: 1000,
                max_ms: 1000,
                buckets: vec![TimestampBucket {
                    upper_ms: 1000,
                    count: 2,
                }],
            });
        assert_decode_err_contains(bad_timestamp_bucket, 1, 0, "timestamp buckets sum");

        let declared_eq = DeclaredIndexStatsFingerprint {
            index_id: 11,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            type_id: 7,
            prop_key: "color".to_string(),
            range_domain: None,
        };
        let mut bad_equality_count = minimal_stats(9);
        bad_equality_count.declared_indexes.push(declared_eq);
        bad_equality_count
            .equality_index_stats
            .push(EqualityIndexPlannerStats {
                index_id: 11,
                type_id: 7,
                prop_key: "color".to_string(),
                total_postings: 2,
                value_group_count: 1,
                max_group_postings: 2,
                top_value_hashes: Vec::new(),
                sidecar_present_at_build: true,
            });
        assert_decode_err_contains(bad_equality_count, 1, 0, "postings exceed");

        let declared_range = DeclaredIndexStatsFingerprint {
            index_id: 12,
            kind: PlannerStatsDeclaredIndexKind::Range,
            type_id: 7,
            prop_key: "score".to_string(),
            range_domain: Some(SecondaryIndexRangeDomain::Int),
        };
        let mut bad_range_bucket = minimal_stats(9);
        bad_range_bucket.declared_indexes.push(declared_range);
        bad_range_bucket
            .range_index_stats
            .push(RangeIndexPlannerStats {
                index_id: 12,
                type_id: 7,
                prop_key: "score".to_string(),
                domain: SecondaryIndexRangeDomain::Int,
                total_entries: 1,
                min_encoded: Some(1),
                max_encoded: Some(1),
                buckets: vec![RangeBucket {
                    upper_encoded: 1,
                    count: 2,
                }],
                sidecar_present_at_build: true,
            });
        assert_decode_err_contains(bad_range_bucket, 1, 0, "range buckets sum");

        let mut bad_adjacency = minimal_stats(9);
        bad_adjacency.edge_count = 1;
        bad_adjacency.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_type_id: None,
            source_node_count: 1,
            total_edges: 2,
            min_fanout: 1,
            max_fanout: 2,
            p50_fanout: 1,
            p90_fanout: 2,
            p99_fanout: 2,
            top_hubs: Vec::new(),
        });
        assert_decode_err_contains(bad_adjacency, 1, 1, "adjacency total exceeds");
    }

    fn assert_decode_err_contains(
        stats: SegmentPlannerStatsV1,
        expected_node_count: u64,
        expected_edge_count: u64,
        expected: &str,
    ) {
        let segment_id = stats.segment_id;
        let data = encode_enveloped_stats(&stats).unwrap();
        assert!(decode_planner_stats_envelope(
            &data,
            segment_id,
            expected_node_count,
            expected_edge_count,
        )
        .unwrap_err()
        .contains(expected));
    }

    #[test]
    fn bounded_property_candidate_tracker_keeps_late_frequent_key() {
        let mut tracker =
            PropertyKeyCandidateTracker::new(PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_TYPE);
        for idx in 0..PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_TYPE {
            tracker.observe(&format!("one_off_{:04}", idx));
        }
        for _ in 0..32 {
            tracker.observe("zz_late_hot");
        }

        let keys: Vec<_> = tracker.into_keys().collect();
        assert_eq!(
            keys.len(),
            PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_TYPE
        );
        assert!(keys.iter().any(|key| key == "zz_late_hot"));
    }

    #[test]
    fn rollup_tracks_family_coverage_and_declared_equality() {
        let red_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        stats.timestamp_stats.push(TimestampPlannerStats {
            type_id: 7,
            count: 1,
            min_ms: 1000,
            max_ms: 1000,
            buckets: vec![TimestampBucket {
                upper_ms: 1000,
                count: 1,
            }],
        });
        stats.property_stats.push(PropertyPlannerStats {
            type_id: 7,
            prop_key: "color".to_string(),
            tracked_reason: PropertyStatsTrackedReason::DeclaredEquality,
            present_count: 1,
            null_count: 0,
            value_kind_counts: ValueKindCounts {
                string_count: 1,
                ..Default::default()
            },
            exact_distinct_count: Some(1),
            distinct_lower_bound: None,
            top_values: vec![ValueFrequency {
                value_hash: red_hash,
                count: 1,
            }],
            numeric_summaries: Vec::new(),
        });
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            1,
            1,
            vec![ValueFrequency {
                value_hash: red_hash,
                count: 1,
            }],
        );
        add_range_stats(
            &mut stats,
            12,
            7,
            "score",
            SecondaryIndexRangeDomain::Int,
            1,
        );
        stats.edge_count = 1;
        stats.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_type_id: Some(5),
            source_node_count: 1,
            total_edges: 1,
            min_fanout: 1,
            max_fanout: 1,
            p50_fanout: 1,
            p90_fanout: 1,
            p99_fanout: 1,
            top_hubs: Vec::new(),
        });
        let missing = PlannerStatsAvailability::Missing;
        let unavailable = PlannerStatsAvailability::Unavailable {
            reason: "bad crc".to_string(),
        };
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 1,
                edge_count: 0,
                availability: &available,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 10,
                edge_count: 0,
                availability: &missing,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 3,
                node_count: 10,
                edge_count: 0,
                availability: &unavailable,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(
            44,
            &segments,
            &[
                ready_eq_entry(11, 7, "color"),
                ready_range_entry(12, 7, "score", SecondaryIndexRangeDomain::Int),
            ],
        );

        assert_eq!(view.generation, 44);
        assert_eq!(view.segment_count, 3);
        assert_eq!(view.available_segment_stats, 1);
        assert_eq!(view.missing_segment_stats, 1);
        assert_eq!(view.unavailable_segment_stats, 1);
        assert_eq!(view.full_rollup.node_count, 1);
        assert_eq!(view.full_rollup.coverage.covered_segment_ids, vec![1]);
        assert_eq!(view.full_rollup.coverage.uncovered_segment_ids, vec![2, 3]);
        assert_eq!(view.type_node_count(7), 1);
        assert_eq!(view.timestamp_coverage.covered_segment_ids, vec![1]);
        assert_eq!(view.type_rollups.get(&7).unwrap().type_id, 7);
        assert_eq!(view.timestamp_rollups.get(&7).unwrap().type_id, 7);
        let property = view
            .property_rollups
            .get(&(7, "color".to_string()))
            .unwrap();
        assert_eq!(property.type_id, 7);
        assert_eq!(property.prop_key, "color");
        assert_eq!(property.present_count, 1);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.type_id, 7);
        assert_eq!(equality.prop_key, "color");
        assert_eq!(equality.coverage.covered_segment_ids, vec![1]);
        assert_eq!(equality.coverage.uncovered_segment_ids, vec![2, 3]);
        assert_eq!(
            view.equality_segment_estimate(11, 1, &[red_hash]),
            Some(PlannerStatsValueEstimate {
                count: 1,
                exact: true,
            })
        );
        let range = view.range_index_rollups.get(&12).unwrap();
        assert_eq!(range.type_id, 7);
        assert_eq!(range.prop_key, "score");
        assert_eq!(range.domain, SecondaryIndexRangeDomain::Int);
        assert_eq!(range.total_entries, 1);
        let adjacency = view
            .adjacency_rollups
            .get(&(PlannerStatsDirection::Outgoing, Some(5)))
            .unwrap();
        assert_eq!(adjacency.direction, PlannerStatsDirection::Outgoing);
        assert_eq!(adjacency.edge_type_id, Some(5));
        assert_eq!(adjacency.total_edges, 1);
    }

    #[test]
    fn rollup_declared_index_mismatch_drops_only_index_block() {
        let red_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            1,
            1,
            vec![ValueFrequency {
                value_hash: red_hash,
                count: 1,
            }],
        );
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 1,
            edge_count: 0,
            availability: &available,
        }];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_eq_entry(11, 7, "status")],
        );

        assert_eq!(view.full_rollup.coverage.covered_segment_ids, vec![1]);
        assert_eq!(view.type_node_count(7), 1);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.coverage.mismatched_segment_ids, vec![1]);
        assert_eq!(view.equality_segment_estimate(11, 1, &[red_hash]), None);
    }

    #[test]
    fn rollup_declared_index_stats_require_runtime_sidecar_coverage() {
        let red_hash = hash_prop_value(&PropValue::String("red".to_string()));
        for state in [
            DeclaredIndexRuntimeCoverageState::Available,
            DeclaredIndexRuntimeCoverageState::Missing,
            DeclaredIndexRuntimeCoverageState::Corrupt,
        ] {
            let mut stats = minimal_stats(1);
            add_eq_stats(
                &mut stats,
                11,
                7,
                "color",
                1,
                1,
                vec![ValueFrequency {
                    value_hash: red_hash,
                    count: 1,
                }],
            );
            add_range_stats(
                &mut stats,
                12,
                7,
                "score",
                SecondaryIndexRangeDomain::Int,
                1,
            );
            let available = PlannerStatsAvailability::Available(Box::new(stats));
            let segments = vec![PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 1,
                edge_count: 0,
                availability: &available,
            }];
            let indexes = [
                ready_eq_entry(11, 7, "color"),
                ready_range_entry(12, 7, "score", SecondaryIndexRangeDomain::Int),
            ];
            let mut runtime_coverage = DeclaredIndexRuntimeCoverage::default();
            runtime_coverage.insert(1, 11, PlannerStatsDeclaredIndexKind::Equality, state);
            runtime_coverage.insert(1, 12, PlannerStatsDeclaredIndexKind::Range, state);

            let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
                1,
                &segments,
                &indexes,
                &runtime_coverage,
            );
            let equality = view.equality_index_rollups.get(&11).unwrap();
            let range = view.range_index_rollups.get(&12).unwrap();
            if state == DeclaredIndexRuntimeCoverageState::Available {
                assert_eq!(equality.coverage.covered_segment_ids, vec![1]);
                assert_eq!(range.coverage.covered_segment_ids, vec![1]);
                assert_eq!(
                    view.equality_segment_estimate(11, 1, &[red_hash]),
                    Some(PlannerStatsValueEstimate {
                        count: 1,
                        exact: true,
                    })
                );
                assert_eq!(range.total_entries, 1);
            } else {
                assert_eq!(equality.coverage.mismatched_segment_ids, vec![1]);
                assert_eq!(range.coverage.mismatched_segment_ids, vec![1]);
                assert_eq!(view.equality_segment_estimate(11, 1, &[red_hash]), None);
                assert_eq!(range.total_entries, 0);
            }
        }
    }

    #[test]
    fn rollup_declared_index_stats_treat_unknown_runtime_coverage_as_unavailable() {
        let red_hash = hash_prop_value(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            1,
            1,
            vec![ValueFrequency {
                value_hash: red_hash,
                count: 1,
            }],
        );
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 1,
            edge_count: 0,
            availability: &available,
        }];
        let runtime_coverage = DeclaredIndexRuntimeCoverage::default();

        let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
            1,
            &segments,
            &[ready_eq_entry(11, 7, "color")],
            &runtime_coverage,
        );

        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.coverage.mismatched_segment_ids, vec![1]);
        assert_eq!(view.equality_segment_estimate(11, 1, &[red_hash]), None);
    }

    #[test]
    fn rollup_declared_index_type_zero_is_valid_shape() {
        let mut stats = minimal_stats(1);
        add_eq_stats(&mut stats, 11, 0, "color", 0, 0, Vec::new());
        add_range_stats(
            &mut stats,
            12,
            0,
            "score",
            SecondaryIndexRangeDomain::Int,
            0,
        );
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 1,
            edge_count: 0,
            availability: &available,
        }];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[
                ready_eq_entry(11, 0, "color"),
                ready_range_entry(12, 0, "score", SecondaryIndexRangeDomain::Int),
            ],
        );

        assert_eq!(view.equality_index_rollups.get(&11).unwrap().type_id, 0);
        assert_eq!(view.range_index_rollups.get(&12).unwrap().type_id, 0);
    }

    #[test]
    fn rollup_range_and_timestamp_histograms_use_conservative_upper_estimates() {
        let mut stats = minimal_stats(1);
        stats.timestamp_stats = vec![TimestampPlannerStats {
            type_id: 7,
            count: 6,
            min_ms: 10,
            max_ms: 60,
            buckets: vec![
                TimestampBucket {
                    upper_ms: 20,
                    count: 2,
                },
                TimestampBucket {
                    upper_ms: 40,
                    count: 2,
                },
                TimestampBucket {
                    upper_ms: 60,
                    count: 2,
                },
            ],
        }];
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            index_id: 12,
            kind: PlannerStatsDeclaredIndexKind::Range,
            type_id: 7,
            prop_key: "score".to_string(),
            range_domain: Some(SecondaryIndexRangeDomain::Int),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: 12,
            type_id: 7,
            prop_key: "score".to_string(),
            domain: SecondaryIndexRangeDomain::Int,
            total_entries: 6,
            min_encoded: Some(10),
            max_encoded: Some(60),
            buckets: vec![
                RangeBucket {
                    upper_encoded: 20,
                    count: 2,
                },
                RangeBucket {
                    upper_encoded: 40,
                    count: 2,
                },
                RangeBucket {
                    upper_encoded: 60,
                    count: 2,
                },
            ],
            sidecar_present_at_build: true,
        });
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 6,
            edge_count: 0,
            availability: &available,
        }];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_range_entry(
                12,
                7,
                "score",
                SecondaryIndexRangeDomain::Int,
            )],
        );

        assert_eq!(
            view.range_index_estimate(12, Some((25, true)), Some((35, true))),
            Some(PlannerStatsValueEstimate {
                count: 2,
                exact: false,
            })
        );
        assert_eq!(
            view.timestamp_estimate(7, 25, 35),
            Some(PlannerStatsValueEstimate {
                count: 2,
                exact: false,
            })
        );
        assert_eq!(
            view.range_index_estimate(12, Some((100, true)), None),
            Some(PlannerStatsValueEstimate {
                count: 0,
                exact: true,
            })
        );
        assert_eq!(
            view.timestamp_estimate(7, i64::MIN, i64::MAX),
            Some(PlannerStatsValueEstimate {
                count: 6,
                exact: true,
            })
        );
    }

    #[test]
    fn range_rollup_estimate_sums_incompatible_segment_buckets_conservatively() {
        let mut stats_a = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        add_range_histogram_stats(
            &mut stats_a,
            RangeHistogramInput {
                index_id: 12,
                type_id: 7,
                prop_key: "score",
                domain: SecondaryIndexRangeDomain::Int,
                count: 100,
                min_encoded: 0,
                max_encoded: 100,
            },
        );
        let mut stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        add_range_histogram_stats(
            &mut stats_b,
            RangeHistogramInput {
                index_id: 12,
                type_id: 7,
                prop_key: "score",
                domain: SecondaryIndexRangeDomain::Int,
                count: 100,
                min_encoded: 0,
                max_encoded: 300,
            },
        );
        let available_a = PlannerStatsAvailability::Available(Box::new(stats_a));
        let available_b = PlannerStatsAvailability::Available(Box::new(stats_b));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available_a,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &available_b,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_range_entry(
                12,
                7,
                "score",
                SecondaryIndexRangeDomain::Int,
            )],
        );

        assert_eq!(
            view.range_index_estimate(12, Some((50, true)), Some((75, true))),
            Some(PlannerStatsValueEstimate {
                count: 200,
                exact: false,
            })
        );
    }

    #[test]
    fn timestamp_rollup_estimate_sums_incompatible_segment_buckets_conservatively() {
        let stats_a = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        let stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        let available_a = PlannerStatsAvailability::Available(Box::new(stats_a));
        let available_b = PlannerStatsAvailability::Available(Box::new(stats_b));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available_a,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &available_b,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(1, &segments, &[]);

        assert_eq!(
            view.timestamp_estimate(7, 50, 75),
            Some(PlannerStatsValueEstimate {
                count: 200,
                exact: false,
            })
        );
    }

    #[test]
    fn range_histogram_out_of_domain_still_exact_zero() {
        let mut stats_a = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        add_range_histogram_stats(
            &mut stats_a,
            RangeHistogramInput {
                index_id: 12,
                type_id: 7,
                prop_key: "score",
                domain: SecondaryIndexRangeDomain::Int,
                count: 100,
                min_encoded: 0,
                max_encoded: 100,
            },
        );
        let mut stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        add_range_histogram_stats(
            &mut stats_b,
            RangeHistogramInput {
                index_id: 12,
                type_id: 7,
                prop_key: "score",
                domain: SecondaryIndexRangeDomain::Int,
                count: 100,
                min_encoded: 0,
                max_encoded: 300,
            },
        );
        let available_a = PlannerStatsAvailability::Available(Box::new(stats_a));
        let available_b = PlannerStatsAvailability::Available(Box::new(stats_b));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available_a,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &available_b,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_range_entry(
                12,
                7,
                "score",
                SecondaryIndexRangeDomain::Int,
            )],
        );

        assert_eq!(
            view.range_index_estimate(12, Some((301, true)), None),
            Some(PlannerStatsValueEstimate {
                count: 0,
                exact: true,
            })
        );
    }

    #[test]
    fn timestamp_histogram_out_of_domain_still_exact_zero() {
        let stats_a = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        let stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        let available_a = PlannerStatsAvailability::Available(Box::new(stats_a));
        let available_b = PlannerStatsAvailability::Available(Box::new(stats_b));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available_a,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &available_b,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(1, &segments, &[]);

        assert_eq!(
            view.timestamp_estimate(7, 301, i64::MAX),
            Some(PlannerStatsValueEstimate {
                count: 0,
                exact: true,
            })
        );
    }

    #[test]
    fn mixed_covered_uncovered_range_estimate_does_not_double_count() {
        let mut stats = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        add_range_histogram_stats(
            &mut stats,
            RangeHistogramInput {
                index_id: 12,
                type_id: 7,
                prop_key: "score",
                domain: SecondaryIndexRangeDomain::Int,
                count: 100,
                min_encoded: 0,
                max_encoded: 100,
            },
        );
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let missing = PlannerStatsAvailability::Missing;
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &missing,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_range_entry(
                12,
                7,
                "score",
                SecondaryIndexRangeDomain::Int,
            )],
        );

        let range = view.range_index_rollups.get(&12).unwrap();
        assert_eq!(range.coverage.covered_segment_ids, vec![1]);
        assert_eq!(range.coverage.uncovered_segment_ids, vec![2]);
        assert_eq!(
            view.range_index_estimate(12, Some((50, true)), Some((75, true))),
            Some(PlannerStatsValueEstimate {
                count: 100,
                exact: false,
            })
        );
    }

    #[test]
    fn mixed_covered_uncovered_timestamp_estimate_does_not_double_count() {
        let stats = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let missing = PlannerStatsAvailability::Missing;
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 100,
                edge_count: 0,
                availability: &available,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 100,
                edge_count: 0,
                availability: &missing,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(1, &segments, &[]);

        let timestamp = view.timestamp_rollups.get(&7).unwrap();
        assert_eq!(timestamp.coverage.covered_segment_ids, vec![1]);
        assert_eq!(timestamp.coverage.uncovered_segment_ids, vec![2]);
        assert_eq!(
            view.timestamp_estimate(7, 50, 75),
            Some(PlannerStatsValueEstimate {
                count: 100,
                exact: false,
            })
        );
        assert!(view.timestamp_covers_segment(7, 1));
        assert!(!view.timestamp_covers_segment(7, 2));
    }

    #[test]
    fn timestamp_absent_type_is_exact_zero_only_when_type_stats_cover_segments() {
        let stats = stats_with_timestamp_histogram(1, 7, 100, 0, 100);
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 100,
            edge_count: 0,
            availability: &available,
        }];

        let view = build_planner_stats_view_from_snapshots(1, &segments, &[]);

        assert_eq!(
            view.timestamp_estimate(99, i64::MIN, i64::MAX),
            Some(PlannerStatsValueEstimate {
                count: 0,
                exact: true,
            })
        );
        assert!(view.timestamp_covers_segment(99, 1));
    }

    #[test]
    fn rollup_equality_residual_and_overflow_are_deterministic() {
        let hot_hash = 10;
        let cold_hash = 20;
        let mut stats = minimal_stats(1);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            u64::MAX - 4,
            3,
            vec![ValueFrequency {
                value_hash: hot_hash,
                count: 5,
            }],
        );
        let mut second = minimal_stats(2);
        add_eq_stats(
            &mut second,
            11,
            7,
            "color",
            10,
            1,
            vec![ValueFrequency {
                value_hash: hot_hash,
                count: 10,
            }],
        );
        let first = PlannerStatsAvailability::Available(Box::new(stats));
        let second = PlannerStatsAvailability::Available(Box::new(second));
        let segments = vec![
            PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 1,
                edge_count: 0,
                availability: &first,
            },
            PlannerStatsSegmentSnapshot {
                segment_id: 2,
                node_count: 1,
                edge_count: 0,
                availability: &second,
            },
        ];

        let view = build_planner_stats_view_from_snapshots(
            1,
            &segments,
            &[ready_eq_entry(11, 7, "color")],
        );

        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.total_postings, u64::MAX);
        assert_eq!(
            view.equality_segment_estimate(11, 1, &[hot_hash]),
            Some(PlannerStatsValueEstimate {
                count: 5,
                exact: true,
            })
        );
        assert_eq!(
            view.equality_segment_estimate(11, 1, &[cold_hash]),
            Some(PlannerStatsValueEstimate {
                count: (u64::MAX - 9) / 2,
                exact: false,
            })
        );
        assert_eq!(
            view.equality_segment_estimate(11, 2, &[cold_hash]),
            Some(PlannerStatsValueEstimate {
                count: 0,
                exact: true,
            })
        );
    }

    #[test]
    fn rollup_many_stats_bearing_segments_guard_without_wall_clock() {
        let value_hash = hash_prop_value(&PropValue::String("active".to_string()));
        let mut availabilities = Vec::new();
        for segment_id in 1..=256 {
            let mut stats = minimal_stats(segment_id);
            add_eq_stats(
                &mut stats,
                11,
                7,
                "status",
                1,
                1,
                vec![ValueFrequency {
                    value_hash,
                    count: 1,
                }],
            );
            availabilities.push(PlannerStatsAvailability::Available(Box::new(stats)));
        }
        let snapshots: Vec<_> = availabilities
            .iter()
            .enumerate()
            .map(|(idx, availability)| PlannerStatsSegmentSnapshot {
                segment_id: (idx + 1) as u64,
                node_count: 1,
                edge_count: 0,
                availability,
            })
            .collect();

        let view = build_planner_stats_view_from_snapshots(
            9,
            &snapshots,
            &[ready_eq_entry(11, 7, "status")],
        );

        assert_eq!(view.segment_count, 256);
        assert_eq!(view.available_segment_stats, 256);
        assert_eq!(view.full_rollup.node_count, 256);
        assert_eq!(view.type_node_count(7), 256);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.coverage.covered_segment_ids.len(), 256);
        assert_eq!(equality.total_postings, 256);
        assert_eq!(equality.top_value_hashes.get(&value_hash), Some(&256));
    }

    #[test]
    fn rollup_many_ready_indexes_leaves_missing_blocks_uncovered() {
        let value_hash = hash_prop_value(&PropValue::String("active".to_string()));
        let mut stats = minimal_stats(1);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "status",
            1,
            1,
            vec![ValueFrequency {
                value_hash,
                count: 1,
            }],
        );
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 1,
            edge_count: 0,
            availability: &available,
        }];
        let mut indexes = vec![ready_eq_entry(11, 7, "status")];
        for index_id in 100..228 {
            indexes.push(ready_eq_entry(index_id, 7, &format!("status_{index_id}")));
        }

        let view = build_planner_stats_view_from_snapshots(1, &segments, &indexes);

        assert_eq!(view.equality_index_rollups.len(), 129);
        let covered = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(covered.coverage.covered_segment_ids, vec![1]);
        assert_eq!(covered.total_postings, 1);
        let missing = view.equality_index_rollups.get(&100).unwrap();
        assert_eq!(missing.coverage.uncovered_segment_ids, vec![1]);
        assert!(missing.coverage.mismatched_segment_ids.is_empty());
        assert_eq!(missing.total_postings, 0);
    }

    #[test]
    fn histograms_are_deterministic_equi_depth() {
        let values = [1, 2, 3, 4, 5, 6, 7, 8];
        let buckets = range_buckets(&values, 4);
        assert_eq!(
            buckets,
            vec![
                RangeBucket {
                    upper_encoded: 2,
                    count: 2
                },
                RangeBucket {
                    upper_encoded: 4,
                    count: 2
                },
                RangeBucket {
                    upper_encoded: 6,
                    count: 2
                },
                RangeBucket {
                    upper_encoded: 8,
                    count: 2
                },
            ]
        );
    }

    #[test]
    fn size_reduction_preserves_core_before_skip() {
        let mut stats = minimal_stats(1);
        stats.property_stats.push(PropertyPlannerStats {
            type_id: 1,
            prop_key: "large_general".to_string(),
            tracked_reason: PropertyStatsTrackedReason::GeneralTopProperty,
            present_count: 1,
            null_count: 0,
            value_kind_counts: ValueKindCounts::default(),
            exact_distinct_count: Some(1),
            distinct_lower_bound: None,
            top_values: (0..128)
                .map(|value_hash| ValueFrequency {
                    value_hash,
                    count: 1,
                })
                .collect(),
            numeric_summaries: Vec::new(),
        });
        let encoded = serialize_stats_with_limits(stats.clone(), 128, 4096)
            .unwrap()
            .expect("core stats should fit");
        let reduced = decode_planner_stats_envelope(&encoded, 1, 1, 0).unwrap();
        assert!(reduced.truncated);
        assert!(reduced.property_stats.is_empty());
        assert_eq!(reduced.type_stats, stats.type_stats);

        assert!(serialize_stats_with_limits(stats, 64, 64)
            .unwrap()
            .is_none());
    }

    #[test]
    fn size_reduction_drops_adjacency_before_declared_index_detail() {
        let mut stats = minimal_stats(1);
        stats.edge_count = 1;
        stats.declared_indexes = vec![
            DeclaredIndexStatsFingerprint {
                index_id: 11,
                kind: PlannerStatsDeclaredIndexKind::Equality,
                type_id: 7,
                prop_key: "color".to_string(),
                range_domain: None,
            },
            DeclaredIndexStatsFingerprint {
                index_id: 12,
                kind: PlannerStatsDeclaredIndexKind::Range,
                type_id: 7,
                prop_key: "score".to_string(),
                range_domain: Some(SecondaryIndexRangeDomain::Int),
            },
        ];
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id: 11,
            type_id: 7,
            prop_key: "color".to_string(),
            total_postings: 1,
            value_group_count: 1,
            max_group_postings: 1,
            top_value_hashes: vec![ValueFrequency {
                value_hash: 100,
                count: 1,
            }],
            sidecar_present_at_build: true,
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: 12,
            type_id: 7,
            prop_key: "score".to_string(),
            domain: SecondaryIndexRangeDomain::Int,
            total_entries: 1,
            min_encoded: Some(10),
            max_encoded: Some(10),
            buckets: vec![RangeBucket {
                upper_encoded: 10,
                count: 1,
            }],
            sidecar_present_at_build: true,
        });
        stats.property_stats.push(PropertyPlannerStats {
            type_id: 7,
            prop_key: "general".to_string(),
            tracked_reason: PropertyStatsTrackedReason::GeneralTopProperty,
            present_count: 1,
            null_count: 0,
            value_kind_counts: ValueKindCounts {
                string_count: 1,
                ..Default::default()
            },
            exact_distinct_count: Some(1),
            distinct_lower_bound: None,
            top_values: (0..128)
                .map(|value_hash| ValueFrequency {
                    value_hash,
                    count: 1,
                })
                .collect(),
            numeric_summaries: Vec::new(),
        });
        for edge_type_id in 0..256 {
            stats.adjacency_stats.push(AdjacencyPlannerStats {
                direction: PlannerStatsDirection::Outgoing,
                edge_type_id: Some(edge_type_id),
                source_node_count: 1,
                total_edges: 1,
                min_fanout: 1,
                max_fanout: 1,
                p50_fanout: 1,
                p90_fanout: 1,
                p99_fanout: 1,
                top_hubs: Vec::new(),
            });
        }

        let mut after_general = stats.clone();
        assert!(apply_reduction_step(
            &mut after_general,
            ReductionStep::GeneralProperties
        ));
        let mut after_hubs = after_general.clone();
        apply_reduction_step(&mut after_hubs, ReductionStep::AdjacencyHubSamples);
        let after_hubs_len = encode_enveloped_stats(&after_hubs).unwrap().len();
        let mut after_adjacency = after_hubs.clone();
        assert!(apply_reduction_step(
            &mut after_adjacency,
            ReductionStep::AdjacencyStats
        ));
        let after_adjacency_len = encode_enveloped_stats(&after_adjacency).unwrap().len();
        assert!(after_hubs_len > after_adjacency_len);

        let soft_limit = after_adjacency_len + ((after_hubs_len - after_adjacency_len) / 2);
        let encoded = serialize_stats_with_limits(stats, soft_limit, usize::MAX)
            .unwrap()
            .expect("reduced stats should fit");
        let reduced = decode_planner_stats_envelope(&encoded, 1, 1, 1).unwrap();
        assert!(reduced.truncated);
        assert!(reduced.adjacency_stats.is_empty());
        assert!(reduced.property_stats.is_empty());
        assert_eq!(reduced.equality_index_stats[0].top_value_hashes.len(), 1);
        assert_eq!(reduced.range_index_stats[0].buckets.len(), 1);
    }
}
