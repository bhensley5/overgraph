use crate::error::EngineError;
use crate::property_value_semantics::{
    hash_prop_equality_key, numeric_range_sort_key_for_value, NumericRangeSortKey,
    NUMERIC_RANGE_KEY_BYTES,
};
use crate::secondary_index_key::{decode_compound_tuple_components, CompoundComponentClass};
#[cfg(test)]
use crate::secondary_index_key::{
    encode_compound_tuple_key, for_each_compound_sidecar_entry, CompoundFieldValue,
    CompoundSidecarDeclaration, CompoundTupleContext,
};
use crate::segment_components::secondary_index_declaration_fingerprint_for_entry;
#[cfg(test)]
use crate::segment_components::{
    decode_identity_header, COMPONENT_IDENTITY_HEADER_LEN, COMPONENT_IDENTITY_HEADER_MAGIC,
};
use crate::segment_reader::SegmentReader;
use crate::segment_writer::{
    publish_planner_stats_component_payload_from_latest, CompactEdgeMeta, CompactNodeMeta,
};
use crate::types::{
    EdgeRecord, NodeIdMap, NodeRecord, PropValue, SecondaryIndexFieldManifest, SecondaryIndexKind,
    SecondaryIndexManifestEntry, SecondaryIndexState, SecondaryIndexTarget,
    MAX_NODE_LABELS_PER_NODE, MAX_SECONDARY_INDEX_FIELDS,
};
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::fs::File;
#[cfg(test)]
use std::io::Read;
#[cfg(all(test, unix))]
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub(crate) const PLANNER_STATS_FILENAME: &str = "planner_stats.dat";
#[cfg(all(test, unix))]
const PLANNER_STATS_TMP_FILENAME: &str = "planner_stats.tmp";
const PLANNER_STATS_MAGIC: [u8; 8] = *b"OGPST01\0";
pub(crate) const PLANNER_STATS_FORMAT_VERSION: u32 = 2;
const PLANNER_STATS_ENVELOPE_LEN: usize = 8 + 4 + 8 + 4 + 4;

pub(crate) const PLANNER_STATS_MAX_PROPERTY_KEYS_PER_LABEL: usize = 256;
const PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_LABEL: usize = 1024;
pub(crate) const PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY: usize = 32;
pub(crate) const PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES: usize = 4096;
pub(crate) const PLANNER_STATS_RANGE_BUCKETS: usize = 64;
pub(crate) const PLANNER_STATS_TIMESTAMP_BUCKETS: usize = 64;
pub(crate) const PLANNER_STATS_NODE_ID_SAMPLE_SIZE: usize = 1024;
pub(crate) const PLANNER_STATS_TOP_HUBS_PER_EDGE_LABEL: usize = 32;
pub(crate) const PLANNER_STATS_SOFT_SIDECAR_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const PLANNER_STATS_HARD_SIDECAR_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const PLANNER_STATS_HARD_CANDIDATE_CAP: usize = 65_536;
pub(crate) const PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP: usize = 4096;
pub(crate) const COMPOUND_STATS_EXACT_PREFIX_LIMIT: usize = 4096;
pub(crate) const PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_NODES: usize = 1024;
pub(crate) const PLANNER_STATS_COMPACTION_GENERAL_PROP_DECODE_BUDGET_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const PLANNER_STATS_REFRESH_GENERAL_PROP_DECODE_BUDGET_NODES: usize = 0;
pub(crate) type RangeStatsKey = [u8; NUMERIC_RANGE_KEY_BYTES];

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

#[derive(
    Clone, Copy, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) enum PlannerStatsDeclaredIndexKind {
    #[default]
    Equality,
    Range,
}

#[derive(
    Clone, Copy, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) enum PlannerStatsDeclaredIndexTarget {
    #[default]
    NodeProperty,
    EdgeProperty,
    NodeFieldIndex,
    EdgeFieldIndex,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DeclaredIndexRuntimeCoverageState {
    Available,
    Missing,
    Corrupt,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeclaredIndexRuntimeCoverageKey {
    pub segment_id: u64,
    pub index_id: u64,
    pub target: PlannerStatsDeclaredIndexTarget,
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
            let target = planner_stats_declared_index_target(entry);
            let kind = match entry.kind {
                SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
                SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
            };
            for segment in segments {
                coverage.insert(
                    segment.segment_id,
                    entry.index_id,
                    target,
                    kind,
                    segment.declared_index_runtime_coverage_state_for_target(
                        entry.index_id,
                        target,
                        kind,
                    ),
                );
            }
        }
        coverage
    }

    pub(crate) fn insert(
        &mut self,
        segment_id: u64,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        kind: PlannerStatsDeclaredIndexKind,
        state: DeclaredIndexRuntimeCoverageState,
    ) {
        self.states.insert(
            DeclaredIndexRuntimeCoverageKey {
                segment_id,
                index_id,
                target,
                kind,
            },
            state,
        );
    }

    pub(crate) fn state(
        &self,
        segment_id: u64,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> DeclaredIndexRuntimeCoverageState {
        self.states
            .get(&DeclaredIndexRuntimeCoverageKey {
                segment_id,
                index_id,
                target,
                kind,
            })
            .copied()
            .unwrap_or(DeclaredIndexRuntimeCoverageState::Unknown)
    }

    pub(crate) fn is_available(
        &self,
        segment_id: u64,
        index_id: u64,
        target: PlannerStatsDeclaredIndexTarget,
        kind: PlannerStatsDeclaredIndexKind,
    ) -> bool {
        self.state(segment_id, index_id, target, kind)
            == DeclaredIndexRuntimeCoverageState::Available
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.states.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DeclaredIndexStatsFingerprint {
    pub index_id: u64,
    #[serde(default)]
    pub target: PlannerStatsDeclaredIndexTarget,
    pub kind: PlannerStatsDeclaredIndexKind,
    pub target_label_id: u32,
    pub prop_key: String,
    #[serde(default)]
    pub field_fingerprint: u64,
    #[serde(default)]
    pub field_count: u16,
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
    pub node_label_stats: Vec<NodeLabelPlannerStats>,
    pub timestamp_stats: Vec<TimestampPlannerStats>,
    pub property_stats: Vec<PropertyPlannerStats>,
    pub equality_index_stats: Vec<EqualityIndexPlannerStats>,
    pub range_index_stats: Vec<RangeIndexPlannerStats>,
    pub adjacency_stats: Vec<AdjacencyPlannerStats>,
    pub node_id_sample: Vec<u64>,
    #[serde(default)]
    pub compound_index_stats: Vec<CompoundIndexPlannerStats>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StatsCorePartial {
    pub node_count: u64,
    pub edge_count: u64,
    pub truncated: bool,
    pub general_property_stats_complete: bool,
    pub general_property_sampled_node_count: u64,
    pub general_property_sampled_raw_bytes: u64,
    pub general_property_budget_exhausted: bool,
    pub node_label_stats: Vec<NodeLabelPlannerStats>,
    pub timestamp_stats: Vec<TimestampPlannerStats>,
    pub property_stats: Vec<PropertyPlannerStats>,
    pub adjacency_stats: Vec<AdjacencyPlannerStats>,
    pub node_id_sample: Vec<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DeclaredIndexStatsEvidence {
    pub equality_index_stats: Vec<EqualityIndexPlannerStats>,
    pub range_index_stats: Vec<RangeIndexPlannerStats>,
    pub compound_index_stats: Vec<CompoundIndexPlannerStats>,
}

impl DeclaredIndexStatsEvidence {
    pub(crate) fn sort(&mut self) {
        self.equality_index_stats
            .sort_by_key(|stats| stats.index_id);
        self.range_index_stats.sort_by_key(|stats| stats.index_id);
        self.compound_index_stats
            .sort_by_key(|stats| (stats.index_id, declared_index_kind_rank(stats.kind)));
    }

    pub(crate) fn extend(&mut self, mut other: DeclaredIndexStatsEvidence) {
        self.equality_index_stats
            .append(&mut other.equality_index_stats);
        self.range_index_stats.append(&mut other.range_index_stats);
        self.compound_index_stats
            .append(&mut other.compound_index_stats);
        self.sort();
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeLabelPlannerStats {
    pub label_id: u32,
    pub node_count: u64,
    pub min_node_id: Option<u64>,
    pub max_node_id: Option<u64>,
    pub min_updated_at_ms: Option<i64>,
    pub max_updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TimestampPlannerStats {
    pub label_id: u32,
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
    pub label_id: u32,
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
    pub count: u64,
    pub min_key: Option<RangeStatsKey>,
    pub max_key: Option<RangeStatsKey>,
    pub buckets: Vec<RangeBucket>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct EqualityIndexPlannerStats {
    pub index_id: u64,
    pub target_label_id: u32,
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
    pub target_label_id: u32,
    pub prop_key: String,
    pub total_entries: u64,
    pub min_key: Option<RangeStatsKey>,
    pub max_key: Option<RangeStatsKey>,
    pub buckets: Vec<RangeBucket>,
    pub sidecar_present_at_build: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct CompoundIndexPlannerStats {
    pub index_id: u64,
    pub target: PlannerStatsDeclaredIndexTarget,
    pub target_label_id: u32,
    pub kind: PlannerStatsDeclaredIndexKind,
    pub field_fingerprint: u64,
    pub field_count: u16,
    pub total_postings: u64,
    pub distinct_full_keys: u64,
    pub prefix_stats: Vec<CompoundPrefixStats>,
    pub range_stats: Vec<CompoundRangeStats>,
    pub coverage: DeclaredIndexRuntimeCoverageState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CompoundPrefixStats {
    pub prefix_len: u16,
    pub distinct_prefixes: u64,
    pub max_postings_per_prefix: u64,
    pub exact_prefix_postings: Vec<CompoundExactPrefixStat>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CompoundExactPrefixStat {
    pub encoded_prefix: Vec<u8>,
    pub postings: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct CompoundRangeStats {
    pub equality_prefix_len: u16,
    pub range_field_ordinal: u16,
    pub total_numeric_entries: u64,
    pub min_key: Option<RangeStatsKey>,
    pub max_key: Option<RangeStatsKey>,
    pub buckets: Vec<RangeBucket>,
}

impl CompoundRangeStats {
    /// Bound-aware posting estimate over this block's numeric histogram for
    /// the query's actual range bounds (keys use the sidecar numeric range
    /// key encoding).
    pub(crate) fn estimate_range_postings(
        &self,
        lower: Option<(RangeStatsKey, bool)>,
        upper: Option<(RangeStatsKey, bool)>,
    ) -> PlannerStatsValueEstimate {
        estimate_range_key_histogram(
            self.total_numeric_entries,
            self.min_key,
            self.max_key,
            self.buckets
                .iter()
                .map(|bucket| (bucket.upper_key, bucket.count)),
            lower,
            upper,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RangeBucket {
    pub upper_key: RangeStatsKey,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct AdjacencyPlannerStats {
    pub direction: PlannerStatsDirection,
    pub edge_label_id: Option<u32>,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StalePostingRisk {
    Low,
    Medium,
    High,
    // An absent risk classification must never read as safe.
    #[default]
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
    pub node_label_coverage: PlannerStatsFamilyCoverage,
    pub timestamp_coverage: PlannerStatsFamilyCoverage,
    pub property_rollups: BTreeMap<(u32, String), PropertyRollupStats>,
    pub node_label_rollups: BTreeMap<u32, NodeLabelRollupStats>,
    pub timestamp_rollups: BTreeMap<u32, TimestampRollupStats>,
    pub equality_index_rollups: BTreeMap<u64, EqualityIndexRollupStats>,
    pub range_index_rollups: BTreeMap<u64, RangeIndexRollupStats>,
    pub compound_index_rollups: BTreeMap<u64, CompoundIndexRollupStats>,
    pub adjacency_rollups: BTreeMap<(PlannerStatsDirection, Option<u32>), AdjacencyRollupStats>,
    pub segment_stale_risks: BTreeMap<u64, StalePostingRisk>,
    /// Highest-ranked risk in `segment_stale_risks`, precomputed at view
    /// build so per-estimate calls do not iterate every segment.
    pub max_segment_stale_risk: StalePostingRisk,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FullRollupStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub coverage: PlannerStatsFamilyCoverage,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLabelRollupStats {
    pub label_id: u32,
    pub node_count: u64,
    pub min_node_id: Option<u64>,
    pub max_node_id: Option<u64>,
    pub min_updated_at_ms: Option<i64>,
    pub max_updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TimestampRollupStats {
    pub label_id: u32,
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
    pub label_id: u32,
    pub prop_key: String,
    pub present_count: u64,
    pub null_count: u64,
    pub top_values: BTreeMap<u64, u64>,
    pub coverage: PlannerStatsFamilyCoverage,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EqualityIndexRollupStats {
    pub index_id: u64,
    pub target_label_id: u32,
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

#[derive(Clone, Debug, Default)]
pub(crate) struct RangeIndexRollupStats {
    pub index_id: u64,
    pub target_label_id: u32,
    pub prop_key: String,
    pub total_entries: u64,
    pub min_key: Option<RangeStatsKey>,
    pub max_key: Option<RangeStatsKey>,
    pub coverage: PlannerStatsFamilyCoverage,
    segment_rollups: BTreeMap<u64, RangeIndexSegmentRollupStats>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
struct RangeIndexSegmentRollupStats {
    total_entries: u64,
    min_key: Option<RangeStatsKey>,
    max_key: Option<RangeStatsKey>,
    buckets: Vec<RangeBucket>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct CompoundIndexRollupStats {
    pub index_id: u64,
    pub target: PlannerStatsDeclaredIndexTarget,
    pub target_label_id: u32,
    pub kind: PlannerStatsDeclaredIndexKind,
    pub field_fingerprint: u64,
    pub field_count: u16,
    pub total_postings: u64,
    pub distinct_full_keys: u64,
    pub prefix_stats: Vec<CompoundPrefixStats>,
    pub range_stats: Vec<CompoundRangeStats>,
    pub coverage: PlannerStatsFamilyCoverage,
    segment_rollups: BTreeMap<u64, CompoundIndexPlannerStats>,
    /// Prefix lengths whose merged exact-prefix map overflowed the cap (or
    /// merged a segment whose own exact list was incomplete). Overflow is
    /// sticky: later segments must not rebuild a partial map that costing
    /// would trust as exact.
    exact_overflowed_prefix_lens: BTreeSet<u16>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AdjacencyRollupStats {
    pub direction: PlannerStatsDirection,
    pub edge_label_id: Option<u32>,
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
        view.set_segment_stale_risks(segment_stale_risks);
        view
    }

    /// Single mutation point for segment stale risks: keeps the precomputed
    /// max in lockstep with the map so estimates can never read a stale max.
    pub(crate) fn set_segment_stale_risks(&mut self, risks: BTreeMap<u64, StalePostingRisk>) {
        self.max_segment_stale_risk = risks
            .values()
            .copied()
            .max_by_key(|risk| risk.rank())
            .unwrap_or(StalePostingRisk::Unknown);
        self.segment_stale_risks = risks;
    }

    pub(crate) fn node_label_count(&self, label_id: u32) -> u64 {
        self.node_label_rollups
            .get(&label_id)
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

    #[allow(dead_code)]
    pub(crate) fn range_index_estimate(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
    ) -> Option<PlannerStatsValueEstimate> {
        let rollup = self.range_index_rollups.get(&index_id)?;
        if rollup.segment_rollups.is_empty() {
            return None;
        }
        let lower = lower.map(|(key, inclusive)| (range_stats_key(key), inclusive));
        let upper = upper.map(|(key, inclusive)| (range_stats_key(key), inclusive));
        let mut count = 0u64;
        let mut exact = !rollup.coverage.has_uncovered();
        for segment in rollup.segment_rollups.values() {
            let estimate = estimate_range_key_histogram(
                segment.total_entries,
                segment.min_key,
                segment.max_key,
                segment
                    .buckets
                    .iter()
                    .map(|bucket| (bucket.upper_key, bucket.count)),
                lower,
                upper,
            );
            count = count.saturating_add(estimate.count);
            exact &= estimate.exact;
        }
        Some(PlannerStatsValueEstimate { count, exact })
    }

    pub(crate) fn range_segment_estimate(
        &self,
        index_id: u64,
        segment_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
    ) -> Option<PlannerStatsValueEstimate> {
        let rollup = self.range_index_rollups.get(&index_id)?;
        let segment = rollup.segment_rollups.get(&segment_id)?;
        let lower = lower.map(|(key, inclusive)| (range_stats_key(key), inclusive));
        let upper = upper.map(|(key, inclusive)| (range_stats_key(key), inclusive));
        Some(estimate_range_key_histogram(
            segment.total_entries,
            segment.min_key,
            segment.max_key,
            segment
                .buckets
                .iter()
                .map(|bucket| (bucket.upper_key, bucket.count)),
            lower,
            upper,
        ))
    }

    pub(crate) fn timestamp_estimate(
        &self,
        label_id: u32,
        lower_ms: i64,
        upper_ms: i64,
    ) -> Option<PlannerStatsValueEstimate> {
        let Some(rollup) = self.timestamp_rollups.get(&label_id) else {
            if self.node_label_count(label_id) == 0 && self.node_label_coverage.covered_count() > 0
            {
                return Some(PlannerStatsValueEstimate {
                    count: 0,
                    exact: !self.node_label_coverage.has_uncovered(),
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

    pub(crate) fn timestamp_covers_segment(&self, label_id: u32, segment_id: u64) -> bool {
        self.timestamp_rollups.get(&label_id).map_or_else(
            || self.node_label_count(label_id) == 0 && self.node_label_coverage.covers(segment_id),
            |rollup| rollup.coverage.covers(segment_id),
        )
    }

    pub(crate) fn max_segment_stale_risk(&self) -> StalePostingRisk {
        self.max_segment_stale_risk
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
            self.node_label_coverage.covered_count() <= self.segment_count,
            "node label stats coverage exceeds segment count"
        );
        debug_assert!(
            self.timestamp_coverage.covered_count() <= self.segment_count,
            "timestamp stats coverage exceeds segment count"
        );
        for (label_id, rollup) in &self.node_label_rollups {
            debug_assert_eq!(*label_id, rollup.label_id);
        }
        for (label_id, rollup) in &self.timestamp_rollups {
            debug_assert_eq!(*label_id, rollup.label_id);
        }
        for ((label_id, prop_key), rollup) in &self.property_rollups {
            debug_assert_eq!(*label_id, rollup.label_id);
            debug_assert_eq!(prop_key, &rollup.prop_key);
        }
        for (index_id, rollup) in &self.equality_index_rollups {
            debug_assert_eq!(*index_id, rollup.index_id);
            debug_assert!(
                !rollup.prop_key.is_empty(),
                "equality rollup for target label {} must have property key",
                rollup.target_label_id
            );
        }
        for (index_id, rollup) in &self.range_index_rollups {
            debug_assert_eq!(*index_id, rollup.index_id);
            debug_assert!(
                !rollup.prop_key.is_empty(),
                "range rollup for target label {} must have property key",
                rollup.target_label_id
            );
        }
        for ((direction, edge_label_id), rollup) in &self.adjacency_rollups {
            debug_assert_eq!(*direction, rollup.direction);
            debug_assert_eq!(*edge_label_id, rollup.edge_label_id);
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

struct CompoundRollupBuilder {
    stats: CompoundIndexRollupStats,
    coverage: CoverageBuilder,
}

struct AdjacencyRollupBuilder {
    stats: AdjacencyRollupStats,
    coverage: CoverageBuilder,
}

#[derive(Clone)]
struct EqualityIndexDeclaration {
    target: PlannerStatsDeclaredIndexTarget,
    target_label_id: u32,
    prop_key: String,
}

#[derive(Clone)]
struct RangeIndexDeclaration {
    target: PlannerStatsDeclaredIndexTarget,
    target_label_id: u32,
    prop_key: String,
}

#[derive(Clone)]
struct CompoundIndexDeclaration {
    target: PlannerStatsDeclaredIndexTarget,
    target_label_id: u32,
    kind: PlannerStatsDeclaredIndexKind,
    field_fingerprint: u64,
    field_count: u16,
}

type DeclaredIndexFingerprintSet = BTreeSet<(u64, u8, u8, u32, u64, u16, String)>;

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
        if entry.state != SecondaryIndexState::Ready {
            continue;
        }
        let target = planner_stats_declared_index_target(entry);
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
            SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
        };
        for segment in segments {
            coverage.insert(
                segment.segment_id,
                entry.index_id,
                target,
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
    let mut node_label_coverage = CoverageBuilder::new(all_segment_ids.clone());
    let mut timestamp_coverage = CoverageBuilder::new(all_segment_ids.clone());
    let mut full_rollup = FullRollupStats::default();
    let mut node_label_rollups: BTreeMap<u32, NodeLabelRollupStats> = BTreeMap::new();
    let mut timestamp_rollups: BTreeMap<u32, TimestampRollupStats> = BTreeMap::new();
    let mut segment_label_ids: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    let mut segment_timestamp_label_ids: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    let mut property_builders: BTreeMap<(u32, String), PropertyRollupBuilder> = BTreeMap::new();
    let equality_declarations = ready_equality_declarations(secondary_indexes);
    let range_declarations = ready_range_declarations(secondary_indexes);
    let compound_declarations = ready_compound_declarations(secondary_indexes);
    let mut equality_builders =
        equality_rollup_builders(&equality_declarations, all_segment_ids.clone());
    let mut range_builders = range_rollup_builders(&range_declarations, all_segment_ids.clone());
    let mut compound_builders =
        compound_rollup_builders(&compound_declarations, all_segment_ids.clone());
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
                node_label_coverage.mark_covered(segment.segment_id);
                timestamp_coverage.mark_covered(segment.segment_id);
                full_rollup.node_count = full_rollup.node_count.saturating_add(stats.node_count);
                full_rollup.edge_count = full_rollup.edge_count.saturating_add(stats.edge_count);
                if stats.general_property_stats_complete {
                    complete_property_segment_ids.insert(segment.segment_id);
                }
                segment_label_ids.insert(
                    segment.segment_id,
                    stats
                        .node_label_stats
                        .iter()
                        .map(|node_label_stats| node_label_stats.label_id)
                        .collect(),
                );
                segment_timestamp_label_ids.insert(
                    segment.segment_id,
                    stats
                        .timestamp_stats
                        .iter()
                        .map(|timestamp| timestamp.label_id)
                        .collect(),
                );
                add_node_label_rollups(&mut node_label_rollups, stats);
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
                add_compound_rollups(
                    &mut compound_builders,
                    segment.segment_id,
                    stats,
                    &compound_declarations,
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
    let node_label_coverage = node_label_coverage.finish();
    let timestamp_coverage = timestamp_coverage.finish();
    finalize_timestamp_rollup_coverage(
        &mut timestamp_rollups,
        &node_label_rollups,
        all_segment_ids.clone(),
        &segment_label_ids,
        &segment_timestamp_label_ids,
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

    let compound_index_rollups = compound_builders
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
        node_label_coverage,
        timestamp_coverage,
        property_rollups,
        node_label_rollups,
        timestamp_rollups,
        equality_index_rollups,
        range_index_rollups,
        compound_index_rollups,
        adjacency_rollups,
        segment_stale_risks: BTreeMap::new(),
        max_segment_stale_risk: StalePostingRisk::Unknown,
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
        let Some((target, target_label_id, prop_key)) = ready_property_target(entry) else {
            continue;
        };
        if !matches!(entry.kind, SecondaryIndexKind::Equality) {
            continue;
        }
        declarations.insert(
            entry.index_id,
            EqualityIndexDeclaration {
                target,
                target_label_id,
                prop_key,
            },
        );
    }
    declarations
}

fn ready_range_declarations(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> BTreeMap<u64, RangeIndexDeclaration> {
    let mut declarations = BTreeMap::new();
    for entry in secondary_indexes {
        let Some((target, target_label_id, prop_key)) = ready_property_target(entry) else {
            continue;
        };
        if !matches!(&entry.kind, SecondaryIndexKind::Range) {
            continue;
        }
        declarations.insert(
            entry.index_id,
            RangeIndexDeclaration {
                target,
                target_label_id,
                prop_key,
            },
        );
    }
    declarations
}

fn ready_compound_declarations(
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> BTreeMap<u64, CompoundIndexDeclaration> {
    let mut declarations = BTreeMap::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready {
            continue;
        }
        let (target, target_label_id, field_count) = match &entry.target {
            SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => (
                PlannerStatsDeclaredIndexTarget::NodeFieldIndex,
                *label_id,
                fields.len() as u16,
            ),
            SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => (
                PlannerStatsDeclaredIndexTarget::EdgeFieldIndex,
                *label_id,
                fields.len() as u16,
            ),
            SecondaryIndexTarget::NodeProperty { .. }
            | SecondaryIndexTarget::EdgeProperty { .. } => {
                continue;
            }
        };
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
            SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
        };
        declarations.insert(
            entry.index_id,
            CompoundIndexDeclaration {
                target,
                target_label_id,
                kind,
                field_fingerprint: secondary_index_declaration_fingerprint_for_entry(entry),
                field_count,
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
                    target_label_id: declaration.target_label_id,
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
                    target_label_id: declaration.target_label_id,
                    prop_key: declaration.prop_key.clone(),
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            },
        );
    }
    builders
}

fn compound_rollup_builders(
    declarations: &BTreeMap<u64, CompoundIndexDeclaration>,
    all_segment_ids: Arc<[u64]>,
) -> BTreeMap<u64, CompoundRollupBuilder> {
    let mut builders = BTreeMap::new();
    for (index_id, declaration) in declarations {
        builders.insert(
            *index_id,
            CompoundRollupBuilder {
                stats: CompoundIndexRollupStats {
                    index_id: *index_id,
                    target: declaration.target,
                    target_label_id: declaration.target_label_id,
                    kind: declaration.kind,
                    field_fingerprint: declaration.field_fingerprint,
                    field_count: declaration.field_count,
                    ..Default::default()
                },
                coverage: CoverageBuilder::new(all_segment_ids.clone()),
            },
        );
    }
    builders
}

fn ready_property_target(
    entry: &SecondaryIndexManifestEntry,
) -> Option<(PlannerStatsDeclaredIndexTarget, u32, String)> {
    if entry.state != crate::types::SecondaryIndexState::Ready {
        return None;
    }
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key } => Some((
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            *label_id,
            prop_key.clone(),
        )),
        SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => Some((
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            *label_id,
            prop_key.clone(),
        )),
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => None,
    }
}

fn add_node_label_rollups(
    node_label_rollups: &mut BTreeMap<u32, NodeLabelRollupStats>,
    stats: &SegmentPlannerStatsV1,
) {
    for node_label_stats in &stats.node_label_stats {
        let rollup = node_label_rollups
            .entry(node_label_stats.label_id)
            .or_insert_with(|| NodeLabelRollupStats {
                label_id: node_label_stats.label_id,
                ..Default::default()
            });
        rollup.node_count = rollup
            .node_count
            .saturating_add(node_label_stats.node_count);
        rollup.min_node_id = min_option(rollup.min_node_id, node_label_stats.min_node_id);
        rollup.max_node_id = max_option(rollup.max_node_id, node_label_stats.max_node_id);
        rollup.min_updated_at_ms =
            min_option(rollup.min_updated_at_ms, node_label_stats.min_updated_at_ms);
        rollup.max_updated_at_ms =
            max_option(rollup.max_updated_at_ms, node_label_stats.max_updated_at_ms);
    }
}

fn add_timestamp_rollups(
    timestamp_rollups: &mut BTreeMap<u32, TimestampRollupStats>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
) {
    for timestamp in &stats.timestamp_stats {
        let rollup = timestamp_rollups
            .entry(timestamp.label_id)
            .or_insert_with(|| TimestampRollupStats {
                label_id: timestamp.label_id,
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
    node_label_rollups: &BTreeMap<u32, NodeLabelRollupStats>,
    all_segment_ids: Arc<[u64]>,
    segment_label_ids: &BTreeMap<u64, BTreeSet<u32>>,
    segment_timestamp_label_ids: &BTreeMap<u64, BTreeSet<u32>>,
) {
    let timestamp_label_ids: Vec<u32> = node_label_rollups
        .keys()
        .chain(timestamp_rollups.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for label_id in timestamp_label_ids {
        let mut coverage = CoverageBuilder::new(all_segment_ids.clone());
        for segment_id in all_segment_ids.iter().copied() {
            let Some(segment_labels) = segment_label_ids.get(&segment_id) else {
                continue;
            };
            if !segment_labels.contains(&label_id) {
                coverage.mark_covered(segment_id);
                continue;
            }
            if segment_timestamp_label_ids
                .get(&segment_id)
                .is_some_and(|timestamps| timestamps.contains(&label_id))
            {
                coverage.mark_covered(segment_id);
            } else {
                coverage.mark_mismatched(segment_id);
            }
        }
        let rollup = timestamp_rollups
            .entry(label_id)
            .or_insert_with(|| TimestampRollupStats {
                label_id,
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
        let key = (property.label_id, property.prop_key.clone());
        let builder = property_builders
            .entry(key)
            .or_insert_with(|| PropertyRollupBuilder {
                stats: PropertyRollupStats {
                    label_id: property.label_id,
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
                declaration.target,
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
                declaration.target,
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
        builder.stats.min_key = min_option(builder.stats.min_key, index_stats.min_key);
        builder.stats.max_key = max_option(builder.stats.max_key, index_stats.max_key);
        builder.stats.segment_rollups.insert(
            segment_id,
            RangeIndexSegmentRollupStats {
                total_entries: index_stats.total_entries,
                min_key: index_stats.min_key,
                max_key: index_stats.max_key,
                buckets: index_stats.buckets.clone(),
            },
        );
    }
}

fn add_compound_rollups(
    compound_builders: &mut BTreeMap<u64, CompoundRollupBuilder>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
    declarations: &BTreeMap<u64, CompoundIndexDeclaration>,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
    runtime_coverage: &DeclaredIndexRuntimeCoverage,
) {
    let mut seen = BTreeSet::new();
    for index_stats in &stats.compound_index_stats {
        if !seen.insert(index_stats.index_id) {
            continue;
        };
        let Some(builder) = compound_builders.get_mut(&index_stats.index_id) else {
            continue;
        };
        let Some(declaration) = declarations.get(&index_stats.index_id) else {
            continue;
        };
        if !declared_compound_block_matches(index_stats, declaration, declared_fingerprints)
            || !runtime_coverage.is_available(
                segment_id,
                index_stats.index_id,
                declaration.target,
                declaration.kind,
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
        builder.stats.distinct_full_keys = builder
            .stats
            .distinct_full_keys
            .saturating_add(index_stats.distinct_full_keys);
        merge_compound_prefix_stats(
            &mut builder.stats.prefix_stats,
            &mut builder.stats.exact_overflowed_prefix_lens,
            &index_stats.prefix_stats,
        );
        merge_compound_range_stats(&mut builder.stats.range_stats, &index_stats.range_stats);
        builder
            .stats
            .segment_rollups
            .insert(segment_id, index_stats.clone());
    }
}

fn merge_compound_prefix_stats(
    target: &mut Vec<CompoundPrefixStats>,
    exact_overflowed_prefix_lens: &mut BTreeSet<u16>,
    source: &[CompoundPrefixStats],
) {
    for source_stats in source {
        let target_stats = match target
            .iter_mut()
            .find(|stats| stats.prefix_len == source_stats.prefix_len)
        {
            Some(stats) => stats,
            None => {
                target.push(CompoundPrefixStats {
                    prefix_len: source_stats.prefix_len,
                    distinct_prefixes: 0,
                    max_postings_per_prefix: 0,
                    exact_prefix_postings: Vec::new(),
                });
                target.last_mut().expect("compound prefix stat inserted")
            }
        };
        target_stats.distinct_prefixes = target_stats
            .distinct_prefixes
            .saturating_add(source_stats.distinct_prefixes);
        target_stats.max_postings_per_prefix = target_stats
            .max_postings_per_prefix
            .max(source_stats.max_postings_per_prefix);
        if exact_overflowed_prefix_lens.contains(&source_stats.prefix_len) {
            continue;
        }
        // A segment whose own exact list overflowed contributes an empty
        // list while reporting more distinct prefixes; merging anything
        // partial would later be trusted as exact by costing, so overflow
        // poisons the merged map permanently.
        let source_exact_complete =
            source_stats.exact_prefix_postings.len() as u64 == source_stats.distinct_prefixes;
        if !source_exact_complete {
            exact_overflowed_prefix_lens.insert(source_stats.prefix_len);
            target_stats.exact_prefix_postings = Vec::new();
            continue;
        }
        let mut exact: BTreeMap<Vec<u8>, u64> = target_stats
            .exact_prefix_postings
            .iter()
            .map(|stat| (stat.encoded_prefix.clone(), stat.postings))
            .collect();
        let mut overflowed = false;
        for stat in &source_stats.exact_prefix_postings {
            saturating_add_map_value(&mut exact, stat.encoded_prefix.clone(), stat.postings);
            if exact.len() > COMPOUND_STATS_EXACT_PREFIX_LIMIT {
                exact.clear();
                overflowed = true;
                break;
            }
        }
        if overflowed {
            exact_overflowed_prefix_lens.insert(source_stats.prefix_len);
        }
        target_stats.exact_prefix_postings = exact
            .into_iter()
            .map(|(encoded_prefix, postings)| CompoundExactPrefixStat {
                encoded_prefix,
                postings,
            })
            .collect();
    }
    target.sort_by_key(|stats| stats.prefix_len);
}

fn merge_compound_range_stats(target: &mut Vec<CompoundRangeStats>, source: &[CompoundRangeStats]) {
    for source_stats in source {
        let target_stats = match target.iter_mut().find(|stats| {
            stats.equality_prefix_len == source_stats.equality_prefix_len
                && stats.range_field_ordinal == source_stats.range_field_ordinal
        }) {
            Some(stats) => stats,
            None => {
                target.push(CompoundRangeStats {
                    equality_prefix_len: source_stats.equality_prefix_len,
                    range_field_ordinal: source_stats.range_field_ordinal,
                    total_numeric_entries: 0,
                    min_key: None,
                    max_key: None,
                    buckets: Vec::new(),
                });
                target.last_mut().expect("compound range stat inserted")
            }
        };
        target_stats.total_numeric_entries = target_stats
            .total_numeric_entries
            .saturating_add(source_stats.total_numeric_entries);
        target_stats.min_key = min_option(target_stats.min_key, source_stats.min_key);
        target_stats.max_key = max_option(target_stats.max_key, source_stats.max_key);
        target_stats
            .buckets
            .extend(source_stats.buckets.iter().cloned());
        target_stats.buckets.sort_by_key(|left| left.upper_key);
    }
    target.sort_by_key(|stats| (stats.equality_prefix_len, stats.range_field_ordinal));
}

fn add_adjacency_rollups(
    adjacency_builders: &mut BTreeMap<(PlannerStatsDirection, Option<u32>), AdjacencyRollupBuilder>,
    all_segment_ids: Arc<[u64]>,
    segment_id: u64,
    stats: &SegmentPlannerStatsV1,
) {
    for adjacency in &stats.adjacency_stats {
        let key = (adjacency.direction, adjacency.edge_label_id);
        let builder = adjacency_builders
            .entry(key)
            .or_insert_with(|| AdjacencyRollupBuilder {
                stats: AdjacencyRollupStats {
                    direction: adjacency.direction,
                    edge_label_id: adjacency.edge_label_id,
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
    merged.truncate(PLANNER_STATS_TOP_HUBS_PER_EDGE_LABEL);
    *current = merged;
}

fn declared_equality_block_matches(
    block: &EqualityIndexPlannerStats,
    declaration: &EqualityIndexDeclaration,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
) -> bool {
    if block.target_label_id != declaration.target_label_id
        || block.prop_key != declaration.prop_key
    {
        return false;
    }
    declared_fingerprints.contains(&declared_index_key(
        block.index_id,
        declaration.target,
        PlannerStatsDeclaredIndexKind::Equality,
        declaration.target_label_id,
        0,
        0,
        &declaration.prop_key,
    ))
}

fn declared_range_block_matches(
    block: &RangeIndexPlannerStats,
    declaration: &RangeIndexDeclaration,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
) -> bool {
    if block.target_label_id != declaration.target_label_id
        || block.prop_key != declaration.prop_key
    {
        return false;
    }
    declared_fingerprints.contains(&declared_index_key(
        block.index_id,
        declaration.target,
        PlannerStatsDeclaredIndexKind::Range,
        declaration.target_label_id,
        0,
        0,
        &declaration.prop_key,
    ))
}

fn declared_compound_block_matches(
    block: &CompoundIndexPlannerStats,
    declaration: &CompoundIndexDeclaration,
    declared_fingerprints: &DeclaredIndexFingerprintSet,
) -> bool {
    if block.target != declaration.target
        || block.target_label_id != declaration.target_label_id
        || block.kind != declaration.kind
        || block.field_fingerprint != declaration.field_fingerprint
        || block.field_count != declaration.field_count
    {
        return false;
    }
    declared_fingerprints.contains(&declared_index_key(
        block.index_id,
        declaration.target,
        declaration.kind,
        declaration.target_label_id,
        declaration.field_fingerprint,
        declaration.field_count,
        "",
    ))
}

fn declared_index_fingerprint_set(stats: &SegmentPlannerStatsV1) -> DeclaredIndexFingerprintSet {
    stats
        .declared_indexes
        .iter()
        .map(|declared| {
            declared_index_key(
                declared.index_id,
                declared.target,
                declared.kind,
                declared.target_label_id,
                declared.field_fingerprint,
                declared.field_count,
                &declared.prop_key,
            )
        })
        .collect()
}

fn declared_index_key(
    index_id: u64,
    target: PlannerStatsDeclaredIndexTarget,
    kind: PlannerStatsDeclaredIndexKind,
    target_label_id: u32,
    field_fingerprint: u64,
    field_count: u16,
    prop_key: &str,
) -> (u64, u8, u8, u32, u64, u16, String) {
    (
        index_id,
        declared_index_target_rank(target),
        declared_index_kind_rank(kind),
        target_label_id,
        field_fingerprint,
        field_count,
        prop_key.to_string(),
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

fn saturating_add_map_value<K: Ord>(map: &mut BTreeMap<K, u64>, key: K, value: u64) {
    map.entry(key)
        .and_modify(|count| *count = count.saturating_add(value))
        .or_insert(value);
}

#[allow(dead_code)]
fn invalid_u64_bounds(lower: Option<(u64, bool)>, upper: Option<(u64, bool)>) -> bool {
    let (Some((lower_value, lower_inclusive)), Some((upper_value, upper_inclusive))) =
        (lower, upper)
    else {
        return false;
    };
    lower_value > upper_value
        || (lower_value == upper_value && (!lower_inclusive || !upper_inclusive))
}

#[allow(dead_code)]
fn u64_bucket_below_lower(bucket_upper: u64, lower: Option<(u64, bool)>) -> bool {
    lower.is_some_and(|(lower_value, inclusive)| {
        bucket_upper < lower_value || (!inclusive && bucket_upper <= lower_value)
    })
}

#[allow(dead_code)]
fn u64_bucket_above_upper(bucket_lower_floor: Option<u64>, upper: Option<(u64, bool)>) -> bool {
    let (Some(bucket_lower_floor), Some((upper_value, inclusive))) = (bucket_lower_floor, upper)
    else {
        return false;
    };
    bucket_lower_floor > upper_value || (!inclusive && bucket_lower_floor >= upper_value)
}

#[allow(dead_code)]
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

fn invalid_range_key_bounds(
    lower: Option<(RangeStatsKey, bool)>,
    upper: Option<(RangeStatsKey, bool)>,
) -> bool {
    match (lower, upper) {
        (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
            lower > upper || (lower == upper && !(lower_inclusive && upper_inclusive))
        }
        _ => false,
    }
}

fn range_key_bucket_below_lower(
    value: RangeStatsKey,
    lower: Option<(RangeStatsKey, bool)>,
) -> bool {
    match lower {
        Some((lower, inclusive)) if inclusive => value < lower,
        Some((lower, _)) => value <= lower,
        None => false,
    }
}

fn range_key_bucket_above_upper(
    value: Option<RangeStatsKey>,
    upper: Option<(RangeStatsKey, bool)>,
) -> bool {
    match (value, upper) {
        (Some(value), Some((upper, inclusive))) if inclusive => value > upper,
        (Some(value), Some((upper, _))) => value >= upper,
        (None, Some(_)) => false,
        (_, None) => false,
    }
}

fn range_key_bucket_fully_inside(
    min_value: Option<RangeStatsKey>,
    max_value: Option<RangeStatsKey>,
    lower: Option<(RangeStatsKey, bool)>,
    upper: Option<(RangeStatsKey, bool)>,
) -> bool {
    let lower_inside = match (min_value, lower) {
        (Some(min_value), Some((lower, inclusive))) if inclusive => min_value >= lower,
        (Some(min_value), Some((lower, _))) => min_value > lower,
        (_, None) => true,
        (None, Some(_)) => false,
    };
    let upper_inside = match (max_value, upper) {
        (Some(max_value), Some((upper, inclusive))) if inclusive => max_value <= upper,
        (Some(max_value), Some((upper, _))) => max_value < upper,
        (_, None) => true,
        (None, Some(_)) => false,
    };
    lower_inside && upper_inside
}

fn estimate_range_key_histogram(
    total_count: u64,
    min_value: Option<RangeStatsKey>,
    max_value: Option<RangeStatsKey>,
    buckets: impl Iterator<Item = (RangeStatsKey, u64)>,
    lower: Option<(RangeStatsKey, bool)>,
    upper: Option<(RangeStatsKey, bool)>,
) -> PlannerStatsValueEstimate {
    if total_count == 0 || invalid_range_key_bounds(lower, upper) {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if max_value.is_some_and(|max_value| range_key_bucket_below_lower(max_value, lower))
        || min_value.is_some_and(|min_value| range_key_bucket_above_upper(Some(min_value), upper))
    {
        return PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        };
    }
    if min_value.is_some()
        && max_value.is_some()
        && range_key_bucket_fully_inside(min_value, max_value, lower, upper)
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
        if range_key_bucket_below_lower(bucket_upper, lower)
            || range_key_bucket_above_upper(previous_upper, upper)
        {
            previous_upper = Some(bucket_upper);
            continue;
        }
        estimated = estimated.saturating_add(bucket_count);
        if !range_key_bucket_fully_inside(previous_upper, Some(bucket_upper), lower, upper) {
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

#[allow(dead_code)]
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
struct NodeLabelAccumulator {
    node_count: u64,
    min_node_id: Option<u64>,
    max_node_id: Option<u64>,
    min_updated_at_ms: Option<i64>,
    max_updated_at_ms: Option<i64>,
    updated_values: Vec<i64>,
}

#[derive(Clone)]
struct PropertyAccumulator {
    label_id: u32,
    prop_key: String,
    tracked_reason: PropertyStatsTrackedReason,
    present_count: u64,
    null_count: u64,
    value_kind_counts: ValueKindCounts,
    value_counts: BTreeMap<u64, u64>,
    distinct_overflow: bool,
    numeric_values: Vec<RangeStatsKey>,
}

impl PropertyAccumulator {
    fn new(label_id: u32, prop_key: String, tracked_reason: PropertyStatsTrackedReason) -> Self {
        Self {
            label_id,
            prop_key,
            tracked_reason,
            present_count: 0,
            null_count: 0,
            value_kind_counts: ValueKindCounts::default(),
            value_counts: BTreeMap::new(),
            distinct_overflow: false,
            numeric_values: Vec::new(),
        }
    }

    fn observe(&mut self, value: &PropValue) {
        self.present_count += 1;
        self.value_kind_counts.observe(value);
        if matches!(value, PropValue::Null) {
            self.null_count += 1;
        }

        let value_hash = hash_prop_equality_key(value);
        if let Some(count) = self.value_counts.get_mut(&value_hash) {
            *count += 1;
        } else if self.value_counts.len() < PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES {
            self.value_counts.insert(value_hash, 1);
        } else {
            self.distinct_overflow = true;
        }

        if let Some(encoded) = numeric_range_sort_key_for_value(value) {
            push_capped_numeric(&mut self.numeric_values, encoded);
        }
    }

    fn into_stats(mut self) -> PropertyPlannerStats {
        self.numeric_values.sort_unstable();
        let mut numeric_summaries = Vec::new();
        if !self.numeric_values.is_empty() {
            numeric_summaries.push(range_summary_from_values(&self.numeric_values));
        }

        let exact_distinct_count =
            (!self.distinct_overflow).then_some(self.value_counts.len() as u64);
        let distinct_lower_bound = self
            .distinct_overflow
            .then_some(self.value_counts.len() as u64);
        let top_values =
            top_value_frequencies(self.value_counts, PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY);
        PropertyPlannerStats {
            label_id: self.label_id,
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

#[cfg(test)]
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

pub(crate) fn read_planner_stats_payload(
    data: &[u8],
    expected_segment_id: u64,
    expected_node_count: u64,
    expected_edge_count: u64,
) -> PlannerStatsAvailability {
    if data.is_empty() {
        return PlannerStatsAvailability::Missing;
    }
    if data.len() > PLANNER_STATS_HARD_SIDECAR_BYTES {
        return PlannerStatsAvailability::Unavailable {
            reason: format!(
                "planner stats sidecar exceeds hard cap: {} bytes",
                data.len()
            ),
        };
    }
    match decode_planner_stats_envelope(
        data,
        expected_segment_id,
        expected_node_count,
        expected_edge_count,
    ) {
        Ok(stats) => PlannerStatsAvailability::Available(Box::new(stats)),
        Err(reason) => PlannerStatsAvailability::Unavailable { reason },
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

    let target_is_property = matches!(
        target_index.target,
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. }
    );
    let target_is_compound = matches!(
        target_index.target,
        SecondaryIndexTarget::NodeFieldIndex { .. } | SecondaryIndexTarget::EdgeFieldIndex { .. }
    );
    let target_equality_stats = if target_is_property
        && matches!(target_index.kind, SecondaryIndexKind::Equality)
    {
        let mut stats =
            build_equality_index_stats_from_segment(segment, std::slice::from_ref(target_index))?;
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
    let target_range_stats =
        if target_is_property && matches!(target_index.kind, SecondaryIndexKind::Range) {
            let mut stats =
                build_range_index_stats_from_segment(segment, std::slice::from_ref(target_index))?;
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
    let target_compound_stats = if target_is_compound {
        let mut stats =
            build_compound_index_stats_from_segment(segment, std::slice::from_ref(target_index))?;
        let Some(stats) = stats.pop() else {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        };
        if stats.coverage != DeclaredIndexRuntimeCoverageState::Available {
            return Ok(PlannerStatsWriteOutcome::SkippedTargetUnavailable);
        }
        Some(stats)
    } else {
        None
    };

    let written = publish_planner_stats_component_payload_from_latest(
        seg_dir,
        &ready_indexes,
        |current_payload, segment_id, node_count, edge_count| {
            let base_stats = current_payload
                .and_then(|payload| {
                    match read_planner_stats_payload(payload, segment_id, node_count, edge_count) {
                        PlannerStatsAvailability::Available(stats) => Some(stats.as_ref().clone()),
                        PlannerStatsAvailability::Missing
                        | PlannerStatsAvailability::Unavailable { .. } => None,
                    }
                })
                .map(Ok)
                .unwrap_or_else(|| build_minimal_targeted_refresh_stats(segment))?;
            let stats = merge_targeted_declared_index_stats(
                base_stats,
                &ready_indexes,
                target_index.index_id,
                target_equality_stats,
                target_range_stats,
                target_compound_stats,
            );
            planner_stats_sidecar_payload(stats)
        },
    )?;
    if !written {
        return Ok(PlannerStatsWriteOutcome::SkippedOversize);
    }
    Ok(PlannerStatsWriteOutcome::Written)
}

pub(crate) fn planner_stats_declaration_fingerprint_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> u64 {
    declaration_fingerprint(&declared_index_fingerprints(std::slice::from_ref(entry)))
}

pub(crate) fn build_flush_stats_core_partial(
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<StatsCorePartial, EngineError> {
    let policy = PlannerStatsBuildPolicy::flush();
    let declared_property_reasons = declared_property_reasons(secondary_indexes);
    let mut label_accs = BTreeMap::new();
    let mut property_candidates = BTreeMap::new();

    let mut sorted_nodes: Vec<&NodeRecord> = nodes.values().collect();
    sorted_nodes.sort_unstable_by_key(|node| node.id);
    for node in &sorted_nodes {
        for &label_id in node.label_ids.as_slice() {
            observe_label(&mut label_accs, node.id, label_id, node.updated_at);
            if policy.allow_general_property_decode {
                observe_general_property_candidates(
                    &mut property_candidates,
                    &declared_property_reasons,
                    label_id,
                    &node.props,
                );
            }
        }
    }
    let mut property_accs =
        seed_property_accumulators(&declared_property_reasons, property_candidates, &label_accs);
    if policy.allow_general_property_decode {
        for node in &sorted_nodes {
            for &label_id in node.label_ids.as_slice() {
                observe_selected_node_properties(&mut property_accs, label_id, &node.props);
            }
        }
    }

    let mut sorted_edges: Vec<&EdgeRecord> = edges.values().collect();
    sorted_edges.sort_unstable_by_key(|edge| edge.id);
    let timestamp_stats = finalize_timestamp_stats_from_label_accs(&label_accs);
    Ok(StatsCorePartial {
        node_count: sorted_nodes.len() as u64,
        edge_count: sorted_edges.len() as u64,
        truncated: false,
        general_property_stats_complete: true,
        general_property_sampled_node_count: sorted_nodes.len() as u64,
        general_property_sampled_raw_bytes: 0,
        general_property_budget_exhausted: false,
        node_label_stats: finalize_node_label_stats(label_accs),
        timestamp_stats,
        property_stats: finalize_property_stats(property_accs),
        adjacency_stats: build_adjacency_stats_from_edges(sorted_edges.iter().copied()),
        node_id_sample: node_id_sample(sorted_nodes.iter().map(|node| node.id)),
    })
}

pub(crate) fn assemble_flush_stats_from_partials(
    segment_id: u64,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    core: StatsCorePartial,
    declared_evidence: DeclaredIndexStatsEvidence,
) -> SegmentPlannerStatsV1 {
    assemble_stats_from_partials(
        segment_id,
        PlannerStatsBuildKind::Flush,
        secondary_indexes,
        core,
        declared_evidence,
    )
}

#[cfg(test)]
pub(crate) fn build_flush_stats(
    segment_id: u64,
    seg_dir: &Path,
    nodes: &NodeIdMap<NodeRecord>,
    edges: &NodeIdMap<EdgeRecord>,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let core = build_flush_stats_core_partial(nodes, edges, secondary_indexes)?;
    let declared_evidence = DeclaredIndexStatsEvidence {
        equality_index_stats: build_equality_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        range_index_stats: build_range_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        compound_index_stats: build_compound_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
    };
    Ok(assemble_flush_stats_from_partials(
        segment_id,
        secondary_indexes,
        core,
        declared_evidence,
    ))
}

pub(crate) fn build_compaction_stats_core_partial(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<StatsCorePartial, EngineError> {
    build_compaction_stats_core_partial_with_policy(
        segments,
        node_metas,
        edge_metas,
        secondary_indexes,
        PlannerStatsBuildPolicy::compaction(),
    )
}

fn build_compaction_stats_core_partial_with_policy(
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
    policy: PlannerStatsBuildPolicy,
) -> Result<StatsCorePartial, EngineError> {
    let declared_property_reasons = declared_property_reasons(secondary_indexes);
    let mut label_accs = BTreeMap::new();
    for meta in node_metas {
        for &label_id in meta.label_ids.as_slice() {
            observe_label(&mut label_accs, meta.node_id, label_id, meta.updated_at);
        }
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
            for &label_id in meta.label_ids.as_slice() {
                observe_general_property_candidates(
                    &mut property_candidates,
                    &declared_property_reasons,
                    label_id,
                    &props,
                );
            }
            sampled_props.push((meta.label_ids, props));
            sampled_node_count += 1;
            sampled_raw_bytes = next_bytes;
        }
    }
    let mut property_accs =
        seed_property_accumulators(&declared_property_reasons, property_candidates, &label_accs);
    for (label_ids, props) in &sampled_props {
        for &label_id in label_ids.as_slice() {
            observe_selected_node_properties(&mut property_accs, label_id, props);
        }
    }
    let general_property_stats_complete =
        sampled_node_count == node_metas.len() as u64 && !budget_exhausted;

    let edge_refs = edge_metas.iter().map(EdgeMetaRef::from);
    let timestamp_stats = finalize_timestamp_stats_from_label_accs(&label_accs);
    Ok(StatsCorePartial {
        node_count: node_metas.len() as u64,
        edge_count: edge_metas.len() as u64,
        truncated: !general_property_stats_complete,
        general_property_stats_complete,
        general_property_sampled_node_count: sampled_node_count,
        general_property_sampled_raw_bytes: sampled_raw_bytes,
        general_property_budget_exhausted: budget_exhausted,
        node_label_stats: finalize_node_label_stats(label_accs),
        timestamp_stats,
        property_stats: finalize_property_stats(property_accs),
        adjacency_stats: build_adjacency_stats_from_edge_meta(edge_refs),
        node_id_sample: node_id_sample(node_metas.iter().map(|meta| meta.node_id)),
    })
}

pub(crate) fn assemble_compaction_stats_from_partials(
    segment_id: u64,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    core: StatsCorePartial,
    declared_evidence: DeclaredIndexStatsEvidence,
) -> SegmentPlannerStatsV1 {
    assemble_stats_from_partials(
        segment_id,
        PlannerStatsBuildKind::Compaction,
        secondary_indexes,
        core,
        declared_evidence,
    )
}

#[cfg(test)]
pub(crate) fn build_compaction_stats(
    segment_id: u64,
    seg_dir: &Path,
    segments: &[Arc<SegmentReader>],
    node_metas: &[CompactNodeMeta],
    edge_metas: &[CompactEdgeMeta],
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let core =
        build_compaction_stats_core_partial(segments, node_metas, edge_metas, secondary_indexes)?;
    let declared_evidence = DeclaredIndexStatsEvidence {
        equality_index_stats: build_equality_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        range_index_stats: build_range_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
        compound_index_stats: build_compound_index_stats_from_sidecars(seg_dir, secondary_indexes)?,
    };
    Ok(assemble_compaction_stats_from_partials(
        segment_id,
        secondary_indexes,
        core,
        declared_evidence,
    ))
}

fn assemble_stats_from_partials(
    segment_id: u64,
    build_kind: PlannerStatsBuildKind,
    secondary_indexes: &[SecondaryIndexManifestEntry],
    core: StatsCorePartial,
    mut declared_evidence: DeclaredIndexStatsEvidence,
) -> SegmentPlannerStatsV1 {
    declared_evidence.sort();
    let declared = declared_index_fingerprints(secondary_indexes);
    SegmentPlannerStatsV1 {
        format_version: PLANNER_STATS_FORMAT_VERSION,
        segment_id,
        build_kind,
        built_at_ms: 0,
        declaration_fingerprint: declaration_fingerprint(&declared),
        declared_indexes: declared,
        node_count: core.node_count,
        edge_count: core.edge_count,
        truncated: core.truncated,
        general_property_stats_complete: core.general_property_stats_complete,
        general_property_sampled_node_count: core.general_property_sampled_node_count,
        general_property_sampled_raw_bytes: core.general_property_sampled_raw_bytes,
        general_property_budget_exhausted: core.general_property_budget_exhausted,
        node_label_stats: core.node_label_stats,
        timestamp_stats: core.timestamp_stats,
        property_stats: core.property_stats,
        equality_index_stats: declared_evidence.equality_index_stats,
        range_index_stats: declared_evidence.range_index_stats,
        compound_index_stats: declared_evidence.compound_index_stats,
        adjacency_stats: core.adjacency_stats,
        node_id_sample: core.node_id_sample,
    }
}

#[cfg(test)]
fn build_equality_index_stats_from_sidecars(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<EqualityIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready
            || !matches!(entry.kind, SecondaryIndexKind::Equality)
            || entry.target.single_property_key().is_none()
        {
            continue;
        }
        let file_name = match &entry.target {
            SecondaryIndexTarget::NodeProperty { .. } => {
                format!("node_prop_eq_{}.dat", entry.index_id)
            }
            SecondaryIndexTarget::EdgeProperty { .. } => {
                format!("edge_prop_eq_{}.dat", entry.index_id)
            }
            SecondaryIndexTarget::NodeFieldIndex { .. }
            | SecondaryIndexTarget::EdgeFieldIndex { .. } => continue,
        };
        let path = seg_dir.join("secondary_indexes").join(file_name);
        let groups = read_secondary_eq_group_counts(&path)?;
        let sidecar_present_at_build = groups.is_some();
        let groups = groups.unwrap_or_default();
        result.push(equality_index_stats_from_group_counts(
            entry,
            &groups,
            sidecar_present_at_build,
        ));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

fn build_equality_index_stats_from_segment(
    segment: &SegmentReader,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<EqualityIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready
            || !matches!(entry.kind, SecondaryIndexKind::Equality)
        {
            continue;
        }
        let mut groups = BTreeMap::new();
        let sidecar_present_at_build =
            segment.for_each_declared_secondary_eq_group(entry, |value_hash, ids| {
                groups.insert(value_hash, ids.len() as u64);
                Ok(())
            })?;
        result.push(equality_index_stats_from_group_counts(
            entry,
            &groups,
            sidecar_present_at_build,
        ));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

pub(crate) fn equality_index_stats_from_written_groups(
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, Vec<u64>>,
) -> EqualityIndexPlannerStats {
    let group_counts: BTreeMap<u64, u64> = groups
        .iter()
        .map(|(&value_hash, ids)| (value_hash, ids.len() as u64))
        .collect();
    equality_index_stats_from_group_counts(entry, &group_counts, true)
}

pub(crate) fn equality_index_stats_from_group_counts(
    entry: &SecondaryIndexManifestEntry,
    groups: &BTreeMap<u64, u64>,
    sidecar_present_at_build: bool,
) -> EqualityIndexPlannerStats {
    let (target_label_id, prop_key) = match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key } => (*label_id, prop_key),
        SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => (*label_id, prop_key),
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            unreachable!("single-property equality stats require property target")
        }
    };
    let mut value_counts = BTreeMap::new();
    let mut total_postings = 0u64;
    let mut max_group_postings = 0u64;
    for (&value_hash, &count) in groups {
        total_postings += count;
        max_group_postings = max_group_postings.max(count);
        value_counts.insert(value_hash, count);
    }
    EqualityIndexPlannerStats {
        index_id: entry.index_id,
        target_label_id,
        prop_key: prop_key.clone(),
        total_postings,
        value_group_count: groups.len() as u64,
        max_group_postings,
        top_value_hashes: top_value_frequencies(
            value_counts,
            PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY,
        ),
        sidecar_present_at_build,
    }
}

#[cfg(test)]
fn build_range_index_stats_from_sidecars(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<RangeIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready {
            continue;
        }
        if !matches!(&entry.kind, SecondaryIndexKind::Range) {
            continue;
        }
        if entry.target.single_property_key().is_none() {
            continue;
        }
        let file_name = match &entry.target {
            SecondaryIndexTarget::NodeProperty { .. } => {
                format!("node_prop_range_{}.dat", entry.index_id)
            }
            SecondaryIndexTarget::EdgeProperty { .. } => {
                format!("edge_prop_range_{}.dat", entry.index_id)
            }
            SecondaryIndexTarget::NodeFieldIndex { .. }
            | SecondaryIndexTarget::EdgeFieldIndex { .. } => continue,
        };
        let path = seg_dir.join("secondary_indexes").join(file_name);
        let encoded_values = read_secondary_range_encoded_values(&path)?;
        let sidecar_present_at_build = encoded_values.is_some();
        result.push(range_index_stats_from_encoded_values(
            entry,
            &encoded_values.unwrap_or_default(),
            sidecar_present_at_build,
        ));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

fn build_range_index_stats_from_segment(
    segment: &SegmentReader,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<RangeIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready {
            continue;
        }
        if !matches!(&entry.kind, SecondaryIndexKind::Range) {
            continue;
        }
        let mut encoded_values = Vec::new();
        let sidecar_present_at_build = segment.for_each_declared_secondary_range_entry(
            entry,
            |encoded_value, _record_id| {
                encoded_values.push(range_stats_key(encoded_value));
                Ok(())
            },
        )?;
        result.push(range_index_stats_from_encoded_values(
            entry,
            &encoded_values,
            sidecar_present_at_build,
        ));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

pub(crate) fn range_index_stats_from_written_entries(
    entry: &SecondaryIndexManifestEntry,
    entries: &[(NumericRangeSortKey, u64)],
) -> RangeIndexPlannerStats {
    let encoded_values: Vec<RangeStatsKey> = entries
        .iter()
        .map(|(encoded_value, _node_id)| range_stats_key(*encoded_value))
        .collect();
    range_index_stats_from_encoded_values(entry, &encoded_values, true)
}

pub(crate) fn range_index_stats_from_encoded_values(
    entry: &SecondaryIndexManifestEntry,
    encoded_values: &[RangeStatsKey],
    sidecar_present_at_build: bool,
) -> RangeIndexPlannerStats {
    if !matches!(&entry.kind, SecondaryIndexKind::Range) {
        unreachable!("range stats require a range secondary index")
    }
    let (target_label_id, prop_key) = match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key } => (*label_id, prop_key),
        SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => (*label_id, prop_key),
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            unreachable!("single-property range stats require property target")
        }
    };
    let mut encoded_values = encoded_values.to_vec();
    encoded_values.sort_unstable();
    RangeIndexPlannerStats {
        index_id: entry.index_id,
        target_label_id,
        prop_key: prop_key.clone(),
        total_entries: encoded_values.len() as u64,
        min_key: encoded_values.first().copied(),
        max_key: encoded_values.last().copied(),
        buckets: range_buckets(&encoded_values, PLANNER_STATS_RANGE_BUCKETS),
        sidecar_present_at_build,
    }
}

#[cfg(test)]
fn build_compound_index_stats_from_sidecars(
    seg_dir: &Path,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<CompoundIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready
            || !matches!(
                entry.target,
                SecondaryIndexTarget::NodeFieldIndex { .. }
                    | SecondaryIndexTarget::EdgeFieldIndex { .. }
            )
        {
            continue;
        }
        let file_name = match (&entry.target, &entry.kind) {
            (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
                format!("node_compound_eq_{}.dat", entry.index_id)
            }
            (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Range) => {
                format!("node_compound_range_{}.dat", entry.index_id)
            }
            (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
                format!("edge_compound_eq_{}.dat", entry.index_id)
            }
            (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Range) => {
                format!("edge_compound_range_{}.dat", entry.index_id)
            }
            _ => continue,
        };
        let path = seg_dir.join("secondary_indexes").join(file_name);
        let Some(data) = read_optional_component_payload(&path)? else {
            result.push(compound_index_stats_from_written_entries(
                entry,
                &[],
                DeclaredIndexRuntimeCoverageState::Missing,
            )?);
            continue;
        };
        let declaration = CompoundSidecarDeclaration::from_manifest_entry(
            entry,
            secondary_index_declaration_fingerprint_for_entry(entry),
        )?;
        let mut builder = CompoundStatsBuilder::new(entry)?;
        for_each_compound_sidecar_entry(&data, &declaration, |key, _id| builder.observe(key))?;
        result.push(builder.finish(DeclaredIndexRuntimeCoverageState::Available));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

fn build_compound_index_stats_from_segment(
    segment: &SegmentReader,
    secondary_indexes: &[SecondaryIndexManifestEntry],
) -> Result<Vec<CompoundIndexPlannerStats>, EngineError> {
    let mut result = Vec::new();
    for entry in secondary_indexes {
        if entry.state != SecondaryIndexState::Ready
            || !matches!(
                entry.target,
                SecondaryIndexTarget::NodeFieldIndex { .. }
                    | SecondaryIndexTarget::EdgeFieldIndex { .. }
            )
        {
            continue;
        }
        let mut builder = CompoundStatsBuilder::new(entry)?;
        let coverage =
            match segment.for_each_compound_sidecar_entry(entry, |key, _id| builder.observe(key)) {
                Ok(true) => DeclaredIndexRuntimeCoverageState::Available,
                Ok(false) => DeclaredIndexRuntimeCoverageState::Missing,
                Err(_) => DeclaredIndexRuntimeCoverageState::Corrupt,
            };
        result.push(builder.finish(coverage));
    }
    result.sort_by_key(|stats| stats.index_id);
    Ok(result)
}

pub(crate) fn compound_index_stats_from_written_entries(
    entry: &SecondaryIndexManifestEntry,
    entries: &[(Vec<u8>, u64)],
    coverage: DeclaredIndexRuntimeCoverageState,
) -> Result<CompoundIndexPlannerStats, EngineError> {
    let mut builder = CompoundStatsBuilder::new(entry)?;
    if entries.windows(2).all(|pair| pair[0].0 <= pair[1].0) {
        for (key, _id) in entries {
            builder.observe(key)?;
        }
    } else {
        let mut ordered: Vec<&(Vec<u8>, u64)> = entries.iter().collect();
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        for (key, _id) in ordered {
            builder.observe(key)?;
        }
    }
    Ok(builder.finish(coverage))
}

/// Streaming accumulator for compound declaration planner stats.
///
/// Every construction path visits sidecar entries in sorted tuple-key order
/// (flush state, compaction merge output, and sidecar scans), so the
/// accumulator keeps O(field_count) running state per open prefix plus the
/// bounded exact-prefix lists and one weighted (value, count) pair per
/// distinct key for range stats — never a map over every distinct tuple key
/// or a value repeated per posting.
pub(crate) struct CompoundStatsBuilder<'a> {
    entry: &'a SecondaryIndexManifestEntry,
    target: PlannerStatsDeclaredIndexTarget,
    target_label_id: u32,
    fields: &'a [SecondaryIndexFieldManifest],
    kind: PlannerStatsDeclaredIndexKind,
    total_postings: u64,
    distinct_full_keys: u64,
    has_current: bool,
    current_key: Vec<u8>,
    current_key_postings: u64,
    current_component_ends: [usize; MAX_SECONDARY_INDEX_FIELDS],
    current_numeric_values: [Option<RangeStatsKey>; MAX_SECONDARY_INDEX_FIELDS],
    prefix_state: Vec<CompoundPrefixAccumulator>,
    range_values: Vec<Vec<(RangeStatsKey, u64)>>,
}

#[derive(Default)]
struct CompoundPrefixAccumulator {
    open_postings: u64,
    distinct_prefixes: u64,
    max_postings_per_prefix: u64,
    exact_prefix_postings: Vec<CompoundExactPrefixStat>,
    exact_overflowed: bool,
}

impl<'a> CompoundStatsBuilder<'a> {
    pub(crate) fn new(entry: &'a SecondaryIndexManifestEntry) -> Result<Self, EngineError> {
        let (target, target_label_id, fields) = match &entry.target {
            SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => (
                PlannerStatsDeclaredIndexTarget::NodeFieldIndex,
                *label_id,
                fields.as_slice(),
            ),
            SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => (
                PlannerStatsDeclaredIndexTarget::EdgeFieldIndex,
                *label_id,
                fields.as_slice(),
            ),
            SecondaryIndexTarget::NodeProperty { .. }
            | SecondaryIndexTarget::EdgeProperty { .. } => {
                return Err(EngineError::InvalidOperation(
                    "compound secondary index unavailable: single-property declaration cannot build compound stats"
                        .to_string(),
                ));
            }
        };
        if fields.is_empty() || fields.len() > MAX_SECONDARY_INDEX_FIELDS {
            return Err(EngineError::InvalidOperation(format!(
                "compound secondary index unavailable: declaration field count {} is outside 1..={MAX_SECONDARY_INDEX_FIELDS}",
                fields.len()
            )));
        }
        let kind = match entry.kind {
            SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
            SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
        };
        Ok(Self {
            entry,
            target,
            target_label_id,
            fields,
            kind,
            total_postings: 0,
            distinct_full_keys: 0,
            has_current: false,
            current_key: Vec::new(),
            current_key_postings: 0,
            current_component_ends: [0; MAX_SECONDARY_INDEX_FIELDS],
            current_numeric_values: [None; MAX_SECONDARY_INDEX_FIELDS],
            prefix_state: (0..fields.len())
                .map(|_| CompoundPrefixAccumulator::default())
                .collect(),
            range_values: vec![Vec::new(); fields.len()],
        })
    }

    pub(crate) fn observe(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.total_postings = self.total_postings.saturating_add(1);
        if self.has_current {
            match key.cmp(self.current_key.as_slice()) {
                std::cmp::Ordering::Equal => {
                    self.current_key_postings = self.current_key_postings.saturating_add(1);
                    return Ok(());
                }
                std::cmp::Ordering::Less => {
                    return Err(EngineError::CorruptRecord(
                        "compound stats input keys are not in sorted order".to_string(),
                    ));
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        let components = decode_compound_tuple_components(key, self.fields)?;
        let mut new_ends = [0usize; MAX_SECONDARY_INDEX_FIELDS];
        let mut new_numeric_values = [None; MAX_SECONDARY_INDEX_FIELDS];
        let mut offset = 0usize;
        for (ordinal, component) in components.iter().enumerate() {
            offset += 3 + component.payload.len();
            new_ends[ordinal] = offset;
            if matches!(self.kind, PlannerStatsDeclaredIndexKind::Range)
                && ordinal > 0
                && component.class == CompoundComponentClass::Numeric
                && component.payload.len() == NUMERIC_RANGE_KEY_BYTES
            {
                let mut key_bytes = [0_u8; NUMERIC_RANGE_KEY_BYTES];
                key_bytes.copy_from_slice(component.payload);
                NumericRangeSortKey::from_sidecar_bytes(key_bytes)?;
                new_numeric_values[ordinal] = Some(key_bytes);
            }
        }

        if self.has_current {
            let divergence = (0..self.fields.len())
                .find(|&ordinal| {
                    key[..new_ends[ordinal]]
                        != self.current_key[..self.current_component_ends[ordinal]]
                })
                .unwrap_or(self.fields.len());
            self.close_current_key();
            for ordinal in divergence..self.fields.len() {
                self.close_prefix(ordinal);
            }
        }

        self.current_key.clear();
        self.current_key.extend_from_slice(key);
        self.current_key_postings = 1;
        self.current_component_ends = new_ends;
        self.current_numeric_values = new_numeric_values;
        self.has_current = true;
        Ok(())
    }

    fn close_current_key(&mut self) {
        self.distinct_full_keys += 1;
        for state in &mut self.prefix_state {
            state.open_postings = state
                .open_postings
                .saturating_add(self.current_key_postings);
        }
        for ordinal in 1..self.fields.len() {
            if let Some(value) = self.current_numeric_values[ordinal] {
                self.range_values[ordinal].push((value, self.current_key_postings));
            }
        }
    }

    fn close_prefix(&mut self, ordinal: usize) {
        let end = self.current_component_ends[ordinal];
        let state = &mut self.prefix_state[ordinal];
        state.distinct_prefixes += 1;
        state.max_postings_per_prefix = state.max_postings_per_prefix.max(state.open_postings);
        if !state.exact_overflowed {
            if state.exact_prefix_postings.len() >= COMPOUND_STATS_EXACT_PREFIX_LIMIT {
                state.exact_overflowed = true;
                state.exact_prefix_postings = Vec::new();
            } else {
                state.exact_prefix_postings.push(CompoundExactPrefixStat {
                    encoded_prefix: self.current_key[..end].to_vec(),
                    postings: state.open_postings,
                });
            }
        }
        state.open_postings = 0;
    }

    pub(crate) fn finish(
        mut self,
        coverage: DeclaredIndexRuntimeCoverageState,
    ) -> CompoundIndexPlannerStats {
        if self.has_current {
            self.close_current_key();
            for ordinal in 0..self.fields.len() {
                self.close_prefix(ordinal);
            }
        }

        let mut prefix_stats = Vec::with_capacity(self.fields.len());
        for (ordinal, state) in std::mem::take(&mut self.prefix_state)
            .into_iter()
            .enumerate()
        {
            if state.distinct_prefixes == 0 {
                continue;
            }
            prefix_stats.push(CompoundPrefixStats {
                prefix_len: (ordinal + 1) as u16,
                distinct_prefixes: state.distinct_prefixes,
                max_postings_per_prefix: state.max_postings_per_prefix,
                exact_prefix_postings: if state.exact_overflowed {
                    Vec::new()
                } else {
                    state.exact_prefix_postings
                },
            });
        }

        let mut range_stats = Vec::new();
        for (ordinal, mut values) in std::mem::take(&mut self.range_values)
            .into_iter()
            .enumerate()
        {
            if ordinal == 0 || values.is_empty() {
                continue;
            }
            values.sort_unstable();
            range_stats.push(CompoundRangeStats {
                equality_prefix_len: ordinal as u16,
                range_field_ordinal: ordinal as u16,
                total_numeric_entries: values.iter().map(|(_, count)| *count).sum(),
                min_key: values.first().map(|(value, _)| *value),
                max_key: values.last().map(|(value, _)| *value),
                buckets: weighted_range_buckets(&values, PLANNER_STATS_RANGE_BUCKETS),
            });
        }

        CompoundIndexPlannerStats {
            index_id: self.entry.index_id,
            target: self.target,
            target_label_id: self.target_label_id,
            kind: self.kind,
            field_fingerprint: secondary_index_declaration_fingerprint_for_entry(self.entry),
            field_count: self.fields.len() as u16,
            total_postings: self.total_postings,
            distinct_full_keys: self.distinct_full_keys,
            prefix_stats,
            range_stats,
            coverage,
        }
    }
}

/// `range_buckets` over weighted `(value, count)` pairs, producing the same
/// buckets the count-expanded value list would without materializing it.
fn weighted_range_buckets(values: &[(RangeStatsKey, u64)], cap: usize) -> Vec<RangeBucket> {
    let total: u64 = values.iter().map(|(_, count)| *count).sum();
    if total == 0 || cap == 0 {
        return Vec::new();
    }
    let bucket_count = total.min(cap as u64);
    let mut buckets = Vec::with_capacity(bucket_count as usize);
    let mut start = 0u64;
    let mut pair_idx = 0usize;
    let mut cumulative = 0u64;
    for bucket_idx in 0..bucket_count {
        let end =
            (((bucket_idx as u128 + 1) * total as u128).div_ceil(bucket_count as u128)) as u64;
        if end > start {
            while cumulative < end {
                cumulative = cumulative.saturating_add(values[pair_idx].1);
                pair_idx += 1;
            }
            buckets.push(RangeBucket {
                upper_key: values[pair_idx - 1].0,
                count: end - start,
            });
        }
        start = end;
    }
    buckets
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
                    && secondary_index_target_matches_stats(
                        &entry.target,
                        block.target_label_id,
                        &block.prop_key,
                    )
            })
    });
    stats.range_index_stats.retain(|block| {
        block.index_id != target_index_id
            && block.sidecar_present_at_build
            && ready_indexes.iter().any(|entry| {
                matches!(entry.kind, SecondaryIndexKind::Range)
                    && entry.index_id == block.index_id
                    && secondary_index_target_matches_stats(
                        &entry.target,
                        block.target_label_id,
                        &block.prop_key,
                    )
            })
    });
    stats.compound_index_stats.retain(|block| {
        block.index_id != target_index_id
            && block.coverage == DeclaredIndexRuntimeCoverageState::Available
            && ready_indexes.iter().any(|entry| {
                let kind = match entry.kind {
                    SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
                    SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
                };
                entry.index_id == block.index_id
                    && kind == block.kind
                    && secondary_index_target_matches_compound_stats(
                        entry,
                        block.target,
                        block.target_label_id,
                        block.field_fingerprint,
                        block.field_count,
                    )
            })
    });
}

fn merge_targeted_declared_index_stats(
    mut stats: SegmentPlannerStatsV1,
    ready_indexes: &[SecondaryIndexManifestEntry],
    target_index_id: u64,
    target_equality_stats: Option<EqualityIndexPlannerStats>,
    target_range_stats: Option<RangeIndexPlannerStats>,
    target_compound_stats: Option<CompoundIndexPlannerStats>,
) -> SegmentPlannerStatsV1 {
    let declared = declared_index_fingerprints(ready_indexes);
    let declaration_fingerprint = declaration_fingerprint(&declared);
    retain_current_declared_index_stats(&mut stats, ready_indexes, target_index_id);

    stats.build_kind = PlannerStatsBuildKind::SecondaryIndexRefresh;
    stats.built_at_ms = 0;
    stats.declared_indexes = declared;
    stats.declaration_fingerprint = declaration_fingerprint;
    stats.truncated |= !stats.general_property_stats_complete;

    if let Some(equality) = target_equality_stats {
        stats.equality_index_stats.push(equality);
    }
    if let Some(range) = target_range_stats {
        stats.range_index_stats.push(range);
    }
    if let Some(compound) = target_compound_stats {
        stats.compound_index_stats.push(compound);
    }
    stats
        .equality_index_stats
        .sort_by_key(|index_stats| index_stats.index_id);
    stats
        .range_index_stats
        .sort_by_key(|index_stats| index_stats.index_id);
    stats.compound_index_stats.sort_by_key(|index_stats| {
        (
            index_stats.index_id,
            declared_index_kind_rank(index_stats.kind),
        )
    });
    stats
}

fn secondary_index_target_matches_stats(
    target: &SecondaryIndexTarget,
    target_label_id: u32,
    prop_key: &str,
) -> bool {
    match target {
        SecondaryIndexTarget::NodeProperty {
            label_id: expected_label_id,
            prop_key: target_prop_key,
        }
        | SecondaryIndexTarget::EdgeProperty {
            label_id: expected_label_id,
            prop_key: target_prop_key,
        } => *expected_label_id == target_label_id && target_prop_key == prop_key,
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => false,
    }
}

fn secondary_index_target_matches_compound_stats(
    entry: &SecondaryIndexManifestEntry,
    target: PlannerStatsDeclaredIndexTarget,
    target_label_id: u32,
    field_fingerprint: u64,
    field_count: u16,
) -> bool {
    let (entry_target, entry_label_id, entry_field_count) = match &entry.target {
        SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => (
            PlannerStatsDeclaredIndexTarget::NodeFieldIndex,
            *label_id,
            fields.len() as u16,
        ),
        SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => (
            PlannerStatsDeclaredIndexTarget::EdgeFieldIndex,
            *label_id,
            fields.len() as u16,
        ),
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. } => {
            return false;
        }
    };
    entry_target == target
        && entry_label_id == target_label_id
        && entry_field_count == field_count
        && secondary_index_declaration_fingerprint_for_entry(entry) == field_fingerprint
}

fn build_minimal_targeted_refresh_stats(
    segment: &SegmentReader,
) -> Result<SegmentPlannerStatsV1, EngineError> {
    let mut label_accs = BTreeMap::new();
    let mut node_ids = Vec::with_capacity(segment.node_meta_count() as usize);
    for index in 0..segment.node_meta_count() as usize {
        let meta = segment.node_meta_at(index)?;
        for &label_id in meta.label_ids.as_slice() {
            observe_label(&mut label_accs, meta.node_id, label_id, meta.updated_at);
        }
        node_ids.push(meta.node_id);
    }

    let mut edge_refs = Vec::with_capacity(segment.edge_meta_count() as usize);
    for index in 0..segment.edge_meta_count() as usize {
        let (
            _edge_id,
            _data_offset,
            _data_len,
            from,
            to,
            label_id,
            _updated_at,
            _weight,
            _valid_from,
            _valid_to,
            _last_write_seq,
        ) = segment.edge_meta_at(index)?;
        edge_refs.push(EdgeMetaRef { label_id, from, to });
    }

    let timestamp_stats = finalize_timestamp_stats_from_label_accs(&label_accs);
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
        node_label_stats: finalize_node_label_stats(label_accs),
        timestamp_stats,
        property_stats: Vec::new(),
        equality_index_stats: Vec::new(),
        range_index_stats: Vec::new(),
        compound_index_stats: Vec::new(),
        adjacency_stats: build_adjacency_stats_from_edge_meta(edge_refs.into_iter()),
        node_id_sample: node_id_sample(node_ids.into_iter()),
    })
}

#[cfg(all(test, unix))]
pub(crate) fn write_planner_stats_sidecar_atomic(
    seg_dir: &Path,
    stats: SegmentPlannerStatsV1,
) -> Result<PlannerStatsWriteOutcome, EngineError> {
    let Some(payload) = planner_stats_sidecar_payload(stats)? else {
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

#[cfg(all(test, unix))]
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

pub(crate) fn planner_stats_sidecar_payload(
    stats: SegmentPlannerStatsV1,
) -> Result<Option<Vec<u8>>, EngineError> {
    serialize_stats_with_limits(
        stats,
        PLANNER_STATS_SOFT_SIDECAR_BYTES,
        PLANNER_STATS_HARD_SIDECAR_BYTES,
    )
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

#[cfg(test)]
enum PlannerStatsReadFailure {
    Missing,
    Unavailable(String),
}

#[cfg(test)]
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
    if file_len > (PLANNER_STATS_HARD_SIDECAR_BYTES + COMPONENT_IDENTITY_HEADER_LEN) as u64 {
        return Err(PlannerStatsReadFailure::Unavailable(format!(
            "planner stats sidecar exceeds hard cap: {} bytes",
            file_len
        )));
    }
    let mut data = Vec::with_capacity(file_len as usize);
    file.take(file_len.saturating_add(1))
        .read_to_end(&mut data)
        .map_err(|error| PlannerStatsReadFailure::Unavailable(error.to_string()))?;
    let data = planner_stats_payload_slice(&data)?;
    if data.len() > PLANNER_STATS_HARD_SIDECAR_BYTES {
        return Err(PlannerStatsReadFailure::Unavailable(format!(
            "planner stats sidecar exceeds hard cap: {} bytes",
            data.len()
        )));
    }
    decode_planner_stats_envelope(
        data,
        expected_segment_id,
        expected_node_count,
        expected_edge_count,
    )
    .map_err(PlannerStatsReadFailure::Unavailable)
}

#[cfg(test)]
fn planner_stats_payload_slice(data: &[u8]) -> Result<&[u8], PlannerStatsReadFailure> {
    if data.len() >= COMPONENT_IDENTITY_HEADER_LEN
        && data[0..COMPONENT_IDENTITY_HEADER_MAGIC.len()] == COMPONENT_IDENTITY_HEADER_MAGIC
    {
        let header = decode_identity_header(data)
            .map_err(|error| PlannerStatsReadFailure::Unavailable(error.to_string()))?;
        let end = header
            .payload_offset
            .checked_add(header.payload_len)
            .ok_or_else(|| {
                PlannerStatsReadFailure::Unavailable(
                    "planner stats identity payload range overflow".into(),
                )
            })?;
        if end > data.len() as u64 {
            return Err(PlannerStatsReadFailure::Unavailable(format!(
                "planner stats identity payload range [{}, {}) exceeds file length {}",
                header.payload_offset,
                end,
                data.len()
            )));
        }
        return Ok(&data[header.payload_offset as usize..end as usize]);
    }
    Ok(data)
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
    let label_counts = validate_node_label_stats(stats)?;
    validate_timestamp_stats(stats, &label_counts)?;
    validate_property_stats(stats, &label_counts)?;
    validate_declared_index_stats(stats, &label_counts)?;
    validate_adjacency_stats(stats)?;
    Ok(())
}

fn validate_node_label_stats(stats: &SegmentPlannerStatsV1) -> Result<BTreeMap<u32, u64>, String> {
    let mut label_counts = BTreeMap::new();
    let mut total = 0u64;
    for label_stat in &stats.node_label_stats {
        if label_stat.node_count == 0 {
            return Err(format!(
                "planner stats label {} has zero node count",
                label_stat.label_id
            ));
        }
        if label_stat.node_count > stats.node_count {
            return Err(format!(
                "planner stats label {} node count {} exceeds segment node count {}",
                label_stat.label_id, label_stat.node_count, stats.node_count
            ));
        }
        if label_counts
            .insert(label_stat.label_id, label_stat.node_count)
            .is_some()
        {
            return Err(format!(
                "planner stats label {} appears more than once",
                label_stat.label_id
            ));
        }
        total = checked_add_count(total, label_stat.node_count, "node label counts")?;
        validate_ordered_option_pair(
            label_stat.min_node_id,
            label_stat.max_node_id,
            "node label id bounds",
        )?;
        validate_ordered_option_pair(
            label_stat.min_updated_at_ms,
            label_stat.max_updated_at_ms,
            "node label updated-at bounds",
        )?;
    }
    if stats.node_label_stats.len() <= 1 && total != stats.node_count {
        return Err(format!(
            "planner stats label counts sum to {}, expected {} for single-label stats",
            total, stats.node_count
        ));
    }
    let max_memberships = stats
        .node_count
        .checked_mul(MAX_NODE_LABELS_PER_NODE as u64)
        .ok_or_else(|| "planner stats label count bound overflow".to_string())?;
    if total < stats.node_count || total > max_memberships {
        return Err(format!(
            "planner stats label counts sum to {}, expected between {} and {}",
            total, stats.node_count, max_memberships
        ));
    }
    Ok(label_counts)
}

fn validate_timestamp_stats(
    stats: &SegmentPlannerStatsV1,
    label_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let mut seen = BTreeMap::new();
    for timestamp in &stats.timestamp_stats {
        let Some(label_count) = label_counts.get(&timestamp.label_id) else {
            return Err(format!(
                "planner stats timestamp section references unknown label {}",
                timestamp.label_id
            ));
        };
        if seen.insert(timestamp.label_id, ()).is_some() {
            return Err(format!(
                "planner stats timestamp section repeats label {}",
                timestamp.label_id
            ));
        }
        if timestamp.count != *label_count {
            return Err(format!(
                "planner stats timestamp count for label {} is {}, expected {}",
                timestamp.label_id, timestamp.count, label_count
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
    label_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let mut seen = BTreeMap::new();
    for prop in &stats.property_stats {
        let Some(label_count) = label_counts.get(&prop.label_id) else {
            return Err(format!(
                "planner stats property {} references unknown label {}",
                prop.prop_key, prop.label_id
            ));
        };
        let key = (prop.label_id, prop.prop_key.as_str());
        if seen.insert(key, ()).is_some() {
            return Err(format!(
                "planner stats property {} for label {} appears more than once",
                prop.prop_key, prop.label_id
            ));
        }
        if prop.present_count > *label_count {
            return Err(format!(
                "planner stats property {} present count exceeds label count",
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
                summary.min_key,
                summary.max_key,
                "property numeric bounds",
            )?;
            validate_range_buckets(summary.count, &summary.buckets)?;
        }
    }
    Ok(())
}

fn validate_declared_index_stats(
    stats: &SegmentPlannerStatsV1,
    label_counts: &BTreeMap<u32, u64>,
) -> Result<(), String> {
    let declared = declared_index_map(stats)?;
    let edge_label_counts = edge_label_counts_from_adjacency_stats(stats);
    let mut equality_seen = BTreeMap::new();
    for equality in &stats.equality_index_stats {
        let Some(declared_index) = declared.get(&equality.index_id) else {
            return Err(format!(
                "planner stats equality index {} has no declaration",
                equality.index_id
            ));
        };
        if declared_index.kind != PlannerStatsDeclaredIndexKind::Equality
            || declared_index.target_label_id != equality.target_label_id
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
        let target_count =
            declared_index_target_count(declared_index, label_counts, &edge_label_counts);
        if equality.total_postings > target_count {
            return Err(format!(
                "planner stats equality index {} postings exceed target count",
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
            || declared_index.target_label_id != range.target_label_id
            || declared_index.prop_key != range.prop_key
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
        let target_count =
            declared_index_target_count(declared_index, label_counts, &edge_label_counts);
        if range.total_entries > target_count {
            return Err(format!(
                "planner stats range index {} entries exceed target count",
                range.index_id
            ));
        }
        validate_ordered_option_pair(range.min_key, range.max_key, "range index bounds")?;
        validate_range_buckets(range.total_entries, &range.buckets)?;
    }

    let mut compound_seen = BTreeMap::new();
    for compound in &stats.compound_index_stats {
        let Some(declared_index) = declared.get(&compound.index_id) else {
            return Err(format!(
                "planner stats compound index {} has no declaration",
                compound.index_id
            ));
        };
        if declared_index.kind != compound.kind
            || declared_index.target != compound.target
            || declared_index.target_label_id != compound.target_label_id
            || declared_index.field_fingerprint != compound.field_fingerprint
            || declared_index.field_count != compound.field_count
        {
            return Err(format!(
                "planner stats compound index {} declaration mismatch",
                compound.index_id
            ));
        }
        if compound_seen.insert(compound.index_id, ()).is_some() {
            return Err(format!(
                "planner stats compound index {} appears more than once",
                compound.index_id
            ));
        }
        let target_count =
            declared_index_target_count(declared_index, label_counts, &edge_label_counts);
        if compound.total_postings > target_count {
            return Err(format!(
                "planner stats compound index {} postings exceed target count",
                compound.index_id
            ));
        }
        if compound.distinct_full_keys > compound.total_postings {
            return Err(format!(
                "planner stats compound index {} distinct keys exceed postings",
                compound.index_id
            ));
        }
        for prefix in &compound.prefix_stats {
            if prefix.distinct_prefixes > compound.distinct_full_keys {
                return Err(format!(
                    "planner stats compound index {} prefix distinct count exceeds full keys",
                    compound.index_id
                ));
            }
            if prefix.max_postings_per_prefix > compound.total_postings {
                return Err(format!(
                    "planner stats compound index {} prefix max exceeds postings",
                    compound.index_id
                ));
            }
            if prefix.exact_prefix_postings.len() > COMPOUND_STATS_EXACT_PREFIX_LIMIT {
                return Err(format!(
                    "planner stats compound index {} exact prefix map exceeds cap",
                    compound.index_id
                ));
            }
        }
        for range in &compound.range_stats {
            if range.total_numeric_entries > compound.total_postings {
                return Err(format!(
                    "planner stats compound index {} range entries exceed postings",
                    compound.index_id
                ));
            }
            validate_ordered_option_pair(range.min_key, range.max_key, "compound range bounds")?;
            validate_range_buckets(range.total_numeric_entries, &range.buckets)?;
        }
    }
    Ok(())
}

fn edge_label_counts_from_adjacency_stats(stats: &SegmentPlannerStatsV1) -> BTreeMap<u32, u64> {
    let mut counts: BTreeMap<u32, u64> = BTreeMap::new();
    for adjacency in &stats.adjacency_stats {
        let Some(edge_label_id) = adjacency.edge_label_id else {
            continue;
        };
        counts
            .entry(edge_label_id)
            .and_modify(|count| *count = (*count).max(adjacency.total_edges))
            .or_insert(adjacency.total_edges);
    }
    counts
}

fn declared_index_target_count(
    declared_index: &DeclaredIndexStatsFingerprint,
    node_label_counts: &BTreeMap<u32, u64>,
    edge_label_counts: &BTreeMap<u32, u64>,
) -> u64 {
    match declared_index.target {
        PlannerStatsDeclaredIndexTarget::NodeProperty
        | PlannerStatsDeclaredIndexTarget::NodeFieldIndex => *node_label_counts
            .get(&declared_index.target_label_id)
            .unwrap_or(&0),
        PlannerStatsDeclaredIndexTarget::EdgeProperty
        | PlannerStatsDeclaredIndexTarget::EdgeFieldIndex => *edge_label_counts
            .get(&declared_index.target_label_id)
            .unwrap_or(&0),
    }
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
        let key = (adjacency.direction, adjacency.edge_label_id);
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
        if adjacency.edge_label_id.is_none() && adjacency.total_edges != stats.edge_count {
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
        .all(|pair| pair[0].upper_key <= pair[1].upper_key)
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

fn observe_label(
    label_accs: &mut BTreeMap<u32, NodeLabelAccumulator>,
    node_id: u64,
    label_id: u32,
    updated_at_ms: i64,
) {
    let acc = label_accs.entry(label_id).or_default();
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

fn finalize_node_label_stats(
    label_accs: BTreeMap<u32, NodeLabelAccumulator>,
) -> Vec<NodeLabelPlannerStats> {
    label_accs
        .into_iter()
        .map(|(label_id, acc)| NodeLabelPlannerStats {
            label_id,
            node_count: acc.node_count,
            min_node_id: acc.min_node_id,
            max_node_id: acc.max_node_id,
            min_updated_at_ms: acc.min_updated_at_ms,
            max_updated_at_ms: acc.max_updated_at_ms,
        })
        .collect()
}

fn finalize_timestamp_stats_from_label_accs(
    label_accs: &BTreeMap<u32, NodeLabelAccumulator>,
) -> Vec<TimestampPlannerStats> {
    label_accs
        .iter()
        .filter_map(|(&label_id, acc)| {
            if acc.updated_values.is_empty() {
                return None;
            }
            let mut values = acc.updated_values.clone();
            values.sort_unstable();
            let min_ms = *values.first().unwrap();
            let max_ms = *values.last().unwrap();
            let buckets = timestamp_buckets(&values, PLANNER_STATS_TIMESTAMP_BUCKETS);
            Some(TimestampPlannerStats {
                label_id,
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
        let SecondaryIndexTarget::NodeProperty { label_id, prop_key } = &entry.target else {
            continue;
        };
        let new_reason = match entry.kind {
            SecondaryIndexKind::Equality => PropertyStatsTrackedReason::DeclaredEquality,
            SecondaryIndexKind::Range => PropertyStatsTrackedReason::DeclaredRange,
        };
        reasons
            .entry((*label_id, prop_key.clone()))
            .and_modify(|reason| *reason = combine_property_reason(*reason, new_reason))
            .or_insert(new_reason);
    }
    reasons
}

fn seed_property_accumulators(
    declared_reasons: &BTreeMap<(u32, String), PropertyStatsTrackedReason>,
    property_candidates: BTreeMap<u32, PropertyKeyCandidateTracker>,
    label_accs: &BTreeMap<u32, NodeLabelAccumulator>,
) -> BTreeMap<(u32, String), PropertyAccumulator> {
    let mut accs = BTreeMap::new();
    for ((label_id, prop_key), reason) in declared_reasons {
        if !label_accs.contains_key(label_id) {
            continue;
        }
        accs.insert(
            (*label_id, prop_key.clone()),
            PropertyAccumulator::new(*label_id, prop_key.clone(), *reason),
        );
    }
    for (label_id, tracker) in property_candidates {
        for prop_key in tracker.into_keys() {
            accs.entry((label_id, prop_key.clone())).or_insert_with(|| {
                PropertyAccumulator::new(
                    label_id,
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
    label_id: u32,
    props: &BTreeMap<String, PropValue>,
) {
    let tracker = candidates.entry(label_id).or_insert_with(|| {
        PropertyKeyCandidateTracker::new(PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_LABEL)
    });
    for key in props.keys() {
        if declared_reasons.contains_key(&(label_id, key.clone())) {
            continue;
        }
        tracker.observe(key);
    }
}

fn observe_selected_node_properties(
    accs: &mut BTreeMap<(u32, String), PropertyAccumulator>,
    label_id: u32,
    props: &BTreeMap<String, PropValue>,
) {
    for (key, value) in props {
        if let Some(acc) = accs.get_mut(&(label_id, key.clone())) {
            acc.observe(value);
        }
    }
}

fn finalize_property_stats(
    accs: BTreeMap<(u32, String), PropertyAccumulator>,
) -> Vec<PropertyPlannerStats> {
    let mut by_label: BTreeMap<u32, Vec<PropertyAccumulator>> = BTreeMap::new();
    for acc in accs.into_values() {
        by_label.entry(acc.label_id).or_default().push(acc);
    }

    let mut stats = Vec::new();
    for (_label_id, mut props) in by_label {
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
                .take(PLANNER_STATS_MAX_PROPERTY_KEYS_PER_LABEL)
                .map(PropertyAccumulator::into_stats),
        );
    }
    stats.sort_by(|a, b| {
        a.label_id
            .cmp(&b.label_id)
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
        .filter(|entry| entry.state == SecondaryIndexState::Ready)
        .map(|entry| {
            let target = planner_stats_declared_index_target(entry);
            let (target_label_id, prop_key, field_fingerprint, field_count) =
                planner_stats_target_field_identity(entry);
            let kind = match entry.kind {
                SecondaryIndexKind::Equality => PlannerStatsDeclaredIndexKind::Equality,
                SecondaryIndexKind::Range => PlannerStatsDeclaredIndexKind::Range,
            };
            DeclaredIndexStatsFingerprint {
                index_id: entry.index_id,
                target,
                kind,
                target_label_id,
                field_fingerprint,
                field_count,
                prop_key,
            }
        })
        .collect();
    declared.sort_by(|a, b| {
        a.index_id
            .cmp(&b.index_id)
            .then_with(|| {
                declared_index_target_rank(a.target).cmp(&declared_index_target_rank(b.target))
            })
            .then_with(|| declared_index_kind_rank(a.kind).cmp(&declared_index_kind_rank(b.kind)))
            .then_with(|| a.target_label_id.cmp(&b.target_label_id))
            .then_with(|| a.field_fingerprint.cmp(&b.field_fingerprint))
            .then_with(|| a.field_count.cmp(&b.field_count))
            .then_with(|| a.prop_key.cmp(&b.prop_key))
    });
    declared
}

pub(crate) fn planner_stats_declared_index_target(
    entry: &SecondaryIndexManifestEntry,
) -> PlannerStatsDeclaredIndexTarget {
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { .. } => PlannerStatsDeclaredIndexTarget::NodeProperty,
        SecondaryIndexTarget::EdgeProperty { .. } => PlannerStatsDeclaredIndexTarget::EdgeProperty,
        SecondaryIndexTarget::NodeFieldIndex { .. } => {
            PlannerStatsDeclaredIndexTarget::NodeFieldIndex
        }
        SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            PlannerStatsDeclaredIndexTarget::EdgeFieldIndex
        }
    }
}

fn planner_stats_target_field_identity(
    entry: &SecondaryIndexManifestEntry,
) -> (u32, String, u64, u16) {
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key }
        | SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => {
            (*label_id, prop_key.clone(), 0, 0)
        }
        SecondaryIndexTarget::NodeFieldIndex { label_id, fields }
        | SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => (
            *label_id,
            String::new(),
            secondary_index_declaration_fingerprint_for_entry(entry),
            fields.len() as u16,
        ),
    }
}

fn declaration_fingerprint(declared: &[DeclaredIndexStatsFingerprint]) -> u64 {
    let mut hash = FNV_OFFSET;
    for entry in declared {
        hash = fnv_update_u64(hash, entry.index_id);
        hash = fnv_update_u8(hash, declared_index_target_rank(entry.target));
        hash = fnv_update_u8(
            hash,
            match entry.kind {
                PlannerStatsDeclaredIndexKind::Equality => 1,
                PlannerStatsDeclaredIndexKind::Range => 2,
            },
        );
        hash = fnv_update_u32(hash, entry.target_label_id);
        hash = fnv_update_u64(hash, entry.field_fingerprint);
        hash = fnv_update_u32(hash, entry.field_count as u32);
        hash = fnv_update_bytes(hash, entry.prop_key.as_bytes());
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

fn declared_index_target_rank(target: PlannerStatsDeclaredIndexTarget) -> u8 {
    match target {
        PlannerStatsDeclaredIndexTarget::NodeProperty => 0,
        PlannerStatsDeclaredIndexTarget::EdgeProperty => 1,
        PlannerStatsDeclaredIndexTarget::NodeFieldIndex => 2,
        PlannerStatsDeclaredIndexTarget::EdgeFieldIndex => 3,
    }
}

fn declared_index_kind_rank(kind: PlannerStatsDeclaredIndexKind) -> u8 {
    match kind {
        PlannerStatsDeclaredIndexKind::Equality => 0,
        PlannerStatsDeclaredIndexKind::Range => 1,
    }
}

fn range_stats_key(key: NumericRangeSortKey) -> RangeStatsKey {
    key.as_bytes()
}

fn push_capped_numeric(values: &mut Vec<RangeStatsKey>, encoded: NumericRangeSortKey) {
    if values.len() < PLANNER_STATS_MAX_DISTINCT_TRACKED_VALUES {
        values.push(range_stats_key(encoded));
    }
}

fn range_summary_from_values(values: &[RangeStatsKey]) -> RangeValueSummary {
    RangeValueSummary {
        count: values.len() as u64,
        min_key: values.first().copied(),
        max_key: values.last().copied(),
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

fn range_buckets(values: &[RangeStatsKey], cap: usize) -> Vec<RangeBucket> {
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
                upper_key: values[end - 1],
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
    fn edge_label_id(&self) -> u32;
    fn source_node_id(&self) -> u64;
    fn target_node_id(&self) -> u64;
}

impl EdgeLike for EdgeRecord {
    fn edge_label_id(&self) -> u32 {
        self.label_id
    }
    fn source_node_id(&self) -> u64 {
        self.from
    }
    fn target_node_id(&self) -> u64 {
        self.to
    }
}

impl<T: EdgeLike + ?Sized> EdgeLike for &T {
    fn edge_label_id(&self) -> u32 {
        (*self).edge_label_id()
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
    label_id: u32,
    from: u64,
    to: u64,
}

impl From<&CompactEdgeMeta> for EdgeMetaRef {
    fn from(meta: &CompactEdgeMeta) -> Self {
        Self {
            label_id: meta.label_id,
            from: meta.from,
            to: meta.to,
        }
    }
}

impl EdgeLike for EdgeMetaRef {
    fn edge_label_id(&self) -> u32 {
        self.label_id
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
        let label_id = edge.edge_label_id();
        for edge_label_id in [None, Some(label_id)] {
            *groups
                .entry((PlannerStatsDirection::Outgoing, edge_label_id))
                .or_default()
                .entry(edge.source_node_id())
                .or_default() += 1;
            *groups
                .entry((PlannerStatsDirection::Incoming, edge_label_id))
                .or_default()
                .entry(edge.target_node_id())
                .or_default() += 1;
        }
    }

    groups
        .into_iter()
        .filter_map(|((direction, edge_label_id), fanouts)| {
            if fanouts.is_empty() {
                return None;
            }
            Some(adjacency_stats_from_fanouts(
                direction,
                edge_label_id,
                fanouts,
            ))
        })
        .collect()
}

fn adjacency_stats_from_fanouts(
    direction: PlannerStatsDirection,
    edge_label_id: Option<u32>,
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
    top_hubs.truncate(PLANNER_STATS_TOP_HUBS_PER_EDGE_LABEL);
    AdjacencyPlannerStats {
        direction,
        edge_label_id,
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
    let label_count = *data.get(start).ok_or_else(|| {
        EngineError::CorruptRecord(format!("node {} record too short for label count", node_id))
    })? as usize;
    if label_count == 0 || label_count > crate::types::MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node {} record has invalid label count {}",
            node_id, label_count
        )));
    }
    let key_len_start = start.checked_add(1 + label_count * 4).ok_or_else(|| {
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

#[cfg(test)]
fn read_secondary_eq_group_counts(path: &Path) -> Result<Option<BTreeMap<u64, u64>>, EngineError> {
    let data = match read_optional_component_payload(path)? {
        Some(data) => data,
        None => return Ok(None),
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

#[cfg(test)]
fn read_secondary_range_encoded_values(
    path: &Path,
) -> Result<Option<Vec<RangeStatsKey>>, EngineError> {
    let data = match read_optional_component_payload(path)? {
        Some(data) => data,
        None => return Ok(None),
    };
    if data.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "secondary range sidecar {} missing header",
            path.display()
        )));
    }
    let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let expected_len = count
        .checked_mul(NUMERIC_RANGE_KEY_BYTES + 8)
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
        let off = 8 + i * (NUMERIC_RANGE_KEY_BYTES + 8);
        let encoded: RangeStatsKey = data[off..off + NUMERIC_RANGE_KEY_BYTES]
            .try_into()
            .expect("fixed numeric range key length");
        NumericRangeSortKey::from_sidecar_bytes(encoded)?;
        encoded_values.push(encoded);
    }
    Ok(Some(encoded_values))
}

#[cfg(test)]
fn read_optional_component_payload(path: &Path) -> Result<Option<Vec<u8>>, EngineError> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if data.len() >= COMPONENT_IDENTITY_HEADER_LEN
        && data[0..COMPONENT_IDENTITY_HEADER_MAGIC.len()] == COMPONENT_IDENTITY_HEADER_MAGIC
    {
        let header = decode_identity_header(&data)?;
        let end = header
            .payload_offset
            .checked_add(header.payload_len)
            .ok_or_else(|| {
                EngineError::CorruptRecord(format!(
                    "component payload range overflows for {}",
                    path.display()
                ))
            })?;
        if end > data.len() as u64 {
            return Err(EngineError::CorruptRecord(format!(
                "component payload range [{}, {}) exceeds file length {} for {}",
                header.payload_offset,
                end,
                data.len(),
                path.display()
            )));
        }
        return Ok(Some(
            data[header.payload_offset as usize..end as usize].to_vec(),
        ));
    }
    Ok(Some(data))
}

#[cfg(all(test, unix))]
fn cleanup_stats_tmp(seg_dir: &Path) {
    let _ = fs::remove_file(seg_dir.join(PLANNER_STATS_TMP_FILENAME));
}

#[cfg(all(test, unix))]
fn fsync_dir(dir: &Path) -> Result<(), EngineError> {
    let d = File::open(dir)?;
    d.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        SecondaryIndexFieldManifest, SecondaryIndexKind, SecondaryIndexManifestEntry,
        SecondaryIndexState, SecondaryIndexTarget,
    };

    fn test_range_key(value: PropValue) -> NumericRangeSortKey {
        numeric_range_sort_key_for_value(&value).expect("test value must encode as numeric key")
    }

    #[test]
    fn max_segment_stale_risk_precompute_matches_iterated_max() {
        // The per-estimate hot path reads a precomputed field; it must agree
        // with iterating the map for every shape, including empty (Unknown).
        let cases: Vec<(Vec<(u64, StalePostingRisk)>, StalePostingRisk)> = vec![
            (vec![], StalePostingRisk::Unknown),
            (vec![(1, StalePostingRisk::Low)], StalePostingRisk::Low),
            (
                vec![(1, StalePostingRisk::Low), (2, StalePostingRisk::High)],
                StalePostingRisk::High,
            ),
            (
                vec![
                    (1, StalePostingRisk::Medium),
                    (2, StalePostingRisk::Unknown),
                ],
                StalePostingRisk::Unknown,
            ),
        ];
        for (entries, expected) in cases {
            let risks: BTreeMap<u64, StalePostingRisk> = entries.into_iter().collect();
            let iterated = risks
                .values()
                .copied()
                .max_by_key(|risk| risk.rank())
                .unwrap_or(StalePostingRisk::Unknown);
            // Production wiring: the setter must keep the precomputed max in
            // lockstep with the map.
            let mut view = PlannerStatsView::default();
            view.set_segment_stale_risks(risks);
            assert_eq!(view.max_segment_stale_risk(), iterated);
            assert_eq!(view.max_segment_stale_risk(), expected);
        }
    }

    fn test_range_key_i64(value: i64) -> NumericRangeSortKey {
        test_range_key(PropValue::Int(value))
    }

    fn test_range_stats_key_i64(value: i64) -> RangeStatsKey {
        range_stats_key(test_range_key_i64(value))
    }

    fn test_range_bucket_i64(value: i64, count: u64) -> RangeBucket {
        RangeBucket {
            upper_key: test_range_stats_key_i64(value),
            count,
        }
    }

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
            node_label_stats: vec![NodeLabelPlannerStats {
                label_id: 7,
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
            compound_index_stats: Vec::new(),
            adjacency_stats: Vec::new(),
            node_id_sample: vec![42],
        }
    }

    fn equality_entry(index_id: u64) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 7,
                prop_key: "color".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn range_entry(index_id: u64) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 7,
                prop_key: "score".to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn compound_node_entry(index_id: u64, kind: SecondaryIndexKind) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "score".to_string(),
                    },
                ],
            },
            kind,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn encode_compound_test_tuple(
        entry: &SecondaryIndexManifestEntry,
        tenant: &str,
        score: i64,
    ) -> Vec<u8> {
        let context = CompoundTupleContext::from_manifest_entry(entry).unwrap();
        let tenant_value = PropValue::String(tenant.to_string());
        let score_value = PropValue::Int(score);
        encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&tenant_value)),
                CompoundFieldValue::Property(Some(&score_value)),
            ],
        )
        .unwrap()
    }

    fn compound_written_entries(entry: &SecondaryIndexManifestEntry) -> Vec<(Vec<u8>, u64)> {
        vec![
            (encode_compound_test_tuple(entry, "acme", 20), 3),
            (encode_compound_test_tuple(entry, "globex", 10), 4),
            (encode_compound_test_tuple(entry, "acme", 10), 1),
            (encode_compound_test_tuple(entry, "acme", 20), 2),
        ]
    }

    #[test]
    fn equality_stats_from_written_groups_match_sidecar_read_stats() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(crate::segment_writer::secondary_indexes_dir(dir.path())).unwrap();
        let entry = equality_entry(41);
        let mut groups = BTreeMap::new();
        groups.insert(100, vec![1, 3, 5]);
        groups.insert(200, vec![2]);
        groups.insert(300, Vec::new());

        crate::segment_writer::write_node_prop_eq_sidecar_to_path(
            &crate::segment_writer::node_prop_eq_sidecar_path(dir.path(), entry.index_id),
            &groups,
        )
        .unwrap();

        let from_written = equality_index_stats_from_written_groups(&entry, &groups);
        let from_sidecar =
            build_equality_index_stats_from_sidecars(dir.path(), std::slice::from_ref(&entry))
                .unwrap()
                .pop()
                .unwrap();
        assert_eq!(from_written, from_sidecar);
    }

    #[test]
    fn range_stats_from_written_entries_match_sidecar_read_stats() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(crate::segment_writer::secondary_indexes_dir(dir.path())).unwrap();
        let entry = range_entry(42);
        let entries = vec![
            (test_range_key_i64(30), 3),
            (test_range_key_i64(10), 1),
            (test_range_key_i64(20), 2),
            (test_range_key_i64(20), 4),
        ];

        crate::segment_writer::write_node_prop_range_sidecar_to_path(
            &crate::segment_writer::node_prop_range_sidecar_path(dir.path(), entry.index_id),
            &entries,
        )
        .unwrap();

        let from_written = range_index_stats_from_written_entries(&entry, &entries);
        let from_sidecar =
            build_range_index_stats_from_sidecars(dir.path(), std::slice::from_ref(&entry))
                .unwrap()
                .pop()
                .unwrap();
        assert_eq!(from_written, from_sidecar);
    }

    #[test]
    fn compound_stats_from_written_entries_are_deterministic_and_distinct() {
        let entry = compound_node_entry(91, SecondaryIndexKind::Range);
        let entries = compound_written_entries(&entry);
        let mut reversed = entries.clone();
        reversed.reverse();

        let stats = compound_index_stats_from_written_entries(
            &entry,
            &entries,
            DeclaredIndexRuntimeCoverageState::Available,
        )
        .unwrap();
        let reversed_stats = compound_index_stats_from_written_entries(
            &entry,
            &reversed,
            DeclaredIndexRuntimeCoverageState::Available,
        )
        .unwrap();

        assert_eq!(stats, reversed_stats);
        assert_eq!(stats.index_id, 91);
        assert_eq!(
            stats.target,
            PlannerStatsDeclaredIndexTarget::NodeFieldIndex
        );
        assert_eq!(stats.kind, PlannerStatsDeclaredIndexKind::Range);
        assert_eq!(stats.target_label_id, 7);
        assert_eq!(stats.field_count, 2);
        assert_ne!(stats.field_fingerprint, 0);
        assert_eq!(stats.total_postings, 4);
        assert_eq!(stats.distinct_full_keys, 3);
        assert_ne!(
            planner_stats_declaration_fingerprint_for_entry(&entry),
            planner_stats_declaration_fingerprint_for_entry(&range_entry(91))
        );

        let prefix_one = stats
            .prefix_stats
            .iter()
            .find(|prefix| prefix.prefix_len == 1)
            .expect("first prefix stats");
        assert_eq!(prefix_one.distinct_prefixes, 2);
        assert_eq!(prefix_one.max_postings_per_prefix, 3);
        assert_eq!(prefix_one.exact_prefix_postings.len(), 2);
        assert!(prefix_one
            .exact_prefix_postings
            .iter()
            .all(|exact| exact.postings > 0));

        let prefix_two = stats
            .prefix_stats
            .iter()
            .find(|prefix| prefix.prefix_len == 2)
            .expect("full tuple prefix stats");
        assert_eq!(prefix_two.distinct_prefixes, 3);
        assert_eq!(prefix_two.max_postings_per_prefix, 2);
        assert_eq!(prefix_two.exact_prefix_postings.len(), 3);

        assert_eq!(stats.range_stats.len(), 1);
        let range = &stats.range_stats[0];
        assert_eq!(range.equality_prefix_len, 1);
        assert_eq!(range.range_field_ordinal, 1);
        assert_eq!(range.total_numeric_entries, 4);
        assert_eq!(range.min_key, Some(test_range_stats_key_i64(10)));
        assert_eq!(range.max_key, Some(test_range_stats_key_i64(20)));
        assert_eq!(
            range.buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
            4
        );
    }

    #[test]
    fn merged_exact_prefix_overflow_is_sticky_across_segments() {
        fn exact(prefix: &[u8], postings: u64) -> CompoundExactPrefixStat {
            CompoundExactPrefixStat {
                encoded_prefix: prefix.to_vec(),
                postings,
            }
        }
        fn segment(
            distinct: u64,
            exact_stats: Vec<CompoundExactPrefixStat>,
        ) -> Vec<CompoundPrefixStats> {
            vec![CompoundPrefixStats {
                prefix_len: 1,
                distinct_prefixes: distinct,
                max_postings_per_prefix: 5,
                exact_prefix_postings: exact_stats,
            }]
        }

        // Overflow during the merge itself: a full complete segment plus two
        // new prefixes crosses the cap and clears the map.
        let mut target = Vec::new();
        let mut overflowed = BTreeSet::new();
        let full: Vec<CompoundExactPrefixStat> = (0..COMPOUND_STATS_EXACT_PREFIX_LIMIT)
            .map(|ordinal| exact(format!("p{ordinal:05}").as_bytes(), 1))
            .collect();
        merge_compound_prefix_stats(
            &mut target,
            &mut overflowed,
            &segment(COMPOUND_STATS_EXACT_PREFIX_LIMIT as u64, full),
        );
        assert_eq!(
            target[0].exact_prefix_postings.len(),
            COMPOUND_STATS_EXACT_PREFIX_LIMIT
        );
        merge_compound_prefix_stats(
            &mut target,
            &mut overflowed,
            &segment(2, vec![exact(b"zz-1", 3), exact(b"zz-2", 4)]),
        );
        assert!(target[0].exact_prefix_postings.is_empty());

        // The bug: a later small complete segment must not rebuild a partial
        // exact map that costing would trust as exact.
        merge_compound_prefix_stats(
            &mut target,
            &mut overflowed,
            &segment(1, vec![exact(b"zz-3", 7)]),
        );
        assert!(
            target[0].exact_prefix_postings.is_empty(),
            "partial exact map must not be rebuilt after overflow"
        );

        // A source segment whose own exact list overflowed (empty list, many
        // distinct prefixes) poisons the merged map the same way.
        let mut target = Vec::new();
        let mut overflowed = BTreeSet::new();
        merge_compound_prefix_stats(
            &mut target,
            &mut overflowed,
            &segment((COMPOUND_STATS_EXACT_PREFIX_LIMIT as u64) + 10, Vec::new()),
        );
        merge_compound_prefix_stats(
            &mut target,
            &mut overflowed,
            &segment(1, vec![exact(b"a", 2)]),
        );
        assert!(
            target[0].exact_prefix_postings.is_empty(),
            "incomplete source segment must poison the merged exact map"
        );
        assert_eq!(
            target[0].distinct_prefixes,
            (COMPOUND_STATS_EXACT_PREFIX_LIMIT as u64) + 11
        );
    }

    #[test]
    fn weighted_range_buckets_match_expanded_range_buckets() {
        let pairs: Vec<(RangeStatsKey, u64)> = (0..200)
            .map(|value| (test_range_stats_key_i64(value), (value % 7 + 1) as u64))
            .collect();
        let expanded: Vec<RangeStatsKey> = pairs
            .iter()
            .flat_map(|(key, count)| std::iter::repeat_n(*key, *count as usize))
            .collect();
        for cap in [1usize, 3, PLANNER_STATS_RANGE_BUCKETS, 1000] {
            assert_eq!(
                weighted_range_buckets(&pairs, cap),
                range_buckets(&expanded, cap),
                "weighted buckets diverge from expanded buckets at cap {cap}"
            );
        }
        assert!(weighted_range_buckets(&[], PLANNER_STATS_RANGE_BUCKETS).is_empty());
    }

    #[test]
    fn compound_stats_builder_rejects_unsorted_streamed_keys() {
        let entry = compound_node_entry(93, SecondaryIndexKind::Equality);
        let later = encode_compound_test_tuple(&entry, "tenant-b", 1);
        let earlier = encode_compound_test_tuple(&entry, "tenant-a", 1);
        let mut builder = CompoundStatsBuilder::new(&entry).unwrap();
        builder.observe(&later).unwrap();
        assert!(builder.observe(&earlier).is_err());
    }

    #[test]
    fn compound_exact_prefix_stats_are_bounded() {
        let entry = compound_node_entry(92, SecondaryIndexKind::Equality);
        let mut entries = Vec::new();
        for index in 0..=COMPOUND_STATS_EXACT_PREFIX_LIMIT {
            entries.push((
                encode_compound_test_tuple(&entry, &format!("tenant-{index}"), index as i64),
                index as u64,
            ));
        }

        let stats = compound_index_stats_from_written_entries(
            &entry,
            &entries,
            DeclaredIndexRuntimeCoverageState::Available,
        )
        .unwrap();

        for prefix in &stats.prefix_stats {
            assert!(prefix.distinct_prefixes > COMPOUND_STATS_EXACT_PREFIX_LIMIT as u64);
            assert!(prefix.exact_prefix_postings.is_empty());
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
            edge_label_id: Some(10),
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

    #[test]
    fn validate_declared_index_stats_uses_edge_label_counts_for_edge_targets() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        stats.node_count = 1;
        stats.edge_count = 3;
        stats.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_label_id: Some(7),
            source_node_count: 1,
            total_edges: 3,
            min_fanout: 3,
            max_fanout: 3,
            p50_fanout: 3,
            p90_fanout: 3,
            p99_fanout: 3,
            top_hubs: Vec::new(),
        });
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::EdgeProperty,
            index_id: 31,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "color".to_string(),
        });
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id: 31,
            target_label_id: 7,
            prop_key: "color".to_string(),
            total_postings: 3,
            value_group_count: 1,
            max_group_postings: 3,
            top_value_hashes: vec![ValueFrequency {
                value_hash: red_hash,
                count: 3,
            }],
            sidecar_present_at_build: true,
        });
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::EdgeProperty,
            index_id: 32,
            kind: PlannerStatsDeclaredIndexKind::Range,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "score".to_string(),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: 32,
            target_label_id: 7,
            prop_key: "score".to_string(),
            total_entries: 3,
            min_key: Some(test_range_stats_key_i64(10)),
            max_key: Some(test_range_stats_key_i64(30)),
            buckets: vec![test_range_bucket_i64(30, 3)],
            sidecar_present_at_build: true,
        });
        let mut node_label_counts = BTreeMap::new();
        node_label_counts.insert(7, 1);

        assert!(validate_declared_index_stats(&stats, &node_label_counts).is_ok());
    }

    fn ready_eq_entry(index_id: u64, label_id: u32, prop_key: &str) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                label_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn ready_edge_eq_entry(
        index_id: u64,
        label_id: u32,
        prop_key: &str,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::EdgeProperty {
                label_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn ready_range_entry(
        index_id: u64,
        label_id: u32,
        prop_key: &str,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                label_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn add_eq_stats(
        stats: &mut SegmentPlannerStatsV1,
        index_id: u64,
        target_label_id: u32,
        prop_key: &str,
        total_postings: u64,
        value_group_count: u64,
        top_value_hashes: Vec<ValueFrequency>,
    ) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            target_label_id,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: prop_key.to_string(),
        });
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id,
            target_label_id,
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
        target_label_id: u32,
        prop_key: &str,
        total_entries: u64,
    ) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id,
            kind: PlannerStatsDeclaredIndexKind::Range,
            target_label_id,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: prop_key.to_string(),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id,
            target_label_id,
            prop_key: prop_key.to_string(),
            total_entries,
            min_key: Some(test_range_stats_key_i64(10)),
            max_key: Some(test_range_stats_key_i64(20)),
            buckets: vec![test_range_bucket_i64(20, total_entries)],
            sidecar_present_at_build: true,
        });
    }

    #[test]
    fn targeted_stats_merge_replaces_only_target_block_and_keeps_siblings() {
        let mut stats = minimal_stats(9);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            3,
            2,
            vec![ValueFrequency {
                value_hash: 111,
                count: 2,
            }],
        );
        add_range_stats(&mut stats, 12, 7, "score", 1);
        let ready_indexes = vec![
            ready_range_entry(12, 7, "score"),
            ready_eq_entry(11, 7, "color"),
        ];
        let replacement_range = RangeIndexPlannerStats {
            index_id: 12,
            target_label_id: 7,
            prop_key: "score".to_string(),
            total_entries: 3,
            min_key: Some(test_range_stats_key_i64(10)),
            max_key: Some(test_range_stats_key_i64(30)),
            buckets: vec![test_range_bucket_i64(30, 3)],
            sidecar_present_at_build: true,
        };

        let merged = merge_targeted_declared_index_stats(
            stats,
            &ready_indexes,
            12,
            None,
            Some(replacement_range),
            None,
        );

        assert_eq!(
            merged.build_kind,
            PlannerStatsBuildKind::SecondaryIndexRefresh
        );
        assert_eq!(merged.equality_index_stats.len(), 1);
        assert_eq!(merged.equality_index_stats[0].index_id, 11);
        assert_eq!(merged.range_index_stats.len(), 1);
        assert_eq!(merged.range_index_stats[0].index_id, 12);
        assert_eq!(merged.range_index_stats[0].total_entries, 3);
        assert_eq!(
            merged
                .range_index_stats
                .iter()
                .filter(|stats| stats.index_id == 12)
                .count(),
            1
        );
        assert_eq!(
            merged
                .declared_indexes
                .iter()
                .map(|declared| declared.index_id)
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
    }

    fn stats_with_timestamp_histogram(
        segment_id: u64,
        label_id: u32,
        count: u64,
        min_ms: i64,
        max_ms: i64,
    ) -> SegmentPlannerStatsV1 {
        let mut stats = minimal_stats(segment_id);
        stats.node_count = count;
        stats.general_property_sampled_node_count = stats
            .general_property_sampled_node_count
            .min(stats.node_count);
        stats.node_label_stats = vec![NodeLabelPlannerStats {
            label_id,
            node_count: count,
            min_node_id: Some(segment_id.saturating_mul(1_000)),
            max_node_id: Some(segment_id.saturating_mul(1_000).saturating_add(count)),
            min_updated_at_ms: Some(min_ms),
            max_updated_at_ms: Some(max_ms),
        }];
        stats.timestamp_stats = vec![TimestampPlannerStats {
            label_id,
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
        label_id: u32,
        prop_key: &'static str,
        count: u64,
        min_value: i64,
        max_value: i64,
    }

    fn add_range_histogram_stats(stats: &mut SegmentPlannerStatsV1, input: RangeHistogramInput) {
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id: input.index_id,
            kind: PlannerStatsDeclaredIndexKind::Range,
            target_label_id: input.label_id,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: input.prop_key.to_string(),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: input.index_id,
            target_label_id: input.label_id,
            prop_key: input.prop_key.to_string(),
            total_entries: input.count,
            min_key: Some(test_range_stats_key_i64(input.min_value)),
            max_key: Some(test_range_stats_key_i64(input.max_value)),
            buckets: vec![test_range_bucket_i64(input.max_value, input.count)],
            sidecar_present_at_build: true,
        });
    }

    fn encode_legacy_enveloped_stats<T: serde::Serialize>(stats: &T) -> Vec<u8> {
        let payload = rmp_serde::to_vec(stats).unwrap();
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
        data
    }

    #[test]
    fn envelope_round_trip() {
        let stats = minimal_stats(9);
        let data = encode_enveloped_stats(&stats).unwrap();
        let decoded = decode_planner_stats_envelope(&data, 9, 1, 0).unwrap();
        assert_eq!(decoded, stats);
    }

    #[test]
    fn legacy_stats_without_compound_fields_decode_with_defaults() {
        #[derive(serde::Serialize)]
        struct LegacyDeclaredIndexStatsFingerprint {
            index_id: u64,
            target: PlannerStatsDeclaredIndexTarget,
            kind: PlannerStatsDeclaredIndexKind,
            target_label_id: u32,
            prop_key: String,
        }

        #[derive(serde::Serialize)]
        struct LegacySegmentPlannerStatsV1 {
            format_version: u32,
            segment_id: u64,
            build_kind: PlannerStatsBuildKind,
            built_at_ms: i64,
            declaration_fingerprint: u64,
            declared_indexes: Vec<LegacyDeclaredIndexStatsFingerprint>,
            node_count: u64,
            edge_count: u64,
            truncated: bool,
            general_property_stats_complete: bool,
            general_property_sampled_node_count: u64,
            general_property_sampled_raw_bytes: u64,
            general_property_budget_exhausted: bool,
            node_label_stats: Vec<NodeLabelPlannerStats>,
            timestamp_stats: Vec<TimestampPlannerStats>,
            property_stats: Vec<PropertyPlannerStats>,
            equality_index_stats: Vec<EqualityIndexPlannerStats>,
            range_index_stats: Vec<RangeIndexPlannerStats>,
            adjacency_stats: Vec<AdjacencyPlannerStats>,
            node_id_sample: Vec<u64>,
        }

        let mut stats = minimal_stats(9);
        add_eq_stats(
            &mut stats,
            11,
            7,
            "color",
            1,
            1,
            vec![ValueFrequency {
                value_hash: hash_prop_equality_key(&PropValue::String("red".to_string())),
                count: 1,
            }],
        );
        let legacy = LegacySegmentPlannerStatsV1 {
            format_version: stats.format_version,
            segment_id: stats.segment_id,
            build_kind: stats.build_kind,
            built_at_ms: stats.built_at_ms,
            declaration_fingerprint: stats.declaration_fingerprint,
            declared_indexes: stats
                .declared_indexes
                .iter()
                .map(|declared| LegacyDeclaredIndexStatsFingerprint {
                    index_id: declared.index_id,
                    target: declared.target,
                    kind: declared.kind,
                    target_label_id: declared.target_label_id,
                    prop_key: declared.prop_key.clone(),
                })
                .collect(),
            node_count: stats.node_count,
            edge_count: stats.edge_count,
            truncated: stats.truncated,
            general_property_stats_complete: stats.general_property_stats_complete,
            general_property_sampled_node_count: stats.general_property_sampled_node_count,
            general_property_sampled_raw_bytes: stats.general_property_sampled_raw_bytes,
            general_property_budget_exhausted: stats.general_property_budget_exhausted,
            node_label_stats: stats.node_label_stats.clone(),
            timestamp_stats: stats.timestamp_stats.clone(),
            property_stats: stats.property_stats.clone(),
            equality_index_stats: stats.equality_index_stats.clone(),
            range_index_stats: stats.range_index_stats.clone(),
            adjacency_stats: stats.adjacency_stats.clone(),
            node_id_sample: stats.node_id_sample.clone(),
        };

        let data = encode_legacy_enveloped_stats(&legacy);
        let decoded = decode_planner_stats_envelope(&data, 9, 1, 0).unwrap();

        assert!(decoded.compound_index_stats.is_empty());
        assert_eq!(decoded.declared_indexes.len(), 1);
        assert_eq!(decoded.declared_indexes[0].prop_key, "color");
        assert_eq!(decoded.declared_indexes[0].field_fingerprint, 0);
        assert_eq!(decoded.declared_indexes[0].field_count, 0);
        assert_eq!(decoded.equality_index_stats, stats.equality_index_stats);
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
    fn envelope_allows_label_memberships_above_physical_node_count() {
        let mut stats = minimal_stats(29);
        stats.node_count = 2;
        stats.general_property_sampled_node_count = 2;
        stats.node_id_sample = vec![41, 42];
        stats.node_label_stats = vec![
            NodeLabelPlannerStats {
                label_id: 7,
                node_count: 2,
                min_node_id: Some(41),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1000),
                max_updated_at_ms: Some(1001),
            },
            NodeLabelPlannerStats {
                label_id: 9,
                node_count: 1,
                min_node_id: Some(42),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1001),
                max_updated_at_ms: Some(1001),
            },
        ];
        stats.timestamp_stats = vec![
            TimestampPlannerStats {
                label_id: 7,
                count: 2,
                min_ms: 1000,
                max_ms: 1001,
                buckets: vec![TimestampBucket {
                    upper_ms: 1001,
                    count: 2,
                }],
            },
            TimestampPlannerStats {
                label_id: 9,
                count: 1,
                min_ms: 1001,
                max_ms: 1001,
                buckets: vec![TimestampBucket {
                    upper_ms: 1001,
                    count: 1,
                }],
            },
        ];

        let data = encode_enveloped_stats(&stats).unwrap();
        let decoded = decode_planner_stats_envelope(&data, 29, 2, 0).unwrap();
        assert_eq!(
            decoded
                .node_label_stats
                .iter()
                .map(|stat| stat.node_count)
                .sum::<u64>(),
            3
        );
        assert_eq!(decoded.node_count, 2);
    }

    #[test]
    fn envelope_rejects_label_memberships_beyond_max_label_bound() {
        let mut stats = minimal_stats(30);
        stats.node_count = 1;
        stats.node_label_stats = (1..=11)
            .map(|label_id| NodeLabelPlannerStats {
                label_id,
                node_count: 1,
                min_node_id: Some(42),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1000),
                max_updated_at_ms: Some(1000),
            })
            .collect();
        stats.timestamp_stats = (1..=11)
            .map(|label_id| TimestampPlannerStats {
                label_id,
                count: 1,
                min_ms: 1000,
                max_ms: 1000,
                buckets: vec![TimestampBucket {
                    upper_ms: 1000,
                    count: 1,
                }],
            })
            .collect();

        assert_decode_err_contains(stats, 1, 0, "expected between 1 and 10");
    }

    #[test]
    fn envelope_rejects_per_label_membership_count_above_node_count() {
        let mut stats = minimal_stats(31);
        stats.node_count = 2;
        stats.general_property_sampled_node_count = 2;
        stats.node_id_sample = vec![41, 42];
        stats.node_label_stats = vec![
            NodeLabelPlannerStats {
                label_id: 7,
                node_count: 3,
                min_node_id: Some(41),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1000),
                max_updated_at_ms: Some(1001),
            },
            NodeLabelPlannerStats {
                label_id: 9,
                node_count: 1,
                min_node_id: Some(42),
                max_node_id: Some(42),
                min_updated_at_ms: Some(1001),
                max_updated_at_ms: Some(1001),
            },
        ];
        stats.timestamp_stats = vec![
            TimestampPlannerStats {
                label_id: 7,
                count: 3,
                min_ms: 1000,
                max_ms: 1001,
                buckets: vec![TimestampBucket {
                    upper_ms: 1001,
                    count: 3,
                }],
            },
            TimestampPlannerStats {
                label_id: 9,
                count: 1,
                min_ms: 1001,
                max_ms: 1001,
                buckets: vec![TimestampBucket {
                    upper_ms: 1001,
                    count: 1,
                }],
            },
        ];

        assert_decode_err_contains(stats, 2, 0, "exceeds segment node count");
    }

    #[test]
    fn envelope_rejects_internal_count_sanity_failures() {
        let mut bad_label_count = minimal_stats(9);
        bad_label_count.node_count = 2;
        bad_label_count.general_property_sampled_node_count = 2;
        bad_label_count.node_id_sample = vec![42, 43];
        assert_decode_err_contains(bad_label_count, 2, 0, "label counts sum");

        let mut bad_property_count = minimal_stats(9);
        bad_property_count
            .property_stats
            .push(PropertyPlannerStats {
                label_id: 7,
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
                    label_id: 7,
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
                label_id: 7,
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
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id: 11,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "color".to_string(),
        };
        let mut bad_equality_count = minimal_stats(9);
        bad_equality_count.declared_indexes.push(declared_eq);
        bad_equality_count
            .equality_index_stats
            .push(EqualityIndexPlannerStats {
                index_id: 11,
                target_label_id: 7,
                prop_key: "color".to_string(),
                total_postings: 2,
                value_group_count: 1,
                max_group_postings: 2,
                top_value_hashes: Vec::new(),
                sidecar_present_at_build: true,
            });
        assert_decode_err_contains(bad_equality_count, 1, 0, "postings exceed");

        let declared_range = DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id: 12,
            kind: PlannerStatsDeclaredIndexKind::Range,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "score".to_string(),
        };
        let mut bad_range_bucket = minimal_stats(9);
        bad_range_bucket.declared_indexes.push(declared_range);
        bad_range_bucket
            .range_index_stats
            .push(RangeIndexPlannerStats {
                index_id: 12,
                target_label_id: 7,
                prop_key: "score".to_string(),
                total_entries: 1,
                min_key: Some(test_range_stats_key_i64(1)),
                max_key: Some(test_range_stats_key_i64(1)),
                buckets: vec![test_range_bucket_i64(1, 2)],
                sidecar_present_at_build: true,
            });
        assert_decode_err_contains(bad_range_bucket, 1, 0, "range buckets sum");

        let mut bad_adjacency = minimal_stats(9);
        bad_adjacency.edge_count = 1;
        bad_adjacency.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_label_id: None,
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
            PropertyKeyCandidateTracker::new(PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_LABEL);
        for idx in 0..PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_LABEL {
            tracker.observe(&format!("one_off_{:04}", idx));
        }
        for _ in 0..32 {
            tracker.observe("zz_late_hot");
        }

        let keys: Vec<_> = tracker.into_keys().collect();
        assert_eq!(
            keys.len(),
            PLANNER_STATS_PROPERTY_KEY_CANDIDATE_CAP_PER_LABEL
        );
        assert!(keys.iter().any(|key| key == "zz_late_hot"));
    }

    #[test]
    fn rollup_tracks_family_coverage_and_declared_equality() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        stats.timestamp_stats.push(TimestampPlannerStats {
            label_id: 7,
            count: 1,
            min_ms: 1000,
            max_ms: 1000,
            buckets: vec![TimestampBucket {
                upper_ms: 1000,
                count: 1,
            }],
        });
        stats.property_stats.push(PropertyPlannerStats {
            label_id: 7,
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
        add_range_stats(&mut stats, 12, 7, "score", 1);
        stats.edge_count = 1;
        stats.adjacency_stats.push(AdjacencyPlannerStats {
            direction: PlannerStatsDirection::Outgoing,
            edge_label_id: Some(5),
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
                ready_range_entry(12, 7, "score"),
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
        assert_eq!(view.node_label_count(7), 1);
        assert_eq!(view.timestamp_coverage.covered_segment_ids, vec![1]);
        assert_eq!(view.node_label_rollups.get(&7).unwrap().label_id, 7);
        assert_eq!(view.timestamp_rollups.get(&7).unwrap().label_id, 7);
        let property = view
            .property_rollups
            .get(&(7, "color".to_string()))
            .unwrap();
        assert_eq!(property.label_id, 7);
        assert_eq!(property.prop_key, "color");
        assert_eq!(property.present_count, 1);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.target_label_id, 7);
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
        assert_eq!(range.target_label_id, 7);
        assert_eq!(range.prop_key, "score");
        assert_eq!(range.total_entries, 1);
        let adjacency = view
            .adjacency_rollups
            .get(&(PlannerStatsDirection::Outgoing, Some(5)))
            .unwrap();
        assert_eq!(adjacency.direction, PlannerStatsDirection::Outgoing);
        assert_eq!(adjacency.edge_label_id, Some(5));
        assert_eq!(adjacency.total_edges, 1);
    }

    #[test]
    fn rollup_declared_index_mismatch_drops_only_index_block() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
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
        assert_eq!(view.node_label_count(7), 1);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.coverage.mismatched_segment_ids, vec![1]);
        assert_eq!(view.equality_segment_estimate(11, 1, &[red_hash]), None);
    }

    #[test]
    fn rollup_declared_index_stats_require_runtime_sidecar_coverage() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
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
            add_range_stats(&mut stats, 12, 7, "score", 1);
            let available = PlannerStatsAvailability::Available(Box::new(stats));
            let segments = vec![PlannerStatsSegmentSnapshot {
                segment_id: 1,
                node_count: 1,
                edge_count: 0,
                availability: &available,
            }];
            let indexes = [
                ready_eq_entry(11, 7, "color"),
                ready_range_entry(12, 7, "score"),
            ];
            let mut runtime_coverage = DeclaredIndexRuntimeCoverage::default();
            runtime_coverage.insert(
                1,
                11,
                PlannerStatsDeclaredIndexTarget::NodeProperty,
                PlannerStatsDeclaredIndexKind::Equality,
                state,
            );
            runtime_coverage.insert(
                1,
                12,
                PlannerStatsDeclaredIndexTarget::NodeProperty,
                PlannerStatsDeclaredIndexKind::Range,
                state,
            );

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
    fn rollup_compound_stats_require_runtime_coverage_and_matching_declaration() {
        let entry = compound_node_entry(93, SecondaryIndexKind::Range);
        let compound_stats = compound_index_stats_from_written_entries(
            &entry,
            &compound_written_entries(&entry),
            DeclaredIndexRuntimeCoverageState::Available,
        )
        .unwrap();
        let mut stats = minimal_stats(1);
        stats.node_count = 4;
        stats.general_property_sampled_node_count = 4;
        stats.node_label_stats = vec![NodeLabelPlannerStats {
            label_id: 7,
            node_count: 4,
            min_node_id: Some(1),
            max_node_id: Some(4),
            min_updated_at_ms: Some(1000),
            max_updated_at_ms: Some(1000),
        }];
        stats.declared_indexes = declared_index_fingerprints(std::slice::from_ref(&entry));
        stats.compound_index_stats.push(compound_stats.clone());
        let available = PlannerStatsAvailability::Available(Box::new(stats.clone()));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 4,
            edge_count: 0,
            availability: &available,
        }];

        for state in [
            DeclaredIndexRuntimeCoverageState::Available,
            DeclaredIndexRuntimeCoverageState::Missing,
            DeclaredIndexRuntimeCoverageState::Corrupt,
            DeclaredIndexRuntimeCoverageState::Unknown,
        ] {
            let mut runtime_coverage = DeclaredIndexRuntimeCoverage::default();
            if state != DeclaredIndexRuntimeCoverageState::Unknown {
                runtime_coverage.insert(
                    1,
                    93,
                    PlannerStatsDeclaredIndexTarget::NodeFieldIndex,
                    PlannerStatsDeclaredIndexKind::Range,
                    state,
                );
            }
            let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
                1,
                &segments,
                std::slice::from_ref(&entry),
                &runtime_coverage,
            );
            let rollup = view.compound_index_rollups.get(&93).unwrap();
            if state == DeclaredIndexRuntimeCoverageState::Available {
                assert_eq!(rollup.coverage.covered_segment_ids, vec![1]);
                assert_eq!(rollup.total_postings, 4);
                assert_eq!(rollup.distinct_full_keys, 3);
                assert_eq!(rollup.prefix_stats, compound_stats.prefix_stats);
                assert_eq!(rollup.range_stats, compound_stats.range_stats);
            } else {
                assert_eq!(rollup.coverage.mismatched_segment_ids, vec![1]);
                assert_eq!(rollup.total_postings, 0);
                assert!(rollup.prefix_stats.is_empty());
                assert!(rollup.range_stats.is_empty());
            }
        }

        let missing = PlannerStatsAvailability::Missing;
        let missing_segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 4,
            edge_count: 0,
            availability: &missing,
        }];
        let view = build_planner_stats_view_from_snapshots(
            1,
            &missing_segments,
            std::slice::from_ref(&entry),
        );
        let rollup = view.compound_index_rollups.get(&93).unwrap();
        assert_eq!(rollup.coverage.uncovered_segment_ids, vec![1]);
        assert_eq!(rollup.total_postings, 0);

        let unavailable = PlannerStatsAvailability::Unavailable {
            reason: "bad compound stats crc".to_string(),
        };
        let unavailable_segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 4,
            edge_count: 0,
            availability: &unavailable,
        }];
        let view = build_planner_stats_view_from_snapshots(
            1,
            &unavailable_segments,
            std::slice::from_ref(&entry),
        );
        let rollup = view.compound_index_rollups.get(&93).unwrap();
        assert_eq!(rollup.coverage.uncovered_segment_ids, vec![1]);
        assert_eq!(rollup.total_postings, 0);

        let mut stale_stats = stats;
        stale_stats.compound_index_stats[0].field_fingerprint = stale_stats.compound_index_stats[0]
            .field_fingerprint
            .wrapping_add(1);
        let stale_available = PlannerStatsAvailability::Available(Box::new(stale_stats));
        let stale_segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 4,
            edge_count: 0,
            availability: &stale_available,
        }];
        let mut runtime_coverage = DeclaredIndexRuntimeCoverage::default();
        runtime_coverage.insert(
            1,
            93,
            PlannerStatsDeclaredIndexTarget::NodeFieldIndex,
            PlannerStatsDeclaredIndexKind::Range,
            DeclaredIndexRuntimeCoverageState::Available,
        );
        let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
            1,
            &stale_segments,
            std::slice::from_ref(&entry),
            &runtime_coverage,
        );
        let rollup = view.compound_index_rollups.get(&93).unwrap();
        assert_eq!(rollup.coverage.mismatched_segment_ids, vec![1]);
        assert_eq!(rollup.total_postings, 0);
    }

    #[test]
    fn rollup_declared_index_stats_treat_unknown_runtime_coverage_as_unavailable() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
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
    fn rollup_declared_index_stats_require_matching_target_coverage() {
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let mut stats = minimal_stats(1);
        stats.declared_indexes.push(DeclaredIndexStatsFingerprint {
            target: PlannerStatsDeclaredIndexTarget::EdgeProperty,
            index_id: 31,
            kind: PlannerStatsDeclaredIndexKind::Equality,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "color".to_string(),
        });
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id: 31,
            target_label_id: 7,
            prop_key: "color".to_string(),
            total_postings: 1,
            value_group_count: 1,
            max_group_postings: 1,
            top_value_hashes: vec![ValueFrequency {
                value_hash: red_hash,
                count: 1,
            }],
            sidecar_present_at_build: true,
        });
        let available = PlannerStatsAvailability::Available(Box::new(stats));
        let segments = vec![PlannerStatsSegmentSnapshot {
            segment_id: 1,
            node_count: 0,
            edge_count: 1,
            availability: &available,
        }];
        let indexes = [ready_edge_eq_entry(31, 7, "color")];
        let mut runtime_coverage = DeclaredIndexRuntimeCoverage::default();
        runtime_coverage.insert(
            1,
            31,
            PlannerStatsDeclaredIndexTarget::NodeProperty,
            PlannerStatsDeclaredIndexKind::Equality,
            DeclaredIndexRuntimeCoverageState::Available,
        );

        let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
            1,
            &segments,
            &indexes,
            &runtime_coverage,
        );
        assert_eq!(
            view.equality_index_rollups
                .get(&31)
                .unwrap()
                .coverage
                .mismatched_segment_ids,
            vec![1]
        );

        runtime_coverage.insert(
            1,
            31,
            PlannerStatsDeclaredIndexTarget::EdgeProperty,
            PlannerStatsDeclaredIndexKind::Equality,
            DeclaredIndexRuntimeCoverageState::Available,
        );
        let view = build_planner_stats_view_from_snapshots_with_runtime_coverage(
            1,
            &segments,
            &indexes,
            &runtime_coverage,
        );
        assert_eq!(
            view.equality_index_rollups
                .get(&31)
                .unwrap()
                .coverage
                .covered_segment_ids,
            vec![1]
        );
    }

    #[test]
    fn rollup_declared_index_label_zero_is_valid_shape() {
        let mut stats = minimal_stats(1);
        add_eq_stats(&mut stats, 11, 0, "color", 0, 0, Vec::new());
        add_range_stats(&mut stats, 12, 0, "score", 0);
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
                ready_range_entry(12, 0, "score"),
            ],
        );

        assert_eq!(
            view.equality_index_rollups
                .get(&11)
                .unwrap()
                .target_label_id,
            0
        );
        assert_eq!(
            view.range_index_rollups.get(&12).unwrap().target_label_id,
            0
        );
    }

    #[test]
    fn rollup_range_and_timestamp_histograms_use_conservative_upper_estimates() {
        let mut stats = minimal_stats(1);
        stats.timestamp_stats = vec![TimestampPlannerStats {
            label_id: 7,
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
            target: PlannerStatsDeclaredIndexTarget::NodeProperty,
            index_id: 12,
            kind: PlannerStatsDeclaredIndexKind::Range,
            target_label_id: 7,
            field_fingerprint: 0,
            field_count: 0,
            prop_key: "score".to_string(),
        });
        stats.range_index_stats.push(RangeIndexPlannerStats {
            index_id: 12,
            target_label_id: 7,
            prop_key: "score".to_string(),
            total_entries: 6,
            min_key: Some(test_range_stats_key_i64(10)),
            max_key: Some(test_range_stats_key_i64(60)),
            buckets: vec![
                test_range_bucket_i64(20, 2),
                test_range_bucket_i64(40, 2),
                test_range_bucket_i64(60, 2),
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
            &[ready_range_entry(12, 7, "score")],
        );

        assert_eq!(
            view.range_index_estimate(
                12,
                Some((test_range_key_i64(25), true)),
                Some((test_range_key_i64(35), true))
            ),
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
            view.range_index_estimate(12, Some((test_range_key_i64(100), true)), None),
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
                label_id: 7,
                prop_key: "score",
                count: 100,
                min_value: 0,
                max_value: 100,
            },
        );
        let mut stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        add_range_histogram_stats(
            &mut stats_b,
            RangeHistogramInput {
                index_id: 12,
                label_id: 7,
                prop_key: "score",
                count: 100,
                min_value: 0,
                max_value: 300,
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
            &[ready_range_entry(12, 7, "score")],
        );

        assert_eq!(
            view.range_index_estimate(
                12,
                Some((test_range_key_i64(50), true)),
                Some((test_range_key_i64(75), true))
            ),
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
                label_id: 7,
                prop_key: "score",
                count: 100,
                min_value: 0,
                max_value: 100,
            },
        );
        let mut stats_b = stats_with_timestamp_histogram(2, 7, 100, 0, 300);
        add_range_histogram_stats(
            &mut stats_b,
            RangeHistogramInput {
                index_id: 12,
                label_id: 7,
                prop_key: "score",
                count: 100,
                min_value: 0,
                max_value: 300,
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
            &[ready_range_entry(12, 7, "score")],
        );

        assert_eq!(
            view.range_index_estimate(12, Some((test_range_key_i64(301), true)), None),
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
                label_id: 7,
                prop_key: "score",
                count: 100,
                min_value: 0,
                max_value: 100,
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
            &[ready_range_entry(12, 7, "score")],
        );

        let range = view.range_index_rollups.get(&12).unwrap();
        assert_eq!(range.coverage.covered_segment_ids, vec![1]);
        assert_eq!(range.coverage.uncovered_segment_ids, vec![2]);
        assert_eq!(
            view.range_index_estimate(
                12,
                Some((test_range_key_i64(50), true)),
                Some((test_range_key_i64(75), true))
            ),
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
    fn timestamp_absent_label_is_exact_zero_only_when_node_label_stats_cover_segments() {
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
        let value_hash = hash_prop_equality_key(&PropValue::String("active".to_string()));
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
        assert_eq!(view.node_label_count(7), 256);
        let equality = view.equality_index_rollups.get(&11).unwrap();
        assert_eq!(equality.coverage.covered_segment_ids.len(), 256);
        assert_eq!(equality.total_postings, 256);
        assert_eq!(equality.top_value_hashes.get(&value_hash), Some(&256));
    }

    #[test]
    fn rollup_many_ready_indexes_leaves_missing_blocks_uncovered() {
        let value_hash = hash_prop_equality_key(&PropValue::String("active".to_string()));
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
        let values = [
            test_range_stats_key_i64(1),
            test_range_stats_key_i64(2),
            test_range_stats_key_i64(3),
            test_range_stats_key_i64(4),
            test_range_stats_key_i64(5),
            test_range_stats_key_i64(6),
            test_range_stats_key_i64(7),
            test_range_stats_key_i64(8),
        ];
        let buckets = range_buckets(&values, 4);
        assert_eq!(
            buckets,
            vec![
                test_range_bucket_i64(2, 2),
                test_range_bucket_i64(4, 2),
                test_range_bucket_i64(6, 2),
                test_range_bucket_i64(8, 2),
            ]
        );
    }

    #[test]
    fn size_reduction_preserves_core_before_skip() {
        let mut stats = minimal_stats(1);
        stats.property_stats.push(PropertyPlannerStats {
            label_id: 1,
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
        assert_eq!(reduced.node_label_stats, stats.node_label_stats);

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
                target: PlannerStatsDeclaredIndexTarget::NodeProperty,
                index_id: 11,
                kind: PlannerStatsDeclaredIndexKind::Equality,
                target_label_id: 7,
                field_fingerprint: 0,
                field_count: 0,
                prop_key: "color".to_string(),
            },
            DeclaredIndexStatsFingerprint {
                target: PlannerStatsDeclaredIndexTarget::NodeProperty,
                index_id: 12,
                kind: PlannerStatsDeclaredIndexKind::Range,
                target_label_id: 7,
                field_fingerprint: 0,
                field_count: 0,
                prop_key: "score".to_string(),
            },
        ];
        stats.equality_index_stats.push(EqualityIndexPlannerStats {
            index_id: 11,
            target_label_id: 7,
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
            target_label_id: 7,
            prop_key: "score".to_string(),
            total_entries: 1,
            min_key: Some(test_range_stats_key_i64(10)),
            max_key: Some(test_range_stats_key_i64(10)),
            buckets: vec![test_range_bucket_i64(10, 1)],
            sidecar_present_at_build: true,
        });
        stats.property_stats.push(PropertyPlannerStats {
            label_id: 7,
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
        for edge_label_id in 0..256 {
            stats.adjacency_stats.push(AdjacencyPlannerStats {
                direction: PlannerStatsDirection::Outgoing,
                edge_label_id: Some(edge_label_id),
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
