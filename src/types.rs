use crate::error::EngineError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

// ---------------------------------------------------------------------------
// Identity hasher for engine-generated u64 IDs (node IDs, edge IDs).
//
// Engine IDs are monotonically assigned u64 values. They are never
// adversarial, never strings, and never externally controlled. An identity
// hash (hash(x) = x) eliminates the ~12ns-per-key SipHash overhead that
// the default HashMap hasher imposes, giving 10-20x faster lookups and
// inserts on ID-keyed maps.
//
// `NodeIdMap<V>` is the public type alias users see in return positions.
// Internally the engine also uses `IdMap<V>` / `IdSet` (private aliases
// over the same hasher) for transient working sets.
// ---------------------------------------------------------------------------

/// Identity hasher for engine-generated numeric IDs. Use [`NodeIdMap<V>`]
/// instead of referencing this type directly.
#[doc(hidden)]
#[derive(Default)]
pub struct NodeIdHasher(u64);

impl Hasher for NodeIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut value = 0u64;
        for (index, byte) in bytes.iter().take(8).enumerate() {
            value |= (*byte as u64) << (index * 8);
        }
        self.0 = value;
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.0 = i as u64;
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.0 = i as u64;
    }
}

/// Build-hasher for [`NodeIdHasher`]. Use [`NodeIdMap<V>`] instead.
#[doc(hidden)]
pub type NodeIdBuildHasher = BuildHasherDefault<NodeIdHasher>;

/// A `HashMap` keyed by node or edge ID with identity hashing.
///
/// Returned by graph APIs that produce per-node result maps
/// (`connected_components`, `degrees`, `neighbors_batch`). Supports all
/// normal `HashMap` operations: iteration, indexing, `get`, `contains_key`,
/// etc.
pub type NodeIdMap<V> = HashMap<u64, V, NodeIdBuildHasher>;

/// A `HashSet` of node or edge IDs with identity hashing.
pub type NodeIdSet = HashSet<u64, NodeIdBuildHasher>;

/// Property value types supported in node/edge properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<PropValue>),
    Map(BTreeMap<String, PropValue>),
}

/// Deterministic FNV-1a hash for byte slices.
/// Used to hash property keys and values for index lookups.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Compute a deterministic hash for a property key (string).
pub fn hash_prop_key(key: &str) -> u64 {
    fnv1a(key.as_bytes())
}

/// Compute a deterministic hash for a property value.
/// Uses MessagePack serialization for a canonical byte representation.
pub fn hash_prop_value(value: &PropValue) -> u64 {
    let bytes = rmp_serde::to_vec(value).expect("PropValue must be serializable");
    fnv1a(&bytes)
}

/// Dense vector payload stored on a node.
pub type DenseVector = Vec<f32>;

/// Sparse vector payload stored on a node: `(dimension_id, weight)`.
pub type SparseVector = Vec<(u32, f32)>;

/// Distance metric used for the DB-scoped dense vector space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenseMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

/// HNSW build parameters for the DB-scoped dense vector space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswConfig {
    pub m: u16,
    pub ef_construction: u16,
}

/// Default ANN expansion for dense queries when `VectorSearchRequest.ef_search` is omitted.
pub const DEFAULT_DENSE_EF_SEARCH: usize = 128;

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
        }
    }
}

/// Configuration for the single DB-scoped dense vector space in Phase 19.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenseVectorConfig {
    pub dimension: u32,
    pub metric: DenseMetric,
    #[serde(default)]
    pub hnsw: HnswConfig,
}

/// Search mode for `vector_search`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorSearchMode {
    Dense,
    Sparse,
    Hybrid,
}

/// Fusion strategy for hybrid vector search.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FusionMode {
    /// Weighted reciprocal rank fusion (default).
    /// `score(d) = w_dense / (k + rank_dense) + w_sparse / (k + rank_sparse)`
    #[default]
    WeightedRankFusion,
    /// Unweighted reciprocal rank fusion (ignores `dense_weight`/`sparse_weight`).
    /// `score(d) = 1 / (k + rank_dense) + 1 / (k + rank_sparse)`
    ReciprocalRankFusion,
    /// Weighted score fusion with min-max normalization per modality.
    /// `score(d) = w_dense * norm(dense_score) + w_sparse * norm(sparse_score)`
    WeightedScoreFusion,
}

/// Traversal-shaped graph scope for vector search.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSearchScope {
    pub start_node_id: u64,
    pub max_depth: u32,
    pub direction: Direction,
    pub edge_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

/// Request parameters for dense, sparse, or hybrid vector search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorSearchRequest {
    pub mode: VectorSearchMode,
    pub dense_query: Option<DenseVector>,
    pub sparse_query: Option<SparseVector>,
    pub k: usize,
    pub type_filter: Option<Vec<u32>>,
    pub ef_search: Option<usize>,
    pub scope: Option<VectorSearchScope>,
    pub dense_weight: Option<f32>,
    pub sparse_weight: Option<f32>,
    pub fusion_mode: Option<FusionMode>,
}

/// A scored vector-search hit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorHit {
    pub node_id: u64,
    pub score: f32,
}

/// Validate DB-scoped dense vector configuration.
pub fn validate_dense_vector_config(config: &DenseVectorConfig) -> Result<(), EngineError> {
    if config.dimension == 0 {
        return Err(EngineError::InvalidOperation(
            "dense vector dimension must be > 0".into(),
        ));
    }
    if config.hnsw.m == 0 {
        return Err(EngineError::InvalidOperation(
            "dense HNSW m must be > 0".into(),
        ));
    }
    if config.hnsw.ef_construction == 0 {
        return Err(EngineError::InvalidOperation(
            "dense HNSW ef_construction must be > 0".into(),
        ));
    }
    if config.hnsw.ef_construction < config.hnsw.m {
        return Err(EngineError::InvalidOperation(format!(
            "dense HNSW ef_construction ({}) must be >= m ({})",
            config.hnsw.ef_construction, config.hnsw.m
        )));
    }
    Ok(())
}

fn validate_finite_vector_component(value: f32, context: &str) -> Result<(), EngineError> {
    if !value.is_finite() {
        return Err(EngineError::InvalidOperation(format!(
            "{} contains NaN or infinite value",
            context
        )));
    }
    Ok(())
}

/// Validate a dense vector against the configured DB-scoped dense space.
pub fn validate_dense_vector(
    values: &[f32],
    config: &DenseVectorConfig,
) -> Result<(), EngineError> {
    validate_dense_vector_config(config)?;
    if values.len() != config.dimension as usize {
        return Err(EngineError::InvalidOperation(format!(
            "dense vector length {} does not match configured dimension {}",
            values.len(),
            config.dimension
        )));
    }
    for &value in values {
        validate_finite_vector_component(value, "dense vector")?;
    }
    Ok(())
}

fn canonicalize_sparse_vector_entries(
    mut entries: SparseVector,
) -> Result<Option<SparseVector>, EngineError> {
    if entries.is_empty() {
        return Ok(None);
    }

    for &(_, weight) in &entries {
        validate_finite_vector_component(weight, "sparse vector")?;
        if weight < 0.0 {
            return Err(EngineError::InvalidOperation(
                "sparse vector weights must be non-negative".into(),
            ));
        }
    }
    entries.sort_unstable_by_key(|&(dimension_id, _)| dimension_id);

    let mut canonical = Vec::with_capacity(entries.len());
    for (dimension_id, weight) in entries {
        if let Some((last_dimension_id, last_weight)) = canonical.last_mut() {
            if *last_dimension_id == dimension_id {
                *last_weight += weight;
                continue;
            }
        }
        canonical.push((dimension_id, weight));
    }

    canonical.retain(|&(_, weight)| weight != 0.0);
    if canonical.is_empty() {
        return Ok(None);
    }

    for &(_, weight) in &canonical {
        validate_finite_vector_component(weight, "sparse vector")?;
    }

    Ok(Some(canonical))
}

/// Canonicalize a sparse vector: sort by dimension, merge duplicates, and drop zeros.
pub fn canonicalize_sparse_vector(
    values: &[(u32, f32)],
) -> Result<Option<SparseVector>, EngineError> {
    canonicalize_sparse_vector_entries(values.to_vec())
}

/// Canonicalize an owned sparse vector without taking an extra clone.
pub fn canonicalize_sparse_vector_owned(
    values: SparseVector,
) -> Result<Option<SparseVector>, EngineError> {
    canonicalize_sparse_vector_entries(values)
}

/// A tombstone entry recording when a record was deleted and its last write sequence.
#[derive(Debug, Clone, Copy)]
pub struct TombstoneEntry {
    pub deleted_at: i64,
    pub last_write_seq: u64,
}

/// A node record in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: u64,
    pub type_id: u32,
    pub key: String,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: i64,
    pub updated_at: i64,
    pub weight: f32,
    #[serde(default)]
    pub dense_vector: Option<DenseVector>,
    #[serde(default)]
    pub sparse_vector: Option<SparseVector>,
    #[serde(default)]
    pub last_write_seq: u64,
}

/// An edge record in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeRecord {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub type_id: u32,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: i64,
    pub updated_at: i64,
    pub weight: f32,
    /// Start of the edge's validity window (epoch millis). 0 means "always valid".
    pub valid_from: i64,
    /// End of the edge's validity window (epoch millis). i64::MAX means "still valid / no expiry".
    pub valid_to: i64,
    #[serde(default)]
    pub last_write_seq: u64,
}

/// Request parameters for cursor-based pagination.
///
/// Both fields are optional:
/// - `limit: None` = return all results (backward compat)
/// - `after: None` = start from the beginning
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PageRequest {
    /// Maximum number of items to return. `None` = unlimited.
    pub limit: Option<usize>,
    /// Cursor: return items with IDs strictly greater than this value.
    /// The cursor is the last ID from the previous page.
    pub after: Option<u64>,
}

/// Result of a paginated query.
///
/// `next_cursor` is `None` when there are no more results.
/// To fetch the next page, pass `next_cursor` as `PageRequest::after`.
#[derive(Debug, Clone)]
pub struct PageResult<T> {
    /// The items for this page.
    pub items: Vec<T>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<u64>,
}

/// Request for planner-backed node queries.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeQuery {
    pub type_id: Option<u32>,
    pub ids: Vec<u64>,
    pub keys: Vec<String>,
    pub filter: Option<NodeFilterExpr>,
    pub page: PageRequest,
    pub order: NodeQueryOrder,
    pub allow_full_scan: bool,
}

impl Default for NodeQuery {
    fn default() -> Self {
        Self {
            type_id: None,
            ids: Vec::new(),
            keys: Vec::new(),
            filter: None,
            page: PageRequest::default(),
            order: NodeQueryOrder::NodeIdAsc,
            allow_full_scan: false,
        }
    }
}

/// Recursive boolean filter supported by planner-backed node queries.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeFilterExpr {
    PropertyEquals {
        key: String,
        value: PropValue,
    },
    PropertyIn {
        key: String,
        values: Vec<PropValue>,
    },
    PropertyRange {
        key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    PropertyExists {
        key: String,
    },
    PropertyMissing {
        key: String,
    },
    UpdatedAtRange {
        lower_ms: Option<i64>,
        upper_ms: Option<i64>,
    },
    And(Vec<NodeFilterExpr>),
    Or(Vec<NodeFilterExpr>),
    Not(Box<NodeFilterExpr>),
}

/// Result ordering for planner-backed node queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeQueryOrder {
    NodeIdAsc,
}

/// ID-only result for planner-backed node queries.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryNodeIdsResult {
    pub items: Vec<u64>,
    pub next_cursor: Option<u64>,
}

/// Hydrated result for planner-backed node queries.
#[derive(Debug, Clone)]
pub struct QueryNodesResult {
    pub items: Vec<NodeRecord>,
    pub next_cursor: Option<u64>,
}

/// Request for planner-backed graph pattern queries.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphPatternQuery {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
    pub at_epoch: Option<i64>,
    pub limit: usize,
    pub order: PatternOrder,
}

/// Node variable inside a graph pattern query.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub alias: String,
    pub type_id: Option<u32>,
    pub ids: Vec<u64>,
    pub keys: Vec<String>,
    pub filter: Option<NodeFilterExpr>,
}

/// Edge variable or edge constraint inside a graph pattern query.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub alias: Option<String>,
    pub from_alias: String,
    pub to_alias: String,
    pub direction: Direction,
    pub type_filter: Option<Vec<u32>>,
    pub property_predicates: Vec<EdgePostFilterPredicate>,
}

/// Predicate supported as a bounded post-filter on expanded edges.
#[derive(Debug, Clone, PartialEq)]
pub enum EdgePostFilterPredicate {
    PropertyEquals {
        key: String,
        value: PropValue,
    },
    PropertyRange {
        key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
}

/// Result ordering for planner-backed graph pattern queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternOrder {
    AnchorThenAliasesAsc,
}

/// ID-binding result for planner-backed graph pattern queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPatternResult {
    pub matches: Vec<QueryMatch>,
    pub truncated: bool,
}

/// One graph pattern match. Unnamed edge patterns are constraints only.
///
/// Distinct node aliases bind distinct node IDs. Distinct edge aliases may bind
/// the same edge ID when multiple pattern edge variables are satisfied by the
/// same visible edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMatch {
    pub nodes: BTreeMap<String, u64>,
    pub edges: BTreeMap<String, u64>,
}

/// Kind of planner-backed query represented by a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlanKind {
    NodeQuery,
    PatternQuery,
}

/// Explain output for planner-backed queries.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub kind: QueryPlanKind,
    pub root: QueryPlanNode,
    pub estimated_candidates: Option<u64>,
    pub warnings: Vec<QueryPlanWarning>,
}

/// Explain tree node for planner-backed queries.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlanNode {
    ExplicitIds,
    KeyLookup,
    NodeTypeIndex,
    PropertyEqualityIndex,
    PropertyRangeIndex,
    TimestampIndex,
    AdjacencyExpansion,
    Intersect {
        inputs: Vec<QueryPlanNode>,
    },
    Union {
        inputs: Vec<QueryPlanNode>,
    },
    VerifyNodeFilter {
        input: Box<QueryPlanNode>,
    },
    VerifyEdgePredicates {
        input: Box<QueryPlanNode>,
    },
    PatternExpand {
        anchor_alias: String,
        input: Box<QueryPlanNode>,
    },
    FallbackTypeScan,
    FallbackFullNodeScan,
    EmptyResult,
}

/// Warning emitted by planner explain output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPlanWarning {
    MissingReadyIndex,
    UsingFallbackScan,
    FullScanRequiresOptIn,
    FullScanExplicitlyAllowed,
    UnboundedPatternRejected,
    EdgePropertyPostFilter,
    IndexSkippedAsBroad,
    CandidateCapExceeded,
    RangeCandidateCapExceeded,
    TimestampCandidateCapExceeded,
    VerifyOnlyFilter,
    BooleanBranchFallback,
    PlanningProbeBudgetExceeded,
}

/// Range domain for an optional secondary index declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexRangeDomain {
    Int,
    UInt,
    Float,
}

/// Kind of optional secondary index declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexKind {
    Equality,
    Range { domain: SecondaryIndexRangeDomain },
}

/// Target for an optional secondary index declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexTarget {
    NodeProperty { type_id: u32, prop_key: String },
}

/// Lifecycle state for an optional secondary index declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexState {
    Building,
    Ready,
    Failed,
}

/// Persisted manifest entry for an optional secondary index declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexManifestEntry {
    pub index_id: u64,
    pub target: SecondaryIndexTarget,
    pub kind: SecondaryIndexKind,
    pub state: SecondaryIndexState,
    #[serde(default)]
    pub last_error: Option<String>,
}

/// User-facing information about a node-property optional secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePropertyIndexInfo {
    pub index_id: u64,
    pub type_id: u32,
    pub prop_key: String,
    pub kind: SecondaryIndexKind,
    pub state: SecondaryIndexState,
    pub last_error: Option<String>,
}

/// Bound for a property range query.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyRangeBound {
    Included(PropValue),
    Excluded(PropValue),
}

impl PropertyRangeBound {
    pub fn value(&self) -> &PropValue {
        match self {
            PropertyRangeBound::Included(value) | PropertyRangeBound::Excluded(value) => value,
        }
    }

    pub fn is_inclusive(&self) -> bool {
        matches!(self, PropertyRangeBound::Included(_))
    }
}

/// Cursor for property-range pagination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyRangeCursor {
    pub value: PropValue,
    pub node_id: u64,
}

/// Request parameters for property-range pagination.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PropertyRangePageRequest {
    pub limit: Option<usize>,
    pub after: Option<PropertyRangeCursor>,
}

/// Result page for property-range queries.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyRangePageResult<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<PropertyRangeCursor>,
}

/// WAL operation types.
#[derive(Debug, Clone)]
pub(crate) enum WalOp {
    UpsertNode(NodeRecord),
    UpsertEdge(EdgeRecord),
    DeleteNode { id: u64, deleted_at: i64 },
    DeleteEdge { id: u64, deleted_at: i64 },
}

/// Operation type tags for binary encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OpTag {
    UpsertNode = 1,
    UpsertEdge = 2,
    DeleteNode = 3,
    DeleteEdge = 4,
}

impl OpTag {
    pub(crate) fn from_u8(v: u8) -> Option<OpTag> {
        match v {
            1 => Some(OpTag::UpsertNode),
            2 => Some(OpTag::UpsertEdge),
            3 => Some(OpTag::DeleteNode),
            4 => Some(OpTag::DeleteEdge),
            _ => None,
        }
    }
}

/// Information about a segment on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub id: u64,
    pub node_count: u64,
    pub edge_count: u64,
}

/// Manifest state: the atomic checkpoint of the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestState {
    pub version: u32,
    pub segments: Vec<SegmentInfo>,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    /// DB-scoped dense vector configuration for Phase 19.
    #[serde(default)]
    pub dense_vector: Option<DenseVectorConfig>,
    /// Named prune policies applied automatically during compaction.
    /// Absent from older manifests; defaults to empty.
    #[serde(default)]
    pub prune_policies: BTreeMap<String, PrunePolicy>,
    /// Next engine sequence number to assign. Persisted across flush/reopen.
    #[serde(default)]
    pub next_engine_seq: u64,
    /// Next WAL generation ID to allocate. Monotonically increasing.
    #[serde(default)]
    pub next_wal_generation_id: u64,
    /// WAL generation ID of the currently active (writable) WAL file.
    #[serde(default)]
    pub active_wal_generation_id: u64,
    /// Flush epochs that are in-flight (frozen or published but not yet retired).
    #[serde(default)]
    pub pending_flush_epochs: Vec<FlushEpochMeta>,
    /// Optional secondary index declarations.
    #[serde(default)]
    pub secondary_indexes: Vec<SecondaryIndexManifestEntry>,
    /// Next declaration ID to allocate.
    #[serde(default)]
    pub next_secondary_index_id: u64,
}

/// State of a flush epoch in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlushEpochState {
    /// Memtable frozen, WAL generation retained, segment not yet built.
    FrozenPendingFlush,
    /// Segment published, WAL generation pending deletion.
    PublishedPendingRetire,
}

/// Manifest entry for a flush epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushEpochMeta {
    pub epoch_id: u64,
    pub wal_generation_id: u64,
    pub state: FlushEpochState,
    /// Segment ID, set once the epoch's segment is published.
    pub segment_id: Option<u64>,
}

/// Phase of a running compaction, reported via progress callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    /// Collecting tombstones from all input segments.
    CollectingTombstones,
    /// Merging node records from input segments.
    MergingNodes,
    /// Merging edge records from input segments.
    MergingEdges,
    /// Writing the output segment to disk.
    WritingOutput,
}

/// Progress information reported during compaction via callback.
#[derive(Debug, Clone)]
pub struct CompactionProgress {
    /// Current phase of compaction.
    pub phase: CompactionPhase,
    /// Number of input segments processed so far in the current phase.
    pub segments_processed: usize,
    /// Total number of input segments being compacted.
    pub total_segments: usize,
    /// Records processed so far in the current phase.
    pub records_processed: u64,
    /// Estimated total records in the current phase (0 if unknown).
    pub total_records: u64,
}

/// Stats returned by a compaction run.
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Number of input segments merged.
    pub segments_merged: usize,
    /// Number of nodes in the output segment.
    pub nodes_kept: u64,
    /// Number of nodes removed (tombstoned or superseded).
    pub nodes_removed: u64,
    /// Number of edges in the output segment.
    pub edges_kept: u64,
    /// Number of edges removed (tombstoned, superseded, or dangling from deleted endpoints).
    pub edges_removed: u64,
    /// Wall-clock time for the compaction run in milliseconds.
    pub duration_ms: u64,
    /// Segment ID of the compaction output.
    pub output_segment_id: u64,
    /// Number of nodes auto-pruned by registered compaction policies (subset of nodes_removed).
    pub nodes_auto_pruned: u64,
    /// Number of edges cascade-dropped due to auto-pruned nodes (subset of edges_removed).
    pub edges_auto_pruned: u64,
}

/// Input for batch node upsert (user-facing, no ID or timestamps).
#[derive(Debug, Clone)]
pub struct NodeInput {
    pub type_id: u32,
    pub key: String,
    pub props: BTreeMap<String, PropValue>,
    pub weight: f32,
    pub dense_vector: Option<DenseVector>,
    pub sparse_vector: Option<SparseVector>,
}

/// Options for `upsert_node`. All fields have sensible defaults:
/// empty properties, weight 1.0, no vectors.
///
/// ```
/// # use overgraph::UpsertNodeOptions;
/// let opts = UpsertNodeOptions::default(); // empty props, weight 1.0
/// let opts = UpsertNodeOptions { weight: 2.5, ..Default::default() };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertNodeOptions {
    /// Node properties. Default: empty.
    pub props: BTreeMap<String, PropValue>,
    /// Node weight. Default: 1.0.
    pub weight: f32,
    /// Optional dense vector payload.
    pub dense_vector: Option<DenseVector>,
    /// Optional sparse vector payload.
    pub sparse_vector: Option<SparseVector>,
}

impl Default for UpsertNodeOptions {
    fn default() -> Self {
        Self {
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }
    }
}

/// Options for `upsert_edge`. All fields have sensible defaults:
/// empty properties, weight 1.0, no validity window override.
///
/// ```
/// # use overgraph::UpsertEdgeOptions;
/// let opts = UpsertEdgeOptions::default(); // empty props, weight 1.0
/// let opts = UpsertEdgeOptions { weight: 0.5, ..Default::default() };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertEdgeOptions {
    /// Edge properties. Default: empty.
    pub props: BTreeMap<String, PropValue>,
    /// Edge weight. Default: 1.0.
    pub weight: f32,
    /// Start of validity window (epoch millis). Default: None (uses created_at).
    pub valid_from: Option<i64>,
    /// End of validity window (epoch millis). Default: None (no expiry).
    pub valid_to: Option<i64>,
}

impl Default for UpsertEdgeOptions {
    fn default() -> Self {
        Self {
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }
    }
}

/// Process-local reference assigned to an intent staged in a write transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxnLocalRef {
    Slot(u32),
    Alias(String),
}

/// Reference to a node target inside a write transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxnNodeRef {
    Id(u64),
    Key { type_id: u32, key: String },
    Local(TxnLocalRef),
}

/// Reference to an edge target inside a write transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxnEdgeRef {
    Id(u64),
    Triple {
        from: TxnNodeRef,
        to: TxnNodeRef,
        type_id: u32,
    },
    Local(TxnLocalRef),
}

/// Ordered logical write intent staged by a write transaction.
#[derive(Debug, Clone, PartialEq)]
pub enum TxnIntent {
    UpsertNode {
        alias: Option<String>,
        type_id: u32,
        key: String,
        options: UpsertNodeOptions,
    },
    UpsertEdge {
        alias: Option<String>,
        from: TxnNodeRef,
        to: TxnNodeRef,
        type_id: u32,
        options: UpsertEdgeOptions,
    },
    DeleteNode {
        target: TxnNodeRef,
    },
    DeleteEdge {
        target: TxnEdgeRef,
    },
    InvalidateEdge {
        target: TxnEdgeRef,
        valid_to: i64,
    },
}

/// Node view returned by bounded write-transaction reads.
#[derive(Debug, Clone, PartialEq)]
pub struct TxnNodeView {
    pub id: Option<u64>,
    pub local: Option<TxnLocalRef>,
    pub type_id: u32,
    pub key: String,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f32,
    pub dense_vector: Option<DenseVector>,
    pub sparse_vector: Option<SparseVector>,
}

/// Edge view returned by bounded write-transaction reads.
#[derive(Debug, Clone, PartialEq)]
pub struct TxnEdgeView {
    pub id: Option<u64>,
    pub local: Option<TxnLocalRef>,
    pub from: TxnNodeRef,
    pub to: TxnNodeRef,
    pub type_id: u32,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f32,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

/// Result returned by a successful write-transaction commit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TxnCommitResult {
    pub node_ids: Vec<u64>,
    pub edge_ids: Vec<u64>,
    pub local_node_ids: BTreeMap<TxnLocalRef, u64>,
    pub local_edge_ids: BTreeMap<TxnLocalRef, u64>,
}

impl TxnCommitResult {
    pub fn node_id(&self, target: &TxnNodeRef) -> Option<u64> {
        match target {
            TxnNodeRef::Id(id) => Some(*id),
            TxnNodeRef::Local(local) => self.local_node_ids.get(local).copied(),
            TxnNodeRef::Key { .. } => None,
        }
    }

    pub fn edge_id(&self, target: &TxnEdgeRef) -> Option<u64> {
        match target {
            TxnEdgeRef::Id(id) => Some(*id),
            TxnEdgeRef::Local(local) => self.local_edge_ids.get(local).copied(),
            TxnEdgeRef::Triple { .. } => None,
        }
    }
}

/// Options for `neighbors`, `neighbors_batch`, and `neighbors_paged`.
///
/// For `neighbors_batch`, `limit` is ignored. For `neighbors_paged`,
/// `limit` is ignored (use `PageRequest` instead).
///
/// ```
/// # use overgraph::NeighborOptions;
/// let opts = NeighborOptions::default(); // Outgoing, no filter, no limit
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct NeighborOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only include edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Maximum number of results. Default: None (unlimited).
    pub limit: Option<usize>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Exponential decay lambda for scoring. Default: None (no decay).
    pub decay_lambda: Option<f32>,
}

impl Default for NeighborOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            limit: None,
            at_epoch: None,
            decay_lambda: None,
        }
    }
}

/// Options for `degree`, `degrees`, `sum_edge_weights`, and `avg_edge_weight`.
///
/// ```
/// # use overgraph::DegreeOptions;
/// let opts = DegreeOptions::default(); // Outgoing, no filter
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct DegreeOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only include edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for DegreeOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            at_epoch: None,
        }
    }
}

/// Options for `top_k_neighbors`.
///
/// ```
/// # use overgraph::TopKOptions;
/// let opts = TopKOptions::default(); // Outgoing, Weight scoring
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TopKOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only include edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Scoring mode for ranking. Default: Weight.
    pub scoring: ScoringMode,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for TopKOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            scoring: ScoringMode::Weight,
            at_epoch: None,
        }
    }
}

/// Options for `traverse`.
///
/// ```
/// # use overgraph::TraverseOptions;
/// let opts = TraverseOptions::default(); // min_depth=1, Outgoing
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TraverseOptions {
    /// Minimum hop depth (inclusive). Default: 1.
    pub min_depth: u32,
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only traverse edges of these types. Default: None (all types).
    pub edge_type_filter: Option<Vec<u32>>,
    /// Only emit nodes of these types. Default: None (all types).
    pub node_type_filter: Option<Vec<u32>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Exponential decay lambda for depth-based scoring. Default: None.
    pub decay_lambda: Option<f64>,
    /// Maximum number of results. Default: None (unlimited).
    pub limit: Option<usize>,
    /// Cursor for pagination. Default: None (start from beginning).
    pub cursor: Option<TraversalCursor>,
}

impl Default for TraverseOptions {
    fn default() -> Self {
        Self {
            min_depth: 1,
            direction: Direction::Outgoing,
            edge_type_filter: None,
            node_type_filter: None,
            at_epoch: None,
            decay_lambda: None,
            limit: None,
            cursor: None,
        }
    }
}

/// Options for `extract_subgraph`.
///
/// ```
/// # use overgraph::SubgraphOptions;
/// let opts = SubgraphOptions::default(); // Outgoing, no filter
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SubgraphOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only traverse edges of these types. Default: None (all types).
    pub edge_type_filter: Option<Vec<u32>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for SubgraphOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_type_filter: None,
            at_epoch: None,
        }
    }
}

/// Options for `shortest_path`.
///
/// ```
/// # use overgraph::ShortestPathOptions;
/// let opts = ShortestPathOptions::default(); // Outgoing, BFS (no weight_field)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ShortestPathOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only traverse edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Property key to use as edge weight (Dijkstra). Default: None (BFS hop count).
    pub weight_field: Option<String>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Maximum search depth in hops. Default: None (unlimited).
    pub max_depth: Option<u32>,
    /// Maximum total path cost. Default: None (unlimited).
    pub max_cost: Option<f64>,
}

impl Default for ShortestPathOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            weight_field: None,
            at_epoch: None,
            max_depth: None,
            max_cost: None,
        }
    }
}

/// Options for `all_shortest_paths`.
///
/// ```
/// # use overgraph::AllShortestPathsOptions;
/// let opts = AllShortestPathsOptions::default(); // Outgoing, BFS
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct AllShortestPathsOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only traverse edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Property key to use as edge weight (Dijkstra). Default: None (BFS hop count).
    pub weight_field: Option<String>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Maximum search depth in hops. Default: None (unlimited).
    pub max_depth: Option<u32>,
    /// Maximum total path cost. Default: None (unlimited).
    pub max_cost: Option<f64>,
    /// Maximum number of paths to return. Default: None (all).
    pub max_paths: Option<usize>,
}

impl Default for AllShortestPathsOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            weight_field: None,
            at_epoch: None,
            max_depth: None,
            max_cost: None,
            max_paths: None,
        }
    }
}

/// Options for `is_connected`.
///
/// ```
/// # use overgraph::IsConnectedOptions;
/// let opts = IsConnectedOptions::default(); // Outgoing
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct IsConnectedOptions {
    /// Edge direction. Default: Outgoing.
    pub direction: Direction,
    /// Only traverse edges of these types. Default: None (all types).
    pub type_filter: Option<Vec<u32>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Maximum search depth in hops. Default: None (unlimited).
    pub max_depth: Option<u32>,
}

impl Default for IsConnectedOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            type_filter: None,
            at_epoch: None,
            max_depth: None,
        }
    }
}

/// Options for `connected_components` and `component_of`.
///
/// ```
/// # use overgraph::ComponentOptions;
/// let opts = ComponentOptions::default(); // no filters
/// ```
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ComponentOptions {
    /// Only traverse edges of these types. Default: None (all types).
    pub edge_type_filter: Option<Vec<u32>>,
    /// Only include nodes of these types. Default: None (all types).
    pub node_type_filter: Option<Vec<u32>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

/// Input for batch edge upsert (user-facing, no ID or timestamps).
#[derive(Debug, Clone)]
pub struct EdgeInput {
    pub from: u64,
    pub to: u64,
    pub type_id: u32,
    pub props: BTreeMap<String, PropValue>,
    pub weight: f32,
    /// Optional start of validity window. If None, defaults to created_at.
    pub valid_from: Option<i64>,
    /// Optional end of validity window. If None, defaults to i64::MAX (no expiry).
    pub valid_to: Option<i64>,
}

/// Input for atomic graph patch: mixed mutations in a single WAL batch.
#[derive(Debug, Clone, Default)]
pub struct GraphPatch {
    pub upsert_nodes: Vec<NodeInput>,
    pub upsert_edges: Vec<EdgeInput>,
    /// Edge invalidations: (edge_id, valid_to_epoch).
    pub invalidate_edges: Vec<(u64, i64)>,
    pub delete_node_ids: Vec<u64>,
    pub delete_edge_ids: Vec<u64>,
}

/// Result of an atomic graph patch.
#[derive(Debug, Clone)]
pub struct PatchResult {
    /// Allocated node IDs, one per upsert_nodes entry (input order preserved).
    pub node_ids: Vec<u64>,
    /// Allocated edge IDs, one per upsert_edges entry (input order preserved).
    pub edge_ids: Vec<u64>,
}

/// Policy for pruning (deleting) nodes that match all specified criteria.
/// All fields are optional; when multiple are set, they combine with AND logic.
/// At least one of `max_age_ms` or `max_weight` must be set. An empty policy
/// (or one with only `type_id`) is rejected to prevent accidental mass deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrunePolicy {
    /// Prune nodes whose `updated_at` is older than `now - max_age_ms`.
    pub max_age_ms: Option<i64>,
    /// Prune nodes whose `weight <= max_weight`.
    pub max_weight: Option<f32>,
    /// Scope pruning to a single node type. If None, all types are eligible.
    pub type_id: Option<u32>,
}

/// Result of a prune operation.
#[derive(Debug, Clone)]
pub struct PruneResult {
    /// Number of nodes deleted.
    pub nodes_pruned: u64,
    /// Number of edges cascade-deleted (incident edges of pruned nodes).
    pub edges_pruned: u64,
}

/// Read-only runtime statistics for introspection.
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Bytes buffered in the WAL but not yet fsynced. Always 0 in Immediate mode.
    pub pending_wal_bytes: usize,
    /// Number of on-disk segments (excludes the in-memory memtable).
    pub segment_count: usize,
    /// Number of node tombstones in the memtable (pending deletes).
    pub node_tombstone_count: usize,
    /// Number of edge tombstones in the memtable (pending deletes).
    pub edge_tombstone_count: usize,
    /// Wall-clock timestamp (ms since epoch) of the last completed compaction,
    /// or `None` if no compaction has run since open.
    pub last_compaction_ms: Option<i64>,
    /// The WAL sync mode this database was opened with.
    pub wal_sync_mode: String,
    /// Estimated bytes in the active (mutable) memtable.
    pub active_memtable_bytes: usize,
    /// Estimated bytes across all immutable memtables pending flush.
    pub immutable_memtable_bytes: usize,
    /// Number of immutable memtables pending flush.
    pub immutable_memtable_count: usize,
    /// Number of flush operations currently in flight (enqueued to bg worker).
    pub pending_flush_count: usize,
    /// The WAL generation ID currently being written to.
    pub active_wal_generation_id: u64,
    /// The oldest WAL generation ID still retained for recovery.
    /// Equal to `active_wal_generation_id` when no immutable memtables are pending.
    pub oldest_retained_wal_generation_id: u64,
}

/// Direction for neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Follow outgoing edges (from → to).
    Outgoing,
    /// Follow incoming edges (to ← from).
    Incoming,
    /// Follow edges in both directions.
    Both,
}

/// A single result from a neighbor query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NeighborEntry {
    /// The neighboring node ID.
    pub node_id: u64,
    /// The edge connecting to this neighbor.
    pub edge_id: u64,
    /// The edge's type_id.
    pub edge_type_id: u32,
    /// The edge weight.
    pub weight: f32,
    /// Start of validity window (epoch ms). 0 means always-valid.
    pub valid_from: i64,
    /// End of validity window (epoch ms). i64::MAX means open-ended.
    pub valid_to: i64,
}

/// A single BFS traversal hit emitted by `traverse()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraversalHit {
    /// The discovered node ID.
    pub node_id: u64,
    /// Minimum hop distance from the start node.
    pub depth: u32,
    /// The deterministically chosen edge for this node's minimum-hop layer, or
    /// `None` for the start node. When multiple same-depth candidates exist,
    /// traversal breaks ties by `(source_node_id, edge_id)`.
    pub via_edge_id: Option<u64>,
    /// Optional decay-derived score for the hit.
    pub score: Option<f64>,
}

/// Cursor for `traverse()` pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalCursor {
    /// Depth of the last emitted hit.
    pub depth: u32,
    /// Node ID of the last emitted hit.
    pub last_node_id: u64,
}

/// Result page for traversal queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraversalPageResult {
    /// The items for this page.
    pub items: Vec<TraversalHit>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<TraversalCursor>,
}

/// Scoring mode for top-k neighbor queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScoringMode {
    /// Sort by raw edge weight (descending).
    Weight,
    /// Sort by recency: most recently created edges first (valid_from descending).
    Recency,
    /// Decay-adjusted: `weight * exp(-lambda * age_hours)` where
    /// `age_hours = (reference_time - valid_from) / 3_600_000`.
    DecayAdjusted { lambda: f32 },
}

/// A shortest path result: ordered sequence of nodes and edges with total cost.
#[derive(Debug, Clone, PartialEq)]
pub struct ShortestPath {
    /// Ordered node IDs along the path: `[from, ..., to]`.
    pub nodes: Vec<u64>,
    /// Edge IDs connecting consecutive nodes. `edges.len() == nodes.len() - 1`
    /// (empty when `from == to`).
    pub edges: Vec<u64>,
    /// Total path cost: hop count for BFS, sum of weights for Dijkstra.
    pub total_cost: f64,
}

/// An extracted subgraph: all nodes and edges reachable within N hops of a starting node.
#[derive(Debug, Clone)]
pub struct Subgraph {
    /// All nodes in the subgraph (including the starting node).
    pub nodes: Vec<NodeRecord>,
    /// All edges connecting nodes in the subgraph discovered during traversal.
    pub edges: Vec<EdgeRecord>,
}

/// Options for Personalized PageRank computation.
#[derive(Debug, Clone)]
pub struct PprOptions {
    /// Algorithm used for Personalized PageRank computation.
    pub algorithm: PprAlgorithm,
    /// Damping factor (probability of following an edge vs. teleporting).
    /// Default: 0.85. Range: (0.0, 1.0).
    pub damping_factor: f64,
    /// Maximum number of power iterations. Default: 20.
    pub max_iterations: u32,
    /// Convergence threshold (L1 norm of rank delta). Default: 1e-6.
    pub epsilon: f64,
    /// Residual stopping tolerance for approximate forward-push PPR.
    /// Default: 1e-5. Used only when `algorithm` is `ApproxForwardPush`.
    pub approx_residual_tolerance: f64,
    /// Optional edge type filter. Only walk edges of these types.
    pub edge_type_filter: Option<Vec<u32>>,
    /// Optional top-k cutoff on returned results.
    pub max_results: Option<usize>,
}

/// Algorithm choices for Personalized PageRank.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PprAlgorithm {
    ExactPowerIteration,
    ApproxForwardPush,
}

impl Default for PprOptions {
    fn default() -> Self {
        PprOptions {
            algorithm: PprAlgorithm::ExactPowerIteration,
            damping_factor: 0.85,
            max_iterations: 20,
            epsilon: 1e-6,
            approx_residual_tolerance: 1e-5,
            edge_type_filter: None,
            max_results: None,
        }
    }
}

/// Approximate PPR metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct PprApproxMeta {
    /// Residual tolerance used for forward push.
    pub residual_tolerance: f64,
    /// Number of push operations performed.
    pub pushes: u64,
    /// Maximum residual mass remaining on any node when the algorithm stopped.
    pub max_remaining_residual: f64,
}

/// Result of a Personalized PageRank computation.
#[derive(Debug, Clone)]
pub struct PprResult {
    /// Scored nodes sorted by score descending: (node_id, score).
    pub scores: Vec<(u64, f64)>,
    /// Number of iterations actually performed.
    pub iterations: u32,
    /// Whether the computation converged (L1 delta < epsilon).
    pub converged: bool,
    /// Algorithm used to produce this result.
    pub algorithm: PprAlgorithm,
    /// Approximate-mode metadata when `algorithm` is `ApproxForwardPush`.
    pub approx: Option<PprApproxMeta>,
}

/// Options for graph adjacency export.
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Only include nodes of these types. None means all types.
    pub node_type_filter: Option<Vec<u32>>,
    /// Only include edges of these types. None means all types.
    pub edge_type_filter: Option<Vec<u32>>,
    /// Include edge weights in the output. Default: true.
    pub include_weights: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            node_type_filter: None,
            edge_type_filter: None,
            include_weights: true,
        }
    }
}

/// An exported edge: (from_node_id, to_node_id, edge_type_id, weight).
pub type ExportEdge = (u64, u64, u32, f32);

/// Result of a graph adjacency export.
#[derive(Debug, Clone)]
pub struct AdjacencyExport {
    /// All live node IDs in the exported subgraph.
    pub node_ids: Vec<u64>,
    /// All live edges: (from, to, type_id, weight).
    pub edges: Vec<ExportEdge>,
}

/// WAL sync mode controls the trade-off between write latency and durability.
#[derive(Debug, Clone, PartialEq)]
pub enum WalSyncMode {
    /// Fsync after every write. Maximum durability, highest latency (~4ms/write).
    Immediate,

    /// Background fsync on a timer. Lowest latency, small data-loss window.
    GroupCommit {
        /// How often the background thread fsyncs (default: 50ms).
        interval_ms: u64,

        /// Soft trigger: fsync early when buffered bytes exceed this (default: 2MB).
        soft_trigger_bytes: usize,

        /// Hard cap: block incoming writers when buffered bytes exceed this
        /// (default: 16MB). Backpressure prevents unbounded memory growth.
        hard_cap_bytes: usize,
    },
}

impl Default for WalSyncMode {
    fn default() -> Self {
        WalSyncMode::GroupCommit {
            interval_ms: 50,
            soft_trigger_bytes: 2 * 1024 * 1024, // 2 MB
            hard_cap_bytes: 16 * 1024 * 1024,    // 16 MB
        }
    }
}

/// Options for opening a database.
#[derive(Debug, Clone)]
pub struct DbOptions {
    pub create_if_missing: bool,
    pub memtable_flush_threshold: usize,
    pub edge_uniqueness: bool,
    /// Optional DB-scoped dense vector configuration persisted in the manifest.
    pub dense_vector: Option<DenseVectorConfig>,
    /// Trigger compaction automatically after this many flushes. 0 = disabled.
    pub compact_after_n_flushes: u32,
    /// WAL sync mode. Default: `WalSyncMode::GroupCommit`.
    pub wal_sync_mode: WalSyncMode,
    /// Hard cap on memtable size in bytes. When the memtable reaches this size,
    /// writes trigger a synchronous flush before proceeding. 0 = disabled.
    /// Should be >= memtable_flush_threshold when both are non-zero.
    /// Note: batch operations check backpressure once before writing; a single
    /// large batch may temporarily exceed the cap.
    pub memtable_hard_cap_bytes: usize,
    /// Maximum number of immutable memtables pending flush before writers block.
    /// When the pending immutable queue reaches this count, the next write
    /// triggers a synchronous flush to drain one immutable before proceeding.
    /// Default: 4. Set to 0 to disable immutable count backpressure.
    pub max_immutable_memtables: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            create_if_missing: true,
            memtable_flush_threshold: 128 * 1024 * 1024, // 128MB
            edge_uniqueness: false,
            dense_vector: None,
            compact_after_n_flushes: 4,
            wal_sync_mode: WalSyncMode::default(),
            memtable_hard_cap_bytes: 512 * 1024 * 1024, // 512MB (4x flush threshold)
            max_immutable_memtables: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_default_db_options() {
        let opts = DbOptions::default();
        assert!(opts.create_if_missing);
        assert_eq!(opts.memtable_flush_threshold, 128 * 1024 * 1024);
        assert!(!opts.edge_uniqueness);
        assert!(opts.dense_vector.is_none());
        assert_eq!(opts.compact_after_n_flushes, 4);
        assert!(matches!(
            opts.wal_sync_mode,
            WalSyncMode::GroupCommit {
                interval_ms: 50,
                soft_trigger_bytes: 2097152,
                hard_cap_bytes: 16777216,
            }
        ));
        assert_eq!(opts.memtable_hard_cap_bytes, 512 * 1024 * 1024);
        assert_eq!(opts.max_immutable_memtables, 4);
    }

    #[test]
    fn test_prop_value_equality() {
        assert_eq!(PropValue::Null, PropValue::Null);
        assert_eq!(PropValue::Bool(true), PropValue::Bool(true));
        assert_ne!(PropValue::Int(1), PropValue::Int(2));
        assert_eq!(
            PropValue::String("hello".to_string()),
            PropValue::String("hello".to_string())
        );
        assert_eq!(
            PropValue::Array(vec![PropValue::Int(1), PropValue::Int(2)]),
            PropValue::Array(vec![PropValue::Int(1), PropValue::Int(2)])
        );
    }

    #[test]
    fn test_prop_value_map() {
        let mut inner = BTreeMap::new();
        inner.insert("nested_key".to_string(), PropValue::Int(42));
        inner.insert("flag".to_string(), PropValue::Bool(true));
        let map = PropValue::Map(inner.clone());
        assert_eq!(map, PropValue::Map(inner));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_prop_value_map_msgpack_roundtrip() {
        let mut inner = BTreeMap::new();
        inner.insert("x".to_string(), PropValue::Float(3.14));
        inner.insert("label".to_string(), PropValue::String("hello".into()));
        inner.insert(
            "items".to_string(),
            PropValue::Array(vec![PropValue::Int(1), PropValue::Int(2)]),
        );
        let mut nested = BTreeMap::new();
        nested.insert("deep".to_string(), PropValue::Bool(false));
        inner.insert("child".to_string(), PropValue::Map(nested));

        let map = PropValue::Map(inner);
        let bytes = rmp_serde::to_vec(&map).expect("serialize");
        let decoded: PropValue = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(map, decoded);
    }

    #[test]
    fn test_op_tag_roundtrip() {
        for tag_val in 1u8..=4 {
            let tag = OpTag::from_u8(tag_val).unwrap();
            assert_eq!(tag as u8, tag_val);
        }
        assert!(OpTag::from_u8(0).is_none());
        assert!(OpTag::from_u8(5).is_none());
        assert!(OpTag::from_u8(255).is_none());
    }

    #[test]
    fn test_direction_serde_roundtrip() {
        for dir in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            let json = serde_json::to_string(&dir).unwrap();
            let back: Direction = serde_json::from_str(&json).unwrap();
            assert_eq!(dir, back);
        }
    }

    #[test]
    fn test_neighbor_entry_serde_roundtrip() {
        let entry = NeighborEntry {
            node_id: 42,
            edge_id: 99,
            edge_type_id: 7,
            weight: 0.75,
            valid_from: 1000,
            valid_to: i64::MAX,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: NeighborEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn test_manifest_state_serde() {
        let state = ManifestState {
            version: 1,
            segments: vec![
                SegmentInfo {
                    id: 1,
                    node_count: 100,
                    edge_count: 200,
                },
                SegmentInfo {
                    id: 2,
                    node_count: 50,
                    edge_count: 75,
                },
            ],
            next_node_id: 151,
            next_edge_id: 276,
            dense_vector: Some(DenseVectorConfig {
                dimension: 384,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            prune_policies: BTreeMap::new(),
            next_engine_seq: 0,
            next_wal_generation_id: 0,
            active_wal_generation_id: 0,
            pending_flush_epochs: Vec::new(),
            secondary_indexes: Vec::new(),
            next_secondary_index_id: 1,
        };
        let json = serde_json::to_string(&state).unwrap();
        let loaded: ManifestState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.segments.len(), 2);
        assert_eq!(loaded.next_node_id, 151);
        assert_eq!(loaded.next_edge_id, 276);
        assert_eq!(loaded.dense_vector, state.dense_vector);
    }

    #[test]
    fn test_validate_dense_vector_config_rejects_invalid_values() {
        let err = validate_dense_vector_config(&DenseVectorConfig {
            dimension: 0,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        })
        .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));

        let err = validate_dense_vector_config(&DenseVectorConfig {
            dimension: 8,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 32,
                ef_construction: 16,
            },
        })
        .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
    }

    #[test]
    fn test_validate_dense_vector_rejects_wrong_length_and_non_finite_values() {
        let config = DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::DotProduct,
            hnsw: HnswConfig::default(),
        };

        let err = validate_dense_vector(&[1.0, 2.0], &config).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));

        let err = validate_dense_vector(&[1.0, f32::NAN, 3.0], &config).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
    }

    #[test]
    fn test_canonicalize_sparse_vector_sorts_merges_and_drops_zeros() {
        let canonical = canonicalize_sparse_vector(&[
            (9, 0.0),
            (4, 1.5),
            (2, 2.0),
            (4, 0.5),
            (2, 0.0),
            (7, 3.0),
            (4, 1.0),
        ])
        .unwrap()
        .unwrap();

        assert_eq!(canonical, vec![(2, 2.0), (4, 3.0), (7, 3.0)]);
    }

    #[test]
    fn test_canonicalize_sparse_vector_rejects_non_finite_values() {
        let err = canonicalize_sparse_vector(&[(1, f32::INFINITY)]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
    }

    #[test]
    fn test_canonicalize_sparse_vector_rejects_negative_values() {
        let err = canonicalize_sparse_vector(&[(1, -0.25)]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        assert!(err
            .to_string()
            .contains("sparse vector weights must be non-negative"));
    }

    #[test]
    fn test_upsert_node_options_default() {
        let opts = UpsertNodeOptions::default();
        assert!(opts.props.is_empty());
        assert_eq!(opts.weight, 1.0);
        assert!(opts.dense_vector.is_none());
        assert!(opts.sparse_vector.is_none());
    }

    #[test]
    fn test_upsert_edge_options_default() {
        let opts = UpsertEdgeOptions::default();
        assert!(opts.props.is_empty());
        assert_eq!(opts.weight, 1.0);
        assert!(opts.valid_from.is_none());
        assert!(opts.valid_to.is_none());
    }

    #[test]
    fn test_neighbor_options_default() {
        let opts = NeighborOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.type_filter.is_none());
        assert!(opts.limit.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.decay_lambda.is_none());
    }

    #[test]
    fn test_degree_options_default() {
        let opts = DegreeOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.type_filter.is_none());
        assert!(opts.at_epoch.is_none());
    }

    #[test]
    fn test_traverse_options_default() {
        let opts = TraverseOptions::default();
        assert_eq!(opts.min_depth, 1);
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.edge_type_filter.is_none());
        assert!(opts.node_type_filter.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.decay_lambda.is_none());
        assert!(opts.limit.is_none());
        assert!(opts.cursor.is_none());
    }

    #[test]
    fn test_shortest_path_options_default() {
        let opts = ShortestPathOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.type_filter.is_none());
        assert!(opts.weight_field.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.max_depth.is_none());
        assert!(opts.max_cost.is_none());
    }

    #[test]
    fn test_component_options_default() {
        let opts = ComponentOptions::default();
        assert!(opts.edge_type_filter.is_none());
        assert!(opts.node_type_filter.is_none());
        assert!(opts.at_epoch.is_none());
    }

    #[test]
    fn test_page_request_default() {
        let req = PageRequest::default();
        assert!(req.limit.is_none());
        assert!(req.after.is_none());
    }

    #[test]
    fn test_page_result_last_page() {
        let result: PageResult<u64> = PageResult {
            items: vec![1, 2, 3],
            next_cursor: None,
        };
        assert_eq!(result.items.len(), 3);
        assert!(result.next_cursor.is_none());
    }

    #[test]
    fn test_page_result_has_more() {
        let result: PageResult<u64> = PageResult {
            items: vec![1, 2, 3],
            next_cursor: Some(3),
        };
        assert_eq!(result.items.len(), 3);
        assert_eq!(result.next_cursor, Some(3));
    }
}
