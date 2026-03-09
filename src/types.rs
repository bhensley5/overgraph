use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};

// ---------------------------------------------------------------------------
// Identity hasher for engine-generated u64 IDs (node IDs, edge IDs).
//
// Engine IDs are monotonically assigned u64 values — they are never
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
/// normal `HashMap` operations — iteration, indexing, `get`, `contains_key`,
/// etc.
pub type NodeIdMap<V> = HashMap<u64, V, NodeIdBuildHasher>;

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

/// WAL operation types.
#[derive(Debug, Clone)]
pub enum WalOp {
    UpsertNode(NodeRecord),
    UpsertEdge(EdgeRecord),
    DeleteNode { id: u64, deleted_at: i64 },
    DeleteEdge { id: u64, deleted_at: i64 },
}

/// Operation type tags for binary encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpTag {
    UpsertNode = 1,
    UpsertEdge = 2,
    DeleteNode = 3,
    DeleteEdge = 4,
}

impl OpTag {
    pub fn from_u8(v: u8) -> Option<OpTag> {
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
    /// Named prune policies applied automatically during compaction.
    /// Absent from older manifests; defaults to empty.
    #[serde(default)]
    pub prune_policies: BTreeMap<String, PrunePolicy>,
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
}

/// Direction for neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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
    /// Damping factor (probability of following an edge vs. teleporting).
    /// Default: 0.85. Range: (0.0, 1.0).
    pub damping_factor: f64,
    /// Maximum number of power iterations. Default: 20.
    pub max_iterations: u32,
    /// Convergence threshold (L1 norm of rank delta). Default: 1e-6.
    pub epsilon: f64,
    /// Optional edge type filter. Only walk edges of these types.
    pub edge_type_filter: Option<Vec<u32>>,
    /// Optional top-k cutoff on returned results.
    pub max_results: Option<usize>,
}

impl Default for PprOptions {
    fn default() -> Self {
        PprOptions {
            damping_factor: 0.85,
            max_iterations: 20,
            epsilon: 1e-6,
            edge_type_filter: None,
            max_results: None,
        }
    }
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
        /// How often the background thread fsyncs (default: 10ms).
        interval_ms: u64,

        /// Soft trigger: fsync early when buffered bytes exceed this (default: 4MB).
        soft_trigger_bytes: usize,

        /// Hard cap: block incoming writers when buffered bytes exceed this
        /// (default: 16MB). Backpressure prevents unbounded memory growth.
        hard_cap_bytes: usize,
    },
}

impl Default for WalSyncMode {
    fn default() -> Self {
        WalSyncMode::GroupCommit {
            interval_ms: 10,
            soft_trigger_bytes: 4 * 1024 * 1024, // 4 MB
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
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            create_if_missing: true,
            memtable_flush_threshold: 64 * 1024 * 1024, // 64MB
            edge_uniqueness: false,
            compact_after_n_flushes: 3,
            wal_sync_mode: WalSyncMode::default(),
            memtable_hard_cap_bytes: 128 * 1024 * 1024, // 128MB (2x flush threshold)
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
        assert_eq!(opts.memtable_flush_threshold, 64 * 1024 * 1024);
        assert!(!opts.edge_uniqueness);
        assert_eq!(opts.compact_after_n_flushes, 3);
        assert!(matches!(
            opts.wal_sync_mode,
            WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4194304,
                hard_cap_bytes: 16777216,
            }
        ));
        assert_eq!(opts.memtable_hard_cap_bytes, 128 * 1024 * 1024);
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
            prune_policies: BTreeMap::new(),
        };
        let json = serde_json::to_string(&state).unwrap();
        let loaded: ManifestState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.segments.len(), 2);
        assert_eq!(loaded.next_node_id, 151);
        assert_eq!(loaded.next_edge_id, 276);
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
