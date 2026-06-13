use crate::error::EngineError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::hash::{BuildHasherDefault, Hasher};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSpan {
    pub offset: usize,
    pub length: usize,
    pub line: u32,
    pub column: u32,
}

impl SourceSpan {
    pub const fn new(offset: usize, length: usize, line: u32, column: u32) -> Self {
        Self {
            offset,
            length,
            line,
            column,
        }
    }

    pub fn end_offset(&self) -> usize {
        self.offset.saturating_add(self.length)
    }
}

pub type GqlParams = BTreeMap<String, GqlParamValue>;

#[derive(Clone, Debug, PartialEq)]
pub enum GqlParamValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GqlParamValue>),
    Map(BTreeMap<String, GqlParamValue>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GqlStatementKind {
    Query,
    Mutation,
    Schema,
    Index,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GqlExecutionMode {
    Auto,
    ReadOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlExecutionOptions {
    pub mode: GqlExecutionMode,
    pub allow_full_scan: bool,
    pub max_rows: usize,
    pub cursor: Option<String>,
    pub max_cursor_bytes: usize,
    pub max_mutation_rows: usize,
    pub max_mutation_ops: usize,
    pub max_pipeline_rows: usize,
    pub max_groups: usize,
    pub max_collect_items: usize,
    pub max_union_branches: usize,
    pub max_subquery_invocations: usize,
    pub max_subquery_depth: usize,
    pub max_shortest_path_pairs: usize,
    pub max_query_bytes: usize,
    pub max_param_bytes: usize,
    pub max_ast_depth: usize,
    pub max_literal_items: usize,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_order_materialization: usize,
    pub max_skip: usize,
    pub include_plan: bool,
    pub profile: bool,
    pub compact_rows: bool,
    pub include_vectors: bool,
}

impl Default for GqlExecutionOptions {
    fn default() -> Self {
        Self {
            mode: GqlExecutionMode::Auto,
            allow_full_scan: false,
            max_rows: 10_000,
            cursor: None,
            max_cursor_bytes: 16 * 1024,
            max_mutation_rows: 10_000,
            max_mutation_ops: 50_000,
            max_pipeline_rows: 65_536,
            max_groups: 65_536,
            max_collect_items: 65_536,
            max_union_branches: 16,
            max_subquery_invocations: 4_096,
            max_subquery_depth: 2,
            max_shortest_path_pairs: 4_096,
            max_query_bytes: 1_048_576,
            max_param_bytes: 1_048_576,
            max_ast_depth: 256,
            max_literal_items: 10_000,
            max_intermediate_bindings: 65_536,
            max_frontier: 65_536,
            max_path_hops: 16,
            max_paths_per_start: 4_096,
            max_order_materialization: 65_536,
            max_skip: 100_000,
            include_plan: false,
            profile: false,
            compact_rows: false,
            include_vectors: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum GqlValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GqlValue>),
    Map(BTreeMap<String, GqlValue>),
    Node(GqlNode),
    Edge(GqlEdge),
    Path(GqlPath),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlNode {
    pub id: Option<u64>,
    pub labels: Option<Vec<String>>,
    pub key: Option<String>,
    pub props: Option<BTreeMap<String, GqlValue>>,
    pub weight: Option<f32>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub dense_vector: Option<Vec<f32>>,
    pub sparse_vector: Option<Vec<(u32, f32)>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlEdge {
    pub id: Option<u64>,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub label: Option<String>,
    pub props: Option<BTreeMap<String, GqlValue>>,
    pub weight: Option<f32>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlPath {
    pub node_ids: Vec<u64>,
    pub edge_ids: Vec<u64>,
    pub nodes: Option<Vec<GqlNode>>,
    pub edges: Option<Vec<GqlEdge>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlRow {
    pub values: Vec<GqlValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlExecutionResult {
    pub kind: GqlStatementKind,
    pub columns: Vec<String>,
    pub rows: Vec<GqlRow>,
    pub next_cursor: Option<String>,
    pub stats: GqlExecutionStats,
    pub mutation_stats: Option<GqlMutationStats>,
    pub schema_stats: Option<GqlSchemaStats>,
    pub index_stats: Option<GqlIndexStats>,
    pub plan: Option<GqlExecutionExplain>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlExecutionStats {
    pub rows_returned: usize,
    pub rows_matched: usize,
    pub rows_after_filter: usize,
    pub intermediate_bindings: usize,
    pub db_hits: usize,
    pub elapsed_us: Option<u64>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlMutationStats {
    pub rows_matched: usize,
    pub mutation_rows: usize,
    pub mutation_ops: usize,
    pub nodes_created: usize,
    pub nodes_updated: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_updated: usize,
    pub edges_deleted: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub skipped_null_targets: usize,
    pub duplicate_targets: usize,
    pub db_hits: usize,
    pub elapsed_us: Option<u64>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlSchemaStats {
    pub operation: String,
    pub targets_checked: u64,
    pub targets_published: u64,
    pub targets_dropped: u64,
    pub checked_records: u64,
    pub violation_count: u64,
    pub truncated: bool,
    pub scan_limit_hit: bool,
    pub elapsed_us: Option<u64>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlIndexStats {
    pub operation: String,
    pub indexes_ensured: u64,
    pub indexes_dropped: u64,
    pub indexes_returned: u64,
    pub elapsed_us: Option<u64>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlExplain {
    pub columns: Vec<String>,
    pub target: GqlLoweringTarget,
    pub native_plan: Option<QueryPlan>,
    pub pushed_down: Vec<String>,
    pub residual: Vec<String>,
    pub projection: Vec<String>,
    pub row_ops: Vec<GqlRowOperation>,
    pub caps: GqlCapSummary,
    pub warnings: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GqlLoweringTarget {
    NodeQuery,
    EdgeQuery,
    GraphRowQuery,
    GraphPipelineQuery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GqlRowOperation {
    ResidualFilter,
    Projection,
    Sort,
    Skip,
    Limit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlCapSummary {
    pub allow_full_scan: bool,
    pub max_rows: usize,
    pub max_intermediate_bindings: usize,
    pub max_skip: usize,
    pub max_query_bytes: usize,
    pub max_param_bytes: usize,
    pub max_ast_depth: usize,
    pub max_literal_items: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlExecutionExplain {
    pub kind: GqlStatementKind,
    pub columns: Vec<String>,
    pub read: Option<GqlExplain>,
    pub mutation: Option<GqlMutationExplain>,
    pub schema: Option<GqlSchemaExplain>,
    pub index: Option<GqlIndexExplain>,
    pub caps: GqlExecutionCapSummary,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlIndexExplain {
    pub operation: String,
    pub targets: Vec<GqlIndexExplainTarget>,
    pub uses_core_write_queue: bool,
    pub publishes_manifest: bool,
    pub creates_labels: bool,
    pub schedules_background_build: bool,
    pub drops_index_data_async: bool,
    pub side_effect_free: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlIndexExplainTarget {
    pub target_kind: String,
    pub label: Option<String>,
    pub fields: Vec<GqlIndexExplainField>,
    pub kind: Option<String>,
    pub action: Option<String>,
    pub compound: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlIndexExplainField {
    pub source: String,
    pub key: Option<String>,
    pub field: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlSchemaExplain {
    pub operation: String,
    pub targets: Vec<GqlSchemaExplainTarget>,
    pub replaces_entire_catalog: bool,
    pub publishes_manifest: bool,
    pub validates_existing_data: bool,
    pub uses_core_write_queue: bool,
    pub side_effect_free: bool,
    pub options: GqlSchemaExplainOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlSchemaExplainTarget {
    pub target_kind: String,
    pub label: Option<String>,
    pub action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlSchemaExplainOptions {
    pub max_violations: Option<usize>,
    pub chunk_size: Option<usize>,
    pub scan_limit: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlMutationExplain {
    pub read_prefix: Option<GqlMutationReadPrefixExplain>,
    pub operations: Vec<GqlMutationOperationExplain>,
    pub return_plan: Option<GqlMutationReturnExplain>,
    pub would_create_node_labels: Vec<String>,
    pub would_create_edge_labels: Vec<String>,
    pub uses_transaction_snapshot: bool,
    pub uses_write_txn: bool,
    pub replacement_adapters: bool,
    pub atomic_commit: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GqlMutationReadPrefixExplain {
    pub graph_row_target: GqlExplain,
    pub internal_columns: Vec<String>,
    pub target_aliases: Vec<String>,
    pub expression_columns: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlMutationOperationExplain {
    pub op: String,
    pub target_alias: Option<String>,
    pub row_multiplicity: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlMutationReturnExplain {
    pub columns: Vec<String>,
    pub order_items: usize,
    pub skip: usize,
    pub limit: Option<usize>,
    pub post_commit_hydration: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlExecutionCapSummary {
    pub allow_full_scan: bool,
    pub max_rows: usize,
    pub max_cursor_bytes: usize,
    pub max_mutation_rows: usize,
    pub max_mutation_ops: usize,
    pub max_pipeline_rows: usize,
    pub max_groups: usize,
    pub max_collect_items: usize,
    pub max_union_branches: usize,
    pub max_subquery_invocations: usize,
    pub max_subquery_depth: usize,
    pub max_shortest_path_pairs: usize,
    pub max_query_bytes: usize,
    pub max_param_bytes: usize,
    pub max_ast_depth: usize,
    pub max_literal_items: usize,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_order_materialization: usize,
    pub max_skip: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GqlSemanticErrorCode {
    DuplicateAlias,
    UnknownVariable,
    InvalidParameter,
    ParameterTypeMismatch,
    InvalidReturnExpression,
    InvalidPropertyAccess,
    DynamicLabelNotSupported,
    DynamicRelationshipTypeNotSupported,
    FullScanNotAllowed,
    ReadOnlyViolation,
}

pub(crate) const LABEL_TOKEN_SCHEMA_VERSION: u32 = 1;
pub(crate) const SCHEMA_CATALOG_VERSION: u32 = 1;
#[allow(dead_code)]
pub(crate) const MAX_NODE_LABELS_PER_NODE: usize = 10;

pub(crate) fn validate_label_token_name(name: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::InvalidOperation(
            "label token name must not be empty".to_string(),
        ));
    }
    if name.len() > 255 {
        return Err(EngineError::InvalidOperation(format!(
            "label token name must be at most 255 UTF-8 bytes, got {}",
            name.len()
        )));
    }
    if name.trim_matches(char::is_whitespace).len() != name.len() {
        return Err(EngineError::InvalidOperation(
            "label token name must not contain leading or trailing whitespace".to_string(),
        ));
    }
    if name
        .chars()
        .any(|ch| ch == '\0' || (ch.is_ascii() && ch.is_control()))
    {
        return Err(EngineError::InvalidOperation(
            "label token name must not contain ASCII control characters or NUL".to_string(),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn validate_public_node_label_list<'a, I>(labels: I) -> Result<(), EngineError>
where
    I: IntoIterator<Item = &'a str>,
{
    ValidatedNodeLabelList::new(labels).map(|_| ())
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) struct ValidatedNodeLabelList<'a> {
    count: u8,
    labels: [&'a str; MAX_NODE_LABELS_PER_NODE],
}

#[allow(dead_code)]
impl<'a> ValidatedNodeLabelList<'a> {
    pub(crate) fn new<I>(labels: I) -> Result<Self, EngineError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut stored: [&'a str; MAX_NODE_LABELS_PER_NODE] = [""; MAX_NODE_LABELS_PER_NODE];
        let mut count = 0usize;
        let mut total_count = 0usize;
        for label in labels {
            validate_label_token_name(label)?;
            total_count += 1;
            if count < MAX_NODE_LABELS_PER_NODE {
                stored[count] = label;
                count += 1;
            }
        }
        if total_count == 0 {
            return Err(EngineError::InvalidOperation(
                "node label set must contain at least one label".to_string(),
            ));
        }
        if total_count > MAX_NODE_LABELS_PER_NODE {
            return Err(EngineError::InvalidOperation(format!(
                "node label set must contain at most {} labels",
                MAX_NODE_LABELS_PER_NODE
            )));
        }
        for idx in 0..count {
            if stored[..idx].iter().any(|&seen| seen == stored[idx]) {
                return Err(EngineError::InvalidOperation(format!(
                    "node label set contains duplicate label '{}'",
                    stored[idx]
                )));
            }
        }
        Ok(Self {
            count: count as u8,
            labels: stored,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.count as usize
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[&'a str] {
        &self.labels[..self.len()]
    }
}

#[allow(dead_code)]
pub(crate) fn validate_node_label_filter(filter: &NodeLabelFilter) -> Result<(), EngineError> {
    validate_public_node_label_list(filter.labels.iter().map(String::as_str))
}

mod node_label_input_seal {
    pub trait Sealed {}
}

/// Converts accepted public Rust node-label inputs into the owned label payload
/// used by queued write requests.
pub trait IntoNodeLabels: node_label_input_seal::Sealed {
    fn into_node_labels(self) -> Vec<String>;
}

impl node_label_input_seal::Sealed for &str {}

impl IntoNodeLabels for &str {
    fn into_node_labels(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl node_label_input_seal::Sealed for String {}

impl IntoNodeLabels for String {
    fn into_node_labels(self) -> Vec<String> {
        vec![self]
    }
}

impl node_label_input_seal::Sealed for &String {}

impl IntoNodeLabels for &String {
    fn into_node_labels(self) -> Vec<String> {
        vec![self.clone()]
    }
}

impl node_label_input_seal::Sealed for &[&str] {}

impl IntoNodeLabels for &[&str] {
    fn into_node_labels(self) -> Vec<String> {
        self.iter().map(|label| (*label).to_string()).collect()
    }
}

impl node_label_input_seal::Sealed for &[String] {}

impl IntoNodeLabels for &[String] {
    fn into_node_labels(self) -> Vec<String> {
        self.to_vec()
    }
}

impl node_label_input_seal::Sealed for Vec<String> {}

impl IntoNodeLabels for Vec<String> {
    fn into_node_labels(self) -> Vec<String> {
        self
    }
}

impl<const N: usize> node_label_input_seal::Sealed for &[&str; N] {}

impl<const N: usize> IntoNodeLabels for &[&str; N] {
    fn into_node_labels(self) -> Vec<String> {
        self.as_slice().into_node_labels()
    }
}

impl<const N: usize> node_label_input_seal::Sealed for &[String; N] {}

impl<const N: usize> IntoNodeLabels for &[String; N] {
    fn into_node_labels(self) -> Vec<String> {
        self.as_slice().into_node_labels()
    }
}

#[doc(hidden)]
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct NodeLabelSet {
    count: u8,
    label_ids: [u32; MAX_NODE_LABELS_PER_NODE],
}

#[allow(dead_code)]
impl NodeLabelSet {
    pub(crate) fn single(label_id: u32) -> Result<Self, EngineError> {
        Self::from_canonical_ids(&[label_id])
    }

    pub(crate) fn from_label_ids<I>(label_ids: I) -> Result<Self, EngineError>
    where
        I: IntoIterator<Item = u32>,
    {
        let mut ids = [0u32; MAX_NODE_LABELS_PER_NODE];
        let mut count = 0usize;
        for label_id in label_ids {
            if count == MAX_NODE_LABELS_PER_NODE {
                return Err(EngineError::InvalidOperation(format!(
                    "node label set must contain at most {} labels",
                    MAX_NODE_LABELS_PER_NODE
                )));
            }
            ids[count] = label_id;
            count += 1;
        }
        if count == 0 {
            return Err(EngineError::InvalidOperation(
                "node label set must contain at least one label".to_string(),
            ));
        }
        ids[..count].sort_unstable();
        Self::from_sorted_prefix(ids, count)
    }

    pub(crate) fn from_canonical_ids(label_ids: &[u32]) -> Result<Self, EngineError> {
        if label_ids.is_empty() {
            return Err(EngineError::InvalidOperation(
                "node label set must contain at least one label".to_string(),
            ));
        }
        if label_ids.len() > MAX_NODE_LABELS_PER_NODE {
            return Err(EngineError::InvalidOperation(format!(
                "node label set must contain at most {} labels",
                MAX_NODE_LABELS_PER_NODE
            )));
        }
        let mut ids = [0u32; MAX_NODE_LABELS_PER_NODE];
        for (idx, &label_id) in label_ids.iter().enumerate() {
            ids[idx] = label_id;
            if label_id == 0 {
                return Err(EngineError::InvalidOperation(
                    "node label token ID 0 is reserved".to_string(),
                ));
            }
            if idx > 0 && label_ids[idx - 1] >= label_id {
                return Err(EngineError::InvalidOperation(
                    "node label IDs must be sorted ascending and unique".to_string(),
                ));
            }
        }
        Ok(Self {
            count: label_ids.len() as u8,
            label_ids: ids,
        })
    }

    fn from_sorted_prefix(
        label_ids: [u32; MAX_NODE_LABELS_PER_NODE],
        count: usize,
    ) -> Result<Self, EngineError> {
        for idx in 0..count {
            if label_ids[idx] == 0 {
                return Err(EngineError::InvalidOperation(
                    "node label token ID 0 is reserved".to_string(),
                ));
            }
            if idx > 0 && label_ids[idx - 1] == label_ids[idx] {
                return Err(EngineError::InvalidOperation(format!(
                    "node label set contains duplicate label ID {}",
                    label_ids[idx]
                )));
            }
        }
        Ok(Self {
            count: count as u8,
            label_ids,
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.count as usize
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u32] {
        &self.label_ids[..self.len()]
    }

    #[inline]
    pub(crate) fn contains(&self, label_id: u32) -> bool {
        self.as_slice().binary_search(&label_id).is_ok()
    }

    #[inline]
    pub(crate) fn contains_all(&self, required: &NodeLabelSet) -> bool {
        required
            .as_slice()
            .iter()
            .all(|&label_id| self.contains(label_id))
    }

    #[inline]
    pub(crate) fn contains_any(&self, candidates: &NodeLabelSet) -> bool {
        candidates
            .as_slice()
            .iter()
            .any(|&label_id| self.contains(label_id))
    }

    #[inline]
    pub(crate) fn single_label_id(&self) -> u32 {
        debug_assert_eq!(self.len(), 1);
        self.as_slice()[0]
    }

    pub(crate) fn require_single_label_id(&self, context: &str) -> Result<u32, EngineError> {
        if self.len() == 1 {
            Ok(self.single_label_id())
        } else {
            Err(EngineError::InvalidOperation(format!(
                "{context} currently supports exactly one node label, got {}",
                self.len()
            )))
        }
    }
}

impl fmt::Debug for NodeLabelSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NodeLabelSet")
            .field(&self.as_slice())
            .finish()
    }
}

/// Match mode for public node-label filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelMatchMode {
    Any,
    All,
}

/// Public node-label filter used by multi-label-capable APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLabelFilter {
    pub labels: Vec<String>,
    pub mode: LabelMatchMode,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedNodeLabelFilter {
    Unconstrained,
    Empty {
        mode: LabelMatchMode,
        unknown_label_count: usize,
    },
    LabelSet {
        mode: LabelMatchMode,
        label_ids: NodeLabelSet,
        unknown_label_count: usize,
    },
}

#[allow(dead_code)]
impl ResolvedNodeLabelFilter {
    pub(crate) fn known(
        mode: LabelMatchMode,
        label_ids: NodeLabelSet,
        unknown_label_count: usize,
    ) -> Self {
        Self::LabelSet {
            mode,
            label_ids,
            unknown_label_count,
        }
    }

    pub(crate) fn empty(mode: LabelMatchMode, unknown_label_count: usize) -> Self {
        Self::Empty {
            mode,
            unknown_label_count,
        }
    }

    #[inline]
    pub(crate) fn mode(&self) -> Option<LabelMatchMode> {
        match self {
            Self::Unconstrained => None,
            Self::Empty { mode, .. } | Self::LabelSet { mode, .. } => Some(*mode),
        }
    }

    #[inline]
    pub(crate) fn label_ids(&self) -> Option<NodeLabelSet> {
        match self {
            Self::LabelSet { label_ids, .. } => Some(*label_ids),
            Self::Unconstrained | Self::Empty { .. } => None,
        }
    }

    #[inline]
    pub(crate) fn is_empty_constraint(&self) -> bool {
        matches!(self, Self::Empty { .. })
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NodeVisibilityMeta {
    pub(crate) label_ids: NodeLabelSet,
    pub(crate) updated_at: i64,
    pub(crate) weight: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NodeVisibilityState {
    Live(NodeVisibilityMeta),
    Deleted,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NodeMetadataForQuery {
    pub(crate) id: u64,
    pub(crate) label_ids: NodeLabelSet,
    pub(crate) updated_at: i64,
    pub(crate) weight: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SelectedNodeFields {
    pub(crate) meta: NodeMetadataForQuery,
    pub(crate) key: Option<String>,
    pub(crate) props: BTreeMap<String, PropValue>,
    pub(crate) created_at: Option<i64>,
    pub(crate) dense_vector: Option<DenseVector>,
    pub(crate) sparse_vector: Option<SparseVector>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeVisibilityState {
    Live,
    Deleted,
    Missing,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct EdgeMetadataForQuery {
    pub(crate) id: u64,
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) label_id: u32,
    pub(crate) updated_at: i64,
    pub(crate) weight: f32,
    pub(crate) valid_from: i64,
    pub(crate) valid_to: i64,
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedEdgeFields {
    pub(crate) meta: EdgeMetadataForQuery,
    pub(crate) props: BTreeMap<String, PropValue>,
    pub(crate) created_at: Option<i64>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct SelectedFieldReadCounters {
    node_selected_field_batches: AtomicUsize,
    node_selected_field_ids: AtomicUsize,
    edge_selected_field_batches: AtomicUsize,
    edge_selected_field_ids: AtomicUsize,
    node_dense_vector_projection_reads: AtomicUsize,
    node_sparse_vector_projection_reads: AtomicUsize,
}

#[cfg(test)]
impl SelectedFieldReadCounters {
    pub(crate) fn note_node_selected_field_batch(&self, ids: usize) {
        self.node_selected_field_batches
            .fetch_add(1, Ordering::Relaxed);
        self.node_selected_field_ids
            .fetch_add(ids, Ordering::Relaxed);
    }

    pub(crate) fn note_edge_selected_field_batch(&self, ids: usize) {
        self.edge_selected_field_batches
            .fetch_add(1, Ordering::Relaxed);
        self.edge_selected_field_ids
            .fetch_add(ids, Ordering::Relaxed);
    }

    pub(crate) fn note_node_dense_vector_projection_read(&self) {
        self.node_dense_vector_projection_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_node_sparse_vector_projection_read(&self) {
        self.node_sparse_vector_projection_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn node_selected_field_batches(&self) -> usize {
        self.node_selected_field_batches.load(Ordering::Relaxed)
    }

    pub(crate) fn node_selected_field_ids(&self) -> usize {
        self.node_selected_field_ids.load(Ordering::Relaxed)
    }

    pub(crate) fn edge_selected_field_batches(&self) -> usize {
        self.edge_selected_field_batches.load(Ordering::Relaxed)
    }

    pub(crate) fn edge_selected_field_ids(&self) -> usize {
        self.edge_selected_field_ids.load(Ordering::Relaxed)
    }

    pub(crate) fn node_dense_vector_projection_reads(&self) -> usize {
        self.node_dense_vector_projection_reads
            .load(Ordering::Relaxed)
    }

    pub(crate) fn node_sparse_vector_projection_reads(&self) -> usize {
        self.node_sparse_vector_projection_reads
            .load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.node_selected_field_batches.store(0, Ordering::Relaxed);
        self.node_selected_field_ids.store(0, Ordering::Relaxed);
        self.edge_selected_field_batches.store(0, Ordering::Relaxed);
        self.edge_selected_field_ids.store(0, Ordering::Relaxed);
        self.node_dense_vector_projection_reads
            .store(0, Ordering::Relaxed);
        self.node_sparse_vector_projection_reads
            .store(0, Ordering::Relaxed);
    }
}

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
pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
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

/// Configuration for the single DB-scoped dense vector space.
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
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

/// Request parameters for dense, sparse, or hybrid vector search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorSearchRequest {
    pub mode: VectorSearchMode,
    pub dense_query: Option<DenseVector>,
    pub sparse_query: Option<SparseVector>,
    pub k: usize,
    pub label_filter: Option<NodeLabelFilter>,
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

/// Internal numeric node record used by storage, WAL, indexes, and planners.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeRecord {
    pub id: u64,
    pub label_ids: NodeLabelSet,
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

/// Internal numeric edge record used by storage, WAL, indexes, and planners.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeRecord {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label_id: u32,
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

/// Public, fully hydrated node record returned by core point-read APIs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeView {
    pub id: u64,
    pub labels: Vec<String>,
    pub key: String,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: i64,
    pub updated_at: i64,
    pub weight: f32,
    pub dense_vector: Option<DenseVector>,
    pub sparse_vector: Option<SparseVector>,
}

/// Public, fully hydrated edge record returned by core point-read APIs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeView {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label: String,
    pub props: BTreeMap<String, PropValue>,
    pub created_at: i64,
    pub updated_at: i64,
    pub weight: f32,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// Public key lookup request for `get_nodes_by_keys`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeKeyQuery {
    pub label: String,
    pub key: String,
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
    /// Optional node-label membership filter.
    pub label_filter: Option<NodeLabelFilter>,
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
            label_filter: None,
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
    IdRange {
        lower: Option<u64>,
        upper: Option<u64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    KeyEquals(String),
    KeyIn(Vec<String>),
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
    WeightRange {
        lower: Option<f32>,
        upper: Option<f32>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    CreatedAtRange {
        lower: Option<i64>,
        upper: Option<i64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
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
    pub items: Vec<NodeView>,
    pub next_cursor: Option<u64>,
}

/// Request for planner-backed edge queries.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeQuery {
    pub label: Option<String>,
    pub ids: Vec<u64>,
    pub from_ids: Vec<u64>,
    pub to_ids: Vec<u64>,
    pub endpoint_ids: Vec<u64>,
    pub filter: Option<EdgeFilterExpr>,
    pub page: PageRequest,
    pub order: EdgeQueryOrder,
    pub allow_full_scan: bool,
}

impl Default for EdgeQuery {
    fn default() -> Self {
        Self {
            label: None,
            ids: Vec::new(),
            from_ids: Vec::new(),
            to_ids: Vec::new(),
            endpoint_ids: Vec::new(),
            filter: None,
            page: PageRequest::default(),
            order: EdgeQueryOrder::EdgeIdAsc,
            allow_full_scan: false,
        }
    }
}

/// Recursive boolean filter supported by planner-backed edge queries.
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeFilterExpr {
    IdRange {
        lower: Option<u64>,
        upper: Option<u64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
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
    WeightRange {
        lower: Option<f32>,
        upper: Option<f32>,
    },
    UpdatedAtRange {
        lower_ms: Option<i64>,
        upper_ms: Option<i64>,
    },
    CreatedAtRange {
        lower: Option<i64>,
        upper: Option<i64>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    ValidAt {
        epoch_ms: i64,
    },
    ValidFromRange {
        lower_ms: Option<i64>,
        upper_ms: Option<i64>,
    },
    ValidToRange {
        lower_ms: Option<i64>,
        upper_ms: Option<i64>,
    },
    And(Vec<EdgeFilterExpr>),
    Or(Vec<EdgeFilterExpr>),
    Not(Box<EdgeFilterExpr>),
}

/// Result ordering for planner-backed edge queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeQueryOrder {
    EdgeIdAsc,
}

/// ID-only result for planner-backed edge queries.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryEdgeIdsResult {
    pub edge_ids: Vec<u64>,
    pub next_cursor: Option<u64>,
}

/// Hydrated result for planner-backed edge queries.
#[derive(Debug, Clone)]
pub struct QueryEdgesResult {
    pub edges: Vec<EdgeView>,
    pub next_cursor: Option<u64>,
}

/// Public structured graph-row query request.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphRowQuery {
    pub nodes: Vec<GraphNodePattern>,
    pub pieces: Vec<GraphPatternPiece>,
    pub where_: Option<GraphExpr>,
    pub return_items: Option<Vec<GraphReturnItem>>,
    pub order_by: Vec<GraphOrderItem>,
    pub page: GraphPageRequest,
    pub at_epoch: Option<i64>,
    pub params: BTreeMap<String, GraphParamValue>,
    pub output: GraphOutputOptions,
    pub options: GraphQueryOptions,
}

/// Public structured graph pipeline query request.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineQuery {
    pub stages: Vec<GraphPipelineStage>,
    pub params: BTreeMap<String, GraphParamValue>,
    pub at_epoch: Option<i64>,
    pub page: GraphPageRequest,
    pub output: GraphOutputOptions,
    pub options: GraphPipelineOptions,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GraphPipelineStage {
    Match(GraphPipelineMatchStage),
    Project(GraphProjectStage),
    ShortestPath(GraphShortestPathStage),
    Call(GraphSubqueryStage),
    Union(GraphUnionStage),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineMatchStage {
    pub optional: bool,
    pub nodes: Vec<GraphNodePattern>,
    pub pieces: Vec<GraphPatternPiece>,
    pub where_: Option<GraphExpr>,
    /// Optional-match candidate predicate evaluated before left-outer null extension.
    pub optional_candidate_where: Option<GraphExpr>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphProjectStage {
    pub kind: GraphProjectKind,
    pub items: GraphProjectionItems,
    pub distinct: bool,
    pub where_: Option<GraphExpr>,
    pub order_by: Vec<GraphOrderItem>,
    pub skip: Option<GraphExpr>,
    pub limit: Option<GraphExpr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphProjectKind {
    With,
    Return,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GraphProjectionItems {
    Star,
    Items(Vec<GraphProjectItem>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphProjectItem {
    pub expr: GraphExpr,
    pub alias: Option<String>,
    pub projection: GraphReturnProjection,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphUnionStage {
    pub branches: Vec<GraphPipelineQuery>,
    pub all: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphSubqueryStage {
    pub query: Box<GraphPipelineQuery>,
    pub import_aliases: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphShortestPathStage {
    pub optional: bool,
    pub output_path_alias: String,
    pub mode: GraphShortestPathMode,
    pub from: GraphShortestPathEndpoint,
    pub to: GraphShortestPathEndpoint,
    pub direction: Direction,
    pub edge_label_filter: Vec<String>,
    pub min_hops: u8,
    pub max_hops: u8,
    pub weight_field: Option<String>,
    pub max_cost: Option<f64>,
    pub max_paths: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphShortestPathMode {
    One,
    All,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GraphShortestPathEndpoint {
    Alias(String),
    NodeId(u64),
    NodeKey { label: String, key: String },
    Expr(GraphExpr),
}

/// Graph pipeline validation, safety, and explain options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphPipelineOptions {
    pub allow_full_scan: bool,
    pub max_rows: usize,
    pub max_pipeline_rows: usize,
    pub max_groups: usize,
    pub max_collect_items: usize,
    pub max_union_branches: usize,
    pub max_subquery_invocations: usize,
    pub max_subquery_depth: usize,
    pub max_shortest_path_pairs: usize,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_order_materialization: usize,
    pub max_skip: usize,
    pub max_cursor_bytes: usize,
    pub max_query_bytes: usize,
    pub max_param_bytes: usize,
    pub max_ast_depth: usize,
    pub max_literal_items: usize,
    pub include_plan: bool,
    pub profile: bool,
}

impl Default for GraphPipelineOptions {
    fn default() -> Self {
        Self {
            allow_full_scan: false,
            max_rows: 10_000,
            max_pipeline_rows: 65_536,
            max_groups: 65_536,
            max_collect_items: 65_536,
            max_union_branches: 16,
            max_subquery_invocations: 4_096,
            max_subquery_depth: 2,
            max_shortest_path_pairs: 4_096,
            max_intermediate_bindings: 65_536,
            max_frontier: 65_536,
            max_path_hops: 16,
            max_paths_per_start: 4_096,
            max_order_materialization: 65_536,
            max_skip: 100_000,
            max_cursor_bytes: 16 * 1024,
            max_query_bytes: 1_048_576,
            max_param_bytes: 1_048_576,
            max_ast_depth: 256,
            max_literal_items: 10_000,
            include_plan: false,
            profile: false,
        }
    }
}

/// Parameter value accepted by graph-row requests.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphParamValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GraphParamValue>),
    Map(BTreeMap<String, GraphParamValue>),
}

/// Node variable declaration for graph-row queries.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNodePattern {
    pub alias: String,
    pub label_filter: Option<NodeLabelFilter>,
    pub ids: Vec<u64>,
    pub keys: Vec<NodeKeyQuery>,
    pub filter: Option<NodeFilterExpr>,
}

/// Pattern piece inside a graph-row query.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphPatternPiece {
    Edge(GraphEdgePattern),
    Optional(GraphOptionalGroup),
    VariableLength(GraphVariableLengthPattern),
}

/// Fixed edge pattern piece for graph-row queries.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgePattern {
    pub alias: Option<String>,
    pub from_alias: String,
    pub to_alias: String,
    pub direction: Direction,
    pub label_filter: Vec<String>,
    pub filter: Option<EdgeFilterExpr>,
}

/// Optional graph-row pattern group.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphOptionalGroup {
    pub pieces: Vec<GraphPatternPiece>,
    pub where_: Option<GraphExpr>,
}

/// Bounded variable-length path pattern piece.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphVariableLengthPattern {
    pub path_alias: Option<String>,
    pub edge_alias: Option<String>,
    pub from_alias: String,
    pub to_alias: String,
    pub direction: Direction,
    pub label_filter: Vec<String>,
    pub filter: Option<EdgeFilterExpr>,
    pub min_hops: u8,
    pub max_hops: u8,
}

/// Row-level graph expression shared by native graph-row APIs and text lowering.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphExpr {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GraphExpr>),
    Map(BTreeMap<String, GraphExpr>),
    Param(String),
    Binding(String),
    Property {
        alias: String,
        key: String,
    },
    NodeField {
        alias: String,
        field: GraphNodeField,
    },
    EdgeField {
        alias: String,
        field: GraphEdgeField,
    },
    PathField {
        alias: String,
        field: GraphPathField,
    },
    Function {
        name: GraphFunction,
        args: Vec<GraphExpr>,
    },
    AggregateCall {
        function: GraphAggregateFunction,
        distinct: bool,
        arg: Option<Box<GraphExpr>>,
    },
    ExistsSubquery(GraphSubqueryStage),
    Unary {
        op: GraphUnaryOp,
        expr: Box<GraphExpr>,
    },
    Binary {
        left: Box<GraphExpr>,
        op: GraphBinaryOp,
        right: Box<GraphExpr>,
    },
    Case {
        operand: Option<Box<GraphExpr>>,
        branches: Vec<GraphCaseBranch>,
        else_expr: Option<Box<GraphExpr>>,
    },
    IsNull(Box<GraphExpr>),
    IsNotNull(Box<GraphExpr>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphCaseBranch {
    pub when: GraphExpr,
    pub then: GraphExpr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphNodeField {
    Id,
    Labels,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphEdgeField {
    Id,
    From,
    To,
    Label,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphPathField {
    NodeIds,
    EdgeIds,
    Length,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphFunction {
    Id,
    Labels,
    Type,
    Length,
    StartNode,
    EndNode,
    Nodes,
    Relationships,
    Coalesce,
    ToString,
    ToInteger,
    ToFloat,
    Abs,
    Floor,
    Ceil,
    Round,
    Lower,
    Upper,
    Trim,
    Substring,
    Size,
    Head,
    Last,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphAggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphUnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphBinaryOp {
    Or,
    And,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Add,
    Sub,
    Mul,
    Div,
    StartsWith,
    EndsWith,
    Contains,
}

/// One output column requested by a graph-row query.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphReturnItem {
    pub expr: GraphExpr,
    pub alias: Option<String>,
    pub projection: GraphReturnProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphReturnProjection {
    Auto,
    IdOnly,
    Element(GraphElementProjection),
    Selected(GraphSelectedProjection),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphElementProjection {
    IdOnly,
    Compact,
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphSelectedProjection {
    Node(GraphSelectedNodeProjection),
    Edge(GraphSelectedEdgeProjection),
    Path(GraphSelectedPathProjection),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphSelectedNodeProjection {
    pub id: bool,
    pub labels: bool,
    pub key: bool,
    pub props: GraphPropertySelection,
    pub weight: bool,
    pub created_at: bool,
    pub updated_at: bool,
    pub vectors: GraphVectorSelection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphSelectedEdgeProjection {
    pub id: bool,
    pub from: bool,
    pub to: bool,
    pub label: bool,
    pub props: GraphPropertySelection,
    pub weight: bool,
    pub created_at: bool,
    pub updated_at: bool,
    pub valid_from: bool,
    pub valid_to: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphSelectedPathProjection {
    pub node_ids: bool,
    pub edge_ids: bool,
    pub nodes: Option<GraphSelectedNodeProjection>,
    pub edges: Option<GraphSelectedEdgeProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphPropertySelection {
    None,
    Keys(Vec<String>),
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphVectorSelection {
    None,
    Dense,
    Sparse,
    Both,
}

/// Output defaults for graph-row values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphOutputOptions {
    pub mode: GraphOutputMode,
    pub compact_rows: bool,
    pub include_vectors: bool,
}

impl Default for GraphOutputOptions {
    fn default() -> Self {
        Self {
            mode: GraphOutputMode::Ids,
            compact_rows: false,
            include_vectors: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphOutputMode {
    Ids,
    Elements,
    Projected,
}

/// Runtime graph-row value.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GraphValue>),
    Map(BTreeMap<String, GraphValue>),
    NodeId(u64),
    EdgeId(u64),
    Node(GraphNodeValue),
    Edge(GraphEdgeValue),
    Path(GraphPathValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphNodeValue {
    pub id: Option<u64>,
    pub labels: Option<Vec<String>>,
    pub key: Option<String>,
    pub props: Option<BTreeMap<String, GraphValue>>,
    pub weight: Option<f32>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub dense_vector: Option<Vec<f32>>,
    pub sparse_vector: Option<Vec<(u32, f32)>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgeValue {
    pub id: Option<u64>,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub label: Option<String>,
    pub props: Option<BTreeMap<String, GraphValue>>,
    pub weight: Option<f32>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

/// Compact path identity used by graph-row execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphPath {
    pub nodes: Vec<u64>,
    pub edges: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphPathValue {
    pub node_ids: Vec<u64>,
    pub edge_ids: Vec<u64>,
    pub nodes: Option<Vec<GraphNodeValue>>,
    pub edges: Option<Vec<GraphEdgeValue>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphOrderItem {
    pub expr: GraphExpr,
    pub direction: GraphOrderDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphOrderDirection {
    Asc,
    Desc,
}

/// Final-row page request for native graph-row APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphPageRequest {
    pub skip: usize,
    pub limit: usize,
    pub cursor: Option<String>,
}

/// Graph-row validation, safety, and explain options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphQueryOptions {
    pub allow_full_scan: bool,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_page_limit: usize,
    pub max_order_materialization: usize,
    pub max_cursor_bytes: usize,
    pub max_query_bytes: usize,
    pub include_plan: bool,
    pub profile: bool,
}

impl Default for GraphQueryOptions {
    fn default() -> Self {
        Self {
            allow_full_scan: false,
            max_intermediate_bindings: 65_536,
            max_frontier: 65_536,
            max_path_hops: 16,
            max_paths_per_start: 4_096,
            max_page_limit: 10_000,
            max_order_materialization: 65_536,
            max_cursor_bytes: 16 * 1024,
            max_query_bytes: 1_048_576,
            include_plan: false,
            profile: false,
        }
    }
}

/// Result of a graph-row query.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphRowResult {
    pub columns: Vec<String>,
    pub rows: Vec<GraphRow>,
    pub next_cursor: Option<String>,
    pub stats: GraphRowStats,
    pub plan: Option<GraphRowExplain>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphRow {
    pub values: Vec<GraphValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphRowStats {
    pub rows_returned: usize,
    pub rows_after_filter: usize,
    pub rows_seen_for_page: usize,
    pub intermediate_bindings_peak: usize,
    pub frontier_peak: usize,
    pub paths_enumerated: usize,
    pub db_hits: usize,
    pub elapsed_us: Option<u64>,
    pub effective_at_epoch: i64,
    pub warnings: Vec<String>,
}

/// Result of a graph pipeline query.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineResult {
    pub columns: Vec<String>,
    pub rows: Vec<GraphRow>,
    pub next_cursor: Option<String>,
    pub stats: GraphPipelineStats,
    pub plan: Option<GraphPipelineExplain>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineStats {
    pub rows_returned: usize,
    pub rows_entered_pipeline: usize,
    pub rows_after_filter: usize,
    pub intermediate_rows: usize,
    pub pipeline_rows_materialized: usize,
    pub groups: usize,
    pub collect_items: usize,
    pub union_branches: usize,
    pub union_dedup_keys: usize,
    pub subquery_invocations: usize,
    pub subquery_cache_hits: usize,
    pub shortest_path_pairs: usize,
    pub shortest_path_cache_hits: usize,
    pub db_hits: usize,
    pub elapsed_us: Option<u64>,
    pub effective_at_epoch: i64,
    pub warnings: Vec<String>,
}

/// Explain output for graph-row planning and execution.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphRowExplain {
    pub columns: Vec<String>,
    pub effective_at_epoch: Option<i64>,
    pub fingerprint: String,
    pub plan: Vec<GraphExplainNode>,
    pub row_ops: Vec<GraphRowOperationExplain>,
    pub order: GraphOrderExplain,
    pub cursor: GraphCursorExplain,
    pub projection: GraphProjectionExplain,
    pub caps: GraphCapExplain,
    pub summaries: GraphExecutionSummaries,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

/// Explain output for graph pipeline planning and execution.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineExplain {
    pub columns: Vec<String>,
    pub effective_at_epoch: Option<i64>,
    pub fingerprint: String,
    pub stages: Vec<GraphPipelineStageExplain>,
    pub row_ops: Vec<GraphRowOperationExplain>,
    pub order: GraphOrderExplain,
    pub cursor: GraphCursorExplain,
    pub projection: GraphProjectionExplain,
    pub caps: GraphPipelineCapExplain,
    pub summaries: GraphExecutionSummaries,
    pub stats: GraphPipelineStats,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GraphPipelineStageExplain {
    pub index: usize,
    pub kind: String,
    pub detail: String,
    pub columns: Vec<String>,
    pub graph_row: Option<Box<GraphRowExplain>>,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphPipelineCapExplain {
    pub allow_full_scan: bool,
    pub max_rows: usize,
    pub max_pipeline_rows: usize,
    pub max_groups: usize,
    pub max_collect_items: usize,
    pub max_union_branches: usize,
    pub max_subquery_invocations: usize,
    pub max_subquery_depth: usize,
    pub max_shortest_path_pairs: usize,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_order_materialization: usize,
    pub max_skip: usize,
    pub max_cursor_bytes: usize,
    pub max_query_bytes: usize,
    pub max_param_bytes: usize,
    pub max_ast_depth: usize,
    pub max_literal_items: usize,
}

/// Minimal graph-row explain plan node. Future work can fill in richer structured
/// details without changing the root explain contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphExplainNode {
    pub kind: String,
    pub detail: String,
    pub children: Vec<GraphExplainNode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphRowOperationExplain {
    pub kind: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphOrderExplain {
    pub explicit: bool,
    pub items: usize,
    pub stable_logical_row_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphCursorExplain {
    pub supplied: bool,
    pub codec_implemented: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphProjectionExplain {
    pub columns: Vec<String>,
    pub output_mode: GraphOutputMode,
    pub include_vectors: bool,
    pub compact_rows: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphCapExplain {
    pub allow_full_scan: bool,
    pub max_intermediate_bindings: usize,
    pub max_frontier: usize,
    pub max_path_hops: u8,
    pub max_paths_per_start: usize,
    pub max_page_limit: usize,
    pub max_order_materialization: usize,
    pub max_cursor_bytes: usize,
    pub max_query_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphExecutionSummaries {
    pub validation_only: bool,
    pub rows_planned: usize,
    pub warnings: Vec<String>,
}

/// Kind of planner-backed query represented by a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlanKind {
    NodeQuery,
    EdgeQuery,
}

/// Explain output for planner-backed queries.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub kind: QueryPlanKind,
    pub root: QueryPlanNode,
    pub estimated_candidates: Option<u64>,
    pub warnings: Vec<QueryPlanWarning>,
    pub notes: Vec<QueryPlanNote>,
    pub public_inputs: QueryPlanPublicInputs,
}

/// Public names referenced by planner explain input normalization.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryPlanPublicInputs {
    pub node_labels: Vec<QueryPlanPublicName>,
    pub edge_labels: Vec<QueryPlanPublicName>,
}

/// One public node-label or edge-label name surfaced in explain output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlanPublicName {
    pub alias: Option<String>,
    pub name: String,
    pub known: bool,
    pub mode: Option<LabelMatchMode>,
}

/// Non-warning explain notes for planner behavior that is expected and
/// correctness-relevant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPlanNote {
    NodeLabelAnyDedupeBeforePagination,
    NodeLabelAnyFinalVerification,
    NodeLabelAllSupersetVerification,
    StaleNodeLabelMembershipVerification,
}

/// Explain tree node for planner-backed queries.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlanNode {
    ExplicitIds,
    KeyLookup,
    NodeLabelIndex,
    NodeLabelAnyIndex,
    CompoundEqualityIndex { details: CompoundIndexPlanDetails },
    CompoundRangeIndex { details: CompoundIndexPlanDetails },
    PropertyEqualityIndex,
    PropertyRangeIndex,
    TimestampIndex,
    AdjacencyExpansion,
    ExplicitEdgeIds,
    EdgeLabelIndex,
    EdgeTripleIndex,
    EdgeEndpointAdjacency,
    EdgeWeightIndex,
    EdgeUpdatedAtIndex,
    EdgeValidityIndex,
    EdgeMetadataScan,
    EdgePropertyEqualityIndex,
    EdgePropertyRangeIndex,
    Intersect { inputs: Vec<QueryPlanNode> },
    Union { inputs: Vec<QueryPlanNode> },
    VerifyNodeFilter { input: Box<QueryPlanNode> },
    VerifyEdgeFilter { input: Box<QueryPlanNode> },
    VerifyEdgePredicates { input: Box<QueryPlanNode> },
    FallbackNodeLabelScan,
    FallbackFullNodeScan,
    FallbackEdgeLabelScan,
    FallbackFullEdgeScan,
    EmptyResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlanCompoundTargetKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompoundIndexPlanDetails {
    pub index_id: u64,
    pub target_kind: QueryPlanCompoundTargetKind,
    pub label: Option<String>,
    pub kind: SecondaryIndexKind,
    pub fields: Vec<SecondaryIndexField>,
    pub compound: bool,
    pub matched_prefix_len: usize,
    pub range_field: Option<SecondaryIndexField>,
    pub in_expansions: usize,
    pub estimated_candidates: Option<u64>,
    pub coverage: String,
    pub residual_predicates: usize,
    pub final_verification: bool,
    pub fallback_reason: Option<String>,
}

/// Warning emitted by planner explain output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPlanWarning {
    MissingReadyIndex,
    UsingFallbackScan,
    FullScanRequiresOptIn,
    FullScanExplicitlyAllowed,
    EdgePropertyPostFilter,
    IndexSkippedAsBroad,
    CandidateCapExceeded,
    RangeCandidateCapExceeded,
    TimestampCandidateCapExceeded,
    VerifyOnlyFilter,
    BooleanBranchFallback,
    PlanningProbeBudgetExceeded,
    CompoundIndexPrefixNotSatisfied,
    UnknownNodeLabel,
    UnknownEdgeLabel,
}

pub(crate) fn gql_query_plan_warning_message(warning: QueryPlanWarning) -> &'static str {
    match warning {
        QueryPlanWarning::CompoundIndexPrefixNotSatisfied => {
            "compound secondary index skipped because query predicates do not constrain a left prefix of the declaration"
        }
        QueryPlanWarning::MissingReadyIndex => "MissingReadyIndex",
        QueryPlanWarning::UsingFallbackScan => "UsingFallbackScan",
        QueryPlanWarning::FullScanRequiresOptIn => "FullScanRequiresOptIn",
        QueryPlanWarning::FullScanExplicitlyAllowed => "FullScanExplicitlyAllowed",
        QueryPlanWarning::EdgePropertyPostFilter => "EdgePropertyPostFilter",
        QueryPlanWarning::IndexSkippedAsBroad => "IndexSkippedAsBroad",
        QueryPlanWarning::CandidateCapExceeded => "CandidateCapExceeded",
        QueryPlanWarning::RangeCandidateCapExceeded => "RangeCandidateCapExceeded",
        QueryPlanWarning::TimestampCandidateCapExceeded => "TimestampCandidateCapExceeded",
        QueryPlanWarning::VerifyOnlyFilter => "VerifyOnlyFilter",
        QueryPlanWarning::BooleanBranchFallback => "BooleanBranchFallback",
        QueryPlanWarning::PlanningProbeBudgetExceeded => "PlanningProbeBudgetExceeded",
        QueryPlanWarning::UnknownNodeLabel => "UnknownNodeLabel",
        QueryPlanWarning::UnknownEdgeLabel => "UnknownEdgeLabel",
    }
}

/// Kind of optional secondary index declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexKind {
    Equality,
    Range,
}

pub const MAX_SECONDARY_INDEX_FIELDS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NodeMetadataIndexField {
    Id,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeMetadataIndexField {
    Id,
    From,
    To,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SecondaryIndexField {
    Property { key: String },
    NodeMetadata(NodeMetadataIndexField),
    EdgeMetadata(EdgeMetadataIndexField),
}

impl SecondaryIndexField {
    pub fn property(key: impl Into<String>) -> Self {
        Self::Property { key: key.into() }
    }

    pub fn node_meta(field: NodeMetadataIndexField) -> Self {
        Self::NodeMetadata(field)
    }

    pub fn edge_meta(field: EdgeMetadataIndexField) -> Self {
        Self::EdgeMetadata(field)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecondaryIndexSpec {
    pub fields: Vec<SecondaryIndexField>,
    pub kind: SecondaryIndexKind,
}

impl SecondaryIndexSpec {
    pub fn equality(fields: impl Into<Vec<SecondaryIndexField>>) -> Self {
        Self {
            fields: fields.into(),
            kind: SecondaryIndexKind::Equality,
        }
    }

    pub fn range(fields: impl Into<Vec<SecondaryIndexField>>) -> Self {
        Self {
            fields: fields.into(),
            kind: SecondaryIndexKind::Range,
        }
    }

    pub(crate) fn validate_for_node(&self) -> Result<(), EngineError> {
        validate_secondary_index_fields(&self.fields, SecondaryIndexTargetKind::Node)
    }

    pub(crate) fn validate_for_edge(&self) -> Result<(), EngineError> {
        validate_secondary_index_fields(&self.fields, SecondaryIndexTargetKind::Edge)
    }

    pub(crate) fn node_target(&self, label_id: u32) -> Result<SecondaryIndexTarget, EngineError> {
        self.validate_for_node()?;
        Ok(match self.fields.as_slice() {
            [SecondaryIndexField::Property { key }] => SecondaryIndexTarget::NodeProperty {
                label_id,
                prop_key: key.clone(),
            },
            fields => SecondaryIndexTarget::NodeFieldIndex {
                label_id,
                fields: fields
                    .iter()
                    .map(SecondaryIndexFieldManifest::from_public)
                    .collect::<Result<Vec<_>, _>>()?,
            },
        })
    }

    pub(crate) fn edge_target(&self, label_id: u32) -> Result<SecondaryIndexTarget, EngineError> {
        self.validate_for_edge()?;
        Ok(match self.fields.as_slice() {
            [SecondaryIndexField::Property { key }] => SecondaryIndexTarget::EdgeProperty {
                label_id,
                prop_key: key.clone(),
            },
            fields => SecondaryIndexTarget::EdgeFieldIndex {
                label_id,
                fields: fields
                    .iter()
                    .map(SecondaryIndexFieldManifest::from_public)
                    .collect::<Result<Vec<_>, _>>()?,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NodeMetadataIndexFieldManifest {
    Id,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeMetadataIndexFieldManifest {
    Id,
    From,
    To,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SecondaryIndexFieldManifest {
    Property {
        key: String,
    },
    NodeMetadata {
        field: NodeMetadataIndexFieldManifest,
    },
    EdgeMetadata {
        field: EdgeMetadataIndexFieldManifest,
    },
}

impl From<NodeMetadataIndexField> for NodeMetadataIndexFieldManifest {
    fn from(field: NodeMetadataIndexField) -> Self {
        match field {
            NodeMetadataIndexField::Id => Self::Id,
            NodeMetadataIndexField::Key => Self::Key,
            NodeMetadataIndexField::Weight => Self::Weight,
            NodeMetadataIndexField::CreatedAt => Self::CreatedAt,
            NodeMetadataIndexField::UpdatedAt => Self::UpdatedAt,
        }
    }
}

impl From<NodeMetadataIndexFieldManifest> for NodeMetadataIndexField {
    fn from(field: NodeMetadataIndexFieldManifest) -> Self {
        match field {
            NodeMetadataIndexFieldManifest::Id => Self::Id,
            NodeMetadataIndexFieldManifest::Key => Self::Key,
            NodeMetadataIndexFieldManifest::Weight => Self::Weight,
            NodeMetadataIndexFieldManifest::CreatedAt => Self::CreatedAt,
            NodeMetadataIndexFieldManifest::UpdatedAt => Self::UpdatedAt,
        }
    }
}

impl From<EdgeMetadataIndexField> for EdgeMetadataIndexFieldManifest {
    fn from(field: EdgeMetadataIndexField) -> Self {
        match field {
            EdgeMetadataIndexField::Id => Self::Id,
            EdgeMetadataIndexField::From => Self::From,
            EdgeMetadataIndexField::To => Self::To,
            EdgeMetadataIndexField::Weight => Self::Weight,
            EdgeMetadataIndexField::CreatedAt => Self::CreatedAt,
            EdgeMetadataIndexField::UpdatedAt => Self::UpdatedAt,
            EdgeMetadataIndexField::ValidFrom => Self::ValidFrom,
            EdgeMetadataIndexField::ValidTo => Self::ValidTo,
        }
    }
}

impl From<EdgeMetadataIndexFieldManifest> for EdgeMetadataIndexField {
    fn from(field: EdgeMetadataIndexFieldManifest) -> Self {
        match field {
            EdgeMetadataIndexFieldManifest::Id => Self::Id,
            EdgeMetadataIndexFieldManifest::From => Self::From,
            EdgeMetadataIndexFieldManifest::To => Self::To,
            EdgeMetadataIndexFieldManifest::Weight => Self::Weight,
            EdgeMetadataIndexFieldManifest::CreatedAt => Self::CreatedAt,
            EdgeMetadataIndexFieldManifest::UpdatedAt => Self::UpdatedAt,
            EdgeMetadataIndexFieldManifest::ValidFrom => Self::ValidFrom,
            EdgeMetadataIndexFieldManifest::ValidTo => Self::ValidTo,
        }
    }
}

impl SecondaryIndexFieldManifest {
    pub(crate) fn from_public(field: &SecondaryIndexField) -> Result<Self, EngineError> {
        Ok(match field {
            SecondaryIndexField::Property { key } => Self::Property { key: key.clone() },
            SecondaryIndexField::NodeMetadata(field) => Self::NodeMetadata {
                field: (*field).into(),
            },
            SecondaryIndexField::EdgeMetadata(field) => Self::EdgeMetadata {
                field: (*field).into(),
            },
        })
    }

    pub(crate) fn to_public(&self) -> SecondaryIndexField {
        match self {
            Self::Property { key } => SecondaryIndexField::Property { key: key.clone() },
            Self::NodeMetadata { field } => SecondaryIndexField::NodeMetadata((*field).into()),
            Self::EdgeMetadata { field } => SecondaryIndexField::EdgeMetadata((*field).into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SecondaryIndexTargetKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryIndexLogicalIdentity {
    pub target_kind: SecondaryIndexTargetKind,
    pub label_id: u32,
    pub fields: Vec<SecondaryIndexField>,
    pub kind: SecondaryIndexKind,
}

/// Diagnostic/internal target for an optional secondary index declaration.
///
/// This is exposed only because raw manifest inspection is a diagnostic surface.
/// Ordinary property-index APIs use `NodePropertyIndexInfo` and
/// `EdgePropertyIndexInfo`, which expose labels and edge labels instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexTarget {
    NodeProperty {
        label_id: u32,
        prop_key: String,
    },
    EdgeProperty {
        label_id: u32,
        prop_key: String,
    },
    NodeFieldIndex {
        label_id: u32,
        fields: Vec<SecondaryIndexFieldManifest>,
    },
    EdgeFieldIndex {
        label_id: u32,
        fields: Vec<SecondaryIndexFieldManifest>,
    },
}

impl SecondaryIndexTarget {
    pub(crate) fn target_kind(&self) -> SecondaryIndexTargetKind {
        match self {
            Self::NodeProperty { .. } | Self::NodeFieldIndex { .. } => {
                SecondaryIndexTargetKind::Node
            }
            Self::EdgeProperty { .. } | Self::EdgeFieldIndex { .. } => {
                SecondaryIndexTargetKind::Edge
            }
        }
    }

    pub(crate) fn label_id(&self) -> u32 {
        match self {
            Self::NodeProperty { label_id, .. }
            | Self::EdgeProperty { label_id, .. }
            | Self::NodeFieldIndex { label_id, .. }
            | Self::EdgeFieldIndex { label_id, .. } => *label_id,
        }
    }

    pub(crate) fn single_property_key(&self) -> Option<&str> {
        match self {
            Self::NodeProperty { prop_key, .. } | Self::EdgeProperty { prop_key, .. } => {
                Some(prop_key)
            }
            _ => None,
        }
    }

    pub(crate) fn public_fields(&self) -> Vec<SecondaryIndexField> {
        match self {
            Self::NodeProperty { prop_key, .. } | Self::EdgeProperty { prop_key, .. } => {
                vec![SecondaryIndexField::property(prop_key.clone())]
            }
            Self::NodeFieldIndex { fields, .. } | Self::EdgeFieldIndex { fields, .. } => fields
                .iter()
                .map(SecondaryIndexFieldManifest::to_public)
                .collect(),
        }
    }

    pub(crate) fn is_compound(&self) -> bool {
        self.public_fields().len() >= 2
    }
}

pub(crate) fn secondary_index_logical_identity(
    entry: &SecondaryIndexManifestEntry,
) -> Result<SecondaryIndexLogicalIdentity, EngineError> {
    validate_secondary_index_target(&entry.target)?;
    Ok(SecondaryIndexLogicalIdentity {
        target_kind: entry.target.target_kind(),
        label_id: entry.target.label_id(),
        fields: entry.target.public_fields(),
        kind: entry.kind.clone(),
    })
}

pub(crate) fn validate_secondary_index_target(
    target: &SecondaryIndexTarget,
) -> Result<(), EngineError> {
    let target_kind = target.target_kind();
    validate_secondary_index_fields(&target.public_fields(), target_kind)
}

pub(crate) fn validate_secondary_index_fields(
    fields: &[SecondaryIndexField],
    target_kind: SecondaryIndexTargetKind,
) -> Result<(), EngineError> {
    match fields.len() {
        0 => {
            return Err(invalid_secondary_index(
                "field list must contain 1 to 8 fields",
            ));
        }
        1..=MAX_SECONDARY_INDEX_FIELDS => {}
        _ => {
            return Err(invalid_secondary_index(
                "compound secondary indexes support at most 8 fields",
            ));
        }
    }

    let mut seen = HashSet::with_capacity(fields.len());
    for field in fields {
        match (target_kind, field) {
            (_, SecondaryIndexField::Property { key }) => {
                if key.is_empty() {
                    return Err(invalid_secondary_index("property key must not be empty"));
                }
            }
            (SecondaryIndexTargetKind::Node, SecondaryIndexField::NodeMetadata(_)) => {}
            (SecondaryIndexTargetKind::Edge, SecondaryIndexField::EdgeMetadata(_)) => {}
            (SecondaryIndexTargetKind::Node, SecondaryIndexField::EdgeMetadata(_)) => {
                return Err(invalid_secondary_index(
                    "node indexes cannot include edge metadata fields",
                ));
            }
            (SecondaryIndexTargetKind::Edge, SecondaryIndexField::NodeMetadata(_)) => {
                return Err(invalid_secondary_index(
                    "edge indexes cannot include node metadata fields",
                ));
            }
        }
        if !seen.insert(field.clone()) {
            return Err(invalid_secondary_index(format!(
                "duplicate field {}",
                secondary_index_field_display(field)
            )));
        }
    }

    Ok(())
}

fn invalid_secondary_index(message: impl Into<String>) -> EngineError {
    EngineError::InvalidOperation(format!("invalid secondary index: {}", message.into()))
}

fn secondary_index_field_display(field: &SecondaryIndexField) -> String {
    match field {
        SecondaryIndexField::Property { key } => format!("property `{key}`"),
        SecondaryIndexField::NodeMetadata(field) => {
            format!("node metadata `{}`", node_metadata_index_field_name(*field))
        }
        SecondaryIndexField::EdgeMetadata(field) => {
            format!("edge metadata `{}`", edge_metadata_index_field_name(*field))
        }
    }
}

pub(crate) fn node_metadata_index_field_name(field: NodeMetadataIndexField) -> &'static str {
    match field {
        NodeMetadataIndexField::Id => "id",
        NodeMetadataIndexField::Key => "key",
        NodeMetadataIndexField::Weight => "weight",
        NodeMetadataIndexField::CreatedAt => "created_at",
        NodeMetadataIndexField::UpdatedAt => "updated_at",
    }
}

pub(crate) fn edge_metadata_index_field_name(field: EdgeMetadataIndexField) -> &'static str {
    match field {
        EdgeMetadataIndexField::Id => "id",
        EdgeMetadataIndexField::From => "from",
        EdgeMetadataIndexField::To => "to",
        EdgeMetadataIndexField::Weight => "weight",
        EdgeMetadataIndexField::CreatedAt => "created_at",
        EdgeMetadataIndexField::UpdatedAt => "updated_at",
        EdgeMetadataIndexField::ValidFrom => "valid_from",
        EdgeMetadataIndexField::ValidTo => "valid_to",
    }
}

/// Lifecycle state for an optional secondary index declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecondaryIndexState {
    Building,
    Ready,
    Failed,
}

/// Persisted manifest entry for an optional secondary index declaration.
///
/// This raw manifest shape is diagnostic introspection. Ordinary public APIs
/// return nameful index info DTOs instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexManifestEntry {
    pub index_id: u64,
    pub target: SecondaryIndexTarget,
    pub kind: SecondaryIndexKind,
    pub state: SecondaryIndexState,
    #[serde(default)]
    pub last_error: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaAdditionalPropertiesManifest {
    #[default]
    Allow,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaValueTypeManifest {
    Bool,
    Int,
    UInt,
    Float,
    Number,
    String,
    Bytes,
    Array,
    Map,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaVectorPresenceManifest {
    #[default]
    Optional,
    Required,
    Forbidden,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaNumericBoundManifest {
    pub value: PropValue,
    #[serde(default = "default_true")]
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertySchemaManifestRule {
    #[serde(default)]
    pub required: bool,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub types: Vec<SchemaValueTypeManifest>,
    #[serde(default)]
    pub numeric_min: Option<SchemaNumericBoundManifest>,
    #[serde(default)]
    pub numeric_max: Option<SchemaNumericBoundManifest>,
    #[serde(default)]
    pub string_min_bytes: Option<usize>,
    #[serde(default)]
    pub string_max_bytes: Option<usize>,
    #[serde(default)]
    pub bytes_min_len: Option<usize>,
    #[serde(default)]
    pub bytes_max_len: Option<usize>,
    #[serde(default)]
    pub array_min_items: Option<usize>,
    #[serde(default)]
    pub array_max_items: Option<usize>,
    #[serde(default)]
    pub map_min_entries: Option<usize>,
    #[serde(default)]
    pub map_max_entries: Option<usize>,
    #[serde(default)]
    pub enum_values: Vec<PropValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StringFieldSchemaManifestRule {
    #[serde(default)]
    pub min_bytes: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub enum_values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericFieldSchemaManifestRule {
    #[serde(default)]
    pub min: Option<SchemaNumericBoundManifest>,
    #[serde(default)]
    pub max: Option<SchemaNumericBoundManifest>,
    #[serde(default = "default_true")]
    pub finite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLabelConstraintManifestRule {
    #[serde(default)]
    pub all_of: Vec<u32>,
    #[serde(default)]
    pub any_of: Vec<u32>,
    #[serde(default)]
    pub none_of: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointLabelManifestRule {
    #[serde(default)]
    pub all_of: Vec<u32>,
    #[serde(default)]
    pub any_of: Vec<u32>,
    #[serde(default)]
    pub none_of: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenseVectorSchemaManifestRule {
    #[serde(default)]
    pub presence: SchemaVectorPresenceManifest,
    #[serde(default)]
    pub dimension: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparseVectorSchemaManifestRule {
    #[serde(default)]
    pub presence: SchemaVectorPresenceManifest,
    #[serde(default)]
    pub min_entries: Option<usize>,
    #[serde(default)]
    pub max_entries: Option<usize>,
    #[serde(default)]
    pub max_dimension_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeValiditySchemaManifestRule {
    #[serde(default)]
    pub require_valid_from_before_valid_to: bool,
    #[serde(default)]
    pub valid_from_min: Option<i64>,
    #[serde(default)]
    pub valid_from_max: Option<i64>,
    #[serde(default)]
    pub valid_to_min: Option<i64>,
    #[serde(default)]
    pub valid_to_max: Option<i64>,
    #[serde(default = "default_true")]
    pub allow_open_ended_valid_to: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSchemaManifestEntry {
    pub schema_id: u64,
    pub revision: u64,
    pub label_id: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub additional_properties: SchemaAdditionalPropertiesManifest,
    #[serde(default)]
    pub properties: BTreeMap<String, PropertySchemaManifestRule>,
    #[serde(default)]
    pub key: Option<StringFieldSchemaManifestRule>,
    #[serde(default)]
    pub label_constraints: Option<NodeLabelConstraintManifestRule>,
    #[serde(default)]
    pub weight: Option<NumericFieldSchemaManifestRule>,
    #[serde(default)]
    pub dense_vector: Option<DenseVectorSchemaManifestRule>,
    #[serde(default)]
    pub sparse_vector: Option<SparseVectorSchemaManifestRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSchemaManifestEntry {
    pub schema_id: u64,
    pub revision: u64,
    pub label_id: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub additional_properties: SchemaAdditionalPropertiesManifest,
    #[serde(default)]
    pub properties: BTreeMap<String, PropertySchemaManifestRule>,
    #[serde(default)]
    pub from: Option<EndpointLabelManifestRule>,
    #[serde(default)]
    pub to: Option<EndpointLabelManifestRule>,
    #[serde(default = "default_true")]
    pub allow_self_loops: bool,
    #[serde(default)]
    pub weight: Option<NumericFieldSchemaManifestRule>,
    #[serde(default)]
    pub validity: Option<EdgeValiditySchemaManifestRule>,
}

/// User-facing information about a node-property optional secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePropertyIndexInfo {
    pub index_id: u64,
    pub label: String,
    pub fields: Vec<SecondaryIndexField>,
    pub kind: SecondaryIndexKind,
    pub state: SecondaryIndexState,
    pub last_error: Option<String>,
    pub compound: bool,
}

/// User-facing diagnostic information about a node-label token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLabelInfo {
    pub label: String,
    pub label_id: u32,
}

/// User-facing diagnostic information about an edge-label token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeLabelInfo {
    pub label: String,
    pub label_id: u32,
}

/// User-facing information about an edge-property optional secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePropertyIndexInfo {
    pub index_id: u64,
    pub label: String,
    pub fields: Vec<SecondaryIndexField>,
    pub kind: SecondaryIndexKind,
    pub state: SecondaryIndexState,
    pub last_error: Option<String>,
    pub compound: bool,
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
    EnsureNodeLabel { label: String, label_id: u32 },
    EnsureEdgeLabel { label: String, label_id: u32 },
    BeginAtomicBatch { first_seq: u64, op_count: u32 },
    CommitAtomicBatch { first_seq: u64, op_count: u32 },
}

/// Operation type tags for binary encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OpTag {
    UpsertNode = 1,
    UpsertEdge = 2,
    DeleteNode = 3,
    DeleteEdge = 4,
    EnsureNodeLabel = 5,
    EnsureEdgeLabel = 6,
    BeginAtomicBatch = 7,
    CommitAtomicBatch = 8,
}

impl OpTag {
    pub(crate) fn from_u8(v: u8) -> Option<OpTag> {
        match v {
            1 => Some(OpTag::UpsertNode),
            2 => Some(OpTag::UpsertEdge),
            3 => Some(OpTag::DeleteNode),
            4 => Some(OpTag::DeleteEdge),
            5 => Some(OpTag::EnsureNodeLabel),
            6 => Some(OpTag::EnsureEdgeLabel),
            7 => Some(OpTag::BeginAtomicBatch),
            8 => Some(OpTag::CommitAtomicBatch),
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
    #[serde(default)]
    pub segment_format_version: u32,
    #[serde(default)]
    pub segment_data_id: [u8; 32],
}

/// Manifest state: the atomic checkpoint of the database.
///
/// This raw structure is exposed for explicit diagnostic introspection through
/// `DatabaseEngine::manifest()` and `manifest::load_manifest_readonly()`.
/// Ordinary graph APIs use named labels and edge labels and do not accept these
/// internal numeric token IDs as inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestState {
    pub version: u32,
    /// Named node-label / edge-label token schema marker.
    #[serde(default)]
    pub label_token_schema_version: u32,
    /// DB-scoped node-label catalog: public label -> internal label_id.
    #[serde(default)]
    pub node_label_tokens: BTreeMap<String, u32>,
    /// DB-scoped edge-label catalog: public edge label -> internal label_id.
    #[serde(default)]
    pub edge_label_tokens: BTreeMap<String, u32>,
    /// Next node-label token ID to allocate.
    #[serde(default)]
    pub next_node_label_id: u32,
    /// Next edge-label token ID to allocate.
    #[serde(default)]
    pub next_edge_label_id: u32,
    pub segments: Vec<SegmentInfo>,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    /// DB-scoped dense vector configuration.
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
    /// Schema catalog format marker. Version 0 with no entries is an old manifest.
    #[serde(default)]
    pub schema_catalog_version: u32,
    /// Next schema ID to allocate.
    #[serde(default)]
    pub next_schema_id: u64,
    /// Persisted node schema declarations keyed by internal node label IDs.
    #[serde(default)]
    pub node_schemas: Vec<NodeSchemaManifestEntry>,
    /// Persisted edge schema declarations keyed by internal edge label IDs.
    #[serde(default)]
    pub edge_schemas: Vec<EdgeSchemaManifestEntry>,
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
    pub labels: Vec<String>,
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
    Key { label: String, key: String },
    Local(TxnLocalRef),
}

/// Reference to an edge target inside a write transaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxnEdgeRef {
    Id(u64),
    Triple {
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
    },
    Local(TxnLocalRef),
}

/// Ordered logical write intent staged by a write transaction.
#[derive(Debug, Clone, PartialEq)]
pub enum TxnIntent {
    UpsertNode {
        alias: Option<String>,
        labels: Vec<String>,
        key: String,
        options: UpsertNodeOptions,
    },
    UpsertEdge {
        alias: Option<String>,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
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
    pub labels: Vec<String>,
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
    pub label: String,
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
    /// Only include edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
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
            edge_label_filter: None,
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
    /// Only include edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for DegreeOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_label_filter: None,
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
    /// Only include edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Scoring mode for ranking. Default: Weight.
    pub scoring: ScoringMode,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for TopKOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Only emit nodes matching this label filter. Default: None (all labels).
    pub emit_node_label_filter: Option<NodeLabelFilter>,
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
            edge_label_filter: None,
            emit_node_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Only include and expand through nodes matching this label filter. Default: None (all labels).
    pub node_label_filter: Option<NodeLabelFilter>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

impl Default for SubgraphOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_label_filter: None,
            node_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
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
            edge_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
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
            edge_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
    /// Maximum search depth in hops. Default: None (unlimited).
    pub max_depth: Option<u32>,
}

impl Default for IsConnectedOptions {
    fn default() -> Self {
        Self {
            direction: Direction::Outgoing,
            edge_label_filter: None,
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
    /// Only traverse edges with these labels. Default: None (all labels).
    pub edge_label_filter: Option<Vec<String>>,
    /// Only include nodes matching this label filter. Default: None (all labels).
    pub node_label_filter: Option<NodeLabelFilter>,
    /// Point-in-time epoch for temporal filtering. Default: None (current time).
    pub at_epoch: Option<i64>,
}

/// Input for batch edge upsert (user-facing, no ID or timestamps).
#[derive(Debug, Clone)]
pub struct EdgeInput {
    pub from: u64,
    pub to: u64,
    pub label: String,
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

/// User-facing information about a named prune policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrunePolicyInfo {
    pub name: String,
    pub policy: PrunePolicy,
}

/// Policy for pruning (deleting) nodes that match all specified criteria.
/// All fields are optional; when multiple are set, they combine with AND logic.
/// At least one of `max_age_ms` or `max_weight` must be set. An empty policy
/// (or one with only `label`) is rejected to prevent accidental mass deletion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrunePolicy {
    /// Prune nodes whose `updated_at` is older than `now - max_age_ms`.
    pub max_age_ms: Option<i64>,
    /// Prune nodes whose `weight <= max_weight`.
    pub max_weight: Option<f32>,
    /// Scope pruning to a single node label. If None, all labels are eligible.
    pub label: Option<String>,
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

/// Result of an offline integrity scrub of all segments in the database.
#[derive(Debug, Clone)]
pub struct ScrubReport {
    pub segments: Vec<SegmentScrubResult>,
    pub total_components_checked: u64,
    pub total_components_ok: u64,
    pub total_components_failed: u64,
    pub total_bytes_digested: u64,
    pub duration_ms: u64,
}

/// Scrub result for a single segment.
#[derive(Debug, Clone)]
pub struct SegmentScrubResult {
    pub segment_id: u64,
    pub findings: Vec<ComponentScrubFinding>,
    pub components_ok: u64,
    pub bytes_digested: u64,
}

/// A single problem detected during offline scrub.
#[derive(Debug, Clone)]
pub struct ComponentScrubFinding {
    pub component_kind: String,
    pub finding_type: ScrubFindingType,
    pub detail: String,
}

/// Classification of a scrub finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrubFindingType {
    PayloadDigestMismatch,
    ComponentIdMismatch,
    DependencyDigestMismatch,
    IdentityHeaderMismatch,
    ContainerIdMismatch,
    SegmentIdentityMismatch,
    SemanticMismatch,
    RangeOverflow,
    RangeOverlap,
    FileMissing,
    IoError,
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
    /// The edge label.
    pub label: String,
    /// The edge weight.
    pub weight: f32,
    /// Start of validity window (epoch ms). 0 means always-valid.
    pub valid_from: i64,
    /// End of validity window (epoch ms). i64::MAX means open-ended.
    pub valid_to: i64,
}

/// Internal numeric adjacency entry used by graph traversal/storage paths.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NeighborRecord {
    pub node_id: u64,
    pub edge_id: u64,
    pub edge_label_id: u32,
    pub weight: f32,
    pub valid_from: i64,
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
#[derive(Debug, Clone, PartialEq)]
pub struct Subgraph {
    /// All nodes in the subgraph (including the starting node).
    pub nodes: Vec<NodeView>,
    /// All edges connecting nodes in the subgraph discovered during traversal.
    pub edges: Vec<EdgeView>,
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
    /// Optional edge label filter. Only walk edges with these labels.
    pub edge_label_filter: Option<Vec<String>>,
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
            edge_label_filter: None,
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
    /// Only include nodes matching this label filter. None means all labels.
    pub node_label_filter: Option<NodeLabelFilter>,
    /// Only include edges with these labels. None means all labels.
    pub edge_label_filter: Option<Vec<String>>,
    /// Include edge weights in the output. Default: true.
    pub include_weights: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            node_label_filter: None,
            edge_label_filter: None,
            include_weights: true,
        }
    }
}

/// An exported edge. `edge_label_index` is local to the export's `edge_labels` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExportEdge {
    pub from: u64,
    pub to: u64,
    pub edge_label_index: u32,
    pub weight: Option<f32>,
}

/// Result of a graph adjacency export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdjacencyExport {
    /// All live node IDs in the exported subgraph.
    pub node_ids: Vec<u64>,
    /// Export-local node-label side table.
    #[serde(default)]
    pub node_labels: Vec<String>,
    /// Per-node label side-table indexes, aligned with `node_ids`.
    #[serde(default)]
    pub node_label_indexes: Vec<Vec<u32>>,
    /// Export-local edge-label side table.
    pub edge_labels: Vec<String>,
    /// All live edges, referencing `edge_labels` by export-local index.
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

    fn assert_invalid_secondary_index(result: Result<(), EngineError>, expected: &str) {
        let err = result.expect_err("secondary index spec should be invalid");
        let message = err.to_string();
        assert!(
            message.contains("invalid secondary index:"),
            "missing invalid secondary index prefix in `{message}`"
        );
        assert!(
            message.contains(expected),
            "expected `{expected}` in `{message}`"
        );
    }

    #[test]
    fn secondary_index_spec_validates_field_counts() {
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(Vec::new()).validate_for_node(),
            "field list must contain 1 to 8 fields",
        );

        SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("a")])
            .validate_for_node()
            .unwrap();

        let eight_fields = (0..MAX_SECONDARY_INDEX_FIELDS)
            .map(|idx| SecondaryIndexField::property(format!("p{idx}")))
            .collect::<Vec<_>>();
        SecondaryIndexSpec::equality(eight_fields)
            .validate_for_node()
            .unwrap();

        let nine_fields = (0..=MAX_SECONDARY_INDEX_FIELDS)
            .map(|idx| SecondaryIndexField::property(format!("p{idx}")))
            .collect::<Vec<_>>();
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(nine_fields).validate_for_node(),
            "compound secondary indexes support at most 8 fields",
        );
    }

    #[test]
    fn secondary_index_spec_rejects_duplicate_and_invalid_fields() {
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::property("status"),
            ])
            .validate_for_node(),
            "duplicate field property `status`",
        );
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
            ])
            .validate_for_node(),
            "duplicate field node metadata `updated_at`",
        );
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("")])
                .validate_for_node(),
            "property key must not be empty",
        );
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::edge_meta(
                EdgeMetadataIndexField::From,
            )])
            .validate_for_node(),
            "node indexes cannot include edge metadata fields",
        );
        assert_invalid_secondary_index(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::Key,
            )])
            .validate_for_edge(),
            "edge indexes cannot include node metadata fields",
        );

        SecondaryIndexSpec::equality(vec![
            SecondaryIndexField::property("updated_at"),
            SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
        ])
        .validate_for_node()
        .unwrap();
    }

    #[test]
    fn secondary_index_spec_routes_to_physical_manifest_targets() {
        assert_eq!(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")])
                .node_target(7)
                .unwrap(),
            SecondaryIndexTarget::NodeProperty {
                label_id: 7,
                prop_key: "status".to_string(),
            }
        );
        assert_eq!(
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("score")])
                .edge_target(9)
                .unwrap(),
            SecondaryIndexTarget::EdgeProperty {
                label_id: 9,
                prop_key: "score".to_string(),
            }
        );
        assert_eq!(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::UpdatedAt,
            )])
            .node_target(7)
            .unwrap(),
            SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: NodeMetadataIndexFieldManifest::UpdatedAt,
                }],
            }
        );
        assert_eq!(
            SecondaryIndexSpec::equality(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::property("tier"),
            ])
            .node_target(7)
            .unwrap(),
            SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "status".to_string(),
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "tier".to_string(),
                    },
                ],
            }
        );
        assert_eq!(
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::edge_meta(
                EdgeMetadataIndexField::UpdatedAt,
            )])
            .edge_target(9)
            .unwrap(),
            SecondaryIndexTarget::EdgeFieldIndex {
                label_id: 9,
                fields: vec![SecondaryIndexFieldManifest::EdgeMetadata {
                    field: EdgeMetadataIndexFieldManifest::UpdatedAt,
                }],
            }
        );
    }

    #[test]
    fn secondary_index_logical_identity_collapses_legacy_and_field_property_targets() {
        let legacy = SecondaryIndexManifestEntry {
            index_id: 1,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 4,
                prop_key: "status".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };
        let field_target = SecondaryIndexManifestEntry {
            index_id: 2,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 4,
                fields: vec![SecondaryIndexFieldManifest::Property {
                    key: "status".to_string(),
                }],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Building,
            last_error: None,
        };

        assert_eq!(
            secondary_index_logical_identity(&legacy).unwrap(),
            secondary_index_logical_identity(&field_target).unwrap()
        );
    }

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
        assert_eq!(OpTag::from_u8(5), Some(OpTag::EnsureNodeLabel));
        assert_eq!(OpTag::from_u8(6), Some(OpTag::EnsureEdgeLabel));
        assert_eq!(OpTag::from_u8(7), Some(OpTag::BeginAtomicBatch));
        assert_eq!(OpTag::from_u8(8), Some(OpTag::CommitAtomicBatch));
        assert!(OpTag::from_u8(255).is_none());
    }

    #[test]
    fn test_node_label_set_canonicalizes_distinct_ids() {
        let set = NodeLabelSet::from_label_ids([7, 3, 5]).unwrap();
        assert_eq!(set.len(), 3);
        assert_eq!(set.as_slice(), &[3, 5, 7]);
        assert!(set.contains(5));
        assert!(!set.contains(4));
        assert!(set.contains_all(&NodeLabelSet::from_label_ids([3, 7]).unwrap()));
        assert!(set.contains_any(&NodeLabelSet::from_label_ids([2, 7]).unwrap()));
        assert!(!set.contains_any(&NodeLabelSet::from_label_ids([1, 2]).unwrap()));
        assert_eq!(NodeLabelSet::single(9).unwrap().as_slice(), &[9]);
    }

    #[test]
    fn test_node_label_set_rejects_empty_duplicate_zero_and_too_many_ids() {
        assert!(NodeLabelSet::from_label_ids([]).is_err());
        assert!(NodeLabelSet::from_label_ids([3, 3]).is_err());
        assert!(NodeLabelSet::from_label_ids([0]).is_err());
        assert!(NodeLabelSet::from_label_ids(1..=11).is_err());
    }

    #[test]
    fn test_node_label_set_canonical_decoder_rejects_unsorted_and_duplicates() {
        assert_eq!(
            NodeLabelSet::from_canonical_ids(&[1, 3, 5])
                .unwrap()
                .as_slice(),
            &[1, 3, 5]
        );
        assert!(NodeLabelSet::from_canonical_ids(&[]).is_err());
        assert!(NodeLabelSet::from_canonical_ids(&[2, 1]).is_err());
        assert!(NodeLabelSet::from_canonical_ids(&[2, 2]).is_err());
        assert!(NodeLabelSet::from_canonical_ids(&(1..=11).collect::<Vec<_>>()).is_err());
    }

    #[test]
    fn test_public_node_label_list_validation() {
        validate_public_node_label_list(["Person", "Employee"]).unwrap();
        assert!(validate_public_node_label_list(std::iter::empty::<&str>()).is_err());
        assert!(validate_public_node_label_list(["Person", "Person"]).is_err());
        assert!(validate_public_node_label_list([" Person"]).is_err());
        assert!(validate_public_node_label_list([
            "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10", "L11"
        ])
        .is_err());
    }

    #[test]
    fn test_node_label_filter_validation_rejects_empty_and_duplicate_labels() {
        validate_node_label_filter(&NodeLabelFilter {
            labels: vec!["Person".to_string(), "Employee".to_string()],
            mode: LabelMatchMode::All,
        })
        .unwrap();
        assert!(validate_node_label_filter(&NodeLabelFilter {
            labels: Vec::new(),
            mode: LabelMatchMode::Any,
        })
        .is_err());
        assert!(validate_node_label_filter(&NodeLabelFilter {
            labels: vec!["Person".to_string(), "Person".to_string()],
            mode: LabelMatchMode::Any,
        })
        .is_err());
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
            label: "FRIENDS_WITH".to_string(),
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
            label_token_schema_version: LABEL_TOKEN_SCHEMA_VERSION,
            node_label_tokens: BTreeMap::new(),
            edge_label_tokens: BTreeMap::new(),
            next_node_label_id: 1,
            next_edge_label_id: 1,
            segments: vec![
                SegmentInfo {
                    id: 1,
                    node_count: 100,
                    edge_count: 200,
                    segment_format_version: 10,
                    segment_data_id: [1; 32],
                },
                SegmentInfo {
                    id: 2,
                    node_count: 50,
                    edge_count: 75,
                    segment_format_version: 10,
                    segment_data_id: [2; 32],
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
            schema_catalog_version: SCHEMA_CATALOG_VERSION,
            next_schema_id: 1,
            node_schemas: Vec::new(),
            edge_schemas: Vec::new(),
        };
        let json = serde_json::to_string(&state).unwrap();
        let loaded: ManifestState = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.segments.len(), 2);
        assert_eq!(loaded.next_node_id, 151);
        assert_eq!(loaded.next_edge_id, 276);
        assert_eq!(loaded.dense_vector, state.dense_vector);
        assert_eq!(loaded.schema_catalog_version, SCHEMA_CATALOG_VERSION);
        assert_eq!(loaded.next_schema_id, 1);
        assert!(loaded.node_schemas.is_empty());
        assert!(loaded.edge_schemas.is_empty());
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
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.limit.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.decay_lambda.is_none());
    }

    #[test]
    fn test_degree_options_default() {
        let opts = DegreeOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.at_epoch.is_none());
    }

    #[test]
    fn test_traverse_options_default() {
        let opts = TraverseOptions::default();
        assert_eq!(opts.min_depth, 1);
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.emit_node_label_filter.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.decay_lambda.is_none());
        assert!(opts.limit.is_none());
        assert!(opts.cursor.is_none());
    }

    #[test]
    fn test_subgraph_options_default() {
        let opts = SubgraphOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.node_label_filter.is_none());
        assert!(opts.at_epoch.is_none());
    }

    #[test]
    fn test_shortest_path_options_default() {
        let opts = ShortestPathOptions::default();
        assert_eq!(opts.direction, Direction::Outgoing);
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.weight_field.is_none());
        assert!(opts.at_epoch.is_none());
        assert!(opts.max_depth.is_none());
        assert!(opts.max_cost.is_none());
    }

    #[test]
    fn test_component_options_default() {
        let opts = ComponentOptions::default();
        assert!(opts.edge_label_filter.is_none());
        assert!(opts.node_label_filter.is_none());
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
