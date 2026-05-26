"""Type stubs for the overgraph Python connector."""

from typing import Any, Callable, Mapping, Sequence, TypedDict

IntList = list[int] | tuple[int, ...]
StrList = list[str] | tuple[str, ...]
NodeLabels = str | list[str] | tuple[str, ...]
MappingList = list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]
QueryNodeFilter = Mapping[str, Any]
QueryEdgeFilter = Mapping[str, Any]
NodeLabelFilter = Mapping[str, Any]
GqlScalar = None | bool | int | float | str | bytes
GqlParam = GqlScalar | list["GqlParam"] | tuple["GqlParam", ...] | Mapping[str, "GqlParam"]
GqlParams = Mapping[str, GqlParam]

# ============================================================
# Data types
# ============================================================

class ScrubReport:
    total_components_checked: int
    total_components_ok: int
    total_components_failed: int
    total_bytes_digested: int
    duration_ms: int
    @property
    def segments(self) -> list[SegmentScrubResult]: ...
    def __repr__(self) -> str: ...

class SegmentScrubResult:
    segment_id: int
    components_ok: int
    bytes_digested: int
    @property
    def findings(self) -> list[ComponentScrubFinding]: ...
    def __repr__(self) -> str: ...

class ComponentScrubFinding:
    component_kind: str
    finding_type: str
    detail: str
    def __repr__(self) -> str: ...

class DbStats:
    pending_wal_bytes: int
    segment_count: int
    node_tombstone_count: int
    edge_tombstone_count: int
    last_compaction_ms: int | None
    wal_sync_mode: str
    active_memtable_bytes: int
    immutable_memtable_bytes: int
    immutable_memtable_count: int
    pending_flush_count: int
    active_wal_generation_id: int
    oldest_retained_wal_generation_id: int
    def __repr__(self) -> str: ...

class NodeView:
    id: int
    labels: list[str]
    key: str
    props: dict[str, Any]
    weight: float
    dense_vector: list[float] | None
    sparse_vector: list[tuple[int, float]] | None
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class EdgeView:
    id: int
    from_id: int
    to_id: int
    label: str
    props: dict[str, Any]
    weight: float
    valid_from: int
    valid_to: int
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class PatchResult:
    node_ids: list[int]
    edge_ids: list[int]
    def __repr__(self) -> str: ...

class TxnCommitResult:
    node_ids: list[int]
    edge_ids: list[int]
    node_aliases: dict[str, int]
    edge_aliases: dict[str, int]
    def __repr__(self) -> str: ...

class NeighborEntry:
    node_id: int
    edge_id: int
    label: str
    weight: float
    valid_from: int
    valid_to: int
    def __repr__(self) -> str: ...

class TraversalHit:
    node_id: int
    depth: int
    via_edge_id: int | None
    score: float | None
    def __repr__(self) -> str: ...

class VectorHit:
    node_id: int
    score: float
    def __repr__(self) -> str: ...

class NodePropertyIndexInfo:
    index_id: int
    label: str
    prop_key: str
    kind: str
    state: str
    last_error: str | None
    def __repr__(self) -> str: ...

class EdgePropertyIndexInfo:
    index_id: int
    label: str
    prop_key: str
    kind: str
    state: str
    last_error: str | None
    def __repr__(self) -> str: ...

class PropertyRangeBound:
    value: int | float
    inclusive: bool
    domain: str
    def __init__(self, value: int | float, *, inclusive: bool = True, domain: str) -> None: ...
    def __repr__(self) -> str: ...

class PropertyRangeCursor:
    value: int | float
    node_id: int
    domain: str
    def __init__(self, value: int | float, node_id: int, *, domain: str) -> None: ...
    def __repr__(self) -> str: ...

class TraversalCursor:
    depth: int
    last_node_id: int
    def __init__(self, depth: int, last_node_id: int) -> None: ...
    def __repr__(self) -> str: ...

class ShortestPath:
    nodes: list[int]
    edges: list[int]
    total_cost: float
    def __repr__(self) -> str: ...

class Subgraph:
    nodes: list[NodeView]
    edges: list[EdgeView]
    def __repr__(self) -> str: ...

class PruneResult:
    nodes_pruned: int
    edges_pruned: int
    def __repr__(self) -> str: ...

class NamedPrunePolicy:
    name: str
    max_age_ms: int | None
    max_weight: float | None
    label: str | None
    def __repr__(self) -> str: ...

class NodeLabelInfo:
    label: str
    label_id: int
    def __repr__(self) -> str: ...

class EdgeLabelInfo:
    label: str
    label_id: int
    def __repr__(self) -> str: ...

class SegmentInfo:
    id: int
    node_count: int
    edge_count: int
    def __repr__(self) -> str: ...

class CompactionStats:
    segments_merged: int
    nodes_kept: int
    nodes_removed: int
    edges_kept: int
    edges_removed: int
    duration_ms: int
    output_segment_id: int
    nodes_auto_pruned: int
    edges_auto_pruned: int
    def __repr__(self) -> str: ...

class CompactionProgress:
    phase: str
    segments_processed: int
    total_segments: int
    records_processed: int
    total_records: int
    def __repr__(self) -> str: ...

class IdArray:
    """Lazy sequence wrapper. Data stays in Rust, converted on access."""
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> int: ...
    def __iter__(self) -> IdArrayIter: ...
    def __bool__(self) -> bool: ...
    def __contains__(self, val: int) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...
    def to_list(self) -> list[int]: ...

class IdArrayIter:
    def __iter__(self) -> IdArrayIter: ...
    def __next__(self) -> int: ...

class IdPageResult:
    items: IdArray
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class NodePageResult:
    items: list[NodeView]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class EdgePageResult:
    items: list[EdgeView]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class NeighborPageResult:
    items: list[NeighborEntry]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PropertyRangePageResult:
    items: IdArray
    next_cursor: PropertyRangeCursor | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class TraversalPageResult:
    items: list[TraversalHit]
    next_cursor: TraversalCursor | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PprResult:
    node_ids: list[int]
    scores: list[float]
    iterations: int
    converged: bool
    algorithm: str
    approx: PprApproxMeta | None
    def __repr__(self) -> str: ...

class PprApproxMeta:
    residual_tolerance: float
    pushes: int
    max_remaining_residual: float
    def __repr__(self) -> str: ...

class ExportEdge:
    from_id: int
    to_id: int
    edge_label_index: int
    weight: float | None
    def __repr__(self) -> str: ...

class AdjacencyExport:
    node_ids: list[int]
    node_labels: list[str]
    node_label_indexes: list[list[int]]
    edge_labels: list[str]
    edges: list[ExportEdge]
    def __repr__(self) -> str: ...

class NodeQueryRequest:
    label_filter: NodeLabelFilter | None
    ids: IntList | None
    keys: StrList | None
    filter: QueryNodeFilter | None
    order_by: str | None
    limit: int | None
    after: int | None
    allow_full_scan: bool
    def __init__(
        self,
        label_filter: NodeLabelFilter | None = None,
        ids: IntList | None = None,
        keys: StrList | None = None,
        filter: QueryNodeFilter | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        after: int | None = None,
        allow_full_scan: bool = False,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

QueryNodeRequest = NodeQueryRequest

class EdgeQueryRequest:
    label: str | None
    ids: IntList | None
    from_ids: IntList | None
    to_ids: IntList | None
    endpoint_ids: IntList | None
    filter: QueryEdgeFilter | None
    limit: int | None
    after: int | None
    allow_full_scan: bool
    def __init__(
        self,
        label: str | None = None,
        ids: IntList | None = None,
        from_ids: IntList | None = None,
        to_ids: IntList | None = None,
        endpoint_ids: IntList | None = None,
        filter: QueryEdgeFilter | None = None,
        limit: int | None = None,
        after: int | None = None,
        allow_full_scan: bool = False,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

QueryEdgeRequest = EdgeQueryRequest

GraphParamValue = None | bool | int | float | str | bytes | list["GraphParamValue"] | dict[str, "GraphParamValue"]
GraphExpr = Any
GraphRowRequest = Mapping[str, Any]

class GraphPathValue(TypedDict, total=False):
    node_ids: list[int]
    edge_ids: list[int]
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]

GraphValue = None | bool | int | float | str | bytes | list["GraphValue"] | dict[str, Any] | GraphPathValue

class GraphRowStats(TypedDict):
    rows_returned: int
    rows_after_filter: int
    rows_seen_for_page: int
    intermediate_bindings_peak: int
    frontier_peak: int
    paths_enumerated: int
    db_hits: int
    elapsed_us: int | None
    effective_at_epoch: int
    warnings: list[str]

class GraphRowResult(TypedDict):
    columns: list[str]
    rows: list[dict[str, GraphValue]] | list[list[GraphValue]]
    next_cursor: str | None
    stats: GraphRowStats
    plan: dict[str, Any] | None

GraphRowExplain = dict[str, Any]

class GqlNode(TypedDict, total=False):
    id: int
    labels: list[str]
    key: str
    props: dict[str, Any]
    weight: float
    created_at: int
    updated_at: int
    dense_vector: list[float]
    sparse_vector: list[tuple[int, float]]

class GqlEdge(TypedDict, total=False):
    id: int
    from_id: int
    to_id: int
    label: str
    props: dict[str, Any]
    weight: float
    created_at: int
    updated_at: int
    valid_from: int
    valid_to: int

class GqlPath(TypedDict, total=False):
    node_ids: list[int]
    edge_ids: list[int]
    nodes: list[GqlNode]
    edges: list[GqlEdge]

GqlValue = GqlScalar | list["GqlValue"] | dict[str, "GqlValue"] | GqlNode | GqlEdge | GqlPath

class GqlExecutionStats(TypedDict):
    rows_returned: int
    rows_matched: int
    rows_after_filter: int
    intermediate_bindings: int
    db_hits: int
    elapsed_us: int | None
    truncated: bool
    warnings: list[str]

class GqlCapSummary(TypedDict):
    allow_full_scan: bool
    max_rows: int
    max_intermediate_bindings: int
    max_skip: int
    max_query_bytes: int
    max_param_bytes: int
    max_ast_depth: int
    max_literal_items: int

class GqlExplain(TypedDict):
    columns: list[str]
    target: str
    native_plan: dict[str, Any] | None
    pushed_down: list[str]
    residual: list[str]
    projection: list[str]
    row_ops: list[str]
    caps: GqlCapSummary
    warnings: list[str]

class GqlResult(TypedDict):
    columns: list[str]
    rows: list[dict[str, GqlValue]] | list[list[GqlValue]]
    next_cursor: str | None
    stats: GqlExecutionStats
    plan: GqlExplain | None

# ============================================================
# Exception
# ============================================================

class OverGraphError(Exception): ...

# ============================================================
# Sync API
# ============================================================

class OverGraph:
    @staticmethod
    def open(path: str, **kwargs: Any) -> OverGraph: ...
    def close(self, force: bool = False) -> None: ...
    def __enter__(self) -> OverGraph: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Any = None,
    ) -> bool: ...
    def stats(self) -> DbStats: ...
    def scrub(self) -> ScrubReport: ...

    # Catalog
    def ensure_node_label(self, label: str) -> int: ...
    def ensure_edge_label(self, label: str) -> int: ...
    def get_node_label_id(self, label: str) -> int | None: ...
    def get_edge_label_id(self, label: str) -> int | None: ...
    def get_node_label(self, label_id: int) -> str | None: ...
    def get_edge_label(self, label_id: int) -> str | None: ...
    def list_node_labels(self) -> list[NodeLabelInfo]: ...
    def list_edge_labels(self) -> list[EdgeLabelInfo]: ...

    # Single CRUD
    def upsert_node(
        self,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> int: ...
    def add_node_label(self, node_id: int, label: str) -> bool: ...
    def remove_node_label(self, node_id: int, label: str) -> bool: ...
    def upsert_edge(
        self,
        from_id: int,
        to_id: int,
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> int: ...
    def get_node(self, node_id: int) -> NodeView | None: ...
    def get_edge(self, edge_id: int) -> EdgeView | None: ...
    def get_node_by_key(self, label: str, key: str) -> NodeView | None: ...
    def get_edge_by_triple(self, from_id: int, to_id: int, label: str) -> EdgeView | None: ...
    def delete_node(self, node_id: int) -> None: ...
    def delete_edge(self, edge_id: int) -> None: ...
    def invalidate_edge(self, edge_id: int, valid_to: int) -> EdgeView | None: ...

    # Batch
    def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    def get_nodes(self, node_ids: list[int]) -> list[NodeView | None]: ...
    def get_nodes_by_keys(self, keys: list[dict[str, Any]]) -> list[NodeView | None]: ...
    def get_edges(self, edge_ids: list[int]) -> list[EdgeView | None]: ...
    def graph_patch(self, patch: dict[str, Any]) -> PatchResult: ...
    def begin_write_txn(self) -> WriteTxn: ...

    # Queries
    def find_nodes(self, label: str, prop_key: str, prop_value: Any) -> IdArray: ...
    def query_node_ids(self, request: dict[str, Any] | NodeQueryRequest) -> IdPageResult: ...
    def query_nodes(self, request: dict[str, Any] | NodeQueryRequest) -> NodePageResult: ...
    def query_edge_ids(self, request: dict[str, Any] | EdgeQueryRequest) -> IdPageResult: ...
    def query_edges(self, request: dict[str, Any] | EdgeQueryRequest) -> EdgePageResult: ...
    def query_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowResult: ...
    def explain_node_query(self, request: dict[str, Any] | NodeQueryRequest) -> dict[str, Any]: ...
    def explain_edge_query(self, request: dict[str, Any] | EdgeQueryRequest) -> dict[str, Any]: ...
    def explain_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowExplain: ...
    def execute_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlResult: ...
    def explain_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlExplain: ...
    def ensure_node_property_index(self, label: str, prop_key: str, kind: str) -> NodePropertyIndexInfo: ...
    def drop_node_property_index(self, label: str, prop_key: str, kind: str) -> bool: ...
    def list_node_property_indexes(self) -> list[NodePropertyIndexInfo]: ...
    def ensure_edge_property_index(self, label: str, prop_key: str, kind: str) -> EdgePropertyIndexInfo: ...
    def drop_edge_property_index(self, label: str, prop_key: str, kind: str) -> bool: ...
    def list_edge_property_indexes(self) -> list[EdgePropertyIndexInfo]: ...
    def nodes_by_labels(self, labels: NodeLabels) -> IdArray: ...
    def edges_by_label(self, label: str) -> IdArray: ...
    def get_nodes_by_labels(self, labels: NodeLabels) -> list[NodeView]: ...
    def get_edges_by_label(self, label: str) -> list[EdgeView]: ...
    def count_nodes_by_labels(self, labels: NodeLabels) -> int: ...
    def count_edges_by_label(self, label: str) -> int: ...
    def find_nodes_by_time_range(self, label: str, from_ms: int, to_ms: int) -> IdArray: ...
    def find_nodes_range(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None) -> IdArray: ...

    # Binary batch
    def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...

    # Traversal
    def neighbors(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[NeighborEntry]: ...
    def traverse(
        self,
        start: int,
        max_depth: int,
        *,
        min_depth: int = 1,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        emit_node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
        limit: int | None = None,
        cursor: TraversalCursor | None = None,
    ) -> TraversalPageResult: ...
    def top_k_neighbors(
        self,
        node_id: int,
        k: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        scoring: str = "weight",
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[NeighborEntry]: ...
    def extract_subgraph(
        self,
        start_node_id: int,
        max_depth: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> Subgraph: ...
    def neighbors_batch(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> dict[int, list[NeighborEntry]]: ...

    # Degree counts + aggregations
    def degree(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> int: ...
    def sum_edge_weights(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> float: ...
    def avg_edge_weight(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> float | None: ...
    def degrees(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]: ...

    # Shortest path
    def shortest_path(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
    ) -> ShortestPath | None: ...
    def is_connected(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
    ) -> bool: ...
    def all_shortest_paths(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
        max_paths: int | None = None,
    ) -> list[ShortestPath]: ...

    # Retention
    def prune(
        self,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        label: str | None = None,
    ) -> PruneResult: ...
    def set_prune_policy(
        self,
        name: str,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        label: str | None = None,
    ) -> None: ...
    def remove_prune_policy(self, name: str) -> bool: ...
    def list_prune_policies(self) -> list[NamedPrunePolicy]: ...

    # Maintenance
    def sync(self) -> None: ...
    def flush(self) -> SegmentInfo | None: ...
    def ingest_mode(self) -> None: ...
    def end_ingest(self) -> CompactionStats | None: ...
    def compact(self) -> CompactionStats | None: ...
    def compact_with_progress(self, callback: Callable[[CompactionProgress], bool]) -> CompactionStats | None: ...

    # Pagination
    def nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def get_nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> NodePageResult: ...
    def get_edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> EdgePageResult: ...
    def find_nodes_paged(self, label: str, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def find_nodes_by_time_range_paged(self, label: str, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def find_nodes_range_paged(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None, *, limit: int | None = None, after: PropertyRangeCursor | None = None) -> PropertyRangePageResult: ...
    def neighbors_paged(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> NeighborPageResult: ...

    # Analytics
    def personalized_pagerank(
        self,
        seed_node_ids: list[int],
        *,
        algorithm: str | None = None,
        damping_factor: float | None = None,
        max_iterations: int | None = None,
        epsilon: float | None = None,
        approx_residual_tolerance: float | None = None,
        edge_label_filter: list[str] | None = None,
        max_results: int | None = None,
    ) -> PprResult: ...
    def export_adjacency(
        self,
        *,
        node_label_filter: NodeLabelFilter | None = None,
        edge_label_filter: list[str] | None = None,
        include_weights: bool = True,
    ) -> AdjacencyExport: ...

    # Connected components
    def connected_components(
        self,
        *,
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]: ...
    def component_of(
        self,
        node_id: int,
        *,
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> list[int]: ...

    # Vector search
    def vector_search(
        self,
        mode: str,
        k: int,
        *,
        dense_query: list[float] | None = None,
        sparse_query: list[tuple[int, float]] | None = None,
        label_filter: NodeLabelFilter | None = None,
        ef_search: int | None = None,
        scope_start_node_id: int | None = None,
        scope_max_depth: int | None = None,
        scope_direction: str | None = None,
        scope_edge_label_filter: list[str] | None = None,
        scope_at_epoch: int | None = None,
        dense_weight: float | None = None,
        sparse_weight: float | None = None,
        fusion_mode: str | None = None,
    ) -> list[VectorHit]: ...

class WriteTxn:
    def upsert_node(
        self,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]: ...
    def upsert_node_as(
        self,
        alias: str,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]: ...
    def add_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    def remove_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    def upsert_edge(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]: ...
    def upsert_edge_as(
        self,
        alias: str,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]: ...
    def delete_node(self, target: dict[str, Any]) -> None: ...
    def delete_edge(self, target: dict[str, Any]) -> None: ...
    def invalidate_edge(self, target: dict[str, Any], valid_to: int) -> None: ...
    def stage(self, operations: list[dict[str, Any]]) -> None: ...
    def get_node(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    def get_edge(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    def get_node_by_key(self, label: str, key: str) -> dict[str, Any] | None: ...
    def get_edge_by_triple(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
    ) -> dict[str, Any] | None: ...
    def commit(self) -> TxnCommitResult: ...
    def rollback(self) -> None: ...

# ============================================================
# Async API
# ============================================================

class AsyncOverGraph:
    @staticmethod
    async def open(path: str, **kwargs: Any) -> AsyncOverGraph: ...
    async def close(self, force: bool = False) -> None: ...
    async def __aenter__(self) -> AsyncOverGraph: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None: ...
    async def stats(self) -> DbStats: ...
    async def scrub(self) -> ScrubReport: ...

    # Catalog
    async def ensure_node_label(self, label: str) -> int: ...
    async def ensure_edge_label(self, label: str) -> int: ...
    async def get_node_label_id(self, label: str) -> int | None: ...
    async def get_edge_label_id(self, label: str) -> int | None: ...
    async def get_node_label(self, label_id: int) -> str | None: ...
    async def get_edge_label(self, label_id: int) -> str | None: ...
    async def list_node_labels(self) -> list[NodeLabelInfo]: ...
    async def list_edge_labels(self) -> list[EdgeLabelInfo]: ...

    # Single CRUD
    async def upsert_node(self, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> int: ...
    async def add_node_label(self, node_id: int, label: str) -> bool: ...
    async def remove_node_label(self, node_id: int, label: str) -> bool: ...
    async def upsert_edge(self, from_id: int, to_id: int, label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> int: ...
    async def get_node(self, node_id: int) -> NodeView | None: ...
    async def get_edge(self, edge_id: int) -> EdgeView | None: ...
    async def get_node_by_key(self, label: str, key: str) -> NodeView | None: ...
    async def get_edge_by_triple(self, from_id: int, to_id: int, label: str) -> EdgeView | None: ...
    async def delete_node(self, node_id: int) -> None: ...
    async def delete_edge(self, edge_id: int) -> None: ...
    async def invalidate_edge(self, edge_id: int, valid_to: int) -> EdgeView | None: ...

    # Batch
    async def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    async def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...
    async def get_nodes(self, node_ids: list[int]) -> list[NodeView | None]: ...
    async def get_nodes_by_keys(self, keys: list[dict[str, Any]]) -> list[NodeView | None]: ...
    async def get_edges(self, edge_ids: list[int]) -> list[EdgeView | None]: ...
    async def graph_patch(self, patch: dict[str, Any]) -> PatchResult: ...
    async def begin_write_txn(self) -> AsyncWriteTxn: ...

    # Queries
    async def find_nodes(self, label: str, prop_key: str, prop_value: Any) -> IdArray: ...
    async def query_node_ids(self, request: dict[str, Any] | NodeQueryRequest) -> IdPageResult: ...
    async def query_nodes(self, request: dict[str, Any] | NodeQueryRequest) -> NodePageResult: ...
    async def query_edge_ids(self, request: dict[str, Any] | EdgeQueryRequest) -> IdPageResult: ...
    async def query_edges(self, request: dict[str, Any] | EdgeQueryRequest) -> EdgePageResult: ...
    async def query_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowResult: ...
    async def explain_node_query(self, request: dict[str, Any] | NodeQueryRequest) -> dict[str, Any]: ...
    async def explain_edge_query(self, request: dict[str, Any] | EdgeQueryRequest) -> dict[str, Any]: ...
    async def explain_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowExplain: ...
    async def execute_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        **options: Any,
    ) -> GqlResult: ...
    async def explain_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        **options: Any,
    ) -> GqlExplain: ...
    async def ensure_node_property_index(self, label: str, prop_key: str, kind: str) -> NodePropertyIndexInfo: ...
    async def drop_node_property_index(self, label: str, prop_key: str, kind: str) -> bool: ...
    async def list_node_property_indexes(self) -> list[NodePropertyIndexInfo]: ...
    async def ensure_edge_property_index(self, label: str, prop_key: str, kind: str) -> EdgePropertyIndexInfo: ...
    async def drop_edge_property_index(self, label: str, prop_key: str, kind: str) -> bool: ...
    async def list_edge_property_indexes(self) -> list[EdgePropertyIndexInfo]: ...
    async def nodes_by_labels(self, labels: NodeLabels) -> IdArray: ...
    async def edges_by_label(self, label: str) -> IdArray: ...
    async def get_nodes_by_labels(self, labels: NodeLabels) -> list[NodeView]: ...
    async def get_edges_by_label(self, label: str) -> list[EdgeView]: ...
    async def count_nodes_by_labels(self, labels: NodeLabels) -> int: ...
    async def count_edges_by_label(self, label: str) -> int: ...
    async def find_nodes_by_time_range(self, label: str, from_ms: int, to_ms: int) -> IdArray: ...
    async def find_nodes_range(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None) -> IdArray: ...

    # Traversal
    async def neighbors(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, limit: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> list[NeighborEntry]: ...
    async def traverse(self, start: int, max_depth: int, *, min_depth: int = 1, direction: str = "outgoing", edge_label_filter: list[str] | None = None, emit_node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None, decay_lambda: float | None = None, limit: int | None = None, cursor: TraversalCursor | None = None) -> TraversalPageResult: ...
    async def top_k_neighbors(self, node_id: int, k: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, scoring: str = "weight", at_epoch: int | None = None, decay_lambda: float | None = None) -> list[NeighborEntry]: ...
    async def extract_subgraph(self, start_node_id: int, max_depth: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> Subgraph: ...
    async def neighbors_batch(self, node_ids: list[int], *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> dict[int, list[NeighborEntry]]: ...

    # Degree counts + aggregations
    async def degree(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> int: ...
    async def sum_edge_weights(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> float: ...
    async def avg_edge_weight(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> float | None: ...
    async def degrees(self, node_ids: list[int], *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> dict[int, int]: ...

    # Shortest path
    async def shortest_path(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None) -> ShortestPath | None: ...
    async def is_connected(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None, max_depth: int | None = None) -> bool: ...
    async def all_shortest_paths(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None, max_paths: int | None = None) -> list[ShortestPath]: ...

    # Retention
    async def prune(self, *, max_age_ms: int | None = None, max_weight: float | None = None, label: str | None = None) -> PruneResult: ...
    async def set_prune_policy(self, name: str, *, max_age_ms: int | None = None, max_weight: float | None = None, label: str | None = None) -> None: ...
    async def remove_prune_policy(self, name: str) -> bool: ...
    async def list_prune_policies(self) -> list[NamedPrunePolicy]: ...

    # Maintenance
    async def sync(self) -> None: ...
    async def flush(self) -> SegmentInfo | None: ...
    async def ingest_mode(self) -> None: ...
    async def end_ingest(self) -> CompactionStats | None: ...
    async def compact(self) -> CompactionStats | None: ...
    async def compact_with_progress(self, callback: Callable[[CompactionProgress], bool]) -> CompactionStats | None: ...

    # Pagination
    async def nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def get_nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> NodePageResult: ...
    async def get_edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> EdgePageResult: ...
    async def find_nodes_paged(self, label: str, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def find_nodes_by_time_range_paged(self, label: str, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def find_nodes_range_paged(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None, *, limit: int | None = None, after: PropertyRangeCursor | None = None) -> PropertyRangePageResult: ...
    async def neighbors_paged(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, limit: int | None = None, after: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> NeighborPageResult: ...

    # Analytics
    async def personalized_pagerank(self, seed_node_ids: list[int], *, algorithm: str | None = None, damping_factor: float | None = None, max_iterations: int | None = None, epsilon: float | None = None, approx_residual_tolerance: float | None = None, edge_label_filter: list[str] | None = None, max_results: int | None = None) -> PprResult: ...
    async def export_adjacency(self, *, node_label_filter: NodeLabelFilter | None = None, edge_label_filter: list[str] | None = None, include_weights: bool = True) -> AdjacencyExport: ...
    async def connected_components(self, *, edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> dict[int, int]: ...
    async def component_of(self, node_id: int, *, edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> list[int]: ...
    async def vector_search(self, mode: str, k: int, *, dense_query: list[float] | None = None, sparse_query: list[tuple[int, float]] | None = None, label_filter: NodeLabelFilter | None = None, ef_search: int | None = None, scope_start_node_id: int | None = None, scope_max_depth: int | None = None, scope_direction: str | None = None, scope_edge_label_filter: list[str] | None = None, scope_at_epoch: int | None = None, dense_weight: float | None = None, sparse_weight: float | None = None, fusion_mode: str | None = None) -> list[VectorHit]: ...

class AsyncWriteTxn:
    async def upsert_node(self, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> dict[str, Any]: ...
    async def upsert_node_as(self, alias: str, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> dict[str, Any]: ...
    async def add_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    async def remove_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    async def upsert_edge(self, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> dict[str, Any]: ...
    async def upsert_edge_as(self, alias: str, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> dict[str, Any]: ...
    async def delete_node(self, target: dict[str, Any]) -> None: ...
    async def delete_edge(self, target: dict[str, Any]) -> None: ...
    async def invalidate_edge(self, target: dict[str, Any], valid_to: int) -> None: ...
    async def stage(self, operations: list[dict[str, Any]]) -> None: ...
    async def get_node(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    async def get_edge(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    async def get_node_by_key(self, label: str, key: str) -> dict[str, Any] | None: ...
    async def get_edge_by_triple(self, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str) -> dict[str, Any] | None: ...
    async def commit(self) -> TxnCommitResult: ...
    async def rollback(self) -> None: ...
