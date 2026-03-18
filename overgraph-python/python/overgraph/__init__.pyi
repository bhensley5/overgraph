"""Type stubs for the overgraph Python connector."""

from typing import Any, Callable

# ============================================================
# Data types
# ============================================================

class PyDbStats:
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

class PyNodeRecord:
    id: int
    type_id: int
    key: str
    props: dict[str, Any]
    weight: float
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class PyEdgeRecord:
    id: int
    from_id: int
    to_id: int
    type_id: int
    props: dict[str, Any]
    weight: float
    valid_from: int
    valid_to: int
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class PyPatchResult:
    node_ids: list[int]
    edge_ids: list[int]
    def __repr__(self) -> str: ...

class PyNeighborEntry:
    node_id: int
    edge_id: int
    edge_type_id: int
    weight: float
    valid_from: int
    valid_to: int
    def __repr__(self) -> str: ...

class PyTraversalHit:
    node_id: int
    depth: int
    via_edge_id: int | None
    score: float | None
    def __repr__(self) -> str: ...

class PyVectorHit:
    node_id: int
    score: float
    def __repr__(self) -> str: ...

class PyTraversalCursor:
    depth: int
    last_node_id: int
    def __init__(self, depth: int, last_node_id: int) -> None: ...
    def __repr__(self) -> str: ...

class PyShortestPath:
    nodes: list[int]
    edges: list[int]
    total_cost: float
    def __repr__(self) -> str: ...

class PySubgraph:
    nodes: list[PyNodeRecord]
    edges: list[PyEdgeRecord]
    def __repr__(self) -> str: ...

class PyPruneResult:
    nodes_pruned: int
    edges_pruned: int
    def __repr__(self) -> str: ...

class PyNamedPrunePolicy:
    name: str
    max_age_ms: int | None
    max_weight: float | None
    type_id: int | None
    def __repr__(self) -> str: ...

class PySegmentInfo:
    id: int
    node_count: int
    edge_count: int
    def __repr__(self) -> str: ...

class PyCompactionStats:
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

class PyCompactionProgress:
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
    def __iter__(self) -> PyIdArrayIter: ...
    def __bool__(self) -> bool: ...
    def __contains__(self, val: int) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...
    def to_list(self) -> list[int]: ...

class PyIdArrayIter:
    def __iter__(self) -> PyIdArrayIter: ...
    def __next__(self) -> int: ...

class PyIdPageResult:
    items: IdArray
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PyNodePageResult:
    items: list[PyNodeRecord]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PyEdgePageResult:
    items: list[PyEdgeRecord]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PyNeighborPageResult:
    items: list[PyNeighborEntry]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PyTraversalPageResult:
    items: list[PyTraversalHit]
    next_cursor: PyTraversalCursor | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PyPprResult:
    node_ids: list[int]
    scores: list[float]
    iterations: int
    converged: bool
    def __repr__(self) -> str: ...

class PyExportEdge:
    from_id: int
    to_id: int
    type_id: int
    weight: float
    def __repr__(self) -> str: ...

class PyAdjacencyExport:
    node_ids: list[int]
    edges: list[PyExportEdge]
    def __repr__(self) -> str: ...

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
    def stats(self) -> PyDbStats: ...

    # Single CRUD
    def upsert_node(
        self,
        type_id: int,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> int: ...
    def upsert_edge(
        self,
        from_id: int,
        to_id: int,
        type_id: int,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> int: ...
    def get_node(self, node_id: int) -> PyNodeRecord | None: ...
    def get_edge(self, edge_id: int) -> PyEdgeRecord | None: ...
    def get_node_by_key(self, type_id: int, key: str) -> PyNodeRecord | None: ...
    def get_edge_by_triple(self, from_id: int, to_id: int, type_id: int) -> PyEdgeRecord | None: ...
    def delete_node(self, node_id: int) -> None: ...
    def delete_edge(self, edge_id: int) -> None: ...
    def invalidate_edge(self, edge_id: int, valid_to: int) -> PyEdgeRecord | None: ...

    # Batch
    def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    def get_nodes(self, node_ids: list[int]) -> list[PyNodeRecord | None]: ...
    def get_nodes_by_keys(self, keys: list[tuple[int, str]]) -> list[PyNodeRecord | None]: ...
    def get_edges(self, edge_ids: list[int]) -> list[PyEdgeRecord | None]: ...
    def graph_patch(self, patch: dict[str, Any]) -> PyPatchResult: ...

    # Queries
    def find_nodes(self, type_id: int, prop_key: str, prop_value: Any) -> IdArray: ...
    def nodes_by_type(self, type_id: int) -> IdArray: ...
    def edges_by_type(self, type_id: int) -> IdArray: ...
    def get_nodes_by_type(self, type_id: int) -> list[PyNodeRecord]: ...
    def get_edges_by_type(self, type_id: int) -> list[PyEdgeRecord]: ...
    def count_nodes_by_type(self, type_id: int) -> int: ...
    def count_edges_by_type(self, type_id: int) -> int: ...
    def find_nodes_by_time_range(self, type_id: int, from_ms: int, to_ms: int) -> IdArray: ...

    # Binary batch
    def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...

    # Traversal
    def neighbors(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]: ...
    def traverse(
        self,
        start: int,
        max_depth: int,
        *,
        min_depth: int = 1,
        direction: str = "outgoing",
        edge_type_filter: list[int] | None = None,
        node_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
        limit: int | None = None,
        cursor: PyTraversalCursor | None = None,
    ) -> PyTraversalPageResult: ...
    def top_k_neighbors(
        self,
        node_id: int,
        k: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        scoring: str = "weight",
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]: ...
    def extract_subgraph(
        self,
        start_node_id: int,
        max_depth: int,
        *,
        direction: str = "outgoing",
        edge_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> PySubgraph: ...
    def neighbors_batch(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> dict[int, list[PyNeighborEntry]]: ...

    # Degree counts + aggregations
    def degree(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> int: ...
    def sum_edge_weights(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> float: ...
    def avg_edge_weight(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> float | None: ...
    def degrees(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]: ...

    # Shortest path
    def shortest_path(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
    ) -> PyShortestPath | None: ...
    def is_connected(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
    ) -> bool: ...
    def all_shortest_paths(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
        max_paths: int | None = None,
    ) -> list[PyShortestPath]: ...

    # Retention
    def prune(
        self,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> PyPruneResult: ...
    def set_prune_policy(
        self,
        name: str,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> None: ...
    def remove_prune_policy(self, name: str) -> bool: ...
    def list_prune_policies(self) -> list[PyNamedPrunePolicy]: ...

    # Maintenance
    def sync(self) -> None: ...
    def flush(self) -> PySegmentInfo | None: ...
    def ingest_mode(self) -> None: ...
    def end_ingest(self) -> PyCompactionStats | None: ...
    def compact(self) -> PyCompactionStats | None: ...
    def compact_with_progress(self, callback: Callable[[PyCompactionProgress], bool]) -> PyCompactionStats | None: ...

    # Pagination
    def nodes_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    def edges_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    def get_nodes_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyNodePageResult: ...
    def get_edges_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyEdgePageResult: ...
    def find_nodes_paged(self, type_id: int, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    def find_nodes_by_time_range_paged(self, type_id: int, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    def neighbors_paged(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> PyNeighborPageResult: ...

    # Analytics
    def personalized_pagerank(
        self,
        seed_node_ids: list[int],
        *,
        damping_factor: float | None = None,
        max_iterations: int | None = None,
        epsilon: float | None = None,
        edge_type_filter: list[int] | None = None,
        max_results: int | None = None,
    ) -> PyPprResult: ...
    def export_adjacency(
        self,
        *,
        node_type_filter: list[int] | None = None,
        edge_type_filter: list[int] | None = None,
        include_weights: bool = True,
    ) -> PyAdjacencyExport: ...

    # Connected Components (Phase 18d)
    def connected_components(
        self,
        *,
        edge_type_filter: list[int] | None = None,
        node_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]:
        """Weakly connected components. Returns {node_id: component_id} where component_id = min node ID in component."""
        ...
    def component_of(
        self,
        node_id: int,
        *,
        edge_type_filter: list[int] | None = None,
        node_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> list[int]:
        """Returns sorted list of node IDs in the same WCC component as node_id. Empty if node missing/hidden."""
        ...

    # Vector search (Phase 19)
    def vector_search(
        self,
        mode: str,
        k: int,
        *,
        dense_query: list[float] | None = None,
        sparse_query: list[tuple[int, float]] | None = None,
        type_filter: list[int] | None = None,
        ef_search: int | None = None,
        scope_start_node_id: int | None = None,
        scope_max_depth: int | None = None,
        scope_direction: str | None = None,
        scope_edge_type_filter: list[int] | None = None,
        scope_at_epoch: int | None = None,
        dense_weight: float | None = None,
        sparse_weight: float | None = None,
        fusion_mode: str | None = None,
    ) -> list[PyVectorHit]: ...

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
    async def stats(self) -> PyDbStats: ...

    # Single CRUD
    async def upsert_node(self, type_id: int, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> int: ...
    async def upsert_edge(self, from_id: int, to_id: int, type_id: int, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> int: ...
    async def get_node(self, node_id: int) -> PyNodeRecord | None: ...
    async def get_edge(self, edge_id: int) -> PyEdgeRecord | None: ...
    async def get_node_by_key(self, type_id: int, key: str) -> PyNodeRecord | None: ...
    async def get_edge_by_triple(self, from_id: int, to_id: int, type_id: int) -> PyEdgeRecord | None: ...
    async def delete_node(self, node_id: int) -> None: ...
    async def delete_edge(self, edge_id: int) -> None: ...
    async def invalidate_edge(self, edge_id: int, valid_to: int) -> PyEdgeRecord | None: ...

    # Batch
    async def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    async def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...
    async def get_nodes(self, node_ids: list[int]) -> list[PyNodeRecord | None]: ...
    async def get_nodes_by_keys(self, keys: list[tuple[int, str]]) -> list[PyNodeRecord | None]: ...
    async def get_edges(self, edge_ids: list[int]) -> list[PyEdgeRecord | None]: ...
    async def graph_patch(self, patch: dict[str, Any]) -> PyPatchResult: ...

    # Queries
    async def find_nodes(self, type_id: int, prop_key: str, prop_value: Any) -> IdArray: ...
    async def nodes_by_type(self, type_id: int) -> IdArray: ...
    async def edges_by_type(self, type_id: int) -> IdArray: ...
    async def get_nodes_by_type(self, type_id: int) -> list[PyNodeRecord]: ...
    async def get_edges_by_type(self, type_id: int) -> list[PyEdgeRecord]: ...
    async def count_nodes_by_type(self, type_id: int) -> int: ...
    async def count_edges_by_type(self, type_id: int) -> int: ...
    async def find_nodes_by_time_range(self, type_id: int, from_ms: int, to_ms: int) -> IdArray: ...

    # Traversal
    async def neighbors(self, node_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, limit: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> list[PyNeighborEntry]: ...
    async def traverse(self, start: int, max_depth: int, *, min_depth: int = 1, direction: str = "outgoing", edge_type_filter: list[int] | None = None, node_type_filter: list[int] | None = None, at_epoch: int | None = None, decay_lambda: float | None = None, limit: int | None = None, cursor: PyTraversalCursor | None = None) -> PyTraversalPageResult: ...
    async def top_k_neighbors(self, node_id: int, k: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, scoring: str = "weight", at_epoch: int | None = None, decay_lambda: float | None = None) -> list[PyNeighborEntry]: ...
    async def extract_subgraph(self, start_node_id: int, max_depth: int, *, direction: str = "outgoing", edge_type_filter: list[int] | None = None, at_epoch: int | None = None) -> PySubgraph: ...
    async def neighbors_batch(self, node_ids: list[int], *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> dict[int, list[PyNeighborEntry]]: ...

    # Degree counts + aggregations
    async def degree(self, node_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None) -> int: ...
    async def sum_edge_weights(self, node_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None) -> float: ...
    async def avg_edge_weight(self, node_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None) -> float | None: ...
    async def degrees(self, node_ids: list[int], *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None) -> dict[int, int]: ...

    # Shortest path
    async def shortest_path(self, from_id: int, to_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None) -> PyShortestPath | None: ...
    async def is_connected(self, from_id: int, to_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, at_epoch: int | None = None, max_depth: int | None = None) -> bool: ...
    async def all_shortest_paths(self, from_id: int, to_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None, max_paths: int | None = None) -> list[PyShortestPath]: ...

    # Retention
    async def prune(self, *, max_age_ms: int | None = None, max_weight: float | None = None, type_id: int | None = None) -> PyPruneResult: ...
    async def set_prune_policy(self, name: str, *, max_age_ms: int | None = None, max_weight: float | None = None, type_id: int | None = None) -> None: ...
    async def remove_prune_policy(self, name: str) -> bool: ...
    async def list_prune_policies(self) -> list[PyNamedPrunePolicy]: ...

    # Maintenance
    async def sync(self) -> None: ...
    async def flush(self) -> PySegmentInfo | None: ...
    async def ingest_mode(self) -> None: ...
    async def end_ingest(self) -> PyCompactionStats | None: ...
    async def compact(self) -> PyCompactionStats | None: ...
    async def compact_with_progress(self, callback: Callable[[PyCompactionProgress], bool]) -> PyCompactionStats | None: ...

    # Pagination
    async def nodes_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    async def edges_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    async def get_nodes_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyNodePageResult: ...
    async def get_edges_by_type_paged(self, type_id: int, *, limit: int | None = None, after: int | None = None) -> PyEdgePageResult: ...
    async def find_nodes_paged(self, type_id: int, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    async def find_nodes_by_time_range_paged(self, type_id: int, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> PyIdPageResult: ...
    async def neighbors_paged(self, node_id: int, *, direction: str = "outgoing", type_filter: list[int] | None = None, limit: int | None = None, after: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> PyNeighborPageResult: ...

    # Analytics
    async def personalized_pagerank(self, seed_node_ids: list[int], *, damping_factor: float | None = None, max_iterations: int | None = None, epsilon: float | None = None, edge_type_filter: list[int] | None = None, max_results: int | None = None) -> PyPprResult: ...
    async def export_adjacency(self, *, node_type_filter: list[int] | None = None, edge_type_filter: list[int] | None = None, include_weights: bool = True) -> PyAdjacencyExport: ...
    async def connected_components(self, *, edge_type_filter: list[int] | None = None, node_type_filter: list[int] | None = None, at_epoch: int | None = None) -> dict[int, int]: ...
    async def component_of(self, node_id: int, *, edge_type_filter: list[int] | None = None, node_type_filter: list[int] | None = None, at_epoch: int | None = None) -> list[int]: ...
    async def vector_search(self, mode: str, k: int, *, dense_query: list[float] | None = None, sparse_query: list[tuple[int, float]] | None = None, type_filter: list[int] | None = None, ef_search: int | None = None, scope_start_node_id: int | None = None, scope_max_depth: int | None = None, scope_direction: str | None = None, scope_edge_type_filter: list[int] | None = None, scope_at_epoch: int | None = None, dense_weight: float | None = None, sparse_weight: float | None = None, fusion_mode: str | None = None) -> list[PyVectorHit]: ...
