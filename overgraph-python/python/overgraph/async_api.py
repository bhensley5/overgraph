"""Async wrapper for OverGraph using asyncio.to_thread()."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from .overgraph import (
    OverGraph,
    IdArray,
    PyAdjacencyExport,
    PyCompactionProgress,
    PyCompactionStats,
    PyDbStats,
    PyEdgePageResult,
    PyEdgeRecord,
    PyIdPageResult,
    PyNamedPrunePolicy,
    PyNeighborEntry,
    PyNeighborPageResult,
    PyNodePropertyIndexInfo,
    PyNodePageResult,
    PyNodeRecord,
    PyPatchResult,
    PyPprResult,
    PyPropertyRangeBound,
    PyPropertyRangeCursor,
    PyPropertyRangePageResult,
    PyPruneResult,
    PySegmentInfo,
    PyShortestPath,
    PySubgraph,
    PyTraversalCursor,
    PyTraversalHit,
    PyTraversalPageResult,
    PyTxnCommitResult,
    PyVectorHit,
    PyWriteTxn,
)


class AsyncWriteTxn:
    """Async wrapper around a stateful write transaction.

    Operations are serialized with an asyncio lock so staged writes, reads,
    commit, and rollback preserve caller await order on this transaction.
    """

    __slots__ = ("_txn", "_lock")

    def __init__(self, txn: PyWriteTxn) -> None:
        self._txn = txn
        self._lock = asyncio.Lock()

    def __repr__(self) -> str:
        return f"AsyncWriteTxn(txn={self._txn!r})"

    async def upsert_node(
        self,
        type_id: int,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._txn.upsert_node,
                type_id,
                key,
                props=props,
                weight=weight,
                dense_vector=dense_vector,
                sparse_vector=sparse_vector,
            )

    async def upsert_node_as(
        self,
        alias: str,
        type_id: int,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._txn.upsert_node_as,
                alias,
                type_id,
                key,
                props=props,
                weight=weight,
                dense_vector=dense_vector,
                sparse_vector=sparse_vector,
            )

    async def upsert_edge(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        type_id: int,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._txn.upsert_edge,
                from_ref,
                to_ref,
                type_id,
                props=props,
                weight=weight,
                valid_from=valid_from,
                valid_to=valid_to,
            )

    async def upsert_edge_as(
        self,
        alias: str,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        type_id: int,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._txn.upsert_edge_as,
                alias,
                from_ref,
                to_ref,
                type_id,
                props=props,
                weight=weight,
                valid_from=valid_from,
                valid_to=valid_to,
            )

    async def delete_node(self, target: dict[str, Any]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._txn.delete_node, target)

    async def delete_edge(self, target: dict[str, Any]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._txn.delete_edge, target)

    async def invalidate_edge(self, target: dict[str, Any], valid_to: int) -> None:
        async with self._lock:
            await asyncio.to_thread(self._txn.invalidate_edge, target, valid_to)

    async def stage(self, operations: list[dict[str, Any]]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._txn.stage, operations)

    async def get_node(self, target: dict[str, Any]) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(self._txn.get_node, target)

    async def get_edge(self, target: dict[str, Any]) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(self._txn.get_edge, target)

    async def get_node_by_key(self, type_id: int, key: str) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(self._txn.get_node_by_key, type_id, key)

    async def get_edge_by_triple(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        type_id: int,
    ) -> dict[str, Any] | None:
        async with self._lock:
            return await asyncio.to_thread(
                self._txn.get_edge_by_triple, from_ref, to_ref, type_id
            )

    async def commit(self) -> PyTxnCommitResult:
        async with self._lock:
            return await asyncio.to_thread(self._txn.commit)

    async def rollback(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._txn.rollback)


class AsyncOverGraph:
    """Async wrapper around OverGraph. All methods release the GIL via to_thread()."""

    __slots__ = ("_db",)

    def __init__(self, db: OverGraph) -> None:
        self._db = db

    def __repr__(self) -> str:
        return f"AsyncOverGraph(db={self._db!r})"

    @staticmethod
    async def open(path: str, **kwargs: Any) -> AsyncOverGraph:
        db = await asyncio.to_thread(OverGraph.open, path, **kwargs)
        return AsyncOverGraph(db)

    async def close(self, force: bool = False) -> None:
        await asyncio.to_thread(self._db.close, force)

    async def __aenter__(self) -> AsyncOverGraph:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def stats(self) -> PyDbStats:
        return await asyncio.to_thread(self._db.stats)

    # --- Single CRUD ---

    async def upsert_node(
        self,
        type_id: int,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> int:
        return await asyncio.to_thread(
            self._db.upsert_node, type_id, key,
            props=props, weight=weight,
            dense_vector=dense_vector, sparse_vector=sparse_vector,
        )

    async def upsert_edge(
        self,
        from_id: int,
        to_id: int,
        type_id: int,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> int:
        return await asyncio.to_thread(
            self._db.upsert_edge, from_id, to_id, type_id,
            props=props, weight=weight, valid_from=valid_from, valid_to=valid_to,
        )

    async def get_node(self, node_id: int) -> PyNodeRecord | None:
        return await asyncio.to_thread(self._db.get_node, node_id)

    async def get_edge(self, edge_id: int) -> PyEdgeRecord | None:
        return await asyncio.to_thread(self._db.get_edge, edge_id)

    async def get_node_by_key(self, type_id: int, key: str) -> PyNodeRecord | None:
        return await asyncio.to_thread(self._db.get_node_by_key, type_id, key)

    async def get_edge_by_triple(self, from_id: int, to_id: int, type_id: int) -> PyEdgeRecord | None:
        return await asyncio.to_thread(self._db.get_edge_by_triple, from_id, to_id, type_id)

    async def delete_node(self, node_id: int) -> None:
        await asyncio.to_thread(self._db.delete_node, node_id)

    async def delete_edge(self, edge_id: int) -> None:
        await asyncio.to_thread(self._db.delete_edge, edge_id)

    async def invalidate_edge(self, edge_id: int, valid_to: int) -> PyEdgeRecord | None:
        return await asyncio.to_thread(self._db.invalidate_edge, edge_id, valid_to)

    # --- Batch ---

    async def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_nodes, nodes)

    async def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_edges, edges)

    async def get_nodes(self, node_ids: list[int]) -> list[PyNodeRecord | None]:
        return await asyncio.to_thread(self._db.get_nodes, node_ids)

    async def get_nodes_by_keys(self, keys: list[tuple[int, str]]) -> list[PyNodeRecord | None]:
        return await asyncio.to_thread(self._db.get_nodes_by_keys, keys)

    async def get_edges(self, edge_ids: list[int]) -> list[PyEdgeRecord | None]:
        return await asyncio.to_thread(self._db.get_edges, edge_ids)

    async def graph_patch(self, patch: dict[str, Any]) -> PyPatchResult:
        return await asyncio.to_thread(self._db.graph_patch, patch)

    async def begin_write_txn(self) -> AsyncWriteTxn:
        txn = await asyncio.to_thread(self._db.begin_write_txn)
        return AsyncWriteTxn(txn)

    # --- Queries ---

    async def find_nodes(self, type_id: int, prop_key: str, prop_value: Any) -> IdArray:
        return await asyncio.to_thread(self._db.find_nodes, type_id, prop_key, prop_value)

    async def query_node_ids(self, request: Any) -> PyIdPageResult:
        return await asyncio.to_thread(self._db.query_node_ids, request)

    async def query_nodes(self, request: Any) -> PyNodePageResult:
        return await asyncio.to_thread(self._db.query_nodes, request)

    async def query_pattern(self, request: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._db.query_pattern, request)

    async def explain_node_query(self, request: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._db.explain_node_query, request)

    async def explain_pattern_query(self, request: Any) -> dict[str, Any]:
        return await asyncio.to_thread(self._db.explain_pattern_query, request)

    async def ensure_node_property_index(
        self,
        type_id: int,
        prop_key: str,
        kind: str,
        *,
        domain: str | None = None,
    ) -> PyNodePropertyIndexInfo:
        return await asyncio.to_thread(
            self._db.ensure_node_property_index,
            type_id,
            prop_key,
            kind,
            domain=domain,
        )

    async def drop_node_property_index(
        self,
        type_id: int,
        prop_key: str,
        kind: str,
        *,
        domain: str | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            self._db.drop_node_property_index,
            type_id,
            prop_key,
            kind,
            domain=domain,
        )

    async def list_node_property_indexes(self) -> list[PyNodePropertyIndexInfo]:
        return await asyncio.to_thread(self._db.list_node_property_indexes)

    async def nodes_by_type(self, type_id: int) -> IdArray:
        return await asyncio.to_thread(self._db.nodes_by_type, type_id)

    async def edges_by_type(self, type_id: int) -> IdArray:
        return await asyncio.to_thread(self._db.edges_by_type, type_id)

    async def get_nodes_by_type(self, type_id: int) -> list[PyNodeRecord]:
        return await asyncio.to_thread(self._db.get_nodes_by_type, type_id)

    async def get_edges_by_type(self, type_id: int) -> list[PyEdgeRecord]:
        return await asyncio.to_thread(self._db.get_edges_by_type, type_id)

    async def count_nodes_by_type(self, type_id: int) -> int:
        return await asyncio.to_thread(self._db.count_nodes_by_type, type_id)

    async def count_edges_by_type(self, type_id: int) -> int:
        return await asyncio.to_thread(self._db.count_edges_by_type, type_id)

    async def find_nodes_by_time_range(
        self, type_id: int, from_ms: int, to_ms: int
    ) -> IdArray:
        return await asyncio.to_thread(
            self._db.find_nodes_by_time_range, type_id, from_ms, to_ms
        )

    async def find_nodes_range(
        self,
        type_id: int,
        prop_key: str,
        lower: PyPropertyRangeBound | None = None,
        upper: PyPropertyRangeBound | None = None,
    ) -> IdArray:
        return await asyncio.to_thread(
            self._db.find_nodes_range, type_id, prop_key, lower, upper
        )

    # --- Traversal ---

    async def neighbors(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.neighbors, node_id,
            direction=direction, type_filter=type_filter, limit=limit,
            at_epoch=at_epoch, decay_lambda=decay_lambda,
        )

    async def traverse(
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
    ) -> PyTraversalPageResult:
        return await asyncio.to_thread(
            self._db.traverse,
            start,
            max_depth,
            min_depth=min_depth,
            direction=direction,
            edge_type_filter=edge_type_filter,
            node_type_filter=node_type_filter,
            at_epoch=at_epoch,
            decay_lambda=decay_lambda,
            limit=limit,
            cursor=cursor,
        )

    async def neighbors_batch(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> dict[int, list[PyNeighborEntry]]:
        return await asyncio.to_thread(
            self._db.neighbors_batch, node_ids,
            direction=direction, type_filter=type_filter,
            at_epoch=at_epoch, decay_lambda=decay_lambda,
        )

    async def degree(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> int:
        return await asyncio.to_thread(
            self._db.degree, node_id,
            direction=direction, type_filter=type_filter, at_epoch=at_epoch,
        )

    async def sum_edge_weights(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> float:
        return await asyncio.to_thread(
            self._db.sum_edge_weights, node_id,
            direction=direction, type_filter=type_filter, at_epoch=at_epoch,
        )

    async def avg_edge_weight(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> float | None:
        return await asyncio.to_thread(
            self._db.avg_edge_weight, node_id,
            direction=direction, type_filter=type_filter, at_epoch=at_epoch,
        )

    async def degrees(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]:
        return await asyncio.to_thread(
            self._db.degrees, node_ids,
            direction=direction, type_filter=type_filter, at_epoch=at_epoch,
        )

    async def shortest_path(
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
    ) -> PyShortestPath | None:
        return await asyncio.to_thread(
            self._db.shortest_path, from_id, to_id,
            direction=direction, type_filter=type_filter, weight_field=weight_field,
            at_epoch=at_epoch, max_depth=max_depth, max_cost=max_cost,
        )

    async def is_connected(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            self._db.is_connected, from_id, to_id,
            direction=direction, type_filter=type_filter,
            at_epoch=at_epoch, max_depth=max_depth,
        )

    async def all_shortest_paths(
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
    ) -> list[PyShortestPath]:
        return await asyncio.to_thread(
            self._db.all_shortest_paths, from_id, to_id,
            direction=direction, type_filter=type_filter, weight_field=weight_field,
            at_epoch=at_epoch, max_depth=max_depth, max_cost=max_cost, max_paths=max_paths,
        )

    async def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_nodes_binary, buffer)

    async def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_edges_binary, buffer)

    async def top_k_neighbors(
        self,
        node_id: int,
        k: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        scoring: str = "weight",
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.top_k_neighbors, node_id, k,
            direction=direction, type_filter=type_filter, scoring=scoring,
            at_epoch=at_epoch, decay_lambda=decay_lambda,
        )

    async def extract_subgraph(
        self,
        start_node_id: int,
        max_depth: int,
        *,
        direction: str = "outgoing",
        edge_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> PySubgraph:
        return await asyncio.to_thread(
            self._db.extract_subgraph,
            start_node_id, max_depth,
            direction=direction, edge_type_filter=edge_type_filter, at_epoch=at_epoch,
        )

    # --- Retention ---

    async def prune(
        self,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> PyPruneResult:
        return await asyncio.to_thread(
            self._db.prune,
            max_age_ms=max_age_ms, max_weight=max_weight, type_id=type_id,
        )

    async def set_prune_policy(
        self,
        name: str,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._db.set_prune_policy, name,
            max_age_ms=max_age_ms, max_weight=max_weight, type_id=type_id,
        )

    async def remove_prune_policy(self, name: str) -> bool:
        return await asyncio.to_thread(self._db.remove_prune_policy, name)

    async def list_prune_policies(self) -> list[PyNamedPrunePolicy]:
        return await asyncio.to_thread(self._db.list_prune_policies)

    # --- Maintenance ---

    async def sync(self) -> None:
        await asyncio.to_thread(self._db.sync)

    async def flush(self) -> PySegmentInfo | None:
        return await asyncio.to_thread(self._db.flush)

    async def ingest_mode(self) -> None:
        return await asyncio.to_thread(self._db.ingest_mode)

    async def end_ingest(self) -> PyCompactionStats | None:
        return await asyncio.to_thread(self._db.end_ingest)

    async def compact(self) -> PyCompactionStats | None:
        return await asyncio.to_thread(self._db.compact)

    async def compact_with_progress(
        self, callback: Callable[[PyCompactionProgress], bool]
    ) -> PyCompactionStats | None:
        return await asyncio.to_thread(self._db.compact_with_progress, callback)

    # --- Pagination ---

    async def nodes_by_type_paged(
        self, type_id: int, *, limit: int | None = None, after: int | None = None
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.nodes_by_type_paged, type_id, limit=limit, after=after,
        )

    async def edges_by_type_paged(
        self, type_id: int, *, limit: int | None = None, after: int | None = None
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.edges_by_type_paged, type_id, limit=limit, after=after,
        )

    async def get_nodes_by_type_paged(
        self, type_id: int, *, limit: int | None = None, after: int | None = None
    ) -> PyNodePageResult:
        return await asyncio.to_thread(
            self._db.get_nodes_by_type_paged, type_id, limit=limit, after=after,
        )

    async def get_edges_by_type_paged(
        self, type_id: int, *, limit: int | None = None, after: int | None = None
    ) -> PyEdgePageResult:
        return await asyncio.to_thread(
            self._db.get_edges_by_type_paged, type_id, limit=limit, after=after,
        )

    async def find_nodes_paged(
        self,
        type_id: int,
        prop_key: str,
        prop_value: Any,
        *,
        limit: int | None = None,
        after: int | None = None,
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.find_nodes_paged, type_id, prop_key, prop_value,
            limit=limit, after=after,
        )

    async def find_nodes_by_time_range_paged(
        self,
        type_id: int,
        from_ms: int,
        to_ms: int,
        *,
        limit: int | None = None,
        after: int | None = None,
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.find_nodes_by_time_range_paged, type_id, from_ms, to_ms,
            limit=limit, after=after,
        )

    async def find_nodes_range_paged(
        self,
        type_id: int,
        prop_key: str,
        lower: PyPropertyRangeBound | None = None,
        upper: PyPropertyRangeBound | None = None,
        *,
        limit: int | None = None,
        after: PyPropertyRangeCursor | None = None,
    ) -> PyPropertyRangePageResult:
        return await asyncio.to_thread(
            self._db.find_nodes_range_paged,
            type_id,
            prop_key,
            lower,
            upper,
            limit=limit,
            after=after,
        )

    async def neighbors_paged(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> PyNeighborPageResult:
        return await asyncio.to_thread(
            self._db.neighbors_paged, node_id,
            direction=direction, type_filter=type_filter, limit=limit,
            after=after, at_epoch=at_epoch, decay_lambda=decay_lambda,
        )

    # --- Analytics ---

    async def personalized_pagerank(
        self,
        seed_node_ids: list[int],
        *,
        algorithm: str | None = None,
        damping_factor: float | None = None,
        max_iterations: int | None = None,
        epsilon: float | None = None,
        approx_residual_tolerance: float | None = None,
        edge_type_filter: list[int] | None = None,
        max_results: int | None = None,
    ) -> PyPprResult:
        return await asyncio.to_thread(
            self._db.personalized_pagerank, seed_node_ids,
            algorithm=algorithm,
            damping_factor=damping_factor, max_iterations=max_iterations,
            epsilon=epsilon, approx_residual_tolerance=approx_residual_tolerance,
            edge_type_filter=edge_type_filter, max_results=max_results,
        )

    async def export_adjacency(
        self,
        *,
        node_type_filter: list[int] | None = None,
        edge_type_filter: list[int] | None = None,
        include_weights: bool = True,
    ) -> PyAdjacencyExport:
        return await asyncio.to_thread(
            self._db.export_adjacency,
            node_type_filter=node_type_filter, edge_type_filter=edge_type_filter,
            include_weights=include_weights,
        )

    async def connected_components(
        self,
        *,
        edge_type_filter: list[int] | None = None,
        node_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]:
        return await asyncio.to_thread(
            self._db.connected_components,
            edge_type_filter=edge_type_filter, node_type_filter=node_type_filter,
            at_epoch=at_epoch,
        )

    async def component_of(
        self,
        node_id: int,
        *,
        edge_type_filter: list[int] | None = None,
        node_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> list[int]:
        return await asyncio.to_thread(
            self._db.component_of, node_id,
            edge_type_filter=edge_type_filter, node_type_filter=node_type_filter,
            at_epoch=at_epoch,
        )

    # --- Vector search (Phase 19) ---

    async def vector_search(
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
    ) -> list[PyVectorHit]:
        return await asyncio.to_thread(
            self._db.vector_search,
            mode, k,
            dense_query=dense_query, sparse_query=sparse_query,
            type_filter=type_filter, ef_search=ef_search,
            scope_start_node_id=scope_start_node_id, scope_max_depth=scope_max_depth,
            scope_direction=scope_direction, scope_edge_type_filter=scope_edge_type_filter,
            scope_at_epoch=scope_at_epoch,
            dense_weight=dense_weight, sparse_weight=sparse_weight,
            fusion_mode=fusion_mode,
        )
