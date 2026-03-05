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
    PyNodePageResult,
    PyNodeRecord,
    PyPatchResult,
    PyPprResult,
    PyPruneResult,
    PySegmentInfo,
    PySubgraph,
)


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
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
    ) -> int:
        return await asyncio.to_thread(self._db.upsert_node, type_id, key, props, weight)

    async def upsert_edge(
        self,
        from_id: int,
        to_id: int,
        type_id: int,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> int:
        return await asyncio.to_thread(
            self._db.upsert_edge, from_id, to_id, type_id, props, weight, valid_from, valid_to
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

    async def get_edges(self, edge_ids: list[int]) -> list[PyEdgeRecord | None]:
        return await asyncio.to_thread(self._db.get_edges, edge_ids)

    async def graph_patch(self, patch: dict[str, Any]) -> PyPatchResult:
        return await asyncio.to_thread(self._db.graph_patch, patch)

    # --- Queries ---

    async def find_nodes(self, type_id: int, prop_key: str, prop_value: Any) -> IdArray:
        return await asyncio.to_thread(self._db.find_nodes, type_id, prop_key, prop_value)

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

    # --- Traversal ---

    async def neighbors(
        self,
        node_id: int,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.neighbors, node_id, direction, type_filter, limit, at_epoch, decay_lambda
        )

    async def neighbors_2hop(
        self,
        node_id: int,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.neighbors_2hop, node_id, direction, type_filter, limit, at_epoch, decay_lambda
        )

    async def neighbors_2hop_constrained(
        self,
        node_id: int,
        direction: str = "outgoing",
        traverse_edge_types: list[int] | None = None,
        target_node_types: list[int] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.neighbors_2hop_constrained,
            node_id, direction, traverse_edge_types, target_node_types,
            limit, at_epoch, decay_lambda,
        )

    async def neighbors_batch(
        self,
        node_ids: list[int],
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> dict[int, list[PyNeighborEntry]]:
        return await asyncio.to_thread(
            self._db.neighbors_batch, node_ids, direction, type_filter, at_epoch, decay_lambda
        )

    async def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_nodes_binary, buffer)

    async def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]:
        return await asyncio.to_thread(self._db.batch_upsert_edges_binary, buffer)

    async def top_k_neighbors(
        self,
        node_id: int,
        k: int,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        scoring: str = "weight",
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[PyNeighborEntry]:
        return await asyncio.to_thread(
            self._db.top_k_neighbors,
            node_id, k, direction, type_filter, scoring, at_epoch, decay_lambda,
        )

    async def extract_subgraph(
        self,
        start_node_id: int,
        max_depth: int = 2,
        direction: str = "outgoing",
        edge_type_filter: list[int] | None = None,
        at_epoch: int | None = None,
    ) -> PySubgraph:
        return await asyncio.to_thread(
            self._db.extract_subgraph,
            start_node_id, max_depth, direction, edge_type_filter, at_epoch,
        )

    # --- Retention ---

    async def prune(
        self,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> PyPruneResult:
        return await asyncio.to_thread(self._db.prune, max_age_ms, max_weight, type_id)

    async def set_prune_policy(
        self,
        name: str,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        type_id: int | None = None,
    ) -> None:
        await asyncio.to_thread(self._db.set_prune_policy, name, max_age_ms, max_weight, type_id)

    async def remove_prune_policy(self, name: str) -> bool:
        return await asyncio.to_thread(self._db.remove_prune_policy, name)

    async def list_prune_policies(self) -> list[PyNamedPrunePolicy]:
        return await asyncio.to_thread(self._db.list_prune_policies)

    # --- Maintenance ---

    async def sync(self) -> None:
        await asyncio.to_thread(self._db.sync)

    async def flush(self) -> PySegmentInfo | None:
        return await asyncio.to_thread(self._db.flush)

    async def compact(self) -> PyCompactionStats | None:
        return await asyncio.to_thread(self._db.compact)

    async def compact_with_progress(
        self, callback: Callable[[PyCompactionProgress], bool]
    ) -> PyCompactionStats | None:
        return await asyncio.to_thread(self._db.compact_with_progress, callback)

    # --- Pagination ---

    async def nodes_by_type_paged(
        self, type_id: int, limit: int | None = None, after: int | None = None
    ) -> PyIdPageResult:
        return await asyncio.to_thread(self._db.nodes_by_type_paged, type_id, limit, after)

    async def edges_by_type_paged(
        self, type_id: int, limit: int | None = None, after: int | None = None
    ) -> PyIdPageResult:
        return await asyncio.to_thread(self._db.edges_by_type_paged, type_id, limit, after)

    async def get_nodes_by_type_paged(
        self, type_id: int, limit: int | None = None, after: int | None = None
    ) -> PyNodePageResult:
        return await asyncio.to_thread(self._db.get_nodes_by_type_paged, type_id, limit, after)

    async def get_edges_by_type_paged(
        self, type_id: int, limit: int | None = None, after: int | None = None
    ) -> PyEdgePageResult:
        return await asyncio.to_thread(self._db.get_edges_by_type_paged, type_id, limit, after)

    async def find_nodes_paged(
        self,
        type_id: int,
        prop_key: str,
        prop_value: Any,
        limit: int | None = None,
        after: int | None = None,
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.find_nodes_paged, type_id, prop_key, prop_value, limit, after
        )

    async def find_nodes_by_time_range_paged(
        self,
        type_id: int,
        from_ms: int,
        to_ms: int,
        limit: int | None = None,
        after: int | None = None,
    ) -> PyIdPageResult:
        return await asyncio.to_thread(
            self._db.find_nodes_by_time_range_paged, type_id, from_ms, to_ms, limit, after
        )

    async def neighbors_paged(
        self,
        node_id: int,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> PyNeighborPageResult:
        return await asyncio.to_thread(
            self._db.neighbors_paged,
            node_id, direction, type_filter, limit, after, at_epoch, decay_lambda,
        )

    async def neighbors_2hop_paged(
        self,
        node_id: int,
        direction: str = "outgoing",
        type_filter: list[int] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> PyNeighborPageResult:
        return await asyncio.to_thread(
            self._db.neighbors_2hop_paged,
            node_id, direction, type_filter, limit, after, at_epoch, decay_lambda,
        )

    async def neighbors_2hop_constrained_paged(
        self,
        node_id: int,
        direction: str = "outgoing",
        traverse_edge_types: list[int] | None = None,
        target_node_types: list[int] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> PyNeighborPageResult:
        return await asyncio.to_thread(
            self._db.neighbors_2hop_constrained_paged,
            node_id, direction, traverse_edge_types, target_node_types,
            limit, after, at_epoch, decay_lambda,
        )

    # --- Analytics ---

    async def personalized_pagerank(
        self,
        seed_node_ids: list[int],
        damping_factor: float | None = None,
        max_iterations: int | None = None,
        epsilon: float | None = None,
        edge_type_filter: list[int] | None = None,
        max_results: int | None = None,
    ) -> PyPprResult:
        return await asyncio.to_thread(
            self._db.personalized_pagerank,
            seed_node_ids, damping_factor, max_iterations, epsilon,
            edge_type_filter, max_results,
        )

    async def export_adjacency(
        self,
        node_type_filter: list[int] | None = None,
        edge_type_filter: list[int] | None = None,
        include_weights: bool = True,
    ) -> PyAdjacencyExport:
        return await asyncio.to_thread(
            self._db.export_adjacency, node_type_filter, edge_type_filter, include_weights
        )
