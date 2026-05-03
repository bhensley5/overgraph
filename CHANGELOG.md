# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.7.0] - 2026-05-02

### Added

#### Native Query Planner
- **Planner-backed node queries.** Added `query_node_ids`, `query_nodes`, and explain APIs across Rust, Node.js, and Python for ID, key, type, property, timestamp, and compound predicate queries.
- **Bounded graph pattern queries.** Added `query_pattern` and `explain_pattern_query` for deterministic bounded pattern matching with node filters and edge-scoped post-filters.
- **Boolean filter model.** Added canonical recursive node filters with `and`, `or`, `not`, `in`, `exists`, `missing`, equality, numeric range, and updated-at range predicates.
- **Durable planner statistics.** Segments may now include optional `planner_stats.dat` sidecars that provide private cost estimates, adaptive candidate caps, and graph-pattern fanout evidence.

### Changed

- **Index-aware planning.** Query execution now chooses among explicit IDs, key lookups, type indexes, declared equality/range property indexes, timestamp indexes, sorted intersections, sorted unions, and bounded fallback scans.
- **Advisory stats model.** Planner statistics improve costing and explain output but never decide correctness; visible-record verification remains the final authority for every result.
- **Connector query parity.** Rust, Node.js, and Python now share the same query, pattern, explain, filter, pagination, and warning semantics, including async connector variants.

### Fixed

- **Planner correctness hardening.** Boolean predicates, graph-pattern node filters, pagination, stale index candidates, tombstones, prune policies, signed-zero float probes, and hash-collision edge cases are verified against latest visible records.
- **Property-index readiness consistency.** Public index listing now reports `Ready` only when new public reads can use the same published ready catalog.
- **Stats refresh robustness.** Targeted secondary-index stats refresh handles missing, corrupt, obsolete, or unavailable sidecars without blocking open or query execution.

## [0.6.0] - 2026-04-25

### Added

#### Shared Handles And Concurrent Reads
- **Cloneable database handles.** `DatabaseEngine` is now a lightweight cloneable handle over shared process-local runtime state, so multiple callers in one process can share one open database family safely.
- **Published read views.** Public reads now capture immutable call-scoped snapshots instead of reading through the live writer state, improving read stability during concurrent writes, flushes, compactions, and close operations.
- **Coordinated lifecycle operations.** Writes, flush adoption, compaction adoption, sync, and close now route through one shared coordinator with close-aware sequencing.

#### Explicit Write Transactions
- **Public write transaction API.** Added explicit `begin_write_txn()` / `beginWriteTxn()` / `begin_write_txn()` transaction handles across Rust, Node.js, and Python.
- **Ordered local staging.** Transactions support local node and edge aliases, ordered staging, bounded point reads, read-own-writes, rollback, and atomic commit.
- **Conflict detection.** Transaction commits use optimistic write-target conflict detection. A conflict prevents the whole commit, with no partial WAL append or partial publication.
- **Async connector parity.** Node.js and Python expose transaction APIs through both sync and async surfaces.

#### Published Degree Fast Path
- **Degree sidecars.** New segment-level `degree_delta.dat` sidecars store signed degree and edge-weight deltas for fast degree-family reads.
- **Published degree overlays.** Active and frozen WAL/memtable contributions are published with each read view as immutable in-memory overlays.
- **Fast degree-family reads.** Eligible `degree`, `degrees`, `sum_edge_weights`, and `avg_edge_weight` calls now sum published overlays plus segment sidecars instead of walking adjacency.

### Changed

- **Write APIs use the shared coordinator.** Existing implicit write APIs keep their public shape while sharing the same internal commit path used by explicit transactions.
- **Degree fallback remains conservative.** Filtered, temporal, prune-policy, temporal-edge, and sidecar-unavailable degree-family reads fall back to the existing adjacency walk path.
- **Segment directories may include optional degree sidecars.** Databases with older segments that lack `degree_delta.dat` can still read through fallback paths where the segment format is otherwise supported. No migration or sidecar backfill is performed.

### Fixed

- **Read and lifecycle race hardening.** Snapshot publication, background flush adoption, background compaction adoption, and close behavior were hardened around concurrent readers and shared handles.
- **Degree correctness across source states.** Degree-family results now preserve parity across active memtables, frozen memtables, flushed segments, compaction, corrupt or missing degree sidecars, reopen, and connector calls.

## [0.5.0] - 2026-04-16

### Added

#### Optional Property Indexes
- **Opt-in property index declarations.** Property indexing is now declaration-backed instead of always-on. Register equality or numeric range indexes only where they pay off with `ensure_node_property_index`, inspect lifecycle/error state with `list_node_property_indexes`, and remove declarations with `drop_node_property_index`.
- **Index-transparent query routing.** Public query APIs stay stable: when a matching property-index declaration is `Ready`, OverGraph uses the declaration-backed path; otherwise it falls back to the same type-scoped public query path.
- **Cross-language parity.** Property-index declaration and inspection APIs are available across Rust, Node.js, and Python, including async connector coverage.

#### Numeric Range Property Indexes
- **Range indexes for numeric properties.** Added optional node-property range indexes for `int`, `uint`, and `float` domains.
- **Range query APIs.** Added range-query and paged range-query APIs across Rust, Node.js, and Python with declaration-aware routing, flush/compaction parity, and restart/recovery coverage.

### Changed

#### Node.js Neighbor Return Shape
- **Neighbor-returning APIs now use plain objects.** In the Node.js connector, `neighbors()`, `neighborsPaged()`, `neighborsBatch()`, and `topKNeighbors()` now return normal `JsNeighborEntry` object arrays rather than wrapper list types, so results can be accessed with standard array/object syntax like `list[i].nodeId`.
- The same plain-object shape now applies to the corresponding async Node.js APIs.

## [0.4.1] - 2026-03-21

### Fixed
- Fixed incomplete Cargo.lock that prevented crates.io publish.

## [0.4.0] - 2026-03-21

### Added

#### Engine Parallelism (Phase 22)
- **Shared parallelism runtime.** CPU-bound read and build paths now use a shared bounded Rayon thread pool. Hybrid vector search, flush index builds, compaction index builds, and multi-segment queries all share the same pool with no thread explosion risk.
- **Parallel dense vector search.** Multi-segment dense candidate collection runs per-segment HNSW searches in parallel with a tightened ordered merge path. Near-linear speedup on multi-segment workloads.
- **Parallel sparse vector search.** Multi-segment sparse scoring runs per-segment posting-list walks in parallel with tightened merge-path allocations.
- **Parallel flush index builds.** Flush-path index generation (adjacency, type, key, property, timestamp, tombstone, and vector indexes) uses staged coarse-task fanout on the shared pool.
- **Parallel compaction index builds.** Compaction-path index generation uses the same staged fanout as flush, maintaining dual-path parity.
- **Parallel HNSW construction.** Dense HNSW index builds use per-node read-write locks for concurrent neighbor-list updates, achieving approximately 7x build speedup on multi-core machines.
- **Approximate forward-push PPR.** New `ApproxForwardPush` algorithm option for Personalized PageRank. Seed-centric forward push that avoids full reachable-graph discovery, much faster for local retrieval workloads. Exact power-iteration remains the default.
- **Parallel exact PPR.** The existing exact power-iteration PPR now runs its per-iteration matrix-vector products in parallel.

### Changed
- **GroupCommit defaults tuned.** Default sync interval changed from 200ms to 50ms, soft trigger from 8MB to 2MB for better latency-throughput balance on typical workloads.
- Identity-hashed self-loop tracking in neighbor queries replaces SipHash, reducing per-edge overhead.
- Merge-path allocations tightened across dense and sparse search for lower memory pressure during multi-segment queries.

### Fixed
- Fixed compaction scheduling gaps that could delay segment merges under certain layouts.
- Fixed inspect CLI crash from a removed internal method.

## [0.3.0] - 2026-03-17

### Changed

#### Storage / Recovery Format
- **Breaking on-disk format change.** OverGraph now uses:
  - **WAL format v3**: each WAL record persists the engine-assigned write sequence (`engine_seq`) alongside the operation payload, ensuring exact `last_write_seq` stability across reopen cycles.
- Databases created with older WAL formats are **not supported** by this release. Opening an older database will fail with a clear version error.
- Users should **recreate the database** or rebuild data into a fresh DB directory when upgrading across this change.
- This release intentionally prioritizes recovery correctness over backward compatibility while OverGraph remains pre-1.0.

#### Async Flush Pipeline
- **Writes no longer block for segment I/O.** The background flush pipeline is now split into three stages: build worker (segment write + fsync), publisher worker (segment open + manifest write + WAL retire), and foreground adoption (cheap in-memory swap). Normal write operations perform zero disk I/O for flush completion.
- Auto-flush threshold now checks only the active memtable size, not total buffered memory. Backpressure (hard cap + immutable count limit) handles total buffer pressure separately.
- Flush result application uses a single manifest write instead of two, reducing per-flush fsync overhead.

### Fixed

#### Crash Recovery
- Fixed a repeated-crash recovery bug in the async flush pipeline. `FrozenPendingFlush` WAL generations are now retained and rebuilt as immutable epochs on reopen instead of being folded into the active memtable and retired too early. This prevents data loss across repeated crash/reopen cycles before a frozen epoch has been durably flushed to a segment.
- WAL replay now preserves the original persisted write sequence metadata (`last_write_seq`) instead of re-deriving it during reopen.

### Added

#### Phase 19 - Vector / Embedding Search
- **Dense vector search (HNSW).** Attach `f32` embedding vectors to nodes via `dense_vector` on `upsert_node` / `batch_upsert_nodes` / `graph_patch`. Per-segment HNSW indexes built at flush time. `vector_search(mode="dense")` with cosine, Euclidean, or dot-product distance. DB-scoped config: `DenseVectorConfig { dimension, metric, hnsw: { m, ef_construction } }`. Configurable `ef_search` per query.
- **Sparse vector search.** Attach sparse vectors (`(dimension_id, weight)` pairs) to nodes. Inverted posting-list indexes per segment. `vector_search(mode="sparse")` with exact dot-product scoring. Works with pre-computed sparse embeddings (SPLADE, BGE-M3, etc.). Sparse vectors canonicalized on write (sorted, deduped, zero-dropped, non-negative).
- **Hybrid search.** `vector_search(mode="hybrid")` combines dense and sparse candidates. Three built-in fusion modes: `weighted-rank-fusion` (default), `reciprocal-rank-fusion`, `weighted-score-fusion`. Configurable `dense_weight` and `sparse_weight`. Degenerates cleanly to one modality when only one query is provided.
- **Graph-scoped search.** `scope` parameter on `vector_search` restricts results to nodes reachable from a start node via traversal. Supports `max_depth`, `direction`, `edge_type_filter`, and `at_epoch`. Uses the same traversal machinery as `traverse()`.
- **Vector compaction.** Dense HNSW indexes rebuilt and sparse posting lists merged during compaction. Vector blob payloads raw-copied for surviving records. Full reopen/recovery parity.
- **Zero-overhead contract.** Databases that never write vectors see no meaningful regression. Vector index files are only created for segments containing vectors. Flush and compaction skip vector index generation entirely when no surviving node has vectors.
- **Node.js bindings.** `vectorSearch()` / `vectorSearchAsync()` with `JsVectorSearchOptions`, `JsVectorSearchScope`, `JsVectorHit`. Dense/sparse vector fields on `upsertNode`, `batchUpsertNodes`, `graphPatch`. `denseVector` config on `open()`.
- **Python bindings.** `vector_search()` (sync + async) with flat kwargs including `scope_*` fields. Dense/sparse vector fields on `upsert_node`, `batch_upsert_nodes`, `graph_patch`. `dense_vector_dimension` / `dense_vector_metric` kwargs on `open()`.

## [0.2.0] - 2026-03-09

### Added

#### Phase 18a - Degree counts and aggregations
- `degree()` - count edges for a node with direction/type/temporal filters
- `sum_edge_weights()` - sum edge weights without materializing neighbor list
- `avg_edge_weight()` - average edge weight (returns `None` if zero edges)
- `degrees()` - batch degree counts with sorted cursor walk for bulk analysis
- Node.js and Python bindings for all degree/weight methods

#### Phase 18b - Shortest path (BFS + Dijkstra)
- `shortest_path()` - find shortest path between two nodes; BFS (unweighted) or bidirectional Dijkstra (weighted)
- `is_connected()` - fast reachability check using bidirectional BFS with no parent tracking
- `all_shortest_paths()` - enumerate all shortest paths with equal cost, capped at `max_paths`
- Supports `weight_field` for automatic algorithm selection: `None` → BFS, `"weight"` → fast Dijkstra, other → hydrated Dijkstra
- Direction control, edge type filtering, temporal filtering (`at_epoch`), `max_depth`, and `max_cost` parameters
- Node.js bindings: `shortestPath()`, `isConnected()`, `allShortestPaths()` (sync + async)
- Python bindings: `shortest_path()`, `is_connected()`, `all_shortest_paths()` (sync + async)
- Criterion benchmarks for BFS and Dijkstra on 10K and 100K node graphs
- Cross-language parity harness entries (S-TRAV-005, S-TRAV-006)

#### Phase 18c - Deterministic traversal
- `traverse()` - breadth-first traversal with depth windows, edge-type filtering, emission-only node-type filtering, and traversal-specific pagination
- Replaces `neighbors_2hop*` family with generic depth-bounded traversal
- Node.js and Python bindings (sync + async)

#### Phase 18d - Connected components (WCC)
- `connected_components()` - global weakly-connected-component labelling via union-find with path compression and union by rank; returns `{node_id → component_id}` map where component_id is the minimum node ID in the component
- `component_of(node_id)` - BFS-based single-component membership query; returns sorted member list
- Edge-type, node-type, and temporal (`at_epoch`) filtering on both methods
- Prune-policy awareness: pruned nodes are invisible to WCC/component_of
- Node.js bindings: `connectedComponents()`, `componentOf()` (sync + async)
- Python bindings: `connected_components()`, `component_of()` (sync + async)
- Note: strongly connected components (SCC) are deferred to Phase 18m

## [0.1.0] - 2026-03-04

Initial release.

### Core Engine
- Log-structured merge tree storage engine, written entirely in Rust with zero C/C++ dependencies
- Write-ahead log with CRC32 integrity checks and crash recovery
- Configurable durability: `Immediate` (fsync per write) or `GroupCommit` (batched fsync, ~20x throughput)
- Immutable segments with memory-mapped reads (no application-level caching)
- Background compaction with metadata sidecars, raw binary copy, and metadata-driven index building
- Atomic manifest updates with rollback safety
- Directory-scoped databases (each DB is a self-contained folder)

### Data Model
- Typed nodes with `(type_id, key)` upsert semantics
- Typed edges with optional `(from, to, type_id)` uniqueness
- Weighted nodes and edges (`f32` weight field)
- Schemaless properties encoded as MessagePack (supports null, bool, int, float, string, bytes, arrays)

### Graph Operations
- Single and batch upsert for nodes and edges
- Packed binary batch format for maximum throughput
- Delete with tombstone-based soft deletion
- Atomic `graph_patch` for multi-operation mutations
- Point lookups by ID, by `(type_id, key)`, and by `(from, to, type_id)` triple
- Bulk reads with sorted merge-walk (not per-item lookups)

### Query and Traversal
- 1-hop and 2-hop neighbor expansion with edge type filters and direction control
- Constrained 2-hop: traverse specific edge types, filter target nodes by type
- Top-K neighbors by weight, recency, or decay-adjusted score
- Property equality search (hash-indexed)
- Type-based node and edge listing with counts
- Time-range queries on a sorted timestamp index
- Subgraph extraction up to N hops deep
- Personalized PageRank from seed nodes
- Graph adjacency export with type filters

### Temporal Features
- Bi-temporal edges with `valid_from` and `valid_to` timestamps
- Point-in-time queries via `at_epoch` parameter
- Edge invalidation (mark as no longer valid without deleting)
- Exponential decay scoring via `decay_lambda` parameter

### Pagination
- Keyset pagination on all collection-returning APIs
- Stable cursors across concurrent writes
- K-way merge with binary-seek cursor for efficient multi-source pagination

### Retention
- Manual `prune()` by age, weight threshold, or node type
- Named prune policies stored in manifest, evaluated at read time (lazy expiration) and compaction time (physical deletion)
- Automatic edge cascade on node pruning

### Indexes
- Outgoing and incoming adjacency indexes with delta-encoded postings
- `(type_id, key)` to node_id key index
- `type_id` to sorted ID list type index
- Property equality hash index
- Sorted timestamp index for time-range queries
- Tombstone index

### Performance
- Node lookups: ~26ns
- Neighbor traversal: ~2μs
- Batch writes: 600K+ nodes/sec
- Sorted cursor walk for batch adjacency operations (PPR, subgraph, export)
- Memtable backpressure (64MB hard cap)
- Segment format v5 with metadata sidecars for fast filtered compaction

### Node.js Connector
- napi-rs bindings with full API parity
- Sync and async variants of every method
- Lazy getters on record types (no deserialization until access)
- Typed arrays (`Float64Array`, `BigInt64Array`) for bulk data
- Packed binary batch protocol for node and edge upserts
- Context manager support

### Python Connector
- PyO3 + maturin bindings with full API parity
- Sync `OverGraph` and async `AsyncOverGraph` classes
- GIL released for all Rust calls via `py.allow_threads()`
- Lazy `.props` deserialization
- `IdArray` lazy sequence wrapper
- Context manager support (`with` / `async with`)
- PEP 561 type stubs (`.pyi`)
- Compaction progress callback with Python exception capture

### CLI
- `overgraph inspect <path>`: show manifest, segment count, node/edge counts, WAL size, prune policies

### CI
- Cross-platform CI: macOS, Linux, Windows
- Benchmark CI with regression detection and cross-language parity validation

[0.7.0]: https://github.com/bhensley5/overgraph/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/bhensley5/overgraph/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/Bhensley5/overgraph/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/Bhensley5/overgraph/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/Bhensley5/overgraph/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Bhensley5/overgraph/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Bhensley5/overgraph/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Bhensley5/overgraph/releases/tag/v0.1.0
