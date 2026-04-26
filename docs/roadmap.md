# Roadmap

Where OverGraph is headed and what's already shipped.

This roadmap reflects real priorities, not promises. Items move between buckets as we learn from benchmarks, user feedback, and implementation experience. If something here matters to you, [open an issue](https://github.com/Bhensley5/overgraph/issues) and tell us how you'd use it.

---

## Now

Active development. These are the things we're building right now.

### Native query planner

OverGraph has always been API-first: you call functions, not write query strings. The query planner keeps that philosophy but adds a planning layer underneath. You describe what you want with a builder pattern, and the planner figures out the fastest way to get it.

- **Query IR and builder API.** A composable query model for node predicates and small graph patterns. Aliases, compound predicates, hydration modes, deterministic ordering, limits. Works across Rust, Node.js, and Python.
- **Index-backed candidate sources.** The planner automatically selects from node ID/key lookups, type indexes, equality and range property indexes, timestamp filters, and adjacency sources. It picks the most selective anchor, intersects multiple sources, and falls back gracefully when no index covers the query.
- **Compound predicate planning.** Multi-predicate queries that intersect index results instead of scanning. The planner chooses the cheapest path and verifies remaining predicates after the index narrows the candidates. Full scans require explicit opt-in.
- **Graph pattern matching.** Describe a small subgraph pattern (linear chains, branching shapes, repeated-variable equality) and the planner finds matches using the same index infrastructure. Built on the traversal substrate, not a separate engine.
- **Explain output.** Every query produces an inspectable plan so you can see exactly which indexes were used, what was filtered, and where time went.

---

## Next

Queued up behind the current work. These build directly on the query planner.

### Compound and composite indexes
Declare a single index over ordered property tuples like `(type_id, status, score)` or `(type_id, tenant_id, updated_at)` with prefix semantics. The planner picks them up automatically.

### Boolean predicate planning
Extend queries beyond AND-only to support `OR`, `NOT`, `IN`, `exists`, and `missing` with index-union/intersection support.

### Query projection modes
Return exactly what you need: selected properties, selected metadata fields, or compact payloads. Avoid full record hydration when you only need IDs or a few fields.

### Edge queries and edge indexes
Promote edge predicates from post-filters to planned query sources. Query by edge type + property, endpoint + edge property, temporal windows, or weight ranges directly.

### Graph pattern queries v2
Pagination and cursors for pattern results. Variable-length path patterns. Optional pattern pieces. Better branch-order costing.

### GQL / Cypher-style query syntax
A read-only query-string surface that compiles into the native query IR and planner. You get the convenience of a familiar syntax without giving up the API-first foundation underneath. This is an adapter, not a replacement.

### Planner statistics and selectivity model
Durable stats for per-type counts, property cardinality, value distribution, and adjacency fanout so the planner can make evidence-based decisions instead of heuristic guesses.

### Graph-algorithm-scoped vector search
Generalize vector search scoping beyond traversal start points. Run dense, sparse, or hybrid search over query results, pattern matches, PPR neighborhoods, community memberships, or explicit node sets.

---

## Exploring

Research-phase items. We're investigating feasibility, design tradeoffs, and whether there's real demand. These may change shape significantly before they land.

### RDF import/export
Define a canonical mapping between OverGraph's property-graph model and RDF terms so you can move existing graph data in and out. Import and export for common RDF syntaxes (Turtle, N-Triples, JSON-LD). This is the foundation for everything else in the standards track.

### SPARQL read support
A read-only SPARQL query layer over a mapped RDF view of the graph. `SELECT`, `ASK`, and `CONSTRUCT` queries that compile down to OverGraph's native planner. Built on top of the RDF mapping, not a separate triplestore.

### OWL reasoning
Ontology support above the RDF/SPARQL layer. Starting with an implementable OWL 2 profile (likely RL) and deciding between materialized inference at import time vs. derived inference at query time. This is genuinely ambitious and won't happen until SPARQL proves demand.

### Migration tooling
Tools for bringing existing graph data into OverGraph from other databases and formats. The RDF layer covers semantic-web sources, but we're also looking at property-graph interchange formats and direct migration paths from other embedded graph stores.

---

## On Deck

Planned features that are waiting for the right time. These are designed and scoped but not actively being built.

### Graph projections and analytics substrate
CSR-format snapshots of the live graph for algorithm workloads. In-memory or mmap-backed, with explicit revision and timestamp metadata. The foundation for projection-backed algorithms.

### Community detection
Louvain modularity optimization with multi-level hierarchy and resolution parameter. Runs on graph projections, not the live engine.

### Centrality metrics
Betweenness centrality and harmonic closeness over graph projections. Reuses projections instead of rebuilding graph state inside each algorithm.

### Triangle counting and clustering coefficients
Sorted adjacency intersection for per-node and global triangle counts. Clustering coefficients for identifying tightly connected neighborhoods.

### Multi-label nodes
Upgrade from a single `type_id` to `type_ids` with any/all matching. Format version bump with migration.

### Schema and constraints
Optional property validation, required fields, edge endpoint constraints, and uniqueness enforcement. Manifest-stored, fully opt-in. Schemas are never required.

### Strongly connected components
Tarjan or Kosaraju SCC on the live directed graph, complementing the existing weakly connected components.

### Engine parallelism
Multi-segment parallel reads for batch adjacency, traversal, and graph-algorithm workloads where benchmarks prove the overhead is worth it.

---

## Recently shipped

A selection of what's already in the engine today. Full changelog in the [releases](https://github.com/Bhensley5/overgraph/releases).

### v0.6.0

- **Shared-handle concurrency.** Cloneable database handles with published read views for stable concurrent reads during writes, flushes, compactions, and close.
- **Explicit write transactions.** Stage ordered node and edge mutations locally, read your own staged writes, commit atomically with optimistic conflict detection. Available in Rust, Node.js, and Python.
- **Published degree cache.** Pre-computed degree overlays and segment-level degree delta sidecars for fast unfiltered degree, sum-weight, and average-weight queries.

### v0.5.0

- **Property indexes.** Optional equality and numeric range indexes on node properties. Declare them when you want, query without them when you don't. The public query API stays index-transparent and falls back to scan-based paths until declarations are ready.

### v0.4.0

- **Engine parallelism.** Shared bounded Rayon thread pool for parallel per-segment vector search, parallel flush/compaction index generation, and parallel HNSW construction (~7x build speedup on multi-core).
- **Parallel HNSW rebuild.** Per-node RwLock-based concurrent HNSW index construction during compaction.
- **Approximate PPR.** Forward-push Personalized PageRank for fast seed-centric retrieval workloads, with parallel exact PPR on the shared pool.
- **API reference.** Comprehensive cross-language documentation for every method, parameter, type, and return value.

### v0.3.0

- **Dense vector search (HNSW).** Cosine, dot product, and Euclidean distance. Per-segment HNSW indexes built at flush time with configurable dimension, m, ef_construction, and ef_search.
- **Sparse vector search.** Inverted posting lists for SPLADE, BGE-M3, and other learned sparse representations. Exact dot-product scoring.
- **Hybrid fusion.** Reciprocal rank fusion, weighted rank fusion, and weighted score fusion across dense and sparse results in a single query.
- **Traversal-scoped vector search.** Restrict search to graph neighborhoods ("find similar nodes within 2 hops of X").
- **Async flush pipeline.** Writers never block for segment I/O. Frozen memtables flush in a background publisher thread with zero foreground disk latency. WAL v3 format with persisted engine sequence numbers.

### v0.2.0

- **Graph algorithms.** Degree counts, shortest path (BFS and bidirectional Dijkstra), reachability checks, breadth-first traversal with depth windows, weakly connected components, and single-component membership queries.
- **Modular engine architecture.** Refactored into read/write/graph_ops split with callback-based adjacency iteration for zero-allocation aggregations.
- **Python connector.** Full PyO3 bindings with sync and async APIs, type stubs, and pip-installable wheels.
- **Node.js connector.** napi-rs bindings with sync and async methods, TypeScript types, and npm-installable packages.

### Earlier

Core storage engine (WAL, memtable, segments, compaction, tombstones), secondary indexes, cursor pagination, production hardening, 1.29M+ node/sec batch writes, 34ns point lookups, sub-microsecond neighbor traversal.
