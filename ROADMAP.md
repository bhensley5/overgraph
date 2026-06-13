# Roadmap

Where OverGraph is headed, what is already real, and what is still being explored.

This is a living roadmap, not a promise list. Priorities move as benchmarks, real usage, and implementation work teach us more. The changelog is still the source of truth for exact release notes. This file keeps the public direction easy to scan.

Last updated: 2026-05-25

## Product Direction

OverGraph is an embedded graph database. The main bet is that a fast in-process graph engine should feel like a library, not a service you have to operate.

Built-in vector search is a core part of that bet, not an add-on bolted to the side. Embeddings live on graph nodes, vector indexes are built by the same segment pipeline as adjacency and label indexes, and searches can stay inside graph neighborhoods. The product direction is graph structure and semantic similarity in one local engine.

- Keep the core pure Rust, with no C or C++ storage dependencies.
- Keep databases directory-scoped so they are easy to copy, back up, inspect, and rebuild.
- Keep Rust, Node.js, and Python APIs in parity.
- Give users a real choice between programmatic APIs and GQL. Native Rust, Node.js, and Python APIs are first-class for code-built workflows. GQL is a first-class query-string surface for declarative graph reads and writes.
- Keep both API styles on the same execution substrates. GQL should compile into the same native planners, indexes, transactions, caps, and explain machinery instead of becoming a second engine.
- Treat vector search as graph-native: dense, sparse, hybrid, and scoped by graph structure.
- Keep hot reads mmap-first and allocation-aware.
- Make correctness inspectable through explain output, scrub diagnostics, and clear fallback behavior.

## Available Today

These are already part of the public engine.

### Embedded Storage

- Log-structured storage with WAL, memtables, immutable mmap segments, background flush, and background compaction.
- Packed `segment.core` storage in the current v10 segment format.
- Component identity, dependency digests, build fingerprints, and required versus optional component validation.
- Directory-scoped databases with crash recovery and atomic manifest updates.
- MessagePack property encoding.

### Graph Model

- Named node labels and edge labels across Rust, Node.js, and Python.
- Multi-label nodes with deterministic label membership indexes.
- Single-label edges with weighted and temporal relationship support.
- Catalog diagnostics for labels.
- Explicit write transactions with read-your-own-writes and atomic commit.

### Query And Planning

- Planner-backed node queries with boolean filters, property predicates, updated-at filters, key/id anchors, pagination, and explain output.
- Bounded graph pattern queries with deterministic bindings.
- Direct edge query APIs with label, endpoint, metadata, property, weight, and temporal predicates.
- Optional node and edge property indexes for equality and numeric range lookups.
- Durable planner statistics that improve costing without deciding correctness.

### Graph Operations And Algorithms

- Immediate, paged, and batched neighbor reads.
- Top-k neighbors by weight, recency, or decay-adjusted score.
- Deterministic bounded traversal and subgraph extraction.
- Shortest path, reachability, and all-shortest-path variants.
- Degree counts, batch degree reads, sum weights, and average edge weights.
- Weakly connected components and single-component membership.
- Personalized PageRank for retrieval-style workloads.
- Adjacency export for analytics and interoperability.
- Manual prune and named prune policies for retention.

### Graph-Native Vector Search

- Dense vector search with HNSW.
- Sparse vector search with posting lists.
- Hybrid fusion across dense and sparse results.
- Traversal-scoped vector search, so similarity search can stay inside graph neighborhoods.
- Vector payloads stored directly on nodes, with segment indexes maintained by the same flush and compaction lifecycle as the graph.

### Connectors

- Rust core API.
- Node.js connector built with napi-rs, including TypeScript declarations and async APIs.
- Python connector built with PyO3 and maturin, including type stubs and async APIs.

## Now

Active work expanding the GQL surface, tightening numeric property semantics, and improving projection and late hydration.

### GQL Reads And Mutations

GQL is live in Rust, Node.js, and Python. GQL/Cypher-style query strings compile into the same native read and write substrates as the structured APIs, so callers move between request objects and `MATCH` / `CREATE` strings without changing engines.

- Reads: `MATCH`, `OPTIONAL MATCH`, bounded variable-length paths, path values, row projection, parameters, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, continuation cursors, and explain output.
- Mutations: `CREATE`, `SET`, `REMOVE`, `DELETE r`, and `DETACH DELETE n`, with mutation `RETURN` for `CREATE`, `SET`, and `REMOVE`.
- Reads lower into the same graph-row planner and executor as the native Rust, Node.js, and Python graph-row APIs. Mutations lower into existing write transactions and native write intents.
- Predictable row output: late hydration, selected-field projection, compact connector rows, vector payloads excluded unless requested.
- Unsupported syntax is explicit so callers know exactly what is in scope.

`execute_gql` and `explain_gql` route reads and mutations through the right native substrate. `explain_gql` is side-effect-free. Rust, Node.js, and Python parity remains the bar as the GQL surface grows.

### Numeric Property Semantics

Property equality and range planning are being tightened around real numeric behavior.

- Finite `Int`, `UInt`, and `Float` values should compare semantically where that is exact and safe.
- String equality remains string equality.
- Numeric range indexes are moving toward domainless finite numeric sort keys.
- Query paths still verify latest visible records before returning indexed candidates.

### Projection And Late Hydration

The planner is gaining better control over what has to be read and serialized.

- ID-only paths should stay cheap.
- Selected-field row output should avoid full node or edge hydration when callers only ask for a few fields.
- Vector payloads should stay out of default row output unless requested.
- Connector results should stay predictable and compact.

## Next

These are likely candidates for the next few 0.x releases. Ordering may change.

### Compound And Composite Indexes

Declare one index over ordered property tuples such as `(label, status, score)` or `(label, tenant_id, updated_at)`, with prefix semantics and planner costing.

### Query Projection Modes

Expose public projection modes between ID-only and full hydration:

- selected properties
- selected metadata fields
- compact connector-friendly payloads

### Streaming Index Intersection

For broad but useful indexed sources, intersect sorted candidate streams instead of eagerly materializing huge posting lists or falling back too early to scans.

### Graph-Algorithm-Scoped Vector Search

Let dense, sparse, and hybrid vector search run over candidate sets produced by graph algorithms and planner results, not only traversal neighborhoods.

Potential scopes include query results, pattern matches, PPR neighborhoods, projection memberships, community memberships, and explicit node sets.

### Diagnostics And Storage Cleanup

- Path-level scrub that can inspect a database directory even when open fails.
- Cleanup of old storage compatibility paths that are no longer needed before 1.0.
- Clearer sidecar provenance for optional index rebuilds and refreshes.

## Later

These are planned or plausible, but they should wait until the nearer planner and storage work settles.

### Graph Projections And Analytics

Build reusable projection snapshots for algorithm workloads, likely CSR-shaped and revision-aware.

This unlocks larger analytics features without rebuilding graph state inside every algorithm.

### More Graph Algorithms

- Louvain or Leiden-style community detection.
- Betweenness and harmonic closeness centrality.
- Triangle counting and clustering coefficients.
- Strongly connected components.

### Schema And Constraints

Optional schema tools for required properties, property validation, endpoint constraints, and uniqueness rules. Schemas should stay opt-in.

### Migration Tooling

Import and migration paths from existing graph data formats and other embedded graph stores.

## Exploring

These are research tracks. They are interesting, but they need real demand and careful design before they become committed roadmap items.

### RDF Interoperability

Define a canonical mapping between OverGraph's property graph model and RDF terms, then support import and export for common RDF formats.

### SPARQL Read Support

Expose a mapped RDF view and compile read-only SPARQL queries into OverGraph's native planner where practical.

### OWL Reasoning

Explore ontology support after RDF and SPARQL have proven demand. A practical first step would likely be a constrained profile such as RDFS or OWL 2 RL, not full OWL 2 DL.

## Recently Shipped

Highlights below. Full release notes live in [CHANGELOG.md](CHANGELOG.md).

### 0.8.0

- Named node and edge labels replaced numeric type IDs in public graph APIs.
- Nodes gained bounded multi-label support.
- Direct edge queries and edge property indexes became public across Rust, Node.js, and Python.
- Pattern planning can anchor on selective edge predicates.
- Segment format v10 introduced packed `segment.core`, component identity, optional sidecar validation, and public scrub diagnostics.
- This release intentionally reset pre-1.0 storage compatibility. Existing database directories should be rebuilt when moving to 0.8.0.

### 0.7.0

- Native planner-backed node queries shipped across Rust, Node.js, and Python.
- Boolean filters, `IN`, `exists`, `missing`, equality, range, and updated-at predicates became part of the canonical query model.
- Bounded graph pattern queries shipped with explain output.
- Planner statistics became durable optional sidecars.
- Query execution became index-aware while keeping visible-record verification as the final authority.

### Earlier Foundation

Earlier releases delivered the core LSM-style storage engine, Node.js and Python connectors, graph algorithms, vector search, hybrid retrieval, property indexes, shared-handle concurrency, explicit write transactions, and degree fast paths.
