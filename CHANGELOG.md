# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.13.0] - 2026-06-13

### Breaking Changes

#### Compound Secondary Index APIs
- **Property-index APIs now accept field-list specs.** The Rust, Node.js, and Python `ensure_*_property_index`, `drop_*_property_index`, and list APIs now use one unified declaration shape for single-field and compound secondary indexes instead of the older `(label, property, kind)` signature.
- **GQL property-index DDL now distinguishes metadata functions from property fields.** Index field lists use dot access for user properties and function calls for metadata fields, matching the new GQL metadata rule.

#### GQL Metadata Syntax
- **Function calls are now the only GQL metadata surface.** Metadata reads and writes now use functions such as `elementKey(n)`, `updatedAt(n)`, `weight(r)`, `validFrom(r)`, `validTo(r)`, `id(startNode(r))`, and `id(endNode(r))`.
- **Dot access is now always a user-property lookup.** Expressions such as `n.updated_at`, `n.key`, `r.valid_from`, and `n.weight` now read ordinary properties with those names instead of engine metadata.
- **GQL CREATE and MERGE node identity maps now use `elementKey`.** Node creation and keyed merge maps use `{elementKey: ...}` for the node key; `key` is now an ordinary property name.
- **Path and scalar helper names are camelCase.** Path helpers now use `startNode`, `endNode`, `nodeIds`, and `edgeIds`; scalar conversion helpers now use `toString`, `toInteger`, and `toFloat`.

### Added

#### Compound Secondary Indexes
- **Manifest-backed compound secondary indexes.** Added ordered node and edge secondary-index declarations over one to eight fields, combining user properties with supported metadata fields such as node key, IDs, weights, timestamps, endpoints, and edge validity windows.
- **Tuple-capable sidecars.** Added a tuple secondary-index sidecar family for multi-field and metadata-field declarations while preserving the hardened single-property sidecars for one-property declarations.
- **Compound-aware planning.** Node, edge, graph-row, and GQL planning can use Ready declarations for equality prefixes, bounded `IN` expansion, and equality-prefix-plus-one-range query shapes, with final visible-record verification preserved for correctness.
- **Compound index DDL and connector parity.** Rust, Node.js, Python, async connector APIs, TypeScript declarations, Python stubs, GQL `CREATE` / `DROP` / `SHOW PROPERTY INDEXES`, explain output, and benchmark scenarios now cover compound declarations.

### Changed

#### Secondary Index Lifecycle
- **Secondary-index lifecycle is shared across single-field and compound declarations.** Ensures, drops, retry from failed declarations, active and frozen memtable maintenance, flush, compaction, reopen validation, background builds, optional refresh, drop cleanup, and planner stats now use one declaration lifecycle.
- **Index explain and SHOW output surface ordered fields.** Public diagnostics report declaration fields, prefix eligibility, range eligibility, lifecycle state, and planner warnings for compound declarations without exposing internal token IDs.

#### GQL Metadata Model
- **Metadata parsing, validation, lowering, and execution now share one resolver.** GQL expression handling and property-index DDL use the same metadata-field mapping, keeping function spelling, target-kind checks, and display output aligned.
- **Metadata SET targets use function l-values.** `SET weight(n) = ...`, `SET weight(r) = ...`, `SET validFrom(r) = ...`, and `SET validTo(r) = ...` route to the existing metadata mutation paths, including `MERGE ON CREATE SET` and `MERGE ON MATCH SET`.
- **Pattern maps can filter metadata directly.** MATCH maps route `elementKey`, `weight`, `validFrom`, and `validTo` through native metadata filters where valid, while snake_case keys remain ordinary properties.

### Fixed

- **Compound index correctness hardening.** Compound declarations stay in sync across active writes, frozen memtables, flush, compaction, background build, reopen repair, missing/corrupt sidecar fallback, stale posting verification, label changes, tombstones, and prune visibility.
- **Metadata syntax hardening.** Fixed endpoint-id validation over projection aliases, kind checks for reused-alias pattern maps, metadata explain spelling, and mutation-return validation for endpoint ID functions.
- **Connector and docs parity.** Updated Rust, Node.js, Python, benchmark, README, getting-started, API reference, architecture overview, and GQL subset examples for compound indexes and function-style metadata.

## [0.12.0] - 2026-06-08

### Added

#### Optional Graph Schemas
- **Manifest-backed optional schemas.** Added open-by-default node and edge schemas that validate required properties, value types, numeric and collection bounds, enum values, closed-property rules, edge metadata, self-loop policy, and endpoint label rules while preserving schemaless behavior for databases without schemas.
- **Shared schema enforcement.** Schema validation now runs in the Rust core before WAL append, so native writes, transactions, batch ingestion, graph patches, GQL mutations, Node.js, and Python receive the same atomic validation behavior.
- **Schema management APIs.** Added Rust, Node.js, and Python APIs for setting, dropping, listing, and checking node and edge schemas, plus graph-level bulk schema publication with bounded violation reports and persistence across reopen.

#### GQL Schema And Index DDL
- **GQL current-graph-type DDL.** Added `ALTER CURRENT GRAPH TYPE`, `CHECK CURRENT GRAPH TYPE`, `DROP CURRENT GRAPH TYPE`, `SHOW CURRENT GRAPH TYPE`, `SHOW NODE SCHEMAS`, and `SHOW EDGE SCHEMAS` over the same schema management substrate as the native APIs.
- **GQL property-index DDL.** Added target-based `CREATE PROPERTY INDEX`, `DROP PROPERTY INDEX`, and `SHOW PROPERTY INDEXES` for existing node and edge single-property equality and range indexes.
- **Connector DDL parity.** Node.js and Python now convert schema stats, index stats, schema explain payloads, index explain payloads, schema rows, index rows, and async GQL DDL results with connector-native field naming.

### Changed

#### Validation And Catalogs
- **Schema publication is atomic.** Bulk schema add, replace, check, and drop operations now publish through one graph-schema management path with stable per-target result details and no partial catalog updates on validation failures.
- **Label membership scanning is shared.** Schema endpoint validation and query endpoint scans now reuse shared source-list scanning machinery, reducing duplicate cursor logic while preserving visibility and deterministic behavior.
- **Public docs now cover schemas and DDL.** README, getting-started, API reference, GQL subset docs, and architecture docs now include optional schemas, GQL schema DDL, and GQL property-index DDL.

### Fixed

- **Schema correctness hardening.** Fixed same-plan endpoint-rule rewrites, ID rollback after schema violations, manifest schema validation, canonical schema literal conversion, queued-write release after schema publication outcomes, and dry-run behavior under intervening writes.
- **Connector schema fidelity.** Fixed Node.js and Python schema literal round trips for tagged unsigned integers, bytes, map markers, enum values, numeric bounds, returned-schema reuse, async dry-run coverage, and stub/type declaration parity.
- **Index DDL guardrails.** Fixed unsupported GQL property-index syntax handling, side-effect-free `EXPLAIN`, unknown-label `DROP` behavior, cursor rejection, read-only execution behavior, manifest shape preservation, and planner fallback after dropped declarations.

## [0.11.0] - 2026-06-02

### Added

#### GQL Language Expansion
- **Composable GQL read pipelines.** Added `WITH` pipelines, scalar aliases, projection-local row operations, and seeded later `MATCH` / `OPTIONAL MATCH` execution over the native graph pipeline substrate.
- **Richer scalar expressions and functions.** Added arithmetic, string predicates, `CASE`, scalar functions, and shared expression evaluation for GQL reads, mutations, and graph-row-backed execution.
- **`DISTINCT` and aggregation.** Added `RETURN DISTINCT`, `WITH DISTINCT`, `count`, `sum`, `avg`, `min`, `max`, `collect`, aggregate `DISTINCT`, grouping, aggregate row operations, and compact-row output support.
- **Read-only set and subquery constructs.** Added read-only `UNION`, `UNION ALL`, `EXISTS {}` predicates, and `CALL {}` subqueries with deterministic ordering, cursors, cap enforcement, correlation caching, and explain output.
- **Shortest-path GQL syntax.** Added constrained `shortestPath` and `allShortestPaths` path assignments backed by the native shortest-path algorithms, including path helper return values.
- **Keyed `MERGE`.** Added keyed node and relationship `MERGE` with `ON CREATE SET` / `ON MATCH SET`, mutation stats, mutation `RETURN DISTINCT`, and transaction-backed atomic execution.
- **Native graph pipeline connector APIs.** Added Rust, Node.js, and Python access to structured graph pipeline execution and explain surfaces, including async connector parity.

### Changed

#### GQL Execution
- **GQL lowering now targets a reusable native graph pipeline substrate.** Multi-stage reads, aggregation, unions, subqueries, shortest paths, and keyed merge reuse existing graph-row, graph algorithm, and transaction machinery instead of adding parser-owned execution paths.
- **Connector GQL coverage now includes the Phase 34 feature set.** Node.js TypeScript declarations, Python stubs, async wrappers, compact rows, cap forwarding, nested graph/path value conversion, and explain fields now cover the expanded GQL subset.
- **Docs now present GQL as a composable query surface.** README, getting-started, API reference, and GQL subset docs now cover pipelines, aggregation, unions, subqueries, shortest paths, and keyed merge while preserving the native API-first positioning.

### Fixed

- **Pipeline correctness hardening.** Fixed edge cases around scalar alias scope, mixed graph/scalar rows, cursor shape validation, selected-field projection preservation, final-page hydration, null semantics, aggregation caps, and deterministic `UNION` de-duplication.
- **Subquery and shortest-path guardrails.** Fixed correlated subquery cache behavior, nested invocation budgeting, `CALL` row cap handling, optional `EXISTS` semantics, shortest-path endpoint resolution, row-cap enforcement, and truthful explain output.
- **MERGE and mutation parity.** Fixed keyed `MERGE` row counting, mutation profiling, late `SET` evaluation over created or matched aliases, deterministic mutation `RETURN DISTINCT`, and transaction-local overlay/coalescing behavior.

## [0.10.0] - 2026-05-27

### Added

#### GQL Mutations
- **Executable GQL writes across Rust, Node.js, and Python.** Added keyed `CREATE`, `SET`, `REMOVE`, `DELETE r`, and `DETACH DELETE n` execution to `execute_gql` / `executeGql`, using the same write-transaction machinery and durability path as the structured APIs.
- **Mutation `RETURN` support.** `CREATE`, `SET`, and `REMOVE` statements can now return object rows or compact rows with aliases, scalar expressions, ordering, `SKIP` / `OFFSET`, `LIMIT`, vector opt-in, and embedded explain/profile output.
- **Mutation result and explain surfaces.** GQL execution now returns unified mutation result payloads with `mutation_stats`, and `explain_gql` can describe mutation plans without side effects.

### Changed

#### GQL Execution
- **Reads and writes now share one public GQL surface.** GQL is no longer limited to read queries; read prefixes, mutation staging, caps, and result shaping now flow through one execution contract across all three language connectors.
- **Connector docs and getting-started guidance now cover writes.** README, getting-started, API reference, and `docs/gql-subset.md` now document mutation syntax, limits, `ReadOnly` mode, and the supported mutation `RETURN` behavior.

### Fixed

- **Mutation planner and runtime parity.** GQL mutations now preserve native semantics for cap enforcement, optional-null handling, delete deduplication, replacement ordering, conflict detection, cursor rejection, and truthful profiling.
- **Connector parity and validation.** Node.js and Python now match the Rust core for mutation options, error surfaces, compact-row output, vector opt-in, and mutation `RETURN ORDER BY` guardrails.

## [0.9.0] - 2026-05-26

### Added

#### GQL
- **Read-only GQL/Cypher-style query strings.** Added `execute_gql` and `explain_gql` in Rust, `executeGql` / `executeGqlAsync` and `explainGql` / `explainGqlAsync` in Node.js, and `execute_gql` / `explain_gql` on both Python sync and async APIs.
- **Graph-row-backed execution.** GQL now lowers into the same shared graph-row executor as native APIs, so required matches, `OPTIONAL MATCH`, bounded variable-length paths, path values, row operations, cursors, caps, explain output, and connector result shapes share one implementation.
- **Supported read syntax.** Added GQL support for `MATCH`, `OPTIONAL MATCH`, node and edge labels, relationship directions, finite bounded path quantifiers, `WHERE`, `RETURN`, aliases, scalar expressions, path functions, `ORDER BY`, `SKIP` / `OFFSET`, `LIMIT`, parameters, and explicit full-scan opt-in.
- **Path result values.** Returning path aliases can now produce node IDs, edge IDs, optional hydrated elements, `length`, `start_node`, `end_node`, `nodes`, `relationships`, `node_ids`, and `edge_ids` values across Rust, Node.js, and Python.

### Changed

#### Query Execution
- **Native graph-row substrate is now shared public API machinery.** Structured graph-row queries now cover optional groups, bounded paths, ordering, cursors, compact rows, selected-field projection, vector opt-in, and truthful explain/profile summaries across all connectors.
- **GQL public naming is unified around execute/explain.** The advertised GQL surface uses `execute_gql` / `executeGql` and `explain_gql` / `explainGql`, matching the read execution model and leaving mutation-capable naming room.
- **Public docs position GQL as a first-class query option.** README, API reference, and `docs/gql-subset.md` now document when to use native function APIs versus GQL query strings, the supported subset, and intentionally unsupported features.

### Fixed

- **GQL planner and runtime parity.** GQL reads now match native graph-row behavior for optional null extension, path expansion, relaxed self-loop distinctness, temporal and prune visibility, stale index verification, row caps, cursor validation, ordering, projection needs, and vector omission defaults.
- **Connector parity.** Node.js TypeScript declarations, Python stubs, async wrappers, and tests now expose the same GQL behavior as the Rust core.

## [0.8.0] - 2026-05-20

### Breaking Changes

#### Pre-1.0 Storage Format Reset
- **Existing database directories must be rebuilt for this release.** OverGraph now writes segment format v10 with a new component identity model and packed `segment.core` layout. Databases created by earlier releases are not expected to open on `0.8.0`.
- **Upgrade guidance:** export or re-ingest your data into a fresh database directory when moving to `0.8.0`. This is an intentional pre-1.0 compatibility break so the storage layout, label model, and identity checks can settle before wider production use.
- **Node identity changed from one type token to label sets.** Stored node records now carry node label IDs rather than a single `type_id`. Public node records expose `labels` instead of `type_id`.
- **Edge identity vocabulary changed from type IDs to label IDs.** Edges still have exactly one edge label, but the durable and diagnostic vocabulary is now `label_id` / `labelId`, not `type_id` / `typeId`.
- **No numeric type-ID compatibility aliases.** Public APIs now take node-label and edge-label names such as `"User"` and `"WORKS_AT"`. Ordinary graph APIs auto-create or resolve the internal label IDs for you. Numeric label IDs are exposed only through catalog diagnostics.

### Added

#### Public Label Model
- **Named node and edge labels.** Rust, Node.js, and Python APIs now accept label names directly. You no longer pass `type_id` values into normal writes, reads, queries, traversals, vector scopes, prune policies, or exports.
- **Automatic label catalog creation.** Mutating APIs durably create missing node-label and edge-label catalog entries as part of the same logical write plan. Read and query APIs resolve names without creating new catalog entries.
- **Catalog diagnostics.** Added `ensure_node_label`, `ensure_edge_label`, `get_node_label_id`, `get_edge_label_id`, `get_node_label`, `get_edge_label`, `list_node_labels`, and `list_edge_labels` across Rust, Node.js, Python, and async connector surfaces.
- **Multi-label nodes.** Nodes can now carry bounded label sets. Upserts accept one label or multiple labels; the engine maintains deterministic label-membership indexes and enforces conflict rules when the same key maps to different live nodes across supplied labels.
- **Explicit Any/All label filters.** Added `NodeLabelFilter` / `LabelMatchMode` across query, traversal, vector search scope, graph algorithms, export, prune, and pattern APIs so callers can ask for any listed label or every listed label.

#### Edge Queries And Edge Indexes
- **Direct edge query APIs.** Added `query_edge_ids`, `query_edges`, and `explain_edge_query` across Rust, Node.js, Python, and async connectors.
- **Edge query anchors.** Direct edge queries can combine explicit edge IDs, edge labels, `from` endpoint sets, `to` endpoint sets, either-endpoint sets, pagination, and explicit full-scan opt-in.
- **Canonical edge filters.** Edge queries and graph-pattern edges now support recursive `and` / `or` / `not` filters over weight ranges, validity windows, built-in `updated_at`, property equality, `in`, property ranges, `exists`, and `missing`.
- **Edge property index declarations.** Added `ensure_edge_property_index`, `drop_edge_property_index`, and `list_edge_property_indexes` for optional edge equality and numeric range indexes scoped by edge label.
- **Edge-property-backed planning.** Ready edge property indexes can participate in direct edge query plans and graph-pattern edge-anchor plans while final results are still verified against visible edge records.
- **Graph-pattern edge anchors.** Pattern planning can now start from selective edge labels, endpoint constraints, edge metadata, or indexed edge property predicates instead of always expanding from a node anchor first.

#### Storage Identity And Scrub
- **Segment component identity.** Added `segment_manifest.dat` component records, source-group dependency digests, build fingerprints, identity headers, required-vs-optional availability rules, and generation-aware optional refresh.
- **Packed core segments.** Added the v10 `segment.core` container for immutable core source truth and required maintained indexes.
- **Public scrub API.** Added database scrub diagnostics for segment identity, packed ranges, external sidecars, missing files, identity header mismatches, dependency mismatches, and semantic index divergence.

### Changed

#### API Model
- **Type vocabulary is now label vocabulary.** Public docs, examples, TypeScript declarations, Python stubs, Rust APIs, benchmark metadata, and connector tests now use node labels and edge labels consistently.
- **Node records expose `labels`.** Hydrated node records now return the complete public label set. Node label collection APIs are named `nodes_by_labels`, `get_nodes_by_labels`, `count_nodes_by_labels`, and paged variants.
- **Edge label diagnostics expose label IDs.** Node.js catalog diagnostics now expose `labelId`; Python exposes `label_id`. These are diagnostic token IDs, not ordinary graph API inputs.
- **Graph and vector APIs resolve names internally.** Neighbor, degree, shortest path, traversal, PPR, connected components, vector search, export, prune, query, transaction, graph patch, and batch APIs all accept public names and resolve compact numeric labels inside the engine.
- **Atomic WAL replay batches.** First-use label-token writes and dependent records are grouped with reusable atomic WAL markers, so recovery replays the complete logical mutation or discards an incomplete tail without partial catalog, record, sequence, degree, or ID effects.

#### Segment Layout
- **Required core objects moved into `segment.core`.** Node records, edge records, tombstones, node metadata, edge metadata, key indexes, node-label indexes, edge-label indexes, timestamp indexes, edge triple indexes, adjacency indexes/postings, vector source-truth blobs, and immutable edge metadata indexes are now packed into one required core container.
- **Optional accelerators stay external.** Declared node and edge property indexes, planner stats, degree deltas, dense HNSW accelerators, and sparse posting-list accelerators remain refreshable optional sidecars. If they are missing, stale, corrupt, or identity-incompatible, reads fall back to the correct non-accelerated path.
- **Flush and compaction share the same index contract.** Required indexes and optional sidecars are built through both flush and compaction paths with matching label semantics, component identity records, and dependency checks.
- **Segment open is identity-aware but still mmap-first.** Required components validate identity at open; hot read paths continue to use raw mmap payload slices without per-query digest checks.

#### Planner And Execution
- **Edge predicates are planned sources, not just post-filters.** Edge labels, endpoints, edge metadata, temporal windows, weight filters, and ready edge property indexes can all participate in costed plans.
- **Endpoint plus property queries intersect candidate sources.** Queries such as "outgoing WORKS_AT edges from these nodes where role = lead" intersect endpoint/label sources with property-index candidates when that is cheaper than hydrating the endpoint universe.
- **Pattern queries can choose edge-first plans.** High-fanout patterns with selective relationship predicates can anchor on the edge set, then bind endpoint aliases, while preserving deterministic logical result order.
- **Planner stats understand the new model.** Advisory stats now account for node label memberships, edge labels, edge property declarations, sidecar runtime coverage, stale risk, and graph-pattern fanout under the v10 layout.

### Fixed

- **Multi-label visibility across every source.** Active memtables, frozen memtables, flushed segments, compaction output, reopened databases, transactions, prune policies, exports, connector hydration, and query plans all suppress stale label memberships and preserve latest-visible node semantics.
- **Edge query correctness hardening.** Edge property filtering, endpoint visibility, tombstones, updated-at windows, valid-at windows, stale index candidates, signed-zero probes, hash collisions, pagination, and graph-pattern edge bindings are verified against visible records.
- **Edge property sidecar lifecycle.** Edge property sidecars are maintained through active writes, frozen memtables, flush, compaction, background builds, drops, reopen, targeted stats refresh, and optional refresh without making bad sidecars authoritative.
- **Packed-core and identity hardening.** Open, compaction, optional refresh, and scrub now reject or quarantine copied, stale, mismatched, missing, malformed, or wrong-container components according to whether the component is required or optional.
- **Crash-recovery atomicity.** Torn first-use label creation, batch writes, graph patches, transaction commits, cascaded deletes, and prune operations no longer leave partial catalog or record state after WAL replay.
- **Connector parity.** Node.js and Python sync/async APIs, TypeScript declarations, Python stubs, docs, examples, and tests now match the Rust core for label inputs, multi-label nodes, edge queries, edge property indexes, scrub, and catalog diagnostics.

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

[0.13.0]: https://github.com/bhensley5/overgraph/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/bhensley5/overgraph/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/bhensley5/overgraph/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/bhensley5/overgraph/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/bhensley5/overgraph/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/bhensley5/overgraph/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/bhensley5/overgraph/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/bhensley5/overgraph/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/Bhensley5/overgraph/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/Bhensley5/overgraph/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/Bhensley5/overgraph/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Bhensley5/overgraph/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Bhensley5/overgraph/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Bhensley5/overgraph/releases/tag/v0.1.0
