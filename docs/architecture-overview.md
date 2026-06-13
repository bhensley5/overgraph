# How OverGraph Works

This is a technical overview of the OverGraph storage engine for contributors and curious engineers. It covers the core architecture without going into every implementation detail.

## The big picture

OverGraph is a log-structured merge tree (LSM) graph database with built-in vector search. If you've worked with LevelDB, RocksDB, or Cassandra's storage engine, the core ideas will feel familiar. The key difference is that OverGraph is purpose-built for graph data: adjacency indexes, labeled nodes and labeled edges, temporal validity, decay scoring, and vector indexes (dense HNSW + sparse inverted posting lists) are first-class concepts in the storage format.

```
                    ┌─────────────────────┐
                    │   Your Application  │
                    │  (Rust/Node/Python)  │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   OverGraph Engine   │
                    │                     │
                    │  ┌───────────────┐  │
                    │  │   Memtable    │  │  ← in-memory, mutable
                    │  │  (HashMap)    │  │    (exact brute-force for vectors)
                    │  └───────┬───────┘  │
                    │          │ flush    │
                    │  ┌───────▼───────┐  │
                    │  │   Segments    │  │  ← on-disk, immutable, mmap'd
                    │  │  seg_0001/    │  │    (HNSW + posting-list indexes)
                    │  │  seg_0002/    │  │
                    │  │  seg_0003/    │  │
                    │  └───────┬───────┘  │
                    │          │ compact  │
                    │  ┌───────▼───────┐  │
                    │  │  Merged Seg   │  │  ← fewer, larger segments
                    │  └───────────────┘  │    (indexes rebuilt from metadata)
                    └─────────────────────┘
```

## Write path

Every mutation follows the same path:

1. **WAL append.** The operation is serialized and appended to the active write-ahead log generation (`wal_<generation>.wal`, starting with `wal_0.wal`). This is the durability guarantee: if we crash after this point, we can replay retained WAL generations on restart.

2. **Memtable apply.** The operation is applied to the in-memory memtable. The memtable uses HashMaps for nodes, edges, adjacency lists, key lookups, and internal label-token indexes. Reads served from the memtable are always the freshest.

3. **Ack.** The caller gets back the node or edge ID. The write is durable (depending on sync mode) and immediately visible to subsequent reads.

WAL records use a simple `[length: u32][crc32: u32][payload: bytes]` format. The CRC32 catches corruption. Payload encoding is compact binary with MessagePack for the property map.

### Sync modes

- **Immediate:** Every write fsyncs the WAL. Maximum durability, ~4ms per write (dominated by disk latency).
- **GroupCommit (default):** A background thread fsyncs the WAL on a timer (default 50ms). Writes return as soon as they hit the OS buffer. This gives ~20x better throughput at the cost of up to one timer interval of data at risk on a hard crash. You can call `sync()` manually to force an fsync at any point.

## Read path

Reads check multiple sources and merge them:

1. **Memtable** (freshest data, always checked first)
2. **Immutable segments** (scanned newest to oldest)

For point lookups (`get_node`, `get_edge`, `get_node_by_key`), we stop at the first source that has the record. For collection queries (`neighbors`, `find_nodes`, `nodes_by_labels`), we merge results from all sources using a K-way merge with a min-heap. Multi-label convenience scans use `All` semantics over label memberships; `Any` semantics are expressed through explicit node label filters. Planner-backed queries can also use optional per-segment `planner_stats.dat` sidecars for private cost estimates, adaptive candidate caps, and graph-row fanout planning; the stats are advisory only, and final visible-record verification still decides results. For eligible aggregation queries (`degree`, `degrees`, `sum_edge_weights`, `avg_edge_weight` with no edge-label filter, no explicit epoch, no active prune policy, and valid degree sidecars on all visible segments), reads sum published degree overlays plus per-segment `degree_delta.dat` sidecars without walking adjacency. Filtered, temporal, prune-policy, temporal-edge, or sidecar-unavailable cases fall back to the adjacency walk path.

Tombstones (from `delete_node` / `delete_edge`) are applied during the merge. Prune policies are also evaluated at read time, so a registered policy takes effect immediately without waiting for compaction.

### Pagination

All collection queries support keyset pagination via `limit` + `after`. The `after` cursor is the last ID seen. When a cursor is provided, each source binary-searches to skip past it in O(log N), so page 1000 is just as fast as page 1.

## Segments

When the memtable exceeds a size threshold (default 128MB), it gets frozen and flushed to disk as a new segment. A fresh memtable is allocated for incoming writes, so the flush never blocks the write path.

Each segment is a directory containing a small manifest plus a packed immutable core:

| File | Purpose |
|---|---|
| `segment_manifest.dat` | Component table of contents with identity/dependency records |
| `segment.core` | Packed immutable core payloads and maintained indexes |
| `secondary_indexes/` | Optional declared single-field/compound property-index sidecars |
| `degree_delta.dat` | Optional signed degree delta sidecar for degree/weight fast paths |
| `planner_stats.dat` | Optional advisory planner statistics for private query costing |
| `dense_hnsw_meta.dat` / `dense_hnsw_graph.dat` | Optional dense-vector HNSW accelerator |
| `sparse_posting_index.dat` / `sparse_postings.dat` | Optional sparse-vector inverted index accelerator |

`segment.core` holds the logical payloads that older documentation described as separate
core files: node and edge records, tombstones, node/edge metadata, key/internal-node-label-token/internal-edge-label-token/timestamp/triple
indexes, adjacency indexes/postings, vector source-truth blobs, and immutable edge metadata
indexes. The segment manifest records each logical component as a range inside `segment.core`,
so readers still expose payload-local byte slices to the same parsers while only mapping the
core container once.

Refreshable optional sidecars remain separate files. Declared single-field and compound property indexes, planner stats,
degree deltas, dense HNSW, and sparse postings can be missing, rebuilt, or refreshed without
rewriting `segment.core`; query correctness falls back to scans or exact vector search when an
optional accelerator is unavailable.

The packed core is immutable after creation. Reads use memory-mapped I/O (`mmap`), so the OS page cache handles caching without any application-level buffer management. This means reads never block writes and there's no cache invalidation to worry about.

### Adjacency index

The adjacency index is the core structure that makes graph traversal fast. Public APIs pass edge-label names; the read boundary resolves those names to internal numeric edge-label tokens before the storage engine walks adjacency. For each `(node_id, edge_label_token)` pair, the index stores the offset and count of neighbor entries in a postings payload.

**Index payload** (count header plus sorted, binary-searchable entries):
```
[count: u64]
(node_id: u64, edge_label_token: u32, offset: u64, count: u32)
```

**Postings payload** (variable-length delta/varint encoded group at each offset):
```
first:      varint(edge_id) + varint(neighbor_id) + f32(weight) + varint(valid_from) + varint(valid_to)
subsequent: varint(edge_id_delta) + varint(neighbor_id) + f32(weight) + varint(valid_from) + varint(valid_to)
```

Looking up neighbors is a binary search in the index followed by a sequential scan of the postings. This is why neighbor lookups are ~2μs even with thousands of edges per node.

### Degree counts and aggregations

Sometimes you just need "how many edges does this node have?" or "what's the total weight?" without materializing the full neighbor list. For common unfiltered, non-temporal reads, `degree()`, `degrees()`, `sum_edge_weights()`, and `avg_edge_weight()` use published degree state instead of walking adjacency postings.

Each segment may include a validated `degree_delta.dat` sidecar with signed per-node degree and weight deltas. Active and frozen WAL/memtable contributions are published with each `ReadView` as immutable overlays. Eligible degree-family reads sum the active overlay, frozen overlays, and visible segment sidecars, so the work is bounded by visible source count rather than neighbor count.

Filtered reads, explicit `at_epoch` reads, active prune policies, temporal-edge cases, and segments with missing or corrupt degree sidecars fall back to the adjacency walk path. That fallback uses the same visibility rules as `neighbors()` and preserves the old consistency guarantees.

### Shortest path algorithms

`shortest_path()`, `is_connected()`, and `all_shortest_paths()` use bidirectional search to minimize the explored frontier. Both endpoints are known, so expanding from both sides cuts the search space from O(b^d) to O(b^(d/2)).

- **BFS** (when `weight_field` is `None`): Two frontiers expand alternately, always picking the smaller one. Each step uses `for_each_adj_posting_batch` with inline visited checks and no intermediate `Vec<NeighborEntry>` allocation. `is_connected` is a specialized variant that skips parent tracking for even less overhead.
- **Dijkstra** (when `weight_field` is set): Two min-heaps with distance maps. Termination when `fwd_min + bwd_min >= mu` (best known path cost through any meeting node). When `weight_field = "weight"`, weights come directly from `NeighborEntry.weight` in the adjacency postings without edge hydration. Other field names trigger per-edge hydration via `get_edge_raw()`.

### Connected components

`connected_components()` computes a global weakly-connected-component (WCC) labelling using union-find with path compression and union by rank for near-linear O(N·α(N)) time. The algorithm collects visible nodes by resolved internal node-label token, then performs a single outgoing `neighbors_batch()` scan to union endpoints. A final pass normalizes each component ID to the minimum node ID in the component for deterministic output.

`component_of(node_id, &ComponentOptions)` answers the targeted question "which nodes are in this node's component?" via BFS using `neighbors_batch()` with both-direction traversal per frontier layer. This avoids scanning the entire graph when only one component is needed.

Both methods support edge-label, node-label, and temporal filtering, and respect active prune policies (pruned nodes are invisible to the algorithm).

### Batch adjacency

For operations that need to traverse from many nodes at once (PPR, subgraph extraction, graph export, batch degree queries), OverGraph uses a sorted cursor walk. All source node IDs are sorted, and the adjacency index is walked once with a single cursor. This avoids repeated binary searches and is significantly faster than individual lookups when the source set is large.

### Vector indexes

OverGraph embeds two kinds of vector indexes directly in the storage engine, following the same per-segment immutable index model as adjacency indexes and declared single-field/compound property-index sidecars.

**Dense HNSW index.** Each segment containing dense vectors gets an HNSW (Hierarchical Navigable Small World) graph built at flush time. The HNSW implementation is owned by OverGraph (not delegated to an external ANN library), so the on-disk format, reopen path, and segment lifecycle are fully controlled. The DB is configured with one dense vector space (fixed dimension + distance metric: cosine, Euclidean, or dot-product). HNSW parameters (`m` and `ef_construction`) are configurable.

**Sparse inverted index.** Sparse vectors are stored as canonical `(dimension_id, weight)` pairs (sorted, deduplicated, zero-dropped, non-negative). Each segment gets an inverted posting-list index mapping `dimension_id → [(node_id, score)]`. Sparse search scores candidates by exact dot-product against the query, producing correct top-K results without approximation.

**Multi-source merge.** Vector search follows the same visibility model as graph reads: the memtable is checked first (exact brute-force scan), then segments are searched newest-to-oldest. The engine merges candidates across all sources, applies tombstone/shadowing deduplication, and over-fetches as needed to guarantee `k` visible winners. A newer version of a node in a newer segment shadows the same node's vector in an older segment.

**Graph-scoped search.** The `scope` parameter on `vector_search` adds traversal-based reachable-node filtering. The engine first resolves the reachable set from a start node using the same traversal machinery as `traverse()`, then applies that set as a filter during vector candidate scoring. This enables queries like "find the 10 most similar nodes within 3 hops of X" as a single engine call.

**Hybrid fusion.** Hybrid mode runs dense and sparse sub-searches (optionally in parallel via threads), then combines the two candidate lists using one of three built-in fusion modes: weighted rank fusion, reciprocal rank fusion, or weighted score fusion. The caller controls `dense_weight` and `sparse_weight` to tune the blend.

## Compaction

Over time, segments accumulate. Old segments may contain outdated versions of records, tombstoned entries, or nodes that should be pruned by retention policies. Compaction merges multiple segments into fewer, cleaner ones.

OverGraph's compaction is designed to be fast:

1. **Plan from metadata.** Each segment has packed metadata payloads with per-record summary info (ID, timestamps, weight, tombstone status). The compaction planner reads only metadata payloads to decide which records survive, without touching the actual record data.

2. **Binary copy.** Winning records are copied as raw byte spans from input logical payloads to the output `segment.core`. No deserialization, no re-serialization. Just memcpy.

3. **Metadata-driven index building.** Maintained core output indexes (adjacency, key, internal label-token, timestamp, triple, and edge metadata indexes) are built from the metadata of winning records, not from the records themselves. This avoids a second pass over the data. Optional declared property indexes, HNSW, and sparse posting lists remain external accelerators.

4. **Cascade deletes.** If a node is tombstoned or pruned, all its incident edges are automatically dropped during the edge merge pass.

5. **Atomic swap.** The manifest is updated atomically (write to temp file, fsync, rename). Old segments are deleted only after the manifest update succeeds.

Compaction runs on a background thread and is fully cancellable (for fast shutdown). It never blocks reads or writes. If you need compaction to happen right now, call `compact()` or `compact_with_progress(callback)`.

### Prune policies and compaction

Named prune policies registered via `set_prune_policy()` are stored in the manifest and evaluated in two places:

- **Read time:** Matching nodes are filtered out of query results immediately. This is the lazy expiration pattern.
- **Compaction time:** Matching nodes are physically deleted during the merge pass, reclaiming disk space.

This means policies take effect instantly for reads, while space reclamation happens asynchronously during compaction. Same pattern as Cassandra TTLs or Redis key expiration.

## Manifest

The manifest (`manifest.current`) is a small JSON file that tracks:

- The list of live segment IDs (including per-segment vector presence flags)
- Next node ID and next edge ID counters
- Registered prune policies
- Dense vector configuration (dimension, metric, HNSW parameters)
- WAL state

The manifest is the source of truth for what data exists. On startup, OverGraph reads the manifest to discover segments, then replays the WAL to recover any writes that happened after the last flush.

Updates to the manifest are atomic: write to `manifest.tmp`, fsync, rename to `manifest.current`. The previous manifest is kept as `manifest.prev` for rollback safety.

## Concurrency model

- **Cloneable shared handles.** `DatabaseEngine` is a lightweight handle over shared process-local runtime state. Clones share the same coordinator, WAL, published read state, lifecycle workers, and close barrier.
- **Serialized commits.** Public write APIs and explicit transaction commits enter an internal commit/lifecycle coordinator. The coordinator orders WAL append, memtable apply, snapshot publication, flush adoption, compaction adoption, and close barriers so there is one authoritative mutation path.
- **Published read views.** Public reads capture the current `ReadView` snapshot at call start and run without holding the coordinator. A read view contains the active memtable snapshot, frozen memtables, visible segment readers, manifest-visible read metadata, and published degree overlays.
- **Process-local scope.** Multiple handles cloned from one open database are safe inside one process. Opening the same database directory independently from multiple processes is not supported.
- **Background workers.** Flush and compaction still run on background threads, but their completed outputs are adopted through the coordinator before becoming visible to new read views.

## FFI connectors

The Node.js and Python connectors are thin wrappers around the Rust engine:

- **Node.js (napi-rs):** All heavy work is dispatched to Rust via napi-rs. Async methods schedule work on the libuv thread pool and resolve JavaScript Promises. Record types use lazy getters, so property deserialization only happens when you actually access `.props`. ID-oriented bulk results use typed arrays (`Float64Array`, `BigInt64Array`), while record and neighbor APIs return normal JavaScript objects and arrays.

- **Python (PyO3):** All Rust calls release the GIL via `py.allow_threads()`, so other Python threads can run while the database does its work. The async API (`AsyncOverGraph`) uses `asyncio.to_thread()` to wrap sync calls. Properties are deserialized lazily on access, same as Node.js.

Both connectors expose the exact same API surface as the Rust core, including vector search. There's no feature gap between languages. Vector writes (`dense_vector`, `sparse_vector` on upsert) and `vector_search` (all modes, scoping, fusion) are available in sync and async variants.

## Property encoding

Properties are encoded as MessagePack maps. Supported value types: null, bool, integer (i64/u64), float (f64), string, bytes, and arrays of the above. MessagePack was chosen because it's compact, schema-less, fast to encode/decode, and well-supported across Rust, JavaScript, and Python.

On disk, properties are stored as opaque byte blobs. They're only deserialized when the application actually reads them. This is why `get_node` without accessing `.props` is so fast: we just return the raw record metadata without touching the property bytes.
