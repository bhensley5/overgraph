# How OverGraph Works

This is a technical overview of the OverGraph storage engine for contributors and curious engineers. It covers the core architecture without going into every implementation detail. If you want the full internal spec, see [Architecture.md](internal/architecture/Architecture.md).

## The big picture

OverGraph is a log-structured merge tree (LSM) graph database with built-in vector search. If you've worked with LevelDB, RocksDB, or Cassandra's storage engine, the core ideas will feel familiar. The key difference is that OverGraph is purpose-built for graph data: adjacency indexes, typed nodes and edges, temporal validity, decay scoring, and vector indexes (dense HNSW + sparse inverted posting lists) are first-class concepts in the storage format.

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

1. **WAL append.** The operation is serialized and appended to the write-ahead log (`data.wal`). This is the durability guarantee: if we crash after this point, we can replay the WAL on restart.

2. **Memtable apply.** The operation is applied to the in-memory memtable. The memtable uses HashMaps for nodes, edges, adjacency lists, key lookups, and type indexes. Reads served from the memtable are always the freshest.

3. **Ack.** The caller gets back the node or edge ID. The write is durable (depending on sync mode) and immediately visible to subsequent reads.

WAL records use a simple `[length: u32][crc32: u32][payload: bytes]` format. The CRC32 catches corruption. Payload encoding is compact binary with MessagePack for the property map.

### Sync modes

- **Immediate:** Every write fsyncs the WAL. Maximum durability, ~4ms per write (dominated by disk latency).
- **GroupCommit (default):** A background thread fsyncs the WAL on a timer (default 10ms). Writes return as soon as they hit the OS buffer. This gives ~20x better throughput at the cost of up to one timer interval of data at risk on a hard crash. You can call `sync()` manually to force an fsync at any point.

## Read path

Reads check multiple sources and merge them:

1. **Memtable** (freshest data, always checked first)
2. **Immutable segments** (scanned newest to oldest)

For point lookups (`get_node`, `get_edge`, `get_node_by_key`), we stop at the first source that has the record. For collection queries (`neighbors`, `find_nodes`, `nodes_by_type`), we merge results from all sources using a K-way merge with a min-heap. For aggregation queries (`degree`, `sum_edge_weights`, `avg_edge_weight`), we walk adjacency postings with a callback instead of materializing results, counting and accumulating in-place.

Tombstones (from `delete_node` / `delete_edge`) are applied during the merge. Prune policies are also evaluated at read time, so a registered policy takes effect immediately without waiting for compaction.

### Pagination

All collection queries support keyset pagination via `limit` + `after`. The `after` cursor is the last ID seen. When a cursor is provided, each source binary-searches to skip past it in O(log N), so page 1000 is just as fast as page 1.

## Segments

When the memtable exceeds a size threshold (default 32MB), it gets frozen and flushed to disk as a new segment. A fresh memtable is allocated for incoming writes, so the flush never blocks the write path.

Each segment is a directory containing:

| File | Purpose |
|---|---|
| `nodes.dat` | Node records (binary, packed) |
| `edges.dat` | Edge records (binary, packed) |
| `adj_out.idx` / `adj_out.dat` | Outgoing adjacency index + postings |
| `adj_in.idx` / `adj_in.dat` | Incoming adjacency index + postings |
| `key_index.dat` | `(type_id, key)` to node_id mapping |
| `type_index.dat` | `type_id` to sorted list of IDs |
| `prop_index.dat` | Property equality hash index |
| `timestamp_index.dat` | Sorted `(updated_at, node_id)` pairs |
| `tombstones.dat` | Set of deleted IDs |
| `metadata.dat` | Sidecar with per-record metadata for fast compaction |
| `node_dense_vectors.dat` | Dense vector blob (present only when segment has vectors) |
| `node_sparse_vectors.dat` | Sparse vector blob (present only when segment has vectors) |
| `dense_hnsw_graph.dat` | HNSW graph index for dense ANN search |
| `sparse_postings.dat` | Inverted posting lists for sparse dot-product search |
| `node_vector_meta.dat` | Per-node vector presence, offsets, and lengths |

Vector files are optional per segment. Segments containing no vectors skip vector file generation entirely: no storage overhead, no index building, no mmap. The manifest tracks which segments have vector data so the engine can skip unnecessary I/O on open.

All segment files are immutable after creation. Reads use memory-mapped I/O (`mmap`), so the OS page cache handles caching without any application-level buffer management. This means reads never block writes and there's no cache invalidation to worry about.

### Adjacency index

The adjacency index is the core structure that makes graph traversal fast. For each `(node_id, edge_type_id)` pair, it stores the offset and count of neighbor entries in a postings file.

**Index file** (sorted array, binary searchable):
```
(node_id: u64, type_id: u32, offset: u64, count: u32)
```

**Postings file** (packed array at each offset):
```
(edge_id: u64, neighbor_id: u64, weight: f32, valid_from: i64, valid_to: i64)
```

Looking up neighbors is a binary search in the index followed by a sequential scan of the postings. This is why neighbor lookups are ~2μs even with thousands of edges per node.

### Degree counts and aggregations

Sometimes you just need "how many edges does this node have?" or "what's the total weight?" without materializing the full neighbor list. The `degree()`, `sum_edge_weights()`, and `avg_edge_weight()` methods use a callback-based iteration over adjacency postings: they decode each posting to extract the edge ID (for dedup across sources) and weight (for aggregation), but never allocate a `Vec<NeighborEntry>`. The only allocation is a `HashSet<u64>` for edge dedup.

This means `degree()` costs O(postings) time with O(unique edges) memory, regardless of how many neighbors a node has. Weight is embedded directly in the adjacency postings (as f32), so aggregations come free during the same decode pass without edge record hydration.

When prune policies are active, the degree methods track per-neighbor-id stats during the walk, then batch-check neighbor IDs against policies and subtract excluded neighbors. Same pattern as `neighbors()`, same consistency guarantees.

### Shortest path algorithms

`shortest_path()`, `is_connected()`, and `all_shortest_paths()` use bidirectional search to minimize the explored frontier. Both endpoints are known, so expanding from both sides cuts the search space from O(b^d) to O(b^(d/2)).

- **BFS** (when `weight_field` is `None`): Two frontiers expand alternately, always picking the smaller one. Each step uses `for_each_adj_posting_batch` with inline visited checks and no intermediate `Vec<NeighborEntry>` allocation. `is_connected` is a specialized variant that skips parent tracking for even less overhead.
- **Dijkstra** (when `weight_field` is set): Two min-heaps with distance maps. Termination when `fwd_min + bwd_min >= mu` (best known path cost through any meeting node). When `weight_field = "weight"`, weights come directly from `NeighborEntry.weight` in the adjacency postings without edge hydration. Other field names trigger per-edge hydration via `get_edge_raw()`.

### Connected components

`connected_components()` computes a global weakly-connected-component (WCC) labelling using union-find with path compression and union by rank for near-linear O(N·α(N)) time. The algorithm collects all visible nodes via `nodes_by_type()`, then performs a single outgoing `neighbors_batch()` scan to union endpoints. A final pass normalizes each component ID to the minimum node ID in the component for deterministic output.

`component_of(node_id, &ComponentOptions)` answers the targeted question "which nodes are in this node's component?" via BFS using `neighbors_batch()` with both-direction traversal per frontier layer. This avoids scanning the entire graph when only one component is needed.

Both methods support edge-type, node-type, and temporal filtering, and respect active prune policies (pruned nodes are invisible to the algorithm).

### Batch adjacency

For operations that need to traverse from many nodes at once (PPR, subgraph extraction, graph export, batch degree queries), OverGraph uses a sorted cursor walk. All source node IDs are sorted, and the adjacency index is walked once with a single cursor. This avoids repeated binary searches and is significantly faster than individual lookups when the source set is large.

### Vector indexes

OverGraph embeds two kinds of vector indexes directly in the storage engine, following the same per-segment immutable index model as adjacency and property indexes.

**Dense HNSW index.** Each segment containing dense vectors gets an HNSW (Hierarchical Navigable Small World) graph built at flush time. The HNSW implementation is owned by OverGraph (not delegated to an external ANN library), so the on-disk format, reopen path, and segment lifecycle are fully controlled. The DB is configured with one dense vector space (fixed dimension + distance metric: cosine, Euclidean, or dot-product). HNSW parameters (`m` and `ef_construction`) are configurable.

**Sparse inverted index.** Sparse vectors are stored as canonical `(dimension_id, weight)` pairs (sorted, deduplicated, zero-dropped, non-negative). Each segment gets an inverted posting-list index mapping `dimension_id → [(node_id, score)]`. Sparse search scores candidates by exact dot-product against the query, producing correct top-K results without approximation.

**Multi-source merge.** Vector search follows the same visibility model as graph reads: the memtable is checked first (exact brute-force scan), then segments are searched newest-to-oldest. The engine merges candidates across all sources, applies tombstone/shadowing deduplication, and over-fetches as needed to guarantee `k` visible winners. A newer version of a node in a newer segment shadows the same node's vector in an older segment.

**Graph-scoped search.** The `scope` parameter on `vector_search` adds traversal-based reachable-node filtering. The engine first resolves the reachable set from a start node using the same traversal machinery as `traverse()`, then applies that set as a filter during vector candidate scoring. This enables queries like "find the 10 most similar nodes within 3 hops of X" as a single engine call.

**Hybrid fusion.** Hybrid mode runs dense and sparse sub-searches (optionally in parallel via threads), then combines the two candidate lists using one of three built-in fusion modes: weighted rank fusion, reciprocal rank fusion, or weighted score fusion. The caller controls `dense_weight` and `sparse_weight` to tune the blend.

## Compaction

Over time, segments accumulate. Old segments may contain outdated versions of records, tombstoned entries, or nodes that should be pruned by retention policies. Compaction merges multiple segments into fewer, cleaner ones.

OverGraph's compaction is designed to be fast:

1. **Plan from metadata.** Each segment has a metadata sidecar with per-record summary info (ID, timestamps, weight, tombstone status). The compaction planner reads only sidecars to decide which records survive, without touching the actual record data.

2. **Binary copy.** Winning records are copied as raw byte spans from input segments to the output segment. No deserialization, no re-serialization. Just memcpy.

3. **Metadata-driven index building.** All output indexes (adjacency, key, type, property, timestamp) are built from the metadata of winning records, not from the records themselves. This avoids a second pass over the data. Vector indexes (HNSW and sparse posting lists) are rebuilt from surviving vector blob payloads and metadata.

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

- **Single writer.** All writes are serialized through a mutex-protected WAL + memtable path. This keeps the write path simple and correct. For an embedded database, write serialization is not a bottleneck because there's no network latency to hide.
- **Concurrent readers.** Immutable segments are safe for concurrent reads via mmap. The memtable is read under the same mutex, but reads are fast (HashMap lookups).
- **Background threads.** Flush and compaction run on separate threads. They never hold the write mutex except for the brief moment of swapping the manifest.

## FFI connectors

The Node.js and Python connectors are thin wrappers around the Rust engine:

- **Node.js (napi-rs):** All heavy work is dispatched to Rust via napi-rs. Async methods schedule work on the libuv thread pool and resolve JavaScript Promises. Record types use lazy getters, so property deserialization only happens when you actually access `.props`. Bulk data uses typed arrays (`Float64Array`, `BigInt64Array`) instead of JavaScript object arrays.

- **Python (PyO3):** All Rust calls release the GIL via `py.allow_threads()`, so other Python threads can run while the database does its work. The async API (`AsyncOverGraph`) uses `asyncio.to_thread()` to wrap sync calls. Properties are deserialized lazily on access, same as Node.js.

Both connectors expose the exact same API surface as the Rust core, including vector search. There's no feature gap between languages. Vector writes (`dense_vector`, `sparse_vector` on upsert) and `vector_search` (all modes, scoping, fusion) are available in sync and async variants.

## Property encoding

Properties are encoded as MessagePack maps. Supported value types: null, bool, integer (i64/u64), float (f64), string, bytes, and arrays of the above. MessagePack was chosen because it's compact, schema-less, fast to encode/decode, and well-supported across Rust, JavaScript, and Python.

On disk, properties are stored as opaque byte blobs. They're only deserialized when the application actually reads them. This is why `get_node` without accessing `.props` is so fast: we just return the raw record metadata without touching the property bytes.
