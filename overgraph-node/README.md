<p align="center">
  <h1 align="center">OverGraph</h1>
  <p align="center">
    An absurdly fast embedded graph database with built-in vector search.<br>
    Pure Rust. Sub-microsecond reads. Native Node.js connector.<br>
    Built for AI agent memory, knowledge graphs, RAG pipelines, and semantic search.
  </p>
</p>

<p align="center">
  <a href="https://overgraph.io">overgraph.io</a>
</p>

<p align="center">
  <a href="LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License"></a>
  <a href="https://github.com/Bhensley5/overgraph/actions"><img src="https://img.shields.io/github/actions/workflow/status/Bhensley5/overgraph/ci.yml?branch=main&label=CI" alt="CI"></a>
</p>

---

OverGraph is a graph database that runs inside your process. No server, no network calls, no Docker containers. You open a directory, and you have a full graph database with temporal edges, weighted relationships, sub-microsecond lookups, and built-in vector search.

I built it because I wanted a graph database that was genuinely fast. Not "fast for a database," but fast enough that you forget it's there. Node lookups in 34 nanoseconds. Neighbor traversals in 2 microseconds. Batch writes at 1.29M+ nodes per second. And I wanted graph structure and vector similarity to live together in one engine. No separate vector database, no external index, no synchronization headaches.

It's written entirely in Rust and ships a native Node.js connector built with napi-rs, so JavaScript and TypeScript applications call the engine in-process without a server or REST layer.

## Built for

- **AI agent memory.** Store conversations, tool outputs, entity relationships. Attach embeddings to nodes and retrieve by semantic similarity. Decay scoring ages out stale context automatically.
- **Knowledge graphs.** Domain ontologies with shortest path queries, degree analysis, and structure exploration.
- **Semantic search.** Dense HNSW vector search, sparse keyword vectors (SPLADE, BGE-M3), and hybrid fusion. All inside the graph engine. No external vector database needed.
- **RAG pipelines.** Graph-augmented retrieval with vector search scoped to graph neighborhoods. Combine embedding similarity with graph structure in a single query.
- **Recommendation engines.** Collaborative filtering through graph traversal. PPR from seed items, top-K by weight.
- **Social and network analysis.** Degree centrality, shortest paths, and connectivity checks. The building blocks of network science.
- **Fraud detection.** Spot suspicious structures through connectivity patterns and graph algorithms.

## What makes it different

- **Truly embedded.** No separate process. No socket. Your database is a folder on disk. Copy it, move it, back it up with `cp -r`.
- **Graph + vectors in one engine.** Dense HNSW and sparse inverted indexes live alongside graph adjacency indexes in the same storage engine. Vector search can be scoped to graph neighborhoods ("find similar nodes within 2 hops of X") without a second database or a synchronization layer.
- **Rich graph primitives.** Weighted nodes and edges, temporal validity windows, exponential decay scoring, automatic retention policies. Model relationships that evolve over time, and let the graph clean up what's no longer relevant.
- **Fast where it matters.** Node lookups in ~34ns. Neighbor traversal in ~2μs. Batch writes at 1.29M+ nodes/sec. The storage engine is a log-structured merge tree with mmap'd immutable segments, so reads never block writes.
- **Explicit write transactions.** Stage ordered node and edge mutations locally, read your own staged writes, then commit atomically with optimistic conflict detection through the Node.js API.
- **Native Node.js, one engine.** Rust core with napi-rs bindings. Not a wrapper around a REST API. Actual FFI into the same Rust engine with minimal overhead.
- **Full queries as functions.** Use regular APIs for everything: `findNodes` for direct property lookups, `queryNodeIds` / `queryNodes` for full boolean node queries, and `queryPattern` for bounded graph pattern matching. No query strings to parse, escape, or generate.

## Performance

All numbers from a real benchmark suite running on the Rust core (group-commit durability mode, small profile: 10K nodes / 50K edges). Full methodology and reproducibility guide in [`docs/04-quality/Benchmark-Methodology.md`](../docs/04-quality/Benchmark-Methodology.md).

| Operation | Latency | Throughput |
|---|---|---|
| `getNode` | 34 ns | 29M ops/s |
| `upsertNode` | 2.2 μs | 807K ops/s |
| `neighbors` (1-hop) | 2.1 μs | 541K ops/s |
| `batchUpsertNodes` (100) | 77.261 µs | 1.29M nodes/s |
| `topKNeighbors` | 17.5 μs | 81K ops/s |
| `personalizedPagerank` | 254 μs | 4.3K ops/s |

The Node.js connector adds minimal overhead. Batch operations are especially efficient because they amortize the FFI boundary cost. Full methodology is covered in the [launch benchmark pack](../docs/04-quality/reports/2026-03-04-launch-pack-parity/).

## Install

Prebuilt binaries are available for macOS (ARM + Intel), Linux (x64), and Windows (x64). No Rust toolchain required.

```bash
npm install overgraph
```

## Quick start

```javascript
import { OverGraph } from 'overgraph';

const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 384 },
});

// Embeddings come from your model. Dense vectors must match the configured dimension.
// Sparse vectors use { dimension, value } entries from your sparse encoder.
// Also accepts multiple labels: ['User', 'Engineer']
const alice = db.upsertNode('User', 'alice', {
  props: { name: 'Alice' },
  denseVector: aliceEmbedding,
  sparseVector: aliceSparse,
});

const project = db.upsertNode('Project', 'overgraph', {
  denseVector: projectEmbedding,
  sparseVector: projectSparse,
});

db.upsertEdge(alice, project, 'CREATED');

// Hybrid vector search scoped to a graph neighborhood
const hits = db.vectorSearch('hybrid', {
  k: 10,
  denseQuery: queryEmbedding,
  sparseQuery: querySparse,
  scope: { startNodeId: alice, maxDepth: 3 },
});

hits.forEach(h => console.log(`node ${h.nodeId} score ${h.score.toFixed(4)}`));
db.close();
```

### Async support

The Node.js connector includes `Async` suffixed variants for every API, such as `upsertNodeAsync`, `queryNodesAsync`, and `vectorSearchAsync`.

## Features

### Vector search
- **Dense vector search.** Attach `f32` embedding vectors to any node. HNSW indexes are built per segment at flush time for fast approximate nearest neighbor search. Supports cosine, Euclidean, and dot-product distance metrics. One dense vector space per DB with configurable dimension.
- **Sparse vector search.** Attach sparse vectors (dimension-value pairs) for keyword-weighted retrieval. Works with pre-computed sparse embeddings from models like SPLADE or BGE-M3. Inverted posting-list indexes for exact dot-product scoring.
- **Hybrid search.** Combine dense and sparse results with built-in fusion modes: weighted rank fusion, reciprocal rank fusion, or weighted score fusion. Adjustable `denseWeight` and `sparseWeight` for tuning the blend.
- **Graph-scoped search.** Scope vector search to a graph neighborhood: "find the 10 most similar nodes within 3 hops of node X." Uses traversal-based reachable-node filtering with edge-label and temporal support. Combine graph structure with vector similarity in a single query.
- **Zero overhead when unused.** Nodes without vectors pay no storage or runtime cost. Vector source payloads and accelerators are only created for segments that contain vectors.

### Core graph operations
- **Upsert semantics.** Nodes carry one or more labels and one key. Each live `(label, key)` membership is unique, so a multi-label node owns the same key in every label it carries. Node upserts accept a single label string or an array of labels, and node records return `labels`. Edges can optionally enforce uniqueness on `(from, to, label)`.
- **Batch operations.** `batchUpsertNodes` and `batchUpsertEdges` amortize WAL and memtable overhead. `getNodes` and `getNodesByKeys` do batched reads with sorted merge-walks instead of per-item lookups. There's also a packed binary format for maximum write throughput.
- **Atomic graph patch.** `graphPatch` lets you upsert nodes, upsert edges, delete nodes, delete edges, and invalidate edges in a single atomic operation.
- **Explicit transactions.** `beginWriteTxn()` gives you ordered staging, rollback, read-own-writes point lookups, local aliases, atomic commit, and clean conflict errors for retry loops.

### Temporal edges
- **Validity windows.** Edges have optional `validFrom` and `validTo` timestamps. Query at any point in time with the `atEpoch` parameter and only see edges that were valid at that moment.
- **Edge invalidation.** Mark an edge as no longer valid without deleting it. The history is preserved.
- **Decay scoring.** Pass `decayLambda` to neighbor queries and edge weights are automatically scaled by `exp(-lambda * age_hours)`. Recent connections matter more.

### Queries and traversal
- **Neighbors and bounded traversal.** `neighbors()` handles 1-hop expansion and returns normal neighbor entry collections in the Node.js API; `traverse()` covers deterministic breadth-first traversal across arbitrary depth windows with optional edge-label filtering, emission-only node-label filtering, and traversal-specific pagination.
- **Depth slices without special-case APIs.** Exact depth-2 traversals are expressed as `traverse(start, 2, { minDepth: 2 })`, so 2-hop use cases stay available without a separate public method family.
- **Top-K neighbors.** Get the K highest-scoring neighbors by weight, recency, or decay-adjusted score.
- **Personalized PageRank.** Run PPR from seed nodes with `personalizedPagerank()`, using either exact power iteration or the faster approximate forward-push mode for seed-centric retrieval workloads.
- **Subgraph extraction.** Pull out a connected subgraph up to N hops deep. Good for building local context windows.
- **Shortest path.** BFS (unweighted) or bidirectional Dijkstra (weighted). `isConnected` for fast reachability checks. `allShortestPaths` when there are ties.
- **Connected components.** `connectedComponents()` returns a global WCC labelling (union-find, near-linear). `componentOf(node)` returns the members of a single node's component via BFS. Both support edge-label, node-label, and temporal filters.
- **Degree counts.** Count edges, sum weights, and compute averages without materializing neighbor lists. Batch `degrees` for bulk analysis.
- **Direct property queries.** `findNodes` and `findNodesPaged` do focused equality lookups. `findNodesRange` and `findNodesRangePaged` do numeric range scans with exact bound and cursor semantics.
- **Optional property indexes.** Declare node or edge equality/range indexes only where they pay off. Use `ensureNodePropertyIndex` / `ensureEdgePropertyIndex`, list APIs, and drop APIs to manage them. Public query APIs stay index-transparent: when a matching declaration is `Ready`, OverGraph uses the declaration-backed path; otherwise it falls back to the same public API.
- **Full query APIs.** `queryNodeIds`, `queryNodes`, `queryEdgeIds`, `queryEdges`, `queryPattern`, and explain APIs combine IDs, keys, node label filters (`{ labels, mode: 'any' | 'all' }`), edge labels, endpoint constraints, property equality/IN/range/exists/missing filters, edge metadata filters, updated-at ranges, and bounded graph patterns without a query string. OverGraph chooses the cheapest legal path with available indexes and planner stats, then verifies results against visible records.
- **Time-range queries.** Find nodes created or updated within a time window. Sorted timestamp index for efficient range scans.

### Pagination
ID-keyed collection APIs use keyset pagination with `limit` and `after`. `traverse()` uses `limit` plus a traversal cursor keyed by `(depth, node_id)`. No offset-based pagination. Traversal cursors assume the same query arguments and a stable logical graph state; strict snapshot isolation across intervening writes is not promised.

### Retention and pruning
- **Manual prune.** Drop nodes older than X, below weight Y, or matching a label. Incident edges cascade automatically.
- **Named prune policies.** Register policies like `"short_term_memory"` that run automatically during compaction. Nodes matching any policy are invisible to reads immediately (lazy expiration) and cleaned up during the next compaction pass.

### Storage engine
- **Write-ahead log.** Every mutation hits the WAL before the memtable. Crash recovery replays the WAL on startup.
- **Configurable durability.** `Immediate` mode fsyncs every write for maximum safety. `GroupCommit` mode (default) batches fsyncs on a 50ms timer for ~20x better write throughput with at most one timer interval of data at risk.
- **Background compaction.** Segments are merged automatically when thresholds are met. Compaction runs on a background thread and never blocks reads or writes. Uses packed metadata payloads for fast filtered merging without full record decoding.
- **Bulk ingest mode.** Temporarily disable auto-compaction during large write bursts with `ingestMode()`, then call `endIngest()` to compact accumulated segments and restore normal behavior. This favors ingest throughput over read performance during the ingest window.
- **mmap'd reads.** Immutable segments are memory-mapped. The OS page cache handles caching. Reads never block writes.
- **Portable databases.** Each database is a self-contained directory. `cp -r ./my-db /backup/my-db` and you're done.

## How it works

OverGraph uses a log-structured storage engine purpose-built from scratch in pure Rust. Unlike generic LSM key-value stores, every segment is a fully indexed graph structure. Adjacency lists, label indexes, temporal indexes, and vector indexes are all materialized at flush time, not just at compaction. Reads are near-optimal the moment data hits disk, while writes stay append-only and fast.

**Write path:** Mutations are appended to a write-ahead log and applied to an in-memory memtable. When the memtable reaches its threshold, it's frozen and flushed to disk as an immutable segment in the background. Writes continue unblocked against a fresh memtable. Each segment ships with pre-built adjacency indexes (inbound and outbound), optional declared property-index sidecars, optional advisory planner statistics, optional signed degree-delta sidecars for degree/weight fast paths, and, when the segment contains vectors, HNSW and sparse posting-list indexes.

**Read path:** Queries check the memtable first (freshest data), then merge results across immutable segments using the per-segment indexes. Because every segment carries its own adjacency index, a neighbor query is a handful of index lookups, not a scan across sorted keys. Vector search follows the same model: memtable candidates are found by exact brute-force scan, segment candidates via HNSW or posting-list indexes, then the engine merges and deduplicates across all sources. Property equality and numeric range queries stay index-transparent too: if a matching optional property-index declaration is `Ready`, the engine uses the declaration-backed path, otherwise it falls back to a label-scoped scan through the same public API. Pagination uses early termination to avoid unnecessary work.

**Compaction:** A background thread merges older segments together, applying tombstones, prune policies, and deduplication. The compaction path uses packed metadata payloads to plan merges and raw-copies winning records without full deserialization, then rebuilds unified indexes from metadata. This includes rebuilding HNSW and sparse posting-list indexes for the merged output. Fewer segments after compaction means fewer index lookups per query, but even before compaction, reads are fast because every segment is self-indexed.

**On-disk layout:**
```
my-graph/
  manifest.current        # atomic checkpoint (JSON)
  wal_0.wal               # append-only write-ahead log generation
  segments/
    seg_0001/
      segment_manifest.dat # component table of contents
      segment.core        # packed immutable core records, metadata, and maintained indexes
      secondary_indexes/  # optional declared equality/range property-index sidecars
      planner_stats.dat   # optional advisory planner statistics, refreshable
      degree_delta.dat    # optional signed degree deltas for fast degree/weight reads
      dense_hnsw_meta.dat # optional dense-vector HNSW metadata
      dense_hnsw_graph.dat # optional dense-vector HNSW graph
      sparse_posting_index.dat # optional sparse-vector posting index
      sparse_postings.dat # optional sparse-vector posting lists
    seg_0002/
      ...
```

`segment.core` is addressed through `segment_manifest.dat`. It contains the logical
node/edge record payloads, tombstones, key/label-token/timestamp/triple indexes, adjacency
indexes/postings, node/edge metadata, vector source-truth blobs, and immutable edge
metadata indexes. Refreshable optional accelerators stay outside the packed core so
they can be rebuilt or dropped without rewriting source data.

For a deeper dive, see the [architecture overview](../docs/architecture-overview.md).

## Documentation

- **[overgraph.io/docs](https://overgraph.io/docs)** - full documentation, getting started guide, and API reference.
- **[API Reference](../docs/api-reference.md)** - every Node.js method, parameter, type, and return value.
- **[Roadmap](../docs/roadmap.md)** - where OverGraph is headed and what's already shipped.

## Running the benchmarks

```bash
scripts/bench/run-node.sh --profile small --warmup 20 --iters 80
```

Benchmark methodology, FAQ, and reproducibility instructions are in [`docs/04-quality/`](../docs/04-quality/).

## Building from source

```bash
cd overgraph-node
npm install
npm run build
npm test
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, coding conventions, and how to submit a pull request.

## License

Licensed under either of

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
