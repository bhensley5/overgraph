<p align="center">
  <h1 align="center">OverGraph</h1>
  <p align="center">
    An absurdly fast embedded graph database with built-in vector search.<br>
    Pure Rust. Sub-microsecond reads. Native connectors for Node.js and Python.<br>
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

It's written entirely in Rust and it ships native connectors for Node.js (napi-rs) and Python (PyO3) so you can use it from whatever you're building in.

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
- **Explicit write transactions.** Stage ordered node and edge mutations locally, read your own staged writes, then commit atomically with optimistic conflict detection. Available in Rust, Node.js, and Python.
- **Three languages, one engine.** Rust core with native bindings for Node.js (napi-rs) and Python (PyO3). Not a wrapper around a REST API. Actual FFI into the same Rust engine with minimal overhead.
- **Full queries as functions.** Use regular APIs for everything: `find_nodes` for direct property lookups, `query_node_ids` / `query_nodes` for full boolean node queries, and `query_pattern` for bounded graph pattern matching. No query strings to parse, escape, or generate.

## Performance

All numbers from a real benchmark suite running on the Rust core (group-commit durability mode, small profile: 10K nodes / 50K edges). Full methodology and reproducibility guide in [`docs/04-quality/Benchmark-Methodology.md`](docs/04-quality/Benchmark-Methodology.md).

| Operation | Latency | Throughput |
|---|---|---|
| `get_node` | 34 ns | 29M ops/s |
| `upsert_node` | 2.2 μs | 807K ops/s |
| `neighbors` (1-hop) | 2.1 μs | 541K ops/s |
| `batch_upsert_nodes` (100) | 77.261 µs | 1.29M nodes/s |
| `top_k_neighbors` | 17.5 μs | 81K ops/s |
| `personalized_pagerank` | 254 μs | 4.3K ops/s |

Node.js and Python connectors add minimal overhead. Batch operations are especially efficient because they amortize the FFI boundary cost. Full cross-language comparison in the [launch benchmark pack](docs/04-quality/reports/2026-03-04-launch-pack-parity/).

## Install

Prebuilt binaries are available for macOS (ARM + Intel), Linux (x64), and Windows (x64). No Rust toolchain required.

**Python**
```bash
pip install overgraph
```

**Node.js**
```bash
npm install overgraph
```

**Rust**
```bash
cargo add overgraph
```

## Quick start

### Python

```python
from overgraph import OverGraph

USER = 1
PROJECT = 2
CREATED = 10

with OverGraph.open("./my-graph", dense_vector_dimension=384) as db:
    # Embeddings come from your model (sentence-transformers, OpenAI, etc.)
    # dense: model.encode("Alice is an engineer") -> [f32; 384]
    # sparse: splade.encode("Alice is an engineer") -> [(token_id, weight), ...]
    alice = db.upsert_node(USER, "user:alice",
        props={"name": "Alice"},
        dense_vector=alice_embedding,
        sparse_vector=alice_sparse)

    project = db.upsert_node(PROJECT, "project:overgraph",
        dense_vector=project_embedding,
        sparse_vector=project_sparse)

    db.upsert_edge(alice, project, CREATED)

    # Hybrid vector search scoped to a graph neighborhood
    hits = db.vector_search("hybrid", k=10,
        dense_query=query_embedding,
        sparse_query=query_sparse,
        scope_start_node_id=alice,
        scope_max_depth=3)

    for hit in hits:
        print(f"node {hit.node_id} score {hit.score:.4f}")
```

### Node.js

```javascript
import { OverGraph } from 'overgraph';

const USER = 1;
const PROJECT = 2;
const CREATED = 10;

const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 384 },
});

// Embeddings come from your model (sentence-transformers, OpenAI, etc.)
// dense: model.encode("Alice is an engineer") -> Float32Array(384)
// sparse: splade.encode("Alice is an engineer") -> [{ dimension, value }, ...]
const alice = db.upsertNode(USER, 'user:alice', {
  props: { name: 'Alice' },
  denseVector: aliceEmbedding,
  sparseVector: aliceSparse,
});

const project = db.upsertNode(PROJECT, 'project:overgraph', {
  denseVector: projectEmbedding,
  sparseVector: projectSparse,
});

db.upsertEdge(alice, project, CREATED);

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

### Rust

```rust
use overgraph::*;
use std::collections::BTreeMap;
use std::path::Path;

const USER: u32 = 1;
const PROJECT: u32 = 2;
const CREATED: u32 = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 384,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..Default::default()
    };
    let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;

    // Embeddings come from your model (sentence-transformers, OpenAI, etc.)
    // dense: model.encode("Alice is an engineer") -> Vec<f32> with 384 dims
    // sparse: splade.encode("Alice is an engineer") -> Vec<(u32, f32)>
    let mut props = BTreeMap::new();
    props.insert("name".into(), PropValue::String("Alice".into()));
    let alice = db.upsert_node(USER, "user:alice", UpsertNodeOptions {
        props,
        dense_vector: Some(alice_embedding),
        sparse_vector: Some(alice_sparse),
        ..Default::default()
    })?;

    let project = db.upsert_node(PROJECT, "project:overgraph", UpsertNodeOptions {
        dense_vector: Some(project_embedding),
        sparse_vector: Some(project_sparse),
        ..Default::default()
    })?;

    db.upsert_edge(alice, project, CREATED, UpsertEdgeOptions::default())?;

    // Hybrid vector search: dense + sparse with graph scoping
    let hits = db.vector_search(&VectorSearchRequest {
        mode: VectorSearchMode::Hybrid,
        dense_query: Some(query_embedding),
        sparse_query: Some(query_sparse),
        k: 10,                                              // required: 0 returns empty
        type_filter: Some(vec![USER, PROJECT]),               // default: None (no filtering)
        ef_search: Some(200),                                // default: 128
        scope: Some(VectorSearchScope {                       // default: None (search all nodes)
            start_node_id: alice,
            max_depth: 3,
            direction: Direction::Outgoing,                  // default: Outgoing
            edge_type_filter: Some(vec![CREATED]),            // default: None (all edge types)
            at_epoch: None,                                  // default: None (current time)
        }),
        dense_weight: Some(0.7),                             // default: 1.0
        sparse_weight: Some(0.3),                            // default: 1.0
        fusion_mode: Some(FusionMode::ReciprocalRankFusion), // default: WeightedRankFusion
    })?;
    for hit in &hits {
        println!("node {} score {:.4}", hit.node_id, hit.score);
    }

    db.close()?;
    Ok(())
}
```

### Async support

Both Python and Node.js connectors include full async variants of every API. Python provides `AsyncOverGraph` with native `asyncio` support. Node.js methods have `Async` suffixed variants (e.g. `upsertNodeAsync`, `vectorSearchAsync`).

## Features

### Vector search
- **Dense vector search.** Attach `f32` embedding vectors to any node. HNSW indexes are built per segment at flush time for fast approximate nearest neighbor search. Supports cosine, Euclidean, and dot-product distance metrics. One dense vector space per DB with configurable dimension.
- **Sparse vector search.** Attach sparse vectors (dimension-value pairs) for keyword-weighted retrieval. Works with pre-computed sparse embeddings from models like SPLADE or BGE-M3. Inverted posting-list indexes for exact dot-product scoring.
- **Hybrid search.** Combine dense and sparse results with built-in fusion modes: weighted rank fusion, reciprocal rank fusion, or weighted score fusion. Adjustable `dense_weight` and `sparse_weight` for tuning the blend.
- **Graph-scoped search.** Scope vector search to a graph neighborhood: "find the 10 most similar nodes within 3 hops of node X." Uses traversal-based reachable-node filtering with edge-type and temporal support. Combine graph structure with vector similarity in a single query.
- **Zero overhead when unused.** Nodes without vectors pay no storage or runtime cost. Vector index files are only created for segments that contain vectors.

### Core graph operations
- **Upsert semantics.** Nodes are keyed by `(type_id, key)`. Upsert the same key twice and you get an update, not a duplicate. Edges can optionally enforce uniqueness on `(from, to, type_id)`.
- **Batch operations.** `batch_upsert_nodes` and `batch_upsert_edges` amortize WAL and memtable overhead. `get_nodes` and `get_nodes_by_keys` do batched reads with sorted merge-walks instead of per-item lookups. There's also a packed binary format for maximum write throughput.
- **Atomic graph patch.** `graph_patch` lets you upsert nodes, upsert edges, delete nodes, delete edges, and invalidate edges in a single atomic operation.
- **Explicit transactions.** `begin_write_txn()` / `beginWriteTxn()` gives you ordered staging, rollback, read-own-writes point lookups, local aliases, atomic commit, and clean conflict errors for retry loops.

### Temporal edges
- **Validity windows.** Edges have optional `valid_from` and `valid_to` timestamps. Query at any point in time with the `at_epoch` parameter and only see edges that were valid at that moment.
- **Edge invalidation.** Mark an edge as no longer valid without deleting it. The history is preserved.
- **Decay scoring.** Pass a `decay_lambda` to neighbor queries and edge weights are automatically scaled by `exp(-lambda * age_hours)`. Recent connections matter more.

### Queries and traversal
- **Neighbors and bounded traversal.** `neighbors()` handles 1-hop expansion and returns normal neighbor entry collections in every connector; `traverse()` covers deterministic breadth-first traversal across arbitrary depth windows with optional edge-type filtering, emission-only node-type filtering, and traversal-specific pagination.
- **Depth slices without special-case APIs.** Exact depth-2 traversals are expressed as `traverse(start, 2, min_depth=2)`, so 2-hop use cases stay available without a separate public method family.
- **Top-K neighbors.** Get the K highest-scoring neighbors by weight, recency, or decay-adjusted score.
- **Personalized PageRank.** Run PPR from seed nodes to find the most relevant nodes in the graph. Rust, Node.js, and Python expose both exact power-iteration PPR and a much faster approximate forward-push mode for seed-centric retrieval workloads.
- **Subgraph extraction.** Pull out a connected subgraph up to N hops deep. Good for building local context windows.
- **Shortest path.** BFS (unweighted) or bidirectional Dijkstra (weighted). `is_connected` for fast reachability checks. `all_shortest_paths` when there are ties.
- **Connected components.** `connected_components()` returns a global WCC labelling (union-find, near-linear). `component_of(node)` returns the members of a single node's component via BFS. Both support edge-type, node-type, and temporal filters.
- **Degree counts.** Count edges, sum weights, and compute averages without materializing neighbor lists. Batch `degrees` for bulk analysis.
- **Direct property queries.** `find_nodes` and `find_nodes_paged` do focused equality lookups. `find_nodes_range` and `find_nodes_range_paged` do numeric range scans with exact bound and cursor semantics.
- **Optional property indexes.** Declare equality or numeric range indexes only where they pay off. Use `ensure_node_property_index`, `list_node_property_indexes`, and `drop_node_property_index` to manage them. Public query APIs stay index-transparent: when a matching declaration is `Ready`, OverGraph uses the declaration-backed path; otherwise it falls back to the same public API.
- **Full query APIs.** `query_node_ids`, `query_nodes`, `query_pattern`, and explain APIs combine IDs, keys, types, property equality/IN/range/exists/missing filters, updated-at ranges, and bounded graph patterns without a query string. OverGraph chooses the cheapest legal path with available indexes and planner stats, then verifies results against visible records.
- **Time-range queries.** Find nodes created or updated within a time window. Sorted timestamp index for efficient range scans.

### Pagination
ID-keyed collection APIs use keyset pagination with `limit` and `after`. `traverse()` uses `limit` plus a traversal cursor keyed by `(depth, node_id)`. No offset-based pagination. Traversal cursors assume the same query arguments and a stable logical graph state; strict snapshot isolation across intervening writes is not promised.

### Retention and pruning
- **Manual prune.** Drop nodes older than X, below weight Y, or matching type Z. Incident edges cascade automatically.
- **Named prune policies.** Register policies like `"short_term_memory"` that run automatically during compaction. Nodes matching any policy are invisible to reads immediately (lazy expiration) and cleaned up during the next compaction pass.

### Storage engine
- **Write-ahead log.** Every mutation hits the WAL before the memtable. Crash recovery replays the WAL on startup.
- **Configurable durability.** `Immediate` mode fsyncs every write for maximum safety. `GroupCommit` mode (default) batches fsyncs on a 50ms timer for ~20x better write throughput with at most one timer interval of data at risk.
- **Background compaction.** Segments are merged automatically when thresholds are met. Compaction runs on a background thread and never blocks reads or writes. Uses metadata sidecars for fast filtered merging without full record decoding.
- **Bulk ingest mode.** Temporarily disable auto-compaction during large write bursts with `ingest_mode()`, then call `end_ingest()` to compact accumulated segments and restore normal behavior. This favors ingest throughput over read performance during the ingest window.
- **mmap'd reads.** Immutable segments are memory-mapped. The OS page cache handles caching. Reads never block writes.
- **Portable databases.** Each database is a self-contained directory. `cp -r ./my-db /backup/my-db` and you're done.

## How it works

OverGraph uses a log-structured storage engine purpose-built from scratch in pure Rust. Unlike generic LSM key-value stores, every segment is a fully indexed graph structure. Adjacency lists, label indexes, temporal indexes, declared property indexes, and vector indexes are all materialized at flush time when applicable, not just at compaction. Reads are near-optimal the moment data hits disk, while writes stay append-only and fast.

**Write path:** Mutations are appended to a write-ahead log and applied to an in-memory memtable. When the memtable reaches its threshold, it's frozen and flushed to disk as an immutable segment in the background. Writes continue unblocked against a fresh memtable. Each segment ships with pre-built adjacency indexes (inbound and outbound), optional declared property-index sidecars, optional advisory planner statistics, optional signed degree-delta sidecars for degree/weight fast paths, and, when the segment contains vectors, HNSW and sparse posting-list indexes.

**Read path:** Queries check the memtable first (freshest data), then merge results across immutable segments using the per-segment indexes. Because every segment carries its own adjacency index, a neighbor query is a handful of index lookups, not a scan across sorted keys. Vector search follows the same model: memtable candidates are found by exact brute-force scan, segment candidates via HNSW or posting-list indexes, then the engine merges and deduplicates across all sources. Property equality and numeric range queries stay index-transparent too: if a matching optional property-index declaration is `Ready`, the engine uses the declaration-backed path, otherwise it falls back to a type-scoped scan through the same public API. Pagination uses early termination to avoid unnecessary work.

**Compaction:** A background thread merges older segments together, applying tombstones, prune policies, and deduplication. The compaction path uses metadata sidecars to plan merges and raw-copies winning records without full deserialization, then rebuilds unified indexes from metadata. This includes rebuilding HNSW and sparse posting-list indexes for the merged output. Fewer segments after compaction means fewer index lookups per query, but even before compaction, reads are fast because every segment is self-indexed.

**On-disk layout:**
```
my-graph/
  manifest.current        # atomic checkpoint (JSON)
  data.wal                # append-only write-ahead log
  segments/
    seg_0001/
      nodes.dat           # node records
      edges.dat           # edge records
      adj_out.idx         # outgoing adjacency index
      adj_in.idx          # incoming adjacency index
      key_index.dat       # (type_id, key) -> node_id
      type_index.dat      # type_id -> [id...]
      tombstones.dat      # deleted IDs
      secondary_indexes/  # optional declared property-index sidecars
      planner_stats.dat   # optional advisory planner statistics
      degree_delta.dat    # optional signed degree deltas for fast degree/weight reads
      node_dense_vectors.dat    # dense vector blob (when present)
      node_sparse_vectors.dat   # sparse vector blob (when present)
      dense_hnsw_graph.dat      # HNSW graph index (when present)
      sparse_postings.dat       # sparse posting lists (when present)
      node_vector_meta.dat      # vector offsets/lengths per node
    seg_0002/
      ...
```

For a deeper dive, see the [architecture overview](docs/architecture-overview.md).

## Documentation

- **[overgraph.io/docs](https://overgraph.io/docs)** - full documentation, getting started guide, and API reference.
- **[API Reference](docs/api-reference.md)** - every method, parameter, type, and return value across Python, Node.js, and Rust.
- **[Roadmap](docs/roadmap.md)** - where OverGraph is headed and what's already shipped.

## Running the benchmarks

```bash
# Rust
scripts/bench/run-rust.sh --profile small --warmup 20 --iters 80

# Node.js
scripts/bench/run-node.sh --profile small --warmup 20 --iters 80

# Python
scripts/bench/run-python.sh --profile small --warmup 20 --iters 80
```

Benchmark methodology, FAQ, and reproducibility instructions are in [`docs/04-quality/`](docs/04-quality/).

## Building from source

```bash
# Rust core
cargo build --release
cargo test

# Node.js connector
cd overgraph-node
npm install
npm run build
npm test

# Python connector
cd overgraph-python
pip install maturin
maturin develop
pytest
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, coding conventions, and how to submit a pull request.

## License

Licensed under either of

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
