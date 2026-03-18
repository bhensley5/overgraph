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

I built it because I wanted a graph database that was genuinely fast. Not "fast for a database," but fast enough that you forget it's there. Node lookups in 26 nanoseconds. Neighbor traversals in 2 microseconds. Batch writes at 600K+ nodes per second. And I wanted graph structure and vector similarity to live together in one engine. No separate vector database, no external index, no synchronization headaches.

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
- **Fast where it matters.** Node lookups in ~26ns. Neighbor traversal in ~2μs. Batch writes at 600K+ nodes/sec. The storage engine is a log-structured merge tree with mmap'd immutable segments, so reads never block writes.
- **Three languages, one engine.** Rust core with native bindings for Node.js (napi-rs) and Python (PyO3). Not a wrapper around a REST API. Actual FFI into the same Rust engine with minimal overhead.
- **No query language.** Just functions. `upsert_node`, `neighbors`, `vector_search`. If you can call a function, you can use OverGraph.

## Performance

All numbers from a real benchmark suite running on the Rust core (group-commit durability mode, small profile: 10K nodes / 50K edges). Full methodology and reproducibility guide in [`docs/04-quality/Benchmark-Methodology.md`](docs/04-quality/Benchmark-Methodology.md).

| Operation | Latency | Throughput |
|---|---|---|
| `get_node` | 26 ns | 39M ops/s |
| `upsert_node` | 2.2 μs | 807K ops/s |
| `neighbors` (1-hop) | 2.1 μs | 541K ops/s |
| `batch_upsert_nodes` (100) | 236 μs | 625K nodes/s |
| `top_k_neighbors` | 17.5 μs | 81K ops/s |
| `personalized_pagerank` | 254 μs | 4.3K ops/s |

Node.js and Python connectors add minimal overhead. Batch operations are especially efficient because they amortize the FFI boundary cost. Full cross-language comparison in the [launch benchmark pack](docs/04-quality/reports/2026-03-04-launch-pack-parity/).

## Install

Prebuilt binaries are available for macOS (ARM + Intel), Linux (x64), and Windows (x64). No Rust toolchain required.

**Rust**
```bash
cargo add overgraph
```

**Node.js**
```bash
npm install overgraph
```

**Python**
```bash
pip install overgraph
```

## Quick start

### Rust

```rust
use overgraph::*;
use std::collections::BTreeMap;
use std::path::Path;

// Define your schema's type IDs
const USER: u32 = 1;
const PROJECT: u32 = 2;
const CREATED: u32 = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open with dense vector config (384-dim, cosine similarity)
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 384,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..Default::default()
    };
    let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;

    // Create nodes with dense + sparse embeddings
    let mut props = BTreeMap::new();
    props.insert("name".into(), PropValue::String("Alice".into()));
    let alice = db.upsert_node(USER, "user:alice", UpsertNodeOptions {
        props,
        dense_vector: Some(vec![0.1; 384]),
        sparse_vector: Some(vec![(42, 0.8), (99, 0.3)]),
        ..Default::default()
    })?;

    let project = db.upsert_node(PROJECT, "project:overgraph", UpsertNodeOptions {
        dense_vector: Some(vec![0.2; 384]),
        sparse_vector: Some(vec![(42, 0.5), (150, 0.9)]),
        ..Default::default()
    })?;

    // Connect them
    db.upsert_edge(alice, project, CREATED, UpsertEdgeOptions::default())?;

    // Hybrid vector search: dense + sparse with graph scoping
    let hits = db.vector_search(&VectorSearchRequest {
        mode: VectorSearchMode::Hybrid,
        dense_query: Some(vec![0.15; 384]),
        sparse_query: Some(vec![(42, 0.9), (99, 0.5)]),
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

### Node.js

```javascript
import { OverGraph } from 'overgraph';

// Define your schema's type IDs
const USER = 1;
const PROJECT = 2;
const CREATED = 10;

// Open with dense vector config
const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 384, metric: 'cosine' },
});

// Create nodes with dense + sparse embeddings
const alice = db.upsertNode(USER, 'user:alice', {
  props: { name: 'Alice', role: 'engineer' },
  denseVector: new Array(384).fill(0.1),
  sparseVector: [{ dimension: 42, value: 0.8 }, { dimension: 99, value: 0.3 }],
});
const project = db.upsertNode(PROJECT, 'project:overgraph', {
  props: { status: 'active' },
  denseVector: new Array(384).fill(0.2),
  sparseVector: [{ dimension: 42, value: 0.5 }, { dimension: 150, value: 0.9 }],
});

// Connect them
db.upsertEdge(alice, project, CREATED, { props: { role: 'creator' } });

// Hybrid vector search: dense + sparse with graph scoping
const hits = db.vectorSearch('hybrid', {
  denseQuery: new Array(384).fill(0.15),
  sparseQuery: [{ dimension: 42, value: 0.9 }, { dimension: 99, value: 0.5 }],
  k: 10,                            // required: 0 returns empty
  typeFilter: [USER, PROJECT],       // default: undefined (no filtering)
  efSearch: 200,                     // default: 128
  scope: {                               // default: undefined (search all nodes)
    startNodeId: alice,
    maxDepth: 3,
    direction: 'outgoing',           // default: 'outgoing'
    edgeTypeFilter: [CREATED],        // default: undefined (all edge types)
    atEpoch: undefined,              // default: undefined (current time)
  },
  denseWeight: 0.7,                  // default: 1.0
  sparseWeight: 0.3,                 // default: 1.0
  fusionMode: 'reciprocal-rank-fusion', // default: 'weighted-rank-fusion'
});
hits.forEach(h => console.log(`node ${h.nodeId} score ${h.score.toFixed(4)}`));

db.close();
```

Every method has an async variant (e.g. `vectorSearchAsync`, `upsertNodeAsync`).

### Python

```python
from overgraph import OverGraph

# Define your schema's type IDs
USER = 1
PROJECT = 2
CREATED = 10

with OverGraph.open("./my-graph",
                    dense_vector_dimension=384,
                    dense_vector_metric="cosine") as db:
    # Create nodes with dense + sparse embeddings
    alice = db.upsert_node(USER, "user:alice",
        props={"name": "Alice", "role": "engineer"},
        dense_vector=[0.1] * 384,
        sparse_vector=[(42, 0.8), (99, 0.3)])
    project = db.upsert_node(PROJECT, "project:overgraph",
        props={"status": "active"},
        dense_vector=[0.2] * 384,
        sparse_vector=[(42, 0.5), (150, 0.9)])

    # Connect them
    db.upsert_edge(alice, project, CREATED, props={"role": "creator"})

    # Hybrid vector search: dense + sparse with graph scoping
    hits = db.vector_search("hybrid",
        k=10,                                    # required: 0 returns empty
        dense_query=[0.15] * 384,
        sparse_query=[(42, 0.9), (99, 0.5)],
        type_filter=[USER, PROJECT],              # default: None (no filtering)
        ef_search=200,                           # default: 128
        scope_start_node_id=alice,               # default: None (search all nodes)
        scope_max_depth=3,
        scope_direction="outgoing",              # default: "outgoing"
        scope_edge_type_filter=[CREATED],         # default: None (all edge types)
        scope_at_epoch=None,                     # default: None (current time)
        dense_weight=0.7,                        # default: 1.0
        sparse_weight=0.3,                       # default: 1.0
        fusion_mode="reciprocal-rank-fusion",    # default: "weighted-rank-fusion"
    )
    for hit in hits:
        print(f"node {hit.node_id} score {hit.score:.4f}")
```

Or async:

```python
from overgraph import AsyncOverGraph

async with await AsyncOverGraph.open("./my-graph",
                                     dense_vector_dimension=384) as db:
    alice = await db.upsert_node(USER, "user:alice",
        props={"name": "Alice"},
        dense_vector=[0.1] * 384,
        sparse_vector=[(42, 0.8)])
    hits = await db.vector_search("hybrid", k=5,
        dense_query=[0.15] * 384,
        sparse_query=[(42, 0.9)])
```

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

### Temporal edges
- **Validity windows.** Edges have optional `valid_from` and `valid_to` timestamps. Query at any point in time with the `at_epoch` parameter and only see edges that were valid at that moment.
- **Edge invalidation.** Mark an edge as no longer valid without deleting it. The history is preserved.
- **Decay scoring.** Pass a `decay_lambda` to neighbor queries and edge weights are automatically scaled by `exp(-lambda * age_hours)`. Recent connections matter more.

### Queries and traversal
- **Neighbors and bounded traversal.** `neighbors()` handles 1-hop expansion; `traverse()` covers deterministic breadth-first traversal across arbitrary depth windows with optional edge-type filtering, emission-only node-type filtering, and traversal-specific pagination.
- **Depth slices without special-case APIs.** Exact depth-2 traversals are expressed as `traverse(start, 2, min_depth=2)`, so 2-hop use cases stay available without a separate public method family.
- **Top-K neighbors.** Get the K highest-scoring neighbors by weight, recency, or decay-adjusted score.
- **Personalized PageRank.** Run PPR from seed nodes to find the most relevant nodes in the graph. Useful for context retrieval in RAG pipelines.
- **Subgraph extraction.** Pull out a connected subgraph up to N hops deep. Good for building local context windows.
- **Shortest path.** BFS (unweighted) or bidirectional Dijkstra (weighted). `is_connected` for fast reachability checks. `all_shortest_paths` when there are ties.
- **Connected components.** `connected_components()` returns a global WCC labelling (union-find, near-linear). `component_of(node)` returns the members of a single node's component via BFS. Both support edge-type, node-type, and temporal filters.
- **Degree counts.** Count edges, sum weights, and compute averages without materializing neighbor lists. Batch `degrees` for bulk analysis.
- **Property search.** Find nodes by `type_id` + property equality. Hash-indexed for O(1) lookup.
- **Time-range queries.** Find nodes created or updated within a time window. Sorted timestamp index for efficient range scans.

### Pagination
ID-keyed collection APIs use keyset pagination with `limit` and `after`. `traverse()` uses `limit` plus a traversal cursor keyed by `(depth, node_id)`. No offset-based pagination. Traversal cursors assume the same query arguments and a stable logical graph state; strict snapshot isolation across intervening writes is not promised.

### Retention and pruning
- **Manual prune.** Drop nodes older than X, below weight Y, or matching type Z. Incident edges cascade automatically.
- **Named prune policies.** Register policies like `"short_term_memory"` that run automatically during compaction. Nodes matching any policy are invisible to reads immediately (lazy expiration) and cleaned up during the next compaction pass.

### Storage engine
- **Write-ahead log.** Every mutation hits the WAL before the memtable. Crash recovery replays the WAL on startup.
- **Configurable durability.** `Immediate` mode fsyncs every write for maximum safety. `GroupCommit` mode (default) batches fsyncs on a 10ms timer for ~20x better write throughput with at most one timer interval of data at risk.
- **Background compaction.** Segments are merged automatically when thresholds are met. Compaction runs on a background thread and never blocks reads or writes. Uses metadata sidecars for fast filtered merging without full record decoding.
- **Bulk ingest mode.** Temporarily disable auto-compaction during large write bursts with `ingest_mode()`, then call `end_ingest()` to compact accumulated segments and restore normal behavior. This favors ingest throughput over read performance during the ingest window.
- **mmap'd reads.** Immutable segments are memory-mapped. The OS page cache handles caching. Reads never block writes.
- **Portable databases.** Each database is a self-contained directory. `cp -r ./my-db /backup/my-db` and you're done.

## How it works

OverGraph uses a log-structured storage engine purpose-built from scratch in pure Rust. Unlike generic LSM key-value stores, every segment is a fully indexed graph structure. Adjacency lists, label indexes, temporal indexes, and vector indexes are all materialized at flush time, not just at compaction. Reads are near-optimal the moment data hits disk, while writes stay append-only and fast.

**Write path:** Mutations are appended to a write-ahead log and applied to an in-memory memtable. When the memtable reaches its threshold, it's frozen and flushed to disk as an immutable segment in the background. Writes continue unblocked against a fresh memtable. Each segment ships with pre-built adjacency indexes (inbound and outbound) and, when the segment contains vectors, HNSW and sparse posting-list indexes.

**Read path:** Queries check the memtable first (freshest data), then merge results across immutable segments using the per-segment indexes. Because every segment carries its own adjacency index, a neighbor query is a handful of index lookups, not a scan across sorted keys. Vector search follows the same model: memtable candidates are found by exact brute-force scan, segment candidates via HNSW or posting-list indexes, then the engine merges and deduplicates across all sources. Pagination uses early termination to avoid unnecessary work.

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
      node_dense_vectors.dat    # dense vector blob (when present)
      node_sparse_vectors.dat   # sparse vector blob (when present)
      dense_hnsw_graph.dat      # HNSW graph index (when present)
      sparse_postings.dat       # sparse posting lists (when present)
      node_vector_meta.dat      # vector offsets/lengths per node
    seg_0002/
      ...
```

For a deeper dive, see the [architecture overview](docs/architecture-overview.md).

Node.js keeps required arguments positional and groups optional fields in a trailing
options object. Python expresses the same optional fields as keyword arguments.

## API reference

- **Rust:** Run `cargo doc --open` to browse the full rustdoc. All public types and methods are documented.
- **Node.js:** See the [TypeScript declarations](overgraph-node/index.d.ts) for the complete API surface. Every method has a sync and async variant.
- **Python:** See the [type stubs](overgraph-python/python/overgraph/__init__.pyi) for the complete API surface. Both sync (`OverGraph`) and async (`AsyncOverGraph`) classes are available.
- **Cross-language parity:** See [`docs/internal/architecture/API-Parity-Matrix.md`](docs/internal/architecture/API-Parity-Matrix.md) for the shared API matrix across Rust, Node.js, and Python.

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
