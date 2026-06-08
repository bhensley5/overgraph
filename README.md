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

It's built to feel like a library, not a service you have to operate. Drive it with function calls when you're building in code, or with GQL if you want a familiar query language. Both are first-class and run on the same engine, so you're picking syntax, not implementations. And it's genuinely fast. Not "fast for a database," but fast enough that you forget it's there. Node lookups land in tens of nanoseconds and batch writes push past a million nodes per second, so the engine stays out of the way of the rest of your stack.

Graph structure and vector similarity can live in the same engine, so you can ask things like "find similar nodes within 2 hops of X" without bolting a second database onto the side. The core is pure Rust, with native connectors for Node.js and Python so you can call it from whatever you're building in.

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
- **Full queries as functions.** Use regular APIs for everything: `find_nodes` for direct property lookups, `query_node_ids` / `query_nodes` for full boolean node queries, and `query_graph_rows` for row-shaped graph patterns, optional matches, and bounded paths.
- **GQL Beta.** Write graph reads, writes, graph-schema DDL, and property-index DDL as GQL/Cypher-style strings when that is easier than building request objects. Use `MATCH`, `WITH`, `DISTINCT`, aggregation, `UNION`, read-only subqueries, constrained shortest paths, `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE r`, `DETACH DELETE n`, mutation returns, `ALTER` / `CHECK` / `SHOW CURRENT GRAPH TYPE`, and `CREATE` / `DROP` / `SHOW PROPERTY INDEXES`.

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

These snippets intentionally show different parts of the same engine. Each workflow is available across Rust, Node.js, and Python; each block uses the language where that workflow reads cleanest.

### Python

```python
from overgraph import OverGraph

with OverGraph.open("./my-graph", dense_vector_dimension=384) as db:
    # Embeddings come from your model. Dense vectors must match the configured dimension.
    # Sparse vectors use (dimension, weight) pairs from your sparse encoder.
    # Also accepts multiple labels: ["User", "Engineer"]
    alice = db.upsert_node("User", "alice",
        props={"name": "Alice"},
        dense_vector=alice_embedding,
        sparse_vector=alice_sparse)

    project = db.upsert_node("Project", "overgraph",
        dense_vector=project_embedding,
        sparse_vector=project_sparse)

    db.upsert_edge(alice, project, "CREATED")

    # Hybrid vector search scoped to a graph neighborhood
    hits = db.vector_search("hybrid", k=10,
        dense_query=query_embedding,
        sparse_query=query_sparse,
        scope_start_node_id=alice,
        scope_max_depth=3)

    for hit in hits:
        print(f"node {hit.node_id} score {hit.score:.4f}")
```

Vector search is available across Rust, Node.js, and Python; this snippet shows the Python surface. The vector variables are placeholders from your embedding model.

### Node.js

```javascript
import { OverGraph } from 'overgraph';

const db = OverGraph.open('./my-graph');

const [alice, bob, carol, project] = db.batchUpsertNodes([
  { labels: 'User', key: 'alice', props: { name: 'Alice' } },
  { labels: 'User', key: 'bob', props: { name: 'Bob' } },
  { labels: 'User', key: 'carol', props: { name: 'Carol' } },
  { labels: 'Project', key: 'atlas', props: { name: 'Atlas' } },
]);

db.batchUpsertEdges([
  { from: alice, to: bob, label: 'KNOWS' },
  { from: bob, to: carol, label: 'KNOWS' },
  { from: carol, to: project, label: 'WORKS_ON' },
]);

const neighbors = db.neighbors(alice, {
  direction: 'outgoing',
  edgeLabelFilter: ['KNOWS'],
});

const twoHop = db.traverse(alice, 2, {
  direction: 'outgoing',
  minDepth: 1,
});

const path = db.shortestPath(alice, project, {
  direction: 'outgoing',
  maxDepth: 3,
});

console.log(neighbors.map(n => n.nodeId));
console.log(twoHop.items.map(hit => [hit.nodeId, hit.depth]));
console.log(path?.nodes ?? []);
db.close();
```

Neighbor expansion, bounded traversal, and shortest paths are available across Rust, Node.js, and Python; this snippet shows the Node.js surface.

### Rust

```rust
use overgraph::*;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = DatabaseEngine::open(Path::new("./my-graph"), &DbOptions::default())?;

    let alice = db.upsert_node("User", "alice", UpsertNodeOptions::default())?;
    let bob = db.upsert_node("User", "bob", UpsertNodeOptions::default())?;
    let carol = db.upsert_node("User", "carol", UpsertNodeOptions::default())?;
    let project = db.upsert_node("Project", "atlas", UpsertNodeOptions::default())?;

    db.upsert_edge(alice, bob, "FOLLOWS", UpsertEdgeOptions::default())?;
    db.upsert_edge(bob, carol, "FOLLOWS", UpsertEdgeOptions::default())?;
    db.upsert_edge(carol, project, "WORKS_ON", UpsertEdgeOptions::default())?;

    let ranks = db.personalized_pagerank(&[alice], &PprOptions {
        algorithm: PprAlgorithm::ApproxForwardPush,
        edge_label_filter: Some(vec!["FOLLOWS".into(), "WORKS_ON".into()]),
        max_results: Some(5),
        ..Default::default()
    })?;

    for (node_id, score) in ranks.scores {
        println!("node {node_id} rank {score:.4}");
    }

    db.close()?;
    Ok(())
}
```

Personalized PageRank is available across Rust, Node.js, and Python; this snippet shows the Rust surface.

## GQL Beta

OverGraph includes **GQL Beta**: a GQL/Cypher-style query language for graph reads, writes, graph-schema DDL, and property-index DDL. Use it when a graph operation is easier to read as text: create records, match patterns, shape rows with `WITH`, aggregate, combine branches with `UNION`, run read-only subqueries, use constrained shortest paths, return mutation results, manage the current graph type with `ALTER`, `CHECK`, `DROP`, and `SHOW`, or manage property-index declarations with `CREATE`, `DROP`, and `SHOW PROPERTY INDEXES`.

```python
db.execute_gql(
    """
    CREATE (p:Person {key: 'gql-ada', name: 'Ada', status: 'active'})
           -[r:WORKS_AT {role: 'engineer', since: 2026}]->
           (c:Company {key: 'gql-overgraph', name: 'OverGraph'})
    RETURN p.name AS person, c.name AS company, r.role AS role
    """
)

result = db.execute_gql(
    """
    MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
    WHERE p.status = 'active' AND r.since >= 2020
    RETURN p.name AS person, r.role AS role, c.name AS company
    ORDER BY r.since DESC
    LIMIT 10
    """,
    include_plan=True,
    profile=True,
)

print(result["rows"])
print(result["stats"])
print(result["plan"]["read"]["row_ops"])
```

GQL Beta is available across Rust, Node.js, and Python. It supports params, read cursors, compact rows, vector opt-in for returned node values, explain/profile, read-only execution, mutation stats, schema stats, index stats, async connector calls, and consistent result shapes across languages. See the full [GQL Beta API reference](docs/api-reference.md#gql-beta) for syntax, result shapes, options, and examples.

### Async support

Both Python and Node.js connectors include full async variants of every API. Python provides `AsyncOverGraph`, an asyncio wrapper that runs sync operations in a thread pool via `asyncio.to_thread()`. Node.js methods have `Async` suffixed variants (e.g. `upsertNodeAsync`, `vectorSearchAsync`).

## Features

### Vector search
- **Dense vector search.** Attach `f32` embedding vectors to any node. HNSW indexes are built per segment at flush time for fast approximate nearest neighbor search. Supports cosine, Euclidean, and dot-product distance metrics. One dense vector space per DB with configurable dimension.
- **Sparse vector search.** Attach sparse vectors (dimension-value pairs) for keyword-weighted retrieval. Works with pre-computed sparse embeddings from models like SPLADE or BGE-M3. Inverted posting-list indexes for exact dot-product scoring.
- **Hybrid search.** Combine dense and sparse results with built-in fusion modes: weighted rank fusion, reciprocal rank fusion, or weighted score fusion. Adjustable `dense_weight` and `sparse_weight` for tuning the blend.
- **Graph-scoped search.** Scope vector search to a graph neighborhood: "find the 10 most similar nodes within 3 hops of node X." Uses traversal-based reachable-node filtering with edge-label and temporal support. Combine graph structure with vector similarity in a single query.
- **Zero overhead when unused.** Nodes without vectors pay no storage or runtime cost. Vector index files are only created for segments that contain vectors.

### Core graph operations
- **Upsert semantics.** Nodes carry one or more labels and one key. Each live `(label, key)` membership is unique, so a multi-label node owns the same key in every label it carries. Upserting a key that resolves to the same node through the supplied labels updates it; if supplied label memberships resolve to different nodes, the write is rejected as a conflict. Edges can optionally enforce uniqueness on `(from, to, label)`.
- **Batch operations.** `batch_upsert_nodes` and `batch_upsert_edges` amortize WAL and memtable overhead. `get_nodes` and `get_nodes_by_keys` do batched reads with sorted merge-walks instead of per-item lookups. There's also a packed binary format for maximum write throughput.
- **Atomic graph patch.** `graph_patch` lets you upsert nodes, upsert edges, delete nodes, delete edges, and invalidate edges in a single atomic operation.
- **Explicit transactions.** `begin_write_txn()` / `beginWriteTxn()` gives you ordered staging, rollback, read-own-writes point lookups, local aliases, atomic commit, and clean conflict errors for retry loops.

### Temporal edges
- **Validity windows.** Edges have optional `valid_from` and `valid_to` timestamps. Query at any point in time with the `at_epoch` parameter and only see edges that were valid at that moment.
- **Edge invalidation.** Mark an edge as no longer valid without deleting it. The history is preserved.
- **Decay scoring.** Pass a `decay_lambda` to neighbor queries and edge weights are automatically scaled by `exp(-lambda * age_hours)`. Recent connections matter more.

### Queries and traversal
- **Neighbors and bounded traversal.** `neighbors()` handles 1-hop expansion and returns normal neighbor entry collections in every connector; `traverse()` covers deterministic breadth-first traversal across arbitrary depth windows with optional edge-label filtering, emission-only node-label filtering, and traversal-specific pagination.
- **Depth slices without special-case APIs.** Exact depth-2 traversals are expressed as `traverse(start, 2, min_depth=2)`, so 2-hop use cases stay available without a separate public method family.
- **Top-K neighbors.** Get the K highest-scoring neighbors by weight, recency, or decay-adjusted score.
- **Personalized PageRank.** Run PPR from seed nodes to find the most relevant nodes in the graph. Rust, Node.js, and Python expose both exact power-iteration PPR and a much faster approximate forward-push mode for seed-centric retrieval workloads.
- **Subgraph extraction.** Pull out a connected subgraph up to N hops deep. Good for building local context windows.
- **Shortest path.** BFS (unweighted) or bidirectional Dijkstra (weighted). `is_connected` for fast reachability checks. `all_shortest_paths` when there are ties.
- **Connected components.** `connected_components()` returns a global WCC labelling (union-find, near-linear). `component_of(node)` returns the members of a single node's component via BFS. Both support edge-label, node-label, and temporal filters.
- **Degree counts.** Count edges, sum weights, and compute averages without materializing neighbor lists. Batch `degrees` for bulk analysis.
- **Direct property queries.** `find_nodes` and `find_nodes_paged` do focused equality lookups with semantic numeric equality for finite scalars. `find_nodes_range` and `find_nodes_range_paged` do domainless numeric range scans with exact bound and cursor semantics.
- **Optional property indexes.** Declare node or edge equality/range indexes only where they pay off. Range indexes cover finite scalar numeric values across signed integers, unsigned integers, and finite floats; non-finite floats and non-numeric values are excluded. Use `ensure_node_property_index` / `ensure_edge_property_index`, list APIs, and drop APIs to manage them. Public query APIs stay index-transparent: when a matching declaration is `Ready`, OverGraph uses the declaration-backed path; otherwise it falls back to the same public API.
- **Optional schemas and constraints.** Databases stay open by default; label-scoped node and edge schemas can validate required properties, value types, metadata, and endpoint labels through the shared Rust write path across Rust, Node.js, Python, and GQL mutations. Manage them with single-target helpers, atomic graph-level schema APIs, or the supported GQL current-graph-type DDL subset.
- **Full query APIs.** `query_node_ids`, `query_nodes`, `query_edge_ids`, `query_edges`, `query_graph_rows`, and explain APIs combine IDs, keys, labels, edge labels, endpoint constraints, property equality/IN/range/exists/missing filters, edge metadata filters, updated-at ranges, row-shaped graph patterns, optional groups, and bounded paths without a query string. `execute_gql` / `executeGql` adds GQL Beta for query-string reads and mutations. OverGraph chooses the cheapest legal path with available indexes and planner stats, then verifies results against visible records.
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
- **Bulk ingest mode.** Temporarily disable auto-compaction during large write bursts with `ingest_mode()`, then call `end_ingest()` to compact accumulated segments and restore normal behavior. This favors ingest throughput over read performance during the ingest window.
- **mmap'd reads.** Immutable segments are memory-mapped. The OS page cache handles caching. Reads never block writes.
- **Portable databases.** Each database is a self-contained directory. `cp -r ./my-db /backup/my-db` and you're done.

## How it works

OverGraph uses a log-structured storage engine purpose-built from scratch in pure Rust. Unlike generic LSM key-value stores, every segment is a fully indexed graph structure. Adjacency lists, label indexes, temporal indexes, declared property indexes, and vector indexes are all materialized at flush time when applicable, not just at compaction. Reads are near-optimal the moment data hits disk, while writes stay append-only and fast.

**Write path:** Mutations are appended to a write-ahead log and applied to an in-memory memtable. When the memtable reaches its threshold, it's frozen and flushed to disk as an immutable segment in the background. Writes continue unblocked against a fresh memtable. Each segment ships with pre-built adjacency indexes (inbound and outbound), optional declared property-index sidecars, optional advisory planner statistics, optional signed degree-delta sidecars for degree/weight fast paths, and, when the segment contains vectors, HNSW and sparse posting-list indexes.

**Read path:** Queries check the memtable first (freshest data), then merge results across immutable segments using the per-segment indexes. Because every segment carries its own adjacency index, a neighbor query is a handful of index lookups, not a scan across sorted keys. Vector search follows the same model: memtable candidates are found by exact brute-force scan, segment candidates via HNSW or posting-list indexes, then the engine merges and deduplicates across all sources. Property equality and domainless numeric range queries stay index-transparent too: if a matching optional property-index declaration is `Ready`, the engine uses the declaration-backed path, otherwise it falls back to a label-scoped scan through the same public API. Pagination uses early termination to avoid unnecessary work, and index candidates are verified against the latest visible records before results are returned.

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

For a deeper dive, see the [architecture overview](docs/architecture-overview.md).

## Documentation

- **[overgraph.io/docs](https://overgraph.io/docs)** - full documentation, getting started guide, and API reference.
- **[API Reference](docs/api-reference.md)** - every method, parameter, type, and return value across Python, Node.js, and Rust.
- **[Roadmap](ROADMAP.md)** - where OverGraph is headed and what's already shipped.

## Community Integrations

- **[OvergraphSwiftBridge](https://github.com/wildthink/OvergraphSwiftBridge)** - a community-maintained bridge for bringing OverGraph into Swift and Apple-platform projects.

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
