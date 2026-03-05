<p align="center">
  <h1 align="center">OverGraph</h1>
  <p align="center">
    An absurdly fast embedded graph database.<br>
    Pure Rust. Sub-microsecond reads. Native connectors for Node.js and Python.
  </p>
</p>

<p align="center">
  <a href="LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License"></a>
  <a href="https://github.com/Bhensley5/overgraph/actions"><img src="https://img.shields.io/github/actions/workflow/status/Bhensley5/overgraph/ci.yml?branch=main&label=CI" alt="CI"></a>
</p>

---

OverGraph is a graph database that runs inside your process. No server, no network calls, no Docker containers. You open a directory, and you have a full graph database with temporal edges, weighted relationships, and sub-microsecond lookups.

I built it because I wanted a graph database that was genuinely fast. Not "fast for a database," but fast enough that you forget it's there. Node lookups in 200 nanoseconds. Neighbor traversals in 2 microseconds. Batch writes at 600K+ nodes per second. And I wanted it to work everywhere I work: Rust, Node.js, and Python, all backed by the same engine.

It's written entirely in Rust and it ships native connectors for Node.js (napi-rs) and Python (PyO3) so you can use it from whatever you're building in.

## What makes it different

- **Truly embedded.** No separate process. No socket. Your database is a folder on disk. Copy it, move it, back it up with `cp -r`.
- **Rich graph primitives.** Weighted nodes and edges, temporal validity windows, exponential decay scoring, automatic retention policies. Model relationships that evolve over time, and let the graph clean up what's no longer relevant.
- **Fast where it matters.** Node lookups in ~200ns. Neighbor traversal in ~2μs. Batch writes at 600K+ nodes/sec. The storage engine is a log-structured merge tree with mmap'd immutable segments, so reads never block writes.
- **Three languages, one engine.** Rust core with native bindings for Node.js (napi-rs) and Python (PyO3). Not a wrapper around a REST API. Actual FFI into the same Rust engine, with <3x overhead on most operations.
- **No query language.** Just functions. `upsert_node`, `neighbors`, `find_nodes`. If you can call a function, you can use OverGraph.

## Performance

All numbers from a real benchmark suite running on the Rust core (group-commit durability mode, small profile: 10K nodes / 50K edges). Full methodology and reproducibility guide in [`docs/04-quality/Benchmark-Methodology.md`](docs/04-quality/Benchmark-Methodology.md).

| Operation | Latency | Throughput |
|---|---|---|
| `get_node` | 0.2 μs | 5.5M ops/s |
| `upsert_node` | 2.2 μs | 807K ops/s |
| `neighbors` (1-hop) | 2.1 μs | 541K ops/s |
| `batch_upsert_nodes` (100) | 236 μs | 625K nodes/s |
| `top_k_neighbors` | 17.5 μs | 81K ops/s |
| `personalized_pagerank` | 254 μs | 4.3K ops/s |

Node.js and Python connectors add roughly 1.5-3x overhead depending on the operation. Batch operations are nearly 1:1 because they amortize the FFI boundary cost. Full cross-language comparison in the [launch benchmark pack](docs/04-quality/reports/2026-03-04-launch-pack-parity/).

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
use overgraph::{DatabaseEngine, DbOptions, Direction, PropValue};
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = DatabaseEngine::open(Path::new("./my-graph"), &DbOptions::default())?;

    // Create some nodes
    let mut props = BTreeMap::new();
    props.insert("name".into(), PropValue::String("Alice".into()));
    props.insert("role".into(), PropValue::String("engineer".into()));
    let alice = db.upsert_node(1, "user:alice", props, 1.0)?;

    let mut props = BTreeMap::new();
    props.insert("status".into(), PropValue::String("active".into()));
    let project = db.upsert_node(2, "project:overgraph", props, 0.9)?;

    // Connect them
    let _edge = db.upsert_edge(alice, project, 10, Default::default(), 1.0, None, None)?;

    // Traverse
    let neighbors = db.neighbors(alice, Direction::Outgoing, Some(&[10]), 50, None, None)?;
    println!("Alice is connected to {} nodes", neighbors.len());

    db.close()?;
    Ok(())
}
```

### Node.js

```javascript
import { OverGraph } from 'overgraph';

const db = await OverGraph.openAsync('./my-graph');

// Create nodes
const [alice, project] = await db.batchUpsertNodesAsync([
  { typeId: 1, key: 'user:alice', props: { name: 'Alice', role: 'engineer' }, weight: 1.0 },
  { typeId: 2, key: 'project:overgraph', props: { status: 'active' }, weight: 0.9 },
]);

// Connect them
await db.upsertEdgeAsync(alice, project, 10, { role: 'creator' }, 1.0);

// Traverse
const neighbors = await db.neighborsAsync(alice, 'outgoing', [10], 50);
console.log(`Alice is connected to ${neighbors.length} nodes`);

await db.closeAsync();
```

### Python

```python
from overgraph import OverGraph

with OverGraph.open("./my-graph") as db:
    # Create nodes
    alice = db.upsert_node(1, "user:alice",
        props={"name": "Alice", "role": "engineer"}, weight=1.0)
    project = db.upsert_node(2, "project:overgraph",
        props={"status": "active"}, weight=0.9)

    # Connect them
    db.upsert_edge(alice, project, 10, props={"role": "creator"}, weight=1.0)

    # Traverse
    neighbors = db.neighbors(alice, "outgoing", type_filter=[10], limit=50)
    print(f"Alice is connected to {len(neighbors)} nodes")
```

Or async:

```python
from overgraph import AsyncOverGraph

async with await AsyncOverGraph.open("./my-graph") as db:
    alice = await db.upsert_node(1, "user:alice", props={"name": "Alice"})
    neighbors = await db.neighbors(alice, "outgoing")
```

## Features

### Core graph operations
- **Upsert semantics.** Nodes are keyed by `(type_id, key)`. Upsert the same key twice and you get an update, not a duplicate. Edges can optionally enforce uniqueness on `(from, to, type_id)`.
- **Batch operations.** `batch_upsert_nodes` and `batch_upsert_edges` amortize WAL and memtable overhead. There's also a packed binary format for maximum throughput.
- **Atomic graph patch.** `graph_patch` lets you upsert nodes, upsert edges, delete nodes, delete edges, and invalidate edges in a single atomic operation.

### Temporal edges
- **Validity windows.** Edges have optional `valid_from` and `valid_to` timestamps. Query at any point in time with the `at_epoch` parameter and only see edges that were valid at that moment.
- **Edge invalidation.** Mark an edge as no longer valid without deleting it. The history is preserved.
- **Decay scoring.** Pass a `decay_lambda` to neighbor queries and edge weights are automatically scaled by `exp(-lambda * age_hours)`. Recent connections matter more.

### Queries and traversal
- **1-hop and 2-hop neighbors.** With optional edge type filters, direction control, and limit.
- **Constrained 2-hop.** Traverse specific edge types in the first hop, then filter target nodes by type in the second hop. Built for "find all X related to Y through Z" patterns.
- **Top-K neighbors.** Get the K highest-scoring neighbors by weight, recency, or decay-adjusted score.
- **Personalized PageRank.** Run PPR from seed nodes to find the most relevant nodes in the graph. Useful for context retrieval in RAG pipelines.
- **Subgraph extraction.** Pull out a connected subgraph up to N hops deep. Good for building local context windows.
- **Property search.** Find nodes by `type_id` + property equality. Hash-indexed for O(1) lookup.
- **Time-range queries.** Find nodes created or updated within a time window. Sorted timestamp index for efficient range scans.

### Pagination
Every collection-returning API supports keyset pagination with `limit` and `after` parameters. Cursors are stable across concurrent writes. No offset-based pagination, no skipping, no missed records.

### Retention and pruning
- **Manual prune.** Drop nodes older than X, below weight Y, or matching type Z. Incident edges cascade automatically.
- **Named prune policies.** Register policies like `"short_term_memory"` that run automatically during compaction. Nodes matching any policy are invisible to reads immediately (lazy expiration) and cleaned up during the next compaction pass.

### Storage engine
- **Write-ahead log.** Every mutation hits the WAL before the memtable. Crash recovery replays the WAL on startup.
- **Configurable durability.** `Immediate` mode fsyncs every write for maximum safety. `GroupCommit` mode (default) batches fsyncs on a 10ms timer for ~20x better write throughput with at most one timer interval of data at risk.
- **Background compaction.** Segments are merged automatically when thresholds are met. Compaction runs on a background thread and never blocks reads or writes. Uses metadata sidecars for fast filtered merging without full record decoding.
- **mmap'd reads.** Immutable segments are memory-mapped. The OS page cache handles caching. Reads never block writes.
- **Portable databases.** Each database is a self-contained directory. `cp -r ./my-db /backup/my-db` and you're done.

## How it works

OverGraph uses a log-structured merge tree (LSM) storage engine, similar in spirit to how LevelDB and RocksDB work, but purpose-built from scratch in pure Rust.

**Write path:** Mutations are appended to a write-ahead log and applied to an in-memory table (the memtable). When the memtable gets large enough, it's frozen and flushed to disk as an immutable segment with pre-built indexes.

**Read path:** Queries check the memtable first (freshest data), then scan immutable segments from newest to oldest. A K-way merge combines results across all sources with early termination for paginated queries.

**Compaction:** A background thread periodically merges older segments together, applying tombstones, prune policies, and deduplication. The compaction path uses metadata sidecars to plan merges and raw-copies winning records without deserializing them, then rebuilds all indexes from metadata. This keeps compaction fast even on large datasets.

**On-disk layout:**
```
my-graph/
  manifest.current    # atomic checkpoint (JSON)
  data.wal            # append-only write-ahead log
  segments/
    seg_0001/
      nodes.dat       # node records
      edges.dat       # edge records
      adj_out.idx     # outgoing adjacency index
      adj_in.idx      # incoming adjacency index
      key_index.dat   # (type_id, key) -> node_id
      type_index.dat  # type_id -> [id...]
      tombstones.dat  # deleted IDs
    seg_0002/
      ...
```

For a deeper dive, see the [architecture overview](docs/architecture-overview.md).

## API reference

- **Rust:** Run `cargo doc --open` to browse the full rustdoc. All public types and methods are documented.
- **Node.js:** See the [TypeScript declarations](overgraph-node/index.d.ts) for the complete API surface. Every method has a sync and async variant.
- **Python:** See the [type stubs](overgraph-python/python/overgraph/__init__.pyi) for the complete API surface. Both sync (`OverGraph`) and async (`AsyncOverGraph`) classes are available.

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
