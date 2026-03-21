# OverGraph API Reference

Complete reference for OverGraph's public API across **Rust**, **Node.js**, and **Python**. Every method, parameter, type, and return value is documented.

> **Conventions used in this document:**
>
> - Parameters marked **required** must always be provided. Parameters marked **optional** may be omitted and will use documented defaults.
> - `u32` / `u64` / `i64` / `f32` / `f64` refer to fixed-width numeric types. In Node.js these map to `number`; in Python to `int` or `float`.
> - All timestamps are **milliseconds since Unix epoch** (January 1, 1970 00:00:00 UTC).
> - All IDs (`node_id`, `edge_id`) are unsigned 64-bit integers. In Node.js they are represented as `number` (safe up to 2^53 - 1). In Python they are `int` (unlimited precision).
> - Code examples show all three languages. Rust examples assume `use overgraph::*;` is in scope.

---

## Table of Contents

- [Installation](#installation)
- [Database Lifecycle](#database-lifecycle)
  - [open](#open)
  - [close](#close)
  - [stats](#stats)
- [Configuration](#configuration)
  - [DbOptions](#dboptions)
  - [WalSyncMode](#walsyncmode)
  - [DenseVectorConfig](#densevectorconfig)
- [Data Model](#data-model)
  - [NodeRecord](#noderecord)
  - [EdgeRecord](#edgerecord)
  - [PropValue](#propvalue)
  - [Direction](#direction)
- [Node Operations](#node-operations)
  - [upsert_node](#upsert_node)
  - [get_node](#get_node)
  - [get_node_by_key](#get_node_by_key)
  - [delete_node](#delete_node)
  - [batch_upsert_nodes](#batch_upsert_nodes)
  - [get_nodes](#get_nodes)
  - [get_nodes_by_keys](#get_nodes_by_keys)
- [Edge Operations](#edge-operations)
  - [upsert_edge](#upsert_edge)
  - [get_edge](#get_edge)
  - [get_edge_by_triple](#get_edge_by_triple)
  - [delete_edge](#delete_edge)
  - [invalidate_edge](#invalidate_edge)
  - [batch_upsert_edges](#batch_upsert_edges)
  - [get_edges](#get_edges)
- [Atomic Operations](#atomic-operations)
  - [graph_patch](#graph_patch)
- [Type-Based Queries](#type-based-queries)
  - [nodes_by_type](#nodes_by_type)
  - [edges_by_type](#edges_by_type)
  - [get_nodes_by_type](#get_nodes_by_type)
  - [get_edges_by_type](#get_edges_by_type)
  - [count_nodes_by_type](#count_nodes_by_type)
  - [count_edges_by_type](#count_edges_by_type)
- [Property & Time Queries](#property--time-queries)
  - [find_nodes](#find_nodes)
  - [find_nodes_by_time_range](#find_nodes_by_time_range)
- [Pagination](#pagination)
  - [nodes_by_type_paged](#nodes_by_type_paged)
  - [edges_by_type_paged](#edges_by_type_paged)
  - [get_nodes_by_type_paged](#get_nodes_by_type_paged)
  - [get_edges_by_type_paged](#get_edges_by_type_paged)
  - [find_nodes_paged](#find_nodes_paged)
  - [find_nodes_by_time_range_paged](#find_nodes_by_time_range_paged)
- [Neighbor Queries](#neighbor-queries)
  - [neighbors](#neighbors)
  - [neighbors_paged](#neighbors_paged)
  - [neighbors_batch](#neighbors_batch)
  - [top_k_neighbors](#top_k_neighbors)
- [Degree & Weight Aggregation](#degree--weight-aggregation)
  - [degree](#degree)
  - [degrees](#degrees)
  - [sum_edge_weights](#sum_edge_weights)
  - [avg_edge_weight](#avg_edge_weight)
- [Traversal](#traversal)
  - [traverse](#traverse)
  - [extract_subgraph](#extract_subgraph)
- [Pathfinding](#pathfinding)
  - [shortest_path](#shortest_path)
  - [all_shortest_paths](#all_shortest_paths)
  - [is_connected](#is_connected)
- [Graph Analytics](#graph-analytics)
  - [connected_components](#connected_components)
  - [component_of](#component_of)
  - [personalized_pagerank](#personalized_pagerank)
  - [export_adjacency](#export_adjacency)
- [Vector Search](#vector-search)
  - [vector_search](#vector_search)
- [Retention & Pruning](#retention--pruning)
  - [prune](#prune)
  - [set_prune_policy](#set_prune_policy)
  - [remove_prune_policy](#remove_prune_policy)
  - [list_prune_policies](#list_prune_policies)
- [Maintenance](#maintenance)
  - [sync](#sync)
  - [flush](#flush)
  - [compact](#compact)
  - [compact_with_progress](#compact_with_progress)
  - [ingest_mode](#ingest_mode)
  - [end_ingest](#end_ingest)
- [Introspection](#introspection)
  - [node_count](#node_count)
  - [edge_count](#edge_count)
  - [segment_count](#segment_count)
- [Binary Batch Ingestion](#binary-batch-ingestion)
  - [batch_upsert_nodes_binary](#batch_upsert_nodes_binary)
  - [batch_upsert_edges_binary](#batch_upsert_edges_binary)
- [Error Handling](#error-handling)
- [Async API](#async-api)

---

## Installation

**Rust** - add to `Cargo.toml`:
```toml
[dependencies]
overgraph = "0.4"
```

**Node.js**:
```bash
npm install overgraph
```

**Python**:
```bash
pip install overgraph
```

Prebuilt binaries are published for Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon), and Windows (x86_64). If no prebuilt binary exists for your platform, install a Rust toolchain and the package will compile from source.

---

## Database Lifecycle

### open

Opens an existing database or creates a new one. A database is a self-contained directory on disk.

**Rust**
```rust
let mut db = DatabaseEngine::open(Path::new("./my-graph"), &DbOptions::default())?;
```

**Node.js**
```javascript
const db = OverGraph.open('./my-graph', { /* options */ });
```

**Python**
```python
db = OverGraph.open("./my-graph", **options)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| path | `&Path` | `string` | `str` | Yes | — | Directory path for the database. Created if it doesn't exist (when `create_if_missing` is true). Must be a valid filesystem path. |
| options | `&DbOptions` | `object` | `**kwargs` | No | See [DbOptions](#dboptions) | Database configuration. See the full options reference below. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<DatabaseEngine, EngineError>` | `OverGraph` | `OverGraph` |

A new database instance. On failure, raises/returns an error if the path is inaccessible, the manifest is corrupt, or WAL replay fails.

#### Behavior

- If the directory doesn't exist and `create_if_missing` is true (the default), the directory is created.
- If the directory contains an existing database, OverGraph loads the manifest, opens all segments, and replays WAL generations to recover in-flight state.
- Configuration values (`wal_sync_mode`, `dense_vector`, etc.) are persisted in the manifest on first open. Subsequent opens use the persisted configuration; the values you pass are only used for the initial creation.
- Opening the same directory from multiple processes simultaneously is **not supported** and may cause data corruption.

#### Example

```rust
// Rust - open with custom options
let opts = DbOptions {
    wal_sync_mode: WalSyncMode::GroupCommit {
        interval_ms: 50,
        soft_trigger_bytes: 2 * 1024 * 1024,
        hard_cap_bytes: 16 * 1024 * 1024,
    },
    edge_uniqueness: true,
    dense_vector: Some(DenseVectorConfig {
        dimension: 384,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    }),
    ..Default::default()
};
let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;
```

```javascript
// Node.js - open with custom options
const db = OverGraph.open('./my-graph', {
  walSyncMode: 'group-commit',
  groupCommitIntervalMs: 50,
  edgeUniqueness: true,
  denseVector: { dimension: 384, metric: 'cosine' },
  compactAfterNFlushes: 5,
});
```

```python
# Python - open with custom options
db = OverGraph.open(
    "./my-graph",
    wal_sync_mode="group_commit",
    group_commit_interval_ms=50,
    edge_uniqueness=True,
    dense_vector_dimension=384,
    dense_vector_metric="cosine",
    compact_after_n_flushes=5,
)
```

---

### close

Shuts down the database cleanly.

**Rust**
```rust
db.close()?;
```

**Node.js**
```javascript
db.close();            // sync, waits for compaction
db.close({ force: true }); // sync, cancels compaction
await db.closeAsync(); // async
```

**Python**
```python
db.close()           # waits for compaction
db.close(force=True) # cancels compaction
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| force | Use `close_fast()` instead | `boolean` | `bool` | No | `false` | If `true`, cancels any in-progress background compaction instead of waiting for it to finish. Pending WAL data is still synced. |

#### Behavior

**Normal close** (`force=false`):
1. Freezes the active memtable.
2. Flushes all pending immutable memtables to segments.
3. Waits for any in-progress background compaction to finish.
4. Writes the final manifest.
5. After close, no immutable memtables or retained WAL generations remain.

**Fast close** (`force=true` / `close_fast()` in Rust):
1. Cancels in-progress background compaction (safe because no state is modified until the atomic swap).
2. Syncs the active WAL.
3. Persists the manifest with retained WAL generations (so WAL replay recovers state on next open).

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<(), EngineError>` | `void` / `Promise<void>` | `None` |

#### Context Manager / Destructor

**Python** supports context manager syntax:
```python
with OverGraph.open("./my-graph") as db:
    db.upsert_node(1, "alice")
# db.close() called automatically on exit
```

**Node.js** has no built-in equivalent; call `close()` or `closeAsync()` explicitly in a `finally` block.

---

### stats

Returns a read-only snapshot of current database statistics.

**Rust**
```rust
let s = db.stats();
println!("segments: {}, WAL bytes: {}", s.segment_count, s.pending_wal_bytes);
```

**Node.js**
```javascript
const s = db.stats();
console.log(`segments: ${s.segmentCount}, WAL bytes: ${s.pendingWalBytes}`);
```

**Python**
```python
s = db.stats()
print(f"segments: {s.segment_count}, WAL bytes: {s.pending_wal_bytes}")
```

#### Parameters

None.

#### Returns: DbStats

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| pending_wal_bytes | `usize` | `number` | `int` | Bytes buffered in the WAL not yet fsynced to disk. |
| segment_count | `usize` | `number` | `int` | Number of immutable segments on disk. |
| node_tombstone_count | `usize` | `number` | `int` | Soft-deleted nodes in the active memtable (reclaimed at compaction). |
| edge_tombstone_count | `usize` | `number` | `int` | Soft-deleted edges in the active memtable. |
| last_compaction_ms | `Option<i64>` | `number \| null` | `int \| None` | Unix timestamp (ms) of the last completed compaction, or null if none. |
| wal_sync_mode | `String` | `string` | `str` | `"immediate"` or `"group_commit"`. |
| active_memtable_bytes | `usize` | `number` | `int` | Estimated byte size of the active (writable) memtable. |
| immutable_memtable_bytes | `usize` | `number` | `int` | Estimated byte size of all sealed memtables waiting to flush. |
| immutable_memtable_count | `usize` | `number` | `int` | Count of sealed memtables waiting to flush. |
| pending_flush_count | `usize` | `number` | `int` | Flush operations currently in flight. |
| active_wal_generation_id | `u64` | `number` | `int` | Generation ID of the WAL file currently being written. |
| oldest_retained_wal_generation_id | `u64` | `number` | `int` | Oldest WAL generation kept on disk (needed for crash recovery). |

---

## Configuration

### DbOptions

Options passed to [`open()`](#open). All fields are optional with sensible defaults.

| Option | Rust type | Node.js key | Python kwarg | Default | Description |
|--------|-----------|-------------|--------------|---------|-------------|
| create_if_missing | `bool` | `createIfMissing` | `create_if_missing` | `true` | Create the database directory if it doesn't exist. If `false` and the directory is missing, `open()` returns an error. |
| wal_sync_mode | `WalSyncMode` | `walSyncMode` | `wal_sync_mode` | `GroupCommit` | Controls WAL durability. See [WalSyncMode](#walsyncmode). |
| group_commit_interval_ms | — (part of enum) | `groupCommitIntervalMs` | `group_commit_interval_ms` | `50` | Milliseconds between group-commit fsyncs. Only applies when `wal_sync_mode` is `group_commit`. |
| memtable_flush_threshold | `usize` | `memtableFlushThreshold` | `memtable_flush_threshold` | `134217728` (128 MB) | When the active memtable exceeds this size in bytes, it is sealed and queued for flush to a segment. |
| memtable_hard_cap_bytes | `usize` | `memtableHardCapBytes` | `memtable_hard_cap_bytes` | `536870912` (512 MB) | Writes block when the active memtable exceeds this size and the flush queue is full. Prevents unbounded memory growth under heavy write load. Set to `0` to disable. |
| max_immutable_memtables | `usize` | `maxImmutableMemtables` | `max_immutable_memtables` | `4` | Maximum number of sealed memtables allowed before the flush thread must drain one. Controls memory usage under write bursts. |
| edge_uniqueness | `bool` | `edgeUniqueness` | `edge_uniqueness` | `false` | When `true`, `upsert_edge` enforces at most one edge per `(from, to, type_id)` triple. An upsert with the same triple updates the existing edge. When `false`, every `upsert_edge` call creates a new edge. |
| compact_after_n_flushes | `u32` | `compactAfterNFlushes` | `compact_after_n_flushes` | `3` | Trigger background compaction after this many flushes. Set to `0` to disable auto-compaction. |
| dense_vector | `Option<DenseVectorConfig>` | `denseVector` | See below | `None` | Enable dense vector search. See [DenseVectorConfig](#densevectorconfig). In Python, use separate kwargs: `dense_vector_dimension` and `dense_vector_metric`. |

### WalSyncMode

Controls the trade-off between durability and write throughput.

| Mode | Rust | Node.js | Python | Behavior |
|------|------|---------|--------|----------|
| Immediate | `WalSyncMode::Immediate` | `"immediate"` | `"immediate"` | Every write triggers an `fsync`. Maximum crash safety. Data is durable before the write call returns. Lower throughput (~4ms per write on typical SSDs). |
| GroupCommit | `WalSyncMode::GroupCommit { .. }` | `"group-commit"` | `"group_commit"` | Writes are buffered and fsynced on a timer or when the buffer fills. Higher throughput (batched fsync amortizes the cost across many writes). A crash can lose at most one group-commit interval of writes. |

**GroupCommit parameters** (Rust only; Node.js/Python expose these as top-level options):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| interval_ms | `u32` | `50` | Maximum time between fsyncs. |
| soft_trigger_bytes | `usize` | `2097152` (2 MB) | Trigger an fsync when buffered bytes reach this threshold, even before the interval fires. |
| hard_cap_bytes | `usize` | `16777216` (16 MB) | Maximum WAL buffer size. Writes block if the buffer reaches this limit before the background syncer drains it. |

### DenseVectorConfig

Configures the HNSW index for dense vector search. Set once at database creation; cannot be changed later.

| Parameter | Rust | Node.js | Python | Default | Description |
|-----------|------|---------|--------|---------|-------------|
| dimension | `u32` | `dimension: number` | `dense_vector_dimension: int` | — (required if enabling vectors) | Dimensionality of dense vectors. Every node's `dense_vector` must have exactly this many elements. |
| metric | `DenseMetric` | `metric: string` | `dense_vector_metric: str` | `Cosine` | Distance metric for similarity. One of: `Cosine`, `Euclidean`, `DotProduct`. |

**DenseMetric values:**

| Metric | Description | Score semantics |
|--------|-------------|-----------------|
| `Cosine` | Cosine similarity. Vectors are L2-normalized before comparison. | Higher = more similar (range: -1 to 1). |
| `Euclidean` | L2 (Euclidean) distance. | Lower = more similar (range: 0 to ∞). Results are returned as negative distance so higher scores remain "better." |
| `DotProduct` | Raw inner product. Useful when vectors are already normalized or when magnitude matters. | Higher = more similar. |

**HNSW parameters** (Rust only; Node.js/Python use defaults):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| m | `usize` | `16` | Maximum number of bi-directional links per node per layer. Higher values improve recall at the cost of memory and build time. |
| ef_construction | `usize` | `200` | Size of the dynamic candidate list during index construction. Higher values improve recall at the cost of slower inserts. |

---

## Data Model

### NodeRecord

A node stored in the graph. Returned by read operations.

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| id | `u64` | `number` | `int` | Unique, auto-assigned node ID. Monotonically increasing. |
| type_id | `u32` | `number` | `int` | User-defined type identifier. Use constants (e.g., `USER = 1`) for readability. |
| key | `String` | `string` | `str` | Unique key within the type. The `(type_id, key)` pair uniquely identifies a node. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | User-defined properties. See [PropValue](#propvalue) for supported types. Lazily deserialized from MessagePack on first access. |
| weight | `f32` | `number` | `float` | Numeric weight. Default `1.0`. Used by pruning policies and scoring algorithms. |
| created_at | `i64` | `number` | `int` | Timestamp (ms) when the node was first created. |
| updated_at | `i64` | `number` | `int` | Timestamp (ms) of the most recent upsert. |
| dense_vector | `Option<DenseVector>` | — | — | Dense vector (Rust only). Node.js and Python access vectors through vector search, not direct record access. |
| sparse_vector | `Option<SparseVector>` | — | — | Sparse vector (Rust only). |

### EdgeRecord

An edge (relationship) stored in the graph.

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| id | `u64` | `number` | `int` | Unique, auto-assigned edge ID. |
| from / from_id | `u64` | `from: number` | `from_id: int` | Source node ID. |
| to / to_id | `u64` | `to: number` | `to_id: int` | Destination node ID. |
| type_id | `u32` | `number` | `int` | User-defined edge type. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | User-defined properties. |
| weight | `f32` | `number` | `float` | Edge weight. Default `1.0`. |
| valid_from | `i64` | `number` | `int` | Start of the edge's validity window (ms). `0` means "always valid from the beginning of time." |
| valid_to | `i64` | `number` | `int` | End of the edge's validity window (ms). `i64::MAX` means "no expiration." |
| created_at | `i64` | `number` | `int` | Creation timestamp (ms). |
| updated_at | `i64` | `number` | `int` | Last update timestamp (ms). |

### PropValue

Property values are strongly typed. The following types are supported across all three languages:

| Type | Rust | Node.js | Python | Notes |
|------|------|---------|--------|-------|
| Null | `PropValue::Null` | `null` | `None` | |
| Boolean | `PropValue::Bool(bool)` | `boolean` | `bool` | |
| Integer | `PropValue::Int(i64)` | `number` | `int` | 64-bit signed. |
| Unsigned | `PropValue::UInt(u64)` | `number` | `int` | 64-bit unsigned. |
| Float | `PropValue::Float(f64)` | `number` | `float` | 64-bit IEEE 754. |
| String | `PropValue::String(String)` | `string` | `str` | UTF-8. |
| Bytes | `PropValue::Bytes(Vec<u8>)` | `Buffer` | `bytes` | Raw byte data. |
| Array | `PropValue::Array(Vec<PropValue>)` | `any[]` | `list` | Heterogeneous array. |
| Map | `PropValue::Map(BTreeMap<String, PropValue>)` | `object` | `dict` | Nested properties. |

Properties are encoded with [MessagePack](https://msgpack.org) internally and converted lazily when accessed from Node.js or Python.

### Direction

Controls edge traversal direction. Used across neighbor queries, traversal, pathfinding, and analytics.

| Value | Rust | Node.js | Python | Meaning |
|-------|------|---------|--------|---------|
| Outgoing | `Direction::Outgoing` | `"outgoing"` | `"outgoing"` | Follow edges in the `from → to` direction. |
| Incoming | `Direction::Incoming` | `"incoming"` | `"incoming"` | Follow edges in the `to → from` direction. |
| Both | `Direction::Both` | `"both"` | `"both"` | Follow edges in both directions (treat graph as undirected). |

---

## Node Operations

### upsert_node

Creates a new node or updates an existing one. Nodes are identified by the `(type_id, key)` pair. If a node with the same type and key already exists, it is updated in place (preserving its ID).

**Rust**
```rust
let id = db.upsert_node(USER, "alice", UpsertNodeOptions {
    props: BTreeMap::from([("role".into(), PropValue::String("admin".into()))]),
    weight: 1.0,
    ..Default::default()
})?;
```

**Node.js**
```javascript
const id = db.upsertNode(USER, 'alice', {
  props: { role: 'admin' },
  weight: 1.0,
});
```

**Python**
```python
id = db.upsert_node(USER, "alice", props={"role": "admin"}, weight=1.0)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| type_id | `u32` | `number` | `int` | Yes | — | User-defined type identifier. Arbitrary integer (0–4,294,967,295). Define as constants for readability. |
| key | `&str` | `string` | `str` | Yes | — | Unique key within the type. The `(type_id, key)` pair is the node's identity. If a node with this pair exists, it is updated. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | No | `{}` | Arbitrary key-value properties. On update, the entire props map is replaced (not merged). |
| weight | `f32` | `number` | `float` | No | `1.0` | Numeric weight. Used by pruning policies (`max_weight`) and scoring algorithms. |
| dense_vector | `Option<Vec<f32>>` | `number[]` | `list[float]` | No | `None` | Dense vector for similarity search. Length must match the `dimension` configured at `open()`. Requires `dense_vector` to be enabled in DbOptions. |
| sparse_vector | `Option<Vec<(u32, f32)>>` | `SparseEntry[]` | `list[tuple[int, float]]` | No | `None` | Sparse vector as `(dimension_index, value)` pairs. Dimension indices must be unique. No upfront dimension configuration required. |

**SparseEntry** (Node.js):
```typescript
{ dimension: number, value: number }
```

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u64, EngineError>` | `number` | `int` |

The node's ID. If the node was newly created, this is a fresh ID. If the node already existed, this is the existing ID.

#### Behavior

- **Upsert semantics**: On insert, allocates a new ID, sets `created_at` and `updated_at` to the current time. On update, keeps the original `created_at`, refreshes `updated_at`, and replaces all fields (props, weight, vectors).
- **Atomicity**: The write is applied to the WAL and memtable in a single operation.
- **Performance**: ~4ms per call in `Immediate` sync mode (dominated by `fsync`). Use [`batch_upsert_nodes`](#batch_upsert_nodes) for bulk operations where a single fsync is shared across the batch.

---

### get_node

Retrieves a node by its ID.

**Rust**
```rust
if let Some(node) = db.get_node(id)? {
    println!("key={}, type={}", node.key, node.type_id);
}
```

**Node.js**
```javascript
const node = db.getNode(id);
if (node) console.log(node.key, node.typeId);
```

**Python**
```python
node = db.get_node(id)
if node:
    print(node.key, node.type_id)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Node ID to retrieve. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Option<NodeRecord>, EngineError>` | `NodeRecord \| null` | `NodeRecord \| None` |

Returns `None`/`null` if the node does not exist or has been deleted.

#### Performance

~38ns per lookup (memtable hot path). Segment reads require I/O but are mmap-accelerated.

---

### get_node_by_key

Looks up a node by its `(type_id, key)` pair. Uses the type index for fast lookup.

**Rust**
```rust
let node = db.get_node_by_key(USER, "alice")?;
```

**Node.js**
```javascript
const node = db.getNodeByKey(USER, 'alice');
```

**Python**
```python
node = db.get_node_by_key(USER, "alice")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| type_id | `u32` | `number` | `int` | Yes | Node type identifier. |
| key | `&str` | `string` | `str` | Yes | Node key within the type. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Option<NodeRecord>, EngineError>` | `NodeRecord \| null` | `NodeRecord \| None` |

Returns `None`/`null` if no node with that `(type_id, key)` exists.

---

### delete_node

Deletes a node by ID. **Cascade-deletes all incident edges** (both incoming and outgoing) in the same WAL batch.

**Rust**
```rust
db.delete_node(id)?;
```

**Node.js**
```javascript
db.deleteNode(id);
```

**Python**
```python
db.delete_node(id)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Node ID to delete. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<(), EngineError>` | `void` | `None` |

#### Behavior

- Writes tombstones for the node and all its incident edges (memtable + segments scanned) in a single WAL batch for atomicity.
- Tombstoned records are excluded from all subsequent reads.
- Tombstones are physically removed during [compaction](#compact).
- Deleting a nonexistent or already-deleted node is a no-op (idempotent).

---

### batch_upsert_nodes

Upserts multiple nodes in a single batch with one WAL fsync. Significantly faster than calling `upsert_node` in a loop.

**Rust**
```rust
let inputs = vec![
    NodeInput { type_id: USER, key: "alice".into(), weight: 1.0, ..Default::default() },
    NodeInput { type_id: USER, key: "bob".into(), weight: 0.8, ..Default::default() },
];
let ids = db.batch_upsert_nodes(&inputs)?;
```

**Node.js**
```javascript
const ids = db.batchUpsertNodes([
  { typeId: USER, key: 'alice', weight: 1.0 },
  { typeId: USER, key: 'bob', weight: 0.8, props: { role: 'viewer' } },
]);
// ids is a Float64Array
```

**Python**
```python
ids = db.batch_upsert_nodes([
    {"type_id": USER, "key": "alice", "weight": 1.0},
    {"type_id": USER, "key": "bob", "weight": 0.8, "props": {"role": "viewer"}},
])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| nodes | `&[NodeInput]` | `NodeInput[]` | `list[dict]` | Yes | Array of node inputs. Each element has the same fields as [`upsert_node`](#upsert_node) parameters. |

**NodeInput fields:**

| Field | Rust | Node.js | Python dict key | Required | Default | Description |
|-------|------|---------|-----------------|----------|---------|-------------|
| type_id | `u32` | `typeId: number` | `"type_id"` | Yes | — | Node type. |
| key | `String` | `key: string` | `"key"` | Yes | — | Node key. |
| props | `BTreeMap<String, PropValue>` | `props: object` | `"props"` | No | `{}` | Properties. |
| weight | `f32` | `weight: number` | `"weight"` | No | `1.0` | Weight. |
| dense_vector | `Option<Vec<f32>>` | `denseVector: number[]` | `"dense_vector"` | No | `None` | Dense vector. |
| sparse_vector | `Option<Vec<(u32, f32)>>` | `sparseVector: SparseEntry[]` | `"sparse_vector"` | No | `None` | Sparse vector. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `list[int]` |

An array of node IDs in the same order as the input array.

#### Performance

A single fsync is performed for the entire batch. At 100 nodes, this achieves ~46μs per node amortized (vs. ~4ms per node for individual calls). Use this for all bulk operations.

---

### get_nodes

Batch-retrieves multiple nodes by ID. Uses a sorted merge-walk across all data sources, **much faster than calling `get_node` in a loop**.

**Rust**
```rust
let nodes = db.get_nodes(&[1, 2, 3])?;
// nodes[0] is Option<NodeRecord> for ID 1, etc.
```

**Node.js**
```javascript
const nodes = db.getNodes([1, 2, 3]);
// nodes[0] is NodeRecord | null for ID 1, etc.
```

**Python**
```python
nodes = db.get_nodes([1, 2, 3])
# nodes[0] is NodeRecord | None for ID 1, etc.
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| ids | `&[u64]` | `number[]` | `list[int]` | Yes | Array of node IDs to fetch. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<Option<NodeRecord>>, EngineError>` | `(NodeRecord \| null)[]` | `list[NodeRecord \| None]` |

An array the same length as the input, where each element is the node record or `None`/`null` if that ID doesn't exist.

---

### get_nodes_by_keys

Batch-retrieves multiple nodes by `(type_id, key)` pairs.

**Rust**
```rust
let nodes = db.get_nodes_by_keys(&[(USER, "alice".into()), (USER, "bob".into())])?;
```

**Node.js**
```javascript
const nodes = db.getNodesByKeys([
  { typeId: USER, key: 'alice' },
  { typeId: USER, key: 'bob' },
]);
```

**Python**
```python
nodes = db.get_nodes_by_keys([(USER, "alice"), (USER, "bob")])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| keys | `&[(u32, String)]` | `KeyQuery[]` | `list[tuple[int, str]]` | Yes | Array of `(type_id, key)` pairs. |

**KeyQuery** (Node.js):
```typescript
{ typeId: number, key: string }
```

#### Returns

Same shape as [`get_nodes`](#get_nodes): an array of `NodeRecord | None` in input order.

---

## Edge Operations

### upsert_edge

Creates a new edge or updates an existing one. When `edge_uniqueness` is enabled, edges are identified by the `(from, to, type_id)` triple.

**Rust**
```rust
let id = db.upsert_edge(alice_id, project_id, WORKS_ON, UpsertEdgeOptions {
    props: BTreeMap::from([("since".into(), PropValue::String("2024".into()))]),
    weight: 1.0,
    ..Default::default()
})?;
```

**Node.js**
```javascript
const id = db.upsertEdge(aliceId, projectId, WORKS_ON, {
  props: { since: '2024' },
  weight: 1.0,
});
```

**Python**
```python
id = db.upsert_edge(alice_id, project_id, WORKS_ON,
    props={"since": "2024"}, weight=1.0)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| from | `u64` | `number` | `int` | Yes | — | Source node ID. |
| to | `u64` | `number` | `int` | Yes | — | Destination node ID. |
| type_id | `u32` | `number` | `int` | Yes | — | Edge type identifier. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | No | `{}` | Edge properties. Replaced entirely on update. |
| weight | `f32` | `number` | `float` | No | `1.0` | Edge weight. Used by shortest path (as cost), top-k scoring, and pruning. |
| valid_from | `Option<i64>` | `number` | `int` | No | `0` (always valid) | Start of the edge's temporal validity window (ms). Edges with `valid_from > at_epoch` are excluded from temporal queries. |
| valid_to | `Option<i64>` | `number` | `int` | No | `i64::MAX` (no expiration) | End of the validity window (ms). Edges with `valid_to <= at_epoch` are excluded from temporal queries. See [`invalidate_edge`](#invalidate_edge). |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u64, EngineError>` | `number` | `int` |

The edge ID.

#### Behavior

- **With `edge_uniqueness` enabled**: If an edge with the same `(from, to, type_id)` exists, it is updated and the existing ID is returned. Otherwise a new edge is created.
- **With `edge_uniqueness` disabled** (default): Every call creates a new edge (parallel edges are allowed).

---

### get_edge

Retrieves an edge by ID.

**Rust**
```rust
let edge = db.get_edge(edge_id)?;
```

**Node.js**
```javascript
const edge = db.getEdge(edgeId);
```

**Python**
```python
edge = db.get_edge(edge_id)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Edge ID. |

#### Returns

`EdgeRecord | None`. Returns `None`/`null` if the edge doesn't exist or has been deleted.

---

### get_edge_by_triple

Looks up an edge by its `(from, to, type_id)` triple. Only meaningful when `edge_uniqueness` is enabled.

**Rust**
```rust
let edge = db.get_edge_by_triple(alice_id, project_id, WORKS_ON)?;
```

**Node.js**
```javascript
const edge = db.getEdgeByTriple(aliceId, projectId, WORKS_ON);
```

**Python**
```python
edge = db.get_edge_by_triple(alice_id, project_id, WORKS_ON)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| from | `u64` | `number` | `int` | Yes | Source node ID. |
| to | `u64` | `number` | `int` | Yes | Destination node ID. |
| type_id | `u32` | `number` | `int` | Yes | Edge type. |

#### Returns

`EdgeRecord | None`.

---

### delete_edge

Deletes an edge by ID.

**Rust**
```rust
db.delete_edge(edge_id)?;
```

**Node.js**
```javascript
db.deleteEdge(edgeId);
```

**Python**
```python
db.delete_edge(edge_id)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Edge ID to delete. |

#### Behavior

- Writes a tombstone. Idempotent: deleting a nonexistent or already-deleted edge is a no-op.
- Tombstones are reclaimed during [compaction](#compact).

---

### invalidate_edge

Closes an edge's validity window by setting its `valid_to` timestamp. The edge remains in the database (not tombstoned) but is excluded from queries that use temporal filtering (`at_epoch`).

**Rust**
```rust
let updated = db.invalidate_edge(edge_id, now_ms)?;
```

**Node.js**
```javascript
const updated = db.invalidateEdge(edgeId, Date.now());
```

**Python**
```python
updated = db.invalidate_edge(edge_id, int(time.time() * 1000))
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Edge ID. |
| valid_to | `i64` | `number` | `int` | Yes | New end-of-validity timestamp (ms). The edge is considered expired for any `at_epoch >= valid_to`. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Option<EdgeRecord>, EngineError>` | `EdgeRecord \| null` | `EdgeRecord \| None` |

The updated edge record, or `None`/`null` if the edge doesn't exist.

#### Use Case

Temporal graphs: rather than hard-deleting edges, close their validity window. This preserves historical data while excluding expired edges from current queries:

```javascript
// Only returns edges valid at the given timestamp
const neighbors = db.neighbors(nodeId, { atEpoch: Date.now() });
```

---

### batch_upsert_edges

Upserts multiple edges in a single batch with one WAL fsync.

**Rust**
```rust
let inputs = vec![
    EdgeInput { from: 1, to: 2, type_id: WORKS_ON, weight: 1.0, ..Default::default() },
    EdgeInput { from: 1, to: 3, type_id: WORKS_ON, weight: 0.5, ..Default::default() },
];
let ids = db.batch_upsert_edges(&inputs)?;
```

**Node.js**
```javascript
const ids = db.batchUpsertEdges([
  { from: 1, to: 2, typeId: WORKS_ON, weight: 1.0 },
  { from: 1, to: 3, typeId: WORKS_ON, weight: 0.5 },
]);
```

**Python**
```python
ids = db.batch_upsert_edges([
    {"from_id": 1, "to_id": 2, "type_id": WORKS_ON, "weight": 1.0},
    {"from_id": 1, "to_id": 3, "type_id": WORKS_ON, "weight": 0.5},
])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| edges | `&[EdgeInput]` | `EdgeInput[]` | `list[dict]` | Yes | Array of edge inputs. |

**EdgeInput fields:**

| Field | Rust | Node.js | Python dict key | Required | Default | Description |
|-------|------|---------|-----------------|----------|---------|-------------|
| from | `u64` | `from: number` | `"from_id"` | Yes | — | Source node ID. |
| to | `u64` | `to: number` | `"to_id"` | Yes | — | Destination node ID. |
| type_id | `u32` | `typeId: number` | `"type_id"` | Yes | — | Edge type. |
| props | `BTreeMap<String, PropValue>` | `props: object` | `"props"` | No | `{}` | Properties. |
| weight | `f32` | `weight: number` | `"weight"` | No | `1.0` | Weight. |
| valid_from | `Option<i64>` | `validFrom: number` | `"valid_from"` | No | `0` | Validity start. |
| valid_to | `Option<i64>` | `validTo: number` | `"valid_to"` | No | `i64::MAX` | Validity end. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `list[int]` |

Edge IDs in input order.

---

### get_edges

Batch-retrieves multiple edges by ID using a sorted merge-walk.

**Rust**
```rust
let edges = db.get_edges(&[10, 20, 30])?;
```

**Node.js**
```javascript
const edges = db.getEdges([10, 20, 30]);
```

**Python**
```python
edges = db.get_edges([10, 20, 30])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| ids | `&[u64]` | `number[]` | `list[int]` | Yes | Edge IDs to fetch. |

#### Returns

Array of `EdgeRecord | None` in input order.

---

## Atomic Operations

### graph_patch

Applies multiple operations atomically in a single WAL batch: node upserts, edge upserts, edge invalidations, and deletes.

**Rust**
```rust
let result = db.graph_patch(&GraphPatch {
    upsert_nodes: vec![NodeInput { type_id: USER, key: "carol".into(), ..Default::default() }],
    upsert_edges: vec![EdgeInput { from: 1, to: 2, type_id: WORKS_ON, ..Default::default() }],
    invalidate_edges: vec![(edge_id, now_ms)],
    delete_node_ids: vec![old_node_id],
    delete_edge_ids: vec![old_edge_id],
})?;
```

**Node.js**
```javascript
const result = db.graphPatch({
  upsertNodes: [{ typeId: USER, key: 'carol' }],
  upsertEdges: [{ from: 1, to: 2, typeId: WORKS_ON }],
  invalidateEdges: [{ edgeId: 5, validTo: Date.now() }],
  deleteNodeIds: [oldNodeId],
  deleteEdgeIds: [oldEdgeId],
});
```

**Python**
```python
result = db.graph_patch({
    "upsert_nodes": [{"type_id": USER, "key": "carol"}],
    "upsert_edges": [{"from_id": 1, "to_id": 2, "type_id": WORKS_ON}],
    "invalidate_edges": [{"edge_id": 5, "valid_to": int(time.time() * 1000)}],
    "delete_node_ids": [old_node_id],
    "delete_edge_ids": [old_edge_id],
})
```

#### Parameters

All fields in the patch object are optional. Omit any you don't need.

| Field | Rust | Node.js | Python dict key | Description |
|-------|------|---------|-----------------|-------------|
| upsert_nodes | `Vec<NodeInput>` | `upsertNodes: NodeInput[]` | `"upsert_nodes"` | Nodes to create or update. Same format as [`batch_upsert_nodes`](#batch_upsert_nodes). |
| upsert_edges | `Vec<EdgeInput>` | `upsertEdges: EdgeInput[]` | `"upsert_edges"` | Edges to create or update. Same format as [`batch_upsert_edges`](#batch_upsert_edges). |
| invalidate_edges | `Vec<(u64, i64)>` | `invalidateEdges: {edgeId, validTo}[]` | `"invalidate_edges"` | Edges to invalidate. Each entry specifies an edge ID and a `valid_to` timestamp. |
| delete_node_ids | `Vec<u64>` | `deleteNodeIds: number[]` | `"delete_node_ids"` | Node IDs to delete. **Cascade**: incident edges are automatically deleted. |
| delete_edge_ids | `Vec<u64>` | `deleteEdgeIds: number[]` | `"delete_edge_ids"` | Edge IDs to delete. |

#### Execution Order

Operations within a patch are applied in a deterministic order:

1. **Node upserts** - create/update nodes (so new nodes can be referenced by edge upserts)
2. **Edge upserts** - create/update edges
3. **Edge invalidations** - set `valid_to` on edges
4. **Edge deletes** - tombstone edges
5. **Node deletes** - tombstone nodes and cascade-delete all incident edges

#### Returns: PatchResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_ids | `Vec<u64>` | `Float64Array` | `list[int]` | IDs of all upserted nodes, in input order. |
| edge_ids | `Vec<u64>` | `Float64Array` | `list[int]` | IDs of all upserted edges, in input order. |

---

## Type-Based Queries

### nodes_by_type

Returns all node IDs of a given type.

**Rust**
```rust
let ids: Vec<u64> = db.nodes_by_type(USER)?;
```

**Node.js**
```javascript
const ids = db.nodesByType(USER); // Float64Array
```

**Python**
```python
ids = db.nodes_by_type(USER)  # IdArray (lazy)
ids_list = ids.to_list()      # materialize to list[int]
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| type_id | `u32` | `number` | `int` | Yes | Node type to query. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

All node IDs of the given type. Filtered (excludes deleted/pruned nodes).

**Python `IdArray`**: A lazy wrapper that avoids copying IDs to Python memory until accessed. Supports `len()`, indexing (`arr[i]`), iteration, `in` operator, and `to_list()`.

#### Performance

O(type index size), not O(total nodes). Uses the per-type index.

---

### edges_by_type

Returns all edge IDs of a given type.

```rust
let ids = db.edges_by_type(WORKS_ON)?;
```

```javascript
const ids = db.edgesByType(WORKS_ON);
```

```python
ids = db.edges_by_type(WORKS_ON)
```

Same signature pattern as [`nodes_by_type`](#nodes_by_type). Filtered (excludes dangling edges from deleted nodes).

---

### get_nodes_by_type

Returns full node records for all nodes of a given type.

```rust
let nodes: Vec<NodeRecord> = db.get_nodes_by_type(USER)?;
```

```javascript
const nodes = db.getNodesByType(USER); // NodeRecord[]
```

```python
nodes = db.get_nodes_by_type(USER)  # list[NodeRecord]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type_id | `u32` / `number` / `int` | Yes | Node type. |

#### Returns

Array of full `NodeRecord` objects. Includes all fields (id, key, props, weight, timestamps, vectors).

---

### get_edges_by_type

Returns full edge records for all edges of a given type. Same pattern as `get_nodes_by_type`.

---

### count_nodes_by_type

Returns the count of nodes of a given type. More efficient than fetching all IDs and measuring the length.

```rust
let count: u64 = db.count_nodes_by_type(USER)?;
```

```javascript
const count = db.countNodesByType(USER);
```

```python
count = db.count_nodes_by_type(USER)
```

---

### count_edges_by_type

Returns the count of edges of a given type. Same pattern as `count_nodes_by_type`.

---

## Property & Time Queries

### find_nodes

Finds all nodes of a given type where a specific property matches a given value (exact match).

**Rust**
```rust
let ids = db.find_nodes(&FindNodesQuery {
    type_id: USER,
    prop_key: "role".into(),
    prop_value: PropValue::String("admin".into()),
})?;
```

**Node.js**
```javascript
const ids = db.findNodes(USER, 'role', 'admin'); // Float64Array
```

**Python**
```python
ids = db.find_nodes(USER, "role", "admin")  # IdArray
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| type_id | `u32` | `number` | `int` | Yes | Restrict search to this node type. |
| prop_key | `String` | `string` | `str` | Yes | Property key to match on. |
| prop_value | `PropValue` | `any` | `Any` | Yes | Exact value to match. Type must match (string "1" does not match integer 1). |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

Matching node IDs. Uses the property index for acceleration.

---

### find_nodes_by_time_range

Finds all nodes of a given type with `updated_at` within a time range.

```rust
let ids = db.find_nodes_by_time_range(USER, start_ms, end_ms)?;
```

```javascript
const ids = db.findNodesByTimeRange(USER, startMs, endMs);
```

```python
ids = db.find_nodes_by_time_range(USER, start_ms, end_ms)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| type_id | `u32` | `number` | `int` | Yes | Node type. |
| from_ms | `i64` | `number` | `int` | Yes | Start of range (inclusive), ms since epoch. |
| to_ms | `i64` | `number` | `int` | Yes | End of range (inclusive), ms since epoch. |

#### Returns

Node IDs matching the time range. Uses the timestamp index.

---

## Pagination

All paginated methods use **keyset (cursor-based) pagination**, not offset-based. This provides stable results even when data is inserted between pages.

The pattern is the same across all paginated methods:
- Pass `limit` for the page size and `after` as the cursor (a node or edge ID).
- The result includes `items` (the page data) and `next_cursor` (`None`/`null` when there are no more pages).

### nodes_by_type_paged

Paginated version of [`nodes_by_type`](#nodes_by_type). Returns IDs only.

```rust
let page = db.nodes_by_type_paged(USER, &PageRequest { limit: Some(100), after: None })?;
// page.items: Vec<u64>, page.next_cursor: Option<u64>
```

```javascript
let page = db.nodesByTypePaged(USER, 100); // limit=100, no cursor
// page = { items: Float64Array, nextCursor: number | null }

// Next page:
page = db.nodesByTypePaged(USER, 100, page.nextCursor);
```

```python
page = db.nodes_by_type_paged(USER, limit=100)
# page.items: IdArray, page.next_cursor: int | None

# Next page:
page = db.nodes_by_type_paged(USER, limit=100, after=page.next_cursor)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| type_id | `u32` | `number` | `int` | Yes | — | Node type. |
| limit | `Option<usize>` | `number` | `int` | No | Unlimited | Maximum items per page. |
| after | `Option<u64>` | `number` | `int` | No | `None` (start from beginning) | Cursor. Returns items with IDs strictly greater than this value. Use `next_cursor` from a previous result. |

#### Returns: PageResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<u64>` | `Float64Array` | `IdArray` | IDs in this page. |
| next_cursor | `Option<u64>` | `number \| null` | `int \| None` | Cursor for the next page. `None`/`null` means this is the last page. |

---

### edges_by_type_paged

Paginated version of [`edges_by_type`](#edges_by_type). Same pattern as `nodes_by_type_paged`.

---

### get_nodes_by_type_paged

Paginated version of [`get_nodes_by_type`](#get_nodes_by_type). Returns full `NodeRecord` objects.

```rust
let page = db.get_nodes_by_type_paged(USER, &PageRequest { limit: Some(50), after: None })?;
// page.items: Vec<NodeRecord>
```

```javascript
const page = db.getNodesByTypePaged(USER, 50);
// page.items: NodeRecord[]
```

```python
page = db.get_nodes_by_type_paged(USER, limit=50)
# page.items: list[NodeRecord]
```

---

### get_edges_by_type_paged

Paginated version of [`get_edges_by_type`](#get_edges_by_type). Returns full `EdgeRecord` objects.

---

### find_nodes_paged

Paginated version of [`find_nodes`](#find_nodes).

```rust
let page = db.find_nodes_paged(
    &FindNodesQuery { type_id: USER, prop_key: "role".into(), prop_value: PropValue::String("admin".into()) },
    &PageRequest { limit: Some(50), after: None },
)?;
```

```javascript
const page = db.findNodesPaged(USER, 'role', 'admin', { limit: 50 });
```

```python
page = db.find_nodes_paged(USER, "role", "admin", limit=50)
```

---

### find_nodes_by_time_range_paged

Paginated version of [`find_nodes_by_time_range`](#find_nodes_by_time_range).

```javascript
const page = db.findNodesByTimeRangePaged(USER, startMs, endMs, { limit: 50 });
```

```python
page = db.find_nodes_by_time_range_paged(USER, start_ms, end_ms, limit=50)
```

---

## Neighbor Queries

### neighbors

Retrieves the immediate neighbors of a node (one hop). The most common graph query operation.

**Rust**
```rust
let entries = db.neighbors(node_id, &NeighborOptions {
    direction: Direction::Outgoing,
    type_filter: Some(vec![WORKS_ON]),
    limit: Some(10),
    at_epoch: None,
    decay_lambda: None,
})?;

for entry in &entries {
    println!("neighbor={}, edge={}, weight={}", entry.node_id, entry.edge_id, entry.weight);
}
```

**Node.js**
```javascript
const list = db.neighbors(nodeId, {
  direction: 'outgoing',
  typeFilter: [WORKS_ON],
  limit: 10,
});

for (let i = 0; i < list.length; i++) {
  console.log(list.nodeId(i), list.edgeId(i), list.weight(i));
}
// Or materialize all at once:
const arr = list.toArray();
```

**Python**
```python
entries = db.neighbors(node_id, direction="outgoing", type_filter=[WORKS_ON], limit=10)
for entry in entries:
    print(entry.node_id, entry.edge_id, entry.weight)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_id | `u64` | `number` | `int` | Yes | — | Node to query neighbors for. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Traversal direction. `"outgoing"`, `"incoming"`, or `"both"`. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` (all types) | Only return neighbors connected by edges of these types. |
| limit | `Option<usize>` | `number` | `int` | No | `None` (unlimited) | Maximum number of neighbors to return. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` (current time) | Temporal filter. Only edges whose validity window contains this timestamp are included. `None` means the current wall-clock time. |
| decay_lambda | `Option<f32>` | `number` | `float` | No | `None` (no decay) | Exponential decay factor. When set, each neighbor's weight is multiplied by `exp(-λ × age_ms)` where `age_ms` is the edge's age. Produces a time-decayed relevance score. |

#### Returns: NeighborEntry

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_id | `u64` | `number` | `int` | ID of the neighboring node. |
| edge_id | `u64` | `number` | `int` | ID of the connecting edge. |
| edge_type_id | `u32` | `number` | `int` | Type of the connecting edge. |
| weight | `f32` | `number` | `float` | Edge weight (or decay-adjusted score if `decay_lambda` is set). |
| valid_from | `i64` | `number` | `int` | Edge validity start (ms). |
| valid_to | `i64` | `number` | `int` | Edge validity end (ms). |

**Node.js `JsNeighborList`**: A lazy wrapper for performance. Access individual fields by index (`list.nodeId(i)`, `list.weight(i)`) or materialize with `list.toArray()`. Property `list.length` returns the count.

#### Performance

~294ns for a node with 10 edges, ~2.1μs for 100 edges (memtable hot path).

---

### neighbors_paged

Paginated version of [`neighbors`](#neighbors).

```javascript
let page = db.neighborsPaged(nodeId, { direction: 'outgoing', limit: 20 });
// page.items: JsNeighborList, page.nextCursor: number | null

// Next page:
page = db.neighborsPaged(nodeId, { direction: 'outgoing', limit: 20, after: page.nextCursor });
```

```python
page = db.neighbors_paged(node_id, direction="outgoing", limit=20)
# page.items: list[NeighborEntry], page.next_cursor: int | None

page = db.neighbors_paged(node_id, direction="outgoing", limit=20, after=page.next_cursor)
```

#### Additional Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| limit | `usize` / `number` / `int` | No | Unlimited | Page size. |
| after | `u64` / `number` / `int` | No | `None` | Cursor (edge ID) for the next page. |

---

### neighbors_batch

Batch-queries neighbors for multiple nodes in a single call. More efficient than calling `neighbors` in a loop.

**Rust**
```rust
let results = db.neighbors_batch(&[1, 2, 3], &NeighborOptions::default())?;
// results: NodeIdMap<Vec<NeighborEntry>> - map from node_id to its neighbors
```

**Node.js**
```javascript
const results = db.neighborsBatch([1, 2, 3], { direction: 'outgoing' });
// results: { queryNodeId: number, neighbors: JsNeighborList }[]
```

**Python**
```python
results = db.neighbors_batch([1, 2, 3], direction="outgoing")
# results: dict[int, list[NeighborEntry]]
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| node_ids | `&[u64]` | `number[]` | `list[int]` | Yes | Node IDs to query neighbors for. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Traversal direction. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Edge type filter. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| decay_lambda | `Option<f32>` | `number` | `float` | No | `None` | Decay factor. |

#### Returns

A map/array mapping each query node ID to its list of neighbors.

---

### top_k_neighbors

Returns the top K neighbors of a node ranked by a scoring criterion.

**Rust**
```rust
let top = db.top_k_neighbors(node_id, 5, &TopKOptions {
    direction: Direction::Outgoing,
    scoring: ScoringMode::Weight,
    ..Default::default()
})?;
```

**Node.js**
```javascript
const top = db.topKNeighbors(nodeId, 5, {
  direction: 'outgoing',
  scoring: 'weight',
});
```

**Python**
```python
top = db.top_k_neighbors(node_id, 5, direction="outgoing", scoring="weight")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_id | `u64` | `number` | `int` | Yes | — | Source node. |
| k | `usize` | `number` | `int` | Yes | — | Number of top neighbors to return. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Traversal direction. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Edge type filter. |
| scoring | `ScoringMode` | `string` | `str` | No | `Weight` | Scoring criterion. See below. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| decay_lambda | `Option<f32>` | `number` | `float` | No | `None` | Required when `scoring = "decay"`. |

**Scoring modes:**

| Mode | Rust | Node.js / Python | Description |
|------|------|------------------|-------------|
| Weight | `ScoringMode::Weight` | `"weight"` | Rank by edge weight (descending). |
| RecencyDecay | `ScoringMode::RecencyDecay` | `"recency"` | Rank by recency. More recent edges score higher. |
| — | — | `"decay"` | Exponential decay: `weight × exp(-λ × age_ms)`. Requires `decay_lambda`. |

#### Returns

Array of `NeighborEntry` sorted by score descending. Length is `min(k, actual_neighbor_count)`.

---

## Degree & Weight Aggregation

### degree

Counts the number of edges connected to a node.

```rust
let d = db.degree(node_id, &DegreeOptions::default())?;
```

```javascript
const d = db.degree(nodeId, { direction: 'both' });
```

```python
d = db.degree(node_id, direction="both")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_id | `u64` | `number` | `int` | Yes | — | Node to count edges for. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Which edges to count. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only count edges of these types. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u32, EngineError>` | `number` | `int` |

The edge count.

#### Performance

O(1) for unfiltered, non-temporal queries (cached degree counts). O(edges) when filtering by type or temporal epoch.

---

### degrees

Batch degree query for multiple nodes.

```rust
let map = db.degrees(&[1, 2, 3], &DegreeOptions::default())?;
// map: NodeIdMap<u32>
```

```javascript
const entries = db.degrees([1, 2, 3], { direction: 'outgoing' });
// entries: { nodeId: number, degree: number }[]
```

```python
result = db.degrees([1, 2, 3], direction="outgoing")
# result: dict[int, int]
```

---

### sum_edge_weights

Sums the weights of all edges connected to a node.

```rust
let total = db.sum_edge_weights(node_id, &DegreeOptions::default())?;
```

```javascript
const total = db.sumEdgeWeights(nodeId, { direction: 'outgoing' });
```

```python
total = db.sum_edge_weights(node_id, direction="outgoing")
```

Same parameters as [`degree`](#degree). Returns `f64` / `number` / `float`.

---

### avg_edge_weight

Average weight of edges connected to a node.

```rust
let avg = db.avg_edge_weight(node_id, &DegreeOptions::default())?;
```

```javascript
const avg = db.avgEdgeWeight(nodeId); // number | null
```

```python
avg = db.avg_edge_weight(node_id)  # float | None
```

Same parameters as [`degree`](#degree). Returns `None`/`null` if the node has no edges.

---

## Traversal

### traverse

Breadth-first traversal from a starting node up to a maximum depth. Supports pagination, type filtering, temporal filtering, and decay scoring.

**Rust**
```rust
let result = db.traverse(start_id, &TraverseOptions {
    min_depth: 1,
    direction: Direction::Outgoing,
    edge_type_filter: Some(vec![WORKS_ON]),
    node_type_filter: None,
    at_epoch: None,
    decay_lambda: None,
    limit: Some(100),
    cursor: None,
})?;

for hit in &result {
    println!("node={}, depth={}", hit.node_id, hit.depth);
}
```

**Node.js**
```javascript
const result = db.traverse(startId, 3, {
  minDepth: 1,
  direction: 'outgoing',
  edgeTypeFilter: [WORKS_ON],
  limit: 100,
});

for (const hit of result.items) {
  console.log(hit.nodeId, hit.depth, hit.viaEdgeId);
}

// Paginate:
if (result.nextCursor) {
  const page2 = db.traverse(startId, 3, { cursor: result.nextCursor, limit: 100 });
}
```

**Python**
```python
result = db.traverse(start_id, max_depth=3,
    min_depth=1, direction="outgoing",
    edge_type_filter=[WORKS_ON], limit=100)

for hit in result.items:
    print(hit.node_id, hit.depth, hit.via_edge_id)

# Paginate:
if result.next_cursor:
    page2 = db.traverse(start_id, max_depth=3, cursor=result.next_cursor, limit=100)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| start_node_id | `u64` | `number` | `int` | Yes | — | Starting node for the BFS traversal. |
| max_depth | (part of TraverseOptions in Rust) | `number` | `int` | Yes | — | Maximum number of hops from the start node. `1` = immediate neighbors, `2` = neighbors of neighbors, etc. |
| min_depth | `u32` | `number` | `int` | No | `1` | Minimum depth to include in results. Set to `0` to include the start node itself. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Edge traversal direction. |
| edge_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` (all types) | Only follow edges of these types. |
| node_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` (all types) | Only include nodes of these types in results (edges to other types are still followed). |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter for edge validity. |
| decay_lambda | `Option<f64>` | `number` | `float` | No | `None` | Exponential decay scoring. When set, each hit receives a score: `exp(-λ × Σ age_ms)` accumulated along the path. |
| limit | `Option<usize>` | `number` | `int` | No | `None` (unlimited) | Maximum results per page. Use with `cursor` for pagination. |
| cursor | `Option<TraversalCursor>` | `TraversalCursor` | `TraversalCursor` | No | `None` | Resume traversal from a previous page. |

#### Returns: TraversalHit

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_id | `u64` | `number` | `int` | Node reached by the traversal. |
| depth | `u32` | `number` | `int` | Distance (hops) from the start node. |
| via_edge_id | `Option<u64>` | `number \| null` | `int \| None` | Edge ID used to reach this node. `None` for the start node (depth 0). |
| score | `Option<f64>` | `number \| null` | `float \| None` | Decay-adjusted score (only present when `decay_lambda` is set). |

Results are ordered by `(depth ASC, node_id ASC)` with deterministic tie-breaking.

---

### extract_subgraph

Extracts a complete subgraph (all reachable nodes and edges) rooted at a given node.

**Rust**
```rust
let sg = db.extract_subgraph(root_id, &SubgraphOptions {
    direction: Direction::Outgoing,
    edge_type_filter: None,
    at_epoch: None,
})?;
println!("{} nodes, {} edges", sg.nodes.len(), sg.edges.len());
```

**Node.js**
```javascript
const sg = db.extractSubgraph(rootId, 3, { direction: 'outgoing' });
console.log(sg.nodes.length, 'nodes,', sg.edges.length, 'edges');
```

**Python**
```python
sg = db.extract_subgraph(root_id, max_depth=3, direction="outgoing")
print(len(sg.nodes), "nodes,", len(sg.edges), "edges")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| start_node_id | `u64` | `number` | `int` | Yes | — | Root node. |
| max_depth | — (in SubgraphOptions) | `number` | `int` | Yes | — | Maximum hops from root. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Direction. |
| edge_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Edge type filter. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns: Subgraph

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| nodes | `NodeIdMap<NodeRecord>` | `NodeRecord[]` | `list[NodeRecord]` | All nodes in the subgraph (full records). |
| edges | `Vec<EdgeRecord>` or tuples | `EdgeRecord[]` | `list[EdgeRecord]` | All edges in the subgraph (full records). |

---

## Pathfinding

### shortest_path

Finds the shortest (lowest-cost) path between two nodes.

**Rust**
```rust
let path = db.shortest_path(from_id, to_id, &ShortestPathOptions {
    direction: Direction::Outgoing,
    weight_field: None, // uses edge.weight; set to Some("cost".into()) for property-based cost
    max_depth: Some(10),
    max_cost: Some(100.0),
    ..Default::default()
})?;

if let Some(p) = path {
    println!("path: {:?}, cost: {}", p.nodes, p.cost);
}
```

**Node.js**
```javascript
const path = db.shortestPath(fromId, toId, {
  direction: 'outgoing',
  maxDepth: 10,
  maxCost: 100.0,
});

if (path) {
  console.log('nodes:', path.nodes, 'cost:', path.totalCost);
}
```

**Python**
```python
path = db.shortest_path(from_id, to_id, direction="outgoing", max_depth=10, max_cost=100.0)
if path:
    print("nodes:", path.nodes, "cost:", path.total_cost)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| from | `u64` | `number` | `int` | Yes | — | Source node ID. |
| to | `u64` | `number` | `int` | Yes | — | Destination node ID. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Direction to follow edges. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only traverse these edge types. |
| weight_field | `Option<String>` | `string` | `str` | No | `None` | Property key on edges to use as cost. When `None`, uses `edge.weight`. When set, reads the named property as the edge cost (must be numeric). |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| max_depth | `Option<u32>` | `number` | `int` | No | `None` (unlimited) | Stop searching after this many hops. Prevents runaway searches on deep graphs. |
| max_cost | `Option<f64>` | `number` | `float` | No | `None` (unlimited) | Stop searching when accumulated cost exceeds this threshold. |

#### Algorithm

- When `weight_field` is `None` **and all edge weights are 1.0**: BFS (unweighted shortest path).
- Otherwise: Dijkstra's algorithm (weighted shortest path).

#### Returns: ShortestPath

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| nodes | `Vec<u64>` | `number[]` | `list[int]` | Ordered list of node IDs from source to destination (inclusive). |
| edges | `Vec<u64>` | `number[]` | `list[int]` | Edge IDs along the path. Length = `nodes.length - 1`. |
| cost / total_cost | `f64` | `number` | `float` | Sum of edge costs along the path. |

Returns `None`/`null` if no path exists within the given constraints.

---

### all_shortest_paths

Finds **all** shortest paths (when multiple paths have the same minimum cost).

```rust
let paths = db.all_shortest_paths(from_id, to_id, &AllShortestPathsOptions {
    max_paths: Some(10),
    ..Default::default()
})?;
```

```javascript
const paths = db.allShortestPaths(fromId, toId, { maxPaths: 10 });
```

```python
paths = db.all_shortest_paths(from_id, to_id, max_paths=10)
```

#### Additional Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| max_paths | `Option<usize>` | `number` | `int` | No | `None` (unlimited) | Stop after finding this many paths. Use to prevent combinatorial explosion on highly connected graphs. |

All other parameters are the same as [`shortest_path`](#shortest_path).

#### Returns

Array of `ShortestPath` objects. All paths have the same cost (the minimum). Can be empty if no path exists.

---

### is_connected

Fast reachability check: does any path exist between two nodes? Uses BFS with early termination.

```rust
let connected = db.is_connected(from_id, to_id, &IsConnectedOptions::default())?;
```

```javascript
const connected = db.isConnected(fromId, toId, { direction: 'both', maxDepth: 5 });
```

```python
connected = db.is_connected(from_id, to_id, direction="both", max_depth=5)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| from | `u64` | `number` | `int` | Yes | — | Source node. |
| to | `u64` | `number` | `int` | Yes | — | Destination node. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Direction. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Edge type filter. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| max_depth | `Option<u32>` | `number` | `int` | No | `None` | Maximum search depth. |

#### Returns

`bool`. Returns `true` if a path exists, `false` otherwise.

---

## Graph Analytics

### connected_components

Computes all [weakly connected components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) in the graph. Treats edges as undirected regardless of their actual direction.

**Rust**
```rust
let components = db.connected_components(&ComponentOptions::default())?;
// components: NodeIdMap<u64> - node_id to component_id
```

**Node.js**
```javascript
const entries = db.connectedComponents();
// entries: { nodeId: number, componentId: number }[]
```

**Python**
```python
components = db.connected_components()
# components: dict[int, int] - node_id to component_id
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| edge_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only consider these edge types when determining connectivity. |
| node_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only include nodes of these types. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns

A mapping from every node ID to its component ID. The component ID is the smallest node ID within each component (a canonical representative).

---

### component_of

Returns all nodes in the same connected component as a given node.

```rust
let component_id = db.component_of(node_id, &ComponentOptions::default())?;
```

```javascript
const nodeIds = db.componentOf(nodeId); // Float64Array
```

```python
node_ids = db.component_of(node_id)  # list[int]
```

#### Parameters

Same as [`connected_components`](#connected_components), plus:

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| node_id | `u64` | `number` | `int` | Yes | Node to find the component for. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u64, EngineError>` (component ID) | `Float64Array` (all node IDs) | `list[int]` (all node IDs) |

Note: The Rust API returns just the component ID (representative node), while Node.js and Python return all node IDs in the component.

---

### personalized_pagerank

Computes [Personalized PageRank](https://en.wikipedia.org/wiki/PageRank#Personalized_PageRank) from one or more seed nodes. Useful for recommendation, influence scoring, and relevance ranking.

OverGraph exposes two PPR algorithms:
- `exact` / `ExactPowerIteration` (default): reference implementation using power iteration.
- `approx` / `ApproxForwardPush`: local forward-push approximation, usually much faster for seed-centric retrieval workloads.

**Rust**
```rust
let result = db.personalized_pagerank(&[seed_id], &PprOptions {
    algorithm: PprAlgorithm::ApproxForwardPush,
    approx_residual_tolerance: 1e-5,
    max_results: Some(50),
    ..Default::default()
})?;
```

**Node.js**
```javascript
const result = db.personalizedPagerank([seedId], {
  algorithm: 'approx',
  approxResidualTolerance: 1e-5,
  maxResults: 50,
});

console.log('algorithm:', result.algorithm);
for (let i = 0; i < result.nodeIds.length; i++) {
  console.log(result.nodeIds[i], result.scores[i]);
}
```

**Python**
```python
result = db.personalized_pagerank([seed_id],
    algorithm="approx",
    approx_residual_tolerance=1e-5,
    max_results=50)

print("algorithm:", result.algorithm)
for nid, score in zip(result.node_ids, result.scores):
    print(nid, score)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| seed_node_ids | `Vec<u64>` | `number[]` | `list[int]` | Yes | — | Seed nodes. The random walk teleports back to these nodes with probability `1 - damping_factor`. Multiple seeds distribute the teleport probability evenly. |
| algorithm | `PprAlgorithm` | `string` | `str` | No | `ExactPowerIteration` / `"exact"` | PPR algorithm. Rust accepts `ExactPowerIteration` or `ApproxForwardPush`. Node/Python accept `"exact"` or `"approx"`. |
| damping_factor | `f64` | `number` | `float` | No | `0.85` | Probability of following an edge (vs. teleporting back to a seed). Standard PageRank uses 0.85. Higher values explore further from seeds; lower values stay closer. |
| max_iterations | `u32` | `number` | `int` | No | `20` | Maximum power iterations for exact mode. The algorithm stops when it converges or reaches this limit. |
| epsilon | `f64` | `number` | `float` | No | `1e-6` | Convergence threshold. Iteration stops when the L1 norm of the score change vector drops below this value. |
| approx_residual_tolerance | `f64` | `number` | `float` | No | `1e-5` | Approximate-mode stopping tolerance for forward push. Smaller values improve fidelity and increase work. |
| edge_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only follow these edge types during the walk. |
| max_results | `Option<usize>` | `number` | `int` | No | `None` (all) | Return only the top N nodes by score. |

#### Returns: PprResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_ids | `Vec<u64>` | `Float64Array` | `list[int]` | Node IDs sorted by score (descending). |
| scores | `Vec<f64>` | `Float64Array` | `list[float]` | Corresponding scores. Exact PPR sums to 1.0 (or very close); approximate PPR is optimized for ranking quality rather than strict normalization. |
| iterations | `u32` | `number` | `int` | Number of exact power iterations performed. Approximate mode returns `0`. |
| converged | `bool` | `boolean` | `bool` | Exact mode: whether the algorithm converged within `max_iterations`. Approximate mode: `true` when no node remains above the residual tolerance. |
| algorithm | `PprAlgorithm` | `string` | `str` | Which algorithm produced the result. |
| approx | `Option<PprApproxMeta>` | `JsPprApproxMeta \| null` | `PyPprApproxMeta \| None` | Approximate-mode metadata. `None`/`null` in exact mode. |

---

### export_adjacency

Exports the graph's adjacency structure as flat arrays. Useful for bulk analysis, NetworkX integration, or external graph processing.

**Rust**
```rust
let export = db.export_adjacency()?; // uses default options
```

**Node.js**
```javascript
const adj = db.exportAdjacency({ includeWeights: true });
// adj.nodeIds: Float64Array
// adj.edgeFrom: Float64Array
// adj.edgeTo: Float64Array
// adj.edgeTypeIds: Uint32Array
// adj.edgeWeights: Float64Array | null
```

**Python**
```python
adj = db.export_adjacency(include_weights=True)
# adj.node_ids: list[int]
# adj.edges: list[ExportEdge] - each has from_id, to_id, type_id, weight
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only export nodes of these types. |
| edge_type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Only export edges of these types. |
| include_weights | `bool` | `boolean` | `bool` | No | `true` | Include edge weights in the export. Set to `false` to save memory/bandwidth when weights aren't needed. |

---

## Vector Search

### vector_search

Performs similarity search using dense vectors (HNSW approximate nearest neighbors), sparse vectors (inverted index dot product), or hybrid mode (fusion of both).

**Rust**
```rust
// Dense search
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Dense,
    dense_query: Some(vec![0.1, 0.2, 0.3, /* ... 384 dims */]),
    k: 10,
    ef_search: Some(100),
    ..Default::default()
})?;

// Sparse search
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Sparse,
    sparse_query: Some(vec![(42, 0.9), (128, 0.5)]),
    k: 10,
    ..Default::default()
})?;

// Hybrid search
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Hybrid,
    dense_query: Some(embedding),
    sparse_query: Some(sparse_terms),
    k: 10,
    dense_weight: Some(0.7),
    sparse_weight: Some(0.3),
    fusion_mode: Some(FusionMode::WeightedScoreFusion),
    ..Default::default()
})?;
```

**Node.js**
```javascript
// Dense search
const hits = db.vectorSearch('dense', {
  k: 10,
  denseQuery: [0.1, 0.2, 0.3, /* ... */],
  efSearch: 100,
});

// Sparse search
const hits = db.vectorSearch('sparse', {
  k: 10,
  sparseQuery: [{ dimension: 42, value: 0.9 }, { dimension: 128, value: 0.5 }],
});

// Hybrid search with graph scope
const hits = db.vectorSearch('hybrid', {
  k: 10,
  denseQuery: embedding,
  sparseQuery: sparseTerms,
  denseWeight: 0.7,
  sparseWeight: 0.3,
  fusionMode: 'weighted_score',
  scope: {
    startNodeId: rootId,
    maxDepth: 2,
    direction: 'outgoing',
  },
});
```

**Python**
```python
# Dense search
hits = db.vector_search("dense", k=10, dense_query=[0.1, 0.2, ...], ef_search=100)

# Sparse search
hits = db.vector_search("sparse", k=10, sparse_query=[(42, 0.9), (128, 0.5)])

# Hybrid with graph scope
hits = db.vector_search("hybrid", k=10,
    dense_query=embedding,
    sparse_query=sparse_terms,
    dense_weight=0.7, sparse_weight=0.3,
    fusion_mode="weighted_score",
    scope_start_node_id=root_id,
    scope_max_depth=2,
    scope_direction="outgoing")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| mode | `VectorSearchMode` | `string` | `str` | Yes | — | `"dense"`, `"sparse"`, or `"hybrid"`. Determines which query vector(s) and index to use. |
| k | `usize` | `number` | `int` | Yes | — | Number of top results to return. |
| dense_query | `Option<Vec<f32>>` | `number[]` | `list[float]` | Required for `dense`/`hybrid` | `None` | Query vector for dense search. Must have the same dimension as configured at `open()`. |
| sparse_query | `Option<Vec<(u32, f32)>>` | `SparseEntry[]` | `list[tuple[int, float]]` | Required for `sparse`/`hybrid` | `None` | Query vector for sparse search. List of `(dimension_index, value)` pairs. |
| type_filter | `Option<Vec<u32>>` | `number[]` | `list[int]` | No | `None` | Restrict results to nodes of these types. |
| ef_search | `Option<usize>` | `number` | `int` | No | `2 × k` | HNSW search expansion factor. Higher values improve recall at the cost of latency. Only applies to dense/hybrid modes. |

**Hybrid fusion parameters** (only used when `mode = "hybrid"`):

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| dense_weight | `Option<f32>` | `number` | `float` | No | `0.5` | Weight for dense scores in fusion (0.0–1.0). |
| sparse_weight | `Option<f32>` | `number` | `float` | No | `0.5` | Weight for sparse scores in fusion (0.0–1.0). |
| fusion_mode | `Option<FusionMode>` | `string` | `str` | No | `WeightedRankFusion` | How to combine dense and sparse results. See fusion modes below. |

**Fusion modes:**

| Mode | Rust | Node.js / Python | Description |
|------|------|------------------|-------------|
| WeightedRankFusion | `FusionMode::WeightedRankFusion` | `"weighted_rank"` | Weighted reciprocal rank fusion. Default. Combines rank positions with weights. Robust when score distributions differ. |
| ReciprocalRankFusion | `FusionMode::ReciprocalRankFusion` | `"reciprocal_rank"` | Standard RRF (unweighted). Equal contribution from both signals. |
| WeightedScoreFusion | `FusionMode::WeightedScoreFusion` | `"weighted_score"` | Min-max normalized score fusion. Directly combines normalized scores. Best when score magnitudes are meaningful. |

**Graph-scoped search** (restrict vector search to a subgraph):

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| scope.start_node_id | `u64` | `scope.startNodeId: number` | `scope_start_node_id: int` | No | `None` | Root node for scope traversal. When set, only nodes reachable from this node within `max_depth` are candidates. |
| scope.max_depth | `u32` | `scope.maxDepth: number` | `scope_max_depth: int` | Required if scope set | — | Maximum hops from the scope root. |
| scope.direction | `Direction` | `scope.direction: string` | `scope_direction: str` | No | `Outgoing` | Direction for scope traversal. |
| scope.edge_type_filter | `Option<Vec<u32>>` | `scope.edgeTypeFilter: number[]` | `scope_edge_type_filter: list[int]` | No | `None` | Edge types for scope traversal. |
| scope.at_epoch | `Option<i64>` | `scope.atEpoch: number` | `scope_at_epoch: int` | No | `None` | Temporal filter for scope. |

#### Returns: VectorHit

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_id | `u64` | `number` | `int` | Matching node ID. |
| score | `f32` | `number` | `float` | Similarity score. Higher is better. |

Results are sorted by score descending.

---

## Retention & Pruning

### prune

Immediately deletes nodes matching the specified criteria. Cascade-deletes all incident edges. Applied atomically in a single WAL batch.

```rust
let result = db.prune(&PrunePolicy {
    max_age_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
    max_weight: Some(0.1),                       // weight <= 0.1
    type_id: Some(CONVERSATION),                  // only conversations
})?;
println!("pruned {} nodes, {} edges", result.nodes_pruned, result.edges_pruned);
```

```javascript
const result = db.prune({
  maxAgeMs: 7 * 24 * 60 * 60 * 1000,
  maxWeight: 0.1,
  typeId: CONVERSATION,
});
```

```python
result = db.prune(
    max_age_ms=7 * 24 * 60 * 60 * 1000,
    max_weight=0.1,
    type_id=CONVERSATION,
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| max_age_ms | `Option<i64>` | `number` | `int` | No* | `None` | Delete nodes older than `now - max_age_ms` milliseconds. Age is computed from `updated_at`. |
| max_weight | `Option<f32>` | `number` | `float` | No* | `None` | Delete nodes with `weight <= max_weight`. |
| type_id | `Option<u32>` | `number` | `int` | No | `None` (all types) | Restrict pruning to a single node type. |

\* At least one of `max_age_ms` or `max_weight` must be provided. This guards against accidental mass deletion (calling `prune({})` with no criteria is an error).

**Criteria are combined with AND logic.** A node is pruned only if it matches *all* specified criteria.

#### Returns: PruneResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| nodes_pruned | `u64` | `number` | `int` | Number of nodes deleted. |
| edges_pruned | `u64` | `number` | `int` | Number of edges cascade-deleted. |

---

### set_prune_policy

Registers a named prune policy that is automatically applied during [compaction](#compact). Multiple policies can coexist.

```rust
db.set_prune_policy("stale-conversations", PrunePolicy {
    max_age_ms: Some(30 * 24 * 60 * 60 * 1000), // 30 days
    type_id: Some(CONVERSATION),
    ..Default::default()
})?;
```

```javascript
db.setPrunePolicy('stale-conversations', {
  maxAgeMs: 30 * 24 * 60 * 60 * 1000,
  typeId: CONVERSATION,
});
```

```python
db.set_prune_policy("stale-conversations",
    max_age_ms=30 * 24 * 60 * 60 * 1000,
    type_id=CONVERSATION)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| name | `&str` | `string` | `str` | Yes | Policy name. Used to remove or list the policy later. |
| policy | `PrunePolicy` | `object` | `**kwargs` | Yes | Pruning criteria (same fields as [`prune`](#prune)). |

#### Behavior

- Persisted in the manifest. Survives database close/reopen.
- Applied automatically during compaction: matching nodes are pruned and their edges cascade-deleted.
- Multiple policies combine with **OR logic across policies**: a node matching *any* policy is pruned. Within a single policy, criteria combine with AND logic.
- Setting a policy with the same name replaces the previous one.

---

### remove_prune_policy

Removes a named prune policy.

```rust
let existed = db.remove_prune_policy("stale-conversations")?;
```

```javascript
const existed = db.removePrunePolicy('stale-conversations');
```

```python
existed = db.remove_prune_policy("stale-conversations")
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| name | `&str` / `string` / `str` | Yes | Name of the policy to remove. |

#### Returns

`bool`. Returns `true` if the policy existed and was removed, `false` if no policy with that name was found.

---

### list_prune_policies

Lists all registered prune policies.

```rust
let policies = db.list_prune_policies();
for (name, policy) in &policies {
    println!("{}: max_age_ms={:?}", name, policy.max_age_ms);
}
```

```javascript
const policies = db.listPrunePolicies();
// [{ name: string, policy: { maxAgeMs?, maxWeight?, typeId? } }]
```

```python
policies = db.list_prune_policies()
for p in policies:
    print(p.name, p.max_age_ms, p.max_weight, p.type_id)
```

#### Returns

Array of named policies. Each entry includes:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| name | `String` | `string` | `str` | Policy name. |
| max_age_ms | `Option<i64>` | `number \| undefined` | `int \| None` | Age threshold. |
| max_weight | `Option<f32>` | `number \| undefined` | `float \| None` | Weight threshold. |
| type_id | `Option<u32>` | `number \| undefined` | `int \| None` | Type scope. |

---

## Maintenance

### sync

Forces an immediate WAL fsync, ensuring all buffered writes are durable on disk.

```rust
db.sync()?;
```

```javascript
db.sync();
```

```python
db.sync()
```

#### Behavior

- In **Immediate** mode: no-op (every write already triggers fsync).
- In **GroupCommit** mode: blocks until all currently buffered data is fsynced.

---

### flush

Flushes the active memtable to a new on-disk segment. Blocks until all pending immutable memtables are written.

```rust
let info = db.flush()?;
```

```javascript
db.flush();
```

```python
info = db.flush()  # PySegmentInfo | None
```

#### Returns

| Rust | Node.js | Python | Description |
|------|---------|--------|-------------|
| `Result<Option<SegmentInfo>, EngineError>` | `void` | `SegmentInfo \| None` | Info about the written segment, or `None` if the memtable was empty. |

**SegmentInfo** (Python only):

| Field | Type | Description |
|-------|------|-------------|
| id | `int` | Segment ID on disk. |
| node_count | `int` | Nodes in the segment. |
| edge_count | `int` | Edges in the segment. |

---

### compact

Merges all segments into a single segment. Applies prune policies during the merge. Reclaims space from tombstones.

```rust
let stats = db.compact()?;
```

```javascript
const stats = db.compact();
```

```python
stats = db.compact()
```

#### Returns: CompactionStats

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| segments_merged | `usize` | `number` | `int` | Number of input segments. |
| nodes_kept | `u64` | `number` | `int` | Live nodes in the output segment. |
| nodes_removed | `u64` | `number` | `int` | Tombstoned nodes reclaimed. |
| edges_kept | `u64` | `number` | `int` | Live edges in the output. |
| edges_removed | `u64` | `number` | `int` | Tombstoned edges reclaimed. |
| duration_ms | `u64` | `number` | `int` | Wall-clock time of compaction. |
| output_segment_id | `u64` | `number` | `int` | ID of the output segment. |
| nodes_auto_pruned | `u64` | `number` | `int` | Nodes removed by prune policies. |
| edges_auto_pruned | `u64` | `number` | `int` | Edges cascade-deleted by auto-prune. |

Returns `None`/`null` if there are fewer than 2 segments (nothing to compact).

---

### compact_with_progress

Compaction with a progress callback. The callback is invoked at key phases and can cancel the compaction.

```rust
let stats = db.compact_with_progress(|progress| {
    println!("phase: {:?}, {}/{} records",
        progress.phase, progress.records_processed, progress.total_records);
    true // return false to cancel
})?;
```

```javascript
// Sync (blocks event loop):
const stats = db.compactWithProgress((progress) => {
  console.log(progress.phase, progress.recordsProcessed, '/', progress.totalRecords);
  return true; // return false to cancel
});

// Async (preferred for UIs):
const stats = await db.compactWithProgressAsync((progress) => {
  console.log(progress.phase);
  // async version cannot cancel, returns void
});
```

```python
def on_progress(progress):
    print(progress.phase, progress.records_processed, "/", progress.total_records)
    return True  # return False to cancel

stats = db.compact_with_progress(on_progress)
```

#### Progress Object

| Field | Type | Description |
|-------|------|-------------|
| phase | `string` | Current phase: `"collecting_tombstones"`, `"merging_nodes"`, `"merging_edges"`, `"writing_output"`. |
| segments_processed | `u32` / `number` / `int` | Segments completed so far. |
| total_segments | `u32` / `number` / `int` | Total segments to process. |
| records_processed | `u64` / `number` / `int` | Individual records processed. |
| total_records | `u64` / `number` / `int` | Total records to process. |

**Cancellation**: Return `false` from the callback to safely cancel compaction. No state is modified because cancellation happens before the atomic segment swap.

---

### ingest_mode

Enters bulk ingest mode. Disables auto-compaction so that rapid writes don't trigger background merges. Call [`end_ingest`](#end_ingest) when done.

```rust
db.ingest_mode();
// ... bulk writes ...
let stats = db.end_ingest()?;
```

```javascript
db.ingestMode();
// ... bulk writes ...
const stats = db.endIngest();
```

```python
db.ingest_mode()
# ... bulk writes ...
stats = db.end_ingest()
```

#### Behavior

- No compaction is triggered while in ingest mode, regardless of `compact_after_n_flushes`.
- The memtable still flushes to segments when the threshold is reached.
- Ideal for initial data loading: write millions of records, then compact once.

---

### end_ingest

Exits ingest mode and immediately compacts all segments.

```rust
let stats = db.end_ingest()?;
```

```javascript
const stats = db.endIngest(); // CompactionStats | null
```

```python
stats = db.end_ingest()  # CompactionStats | None
```

#### Returns

`CompactionStats` (same as [`compact`](#compact)), or `None`/`null` if there was nothing to compact.

---

## Introspection

These methods provide quick diagnostic information. They are **approximate** and counts may slightly overcount when the same ID appears in multiple memtables or segments.

### node_count

```rust
let count = db.node_count(); // usize
```

Approximate count of live nodes across all data sources.

### edge_count

```rust
let count = db.edge_count(); // usize
```

Approximate count of live edges.

### segment_count

```rust
let count = db.segment_count(); // usize
```

Number of on-disk segments. After compaction, this is typically 0 or 1.

---

## Binary Batch Ingestion

High-performance binary format for batch upserts. Avoids JSON parsing overhead. Useful when ingesting data from a custom pipeline.

### batch_upsert_nodes_binary

```javascript
const buf = Buffer.alloc(/* ... */);
// Format: [count: u32_LE][per node: type_id: u32_LE, weight: f32_LE, key_len: u16_LE, key: utf8, props_len: u32_LE, props: json_utf8]
const ids = db.batchUpsertNodesBinary(buf);
```

```python
buf = b'...'  # same binary format
ids = db.batch_upsert_nodes_binary(buf)
```

#### Binary Format (little-endian)

```
┌──────────────────────────────────────┐
│ count: u32                           │  ← number of nodes in this batch
├──────────────────────────────────────┤
│ For each node:                       │
│   type_id:   u32                     │
│   weight:    f32                     │
│   key_len:   u16                     │
│   key:       [u8; key_len]  (UTF-8)  │
│   props_len: u32                     │
│   props:     [u8; props_len] (JSON)  │
└──────────────────────────────────────┘
```

#### Returns

Array of node IDs (same order as packed nodes).

---

### batch_upsert_edges_binary

```javascript
const ids = db.batchUpsertEdgesBinary(buf);
```

```python
ids = db.batch_upsert_edges_binary(buf)
```

#### Binary Format (little-endian)

```
┌──────────────────────────────────────────┐
│ count: u32                               │
├──────────────────────────────────────────┤
│ For each edge:                           │
│   from:       u64                        │
│   to:         u64                        │
│   type_id:    u32                        │
│   weight:     f32                        │
│   valid_from: i64                        │
│   valid_to:   i64                        │
│   props_len:  u32                        │
│   props:      [u8; props_len]  (JSON)    │
└──────────────────────────────────────────┘
```

---

## Error Handling

All methods can fail. Errors are returned differently across languages:

| Language | Error mechanism | Error type |
|----------|----------------|------------|
| Rust | `Result<T, EngineError>` | `EngineError` enum |
| Node.js | Thrown `Error` | Standard `Error` with message |
| Python | Raised exception | `OverGraphError(Exception)` |

### EngineError Variants (Rust)

| Variant | Description |
|---------|-------------|
| `IoError(io::Error)` | Filesystem I/O failure (disk full, permission denied, etc.). |
| `CorruptRecord(String)` | A record failed deserialization. Indicates data corruption. |
| `CorruptWal(String)` | WAL file is corrupt (truncated, bad checksum). |
| `SerializationError(String)` | Property encoding/decoding failed. |
| `ManifestError(String)` | Manifest file is corrupt or incompatible. |
| `DatabaseNotFound(String)` | Directory doesn't exist and `create_if_missing` is false. |
| `InvalidOperation(String)` | Invalid API usage (e.g., writing to a closed database). |
| `CompactionCancelled` | Compaction was cancelled via the progress callback. |
| `WalSyncFailed(String)` | WAL fsync failed. |

### Error Handling Examples

**Rust**
```rust
match db.get_node(42) {
    Ok(Some(node)) => println!("found: {}", node.key),
    Ok(None) => println!("not found"),
    Err(e) => eprintln!("error: {}", e),
}
```

**Node.js**
```javascript
try {
  const node = db.getNode(42);
} catch (e) {
  console.error('OverGraph error:', e.message);
}
```

**Python**
```python
from overgraph import OverGraph, OverGraphError

try:
    node = db.get_node(42)
except OverGraphError as e:
    print(f"OverGraph error: {e}")
```

---

## Async API

Both Node.js and Python provide async variants of all methods.

### Node.js

Every synchronous method has an async counterpart with an `Async` suffix that returns a `Promise`:

```javascript
// Sync
const node = db.getNode(42);

// Async
const node = await db.getNodeAsync(42);
```

Async methods run on the libuv thread pool. Write operations acquire an exclusive lock; read operations acquire a shared lock (allowing concurrent reads).

**Available async methods:** `closeAsync`, `upsertNodeAsync`, `upsertEdgeAsync`, `batchUpsertNodesAsync`, `batchUpsertEdgesAsync`, `getNodeAsync`, `getEdgeAsync`, `getNodeByKeyAsync`, `getEdgeByTripleAsync`, `getNodesAsync`, `getNodesByKeysAsync`, `getEdgesAsync`, `deleteNodeAsync`, `deleteEdgeAsync`, `invalidateEdgeAsync`, `graphPatchAsync`, `neighborsAsync`, `neighborsPagedAsync`, `neighborsBatchAsync`, `traverseAsync`, `topKNeighborsAsync`, `extractSubgraphAsync`, `shortestPathAsync`, `allShortestPathsAsync`, `isConnectedAsync`, `degreeAsync`, `degreesAsync`, `sumEdgeWeightsAsync`, `avgEdgeWeightAsync`, `findNodesAsync`, `findNodesPagedAsync`, `findNodesByTimeRangeAsync`, `findNodesByTimeRangePagedAsync`, `nodesByTypeAsync`, `edgesByTypeAsync`, `getNodesByTypeAsync`, `getEdgesByTypeAsync`, `countNodesByTypeAsync`, `countEdgesByTypeAsync`, `nodesByTypePagedAsync`, `edgesByTypePagedAsync`, `getNodesByTypePagedAsync`, `getEdgesByTypePagedAsync`, `personalizedPagerankAsync`, `connectedComponentsAsync`, `componentOfAsync`, `vectorSearchAsync`, `exportAdjacencyAsync`, `pruneAsync`, `setPrunePolicyAsync`, `removePrunePolicyAsync`, `listPrunePoliciesAsync`, `syncAsync`, `flushAsync`, `compactAsync`, `compactWithProgressAsync`, `ingestModeAsync`, `endIngestAsync`.

### Python

The `AsyncOverGraph` class wraps every `OverGraph` method with `asyncio.to_thread()`:

```python
from overgraph import AsyncOverGraph

async def main():
    async with await AsyncOverGraph.open("./my-graph") as db:
        node_id = await db.upsert_node(1, "alice")
        node = await db.get_node(node_id)
        neighbors = await db.neighbors(node_id)

asyncio.run(main())
```

**All methods have identical signatures and semantics** to the sync `OverGraph` class but return coroutines.

**GIL behavior**: The sync `OverGraph` releases the Python GIL during all Rust operations, enabling true parallelism in multi-threaded Python. The `AsyncOverGraph` uses `asyncio.to_thread()` to run sync operations in the default thread pool executor.

---

## Appendix: Quick Reference

### All Methods at a Glance

| Category | Method | Description |
|----------|--------|-------------|
| **Lifecycle** | `open` | Open or create database |
| | `close` | Shut down database |
| | `stats` | Runtime statistics |
| **Nodes** | `upsert_node` | Create or update node |
| | `get_node` | Get node by ID |
| | `get_node_by_key` | Get node by type + key |
| | `delete_node` | Delete node (cascade edges) |
| | `batch_upsert_nodes` | Batch create/update nodes |
| | `get_nodes` | Batch get nodes by ID |
| | `get_nodes_by_keys` | Batch get nodes by type + key |
| **Edges** | `upsert_edge` | Create or update edge |
| | `get_edge` | Get edge by ID |
| | `get_edge_by_triple` | Get edge by from + to + type |
| | `delete_edge` | Delete edge |
| | `invalidate_edge` | Close validity window |
| | `batch_upsert_edges` | Batch create/update edges |
| | `get_edges` | Batch get edges by ID |
| **Atomic** | `graph_patch` | Multi-op atomic batch |
| **Query** | `nodes_by_type` | All node IDs of a type |
| | `edges_by_type` | All edge IDs of a type |
| | `get_nodes_by_type` | All node records of a type |
| | `get_edges_by_type` | All edge records of a type |
| | `count_nodes_by_type` | Count nodes of a type |
| | `count_edges_by_type` | Count edges of a type |
| | `find_nodes` | Property search |
| | `find_nodes_by_time_range` | Time range search |
| **Pagination** | `*_paged` | Paginated variants |
| **Neighbors** | `neighbors` | Immediate neighbors |
| | `neighbors_paged` | Paginated neighbors |
| | `neighbors_batch` | Multi-node neighbors |
| | `top_k_neighbors` | Top K by score |
| **Degree** | `degree` | Edge count |
| | `degrees` | Batch edge counts |
| | `sum_edge_weights` | Sum of edge weights |
| | `avg_edge_weight` | Average edge weight |
| **Traversal** | `traverse` | BFS traversal |
| | `extract_subgraph` | Subgraph extraction |
| **Pathfinding** | `shortest_path` | Shortest path |
| | `all_shortest_paths` | All shortest paths |
| | `is_connected` | Reachability check |
| **Analytics** | `connected_components` | WCC decomposition |
| | `component_of` | Component membership |
| | `personalized_pagerank` | PPR scoring |
| | `export_adjacency` | Adjacency export |
| **Vectors** | `vector_search` | Dense/sparse/hybrid search |
| **Retention** | `prune` | Immediate pruning |
| | `set_prune_policy` | Register auto-prune |
| | `remove_prune_policy` | Remove auto-prune |
| | `list_prune_policies` | List policies |
| **Maintenance** | `sync` | Force WAL fsync |
| | `flush` | Memtable → segment |
| | `compact` | Merge segments |
| | `compact_with_progress` | Merge with progress |
| | `ingest_mode` | Enter bulk mode |
| | `end_ingest` | Exit bulk mode + compact |
| **Binary** | `batch_upsert_nodes_binary` | Binary batch nodes |
| | `batch_upsert_edges_binary` | Binary batch edges |
