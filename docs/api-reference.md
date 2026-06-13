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
  - [close_fast](#close_fast)
  - [stats](#stats)
- [Configuration](#configuration)
  - [DbOptions](#dboptions)
  - [WalSyncMode](#walsyncmode)
  - [DenseVectorConfig](#densevectorconfig)
- [Data Model](#data-model)
  - [Node Records](#node-records)
  - [Edge Records](#edge-records)
  - [PropValue](#propvalue)
  - [IntoNodeLabels](#intonodelabels-rust-only)
  - [Direction](#direction)
  - [NodeLabelFilter / LabelMatchMode](#nodelabelfilter--labelmatchmode)
- [Catalog APIs](#catalog-apis)
  - [ensure_node_label / ensure_edge_label](#ensure_node_label--ensure_edge_label)
  - [get_node_label_id / get_edge_label_id](#get_node_label_id--get_edge_label_id)
  - [get_node_label / get_edge_label](#get_node_label--get_edge_label)
  - [list_node_labels / list_edge_labels](#list_node_labels--list_edge_labels)
- [Node Operations](#node-operations)
  - [upsert_node](#upsert_node)
  - [get_node](#get_node)
  - [get_node_by_key](#get_node_by_key)
  - [add_node_label / remove_node_label](#add_node_label--remove_node_label)
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
  - [write transactions](#write-transactions)
- [Label and Edge-Label Queries](#label-and-edge-label-queries)
  - [nodes_by_labels](#nodes_by_labels)
  - [edges_by_label](#edges_by_label)
  - [get_nodes_by_labels](#get_nodes_by_labels)
  - [get_edges_by_label](#get_edges_by_label)
  - [count_nodes_by_labels](#count_nodes_by_labels)
  - [count_edges_by_label](#count_edges_by_label)
- [Property Index Management](#property-index-management)
  - [ensure_node_property_index](#ensure_node_property_index)
  - [drop_node_property_index](#drop_node_property_index)
  - [list_node_property_indexes](#list_node_property_indexes)
  - [NodePropertyIndexInfo](#nodepropertyindexinfo)
  - [ensure_edge_property_index](#ensure_edge_property_index)
  - [drop_edge_property_index](#drop_edge_property_index)
  - [list_edge_property_indexes](#list_edge_property_indexes)
  - [EdgePropertyIndexInfo](#edgepropertyindexinfo)
  - [PropertyRangeBound](#propertyrangebound)
  - [PropertyRangeCursor](#propertyrangecursor)
  - [PropertyRangePageRequest](#propertyrangepagerequest-rust-only)
  - [PropertyRangePageResult](#propertyrangepageresult)
- [Schema Management](#schema-management)
  - [Schema semantics](#schema-semantics)
  - [Schema methods](#schema-methods)
  - [Schema DTOs](#schema-dtos)
  - [Schema examples](#schema-examples)
- [Property & Time Queries](#property--time-queries)
  - [find_nodes](#find_nodes)
  - [find_nodes_range](#find_nodes_range)
  - [find_nodes_by_time_range](#find_nodes_by_time_range)
- [Queries](#queries)
  - [Node Queries](#node-queries)
    - [query_node_ids](#query_node_ids)
    - [query_nodes](#query_nodes)
    - [explain_node_query](#explain_node_query)
  - [Direct Edge Queries](#direct-edge-queries)
    - [query_edge_ids](#query_edge_ids)
    - [query_edges](#query_edges)
    - [explain_edge_query](#explain_edge_query)
  - [Graph Row Queries](#graph-row-queries)
    - [query_graph_rows](#query_graph_rows)
    - [explain_graph_rows](#explain_graph_rows)
  - [Graph Pipeline Queries](#graph-pipeline-queries)
    - [query_graph_pipeline](#query_graph_pipeline)
    - [explain_graph_pipeline](#explain_graph_pipeline)
  - [GQL](#gql)
    - [Overview](#overview)
    - [Read Syntax](#read-syntax)
    - [Mutation Syntax](#mutation-syntax)
    - [Method Reference](#method-reference)
    - [Parameters and Options](#parameters-and-options)
    - [Results and Row Formats](#results-and-row-formats)
    - [Nodes, Edges, Values, and Vectors](#nodes-edges-values-and-vectors)
    - [Params](#params)
    - [Explain, Profile, and Stats](#explain-profile-and-stats)
    - [Examples](#examples)
    - [Current Limits](#current-limits)
  - [Query Request Types and Plans](#query-request-types-and-plans)
    - [NodeQuery](#nodequery)
    - [NodeFilter / QueryNodeFilter](#nodefilter--querynodefilter)
    - [EdgeQuery](#edgequery)
    - [EdgeFilter / QueryEdgeFilter](#edgefilter--queryedgefilter)
    - [GraphRowQuery](#graphrowquery)
    - [QueryPlan](#queryplan)
    - [Validation notes](#validation-notes)
- [Pagination](#pagination)
  - [nodes_by_labels_paged](#nodes_by_labels_paged)
  - [edges_by_label_paged](#edges_by_label_paged)
  - [get_nodes_by_labels_paged](#get_nodes_by_labels_paged)
  - [get_edges_by_label_paged](#get_edges_by_label_paged)
  - [find_nodes_paged](#find_nodes_paged)
  - [find_nodes_range_paged](#find_nodes_range_paged)
  - [find_nodes_by_time_range_paged](#find_nodes_by_time_range_paged)
- [Traversal](#traversal)
  - [neighbors](#neighbors)
  - [neighbors_paged](#neighbors_paged)
  - [neighbors_batch](#neighbors_batch)
  - [top_k_neighbors](#top_k_neighbors)
  - [traverse](#traverse)
  - [extract_subgraph](#extract_subgraph)
  - [shortest_path](#shortest_path)
  - [all_shortest_paths](#all_shortest_paths)
  - [is_connected](#is_connected)
- [Degree & Weight Aggregation](#degree--weight-aggregation)
  - [degree](#degree)
  - [degrees](#degrees)
  - [sum_edge_weights](#sum_edge_weights)
  - [avg_edge_weight](#avg_edge_weight)
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
  - [scrub](#scrub)
- [Introspection](#introspection)
  - [node_count](#node_count)
  - [edge_count](#edge_count)
  - [next_node_id](#next_node_id)
  - [next_edge_id](#next_edge_id)
  - [segment_count](#segment_count)
  - [segment_tombstone_node_count](#segment_tombstone_node_count)
  - [segment_tombstone_edge_count](#segment_tombstone_edge_count)
  - [path](#path)
  - [manifest](#manifest)
  - [manifest::load_manifest_readonly](#manifestload_manifest_readonly-rust-only)
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
overgraph = "0.7"
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
  compactAfterNFlushes: 4,
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
    compact_after_n_flushes=4,
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
    # Also accepts multiple labels: ["User", "Admin"]
    db.upsert_node("User", "alice")
# db.close() called automatically on exit
```

**Node.js** has no built-in equivalent; call `close()` or `closeAsync()` explicitly in a `finally` block.

---

### close_fast

Rust-only fast close. This is the same behavior exposed by `close({ force: true })` in Node.js and `close(force=True)` in Python.

```rust
db.close_fast()?;
```

It cancels any in-progress background compaction, syncs the active WAL, and persists a manifest that retains the WAL generations needed for replay on the next open.

---

### stats

Returns a read-only snapshot of current database statistics.

**Rust**
```rust
let s = db.stats()?;
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
| edge_uniqueness | `bool` | `edgeUniqueness` | `edge_uniqueness` | `false` | When `true`, `upsert_edge` enforces at most one edge per `(from, to, label)` triple. An upsert with the same triple updates the existing edge. When `false`, every `upsert_edge` call creates a new edge. |
| compact_after_n_flushes | `u32` | `compactAfterNFlushes` | `compact_after_n_flushes` | `4` | Trigger background compaction after this many flushes. Set to `0` to disable auto-compaction. |
| dense_vector | `Option<DenseVectorConfig>` | `denseVector` | See below | `None` | Enable dense vector search. See [DenseVectorConfig](#densevectorconfig). In Python, use separate kwargs: `dense_vector_dimension` and `dense_vector_metric`. |

### WalSyncMode

Controls the trade-off between durability and write throughput.

| Mode | Rust | Node.js | Python | Behavior |
|------|------|---------|--------|----------|
| Immediate | `WalSyncMode::Immediate` | `"immediate"` | `"immediate"` | Every write triggers an `fsync`. Maximum crash safety. Data is durable before the write call returns. Lower throughput (~4ms per write on typical SSDs). |
| GroupCommit | `WalSyncMode::GroupCommit { .. }` | `"group-commit"` | `"group_commit"` | Writes are buffered and fsynced on a timer or when the buffer fills. Higher throughput (batched fsync amortizes the cost across many writes). A crash can lose at most one group-commit interval of writes. |

Current Node.js connector parsing treats unknown `walSyncMode` strings as group commit. Python validates `wal_sync_mode` and rejects unknown strings.

**GroupCommit parameters** (Node.js/Python expose these as top-level options):

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
| metric | `DenseMetric` | `metric: string` | `dense_vector_metric: str` | `Cosine` | Distance metric for similarity. Rust uses enum variants. Node.js and Python use lower-case strings. |

**DenseMetric values:**

| Metric | Rust | Node.js / Python | Score semantics |
|--------|------|------------------|-----------------|
| Cosine | `DenseMetric::Cosine` | `"cosine"` | Higher = more similar (range: -1 to 1). |
| Euclidean | `DenseMetric::Euclidean` | `"euclidean"` | Lower distance is more similar. Results are returned as negative distance so higher scores remain "better." |
| Dot product | `DenseMetric::DotProduct` | `"dot_product"` | Higher = more similar. |

Current Node.js and Python connector parsers fall back to cosine for unknown metric strings.

**HNSW parameters** (Node.js/Python use defaults):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| m | `usize` | `16` | Maximum number of bi-directional links per node per layer. Higher values improve recall at the cost of memory and build time. |
| ef_construction | `usize` | `200` | Size of the dynamic candidate list during index construction. Higher values improve recall at the cost of slower inserts. |

---

## Data Model

### Node Records

A public, hydrated node record returned by read operations.

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| id | `u64` | `number` | `int` | Unique, auto-assigned node ID. Monotonically increasing. |
| labels | `Vec<String>` | `string[]` | `list[str]` | Complete node label set. |
| key | `String` | `string` | `str` | Unique key within the node's label identity. Do not repeat the label in the key unless it is part of an external source ID. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | User-defined properties. See [PropValue](#propvalue) for supported types. Lazily deserialized from MessagePack on first access. |
| weight | `f32` | `number` | `float` | Numeric weight. Default `1.0`. Used by pruning policies and scoring algorithms. |
| created_at | `i64` | `number` | `int` | Timestamp (ms) when the node was first created. |
| updated_at | `i64` | `number` | `int` | Timestamp (ms) of the most recent upsert. |
| dense_vector / denseVector | `Option<DenseVector>` | `number[] \| null` | `list[float] \| None` | Dense vector stored on the node. |
| sparse_vector / sparseVector | `Option<SparseVector>` | `SparseEntry[] \| null` | `list[tuple[int, float]] \| None` | Sparse vector stored on the node. |

Rust returns `NodeView`; Node.js returns `NodeView`; Python returns `NodeView`.

### Edge Records

A public, hydrated edge record returned by read operations.

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| id | `u64` | `number` | `int` | Unique, auto-assigned edge ID. |
| from / from_id | `u64` | `from: number` | `from_id: int` | Source node ID. |
| to / to_id | `u64` | `to: number` | `to_id: int` | Destination node ID. |
| label | `String` | `label: string` | `label: str` | Public edge label. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | User-defined properties. |
| weight | `f32` | `number` | `float` | Edge weight. Default `1.0`. |
| valid_from | `i64` | `number` | `int` | Start of the edge's validity window (ms). If omitted when writing, OverGraph uses the edge's `created_at` timestamp. |
| valid_to | `i64` | `number` | `int` | End of the edge's validity window (ms). If omitted when writing, OverGraph uses `i64::MAX` / no expiration. |
| created_at | `i64` | `number` | `int` | Creation timestamp (ms). |
| updated_at | `i64` | `number` | `int` | Last update timestamp (ms). |

Rust returns `EdgeView`; Node.js returns `EdgeView`; Python returns `EdgeView`.

### PropValue

Property values are strongly typed in the Rust core. Connector inputs use their host-language conversion rules and do not expose every Rust variant as a distinct writable type.

| Type | Rust | Node.js | Python | Notes |
|------|------|---------|--------|-------|
| Null | `PropValue::Null` | `null` | `None` | |
| Boolean | `PropValue::Bool(bool)` | `boolean` | `bool` | |
| Integer | `PropValue::Int(i64)` | `number` | `int` | Node.js and normal Python integer inputs write signed integers. |
| Unsigned | `PropValue::UInt(u64)` | Readable as `number` | Readable as `int` | Rust can construct this directly. Connector property inputs do not provide a separate unsigned marker. |
| Float | `PropValue::Float(f64)` | `number` | `float` | 64-bit IEEE 754. |
| String | `PropValue::String(String)` | `string` | `str` | UTF-8. |
| Bytes | `PropValue::Bytes(Vec<u8>)` | Readable as JSON array | `bytes` | Python can write `bytes`. Node.js property input is JSON-like and does not currently convert `Buffer` to `PropValue::Bytes`. |
| Array | `PropValue::Array(Vec<PropValue>)` | `any[]` | `list` | Heterogeneous array. |
| Map | `PropValue::Map(BTreeMap<String, PropValue>)` | `object` | `dict` | Nested properties. |

Properties are encoded with [MessagePack](https://msgpack.org) internally and converted lazily when accessed from Node.js or Python.

Connector property conversion is intentionally host-language shaped. Node.js writes JSON-like values (`null`, booleans, numbers, strings, arrays, and objects); it does not currently use `Buffer` as a bytes marker or expose a separate unsigned-integer marker. Python writes the same common values plus `bytes`; normal Python `int` inputs write signed integers. Rust callers can construct every `PropValue` variant directly.

Property storage keeps these variants intact. Predicate semantics are numeric-aware only for finite scalar numbers: signed integers, unsigned integers, and finite floats compare by exact numeric value for equality and range filters. Strings, bytes, booleans, nulls, arrays, maps, and non-finite floats keep exact non-numeric equality behavior and are excluded from numeric range indexes.

### IntoNodeLabels (Rust only)

Rust node-label APIs accept `impl IntoNodeLabels` for single-label and multi-label calls. Accepted input forms are `&str`, `String`, `&String`, `&[&str]`, `&[String]`, `Vec<String>`, `&[&str; N]`, and `&[String; N]`.

### Direction

Controls edge traversal direction. Used across traversal and graph analytics APIs.

| Value | Rust | Node.js | Python | Meaning |
|-------|------|---------|--------|---------|
| Outgoing | `Direction::Outgoing` | `"outgoing"` | `"outgoing"` | Follow edges in the `from → to` direction. |
| Incoming | `Direction::Incoming` | `"incoming"` | `"incoming"` | Follow edges in the `to → from` direction. |
| Both | `Direction::Both` | `"both"` | `"both"` | Follow edges in both directions (treat graph as undirected). |

### NodeLabelFilter / LabelMatchMode

Use `NodeLabelFilter` when callers need explicit `Any` or `All` semantics over node labels.

```rust
let any_user_or_admin = NodeLabelFilter {
    labels: vec!["User".into(), "Admin".into()],
    mode: LabelMatchMode::Any,
};

let both_user_and_admin = NodeLabelFilter {
    labels: vec!["User".into(), "Admin".into()],
    mode: LabelMatchMode::All,
};
```

```python
any_user_or_admin = {"labels": ["User", "Admin"], "mode": "any"}
both_user_and_admin = {"labels": ["User", "Admin"], "mode": "all"}
```

```javascript
const anyUserOrAdmin = { labels: ['User', 'Admin'], mode: 'any' };
const bothUserAndAdmin = { labels: ['User', 'Admin'], mode: 'all' };
```

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| labels | `Vec<String>` | `labels: string[]` | `"labels": list[str]` | Public node labels to match. Must be non-empty and contain no duplicates. |
| mode | `LabelMatchMode` | `mode: "any" \| "all"` | `"mode": "any" \| "all"` | `Any`/`"any"` matches nodes with at least one listed label. `All`/`"all"` matches nodes with every listed label. |

---

## Catalog APIs

Catalog APIs explicitly manage or inspect the node-label and edge-label token catalog. Ordinary graph APIs accept and return names; catalog diagnostics are the only public surface that exposes numeric token IDs.

### ensure_node_label / ensure_edge_label

Ensure a catalog token exists for a public node label or edge label and return its diagnostic token ID.

```rust
let user_label_id = db.ensure_node_label("User")?;
let created_label_id = db.ensure_edge_label("CREATED")?;
```

```javascript
const userLabelId = db.ensureNodeLabel('User');
const createdLabelId = db.ensureEdgeLabel('CREATED');
```

```python
user_label_id = db.ensure_node_label("User")
created_label_id = db.ensure_edge_label("CREATED")
```

These methods are optional for normal writes: `upsert_node`, `upsert_edge`, batch writes, graph patch, and write transactions auto-create missing names durably. Use explicit ensures when you want catalog IDs for diagnostics or want to prepare names before writes.

### get_node_label_id / get_edge_label_id

Read-only lookup from public name to diagnostic token ID.

```rust
let id = db.get_node_label_id("User")?;
let edge_id = db.get_edge_label_id("CREATED")?;
```

```javascript
const id = db.getNodeLabelId('User');
const edgeId = db.getEdgeLabelId('CREATED');
```

```python
id = db.get_node_label_id("User")
edge_id = db.get_edge_label_id("CREATED")
```

Returns `None`/`null` when the name is unknown.

### get_node_label / get_edge_label

Diagnostic reverse lookup from token ID to public name.

```rust
let label = db.get_node_label(label_id)?;
let edge_label = db.get_edge_label(label_id)?;
```

```javascript
const label = db.getNodeLabel(labelId);
const edgeLabel = db.getEdgeLabel(labelId);
```

```python
label = db.get_node_label(label_id)
edge_label = db.get_edge_label(label_id)
```

The node and edge `label_id` / `labelId` arguments are catalog token IDs, not normal graph API inputs.

### list_node_labels / list_edge_labels

List published catalog entries.

```rust
let labels = db.list_node_labels()?;
let edge_labels = db.list_edge_labels()?;
```

```javascript
const labels = db.listNodeLabels();
const edgeLabels = db.listEdgeLabels();
```

```python
labels = db.list_node_labels()
edge_labels = db.list_edge_labels()
```

| Entry | Rust fields | Node.js fields | Python fields |
|-------|-------------|----------------|---------------|
| Node label | `label`, `label_id` | `label`, `labelId` | `label`, `label_id` |
| Edge label | `label`, `label_id` | `label`, `labelId` | `label`, `label_id` |

`label_id` and `labelId` in these entries are diagnostic catalog metadata. Do not use them as input to ordinary node, edge, query, traversal, or vector APIs.

---

## Node Operations

### upsert_node

Creates a new node or updates an existing one. If the key already resolves to the same node through any supplied label, the node is updated in place; if the same key resolves to different nodes across supplied labels, the write is rejected as a conflict.

**Rust**
```rust
// Also accepts multiple labels: &["User", "Admin"]
let id = db.upsert_node("User", "alice", UpsertNodeOptions {
    props: BTreeMap::from([("role".into(), PropValue::String("admin".into()))]),
    weight: 1.0,
    ..Default::default()
})?;
```

**Node.js**
```javascript
// Also accepts multiple labels: ['User', 'Admin']
const id = db.upsertNode('User', 'alice', {
  props: { role: 'admin' },
  weight: 1.0,
});
```

**Python**
```python
# Also accepts multiple labels: ["User", "Admin"]
id = db.upsert_node("User", "alice", props={"role": "admin"}, weight=1.0)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| labels | `impl IntoNodeLabels` | `string \| string[]` | `str \| list[str]` | Yes | — | One or more public node labels. |
| key | `&str` | `string` | `str` | Yes | — | Unique key scoped by node labels. If the supplied label set and key resolve to an existing node, it is updated. |
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

- **Upsert semantics**: On insert, allocates a new ID, sets `created_at` and `updated_at` to the current time. On update, keeps the original `created_at`, refreshes `updated_at`, and replaces labels, props, weight, and vectors.
- **Atomicity**: The write is applied to the WAL and memtable in a single operation.
- **Performance**: ~4ms per call in `Immediate` sync mode (dominated by `fsync`). Use [`batch_upsert_nodes`](#batch_upsert_nodes) for bulk operations where a single fsync is shared across the batch.

---

### get_node

Retrieves a node by its ID.

**Rust**
```rust
if let Some(node) = db.get_node(id)? {
    println!("labels={:?}, key={}", node.labels, node.key);
}
```

**Node.js**
```javascript
const node = db.getNode(id);
if (node) console.log(node.labels, node.key);
```

**Python**
```python
node = db.get_node(id)
if node:
    print(node.labels, node.key)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Node ID to retrieve. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Option<NodeView>, EngineError>` | `NodeView \| null` | `NodeView \| None` |

Returns `None`/`null` if the node does not exist or has been deleted.

#### Performance

~38ns per lookup (memtable hot path). Segment reads require I/O but are mmap-accelerated.

---

### get_node_by_key

Looks up a node by its `(label, key)` pair. Uses the label-scoped key lookup/index for fast lookup.

**Rust**
```rust
let node = db.get_node_by_key("User", "alice")?;
```

**Node.js**
```javascript
const node = db.getNodeByKey('User', 'alice');
```

**Python**
```python
node = db.get_node_by_key("User", "alice")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Node label. |
| key | `&str` | `string` | `str` | Yes | Node key within the label. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Option<NodeView>, EngineError>` | `NodeView \| null` | `NodeView \| None` |

Returns `None`/`null` if no node with that `(label, key)` exists.

---

### add_node_label / remove_node_label

Node label-set mutation helpers. These update a node's label set without changing its ID, key, properties, weight, or vectors.

```rust
let added = db.add_node_label(node_id, "Admin")?;
let removed = db.remove_node_label(node_id, "Trial")?;
```

```javascript
const added = db.addNodeLabel(nodeId, 'Admin');
const removed = db.removeNodeLabel(nodeId, 'Trial');
```

```python
added = db.add_node_label(node_id, "Admin")
removed = db.remove_node_label(node_id, "Trial")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| id | `u64` | `number` | `int` | Yes | Node ID to mutate. |
| label | `&str` | `string` | `str` | Yes | Public node label to add or remove. |

#### Returns

| Rust | Node.js | Python | Description |
|------|---------|--------|-------------|
| `Result<bool, EngineError>` | `boolean` | `bool` | `true` when the node's label set changed, `false` when the requested label was already present for add or absent for remove. |

#### Behavior

- Adding a label auto-creates the label token when needed.
- Adding a label fails if another node already owns the same `(label, key)` identity.
- Removing an unknown or absent label returns `false`.
- Removing the last remaining node label returns an error.

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
    NodeInput {
        labels: vec!["User".into()],
        key: "alice".into(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    },
    NodeInput {
        labels: vec!["User".into(), "Admin".into()],
        key: "bob".into(),
        props: BTreeMap::from([("role".into(), PropValue::String("viewer".into()))]),
        weight: 0.8,
        dense_vector: None,
        sparse_vector: None,
    },
];
let ids = db.batch_upsert_nodes(inputs)?;
```

**Node.js**
```javascript
const ids = db.batchUpsertNodes([
  { labels: ['User'], key: 'alice', weight: 1.0 },
  { labels: ['User', 'Admin'], key: 'bob', weight: 0.8, props: { role: 'viewer' } },
]);
// ids is a Float64Array
```

**Python**
```python
ids = db.batch_upsert_nodes([
    {"labels": ["User"], "key": "alice", "weight": 1.0},
    {"labels": ["User", "Admin"], "key": "bob", "weight": 0.8, "props": {"role": "viewer"}},
])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| nodes | `Vec<NodeInput>` | `NodeInput[]` | `list[dict]` | Yes | Array of node inputs. Each element has the same fields as [`upsert_node`](#upsert_node) parameters. |

**NodeInput fields:**

| Field | Rust | Node.js | Python dict key | Required | Default | Description |
|-------|------|---------|-----------------|----------|---------|-------------|
| labels | `labels: Vec<String>` | `labels: string \| string[]` | `"labels"` | Yes | — | One or more node labels. Node.js accepts a single string or a non-empty string array for dict-based node inputs. |
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
// nodes[0] is Option<NodeView> for ID 1, etc.
```

**Node.js**
```javascript
const nodes = db.getNodes([1, 2, 3]);
// nodes[0] is NodeView | null for ID 1, etc.
```

**Python**
```python
nodes = db.get_nodes([1, 2, 3])
# nodes[0] is NodeView | None for ID 1, etc.
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| ids | `&[u64]` | `number[]` | `list[int]` | Yes | Array of node IDs to fetch. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<Option<NodeView>>, EngineError>` | `(NodeView \| null)[]` | `list[NodeView \| None]` |

An array the same length as the input, where each element is the node record or `None`/`null` if that ID doesn't exist.

---

### get_nodes_by_keys

Batch-retrieves multiple nodes by `(label, key)` pairs.

**Rust**
```rust
let nodes = db.get_nodes_by_keys(&[
    NodeKeyQuery { label: "User".into(), key: "alice".into() },
    NodeKeyQuery { label: "User".into(), key: "bob".into() },
])?;
```

**Node.js**
```javascript
const nodes = db.getNodesByKeys([
  { label: 'User', key: 'alice' },
  { label: 'User', key: 'bob' },
]);
```

**Python**
```python
nodes = db.get_nodes_by_keys([
    {"labels": ["User"], "key": "alice"},
    {"labels": ["User"], "key": "bob"},
])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| keys | `&[NodeKeyQuery]` | `KeyQuery[]` | `list[dict]` | Yes | Array of key lookups. Python uses `{"labels": "User" \| ["User"], "key": ...}` and requires exactly one label because keys are label-scoped. |

**KeyQuery** (Node.js):
```typescript
{ label: string, key: string }
```

#### Returns

Same shape as [`get_nodes`](#get_nodes): an array of node records or `None`/`null` in input order.

---

## Edge Operations

### upsert_edge

Creates a new edge or updates an existing one. When `edge_uniqueness` is enabled, edges are identified by the `(from, to, label)` triple.

**Rust**
```rust
let id = db.upsert_edge(alice_id, project_id, "WORKS_ON", UpsertEdgeOptions {
    props: BTreeMap::from([("since".into(), PropValue::String("2024".into()))]),
    weight: 1.0,
    ..Default::default()
})?;
```

**Node.js**
```javascript
const id = db.upsertEdge(aliceId, projectId, 'WORKS_ON', {
  props: { since: '2024' },
  weight: 1.0,
});
```

**Python**
```python
id = db.upsert_edge(alice_id, project_id, "WORKS_ON",
    props={"since": "2024"}, weight=1.0)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| from | `u64` | `number` | `int` | Yes | — | Source node ID. |
| to | `u64` | `number` | `int` | Yes | — | Destination node ID. |
| label | `&str` | `string` | `str` | Yes | — | Public edge label such as `"WORKS_ON"` or `"KNOWS"`. |
| props | `BTreeMap<String, PropValue>` | `Record<string, any>` | `dict[str, Any]` | No | `{}` | Edge properties. Replaced entirely on update. |
| weight | `f32` | `number` | `float` | No | `1.0` | Edge weight. Used by shortest path (as cost), top-k scoring, and pruning. |
| valid_from | `Option<i64>` | `number` | `int` | No | edge `created_at` | Start of the edge's temporal validity window (ms). Edges with `valid_from > at_epoch` are excluded from temporal queries. |
| valid_to | `Option<i64>` | `number` | `int` | No | `i64::MAX` (no expiration) | End of the validity window (ms). Edges with `valid_to <= at_epoch` are excluded from temporal queries. See [`invalidate_edge`](#invalidate_edge). |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u64, EngineError>` | `number` | `int` |

The edge ID.

#### Behavior

- **With `edge_uniqueness` enabled**: If an edge with the same `(from, to, label)` exists, it is updated and the existing ID is returned. Otherwise a new edge is created.
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

`EdgeView` / `EdgeView` / `EdgeView`, or `None`/`null` if the edge doesn't exist or has been deleted.

---

### get_edge_by_triple

Looks up an edge by its `(from, to, label)` triple. Only meaningful when `edge_uniqueness` is enabled.

**Rust**
```rust
let edge = db.get_edge_by_triple(alice_id, project_id, "WORKS_ON")?;
```

**Node.js**
```javascript
const edge = db.getEdgeByTriple(aliceId, projectId, 'WORKS_ON');
```

**Python**
```python
edge = db.get_edge_by_triple(alice_id, project_id, "WORKS_ON")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| from | `u64` | `number` | `int` | Yes | Source node ID. |
| to | `u64` | `number` | `int` | Yes | Destination node ID. |
| label | `&str` | `string` | `str` | Yes | Edge label. |

#### Returns

`EdgeView` / `EdgeView` / `EdgeView`, or `None`/`null`.

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
| `Result<Option<EdgeView>, EngineError>` | `EdgeView \| null` | `EdgeView \| None` |

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
    EdgeInput {
        from: 1,
        to: 2,
        label: "WORKS_ON".into(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    },
    EdgeInput {
        from: 1,
        to: 3,
        label: "WORKS_ON".into(),
        props: BTreeMap::new(),
        weight: 0.5,
        valid_from: None,
        valid_to: None,
    },
];
let ids = db.batch_upsert_edges(inputs)?;
```

**Node.js**
```javascript
const ids = db.batchUpsertEdges([
  { from: 1, to: 2, label: 'WORKS_ON', weight: 1.0 },
  { from: 1, to: 3, label: 'WORKS_ON', weight: 0.5 },
]);
```

**Python**
```python
ids = db.batch_upsert_edges([
    {"from_id": 1, "to_id": 2, "label": "WORKS_ON", "weight": 1.0},
    {"from_id": 1, "to_id": 3, "label": "WORKS_ON", "weight": 0.5},
])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| edges | `Vec<EdgeInput>` | `EdgeInput[]` | `list[dict]` | Yes | Array of edge inputs. |

**EdgeInput fields:**

| Field | Rust | Node.js | Python dict key | Required | Default | Description |
|-------|------|---------|-----------------|----------|---------|-------------|
| from | `u64` | `from: number` | `"from_id"` | Yes | — | Source node ID. |
| to | `u64` | `to: number` | `"to_id"` | Yes | — | Destination node ID. |
| label | `String` | `label: string` | `"label"` | Yes | — | Edge label. |
| props | `BTreeMap<String, PropValue>` | `props: object` | `"props"` | No | `{}` | Properties. |
| weight | `f32` | `weight: number` | `"weight"` | No | `1.0` | Weight. |
| valid_from | `Option<i64>` | `validFrom: number` | `"valid_from"` | No | edge `created_at` | Validity start. |
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

Array of edge records or `None`/`null` in input order.

---

## Atomic Operations

### graph_patch

Applies multiple operations atomically in a single WAL batch: node upserts, edge upserts, edge invalidations, and deletes.

**Rust**
```rust
let result = db.graph_patch(GraphPatch {
    upsert_nodes: vec![NodeInput {
        labels: vec!["User".into(), "Admin".into()],
        key: "carol".into(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }],
    upsert_edges: vec![EdgeInput {
        from: 1,
        to: 2,
        label: "WORKS_ON".into(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }],
    invalidate_edges: vec![(edge_id, now_ms)],
    delete_node_ids: vec![old_node_id],
    delete_edge_ids: vec![old_edge_id],
})?;
```

**Node.js**
```javascript
const result = db.graphPatch({
  upsertNodes: [{ labels: ['User'], key: 'carol' }],
  upsertEdges: [{ from: 1, to: 2, label: 'WORKS_ON' }],
  invalidateEdges: [{ edgeId: 5, validTo: Date.now() }],
  deleteNodeIds: [oldNodeId],
  deleteEdgeIds: [oldEdgeId],
});
```

**Python**
```python
result = db.graph_patch({
    "upsert_nodes": [{"labels": ["User"], "key": "carol"}],
    "upsert_edges": [{"from_id": 1, "to_id": 2, "label": "WORKS_ON"}],
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

### write transactions

Explicit write transactions stage ordered graph mutations locally, support bounded read-own-writes point lookups, and commit as one atomic WAL batch. Conflict detection is optimistic and write-target based: if a staged target changed after the transaction began, `commit` fails with a transaction conflict and no partial state is published.

Use transactions when later operations need local aliases from earlier staged upserts, or when a caller needs rollback before durability. Use `graph_patch` for simpler grouped atomic batches that do not need ordered local references.

Transaction reads are intentionally bounded. A transaction can read committed state from its begin snapshot plus its own staged writes for point/dedup lookups only: `get_node`, `get_edge`, `get_node_by_key`, and `get_edge_by_triple`. Traversal, vector search, property queries, pagination, export, analytics, prune-policy mutation, and maintenance APIs are not exposed on `WriteTxn`.

**Rust**
```rust
let mut txn = db.begin_write_txn()?;
let alice = txn.upsert_node_as("alice", &["User", "Admin"], "alice", UpsertNodeOptions::default())?;
let bob = txn.upsert_node_as("bob", "User", "bob", UpsertNodeOptions::default())?;
txn.upsert_edge_as("knows", alice.clone(), bob.clone(), "KNOWS", UpsertEdgeOptions::default())?;
assert!(txn.add_node_label(alice.clone(), "Manager")?);
assert!(txn.remove_node_label(alice.clone(), "Admin")?);
let staged = txn.get_node_by_key("User", "alice")?;
if let Some(view) = &staged {
    println!("staged labels: {:?}", view.labels);
}
let result = txn.commit()?;
```

**Node.js**
```javascript
const txn = db.beginWriteTxn();
txn.stage([
  { op: 'upsertNode', alias: 'alice', labels: ['User', 'Admin'], key: 'alice' },
  { op: 'upsertNode', alias: 'bob', labels: ['User'], key: 'bob' },
  { op: 'upsertEdge', alias: 'knows', from: { local: 'alice' }, to: { local: 'bob' }, label: 'KNOWS' },
]);
txn.addNodeLabel({ local: 'bob' }, 'Trial');
const staged = txn.getNode({ local: 'alice' });
const result = txn.commit();
```

**Python**
```python
txn = db.begin_write_txn()
txn.stage([
    {"op": "upsert_node", "alias": "alice", "labels": ["User", "Admin"], "key": "alice"},
    {"op": "upsert_node", "alias": "bob", "labels": ["User"], "key": "bob"},
    {"op": "upsert_edge", "alias": "knows", "from": {"local": "alice"}, "to": {"local": "bob"}, "label": "KNOWS"},
])
txn.add_node_label({"local": "bob"}, "Trial")
staged = txn.get_node({"local": "alice"})
result = txn.commit()
```

#### Transaction Surface

| Operation | Rust | Node.js | Python |
|-----------|------|---------|--------|
| Begin | `begin_write_txn()` | `beginWriteTxn()` | `begin_write_txn()` |
| Stage node | `upsert_node`, `upsert_node_as` | `upsertNode`, `upsertNodeAs` | `upsert_node`, `upsert_node_as` |
| Mutate node labels | `add_node_label`, `remove_node_label` | `addNodeLabel`, `removeNodeLabel` | `add_node_label`, `remove_node_label` |
| Stage edge | `upsert_edge`, `upsert_edge_as` | `upsertEdge`, `upsertEdgeAs` | `upsert_edge`, `upsert_edge_as` |
| Bulk ordered stage | `stage_intents(Vec<TxnIntent>)` | `stage(operations)` | `stage(operations)` |
| Reads | `get_node`, `get_edge`, `get_node_by_key`, `get_edge_by_triple` | same camelCase names | same snake_case names |
| Finish | `commit`, `rollback` | `commit`, `rollback` | `commit`, `rollback` |

#### Rust Transaction DTOs

Rust exposes the transaction reference and intent objects directly:

| Object | Variants / fields | Description |
|--------|-------------------|-------------|
| `TxnNodeRef` | `Id(u64)`, `Key { label, key }`, `Local(TxnLocalRef)` | Node target for transaction writes and bounded transaction reads. `Key` is single-label scoped. |
| `TxnEdgeRef` | `Id(u64)`, `Triple { from, to, label }`, `Local(TxnLocalRef)` | Edge target by ID, by endpoint refs plus edge label, or by local transaction ref. |
| `TxnIntent::UpsertNode` | `alias`, `labels`, `key`, `options` | Ordered staged node upsert. `labels` is the complete node-label set for the write. |
| `TxnIntent::UpsertEdge` | `alias`, `from`, `to`, `label`, `options` | Ordered staged edge upsert using transaction node refs. |
| `TxnIntent::DeleteNode` | `target` | Ordered staged node delete. |
| `TxnIntent::DeleteEdge` | `target` | Ordered staged edge delete. |
| `TxnIntent::InvalidateEdge` | `target`, `valid_to` | Ordered staged temporal edge invalidation. |

#### Builder Methods

| Method | Required inputs | Optional inputs | Returns |
|--------|-----------------|-----------------|---------|
| `upsert_node` / `upsertNode` | `labels`, `key` | node upsert options: `props`, `weight`, `dense_vector` / `denseVector`, `sparse_vector` / `sparseVector` | node ref addressable by key |
| `upsert_node_as` / `upsertNodeAs` | `alias`, `labels`, `key` | node upsert options | local node ref `{ local: alias }` |
| `add_node_label` / `remove_node_label` | node ref, `label` | — | `bool` changed flag |
| `upsert_edge` / `upsertEdge` | `from`, `to`, `label` | edge upsert options: `props`, `weight`, `valid_from` / `validFrom`, `valid_to` / `validTo` | edge ref addressable by triple |
| `upsert_edge_as` / `upsertEdgeAs` | `alias`, `from`, `to`, `label` | edge upsert options | local edge ref `{ local: alias }` |
| `delete_node` / `deleteNode` | node ref | — | `void` / `None` |
| `delete_edge` / `deleteEdge` | edge ref | — | `void` / `None` |
| `invalidate_edge` / `invalidateEdge` | edge ref, `valid_to` / `validTo` | — | `void` / `None` |
| `stage` / `stage_intents` | ordered operation payloads | — | `void` / `None` |

#### Ordered Operation Payloads

Node.js uses camelCase fields and op names: `upsertNode`, `upsertEdge`, `deleteNode`, `deleteEdge`, `invalidateEdge`.

Python uses snake_case fields and op names: `upsert_node`, `upsert_edge`, `delete_node`, `delete_edge`, `invalidate_edge`.

References are one of:

| Ref kind | Node.js | Python |
|----------|---------|--------|
| Node by ID | `{ id }` | `{"id": id}` |
| Node by key | `{ labels, key }` | `{"labels": label_or_single_label_list, "key": key}` |
| Node local alias | `{ local }` | `{"local": local}` |
| Edge by ID | `{ id }` | `{"id": id}` |
| Edge by triple | `{ from, to, label }` | `{"from": from, "to": to, "label": label}` |
| Edge local alias | `{ local }` | `{"local": local}` |

Node-by-key transaction refs use `labels` and `key` but are still single-label scoped:
`labels` may be a string or a one-item list/array.

Aliases are optional, process-local, and never persisted. When present, aliases must be unique within the transaction across node aliases and unique across edge aliases.

#### Transaction Read Views

`get_node` and `get_node_by_key` on a transaction return a transaction node view:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| id | `Option<u64>` | `id?: number` | `"id": int \| None` | Committed node ID when already known; `None`/omitted for staged creates that allocate an ID at commit. |
| local | `Option<TxnLocalRef>` | `local?: string` | `"local": str \| None` | Local alias for aliased staged records. Internal unaliased slots are not exposed as strings. |
| labels | `Vec<String>` | `labels: string[]` | `"labels": list[str]` | Complete node label set visible inside the transaction. |
| key | `String` | `key` | `"key"` | Node key. |
| props | `BTreeMap<String, PropValue>` | `props` | `"props"` | Node properties visible inside the transaction. |
| created_at / updated_at | `Option<i64>` | `createdAt?` / `updatedAt?` | `"created_at"` / `"updated_at"` | Present for committed records; absent/`None` for staged creates before commit. |
| weight | `f32` | `weight` | `"weight"` | Node weight. |
| dense_vector / sparse_vector | `Option<DenseVector>` / `Option<SparseVector>` | `denseVector?` / `sparseVector?` | `"dense_vector"` / `"sparse_vector"` | Staged or committed vectors when present. |

`get_edge` and `get_edge_by_triple` return a transaction edge view:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| id | `id?: number` | `"id": int \| None` | Committed edge ID when already known; `None`/omitted for staged creates that allocate an ID at commit. |
| local | `local?: string` | `"local": str \| None` | Local alias for aliased staged records. |
| from / to | `from` / `to` | `"from"` / `"to"` | Endpoint refs visible inside the transaction. |
| label | `label` | `"label"` | Edge label. |
| props | `props` | `"props"` | Edge properties visible inside the transaction. |
| created_at / updated_at | `createdAt?` / `updatedAt?` | `"created_at"` / `"updated_at"` | Present for committed records; absent/`None` for staged creates before commit. |
| weight | `weight` | `"weight"` | Edge weight. |
| valid_from / valid_to | `validFrom?` / `validTo?` | `"valid_from"` / `"valid_to"` | Temporal validity bounds when present. |

#### Commit Result

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node IDs | `node_ids: Vec<u64>` | `nodeIds: Float64Array` | `node_ids: list[int]` | IDs returned by node upsert intents in input order. |
| edge IDs | `edge_ids: Vec<u64>` | `edgeIds: Float64Array` | `edge_ids: list[int]` | IDs returned by edge upsert intents in input order. |
| node aliases | `local_node_ids` | `nodeAliases` | `node_aliases` | Alias-to-node-ID map for aliased staged node upserts. |
| edge aliases | `local_edge_ids` | `edgeAliases` | `edge_aliases` | Alias-to-edge-ID map for aliased staged edge upserts. |

After `commit()` or `rollback()`, the transaction handle is closed. Further use fails with `TxnClosed` / `transaction is closed`.

#### Conflict Handling

`TxnConflict` means the transaction definitely did not commit: no WAL entry was appended and no partial state was published. The caller can retry by starting a new transaction, restaging the desired operations, re-reading any needed point records, and committing again. OverGraph does not automatically retry because conflict-safe retry policy depends on caller intent.

---

## Label and Edge-Label Queries

### nodes_by_labels

Returns all node IDs containing every supplied node label.

**Rust**
```rust
let ids: Vec<u64> = db.nodes_by_labels("User")?;
let admin_ids: Vec<u64> = db.nodes_by_labels(vec!["User".into(), "Admin".into()])?;
```

**Node.js**
```javascript
const ids = db.nodesByLabels('User'); // Float64Array
const adminIds = db.nodesByLabels(['User', 'Admin']);
```

**Python**
```python
ids = db.nodes_by_labels("User")  # IdArray (lazy)
ids_list = ids.to_list()      # materialize to list[int]
admin_ids = db.nodes_by_labels(["User", "Admin"])
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| labels | `impl IntoNodeLabels` | `string \| string[]` | `str \| list[str]` | Yes | Label or labels to match. Nodes must contain every supplied node label. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

All matching node IDs. Filtered (excludes deleted/pruned nodes).

**Python `IdArray`**: A lazy wrapper that avoids copying IDs to Python memory until accessed. Supports `len()`, indexing (`arr[i]`), iteration, `in` operator, and `to_list()`.

#### Performance

Single-label input uses the direct per-label fast path. Multi-label input uses `All` semantics, drives from the best label posting, and metadata-verifies current label membership. Use [`query_node_ids`](#query_node_ids) with `NodeLabelFilter` when `Any` semantics are needed.

---

### edges_by_label

Returns all edge IDs of a given edge label.

**Rust**
```rust
let ids: Vec<u64> = db.edges_by_label("WORKS_ON")?;
```

**Node.js**
```javascript
const ids = db.edgesByLabel('WORKS_ON'); // Float64Array
```

**Python**
```python
ids = db.edges_by_label("WORKS_ON")  # IdArray (lazy)
ids_list = ids.to_list()             # materialize to list[int]
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Public edge label to match, such as `"WORKS_ON"` or `"KNOWS"`. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

All matching live edge IDs. Tombstoned edges are excluded. Unknown edge labels return an empty result.

**Python `IdArray`**: A lazy wrapper that avoids copying IDs to Python memory until accessed. Supports `len()`, indexing (`arr[i]`), iteration, `in` operator, and `to_list()`.

#### Performance

Uses the edge-label posting index and does not hydrate edge records. Edges have exactly one public label, so this API accepts a single label string. Use [`query_edge_ids`](#query_edge_ids) when you need additional edge predicates.

---

### get_nodes_by_labels

Returns full node records for nodes containing every supplied node label.

```rust
let nodes: Vec<NodeView> = db.get_nodes_by_labels("User")?;
let admin_nodes: Vec<NodeView> =
    db.get_nodes_by_labels(vec!["User".into(), "Admin".into()])?;
```

```javascript
const nodes = db.getNodesByLabels('User'); // NodeView[]
const admins = db.getNodesByLabels(['User', 'Admin']);
```

```python
nodes = db.get_nodes_by_labels("User")  # list[NodeView]
admins = db.get_nodes_by_labels(["User", "Admin"])
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| labels | `impl IntoNodeLabels` / `string \| string[]` / `str \| list[str]` | Yes | Label or labels to match. Nodes must contain every supplied node label. |

#### Returns

Array of full node records. Includes all public fields (id, labels, key, props, weight, timestamps, vectors).

Multi-label input always uses `All` semantics. Use [`query_nodes`](#query_nodes) with `NodeLabelFilter` when `Any` semantics are needed.

---

### get_edges_by_label

Returns full edge records for all edges of a given edge label.

**Rust**
```rust
let edges: Vec<EdgeView> = db.get_edges_by_label("WORKS_ON")?;
```

**Node.js**
```javascript
const edges = db.getEdgesByLabel('WORKS_ON'); // EdgeView[]
```

**Python**
```python
edges = db.get_edges_by_label("WORKS_ON")  # list[EdgeView]
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Public edge label to match. |

#### Returns

Array of full edge records. Includes all public edge fields: id, endpoints (`from`/`to` in Rust and Node.js, `from_id`/`to_id` in Python), label, props, weight, timestamps, and validity window.

Unknown edge labels return an empty array. Tombstoned edges are excluded.

#### Performance

Uses the edge-label posting index to collect matching IDs, then batch-hydrates the matching records. Use [`edges_by_label`](#edges_by_label) when IDs are enough.

---

### count_nodes_by_labels

Returns the count of nodes containing every supplied node label.

```rust
let count: u64 = db.count_nodes_by_labels("User")?;
let admin_count: u64 =
    db.count_nodes_by_labels(vec!["User".into(), "Admin".into()])?;
```

```javascript
const count = db.countNodesByLabels('User');
```

```python
count = db.count_nodes_by_labels("User")
admin_count = db.count_nodes_by_labels(["User", "Admin"])
```

Count uses metadata-only verification and does not hydrate node records or allocate the final ID result vector. Multi-label input always uses `All` semantics. Use [`query_node_ids`](#query_node_ids) with `NodeLabelFilter` when `Any` semantics are needed.

---

### count_edges_by_label

Returns the count of live edges of a given edge label.

**Rust**
```rust
let count: u64 = db.count_edges_by_label("WORKS_ON")?;
```

**Node.js**
```javascript
const count = db.countEdgesByLabel('WORKS_ON');
```

**Python**
```python
count = db.count_edges_by_label("WORKS_ON")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Public edge label to count. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u64, EngineError>` | `number` | `int` |

Unknown edge labels return `0`. Tombstoned edges are excluded.

#### Performance

Counts through the edge-label posting path without hydrating edge records.

---

## Property Index Management

Property indexes are optional declarations on ordered node or edge field lists. A field can be a
user property or supported record metadata; public query methods stay the same whether or not you
declare an index.

Lifecycle rules:
- `ensure_node_property_index` registers an equality or range declaration over one to eight ordered node fields and starts background build work when needed.
- `ensure_edge_property_index` does the same for edge fields, scoped by edge label.
- `list_node_property_indexes` exposes declaration kind, lifecycle state, and any last error from the published read snapshot, so `Ready` means new public reads can use the same ready catalog.
- `list_edge_property_indexes` exposes the same state for edge declarations.
- `find_nodes`, `find_nodes_paged`, `find_nodes_range`, and `find_nodes_range_paged` use declaration-backed execution only when a matching declaration is `Ready`.
- `query_edge_ids`, `query_edges`, and `query_graph_rows` may use ready edge-property declarations as candidate sources while still verifying final edge filters.
- If a declaration is absent, `Building`, `Failed`, or cannot be used for a specific lookup, OverGraph falls back to the same public query API for that call.

### ensure_node_property_index

Ensures an optional secondary index declaration for a node field list.

**Rust**
```rust
let eq = db.ensure_node_property_index(
    "User",
    SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
)?;

let range = db.ensure_node_property_index(
    "User",
    SecondaryIndexSpec::range(vec![
        SecondaryIndexField::property("tenant_id"),
        SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
    ]),
)?;
```

**Node.js**
```javascript
const eq = db.ensureNodePropertyIndex('User', {
  kind: 'equality',
  fields: [{ source: 'property', key: 'role' }],
});

const range = db.ensureNodePropertyIndex('User', {
  kind: 'range',
  fields: [
    { source: 'property', key: 'tenant_id' },
    { source: 'metadata', field: 'updated_at' },
  ],
});
```

**Python**
```python
eq = db.ensure_node_property_index(
    "User",
    {"kind": "equality", "fields": [{"source": "property", "key": "role"}]},
)

range_info = db.ensure_node_property_index(
    "User",
    {
        "kind": "range",
        "fields": [
            {"source": "property", "key": "tenant_id"},
            {"source": "metadata", "field": "updated_at"},
        ],
    },
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Restrict the declaration to this node label. |
| spec | `SecondaryIndexSpec` | `SecondaryIndexSpec` | `dict` / `SecondaryIndexSpecLike` | Yes | Ordered field list plus `equality` or `range` kind. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<NodePropertyIndexInfo, EngineError>` | `NodePropertyIndexInfo` | `NodePropertyIndexInfo` |

The current declaration info.

#### Behavior

- Equality declarations use `SecondaryIndexKind::Equality` or `"equality"`. Finite scalar numeric equality is semantic across signed integers, unsigned integers, and finite floats; string equality and other non-numeric equality remain exact.
- Range declarations use `SecondaryIndexKind::Range` or `"range"`. Range narrowing applies to the first non-equality field after a constrained left prefix and is domainless numeric over finite scalar numeric values across signed integers, unsigned integers, and finite floats.
- Range indexes exclude non-finite floats, non-numeric values, arrays, and maps.
- Re-ensuring an existing declaration returns the existing declaration info.
- Re-ensuring a `Failed` declaration retries it by moving it back to `Building`.
- The field list must contain one to eight fields. Duplicate fields are rejected. A property named like metadata, such as `updated_at`, stays a property when declared as `{ source: "property" }`.
- Supported node metadata fields are `id`, `key`, `weight`, `created_at`, and `updated_at`. Supported edge metadata fields are listed under [`ensure_edge_property_index`](#ensure_edge_property_index). Node declarations reject edge metadata fields.
- A declaration becoming `Ready` is what enables declaration-backed routing. Callers do not switch to a different query method.
- Compound candidates obey left-prefix semantics: equality predicates must constrain fields from the start of the declaration, optionally followed by one range field. If predicates skip the left prefix, explain warnings may include `CompoundIndexPrefixNotSatisfied` / `compound_index_prefix_not_satisfied`.
- Ready equality and range index candidates are still verified against the latest visible records before results are returned.

---

### drop_node_property_index

Drops an optional node-property secondary index declaration.

**Rust**
```rust
let removed = db.drop_node_property_index(
    "User",
    SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
)?;
```

**Node.js**
```javascript
const removed = db.dropNodePropertyIndex('User', {
  kind: 'equality',
  fields: [{ source: 'property', key: 'role' }],
});
```

**Python**
```python
removed = db.drop_node_property_index(
    "User",
    {"kind": "equality", "fields": [{"source": "property", "key": "role"}]},
)
```

#### Parameters

Same parameters and kind values as [`ensure_node_property_index`](#ensure_node_property_index).

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<bool, EngineError>` | `boolean` | `bool` |

`true` if a declaration existed and was removed, `false` otherwise.

#### Behavior

- Dropping a declaration removes the optional declaration state and subsequent declaration-backed routing for that property.
- Property queries continue to work after a drop. They fall back to scan through the same public query APIs.

---

### list_node_property_indexes

Lists all optional node-property secondary index declarations.

**Rust**
```rust
let indexes = db.list_node_property_indexes()?;
```

**Node.js**
```javascript
const indexes = db.listNodePropertyIndexes();
```

**Python**
```python
indexes = db.list_node_property_indexes()
```

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Vec<NodePropertyIndexInfo>` | `Array<NodePropertyIndexInfo>` | `list[NodePropertyIndexInfo]` |

One entry per declaration.

---

### NodePropertyIndexInfo

User-facing declaration information returned by [`ensure_node_property_index`](#ensure_node_property_index) and [`list_node_property_indexes`](#list_node_property_indexes).

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| index_id | `u64` | `indexId: number` | `index_id: int` | Stable declaration ID. |
| label | `String` | `label: string` | `label: str` | Declared node label. |
| fields | `Vec<SecondaryIndexField>` | `fields: SecondaryIndexField[]` | `fields: list[dict]` | Ordered declaration fields. |
| kind | `SecondaryIndexKind` | `kind: string` | `kind: str` | `equality` or `range`. |
| state | `SecondaryIndexState` | `state: string` | `state: str` | `building`, `ready`, or `failed`. |
| last_error | `Option<String>` | `lastError: string \| null` | `last_error: str \| None` | Most recent build or validation failure, if any. |
| compound | `bool` | `compound: boolean` | `compound: bool` | True when the declaration has two or more fields. |

State meanings:
- `building`: the declaration exists, but the declaration-backed path is not live yet.
- `ready`: the declaration-backed path has full live coverage and may be used by matching queries.
- `failed`: the declaration could not be built or validated. Matching queries fall back to scan until the declaration is retried or dropped.

---

### ensure_edge_property_index

Ensures an optional secondary index declaration for an edge field list, scoped to one edge label.

**Rust**
```rust
let eq = db.ensure_edge_property_index(
    "WORKS_AT",
    SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
)?;

let range = db.ensure_edge_property_index(
    "WORKS_AT",
    SecondaryIndexSpec::range(vec![
        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
        SecondaryIndexField::property("score"),
    ]),
)?;
```

**Node.js**
```javascript
const eq = db.ensureEdgePropertyIndex('WORKS_AT', {
  kind: 'equality',
  fields: [{ source: 'property', key: 'role' }],
});

const range = db.ensureEdgePropertyIndex('WORKS_AT', {
  kind: 'range',
  fields: [
    { source: 'metadata', field: 'from' },
    { source: 'property', key: 'score' },
  ],
});
```

**Python**
```python
eq = db.ensure_edge_property_index(
    "WORKS_AT",
    {"kind": "equality", "fields": [{"source": "property", "key": "role"}]},
)

range_info = db.ensure_edge_property_index(
    "WORKS_AT",
    {
        "kind": "range",
        "fields": [
            {"source": "metadata", "field": "from"},
            {"source": "property", "key": "score"},
        ],
    },
)
```

Parameters, kind values, and lifecycle states match [`ensure_node_property_index`](#ensure_node_property_index), except `label` is the edge label.

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<EdgePropertyIndexInfo, EngineError>` | `EdgePropertyIndexInfo` | `EdgePropertyIndexInfo` |

#### Behavior

- Edge property declarations are edge-label-scoped. A property filter without an edge label cannot use an edge-label-scoped edge-property declaration as a direct-query anchor.
- Supported edge metadata fields are `id`, `from`, `to`, `weight`, `created_at`, `updated_at`, `valid_from`, and `valid_to`. Edge declarations reject node metadata fields.
- Ready edge declarations are candidate sources only. `query_edge_ids`, `query_edges`, and graph-row execution still verify edge metadata and edge property predicates before returning results.
- Direct `EdgeQuery` anchor legality is unchanged: edge property indexes improve planning inside legal direct edge queries, but do not make filter-only direct edge queries legal by themselves.
- Graph-row plans may choose a ready edge-property equality or range source as an edge anchor when it is cheaper than node-anchor expansion.

---

### drop_edge_property_index

Drops an optional edge-property secondary index declaration.

**Rust**
```rust
let removed = db.drop_edge_property_index(
    "WORKS_AT",
    SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
)?;
```

**Node.js**
```javascript
const removed = db.dropEdgePropertyIndex('WORKS_AT', {
  kind: 'equality',
  fields: [{ source: 'property', key: 'role' }],
});
```

**Python**
```python
removed = db.drop_edge_property_index(
    "WORKS_AT",
    {"kind": "equality", "fields": [{"source": "property", "key": "role"}]},
)
```

Same parameters and kind values as [`ensure_edge_property_index`](#ensure_edge_property_index). Returns `true` if a declaration existed and was removed, `false` otherwise.

---

### list_edge_property_indexes

Lists all optional edge-property secondary index declarations.

**Rust**
```rust
let indexes = db.list_edge_property_indexes()?;
```

**Node.js**
```javascript
const indexes = db.listEdgePropertyIndexes();
```

**Python**
```python
indexes = db.list_edge_property_indexes()
```

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<EdgePropertyIndexInfo>, EngineError>` | `Array<EdgePropertyIndexInfo>` | `list[EdgePropertyIndexInfo]` |

One entry per edge declaration.

---

### EdgePropertyIndexInfo

User-facing declaration information returned by [`ensure_edge_property_index`](#ensure_edge_property_index) and [`list_edge_property_indexes`](#list_edge_property_indexes).

Fields match [`NodePropertyIndexInfo`](#nodepropertyindexinfo), with the edge-label scope exposed as `label`: `index_id` / `indexId`, `label`, `fields`, `kind`, `state`, `last_error` / `lastError`, and `compound`.

---

### PropertyRangeBound

Bound object for [`find_nodes_range`](#find_nodes_range) and [`find_nodes_range_paged`](#find_nodes_range_paged).

**Rust**
```rust
let lower = PropertyRangeBound::Included(PropValue::Int(10));
let upper = PropertyRangeBound::Excluded(PropValue::Float(20.0));
```

**Node.js**
```javascript
const lower = { value: 10, inclusive: true, domain: 'int' };
const upper = { value: 20, inclusive: false, domain: 'float' };
```

**Python**
```python
lower = PropertyRangeBound(10, domain="int")
upper = PropertyRangeBound(20.0, inclusive=False, domain="float")
```

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| value | `PropValue` | `value: number` | `value: int \| float` | Numeric bound value. |
| inclusive | Encoded by enum variant | `inclusive?: boolean` | `inclusive: bool` | Inclusive when `true`, exclusive when `false`. |
| domain | Inferred from `PropValue` | `domain: string` | `domain: str` | Node.js and Python value-conversion hint. One of `int`, `uint`, or `float`. |

Notes:
- Range bounds must be finite scalar numeric values. Non-finite floats, non-numeric values, arrays, and maps are invalid bounds.
- Node.js and Python `domain` fields construct the exact host value variant for a bound or cursor. They are not range index declaration domains.
- Bounds may mix `int`, `uint`, and `float` conversion hints. OverGraph compares finite numeric values by exact numeric value.
- Empty finite numeric intervals return empty results.

---

### PropertyRangeCursor

Cursor object for [`find_nodes_range_paged`](#find_nodes_range_paged). The cursor key is `(value, node_id)`.

**Rust**
```rust
PropertyRangeCursor {
    value: PropValue::Int(20),
    node_id: 42,
}
```

**Node.js**
```javascript
{ value: 20, nodeId: 42, domain: 'int' }
```

**Python**
```python
PropertyRangeCursor(20, 42, domain="int")
```

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| value | `PropValue` | `value: number` | `value: int \| float` | Last value returned on the previous page. |
| node_id | `u64` | `nodeId: number` | `node_id: int` | Last node ID returned at that value. |
| domain | Inferred from `value` | `domain: string` | `domain: str` | Node.js and Python value-conversion hint for the cursor value. |

---

### PropertyRangePageRequest (Rust only)

Rust request object for [`find_nodes_range_paged`](#find_nodes_range_paged).

```rust
PropertyRangePageRequest {
    limit: Some(100),
    after: None,
}
```

| Field | Rust | Description |
|-------|------|-------------|
| limit | `Option<usize>` | Maximum node IDs to return. `None` means no explicit page size. |
| after | `Option<PropertyRangeCursor>` | Cursor from the previous page. `None` starts at the lower bound. |

---

### PropertyRangePageResult

Result object returned by [`find_nodes_range_paged`](#find_nodes_range_paged).

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<u64>` | `Float64Array` | `IdArray` | Node IDs in range order for this page. |
| next_cursor | `Option<PropertyRangeCursor>` | `nextCursor?: PropertyRangeCursor` | `next_cursor: PropertyRangeCursor \| None` | Cursor for the next page. Omitted / `None` on the last page. |

---

## Schema Management

Schemas are optional constraints on graph records. Databases are open by default: if no schema is
published for a label, matching writes are accepted by the normal API rules.

Schemas are not property indexes. Optional property indexes are performance declarations for query
routing; they do not enforce uniqueness or required fields. Graph schemas validate record shape
and values, but they do not add arbitrary property uniqueness constraints.

### Schema semantics

- Node schemas are label-scoped. A node can carry multiple labels, and every matching node schema
  composes against the final node record. If `User` and `Employee` both have schemas, a
  `["User", "Employee"]` node must satisfy both.
- Edge schemas are scoped by edge label. They can validate edge properties, weight, validity
  metadata, self-loop policy, and endpoint node labels.
- `set_node_schema` / `setNodeSchema` / `set_node_schema` and the edge equivalents validate
  existing matching live data before publishing the schema. Publication is serialized through the
  core write queue, so a large validation can block writes until it finishes.
- `check_node_schema` / `checkNodeSchema` / `check_node_schema` and the edge equivalents are
  non-publishing dry runs. They validate against a published read snapshot, return a bounded report,
  do not reserve labels permanently, and do not occupy the write queue. A clean dry run can still be
  invalidated by later writes before `set_*_schema` runs.
- Active schemas are enforced in the Rust core before WAL append. Rejected writes do not append to
  the WAL and do not partially mutate the memtable.
- Rust, Node.js, and Python expose parity APIs over the same Rust enforcement. Connectors convert
  DTOs and may reject malformed schema objects early, but they do not provide separate
  connector-side enforcement semantics.
- GQL mutations use the shared write path, so mutations that create, update, or leave live records
  must satisfy active schemas before commit. The supported GQL schema DDL subset is an adapter over
  the same graph-schema APIs described here.
- Manifest schema fields are diagnostic and introspection data. Do not treat raw manifest JSON as a
  user-editable persistence contract.

### Schema methods

| Operation | Rust | Node.js | Python | Description |
|-----------|------|---------|--------|-------------|
| Publish node schema | `set_node_schema(label, schema)` / `set_node_schema_with_options` | `setNodeSchema(label, schema, options?)` | `set_node_schema(label, schema, **options)` | Validate matching live nodes, then publish. |
| Dry-run node schema | `check_node_schema(label, schema, options)` | `checkNodeSchema(label, schema, options?)` | `check_node_schema(label, schema, **options)` | Snapshot-scoped report only. |
| Drop node schema | `drop_node_schema(label)` | `dropNodeSchema(label)` | `drop_node_schema(label)` | Remove the active node schema for a label. |
| Get node schema | `get_node_schema(label)` | `getNodeSchema(label)` | `get_node_schema(label)` | Return one schema info object or `None` / `null`. |
| List node schemas | `list_node_schemas()` | `listNodeSchemas()` | `list_node_schemas()` | Return published node schemas sorted by label. |
| Publish edge schema | `set_edge_schema(label, schema)` / `set_edge_schema_with_options` | `setEdgeSchema(label, schema, options?)` | `set_edge_schema(label, schema, **options)` | Validate matching live edges, then publish. |
| Dry-run edge schema | `check_edge_schema(label, schema, options)` | `checkEdgeSchema(label, schema, options?)` | `check_edge_schema(label, schema, **options)` | Snapshot-scoped report only. |
| Drop edge schema | `drop_edge_schema(label)` | `dropEdgeSchema(label)` | `drop_edge_schema(label)` | Remove the active edge schema for an edge label. |
| Get edge schema | `get_edge_schema(label)` | `getEdgeSchema(label)` | `get_edge_schema(label)` | Return one schema info object or `None` / `null`. |
| List edge schemas | `list_edge_schemas()` | `listEdgeSchemas()` | `list_edge_schemas()` | Return published edge schemas sorted by label. |
| Replace graph schema | `set_graph_schema(schema, options)` | `setGraphSchema(schema, options?)` / `setGraphSchemaAsync(...)` | `set_graph_schema(schema, **options)` / async | Atomically replace the node/edge schema catalog. `GraphSchema::default()` / `{}` clears it. |
| Alter graph schema | `alter_graph_schema(operations, options)` | `alterGraphSchema(operations, options?)` / `alterGraphSchemaAsync(...)` | `alter_graph_schema(operations, **options)` / async | Atomically set/add/drop selected node and edge schema targets. |
| Dry-run graph schema SET | `check_graph_schema_set(schema, options)` | `checkGraphSchemaSet(schema, options?)` / `checkGraphSchemaSetAsync(...)` | `check_graph_schema_set(schema, **options)` / async | Validate the proposed full replacement without publishing. |
| Dry-run graph schema ADD | `check_graph_schema_add(schema, options)` | `checkGraphSchemaAdd(schema, options?)` / `checkGraphSchemaAddAsync(...)` | `check_graph_schema_add(schema, **options)` / async | Validate adding selected schema targets without publishing. |
| Drop graph schema | `drop_graph_schema()` | `dropGraphSchema()` / `dropGraphSchemaAsync()` | `drop_graph_schema()` / async | Atomically remove all active node and edge schemas. |

Set/check option fields:

| Option | Rust | Node.js | Python | Default for `set` | Default for `check` | Description |
|--------|------|---------|--------|-------------------|---------------------|-------------|
| Max retained violations | `max_violations` | `maxViolations` | `max_violations` | `1` | `100` | Caps sampled violations returned in the report/error. |
| Chunk size | `chunk_size` | `chunkSize` | `chunk_size` | `4096` | `4096` | Bounded scan chunk size. |
| Scan limit | `scan_limit` | `scanLimit` | `scan_limit` | `None` / `null` | `None` / `null` | Optional maximum matching records to scan. |

`SchemaValidationReport` fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Checked records | `checked_records` | `checkedRecords` | `checked_records` | Matching live records scanned. |
| Violation count | `violation_count` | `violationCount` | `violation_count` | Total violations observed, including those omitted by truncation. |
| Violations | `violations` | `violations` | `violations` | Sampled violation targets, paths, and messages. |
| Truncated | `truncated` | `truncated` | `truncated` | More violations existed than were retained. |
| Scan limit hit | `scan_limit_hit` | `scanLimitHit` | `scan_limit_hit` | The optional scan limit stopped the scan. |

Graph-level schema APIs use the same `NodeSchema` and `EdgeSchema` DTOs inside a `GraphSchema`:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Node schemas | `node_schemas: Vec<NodeSchemaInfo>` | `nodeSchemas?: GraphSchemaNodeEntry[]` | `node_schemas?: list[GraphSchemaNodeEntry]` | Label/schema pairs to publish, check, or replace. |
| Edge schemas | `edge_schemas: Vec<EdgeSchemaInfo>` | `edgeSchemas?: GraphSchemaEdgeEntry[]` | `edge_schemas?: list[GraphSchemaEdgeEntry]` | Edge-label/schema pairs to publish, check, or replace. |

`alter_graph_schema` / `alterGraphSchema` / `alter_graph_schema` takes ordered operations:

| Operation | Node.js `kind` | Python `kind` | Rust variant |
|-----------|----------------|---------------|--------------|
| Set node schema | `setNode` | `set_node` | `GraphSchemaOperation::SetNode { label, schema }` |
| Set edge schema | `setEdge` | `set_edge` | `GraphSchemaOperation::SetEdge { label, schema }` |
| Drop node schema | `dropNode` | `drop_node` | `GraphSchemaOperation::DropNode { label }` |
| Drop edge schema | `dropEdge` | `drop_edge` | `GraphSchemaOperation::DropEdge { label }` |

Bulk check reports aggregate one entry per validated target:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Operation | `operation` | `operation` | `operation` | `Add`, `Set`, `Drop`, `DropAll`, `CheckAdd`, or `CheckSet`; connectors use `add` / `dropAll` / `drop_all` naming. |
| Entries | `entries` | `entries` | `entries` | Per-target `target_kind`, `label`, and nested `SchemaValidationReport`. |
| Checked records | `checked_records` | `checkedRecords` | `checked_records` | Sum across validated targets. |
| Violation count | `violation_count` | `violationCount` | `violation_count` | Sum across validated targets. |
| Truncated | `truncated` | `truncated` | `truncated` | True if any target report was truncated. |
| Scan limit hit | `scan_limit_hit` | `scanLimitHit` | `scan_limit_hit` | True if any target hit the optional scan limit. |

Bulk publish/drop results include the final catalog snapshot and exact drop accounting:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Operation | `operation` | `operation` | `operation` | Publish/drop operation kind. |
| Node schemas | `node_schemas` | `nodeSchemas` | `node_schemas` | Final published node-schema snapshot, sorted by label. |
| Edge schemas | `edge_schemas` | `edgeSchemas` | `edge_schemas` | Final published edge-schema snapshot, sorted by label. |
| Validation | `validation` | `validation` | `validation` | Aggregate check report from publish validation. |
| Targets published | `targets_published` | `targetsPublished` | `targets_published` | Number of schema targets published. |
| Targets dropped | `targets_dropped` | `targetsDropped` | `targets_dropped` | Number of schema targets removed. |
| Drop targets | `drop_targets` | `dropTargets` | `drop_targets` | Ordered selected-drop results with `target_kind`, `label`, and `dropped` / `notFound` / `not_found`. |
| Node schemas dropped | `node_schemas_dropped` | `nodeSchemasDropped` | `node_schemas_dropped` | Node-schema count removed by replacement or drop-all. |
| Edge schemas dropped | `edge_schemas_dropped` | `edgeSchemasDropped` | `edge_schemas_dropped` | Edge-schema count removed by replacement or drop-all. |

### Schema DTOs

Node schema fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Additional properties | `additional_properties` | `additionalProperties` | `additional_properties` | `"allow"` / `Allow` by default, or `"reject"` / `Reject` to reject undeclared props. |
| Properties | `properties` | `properties` | `properties` | Property-key map of `PropertySchema` rules. |
| Key | `key` | `key` | `key` | Optional string rules for the node key. |
| Label constraints | `label_constraints` | `labelConstraints` | `label_constraints` | Optional final-label `all_of`, `any_of`, and `none_of` rules. |
| Weight | `weight` | `weight` | `weight` | Optional finite numeric rules for node weight. |
| Dense vector | `dense_vector` | `denseVector` | `dense_vector` | Optional required/forbidden/dimension rule. |
| Sparse vector | `sparse_vector` | `sparseVector` | `sparse_vector` | Optional required/forbidden/count/dimension rule. |

Edge schema fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Additional properties | `additional_properties` | `additionalProperties` | `additional_properties` | `"allow"` / `Allow` by default, or `"reject"` / `Reject`. |
| Properties | `properties` | `properties` | `properties` | Property-key map of `PropertySchema` rules. |
| From endpoint | `from` | `from` | `from` | Optional source-node label rules. |
| To endpoint | `to` | `to` | `to` | Optional target-node label rules. |
| Self loops | `allow_self_loops` | `allowSelfLoops` | `allow_self_loops` | Defaults to true. |
| Weight | `weight` | `weight` | `weight` | Optional finite numeric rules for edge weight. |
| Validity | `validity` | `validity` | `validity` | Optional `valid_from` / `valid_to` rules. |

Property schemas support `required`, `nullable`, `types`, numeric min/max, string byte bounds,
bytes length bounds, array length bounds, map entry-count bounds, and exact enum values. Schemas
do not add recursive array item schemas or map key/value schemas. Value types are `bool`, `int`,
`uint`, `float`, `number`, `string`, `bytes`, `array`, and `map`; `number` accepts finite signed
integers, unsigned integers, and floats.

Node.js schema literals use camelCase and explicit tagged literals when the schema needs exact
unsigned or bytes values:

```javascript
{
  numericMin: { value: { type: 'uint', value: '0' } },
  enumValues: [{ type: 'bytes', value: Buffer.from([1, 2, 3]) }],
}
```

Returned Node.js schemas canonicalize UInt literals as `{ type: 'uint', value: string }` and bytes
literals as `{ type: 'bytes', value: Buffer }`. A schema map literal can be escaped as
`{ type: 'map', value: { ... } }` when a plain map would otherwise look like a tagged literal.

Python schema dicts use snake_case. Python accepts explicit UInt literals as
`{"type": "uint", "value": int | str}` and bytes literals as `bytes` or `bytearray`; returned
schemas canonicalize UInt as `{"type": "uint", "value": int}` and bytes as `bytes`.

### Schema examples

Rust:

```rust
use overgraph::*;
use std::collections::BTreeMap;

let user_schema = NodeSchema {
    additional_properties: SchemaAdditionalProperties::Reject,
    properties: BTreeMap::from([
        ("name".to_string(), PropertySchema {
            required: true,
            nullable: false,
            types: vec![SchemaValueType::String],
            ..Default::default()
        }),
    ]),
    ..Default::default()
};

let report = db.check_node_schema("User", user_schema.clone(), SchemaCheckOptions::default())?;
if report.violation_count == 0 {
    db.set_node_schema("User", user_schema)?;
}

let works_at_schema = EdgeSchema {
    properties: BTreeMap::from([(
        "since".to_string(),
        PropertySchema {
            required: true,
            nullable: false,
            types: vec![SchemaValueType::Int],
            ..Default::default()
        },
    )]),
    from: Some(EndpointLabelSchema {
        all_of: vec!["User".into()],
        ..Default::default()
    }),
    to: Some(EndpointLabelSchema {
        all_of: vec!["Company".into()],
        ..Default::default()
    }),
    allow_self_loops: false,
    ..Default::default()
};

db.set_edge_schema("WORKS_AT", works_at_schema)?;
```

Node.js:

```javascript
const userSchema = {
  additionalProperties: 'reject',
  properties: {
    name: { required: true, nullable: false, types: ['string'] },
    accountId: {
      required: true,
      nullable: false,
      types: ['uint'],
      numericMin: { value: { type: 'uint', value: '1' } },
    },
  },
};

const report = db.checkNodeSchema('User', userSchema);
if (report.violationCount === 0) db.setNodeSchema('User', userSchema);

db.setEdgeSchema('WORKS_AT', {
  properties: {
    since: { required: true, nullable: false, types: ['int'] },
  },
  from: { allOf: ['User'] },
  to: { allOf: ['Company'] },
  allowSelfLoops: false,
});
```

Python:

```python
user_schema = {
    "additional_properties": "reject",
    "properties": {
        "name": {"required": True, "nullable": False, "types": ["string"]},
        "account_id": {
            "required": True,
            "nullable": False,
            "types": ["uint"],
            "numeric_min": {"value": {"type": "uint", "value": 1}},
        },
    },
}

report = db.check_node_schema("User", user_schema)
if report.violation_count == 0:
    db.set_node_schema("User", user_schema)

db.set_edge_schema("WORKS_AT", {
    "properties": {
        "since": {"required": True, "nullable": False, "types": ["int"]},
    },
    "from": {"all_of": ["User"]},
    "to": {"all_of": ["Company"]},
    "allow_self_loops": False,
})
```

Bulk graph-schema APIs publish multiple targets atomically and use the same DTO fields.

Rust:

```rust
let graph = GraphSchema {
    node_schemas: vec![NodeSchemaInfo {
        label: "User".into(),
        schema: user_schema.clone(),
    }],
    edge_schemas: vec![EdgeSchemaInfo {
        label: "WORKS_AT".into(),
        schema: works_at_schema.clone(),
    }],
};

let dry_run = db.check_graph_schema_set(graph.clone(), GraphSchemaCheckOptions::default())?;
if dry_run.violation_count == 0 {
    let published = db.set_graph_schema(graph, GraphSchemaSetOptions::default())?;
    assert_eq!(published.targets_published, 2);
}
```

Node.js:

```javascript
const graph = {
  nodeSchemas: [{ label: 'User', schema: userSchema }],
  edgeSchemas: [{
    label: 'WORKS_AT',
    schema: {
      properties: { since: { required: true, nullable: false, types: ['int'] } },
      from: { allOf: ['User'] },
      to: { allOf: ['Company'] },
    },
  }],
};

const dryRun = db.checkGraphSchemaSet(graph);
if (dryRun.violationCount === 0) {
  const published = db.setGraphSchema(graph);
  console.log(published.targetsPublished);
}

const removed = db.alterGraphSchema([
  { kind: 'dropNode', label: 'ArchivedUser' },
  { kind: 'dropEdge', label: 'OLD_EDGE' },
]);
console.log(removed.dropTargets);
```

Python:

```python
graph = {
    "node_schemas": [{"label": "User", "schema": user_schema}],
    "edge_schemas": [{
        "label": "WORKS_AT",
        "schema": {
            "properties": {"since": {"required": True, "nullable": False, "types": ["int"]}},
            "from": {"all_of": ["User"]},
            "to": {"all_of": ["Company"]},
        },
    }],
}

dry_run = db.check_graph_schema_set(graph)
if dry_run.violation_count == 0:
    published = db.set_graph_schema(graph)
    print(published.targets_published)

removed = db.alter_graph_schema([
    {"kind": "drop_node", "label": "ArchivedUser"},
    {"kind": "drop_edge", "label": "OLD_EDGE"},
])
print([(target.label, target.action) for target in removed.drop_targets])
```

---

## Property & Time Queries

Equality and numeric range queries are index-transparent. Callers do not choose indexed versus fallback execution. If a matching optional declaration is `Ready`, OverGraph uses it. Otherwise it scans through the same public query method.

### find_nodes

Finds all nodes with a given label where a specific property matches a given value.

**Rust**
```rust
let ids = db.find_nodes(
    "User",
    "role",
    &PropValue::String("admin".into()),
)?;
```

**Node.js**
```javascript
const ids = db.findNodes('User', 'role', 'admin'); // Float64Array
```

**Python**
```python
ids = db.find_nodes("User", "role", "admin")  # IdArray
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Restrict search to this node label. |
| prop_key | `&str` | `string` | `str` | Yes | Property key to match on. |
| prop_value | `PropValue` | `any` | `Any` | Yes | Value to match. Finite numeric scalars use semantic equality across signed integers, unsigned integers, and finite floats. Strings and other non-numeric values remain exact, so string `"1"` does not match integer `1`. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

Matching node IDs. If a matching equality declaration is `Ready`, OverGraph uses the declaration-backed index path. Otherwise it scans nodes of the requested label.

---

### find_nodes_range

Finds all nodes with a given label where a numeric property falls within a range.

Results are ordered by `(property_value asc, node_id asc)`.

**Rust**
```rust
let ids = db.find_nodes_range(
    "User",
    "score",
    Some(&PropertyRangeBound::Included(PropValue::Int(10))),
    Some(&PropertyRangeBound::Excluded(PropValue::Float(20.0))),
)?;
```

**Node.js**
```javascript
const ids = db.findNodesRange(
  'User',
  'score',
  { value: 10, inclusive: true, domain: 'int' },
  { value: 20, inclusive: false, domain: 'float' },
);
```

**Python**
```python
ids = db.find_nodes_range(
    "User",
    "score",
    PropertyRangeBound(10, domain="int"),
    PropertyRangeBound(20.0, inclusive=False, domain="float"),
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Restrict search to this node label. |
| prop_key | `&str` | `string` | `str` | Yes | Numeric property key to query. |
| lower | `Option<&PropertyRangeBound>` | `PropertyRangeBound \| null \| undefined` | `PropertyRangeBound \| None` | No | Lower bound. Omit for an unbounded start. |
| upper | `Option<&PropertyRangeBound>` | `PropertyRangeBound \| null \| undefined` | `PropertyRangeBound \| None` | No | Upper bound. Omit for an unbounded end. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `IdArray` |

Matching node IDs in range order.

#### Behavior

- At least one finite numeric bound is required.
- Bounds may mix signed integer, unsigned integer, and finite float values.
- Empty finite numeric intervals return an empty result.
- Non-finite floats, non-numeric values, arrays, and maps are invalid bounds.
- If a matching range declaration is `Ready`, OverGraph uses the declaration-backed range path.
- If no matching `Ready` declaration exists, OverGraph falls back to a scan of nodes of the requested label.
- Index-backed results remain verified against the latest visible records.
- Invalid bound combinations return an error.

---

### find_nodes_by_time_range

Finds all nodes with a given label and `updated_at` within a time range.

```rust
let ids = db.find_nodes_by_time_range("User", start_ms, end_ms)?;
```

```javascript
const ids = db.findNodesByTimeRange('User', startMs, endMs);
```

```python
ids = db.find_nodes_by_time_range("User", start_ms, end_ms)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| label | `&str` | `string` | `str` | Yes | Node label. |
| from_ms | `i64` | `number` | `int` | Yes | Start of range (inclusive), ms since epoch. |
| to_ms | `i64` | `number` | `int` | Yes | End of range (inclusive), ms since epoch. |

#### Returns

Node IDs matching the time range. Uses the timestamp index.

---

## Queries

Native query APIs combine explicit IDs, label-scoped keys, label/edge-label constraints, property filters, timestamp filters, and
row-producing graph patterns through normal function-call and object APIs. GQL, documented below, adds a
query-string API for graph reads and mutations over the same native substrates.

Native query APIs return matching IDs, hydrated records, or graph-row results with explicit columns,
rows, optional path values, cursors, stats, and explain output.

Node queries use a recursive `filter` tree.

Top-level request fields such as node `label_filter`, edge `label`, `ids`, and `keys` are not part of the
filter tree. They are top-level constraints and are ANDed with the filter. Within `ids` and
`keys`, values are OR alternatives.

Use `filter` for all node and edge predicates.

Query APIs use the same published read snapshot and visibility rules as direct read APIs.
Internally, OverGraph may use explicit IDs, key lookup, the node-label index, ready property
equality/range indexes, the timestamp index, sorted intersection, sorted union, fallback scans, or
bounded adjacency expansion. Candidate indexes are verified after candidate planning; indexes are
never trusted as final truth.

OverGraph may also use private durable planner statistics when they are available. These stats can
improve cost estimates, adaptive caps, OR/IN costing, and graph-row fanout planning, but they do
not change request shapes or result semantics. Missing, corrupt, or stale stats only degrade
planning quality; every returned result is still verified against the visible record.

### What Query APIs Are

Node queries are the API-first query surface for combining top-level constraints with a recursive
node `filter` tree. They are useful when a request needs more than one constraint, when an index may
help but should remain optional, or when the same filter should be explained.

Direct edge queries and graph-row edge constraints use the canonical edge `filter` tree for
edge metadata and property predicates. Maintained edge-property indexes are used when available;
otherwise predicates are verified over the planned edge universe.

### Choosing the Right Query API

Use [`query_node_ids`](#query_node_ids) when you need matching IDs and want OverGraph to combine
top-level constraints with a node filter tree.

Use [`query_nodes`](#query_nodes) for the same query shape when you need hydrated node records. It
shares the same plan and verifier as `query_node_ids`; only the final payload differs.

Use [`explain_node_query`](#explain_node_query) to inspect the selected physical plan and warnings
without executing the page.

Use [`query_edge_ids`](#query_edge_ids) when you need matching edge IDs from explicit edge IDs, edge-label
constraints, endpoint constraints, or an explicit full-scan opt-in.

Use [`query_edges`](#query_edges) for the same edge query shape when you need hydrated edge records.
Metadata-only filters hydrate only the final page. Property filters hydrate bounded verifier
candidates.

Use [`explain_edge_query`](#explain_edge_query) to inspect direct edge query planning.

Use direct property and time queries such as [`find_nodes`](#find_nodes),
[`find_nodes_range`](#find_nodes_range), and
[`find_nodes_by_time_range`](#find_nodes_by_time_range) when you already know you need one direct
indexed lookup or range lookup. Those APIs keep their existing shapes.

Use [`query_graph_rows`](#query_graph_rows) when the result is a row set over node, edge, and path
bindings, or when you need optional matches, bounded variable-length paths, final-row cursors, or
compact/projection output without writing a GQL string.

### Node Queries

#### query_node_ids

Runs a node query and returns matching node IDs.

**Rust**
```rust
let page = db.query_node_ids(&NodeQuery {
    label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into()],
        mode: LabelMatchMode::All,
    }),
    filter: Some(NodeFilterExpr::And(vec![
        NodeFilterExpr::PropertyEquals {
            key: "status".into(),
            value: PropValue::String("active".into()),
        },
        NodeFilterExpr::PropertyRange {
            key: "score".into(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(50))),
            upper: None,
        },
    ])),
    page: PageRequest { limit: Some(100), after: None },
    ..Default::default()
})?;
```

**Node.js**
```javascript
const page = db.queryNodeIds({
  labelFilter: { labels: ['User'], mode: 'all' },
  filter: {
    and: [
      { property: 'status', eq: 'active' },
      { property: 'score', gte: 50 },
    ],
  },
  limit: 100,
});
```

**Python**
```python
page = db.query_node_ids({
    "label_filter": {"labels": ["User"], "mode": "all"},
    "filter": {
        "and": [
            {"property": "status", "eq": "active"},
            {"property": "score", "gte": 50},
        ],
    },
    "limit": 100,
})
```

##### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| request | `&NodeQuery` | `QueryNodeRequest` | `dict \| NodeQueryRequest` | Yes | Node query request. See [NodeQuery](#nodequery). |

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<QueryNodeIdsResult, EngineError>` | `IdPageResult` | `IdPageResult` |

Result fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `items: Vec<u64>` | `items: Float64Array` | `items: IdArray` | Matching node IDs in ascending node ID order. |
| cursor | `next_cursor: Option<u64>` | `nextCursor?: number` | `next_cursor: int \| None` | Cursor for the next page. Pass it as `after`. |

---

#### query_nodes

Runs the same node query as [`query_node_ids`](#query_node_ids), then hydrates the final page of
matching nodes.

**Rust**
```rust
let page = db.query_nodes(&NodeQuery {
    label_filter: Some(NodeLabelFilter {
        labels: vec!["Document".into(), "Published".into()],
        mode: LabelMatchMode::All,
    }),
    filter: Some(NodeFilterExpr::PropertyEquals {
        key: "status".into(),
        value: PropValue::String("active".into()),
    }),
    page: PageRequest { limit: Some(25), after: None },
    ..Default::default()
})?;
```

**Node.js**
```javascript
const page = db.queryNodes({
  labelFilter: { labels: ['Document', 'Published'], mode: 'all' },
  filter: {
    and: [
      { property: 'status', in: ['active', 'trial'] },
      { not: { property: 'archivedAt', exists: true } },
      {
        or: [
          { property: 'priority', gte: 8 },
          { property: 'source', eq: 'user' },
        ],
      },
    ],
  },
  limit: 25,
});
```

**Python**
```python
page = db.query_nodes({
    "label_filter": {"labels": ["Document", "Published"], "mode": "all"},
    "filter": {
        "and": [
            {"property": "status", "in": ["active", "trial"]},
            {"not": {"property": "archived_at", "exists": True}},
            {
                "or": [
                    {"property": "priority", "gte": 8},
                    {"property": "source", "eq": "user"},
                ],
            },
        ],
    },
    "limit": 25,
})
```

##### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| request | `&NodeQuery` | `QueryNodeRequest` | `dict \| NodeQueryRequest` | Yes | Node query request. See [NodeQuery](#nodequery). |

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<QueryNodesResult, EngineError>` | `NodePageResult` | `NodePageResult` |

Result fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `items: Vec<NodeView>` | `items: NodeView[]` | `items: list[NodeView]` | Hydrated final page of matching nodes. |
| cursor | `next_cursor: Option<u64>` | `nextCursor: number \| null` | `next_cursor: int \| None` | Cursor for the next page. Pass it as `after`. |

Connector node records expose top-level fields eagerly. Property maps are converted only when the
`.props` getter is accessed.

---

#### explain_node_query

Returns the deterministic planner tree, estimates, and warnings for a node query. It applies the
same validation rules as execution.

**Rust**
```rust
let plan = db.explain_node_query(&query)?;
```

**Node.js**
```javascript
const plan = db.explainNodeQuery({
  labelFilter: { labels: ['User'], mode: 'all' },
  filter: { property: 'status', eq: 'active' },
});
```

**Python**
```python
plan = db.explain_node_query({
    "label_filter": {"labels": ["User"], "mode": "all"},
    "filter": {"property": "status", "eq": "active"},
})
```

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<QueryPlan, EngineError>` | `object` | `dict` |

See [QueryPlan](#queryplan).

---

### Direct Edge Queries

#### query_edge_ids

Runs a direct edge query and returns matching edge IDs in ascending edge ID order.

**Rust**
```rust
let page = db.query_edge_ids(&EdgeQuery {
    label: Some("WORKS_AT".into()),
    from_ids: vec![person_id],
    filter: Some(EdgeFilterExpr::And(vec![
        EdgeFilterExpr::WeightRange { lower: Some(1.0), upper: None },
        EdgeFilterExpr::ValidAt { epoch_ms },
    ])),
    page: PageRequest { limit: Some(100), after: None },
    ..Default::default()
})?;
```

**Node.js**
```javascript
const page = db.queryEdgeIds({
  label: 'WORKS_AT',
  fromIds: [personId],
  filter: {
    and: [
      { weight: { gte: 1.0 } },
      { validAt: epochMs },
    ],
  },
  limit: 100,
});
```

**Python**
```python
page = db.query_edge_ids({
    "label": "WORKS_AT",
    "from_ids": [person_id],
    "filter": {
        "and": [
            {"weight": {"gte": 1.0}},
            {"valid_at": epoch_ms},
        ],
    },
    "limit": 100,
})
```

#### query_edges

Runs the same direct edge query as [`query_edge_ids`](#query_edge_ids), then hydrates the final page
of matching edge records.

**Node.js**
```javascript
const page = db.queryEdges({
  label: 'WORKS_AT',
  endpointIds: [personId],
  filter: { property: 'role', eq: 'lead' },
  limit: 25,
});
```

**Python**
```python
page = db.query_edges({
    "label": "WORKS_AT",
    "endpoint_ids": [person_id],
    "filter": {"property": "role", "eq": "lead"},
    "limit": 25,
})
```

#### explain_edge_query

Returns the deterministic planner tree, estimates, and warnings for a direct edge query.

**Rust**
```rust
let plan = db.explain_edge_query(&query)?;
```

**Node.js**
```javascript
const plan = db.explainEdgeQuery({ label: 'WORKS_AT', fromIds: [personId] });
```

**Python**
```python
plan = db.explain_edge_query({"label": "WORKS_AT", "from_ids": [person_id]})
```

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<QueryEdgeIdsResult, EngineError>` | `IdPageResult` | `IdPageResult` |
| `Result<QueryEdgesResult, EngineError>` | `EdgePageResult` | `EdgePageResult` |
| `Result<QueryPlan, EngineError>` | `object` | `dict` |

Rust `QueryEdgeIdsResult` contains `edge_ids: Vec<u64>` and `next_cursor: Option<u64>`. Rust `QueryEdgesResult` contains `edges: Vec<EdgeView>` and `next_cursor: Option<u64>`. Node.js and Python page result objects use `items` for the returned IDs or edges.

---

### Graph Row Queries

Graph-row queries are the structured public API for row-producing graph reads. They replace the
old graph-pattern API surface. Rust no longer exports `GraphPatternQuery`, `PatternOrder`,
`QueryMatch`, `query_pattern`, or `explain_pattern_query`. Node.js no longer exposes
`queryPattern` / `explainPatternQuery`. Python keeps legacy runtime methods only to return an
unsupported error that points callers to `query_graph_rows` / `explain_graph_rows`.

Use graph-row queries when you need row bindings across nodes and edges, optional groups,
bounded variable-length paths, path values, explicit return columns, final-row cursors, compact rows,
or the same substrate used by GQL.

#### query_graph_rows

Runs a graph-row request and returns explicit columns plus rows. Native graph-row execution uses the
same node/edge planners, indexes, adjacency readers, visibility rules, tombstone/shadow handling,
temporal edge checks, prune policy checks, and final verification as the direct query APIs. Row
assembly, optional semantics, path values, ordering, cursors, and projection are handled by the
graph-row executor.

**Rust**
```rust
let result = db.query_graph_rows(&GraphRowQuery {
    nodes: vec![
        GraphNodePattern {
            alias: "person".into(),
            label_filter: Some(NodeLabelFilter {
                labels: vec!["Person".into()],
                mode: LabelMatchMode::All,
            }),
            ids: vec![],
            keys: vec![],
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "status".into(),
                value: PropValue::String("active".into()),
            }),
        },
        GraphNodePattern {
            alias: "company".into(),
            label_filter: Some(NodeLabelFilter {
                labels: vec!["Company".into()],
                mode: LabelMatchMode::All,
            }),
            ids: vec![],
            keys: vec![],
            filter: None,
        },
    ],
    pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
        alias: Some("works".into()),
        from_alias: "person".into(),
        to_alias: "company".into(),
        direction: Direction::Outgoing,
        label_filter: vec!["WORKS_AT".into()],
        filter: None,
    })],
    where_: None,
    return_items: Some(vec![
        GraphReturnItem {
            expr: GraphExpr::Property { alias: "person".into(), key: "name".into() },
            alias: Some("person".into()),
            projection: GraphReturnProjection::Auto,
        },
        GraphReturnItem {
            expr: GraphExpr::Property { alias: "company".into(), key: "name".into() },
            alias: Some("company".into()),
            projection: GraphReturnProjection::Auto,
        },
    ]),
    order_by: vec![GraphOrderItem {
        expr: GraphExpr::Property { alias: "person".into(), key: "name".into() },
        direction: GraphOrderDirection::Asc,
    }],
    page: GraphPageRequest { skip: 0, limit: 100, cursor: None },
    at_epoch: None,
    params: BTreeMap::new(),
    output: GraphOutputOptions::default(),
    options: GraphQueryOptions::default(),
})?;
```

**Node.js**
```javascript
const result = db.queryGraphRows({
  nodes: [
    { alias: 'person', labelFilter: { labels: ['Person'], mode: 'all' } },
    { alias: 'company', labelFilter: { labels: ['Company'], mode: 'all' } },
  ],
  pieces: [
    { kind: 'edge', alias: 'works', fromAlias: 'person', toAlias: 'company', labelFilter: ['WORKS_AT'] },
  ],
  where: { op: '=', left: { property: { alias: 'person', key: 'status' } }, right: { param: 'status' } },
  return: [
    { expr: { property: { alias: 'person', key: 'name' } }, as: 'person' },
    { expr: { property: { alias: 'company', key: 'name' } }, as: 'company' },
  ],
  orderBy: [{ expr: { property: { alias: 'person', key: 'name' } }, direction: 'asc' }],
  params: { status: 'active' },
  limit: 100,
});
```

**Python**
```python
result = db.query_graph_rows({
    "nodes": [
        {"alias": "person", "label_filter": {"labels": ["Person"], "mode": "all"}},
        {"alias": "company", "label_filter": {"labels": ["Company"], "mode": "all"}},
    ],
    "pieces": [
        {"kind": "edge", "alias": "works", "from": "person", "to": "company", "labels": ["WORKS_AT"]},
    ],
    "where": {"op": "=", "left": {"property": {"alias": "person", "key": "status"}}, "right": {"param": "status"}},
    "return": [
        {"expr": {"property": {"alias": "person", "key": "name"}}, "as": "person"},
        {"expr": {"property": {"alias": "company", "key": "name"}}, "as": "company"},
    ],
    "order_by": [{"expr": {"property": {"alias": "person", "key": "name"}}, "direction": "asc"}],
    "params": {"status": "active"},
    "limit": 100,
})
```

##### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| request | `&GraphRowQuery` | `GraphRowRequest` | `dict \| GraphRowRequest` | Yes | Graph-row request. See [GraphRowQuery](#graphrowquery). |

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<GraphRowResult, EngineError>` | `GraphRowResult` | `GraphRowResult` |

Result fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| columns | `columns` | `columns` | `columns` | Output column names in row order. |
| rows | `rows: Vec<GraphRow>` | `rows: object[]` or `rows: any[][]` | `rows: list[dict]` or `list[list]` | Rust rows are positional `values`. Connectors return objects by default and arrays when compact rows are enabled. |
| next cursor | `next_cursor` | `nextCursor` | `next_cursor` | Final logical row cursor for the next page, or null/None on the last page. |
| stats | `stats` | `stats` | `stats` | Row counts, peak intermediate/frontier counts, path count, db-hit counter, effective epoch, elapsed time, and warnings. |
| plan | `plan` | `plan` | `plan` | `GraphRowExplain` when `options.include_plan` / `includePlan` / `include_plan` is true; otherwise null/None. |

Optional misses produce null values. In Rust that is `GraphValue::Null`; Node.js serializes it as
`null`; Python serializes it as `None`.

Path values contain identity arrays plus optional hydrated elements:

| Path field | Rust | Node.js | Python |
|------------|------|---------|--------|
| Node IDs | `node_ids` | `nodeIds` | `node_ids` |
| Edge IDs | `edge_ids` | `edgeIds` | `edge_ids` |
| Hydrated nodes | `nodes: Option<Vec<GraphNodeValue>>` | `nodes?: GraphNodeValue[]` | `nodes?: list[dict]` |
| Hydrated edges | `edges: Option<Vec<GraphEdgeValue>>` | `edges?: GraphEdgeValue[]` | `edges?: list[dict]` |

Default graph-row output mode is ID-oriented. Returning a path binding in default mode returns
`node_ids` / `edge_ids` only. `output.mode = elements` / `mode: 'elements'` / `"mode": "elements"`
hydrates returned node, edge, and path element values. Full node element hydration omits dense and
sparse vectors unless `include_vectors` / `includeVectors` is true. Selected projections can request
specific fields, including vector fields for selected node values.

#### explain_graph_rows

Returns graph-row validation and plan/explain output without returning rows.

**Rust**
```rust
let explain = db.explain_graph_rows(&query)?;
```

**Node.js**
```javascript
const explain = db.explainGraphRows(request);
const asyncExplain = await db.explainGraphRowsAsync(request);
```

**Python**
```python
explain = db.explain_graph_rows(request)
async_explain = await async_db.explain_graph_rows(request)
```

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<GraphRowExplain, EngineError>` | `GraphRowExplain` | `dict` |

`GraphRowExplain` fields are:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| columns | `columns` | `columns` | `columns` | Output columns after return expansion. |
| effective epoch | `effective_at_epoch` | `effectiveAtEpoch` | `effective_at_epoch` | Temporal epoch used by planning when known. |
| fingerprint | `fingerprint` | `fingerprint` | `fingerprint` | Normalized query fingerprint used by cursors. |
| plan | `plan` | `plan` | `plan` | Graph-row explain nodes, including optional/path/runtime details. |
| row ops | `row_ops` | `rowOps` | `row_ops` | Sort, skip, limit, projection, and related row-operation notes. |
| order | `order` | `order` | `order` | Whether explicit ordering is present and whether stable logical row keys are used. |
| cursor | `cursor` | `cursor` | `cursor` | Cursor supplied/codec status and message. |
| projection | `projection` | `projection` | `projection` | Columns, output mode, vector policy, and compact-row policy. |
| caps | `caps` | `caps` | `caps` | Effective graph-row safety caps. |
| summaries | `summaries` | `summaries` | `summaries` | Validation/runtime summary counters and warnings. |
| warnings / notes | `warnings`, `notes` | `warnings`, `notes` | `warnings`, `notes` | Planner warnings and explanatory notes. |

Node.js and Python async APIs expose graph-row query and explain methods through
`queryGraphRowsAsync`, `explainGraphRowsAsync`, and `AsyncOverGraph.query_graph_rows` /
`AsyncOverGraph.explain_graph_rows`.

---

### Graph Pipeline Queries

Graph pipeline queries are the structured public API for composable multi-stage graph reads. They
use the same native executor that GQL lowers into for `WITH`, `DISTINCT`, aggregation, `UNION`,
read-only `CALL`, and shortest-path stages. Use graph pipelines when you want the Phase 34 row
pipeline substrate without parsing a GQL string.

#### query_graph_pipeline

Runs a graph pipeline request and returns explicit columns plus rows. The final page is governed by
`limit` and `options.max_rows`; intermediate pipeline materialization is governed by
`options.max_pipeline_rows`, `options.max_groups`, `options.max_collect_items`,
`options.max_union_branches`, `options.max_subquery_invocations`, and
`options.max_shortest_path_pairs`.

**Rust**
```rust
let result = db.query_graph_pipeline(&GraphPipelineQuery {
    stages: vec![
        GraphPipelineStage::Match(GraphPipelineMatchStage {
            optional: false,
            nodes: vec![GraphNodePattern {
                alias: "n".into(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Person".into()],
                    mode: LabelMatchMode::All,
                }),
                ids: vec![],
                keys: vec![],
                filter: None,
            }],
            pieces: vec![],
            where_: None,
            optional_candidate_where: None,
        }),
        GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::With,
            items: GraphProjectionItems::Items(vec![
                GraphProjectItem {
                    expr: GraphExpr::Property { alias: "n".into(), key: "name".into() },
                    alias: Some("name".into()),
                    projection: GraphReturnProjection::Auto,
                },
                GraphProjectItem {
                    expr: GraphExpr::Property { alias: "n".into(), key: "rank".into() },
                    alias: Some("rank".into()),
                    projection: GraphReturnProjection::Auto,
                },
            ]),
            distinct: false,
            where_: None,
            order_by: vec![GraphOrderItem {
                expr: GraphExpr::Binding("rank".into()),
                direction: GraphOrderDirection::Desc,
            }],
            skip: None,
            limit: Some(GraphExpr::UInt(10)),
        }),
        GraphPipelineStage::Project(GraphProjectStage {
            kind: GraphProjectKind::Return,
            items: GraphProjectionItems::Items(vec![GraphProjectItem {
                expr: GraphExpr::Binding("name".into()),
                alias: Some("name".into()),
                projection: GraphReturnProjection::Auto,
            }]),
            distinct: false,
            where_: None,
            order_by: vec![],
            skip: None,
            limit: None,
        }),
    ],
    params: BTreeMap::new(),
    at_epoch: None,
    page: GraphPageRequest { skip: 0, limit: 100, cursor: None },
    output: GraphOutputOptions::default(),
    options: GraphPipelineOptions::default(),
})?;
```

**Node.js**
```javascript
const result = db.queryGraphPipeline({
  stages: [
    { kind: 'match', nodes: [{ alias: 'n', labelFilter: { labels: ['Person'], mode: 'all' } }] },
    {
      kind: 'project',
      projectKind: 'with',
      items: [
        { expr: { property: { alias: 'n', key: 'name' } }, as: 'name' },
        { expr: { property: { alias: 'n', key: 'rank' } }, as: 'rank' },
      ],
      orderBy: [{ expr: { binding: 'rank' }, direction: 'desc' }],
      limit: 10,
    },
    { kind: 'project', projectKind: 'return', items: [{ expr: { binding: 'name' }, as: 'name' }] },
  ],
  limit: 100,
});
```

**Python**
```python
result = db.query_graph_pipeline({
    "stages": [
        {"kind": "match", "nodes": [{"alias": "n", "label_filter": {"labels": ["Person"], "mode": "all"}}]},
        {
            "kind": "project",
            "project_kind": "with",
            "items": [
                {"expr": {"property": {"alias": "n", "key": "name"}}, "as": "name"},
                {"expr": {"property": {"alias": "n", "key": "rank"}}, "as": "rank"},
            ],
            "order_by": [{"expr": {"binding": "rank"}, "direction": "desc"}],
            "limit": 10,
        },
        {"kind": "project", "project_kind": "return", "items": [{"expr": {"binding": "name"}, "as": "name"}]},
    ],
    "limit": 100,
})
```

Aggregation uses `GraphExpr::AggregateCall` in Rust and the `aggregate` expression tag in Node.js
and Python:

```javascript
const counts = db.queryGraphPipeline({
  stages: [
    { kind: 'match', nodes: [{ alias: 'n', labelFilter: { labels: ['Person'], mode: 'all' } }] },
    { kind: 'return', items: [{ expr: { aggregate: { function: 'count' } }, as: 'people' }] },
  ],
  limit: 10,
});
```

```python
counts = db.query_graph_pipeline({
    "stages": [
        {"kind": "match", "nodes": [{"alias": "n", "label_filter": {"labels": ["Person"], "mode": "all"}}]},
        {"kind": "return", "items": [{"expr": {"aggregate": {"function": "count"}}, "as": "people"}]},
    ],
    "limit": 10,
})
```

##### Parameters

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| request | `&GraphPipelineQuery` | `GraphPipelineRequest` | `dict \| GraphPipelineRequest` | Yes | Ordered pipeline stages plus params, output, page, and safety options. |

Pipeline request fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| stages | `stages` | `stages` | `stages` | Ordered stage list. Must end in `Project(Return)`. |
| params | `params` | `params` | `params` | Structured parameter values referenced by expressions. |
| at epoch | `at_epoch` | `atEpoch` | `at_epoch` | Optional snapshot epoch for temporal reads. |
| page | `page` | `skip`, `limit`, `cursor` | `skip`, `limit`, `cursor` | Final logical row pagination. Pipeline cursors are separate from graph-row cursors. |
| output | `output` | `output` | `output` | Same output modes as graph-row queries. |
| options | `options` | `options` | `options` | Pipeline safety caps, `include_plan`, and `profile`. |

Supported stages:

| Stage | Rust | Node.js kind | Python kind | Purpose |
|-------|------|--------------|-------------|---------|
| Match | `GraphPipelineStage::Match` | `match` | `match` | Graph-row-backed match stage. |
| Project | `GraphPipelineStage::Project` | `project`, `with`, `return` | `project`, `with`, `return` | `WITH` or terminal `RETURN` projection, `DISTINCT`, row ops, and post-projection filter. |
| Union | `GraphPipelineStage::Union` | `union` | `union` | `UNION` / `UNION ALL` over read pipeline branches. |
| Call | `GraphPipelineStage::Call` | `call` | `call` | Read-only subquery stage with imported aliases. |
| Shortest path | `GraphPipelineStage::ShortestPath` | `shortestPath` | `shortest_path` | Bounded native shortest-path stage. |

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<GraphPipelineResult, EngineError>` | `GraphPipelineResult` | `GraphPipelineResult` |

`GraphPipelineResult` has `columns`, `rows`, `next_cursor` / `nextCursor`, `stats`, and optional
`plan`, matching graph-row result shape. Rust and Python use snake_case result/stat fields; Node.js
uses camelCase. Pipeline stats add `rows_entered_pipeline` / `rowsEnteredPipeline`,
`intermediate_rows` / `intermediateRows`, `pipeline_rows_materialized` /
`pipelineRowsMaterialized`, `groups`, `collect_items` / `collectItems`,
union/subquery/shortest-path counters, `db_hits` / `dbHits`, `elapsed_us` / `elapsedUs`,
`effective_at_epoch` / `effectiveAtEpoch`, and `warnings`.

#### explain_graph_pipeline

Returns pipeline validation, normalized stage details, caps, stats, and nested graph-row explains
without returning rows.

**Rust**
```rust
let explain = db.explain_graph_pipeline(&query)?;
```

**Node.js**
```javascript
const explain = db.explainGraphPipeline(request);
const asyncExplain = await db.explainGraphPipelineAsync(request);
```

**Python**
```python
explain = db.explain_graph_pipeline(request)
async_explain = await async_db.explain_graph_pipeline(request)
```

##### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<GraphPipelineExplain, EngineError>` | `GraphPipelineExplain` | `dict` |

`GraphPipelineExplain` includes `columns`, `effective_at_epoch` / `effectiveAtEpoch`,
`fingerprint`, `stages`, `row_ops` / `rowOps`, `order`, `cursor`, `projection`, `caps`,
`summaries`, `stats`, `warnings`, and `notes`.

Node.js and Python async APIs expose pipeline query and explain methods through
`queryGraphPipelineAsync`, `explainGraphPipelineAsync`, and
`AsyncOverGraph.query_graph_pipeline` / `AsyncOverGraph.explain_graph_pipeline`.

---

### GQL

#### Overview

**GQL** is OverGraph's GQL/Cypher-style query language for graph reads and writes, running in
OverGraph's embedded Rust engine.

Use GQL when a graph query or mutation is clearer as text than as request objects. It is not a
full ISO GQL or Cypher implementation; supported syntax is documented below, and unsupported
features are collected in [Current Limits](#current-limits).

Supported at a glance:

- `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` query strings
- `WITH`, `WITH *`, `WITH DISTINCT`, later `MATCH` stages, and terminal `RETURN DISTINCT`
- aggregation with `count`, `sum`, `avg`, `min`, `max`, and `collect`
- read-only `UNION`, `UNION ALL`, `EXISTS { ... }`, and `CALL { ... }`
- required patterns, optional patterns, bounded variable-length paths, and constrained shortest paths
- rich scalar expressions, arithmetic, string predicates, `CASE`, and scalar functions
- keyed mutations: `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE r`, and `DETACH DELETE n`
- keyed node `MERGE`, unique relationship `MERGE`, `ON CREATE SET`, and `ON MATCH SET`
- mutation `RETURN` for `CREATE`, `MERGE`, `SET`, and `REMOVE`, including `RETURN DISTINCT`
- graph-schema DDL: `ALTER CURRENT GRAPH TYPE`, `CHECK CURRENT GRAPH TYPE`, `DROP CURRENT GRAPH TYPE`, and schema `SHOW`
- property-index DDL: `CREATE PROPERTY INDEX`, `DROP PROPERTY INDEX`, and `SHOW PROPERTY INDEXES`
- mutation stats and unified query/mutation result shapes
- scalar values, node values, edge values, path values, lists, maps, bytes, and nulls
- params, read cursors, full-scan opt-in, caps, ReadOnly mode, explain/profile, warnings, and stats
- Rust, Node.js, Python, and async connector methods

#### Read Syntax

Read clause order:

```gql
MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
OPTIONAL MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
WITH [DISTINCT] <items> [ORDER BY ...] [SKIP ...] [LIMIT ...] [WHERE <predicate>]
CALL { <read clauses ending in RETURN> }
RETURN [DISTINCT] <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

`WHERE`, `ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` are optional. Each `MATCH` or `OPTIONAL MATCH`
can have its own `WHERE`. `OPTIONAL MATCH` follows an initial required `MATCH`. `SKIP` and `OFFSET`
are synonyms; using both in one query is rejected. `LIMIT 0` returns no result rows.

`WITH` projects the names available to later clauses. `WITH *` preserves visible aliases,
`WITH DISTINCT` deduplicates rows, `WITH ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` apply
before `WITH ... WHERE` and before the next clause.

Later `MATCH` and `OPTIONAL MATCH` stages can be seeded from aliases preserved by earlier stages:

```gql
MATCH (p:Person)
WITH p, lower(trim(p.email)) AS email
WHERE email ENDS WITH '@example.com'
MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name AS person, email, c.name AS company
ORDER BY person
LIMIT 20
```

Terminal `RETURN` supports `RETURN DISTINCT`, `RETURN DISTINCT *`, `ORDER BY`, `SKIP` / `OFFSET`,
and `LIMIT`.

Read queries can be combined with `UNION` and `UNION ALL`:

```gql
MATCH (p:Person) WHERE p.status = 'active'
RETURN p.name AS name
UNION
MATCH (p:Person) WHERE p.status = 'invited'
RETURN p.name AS name
```

Every union branch must be read-only, end in `RETURN`, and return the same output names. `UNION ALL`
keeps duplicates. `UNION` removes duplicate returned rows. Branch-local `ORDER BY`,
`SKIP` / `OFFSET`, and `LIMIT` apply before union results are combined. Union branch count is capped
by `max_union_branches`; `UNION` dedupe counts against `max_groups`.

Pattern shapes:

| Shape | Example |
|-------|---------|
| Node pattern | `MATCH (n:Person)` |
| Fixed edge pattern | `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)` |
| Multiple fixed hops | `MATCH (a)-[r:KNOWS]->(b)-[s:LIKES]->(c)` |
| Undirected fixed edge | `MATCH (a)-[r:KNOWS]-(b)` |
| Optional group | `MATCH (p:Person) OPTIONAL MATCH (p)-[r:REPORTS_TO]->(m:Person)` |
| Bounded variable-length path | `MATCH p = (a)-[:KNOWS*1..3]->(b)` |
| Zero-to-N bounded path | `MATCH p = (a)-[:KNOWS*0..2]->(b)` or `MATCH p = (a)-[:KNOWS*..2]->(b)` |
| Exact-length path | `MATCH p = (a)-[:KNOWS*2]->(b)` |
| One-hop path plus edge alias | `MATCH p = (a)-[r:KNOWS*1..1]->(b)` |
| Shortest path | `MATCH p = shortestPath((a)-[:KNOWS*1..5]->(b))` |
| All equal shortest paths | `MATCH p = allShortestPaths((a)-[:KNOWS*1..5]-(b))` |
| Map predicates | `MATCH (n:Person {name: $name})`, `MATCH (n:Person {elementKey: 'ada'})` |

Relationship quantifiers must have a finite upper bound no greater than `max_path_hops`.
Variable-length paths are relationship-simple: one path cannot reuse the same edge ID.

Shortest-path reads use `shortestPath` or `allShortestPaths` with a required path alias. Bind the
start and end node aliases first, then match a bounded relationship pattern such as `*1..4`.
`shortestPath` returns at most one path for each input row. `allShortestPaths` returns all equal
shortest paths up to the path caps. `OPTIONAL MATCH` binds the path alias to null when no path is
found. GQL shortest paths are unweighted; use the native `shortest_path` APIs for weighted paths.

Bind endpoints first:

```gql
MATCH (a:Person {elementKey: $from})
WITH a
MATCH (b:Person {elementKey: $to})
WITH a, b
MATCH p = shortestPath((a)-[:KNOWS*1..4]->(b))
RETURN p, nodeIds(p) AS node_ids, edgeIds(p) AS edge_ids, length(p) AS hops
```

Expressions:

| Expression | Example |
|------------|---------|
| Variables | `n`, `r`, `company` |
| Node metadata | `id(n)`, `labels(n)`, `elementKey(n)`, `weight(n)`, `createdAt(n)`, `updatedAt(n)` |
| Edge metadata | `id(r)`, `type(r)`, `weight(r)`, `createdAt(r)`, `updatedAt(r)`, `validFrom(r)`, `validTo(r)`, `id(startNode(r))`, `id(endNode(r))` |
| Path functions | `length(p)`, `nodeIds(p)`, `edgeIds(p)`, `nodes(p)`, `relationships(p)`, `startNode(p)`, `endNode(p)` |
| Property access | `n.name`, `n.rank`, `r.since`, `r.role` |
| Literals | `null`, booleans, integers, floats, strings, lists, maps |
| Params | `$name`, `$ids`, `$minSince`, `$payload` |
| Boolean predicates | `AND`, `OR`, `NOT` |
| Comparisons | `=`, `<>`, `<`, `<=`, `>`, `>=` |
| Null checks | `IS NULL`, `IS NOT NULL` |
| Membership | `IN` |
| Arithmetic | `n.rank + 1`, `n.score * 2`, `n.total / 4`, `-n.rank` |
| String predicates | `n.name STARTS WITH 'A'`, `n.email ENDS WITH '.org'`, `n.name CONTAINS 'da'` |
| Generic `CASE` | `CASE WHEN n.rank > 10 THEN 'high' ELSE 'low' END` |
| Simple `CASE` | `CASE n.status WHEN 'active' THEN 1 ELSE 0 END` |
| Return all bound aliases | `RETURN *` |

##### Metadata Functions

A function call reads engine metadata; a dot access reads a user property. Metadata functions work
in every expression context: `WHERE`, `RETURN`, `ORDER BY`, `WITH`, aggregates, and `CASE`.
Function names match case-insensitively; the canonical spelling is camelCase.

| Function | Valid argument | Reads | Writable via `SET` |
|----------|----------------|-------|--------------------|
| `id(n)`, `id(r)` | node or edge alias | element ID | read-only |
| `labels(n)` | node alias | label list | read-only (`SET n:Label` / `REMOVE n:Label`) |
| `type(r)` | edge alias | edge label | read-only |
| `elementKey(n)` | node alias | node key | read-only (set at creation via the `elementKey` map entry) |
| `weight(n)`, `weight(r)` | node or edge alias | weight | yes: `SET weight(n) = 2.5` |
| `createdAt(n)`, `createdAt(r)` | node or edge alias | created timestamp | read-only |
| `updatedAt(n)`, `updatedAt(r)` | node or edge alias | updated timestamp | read-only |
| `validFrom(r)` | edge alias | validity start | yes: `SET validFrom(r) = 10` |
| `validTo(r)` | edge alias | validity end | yes: `SET validTo(r) = 20` |
| `id(startNode(r))` | edge alias | source node ID | read-only |
| `id(endNode(r))` | edge alias | target node ID | read-only |

Using a read-only metadata function as a `SET` target is an error, and `REMOVE` never accepts
metadata functions. On an edge alias, `startNode(r)` / `endNode(r)` are valid only as the direct
argument of `id(...)`; use the bound pattern alias when the full endpoint node is needed.

No property name is reserved. `n.weight`, `n.key`, `n.updated_at`, and `r.valid_from` are ordinary
property lookups (null when the property is absent) and never read metadata; a user property named
`weight` coexists with the engine weight read by `weight(n)`.

##### Path Functions

| Function | Valid argument | Returns |
|----------|----------------|---------|
| `length(p)` | path alias | hop count |
| `startNode(p)` | path alias | first node |
| `endNode(p)` | path alias | last node |
| `nodes(p)` | path alias | node ID list |
| `relationships(p)` | path alias | edge ID list |
| `nodeIds(p)` | path alias | node ID list |
| `edgeIds(p)` | path alias | edge ID list |

Paths have no dot fields; `p.anything` is an error that suggests the path functions.

##### Scalar Functions

| Function | Valid argument |
|----------|----------------|
| `coalesce(value, ...)` | one or more scalar/list/map/null values |
| `toString(value)` | scalar numeric, boolean, string, or null |
| `toInteger(value)` | numeric, base-10 integer string, or null |
| `toFloat(value)` | numeric, finite-float string, or null |
| `abs(value)` | numeric or null |
| `floor(value)` | numeric or null |
| `ceil(value)` | numeric or null |
| `round(value)` | numeric or null |
| `lower(value)` | string or null |
| `upper(value)` | string or null |
| `trim(value)` | string or null |
| `substring(value, start[, length])` | string plus non-negative integer offsets |
| `size(value)` | string, list, map, or null |
| `head(list)` | list or null |
| `last(list)` | list or null |

`ORDER BY` can sort null, bool, finite numeric, string, bytes, node, edge, and path values. Nulls
sort last. Lists, maps, and non-finite floats are rejected.

Numeric expression behavior is checked. Integer arithmetic overflows are errors, division by zero is
an error, `/` returns a finite float, and non-finite float input or output is rejected.

Rich expression example:

```gql
MATCH (n:Person)
WITH n.name AS name,
     lower(trim(n.email)) AS email,
     n.rank + 2 AS boosted,
     CASE n.status WHEN 'active' THEN upper(n.status) ELSE 'OTHER' END AS bucket
WHERE email CONTAINS '@'
RETURN name, email, boosted, bucket
```

`DISTINCT` works in both `RETURN` and `WITH`, including `RETURN DISTINCT *` and `WITH DISTINCT *`.
Scalars compare by value, nodes by node ID, edges by edge ID, paths by ordered `node_ids` /
`edge_ids`, lists by element value, and maps by sorted string keys. Distinct keys count against
`max_groups`.

```gql
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH DISTINCT p
RETURN DISTINCT p.status AS status
ORDER BY status
```

Aggregation is available in `WITH` and terminal `RETURN` projections:

| Aggregate | Behavior |
|-----------|----------|
| `count(*)` | Counts every input row. |
| `count(expr)` | Counts non-null values. |
| `sum(expr)` | Sums numeric non-null values with checked numeric behavior. |
| `avg(expr)` | Returns a finite float average over numeric non-null values. |
| `min(expr)` / `max(expr)` | Accept numeric, string, and boolean comparable domains. |
| `collect(expr)` | Collects non-null values in input order. |

Aggregate `DISTINCT` is supported, for example `count(DISTINCT n.email)` and
`collect(DISTINCT n.status)`. Aggregate calls can appear inside projection expressions and
projection-local `ORDER BY`, such as `coalesce(avg(n.rank), 0.0)` or `ORDER BY count(*) DESC`.
Non-aggregate projected expressions become group keys. Empty `count` returns `0`, empty `collect`
returns `[]`, and empty `sum`, `avg`, `min`, and `max` return `null`. With no group keys, a zero-row
aggregate returns one row; with group keys, it returns zero rows. Aggregation uses `max_groups`;
`collect` also uses `max_collect_items`.

```gql
MATCH (n:Person)
WITH n.group AS group,
     count(*) AS total,
     avg(n.rank) AS avg_rank,
     collect(DISTINCT n.status) AS statuses
WHERE total > 1
RETURN group, total, coalesce(avg_rank, 0.0) AS avg_rank, statuses
ORDER BY total DESC
```

Element metadata always uses function syntax, such as `id(r)`, `type(r)`, and `updatedAt(n)`. Dot
access such as `r.id`, `r.label`, and `n.updated_at` reads ordinary user properties with those
names when present.

##### Read-Only Subqueries

`EXISTS { <read clauses ending in RETURN> }` is a predicate expression. It returns true when the
subquery emits at least one row and false otherwise. It can reference aliases from the outer query
and uses the same read snapshot. Subquery columns are not exposed.

```gql
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p)-[:WORKS_AT]->(c:Company)
  WHERE c.status = 'active'
  RETURN c
}
RETURN p.name AS name
```

`CALL { <read clauses ending in RETURN> }` is a read-only subquery stage. It can reference aliases
from the outer query. Returned subquery rows are joined back to each outer row; if a subquery returns
zero rows for an outer row, that outer row is dropped. Returned column names must not collide with
preserved outer names.

```gql
MATCH (p:Person)
CALL {
  MATCH (p)-[:WORKS_AT]->(c:Company)
  RETURN c.name AS company
}
RETURN p.name AS person, company
ORDER BY person
```

Nested read-only subqueries are allowed up to `max_subquery_depth`; total invocations are capped by
`max_subquery_invocations`. Mutating subqueries and procedure calls such as `CALL db.labels()` are
unsupported.

#### Mutation Syntax

Mutation clause order:

```gql
MATCH <pattern> [WHERE <predicate>]
OPTIONAL MATCH <pattern> [WHERE <predicate>]
WITH [DISTINCT] <items> [ORDER BY ...] [SKIP ...] [LIMIT ...] [WHERE <predicate>]
CALL { <read clauses ending in RETURN> }
CREATE <pattern> [, <pattern>...]
MERGE (n:Label {elementKey: expr}) [ON CREATE SET ...] [ON MATCH SET ...]
MERGE (a)-[r:TYPE]->(b) [ON CREATE SET ...] [ON MATCH SET ...]
SET <assignment>
REMOVE <target>
DELETE <edge-alias>
DETACH DELETE <node-alias>
RETURN [DISTINCT] <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

Read prefixes are optional for create-only statements. When a mutation does read first, put every
`MATCH`, `OPTIONAL MATCH`, `WITH`, `EXISTS {}`, or read-only `CALL {}` before the first write clause.
For mutation read prefixes, use repeated `MATCH` clauses instead of comma-separated pattern lists.

Mutation forms:

| Form | Example | Notes |
|------|---------|-------|
| Create node | `CREATE (n:Person {elementKey: 'ada', name: 'Ada'})` | A created node needs at least one label and a string `elementKey` map entry. |
| Create edge | `MATCH (a:Person {elementKey: 'a'}) MATCH (b:Person {elementKey: 'b'}) CREATE (a)-[r:KNOWS {since: 2026}]->(b)` | The edge needs exactly one relationship label. |
| Merge keyed node | `MERGE (n:Person {elementKey: $key}) ON CREATE SET n.created = true ON MATCH SET n.seen = true` | Exactly one static label and identity entry named `elementKey`. The key must evaluate to a non-null string. |
| Merge unique edge | `MATCH (a:Person {elementKey: $a}) MATCH (b:Person {elementKey: $b}) MERGE (a)-[r:KNOWS]->(b)` | Requires bound non-null endpoints and `edge_uniqueness = true`. Null endpoint rows are skipped. |
| Set property | `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n.status = 'active'` | `null` removes the property. |
| Set metadata | `MATCH (a)-[r:KNOWS]->(b) SET weight(r) = 0.5` | Function l-values: `weight(n)`, `weight(r)`, `validFrom(r)`, `validTo(r)`. All other metadata functions are read-only `SET` targets. |
| Merge property map | `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n += $props` | The right side must be a map of pure user properties; no map key is treated as metadata. Null map values remove properties. |
| Add node label | `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n:Engineer` | Label/key conflicts reject the whole statement. |
| Remove property | `MATCH (n:Person) WHERE elementKey(n) = 'ada' REMOVE n.status` | Missing properties are no-ops. `REMOVE` does not accept metadata functions. |
| Remove node label | `MATCH (n:Person) WHERE elementKey(n) = 'ada' REMOVE n:Engineer` | Removing the last live label is rejected. |
| Delete edge | `MATCH (a)-[r:KNOWS]->(b) DELETE r` | Node deletion requires `DETACH DELETE`. |
| Detach delete node | `MATCH (n:Person) WHERE elementKey(n) = 'ada' DETACH DELETE n` | Incident edges are deleted with the node. |

##### Element Maps

Element maps in `CREATE`, `MERGE`, and `MATCH` patterns describe the element itself, so they carry
metadata under the exact camelCase metadata function names:

- Node maps: `elementKey` (required in `CREATE` node maps; the `MERGE` node identity entry) and
  optional `weight`.
- Edge maps: optional `weight`, `validFrom`, and `validTo` (`validFrom` must be less than
  `validTo`).
- `MATCH` pattern maps filter on the same vocabulary: `MATCH (n:Person {elementKey: 'ada'})`
  matches the node key, and edge maps route `weight` / `validFrom` / `validTo` to the
  corresponding metadata filters.
- Every other map key is a user property, with no name restrictions.
- Metadata map keys that do not fit the target kind, such as `validFrom` on a node map, are errors.
- `SET x += {...}` maps are pure user properties and never route map keys to metadata.

Because element-map keys named after metadata always mean metadata, a user property literally named
`weight`, `elementKey`, `validFrom`, or `validTo` cannot be set through a `CREATE`/`MERGE` element
map. Set it with `SET` after creation:

```gql
CREATE (n:Part {elementKey: 'p1'})
SET n.weight = 'heavy'
```

`CREATE` is strict. It fails if a node `(label, key)` membership already exists or was already
created earlier in the same statement. When `edge_uniqueness = true`, edge `CREATE` also fails if
the same `(from, to, label)` triple already exists. With `edge_uniqueness = false`, parallel edge
creates are allowed.

`MERGE` supports two shapes:

- keyed node: `MERGE (n:Label {elementKey: expr})`
- unique relationship: `MERGE (a)-[r:TYPE]->(b)`

`ON CREATE SET` runs only when the entity is created. `ON MATCH SET` runs when the entity already
exists or was created by an earlier row in the same statement. If the same missing key or
relationship triple appears more than once in one statement, the first row creates it and later rows
match that same entity. Later property assignments win deterministically. The statement commits as
one transaction with no partial writes.

Optional-null mutation targets are no-ops. Duplicate updates to the same target are deterministic:
later mutation input rows win for updates, and duplicate deletes are idempotent.

Mutation statements commit zero or one transaction. If parsing, validation, caps, strict-create,
conflict checking, or commit fails, no partial writes are published.

##### Mutation RETURN

`CREATE`, `MERGE`, `SET`, and `REMOVE` may include `RETURN`, `RETURN DISTINCT`, `ORDER BY`,
`SKIP` / `OFFSET`, and `LIMIT`. `DELETE` and `DETACH DELETE` still reject `RETURN`.

Mutation row operations affect returned rows only. The mutation clauses still apply to every input
row produced by the read prefix. For example, `RETURN ... LIMIT 0` performs the mutation and returns
zero rows. Mutation statements never emit cursors.

Mutation `RETURN` supports:

- created and mutated aliases
- non-mutated read-prefix aliases
- path aliases captured by the read prefix
- `RETURN DISTINCT` over prevalidated return rows
- compact rows in connectors
- vector inclusion for returned node values when `include_vectors` / `includeVectors` is true
- `ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` over prevalidated return expressions

Mutation `RETURN` aggregation remains unsupported. Mutation `RETURN ORDER BY`, `RETURN DISTINCT`,
and `MERGE` actions cannot depend on commit-assigned metadata of created or merged aliases: `id(x)`,
`createdAt(x)`, `updatedAt(x)`, `id(startNode(r))`, and `id(endNode(r))` are rejected there.

#### Schema DDL Syntax

GQL can manage the current graph type, which is OverGraph's active label-scoped schema catalog.
Schema DDL is atomic at the graph-schema target level and uses the same Rust core schema-management
APIs as the programmatic Rust, Node.js, and Python methods.

Supported statements:

```gql
ALTER CURRENT GRAPH TYPE ADD { NODE <label> = <node-schema>, EDGE <label> = <edge-schema> } [OPTIONS <options>]
ALTER CURRENT GRAPH TYPE SET { NODE <label> = <node-schema>, EDGE <label> = <edge-schema> } [OPTIONS <options>]
ALTER CURRENT GRAPH TYPE DROP { NODE <label>, EDGE <label> }
DROP CURRENT GRAPH TYPE
CHECK CURRENT GRAPH TYPE ADD { NODE <label> = <node-schema>, EDGE <label> = <edge-schema> } [OPTIONS <options>]
CHECK CURRENT GRAPH TYPE SET { NODE <label> = <node-schema>, EDGE <label> = <edge-schema> } [OPTIONS <options>]
SHOW CURRENT GRAPH TYPE
SHOW NODE SCHEMAS
SHOW EDGE SCHEMAS
SHOW NODE SCHEMA <label>
SHOW EDGE SCHEMA <label>
```

`ADD` publishes listed schemas while preserving unlisted targets. `SET` replaces the entire schema
catalog; `SET {}` clears it. Selected `DROP` reports one row per requested target, including
`not_found` rows. `DROP CURRENT GRAPH TYPE` removes all node and edge schemas and returns one
summary row. `CHECK` validates the proposed `ADD` or `SET` against a published read snapshot and
does not publish labels or schema state. `OPTIONS` applies to `ADD`, `SET`, and `CHECK`; selected
and full-catalog `DROP` statements do not accept options.

Schema maps use the same fields as `NodeSchema` and `EdgeSchema`, but field names are snake_case in
GQL text. `OPTIONS` supports `max_violations`, `chunk_size`, and `scan_limit`:

```gql
ALTER CURRENT GRAPH TYPE ADD {
  NODE Person = {
    additional_properties: 'reject',
    properties: {
      name: { required: true, nullable: false, types: ['string'] },
      account_id: {
        required: true,
        nullable: false,
        types: ['uint'],
        numeric_min: { value: { type: 'uint', value: '1' } }
      },
      token: {
        enum_values: [{ type: 'bytes', value: [0, 1, 255] }]
      }
    }
  },
  EDGE WORKS_AT = {
    from: { all_of: ['Person'] },
    to: { all_of: ['Company'] },
    properties: {
      since: { required: true, nullable: false, types: ['int'] }
    },
    allow_self_loops: false
  }
} OPTIONS { max_violations: 1, chunk_size: 4096, scan_limit: null }
```

Dry-run and introspection examples:

```gql
CHECK CURRENT GRAPH TYPE ADD {
  NODE Person = { properties: { name: { required: true, nullable: false, types: ['string'] } } }
} OPTIONS { max_violations: 10 }

SHOW CURRENT GRAPH TYPE
SHOW NODE SCHEMA Person
ALTER CURRENT GRAPH TYPE DROP { NODE ArchivedPerson, EDGE OLD_EDGE }
```

Result row columns are stable:

| Statement | Columns |
|-----------|---------|
| `ALTER ... ADD` / `ALTER ... SET` | `operation`, `target_kind`, `label`, `action`, `checked_records`, `violation_count`, `truncated`, `scan_limit_hit` |
| `ALTER ... DROP` | `operation`, `target_kind`, `label`, `action` |
| `DROP CURRENT GRAPH TYPE` | `operation`, `target_kind`, `label`, `action`, `node_schemas_dropped`, `edge_schemas_dropped` |
| `CHECK ... ADD` / `CHECK ... SET` | `operation`, `target_kind`, `label`, `checked_records`, `violation_count`, `truncated`, `scan_limit_hit`, `violations` |
| `SHOW ...` | `target_kind`, `label`, `schema` |

Operation values are snake_case strings such as `alter_graph_type_add`,
`drop_current_graph_type`, `check_graph_type_set`, and `show_current_graph_type`.
`target_kind` is `node`, `edge`, or `graph`. `SHOW` returns canonical schema maps; unsigned integer
schema literals are returned as `{ type: 'uint', value: '<decimal>' }` in Node.js and as
`{"type": "uint", "value": "<decimal>"}` through GQL rows, while bytes literals use
`{ type: 'bytes', value: [0, 1] }`.

Schema statements return `kind = schema`, `mutation_stats = null`, `next_cursor = null`, and
`schema_stats` / `schemaStats` populated. `CHECK` and `SHOW` are allowed in `ReadOnly` mode.
`ALTER` and `DROP` are rejected in `ReadOnly`. All schema statements reject `cursor`; schema
introspection does not page. For `SHOW`, `max_rows` is a hard cap: if the schema catalog would
return more rows than `max_rows`, OverGraph returns an error instead of silently truncating.

#### Property-Index DDL Syntax

GQL can manage the same optional equality and range property-index declarations exposed by the
native Rust, Node.js, and Python property-index APIs. Declarations use one to eight ordered fields.
Fields can be properties or supported metadata functions. These declarations are performance
accelerators for eligible node and edge predicates; they do not enforce uniqueness, required
properties, value constraints, or query correctness. Public query APIs remain correct without them,
while they are building, or after they are dropped.

The native APIs use the same field-list declarations described above. Use GQL property-index DDL
when target-based text is clearer than calling the Rust/Python snake_case or Node.js camelCase
ensure, list, and drop property-index methods directly.

Supported statements:

```gql
CREATE PROPERTY INDEX FOR (n:<node-label>) ON (n.<property-key>) KIND EQUALITY
CREATE PROPERTY INDEX FOR (n:<node-label>) ON (n.<property-key>) KIND RANGE
CREATE PROPERTY INDEX FOR (n:<node-label>) ON (<field>, <field>, ...) KIND EQUALITY
CREATE PROPERTY INDEX FOR (n:<node-label>) ON (<field>, <field>, ...) KIND RANGE
CREATE PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (r.<property-key>) KIND EQUALITY
CREATE PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (r.<property-key>) KIND RANGE
CREATE PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (<field>, <field>, ...) KIND EQUALITY
CREATE PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (<field>, <field>, ...) KIND RANGE
DROP PROPERTY INDEX FOR (n:<node-label>) ON (n.<property-key>) KIND EQUALITY
DROP PROPERTY INDEX FOR (n:<node-label>) ON (n.<property-key>) KIND RANGE
DROP PROPERTY INDEX FOR (n:<node-label>) ON (<field>, <field>, ...) KIND EQUALITY
DROP PROPERTY INDEX FOR (n:<node-label>) ON (<field>, <field>, ...) KIND RANGE
DROP PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (r.<property-key>) KIND EQUALITY
DROP PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (r.<property-key>) KIND RANGE
DROP PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (<field>, <field>, ...) KIND EQUALITY
DROP PROPERTY INDEX FOR ()-[r:<edge-label>]-() ON (<field>, <field>, ...) KIND RANGE
SHOW PROPERTY INDEXES
SHOW NODE PROPERTY INDEXES
SHOW EDGE PROPERTY INDEXES
```

For `SHOW`, singular `INDEX` and plural `INDEXES` are both accepted.

Examples:

```gql
CREATE PROPERTY INDEX FOR (n:Person) ON (n.status) KIND EQUALITY
CREATE PROPERTY INDEX FOR (n:Person) ON (n.tenant_id, updatedAt(n)) KIND RANGE
CREATE PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.since) KIND RANGE
CREATE PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (id(startNode(r)), r.status, validTo(r)) KIND RANGE
SHOW PROPERTY INDEXES
DROP PROPERTY INDEX FOR (n:Person) ON (n.status) KIND EQUALITY
DROP PROPERTY INDEX FOR ()-[r:WORKS_AT]-() ON (r.since) KIND RANGE
```

Labels and property keys can be bare identifiers or quoted strings. Dot fields in index DDL are
property fields, even when the property name looks like metadata. Metadata fields use function
syntax: node `id(n)`, `elementKey(n)`, `weight(n)`, `createdAt(n)`, `updatedAt(n)`; edge `id(r)`,
`weight(r)`, `createdAt(r)`, `updatedAt(r)`, `validFrom(r)`, `validTo(r)`, `id(startNode(r))`,
and `id(endNode(r))`. Index identity is target-based: target kind, label, ordered field list, and
kind. There are no user-assigned index names in this GQL surface.

Result row columns are stable:

| Statement | Columns |
|-----------|---------|
| `CREATE PROPERTY INDEX ...` | `operation`, `target_kind`, `label`, `fields`, `kind`, `action`, `state`, `index_id`, `last_error`, `compound`, `field_count` |
| `DROP PROPERTY INDEX ...` | `operation`, `target_kind`, `label`, `fields`, `kind`, `action`, `compound`, `field_count` |
| `SHOW PROPERTY INDEXES` | `target_kind`, `label`, `fields`, `kind`, `state`, `index_id`, `last_error`, `compound`, `field_count` |

Operation values are `create_property_index`, `drop_property_index`, `show_property_indexes`,
`show_node_property_indexes`, or `show_edge_property_indexes`. `target_kind` is `node` or `edge`
in action and catalog rows. `kind` is `equality` or `range`. Row `action` is `ensured`, `dropped`,
or `not_found`. Explain target `action` is `ensure`, `drop`, or `show`. `state` is the current
declaration lifecycle state, such as `building`, `ready`, or `failed`; `last_error` is null unless
the declaration reports a retained error. `fields` is an ordered list of objects with `source`
(`property` or `metadata`), `key` for property fields, and `field` for metadata fields. Metadata
`field` values use the camelCase function spellings: `id`, `elementKey`, `weight`, `createdAt`,
`updatedAt`, `validFrom`, `validTo`, and `startNode` / `endNode` for edge endpoint IDs. `compound`
is true when the list has two or more fields, and `field_count` is the list length.

Index statements return `kind = index`, `mutation_stats` / `mutationStats = null`,
`schema_stats` / `schemaStats = null`, `next_cursor` / `nextCursor = null`, and
`index_stats` / `indexStats` populated. Non-index statements include the same index payload field as
null.

Index stats fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Operation | `operation` | `operation` | `operation` | Index operation string. |
| Indexes ensured | `indexes_ensured` | `indexesEnsured` | `indexes_ensured` | Number of declarations ensured by this statement. |
| Indexes dropped | `indexes_dropped` | `indexesDropped` | `indexes_dropped` | Number of declarations removed by this statement. |
| Indexes returned | `indexes_returned` | `indexesReturned` | `indexes_returned` | Number of catalog rows returned by `SHOW`. |
| Elapsed time | `elapsed_us` | `elapsedUs` | `elapsed_us` | Populated only when profile is true. |
| Warnings | `warnings` | `warnings` | `warnings` | Index statement warnings. |

`explain_gql` / `explainGql` and `include_plan` / `includePlan` return the same `index` explain
payload for index statements, with `read`, `mutation`, and `schema` set to null.

Nested index explain fields:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| `operation` | `operation` | `operation` | Index operation string. |
| `targets` | `targets` | `targets` | Planned targets with target kind, label, fields, kind, action, and compound flag. |
| `usesCoreWriteQueue` / `uses_core_write_queue` | `usesCoreWriteQueue` | `uses_core_write_queue` | True for mutating CREATE and DROP statements. |
| `publishesManifest` / `publishes_manifest` | `publishesManifest` | `publishes_manifest` | True for statements that can publish index declaration state when executed. |
| `createsLabels` / `creates_labels` | `createsLabels` | `creates_labels` | True for CREATE statements that may create missing label tokens. |
| `schedulesBackgroundBuild` / `schedules_background_build` | `schedulesBackgroundBuild` | `schedules_background_build` | True for CREATE statements that may start asynchronous index build work. |
| `dropsIndexDataAsync` / `drops_index_data_async` | `dropsIndexDataAsync` | `drops_index_data_async` | True for DROP statements that may remove declaration-backed index data asynchronously. |
| `sideEffectFree` / `side_effect_free` | `sideEffectFree` | `side_effect_free` | True for SHOW; false for CREATE and DROP. |

Index explain target fields:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| `targetKind` / `target_kind` | `targetKind` | `target_kind` | `node`, `edge`, or `property_index_catalog` for all-index SHOW. |
| `label` | `label` | `label` | Node or edge label for CREATE/DROP; null for SHOW. |
| `fields` | `fields` | `fields` | Ordered field-list objects; empty for SHOW explain targets. |
| `kind` | `kind` | `kind` | `equality` or `range` for CREATE/DROP; null for SHOW. |
| `action` | `action` | `action` | `ensure`, `drop`, or `show`. |
| `compound` | `compound` | `compound` | True when the target field list has two or more fields. |

`CREATE` and `DROP` are rejected in `ReadOnly` mode. `SHOW PROPERTY INDEXES`, `SHOW NODE PROPERTY
INDEXES`, and `SHOW EDGE PROPERTY INDEXES` are allowed in `ReadOnly` mode. All index statements
reject `cursor`; index catalog introspection does not page. For `SHOW`, `max_rows` is a hard cap:
if the index catalog would return more rows than `max_rows`, OverGraph returns an error instead of
silently truncating.

#### Method Reference

| Language | Execute | Explain |
|----------|---------|---------|
| Rust | `DatabaseEngine::execute_gql(statement, params, options)` | `DatabaseEngine::explain_gql(statement, params, options)` |
| Node.js | `db.executeGql(statement, params?, options?)` | `db.explainGql(statement, params?, options?)` |
| Node.js async | `db.executeGqlAsync(statement, params?, options?)` | `db.explainGqlAsync(statement, params?, options?)` |
| Python | `db.execute_gql(statement, params=None, **options)` | `db.explain_gql(statement, params=None, **options)` |
| Python async | `await db.execute_gql(statement, params=None, **options)` | `await db.explain_gql(statement, params=None, **options)` |

**Rust**
```rust
let result = db.execute_gql(
    "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;

let grouped = db.execute_gql(
    "MATCH (n:Person) \
     WITH n.group AS group, count(*) AS total, collect(DISTINCT n.status) AS statuses \
     RETURN group, total, statuses ORDER BY total DESC",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;

let created = db.execute_gql(
    "CREATE (n:Person {elementKey: 'ada', name: 'Ada'}) RETURN n.name AS name",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;

let explain = db.explain_gql(
    "MATCH (n:Person) RETURN n.name AS name",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;
```

**Node.js**
```javascript
const result = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);

const grouped = db.executeGql(
  `MATCH (n:Person)
   WITH n.group AS group, count(*) AS total
   WHERE total > 1
   RETURN group, total
   ORDER BY total DESC`
);

const asyncResult = await db.executeGqlAsync(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);

const created = db.executeGql(
  "CREATE (n:Person {elementKey: 'ada', name: 'Ada'}) RETURN n.name AS name"
);

const merged = db.executeGql(
  `MERGE (n:Person {elementKey: 'ada'})
   ON CREATE SET n.status = 'created'
   ON MATCH SET n.status = 'matched'
   RETURN elementKey(n) AS key, n.status AS status`
);

const explain = db.explainGql(
  'MATCH (n:Person) RETURN n.name AS name'
);
```

**Python**
```python
result = db.execute_gql(
    "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10"
)

path_result = db.execute_gql(
    """
    MATCH (a:Person {elementKey: $from_key})
    WITH a
    MATCH (b:Person {elementKey: $to_key})
    WITH a, b
    MATCH p = shortestPath((a)-[:KNOWS*1..4]->(b))
    RETURN nodeIds(p) AS node_ids, edgeIds(p) AS edge_ids
    """,
    {"from_key": "ada", "to_key": "ben"},
)

async_result = await async_db.execute_gql(
    "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10"
)

created = db.execute_gql(
    "CREATE (n:Person {elementKey: 'ada', name: 'Ada'}) RETURN n.name AS name"
)

explain = db.explain_gql(
    "MATCH (n:Person) RETURN n.name AS name"
)
```

#### Parameters and Options

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| statement | `&str` | `string` | `str` | Yes | - | One GQL read, mutation, schema, or property-index statement. |
| params | `&GqlParams` | `GqlParams \| null` | `GqlParams \| None` | No | empty | Named params referenced as `$name` in the statement. |
| options | `&GqlExecutionOptions` | `GqlExecutionOptions \| null` | keyword options | No | default options | Execution mode, caps, explain/profile, row-format, cursor, and vector options. |

Option fields:

| Option | Rust | Node.js | Python | Default | Description |
|--------|------|---------|--------|---------|-------------|
| Mode | `mode` | `mode` | `mode` | `Auto` / `"auto"` | `"auto"` permits reads and mutations. `"readOnly"` / `"read_only"` rejects mutation statements before write staging. |
| Full-scan opt-in | `allow_full_scan` | `allowFullScan` | `allow_full_scan` | `false` | Allows legal broad node/edge scans when no bounded anchor exists. |
| Result row cap | `max_rows` | `maxRows` | `max_rows` | `10000` | Maximum returned rows after row operations. Mutations do not page with cursors, so mutation `RETURN` must fit this cap. Schema and index `SHOW` treat this as a hard cap and error if the catalog would exceed it. |
| Cursor | `cursor` | `cursor` | `cursor` | `None` / `null` | Read continuation token from `next_cursor` / `nextCursor`. Mutation, schema, and index statements reject cursors. |
| Cursor byte cap | `max_cursor_bytes` | `maxCursorBytes` | `max_cursor_bytes` | `16384` | Maximum accepted or emitted read cursor token size. |
| Mutation row cap | `max_mutation_rows` | `maxMutationRows` | `max_mutation_rows` | `10000` | Maximum input rows a mutation may write from. |
| Mutation op cap | `max_mutation_ops` | `maxMutationOps` | `max_mutation_ops` | `50000` | Maximum staged logical mutation operations before commit, including cascaded deletes. |
| Pipeline row cap | `max_pipeline_rows` | `maxPipelineRows` | `max_pipeline_rows` | `65536` | Maximum intermediate rows retained by multi-stage GQL reads. |
| Group/dedupe cap | `max_groups` | `maxGroups` | `max_groups` | `65536` | Maximum aggregate groups or canonical dedupe keys for `DISTINCT` and `UNION`. |
| Collect item cap | `max_collect_items` | `maxCollectItems` | `max_collect_items` | `65536` | Maximum collected values retained by aggregate collection stages. |
| Union branch cap | `max_union_branches` | `maxUnionBranches` | `max_union_branches` | `16` | Maximum read branches allowed in one `UNION` / `UNION ALL` statement. |
| Subquery invocation cap | `max_subquery_invocations` | `maxSubqueryInvocations` | `max_subquery_invocations` | `4096` | Maximum subquery invocations for supported subquery execution. |
| Subquery depth cap | `max_subquery_depth` | `maxSubqueryDepth` | `max_subquery_depth` | `2` | Maximum nested subquery depth. |
| Shortest-path pair cap | `max_shortest_path_pairs` | `maxShortestPathPairs` | `max_shortest_path_pairs` | `4096` | Maximum source/target pairs for supported shortest-path planning. |
| Query byte cap | `max_query_bytes` | `maxQueryBytes` | `max_query_bytes` | `1048576` | Maximum GQL source text bytes accepted by the parser. |
| Param byte cap | `max_param_bytes` | `maxParamBytes` | `max_param_bytes` | `1048576` | Maximum referenced param string/bytes/map-key bytes, both per value/key and total across referenced params. |
| AST/param depth cap | `max_ast_depth` | `maxAstDepth` | `max_ast_depth` | `256` | Maximum parser AST depth and referenced runtime list/map nesting depth. |
| Literal/param item cap | `max_literal_items` | `maxLiteralItems` | `max_literal_items` | `10000` | Maximum list/map literal items, per referenced list/map container, and total referenced list/map items. |
| Intermediate cap | `max_intermediate_bindings` | `maxIntermediateBindings` | `max_intermediate_bindings` | `65536` | Maximum intermediate row bindings held while executing reads or mutation read prefixes. |
| Frontier cap | `max_frontier` | `maxFrontier` | `max_frontier` | `65536` | Maximum relationship-expansion frontier size. |
| Path hop cap | `max_path_hops` | `maxPathHops` | `max_path_hops` | `16` | Maximum finite upper bound for variable-length paths. |
| Paths per start cap | `max_paths_per_start` | `maxPathsPerStart` | `max_paths_per_start` | `4096` | Maximum variable-length paths retained per start row. |
| Order materialization cap | `max_order_materialization` | `maxOrderMaterialization` | `max_order_materialization` | `65536` | Maximum rows/materialized order keys for ordered reads and mutation returns. |
| Skip cap | `max_skip` | `maxSkip` | `max_skip` | `100000` | Maximum allowed `SKIP` / `OFFSET` value. |
| Include plan | `include_plan` | `includePlan` | `include_plan` | `false` | Attaches the same unified explain payload to `GqlExecutionResult.plan`. |
| Profile | `profile` | `profile` | `profile` | `false` | Adds elapsed time and best-effort work counters to stats. |
| Compact rows | `compact_rows` | `compactRows` | `compact_rows` | `false` | Rust rows are already positional. In connectors, returns row arrays instead of row objects. Does not change execution. |
| Include vectors | `include_vectors` | `includeVectors` | `include_vectors` | `false` | Includes dense/sparse vectors when returning node element values. |

`compactRows` / `compact_rows` changes only connector row serialization. It does not change selected
fields, vector policy, ordering, stats, caps, warnings, or explain output.

#### Results and Row Formats

Rust returns positional rows:

```rust
GqlExecutionResult {
    kind: GqlStatementKind::Query, // or Mutation, Schema, or Index
    columns: Vec<String>,
    rows: Vec<GqlRow>,          // each GqlRow has values: Vec<GqlValue>
    next_cursor: Option<String>,
    stats: GqlExecutionStats,
    mutation_stats: Option<GqlMutationStats>,
    schema_stats: Option<GqlSchemaStats>,
    index_stats: Option<GqlIndexStats>,
    plan: Option<GqlExecutionExplain>,
}
```

Node.js and Python return object rows by default:

```javascript
{
  kind: 'query',
  columns: ['name', 'rank'],
  rows: [
    { name: 'Ada', rank: 2 },
    { name: 'Ben', rank: 4 },
  ],
  nextCursor: null,
  stats: {
    rowsReturned: 2,
    rowsMatched: 2,
    rowsAfterFilter: 2,
    intermediateBindings: 2,
    dbHits: 2,
    elapsedUs: null,
    warnings: [],
  },
  mutationStats: null,
  schemaStats: null,
  indexStats: null,
  plan: null,
}
```

With compact rows enabled, connectors keep the same `columns` array and return positional row arrays:

```javascript
const result = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name, n.rank AS rank ORDER BY n.rank',
  null,
  { compactRows: true }
);

// result.columns: ['name', 'rank']
// result.rows: [['Ben', 1], ['Ada', 2]]
```

Duplicate output names are allowed. Object-row connectors use the duplicate column name as the object
key, so prefer aliases that are unique when using object rows. Use compact rows when duplicate column
names must be preserved positionally. Row-operation references to return aliases must be
unambiguous: if multiple `RETURN` items use the same alias, clauses such as `ORDER BY x` or
`LIMIT x` reject `x` instead of choosing one of the duplicate columns.

When a result has another page, it includes `next_cursor` / `nextCursor`. Pass that value as the
next call's `cursor` option with the same logical query and params. GQL cursors continue final
logical result rows; they are not pinned storage snapshots across pages. Mutation, schema, and
index statements reject `cursor` and always return `next_cursor` / `nextCursor` as null.

Mutation results use the same row shapes and include `mutation_stats` / `mutationStats`:

```javascript
const result = db.executeGql(
  "MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n.status = 'active' RETURN elementKey(n) AS key, n.status AS status"
);

console.log(result.kind);                    // 'mutation'
console.log(result.nextCursor);              // null
console.log(result.mutationStats.nodesUpdated);
console.log(result.rows[0].status);
```

Schema results use the same row format, set `kind` to `schema`, leave `mutation_stats` /
`mutationStats` null, and include `schema_stats` / `schemaStats`:

```javascript
const result = db.executeGql(
  `CHECK CURRENT GRAPH TYPE ADD {
     NODE Person = { properties: { name: { required: true, nullable: false, types: ['string'] } } }
   }`
);

console.log(result.kind);                         // 'schema'
console.log(result.schemaStats.operation);        // 'check_graph_type_add'
console.log(result.rows[0].violation_count);
```

#### Nodes, Edges, Values, and Vectors

GQL values can be:

- `null`
- booleans
- signed integers, unsigned integers, and finite floats
- strings
- bytes
- lists
- maps with string keys
- node values
- edge values
- path values

Node.js bytes are returned as `Buffer`. Python bytes are returned as `bytes`. Rust uses
`GqlValue::Bytes(Vec<u8>)`.

`collect` returns a list whose items follow normal GQL expression value rules. It can collect nested
lists, maps, and path values; when a node or edge alias is collected as an expression, the collected
value is its ID. Return node, edge, or path aliases directly when the result should contain hydrated
graph element values. Path helper functions return scalar/list values: `length(p)` returns hop
count, `nodeIds(p)` / `edgeIds(p)` return ID lists, and `nodes(p)` / `relationships(p)` return ID
lists for helper expressions while returning a path alias as a value hydrates path `nodes` / `edges`.

Node values expose only requested fields:

| Field | Rust | Node.js | Python |
|-------|------|---------|--------|
| ID | `id` | `id` | `id` |
| Labels | `labels` | `labels` | `labels` |
| Key | `key` | `key` | `key` |
| Properties | `props` | `props` | `props` |
| Weight | `weight` | `weight` | `weight` |
| Created timestamp | `created_at` | `createdAt` | `created_at` |
| Updated timestamp | `updated_at` | `updatedAt` | `updated_at` |
| Dense vector | `dense_vector` | `denseVector` | `dense_vector` |
| Sparse vector | `sparse_vector` | `sparseVector` | `sparse_vector` |

Edge values expose only requested fields:

| Field | Rust | Node.js | Python |
|-------|------|---------|--------|
| ID | `id` | `id` | `id` |
| Source node ID | `from` | `from` | `from_id` |
| Target node ID | `to` | `to` | `to_id` |
| Label | `label` | `label` | `label` |
| Properties | `props` | `props` | `props` |
| Weight | `weight` | `weight` | `weight` |
| Created timestamp | `created_at` | `createdAt` | `created_at` |
| Updated timestamp | `updated_at` | `updatedAt` | `updated_at` |
| Valid-from timestamp | `valid_from` | `validFrom` | `valid_from` |
| Valid-to timestamp | `valid_to` | `validTo` | `valid_to` |

Returning a node element omits dense and sparse vectors by default:

```javascript
const withoutVectors = db.executeGql(
  "MATCH (n:Person {name: 'Ada'}) RETURN n"
);

// withoutVectors.rows[0].n.denseVector is undefined
```

Opt in when the query result needs vectors:

```javascript
const withVectors = db.executeGql(
  "MATCH (n:Person {name: 'Ada'}) RETURN n",
  null,
  { includeVectors: true }
);
```

Path values expose identity arrays and, when a path alias is returned as a value, hydrated node and
edge elements:

| Field | Rust | Node.js | Python |
|-------|------|---------|--------|
| Node IDs | `node_ids` | `nodeIds` | `node_ids` |
| Edge IDs | `edge_ids` | `edgeIds` | `edge_ids` |
| Nodes | `nodes` | `nodes` | `nodes` |
| Edges | `edges` | `edges` | `edges` |

Hydrated nodes inside path values follow the same vector policy as returned node values.

#### Params

Params are named and referenced with `$name` syntax.

Only params referenced by the query are resource-validated. Referenced list/map params are bounded
by `max_ast_depth` and `max_literal_items`; referenced string, bytes, and map-key payload bytes are
bounded by `max_param_bytes`. Extra unused params are ignored by the GQL engine.

Param values:

| Shape | Node.js | Python | Rust |
|-------|---------|--------|------|
| Null | `null` / `undefined` value | `None` | `GqlParamValue::Null` |
| Boolean | `true` / `false` | `True` / `False` | `GqlParamValue::Bool` |
| Signed integer | negative safe integer | negative `int` | `GqlParamValue::Int` |
| Unsigned integer | non-negative safe integer | non-negative `int` / `u64` extraction | `GqlParamValue::UInt` |
| Float | finite non-integer `number` | finite `float` | `GqlParamValue::Float` |
| String | `string` | `str` | `GqlParamValue::String` |
| Bytes | `Buffer` / `ArrayBuffer` | `bytes` | `GqlParamValue::Bytes` |
| List | `Array` | `list` / `tuple` | `GqlParamValue::List` |
| Map | plain object | `dict` with string keys | `GqlParamValue::Map` |

Node.js example:

```javascript
const result = db.executeGql(
  `MATCH (p:Person {name: $name})-[r:WORKS_AT]->(c:Company)
   WHERE r.since >= $minSince AND p.status IN $statuses
   RETURN id(p) AS personId, p.name AS person, c.name AS company, $payload AS payload`,
  {
    name: 'Ada',
    minSince: 2020,
    statuses: ['active', 'trial'],
    payload: { source: 'api', bytes: Buffer.from('trace-id') },
  }
);
```

Rust typed params and options:

```rust
let params = GqlParams::from([
    ("name".to_string(), GqlParamValue::String("Ada".to_string())),
    ("minRank".to_string(), GqlParamValue::Int(2)),
]);

let options = GqlExecutionOptions {
    include_plan: true,
    profile: true,
    ..GqlExecutionOptions::default()
};

let result = db.execute_gql(
    "MATCH (n:Person {name: $name}) WHERE n.rank >= $minRank RETURN id(n) AS id",
    &params,
    &options,
)?;
```

#### Explain, Profile, and Stats

`explain_gql` / `explainGql` validates and plans the statement without executing read rows or
mutating data. In `Auto` mode, mutation explain is side-effect safe: it does not allocate IDs,
create labels, stage writes, or commit. Schema and index explain are also side-effect safe and do
not publish. In `ReadOnly` mode, data mutations, schema publication statements, and mutating index
statements are rejected.

Explain result fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Kind | `kind` | `kind` | `kind` | `Query` / `Mutation` / `Schema` / `Index`, or `"query"` / `"mutation"` / `"schema"` / `"index"` in connectors. |
| Columns | `columns` | `columns` | `columns` | Output column names after `RETURN` expansion and aliases. |
| Read explain | `read` | `read` | `read` | Nested read-plan payload for read statements or mutation read prefixes. |
| Mutation explain | `mutation` | `mutation` | `mutation` | Nested mutation plan payload for mutation statements. |
| Schema explain | `schema` | `schema` | `schema` | Nested schema plan payload for schema statements. |
| Index explain | `index` | `index` | `index` | Nested index plan payload for property-index statements. |
| Caps | `caps` | `caps` | `caps` | Effective execution caps. |
| Warnings | `warnings` | `warnings` | `warnings` | GQL planning warnings. |
| Notes | `notes` | `notes` | `notes` | Human-readable execution/planning notes. |

Nested read explain fields:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| `columns` | `columns` | `columns` | Output columns for the read target. |
| `target` | `target` | `target` | One of `node_query`, `edge_query`, `graph_row_query`, or `graph_pipeline_query`. |
| `nativePlan` / `native_plan` | `nativePlan` | `native_plan` | Populated for direct node/edge plans; row and pipeline plans are summarized in projection/warnings. |
| `pushedDown` / `pushed_down` | `pushedDown` | `pushed_down` | Predicates represented in the selected read plan. |
| `residual` | `residual` | `residual` | Predicates evaluated after the selected read plan. |
| `projection` | `projection` | `projection` | Projection, row-op, order, cursor, cap, and note summaries. |
| `rowOps` / `row_ops` | `rowOps` | `row_ops` | `residual_filter`, `sort`, `skip`, `limit`, `projection`. |
| `caps` | `caps` | `caps` | Effective read cap summary. |
| `warnings` | `warnings` | `warnings` | Read planning warnings. |

For `graph_pipeline_query`, `projection` summarizes the read stages, including match, projection,
`DISTINCT`, aggregation, union, shortest path, subquery, row operations, cursors, and caps. Execution
stats are reported on `stats`; `elapsedUs` / `elapsed_us` is populated only when `profile` is true.

Nested mutation explain fields:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| `readPrefix` / `read_prefix` | `readPrefix` | `read_prefix` | Planned read-prefix payload, if present. Its nested `graphRowTarget` / `graph_row_target` can report `graph_row_query` or `graph_pipeline_query`. |
| `operations` | `operations` | `operations` | Mutation operation summaries with op, target alias, row multiplicity, and details. |
| `returnPlan` / `return_plan` | `returnPlan` | `return_plan` | Mutation `RETURN` columns, order item count, skip, limit, and post-commit hydration summary. |
| `wouldCreateNodeLabels` / `would_create_node_labels` | `wouldCreateNodeLabels` | `would_create_node_labels` | Node labels that could be created on execution. |
| `wouldCreateEdgeLabels` / `would_create_edge_labels` | `wouldCreateEdgeLabels` | `would_create_edge_labels` | Edge labels that could be created on execution. |
| `usesTransactionSnapshot` / `uses_transaction_snapshot` | `usesTransactionSnapshot` | `uses_transaction_snapshot` | True for mutation planning over the write transaction snapshot. |
| `usesWriteTxn` / `uses_write_txn` | `usesWriteTxn` | `uses_write_txn` | True for executable mutations. |
| `replacementAdapters` / `replacement_adapters` | `replacementAdapters` | `replacement_adapters` | True when SET/REMOVE may replace records by ID. |
| `atomicCommit` / `atomic_commit` | `atomicCommit` | `atomic_commit` | True when the plan commits as one transaction. |

Nested schema explain fields:

| Field | Node.js | Python | Description |
|-------|---------|--------|-------------|
| `operation` | `operation` | `operation` | Operation string such as `alter_graph_type_set`, `check_graph_type_add`, or `show_node_schema`. |
| `targets` | `targets` | `targets` | Planned node/edge/schema targets with target kind, label, and action. |
| `replacesEntireCatalog` / `replaces_entire_catalog` | `replacesEntireCatalog` | `replaces_entire_catalog` | True for full-catalog replacement statements. |
| `publishesManifest` / `publishes_manifest` | `publishesManifest` | `publishes_manifest` | True for statements that can publish schema state when executed. |
| `validatesExistingData` / `validates_existing_data` | `validatesExistingData` | `validates_existing_data` | True for publish/check statements that scan affected existing records. |
| `usesCoreWriteQueue` / `uses_core_write_queue` | `usesCoreWriteQueue` | `uses_core_write_queue` | True for mutating schema statements. |
| `sideEffectFree` / `side_effect_free` | `sideEffectFree` | `side_effect_free` | True for `CHECK`, `SHOW`, and all `explain_gql` schema planning. |
| `options` | `options` | `options` | Effective `max_violations`, `chunk_size`, and `scan_limit` for schema validation. |

`includePlan` / `include_plan` attaches that same explain payload to executed results:

```javascript
const result = db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WITH c.name AS company, count(*) AS people
   RETURN company, people
   ORDER BY people DESC
   LIMIT 10`,
  null,
  { includePlan: true, profile: true }
);

console.log(result.plan.kind);         // 'query'
console.log(result.plan.read.target);  // 'graph_pipeline_query'
console.log(result.plan.read.rowOps);  // e.g. ['sort', 'limit', 'projection']
console.log(result.stats.elapsedUs);   // populated when profile is true
```

Stats fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Rows returned | `rows_returned` | `rowsReturned` | `rows_returned` | Final result row count. |
| Rows matched | `rows_matched` | `rowsMatched` | `rows_matched` | Rows produced or observed by read execution before final projection. |
| Rows after filter | `rows_after_filter` | `rowsAfterFilter` | `rows_after_filter` | Rows remaining after residual filtering before final row ops. |
| Intermediate bindings | `intermediate_bindings` | `intermediateBindings` | `intermediate_bindings` | Maximum/representative intermediate row count held by execution. |
| Work counter | `db_hits` | `dbHits` | `db_hits` | Best-effort profile work units, not a storage IO contract. |
| Elapsed time | `elapsed_us` | `elapsedUs` | `elapsed_us` | Populated only when profile is true. |
| Warnings | `warnings` | `warnings` | `warnings` | Cap, ordering, full-scan, and planning warnings. |

Mutation stats fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Rows matched | `rows_matched` | `rowsMatched` | `rows_matched` | Read-prefix rows observed by the mutation. |
| Mutation rows | `mutation_rows` | `mutationRows` | `mutation_rows` | Rows that contributed mutation work. |
| Mutation ops | `mutation_ops` | `mutationOps` | `mutation_ops` | Logical mutation operations staged, including cascades. |
| Nodes created | `nodes_created` | `nodesCreated` | `nodes_created` | Created nodes. |
| Nodes updated | `nodes_updated` | `nodesUpdated` | `nodes_updated` | Updated nodes after coalescing. |
| Nodes deleted | `nodes_deleted` | `nodesDeleted` | `nodes_deleted` | Deleted nodes. |
| Edges created | `edges_created` | `edgesCreated` | `edges_created` | Created edges. |
| Edges updated | `edges_updated` | `edgesUpdated` | `edges_updated` | Updated edges after coalescing. |
| Edges deleted | `edges_deleted` | `edgesDeleted` | `edges_deleted` | Direct and cascaded deleted edges. |
| Labels added | `labels_added` | `labelsAdded` | `labels_added` | Node labels added. |
| Labels removed | `labels_removed` | `labelsRemoved` | `labels_removed` | Node labels removed. |
| Properties set | `properties_set` | `propertiesSet` | `properties_set` | Properties or metadata fields set. |
| Properties removed | `properties_removed` | `propertiesRemoved` | `properties_removed` | Properties removed. |
| Skipped null targets | `skipped_null_targets` | `skippedNullTargets` | `skipped_null_targets` | Optional-null targets skipped without writes. |
| Duplicate targets | `duplicate_targets` | `duplicateTargets` | `duplicate_targets` | Duplicate writes/deletes coalesced or deduped. |
| Work counter | `db_hits` | `dbHits` | `db_hits` | Best-effort profile work units, not a storage IO contract. |
| Elapsed time | `elapsed_us` | `elapsedUs` | `elapsed_us` | Populated only when profile is true. |
| Warnings | `warnings` | `warnings` | `warnings` | Mutation planning/execution warnings. |

Schema stats fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Operation | `operation` | `operation` | `operation` | Schema operation string, for example `alter_graph_type_add` or `show_current_graph_type`. |
| Targets checked | `targets_checked` | `targetsChecked` | `targets_checked` | Number of schema targets validated. |
| Targets published | `targets_published` | `targetsPublished` | `targets_published` | Number of schema targets published. |
| Targets dropped | `targets_dropped` | `targetsDropped` | `targets_dropped` | Number of schema targets removed. |
| Checked records | `checked_records` | `checkedRecords` | `checked_records` | Matching live records scanned during validation. |
| Violation count | `violation_count` | `violationCount` | `violation_count` | Total schema violations observed. |
| Truncated | `truncated` | `truncated` | `truncated` | True if retained violation examples were capped. |
| Scan limit hit | `scan_limit_hit` | `scanLimitHit` | `scan_limit_hit` | True if validation stopped at `scan_limit`. |
| Elapsed time | `elapsed_us` | `elapsedUs` | `elapsed_us` | Populated only when profile is true. |
| Warnings | `warnings` | `warnings` | `warnings` | Schema planning/execution warnings. |

Full scans are rejected unless explicitly allowed:

```javascript
db.executeGql('MATCH (n) RETURN id(n) AS id');
// throws: full scan / allowFullScan required

const broad = db.executeGql(
  'MATCH (n) RETURN id(n) AS id LIMIT 100',
  null,
  { allowFullScan: true }
);
```

The same full-scan rule applies to mutation read prefixes. For example,
`MATCH (n) SET n.seen = true` requires `allowFullScan: true` / `allow_full_scan=True`.

ReadOnly mode rejects data mutations, mutating schema statements, and mutating index statements
before write staging or catalog publication. `CHECK` / schema `SHOW` and property-index `SHOW`
statements are allowed:

```javascript
db.executeGql(
  "CREATE (n:Person {elementKey: 'blocked'})",
  null,
  { mode: 'readOnly' }
);
// throws: ReadOnly violation
```

Mutation, schema, and index statements reject cursors:

```javascript
db.executeGql(
  "CREATE (n:Person {elementKey: 'bad-cursor'})",
  null,
  { cursor: 'opaque-read-cursor' }
);
// throws: GQL mutation statements do not accept cursors

db.executeGql(
  "SHOW CURRENT GRAPH TYPE",
  null,
  { cursor: 'opaque-read-cursor' }
);
// throws: GQL schema statements do not accept cursors

db.executeGql(
  "SHOW PROPERTY INDEXES",
  null,
  { cursor: 'opaque-read-cursor' }
);
// throws: GQL index statements do not accept cursors
```

#### Examples

GQL create mutation:

```javascript
const created = db.executeGql(
  `CREATE (p:Person {elementKey: $key, name: $name, status: 'active'})
   RETURN p.name AS name`,
  { key: 'ada', name: 'Ada' }
);

console.log(created.kind);                    // 'mutation'
console.log(created.mutationStats.nodesCreated);
```

GQL set mutation with returned rows:

```javascript
const updated = db.executeGql(
  `MATCH (p:Person)
   WHERE elementKey(p) = $key
   SET p.status = 'active'
   RETURN elementKey(p) AS key, p.status AS status`,
  { key: 'ada' }
);

console.log(updated.rows);
console.log(updated.mutationStats.nodesUpdated);
```

Keyed node `MERGE` with `ON CREATE SET` / `ON MATCH SET`:

```javascript
const merged = db.executeGql(
  `MATCH (s:Source)
   WITH s.target_key AS key
   MERGE (a:Account {elementKey: key})
   ON CREATE SET a.status = 'created', a.count = 1
   ON MATCH SET a.status = 'matched', a.count = coalesce(a.count, 0) + 1
   RETURN DISTINCT elementKey(a) AS key, a.status AS status, a.count AS count`
);
```

Unique relationship `MERGE`:

```javascript
const rel = db.executeGql(
  `MATCH (a:Person {elementKey: $from_key})
   MATCH (b:Person {elementKey: $to_key})
   MERGE (a)-[r:KNOWS]->(b)
   ON CREATE SET r.since = 2026
   ON MATCH SET r.seen = true
   RETURN r`
);
```

GQL delete mutation:

```javascript
db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WHERE elementKey(p) = $key
   DELETE r`,
  { key: 'ada' }
);
```

GQL schema publish and show:

```javascript
db.executeGql(
  `ALTER CURRENT GRAPH TYPE ADD {
     NODE Person = {
       properties: { name: { required: true, nullable: false, types: ['string'] } }
     }
   }`
);

const schemas = db.executeGql('SHOW NODE SCHEMA Person');
console.log(schemas.kind); // 'schema'
console.log(schemas.rows[0].schema.properties.name.types);
```

Basic node rows:

```javascript
const people = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);
```

Parameterized property lookup with a property map:

```javascript
const person = db.executeGql(
  'MATCH (n:Person {name: $name}) RETURN id(n) AS id, n.name AS name',
  { name: 'Ada' }
);
```

One-hop relationship pattern:

```javascript
const jobs = db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   RETURN p.name AS person, r.since AS since, c.name AS company
   ORDER BY r.since DESC
   LIMIT 20`
);
```

Residual `WHERE` with params:

```javascript
const filtered = db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WHERE p.status = $status AND r.since >= $minSince
   RETURN p.name AS person, r.role AS role, c.name AS company`,
  { status: 'active', minSince: 2020 }
);
```

Direct edge query:

```javascript
const likes = db.executeGql(
  'MATCH ()-[r:LIKES]->() RETURN id(r) AS id, type(r) AS label, weight(r) AS weight LIMIT 25'
);
```

Multi-hop fixed pattern:

```javascript
const paths = db.executeGql(
  `MATCH (a:Person)-[r:KNOWS]->(b:Person)-[s:WORKS_AT]->(c:Company)
   RETURN a.name AS source, b.name AS friend, c.name AS company
   LIMIT 50`
);
```

Optional match:

```javascript
const rows = db.executeGql(
  `MATCH (p:Person)
   OPTIONAL MATCH (p)-[:REPORTS_TO]->(m:Person)
   RETURN p.name AS person, m.name AS manager
   ORDER BY p.name`,
  null,
  { allowFullScan: true }
);
```

`WITH`, rich expressions, `WITH DISTINCT`, and aggregation:

```javascript
const rows = db.executeGql(
  `MATCH (p:Person)
   WITH DISTINCT p.group AS group,
        count(*) AS people,
        collect(DISTINCT lower(trim(p.status))) AS statuses
   WHERE people > 1
   RETURN group, people, statuses
   ORDER BY people DESC`
);
```

Bounded path value and path functions:

```javascript
const rows = db.executeGql(
  `MATCH p = (a:Person)-[:KNOWS*1..3]->(b:Person)
   WHERE a.name = $name
   RETURN p, length(p) AS hops, nodeIds(p) AS nodeIds, edgeIds(p) AS edgeIds
   ORDER BY hops ASC
   LIMIT 10`,
  { name: 'Ada' }
);

console.log(rows.rows[0].p.nodeIds);
console.log(rows.rows[0].p.edgeIds);
```

Constrained shortest path with pre-bound endpoints:

```python
paths = db.execute_gql(
    """
    MATCH (a:Person {elementKey: $from_key})
    WITH a
    MATCH (b:Person {elementKey: $to_key})
    WITH a, b
    MATCH p = shortestPath((a)-[:KNOWS*1..4]->(b))
    RETURN p, nodeIds(p) AS node_ids, edgeIds(p) AS edge_ids, length(p) AS hops
    """,
    {"from_key": "ada", "to_key": "cy"},
)
```

Continuation cursor:

```javascript
const first = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);

const second = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10',
  null,
  { cursor: first.nextCursor }
);
```

Read-only union:

```javascript
const candidates = db.executeGql(
  `MATCH (p:Person) WHERE p.status = 'active'
   RETURN p.name AS name
   UNION ALL
   MATCH (p:Person) WHERE p.status = 'invited'
   RETURN p.name AS name`
);
```

Read-only `EXISTS {}` and `CALL {}` subqueries:

```javascript
const rows = db.executeGql(
  `MATCH (p:Person)
   WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(c:Company) RETURN c }
   WITH p
   CALL { MATCH (p)-[:WORKS_AT]->(c:Company) RETURN c.name AS company }
   RETURN p.name AS person, company`
);
```

`RETURN *` expands user-visible bound aliases in deterministic binding order:

```javascript
const bindings = db.executeGql(
  'MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN * LIMIT 10'
);
```

Element projection returns node and edge objects:

```javascript
const rows = db.executeGql(
  'MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p, r, c LIMIT 5'
);

console.log(rows.rows[0].p.labels);
console.log(rows.rows[0].r.from, rows.rows[0].r.to);
```

`SKIP` / `OFFSET` and `LIMIT`:

```javascript
const page = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank DESC SKIP 20 LIMIT 10'
);
```

Python result shape:

```python
result = db.execute_gql(
    "MATCH (n:Person) RETURN n.name AS name, n.rank AS rank ORDER BY n.rank",
    compact_rows=True,
)

print(result["columns"])  # ["name", "rank"]
print(result["rows"])     # [["Ben", 1], ["Ada", 2]]
print(result["stats"]["rows_returned"])
```

Explain without execution:

```javascript
const explain = db.explainGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WHERE p.status = $status
   RETURN p.name AS person, c.name AS company`,
  { status: 'active' }
);

console.log(explain.kind);
console.log(explain.read?.target);
console.log(explain.read?.warnings);
```

Async connector calls:

```javascript
const result = await db.executeGqlAsync(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);
```

```python
result = await async_db.execute_gql(
    "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10"
)
```

#### Current Limits

GQL is intentionally narrower than ISO GQL and Cypher. It rejects:

- Full ISO GQL
- Full Cypher compatibility
- `DELETE n` without `DETACH`, and `RETURN` after `DELETE` or `DETACH DELETE`
- Mutation cursors and mutation `RETURN` aggregation
- Read-after-write graph matching, including `WITH`, `MATCH`, `CALL`, `UNION`, or subqueries after the first write clause
- Schema/index operations outside the supported current-graph-type and property-index DDL subsets,
  including name-based `CREATE INDEX`, constraints, named graph types, graph catalog/session DDL, and
  database lifecycle DDL
- Vector writes or vector mutation syntax
- Mutating subqueries and procedure calls such as `CALL db.labels()`
- Unsupported `MERGE` shapes: unkeyed nodes, multi-label nodes, identity maps beyond a single `elementKey` entry,
  relationship properties in the `MERGE` pattern, unbound endpoints, relationship MERGE without
  `edge_uniqueness = true`, undirected or variable-length relationship MERGE, path-assigned MERGE,
  and general pattern MERGE
- Mixed `UNION` / `UNION ALL` chains and mutation branches in `UNION`
- `UNWIND`, `FOREACH`, and `LOAD CSV`
- Dynamic labels and dynamic relationship types
- Unbounded variable-length paths, weighted shortest-path GQL syntax, all-pairs shortest path, and broad shortest-path endpoint scans
- Advanced path functions beyond `length`, `startNode`, `endNode`, `nodes`, `relationships`, `nodeIds`, and `edgeIds`
- Multi-hop relationship-list aliases separate from path aliases
- Path assignment over multiple relationship segments
- Pattern-local predicates inside node or relationship patterns
- List/map and non-finite-float `ORDER BY` domains
- Mutation `RETURN ORDER BY` or `RETURN DISTINCT` on commit-assigned or same-mutation-volatile metadata

Use structured query APIs when you need request-object construction, strongly bounded pagination by
ID, native upsert semantics, vector writes, schema/index management, or APIs outside GQL. Use
GQL when a graph query or mutation reads better as text.

---

### Query Request Types and Plans

#### NodeQuery

Node query request fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| label_filter / labelFilter | `Option<NodeLabelFilter>` | `labelFilter?: { labels: string[], mode: "any" \| "all" }` | `label_filter?: {"labels": list[str], "mode": "any" \| "all"}` | Node-label constraint with explicit `Any` / `All` semantics. A one-label `All` filter uses the single-label fast path. |
| ids | `Vec<u64>` | `ids?: number[]` | `ids?: list[int]` | Explicit node ID candidates. OR within the list. |
| keys | `Vec<String>` | `keys?: string[]` | `keys?: list[str]` | Label-scoped key candidates. OR within the list. |
| filter | `Option<NodeFilterExpr>` | `filter?: QueryNodeFilter \| null` | `filter?: QueryNodeFilter \| None` | Recursive node filter tree. Omit or pass null/None for no filter. |
| limit | `page.limit` | `limit?: number` | `limit?: int` | Page size. Omit for unlimited. Connector `0` means unlimited. |
| after | `page.after` | `after?: number` | `after?: int` | Cursor from a previous page. Returns items with node IDs strictly greater than `after`. |
| allow_full_scan | `bool` | `allowFullScan?: boolean` | `allow_full_scan?: bool` | Required for unanchored full scan fallback. |

Top-level `label_filter` / `labelFilter`, `ids`, and `keys` are ANDed with `filter`.
A one-label `All` filter uses the direct node-label fast path. Key lookups require exactly one resolved node label.
Label-less verify-only filters require `allow_full_scan` / `allowFullScan` unless `ids` or `keys`
provide a legal bounded universe.

---

#### NodeFilter / QueryNodeFilter

Rust uses `NodeFilterExpr`. Node.js and Python use the canonical recursive `QueryNodeFilter`
object shape.

| Filter shape | Node.js | Python | Meaning |
|--------------|---------|--------|---------|
| Equality | `{ property: "status", eq: "active" }` | `{"property": "status", "eq": "active"}` | Property exactly equals value |
| IN | `{ property: "status", in: ["active", "trial"] }` | `{"property": "status", "in": ["active", "trial"]}` | Property equals any listed value |
| Range | `{ property: "score", gte: 50 }` | `{"property": "score", "gte": 50}` | Numeric/range comparison |
| Range with two bounds | `{ property: "score", gt: 50, lte: 100 }` | `{"property": "score", "gt": 50, "lte": 100}` | Bounded numeric/range comparison |
| Exists | `{ property: "embedding", exists: true }` | `{"property": "embedding", "exists": True}` | Property key is present |
| Missing | `{ property: "deletedAt", missing: true }` | `{"property": "deleted_at", "missing": True}` | Property key is absent |
| AND | `{ and: [filter, ...] }` | `{"and": [filter, ...]}` | All children must match |
| OR | `{ or: [filter, ...] }` | `{"or": [filter, ...]}` | Any child may match |
| NOT | `{ not: filter }` | `{"not": filter}` | Child must not match |
| Updated-at range | `{ updatedAt: { gte: ms } }` | `{"updated_at": {"gte": ms}}` | Built-in node `updated_at` timestamp range |

Property values use OverGraph's normal `PropValue` conversion rules. There is no query-only
coercion for non-numeric values. For example, string `"1"` does not match integer `1`.
Finite scalar numeric values do use semantic numeric equality and ordering, so integer `1`,
unsigned integer `1`, and float `1.0` compare equal while range comparisons can mix numeric
bound variants.

`in` is equivalent to equality OR for matching semantics. When a ready equality index exists, the
query engine may evaluate it as an indexed union, but final visible-record verification still decides
correctness.

`exists` and `missing` are key-presence predicates, not null checks. `exists` matches when the
property key is present even if its value is null. `missing` matches only when the key is absent.

`not` is verifier-first. A negative filter does not anchor a broad query by itself.

`or` can use an indexed union only when every branch is bounded. If any OR branch is verify-only or
requires fallback, the whole OR subtree is verified over the nearest legal universe rather than
planned as a partial union.

Results from node queries are ordered by node ID ascending. The `after` cursor means strictly
greater than that node ID.

##### Built-in timestamp versus same-named user property

The built-in timestamp filter is a structural field. User property names always live in the
`property` value field, so they do not collide with built-ins.

**Node.js**
```javascript
// Built-in node timestamp:
db.queryNodeIds({
  labelFilter: { labels: ['Document'], mode: 'all' },
  filter: { updatedAt: { gte: startMs, lt: endMs } },
});

// User property literally named "updatedAt":
db.queryNodeIds({
  labelFilter: { labels: ['Document'], mode: 'all' },
  filter: { property: 'updatedAt', eq: 'manual-value' },
});
```

**Python**
```python
# Built-in node timestamp:
db.query_node_ids({
    "label_filter": {"labels": ["Document"], "mode": "all"},
    "filter": {"updated_at": {"gte": start_ms, "lt": end_ms}},
})

# User property literally named "updated_at":
db.query_node_ids({
    "label_filter": {"labels": ["Document"], "mode": "all"},
    "filter": {"property": "updated_at", "eq": "manual-value"},
})
```

---

#### EdgeQuery

Direct edge query request fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| label | `Option<String>` | `label?: string` | `label?: str` | Optional edge-label constraint. |
| ids | `Vec<u64>` | `ids?: number[]` | `ids?: list[int]` | Explicit edge ID candidates. OR within the list. |
| from_ids | `Vec<u64>` | `fromIds?: number[]` | `from_ids?: list[int]` | Source endpoint candidates. OR within the list. |
| to_ids | `Vec<u64>` | `toIds?: number[]` | `to_ids?: list[int]` | Target endpoint candidates. OR within the list. |
| endpoint_ids | `Vec<u64>` | `endpointIds?: number[]` | `endpoint_ids?: list[int]` | Either-endpoint candidates. OR within the list. |
| filter | `Option<EdgeFilterExpr>` | `filter?: QueryEdgeFilter \| null` | `filter?: QueryEdgeFilter \| None` | Recursive edge filter tree. |
| limit | `page.limit` | `limit?: number` | `limit?: int` | Page size. Omit for unlimited. Connector `0` means unlimited. |
| after | `page.after` | `after?: number` | `after?: int` | Cursor from a previous page. Returns edge IDs strictly greater than `after`. |
| allow_full_scan | `bool` | `allowFullScan?: boolean` | `allow_full_scan?: bool` | Required for direct filter-only or unanchored full scans. |

Top-level edge anchors are ANDed with `filter`; values inside each list are ORed. A filter-only
direct edge query requires explicit full-scan opt-in even when metadata sidecars are available.

---

#### EdgeFilter / QueryEdgeFilter

Rust uses `EdgeFilterExpr`. Node.js and Python use the canonical recursive `QueryEdgeFilter`
object shape.

| Filter shape | Node.js | Python | Meaning |
|--------------|---------|--------|---------|
| Equality | `{ property: "role", eq: "lead" }` | `{"property": "role", "eq": "lead"}` | Edge property exactly equals value |
| IN | `{ property: "role", in: ["lead", "owner"] }` | `{"property": "role", "in": ["lead", "owner"]}` | Edge property equals any listed value |
| Range | `{ property: "score", gte: 50 }` | `{"property": "score", "gte": 50}` | Edge property range comparison |
| Exists | `{ property: "role", exists: true }` | `{"property": "role", "exists": True}` | Edge property key is present |
| Missing | `{ property: "role", missing: true }` | `{"property": "role", "missing": True}` | Edge property key is absent |
| Weight range | `{ weight: { gte: 1.0 } }` | `{"weight": {"gte": 1.0}}` | Built-in edge weight range |
| Updated-at range | `{ updatedAt: { gte: ms } }` | `{"updated_at": {"gte": ms}}` | Built-in edge update timestamp range |
| Valid-at | `{ validAt: ms }` | `{"valid_at": ms}` | Half-open validity check: `valid_from <= ms < valid_to` |
| Valid-from range | `{ validFrom: { gte: ms } }` | `{"valid_from": {"gte": ms}}` | Built-in `valid_from` range |
| Valid-to range | `{ validTo: { gt: ms } }` | `{"valid_to": {"gt": ms}}` | Built-in `valid_to` range |
| AND | `{ and: [filter, ...] }` | `{"and": [filter, ...]}` | All children must match |
| OR | `{ or: [filter, ...] }` | `{"or": [filter, ...]}` | Any child may match |
| NOT | `{ not: filter }` | `{"not": filter}` | Child must not match |

Weight ranges reject NaN. `-0.0` and `+0.0` compare as the same value. Ready edge-property
declarations may provide equality, `IN`, and range candidate sources for edge-label-scoped edge filters;
metadata filters may use private edge metadata sources when available. All edge filters still run
final verification for correctness.

---

#### GraphRowQuery

`GraphRowQuery` is the public structured row-query request. It is exported by Rust and accepted by
the Node.js and Python connectors as host-language objects/dicts.

Top-level request fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| nodes | `nodes: Vec<GraphNodePattern>` | `nodes?: GraphRowNodePattern[]` | `"nodes": list[dict]` | Node aliases and node constraints. |
| pieces | `pieces: Vec<GraphPatternPiece>` | `pieces?: GraphRowPatternPiece[]` | `"pieces": list[dict]` | Fixed edge pieces, optional groups, and bounded variable-length path pieces. |
| where | `where_: Option<GraphExpr>` | `where?: GraphExpr` | `"where": GraphExpr` | Row-level residual predicate with null semantics. |
| returns | `return_items: Option<Vec<GraphReturnItem>>` | `return?: GraphReturnItem[]` | `"return": list[dict]` | Output columns. Omitted means `RETURN *` over visible aliases. |
| order | `order_by: Vec<GraphOrderItem>` | `orderBy?: GraphOrderItem[]` | `"order_by": list[dict]` | Final-row ordering. Omitted uses stable logical row-key order. |
| page | `page: GraphPageRequest` | `skip`, `limit`, `cursor` | `skip`, `limit`, `cursor` | Final-row skip/limit/cursor. Native graph-row `limit` must be greater than zero. |
| at epoch | `at_epoch: Option<i64>` | `atEpoch?: number` | `at_epoch?: int` | Temporal edge visibility timestamp. Omitted resolves an effective epoch for the operation. |
| params | `params: BTreeMap<String, GraphParamValue>` | `params?: Record<string, GraphParamValue>` | `"params": dict` | Named values referenced by `GraphExpr::Param`. |
| output | `output: GraphOutputOptions` | `output?: GraphOutputOptions` | `"output": dict` | Output mode, compact rows, and vector inclusion. |
| options | `options: GraphQueryOptions` | `options?: GraphQueryOptions` | `"options": dict` | Validation, safety caps, plan/profile options. |

Node pattern fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| alias | `alias` | `alias` | `alias` | Unique node alias. |
| label filter | `label_filter` | `labelFilter` | `label_filter` | Optional `NodeLabelFilter` with `Any` / `All` semantics. |
| IDs | `ids` | `ids` | `ids` | Explicit node ID candidates. |
| Keys | `keys` | `keys` | `keys` | Label-scoped key candidates. Node.js accepts strings or `{ label, key }`; Python accepts strings or dicts. Node.js string shorthand requires a single-label `labelFilter`; Python string shorthand requires `label_filter` with exactly one `all`-mode label. Use explicit label/key objects or dicts otherwise. |
| filter | `filter` | `filter` | `filter` | Recursive node filter tree. |

Pattern pieces:

| Piece | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| Fixed edge | `GraphPatternPiece::Edge(GraphEdgePattern)` | `{ kind: 'edge', ... }` | `{"kind": "edge", ...}` | Binds or constrains one edge between two node aliases. |
| Optional group | `GraphPatternPiece::Optional(GraphOptionalGroup)` | `{ kind: 'optional', pieces, where? }` | `{"kind": "optional", "pieces": [...], "where": ...}` | Left-outer optional group. On miss, aliases introduced by the group are null. |
| Variable-length path | `GraphPatternPiece::VariableLength(GraphVariableLengthPattern)` | `{ kind: 'variableLength', ... }` | `{"kind": "variable_length", ...}` | Bounded path with `minHops`/`maxHops` or `min_hops`/`max_hops`. |

Fixed edge and variable-length pieces share these fields:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| edge alias | `alias` / `edge_alias` | `alias` / `edgeAlias` | `alias` / `edge_alias` | Fixed edge alias, or one-hop VLP edge alias. Multi-hop VLP edge aliases are rejected. |
| path alias | `path_alias` | `pathAlias` | `path_alias` | Optional path alias for variable-length pieces. |
| endpoints | `from_alias`, `to_alias` | `fromAlias`, `toAlias` | `from`/`from_alias`, `to`/`to_alias` | Node aliases at each end of the piece. |
| direction | `direction` | `direction` | `direction` | `outgoing`, `incoming`, or `both`; defaults to outgoing in connectors. |
| labels | `label_filter` | `labelFilter` | `labels` or `label_filter` | Edge-label list. |
| filter | `filter` | `filter` | `filter` | Recursive edge filter tree. |
| hops | `min_hops`, `max_hops` | `minHops`, `maxHops` | `min_hops`, `max_hops` | Required for variable-length pieces. Must be finite and within caps. |

Graph expressions:

| Expression | Rust | Node.js | Python |
|------------|------|---------|--------|
| Literal null/bool/number/string | `GraphExpr::Null`, etc. | `null`, booleans, numbers, strings | `None`, booleans, numbers, strings |
| Bytes | `GraphExpr::Bytes` | `{ bytes: number[] }` | `{"bytes": [0, 1]}` or Python `bytes` for params |
| List | `GraphExpr::List` | `{ list: [...] }` | `{"list": [...]}` |
| Map | `GraphExpr::Map` | `{ map: { key: expr } }` | `{"map": {"key": expr}}` |
| Param | `GraphExpr::Param("name")` | `{ param: 'name' }` | `{"param": "name"}` |
| Binding | `GraphExpr::Binding("alias")` | `{ binding: 'alias' }` | `{"binding": "alias"}` |
| Property | `GraphExpr::Property { alias, key }` | `{ property: { alias, key } }` | `{"property": {"alias": alias, "key": key}}` |
| Node field | `GraphExpr::NodeField` | `{ nodeField: { alias, field } }` | `{"node_field": {"alias": alias, "field": field}}` |
| Edge field | `GraphExpr::EdgeField` | `{ edgeField: { alias, field } }` | `{"edge_field": {"alias": alias, "field": field}}` |
| Path field | `GraphExpr::PathField` | `{ pathField: { alias, field } }` | `{"path_field": {"alias": alias, "field": field}}` |
| Function | `GraphExpr::Function` | `{ fn: 'id', args: [...] }` | `{"fn": "id", "args": [...]}` |
| Binary op | `GraphExpr::Binary` | `{ op: '=', left, right }` | `{"op": "=", "left": ..., "right": ...}` |
| Not | `GraphExpr::Unary` | `{ op: 'not', expr }` | `{"op": "not", "expr": ...}` |
| Null tests | `GraphExpr::IsNull` / `IsNotNull` | `{ isNull: expr }` / `{ isNotNull: expr }` | `{"is_null": expr}` / `{"is_not_null": expr}` |

Supported node fields are `id`, `labels`, `key`, `weight`, `created_at`/`createdAt`, and
`updated_at`/`updatedAt`. Supported edge fields are `id`, `from`, `to`, `label`/`type`, `weight`,
`created_at`/`createdAt`, `updated_at`/`updatedAt`, `valid_from`/`validFrom`, and
`valid_to`/`validTo`. Supported path fields are `node_ids`/`nodeIds`, `edge_ids`/`edgeIds`, and
`length`.

Supported native function names are `id`, `labels`, `type`, `length`, `nodes`, and
`relationships`; Rust names the path endpoint functions `start_node` and `end_node`. Node.js accepts
`startNode`/`endNode` and also accepts snake_case `start_node`/`end_node`. Python accepts
`start_node`/`end_node`.

Rust uses `GraphExpr::PathField` for `node_ids` and `edge_ids`. Node.js accepts `nodeIds`,
`node_ids`, `edgeIds`, and `edge_ids` in the `fn` tag as connector conveniences. Python accepts
`node_ids` and `edge_ids` in the `fn` tag. These conveniences parse into path-field expressions and
require a direct path binding argument.

Binary ops are `or`, `and`, equality (`=`, `==`, `eq`), inequality (`<>`, `!=`, `neq`), comparisons
(`<`, `<=`, `>`, `>=`), and `in`. Node.js also accepts comparison aliases `lt`, `lte`, `gt`, and
`gte`.

Output options:

| Field | Rust | Node.js | Python | Default | Description |
|-------|------|---------|--------|---------|-------------|
| mode | `GraphOutputMode` | `mode: 'ids' \| 'elements' \| 'projected'` | `"mode": "ids" \| "elements" \| "projected"` | `ids` | Default projection mode for `auto` return items. |
| compact rows | `compact_rows` | `compactRows` | `compact_rows` | `false` | Connector rows become arrays instead of objects. |
| include vectors | `include_vectors` | `includeVectors` | `include_vectors` | `false` | Include dense/sparse vectors in full hydrated node values. |

Graph-row caps:

| Field | Rust | Node.js | Python | Default |
|-------|------|---------|--------|---------|
| allow full scan | `allow_full_scan` | `allowFullScan` | `allow_full_scan` | `false` |
| max intermediate bindings | `max_intermediate_bindings` | `maxIntermediateBindings` | `max_intermediate_bindings` | `65536` |
| max frontier | `max_frontier` | `maxFrontier` | `max_frontier` | `65536` |
| max path hops | `max_path_hops` | `maxPathHops` | `max_path_hops` | `16` |
| max paths per start | `max_paths_per_start` | `maxPathsPerStart` | `max_paths_per_start` | `4096` |
| max page limit | `max_page_limit` | `maxPageLimit` | `max_page_limit` | `10000` |
| max order materialization | `max_order_materialization` | `maxOrderMaterialization` | `max_order_materialization` | `65536` |
| max cursor bytes | `max_cursor_bytes` | `maxCursorBytes` | `max_cursor_bytes` | `16384` |
| max query bytes | `max_query_bytes` | `maxQueryBytes` | `max_query_bytes` | `1048576` |
| include plan | `include_plan` | `includePlan` | `include_plan` | `false` |
| profile | `profile` | `profile` | `profile` | `false` |

Native graph-row cursors are final-row cursors over `(order atoms, logical row key)`. They validate
the normalized query fingerprint and are not physical frontier cursors or pinned storage snapshots.
When `at_epoch` is omitted, the engine resolves an effective epoch for the first page and stores it
in the cursor.

---

#### QueryPlan

Explain APIs return:

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| kind | `kind` | `kind` | `kind` | `node_query` or `edge_query`. Graph-row explain uses `GraphRowExplain`, not `QueryPlan`. |
| root | `root` | `root` | `root` | Recursive plan node. |
| estimated candidates | `estimated_candidates` | `estimatedCandidates` | `estimated_candidates` | Optional candidate count estimate. |
| warnings | `warnings` | `warnings` | `warnings` | Stable lower_snake warning strings. |
| notes | `notes` | `notes` | `notes` | Stable lower_snake informational planner notes. |
| public inputs | `public_inputs` | `publicInputs` | `public_inputs` | Normalized public node-label and edge-label names referenced during planning. |

Plan node kinds include:

| Plan node kind | Meaning |
|----------------|---------|
| `empty_result` | Impossible filter or empty candidate universe. |
| `explicit_ids` | Explicit ID candidate universe. |
| `key_lookup` | Label-scoped key lookup. |
| `node_label_index` | Label index candidate source. |
| `node_label_any_index` | Node-label `Any` candidate source. |
| `property_equality_index` | Ready equality property index candidate source. |
| `property_range_index` | Ready range property index candidate source. |
| `compound_equality_index` | Ready compound equality declaration candidate source using a satisfied left prefix. |
| `compound_range_index` | Ready compound range declaration candidate source using equality prefix fields plus one range field. |
| `timestamp_index` | Built-in timestamp index candidate source. |
| `explicit_edge_ids` | Explicit edge ID candidate universe. |
| `edge_label_index` | Edge label index candidate source. |
| `edge_triple_index` | Exact `(from, to, label)` edge lookup source. |
| `edge_endpoint_adjacency` | Endpoint adjacency candidate source. |
| `edge_weight_index` | Optional edge weight sidecar candidate source. |
| `edge_updated_at_index` | Optional edge update-time sidecar candidate source. |
| `edge_validity_index` | Optional edge validity sidecar candidate source. |
| `edge_metadata_scan` | Edge metadata scan candidate source. |
| `edge_property_equality_index` | Ready edge-property equality declaration candidate source. |
| `edge_property_range_index` | Ready edge-property range declaration candidate source. |
| `intersect` | Sorted intersection of bounded candidate sources. |
| `union` | Sorted union of bounded OR/IN candidate sources. |
| `verify_node_filter` | Final visible-record verification of the full node filter. |
| `verify_edge_filter` | Final visible-edge metadata/property verification. |
| `adjacency_expansion` | Bounded adjacency expansion used by planner-backed edge candidates. |
| `verify_edge_predicates` | Edge post-filter verification. |
| `fallback_node_label_scan` | Label-scoped scan universe. |
| `fallback_full_node_scan` | Explicit full node scan universe. |
| `fallback_edge_label_scan` | Edge-label-scoped edge scan universe. |
| `fallback_full_edge_scan` | Explicit full edge scan universe. |

Warning strings include:

| Warning | Meaning |
|---------|---------|
| `missing_ready_index` | Needed index is absent, not ready, or unavailable. |
| `compound_index_prefix_not_satisfied` | A compound declaration existed, but the query did not constrain its left prefix. |
| `using_fallback_scan` | Query used a scan universe. |
| `full_scan_requires_opt_in` | Query would need a full scan but the caller did not opt in. |
| `full_scan_explicitly_allowed` | Full scan ran because caller opted in. |
| `edge_property_post_filter` | Edge properties were checked after bounded expansion. |
| `index_skipped_as_broad` | Ready index existed but was skipped as too broad. |
| `candidate_cap_exceeded` | Candidate cap prevented materializing a source. |
| `range_candidate_cap_exceeded` | Range candidate cap prevented bounded range materialization. |
| `timestamp_candidate_cap_exceeded` | Timestamp candidate cap prevented bounded timestamp materialization. |
| `verify_only_filter` | Some filter subtree ran only through verification. |
| `boolean_branch_fallback` | Boolean branch or OR was cheaper or safer as verifier fallback. |
| `planning_probe_budget_exceeded` | Planning probe/union budget forced fallback. |
| `unknown_node_label` | A requested node label is not present in the catalog. |
| `unknown_edge_label` | A requested edge label is not present in the catalog. |

Note strings include:

| Note | Meaning |
|------|---------|
| `node_label_any_dedupe_before_pagination` | `Any` node-label planning deduplicates candidates before pagination. |
| `node_label_any_final_verification` | `Any` node-label results are verified against final visible node records. |
| `node_label_all_superset_verification` | `All` node-label planning used a superset index source followed by final verification. |
| `stale_node_label_membership_verification` | Node-label index membership may include stale entries and is verified against visible records. |

---

#### Validation notes

Invalid filter shapes:

| Invalid shape | Why |
|---------------|-----|
| `{}` | Empty filter object is not a valid filter. |
| `{ and: [] }` | `and` must contain at least one child. |
| `{ or: [] }` | `or` must contain at least one child. |
| `{ not: null }` | `not` must contain exactly one filter object. |
| `{ AND: [...] }` | Uppercase boolean aliases are not supported. |
| `{ property: "", eq: "active" }` | Property key must be non-empty. |
| `{ eq: "active" }` | Property leaves require `property`. |
| `{ property: "status" }` | Property leaf must specify one operator family. |
| `{ property: "status", eq: "active", in: ["active"] }` | Mixed operator families are invalid. |
| `{ property: "status", in: [] }` | `in` list must be non-empty. |
| `{ property: "x", exists: false }` | `exists` only accepts true; use `missing: true` for the opposite. |
| `{ property: "x", missing: false }` | `missing` only accepts true; use `exists: true` for the opposite. |
| `{ property: "score", gt: 1, gte: 1 }` | Cannot specify both exclusive and inclusive lower bounds. |
| `{ updatedAt: { eq: 123 } }` | Updated-at filters support range bounds only. |

`filter` omitted/null/undefined/None means no node filter. `filter: {}` is invalid.

Boolean objects cannot contain sibling tags. For example, `{ and: [...], or: [...] }` and
`{ and: [...], property: "x", eq: 1 }` are invalid. No uppercase boolean or operator aliases are
accepted.

---

## Pagination

All paginated methods use **keyset (cursor-based) pagination**, not offset-based. This provides stable results even when data is inserted between pages.

The pattern is the same across all paginated methods:
- Pass `limit` for the page size and `after` as the cursor.
- Most paginated methods use an ID cursor.
- `find_nodes_range_paged` uses a structured range cursor keyed by `(value, node_id)`.
- The result includes `items` and `next_cursor` (`None`/`null` when there are no more pages).

### nodes_by_labels_paged

Paginated node-label scan. Returns IDs only.

```rust
let page = db.nodes_by_labels_paged("User", &PageRequest { limit: Some(100), after: None })?;
let admin_page = db.nodes_by_labels_paged(
    vec!["User".into(), "Admin".into()],
    &PageRequest { limit: Some(100), after: None },
)?;
// page.items: Vec<u64>, page.next_cursor: Option<u64>
```

```javascript
let page = db.nodesByLabelsPaged('User', 100); // limit=100, no cursor
let adminPage = db.nodesByLabelsPaged(['User', 'Admin'], 100);
// page = { items: Float64Array, nextCursor: number | null }

// Next page:
page = db.nodesByLabelsPaged('User', 100, page.nextCursor);
```

```python
page = db.nodes_by_labels_paged("User", limit=100)
admin_page = db.nodes_by_labels_paged(["User", "Admin"], limit=100)
# page.items: IdArray, page.next_cursor: int | None

# Next page:
page = db.nodes_by_labels_paged("User", limit=100, after=page.next_cursor)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| labels | `impl IntoNodeLabels` | `string \| string[]` | `str \| list[str]` | Yes | — | Label or labels to match. Nodes must contain every supplied node label. |
| limit | `Option<usize>` | `number` | `int` | No | Unlimited | Maximum items per page. |
| after | `Option<u64>` | `number` | `int` | No | `None` (start from beginning) | Cursor. Returns items with IDs strictly greater than this value. Use `next_cursor` from a previous result. |

#### Returns: PageResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<u64>` | `Float64Array` | `IdArray` | IDs in this page. |
| next_cursor | `Option<u64>` | `number \| null` | `int \| None` | Cursor for the next page. `None`/`null` means this is the last page. |

---

### edges_by_label_paged

Paginated edge-label scan. Returns edge IDs only.

```rust
let page = db.edges_by_label_paged(
    "WORKS_ON",
    &PageRequest { limit: Some(100), after: None },
)?;
// page.items: Vec<u64>, page.next_cursor: Option<u64>
```

```javascript
let page = db.edgesByLabelPaged('WORKS_ON', 100); // limit=100, no cursor
// page = { items: Float64Array, nextCursor: number | null }

// Next page:
page = db.edgesByLabelPaged('WORKS_ON', 100, page.nextCursor);
```

```python
page = db.edges_by_label_paged("WORKS_ON", limit=100)
# page.items: IdArray, page.next_cursor: int | None

# Next page:
page = db.edges_by_label_paged("WORKS_ON", limit=100, after=page.next_cursor)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| label | `&str` | `string` | `str` | Yes | - | Public edge label to match. |
| limit | `Option<usize>` | `number` | `int` | No | Unlimited | Maximum edge IDs per page. |
| after | `Option<u64>` | `number` | `int` | No | `None` (start from beginning) | Cursor. Returns edge IDs strictly greater than this value. Use `next_cursor` from a previous result. |

#### Returns: PageResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<u64>` | `Float64Array` | `IdArray` | Edge IDs in this page. |
| next_cursor | `Option<u64>` | `number \| null` | `int \| None` | Cursor for the next page. `None`/`null` means this is the last page. |

Unknown edge labels return an empty page. Tombstoned edges are excluded. Paged edge-label scans are ordered by edge ID.

---

### get_nodes_by_labels_paged

Paginated hydrated node-label scan. Returns full node records.

```rust
let page = db.get_nodes_by_labels_paged("User", &PageRequest { limit: Some(50), after: None })?;
let admin_page = db.get_nodes_by_labels_paged(
    vec!["User".into(), "Admin".into()],
    &PageRequest { limit: Some(50), after: None },
)?;
// page.items: Vec<NodeView>
```

```javascript
const page = db.getNodesByLabelsPaged('User', 50);
const adminPage = db.getNodesByLabelsPaged(['User', 'Admin'], 50);
// page.items: NodeView[]
```

```python
page = db.get_nodes_by_labels_paged("User", limit=50)
admin_page = db.get_nodes_by_labels_paged(["User", "Admin"], limit=50)
# page.items: list[NodeView]
```

---

### get_edges_by_label_paged

Paginated hydrated edge-label scan. Returns full edge records.

```rust
let page = db.get_edges_by_label_paged(
    "WORKS_ON",
    &PageRequest { limit: Some(50), after: None },
)?;
// page.items: Vec<EdgeView>
```

```javascript
const page = db.getEdgesByLabelPaged('WORKS_ON', 50);
// page.items: EdgeView[]
```

```python
page = db.get_edges_by_label_paged("WORKS_ON", limit=50)
# page.items: list[EdgeView]
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| label | `&str` | `string` | `str` | Yes | - | Public edge label to match. |
| limit | `Option<usize>` | `number` | `int` | No | Unlimited | Maximum edge records per page. |
| after | `Option<u64>` | `number` | `int` | No | `None` (start from beginning) | Cursor. Returns edge records with IDs strictly greater than this value. Use `next_cursor` from a previous result. |

#### Returns: PageResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<EdgeView>` | `EdgeView[]` | `list[EdgeView]` | Full edge records in this page. |
| next_cursor | `Option<u64>` | `number \| null` | `int \| None` | Cursor for the next page. `None`/`null` means this is the last page. |

Unknown edge labels return an empty page. Tombstoned edges are excluded. The implementation pages IDs first and hydrates only the requested page.

---

### find_nodes_paged

Paginated version of [`find_nodes`](#find_nodes).

```rust
let page = db.find_nodes_paged(
    "User",
    "role",
    &PropValue::String("admin".into()),
    &PageRequest { limit: Some(50), after: None },
)?;
```

```javascript
const page = db.findNodesPaged('User', 'role', 'admin', { limit: 50 });
```

```python
page = db.find_nodes_paged("User", "role", "admin", limit=50)
```

---

### find_nodes_range_paged

Paginated version of [`find_nodes_range`](#find_nodes_range).

**Rust**
```rust
let page = db.find_nodes_range_paged(
    "User",
    "score",
    Some(&PropertyRangeBound::Included(PropValue::Int(10))),
    Some(&PropertyRangeBound::Excluded(PropValue::Float(20.0))),
    &PropertyRangePageRequest {
        limit: Some(50),
        after: None,
    },
)?;
```

**Node.js**
```javascript
const page = db.findNodesRangePaged(
  'User',
  'score',
  { value: 10, inclusive: true, domain: 'int' },
  { value: 20, inclusive: false, domain: 'float' },
  { limit: 50 },
);
```

**Python**
```python
page = db.find_nodes_range_paged(
    "User",
    "score",
    PropertyRangeBound(10, domain="int"),
    PropertyRangeBound(20.0, inclusive=False, domain="float"),
    limit=50,
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| label | `&str` | `string` | `str` | Yes | — | Restrict search to this node label. |
| prop_key | `&str` | `string` | `str` | Yes | — | Numeric property key to query. |
| lower | `Option<&PropertyRangeBound>` | `PropertyRangeBound \| null \| undefined` | `PropertyRangeBound \| None` | No | Unbounded | Lower bound. |
| upper | `Option<&PropertyRangeBound>` | `PropertyRangeBound \| null \| undefined` | `PropertyRangeBound \| None` | No | Unbounded | Upper bound. |
| limit | `Option<usize>` | `number` | `int` | No | Unlimited | Maximum items per page. |
| after | `Option<PropertyRangeCursor>` | `PropertyRangeCursor` | `PropertyRangeCursor` | No | `None` | Cursor from a previous range page. Reuse the same query arguments when resuming. |

#### Returns: PropertyRangePageResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| items | `Vec<u64>` | `Float64Array` | `IdArray` | Node IDs in range order for this page. |
| next_cursor | `Option<PropertyRangeCursor>` | `PropertyRangeCursor \| null \| undefined` | `PropertyRangeCursor \| None` | Cursor for the next page, or no cursor on the last page. |

#### Behavior

- At least one finite numeric bound is required.
- Bounds and cursors may mix signed integer, unsigned integer, and finite float values.
- Empty finite numeric intervals return an empty page.
- Non-finite floats, non-numeric values, arrays, and maps are invalid bounds or cursors.
- When resuming with `after`, keep the same `label`, `prop_key`, and bounds.
- Invalid bound or cursor combinations return an error.

---

### find_nodes_by_time_range_paged

Paginated version of [`find_nodes_by_time_range`](#find_nodes_by_time_range).

```javascript
const page = db.findNodesByTimeRangePaged('User', startMs, endMs, { limit: 50 });
```

```python
page = db.find_nodes_by_time_range_paged("User", start_ms, end_ms, limit=50)
```

---

## Traversal

### neighbors

Retrieves the immediate neighbors of a node (one hop). The most common graph traversal operation.

**Rust**
```rust
let entries = db.neighbors(node_id, &NeighborOptions {
    direction: Direction::Outgoing,
    edge_label_filter: Some(vec!["WORKS_ON".into()]),
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
  edgeLabelFilter: ['WORKS_ON'],
  limit: 10,
});

for (const n of list) {
  console.log(n.nodeId, n.edgeId, n.weight);
}
```

**Python**
```python
entries = db.neighbors(node_id, direction="outgoing", edge_label_filter=["WORKS_ON"], limit=10)
for entry in entries:
    print(entry.node_id, entry.edge_id, entry.weight)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_id | `u64` | `number` | `int` | Yes | — | Node to query neighbors for. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Traversal direction. `"outgoing"`, `"incoming"`, or `"both"`. |
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` (all labels) | Only return neighbors connected by edges with these labels. |
| limit | `Option<usize>` | `number` | `int` | No | `None` (unlimited) | Maximum number of neighbors to return. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` (current time) | Temporal filter. Only edges whose validity window contains this timestamp are included. `None` means the current wall-clock time. |
| decay_lambda | `Option<f32>` | `number` | `float` | No | `None` (no decay) | Exponential decay factor. When set, each neighbor's weight is multiplied by `exp(-λ × age_hours)` where `age_hours = max(at_epoch - valid_from, 0) / 3_600_000`. |

#### Returns: NeighborEntry

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| node_id | `u64` | `number` | `int` | ID of the neighboring node. |
| edge_id | `u64` | `number` | `int` | ID of the connecting edge. |
| label | `String` | `label: string` | `label: str` | Label of the connecting edge. |
| weight | `f32` | `number` | `float` | Edge weight (or decay-adjusted score if `decay_lambda` is set). |
| valid_from | `i64` | `number` | `int` | Edge validity start (ms). |
| valid_to | `i64` | `number` | `int` | Edge validity end (ms). |

**Node.js**: Returns `NeighborEntry[]` as plain objects, so you can use normal array access like `list[i].nodeId`.

#### Performance

~294ns for a node with 10 edges, ~2.1μs for 100 edges (memtable hot path).

---

### neighbors_paged

Paginated version of [`neighbors`](#neighbors).

```javascript
let page = db.neighborsPaged(nodeId, { direction: 'outgoing', limit: 20 });
// page.items: NeighborEntry[], page.nextCursor: number | null
console.log(page.items[0].nodeId);

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
// results: { queryNodeId: number, neighbors: NeighborEntry[] }[]
console.log(results[0].neighbors[0].nodeId);
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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Edge label filter. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| decay_lambda | `Option<f32>` | `number` | `float` | No | `None` | Decay factor. Uses hours from `valid_from` when set. |

#### Returns

A map/array mapping each query node ID to its list of neighbors.

---

### top_k_neighbors

Returns the top K neighbors of a node ranked by a scoring criterion.

**Rust**
```rust
let top = db.top_k_neighbors(node_id, 5, &TopKOptions {
    direction: Direction::Outgoing,
    scoring: ScoringMode::DecayAdjusted { lambda: 0.01 },
    ..Default::default()
})?;
```

**Node.js**
```javascript
const top = db.topKNeighbors(nodeId, 5, {
  direction: 'outgoing',
  scoring: 'decay',
  decayLambda: 0.01,
});
console.log(top[0].nodeId, top[0].weight);
```

**Python**
```python
top = db.top_k_neighbors(
    node_id,
    5,
    direction="outgoing",
    scoring="decay",
    decay_lambda=0.01,
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_id | `u64` | `number` | `int` | Yes | — | Source node. |
| k | `usize` | `number` | `int` | Yes | — | Number of top neighbors to return. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Traversal direction. |
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Edge label filter. |
| scoring | `ScoringMode` | `string` | `str` | No | `Weight` | Scoring criterion. Rust carries the decay lambda inside `ScoringMode::DecayAdjusted { lambda }`; connectors use `scoring: "decay"` plus `decayLambda` / `decay_lambda`. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| decay_lambda | — | `number` | `float` | No | `None` | Connector-only option required when `scoring = "decay"`. |

**Scoring modes:**

| Mode | Rust | Node.js / Python | Description |
|------|------|------------------|-------------|
| Weight | `ScoringMode::Weight` | `"weight"` | Rank by edge weight (descending). |
| Recency | `ScoringMode::Recency` | `"recency"` | Rank by recency. More recent edges score higher. |
| DecayAdjusted | `ScoringMode::DecayAdjusted { lambda }` | `"decay"` | Exponential decay: `weight × exp(-λ × age_hours)`, where `age_hours = max(at_epoch - valid_from, 0) / 3_600_000`. Connectors require `decay_lambda`. |

#### Returns

Array of `NeighborEntry` sorted by score descending. Length is `min(k, actual_neighbor_count)`.

---

### traverse

Breadth-first traversal from a starting node up to a maximum depth. Supports pagination, edge-label filtering, emission-only node-label filtering, temporal filtering, and decay scoring.

**Rust**
```rust
let result = db.traverse(start_id, &TraverseOptions {
    min_depth: 1,
    direction: Direction::Outgoing,
    edge_label_filter: Some(vec!["WORKS_ON".into()]),
    emit_node_label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into(), "Admin".into()],
        mode: LabelMatchMode::Any,
    }),
    at_epoch: None,
    decay_lambda: None,
    limit: Some(100),
    cursor: None,
})?;

for hit in &result.items {
    println!("node={}, depth={}", hit.node_id, hit.depth);
}
```

**Node.js**
```javascript
const result = db.traverse(startId, 3, {
  minDepth: 1,
  direction: 'outgoing',
  edgeLabelFilter: ['WORKS_ON'],
  emitNodeLabelFilter: { labels: ['User', 'Admin'], mode: 'any' },
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
    edge_label_filter=["WORKS_ON"],
    emit_node_label_filter={"labels": ["User", "Admin"], "mode": "any"},
    limit=100)

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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` (all labels) | Only follow edges with these labels. |
| emit_node_label_filter | `Option<NodeLabelFilter>` | `emitNodeLabelFilter: { labels: string[], mode: "any" \| "all" }` | `emit_node_label_filter: dict` | No | `None` (all labels) | Node-label filter for emitted nodes. Traversal may still pass through non-emitted labels. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter for edge validity. |
| decay_lambda | `Option<f64>` | `number` | `float` | No | `None` | Depth-based traversal score. When set, each hit receives `exp(-λ × depth)`. |
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
let sg = db.extract_subgraph(root_id, 3, &SubgraphOptions {
    direction: Direction::Outgoing,
    edge_label_filter: None,
    node_label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into()],
        mode: LabelMatchMode::Any,
    }),
    at_epoch: None,
})?;
println!("{} nodes, {} edges", sg.nodes.len(), sg.edges.len());
```

**Node.js**
```javascript
const sg = db.extractSubgraph(rootId, 3, {
  direction: 'outgoing',
  nodeLabelFilter: { labels: ['User'], mode: 'any' },
});
console.log(sg.nodes.length, 'nodes,', sg.edges.length, 'edges');
```

**Python**
```python
sg = db.extract_subgraph(
    root_id,
    max_depth=3,
    direction="outgoing",
    node_label_filter={"labels": ["User"], "mode": "any"},
)
print(len(sg.nodes), "nodes,", len(sg.edges), "edges")
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| start_node_id | `u64` | `number` | `int` | Yes | — | Root node. |
| max_depth | `u32` | `number` | `int` | Yes | — | Maximum hops from root. |
| direction | `Direction` | `string` | `str` | No | `Outgoing` | Direction. |
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Edge label filter. |
| node_label_filter | `Option<NodeLabelFilter>` | `nodeLabelFilter: { labels: string[], mode: "any" \| "all" }` | `node_label_filter: dict` | No | `None` | Node-label filter for nodes to include and expand through. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns: Subgraph

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| nodes | `Vec<NodeView>` | `NodeView[]` | `list[NodeView]` | All nodes in the subgraph (full records). |
| edges | `Vec<EdgeView>` | `EdgeView[]` | `list[EdgeView]` | All edges in the subgraph (full records). |

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
    println!("path: {:?}, cost: {}", p.nodes, p.total_cost);
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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Only traverse these edge labels. |
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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Edge label filter. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |
| max_depth | `Option<u32>` | `number` | `int` | No | `None` | Maximum search depth. |

#### Returns

`bool`. Returns `true` if a path exists, `false` otherwise.

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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Only count edges with these labels. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<u32, EngineError>` | `number` | `int` |

The edge count.

#### Performance

Metadata-only fast path for unfiltered, non-temporal queries when all visible segments have valid degree sidecars. O(edges) walk fallback when filtering by edge label, using a temporal epoch, running with active prune policies, reading a node with temporal incident edges, or reading through a segment whose degree sidecar is missing/corrupt.

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

## Graph Analytics

### connected_components

Computes all [weakly connected components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) in the graph. Treats edges as undirected regardless of their actual direction.

**Rust**
```rust
let components = db.connected_components(&ComponentOptions {
    node_label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into()],
        mode: LabelMatchMode::Any,
    }),
    ..Default::default()
})?;
// components: NodeIdMap<u64> - node_id to component_id
```

**Node.js**
```javascript
const entries = db.connectedComponents({
  nodeLabelFilter: { labels: ['User'], mode: 'any' },
});
// entries: { nodeId: number, componentId: number }[]
```

**Python**
```python
components = db.connected_components(node_label_filter={"labels": ["User"], "mode": "any"})
# components: dict[int, int] - node_id to component_id
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Only consider these edge labels when determining connectivity. |
| node_label_filter | `Option<NodeLabelFilter>` | `nodeLabelFilter: { labels: string[], mode: "any" \| "all" }` | `node_label_filter: dict` | No | `None` | Node-label filter for nodes included in components. |
| at_epoch | `Option<i64>` | `number` | `int` | No | `None` | Temporal filter. |

#### Returns

A mapping from every node ID to its component ID. The component ID is the smallest node ID within each component (a canonical representative).

---

### component_of

Returns all nodes in the same connected component as a given node.

```rust
let node_ids = db.component_of(node_id, &ComponentOptions {
    node_label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into()],
        mode: LabelMatchMode::Any,
    }),
    ..Default::default()
})?;
```

```javascript
const nodeIds = db.componentOf(nodeId); // Float64Array
```

```python
node_ids = db.component_of(node_id, node_label_filter={"labels": ["User"], "mode": "any"})  # list[int]
```

#### Parameters

Same as [`connected_components`](#connected_components), plus:

| Parameter | Rust | Node.js | Python | Required | Description |
|-----------|------|---------|--------|----------|-------------|
| node_id | `u64` | `number` | `int` | Yes | Node to find the component for. |

#### Returns

| Rust | Node.js | Python |
|------|---------|--------|
| `Result<Vec<u64>, EngineError>` | `Float64Array` | `list[int]` |

All three surfaces return the sorted node IDs in the same connected component as the requested node.

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
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Only follow these edge labels during the walk. |
| max_results | `Option<usize>` | `number` | `int` | No | `None` (all) | Return only the top N nodes by score. |

#### Returns: PprResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| scores | `Vec<(u64, f64)>` | — | — | Rust scored node pairs sorted by score descending. |
| node IDs | — | `nodeIds: Float64Array` | `node_ids: list[int]` | Connector node IDs sorted by score descending. |
| scores | — | `scores: Float64Array` | `scores: list[float]` | Connector scores corresponding to node IDs. Exact PPR sums to 1.0 (or very close); approximate PPR is optimized for ranking quality rather than strict normalization. |
| iterations | `u32` | `number` | `int` | Number of exact power iterations performed. Approximate mode returns `0`. |
| converged | `bool` | `boolean` | `bool` | Exact mode: whether the algorithm converged within `max_iterations`. Approximate mode: `true` when no node remains above the residual tolerance. |
| algorithm | `PprAlgorithm` | `string` | `str` | Which algorithm produced the result. |
| approx | `Option<PprApproxMeta>` | `PprApproxMeta \| null` | `PprApproxMeta \| None` | Approximate-mode metadata. `None`/`null` in exact mode. |

---

### export_adjacency

Exports the graph's adjacency structure as flat arrays. Useful for bulk analysis, NetworkX integration, or external graph processing.

**Rust**
```rust
let export = db.export_adjacency(&ExportOptions {
    node_label_filter: Some(NodeLabelFilter {
        labels: vec!["User".into(), "Admin".into()],
        mode: LabelMatchMode::Any,
    }),
    include_weights: true,
    ..Default::default()
})?;
println!("node label side table: {:?}", export.node_labels);
println!("per-node label indexes: {:?}", export.node_label_indexes);
```

**Node.js**
```javascript
const adj = db.exportAdjacency({
  nodeLabelFilter: { labels: ['User', 'Admin'], mode: 'any' },
  includeWeights: true,
});
// adj.nodeIds: Float64Array
// adj.edgeLabels: string[]
// adj.edgeFrom: Float64Array
// adj.edgeTo: Float64Array
// adj.edgeLabelIndexes: Uint32Array
// adj.edgeWeights: Float64Array | undefined
```

**Python**
```python
adj = db.export_adjacency(
    node_label_filter={"labels": ["User", "Admin"], "mode": "any"},
    include_weights=True,
)
# adj.node_ids: list[int]
# adj.node_labels: list[str]
# adj.node_label_indexes: list[list[int]]
# adj.edge_labels: list[str]
# adj.edges: list[ExportEdge] - each has from_id, to_id, edge_label_index, weight
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| node_label_filter | `Option<NodeLabelFilter>` | `nodeLabelFilter: { labels: string[], mode: "any" \| "all" }` | `node_label_filter: dict` | No | `None` | Node-label filter for exported nodes. |
| edge_label_filter | `Option<Vec<String>>` | `edgeLabelFilter: string[]` | `edge_label_filter: list[str]` | No | `None` | Only export edges with these labels. |
| include_weights | `bool` | `boolean` | `bool` | No | `true` | Include edge weights in the export. Set to `false` to save memory/bandwidth when weights aren't needed. |

#### Returns: AdjacencyExport

Rust:

| Field | Type | Description |
|-------|------|-------------|
| node_ids | `Vec<u64>` | Live node IDs in the exported graph. |
| node_labels | `Vec<String>` | Export-local node-label side table. |
| node_label_indexes | `Vec<Vec<u32>>` | Per-node label side-table indexes, aligned with `node_ids`. |
| edge_labels | `Vec<String>` | Export-local edge-label side table. |
| edges | `Vec<ExportEdge>` | Exported edges. Each `ExportEdge.edge_label_index` references `edge_labels`. |

Node.js:

| Field | Type | Description |
|-------|------|-------------|
| nodeIds | `Float64Array` | Live node IDs in the exported graph. |
| edgeLabels | `string[]` | Export-local edge-label side table. |
| edgeFrom | `Float64Array` | Source node IDs, aligned with `edgeTo` and `edgeLabelIndexes`. |
| edgeTo | `Float64Array` | Destination node IDs. |
| edgeLabelIndexes | `Uint32Array` | Edge-label side-table indexes, aligned with `edgeFrom` / `edgeTo`. |
| edgeWeights | `Float64Array \| undefined` | Edge weights when `includeWeights` is true. |

Python:

| Field | Type | Description |
|-------|------|-------------|
| node_ids | `list[int]` | Live node IDs in the exported graph. |
| node_labels | `list[str]` | Export-local node-label side table. |
| node_label_indexes | `list[list[int]]` | Per-node label side-table indexes, aligned with `node_ids`. |
| edge_labels | `list[str]` | Export-local edge-label side table. |
| edges | `list[ExportEdge]` | Exported edges. Each edge has `from_id`, `to_id`, `edge_label_index`, and optional `weight`. |

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
    sparse_query: None,
    k: 10,
    label_filter: None,
    ef_search: Some(100),
    scope: None,
    dense_weight: None,
    sparse_weight: None,
    fusion_mode: None,
})?;

// Sparse search
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Sparse,
    dense_query: None,
    sparse_query: Some(vec![(42, 0.9), (128, 0.5)]),
    k: 10,
    label_filter: None,
    ef_search: None,
    scope: None,
    dense_weight: None,
    sparse_weight: None,
    fusion_mode: None,
})?;

// Hybrid search
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Hybrid,
    dense_query: Some(embedding),
    sparse_query: Some(sparse_terms),
    k: 10,
    label_filter: Some(NodeLabelFilter {
        labels: vec!["Document".into(), "Published".into()],
        mode: LabelMatchMode::All,
    }),
    ef_search: None,
    scope: None,
    dense_weight: Some(0.7),
    sparse_weight: Some(0.3),
    fusion_mode: Some(FusionMode::WeightedScoreFusion),
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
  labelFilter: { labels: ['User', 'Project'], mode: 'any' },
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
    label_filter={"labels": ["Document", "Published"], "mode": "all"},
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
| label_filter | `Option<NodeLabelFilter>` | `labelFilter: { labels: string[], mode: "any" \| "all" }` | `label_filter: dict` | No | `None` | Node-label filter. |
| ef_search | `Option<usize>` | `number` | `int` | No | `128` | HNSW search expansion factor. The effective dense fetch limit is at least `k` and at least `8`. Higher values improve recall at the cost of latency. Only applies to dense/hybrid modes. |

**Hybrid fusion parameters** (only used when `mode = "hybrid"`):

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| dense_weight | `Option<f32>` | `number` | `float` | No | `1.0` | Weight for dense scores in fusion. |
| sparse_weight | `Option<f32>` | `number` | `float` | No | `1.0` | Weight for sparse scores in fusion. |
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
| scope.edge_label_filter | `Option<Vec<String>>` | `scope.edgeLabelFilter: string[]` | `scope_edge_label_filter: list[str]` | No | `None` | Edge labels for scope traversal. |
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
    label: Some("Conversation".into()),           // only conversations
})?;
println!("pruned {} nodes, {} edges", result.nodes_pruned, result.edges_pruned);
```

```javascript
const result = db.prune({
  maxAgeMs: 7 * 24 * 60 * 60 * 1000,
  maxWeight: 0.1,
  label: 'Conversation',
});
```

```python
result = db.prune(
    max_age_ms=7 * 24 * 60 * 60 * 1000,
    max_weight=0.1,
    label="Conversation",
)
```

#### Parameters

| Parameter | Rust | Node.js | Python | Required | Default | Description |
|-----------|------|---------|--------|----------|---------|-------------|
| max_age_ms | `Option<i64>` | `number` | `int` | No* | `None` | Delete nodes older than `now - max_age_ms` milliseconds. Age is computed from `updated_at`. |
| max_weight | `Option<f32>` | `number` | `float` | No* | `None` | Delete nodes with `weight <= max_weight`. |
| label | `Option<String>` | `string` | `str` | No | `None` (all labels) | Restrict pruning to a single node label. |

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
    max_weight: None,
    label: Some("Conversation".into()),
})?;
```

```javascript
db.setPrunePolicy('stale-conversations', {
  maxAgeMs: 30 * 24 * 60 * 60 * 1000,
  label: 'Conversation',
});
```

```python
db.set_prune_policy("stale-conversations",
    max_age_ms=30 * 24 * 60 * 60 * 1000,
    label="Conversation")
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
let policies = db.list_prune_policies()?;
for info in &policies {
    println!("{}: max_age_ms={:?}", info.name, info.policy.max_age_ms);
}
```

```javascript
const policies = db.listPrunePolicies();
// [{ name: string, policy: { maxAgeMs?, maxWeight?, label? } }]
```

```python
policies = db.list_prune_policies()
for p in policies:
    print(p.name, p.max_age_ms, p.max_weight, p.label)
```

#### Returns

Array of named policies. Rust and Node.js entries contain a nested policy object; Python flattens
policy fields onto each entry.

| Field path | Rust | Node.js | Python | Description |
|------------|------|---------|--------|-------------|
| name | `info.name: String` | `entry.name: string` | `p.name: str` | Policy name. |
| policy | `info.policy: PrunePolicy` | `entry.policy: PrunePolicy` | — | Nested policy object in Rust and Node.js. |
| max age | `info.policy.max_age_ms: Option<i64>` | `entry.policy.maxAgeMs?: number` | `p.max_age_ms: int \| None` | Age threshold. |
| max weight | `info.policy.max_weight: Option<f32>` | `entry.policy.maxWeight?: number` | `p.max_weight: float \| None` | Weight threshold. |
| label | `info.policy.label: Option<String>` | `entry.policy.label?: string` | `p.label: str \| None` | Node-label scope. |

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
info = db.flush()  # SegmentInfo | None
```

#### Returns

| Rust | Node.js | Python | Description |
|------|---------|--------|-------------|
| `Result<Option<SegmentInfo>, EngineError>` | `void` | `SegmentInfo \| None` | Info about the written segment, or `None` if the memtable was empty. |

**SegmentInfo** (Rust/Python):

| Field | Rust | Python | Description |
|-------|------|--------|-------------|
| id | `u64` | `int` | Segment ID on disk. |
| node_count | `u64` | `int` | Nodes in the segment. |
| edge_count | `u64` | `int` | Edges in the segment. |
| segment_format_version | `u32` | — | Rust segment format version. |
| segment_data_id | `[u8; 32]` | — | Rust segment data identifier. |

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

### scrub

Runs an offline integrity check across all segments. Recomputes SHA-256 payload digests for every component and compares them to the digests recorded at write time. Reports mismatches without modifying any data.

**Rust**
```rust
let report = db.scrub()?;
println!("checked: {}, failed: {}", report.total_components_checked, report.total_components_failed);
for seg in &report.segments {
    for f in &seg.findings {
        eprintln!("segment {}: {} — {}", seg.segment_id, f.finding_type, f.detail);
    }
}
```

**Node.js**
```javascript
const report = db.scrub();
console.log(`checked: ${report.totalComponentsChecked}, failed: ${report.totalComponentsFailed}`);

// async
const report = await db.scrubAsync();
```

**Python**
```python
report = db.scrub()
print(f"checked: {report.total_components_checked}, failed: {report.total_components_failed}")

# async
report = await db.scrub()
```

#### Parameters

None.

#### Returns: ScrubReport

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| segments | `Vec<SegmentScrubResult>` | `Array<SegmentScrubResult>` | `list[SegmentScrubResult]` | Per-segment results. |
| total_components_checked | `u64` | `number` | `int` | Total components examined. |
| total_components_ok | `u64` | `number` | `int` | Components that passed all checks. |
| total_components_failed | `u64` | `number` | `int` | Components with at least one finding. |
| total_bytes_digested | `u64` | `number` | `int` | Total payload bytes hashed during the scrub. |
| duration_ms | `u64` | `number` | `int` | Wall-clock time of the scrub in milliseconds. |

#### SegmentScrubResult

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| segment_id | `u64` | `number` | `int` | Segment that was checked. |
| findings | `Vec<ComponentScrubFinding>` | `Array<ComponentScrubFinding>` | `list[ComponentScrubFinding]` | Problems found (empty if healthy). |
| components_ok | `u64` | `number` | `int` | Components that passed in this segment. |
| bytes_digested | `u64` | `number` | `int` | Payload bytes hashed in this segment. |

#### ComponentScrubFinding

| Field | Rust | Node.js | Python | Description |
|-------|------|---------|--------|-------------|
| component_kind | `String` | `string` | `str` | Which component type had the problem (e.g. `"NodeRecords"`, `"PlannerStats"`). |
| finding_type | `ScrubFindingType` | `string` | `str` | Classification: `PayloadDigestMismatch`, `ComponentIdMismatch`, `DependencyDigestMismatch`, `IdentityHeaderMismatch`, `ContainerIdMismatch`, `SegmentIdentityMismatch`, `RangeOverflow`, `RangeOverlap`, `FileMissing`, or `IoError`. |
| detail | `String` | `string` | `str` | Human-readable description of the finding. |

#### Behavior

- Scrub is **read-only** — it never modifies data on disk.
- Scrub is **offline** — it is never called automatically during open, query, flush, or compaction. You must call it explicitly.
- Segments are checked in parallel using a shared thread pool. The work is I/O-bound (streaming 64KB-buffered reads + SHA-256), not CPU-bound.
- A healthy database returns `total_components_failed == 0` and an empty `findings` array for every segment.
- If a segment directory is missing (e.g. deleted concurrently), the scrub reports a `FileMissing` finding rather than panicking.

---

## Introspection

These methods provide quick diagnostic information. They are **approximate** and counts may slightly overcount when the same ID appears in multiple memtables or segments.

### node_count

```rust
let count = db.node_count()?; // usize
```

Approximate count of live nodes across all data sources.

### edge_count

```rust
let count = db.edge_count()?; // usize
```

Approximate count of live edges.

### next_node_id

```rust
let next = db.next_node_id()?; // u64
```

Rust-only diagnostic: the next auto-assigned node ID that would be used by a new node write.

### next_edge_id

```rust
let next = db.next_edge_id()?; // u64
```

Rust-only diagnostic: the next auto-assigned edge ID that would be used by a new edge write.

### segment_count

```rust
let count = db.segment_count()?; // usize
```

Number of on-disk segments. After compaction, this is typically 0 or 1.

### segment_tombstone_node_count

```rust
let count = db.segment_tombstone_node_count()?; // usize
```

Rust-only diagnostic: deleted node records currently retained in immutable segments.

### segment_tombstone_edge_count

```rust
let count = db.segment_tombstone_edge_count()?; // usize
```

Rust-only diagnostic: deleted edge records currently retained in immutable segments.

### path

```rust
let path = db.path();
```

Returns the database directory path as `&Path`.

### manifest

Rust diagnostic API for reading the current raw manifest state.

```rust
let manifest = db.manifest()?;
println!("label token schema: {}", manifest.label_token_schema_version);
```

#### Returns: ManifestState

`ManifestState` is a raw diagnostic object. Ordinary graph APIs accept public label names, not these internal numeric IDs.

| Field | Rust | Description |
|-------|------|-------------|
| label_token_schema_version | `u32` | Node-label / edge-label catalog schema marker. |
| node_label_tokens | `BTreeMap<String, u32>` | Public node label to internal `label_id`. |
| edge_label_tokens | `BTreeMap<String, u32>` | Public edge label to internal `label_id`. |
| secondary_indexes | `Vec<SecondaryIndexManifestEntry>` | Raw optional secondary-index declarations. Node targets use `SecondaryIndexTarget::Node { label_id }`; edge targets use `SecondaryIndexTarget::Edge { label_id }`; ordered fields are stored separately on each entry. |
| schema_catalog_version | `u32` | Raw schema catalog marker for diagnostics. |
| node_schemas | `Vec<NodeSchemaManifestEntry>` | Raw persisted node-schema entries keyed by internal label IDs. Use schema APIs for ordinary reads/writes. |
| edge_schemas | `Vec<EdgeSchemaManifestEntry>` | Raw persisted edge-schema entries keyed by internal label IDs. Use schema APIs for ordinary reads/writes. |
| segments | `Vec<SegmentInfo>` | Published segment metadata. |

Manifest schema fields are diagnostic/introspection fields, not a user-editable persistence
contract. Use [`set_node_schema`](#schema-management), [`set_edge_schema`](#schema-management), and
the matching get/list/drop APIs to manage schemas.

### manifest::load_manifest_readonly (Rust only)

Diagnostic read-only manifest loader that inspects the manifest priority chain without writing to disk.

```rust
let manifest = overgraph::manifest::load_manifest_readonly(Path::new("./my-graph"))?;
```

Returns `Result<Option<ManifestState>, EngineError>`. The returned manifest is a raw diagnostic view and may contain internal numeric token IDs.

---

## Binary Batch Ingestion

High-performance connector-only binary format for batch upserts. Avoids JSON parsing overhead. Useful when ingesting data from a custom pipeline. Rust callers use the structured `batch_upsert_nodes` and `batch_upsert_edges` APIs directly.

### batch_upsert_nodes_binary

```javascript
const buf = Buffer.alloc(/* ... */);
// Format: "OGNB", version 2, count, then per-node labels/key/props payloads.
const ids = db.batchUpsertNodesBinary(buf);
```

```python
buf = b'...'  # same binary format
ids = db.batch_upsert_nodes_binary(buf)
```

#### Binary Format (little-endian)

```
┌──────────────────────────────────────┐
│ magic: "OGNB"                        │
│ version: u16 = 2                     │
│ count: u32                           │  ← number of nodes in this batch
├──────────────────────────────────────┤
│ For each node:                       │
│   label_count: u8                    │
│   repeated label_count times:        │
│     label_len: u16                   │
│     label: [u8; label_len] (UTF-8)   │
│   weight:    f32                     │
│   key_len:   u16                     │
│   key:       [u8; key_len]  (UTF-8)  │
│   props_len: u32                     │
│   props:     [u8; props_len] (JSON)  │
└──────────────────────────────────────┘
```

#### Returns

Array of node IDs (same order as packed nodes).

Version 1 node buffers are rejected. Use version 2 for every connector so each packed node carries its full label set.

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
│ magic: "OGEB"                            │
│ version: u16 = 1                         │
│ count: u32                               │
├──────────────────────────────────────────┤
│ For each edge:                           │
│   from:       u64                        │
│   to:         u64                        │
│   label_len:  u16                       │
│   label:      [u8; label_len] (UTF-8)   │
│   weight:     f32                        │
│   valid_from: i64                        │
│   valid_to:   i64                        │
│   props_len:  u32                        │
│   props:      [u8; props_len]  (JSON)    │
└──────────────────────────────────────────┘
```

In the packed binary edge format only, `valid_from = 0` and `valid_to = 0` are sentinels for the engine defaults. `valid_from = 0` means "use the edge's `created_at` timestamp"; `valid_to = 0` means "use `i64::MAX` / no expiration." Because of these sentinels, epoch `0` cannot be represented as an explicit edge validity bound in this packed format.

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
| `DatabaseClosed` | Operation attempted after the engine was closed. |
| `InvalidOperation(String)` | Invalid API usage (e.g., writing to a closed database). |
| `TxnConflict(String)` | Explicit write transaction conflict. No WAL entry was appended and the transaction did not commit. |
| `TxnClosed` | Explicit write transaction was already committed or rolled back. |
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

**Available async methods:** `closeAsync`, `ensureNodeLabelAsync`, `ensureEdgeLabelAsync`, `getNodeLabelIdAsync`, `getEdgeLabelIdAsync`, `getNodeLabelAsync`, `getEdgeLabelAsync`, `listNodeLabelsAsync`, `listEdgeLabelsAsync`, `setNodeSchemaAsync`, `checkNodeSchemaAsync`, `dropNodeSchemaAsync`, `getNodeSchemaAsync`, `listNodeSchemasAsync`, `setEdgeSchemaAsync`, `checkEdgeSchemaAsync`, `dropEdgeSchemaAsync`, `getEdgeSchemaAsync`, `listEdgeSchemasAsync`, `upsertNodeAsync`, `upsertEdgeAsync`, `addNodeLabelAsync`, `removeNodeLabelAsync`, `batchUpsertNodesAsync`, `batchUpsertEdgesAsync`, `batchUpsertNodesBinaryAsync`, `batchUpsertEdgesBinaryAsync`, `getNodeAsync`, `getEdgeAsync`, `getNodeByKeyAsync`, `getEdgeByTripleAsync`, `getNodesAsync`, `getNodesByKeysAsync`, `getEdgesAsync`, `deleteNodeAsync`, `deleteEdgeAsync`, `invalidateEdgeAsync`, `graphPatchAsync`, `beginWriteTxnAsync`, `neighborsAsync`, `neighborsPagedAsync`, `neighborsBatchAsync`, `traverseAsync`, `topKNeighborsAsync`, `extractSubgraphAsync`, `shortestPathAsync`, `allShortestPathsAsync`, `isConnectedAsync`, `degreeAsync`, `degreesAsync`, `sumEdgeWeightsAsync`, `avgEdgeWeightAsync`, `findNodesAsync`, `findNodesPagedAsync`, `ensureNodePropertyIndexAsync`, `dropNodePropertyIndexAsync`, `listNodePropertyIndexesAsync`, `ensureEdgePropertyIndexAsync`, `dropEdgePropertyIndexAsync`, `listEdgePropertyIndexesAsync`, `findNodesRangeAsync`, `findNodesRangePagedAsync`, `findNodesByTimeRangeAsync`, `findNodesByTimeRangePagedAsync`, `nodesByLabelsAsync`, `edgesByLabelAsync`, `getNodesByLabelsAsync`, `getEdgesByLabelAsync`, `countNodesByLabelsAsync`, `countEdgesByLabelAsync`, `nodesByLabelsPagedAsync`, `edgesByLabelPagedAsync`, `getNodesByLabelsPagedAsync`, `getEdgesByLabelPagedAsync`, `queryNodeIdsAsync`, `queryNodesAsync`, `queryEdgeIdsAsync`, `queryEdgesAsync`, `queryGraphRowsAsync`, `queryGraphPipelineAsync`, `explainNodeQueryAsync`, `explainEdgeQueryAsync`, `explainGraphRowsAsync`, `explainGraphPipelineAsync`, `executeGqlAsync`, `explainGqlAsync`, `personalizedPagerankAsync`, `connectedComponentsAsync`, `componentOfAsync`, `vectorSearchAsync`, `exportAdjacencyAsync`, `pruneAsync`, `setPrunePolicyAsync`, `removePrunePolicyAsync`, `listPrunePoliciesAsync`, `syncAsync`, `flushAsync`, `compactAsync`, `compactWithProgressAsync`, `ingestModeAsync`, `endIngestAsync`.

`WriteTxn` handles expose async counterparts for the full transaction surface: `upsertNodeAsync`, `upsertNodeAsAsync`, `upsertEdgeAsync`, `upsertEdgeAsAsync`, `deleteNodeAsync`, `deleteEdgeAsync`, `invalidateEdgeAsync`, `stageAsync`, `getNodeAsync`, `getEdgeAsync`, `getNodeByKeyAsync`, `getEdgeByTripleAsync`, `commitAsync`, and `rollbackAsync`. Async transaction operations on one handle execute in call order.

### Python

The `AsyncOverGraph` class wraps every `OverGraph` method with `asyncio.to_thread()`. `begin_write_txn()` returns an `AsyncWriteTxn` whose methods mirror `WriteTxn`:

```python
from overgraph import AsyncOverGraph

async def main():
    async with await AsyncOverGraph.open("./my-graph") as db:
        # Also accepts multiple labels: ["User", "Admin"]
        node_id = await db.upsert_node("User", "alice")
        node = await db.get_node(node_id)
        neighbors = await db.neighbors(node_id)

asyncio.run(main())
```

**All methods have identical signatures and semantics** to the sync `OverGraph` class but return coroutines.
`AsyncWriteTxn` also serializes operations on each transaction handle so staged writes, reads, `commit()`, and `rollback()` run in await/call order.

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
| | `get_node_by_key` | Get node by label + key |
| | `add_node_label` | Add a node label to an existing node |
| | `remove_node_label` | Remove a node label from an existing node |
| | `delete_node` | Delete node (cascade edges) |
| | `batch_upsert_nodes` | Batch create/update nodes |
| | `get_nodes` | Batch get nodes by ID |
| | `get_nodes_by_keys` | Batch get nodes by label + key |
| **Edges** | `upsert_edge` | Create or update edge |
| | `get_edge` | Get edge by ID |
| | `get_edge_by_triple` | Get edge by from + to + edge label |
| | `delete_edge` | Delete edge |
| | `invalidate_edge` | Close validity window |
| | `batch_upsert_edges` | Batch create/update edges |
| | `get_edges` | Batch get edges by ID |
| **Atomic** | `graph_patch` | Multi-op atomic batch |
| | `begin_write_txn` / `beginWriteTxn` | Explicit ordered write transaction |
| **Catalog** | `ensure_node_label` / `ensureNodeLabel` | Ensure node label token |
| | `ensure_edge_label` / `ensureEdgeLabel` | Ensure edge label token |
| | `get_node_label_id` / `getNodeLabelId` | Diagnostic name-to-ID lookup |
| | `get_edge_label_id` / `getEdgeLabelId` | Diagnostic name-to-ID lookup |
| | `get_node_label` / `getNodeLabel` | Diagnostic ID-to-name lookup |
| | `get_edge_label` / `getEdgeLabel` | Diagnostic ID-to-name lookup |
| | `list_node_labels` / `listNodeLabels` | List node-label catalog entries |
| | `list_edge_labels` / `listEdgeLabels` | List edge-label catalog entries |
| **Schemas** | `set_node_schema` / `setNodeSchema` | Publish label-scoped node constraints |
| | `check_node_schema` / `checkNodeSchema` | Dry-run node schema validation |
| | `drop_node_schema` / `dropNodeSchema` | Remove a node schema |
| | `get_node_schema` / `getNodeSchema` | Inspect one node schema |
| | `list_node_schemas` / `listNodeSchemas` | List node schemas |
| | `set_edge_schema` / `setEdgeSchema` | Publish edge-label-scoped constraints |
| | `check_edge_schema` / `checkEdgeSchema` | Dry-run edge schema validation |
| | `drop_edge_schema` / `dropEdgeSchema` | Remove an edge schema |
| | `get_edge_schema` / `getEdgeSchema` | Inspect one edge schema |
| | `list_edge_schemas` / `listEdgeSchemas` | List edge schemas |
| | `set_graph_schema` / `setGraphSchema` | Atomically replace graph schema catalog |
| | `alter_graph_schema` / `alterGraphSchema` | Atomically alter selected schema targets |
| | `check_graph_schema_set` / `checkGraphSchemaSet` | Dry-run full graph schema replacement |
| | `check_graph_schema_add` / `checkGraphSchemaAdd` | Dry-run additive graph schema publish |
| | `drop_graph_schema` / `dropGraphSchema` | Remove all graph schemas |
| **Label and Edge-Label Queries** | `nodes_by_labels` | Node ID convenience query |
| | `edges_by_label` | All edge IDs of an edge label |
| | `get_nodes_by_labels` | Hydrated node convenience query |
| | `get_edges_by_label` | All edge records of an edge label |
| | `count_nodes_by_labels` | Node count convenience query |
| | `count_edges_by_label` | Count edges of an edge label |
| **Property Indexes** | `ensure_node_property_index` | Declare optional node equality or range index |
| | `drop_node_property_index` | Remove optional node property index declaration |
| | `list_node_property_indexes` | Inspect node declaration state |
| | `ensure_edge_property_index` | Declare optional edge equality or range index |
| | `drop_edge_property_index` | Remove optional edge property index declaration |
| | `list_edge_property_indexes` | Inspect edge declaration state |
| **Property & Time Queries** | `find_nodes` | Property search |
| | `find_nodes_range` | Numeric property range search |
| | `find_nodes_by_time_range` | Time range search |
| **Queries** | `query_node_ids` | Node query returning IDs |
| | `query_nodes` | Node query returning hydrated nodes |
| | `explain_node_query` | Explain a node query plan |
| | `query_edge_ids` | Edge query returning IDs |
| | `query_edges` | Edge query returning hydrated edges |
| | `explain_edge_query` | Explain an edge query plan |
| | `query_graph_rows` | Structured graph-row query |
| | `explain_graph_rows` | Explain a graph-row query |
| | `execute_gql` | GQL query-string reads and mutations |
| | `explain_gql` | Explain a GQL statement |
| **Pagination** | `*_paged` | Paginated variants |
| **Traversal** | `neighbors` | Immediate neighbors |
| | `neighbors_paged` | Paginated neighbors |
| | `neighbors_batch` | Multi-node neighbors |
| | `top_k_neighbors` | Top K by score |
| | `traverse` | BFS traversal |
| | `extract_subgraph` | Subgraph extraction |
| | `shortest_path` | Shortest path |
| | `all_shortest_paths` | All shortest paths |
| | `is_connected` | Reachability check |
| **Degree & Weight** | `degree` | Edge count |
| | `degrees` | Batch edge counts |
| | `sum_edge_weights` | Sum of edge weights |
| | `avg_edge_weight` | Average edge weight |
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
| | `scrub` | Validate database integrity |
| **Introspection** | `path` | Database directory path |
| | `manifest` | Rust raw manifest diagnostics |
| | `next_node_id` | Rust next node ID diagnostic |
| | `next_edge_id` | Rust next edge ID diagnostic |
| | `segment_tombstone_node_count` | Rust segment node tombstone diagnostic |
| | `segment_tombstone_edge_count` | Rust segment edge tombstone diagnostic |
| | `manifest::load_manifest_readonly` | Rust read-only manifest diagnostic |
| **Binary** | `batch_upsert_nodes_binary` | Connector-only binary batch nodes |
| | `batch_upsert_edges_binary` | Connector-only binary batch edges |
