# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

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
- Node lookups: ~200ns
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

[0.1.0]: https://github.com/Bhensley5/overgraph/releases/tag/v0.1.0
