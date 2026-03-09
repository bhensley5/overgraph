# Getting Started with OverGraph

This guide walks you through the core features of OverGraph: creating a database, storing nodes and edges, querying neighbors, working with temporal edges, pagination, and retention policies. Code examples are shown in Rust, Node.js, and Python side by side.

## Opening a database

A database is just a directory. If it doesn't exist, OverGraph creates it. If it does exist, OverGraph opens it and replays the WAL to recover any in-flight state.

**Rust**
```rust
use overgraph::{DatabaseEngine, DbOptions};
use std::path::Path;

let mut db = DatabaseEngine::open(Path::new("./my-graph"), &DbOptions::default())?;
```

**Node.js**
```javascript
import { OverGraph } from 'overgraph';

const db = await OverGraph.openAsync('./my-graph', {
  walSyncMode: 'group_commit',
});
```

**Python**
```python
from overgraph import OverGraph

db = OverGraph.open("./my-graph", wal_sync_mode="group_commit")
```

The `wal_sync_mode` controls durability. `GroupCommit` (default) batches fsync calls for better throughput. `Immediate` fsyncs every single write for maximum crash safety.

## Creating nodes

Nodes have a `type_id` (a u32 you define), a `key` (a string for upsert dedup), an optional property map, and an optional `weight`.

**Rust**
```rust
use std::collections::BTreeMap;
use overgraph::PropValue;

let mut props = BTreeMap::new();
props.insert("name".to_string(), PropValue::String("Alice".to_string()));
props.insert("role".to_string(), PropValue::String("engineer".to_string()));

let alice_id = db.upsert_node(1, "user:alice", props, 1.0)?;
```

**Node.js**
```javascript
const aliceId = await db.upsertNodeAsync(1, 'user:alice', {
  name: 'Alice',
  role: 'engineer',
}, 1.0);
```

**Python**
```python
alice_id = db.upsert_node(1, "user:alice",
    props={"name": "Alice", "role": "engineer"}, weight=1.0)
```

If you upsert a node with the same `(type_id, key)` pair again, it updates the existing node instead of creating a duplicate.

## Creating edges

Edges connect two nodes. They also have a `type_id`, optional properties, and an optional `weight`.

**Rust**
```rust
let edge_id = db.upsert_edge(
    alice_id, project_id, 10,
    Default::default(),
    1.0,
    None, None  // valid_from, valid_to
)?;
```

**Node.js**
```javascript
const edgeId = await db.upsertEdgeAsync(aliceId, projectId, 10, {
  role: 'creator',
}, 1.0);
```

**Python**
```python
edge_id = db.upsert_edge(alice_id, project_id, 10,
    props={"role": "creator"}, weight=1.0)
```

## Batch operations

For bulk loading, batch APIs amortize the WAL and memtable overhead. This is significantly faster than individual upserts in a loop.

**Rust**
```rust
use overgraph::NodeInput;

let node_ids = db.batch_upsert_nodes(vec![
    NodeInput { type_id: 1, key: "user:alice".into(), props: props_alice, weight: 1.0 },
    NodeInput { type_id: 1, key: "user:bob".into(), props: props_bob, weight: 0.8 },
    NodeInput { type_id: 2, key: "project:atlas".into(), props: Default::default(), weight: 0.9 },
])?;
```

**Node.js**
```javascript
const nodeIds = await db.batchUpsertNodesAsync([
  { typeId: 1, key: 'user:alice', props: { name: 'Alice' }, weight: 1.0 },
  { typeId: 1, key: 'user:bob', props: { name: 'Bob' }, weight: 0.8 },
  { typeId: 2, key: 'project:atlas', props: {}, weight: 0.9 },
]);
```

**Python**
```python
node_ids = db.batch_upsert_nodes([
    {"type_id": 1, "key": "user:alice", "props": {"name": "Alice"}, "weight": 1.0},
    {"type_id": 1, "key": "user:bob", "props": {"name": "Bob"}, "weight": 0.8},
    {"type_id": 2, "key": "project:atlas", "props": {}, "weight": 0.9},
])
```

## Reading nodes and edges

**Rust**
```rust
// By ID
let node = db.get_node(alice_id)?; // Option<NodeRecord>

// By key
let node = db.get_node_by_key(1, "user:alice")?; // Option<NodeRecord>

// Bulk read (sorted merge-walk, not per-item lookups)
let nodes = db.get_nodes(&[id1, id2, id3])?; // Vec<Option<NodeRecord>>
```

**Node.js**
```javascript
const node = await db.getNodeAsync(aliceId);
const nodeByKey = await db.getNodeByKeyAsync(1, 'user:alice');
const nodes = await db.getNodesAsync([id1, id2, id3]);
```

**Python**
```python
node = db.get_node(alice_id)
node_by_key = db.get_node_by_key(1, "user:alice")
nodes = db.get_nodes([id1, id2, id3])
```

## Querying neighbors

The `neighbors` function does a 1-hop traversal from a node. You can filter by edge type, limit results, and control direction.

**Rust**
```rust
use overgraph::Direction;

// All outgoing neighbors of alice, any edge type, limit 50
let results = db.neighbors(alice_id, Direction::Outgoing, None, 50, None, None)?;

for n in &results {
    println!("neighbor node={} via edge={} weight={}", n.node_id, n.edge_id, n.weight);
}
```

**Node.js**
```javascript
const results = await db.neighborsAsync(aliceId, 'outgoing', null, 50);
for (let i = 0; i < results.length; i++) {
  console.log(`neighbor node=${results.nodeId(i)} weight=${results.weight(i)}`);
}
```

**Python**
```python
results = db.neighbors(alice_id, "outgoing", limit=50)
for n in results:
    print(f"neighbor node={n.node_id} weight={n.weight}")
```

### Traverse Exactly 2 Hops

```python
# Find nodes exactly 2 hops from alice
page = db.traverse(
    alice_id,
    min_depth=2,
    max_depth=2,
    direction="outgoing",
    limit=100,
)
results = page.items
```

### Constrained Traversal

This is the "find all X related to Y through Z" pattern. Traverse specific edge types, then emit only matching node types.

```python
# Find all projects (type 2) connected to alice through "works_on" edges (type 10)
page = db.traverse(
    alice_id,
    min_depth=2,
    max_depth=2,
    direction="outgoing",
    edge_type_filter=[10],
    node_type_filter=[2],
    limit=50,
)
results = page.items
```

### Top-K neighbors

Get the K highest-scoring neighbors. Scoring can be by `weight`, `recency`, or decay-adjusted.

```python
top = db.top_k_neighbors(alice_id, k=10, direction="outgoing", scoring="weight")
```

## Degree counts and weight aggregations

Sometimes you just need the count or total weight without materializing the full neighbor list. These methods are O(postings) with no allocation beyond edge dedup.

**Rust**
```rust
// Outgoing degree
let count = db.degree(alice_id, Direction::Outgoing, None, None)?;

// Sum of outgoing edge weights
let total = db.sum_edge_weights(alice_id, Direction::Outgoing, None, None)?;

// Average outgoing edge weight (None if zero edges)
let avg = db.avg_edge_weight(alice_id, Direction::Outgoing, None, None)?;

// Batch: degrees for many nodes at once (sorted cursor walk)
let degrees = db.degrees(&[alice_id, bob_id], Direction::Outgoing, None, None)?;
// degrees: HashMap<u64, u64>
```

**Node.js**
```javascript
const count = db.degree(aliceId);
const total = db.sumEdgeWeights(aliceId);
const avg = db.avgEdgeWeight(aliceId);

// With options
const filtered = db.degree(aliceId, 'outgoing', [10, 20]);
const atTime = db.degree(aliceId, 'outgoing', null, 1706745600000);

// Batch
const results = db.degrees([aliceId, bobId]);
// results: [{ nodeId, degree }, ...]
```

**Python**
```python
count = db.degree(alice_id)
total = db.sum_edge_weights(alice_id)
avg = db.avg_edge_weight(alice_id)  # None if no edges

# With options
filtered = db.degree(alice_id, "outgoing", type_filter=[10, 20])
at_time = db.degree(alice_id, "outgoing", at_epoch=1706745600000)

# Batch
degrees = db.degrees([alice_id, bob_id])
# degrees: {node_id: count, ...}
```

All degree/weight methods support the same `direction`, `type_filter`, and `at_epoch` parameters as `neighbors()`. Temporal filtering works the same way: expired and future edges are excluded.

## Shortest path and connectivity

Find the shortest path between two nodes, check reachability, or enumerate all shortest paths when there are ties.

**Rust**
```rust
use overgraph::Direction;

// BFS shortest path (unweighted, counts hops)
let path = db.shortest_path(alice_id, project_id, Direction::Outgoing,
    None, None, None, None, None)?;
if let Some(p) = path {
    println!("Path: {:?}, cost: {}", p.nodes, p.total_cost);
}

// Dijkstra shortest path (weighted by edge weight)
let path = db.shortest_path(alice_id, project_id, Direction::Outgoing,
    None, Some("weight"), None, None, None)?;

// Fast reachability check (no path tracking, just true/false)
let connected = db.is_connected(alice_id, project_id,
    Direction::Outgoing, None, None, None)?;

// All shortest paths (when there are ties)
let paths = db.all_shortest_paths(alice_id, project_id,
    Direction::Outgoing, None, None, None, None, None, Some(10))?;
println!("Found {} equal-cost paths", paths.len());
```

**Node.js**
```javascript
// BFS shortest path
const path = db.shortestPath(aliceId, projectId, 'outgoing');
if (path) {
  console.log(`Path: ${path.nodes}, cost: ${path.totalCost}`);
}

// Dijkstra shortest path
const weighted = db.shortestPath(aliceId, projectId, 'outgoing', null, 'weight');

// Reachability check
const connected = db.isConnected(aliceId, projectId, 'outgoing');

// All shortest paths
const paths = db.allShortestPaths(aliceId, projectId, 'outgoing',
    null, null, null, null, null, 10);
```

**Python**
```python
# BFS shortest path
path = db.shortest_path(alice_id, project_id, "outgoing")
if path:
    print(f"Path: {path.nodes}, cost: {path.total_cost}")

# Dijkstra shortest path
weighted = db.shortest_path(alice_id, project_id, "outgoing", weight_field="weight")

# Reachability check
connected = db.is_connected(alice_id, project_id, "outgoing")

# All shortest paths
paths = db.all_shortest_paths(alice_id, project_id, "outgoing", max_paths=10)
print(f"Found {len(paths)} equal-cost paths")
```

Algorithm selection is automatic: pass `weight_field=None` (default) for BFS, `"weight"` to use edge weights from the adjacency index, or any other field name to read weights from edge properties. Both BFS and Dijkstra use bidirectional search for optimal performance. Use `max_depth` to limit hop count and `max_cost` to cap total path cost (Dijkstra only).

## Connected components

Find weakly connected components or look up which component a specific node belongs to. WCC treats all edges as undirected. Edge direction is ignored for component membership.

```rust
// All components: returns NodeIdMap<u64> (node_id → component_id).
// NodeIdMap is a HashMap with fast identity hashing for numeric IDs.
// Component ID = minimum node ID in the component (deterministic).
let comps = db.connected_components(None, None, None)?;

// Look up a single node's component (targeted BFS, no full-graph scan)
let members = db.component_of(some_node_id, None, None, None)?;
// members: sorted Vec<u64> of all nodes in the same component
```

```javascript
// All components: [{nodeId, componentId}, ...] sorted by nodeId
const comps = db.connectedComponents();

// Single node's component: Float64Array of member node IDs
const members = db.componentOf(someNodeId);

// Async variants
const compsAsync = await db.connectedComponentsAsync();
const membersAsync = await db.componentOfAsync(someNodeId);
```

```python
# All components: {node_id: component_id}
comps = db.connected_components()

# Single node's component: sorted list of node IDs
members = db.component_of(some_node_id)

# Filter by edge/node types
comps = db.connected_components(edge_type_filter=[10], node_type_filter=[1])
members = db.component_of(node_id, edge_type_filter=[10])
```

Isolated nodes (no edges) become singleton components. Deleted, tombstoned, and prune-policy-hidden nodes are excluded. Use `at_epoch` for temporal edge visibility. Strongly connected components (SCC) for directed graphs are planned for a future release.

## Temporal edges

Edges can have validity windows. This is useful for modeling things that are true for a limited time, like "Alice works at Acme from January to March."

**Creating a temporal edge:**

```python
db.upsert_edge(alice_id, acme_id, 20,
    props={"title": "contractor"},
    weight=1.0,
    valid_from=1704067200000,  # Jan 1, 2024
    valid_to=1709251200000,    # Mar 1, 2024
)
```

**Querying at a point in time:**

```python
# What was true on February 1st?
neighbors = db.neighbors(alice_id, "outgoing",
    at_epoch=1706745600000  # Feb 1, 2024
)
# The edge to acme_id is included

# What about April 1st?
neighbors = db.neighbors(alice_id, "outgoing",
    at_epoch=1711929600000  # Apr 1, 2024
)
# The edge to acme_id is NOT included (expired)
```

**Invalidating an edge:**

```python
# Mark an edge as no longer valid right now
db.invalidate_edge(edge_id, valid_to=int(time.time() * 1000))
```

### Decay scoring

Pass `decay_lambda` to make recent edges score higher than old ones. The weight is scaled by `exp(-lambda * age_hours)`.

```python
# Neighbors with exponential decay (lambda=0.01)
neighbors = db.neighbors(alice_id, "outgoing", decay_lambda=0.01)
# Older edges have lower effective weights
```

## Finding nodes

**By type and property:**

```python
# Find all nodes of type 1 where "role" == "engineer"
ids = db.find_nodes(1, "role", "engineer")
```

**By type:**

```python
ids = db.nodes_by_type(1)  # All type-1 node IDs
nodes = db.get_nodes_by_type(1)  # Full node records
count = db.count_nodes_by_type(1)  # Just the count
```

**By time range:**

```python
# Nodes of type 1 updated in the last hour
import time
now = int(time.time() * 1000)
ids = db.find_nodes_by_time_range(1, now - 3_600_000, now)
```

## Pagination

Every collection-returning API supports keyset pagination. Pass `limit` to control page size and `after` to continue from where you left off. The cursor is the ID of the last item you saw.

```python
# First page
page = db.nodes_by_type_paged(1, limit=100)
process(page.items)

# Next page
while page.next_cursor is not None:
    page = db.nodes_by_type_paged(1, limit=100, after=page.next_cursor)
    process(page.items)
```

This works on ID-keyed paged query types like `find_nodes_paged`, `get_nodes_by_type_paged`, and `neighbors_paged`. `traverse()` also paginates, but it uses a traversal-specific cursor instead of raw ID pagination.

## Retention policies

### Manual pruning

```python
result = db.prune(max_age_ms=86_400_000, type_id=1)
print(f"Pruned {result.nodes_pruned} nodes and {result.edges_pruned} edges")
```

### Automatic pruning with named policies

Register a policy and it takes effect immediately. Matching nodes become invisible to reads right away (lazy expiration). During the next compaction, they're physically deleted.

```python
# Conversations older than 24 hours get cleaned up
db.set_prune_policy("short_term_memory",
    max_age_ms=86_400_000, type_id=3)

# Important entities are kept longer
db.set_prune_policy("entity_retention",
    max_age_ms=2_592_000_000, type_id=1)  # 30 days

# Check what policies are active
for policy in db.list_prune_policies():
    print(f"{policy.name}: {policy}")

# Remove a policy
db.remove_prune_policy("short_term_memory")
```

## Graph analytics

### Personalized PageRank

Run PPR from seed nodes to find the most contextually relevant nodes in your graph. Useful for building context windows in RAG pipelines.

```python
result = db.personalized_pagerank(
    seed_node_ids=[alice_id],
    max_iterations=100,
    max_results=20,
)
for node_id, score in zip(result.node_ids, result.scores):
    print(f"node {node_id}: relevance {score:.4f}")
```

### Graph export

Export the adjacency structure for external analysis (community detection, visualization, etc.).

```python
adj = db.export_adjacency(
    node_type_filter=[1, 2],
    edge_type_filter=[10],
    include_weights=True,
)
# adj.sources, adj.targets, adj.weights, adj.node_ids
```

## Maintenance

```python
db.sync()     # Force WAL fsync (only needed in GroupCommit mode)
db.flush()    # Flush memtable to a new segment
db.compact()  # Merge segments, apply tombstones and prune policies

# Compact with progress reporting
def on_progress(progress):
    print(f"{progress.phase}: {progress.records_processed}/{progress.total_records}")
    return True  # return False to cancel

db.compact_with_progress(on_progress)
```

## Database stats

```python
stats = db.stats()
print(f"Segments: {stats.segment_count}")
print(f"Pending WAL bytes: {stats.pending_wal_bytes}")
print(f"Tombstones: {stats.tombstone_node_count} nodes, {stats.tombstone_edge_count} edges")
```

## Closing the database

**Normal close** flushes the memtable and waits for any in-progress background compaction to finish:

```python
db.close()
```

**Fast close** cancels any running background compaction and returns immediately. Data integrity is preserved; partial compaction output is cleaned up on the next open.

```python
db.close(force=True)
```

Python and async Python both support context managers (`with` / `async with`) for automatic cleanup.
