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

const db = OverGraph.open('./my-graph', {
  walSyncMode: 'group-commit',
});
```

**Python**
```python
from overgraph import OverGraph

db = OverGraph.open("./my-graph", wal_sync_mode="group_commit")
```

The `wal_sync_mode` controls durability. `GroupCommit` (default) batches fsync calls for better throughput. `Immediate` fsyncs every single write for maximum crash safety.
Node.js keeps required arguments positional and groups optional fields in a trailing
options object. Python expresses the same optional fields as keyword arguments.

### Opening with vector search enabled

To use dense vector search, configure the vector dimension and distance metric at open time. This is DB-scoped metadata persisted in the manifest. You set it once and it applies to all nodes.

**Rust**
```rust
use overgraph::{DatabaseEngine, DbOptions, DenseVectorConfig, DenseMetric, HnswConfig};

let opts = DbOptions {
    dense_vector: Some(DenseVectorConfig {
        dimension: 384,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(), // m=16, ef_construction=200
    }),
    ..Default::default()
};
let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;
```

**Node.js**
```javascript
const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 384, metric: 'cosine' },
});
```

**Python**
```python
db = OverGraph.open("./my-graph",
    dense_vector_dimension=384,
    dense_vector_metric="cosine")  # "cosine" (default), "euclidean", or "dot_product"
```

Sparse vector search requires no upfront configuration. Sparse vectors are variable-length and don't need a fixed dimension.

## Defining your schema

OverGraph uses integer type IDs to classify nodes and edges. Define them as constants so your code reads clearly:

**Rust**
```rust
const USER: u32 = 1;
const PROJECT: u32 = 2;
const CONVERSATION: u32 = 3;
const WORKS_ON: u32 = 10;
const WORKS_AT: u32 = 20;
```

**Node.js**
```javascript
const USER = 1;
const PROJECT = 2;
const CONVERSATION = 3;
const WORKS_ON = 10;
const WORKS_AT = 20;
```

**Python**
```python
USER = 1
PROJECT = 2
CONVERSATION = 3
WORKS_ON = 10
WORKS_AT = 20
```

The examples below use these constants.

## Creating nodes

Nodes have a `type_id` (a u32 you define), a `key` (a string for upsert dedup), an optional property map, and an optional `weight`. Nodes can also carry optional `dense_vector` and `sparse_vector` fields for vector search.

**Rust**
```rust
use std::collections::BTreeMap;
use overgraph::{PropValue, UpsertNodeOptions};

let mut props = BTreeMap::new();
props.insert("name".to_string(), PropValue::String("Alice".to_string()));
props.insert("role".to_string(), PropValue::String("engineer".to_string()));

let alice_id = db.upsert_node(USER, "user:alice", UpsertNodeOptions {
    props,
    weight: 1.0,
    dense_vector: Some(vec![0.1; 384]),   // optional embedding
    sparse_vector: Some(vec![(42, 0.8), (99, 0.3)]),  // optional sparse vector
    ..Default::default()
})?;
```

**Node.js**
```javascript
const aliceId = db.upsertNode(USER, 'user:alice', {
  props: { name: 'Alice', role: 'engineer' },
  weight: 1.0,
  denseVector: new Array(384).fill(0.1),
  sparseVector: [{ dimension: 42, value: 0.8 }, { dimension: 99, value: 0.3 }],
});
```

**Python**
```python
alice_id = db.upsert_node(USER, "user:alice",
    props={"name": "Alice", "role": "engineer"}, weight=1.0,
    dense_vector=[0.1] * 384,
    sparse_vector=[(42, 0.8), (99, 0.3)])
```

If you upsert a node with the same `(type_id, key)` pair again, it updates the existing node instead of creating a duplicate. Vectors are optional; nodes without vectors work exactly as before with zero overhead.

## Creating edges

Edges connect two nodes. They also have a `type_id`, optional properties, and an optional `weight`.

**Rust**
```rust
use overgraph::UpsertEdgeOptions;

let edge_id = db.upsert_edge(
    alice_id, project_id, WORKS_ON,
    UpsertEdgeOptions::default(),
)?;
```

**Node.js**
```javascript
const edgeId = db.upsertEdge(aliceId, projectId, WORKS_ON, {
  props: { role: 'creator' },
  weight: 1.0,
});
```

**Python**
```python
edge_id = db.upsert_edge(alice_id, project_id, WORKS_ON,
    props={"role": "creator"}, weight=1.0)
```

## Batch operations

For bulk loading, batch APIs amortize the WAL and memtable overhead. This is significantly faster than individual upserts in a loop.

**Rust**
```rust
use overgraph::NodeInput;

let node_ids = db.batch_upsert_nodes(vec![
    NodeInput { type_id: USER, key: "user:alice".into(), props: props_alice, weight: 1.0 },
    NodeInput { type_id: USER, key: "user:bob".into(), props: props_bob, weight: 0.8 },
    NodeInput { type_id: PROJECT, key: "project:atlas".into(), props: Default::default(), weight: 0.9 },
])?;
```

**Node.js**
```javascript
const nodeIds = db.batchUpsertNodes([
  { typeId: USER, key: 'user:alice', props: { name: 'Alice' }, weight: 1.0 },
  { typeId: USER, key: 'user:bob', props: { name: 'Bob' }, weight: 0.8 },
  { typeId: PROJECT, key: 'project:atlas', props: {}, weight: 0.9 },
]);
```

**Python**
```python
node_ids = db.batch_upsert_nodes([
    {"type_id": USER, "key": "user:alice", "props": {"name": "Alice"}, "weight": 1.0},
    {"type_id": USER, "key": "user:bob", "props": {"name": "Bob"}, "weight": 0.8},
    {"type_id": PROJECT, "key": "project:atlas", "props": {}, "weight": 0.9},
])
```

## Atomic graph patch

`graph_patch` lets you upsert nodes, upsert edges, delete nodes, delete edges, and invalidate edges in a single atomic operation. All changes share one WAL entry.

**Rust**
```rust
use overgraph::{GraphPatch, NodeInput, EdgeInput};

let result = db.graph_patch(&GraphPatch {
    upsert_nodes: vec![
        NodeInput { type_id: USER, key: "user:charlie".into(), ..Default::default() },
    ],
    upsert_edges: vec![
        EdgeInput { from: alice_id, to: bob_id, type_id: WORKS_ON, ..Default::default() },
    ],
    delete_node_ids: vec![old_node_id],
    delete_edge_ids: vec![old_edge_id],
    invalidate_edges: vec![(stale_edge_id, now_ms)],
})?;
// result.node_ids, result.edge_ids
```

**Node.js**
```javascript
const result = db.graphPatch({
  upsertNodes: [
    { typeId: USER, key: 'user:charlie' },
  ],
  upsertEdges: [
    { from: aliceId, to: bobId, typeId: WORKS_ON },
  ],
  deleteNodeIds: [oldNodeId],
  deleteEdgeIds: [oldEdgeId],
  invalidateEdges: [{ edgeId: staleEdgeId, validTo: Date.now() }],
});
// result.nodeIds, result.edgeIds
```

**Python**
```python
result = db.graph_patch({
    "upsert_nodes": [
        {"type_id": USER, "key": "user:charlie"},
    ],
    "upsert_edges": [
        {"from": alice_id, "to": bob_id, "type_id": WORKS_ON},
    ],
    "delete_node_ids": [old_node_id],
    "delete_edge_ids": [old_edge_id],
    "invalidate_edges": [{"edge_id": stale_edge_id, "valid_to": now_ms}],
})
# result.node_ids, result.edge_ids
```

## Reading nodes and edges

**Rust**
```rust
// By ID
let node = db.get_node(alice_id)?; // Option<NodeRecord>

// By key
let node = db.get_node_by_key(USER, "user:alice")?; // Option<NodeRecord>

// Bulk read by ID (sorted merge-walk, not per-item lookups)
let nodes = db.get_nodes(&[id1, id2, id3])?; // Vec<Option<NodeRecord>>

// Bulk read by key (same merge-walk optimization)
let nodes = db.get_nodes_by_keys(&[(USER, "user:alice"), (USER, "user:bob")])?; // Vec<Option<NodeRecord>>
```

**Node.js**
```javascript
const node = db.getNode(aliceId);
const nodeByKey = db.getNodeByKey(USER, 'user:alice');
const nodes = db.getNodes([id1, id2, id3]);
const nodesByKey = db.getNodesByKeys([
  { typeId: USER, key: 'user:alice' },
  { typeId: USER, key: 'user:bob' },
]);
```

**Python**
```python
node = db.get_node(alice_id)
node_by_key = db.get_node_by_key(USER, "user:alice")
nodes = db.get_nodes([id1, id2, id3])
nodes_by_key = db.get_nodes_by_keys([(USER, "user:alice"), (USER, "user:bob")])
```

Edges work the same way. You can read by ID, by `(from, to, type_id)` triple, or in bulk:

**Rust**
```rust
let edge = db.get_edge(edge_id)?; // Option<EdgeRecord>
let edge = db.get_edge_by_triple(alice_id, project_id, WORKS_ON)?;
let edges = db.get_edges(&[e1, e2, e3])?; // Vec<Option<EdgeRecord>>
```

**Node.js**
```javascript
const edge = db.getEdge(edgeId);
const edge = db.getEdgeByTriple(aliceId, projectId, WORKS_ON);
const edges = db.getEdges([e1, e2, e3]);
```

**Python**
```python
edge = db.get_edge(edge_id)
edge = db.get_edge_by_triple(alice_id, project_id, WORKS_ON)
edges = db.get_edges([e1, e2, e3])
```

## Querying neighbors

The `neighbors` function does a 1-hop traversal from a node. You can filter by edge type, limit results, and control direction.

**Rust**
```rust
use overgraph::NeighborOptions;

// All outgoing neighbors of alice, any edge type, limit 50
let results = db.neighbors(alice_id, &NeighborOptions {
    limit: Some(50),
    ..Default::default()
})?;

for n in &results {
    println!("neighbor node={} via edge={} weight={}", n.node_id, n.edge_id, n.weight);
}
```

**Node.js**
```javascript
const results = db.neighbors(aliceId, { direction: 'outgoing', limit: 50 });
for (let i = 0; i < results.length; i++) {
  console.log(`neighbor node=${results.nodeId(i)} weight=${results.weight(i)}`);
}
```

**Python**
```python
results = db.neighbors(alice_id, direction="outgoing", limit=50)
for n in results:
    print(f"neighbor node={n.node_id} weight={n.weight}")
```

### Traverse Exactly 2 Hops

```python
# Find nodes exactly 2 hops from alice
page = db.traverse(
    alice_id,
    2,  # max_depth (positional)
    min_depth=2,
    direction="outgoing",
    limit=100,
)
results = page.items
```

### Constrained Traversal

This is the "find all X related to Y through Z" pattern. Traverse specific edge types, then emit only matching node types.

```python
# Find all projects connected to alice through WORKS_ON edges
page = db.traverse(
    alice_id,
    2,  # max_depth (positional)
    min_depth=2,
    direction="outgoing",
    edge_type_filter=[WORKS_ON],
    node_type_filter=[PROJECT],
    limit=50,
)
results = page.items
```

### Top-K neighbors

Get the K highest-scoring neighbors. Scoring can be by `weight`, `recency`, or decay-adjusted.

```python
top = db.top_k_neighbors(alice_id, k=10, direction="outgoing", scoring="weight")
```

### Batch neighbor lookups

When you need neighbors for many nodes at once, `neighbors_batch` does a single sorted cursor walk instead of repeated individual lookups.

**Rust**
```rust
use overgraph::NeighborOptions;

let results = db.neighbors_batch(
    &[alice_id, bob_id, charlie_id],
    &NeighborOptions { direction: Direction::Outgoing, ..Default::default() },
)?;
// results: NodeIdMap<Vec<NeighborEntry>> (HashMap keyed by node_id)
for (node_id, neighbors) in &results {
    println!("node {} has {} neighbors", node_id, neighbors.len());
}
```

**Node.js**
```javascript
const results = db.neighborsBatch([aliceId, bobId, charlieId], {
  direction: 'outgoing',
});
// results: [{ queryNodeId, neighbors: JsNeighborList }, ...]
for (const entry of results) {
  console.log(`node ${entry.queryNodeId} has ${entry.neighbors.length} neighbors`);
}
```

**Python**
```python
results = db.neighbors_batch([alice_id, bob_id, charlie_id], direction="outgoing")
# results: {node_id: [NeighborEntry, ...], ...}
for node_id, neighbors in results.items():
    print(f"node {node_id} has {len(neighbors)} neighbors")
```

## Degree counts and weight aggregations

Sometimes you just need the count or total weight without materializing the full neighbor list. These methods are O(postings) with no allocation beyond edge dedup.

**Rust**
```rust
use overgraph::DegreeOptions;

// Outgoing degree
let count = db.degree(alice_id, &DegreeOptions::default())?;

// Sum of outgoing edge weights
let total = db.sum_edge_weights(alice_id, &DegreeOptions::default())?;

// Average outgoing edge weight (None if zero edges)
let avg = db.avg_edge_weight(alice_id, &DegreeOptions::default())?;

// Batch: degrees for many nodes at once (sorted cursor walk)
let degrees = db.degrees(&[alice_id, bob_id], &DegreeOptions::default())?;
// degrees: HashMap<u64, u64>
```

**Node.js**
```javascript
const count = db.degree(aliceId);
const total = db.sumEdgeWeights(aliceId);
const avg = db.avgEdgeWeight(aliceId);

// With options
const filtered = db.degree(aliceId, { direction: 'outgoing', typeFilter: [WORKS_ON, WORKS_AT] });
const atTime = db.degree(aliceId, { direction: 'outgoing', atEpoch: 1706745600000 });

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
filtered = db.degree(alice_id, direction="outgoing", type_filter=[WORKS_ON, WORKS_AT])
at_time = db.degree(alice_id, direction="outgoing", at_epoch=1706745600000)

# Batch
degrees = db.degrees([alice_id, bob_id])
# degrees: {node_id: count, ...}
```

All degree/weight methods support the same `direction`, `type_filter`, and `at_epoch` parameters as `neighbors()`. Temporal filtering works the same way: expired and future edges are excluded.

## Shortest path and connectivity

Find the shortest path between two nodes, check reachability, or enumerate all shortest paths when there are ties.

**Rust**
```rust
use overgraph::{ShortestPathOptions, IsConnectedOptions, AllShortestPathsOptions};

// BFS shortest path (unweighted, counts hops)
let path = db.shortest_path(alice_id, project_id,
    &ShortestPathOptions::default())?;
if let Some(p) = path {
    println!("Path: {:?}, cost: {}", p.nodes, p.total_cost);
}

// Dijkstra shortest path (weighted by edge weight)
let path = db.shortest_path(alice_id, project_id,
    &ShortestPathOptions {
        weight_field: Some("weight".to_string()),
        ..Default::default()
    })?;

// Fast reachability check (no path tracking, just true/false)
let connected = db.is_connected(alice_id, project_id,
    &IsConnectedOptions::default())?;

// All shortest paths (when there are ties)
let paths = db.all_shortest_paths(alice_id, project_id,
    &AllShortestPathsOptions {
        max_paths: Some(10),
        ..Default::default()
    })?;
println!("Found {} equal-cost paths", paths.len());
```

**Node.js**
```javascript
// BFS shortest path
const path = db.shortestPath(aliceId, projectId, { direction: 'outgoing' });
if (path) {
  console.log(`Path: ${path.nodes}, cost: ${path.totalCost}`);
}

// Dijkstra shortest path
const weighted = db.shortestPath(aliceId, projectId, { direction: 'outgoing', weightField: 'weight' });

// Reachability check
const connected = db.isConnected(aliceId, projectId, { direction: 'outgoing' });

// All shortest paths
const paths = db.allShortestPaths(aliceId, projectId, { direction: 'outgoing', maxPaths: 10 });
```

**Python**
```python
# BFS shortest path
path = db.shortest_path(alice_id, project_id)
if path:
    print(f"Path: {path.nodes}, cost: {path.total_cost}")

# Dijkstra shortest path
weighted = db.shortest_path(alice_id, project_id, direction="outgoing", weight_field="weight")

# Reachability check
connected = db.is_connected(alice_id, project_id)

# All shortest paths
paths = db.all_shortest_paths(alice_id, project_id, direction="outgoing", max_paths=10)
print(f"Found {len(paths)} equal-cost paths")
```

Algorithm selection is automatic: pass `weight_field=None` (default) for BFS, `"weight"` to use edge weights from the adjacency index, or any other field name to read weights from edge properties. Both BFS and Dijkstra use bidirectional search for optimal performance. Use `max_depth` to limit hop count and `max_cost` to cap total path cost (Dijkstra only).

## Connected components

Find weakly connected components or look up which component a specific node belongs to. WCC treats all edges as undirected. Edge direction is ignored for component membership.

```rust
use overgraph::ComponentOptions;

// All components: returns NodeIdMap<u64> (node_id → component_id).
// NodeIdMap is a HashMap with fast identity hashing for numeric IDs.
// Component ID = minimum node ID in the component (deterministic).
let comps = db.connected_components(&ComponentOptions::default())?;

// Look up a single node's component (targeted BFS, no full-graph scan)
let members = db.component_of(some_node_id, &ComponentOptions::default())?;
// members: sorted Vec<u64> of all nodes in the same component
```

```javascript
// All components: [{nodeId, componentId}, ...] sorted by nodeId
const comps = db.connectedComponents();

// Single node's component: Float64Array of member node IDs
const members = db.componentOf(someNodeId);
```

```python
# All components: {node_id: component_id}
comps = db.connected_components()

# Single node's component: sorted list of node IDs
members = db.component_of(some_node_id)

# Filter by edge/node types
comps = db.connected_components(edge_type_filter=[WORKS_ON], node_type_filter=[USER])
members = db.component_of(node_id, edge_type_filter=[WORKS_ON])
```

Isolated nodes (no edges) become singleton components. Deleted, tombstoned, and prune-policy-hidden nodes are excluded. Use `at_epoch` for temporal edge visibility. Strongly connected components (SCC) for directed graphs are planned for a future release.

## Temporal edges

Edges can have validity windows. This is useful for modeling things that are true for a limited time, like "Alice works at Acme from January to March."

**Creating a temporal edge:**

```python
db.upsert_edge(alice_id, acme_id, WORKS_AT,
    props={"title": "contractor"},
    weight=1.0,
    valid_from=1704067200000,  # Jan 1, 2024
    valid_to=1709251200000,    # Mar 1, 2024
)
```

**Querying at a point in time:**

```python
# What was true on February 1st?
neighbors = db.neighbors(alice_id, direction="outgoing",
    at_epoch=1706745600000  # Feb 1, 2024
)
# The edge to acme_id is included

# What about April 1st?
neighbors = db.neighbors(alice_id, direction="outgoing",
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
neighbors = db.neighbors(alice_id, direction="outgoing", decay_lambda=0.01)
# Older edges have lower effective weights
```

## Finding nodes

**By type and property:**

```python
# Find all USER nodes where "role" == "engineer"
ids = db.find_nodes(USER, "role", "engineer")
```

**By type:**

```python
ids = db.nodes_by_type(USER)  # All USER node IDs
nodes = db.get_nodes_by_type(USER)  # Full USER node records
count = db.count_nodes_by_type(USER)  # Just the count
```

**By time range:**

```python
# USER nodes updated in the last hour
import time
now = int(time.time() * 1000)
ids = db.find_nodes_by_time_range(USER, now - 3_600_000, now)
```

## Pagination

Every collection-returning API supports keyset pagination. Pass `limit` to control page size and `after` to continue from where you left off. The cursor is the ID of the last item you saw.

```python
# First page
page = db.nodes_by_type_paged(USER, limit=100)
process(page.items)

# Next page
while page.next_cursor is not None:
    page = db.nodes_by_type_paged(USER, limit=100, after=page.next_cursor)
    process(page.items)
```

This works on ID-keyed paged query types like `find_nodes_paged`, `get_nodes_by_type_paged`, and `neighbors_paged`. `traverse()` also paginates, but it uses a traversal-specific cursor instead of raw ID pagination.

## Retention policies

### Manual pruning

```python
result = db.prune(max_age_ms=86_400_000, type_id=USER)
print(f"Pruned {result.nodes_pruned} nodes and {result.edges_pruned} edges")
```

### Automatic pruning with named policies

Register a policy and it takes effect immediately. Matching nodes become invisible to reads right away (lazy expiration). During the next compaction, they're physically deleted.

```python
# Conversations older than 24 hours get cleaned up
db.set_prune_policy("short_term_memory",
    max_age_ms=86_400_000, type_id=CONVERSATION)

# Important entities are kept longer
db.set_prune_policy("entity_retention",
    max_age_ms=2_592_000_000, type_id=USER)  # 30 days

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
    node_type_filter=[USER, PROJECT],
    edge_type_filter=[WORKS_ON],
    include_weights=True,
)
# adj.sources, adj.targets, adj.weights, adj.node_ids
```

## Vector search

OverGraph has built-in dense and sparse vector search. Nodes can carry embeddings, and you search them with `vector_search`. Results are scored hits (`node_id` + `score`), not hydrated records. Use `get_nodes` to hydrate when needed.

### Dense search

Find the K nearest neighbors by embedding similarity. Requires a DB opened with a dense vector config.

**Rust**
```rust
use overgraph::{VectorSearchRequest, VectorSearchMode};

let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Dense,
    dense_query: Some(vec![0.15; 384]),
    sparse_query: None,
    k: 10,
    ef_search: Some(200),          // default: 128
    type_filter: Some(vec![USER]),    // default: None (no filtering)
    scope: None,
    dense_weight: None,            // default: 1.0
    sparse_weight: None,           // default: 1.0
    fusion_mode: None,             // default: WeightedRankFusion
})?;
for hit in &hits {
    println!("node {} score {:.4}", hit.node_id, hit.score);
}
```

**Node.js**
```javascript
const hits = db.vectorSearch('dense', {
  k: 10,
  denseQuery: queryEmbedding,
  efSearch: 200,       // default: 128
  typeFilter: [USER],     // default: undefined (no filtering)
});
// hits: [{ nodeId, score }, ...]
```

**Python**
```python
hits = db.vector_search("dense", k=10,
    dense_query=query_embedding,
    ef_search=200,       # default: 128
    type_filter=[USER])     # default: None (no filtering)
for hit in hits:
    print(f"node {hit.node_id}: {hit.score:.4f}")
```

### Sparse search

Exact dot-product scoring over sparse vectors. Works with pre-computed sparse embeddings from models like SPLADE or BGE-M3. No upfront DB config needed. Sparse vectors are variable-length.

**Rust**
```rust
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Sparse,
    dense_query: None,
    sparse_query: Some(vec![(42, 0.9), (99, 0.5), (150, 0.3)]),
    k: 10,
    type_filter: None,
    ef_search: None,
    scope: None,
    dense_weight: None,
    sparse_weight: None,
    fusion_mode: None,
})?;
```

**Node.js**
```javascript
const hits = db.vectorSearch('sparse', {
  k: 10,
  sparseQuery: [
    { dimension: 42, value: 0.9 },
    { dimension: 99, value: 0.5 },
    { dimension: 150, value: 0.3 },
  ],
});
```

**Python**
```python
hits = db.vector_search("sparse", k=10,
    sparse_query=[(42, 0.9), (99, 0.5), (150, 0.3)])
```

### Hybrid search

Combine dense and sparse results using a fusion mode. Three built-in modes:
- `weighted-rank-fusion` (default): blends rank positions with configurable weights
- `reciprocal-rank-fusion`: combines reciprocal ranks (good when score distributions differ)
- `weighted-score-fusion`: normalizes and blends raw scores

**Rust**
```rust
use overgraph::{FusionMode, VectorSearchScope, Direction};

let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Hybrid,
    dense_query: Some(query_embedding),
    sparse_query: Some(sparse_terms),
    k: 10,                                              // required; 0 returns empty
    type_filter: Some(vec![USER, PROJECT]),                // default: None (no filtering)
    ef_search: Some(200),                                // default: 128
    scope: Some(VectorSearchScope {                       // default: None (search all nodes)
        start_node_id: alice_id,
        max_depth: 3,
        direction: Direction::Outgoing,                  // default: Outgoing
        edge_type_filter: Some(vec![WORKS_ON]),            // default: None (all edge types)
        at_epoch: None,                                  // default: None (current time)
    }),
    dense_weight: Some(0.7),                             // default: 1.0
    sparse_weight: Some(0.3),                            // default: 1.0
    fusion_mode: Some(FusionMode::ReciprocalRankFusion), // default: WeightedRankFusion
})?;
```

**Node.js**
```javascript
const hits = db.vectorSearch('hybrid', {
  denseQuery: queryEmbedding,
  sparseQuery: sparseTerms,
  k: 10,                                  // required; 0 returns empty
  typeFilter: [USER, PROJECT],              // default: undefined (no filtering)
  efSearch: 200,                           // default: 128
  scope: {                                  // default: undefined (search all nodes)
    startNodeId: aliceId,
    maxDepth: 3,
    direction: 'outgoing',                 // default: 'outgoing'
    edgeTypeFilter: [WORKS_ON],            // default: undefined (all edge types)
  },
  denseWeight: 0.7,                        // default: 1.0
  sparseWeight: 0.3,                       // default: 1.0
  fusionMode: 'reciprocal-rank-fusion',    // default: 'weighted-rank-fusion'
});
```

**Python**
```python
hits = db.vector_search("hybrid",
    k=10,                                        # required; 0 returns empty
    dense_query=query_embedding,
    sparse_query=sparse_terms,
    type_filter=[USER, PROJECT],                  # default: None (no filtering)
    ef_search=200,                               # default: 128
    scope_start_node_id=alice_id,                   # default: None (search all nodes)
    scope_max_depth=3,
    scope_direction="outgoing",                  # default: "outgoing"
    scope_edge_type_filter=[WORKS_ON],           # default: None (all edge types)
    scope_at_epoch=None,                         # default: None (current time)
    dense_weight=0.7,                            # default: 1.0
    sparse_weight=0.3,                           # default: 1.0
    fusion_mode="reciprocal-rank-fusion",        # default: "weighted-rank-fusion"
)
```

If you only provide one modality in hybrid mode (e.g. `dense_query` without `sparse_query`), it degenerates cleanly to that modality's ranking.

### Graph-scoped search

Graph scoping is shown above in the hybrid example; it works with any mode. The `scope` parameter restricts results to nodes reachable from a start node via traversal. It uses the same traversal machinery as `traverse()`, so edge-type filtering, direction, and temporal `at_epoch` all work consistently.

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

### Bulk ingest mode

For large imports, you can temporarily disable auto-compaction so writes produce segments as fast as possible, then compact once at the end.

**Rust**
```rust
db.ingest_mode();

// bulk writes here...

db.end_ingest()?; // restores normal auto-compaction and compacts accumulated segments
```

**Node.js**
```javascript
db.ingestMode();

// bulk writes here...

db.endIngest(); // restores normal auto-compaction and compacts accumulated segments
```

**Python**
```python
db.ingest_mode()

# bulk writes here...

db.end_ingest()  # restores normal auto-compaction and compacts accumulated segments
```

Use this when write throughput matters more than read performance during the ingest window. Reads still work, but segment count can grow until `end_ingest()` runs.

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

## Async variants

Every method shown above has an async variant in both Node.js and Python.

**Node.js:** Append `Async` to any method name and `await` the result.

```javascript
// Sync
const node = db.getNode(id);
const hits = db.vectorSearch('dense', { k: 10, denseQuery: embedding });

// Async
const node = await db.getNodeAsync(id);
const hits = await db.vectorSearchAsync('dense', { k: 10, denseQuery: embedding });
```

**Python:** Use the `AsyncOverGraph` class, which mirrors the full `OverGraph` API.

```python
from overgraph import AsyncOverGraph

async with await AsyncOverGraph.open("./my-graph") as db:
    node = await db.get_node(node_id)
    hits = await db.vector_search("dense", k=10, dense_query=embedding)
    neighbors = await db.neighbors(node_id, direction="outgoing")
```
