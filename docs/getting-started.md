# Getting Started with OverGraph

This guide gets you up and running with OverGraph in Python, Node.js, or Rust. You'll open a database, create some nodes and edges, query neighbors, and run a vector search.

For full parameter documentation, see the [API Reference](api-reference.md).

## Install

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

## Open a database

A database is a directory on disk. Pass a vector dimension if you want to use dense vector search.

**Python**
```python
from overgraph import OverGraph

db = OverGraph.open("./my-graph", dense_vector_dimension=384)
```

**Node.js**
```javascript
import { OverGraph } from 'overgraph';

const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 384 },
});
```

**Rust**
```rust
use overgraph::*;
use std::path::Path;

let opts = DbOptions {
    dense_vector: Some(DenseVectorConfig {
        dimension: 384,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    }),
    ..Default::default()
};
let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;
```

## Define type IDs

OverGraph uses integers to classify nodes and edges. Define them as constants:

**Python**
```python
USER = 1
PROJECT = 2
WORKS_ON = 10
```

**Node.js**
```javascript
const USER = 1;
const PROJECT = 2;
const WORKS_ON = 10;
```

**Rust**
```rust
const USER: u32 = 1;
const PROJECT: u32 = 2;
const WORKS_ON: u32 = 10;
```

## Create nodes and edges

**Python**
```python
alice = db.upsert_node(USER, "alice", props={"role": "engineer"})
bob = db.upsert_node(USER, "bob")
project = db.upsert_node(PROJECT, "atlas",
    dense_vector=[0.2] * 384,
    sparse_vector=[(42, 0.5), (150, 0.9)])

db.upsert_edge(alice, project, WORKS_ON)
db.upsert_edge(bob, project, WORKS_ON, weight=0.5)
```

**Node.js**
```javascript
const alice = db.upsertNode(USER, 'alice', { props: { role: 'engineer' } });
const bob = db.upsertNode(USER, 'bob');
const project = db.upsertNode(PROJECT, 'atlas', {
  denseVector: new Array(384).fill(0.2),
  sparseVector: [{ dimension: 42, value: 0.5 }, { dimension: 150, value: 0.9 }],
});

db.upsertEdge(alice, project, WORKS_ON);
db.upsertEdge(bob, project, WORKS_ON, { weight: 0.5 });
```

**Rust**
```rust
let alice = db.upsert_node(USER, "alice", UpsertNodeOptions {
    props: BTreeMap::from([("role".into(), PropValue::String("engineer".into()))]),
    ..Default::default()
})?;
let bob = db.upsert_node(USER, "bob", UpsertNodeOptions::default())?;
let project = db.upsert_node(PROJECT, "atlas", UpsertNodeOptions {
    dense_vector: Some(vec![0.2; 384]),
    sparse_vector: Some(vec![(42, 0.5), (150, 0.9)]),
    ..Default::default()
})?;

db.upsert_edge(alice, project, WORKS_ON, UpsertEdgeOptions::default())?;
db.upsert_edge(bob, project, WORKS_ON, UpsertEdgeOptions { weight: 0.5, ..Default::default() })?;
```

Upserting the same `(type_id, key)` pair updates the existing node instead of creating a duplicate.

## Read data back

**Python**
```python
node = db.get_node(alice)
node = db.get_node_by_key(USER, "alice")
nodes = db.get_nodes([alice, bob])       # batch read
```

**Node.js**
```javascript
const node = db.getNode(alice);
const node2 = db.getNodeByKey(USER, 'alice');
const nodes = db.getNodes([alice, bob]);
```

**Rust**
```rust
let node = db.get_node(alice)?;
let node = db.get_node_by_key(USER, "alice")?;
let nodes = db.get_nodes(&[alice, bob])?;
```

## Query neighbors

**Python**
```python
neighbors = db.neighbors(alice, direction="outgoing")
for n in neighbors:
    print(n.node_id, n.weight)
```

**Node.js**
```javascript
const list = db.neighbors(alice, { direction: 'outgoing' });
for (let i = 0; i < list.length; i++) {
  console.log(list.nodeId(i), list.weight(i));
}
```

**Rust**
```rust
let neighbors = db.neighbors(alice, &NeighborOptions::default())?;
for n in &neighbors {
    println!("{} {}", n.node_id, n.weight);
}
```

## Vector search

**Python**
```python
hits = db.vector_search("hybrid", k=10,
    dense_query=[0.15] * 384,
    sparse_query=[(42, 0.9), (99, 0.5)],
    scope_start_node_id=alice,
    scope_max_depth=3)

for hit in hits:
    print(hit.node_id, hit.score)
```

**Node.js**
```javascript
const hits = db.vectorSearch('hybrid', {
  k: 10,
  denseQuery: new Array(384).fill(0.15),
  sparseQuery: [{ dimension: 42, value: 0.9 }, { dimension: 99, value: 0.5 }],
  scope: { startNodeId: alice, maxDepth: 3 },
});

hits.forEach(h => console.log(h.nodeId, h.score));
```

**Rust**
```rust
let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Hybrid,
    dense_query: Some(vec![0.15; 384]),
    sparse_query: Some(vec![(42, 0.9), (99, 0.5)]),
    k: 10,
    scope: Some(VectorSearchScope {
        start_node_id: alice,
        max_depth: 3,
        direction: Direction::Outgoing,
        edge_type_filter: None,
        at_epoch: None,
    }),
    ..Default::default()
})?;

for hit in &hits {
    println!("{} {:.4}", hit.node_id, hit.score);
}
```

## Close

**Python**
```python
db.close()

# Or use a context manager:
with OverGraph.open("./my-graph") as db:
    db.upsert_node(USER, "alice")
```

**Node.js**
```javascript
db.close();
```

**Rust**
```rust
db.close()?;
```

## Async

**Python** - use `AsyncOverGraph`:
```python
from overgraph import AsyncOverGraph

async with await AsyncOverGraph.open("./my-graph") as db:
    alice = await db.upsert_node(USER, "alice")
    neighbors = await db.neighbors(alice)
```

**Node.js** - append `Async` to any method:
```javascript
const node = await db.getNodeAsync(alice);
const hits = await db.vectorSearchAsync('hybrid', { k: 10, denseQuery: query });
```

## Next steps

- [API Reference](api-reference.md) - every method, parameter, type, and return value across all three languages
- [Architecture Overview](architecture-overview.md) - how the storage engine works under the hood
