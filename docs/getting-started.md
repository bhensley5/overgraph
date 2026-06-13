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

db = OverGraph.open("./my-graph", dense_vector_dimension=3)
```

**Node.js**
```javascript
import { OverGraph } from 'overgraph';

const db = OverGraph.open('./my-graph', {
  denseVector: { dimension: 3 },
});
```

**Rust**
```rust
use overgraph::*;
use std::{collections::BTreeMap, path::Path};

let opts = DbOptions {
    dense_vector: Some(DenseVectorConfig {
        dimension: 3,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    }),
    ..Default::default()
};
let mut db = DatabaseEngine::open(Path::new("./my-graph"), &opts)?;
```

## Choose labels and edge labels

OverGraph uses labels to classify nodes and edge labels to classify edges. They are
ordinary strings at the public API boundary.

## Create nodes and edges

**Python**
```python
project_dense = [0.18, 0.71, 0.39]
project_sparse = [(101, 0.6), (407, 0.8)]

# Also accepts multiple labels: ["User", "Engineer"]
alice = db.upsert_node("User", "alice", props={"role": "engineer"})
bob = db.upsert_node("User", "bob")
project = db.upsert_node("Project", "atlas",
    dense_vector=project_dense,
    sparse_vector=project_sparse)

db.upsert_edge(alice, project, "WORKS_ON")
db.upsert_edge(bob, project, "WORKS_ON", weight=0.5)
```

**Node.js**
```javascript
const projectDense = [0.18, 0.71, 0.39];
const projectSparse = [{ dimension: 101, value: 0.6 }, { dimension: 407, value: 0.8 }];

// Also accepts multiple labels: ['User', 'Engineer']
const alice = db.upsertNode('User', 'alice', { props: { role: 'engineer' } });
const bob = db.upsertNode('User', 'bob');
const project = db.upsertNode('Project', 'atlas', {
  denseVector: projectDense,
  sparseVector: projectSparse,
});

db.upsertEdge(alice, project, 'WORKS_ON');
db.upsertEdge(bob, project, 'WORKS_ON', { weight: 0.5 });
```

**Rust**
```rust
let project_dense = vec![0.18_f32, 0.71, 0.39];
let project_sparse = vec![(101, 0.6_f32), (407, 0.8)];

// Also accepts multiple labels: &["User", "Engineer"]
let alice = db.upsert_node("User", "alice", UpsertNodeOptions {
    props: BTreeMap::from([("role".into(), PropValue::String("engineer".into()))]),
    ..Default::default()
})?;
let bob = db.upsert_node("User", "bob", UpsertNodeOptions::default())?;
let project = db.upsert_node("Project", "atlas", UpsertNodeOptions {
    dense_vector: Some(project_dense),
    sparse_vector: Some(project_sparse),
    ..Default::default()
})?;

db.upsert_edge(alice, project, "WORKS_ON", UpsertEdgeOptions::default())?;
db.upsert_edge(bob, project, "WORKS_ON", UpsertEdgeOptions { weight: 0.5, ..Default::default() })?;
```

Upsert APIs accept either a single label string or a label collection. Each live
`(label, key)` membership points at one node; if every supplied label/key membership
resolves to the same node, the upsert updates that node instead of creating a duplicate.

## Read data back

**Python**
```python
node = db.get_node(alice)
node = db.get_node_by_key("User", "alice")
nodes = db.get_nodes([alice, bob])       # batch read
```

**Node.js**
```javascript
const node = db.getNode(alice);
const node2 = db.getNodeByKey('User', 'alice');
const nodes = db.getNodes([alice, bob]);
```

**Rust**
```rust
let node = db.get_node(alice)?;
let node = db.get_node_by_key("User", "alice")?;
let nodes = db.get_nodes(&[alice, bob])?;
```

## Optional: use GQL for query strings

GQL is useful when a graph read or mutation is clearer as a GQL/Cypher-shaped string. This
example creates nodes, creates edges, then queries the graph with aggregation.

**Python**
```python
db.execute_gql(
    """
    CREATE (alice:User {elementKey: 'gql-alice', role: 'engineer'}),
           (bob:User {elementKey: 'gql-bob', role: 'designer'}),
           (project:Project {elementKey: 'gql-atlas', name: 'Atlas'})
    RETURN elementKey(alice) AS alice, elementKey(bob) AS bob, project.name AS project
    """
)

db.execute_gql(
    """
    MATCH (alice:User {elementKey: 'gql-alice'})
    MATCH (bob:User {elementKey: 'gql-bob'})
    MATCH (project:Project {elementKey: 'gql-atlas'})
    CREATE (alice)-[:WORKS_ON {since: 2026}]->(project),
           (bob)-[:WORKS_ON {since: 2026}]->(project)
    RETURN project.name AS project
    """
)

rows = db.execute_gql(
    """
    MATCH (u:User)-[r:WORKS_ON]->(p:Project)
    WHERE elementKey(p) = 'gql-atlas'
    WITH p.name AS project, count(*) AS contributors, collect(elementKey(u)) AS users
    RETURN project, contributors, users
    """
)

print(rows["rows"])
```

**Node.js**
```javascript
db.executeGql(
  `CREATE (alice:User {elementKey: 'gql-alice', role: 'engineer'}),
          (bob:User {elementKey: 'gql-bob', role: 'designer'}),
          (project:Project {elementKey: 'gql-atlas', name: 'Atlas'})
   RETURN elementKey(alice) AS alice, elementKey(bob) AS bob, project.name AS project`
);

db.executeGql(
  `MATCH (alice:User {elementKey: 'gql-alice'})
   MATCH (bob:User {elementKey: 'gql-bob'})
   MATCH (project:Project {elementKey: 'gql-atlas'})
   CREATE (alice)-[:WORKS_ON {since: 2026}]->(project),
          (bob)-[:WORKS_ON {since: 2026}]->(project)
   RETURN project.name AS project`
);

const rows = db.executeGql(
  `MATCH (u:User)-[r:WORKS_ON]->(p:Project)
   WHERE elementKey(p) = 'gql-atlas'
   WITH p.name AS project, count(*) AS contributors, collect(elementKey(u)) AS users
   RETURN project, contributors, users`
);

console.log(rows.rows);
```

**Rust**
```rust
db.execute_gql(
    "CREATE (alice:User {elementKey: 'gql-alice', role: 'engineer'}), \
            (bob:User {elementKey: 'gql-bob', role: 'designer'}), \
            (project:Project {elementKey: 'gql-atlas', name: 'Atlas'}) \
     RETURN elementKey(alice) AS alice, elementKey(bob) AS bob, project.name AS project",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;

db.execute_gql(
    "MATCH (alice:User {elementKey: 'gql-alice'}) \
     MATCH (bob:User {elementKey: 'gql-bob'}) \
     MATCH (project:Project {elementKey: 'gql-atlas'}) \
     CREATE (alice)-[:WORKS_ON {since: 2026}]->(project), \
            (bob)-[:WORKS_ON {since: 2026}]->(project) \
     RETURN project.name AS project",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;

let rows = db.execute_gql(
    "MATCH (u:User)-[r:WORKS_ON]->(p:Project) \
     WHERE elementKey(p) = 'gql-atlas' \
     WITH p.name AS project, count(*) AS contributors, collect(elementKey(u)) AS users \
     RETURN project, contributors, users",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;
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
const neighbors = db.neighbors(alice, { direction: 'outgoing' });
for (const n of neighbors) {
  console.log(n.nodeId, n.weight);
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
query_dense = [0.14, 0.74, 0.36]
query_sparse = [(101, 1.0)]

hits = db.vector_search("hybrid", k=10,
    dense_query=query_dense,
    sparse_query=query_sparse,
    scope_start_node_id=alice,
    scope_max_depth=3)

for hit in hits:
    print(hit.node_id, hit.score)
```

**Node.js**
```javascript
const queryDense = [0.14, 0.74, 0.36];
const querySparse = [{ dimension: 101, value: 1.0 }];

const hits = db.vectorSearch('hybrid', {
  k: 10,
  denseQuery: queryDense,
  sparseQuery: querySparse,
  scope: { startNodeId: alice, maxDepth: 3 },
});

hits.forEach(h => console.log(h.nodeId, h.score));
```

**Rust**
```rust
let query_dense = vec![0.14_f32, 0.74, 0.36];
let query_sparse = vec![(101, 1.0_f32)];

let hits = db.vector_search(&VectorSearchRequest {
    mode: VectorSearchMode::Hybrid,
    dense_query: Some(query_dense),
    sparse_query: Some(query_sparse),
    k: 10,
    label_filter: None,
    ef_search: None,
    scope: Some(VectorSearchScope {
        start_node_id: alice,
        max_depth: 3,
        direction: Direction::Outgoing,
        edge_label_filter: None,
        at_epoch: None,
    }),
    dense_weight: None,
    sparse_weight: None,
    fusion_mode: None,
})?;

for hit in &hits {
    println!("{} {:.4}", hit.node_id, hit.score);
}
```

## Optional: declare property indexes

Property queries work without any extra setup. If a property is hot in your workload, you can declare an optional equality or numeric range index for it. OverGraph will use the declaration-backed path when the index is `Ready`, and otherwise fall back to the same public query API.

Equality indexes use semantic numeric equality for finite scalar numbers, so signed integers, unsigned integers, and finite floats compare by exact numeric value. String equality and other non-numeric equality remain unchanged. Range indexes are domainless numeric indexes over finite scalar numeric values; non-finite floats, non-numeric values, arrays, and maps are excluded.

**Python**
```python
from overgraph import PropertyRangeBound

db.ensure_node_property_index("User", "role", "equality")
db.ensure_node_property_index("Project", "priority", "range")

user_ids = db.find_nodes("User", "role", "engineer")
priority_ids = db.find_nodes_range(
    "Project",
    "priority",
    PropertyRangeBound(1, domain="int"),
    PropertyRangeBound(5.0, domain="float"),
)
```

**Node.js**
```javascript
db.ensureNodePropertyIndex('User', 'role', 'equality');
db.ensureNodePropertyIndex('Project', 'priority', 'range');

const userIds = db.findNodes('User', 'role', 'engineer');
const priorityIds = db.findNodesRange(
  'Project',
  'priority',
  { value: 1, inclusive: true, domain: 'int' },
  { value: 5, inclusive: true, domain: 'float' },
);
```

**Rust**
```rust
db.ensure_node_property_index("User", "role", SecondaryIndexKind::Equality)?;
db.ensure_node_property_index(
    "Project",
    "priority",
    SecondaryIndexKind::Range,
)?;

let user_ids = db.find_nodes("User", "role", &PropValue::String("engineer".into()))?;
let lower = PropertyRangeBound::Included(PropValue::Int(1));
let upper = PropertyRangeBound::Included(PropValue::Float(5.0));
let priority_ids = db.find_nodes_range(
    "Project",
    "priority",
    Some(&lower),
    Some(&upper),
)?;
```

## Close

**Python**
```python
db.close()

# Or use a context manager:
with OverGraph.open("./my-graph") as db:
    db.upsert_node("User", "alice")
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
    alice = await db.upsert_node("User", "alice")
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
