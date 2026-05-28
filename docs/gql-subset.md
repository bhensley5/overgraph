# GQL Beta

OverGraph ships **GQL Beta**, a GQL/Cypher-style query language for reads and writes that lowers
into the same native substrates used by the structured APIs. Use it when a query or mutation reads
better as text than as a request object.

The authoritative public reference is the [GQL Beta section in the API docs](api-reference.md#gql-beta).
This page is the compact syntax companion.

Connector calls are thin bindings over the Rust implementation:

- Rust: `DatabaseEngine::execute_gql(...)` and `DatabaseEngine::explain_gql(...)`
- Node.js: `executeGql(...)`, `executeGqlAsync(...)`, `explainGql(...)`, `explainGqlAsync(...)`
- Python: `execute_gql(...)`, async `execute_gql(...)`, `explain_gql(...)`, async `explain_gql(...)`

Connectors do not reimplement parsing, lowering, planning, execution, cursor handling, optional
semantics, mutation semantics, or path conversion.

## Read Syntax

Read clause shape:

```gql
MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
OPTIONAL MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
RETURN <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

`WHERE`, `OPTIONAL MATCH`, `ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` are optional. `OPTIONAL
MATCH` clauses follow an initial required `MATCH`. `SKIP` and `OFFSET` are synonyms; specifying
both is rejected. Read `LIMIT 0` validates the statement and returns no rows without running
graph-row execution.

Pattern shapes:

- Node pattern: `MATCH (n:Person)`
- Fixed edge pattern: `MATCH (a)-[r:KNOWS]->(b)`
- Multiple fixed hops: `MATCH (a)-[r]->(b)-[s]->(c)`
- Undirected fixed edge: `MATCH (a)-[r:KNOWS]-(b)`
- Optional group: `MATCH (a) OPTIONAL MATCH (a)-[r:REPORTS_TO]->(m)`
- Bounded path: `MATCH p = (a)-[:KNOWS*1..3]->(b)`
- Zero-to-N bounded path: `MATCH p = (a)-[:KNOWS*..2]->(b)`
- Exact-length path: `MATCH p = (a)-[:KNOWS*2]->(b)`
- One-hop path with relationship alias: `MATCH p = (a)-[r:KNOWS*1..1]->(b)`

Variable-length paths require a finite upper bound and are relationship-simple. Multi-hop
relationship-list aliases are unsupported; return the path alias and inspect `edge_ids` instead.

Expressions include variables, `id(n)`, `id(r)`, `labels(n)`, `type(r)`, path functions, path
fields, property access, literals, params, boolean predicates, comparisons, null checks, `IN`, and
`RETURN *`.

`ORDER BY` accepts graph-row order atoms: null, bool, finite numbers, strings, bytes, nodes, edges,
and paths. Lists, maps, and non-finite floats are rejected.

## Mutation Syntax

Mutation shape:

```gql
MATCH <pattern> [WHERE <predicate>]
OPTIONAL MATCH <pattern> [WHERE <predicate>]
CREATE <pattern> [, <pattern>...]
SET <assignment>
REMOVE <target>
DELETE <edge-alias>
DETACH DELETE <node-alias>
RETURN <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

All read clauses must come before the first mutation clause. Create-only statements do not need a
read prefix. For mutation read prefixes, use repeated `MATCH` clauses instead of comma-separated
pattern lists.

Mutation forms:

- `CREATE (n:Person {key: 'ada', name: 'Ada'})`
- `MATCH (a:Person) WHERE a.key = 'a' MATCH (b:Person) WHERE b.key = 'b' CREATE (a)-[r:KNOWS {since: 2026}]->(b)`
- `MATCH (n:Person) WHERE n.key = 'ada' SET n.status = 'active'`
- `MATCH (n:Person) WHERE n.key = 'ada' SET n += $props`
- `MATCH (n:Person) WHERE n.key = 'ada' SET n:Engineer`
- `MATCH (n:Person) WHERE n.key = 'ada' REMOVE n.status`
- `MATCH (n:Person) WHERE n.key = 'ada' REMOVE n:Engineer`
- `MATCH (a)-[r:KNOWS]->(b) DELETE r`
- `MATCH (n:Person) WHERE n.key = 'ada' DETACH DELETE n`

`CREATE` is strict: existing node `(label, key)` memberships conflict, and unique-edge databases
reject duplicate `(from, to, label)` edge creates. `MERGE` and upsert-like GQL syntax are not in
GQL Beta.

`CREATE`, `SET`, and `REMOVE` can return rows:

```gql
MATCH (n:Person) WHERE n.key = 'ada'
SET n.status = 'active'
RETURN n.key AS key, n.status AS status
ORDER BY key
LIMIT 1
```

Mutation `RETURN ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT` affect returned rows only. The mutation
still applies to every input row from the read prefix. `RETURN ... LIMIT 0` still mutates and
returns zero rows.

`DELETE` and `DETACH DELETE` do not support `RETURN` in Phase 33. Mutation statements do not accept
`cursor` and never return `next_cursor` / `nextCursor`.

Known Phase 33 limitation: mutation `RETURN ORDER BY` rejects commit-assigned or same-mutation
volatile metadata such as created IDs/timestamps, created-edge endpoint metadata, and same-mutation
`updated_at`. The future prepared-transaction option is tracked as `QPX-019`.

## Options

Rust uses `GqlExecutionOptions`. Node and Python expose connector-native option names:

| Rust | Node.js | Python | Default |
|---|---|---|---|
| `mode` | `mode` | `mode` | `Auto` / `"auto"` |
| `allow_full_scan` | `allowFullScan` | `allow_full_scan` | `false` |
| `max_rows` | `maxRows` | `max_rows` | `10000` |
| `cursor` | `cursor` | `cursor` | `None` / `null` |
| `max_cursor_bytes` | `maxCursorBytes` | `max_cursor_bytes` | `16384` |
| `max_mutation_rows` | `maxMutationRows` | `max_mutation_rows` | `10000` |
| `max_mutation_ops` | `maxMutationOps` | `max_mutation_ops` | `50000` |
| `max_query_bytes` | `maxQueryBytes` | `max_query_bytes` | `1048576` |
| `max_param_bytes` | `maxParamBytes` | `max_param_bytes` | `1048576` |
| `max_ast_depth` | `maxAstDepth` | `max_ast_depth` | `256` |
| `max_literal_items` | `maxLiteralItems` | `max_literal_items` | `10000` |
| `max_intermediate_bindings` | `maxIntermediateBindings` | `max_intermediate_bindings` | `65536` |
| `max_frontier` | `maxFrontier` | `max_frontier` | `65536` |
| `max_path_hops` | `maxPathHops` | `max_path_hops` | `16` |
| `max_paths_per_start` | `maxPathsPerStart` | `max_paths_per_start` | `4096` |
| `max_order_materialization` | `maxOrderMaterialization` | `max_order_materialization` | `65536` |
| `max_skip` | `maxSkip` | `max_skip` | `100000` |
| `include_plan` | `includePlan` | `include_plan` | `false` |
| `profile` | `profile` | `profile` | `false` |
| `compact_rows` | `compactRows` | `compact_rows` | `false` |
| `include_vectors` | `includeVectors` | `include_vectors` | `false` |

`mode: "readOnly"` / `mode="read_only"` rejects mutation statements before write staging.
`allowFullScan` / `allow_full_scan` is required for legal broad reads and broad mutation read
prefixes.

`compactRows` / `compact_rows` switches connector row serialization from objects to arrays. It does
not change execution. `includeVectors` / `include_vectors` defaults to false; returned node values,
including nodes inside returned path values, omit dense and sparse vectors unless vector inclusion is
requested.

## Results

Rust returns positional rows:

```rust
GqlExecutionResult {
    kind,
    columns,
    rows,
    next_cursor,
    stats,
    mutation_stats,
    plan,
}
```

Node.js uses camelCase result fields:

```js
{
  kind: 'query',
  columns: ['name', 'rank'],
  rows: [{ name: 'Ada', rank: 2 }],
  nextCursor: null,
  stats: { rowsReturned: 1, rowsMatched: 3, rowsAfterFilter: 1, ... },
  mutationStats: null,
  plan: null
}
```

Python uses snake_case result fields:

```python
{
    "kind": "mutation",
    "columns": ["name"],
    "rows": [{"name": "Ada"}],
    "next_cursor": None,
    "stats": {"rows_returned": 1, "rows_matched": 1, "rows_after_filter": 1, ...},
    "mutation_stats": {"nodes_created": 1, "mutation_ops": 1, ...},
    "plan": None,
}
```

Stats include `rows_returned`, `rows_matched`, `rows_after_filter`, `intermediate_bindings`,
`db_hits`, optional `elapsed_us`, and `warnings`. Mutation stats include matched rows, mutation
rows/ops, created/updated/deleted counters, label/property counters, skipped null targets,
duplicate targets, db hits, elapsed time, and warnings.

## Path Values

Returning a path alias yields a path value:

| Shape | Node.js | Python | Rust |
|---|---|---|---|
| Node IDs | `nodeIds` | `node_ids` | `node_ids` |
| Edge IDs | `edgeIds` | `edge_ids` | `edge_ids` |
| Hydrated nodes | `nodes` | `nodes` | `nodes` |
| Hydrated edges | `edges` | `edges` | `edges` |

`node_ids(p)` and `edge_ids(p)` return ID lists. `nodes(p)` and `relationships(p)` return lists of
node or edge values. `start_node(p)` and `end_node(p)` return node IDs. Returning `p` directly
returns the path value shape above.

## Cursors

Read results with another page include `next_cursor` / `nextCursor`. Pass it back as `cursor` with
the same logical read statement and referenced params:

```js
const first = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10'
);

const second = db.executeGql(
  'MATCH (n:Person) RETURN n.name AS name ORDER BY n.name LIMIT 10',
  null,
  { cursor: first.nextCursor }
);
```

Cursors are continuation tokens over final logical result rows. They validate the normalized
graph-row query fingerprint. They are not pinned storage snapshots across pages.

## Params

Parameter values:

- `null` / `None`
- booleans
- signed and unsigned integer values where the connector can represent them
- finite floats
- strings
- bytes: Node `Buffer` or `ArrayBuffer`, Python `bytes`
- lists / arrays
- maps / dictionaries with string keys

Only referenced params are resource-validated; extra unused params are ignored. Referenced list/map
depth and item counts use the configured depth/item caps, and referenced string, bytes, and map-key
payload bytes are capped by `max_param_bytes`.

## Explain And Profile

`explain_gql` / `explainGql` returns a unified `GqlExecutionExplain` with:

- `kind`
- `columns`
- `read` for read-plan details or mutation read-prefix details
- `mutation` for mutation operation and return-plan details
- `caps`
- `warnings`
- `notes`

Mutation explain is side-effect safe. It can parse, bind, lower, and describe a mutation in
`Auto` mode, but it does not open/stage/commit a write transaction, allocate IDs, create label
tokens, append WAL records, publish snapshots, enqueue index work, or mutate memtables. In
`ReadOnly` mode, mutation statements are rejected.

When `includePlan` / `include_plan` is true on `execute_gql`, the result includes the same explain
payload in `plan`. When `profile` is true, `stats.elapsedUs` / `stats.elapsed_us` is populated.

## Examples

Node.js:

```js
const created = db.executeGql(
  `CREATE (p:Person {key: $key, name: $name, status: 'active'})
   RETURN p.name AS name`,
  { key: 'ada', name: 'Ada' }
);

const rows = db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WHERE p.status = $status
   OPTIONAL MATCH path = (p)-[:KNOWS*1..2]->(friend:Person)
   RETURN p.name AS person, c.name AS company, path, length(path) AS hops
   ORDER BY p.name, hops
   LIMIT 10`,
  { status: 'active' },
  { includePlan: true }
);

console.log(created.mutationStats);
console.log(rows.rows);
```

Python:

```python
created = db.execute_gql(
    "CREATE (p:Person {key: $key, name: $name}) RETURN p.name AS name",
    {"key": "ada", "name": "Ada"},
)

rows = db.execute_gql(
    """
    MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
    WHERE p.status = $status
    RETURN p.name AS person, c.name AS company
    ORDER BY p.name
    LIMIT 10
    """,
    {"status": "active"},
    include_plan=True,
)

print(created["mutation_stats"])
print(rows["rows"])
```

Rust:

```rust
let result = engine.execute_gql(
    "MATCH p = (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN p, node_ids(p) AS ids LIMIT 10",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;
```

## Unsupported

GQL Beta rejects:

- Full ISO GQL
- Full Cypher compatibility
- `MERGE`, `ON CREATE`, `ON MATCH`, and upsert-like GQL syntax
- `DELETE n` without `DETACH`
- `RETURN` after `DELETE` or `DETACH DELETE`
- Mutation cursors
- Read-after-write graph matching such as `MATCH CREATE MATCH`
- Vector writes or vector mutation syntax
- Schema operations
- Aggregation
- `DISTINCT`
- `WITH`
- `UNION`
- `CALL`
- Subqueries and procedures
- Dynamic labels and dynamic relationship types
- Unbounded paths
- Shortest path syntax
- Advanced path functions beyond the listed path functions
- Multi-hop relationship-list aliases separate from path aliases
- Path assignment over multiple relationship segments
- Pattern-local predicates inside node or relationship patterns
- Native multi-label edge OR in pure anonymous direct-edge lowering
- List/map and non-finite-float `ORDER BY` domains
- Mutation `RETURN ORDER BY` on commit-assigned or same-mutation-volatile metadata
