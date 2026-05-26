# GQL Beta

OverGraph supports **GQL Beta**, a read-only GQL/Cypher-style subset that lowers into the same
graph-row engine used by the structured native APIs. It is not full ISO GQL and it is not full
Cypher. Use it when a `MATCH` query string is clearer than building a request object.

The authoritative public reference is the [GQL Beta section in the API docs](api-reference.md#gql-beta).
This page is the compact deep dive for syntax, options, result shapes, path values, cursors, and
examples.

Connector calls are thin bindings over the Rust implementation:

- Rust: `DatabaseEngine::execute_gql(...)` and `DatabaseEngine::explain_gql(...)`
- Node.js: `executeGql(...)`, `executeGqlAsync(...)`, `explainGql(...)`, `explainGqlAsync(...)`
- Python: `execute_gql(...)`, async `execute_gql(...)`, `explain_gql(...)`, async `explain_gql(...)`

Connectors do not reimplement parsing, lowering, planning, execution, cursor handling, optional
semantics, or path conversion.

## Supported Syntax

Supported clause shape:

```gql
MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
OPTIONAL MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
RETURN <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

`WHERE`, `OPTIONAL MATCH`, `ORDER BY`, `SKIP`/`OFFSET`, and `LIMIT` are optional. Each required or
optional match clause can have its own `WHERE`. `OPTIONAL MATCH` clauses follow an initial required
`MATCH`. `SKIP` and `OFFSET` are synonyms; specifying both is rejected. `LIMIT 0` validates the
query and returns an empty result.

Supported pattern shapes:

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

Supported expressions include:

- Variables bound by the pattern
- `id(n)`, `id(r)`, `labels(n)`, `type(r)`
- Path functions: `length(p)`, `start_node(p)`, `end_node(p)`, `nodes(p)`, `relationships(p)`, `node_ids(p)`, `edge_ids(p)`
- Path fields: `p.node_ids`, `p.edge_ids`, `p.length`
- Property access such as `n.name`, `n.rank`, `r.since`
- Literals: `null`, booleans, integers, floats, strings, lists, and maps
- Parameters such as `$name`, `$ids`, `$payload`
- Boolean predicates with `AND`, `OR`, `NOT`
- Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`
- Null checks: `IS NULL`, `IS NOT NULL`
- Membership: `IN`

`ORDER BY` accepts graph-row order atoms: null, bool, finite numbers, strings, bytes, nodes, edges,
and paths. Lists, maps, and non-finite floats are rejected.

## Options

Rust uses `GqlQueryOptions`. Node and Python expose connector-native option names:

| Rust | Node.js | Python | Default |
|---|---|---|---|
| `allow_full_scan` | `allowFullScan` | `allow_full_scan` | `false` |
| `max_rows` | `maxRows` | `max_rows` | `10000` |
| `cursor` | `cursor` | `cursor` | `None` / `null` |
| `max_cursor_bytes` | `maxCursorBytes` | `max_cursor_bytes` | `16384` |
| `max_intermediate_bindings` | `maxIntermediateBindings` | `max_intermediate_bindings` | `65536` |
| `max_skip` | `maxSkip` | `max_skip` | `100000` |
| `max_query_bytes` | `maxQueryBytes` | `max_query_bytes` | `1048576` |
| `max_param_bytes` | `maxParamBytes` | `max_param_bytes` | `1048576` |
| `max_ast_depth` | `maxAstDepth` | `max_ast_depth` | `256` |
| `max_literal_items` | `maxLiteralItems` | `max_literal_items` | `10000` |
| `include_plan` | `includePlan` | `include_plan` | `false` |
| `profile` | `profile` | `profile` | `false` |
| `include_vectors` | `includeVectors` | `include_vectors` | `false` |
| `compact_rows` | `compactRows` | `compact_rows` | `false` |

`compact_rows` exists in Rust options, but Rust rows are already positional. In Node.js and Python,
`compactRows` / `compact_rows` switches connector row serialization from objects to arrays. It does
not change parsing, lowering, planning, execution, selected fields, vector policy, stats, caps, or
plan truth.

`includeVectors` / `include_vectors` defaults to `false`. Returning node values, including nodes
inside path values, omits dense and sparse vectors unless vector inclusion is requested.

## Results

Rust returns positional rows:

```rust
GqlResult {
    columns: Vec<String>,
    rows: Vec<GqlRow>,
    next_cursor: Option<String>,
    stats: GqlExecutionStats,
    plan: Option<GqlExplain>,
}
```

Node and Python return object rows by default. Node.js uses camelCase result fields:

```js
{
  columns: ['name', 'rank'],
  rows: [{ name: 'Ada', rank: 2 }],
  nextCursor: null,
  stats: { rowsReturned: 1, rowsMatched: 3, rowsAfterFilter: 1, ... },
  plan: null
}
```

Python uses snake_case result fields:

```python
{
    "columns": ["name", "rank"],
    "rows": [{"name": "Ada", "rank": 2}],
    "next_cursor": None,
    "stats": {"rows_returned": 1, "rows_matched": 3, "rows_after_filter": 1, ...},
    "plan": None,
}
```

With compact rows enabled, connectors return row arrays:

```js
{
  columns: ['name', 'rank'],
  rows: [['Ada', 2]],
  nextCursor: null,
  stats: { rowsReturned: 1, rowsMatched: 3, rowsAfterFilter: 1, ... },
  plan: null
}
```

Node uses camelCase fields. Python uses snake_case fields. Node byte values return as `Buffer`;
Python byte values return as `bytes`.

## Path Values

Returning a path alias yields a path value:

| Shape | Node.js | Python | Rust |
|---|---|---|---|
| Node IDs | `nodeIds` | `node_ids` | `node_ids` |
| Edge IDs | `edgeIds` | `edge_ids` | `edge_ids` |
| Hydrated nodes | `nodes` | `nodes` | `nodes` |
| Hydrated edges | `edges` | `edges` | `edges` |

`node_ids(p)` and `edge_ids(p)` return ID lists. `nodes(p)` and `relationships(p)` return lists of
node or edge IDs. `start_node(p)` and `end_node(p)` return node IDs. Returning `p` directly returns
the path value shape above; path functions are ID-oriented in the current GQL output.

## Cursors

When a result has another page, it includes `next_cursor` / `nextCursor`. Pass that value as the
next call's `cursor` option with the same logical query and referenced params:

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

Supported parameter values:

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

`explain_gql` / `explainGql` returns:

- `columns`
- `target`, currently `graph_row_query`
- `native_plan` / `nativePlan`, currently null for graph-row lowering
- `pushed_down`
- `residual`
- `projection`, including graph-row plan, row-op, order, cursor, cap, and note summaries
- `row_ops`
- `caps`
- `warnings`

When `includePlan` / `include_plan` is true on `execute_gql`, the result includes the same explain
payload in `plan`. When `profile` is true, `stats.elapsedUs` / `stats.elapsed_us` is populated.

Full scans are rejected unless `allowFullScan` / `allow_full_scan` is set. Caps and warnings are
returned through stats and explain structures.

## Examples

Node.js:

```js
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
```

Python:

```python
rows = db.execute_gql(
    """
    MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
    WHERE p.status = $status
    OPTIONAL MATCH path = (p)-[:KNOWS*1..2]->(friend:Person)
    RETURN p.name AS person, c.name AS company, path, length(path) AS hops
    ORDER BY p.name, hops
    LIMIT 10
    """,
    {"status": "active"},
    include_plan=True,
)
```

Rust:

```rust
let result = engine.execute_gql(
    "MATCH p = (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN p, node_ids(p) AS ids LIMIT 10",
    &GqlParams::new(),
    &GqlQueryOptions::default(),
)?;
```

## Unsupported

GQL Beta intentionally rejects:

- Full ISO GQL
- Full Cypher compatibility
- Writes and mutations
- Schema operations
- Aggregation
- `DISTINCT`
- `WITH`
- `UNION`
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
