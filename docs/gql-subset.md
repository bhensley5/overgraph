# GQL

OverGraph ships **GQL**, a GQL/Cypher-style query surface for graph reads and writes. Use it
when a query or mutation reads better as text than as a request object.

The authoritative public reference is the [GQL section in the API docs](api-reference.md#gql).
This page is the compact syntax companion.

Connector calls:

- Rust: `DatabaseEngine::execute_gql(...)` and `DatabaseEngine::explain_gql(...)`
- Node.js: `executeGql(...)`, `executeGqlAsync(...)`, `explainGql(...)`, `explainGqlAsync(...)`
- Python: `execute_gql(...)`, async `execute_gql(...)`, `explain_gql(...)`, async `explain_gql(...)`

Supported at a glance:

- `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP` / `OFFSET`, and `LIMIT`
- `WITH`, `WITH *`, `WITH DISTINCT`, later `MATCH` stages, and `RETURN DISTINCT`
- aggregation with `count`, `sum`, `avg`, `min`, `max`, and `collect`
- `UNION`, `UNION ALL`, `EXISTS { ... }`, and read-only `CALL { ... }`
- bounded paths, path values, path helper functions, and constrained shortest paths
- `CREATE`, keyed node `MERGE`, unique relationship `MERGE`, `ON CREATE SET`, `ON MATCH SET`,
  `SET`, `REMOVE`, `DELETE r`, `DETACH DELETE n`, and mutation returns
- graph-schema DDL with `ALTER CURRENT GRAPH TYPE`, `CHECK CURRENT GRAPH TYPE`,
  `DROP CURRENT GRAPH TYPE`, and schema `SHOW`
- params, read cursors, compact rows, explain/profile, ReadOnly mode, mutation stats, and vector
  opt-in for returned node values

## Read Syntax

Read clause shape:

```gql
MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
OPTIONAL MATCH <pattern> [, <pattern>...] [WHERE <predicate>]
WITH [DISTINCT] <items> [WHERE <predicate>] [ORDER BY ...] [SKIP ...] [LIMIT ...]
CALL { <read clauses ending in RETURN> }
RETURN [DISTINCT] <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

`WITH` controls what names are visible to later clauses. `WITH *` keeps visible aliases,
`WITH DISTINCT` deduplicates rows, and row operations on `WITH` apply before the next clause.

```gql
MATCH (p:Person)
WITH p, lower(trim(p.email)) AS email
WHERE email ENDS WITH '@example.com'
MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN DISTINCT p.name AS person, email, c.name AS company
ORDER BY person
LIMIT 20
```

Read branches can be combined with `UNION` or `UNION ALL`:

```gql
MATCH (p:Person) WHERE p.status = 'active'
RETURN p.name AS name
UNION ALL
MATCH (p:Person) WHERE p.status = 'invited'
RETURN p.name AS name
```

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
- Shortest path: `MATCH p = shortestPath((a)-[:KNOWS*1..5]->(b))`
- All equal shortest paths: `MATCH p = allShortestPaths((a)-[:KNOWS*1..5]-(b))`

Shortest path uses pre-bound endpoint aliases:

```gql
MATCH (a:Person {elementKey: $from})
WITH a
MATCH (b:Person {elementKey: $to})
WITH a, b
MATCH p = shortestPath((a)-[:KNOWS*1..4]->(b))
RETURN p, nodeIds(p) AS node_ids, edgeIds(p) AS edge_ids, length(p) AS hops
```

Expressions include variables, metadata functions, path functions, property access, literals,
params, boolean predicates, comparisons, null checks, `IN`, arithmetic, string predicates, `CASE`,
and `RETURN *`.

Scalar functions include `coalesce`, `toString`, `toInteger`, `toFloat`, `abs`, `floor`, `ceil`,
`round`, `lower`, `upper`, `trim`, `substring`, `size`, `head`, and `last`.

## Properties And Metadata

GQL has one rule for telling user data apart from engine bookkeeping:

> **A function call is engine metadata. A dot access or map key is a user property.**

Metadata read functions (usable in `WHERE`, `RETURN`, `ORDER BY`, `WITH`, aggregates, and `CASE`):

| Function | Target | Reads |
|---|---|---|
| `id(n)`, `id(r)` | node, edge | element ID |
| `labels(n)` | node | label list |
| `type(r)` | edge | edge label |
| `elementKey(n)` | node | node key |
| `weight(n)`, `weight(r)` | node, edge | weight |
| `createdAt(n)`, `createdAt(r)` | node, edge | created timestamp |
| `updatedAt(n)`, `updatedAt(r)` | node, edge | updated timestamp |
| `validFrom(r)`, `validTo(r)` | edge | validity bounds |
| `id(startNode(r))`, `id(endNode(r))` | edge | endpoint node IDs |

Function names match case-insensitively; the canonical spelling is camelCase. On an edge alias,
`startNode(r)` / `endNode(r)` are valid only as the direct argument of `id(...)`.

Path functions: `length(p)`, `nodes(p)`, `relationships(p)`, `nodeIds(p)`, `edgeIds(p)`,
`startNode(p)`, `endNode(p)`. Paths have no dot fields.

Dot access is always a plain user-property lookup, and NO property name is reserved. `n.weight`,
`n.key`, `n.updated_at`, and `r.valid_from` read ordinary properties with those names (null when
absent) and never touch metadata; `updatedAt(n)` reads the engine timestamp. `SET`, `REMOVE`, and
`SET += ` maps follow the same rule: `SET n.updated_at = 5` writes a user property named
`updated_at`.

Writable metadata uses function l-values in `SET` (including `ON CREATE SET` / `ON MATCH SET`):

```gql
SET weight(n) = 2.5
SET weight(r) = 0.5
SET validFrom(r) = 10
SET validTo(r) = 20
```

All other metadata functions are read-only `SET` targets, and `REMOVE` never accepts metadata
functions.

Element maps in `CREATE`, `MERGE`, and `MATCH` patterns describe the element itself, so they carry
metadata under the exact camelCase function names:

- Node maps: `elementKey` (required in `CREATE` node maps; the `MERGE` node identity entry),
  optional `weight`.
- Edge maps: optional `weight`, `validFrom`, `validTo` (with `validFrom < validTo`).
- `MATCH` pattern maps filter on the same vocabulary: `MATCH (n:Person {elementKey: 'ada'})`
  matches the node key.
- Every other map key is a user property, with no name restrictions.
- `SET x += {...}` maps are pure user properties; no map key is treated as metadata there.

One wart follows from the element-map rule: a user property literally named `weight`,
`elementKey`, `validFrom`, or `validTo` cannot be set through a `CREATE`/`MERGE` element map,
because those map keys mean metadata there. Set it with `SET` after creation:

```gql
CREATE (n:Part {elementKey: 'p1'})
SET n.weight = 'heavy'
```

Aggregation example:

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

Read-only subqueries:

```gql
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(c:Company) RETURN c }
WITH p
CALL { MATCH (p)-[:WORKS_AT]->(c:Company) RETURN c.name AS company }
RETURN p.name AS person, company
```

## Mutation Syntax

Mutation clause shape:

```gql
MATCH <pattern> [WHERE <predicate>]
OPTIONAL MATCH <pattern> [WHERE <predicate>]
WITH [DISTINCT] <items> [WHERE <predicate>] [ORDER BY ...] [SKIP ...] [LIMIT ...]
CALL { <read clauses ending in RETURN> }
CREATE <pattern> [, <pattern>...]
MERGE (n:Label {elementKey: expr}) [ON CREATE SET ...] [ON MATCH SET ...]
MERGE (a)-[r:TYPE]->(b) [ON CREATE SET ...] [ON MATCH SET ...]
SET <alias>.<property> = <expr> | <alias> += <map> | <alias>:<Label> | <metadata-function>(<alias>) = <expr>
REMOVE <target>
DELETE <edge-alias>
DETACH DELETE <node-alias>
RETURN [DISTINCT] <items>
ORDER BY <order-expression> [ASC|DESC], ...
SKIP <integer-or-param>
OFFSET <integer-or-param>
LIMIT <integer-or-param>
```

Read prefixes go before the first write clause. Create-only statements do not need a read prefix.

Mutation forms:

- `CREATE (n:Person {elementKey: 'ada', name: 'Ada'})`
- `CREATE (a:Person {elementKey: 'a'})-[r:KNOWS {since: 2026}]->(b:Person {elementKey: 'b'})`
- `MERGE (n:Person {elementKey: $key}) ON CREATE SET n.created = true ON MATCH SET n.seen = true`
- `MATCH (a:Person {elementKey: $a}) MATCH (b:Person {elementKey: $b}) MERGE (a)-[r:KNOWS]->(b)`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n.status = 'active'`
- `MATCH (n:Person {elementKey: 'ada'})-[r:KNOWS]->(b) SET weight(r) = 0.5`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n += $props`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' SET n:Engineer`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' REMOVE n.status`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' REMOVE n:Engineer`
- `MATCH (a)-[r:KNOWS]->(b) DELETE r`
- `MATCH (n:Person) WHERE elementKey(n) = 'ada' DETACH DELETE n`

`SET` metadata l-values are limited to `weight(n)`, `weight(r)`, `validFrom(r)`, and `validTo(r)`;
every other metadata function is read-only, and `REMOVE` does not accept metadata functions.

Mutation return example:

```gql
MATCH (n:Person) WHERE elementKey(n) = 'ada'
SET n.status = 'active'
RETURN DISTINCT elementKey(n) AS key, n.status AS status
ORDER BY key
LIMIT 1
```

MERGE example:

```gql
MATCH (s:Source)
WITH s.target_key AS key
MERGE (a:Account {elementKey: key})
ON CREATE SET a.status = 'created', a.count = 1
ON MATCH SET a.status = 'matched', a.count = coalesce(a.count, 0) + 1
RETURN DISTINCT elementKey(a) AS key, a.status AS status, a.count AS count
```

## Schema DDL Syntax

GQL manages OverGraph's current graph type, which is the active node/edge schema catalog.
These statements call the same Rust core schema-management APIs as the Rust, Node.js, and Python
bulk graph-schema methods.

Supported forms:

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

`ADD` publishes listed schemas while preserving unlisted targets. `SET` replaces the whole schema
catalog; `SET {}` clears it. Selected `DROP` reports one row for each requested target, including
`not_found` rows. `DROP CURRENT GRAPH TYPE` removes every node and edge schema. `CHECK` validates
the proposed `ADD` or `SET` without publishing. `OPTIONS` applies to `ADD`, `SET`, and `CHECK`;
selected and full-catalog `DROP` statements do not accept options.

Schema maps use snake_case fields:

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

Dry-run, show, and selected drop examples:

```gql
CHECK CURRENT GRAPH TYPE ADD {
  NODE Person = { properties: { name: { required: true, nullable: false, types: ['string'] } } }
} OPTIONS { max_violations: 10 }

SHOW CURRENT GRAPH TYPE
SHOW NODE SCHEMA Person
ALTER CURRENT GRAPH TYPE DROP { NODE ArchivedPerson, EDGE OLD_EDGE }
```

Schema result row columns:

| Statement | Columns |
|---|---|
| `ALTER ... ADD` / `ALTER ... SET` | `operation`, `target_kind`, `label`, `action`, `checked_records`, `violation_count`, `truncated`, `scan_limit_hit` |
| `ALTER ... DROP` | `operation`, `target_kind`, `label`, `action` |
| `DROP CURRENT GRAPH TYPE` | `operation`, `target_kind`, `label`, `action`, `node_schemas_dropped`, `edge_schemas_dropped` |
| `CHECK ... ADD` / `CHECK ... SET` | `operation`, `target_kind`, `label`, `checked_records`, `violation_count`, `truncated`, `scan_limit_hit`, `violations` |
| `SHOW ...` | `target_kind`, `label`, `schema` |

Schema statement results use `kind = "schema"`, no cursor, no mutation stats, and populated
`schemaStats` / `schema_stats`. `SHOW` returns canonical schema maps. Tagged schema literals keep
their exact type, for example `{ type: 'uint', value: '18446744073709551615' }` and
`{ type: 'bytes', value: [0, 1, 255] }`.

`CHECK` and `SHOW` are allowed in ReadOnly mode. `ALTER` and `DROP` are rejected in ReadOnly.
All schema statements reject `cursor`. `SHOW` does not silently truncate: if the catalog would
return more rows than `maxRows` / `max_rows`, the statement errors instead.

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
| `max_pipeline_rows` | `maxPipelineRows` | `max_pipeline_rows` | `65536` |
| `max_groups` | `maxGroups` | `max_groups` | `65536` |
| `max_collect_items` | `maxCollectItems` | `max_collect_items` | `65536` |
| `max_union_branches` | `maxUnionBranches` | `max_union_branches` | `16` |
| `max_subquery_invocations` | `maxSubqueryInvocations` | `max_subquery_invocations` | `4096` |
| `max_subquery_depth` | `maxSubqueryDepth` | `max_subquery_depth` | `2` |
| `max_shortest_path_pairs` | `maxShortestPathPairs` | `max_shortest_path_pairs` | `4096` |
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

For reads, `cursor` carries the next page token. Data mutations and schema statements reject
`cursor`. For schema `SHOW`, `maxRows` / `max_rows` is a hard cap that returns an error when the
catalog row count is larger.

`compactRows` / `compact_rows` switches connector row serialization from objects to arrays.
`includeVectors` / `include_vectors` includes dense and sparse vectors in returned node values.

## Results

Rust returns positional rows. Node.js and Python return object rows by default and positional arrays
when compact rows are enabled.

Node.js result shape:

```js
{
  kind: 'query',
  columns: ['name', 'rank'],
  rows: [{ name: 'Ada', rank: 2 }],
  nextCursor: null,
  stats: { rowsReturned: 1, rowsMatched: 3, rowsAfterFilter: 1, ... },
  mutationStats: null,
  schemaStats: null,
  plan: null
}
```

Python result shape:

```python
{
    "kind": "mutation",
    "columns": ["name"],
    "rows": [{"name": "Ada"}],
    "next_cursor": None,
    "stats": {"rows_returned": 1, "rows_matched": 1, "rows_after_filter": 1, ...},
    "mutation_stats": {"nodes_created": 1, "mutation_ops": 1, ...},
    "schema_stats": None,
    "plan": None,
}
```

For schema statements, `kind` is `"schema"`, `mutationStats` / `mutation_stats` is null, and
`schemaStats` / `schema_stats` contains operation, target, validation, drop, and timing counters.

## Path Values

Returning a path alias yields a path value:

| Shape | Node.js | Python | Rust |
|---|---|---|---|
| Node IDs | `nodeIds` | `node_ids` | `node_ids` |
| Edge IDs | `edgeIds` | `edge_ids` | `edge_ids` |
| Hydrated nodes | `nodes` | `nodes` | `nodes` |
| Hydrated edges | `edges` | `edges` | `edges` |

`length(p)` returns hop count. `nodeIds(p)` and `edgeIds(p)` return ID lists. Returning `p`
directly returns the path value shape above.

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

## Params

Parameter values can be nulls, booleans, signed and unsigned integers where the connector can
represent them, finite floats, strings, bytes, lists, and maps with string keys.

Only referenced params are resource-validated; extra unused params are ignored.

## Explain And Profile

`explain_gql` / `explainGql` returns a unified explain payload with:

- `kind`
- `columns`
- `read` for read-plan details or mutation read-prefix details
- `mutation` for mutation operation and return-plan details
- `schema` for schema DDL/check/show details
- `caps`
- `warnings`
- `notes`

When `includePlan` / `include_plan` is true on `execute_gql`, the result includes the same explain
payload in `plan`. When `profile` is true, `stats.elapsedUs` / `stats.elapsed_us` is populated.

## Examples

Node.js:

```js
db.executeGql(
  `CREATE (p:Person {elementKey: $personKey, name: $personName, status: 'active'})
          -[r:WORKS_AT {role: $role, since: $since}]->
          (c:Company {elementKey: $companyKey, name: $companyName})
   RETURN p.name AS person, c.name AS company, r.role AS role`,
  {
    personKey: 'ada',
    personName: 'Ada',
    companyKey: 'overgraph',
    companyName: 'OverGraph',
    role: 'engineer',
    since: 2026,
  }
);

const rows = db.executeGql(
  `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
   WITH c.name AS company, count(*) AS people, collect(DISTINCT p.name) AS names
   RETURN company, people, names
   ORDER BY people DESC`,
  null,
  { includePlan: true }
);
```

Python:

```python
created = db.execute_gql(
    """
    MERGE (p:Person {elementKey: $key})
    ON CREATE SET p.name = $name, p.status = 'active'
    ON MATCH SET p.seen = true
    RETURN elementKey(p) AS key, p.name AS name
    """,
    {"key": "ada", "name": "Ada"},
)

rows = db.execute_gql(
    """
    MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
    WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(c) RETURN c }
    RETURN p.name AS person, c.name AS company
    ORDER BY p.name
    LIMIT 10
    """,
    include_plan=True,
)
```

Rust:

```rust
let result = engine.execute_gql(
    "MATCH (a:Person {elementKey: 'ada'}) \
     WITH a \
     MATCH (b:Person {elementKey: 'ben'}) \
     WITH a, b \
     MATCH p = shortestPath((a)-[:KNOWS*1..4]->(b)) \
     RETURN p, nodeIds(p) AS ids, length(p) AS hops",
    &GqlParams::new(),
    &GqlExecutionOptions::default(),
)?;
```
