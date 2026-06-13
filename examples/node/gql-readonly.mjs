/**
 * OverGraph Example: GQL
 *
 * Run: node examples/node/gql-readonly.mjs
 * (Requires: npm run build in overgraph-node/ first)
 */

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { OverGraph } from '../../overgraph-node/index.js';

const dbPath = mkdtempSync(join(tmpdir(), 'overgraph-gql-node-'));
let db;

try {
  db = OverGraph.open(dbPath, {
    walSyncMode: 'immediate',
    denseVector: { dimension: 3 },
  });

  const ada = db.upsertNode('Person', 'ada', {
    props: { name: 'Ada', status: 'active', rank: 2 },
    denseVector: [0.1, 0.2, 0.3],
  });
  const ben = db.upsertNode('Person', 'ben', {
    props: { name: 'Ben', status: 'active', rank: 1 },
  });
  const acme = db.upsertNode('Company', 'acme', {
    props: { name: 'Acme' },
  });
  db.upsertEdge(ada, acme, 'WORKS_AT', {
    props: { role: 'engineer', since: 2020 },
  });
  db.upsertEdge(ben, acme, 'WORKS_AT', {
    props: { role: 'designer', since: 2022 },
  });

  const result = db.executeGql(
    `MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
     WHERE p.status = $status
     RETURN p.name AS person, r.role AS role, c.name AS company
     ORDER BY p.rank ASC
     LIMIT 10`,
    { status: 'active' },
    { includePlan: true, profile: true }
  );

  console.log(result.rows);
  console.log(result.stats);
  console.log(result.plan.rowOps);

  const compact = await db.executeGqlAsync(
    'MATCH (n:Person) RETURN n.name AS name, n.rank AS rank ORDER BY n.rank',
    null,
    { compactRows: true }
  );
  console.log(compact.columns, compact.rows);

  const withVectors = db.executeGql(
    "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n",
    null,
    { includeVectors: true }
  );
  console.log(withVectors.rows[0].n.denseVector);
} finally {
  if (db) db.close();
  rmSync(dbPath, { recursive: true, force: true });
}
