import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { OverGraph } from '../overgraph-node/index.js';

let selectedPython = null;

function pythonCandidates() {
  return [
    process.env.PYTHON,
    process.env.PYTHON3,
    process.env.HOME ? join(process.env.HOME, 'anaconda3/bin/python') : null,
    'python3',
    'python',
  ].filter(Boolean);
}

function resolvePython() {
  if (selectedPython !== null) {
    return selectedPython;
  }

  const errors = [];
  for (const candidate of pythonCandidates()) {
    const proc = spawnSync(candidate, ['-c', 'import overgraph'], {
      encoding: 'utf8',
    });
    if (proc.status === 0) {
      selectedPython = candidate;
      return selectedPython;
    }
    errors.push(`${candidate}: ${proc.stderr?.trim() || proc.error?.message || `exit ${proc.status}`}`);
  }

  throw new Error(
    `No Python interpreter could import overgraph. Set PYTHON to the interpreter used by maturin develop.\n${errors.join('\n')}`
  );
}

function runPython(code, payload) {
  const python = resolvePython();
  const proc = spawnSync(python, ['-c', code], {
    input: JSON.stringify(payload),
    encoding: 'utf8',
  });
  if (proc.status !== 0) {
    throw new Error(
      `python smoke step failed with ${proc.status}\nstdout:\n${proc.stdout}\nstderr:\n${proc.stderr}`
    );
  }
  return JSON.parse(proc.stdout);
}

function labels(rows) {
  return rows.map(row => `${row.target_kind}:${row.label}`).sort();
}

function assertShowRows(rows, expected) {
  assert.deepEqual(labels(rows), expected.sort());
  for (const row of rows) {
    assert.equal(typeof row.schema, 'object');
    assert.ok(row.schema !== null);
  }
}

function nodeGqlPublishThenPythonShow(root) {
  const dbPath = join(root, 'node-gql-to-python');
  const db = OverGraph.open(dbPath);
  const published = db.executeGql(`
    ALTER CURRENT GRAPH TYPE SET {
      NODE SmokeNode = {
        properties: {
          name: { required: true, nullable: false, types: ['string'] }
        }
      },
      EDGE SMOKE_EDGE = {
        from: { all_of: ['SmokeNode'] },
        to: { all_of: ['SmokeNode'] },
        properties: {
          role: { required: true, nullable: false, types: ['string'] }
        }
      }
    }
  `);
  assert.equal(published.kind, 'schema');
  assert.equal(published.schemaStats.targetsPublished, 2);
  db.close();

  const observed = runPython(
    `
import json
import sys
from overgraph import OverGraph

payload = json.load(sys.stdin)
db = OverGraph.open(payload["path"])
show = db.execute_gql("SHOW CURRENT GRAPH TYPE")
single = db.execute_gql("SHOW EDGE SCHEMA SMOKE_EDGE")
db.close()

assert show["kind"] == "schema"
assert show["schema_stats"]["operation"] == "show_current_graph_type"
assert single["rows"][0]["schema"]["properties"]["role"]["types"] == ["string"]
print(json.dumps({"rows": show["rows"], "single": single["rows"]}))
    `,
    { path: dbPath }
  );

  assertShowRows(observed.rows, ['edge:SMOKE_EDGE', 'node:SmokeNode']);
  assert.equal(observed.single[0].label, 'SMOKE_EDGE');
}

function pythonBulkPublishThenNodeShow(root) {
  const dbPath = join(root, 'python-bulk-to-node-gql');
  const published = runPython(
    `
import json
import sys
from overgraph import OverGraph

payload = json.load(sys.stdin)
db = OverGraph.open(payload["path"])
result = db.set_graph_schema({
    "node_schemas": [
        {
            "label": "PySmokeNode",
            "schema": {
                "properties": {
                    "name": {
                        "required": True,
                        "nullable": False,
                        "types": ["string"],
                    }
                }
            },
        }
    ],
    "edge_schemas": [
        {
            "label": "PY_SMOKE_EDGE",
            "schema": {
                "from": {"all_of": ["PySmokeNode"]},
                "to": {"all_of": ["PySmokeNode"]},
                "properties": {
                    "role": {
                        "required": True,
                        "nullable": False,
                        "types": ["string"],
                    }
                },
            },
        }
    ],
})
db.close()

assert result.operation == "set"
assert result.targets_published == 2
print(json.dumps({
    "targets_published": result.targets_published,
    "validation_checked": result.validation.checked_records,
}))
    `,
    { path: dbPath }
  );
  assert.equal(published.targets_published, 2);

  const db = OverGraph.open(dbPath);
  const show = db.executeGql('SHOW CURRENT GRAPH TYPE');
  const node = db.executeGql('SHOW NODE SCHEMA PySmokeNode');
  db.close();

  assert.equal(show.kind, 'schema');
  assert.equal(show.schemaStats.operation, 'show_current_graph_type');
  assertShowRows(show.rows, ['edge:PY_SMOKE_EDGE', 'node:PySmokeNode']);
  assert.equal(node.rows[0].schema.properties.name.types[0], 'string');
}

const root = mkdtempSync(join(tmpdir(), 'overgraph-cross-language-schema-'));
try {
  nodeGqlPublishThenPythonShow(root);
  pythonBulkPublishThenNodeShow(root);
  console.log('cross-language schema reopen smoke passed');
} finally {
  rmSync(root, { recursive: true, force: true });
}
