/**
 * OverGraph Node.js Performance Benchmark
 *
 * Measures p50/p95/p99 latency for core operations.
 * Batch upserts are measured both ways:
 *   - "end-to-end": includes JS object/buffer creation in the measurement
 *   - "call-only": pre-builds data, measures only the FFI call + engine work
 *
 * Usage: node __test__/benchmark.mjs
 */

import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { performance } from 'node:perf_hooks';
import { OverGraph } from '../index.js';
import { packNodeBatch, packEdgeBatch } from '../helpers/pack-binary.mjs';

// ── Config ──────────────────────────────────────────────────

const WARMUP_ITERS = 50;
const BENCH_ITERS = 200;

// ── Helpers ─────────────────────────────────────────────────

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: samples.reduce((a, b) => a + b, 0) / samples.length,
  };
}

function fmt(us) {
  if (us >= 1000) return `${(us / 1000).toFixed(2)}ms`;
  return `${us.toFixed(2)}us`;
}

function printRow(name, s) {
  console.log(
    `  ${name.padEnd(40)} ${fmt(s.p50).padStart(10)} ${fmt(s.p95).padStart(10)} ${fmt(s.p99).padStart(10)} ${fmt(s.min).padStart(10)} ${fmt(s.max).padStart(10)} ${fmt(s.mean).padStart(10)}`
  );
}

function printHeader() {
  console.log(
    `  ${'Operation'.padEnd(40)} ${'p50'.padStart(10)} ${'p95'.padStart(10)} ${'p99'.padStart(10)} ${'min'.padStart(10)} ${'max'.padStart(10)} ${'mean'.padStart(10)}`
  );
  console.log('  ' + '-'.repeat(100));
}

/** Run fn for warmup, then collect bench samples (microseconds). */
function bench(fn, { warmup = WARMUP_ITERS, iters = BENCH_ITERS } = {}) {
  for (let i = 0; i < warmup; i++) fn(i);
  const samples = [];
  for (let i = 0; i < iters; i++) {
    const t0 = performance.now();
    fn(warmup + i);
    const t1 = performance.now();
    samples.push((t1 - t0) * 1000); // ms → us
  }
  return stats(samples);
}

// ── Benchmark suite ─────────────────────────────────────────

const tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-bench-'));

try {
  console.log('\nOverGraph Node.js Benchmark');
  console.log(`  iterations: ${BENCH_ITERS} (warmup: ${WARMUP_ITERS})`);
  console.log('  Note: write operations include WAL fsync (~4ms floor per batch)\n');

  // ─── Section 1: Single operations ─────────────────────────

  console.log('  Single operations:');
  printHeader();

  {
    const db = OverGraph.open(join(tmpDir, 'single-node'));
    const s = bench((i) => db.upsertNode(1, `node-${i}`, { props: { idx: i }, weight: 1.0 }));
    printRow('upsert_node', s);
    db.close();
  }

  {
    const db = OverGraph.open(join(tmpDir, 'single-edge'));
    const nodeIds = [];
    for (let i = 0; i < WARMUP_ITERS + BENCH_ITERS + 1; i++) {
      nodeIds.push(db.upsertNode(1, `en-${i}`));
    }
    const s = bench((i) => db.upsertEdge(nodeIds[i], nodeIds[i + 1], 1, { weight: 1.0 }));
    printRow('upsert_edge', s);
    db.close();
  }

  // ─── Section 2: Batch JSON vs Binary (call-only) ──────────
  // Pre-build data, measure only the FFI call + engine work.
  // Uses same keys each iteration (upserts), so WAL cost is identical.

  console.log('\n  Batch call-only (pre-built data, same keys each iter):');
  printHeader();

  // Nodes 100
  {
    const db = OverGraph.open(join(tmpDir, 'call-nodes-100'));
    const jsonArr = Array.from({ length: 100 }, (_, j) => ({
      typeId: 1, key: `cn100-${j}`, props: { idx: j }, weight: 1.0,
    }));
    const binBuf = packNodeBatch(jsonArr);
    const sJson = bench(() => db.batchUpsertNodes(jsonArr));
    printRow('nodes_json (100) call-only', sJson);
    const sBin = bench(() => db.batchUpsertNodesBinary(binBuf));
    printRow('nodes_binary (100) call-only', sBin);
    db.close();
  }

  // Nodes 1000
  {
    const db = OverGraph.open(join(tmpDir, 'call-nodes-1000'));
    const jsonArr = Array.from({ length: 1000 }, (_, j) => ({
      typeId: 1, key: `cn1k-${j}`, props: { idx: j }, weight: 1.0,
    }));
    const binBuf = packNodeBatch(jsonArr);
    const sJson = bench(() => db.batchUpsertNodes(jsonArr), { warmup: 10, iters: 50 });
    printRow('nodes_json (1000) call-only', sJson);
    const sBin = bench(() => db.batchUpsertNodesBinary(binBuf), { warmup: 10, iters: 50 });
    printRow('nodes_binary (1000) call-only', sBin);
    db.close();
  }

  // Edges 100
  {
    const db = OverGraph.open(join(tmpDir, 'call-edges-100'));
    const nids = db.batchUpsertNodes(
      Array.from({ length: 200 }, (_, i) => ({ typeId: 1, key: `ce100-${i}` }))
    );
    const jsonArr = Array.from({ length: 100 }, (_, j) => ({
      from: nids[j], to: nids[j + 100], typeId: 1, weight: 1.0,
    }));
    const binBuf = packEdgeBatch(jsonArr);
    const sJson = bench(() => db.batchUpsertEdges(jsonArr));
    printRow('edges_json (100) call-only', sJson);
    const sBin = bench(() => db.batchUpsertEdgesBinary(binBuf));
    printRow('edges_binary (100) call-only', sBin);
    db.close();
  }

  // Edges 1000
  {
    const db = OverGraph.open(join(tmpDir, 'call-edges-1000'));
    const nids = db.batchUpsertNodes(
      Array.from({ length: 2000 }, (_, i) => ({ typeId: 1, key: `ce1k-${i}` }))
    );
    const jsonArr = Array.from({ length: 1000 }, (_, j) => ({
      from: nids[j], to: nids[j + 1000], typeId: 1, weight: 1.0,
    }));
    const binBuf = packEdgeBatch(jsonArr);
    const sJson = bench(() => db.batchUpsertEdges(jsonArr), { warmup: 10, iters: 50 });
    printRow('edges_json (1000) call-only', sJson);
    const sBin = bench(() => db.batchUpsertEdgesBinary(binBuf), { warmup: 10, iters: 50 });
    printRow('edges_binary (1000) call-only', sBin);
    db.close();
  }

  // ─── Section 3: Batch end-to-end (fresh keys each iteration) ─

  console.log('\n  Batch end-to-end (includes object/buffer creation):');
  printHeader();

  // Nodes 1000 JSON
  {
    const db = OverGraph.open(join(tmpDir, 'e2e-json-1000'));
    const s = bench(
      (i) => {
        const nodes = [];
        for (let j = 0; j < 1000; j++) {
          nodes.push({ typeId: 1, key: `e2e-${i}-${j}`, props: { idx: j }, weight: 1.0 });
        }
        db.batchUpsertNodes(nodes);
      },
      { warmup: 10, iters: 50 }
    );
    printRow('nodes_json (1000) end-to-end', s);
    db.close();
  }

  // Nodes 1000 Binary
  {
    const db = OverGraph.open(join(tmpDir, 'e2e-bin-1000'));
    const s = bench(
      (i) => {
        const nodes = [];
        for (let j = 0; j < 1000; j++) {
          nodes.push({ typeId: 1, key: `e2e-${i}-${j}`, props: { idx: j }, weight: 1.0 });
        }
        db.batchUpsertNodesBinary(packNodeBatch(nodes));
      },
      { warmup: 10, iters: 50 }
    );
    printRow('nodes_binary (1000) end-to-end', s);
    db.close();
  }

  // ─── Section 4: Read operations ───────────────────────────

  console.log('\n  Read operations:');
  printHeader();

  {
    const db = OverGraph.open(join(tmpDir, 'get-node'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: 1000 }, (_, i) => ({
        typeId: 1, key: `gn-${i}`, props: { idx: i, label: `node-${i}` },
      }))
    );
    const s = bench((i) => db.getNode(ids[i % 1000]));
    printRow('get_node', s);
    db.close();
  }

  {
    const db = OverGraph.open(join(tmpDir, 'nbr-10'));
    const hub = db.upsertNode(1, 'hub');
    for (let i = 0; i < 10; i++) {
      const n = db.upsertNode(1, `nbr10-${i}`);
      db.upsertEdge(hub, n, 1, { weight: 1.0 });
    }
    const s = bench(() => db.neighbors(hub, { direction: 'outgoing' }));
    printRow('neighbors (10 edges)', s);
    db.close();
  }

  {
    const db = OverGraph.open(join(tmpDir, 'nbr-100'));
    const hub = db.upsertNode(1, 'hub');
    for (let i = 0; i < 100; i++) {
      const n = db.upsertNode(1, `nbr100-${i}`);
      db.upsertEdge(hub, n, 1, { weight: 1.0 });
    }
    const s = bench(() => db.neighbors(hub, { direction: 'outgoing' }));
    printRow('neighbors (100 edges)', s);
    db.close();
  }

  {
    const db = OverGraph.open(join(tmpDir, 'find'));
    for (let i = 0; i < 1000; i++) {
      db.upsertNode(1, `fn-${i}`, { props: { bucket: i < 500 ? 'target' : 'other' } });
    }
    const s = bench(() => db.findNodes(1, 'bucket', 'target'));
    printRow('find_nodes (500/1000 match)', s);
    db.close();
  }

  // ─── Section 5: Maintenance ───────────────────────────────

  console.log('\n  Maintenance:');
  printHeader();

  {
    const db = OverGraph.open(join(tmpDir, 'flush'));
    const s = bench(
      (i) => {
        const nodes = [];
        for (let j = 0; j < 100; j++) {
          nodes.push({ typeId: 1, key: `fl-${i}-${j}` });
        }
        const ids = db.batchUpsertNodes(nodes);
        const edges = [];
        for (let j = 0; j < 20; j++) {
          edges.push({ from: ids[j], to: ids[j + 1], typeId: 1 });
        }
        db.batchUpsertEdges(edges);
        db.flush();
      },
      { warmup: 5, iters: 30 }
    );
    printRow('flush (100n + 20e)', s);
    db.close();
  }

  console.log('');
} finally {
  rmSync(tmpDir, { recursive: true, force: true });
}
