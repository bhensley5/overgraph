import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { performance } from 'node:perf_hooks';
import { OverGraph } from '../../overgraph-node/index.js';

function percentile(sorted, p) {
  return sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)];
}

function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    p50_us: percentile(sorted, 50),
    p95_us: percentile(sorted, 95),
    min_us: sorted[0],
    max_us: sorted[sorted.length - 1],
    mean_us: samples.reduce((a, b) => a + b, 0) / samples.length,
  };
}

function runBench(fn, warmup, iters) {
  for (let i = 0; i < warmup; i++) fn(i);
  const samples = [];
  for (let i = 0; i < iters; i++) {
    const start = performance.now();
    fn(warmup + i);
    samples.push((performance.now() - start) * 1000);
  }
  return stats(samples);
}

function txnOps(batch, nodeCount, edgeCount) {
  const ops = [];
  for (let i = 0; i < nodeCount; i++) {
    ops.push({ op: 'upsertNode', alias: `n${i}`, typeId: 1, key: `txn:${batch}:n:${i}` });
  }
  for (let i = 0; i < edgeCount; i++) {
    ops.push({
      op: 'upsertEdge',
      alias: `e${i}`,
      from: { local: `n${i % nodeCount}` },
      to: { local: `n${(i + 1) % nodeCount}` },
      typeId: 7,
    });
  }
  return ops;
}

function main() {
  const warmup = Number(process.argv.includes('--warmup') ? process.argv[process.argv.indexOf('--warmup') + 1] : 5);
  const iters = Number(process.argv.includes('--iters') ? process.argv[process.argv.indexOf('--iters') + 1] : 20);
  const tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-txn-node-bench-'));
  const db = OverGraph.open(join(tmpDir, 'db'));
  try {
    const scenarios = [];
    for (const [name, nodes, edges] of [
      ['S-TXN-001-4', 2, 2],
      ['S-TXN-001-16', 8, 8],
      ['S-TXN-001-64', 32, 32],
    ]) {
      scenarios.push({
        scenario_id: name,
        stats: runBench((i) => {
          const txn = db.beginWriteTxn();
          txn.stage(txnOps(i, nodes, edges));
          txn.commit();
        }, warmup, iters),
      });
    }

    scenarios.push({
      scenario_id: 'S-TXN-002',
      stats: runBench((i) => {
        db.upsertNode(9, 'conflict', { props: { i } });
        const txn = db.beginWriteTxn();
        txn.upsertNode(9, 'conflict', { props: { txn: i } });
        db.upsertNode(9, 'conflict', { props: { other: i } });
        try {
          txn.commit();
          throw new Error('expected conflict');
        } catch (err) {
          if (!String(err.message).includes('transaction conflict')) throw err;
        }
      }, warmup, iters),
    });

    scenarios.push({
      scenario_id: 'S-TXN-003',
      stats: runBench((i) => {
        const txn = db.beginWriteTxn();
        txn.stage(txnOps(i, 8, 8));
        txn.rollback();
      }, warmup, iters),
    });

    console.log(JSON.stringify({ language: 'node', warmup, iters, scenarios }, null, 2));
  } finally {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  }
}

main();
