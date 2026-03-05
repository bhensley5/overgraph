/**
 * OverGraph Node.js benchmark v2.
 *
 * Emits machine-readable JSON for runner ingestion.
 * Uses shared workload profiles from docs/04-quality/workloads/profiles.json.
 */

import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { OverGraph } from '../index.js';
import { packNodeBatch } from '../helpers/pack-binary.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function parseArgs(argv) {
  const args = {
    profile: 'small',
    warmup: 20,
    iters: 80,
  };
  for (let i = 2; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--profile' && argv[i + 1]) {
      args.profile = argv[++i];
    } else if (token === '--warmup' && argv[i + 1]) {
      args.warmup = Number(argv[++i]);
    } else if (token === '--iters' && argv[i + 1]) {
      args.iters = Number(argv[++i]);
    }
  }
  return args;
}

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
  return {
    p50_us: percentile(sorted, 50),
    p95_us: percentile(sorted, 95),
    p99_us: percentile(sorted, 99),
    min_us: sorted[0],
    max_us: sorted[sorted.length - 1],
    mean_us: mean,
  };
}

function runBench(fn, warmup, iters, growth = false) {
  for (let i = 0; i < warmup; i++) fn(i);
  const samples = [];
  for (let i = 0; i < iters; i++) {
    const t0 = performance.now();
    fn(warmup + i);
    const t1 = performance.now();
    samples.push((t1 - t0) * 1000); // ms -> us
  }
  const s = stats(samples);
  if (growth && samples.length >= 4) {
    const mid = Math.floor(samples.length / 2);
    const earlyP95 = percentile([...samples.slice(0, mid)].sort((a, b) => a - b), 95);
    const lateP95 = percentile([...samples.slice(mid)].sort((a, b) => a - b), 95);
    s.early_p95_us = earlyP95;
    s.late_p95_us = lateP95;
    s.drift_ratio = earlyP95 > 0 ? lateP95 / earlyP95 : null;
  }
  return s;
}

function throughputOpsPerSec(meanUs, opsPerIter) {
  if (!meanUs || meanUs <= 0) return null;
  return (opsPerIter * 1_000_000.0) / meanUs;
}

function loadWorkloadContracts(profileName) {
  const profilePath = resolve(__dirname, '../../docs/04-quality/workloads/profiles.json');
  const scenarioContractPath = resolve(
    __dirname,
    '../../docs/04-quality/workloads/scenario-contract.json'
  );

  const profilePayload = JSON.parse(readFileSync(profilePath, 'utf8'));
  const profile = profilePayload.profiles[profileName];
  if (!profile) {
    throw new Error(`Unknown profile '${profileName}'`);
  }

  const scenarioContract = JSON.parse(readFileSync(scenarioContractPath, 'utf8'));
  return {
    profilePath,
    profilePayload,
    profile,
    scenarioContractPath,
    scenarioContract,
  };
}

function scenarioIterations(args, scenarioContract, scenarioId) {
  const defaultPolicy = scenarioContract.scenario_iteration_policy.default;
  const policy = scenarioContract.scenario_iteration_policy[scenarioId] || defaultPolicy;

  const warmupDivisor = Math.max(1, Number(policy.warmup_divisor || defaultPolicy.warmup_divisor || 1));
  const warmupMin = Math.max(1, Number(policy.warmup_min || defaultPolicy.warmup_min || 1));
  const itersDivisor = Math.max(1, Number(policy.iters_divisor || defaultPolicy.iters_divisor || 1));
  const itersMin = Math.max(1, Number(policy.iters_min || defaultPolicy.iters_min || 1));
  const itersMultiplier = Math.max(1, Number(policy.iters_multiplier || defaultPolicy.iters_multiplier || 1));

  return {
    warmup: Math.max(warmupMin, Math.floor(args.warmup / warmupDivisor)),
    iters: Math.max(itersMin, Math.floor(args.iters / itersDivisor)) * itersMultiplier,
  };
}

function scenarioComparability(scenarioContract, scenarioId) {
  const entry = scenarioContract.comparability[scenarioId] || {
    status: 'non_comparable',
    reason: 'Missing comparability contract entry',
  };
  return {
    status: entry.status,
    reason: entry.reason || null,
  };
}

function effectiveConfig(profile, scenarioContract) {
  const cfg = scenarioContract.effective_config;
  const nodes = Math.max(cfg.nodes_min, Math.floor(profile.nodes / cfg.nodes_divisor));
  const edges = Math.max(cfg.edges_min, Math.floor(profile.edges / cfg.edges_divisor));
  const fanout = Math.min(
    cfg.fanout_max,
    Math.max(cfg.fanout_min, profile.average_degree_target * cfg.fanout_degree_multiplier)
  );

  const batch_nodes = Math.max(cfg.batch_nodes_min, Number(profile.batch_sizes.nodes || cfg.batch_nodes_min));
  const batch_edges = Math.max(cfg.batch_edges_min, Number(profile.batch_sizes.edges || cfg.batch_edges_min));
  const two_hop_mid = Math.max(cfg.two_hop_mid_min, fanout);

  return {
    nodes,
    edges,
    fanout,
    batch_nodes,
    batch_edges,
    two_hop_mid,
    two_hop_leaves_per_mid: cfg.two_hop_leaves_per_mid,
    top_k_candidates: Math.max(cfg.top_k_candidates_min, Math.floor(nodes / cfg.top_k_candidates_divisor)),
    ppr_nodes: Math.max(cfg.ppr_nodes_min, Math.floor(nodes / cfg.ppr_nodes_divisor)),
    get_node_nodes: Math.min(nodes, cfg.time_range_nodes_cap),
    time_range_nodes: Math.min(nodes, cfg.time_range_nodes_cap),
    export_nodes: Math.min(nodes, cfg.export_nodes_cap),
    export_edges: Math.min(edges, cfg.export_edges_cap),
    flush_nodes_per_iter: Math.min(batch_nodes, cfg.flush_node_batch_cap),
    flush_edges_per_iter_cap: cfg.flush_edge_chain_cap,
    ppr_max_iterations: cfg.ppr_max_iterations,
    ppr_max_results: cfg.ppr_max_results,
    ppr_seed_count: cfg.ppr_seed_count,
    ppr_edge_offsets: cfg.ppr_edge_offsets,
    top_k_limit: cfg.top_k_limit,
    time_range_from_ms: cfg.time_range_from_ms,
    time_range_window_ms: cfg.time_range_window_ms,
    include_weights_on_export: Boolean(cfg.include_weights_on_export),
  };
}

function scenario(
  id,
  name,
  category,
  statsObj,
  iterCfg,
  scenarioParams,
  comparability,
  opsPerIter = 1,
  notes = null
) {
  return {
    scenario_id: id,
    name,
    category,
    warmup_iterations: iterCfg.warmup,
    benchmark_iterations: iterCfg.iters,
    ops_per_iteration: opsPerIter,
    throughput_ops_per_sec: throughputOpsPerSec(statsObj.mean_us, opsPerIter),
    stats: statsObj,
    scenario_params: scenarioParams,
    comparability,
    notes,
  };
}

const args = parseArgs(process.argv);
const { profilePath, profilePayload, profile, scenarioContractPath, scenarioContract } =
  loadWorkloadContracts(args.profile);
const cfg = effectiveConfig(profile, scenarioContract);
const tmpRoot = mkdtempSync(join(tmpdir(), `overgraph-node-bench-v2-${args.profile}-`));

const scenarios = [];

try {
  // S-CRUD-001: single upsert node (growth)
  {
    const scenarioId = 'S-CRUD-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-node'));
    const s = runBench(
      (i) => db.upsertNode(1, `node-${i}`, { idx: i }, 1.0),
      iterCfg.warmup,
      iterCfg.iters,
      true
    );
    scenarios.push(
      scenario(
        scenarioId,
        'upsert_node',
        'crud',
        s,
        iterCfg,
        { type_id: 1, with_props: true, weight: 1.0 },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-CRUD-002: single upsert edge (growth)
  {
    const scenarioId = 'S-CRUD-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-edge'));
    const nodeIds = db.batchUpsertNodes(
      Array.from({ length: iterCfg.warmup + iterCfg.iters + 1 }, (_, i) => ({ typeId: 1, key: `e-${i}` }))
    );
    const s = runBench(
      (i) => db.upsertEdge(nodeIds[i], nodeIds[i + 1], 1, null, 1.0),
      iterCfg.warmup,
      iterCfg.iters,
      true
    );
    scenarios.push(
      scenario(
        scenarioId,
        'upsert_edge',
        'crud',
        s,
        iterCfg,
        { edge_type_id: 1, weight: 1.0 },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-BATCH-001: batch nodes (json)
  {
    const scenarioId = 'S-BATCH-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'batch-nodes-json'));
    const s = runBench(
      (i) => {
        const nodes = Array.from({ length: cfg.batch_nodes }, (_, j) => ({
          typeId: 1,
          key: `bn-${i}-${j}`,
          props: { idx: j },
          weight: 1.0,
        }));
        db.batchUpsertNodes(nodes);
      },
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'batch_upsert_nodes_json',
        'batch',
        s,
        iterCfg,
        { batch_nodes: cfg.batch_nodes, type_id: 1, with_props: true },
        scenarioComparability(scenarioContract, scenarioId),
        cfg.batch_nodes
      )
    );
    db.close();
  }

  // S-BATCH-002: batch nodes (binary)
  {
    const scenarioId = 'S-BATCH-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'batch-nodes-binary'));
    const s = runBench(
      (i) => {
        const nodes = Array.from({ length: cfg.batch_nodes }, (_, j) => ({
          typeId: 1,
          key: `bb-${i}-${j}`,
          props: { idx: j },
          weight: 1.0,
        }));
        db.batchUpsertNodesBinary(packNodeBatch(nodes));
      },
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'batch_upsert_nodes_binary',
        'batch',
        s,
        iterCfg,
        { batch_nodes: cfg.batch_nodes, encoding: 'binary-pack-node-batch' },
        scenarioComparability(scenarioContract, scenarioId),
        cfg.batch_nodes
      )
    );
    db.close();
  }

  // S-CRUD-003: get_node
  {
    const scenarioId = 'S-CRUD-003';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-get-node'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: cfg.get_node_nodes }, (_, i) => ({
        typeId: 1,
        key: `gn-${i}`,
        props: { idx: i },
      }))
    );
    const s = runBench(
      (i) => db.getNode(ids[i % ids.length]),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'get_node',
        'crud',
        s,
        iterCfg,
        { preload_nodes: cfg.get_node_nodes },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-CRUD-004: upsert_node_fixed_key
  {
    const scenarioId = 'S-CRUD-004';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-node-fixed'));
    db.upsertNode(1, 'fixed-node', { idx: 0 }, 1.0);
    const s = runBench(
      (i) => db.upsertNode(1, 'fixed-node', { idx: i }, 1.0),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'upsert_node_fixed_key',
        'crud',
        s,
        iterCfg,
        { type_id: 1, with_props: true, weight: 1.0, fixed_key: true },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-CRUD-005: upsert_edge_fixed_triple
  {
    const scenarioId = 'S-CRUD-005';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-edge-fixed'), {
      edgeUniqueness: true,
    });
    const nodeA = db.upsertNode(1, 'fixed-a');
    const nodeB = db.upsertNode(1, 'fixed-b');
    const s = runBench(
      () => db.upsertEdge(nodeA, nodeB, 1, null, 1.0),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'upsert_edge_fixed_triple',
        'crud',
        s,
        iterCfg,
        { edge_type_id: 1, weight: 1.0, edge_uniqueness: true, fixed_triple: true },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-001: neighbors fanout
  {
    const scenarioId = 'S-TRAV-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-neighbors'));
    const hub = db.upsertNode(1, 'hub');
    for (let i = 0; i < cfg.fanout; i++) {
      const n = db.upsertNode(1, `n-${i}`);
      db.upsertEdge(hub, n, 1, null, 1.0);
    }
    const s = runBench(
      () => db.neighbors(hub, 'outgoing'),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'neighbors',
        'traversal',
        s,
        iterCfg,
        { fanout: cfg.fanout, direction: 'outgoing' },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-002: neighbors_2hop
  {
    const scenarioId = 'S-TRAV-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-neighbors-2hop'));
    const root = db.upsertNode(1, 'root');
    for (let i = 0; i < cfg.two_hop_mid; i++) {
      const mid = db.upsertNode(1, `m-${i}`);
      db.upsertEdge(root, mid, 1, null, 1.0);
      for (let j = 0; j < cfg.two_hop_leaves_per_mid; j++) {
        const leaf = db.upsertNode(1, `l-${i}-${j}`);
        db.upsertEdge(mid, leaf, 1, null, 1.0);
      }
    }
    const s = runBench(
      () => db.neighbors2Hop(root, 'outgoing'),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'neighbors_2hop',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          mid_nodes: cfg.two_hop_mid,
          leaves_per_mid: cfg.two_hop_leaves_per_mid,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-ADV-001: top_k_neighbors
  {
    const scenarioId = 'S-ADV-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'adv-top-k'));
    const hub = db.upsertNode(1, 'hub');
    for (let i = 0; i < cfg.top_k_candidates; i++) {
      const n = db.upsertNode(1, `tk-${i}`);
      db.upsertEdge(hub, n, 1, null, 1.0 + ((i % 100) / 10.0));
    }
    const s = runBench(
      () => db.topKNeighbors(hub, 'outgoing', null, cfg.top_k_limit, 'weight'),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'top_k_neighbors',
        'advanced',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          k: cfg.top_k_limit,
          scoring: 'weight',
          candidate_nodes: cfg.top_k_candidates,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-ADV-003: find_nodes_by_time_range
  {
    const scenarioId = 'S-ADV-003';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'adv-time-range'));
    for (let i = 0; i < cfg.time_range_nodes; i++) {
      db.upsertNode(1, `tr-${i}`, { idx: i }, 1.0);
    }
    const fromMs = cfg.time_range_from_ms;
    const toMs = Date.now() + cfg.time_range_window_ms;
    const s = runBench(
      () => db.findNodesByTimeRange(1, fromMs, toMs),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'find_nodes_by_time_range',
        'advanced',
        s,
        iterCfg,
        {
          type_id: 1,
          preload_nodes: cfg.time_range_nodes,
          from_ms: cfg.time_range_from_ms,
          to_ms_window: cfg.time_range_window_ms,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-ADV-004: personalized_pagerank
  {
    const scenarioId = 'S-ADV-004';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'adv-ppr'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: cfg.ppr_nodes }, (_, i) => ({ typeId: 1, key: `ppr-${i}` }))
    );
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.ppr_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.ppr_edge_offsets[1]) % ids.length];
      db.upsertEdge(from, to1, 1, null, 1.0);
      db.upsertEdge(from, to2, 1, null, 0.7);
    }
    const seed = new Float64Array([ids[0]]);
    const s = runBench(
      () =>
        db.personalizedPagerank(seed, {
          maxIterations: cfg.ppr_max_iterations,
          maxResults: cfg.ppr_max_results,
        }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'personalized_pagerank',
        'advanced',
        s,
        iterCfg,
        {
          ppr_nodes: cfg.ppr_nodes,
          seed_strategy: 'first_node_id',
          seed_count: cfg.ppr_seed_count,
          edge_offsets: cfg.ppr_edge_offsets,
          max_iterations: cfg.ppr_max_iterations,
          max_results: cfg.ppr_max_results,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-ADV-005: export_adjacency
  {
    const scenarioId = 'S-ADV-005';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'adv-export'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: cfg.export_nodes }, (_, i) => ({ typeId: 1, key: `ex-${i}` }))
    );
    for (let i = 0; i < cfg.export_edges; i++) {
      const from = ids[i % ids.length];
      const to = ids[(i * 13 + 7) % ids.length];
      if (from !== to) db.upsertEdge(from, to, 1, null, 1.0);
    }
    const s = runBench(
      () => db.exportAdjacency({ includeWeights: cfg.include_weights_on_export }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'export_adjacency',
        'advanced',
        s,
        iterCfg,
        {
          preload_nodes: cfg.export_nodes,
          preload_edges: cfg.export_edges,
          include_weights: cfg.include_weights_on_export,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-MAIN-001: flush
  {
    const scenarioId = 'S-MAIN-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'maint-flush'));
    const s = runBench(
      (i) => {
        const nodes = Array.from({ length: cfg.flush_nodes_per_iter }, (_, j) => ({
          typeId: 1,
          key: `fl-${i}-${j}`,
          props: { idx: j },
          weight: 1.0,
        }));
        const ids = db.batchUpsertNodes(nodes);
        const edges = [];
        for (let j = 0; j < Math.min(cfg.flush_edges_per_iter_cap, ids.length - 1); j++) {
          edges.push({ from: ids[j], to: ids[j + 1], typeId: 1, weight: 1.0 });
        }
        db.batchUpsertEdges(edges);
        db.flush();
      },
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'flush',
        'maintenance',
        s,
        iterCfg,
        {
          nodes_per_iter: cfg.flush_nodes_per_iter,
          edge_chain_cap: cfg.flush_edges_per_iter_cap,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  const output = {
    schema_version: 1,
    language: 'node',
    harness_stage: 'connector-benchmark-v3-parity',
    profile_name: args.profile,
    generated_at_utc: new Date().toISOString(),
    profile_source: profilePath,
    scenario_contract_source: scenarioContractPath,
    percentile_method: scenarioContract.percentile_method,
    profile_contract: {
      determinism: profilePayload.determinism,
      profile,
      effective_config: cfg,
      scenario_contract_schema_version: scenarioContract.schema_version,
    },
    scenarios,
  };

  process.stdout.write(JSON.stringify(output, null, 2));
  process.stdout.write('\n');
} finally {
  rmSync(tmpRoot, { recursive: true, force: true });
}
