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
    scenarioSet: 'all',
  };
  for (let i = 2; i < argv.length; i++) {
    const token = argv[i];
    if (token === '--profile' && argv[i + 1]) {
      args.profile = argv[++i];
    } else if (token === '--warmup' && argv[i + 1]) {
      args.warmup = Number(argv[++i]);
    } else if (token === '--iters' && argv[i + 1]) {
      args.iters = Number(argv[++i]);
    } else if (token === '--scenario-set' && argv[i + 1]) {
      args.scenarioSet = argv[++i];
    }
  }
  if (!['all', 'query'].includes(args.scenarioSet)) {
    throw new Error(`Unknown scenario set '${args.scenarioSet}'`);
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
    shortest_path_nodes: Math.max(cfg.shortest_path_nodes_min, Math.floor(nodes / cfg.shortest_path_nodes_divisor)),
    shortest_path_edge_offsets: cfg.shortest_path_edge_offsets,
    vector_nodes: Math.max(cfg.vector_nodes_min, Math.floor(profile.nodes / cfg.vector_nodes_divisor)),
    vector_dim: cfg.vector_dim,
    vector_nnz: cfg.vector_nnz,
    vector_sparse_dims: cfg.vector_sparse_dims,
    vector_k: cfg.vector_k,
  };
}

function traverseDeepBranching(fanout) {
  return [Math.max(8, Math.min(24, Math.floor(fanout / 4))), 4, 4];
}

function buildDepthTwoTraversalGraph(db, cfg) {
  const hopNodes = [{ typeId: 1, key: 'root' }];
  for (let i = 0; i < cfg.two_hop_mid; i++) {
    hopNodes.push({ typeId: 1, key: `m-${i}` });
    for (let j = 0; j < cfg.two_hop_leaves_per_mid; j++) {
      hopNodes.push({ typeId: 1, key: `l-${i}-${j}` });
    }
  }
  const hopIds = db.batchUpsertNodes(hopNodes);
  const root = hopIds[0];
  const midStride = 1 + cfg.two_hop_leaves_per_mid;
  const hopEdges = [];
  for (let i = 0; i < cfg.two_hop_mid; i++) {
    const midId = hopIds[1 + i * midStride];
    hopEdges.push({ from: root, to: midId, typeId: 1, weight: 1.0 });
    for (let j = 0; j < cfg.two_hop_leaves_per_mid; j++) {
      const leafId = hopIds[1 + i * midStride + 1 + j];
      hopEdges.push({ from: midId, to: leafId, typeId: 1, weight: 1.0 });
    }
  }
  db.batchUpsertEdges(hopEdges);
  return root;
}

function buildDeepTraversalGraph(db, cfg) {
  const [level1, level2, level3] = traverseDeepBranching(cfg.fanout);
  const nodes = [{ typeId: 1, key: 'root' }];
  for (let i = 0; i < level1; i++) {
    nodes.push({ typeId: 11, key: `lvl1-${i}` });
  }
  for (let i = 0; i < level1; i++) {
    for (let j = 0; j < level2; j++) {
      nodes.push({ typeId: (i + j) % 2 === 0 ? 2 : 3, key: `lvl2-${i}-${j}` });
    }
  }
  for (let i = 0; i < level1; i++) {
    for (let j = 0; j < level2; j++) {
      for (let k = 0; k < level3; k++) {
        nodes.push({ typeId: (i + j + k) % 2 === 0 ? 2 : 3, key: `lvl3-${i}-${j}-${k}` });
      }
    }
  }
  const ids = db.batchUpsertNodes(nodes);
  const root = ids[0];
  const level1Offset = 1;
  const level2Offset = level1Offset + level1;
  const level3Offset = level2Offset + level1 * level2;
  const edges = [];
  for (let i = 0; i < level1; i++) {
    const lvl1Id = ids[level1Offset + i];
    edges.push({ from: root, to: lvl1Id, typeId: 1, weight: 1.0 });
    for (let j = 0; j < level2; j++) {
      const lvl2Idx = i * level2 + j;
      const lvl2Id = ids[level2Offset + lvl2Idx];
      edges.push({ from: lvl1Id, to: lvl2Id, typeId: 1, weight: 1.0 });
      for (let k = 0; k < level3; k++) {
        const lvl3Idx = lvl2Idx * level3 + k;
        edges.push({ from: lvl2Id, to: ids[level3Offset + lvl3Idx], typeId: 1, weight: 1.0 });
      }
    }
  }
  db.batchUpsertEdges(edges);
  return { root, branching: [level1, level2, level3] };
}

function benchSplitmix64(x) {
  x = (x + 0x9E3779B97F4A7C15n) & 0xFFFFFFFFFFFFFFFFn;
  let z = x;
  z = ((z ^ (z >> 30n)) * 0xBF58476D1CE4E5B9n) & 0xFFFFFFFFFFFFFFFFn;
  z = ((z ^ (z >> 27n)) * 0x94D049BB133111EBn) & 0xFFFFFFFFFFFFFFFFn;
  return (z ^ (z >> 31n)) & 0xFFFFFFFFFFFFFFFFn;
}

function benchDenseVector(dim, seed) {
  const values = new Array(dim);
  let state = BigInt(seed);
  for (let i = 0; i < dim; i++) {
    state = benchSplitmix64(state);
    values[i] = Number(state >> 40n) / 16777215 * 2 - 1;
  }
  const norm = Math.sqrt(values.reduce((a, v) => a + v * v, 0));
  if (norm > 0) {
    for (let i = 0; i < dim; i++) values[i] /= norm;
  }
  return values;
}

function benchSparseVector(dimCount, nnz, seed) {
  const dims = [];
  let state = BigInt(seed);
  while (dims.length < nnz) {
    state = benchSplitmix64(state);
    const d = Number(state % BigInt(dimCount));
    if (!dims.includes(d)) dims.push(d);
  }
  dims.sort((a, b) => a - b);
  return dims.map((d, i) => ({ dimension: d, value: 1.0 - i * 0.05 }));
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

function queryBenchProps(i) {
  return {
    status: i % 10 === 0 ? 'active' : 'inactive',
    tier: i % 20 === 0 ? 'gold' : 'standard',
    score: i % 100,
  };
}

function waitForPropertyIndexReady(db, indexId) {
  const deadline = performance.now() + 10_000;
  while (performance.now() < deadline) {
    if (db.listNodePropertyIndexes().some(info => info.indexId === indexId && info.state === 'ready')) {
      return;
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
  throw new Error(`Timed out waiting for property index ${indexId} to become ready`);
}

function queryBenchmarkLayout(preloadNodes) {
  const segments = preloadNodes >= 2 ? 1 : 0;
  const segmentNodes = segments === 0 ? 0 : Math.max(1, Math.floor(preloadNodes / (segments + 1)));
  return {
    segments,
    segment_nodes: segmentNodes,
    memtable_tail_nodes: Math.max(0, preloadNodes - segments * segmentNodes),
  };
}

function queryBenchNodes(start, count) {
  return Array.from({ length: count }, (_, offset) => {
    const i = start + offset;
    return {
      typeId: 1,
      key: `q-${i}`,
      props: queryBenchProps(i),
    };
  });
}

function buildQueryBenchmarkDb(path, preloadNodes) {
  const db = OverGraph.open(path);
  const status = db.ensureNodePropertyIndex(1, 'status', { kind: 'equality' });
  waitForPropertyIndexReady(db, status.indexId);
  const tier = db.ensureNodePropertyIndex(1, 'tier', { kind: 'equality' });
  waitForPropertyIndexReady(db, tier.indexId);
  const score = db.ensureNodePropertyIndex(1, 'score', { kind: 'range', domain: 'int' });
  waitForPropertyIndexReady(db, score.indexId);

  const layout = queryBenchmarkLayout(preloadNodes);
  for (let segment = 0; segment < layout.segments; segment += 1) {
    db.batchUpsertNodes(queryBenchNodes(segment * layout.segment_nodes, layout.segment_nodes));
    db.flush();
  }
  db.batchUpsertNodes(
    queryBenchNodes(layout.segments * layout.segment_nodes, layout.memtable_tail_nodes)
  );
  return { db, layout };
}

function pushQueryScenarios(args, scenarioContract, cfg, tmpRoot, scenarios) {
  const preloadNodes = cfg.time_range_nodes;
  const limit = 100;

  {
    const scenarioId = 'S-QUERY-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'query-node-ids-intersected'), preloadNodes);
    const request = {
      typeId: 1,
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'tier', eq: 'gold' },
        ],
      },
      limit,
    };
    const s = runBench(() => db.queryNodeIds(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_node_ids_intersected_predicates',
        'query',
        s,
        iterCfg,
        {
          type_id: 1,
          preload_nodes: preloadNodes,
          segments: layout.segments,
          segment_nodes: layout.segment_nodes,
          memtable_tail_nodes: layout.memtable_tail_nodes,
          predicates: ['status_eq_active', 'tier_eq_gold'],
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-QUERY-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'query-nodes-hydrated-intersected'), preloadNodes);
    const request = {
      typeId: 1,
      filter: {
        and: [
          { property: 'status', eq: 'active' },
          { property: 'score', gte: 50 },
        ],
      },
      limit,
    };
    const s = runBench(() => db.queryNodes(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_nodes_intersected_predicates_hydrated',
        'query',
        s,
        iterCfg,
        {
          type_id: 1,
          preload_nodes: preloadNodes,
          segments: layout.segments,
          segment_nodes: layout.segment_nodes,
          memtable_tail_nodes: layout.memtable_tail_nodes,
          predicates: ['status_eq_active', 'score_gte_50'],
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }
}

const args = parseArgs(process.argv);
const { profilePath, profilePayload, profile, scenarioContractPath, scenarioContract } =
  loadWorkloadContracts(args.profile);
const cfg = effectiveConfig(profile, scenarioContract);
const tmpRoot = mkdtempSync(join(tmpdir(), `overgraph-node-bench-v2-${args.profile}-`));

const scenarios = [];

try {
  pushQueryScenarios(args, scenarioContract, cfg, tmpRoot, scenarios);

  if (args.scenarioSet === 'all') {
  // S-CRUD-001: single upsert node (growth)
  {
    const scenarioId = 'S-CRUD-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-node'));
    const s = runBench(
      (i) => db.upsertNode(1, `node-${i}`, { props: { idx: i }, weight: 1.0 }),
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
      (i) => db.upsertEdge(nodeIds[i], nodeIds[i + 1], 1, { weight: 1.0 }),
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
    db.upsertNode(1, 'fixed-node', { props: { idx: 0 }, weight: 1.0 });
    const s = runBench(
      (i) => db.upsertNode(1, 'fixed-node', { props: { idx: i }, weight: 1.0 }),
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
      () => db.upsertEdge(nodeA, nodeB, 1, { weight: 1.0 }),
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
    const neighNodeIds = db.batchUpsertNodes([
      { typeId: 1, key: 'hub' },
      ...Array.from({ length: cfg.fanout }, (_, i) => ({ typeId: 1, key: `n-${i}` })),
    ]);
    const hub = neighNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.fanout }, (_, i) => ({
        from: hub,
        to: neighNodeIds[i + 1],
        typeId: 1,
        weight: 1.0,
      }))
    );
    const s = runBench(
      () => db.neighbors(hub, { direction: 'outgoing' }),
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

  // S-TRAV-002: traverse exact depth-2
  {
    const scenarioId = 'S-TRAV-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-traverse-depth2'));
    const root = buildDepthTwoTraversalGraph(db, cfg);
    const s = runBench(
      () => db.traverse(root, 2, { minDepth: 2, direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'traverse_depth_2',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          min_depth: 2,
          max_depth: 2,
          mid_nodes: cfg.two_hop_mid,
          leaves_per_mid: cfg.two_hop_leaves_per_mid,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-007: deeper traverse, memtable, fast path
  {
    const scenarioId = 'S-TRAV-007';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-depth13-memtable'));
    const { root, branching } = buildDeepTraversalGraph(db, cfg);
    const s = runBench(
      () => db.traverse(root, 3, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'traverse_depth_1_to_3',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          layout: 'memtable',
          min_depth: 1,
          max_depth: 3,
          node_type_filter: null,
          branching,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-008: deeper traverse, segmented, fast path
  {
    const scenarioId = 'S-TRAV-008';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-depth13-segment'));
    const { root, branching } = buildDeepTraversalGraph(db, cfg);
    db.flush();
    const s = runBench(
      () => db.traverse(root, 3, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'traverse_depth_1_to_3_segment',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          layout: 'segment',
          min_depth: 1,
          max_depth: 3,
          node_type_filter: null,
          branching,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-009: deeper traverse, memtable, emission-filtered path
  {
    const scenarioId = 'S-TRAV-009';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-depth13-filtered-memtable'));
    const { root, branching } = buildDeepTraversalGraph(db, cfg);
    const s = runBench(
      () => db.traverse(root, 3, { direction: 'outgoing', nodeTypeFilter: [2] }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'traverse_depth_1_to_3_filtered',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          layout: 'memtable',
          min_depth: 1,
          max_depth: 3,
          node_type_filter: [2],
          branching,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-010: deeper traverse, segmented, emission-filtered path
  {
    const scenarioId = 'S-TRAV-010';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-depth13-filtered-segment'));
    const { root, branching } = buildDeepTraversalGraph(db, cfg);
    db.flush();
    const s = runBench(
      () => db.traverse(root, 3, { direction: 'outgoing', nodeTypeFilter: [2] }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'traverse_depth_1_to_3_filtered_segment',
        'traversal',
        s,
        iterCfg,
        {
          direction: 'outgoing',
          layout: 'segment',
          min_depth: 1,
          max_depth: 3,
          node_type_filter: [2],
          branching,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-003: degree (scalar)
  {
    const scenarioId = 'S-TRAV-003';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-degree'));
    const degNodeIds = db.batchUpsertNodes([
      { typeId: 1, key: 'hub' },
      ...Array.from({ length: cfg.fanout }, (_, i) => ({ typeId: 1, key: `d-${i}` })),
    ]);
    const hub = degNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.fanout }, (_, i) => ({
        from: hub,
        to: degNodeIds[i + 1],
        typeId: 1,
        weight: 1.0,
      }))
    );
    const s = runBench(
      () => db.degree(hub, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'degree',
        'traversal',
        s,
        iterCfg,
        { fanout: cfg.fanout, direction: 'outgoing' },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-004: degrees (batch)
  {
    const scenarioId = 'S-TRAV-004';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-degrees'));
    // Batch all hub + spoke nodes: [hub-0, spoke-0-0, spoke-0-1, ..., hub-1, spoke-1-0, ...]
    const allNodes = [];
    for (let h = 0; h < cfg.batch_nodes; h++) {
      allNodes.push({ typeId: 1, key: `hub-${h}` });
      for (let i = 0; i < cfg.fanout; i++) {
        allNodes.push({ typeId: 1, key: `dt-${h}-${i}` });
      }
    }
    const allNodeIds = db.batchUpsertNodes(allNodes);
    const stride = 1 + cfg.fanout; // hub + its spokes
    const hubIds = [];
    const degEdges = [];
    for (let h = 0; h < cfg.batch_nodes; h++) {
      const hubId = allNodeIds[h * stride];
      hubIds.push(hubId);
      for (let i = 0; i < cfg.fanout; i++) {
        degEdges.push({ from: hubId, to: allNodeIds[h * stride + 1 + i], typeId: 1, weight: 1.0 });
      }
    }
    db.batchUpsertEdges(degEdges);
    const s = runBench(
      () => db.degrees(hubIds, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'degrees',
        'traversal',
        s,
        iterCfg,
        { batch_nodes: cfg.batch_nodes, fanout: cfg.fanout, direction: 'outgoing' },
        scenarioComparability(scenarioContract, scenarioId),
        cfg.batch_nodes
      )
    );
    db.close();
  }

  // S-TRAV-005: shortest_path (BFS)
  {
    const scenarioId = 'S-TRAV-005';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-shortest-path'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: cfg.shortest_path_nodes }, (_, i) => ({ typeId: 1, key: `sp-${i}` }))
    );
    const spEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.shortest_path_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.shortest_path_edge_offsets[1]) % ids.length];
      spEdges.push({ from, to: to1, typeId: 1, weight: 1.0 });
      spEdges.push({ from, to: to2, typeId: 1, weight: 1.0 });
    }
    db.batchUpsertEdges(spEdges);
    const spFrom = ids[0];
    const spTo = ids[Math.floor(ids.length / 2)];
    const s = runBench(
      () => db.shortestPath(spFrom, spTo, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'shortest_path',
        'traversal',
        s,
        iterCfg,
        {
          shortest_path_nodes: cfg.shortest_path_nodes,
          edge_offsets: cfg.shortest_path_edge_offsets,
          direction: 'outgoing',
          weight_field: null,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  // S-TRAV-006: is_connected
  {
    const scenarioId = 'S-TRAV-006';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'trav-is-connected'));
    const ids = db.batchUpsertNodes(
      Array.from({ length: cfg.shortest_path_nodes }, (_, i) => ({ typeId: 1, key: `ic-${i}` }))
    );
    const icEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.shortest_path_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.shortest_path_edge_offsets[1]) % ids.length];
      icEdges.push({ from, to: to1, typeId: 1, weight: 1.0 });
      icEdges.push({ from, to: to2, typeId: 1, weight: 1.0 });
    }
    db.batchUpsertEdges(icEdges);
    const spFrom = ids[0];
    const spTo = ids[Math.floor(ids.length / 2)];
    const s = runBench(
      () => db.isConnected(spFrom, spTo, { direction: 'outgoing' }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'is_connected',
        'traversal',
        s,
        iterCfg,
        {
          shortest_path_nodes: cfg.shortest_path_nodes,
          edge_offsets: cfg.shortest_path_edge_offsets,
          direction: 'outgoing',
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
    const topKNodeIds = db.batchUpsertNodes([
      { typeId: 1, key: 'hub' },
      ...Array.from({ length: cfg.top_k_candidates }, (_, i) => ({
        typeId: 1,
        key: `tk-${i}`,
      })),
    ]);
    const hub = topKNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.top_k_candidates }, (_, i) => ({
        from: hub,
        to: topKNodeIds[i + 1],
        typeId: 1,
        weight: 1.0 + ((i % 100) / 10.0),
      }))
    );
    const s = runBench(
      () => db.topKNeighbors(hub, cfg.top_k_limit, { direction: 'outgoing', scoring: 'weight' }),
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
    db.batchUpsertNodes(
      Array.from({ length: cfg.time_range_nodes }, (_, i) => ({
        typeId: 1,
        key: `tr-${i}`,
        props: { idx: i },
        weight: 1.0,
      }))
    );
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
    const pprEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.ppr_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.ppr_edge_offsets[1]) % ids.length];
      pprEdges.push({ from, to: to1, typeId: 1, weight: 1.0 });
      pprEdges.push({ from, to: to2, typeId: 1, weight: 0.7 });
    }
    db.batchUpsertEdges(pprEdges);
    const s = runBench(
      () =>
        db.personalizedPagerank([ids[0]], {
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
    const exportEdges = [];
    for (let i = 0; i < cfg.export_edges; i++) {
      const from = ids[i % ids.length];
      const to = ids[(i * 13 + 7) % ids.length];
      if (from !== to) exportEdges.push({ from, to, typeId: 1, weight: 1.0 });
    }
    db.batchUpsertEdges(exportEdges);
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

  // S-VEC-001: hybrid_vector_search
  {
    const scenarioId = 'S-VEC-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'vec-hybrid'), {
      denseVector: { dimension: cfg.vector_dim, metric: 'cosine' },
    });

    const nodes = Array.from({ length: cfg.vector_nodes }, (_, i) => {
      const seed = 1729 * (i + 1);
      return {
        typeId: 1,
        key: `v-${i}`,
        denseVector: benchDenseVector(cfg.vector_dim, seed),
        sparseVector: benchSparseVector(cfg.vector_sparse_dims, cfg.vector_nnz, seed + 0xCAFE),
      };
    });
    db.batchUpsertNodes(nodes);
    db.flush();

    const querySeed = 0xDEADBEEF;
    const denseQuery = benchDenseVector(cfg.vector_dim, querySeed);
    const sparseQuery = benchSparseVector(cfg.vector_sparse_dims, cfg.vector_nnz, querySeed + 0xCAFE);

    const s = runBench(
      () => db.vectorSearch('hybrid', { k: cfg.vector_k, denseQuery, sparseQuery }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'hybrid_vector_search',
        'vector',
        s,
        iterCfg,
        {
          vector_nodes: cfg.vector_nodes,
          vector_dim: cfg.vector_dim,
          vector_nnz: cfg.vector_nnz,
          vector_sparse_dims: cfg.vector_sparse_dims,
          vector_k: cfg.vector_k,
          mode: 'hybrid',
          fusion_mode: 'weighted_rank',
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }
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
