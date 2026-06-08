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
    scenarioIds: [],
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
    } else if (token === '--scenario-id' && argv[i + 1]) {
      args.scenarioIds.push(argv[++i]);
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

function runBenchWithSetup(setup, fn, warmup, iters) {
  for (let i = 0; i < warmup; i++) {
    setup(i);
    fn(i);
  }
  const samples = [];
  for (let i = 0; i < iters; i++) {
    const idx = warmup + i;
    setup(idx);
    const t0 = performance.now();
    fn(idx);
    const t1 = performance.now();
    samples.push((t1 - t0) * 1000);
  }
  return stats(samples);
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

function scenarioSelected(args, scenarioId) {
  return args.scenarioIds.length === 0 || args.scenarioIds.includes(scenarioId);
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

function nodeInput(label, key, fields = {}) {
  return { labels: [label], key, ...fields };
}

function nodeFilter(label, mode = 'all') {
  return { labels: [label], mode };
}

function buildDepthTwoTraversalGraph(db, cfg) {
  const hopNodes = [nodeInput('Person', 'root')];
  for (let i = 0; i < cfg.two_hop_mid; i++) {
    hopNodes.push(nodeInput('Person', `m-${i}`));
    for (let j = 0; j < cfg.two_hop_leaves_per_mid; j++) {
      hopNodes.push(nodeInput('Person', `l-${i}-${j}`));
    }
  }
  const hopIds = db.batchUpsertNodes(hopNodes);
  const root = hopIds[0];
  const midStride = 1 + cfg.two_hop_leaves_per_mid;
  const hopEdges = [];
  for (let i = 0; i < cfg.two_hop_mid; i++) {
    const midId = hopIds[1 + i * midStride];
    hopEdges.push({ from: root, to: midId, label: 'LINKS_TO', weight: 1.0 });
    for (let j = 0; j < cfg.two_hop_leaves_per_mid; j++) {
      const leafId = hopIds[1 + i * midStride + 1 + j];
      hopEdges.push({ from: midId, to: leafId, label: 'LINKS_TO', weight: 1.0 });
    }
  }
  db.batchUpsertEdges(hopEdges);
  return root;
}

function buildDeepTraversalGraph(db, cfg) {
  const [level1, level2, level3] = traverseDeepBranching(cfg.fanout);
  const nodes = [nodeInput('Person', 'root')];
  for (let i = 0; i < level1; i++) {
    nodes.push(nodeInput('LevelOne', `lvl1-${i}`));
  }
  for (let i = 0; i < level1; i++) {
    for (let j = 0; j < level2; j++) {
      nodes.push(nodeInput((i + j) % 2 === 0 ? 'Company' : 'Document', `lvl2-${i}-${j}`));
    }
  }
  for (let i = 0; i < level1; i++) {
    for (let j = 0; j < level2; j++) {
      for (let k = 0; k < level3; k++) {
        nodes.push(nodeInput((i + j + k) % 2 === 0 ? 'Company' : 'Document', `lvl3-${i}-${j}-${k}`));
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
    edges.push({ from: root, to: lvl1Id, label: 'LINKS_TO', weight: 1.0 });
    for (let j = 0; j < level2; j++) {
      const lvl2Idx = i * level2 + j;
      const lvl2Id = ids[level2Offset + lvl2Idx];
      edges.push({ from: lvl1Id, to: lvl2Id, label: 'LINKS_TO', weight: 1.0 });
      for (let k = 0; k < level3; k++) {
        const lvl3Idx = lvl2Idx * level3 + k;
        edges.push({ from: lvl2Id, to: ids[level3Offset + lvl3Idx], label: 'LINKS_TO', weight: 1.0 });
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

function waitForEdgePropertyIndexReady(db, indexId) {
  const deadline = performance.now() + 10_000;
  while (performance.now() < deadline) {
    if (db.listEdgePropertyIndexes().some(info => info.indexId === indexId && info.state === 'ready')) {
      return;
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
  throw new Error(`Timed out waiting for edge property index ${indexId} to become ready`);
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
    return nodeInput('Person', `q-${i}`, {
      props: queryBenchProps(i),
    });
  });
}

function buildQueryBenchmarkDb(path, preloadNodes) {
  const db = OverGraph.open(path);
  const status = db.ensureNodePropertyIndex('Person', 'status', 'equality');
  waitForPropertyIndexReady(db, status.indexId);
  const tier = db.ensureNodePropertyIndex('Person', 'tier', 'equality');
  waitForPropertyIndexReady(db, tier.indexId);
  const score = db.ensureNodePropertyIndex('Person', 'score', 'range');
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

function buildEdgeQueryBenchmarkDb(path, preloadEdges) {
  const db = OverGraph.open(path);
  const sourceCount = 1;
  const targetCount = Math.max(1, preloadEdges);
  const nodes = [];
  for (let i = 0; i < sourceCount; i += 1) {
    nodes.push(nodeInput('Person', `edge-source-${i}`));
  }
  for (let i = 0; i < targetCount; i += 1) {
    nodes.push(nodeInput('Company', `edge-target-${i}`));
  }
  const ids = db.batchUpsertNodes(nodes);
  const sourceIds = ids.slice(0, sourceCount);
  const targetIds = ids.slice(sourceCount);
  const sourceId = sourceIds[0];
  const segments = preloadEdges >= 2 ? 1 : 0;
  const segmentEdges = segments === 0 ? 0 : Math.max(1, Math.floor(preloadEdges / 2));
  const memtableTailEdges = Math.max(0, preloadEdges - segmentEdges);
  const makeEdges = (start, count) => Array.from({ length: count }, (_, offset) => {
    const i = start + offset;
    return {
      from: sourceIds[i % sourceCount],
      to: targetIds[i % targetIds.length],
      label: 'WORKS_AT',
      props: { role: i % 10 === 0 ? 'lead' : 'member', score: i % 100 },
      weight: i % 2 === 0 ? 2.0 : 0.5,
    };
  });
  if (segmentEdges > 0) {
    db.batchUpsertEdges(makeEdges(0, segmentEdges));
    db.flush();
  }
  if (memtableTailEdges > 0) {
    db.batchUpsertEdges(makeEdges(segmentEdges, memtableTailEdges));
  }
  return {
    db,
    sourceId,
    layout: { segments, segment_edges: segmentEdges, memtable_tail_edges: memtableTailEdges },
  };
}

function buildIndexedEdgeQueryBenchmarkDb(path, preloadEdges) {
  const fixture = buildEdgeQueryBenchmarkDb(path, preloadEdges);
  const role = fixture.db.ensureEdgePropertyIndex('WORKS_AT', 'role', 'equality');
  waitForEdgePropertyIndexReady(fixture.db, role.indexId);
  const score = fixture.db.ensureEdgePropertyIndex('WORKS_AT', 'score', 'range');
  waitForEdgePropertyIndexReady(fixture.db, score.indexId);
  return fixture;
}

function buildGraphRowBenchmarkDb(path, preloadEdges) {
  const db = OverGraph.open(path);
  const sourceCount = 1;
  const targetCount = Math.max(1, preloadEdges);
  const nodes = [];
  for (let i = 0; i < sourceCount; i += 1) {
    nodes.push(nodeInput('Person', `edge-source-${i}`));
  }
  for (let i = 0; i < targetCount; i += 1) {
    nodes.push(nodeInput('Company', `edge-target-${i}`));
  }
  const ids = db.batchUpsertNodes(nodes);
  const sourceIds = ids.slice(0, sourceCount);
  const targetIds = ids.slice(sourceCount);
  const sourceId = sourceIds[0];
  const segments = preloadEdges >= 2 ? 1 : 0;
  const segmentEdges = segments === 0 ? 0 : Math.max(1, Math.floor(preloadEdges / 2));
  const memtableTailEdges = Math.max(0, preloadEdges - segmentEdges);
  const makeEdges = (start, count) => Array.from({ length: count }, (_, offset) => {
    const i = start + offset;
    return {
      from: sourceIds[i % sourceCount],
      to: targetIds[i % targetIds.length],
      label: 'WORKS_AT',
      props: { role: i % 10 === 0 ? 'lead' : 'member', score: i % 100 },
      weight: i % 2 === 0 ? 2.0 : 0.5,
    };
  });
  if (segmentEdges > 0) {
    db.batchUpsertEdges(makeEdges(0, segmentEdges));
    db.flush();
  }
  if (memtableTailEdges > 0) {
    db.batchUpsertEdges(makeEdges(segmentEdges, memtableTailEdges));
  }

  const docs = [];
  for (let i = 0; i < targetCount; i += 8) {
    docs.push(nodeInput('Document', `doc-${i}`));
  }
  const docIds = docs.length ? db.batchUpsertNodes(docs) : [];
  if (docIds.length) {
    db.batchUpsertEdges(Array.from(docIds, (docId, docIndex) => ({
      from: targetIds[docIndex * 8],
      to: docId,
      label: 'MENTIONS',
      weight: 1.0,
    })));
  }

  const role = db.ensureEdgePropertyIndex('WORKS_AT', 'role', 'equality');
  waitForEdgePropertyIndexReady(db, role.indexId);
  const score = db.ensureEdgePropertyIndex('WORKS_AT', 'score', 'range');
  waitForEdgePropertyIndexReady(db, score.indexId);
  return {
    db,
    sourceId,
    layout: { segments, segment_edges: segmentEdges, memtable_tail_edges: memtableTailEdges },
  };
}

function graphRowOptionalRequest(sourceId, limit) {
  return {
    nodes: [
      { alias: 'source', labelFilter: nodeFilter('Person'), ids: [sourceId] },
      { alias: 'target', labelFilter: nodeFilter('Company') },
      { alias: 'doc', labelFilter: nodeFilter('Document') },
    ],
    pieces: [
      {
        kind: 'edge',
        alias: 'edge',
        fromAlias: 'source',
        toAlias: 'target',
        direction: 'outgoing',
        labelFilter: ['WORKS_AT'],
        filter: { property: 'role', eq: 'lead' },
      },
      {
        kind: 'optional',
        pieces: [
          {
            kind: 'edge',
            alias: 'ref',
            fromAlias: 'target',
            toAlias: 'doc',
            direction: 'outgoing',
            labelFilter: ['MENTIONS'],
          },
        ],
      },
    ],
    where: {
      op: '=',
      left: { property: { alias: 'edge', key: 'role' } },
      right: { param: 'role' },
    },
    params: { role: 'lead' },
    return: [
      { expr: { binding: 'source' }, as: 'source', projection: 'id' },
      { expr: { binding: 'edge' }, as: 'edge', projection: 'id' },
      { expr: { binding: 'target' }, as: 'target', projection: 'id' },
      { expr: { binding: 'ref' }, as: 'ref', projection: 'id' },
      { expr: { binding: 'doc' }, as: 'doc', projection: 'id' },
    ],
    orderBy: [
      { expr: { property: { alias: 'edge', key: 'score' } }, direction: 'desc' },
      { expr: { nodeField: { alias: 'target', field: 'id' } }, direction: 'asc' },
    ],
    limit,
  };
}

function graphRowScenarioParams(layout, preloadEdges, limit) {
  return {
    labels: {
      source: 'Person',
      target: 'Company',
      optional: 'Document',
    },
    edge_labels: {
      required: 'WORKS_AT',
      optional: 'MENTIONS',
    },
    preload_edges: preloadEdges,
    segments: layout.segments,
    segment_edges: layout.segment_edges,
    memtable_tail_edges: layout.memtable_tail_edges,
    predicate: 'edge_role_eq_lead_param',
    source_anchor: 'first_source_id',
    optional: 'target_mentions_document_sparse',
    row_ops: ['order_by_edge_score_desc', 'limit'],
    limit,
  };
}

const SCHEMA_SCENARIO_IDS = new Set([
  'S-SCHEMA-001',
  'S-SCHEMA-002',
  'S-SCHEMA-003',
  'S-SCHEMA-004',
]);

const GQL_SCHEMA_ALTER_ADD =
  "ALTER CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128 }";

const GQL_SCHEMA_CHECK_ADD =
  "CHECK CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128, max_violations: 4 }";

function schemaNameProps(i) {
  return { name: `name-${i}` };
}

function schemaRoleProps(i) {
  return { role: `role-${i}` };
}

function schemaNodeSchema() {
  return {
    properties: {
      name: { required: true, nullable: false, types: ['string'] },
    },
  };
}

function schemaEdgeSchema() {
  return {
    properties: {
      role: { required: true, nullable: false, types: ['string'] },
    },
    from: { allOf: ['SchemaPerson'] },
    to: { allOf: ['SchemaCompany'] },
  };
}

function schemaGraphOperations() {
  return [
    { kind: 'setNode', label: 'SchemaPerson', schema: schemaNodeSchema() },
    { kind: 'setEdge', label: 'SCHEMA_WORKS_AT', schema: schemaEdgeSchema() },
  ];
}

function seedSchemaPublishData(db) {
  const person = db.upsertNode('SchemaPerson', 'person-0', { props: schemaNameProps(0) });
  const company = db.upsertNode('SchemaCompany', 'company-0');
  db.upsertEdge(person, company, 'SCHEMA_WORKS_AT', { props: schemaRoleProps(0) });
}

function schemaPublishParams(operation) {
  return {
    api: operation.startsWith('gql_') ? 'gql' : 'native',
    operation,
    node_targets: ['SchemaPerson'],
    edge_targets: ['SCHEMA_WORKS_AT'],
    preload_nodes: 2,
    preload_edges: 1,
    chunk_size: 128,
  };
}

function schemaActiveWriteParams() {
  return {
    api: 'native',
    operation: 'upsert_node_active_schema',
    registered_node_schemas: ['SchemaPerson'],
    registered_edge_schemas: ['SCHEMA_WORKS_AT'],
    write_label: 'SchemaPerson',
    with_props: true,
  };
}

function pushGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios) {
  const scenarioId = 'S-QUERY-007';
  const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
  const { db, layout, sourceId } = buildGraphRowBenchmarkDb(
    join(tmpRoot, 'query-graph-rows-optional-edge'),
    preloadNodes
  );
  try {
    const request = graphRowOptionalRequest(sourceId, limit);
    const s = runBench(() => db.queryGraphRows(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_graph_rows_optional_edge_traversal',
        'query',
        s,
        iterCfg,
        graphRowScenarioParams(layout, preloadNodes, limit),
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
  } finally {
    db.close();
  }
}

function pushGqlGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios) {
  const scenarioId = 'S-GQL-006';
  const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
  const { db, layout, sourceId } = buildGraphRowBenchmarkDb(
    join(tmpRoot, 'gql-graph-row-optional-edge'),
    preloadNodes
  );
  const query = `MATCH (source:Person)-[edge:WORKS_AT {role: $role}]->(target:Company)
                 WHERE id(source) = $source
                 OPTIONAL MATCH (target)-[ref:MENTIONS]->(doc:Document)
                 RETURN id(source) AS source, id(edge) AS edge, id(target) AS target,
                        id(ref) AS ref, id(doc) AS doc
                 ORDER BY edge.score DESC, id(target) LIMIT ${limit}`;
  try {
    const s = runBench(
      () => db.executeGql(query, { role: 'lead', source: sourceId }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_optional_edge_traversal_graph_rows',
        'query',
        s,
        iterCfg,
        graphRowScenarioParams(layout, preloadNodes, limit),
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
  } finally {
    db.close();
  }
}

function pushSchemaScenarios(args, scenarioContract, tmpRoot, scenarios) {
  {
    const scenarioId = 'S-SCHEMA-001';
    if (scenarioSelected(args, scenarioId)) {
      const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
      const db = OverGraph.open(join(tmpRoot, 'schema-gql-alter-add'));
      try {
        seedSchemaPublishData(db);
        const s = runBenchWithSetup(
          () => db.dropGraphSchema(),
          () => {
            const result = db.executeGql(GQL_SCHEMA_ALTER_ADD);
            if (result.schemaStats?.targetsPublished !== 2) {
              throw new Error('GQL schema ALTER benchmark expected two published targets');
            }
          },
          iterCfg.warmup,
          iterCfg.iters
        );
        scenarios.push(
          scenario(
            scenarioId,
            'gql_schema_alter_add_existing_data',
            'schema',
            s,
            iterCfg,
            schemaPublishParams('gql_alter_current_graph_type_add'),
            scenarioComparability(scenarioContract, scenarioId)
          )
        );
      } finally {
        db.close();
      }
    }
  }

  {
    const scenarioId = 'S-SCHEMA-002';
    if (scenarioSelected(args, scenarioId)) {
      const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
      const db = OverGraph.open(join(tmpRoot, 'schema-native-bulk-add'));
      try {
        seedSchemaPublishData(db);
        const s = runBenchWithSetup(
          () => db.dropGraphSchema(),
          () => {
            const result = db.alterGraphSchema(schemaGraphOperations(), { chunkSize: 128 });
            if (result.targetsPublished !== 2) {
              throw new Error('bulk graph-schema benchmark expected two published targets');
            }
          },
          iterCfg.warmup,
          iterCfg.iters
        );
        scenarios.push(
          scenario(
            scenarioId,
            'bulk_graph_schema_add_existing_data',
            'schema',
            s,
            iterCfg,
            schemaPublishParams('alter_graph_schema_add'),
            scenarioComparability(scenarioContract, scenarioId)
          )
        );
      } finally {
        db.close();
      }
    }
  }

  {
    const scenarioId = 'S-SCHEMA-003';
    if (scenarioSelected(args, scenarioId)) {
      const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
      const db = OverGraph.open(join(tmpRoot, 'schema-active-upsert-node'));
      try {
        seedSchemaPublishData(db);
        db.alterGraphSchema(schemaGraphOperations(), { chunkSize: 128 });
        const s = runBench(
          (i) => db.upsertNode('SchemaPerson', `person-write-${i}`, { props: schemaNameProps(i) }),
          iterCfg.warmup,
          iterCfg.iters,
          true
        );
        scenarios.push(
          scenario(
            scenarioId,
            'upsert_node_active_schema',
            'schema',
            s,
            iterCfg,
            schemaActiveWriteParams(),
            scenarioComparability(scenarioContract, scenarioId)
          )
        );
      } finally {
        db.close();
      }
    }
  }

  {
    const scenarioId = 'S-SCHEMA-004';
    if (scenarioSelected(args, scenarioId)) {
      const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
      const db = OverGraph.open(join(tmpRoot, 'schema-gql-check-add'));
      try {
        seedSchemaPublishData(db);
        const s = runBench(
          () => {
            const result = db.executeGql(GQL_SCHEMA_CHECK_ADD);
            if (result.schemaStats?.violationCount !== 0) {
              throw new Error('GQL schema CHECK benchmark expected zero violations');
            }
          },
          iterCfg.warmup,
          iterCfg.iters
        );
        scenarios.push(
          scenario(
            scenarioId,
            'gql_schema_check_add_existing_data',
            'schema',
            s,
            iterCfg,
            {
              api: 'gql',
              operation: 'gql_check_current_graph_type_add',
              node_targets: ['SchemaPerson'],
              edge_targets: ['SCHEMA_WORKS_AT'],
              preload_nodes: 2,
              preload_edges: 1,
              chunk_size: 128,
              max_violations: 4,
            },
            scenarioComparability(scenarioContract, scenarioId)
          )
        );
      } finally {
        db.close();
      }
    }
  }
}

function pushQueryScenarios(args, scenarioContract, cfg, tmpRoot, scenarios) {
  const preloadNodes = cfg.time_range_nodes;
  const limit = 100;
  if (args.scenarioIds.length > 0) {
    const selectedScenarioIds = new Set(args.scenarioIds);
    const supportedScenarioIds = new Set(['S-QUERY-007', 'S-GQL-006', ...SCHEMA_SCENARIO_IDS]);
    const unsupported = [...selectedScenarioIds].filter((id) => !supportedScenarioIds.has(id));
    if (unsupported.length > 0) {
      throw new Error(
        `--scenario-id is currently limited to final graph-row and schema scenarios; unsupported: ${unsupported.sort().join(', ')}`
      );
    }
    if (selectedScenarioIds.has('S-QUERY-007')) {
      pushGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios);
    }
    if (selectedScenarioIds.has('S-GQL-006')) {
      pushGqlGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios);
    }
    return;
  }

  {
    const scenarioId = 'S-QUERY-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'query-node-ids-intersected'), preloadNodes);
    const request = {
      labelFilter: nodeFilter('Person'),
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
          label: 'Person',
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
      labelFilter: nodeFilter('Person'),
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
          label: 'Person',
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

  {
    const scenarioId = 'S-QUERY-003';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout, sourceId } = buildEdgeQueryBenchmarkDb(
      join(tmpRoot, 'query-edge-ids-endpoint-metadata'),
      preloadNodes
    );
    const request = {
      label: 'WORKS_AT',
      fromIds: [sourceId],
      filter: { weight: { gte: 1.0 } },
      limit,
    };
    const s = runBench(() => db.queryEdgeIds(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_edge_ids_endpoint_metadata',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          filter: 'weight_gte_1',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-QUERY-004';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout, sourceId } = buildEdgeQueryBenchmarkDb(
      join(tmpRoot, 'query-edges-endpoint-property-hydrated'),
      preloadNodes
    );
    const request = {
      label: 'WORKS_AT',
      fromIds: [sourceId],
      filter: {
        and: [
          { weight: { gte: 1.0 } },
          { property: 'role', eq: 'lead' },
        ],
      },
      limit,
    };
    const s = runBench(() => db.queryEdges(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_edges_endpoint_property_hydrated',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          filter: 'weight_gte_1_and_role_eq_lead',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-QUERY-005';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout, sourceId } = buildIndexedEdgeQueryBenchmarkDb(
      join(tmpRoot, 'query-edge-ids-property-indexed-equality'),
      preloadNodes
    );
    const request = {
      label: 'WORKS_AT',
      fromIds: [sourceId],
      filter: { property: 'role', eq: 'lead' },
      limit,
    };
    const s = runBench(() => db.queryEdgeIds(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_edge_ids_property_indexed_equality',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          filter: 'role_eq_lead',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-QUERY-006';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout, sourceId } = buildIndexedEdgeQueryBenchmarkDb(
      join(tmpRoot, 'query-edge-ids-property-indexed-range'),
      preloadNodes
    );
    const request = {
      label: 'WORKS_AT',
      fromIds: [sourceId],
      filter: { property: 'score', gte: 90 },
      limit,
    };
    const s = runBench(() => db.queryEdgeIds(request), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'query_edge_ids_property_indexed_range',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          filter: 'score_gte_90',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    pushGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios);
  }

  {
    const scenarioId = 'S-GQL-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'gql-node-row-ops'), preloadNodes);
    const params = { status: 'active' };
    const query = `MATCH (n:Person)
                   WHERE n.status = $status AND n.score >= 50
                   RETURN id(n) AS id, n.score AS score
                   ORDER BY n.score DESC LIMIT ${limit}`;
    const s = runBench(() => db.executeGql(query, params), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_node_residual_row_ops_object_rows',
        'query',
        s,
        iterCfg,
        {
          label: 'Person',
          preload_nodes: preloadNodes,
          segments: layout.segments,
          segment_nodes: layout.segment_nodes,
          memtable_tail_nodes: layout.memtable_tail_nodes,
          predicates: ['status_eq_active', 'score_gte_50'],
          row_ops: ['order_by', 'limit'],
          row_format: 'object',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    pushGqlGraphRowOptionalScenario(args, scenarioContract, tmpRoot, preloadNodes, limit, scenarios);
  }

  {
    const scenarioId = 'S-GQL-002';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'gql-node-compact-row-ops'), preloadNodes);
    const params = { status: 'active' };
    const query = `MATCH (n:Person)
                   WHERE n.status = $status AND n.score >= 50
                   RETURN id(n) AS id, n.score AS score
                   ORDER BY n.score DESC LIMIT ${limit}`;
    const s = runBench(() => db.executeGql(query, params, { compactRows: true }), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_node_residual_row_ops_compact_rows',
        'query',
        s,
        iterCfg,
        {
          label: 'Person',
          preload_nodes: preloadNodes,
          segments: layout.segments,
          segment_nodes: layout.segment_nodes,
          memtable_tail_nodes: layout.memtable_tail_nodes,
          predicates: ['status_eq_active', 'score_gte_50'],
          row_ops: ['order_by', 'limit'],
          row_format: 'compact',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-GQL-003';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildIndexedEdgeQueryBenchmarkDb(
      join(tmpRoot, 'gql-edge-property-indexed'),
      preloadNodes
    );
    const query = `MATCH ()-[r:WORKS_AT]->()
                   WHERE r.role = $role
                   RETURN id(r) AS id, r.score AS score
                   ORDER BY r.score DESC LIMIT ${limit}`;
    const s = runBench(() => db.executeGql(query, { role: 'lead' }), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_edge_property_indexed_row_ops',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          predicate: 'role_eq_lead',
          row_ops: ['order_by', 'limit'],
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-GQL-004';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildIndexedEdgeQueryBenchmarkDb(
      join(tmpRoot, 'gql-pattern-property-anchor-indexed'),
      preloadNodes
    );
    const query = `MATCH (source:Person)-[edge:WORKS_AT]->(target:Company)
                   WHERE edge.role = $role
                   RETURN id(source) AS source, id(edge) AS edge, id(target) AS target
                   LIMIT ${limit}`;
    const s = runBench(() => db.executeGql(query, { role: 'lead' }), iterCfg.warmup, iterCfg.iters);
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_fixed_pattern_property_anchor',
        'query',
        s,
        iterCfg,
        {
          label: 'WORKS_AT',
          preload_edges: preloadNodes,
          segments: layout.segments,
          segment_edges: layout.segment_edges,
          memtable_tail_edges: layout.memtable_tail_edges,
          predicate: 'role_eq_lead',
          limit,
        },
        scenarioComparability(scenarioContract, scenarioId)
      )
    );
    db.close();
  }

  {
    const scenarioId = 'S-GQL-005';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const { db, layout } = buildQueryBenchmarkDb(join(tmpRoot, 'gql-explain-profile'), preloadNodes);
    const query = `MATCH (n:Person)
                   WHERE n.status = $status
                   RETURN n.score AS score
                   ORDER BY n.score DESC LIMIT ${limit}`;
    const s = runBench(
      () => db.executeGql(query, { status: 'active' }, { includePlan: true, profile: true }),
      iterCfg.warmup,
      iterCfg.iters
    );
    scenarios.push(
      scenario(
        scenarioId,
        'execute_gql_include_plan_profile',
        'query',
        s,
        iterCfg,
        {
          label: 'Person',
          preload_nodes: preloadNodes,
          segments: layout.segments,
          segment_nodes: layout.segment_nodes,
          memtable_tail_nodes: layout.memtable_tail_nodes,
          predicate: 'status_eq_active',
          include_plan: true,
          profile: true,
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
  if (
    (args.scenarioSet === 'all' && args.scenarioIds.length === 0) ||
    args.scenarioIds.some((id) => SCHEMA_SCENARIO_IDS.has(id))
  ) {
    pushSchemaScenarios(args, scenarioContract, tmpRoot, scenarios);
  }

  if (args.scenarioSet === 'all' && args.scenarioIds.length === 0) {
  // S-CRUD-001: single upsert node (growth)
  {
    const scenarioId = 'S-CRUD-001';
    const iterCfg = scenarioIterations(args, scenarioContract, scenarioId);
    const db = OverGraph.open(join(tmpRoot, 'crud-upsert-node'));
    const s = runBench(
      (i) => db.upsertNode('Person', `node-${i}`, { props: { idx: i }, weight: 1.0 }),
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
        { label: 'Person', with_props: true, weight: 1.0 },
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
      Array.from({ length: iterCfg.warmup + iterCfg.iters + 1 }, (_, i) => nodeInput('Person', `e-${i}`))
    );
    const s = runBench(
      (i) => db.upsertEdge(nodeIds[i], nodeIds[i + 1], 'LINKS_TO', { weight: 1.0 }),
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
        { label: 'LINKS_TO', weight: 1.0 },
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
          ...nodeInput('Person', `bn-${i}-${j}`, { props: { idx: j }, weight: 1.0 }),
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
        { batch_nodes: cfg.batch_nodes, label: 'Person', with_props: true },
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
          ...nodeInput('Person', `bb-${i}-${j}`, { props: { idx: j }, weight: 1.0 }),
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
        ...nodeInput('Person', `gn-${i}`, { props: { idx: i } }),
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
    db.upsertNode('Person', 'fixed-node', { props: { idx: 0 }, weight: 1.0 });
    const s = runBench(
      (i) => db.upsertNode('Person', 'fixed-node', { props: { idx: i }, weight: 1.0 }),
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
        { label: 'Person', with_props: true, weight: 1.0, fixed_key: true },
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
    const nodeA = db.upsertNode('Person', 'fixed-a');
    const nodeB = db.upsertNode('Person', 'fixed-b');
    const s = runBench(
      () => db.upsertEdge(nodeA, nodeB, 'LINKS_TO', { weight: 1.0 }),
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
        { label: 'LINKS_TO', weight: 1.0, edge_uniqueness: true, fixed_triple: true },
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
      nodeInput('Person', 'hub'),
      ...Array.from({ length: cfg.fanout }, (_, i) => nodeInput('Person', `n-${i}`)),
    ]);
    const hub = neighNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.fanout }, (_, i) => ({
        from: hub,
        to: neighNodeIds[i + 1],
        label: 'LINKS_TO',
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
          node_label_filter: null,
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
          node_label_filter: null,
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
      () => db.traverse(root, 3, { direction: 'outgoing', emitNodeLabelFilter: nodeFilter('Company') }),
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
          node_label_filter: ['Company'],
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
      () => db.traverse(root, 3, { direction: 'outgoing', emitNodeLabelFilter: nodeFilter('Company') }),
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
          node_label_filter: ['Company'],
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
      nodeInput('Person', 'hub'),
      ...Array.from({ length: cfg.fanout }, (_, i) => nodeInput('Person', `d-${i}`)),
    ]);
    const hub = degNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.fanout }, (_, i) => ({
        from: hub,
        to: degNodeIds[i + 1],
        label: 'LINKS_TO',
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
      allNodes.push(nodeInput('Person', `hub-${h}`));
      for (let i = 0; i < cfg.fanout; i++) {
        allNodes.push(nodeInput('Person', `dt-${h}-${i}`));
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
        degEdges.push({ from: hubId, to: allNodeIds[h * stride + 1 + i], label: 'LINKS_TO', weight: 1.0 });
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
      Array.from({ length: cfg.shortest_path_nodes }, (_, i) => nodeInput('Person', `sp-${i}`))
    );
    const spEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.shortest_path_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.shortest_path_edge_offsets[1]) % ids.length];
      spEdges.push({ from, to: to1, label: 'LINKS_TO', weight: 1.0 });
      spEdges.push({ from, to: to2, label: 'LINKS_TO', weight: 1.0 });
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
      Array.from({ length: cfg.shortest_path_nodes }, (_, i) => nodeInput('Person', `ic-${i}`))
    );
    const icEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.shortest_path_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.shortest_path_edge_offsets[1]) % ids.length];
      icEdges.push({ from, to: to1, label: 'LINKS_TO', weight: 1.0 });
      icEdges.push({ from, to: to2, label: 'LINKS_TO', weight: 1.0 });
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
      nodeInput('Person', 'hub'),
      ...Array.from({ length: cfg.top_k_candidates }, (_, i) => ({
        ...nodeInput('Person', `tk-${i}`),
      })),
    ]);
    const hub = topKNodeIds[0];
    db.batchUpsertEdges(
      Array.from({ length: cfg.top_k_candidates }, (_, i) => ({
        from: hub,
        to: topKNodeIds[i + 1],
        label: 'LINKS_TO',
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
        ...nodeInput('Person', `tr-${i}`, { props: { idx: i }, weight: 1.0 }),
      }))
    );
    const fromMs = cfg.time_range_from_ms;
    const toMs = Date.now() + cfg.time_range_window_ms;
    const s = runBench(
      () => db.findNodesByTimeRange('Person', fromMs, toMs),
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
          label: 'Person',
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
      Array.from({ length: cfg.ppr_nodes }, (_, i) => nodeInput('Person', `ppr-${i}`))
    );
    const pprEdges = [];
    for (let i = 0; i < ids.length; i++) {
      const from = ids[i];
      const to1 = ids[(i + cfg.ppr_edge_offsets[0]) % ids.length];
      const to2 = ids[(i + cfg.ppr_edge_offsets[1]) % ids.length];
      pprEdges.push({ from, to: to1, label: 'LINKS_TO', weight: 1.0 });
      pprEdges.push({ from, to: to2, label: 'LINKS_TO', weight: 0.7 });
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
      Array.from({ length: cfg.export_nodes }, (_, i) => nodeInput('Person', `ex-${i}`))
    );
    const exportEdges = [];
    for (let i = 0; i < cfg.export_edges; i++) {
      const from = ids[i % ids.length];
      const to = ids[(i * 13 + 7) % ids.length];
      if (from !== to) exportEdges.push({ from, to, label: 'LINKS_TO', weight: 1.0 });
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
          ...nodeInput('Person', `fl-${i}-${j}`, { props: { idx: j }, weight: 1.0 }),
        }));
        const ids = db.batchUpsertNodes(nodes);
        const edges = [];
        for (let j = 0; j < Math.min(cfg.flush_edges_per_iter_cap, ids.length - 1); j++) {
          edges.push({ from: ids[j], to: ids[j + 1], label: 'LINKS_TO', weight: 1.0 });
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
        labels: ['Person'],
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
