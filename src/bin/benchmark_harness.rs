use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeInput, ExportOptions, NodeInput, PprOptions,
    PropValue, ScoringMode, WalSyncMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const PROFILE_PATH: &str = "docs/04-quality/workloads/profiles.json";
const SCENARIO_CONTRACT_PATH: &str = "docs/04-quality/workloads/scenario-contract.json";

#[derive(Debug)]
struct CliArgs {
    profile: String,
    warmup: usize,
    iters: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProfileBatchSizes {
    nodes: usize,
    edges: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProfileConfig {
    nodes: usize,
    edges: usize,
    average_degree_target: usize,
    batch_sizes: ProfileBatchSizes,
}

#[derive(Debug, Deserialize)]
struct ProfilesPayload {
    determinism: Value,
    profiles: HashMap<String, ProfileConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct EffectiveConfigContract {
    nodes_divisor: usize,
    nodes_min: usize,
    edges_divisor: usize,
    edges_min: usize,
    fanout_min: usize,
    fanout_max: usize,
    fanout_degree_multiplier: usize,
    batch_nodes_min: usize,
    batch_edges_min: usize,
    two_hop_mid_min: usize,
    two_hop_leaves_per_mid: usize,
    top_k_candidates_min: usize,
    top_k_candidates_divisor: usize,
    ppr_nodes_min: usize,
    ppr_nodes_divisor: usize,
    time_range_nodes_cap: usize,
    export_nodes_cap: usize,
    export_edges_cap: usize,
    flush_node_batch_cap: usize,
    flush_edge_chain_cap: usize,
    ppr_max_iterations: u32,
    ppr_max_results: usize,
    ppr_seed_count: usize,
    ppr_edge_offsets: Vec<usize>,
    top_k_limit: usize,
    time_range_from_ms: i64,
    time_range_window_ms: i64,
    include_weights_on_export: bool,
    shortest_path_nodes_min: usize,
    shortest_path_nodes_divisor: usize,
    shortest_path_edge_offsets: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct IterPolicyContract {
    warmup_divisor: Option<usize>,
    warmup_min: Option<usize>,
    iters_divisor: Option<usize>,
    iters_min: Option<usize>,
    iters_multiplier: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ComparabilityContract {
    status: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScenarioContract {
    schema_version: u64,
    effective_config: EffectiveConfigContract,
    scenario_iteration_policy: HashMap<String, IterPolicyContract>,
    comparability: HashMap<String, ComparabilityContract>,
    percentile_method: Value,
}

#[derive(Debug, Clone, Serialize)]
struct EffectiveConfigResolved {
    nodes: usize,
    edges: usize,
    fanout: usize,
    batch_nodes: usize,
    batch_edges: usize,
    two_hop_mid: usize,
    two_hop_leaves_per_mid: usize,
    top_k_candidates: usize,
    ppr_nodes: usize,
    get_node_nodes: usize,
    time_range_nodes: usize,
    export_nodes: usize,
    export_edges: usize,
    flush_nodes_per_iter: usize,
    flush_edges_per_iter_cap: usize,
    ppr_max_iterations: u32,
    ppr_max_results: usize,
    ppr_seed_count: usize,
    ppr_edge_offsets: Vec<usize>,
    top_k_limit: usize,
    time_range_from_ms: i64,
    time_range_window_ms: i64,
    include_weights_on_export: bool,
    shortest_path_nodes: usize,
    shortest_path_edge_offsets: Vec<usize>,
}

#[derive(Debug, Clone, Copy)]
struct IterConfig {
    warmup: usize,
    iters: usize,
}

#[derive(Debug, Clone, Serialize)]
struct Stats {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    min_us: f64,
    max_us: f64,
    mean_us: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    early_p95_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    late_p95_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drift_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ComparabilityOutput {
    status: String,
    reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ScenarioOutput {
    scenario_id: String,
    name: String,
    category: String,
    warmup_iterations: usize,
    benchmark_iterations: usize,
    ops_per_iteration: usize,
    throughput_ops_per_sec: Option<f64>,
    stats: Stats,
    scenario_params: Value,
    comparability: ComparabilityOutput,
    notes: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProfileContractOutput {
    determinism: Value,
    profile: ProfileConfig,
    effective_config: EffectiveConfigResolved,
    scenario_contract_schema_version: u64,
}

#[derive(Debug, Serialize)]
struct HarnessOutput {
    schema_version: u32,
    language: &'static str,
    harness_stage: &'static str,
    profile_name: String,
    generated_at_utc: String,
    profile_source: String,
    scenario_contract_source: String,
    percentile_method: Value,
    profile_contract: ProfileContractOutput,
    scenarios: Vec<ScenarioOutput>,
}

struct TempBenchDir {
    path: PathBuf,
}

impl TempBenchDir {
    fn new(profile: &str) -> Result<Self, String> {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "overgraph-rust-bench-{}-{}-{}",
            profile,
            std::process::id(),
            now_nanos
        ));
        fs::create_dir_all(&path).map_err(|e| format!("create temp dir failed: {e}"))?;
        Ok(Self { path })
    }

    fn db_path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TempBenchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    let profiles_payload: ProfilesPayload = serde_json::from_str(
        &fs::read_to_string(PROFILE_PATH)
            .map_err(|e| format!("read {PROFILE_PATH} failed: {e}"))?,
    )
    .map_err(|e| format!("parse {PROFILE_PATH} failed: {e}"))?;

    let profile = profiles_payload
        .profiles
        .get(&args.profile)
        .cloned()
        .ok_or_else(|| format!("unknown profile '{}'", args.profile))?;

    let scenario_contract: ScenarioContract = serde_json::from_str(
        &fs::read_to_string(SCENARIO_CONTRACT_PATH)
            .map_err(|e| format!("read {SCENARIO_CONTRACT_PATH} failed: {e}"))?,
    )
    .map_err(|e| format!("parse {SCENARIO_CONTRACT_PATH} failed: {e}"))?;

    let cfg = effective_config(&profile, &scenario_contract.effective_config);
    let tmp_root = TempBenchDir::new(&args.profile)?;
    let mut scenarios: Vec<ScenarioOutput> = Vec::new();

    // S-CRUD-001: upsert_node (growth)
    {
        let scenario_id = "S-CRUD-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("crud-upsert-node"))?;
        let stats = run_bench_growth(iter_cfg, |i| {
            engine
                .upsert_node(1, &format!("node-{i}"), idx_props(i), 1.0)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "upsert_node",
            "crud",
            iter_cfg,
            1,
            stats,
            json!({"type_id": 1, "with_props": true, "weight": 1.0}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-002: upsert_edge (growth)
    {
        let scenario_id = "S-CRUD-002";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("crud-upsert-edge"))?;
        let node_inputs: Vec<NodeInput> = (0..(iter_cfg.warmup + iter_cfg.iters + 1))
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("e-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let stats = run_bench_growth(iter_cfg, |i| {
            engine
                .upsert_edge(
                    node_ids[i],
                    node_ids[i + 1],
                    1,
                    BTreeMap::new(),
                    1.0,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "upsert_edge",
            "crud",
            iter_cfg,
            1,
            stats,
            json!({"edge_type_id": 1, "weight": 1.0}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-BATCH-001: batch_upsert_nodes_json
    {
        let scenario_id = "S-BATCH-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("batch-nodes-json"))?;
        let stats = run_bench(iter_cfg, |i| {
            let inputs: Vec<NodeInput> = (0..cfg.batch_nodes)
                .map(|j| NodeInput {
                    type_id: 1,
                    key: format!("bn-{i}-{j}"),
                    props: idx_props(j),
                    weight: 1.0,
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "batch_upsert_nodes_json",
            "batch",
            iter_cfg,
            cfg.batch_nodes,
            stats,
            json!({"batch_nodes": cfg.batch_nodes, "type_id": 1, "with_props": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-003: get_node
    {
        let scenario_id = "S-CRUD-003";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("crud-get-node"))?;
        let node_inputs: Vec<NodeInput> = (0..cfg.get_node_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("gn-{i}"),
                props: idx_props(i),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let stats = run_bench(iter_cfg, |i| {
            let idx = i % node_ids.len();
            engine.get_node(node_ids[idx]).map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "get_node",
            "crud",
            iter_cfg,
            1,
            stats,
            json!({"preload_nodes": cfg.get_node_nodes}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-004: upsert_node_fixed_key
    {
        let scenario_id = "S-CRUD-004";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("crud-upsert-node-fixed"))?;
        engine
            .upsert_node(1, "fixed-node", idx_props(0), 1.0)
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |i| {
            engine
                .upsert_node(1, "fixed-node", idx_props(i), 1.0)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "upsert_node_fixed_key",
            "crud",
            iter_cfg,
            1,
            stats,
            json!({"type_id": 1, "with_props": true, "weight": 1.0, "fixed_key": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-005: upsert_edge_fixed_triple
    {
        let scenario_id = "S-CRUD-005";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut opts = benchmark_db_options();
        opts.edge_uniqueness = true;
        let mut engine = DatabaseEngine::open(&tmp_root.db_path("crud-upsert-edge-fixed"), &opts)
            .map_err(|e| e.to_string())?;
        let node_a = engine
            .upsert_node(1, "fixed-a", BTreeMap::new(), 1.0)
            .map_err(|e| e.to_string())?;
        let node_b = engine
            .upsert_node(1, "fixed-b", BTreeMap::new(), 1.0)
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .upsert_edge(node_a, node_b, 1, BTreeMap::new(), 1.0, None, None)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "upsert_edge_fixed_triple",
            "crud",
            iter_cfg,
            1,
            stats,
            json!({"edge_type_id": 1, "weight": 1.0, "edge_uniqueness": true, "fixed_triple": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-001: neighbors
    {
        let scenario_id = "S-TRAV-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-neighbors"))?;
        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        node_inputs.extend((0..cfg.fanout).map(|i| NodeInput {
            type_id: 1,
            key: format!("n-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
        }));
        let ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = ids[1..]
            .iter()
            .map(|&n| EdgeInput {
                from: hub,
                to: n,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .neighbors(hub, Direction::Outgoing, None, 0, None, None)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "neighbors",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({"fanout": cfg.fanout, "direction": "outgoing"}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-002: traverse depth-2 slice
    {
        let scenario_id = "S-TRAV-002";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-neighbors-2hop"))?;
        let root = build_depth_two_traversal_graph(&mut engine, &cfg)?;

        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    2,
                    2,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "traverse_depth_2",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "min_depth": 2,
                "max_depth": 2,
                "mid_nodes": cfg.two_hop_mid,
                "leaves_per_mid": cfg.two_hop_leaves_per_mid
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-007: deeper traverse, memtable, fast path
    {
        let scenario_id = "S-TRAV-007";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-depth13-memtable"))?;
        let (root, level1, level2, level3) = build_deep_traversal_graph(&mut engine, cfg.fanout)?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "traverse_depth_1_to_3",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "layout": "memtable",
                "min_depth": 1,
                "max_depth": 3,
                "node_type_filter": null,
                "branching": [level1, level2, level3]
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-008: deeper traverse, segmented, fast path
    {
        let scenario_id = "S-TRAV-008";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-depth13-segment"))?;
        let (root, level1, level2, level3) = build_deep_traversal_graph(&mut engine, cfg.fanout)?;
        engine.flush().map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "traverse_depth_1_to_3_segment",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "layout": "segment",
                "min_depth": 1,
                "max_depth": 3,
                "node_type_filter": null,
                "branching": [level1, level2, level3]
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-009: deeper traverse, memtable, emission-filtered path
    {
        let scenario_id = "S-TRAV-009";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-depth13-filtered-memtable"))?;
        let (root, level1, level2, level3) = build_deep_traversal_graph(&mut engine, cfg.fanout)?;
        let node_type_filter = [2u32];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    Some(&node_type_filter),
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "traverse_depth_1_to_3_filtered",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "layout": "memtable",
                "min_depth": 1,
                "max_depth": 3,
                "node_type_filter": [2],
                "branching": [level1, level2, level3]
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-010: deeper traverse, segmented, emission-filtered path
    {
        let scenario_id = "S-TRAV-010";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-depth13-filtered-segment"))?;
        let (root, level1, level2, level3) = build_deep_traversal_graph(&mut engine, cfg.fanout)?;
        engine.flush().map_err(|e| e.to_string())?;
        let node_type_filter = [2u32];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    Some(&node_type_filter),
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "traverse_depth_1_to_3_filtered_segment",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "layout": "segment",
                "min_depth": 1,
                "max_depth": 3,
                "node_type_filter": [2],
                "branching": [level1, level2, level3]
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-003: degree (scalar)
    {
        let scenario_id = "S-TRAV-003";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-degree"))?;
        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        node_inputs.extend((0..cfg.fanout).map(|i| NodeInput {
            type_id: 1,
            key: format!("d-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
        }));
        let ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = ids[1..]
            .iter()
            .map(|&n| EdgeInput {
                from: hub,
                to: n,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .degree(hub, Direction::Outgoing, None, None)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "degree",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({"fanout": cfg.fanout, "direction": "outgoing"}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-004: degrees (batch)
    {
        let scenario_id = "S-TRAV-004";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-degrees"))?;
        let mut node_inputs: Vec<NodeInput> =
            Vec::with_capacity(cfg.batch_nodes * (1 + cfg.fanout));
        for h in 0..cfg.batch_nodes {
            node_inputs.push(NodeInput {
                type_id: 1,
                key: format!("hub-{h}"),
                props: BTreeMap::new(),
                weight: 1.0,
            });
            for i in 0..cfg.fanout {
                node_inputs.push(NodeInput {
                    type_id: 1,
                    key: format!("dt-{h}-{i}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                });
            }
        }
        let all_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;
        let stride = 1 + cfg.fanout;
        let hub_ids: Vec<u64> = (0..cfg.batch_nodes).map(|h| all_ids[h * stride]).collect();
        let mut edge_inputs = Vec::with_capacity(cfg.batch_nodes * cfg.fanout);
        for h in 0..cfg.batch_nodes {
            let hub = all_ids[h * stride];
            for i in 0..cfg.fanout {
                let spoke = all_ids[h * stride + 1 + i];
                edge_inputs.push(EdgeInput {
                    from: hub,
                    to: spoke,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .degrees(&hub_ids, Direction::Outgoing, None, None)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "degrees",
            "traversal",
            iter_cfg,
            cfg.batch_nodes,
            stats,
            json!({"batch_nodes": cfg.batch_nodes, "fanout": cfg.fanout, "direction": "outgoing"}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-005: shortest_path (BFS)
    {
        let scenario_id = "S-TRAV-005";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-shortest-path"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.shortest_path_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("sp-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let offset_a = *cfg
            .shortest_path_edge_offsets
            .first()
            .ok_or_else(|| "shortest_path_edge_offsets missing first value".to_string())?;
        let offset_b = *cfg
            .shortest_path_edge_offsets
            .get(1)
            .ok_or_else(|| "shortest_path_edge_offsets missing second value".to_string())?;
        let edge_inputs: Vec<EdgeInput> = (0..node_ids.len())
            .flat_map(|i| {
                let from = node_ids[i];
                let to1 = node_ids[(i + offset_a) % node_ids.len()];
                let to2 = node_ids[(i + offset_b) % node_ids.len()];
                [
                    EdgeInput {
                        from,
                        to: to1,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;

        let sp_from = node_ids[0];
        let sp_to = node_ids[node_ids.len() / 2];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .shortest_path(
                    sp_from,
                    sp_to,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "shortest_path",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "shortest_path_nodes": cfg.shortest_path_nodes,
                "edge_offsets": cfg.shortest_path_edge_offsets,
                "direction": "outgoing",
                "weight_field": null
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-006: is_connected
    {
        let scenario_id = "S-TRAV-006";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("trav-is-connected"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.shortest_path_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("ic-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let offset_a = *cfg
            .shortest_path_edge_offsets
            .first()
            .ok_or_else(|| "shortest_path_edge_offsets missing first value".to_string())?;
        let offset_b = *cfg
            .shortest_path_edge_offsets
            .get(1)
            .ok_or_else(|| "shortest_path_edge_offsets missing second value".to_string())?;
        let edge_inputs: Vec<EdgeInput> = (0..node_ids.len())
            .flat_map(|i| {
                let from = node_ids[i];
                let to1 = node_ids[(i + offset_a) % node_ids.len()];
                let to2 = node_ids[(i + offset_b) % node_ids.len()];
                [
                    EdgeInput {
                        from,
                        to: to1,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;

        let sp_from = node_ids[0];
        let sp_to = node_ids[node_ids.len() / 2];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .is_connected(sp_from, sp_to, Direction::Outgoing, None, None, None)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "is_connected",
            "traversal",
            iter_cfg,
            1,
            stats,
            json!({
                "shortest_path_nodes": cfg.shortest_path_nodes,
                "edge_offsets": cfg.shortest_path_edge_offsets,
                "direction": "outgoing"
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-ADV-001: top_k_neighbors
    {
        let scenario_id = "S-ADV-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("adv-top-k"))?;
        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        node_inputs.extend((0..cfg.top_k_candidates).map(|i| NodeInput {
            type_id: 1,
            key: format!("tk-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
        }));
        let ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = ids[1..]
            .iter()
            .enumerate()
            .map(|(i, &n)| {
                let weight = 1.0 + ((i % 100) as f32 / 10.0);
                EdgeInput {
                    from: hub,
                    to: n,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight,
                    valid_from: None,
                    valid_to: None,
                }
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;

        let stats = run_bench(iter_cfg, |_i| {
            engine
                .top_k_neighbors(
                    hub,
                    Direction::Outgoing,
                    None,
                    cfg.top_k_limit,
                    ScoringMode::Weight,
                    None,
                )
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "top_k_neighbors",
            "advanced",
            iter_cfg,
            1,
            stats,
            json!({
                "direction": "outgoing",
                "k": cfg.top_k_limit,
                "scoring": "weight",
                "candidate_nodes": cfg.top_k_candidates
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-ADV-003: find_nodes_by_time_range
    {
        let scenario_id = "S-ADV-003";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("adv-time-range"))?;
        let node_inputs: Vec<NodeInput> = (0..cfg.time_range_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("tr-{i}"),
                props: idx_props(i),
                weight: 1.0,
            })
            .collect();
        engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let to_ms = now_millis() + cfg.time_range_window_ms;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .find_nodes_by_time_range(1, cfg.time_range_from_ms, to_ms)
                .map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "find_nodes_by_time_range",
            "advanced",
            iter_cfg,
            1,
            stats,
            json!({
                "type_id": 1,
                "preload_nodes": cfg.time_range_nodes,
                "from_ms": cfg.time_range_from_ms,
                "to_ms_window": cfg.time_range_window_ms
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-ADV-004: personalized_pagerank
    {
        let scenario_id = "S-ADV-004";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("adv-ppr"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.ppr_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("ppr-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let offset_a = *cfg
            .ppr_edge_offsets
            .first()
            .ok_or_else(|| "ppr_edge_offsets missing first value".to_string())?;
        let offset_b = *cfg
            .ppr_edge_offsets
            .get(1)
            .ok_or_else(|| "ppr_edge_offsets missing second value".to_string())?;
        let edge_inputs: Vec<EdgeInput> = (0..node_ids.len())
            .flat_map(|i| {
                let from = node_ids[i];
                let to1 = node_ids[(i + offset_a) % node_ids.len()];
                let to2 = node_ids[(i + offset_b) % node_ids.len()];
                [
                    EdgeInput {
                        from,
                        to: to1,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 0.7,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;

        let seeds: Vec<u64> = node_ids
            .iter()
            .take(cfg.ppr_seed_count.max(1))
            .copied()
            .collect();

        let ppr_opts = PprOptions {
            max_iterations: cfg.ppr_max_iterations,
            max_results: Some(cfg.ppr_max_results),
            ..PprOptions::default()
        };
        let stats = run_bench(iter_cfg, |_i| {
            engine.personalized_pagerank(&seeds, &ppr_opts).map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "personalized_pagerank",
            "advanced",
            iter_cfg,
            1,
            stats,
            json!({
                "ppr_nodes": cfg.ppr_nodes,
                "seed_strategy": "first_node_id",
                "seed_count": cfg.ppr_seed_count,
                "edge_offsets": cfg.ppr_edge_offsets,
                "max_iterations": cfg.ppr_max_iterations,
                "max_results": cfg.ppr_max_results
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-ADV-005: export_adjacency
    {
        let scenario_id = "S-ADV-005";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("adv-export"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.export_nodes)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("ex-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(&node_inputs)
            .map_err(|e| e.to_string())?;

        let edge_inputs: Vec<EdgeInput> = (0..cfg.export_edges)
            .filter_map(|i| {
                let from = node_ids[i % node_ids.len()];
                let to = node_ids[(i * 13 + 7) % node_ids.len()];
                if from != to {
                    Some(EdgeInput {
                        from,
                        to,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    })
                } else {
                    None
                }
            })
            .collect();
        engine
            .batch_upsert_edges(&edge_inputs)
            .map_err(|e| e.to_string())?;

        let export_opts = ExportOptions {
            include_weights: cfg.include_weights_on_export,
            ..ExportOptions::default()
        };
        let stats = run_bench(iter_cfg, |_i| {
            engine.export_adjacency(&export_opts).map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "export_adjacency",
            "advanced",
            iter_cfg,
            1,
            stats,
            json!({
                "preload_nodes": cfg.export_nodes,
                "preload_edges": cfg.export_edges,
                "include_weights": cfg.include_weights_on_export
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-MAIN-001: flush
    {
        let scenario_id = "S-MAIN-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut engine = open_db(&tmp_root.db_path("maint-flush"))?;
        let stats = run_bench(iter_cfg, |i| {
            let nodes: Vec<NodeInput> = (0..cfg.flush_nodes_per_iter)
                .map(|j| NodeInput {
                    type_id: 1,
                    key: format!("fl-{i}-{j}"),
                    props: idx_props(j),
                    weight: 1.0,
                })
                .collect();
            let node_ids = engine.batch_upsert_nodes(&nodes)?;

            let mut edges = Vec::new();
            let edge_count = cfg
                .flush_edges_per_iter_cap
                .min(node_ids.len().saturating_sub(1));
            for j in 0..edge_count {
                edges.push(EdgeInput {
                    from: node_ids[j],
                    to: node_ids[j + 1],
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
            engine.batch_upsert_edges(&edges)?;
            engine.flush().map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "flush",
            "maintenance",
            iter_cfg,
            1,
            stats,
            json!({
                "nodes_per_iter": cfg.flush_nodes_per_iter,
                "edge_chain_cap": cfg.flush_edges_per_iter_cap
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    let output = HarnessOutput {
        schema_version: 1,
        language: "rust",
        harness_stage: "core-benchmark-v1-parity",
        profile_name: args.profile,
        generated_at_utc: now_iso_utc_string(),
        profile_source: PROFILE_PATH.to_string(),
        scenario_contract_source: SCENARIO_CONTRACT_PATH.to_string(),
        percentile_method: scenario_contract.percentile_method,
        profile_contract: ProfileContractOutput {
            determinism: profiles_payload.determinism,
            profile,
            effective_config: cfg,
            scenario_contract_schema_version: scenario_contract.schema_version,
        },
        scenarios,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&output)
            .map_err(|e| format!("serialize benchmark output failed: {e}"))?
    );

    Ok(())
}

fn parse_args() -> Result<CliArgs, String> {
    let mut profile = String::from("small");
    let mut warmup: usize = 20;
    let mut iters: usize = 80;

    let mut args = env::args().skip(1);
    while let Some(token) = args.next() {
        match token.as_str() {
            "--profile" => {
                profile = args
                    .next()
                    .ok_or_else(|| "--profile requires a value".to_string())?;
            }
            "--warmup" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--warmup requires a value".to_string())?;
                warmup = raw
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --warmup: {e}"))?;
            }
            "--iters" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--iters requires a value".to_string())?;
                iters = raw
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --iters: {e}"))?;
            }
            "--help" | "-h" => {
                return Err(help_text());
            }
            _ => {
                return Err(format!("unknown arg: {token}\n{}", help_text()));
            }
        }
    }

    if !matches!(profile.as_str(), "small" | "medium" | "large" | "xlarge") {
        return Err(format!("unsupported profile '{profile}'\n{}", help_text()));
    }
    if warmup == 0 || iters == 0 {
        return Err("--warmup and --iters must be > 0".to_string());
    }

    Ok(CliArgs {
        profile,
        warmup,
        iters,
    })
}

fn help_text() -> String {
    "Usage: cargo run --release --bin benchmark_harness -- --profile <small|medium|large|xlarge> --warmup <n> --iters <n>".to_string()
}

fn effective_config(
    profile: &ProfileConfig,
    cfg: &EffectiveConfigContract,
) -> EffectiveConfigResolved {
    let nodes = cfg.nodes_min.max(profile.nodes / cfg.nodes_divisor.max(1));
    let edges = cfg.edges_min.max(profile.edges / cfg.edges_divisor.max(1));
    let fanout = cfg.fanout_max.min(
        cfg.fanout_min
            .max(profile.average_degree_target * cfg.fanout_degree_multiplier),
    );
    let batch_nodes = cfg.batch_nodes_min.max(profile.batch_sizes.nodes);
    let batch_edges = cfg.batch_edges_min.max(profile.batch_sizes.edges);
    let two_hop_mid = cfg.two_hop_mid_min.max(fanout);

    EffectiveConfigResolved {
        nodes,
        edges,
        fanout,
        batch_nodes,
        batch_edges,
        two_hop_mid,
        two_hop_leaves_per_mid: cfg.two_hop_leaves_per_mid,
        top_k_candidates: cfg
            .top_k_candidates_min
            .max(nodes / cfg.top_k_candidates_divisor.max(1)),
        ppr_nodes: cfg.ppr_nodes_min.max(nodes / cfg.ppr_nodes_divisor.max(1)),
        get_node_nodes: nodes.min(cfg.time_range_nodes_cap),
        time_range_nodes: nodes.min(cfg.time_range_nodes_cap),
        export_nodes: nodes.min(cfg.export_nodes_cap),
        export_edges: edges.min(cfg.export_edges_cap),
        flush_nodes_per_iter: batch_nodes.min(cfg.flush_node_batch_cap),
        flush_edges_per_iter_cap: cfg.flush_edge_chain_cap,
        ppr_max_iterations: cfg.ppr_max_iterations,
        ppr_max_results: cfg.ppr_max_results,
        ppr_seed_count: cfg.ppr_seed_count,
        ppr_edge_offsets: cfg.ppr_edge_offsets.clone(),
        top_k_limit: cfg.top_k_limit,
        time_range_from_ms: cfg.time_range_from_ms,
        time_range_window_ms: cfg.time_range_window_ms,
        include_weights_on_export: cfg.include_weights_on_export,
        shortest_path_nodes: cfg
            .shortest_path_nodes_min
            .max(nodes / cfg.shortest_path_nodes_divisor.max(1)),
        shortest_path_edge_offsets: cfg.shortest_path_edge_offsets.clone(),
    }
}

fn traverse_deep_branching(fanout: usize) -> (usize, usize, usize) {
    ((fanout / 4).clamp(8, 24), 4, 4)
}

fn build_depth_two_traversal_graph(
    engine: &mut DatabaseEngine,
    cfg: &EffectiveConfigResolved,
) -> Result<u64, String> {
    let mut node_inputs = vec![NodeInput {
        type_id: 1,
        key: "root".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
    }];
    for i in 0..cfg.two_hop_mid {
        node_inputs.push(NodeInput {
            type_id: 1,
            key: format!("m-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
        });
        for j in 0..cfg.two_hop_leaves_per_mid {
            node_inputs.push(NodeInput {
                type_id: 1,
                key: format!("l-{i}-{j}"),
                props: BTreeMap::new(),
                weight: 1.0,
            });
        }
    }
    let all_ids = engine
        .batch_upsert_nodes(&node_inputs)
        .map_err(|e| e.to_string())?;
    let root = all_ids[0];
    let mid_stride = 1 + cfg.two_hop_leaves_per_mid;
    let mut edge_inputs = Vec::new();
    for i in 0..cfg.two_hop_mid {
        let mid = all_ids[1 + i * mid_stride];
        edge_inputs.push(EdgeInput {
            from: root,
            to: mid,
            type_id: 1,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
        for j in 0..cfg.two_hop_leaves_per_mid {
            let leaf = all_ids[1 + i * mid_stride + 1 + j];
            edge_inputs.push(EdgeInput {
                from: mid,
                to: leaf,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
    }
    engine
        .batch_upsert_edges(&edge_inputs)
        .map_err(|e| e.to_string())?;
    Ok(root)
}

fn build_deep_traversal_graph(
    engine: &mut DatabaseEngine,
    fanout: usize,
) -> Result<(u64, usize, usize, usize), String> {
    let (level1, level2, level3) = traverse_deep_branching(fanout);
    let mut node_inputs = vec![NodeInput {
        type_id: 1,
        key: "root".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
    }];
    for i in 0..level1 {
        node_inputs.push(NodeInput {
            type_id: 11,
            key: format!("lvl1-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
        });
    }
    for i in 0..level1 {
        for j in 0..level2 {
            node_inputs.push(NodeInput {
                type_id: if (i + j) % 2 == 0 { 2 } else { 3 },
                key: format!("lvl2-{i}-{j}"),
                props: BTreeMap::new(),
                weight: 1.0,
            });
        }
    }
    for i in 0..level1 {
        for j in 0..level2 {
            for k in 0..level3 {
                node_inputs.push(NodeInput {
                    type_id: if (i + j + k) % 2 == 0 { 2 } else { 3 },
                    key: format!("lvl3-{i}-{j}-{k}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                });
            }
        }
    }
    let ids = engine
        .batch_upsert_nodes(&node_inputs)
        .map_err(|e| e.to_string())?;
    let root = ids[0];
    let level1_offset = 1usize;
    let level2_offset = level1_offset + level1;
    let level3_offset = level2_offset + level1 * level2;
    let mut edge_inputs = Vec::new();
    for i in 0..level1 {
        let lvl1 = ids[level1_offset + i];
        edge_inputs.push(EdgeInput {
            from: root,
            to: lvl1,
            type_id: 1,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        });
        for j in 0..level2 {
            let lvl2_idx = i * level2 + j;
            let lvl2 = ids[level2_offset + lvl2_idx];
            edge_inputs.push(EdgeInput {
                from: lvl1,
                to: lvl2,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for k in 0..level3 {
                let lvl3_idx = lvl2_idx * level3 + k;
                edge_inputs.push(EdgeInput {
                    from: lvl2,
                    to: ids[level3_offset + lvl3_idx],
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
    }
    engine
        .batch_upsert_edges(&edge_inputs)
        .map_err(|e| e.to_string())?;
    Ok((root, level1, level2, level3))
}

fn scenario_iterations(
    args: &CliArgs,
    contract: &ScenarioContract,
    scenario_id: &str,
) -> IterConfig {
    let default_policy = contract
        .scenario_iteration_policy
        .get("default")
        .cloned()
        .unwrap_or_default();
    let policy = contract
        .scenario_iteration_policy
        .get(scenario_id)
        .cloned()
        .unwrap_or_else(|| default_policy.clone());

    let warmup_divisor = policy
        .warmup_divisor
        .or(default_policy.warmup_divisor)
        .unwrap_or(1)
        .max(1);
    let warmup_min = policy
        .warmup_min
        .or(default_policy.warmup_min)
        .unwrap_or(1)
        .max(1);
    let iters_divisor = policy
        .iters_divisor
        .or(default_policy.iters_divisor)
        .unwrap_or(1)
        .max(1);
    let iters_min = policy
        .iters_min
        .or(default_policy.iters_min)
        .unwrap_or(1)
        .max(1);
    let iters_multiplier = policy
        .iters_multiplier
        .or(default_policy.iters_multiplier)
        .unwrap_or(1)
        .max(1);

    IterConfig {
        warmup: warmup_min.max(args.warmup / warmup_divisor),
        iters: iters_min.max(args.iters / iters_divisor) * iters_multiplier,
    }
}

fn scenario_comparability(contract: &ScenarioContract, scenario_id: &str) -> ComparabilityOutput {
    match contract.comparability.get(scenario_id) {
        Some(entry) => ComparabilityOutput {
            status: entry.status.clone(),
            reason: entry.reason.clone(),
        },
        None => ComparabilityOutput {
            status: "non_comparable".to_string(),
            reason: Some("Missing comparability contract entry".to_string()),
        },
    }
}

fn benchmark_db_options() -> DbOptions {
    let mut opts = DbOptions::default();
    // Keep benchmark durability mode explicit so report metadata does not silently drift with defaults.
    opts.wal_sync_mode = WalSyncMode::GroupCommit {
        interval_ms: 10,
        soft_trigger_bytes: 4 * 1024 * 1024,
        hard_cap_bytes: 16 * 1024 * 1024,
    };
    opts
}

fn open_db(path: &Path) -> Result<DatabaseEngine, String> {
    let opts = benchmark_db_options();
    DatabaseEngine::open(path, &opts).map_err(|e| e.to_string())
}

fn run_bench<F>(iter_cfg: IterConfig, f: F) -> Result<Stats, String>
where
    F: FnMut(usize) -> Result<(), overgraph::EngineError>,
{
    run_bench_inner(iter_cfg, f, false)
}

fn run_bench_growth<F>(iter_cfg: IterConfig, f: F) -> Result<Stats, String>
where
    F: FnMut(usize) -> Result<(), overgraph::EngineError>,
{
    run_bench_inner(iter_cfg, f, true)
}

fn run_bench_inner<F>(iter_cfg: IterConfig, mut f: F, growth: bool) -> Result<Stats, String>
where
    F: FnMut(usize) -> Result<(), overgraph::EngineError>,
{
    for i in 0..iter_cfg.warmup {
        f(i).map_err(|e| e.to_string())?;
    }

    let mut samples_us = Vec::with_capacity(iter_cfg.iters);
    for i in 0..iter_cfg.iters {
        let idx = iter_cfg.warmup + i;
        let started = Instant::now();
        f(idx).map_err(|e| e.to_string())?;
        samples_us.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }

    let mut stats = compute_stats(&samples_us);
    if growth && samples_us.len() >= 4 {
        let mid = samples_us.len() / 2;
        let early_p95 = percentile_of_slice(&samples_us[..mid], 95.0);
        let late_p95 = percentile_of_slice(&samples_us[mid..], 95.0);
        stats.early_p95_us = Some(early_p95);
        stats.late_p95_us = Some(late_p95);
        stats.drift_ratio = if early_p95 > 0.0 {
            Some(late_p95 / early_p95)
        } else {
            None
        };
    }
    Ok(stats)
}

fn compute_stats(samples_us: &[f64]) -> Stats {
    let mut sorted = samples_us.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mean = samples_us.iter().sum::<f64>() / samples_us.len() as f64;

    Stats {
        p50_us: percentile(&sorted, 50.0),
        p95_us: percentile(&sorted, 95.0),
        p99_us: percentile(&sorted, 99.0),
        min_us: *sorted.first().unwrap_or(&0.0),
        max_us: *sorted.last().unwrap_or(&0.0),
        mean_us: mean,
        early_p95_us: None,
        late_p95_us: None,
        drift_ratio: None,
    }
}

fn percentile_of_slice(samples: &[f64], p: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    percentile(&sorted, p)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as isize - 1;
    let idx = rank.max(0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn throughput_ops_per_sec(mean_us: f64, ops_per_iteration: usize) -> Option<f64> {
    if mean_us <= 0.0 {
        return None;
    }
    Some((ops_per_iteration as f64 * 1_000_000.0) / mean_us)
}

fn make_scenario(
    scenario_id: &str,
    name: &str,
    category: &str,
    iter_cfg: IterConfig,
    ops_per_iteration: usize,
    stats: Stats,
    scenario_params: Value,
    comparability: ComparabilityOutput,
) -> ScenarioOutput {
    let throughput = throughput_ops_per_sec(stats.mean_us, ops_per_iteration);
    ScenarioOutput {
        scenario_id: scenario_id.to_string(),
        name: name.to_string(),
        category: category.to_string(),
        warmup_iterations: iter_cfg.warmup,
        benchmark_iterations: iter_cfg.iters,
        ops_per_iteration,
        throughput_ops_per_sec: throughput,
        stats,
        scenario_params,
        comparability,
        notes: None,
    }
}

fn idx_props(idx: usize) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert("idx".to_string(), PropValue::Int(idx as i64));
    props
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn now_epoch_ms_string() -> String {
    now_millis().to_string()
}

fn now_iso_utc_string() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format_unix_seconds_utc(duration.as_secs() as i64),
        Err(_e) => now_epoch_ms_string(),
    }
}

fn format_unix_seconds_utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let hour = sod / 3_600;
    let minute = (sod % 3_600) / 60;
    let second = sod % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    // Howard Hinnant's civil date algorithm:
    // converts days since Unix epoch to Gregorian year/month/day in UTC.
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    y += if month <= 2 { 1 } else { 0 };
    (y, month, day)
}

#[cfg(test)]
mod timestamp_tests {
    use super::format_unix_seconds_utc;

    #[test]
    fn formats_unix_epoch() {
        assert_eq!(format_unix_seconds_utc(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn formats_known_timestamp() {
        assert_eq!(
            format_unix_seconds_utc(1_709_510_400),
            "2024-03-04T00:00:00Z"
        );
    }
}
