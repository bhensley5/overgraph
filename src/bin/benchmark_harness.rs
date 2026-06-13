use overgraph::{
    DatabaseEngine, DbOptions, DegreeOptions, DenseMetric, DenseVectorConfig, Direction,
    EdgeFilterExpr, EdgeInput, EdgeQuery, EdgeSchema, EndpointLabelSchema, EngineError,
    ExportOptions, GqlExecutionOptions, GqlParamValue, GqlParams, GraphBinaryOp, GraphEdgePattern,
    GraphExpr, GraphNodeField, GraphNodePattern, GraphOrderDirection, GraphOrderItem,
    GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPatternPiece, GraphQueryOptions,
    GraphReturnItem, GraphReturnProjection, GraphRowQuery, GraphSchemaOperation,
    GraphSchemaSetOptions, HnswConfig, IsConnectedOptions, LabelMatchMode, NeighborOptions,
    NodeFilterExpr, NodeInput, NodeLabelFilter, NodeQuery, NodeSchema, PageRequest, PprOptions,
    PropValue, PropertyRangeBound, PropertySchema, SchemaAdditionalProperties, SchemaValueType,
    SecondaryIndexField, SecondaryIndexKind, SecondaryIndexSpec, SecondaryIndexState,
    ShortestPathOptions, TopKOptions, TraverseOptions, UpsertEdgeOptions, UpsertNodeOptions,
    VectorSearchMode, VectorSearchRequest, WalSyncMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

macro_rules! filter_and {
    [] => {
        None
    };
    [$single:expr $(,)?] => {
        Some($single)
    };
    [$($filter:expr),+ $(,)?] => {
        Some(NodeFilterExpr::And(vec![$($filter),+]))
    };
}

const PROFILE_PATH: &str = "docs/04-quality/workloads/profiles.json";
const SCENARIO_CONTRACT_PATH: &str = "docs/04-quality/workloads/scenario-contract.json";

#[derive(Debug)]
struct CliArgs {
    profile: String,
    warmup: usize,
    iters: usize,
    scenario_set: ScenarioSet,
    scenario_ids: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioSet {
    All,
    Query,
}

impl ScenarioSet {
    fn includes_query(self) -> bool {
        matches!(self, ScenarioSet::All | ScenarioSet::Query)
    }

    fn includes_legacy(self) -> bool {
        matches!(self, ScenarioSet::All)
    }
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
    vector_dim: u32,
    vector_nodes_min: usize,
    vector_nodes_divisor: usize,
    vector_nnz: usize,
    vector_sparse_dims: u32,
    vector_k: usize,
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
    vector_nodes: usize,
    vector_dim: u32,
    vector_nnz: usize,
    vector_sparse_dims: u32,
    vector_k: usize,
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

    if args.scenario_set.includes_query() {
        push_query_scenarios(&args, &scenario_contract, &cfg, &tmp_root, &mut scenarios)?;
    }

    if args.scenario_set.includes_legacy()
        || args
            .scenario_ids
            .iter()
            .any(|scenario_id| scenario_id.starts_with("S-SCHEMA-"))
    {
        push_schema_scenarios(&args, &scenario_contract, &cfg, &tmp_root, &mut scenarios)?;
    }

    if !args.scenario_set.includes_legacy() || !args.scenario_ids.is_empty() {
        return emit_output(
            args,
            profiles_payload,
            profile,
            scenario_contract,
            cfg,
            scenarios,
        );
    }

    // S-CRUD-001: upsert_node (growth)
    {
        let scenario_id = "S-CRUD-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("crud-upsert-node"))?;
        let stats = run_bench_growth(iter_cfg, |i| {
            engine
                .upsert_node(
                    "BenchNode",
                    &format!("node-{i}"),
                    UpsertNodeOptions {
                        props: idx_props(i),
                        ..Default::default()
                    },
                )
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
            json!({"label_id": 1, "with_props": true, "weight": 1.0}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-002: upsert_edge (growth)
    {
        let scenario_id = "S-CRUD-002";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("crud-upsert-edge"))?;
        let node_inputs: Vec<NodeInput> = (0..(iter_cfg.warmup + iter_cfg.iters + 1))
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("e-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs)
            .map_err(|e| e.to_string())?;

        let stats = run_bench_growth(iter_cfg, |i| {
            engine
                .upsert_edge(
                    node_ids[i],
                    node_ids[i + 1],
                    "BenchEdge",
                    UpsertEdgeOptions::default(),
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
            json!({"edge_label": "BenchEdge", "weight": 1.0}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-BATCH-001: batch_upsert_nodes_json
    {
        let scenario_id = "S-BATCH-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("batch-nodes-json"))?;
        let stats = run_bench(iter_cfg, |i| {
            let inputs: Vec<NodeInput> = (0..cfg.batch_nodes)
                .map(|j| NodeInput {
                    labels: vec![bench_node_label(1)],
                    key: format!("bn-{i}-{j}"),
                    props: idx_props(j),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect();
            engine.batch_upsert_nodes(inputs).map(|_| ())
        })?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "batch_upsert_nodes_json",
            "batch",
            iter_cfg,
            cfg.batch_nodes,
            stats,
            json!({"batch_nodes": cfg.batch_nodes, "label_id": 1, "with_props": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-003: get_node
    {
        let scenario_id = "S-CRUD-003";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("crud-get-node"))?;
        let node_inputs: Vec<NodeInput> = (0..cfg.get_node_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("gn-{i}"),
                props: idx_props(i),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
        let engine = open_db(&tmp_root.db_path("crud-upsert-node-fixed"))?;
        engine
            .upsert_node(
                "BenchNode",
                "fixed-node",
                UpsertNodeOptions {
                    props: idx_props(0),
                    ..Default::default()
                },
            )
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |i| {
            engine
                .upsert_node(
                    "BenchNode",
                    "fixed-node",
                    UpsertNodeOptions {
                        props: idx_props(i),
                        ..Default::default()
                    },
                )
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
            json!({"label_id": 1, "with_props": true, "weight": 1.0, "fixed_key": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-CRUD-005: upsert_edge_fixed_triple
    {
        let scenario_id = "S-CRUD-005";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let mut opts = benchmark_db_options();
        opts.edge_uniqueness = true;
        let engine = DatabaseEngine::open(&tmp_root.db_path("crud-upsert-edge-fixed"), &opts)
            .map_err(|e| e.to_string())?;
        let node_a = engine
            .upsert_node("BenchNode", "fixed-a", UpsertNodeOptions::default())
            .map_err(|e| e.to_string())?;
        let node_b = engine
            .upsert_node("BenchNode", "fixed-b", UpsertNodeOptions::default())
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .upsert_edge(node_a, node_b, "BenchEdge", UpsertEdgeOptions::default())
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
            json!({"edge_label": "BenchEdge", "weight": 1.0, "edge_uniqueness": true, "fixed_triple": true}),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-001: neighbors
    {
        let scenario_id = "S-TRAV-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("trav-neighbors"))?;
        let mut node_inputs = vec![NodeInput {
            labels: vec![bench_node_label(1)],
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }];
        node_inputs.extend((0..cfg.fanout).map(|i| NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("n-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }));
        let ids = engine
            .batch_upsert_nodes(node_inputs.clone())
            .map_err(|e| e.to_string())?;
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = ids[1..]
            .iter()
            .map(|&n| EdgeInput {
                from: hub,
                to: n,
                label: "BenchEdge1".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .neighbors(hub, &NeighborOptions::default())
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
                    &TraverseOptions {
                        min_depth: 2,
                        ..Default::default()
                    },
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
                .traverse(root, 3, &TraverseOptions::default())
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
                "node_label_filter": null,
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
                .traverse(root, 3, &TraverseOptions::default())
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
                "node_label_filter": null,
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
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    3,
                    &TraverseOptions {
                        emit_node_label_filter: Some(NodeLabelFilter {
                            labels: vec![bench_node_label(2)],
                            mode: LabelMatchMode::Any,
                        }),
                        ..Default::default()
                    },
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
                "node_label_filter": {
                    "labels": [bench_node_label(2)],
                    "mode": "any"
                },
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
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .traverse(
                    root,
                    3,
                    &TraverseOptions {
                        emit_node_label_filter: Some(NodeLabelFilter {
                            labels: vec![bench_node_label(2)],
                            mode: LabelMatchMode::Any,
                        }),
                        ..Default::default()
                    },
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
                "node_label_filter": {
                    "labels": [bench_node_label(2)],
                    "mode": "any"
                },
                "branching": [level1, level2, level3]
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    // S-TRAV-003: degree (scalar)
    {
        let scenario_id = "S-TRAV-003";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_db(&tmp_root.db_path("trav-degree"))?;
        let mut node_inputs = vec![NodeInput {
            labels: vec![bench_node_label(1)],
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }];
        node_inputs.extend((0..cfg.fanout).map(|i| NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("d-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }));
        let ids = engine
            .batch_upsert_nodes(node_inputs.clone())
            .map_err(|e| e.to_string())?;
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = ids[1..]
            .iter()
            .map(|&n| EdgeInput {
                from: hub,
                to: n,
                label: "BenchEdge1".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine.degree(hub, &DegreeOptions::default()).map(|_| ())
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
        let engine = open_db(&tmp_root.db_path("trav-degrees"))?;
        let mut node_inputs: Vec<NodeInput> =
            Vec::with_capacity(cfg.batch_nodes * (1 + cfg.fanout));
        for h in 0..cfg.batch_nodes {
            node_inputs.push(NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("hub-{h}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            });
            for i in 0..cfg.fanout {
                node_inputs.push(NodeInput {
                    labels: vec![bench_node_label(1)],
                    key: format!("dt-{h}-{i}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                });
            }
        }
        let all_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
                    label: "BenchEdge1".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .degrees(&hub_ids, &DegreeOptions::default())
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
        let engine = open_db(&tmp_root.db_path("trav-shortest-path"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.shortest_path_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("sp-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;

        let sp_from = node_ids[0];
        let sp_to = node_ids[node_ids.len() / 2];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .shortest_path(sp_from, sp_to, &ShortestPathOptions::default())
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
        let engine = open_db(&tmp_root.db_path("trav-is-connected"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.shortest_path_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("ic-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;

        let sp_from = node_ids[0];
        let sp_to = node_ids[node_ids.len() / 2];
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .is_connected(sp_from, sp_to, &IsConnectedOptions::default())
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
        let engine = open_db(&tmp_root.db_path("adv-top-k"))?;
        let mut node_inputs = vec![NodeInput {
            labels: vec![bench_node_label(1)],
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }];
        node_inputs.extend((0..cfg.top_k_candidates).map(|i| NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("tk-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }));
        let ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
                    label: "BenchEdge1".to_string(),
                    props: BTreeMap::new(),
                    weight,
                    valid_from: None,
                    valid_to: None,
                }
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
            .map_err(|e| e.to_string())?;

        let stats = run_bench(iter_cfg, |_i| {
            engine
                .top_k_neighbors(hub, cfg.top_k_limit, &TopKOptions::default())
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
        let engine = open_db(&tmp_root.db_path("adv-time-range"))?;
        let node_inputs: Vec<NodeInput> = (0..cfg.time_range_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("tr-{i}"),
                props: idx_props(i),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        engine
            .batch_upsert_nodes(node_inputs.clone())
            .map_err(|e| e.to_string())?;

        let to_ms = now_millis() + cfg.time_range_window_ms;
        let label = bench_node_label(1);
        let stats = run_bench(iter_cfg, |_i| {
            engine
                .find_nodes_by_time_range(&label, cfg.time_range_from_ms, to_ms)
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
                "label": "Person",
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
        let engine = open_db(&tmp_root.db_path("adv-ppr"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.ppr_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("ppr-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
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
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from,
                        to: to2,
                        label: "BenchEdge1".to_string(),
                        props: BTreeMap::new(),
                        weight: 0.7,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine
            .batch_upsert_edges(edge_inputs.clone())
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
        let engine = open_db(&tmp_root.db_path("adv-export"))?;

        let node_inputs: Vec<NodeInput> = (0..cfg.export_nodes)
            .map(|i| NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("ex-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let node_ids = engine
            .batch_upsert_nodes(node_inputs.clone())
            .map_err(|e| e.to_string())?;

        let edge_inputs: Vec<EdgeInput> = (0..cfg.export_edges)
            .filter_map(|i| {
                let from = node_ids[i % node_ids.len()];
                let to = node_ids[(i * 13 + 7) % node_ids.len()];
                if from != to {
                    Some(EdgeInput {
                        from,
                        to,
                        label: "BenchEdge1".to_string(),
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
            .batch_upsert_edges(edge_inputs.clone())
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
        let engine = open_db(&tmp_root.db_path("maint-flush"))?;
        let stats = run_bench(iter_cfg, |i| {
            let nodes: Vec<NodeInput> = (0..cfg.flush_nodes_per_iter)
                .map(|j| NodeInput {
                    labels: vec![bench_node_label(1)],
                    key: format!("fl-{i}-{j}"),
                    props: idx_props(j),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect();
            let node_ids = engine.batch_upsert_nodes(nodes.clone())?;

            let mut edges = Vec::new();
            let edge_count = cfg
                .flush_edges_per_iter_cap
                .min(node_ids.len().saturating_sub(1));
            for j in 0..edge_count {
                edges.push(EdgeInput {
                    from: node_ids[j],
                    to: node_ids[j + 1],
                    label: "BenchEdge1".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
            engine.batch_upsert_edges(edges.clone())?;
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

    // S-VEC-001: hybrid_vector_search
    {
        let scenario_id = "S-VEC-001";
        let iter_cfg = scenario_iterations(&args, &scenario_contract, scenario_id);
        let engine = open_vector_db(&tmp_root.db_path("vec-hybrid"), cfg.vector_dim)?;

        let inputs: Vec<NodeInput> = (0..cfg.vector_nodes)
            .map(|i| {
                let seed = 1729u64.wrapping_mul(i as u64 + 1);
                NodeInput {
                    labels: vec![bench_node_label(1)],
                    key: format!("v-{i}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: Some(bench_dense_vector(cfg.vector_dim as usize, seed)),
                    sparse_vector: Some(bench_sparse_vector(
                        cfg.vector_sparse_dims,
                        cfg.vector_nnz,
                        seed.wrapping_add(0xCAFE),
                    )),
                }
            })
            .collect();
        engine
            .batch_upsert_nodes(inputs.clone())
            .map_err(|e| e.to_string())?;
        engine.flush().map_err(|e| e.to_string())?;

        let query_seed = 0xDEAD_BEEF_u64;
        let dense_query = bench_dense_vector(cfg.vector_dim as usize, query_seed);
        let sparse_query = bench_sparse_vector(
            cfg.vector_sparse_dims,
            cfg.vector_nnz,
            query_seed.wrapping_add(0xCAFE),
        );
        let request = VectorSearchRequest {
            mode: VectorSearchMode::Hybrid,
            dense_query: Some(dense_query),
            sparse_query: Some(sparse_query),
            k: cfg.vector_k,
            label_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };

        let stats = run_bench(iter_cfg, |_| engine.vector_search(&request).map(|_| ()))?;
        engine.close().map_err(|e| e.to_string())?;

        scenarios.push(make_scenario(
            scenario_id,
            "hybrid_vector_search",
            "vector",
            iter_cfg,
            1,
            stats,
            json!({
                "vector_nodes": cfg.vector_nodes,
                "vector_dim": cfg.vector_dim,
                "vector_nnz": cfg.vector_nnz,
                "vector_sparse_dims": cfg.vector_sparse_dims,
                "vector_k": cfg.vector_k,
                "mode": "hybrid",
                "fusion_mode": "weighted_rank"
            }),
            scenario_comparability(&scenario_contract, scenario_id),
        ));
    }

    emit_output(
        args,
        profiles_payload,
        profile,
        scenario_contract,
        cfg,
        scenarios,
    )
}

fn parse_args() -> Result<CliArgs, String> {
    let mut profile = String::from("small");
    let mut warmup: usize = 20;
    let mut iters: usize = 80;
    let mut scenario_set = ScenarioSet::All;
    let mut scenario_ids = HashSet::new();

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
            "--scenario-set" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--scenario-set requires a value".to_string())?;
                scenario_set = match raw.as_str() {
                    "all" => ScenarioSet::All,
                    "query" => ScenarioSet::Query,
                    _ => {
                        return Err(format!(
                            "unsupported --scenario-set '{raw}'\n{}",
                            help_text()
                        ))
                    }
                };
            }
            "--scenario-id" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--scenario-id requires a value".to_string())?;
                scenario_ids.insert(raw);
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
        scenario_set,
        scenario_ids,
    })
}

fn help_text() -> String {
    "Usage: cargo run --release --features cli --bin benchmark-harness -- --profile <small|medium|large|xlarge> --warmup <n> --iters <n> [--scenario-set <all|query>] [--scenario-id <id> ...]".to_string()
}

fn scenario_selected(args: &CliArgs, scenario_id: &str) -> bool {
    args.scenario_ids.is_empty() || args.scenario_ids.contains(scenario_id)
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
        vector_nodes: cfg
            .vector_nodes_min
            .max(profile.nodes / cfg.vector_nodes_divisor.max(1)),
        vector_dim: cfg.vector_dim,
        vector_nnz: cfg.vector_nnz,
        vector_sparse_dims: cfg.vector_sparse_dims,
        vector_k: cfg.vector_k,
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
        labels: vec![bench_node_label(1)],
        key: "root".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }];
    for i in 0..cfg.two_hop_mid {
        node_inputs.push(NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("m-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
        for j in 0..cfg.two_hop_leaves_per_mid {
            node_inputs.push(NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("l-{i}-{j}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            });
        }
    }
    let all_ids = engine
        .batch_upsert_nodes(node_inputs.clone())
        .map_err(|e| e.to_string())?;
    let root = all_ids[0];
    let mid_stride = 1 + cfg.two_hop_leaves_per_mid;
    let mut edge_inputs = Vec::new();
    for i in 0..cfg.two_hop_mid {
        let mid = all_ids[1 + i * mid_stride];
        edge_inputs.push(EdgeInput {
            from: root,
            to: mid,
            label: "BenchEdge1".to_string(),
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
                label: "BenchEdge1".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
    }
    engine
        .batch_upsert_edges(edge_inputs.clone())
        .map_err(|e| e.to_string())?;
    Ok(root)
}

fn build_deep_traversal_graph(
    engine: &mut DatabaseEngine,
    fanout: usize,
) -> Result<(u64, usize, usize, usize), String> {
    let (level1, level2, level3) = traverse_deep_branching(fanout);
    let mut node_inputs = vec![NodeInput {
        labels: vec![bench_node_label(1)],
        key: "root".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }];
    for i in 0..level1 {
        node_inputs.push(NodeInput {
            labels: vec![bench_node_label(11)],
            key: format!("lvl1-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    for i in 0..level1 {
        for j in 0..level2 {
            node_inputs.push(NodeInput {
                labels: vec![bench_node_label(if (i + j) % 2 == 0 { 2 } else { 3 })],
                key: format!("lvl2-{i}-{j}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            });
        }
    }
    for i in 0..level1 {
        for j in 0..level2 {
            for k in 0..level3 {
                node_inputs.push(NodeInput {
                    labels: vec![bench_node_label(if (i + j + k) % 2 == 0 { 2 } else { 3 })],
                    key: format!("lvl3-{i}-{j}-{k}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                });
            }
        }
    }
    let ids = engine
        .batch_upsert_nodes(node_inputs.clone())
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
            label: "BenchEdge1".to_string(),
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
                label: "BenchEdge1".to_string(),
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
                    label: "BenchEdge1".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
    }
    engine
        .batch_upsert_edges(edge_inputs.clone())
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
    // Keep benchmark durability mode explicit so report metadata does not silently drift with defaults.
    DbOptions {
        wal_sync_mode: WalSyncMode::GroupCommit {
            interval_ms: 10,
            soft_trigger_bytes: 4 * 1024 * 1024,
            hard_cap_bytes: 16 * 1024 * 1024,
        },
        ..DbOptions::default()
    }
}

fn open_db(path: &Path) -> Result<DatabaseEngine, String> {
    let opts = benchmark_db_options();
    let engine = DatabaseEngine::open(path, &opts).map_err(|e| e.to_string())?;
    seed_bench_label_tokens(&engine)?;
    Ok(engine)
}

fn open_vector_db(path: &Path, dim: u32) -> Result<DatabaseEngine, String> {
    let mut opts = benchmark_db_options();
    opts.dense_vector = Some(DenseVectorConfig {
        dimension: dim,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    });
    let engine = DatabaseEngine::open(path, &opts).map_err(|e| e.to_string())?;
    seed_bench_label_tokens(&engine)?;
    Ok(engine)
}

fn seed_bench_label_tokens(engine: &DatabaseEngine) -> Result<(), String> {
    for label_id in 1..=256 {
        let node_id = engine
            .ensure_node_label(&bench_node_label(label_id))
            .map_err(|e| e.to_string())?;
        let edge_label_id = engine
            .ensure_edge_label(&format!("BenchEdge{label_id}"))
            .map_err(|e| e.to_string())?;
        if node_id != label_id || edge_label_id != label_id {
            return Err(format!(
                "benchmark label-token seed drifted for label_id {label_id}: node={node_id}, edge={edge_label_id}"
            ));
        }
    }
    Ok(())
}

fn bench_node_label(label_id: u32) -> String {
    format!("BenchNode{label_id}")
}

fn query_person_label() -> String {
    "Person".to_string()
}

fn query_company_label() -> String {
    "Company".to_string()
}

fn query_document_label() -> String {
    "Document".to_string()
}

fn query_work_edge_label() -> String {
    "WORKS_AT".to_string()
}

fn query_optional_edge_label() -> String {
    "MENTIONS".to_string()
}

fn query_bench_props(i: usize) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String(
            if i.is_multiple_of(10) {
                "active"
            } else {
                "inactive"
            }
            .to_string(),
        ),
    );
    props.insert(
        "tier".to_string(),
        PropValue::String(
            if i.is_multiple_of(20) {
                "gold"
            } else {
                "standard"
            }
            .to_string(),
        ),
    );
    props.insert("score".to_string(), PropValue::Int((i % 100) as i64));
    props
}

fn wait_for_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) -> Result<(), String> {
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if engine
            .list_node_property_indexes()
            .map_err(|e| e.to_string())?
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for property index {index_id} to reach {expected_state:?}"
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn wait_for_edge_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) -> Result<(), String> {
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if engine
            .list_edge_property_indexes()
            .map_err(|e| e.to_string())?
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for edge property index {index_id} to reach {expected_state:?}"
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[derive(Clone, Copy)]
struct QueryBenchmarkLayout {
    segments: usize,
    segment_nodes: usize,
    memtable_tail_nodes: usize,
}

fn query_benchmark_layout(preload_nodes: usize) -> QueryBenchmarkLayout {
    let segments = if preload_nodes >= 2 { 1 } else { 0 };
    let segment_nodes = if segments == 0 {
        0
    } else {
        (preload_nodes / (segments + 1)).max(1)
    };
    let flushed_nodes = segments * segment_nodes;

    QueryBenchmarkLayout {
        segments,
        segment_nodes,
        memtable_tail_nodes: preload_nodes.saturating_sub(flushed_nodes),
    }
}

fn build_query_benchmark_engine(
    path: &Path,
    preload_nodes: usize,
) -> Result<(DatabaseEngine, QueryBenchmarkLayout), String> {
    let engine = open_db(path)?;
    let node_label = query_person_label();
    let status = engine
        .ensure_node_property_index(
            &node_label,
            SecondaryIndexSpec {
                fields: vec![SecondaryIndexField::Property {
                    key: ("status").to_string(),
                }],
                kind: SecondaryIndexKind::Equality,
            },
        )
        .map_err(|e| e.to_string())?;
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready)?;

    let tier = engine
        .ensure_node_property_index(
            &node_label,
            SecondaryIndexSpec {
                fields: vec![SecondaryIndexField::Property {
                    key: ("tier").to_string(),
                }],
                kind: SecondaryIndexKind::Equality,
            },
        )
        .map_err(|e| e.to_string())?;
    wait_for_property_index_state(&engine, tier.index_id, SecondaryIndexState::Ready)?;

    let score = engine
        .ensure_node_property_index(
            &node_label,
            SecondaryIndexSpec {
                fields: vec![SecondaryIndexField::Property {
                    key: ("score").to_string(),
                }],
                kind: SecondaryIndexKind::Range,
            },
        )
        .map_err(|e| e.to_string())?;
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready)?;

    let layout = query_benchmark_layout(preload_nodes);
    for segment in 0..layout.segments {
        let start = segment * layout.segment_nodes;
        let inputs: Vec<NodeInput> = (start..start + layout.segment_nodes)
            .map(|i| NodeInput {
                labels: vec![query_person_label()],
                key: format!("q-{i}"),
                props: query_bench_props(i),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        engine
            .batch_upsert_nodes(inputs.clone())
            .map_err(|e| e.to_string())?;
        engine.flush().map_err(|e| e.to_string())?;
    }

    let tail_start = layout.segments * layout.segment_nodes;
    let tail_inputs: Vec<NodeInput> = (tail_start..tail_start + layout.memtable_tail_nodes)
        .map(|i| NodeInput {
            labels: vec![query_person_label()],
            key: format!("q-{i}"),
            props: query_bench_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    engine
        .batch_upsert_nodes(tail_inputs.clone())
        .map_err(|e| e.to_string())?;

    Ok((engine, layout))
}

fn query_ids_intersected_request(limit: usize) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![query_person_label()],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
        ],
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

fn query_nodes_hydrated_request(limit: usize) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![query_person_label()],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(50))),
                upper: None,
            },
        ],
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

struct EdgeBenchmarkLayout {
    segments: usize,
    segment_edges: usize,
    memtable_tail_edges: usize,
}

struct EdgeBenchmarkFixture {
    engine: DatabaseEngine,
    layout: EdgeBenchmarkLayout,
    source_id: u64,
    target_ids: Vec<u64>,
}

fn build_edge_query_benchmark_engine(
    path: &Path,
    preload_edges: usize,
) -> Result<EdgeBenchmarkFixture, String> {
    let engine = open_db(path)?;
    let source_count = 1usize;
    let target_count = preload_edges.max(1);
    let mut nodes = Vec::with_capacity(source_count + target_count);
    nodes.extend((0..source_count).map(|i| NodeInput {
        labels: vec![query_person_label()],
        key: format!("edge-source-{i}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    nodes.extend((0..target_count).map(|i| NodeInput {
        labels: vec![query_company_label()],
        key: format!("edge-target-{i}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let ids = engine
        .batch_upsert_nodes(nodes.clone())
        .map_err(|e| e.to_string())?;
    let source_ids = &ids[..source_count];
    let target_ids = &ids[source_count..];
    let source_id = source_ids[0];
    let target_ids_vec = target_ids.to_vec();

    let segments = if preload_edges >= 2 { 1 } else { 0 };
    let segment_edges = if segments == 0 {
        0
    } else {
        (preload_edges / 2).max(1)
    };
    let memtable_tail_edges = preload_edges.saturating_sub(segment_edges);
    let make_edges = |start: usize, count: usize| -> Vec<EdgeInput> {
        (start..start + count)
            .map(|i| {
                let mut props = BTreeMap::new();
                props.insert(
                    "role".to_string(),
                    PropValue::String(if i % 10 == 0 { "lead" } else { "member" }.to_string()),
                );
                props.insert("score".to_string(), PropValue::Int((i % 100) as i64));
                EdgeInput {
                    from: source_ids[i % source_count],
                    to: target_ids[i % target_ids.len()],
                    label: query_work_edge_label(),
                    props,
                    weight: if i % 2 == 0 { 2.0 } else { 0.5 },
                    valid_from: None,
                    valid_to: None,
                }
            })
            .collect()
    };
    if segment_edges > 0 {
        engine
            .batch_upsert_edges(make_edges(0, segment_edges))
            .map_err(|e| e.to_string())?;
        engine.flush().map_err(|e| e.to_string())?;
    }
    if memtable_tail_edges > 0 {
        engine
            .batch_upsert_edges(make_edges(segment_edges, memtable_tail_edges))
            .map_err(|e| e.to_string())?;
    }

    Ok(EdgeBenchmarkFixture {
        engine,
        layout: EdgeBenchmarkLayout {
            segments,
            segment_edges,
            memtable_tail_edges,
        },
        source_id,
        target_ids: target_ids_vec,
    })
}

fn query_edge_ids_request(source_id: u64, limit: usize) -> EdgeQuery {
    EdgeQuery {
        label: Some(query_work_edge_label()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(1.0),
            upper: None,
        }),
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

fn query_edges_hydrated_request(source_id: u64, limit: usize) -> EdgeQuery {
    EdgeQuery {
        label: Some(query_work_edge_label()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            },
            EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            },
        ])),
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

fn build_indexed_edge_query_benchmark_engine(
    path: &Path,
    preload_edges: usize,
) -> Result<EdgeBenchmarkFixture, String> {
    let fixture = build_edge_query_benchmark_engine(path, preload_edges)?;
    let role = fixture
        .engine
        .ensure_edge_property_index(
            &query_work_edge_label(),
            SecondaryIndexSpec {
                fields: vec![SecondaryIndexField::Property {
                    key: ("role").to_string(),
                }],
                kind: SecondaryIndexKind::Equality,
            },
        )
        .map_err(|e| e.to_string())?;
    wait_for_edge_property_index_state(&fixture.engine, role.index_id, SecondaryIndexState::Ready)?;
    let score = fixture
        .engine
        .ensure_edge_property_index(
            &query_work_edge_label(),
            SecondaryIndexSpec {
                fields: vec![SecondaryIndexField::Property {
                    key: ("score").to_string(),
                }],
                kind: SecondaryIndexKind::Range,
            },
        )
        .map_err(|e| e.to_string())?;
    wait_for_edge_property_index_state(
        &fixture.engine,
        score.index_id,
        SecondaryIndexState::Ready,
    )?;
    Ok(fixture)
}

fn query_edge_ids_indexed_equality_request(source_id: u64, limit: usize) -> EdgeQuery {
    EdgeQuery {
        label: Some(query_work_edge_label()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "role".to_string(),
            value: PropValue::String("lead".to_string()),
        }),
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

fn query_edge_ids_indexed_range_request(source_id: u64, limit: usize) -> EdgeQuery {
    EdgeQuery {
        label: Some(query_work_edge_label()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
            upper: None,
        }),
        page: PageRequest {
            limit: Some(limit),
            after: None,
        },
        ..Default::default()
    }
}

fn build_graph_row_benchmark_engine(
    path: &Path,
    preload_edges: usize,
) -> Result<EdgeBenchmarkFixture, String> {
    let fixture = build_indexed_edge_query_benchmark_engine(path, preload_edges)?;
    let target_count = preload_edges.max(1);
    let docs: Vec<NodeInput> = (0..target_count)
        .filter(|i| i % 8 == 0)
        .map(|i| NodeInput {
            labels: vec![query_document_label()],
            key: format!("doc-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let doc_ids = fixture
        .engine
        .batch_upsert_nodes(docs.clone())
        .map_err(|e| e.to_string())?;
    if !doc_ids.is_empty() {
        let doc_edges: Vec<EdgeInput> = doc_ids
            .iter()
            .enumerate()
            .map(|(doc_index, &doc_id)| {
                let target_ordinal = doc_index * 8;
                EdgeInput {
                    from: fixture.target_ids[target_ordinal],
                    to: doc_id,
                    label: query_optional_edge_label(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                }
            })
            .collect();
        fixture
            .engine
            .batch_upsert_edges(doc_edges)
            .map_err(|e| e.to_string())?;
    }
    Ok(fixture)
}

fn graph_row_optional_request(source_id: u64, limit: usize) -> GraphRowQuery {
    GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![query_person_label()],
                    mode: LabelMatchMode::All,
                }),
                ids: vec![source_id],
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![query_company_label()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "doc".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![query_document_label()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec![query_work_edge_label()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "role".to_string(),
                    value: PropValue::String("lead".to_string()),
                }),
            }),
            GraphPatternPiece::Optional(overgraph::GraphOptionalGroup {
                pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                    alias: Some("ref".to_string()),
                    from_alias: "target".to_string(),
                    to_alias: "doc".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec![query_optional_edge_label()],
                    filter: None,
                })],
                where_: None,
            }),
        ],
        where_: Some(GraphExpr::Binary {
            left: Box::new(GraphExpr::Property {
                alias: "edge".to_string(),
                key: "role".to_string(),
            }),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::Param("role".to_string())),
        }),
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
            graph_row_return_binding("ref"),
            graph_row_return_binding("doc"),
        ]),
        order_by: vec![
            GraphOrderItem {
                expr: GraphExpr::Property {
                    alias: "edge".to_string(),
                    key: "score".to_string(),
                },
                direction: GraphOrderDirection::Desc,
            },
            GraphOrderItem {
                expr: GraphExpr::NodeField {
                    alias: "target".to_string(),
                    field: GraphNodeField::Id,
                },
                direction: GraphOrderDirection::Asc,
            },
        ],
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::from([(
            "role".to_string(),
            GraphParamValue::String("lead".to_string()),
        )]),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions::default(),
    }
}

fn graph_row_return_binding(alias: &str) -> GraphReturnItem {
    GraphReturnItem {
        expr: GraphExpr::Binding(alias.to_string()),
        alias: Some(alias.to_string()),
        projection: GraphReturnProjection::IdOnly,
    }
}

fn graph_row_scenario_params(
    layout: &EdgeBenchmarkLayout,
    preload_edges: usize,
    limit: usize,
) -> Value {
    json!({
        "labels": {
            "source": "Person",
            "target": "Company",
            "optional": "Document"
        },
        "edge_labels": {
            "required": "WORKS_AT",
            "optional": "MENTIONS"
        },
        "preload_edges": preload_edges,
        "segments": layout.segments,
        "segment_edges": layout.segment_edges,
        "memtable_tail_edges": layout.memtable_tail_edges,
        "predicate": "edge_role_eq_lead_param",
        "source_anchor": "first_source_id",
        "optional": "target_mentions_document_sparse",
        "row_ops": ["order_by_edge_score_desc", "limit"],
        "limit": limit
    })
}

fn schema_required_string_node_schema() -> NodeSchema {
    NodeSchema {
        additional_properties: SchemaAdditionalProperties::Allow,
        properties: BTreeMap::from([(
            "name".to_string(),
            PropertySchema {
                required: true,
                nullable: false,
                types: vec![SchemaValueType::String],
                ..Default::default()
            },
        )]),
        ..Default::default()
    }
}

fn schema_name_props(i: usize) -> BTreeMap<String, PropValue> {
    BTreeMap::from([("name".to_string(), PropValue::String(format!("name-{i}")))])
}

fn schema_role_props(i: usize) -> BTreeMap<String, PropValue> {
    BTreeMap::from([("role".to_string(), PropValue::String(format!("role-{i}")))])
}

fn schema_work_edge_schema() -> EdgeSchema {
    EdgeSchema {
        properties: BTreeMap::from([(
            "role".to_string(),
            PropertySchema {
                required: true,
                nullable: false,
                types: vec![SchemaValueType::String],
                ..Default::default()
            },
        )]),
        from: Some(EndpointLabelSchema {
            all_of: vec!["SchemaPerson".to_string()],
            ..Default::default()
        }),
        to: Some(EndpointLabelSchema {
            all_of: vec!["SchemaCompany".to_string()],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn schema_graph_operations() -> Vec<GraphSchemaOperation> {
    vec![
        GraphSchemaOperation::SetNode {
            label: "SchemaPerson".to_string(),
            schema: schema_required_string_node_schema(),
        },
        GraphSchemaOperation::SetEdge {
            label: "SCHEMA_WORKS_AT".to_string(),
            schema: schema_work_edge_schema(),
        },
    ]
}

fn seed_schema_publish_data(engine: &DatabaseEngine) -> Result<(), String> {
    let person = engine
        .upsert_node(
            "SchemaPerson",
            "person-0",
            UpsertNodeOptions {
                props: schema_name_props(0),
                ..Default::default()
            },
        )
        .map_err(|e| e.to_string())?;
    let company = engine
        .upsert_node("SchemaCompany", "company-0", UpsertNodeOptions::default())
        .map_err(|e| e.to_string())?;
    engine
        .upsert_edge(
            person,
            company,
            "SCHEMA_WORKS_AT",
            UpsertEdgeOptions {
                props: schema_role_props(0),
                ..Default::default()
            },
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn schema_publish_params(operation: &str) -> Value {
    json!({
        "api": if operation.starts_with("gql_") { "gql" } else { "native" },
        "operation": operation,
        "node_targets": ["SchemaPerson"],
        "edge_targets": ["SCHEMA_WORKS_AT"],
        "preload_nodes": 2,
        "preload_edges": 1,
        "chunk_size": 128
    })
}

fn schema_active_write_params() -> Value {
    json!({
        "api": "native",
        "operation": "upsert_node_active_schema",
        "registered_node_schemas": ["SchemaPerson"],
        "registered_edge_schemas": ["SCHEMA_WORKS_AT"],
        "write_label": "SchemaPerson",
        "with_props": true
    })
}

const GQL_SCHEMA_ALTER_ADD: &str = "ALTER CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128 }";
const GQL_SCHEMA_CHECK_ADD: &str = "CHECK CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128, max_violations: 4 }";

fn push_schema_scenarios(
    args: &CliArgs,
    scenario_contract: &ScenarioContract,
    _cfg: &EffectiveConfigResolved,
    tmp_root: &TempBenchDir,
    scenarios: &mut Vec<ScenarioOutput>,
) -> Result<(), String> {
    {
        let scenario_id = "S-SCHEMA-001";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let engine = open_db(&tmp_root.db_path("schema-gql-alter-add"))?;
            seed_schema_publish_data(&engine)?;
            let options = GqlExecutionOptions::default();
            let stats = run_bench_with_setup(
                iter_cfg,
                |_i| engine.drop_graph_schema().map(|_| ()),
                |_i| {
                    let result =
                        engine.execute_gql(GQL_SCHEMA_ALTER_ADD, &GqlParams::new(), &options)?;
                    if result
                        .schema_stats
                        .as_ref()
                        .map(|stats| stats.targets_published)
                        == Some(2)
                    {
                        Ok(())
                    } else {
                        Err(EngineError::InvalidOperation(
                            "GQL schema ALTER benchmark expected two published targets".to_string(),
                        ))
                    }
                },
            )?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "gql_schema_alter_add_existing_data",
                "schema",
                iter_cfg,
                1,
                stats,
                schema_publish_params("gql_alter_current_graph_type_add"),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-SCHEMA-002";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let engine = open_db(&tmp_root.db_path("schema-native-bulk-add"))?;
            seed_schema_publish_data(&engine)?;
            let options = GraphSchemaSetOptions {
                chunk_size: 128,
                ..Default::default()
            };
            let stats = run_bench_with_setup(
                iter_cfg,
                |_i| engine.drop_graph_schema().map(|_| ()),
                |_i| {
                    let result =
                        engine.alter_graph_schema(schema_graph_operations(), options.clone())?;
                    if result.targets_published == 2 {
                        Ok(())
                    } else {
                        Err(EngineError::InvalidOperation(
                            "bulk graph-schema benchmark expected two published targets"
                                .to_string(),
                        ))
                    }
                },
            )?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "bulk_graph_schema_add_existing_data",
                "schema",
                iter_cfg,
                1,
                stats,
                schema_publish_params("alter_graph_schema_add"),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-SCHEMA-003";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let engine = open_db(&tmp_root.db_path("schema-active-upsert-node"))?;
            seed_schema_publish_data(&engine)?;
            engine
                .alter_graph_schema(
                    schema_graph_operations(),
                    GraphSchemaSetOptions {
                        chunk_size: 128,
                        ..Default::default()
                    },
                )
                .map_err(|e| e.to_string())?;
            let stats = run_bench_growth(iter_cfg, |i| {
                engine
                    .upsert_node(
                        "SchemaPerson",
                        &format!("person-write-{i}"),
                        UpsertNodeOptions {
                            props: schema_name_props(i),
                            ..Default::default()
                        },
                    )
                    .map(|_| ())
            })?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "upsert_node_active_schema",
                "schema",
                iter_cfg,
                1,
                stats,
                schema_active_write_params(),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-SCHEMA-004";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let engine = open_db(&tmp_root.db_path("schema-gql-check-add"))?;
            seed_schema_publish_data(&engine)?;
            let options = GqlExecutionOptions::default();
            let stats = run_bench(iter_cfg, |_i| {
                let result =
                    engine.execute_gql(GQL_SCHEMA_CHECK_ADD, &GqlParams::new(), &options)?;
                if result
                    .schema_stats
                    .as_ref()
                    .map(|stats| stats.violation_count)
                    == Some(0)
                {
                    Ok(())
                } else {
                    Err(EngineError::InvalidOperation(
                        "GQL schema CHECK benchmark expected zero violations".to_string(),
                    ))
                }
            })?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "gql_schema_check_add_existing_data",
                "schema",
                iter_cfg,
                1,
                stats,
                json!({
                    "api": "gql",
                    "operation": "gql_check_current_graph_type_add",
                    "node_targets": ["SchemaPerson"],
                    "edge_targets": ["SCHEMA_WORKS_AT"],
                    "preload_nodes": 2,
                    "preload_edges": 1,
                    "chunk_size": 128,
                    "max_violations": 4
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    Ok(())
}

fn push_query_scenarios(
    args: &CliArgs,
    scenario_contract: &ScenarioContract,
    cfg: &EffectiveConfigResolved,
    tmp_root: &TempBenchDir,
    scenarios: &mut Vec<ScenarioOutput>,
) -> Result<(), String> {
    let preload_nodes = cfg.time_range_nodes;
    let limit = 100usize;

    {
        let scenario_id = "S-QUERY-001";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let (engine, layout) = build_query_benchmark_engine(
                &tmp_root.db_path("query-node-ids-intersected"),
                preload_nodes,
            )?;
            let request = query_ids_intersected_request(limit);
            let stats = run_bench(iter_cfg, |_i| engine.query_node_ids(&request).map(|_| ()))?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_node_ids_intersected_predicates",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label": "Person",
                    "preload_nodes": preload_nodes,
                    "segments": layout.segments,
                    "segment_nodes": layout.segment_nodes,
                    "memtable_tail_nodes": layout.memtable_tail_nodes,
                    "predicates": ["status_eq_active", "tier_eq_gold"],
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-002";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let (engine, layout) = build_query_benchmark_engine(
                &tmp_root.db_path("query-nodes-hydrated-intersected"),
                preload_nodes,
            )?;
            let request = query_nodes_hydrated_request(limit);
            let stats = run_bench(iter_cfg, |_i| engine.query_nodes(&request).map(|_| ()))?;
            engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_nodes_intersected_predicates_hydrated",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label_id": 1,
                    "preload_nodes": preload_nodes,
                    "segments": layout.segments,
                    "segment_nodes": layout.segment_nodes,
                    "memtable_tail_nodes": layout.memtable_tail_nodes,
                    "predicates": ["status_eq_active", "score_gte_50"],
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-003";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_edge_query_benchmark_engine(
                &tmp_root.db_path("query-edge-ids-endpoint-metadata"),
                preload_nodes,
            )?;
            let request = query_edge_ids_request(fixture.source_id, limit);
            let stats = run_bench(iter_cfg, |_i| {
                fixture.engine.query_edge_ids(&request).map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_edge_ids_endpoint_metadata",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label": "WORKS_AT",
                    "preload_edges": preload_nodes,
                    "segments": fixture.layout.segments,
                    "segment_edges": fixture.layout.segment_edges,
                    "memtable_tail_edges": fixture.layout.memtable_tail_edges,
                    "filter": "weight_gte_1",
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-004";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_edge_query_benchmark_engine(
                &tmp_root.db_path("query-edges-endpoint-property-hydrated"),
                preload_nodes,
            )?;
            let request = query_edges_hydrated_request(fixture.source_id, limit);
            let stats = run_bench(iter_cfg, |_i| {
                fixture.engine.query_edges(&request).map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_edges_endpoint_property_hydrated",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label": "WORKS_AT",
                    "preload_edges": preload_nodes,
                    "segments": fixture.layout.segments,
                    "segment_edges": fixture.layout.segment_edges,
                    "memtable_tail_edges": fixture.layout.memtable_tail_edges,
                    "filter": "weight_gte_1_and_role_eq_lead",
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-005";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_indexed_edge_query_benchmark_engine(
                &tmp_root.db_path("query-edge-ids-property-indexed-equality"),
                preload_nodes,
            )?;
            let request = query_edge_ids_indexed_equality_request(fixture.source_id, limit);
            let stats = run_bench(iter_cfg, |_i| {
                fixture.engine.query_edge_ids(&request).map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_edge_ids_property_indexed_equality",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label": "WORKS_AT",
                    "preload_edges": preload_nodes,
                    "segments": fixture.layout.segments,
                    "segment_edges": fixture.layout.segment_edges,
                    "memtable_tail_edges": fixture.layout.memtable_tail_edges,
                    "filter": "role_eq_lead",
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-006";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_indexed_edge_query_benchmark_engine(
                &tmp_root.db_path("query-edge-ids-property-indexed-range"),
                preload_nodes,
            )?;
            let request = query_edge_ids_indexed_range_request(fixture.source_id, limit);
            let stats = run_bench(iter_cfg, |_i| {
                fixture.engine.query_edge_ids(&request).map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_edge_ids_property_indexed_range",
                "query",
                iter_cfg,
                1,
                stats,
                json!({
                    "label": "WORKS_AT",
                    "preload_edges": preload_nodes,
                    "segments": fixture.layout.segments,
                    "segment_edges": fixture.layout.segment_edges,
                    "memtable_tail_edges": fixture.layout.memtable_tail_edges,
                    "filter": "score_gte_90",
                    "limit": limit
                }),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-QUERY-007";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_graph_row_benchmark_engine(
                &tmp_root.db_path("query-graph-rows-optional-edge"),
                preload_nodes,
            )?;
            let request = graph_row_optional_request(fixture.source_id, limit);
            let stats = run_bench(iter_cfg, |_i| {
                fixture.engine.query_graph_rows(&request).map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "query_graph_rows_optional_edge_traversal",
                "query",
                iter_cfg,
                1,
                stats,
                graph_row_scenario_params(&fixture.layout, preload_nodes, limit),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    {
        let scenario_id = "S-GQL-006";
        if scenario_selected(args, scenario_id) {
            let iter_cfg = scenario_iterations(args, scenario_contract, scenario_id);
            let fixture = build_graph_row_benchmark_engine(
                &tmp_root.db_path("gql-graph-row-optional-edge"),
                preload_nodes,
            )?;
            let query = format!(
                "MATCH (source:Person)-[edge:WORKS_AT {{role: $role}}]->(target:Company) \
             WHERE id(source) = $source \
             OPTIONAL MATCH (target)-[ref:MENTIONS]->(doc:Document) \
             RETURN id(source) AS source, id(edge) AS edge, id(target) AS target, \
                    id(ref) AS ref, id(doc) AS doc \
             ORDER BY edge.score DESC, id(target) LIMIT {limit}"
            );
            let params = GqlParams::from([
                (
                    "role".to_string(),
                    GqlParamValue::String("lead".to_string()),
                ),
                ("source".to_string(), GqlParamValue::UInt(fixture.source_id)),
            ]);
            let options = GqlExecutionOptions::default();
            let stats = run_bench(iter_cfg, |_i| {
                fixture
                    .engine
                    .execute_gql(&query, &params, &options)
                    .map(|_| ())
            })?;
            fixture.engine.close().map_err(|e| e.to_string())?;

            scenarios.push(make_scenario(
                scenario_id,
                "execute_gql_optional_edge_traversal_graph_rows",
                "query",
                iter_cfg,
                1,
                stats,
                graph_row_scenario_params(&fixture.layout, preload_nodes, limit),
                scenario_comparability(scenario_contract, scenario_id),
            ));
        }
    }

    Ok(())
}

fn bench_splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn bench_dense_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut values = Vec::with_capacity(dim);
    let mut state = seed;
    for _ in 0..dim {
        state = bench_splitmix64(state);
        values.push((state >> 40) as f32 / 16_777_215.0 * 2.0 - 1.0);
    }
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut values {
            *v /= norm;
        }
    }
    values
}

fn bench_sparse_vector(dim_count: u32, nnz: usize, seed: u64) -> Vec<(u32, f32)> {
    let mut dims = Vec::with_capacity(nnz);
    let mut state = seed;
    while dims.len() < nnz {
        state = bench_splitmix64(state);
        let d = (state % dim_count as u64) as u32;
        if !dims.contains(&d) {
            dims.push(d);
        }
    }
    dims.sort_unstable();
    dims.into_iter()
        .enumerate()
        .map(|(i, d)| (d, 1.0 - i as f32 * 0.05))
        .collect()
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

fn run_bench_with_setup<S, F>(iter_cfg: IterConfig, mut setup: S, mut f: F) -> Result<Stats, String>
where
    S: FnMut(usize) -> Result<(), overgraph::EngineError>,
    F: FnMut(usize) -> Result<(), overgraph::EngineError>,
{
    for i in 0..iter_cfg.warmup {
        setup(i).map_err(|e| e.to_string())?;
        f(i).map_err(|e| e.to_string())?;
    }

    let mut samples_us = Vec::with_capacity(iter_cfg.iters);
    for i in 0..iter_cfg.iters {
        let idx = iter_cfg.warmup + i;
        setup(idx).map_err(|e| e.to_string())?;
        let started = Instant::now();
        f(idx).map_err(|e| e.to_string())?;
        samples_us.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }

    Ok(compute_stats(&samples_us))
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

#[allow(clippy::too_many_arguments)]
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

fn emit_output(
    args: CliArgs,
    profiles_payload: ProfilesPayload,
    profile: ProfileConfig,
    scenario_contract: ScenarioContract,
    cfg: EffectiveConfigResolved,
    scenarios: Vec<ScenarioOutput>,
) -> Result<(), String> {
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
