use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeFilterExpr, EdgeInput, EdgeQuery,
    GqlExecutionOptions, GqlParamValue, GqlParams, GqlStatementKind, GraphBinaryOp,
    GraphEdgePattern, GraphExpr, GraphNodeField, GraphNodePattern, GraphOrderDirection,
    GraphOrderItem, GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPatch,
    GraphPatternPiece, GraphPipelineMatchStage, GraphPipelineOptions, GraphPipelineQuery,
    GraphPipelineStage, GraphProjectItem, GraphProjectKind, GraphProjectStage,
    GraphProjectionItems, GraphQueryOptions, GraphReturnItem, GraphReturnProjection,
    GraphVariableLengthPattern, LabelMatchMode, NodeFilterExpr, NodeInput, NodeLabelFilter,
    NodeQuery, PageRequest, PropValue, PropertyRangeBound, QueryPlanExecutionMode, QueryPlanNode,
    QueryPlanWarning, SecondaryIndexField, SecondaryIndexSpec, SecondaryIndexState, WalSyncMode,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod phase44b_write_workload;

const QUERY_SEGMENT_COUNT: usize = 1;
const QUERY_NODES_PER_SEGMENT: usize = 5_000;
const QUERY_MEMTABLE_TAIL_COUNT: usize = 5_000;
const QUERY_LIMIT: usize = 100;
const QUERY_LARGE_UNIVERSE_COUNT: usize = 25_000;
const QUERY_SMALL_LABEL_COUNT: usize = 128;
const QUERY_LARGE_IN_VALUE_COUNT: usize = 512;
const STREAMING_SCALE_SEGMENT_COUNT: usize = 3;
const STREAMING_SCALE_NODES_PER_SEGMENT: usize = 60_000;
const STREAMING_SCALE_MEMTABLE_TAIL_COUNT: usize = 20_000;
const STREAMING_SCALE_TOTAL_NODES: usize = STREAMING_SCALE_SEGMENT_COUNT
    * STREAMING_SCALE_NODES_PER_SEGMENT
    + STREAMING_SCALE_MEMTABLE_TAIL_COUNT;
const STREAMING_SCALE_IN_TENANT_COUNT: usize = 20;
const STREAMING_SCALE_SELECTED_TENANT_SIZE: usize = 210;
const EDGE_STREAMING_SCALE_NODE_COUNT: usize = 10_000;
const EDGE_STREAMING_SCALE_SEGMENT_COUNT: usize = 3;
const EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT: usize = 60_000;
const EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT: usize = 20_000;
const EDGE_STREAMING_SCALE_TOTAL_EDGES: usize = EDGE_STREAMING_SCALE_SEGMENT_COUNT
    * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT
    + EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT;
const EDGE_STREAMING_SCALE_HUB_EDGE_COUNT: usize = 5_000;
const GRAPH_ROW_EDGEPULL_NODE_COUNT: usize = 10_000;
const GRAPH_ROW_EDGEPULL_SEGMENT_COUNT: usize = 3;
const GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT: usize = 60_000;
const GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT: usize = 20_000;
const GRAPH_ROW_EDGEPULL_EDGE_COUNT: usize = GRAPH_ROW_EDGEPULL_SEGMENT_COUNT
    * GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT
    + GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT;
const GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX: usize = 4_096;
const GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT: usize = 2_500;
const GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT: usize = 200_000;
const GRAPH_ROW_PLANNER_EDGE_COUNT: usize = 6;
const MEMTABLE_PLANNER_STRESS_COUNT: usize = 20_000;
const MEMTABLE_PLANNER_SEGMENT_COUNT: usize = 512;
const ENDPOINT_ESTIMATE_PLANNER_NODE_COUNT: usize = 1_024;
const LARGE_ENDPOINT_PLANNER_NODE_COUNT: usize = 8_192;
const COMPOUND_BENCH_NODE_COUNT: usize = 2_000;

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

fn temp_db_with_edge_uniqueness(edge_uniqueness: bool) -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        edge_uniqueness,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    seed_bench_label_tokens(&engine);
    (dir, engine)
}

fn temp_db() -> (tempfile::TempDir, DatabaseEngine) {
    temp_db_with_edge_uniqueness(true)
}

fn temp_db_without_auto_compaction() -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    seed_bench_label_tokens(&engine);
    (dir, engine)
}

fn seed_bench_label_tokens(engine: &DatabaseEngine) {
    for label_token_id in 1..=256 {
        assert_eq!(
            engine
                .ensure_node_label(&bench_node_label(label_token_id))
                .unwrap(),
            label_token_id
        );
        assert_eq!(
            engine
                .ensure_edge_label(&format!("BenchEdge{label_token_id}"))
                .unwrap(),
            label_token_id
        );
    }
}

fn bench_node_label(label_token_id: u32) -> String {
    format!("BenchNode{label_token_id}")
}

fn query_props(i: usize) -> BTreeMap<String, PropValue> {
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
    props.insert(
        "region".to_string(),
        PropValue::String(format!("r{:02}", i % 17)),
    );
    props.insert(
        "tenant".to_string(),
        PropValue::String(format!("t{:02}", i % 100)),
    );
    props
}

fn query_nodes(prefix: &str, start: usize, count: usize) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(1)],
            key: format!("{prefix}-{i}"),
            props: query_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn query_nodes_with_label_id(
    label_id: u32,
    prefix: &str,
    start: usize,
    count: usize,
) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(label_id)],
            key: format!("{prefix}-{i}"),
            props: query_props(i),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

fn query_nodes_with_props(
    label_id: u32,
    prefix: &str,
    start: usize,
    count: usize,
    props: BTreeMap<String, PropValue>,
) -> Vec<NodeInput> {
    (start..start + count)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(label_id)],
            key: format!("{prefix}-{i}"),
            props: props.clone(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect()
}

struct StreamingScaleEngine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    hot_ids: Vec<u64>,
}

fn wait_for_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if engine
            .list_node_property_indexes()
            .unwrap()
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for property index {} to reach {:?}",
            index_id,
            expected_state
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_edge_property_index_state(
    engine: &DatabaseEngine,
    index_id: u64,
    expected_state: SecondaryIndexState,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if engine
            .list_edge_property_indexes()
            .unwrap()
            .into_iter()
            .any(|info| info.index_id == index_id && info.state == expected_state)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for edge property index {} to reach {:?}",
            index_id,
            expected_state
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn ensure_query_indexes(engine: &mut DatabaseEngine) {
    let label = bench_node_label(1);
    let status = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);

    let tier = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("tier")]),
        )
        .unwrap();
    wait_for_property_index_state(engine, tier.index_id, SecondaryIndexState::Ready);

    let tenant = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("tenant")]),
        )
        .unwrap();
    wait_for_property_index_state(engine, tenant.index_id, SecondaryIndexState::Ready);

    let score = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("score")]),
        )
        .unwrap();
    wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
}

fn build_indexed_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, mut engine) = temp_db();
    ensure_query_indexes(&mut engine);
    load_query_mixed_sources(&engine, "indexed");
    (dir, engine)
}

fn ensure_bench_equality_index(engine: &DatabaseEngine, prop_key: &str) {
    let label = bench_node_label(1);
    let info = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property(prop_key)]),
        )
        .unwrap();
    wait_for_property_index_state(engine, info.index_id, SecondaryIndexState::Ready);
}

fn ensure_streaming_scale_indexes(engine: &DatabaseEngine) {
    for prop_key in ["status", "region", "tenant"] {
        ensure_bench_equality_index(engine, prop_key);
    }
    let label = bench_node_label(1);
    let score = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("score")]),
        )
        .unwrap();
    wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
}

fn streaming_scale_tenant_ordinal(ordinal: usize) -> usize {
    let selected_tenant_rows =
        STREAMING_SCALE_IN_TENANT_COUNT * STREAMING_SCALE_SELECTED_TENANT_SIZE;
    if ordinal < selected_tenant_rows {
        ordinal % STREAMING_SCALE_IN_TENANT_COUNT
    } else {
        STREAMING_SCALE_IN_TENANT_COUNT
            + ((ordinal - selected_tenant_rows) % (1_000 - STREAMING_SCALE_IN_TENANT_COUNT))
    }
}

fn streaming_scale_score(ordinal: usize) -> i64 {
    if ordinal.is_multiple_of(4) {
        990 + (ordinal % 10) as i64
    } else {
        (ordinal % 990) as i64
    }
}

fn streaming_scale_props(
    ordinal: usize,
    status_override: Option<&str>,
) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String(
            status_override
                .unwrap_or(if ordinal.is_multiple_of(2) {
                    "hot"
                } else {
                    "cold"
                })
                .to_string(),
        ),
    );
    props.insert(
        "region".to_string(),
        PropValue::String(
            if ordinal.is_multiple_of(4) {
                "east"
            } else {
                "west"
            }
            .to_string(),
        ),
    );
    props.insert(
        "tenant".to_string(),
        PropValue::String(format!("t{:03}", streaming_scale_tenant_ordinal(ordinal))),
    );
    props.insert(
        "score".to_string(),
        PropValue::Int(streaming_scale_score(ordinal)),
    );
    props
}

fn streaming_scale_node(ordinal: usize, status_override: Option<&str>) -> NodeInput {
    NodeInput {
        labels: vec![bench_node_label(1)],
        key: format!("stream-scale-{ordinal}"),
        props: streaming_scale_props(ordinal, status_override),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn build_streaming_scale_engine_variant(stale_heavy: bool) -> StreamingScaleEngine {
    let (dir, engine) = temp_db_without_auto_compaction();
    ensure_streaming_scale_indexes(&engine);

    let mut all_ids = Vec::with_capacity(STREAMING_SCALE_TOTAL_NODES);
    for segment in 0..STREAMING_SCALE_SEGMENT_COUNT {
        let start = segment * STREAMING_SCALE_NODES_PER_SEGMENT;
        let nodes = (start..start + STREAMING_SCALE_NODES_PER_SEGMENT)
            .map(|ordinal| streaming_scale_node(ordinal, None))
            .collect::<Vec<_>>();
        all_ids.extend(engine.batch_upsert_nodes(nodes).unwrap());
        engine.flush().unwrap();
    }

    let tail_start = STREAMING_SCALE_SEGMENT_COUNT * STREAMING_SCALE_NODES_PER_SEGMENT;
    let tail_nodes = (tail_start..tail_start + STREAMING_SCALE_MEMTABLE_TAIL_COUNT)
        .map(|ordinal| streaming_scale_node(ordinal, None))
        .collect::<Vec<_>>();
    all_ids.extend(engine.batch_upsert_nodes(tail_nodes).unwrap());
    assert_eq!(all_ids.len(), STREAMING_SCALE_TOTAL_NODES);

    let mut stale_segment_hot_ordinals = std::collections::BTreeSet::new();
    if stale_heavy {
        let segment_hot_total =
            STREAMING_SCALE_SEGMENT_COUNT * STREAMING_SCALE_NODES_PER_SEGMENT / 2;
        let stale_target = segment_hot_total * 95 / 100;
        let mut stale_updates = Vec::with_capacity(stale_target);
        for ordinal in (0..tail_start).filter(|ordinal| ordinal.is_multiple_of(2)) {
            if stale_updates.len() == stale_target {
                break;
            }
            stale_segment_hot_ordinals.insert(ordinal);
            stale_updates.push(streaming_scale_node(ordinal, Some("cold")));
        }
        assert_eq!(stale_updates.len(), stale_target);
        engine.batch_upsert_nodes(stale_updates).unwrap();
    }

    let hot_ids = all_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, &id)| {
            (ordinal.is_multiple_of(2) && !stale_segment_hot_ordinals.contains(&ordinal))
                .then_some(id)
        })
        .collect::<Vec<_>>();
    let east_count = (0..STREAMING_SCALE_TOTAL_NODES)
        .filter(|ordinal| ordinal.is_multiple_of(4))
        .count();
    let selected_tenant_count = (0..STREAMING_SCALE_TOTAL_NODES)
        .filter(|&ordinal| {
            streaming_scale_tenant_ordinal(ordinal) < STREAMING_SCALE_IN_TENANT_COUNT
        })
        .count();
    let high_score_count = (0..STREAMING_SCALE_TOTAL_NODES)
        .filter(|&ordinal| (990..=999).contains(&streaming_scale_score(ordinal)))
        .count();
    assert_eq!(east_count, STREAMING_SCALE_TOTAL_NODES / 4);
    assert!(selected_tenant_count > 4_096);
    assert!(high_score_count > 4_096);
    assert!(hot_ids.len() > 4_096);

    StreamingScaleEngine {
        _dir: dir,
        engine,
        hot_ids,
    }
}

fn build_streaming_scale_engine() -> StreamingScaleEngine {
    build_streaming_scale_engine_variant(false)
}

fn build_streaming_scale_engine_stale_heavy() -> StreamingScaleEngine {
    build_streaming_scale_engine_variant(true)
}

struct EdgeStreamingScaleEngine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    edge_ids: Vec<u64>,
    hot_ids: Vec<u64>,
    hub_source_ids: Vec<u64>,
    stale_rewrite_count: usize,
}

fn temp_edge_streaming_scale_db() -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        compact_after_n_flushes: 0,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    seed_bench_label_tokens(&engine);
    (dir, engine)
}

fn ensure_edge_streaming_scale_indexes(engine: &DatabaseEngine) {
    let label = "BenchEdge1";
    for prop_key in ["status", "region", "tenant"] {
        let info = engine
            .ensure_edge_property_index(
                label,
                SecondaryIndexSpec::equality(vec![SecondaryIndexField::property(prop_key)]),
            )
            .unwrap();
        wait_for_edge_property_index_state(engine, info.index_id, SecondaryIndexState::Ready);
    }
    let score = engine
        .ensure_edge_property_index(
            label,
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("score")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
}

fn edge_streaming_scale_node(ordinal: usize) -> NodeInput {
    NodeInput {
        labels: vec![bench_node_label(2)],
        key: format!("edge-stream-scale-node-{ordinal}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn edge_streaming_scale_tenant_ordinal(ordinal: usize) -> usize {
    streaming_scale_tenant_ordinal(ordinal)
}

fn edge_streaming_scale_score(ordinal: usize) -> i64 {
    if ordinal.is_multiple_of(4) {
        990 + (ordinal % 10) as i64
    } else {
        (ordinal % 990) as i64
    }
}

fn edge_streaming_scale_weight(ordinal: usize) -> f32 {
    if ordinal.is_multiple_of(4) {
        0.995 + ((ordinal % 10) as f32 / 10_000.0)
    } else {
        (ordinal % 990) as f32 / 1000.0
    }
}

fn edge_streaming_scale_props(
    ordinal: usize,
    status_override: Option<&str>,
) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "status".to_string(),
        PropValue::String(
            status_override
                .unwrap_or(if ordinal.is_multiple_of(2) {
                    "hot"
                } else {
                    "cold"
                })
                .to_string(),
        ),
    );
    props.insert(
        "region".to_string(),
        PropValue::String(
            if ordinal.is_multiple_of(4) {
                "east"
            } else {
                "west"
            }
            .to_string(),
        ),
    );
    props.insert(
        "tenant".to_string(),
        PropValue::String(format!(
            "t{:03}",
            edge_streaming_scale_tenant_ordinal(ordinal)
        )),
    );
    props.insert(
        "score".to_string(),
        PropValue::Int(edge_streaming_scale_score(ordinal)),
    );
    props
}

fn edge_streaming_scale_endpoints(node_ids: &[u64], ordinal: usize) -> (u64, u64) {
    debug_assert!(node_ids.len() >= EDGE_STREAMING_SCALE_NODE_COUNT);
    if ordinal < EDGE_STREAMING_SCALE_HUB_EDGE_COUNT {
        return (node_ids[0], node_ids[2 + ordinal]);
    }
    if ordinal < EDGE_STREAMING_SCALE_HUB_EDGE_COUNT * 2 {
        return (
            node_ids[1],
            node_ids[2 + (ordinal - EDGE_STREAMING_SCALE_HUB_EDGE_COUNT)],
        );
    }

    let local = ordinal - EDGE_STREAMING_SCALE_HUB_EDGE_COUNT * 2;
    let spread = EDGE_STREAMING_SCALE_NODE_COUNT - 2;
    let from_slot = local % spread;
    let offset = (local / spread) + 1;
    let to_slot = (from_slot + offset) % spread;
    (node_ids[2 + from_slot], node_ids[2 + to_slot])
}

fn edge_streaming_scale_input(
    node_ids: &[u64],
    ordinal: usize,
    status_override: Option<&str>,
) -> EdgeInput {
    let (from, to) = edge_streaming_scale_endpoints(node_ids, ordinal);
    EdgeInput {
        from,
        to,
        label: "BenchEdge1".to_string(),
        props: edge_streaming_scale_props(ordinal, status_override),
        weight: edge_streaming_scale_weight(ordinal),
        valid_from: None,
        valid_to: None,
    }
}

fn build_edge_streaming_scale_engine_variant(stale_heavy: bool) -> EdgeStreamingScaleEngine {
    let (dir, engine) = temp_edge_streaming_scale_db();
    ensure_edge_streaming_scale_indexes(&engine);

    let node_ids = engine
        .batch_upsert_nodes(
            (0..EDGE_STREAMING_SCALE_NODE_COUNT)
                .map(edge_streaming_scale_node)
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), EDGE_STREAMING_SCALE_NODE_COUNT);

    let mut edge_ids = Vec::with_capacity(EDGE_STREAMING_SCALE_TOTAL_EDGES);
    for segment in 0..EDGE_STREAMING_SCALE_SEGMENT_COUNT {
        let start = segment * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT;
        let inputs = (start..start + EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT)
            .map(|ordinal| edge_streaming_scale_input(&node_ids, ordinal, None))
            .collect::<Vec<_>>();
        edge_ids.extend(engine.batch_upsert_edges(inputs).unwrap());
        engine.flush().unwrap();
    }

    let tail_start = EDGE_STREAMING_SCALE_SEGMENT_COUNT * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT;
    let tail_inputs = (tail_start..tail_start + EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT)
        .map(|ordinal| edge_streaming_scale_input(&node_ids, ordinal, None))
        .collect::<Vec<_>>();
    edge_ids.extend(engine.batch_upsert_edges(tail_inputs).unwrap());
    assert_eq!(edge_ids.len(), EDGE_STREAMING_SCALE_TOTAL_EDGES);

    let mut stale_segment_hot_ordinals = std::collections::BTreeSet::new();
    let mut stale_rewrite_count = 0usize;
    if stale_heavy {
        let segment_hot_total =
            EDGE_STREAMING_SCALE_SEGMENT_COUNT * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT / 2;
        let stale_target = segment_hot_total * 95 / 100;
        let mut stale_updates = Vec::with_capacity(stale_target);
        for ordinal in (0..tail_start).filter(|ordinal| ordinal.is_multiple_of(2)) {
            if stale_updates.len() == stale_target {
                break;
            }
            stale_segment_hot_ordinals.insert(ordinal);
            stale_updates.push(edge_streaming_scale_input(&node_ids, ordinal, Some("cold")));
        }
        let rewritten_ids = engine.batch_upsert_edges(stale_updates).unwrap();
        stale_rewrite_count = rewritten_ids.len();
        assert_eq!(stale_rewrite_count, stale_target);
        for (ordinal, rewritten_id) in stale_segment_hot_ordinals.iter().zip(rewritten_ids) {
            assert_eq!(
                rewritten_id, edge_ids[*ordinal],
                "stale-heavy edge rewrite must preserve edge ID"
            );
        }
    }

    let hot_ids = edge_ids
        .iter()
        .enumerate()
        .filter_map(|(ordinal, &edge_id)| {
            (ordinal.is_multiple_of(2) && !stale_segment_hot_ordinals.contains(&ordinal))
                .then_some(edge_id)
        })
        .collect::<Vec<_>>();
    let east_count = (0..EDGE_STREAMING_SCALE_TOTAL_EDGES)
        .filter(|ordinal| ordinal.is_multiple_of(4))
        .count();
    let selected_tenant_count = (0..EDGE_STREAMING_SCALE_TOTAL_EDGES)
        .filter(|&ordinal| {
            edge_streaming_scale_tenant_ordinal(ordinal) < STREAMING_SCALE_IN_TENANT_COUNT
        })
        .count();
    let high_score_count = (0..EDGE_STREAMING_SCALE_TOTAL_EDGES)
        .filter(|&ordinal| (990..=999).contains(&edge_streaming_scale_score(ordinal)))
        .count();
    let high_weight_count = (0..EDGE_STREAMING_SCALE_TOTAL_EDGES)
        .filter(|&ordinal| edge_streaming_scale_weight(ordinal) >= 0.99)
        .count();
    assert_eq!(east_count, EDGE_STREAMING_SCALE_TOTAL_EDGES / 4);
    assert!(selected_tenant_count > 4_096);
    assert!(high_score_count > 4_096);
    assert!(high_weight_count > 4_096);
    assert!(hot_ids.len() > 4_096);

    let hub_source_ids = vec![node_ids[0], node_ids[1]];
    EdgeStreamingScaleEngine {
        _dir: dir,
        engine,
        node_ids,
        edge_ids,
        hot_ids,
        hub_source_ids,
        stale_rewrite_count,
    }
}

fn build_edge_streaming_scale_engine() -> EdgeStreamingScaleEngine {
    build_edge_streaming_scale_engine_variant(false)
}

fn build_edge_streaming_scale_engine_stale_heavy() -> EdgeStreamingScaleEngine {
    build_edge_streaming_scale_engine_variant(true)
}

struct GraphRowStreamingScaleEngine {
    edge: EdgeStreamingScaleEngine,
}

fn build_graph_row_streaming_scale_engine() -> GraphRowStreamingScaleEngine {
    GraphRowStreamingScaleEngine {
        edge: build_edge_streaming_scale_engine(),
    }
}

fn build_graph_row_streaming_scale_engine_stale_heavy() -> GraphRowStreamingScaleEngine {
    GraphRowStreamingScaleEngine {
        edge: build_edge_streaming_scale_engine_stale_heavy(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphRowChunkedOracleRow {
    source_id: u64,
    edge_id: u64,
    target_id: u64,
    score: i64,
}

struct GraphRowChunkedScaleEngine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    east_anchor_ids: Vec<u64>,
    edge_count: usize,
    flushed_segment_count: usize,
    active_tail_edge_count: usize,
    hub_source_ids: Option<[u64; 2]>,
    p90_anchor_id: u64,
    rows: Vec<GraphRowChunkedOracleRow>,
    residual_rows: Vec<GraphRowChunkedOracleRow>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphRowChunkedScaleLayout {
    WestRemapped,
    HubPoisoned,
}

fn graph_row_chunked_scale_node(ordinal: usize) -> NodeInput {
    let mut props = BTreeMap::new();
    props.insert(
        "region".to_string(),
        PropValue::String(
            if ordinal >= 2 && ordinal % 4 == 2 {
                "east"
            } else {
                "west"
            }
            .to_string(),
        ),
    );
    props.insert(
        "phase43_residual".to_string(),
        PropValue::String(if ordinal % 8 == 3 { "keep" } else { "drop" }.to_string()),
    );
    NodeInput {
        labels: vec!["GraphRowChunkNode".to_string()],
        key: format!("graph-row-chunk-node-{ordinal}"),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn graph_row_chunked_scale_endpoints(
    node_ids: &[u64],
    west_source_ordinals: &[usize],
    ordinal: usize,
) -> (u64, u64) {
    if ordinal >= EDGE_STREAMING_SCALE_HUB_EDGE_COUNT * 2 {
        return edge_streaming_scale_endpoints(node_ids, ordinal);
    }

    // QPX-034 tracks the planner limitation that globally applies unrelated hub
    // surcharge to the indexed east anchors. Keep these non-anchor ordinals
    // low-fanout and west-only while preserving every later endpoint mapping.
    let west_index = ordinal % west_source_ordinals.len();
    let round = ordinal / west_source_ordinals.len();
    let source_ordinal = west_source_ordinals[west_index];
    let target_ordinal = if source_ordinal < 2 {
        (source_ordinal + 21 + round) % EDGE_STREAMING_SCALE_NODE_COUNT
    } else {
        let source_slot = source_ordinal - 2;
        2 + ((source_slot + 21 + round) % (EDGE_STREAMING_SCALE_NODE_COUNT - 2))
    };
    (node_ids[source_ordinal], node_ids[target_ordinal])
}

fn graph_row_chunked_scale_layout_endpoints(
    layout: GraphRowChunkedScaleLayout,
    node_ids: &[u64],
    west_source_ordinals: &[usize],
    ordinal: usize,
) -> (u64, u64) {
    match layout {
        GraphRowChunkedScaleLayout::WestRemapped => {
            graph_row_chunked_scale_endpoints(node_ids, west_source_ordinals, ordinal)
        }
        GraphRowChunkedScaleLayout::HubPoisoned => {
            edge_streaming_scale_endpoints(node_ids, ordinal)
        }
    }
}

fn graph_row_chunked_scale_input(
    layout: GraphRowChunkedScaleLayout,
    node_ids: &[u64],
    west_source_ordinals: &[usize],
    ordinal: usize,
) -> EdgeInput {
    let (from, to) =
        graph_row_chunked_scale_layout_endpoints(layout, node_ids, west_source_ordinals, ordinal);
    EdgeInput {
        from,
        to,
        label: "BenchEdge1".to_string(),
        props: edge_streaming_scale_props(ordinal, None),
        weight: edge_streaming_scale_weight(ordinal),
        valid_from: None,
        valid_to: None,
    }
}

fn build_graph_row_chunked_scale_engine_variant(
    layout: GraphRowChunkedScaleLayout,
) -> GraphRowChunkedScaleEngine {
    let (dir, engine) = temp_edge_streaming_scale_db();
    engine.ensure_node_label("GraphRowChunkNode").unwrap();
    let region = engine
        .ensure_node_property_index(
            "GraphRowChunkNode",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("region")]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, region.index_id, SecondaryIndexState::Ready);
    ensure_edge_streaming_scale_indexes(&engine);

    let node_ids = engine
        .batch_upsert_nodes(
            (0..EDGE_STREAMING_SCALE_NODE_COUNT)
                .map(graph_row_chunked_scale_node)
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), EDGE_STREAMING_SCALE_NODE_COUNT);
    assert!(node_ids.windows(2).all(|ids| ids[0] < ids[1]));

    let east_ordinals = (2..EDGE_STREAMING_SCALE_NODE_COUNT)
        .step_by(4)
        .collect::<Vec<_>>();
    let east_anchor_ids = east_ordinals
        .iter()
        .map(|&ordinal| node_ids[ordinal])
        .collect::<Vec<_>>();
    assert_eq!(east_anchor_ids.len(), 2_500);
    assert!(east_anchor_ids.windows(2).all(|ids| ids[0] < ids[1]));
    let keep_count = (0..EDGE_STREAMING_SCALE_NODE_COUNT)
        .filter(|ordinal| ordinal % 8 == 3)
        .count();
    assert_eq!(keep_count, 1_250);
    assert_eq!(EDGE_STREAMING_SCALE_NODE_COUNT - keep_count, 8_750);

    let west_source_ordinals = (0..EDGE_STREAMING_SCALE_NODE_COUNT)
        .filter(|&ordinal| ordinal < 2 || ordinal % 4 != 2)
        .collect::<Vec<_>>();
    assert_eq!(west_source_ordinals.len(), 7_500);

    let mut edge_ids = Vec::with_capacity(EDGE_STREAMING_SCALE_TOTAL_EDGES);
    let mut flushed_segment_count = 0usize;
    for segment in 0..EDGE_STREAMING_SCALE_SEGMENT_COUNT {
        let start = segment * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT;
        let inputs = (start..start + EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT)
            .map(|ordinal| {
                graph_row_chunked_scale_input(layout, &node_ids, &west_source_ordinals, ordinal)
            })
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT);
        edge_ids.extend(engine.batch_upsert_edges(inputs).unwrap());
        engine.flush().unwrap();
        flushed_segment_count += 1;
    }
    assert_eq!(flushed_segment_count, EDGE_STREAMING_SCALE_SEGMENT_COUNT);
    let tail_start = EDGE_STREAMING_SCALE_SEGMENT_COUNT * EDGE_STREAMING_SCALE_EDGES_PER_SEGMENT;
    let tail_inputs = (tail_start..tail_start + EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT)
        .map(|ordinal| {
            graph_row_chunked_scale_input(layout, &node_ids, &west_source_ordinals, ordinal)
        })
        .collect::<Vec<_>>();
    assert_eq!(tail_inputs.len(), EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT);
    edge_ids.extend(engine.batch_upsert_edges(tail_inputs).unwrap());
    assert_eq!(edge_ids.len(), EDGE_STREAMING_SCALE_TOTAL_EDGES);
    assert_eq!(
        edge_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        EDGE_STREAMING_SCALE_TOTAL_EDGES
    );

    let mut rows = Vec::with_capacity(47_510);
    let mut residual_rows = Vec::with_capacity(6_253);
    let mut first_eleven_rows = 0usize;
    let mut first_eleven_residual_rows = 0usize;
    let mut high_score_rows = 0usize;
    let mut prefix_source_fanout = BTreeMap::<u64, usize>::new();
    let mut hub_outgoing_fanout = BTreeMap::<u64, usize>::new();
    for ordinal in 0..EDGE_STREAMING_SCALE_TOTAL_EDGES {
        let (source_ordinal, target_ordinal) = {
            let (source_id, target_id) = graph_row_chunked_scale_layout_endpoints(
                layout,
                &node_ids,
                &west_source_ordinals,
                ordinal,
            );
            let source_ordinal = node_ids.binary_search(&source_id).unwrap();
            let target_ordinal = node_ids.binary_search(&target_id).unwrap();
            if ordinal < EDGE_STREAMING_SCALE_HUB_EDGE_COUNT * 2 {
                if layout == GraphRowChunkedScaleLayout::WestRemapped {
                    assert!(source_ordinal < 2 || source_ordinal % 4 != 2);
                }
                *prefix_source_fanout.entry(source_id).or_default() += 1;
            }
            if source_id == node_ids[0] || source_id == node_ids[1] {
                *hub_outgoing_fanout.entry(source_id).or_default() += 1;
            }
            (source_ordinal, target_ordinal)
        };
        if source_ordinal < 2 || source_ordinal % 4 != 2 {
            continue;
        }
        let row = GraphRowChunkedOracleRow {
            source_id: node_ids[source_ordinal],
            edge_id: edge_ids[ordinal],
            target_id: node_ids[target_ordinal],
            score: edge_streaming_scale_score(ordinal),
        };
        rows.push(row);
        if source_ordinal <= 42 {
            first_eleven_rows += 1;
        }
        if ordinal.is_multiple_of(4) {
            high_score_rows += 1;
        }
        if target_ordinal % 8 == 3 {
            residual_rows.push(row);
            if source_ordinal <= 42 {
                first_eleven_residual_rows += 1;
            }
        }
    }
    assert_eq!(rows.len(), 47_510);
    assert_eq!(residual_rows.len(), 6_253);
    assert_eq!(first_eleven_rows, 219);
    assert_eq!(first_eleven_residual_rows, 28);
    assert_eq!(high_score_rows, 25_000);
    let hub_source_ids = match layout {
        GraphRowChunkedScaleLayout::WestRemapped => {
            assert_eq!(prefix_source_fanout.len(), 7_500);
            assert_eq!(prefix_source_fanout.values().copied().max(), Some(2));
            None
        }
        GraphRowChunkedScaleLayout::HubPoisoned => {
            assert_eq!(prefix_source_fanout.len(), 2);
            assert_eq!(prefix_source_fanout.get(&node_ids[0]), Some(&5_000));
            assert_eq!(prefix_source_fanout.get(&node_ids[1]), Some(&5_000));
            assert_eq!(hub_outgoing_fanout.len(), 2);
            assert_eq!(hub_outgoing_fanout.get(&node_ids[0]), Some(&5_000));
            assert_eq!(hub_outgoing_fanout.get(&node_ids[1]), Some(&5_000));
            Some([node_ids[0], node_ids[1]])
        }
    };
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    residual_rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));

    let p90_anchor_index = (9 * east_anchor_ids.len()).div_ceil(10) - 1;
    assert_eq!(p90_anchor_index, 2_249);
    assert_eq!(east_ordinals[p90_anchor_index], 8_998);
    let p90_anchor_id = east_anchor_ids[p90_anchor_index];

    GraphRowChunkedScaleEngine {
        _dir: dir,
        engine,
        node_ids,
        east_anchor_ids,
        edge_count: edge_ids.len(),
        flushed_segment_count,
        active_tail_edge_count: EDGE_STREAMING_SCALE_MEMTABLE_TAIL_COUNT,
        hub_source_ids,
        p90_anchor_id,
        rows,
        residual_rows,
    }
}

fn build_graph_row_chunked_scale_engine() -> GraphRowChunkedScaleEngine {
    build_graph_row_chunked_scale_engine_variant(GraphRowChunkedScaleLayout::WestRemapped)
}

struct GraphRowEdgePullScaleEngine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    edge_ids: Vec<u64>,
    flushed_edge_batches: usize,
    active_tail_edge_count: usize,
    rows: Vec<GraphRowChunkedOracleRow>,
    residual_rows: Vec<GraphRowChunkedOracleRow>,
}

fn graph_row_edgepull_node(ordinal: usize) -> NodeInput {
    let mut props = BTreeMap::new();
    props.insert(
        "epull_residual".to_string(),
        PropValue::String(if ordinal % 8 == 3 { "keep" } else { "drop" }.to_string()),
    );
    NodeInput {
        labels: vec!["EdgePullNode".to_string()],
        key: format!("edge-pull-node-{ordinal}"),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn graph_row_edgepull_endpoints(node_ids: &[u64], ordinal: usize) -> (u64, u64) {
    let source_ordinal = ordinal % GRAPH_ROW_EDGEPULL_NODE_COUNT;
    let target_ordinal = (source_ordinal + 1 + ordinal / GRAPH_ROW_EDGEPULL_NODE_COUNT)
        % GRAPH_ROW_EDGEPULL_NODE_COUNT;
    (node_ids[source_ordinal], node_ids[target_ordinal])
}

fn graph_row_edgepull_score(ordinal: usize) -> i64 {
    ((ordinal * 7_919) % 100_000) as i64
}

fn graph_row_edgepull_input(node_ids: &[u64], ordinal: usize) -> EdgeInput {
    let (from, to) = graph_row_edgepull_endpoints(node_ids, ordinal);
    let mut props = BTreeMap::new();
    props.insert(
        "score".to_string(),
        PropValue::Int(graph_row_edgepull_score(ordinal)),
    );
    EdgeInput {
        from,
        to,
        label: "BenchEdgePull".to_string(),
        props,
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }
}

fn build_graph_row_edgepull_scale_engine() -> GraphRowEdgePullScaleEngine {
    let dir = tempfile::tempdir().unwrap();
    let engine = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_bench_label_tokens(&engine);
    engine.ensure_node_label("EdgePullNode").unwrap();
    engine.ensure_edge_label("BenchEdgePull").unwrap();

    let node_ids = engine
        .batch_upsert_nodes(
            (0..GRAPH_ROW_EDGEPULL_NODE_COUNT)
                .map(graph_row_edgepull_node)
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), GRAPH_ROW_EDGEPULL_NODE_COUNT);
    assert!(node_ids.windows(2).all(|ids| ids[0] < ids[1]));
    let keep_nodes = (0..GRAPH_ROW_EDGEPULL_NODE_COUNT)
        .filter(|ordinal| ordinal % 8 == 3)
        .count();
    assert_eq!(keep_nodes, 1_250);
    assert_eq!(GRAPH_ROW_EDGEPULL_NODE_COUNT - keep_nodes, 8_750);

    let mut edge_ids = Vec::with_capacity(GRAPH_ROW_EDGEPULL_EDGE_COUNT);
    let mut flushed_edge_batches = 0usize;
    for batch in 0..GRAPH_ROW_EDGEPULL_SEGMENT_COUNT {
        let start = batch * GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT;
        let inputs = (start..start + GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT)
            .map(|ordinal| graph_row_edgepull_input(&node_ids, ordinal))
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT);
        edge_ids.extend(engine.batch_upsert_edges(inputs).unwrap());
        engine.flush().unwrap();
        flushed_edge_batches += 1;
    }
    assert_eq!(flushed_edge_batches, GRAPH_ROW_EDGEPULL_SEGMENT_COUNT);

    let tail_start = GRAPH_ROW_EDGEPULL_SEGMENT_COUNT * GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT;
    let tail_inputs = (tail_start..tail_start + GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT)
        .map(|ordinal| graph_row_edgepull_input(&node_ids, ordinal))
        .collect::<Vec<_>>();
    assert_eq!(tail_inputs.len(), GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT);
    edge_ids.extend(engine.batch_upsert_edges(tail_inputs).unwrap());
    assert_eq!(edge_ids.len(), GRAPH_ROW_EDGEPULL_EDGE_COUNT);
    assert!(edge_ids.windows(2).all(|ids| ids[0] < ids[1]));
    assert_eq!(
        edge_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        GRAPH_ROW_EDGEPULL_EDGE_COUNT
    );

    let mut rows = Vec::with_capacity(GRAPH_ROW_EDGEPULL_EDGE_COUNT);
    let mut residual_rows = Vec::with_capacity(GRAPH_ROW_EDGEPULL_EDGE_COUNT / 8);
    let mut outgoing_fanout = vec![0usize; GRAPH_ROW_EDGEPULL_NODE_COUNT];
    let mut unique_edges = std::collections::BTreeSet::new();
    for ordinal in 0..GRAPH_ROW_EDGEPULL_EDGE_COUNT {
        let source_ordinal = ordinal % GRAPH_ROW_EDGEPULL_NODE_COUNT;
        let target_ordinal = (source_ordinal + 1 + ordinal / GRAPH_ROW_EDGEPULL_NODE_COUNT)
            % GRAPH_ROW_EDGEPULL_NODE_COUNT;
        let row = GraphRowChunkedOracleRow {
            source_id: node_ids[source_ordinal],
            edge_id: edge_ids[ordinal],
            target_id: node_ids[target_ordinal],
            score: graph_row_edgepull_score(ordinal),
        };
        assert!(unique_edges.insert((row.source_id, row.target_id)));
        outgoing_fanout[source_ordinal] += 1;
        rows.push(row);
        if target_ordinal % 8 == 3 {
            residual_rows.push(row);
        }
    }
    assert_eq!(unique_edges.len(), GRAPH_ROW_EDGEPULL_EDGE_COUNT);
    assert!(outgoing_fanout.iter().all(|fanout| *fanout == 20));
    assert_eq!(rows.len(), GRAPH_ROW_EDGEPULL_EDGE_COUNT);
    assert_eq!(residual_rows.len(), 25_000);
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    residual_rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));

    GraphRowEdgePullScaleEngine {
        _dir: dir,
        engine,
        node_ids,
        edge_ids,
        flushed_edge_batches,
        active_tail_edge_count: GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT,
        rows,
        residual_rows,
    }
}

struct GraphRowEdgePullHubEvidenceEngine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    hub_edge_ids: Vec<u64>,
    seed_edge_ids: Vec<u64>,
    flushed_hub_batches: usize,
    active_hub_tail_count: usize,
    active_seed_tail_count: usize,
}

fn graph_row_edgepull_qpx036_hub_endpoints(node_ids: &[u64], ordinal: usize) -> (u64, u64) {
    match ordinal {
        0..5_000 => (node_ids[0], node_ids[2 + ordinal]),
        5_000..10_000 => (node_ids[1], node_ids[2 + ordinal - 5_000]),
        _ => {
            let j = ordinal - 10_000;
            let source = j % 9_998;
            let offset = 1 + j / 9_998;
            (
                node_ids[2 + source],
                node_ids[2 + ((source + offset) % 9_998)],
            )
        }
    }
}

fn graph_row_edgepull_qpx036_hub_input(node_ids: &[u64], ordinal: usize) -> EdgeInput {
    let (from, to) = graph_row_edgepull_qpx036_hub_endpoints(node_ids, ordinal);
    EdgeInput {
        from,
        to,
        label: "BenchEdgePullHub".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }
}

fn build_graph_row_edgepull_hub_evidence_engine() -> GraphRowEdgePullHubEvidenceEngine {
    let dir = tempfile::tempdir().unwrap();
    let engine = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_bench_label_tokens(&engine);
    engine.ensure_node_label("EdgePullHubEvidenceNode").unwrap();
    engine.ensure_edge_label("BenchEdgePullSeed").unwrap();
    engine.ensure_edge_label("BenchEdgePullHub").unwrap();

    let node_ids = engine
        .batch_upsert_nodes(
            (0..GRAPH_ROW_EDGEPULL_NODE_COUNT)
                .map(|ordinal| NodeInput {
                    labels: vec!["EdgePullHubEvidenceNode".to_string()],
                    key: format!("edge-pull-hub-evidence-node-{ordinal}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), GRAPH_ROW_EDGEPULL_NODE_COUNT);
    assert!(node_ids.windows(2).all(|ids| ids[0] < ids[1]));

    let mut hub_edge_ids = Vec::with_capacity(GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT);
    let mut flushed_hub_batches = 0usize;
    for batch in 0..GRAPH_ROW_EDGEPULL_SEGMENT_COUNT {
        let start = batch * GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT;
        let inputs = (start..start + GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT)
            .map(|ordinal| graph_row_edgepull_qpx036_hub_input(&node_ids, ordinal))
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT);
        hub_edge_ids.extend(engine.batch_upsert_edges(inputs).unwrap());
        engine.flush().unwrap();
        flushed_hub_batches += 1;
    }
    assert_eq!(flushed_hub_batches, GRAPH_ROW_EDGEPULL_SEGMENT_COUNT);

    let hub_tail_start = GRAPH_ROW_EDGEPULL_SEGMENT_COUNT * GRAPH_ROW_EDGEPULL_EDGES_PER_SEGMENT;
    let hub_tail = (hub_tail_start..GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT)
        .map(|ordinal| graph_row_edgepull_qpx036_hub_input(&node_ids, ordinal))
        .collect::<Vec<_>>();
    assert_eq!(hub_tail.len(), GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT);
    hub_edge_ids.extend(engine.batch_upsert_edges(hub_tail).unwrap());
    assert_eq!(hub_edge_ids.len(), GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT);
    assert!(hub_edge_ids.windows(2).all(|ids| ids[0] < ids[1]));

    let seed_inputs = (0..GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT)
        .map(|ordinal| EdgeInput {
            from: node_ids[2 + ordinal],
            to: node_ids[ordinal % 2],
            label: "BenchEdgePullSeed".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect::<Vec<_>>();
    assert_eq!(seed_inputs.len(), GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT);
    let seed_edge_ids = engine.batch_upsert_edges(seed_inputs).unwrap();
    assert_eq!(
        seed_edge_ids.len(),
        GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT
    );
    assert!(seed_edge_ids.windows(2).all(|ids| ids[0] < ids[1]));
    assert!(hub_edge_ids.last() < seed_edge_ids.first());

    let mut outgoing = vec![0usize; GRAPH_ROW_EDGEPULL_NODE_COUNT];
    for ordinal in 0..GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT {
        let (from, to) = graph_row_edgepull_qpx036_hub_endpoints(&node_ids, ordinal);
        let from_ordinal = node_ids.binary_search(&from).unwrap();
        let to_ordinal = node_ids.binary_search(&to).unwrap();
        assert_ne!(from_ordinal, to_ordinal);
        outgoing[from_ordinal] += 1;
    }
    assert_eq!(outgoing[0], 5_000);
    assert_eq!(outgoing[1], 5_000);
    assert_eq!(outgoing[2..].iter().sum::<usize>(), 190_000);
    assert_eq!(
        outgoing[2..].iter().filter(|fanout| **fanout == 20).count(),
        38
    );
    assert_eq!(
        outgoing[2..].iter().filter(|fanout| **fanout == 19).count(),
        9_960
    );
    assert_eq!(outgoing[2..].iter().copied().min(), Some(19));
    assert_eq!(outgoing[2..].iter().copied().max(), Some(20));

    GraphRowEdgePullHubEvidenceEngine {
        _dir: dir,
        engine,
        node_ids,
        hub_edge_ids,
        seed_edge_ids,
        flushed_hub_batches,
        active_hub_tail_count: GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT,
        active_seed_tail_count: GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT,
    }
}

fn fully_flush_and_reopen_graph_row_edgepull_hub_evidence_engine(
    mut fixture: GraphRowEdgePullHubEvidenceEngine,
) -> GraphRowEdgePullHubEvidenceEngine {
    fixture.engine.flush().unwrap();
    fixture.engine.close().unwrap();
    fixture.engine = DatabaseEngine::open(
        fixture._dir.path(),
        &DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            edge_uniqueness: true,
            ..DbOptions::default()
        },
    )
    .unwrap();
    fixture.flushed_hub_batches += 1;
    fixture.active_hub_tail_count = 0;
    fixture.active_seed_tail_count = 0;
    fixture
}

fn build_graph_row_hub_poisoned_scale_engine() -> GraphRowChunkedScaleEngine {
    build_graph_row_chunked_scale_engine_variant(GraphRowChunkedScaleLayout::HubPoisoned)
}

fn fully_flush_and_reopen_graph_row_hub_poisoned_scale_engine(
    mut fixture: GraphRowChunkedScaleEngine,
) -> GraphRowChunkedScaleEngine {
    fixture.engine.flush().unwrap();
    fixture.engine.close().unwrap();
    fixture.engine = DatabaseEngine::open(
        fixture._dir.path(),
        &DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            edge_uniqueness: true,
            ..DbOptions::default()
        },
    )
    .unwrap();
    fixture.flushed_segment_count += 1;
    fixture.active_tail_edge_count = 0;
    fixture
}

struct GraphRowFo033ScaleEngine {
    graph: GraphRowStreamingScaleEngine,
    selected_role_count: usize,
    optional_edge_count: usize,
}

fn build_graph_row_fo033_scale_engine() -> GraphRowFo033ScaleEngine {
    let graph = build_graph_row_streaming_scale_engine();
    let selected_ordinals = (0..EDGE_STREAMING_SCALE_TOTAL_EDGES)
        .step_by(100)
        .collect::<Vec<_>>();
    let selected_updates = selected_ordinals
        .iter()
        .map(|&ordinal| {
            let mut input = edge_streaming_scale_input(&graph.edge.node_ids, ordinal, None);
            input.props.insert(
                "role".to_string(),
                PropValue::String("selected".to_string()),
            );
            input
        })
        .collect::<Vec<_>>();
    let rewritten = graph
        .edge
        .engine
        .batch_upsert_edges(selected_updates)
        .unwrap();
    assert_eq!(rewritten.len(), selected_ordinals.len());
    for (&ordinal, &edge_id) in selected_ordinals.iter().zip(&rewritten) {
        assert_eq!(edge_id, graph.edge.edge_ids[ordinal]);
    }
    let role = graph
        .edge
        .engine
        .ensure_edge_property_index(
            "BenchEdge1",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(
        &graph.edge.engine,
        role.index_id,
        SecondaryIndexState::Ready,
    );

    let optional_ordinals = selected_ordinals
        .iter()
        .step_by(8)
        .copied()
        .collect::<Vec<_>>();
    let docs = optional_ordinals
        .iter()
        .enumerate()
        .map(|(index, _)| NodeInput {
            labels: vec![bench_node_label(3)],
            key: format!("graph-row-fo033-doc-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect::<Vec<_>>();
    let doc_ids = graph.edge.engine.batch_upsert_nodes(docs).unwrap();
    let optional_edges = optional_ordinals
        .iter()
        .zip(doc_ids.iter())
        .map(|(&ordinal, &doc_id)| {
            let (_, target) = edge_streaming_scale_endpoints(&graph.edge.node_ids, ordinal);
            EdgeInput {
                from: target,
                to: doc_id,
                label: "BenchEdgeOptional".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }
        })
        .collect::<Vec<_>>();
    let optional_edge_count = graph
        .edge
        .engine
        .batch_upsert_edges(optional_edges)
        .unwrap()
        .len();
    assert_eq!(optional_edge_count, optional_ordinals.len());

    GraphRowFo033ScaleEngine {
        graph,
        selected_role_count: selected_ordinals.len(),
        optional_edge_count,
    }
}

fn build_corrected_gate_middle_zone_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    ensure_bench_equality_index(&engine, "anchor");
    ensure_bench_equality_index(&engine, "broad");

    let nodes = (0..10_000)
        .map(|i| {
            let mut props = BTreeMap::new();
            props.insert(
                "anchor".to_string(),
                PropValue::String(if i < 3_000 { "yes" } else { "no" }.to_string()),
            );
            props.insert(
                "broad".to_string(),
                PropValue::String(if i < 9_000 { "yes" } else { "no" }.to_string()),
            );
            NodeInput {
                labels: vec![bench_node_label(1)],
                key: format!("corrected-gate-{i}"),
                props,
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            }
        })
        .collect::<Vec<_>>();
    engine.batch_upsert_nodes(nodes).unwrap();
    engine.flush().unwrap();
    (dir, engine)
}

fn load_query_mixed_sources(engine: &DatabaseEngine, prefix: &str) {
    for segment in 0..QUERY_SEGMENT_COUNT {
        let start = segment * QUERY_NODES_PER_SEGMENT;
        let nodes = query_nodes(prefix, start, QUERY_NODES_PER_SEGMENT);
        engine.batch_upsert_nodes(nodes.clone()).unwrap();
        engine.flush().unwrap();
    }

    let tail_start = QUERY_SEGMENT_COUNT * QUERY_NODES_PER_SEGMENT;
    let tail_nodes = query_nodes(prefix, tail_start, QUERY_MEMTABLE_TAIL_COUNT);
    engine.batch_upsert_nodes(tail_nodes.clone()).unwrap();
}

fn build_fallback_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    load_query_mixed_sources(&engine, "fallback");
    (dir, engine)
}

fn build_compound_query_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let nodes = query_nodes("compound", 0, COMPOUND_BENCH_NODE_COUNT);
    engine.batch_upsert_nodes(nodes.clone()).unwrap();
    engine.flush().unwrap();

    let label = bench_node_label(1);
    let compound = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::property("score"),
            ]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, compound.index_id, SecondaryIndexState::Ready);
    (dir, engine)
}

fn build_small_label_universe_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let filler = query_nodes_with_label_id(2, "large-universe", 0, QUERY_LARGE_UNIVERSE_COUNT);
    engine.batch_upsert_nodes(filler.clone()).unwrap();
    engine.flush().unwrap();

    let segment_small = query_nodes_with_label_id(1, "small-label", 0, QUERY_SMALL_LABEL_COUNT / 2);
    engine.batch_upsert_nodes(segment_small.clone()).unwrap();
    engine.flush().unwrap();

    let memtable_small = query_nodes_with_label_id(
        1,
        "small-label",
        QUERY_SMALL_LABEL_COUNT / 2,
        QUERY_SMALL_LABEL_COUNT - QUERY_SMALL_LABEL_COUNT / 2,
    );
    engine.batch_upsert_nodes(memtable_small.clone()).unwrap();
    (dir, engine)
}

fn two_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
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
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn status_active_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn equality_and_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
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
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn compound_tenant_score_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("t77".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(75))),
                upper: None,
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("inactive".to_string()),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_and_selective_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("t07".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn broad_equality_and_selective_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(99))),
                upper: None,
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn corrected_gate_middle_zone_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "anchor".to_string(),
                value: PropValue::String("yes".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "broad".to_string(),
                value: PropValue::String("yes".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn assert_stream_declined_to_eager_anchor(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::IndexSkippedAsBroad,
            QueryPlanWarning::VerifyOnlyFilter
        ]
    );
    let QueryPlanNode::VerifyNodeFilter { input } = &plan.root else {
        panic!("expected verified eager anchor plan, got {:?}", plan.root);
    };
    assert_eq!(input.as_ref(), &QueryPlanNode::PropertyEqualityIndex);
}

fn assert_middle_zone_declines_stream(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::VerifyNodeFilter { input } = &plan.root else {
        panic!("expected verified eager plan, got {:?}", plan.root);
    };
    let QueryPlanNode::Intersect { inputs, mode } = input.as_ref() else {
        panic!("expected eager intersect, got {:?}", input);
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Eager);
    assert_eq!(
        inputs,
        &vec![
            QueryPlanNode::PropertyEqualityIndex,
            QueryPlanNode::PropertyEqualityIndex
        ]
    );
}

fn streaming_scale_status_region_tenant_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("t000".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn streaming_scale_status_region_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn streaming_scale_status_hot_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn streaming_scale_range_plus_equality_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(990))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(999))),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
        ],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn streaming_scale_in_union_broad_query(limit: Option<usize>) -> NodeQuery {
    let values = (0..STREAMING_SCALE_IN_TENANT_COUNT)
        .map(|tenant| PropValue::String(format!("t{tenant:03}")))
        .collect();
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "tenant".to_string(),
            values,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn streaming_scale_hydrated_final_page_query(after: u64) -> NodeQuery {
    NodeQuery {
        page: PageRequest {
            limit: Some(10),
            after: Some(after),
        },
        ..streaming_scale_status_hot_query(None)
    }
}

fn edge_streaming_scale_status_region_tenant_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("t000".to_string()),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_status_region_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_status_hot_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_range_plus_equality_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(990))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(999))),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_weight_plus_equality_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::WeightRange {
                lower: Some(0.99),
                upper: Some(1.0),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_endpoint_hub_equality_query(
    hub_source_id: u64,
    limit: Option<usize>,
) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        from_ids: vec![hub_source_id],
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_in_union_broad_query(limit: Option<usize>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge1".to_string()),
        filter: Some(EdgeFilterExpr::PropertyIn {
            key: "tenant".to_string(),
            values: (0..STREAMING_SCALE_IN_TENANT_COUNT)
                .map(|tenant| PropValue::String(format!("t{tenant:03}")))
                .collect(),
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn edge_streaming_scale_hydrated_final_page_query(after: u64) -> EdgeQuery {
    EdgeQuery {
        page: PageRequest {
            limit: Some(10),
            after: Some(after),
        },
        ..edge_streaming_scale_status_hot_query(None)
    }
}

fn node_plan_input(plan: &QueryPlanNode) -> &QueryPlanNode {
    match plan {
        QueryPlanNode::VerifyNodeFilter { input } => input.as_ref(),
        other => other,
    }
}

fn edge_plan_input(plan: &QueryPlanNode) -> &QueryPlanNode {
    match plan {
        QueryPlanNode::VerifyEdgeFilter { input } => input.as_ref(),
        other => other,
    }
}

fn plan_contains(plan: &QueryPlanNode, expected: &QueryPlanNode) -> bool {
    if plan == expected {
        return true;
    }
    match plan {
        QueryPlanNode::Intersect { inputs, .. } | QueryPlanNode::Union { inputs, .. } => {
            inputs.iter().any(|input| plan_contains(input, expected))
        }
        QueryPlanNode::StreamedSource { input }
        | QueryPlanNode::BufferedIdSort { input }
        | QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input } => plan_contains(input, expected),
        _ => false,
    }
}

fn assert_streaming_scale_declines_to_eager(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::IndexSkippedAsBroad,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_eq!(
        node_plan_input(&plan.root),
        &QueryPlanNode::PropertyEqualityIndex
    );
}

fn assert_streamed_intersect(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Intersect { mode, .. } = node_plan_input(&plan.root) else {
        panic!("expected streamed intersect, got {:?}", plan.root);
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
}

fn assert_streamed_range_plus_equality(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Intersect { inputs, mode } = node_plan_input(&plan.root) else {
        panic!(
            "expected streamed range/equality intersect, got {:?}",
            plan.root
        );
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert!(inputs.contains(&QueryPlanNode::PropertyEqualityIndex));
    assert!(inputs.contains(&QueryPlanNode::BufferedIdSort {
        input: Box::new(QueryPlanNode::PropertyRangeIndex),
    }));
}

fn assert_streamed_source(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(
        node_plan_input(&plan.root),
        &QueryPlanNode::StreamedSource {
            input: Box::new(QueryPlanNode::PropertyEqualityIndex),
        }
    );
}

fn assert_streamed_union(engine: &DatabaseEngine, query: &NodeQuery) {
    let plan = engine.explain_node_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Union { inputs, mode } = node_plan_input(&plan.root) else {
        panic!("expected streamed union, got {:?}", plan.root);
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert_eq!(inputs.len(), STREAMING_SCALE_IN_TENANT_COUNT);
}

fn assert_edge_stream_declines_to_eager(engine: &DatabaseEngine, query: &EdgeQuery) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::IndexSkippedAsBroad));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(plan_contains(
        edge_plan_input(&plan.root),
        &QueryPlanNode::EdgePropertyEqualityIndex,
    ));
    assert!(
        !matches!(
            edge_plan_input(&plan.root),
            QueryPlanNode::Intersect {
                mode: QueryPlanExecutionMode::Streamed,
                ..
            } | QueryPlanNode::StreamedSource { .. }
                | QueryPlanNode::Union {
                    mode: QueryPlanExecutionMode::Streamed,
                    ..
                }
        ),
        "decline case must stay eager: {plan:?}"
    );
    assert!(
        !plan_contains(
            edge_plan_input(&plan.root),
            &QueryPlanNode::StreamedSource {
                input: Box::new(QueryPlanNode::EdgePropertyEqualityIndex)
            }
        ),
        "decline case must not become streamed: {plan:?}"
    );
}

fn assert_streamed_edge_intersect(engine: &DatabaseEngine, query: &EdgeQuery) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Intersect { inputs, mode } = edge_plan_input(&plan.root) else {
        panic!("expected streamed edge intersect, got {:?}", plan.root);
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert_eq!(
        inputs
            .iter()
            .filter(|input| **input == QueryPlanNode::EdgePropertyEqualityIndex)
            .count(),
        2
    );
}

fn assert_streamed_edge_source(engine: &DatabaseEngine, query: &EdgeQuery) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(
        edge_plan_input(&plan.root),
        &QueryPlanNode::StreamedSource {
            input: Box::new(QueryPlanNode::EdgePropertyEqualityIndex),
        }
    );
}

fn assert_streamed_edge_buffered_input(
    engine: &DatabaseEngine,
    query: &EdgeQuery,
    buffered_input: QueryPlanNode,
) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Intersect { inputs, mode } = edge_plan_input(&plan.root) else {
        panic!(
            "expected streamed buffered edge intersect, got {:?}",
            plan.root
        );
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert!(inputs.contains(&QueryPlanNode::EdgePropertyEqualityIndex));
    assert!(inputs.contains(&QueryPlanNode::BufferedIdSort {
        input: Box::new(buffered_input),
    }));
}

fn assert_streamed_edge_endpoint(engine: &DatabaseEngine, query: &EdgeQuery) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Intersect { inputs, mode } = edge_plan_input(&plan.root) else {
        panic!(
            "expected streamed endpoint edge intersect, got {:?}",
            plan.root
        );
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert!(inputs.contains(&QueryPlanNode::EdgeEndpointAdjacency));
    assert!(inputs.contains(&QueryPlanNode::EdgePropertyEqualityIndex));
}

fn assert_streamed_edge_union(engine: &DatabaseEngine, query: &EdgeQuery) {
    let plan = engine.explain_edge_query(query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    let QueryPlanNode::Union { inputs, mode } = edge_plan_input(&plan.root) else {
        panic!("expected streamed edge union, got {:?}", plan.root);
    };
    assert_eq!(*mode, QueryPlanExecutionMode::Streamed);
    assert_eq!(inputs.len(), STREAMING_SCALE_IN_TENANT_COUNT);
    assert!(
        inputs
            .iter()
            .all(|input| *input == QueryPlanNode::EdgePropertyEqualityIndex),
        "{inputs:?}"
    );
}

fn criterion_filter_matches(candidates: &[&str]) -> bool {
    let mut filters = Vec::new();
    let mut skip_next = false;
    for arg in std::env::args().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if matches!(
            arg.as_str(),
            "--baseline"
                | "--save-baseline"
                | "--load-baseline"
                | "--sample-size"
                | "--measurement-time"
                | "--warm-up-time"
                | "--profile-time"
        ) {
            skip_next = true;
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        filters.push(arg);
    }

    filters.is_empty()
        || filters.iter().any(|filter| {
            candidates
                .iter()
                .any(|candidate| filter.contains(candidate) || candidate.contains(filter))
        })
}

fn criterion_requested_benchmark(group: &str, scenario: &str) -> bool {
    let mut filters = Vec::new();
    let mut skip_next = false;
    for arg in std::env::args().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if matches!(
            arg.as_str(),
            "--baseline"
                | "--save-baseline"
                | "--load-baseline"
                | "--sample-size"
                | "--measurement-time"
                | "--warm-up-time"
                | "--profile-time"
        ) {
            skip_next = true;
            continue;
        }
        if !arg.starts_with('-') {
            filters.push(arg);
        }
    }

    filters.is_empty()
        || filters
            .iter()
            .any(|filter| filter == group || filter.contains(scenario))
}

fn criterion_requested_streaming_scale_group() -> bool {
    const STREAMING_SCALE_FILTERS: &[&str] = &[
        "query_node_planner_streamed",
        "query_node_ids_stream_declines_tiny_anchor_broad_filters",
        "query_node_ids_streamed_two_broad_small_limit",
        "query_node_ids_streamed_single_broad_source_limit_ten",
        "query_node_ids_streamed_stale_heavy_equality",
        "query_node_ids_streamed_range_plus_equality",
        "query_node_ids_streamed_in_union_broad",
        "query_nodes_streamed_hydrated_final_page",
        "explain_node_query_streamed_intersect",
    ];
    criterion_filter_matches(STREAMING_SCALE_FILTERS)
}

fn criterion_requested_edge_streaming_scale_group() -> bool {
    const EDGE_STREAMING_SCALE_FILTERS: &[&str] = &[
        "query_edge_planner_streamed",
        "query_edge_ids_stream_declines_tiny_anchor_broad_filters",
        "query_edge_ids_streamed_two_broad_small_limit",
        "query_edge_ids_streamed_single_broad_source_limit_ten",
        "query_edge_ids_streamed_stale_heavy_equality",
        "query_edge_ids_streamed_range_plus_equality",
        "query_edge_ids_streamed_weight_plus_equality",
        "query_edge_ids_streamed_endpoint_hub_equality",
        "query_edge_ids_streamed_in_union_broad",
        "query_edges_streamed_hydrated_final_page",
        "explain_edge_query_streamed_intersect",
    ];
    criterion_filter_matches(EDGE_STREAMING_SCALE_FILTERS)
}

fn range_stats_selective_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(7))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(7))),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn range_stats_broad_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(99))),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn now_millis_for_bench() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn timestamp_stats_recent_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(now_millis_for_bench().saturating_sub(60_000)),
            upper_ms: None,
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn timestamp_stats_broad_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(i64::MAX),
        }],
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_scan_fallback_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn label_scoped_verify_only_boolean_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "tier".to_string(),
                    value: PropValue::String("gold".to_string()),
                },
            ]),
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tenant".to_string(),
            })),
            NodeFilterExpr::PropertyExists {
                key: "region".to_string(),
            },
            NodeFilterExpr::PropertyMissing {
                key: "deletedAt".to_string(),
            },
        ])),
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn tenant_eq_filter(value: &str) -> NodeFilterExpr {
    NodeFilterExpr::PropertyEquals {
        key: "tenant".to_string(),
        value: PropValue::String(value.to_string()),
    }
}

fn score_at_least_filter(value: i64) -> NodeFilterExpr {
    NodeFilterExpr::PropertyRange {
        key: "score".to_string(),
        lower: Some(PropertyRangeBound::Included(PropValue::Int(value))),
        upper: None,
    }
}

fn boolean_or_union_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::Or(vec![
            tenant_eq_filter("t07"),
            tenant_eq_filter("t11"),
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_in_union_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "tenant".to_string(),
            values: vec![
                PropValue::String("t03".to_string()),
                PropValue::String("t07".to_string()),
                PropValue::String("t11".to_string()),
                PropValue::String("t19".to_string()),
            ],
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_and_or_range_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                tenant_eq_filter("t91"),
                tenant_eq_filter("t95"),
                tenant_eq_filter("t99"),
            ]),
            score_at_least_filter(95),
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_verify_only_label_fallback_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::Or(vec![
            tenant_eq_filter("t07"),
            NodeFilterExpr::PropertyMissing {
                key: "deletedAt".to_string(),
            },
        ])),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn boolean_large_in_verify_only_query(limit: Option<usize>) -> NodeQuery {
    let mut values: Vec<PropValue> = (0..QUERY_LARGE_IN_VALUE_COUNT)
        .map(|index| PropValue::String(format!("missing-region-{index:03}")))
        .collect();
    values.push(PropValue::String("r03".to_string()));

    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "region".to_string(),
            values,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_only_query(limit: Option<usize>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        page: PageRequest { limit, after: None },
        ..Default::default()
    }
}

fn label_with_large_explicit_ids_query(ids: Vec<u64>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        ids,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn label_with_large_keys_query(keys: Vec<String>) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        keys,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn full_scan_query() -> NodeQuery {
    NodeQuery {
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn full_scan_limit_one_query() -> NodeQuery {
    NodeQuery {
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn label_and_full_scan_explain_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "region".to_string(),
            value: PropValue::String("r03".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn build_hot_memtable_equality_planner_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let label = bench_node_label(40);
    let status = engine
        .ensure_node_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let props = BTreeMap::from([("status".to_string(), PropValue::String("hot".to_string()))]);
    engine
        .batch_upsert_nodes(query_nodes_with_props(
            40,
            "hot-eq-segment",
            0,
            MEMTABLE_PLANNER_SEGMENT_COUNT,
            props.clone(),
        ))
        .unwrap();
    engine.flush().unwrap();
    engine
        .batch_upsert_nodes(query_nodes_with_props(
            40,
            "hot-eq-active",
            MEMTABLE_PLANNER_SEGMENT_COUNT,
            MEMTABLE_PLANNER_STRESS_COUNT,
            props,
        ))
        .unwrap();
    (dir, engine)
}

fn hot_memtable_equality_explain_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(40)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn build_memtable_timestamp_planner_engine() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    engine
        .batch_upsert_nodes(query_nodes_with_label_id(
            41,
            "timestamp-segment",
            0,
            MEMTABLE_PLANNER_SEGMENT_COUNT,
        ))
        .unwrap();
    engine.flush().unwrap();
    engine
        .batch_upsert_nodes(query_nodes_with_label_id(
            41,
            "timestamp-active",
            MEMTABLE_PLANNER_SEGMENT_COUNT,
            MEMTABLE_PLANNER_STRESS_COUNT,
        ))
        .unwrap();
    (dir, engine)
}

fn memtable_timestamp_explain_query() -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(41)],
            mode: LabelMatchMode::All,
        }),
        filter: filter_and![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(i64::MAX),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn explicit_ids_query(ids: &[u64]) -> NodeQuery {
    NodeQuery {
        ids: ids.to_vec(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn explicit_ids_and_selective_property_query(ids: &[u64]) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(1)],
            mode: LabelMatchMode::All,
        }),
        ids: ids.to_vec(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("t07".to_string()),
        }],
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn bench_node_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_node_planner");
    group.sample_size(20);

    group.bench_function("query_node_ids_intersected_two_equality_predicates", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = two_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_nodes_intersected_equality_and_range_hydrated", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_scan_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = label_scan_fallback_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_label_scoped_verify_only_boolean_filter",
        |b| {
            let (_dir, engine) = build_fallback_query_engine();
            let query = label_scoped_verify_only_boolean_query();
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_node_ids_boolean_or_union", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_or_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_in_union", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_in_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_and_or_range", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_and_or_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_boolean_verify_only_label_fallback", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = boolean_verify_only_label_fallback_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_boolean_large_in_verify_only_label_fallback",
        |b| {
            let (_dir, engine) = build_fallback_query_engine();
            let query = boolean_large_in_verify_only_query(Some(QUERY_LIMIT));
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_nodes_boolean_hydrated_final_page", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let all_ids = engine
            .query_node_ids(&NodeQuery {
                page: PageRequest {
                    limit: None,
                    after: None,
                },
                ..boolean_or_union_query(None)
            })
            .unwrap()
            .items;
        let after = all_ids
            .get(all_ids.len().saturating_sub(2))
            .copied()
            .unwrap_or_default();
        let query = NodeQuery {
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: Some(after),
            },
            ..boolean_or_union_query(None)
        };
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_only", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = label_only_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_vs_large_explicit_ids", |b| {
        let (_dir, engine) = build_small_label_universe_engine();
        let ids = (1..=(QUERY_LARGE_UNIVERSE_COUNT + QUERY_SMALL_LABEL_COUNT) as u64).collect();
        let query = label_with_large_explicit_ids_query(ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_large_explicit_ids", |b| {
        let (_dir, engine) = build_small_label_universe_engine();
        let ids = (1..=(QUERY_LARGE_UNIVERSE_COUNT + QUERY_SMALL_LABEL_COUNT) as u64).collect();
        let query = label_with_large_explicit_ids_query(ids);
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_label_vs_large_keys", |b| {
        let (_dir, engine) = build_small_label_universe_engine();
        let mut keys: Vec<String> = (0..QUERY_LARGE_UNIVERSE_COUNT)
            .map(|i| format!("missing-key-{i}"))
            .collect();
        keys.extend((0..QUERY_SMALL_LABEL_COUNT).map(|i| format!("small-label-{i}")));
        let query = label_with_large_keys_query(keys);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_explicit_full_scan_opt_in", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = full_scan_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_explicit_ids_verify", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let nodes = query_nodes("explicit", 0, 512);
        let ids = engine.batch_upsert_nodes(nodes.clone()).unwrap();
        let query = explicit_ids_query(&ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_vs_hydrated_payload_ids", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_vs_hydrated_payload_nodes", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_intersected_predicates", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = equality_and_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_broad_equality", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_legal_universe_reuse", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = label_and_full_scan_explain_query();
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_hot_memtable_equality_estimate", |b| {
        let (_dir, engine) = build_hot_memtable_equality_planner_engine();
        let query = hot_memtable_equality_explain_query();
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_memtable_timestamp_estimate", |b| {
        let (_dir, engine) = build_memtable_timestamp_planner_engine();
        let query = memtable_timestamp_explain_query();
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_broad_equality_selective_equality", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_broad_equality_selective_range", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_range_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_stream_declines_tiny_anchor_broad_filters",
        |b| {
            let (_dir, engine) = build_indexed_query_engine();
            let query = broad_equality_and_selective_equality_query(Some(QUERY_LIMIT));
            assert_stream_declined_to_eager_anchor(&engine, &query);
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function(
        "query_node_ids_corrected_gate_middle_zone_declines_stream",
        |b| {
            let (_dir, engine) = build_corrected_gate_middle_zone_engine();
            let query = corrected_gate_middle_zone_query(None);
            assert_middle_zone_declines_stream(&engine, &query);
            b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_node_ids_range_stats_selective", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_selective_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_range_stats_broad_fallback", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_broad_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_timestamp_stats_recent_window", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = timestamp_stats_recent_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_timestamp_stats_broad_window", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = timestamp_stats_broad_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_stale_heavy_equality_visible_small", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_and_selective_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_heavy_hitter_equality_skipped", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = broad_equality_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_in_heavy_hitter_vs_rare_values", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_in_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_or_union_stats_costed", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = boolean_or_union_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_range_no_candidate_probe", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = range_stats_selective_query(Some(QUERY_LIMIT));
        b.iter(|| black_box(engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_large_explicit_ids_selective_index", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let label = bench_node_label(1);
        let ids = engine.nodes_by_labels(&label).unwrap();
        let query = explicit_ids_and_selective_property_query(&ids);
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_full_scan_limit_one", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let query = full_scan_limit_one_query();
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_nodes_hydrated_final_page", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let all_ids = engine
            .query_node_ids(&NodeQuery {
                page: PageRequest {
                    limit: None,
                    after: None,
                },
                ..broad_equality_and_selective_equality_query(None)
            })
            .unwrap()
            .items;
        let after = all_ids
            .get(all_ids.len().saturating_sub(2))
            .copied()
            .unwrap_or_default();
        let query = NodeQuery {
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: Some(after),
            },
            ..broad_equality_and_selective_equality_query(None)
        };
        b.iter(|| black_box(engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.finish();
}

fn bench_streamed_node_queries(c: &mut Criterion) {
    if !criterion_requested_streaming_scale_group() {
        return;
    }

    // Build cost note: this group constructs one 200k-node scale fixture and one
    // stale-heavy variant outside measured iterations so scenario medians measure
    // query work, not fixture construction.
    let normal = build_streaming_scale_engine();
    let stale = build_streaming_scale_engine_stale_heavy();
    let mut group = c.benchmark_group("query_node_planner_streamed");
    group.sample_size(10);

    group.bench_function(
        "query_node_ids_stream_declines_tiny_anchor_broad_filters",
        |b| {
            let query = streaming_scale_status_region_tenant_query(Some(QUERY_LIMIT));
            assert_streaming_scale_declines_to_eager(&normal.engine, &query);
            b.iter(|| black_box(normal.engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_node_ids_streamed_two_broad_small_limit", |b| {
        let query = streaming_scale_status_region_query(Some(10));
        assert_streamed_intersect(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_node_ids_streamed_single_broad_source_limit_ten",
        |b| {
            let query = streaming_scale_status_hot_query(Some(10));
            assert_streamed_source(&normal.engine, &query);
            b.iter(|| black_box(normal.engine.query_node_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_node_ids_streamed_stale_heavy_equality", |b| {
        let query = streaming_scale_status_hot_query(Some(QUERY_LIMIT));
        assert_streamed_source(&stale.engine, &query);
        b.iter(|| black_box(stale.engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_streamed_range_plus_equality", |b| {
        let query = streaming_scale_range_plus_equality_query(Some(QUERY_LIMIT));
        assert_streamed_range_plus_equality(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_node_ids_streamed_in_union_broad", |b| {
        let query = streaming_scale_in_union_broad_query(Some(QUERY_LIMIT));
        assert_streamed_union(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_nodes_streamed_hydrated_final_page", |b| {
        let after = normal.hot_ids[normal.hot_ids.len() - 11];
        let query = streaming_scale_hydrated_final_page_query(after);
        assert_streamed_source(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_nodes(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_node_query_streamed_intersect", |b| {
        let query = streaming_scale_status_region_query(Some(10));
        assert_streamed_intersect(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.explain_node_query(black_box(&query)).unwrap()));
    });

    group.finish();
}

fn bench_streamed_edge_queries(c: &mut Criterion) {
    if !criterion_requested_edge_streaming_scale_group() {
        return;
    }

    // Build cost note: this group constructs one 200k-edge scale fixture and one
    // stale-heavy variant outside measured iterations so scenario medians measure
    // query work, not fixture construction.
    let normal = build_edge_streaming_scale_engine();
    let stale = build_edge_streaming_scale_engine_stale_heavy();
    assert_eq!(normal.edge_ids.len(), EDGE_STREAMING_SCALE_TOTAL_EDGES);
    assert_eq!(stale.stale_rewrite_count, 85_500);

    let mut group = c.benchmark_group("query_edge_planner_streamed");
    group.sample_size(10);

    group.bench_function(
        "query_edge_ids_stream_declines_tiny_anchor_broad_filters",
        |b| {
            let query = edge_streaming_scale_status_region_tenant_query(Some(QUERY_LIMIT));
            assert_edge_stream_declines_to_eager(&normal.engine, &query);
            b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_edge_ids_streamed_two_broad_small_limit", |b| {
        let query = edge_streaming_scale_status_region_query(Some(10));
        assert_streamed_edge_intersect(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function(
        "query_edge_ids_streamed_single_broad_source_limit_ten",
        |b| {
            let query = edge_streaming_scale_status_hot_query(Some(10));
            assert_streamed_edge_source(&normal.engine, &query);
            b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
        },
    );

    group.bench_function("query_edge_ids_streamed_stale_heavy_equality", |b| {
        let query = edge_streaming_scale_status_hot_query(Some(QUERY_LIMIT));
        assert_streamed_edge_source(&stale.engine, &query);
        b.iter(|| black_box(stale.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_streamed_range_plus_equality", |b| {
        let query = edge_streaming_scale_range_plus_equality_query(Some(QUERY_LIMIT));
        assert_streamed_edge_buffered_input(
            &normal.engine,
            &query,
            QueryPlanNode::EdgePropertyRangeIndex,
        );
        b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_streamed_weight_plus_equality", |b| {
        let query = edge_streaming_scale_weight_plus_equality_query(Some(QUERY_LIMIT));
        assert_streamed_edge_buffered_input(&normal.engine, &query, QueryPlanNode::EdgeWeightIndex);
        b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_streamed_endpoint_hub_equality", |b| {
        let query = edge_streaming_scale_endpoint_hub_equality_query(
            normal.hub_source_ids[0],
            Some(QUERY_LIMIT),
        );
        assert_streamed_edge_endpoint(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_streamed_in_union_broad", |b| {
        let query = edge_streaming_scale_in_union_broad_query(Some(QUERY_LIMIT));
        assert_streamed_edge_union(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edges_streamed_hydrated_final_page", |b| {
        let after = normal.hot_ids[normal.hot_ids.len() - 11];
        let query = edge_streaming_scale_hydrated_final_page_query(after);
        assert_streamed_edge_source(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.query_edges(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_edge_query_streamed_intersect", |b| {
        let query = edge_streaming_scale_status_region_query(Some(10));
        assert_streamed_edge_intersect(&normal.engine, &query);
        b.iter(|| black_box(normal.engine.explain_edge_query(black_box(&query)).unwrap()));
    });

    group.finish();
}

fn bench_compound_index_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("compound_index_queries");
    group.sample_size(20);

    group.bench_function("native_prefix_range_declared", |b| {
        let (_dir, engine) = build_compound_query_engine();
        let query = compound_tenant_score_query(Some(QUERY_LIMIT));
        b.iter(|| {
            let result = engine.query_node_ids(black_box(&query)).unwrap();
            assert_eq!(result.items.len(), 20);
            black_box(result)
        });
    });

    group.bench_function("gql_prefix_range_declared", |b| {
        let (_dir, engine) = build_compound_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (n:BenchNode1) WHERE n.tenant = 't77' AND n.score >= 75 RETURN id(n) LIMIT 100";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.kind, GqlStatementKind::Query);
            assert_eq!(result.rows.len(), 20);
            black_box(result)
        });
    });

    group.finish();
}

fn edge_query_props(i: usize) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "role".to_string(),
        PropValue::String(
            if i.is_multiple_of(10) {
                "lead"
            } else {
                "member"
            }
            .to_string(),
        ),
    );
    props.insert("score".to_string(), PropValue::Int((i % 100) as i64));
    props
}

fn build_edge_query_engine() -> (tempfile::TempDir, DatabaseEngine, u64, Vec<u64>, i64) {
    let (dir, engine) = temp_db();
    let edge_count = QUERY_NODES_PER_SEGMENT + QUERY_MEMTABLE_TAIL_COUNT;
    let valid_epoch = 1_700_000_000_100i64;
    let mut nodes = Vec::with_capacity(edge_count + 1);
    nodes.push(NodeInput {
        labels: vec![bench_node_label(1)],
        key: "edge-query-source".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    });
    nodes.extend((0..edge_count).map(|i| NodeInput {
        labels: vec![bench_node_label(2)],
        key: format!("edge-query-target-{i}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let node_ids = engine.batch_upsert_nodes(nodes.clone()).unwrap();
    let source_id = node_ids[0];
    let target_ids = &node_ids[1..];
    let make_edges = |start: usize, count: usize| -> Vec<EdgeInput> {
        (start..start + count)
            .map(|i| EdgeInput {
                from: source_id,
                to: target_ids[i],
                label: "BenchEdge10".to_string(),
                props: edge_query_props(i),
                weight: if i.is_multiple_of(2) { 2.0 } else { 0.5 },
                valid_from: Some(1_700_000_000_000),
                valid_to: Some(1_700_000_010_000),
            })
            .collect()
    };

    let mut edge_ids = engine
        .batch_upsert_edges(make_edges(0, QUERY_NODES_PER_SEGMENT))
        .unwrap();
    engine.flush().unwrap();
    edge_ids.extend(
        engine
            .batch_upsert_edges(make_edges(
                QUERY_NODES_PER_SEGMENT,
                QUERY_MEMTABLE_TAIL_COUNT,
            ))
            .unwrap(),
    );
    (dir, engine, source_id, edge_ids, valid_epoch)
}

fn build_edge_query_indexed_engine() -> (tempfile::TempDir, DatabaseEngine, u64, Vec<u64>, i64) {
    let (dir, engine, source_id, edge_ids, valid_epoch) = build_edge_query_engine();
    let label = "BenchEdge10".to_string();
    let role = engine
        .ensure_edge_property_index(
            &label,
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("role")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, role.index_id, SecondaryIndexState::Ready);
    let score = engine
        .ensure_edge_property_index(
            &label,
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("score")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    (dir, engine, source_id, edge_ids, valid_epoch)
}

fn edge_query_with_filter(source_id: u64, filter: Option<EdgeFilterExpr>) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge10".to_string()),
        from_ids: vec![source_id],
        filter,
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn edge_legal_universe_explain_query(source_id: u64) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge10".to_string()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(80))),
            upper: None,
        }),
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        allow_full_scan: true,
        ..Default::default()
    }
}

fn build_memtable_edge_metadata_planner_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db();
    let edge_count = MEMTABLE_PLANNER_SEGMENT_COUNT + MEMTABLE_PLANNER_STRESS_COUNT;
    let mut nodes = Vec::with_capacity(edge_count + 1);
    nodes.push(NodeInput {
        labels: vec![bench_node_label(42)],
        key: "edge-metadata-source".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    });
    nodes.extend((0..edge_count).map(|i| NodeInput {
        labels: vec![bench_node_label(43)],
        key: format!("edge-metadata-target-{i}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let source_id = node_ids[0];
    let target_ids = &node_ids[1..];
    engine.flush().unwrap();

    let make_edges = |start: usize, count: usize| -> Vec<EdgeInput> {
        (start..start + count)
            .map(|i| EdgeInput {
                from: source_id,
                to: target_ids[i],
                label: "BenchEdge42".to_string(),
                props: edge_query_props(i),
                weight: if i.is_multiple_of(2) { 2.0 } else { 0.5 },
                valid_from: Some(1_700_000_000_000),
                valid_to: Some(1_700_000_010_000),
            })
            .collect()
    };
    engine
        .batch_upsert_edges(make_edges(0, MEMTABLE_PLANNER_SEGMENT_COUNT))
        .unwrap();
    engine.flush().unwrap();
    engine
        .batch_upsert_edges(make_edges(
            MEMTABLE_PLANNER_SEGMENT_COUNT,
            MEMTABLE_PLANNER_STRESS_COUNT,
        ))
        .unwrap();

    (dir, engine, source_id)
}

fn memtable_edge_metadata_explain_query(source_id: u64) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge42".to_string()),
        from_ids: vec![source_id],
        filter: Some(EdgeFilterExpr::UpdatedAtRange {
            lower_ms: Some(0),
            upper_ms: Some(i64::MAX),
        }),
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn build_many_endpoint_estimate_planner_engine(
    source_count: usize,
) -> (tempfile::TempDir, DatabaseEngine, Vec<u64>) {
    let (dir, engine) = temp_db();
    let mut nodes = Vec::with_capacity(source_count * 2);
    nodes.extend((0..source_count).map(|index| NodeInput {
        labels: vec![bench_node_label(60)],
        key: format!("endpoint-estimate-source-{index}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    nodes.extend((0..source_count).map(|index| NodeInput {
        labels: vec![bench_node_label(61)],
        key: format!("endpoint-estimate-target-{index}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let ids = engine.batch_upsert_nodes(nodes).unwrap();
    let source_ids = ids[..source_count].to_vec();
    let target_ids = &ids[source_count..];
    let make_edges = |start: usize, count: usize| -> Vec<EdgeInput> {
        (start..start + count)
            .map(|index| EdgeInput {
                from: source_ids[index],
                to: target_ids[index],
                label: "BenchEdge60".to_string(),
                props: BTreeMap::new(),
                weight: if index.is_multiple_of(2) { 2.0 } else { 1.0 },
                valid_from: None,
                valid_to: None,
            })
            .collect()
    };
    let half = source_count / 2;
    engine.batch_upsert_edges(make_edges(0, half)).unwrap();
    engine.flush().unwrap();
    engine
        .batch_upsert_edges(make_edges(half, source_count - half))
        .unwrap();
    (dir, engine, source_ids)
}

fn many_endpoint_estimate_explain_query(source_ids: &[u64]) -> EdgeQuery {
    EdgeQuery {
        label: Some("BenchEdge60".to_string()),
        from_ids: source_ids.to_vec(),
        filter: Some(EdgeFilterExpr::WeightRange {
            lower: Some(1.0),
            upper: None,
        }),
        page: PageRequest {
            limit: Some(QUERY_LIMIT),
            after: None,
        },
        ..Default::default()
    }
}

fn bench_edge_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_edge_planner");
    group.sample_size(20);

    group.bench_function("query_edge_ids_explicit_ids", |b| {
        let (_dir, engine, _source_id, edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            ids: edge_ids.iter().take(512).copied().collect(),
            filter: Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_label_only", |b| {
        let (_dir, engine, _source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            label: Some("BenchEdge10".to_string()),
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_from_endpoint_label", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(source_id, None);
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_endpoint_list_label", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = EdgeQuery {
            label: Some("BenchEdge10".to_string()),
            endpoint_ids: vec![source_id],
            page: PageRequest {
                limit: Some(QUERY_LIMIT),
                after: None,
            },
            ..Default::default()
        };
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_weight_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_updated_at_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::UpdatedAtRange {
                lower_ms: Some(0),
                upper_ms: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_valid_at_endpoint", |b| {
        let (_dir, engine, source_id, _edge_ids, valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::ValidAt {
                epoch_ms: valid_epoch,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_verifier_bounded", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_indexed_equality", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edge_ids_property_indexed_range", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("query_edges_metadata_final_page", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::WeightRange {
                lower: Some(1.0),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edges(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_edge_query_legal_universe_reuse", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_legal_universe_explain_query(source_id);
        b.iter(|| black_box(engine.explain_edge_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_edge_query_memtable_metadata_estimates", |b| {
        let (_dir, engine, source_id) = build_memtable_edge_metadata_planner_engine();
        let query = memtable_edge_metadata_explain_query(source_id);
        b.iter(|| black_box(engine.explain_edge_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_edge_query_many_endpoint_estimates", |b| {
        let (_dir, engine, source_ids) =
            build_many_endpoint_estimate_planner_engine(ENDPOINT_ESTIMATE_PLANNER_NODE_COUNT);
        let query = many_endpoint_estimate_explain_query(&source_ids);
        b.iter(|| black_box(engine.explain_edge_query(black_box(&query)).unwrap()));
    });

    group.bench_function("explain_edge_query_large_endpoint_ids", |b| {
        let (_dir, engine, source_ids) =
            build_many_endpoint_estimate_planner_engine(LARGE_ENDPOINT_PLANNER_NODE_COUNT);
        let query = many_endpoint_estimate_explain_query(&source_ids);
        b.iter(|| black_box(engine.explain_edge_query(black_box(&query)).unwrap()));
    });

    group.finish();

    let mut property_group = c.benchmark_group("edge_property_index_queries");
    property_group.sample_size(20);

    property_group.bench_function("equality_fallback_scan", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("equality_declared", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("range_fallback_scan", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.bench_function("range_declared", |b| {
        let (_dir, engine, source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let query = edge_query_with_filter(
            source_id,
            Some(EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(90))),
                upper: None,
            }),
        );
        b.iter(|| black_box(engine.query_edge_ids(black_box(&query)).unwrap()));
    });

    property_group.finish();
}

fn build_pattern_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, mut engine) = temp_db();
    ensure_query_indexes(&mut engine);
    let account_inputs = query_nodes("acct", 0, 1_000);
    let account_ids = engine.batch_upsert_nodes(account_inputs.clone()).unwrap();

    let companies: Vec<NodeInput> = (0..200)
        .map(|i| NodeInput {
            labels: vec![bench_node_label(2)],
            key: format!("company-{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let company_ids = engine.batch_upsert_nodes(companies.clone()).unwrap();

    let edges: Vec<EdgeInput> = account_ids
        .iter()
        .enumerate()
        .map(|(i, &from)| EdgeInput {
            from,
            to: company_ids[i % company_ids.len()],
            label: "BenchEdge10".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(edges.clone()).unwrap();
    engine.flush().unwrap();
    (dir, engine, company_ids[0])
}

const GRAPH_ROW_BENCH_EDGES: usize = 1_000;

fn build_graph_row_optional_engine() -> (tempfile::TempDir, DatabaseEngine, u64) {
    let (dir, engine) = temp_db();
    let source = engine
        .batch_upsert_nodes(vec![NodeInput {
            labels: vec![bench_node_label(1)],
            key: "graph-row-source".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    let targets: Vec<NodeInput> = (0..GRAPH_ROW_BENCH_EDGES)
        .map(|index| NodeInput {
            labels: vec![bench_node_label(2)],
            key: format!("graph-row-target-{index:04}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let target_ids = engine.batch_upsert_nodes(targets.clone()).unwrap();
    let work_edges: Vec<EdgeInput> = target_ids
        .iter()
        .enumerate()
        .map(|(index, &target)| {
            let mut props = BTreeMap::new();
            props.insert(
                "role".to_string(),
                PropValue::String(if index % 10 == 0 { "lead" } else { "member" }.to_string()),
            );
            props.insert("score".to_string(), PropValue::Int((index % 100) as i64));
            EdgeInput {
                from: source,
                to: target,
                label: "BenchEdge10".to_string(),
                props,
                weight: if index % 2 == 0 { 2.0 } else { 0.5 },
                valid_from: None,
                valid_to: None,
            }
        })
        .collect();
    engine.batch_upsert_edges(work_edges).unwrap();
    let docs: Vec<NodeInput> = (0..GRAPH_ROW_BENCH_EDGES)
        .step_by(8)
        .map(|index| NodeInput {
            labels: vec![bench_node_label(3)],
            key: format!("graph-row-doc-{index:04}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let doc_ids = engine.batch_upsert_nodes(docs.clone()).unwrap();
    let mention_edges: Vec<EdgeInput> = doc_ids
        .iter()
        .enumerate()
        .map(|(doc_index, &doc_id)| EdgeInput {
            from: target_ids[doc_index * 8],
            to: doc_id,
            label: "BenchEdge20".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(mention_edges).unwrap();
    engine.flush().unwrap();
    (dir, engine, source)
}

fn graph_row_return_binding(alias: &str) -> GraphReturnItem {
    GraphReturnItem {
        expr: GraphExpr::Binding(alias.to_string()),
        alias: Some(alias.to_string()),
        projection: GraphReturnProjection::IdOnly,
    }
}

fn graph_row_fixed_query(source_id: u64, limit: usize) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![bench_node_label(1)],
                    mode: LabelMatchMode::All,
                }),
                ids: vec![source_id],
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![bench_node_label(2)],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BenchEdge10".to_string()],
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "role".to_string(),
                value: PropValue::String("lead".to_string()),
            }),
        })],
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
        ]),
        order_by: graph_row_order_by_score_then_target(),
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

fn graph_row_optional_query(source_id: u64, limit: usize) -> overgraph::GraphRowQuery {
    let mut query = graph_row_fixed_query(source_id, limit);
    query.nodes.push(GraphNodePattern {
        alias: "doc".to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(3)],
            mode: LabelMatchMode::All,
        }),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: None,
    });
    query
        .pieces
        .push(GraphPatternPiece::Optional(overgraph::GraphOptionalGroup {
            pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("ref".to_string()),
                from_alias: "target".to_string(),
                to_alias: "doc".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdge20".to_string()],
                filter: None,
            })],
            where_: None,
        }));
    query.return_items = Some(vec![
        graph_row_return_binding("source"),
        graph_row_return_binding("edge"),
        graph_row_return_binding("target"),
        graph_row_return_binding("ref"),
        graph_row_return_binding("doc"),
    ]);
    query
}

fn build_graph_row_planner_memo_engine() -> (tempfile::TempDir, DatabaseEngine, Vec<u64>) {
    let (dir, engine) = temp_db();
    let mut nodes = Vec::with_capacity(GRAPH_ROW_PLANNER_EDGE_COUNT + 1);
    nodes.push(NodeInput {
        labels: vec![bench_node_label(10)],
        key: "graph-row-planner-hub".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    });
    nodes.extend((0..GRAPH_ROW_PLANNER_EDGE_COUNT).map(|index| NodeInput {
        labels: vec![bench_node_label(11)],
        key: format!("graph-row-planner-leaf-{index}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub_id = node_ids[0];
    let edge_inputs = node_ids
        .iter()
        .skip(1)
        .enumerate()
        .map(|(index, &leaf_id)| {
            let mut props = BTreeMap::new();
            props.insert(
                "status".to_string(),
                PropValue::String("active".to_string()),
            );
            props.insert("rank".to_string(), PropValue::Int(index as i64));
            EdgeInput {
                from: hub_id,
                to: leaf_id,
                label: "BenchEdge30".to_string(),
                props,
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }
        })
        .collect::<Vec<_>>();
    engine.batch_upsert_edges(edge_inputs).unwrap();
    engine.flush().unwrap();

    let status = engine
        .ensure_edge_property_index(
            "BenchEdge30",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    let rank = engine
        .ensure_edge_property_index(
            "BenchEdge30",
            SecondaryIndexSpec::range(vec![SecondaryIndexField::property("rank")]),
        )
        .unwrap();
    wait_for_edge_property_index_state(&engine, rank.index_id, SecondaryIndexState::Ready);

    (dir, engine, node_ids)
}

fn graph_row_planner_memo_query(node_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut nodes = Vec::with_capacity(GRAPH_ROW_PLANNER_EDGE_COUNT + 1);
    nodes.push(GraphNodePattern {
        alias: "hub".to_string(),
        label_filter: Some(NodeLabelFilter {
            labels: vec![bench_node_label(10)],
            mode: LabelMatchMode::All,
        }),
        ids: vec![node_ids[0]],
        keys: Vec::new(),
        filter: None,
    });
    for index in 0..GRAPH_ROW_PLANNER_EDGE_COUNT {
        nodes.push(GraphNodePattern {
            alias: format!("leaf{index}"),
            label_filter: Some(NodeLabelFilter {
                labels: vec![bench_node_label(11)],
                mode: LabelMatchMode::All,
            }),
            ids: vec![node_ids[index + 1]],
            keys: Vec::new(),
            filter: None,
        });
    }

    let pieces = (0..GRAPH_ROW_PLANNER_EDGE_COUNT)
        .map(|index| {
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some(format!("edge{index}")),
                from_alias: "hub".to_string(),
                to_alias: format!("leaf{index}"),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdge30".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }),
            })
        })
        .collect();

    overgraph::GraphRowQuery {
        nodes,
        pieces,
        where_: None,
        return_items: Some(vec![graph_row_return_binding("hub")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: QUERY_LIMIT,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions::default(),
    }
}

fn build_graph_row_active_memtable_planner_engine() -> (tempfile::TempDir, DatabaseEngine, Vec<u64>)
{
    let (dir, engine) = temp_db();
    let filler_count = MEMTABLE_PLANNER_STRESS_COUNT;
    let mut nodes = Vec::with_capacity(GRAPH_ROW_PLANNER_EDGE_COUNT + filler_count + 1);
    nodes.push(NodeInput {
        labels: vec![bench_node_label(50)],
        key: "graph-row-active-hub".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    });
    nodes.extend((0..GRAPH_ROW_PLANNER_EDGE_COUNT).map(|index| NodeInput {
        labels: vec![bench_node_label(51)],
        key: format!("graph-row-active-leaf-{index}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    nodes.extend((0..filler_count).map(|index| NodeInput {
        labels: vec![bench_node_label(52)],
        key: format!("graph-row-active-filler-{index}"),
        props: BTreeMap::new(),
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }));
    let node_ids = engine.batch_upsert_nodes(nodes).unwrap();
    let hub_id = node_ids[0];
    let mut edge_inputs = Vec::with_capacity(GRAPH_ROW_PLANNER_EDGE_COUNT + filler_count);
    edge_inputs.extend((0..GRAPH_ROW_PLANNER_EDGE_COUNT).map(|index| EdgeInput {
        from: hub_id,
        to: node_ids[index + 1],
        label: "BenchEdge50".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }));
    edge_inputs.extend((0..filler_count).map(|index| EdgeInput {
        from: hub_id,
        to: node_ids[GRAPH_ROW_PLANNER_EDGE_COUNT + 1 + index],
        label: "BenchEdge51".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }));
    engine.batch_upsert_edges(edge_inputs).unwrap();
    (
        dir,
        engine,
        node_ids[..=GRAPH_ROW_PLANNER_EDGE_COUNT].to_vec(),
    )
}

fn graph_row_active_memtable_planner_query(node_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut query = graph_row_planner_memo_query(node_ids);
    for piece in &mut query.pieces {
        if let GraphPatternPiece::Edge(edge) = piece {
            edge.label_filter = vec!["BenchEdge50".to_string()];
            edge.filter = None;
        }
    }
    query.nodes[0].label_filter = Some(NodeLabelFilter {
        labels: vec![bench_node_label(50)],
        mode: LabelMatchMode::All,
    });
    for node in query.nodes.iter_mut().skip(1) {
        node.label_filter = Some(NodeLabelFilter {
            labels: vec![bench_node_label(51)],
            mode: LabelMatchMode::All,
        });
    }
    query
}

fn graph_row_no_plan_many_alternatives_query(node_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut query = graph_row_planner_memo_query(node_ids);
    query.page.limit = 1;
    query.options.include_plan = false;
    if let Some(hub) = query.nodes.first_mut() {
        // Keep the physical planner's alternatives but make execution finish
        // quickly once the chosen explicit node source is materialized.
        hub.ids = vec![u64::MAX - 17];
    }
    query
}

fn graph_row_order_by_score_then_target() -> Vec<GraphOrderItem> {
    vec![
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
    ]
}

fn graph_row_streaming_fixed_query(
    source_id: Option<u64>,
    filter: Option<EdgeFilterExpr>,
    limit: usize,
) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: source_id.map(|_| NodeLabelFilter {
                    labels: vec![bench_node_label(2)],
                    mode: LabelMatchMode::All,
                }),
                ids: source_id.into_iter().collect(),
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BenchEdge1".to_string()],
            filter,
        })],
        where_: None,
        return_items: Some(vec![
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
        ]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            include_plan: true,
            ..GraphQueryOptions::default()
        },
    }
}

fn graph_row_streaming_vlp_query(source_id: u64, limit: usize) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec![bench_node_label(2)],
                    mode: LabelMatchMode::All,
                }),
                ids: vec![source_id],
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::VariableLength(
            GraphVariableLengthPattern {
                path_alias: Some("path".to_string()),
                edge_alias: None,
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdge1".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                }),
                min_hops: 1,
                max_hops: 2,
            },
        )],
        where_: None,
        return_items: Some(vec![graph_row_return_binding("target")]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            include_plan: true,
            ..GraphQueryOptions::default()
        },
    }
}

fn graph_row_streaming_plan_text(result: &overgraph::GraphRowResult) -> String {
    fn append_node(node: &overgraph::GraphExplainNode, text: &mut String) {
        text.push_str(&node.kind);
        text.push(' ');
        text.push_str(&node.detail);
        text.push('\n');
        for child in &node.children {
            append_node(child, text);
        }
    }

    let plan = result
        .plan
        .as_ref()
        .expect("benchmark query must include plan");
    let mut text = String::new();
    for node in &plan.plan {
        append_node(node, &mut text);
    }
    for op in &plan.row_ops {
        text.push_str(&op.kind);
        text.push(' ');
        text.push_str(&op.detail);
        text.push('\n');
    }
    text
}

fn assert_graph_row_streaming_success(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected_rows: usize,
    expected_plan_fragments: &[&str],
) {
    let result = engine.query_graph_rows(query).unwrap();
    assert_eq!(result.rows.len(), expected_rows);
    assert_eq!(result.stats.rows_returned, expected_rows);
    let plan = graph_row_streaming_plan_text(&result);
    for fragment in expected_plan_fragments {
        assert!(
            plan.contains(fragment),
            "unexpected graph-row plan; missing {fragment:?} from {plan}"
        );
    }
}

fn assert_graph_row_result_count(
    result: overgraph::GraphRowResult,
    expected: usize,
) -> overgraph::GraphRowResult {
    assert_eq!(result.rows.len(), expected);
    assert_eq!(result.stats.rows_returned, expected);
    result
}

fn graph_row_chunked_query(
    limit: usize,
    residual: bool,
    order_by_score: bool,
) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "source".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["GraphRowChunkNode".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyEquals {
                    key: "region".to_string(),
                    value: PropValue::String("east".to_string()),
                }),
            },
            GraphNodePattern {
                alias: "target".to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BenchEdge1".to_string()],
            filter: None,
        })],
        where_: residual.then(|| GraphExpr::Binary {
            left: Box::new(GraphExpr::Property {
                alias: "target".to_string(),
                key: "phase43_residual".to_string(),
            }),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::String("keep".to_string())),
        }),
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
        ]),
        order_by: order_by_score
            .then(|| {
                vec![GraphOrderItem {
                    expr: GraphExpr::Property {
                        alias: "edge".to_string(),
                        key: "score".to_string(),
                    },
                    direction: GraphOrderDirection::Desc,
                }]
            })
            .unwrap_or_default(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            include_plan: false,
            ..GraphQueryOptions::default()
        },
    }
}

fn graph_row_edgepull_query(
    limit: usize,
    residual: bool,
    order_by_score: bool,
    raised_caps: bool,
) -> overgraph::GraphRowQuery {
    let mut options = GraphQueryOptions {
        allow_full_scan: false,
        include_plan: false,
        ..GraphQueryOptions::default()
    };
    if raised_caps {
        options.max_frontier = GRAPH_ROW_EDGEPULL_EDGE_COUNT;
        options.max_intermediate_bindings = GRAPH_ROW_EDGEPULL_EDGE_COUNT;
    }
    overgraph::GraphRowQuery {
        nodes: ["source", "target"]
            .into_iter()
            .map(|alias| GraphNodePattern {
                alias: alias.to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            })
            .collect(),
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["BenchEdgePull".to_string()],
            filter: None,
        })],
        where_: residual.then(|| GraphExpr::Binary {
            left: Box::new(GraphExpr::Property {
                alias: "target".to_string(),
                key: "epull_residual".to_string(),
            }),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::String("keep".to_string())),
        }),
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
        ]),
        order_by: order_by_score
            .then(|| {
                vec![GraphOrderItem {
                    expr: GraphExpr::Property {
                        alias: "edge".to_string(),
                        key: "score".to_string(),
                    },
                    direction: GraphOrderDirection::Desc,
                }]
            })
            .unwrap_or_default(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options,
    }
}

fn graph_row_edgepull_qpx036_two_hop_query() -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: ["source", "mid", "target"]
            .into_iter()
            .map(|alias| GraphNodePattern {
                alias: alias.to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            })
            .collect(),
        pieces: vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("seed".to_string()),
                from_alias: "source".to_string(),
                to_alias: "mid".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdgePullSeed".to_string()],
                filter: None,
            }),
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("fanout".to_string()),
                from_alias: "mid".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["BenchEdgePullHub".to_string()],
                filter: None,
            }),
        ],
        where_: None,
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("seed"),
            graph_row_return_binding("mid"),
            graph_row_return_binding("fanout"),
            graph_row_return_binding("target"),
        ]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            max_frontier: 250_000,
            max_intermediate_bindings: 250_000,
            include_plan: true,
            ..GraphQueryOptions::default()
        },
    }
}

fn graph_row_edgepull_qpx036_stage_query(hub_ids: [u64; 2]) -> GraphPipelineQuery {
    GraphPipelineQuery {
        stages: vec![
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![GraphNodePattern {
                    alias: "mid".to_string(),
                    label_filter: None,
                    ids: hub_ids.to_vec(),
                    keys: Vec::new(),
                    filter: None,
                }],
                pieces: Vec::new(),
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::With,
                items: GraphProjectionItems::Items(vec![GraphProjectItem {
                    expr: GraphExpr::Binding("mid".to_string()),
                    alias: Some("mid".to_string()),
                    projection: GraphReturnProjection::Auto,
                }]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
            GraphPipelineStage::Match(GraphPipelineMatchStage {
                optional: false,
                nodes: vec![
                    GraphNodePattern {
                        alias: "mid".to_string(),
                        label_filter: None,
                        ids: Vec::new(),
                        keys: Vec::new(),
                        filter: None,
                    },
                    GraphNodePattern {
                        alias: "target".to_string(),
                        label_filter: None,
                        ids: Vec::new(),
                        keys: Vec::new(),
                        filter: None,
                    },
                ],
                pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                    alias: Some("fanout".to_string()),
                    from_alias: "mid".to_string(),
                    to_alias: "target".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec!["BenchEdgePullHub".to_string()],
                    filter: None,
                })],
                optional_candidate_where: None,
                where_: None,
            }),
            GraphPipelineStage::Project(GraphProjectStage {
                kind: GraphProjectKind::Return,
                items: GraphProjectionItems::Items(vec![
                    GraphProjectItem {
                        expr: GraphExpr::Binding("mid".to_string()),
                        alias: Some("mid".to_string()),
                        projection: GraphReturnProjection::IdOnly,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::Binding("fanout".to_string()),
                        alias: Some("fanout".to_string()),
                        projection: GraphReturnProjection::IdOnly,
                    },
                    GraphProjectItem {
                        expr: GraphExpr::Binding("target".to_string()),
                        alias: Some("target".to_string()),
                        projection: GraphReturnProjection::IdOnly,
                    },
                ]),
                distinct: false,
                where_: None,
                order_by: Vec::new(),
                skip: None,
                limit: None,
            }),
        ],
        params: BTreeMap::new(),
        at_epoch: None,
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        output: GraphOutputOptions::default(),
        options: GraphPipelineOptions {
            allow_full_scan: false,
            include_plan: true,
            max_frontier: 250_000,
            max_intermediate_bindings: 250_000,
            max_order_materialization: 250_000,
            ..GraphPipelineOptions::default()
        },
    }
}

fn graph_row_hub_explicit_ids_query(source_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut query = graph_row_chunked_query(10, true, false);
    query.nodes[0].ids = source_ids.to_vec();
    query.nodes[0].filter = None;
    query
}

fn graph_row_hub_native_query() -> overgraph::GraphRowQuery {
    let mut query = graph_row_chunked_query(10, false, false);
    query.options.max_intermediate_bindings = EDGE_STREAMING_SCALE_TOTAL_EDGES;
    query.options.max_frontier = EDGE_STREAMING_SCALE_TOTAL_EDGES;
    query
}

fn graph_row_known_ids_worst_case_query(source_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut query = graph_row_chunked_query(10, false, false);
    query.nodes[0].ids = source_ids.to_vec();
    query.nodes[0].filter = None;
    let GraphPatternPiece::Edge(edge) = &mut query.pieces[0] else {
        panic!("known-ID worst-case query must retain its single fixed edge");
    };
    edge.direction = Direction::Both;
    query
}

fn graph_row_chunked_result_rows(result: &overgraph::GraphRowResult) -> Vec<(u64, u64, u64)> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [overgraph::GraphValue::NodeId(source), overgraph::GraphValue::EdgeId(edge), overgraph::GraphValue::NodeId(target)] => {
                (*source, *edge, *target)
            }
            values => panic!("expected source/edge/target ID row, got {values:?}"),
        })
        .collect()
}

fn graph_row_chunked_oracle_rows(rows: &[GraphRowChunkedOracleRow]) -> Vec<(u64, u64, u64)> {
    rows.iter()
        .map(|row| (row.source_id, row.edge_id, row.target_id))
        .collect()
}

fn assert_graph_row_chunked_page(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    oracle: &[(u64, u64, u64)],
) -> overgraph::GraphRowResult {
    let first_end = query.page.limit.min(oracle.len());
    let expected = &oracle[..first_end];
    let result = engine.query_graph_rows(query).unwrap();
    assert_eq!(graph_row_chunked_result_rows(&result), expected);
    assert_eq!(result.stats.rows_returned, expected.len());
    if let Some(cursor) = result.next_cursor.clone() {
        let second_end = (first_end + query.page.limit).min(oracle.len());
        let mut next_query = query.clone();
        next_query.page.cursor = Some(cursor);
        let next = engine.query_graph_rows(&next_query).unwrap();
        assert_eq!(
            graph_row_chunked_result_rows(&next),
            oracle[first_end..second_end]
        );
        assert_eq!(next.next_cursor.is_some(), second_end < oracle.len());
    } else {
        assert_eq!(first_end, oracle.len());
    }
    let mut pinned = query.clone();
    pinned.at_epoch = Some(result.stats.effective_at_epoch);
    let pinned_first = engine.query_graph_rows(&pinned).unwrap();
    let pinned_repeated = engine.query_graph_rows(&pinned).unwrap();
    assert_eq!(graph_row_chunked_result_rows(&pinned_first), expected);
    assert_eq!(pinned_repeated.rows, pinned_first.rows);
    assert_eq!(pinned_repeated.next_cursor, pinned_first.next_cursor);
    result
}

fn graph_row_explain_has_node(
    nodes: &[overgraph::GraphExplainNode],
    kind: &str,
    detail_fragments: &[&str],
) -> bool {
    nodes.iter().any(|node| {
        (node.kind == kind
            && detail_fragments
                .iter()
                .all(|fragment| node.detail.contains(fragment)))
            || graph_row_explain_has_node(&node.children, kind, detail_fragments)
    })
}

fn graph_row_explain_details<'a>(
    nodes: &'a [overgraph::GraphExplainNode],
    kind: &str,
    details: &mut Vec<&'a str>,
) {
    for node in nodes {
        if node.kind == kind {
            details.push(node.detail.as_str());
        }
        graph_row_explain_details(&node.children, kind, details);
    }
}

fn assert_graph_row_hub_native_plan(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
) -> overgraph::GraphRowExplain {
    let explain = engine.explain_graph_rows(query).unwrap();
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPhysicalPlan",
            &["initial_driver=NodeAnchor(alias=source"]
        ),
        "hub-poisoned native query must choose the source node anchor: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPlanAlternative",
            &[
                "chosen; kind=NodeAnchor",
                "alias=source",
                "source=PropertyEqualityIndex",
                "cost_work=205008",
            ]
        ),
        "hub-poisoned native source alternative changed: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPlanAlternative",
            &[
                "rejected; kind=EdgeAnchor",
                "edge=alias:edge",
                "source=EdgeLabelIndex",
                "decision=rejected_by=estimated_work",
                "cost_work=600024",
            ]
        ),
        "hub-poisoned native edge alternative changed: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "NodeCandidateSource",
            &[
                "alias=source",
                "context=considered physical node anchor alternative",
                "source=PropertyEqualityIndex",
            ]
        ),
        "hub-poisoned source candidate must remain the region equality index: {explain:?}"
    );
    explain
}

fn assert_graph_row_explicit_id_plan(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
) -> overgraph::GraphRowExplain {
    let explain = engine.explain_graph_rows(query).unwrap();
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPhysicalPlan",
            &["initial_driver=NodeAnchor(alias=source"]
        ),
        "explicit-ID query must choose the source node anchor: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPlanAlternative",
            &[
                "chosen; kind=NodeAnchor",
                "alias=source",
                "source=ExplicitIds",
            ]
        ),
        "explicit-ID source alternative must be truthful: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "AdjacencyExpansion",
            &["edge=alias:edge", "source=EndpointAdjacency"]
        ),
        "explicit-ID plan must retain its BenchEdge1 expansion: {explain:?}"
    );
    explain
}

fn assert_graph_row_explicit_id_plan_cost(
    explain: &overgraph::GraphRowExplain,
    expected_cost_work: u64,
) {
    let expected = format!("cost_work={expected_cost_work}");
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPlanAlternative",
            &[
                "chosen; kind=NodeAnchor",
                "alias=source",
                "source=ExplicitIds",
                expected.as_str(),
            ]
        ),
        "explicit-ID source alternative cost changed from {expected_cost_work}: {explain:?}"
    );
}

fn assert_graph_row_known_id_worst_case_plan(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected_cost_work: u64,
) -> overgraph::GraphRowExplain {
    let explain = assert_graph_row_explicit_id_plan(engine, query);
    assert_graph_row_explicit_id_plan_cost(&explain, expected_cost_work);
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "AdjacencyExpansion",
            &["edge=alias:edge", "direction=Both"]
        ),
        "Direction::Both worst-case plan must retain its bidirectional expansion: {explain:?}"
    );
    explain
}

fn assert_graph_row_chunked_source_plan(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
) -> overgraph::GraphRowResult {
    let mut preflight_query = query.clone();
    preflight_query.options.include_plan = true;
    let result = engine.query_graph_rows(&preflight_query).unwrap();
    let explain = result
        .plan
        .as_ref()
        .expect("executed graph-row preflight must include its physical plan");
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPhysicalPlan",
            &["initial_driver=NodeAnchor(alias=source"]
        ),
        "graph-row chunk fixture must choose the source node anchor: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowRequiredSegment",
            &["initial_driver=NodeAnchor(alias=source"]
        ),
        "required segment must execute from the source node anchor: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "NodeCandidateSource",
            &[
                "alias=source",
                "context=physical initial node driver",
                "source=PropertyEqualityIndex",
            ]
        ),
        "executed source anchor must use the region equality index: {explain:?}"
    );
    result
}

fn assert_graph_row_edgepull_source_plan(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
) -> overgraph::GraphRowResult {
    let mut preflight_query = query.clone();
    preflight_query.options.include_plan = true;
    let result = engine.query_graph_rows(&preflight_query).unwrap();
    let explain = result
        .plan
        .as_ref()
        .expect("EdgePull benchmark preflight must include its executed plan");
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPhysicalPlan",
            &["initial_driver=EdgeAnchor(edge=alias:edge"]
        ),
        "EdgePull fixture must select its edge anchor by legality: {explain:?}"
    );
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowRequiredSegment",
            &["segment=0", "initial_driver=EdgeAnchor(edge=alias:edge"]
        ),
        "EdgePull required segment must be edge-driven: {explain:?}"
    );

    let mut alternatives = Vec::new();
    graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
    for alias in ["source", "target"] {
        let expected = format!(
            "rejected; kind=RejectedNodeAnchor; segment=0; alias={alias}; reason=no legal initial candidate source; decision=not_costed; cost=unavailable"
        );
        assert!(
            alternatives
                .iter()
                .any(|detail| *detail == expected.as_str()),
            "{alias} must be rejected because it has no legal initial source: {alternatives:?}"
        );
    }
    assert!(
        alternatives.iter().any(|detail| {
            detail.starts_with("chosen; kind=EdgeAnchor; segment=0; edge=alias:edge;")
                && detail.contains("source=EdgeLabelIndex")
                && detail.contains("decision=chosen_lowest_cost_or_deterministic_tie_breaker")
        }),
        "the edge-label alternative must be selected without planner bias: {alternatives:?}"
    );
    assert!(
        alternatives
            .iter()
            .filter(|detail| detail.contains("kind=RejectedNodeAnchor"))
            .all(|detail| detail.contains("decision=not_costed; cost=unavailable")),
        "node alternatives must not be rejected by cost: {alternatives:?}"
    );
    eprintln!(
        "CP44.5 EdgePull plan alternatives: {}",
        alternatives.join(" | ")
    );
    result
}

fn assert_graph_row_edgepull_runtime(
    result: &overgraph::GraphRowResult,
    max_scheduled_leaf_size: usize,
    minimum_source_pulls: usize,
) {
    let explain = result
        .plan
        .as_ref()
        .expect("EdgePull public runtime proof must include a plan");
    let mut runtime = Vec::new();
    graph_row_explain_details(&explain.plan, "ChunkedRowProductionRuntime", &mut runtime);
    assert_eq!(
        runtime.len(),
        1,
        "expected one executed runtime line: {runtime:?}"
    );
    let detail = runtime[0];
    for fragment in [
        "source=EdgePull{edge=alias:edge}",
        "eligibility=edge_id_order_not_logical_prefix",
        "early_exit=false",
        "cursor_seek=none",
    ] {
        assert!(
            detail.contains(fragment),
            "missing {fragment:?} from {detail}"
        );
    }
    let source_pulls = detail
        .split("; ")
        .find_map(|field| field.strip_prefix("source_pulls="))
        .expect("EdgePull runtime line must expose source_pulls")
        .parse::<usize>()
        .unwrap();
    assert!(
        source_pulls >= minimum_source_pulls,
        "expected at least {minimum_source_pulls} source pulls, got {source_pulls}: {detail}"
    );
    let public_peak_bound = max_scheduled_leaf_size.saturating_mul(2);
    assert!(
        result.stats.frontier_peak <= public_peak_bound,
        "frontier peak {} exceeded twice the scheduled leaf maximum {public_peak_bound}",
        result.stats.frontier_peak
    );
    assert!(
        result.stats.intermediate_bindings_peak <= public_peak_bound,
        "intermediate peak {} exceeded twice the scheduled leaf maximum {public_peak_bound}",
        result.stats.intermediate_bindings_peak
    );
    eprintln!(
        "CP44.5 EdgePull runtime: {detail}; public_frontier_peak={}; public_intermediate_bindings_peak={}",
        result.stats.frontier_peak, result.stats.intermediate_bindings_peak
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GraphRowEdgePullQpx036Evidence {
    two_hop_lines: Vec<String>,
    stage_lines: Vec<String>,
}

fn assert_graph_row_edgepull_qpx036_cost_fields(line: &str) {
    for field in [
        "cost_work=",
        "simulated_frontier=",
        "fanout_complete=",
        "confidence_rank=",
        "stale_risk_rank=",
        "hub_risk_rank=",
        "frontier_capped=",
    ] {
        assert!(line.contains(field), "missing {field:?} from {line}");
    }
    let cost_work = line
        .split(';')
        .map(str::trim)
        .find_map(|field| field.strip_prefix("cost_work="))
        .expect("QPX-036 alternative must expose cost_work")
        .parse::<u64>()
        .unwrap();
    assert!(cost_work > 0, "QPX-036 cost_work must be nonzero: {line}");
}

fn assert_graph_row_edgepull_qpx036_cost_line(line: &str, edge: &str, chosen: bool) {
    let disposition = if chosen { "chosen" } else { "rejected" };
    assert!(
        line.starts_with(&format!("{disposition}; kind=EdgeAnchor;"))
            && line.contains(&format!("edge=alias:{edge}")),
        "unexpected QPX-036 {edge} alternative: {line}"
    );
    assert_graph_row_edgepull_qpx036_cost_fields(line);
}

fn graph_row_edgepull_qpx036_alternative_lines(
    explain: &overgraph::GraphRowExplain,
    chosen_edge: &str,
    rejected_edge: &str,
) -> Vec<String> {
    let mut alternatives = Vec::new();
    graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
    let rejected_nodes = alternatives
        .iter()
        .filter(|line| line.starts_with("rejected; kind=RejectedNodeAnchor;"))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected_nodes.len(),
        3,
        "QPX-036 must retain all three rejected node alternatives: {alternatives:?}"
    );
    for (line, alias) in rejected_nodes.iter().zip(["source", "mid", "target"]) {
        assert!(
            line.contains(&format!("alias={alias};")),
            "QPX-036 two-hop node-alternative order changed: {alternatives:?}"
        );
    }
    assert!(
        rejected_nodes.iter().all(|line| {
            line.contains("reason=no legal initial candidate source")
                && line.contains("decision=not_costed")
                && line.contains("cost=unavailable")
        }),
        "QPX-036 node alternatives must be uncosted legality rejections: {alternatives:?}"
    );
    let chosen = alternatives
        .iter()
        .find(|line| {
            line.starts_with("chosen; kind=EdgeAnchor;")
                && line.contains(&format!("edge=alias:{chosen_edge}"))
        })
        .expect("QPX-036 chosen edge alternative missing");
    let rejected = alternatives
        .iter()
        .find(|line| {
            line.starts_with("rejected; kind=EdgeAnchor;")
                && line.contains(&format!("edge=alias:{rejected_edge}"))
        })
        .expect("QPX-036 rejected edge alternative missing");
    assert_graph_row_edgepull_qpx036_cost_line(chosen, chosen_edge, true);
    assert_graph_row_edgepull_qpx036_cost_line(rejected, rejected_edge, false);
    assert_eq!(
        alternatives.len(),
        5,
        "QPX-036 two-hop must emit exactly three node and two edge alternatives: {alternatives:?}"
    );
    assert!(
        alternatives[3] == *chosen && alternatives[4] == *rejected,
        "QPX-036 two-hop edge-alternative order changed: {alternatives:?}"
    );
    alternatives.into_iter().map(str::to_string).collect()
}

fn graph_row_edgepull_qpx036_stage_alternative_lines(
    explain: &overgraph::GraphRowExplain,
) -> Vec<String> {
    let mut alternatives = Vec::new();
    graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
    let chosen = alternatives
        .iter()
        .find(|line| {
            line.starts_with("chosen; kind=EdgeAnchor;") && line.contains("edge=alias:fanout")
        })
        .expect("QPX-036 stage chosen Fanout alternative missing");
    assert_graph_row_edgepull_qpx036_cost_fields(chosen);
    let rejected_nodes = alternatives
        .iter()
        .filter(|line| line.starts_with("rejected; kind=RejectedNodeAnchor;"))
        .collect::<Vec<_>>();
    assert_eq!(
        rejected_nodes.len(),
        2,
        "QPX-036 stage must retain both rejected node alternatives: {alternatives:?}"
    );
    assert!(
        rejected_nodes.iter().all(|line| {
            line.contains("reason=no legal initial candidate source")
                && line.contains("decision=not_costed")
                && line.contains("cost=unavailable")
        }),
        "QPX-036 stage node alternatives must be uncosted legality rejections: {alternatives:?}"
    );
    for (line, alias) in rejected_nodes.iter().zip(["mid", "target"]) {
        assert!(
            line.contains(&format!("alias={alias};")),
            "QPX-036 stage node-alternative order changed: {alternatives:?}"
        );
    }
    assert_eq!(
        alternatives.len(),
        3,
        "QPX-036 stage must emit exactly two node alternatives then Fanout: {alternatives:?}"
    );
    assert!(
        alternatives[2] == *chosen,
        "QPX-036 stage edge-alternative order changed: {alternatives:?}"
    );
    alternatives.into_iter().map(str::to_string).collect()
}

fn collect_graph_row_edgepull_qpx036_evidence(
    fixture: &GraphRowEdgePullHubEvidenceEngine,
) -> GraphRowEdgePullQpx036Evidence {
    assert_eq!(fixture.node_ids.len(), GRAPH_ROW_EDGEPULL_NODE_COUNT);
    assert_eq!(
        fixture.hub_edge_ids.len(),
        GRAPH_ROW_EDGEPULL_QPX036_HUB_EDGE_COUNT
    );
    assert_eq!(
        fixture.seed_edge_ids.len(),
        GRAPH_ROW_EDGEPULL_QPX036_SEED_EDGE_COUNT
    );

    let two_hop_query = graph_row_edgepull_qpx036_two_hop_query();
    let two_hop = fixture.engine.explain_graph_rows(&two_hop_query).unwrap();
    assert!(
        graph_row_explain_has_node(
            &two_hop.plan,
            "GraphRowPhysicalPlan",
            &["initial_driver=EdgeAnchor(edge=alias:fanout"]
        ),
        "QPX-036 two-hop fixture changed its amended Fanout choice: {two_hop:?}"
    );
    let two_hop_lines = graph_row_edgepull_qpx036_alternative_lines(&two_hop, "fanout", "seed");
    let chosen_fanout = two_hop_lines
        .iter()
        .find(|line| {
            line.starts_with("chosen; kind=EdgeAnchor;") && line.contains("edge=alias:fanout")
        })
        .expect("QPX-036 chosen Fanout line missing after collection");
    assert!(
        chosen_fanout.contains("cost_work=612548"),
        "QPX-036 chosen Fanout cost changed: {chosen_fanout}"
    );
    let rejected_seed = two_hop_lines
        .iter()
        .find(|line| {
            line.starts_with("rejected; kind=EdgeAnchor;") && line.contains("edge=alias:seed")
        })
        .expect("QPX-036 rejected Seed line missing after collection");
    assert!(
        rejected_seed.contains("cost_work=1007548"),
        "QPX-036 rejected Seed cost changed: {rejected_seed}"
    );

    let stage_query =
        graph_row_edgepull_qpx036_stage_query([fixture.node_ids[0], fixture.node_ids[1]]);
    let GraphPipelineStage::Match(first_match) = &stage_query.stages[0] else {
        panic!("QPX-036 pipeline must start with Match")
    };
    assert_eq!(first_match.nodes[0].ids, fixture.node_ids[..2]);
    let stage_explain = fixture.engine.explain_graph_pipeline(&stage_query).unwrap();
    assert_eq!(stage_explain.stages.len(), 4);
    assert!(stage_explain.stages[2]
        .detail
        .contains("seeded_node_aliases=mid"));
    let nested = stage_explain.stages[2]
        .graph_row
        .as_deref()
        .expect("QPX-036 second Match must expose nested graph-row explain");
    let stage_lines = graph_row_edgepull_qpx036_stage_alternative_lines(nested);

    let executed = fixture.engine.query_graph_pipeline(&stage_query).unwrap();
    let executed_plan = executed
        .plan
        .as_ref()
        .expect("QPX-036 executed pipeline must include plan");
    assert!(
        executed_plan.stages[1].detail.contains("input_rows=2")
            && executed_plan.stages[1].detail.contains("output_rows=2"),
        "QPX-036 WITH must hand exactly two seeded rows to the second Match: {}",
        executed_plan.stages[1].detail
    );
    assert!(executed_plan.stages[2]
        .detail
        .contains("seeded_node_aliases=mid"));

    GraphRowEdgePullQpx036Evidence {
        two_hop_lines,
        stage_lines,
    }
}

fn assert_graph_row_chunked_runtime(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected_eligibility: &str,
    expected_early_exit: Option<bool>,
    expected_first_scheduled_size: Option<usize>,
    expected_cursor_seek: &str,
) -> overgraph::GraphRowResult {
    let mut explain_query = query.clone();
    explain_query.options.include_plan = true;
    let result = engine.query_graph_rows(&explain_query).unwrap();
    let plan = format!(
        "{:?}",
        result
            .plan
            .as_ref()
            .expect("chunked runtime preflight plan")
    );
    assert!(plan.contains("ChunkedRowProductionRuntime"), "{plan}");
    assert!(plan.contains(expected_eligibility), "{plan}");
    if let Some(expected_early_exit) = expected_early_exit {
        assert!(
            plan.contains(&format!("early_exit={expected_early_exit}")),
            "{plan}"
        );
    }
    assert!(
        plan.contains(&format!("cursor_seek={expected_cursor_seek}")),
        "{plan}"
    );
    assert!(
        plan.contains("source_pulls=") && !plan.contains("source_pulls=0"),
        "{plan}"
    );
    assert!(
        plan.contains("successful_leaves=") && !plan.contains("successful_leaves=0"),
        "{plan}"
    );
    if let Some(first) = expected_first_scheduled_size {
        assert!(
            plan.contains(&format!("scheduled_sizes={first}..")),
            "{plan}"
        );
    }
    result
}

struct GraphRowChunkedFo033Engine {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    source_ids: Vec<u64>,
    expected_target_ids: Vec<u64>,
}

fn build_graph_row_chunked_fo033_engine() -> GraphRowChunkedFo033Engine {
    let (dir, engine) = temp_db();
    let selected = [1usize, 7, 13, 19];
    let source_ids = engine
        .batch_upsert_nodes(
            (0..20)
                .map(|ordinal| NodeInput {
                    labels: vec!["Fo033Source".to_string()],
                    key: format!("graph-row-chunk-fo033-source-{ordinal}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    let target_ids = engine
        .batch_upsert_nodes(
            (0..20)
                .map(|ordinal| NodeInput {
                    labels: vec!["Fo033Target".to_string()],
                    key: format!("graph-row-chunk-fo033-target-{ordinal}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(source_ids.len(), 20);
    assert_eq!(target_ids.len(), 20);

    let required_edges = (0..20)
        .map(|ordinal| {
            let mut props = BTreeMap::new();
            props.insert(
                "role".to_string(),
                PropValue::String(
                    if selected.contains(&ordinal) {
                        "selected"
                    } else {
                        "other"
                    }
                    .to_string(),
                ),
            );
            props.insert("score".to_string(), PropValue::Int(ordinal as i64));
            EdgeInput {
                from: source_ids[ordinal],
                to: target_ids[ordinal],
                label: "FO033_REQUIRED".to_string(),
                props,
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }
        })
        .collect();
    assert_eq!(engine.batch_upsert_edges(required_edges).unwrap().len(), 20);

    let optional_ordinals = [7usize, 19];
    let doc_ids = engine
        .batch_upsert_nodes(
            optional_ordinals
                .iter()
                .map(|ordinal| NodeInput {
                    labels: vec!["Fo033Document".to_string()],
                    key: format!("graph-row-chunk-fo033-document-{ordinal}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(doc_ids.len(), 2);
    let optional_edges = optional_ordinals
        .iter()
        .zip(doc_ids.iter())
        .map(|(&ordinal, &doc_id)| EdgeInput {
            from: target_ids[ordinal],
            to: doc_id,
            label: "FO033_OPTIONAL".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    assert_eq!(engine.batch_upsert_edges(optional_edges).unwrap().len(), 2);

    let expected_target_ids = [19usize, 13, 7]
        .into_iter()
        .map(|ordinal| target_ids[ordinal])
        .collect();
    GraphRowChunkedFo033Engine {
        _dir: dir,
        engine,
        source_ids,
        expected_target_ids,
    }
}

fn graph_row_chunked_fo033_query(source_ids: &[u64]) -> overgraph::GraphRowQuery {
    let mut query = overgraph::GraphRowQuery {
        nodes: ["source", "target", "doc"]
            .into_iter()
            .map(|alias| GraphNodePattern {
                alias: alias.to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            })
            .collect(),
        pieces: vec![
            GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["FO033_REQUIRED".to_string()],
                filter: None,
            }),
            GraphPatternPiece::Optional(overgraph::GraphOptionalGroup {
                pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                    alias: Some("opt".to_string()),
                    from_alias: "target".to_string(),
                    to_alias: "doc".to_string(),
                    direction: Direction::Outgoing,
                    label_filter: vec!["FO033_OPTIONAL".to_string()],
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
            right: Box::new(GraphExpr::String("selected".to_string())),
        }),
        return_items: Some(vec![graph_row_return_binding("target")]),
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
            limit: 3,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            max_intermediate_bindings: 8,
            include_plan: false,
            ..GraphQueryOptions::default()
        },
    };
    query.nodes[0].ids = source_ids.to_vec();
    query
}

fn graph_row_chunked_single_node_ids(result: &overgraph::GraphRowResult) -> Vec<u64> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [overgraph::GraphValue::NodeId(id)] => *id,
            values => panic!("expected one node ID, got {values:?}"),
        })
        .collect()
}

fn graph_row_chunked_gql_single_uints(result: &overgraph::GqlExecutionResult) -> Vec<u64> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [overgraph::GqlValue::UInt(id)] => *id,
            values => panic!("expected one GQL uint, got {values:?}"),
        })
        .collect()
}

fn graph_row_chunked_gql_rows(result: &overgraph::GqlExecutionResult) -> Vec<(u64, u64, u64)> {
    result
        .rows
        .iter()
        .map(|row| match row.values.as_slice() {
            [overgraph::GqlValue::UInt(source), overgraph::GqlValue::UInt(edge), overgraph::GqlValue::UInt(target)] => {
                (*source, *edge, *target)
            }
            values => panic!("expected GQL source/edge/target IDs, got {values:?}"),
        })
        .collect()
}

fn bench_graph_row_chunked(c: &mut Criterion) {
    const GROUP: &str = "graph_row_chunked";
    const UNORDERED: &str = "graph_row_unordered_limit_early_exit";
    const RESIDUAL: &str = "graph_row_unordered_limit_where_early_exit";
    const TOPK: &str = "graph_row_order_by_topk_scale";
    const DEEP_CURSOR: &str = "graph_row_deep_cursor_seek";
    const LOW_CAP: &str = "graph_row_low_cap_anchor_capability";
    const FO033: &str = "graph_row_fo033_residual_inflight_cap";
    const GQL_UNORDERED: &str = "gql_graph_row_unordered_limit_early_exit";

    let requested = [
        UNORDERED,
        RESIDUAL,
        TOPK,
        DEEP_CURSOR,
        LOW_CAP,
        FO033,
        GQL_UNORDERED,
    ]
    .map(|scenario| criterion_requested_benchmark(GROUP, scenario));
    if !requested.iter().any(|requested| *requested) {
        return;
    }

    let scale = (requested[0]
        || requested[1]
        || requested[2]
        || requested[3]
        || requested[4]
        || requested[6])
        .then(build_graph_row_chunked_scale_engine);
    let fo033 = requested[5].then(build_graph_row_chunked_fo033_engine);

    if let Some(fixture) = scale.as_ref() {
        assert_eq!(fixture.node_ids.len(), 10_000);
        assert_eq!(fixture.east_anchor_ids.len(), 2_500);
        assert_eq!(fixture.edge_count, 200_000);
        assert_eq!(fixture.flushed_segment_count, 3);
        assert_eq!(fixture.active_tail_edge_count, 20_000);
        assert_eq!(fixture.hub_source_ids, None);
        assert_eq!(fixture.rows.len(), 47_510);
        assert_eq!(fixture.residual_rows.len(), 6_253);
        let plan_query = graph_row_chunked_query(10, false, false);
        let _ = assert_graph_row_chunked_source_plan(&fixture.engine, &plan_query);
    }

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(10);

    if requested[0] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_chunked_query(10, false, false);
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=eligible",
            Some(true),
            Some(11),
            "none",
        );
        group.bench_function(UNORDERED, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[1] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_chunked_query(10, true, false);
        assert_graph_row_chunked_source_plan(&fixture.engine, &query);
        let oracle = graph_row_chunked_oracle_rows(&fixture.residual_rows);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=eligible",
            Some(true),
            Some(11),
            "none",
        );
        group.bench_function(RESIDUAL, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[2] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_chunked_query(100, false, true);
        let mut ordered = fixture.rows.clone();
        ordered.sort_unstable_by(|left, right| {
            right.score.cmp(&left.score).then_with(|| {
                (left.source_id, left.target_id, left.edge_id).cmp(&(
                    right.source_id,
                    right.target_id,
                    right.edge_id,
                ))
            })
        });
        let oracle = graph_row_chunked_oracle_rows(&ordered);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=ordered_query",
            Some(false),
            Some(4_096),
            "none",
        );
        group.bench_function(TOPK, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[3] {
        let fixture = scale.as_ref().unwrap();
        let mut walk = graph_row_chunked_query(100, false, false);
        let (retained_cursor, previous_page_last_tuple) = loop {
            let page = fixture.engine.query_graph_rows(&walk).unwrap();
            let page_rows = graph_row_chunked_result_rows(&page);
            let last_tuple = *page_rows
                .last()
                .expect("cursor walk page must be non-empty");
            let next_cursor = page.next_cursor;
            if last_tuple.0 >= fixture.p90_anchor_id {
                break (
                    next_cursor.expect("p90-threshold page must retain a public cursor"),
                    last_tuple,
                );
            }
            walk.page.cursor = next_cursor;
            assert!(
                walk.page.cursor.is_some(),
                "p90 anchor page must be reachable"
            );
        };
        let boundary_index = fixture
            .rows
            .binary_search_by_key(
                &(
                    previous_page_last_tuple.0,
                    previous_page_last_tuple.2,
                    previous_page_last_tuple.1,
                ),
                |row| (row.source_id, row.target_id, row.edge_id),
            )
            .expect("retained cursor boundary must exist in the fixture oracle");
        let expected_start = boundary_index + 1;
        let expected_end = (expected_start + 100).min(fixture.rows.len());
        let expected_suffix =
            graph_row_chunked_oracle_rows(&fixture.rows[expected_start..expected_end]);
        let mut query = graph_row_chunked_query(100, false, false);
        query.page.cursor = Some(retained_cursor);
        let first = fixture.engine.query_graph_rows(&query).unwrap();
        let actual = graph_row_chunked_result_rows(&first);
        assert_eq!(actual, expected_suffix);
        assert_eq!(actual.last(), expected_suffix.last());
        assert_eq!(
            first.next_cursor.is_some(),
            expected_end < fixture.rows.len()
        );
        if let Some(cursor) = first.next_cursor.clone() {
            let continuation_end = (expected_end + 100).min(fixture.rows.len());
            let mut continuation_query = graph_row_chunked_query(100, false, false);
            continuation_query.page.cursor = Some(cursor);
            let continuation = fixture
                .engine
                .query_graph_rows(&continuation_query)
                .unwrap();
            assert_eq!(
                graph_row_chunked_result_rows(&continuation),
                graph_row_chunked_oracle_rows(&fixture.rows[expected_end..continuation_end])
            );
            assert_eq!(
                continuation.next_cursor.is_some(),
                continuation_end < fixture.rows.len()
            );
        }
        let repeated = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(repeated.rows, first.rows);
        assert_eq!(repeated.next_cursor, first.next_cursor);
        let source_preflight = assert_graph_row_chunked_source_plan(&fixture.engine, &query);
        let seek_preflight = assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=eligible",
            None,
            Some(101),
            &format!("anchor_ge:{}", previous_page_last_tuple.0),
        );
        let seek_plan = format!("{:?}", seek_preflight.plan.as_ref().unwrap());
        assert!(
            seek_plan.contains("source=AnchorPull{alias=source}"),
            "{seek_plan}"
        );
        assert!(seek_plan.contains("anchor_seek=applied"), "{seek_plan}");
        assert!(previous_page_last_tuple.0 >= fixture.p90_anchor_id);
        assert_eq!(seek_preflight.rows, first.rows);
        assert_eq!(seek_preflight.next_cursor, first.next_cursor);
        assert_eq!(source_preflight.rows, first.rows);
        assert_eq!(source_preflight.next_cursor, first.next_cursor);
        assert_eq!(
            source_preflight.plan.as_ref().unwrap().fingerprint,
            seek_preflight.plan.as_ref().unwrap().fingerprint
        );
        group.bench_function(DEEP_CURSOR, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[4] {
        let fixture = scale.as_ref().unwrap();
        let mut query = graph_row_chunked_query(10, false, false);
        query.options.max_intermediate_bindings = 2_048;
        let result = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(
            graph_row_chunked_result_rows(&result),
            graph_row_chunked_oracle_rows(&fixture.rows[..10])
        );
        assert!(result.stats.intermediate_bindings_peak <= 2_048);
        assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=eligible",
            None,
            Some(11),
            "none",
        );
        group.bench_function(LOW_CAP, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query))))
        });
    }

    if requested[5] {
        let fixture = fo033.as_ref().unwrap();
        let query = graph_row_chunked_fo033_query(&fixture.source_ids);
        let gql_query = "MATCH (source)-[edge:FO033_REQUIRED]->(target) WHERE edge.role = 'selected' OPTIONAL MATCH (target)-[opt:FO033_OPTIONAL]->(doc) RETURN id(target) AS target_id ORDER BY edge.score DESC, id(target) LIMIT 3";
        let gql_params = GqlParams::new();
        let gql_options = GqlExecutionOptions {
            include_plan: true,
            max_intermediate_bindings: 8,
            ..GqlExecutionOptions::default()
        };
        let gql_oracle = fixture
            .engine
            .execute_gql(gql_query, &gql_params, &gql_options)
            .unwrap();
        assert_eq!(
            graph_row_chunked_gql_single_uints(&gql_oracle),
            fixture.expected_target_ids
        );
        assert!(gql_oracle.next_cursor.is_none());
        let gql_read = gql_oracle
            .plan
            .as_ref()
            .and_then(|plan| plan.read.as_ref())
            .expect("FO-033 GQL oracle must expose lowering and pushdown evidence");
        assert_eq!(gql_read.target, overgraph::GqlLoweringTarget::GraphRowQuery);
        assert!(
            gql_read
                .pushed_down
                .iter()
                .any(|item| item.contains("edge.role")),
            "FO-033 GQL oracle must push edge.role: {gql_read:?}"
        );
        assert!(
            gql_read.projection.iter().any(|item| {
                item.contains("GraphRowSourceRead") && item.contains("choice=EdgePullChunk")
            }),
            "FO-033 GQL oracle must expose graph-row EdgePull lowering: {gql_read:?}"
        );
        let result = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(
            graph_row_chunked_single_node_ids(&result),
            fixture.expected_target_ids
        );
        assert_graph_row_chunked_runtime(
            &fixture.engine,
            &query,
            "eligibility=ordered_query",
            Some(false),
            Some(8),
            "none",
        );
        group.bench_function(FO033, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query))))
        });
    }

    if requested[6] {
        let fixture = scale.as_ref().unwrap();
        let expected = graph_row_chunked_oracle_rows(&fixture.rows[..10]);
        let native_query = graph_row_chunked_query(10, false, false);
        let native = fixture.engine.query_graph_rows(&native_query).unwrap();
        assert_eq!(graph_row_chunked_result_rows(&native), expected);
        let params = GqlParams::new();
        let query = "MATCH (source:GraphRowChunkNode)-[edge:BenchEdge1]->(target) WHERE source.region = 'east' RETURN id(source), id(edge), id(target) LIMIT 10";
        let explain_options = GqlExecutionOptions {
            include_plan: true,
            ..GqlExecutionOptions::default()
        };
        let preflight = fixture
            .engine
            .execute_gql(query, &params, &explain_options)
            .unwrap();
        let read = preflight
            .plan
            .as_ref()
            .and_then(|plan| plan.read.as_ref())
            .expect("chunked GQL preflight must expose its graph-row target");
        assert_eq!(read.target, overgraph::GqlLoweringTarget::GraphRowQuery);
        assert!(
            read.projection
                .iter()
                .any(|item| item.contains("logical_limit=Some(10)")),
            "GQL lowering must preserve logical LIMIT 10: {read:?}"
        );
        assert_eq!(graph_row_chunked_gql_rows(&preflight), expected);
        let gql_plan = format!("{:?}", preflight.plan.as_ref().unwrap());
        assert!(
            gql_plan.contains("initial_driver=NodeAnchor(alias=source")
                && gql_plan.contains("source=PropertyEqualityIndex"),
            "GQL headline must retain the locked source anchor/index: {gql_plan}"
        );
        assert!(gql_plan.contains("ChunkedRowProductionRuntime"));
        assert!(gql_plan.contains("source=AnchorPull{alias=source}"));
        assert!(gql_plan.contains("eligibility=eligible"));
        assert!(gql_plan.contains("early_exit=true"));
        assert!(gql_plan.contains("cursor_seek=none"));
        assert!(gql_plan.contains("source_pulls=1"));
        assert!(gql_plan.contains("successful_leaves=1"));
        assert!(gql_plan.contains("scheduled_sizes=10..10"));
        assert!(
            preflight.next_cursor.is_none(),
            "logical LIMIT 10 must be exhausted without a continuation cursor"
        );
        let repeated = fixture
            .engine
            .execute_gql(query, &params, &explain_options)
            .unwrap();
        assert_eq!(repeated.rows, preflight.rows);
        assert_eq!(repeated.next_cursor, preflight.next_cursor);
        assert!(repeated.next_cursor.is_none());
        let options = GqlExecutionOptions::default();
        group.bench_function(GQL_UNORDERED, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .engine
                        .execute_gql(black_box(query), black_box(&params), black_box(&options))
                        .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn bench_graph_row_edgepull(c: &mut Criterion) {
    const GROUP: &str = "graph_row_edgepull";
    const UNORDERED: &str = "edge_anchor_unordered_limit10";
    const RESIDUAL: &str = "edge_anchor_residual_where_limit10";
    const TOPK: &str = "edge_anchor_order_by_topk";
    const DEEP_CURSOR: &str = "edge_anchor_deep_cursor_page";
    const DEFAULT_CAPS: &str = "edge_anchor_default_caps_capability";
    const FO033: &str = "graph_row_fo033_unconstrained_inflight_cap";
    const GQL_UNORDERED: &str = "gql_edge_anchor_unordered_limit10";

    let requested = [
        UNORDERED,
        RESIDUAL,
        TOPK,
        DEEP_CURSOR,
        DEFAULT_CAPS,
        FO033,
        GQL_UNORDERED,
    ]
    .map(|scenario| criterion_requested_benchmark(GROUP, scenario));
    if !requested.iter().any(|requested| *requested) {
        return;
    }

    let scale = (requested[0]
        || requested[1]
        || requested[2]
        || requested[3]
        || requested[4]
        || requested[6])
        .then(build_graph_row_edgepull_scale_engine);
    let fo033 = requested[5].then(build_graph_row_chunked_fo033_engine);

    if let Some(fixture) = scale.as_ref() {
        assert_eq!(fixture.node_ids.len(), GRAPH_ROW_EDGEPULL_NODE_COUNT);
        assert_eq!(fixture.edge_ids.len(), GRAPH_ROW_EDGEPULL_EDGE_COUNT);
        assert_eq!(
            fixture.flushed_edge_batches,
            GRAPH_ROW_EDGEPULL_SEGMENT_COUNT
        );
        assert_eq!(
            fixture.active_tail_edge_count,
            GRAPH_ROW_EDGEPULL_ACTIVE_TAIL_COUNT
        );
        assert_eq!(fixture.rows.len(), GRAPH_ROW_EDGEPULL_EDGE_COUNT);
        assert_eq!(fixture.residual_rows.len(), 25_000);
    }

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(10);

    if requested[0] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_edgepull_query(10, false, false, true);
        assert!(!query.options.allow_full_scan);
        assert!(!query.options.include_plan);
        assert_eq!(query.options.max_frontier, GRAPH_ROW_EDGEPULL_EDGE_COUNT);
        assert_eq!(
            query.options.max_intermediate_bindings,
            GRAPH_ROW_EDGEPULL_EDGE_COUNT
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        assert_eq!(graph_row_chunked_result_rows(&proof), oracle[..10]);
        assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        eprintln!("{GROUP}/{UNORDERED}: class=comparable-postphase-success");
        group.bench_function(UNORDERED, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[1] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_edgepull_query(10, true, false, true);
        assert!(query.nodes.iter().all(|node| node.filter.is_none()));
        assert!(query.where_.is_some());
        assert!(!query.options.include_plan);
        let oracle = graph_row_chunked_oracle_rows(&fixture.residual_rows);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        assert_eq!(graph_row_chunked_result_rows(&proof), oracle[..10]);
        assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        eprintln!("{GROUP}/{RESIDUAL}: class=comparable-postphase-success");
        group.bench_function(RESIDUAL, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[2] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_edgepull_query(100, false, true, true);
        assert!(!query.options.include_plan);
        let mut ordered = fixture.rows.clone();
        ordered.sort_unstable_by(|left, right| {
            right.score.cmp(&left.score).then_with(|| {
                (left.source_id, left.target_id, left.edge_id).cmp(&(
                    right.source_id,
                    right.target_id,
                    right.edge_id,
                ))
            })
        });
        assert!(ordered.windows(2).all(|rows| {
            rows[0].score > rows[1].score
                || (rows[0].score == rows[1].score
                    && (rows[0].source_id, rows[0].target_id, rows[0].edge_id)
                        <= (rows[1].source_id, rows[1].target_id, rows[1].edge_id))
        }));
        let oracle = graph_row_chunked_oracle_rows(&ordered);
        let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_some());
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        assert_eq!(graph_row_chunked_result_rows(&proof), oracle[..100]);
        assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        eprintln!("{GROUP}/{TOPK}: class=comparable-postphase-success");
        group.bench_function(TOPK, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[3] {
        let fixture = scale.as_ref().unwrap();
        let mut walk = graph_row_edgepull_query(10_000, false, false, true);
        assert!(!walk.options.include_plan);
        let unpinned = fixture.engine.query_graph_rows(&walk).unwrap();
        let effective_epoch = unpinned.stats.effective_at_epoch;
        walk.at_epoch = Some(effective_epoch);
        let mut retained_cursor = None;
        let mut walked_pages = 0usize;
        while walked_pages * walk.page.limit < 170_000 {
            walk.page.cursor = retained_cursor;
            let page = fixture.engine.query_graph_rows(&walk).unwrap();
            let start = walked_pages * walk.page.limit;
            let end = start + walk.page.limit;
            assert_eq!(
                graph_row_chunked_result_rows(&page),
                graph_row_chunked_oracle_rows(&fixture.rows[start..end])
            );
            assert_eq!(page.stats.effective_at_epoch, effective_epoch);
            retained_cursor = page.next_cursor;
            assert!(retained_cursor.is_some());
            walked_pages += 1;
        }
        assert_eq!(walked_pages, 17);
        let expected_start = walked_pages * walk.page.limit;
        assert_eq!(expected_start, 170_000);
        assert_eq!(expected_start * 100 / GRAPH_ROW_EDGEPULL_EDGE_COUNT, 85);
        let expected_end = expected_start + walk.page.limit;
        let expected = graph_row_chunked_oracle_rows(&fixture.rows[expected_start..expected_end]);
        let mut query = graph_row_edgepull_query(10_000, false, false, true);
        query.at_epoch = Some(effective_epoch);
        query.page.cursor = retained_cursor;
        let first = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(graph_row_chunked_result_rows(&first), expected);
        assert_eq!(first.stats.effective_at_epoch, effective_epoch);
        let continuation_cursor = first
            .next_cursor
            .clone()
            .expect("85% EdgePull page must retain a continuation cursor");
        let continuation_end = expected_end + query.page.limit;
        let mut continuation_query = query.clone();
        continuation_query.page.cursor = Some(continuation_cursor);
        let continuation = fixture
            .engine
            .query_graph_rows(&continuation_query)
            .unwrap();
        assert_eq!(
            graph_row_chunked_result_rows(&continuation),
            graph_row_chunked_oracle_rows(&fixture.rows[expected_end..continuation_end])
        );
        assert!(continuation.next_cursor.is_some());
        let repeated = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(repeated.rows, first.rows);
        assert_eq!(repeated.next_cursor, first.next_cursor);
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        assert_eq!(graph_row_chunked_result_rows(&proof), expected);
        assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        eprintln!(
            "{GROUP}/{DEEP_CURSOR}: class=comparable-postphase-success; walked_pages={walked_pages}; next_page_start={expected_start}; cursor_seek=none"
        );
        group.bench_function(DEEP_CURSOR, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[4] {
        let fixture = scale.as_ref().unwrap();
        let query = graph_row_edgepull_query(10, false, false, false);
        assert_eq!(
            query.options.max_frontier,
            GraphQueryOptions::default().max_frontier
        );
        assert_eq!(
            query.options.max_intermediate_bindings,
            GraphQueryOptions::default().max_intermediate_bindings
        );
        assert!(!query.options.include_plan);
        let started = Instant::now();
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        let wall_time = started.elapsed();
        assert_eq!(
            graph_row_chunked_result_rows(&proof),
            graph_row_chunked_oracle_rows(&fixture.rows[..10])
        );
        assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        eprintln!(
            "{GROUP}/{DEFAULT_CAPS}: class=capability-postphase-success; oracle_rows=10; wall_time={wall_time:?}"
        );
        group.bench_function(DEFAULT_CAPS, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query))))
        });
    }

    if requested[5] {
        let fixture = fo033.as_ref().unwrap();
        let query = graph_row_chunked_fo033_query(&[]);
        assert!(query.nodes[0].ids.is_empty());
        assert_eq!(fixture.expected_target_ids.len(), 3);
        assert!(!query.options.include_plan);
        let started = Instant::now();
        let proof = assert_graph_row_edgepull_source_plan(&fixture.engine, &query);
        let wall_time = started.elapsed();
        assert_eq!(
            graph_row_chunked_single_node_ids(&proof),
            fixture.expected_target_ids
        );
        assert_graph_row_edgepull_runtime(&proof, 8, 2);
        eprintln!(
            "{GROUP}/{FO033}: class=capability-postphase-success; oracle=[19,13,7]; wall_time={wall_time:?}"
        );

        let warm_fixture = build_graph_row_edgepull_hub_evidence_engine();
        assert_eq!(warm_fixture.flushed_hub_batches, 3);
        assert_eq!(warm_fixture.active_hub_tail_count, 20_000);
        assert_eq!(warm_fixture.active_seed_tail_count, 2_500);
        let warm = collect_graph_row_edgepull_qpx036_evidence(&warm_fixture);
        for line in &warm.two_hop_lines {
            eprintln!("QPX-036 warm two-hop: {line}");
        }
        for line in &warm.stage_lines {
            eprintln!("QPX-036 warm stage: {line}");
        }
        let reopened_fixture =
            fully_flush_and_reopen_graph_row_edgepull_hub_evidence_engine(warm_fixture);
        assert_eq!(reopened_fixture.flushed_hub_batches, 4);
        assert_eq!(reopened_fixture.active_hub_tail_count, 0);
        assert_eq!(reopened_fixture.active_seed_tail_count, 0);
        let reopened = collect_graph_row_edgepull_qpx036_evidence(&reopened_fixture);
        for line in &reopened.two_hop_lines {
            eprintln!("QPX-036 reopened two-hop: {line}");
        }
        for line in &reopened.stage_lines {
            eprintln!("QPX-036 reopened stage: {line}");
        }
        eprintln!(
            "QPX-036 warm/reopened line-set match: two_hop={}; stage={}",
            warm.two_hop_lines == reopened.two_hop_lines,
            warm.stage_lines == reopened.stage_lines
        );
        group.bench_function(FO033, |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query))))
        });
    }

    if requested[6] {
        let fixture = scale.as_ref().unwrap();
        let expected = graph_row_chunked_oracle_rows(&fixture.rows[..10]);
        let native_query = graph_row_edgepull_query(10, false, false, true);
        let native = assert_graph_row_edgepull_source_plan(&fixture.engine, &native_query);
        assert_eq!(graph_row_chunked_result_rows(&native), expected);
        assert_graph_row_edgepull_runtime(&native, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 2);
        let params = GqlParams::new();
        let query = "MATCH (source)-[edge:BenchEdgePull]->(target) RETURN id(source), id(edge), id(target) LIMIT 10";
        let options = GqlExecutionOptions {
            allow_full_scan: false,
            max_frontier: GRAPH_ROW_EDGEPULL_EDGE_COUNT,
            max_intermediate_bindings: GRAPH_ROW_EDGEPULL_EDGE_COUNT,
            include_plan: false,
            ..GqlExecutionOptions::default()
        };
        let explain_options = GqlExecutionOptions {
            include_plan: true,
            ..options.clone()
        };
        let preflight = fixture
            .engine
            .execute_gql(query, &params, &explain_options)
            .unwrap();
        assert_eq!(graph_row_chunked_gql_rows(&preflight), expected);
        assert!(preflight.next_cursor.is_none());
        let read = preflight
            .plan
            .as_ref()
            .and_then(|plan| plan.read.as_ref())
            .expect("GQL EdgePull preflight must include a read plan");
        let projection = read.projection.join("\n");
        for fragment in [
            "ChunkedRowProductionRuntime",
            "source=EdgePull{edge=alias:edge}",
            "eligibility=edge_id_order_not_logical_prefix",
            "early_exit=false",
            "cursor_seek=none",
        ] {
            assert!(
                projection.contains(fragment),
                "GQL EdgePull read-plan projection missing {fragment:?}: {projection}"
            );
        }
        let repeated = fixture
            .engine
            .execute_gql(query, &params, &options)
            .unwrap();
        assert_eq!(repeated.rows, preflight.rows);
        assert_eq!(repeated.next_cursor, preflight.next_cursor);
        eprintln!("{GROUP}/{GQL_UNORDERED}: class=comparable-postphase-success");
        group.bench_function(GQL_UNORDERED, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .engine
                        .execute_gql(black_box(query), black_box(&params), black_box(&options))
                        .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn bench_graph_row_phase43b(c: &mut Criterion) {
    const HUB_GROUP: &str = "graph_row_hub_poisoned";
    const NATIVE: &str = "native_unordered_limit10";
    const EXPLICIT: &str = "explicit_ids_residual_limit10";
    const WORST_GROUP: &str = "graph_row_planner_known_id_worst_case";
    const KNOWN_1024: &str = "known_ids_1024_high_both";
    const ABOVE_CAP_1025: &str = "above_cap_ids_1025_high_both";

    let requested = [
        criterion_requested_benchmark(HUB_GROUP, NATIVE),
        criterion_requested_benchmark(HUB_GROUP, EXPLICIT),
        criterion_requested_benchmark(WORST_GROUP, KNOWN_1024),
        criterion_requested_benchmark(WORST_GROUP, ABOVE_CAP_1025),
    ];
    if !requested.iter().any(|requested| *requested) {
        return;
    }

    let fixture = build_graph_row_hub_poisoned_scale_engine();
    assert_eq!(fixture.node_ids.len(), 10_000);
    assert_eq!(fixture.edge_count, 200_000);
    assert_eq!(fixture.flushed_segment_count, 3);
    assert_eq!(fixture.active_tail_edge_count, 20_000);
    assert_eq!(fixture.east_anchor_ids.len(), 2_500);
    assert_eq!(
        fixture.hub_source_ids,
        Some([fixture.node_ids[0], fixture.node_ids[1]])
    );
    assert_eq!(fixture.rows.len(), 47_510);
    assert_eq!(fixture.residual_rows.len(), 6_253);

    if requested[0] || requested[1] {
        let mut group = c.benchmark_group(HUB_GROUP);
        group.sample_size(10);

        if requested[0] {
            let query = graph_row_hub_native_query();
            assert!(!query.options.allow_full_scan);
            assert!(!query.options.include_plan);
            let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
            let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
            assert!(result.next_cursor.is_some());
            let _ = assert_graph_row_hub_native_plan(&fixture.engine, &query);

            let mut default_caps_query = query.clone();
            default_caps_query.options.max_intermediate_bindings =
                GraphQueryOptions::default().max_intermediate_bindings;
            default_caps_query.options.max_frontier = GraphQueryOptions::default().max_frontier;
            let default_result =
                assert_graph_row_chunked_page(&fixture.engine, &default_caps_query, &oracle);
            assert!(default_result.next_cursor.is_some());
            group.bench_function(NATIVE, |b| {
                b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
            });

            let reopened = fully_flush_and_reopen_graph_row_hub_poisoned_scale_engine(
                build_graph_row_hub_poisoned_scale_engine(),
            );
            assert_eq!(reopened.edge_count, 200_000);
            assert_eq!(reopened.flushed_segment_count, 4);
            assert_eq!(reopened.active_tail_edge_count, 0);
            let reopened_result = assert_graph_row_chunked_page(&reopened.engine, &query, &oracle);
            assert!(reopened_result.next_cursor.is_some());
            let _ = assert_graph_row_hub_native_plan(&reopened.engine, &query);
            let reopened_default =
                assert_graph_row_chunked_page(&reopened.engine, &default_caps_query, &oracle);
            assert!(reopened_default.next_cursor.is_some());
        }

        if requested[1] {
            let explicit_source_ids = fixture.east_anchor_ids[..20].to_vec();
            let expected_source_ids = (2..80)
                .step_by(4)
                .map(|ordinal| fixture.node_ids[ordinal])
                .collect::<Vec<_>>();
            assert_eq!(explicit_source_ids, expected_source_ids);
            assert!(explicit_source_ids.windows(2).all(|ids| ids[0] < ids[1]));
            let candidate_rows = fixture
                .rows
                .iter()
                .copied()
                .filter(|row| explicit_source_ids.binary_search(&row.source_id).is_ok())
                .collect::<Vec<_>>();
            let residual_rows = fixture
                .residual_rows
                .iter()
                .copied()
                .filter(|row| explicit_source_ids.binary_search(&row.source_id).is_ok())
                .collect::<Vec<_>>();
            assert_eq!(candidate_rows.len(), 390);
            assert_eq!(residual_rows.len(), 50);
            let query = graph_row_hub_explicit_ids_query(&explicit_source_ids);
            assert_eq!(
                query.nodes[0].label_filter.as_ref().unwrap().labels,
                ["GraphRowChunkNode"]
            );
            assert_eq!(query.nodes[0].ids, explicit_source_ids);
            assert!(query.nodes[0].filter.is_none());
            assert!(!query.options.allow_full_scan);
            assert!(!query.options.include_plan);
            let oracle = graph_row_chunked_oracle_rows(&residual_rows);
            let result = assert_graph_row_chunked_page(&fixture.engine, &query, &oracle);
            assert!(result.next_cursor.is_some());
            let explain = assert_graph_row_explicit_id_plan(&fixture.engine, &query);
            assert_graph_row_explicit_id_plan_cost(&explain, 411);
            assert!(
                graph_row_explain_has_node(
                    &explain.plan,
                    "AdjacencyExpansion",
                    &["edge=alias:edge", "direction=Outgoing"]
                ),
                "explicit-ID residual plan must retain its outgoing expansion: {explain:?}"
            );
            let mut alternatives = Vec::new();
            graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
            eprintln!(
                "CP43b.1 explicit-ID plan alternatives: {}",
                alternatives.join(" | ")
            );
            group.bench_function(EXPLICIT, |b| {
                b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
            });
        }

        group.finish();
    }

    if requested[2] || requested[3] {
        let high_1024 = fixture.node_ids[fixture.node_ids.len() - 1_024..].to_vec();
        let high_1025 = fixture.node_ids[fixture.node_ids.len() - 1_025..].to_vec();
        assert_eq!(high_1024.len(), 1_024);
        assert_eq!(high_1025.len(), 1_025);
        assert!(high_1024.windows(2).all(|ids| ids[0] < ids[1]));
        assert!(high_1025.windows(2).all(|ids| ids[0] < ids[1]));
        assert_eq!(high_1024, fixture.node_ids[8_976..]);
        assert_eq!(high_1025, fixture.node_ids[8_975..]);

        let known_query = graph_row_known_ids_worst_case_query(&high_1024);
        let above_cap_query = graph_row_known_ids_worst_case_query(&high_1025);
        let mut known_structure = known_query.clone();
        let mut above_cap_structure = above_cap_query.clone();
        known_structure.nodes[0].ids.clear();
        above_cap_structure.nodes[0].ids.clear();
        assert_eq!(known_structure, above_cap_structure);
        assert_eq!(known_query.nodes[0].ids.len(), 1_024);
        assert_eq!(above_cap_query.nodes[0].ids.len(), 1_025);
        assert!(known_query.nodes[0].filter.is_none());
        assert!(above_cap_query.nodes[0].filter.is_none());
        assert_eq!(known_query.page, above_cap_query.page);
        assert_eq!(known_query.options, above_cap_query.options);
        let GraphPatternPiece::Edge(known_edge) = &known_query.pieces[0] else {
            panic!("known-ID query must retain its fixed edge");
        };
        let GraphPatternPiece::Edge(above_cap_edge) = &above_cap_query.pieces[0] else {
            panic!("above-cap query must retain its fixed edge");
        };
        assert_eq!(known_edge.direction, Direction::Both);
        assert_eq!(above_cap_edge.direction, Direction::Both);

        let known_explain =
            assert_graph_row_known_id_worst_case_plan(&fixture.engine, &known_query, 39_937);
        let above_cap_explain =
            assert_graph_row_known_id_worst_case_plan(&fixture.engine, &above_cap_query, 401_026);
        for (scenario, explain) in [
            (KNOWN_1024, &known_explain),
            (ABOVE_CAP_1025, &above_cap_explain),
        ] {
            let mut alternatives = Vec::new();
            graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
            eprintln!(
                "CP43b.1 {scenario} plan alternatives: {}",
                alternatives.join(" | ")
            );
        }

        let mut group = c.benchmark_group(WORST_GROUP);
        group.sample_size(10);
        if requested[2] {
            group.bench_function(KNOWN_1024, |b| {
                b.iter(|| {
                    black_box(
                        fixture
                            .engine
                            .explain_graph_rows(black_box(&known_query))
                            .unwrap(),
                    )
                })
            });
        }
        if requested[3] {
            group.bench_function(ABOVE_CAP_1025, |b| {
                b.iter(|| {
                    black_box(
                        fixture
                            .engine
                            .explain_graph_rows(black_box(&above_cap_query))
                            .unwrap(),
                    )
                })
            });
        }
        group.finish();
    }
}

fn bench_graph_row_streamed(c: &mut Criterion) {
    const GROUP: &str = "graph_row_streamed";
    const HUB_BROAD: &str = "graph_row_hub_frontier_broad_equality";
    const HUB_SELECTIVE: &str = "graph_row_hub_frontier_selective_anchor";
    const HUB_UNFILTERED: &str = "graph_row_hub_frontier_unfiltered_traversal";
    const UNBOUND_INTERSECT: &str = "graph_row_unbound_pair_broad_intersect_limit";
    const STALE_HEAVY: &str = "graph_row_stale_heavy_edge_match";
    const FO033: &str = "graph_row_fo033_optional_order_limit";
    const VLP_FILTERED: &str = "graph_row_vlp_filtered_two_hop";
    const GQL_HUB_BROAD: &str = "gql_graph_row_hub_frontier_broad_equality";

    let requested = [
        HUB_BROAD,
        HUB_SELECTIVE,
        HUB_UNFILTERED,
        UNBOUND_INTERSECT,
        STALE_HEAVY,
        FO033,
        VLP_FILTERED,
        GQL_HUB_BROAD,
    ]
    .map(|scenario| criterion_requested_benchmark(GROUP, scenario));
    if !requested.iter().any(|requested| *requested) {
        return;
    }

    let needs_normal = requested[0]
        || requested[1]
        || requested[2]
        || requested[3]
        || requested[6]
        || requested[7];
    let normal = needs_normal.then(build_graph_row_streaming_scale_engine);
    let stale = requested[4].then(build_graph_row_streaming_scale_engine_stale_heavy);
    let fo033 = requested[5].then(build_graph_row_fo033_scale_engine);

    if let Some(fixture) = normal.as_ref() {
        assert_eq!(fixture.edge.node_ids.len(), EDGE_STREAMING_SCALE_NODE_COUNT);
        assert_eq!(
            fixture.edge.edge_ids.len(),
            EDGE_STREAMING_SCALE_TOTAL_EDGES
        );
        assert_eq!(fixture.edge.hub_source_ids.len(), 2);
        assert_eq!(
            fixture.edge.hot_ids.len(),
            EDGE_STREAMING_SCALE_TOTAL_EDGES / 2
        );
    }
    if let Some(fixture) = stale.as_ref() {
        assert_eq!(fixture.edge.node_ids.len(), EDGE_STREAMING_SCALE_NODE_COUNT);
        assert_eq!(
            fixture.edge.edge_ids.len(),
            EDGE_STREAMING_SCALE_TOTAL_EDGES
        );
        assert_eq!(fixture.edge.stale_rewrite_count, 85_500);
        assert_eq!(fixture.edge.hot_ids.len(), 14_500);
    }
    if let Some(fixture) = fo033.as_ref() {
        assert_eq!(fixture.selected_role_count, 2_000);
        assert_eq!(fixture.optional_edge_count, 250);
    }

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(10);

    if requested[0] {
        let fixture = normal.as_ref().unwrap();
        let query = graph_row_streaming_fixed_query(
            Some(fixture.edge.hub_source_ids[0]),
            Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
            QUERY_LIMIT,
        );
        assert_graph_row_streaming_success(
            &fixture.edge.engine,
            &query,
            QUERY_LIMIT,
            &[
                "source=DelegatedEdgeQuery",
                "EdgeEndpointAdjacency",
                "EdgePropertyEqualityIndex",
                "planned_modes=streamed",
            ],
        );
        group.bench_function(HUB_BROAD, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .query_graph_rows(black_box(&query))
                        .unwrap(),
                )
            })
        });
    }

    if requested[1] {
        let fixture = normal.as_ref().unwrap();
        let query = graph_row_streaming_fixed_query(
            Some(fixture.edge.hub_source_ids[0]),
            Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::PropertyEquals {
                    key: "tenant".to_string(),
                    value: PropValue::String("t000".to_string()),
                },
            ])),
            QUERY_LIMIT,
        );
        assert_graph_row_streaming_success(
            &fixture.edge.engine,
            &query,
            QUERY_LIMIT,
            &[
                "source=DelegatedEdgeQuery",
                "EdgePropertyEqualityIndex",
                "planned_modes=eager",
            ],
        );
        group.bench_function(HUB_SELECTIVE, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .query_graph_rows(black_box(&query))
                        .unwrap(),
                )
            })
        });
    }

    if requested[2] {
        let fixture = normal.as_ref().unwrap();
        let query = graph_row_streaming_fixed_query(
            Some(fixture.edge.hub_source_ids[0]),
            None,
            QUERY_LIMIT,
        );
        assert_graph_row_streaming_success(
            &fixture.edge.engine,
            &query,
            QUERY_LIMIT,
            &["source=EndpointAdjacency"],
        );
        let unfiltered_plan =
            graph_row_streaming_plan_text(&fixture.edge.engine.query_graph_rows(&query).unwrap());
        assert!(!unfiltered_plan.contains("DelegatedEdgeQuery"));
        group.bench_function(HUB_UNFILTERED, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .query_graph_rows(black_box(&query))
                        .unwrap(),
                )
            })
        });
    }

    if requested[3] {
        let fixture = normal.as_ref().unwrap();
        let query = graph_row_streaming_fixed_query(
            None,
            Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("hot".to_string()),
                },
                EdgeFilterExpr::PropertyEquals {
                    key: "tenant".to_string(),
                    value: PropValue::String("t000".to_string()),
                },
            ])),
            QUERY_LIMIT,
        );
        assert_graph_row_streaming_success(
            &fixture.edge.engine,
            &query,
            QUERY_LIMIT,
            &[
                "source=EdgePull{edge=alias:edge}",
                "mode: Eager",
                "verified_candidates=210",
            ],
        );
        group.bench_function(UNBOUND_INTERSECT, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .query_graph_rows(black_box(&query))
                        .unwrap(),
                )
            })
        });
    }

    if requested[4] {
        let fixture = stale.as_ref().unwrap();
        let query = graph_row_streaming_fixed_query(
            None,
            Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
            QUERY_LIMIT,
        );
        match fixture.edge.engine.query_graph_rows(&query) {
            Ok(result) => {
                assert_eq!(result.rows.len(), QUERY_LIMIT);
                let plan = graph_row_streaming_plan_text(&result);
                for fragment in [
                    "source=EdgePull{edge=alias:edge}",
                    "EdgePropertyEqualityIndex",
                    "prepared_source=StreamedSingle",
                ] {
                    assert!(
                        plan.contains(fragment),
                        "stale-heavy capability must prove executed delegated/streamed edge plan fragment {fragment:?}: {plan}"
                    );
                }
                eprintln!("{GROUP}/{STALE_HEAVY}: class=capability-postphase-success");
            }
            Err(error) => {
                assert!(error.to_string().contains("max_frontier"), "{error:?}");
                eprintln!("{GROUP}/{STALE_HEAVY}: class=capability-prephase-error");
            }
        }
        group.bench_function(STALE_HEAVY, |b| {
            b.iter(|| black_box(fixture.edge.engine.query_graph_rows(black_box(&query))))
        });
    }

    if requested[5] {
        let fixture = fo033.as_ref().unwrap();
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            include_plan: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (source)-[edge:BenchEdge1]->(target) WHERE edge.role = 'selected' OPTIONAL MATCH (target)-[opt:BenchEdgeOptional]->(doc) RETURN id(target) AS target_id, edge.score AS score ORDER BY edge.score DESC, id(target) LIMIT 100";
        match fixture
            .graph
            .edge
            .engine
            .execute_gql(query, &params, &options)
        {
            Ok(result) => {
                assert_eq!(result.rows.len(), QUERY_LIMIT);
                let read = result
                    .plan
                    .as_ref()
                    .and_then(|plan| plan.read.as_ref())
                    .expect("FO-033 benchmark must include a read plan");
                assert!(
                    read.pushed_down
                        .iter()
                        .any(|item| item.contains("edge.role")),
                    "FO-033 benchmark must prove the role predicate was pushed down: {read:?}"
                );
                let runtime_plan = read.projection.join("\n");
                for fragment in [
                    "GraphRowSourceRead",
                    "GraphRowPreparedEdgeSource",
                    "materialized_source=EdgePullDelegatedMetadata",
                    "source=EdgePull{edge=alias:edge}",
                ] {
                    assert!(
                        runtime_plan.contains(fragment),
                        "FO-033 benchmark must expose executed runtime fragment {fragment:?}: {runtime_plan}"
                    );
                }
                let delegated_probe = graph_row_streaming_fixed_query(
                    None,
                    Some(EdgeFilterExpr::PropertyEquals {
                        key: "role".to_string(),
                        value: PropValue::String("selected".to_string()),
                    }),
                    QUERY_LIMIT,
                );
                assert_graph_row_streaming_success(
                    &fixture.graph.edge.engine,
                    &delegated_probe,
                    QUERY_LIMIT,
                    &[
                        "source=EdgePull{edge=alias:edge}",
                        "choice=EdgePullChunk",
                        "eligibility=edge_id_order_not_logical_prefix",
                    ],
                );
                eprintln!("{GROUP}/{FO033}: class=comparable-postphase-success");
            }
            Err(error) => {
                assert!(
                    error.to_string().contains("max_frontier")
                        || error.to_string().contains("max_intermediate_bindings"),
                    "{error:?}"
                );
                eprintln!("{GROUP}/{FO033}: class=capability-prephase-error");
            }
        }
        group.bench_function(FO033, |b| {
            b.iter(|| {
                black_box(fixture.graph.edge.engine.execute_gql(
                    black_box(query),
                    black_box(&params),
                    black_box(&options),
                ))
            })
        });
    }

    if requested[6] {
        let fixture = normal.as_ref().unwrap();
        let query = graph_row_streaming_vlp_query(fixture.edge.node_ids[2], QUERY_LIMIT);
        let result = fixture.edge.engine.query_graph_rows(&query).unwrap();
        assert!(!result.rows.is_empty());
        assert!(result.rows.len() <= QUERY_LIMIT);
        let plan = graph_row_streaming_plan_text(&result);
        assert!(plan.contains("VariableLengthPathRuntime"));
        assert!(plan.contains("step_source=DelegatedEdgeQuery"));
        assert!(plan.contains("queries="));
        assert!(plan.contains("verified_step_edges="));
        assert!(plan.contains("planned_modes="));
        assert!(plan.contains("planned_warnings=["));
        group.bench_function(VLP_FILTERED, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .query_graph_rows(black_box(&query))
                        .unwrap(),
                )
            })
        });
    }

    if requested[7] {
        let fixture = normal.as_ref().unwrap();
        let params = GqlParams::from([(
            "source_id".to_string(),
            GqlParamValue::UInt(fixture.edge.hub_source_ids[0]),
        )]);
        let options = GqlExecutionOptions {
            include_plan: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (source)-[edge:BenchEdge1]->(target) WHERE id(source) = $source_id AND edge.status = 'hot' RETURN id(target) AS target_id LIMIT 100";
        let result = fixture
            .edge
            .engine
            .execute_gql(query, &params, &options)
            .unwrap();
        assert_eq!(result.rows.len(), QUERY_LIMIT);
        let plan = format!("{:?}", result.plan.as_ref().expect("GQL hub plan"));
        for fragment in [
            "id(source)",
            "edge.status",
            "GraphRowSourceRead",
            "DelegatedEdgeQuery",
            "planned_modes=streamed",
            "planned_warnings=[",
        ] {
            assert!(plan.contains(fragment), "missing {fragment:?} from {plan}");
        }
        // The native probe additionally pins the endpoint-intersect counters, which are
        // intentionally not part of the public GQL explain shape.
        let delegated_probe = graph_row_streaming_fixed_query(
            Some(fixture.edge.hub_source_ids[0]),
            Some(EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            }),
            QUERY_LIMIT,
        );
        assert_graph_row_streaming_success(
            &fixture.edge.engine,
            &delegated_probe,
            QUERY_LIMIT,
            &[
                "source=DelegatedEdgeQuery",
                "EdgeEndpointAdjacency",
                "EdgePropertyEqualityIndex",
                "planned_modes=streamed",
            ],
        );
        group.bench_function(GQL_HUB_BROAD, |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .edge
                        .engine
                        .execute_gql(black_box(query), black_box(&params), black_box(&options))
                        .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn temp_gql_mutation_bench_db() -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    (dir, engine)
}

fn gql_bench_node(label: &str, key: &str, props: BTreeMap<String, PropValue>) -> NodeInput {
    NodeInput {
        labels: vec![label.to_string()],
        key: key.to_string(),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn gql_bench_props(values: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn setup_gql_set_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    engine
        .batch_upsert_nodes(vec![gql_bench_node(
            "GqlBenchSet",
            "n",
            gql_bench_props(&[("status", PropValue::String("old".to_string()))]),
        )])
        .unwrap();
    (dir, engine)
}

fn setup_gql_detach_delete_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    let node_ids = engine
        .batch_upsert_nodes(vec![
            gql_bench_node("GqlBenchDetach", "hub", BTreeMap::new()),
            gql_bench_node("GqlBenchDetach", "left", BTreeMap::new()),
            gql_bench_node("GqlBenchDetach", "right", BTreeMap::new()),
        ])
        .unwrap();
    engine
        .batch_upsert_edges(vec![
            EdgeInput {
                from: node_ids[0],
                to: node_ids[1],
                label: "GQL_BENCH_DETACH".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: node_ids[2],
                to: node_ids[0],
                label: "GQL_BENCH_DETACH".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
        ])
        .unwrap();
    (dir, engine)
}

fn setup_gql_mutation_return_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    let nodes: Vec<NodeInput> = (0..3)
        .map(|i| {
            gql_bench_node(
                "GqlBenchReturn",
                &format!("n{i}"),
                gql_bench_props(&[
                    ("rank", PropValue::Int(i as i64)),
                    ("status", PropValue::String("old".to_string())),
                ]),
            )
        })
        .collect();
    engine.batch_upsert_nodes(nodes).unwrap();
    (dir, engine)
}

fn setup_gql_merge_match_smoke_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_gql_mutation_bench_db();
    engine
        .batch_upsert_nodes(vec![gql_bench_node(
            "GqlBenchMerge",
            "n",
            gql_bench_props(&[("status", PropValue::String("old".to_string()))]),
        )])
        .unwrap();
    (dir, engine)
}

fn assert_gql_mutation_result(
    result: overgraph::GqlExecutionResult,
    expected_rows: usize,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Mutation);
    assert_eq!(result.rows.len(), expected_rows);
    assert_eq!(result.stats.rows_returned, expected_rows);
    assert!(result.next_cursor.is_none());
    assert!(result.mutation_stats.is_some());
    result
}

const GQL_SCHEMA_ALTER_ADD_BENCH: &str = "ALTER CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128 }";
const GQL_SCHEMA_CHECK_ADD_BENCH: &str = "CHECK CURRENT GRAPH TYPE ADD { NODE SchemaPerson = { properties: { name: { required: true, nullable: false, types: ['string'] } } }, EDGE SCHEMA_WORKS_AT = { from: { all_of: ['SchemaPerson'] }, to: { all_of: ['SchemaCompany'] }, properties: { role: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { chunk_size: 128, max_violations: 4 }";

fn setup_gql_schema_existing_data_db() -> (tempfile::TempDir, DatabaseEngine) {
    let (dir, engine) = temp_db();
    let ids = engine
        .batch_upsert_nodes(vec![
            NodeInput {
                labels: vec!["SchemaPerson".to_string()],
                key: "person-0".to_string(),
                props: BTreeMap::from([(
                    "name".to_string(),
                    PropValue::String("name-0".to_string()),
                )]),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["SchemaCompany".to_string()],
                key: "company-0".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
        ])
        .unwrap();
    engine
        .batch_upsert_edges(vec![EdgeInput {
            from: ids[0],
            to: ids[1],
            label: "SCHEMA_WORKS_AT".to_string(),
            props: BTreeMap::from([("role".to_string(), PropValue::String("role-0".to_string()))]),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }])
        .unwrap();
    (dir, engine)
}

fn assert_gql_schema_targets_published(
    result: overgraph::GqlExecutionResult,
    expected_targets: u64,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Schema);
    let stats = result.schema_stats.as_ref().unwrap();
    assert_eq!(stats.targets_published, expected_targets);
    result
}

fn assert_gql_schema_violations(
    result: overgraph::GqlExecutionResult,
    expected_violations: u64,
) -> overgraph::GqlExecutionResult {
    assert_eq!(result.kind, GqlStatementKind::Schema);
    let stats = result.schema_stats.as_ref().unwrap();
    assert_eq!(stats.violation_count, expected_violations);
    result
}

fn bench_gql_queries(c: &mut Criterion) {
    let mut graph_group = c.benchmark_group("graph_row_query");
    graph_group.sample_size(20);

    graph_group.bench_function("graph_row_fixed_connected_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let query = graph_row_fixed_query(source_id, QUERY_LIMIT);
        b.iter(|| {
            let result = engine.query_graph_rows(black_box(&query)).unwrap();
            black_box(assert_graph_row_result_count(result, QUERY_LIMIT))
        });
    });

    graph_group.bench_function("graph_row_optional_edge_traversal_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let query = graph_row_optional_query(source_id, QUERY_LIMIT);
        b.iter(|| {
            let result = engine.query_graph_rows(black_box(&query)).unwrap();
            black_box(assert_graph_row_result_count(result, QUERY_LIMIT))
        });
    });

    graph_group.bench_function("explain_graph_row_bound_edge_source_memo", |b| {
        let (_dir, engine, node_ids) = build_graph_row_planner_memo_engine();
        let query = graph_row_planner_memo_query(&node_ids);
        b.iter(|| black_box(engine.explain_graph_rows(black_box(&query)).unwrap()));
    });

    graph_group.bench_function("explain_graph_row_active_memtable_planning_memo", |b| {
        let (_dir, engine, node_ids) = build_graph_row_active_memtable_planner_engine();
        let query = graph_row_active_memtable_planner_query(&node_ids);
        b.iter(|| black_box(engine.explain_graph_rows(black_box(&query)).unwrap()));
    });

    graph_group.bench_function("query_graph_row_many_alternatives_no_plan", |b| {
        let (_dir, engine, node_ids) = build_graph_row_planner_memo_engine();
        let query = graph_row_no_plan_many_alternatives_query(&node_ids);
        b.iter(|| black_box(engine.query_graph_rows(black_box(&query)).unwrap()));
    });

    graph_group.finish();

    let mut group = c.benchmark_group("execute_gql");
    group.sample_size(20);

    group.bench_function("native_query_node_ids_indexed_property_baseline", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let query = status_active_query(Some(GqlExecutionOptions::default().max_rows));
        b.iter(|| black_box(engine.query_node_ids(black_box(&query)).unwrap()));
    });

    group.bench_function("gql_explain_parse_lower_plan_ordered", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .explain_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_id_indexed_property", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n)";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_property_no_hydration", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_return_node_element_without_vectors", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_order_by_limit", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_full_scan_opt_in", |b| {
        let (_dir, engine) = build_fallback_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            allow_full_scan: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (n) WHERE n.region = 'r03' RETURN id(n) LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_direct_edge_property_indexed_row_ops", |b| {
        let (_dir, engine, _source_id, _edge_ids, _valid_epoch) = build_edge_query_indexed_engine();
        let params = GqlParams::from([(
            "role".to_string(),
            GqlParamValue::String("lead".to_string()),
        )]);
        let options = GqlExecutionOptions::default();
        let query = "MATCH ()-[r:BenchEdge10]->() WHERE r.role = $role RETURN id(r), r.score ORDER BY r.score DESC LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_include_plan_profile", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            include_plan: true,
            profile: true,
            ..GqlExecutionOptions::default()
        };
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN n.tenant ORDER BY n.score LIMIT 25";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_union_all_two_indexed_branches", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n) AS id \
                     UNION ALL \
                     MATCH (n:BenchNode1) WHERE n.tier = 'gold' RETURN id(n) AS id";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1_500);
            black_box(result)
        });
    });

    group.bench_function("gql_union_dedupe_overlapping_indexed_branches", |b| {
        let (_dir, engine) = build_indexed_query_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:BenchNode1) WHERE n.status = 'active' RETURN id(n) AS id \
                     UNION \
                     MATCH (n:BenchNode1) WHERE n.tier = 'gold' RETURN id(n) AS id";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1_000);
            black_box(result)
        });
    });

    group.bench_function("gql_fixed_one_hop_pattern", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (p:BenchNode1)-[r:BenchEdge10]->(c:BenchNode2) RETURN id(p), id(r), id(c)";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_shortest_path_bounded_endpoint_smoke", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (a:BenchNode1) WHERE elementKey(a) = 'acct-0' \
                     WITH a \
                     MATCH (b:BenchNode2) WHERE elementKey(b) = 'company-0' \
                     WITH a, b \
                     MATCH p = shortestPath((a)-[:BenchEdge10*1..1]->(b)) \
                     RETURN length(p)";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), 1);
            black_box(result)
        });
    });

    group.bench_function("gql_fixed_branching_pattern", |b| {
        let (_dir, engine, _company_id) = build_pattern_engine();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (p:BenchNode1)-[:BenchEdge10]->(c:BenchNode2)<-[:BenchEdge10]-(peer:BenchNode1) RETURN id(p), id(c), id(peer) LIMIT 100";
        b.iter(|| {
            black_box(
                engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap(),
            )
        });
    });

    group.bench_function("gql_graph_row_fixed_connected_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let params = GqlParams::from([
            (
                "role".to_string(),
                GqlParamValue::String("lead".to_string()),
            ),
            ("source".to_string(), GqlParamValue::UInt(source_id)),
        ]);
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (source:BenchNode1)-[edge:BenchEdge10 {role: $role}]->(target:BenchNode2) \
                     WHERE id(source) = $source \
                     RETURN id(source) AS source, id(edge) AS edge, id(target) AS target \
                     ORDER BY edge.score DESC, id(target) LIMIT 100";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), QUERY_LIMIT);
            black_box(result)
        });
    });

    group.bench_function("gql_graph_row_optional_edge_traversal_query", |b| {
        let (_dir, engine, source_id) = build_graph_row_optional_engine();
        let params = GqlParams::from([
            (
                "role".to_string(),
                GqlParamValue::String("lead".to_string()),
            ),
            ("source".to_string(), GqlParamValue::UInt(source_id)),
        ]);
        let options = GqlExecutionOptions::default();
        let query =
            "MATCH (source:BenchNode1)-[edge:BenchEdge10 {role: $role}]->(target:BenchNode2) \
                     WHERE id(source) = $source \
                     OPTIONAL MATCH (target)-[ref:BenchEdge20]->(doc:BenchNode3) \
                     RETURN id(source) AS source, id(edge) AS edge, id(target) AS target, \
                            id(ref) AS ref, id(doc) AS doc \
                     ORDER BY edge.score DESC, id(target) LIMIT 100";
        b.iter(|| {
            let result = engine
                .execute_gql(black_box(query), black_box(&params), black_box(&options))
                .unwrap();
            assert_eq!(result.rows.len(), QUERY_LIMIT);
            black_box(result)
        });
    });

    group.bench_function("gql_schema_alter_add_existing_data", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        b.iter_batched(
            setup_gql_schema_existing_data_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(
                        black_box(GQL_SCHEMA_ALTER_ADD_BENCH),
                        black_box(&params),
                        black_box(&options),
                    )
                    .unwrap();
                black_box(assert_gql_schema_targets_published(result, 2))
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_schema_check_add_existing_data", |b| {
        let (_dir, engine) = setup_gql_schema_existing_data_db();
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        b.iter(|| {
            let result = engine
                .execute_gql(
                    black_box(GQL_SCHEMA_CHECK_ADD_BENCH),
                    black_box(&params),
                    black_box(&options),
                )
                .unwrap();
            black_box(assert_gql_schema_violations(result, 0))
        });
    });

    group.bench_function("gql_mutation_create_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "CREATE (n:GqlBenchCreate {elementKey: 'n', status: 'new'})";
        b.iter_batched(
            temp_gql_mutation_bench_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 1);
                assert_eq!(stats.mutation_ops, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_match_set_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchSet) WHERE elementKey(n) = 'n' SET n.status = 'new'";
        b.iter_batched(
            setup_gql_set_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_updated, 1);
                assert_eq!(stats.properties_set, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_merge_create_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MERGE (n:GqlBenchMerge {elementKey: 'n'}) ON CREATE SET n.status = 'created'";
        b.iter_batched(
            temp_gql_mutation_bench_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 1);
                assert_eq!(stats.nodes_updated, 0);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_merge_match_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MERGE (n:GqlBenchMerge {elementKey: 'n'}) ON MATCH SET n.status = 'matched'";
        b.iter_batched(
            setup_gql_merge_match_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_created, 0);
                assert_eq!(stats.nodes_updated, 1);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_detach_delete_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchDetach) WHERE elementKey(n) = 'hub' DETACH DELETE n";
        b.iter_batched(
            setup_gql_detach_delete_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 0);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.nodes_deleted, 1);
                assert_eq!(stats.edges_deleted, 2);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("gql_mutation_return_smoke", |b| {
        let params = GqlParams::new();
        let options = GqlExecutionOptions::default();
        let query = "MATCH (n:GqlBenchReturn) SET n.touched = true RETURN elementKey(n) AS key ORDER BY n.rank LIMIT 2";
        b.iter_batched(
            setup_gql_mutation_return_smoke_db,
            |(_dir, engine)| {
                let result = engine
                    .execute_gql(black_box(query), black_box(&params), black_box(&options))
                    .unwrap();
                let result = assert_gql_mutation_result(result, 2);
                let stats = result.mutation_stats.as_ref().unwrap();
                assert_eq!(stats.mutation_rows, 3);
                assert_eq!(stats.nodes_updated, 3);
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

const PHASE44B_DISTRIBUTED_EDGE_COUNT: usize = 200_000;
const PHASE44B_PAGE_LIMIT: usize = 10;
const PHASE44B_DEEP_PAGE_LIMIT: usize = 10_000;
const PHASE44B_DEEP_PAGE_COUNT: usize = 17;
const PHASE44B_EDGE_LABEL: &str = "Phase44bDistributed";
const PHASE44B_SPARSE_OTHER_LABEL: &str = "Phase44bSparseOther";
const PHASE44B_SPARSE_STRIDE: usize = 1_000;
const PHASE44B_HUB_EDGE_COUNT: usize = 100_005;
const PHASE44B_HUB_FOLLOWER_EDGE_COUNT: usize = 10_000;
const PHASE44B_HUB_PAGE_LIMIT: usize = 10_000;

struct Phase44bOrderedFixture {
    _dir: tempfile::TempDir,
    options: DbOptions,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    rows: Vec<GraphRowChunkedOracleRow>,
}

fn phase44b_options(memtable_flush_threshold: usize) -> DbOptions {
    DbOptions {
        create_if_missing: true,
        memtable_flush_threshold,
        edge_uniqueness: true,
        compact_after_n_flushes: 0,
        memtable_hard_cap_bytes: 0,
        ..DbOptions::default()
    }
}

fn phase44b_node(prefix: &str, ordinal: usize, residual: bool) -> NodeInput {
    let mut props = BTreeMap::new();
    props.insert(
        "phase44b_survivor".to_string(),
        PropValue::Bool(residual && ordinal.is_multiple_of(10)),
    );
    NodeInput {
        labels: vec!["Phase44bNode".to_string()],
        key: format!("{prefix}-{ordinal}"),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn phase44b_edge_input(from: u64, to: u64, label: &str, payload_byte: u8) -> EdgeInput {
    let mut props = BTreeMap::new();
    props.insert(
        "phase44b_payload".to_string(),
        PropValue::Bytes(vec![payload_byte; 16]),
    );
    EdgeInput {
        from,
        to,
        label: label.to_string(),
        props,
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }
}

fn build_phase44b_distributed_fixture() -> Phase44bOrderedFixture {
    let dir = tempfile::tempdir().unwrap();
    let options = phase44b_options(0);
    let mut engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    engine.ensure_node_label("Phase44bNode").unwrap();
    engine.ensure_edge_label(PHASE44B_EDGE_LABEL).unwrap();
    let node_ids = engine
        .batch_upsert_nodes(
            (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
                .map(|ordinal| phase44b_node("phase44b-distributed", ordinal, true))
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), PHASE44B_DISTRIBUTED_EDGE_COUNT);
    assert!(node_ids.windows(2).all(|ids| ids[0] < ids[1]));
    engine.flush().unwrap();
    engine.close().unwrap();
    engine = DatabaseEngine::open(dir.path(), &options).unwrap();

    let edge_ids = engine
        .batch_upsert_edges(
            (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
                .map(|ordinal| {
                    phase44b_edge_input(
                        node_ids[ordinal],
                        node_ids[(ordinal + 1) % node_ids.len()],
                        PHASE44B_EDGE_LABEL,
                        (ordinal % 251) as u8,
                    )
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(edge_ids.len(), PHASE44B_DISTRIBUTED_EDGE_COUNT);
    assert!(edge_ids.windows(2).all(|ids| ids[0] < ids[1]));

    let mut rows = (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
        .map(|ordinal| GraphRowChunkedOracleRow {
            source_id: node_ids[ordinal],
            edge_id: edge_ids[ordinal],
            target_id: node_ids[(ordinal + 1) % node_ids.len()],
            score: 0,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    assert_eq!(rows.len(), PHASE44B_DISTRIBUTED_EDGE_COUNT);
    assert_eq!(
        rows.iter()
            .map(|row| row.source_id)
            .collect::<BTreeSet<_>>()
            .len(),
        PHASE44B_DISTRIBUTED_EDGE_COUNT
    );
    let stats = engine.stats().unwrap();
    assert!(stats.active_memtable_bytes > 0);
    assert_eq!(stats.immutable_memtable_count, 0);
    assert_eq!(stats.pending_flush_count, 0);

    Phase44bOrderedFixture {
        _dir: dir,
        options,
        engine,
        node_ids,
        rows,
    }
}

fn flush_and_reopen_phase44b_fixture(
    mut fixture: Phase44bOrderedFixture,
) -> Phase44bOrderedFixture {
    fixture.engine.flush().unwrap();
    fixture.engine.close().unwrap();
    fixture.engine = DatabaseEngine::open(fixture._dir.path(), &fixture.options).unwrap();
    let stats = fixture.engine.stats().unwrap();
    assert!(stats.segment_count > 0);
    assert_eq!(stats.active_memtable_bytes, 0);
    assert_eq!(stats.immutable_memtable_count, 0);
    assert_eq!(stats.pending_flush_count, 0);
    fixture
}

fn phase44b_graph_row_query(
    labels: &[&str],
    direction: Direction,
    residual: bool,
    limit: usize,
) -> overgraph::GraphRowQuery {
    overgraph::GraphRowQuery {
        nodes: ["source", "target"]
            .into_iter()
            .map(|alias| GraphNodePattern {
                alias: alias.to_string(),
                label_filter: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            })
            .collect(),
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("edge".to_string()),
            from_alias: "source".to_string(),
            to_alias: "target".to_string(),
            direction,
            label_filter: labels.iter().map(|label| (*label).to_string()).collect(),
            filter: None,
        })],
        where_: residual.then(|| GraphExpr::Binary {
            left: Box::new(GraphExpr::Property {
                alias: "target".to_string(),
                key: "phase44b_survivor".to_string(),
            }),
            op: GraphBinaryOp::Eq,
            right: Box::new(GraphExpr::Bool(true)),
        }),
        return_items: Some(vec![
            graph_row_return_binding("source"),
            graph_row_return_binding("edge"),
            graph_row_return_binding("target"),
        ]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions {
            allow_full_scan: false,
            include_plan: false,
            max_frontier: PHASE44B_DISTRIBUTED_EDGE_COUNT,
            max_intermediate_bindings: PHASE44B_DISTRIBUTED_EDGE_COUNT,
            ..GraphQueryOptions::default()
        },
    }
}

fn assert_phase44b_page(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    oracle: &[(u64, u64, u64)],
) -> overgraph::GraphRowResult {
    let expected_end = query.page.limit.min(oracle.len());
    let first = engine.query_graph_rows(query).unwrap();
    assert_eq!(
        graph_row_chunked_result_rows(&first),
        oracle[..expected_end]
    );
    assert_eq!(first.stats.rows_returned, expected_end);
    assert_eq!(first.next_cursor.is_some(), expected_end < oracle.len());

    let mut pinned = query.clone();
    pinned.at_epoch = Some(first.stats.effective_at_epoch);
    let pinned_first = engine.query_graph_rows(&pinned).unwrap();
    let pinned_repeat = engine.query_graph_rows(&pinned).unwrap();
    assert_eq!(pinned_first.rows, first.rows);
    assert_eq!(pinned_first.next_cursor, first.next_cursor);
    assert_eq!(pinned_repeat.rows, first.rows);
    assert_eq!(pinned_repeat.next_cursor, first.next_cursor);
    assert_eq!(
        pinned_repeat.stats.effective_at_epoch,
        first.stats.effective_at_epoch
    );

    if let Some(cursor) = first.next_cursor.clone() {
        let second_end = (expected_end + query.page.limit).min(oracle.len());
        pinned.page.cursor = Some(cursor);
        let second = engine.query_graph_rows(&pinned).unwrap();
        assert_eq!(
            graph_row_chunked_result_rows(&second),
            oracle[expected_end..second_end]
        );
        assert_eq!(
            second.stats.effective_at_epoch,
            first.stats.effective_at_epoch
        );
    }
    first
}

fn assert_phase44b_edgepull(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected: &[(u64, u64, u64)],
) -> overgraph::GraphRowResult {
    let proof = assert_graph_row_edgepull_source_plan(engine, query);
    assert_eq!(graph_row_chunked_result_rows(&proof), expected);
    assert_graph_row_edgepull_runtime(&proof, GRAPH_ROW_EDGEPULL_SCHEDULED_LEAF_MAX, 1);
    proof
}

fn assert_phase44b_residual_source(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected: &[(u64, u64, u64)],
) -> overgraph::GraphRowResult {
    let mut preflight_query = query.clone();
    preflight_query.options.include_plan = true;
    let result = engine.query_graph_rows(&preflight_query).unwrap();
    assert_eq!(graph_row_chunked_result_rows(&result), expected);
    let explain = result
        .plan
        .as_ref()
        .expect("residual benchmark preflight must include its executed plan");

    let mut alternatives = Vec::new();
    graph_row_explain_details(&explain.plan, "GraphRowPlanAlternative", &mut alternatives);
    let adjacency = alternatives
        .iter()
        .find(|detail| detail.contains("kind=AdjacencyPull"))
        .expect("residual preflight must cost the legal adjacency alternative");
    assert!(
        adjacency.contains("R=None"),
        "an unestimated residual must not claim a prefix discount: {alternatives:?}"
    );
    let chosen_adjacency = adjacency.starts_with("chosen; kind=AdjacencyPull");
    assert!(
        chosen_adjacency
            || alternatives
                .iter()
                .any(|detail| detail.starts_with("chosen; kind=EdgeAnchor")),
        "the locked cost model must choose one legal edge source: {alternatives:?}"
    );

    let mut runtime = Vec::new();
    graph_row_explain_details(&explain.plan, "ChunkedRowProductionRuntime", &mut runtime);
    assert_eq!(runtime.len(), 1, "expected one residual runtime line");
    let expected_source = if chosen_adjacency {
        "source=AdjacencyPull{edge=alias:edge}"
    } else {
        "source=EdgePull{edge=alias:edge}"
    };
    assert!(
        runtime[0].contains(expected_source),
        "runtime source must match the costed choice: {}",
        runtime[0]
    );
    eprintln!(
        "Phase 44b residual capability: {}; {}",
        adjacency, runtime[0]
    );
    result
}

fn assert_phase44b_adjacency_pull(
    engine: &DatabaseEngine,
    query: &overgraph::GraphRowQuery,
    expected: &[(u64, u64, u64)],
    cursor_page: bool,
) -> overgraph::GraphRowResult {
    let mut preflight_query = query.clone();
    preflight_query.options.include_plan = true;
    let result = engine.query_graph_rows(&preflight_query).unwrap();
    assert_eq!(graph_row_chunked_result_rows(&result), expected);
    let explain = result
        .plan
        .as_ref()
        .expect("AdjacencyPull benchmark preflight must include its executed plan");
    assert!(
        graph_row_explain_has_node(
            &explain.plan,
            "GraphRowPlanAlternative",
            &["chosen; kind=AdjacencyPull"]
        ),
        "ordered alternative must win the capability preflight: {explain:?}"
    );
    let mut runtime = Vec::new();
    graph_row_explain_details(&explain.plan, "ChunkedRowProductionRuntime", &mut runtime);
    assert_eq!(runtime.len(), 1, "expected one adjacency runtime line");
    let detail = runtime[0];
    for fragment in [
        "source=AdjacencyPull{edge=alias:edge}",
        "eligibility=eligible",
        "early_exit=true",
        "source_order=logical_from_group_asc",
        "proof_boundary=completed_owner_group",
    ] {
        assert!(detail.contains(fragment), "missing {fragment:?}: {detail}");
    }
    assert_eq!(detail.contains("cursor_seek=anchor_ge:"), cursor_page);
    let expected_rows = u64::try_from(expected.len()).unwrap();
    let completed_owner_cap = if expected_rows <= 64 {
        64
    } else {
        expected_rows.saturating_add(2)
    };
    let raw_posting_cap = completed_owner_cap;
    let physical_cap = if expected_rows <= 64 {
        64
    } else {
        expected_rows.saturating_mul(2).saturating_add(64)
    };
    for (name, cap) in [
        ("completed_owner_groups", completed_owner_cap),
        ("physical_scan_units", physical_cap),
        ("raw_adjacency_postings_scanned", raw_posting_cap),
    ] {
        let value = detail
            .split("; ")
            .find_map(|field| field.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("missing {name} from {detail}"))
            .parse::<u64>()
            .unwrap();
        assert!(value <= cap, "{name}={value} exceeds capability cap {cap}");
    }
    eprintln!("Phase 44b AdjacencyPull capability: {detail}");
    result
}

fn phase44b_residual_oracle(
    rows: &[GraphRowChunkedOracleRow],
    first_node_id: u64,
) -> Vec<(u64, u64, u64)> {
    let survivors = rows
        .iter()
        .copied()
        .filter(|row| (row.target_id - first_node_id).is_multiple_of(10))
        .collect::<Vec<_>>();
    // Node IDs begin at one, while fixture ordinals begin at zero. The target of source
    // ordinal i is i+1 modulo N, so target IDs divisible by ten are exactly ten percent.
    assert_eq!(survivors.len(), PHASE44B_DISTRIBUTED_EDGE_COUNT / 10);
    graph_row_chunked_oracle_rows(&survivors)
}

fn build_phase44b_sparse_fixture() -> Phase44bOrderedFixture {
    let dir = tempfile::tempdir().unwrap();
    let options = phase44b_options(0);
    let mut engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    engine.ensure_node_label("Phase44bNode").unwrap();
    engine.ensure_edge_label(PHASE44B_EDGE_LABEL).unwrap();
    engine
        .ensure_edge_label(PHASE44B_SPARSE_OTHER_LABEL)
        .unwrap();
    let node_ids = engine
        .batch_upsert_nodes(
            (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
                .map(|ordinal| phase44b_node("phase44b-sparse", ordinal, false))
                .collect(),
        )
        .unwrap();
    engine.flush().unwrap();
    engine.close().unwrap();
    engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    let edge_ids = engine
        .batch_upsert_edges(
            (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
                .map(|ordinal| {
                    phase44b_edge_input(
                        node_ids[ordinal],
                        node_ids[(ordinal + 1) % node_ids.len()],
                        if ordinal.is_multiple_of(PHASE44B_SPARSE_STRIDE) {
                            PHASE44B_EDGE_LABEL
                        } else {
                            PHASE44B_SPARSE_OTHER_LABEL
                        },
                        (ordinal % 251) as u8,
                    )
                })
                .collect(),
        )
        .unwrap();
    let mut rows = (0..PHASE44B_DISTRIBUTED_EDGE_COUNT)
        .filter(|ordinal| ordinal.is_multiple_of(PHASE44B_SPARSE_STRIDE))
        .map(|ordinal| GraphRowChunkedOracleRow {
            source_id: node_ids[ordinal],
            edge_id: edge_ids[ordinal],
            target_id: node_ids[(ordinal + 1) % node_ids.len()],
            score: 0,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    assert_eq!(rows.len(), 200);
    assert!(rows.len() * 1_000 <= PHASE44B_DISTRIBUTED_EDGE_COUNT);
    flush_and_reopen_phase44b_fixture(Phase44bOrderedFixture {
        _dir: dir,
        options,
        engine,
        node_ids,
        rows,
    })
}

fn build_phase44b_multi_label_both_fixture() -> Phase44bOrderedFixture {
    let dir = tempfile::tempdir().unwrap();
    let options = phase44b_options(0);
    let engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    for label in ["Phase44bBothA", "Phase44bBothB", "Phase44bBothIgnored"] {
        engine.ensure_edge_label(label).unwrap();
    }
    let node_ids = engine
        .batch_upsert_nodes(
            (0..16)
                .map(|ordinal| phase44b_node("phase44b-both", ordinal, false))
                .collect(),
        )
        .unwrap();
    let initial = vec![
        phase44b_edge_input(node_ids[0], node_ids[1], "Phase44bBothA", 1),
        phase44b_edge_input(node_ids[2], node_ids[2], "Phase44bBothA", 2),
        phase44b_edge_input(node_ids[3], node_ids[4], "Phase44bBothB", 3),
        phase44b_edge_input(node_ids[5], node_ids[6], "Phase44bBothIgnored", 4),
    ];
    let initial_ids = engine.batch_upsert_edges(initial).unwrap();
    let patch = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![
                phase44b_edge_input(node_ids[0], node_ids[7], "Phase44bBothA", 5),
                phase44b_edge_input(node_ids[3], node_ids[4], "Phase44bBothA", 6),
            ],
            delete_edge_ids: vec![initial_ids[0], initial_ids[2]],
            ..GraphPatch::default()
        })
        .unwrap();
    assert_eq!(patch.edge_ids.len(), 2);
    let live = [
        (node_ids[2], initial_ids[1], node_ids[2]),
        (node_ids[0], patch.edge_ids[0], node_ids[7]),
        (node_ids[3], patch.edge_ids[1], node_ids[4]),
    ];
    let mut rows = Vec::new();
    for (from, edge, to) in live {
        rows.push(GraphRowChunkedOracleRow {
            source_id: from,
            edge_id: edge,
            target_id: to,
            score: 0,
        });
        if from != to {
            rows.push(GraphRowChunkedOracleRow {
                source_id: to,
                edge_id: edge,
                target_id: from,
                score: 0,
            });
        }
    }
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter()
            .filter(|row| row.source_id == row.target_id)
            .count(),
        1
    );
    flush_and_reopen_phase44b_fixture(Phase44bOrderedFixture {
        _dir: dir,
        options,
        engine,
        node_ids,
        rows,
    })
}

fn build_phase44b_hub_fixture() -> Phase44bOrderedFixture {
    let node_count = PHASE44B_HUB_EDGE_COUNT + 1;
    let dir = tempfile::tempdir().unwrap();
    let options = phase44b_options(0);
    let engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    engine.ensure_edge_label("Phase44bHub").unwrap();
    let node_ids = engine
        .batch_upsert_nodes(
            (0..node_count)
                .map(|ordinal| phase44b_node("phase44b-hub", ordinal, false))
                .collect(),
        )
        .unwrap();
    let mut inputs = Vec::with_capacity(PHASE44B_HUB_EDGE_COUNT + PHASE44B_HUB_FOLLOWER_EDGE_COUNT);
    for target in 1..=PHASE44B_HUB_EDGE_COUNT {
        inputs.push(phase44b_edge_input(
            node_ids[0],
            node_ids[target],
            "Phase44bHub",
            (target % 251) as u8,
        ));
    }
    for target in 2..2 + PHASE44B_HUB_FOLLOWER_EDGE_COUNT {
        inputs.push(phase44b_edge_input(
            node_ids[1],
            node_ids[target],
            "Phase44bHub",
            (target % 251) as u8,
        ));
    }
    let edge_ids = engine.batch_upsert_edges(inputs).unwrap();
    let mut rows = Vec::with_capacity(edge_ids.len());
    for target in 1..=PHASE44B_HUB_EDGE_COUNT {
        rows.push(GraphRowChunkedOracleRow {
            source_id: node_ids[0],
            edge_id: edge_ids[target - 1],
            target_id: node_ids[target],
            score: 0,
        });
    }
    for (offset, target) in (2..2 + PHASE44B_HUB_FOLLOWER_EDGE_COUNT).enumerate() {
        rows.push(GraphRowChunkedOracleRow {
            source_id: node_ids[1],
            edge_id: edge_ids[PHASE44B_HUB_EDGE_COUNT + offset],
            target_id: node_ids[target],
            score: 0,
        });
    }
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));
    assert_eq!(
        rows.iter()
            .take_while(|row| row.source_id == node_ids[0])
            .count(),
        PHASE44B_HUB_EDGE_COUNT
    );
    flush_and_reopen_phase44b_fixture(Phase44bOrderedFixture {
        _dir: dir,
        options,
        engine,
        node_ids,
        rows,
    })
}

fn build_phase44b_mixed_layer_fixture() -> Phase44bOrderedFixture {
    let dir = tempfile::tempdir().unwrap();
    let options = phase44b_options(1024 * 1024);
    let mut engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    for label in [
        "Phase44bMixed",
        "Phase44bMixedOld",
        "Phase44bMixedFillerA",
        "Phase44bMixedFillerB",
    ] {
        engine.ensure_edge_label(label).unwrap();
    }
    let node_ids = engine
        .batch_upsert_nodes(
            (0..5_000)
                .map(|ordinal| phase44b_node("phase44b-mixed", ordinal, false))
                .collect(),
        )
        .unwrap();
    engine.flush().unwrap();
    let first = engine
        .batch_upsert_edges(
            (0..20)
                .map(|ordinal| {
                    phase44b_edge_input(
                        node_ids[ordinal],
                        node_ids[ordinal + 20],
                        "Phase44bMixedOld",
                        ordinal as u8,
                    )
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(first.len(), 20);
    let patch = engine
        .graph_patch(GraphPatch {
            upsert_edges: (0..20)
                .map(|ordinal| {
                    phase44b_edge_input(
                        node_ids[ordinal],
                        node_ids[ordinal + 40],
                        "Phase44bMixed",
                        (ordinal + 100) as u8,
                    )
                })
                .collect(),
            delete_edge_ids: first,
            ..GraphPatch::default()
        })
        .unwrap();
    assert_eq!(patch.edge_ids.len(), 20);
    let mut rows = patch
        .edge_ids
        .iter()
        .enumerate()
        .map(|(ordinal, &edge_id)| GraphRowChunkedOracleRow {
            source_id: node_ids[ordinal],
            edge_id,
            target_id: node_ids[ordinal + 40],
            score: 0,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| (row.source_id, row.target_id, row.edge_id));

    let mut query = phase44b_graph_row_query(
        &["Phase44bMixed"],
        Direction::Outgoing,
        false,
        PHASE44B_PAGE_LIMIT,
    );
    let active = engine.query_graph_rows(&query).unwrap();
    query.at_epoch = Some(active.stats.effective_at_epoch);
    let active = engine.query_graph_rows(&query).unwrap();
    assert_eq!(
        graph_row_chunked_result_rows(&active),
        graph_row_chunked_oracle_rows(&rows[..PHASE44B_PAGE_LIMIT])
    );
    assert!(active.next_cursor.is_some());
    let assert_same_page = |engine: &DatabaseEngine, state: &str| {
        let result = engine.query_graph_rows(&query).unwrap();
        assert_eq!(result.rows, active.rows, "mixed-layer {state} rows/order");
        assert_eq!(
            result.next_cursor, active.next_cursor,
            "mixed-layer {state} cursor bytes"
        );
        assert_eq!(
            result.stats.effective_at_epoch, active.stats.effective_at_epoch,
            "mixed-layer {state} epoch"
        );
        eprintln!(
            "graph_row_ordered_edge_anchor/mixed_layer_moves_limit10: state={state}; rows={}; cursor={:?}; epoch={}",
            result.rows.len(), result.next_cursor, result.stats.effective_at_epoch
        );
    };
    assert_same_page(&engine, "active");

    let filler = |label: &str| {
        (0..2_000)
            .map(|ordinal| {
                let mut input = phase44b_edge_input(
                    node_ids[100 + ordinal],
                    node_ids[3_000 + ordinal],
                    label,
                    (ordinal % 251) as u8,
                );
                input.props.insert(
                    "phase44b_mixed_filler".to_string(),
                    PropValue::Bytes(vec![(ordinal % 251) as u8; 2_048]),
                );
                input
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        engine
            .batch_upsert_edges(filler("Phase44bMixedFillerA"))
            .unwrap()
            .len(),
        2_000
    );
    let immutable = engine.stats().unwrap();
    assert!(
        immutable.immutable_memtable_count > 0 || immutable.pending_flush_count > 0,
        "mixed-layer fixture failed to retain its threshold-frozen immutable epoch"
    );
    assert_same_page(&engine, "immutable");

    engine.flush().unwrap();
    assert_same_page(&engine, "flushed");
    assert_eq!(
        engine
            .batch_upsert_edges(filler("Phase44bMixedFillerB"))
            .unwrap()
            .len(),
        2_000
    );
    engine.flush().unwrap();
    assert!(engine.compact().unwrap().is_some());
    assert_same_page(&engine, "compacted");
    engine.close().unwrap();
    engine = DatabaseEngine::open(dir.path(), &options).unwrap();
    assert_same_page(&engine, "reopened");

    Phase44bOrderedFixture {
        _dir: dir,
        options,
        engine,
        node_ids,
        rows,
    }
}

fn phase44b_hub_straddle_query(
    fixture: &Phase44bOrderedFixture,
) -> (overgraph::GraphRowQuery, Vec<(u64, u64, u64)>) {
    let mut walk = phase44b_graph_row_query(
        &["Phase44bHub"],
        Direction::Outgoing,
        false,
        PHASE44B_HUB_PAGE_LIMIT,
    );
    let first = fixture.engine.query_graph_rows(&walk).unwrap();
    let epoch = first.stats.effective_at_epoch;
    walk.at_epoch = Some(epoch);
    let mut cursor = None;
    for page_index in 0..10 {
        walk.page.cursor = cursor;
        let page = fixture.engine.query_graph_rows(&walk).unwrap();
        let start = page_index * PHASE44B_HUB_PAGE_LIMIT;
        let end = start + PHASE44B_HUB_PAGE_LIMIT;
        assert_eq!(
            graph_row_chunked_result_rows(&page),
            graph_row_chunked_oracle_rows(&fixture.rows[start..end])
        );
        assert!(page
            .rows
            .iter()
            .all(|row| matches!(row.values[0], overgraph::GraphValue::NodeId(id) if id == fixture.node_ids[0])));
        cursor = page.next_cursor;
    }
    let mut query = phase44b_graph_row_query(
        &["Phase44bHub"],
        Direction::Outgoing,
        false,
        PHASE44B_HUB_PAGE_LIMIT,
    );
    query.at_epoch = Some(epoch);
    query.page.cursor = cursor;
    let start = 10 * PHASE44B_HUB_PAGE_LIMIT;
    let end = start + PHASE44B_HUB_PAGE_LIMIT;
    let expected = graph_row_chunked_oracle_rows(&fixture.rows[start..end]);
    let crossing = fixture.engine.query_graph_rows(&query).unwrap();
    assert_eq!(graph_row_chunked_result_rows(&crossing), expected);
    assert_eq!(
        expected
            .iter()
            .take_while(|row| row.0 == fixture.node_ids[0])
            .count(),
        5
    );
    assert!(expected
        .iter()
        .skip(5)
        .all(|row| row.0 == fixture.node_ids[1]));
    let repeat = fixture.engine.query_graph_rows(&query).unwrap();
    assert_eq!(repeat.rows, crossing.rows);
    assert_eq!(repeat.next_cursor, crossing.next_cursor);
    (query, expected)
}

struct Phase44bWriteFixture {
    _dir: tempfile::TempDir,
    engine: DatabaseEngine,
    node_ids: Vec<u64>,
    live_churn_ids: BTreeMap<u64, u64>,
}

fn phase44b_write_edge_input(
    node_ids: &[u64],
    shape: phase44b_write_workload::EdgeShape,
    churn: bool,
) -> EdgeInput {
    let label = if churn {
        phase44b_write_workload::CHURN_LABELS[shape.label_index]
    } else {
        assert_eq!(shape.label_index, 0);
        phase44b_write_workload::UPSERT_LABEL
    };
    let mut props = BTreeMap::new();
    props.insert(
        phase44b_write_workload::EDGE_PAYLOAD_KEY.to_string(),
        PropValue::Bytes(vec![
            shape.payload_byte;
            phase44b_write_workload::EDGE_PAYLOAD_LEN
        ]),
    );
    EdgeInput {
        from: node_ids[shape.from_node_index],
        to: node_ids[shape.to_node_index],
        label: label.to_string(),
        props,
        weight: 1.0,
        valid_from: None,
        valid_to: None,
    }
}

fn build_phase44b_write_fixture(churn: bool) -> Phase44bWriteFixture {
    let dir = tempfile::tempdir().unwrap();
    let preload_options = DbOptions {
        create_if_missing: true,
        memtable_flush_threshold: 0,
        memtable_hard_cap_bytes: 0,
        max_immutable_memtables: 0,
        compact_after_n_flushes: 0,
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: false,
        ..DbOptions::default()
    };
    let preload = DatabaseEngine::open(dir.path(), &preload_options).unwrap();
    preload
        .ensure_edge_label(phase44b_write_workload::UPSERT_LABEL)
        .unwrap();
    for label in phase44b_write_workload::CHURN_LABELS {
        preload.ensure_edge_label(label).unwrap();
    }
    let node_ids = preload
        .batch_upsert_nodes(
            (0..phase44b_write_workload::ENDPOINT_NODE_COUNT)
                .map(|ordinal| NodeInput {
                    labels: vec![phase44b_write_workload::ENDPOINT_NODE_LABEL.to_string()],
                    key: format!(
                        "{}-{ordinal}",
                        phase44b_write_workload::ENDPOINT_NODE_KEY_PREFIX
                    ),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(node_ids.len(), phase44b_write_workload::ENDPOINT_NODE_COUNT);
    preload.close().unwrap();

    let engine = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            create_if_missing: true,
            memtable_flush_threshold: 1024 * 1024,
            memtable_hard_cap_bytes: 4 * 1024 * 1024,
            max_immutable_memtables: 4,
            compact_after_n_flushes: 4,
            wal_sync_mode: WalSyncMode::Immediate,
            edge_uniqueness: false,
            ..DbOptions::default()
        },
    )
    .unwrap();
    let stats = engine.stats().unwrap();
    assert_eq!(stats.immutable_memtable_count, 0);
    assert_eq!(stats.pending_flush_count, 0);
    assert_eq!(stats.active_memtable_bytes, 0);

    let mut live_churn_ids = BTreeMap::new();
    if churn {
        let mut seed_shapes = vec![phase44b_write_workload::churn_sentinel_edge()];
        seed_shapes.extend(phase44b_write_workload::churn_initial_transient_edges());
        let seed_inputs = seed_shapes
            .iter()
            .copied()
            .map(|shape| phase44b_write_edge_input(&node_ids, shape, true))
            .collect();
        let seed_ids = engine.batch_upsert_edges(seed_inputs).unwrap();
        assert_eq!(seed_ids.len(), seed_shapes.len());
        for (shape, edge_id) in seed_shapes.into_iter().zip(seed_ids) {
            assert!(live_churn_ids.insert(shape.ordinal, edge_id).is_none());
        }
        assert!(live_churn_ids.contains_key(&phase44b_write_workload::CHURN_SENTINEL_ORDINAL));
    }

    Phase44bWriteFixture {
        _dir: dir,
        engine,
        node_ids,
        live_churn_ids,
    }
}

fn run_phase44b_upsert_workload(fixture: &Phase44bWriteFixture) -> Duration {
    let mut mutation_count = 0usize;
    let mut measured = Duration::ZERO;
    for call_index in 0..phase44b_write_workload::WRITE_CALL_COUNT {
        let shapes = phase44b_write_workload::upsert_batch(call_index);
        assert_eq!(shapes.len(), phase44b_write_workload::WRITE_BATCH_SIZE);
        let inputs = shapes
            .into_iter()
            .map(|shape| phase44b_write_edge_input(&fixture.node_ids, shape, false))
            .collect();
        let started = Instant::now();
        let result = fixture.engine.batch_upsert_edges(inputs);
        measured = measured.checked_add(started.elapsed()).unwrap();
        let ids = result.unwrap();
        assert_eq!(ids.len(), phase44b_write_workload::WRITE_BATCH_SIZE);
        mutation_count += ids.len();
    }
    assert_eq!(
        mutation_count,
        phase44b_write_workload::TOTAL_MUTATION_COUNT
    );
    measured
}

fn run_phase44b_churn_workload(fixture: &mut Phase44bWriteFixture) -> Duration {
    let mut mutation_count = 0usize;
    let mut measured = Duration::ZERO;
    for call_index in 0..phase44b_write_workload::WRITE_CALL_COUNT {
        let shape = phase44b_write_workload::churn_batch(call_index);
        let delete_edge_ids = shape
            .delete_ordinals
            .iter()
            .map(|ordinal| {
                fixture
                    .live_churn_ids
                    .remove(ordinal)
                    .expect("churn delete ordinal must name a live transient edge")
            })
            .collect::<Vec<_>>();
        let insert_shapes = shape.insert_edges;
        mutation_count += delete_edge_ids.len() + insert_shapes.len();
        let patch = GraphPatch {
            upsert_edges: insert_shapes
                .iter()
                .copied()
                .map(|edge| phase44b_write_edge_input(&fixture.node_ids, edge, true))
                .collect(),
            delete_edge_ids,
            ..GraphPatch::default()
        };
        let started = Instant::now();
        let result = fixture.engine.graph_patch(patch);
        measured = measured.checked_add(started.elapsed()).unwrap();
        let inserted = result.unwrap().edge_ids;
        assert_eq!(
            inserted.len(),
            phase44b_write_workload::CHURN_TRANSIENTS_PER_CALL
        );
        for (edge, edge_id) in insert_shapes.into_iter().zip(inserted) {
            assert!(fixture
                .live_churn_ids
                .insert(edge.ordinal, edge_id)
                .is_none());
        }
    }
    assert_eq!(
        mutation_count,
        phase44b_write_workload::TOTAL_MUTATION_COUNT
    );
    assert!(fixture
        .live_churn_ids
        .contains_key(&phase44b_write_workload::CHURN_SENTINEL_ORDINAL));
    assert_eq!(
        fixture.live_churn_ids.len(),
        phase44b_write_workload::CHURN_TRANSIENTS_PER_CALL + 1
    );
    measured
}

fn finish_phase44b_write_fixture(fixture: Phase44bWriteFixture) {
    fixture.engine.flush().unwrap();
    let stats = fixture.engine.stats().unwrap();
    assert_eq!(stats.immutable_memtable_count, 0);
    assert_eq!(stats.pending_flush_count, 0);
    fixture.engine.close().unwrap();
}

fn bench_graph_row_ordered_edge_anchor(c: &mut Criterion) {
    const GROUP: &str = "graph_row_ordered_edge_anchor";
    const SCENARIOS: [&str; 12] = [
        "distributed_limit10_active",
        "distributed_limit10_flushed",
        "distributed_deep_cursor_flushed",
        "residual_limit10_flushed",
        "multi_label_both_limit10_flushed",
        "sparse_label_limit10_flushed",
        "low_id_hub_first_page_flushed",
        "low_id_hub_cursor_straddle_flushed",
        "mixed_layer_moves_limit10",
        "gql_distributed_limit10_flushed",
        "memtable_group_twin_upsert",
        "memtable_group_twin_churn",
    ];
    let requested = SCENARIOS.map(|scenario| criterion_requested_benchmark(GROUP, scenario));
    if !requested.iter().any(|requested| *requested) {
        return;
    }

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(10);

    let needs_distributed =
        requested[0] || requested[1] || requested[2] || requested[3] || requested[9];
    let mut distributed = needs_distributed.then(build_phase44b_distributed_fixture);
    if requested[0] {
        let fixture = distributed.as_ref().unwrap();
        let query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        let first = assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert_phase44b_edgepull(&fixture.engine, &query, &oracle[..PHASE44B_PAGE_LIMIT]);
        eprintln!(
            "{GROUP}/{}: state=active; rows={}; cursor={:?}; epoch={}",
            SCENARIOS[0],
            first.rows.len(),
            first.next_cursor,
            first.stats.effective_at_epoch
        );
        group.bench_function(SCENARIOS[0], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[1] || requested[2] || requested[3] || requested[9] {
        distributed = Some(flush_and_reopen_phase44b_fixture(
            distributed.take().unwrap(),
        ));
    }
    if requested[1] {
        let fixture = distributed.as_ref().unwrap();
        let query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        let first = assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert_phase44b_adjacency_pull(
            &fixture.engine,
            &query,
            &oracle[..PHASE44B_PAGE_LIMIT],
            false,
        );
        eprintln!(
            "{GROUP}/{}: state=flushed; rows={}; cursor={:?}; epoch={}",
            SCENARIOS[1],
            first.rows.len(),
            first.next_cursor,
            first.stats.effective_at_epoch
        );
        group.bench_function(SCENARIOS[1], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[2] {
        let fixture = distributed.as_ref().unwrap();
        let mut walk = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_DEEP_PAGE_LIMIT,
        );
        let initial = fixture.engine.query_graph_rows(&walk).unwrap();
        let epoch = initial.stats.effective_at_epoch;
        walk.at_epoch = Some(epoch);
        let mut cursor = None;
        for page_index in 0..PHASE44B_DEEP_PAGE_COUNT {
            walk.page.cursor = cursor;
            let page = fixture.engine.query_graph_rows(&walk).unwrap();
            let start = page_index * PHASE44B_DEEP_PAGE_LIMIT;
            let end = start + PHASE44B_DEEP_PAGE_LIMIT;
            assert_eq!(
                graph_row_chunked_result_rows(&page),
                graph_row_chunked_oracle_rows(&fixture.rows[start..end])
            );
            cursor = page.next_cursor;
            assert!(cursor.is_some());
        }
        let expected_start = PHASE44B_DEEP_PAGE_COUNT * PHASE44B_DEEP_PAGE_LIMIT;
        let expected_end = expected_start + PHASE44B_DEEP_PAGE_LIMIT;
        let expected = graph_row_chunked_oracle_rows(&fixture.rows[expected_start..expected_end]);
        let mut query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_DEEP_PAGE_LIMIT,
        );
        query.at_epoch = Some(epoch);
        query.page.cursor = cursor;
        let page = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(graph_row_chunked_result_rows(&page), expected);
        let repeated = fixture.engine.query_graph_rows(&query).unwrap();
        assert_eq!(repeated.rows, page.rows);
        assert_eq!(repeated.next_cursor, page.next_cursor);
        assert_phase44b_adjacency_pull(&fixture.engine, &query, &expected, true);
        eprintln!(
            "{GROUP}/{}: walked_pages={}; suffix_start={}; cursor_seek=inclusive_owner",
            SCENARIOS[2], PHASE44B_DEEP_PAGE_COUNT, expected_start
        );
        group.bench_function(SCENARIOS[2], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[3] {
        let fixture = distributed.as_ref().unwrap();
        let query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            true,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = phase44b_residual_oracle(&fixture.rows, fixture.node_ids[0]);
        assert_eq!(oracle.len(), PHASE44B_DISTRIBUTED_EDGE_COUNT / 10);
        assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert_phase44b_residual_source(&fixture.engine, &query, &oracle[..PHASE44B_PAGE_LIMIT]);
        group.bench_function(SCENARIOS[3], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[4] {
        let fixture = build_phase44b_multi_label_both_fixture();
        let query = phase44b_graph_row_query(
            &["Phase44bBothA", "Phase44bBothB"],
            Direction::Both,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        let result = assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert!(result.next_cursor.is_none());
        assert_phase44b_edgepull(&fixture.engine, &query, &oracle);
        group.bench_function(SCENARIOS[4], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[5] {
        let fixture = build_phase44b_sparse_fixture();
        let query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        assert_eq!(oracle.len(), 200);
        assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert_phase44b_edgepull(&fixture.engine, &query, &oracle[..PHASE44B_PAGE_LIMIT]);
        group.bench_function(SCENARIOS[5], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[6] || requested[7] {
        let fixture = build_phase44b_hub_fixture();
        if requested[6] {
            let query = phase44b_graph_row_query(
                &["Phase44bHub"],
                Direction::Outgoing,
                false,
                PHASE44B_PAGE_LIMIT,
            );
            let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
            assert_phase44b_page(&fixture.engine, &query, &oracle);
            assert_phase44b_edgepull(&fixture.engine, &query, &oracle[..PHASE44B_PAGE_LIMIT]);
            group.bench_function(SCENARIOS[6], |b| {
                b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
            });
        }
        if requested[7] {
            let (query, expected) = phase44b_hub_straddle_query(&fixture);
            assert_phase44b_edgepull(&fixture.engine, &query, &expected);
            group.bench_function(SCENARIOS[7], |b| {
                b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
            });
        }
    }

    if requested[8] {
        let fixture = build_phase44b_mixed_layer_fixture();
        let query = phase44b_graph_row_query(
            &["Phase44bMixed"],
            Direction::Outgoing,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        let oracle = graph_row_chunked_oracle_rows(&fixture.rows);
        assert_phase44b_page(&fixture.engine, &query, &oracle);
        assert_phase44b_edgepull(&fixture.engine, &query, &oracle[..PHASE44B_PAGE_LIMIT]);
        group.bench_function(SCENARIOS[8], |b| {
            b.iter(|| black_box(fixture.engine.query_graph_rows(black_box(&query)).unwrap()))
        });
    }

    if requested[9] {
        let fixture = distributed.as_ref().unwrap();
        let expected = graph_row_chunked_oracle_rows(&fixture.rows[..PHASE44B_PAGE_LIMIT]);
        let native_query = phase44b_graph_row_query(
            &[PHASE44B_EDGE_LABEL],
            Direction::Outgoing,
            false,
            PHASE44B_PAGE_LIMIT,
        );
        assert_phase44b_adjacency_pull(&fixture.engine, &native_query, &expected, false);
        let query = "MATCH (source)-[edge:Phase44bDistributed]->(target) RETURN id(source), id(edge), id(target) LIMIT 10";
        let params = GqlParams::new();
        let options = GqlExecutionOptions {
            allow_full_scan: false,
            max_frontier: PHASE44B_DISTRIBUTED_EDGE_COUNT,
            max_intermediate_bindings: PHASE44B_DISTRIBUTED_EDGE_COUNT,
            include_plan: false,
            ..GqlExecutionOptions::default()
        };
        let preflight = fixture
            .engine
            .execute_gql(
                query,
                &params,
                &GqlExecutionOptions {
                    include_plan: true,
                    ..options.clone()
                },
            )
            .unwrap();
        assert_eq!(graph_row_chunked_gql_rows(&preflight), expected);
        assert!(preflight.next_cursor.is_none());
        let projection = preflight
            .plan
            .as_ref()
            .and_then(|plan| plan.read.as_ref())
            .expect("Phase 44b GQL preflight must include its read plan")
            .projection
            .join("\n");
        for fragment in [
            "source=AdjacencyPull{edge=alias:edge}",
            "eligibility=eligible",
            "early_exit=true",
            "cursor_seek=none",
            "source_order=logical_from_group_asc",
            "proof_boundary=completed_owner_group",
        ] {
            assert!(
                projection.contains(fragment),
                "missing {fragment:?}: {projection}"
            );
        }
        let repeated = fixture
            .engine
            .execute_gql(query, &params, &options)
            .unwrap();
        assert_eq!(repeated.rows, preflight.rows);
        assert_eq!(repeated.next_cursor, preflight.next_cursor);
        group.bench_function(SCENARIOS[9], |b| {
            b.iter(|| {
                black_box(
                    fixture
                        .engine
                        .execute_gql(black_box(query), black_box(&params), black_box(&options))
                        .unwrap(),
                )
            })
        });
    }

    if requested[10] {
        group.bench_function(SCENARIOS[10], |b| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    let fixture = build_phase44b_write_fixture(false);
                    measured = measured
                        .checked_add(run_phase44b_upsert_workload(&fixture))
                        .unwrap();
                    finish_phase44b_write_fixture(fixture);
                }
                measured
            })
        });
    }

    if requested[11] {
        group.bench_function(SCENARIOS[11], |b| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    let mut fixture = build_phase44b_write_fixture(true);
                    measured = measured
                        .checked_add(run_phase44b_churn_workload(&mut fixture))
                        .unwrap();
                    finish_phase44b_write_fixture(fixture);
                }
                measured
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_node_queries,
    bench_streamed_node_queries,
    bench_streamed_edge_queries,
    bench_edge_queries,
    bench_compound_index_queries,
    bench_graph_row_streamed,
    bench_graph_row_chunked,
    bench_graph_row_edgepull,
    bench_graph_row_ordered_edge_anchor,
    bench_graph_row_phase43b,
    bench_gql_queries
);
criterion_main!(benches);
