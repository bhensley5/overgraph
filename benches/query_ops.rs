use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeFilterExpr, EdgeInput, EdgeQuery,
    GqlExecutionOptions, GqlParamValue, GqlParams, GqlStatementKind, GraphBinaryOp,
    GraphEdgePattern, GraphExpr, GraphNodeField, GraphNodePattern, GraphOrderDirection,
    GraphOrderItem, GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPatternPiece,
    GraphQueryOptions, GraphReturnItem, GraphReturnProjection, GraphVariableLengthPattern,
    LabelMatchMode, NodeFilterExpr, NodeInput, NodeLabelFilter, NodeQuery, PageRequest, PropValue,
    PropertyRangeBound, QueryPlanExecutionMode, QueryPlanNode, QueryPlanWarning,
    SecondaryIndexField, SecondaryIndexSpec, SecondaryIndexState,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
                "source=DelegatedEdgeQuery",
                "planned_modes=eager",
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
                    "DelegatedEdgeQuery",
                    "EdgePropertyEqualityIndex",
                    "planned_modes=streamed",
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
                    "DelegatedEdgeQuery",
                    "planned_modes=eager",
                    "planned_warnings=[",
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
                    &["source=DelegatedEdgeQuery", "planned_modes=eager"],
                );
                eprintln!("{GROUP}/{FO033}: class=comparable-prephase-success");
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

criterion_group!(
    benches,
    bench_node_queries,
    bench_streamed_node_queries,
    bench_streamed_edge_queries,
    bench_edge_queries,
    bench_compound_index_queries,
    bench_graph_row_streamed,
    bench_gql_queries
);
criterion_main!(benches);
