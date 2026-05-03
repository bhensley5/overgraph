// Phase 23 CP1 planner tests: validation, scan-backed node queries, and explain.

// --- validation and oracle helpers ---

fn query_test_props(entries: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn insert_query_node(
    engine: &DatabaseEngine,
    type_id: u32,
    key: &str,
    entries: &[(&str, PropValue)],
    weight: f32,
) -> u64 {
    engine
        .upsert_node(
            type_id,
            key,
            UpsertNodeOptions {
                props: query_test_props(entries),
                weight,
                ..Default::default()
            },
        )
        .unwrap()
}

fn query_ids(
    type_id: Option<u32>,
    filter_exprs: Vec<NodeFilterExpr>,
    allow_full_scan: bool,
) -> NodeQuery {
    NodeQuery {
        type_id,
        filter: filter_from_conjunction(filter_exprs),
        allow_full_scan,
        ..Default::default()
    }
}

fn filter_from_conjunction(filter_exprs: Vec<NodeFilterExpr>) -> Option<NodeFilterExpr> {
    match filter_exprs.len() {
        0 => None,
        1 => filter_exprs.into_iter().next(),
        _ => Some(NodeFilterExpr::And(filter_exprs)),
    }
}

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

fn pattern_node(
    alias: &str,
    type_id: Option<u32>,
    filter_exprs: Vec<NodeFilterExpr>,
) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        type_id,
        ids: Vec::new(),
        keys: Vec::new(),
        filter: filter_from_conjunction(filter_exprs),
    }
}

fn pattern_node_with_ids(alias: &str, ids: Vec<u64>) -> NodePattern {
    NodePattern {
        alias: alias.to_string(),
        type_id: None,
        ids,
        keys: Vec::new(),
        filter: None,
    }
}

fn pattern_edge(
    alias: Option<&str>,
    from_alias: &str,
    to_alias: &str,
    direction: Direction,
    type_filter: Option<Vec<u32>>,
) -> EdgePattern {
    EdgePattern {
        alias: alias.map(str::to_string),
        from_alias: from_alias.to_string(),
        to_alias: to_alias.to_string(),
        direction,
        type_filter,
        property_predicates: Vec::new(),
    }
}

fn pattern_query(nodes: Vec<NodePattern>, edges: Vec<EdgePattern>) -> GraphPatternQuery {
    GraphPatternQuery {
        nodes,
        edges,
        at_epoch: None,
        limit: 100,
        order: PatternOrder::AnchorThenAliasesAsc,
    }
}

fn expected_match(nodes: &[(&str, u64)], edges: &[(&str, u64)]) -> QueryMatch {
    QueryMatch {
        nodes: nodes
            .iter()
            .map(|(alias, id)| ((*alias).to_string(), *id))
            .collect(),
        edges: edges
            .iter()
            .map(|(alias, id)| ((*alias).to_string(), *id))
            .collect(),
    }
}

fn oracle_node_matches(query: &NodeQuery, node: &NodeRecord) -> bool {
    if query.type_id.is_some_and(|type_id| node.type_id != type_id) {
        return false;
    }
    if !query.ids.is_empty() && !query.ids.contains(&node.id) {
        return false;
    }
    if !query.keys.is_empty() && !query.keys.contains(&node.key) {
        return false;
    }
    query
        .filter
        .as_ref()
        .is_none_or(|filter| oracle_filter_matches(filter, node))
}

fn oracle_filter_matches(filter: &NodeFilterExpr, node: &NodeRecord) -> bool {
    match filter {
        NodeFilterExpr::PropertyEquals { key, value } => {
            node.props.get(key).is_some_and(|candidate| candidate == value)
        }
        NodeFilterExpr::PropertyIn { key, values } => node
            .props
            .get(key)
            .is_some_and(|candidate| values.iter().any(|value| candidate == value)),
        NodeFilterExpr::PropertyRange { key, lower, upper } => {
            let Some(value) = node.props.get(key) else {
                return false;
            };
            let lower_matches = lower.as_ref().is_none_or(|bound| {
                let Some(ordering) = compare_range_values(value, bound.value()) else {
                    return false;
                };
                match bound {
                    PropertyRangeBound::Included(_) => ordering != std::cmp::Ordering::Less,
                    PropertyRangeBound::Excluded(_) => ordering == std::cmp::Ordering::Greater,
                }
            });
            let upper_matches = upper.as_ref().is_none_or(|bound| {
                let Some(ordering) = compare_range_values(value, bound.value()) else {
                    return false;
                };
                match bound {
                    PropertyRangeBound::Included(_) => ordering != std::cmp::Ordering::Greater,
                    PropertyRangeBound::Excluded(_) => ordering == std::cmp::Ordering::Less,
                }
            });
            lower_matches && upper_matches
        }
        NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            lower_ms.is_none_or(|lower| node.updated_at >= lower)
                && upper_ms.is_none_or(|upper| node.updated_at <= upper)
        }
        NodeFilterExpr::PropertyExists { key } => node.props.contains_key(key),
        NodeFilterExpr::PropertyMissing { key } => !node.props.contains_key(key),
        NodeFilterExpr::And(children) => {
            children.iter().all(|child| oracle_filter_matches(child, node))
        }
        NodeFilterExpr::Or(children) => {
            children.iter().any(|child| oracle_filter_matches(child, node))
        }
        NodeFilterExpr::Not(child) => !oracle_filter_matches(child, node),
    }
}

fn oracle_query_ids(engine: &DatabaseEngine, candidate_ids: &[u64], query: &NodeQuery) -> Vec<u64> {
    let mut ids = candidate_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    engine
        .get_nodes(&ids)
        .unwrap()
        .into_iter()
        .flatten()
        .filter(|node| oracle_node_matches(query, node))
        .map(|node| node.id)
        .collect()
}

fn set_query_node_updated_at(engine: &DatabaseEngine, node_id: u64, updated_at: i64) {
    let node = engine.get_node(node_id).unwrap().unwrap();
    engine
        .write_op(&WalOp::UpsertNode(NodeRecord {
            created_at: updated_at,
            updated_at,
            ..node
        }))
        .unwrap();
}

fn explain_input_node(plan: &QueryPlan) -> &QueryPlanNode {
    match &plan.root {
        QueryPlanNode::VerifyNodeFilter { input } => input.as_ref(),
        other => panic!("expected VerifyNodeFilter root, got {other:?}"),
    }
}

fn explain_input_nodes(plan: &QueryPlan) -> Vec<QueryPlanNode> {
    match explain_input_node(plan) {
        QueryPlanNode::Intersect { inputs } => inputs.clone(),
        node => vec![node.clone()],
    }
}

fn assert_plan_input_nodes(plan: &QueryPlan, expected: Vec<QueryPlanNode>) {
    assert_eq!(explain_input_nodes(plan), expected);
}

fn assert_plan_includes_input_nodes(plan: &QueryPlan, expected: &[QueryPlanNode]) {
    let mut actual = explain_input_nodes(plan);
    for expected_node in expected {
        let position = actual
            .iter()
            .position(|node| node == expected_node)
            .unwrap_or_else(|| panic!("expected plan to include {expected_node:?}; got {actual:?}"));
        actual.remove(position);
    }
}

fn pattern_anchor_plan_node(plan: &QueryPlan) -> (&str, &QueryPlanNode) {
    let pattern = match &plan.root {
        QueryPlanNode::PatternExpand { .. } => &plan.root,
        QueryPlanNode::VerifyEdgePredicates { input } => input.as_ref(),
        other => panic!("expected pattern expand root, got {other:?}"),
    };
    match pattern {
        QueryPlanNode::PatternExpand {
            anchor_alias,
            input,
        } => match input.as_ref() {
            QueryPlanNode::VerifyNodeFilter { input } => (anchor_alias.as_str(), input.as_ref()),
            other => panic!("expected VerifyNodeFilter pattern input, got {other:?}"),
        },
        other => panic!("expected pattern expand node, got {other:?}"),
    }
}

fn pattern_anchor_input_nodes(plan: &QueryPlan) -> Vec<QueryPlanNode> {
    match pattern_anchor_plan_node(plan).1 {
        QueryPlanNode::Intersect { inputs } => inputs.clone(),
        node => vec![node.clone()],
    }
}

fn planned_pattern_anchor_and_edge_aliases(
    engine: &DatabaseEngine,
    query: &GraphPatternQuery,
) -> (String, Vec<String>) {
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_pattern_query(query).unwrap();
    let planned = published
        .view
        .plan_normalized_pattern_query(&normalized)
        .unwrap();
    let aliases = planned
        .expansion_order
        .iter()
        .map(|&edge_index| {
            normalized.edges[edge_index]
                .alias
                .clone()
                .unwrap_or_else(|| format!("edge-{edge_index}"))
        })
        .collect();
    (normalized.nodes[planned.anchor_index].alias.clone(), aliases)
}

fn planned_pattern_anchor_sort_and_edge_aliases(
    engine: &DatabaseEngine,
    query: &GraphPatternQuery,
) -> (String, String, Vec<String>) {
    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let normalized = published.view.normalize_pattern_query(query).unwrap();
    let planned = published
        .view
        .plan_normalized_pattern_query(&normalized)
        .unwrap();
    let aliases = planned
        .expansion_order
        .iter()
        .map(|&edge_index| {
            normalized.edges[edge_index]
                .alias
                .clone()
                .unwrap_or_else(|| format!("edge-{edge_index}"))
        })
        .collect();
    (
        normalized.nodes[planned.anchor_index].alias.clone(),
        planned.sort_anchor_alias,
        aliases,
    )
}

fn plan_contains_fallback_full_node_scan(node: &QueryPlanNode) -> bool {
    match node {
        QueryPlanNode::FallbackFullNodeScan => true,
        QueryPlanNode::Intersect { inputs } | QueryPlanNode::Union { inputs } => {
            inputs.iter().any(plan_contains_fallback_full_node_scan)
        }
        QueryPlanNode::VerifyNodeFilter { input }
        | QueryPlanNode::VerifyEdgePredicates { input }
        | QueryPlanNode::PatternExpand { input, .. } => {
            plan_contains_fallback_full_node_scan(input)
        }
        _ => false,
    }
}

#[test]
fn test_planner_stats_view_rebuilds_only_with_read_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let initial = engine.planner_stats_view_for_test();
    assert_eq!(initial.generation, 1);
    assert_eq!(initial.segment_count, 0);

    insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let after_write = engine.planner_stats_view_for_test();
    assert!(std::sync::Arc::ptr_eq(&initial, &after_write));
    assert_eq!(after_write.generation, initial.generation);

    engine.flush().unwrap();
    let after_flush = engine.planner_stats_view_for_test();
    assert!(!std::sync::Arc::ptr_eq(&after_write, &after_flush));
    assert!(after_flush.generation > after_write.generation);
    assert_eq!(after_flush.segment_count, 1);
    assert_eq!(after_flush.available_segment_stats, 1);
    assert_eq!(after_flush.full_rollup.node_count, 1);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_stale_risk_uses_newer_sample_shadowing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..16 {
        insert_query_node(
            &engine,
            1,
            &format!("shadow-{index:02}"),
            &[("version", PropValue::Int(1))],
            1.0,
        );
    }
    engine.flush().unwrap();
    for index in 0..8 {
        insert_query_node(
            &engine,
            1,
            &format!("shadow-{index:02}"),
            &[("version", PropValue::Int(2))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    assert_eq!(
        stats_view.max_segment_stale_risk(),
        crate::planner_stats::StalePostingRisk::High
    );

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_stale_risk_uses_newer_tombstones() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut ids = Vec::new();
    for index in 0..16 {
        ids.push(insert_query_node(
            &engine,
            1,
            &format!("delete-{index:02}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();
    engine.delete_node(ids[0]).unwrap();
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    assert_eq!(
        stats_view.max_segment_stale_risk(),
        crate::planner_stats::StalePostingRisk::Medium
    );

    engine.close().unwrap();
}

#[test]
fn test_write_adjacent_helper_reads_do_not_rebuild_planner_stats_view() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let delete_a = insert_query_node(&engine, 1, "delete-a", &[], 1.0);
    let delete_b = insert_query_node(&engine, 1, "delete-b", &[], 1.0);
    let patch_c = insert_query_node(&engine, 2, "patch-c", &[], 1.0);
    let patch_d = insert_query_node(&engine, 2, "patch-d", &[], 1.0);
    let prune_e = insert_query_node(&engine, 90, "prune-e", &[], 0.1);
    let prune_f = insert_query_node(&engine, 91, "prune-f", &[], 1.0);
    engine
        .upsert_edge(delete_a, delete_b, 10, UpsertEdgeOptions::default())
        .unwrap();
    let patch_edge = engine
        .upsert_edge(patch_c, patch_d, 20, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(prune_e, prune_f, 30, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let stats_before = engine.planner_stats_view_for_test();
    let generation_before = stats_before.generation;
    let source_builds_before = engine.published_read_source_build_count_for_test();
    engine.reset_publish_counters_for_test();

    engine.delete_node(delete_a).unwrap();
    engine
        .graph_patch(&GraphPatch {
            invalidate_edges: vec![(patch_edge, 1)],
            delete_node_ids: vec![patch_c],
            ..Default::default()
        })
        .unwrap();
    let prune = engine
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.5),
            type_id: Some(90),
        })
        .unwrap();
    assert_eq!(prune.nodes_pruned, 1);
    assert_eq!(prune.edges_pruned, 1);

    let stats_after = engine.planner_stats_view_for_test();
    let counters = engine.publish_counter_snapshot_for_test();
    assert_eq!(counters.rebuild_sources, 0);
    assert_eq!(counters.source_rebuilds, 0);
    assert_eq!(
        engine.published_read_source_build_count_for_test(),
        source_builds_before
    );
    assert!(std::sync::Arc::ptr_eq(&stats_before, &stats_after));
    assert_eq!(stats_after.generation, generation_before);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_corruption_degrades_without_index_repair_followup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
        insert_query_node(
            &engine,
            1,
            "red",
            &[("status", PropValue::String("red".to_string()))],
            1.0,
        );
        insert_query_node(
            &engine,
            1,
            "blue",
            &[("status", PropValue::String("blue".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 1)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.segment_count, 1);
    assert_eq!(stats_view.available_segment_stats, 0);
    assert_eq!(stats_view.unavailable_segment_stats, 1);

    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("red".to_string()),
        }],
        false,
    );
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 1);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert!(!plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_zero_is_advisory_not_empty_result() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        index_id = info.index_id;
        wait_for_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
        insert_query_node(
            &engine,
            1,
            "red",
            &[("status", PropValue::String("red".to_string()))],
            1.0,
        );
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let seg_dir = crate::segment_writer::segment_dir(&db_path, 1);
    let mut stats = match crate::planner_stats::read_planner_stats_sidecar(&seg_dir, 1, 1, 0) {
        crate::planner_stats::PlannerStatsAvailability::Available(stats) => *stats,
        other => panic!("expected available planner stats, got {other:?}"),
    };
    let equality = stats
        .equality_index_stats
        .iter_mut()
        .find(|stats| stats.index_id == index_id)
        .expect("expected equality stats for test index");
    equality.total_postings = 0;
    equality.value_group_count = 0;
    equality.max_group_postings = 0;
    equality.top_value_hashes.clear();
    assert_eq!(
        crate::planner_stats::write_planner_stats_sidecar_atomic(&seg_dir, stats).unwrap(),
        crate::planner_stats::PlannerStatsWriteOutcome::Written
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let red = PropValue::String("red".to_string());
    let red_hash = hash_prop_value(&red);
    assert_eq!(
        reopened
            .planner_stats_view_for_test()
            .equality_segment_estimate(index_id, 1, &[red_hash])
            .unwrap(),
        crate::planner_stats::PlannerStatsValueEstimate {
            count: 0,
            exact: true,
        }
    );
    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: red,
        }],
        false,
    );
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 1);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert_eq!(plan.estimated_candidates, Some(0));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_low_equality_estimate_uses_capped_materialization() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let index_id;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let info = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        index_id = info.index_id;
        wait_for_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
        for idx in 0..=QUERY_RANGE_CANDIDATE_CAP {
            insert_query_node(
                &engine,
                1,
                &format!("active-{idx}"),
                &[("status", PropValue::String("active".to_string()))],
                1.0,
            );
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let node_count = (QUERY_RANGE_CANDIDATE_CAP + 1) as u64;
    let seg_dir = crate::segment_writer::segment_dir(&db_path, 1);
    let mut stats = match crate::planner_stats::read_planner_stats_sidecar(&seg_dir, 1, node_count, 0)
    {
        crate::planner_stats::PlannerStatsAvailability::Available(stats) => *stats,
        other => panic!("expected available planner stats, got {other:?}"),
    };
    let equality = stats
        .equality_index_stats
        .iter_mut()
        .find(|stats| stats.index_id == index_id)
        .expect("expected equality stats for test index");
    equality.total_postings = 0;
    equality.value_group_count = 0;
    equality.max_group_postings = 0;
    equality.top_value_hashes.clear();
    assert_eq!(
        crate::planner_stats::write_planner_stats_sidecar_atomic(&seg_dir, stats).unwrap(),
        crate::planner_stats::PlannerStatsWriteOutcome::Written
    );

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    {
        let (_guard, published) = reopened.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_node_query(&query).unwrap();
        let planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        let NodePhysicalPlan::Source(source) = planned.driver else {
            panic!("expected equality source driver");
        };
        assert_eq!(source.kind, NodeQueryCandidateSourceKind::PropertyEqualityIndex);
        assert_eq!(source.estimate.known_upper_bound(), Some(0));
        assert!(!source.estimate.can_use_uncapped_equality_materialization());
    }
    let result = reopened.query_node_ids(&query).unwrap();
    assert_eq!(result.items.len(), QUERY_RANGE_CANDIDATE_CAP + 1);
    assert_eq!(result.next_cursor, None);

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_back_type_and_full_scan_explain_estimates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for idx in 0..3 {
        insert_query_node(&engine, 1, &format!("type1-{idx}"), &[], 1.0);
    }
    for idx in 0..2 {
        insert_query_node(&engine, 2, &format!("type2-{idx}"), &[], 1.0);
    }
    engine.flush().unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let type_estimate = published.view.node_type_estimate(1).unwrap();
        assert_eq!(type_estimate.kind, PlannerEstimateKind::StatsExact);
        assert_eq!(type_estimate.known_upper_bound(), Some(3));
        let full_estimate = published.view.full_scan_estimate();
        assert_eq!(full_estimate.kind, PlannerEstimateKind::StatsExact);
        assert_eq!(full_estimate.known_upper_bound(), Some(5));
    }

    let type_query = query_ids(Some(1), Vec::new(), false);
    assert_eq!(engine.query_node_ids(&type_query).unwrap().items.len(), 3);
    let type_plan = engine.explain_node_query(&type_query).unwrap();
    assert_eq!(type_plan.estimated_candidates, Some(3));
    assert_plan_input_nodes(&type_plan, vec![QueryPlanNode::NodeTypeIndex]);

    let full_scan_query = NodeQuery {
        allow_full_scan: true,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&full_scan_query).unwrap().items.len(), 5);
    let full_scan_plan = engine.explain_node_query(&full_scan_query).unwrap();
    assert_eq!(full_scan_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&full_scan_plan, vec![QueryPlanNode::FallbackFullNodeScan]);
    assert_eq!(
        full_scan_plan.warnings,
        vec![QueryPlanWarning::FullScanExplicitlyAllowed]
    );

    engine.close().unwrap();
}

#[test]
fn test_active_memtable_only_estimates_are_exact_cheap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    insert_query_node(
        &engine,
        1,
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        1,
        "inactive",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let type_estimate = published.view.node_type_estimate(1).unwrap();
    assert_eq!(type_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(type_estimate.known_upper_bound(), Some(2));
    let full_estimate = published.view.full_scan_estimate();
    assert_eq!(full_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(full_estimate.known_upper_bound(), Some(2));
    let (equality_estimate, followup) = published
        .view
        .equality_candidate_estimate(
            status.index_id,
            "status",
            &PropValue::String("active".to_string()),
        )
        .unwrap();
    assert!(followup.is_none());
    let equality_estimate = equality_estimate.unwrap();
    assert_eq!(equality_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(equality_estimate.known_upper_bound(), Some(1));

    let normalized = NormalizedNodeQuery {
        type_id: Some(1),
        ids: Vec::new(),
        keys: Vec::new(),
        filter: NormalizedNodeFilter::AlwaysTrue,
        allow_full_scan: false,
        page: PageRequest::default(),
    };
    let cap_context = published.view.query_cap_context(&normalized).unwrap();
    let mut budget = BooleanPlanningBudget::new();
    let range_probe = published
        .view
        .range_candidate_probe(
            &normalized,
            cap_context,
            1,
            "score",
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            Some(&PropertyRangeBound::Included(PropValue::Int(10))),
            &mut budget,
        )
        .unwrap();
    let range_estimate = range_probe.source.unwrap().estimate;
    assert_eq!(range_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(range_estimate.known_upper_bound(), Some(1));

    let mut budget = BooleanPlanningBudget::new();
    let timestamp_probe = published
        .view
        .timestamp_candidate_probe(&normalized, cap_context, 1, i64::MIN, i64::MAX, &mut budget)
        .unwrap();
    let timestamp_estimate = timestamp_probe.source.unwrap().estimate;
    assert_eq!(timestamp_estimate.kind, PlannerEstimateKind::ExactCheap);
    assert_eq!(timestamp_estimate.known_upper_bound(), Some(2));

    drop(published);
    drop(_guard);
    engine.close().unwrap();
}

#[test]
fn test_planner_stats_equality_heavy_hitter_and_residual_explain_estimates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let values: Vec<String> = (0..40).map(|idx| format!("status-{idx:02}")).collect();
    for value in &values {
        insert_query_node(
            &engine,
            1,
            value,
            &[("status", PropValue::String(value.clone()))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    let rollup = stats_view.equality_index_rollups.get(&info.index_id).unwrap();
    assert_eq!(rollup.total_postings, 40);
    assert_eq!(
        rollup.top_value_hashes.len(),
        crate::planner_stats::PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY
    );
    let top_value = values
        .iter()
        .find(|value| {
            rollup
                .top_value_hashes
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    let residual_value = values
        .iter()
        .find(|value| {
            !rollup
                .top_value_hashes
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    drop(stats_view);

    let top_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(top_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&top_query).unwrap().items.len(), 1);
    let top_plan = engine.explain_node_query(&top_query).unwrap();
    assert_eq!(top_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(&top_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let residual_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(residual_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&residual_query).unwrap().items.len(), 1);
    let residual_plan = engine.explain_node_query(&residual_query).unwrap();
    assert_eq!(residual_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(&residual_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_rare_residual_equality_beats_broad_type_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let value_count =
        QUERY_RANGE_CANDIDATE_CAP + crate::planner_stats::PLANNER_STATS_MAX_HEAVY_HITTERS_PER_KEY + 1;
    let values: Vec<String> = (0..value_count)
        .map(|idx| format!("rare-status-{idx:04}"))
        .collect();
    for value in &values {
        insert_query_node(
            &engine,
            1,
            value,
            &[("status", PropValue::String(value.clone()))],
            1.0,
        );
    }
    engine.flush().unwrap();

    let stats_view = engine.planner_stats_view_for_test();
    let rollup = stats_view.equality_index_rollups.get(&info.index_id).unwrap();
    let residual_value = values
        .iter()
        .find(|value| {
            !rollup
                .top_value_hashes
                .contains_key(&hash_prop_value(&PropValue::String((*value).clone())))
        })
        .unwrap()
        .clone();
    assert!(rollup.total_postings > QUERY_RANGE_CANDIDATE_CAP as u64);
    drop(stats_view);

    let residual_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String(residual_value),
        }],
        false,
    );
    assert_eq!(engine.query_node_ids(&residual_query).unwrap().items.len(), 1);
    let residual_plan = engine.explain_node_query(&residual_query).unwrap();
    assert_eq!(residual_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(residual_plan.estimated_candidates, Some(1));
    assert_plan_input_nodes(
        &residual_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_broad_heavy_hitter_equality_uses_cheaper_type_scan() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let inputs: Vec<_> = (0..=QUERY_RANGE_CANDIDATE_CAP)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("broad-heavy-{index}"),
            props: query_test_props(&[("status", PropValue::String("broad".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();

    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("broad".to_string()),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::CandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_range_and_timestamp_explain_use_no_planning_probe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let inputs: Vec<_> = (0..32)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("stats-probe-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();

    engine.reset_query_planning_probe_counters_for_test();
    let range_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(12))),
        }],
        false,
    );
    let range_plan = engine.explain_node_query(&range_query).unwrap();
    assert_plan_input_nodes(&range_plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert_eq!(
        engine.query_planning_probe_snapshot_for_test().range,
        0,
        "stats-covered range explain must not materialize planning candidates"
    );

    let timestamp_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(i64::MIN),
            upper_ms: Some(i64::MAX),
        }],
        false,
    );
    let timestamp_plan = engine.explain_node_query(&timestamp_query).unwrap();
    assert_plan_input_nodes(&timestamp_plan, vec![QueryPlanNode::TimestampIndex]);
    assert_eq!(
        engine.query_planning_probe_snapshot_for_test().timestamp,
        0,
        "stats-covered timestamp explain must not materialize planning candidates"
    );

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_range_and_timestamp_mixed_coverage_probe_uncovered_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let all_ids;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let score = engine
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

        let seg1 = [
            ("covered-a", 10, 1_000),
            ("covered-b", 20, 1_100),
            ("covered-c", 100, 9_000),
        ];
        let mut ids = Vec::new();
        for (key, score, updated_at) in seg1 {
            let node_id = insert_query_node(
                &engine,
                1,
                key,
                &[("score", PropValue::Int(score))],
                1.0,
            );
            set_query_node_updated_at(&engine, node_id, updated_at);
            ids.push(node_id);
        }
        engine.flush().unwrap();

        let seg2 = [
            ("uncovered-a", 15, 1_200),
            ("uncovered-b", 25, 1_300),
            ("uncovered-c", 200, 10_000),
        ];
        for (key, score, updated_at) in seg2 {
            let node_id = insert_query_node(
                &engine,
                1,
                key,
                &[("score", PropValue::Int(score))],
                1.0,
            );
            set_query_node_updated_at(&engine, node_id, updated_at);
            ids.push(node_id);
        }
        engine.flush().unwrap();
        all_ids = ids;
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 2)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.available_segment_stats, 1);
    assert_eq!(stats_view.unavailable_segment_stats, 1);
    assert_eq!(stats_view.timestamp_coverage.covered_segment_ids, vec![1]);
    let range_index_id = *stats_view.range_index_rollups.keys().next().unwrap();
    assert_eq!(
        stats_view
            .range_index_rollups
            .get(&range_index_id)
            .unwrap()
            .coverage
            .covered_segment_ids,
        vec![1]
    );
    drop(stats_view);

    let range_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(25))),
        }],
        false,
    );
    reopened.reset_query_planning_probe_counters_for_test();
    let range_plan = reopened.explain_node_query(&range_query).unwrap();
    assert_eq!(range_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(range_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&range_plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert_eq!(reopened.query_planning_probe_snapshot_for_test().range, 1);
    assert_eq!(
        reopened.query_node_ids(&range_query).unwrap().items,
        oracle_query_ids(&reopened, &all_ids, &range_query)
    );

    let timestamp_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(1_000),
            upper_ms: Some(1_300),
        }],
        false,
    );
    reopened.reset_query_planning_probe_counters_for_test();
    let timestamp_plan = reopened.explain_node_query(&timestamp_query).unwrap();
    assert_eq!(timestamp_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_eq!(timestamp_plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&timestamp_plan, vec![QueryPlanNode::TimestampIndex]);
    assert_eq!(
        reopened.query_planning_probe_snapshot_for_test().timestamp,
        1
    );
    assert_eq!(
        reopened.query_node_ids(&timestamp_query).unwrap().items,
        oracle_query_ids(&reopened, &all_ids, &timestamp_query)
    );

    reopened.close().unwrap();
}

#[test]
fn test_planner_stats_adaptive_cap_allows_high_confidence_range_above_default() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let selected_count =
        crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP + 256;
    let total_count = selected_count + 1024;
    let inputs: Vec<_> = (0..total_count)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("adaptive-range-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index as i64))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();

    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(
                selected_count as i64 - 1,
            ))),
        }),
        page: PageRequest {
            limit: Some(16),
            after: None,
        },
        ..Default::default()
    };

    let expected: Vec<_> = oracle_query_ids(&engine, &all_ids, &query)
        .into_iter()
        .take(16)
        .collect();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyRangeIndex]);
    assert!(
        plan.estimated_candidates
            > Some(crate::planner_stats::PLANNER_STATS_DEFAULT_SELECTED_SOURCE_CAP as u64)
    );

    engine.close().unwrap();
}

#[test]
fn test_direct_read_apis_are_unchanged_with_planner_stats_sidecars() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let inputs = vec![
        NodeInput {
            type_id: 1,
            key: "direct-a".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("active".to_string())),
                ("score", PropValue::Int(10)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            type_id: 1,
            key: "direct-b".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("inactive".to_string())),
                ("score", PropValue::Int(20)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            type_id: 2,
            key: "direct-c".to_string(),
            props: query_test_props(&[
                ("status", PropValue::String("active".to_string())),
                ("score", PropValue::Int(10)),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
    ];
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();

    assert_eq!(
        engine
            .find_nodes(1, "status", &PropValue::String("active".to_string()))
            .unwrap(),
        vec![ids[0]]
    );
    assert_eq!(
        engine
            .find_nodes_range(
                1,
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(10))),
                Some(&PropertyRangeBound::Included(PropValue::Int(20))),
            )
            .unwrap(),
        vec![ids[0], ids[1]]
    );
    assert_eq!(
        engine
            .find_nodes_by_time_range(1, i64::MIN, i64::MAX)
            .unwrap(),
        vec![ids[0], ids[1]]
    );
    assert_eq!(engine.nodes_by_type(1).unwrap(), vec![ids[0], ids[1]]);

    engine.close().unwrap();
}

#[test]
fn test_planner_stats_mixed_segment_fallback_estimates_once() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        for idx in 0..2 {
            insert_query_node(&engine, 1, &format!("covered-{idx}"), &[], 1.0);
        }
        engine.flush().unwrap();
        for idx in 0..3 {
            insert_query_node(&engine, 1, &format!("fallback-{idx}"), &[], 1.0);
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 2)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::write(&stats_path, b"corrupt planner stats").unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let stats_view = reopened.planner_stats_view_for_test();
    assert_eq!(stats_view.segment_count, 2);
    assert_eq!(stats_view.available_segment_stats, 1);
    assert_eq!(stats_view.unavailable_segment_stats, 1);
    assert_eq!(stats_view.type_node_count(1), 2);
    assert_eq!(stats_view.type_coverage.covered_segment_ids, vec![1]);
    drop(stats_view);
    {
        let (_guard, published) = reopened.runtime.published_snapshot().unwrap();
        let estimate = published.view.node_type_estimate(1).unwrap();
        assert_eq!(estimate.kind, PlannerEstimateKind::UpperBound);
        assert_eq!(estimate.known_upper_bound(), Some(5));
    }

    let query = query_ids(Some(1), Vec::new(), false);
    assert_eq!(reopened.query_node_ids(&query).unwrap().items.len(), 5);
    let plan = reopened.explain_node_query(&query).unwrap();
    assert_eq!(plan.estimated_candidates, Some(5));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeTypeIndex]);

    reopened.close().unwrap();
}

#[test]
fn test_planner_estimate_sort_prefers_cheaper_count_before_source_rank() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let mut candidates = vec![
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                1,
                "status",
                &PropValue::String("active".to_string()),
                PlannerEstimate::stats_estimated(
                    100,
                    EstimateConfidence::High,
                    StalePostingRisk::Low,
                ),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::fallback_type_scan(
                1,
                PlannerEstimate::upper_bound(10),
            )),
        ];
        published
            .view
            .sort_physical_plans_by_selectivity(&mut candidates);
        assert_eq!(candidates[0].plan_node(), QueryPlanNode::FallbackTypeScan);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_validation_and_explain_reject_type_less_scan_without_opt_in() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert!(matches!(
        engine.query_node_ids(&query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));
    assert!(matches!(
        engine.explain_node_query(&query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let key_query = NodeQuery {
        keys: vec!["alice".to_string()],
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&key_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_range_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: None,
            upper: None,
        }],
        false,
    );
    assert!(matches!(
        engine.query_node_ids(&empty_range_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_time_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: None,
            upper_ms: None,
        }],
        false,
    );
    assert!(matches!(
        engine.explain_node_query(&empty_time_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let inverted_time_query = query_ids(
        Some(1),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(200),
            upper_ms: Some(100),
        }],
        false,
    );
    assert!(engine
        .query_node_ids(&inverted_time_query)
        .unwrap()
        .items
        .is_empty());
    assert!(matches!(
        explain_input_node(&engine.explain_node_query(&inverted_time_query).unwrap()),
        QueryPlanNode::EmptyResult
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_normalization_expands_open_updated_at_bounds() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();

        let lower_open_query = query_ids(
            Some(1),
            vec![NodeFilterExpr::UpdatedAtRange {
                lower_ms: None,
                upper_ms: Some(123),
            }],
            false,
        );
        let normalized = published
            .view
            .normalize_node_query(&lower_open_query)
            .unwrap();
        match normalized.filter {
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                assert_eq!(lower_ms, i64::MIN);
                assert_eq!(upper_ms, 123);
            }
            _ => panic!("expected normalized updated-at range"),
        }

        let upper_open_query = query_ids(
            Some(1),
            vec![NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(456),
                upper_ms: None,
            }],
            false,
        );
        let normalized = published
            .view
            .normalize_node_query(&upper_open_query)
            .unwrap();
        match normalized.filter {
            NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms } => {
                assert_eq!(lower_ms, 456);
                assert_eq!(upper_ms, i64::MAX);
            }
            _ => panic!("expected normalized updated-at range"),
        }
    }

    engine.close().unwrap();
}

#[test]
fn test_query_filter_validation_and_empty_result_without_scan_opt_in() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for filter in [
        NodeFilterExpr::And(Vec::new()),
        NodeFilterExpr::Or(Vec::new()),
        NodeFilterExpr::PropertyEquals {
            key: String::new(),
            value: PropValue::String("x".to_string()),
        },
        NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: Vec::new(),
        },
    ] {
        let query = NodeQuery {
            type_id: Some(1),
            filter: Some(filter),
            ..Default::default()
        };
        assert!(matches!(
            engine.explain_node_query(&query).unwrap_err(),
            EngineError::InvalidOperation(_)
        ));
    }

    let always_false = NodeQuery {
        filter: filter_and![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
        ],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&always_false).unwrap().items.is_empty());
    let plan = engine.explain_node_query(&always_false).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert!(matches!(explain_input_node(&plan), QueryPlanNode::EmptyResult));

    let always_true_without_anchor = NodeQuery {
        filter: Some(NodeFilterExpr::Not(Box::new(
            always_false.filter.clone().unwrap(),
        ))),
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&always_true_without_anchor).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_filter_exists_missing_not_and_or_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let tagged_null = insert_query_node(
        &engine,
        1,
        "tagged-null",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::Null),
        ],
        1.0,
    );
    let missing_tag = insert_query_node(
        &engine,
        1,
        "missing-tag",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let tagged_trial = insert_query_node(
        &engine,
        1,
        "tagged-trial",
        &[
            ("status", PropValue::String("trial".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );

    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("trial".to_string()),
                },
            ]),
            NodeFilterExpr::PropertyExists {
                key: "tag".to_string(),
            },
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![tagged_null, tagged_trial]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);

    let missing_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "tag".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&missing_query).unwrap().items, vec![missing_tag]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_in_dedupes_by_canonical_value_and_uses_union() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut map_value = BTreeMap::new();
    map_value.insert("x".to_string(), PropValue::Int(1));
    let map_value = PropValue::Map(map_value);
    let array_value = PropValue::Array(vec![PropValue::Int(1), PropValue::UInt(2)]);

    let null_id = insert_query_node(&engine, 1, "null", &[("kind", PropValue::Null)], 1.0);
    let int_id = insert_query_node(&engine, 1, "int", &[("kind", PropValue::Int(1))], 1.0);
    let uint_id = insert_query_node(&engine, 1, "uint", &[("kind", PropValue::UInt(1))], 1.0);
    let array_id = insert_query_node(&engine, 1, "array", &[("kind", array_value.clone())], 1.0);
    let map_id = insert_query_node(&engine, 1, "map", &[("kind", map_value.clone())], 1.0);
    let neg_zero_id = insert_query_node(
        &engine,
        1,
        "neg-zero-kind",
        &[("kind", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero_id = insert_query_node(
        &engine,
        1,
        "pos-zero-kind",
        &[("kind", PropValue::Float(0.0))],
        1.0,
    );
    let _missing = insert_query_node(&engine, 1, "missing", &[], 1.0);

    let index = engine
        .ensure_node_property_index(1, "kind", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![
                PropValue::Null,
                PropValue::Null,
                PropValue::UInt(1),
                array_value.clone(),
                map_value.clone(),
                map_value.clone(),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![null_id, uint_id, array_id, map_id]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(!plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let int_only = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![PropValue::Int(1)],
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&int_only).unwrap().items, vec![int_id]);
    let int_only_plan = engine.explain_node_query(&int_only).unwrap();
    assert!(!int_only_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&int_only_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let signed_zero_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "kind".to_string(),
            values: vec![PropValue::Float(-0.0), PropValue::Float(0.0)],
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&signed_zero_query).unwrap().items,
        vec![neg_zero_id, pos_zero_id]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&signed_zero_query).unwrap(),
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_large_verify_only_in_matches_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let string_match = insert_query_node(
        &engine,
        1,
        "token-string-match",
        &[("token", PropValue::String("value-63".to_string()))],
        1.0,
    );
    let signed_zero_match = insert_query_node(
        &engine,
        1,
        "token-signed-zero-match",
        &[("token", PropValue::Float(0.0))],
        1.0,
    );
    let nested_zero_match = insert_query_node(
        &engine,
        1,
        "token-nested-zero-match",
        &[(
            "token",
            PropValue::Array(vec![PropValue::Float(0.0)]),
        )],
        1.0,
    );
    insert_query_node(
        &engine,
        1,
        "token-nan-not-match",
        &[("token", PropValue::Float(f64::NAN))],
        1.0,
    );
    insert_query_node(
        &engine,
        1,
        "token-miss",
        &[("token", PropValue::String("missing".to_string()))],
        1.0,
    );

    let mut values: Vec<PropValue> = (0..64)
        .map(|index| PropValue::String(format!("value-{index}")))
        .collect();
    values.push(PropValue::Float(-0.0));
    values.push(PropValue::Array(vec![PropValue::Float(-0.0)]));
    values.push(PropValue::Float(f64::NAN));
    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "token".to_string(),
            values,
        }),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![string_match, signed_zero_match, nested_zero_match]
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert!(plan.warnings.contains(&QueryPlanWarning::MissingReadyIndex));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_equality_contradictions_match_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let neg_zero = insert_query_node(
        &engine,
        1,
        "neg-zero",
        &[("temperature", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero = insert_query_node(
        &engine,
        1,
        "pos-zero",
        &[("temperature", PropValue::Float(0.0))],
        1.0,
    );

    let query = NodeQuery {
        ids: vec![neg_zero, pos_zero],
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "temperature".to_string(),
                value: PropValue::Float(-0.0),
            },
            NodeFilterExpr::PropertyEquals {
                key: "temperature".to_string(),
                value: PropValue::Float(0.0),
            },
        ])),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::ExplicitIds],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_indexed_float_signed_zero_equality_matches_verifier_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let neg_zero = insert_query_node(
        &engine,
        1,
        "indexed-neg-zero",
        &[("temperature", PropValue::Float(-0.0))],
        1.0,
    );
    let pos_zero = insert_query_node(
        &engine,
        1,
        "indexed-pos-zero",
        &[("temperature", PropValue::Float(0.0))],
        1.0,
    );
    insert_query_node(
        &engine,
        1,
        "indexed-one",
        &[("temperature", PropValue::Float(1.0))],
        1.0,
    );
    engine.flush().unwrap();

    let index = engine
        .ensure_node_property_index(1, "temperature", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let neg_zero_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "temperature".to_string(),
            value: PropValue::Float(-0.0),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&neg_zero_query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&neg_zero_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let pos_zero_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "temperature".to_string(),
            value: PropValue::Float(0.0),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&pos_zero_query).unwrap().items,
        vec![neg_zero, pos_zero]
    );
    assert_plan_input_nodes(
        &engine.explain_node_query(&pos_zero_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_or_and_in_extract_complete_index_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(
        &engine,
        1,
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let _inactive = insert_query_node(
        &engine,
        1,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let or_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&or_query).unwrap().items, vec![active, trial]);
    let or_plan = engine.explain_node_query(&or_query).unwrap();
    assert!(!or_plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &or_plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let singleton_or_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&singleton_or_query).unwrap().items,
        vec![active]
    );
    let singleton_or_plan = engine.explain_node_query(&singleton_or_query).unwrap();
    assert!(!singleton_or_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &singleton_or_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let double_not_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Not(Box::new(NodeFilterExpr::Not(Box::new(
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ))))),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&double_not_query).unwrap().items,
        vec![active]
    );
    let double_not_plan = engine.explain_node_query(&double_not_query).unwrap();
    assert!(!double_not_plan
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(&double_not_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let in_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: vec![
                PropValue::String("active".to_string()),
                PropValue::String("trial".to_string()),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&in_query).unwrap().items, vec![active, trial]);
    let in_plan = engine.explain_node_query(&in_query).unwrap();
    assert!(!in_plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert_plan_input_nodes(
        &in_plan,
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_or_in_union_final_verification_and_pagination() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let stale_active = insert_query_node(
        &engine,
        1,
        "stale-active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let deleted_trial = insert_query_node(
        &engine,
        1,
        "deleted-trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(
        &engine,
        1,
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    for index in 0..3 {
        insert_query_node(
            &engine,
            1,
            &format!("inactive-{index}"),
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let updated = engine
        .upsert_node(
            1,
            "stale-active",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("inactive".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated, stale_active);
    engine.delete_node(deleted_trial).unwrap();

    let or_filter = NodeFilterExpr::Or(vec![
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("trial".to_string()),
        },
    ]);
    let mut or_query = NodeQuery {
        type_id: Some(1),
        filter: Some(or_filter.clone()),
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    let first = engine.query_node_ids(&or_query).unwrap();
    assert_eq!(first.items, vec![active]);
    assert_eq!(first.next_cursor, Some(active));
    or_query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&or_query).unwrap();
    assert_eq!(second.items, vec![trial]);
    assert_eq!(second.next_cursor, None);

    or_query.page = PageRequest::default();
    assert_eq!(engine.query_node_ids(&or_query).unwrap().items, vec![active, trial]);
    assert_plan_input_nodes(
        &engine.explain_node_query(&or_query).unwrap(),
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }],
    );

    let in_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::PropertyIn {
            key: "status".to_string(),
            values: vec![
                PropValue::String("trial".to_string()),
                PropValue::String("active".to_string()),
                PropValue::String("active".to_string()),
            ],
        }),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&in_query).unwrap().items, vec![active, trial]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_and_of_or_intersects_range() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active_high = insert_query_node(
        &engine,
        1,
        "active-high",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let trial_high = insert_query_node(
        &engine,
        1,
        "trial-high",
        &[
            ("status", PropValue::String("trial".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let _active_low = insert_query_node(
        &engine,
        1,
        "active-low",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(1)),
        ],
        1.0,
    );
    let _inactive_high = insert_query_node(
        &engine,
        1,
        "inactive-high",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(40)),
        ],
        1.0,
    );
    engine.flush().unwrap();
    let status_index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score_index = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score_index.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("trial".to_string()),
                },
            ]),
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        vec![active_high, trial_high]
    );
    assert_plan_includes_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        &[
            QueryPlanNode::Union {
                inputs: vec![
                    QueryPlanNode::PropertyEqualityIndex,
                    QueryPlanNode::PropertyEqualityIndex,
                ],
            },
            QueryPlanNode::PropertyRangeIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_filter_fallback_budget_and_empty_plan_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(1)),
        ],
        1.0,
    );
    let scored = insert_query_node(
        &engine,
        1,
        "scored",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(50)),
        ],
        1.0,
    );
    for index in 0..8 {
        insert_query_node(
            &engine,
            1,
            &format!("filler-{index}"),
            &[
                ("status", PropValue::String(format!("v{index}"))),
                ("score", PropValue::Int(index)),
            ],
            1.0,
        );
    }
    engine.flush().unwrap();
    let status_index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);

    let impossible = NodeQuery {
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])),
        ..Default::default()
    };
    assert!(engine.query_node_ids(&impossible).unwrap().items.is_empty());
    assert_plan_input_nodes(
        &engine.explain_node_query(&impossible).unwrap(),
        vec![QueryPlanNode::EmptyResult],
    );

    let always_true_requires_anchor = NodeQuery {
        filter: Some(NodeFilterExpr::Not(Box::new(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("trial".to_string()),
            },
        ])))),
        ..Default::default()
    };
    assert!(matches!(
        engine.query_node_ids(&always_true_requires_anchor),
        Err(EngineError::InvalidOperation(_))
    ));

    let missing_index_or = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(40))),
                upper: None,
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&missing_index_or).unwrap().items,
        vec![active, scored]
    );
    let missing_plan = engine.explain_node_query(&missing_index_or).unwrap();
    assert_eq!(
        missing_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&missing_plan, vec![QueryPlanNode::FallbackTypeScan]);

    let budget_or = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(
            (0..=MAX_BOOLEAN_UNION_INPUTS)
                .map(|index| NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String(format!("v{index}")),
                })
                .collect(),
        )),
        ..Default::default()
    };
    let budget_plan = engine.explain_node_query(&budget_or).unwrap();
    assert_eq!(
        budget_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_or_unknown_branch_falls_back_without_partial_union() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let indexed = insert_query_node(
        &engine,
        1,
        "indexed",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let missing_index = insert_query_node(
        &engine,
        1,
        "missing-index",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let other = insert_query_node(&engine, 1, "other", &[], 1.0);
    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
            },
        ])),
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[indexed, missing_index, other], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_filter_verify_only_uses_expected_legal_universe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let type1_inputs: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 8)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("type1-archived-{index}"),
            props: query_test_props(&[("archived", PropValue::Bool(true))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let type1_ids = engine.batch_upsert_nodes(&type1_inputs).unwrap();
    let type1_archived = type1_ids[0];
    let type1_missing = insert_query_node(&engine, 1, "type1-missing", &[], 1.0);
    let small_missing = insert_query_node(&engine, 2, "small-missing", &[], 1.0);
    let small_archived = insert_query_node(
        &engine,
        2,
        "small-archived",
        &[("archived", PropValue::Bool(true))],
        1.0,
    );
    let active_tag = insert_query_node(
        &engine,
        3,
        "active-tag",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let active_missing = insert_query_node(
        &engine,
        3,
        "active-missing",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive_tag = insert_query_node(
        &engine,
        3,
        "inactive-tag",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );

    let status_index = engine
        .ensure_node_property_index(3, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status_index.index_id, SecondaryIndexState::Ready);

    let mut huge_ids = type1_ids.clone();
    huge_ids.push(type1_missing);
    huge_ids.push(small_missing);
    huge_ids.push(small_archived);
    let type_small_query = NodeQuery {
        type_id: Some(2),
        ids: huge_ids,
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "archived".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&type_small_query).unwrap().items,
        vec![small_missing]
    );
    let type_small_plan = engine.explain_node_query(&type_small_query).unwrap();
    assert_eq!(
        type_small_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&type_small_plan, vec![QueryPlanNode::FallbackTypeScan]);

    let ids_small_query = NodeQuery {
        type_id: Some(1),
        ids: vec![type1_missing, type1_archived],
        filter: Some(NodeFilterExpr::PropertyMissing {
            key: "archived".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&ids_small_query).unwrap().items,
        vec![type1_missing]
    );
    let ids_small_plan = engine.explain_node_query(&ids_small_query).unwrap();
    assert_eq!(
        ids_small_plan.warnings,
        vec![QueryPlanWarning::VerifyOnlyFilter]
    );
    assert_plan_input_nodes(&ids_small_plan, vec![QueryPlanNode::ExplicitIds]);

    let equality_plus_not_missing = NodeQuery {
        type_id: Some(3),
        filter: Some(NodeFilterExpr::And(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::Not(Box::new(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            })),
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine
            .query_node_ids(&equality_plus_not_missing)
            .unwrap()
            .items,
        vec![active_tag]
    );
    let equality_plus_not_missing_plan = engine
        .explain_node_query(&equality_plus_not_missing)
        .unwrap();
    assert_eq!(
        equality_plus_not_missing_plan.warnings,
        vec![QueryPlanWarning::VerifyOnlyFilter]
    );
    assert_plan_input_nodes(
        &equality_plus_not_missing_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let or_missing = NodeQuery {
        type_id: Some(3),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&or_missing).unwrap().items,
        vec![active_tag, active_missing]
    );
    let or_missing_plan = engine.explain_node_query(&or_missing).unwrap();
    assert_eq!(
        or_missing_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&or_missing_plan, vec![QueryPlanNode::FallbackTypeScan]);

    assert!(!engine
        .query_node_ids(&NodeQuery {
            type_id: Some(3),
            ids: vec![inactive_tag],
            filter: Some(NodeFilterExpr::PropertyMissing {
                key: "tag".to_string(),
            }),
            ..Default::default()
        })
        .unwrap()
        .items
        .contains(&inactive_tag));

    engine.close().unwrap();
}

#[test]
fn test_query_filter_range_and_timestamp_probe_budget_overflow_is_cumulative() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let inputs: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 8)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("budget-{index}"),
            props: query_test_props(&[("score", PropValue::Int(index as i64))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
    let score_index = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, score_index.index_id, SecondaryIndexState::Ready);
    let segment_id = engine.segments_for_test()[0].segment_id;
    let stats_path =
        segment_dir(&db_path, segment_id).join(crate::planner_stats::PLANNER_STATS_FILENAME);
    engine.close().unwrap();
    std::fs::remove_file(&stats_path).unwrap();
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(engine.segments_for_test()[0].planner_stats().is_none());

    let range_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::And(
            (0..5)
                .map(|lower| NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(lower))),
                    upper: None,
                })
                .collect(),
        )),
        ..Default::default()
    };
    let range_plan = engine.explain_node_query(&range_query).unwrap();
    assert_eq!(
        range_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    let timestamp_query = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::And(
            (0..5)
                .map(|lower| NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(lower),
                    upper_ms: None,
                })
                .collect(),
        )),
        ..Default::default()
    };
    let timestamp_plan = engine.explain_node_query(&timestamp_query).unwrap();
    assert_eq!(
        timestamp_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::TimestampCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::PlanningProbeBudgetExceeded,
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_boolean_anchor_uses_union_and_filters_stale_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let stale_active = insert_query_node(
        &engine,
        1,
        "stale-active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let deleted_trial = insert_query_node(
        &engine,
        1,
        "deleted-trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(
        &engine,
        1,
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(
        &engine,
        1,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);
    for source in [stale_active, deleted_trial, active, trial, inactive] {
        engine
            .upsert_edge(source, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let updated = engine
        .upsert_node(
            1,
            "stale-active",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("inactive".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated, stale_active);
    engine.delete_node(deleted_trial).unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "person".to_string(),
                type_id: Some(1),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Or(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("trial".to_string()),
                    },
                ])),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "person",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("person", active), ("target", target)], &[("edge", 3)]),
            expected_match(&[("person", trial), ("target", target)], &[("edge", 4)]),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    let (anchor_alias, _) = pattern_anchor_plan_node(&plan);
    assert_eq!(anchor_alias, "person");
    assert_eq!(
        pattern_anchor_input_nodes(&plan),
        vec![QueryPlanNode::Union {
            inputs: vec![
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyEqualityIndex,
            ],
        }]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_boolean_anchor_with_verify_only_branch_uses_type_scan() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let missing = insert_query_node(
        &engine,
        1,
        "missing",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let skipped = insert_query_node(
        &engine,
        1,
        "skipped",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tag", PropValue::String("present".to_string())),
        ],
        1.0,
    );
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);
    for source in [active, missing, skipped] {
        engine
            .upsert_edge(source, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();
    let index = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, index.index_id, SecondaryIndexState::Ready);

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "person".to_string(),
                type_id: Some(1),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Or(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyMissing {
                        key: "tag".to_string(),
                    },
                ])),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "person",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(&[("person", active), ("target", target)], &[("edge", 1)]),
            expected_match(&[("person", missing), ("target", target)], &[("edge", 2)]),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::FallbackTypeScan]);
    assert!(plan.warnings.contains(&QueryPlanWarning::UsingFallbackScan));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(plan.warnings.contains(&QueryPlanWarning::BooleanBranchFallback));
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_exists_anchor_uses_type_scan_and_presence_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let present = insert_query_node(
        &engine,
        1,
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let null_tag = insert_query_node(&engine, 1, "null-tag", &[("tag", PropValue::Null)], 1.0);
    let missing = insert_query_node(&engine, 1, "missing", &[], 1.0);
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);
    let present_edge = engine
        .upsert_edge(present, target, 10, UpsertEdgeOptions::default())
        .unwrap();
    let null_edge = engine
        .upsert_edge(null_tag, target, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(missing, target, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                type_id: Some(1),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyExists {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("source", present), ("target", target)],
                &[("edge", present_edge)]
            ),
            expected_match(
                &[("source", null_tag), ("target", target)],
                &[("edge", null_edge)]
            ),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(
        pattern_anchor_input_nodes(&plan),
        vec![QueryPlanNode::FallbackTypeScan]
    );
    assert!(plan.warnings.contains(&QueryPlanWarning::UsingFallbackScan));
    assert!(plan.warnings.contains(&QueryPlanWarning::VerifyOnlyFilter));
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_ids_with_verify_only_filter_is_valid_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let missing = insert_query_node(&engine, 1, "missing", &[], 1.0);
    let present = insert_query_node(
        &engine,
        1,
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);
    let edge = engine
        .upsert_edge(missing, target, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(present, target, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                type_id: None,
                ids: vec![missing, present],
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("source", missing), ("target", target)], &[("edge", edge)])]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_rejects_typeless_verify_only_initial_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert!(matches!(
        engine.query_pattern(&query).unwrap_err(),
        EngineError::InvalidOperation(message)
            if message.contains("anchorable node pattern")
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_typeless_missing_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
    let missing = insert_query_node(&engine, 2, "missing", &[], 1.0);
    let present = insert_query_node(
        &engine,
        2,
        "present",
        &[("tag", PropValue::String("present".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(anchor, missing, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, present, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            NodePattern {
                alias: "target".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyMissing {
                    key: "tag".to_string(),
                }),
            },
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("anchor", anchor), ("target", missing)], &[("edge", edge)])]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_typeless_not_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
    let active = insert_query_node(
        &engine,
        2,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let missing_status = insert_query_node(&engine, 2, "missing-status", &[], 1.0);
    let inactive = insert_query_node(
        &engine,
        2,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let active_edge = engine
        .upsert_edge(anchor, active, 10, UpsertEdgeOptions::default())
        .unwrap();
    let missing_edge = engine
        .upsert_edge(anchor, missing_status, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, inactive, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            NodePattern {
                alias: "target".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Not(Box::new(
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("inactive".to_string()),
                    },
                ))),
            },
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("anchor", anchor), ("target", active)],
                &[("edge", active_edge)]
            ),
            expected_match(
                &[("anchor", anchor), ("target", missing_status)],
                &[("edge", missing_edge)]
            ),
        ]
    );
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::ExplicitIds]);
    assert!(!plan_contains_fallback_full_node_scan(&plan.root));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_always_false_node_returns_empty_result() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let query = pattern_query(
        vec![
            NodePattern {
                alias: "source".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::And(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("inactive".to_string()),
                    },
                ])),
            },
            pattern_node("target", None, Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    let result = engine.query_pattern(&query).unwrap();
    assert!(result.matches.is_empty());
    assert!(!result.truncated);
    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(pattern_anchor_plan_node(&plan).0, "source");
    assert_eq!(pattern_anchor_input_nodes(&plan), vec![QueryPlanNode::EmptyResult]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_target_in_matches_equivalent_or_filter() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
    let active = insert_query_node(
        &engine,
        2,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let trial = insert_query_node(
        &engine,
        2,
        "trial",
        &[("status", PropValue::String("trial".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(
        &engine,
        2,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let active_edge = engine
        .upsert_edge(anchor, active, 10, UpsertEdgeOptions::default())
        .unwrap();
    let trial_edge = engine
        .upsert_edge(anchor, trial, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, inactive, 10, UpsertEdgeOptions::default())
        .unwrap();

    let base_nodes = vec![pattern_node_with_ids("anchor", vec![anchor])];
    let edges = vec![pattern_edge(
        Some("edge"),
        "anchor",
        "target",
        Direction::Outgoing,
        Some(vec![10]),
    )];
    let in_query = pattern_query(
        {
            let mut nodes = base_nodes.clone();
            nodes.push(NodePattern {
                alias: "target".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyIn {
                    key: "status".to_string(),
                    values: vec![
                        PropValue::String("active".to_string()),
                        PropValue::String("trial".to_string()),
                    ],
                }),
            });
            nodes
        },
        edges.clone(),
    );
    let or_query = pattern_query(
        {
            let mut nodes = base_nodes;
            nodes.push(NodePattern {
                alias: "target".to_string(),
                type_id: None,
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::Or(vec![
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("active".to_string()),
                    },
                    NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("trial".to_string()),
                    },
                ])),
            });
            nodes
        },
        edges,
    );

    let expected = vec![
        expected_match(&[("anchor", anchor), ("target", active)], &[("edge", active_edge)]),
        expected_match(&[("anchor", anchor), ("target", trial)], &[("edge", trial_edge)]),
    ];
    assert_eq!(engine.query_pattern(&in_query).unwrap().matches, expected);
    assert_eq!(
        engine.query_pattern(&or_query).unwrap().matches,
        engine.query_pattern(&in_query).unwrap().matches
    );

    engine.close().unwrap();
}

// --- scan-backed node queries ---

#[test]
fn test_query_type_only_uses_type_index_path() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, 1, "a", &[], 1.0);
    let b = insert_query_node(&engine, 1, "b", &[], 1.0);
    let _other_type = insert_query_node(&engine, 2, "x", &[], 1.0);

    let query = query_ids(Some(1), Vec::new(), false);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, _other_type], &query)
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyNodeFilter { ref input }
            if **input == QueryPlanNode::NodeTypeIndex
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_type_only_pagination_excludes_deleted_and_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let kept = insert_query_node(&engine, 1, "kept", &[], 1.0);
    let deleted = insert_query_node(&engine, 1, "deleted", &[], 1.0);
    let overwritten = insert_query_node(&engine, 1, "overwritten", &[], 1.0);
    let _other_type = insert_query_node(&engine, 2, "other", &[], 1.0);
    engine.flush().unwrap();

    engine.delete_node(deleted).unwrap();
    let overwritten_again = insert_query_node(
        &engine,
        1,
        "overwritten",
        &[("status", PropValue::String("new".to_string()))],
        1.0,
    );
    assert_eq!(overwritten_again, overwritten);
    let memtable = insert_query_node(&engine, 1, "memtable", &[], 1.0);

    let mut expected = vec![kept, overwritten, memtable];
    expected.sort_unstable();

    let mut query = query_ids(Some(1), Vec::new(), false);
    query.page = PageRequest {
        limit: Some(2),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, expected[..2]);
    assert_eq!(page1.next_cursor, Some(expected[1]));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, expected[2..]);
    assert_eq!(page2.next_cursor, None);

    engine.flush().unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(Some(1), Vec::new(), false);
    assert_eq!(reopened.query_node_ids(&query).unwrap().items, expected);
    reopened.close().unwrap();
}

#[test]
fn test_query_type_only_pagination_across_multiple_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut expected = Vec::new();
    for index in 0..6 {
        expected.push(insert_query_node(
            &engine,
            1,
            &format!("seg-a-{index}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();

    for index in 0..6 {
        expected.push(insert_query_node(
            &engine,
            1,
            &format!("seg-b-{index}"),
            &[],
            1.0,
        ));
    }
    engine.flush().unwrap();

    let deleted = expected[3];
    engine.delete_node(deleted).unwrap();
    let memtable = insert_query_node(&engine, 1, "memtable", &[], 1.0);
    expected.retain(|id| *id != deleted);
    expected.push(memtable);
    expected.sort_unstable();

    let mut query = query_ids(Some(1), Vec::new(), false);
    query.page = PageRequest {
        limit: Some(5),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, expected[..5]);
    assert_eq!(page1.next_cursor, Some(expected[4]));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, expected[5..10]);
    assert_eq!(page2.next_cursor, Some(expected[9]));

    query.page.after = page2.next_cursor;
    let page3 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page3.items, expected[10..]);
    assert_eq!(page3.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn test_query_type_universe_beats_large_explicit_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let filler: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 32)
        .map(|index| NodeInput {
            type_id: 2,
            key: format!("filler-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mut all_ids = engine.batch_upsert_nodes(&filler).unwrap();
    engine.flush().unwrap();

    let mut expected = Vec::new();
    for index in 0..8 {
        let id = insert_query_node(&engine, 1, &format!("small-{index}"), &[], 1.0);
        all_ids.push(id);
        expected.push(id);
    }
    all_ids.sort_unstable();
    expected.sort_unstable();

    let query = NodeQuery {
        type_id: Some(1),
        ids: all_ids,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeTypeIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_small_explicit_ids_beat_large_type_universe() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut type_ids = Vec::new();
    for index in 0..300 {
        type_ids.push(insert_query_node(
            &engine,
            1,
            &format!("node-{index}"),
            &[],
            1.0,
        ));
    }

    let expected = vec![type_ids[12], type_ids[223]];
    let query = NodeQuery {
        type_id: Some(1),
        ids: expected.clone(),
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_explain_omits_type_scan_when_property_index_drives_execution() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(
        &engine,
        1,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let other_type = insert_query_node(
        &engine,
        2,
        "other",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[active, inactive, other_type], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_type_universe_beats_large_key_upper_bound() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..300 {
        insert_query_node(&engine, 2, &format!("filler-{index}"), &[], 1.0);
    }
    engine.flush().unwrap();

    let mut expected = Vec::new();
    let mut keys = Vec::new();
    for index in 0..QUERY_RANGE_CANDIDATE_CAP + 32 {
        keys.push(format!("missing-{index}"));
    }
    for index in 0..8 {
        let key = format!("small-{index}");
        expected.push(insert_query_node(&engine, 1, &key, &[], 1.0));
        keys.push(key);
    }
    expected.sort_unstable();

    let query = NodeQuery {
        type_id: Some(1),
        keys,
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, expected);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::NodeTypeIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_type_universe_verifies_large_ids_and_predicate() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let filler: Vec<NodeInput> = (0..QUERY_RANGE_CANDIDATE_CAP + 32)
        .map(|index| NodeInput {
            type_id: 2,
            key: format!("filler-{index}"),
            props: query_test_props(&[("status", PropValue::String("keep".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mut all_ids = engine.batch_upsert_nodes(&filler).unwrap();
    engine.flush().unwrap();

    let keep = insert_query_node(
        &engine,
        1,
        "keep",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let drop = insert_query_node(
        &engine,
        1,
        "drop",
        &[("status", PropValue::String("drop".to_string()))],
        1.0,
    );
    all_ids.push(keep);
    all_ids.push(drop);
    all_ids.sort_unstable();

    let query = NodeQuery {
        type_id: Some(1),
        ids: all_ids,
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![keep]);

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_type_scan_predicates_pagination_and_hydration_parity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        1,
        "c",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let _other_type = insert_query_node(
        &engine,
        2,
        "x",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );

    let mut query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: Some(PropertyRangeBound::Excluded(PropValue::Int(30))),
            },
            NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(0),
                upper_ms: None,
            },
        ],
        false,
    );
    query.page = PageRequest {
        limit: Some(1),
        after: None,
    };

    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, vec![a]);
    assert_eq!(first.next_cursor, Some(a));

    query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&query).unwrap();
    assert!(!second.items.contains(&c));
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.next_cursor, None);

    query.page = PageRequest::default();
    let ids = engine.query_node_ids(&query).unwrap();
    assert_eq!(
        ids.items,
        oracle_query_ids(&engine, &[a, b, c, _other_type], &query)
    );
    let nodes = engine.query_nodes(&query).unwrap();
    assert_eq!(
        ids.items,
        nodes.items.iter().map(|node| node.id).collect::<Vec<_>>()
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.kind, QueryPlanKind::NodeQuery);
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert!(matches!(
        explain_input_node(&plan),
        QueryPlanNode::TimestampIndex
    ));

    engine.close().unwrap();
}

// --- anchor semantics ---

#[test]
fn test_query_multi_anchor_and_semantics_and_conflicts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(
        &engine,
        1,
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let bob = insert_query_node(
        &engine,
        1,
        "bob",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let matched = NodeQuery {
        type_id: Some(1),
        ids: vec![alice, bob],
        keys: vec!["alice".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&matched).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob], &matched)
    );

    let conflict = NodeQuery {
        type_id: Some(1),
        ids: vec![bob],
        keys: vec!["alice".to_string()],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&conflict).unwrap().items.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_key_lookup_anchor_normalization_and_source_choice() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(
        &engine,
        1,
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let bob = insert_query_node(
        &engine,
        1,
        "bob",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let carol = insert_query_node(
        &engine,
        1,
        "carol",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let other_type = insert_query_node(
        &engine,
        2,
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );

    let key_only = NodeQuery {
        type_id: Some(1),
        keys: vec!["bob".to_string(), "alice".to_string(), "alice".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&key_only).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob, carol, other_type], &key_only)
    );
    let key_only_plan = engine.explain_node_query(&key_only).unwrap();
    assert_eq!(key_only_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&key_only_plan, vec![QueryPlanNode::KeyLookup]);

    let key_preferred = NodeQuery {
        type_id: Some(1),
        ids: vec![alice, bob, carol],
        keys: vec!["bob".to_string()],
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&key_preferred).unwrap().items,
        oracle_query_ids(&engine, &[alice, bob, carol, other_type], &key_preferred)
    );
    let key_preferred_plan = engine.explain_node_query(&key_preferred).unwrap();
    assert_eq!(key_preferred_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&key_preferred_plan, vec![QueryPlanNode::KeyLookup]);

    engine.close().unwrap();
}

// --- indexed candidate-source planning ---

#[test]
fn test_query_intersects_ready_equality_indexes_against_oracle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("silver".to_string())),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        1,
        "c",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        1,
        "d",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    let tier = engine
        .ensure_node_property_index(1, "tier", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, tier.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(
        &plan,
        vec![
            QueryPlanNode::PropertyEqualityIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_equality_and_range_indexes_against_oracle() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(20)),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        1,
        "c",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        1,
        "d",
        &[
            ("status", PropValue::String("active".to_string())),
            ("score", PropValue::Int(40)),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(20))),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_includes_input_nodes(
        &plan,
        &[
            QueryPlanNode::PropertyRangeIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_equality_equality_and_range_indexes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(10)),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        1,
        "c",
        &[
            ("status", PropValue::String("active".to_string())),
            ("tier", PropValue::String("silver".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        1,
        "d",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("tier", PropValue::String("gold".to_string())),
            ("score", PropValue::Int(30)),
        ],
        1.0,
    );

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    let tier = engine
        .ensure_node_property_index(1, "tier", SecondaryIndexKind::Equality)
        .unwrap();
    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, tier.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tier".to_string(),
                value: PropValue::String("gold".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(20))),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(
        &plan,
        vec![
            QueryPlanNode::PropertyRangeIndex,
            QueryPlanNode::PropertyEqualityIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_intersects_timestamp_and_property_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        1,
        "c",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        1,
        "d",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    set_query_node_updated_at(&engine, a, 1_000);
    set_query_node_updated_at(&engine, b, 2_000);
    set_query_node_updated_at(&engine, c, 3_000);
    set_query_node_updated_at(&engine, d, 2_500);

    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::UpdatedAtRange {
                lower_ms: Some(1_500),
                upper_ms: Some(2_500),
            },
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            },
        ],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, c, d], &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_includes_input_nodes(
        &plan,
        &[
            QueryPlanNode::TimestampIndex,
            QueryPlanNode::PropertyEqualityIndex,
        ],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_ready_index_sources_match_oracle_across_storage_states() {
    fn lifecycle_query() -> NodeQuery {
        query_ids(
            Some(1),
            vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                    upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
                },
                NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(i64::MIN),
                    upper_ms: Some(i64::MAX),
                },
            ],
            false,
        )
    }

    fn insert_lifecycle_segment_nodes(engine: &DatabaseEngine) -> Vec<u64> {
        vec![
            insert_query_node(
                engine,
                1,
                "a",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(10)),
                ],
                1.0,
            ),
            insert_query_node(
                engine,
                1,
                "b",
                &[
                    ("status", PropValue::String("inactive".to_string())),
                    ("score", PropValue::Int(10)),
                ],
                1.0,
            ),
            insert_query_node(
                engine,
                1,
                "c",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(30)),
                ],
                1.0,
            ),
            insert_query_node(
                engine,
                1,
                "d",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(30)),
                ],
                1.0,
            ),
        ]
    }

    fn insert_lifecycle_active_nodes(engine: &DatabaseEngine) -> Vec<u64> {
        vec![
            insert_query_node(
                engine,
                1,
                "e",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(12)),
                ],
                1.0,
            ),
            insert_query_node(
                engine,
                1,
                "f",
                &[
                    ("status", PropValue::String("active".to_string())),
                    ("score", PropValue::Int(50)),
                ],
                1.0,
            ),
        ]
    }

    fn ensure_lifecycle_indexes(engine: &DatabaseEngine) {
        let status = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        let score = engine
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
    }

    fn assert_tiny_lifecycle_query_uses_fallback(
        engine: &DatabaseEngine,
        all_ids: &[u64],
        query: &NodeQuery,
    ) {
        assert_eq!(
            engine.query_node_ids(query).unwrap().items,
            oracle_query_ids(engine, all_ids, query)
        );
        let plan = engine.explain_node_query(query).unwrap();
        assert_eq!(plan.warnings, vec![QueryPlanWarning::UsingFallbackScan]);
        assert_plan_input_nodes(&plan, vec![QueryPlanNode::FallbackTypeScan]);
    }

    let dir = TempDir::new().unwrap();
    let query = lifecycle_query();

    {
        let db_path = dir.path().join("memtable-only");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("mixed");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        engine.flush().unwrap();
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("compacted-reopened");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut all_ids = insert_lifecycle_segment_nodes(&engine);
        engine.flush().unwrap();
        all_ids.extend(insert_lifecycle_active_nodes(&engine));
        engine.flush().unwrap();
        ensure_lifecycle_indexes(&engine);
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.compact().unwrap().unwrap();
        assert_tiny_lifecycle_query_uses_fallback(&engine, &all_ids, &query);
        engine.close().unwrap();

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_tiny_lifecycle_query_uses_fallback(&reopened, &all_ids, &query);
        reopened.close().unwrap();
    }
}

#[test]
fn test_query_selective_ready_indexes_match_oracle_across_storage_states() {
    fn selective_query() -> NodeQuery {
        query_ids(
            Some(1),
            vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("target".to_string()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
                    upper: Some(PropertyRangeBound::Included(PropValue::Int(15))),
                },
                NodeFilterExpr::UpdatedAtRange {
                    lower_ms: Some(1_000),
                    upper_ms: Some(1_010),
                },
            ],
            false,
        )
    }

    fn ensure_selective_indexes(engine: &DatabaseEngine) {
        let status = engine
            .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
            .unwrap();
        let score = engine
            .ensure_node_property_index(
                1,
                "score",
                SecondaryIndexKind::Range {
                    domain: SecondaryIndexRangeDomain::Int,
                },
            )
            .unwrap();
        wait_for_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(engine, status.index_id, SecondaryIndexState::Ready);
        wait_for_published_property_index_state(engine, score.index_id, SecondaryIndexState::Ready);
    }

    fn insert_selective_nodes(engine: &DatabaseEngine, start: usize, count: usize) -> Vec<u64> {
        let mut ids = Vec::with_capacity(count);
        for index in start..start + count {
            let selected = index % 64 == 0;
            let node_id = insert_query_node(
                engine,
                1,
                &format!("selective-{index}"),
                &[
                    (
                        "status",
                        PropValue::String(if selected { "target" } else { "other" }.to_string()),
                    ),
                    (
                        "score",
                        PropValue::Int(if selected { 10 } else { 1_000 + index as i64 }),
                    ),
                ],
                1.0,
            );
            set_query_node_updated_at(
                engine,
                node_id,
                if selected { 1_005 } else { 10_000 + index as i64 },
            );
            ids.push(node_id);
        }
        ids
    }

    fn assert_selective_indexes_match_oracle(
        engine: &DatabaseEngine,
        all_ids: &[u64],
        query: &NodeQuery,
    ) {
        assert_eq!(
            engine.query_node_ids(query).unwrap().items,
            oracle_query_ids(engine, all_ids, query)
        );
        let plan = engine.explain_node_query(query).unwrap();
        assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
        assert_plan_includes_input_nodes(
            &plan,
            &[
                QueryPlanNode::PropertyEqualityIndex,
                QueryPlanNode::PropertyRangeIndex,
                QueryPlanNode::TimestampIndex,
            ],
        );
    }

    let dir = TempDir::new().unwrap();
    let query = selective_query();

    {
        let db_path = dir.path().join("memtable-only-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let all_ids = insert_selective_nodes(&engine, 0, 512);
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("mixed-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let mut all_ids = insert_selective_nodes(&engine, 0, 256);
        engine.flush().unwrap();
        all_ids.extend(insert_selective_nodes(&engine, 256, 256));
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();
    }

    {
        let db_path = dir.path().join("compacted-reopened-selective");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        ensure_selective_indexes(&engine);
        let mut all_ids = insert_selective_nodes(&engine, 0, 256);
        engine.flush().unwrap();
        all_ids.extend(insert_selective_nodes(&engine, 256, 256));
        engine.flush().unwrap();
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.compact().unwrap().unwrap();
        assert_selective_indexes_match_oracle(&engine, &all_ids, &query);
        engine.close().unwrap();

        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_selective_indexes_match_oracle(&reopened, &all_ids, &query);
        reopened.close().unwrap();
    }
}

#[test]
fn test_query_bounded_range_uses_index_and_broad_sources_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for i in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n{i}"),
            props: query_test_props(&[
                ("score", PropValue::Int(i as i64)),
                (
                    "status",
                    PropValue::String(if i == 0 { "needle" } else { "other" }.to_string()),
                ),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
    let score = engine
        .ensure_node_property_index(
            1,
            "score",
            SecondaryIndexKind::Range {
                domain: SecondaryIndexRangeDomain::Int,
            },
        )
        .unwrap();
    let status = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, score.index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, status.index_id, SecondaryIndexState::Ready);

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let range_lower = PropertyRangeBound::Included(PropValue::Int(0));
        let range_upper =
            PropertyRangeBound::Included(PropValue::Int(QUERY_RANGE_CANDIDATE_CAP as i64));
        let (range_candidates, followup) = published
            .view
            .ready_range_candidate_ids(
                score.index_id,
                SecondaryIndexRangeDomain::Int,
                Some(&range_lower),
                Some(&range_upper),
                QUERY_RANGE_CANDIDATE_CAP + 1,
            )
            .unwrap();
        assert!(followup.is_none());
        assert_eq!(
            range_candidates.unwrap().len(),
            QUERY_RANGE_CANDIDATE_CAP + 1
        );

        let timestamp_candidates = published
            .view
            .timestamp_candidate_ids(1, i64::MIN, i64::MAX, QUERY_RANGE_CANDIDATE_CAP + 1)
            .unwrap();
        assert_eq!(timestamp_candidates.len(), QUERY_RANGE_CANDIDATE_CAP + 1);
    }

    let bounded = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(12))),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&bounded).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &bounded)
    );
    let bounded_plan = engine.explain_node_query(&bounded).unwrap();
    assert_eq!(bounded_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&bounded_plan, vec![QueryPlanNode::PropertyRangeIndex]);

    let broad_range = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(
                QUERY_RANGE_CANDIDATE_CAP as i64,
            ))),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&broad_range).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_range)
    );
    let broad_range_plan = engine.explain_node_query(&broad_range).unwrap();
    assert_eq!(
        broad_range_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&broad_range_plan, vec![QueryPlanNode::FallbackTypeScan]);

    let broad_timestamp = query_ids(
        Some(1),
        vec![NodeFilterExpr::UpdatedAtRange {
            lower_ms: Some(i64::MIN),
            upper_ms: Some(i64::MAX),
        }],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&broad_timestamp).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_timestamp)
    );
    let broad_timestamp_plan = engine.explain_node_query(&broad_timestamp).unwrap();
    assert_eq!(
        broad_timestamp_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::TimestampCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(
        &broad_timestamp_plan,
        vec![QueryPlanNode::FallbackTypeScan],
    );

    let broad_or = NodeQuery {
        type_id: Some(1),
        filter: Some(NodeFilterExpr::Or(vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("needle".to_string()),
            },
            NodeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(
                    QUERY_RANGE_CANDIDATE_CAP as i64,
                ))),
            },
        ])),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&broad_or).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &broad_or)
    );
    let broad_or_plan = engine.explain_node_query(&broad_or).unwrap();
    assert_eq!(
        broad_or_plan.warnings,
        vec![
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::RangeCandidateCapExceeded,
            QueryPlanWarning::VerifyOnlyFilter,
            QueryPlanWarning::BooleanBranchFallback,
        ]
    );
    assert_plan_input_nodes(&broad_or_plan, vec![QueryPlanNode::FallbackTypeScan]);

    engine.close().unwrap();
}

#[test]
fn test_query_missing_building_and_failed_indexes_fallback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        1,
        "b",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b], &query)
    );
    let missing_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        missing_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&missing_plan, vec![QueryPlanNode::FallbackTypeScan]);

    let (build_ready_rx, build_release_tx) = engine.set_secondary_index_build_pause();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    build_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let building_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        building_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&building_plan, vec![QueryPlanNode::FallbackTypeScan]);
    build_release_tx.send(()).unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    engine.shutdown_secondary_index_worker();
    engine
        .with_runtime_manifest_write(|manifest| {
            let entry = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == info.index_id)
                .unwrap();
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some("forced failure".to_string());
            Ok(())
        })
        .unwrap();
    engine.rebuild_secondary_index_catalog().unwrap();
    let failed_plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        failed_plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_plan_input_nodes(&failed_plan, vec![QueryPlanNode::FallbackTypeScan]);
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_query_ready_sidecar_failure_returns_followup_only_for_selected_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let inactive = insert_query_node(
        &engine,
        1,
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    let planned;
    let normalized;
    let policy_cutoffs;
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        normalized = published.view.normalize_node_query(&query).unwrap();
        planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        policy_cutoffs = published.view.query_policy_cutoffs();
    }
    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );
    assert!(engine.declared_index_runtime_coverage_len_for_test() > 0);

    let seg_dir = segment_dir(&db_path, engine.segments_for_test()[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::remove_file(&sidecar_path).unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert_eq!(page.ids, vec![active]);
        assert_eq!(followups.len(), 1);
    }

    assert_plan_input_nodes(
        &engine.explain_node_query(&query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let segment_id = engine.segments_for_test()[0].segment_id;
    engine
        .reopen_segment_reader_and_rebuild_sources_for_test(segment_id)
        .unwrap();
    let explain = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        explain.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::UsingFallbackScan,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[active, inactive], &query)
    );

    engine.close().unwrap();
}

#[test]
fn test_query_explicit_anchor_does_not_scan_ready_property_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let seg_dir = segment_dir(&db_path, engine.segments_for_test()[0].segment_id);
    let sidecar_path = crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, info.index_id);
    std::fs::remove_file(&sidecar_path).unwrap();

    let mut query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    query.ids = vec![active];

    let (_followup_ready_rx, followup_release_tx) = engine.set_runtime_publish_pause();
    assert_eq!(engine.query_node_ids(&query).unwrap().items, vec![active]);
    assert_eq!(engine.pending_secondary_index_followup_count_for_test(), 0);
    followup_release_tx.send(()).unwrap();

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::ExplicitIds]);

    engine.close().unwrap();
}

#[test]
fn test_query_pagination_does_not_skip_after_rejected_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let reject_first = insert_query_node(
        &engine,
        1,
        "reject-first",
        &[("score", PropValue::Int(1))],
        1.0,
    );
    let accept_first = insert_query_node(
        &engine,
        1,
        "accept-first",
        &[("score", PropValue::Int(10))],
        1.0,
    );
    let reject_second = insert_query_node(
        &engine,
        1,
        "reject-second",
        &[("score", PropValue::Int(2))],
        1.0,
    );
    let accept_second = insert_query_node(
        &engine,
        1,
        "accept-second",
        &[("score", PropValue::Int(20))],
        1.0,
    );

    let mut query = NodeQuery {
        ids: vec![reject_first, accept_first, reject_second, accept_second],
        filter: filter_and![NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(10))),
            upper: None,
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    let first = engine.query_node_ids(&query).unwrap();
    assert_eq!(first.items, vec![accept_first]);
    assert_eq!(first.next_cursor, Some(accept_first));

    query.page.after = first.next_cursor;
    let second = engine.query_node_ids(&query).unwrap();
    assert_eq!(second.items, vec![accept_second]);
    assert_eq!(second.next_cursor, None);

    query.page.after = Some(accept_second);
    let third = engine.query_node_ids(&query).unwrap();
    assert!(third.items.is_empty());
    assert!(third.next_cursor.is_none());

    engine.close().unwrap();
}

// --- full-scan opt-in ---

#[test]
fn test_query_explicit_full_scan_opt_in_and_explain_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(
        &engine,
        1,
        "a",
        &[("tenant", PropValue::String("t1".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        2,
        "b",
        &[("tenant", PropValue::String("t1".to_string()))],
        1.0,
    );
    let _c = insert_query_node(
        &engine,
        3,
        "c",
        &[("tenant", PropValue::String("t2".to_string()))],
        1.0,
    );

    let query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("t1".to_string()),
        }],
        true,
    );
    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &[a, b, _c], &query)
    );

    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(
        plan.warnings,
        vec![
            QueryPlanWarning::MissingReadyIndex,
            QueryPlanWarning::FullScanExplicitlyAllowed,
            QueryPlanWarning::VerifyOnlyFilter,
        ]
    );
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyNodeFilter { .. }
    ));

    engine.close().unwrap();
}

// --- visibility matrix ---

#[test]
fn test_query_scan_parity_after_flush_reopen_overwrite_delete_and_prune() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let keep;
    let deleted;
    let low;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        keep = insert_query_node(
            &engine,
            1,
            "keep",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        deleted = insert_query_node(
            &engine,
            1,
            "delete",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        low = insert_query_node(
            &engine,
            1,
            "low",
            &[("status", PropValue::String("active".to_string()))],
            0.1,
        );
        engine.flush().unwrap();

        insert_query_node(
            &engine,
            1,
            "keep",
            &[("status", PropValue::String("inactive".to_string()))],
            1.0,
        );
        insert_query_node(
            &engine,
            1,
            "keep",
            &[("status", PropValue::String("active".to_string()))],
            1.0,
        );
        engine.delete_node(deleted).unwrap();
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(1),
                },
            )
            .unwrap();

        let query = query_ids(
            Some(1),
            vec![NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }],
            false,
        );
        assert_eq!(
            engine.query_node_ids(&query).unwrap().items,
            oracle_query_ids(&engine, &[keep, deleted, low], &query)
        );
        assert!(engine.get_node(low).unwrap().is_none());
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }],
        false,
    );
    assert_eq!(
        reopened.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&reopened, &[keep, deleted, low], &query)
    );
    reopened.close().unwrap();
}

// --- graph pattern execution ---

#[test]
fn test_query_pattern_validation_rejects_invalid_shapes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let empty = pattern_query(Vec::new(), Vec::new());
    assert!(matches!(
        engine.query_pattern(&empty).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let node_only = pattern_query(vec![pattern_node("a", Some(1), Vec::new())], Vec::new());
    assert!(matches!(
        engine.explain_pattern_query(&node_only).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let duplicate_node = pattern_query(
        vec![
            pattern_node("a", Some(1), Vec::new()),
            pattern_node("a", Some(2), Vec::new()),
        ],
        vec![pattern_edge(Some("e"), "a", "a", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&duplicate_node).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_node_alias = pattern_query(
        vec![pattern_node("", Some(1), Vec::new())],
        vec![pattern_edge(Some("e"), "", "", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&empty_node_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let bad_reference = pattern_query(
        vec![pattern_node("a", Some(1), Vec::new())],
        vec![pattern_edge(Some("e"), "a", "missing", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&bad_reference).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let empty_edge_alias = pattern_query(
        vec![pattern_node("a", Some(1), Vec::new())],
        vec![pattern_edge(Some(""), "a", "a", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&empty_edge_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let duplicate_edge_alias = pattern_query(
        vec![
            pattern_node("a", Some(1), Vec::new()),
            pattern_node("b", Some(2), Vec::new()),
        ],
        vec![
            pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None),
            pattern_edge(Some("e"), "b", "a", Direction::Outgoing, None),
        ],
    );
    assert!(matches!(
        engine.query_pattern(&duplicate_edge_alias).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let zero_limit = GraphPatternQuery {
        limit: 0,
        ..pattern_query(
            vec![
                pattern_node("a", Some(1), Vec::new()),
                pattern_node("b", Some(2), Vec::new()),
            ],
            vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
        )
    };
    assert!(matches!(
        engine.query_pattern(&zero_limit).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let mut key_without_type = pattern_node("a", None, Vec::new());
    key_without_type.keys.push("a".to_string());
    let key_query = pattern_query(
        vec![key_without_type, pattern_node("b", Some(2), Vec::new())],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&key_query).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let disconnected_extra_node = pattern_query(
        vec![
            pattern_node("a", Some(1), Vec::new()),
            pattern_node("b", Some(2), Vec::new()),
            pattern_node("c", Some(3), Vec::new()),
        ],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.query_pattern(&disconnected_extra_node).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let disconnected_components = pattern_query(
        vec![
            pattern_node("a", Some(1), Vec::new()),
            pattern_node("b", Some(2), Vec::new()),
            pattern_node("c", Some(3), Vec::new()),
            pattern_node("d", Some(4), Vec::new()),
        ],
        vec![
            pattern_edge(Some("ab"), "a", "b", Direction::Outgoing, None),
            pattern_edge(Some("cd"), "c", "d", Direction::Outgoing, None),
        ],
    );
    assert!(matches!(
        engine.query_pattern(&disconnected_components).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let unanchored = pattern_query(
        vec![pattern_node("a", None, Vec::new()), pattern_node("b", None, Vec::new())],
        vec![pattern_edge(Some("e"), "a", "b", Direction::Outgoing, None)],
    );
    assert!(matches!(
        engine.explain_pattern_query(&unanchored).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_anchor_selection_uses_type_cardinality() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    for index in 0..8 {
        insert_query_node(&engine, 1, &format!("wide-{index}"), &[], 1.0);
    }
    insert_query_node(&engine, 2, "narrow", &[], 1.0);

    let query = pattern_query(
        vec![
            pattern_node("aaa_wide", Some(1), Vec::new()),
            pattern_node("zzz_narrow", Some(2), Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "aaa_wide",
            "zzz_narrow",
            Direction::Both,
            None,
        )],
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_pattern_query(&query).unwrap();
        let planned = published
            .view
            .plan_normalized_pattern_query(&normalized)
            .unwrap();
        assert_eq!(normalized.nodes[planned.anchor_index].alias, "zzz_narrow");
    }

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_linear_uses_reverse_direction_from_selective_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = insert_query_node(
        &engine,
        1,
        "alice",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let _bob = insert_query_node(
        &engine,
        1,
        "bob",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let acme = insert_query_node(
        &engine,
        2,
        "acme",
        &[("tier", PropValue::String("enterprise".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(alice, acme, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node(
                "person",
                Some(1),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                }],
            ),
            pattern_node_with_ids("company", vec![acme]),
        ],
        vec![pattern_edge(
            Some("works_at"),
            "person",
            "company",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("company", acme), ("person", alice)],
            &[("works_at", edge)]
        )]
    );
    assert!(!result.truncated);

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert_eq!(plan.kind, QueryPlanKind::PatternQuery);
    assert!(matches!(plan.root, QueryPlanNode::PatternExpand { .. }));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_typeless_predicate_target_verifies_after_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
    let good = insert_query_node(
        &engine,
        2,
        "good",
        &[("status", PropValue::String("match".to_string()))],
        1.0,
    );
    let bad = insert_query_node(
        &engine,
        3,
        "bad",
        &[("status", PropValue::String("skip".to_string()))],
        1.0,
    );
    let good_edge = engine
        .upsert_edge(anchor, good, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(anchor, bad, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node(
                "target",
                None,
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("match".to_string()),
                }],
            ),
        ],
        vec![pattern_edge(
            Some("edge"),
            "anchor",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_pattern_query(&query).unwrap();
        let planned = published
            .view
            .plan_normalized_pattern_query(&normalized)
            .unwrap();
        assert_eq!(normalized.nodes[planned.anchor_index].alias, "anchor");
    }

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("anchor", anchor), ("target", good)],
            &[("edge", good_edge)]
        )]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_order_limit_truncated_and_direction_both() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let low = insert_query_node(&engine, 2, "low", &[], 1.0);
    let high = insert_query_node(&engine, 2, "high", &[], 1.0);
    let high_edge = engine
        .upsert_edge(root, high, 10, UpsertEdgeOptions::default())
        .unwrap();
    let low_edge = engine
        .upsert_edge(low, root, 10, UpsertEdgeOptions::default())
        .unwrap();

    let base = pattern_query(
        vec![pattern_node_with_ids("root", vec![root]), pattern_node("target", Some(2), Vec::new())],
        vec![pattern_edge(Some("edge"), "root", "target", Direction::Both, Some(vec![10]))],
    );
    let limited = GraphPatternQuery { limit: 1, ..base.clone() };

    let limited_result = engine.query_pattern(&limited).unwrap();
    assert_eq!(
        limited_result.matches,
        vec![expected_match(
            &[("root", root), ("target", low)],
            &[("edge", low_edge)]
        )]
    );
    assert!(limited_result.truncated);

    let full_result = engine.query_pattern(&base).unwrap();
    assert_eq!(
        full_result.matches,
        vec![
            expected_match(&[("root", root), ("target", low)], &[("edge", low_edge)]),
            expected_match(&[("root", root), ("target", high)], &[("edge", high_edge)]),
        ]
    );
    assert!(!full_result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_high_fanout_limit_keeps_deterministic_top_matches() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let mut targets = Vec::new();
    for index in 0..32 {
        targets.push(insert_query_node(
            &engine,
            2,
            &format!("target-{index:02}"),
            &[],
            1.0,
        ));
    }

    let mut edges_by_target = BTreeMap::new();
    for &target in targets.iter().rev() {
        let edge = engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
        edges_by_target.insert(target, edge);
    }

    let query = GraphPatternQuery {
        limit: 5,
        ..pattern_query(
            vec![
                pattern_node_with_ids("root", vec![root]),
                pattern_node("target", Some(2), Vec::new()),
            ],
            vec![pattern_edge(
                Some("edge"),
                "root",
                "target",
                Direction::Outgoing,
                Some(vec![10]),
            )],
        )
    };

    let result = engine.query_pattern(&query).unwrap();
    let expected: Vec<QueryMatch> = targets
        .iter()
        .take(5)
        .map(|&target| {
            expected_match(
                &[("root", root), ("target", target)],
                &[("edge", edges_by_target[&target])],
            )
        })
        .collect();
    assert_eq!(result.matches, expected);
    assert!(result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_cost_can_choose_larger_lower_expansion_anchor() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let hub = insert_query_node(&engine, 1, "hub", &[], 1.0);
    let mut mids = Vec::new();
    for index in 0..300 {
        let mid = insert_query_node(&engine, 3, &format!("mid-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(hub, mid, 10, UpsertEdgeOptions::default())
            .unwrap();
        mids.push(mid);
    }
    let mut anchors = Vec::new();
    for (index, &mid) in mids.iter().enumerate().take(20) {
        let anchor = insert_query_node(&engine, 2, &format!("anchor-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(mid, anchor, 20, UpsertEdgeOptions::default())
            .unwrap();
        anchors.push(anchor);
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node("small_hub", Some(1), Vec::new()),
            pattern_node("larger_anchor", Some(2), Vec::new()),
            pattern_node("middle", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_to_middle"),
                "small_hub",
                "middle",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("middle_to_anchor"),
                "middle",
                "larger_anchor",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );

    let (anchor_alias, _) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(anchor_alias, "larger_anchor");
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches.len(), anchors.len());
    assert!(!result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_physical_anchor_does_not_change_result_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let hub_low = insert_query_node(&engine, 1, "hub-low", &[], 1.0);
    let hub_high = insert_query_node(&engine, 1, "hub-high", &[], 1.0);
    let mut mids = Vec::new();
    for index in 0..300 {
        mids.push(insert_query_node(
            &engine,
            3,
            &format!("mid-{index:03}"),
            &[],
            1.0,
        ));
    }
    let mut anchors = Vec::new();
    for index in 0..20 {
        anchors.push(insert_query_node(
            &engine,
            2,
            &format!("anchor-{index:02}"),
            &[],
            1.0,
        ));
    }
    for &mid in &mids[..150] {
        engine
            .upsert_edge(hub_low, mid, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    for &mid in &mids[150..] {
        engine
            .upsert_edge(hub_high, mid, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine
        .upsert_edge(mids[0], anchors[19], 20, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(mids[150], anchors[0], 20, UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let mut query = pattern_query(
        vec![
            pattern_node("a_small_hub", Some(1), Vec::new()),
            pattern_node("z_larger_anchor", Some(2), Vec::new()),
            pattern_node("middle", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_to_middle"),
                "a_small_hub",
                "middle",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("middle_to_anchor"),
                "middle",
                "z_larger_anchor",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );
    query.limit = 1;

    let (physical_anchor, sort_anchor, _) =
        planned_pattern_anchor_sort_and_edge_aliases(&engine, &query);
    assert_eq!(physical_anchor, "z_larger_anchor");
    assert_eq!(sort_anchor, "a_small_hub");

    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches.len(), 1);
    assert!(result.truncated);
    assert_eq!(result.matches[0].nodes["a_small_hub"], hub_low);
    assert_eq!(result.matches[0].nodes["z_larger_anchor"], anchors[19]);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_fanout_delays_high_hub_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let low = insert_query_node(&engine, 3, "low", &[], 1.0);
    engine
        .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..128 {
        let target = insert_query_node(&engine, 2, &format!("hub-target-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some(2), Vec::new()),
            pattern_node("low_target", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("zzz_low"));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_mutable_edges_preserve_deterministic_expansion_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let low = insert_query_node(&engine, 3, "low", &[], 1.0);
    engine
        .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..96 {
        let target = insert_query_node(&engine, 2, &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let mutable_target = insert_query_node(&engine, 99, "mutable", &[], 1.0);
    engine
        .upsert_edge(root, mutable_target, 99, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some(2), Vec::new()),
            pattern_node("low_target", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("aaa_hub"));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_target_filter_selectivity_reduces_fanout_cost() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .ensure_node_property_index(2, "status", SecondaryIndexKind::Equality)
        .unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    for index in 0..128 {
        let status = if index == 127 { "selected" } else { "other" };
        let target = insert_query_node(
            &engine,
            2,
            &format!("candidate-{index:03}"),
            &[("status", PropValue::String(status.to_string()))],
            1.0,
        );
        engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    for index in 0..16 {
        let target = insert_query_node(&engine, 3, &format!("low-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, 20, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node(
                "selected_target",
                Some(2),
                vec![NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("selected".to_string()),
                }],
            ),
            pattern_node("low_target", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("zzz_selective"),
                "root",
                "selected_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("aaa_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(
        edge_aliases.first().map(String::as_str),
        Some("zzz_selective")
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_absent_edge_type_uses_complete_zero_fanout() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let missing_target = insert_query_node(&engine, 4, "missing-target", &[], 1.0);
    for index in 0..64 {
        let target = insert_query_node(&engine, 2, &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some(2), Vec::new()),
            pattern_node_with_ids("missing_target", vec![missing_target]),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("zzz_missing"),
                "root",
                "missing_target",
                Direction::Outgoing,
                Some(vec![999]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(
        edge_aliases.first().map(String::as_str),
        Some("zzz_missing")
    );
    let result = engine.query_pattern(&query).unwrap();
    assert!(result.matches.is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_both_bound_constraint_stays_before_unbound_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, 1, "a", &[], 1.0);
    let b = insert_query_node(&engine, 2, "b", &[], 1.0);
    let c = insert_query_node(&engine, 3, "c", &[], 1.0);
    engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, 30, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, a, 11, UpsertEdgeOptions::default())
        .unwrap();
    for index in 0..96 {
        let dummy = insert_query_node(&engine, 4, &format!("dummy-{index:03}"), &[], 1.0);
        engine
            .upsert_edge(b, dummy, 11, UpsertEdgeOptions::default())
            .unwrap();
    }
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![a]),
            pattern_node("b", Some(2), Vec::new()),
            pattern_node("c", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_bind_b"),
                "a",
                "b",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("zzz_constraint"),
                "b",
                "a",
                Direction::Outgoing,
                Some(vec![11]),
            ),
            pattern_edge(
                Some("zzz_unbound"),
                "a",
                "c",
                Direction::Outgoing,
                Some(vec![30]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&engine, &query);
    assert_eq!(edge_aliases[0], "aaa_bind_b");
    assert_eq!(edge_aliases[1], "zzz_constraint");

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_missing_fanout_stats_preserves_deterministic_order() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, 1, "root", &[], 1.0);
        let low = insert_query_node(&engine, 3, "low", &[], 1.0);
        engine
            .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
            .unwrap();
        for index in 0..64 {
            let target = insert_query_node(&engine, 2, &format!("target-{index:02}"), &[], 1.0);
            engine
                .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
                .unwrap();
        }
        engine.flush().unwrap();
        engine.close().unwrap();
    }

    let stats_path = crate::segment_writer::segment_dir(&db_path, 1)
        .join(crate::planner_stats::PLANNER_STATS_FILENAME);
    std::fs::remove_file(stats_path).unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let root = reopened.get_node_by_key(1, "root").unwrap().unwrap().id;
    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some(2), Vec::new()),
            pattern_node("low_target", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("aaa_hub"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("zzz_low"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );

    let (_, edge_aliases) = planned_pattern_anchor_and_edge_aliases(&reopened, &query);
    assert_eq!(edge_aliases.first().map(String::as_str), Some("aaa_hub"));

    reopened.close().unwrap();
}

fn fanout_parity_query(root: u64) -> GraphPatternQuery {
    pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("hub_target", Some(2), Vec::new()),
            pattern_node("low_target", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(
                Some("hub_edge"),
                "root",
                "hub_target",
                Direction::Outgoing,
                Some(vec![10]),
            ),
            pattern_edge(
                Some("low_edge"),
                "root",
                "low_target",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    )
}

fn insert_fanout_parity_tail(engine: &DatabaseEngine, root: u64, start: usize, count: usize) {
    for index in start..start + count {
        let target = insert_query_node(engine, 2, &format!("target-{index:02}"), &[], 1.0);
        engine
            .upsert_edge(root, target, 10, UpsertEdgeOptions::default())
            .unwrap();
    }
}

fn assert_fanout_parity_result(engine: &DatabaseEngine, root: u64, expected_matches: &[QueryMatch]) {
    let query = fanout_parity_query(root);
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(result.matches, expected_matches);
    assert!(!result.truncated);
}

#[test]
fn test_query_pattern_fanout_stats_preserve_results_across_storage_states() {
    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("memtable");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, 1, "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 8);
        let low = insert_query_node(&engine, 3, "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
            .unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key(2, &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, 10)
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("flushed");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, 1, "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 8);
        let low = insert_query_node(&engine, 3, "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
            .unwrap();
        engine.flush().unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key(2, &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, 10)
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("reopened");
        let root = {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let root = insert_query_node(&engine, 1, "root", &[], 1.0);
            insert_fanout_parity_tail(&engine, root, 0, 8);
            let low = insert_query_node(&engine, 3, "low", &[], 1.0);
            engine
                .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
                .unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
            root
        };
        let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let low = reopened.get_node_by_key(3, "low").unwrap().unwrap().id;
        let low_edge = reopened
            .get_edge_by_triple(root, low, 20)
            .unwrap()
            .unwrap()
            .id;
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = reopened
                    .get_node_by_key(2, &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = reopened
                    .get_edge_by_triple(root, target, 10)
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&reopened, root, &expected);
        reopened.close().unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("compacted");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let root = insert_query_node(&engine, 1, "root", &[], 1.0);
        insert_fanout_parity_tail(&engine, root, 0, 4);
        engine.flush().unwrap();
        insert_fanout_parity_tail(&engine, root, 4, 4);
        let low = insert_query_node(&engine, 3, "low", &[], 1.0);
        let low_edge = engine
            .upsert_edge(root, low, 20, UpsertEdgeOptions::default())
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap().unwrap();
        let expected: Vec<_> = (0..8)
            .map(|index| {
                let target = engine
                    .get_node_by_key(2, &format!("target-{index:02}"))
                    .unwrap()
                    .unwrap()
                    .id;
                let hub_edge = engine
                    .get_edge_by_triple(root, target, 10)
                    .unwrap()
                    .unwrap()
                    .id;
                expected_match(
                    &[("hub_target", target), ("low_target", low), ("root", root)],
                    &[("hub_edge", hub_edge), ("low_edge", low_edge)],
                )
            })
            .collect();
        assert_fanout_parity_result(&engine, root, &expected);
        engine.close().unwrap();
    }
}

#[test]
fn test_query_pattern_distinct_edge_aliases_may_share_edge_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, 1, "a", &[], 1.0);
    let b = insert_query_node(&engine, 2, "b", &[], 1.0);
    let edge = engine
        .upsert_edge(a, b, 10, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![pattern_node_with_ids("a", vec![a]), pattern_node("b", Some(2), Vec::new())],
        vec![
            pattern_edge(Some("first"), "a", "b", Direction::Outgoing, Some(vec![10])),
            pattern_edge(Some("second"), "a", "b", Direction::Outgoing, Some(vec![10])),
        ],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("a", a), ("b", b)],
            &[("first", edge), ("second", edge)]
        )]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_branching_distinct_aliases_and_no_match() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let root = insert_query_node(&engine, 1, "root", &[], 1.0);
    let left = insert_query_node(&engine, 2, "left", &[], 1.0);
    let right = insert_query_node(&engine, 3, "right", &[], 1.0);
    let shared = insert_query_node(&engine, 4, "shared", &[], 1.0);
    let left_edge = engine
        .upsert_edge(root, left, 10, UpsertEdgeOptions::default())
        .unwrap();
    let right_edge = engine
        .upsert_edge(root, right, 20, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(root, shared, 30, UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(root, shared, 40, UpsertEdgeOptions::default())
        .unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("left", Some(2), Vec::new()),
            pattern_node("right", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(Some("left_edge"), "root", "left", Direction::Outgoing, Some(vec![10])),
            pattern_edge(
                Some("right_edge"),
                "root",
                "right",
                Direction::Outgoing,
                Some(vec![20]),
            ),
        ],
    );
    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(
            &[("left", left), ("right", right), ("root", root)],
            &[("left_edge", left_edge), ("right_edge", right_edge)]
        )]
    );

    let distinct_alias_query = pattern_query(
        vec![
            pattern_node_with_ids("root", vec![root]),
            pattern_node("x", Some(4), Vec::new()),
            pattern_node("y", Some(4), Vec::new()),
        ],
        vec![
            pattern_edge(Some("x_edge"), "root", "x", Direction::Outgoing, Some(vec![30])),
            pattern_edge(Some("y_edge"), "root", "y", Direction::Outgoing, Some(vec![40])),
        ],
    );
    assert!(engine
        .query_pattern(&distinct_alias_query)
        .unwrap()
        .matches
        .is_empty());

    let no_match_query = pattern_query(
        vec![pattern_node_with_ids("root", vec![root]), pattern_node("missing", Some(99), Vec::new())],
        vec![pattern_edge(None, "root", "missing", Direction::Outgoing, None)],
    );
    assert!(engine
        .query_pattern(&no_match_query)
        .unwrap()
        .matches
        .is_empty());

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_cycle_closing_edge_and_self_loop() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, 1, "a", &[], 1.0);
    let b = insert_query_node(&engine, 2, "b", &[], 1.0);
    let c = insert_query_node(&engine, 3, "c", &[], 1.0);
    let ab = engine.upsert_edge(a, b, 10, Default::default()).unwrap();
    let bc = engine.upsert_edge(b, c, 10, Default::default()).unwrap();
    let ca = engine.upsert_edge(c, a, 10, Default::default()).unwrap();
    let aa = engine.upsert_edge(a, a, 99, Default::default()).unwrap();

    let cycle = pattern_query(
        vec![
            pattern_node_with_ids("a", vec![a]),
            pattern_node("b", Some(2), Vec::new()),
            pattern_node("c", Some(3), Vec::new()),
        ],
        vec![
            pattern_edge(Some("ab"), "a", "b", Direction::Outgoing, Some(vec![10])),
            pattern_edge(Some("bc"), "b", "c", Direction::Outgoing, Some(vec![10])),
            pattern_edge(Some("ca"), "c", "a", Direction::Outgoing, Some(vec![10])),
        ],
    );
    assert_eq!(
        engine.query_pattern(&cycle).unwrap().matches,
        vec![expected_match(
            &[("a", a), ("b", b), ("c", c)],
            &[("ab", ab), ("bc", bc), ("ca", ca)]
        )]
    );

    let self_loop = pattern_query(
        vec![pattern_node_with_ids("a", vec![a])],
        vec![pattern_edge(Some("loop"), "a", "a", Direction::Outgoing, Some(vec![99]))],
    );
    assert_eq!(
        engine.query_pattern(&self_loop).unwrap().matches,
        vec![expected_match(&[("a", a)], &[("loop", aa)])]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_edge_property_post_filters_and_explain_warning() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = insert_query_node(&engine, 1, "a", &[], 1.0);
    let b = insert_query_node(&engine, 2, "b", &[], 1.0);
    let c = insert_query_node(&engine, 2, "c", &[], 1.0);
    let good = engine
        .upsert_edge(
            a,
            b,
            10,
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("rel", PropValue::String("friend".to_string())),
                    ("score", PropValue::Int(5)),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            a,
            c,
            10,
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("rel", PropValue::String("friend".to_string())),
                    ("score", PropValue::Int(1)),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let mut edge = pattern_edge(Some("e"), "a", "target", Direction::Outgoing, Some(vec![10]));
    edge.property_predicates = vec![
        EdgePostFilterPredicate::PropertyEquals {
            key: "rel".to_string(),
            value: PropValue::String("friend".to_string()),
        },
        EdgePostFilterPredicate::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(3))),
            upper: None,
        },
    ];
    let query = pattern_query(
        vec![pattern_node_with_ids("a", vec![a]), pattern_node("target", Some(2), Vec::new())],
        vec![edge],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("a", a), ("target", b)], &[("e", good)])]
    );

    let plan = engine.explain_pattern_query(&query).unwrap();
    assert!(plan
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(matches!(
        plan.root,
        QueryPlanNode::VerifyEdgePredicates { .. }
    ));

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_visibility_temporal_delete_prune_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    let anchor;
    let keep;
    let hidden;
    let expired;
    let deleted;
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
        keep = insert_query_node(&engine, 2, "keep", &[], 1.0);
        hidden = insert_query_node(&engine, 2, "hidden", &[], 0.1);
        expired = insert_query_node(&engine, 2, "expired", &[], 1.0);
        deleted = insert_query_node(&engine, 2, "deleted", &[], 1.0);

        engine
            .upsert_edge(
                anchor,
                keep,
                10,
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                anchor,
                hidden,
                10,
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .upsert_edge(
                anchor,
                expired,
                10,
                UpsertEdgeOptions {
                    valid_from: Some(0),
                    valid_to: Some(50),
                    ..Default::default()
                },
            )
            .unwrap();
        let deleted_edge = engine
            .upsert_edge(
                anchor,
                deleted,
                10,
                UpsertEdgeOptions {
                    valid_from: Some(100),
                    valid_to: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.delete_edge(deleted_edge).unwrap();
        engine
            .set_prune_policy(
                "low-weight",
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.5),
                    type_id: Some(2),
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let query = GraphPatternQuery {
            at_epoch: Some(150),
            ..pattern_query(
                vec![pattern_node_with_ids("anchor", vec![anchor]), pattern_node("target", Some(2), Vec::new())],
                vec![pattern_edge(None, "anchor", "target", Direction::Outgoing, Some(vec![10]))],
            )
        };
        assert_eq!(
            engine.query_pattern(&query).unwrap().matches,
            vec![expected_match(&[("anchor", anchor), ("target", keep)], &[])]
        );

        let expired_query = GraphPatternQuery {
            at_epoch: Some(400),
            ..query.clone()
        };
        assert!(engine
            .query_pattern(&expired_query)
            .unwrap()
            .matches
            .is_empty());

        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let query = GraphPatternQuery {
        at_epoch: Some(150),
        ..pattern_query(
            vec![pattern_node_with_ids("anchor", vec![anchor]), pattern_node("target", Some(2), Vec::new())],
            vec![pattern_edge(None, "anchor", "target", Direction::Outgoing, Some(vec![10]))],
        )
    };
    assert_eq!(
        reopened.query_pattern(&query).unwrap().matches,
        vec![expected_match(&[("anchor", anchor), ("target", keep)], &[])]
    );
    assert!(reopened.get_node(hidden).unwrap().is_none());
    assert!(reopened.get_node(expired).unwrap().is_some());
    assert!(reopened.get_node(deleted).unwrap().is_some());
    reopened.close().unwrap();
}

#[test]
fn test_query_broad_index_warning_priority_and_selective_source_choice() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for index in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[
                ("status", PropValue::String("inactive".to_string())),
                (
                    "tenant",
                    PropValue::String(if index < 8 { "tiny" } else { "other" }.to_string()),
                ),
                (
                    "cohort",
                    PropValue::String(if index < 512 { "broad" } else { "other" }.to_string()),
                ),
            ]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();

    for key in ["status", "tenant", "cohort"] {
        let info = engine
            .ensure_node_property_index(1, key, SecondaryIndexKind::Equality)
            .unwrap();
        wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);
    }

    let cap_query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("inactive".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("tiny".to_string()),
            },
        ],
        false,
    );
    assert_eq!(
        engine.query_node_ids(&cap_query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &cap_query)
    );
    let cap_plan = engine.explain_node_query(&cap_query).unwrap();
    assert!(cap_plan
        .warnings
        .contains(&QueryPlanWarning::CandidateCapExceeded));
    assert_plan_input_nodes(&cap_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    let broad_skip_query = query_ids(
        Some(1),
        vec![
            NodeFilterExpr::PropertyEquals {
                key: "cohort".to_string(),
                value: PropValue::String("broad".to_string()),
            },
            NodeFilterExpr::PropertyEquals {
                key: "tenant".to_string(),
                value: PropValue::String("tiny".to_string()),
            },
        ],
        false,
    );
    let broad_skip_plan = engine.explain_node_query(&broad_skip_query).unwrap();
    assert_eq!(
        broad_skip_plan.warnings,
        vec![
            QueryPlanWarning::IndexSkippedAsBroad,
            QueryPlanWarning::VerifyOnlyFilter
        ]
    );
    assert_plan_input_nodes(
        &broad_skip_plan,
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    engine.close().unwrap();
}

#[test]
fn test_query_large_explicit_ids_become_membership_check_for_cheaper_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(5_000);
    for index in 0..5_000 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[(
                "tenant",
                PropValue::String(if index < 12 { "tiny" } else { "other" }.to_string()),
            )]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(1, "tenant", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let query = NodeQuery {
        type_id: Some(1),
        ids: all_ids.clone(),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("tiny".to_string()),
        }],
        ..Default::default()
    };

    let capped_query = NodeQuery {
        ids: all_ids[..QUERY_RANGE_CANDIDATE_CAP].to_vec(),
        ..query.clone()
    };
    assert_eq!(
        engine.query_node_ids(&capped_query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &capped_query)
    );
    let capped_plan = engine.explain_node_query(&capped_query).unwrap();
    assert_eq!(capped_plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&capped_plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_large_keys_become_membership_check_for_cheaper_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(50);
    for index in 0..50 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[(
                "tenant",
                PropValue::String(if index < 3 { "tiny" } else { "other" }.to_string()),
            )]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(1, "tenant", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut keys: Vec<String> = (0..5_000)
        .map(|index| format!("missing-{index}"))
        .collect();
    keys.push("n-0".to_string());
    let query = NodeQuery {
        type_id: Some(1),
        keys,
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "tenant".to_string(),
            value: PropValue::String("tiny".to_string()),
        }],
        ..Default::default()
    };

    assert_eq!(
        engine.query_node_ids(&query).unwrap().items,
        oracle_query_ids(&engine, &all_ids, &query)
    );
    let plan = engine.explain_node_query(&query).unwrap();
    assert_eq!(plan.warnings, Vec::<QueryPlanWarning>::new());
    assert_plan_input_nodes(&plan, vec![QueryPlanNode::PropertyEqualityIndex]);

    engine.close().unwrap();
}

#[test]
fn test_query_full_scan_pagination_proves_extra_match_and_skips_tombstone() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let first = insert_query_node(
        &engine,
        1,
        "first",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let deleted = insert_query_node(
        &engine,
        1,
        "deleted",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let last = insert_query_node(
        &engine,
        1,
        "last",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    engine.delete_node(deleted).unwrap();

    let mut query = query_ids(
        None,
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        }],
        true,
    );
    query.page = PageRequest {
        limit: Some(1),
        after: None,
    };

    let page1 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page1.items, vec![first]);
    assert_eq!(page1.next_cursor, Some(first));

    query.page.after = page1.next_cursor;
    let page2 = engine.query_node_ids(&query).unwrap();
    assert_eq!(page2.items, vec![last]);
    assert_eq!(page2.next_cursor, None);

    engine.close().unwrap();
}

#[test]
fn test_query_unknown_selected_index_source_falls_back_when_execution_cap_exceeded() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(QUERY_RANGE_CANDIDATE_CAP + 1);
    for index in 0..=QUERY_RANGE_CANDIDATE_CAP {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let active = insert_query_node(
        &engine,
        1,
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let public_query = NodeQuery {
        type_id: Some(1),
        filter: filter_and![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("inactive".to_string()),
        }],
        page: PageRequest {
            limit: Some(1),
            after: None,
        },
        ..Default::default()
    };

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_node_query(&public_query).unwrap();
        let cap_context = published.view.query_cap_context(&normalized).unwrap();
        let planned = PlannedNodeQuery {
            driver: NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("inactive".to_string()),
                    PlannerEstimate::unknown(),
                )),
            cap_context,
            warnings: Vec::new(),
        };
        let policy_cutoffs = published.view.query_policy_cutoffs();
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert!(followups.is_empty());
        assert_eq!(page.ids.len(), 1);
        assert_eq!(page.next_cursor, page.ids.last().copied());

        let union_query = NodeQuery {
            type_id: Some(1),
            filter: Some(NodeFilterExpr::Or(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("inactive".to_string()),
                },
                NodeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String("active".to_string()),
                },
            ])),
            page: PageRequest {
                limit: Some(1),
                after: None,
            },
            ..Default::default()
        };
        let normalized = published.view.normalize_node_query(&union_query).unwrap();
        let cap_context = published.view.query_cap_context(&normalized).unwrap();
        let planned = PlannedNodeQuery {
            driver: NodePhysicalPlan::union(vec![
                NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("inactive".to_string()),
                    PlannerEstimate::unknown(),
                )),
                NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                    1,
                    info.index_id,
                    "status",
                    &PropValue::String("active".to_string()),
                    PlannerEstimate::upper_bound(1),
                )),
            ]),
            cap_context,
            warnings: Vec::new(),
        };
        let (page, followups) = published
            .view
            .query_node_page_planned(&normalized, &planned, false, policy_cutoffs.as_ref())
            .unwrap();
        assert!(followups.is_empty());
        assert_eq!(page.ids, vec![ids[0]]);
        assert_eq!(page.next_cursor, Some(ids[0]));
        assert_ne!(page.ids, vec![active]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_skips_shadowed_ids_before_cap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(3);
    for index in 0..3 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    for (index, id) in ids.iter().enumerate().take(2) {
        let updated_id = engine
            .upsert_node(
                1,
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, *id);
    }

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (candidate_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(2),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(candidate_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_skips_newer_segment_shadowed_ids_before_cap() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut inputs = Vec::with_capacity(3);
    for index in 0..3 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    for (index, id) in ids.iter().enumerate().take(2) {
        let updated_id = engine
            .upsert_node(
                1,
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, *id);
    }
    engine.flush().unwrap();

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (candidate_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(2),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(candidate_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_limited_equality_read_enforces_raw_posting_cap_for_stale_segments() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut inputs = Vec::new();
    for index in 0..24 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let surviving_old_id = *ids.last().unwrap();
    engine.flush().unwrap();

    for (index, id) in ids.iter().copied().enumerate().take(23) {
        let updated_id = engine
            .upsert_node(
                1,
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, id);
    }

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let (capped_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited_by_raw_postings(
                info.index_id,
                &PropValue::String("old".to_string()),
                8,
            )
            .unwrap();

        assert!(followup.is_none());
        assert!(capped_ids.is_none());

        let (uncapped_ids, followup) = published
            .view
            .ready_equality_candidate_ids_limited(
                info.index_id,
                "status",
                &PropValue::String("old".to_string()),
                Some(9),
            )
            .unwrap();

        assert!(followup.is_none());
        assert_eq!(uncapped_ids.unwrap(), vec![surviving_old_id]);
    }

    engine.close().unwrap();
}

#[test]
fn test_query_stats_backed_equality_materialization_uses_raw_ids_only() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let info = engine
        .ensure_node_property_index(1, "status", SecondaryIndexKind::Equality)
        .unwrap();
    wait_for_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

    let mut inputs = Vec::new();
    for index in 0..24 {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("n-{index}"),
            props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    let all_ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let surviving_old_id = *all_ids.last().unwrap();
    engine.flush().unwrap();

    for (index, id) in all_ids.iter().copied().enumerate().take(23) {
        let updated_id = engine
            .upsert_node(
                1,
                &format!("n-{index}"),
                UpsertNodeOptions {
                    props: query_test_props(&[(
                        "status",
                        PropValue::String("new".to_string()),
                    )]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated_id, id);
    }

    let query = query_ids(
        Some(1),
        vec![NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("old".to_string()),
        }],
        false,
    );

    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();
        let normalized = published.view.normalize_node_query(&query).unwrap();
        let planned = published.view.plan_normalized_node_query(&normalized).unwrap();
        let NodePhysicalPlan::Source(source) = planned.driver else {
            panic!("expected equality source driver");
        };
        assert_eq!(source.kind, NodeQueryCandidateSourceKind::PropertyEqualityIndex);
        assert_eq!(source.estimate.kind, PlannerEstimateKind::StatsEstimated);
        assert!(!source.estimate.can_use_uncapped_equality_materialization());
    }

    engine.reset_query_execution_counters_for_test();
    let result = engine.query_node_ids(&query).unwrap();
    assert_eq!(result.items, vec![surviving_old_id]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.equality_materialization_record_reads, 0);
    assert!(counters.final_verifier_record_reads >= all_ids.len());

    engine.close().unwrap();
}

#[test]
fn test_query_unknown_estimates_use_stable_rank_then_key() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    {
        let (_guard, published) = engine.runtime.published_snapshot().unwrap();

        let mut candidates = vec![
            NodePhysicalPlan::source(PlannedNodeCandidateSource::timestamp_index(
                1,
                0,
                100,
                PlannerEstimate::unknown(),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                1,
                "b",
                &PropValue::String("x".to_string()),
                PlannerEstimate::unknown(),
            )),
            NodePhysicalPlan::source(PlannedNodeCandidateSource::property_equality_index(
                1,
                2,
                "a",
                &PropValue::String("x".to_string()),
                PlannerEstimate::unknown(),
            )),
        ];
        published
            .view
            .sort_physical_plans_by_selectivity(&mut candidates);
        assert_eq!(
            candidates[0].canonical_key(),
            format!(
                "eq:1:a:{}",
                hash_prop_value(&PropValue::String("x".to_string()))
            )
        );
    }

    engine.close().unwrap();
}

#[test]
fn test_query_empty_edge_filter_rejected_and_segment_unnamed_parallel_edges_dedup() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: false,
            ..DbOptions::default()
        },
    )
    .unwrap();

    let source = insert_query_node(&engine, 1, "source", &[], 1.0);
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);

    let invalid = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some(2), Vec::new()),
        ],
        vec![pattern_edge(
            None,
            "source",
            "target",
            Direction::Outgoing,
            Some(Vec::new()),
        )],
    );
    assert!(matches!(
        engine.query_pattern(&invalid).unwrap_err(),
        EngineError::InvalidOperation(_)
    ));

    let edges: Vec<EdgeInput> = (0..16)
        .map(|_| EdgeInput {
            from: source,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some(2), Vec::new()),
        ],
        vec![pattern_edge(
            None,
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );
    let result = engine.query_pattern(&query).unwrap();
    assert_eq!(
        result.matches,
        vec![expected_match(
            &[("source", source), ("target", target)],
            &[]
        )]
    );
    assert!(!result.truncated);

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_segment_named_parallel_edges_preserve_edge_matches() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: false,
            ..DbOptions::default()
        },
    )
    .unwrap();

    let source = insert_query_node(&engine, 1, "source", &[], 1.0);
    let target = insert_query_node(&engine, 2, "target", &[], 1.0);
    let edges = vec![
        EdgeInput {
            from: source,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: source,
            to: target,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
    ];
    let edge_ids = engine.batch_upsert_edges(&edges).unwrap();
    engine.flush().unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("source", vec![source]),
            pattern_node("target", Some(2), Vec::new()),
        ],
        vec![pattern_edge(
            Some("edge"),
            "source",
            "target",
            Direction::Outgoing,
            Some(vec![10]),
        )],
    );

    assert_eq!(
        engine.query_pattern(&query).unwrap().matches,
        vec![
            expected_match(
                &[("source", source), ("target", target)],
                &[("edge", edge_ids[0])]
            ),
            expected_match(
                &[("source", source), ("target", target)],
                &[("edge", edge_ids[1])]
            ),
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_query_pattern_frontier_budget_rejects_pathological_expansion() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let anchor = insert_query_node(&engine, 1, "anchor", &[], 1.0);
    let sink = insert_query_node(&engine, 3, "sink", &[], 1.0);

    let mid_inputs: Vec<NodeInput> = (0..=PATTERN_FRONTIER_BUDGET)
        .map(|index| NodeInput {
            type_id: 2,
            key: format!("mid-{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let mids = engine.batch_upsert_nodes(&mid_inputs).unwrap();
    let edge_inputs: Vec<EdgeInput> = mids
        .iter()
        .map(|&mid| EdgeInput {
            from: anchor,
            to: mid,
            type_id: 10,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edge_inputs).unwrap();

    let query = pattern_query(
        vec![
            pattern_node_with_ids("anchor", vec![anchor]),
            pattern_node("mid", Some(2), Vec::new()),
            pattern_node_with_ids("sink", vec![sink]),
        ],
        vec![
            pattern_edge(None, "anchor", "mid", Direction::Outgoing, Some(vec![10])),
            pattern_edge(None, "mid", "sink", Direction::Outgoing, Some(vec![11])),
        ],
    );

    let error = engine.query_pattern(&query).unwrap_err();
    assert!(matches!(error, EngineError::InvalidOperation(message) if message.contains("frontier exceeded")));

    engine.close().unwrap();
}
