fn prepared_edge_metadata_pull_outcome_fixture(
) -> (
    TempDir,
    DatabaseEngine,
    Arc<ReadView>,
    NormalizedEdgeQuery,
    PlannedEdgeCandidateSource,
) {
    let (dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "prepared-outcome-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "prepared-outcome-to", &[], 1.0);
    engine
        .upsert_edge(
            from,
            to,
            "PREPARED_OUTCOME",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine.flush().unwrap();

    let view = engine.published_state().view.clone();
    let query = EdgeQuery {
        label: Some("PREPARED_OUTCOME".to_string()),
        page: PageRequest {
            limit: Some(8),
            ..Default::default()
        },
        ..Default::default()
    };
    let normalized = view.normalize_edge_query(&query).unwrap();
    let planned = view.plan_normalized_edge_query(&normalized).unwrap();
    let label_source = prepared_edge_metadata_pull_find_source(
        &planned.driver,
        EdgeQueryCandidateSourceKind::EdgeLabelIndex,
    )
    .expect("label query should expose a label source");
    (dir, engine, view, normalized, label_source)
}

fn prepared_edge_metadata_pull_too_broad_source() -> PlannedEdgeCandidateSource {
    PlannedEdgeCandidateSource {
        kind: EdgeQueryCandidateSourceKind::FallbackFullEdgeScan,
        canonical_key: "prepared-test-full-scan".to_string(),
        estimate: PlannerEstimate::exact_cheap(1),
        materialization: EdgeCandidateMaterialization::FallbackFullEdgeScan,
    }
}

fn prepared_edge_metadata_pull_demoted_source(label_id: u32) -> PlannedEdgeCandidateSource {
    PlannedEdgeCandidateSource {
        kind: EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
        canonical_key: "prepared-test-missing-equality".to_string(),
        estimate: PlannerEstimate::exact_cheap(1),
        materialization: EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
            index_id: u64::MAX,
            label_id,
            prop_key: "missing".to_string(),
            value: PropValue::String("missing".to_string()),
            value_hashes: vec![0],
        },
    }
}

#[test]
fn prepared_edge_metadata_pull_eager_outcome_matrix_is_exact() {
    let (_dir, _engine, view, query, ready_source) =
        prepared_edge_metadata_pull_outcome_fixture();
    let too_broad_source = prepared_edge_metadata_pull_too_broad_source();
    let cap_context = EdgeQueryCapContext::default();

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_eager_edge_physical_plan(
            &query,
            cap_context,
            &EdgePhysicalPlan::Source(too_broad_source.clone()),
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEagerMaterializationResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0, "Source TooBroad must release reservations");

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_eager_edge_physical_plan(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(too_broad_source.clone()),
                    EdgePhysicalPlan::Source(ready_source.clone()),
                ],
                mode: EdgeSetOpExecutionMode::Eager,
            },
            &budget,
        )
        .unwrap();
    match result {
        PreparedEagerMaterializationResult::Ready { ids, followups } => {
            assert_eq!(ids.ids.len(), 1);
            assert!(followups.is_empty());
            assert_eq!(budget.live(), 1);
            drop(ids);
        }
        PreparedEagerMaterializationResult::TooBroad { .. } => {
            panic!("Ready + TooBroad Intersect must retain the Ready input")
        }
    }
    assert_eq!(budget.live(), 0);

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_eager_edge_physical_plan(
            &query,
            cap_context,
            &EdgePhysicalPlan::Empty,
            &budget,
        )
        .unwrap();
    match result {
        PreparedEagerMaterializationResult::Ready { ids, followups } => {
            assert!(ids.ids.is_empty());
            assert!(followups.is_empty());
            drop(ids);
        }
        PreparedEagerMaterializationResult::TooBroad { .. } => {
            panic!("Empty must be an exact empty Ready result")
        }
    }
    assert_eq!(budget.live(), 0);

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_eager_edge_physical_plan(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(too_broad_source.clone()),
                    EdgePhysicalPlan::Source(too_broad_source.clone()),
                ],
                mode: EdgeSetOpExecutionMode::Eager,
            },
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEagerMaterializationResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0, "all-TooBroad Intersect must not retain IDs");

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_eager_edge_physical_plan(
            &query,
            cap_context,
            &EdgePhysicalPlan::Union {
                inputs: vec![
                    EdgePhysicalPlan::Source(ready_source),
                    EdgePhysicalPlan::Source(too_broad_source),
                ],
                mode: EdgeSetOpExecutionMode::Eager,
            },
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEagerMaterializationResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0, "Union TooBroad must release Ready inputs");
}

#[test]
fn prepared_edge_metadata_pull_streamed_demotion_outcome_matrix_is_exact() {
    let (_dir, _engine, view, query, ready_source) =
        prepared_edge_metadata_pull_outcome_fixture();
    let demoted_source = prepared_edge_metadata_pull_demoted_source(
        query.label_id.expect("fixture label must resolve"),
    );
    let too_broad_source = prepared_edge_metadata_pull_too_broad_source();
    let cap_context = EdgeQueryCapContext::default();

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_streamed_edge_driver(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(demoted_source.clone()),
                    EdgePhysicalPlan::Source(ready_source.clone()),
                ],
                mode: EdgeSetOpExecutionMode::Streamed,
            },
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEdgeStreamBuildResult::Ready {
            topology: PreparedEdgeStreamTopology::Intersect(_),
            ..
        }
    ));
    assert_eq!(budget.live(), 0, "skipped Demoted input must retain no IDs");

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_streamed_edge_driver(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(ready_source.clone()),
                    EdgePhysicalPlan::Source(too_broad_source.clone()),
                ],
                mode: EdgeSetOpExecutionMode::Streamed,
            },
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEdgeStreamBuildResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0, "TooBroad Intersect must release built streams");

    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_streamed_edge_driver(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(demoted_source.clone()),
                    EdgePhysicalPlan::Source(demoted_source.clone()),
                ],
                mode: EdgeSetOpExecutionMode::Streamed,
            },
            &budget,
        )
        .unwrap();
    assert!(matches!(
        result,
        PreparedEdgeStreamBuildResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0);

    let unreachable_after_too_broad = PlannedEdgeCandidateSource {
        kind: EdgeQueryCandidateSourceKind::ExplicitEdgeIds,
        canonical_key: "prepared-test-unreachable-after-too-broad".to_string(),
        estimate: PlannerEstimate::exact_cheap(1),
        materialization: EdgeCandidateMaterialization::Precomputed(Arc::new(vec![1])),
    };
    let budget = PreparedEdgeIdBudget::default();
    let result = view
        .prepare_streamed_edge_driver(
            &query,
            cap_context,
            &EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(too_broad_source.clone()),
                    EdgePhysicalPlan::Source(unreachable_after_too_broad),
                ],
                mode: EdgeSetOpExecutionMode::Streamed,
            },
            &budget,
        )
        .expect("TooBroad must terminate the intersect before later inputs are built");
    assert!(matches!(
        result,
        PreparedEdgeStreamBuildResult::TooBroad { .. }
    ));
    assert_eq!(budget.live(), 0);

    for plan in [
        EdgePhysicalPlan::Source(demoted_source.clone()),
        EdgePhysicalPlan::Source(too_broad_source.clone()),
        EdgePhysicalPlan::Union {
            inputs: vec![
                EdgePhysicalPlan::Source(ready_source.clone()),
                EdgePhysicalPlan::Source(demoted_source),
            ],
            mode: EdgeSetOpExecutionMode::Streamed,
        },
        EdgePhysicalPlan::Union {
            inputs: vec![
                EdgePhysicalPlan::Source(ready_source),
                EdgePhysicalPlan::Source(too_broad_source),
            ],
            mode: EdgeSetOpExecutionMode::Streamed,
        },
    ] {
        let budget = PreparedEdgeIdBudget::default();
        let result = view
            .prepare_streamed_edge_driver(&query, cap_context, &plan, &budget)
            .unwrap();
        assert!(matches!(
            result,
            PreparedEdgeStreamBuildResult::TooBroad { .. }
        ));
        assert_eq!(budget.live(), 0);
    }
}

fn prepared_edge_budget_equality_source(
    index_id: u64,
    label_id: u32,
    prop_key: &str,
    count: usize,
) -> PlannedEdgeCandidateSource {
    let value = PropValue::Bool(true);
    PlannedEdgeCandidateSource {
        kind: EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
        canonical_key: format!("prepared-budget-equality:{prop_key}"),
        estimate: PlannerEstimate::exact_cheap(count as u64),
        materialization: EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
            index_id,
            label_id,
            prop_key: prop_key.to_string(),
            value_hashes: equality_probe_value_hashes(&value),
            value,
        },
    }
}

fn prepared_edge_budget_normalized_query(
    view: &ReadView,
    label: &str,
    filter: EdgeFilterExpr,
) -> NormalizedEdgeQuery {
    view.normalize_edge_query(&EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(filter),
        page: PageRequest {
            limit: Some(8),
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap()
}

fn prepared_edge_budget_custom_plan(
    view: &ReadView,
    query: &NormalizedEdgeQuery,
    driver: EdgePhysicalPlan,
) -> PlannedEdgeQuery {
    let baseline = view.plan_normalized_edge_query(query).unwrap();
    PlannedEdgeQuery {
        driver,
        execution_mode: EdgeDriverExecutionMode::Streamed,
        cap_context: baseline.cap_context,
        legal_universe_fallback: baseline.legal_universe_fallback,
        warnings: Vec::new(),
        followups: Vec::new(),
    }
}

#[test]
fn prepared_edge_metadata_pull_real_equality_postings_enforce_global_budget_boundary() {
    const IMMUTABLE_POSTINGS: usize = PREPARED_EDGE_RETAINED_ID_CAP / 2;
    const TOTAL_EDGES: usize = PREPARED_EDGE_RETAINED_ID_CAP + 1;

    let (_dir, engine) = query_test_engine();
    let label = "PREPARED_BUDGET_BOUNDARY";
    let label_id = engine.ensure_edge_label(label).unwrap();
    let overflow_index = ensure_ready_edge_property_index(
        &engine,
        label,
        "overflow",
        SecondaryIndexKind::Equality,
    );
    let at_cap_index = ensure_ready_edge_property_index(
        &engine,
        label,
        "at_cap",
        SecondaryIndexKind::Equality,
    );
    let from = insert_query_node(&engine, "Person", "prepared-budget-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "prepared-budget-to", &[], 1.0);

    // Seed the exact-cap and overflow equality groups directly into the core so the
    // boundary regression exercises real active + immutable memtable postings without
    // issuing 65,537 public WAL writes. Every edge belongs to `overflow`; all but the
    // last belong to `at_cap`.
    let first_edge_id = engine.next_edge_id().unwrap();
    {
        let mut core_guard = engine.runtime.core.lock().unwrap();
        let core = core_guard.as_mut().expect("test engine must remain open");
        let mut write_seq = core.engine_seq;
        for ordinal in 0..IMMUTABLE_POSTINGS {
            let mut props = BTreeMap::new();
            props.insert("overflow".to_string(), PropValue::Bool(true));
            props.insert("at_cap".to_string(), PropValue::Bool(true));
            write_seq += 1;
            core.active_memtable().apply_op(
                &WalOp::UpsertEdge(EdgeRecord {
                    id: first_edge_id + ordinal as u64,
                    from,
                    to,
                    label_id,
                    props,
                    created_at: 1,
                    updated_at: 1,
                    weight: 1.0,
                    valid_from: 0,
                    valid_to: i64::MAX,
                    last_write_seq: 0,
                }),
                write_seq,
            );
        }
        core.engine_seq = write_seq;
        core.freeze_memtable().unwrap();

        for ordinal in IMMUTABLE_POSTINGS..TOTAL_EDGES {
            let mut props = BTreeMap::new();
            props.insert("overflow".to_string(), PropValue::Bool(true));
            if ordinal < PREPARED_EDGE_RETAINED_ID_CAP {
                props.insert("at_cap".to_string(), PropValue::Bool(true));
            }
            write_seq += 1;
            core.active_memtable().apply_op(
                &WalOp::UpsertEdge(EdgeRecord {
                    id: first_edge_id + ordinal as u64,
                    from,
                    to,
                    label_id,
                    props,
                    created_at: 1,
                    updated_at: 1,
                    weight: 1.0,
                    valid_from: 0,
                    valid_to: i64::MAX,
                    last_write_seq: 0,
                }),
                write_seq,
            );
        }
        core.engine_seq = write_seq;
        core.next_edge_id = first_edge_id + TOTAL_EDGES as u64;
        core.update_engine_seq_seen();
        core.update_next_edge_id_seen();
        engine
            .runtime
            .publish_locked(core, PublishImpact::RebuildSources, false)
            .unwrap();
    }

    let view = engine.published_state().view.clone();
    let true_hash = equality_probe_value_hashes(&PropValue::Bool(true))[0];
    assert_eq!(view.immutable_epochs.len(), 1);
    assert_eq!(
        view.memtable.secondary_eq_edge_hash_count_at(
            overflow_index,
            true_hash,
            view.snapshot_seq,
        ),
        TOTAL_EDGES - IMMUTABLE_POSTINGS
    );
    assert_eq!(
        view.immutable_epochs[0]
            .memtable
            .secondary_eq_edge_hash_count_at(
                overflow_index,
                true_hash,
                view.snapshot_seq,
            ),
        IMMUTABLE_POSTINGS
    );
    assert_eq!(
        view.memtable.secondary_eq_edge_hash_count_at(
            at_cap_index,
            true_hash,
            view.snapshot_seq,
        ),
        PREPARED_EDGE_RETAINED_ID_CAP - IMMUTABLE_POSTINGS
    );
    assert_eq!(
        view.immutable_epochs[0]
            .memtable
            .secondary_eq_edge_hash_count_at(at_cap_index, true_hash, view.snapshot_seq),
        IMMUTABLE_POSTINGS
    );

    let at_cap_source = prepared_edge_budget_equality_source(
        at_cap_index,
        label_id,
        "at_cap",
        PREPARED_EDGE_RETAINED_ID_CAP,
    );
    let overflow_source = prepared_edge_budget_equality_source(
        overflow_index,
        label_id,
        "overflow",
        TOTAL_EDGES,
    );

    let at_cap_query = prepared_edge_budget_normalized_query(
        &view,
        label,
        EdgeFilterExpr::PropertyEquals {
            key: "at_cap".to_string(),
            value: PropValue::Bool(true),
        },
    );
    reset_prepared_edge_source_test_snapshot();
    let (at_cap_prepared, followups) = view
        .prepare_edge_metadata_pull_source(
            &at_cap_query,
            &at_cap_query,
            prepared_edge_budget_custom_plan(
                &view,
                &at_cap_query,
                EdgePhysicalPlan::Source(at_cap_source.clone()),
            ),
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        at_cap_prepared.prepared_mode(),
        PreparedEdgeSourceMode::StreamedSingle
    );
    assert_eq!(at_cap_prepared.fallback(), PreparedEdgeFallback::None);
    assert_eq!(
        at_cap_prepared.retained_id_units(),
        PREPARED_EDGE_RETAINED_ID_CAP
    );
    assert_eq!(
        prepared_edge_source_test_snapshot().retained_id_units_peak,
        PREPARED_EDGE_RETAINED_ID_CAP
    );
    drop(at_cap_prepared);

    let overflow_query = prepared_edge_budget_normalized_query(
        &view,
        label,
        EdgeFilterExpr::PropertyEquals {
            key: "overflow".to_string(),
            value: PropValue::Bool(true),
        },
    );
    reset_prepared_edge_source_test_snapshot();
    let (overflow_prepared, followups) = view
        .prepare_edge_metadata_pull_source(
            &overflow_query,
            &overflow_query,
            prepared_edge_budget_custom_plan(
                &view,
                &overflow_query,
                EdgePhysicalPlan::Source(overflow_source.clone()),
            ),
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        overflow_prepared.prepared_mode(),
        PreparedEdgeSourceMode::NativeCursor
    );
    assert_eq!(
        overflow_prepared.fallback(),
        PreparedEdgeFallback::PreparedLegalUniverse
    );
    assert_eq!(overflow_prepared.retained_id_units(), 0);
    let overflow_snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(overflow_snapshot.fallback_selections, 1);
    assert_eq!(
        overflow_snapshot.retained_id_units_peak,
        TOTAL_EDGES - IMMUTABLE_POSTINGS,
        "the active posting reservation must be observed and released when the immutable posting would overflow"
    );
    assert!(
        overflow_snapshot.retained_id_units_peak <= PREPARED_EDGE_RETAINED_ID_CAP + 1
    );
    drop(overflow_prepared);

    // The first intersection branch reserves IDs from the active memtable and then
    // demotes when adding the immutable postings would exceed the cap. The second
    // branch can retain the full cap only if the first branch dropped its reservation.
    let intersection_query = prepared_edge_budget_normalized_query(
        &view,
        label,
        EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "overflow".to_string(),
                value: PropValue::Bool(true),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "at_cap".to_string(),
                value: PropValue::Bool(true),
            },
        ]),
    );
    reset_prepared_edge_source_test_snapshot();
    let (sequential_prepared, followups) = view
        .prepare_edge_metadata_pull_source(
            &intersection_query,
            &intersection_query,
            prepared_edge_budget_custom_plan(
                &view,
                &intersection_query,
                EdgePhysicalPlan::Intersect {
                    inputs: vec![
                        EdgePhysicalPlan::Source(overflow_source),
                        EdgePhysicalPlan::Source(at_cap_source),
                    ],
                    mode: EdgeSetOpExecutionMode::Streamed,
                },
            ),
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(
        sequential_prepared.prepared_mode(),
        PreparedEdgeSourceMode::StreamedIntersect
    );
    assert_eq!(sequential_prepared.fallback(), PreparedEdgeFallback::None);
    assert_eq!(
        sequential_prepared.retained_id_units(),
        PREPARED_EDGE_RETAINED_ID_CAP
    );
    let sequential_snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(sequential_snapshot.fallback_selections, 0);
    assert_eq!(
        sequential_snapshot.retained_id_units_peak,
        PREPARED_EDGE_RETAINED_ID_CAP
    );
    assert_eq!(
        sequential_snapshot.streamed_cursor_builds, 3,
        "one cursor from the demoted branch must be dropped before both exact-cap cursors build"
    );
    drop(sequential_prepared);
}
