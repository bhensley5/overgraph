fn prepared_edge_pull_ids(
    prepared: &mut PreparedEdgeMetadataPullSource<'_, '_>,
    pull_limit: usize,
) -> (Vec<u64>, Vec<Vec<u64>>, usize) {
    let mut all = Vec::new();
    let mut batches = Vec::new();
    let mut pulls = 0;
    loop {
        let pull = prepared.pull(pull_limit).unwrap();
        pulls += 1;
        assert!(pull.followups.is_empty());
        let batch = pull
            .metadata
            .into_iter()
            .map(|metadata| metadata.id)
            .collect::<Vec<_>>();
        if pull.has_more {
            assert!(!batch.is_empty(), "nonterminal pulls must make progress");
        }
        all.extend_from_slice(&batch);
        batches.push(batch);
        if !pull.has_more {
            break;
        }
    }
    (all, batches, pulls)
}

fn stateless_edge_metadata_ids_with_cutoffs(
    view: &ReadView,
    normalized: &NormalizedEdgeQuery,
    page_limit: usize,
    policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
) -> Vec<u64> {
    let mut query = normalized.clone();
    query.page.limit = Some(page_limit);
    query.page.after = None;
    let mut ids = Vec::new();
    loop {
        let planned = view.plan_normalized_edge_query(&query).unwrap();
        let (page, followups) = view
            .query_edge_metadata_page_planned(&query, planned, policy_cutoffs)
            .unwrap();
        assert!(followups.is_empty());
        ids.extend(page.metadata.iter().map(|metadata| metadata.id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        query.page.after = Some(next_cursor);
    }
    ids
}

fn prepared_edge_construction_counts(
    snapshot: PreparedEdgeSourceTestSnapshot,
) -> (usize, usize, usize, usize, usize, usize, usize) {
    (
        snapshot.branch_plans,
        snapshot.preparations,
        snapshot.native_cursor_builds,
        snapshot.streamed_cursor_builds,
        snapshot.eager_materializations,
        snapshot.buffered_sorts,
        snapshot.fallback_selections,
    )
}

#[derive(Clone, Copy)]
enum PreparedEdgeConstructionCase {
    Eager,
    StreamedSingle,
    StreamedUnion,
    Buffered,
    Fallback,
}

fn prepared_edge_collect_sources(
    plan: &EdgePhysicalPlan,
    kind: EdgeQueryCandidateSourceKind,
    sources: &mut Vec<PlannedEdgeCandidateSource>,
) {
    match plan {
        EdgePhysicalPlan::Source(source) if source.kind == kind => sources.push(source.clone()),
        EdgePhysicalPlan::BufferedIdSort(input) => {
            prepared_edge_collect_sources(input, kind, sources);
        }
        EdgePhysicalPlan::Intersect { inputs, .. } | EdgePhysicalPlan::Union { inputs, .. } => {
            for input in inputs {
                prepared_edge_collect_sources(input, kind, sources);
            }
        }
        EdgePhysicalPlan::Empty | EdgePhysicalPlan::Source(_) => {}
    }
}

fn run_prepared_edge_construction_case(
    view: &ReadView,
    normalized: &NormalizedEdgeQuery,
    case: PreparedEdgeConstructionCase,
    pull_limit: usize,
) -> (Vec<u64>, PreparedEdgeSourceTestSnapshot, usize) {
    reset_prepared_edge_source_test_snapshot();
    note_prepared_edge_branch_plan();
    let mut planned = view.plan_normalized_edge_query(normalized).unwrap();
    match case {
        PreparedEdgeConstructionCase::Eager => {
            planned.execution_mode = EdgeDriverExecutionMode::Eager;
            let mut equality_sources = Vec::new();
            prepared_edge_collect_sources(
                &planned.driver,
                EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
                &mut equality_sources,
            );
            assert_eq!(equality_sources.len(), 2);
            planned.driver = EdgePhysicalPlan::Union {
                inputs: equality_sources
                    .into_iter()
                    .map(EdgePhysicalPlan::Source)
                    .collect(),
                mode: EdgeSetOpExecutionMode::Eager,
            };
        }
        PreparedEdgeConstructionCase::StreamedSingle => {
            planned.execution_mode = EdgeDriverExecutionMode::Streamed;
            planned.driver = EdgePhysicalPlan::Source(
                prepared_edge_metadata_pull_find_source(
                    &planned.driver,
                    EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
                )
                .expect("streamed-single construction case must have an equality source"),
            );
        }
        PreparedEdgeConstructionCase::StreamedUnion => {
            planned.execution_mode = EdgeDriverExecutionMode::Streamed;
            let mut equality_sources = Vec::new();
            prepared_edge_collect_sources(
                &planned.driver,
                EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
                &mut equality_sources,
            );
            assert_eq!(equality_sources.len(), 2);
            planned.driver = EdgePhysicalPlan::Union {
                inputs: equality_sources
                    .into_iter()
                    .map(EdgePhysicalPlan::Source)
                    .collect(),
                mode: EdgeSetOpExecutionMode::Streamed,
            };
        }
        PreparedEdgeConstructionCase::Buffered => {
            planned.execution_mode = EdgeDriverExecutionMode::Streamed;
            let equality_source = prepared_edge_metadata_pull_find_source(
                &planned.driver,
                EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
            )
            .expect("buffered construction case must have an equality source");
            let range_source = prepared_edge_metadata_pull_find_source(
                &planned.driver,
                EdgeQueryCandidateSourceKind::EdgePropertyRangeIndex,
            )
            .expect("buffered construction case must have a range source");
            planned.driver = EdgePhysicalPlan::Intersect {
                inputs: vec![
                    EdgePhysicalPlan::Source(equality_source),
                    EdgePhysicalPlan::BufferedIdSort(Box::new(EdgePhysicalPlan::Source(
                        range_source,
                    ))),
                ],
                mode: EdgeSetOpExecutionMode::Streamed,
            };
        }
        PreparedEdgeConstructionCase::Fallback => {
            let fallback_source = match &planned.driver {
                EdgePhysicalPlan::Source(source) => source.clone(),
                _ => panic!("fallback construction case must plan a source"),
            };
            planned.execution_mode = EdgeDriverExecutionMode::Streamed;
            planned.driver = EdgePhysicalPlan::Source(fallback_source.clone());
            planned.legal_universe_fallback = Some(fallback_source);
        }
    }
    let (mut prepared, followups) = view
        .prepare_edge_metadata_pull_source(normalized, normalized, planned, None)
        .unwrap();
    assert!(followups.is_empty());
    let expected_mode = match case {
        PreparedEdgeConstructionCase::Eager => PreparedEdgeSourceMode::RetainedIds,
        PreparedEdgeConstructionCase::StreamedSingle => {
            PreparedEdgeSourceMode::StreamedSingle
        }
        PreparedEdgeConstructionCase::StreamedUnion => PreparedEdgeSourceMode::StreamedUnion,
        PreparedEdgeConstructionCase::Buffered => PreparedEdgeSourceMode::StreamedIntersect,
        PreparedEdgeConstructionCase::Fallback => PreparedEdgeSourceMode::NativeCursor,
    };
    assert_eq!(prepared.prepared_mode(), expected_mode);
    let (ids, _, pulls) = prepared_edge_pull_ids(&mut prepared, pull_limit);
    (ids, prepared_edge_source_test_snapshot(), pulls)
}

#[test]
fn prepared_edge_pull_sparse_lookahead_preserves_surplus_without_loss_or_rebuild() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(
        &engine,
        "PreparedSparseSource",
        "prepared-sparse-source",
        &[],
        1.0,
    );
    let retained_ordinals = [0usize, 1, 255, 256, 511, 619];
    let mut expected = Vec::new();
    for ordinal in 0..620 {
        let target = insert_query_node(
            &engine,
            "PreparedSparseTarget",
            &format!("prepared-sparse-target-{ordinal}"),
            &[],
            1.0,
        );
        let retained = retained_ordinals.contains(&ordinal);
        let edge_id = engine
            .upsert_edge(
                source,
                target,
                "CP44_PREPARED_SPARSE",
                UpsertEdgeOptions {
                    props: query_test_props(&[("retained", PropValue::Bool(retained))]),
                    ..Default::default()
                },
            )
            .unwrap();
        if retained {
            expected.push(edge_id);
        }
    }

    let query = EdgeQuery {
        label: Some("CP44_PREPARED_SPARSE".to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "retained".to_string(),
            value: PropValue::Bool(true),
        }),
        page: PageRequest {
            limit: Some(expected.len()),
            after: None,
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let normalized = view.normalize_edge_query(&query).unwrap();

    reset_prepared_edge_source_test_snapshot();
    let (mut one_pull, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    let single = one_pull.pull(expected.len()).unwrap();
    assert!(!single.has_more);
    assert!(single.followups.is_empty());
    assert_eq!(
        single
            .metadata
            .into_iter()
            .map(|metadata| metadata.id)
            .collect::<Vec<_>>(),
        expected
    );
    let one_pull_snapshot = prepared_edge_source_test_snapshot();

    reset_prepared_edge_source_test_snapshot();
    let (mut many_pulls, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&normalized, &normalized, None)
        .unwrap();
    assert!(followups.is_empty());
    let mut actual = Vec::new();
    for expected_batch in expected.chunks(2) {
        let pull = many_pulls.pull(2).unwrap();
        assert!(pull.followups.is_empty());
        let actual_batch = pull
            .metadata
            .into_iter()
            .map(|metadata| metadata.id)
            .collect::<Vec<_>>();
        assert_eq!(actual_batch, expected_batch);
        assert_eq!(pull.has_more, actual.len() + actual_batch.len() < expected.len());
        actual.extend(actual_batch);
    }
    assert_eq!(actual, expected);
    let terminal = many_pulls.pull(2).unwrap();
    assert!(terminal.metadata.is_empty());
    assert!(!terminal.has_more);
    assert!(terminal.followups.is_empty());
    let many_pull_snapshot = prepared_edge_source_test_snapshot();

    assert_eq!(one_pull_snapshot.branch_plans, 1);
    assert_eq!(one_pull_snapshot.preparations, 1);
    assert_eq!(many_pull_snapshot.branch_plans, 1);
    assert_eq!(many_pull_snapshot.preparations, 1);
    assert_eq!(
        many_pull_snapshot.native_cursor_builds,
        one_pull_snapshot.native_cursor_builds
    );
    assert_eq!(
        many_pull_snapshot.streamed_cursor_builds,
        one_pull_snapshot.streamed_cursor_builds
    );
    assert_eq!(
        many_pull_snapshot.eager_materializations,
        one_pull_snapshot.eager_materializations
    );
    assert_eq!(many_pull_snapshot.buffered_sorts, one_pull_snapshot.buffered_sorts);
    assert_eq!(
        many_pull_snapshot.fallback_selections,
        one_pull_snapshot.fallback_selections
    );
    assert_eq!(
        many_pull_snapshot.retained_id_units_peak,
        one_pull_snapshot.retained_id_units_peak
    );
    assert!(many_pull_snapshot.verification_endpoint_cache_units_peak
        <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP + 2 * QUERY_VERIFY_CHUNK);
    assert_eq!(
        many_pull_snapshot.verification_endpoint_visibility_resolutions,
        one_pull_snapshot.verification_endpoint_visibility_resolutions,
        "pull boundaries must not re-resolve persistent endpoint answers"
    );
    assert!(
        many_pull_snapshot.verification_endpoint_cache_retained_peak
            <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP
    );
    assert!(
        many_pull_snapshot.verification_endpoint_cache_transient_peak
            <= 2 * QUERY_VERIFY_CHUNK
    );

    let bounded_query = EdgeQuery {
        label: query.label.clone(),
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "retained".to_string(),
                value: PropValue::Bool(true),
            },
            EdgeFilterExpr::IdRange {
                lower: Some(expected[1]),
                upper: Some(expected[4]),
                lower_inclusive: false,
                upper_inclusive: true,
            },
        ])),
        page: PageRequest {
            limit: Some(2),
            after: Some(expected[0]),
        },
        ..Default::default()
    };
    let bounded = view.normalize_edge_query(&bounded_query).unwrap();
    let (mut bounded_prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&bounded, &bounded, None)
        .unwrap();
    assert!(followups.is_empty());
    let (bounded_actual, bounded_batches, _) = prepared_edge_pull_ids(&mut bounded_prepared, 2);
    assert_eq!(bounded_actual, expected[2..=4]);
    assert_eq!(bounded_batches, vec![expected[2..4].to_vec(), vec![expected[4]]]);

    let mut exhausted_query = bounded_query;
    exhausted_query.page.after = Some(u64::MAX);
    let exhausted = view.normalize_edge_query(&exhausted_query).unwrap();
    let (mut exhausted_prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&exhausted, &exhausted, None)
        .unwrap();
    assert!(followups.is_empty());
    let exhausted_pull = exhausted_prepared.pull(2).unwrap();
    assert!(exhausted_pull.metadata.is_empty());
    assert!(!exhausted_pull.has_more);
}

#[test]
fn prepared_edge_pull_policy_cutoff_parity_across_many_pulls() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let source = insert_query_node(
        &engine,
        "PreparedPolicySource",
        "prepared-policy-source",
        &[],
        1.0,
    );
    let mut endpoint_invisible_edges = Vec::new();
    let mut edge_invisible_edges = Vec::new();
    for ordinal in 0..14 {
        let endpoint_is_invisible = ordinal % 3 == 1;
        let edge_is_invisible = ordinal % 5 == 2;
        let target = insert_query_node(
            &engine,
            "PreparedPolicyTarget",
            &format!("prepared-policy-target-{ordinal}"),
            &[],
            if endpoint_is_invisible { 0.25 } else { 1.0 },
        );
        let edge_id = engine
            .upsert_edge(
                source,
                target,
                "CP44_PREPARED_POLICY",
                UpsertEdgeOptions {
                    valid_from: Some(if edge_is_invisible { 200 } else { 0 }),
                    valid_to: Some(if edge_is_invisible { 300 } else { i64::MAX }),
                    ..Default::default()
                },
            )
            .unwrap();
        if endpoint_is_invisible {
            endpoint_invisible_edges.push(edge_id);
        }
        if edge_is_invisible {
            edge_invisible_edges.push(edge_id);
        }
    }
    engine
        .set_prune_policy(
            "cp44-prepared-low-weight-endpoint",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("PreparedPolicyTarget".to_string()),
            },
        )
        .unwrap();

    engine.flush().unwrap();

    let planning_query = EdgeQuery {
        label: Some("CP44_PREPARED_POLICY".to_string()),
        page: PageRequest {
            limit: Some(8),
            after: None,
        },
        ..Default::default()
    };
    let execution_query = EdgeQuery {
        filter: Some(EdgeFilterExpr::ValidAt { epoch_ms: 100 }),
        ..planning_query.clone()
    };
    let view = engine.published_state().view.clone();
    let planning = view.normalize_edge_query(&planning_query).unwrap();
    let execution = view.normalize_edge_query(&execution_query).unwrap();
    let cutoffs = view.query_policy_cutoffs().unwrap();
    let expected = stateless_edge_metadata_ids_with_cutoffs(&view, &execution, 2, Some(&cutoffs));
    assert!(expected.len() >= 6);
    assert!(
        endpoint_invisible_edges
            .iter()
            .all(|edge_id| !expected.contains(edge_id))
    );
    assert!(
        edge_invisible_edges
            .iter()
            .all(|edge_id| !expected.contains(edge_id))
    );

    reset_prepared_edge_source_test_snapshot();
    let (mut prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(
            &planning,
            &execution,
            Some(&cutoffs),
        )
        .unwrap();
    assert!(followups.is_empty());
    let (actual, batches, pulls) = prepared_edge_pull_ids(&mut prepared, 2);
    assert_eq!(actual, expected);
    assert!(pulls >= 3);
    assert!(batches[..batches.len() - 1]
        .iter()
        .all(|batch| !batch.is_empty()));

    let snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(snapshot.branch_plans, 1);
    assert_eq!(snapshot.preparations, 1);
    assert_eq!(snapshot.native_cursor_builds, 2);
    assert_eq!(snapshot.streamed_cursor_builds, 0);
    assert_eq!(snapshot.eager_materializations, 0);
    assert_eq!(snapshot.buffered_sorts, 0);
    assert_eq!(snapshot.fallback_selections, 0);
    assert_eq!(snapshot.retained_id_units_peak, 0);
    assert!(snapshot.verification_endpoint_cache_units_peak
        <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP + 2 * QUERY_VERIFY_CHUNK);
    assert!(
        snapshot.verification_endpoint_cache_retained_peak
            <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP
    );
    assert!(
        snapshot.verification_endpoint_cache_transient_peak <= 2 * QUERY_VERIFY_CHUNK
    );
    let warm_endpoint_resolutions = snapshot.verification_endpoint_visibility_resolutions;

    drop(prepared);
    drop(cutoffs);
    drop(view);
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let reopened_view = reopened.published_state().view.clone();
    let reopened_planning = reopened_view.normalize_edge_query(&planning_query).unwrap();
    let reopened_execution = reopened_view.normalize_edge_query(&execution_query).unwrap();
    let reopened_cutoffs = reopened_view.query_policy_cutoffs().unwrap();
    let reopened_expected = stateless_edge_metadata_ids_with_cutoffs(
        &reopened_view,
        &reopened_execution,
        2,
        Some(&reopened_cutoffs),
    );
    assert_eq!(reopened_expected, expected);
    reset_prepared_edge_source_test_snapshot();
    let (mut reopened_prepared, followups) = reopened_view
        .plan_and_prepare_edge_metadata_pull_source_for_test(
            &reopened_planning,
            &reopened_execution,
            Some(&reopened_cutoffs),
        )
        .unwrap();
    assert!(followups.is_empty());
    let (reopened_actual, _, reopened_pulls) =
        prepared_edge_pull_ids(&mut reopened_prepared, 2);
    assert_eq!(reopened_actual, reopened_expected);
    assert!(reopened_pulls >= 3);
    let reopened_snapshot = prepared_edge_source_test_snapshot();
    assert_eq!(
        reopened_snapshot.verification_endpoint_visibility_resolutions,
        warm_endpoint_resolutions
    );
    assert!(
        reopened_snapshot.verification_endpoint_cache_retained_peak
            <= PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP
    );
    assert!(
        reopened_snapshot.verification_endpoint_cache_transient_peak
            <= 2 * QUERY_VERIFY_CHUNK
    );
}

#[test]
fn prepared_edge_pull_consumes_u64_max_and_stops_above_end_bound_before_verification() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "PreparedMax", "prepared-max-from", &[], 1.0);
    let to = insert_query_node(&engine, "PreparedMax", "prepared-max-to", &[], 1.0);
    let label = "CP44_PREPARED_MAX";
    let label_id = engine.ensure_edge_label(label).unwrap();
    let candidate_ids = [100u64, 200, u64::MAX];
    engine
        .with_core_mut(|core| {
            let mut write_seq = core.engine_seq;
            for edge_id in candidate_ids {
                write_seq += 1;
                core.active_memtable().apply_op(
                    &WalOp::UpsertEdge(EdgeRecord {
                        id: edge_id,
                        from,
                        to,
                        label_id,
                        props: BTreeMap::new(),
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
            Ok(())
        })
        .unwrap();

    let view = engine.published_state().view.clone();
    let max_query = EdgeQuery {
        label: Some(label.to_string()),
        page: PageRequest {
            limit: Some(4),
            after: None,
        },
        ..Default::default()
    };
    let max_normalized = view.normalize_edge_query(&max_query).unwrap();
    engine.reset_query_execution_counters_for_test();
    let (mut max_prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(
            &max_normalized,
            &max_normalized,
            None,
        )
        .unwrap();
    assert!(followups.is_empty());
    let max_pull = max_prepared.pull(4).unwrap();
    assert_eq!(
        max_pull
            .metadata
            .iter()
            .map(|metadata| metadata.id)
            .collect::<Vec<_>>(),
        candidate_ids
    );
    assert!(!max_pull.has_more);
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .edge_verifier_metadata_lookup_ids,
        candidate_ids.len()
    );
    let repeated = max_prepared.pull(4).unwrap();
    assert!(repeated.metadata.is_empty());
    assert!(!repeated.has_more);
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .edge_verifier_metadata_lookup_ids,
        candidate_ids.len(),
        "the terminal u64::MAX candidate must be consumed and verified exactly once"
    );

    let bounded_query = EdgeQuery {
        filter: Some(EdgeFilterExpr::IdRange {
            lower: Some(100),
            upper: Some(200),
            lower_inclusive: true,
            upper_inclusive: true,
        }),
        ..max_query
    };
    let bounded = view.normalize_edge_query(&bounded_query).unwrap();
    engine.reset_query_execution_counters_for_test();
    let (mut bounded_prepared, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&bounded, &bounded, None)
        .unwrap();
    assert!(followups.is_empty());
    let bounded_pull = bounded_prepared.pull(4).unwrap();
    assert_eq!(
        bounded_pull
            .metadata
            .iter()
            .map(|metadata| metadata.id)
            .collect::<Vec<_>>(),
        vec![100, 200]
    );
    assert!(!bounded_pull.has_more);
    assert_eq!(
        engine
            .query_execution_counter_snapshot_for_test()
            .edge_verifier_metadata_lookup_ids,
        2,
        "the candidate above end_bound must exhaust without entering verification"
    );
}

#[test]
fn prepared_edge_pull_source_modes_match_stateless_and_construct_once_across_pull_counts() {
    let (_dir, engine) = query_test_engine();
    let label = "CP44_PREPARED_CONSTRUCTION";
    ensure_ready_edge_property_index(&engine, label, "status", SecondaryIndexKind::Equality);
    ensure_ready_edge_property_index(&engine, label, "region", SecondaryIndexKind::Equality);
    ensure_ready_edge_property_index(&engine, label, "score", SecondaryIndexKind::Range);
    let (_nodes, _edge_ids) = seed_streamed_edge_union_fixture(&engine, label, 18);
    engine.flush().unwrap();
    let view = engine.published_state().view.clone();

    let union_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::Or(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyEquals {
                key: "region".to_string(),
                value: PropValue::String("east".to_string()),
            },
        ])),
        page: PageRequest {
            limit: Some(32),
            after: None,
        },
        ..Default::default()
    };
    let union = view.normalize_edge_query(&union_query).unwrap();
    let single_query = EdgeQuery {
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        ..union_query.clone()
    };
    let single = view.normalize_edge_query(&single_query).unwrap();
    let buffered_query = EdgeQuery {
        filter: Some(EdgeFilterExpr::And(vec![
            EdgeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("hot".to_string()),
            },
            EdgeFilterExpr::PropertyRange {
                key: "score".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(0))),
                upper: Some(PropertyRangeBound::Excluded(PropValue::Int(18))),
            },
        ])),
        ..union_query.clone()
    };
    let buffered = view.normalize_edge_query(&buffered_query).unwrap();
    let fallback_query = EdgeQuery {
        allow_full_scan: true,
        page: PageRequest {
            limit: Some(32),
            after: None,
        },
        ..Default::default()
    };
    let fallback = view.normalize_edge_query(&fallback_query).unwrap();

    for (name, normalized, case) in [
        ("eager", &union, PreparedEdgeConstructionCase::Eager),
        (
            "streamed-single",
            &single,
            PreparedEdgeConstructionCase::StreamedSingle,
        ),
        (
            "streamed-union",
            &union,
            PreparedEdgeConstructionCase::StreamedUnion,
        ),
        (
            "buffered",
            &buffered,
            PreparedEdgeConstructionCase::Buffered,
        ),
        (
            "fallback",
            &fallback,
            PreparedEdgeConstructionCase::Fallback,
        ),
    ] {
        let stateless =
            stateless_edge_metadata_ids_with_cutoffs(&view, normalized, 2, None);
        let (one_pull_ids, one_pull_snapshot, one_pull_count) =
            run_prepared_edge_construction_case(&view, normalized, case, 32);
        let (many_pull_ids, many_pull_snapshot, many_pull_count) =
            run_prepared_edge_construction_case(&view, normalized, case, 2);
        assert_eq!(one_pull_ids, stateless, "{name} one-pull parity");
        assert_eq!(many_pull_ids, stateless, "{name} many-pull parity");
        assert_eq!(one_pull_count, 1, "{name} should fit one pull");
        assert!(many_pull_count >= 3, "{name} must exercise persistent pulls");
        assert_eq!(
            prepared_edge_construction_counts(one_pull_snapshot),
            prepared_edge_construction_counts(many_pull_snapshot),
            "{name} physical construction counts must not depend on pull count"
        );
        assert_eq!(one_pull_snapshot.branch_plans, 1);
        assert_eq!(one_pull_snapshot.preparations, 1);
        assert!(one_pull_snapshot.fallback_selections <= 1);
    }

    let empty_query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::IdRange {
            lower: Some(10),
            upper: Some(9),
            lower_inclusive: true,
            upper_inclusive: true,
        }),
        page: PageRequest {
            limit: Some(32),
            after: None,
        },
        ..Default::default()
    };
    let empty = view.normalize_edge_query(&empty_query).unwrap();
    assert!(stateless_edge_metadata_ids_with_cutoffs(&view, &empty, 2, None).is_empty());
    let (mut prepared_empty, followups) = view
        .plan_and_prepare_edge_metadata_pull_source_for_test(&empty, &empty, None)
        .unwrap();
    assert!(followups.is_empty());
    assert_eq!(prepared_empty.prepared_mode(), PreparedEdgeSourceMode::Empty);
    let pull = prepared_empty.pull(2).unwrap();
    assert!(pull.metadata.is_empty());
    assert!(!pull.has_more);
}

#[test]
fn prepared_edge_pull_moves_planning_and_preparation_followups_once_before_fallback() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let label = "CP44_PREPARED_FOLLOWUPS";
    let index_id =
        ensure_ready_edge_property_index(&engine, label, "status", SecondaryIndexKind::Equality);
    let (_nodes, _edge_ids) = seed_streamed_edge_union_fixture(&engine, label, 12);
    engine.flush().unwrap();

    let query = EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("hot".to_string()),
        }),
        page: PageRequest {
            limit: Some(16),
            after: None,
        },
        ..Default::default()
    };
    let view = engine.published_state().view.clone();
    let normalized = view.normalize_edge_query(&query).unwrap();
    let stateless = stateless_edge_metadata_ids_with_cutoffs(&view, &normalized, 2, None);
    let mut planned = view.plan_normalized_edge_query(&normalized).unwrap();
    let equality_source = prepared_edge_metadata_pull_find_source(
        &planned.driver,
        EdgeQueryCandidateSourceKind::EdgePropertyEqualityIndex,
    )
    .expect("followup fixture must plan the equality source");
    let label_source = planned
        .legal_universe_fallback
        .clone()
        .expect("label-constrained query must retain its legal-universe fallback");
    planned.driver = EdgePhysicalPlan::Source(equality_source);
    planned.execution_mode = EdgeDriverExecutionMode::Streamed;
    planned.legal_universe_fallback = Some(label_source);
    let planning_index_id = u64::MAX;
    planned
        .followups
        .push(test_equality_read_followup(planning_index_id));

    let segment_id = engine.segments_for_test()[0].segment_id;
    let sidecar_path = crate::segment_writer::edge_prop_eq_sidecar_path(
        &crate::segment_writer::segment_dir(&db_path, segment_id),
        index_id,
    );
    corrupt_sidecar_header_in_place(&sidecar_path);

    reset_prepared_edge_source_test_snapshot();
    note_prepared_edge_branch_plan();
    let (mut prepared, followups) = view
        .prepare_edge_metadata_pull_source(&normalized, &normalized, planned, None)
        .unwrap();
    let followup_keys = followups
        .iter()
        .map(SecondaryIndexReadFollowup::dedup_key)
        .collect::<Vec<_>>();
    assert_eq!(followup_keys.len(), 2);
    assert!(followups.iter().any(|followup| matches!(
        followup,
        SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id, error: None }
            if *index_id == planning_index_id
    )));
    assert!(followups.iter().any(|followup| matches!(
        followup,
        SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id: actual, error: Some(_) }
            if *actual == index_id
    )));
    assert_eq!(prepared.fallback(), PreparedEdgeFallback::PreparedLegalUniverse);
    assert_eq!(prepared_edge_source_test_snapshot().fallback_selections, 1);

    let (actual, _, pulls) = prepared_edge_pull_ids(&mut prepared, 1);
    assert_eq!(actual, stateless);
    assert!(pulls >= 3);
    assert_eq!(
        prepared_edge_source_test_snapshot().fallback_selections,
        1,
        "fallback must not be selected again by later pulls"
    );
    // Metadata verification has no recoverable secondary-index read in this
    // substrate, so genuine pull-time reads return no followups; planning and
    // preparation ownership was exhausted by the single preparation return above.
    let terminal = prepared.pull(2).unwrap();
    assert!(terminal.metadata.is_empty());
    assert!(!terminal.has_more);
    assert!(terminal.followups.is_empty());
}
