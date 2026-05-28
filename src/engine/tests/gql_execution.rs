// Public Rust GQL execution tests.

fn gql_opts() -> GqlExecutionOptions {
    GqlExecutionOptions::default()
}

fn gql_read_explain(explain: &GqlExecutionExplain) -> &GqlExplain {
    assert_eq!(explain.kind, GqlStatementKind::Query);
    assert!(explain.mutation.is_none());
    explain.read.as_ref().expect("read explain should be present")
}

fn execute_gql_ok(engine: &DatabaseEngine, source: &str) -> GqlExecutionResult {
    engine
        .execute_gql(source, &GqlParams::new(), &gql_opts())
        .unwrap()
}

fn execute_gql_with_options(
    engine: &DatabaseEngine,
    source: &str,
    options: GqlExecutionOptions,
) -> GqlExecutionResult {
    engine
        .execute_gql(source, &GqlParams::new(), &options)
        .unwrap()
}

fn execute_gql_with_params(
    engine: &DatabaseEngine,
    source: &str,
    params: GqlParams,
) -> GqlExecutionResult {
    engine.execute_gql(source, &params, &gql_opts()).unwrap()
}

fn lowered_gql_for_projection_test(source: &str) -> crate::gql::lower::GqlLoweredPlan {
    let params = GqlParams::new();
    let ast = crate::gql::parser::parse_query(
        source,
        &crate::gql::parser::GqlParseOptions::default(),
    )
    .unwrap();
    let semantic = crate::gql::semantic::bind_query(ast, &params).unwrap();
    crate::gql::lower::lower_semantic_plan(
        semantic,
        &params,
        &GqlExecutionOptions {
            allow_full_scan: true,
            ..GqlExecutionOptions::default()
        },
    )
    .unwrap()
}

fn assert_node_need_props(
    needs: &EntityProjectionNeeds,
    alias: &str,
    expected_keys: &[&str],
) {
    let expected = PropertySelection::Keys(
        expected_keys
            .iter()
            .map(|key| (*key).to_string())
            .collect(),
    );
    assert_eq!(needs.nodes.get(alias).map(|needs| &needs.props), Some(&expected));
}

fn assert_edge_need_props(
    needs: &EntityProjectionNeeds,
    alias: &str,
    expected_keys: &[&str],
) {
    let expected = PropertySelection::Keys(
        expected_keys
            .iter()
            .map(|key| (*key).to_string())
            .collect(),
    );
    assert_eq!(needs.edges.get(alias).map(|needs| &needs.props), Some(&expected));
}

fn assert_entity_needs_do_not_request_all_properties(needs: &EntityProjectionNeeds) {
    for node_needs in needs.nodes.values() {
        assert!(!matches!(node_needs.props, PropertySelection::All));
    }
    for edge_needs in needs.edges.values() {
        assert!(!matches!(edge_needs.props, PropertySelection::All));
    }
}

fn assert_gql_param_error(err: EngineError, expected_name: &str, expected_message: &str) {
    match err {
        EngineError::GqlParameter { name, message, .. } => {
            assert_eq!(name, expected_name);
            assert!(
                message.contains(expected_message),
                "expected message to contain {expected_message:?}, got {message:?}"
            );
        }
        other => panic!("expected GQL parameter error, got {other:?}"),
    }
}

fn gql_param_cap_options(
    max_literal_items: usize,
    max_ast_depth: usize,
    max_param_bytes: usize,
) -> GqlExecutionOptions {
    GqlExecutionOptions {
        allow_full_scan: true,
        max_literal_items,
        max_ast_depth,
        max_param_bytes,
        ..GqlExecutionOptions::default()
    }
}

fn gql_u64_column(result: &GqlExecutionResult, index: usize) -> Vec<u64> {
    result
        .rows
        .iter()
        .map(|row| match &row.values[index] {
            GqlValue::UInt(value) => *value,
            other => panic!("expected UInt column, got {other:?}"),
        })
        .collect()
}

fn gql_string_column(result: &GqlExecutionResult, index: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row.values[index] {
            GqlValue::String(value) => value.clone(),
            other => panic!("expected String column, got {other:?}"),
        })
        .collect()
}

fn gql_single_node(value: &GqlValue) -> &GqlNode {
    match value {
        GqlValue::Node(node) => node,
        other => panic!("expected GQL node, got {other:?}"),
    }
}

fn gql_single_edge(value: &GqlValue) -> &GqlEdge {
    match value {
        GqlValue::Edge(edge) => edge,
        other => panic!("expected GQL edge, got {other:?}"),
    }
}

fn gql_single_path(value: &GqlValue) -> &GqlPath {
    match value {
        GqlValue::Path(path) => path,
        other => panic!("expected GQL path, got {other:?}"),
    }
}

#[test]
fn gql_execution_options_default_matches_spec() {
    let options = GqlExecutionOptions::default();
    assert_eq!(options.mode, GqlExecutionMode::Auto);
    assert!(!options.allow_full_scan);
    assert_eq!(options.max_rows, 10_000);
    assert_eq!(options.cursor, None);
    assert_eq!(options.max_cursor_bytes, 16 * 1024);
    assert_eq!(options.max_mutation_rows, 10_000);
    assert_eq!(options.max_mutation_ops, 50_000);
    assert_eq!(options.max_query_bytes, 1_048_576);
    assert_eq!(options.max_param_bytes, 1_048_576);
    assert_eq!(options.max_ast_depth, 256);
    assert_eq!(options.max_literal_items, 10_000);
    assert_eq!(options.max_intermediate_bindings, 65_536);
    assert_eq!(options.max_frontier, 65_536);
    assert_eq!(options.max_path_hops, 16);
    assert_eq!(options.max_paths_per_start, 4_096);
    assert_eq!(options.max_order_materialization, 65_536);
    assert_eq!(options.max_skip, 100_000);
    assert!(!options.include_plan);
    assert!(!options.profile);
    assert!(!options.compact_rows);
    assert!(!options.include_vectors);
}

#[test]
fn execute_gql_read_uses_unified_result_and_read_plan_wrapper() {
    let (_dir, engine) = query_test_engine();
    let active = insert_query_node(
        &engine,
        "Person",
        "gql-read-active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let result = engine
        .execute_gql(
            "MATCH (n:Person {status: 'active'}) RETURN id(n) AS id LIMIT 1",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(result.kind, GqlStatementKind::Query);
    assert_eq!(result.mutation_stats, None);
    assert_eq!(result.columns, vec!["id"]);
    assert_eq!(gql_u64_column(&result, 0), vec![active]);
    let plan = result.plan.as_ref().expect("include_plan should return plan");
    assert_eq!(plan.kind, GqlStatementKind::Query);
    assert_eq!(plan.columns, vec!["id"]);
    assert!(plan.read.is_some());
    assert!(plan.mutation.is_none());
}

#[test]
fn execute_gql_create_mutation_preserves_cursor_and_readonly_ordering() {
    let (_dir, engine) = query_test_engine();
    let source = "CREATE (n:GqlMutationNoSideEffect {key: 'n'}) RETURN n";

    let cursor_first = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some("not-a-read-cursor".to_string()),
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    match cursor_first {
        EngineError::InvalidCursor { message } => {
            assert_eq!(message, "GQL mutation statements do not accept cursors");
        }
        err => panic!("expected mutation cursor error, got {err:?}"),
    }

    let read_only = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(
        read_only,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::ReadOnlyViolation,
            ..
        }
    ));

    assert_eq!(engine.get_node_label_id("GqlMutationNoSideEffect").unwrap(), None);
    let result = engine
        .execute_gql(source, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_eq!(result.kind, GqlStatementKind::Mutation);
    assert_eq!(result.columns, vec!["n"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.mutation_stats.as_ref().unwrap().nodes_created,
        1
    );
    assert!(engine
        .get_node_by_key("GqlMutationNoSideEffect", "n")
        .unwrap()
        .is_some());

    let planned = engine
        .execute_gql(
            "CREATE (n:GqlMutationIncludePlan {key: 'n'}) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let plan = planned.plan.expect("mutation include_plan should return a plan");
    assert_eq!(plan.kind, GqlStatementKind::Mutation);
    let mutation = plan.mutation.expect("mutation plan should be present");
    assert!(mutation.uses_write_txn);
    assert!(mutation.atomic_commit);
    assert_eq!(
        mutation.would_create_node_labels,
        vec!["GqlMutationIncludePlan".to_string()]
    );
}

#[test]
fn explain_gql_mutation_plan_is_side_effect_free() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let wal_path = wal_generation_path(&db_path, 0);
    let before_wal_len = std::fs::metadata(&wal_path).map(|metadata| metadata.len()).unwrap_or(0);
    let label = "GqlExplainNoSideEffect";
    let source = format!("CREATE (n:{label} {{key: 'n'}}) RETURN n");
    assert_eq!(engine.get_node_label_id(label).unwrap(), None);

    for options in [
        GqlExecutionOptions {
            cursor: Some("not-a-read-cursor".to_string()),
            ..gql_opts()
        },
        GqlExecutionOptions {
            cursor: Some("not-a-read-cursor".to_string()),
            mode: GqlExecutionMode::ReadOnly,
            ..gql_opts()
        },
    ] {
        let err = engine
            .explain_gql(&source, &GqlParams::new(), &options)
            .unwrap_err();
        assert!(matches!(
            err,
            EngineError::InvalidCursor { message }
                if message == "GQL mutation statements do not accept cursors"
        ));
    }

    let read_only = engine
        .explain_gql(
            &source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(
        read_only,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::ReadOnlyViolation,
            ..
        }
    ));

    let explain = engine
        .explain_gql(&source, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_eq!(explain.kind, GqlStatementKind::Mutation);
    assert!(explain.read.is_none());
    let mutation = explain.mutation.expect("mutation explain should be present");
    assert_eq!(mutation.would_create_node_labels, vec![label.to_string()]);
    assert!(mutation.uses_write_txn);
    assert!(mutation.atomic_commit);
    assert!(!mutation.uses_transaction_snapshot);
    assert!(mutation.read_prefix.is_none());
    assert_eq!(engine.get_node_label_id(label).unwrap(), None);
    let after_wal_len = std::fs::metadata(&wal_path).map(|metadata| metadata.len()).unwrap_or(0);
    assert_eq!(after_wal_len, before_wal_len);
}

#[test]
fn mutation_errors_surface_before_execution_validation() {
    let (_dir, engine) = query_test_engine();

    let missing = engine
        .execute_gql(
            "CREATE (n:Person {key: $key}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_gql_param_error(missing, "key", "missing parameter");

    let invalid_target = engine
        .execute_gql(
            "MATCH p = (a)-[r:KNOWS]->(b) SET p.name = 'x'",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(
        invalid_target,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidPropertyAccess,
            ..
        }
    ));

    let full_scan = engine
        .execute_gql(
            "MATCH (n) SET n.name = 'Ada'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        full_scan,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::FullScanNotAllowed,
            ..
        }
    ));

    let explain_full_scan = engine
        .explain_gql(
            "MATCH (n) SET n.name = 'Ada'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        explain_full_scan,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::FullScanNotAllowed,
            ..
        }
    ));
}

#[test]
fn mutation_referenced_params_validate_before_execution() {
    let (_dir, engine) = query_test_engine();
    let options = GqlExecutionOptions {
        allow_full_scan: true,
        ..gql_opts()
    };
    for (source, missing_name) in [
        ("MATCH (n:Person {key: $key}) SET n.name = 'Ada'", "key"),
        ("CREATE (n:Person {key: $key})", "key"),
        ("MATCH (n:Person {key: 'a'}) SET n.name = $name", "name"),
        ("MATCH (n:Person {key: 'a'}) SET n += $props", "props"),
        ("MATCH (n:Person {key: 'a'}) DELETE $target", "target"),
        ("CREATE (n:Person {key: 'a'}) RETURN $value", "value"),
        (
            "CREATE (n:Person {key: 'a'}) RETURN n ORDER BY $order",
            "order",
        ),
        ("CREATE (n:Person {key: 'a'}) RETURN n SKIP $skip", "skip"),
        ("CREATE (n:Person {key: 'a'}) RETURN n LIMIT $limit", "limit"),
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &options)
            .unwrap_err();
        assert_gql_param_error(err, missing_name, "missing parameter");
    }

    let cap_err = engine
        .execute_gql(
            "CREATE (n:Person {key: $key})",
            &GqlParams::from([(
                "key".to_string(),
                GqlParamValue::String("too-long".to_string()),
            )]),
            &GqlExecutionOptions {
                max_param_bytes: 3,
                ..options.clone()
            },
        )
        .unwrap_err();
    assert_gql_param_error(cap_err, "key", "exceeding max_param_bytes");

    let ignored = engine
        .execute_gql(
            "CREATE (n:Person {key: 'literal'})",
            &GqlParams::from([(
                "unused".to_string(),
                GqlParamValue::String("too-long".to_string()),
            )]),
            &GqlExecutionOptions {
                max_param_bytes: 3,
                ..options
            },
        )
        .unwrap();
    assert_eq!(ignored.mutation_stats.as_ref().unwrap().nodes_created, 1);
}

fn gql_create_test_engine_with_options(options: DbOptions) -> (TempDir, DatabaseEngine) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &options).unwrap();
    seed_query_test_catalog(&engine);
    (dir, engine)
}

#[test]
fn gql_create_node_executes_through_transaction_and_returns_created_values() {
    let (_dir, engine) = query_test_engine();
    let result = engine
        .execute_gql(
            "CREATE (n:Person:Employee {key: 'gql-create-ada', name: 'Ada', age: 37, weight: 2.5, nullable: null}) RETURN n, id(n), n.name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();

    assert_eq!(result.kind, GqlStatementKind::Mutation);
    assert_eq!(result.columns, vec!["n", "id(n)", "n.name"]);
    assert_eq!(result.next_cursor, None);
    assert_eq!(result.rows.len(), 1);
    let node = gql_single_node(&result.rows[0].values[0]);
    let created_id = match result.rows[0].values[1] {
        GqlValue::UInt(id) => id,
        ref other => panic!("expected id UInt, got {other:?}"),
    };
    assert_eq!(node.id, Some(created_id));
    assert_eq!(node.key.as_deref(), Some("gql-create-ada"));
    assert_eq!(result.rows[0].values[2], GqlValue::String("Ada".to_string()));

    let stored = engine
        .get_node_by_key("Person", "gql-create-ada")
        .unwrap()
        .unwrap();
    assert_eq!(stored.id, created_id);
    assert!(stored.labels.iter().any(|label| label == "Person"));
    assert!(stored.labels.iter().any(|label| label == "Employee"));
    assert_eq!(
        engine
            .get_node_by_key("Employee", "gql-create-ada")
            .unwrap()
            .unwrap()
            .id,
        created_id
    );
    assert_eq!(stored.props.get("name"), Some(&PropValue::String("Ada".to_string())));
    assert_eq!(stored.props.get("age"), Some(&PropValue::Int(37)));
    assert_eq!(stored.props.get("nullable"), Some(&PropValue::Null));
    assert!(!stored.props.contains_key("key"));
    assert!(!stored.props.contains_key("weight"));
    assert_eq!(stored.weight, 2.5);

    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.rows_matched, 1);
    assert_eq!(stats.mutation_rows, 1);
    assert_eq!(stats.mutation_ops, 1);
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.edges_created, 0);
    assert_eq!(stats.properties_set, 3);
    assert_eq!(stats.labels_added, 2);
}

#[test]
fn gql_create_node_properties_are_visible_to_gql_indexed_reads() {
    let (_dir, engine) = query_test_engine();
    engine
        .ensure_node_property_index("GqlCreatedIndexed", "status", SecondaryIndexKind::Equality)
        .unwrap();

    let created = engine
        .execute_gql(
            "CREATE (n:GqlCreatedIndexed {key: 'n', status: 'ready', score: 7}) RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let created_id = gql_u64_column(&created, 0)[0];

    let read = engine
        .execute_gql(
            "MATCH (n:GqlCreatedIndexed {status: 'ready'}) RETURN id(n), n.score",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(gql_u64_column(&read, 0), vec![created_id]);
    assert_eq!(read.rows[0].values[1], GqlValue::Int(7));
}

#[test]
fn gql_create_node_strict_duplicates_reject_before_write() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "gql-create-existing", &[], 1.0);

    let visible = engine
        .execute_gql(
            "CREATE (n:Person {key: 'gql-create-existing', name: 'new'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(visible, EngineError::InvalidOperation(message) if message.contains("already exists")));
    assert_eq!(
        engine
            .get_node_by_key("Person", "gql-create-existing")
            .unwrap()
            .unwrap()
            .props
            .get("name"),
        None
    );

    let duplicate = engine
        .execute_gql(
            "CREATE (a:GqlCreateDup {key: 'dup'}), (b:GqlCreateDup {key: 'dup'}) RETURN a",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(duplicate, EngineError::InvalidOperation(message) if message.contains("duplicate node CREATE target")));
    assert!(engine
        .get_node_by_key("GqlCreateDup", "dup")
        .unwrap()
        .is_none());

    insert_query_node(
        &engine,
        "GqlCreateFinalConflict",
        "final-key",
        &[("name", PropValue::String("old".to_string()))],
        1.0,
    );
    let final_label_visible = engine
        .execute_gql(
            "CREATE (n:GqlCreateInitialOnly {key: 'final-key', name: 'new'}) SET n:GqlCreateFinalConflict RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(final_label_visible, EngineError::InvalidOperation(message) if message.contains("already exists")));
    assert_eq!(
        engine
            .get_node_by_key("GqlCreateFinalConflict", "final-key")
            .unwrap()
            .unwrap()
            .props
            .get("name"),
        Some(&PropValue::String("old".to_string()))
    );
    assert!(engine
        .get_node_by_key("GqlCreateInitialOnly", "final-key")
        .unwrap()
        .is_none());

    let final_label_duplicate = engine
        .execute_gql(
            "CREATE (a:GqlCreateFinalLeft {key: 'final-dup'}), (b:GqlCreateFinalRight {key: 'final-dup'}) SET a:GqlCreateFinalShared SET b:GqlCreateFinalShared RETURN a",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(final_label_duplicate, EngineError::InvalidOperation(message) if message.contains("duplicate node CREATE target")));
    assert!(engine
        .get_node_by_key("GqlCreateFinalShared", "final-dup")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlCreateFinalLeft", "final-dup")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlCreateFinalRight", "final-dup")
        .unwrap()
        .is_none());

    let existing_old = insert_query_node(
        &engine,
        "GqlCreateRemovedOld",
        "final-free",
        &[("name", PropValue::String("old".to_string()))],
        1.0,
    );
    let final_removed_old = engine
        .execute_gql(
            "CREATE (n:GqlCreateRemovedOld {key: 'final-free', name: 'new'}) SET n:GqlCreateFinalNew REMOVE n:GqlCreateRemovedOld RETURN id(n), labels(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let final_removed_old_id = match final_removed_old.rows[0].values[0] {
        GqlValue::UInt(id) => id,
        ref other => panic!("expected created id, got {other:?}"),
    };
    assert_ne!(final_removed_old_id, existing_old);
    assert_eq!(
        engine
            .get_node_by_key("GqlCreateRemovedOld", "final-free")
            .unwrap()
            .unwrap()
            .id,
        existing_old
    );
    let final_new = engine
        .get_node_by_key("GqlCreateFinalNew", "final-free")
        .unwrap()
        .unwrap();
    assert_eq!(final_new.id, final_removed_old_id);
    assert!(!final_new
        .labels
        .contains(&"GqlCreateRemovedOld".to_string()));

    insert_query_node(
        &engine,
        "GqlCreateSeed",
        "seed-a",
        &[("target", PropValue::String("same".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlCreateSeed",
        "seed-b",
        &[("target", PropValue::String("same".to_string()))],
        1.0,
    );
    let multi_row = engine
        .execute_gql(
            "MATCH (s:GqlCreateSeed) CREATE (n:GqlCreateRollback {key: s.target}) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(multi_row, EngineError::InvalidOperation(message) if message.contains("duplicate node CREATE target")));
    assert!(engine
        .get_node_by_key("GqlCreateRollback", "same")
        .unwrap()
        .is_none());
}

#[test]
fn gql_create_node_rejects_prune_hidden_existing_key_before_write() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "GqlPruneHiddenCreate",
        "hidden",
        &[("source", PropValue::String("old".to_string()))],
        0.1,
    );
    engine
        .set_prune_policy(
            "gql-hide-create-target",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("GqlPruneHiddenCreate".to_string()),
            },
        )
        .unwrap();
    assert!(engine
        .get_node_by_key("GqlPruneHiddenCreate", "hidden")
        .unwrap()
        .is_none());

    let hidden_duplicate = engine
        .execute_gql(
            "CREATE (n:GqlPruneHiddenCreate {key: 'hidden', name: 'new'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(hidden_duplicate, EngineError::InvalidOperation(message) if message.contains("already exists")));

    assert!(engine.remove_prune_policy("gql-hide-create-target").unwrap());
    let original = engine
        .get_node_by_key("GqlPruneHiddenCreate", "hidden")
        .unwrap()
        .unwrap();
    assert_eq!(
        original.props.get("source"),
        Some(&PropValue::String("old".to_string()))
    );
    assert!(!original.props.contains_key("name"));
}

#[test]
fn gql_create_invalid_node_metadata_and_property_values_reject_before_write() {
    let (_dir, engine) = query_test_engine();

    let bad_key = engine
        .execute_gql(
            "CREATE (n:GqlBadKey {key: 42, name: 'bad'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(bad_key, EngineError::InvalidOperation(message) if message.contains("key")));
    assert!(engine
        .get_node_by_key("GqlBadKey", "42")
        .unwrap()
        .is_none());

    let bad_weight = engine
        .execute_gql(
            "CREATE (n:GqlBadWeight {key: 'n', weight: 'heavy'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_weight, EngineError::InvalidOperation(message) if message.contains("weight"))
    );
    assert!(engine
        .get_node_by_key("GqlBadWeight", "n")
        .unwrap()
        .is_none());

    let bad_property = engine
        .execute_gql(
            "CREATE (n:GqlBadProp {key: 'n', score: $bad}) RETURN n",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_property, EngineError::InvalidOperation(message) if message.contains("finite"))
    );
    assert!(engine
        .get_node_by_key("GqlBadProp", "n")
        .unwrap()
        .is_none());
}

#[test]
fn gql_create_edge_executes_for_matched_and_created_endpoints() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "gql-create-edge-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "gql-create-edge-b", &[], 1.0);

    let result = engine
        .execute_gql(
            "MATCH (a:Person) WHERE a.key = 'gql-create-edge-a' MATCH (b:Person) WHERE b.key = 'gql-create-edge-b' CREATE (a)-[r:Gql_CREATED {since: 2026, weight: 0.8, valid_from: 10, valid_to: 20}]->(b) RETURN r, id(r), r.since",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let edge_id = match result.rows[0].values[1] {
        GqlValue::UInt(id) => id,
        ref other => panic!("expected edge id UInt, got {other:?}"),
    };
    assert_eq!(result.rows[0].values[2], GqlValue::Int(2026));
    let returned = gql_single_edge(&result.rows[0].values[0]);
    assert_eq!(returned.id, Some(edge_id));
    assert_eq!(returned.from, Some(a));
    assert_eq!(returned.to, Some(b));
    let stored = engine.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(stored.from, a);
    assert_eq!(stored.to, b);
    assert_eq!(stored.label, "Gql_CREATED");
    assert_eq!(stored.props.get("since"), Some(&PropValue::Int(2026)));
    assert!(!stored.props.contains_key("weight"));
    assert!(!stored.props.contains_key("valid_from"));
    assert!(!stored.props.contains_key("valid_to"));
    assert_eq!(stored.weight, 0.8);
    assert_eq!(stored.valid_from, 10);
    assert_eq!(stored.valid_to, 20);

    let chain = engine
        .execute_gql(
            "CREATE (a:GqlChain {key: 'a'})-[r:Gql_CHAIN {rank: 1}]->(b:GqlChain {key: 'b'}) RETURN id(a), id(r), id(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(chain.mutation_stats.as_ref().unwrap().nodes_created, 2);
    assert_eq!(chain.mutation_stats.as_ref().unwrap().edges_created, 1);
    let ids = gql_u64_column(&chain, 0);
    assert_eq!(ids.len(), 1);
    let edge_ids = gql_u64_column(&chain, 1);
    let b_ids = gql_u64_column(&chain, 2);
    let chain_edge = engine.get_edge(edge_ids[0]).unwrap().unwrap();
    assert_eq!(chain_edge.from, ids[0]);
    assert_eq!(chain_edge.to, b_ids[0]);
}

#[test]
fn gql_create_invalid_edge_validity_and_metadata_return_behaviors() {
    let (_dir, engine) = query_test_engine();

    let bad_valid_to = engine
        .execute_gql(
            "CREATE (a:GqlBadEdgeWindow {key: 'a'})-[r:Gql_BAD_WINDOW {valid_to: 0}]->(b:GqlBadEdgeWindow {key: 'b'}) RETURN r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_valid_to, EngineError::InvalidOperation(message) if message.contains("valid_from < valid_to"))
    );
    assert!(engine
        .get_node_by_key("GqlBadEdgeWindow", "a")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlBadEdgeWindow", "b")
        .unwrap()
        .is_none());

    let bad_valid_from = engine
        .execute_gql(
            "CREATE (a:GqlBadEdgeWindowMax {key: 'a'})-[r:Gql_BAD_WINDOW_MAX {valid_from: 9223372036854775807}]->(b:GqlBadEdgeWindowMax {key: 'b'}) RETURN r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_valid_from, EngineError::InvalidOperation(message) if message.contains("valid_from < valid_to"))
    );
    assert!(engine
        .get_node_by_key("GqlBadEdgeWindowMax", "a")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlBadEdgeWindowMax", "b")
        .unwrap()
        .is_none());

    let node_metadata_return = engine
        .execute_gql(
            "CREATE (n:GqlReturnNodeMetadata {key: 'n'}) RETURN n.created_at",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(node_metadata_return.rows.len(), 1);
    assert!(matches!(
        node_metadata_return.rows[0].values[0],
        GqlValue::Int(value) if value > 0
    ));
    assert!(engine
        .get_node_by_key("GqlReturnNodeMetadata", "n")
        .unwrap()
        .is_some());

    let edge_metadata_return = engine
        .execute_gql(
            "CREATE (a:GqlReturnEdgeMetadata {key: 'a'})-[r:Gql_RETURN_META]->(b:GqlReturnEdgeMetadata {key: 'b'}) RETURN r.updated_at",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(edge_metadata_return.rows.len(), 1);
    assert!(matches!(
        edge_metadata_return.rows[0].values[0],
        GqlValue::Int(value) if value > 0
    ));
    assert!(engine
        .get_node_by_key("GqlReturnEdgeMetadata", "a")
        .unwrap()
        .is_some());
    assert!(engine
        .get_node_by_key("GqlReturnEdgeMetadata", "b")
        .unwrap()
        .is_some());
}

#[test]
fn gql_create_edge_strict_uniqueness_respects_engine_option() {
    let (_dir, unique_engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    });
    let a = insert_query_node(&unique_engine, "Person", "gql-unique-a", &[], 1.0);
    let b = insert_query_node(&unique_engine, "Person", "gql-unique-b", &[], 1.0);
    unique_engine
        .upsert_edge(a, b, "Gql_UNIQUE", UpsertEdgeOptions::default())
        .unwrap();
    let duplicate = unique_engine
        .execute_gql(
            "MATCH (a:Person) WHERE a.key = 'gql-unique-a' MATCH (b:Person) WHERE b.key = 'gql-unique-b' CREATE (a)-[:Gql_UNIQUE]->(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(duplicate, EngineError::InvalidOperation(message) if message.contains("already exists")));

    let (_dir, parallel_engine) = query_test_engine();
    insert_query_node(&parallel_engine, "Person", "gql-parallel-a", &[], 1.0);
    insert_query_node(&parallel_engine, "Person", "gql-parallel-b", &[], 1.0);
    let parallel = parallel_engine
        .execute_gql(
            "MATCH (a:Person) WHERE a.key = 'gql-parallel-a' MATCH (b:Person) WHERE b.key = 'gql-parallel-b' CREATE (a)-[:Gql_PARALLEL]->(b), (a)-[:Gql_PARALLEL]->(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(parallel.mutation_stats.as_ref().unwrap().edges_created, 2);
    assert_eq!(parallel.mutation_stats.as_ref().unwrap().mutation_ops, 2);
}

#[test]
fn gql_create_match_backed_rows_caps_and_optional_null_skip_are_atomic() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlBatch", "a", &[], 1.0);
    insert_query_node(&engine, "GqlBatch", "b", &[], 1.0);

    let cap = engine
        .execute_gql(
            "MATCH (s:GqlBatch) CREATE (n:GqlCapCreate {key: s.key}) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_mutation_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_rows")));
    assert!(engine
        .get_node_by_key("GqlCapCreate", "a")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlCapCreate", "b")
        .unwrap()
        .is_none());

    let cursor_cap = engine
        .execute_gql(
            "MATCH (s:GqlBatch) CREATE (n:GqlCursorCapCreate {key: s.key}) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_mutation_rows: 10,
                max_intermediate_bindings: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        matches!(cursor_cap, EngineError::InvalidOperation(ref message) if message.contains("max_page_limit")),
        "{cursor_cap:?}"
    );
    assert!(engine
        .get_node_by_key("GqlCursorCapCreate", "a")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlCursorCapCreate", "b")
        .unwrap()
        .is_none());

    let root = insert_query_node(&engine, "GqlOptionalRoot", "root", &[], 1.0);
    let skipped = engine
        .execute_gql(
            "MATCH (a:GqlOptionalRoot) WHERE a.key = 'root' OPTIONAL MATCH (a)-[r:Gql_MISSING]->(b) CREATE (b)-[:Gql_SKIP]->(c:GqlSkipped {key: 'c'}) RETURN c",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(skipped.rows.len(), 1);
    assert_eq!(skipped.rows[0].values[0], GqlValue::Null);
    let stats = skipped.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.rows_matched, 1);
    assert_eq!(stats.mutation_rows, 0);
    assert_eq!(stats.skipped_null_targets, 1);
    assert_eq!(stats.nodes_created, 0);
    assert!(engine.get_node_by_key("GqlSkipped", "c").unwrap().is_none());
    assert!(engine
        .query_edges(&EdgeQuery {
            from_ids: vec![root],
            label: Some("Gql_SKIP".to_string()),
            ..Default::default()
        })
        .unwrap()
        .edges
        .is_empty());
}

#[test]
fn gql_create_cap_fails_during_materialization_without_writes() {
    let (_dir, engine) = query_test_engine();
    for key in ["a", "b", "c"] {
        insert_query_node(&engine, "GqlCreateEarlyCapSource", key, &[], 1.0);
    }

    let err = engine
        .execute_gql(
            "MATCH (s:GqlCreateEarlyCapSource) CREATE (n:GqlCreateEarlyCap {key: s.key})",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_mutation_ops: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    for key in ["a", "b", "c"] {
        assert!(engine
            .get_node_by_key("GqlCreateEarlyCap", key)
            .unwrap()
            .is_none());
    }
}

#[test]
fn gql_create_return_order_by_id_and_later_delete_executes() {
    let (_dir, engine) = query_test_engine();
    let supported_return = engine
        .execute_gql(
            "CREATE (n:GqlReturnSupportedOrder {key: 'n'}) RETURN n ORDER BY n.key",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(supported_return.rows.len(), 1);
    let returned = gql_single_node(&supported_return.rows[0].values[0]);
    assert_eq!(returned.key.as_deref(), Some("n"));
    assert!(engine
        .get_node_by_key("GqlReturnSupportedOrder", "n")
        .unwrap()
        .is_some());

    let supported_set = engine
        .execute_gql(
            "CREATE (n:GqlUnsupportedSet {key: 'n'}) SET n.name = 'Ada'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(supported_set.mutation_stats.as_ref().unwrap().nodes_created, 1);
    assert_eq!(
        engine
            .get_node_by_key("GqlUnsupportedSet", "n")
            .unwrap()
            .unwrap()
            .props
            .get("name"),
        Some(&PropValue::String("Ada".to_string()))
    );

    let delete = engine
        .execute_gql(
            "MATCH (n:GqlUnsupportedSet) WHERE n.key = 'n' DETACH DELETE n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(delete.mutation_stats.as_ref().unwrap().nodes_deleted, 1);
    assert!(engine
        .get_node_by_key("GqlUnsupportedSet", "n")
        .unwrap()
        .is_none());
}

#[test]
fn gql_set_node_property_updates_existing_node_index_and_return() {
    let (_dir, engine) = query_test_engine();
    engine
        .ensure_node_property_index("GqlSetIndexed", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let node_id = insert_query_node(
        &engine,
        "GqlSetIndexed",
        "n",
        &[
            ("status", PropValue::String("old".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.25,
    );
    let before = engine.get_node(node_id).unwrap().unwrap();

    let result = engine
        .execute_gql(
            "MATCH (n:GqlSetIndexed) WHERE n.key = 'n' SET n.status = 'new' RETURN n, id(n), n.status, n.weight",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[1], GqlValue::UInt(node_id));
    assert_eq!(result.rows[0].values[2], GqlValue::String("new".to_string()));
    assert_eq!(result.rows[0].values[3], GqlValue::Float(1.25));
    let returned = gql_single_node(&result.rows[0].values[0]);
    assert_eq!(returned.id, Some(node_id));
    assert_eq!(
        returned.props.as_ref().unwrap().get("status"),
        Some(&GqlValue::String("new".to_string()))
    );

    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert_eq!(stored.id, node_id);
    assert_eq!(stored.created_at, before.created_at);
    assert!(stored.updated_at >= before.updated_at);
    assert_eq!(
        stored.props.get("status"),
        Some(&PropValue::String("new".to_string()))
    );
    let new_read = execute_gql_ok(
        &engine,
        "MATCH (n:GqlSetIndexed {status: 'new'}) RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&new_read, 0), vec![node_id]);
    let old_read = execute_gql_ok(
        &engine,
        "MATCH (n:GqlSetIndexed {status: 'old'}) RETURN id(n)",
    );
    assert!(old_read.rows.is_empty());
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_updated, 1);
    assert_eq!(stats.properties_set, 1);
    assert_eq!(stats.mutation_ops, 1);
}

#[test]
fn gql_set_edge_property_and_metadata_preserves_edge_identity() {
    let (_dir, engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: false,
        ..DbOptions::default()
    });
    let a = insert_query_node(&engine, "GqlEdgeSetNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlEdgeSetNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_SET_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2020))]),
                weight: 0.5,
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
            },
        )
        .unwrap();
    let before = engine.get_edge(edge_id).unwrap().unwrap();

    let result = engine
        .execute_gql(
            "MATCH (a:GqlEdgeSetNode) WHERE a.key = 'a' MATCH (b:GqlEdgeSetNode) WHERE b.key = 'b' MATCH (a)-[r:Gql_SET_EDGE]->(b) \
             SET r.since = 2026 SET r.weight = 2.5 SET r.valid_from = 10 SET r.valid_to = 20 \
             RETURN id(r), r.since, r.weight, r.valid_from, r.valid_to",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(edge_id));
    assert_eq!(result.rows[0].values[1], GqlValue::Int(2026));
    assert_eq!(result.rows[0].values[2], GqlValue::Float(2.5));
    assert_eq!(result.rows[0].values[3], GqlValue::Int(10));
    assert_eq!(result.rows[0].values[4], GqlValue::Int(20));

    let after = engine.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(after.id, edge_id);
    assert_eq!(after.from, a);
    assert_eq!(after.to, b);
    assert_eq!(after.label, "Gql_SET_EDGE");
    assert_eq!(after.created_at, before.created_at);
    assert!(after.updated_at >= before.updated_at);
    assert_eq!(after.props.get("since"), Some(&PropValue::Int(2026)));
    assert_eq!(after.weight, 2.5);
    assert_eq!(after.valid_from, 10);
    assert_eq!(after.valid_to, 20);
    assert_eq!(result.mutation_stats.as_ref().unwrap().edges_updated, 1);
}

#[test]
fn gql_set_existing_edge_allows_same_statement_parallel_create_when_nonunique() {
    let (_dir, engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: false,
        ..DbOptions::default()
    });
    let a = insert_query_node(&engine, "GqlParallelRplNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlParallelRplNode", "b", &[], 1.0);
    let existing = engine
        .upsert_edge(
            a,
            b,
            "Gql_PARALLEL_REPLACE",
            UpsertEdgeOptions {
                props: query_test_props(&[("kind", PropValue::String("old".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (a:GqlParallelRplNode) WHERE a.key = 'a' \
             MATCH (b:GqlParallelRplNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_PARALLEL_REPLACE]->(b) \
             CREATE (a)-[x:Gql_PARALLEL_REPLACE {kind: 'new'}]->(b) \
             SET r.kind = 'updated' RETURN id(r), r.kind",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(existing));
    assert_eq!(
        result.rows[0].values[1],
        GqlValue::String("updated".to_string())
    );
    let edges = engine
        .query_edges(&EdgeQuery {
            label: Some("Gql_PARALLEL_REPLACE".to_string()),
            from_ids: vec![a],
            to_ids: vec![b],
            ..Default::default()
        })
        .unwrap()
        .edges;
    assert_eq!(edges.len(), 2);
    let existing_after = engine.get_edge(existing).unwrap().unwrap();
    assert_eq!(existing_after.id, existing);
    assert_eq!(
        existing_after.props.get("kind"),
        Some(&PropValue::String("updated".to_string()))
    );
    assert!(edges.iter().any(|edge| {
        edge.id != existing
            && edge.props.get("kind") == Some(&PropValue::String("new".to_string()))
    }));
}

#[test]
fn gql_set_map_merge_handles_nulls_and_weight_as_property() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "GqlMapMerge",
        "n",
        &[
            ("old", PropValue::String("remove".to_string())),
            ("keep", PropValue::Int(1)),
        ],
        3.0,
    );
    let params = GqlParams::from([(
        "props".to_string(),
        GqlParamValue::Map(BTreeMap::from([
            ("old".to_string(), GqlParamValue::Null),
            ("keep".to_string(), GqlParamValue::Int(2)),
            (
                "nested".to_string(),
                GqlParamValue::List(vec![GqlParamValue::Null, GqlParamValue::String("x".to_string())]),
            ),
            ("weight".to_string(), GqlParamValue::String("stored-prop".to_string())),
        ])),
    )]);

    engine
        .execute_gql(
            "MATCH (n:GqlMapMerge) WHERE n.key = 'n' SET n += $props RETURN n.keep, n.old, n.nested, n.weight",
            &params,
            &gql_opts(),
        )
        .unwrap();
    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert_eq!(stored.weight, 3.0);
    assert!(!stored.props.contains_key("old"));
    assert_eq!(stored.props.get("keep"), Some(&PropValue::Int(2)));
    assert_eq!(
        stored.props.get("nested"),
        Some(&PropValue::Array(vec![
            PropValue::Null,
            PropValue::String("x".to_string())
        ]))
    );
    assert_eq!(
        stored.props.get("weight"),
        Some(&PropValue::String("stored-prop".to_string()))
    );

    let non_map = engine
        .execute_gql(
            "MATCH (n:GqlMapMerge) WHERE n.key = 'n' SET n += 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(non_map, EngineError::InvalidOperation(message) if message.contains("map")));
}

#[test]
fn gql_set_map_merge_rejects_reserved_metadata_keys() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "GqlReservedMapMerge",
        "a",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );
    let b = insert_query_node(&engine, "GqlReservedMapMerge", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_RESERVED_MERGE_EDGE",
            UpsertEdgeOptions {
                props: BTreeMap::from([(
                    "status".to_string(),
                    PropValue::String("old".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();

    for key in [
        "id",
        "labels",
        "key",
        "created_at",
        "updated_at",
        "dense_vector",
        "sparse_vector",
    ] {
        let err = engine
            .execute_gql(
                "MATCH (n:GqlReservedMapMerge) WHERE n.key = 'a' SET n += $props",
                &GqlParams::from([(
                    "props".to_string(),
                    GqlParamValue::Map(BTreeMap::from([(
                        key.to_string(),
                        GqlParamValue::Int(1),
                    )])),
                )]),
                &gql_opts(),
            )
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::InvalidOperation(message) if message.contains("reserved metadata")),
            "expected reserved metadata error for node key {key}, got {err:?}"
        );
    }
    let node = engine.get_node(a).unwrap().unwrap();
    assert_eq!(
        node.props.get("status"),
        Some(&PropValue::String("old".to_string()))
    );
    assert!(!node.props.contains_key("id"));
    assert!(!node.props.contains_key("key"));
    assert!(!node.props.contains_key("dense_vector"));

    for key in ["id", "from", "to", "label", "type", "created_at", "updated_at"] {
        let err = engine
            .execute_gql(
                "MATCH (a:GqlReservedMapMerge) WHERE a.key = 'a' \
                 MATCH (b:GqlReservedMapMerge) WHERE b.key = 'b' \
                 MATCH (a)-[r:Gql_RESERVED_MERGE_EDGE]->(b) SET r += $props",
                &GqlParams::from([(
                    "props".to_string(),
                    GqlParamValue::Map(BTreeMap::from([(
                        key.to_string(),
                        GqlParamValue::Int(1),
                    )])),
                )]),
                &gql_opts(),
            )
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::InvalidOperation(message) if message.contains("reserved metadata")),
            "expected reserved metadata error for edge key {key}, got {err:?}"
        );
    }
    let edge = engine.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(
        edge.props.get("status"),
        Some(&PropValue::String("old".to_string()))
    );
    assert!(!edge.props.contains_key("from"));
    assert!(!edge.props.contains_key("type"));
}

#[test]
fn gql_remove_property_and_label_are_noop_safe_and_atomic() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node_with_labels(
        &engine,
        &["GqlRemove", "GqlRemoveExtra"],
        "n",
        &[("drop", PropValue::Bool(true))],
        1.0,
    );

    let result = engine
        .execute_gql(
            "MATCH (n:GqlRemove) WHERE n.key = 'n' REMOVE n.drop REMOVE n.missing REMOVE n:GqlRemoveExtra RETURN n.drop, labels(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0], GqlValue::Null);
    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert!(!stored.props.contains_key("drop"));
    assert!(stored.labels.iter().any(|label| label == "GqlRemove"));
    assert!(!stored.labels.iter().any(|label| label == "GqlRemoveExtra"));
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.labels_removed, 1);
    assert_eq!(stats.properties_removed, 1);

    let last_label = engine
        .execute_gql(
            "MATCH (n:GqlRemove) WHERE n.key = 'n' REMOVE n:GqlRemove",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(last_label, EngineError::InvalidOperation(message) if message.contains("last node label")));
    assert!(engine.get_node(node_id).unwrap().unwrap().labels.contains(&"GqlRemove".to_string()));

    let optional = engine
        .execute_gql(
            "MATCH (n:GqlRemove) WHERE n.key = 'n' OPTIONAL MATCH (n)-[r:Gql_REMOVE_MISSING]->(m) SET m.name = 'x' REMOVE m.missing",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(optional.mutation_stats.as_ref().unwrap().skipped_null_targets, 2);
    assert_eq!(optional.mutation_stats.as_ref().unwrap().mutation_ops, 0);
}

#[test]
fn gql_set_duplicate_targets_are_coalesced_last_write_wins() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(&engine, "GqlDuplicateSet", "n", &[], 1.0);

    let result = engine
        .execute_gql(
            "MATCH (n:GqlDuplicateSet) WHERE n.key = 'n' SET n.name = 'first' SET n.name = 'second' RETURN n.name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0], GqlValue::String("second".to_string()));
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("name"),
        Some(&PropValue::String("second".to_string()))
    );
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_ops, 1);
    assert_eq!(stats.duplicate_targets, 1);
}

#[test]
fn gql_mixed_create_set_remove_returns_final_created_alias() {
    let (_dir, engine) = query_test_engine();
    let result = engine
        .execute_gql(
            "CREATE (n:GqlMixedCreate {key: 'n', old: 'x'}) SET n.name = 'Ada' REMOVE n.old SET n:GqlMixedExtra RETURN n.name, n.old, labels(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows[0].values[0], GqlValue::String("Ada".to_string()));
    assert_eq!(result.rows[0].values[1], GqlValue::Null);
    match &result.rows[0].values[2] {
        GqlValue::List(labels) => {
            assert!(labels.contains(&GqlValue::String("GqlMixedCreate".to_string())));
            assert!(labels.contains(&GqlValue::String("GqlMixedExtra".to_string())));
        }
        other => panic!("expected labels list, got {other:?}"),
    }
    let stored = engine
        .get_node_by_key("GqlMixedExtra", "n")
        .unwrap()
        .unwrap();
    assert_eq!(stored.props.get("name"), Some(&PropValue::String("Ada".to_string())));
    assert!(!stored.props.contains_key("old"));
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.nodes_updated, 0);
    assert_eq!(stats.mutation_ops, 1);
}

#[test]
fn gql_set_remove_errors_leave_database_unchanged() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "GqlSetAtomic",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );
    let bad_prop = engine
        .execute_gql(
            "MATCH (n:GqlSetAtomic) WHERE n.key = 'n' SET n.status = 'new' SET n.bad = $bad",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(bad_prop, EngineError::InvalidOperation(message) if message.contains("finite")));
    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert_eq!(stored.props.get("status"), Some(&PropValue::String("old".to_string())));
    assert!(!stored.props.contains_key("bad"));

    let a = insert_query_node(&engine, "GqlSetAtomicEdgeNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlSetAtomicEdgeNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_SET_ATOMIC_EDGE",
            UpsertEdgeOptions {
                valid_from: Some(1),
                valid_to: Some(i64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    let bad_window = engine
        .execute_gql(
            "MATCH (a:GqlSetAtomicEdgeNode) WHERE a.key = 'a' MATCH (b:GqlSetAtomicEdgeNode) WHERE b.key = 'b' MATCH (a)-[r:Gql_SET_ATOMIC_EDGE]->(b) SET r.valid_from = 9223372036854775807",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(bad_window, EngineError::InvalidOperation(message) if message.contains("valid_from < valid_to")));
    assert_eq!(engine.get_edge(edge_id).unwrap().unwrap().valid_from, 1);

    insert_query_node(&engine, "GqlSetCap", "a", &[], 1.0);
    insert_query_node(&engine, "GqlSetCap", "b", &[], 1.0);
    let cap = engine
        .execute_gql(
            "MATCH (n:GqlSetCap) SET n.flag = true",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert!(engine
        .get_node_by_key("GqlSetCap", "a")
        .unwrap()
        .unwrap()
        .props
        .get("flag")
        .is_none());
}

#[test]
fn gql_existing_update_cap_uses_final_replacement_count() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "GqlSetRevertCap",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );

    let reverted = engine
        .execute_gql(
            "MATCH (n:GqlSetRevertCap) WHERE n.key = 'n' SET n.status = 'new' SET n.status = 'old'",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 0,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(reverted.mutation_stats.as_ref().unwrap().mutation_ops, 0);
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );

    let changed = engine
        .execute_gql(
            "MATCH (n:GqlSetRevertCap) WHERE n.key = 'n' SET n.status = 'new'",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 0,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(changed, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );
}

#[test]
fn gql_set_label_preserves_vectors() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_query_test_catalog(&engine);
    let node_id = engine
        .upsert_node(
            "GqlVectorSet",
            "n",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(2, 1.0), (2, 0.5)]),
                ..Default::default()
            },
        )
        .unwrap();

    engine
        .execute_gql(
            "MATCH (n:GqlVectorSet) WHERE n.key = 'n' SET n:GqlVectorSetExtra SET n.status = 'ok'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert!(stored.labels.iter().any(|label| label == "GqlVectorSetExtra"));
    assert_eq!(stored.dense_vector, Some(vec![0.1, 0.2, 0.3]));
    assert_eq!(stored.sparse_vector, Some(vec![(2, 1.5)]));
}

#[test]
fn gql_set_label_transfer_uses_final_replacement_key_state() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node_with_labels(
        &engine,
        &["GqlTransferSource", "GqlTransferLabel"],
        "shared",
        &[],
        1.0,
    );
    let target = insert_query_node(&engine, "GqlTransferTarget", "shared", &[], 1.0);

    engine
        .execute_gql(
            "MATCH (a:GqlTransferLabel) WHERE a.key = 'shared' \
             MATCH (b:GqlTransferTarget) WHERE b.key = 'shared' \
             SET b:GqlTransferLabel REMOVE a:GqlTransferLabel",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(
        engine
            .get_node_by_key("GqlTransferLabel", "shared")
            .unwrap()
            .unwrap()
            .id,
        target
    );
    assert!(!engine
        .get_node(source)
        .unwrap()
        .unwrap()
        .labels
        .contains(&"GqlTransferLabel".to_string()));

    let held = insert_query_node(&engine, "GqlConflictHeld", "dup", &[], 1.0);
    let candidate = insert_query_node(&engine, "GqlConflictCandidate", "dup", &[], 1.0);
    let conflict = engine
        .execute_gql(
            "MATCH (n:GqlConflictCandidate) WHERE n.key = 'dup' SET n:GqlConflictHeld",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(conflict, EngineError::InvalidOperation(message) if message.contains("node key conflict")));
    assert_eq!(
        engine
            .get_node_by_key("GqlConflictHeld", "dup")
            .unwrap()
            .unwrap()
            .id,
        held
    );
    assert!(!engine
        .get_node(candidate)
        .unwrap()
        .unwrap()
        .labels
        .contains(&"GqlConflictHeld".to_string()));
}

#[test]
fn gql_set_label_cyclic_transfer_rejects_without_index_corruption() {
    let (_dir, engine) = query_test_engine();
    let left = insert_query_node(&engine, "GqlCycleLeft", "shared", &[], 1.0);
    let right = insert_query_node(&engine, "GqlCycleRight", "shared", &[], 1.0);

    let err = engine
        .execute_gql(
            "MATCH (a:GqlCycleLeft) WHERE a.key = 'shared' \
             MATCH (b:GqlCycleRight) WHERE b.key = 'shared' \
             SET a:GqlCycleRight SET b:GqlCycleLeft REMOVE a:GqlCycleLeft REMOVE b:GqlCycleRight",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InvalidOperation(ref message) if message.contains("cyclic node label/key replacements")),
        "{err:?}"
    );
    assert_eq!(
        engine
            .get_node_by_key("GqlCycleLeft", "shared")
            .unwrap()
            .unwrap()
            .id,
        left
    );
    assert_eq!(
        engine
            .get_node_by_key("GqlCycleRight", "shared")
            .unwrap()
            .unwrap()
            .id,
        right
    );
    assert_eq!(
        engine.get_node(left).unwrap().unwrap().labels,
        vec!["GqlCycleLeft".to_string()]
    );
    assert_eq!(
        engine.get_node(right).unwrap().unwrap().labels,
        vec!["GqlCycleRight".to_string()]
    );
}

#[test]
fn gql_mutation_return_non_mutated_existing_alias_projects_and_commits() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "GqlNoopReturn",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );

    let result = engine
        .execute_gql(
            "MATCH (n:GqlNoopReturn) WHERE n.key = 'n' CREATE (c:GqlNoopReturnCreated {key: 'c'}) SET n.missing = null RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.stats.rows_returned, 1);
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.mutation_ops, 1);
    let returned = gql_single_node(&result.rows[0].values[0]);
    assert_eq!(returned.id, Some(node_id));
    assert_eq!(
        returned.props.as_ref().unwrap().get("status"),
        Some(&GqlValue::String("old".to_string()))
    );
    assert!(!returned.props.as_ref().unwrap().contains_key("missing"));
    let stored = engine.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        stored.props.get("status"),
        Some(&PropValue::String("old".to_string()))
    );
    assert!(!stored.props.contains_key("missing"));
    assert!(engine
        .get_node_by_key("GqlNoopReturnCreated", "c")
        .unwrap()
        .is_some());
}

#[test]
fn gql_mutation_return_compact_rows_and_vectors_are_accepted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_query_test_catalog(&engine);
    let node_id = engine
        .upsert_node(
            "GqlReturnOptions",
            "n",
            UpsertNodeOptions {
                props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(7, 2.5)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();

    let omitted = engine
        .execute_gql(
            "MATCH (n:GqlReturnOptions) WHERE n.key = 'n' SET n.status = 'new' RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let returned = gql_single_node(&omitted.rows[0].values[0]);
    assert_eq!(returned.id, Some(node_id));
    assert!(returned.dense_vector.is_none());
    assert!(returned.sparse_vector.is_none());
    assert_eq!(
        returned.props.as_ref().unwrap().get("status"),
        Some(&GqlValue::String("new".to_string()))
    );

    let vectors = engine
        .execute_gql(
            "MATCH (n:GqlReturnOptions) WHERE n.key = 'n' SET n.status = 'newer' RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_vectors: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let returned = gql_single_node(&vectors.rows[0].values[0]);
    assert_eq!(returned.dense_vector.as_deref(), Some([0.1, 0.2, 0.3].as_slice()));
    assert_eq!(returned.sparse_vector.as_deref(), Some([(7, 2.5)].as_slice()));
    assert_eq!(
        returned.props.as_ref().unwrap().get("status"),
        Some(&GqlValue::String("newer".to_string()))
    );

    let compact = engine
        .execute_gql(
            "MATCH (n:GqlReturnOptions) WHERE n.key = 'n' SET n.status = 'compact' RETURN n.status",
            &GqlParams::new(),
            &GqlExecutionOptions {
                compact_rows: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(compact.columns, vec!["n.status"]);
    assert_eq!(compact.rows[0].values[0], GqlValue::String("compact".to_string()));
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("compact".to_string()))
    );
}

#[test]
fn gql_mutation_profile_db_hits_are_gated_and_nonzero_for_existing_reads() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "GqlMutationProfileHits",
        "a",
        &[("status", PropValue::String("left".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "GqlMutationProfileHits",
        "b",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );
    engine
        .upsert_edge(a, b, "Gql_PROFILE_HITS", UpsertEdgeOptions::default())
        .unwrap();

    let source = format!(
        "MATCH (a:GqlMutationProfileHits)-[r:Gql_PROFILE_HITS]->(b:GqlMutationProfileHits) \
         WHERE id(a) = {a} \
         SET b.status = $status \
         RETURN b.key ORDER BY a.status, type(r)"
    );
    let no_profile = engine
        .execute_gql(
            &source,
            &GqlParams::from([(
                "status".to_string(),
                GqlParamValue::String("first".to_string()),
            )]),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(no_profile.stats.db_hits, 0);
    assert_eq!(no_profile.mutation_stats.as_ref().unwrap().db_hits, 0);

    let profiled_create = engine
        .execute_gql(
            "CREATE (n:GqlMutationProfileCreate {key: 'n'})",
            &GqlParams::new(),
            &GqlExecutionOptions {
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(profiled_create.stats.db_hits, 0);
    assert_eq!(
        profiled_create.mutation_stats.as_ref().unwrap().db_hits,
        0
    );

    let profiled = engine
        .execute_gql(
            &source,
            &GqlParams::from([(
                "status".to_string(),
                GqlParamValue::String("second".to_string()),
            )]),
            &GqlExecutionOptions {
                allow_full_scan: true,
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let mutation_stats = profiled.mutation_stats.as_ref().unwrap();
    assert!(profiled.stats.db_hits > 0);
    assert_eq!(profiled.stats.db_hits, mutation_stats.db_hits);
    assert!(profiled.stats.elapsed_us.is_some());
    assert!(mutation_stats.elapsed_us.is_some());
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("second".to_string()))
    );
}

#[test]
fn gql_mutation_return_row_ops_affect_rows_not_mutations() {
    let (_dir, engine) = query_test_engine();
    for (key, rank) in [("a", 1), ("b", 2), ("c", 3)] {
        insert_query_node(
            &engine,
            "GqlCreateReturnOpsSeed",
            key,
            &[("rank", PropValue::Int(rank))],
            1.0,
        );
    }
    let options = GqlExecutionOptions {
        allow_full_scan: true,
        ..gql_opts()
    };

    let created = engine
        .execute_gql(
            "MATCH (s:GqlCreateReturnOpsSeed) CREATE (n:GqlCreateReturnOps {key: s.key, rank: s.rank}) RETURN n.key ORDER BY n.rank DESC SKIP 1 LIMIT 1",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(gql_string_column(&created, 0), vec!["b".to_string()]);
    assert_eq!(created.stats.rows_returned, 1);
    let stats = created.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_rows, 3);
    assert_eq!(stats.nodes_created, 3);
    for key in ["a", "b", "c"] {
        assert!(engine
            .get_node_by_key("GqlCreateReturnOps", key)
            .unwrap()
            .is_some());
    }

    for (key, rank) in [("a", Some(1)), ("b", Some(1)), ("c", None)] {
        let mut props = Vec::new();
        if let Some(rank) = rank {
            props.push(("rank", PropValue::Int(rank)));
        }
        insert_query_node(&engine, "GqlSetReturnOps", key, &props, 1.0);
    }
    let set = engine
        .execute_gql(
            "MATCH (n:GqlSetReturnOps) SET n.touched = true RETURN n.key ORDER BY n.rank, id(n) SKIP 1 LIMIT 2",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&set, 0),
        vec!["b".to_string(), "c".to_string()]
    );
    assert_eq!(set.stats.rows_returned, 2);
    let stats = set.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_rows, 3);
    assert_eq!(stats.nodes_updated, 3);
    for key in ["a", "b", "c"] {
        assert_eq!(
            engine
                .get_node_by_key("GqlSetReturnOps", key)
                .unwrap()
                .unwrap()
                .props
                .get("touched"),
            Some(&PropValue::Bool(true))
        );
    }

    for (key, rank) in [("low", Some(1)), ("high", Some(3)), ("missing", None)] {
        let mut props = Vec::new();
        if let Some(rank) = rank {
            props.push(("rank", PropValue::Int(rank)));
        }
        insert_query_node(&engine, "GqlNullDescReturnOps", key, &props, 1.0);
    }
    let null_desc = engine
        .execute_gql(
            "MATCH (n:GqlNullDescReturnOps) SET n.checked = true \
             RETURN n.key ORDER BY n.rank DESC LIMIT 1",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(gql_string_column(&null_desc, 0), vec!["high".to_string()]);
    let stats = null_desc.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_rows, 3);
    assert_eq!(stats.nodes_updated, 3);
    for key in ["low", "high", "missing"] {
        assert_eq!(
            engine
                .get_node_by_key("GqlNullDescReturnOps", key)
                .unwrap()
                .unwrap()
                .props
                .get("checked"),
            Some(&PropValue::Bool(true))
        );
    }

    let limit_zero = engine
        .execute_gql(
            "MATCH (n:GqlSetReturnOps) SET n.limit_zero = true RETURN n.key ORDER BY n.rank LIMIT 0",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert!(limit_zero.rows.is_empty());
    assert_eq!(limit_zero.stats.rows_returned, 0);
    let stats = limit_zero.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_rows, 3);
    assert_eq!(stats.nodes_updated, 3);
    for key in ["a", "b", "c"] {
        assert_eq!(
            engine
                .get_node_by_key("GqlSetReturnOps", key)
                .unwrap()
                .unwrap()
                .props
                .get("limit_zero"),
            Some(&PropValue::Bool(true))
        );
    }

    for (key, rank) in [("a", 1), ("b", 2), ("c", 3)] {
        insert_query_node(
            &engine,
            "GqlRemoveReturnOps",
            key,
            &[("rank", PropValue::Int(rank)), ("drop", PropValue::String("x".to_string()))],
            1.0,
        );
    }
    let removed = engine
        .execute_gql(
            "MATCH (n:GqlRemoveReturnOps) REMOVE n.drop RETURN n.key ORDER BY n.rank DESC LIMIT 2",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&removed, 0),
        vec!["c".to_string(), "b".to_string()]
    );
    assert_eq!(removed.stats.rows_returned, 2);
    let stats = removed.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.mutation_rows, 3);
    assert_eq!(stats.nodes_updated, 3);
    for key in ["a", "b", "c"] {
        assert!(!engine
            .get_node_by_key("GqlRemoveReturnOps", key)
            .unwrap()
            .unwrap()
            .props
            .contains_key("drop"));
    }
}

#[test]
fn gql_mutation_return_caps_and_order_errors_are_atomic() {
    let (_dir, engine) = query_test_engine();
    for key in ["a", "b"] {
        insert_query_node(
            &engine,
            "GqlReturnCapRows",
            key,
            &[("status", PropValue::String("old".to_string()))],
            1.0,
        );
    }
    let max_rows = engine
        .execute_gql(
            "MATCH (n:GqlReturnCapRows) SET n.status = 'new' RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        max_rows.to_string().contains("max_rows"),
        "unexpected error: {max_rows:?}"
    );
    for key in ["a", "b"] {
        assert_eq!(
            engine
                .get_node_by_key("GqlReturnCapRows", key)
                .unwrap()
                .unwrap()
                .props
                .get("status"),
            Some(&PropValue::String("old".to_string()))
        );
    }

    let max_skip = engine
        .execute_gql(
            "MATCH (n:GqlReturnCapRows) SET n.status = 'skip' RETURN n SKIP 2",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_skip: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        max_skip.to_string().contains("max_skip"),
        "unexpected error: {max_skip:?}"
    );

    let max_order = engine
        .execute_gql(
            "MATCH (n:GqlReturnCapRows) SET n.status = 'ordered' RETURN n.key ORDER BY n.key",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_order_materialization: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        max_order.to_string().contains("max_order_materialization"),
        "unexpected error: {max_order:?}"
    );

    let unsupported_order = engine
        .execute_gql(
            "MATCH (n:GqlReturnCapRows) SET n.status = 'bad-order' RETURN n.key ORDER BY n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        unsupported_order.to_string().contains("ORDER BY"),
        "unexpected error: {unsupported_order:?}"
    );
    for key in ["a", "b"] {
        assert_eq!(
            engine
                .get_node_by_key("GqlReturnCapRows", key)
                .unwrap()
                .unwrap()
                .props
                .get("status"),
            Some(&PropValue::String("old".to_string()))
        );
    }
}

#[test]
fn gql_mutation_return_prevalidates_order_and_projection_against_final_state() {
    let (_dir, engine) = query_test_engine();
    let rank_id = insert_query_node(
        &engine,
        "GqlReturnFinalValidation",
        "rank",
        &[("rank", PropValue::Int(1))],
        1.0,
    );
    let order_err = engine
        .execute_gql(
            "MATCH (n:GqlReturnFinalValidation) WHERE n.key = 'rank' \
             SET n.rank = [1] RETURN n.key ORDER BY n.rank",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        order_err.to_string().contains("ORDER BY"),
        "unexpected error: {order_err:?}"
    );
    assert_eq!(
        engine
            .get_node(rank_id)
            .unwrap()
            .unwrap()
            .props
            .get("rank"),
        Some(&PropValue::Int(1))
    );

    let mut payload = BTreeMap::new();
    payload.insert("inner".to_string(), PropValue::String("ok".to_string()));
    let nested_id = insert_query_node(
        &engine,
        "GqlReturnFinalValidation",
        "nested",
        &[("payload", PropValue::Map(payload.clone()))],
        1.0,
    );
    let projection_err = engine
        .execute_gql(
            "MATCH (n:GqlReturnFinalValidation) WHERE n.key = 'nested' \
             SET n.payload = 7 RETURN n.payload.inner",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(projection_err, EngineError::GqlSemantic { .. }),
        "unexpected error: {projection_err:?}"
    );
    assert_eq!(
        engine
            .get_node(nested_id)
            .unwrap()
            .unwrap()
            .props
            .get("payload"),
        Some(&PropValue::Map(payload))
    );

    let metadata_id_err = engine
        .execute_gql(
            "MATCH (n:GqlReturnFinalValidation) WHERE n.key = 'rank' \
             SET n.status = 'metadata-id-bad' RETURN n.updated_at.inner",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(metadata_id_err, EngineError::GqlSemantic { .. }),
        "unexpected error: {metadata_id_err:?}"
    );
    assert_eq!(
        engine
            .get_node(rank_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        None
    );
}

#[test]
fn gql_mutation_return_volatile_metadata_order_rejects_before_write() {
    let (_dir, engine) = query_test_engine();
    for key in ["a", "b", "c"] {
        insert_query_node(&engine, "GqlReturnCreatedMetaSeed", key, &[], 1.0);
    }
    let options = GqlExecutionOptions {
        allow_full_scan: true,
        ..gql_opts()
    };

    let node_order = engine
        .execute_gql(
            "MATCH (s:GqlReturnCreatedMetaSeed) \
             CREATE (n:GqlReturnCreatedMeta {key: s.key}) \
             RETURN n.key ORDER BY n.id DESC SKIP 1 LIMIT 1",
            &GqlParams::new(),
            &options,
        )
        .unwrap_err();
    assert!(
        node_order.to_string().contains("ORDER BY"),
        "unexpected error: {node_order:?}"
    );
    for key in ["a", "b", "c"] {
        assert!(engine
            .get_node_by_key("GqlReturnCreatedMeta", key)
            .unwrap()
            .is_none());
    }

    let root = insert_query_node(&engine, "GqlReturnCreatedEdgeMetaRoot", "root", &[], 1.0);
    for key in ["a", "b", "c"] {
        insert_query_node(&engine, "GqlReturnCreatedEdgeMetaTarget", key, &[], 1.0);
    }
    let edge_order = engine
        .execute_gql(
            &format!(
                "MATCH (from:GqlReturnCreatedEdgeMetaRoot) \
                 MATCH (to:GqlReturnCreatedEdgeMetaTarget) \
                 WHERE id(from) = {root} \
                 CREATE (from)-[r:Gql_RETURN_CREATED_EDGE_META]->(to) \
                 RETURN to.key ORDER BY r.to DESC SKIP 1 LIMIT 1"
            ),
            &GqlParams::new(),
            &options,
        )
        .unwrap_err();
    assert!(
        edge_order.to_string().contains("ORDER BY"),
        "unexpected error: {edge_order:?}"
    );
    assert!(engine
        .query_edges(&EdgeQuery {
            label: Some("Gql_RETURN_CREATED_EDGE_META".to_string()),
            ..EdgeQuery::default()
        })
        .unwrap()
        .edges
        .is_empty());

    let changed = insert_query_node(
        &engine,
        "GqlReturnChangedUpdatedAt",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );
    let updated_at_order = engine
        .execute_gql(
            "MATCH (n:GqlReturnChangedUpdatedAt) \
             SET n.status = 'new' RETURN n.key ORDER BY n.updated_at",
            &GqlParams::new(),
            &options,
        )
        .unwrap_err();
    assert!(
        updated_at_order.to_string().contains("ORDER BY"),
        "unexpected error: {updated_at_order:?}"
    );
    assert_eq!(
        engine
            .get_node(changed)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );
}

#[test]
fn gql_mutation_return_gql_read_set_conflicts_for_returned_and_ordered_hydration() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "GqlReturnReadSet",
        "a",
        &[("status", PropValue::String("old-a".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "GqlReturnReadSet",
        "b",
        &[("status", PropValue::String("old-b".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(a, b, "Gql_RETURN_READ_SET", UpsertEdgeOptions::default())
        .unwrap();

    let run_paused = |source: String, engine: &DatabaseEngine| {
        let worker = DatabaseEngine {
            runtime: std::sync::Arc::clone(&engine.runtime),
        };
        let (ready_rx, release_tx) = engine.set_gql_mutation_before_commit_pause();
        let handle = std::thread::spawn(move || {
            worker.execute_gql(
                &source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    ..GqlExecutionOptions::default()
                },
            )
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("GQL mutation did not pause before commit");
        (release_tx, handle)
    };

    let source = format!(
        "MATCH (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'returned-existing-conflict' RETURN a"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine
        .upsert_node(
            "GqlReturnReadSet",
            "a",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("outside-a".to_string()),
                )]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let source = format!(
        "MATCH (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'order-only-node-conflict' \
         RETURN b.key ORDER BY a.status"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine
        .upsert_node(
            "GqlReturnReadSet",
            "a",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("outside-order-a".to_string()),
                )]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let source = format!(
        "MATCH (a:GqlReturnReadSet)-[r:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'order-only-edge-conflict' \
         RETURN b.key ORDER BY r.status"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine.delete_edge(edge).unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let edge = engine
        .upsert_edge(a, b, "Gql_RETURN_READ_SET", UpsertEdgeOptions::default())
        .unwrap();
    let source = format!(
        "MATCH p = (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'path-conflict' RETURN p"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine.delete_edge(edge).unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let edge = engine
        .upsert_edge(a, b, "Gql_RETURN_READ_SET", UpsertEdgeOptions::default())
        .unwrap();
    let source = format!(
        "MATCH p = (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'start-node-conflict' RETURN start_node(p)"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine
        .upsert_node(
            "GqlReturnReadSet",
            "a",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("outside-start".to_string()),
                )]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let source = format!(
        "MATCH p = (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'relationships-conflict' RETURN relationships(p)"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine.delete_edge(edge).unwrap();
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)), "{err:?}");
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old-b".to_string()))
    );

    let edge = engine
        .upsert_edge(a, b, "Gql_RETURN_READ_SET", UpsertEdgeOptions::default())
        .unwrap();
    let source = format!(
        "MATCH p = (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'path-helper-no-conflict' RETURN node_ids(p)"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine.delete_edge(edge).unwrap();
    release_tx.send(()).unwrap();
    let helper = handle.join().unwrap().unwrap();
    assert_eq!(helper.stats.rows_returned, 1);
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("path-helper-no-conflict".to_string()))
    );

    let edge = engine
        .upsert_edge(a, b, "Gql_RETURN_READ_SET", UpsertEdgeOptions::default())
        .unwrap();
    let source = format!(
        "MATCH p = (a:GqlReturnReadSet)-[:Gql_RETURN_READ_SET]->(b:GqlReturnReadSet) \
         WHERE id(a) = {a} SET b.status = 'limit-zero-no-conflict' RETURN p LIMIT 0"
    );
    let (release_tx, handle) = run_paused(source, &engine);
    engine.delete_edge(edge).unwrap();
    release_tx.send(()).unwrap();
    let limit_zero = handle.join().unwrap().unwrap();
    assert!(limit_zero.rows.is_empty());
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("limit-zero-no-conflict".to_string()))
    );
}

#[test]
fn gql_mutation_return_paths_and_existing_aliases_project_after_commit() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "GqlReturnPath",
        "a",
        &[("status", PropValue::String("old-a".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "GqlReturnPath",
        "b",
        &[("status", PropValue::String("old-b".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(
            a,
            b,
            "Gql_RETURN_PATH",
            UpsertEdgeOptions {
                props: query_test_props(&[("kind", PropValue::String("direct".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    let source = format!(
        "MATCH p = (a:GqlReturnPath)-[r:Gql_RETURN_PATH]->(b:GqlReturnPath) \
         WHERE id(a) = {a} \
         SET b.status = 'new-b' \
         RETURN p, a.status, b.status, length(p), node_ids(p), edge_ids(p), r.kind, \
                start_node(p), end_node(p), nodes(p), relationships(p)"
    );
    let result = engine
        .execute_gql(&source, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    let path = gql_single_path(&values[0]);
    assert_eq!(path.node_ids, vec![a, b]);
    assert_eq!(path.edge_ids, vec![edge]);
    assert_eq!(path.nodes.as_ref().unwrap().len(), 2);
    assert_eq!(path.edges.as_ref().unwrap().len(), 1);
    assert_eq!(values[1], GqlValue::String("old-a".to_string()));
    assert_eq!(values[2], GqlValue::String("new-b".to_string()));
    assert_eq!(values[3], GqlValue::UInt(1));
    assert_eq!(
        values[4],
        GqlValue::List(vec![GqlValue::UInt(a), GqlValue::UInt(b)])
    );
    assert_eq!(values[5], GqlValue::List(vec![GqlValue::UInt(edge)]));
    assert_eq!(values[6], GqlValue::String("direct".to_string()));
    assert_eq!(gql_single_node(&values[7]).id, Some(a));
    assert_eq!(gql_single_node(&values[8]).id, Some(b));
    let GqlValue::List(nodes) = &values[9] else {
        panic!("expected nodes(p) list");
    };
    assert_eq!(nodes.len(), 2);
    assert_eq!(gql_single_node(&nodes[0]).id, Some(a));
    assert_eq!(gql_single_node(&nodes[1]).id, Some(b));
    let GqlValue::List(edges) = &values[10] else {
        panic!("expected relationships(p) list");
    };
    assert_eq!(edges.len(), 1);
    assert_eq!(gql_single_edge(&edges[0]).id, Some(edge));
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("new-b".to_string()))
    );

    let invalid_projection = engine
        .execute_gql(
            &format!(
                "MATCH p = (a:GqlReturnPath)-[:Gql_RETURN_PATH]->(b:GqlReturnPath) \
                 WHERE id(a) = {a} SET b.status = 'bad-limit-zero' \
                 RETURN start_node(p).key LIMIT 0"
            ),
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(invalid_projection, EngineError::GqlSemantic { .. }),
        "unexpected error: {invalid_projection:?}"
    );
    assert_eq!(
        engine
            .get_node(b)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("new-b".to_string()))
    );
}

#[test]
fn gql_mutation_return_missing_params_and_unsupported_projection_are_atomic() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "GqlReturnPrevalidate",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );

    let missing = engine
        .execute_gql(
            "MATCH (n:GqlReturnPrevalidate) WHERE n.key = 'n' SET n.status = 'new' RETURN $missing",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_gql_param_error(missing, "missing", "missing");
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );

    let unsupported = engine
        .execute_gql(
            "MATCH (n:GqlReturnPrevalidate) WHERE n.key = 'n' SET n.status = 'new' RETURN relationships(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(unsupported, EngineError::GqlSemantic { .. } | EngineError::GqlUnsupported { .. }),
        "{unsupported:?}"
    );
    assert_eq!(
        engine
            .get_node(node_id)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );
}

#[test]
fn gql_set_remove_edge_index_flush_reopen_and_stale_candidates() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    engine
        .ensure_edge_property_index("Gql_EDGE_INDEX", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let a = insert_query_node(&engine, "GqlEdgeIndexNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlEdgeIndexNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_EDGE_INDEX",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let edge_ids_for = |engine: &DatabaseEngine, status: &str| {
        engine
            .query_edge_ids(&EdgeQuery {
                label: Some("Gql_EDGE_INDEX".to_string()),
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "status".to_string(),
                    value: PropValue::String(status.to_string()),
                }),
                ..Default::default()
            })
            .unwrap()
            .edge_ids
    };

    engine
        .execute_gql(
            "MATCH (a:GqlEdgeIndexNode) WHERE a.key = 'a' \
             MATCH (b:GqlEdgeIndexNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_EDGE_INDEX]->(b) SET r.status = 'new'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(edge_ids_for(&engine, "new"), vec![edge_id]);
    assert!(edge_ids_for(&engine, "old").is_empty());
    engine.flush().unwrap();
    assert_eq!(edge_ids_for(&engine, "new"), vec![edge_id]);

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(edge_ids_for(&reopened, "new"), vec![edge_id]);
    let removed = reopened
        .execute_gql(
            "MATCH (a:GqlEdgeIndexNode) WHERE a.key = 'a' \
             MATCH (b:GqlEdgeIndexNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_EDGE_INDEX]->(b) REMOVE r.status RETURN r.status",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(removed.rows[0].values[0], GqlValue::Null);
    assert!(edge_ids_for(&reopened, "new").is_empty());
    let stale_candidate_read = execute_gql_ok(
        &reopened,
        "MATCH ()-[r:Gql_EDGE_INDEX {status: 'new'}]->() RETURN id(r)",
    );
    assert!(stale_candidate_read.rows.is_empty());
}

#[test]
fn gql_delete_edge_dedupes_updates_indexes_and_survives_reopen() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    engine
        .ensure_edge_property_index("Gql_DELETE_EDGE", "status", SecondaryIndexKind::Equality)
        .unwrap();
    let a = insert_query_node(&engine, "GqlDeleteEdgeNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlDeleteEdgeNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_DELETE_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("live".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (a:GqlDeleteEdgeNode) WHERE a.key = 'a' \
             MATCH (b:GqlDeleteEdgeNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_DELETE_EDGE {status: 'live'}]->(b) DELETE r DELETE r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(result.rows.is_empty());
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.rows_matched, 1);
    assert_eq!(stats.mutation_rows, 1);
    assert_eq!(stats.mutation_ops, 1);
    assert_eq!(stats.edges_deleted, 1);
    assert_eq!(stats.duplicate_targets, 1);
    assert!(engine.get_edge(edge_id).unwrap().is_none());
    let stale_index_read = execute_gql_ok(
        &engine,
        "MATCH ()-[r:Gql_DELETE_EDGE {status: 'live'}]->() RETURN id(r)",
    );
    assert!(stale_index_read.rows.is_empty());

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(reopened.get_edge(edge_id).unwrap().is_none());
}

#[test]
fn gql_delete_same_edge_across_multiple_rows_deletes_once() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "GqlDeleteRowsNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlDeleteRowsNode", "b", &[], 1.0);
    insert_query_node(&engine, "GqlDeleteRowsMarker", "x1", &[], 1.0);
    insert_query_node(&engine, "GqlDeleteRowsMarker", "x2", &[], 1.0);
    let edge_id = engine
        .upsert_edge(a, b, "Gql_DELETE_ROWS", UpsertEdgeOptions::default())
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (a:GqlDeleteRowsNode) WHERE a.key = 'a' \
             MATCH (b:GqlDeleteRowsNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_DELETE_ROWS]->(b) MATCH (x:GqlDeleteRowsMarker) DELETE r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.rows_matched, 2);
    assert_eq!(stats.edges_deleted, 1);
    assert_eq!(stats.mutation_ops, 1);
    assert_eq!(stats.duplicate_targets, 1);
    assert!(engine.get_edge(edge_id).unwrap().is_none());
}

#[test]
fn gql_detach_delete_node_cascades_active_and_segment_edges_once() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let hub = insert_query_node(&engine, "GqlDetachNode", "hub", &[], 1.0);
    let left = insert_query_node(&engine, "GqlDetachNode", "left", &[], 1.0);
    let right = insert_query_node(&engine, "GqlDetachNode", "right", &[], 1.0);
    let segment_edge = engine
        .upsert_edge(hub, left, "Gql_DETACH_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    let active_edge = engine
        .upsert_edge(right, hub, "Gql_DETACH_EDGE", UpsertEdgeOptions::default())
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (n:GqlDetachNode) WHERE n.key = 'hub' DETACH DELETE n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_deleted, 1);
    assert_eq!(stats.edges_deleted, 2);
    assert_eq!(stats.mutation_ops, 3);
    assert!(engine.get_node(hub).unwrap().is_none());
    assert!(engine.get_edge(segment_edge).unwrap().is_none());
    assert!(engine.get_edge(active_edge).unwrap().is_none());

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(reopened.get_node(hub).unwrap().is_none());
    assert!(reopened.get_edge(segment_edge).unwrap().is_none());
    assert!(reopened.get_edge(active_edge).unwrap().is_none());
}

#[test]
fn gql_detach_delete_dedupes_shared_and_direct_cascade_edges() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "GqlDetachDedupeNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlDetachDedupeNode", "b", &[], 1.0);
    let shared = engine
        .upsert_edge(a, b, "Gql_DETACH_DEDUPE", UpsertEdgeOptions::default())
        .unwrap();

    let shared_result = engine
        .execute_gql(
            "MATCH (a:GqlDetachDedupeNode) WHERE a.key = 'a' \
             MATCH (b:GqlDetachDedupeNode) WHERE b.key = 'b' DETACH DELETE a DETACH DELETE b",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let shared_stats = shared_result.mutation_stats.as_ref().unwrap();
    assert_eq!(shared_stats.nodes_deleted, 2);
    assert_eq!(shared_stats.edges_deleted, 1);
    assert_eq!(shared_stats.mutation_ops, 3);
    assert!(engine.get_edge(shared).unwrap().is_none());

    let c = insert_query_node(&engine, "GqlDetachDedupeNode", "c", &[], 1.0);
    let d = insert_query_node(&engine, "GqlDetachDedupeNode", "d", &[], 1.0);
    let direct = engine
        .upsert_edge(c, d, "Gql_DETACH_DIRECT", UpsertEdgeOptions::default())
        .unwrap();
    let direct_result = engine
        .execute_gql(
            "MATCH (c:GqlDetachDedupeNode) WHERE c.key = 'c' \
             MATCH (d:GqlDetachDedupeNode) WHERE d.key = 'd' \
             MATCH (c)-[r:Gql_DETACH_DIRECT]->(d) DELETE r DETACH DELETE c",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let direct_stats = direct_result.mutation_stats.as_ref().unwrap();
    assert_eq!(direct_stats.nodes_deleted, 1);
    assert_eq!(direct_stats.edges_deleted, 1);
    assert_eq!(direct_stats.mutation_ops, 2);
    assert_eq!(direct_stats.duplicate_targets, 1);
    assert!(engine.get_edge(direct).unwrap().is_none());
    assert!(engine.get_node(d).unwrap().is_some());
}

#[test]
fn gql_delete_optional_null_targets_are_noops() {
    let (_dir, engine) = query_test_engine();
    let root = insert_query_node(&engine, "GqlDeleteOptional", "root", &[], 1.0);
    let result = engine
        .execute_gql(
            "MATCH (n:GqlDeleteOptional) WHERE n.key = 'root' \
             OPTIONAL MATCH (n)-[r:Gql_DELETE_MISSING]->(m) DELETE r DETACH DELETE m",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.rows_matched, 1);
    assert_eq!(stats.mutation_rows, 0);
    assert_eq!(stats.mutation_ops, 0);
    assert_eq!(stats.skipped_null_targets, 2);
    assert_eq!(stats.nodes_deleted, 0);
    assert_eq!(stats.edges_deleted, 0);
    assert!(engine.get_node(root).unwrap().is_some());
}

#[test]
fn gql_delete_wins_over_earlier_replacements() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "GqlDeleteWinsNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlDeleteWinsNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "Gql_DELETE_WINS",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("old".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (a:GqlDeleteWinsNode) WHERE a.key = 'a' \
             MATCH (b:GqlDeleteWinsNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_DELETE_WINS]->(b) SET r.status = 'new' DELETE r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let stats = result.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.edges_deleted, 1);
    assert_eq!(stats.edges_updated, 0);
    assert_eq!(stats.properties_set, 0);
    assert_eq!(stats.mutation_ops, 1);
    assert_eq!(stats.duplicate_targets, 1);
    assert!(engine.get_edge(edge_id).unwrap().is_none());
}

#[test]
fn gql_delete_created_edge_and_detach_created_node_use_local_refs() {
    let (_dir, engine) = query_test_engine();
    let direct = engine
        .execute_gql(
            "CREATE (a:GqlCreatedEdgeDelete {key: 'a'})-[r:Gql_CREATED_EDGE_DELETE]->(b:GqlCreatedEdgeDelete {key: 'b'}) DELETE r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let direct_stats = direct.mutation_stats.as_ref().unwrap();
    assert_eq!(direct_stats.nodes_created, 2);
    assert_eq!(direct_stats.edges_created, 0);
    assert_eq!(direct_stats.edges_deleted, 0);
    assert!(engine
        .query_edges(&EdgeQuery {
            label: Some("Gql_CREATED_EDGE_DELETE".to_string()),
            ..Default::default()
        })
        .unwrap()
        .edges
        .is_empty());

    let detached = engine
        .execute_gql(
            "CREATE (a:GqlCreatedDetach {key: 'a'})-[r:Gql_CREATED_DETACH]->(b:GqlCreatedDetach {key: 'b'}) DETACH DELETE a",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let detached_stats = detached.mutation_stats.as_ref().unwrap();
    assert_eq!(detached_stats.nodes_created, 1);
    assert_eq!(detached_stats.nodes_deleted, 0);
    assert_eq!(detached_stats.edges_created, 0);
    assert_eq!(detached_stats.edges_deleted, 0);
    assert!(engine
        .get_node_by_key("GqlCreatedDetach", "a")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("GqlCreatedDetach", "b")
        .unwrap()
        .is_some());
    assert!(engine
        .query_edges(&EdgeQuery {
            label: Some("Gql_CREATED_DETACH".to_string()),
            ..Default::default()
        })
        .unwrap()
        .edges
        .is_empty());
}

#[test]
fn gql_delete_caps_fail_before_staging_or_commit() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "GqlDeleteCapNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlDeleteCapNode", "b", &[], 1.0);
    let edge_id = engine
        .upsert_edge(a, b, "Gql_DELETE_CAP", UpsertEdgeOptions::default())
        .unwrap();

    let direct_cap = engine
        .execute_gql(
            "MATCH (a:GqlDeleteCapNode) WHERE a.key = 'a' \
             MATCH (b:GqlDeleteCapNode) WHERE b.key = 'b' \
             MATCH (a)-[r:Gql_DELETE_CAP]->(b) DELETE r",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 0,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(direct_cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert!(engine.get_edge(edge_id).unwrap().is_some());

    let detach_cap = engine
        .execute_gql(
            "MATCH (n:GqlDeleteCapNode) WHERE n.key = 'a' DETACH DELETE n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(detach_cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert!(engine.get_node(a).unwrap().is_some());
    assert!(engine.get_edge(edge_id).unwrap().is_some());

    let row_cap = engine
        .execute_gql(
            "MATCH (a:GqlDeleteCapNode)-[r:Gql_DELETE_CAP]->(b:GqlDeleteCapNode) DELETE r",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_rows: 0,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(row_cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_rows")));
    assert!(engine.get_edge(edge_id).unwrap().is_some());
}

#[test]
fn gql_detach_delete_cap_bounds_high_fanout_cascade() {
    let (_dir, engine) = query_test_engine();
    let hub = insert_query_node(&engine, "GqlDetachCapHub", "hub", &[], 1.0);
    let mut edge_ids = Vec::new();
    for idx in 0..8 {
        let leaf = insert_query_node(
            &engine,
            "GqlDetachCapLeaf",
            &format!("segment-{idx}"),
            &[],
            1.0,
        );
        edge_ids.push(
            engine
                .upsert_edge(hub, leaf, "Gql_DETACH_CAP_FANOUT", UpsertEdgeOptions::default())
                .unwrap(),
        );
    }
    engine.flush().unwrap();
    for idx in 0..8 {
        let leaf = insert_query_node(
            &engine,
            "GqlDetachCapLeaf",
            &format!("active-{idx}"),
            &[],
            1.0,
        );
        edge_ids.push(
            engine
                .upsert_edge(hub, leaf, "Gql_DETACH_CAP_FANOUT", UpsertEdgeOptions::default())
                .unwrap(),
        );
    }

    let err = engine
        .execute_gql(
            "MATCH (n:GqlDetachCapHub) WHERE n.key = 'hub' DETACH DELETE n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 3,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert!(engine.get_node(hub).unwrap().is_some());
    for edge_id in edge_ids {
        assert!(engine.get_edge(edge_id).unwrap().is_some());
    }
}

#[test]
fn gql_detach_delete_commit_budget_bounds_edges_added_after_snapshot() {
    let (_dir, engine) = query_test_engine();
    let hub = insert_query_node(&engine, "GqlDetachCommitCapHub", "hub", &[], 1.0);
    let worker = DatabaseEngine {
        runtime: std::sync::Arc::clone(&engine.runtime),
    };
    let (ready_rx, release_tx) = engine.set_gql_mutation_before_commit_pause();
    let handle = std::thread::spawn(move || {
        worker.execute_gql(
            "MATCH (n:GqlDetachCommitCapHub) WHERE n.key = 'hub' DETACH DELETE n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_mutation_ops: 2,
                ..GqlExecutionOptions::default()
            },
        )
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("GQL mutation did not pause before commit");

    let mut edge_ids = Vec::new();
    for idx in 0..8 {
        let leaf = insert_query_node(
            &engine,
            "GqlDetachCommitCapLeaf",
            &format!("leaf-{idx}"),
            &[],
            1.0,
        );
        edge_ids.push(
            engine
                .upsert_edge(
                    hub,
                    leaf,
                    "Gql_DETACH_COMMIT_CAP",
                    UpsertEdgeOptions::default(),
                )
                .unwrap(),
        );
    }
    release_tx.send(()).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    assert!(engine.get_node(hub).unwrap().is_some());
    for edge_id in edge_ids {
        assert!(engine.get_edge(edge_id).unwrap().is_some());
    }
}

#[test]
fn gql_delete_rejections_still_happen_before_writes() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(&engine, "GqlDeleteReject", "n", &[], 1.0);
    let delete_node = engine
        .execute_gql(
            "MATCH (n:GqlDeleteReject) WHERE n.key = 'n' DELETE n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        delete_node,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));
    assert!(engine.get_node(node_id).unwrap().is_some());

    let return_after_delete = engine
        .execute_gql(
            "MATCH (n:GqlDeleteReject) WHERE n.key = 'n' DETACH DELETE n RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        return_after_delete,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));
    assert!(engine.get_node(node_id).unwrap().is_some());

    let cursor_first = engine
        .execute_gql(
            "MATCH (n:GqlDeleteReject) WHERE n.key = 'n' DETACH DELETE n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some("read-cursor".to_string()),
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    match cursor_first {
        EngineError::InvalidCursor { message } => {
            assert_eq!(message, "GQL mutation statements do not accept cursors");
        }
        err => panic!("expected mutation cursor error, got {err:?}"),
    }
    assert!(engine.get_node(node_id).unwrap().is_some());
}

#[test]
fn gql_replacement_adapter_static_audit_keeps_public_surfaces_clean() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let forbidden = [
        ["Replace", "Node"].concat(),
        ["Replace", "Edge"].concat(),
    ];
    for path in [
        "src/types.rs",
        "overgraph-node/src/lib.rs",
        "overgraph-node/index.d.ts",
        "overgraph-node/query-types.d.ts",
        "overgraph-python/src/lib.rs",
        "overgraph-python/python/overgraph/__init__.pyi",
        "overgraph-python/python/overgraph/async_api.py",
    ] {
        let contents = std::fs::read_to_string(manifest_dir.join(path)).unwrap();
        for needle in &forbidden {
            assert!(
                !contents.contains(needle),
                "{path} exposes a public replacement transaction API"
            );
        }
    }
}

#[test]
fn gql_delete_static_audit_uses_transaction_intents_not_public_delete_loops() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let query = std::fs::read_to_string(manifest_dir.join("src/engine/query.rs")).unwrap();
    assert!(query.contains("TxnIntent::DeleteNode"));
    assert!(query.contains("TxnIntent::DeleteEdge"));
    assert!(query.contains("txn_delete_incident_edge_ids_limited"));
    assert!(!query.contains(".delete_node("));
    assert!(!query.contains(".delete_edge("));

    let txn = std::fs::read_to_string(manifest_dir.join("src/engine/txn.rs")).unwrap();
    assert!(txn.contains("pub(crate) struct TxnGraphOpBudget"));
    assert!(txn.contains("fn incident_edge_ids_for_txn_delete_limited"));
    assert!(txn.contains("fn limited_scan_len"));
    for needle in [
        "pub struct TxnGraphOpBudget",
        "pub fn gql_apply_mutation_op_budget",
    ] {
        assert!(
            !txn.contains(needle),
            "transaction mutation budget helper leaked into the public API"
        );
    }
}

#[test]
fn gql_mutation_return_static_audit_keeps_read_set_private_and_projection_batched() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let txn = std::fs::read_to_string(manifest_dir.join("src/engine/txn.rs")).unwrap();
    assert!(txn.contains("pub(crate) struct TxnReturnReadSet"));
    assert!(txn.contains("pub(crate) fn gql_validate_return_read_set"));
    assert!(txn.contains("pub(crate) fn commit_with_gql_return_view"));
    let read_set_start = txn.find("fn validate_gql_return_read_set").unwrap();
    let read_set_end = txn[read_set_start..]
        .find("fn resolve_node_ref_required")
        .map(|offset| read_set_start + offset)
        .unwrap();
    let read_set_body = &txn[read_set_start..read_set_end];
    assert!(read_set_body.contains("self.get_nodes_raw(&node_ids)?"));
    assert!(read_set_body.contains("self.get_edges(&edge_ids)?"));
    assert!(!read_set_body.contains("validate_node_id_conflict"));
    assert!(!read_set_body.contains("validate_edge_id_conflict"));
    for needle in [
        "pub struct TxnReturnReadSet",
        "pub fn gql_validate_return_read_set",
        "pub fn commit_with_gql_return_view",
    ] {
        assert!(
            !txn.contains(needle),
            "GQL mutation RETURN read-set/view helper leaked into the public transaction API"
        );
    }

    let query = std::fs::read_to_string(manifest_dir.join("src/engine/query.rs")).unwrap();
    assert!(query.contains("view.get_nodes_raw(&node_ids)"));
    assert!(query.contains("view.get_edges(&edge_ids)"));
    assert!(!query.contains(".get_node("));
    assert!(!query.contains(".get_edge("));
    assert!(query.contains("fn execute_gql_mutation("));
    assert!(query.contains("fn explain_gql_mutation("));
    let execute_start = query.find("fn execute_gql_create_mutation").unwrap();
    let execute_end = query[execute_start..]
        .find("fn gql_create_input_rows")
        .map(|offset| execute_start + offset)
        .unwrap();
    let execute_body = &query[execute_start..execute_end];
    assert!(execute_body.contains("let snapshot = txn.gql_snapshot()?;"));
    assert!(execute_body.contains("build_gql_mutation_explain_with_snapshot"));
    assert!(
        execute_body.find("let snapshot = txn.gql_snapshot()?;").unwrap()
            < execute_body
                .find("build_gql_mutation_explain_with_snapshot")
                .unwrap(),
        "embedded mutation explain must use the transaction snapshot"
    );
    let explain_start = query
        .find("fn build_gql_mutation_explain_with_snapshot")
        .unwrap();
    let explain_end = query[explain_start..]
        .find("fn gql_execution_cap_summary")
        .map(|offset| explain_start + offset)
        .unwrap();
    let explain_body = &query[explain_start..explain_end];
    assert!(
        !explain_body.contains("published_snapshot"),
        "snapshot-specific mutation explain builder must not capture a second snapshot"
    );
    assert!(query.contains("gql_mutation_return_needs_committed_view"));
    assert!(query.contains("if selected.is_empty()"));
}

#[test]
fn gql_create_node_survives_reopen() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    engine
        .execute_gql(
            "CREATE (n:GqlReopen {key: 'persisted', name: 'stored'}) RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    drop(engine);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let node = reopened
        .get_node_by_key("GqlReopen", "persisted")
        .unwrap()
        .unwrap();
    assert_eq!(node.props.get("name"), Some(&PropValue::String("stored".to_string())));
}

#[test]
fn gql_create_edge_label_survives_reopen() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    let result = engine
        .execute_gql(
            "CREATE (a:GqlEdgeReopen {key: 'a'})-[r:Gql_EDGE_REOPEN {since: 7}]->(b:GqlEdgeReopen {key: 'b'}) RETURN id(a), id(r), id(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let a_id = gql_u64_column(&result, 0)[0];
    let edge_id = gql_u64_column(&result, 1)[0];
    let b_id = gql_u64_column(&result, 2)[0];
    drop(engine);

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(
        reopened
            .get_node_by_key("GqlEdgeReopen", "a")
            .unwrap()
            .unwrap()
            .id,
        a_id
    );
    assert_eq!(
        reopened
            .get_node_by_key("GqlEdgeReopen", "b")
            .unwrap()
            .unwrap()
            .id,
        b_id
    );
    let edge = reopened.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(edge.from, a_id);
    assert_eq!(edge.to, b_id);
    assert_eq!(edge.label, "Gql_EDGE_REOPEN");
    assert_eq!(edge.props.get("since"), Some(&PropValue::Int(7)));
    assert_eq!(
        reopened
            .get_edge_by_triple(a_id, b_id, "Gql_EDGE_REOPEN")
            .unwrap()
            .unwrap()
            .id,
        edge_id
    );
}

#[test]
fn mutation_explain_includes_read_prefix_and_operations() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "explain-mutation-ada", &[], 1.0);

    let explain = engine
        .explain_gql(
            "MATCH (n:Person {key: 'explain-mutation-ada'}) SET n.name = 'Ada' RETURN n.name ORDER BY n.name SKIP 0 LIMIT 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(explain.kind, GqlStatementKind::Mutation);
    assert_eq!(explain.columns, vec!["n.name"]);
    assert!(matches!(
        explain.read.as_ref().map(|read| read.target),
        Some(GqlLoweringTarget::GraphRowQuery)
    ));
    let mutation = explain.mutation.expect("mutation explain");
    assert!(mutation.uses_write_txn);
    assert!(mutation.uses_transaction_snapshot);
    assert!(mutation.atomic_commit);
    assert!(mutation.replacement_adapters);
    let read_prefix = mutation.read_prefix.expect("read prefix explain");
    assert_eq!(read_prefix.graph_row_target.target, GqlLoweringTarget::GraphRowQuery);
    assert!(read_prefix
        .internal_columns
        .iter()
        .any(|column| column.contains("target id: n")));
    assert!(mutation
        .operations
        .iter()
        .any(|op| op.op == "SET PROPERTY" && op.target_alias.as_deref() == Some("n")));
    let return_plan = mutation.return_plan.as_ref().expect("return explain");
    assert_eq!(return_plan.columns, vec!["n.name"]);
    assert_eq!(return_plan.order_items, 1);
    assert_eq!(return_plan.skip, 0);
    assert_eq!(return_plan.limit, Some(1));
    assert!(return_plan.post_commit_hydration.contains("prevalidates"));
    assert!(return_plan.post_commit_hydration.contains("read-set"));

    let param_explain = engine
        .explain_gql(
            "MATCH (n:Person {key: 'explain-mutation-ada'}) SET n.name = 'Ada' \
             RETURN n.name ORDER BY n.name SKIP $skip LIMIT $limit",
            &GqlParams::from([
                ("skip".to_string(), GqlParamValue::UInt(2)),
                ("limit".to_string(), GqlParamValue::Int(3)),
            ]),
            &gql_opts(),
        )
        .unwrap();
    let mutation = param_explain.mutation.expect("mutation explain");
    let return_plan = mutation.return_plan.as_ref().expect("return explain");
    assert_eq!(return_plan.skip, 2);
    assert_eq!(return_plan.limit, Some(3));

    let full_scan_explain = engine
        .explain_gql(
            "MATCH (n) SET n.name = 'Ada'",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let mutation = full_scan_explain.mutation.expect("mutation explain");
    let read_prefix = mutation.read_prefix.expect("read prefix explain");
    assert!(read_prefix
        .graph_row_target
        .warnings
        .iter()
        .any(|warning| warning.contains("full scan")));
}

#[derive(Clone)]
struct RichGqlGraph {
    alice: u64,
    bob: u64,
    acme: u64,
    globex: u64,
    lead_edge: u64,
    review_edge: u64,
    startup_edge: u64,
    mentor_edge: u64,
}

#[derive(Clone, Copy)]
struct RichGqlIndexes {
    employee_status: u64,
    employee_score: u64,
    works_role: u64,
    works_hours: u64,
}

fn seed_rich_gql_graph(engine: &DatabaseEngine) -> RichGqlGraph {
    let acme = insert_query_node(
        engine,
        "Company",
        "rich-acme",
        &[("tier", PropValue::String("enterprise".to_string()))],
        3.0,
    );
    let globex = insert_query_node(
        engine,
        "Company",
        "rich-globex",
        &[("tier", PropValue::String("startup".to_string()))],
        2.0,
    );
    let alice = insert_query_node_with_labels(
        engine,
        &["Person", "Employee", "Manager"],
        "rich-alice",
        &[
            ("status", PropValue::String("focus".to_string())),
            ("score", PropValue::Int(91)),
            ("department", PropValue::String("platform".to_string())),
            ("rank", PropValue::Int(2)),
        ],
        1.25,
    );
    let bob = insert_query_node_with_labels(
        engine,
        &["Person", "Employee"],
        "rich-bob",
        &[
            ("status", PropValue::String("focus".to_string())),
            ("score", PropValue::Int(76)),
            ("department", PropValue::String("platform".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.5,
    );
    insert_query_node_with_labels(
        engine,
        &["Person", "Employee"],
        "rich-carol",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("score", PropValue::Int(88)),
            ("department", PropValue::String("research".to_string())),
            ("rank", PropValue::Null),
        ],
        1.0,
    );
    insert_query_node_with_labels(
        engine,
        &["Person", "Contractor"],
        "rich-dana",
        &[
            ("status", PropValue::String("focus".to_string())),
            ("score", PropValue::Int(85)),
        ],
        1.0,
    );
    insert_query_node(
        engine,
        "Person",
        "rich-eve",
        &[
            ("status", PropValue::String("focus".to_string())),
            ("score", PropValue::Int(82)),
        ],
        1.0,
    );
    insert_query_node_with_labels(
        engine,
        &["Person", "Employee"],
        "rich-frank",
        &[
            ("status", PropValue::String("focus".to_string())),
            ("score", PropValue::Int(63)),
        ],
        1.0,
    );
    insert_query_node_with_labels(
        engine,
        &["Person", "Employee"],
        "rich-grace",
        &[("score", PropValue::Int(99))],
        1.0,
    );

    for index in 0..24 {
        let status = if index % 4 == 0 { "focus" } else { "inactive" };
        let filler = insert_query_node_with_labels(
            engine,
            &["Person", "Employee"],
            &format!("rich-filler-{index:02}"),
            &[
                ("status", PropValue::String(status.to_string())),
                ("score", PropValue::Int(20 + i64::from(index))),
            ],
            0.5,
        );
        if index < 12 {
            engine
                .upsert_edge(
                    filler,
                    globex,
                    "WORKS_ON",
                    UpsertEdgeOptions {
                        props: query_test_props(&[
                            ("role", PropValue::String("support".to_string())),
                            ("hours", PropValue::Int(5 + i64::from(index))),
                        ]),
                        weight: 0.25,
                        valid_from: Some(10),
                        valid_to: Some(20),
                    },
                )
                .unwrap();
        }
    }

    let lead_edge = engine
        .upsert_edge(
            alice,
            acme,
            "WORKS_ON",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("role", PropValue::String("lead".to_string())),
                    ("hours", PropValue::Int(40)),
                ]),
                weight: 2.5,
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
            },
        )
        .unwrap();
    let review_edge = engine
        .upsert_edge(
            bob,
            acme,
            "WORKS_ON",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("role", PropValue::String("reviewer".to_string())),
                    ("hours", PropValue::Int(35)),
                ]),
                weight: 1.75,
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
            },
        )
        .unwrap();
    let startup_edge = engine
        .upsert_edge(
            alice,
            globex,
            "WORKS_ON",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("role", PropValue::String("lead".to_string())),
                    ("hours", PropValue::Int(10)),
                ]),
                weight: 0.75,
                valid_from: Some(0),
                valid_to: Some(i64::MAX),
            },
        )
        .unwrap();
    let mentor_edge = engine
        .upsert_edge(
            alice,
            bob,
            "MENTORS",
            UpsertEdgeOptions {
                props: query_test_props(&[("role", PropValue::String("mentor".to_string()))]),
                weight: 1.0,
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            bob,
            globex,
            "MENTORS",
            UpsertEdgeOptions {
                props: query_test_props(&[("role", PropValue::String("mentor".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    RichGqlGraph {
        alice,
        bob,
        acme,
        globex,
        lead_edge,
        review_edge,
        startup_edge,
        mentor_edge,
    }
}

fn install_rich_gql_indexes(engine: &DatabaseEngine) -> RichGqlIndexes {
    let employee_status = engine
        .ensure_node_property_index("Employee", "status", SecondaryIndexKind::Equality)
        .unwrap()
        .index_id;
    wait_for_property_index_state(engine, employee_status, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, employee_status, SecondaryIndexState::Ready);

    let employee_score = engine
        .ensure_node_property_index(
            "Employee",
            "score",
            SecondaryIndexKind::Range,
        )
        .unwrap()
        .index_id;
    wait_for_property_index_state(engine, employee_score, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, employee_score, SecondaryIndexState::Ready);

    let works_role = engine
        .ensure_edge_property_index("WORKS_ON", "role", SecondaryIndexKind::Equality)
        .unwrap()
        .index_id;
    wait_for_edge_property_index_state(engine, works_role, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, works_role, SecondaryIndexState::Ready);

    let works_hours = engine
        .ensure_edge_property_index(
            "WORKS_ON",
            "hours",
            SecondaryIndexKind::Range,
        )
        .unwrap()
        .index_id;
    wait_for_edge_property_index_state(engine, works_hours, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, works_hours, SecondaryIndexState::Ready);

    RichGqlIndexes {
        employee_status,
        employee_score,
        works_role,
        works_hours,
    }
}

fn node_prop_i64(engine: &DatabaseEngine, id: u64, key: &str) -> i64 {
    match engine
        .get_node(id)
        .unwrap()
        .unwrap()
        .props
        .get(key)
        .unwrap()
    {
        PropValue::Int(value) => *value,
        other => panic!("expected int node property {key}, got {other:?}"),
    }
}

fn edge_prop_i64(engine: &DatabaseEngine, id: u64, key: &str) -> i64 {
    match engine
        .get_edge(id)
        .unwrap()
        .unwrap()
        .props
        .get(key)
        .unwrap()
    {
        PropValue::Int(value) => *value,
        other => panic!("expected int edge property {key}, got {other:?}"),
    }
}

fn sorted_rich_employee_focus_score_oracle(engine: &DatabaseEngine, min_score: i64) -> Vec<u64> {
    let mut native = engine
        .query_node_ids(&NodeQuery {
            label_filter: Some(node_label_filter(
                &["Person", "Employee"],
                LabelMatchMode::All,
            )),
            filter: Some(NodeFilterExpr::And(vec![
                NodeFilterExpr::PropertyIn {
                    key: "status".to_string(),
                    values: vec![PropValue::String("focus".to_string())],
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(min_score))),
                    upper: None,
                },
            ])),
            ..NodeQuery::default()
        })
        .unwrap()
        .items;
    native.sort_by(|left, right| {
        let left_node = engine.get_node(*left).unwrap().unwrap();
        let right_node = engine.get_node(*right).unwrap().unwrap();
        node_prop_i64(engine, *left, "score")
            .cmp(&node_prop_i64(engine, *right, "score"))
            .then_with(|| left_node.key.cmp(&right_node.key))
            .then_with(|| left.cmp(right))
    });
    native
}

fn sorted_rich_work_edge_oracle(engine: &DatabaseEngine, min_hours: i64) -> Vec<u64> {
    let mut native = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("WORKS_ON".to_string()),
            filter: Some(EdgeFilterExpr::And(vec![
                EdgeFilterExpr::PropertyIn {
                    key: "role".to_string(),
                    values: vec![
                        PropValue::String("lead".to_string()),
                        PropValue::String("reviewer".to_string()),
                    ],
                },
                EdgeFilterExpr::PropertyRange {
                    key: "hours".to_string(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(min_hours))),
                    upper: None,
                },
            ])),
            ..EdgeQuery::default()
        })
        .unwrap()
        .edge_ids;
    native.sort_by(|left, right| {
        edge_prop_i64(engine, *left, "hours")
            .cmp(&edge_prop_i64(engine, *right, "hours"))
            .then_with(|| left.cmp(right))
    });
    native
}

fn rich_pattern_oracle(engine: &DatabaseEngine, role: &str) -> Vec<(u64, u64, u64)> {
    let mut query = GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "p".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Person".to_string(), "Employee".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyEquals {
                        key: "status".to_string(),
                        value: PropValue::String("focus".to_string()),
                    }),
            },
            GraphNodePattern {
                alias: "c".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Company".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: Some(NodeFilterExpr::PropertyEquals {
                        key: "tier".to_string(),
                        value: PropValue::String("enterprise".to_string()),
                    }),
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("r".to_string()),
                from_alias: "p".to_string(),
                to_alias: "c".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["WORKS_ON".to_string()],
                filter: Some(EdgeFilterExpr::PropertyEquals {
                    key: "role".to_string(),
                    value: PropValue::String(role.to_string()),
                }),
            })],
        where_: None,
        return_items: Some(vec![
            GraphReturnItem {
                expr: GraphExpr::Binding("p".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("p".to_string()),
            },
            GraphReturnItem {
                expr: GraphExpr::Binding("r".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("r".to_string()),
            },
            GraphReturnItem {
                expr: GraphExpr::Binding("c".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("c".to_string()),
            },
        ]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 100,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions::default(),
    };
    query.options.allow_full_scan = true;
    let mut matches = engine
        .query_graph_rows(&query)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [
                GraphValue::NodeId(p),
                GraphValue::EdgeId(r),
                GraphValue::NodeId(c),
            ] => (*p, *r, *c),
            other => panic!("expected graph-row id tuple, got {other:?}"),
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        engine
            .get_node(left.0)
            .unwrap()
            .unwrap()
            .key
            .cmp(&engine.get_node(right.0).unwrap().unwrap().key)
            .then_with(|| left.1.cmp(&right.1))
    });
    matches
}

#[test]
fn gql_node_query_executes_and_matches_native_node_oracle() {
    let (_dir, engine) = query_test_engine();
    let active = insert_query_node(
        &engine,
        "Person",
        "active-node",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "inactive-node",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );

    let native = engine
        .query_node_ids(&NodeQuery {
            label_filter: Some(node_label_filter(&["Person"], LabelMatchMode::All)),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("active".to_string()),
            }),
            ..NodeQuery::default()
        })
        .unwrap()
        .items;
    let gql = execute_gql_ok(
        &engine,
        "MATCH (n:Person {status: 'active'}) RETURN id(n) AS id",
    );

    assert_eq!(native, vec![active]);
    assert_eq!(gql.columns, vec!["id"]);
    assert_eq!(gql_u64_column(&gql, 0), native);
    assert_eq!(gql.stats.rows_matched, 1);
    assert_eq!(gql.stats.rows_after_filter, 1);
    assert_eq!(gql.stats.rows_returned, 1);

    let id_float_eq = execute_gql_ok(
        &engine,
        &format!("MATCH (n) WHERE id(n) = {active}.0 RETURN id(n)"),
    );
    assert_eq!(gql_u64_column(&id_float_eq, 0), vec![active]);

    let id_float_in = execute_gql_ok(
        &engine,
        &format!("MATCH (n) WHERE id(n) IN [{active}.0] RETURN id(n)"),
    );
    assert_eq!(gql_u64_column(&id_float_in, 0), vec![active]);
}

#[test]
fn gql_edge_query_executes_and_matches_native_edge_oracle() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "edge-from", &[], 1.0);
    let to = insert_query_node(&engine, "Article", "edge-to", &[], 1.0);
    let other_to = insert_query_node(&engine, "Article", "edge-other-to", &[], 1.0);
    let keep = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2024))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            from,
            other_to,
            "MENTIONS",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2025))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            to,
            from,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2019))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    let native = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("LIKES".to_string()),
            filter: Some(EdgeFilterExpr::PropertyRange {
                key: "since".to_string(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(2020))),
                upper: None,
            }),
            ..EdgeQuery::default()
        })
        .unwrap()
        .edge_ids;
    let gql = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() WHERE r.since >= 2020 RETURN id(r) AS id",
    );

    assert_eq!(native, vec![keep]);
    assert_eq!(gql_u64_column(&gql, 0), native);

    let endpoint_float_ids = execute_gql_ok(
        &engine,
        &format!("MATCH ()-[r:LIKES]->() WHERE r.from = {from}.0 AND r.to IN [{to}.0] RETURN id(r)"),
    );
    assert_eq!(gql_u64_column(&endpoint_float_ids, 0), vec![keep]);

    let id_float_eq = execute_gql_ok(
        &engine,
        &format!("MATCH ()-[r]->() WHERE id(r) = {keep}.0 RETURN id(r)"),
    );
    assert_eq!(gql_u64_column(&id_float_eq, 0), vec![keep]);

    let id_float_in = execute_gql_ok(
        &engine,
        &format!("MATCH ()-[r]->() WHERE id(r) IN [{keep}.0] RETURN id(r)"),
    );
    assert_eq!(gql_u64_column(&id_float_in, 0), vec![keep]);

    let mut edge_id_params = GqlParams::new();
    edge_id_params.insert("rid".to_string(), GqlParamValue::UInt(keep));
    let id_param = execute_gql_with_params(
        &engine,
        "MATCH ()-[r]->() WHERE id(r) = $rid RETURN id(r)",
        edge_id_params.clone(),
    );
    assert_eq!(gql_u64_column(&id_param, 0), vec![keep]);

    let explain = engine
        .explain_gql(
            "MATCH ()-[r]->() WHERE id(r) = $rid RETURN id(r)",
            &edge_id_params,
            &gql_opts(),
        )
        .unwrap();
    let explain = gql_read_explain(&explain);
    assert!(!explain.caps.allow_full_scan);
    assert!(explain
        .pushed_down
        .iter()
        .any(|push| push == &format!("id(r) = {keep}")));

    let rejected_optional = engine
        .execute_gql(
            "MATCH ()-[r]->() WHERE id(r) = $rid \
             OPTIONAL MATCH ()-[s]->() RETURN id(r), id(s)",
            &edge_id_params,
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        rejected_optional,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::FullScanNotAllowed,
            ..
        }
    ));

    for index in 0..4 {
        insert_query_node(&engine, "Person", &format!("edge-id-cap-extra-{index}"), &[], 1.0);
    }
    let capped_edge_id = execute_gql_with_options(
        &engine,
        &format!("MATCH ()-[r]->() WHERE id(r) = {keep} RETURN id(r)"),
        GqlExecutionOptions {
            max_intermediate_bindings: 1,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(gql_u64_column(&capped_edge_id, 0), vec![keep]);

    let capped_endpoint_and_edge_id = execute_gql_with_options(
        &engine,
        &format!("MATCH ()-[r]->() WHERE r.from = {from} AND id(r) = {keep} RETURN id(r)"),
        GqlExecutionOptions {
            max_intermediate_bindings: 1,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(gql_u64_column(&capped_endpoint_and_edge_id, 0), vec![keep]);
}

#[test]
fn gql_fixed_one_hop_and_chained_patterns_match_native_oracles() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "chain-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "chain-b", &[], 1.0);
    let c = insert_query_node(&engine, "Article", "chain-c", &[], 1.0);
    let knows = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let likes = engine
        .upsert_edge(b, c, "LIKES", UpsertEdgeOptions::default())
        .unwrap();

    let one_hop = execute_gql_ok(
        &engine,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN id(a), id(r), id(b)",
    );
    assert_eq!(one_hop.rows.len(), 1);
    assert_eq!(one_hop.rows[0].values, vec![
        GqlValue::UInt(a),
        GqlValue::UInt(knows),
        GqlValue::UInt(b),
    ]);

    let edge_id_eq = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             WHERE id(r) = {knows}.0 RETURN id(r)"
        ),
    );
    assert_eq!(gql_u64_column(&edge_id_eq, 0), vec![knows]);

    let edge_id_in = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             WHERE id(r) IN [{knows}.0] RETURN id(r)"
        ),
    );
    assert_eq!(gql_u64_column(&edge_id_in, 0), vec![knows]);

    let low_cap_edge_id_pattern = execute_gql_with_options(
        &engine,
        &format!("MATCH (a)-[r]->(b) WHERE id(r) = {likes} RETURN id(a), id(r), id(b)"),
        GqlExecutionOptions {
            max_intermediate_bindings: 1,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(low_cap_edge_id_pattern.rows.len(), 1);
    assert_eq!(low_cap_edge_id_pattern.rows[0].values, vec![
        GqlValue::UInt(b),
        GqlValue::UInt(likes),
        GqlValue::UInt(c),
    ]);

    let conflicting_edge_id_pattern = execute_gql_ok(
        &engine,
        &format!("MATCH (a)-[r]->(b) WHERE id(r) = {knows} AND id(r) = {likes} RETURN id(r)"),
    );
    assert!(conflicting_edge_id_pattern.rows.is_empty());

    let chained = execute_gql_ok(
        &engine,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person)-[s:LIKES]->(c:Article) \
         RETURN id(a), id(r), id(b), id(s), id(c)",
    );
    assert_eq!(chained.rows.len(), 1);
    assert_eq!(chained.rows[0].values, vec![
        GqlValue::UInt(a),
        GqlValue::UInt(knows),
        GqlValue::UInt(b),
        GqlValue::UInt(likes),
        GqlValue::UInt(c),
    ]);
}

#[test]
fn gql_optional_match_preserves_graph_row_outer_apply_semantics() {
    let (_dir, engine) = query_test_engine();
    let a_hit = insert_query_node(&engine, "Person", "gql-optional-hit-a", &[], 1.0);
    let b_hit = insert_query_node(&engine, "Person", "gql-optional-hit-b", &[], 1.0);
    let a_miss = insert_query_node(&engine, "Person", "gql-optional-miss-a", &[], 1.0);
    let b_miss = insert_query_node(&engine, "Person", "gql-optional-miss-b", &[], 1.0);
    let c1 = insert_query_node(&engine, "Company", "gql-optional-c1", &[], 1.0);
    let c2 = insert_query_node(&engine, "Company", "gql-optional-c2", &[], 1.0);
    engine
        .upsert_edge(
            a_hit,
            b_hit,
            "GQL_OPTIONAL_REQUIRED",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine
        .upsert_edge(
            a_miss,
            b_miss,
            "GQL_OPTIONAL_REQUIRED",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    let s1 = engine
        .upsert_edge(
            b_hit,
            c1,
            "GQL_OPTIONAL_HIT",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    let s2 = engine
        .upsert_edge(
            b_hit,
            c2,
            "GQL_OPTIONAL_HIT",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    let result = execute_gql_ok(
        &engine,
        "MATCH (a:Person)-[:GQL_OPTIONAL_REQUIRED]->(b:Person) \
         OPTIONAL MATCH (b)-[s:GQL_OPTIONAL_HIT]->(c:Company) \
         RETURN id(a), id(s), id(c) ORDER BY id(a), id(c)",
    );
    assert_eq!(
        result.rows.iter().map(|row| row.values.clone()).collect::<Vec<_>>(),
        vec![
            vec![GqlValue::UInt(a_hit), GqlValue::UInt(s1), GqlValue::UInt(c1)],
            vec![GqlValue::UInt(a_hit), GqlValue::UInt(s2), GqlValue::UInt(c2)],
            vec![GqlValue::UInt(a_miss), GqlValue::Null, GqlValue::Null],
        ]
    );

    let filtered_miss = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:Person)-[:GQL_OPTIONAL_REQUIRED]->(b:Person) \
             WHERE id(a) = {a_hit} \
             OPTIONAL MATCH (b)-[s:GQL_OPTIONAL_HIT]->(c:Company) WHERE s.status = 'active' \
             RETURN id(a), id(s), id(c)"
        ),
    );
    assert_eq!(
        filtered_miss.rows[0].values,
        vec![GqlValue::UInt(a_hit), GqlValue::Null, GqlValue::Null]
    );

    let chained_miss = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:Person)-[:GQL_OPTIONAL_REQUIRED]->(b:Person) \
             WHERE id(a) = {a_hit} \
             OPTIONAL MATCH (b)-[s:GQL_OPTIONAL_MISSING]->(c:Company) \
             OPTIONAL MATCH (c)-[t:GQL_OPTIONAL_SECOND]->(d:Topic) \
             RETURN id(s), id(c), id(t), id(d)"
        ),
    );
    assert_eq!(
        chained_miss.rows[0].values,
        vec![GqlValue::Null, GqlValue::Null, GqlValue::Null, GqlValue::Null]
    );
}

#[test]
fn gql_optional_reused_node_constraints_are_optional_local() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "gql-optional-reuse-a", &[], 1.0);
    let b = insert_query_node(&engine, "Company", "gql-optional-reuse-b", &[], 1.0);
    let c = insert_query_node(&engine, "Topic", "gql-optional-reuse-c", &[], 1.0);
    engine
        .upsert_edge(a, b, "GQL_OPTIONAL_REUSE_R", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, c, "GQL_OPTIONAL_REUSE_S", UpsertEdgeOptions::default())
        .unwrap();

    let result = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:Person) WHERE id(a) = {a} \
             OPTIONAL MATCH (a)-[:GQL_OPTIONAL_REUSE_R]->(b:Company) \
             OPTIONAL MATCH (b:Person)-[:GQL_OPTIONAL_REUSE_S]->(c) \
             RETURN id(b), id(c)"
        ),
    );
    assert_eq!(
        result.rows[0].values,
        vec![GqlValue::UInt(b), GqlValue::Null]
    );
}

#[test]
fn gql_bounded_vlp_path_assignment_functions_and_cursors_match_graph_row() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "PathStart", "gql-path-a", &[], 1.0);
    let b = insert_query_node(&engine, "PathNode", "gql-path-b", &[], 1.0);
    let c = insert_query_node(&engine, "PathNode", "gql-path-c", &[], 1.0);
    let ab = engine
        .upsert_edge(a, b, "GQL_PATH", UpsertEdgeOptions::default())
        .unwrap();
    let ac = engine
        .upsert_edge(a, c, "GQL_PATH", UpsertEdgeOptions::default())
        .unwrap();
    let bc = engine
        .upsert_edge(b, c, "GQL_PATH", UpsertEdgeOptions::default())
        .unwrap();
    let ca = engine
        .upsert_edge(c, a, "GQL_PATH", UpsertEdgeOptions::default())
        .unwrap();

    let source = format!(
        "MATCH p = (a)-[:GQL_PATH*0..2]->(z) WHERE id(a) = {a} \
         RETURN p, node_ids(p), edge_ids(p), length(p) \
         ORDER BY p"
    );
    let gql = execute_gql_ok(&engine, &source);

    let mut native = graph_query(
        &["a", "z"],
        vec![graph_vlp(Some("p"), None, "a", "z", 0, 2)],
    );
    native.nodes[0].ids = vec![a];
    if let GraphPatternPiece::VariableLength(path) = &mut native.pieces[0] {
        path.label_filter = vec!["GQL_PATH".to_string()];
    }
    native.return_items = Some(vec![graph_return_binding(
        "p",
        GraphReturnProjection::Element(GraphElementProjection::Full),
    )]);
    native.order_by = vec![
        GraphOrderItem {
            expr: GraphExpr::Binding("p".to_string()),
            direction: GraphOrderDirection::Asc,
        },
    ];
    let native_paths = graph_row_path_ids(engine.query_graph_rows(&native).unwrap());
    let gql_paths = gql
        .rows
        .iter()
        .map(|row| {
            let path = gql_single_path(&row.values[0]);
            assert_eq!(
                row.values[1],
                GqlValue::List(path.node_ids.iter().copied().map(GqlValue::UInt).collect())
            );
            assert_eq!(
                row.values[2],
                GqlValue::List(path.edge_ids.iter().copied().map(GqlValue::UInt).collect())
            );
            assert_eq!(row.values[3], GqlValue::UInt(path.edge_ids.len() as u64));
            (path.node_ids.clone(), path.edge_ids.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(gql_paths, native_paths);
    assert_eq!(
        gql_paths,
        vec![
            (vec![a], vec![]),
            (vec![a, b], vec![ab]),
            (vec![a, c], vec![ac]),
            (vec![a, b, c], vec![ab, bc]),
            (vec![a, c, a], vec![ac, ca]),
        ]
    );

    let two_hop = execute_gql_ok(
        &engine,
        &format!(
            "MATCH p = (a)-[:GQL_PATH*0..2]->(z) \
             WHERE id(a) = {a} AND length(p) = 2 \
             RETURN edge_ids(p) ORDER BY p"
        ),
    );
    assert_eq!(
        two_hop.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
        vec![
            GqlValue::List(vec![GqlValue::UInt(ab), GqlValue::UInt(bc)]),
            GqlValue::List(vec![GqlValue::UInt(ac), GqlValue::UInt(ca)]),
        ]
    );

    let path_function_values = execute_gql_ok(
        &engine,
        &format!(
            "MATCH p = (a)-[:GQL_PATH*1..1]->(z) WHERE id(a) = {a} \
             RETURN start_node(p), end_node(p), nodes(p), relationships(p) ORDER BY p LIMIT 1"
        ),
    );
    let values = &path_function_values.rows[0].values;
    assert_eq!(values[0], GqlValue::UInt(a));
    assert_eq!(values[1], GqlValue::UInt(b));
    let GqlValue::List(nodes) = &values[2] else {
        panic!("expected nodes(p) list");
    };
    assert_eq!(nodes, &vec![GqlValue::UInt(a), GqlValue::UInt(b)]);
    let GqlValue::List(edges) = &values[3] else {
        panic!("expected relationships(p) list");
    };
    assert_eq!(edges, &vec![GqlValue::UInt(ab)]);

    let mut page_options = GqlExecutionOptions {
        max_rows: 1,
        ..GqlExecutionOptions::default()
    };
    let mut cursor = None;
    let mut paged = Vec::new();
    loop {
        page_options.cursor = cursor.take();
        let page = execute_gql_with_options(&engine, &source, page_options.clone());
        if let Some(next) = page.next_cursor.clone() {
            assert!(next.starts_with("ogr32c1_"));
            cursor = Some(next);
        }
        paged.extend(page.rows.into_iter().map(|row| {
            let path = gql_single_path(&row.values[0]);
            (path.node_ids.clone(), path.edge_ids.clone())
        }));
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(paged, native_paths);

    let compact = execute_gql_with_options(
        &engine,
        &source,
        GqlExecutionOptions {
            compact_rows: true,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(
        compact
            .rows
            .iter()
            .map(|row| {
                let path = gql_single_path(&row.values[0]);
                (path.node_ids.clone(), path.edge_ids.clone())
            })
            .collect::<Vec<_>>(),
        native_paths
    );

    let first_page_cursor = execute_gql_with_options(
        &engine,
        &source,
        GqlExecutionOptions {
            max_rows: 1,
            ..GqlExecutionOptions::default()
        },
    )
    .next_cursor;
    page_options.cursor = first_page_cursor.clone();
    let mismatch = engine
        .execute_gql(
            &format!(
                "MATCH p = (a)-[:GQL_PATH*0..2]->(z) WHERE id(a) = {a} \
                 RETURN p ORDER BY length(p)"
            ),
            &GqlParams::new(),
            &page_options,
        )
        .unwrap_err();
    assert!(matches!(mismatch, EngineError::InvalidCursor { .. }));

    let oversized_cursor = engine
        .execute_gql(
            &source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: first_page_cursor,
                max_rows: 1,
                max_cursor_bytes: 8,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(oversized_cursor, EngineError::InvalidCursor { .. }));
}

#[test]
fn gql_fixed_multi_hop_path_assignment_composes_after_fixed_matching() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "FixedPathStart", "gql-fixed-path-a", &[], 1.0);
    let b = insert_query_node(&engine, "FixedPathMid", "gql-fixed-path-b", &[], 1.0);
    let c = insert_query_node(&engine, "FixedPathEnd", "gql-fixed-path-c", &[], 1.0);
    let ab = engine
        .upsert_edge(
            a,
            b,
            "GQL_FIXED_PATH_R",
            UpsertEdgeOptions {
                props: query_test_props(&[("kind", PropValue::String("first".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let cb = engine
        .upsert_edge(c, b, "GQL_FIXED_PATH_S", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, "GQL_FIXED_PATH_R", UpsertEdgeOptions::default())
        .unwrap();

    let source = format!(
        "MATCH p = (a:FixedPathStart)-[:GQL_FIXED_PATH_R {{kind: 'first'}}]->(b)<-[s:GQL_FIXED_PATH_S]-(c) \
         WHERE id(a) = {a} \
         RETURN p, node_ids(p), edge_ids(p), length(p), id(s)"
    );
    let result = execute_gql_ok(&engine, &source);
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    let path = gql_single_path(&values[0]);
    assert_eq!(path.node_ids, vec![a, b, c]);
    assert_eq!(path.edge_ids, vec![ab, cb]);
    assert_eq!(
        values[1],
        GqlValue::List(vec![GqlValue::UInt(a), GqlValue::UInt(b), GqlValue::UInt(c)])
    );
    assert_eq!(
        values[2],
        GqlValue::List(vec![GqlValue::UInt(ab), GqlValue::UInt(cb)])
    );
    assert_eq!(values[3], GqlValue::UInt(2));
    assert_eq!(values[4], GqlValue::UInt(cb));

    let explain = engine
        .explain_gql(
            &source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let explain = gql_read_explain(&explain);
    assert!(explain
        .projection
        .iter()
        .any(|item| item.contains("FixedPathCompose")));
}

#[test]
fn gql_optional_fixed_multi_hop_path_assignment_null_extends_and_filters() {
    let (_dir, engine) = query_test_engine();
    let hit = insert_query_node(&engine, "FixedPathAnchor", "gql-fixed-path-hit", &[], 1.0);
    let miss = insert_query_node(&engine, "FixedPathAnchor", "gql-fixed-path-miss", &[], 1.0);
    let mid = insert_query_node(&engine, "FixedPathMid", "gql-fixed-path-mid", &[], 1.0);
    let end = insert_query_node(&engine, "FixedPathEnd", "gql-fixed-path-end", &[], 1.0);
    let hm = engine
        .upsert_edge(hit, mid, "GQL_OPTIONAL_FIXED_R", UpsertEdgeOptions::default())
        .unwrap();
    let me = engine
        .upsert_edge(mid, end, "GQL_OPTIONAL_FIXED_S", UpsertEdgeOptions::default())
        .unwrap();

    let result = execute_gql_ok(
        &engine,
        "MATCH (a:FixedPathAnchor) \
         OPTIONAL MATCH p = (a)-[:GQL_OPTIONAL_FIXED_R]->(b)-[:GQL_OPTIONAL_FIXED_S]->(c) \
         WHERE length(p) = 2 \
         RETURN id(a), p, length(p) ORDER BY id(a)",
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(hit));
    let path = gql_single_path(&result.rows[0].values[1]);
    assert_eq!(path.node_ids, vec![hit, mid, end]);
    assert_eq!(path.edge_ids, vec![hm, me]);
    assert_eq!(result.rows[0].values[2], GqlValue::UInt(2));
    assert_eq!(result.rows[1].values[0], GqlValue::UInt(miss));
    assert_eq!(result.rows[1].values[1], GqlValue::Null);
    assert_eq!(result.rows[1].values[2], GqlValue::Null);
}

#[test]
fn gql_fixed_multi_hop_path_assignment_uses_final_row_cursors() {
    let (_dir, engine) = query_test_engine();
    let a1 = insert_query_node(&engine, "FixedPathPageStart", "gql-fixed-page-a1", &[], 1.0);
    let b1 = insert_query_node(&engine, "FixedPathPageMid", "gql-fixed-page-b1", &[], 1.0);
    let c1 = insert_query_node(&engine, "FixedPathPageEnd", "gql-fixed-page-c1", &[], 1.0);
    let a2 = insert_query_node(&engine, "FixedPathPageStart", "gql-fixed-page-a2", &[], 1.0);
    let b2 = insert_query_node(&engine, "FixedPathPageMid", "gql-fixed-page-b2", &[], 1.0);
    let c2 = insert_query_node(&engine, "FixedPathPageEnd", "gql-fixed-page-c2", &[], 1.0);
    let a1b1 = engine
        .upsert_edge(a1, b1, "GQL_FIXED_PAGE_R", UpsertEdgeOptions::default())
        .unwrap();
    let b1c1 = engine
        .upsert_edge(b1, c1, "GQL_FIXED_PAGE_S", UpsertEdgeOptions::default())
        .unwrap();
    let a2b2 = engine
        .upsert_edge(a2, b2, "GQL_FIXED_PAGE_R", UpsertEdgeOptions::default())
        .unwrap();
    let b2c2 = engine
        .upsert_edge(b2, c2, "GQL_FIXED_PAGE_S", UpsertEdgeOptions::default())
        .unwrap();

    let source = "MATCH p = (a:FixedPathPageStart)-[:GQL_FIXED_PAGE_R]->(b)-[:GQL_FIXED_PAGE_S]->(c) \
                  RETURN p ORDER BY p";
    let mut options = GqlExecutionOptions {
        max_rows: 1,
        ..GqlExecutionOptions::default()
    };
    let mut cursor = None;
    let mut paths = Vec::new();
    loop {
        options.cursor = cursor.take();
        let page = execute_gql_with_options(&engine, source, options.clone());
        paths.extend(page.rows.iter().map(|row| {
            let path = gql_single_path(&row.values[0]);
            (path.node_ids.clone(), path.edge_ids.clone())
        }));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        paths,
        vec![
            (vec![a1, b1, c1], vec![a1b1, b1c1]),
            (vec![a2, b2, c2], vec![a2b2, b2c2]),
        ]
    );

    let first_cursor = execute_gql_with_options(
        &engine,
        source,
        GqlExecutionOptions {
            max_rows: 1,
            ..GqlExecutionOptions::default()
        },
    )
    .next_cursor
    .expect("first page should emit a cursor");
    let mismatch = engine
        .execute_gql(
            "MATCH p = (a:FixedPathPageStart)-[:GQL_FIXED_PAGE_R]->(b)-[:GQL_FIXED_PAGE_S]->(c) \
             RETURN edge_ids(p) ORDER BY p",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(first_cursor),
                max_rows: 1,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(mismatch, EngineError::InvalidCursor { .. }));
}

#[test]
fn gql_vlp_direction_self_loop_and_parallel_edges_match_graph_row() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "DirectionPath", "gql-direction-a", &[], 1.0);
    let b = insert_query_node(&engine, "DirectionPath", "gql-direction-b", &[], 1.0);
    let incoming_edge = engine
        .upsert_edge(b, a, "GQL_INCOMING_PATH", UpsertEdgeOptions::default())
        .unwrap();

    let incoming_gql = execute_gql_ok(
        &engine,
        &format!(
            "MATCH p = (a)<-[:GQL_INCOMING_PATH*1..1]-(b) \
             WHERE id(a) = {a} AND id(b) = {b} RETURN p"
        ),
    );
    let incoming_path = gql_single_path(&incoming_gql.rows[0].values[0]);
    assert_eq!(incoming_path.node_ids, vec![a, b]);
    assert_eq!(incoming_path.edge_ids, vec![incoming_edge]);

    let mut incoming_native = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 1)],
    );
    if let GraphPatternPiece::VariableLength(path) = &mut incoming_native.pieces[0] {
        path.direction = Direction::Incoming;
        path.label_filter = vec!["GQL_INCOMING_PATH".to_string()];
    }
    incoming_native.nodes[0].ids = vec![a];
    incoming_native.nodes[1].ids = vec![b];
    incoming_native.return_items = Some(vec![graph_return_binding(
        "p",
        GraphReturnProjection::Element(GraphElementProjection::Full),
    )]);
    assert_eq!(
        vec![(incoming_path.node_ids.clone(), incoming_path.edge_ids.clone())],
        graph_row_path_ids(engine.query_graph_rows(&incoming_native).unwrap())
    );

    let loop_node = insert_query_node(&engine, "DirectionPath", "gql-direction-loop", &[], 1.0);
    let loop_edge = engine
        .upsert_edge(
            loop_node,
            loop_node,
            "GQL_BOTH_PATH",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    let p1 = engine
        .upsert_edge(a, b, "GQL_BOTH_PATH", UpsertEdgeOptions::default())
        .unwrap();
    let p2 = engine
        .upsert_edge(a, b, "GQL_BOTH_PATH", UpsertEdgeOptions::default())
        .unwrap();

    let self_loop = execute_gql_ok(
        &engine,
        &format!(
            "MATCH p = (n)-[:GQL_BOTH_PATH*1..1]-(n) WHERE id(n) = {loop_node} RETURN p"
        ),
    );
    let loop_path = gql_single_path(&self_loop.rows[0].values[0]);
    assert_eq!(loop_path.node_ids, vec![loop_node, loop_node]);
    assert_eq!(loop_path.edge_ids, vec![loop_edge]);

    let parallel = execute_gql_ok(
        &engine,
        &format!(
            "MATCH p = (a)-[:GQL_BOTH_PATH*1..1]-(b) \
             WHERE id(a) = {a} AND id(b) = {b} RETURN p ORDER BY p"
        ),
    );
    let parallel_paths = parallel
        .rows
        .iter()
        .map(|row| {
            let path = gql_single_path(&row.values[0]);
            (path.node_ids.clone(), path.edge_ids.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(parallel_paths, vec![(vec![a, b], vec![p1]), (vec![a, b], vec![p2])]);
}

#[test]
fn gql_vlp_caps_surface_graph_row_errors() {
    let (_dir, engine) = query_test_engine();
    let start = insert_query_node(&engine, "GqlVlpCap", "gql-vlp-cap-start", &[], 1.0);
    let a = insert_query_node(&engine, "GqlVlpCap", "gql-vlp-cap-a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlVlpCap", "gql-vlp-cap-b", &[], 1.0);
    engine
        .upsert_edge(start, a, "GQL_VLP_CAP", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(start, b, "GQL_VLP_CAP", UpsertEdgeOptions::default())
        .unwrap();

    let err = engine
        .execute_gql(
            &format!(
                "MATCH p = (a)-[:GQL_VLP_CAP*1..1]->(b) WHERE id(a) = {start} RETURN p"
            ),
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_intermediate_bindings: 1,
                max_frontier: 1,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("max_frontier"));
    assert!(message.contains("configured cap 1"));
    assert!(message.contains("path=p"));
}

#[test]
fn gql_vlp_source_correctness_matches_graph_row_oracle() {
    let (_dir, engine) = query_test_engine();
    let start = insert_query_node(&engine, "GqlVlpSource", "gql-vlp-source-start", &[], 1.0);
    let keep_mid = insert_query_node(&engine, "GqlVlpSource", "gql-vlp-source-mid", &[], 1.0);
    let keep_end = insert_query_node(
        &engine,
        "GqlVlpEnd",
        "gql-vlp-source-keep",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let drop_end = insert_query_node(
        &engine,
        "GqlVlpEnd",
        "gql-vlp-source-drop",
        &[("status", PropValue::String("drop".to_string()))],
        1.0,
    );
    let deleted_end = insert_query_node(
        &engine,
        "GqlVlpEnd",
        "gql-vlp-source-deleted",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    let pruned_end = insert_query_node(
        &engine,
        "GqlVlpEnd",
        "gql-vlp-source-pruned",
        &[("status", PropValue::String("keep".to_string()))],
        0.1,
    );
    let first = engine
        .upsert_edge(
            start,
            keep_mid,
            "GQL_VLP_SOURCE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("open".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            keep_mid,
            keep_end,
            "GQL_VLP_SOURCE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("open".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            start,
            drop_end,
            "GQL_VLP_SOURCE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("open".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let deleted_edge = engine
        .upsert_edge(
            start,
            deleted_end,
            "GQL_VLP_SOURCE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("open".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            start,
            pruned_end,
            "GQL_VLP_SOURCE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("open".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine.delete_node(deleted_end).unwrap();
    engine.delete_edge(deleted_edge).unwrap();
    engine
        .set_prune_policy(
            "gql-vlp-low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("GqlVlpEnd".to_string()),
            },
        )
        .unwrap();

    let source = format!(
        "MATCH p = (a)-[:GQL_VLP_SOURCE*1..2 {{status: 'open'}}]->(b:GqlVlpEnd {{status: 'keep'}}) \
         WHERE id(a) = {start} RETURN p ORDER BY p"
    );
    let gql = execute_gql_ok(&engine, &source);
    let gql_paths = gql
        .rows
        .iter()
        .map(|row| {
            let path = gql_single_path(&row.values[0]);
            (path.node_ids.clone(), path.edge_ids.clone())
        })
        .collect::<Vec<_>>();

    let mut native = graph_query(
        &["a", "b"],
        vec![graph_vlp(Some("p"), None, "a", "b", 1, 2)],
    );
    native.nodes[0].ids = vec![start];
    native.nodes[1].label_filter = Some(NodeLabelFilter {
        labels: vec!["GqlVlpEnd".to_string()],
        mode: LabelMatchMode::All,
    });
    native.nodes[1].filter = Some(NodeFilterExpr::PropertyEquals {
        key: "status".to_string(),
        value: PropValue::String("keep".to_string()),
    });
    if let GraphPatternPiece::VariableLength(path) = &mut native.pieces[0] {
        path.label_filter = vec!["GQL_VLP_SOURCE".to_string()];
        path.filter = Some(EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("open".to_string()),
        });
    }
    native.return_items = Some(vec![graph_return_binding(
        "p",
        GraphReturnProjection::Element(GraphElementProjection::Full),
    )]);
    native.order_by = vec![GraphOrderItem {
        expr: GraphExpr::Binding("p".to_string()),
        direction: GraphOrderDirection::Asc,
    }];
    let native_paths = graph_row_path_ids(engine.query_graph_rows(&native).unwrap());
    assert_eq!(gql_paths, native_paths);
    assert_eq!(native_paths, vec![(vec![start, keep_mid, keep_end], vec![first, second])]);
}

#[test]
fn gql_path_outputs_hydrate_elements_and_respect_vector_policy() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_query_test_catalog(&engine);
    let a = engine
        .upsert_node(
            "PathVector",
            "gql-path-vector-a",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(1, 1.0)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node(
            "PathVector",
            "gql-path-vector-b",
            UpsertNodeOptions {
                dense_vector: Some(vec![0.4, 0.5, 0.6]),
                sparse_vector: Some(vec![(2, 2.0)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    let edge = engine
        .upsert_edge(a, b, "GQL_PATH_VECTOR", UpsertEdgeOptions::default())
        .unwrap();

    let source = format!("MATCH p = (a)-[:GQL_PATH_VECTOR*1..1]->(b) WHERE id(a) = {a} RETURN p");
    let default_path = gql_single_path(&execute_gql_ok(&engine, &source).rows[0].values[0]).clone();
    assert_eq!(default_path.node_ids, vec![a, b]);
    assert_eq!(default_path.edge_ids, vec![edge]);
    let nodes = default_path.nodes.as_ref().expect("direct path should hydrate nodes");
    let edges = default_path.edges.as_ref().expect("direct path should hydrate edges");
    assert_eq!(nodes.len(), 2);
    assert_eq!(edges.len(), 1);
    assert!(nodes.iter().all(|node| node.dense_vector.is_none()));
    assert!(nodes.iter().all(|node| node.sparse_vector.is_none()));

    let vector_path = gql_single_path(
        &execute_gql_with_options(
            &engine,
            &source,
            GqlExecutionOptions {
                include_vectors: true,
                ..GqlExecutionOptions::default()
            },
        )
        .rows[0]
        .values[0],
    )
    .clone();
    let nodes = vector_path.nodes.as_ref().unwrap();
    assert_eq!(nodes[0].dense_vector.as_deref(), Some([0.1, 0.2, 0.3].as_slice()));
    assert_eq!(nodes[1].sparse_vector.as_deref(), Some([(2, 2.0)].as_slice()));
}

#[test]
fn gql_optional_vlp_path_explain_surfaces_graph_row_root() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "gql-explain-path-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "gql-explain-path-b", &[], 1.0);
    engine
        .upsert_edge(a, b, "GQL_EXPLAIN_PATH", UpsertEdgeOptions::default())
        .unwrap();

    let explain = engine
        .explain_gql(
            &format!(
                "MATCH (a:Person) WHERE id(a) = {a} \
                 OPTIONAL MATCH p = (a)-[:GQL_EXPLAIN_PATH*1..2]->(b) \
                 RETURN p ORDER BY length(p) LIMIT 1"
            ),
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let explain = gql_read_explain(&explain);
    assert_eq!(explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(explain.native_plan.is_none());
    for expected in [
        "GraphRowPhysicalPlan",
        "VariableLengthPath",
        "Optional",
        "path element p",
    ] {
        assert!(
            explain
                .projection
                .iter()
                .any(|item| item.contains(expected)),
            "expected explain projection to contain {expected:?}, got {:?}",
            explain.projection
        );
    }
}

#[test]
fn gql_fixed_pattern_explain_asserts_fanout_aware_physical_choice() {
    let (_dir, engine) = query_test_engine();
    let small = insert_query_node(&engine, "GQL_FANOUT_SMALL", "gql-fanout-small", &[], 1.0);
    let bridge_hit = insert_query_node(
        &engine,
        "GQL_FANOUT_BRIDGE",
        "gql-fanout-bridge-hit",
        &[],
        1.0,
    );
    engine
        .upsert_edge(
            small,
            bridge_hit,
            "GQL_FANOUT_HIGH",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    for index in 0..39 {
        let bridge = insert_query_node(
            &engine,
            "GQL_FANOUT_BRIDGE",
            &format!("gql-fanout-bridge-{index}"),
            &[],
            1.0,
        );
        engine
            .upsert_edge(small, bridge, "GQL_FANOUT_HIGH", UpsertEdgeOptions::default())
            .unwrap();
    }
    let mut expected = Vec::new();
    for index in 0..5 {
        let larger = insert_query_node(
            &engine,
            "GQL_FANOUT_LARGER",
            &format!("gql-fanout-larger-{index}"),
            &[],
            1.0,
        );
        expected.push(larger);
        engine
            .upsert_edge(
                larger,
                bridge_hit,
                "GQL_FANOUT_LOW",
                UpsertEdgeOptions::default(),
            )
            .unwrap();
    }
    engine.flush().unwrap();
    expected.sort_unstable();

    let source = "MATCH (small:GQL_FANOUT_SMALL)-[high_edge:GQL_FANOUT_HIGH]->\
                  (bridge:GQL_FANOUT_BRIDGE)<-[low_edge:GQL_FANOUT_LOW]-\
                  (larger:GQL_FANOUT_LARGER) \
                  RETURN id(larger) ORDER BY id(larger)";
    let result = execute_gql_ok(&engine, source);
    assert_eq!(gql_u64_column(&result, 0), expected);

    let explain = engine
        .explain_gql(source, &GqlParams::new(), &gql_opts())
        .unwrap();
    let explain = gql_read_explain(&explain);
    assert_eq!(explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(explain.native_plan.is_none());
    for expected in [
        "graph row plan: GraphRowPhysicalPlan",
        "physical_edge_order=[\"alias:low_edge\", \"alias:high_edge\"]",
        "initial_driver=EdgeAnchor(edge=alias:low_edge",
        "graph row plan: GraphRowPlanAlternative",
        "chosen; kind=EdgeAnchor",
        "source=EdgeCandidateSource",
    ] {
        assert!(
            explain
                .projection
                .iter()
                .any(|item| item.contains(expected)),
            "expected GQL explain projection to contain {expected:?}, got {:?}",
            explain.projection
        );
    }
}

#[test]
fn gql_fixed_match_uses_graph_row_relaxed_distinctness_for_self_loops() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(&engine, "Person", "gql-self-loop", &[], 1.0);
    let edge = engine
        .upsert_edge(node, node, "LOOP", UpsertEdgeOptions::default())
        .unwrap();

    let result = execute_gql_ok(
        &engine,
        "MATCH (a:Person)-[r:LOOP]->(b:Person) RETURN id(a), id(r), id(b)",
    );

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].values,
        vec![GqlValue::UInt(node), GqlValue::UInt(edge), GqlValue::UInt(node)]
    );
}

#[test]
fn gql_rich_graph_indexed_queries_match_native_oracles() {
    let (_dir, engine) = query_test_engine();
    let fixture = seed_rich_gql_graph(&engine);
    engine.flush().unwrap();
    let _indexes = install_rich_gql_indexes(&engine);

    let node_query = "MATCH (n:Person:Employee) \
         WHERE n.status IN $statuses AND n.score >= $min_score \
         RETURN id(n) AS id, n.key AS key, labels(n) AS labels, n.weight AS weight, \
                n.created_at AS created_at, n.updated_at AS updated_at, \
                $payload AS payload, $shape AS shape \
         ORDER BY n.score ASC, n.key ASC";
    let node_params = GqlParams::from([
        (
            "statuses".to_string(),
            GqlParamValue::List(vec![GqlParamValue::String("focus".to_string())]),
        ),
        ("min_score".to_string(), GqlParamValue::Int(70)),
        (
            "payload".to_string(),
            GqlParamValue::Bytes(vec![7, 8, 9]),
        ),
        (
            "shape".to_string(),
            GqlParamValue::Map(BTreeMap::from([
                (
                    "kind".to_string(),
                    GqlParamValue::String("employee-score".to_string()),
                ),
                (
                    "thresholds".to_string(),
                    GqlParamValue::List(vec![
                        GqlParamValue::Int(70),
                        GqlParamValue::String("focus".to_string()),
                    ]),
                ),
            ])),
        ),
    ]);
    let node_result = execute_gql_with_params(&engine, node_query, node_params.clone());
    let native_node_ids = sorted_rich_employee_focus_score_oracle(&engine, 70);
    assert_eq!(
        node_result.columns,
        vec!["id", "key", "labels", "weight", "created_at", "updated_at", "payload", "shape"]
    );
    assert_eq!(gql_u64_column(&node_result, 0), native_node_ids);
    assert_eq!(native_node_ids, vec![fixture.bob, fixture.alice]);

    let expected_payload = GqlValue::Bytes(vec![7, 8, 9]);
    let expected_shape = GqlValue::Map(BTreeMap::from([
        (
            "kind".to_string(),
            GqlValue::String("employee-score".to_string()),
        ),
        (
            "thresholds".to_string(),
            GqlValue::List(vec![
                GqlValue::Int(70),
                GqlValue::String("focus".to_string()),
            ]),
        ),
    ]));
    for (row, node_id) in node_result.rows.iter().zip(native_node_ids.iter().copied()) {
        let node = engine.get_node(node_id).unwrap().unwrap();
        assert_eq!(row.values[1], GqlValue::String(node.key));
        assert_eq!(
            row.values[2],
            GqlValue::List(node.labels.into_iter().map(GqlValue::String).collect())
        );
        assert_eq!(row.values[3], GqlValue::Float(node.weight as f64));
        assert_eq!(row.values[4], GqlValue::Int(node.created_at));
        assert_eq!(row.values[5], GqlValue::Int(node.updated_at));
        assert_eq!(row.values[6], expected_payload);
        assert_eq!(row.values[7], expected_shape);
    }

    let alice_labels = node_result
        .rows
        .iter()
        .find(|row| row.values[0] == GqlValue::UInt(fixture.alice))
        .map(|row| row.values[2].clone())
        .unwrap();
    assert_eq!(
        alice_labels,
        GqlValue::List(
            engine
                .get_node(fixture.alice)
                .unwrap()
                .unwrap()
                .labels
                .into_iter()
                .map(GqlValue::String)
                .collect()
        )
    );

    let node_explain = engine
        .explain_gql(node_query, &node_params, &gql_opts())
        .unwrap();
    let node_explain = gql_read_explain(&node_explain);
    assert_eq!(node_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(node_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.status")));
    assert!(node_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.score")));
    assert!(node_explain.native_plan.is_none());

    let range_explain = engine
        .explain_gql(
            "MATCH (n:Person:Employee) WHERE n.score >= $min_score RETURN id(n)",
            &GqlParams::from([("min_score".to_string(), GqlParamValue::Int(70))]),
            &gql_opts(),
        )
        .unwrap();
    let range_explain = gql_read_explain(&range_explain);
    assert_eq!(range_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(range_explain.native_plan.is_none());
    assert!(range_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.score")));

    let fallback_result = execute_gql_ok(
        &engine,
        "MATCH (n:Person:Employee) WHERE n.department = 'platform' \
         RETURN id(n) ORDER BY id(n)",
    );
    let mut fallback_native = engine
        .query_node_ids(&NodeQuery {
            label_filter: Some(node_label_filter(
                &["Person", "Employee"],
                LabelMatchMode::All,
            )),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "department".to_string(),
                value: PropValue::String("platform".to_string()),
            }),
            ..NodeQuery::default()
        })
        .unwrap()
        .items;
    fallback_native.sort_unstable();
    assert_eq!(gql_u64_column(&fallback_result, 0), fallback_native);
    let fallback_explain = engine
        .explain_gql(
            "MATCH (n:Person:Employee) WHERE n.department = 'platform' RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let fallback_explain = gql_read_explain(&fallback_explain);
    assert!(fallback_explain.native_plan.is_none());

    let edge_query = "MATCH ()-[r:WORKS_ON]->() \
         WHERE r.role IN $roles AND r.hours >= $min_hours \
         RETURN id(r) AS id, r.from AS from, r.to AS to, type(r) AS label, \
                r.hours AS hours, r.weight AS weight, r.created_at AS created_at, \
                r.updated_at AS updated_at, r.valid_from AS valid_from, r.valid_to AS valid_to \
         ORDER BY r.hours ASC, id(r) ASC";
    let edge_params = GqlParams::from([
        (
            "roles".to_string(),
            GqlParamValue::List(vec![
                GqlParamValue::String("lead".to_string()),
                GqlParamValue::String("reviewer".to_string()),
            ]),
        ),
        ("min_hours".to_string(), GqlParamValue::Int(30)),
    ]);
    let edge_result = execute_gql_with_params(&engine, edge_query, edge_params.clone());
    let native_edge_ids = sorted_rich_work_edge_oracle(&engine, 30);
    assert_eq!(gql_u64_column(&edge_result, 0), native_edge_ids);
    assert_eq!(native_edge_ids, vec![fixture.review_edge, fixture.lead_edge]);
    for (row, edge_id) in edge_result.rows.iter().zip(native_edge_ids.iter().copied()) {
        let edge = engine.get_edge(edge_id).unwrap().unwrap();
        assert_eq!(row.values[1], GqlValue::UInt(edge.from));
        assert_eq!(row.values[2], GqlValue::UInt(edge.to));
        assert_eq!(row.values[3], GqlValue::String(edge.label));
        assert_eq!(row.values[4], GqlValue::Int(edge_prop_i64(&engine, edge_id, "hours")));
        assert_eq!(row.values[5], GqlValue::Float(edge.weight as f64));
        assert_eq!(row.values[6], GqlValue::Int(edge.created_at));
        assert_eq!(row.values[7], GqlValue::Int(edge.updated_at));
        assert_eq!(row.values[8], GqlValue::Int(edge.valid_from));
        assert_eq!(row.values[9], GqlValue::Int(edge.valid_to));
    }

    let edge_explain = engine
        .explain_gql(edge_query, &edge_params, &gql_opts())
        .unwrap();
    let edge_explain = gql_read_explain(&edge_explain);
    assert_eq!(edge_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(edge_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.role")));
    assert!(edge_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.hours")));
    assert!(edge_explain.native_plan.is_none());
    let edge_range_explain = engine
        .explain_gql(
            "MATCH ()-[r:WORKS_ON]->() WHERE r.hours >= $min_hours RETURN id(r)",
            &GqlParams::from([("min_hours".to_string(), GqlParamValue::Int(30))]),
            &gql_opts(),
        )
        .unwrap();
    let edge_range_explain = gql_read_explain(&edge_range_explain);
    assert_eq!(edge_range_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(edge_range_explain.native_plan.is_none());
    assert!(edge_range_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.hours")));

    let endpoint_result = execute_gql_with_params(
        &engine,
        "MATCH ()-[r:WORKS_ON]->() \
         WHERE r.from = $from AND r.to IN $targets RETURN id(r) ORDER BY id(r)",
        GqlParams::from([
            ("from".to_string(), GqlParamValue::UInt(fixture.alice)),
            (
                "targets".to_string(),
                GqlParamValue::List(vec![
                    GqlParamValue::UInt(fixture.acme),
                    GqlParamValue::UInt(fixture.globex),
                ]),
            ),
        ]),
    );
    let mut endpoint_native = engine
        .query_edge_ids(&EdgeQuery {
            label: Some("WORKS_ON".to_string()),
            from_ids: vec![fixture.alice],
            to_ids: vec![fixture.acme, fixture.globex],
            ..EdgeQuery::default()
        })
        .unwrap()
        .edge_ids;
    endpoint_native.sort_unstable();
    assert_eq!(gql_u64_column(&endpoint_result, 0), endpoint_native);
    assert_eq!(endpoint_native, vec![fixture.lead_edge, fixture.startup_edge]);

    let pattern_query = "MATCH (p:Person:Employee)-[r:WORKS_ON]->(c:Company) \
         WHERE p.status = 'focus' AND r.role = 'lead' AND c.tier = 'enterprise' \
         RETURN id(p), id(r), id(c) ORDER BY p.key, id(r)";
    let pattern_result = execute_gql_ok(&engine, pattern_query);
    let pattern_native = rich_pattern_oracle(&engine, "lead");
    let pattern_gql = pattern_result
        .rows
        .iter()
        .map(|row| match (&row.values[0], &row.values[1], &row.values[2]) {
            (GqlValue::UInt(p), GqlValue::UInt(r), GqlValue::UInt(c)) => (*p, *r, *c),
            other => panic!("expected id tuple, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(pattern_gql, pattern_native);
    assert_eq!(pattern_native, vec![(fixture.alice, fixture.lead_edge, fixture.acme)]);
    let pattern_explain = engine
        .explain_gql(pattern_query, &GqlParams::new(), &gql_opts())
        .unwrap();
    let pattern_explain = gql_read_explain(&pattern_explain);
    assert_eq!(pattern_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(pattern_explain.residual.is_empty());
    assert!(pattern_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("p.status")));
    assert!(pattern_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.role")));
    assert!(pattern_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("c.tier")));
    assert!(pattern_explain.native_plan.is_none());

    let alt_result = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (p:Person)-[r:WORKS_ON|MENTORS]->(x) \
             WHERE id(p) = {} RETURN id(r) ORDER BY id(r)",
            fixture.alice
        ),
    );
    assert_eq!(
        gql_u64_column(&alt_result, 0),
        vec![fixture.lead_edge, fixture.startup_edge, fixture.mentor_edge]
    );
}

#[test]
fn gql_residual_where_filters_with_null_semantics_after_pushdown() {
    let (_dir, engine) = query_test_engine();
    let keep = insert_query_node(
        &engine,
        "Person",
        "residual-keep",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "residual-drop",
        &[
            ("status", PropValue::String("active".to_string())),
            ("blocked", PropValue::Bool(true)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "residual-inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );

    let result = execute_gql_ok(
        &engine,
        "MATCH (n:Person) \
         WHERE n.status = 'active' AND n.blocked IS NULL AND n.missing <> 'x' \
         RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&result, 0), Vec::<u64>::new());

    let result = execute_gql_ok(
        &engine,
        "MATCH (n:Person) \
         WHERE n.status = 'active' AND n.blocked IS NULL \
         RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&result, 0), vec![keep]);
}

#[test]
fn gql_return_scalars_missing_null_params_and_duplicate_columns() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node_with_labels(
        &engine,
        &["Person", "Topic"],
        "scalar-node",
        &[
            ("name", PropValue::String("Ada".to_string())),
            ("optional", PropValue::Null),
        ],
        1.0,
    );
    let params = GqlParams::from([
        ("wanted".to_string(), GqlParamValue::String("Ada".to_string())),
        ("answer".to_string(), GqlParamValue::Int(42)),
    ]);
    let result = execute_gql_with_params(
        &engine,
        "MATCH (n:Person) WHERE n.name = $wanted \
         RETURN id(n) AS id, labels(n) AS labels, n.name AS x, n.missing AS missing, \
                n.optional AS opt, n.key AS x, $answer",
        params,
    );

    assert_eq!(result.columns, vec!["id", "labels", "x", "missing", "opt", "x", "$answer"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(node));
    assert_eq!(
        result.rows[0].values[1],
        GqlValue::List(vec![
            GqlValue::String("Person".to_string()),
            GqlValue::String("Topic".to_string()),
        ])
    );
    assert_eq!(result.rows[0].values[2], GqlValue::String("Ada".to_string()));
    assert_eq!(result.rows[0].values[3], GqlValue::Null);
    assert_eq!(result.rows[0].values[4], GqlValue::Null);
    assert_eq!(result.rows[0].values[5], GqlValue::String("scalar-node".to_string()));
    assert_eq!(result.rows[0].values[6], GqlValue::Int(42));

    let numeric_result = execute_gql_with_params(
        &engine,
        &format!(
            "MATCH (n:Person) WHERE n.name = $wanted \
             RETURN id(n) = {node}.0 AS eq, id(n) IN [{node}.0] AS in_id"
        ),
        GqlParams::from([(
            "wanted".to_string(),
            GqlParamValue::String("Ada".to_string()),
        )]),
    );
    assert_eq!(
        numeric_result.rows[0].values,
        vec![GqlValue::Bool(true), GqlValue::Bool(true)]
    );

    let ambiguous_order = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name AS x, n.key AS x ORDER BY x",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        ambiguous_order,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let ambiguous_limit = engine
        .execute_gql(
            "MATCH (n:Person) RETURN 1 AS x, 2 AS x LIMIT x",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        ambiguous_limit,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let bound_variable_takes_priority = execute_gql_ok(
        &engine,
        "MATCH (x:Person) RETURN 0 AS x ORDER BY x.name",
    );
    assert_eq!(bound_variable_takes_priority.rows.len(), 1);
}

#[test]
fn gql_numeric_property_predicates_match_native_semantics_without_indexes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gql-numeric-semantics");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut expected_nodes = Vec::new();
    for (key, value) in [
        ("score-int", PropValue::Int(1)),
        ("score-uint", PropValue::UInt(1)),
        ("score-float", PropValue::Float(1.0)),
    ] {
        expected_nodes.push(
            engine
                .upsert_node(
                    "Person",
                    key,
                    UpsertNodeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    engine
        .upsert_node(
            "Person",
            "score-string",
            UpsertNodeOptions {
                props: query_test_props(&[("score", PropValue::String("1".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let eq = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.score = 1.0 RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&eq, 0), expected_nodes);

    let in_result = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.score IN [1, 1.0] RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&in_result, 0), expected_nodes);

    let range_result = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.score >= -0.0 AND n.score <= 1.0 RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&range_result, 0), expected_nodes);

    let a = expected_nodes[0];
    let b = expected_nodes[1];
    let mut expected_edges = Vec::new();
    for value in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        expected_edges.push(
            engine
                .upsert_edge(
                    a,
                    b,
                    "LIKES",
                    UpsertEdgeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    let edge_eq = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() WHERE r.score = 1.0 RETURN id(r)",
    );
    assert_eq!(gql_u64_column(&edge_eq, 0), expected_edges);

    engine.close().unwrap();
}

#[test]
fn gql_numeric_equality_uses_semantic_equality_indexes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gql-indexed-numeric-equality");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let node_index = engine
        .ensure_node_property_index("Person", "score", SecondaryIndexKind::Equality)
        .unwrap()
        .index_id;
    let edge_index = engine
        .ensure_edge_property_index("LIKES", "score", SecondaryIndexKind::Equality)
        .unwrap()
        .index_id;
    wait_for_property_index_state(&engine, node_index, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&engine, edge_index, SecondaryIndexState::Ready);

    let mut expected_nodes = Vec::new();
    for (key, value) in [
        ("score-index-int", PropValue::Int(1)),
        ("score-index-uint", PropValue::UInt(1)),
        ("score-index-float", PropValue::Float(1.0)),
    ] {
        expected_nodes.push(
            engine
                .upsert_node(
                    "Person",
                    key,
                    UpsertNodeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    engine
        .upsert_node(
            "Person",
            "score-index-string",
            UpsertNodeOptions {
                props: query_test_props(&[("score", PropValue::String("1".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let mut expected_edges = Vec::new();
    for value in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        expected_edges.push(
            engine
                .upsert_edge(
                    expected_nodes[0],
                    expected_nodes[1],
                    "LIKES",
                    UpsertEdgeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    engine
        .upsert_edge(
            expected_nodes[0],
            expected_nodes[2],
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::String("1".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    expected_nodes.sort_unstable();
    expected_edges.sort_unstable();

    let where_eq = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.score = 1.0 RETURN id(n) ORDER BY id(n)",
    );
    assert_eq!(gql_u64_column(&where_eq, 0), expected_nodes);
    let map_eq = execute_gql_ok(
        &engine,
        "MATCH (n:Person {score: 1.0}) RETURN id(n) ORDER BY id(n)",
    );
    assert_eq!(gql_u64_column(&map_eq, 0), expected_nodes);
    let in_eq = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.score IN [1, 1.0] RETURN id(n) ORDER BY id(n)",
    );
    assert_eq!(gql_u64_column(&in_eq, 0), expected_nodes);

    let node_explain = engine
        .explain_gql(
            "MATCH (n:Person) WHERE n.score = 1.0 RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let node_explain = gql_read_explain(&node_explain);
    assert_eq!(node_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(node_explain.native_plan.is_none());
    assert!(node_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.score")));

    let edge_eq = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() WHERE r.score = 1.0 RETURN id(r) ORDER BY id(r)",
    );
    assert_eq!(gql_u64_column(&edge_eq, 0), expected_edges);
    let edge_in = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() WHERE r.score IN [1, 1.0] RETURN id(r) ORDER BY id(r)",
    );
    assert_eq!(gql_u64_column(&edge_in, 0), expected_edges);
    let edge_explain = engine
        .explain_gql(
            "MATCH ()-[r:LIKES]->() WHERE r.score = 1.0 RETURN id(r)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let edge_explain = gql_read_explain(&edge_explain);
    assert_eq!(edge_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(edge_explain.native_plan.is_none());
    assert!(edge_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.score")));

    engine.close().unwrap();
}

#[test]
fn gql_numeric_range_uses_domainless_indexes_for_mixed_numeric_values() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("gql-indexed-numeric-range");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let node_index = engine
        .ensure_node_property_index("Person", "score", SecondaryIndexKind::Range)
        .unwrap()
        .index_id;
    let edge_index = engine
        .ensure_edge_property_index("LIKES", "score", SecondaryIndexKind::Range)
        .unwrap()
        .index_id;
    wait_for_property_index_state(&engine, node_index, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, node_index, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&engine, edge_index, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, edge_index, SecondaryIndexState::Ready);

    fn assert_domainless_indexed_range_gql(
        engine: &DatabaseEngine,
        expected_nodes: &[u64],
        expected_edges: &[u64],
    ) {
        let node_range = execute_gql_ok(
            engine,
            "MATCH (n:Person) WHERE n.score >= 1 AND n.score <= 1.0 \
             RETURN id(n) ORDER BY id(n)",
        );
        assert_eq!(gql_u64_column(&node_range, 0), expected_nodes);
        let node_range_explain = engine
            .explain_gql(
                "MATCH (n:Person) WHERE n.score >= 1 AND n.score <= 1.0 RETURN id(n)",
                &GqlParams::new(),
                &gql_opts(),
            )
            .unwrap();
        let node_range_explain = gql_read_explain(&node_range_explain);
        assert_eq!(node_range_explain.target, GqlLoweringTarget::GraphRowQuery);
        assert!(node_range_explain.native_plan.is_none());
        assert!(node_range_explain
            .pushed_down
            .iter()
            .any(|item| item.contains("n.score")));

        let edge_range = execute_gql_ok(
            engine,
            "MATCH ()-[r:LIKES]->() WHERE r.score >= 1 AND r.score <= 1.0 \
             RETURN id(r) ORDER BY id(r)",
        );
        assert_eq!(gql_u64_column(&edge_range, 0), expected_edges);
        let edge_range_explain = engine
            .explain_gql(
                "MATCH ()-[r:LIKES]->() WHERE r.score >= 1 AND r.score <= 1.0 RETURN id(r)",
                &GqlParams::new(),
                &gql_opts(),
            )
            .unwrap();
        let edge_range_explain = gql_read_explain(&edge_range_explain);
        assert_eq!(edge_range_explain.target, GqlLoweringTarget::GraphRowQuery);
        assert!(edge_range_explain.native_plan.is_none());
        assert!(edge_range_explain
            .pushed_down
            .iter()
            .any(|item| item.contains("r.score")));
    }

    let mut expected_nodes = Vec::new();
    for (key, value) in [
        ("score-range-int", PropValue::Int(1)),
        ("score-range-uint", PropValue::UInt(1)),
        ("score-range-float", PropValue::Float(1.0)),
    ] {
        expected_nodes.push(
            engine
                .upsert_node(
                    "Person",
                    key,
                    UpsertNodeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    for (key, value) in [
        ("score-range-higher", PropValue::Float(2.5)),
        ("score-range-string", PropValue::String("1".to_string())),
        ("score-range-nan", PropValue::Float(f64::NAN)),
    ] {
        engine
            .upsert_node(
                "Person",
                key,
                UpsertNodeOptions {
                    props: query_test_props(&[("score", value)]),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    let mut expected_edges = Vec::new();
    for value in [PropValue::Int(1), PropValue::UInt(1), PropValue::Float(1.0)] {
        expected_edges.push(
            engine
                .upsert_edge(
                    expected_nodes[0],
                    expected_nodes[1],
                    "LIKES",
                    UpsertEdgeOptions {
                        props: query_test_props(&[("score", value)]),
                        ..Default::default()
                    },
                )
                .unwrap(),
        );
    }
    for value in [
        PropValue::Float(2.5),
        PropValue::String("1".to_string()),
        PropValue::Float(f64::NAN),
    ] {
        engine
            .upsert_edge(
                expected_nodes[0],
                expected_nodes[2],
                "LIKES",
                UpsertEdgeOptions {
                    props: query_test_props(&[("score", value)]),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    expected_nodes.sort_unstable();
    expected_edges.sort_unstable();
    assert_domainless_indexed_range_gql(&engine, &expected_nodes, &expected_edges);

    engine.flush().unwrap();
    assert_domainless_indexed_range_gql(&engine, &expected_nodes, &expected_edges);

    engine.close().unwrap();
}

#[test]
fn gql_empty_results_and_parameter_values_use_public_handler_path() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(
        &engine,
        "Person",
        "boundary-node",
        &[("name", PropValue::String("Ada".to_string()))],
        1.0,
    );
    let from = insert_query_node(&engine, "Person", "boundary-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "boundary-to", &[], 1.0);
    let edge = engine
        .upsert_edge(from, to, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let unknown_nodes = execute_gql_ok(&engine, "MATCH (n:DefinitelyMissing) RETURN id(n)");
    assert!(unknown_nodes.rows.is_empty());
    assert_eq!(engine.get_node_label_id("DefinitelyMissing").unwrap(), None);

    let unknown_edges = execute_gql_ok(&engine, "MATCH ()-[r:DEFINITELY_MISSING]->() RETURN id(r)");
    assert!(unknown_edges.rows.is_empty());
    assert_eq!(engine.get_edge_label_id("DEFINITELY_MISSING").unwrap(), None);

    let missing_property = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.no_such_property = 'x' RETURN id(n)",
    );
    assert!(missing_property.rows.is_empty());

    let impossible_node_id = execute_gql_ok(
        &engine,
        &format!("MATCH (n) WHERE id(n) = {}.5 RETURN id(n)", node),
    );
    assert!(impossible_node_id.rows.is_empty());
    assert_eq!(impossible_node_id.stats.rows_matched, 0);

    let impossible_edge_id = execute_gql_ok(
        &engine,
        &format!("MATCH ()-[r]->() WHERE id(r) = {}.5 RETURN id(r)", edge),
    );
    assert!(impossible_edge_id.rows.is_empty());
    assert_eq!(impossible_edge_id.stats.rows_matched, 0);

    let result = execute_gql_with_params(
        &engine,
        "MATCH (n:Person) WHERE n.key = $key \
         RETURN $payload AS payload, $shape AS shape, $names AS names, n.name",
        GqlParams::from([
            (
                "key".to_string(),
                GqlParamValue::String("boundary-node".to_string()),
            ),
            (
                "payload".to_string(),
                GqlParamValue::Bytes(vec![1, 2, 3, 4]),
            ),
            (
                "shape".to_string(),
                GqlParamValue::Map(BTreeMap::from([
                    ("enabled".to_string(), GqlParamValue::Bool(true)),
                    ("score".to_string(), GqlParamValue::Float(1.5)),
                ])),
            ),
            (
                "names".to_string(),
                GqlParamValue::List(vec![
                    GqlParamValue::String("Ada".to_string()),
                    GqlParamValue::Null,
                ]),
            ),
        ]),
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::Bytes(vec![1, 2, 3, 4]));
    assert_eq!(
        result.rows[0].values[1],
        GqlValue::Map(BTreeMap::from([
            ("enabled".to_string(), GqlValue::Bool(true)),
            ("score".to_string(), GqlValue::Float(1.5)),
        ]))
    );
    assert_eq!(
        result.rows[0].values[2],
        GqlValue::List(vec![GqlValue::String("Ada".to_string()), GqlValue::Null])
    );
    assert_eq!(result.rows[0].values[3], GqlValue::String("Ada".to_string()));
}

#[test]
fn gql_return_relationship_type_properties_and_elements() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "element-from", &[], 1.0);
    let to = insert_query_node(&engine, "Article", "element-to", &[], 1.0);
    let edge = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2025))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    let result = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() RETURN type(r) AS t, r.since AS since, r",
    );
    assert_eq!(result.columns, vec!["t", "since", "r"]);
    assert_eq!(result.rows[0].values[0], GqlValue::String("LIKES".to_string()));
    assert_eq!(result.rows[0].values[1], GqlValue::Int(2025));
    let projected = gql_single_edge(&result.rows[0].values[2]);
    assert_eq!(projected.id, Some(edge));
    assert_eq!(projected.from, Some(from));
    assert_eq!(projected.to, Some(to));
    assert_eq!(projected.label.as_deref(), Some("LIKES"));
    assert_eq!(
        projected.props.as_ref().unwrap().get("since"),
        Some(&GqlValue::Int(2025))
    );
}

#[test]
fn gql_return_node_element_star_order_and_anonymous_alias_omission() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "Person",
        "star-a",
        &[("name", PropValue::String("A".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "Person",
        "star-b",
        &[("name", PropValue::String("B".to_string()))],
        1.0,
    );
    let edge = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let node_result = execute_gql_ok(&engine, "MATCH (n:Person) WHERE id(n) = 1 RETURN n");
    let node = gql_single_node(&node_result.rows[0].values[0]);
    assert!(node.dense_vector.is_none());
    assert!(node.sparse_vector.is_none());
    assert!(node.props.as_ref().unwrap().contains_key("name"));

    let star = execute_gql_ok(&engine, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN *");
    assert_eq!(star.columns, vec!["a", "r", "b"]);
    assert_eq!(gql_single_node(&star.rows[0].values[0]).id, Some(a));
    assert_eq!(gql_single_edge(&star.rows[0].values[1]).id, Some(edge));
    assert_eq!(gql_single_node(&star.rows[0].values[2]).id, Some(b));

    let anonymous = execute_gql_ok(&engine, "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN *");
    assert_eq!(anonymous.columns, vec!["r"]);
    assert_eq!(gql_single_edge(&anonymous.rows[0].values[0]).id, Some(edge));
}

#[test]
fn gql_parameter_and_deferred_feature_errors_are_clear() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "Person",
        "param-node",
        &[("name", PropValue::String("Ada".to_string()))],
        1.0,
    );

    let missing = engine
        .execute_gql(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        missing,
        EngineError::GqlParameter { ref name, .. } if name == "name"
    ));
}

#[test]
fn gql_referenced_param_list_cap_rejects_before_native_execution() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "param-cap-node", &[], 1.0);
    engine.reset_query_execution_counters_for_test();

    let params = GqlParams::from([(
        "ids".to_string(),
        GqlParamValue::List(vec![
            GqlParamValue::UInt(1),
            GqlParamValue::UInt(2),
            GqlParamValue::UInt(3),
        ]),
    )]);
    let err = engine
        .execute_gql(
            "MATCH (n:Person) WHERE id(n) IN $ids RETURN n.name LIMIT 1",
            &params,
            &gql_param_cap_options(2, 8, 1_024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "ids", "exceeding max_literal_items");
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );
}

#[test]
fn gql_referenced_param_nested_depth_cap_rejects_iteratively() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "param-depth-node", &[], 1.0);
    engine.reset_query_execution_counters_for_test();

    let params = GqlParams::from([(
        "payload".to_string(),
        GqlParamValue::List(vec![GqlParamValue::List(vec![GqlParamValue::List(vec![
            GqlParamValue::Int(1),
        ])])]),
    )]);
    let err = engine
        .execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 1",
            &params,
            &gql_param_cap_options(8, 2, 1_024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "payload", "nested list/map depth");
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );
}

#[test]
fn gql_referenced_param_total_items_rejects_even_with_limit_zero() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "param-total-node", &[], 1.0);

    let params = GqlParams::from([(
        "payload".to_string(),
        GqlParamValue::List(vec![
            GqlParamValue::List(vec![GqlParamValue::Int(1), GqlParamValue::Int(2)]),
            GqlParamValue::Int(3),
        ]),
    )]);
    let err = engine
        .execute_gql(
            "MATCH (n:Person) RETURN $payload LIMIT 0",
            &params,
            &gql_param_cap_options(3, 8, 1_024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "payload", "total list/map items");
}

#[test]
fn gql_referenced_param_string_bytes_and_map_key_bytes_are_capped() {
    let (_dir, engine) = query_test_engine();
    let string_source = "MATCH (n:Person) RETURN $p LIMIT 0";
    let string_err = engine
        .execute_gql(
            string_source,
            &GqlParams::from([(
                "p".to_string(),
                GqlParamValue::String("x".repeat(5)),
            )]),
            &gql_param_cap_options(8, 8, 4),
        )
        .unwrap_err();
    assert_gql_param_error(string_err, "p", "string is");

    let bytes_source = "MATCH (n:Person) RETURN $b LIMIT 0";
    let bytes_err = engine
        .execute_gql(
            bytes_source,
            &GqlParams::from([(
                "b".to_string(),
                GqlParamValue::Bytes(vec![7; 5]),
            )]),
            &gql_param_cap_options(8, 8, 4),
        )
        .unwrap_err();
    assert_gql_param_error(bytes_err, "b", "bytes is");

    let key_source = "MATCH (n:Person) RETURN $payload LIMIT 0";
    let key_err = engine
        .execute_gql(
            key_source,
            &GqlParams::from([(
                "payload".to_string(),
                GqlParamValue::Map(BTreeMap::from([("k".repeat(5), GqlParamValue::Null)])),
            )]),
            &gql_param_cap_options(8, 8, 4),
        )
        .unwrap_err();
    assert_gql_param_error(key_err, "payload", "map key is");
}

#[test]
fn gql_boundary_sized_referenced_params_work_and_unused_oversized_params_are_ignored() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(&engine, "Person", "param-boundary-node", &[], 1.0);

    let source = "MATCH (n:Person) RETURN $payload LIMIT 1";
    let params = GqlParams::from([(
        "payload".to_string(),
        GqlParamValue::Map(BTreeMap::from([(
            "key".to_string(),
            GqlParamValue::List(vec![
                GqlParamValue::String("x".repeat(61)),
                GqlParamValue::Null,
            ]),
        )])),
    )]);
    let result = engine
        .execute_gql(source, &params, &gql_param_cap_options(3, 2, 64))
        .unwrap();
    assert_eq!(
        result.rows[0].values[0],
        GqlValue::Map(BTreeMap::from([(
            "key".to_string(),
            GqlValue::List(vec![GqlValue::String("x".repeat(61)), GqlValue::Null])
        )]))
    );

    let unused = engine
        .execute_gql(
            "MATCH (n:Person) RETURN id(n) LIMIT 1",
            &GqlParams::from([(
                "unused".to_string(),
                GqlParamValue::List(vec![
                    GqlParamValue::Int(1),
                    GqlParamValue::Int(2),
                    GqlParamValue::Int(3),
                ]),
            )]),
            &gql_param_cap_options(1, 8, 128),
        )
        .unwrap();
    assert_eq!(unused.rows[0].values[0], GqlValue::UInt(node));
}

#[test]
fn gql_explain_enforces_referenced_param_caps_like_query() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "param-explain-node", &[], 1.0);

    let params = GqlParams::from([(
        "ids".to_string(),
        GqlParamValue::List(vec![
            GqlParamValue::UInt(1),
            GqlParamValue::UInt(2),
            GqlParamValue::UInt(3),
        ]),
    )]);
    let err = engine
        .explain_gql(
            "MATCH (n:Person) WHERE id(n) IN $ids RETURN id(n)",
            &params,
            &gql_param_cap_options(2, 8, 1_024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "ids", "exceeding max_literal_items");
}

#[test]
fn gql_beta_unsupported_features_are_rejected_by_execution_api() {
    let (_dir, engine) = query_test_engine();
    let cases = [
        ("MERGE (n:Person {key: 'ada'}) RETURN n", "write clauses", "MERGE"),
        (
            "CREATE INDEX node_status FOR (n:User) ON (n.status)",
            "schema/DDL",
            "CREATE",
        ),
        ("DROP INDEX node_status", "schema/DDL", "DROP"),
        (
            "MATCH (n:Person)-[*]->(m) RETURN n",
            "unbounded VLP",
            "*",
        ),
        ("MATCH (n:Person) RETURN DISTINCT n", "DISTINCT", "DISTINCT"),
        ("MATCH (n:Person) RETURN count(n)", "aggregation", "count"),
        ("MATCH (n:Person) WITH n RETURN n", "WITH", "WITH"),
        (
            "MATCH (n:Person) RETURN n UNION MATCH (m:Person) RETURN m",
            "UNION",
            "UNION",
        ),
        ("CALL db.labels()", "CALL", "CALL"),
    ];

    for (source, expected_feature, expected_span) in cases {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        match err {
            EngineError::GqlUnsupported { feature, span, .. } => {
                assert_eq!(feature, expected_feature, "query: {source}");
                assert_eq!(
                    span.offset,
                    source.find(expected_span).unwrap(),
                    "query: {source}"
                );
            }
            other => panic!("expected unsupported {expected_feature} for {source}, got {other:?}"),
        }
    }
}

#[test]
fn gql_deferred_features_remain_rejected_after_row_ops() {
    let (_dir, engine) = query_test_engine();
    for source in [
        "MATCH (n:Person)-[*]->(m) RETURN n",
        "MATCH (n:Person) RETURN DISTINCT n",
        "MATCH (n:Person) RETURN count(n)",
        "MATCH (n:Person) WITH n RETURN n",
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        assert!(
            matches!(err, EngineError::GqlUnsupported { .. } | EngineError::GqlParse { .. }),
            "expected unsupported/parse error for {source}, got {err:?}"
        );
    }

    let skip_offset = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n SKIP 1 OFFSET 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(skip_offset, EngineError::GqlParse { .. }));
}

#[test]
fn gql_order_by_skip_offset_limit_and_scalar_order_domains() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "Person",
        "order-a",
        &[
            ("name", PropValue::String("b".to_string())),
            ("rank", PropValue::Int(2)),
            ("group", PropValue::Int(1)),
            ("flag", PropValue::Bool(false)),
        ],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "Person",
        "order-b",
        &[
            ("name", PropValue::String("a".to_string())),
            ("rank", PropValue::UInt(1)),
            ("group", PropValue::Int(1)),
            ("flag", PropValue::Bool(true)),
        ],
        1.0,
    );
    let c = insert_query_node(
        &engine,
        "Person",
        "order-c",
        &[
            ("name", PropValue::String("c".to_string())),
            ("rank", PropValue::Float(2.0)),
            ("group", PropValue::Int(2)),
            ("flag", PropValue::Bool(false)),
        ],
        1.0,
    );
    let d = insert_query_node(
        &engine,
        "Person",
        "order-d",
        &[("name", PropValue::String("d".to_string()))],
        1.0,
    );
    let e = insert_query_node(
        &engine,
        "Person",
        "order-e",
        &[
            ("name", PropValue::String("e".to_string())),
            ("rank", PropValue::Null),
        ],
        1.0,
    );

    let asc = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.rank ASC");
    assert_eq!(
        gql_string_column(&asc, 0),
        vec!["a", "b", "c", "d", "e"]
    );

    let desc = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.rank DESC");
    assert_eq!(
        gql_string_column(&desc, 0),
        vec!["b", "c", "a", "d", "e"]
    );

    let multi = execute_gql_ok(
        &engine,
        "MATCH (n:Person) RETURN n.name ORDER BY n.group ASC, n.rank DESC",
    );
    assert_eq!(
        gql_string_column(&multi, 0),
        vec!["b", "a", "c", "d", "e"]
    );

    let alias = execute_gql_ok(
        &engine,
        "MATCH (n:Person) RETURN n.rank AS r, n.name ORDER BY r DESC LIMIT 1",
    );
    assert_eq!(alias.rows[0].values[1], GqlValue::String("b".to_string()));

    let id_desc = execute_gql_ok(&engine, "MATCH (n:Person) RETURN id(n) ORDER BY id(n) DESC LIMIT 2");
    assert_eq!(gql_u64_column(&id_desc, 0), vec![e, d]);

    let node_alias_desc =
        execute_gql_ok(&engine, "MATCH (n:Person) RETURN id(n) ORDER BY n DESC LIMIT 2");
    assert_eq!(gql_u64_column(&node_alias_desc, 0), vec![e, d]);

    let edge_one = engine
        .upsert_edge(a, b, "ORDER_ALIAS_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    let edge_two = engine
        .upsert_edge(b, c, "ORDER_ALIAS_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    let edge_three = engine
        .upsert_edge(c, d, "ORDER_ALIAS_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    let edge_alias_desc = execute_gql_ok(
        &engine,
        "MATCH ()-[r:ORDER_ALIAS_EDGE]->() RETURN id(r) ORDER BY r DESC LIMIT 2",
    );
    assert_eq!(
        gql_u64_column(&edge_alias_desc, 0),
        vec![edge_three, edge_two]
    );
    assert!(edge_one < edge_two);

    let bool_order = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.flag ASC");
    assert_eq!(
        gql_string_column(&bool_order, 0),
        vec!["b", "c", "a", "d", "e"]
    );

    let skip_limit = execute_gql_ok(
        &engine,
        "MATCH (n:Person) RETURN n.name ORDER BY n.rank ASC SKIP 1 LIMIT 2",
    );
    assert_eq!(gql_string_column(&skip_limit, 0), vec!["b", "c"]);
    assert!(skip_limit.next_cursor.is_none());

    let offset = execute_gql_ok(
        &engine,
        "MATCH (n:Person) RETURN n.name ORDER BY n.rank ASC OFFSET 2 LIMIT 1",
    );
    assert_eq!(gql_string_column(&offset, 0), vec!["c"]);

    engine.reset_query_execution_counters_for_test();
    let limit_zero = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.rank LIMIT 0");
    assert!(limit_zero.rows.is_empty());
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );

    let default_scan_limit_zero = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name LIMIT 0",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: false,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(default_scan_limit_zero.columns, vec!["n.name"]);
    assert!(default_scan_limit_zero.rows.is_empty());
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );

    let default_scan_limit_zero_plan = engine
        .execute_gql(
            "MATCH (n) RETURN n.name LIMIT 0",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: false,
                include_plan: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert!(default_scan_limit_zero_plan.rows.is_empty());
    let plan = default_scan_limit_zero_plan.plan.unwrap();
    let plan = gql_read_explain(&plan);
    assert_eq!(plan.target, GqlLoweringTarget::GraphRowQuery);
    assert!(plan.native_plan.is_none());
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );

    let constant_order_limit_zero =
        execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY 1 LIMIT 0");
    assert!(constant_order_limit_zero.rows.is_empty());

    let bytes_order_limit_zero = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name ORDER BY $bytes LIMIT 0",
            &GqlParams::from([(
                "bytes".to_string(),
                GqlParamValue::Bytes(vec![1, 2, 3]),
            )]),
            &gql_opts(),
        )
        .unwrap();
    assert!(bytes_order_limit_zero.rows.is_empty());

    let list_order_limit_zero = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name ORDER BY $bad LIMIT 0",
            &GqlParams::from([(
                "bad".to_string(),
                GqlParamValue::List(vec![GqlParamValue::Int(1)]),
            )]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        list_order_limit_zero,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let top_k = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 2");
    assert_eq!(gql_string_column(&top_k, 0), vec!["a", "b"]);
    assert!(top_k.next_cursor.is_none());

    let finite_source = "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 5";
    let finite_first = engine
        .execute_gql(
            finite_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                include_plan: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&finite_first, 0), vec!["a", "b"]);
    let finite_first_cursor = finite_first
        .next_cursor
        .clone()
        .expect("finite LIMIT should page when transport page is smaller");
    assert!(finite_first
        .plan
        .as_ref()
        .map(gql_read_explain)
        .unwrap()
        .projection
        .iter()
        .any(|item| item.contains("logical_limit=Some(5)")
            && item.contains("effective_page_limit=2")));

    let finite_second = engine
        .execute_gql(
            finite_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(finite_first_cursor.clone()),
                max_rows: 1,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&finite_second, 0), vec!["c"]);
    let finite_second_cursor = finite_second
        .next_cursor
        .clone()
        .expect("finite LIMIT should preserve remaining rows across cursor pages");

    let finite_third = engine
        .execute_gql(
            finite_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(finite_second_cursor),
                max_rows: 10,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&finite_third, 0), vec!["d", "e"]);
    assert!(finite_third.next_cursor.is_none());

    let skip_finite_source = "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 1 LIMIT 4";
    let skip_finite_first = engine
        .execute_gql(
            skip_finite_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&skip_finite_first, 0), vec!["b", "c"]);
    let skip_finite_cursor = skip_finite_first
        .next_cursor
        .clone()
        .expect("SKIP plus finite LIMIT should page within the logical limit");
    let skip_finite_second = engine
        .execute_gql(
            skip_finite_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(skip_finite_cursor),
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&skip_finite_second, 0), vec!["d", "e"]);
    assert!(skip_finite_second.next_cursor.is_none());

    let changed_limit = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 4",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(finite_first_cursor),
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(changed_limit, EngineError::InvalidCursor { .. }));

    let default_order_full = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name");
    let mut default_order_options = GqlExecutionOptions {
        max_rows: 2,
        ..GqlExecutionOptions::default()
    };
    let mut default_order_cursor = None;
    let mut default_order_paged = Vec::new();
    loop {
        default_order_options.cursor = default_order_cursor.take();
        let page = execute_gql_with_options(
            &engine,
            "MATCH (n:Person) RETURN n.name",
            default_order_options.clone(),
        );
        default_order_cursor = page.next_cursor.clone();
        default_order_paged.extend(gql_string_column(&page, 0));
        if default_order_cursor.is_none() {
            break;
        }
    }
    assert_eq!(default_order_paged, gql_string_column(&default_order_full, 0));

    let bounded_huge_limit = execute_gql_with_params(
        &engine,
        "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $limit",
        GqlParams::from([("limit".to_string(), GqlParamValue::UInt(usize::MAX as u64))]),
    );
    assert_eq!(
        gql_string_column(&bounded_huge_limit, 0),
        vec!["a", "b", "c", "d", "e"]
    );

    let safety_capped_huge_limit = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT $limit",
            &GqlParams::from([("limit".to_string(), GqlParamValue::UInt(usize::MAX as u64))]),
            &GqlExecutionOptions {
                max_rows: 2,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&safety_capped_huge_limit, 0),
        vec!["a", "b"]
    );
    assert!(safety_capped_huge_limit
        .stats
        .warnings
        .iter()
        .any(|warning| warning.contains("max_rows=2")));

    assert!(a < b && b < c);
}

#[test]
fn gql_order_by_edge_label_and_unsupported_order_keys_are_clear() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "order-edge-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "order-edge-to", &[], 1.0);
    engine
        .upsert_edge(from, to, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(from, to, "LIKES", UpsertEdgeOptions::default())
        .unwrap();

    let edge_order = execute_gql_with_options(
        &engine,
        "MATCH ()-[r]->() RETURN type(r) ORDER BY type(r) DESC",
        GqlExecutionOptions {
            allow_full_scan: true,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(
        gql_string_column(&edge_order, 0),
        vec!["LIKES".to_string(), "KNOWS".to_string()]
    );

    let labels_err = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n ORDER BY labels(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        labels_err,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));
    let labels_property_err = engine
        .explain_gql(
            "MATCH (n:Person) RETURN n ORDER BY n.labels",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        labels_property_err,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));
    let labels_alias_err = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.labels AS ls ORDER BY ls",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        labels_alias_err,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let path_from = insert_query_node(&engine, "PathOrder", "path-order-from", &[], 1.0);
    let path_to = insert_query_node(&engine, "PathOrder", "path-order-to", &[], 1.0);
    engine
        .upsert_edge(
            path_from,
            path_to,
            "PATH_ORDER",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    for source in [
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY node_ids(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY edge_ids(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY nodes(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY relationships(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p.edge_ids AS ids ORDER BY ids",
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        match err {
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                span,
                ..
            } => assert!(span.length > 0),
            other => panic!("expected spanful invalid ORDER BY error, got {other:?}"),
        }
    }

    let mixed_int = insert_query_node(
        &engine,
        "MixedOrder",
        "mixed-int",
        &[("mixed", PropValue::Int(1))],
        1.0,
    );
    let mixed_string = insert_query_node(
        &engine,
        "MixedOrder",
        "mixed-string",
        &[("mixed", PropValue::String("x".to_string()))],
        1.0,
    );
    let mixed_bytes = insert_query_node(
        &engine,
        "MixedOrder",
        "mixed-bytes",
        &[("mixed", PropValue::Bytes(vec![1]))],
        1.0,
    );
    let mixed = execute_gql_ok(&engine, "MATCH (n:MixedOrder) RETURN id(n) ORDER BY n.mixed");
    assert_eq!(
        gql_u64_column(&mixed, 0),
        vec![mixed_int, mixed_string, mixed_bytes]
    );

    let non_finite = engine
        .execute_gql(
            "MATCH (n:Person) RETURN n.key ORDER BY $bad",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        non_finite,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let empty_non_finite = engine
        .execute_gql(
            "MATCH (n:Person) WHERE n.key = 'missing-order-row' RETURN n.key ORDER BY $bad",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        empty_non_finite,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let explain_bytes_order_param = engine
        .explain_gql(
            "MATCH (n:Person) RETURN n.key ORDER BY $bad",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Bytes(vec![1, 2, 3]))]),
            &gql_opts(),
        )
        .unwrap();
    let explain_bytes_order_param = gql_read_explain(&explain_bytes_order_param);
    assert!(explain_bytes_order_param
        .projection
        .iter()
        .any(|item| item.contains("order key 1: $bad")));

    insert_query_node(
        &engine,
        "Person",
        "bytes-key",
        &[("payload", PropValue::Bytes(vec![1, 2, 3]))],
        1.0,
    );
    let bytes_limit_zero = engine
        .execute_gql(
            "MATCH (n:Person) WHERE n.key = 'bytes-key' RETURN id(n) ORDER BY n.payload LIMIT 0",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(bytes_limit_zero.rows.is_empty());
}

#[test]
fn gql_row_op_caps_and_stats_are_truthful() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "Person",
        "stats-a",
        &[("flag", PropValue::Bool(true)), ("rank", PropValue::Int(1))],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "stats-b",
        &[("rank", PropValue::Int(2))],
        1.0,
    );

    let no_residual = execute_gql_ok(&engine, "MATCH (n:Person) RETURN id(n)");
    assert_eq!(no_residual.stats.rows_matched, 2);
    assert_eq!(no_residual.stats.rows_after_filter, 2);
    assert_eq!(no_residual.stats.rows_returned, 2);
    assert_eq!(no_residual.stats.db_hits, 0);
    assert_eq!(no_residual.stats.elapsed_us, None);

    let residual_true = execute_gql_ok(&engine, "MATCH (n:Person) WHERE n.flag IS NOT NULL RETURN id(n)");
    assert_eq!(residual_true.stats.rows_matched, 2);
    assert_eq!(residual_true.stats.rows_after_filter, 1);
    assert_eq!(residual_true.stats.rows_returned, 1);

    let residual_false = execute_gql_ok(&engine, "MATCH (n:Person) WHERE n.flag IS NULL RETURN id(n)");
    assert_eq!(residual_false.stats.rows_matched, 2);
    assert_eq!(residual_false.stats.rows_after_filter, 1);
    assert_eq!(residual_false.stats.rows_returned, 1);

    let user_limit = execute_gql_ok(&engine, "MATCH (n:Person) RETURN id(n) LIMIT 1");
    assert_eq!(user_limit.rows.len(), 1);

    let profiled = engine
        .execute_gql(
            "MATCH (n:Person) RETURN id(n) ORDER BY n.rank LIMIT 1",
            &GqlParams::new(),
            &GqlExecutionOptions {
                profile: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(profiled.stats.rows_returned, 1);
    assert!(profiled.stats.elapsed_us.is_some());
    assert_eq!(profiled.stats.db_hits, 2);
    let profiled_residual = engine
        .execute_gql(
            "MATCH (n:Person) WHERE n.flag IS NOT NULL RETURN id(n) ORDER BY n.rank LIMIT 1",
            &GqlParams::new(),
            &GqlExecutionOptions {
                profile: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(profiled_residual.stats.rows_returned, 1);
    assert_eq!(profiled_residual.stats.rows_after_filter, 1);
    assert_eq!(profiled_residual.stats.db_hits, 2);

    let max_skip = engine
        .execute_gql(
            "MATCH (n:Person) RETURN id(n) SKIP 2",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_skip: 1,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        max_skip,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::InvalidReturnExpression,
            ..
        }
    ));

    let capped_order = engine
        .execute_gql(
            "MATCH (n:Person) RETURN id(n) ORDER BY n.rank",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_intermediate_bindings: 1,
                max_frontier: 1,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap_err();
    assert!(
        capped_order
            .to_string()
            .contains("max_intermediate_bindings exceeded configured cap 1"),
        "unexpected error: {capped_order}"
    );
}

#[test]
fn gql_explain_reports_targets_row_ops_caps_and_does_not_execute_rows() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(
        &engine,
        "Person",
        "explain-from",
        &[("name", PropValue::String("Ada".to_string()))],
        1.0,
    );
    let to = insert_query_node(&engine, "Article", "explain-to", &[], 1.0);
    engine
        .upsert_edge(from, to, "LIKES", UpsertEdgeOptions::default())
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let node = engine
        .explain_gql(
            "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n.name ORDER BY n.name LIMIT 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let node = gql_read_explain(&node);
    assert_eq!(node.columns, vec!["n.name"]);
    assert_eq!(node.target, GqlLoweringTarget::GraphRowQuery);
    assert!(node.native_plan.is_none());
    assert!(node
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: GraphRowPhysicalPlan")));
    assert!(node
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: NodeCandidateSource")));
    assert!(node
        .projection
        .iter()
        .any(|item| item.contains("graph row row op: Order")));
    assert!(node
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: FinalHydrationProjection")));
    for expected in [
        "graph row order: explicit=true",
        "graph row cursor: supplied=false",
        "graph row caps: allow_full_scan=",
        "max_order_materialization=",
        "graph row note: source correctness",
        "graph row note: effective_at_epoch source",
        "graph row note: fanout-aware physical source choice is advisory only",
    ] {
        assert!(
            node.projection.iter().any(|item| item.contains(expected)),
            "expected graph-row explain summary {expected:?}, got {:?}",
            node.projection
        );
    }
    assert!(node.pushed_down.iter().any(|item| item.contains("n.name")));
    assert!(node.projection.iter().any(|item| item.contains("n.name")));
    assert!(node
        .projection
        .iter()
        .any(|item| item.contains("order selected field: n.name")));
    assert!(node.row_ops.contains(&GqlRowOperation::Sort));
    assert!(node.row_ops.contains(&GqlRowOperation::Limit));
    assert_eq!(node.caps.max_rows, GqlExecutionOptions::default().max_rows);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.graph_row_query_calls, 0);
    assert_eq!(counters.public_node_query_calls, 0);
    assert_eq!(counters.node_selected_field_batches, 0);

    let cap_options = GqlExecutionOptions {
        max_rows: 7,
        max_intermediate_bindings: 17,
        max_skip: 19,
        max_query_bytes: 1_024,
        max_param_bytes: 1_025,
        max_ast_depth: 31,
        max_literal_items: 37,
        ..gql_opts()
    };
    let cap_summary = engine
        .explain_gql(
            "MATCH (n:Person) RETURN id(n) LIMIT 1",
            &GqlParams::new(),
            &cap_options,
        )
        .unwrap()
        .caps;
    assert_eq!(cap_summary.max_rows, 7);
    assert_eq!(cap_summary.max_intermediate_bindings, 17);
    assert_eq!(cap_summary.max_skip, 19);
    assert_eq!(cap_summary.max_query_bytes, 1_024);
    assert_eq!(cap_summary.max_param_bytes, 1_025);
    assert_eq!(cap_summary.max_ast_depth, 31);
    assert_eq!(cap_summary.max_literal_items, 37);

    let default_node_projection = engine
        .explain_gql("MATCH (n:Person) RETURN n", &GqlParams::new(), &gql_opts())
        .unwrap();
    let default_node_projection = gql_read_explain(&default_node_projection);
    assert!(default_node_projection
        .projection
        .iter()
        .any(|item| item.contains("node element n (vectors omitted)")));
    let vector_node_projection = engine
        .explain_gql(
            "MATCH (n:Person) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_vectors: true,
                ..GqlExecutionOptions::default()
            },
        )
        .unwrap();
    let vector_node_projection = gql_read_explain(&vector_node_projection);
    assert!(vector_node_projection
        .projection
        .iter()
        .any(|item| item.contains("node element n (vectors included)")));

    let residual_order = engine
        .explain_gql(
            "MATCH (n:Person) WHERE n.name IS NOT NULL RETURN id(n) ORDER BY n.name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let residual_order = gql_read_explain(&residual_order);
    assert!(residual_order
        .projection
        .iter()
        .any(|item| item.contains("residual selected field: n.name")));
    assert!(residual_order
        .projection
        .iter()
        .any(|item| item.contains("order selected field: n.name")));

    let id_order = engine
        .explain_gql(
            "MATCH (n:Person) RETURN n.key ORDER BY id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let id_order = gql_read_explain(&id_order);
    assert!(id_order
        .projection
        .iter()
        .any(|item| item.contains("order key 1: id(n)")));

    let labels_return = engine
        .explain_gql("MATCH (n:Person) RETURN labels(n)", &GqlParams::new(), &gql_opts())
        .unwrap();
    let labels_return = gql_read_explain(&labels_return);
    assert!(labels_return
        .projection
        .iter()
        .any(|item| item.contains("output selected field: n.labels")));

    let edge = engine
        .explain_gql(
            "MATCH ()-[r:LIKES]->() RETURN r.from, r.to, type(r), r.valid_from, r.valid_to",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let edge = gql_read_explain(&edge);
    assert_eq!(edge.target, GqlLoweringTarget::GraphRowQuery);
    assert!(edge.native_plan.is_none());
    for expected in ["r.from", "r.to", "r.label", "r.valid_from", "r.valid_to"] {
        assert!(
            edge.projection.iter().any(|item| item.contains(expected)),
            "expected projection summary for {expected}, got {:?}",
            edge.projection
        );
    }

    let pattern = engine
        .explain_gql(
            "MATCH (a:Person)-[r:LIKES]->(b:Article) RETURN id(a), id(r), id(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let pattern = gql_read_explain(&pattern);
    assert_eq!(pattern.target, GqlLoweringTarget::GraphRowQuery);
    assert!(pattern.native_plan.is_none());
    assert!(pattern
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: AdjacencyExpansion")));
    assert!(pattern
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: GraphRowPlanAlternative")));
    assert!(pattern
        .projection
        .iter()
        .any(|item| item.contains("chosen; kind=")));
    assert!(pattern
        .projection
        .iter()
        .any(|item| item.contains("source=EndpointAdjacency")));
    assert!(pattern
        .projection
        .iter()
        .any(|item| item.contains("graph row plan: EndpointNodeVerification")));
    assert!(!pattern
        .projection
        .iter()
        .any(|item| item.contains("PatternQuery") || item.contains("PatternExpand")));

    let with_plan = execute_gql_with_options(
        &engine,
        "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 1",
        GqlExecutionOptions {
            include_plan: true,
            ..GqlExecutionOptions::default()
        },
    );
    assert!(with_plan.plan.is_some());
    assert_eq!(
        gql_read_explain(with_plan.plan.as_ref().unwrap()).target,
        GqlLoweringTarget::GraphRowQuery
    );

    let standalone = engine
        .explain_gql(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(with_plan.plan.unwrap(), standalone);
}

#[test]
fn gql_full_scan_rejection_allowance_and_row_caps_are_truthful() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "scan-a", &[], 1.0);
    insert_query_node(&engine, "Person", "scan-b", &[], 1.0);

    let rejected = engine
        .execute_gql("MATCH (n) RETURN id(n)", &GqlParams::new(), &gql_opts())
        .unwrap_err();
    assert!(matches!(
        rejected,
        EngineError::GqlSemantic {
            code: GqlSemanticErrorCode::FullScanNotAllowed,
            ..
        }
    ));

    let capped = execute_gql_with_options(
        &engine,
        "MATCH (n) RETURN id(n)",
        GqlExecutionOptions {
            allow_full_scan: true,
            max_rows: 1,
            max_intermediate_bindings: 100,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(capped.rows.len(), 1);
    assert_eq!(capped.stats.rows_matched, 1);

    let constant_residual = execute_gql_with_options(
        &engine,
        "MATCH (n) WHERE true RETURN id(n)",
        GqlExecutionOptions {
            allow_full_scan: true,
            max_rows: 1,
            max_intermediate_bindings: 100,
            ..GqlExecutionOptions::default()
        },
    );
    assert_eq!(constant_residual.rows.len(), 1);
    assert_eq!(constant_residual.stats.rows_matched, 1);

    let false_residual = execute_gql_with_options(
        &engine,
        "MATCH (n) WHERE false RETURN id(n)",
        GqlExecutionOptions {
            allow_full_scan: true,
            max_rows: 1,
            max_intermediate_bindings: 100,
            ..GqlExecutionOptions::default()
        },
    );
    assert!(false_residual.rows.is_empty());
    assert_eq!(false_residual.stats.rows_matched, 2);
    assert_eq!(false_residual.stats.rows_after_filter, 0);
    assert!(!false_residual
        .stats
        .warnings
        .iter()
        .any(|warning| warning.contains("native/intermediate")));

    let impossible_float_id = execute_gql_with_options(
        &engine,
        "MATCH (n) WHERE id(n) = 1.5 RETURN id(n)",
        GqlExecutionOptions {
            max_intermediate_bindings: 1,
            ..GqlExecutionOptions::default()
        },
    );
    assert!(impossible_float_id.rows.is_empty());
    assert_eq!(impossible_float_id.stats.rows_matched, 0);
}

#[test]
fn gql_filter_only_unindexed_sources_report_structured_full_scan_errors() {
    let (_dir, engine) = query_test_engine();
    let source_node = insert_query_node(
        &engine,
        "Person",
        "runtime-full-scan-source",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let target_node = insert_query_node(&engine, "Person", "runtime-full-scan-target", &[], 1.0);
    engine
        .upsert_edge(
            source_node,
            target_node,
            "RUNTIME_FULL_SCAN_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    for source in [
        "MATCH (n) WHERE n.status = 'active' RETURN id(n)",
        "MATCH ()-[r]->() WHERE r.status = 'active' RETURN id(r)",
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        match err {
            EngineError::GqlSemantic { code, message, .. } => {
                assert_eq!(code, GqlSemanticErrorCode::FullScanNotAllowed);
                assert!(
                    message.contains("allow_full_scan"),
                    "unexpected full-scan message for {source:?}: {message}"
                );
            }
            other => {
                panic!("expected structured GQL full-scan error for {source:?}, got {other:?}")
            }
        }
    }
}

#[test]
fn gql_projection_counters_prove_scalar_fast_paths_and_no_public_query_calls() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(
        &engine,
        "Person",
        "counter-from",
        &[("name", PropValue::String("Ada".to_string()))],
        1.0,
    );
    let to = insert_query_node(&engine, "Article", "counter-to", &[], 1.0);
    let edge = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2026))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let node_prop = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name");
    assert_eq!(gql_string_column(&node_prop, 0), vec!["Ada".to_string()]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.public_node_query_calls, 0);
    assert_eq!(counters.public_edge_query_calls, 0);

    engine.reset_query_execution_counters_for_test();
    let residual_and_output = execute_gql_ok(&engine, "MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n.name");
    assert_eq!(
        gql_string_column(&residual_and_output, 0),
        vec!["Ada".to_string()]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.public_node_query_calls, 0);

    engine.reset_query_execution_counters_for_test();
    execute_gql_ok(&engine, "MATCH (n:Person) RETURN id(n)");
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 0);
    assert_eq!(counters.public_node_query_calls, 0);

    engine.reset_query_execution_counters_for_test();
    let edge_prop = execute_gql_ok(&engine, "MATCH ()-[r:LIKES]->() RETURN r.since");
    assert_eq!(edge_prop.rows[0].values[0], GqlValue::Int(2026));
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.public_edge_query_calls, 0);

    engine.reset_query_execution_counters_for_test();
    let edge_metadata = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() RETURN id(r), type(r), r.from, r.to",
    );
    assert_eq!(
        edge_metadata.rows[0].values,
        vec![
            GqlValue::UInt(edge),
            GqlValue::String("LIKES".to_string()),
            GqlValue::UInt(from),
            GqlValue::UInt(to),
        ]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_batches, 1);
    assert_eq!(counters.edge_selected_field_ids, 1);
    assert_eq!(counters.public_edge_query_calls, 0);

    engine.reset_query_execution_counters_for_test();
    let ordered_scalar = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n.name ORDER BY n.name");
    assert_eq!(
        gql_string_column(&ordered_scalar, 0),
        vec!["Ada".to_string()]
    );
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.public_node_query_calls, 0);
}

#[test]
fn gql_projection_need_classes_are_truthful_for_node_residual_order_output() {
    let lowered = lowered_gql_for_projection_test(
        "MATCH (n:Person) WHERE n.status = 'active' OR false RETURN id(n) ORDER BY n.rank",
    );
    assert_eq!(lowered.residual_predicates.len(), 1);
    let alias_projection = gql_alias_projection_map(&lowered);
    let projection_alias = alias_projection.get("n").unwrap();
    let order_by = resolve_order_by_return_aliases(&lowered).unwrap();
    let order_exprs = order_by
        .iter()
        .map(|item| item.expr.clone())
        .collect::<Vec<_>>();

    let residual_projection = crate::gql::eval::build_runtime_projection_for_need_class(
        &lowered.residual_predicates,
        &lowered.semantic,
        &alias_projection,
        false,
        false,
        crate::row_projection::ProjectionNeedClass::Residual,
    )
    .unwrap();
    let order_projection = crate::gql::eval::build_runtime_projection_for_need_class(
        &order_exprs,
        &lowered.semantic,
        &alias_projection,
        false,
        false,
        crate::row_projection::ProjectionNeedClass::Order,
    )
    .unwrap();
    let pre_projection = crate::gql::eval::build_runtime_projection_for_need_classes(
        &[
            crate::gql::eval::GqlRuntimeProjectionExprs {
                exprs: &lowered.residual_predicates,
                need_class: crate::row_projection::ProjectionNeedClass::Residual,
            },
            crate::gql::eval::GqlRuntimeProjectionExprs {
                exprs: &order_exprs,
                need_class: crate::row_projection::ProjectionNeedClass::Order,
            },
        ],
        &lowered.semantic,
        &alias_projection,
        false,
        false,
    )
    .unwrap();
    let pre_keys = pre_projection
        .keys
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let returns = crate::gql::eval::return_exprs(&lowered.semantic);
    let return_exprs = returns
        .iter()
        .map(|return_expr| return_expr.expr.clone())
        .collect::<Vec<_>>();
    let output_projection = crate::gql::eval::build_runtime_projection_excluding(
        &return_exprs,
        &lowered.semantic,
        &alias_projection,
        true,
        false,
        &pre_keys,
    )
    .unwrap();

    assert_node_need_props(&residual_projection.plan.needs.residual, projection_alias, &["status"]);
    assert!(residual_projection.plan.needs.output.nodes.is_empty());
    assert_node_need_props(&order_projection.plan.needs.order, projection_alias, &["rank"]);
    assert!(order_projection.plan.needs.output.nodes.is_empty());

    assert_node_need_props(&pre_projection.plan.needs.residual, projection_alias, &["status"]);
    assert_node_need_props(&pre_projection.plan.needs.order, projection_alias, &["rank"]);
    assert!(pre_projection.plan.needs.output.nodes.is_empty());
    assert_entity_needs_do_not_request_all_properties(&pre_projection.plan.needs.residual);
    assert_entity_needs_do_not_request_all_properties(&pre_projection.plan.needs.order);

    assert_eq!(
        output_projection.keys,
        vec![crate::gql::eval::GqlRuntimeValueKey::NodeMetadata {
            alias: "n".to_string(),
            field: NodeProjectionField::Id,
        }]
    );
    assert!(output_projection.plan.needs.output.nodes.is_empty());
}

#[test]
fn gql_projection_need_classes_keep_return_node_as_output_element() {
    let lowered = lowered_gql_for_projection_test("MATCH (n:Person) RETURN n");
    let alias_projection = gql_alias_projection_map(&lowered);
    let projection_alias = alias_projection.get("n").unwrap();
    let returns = crate::gql::eval::return_exprs(&lowered.semantic);
    let return_exprs = returns
        .iter()
        .map(|return_expr| return_expr.expr.clone())
        .collect::<Vec<_>>();

    let output_projection = crate::gql::eval::build_runtime_projection_excluding(
        &return_exprs,
        &lowered.semantic,
        &alias_projection,
        true,
        false,
        &std::collections::BTreeSet::new(),
    )
    .unwrap();

    let node_needs = output_projection
        .plan
        .needs
        .output
        .nodes
        .get(projection_alias)
        .unwrap();
    assert_eq!(node_needs.props, PropertySelection::All);
    assert!(!node_needs.vectors.needs_dense());
    assert!(!node_needs.vectors.needs_sparse());
    assert!(output_projection.plan.needs.residual.nodes.is_empty());
    assert!(output_projection.plan.needs.order.nodes.is_empty());
}

#[test]
fn gql_residual_and_order_selected_field_reads_are_merged_for_node_scalars() {
    let (_dir, engine) = query_test_engine();
    let high = insert_query_node(
        &engine,
        "Person",
        "merge-high",
        &[
            ("status", PropValue::String("active".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.0,
    );
    let low = insert_query_node(
        &engine,
        "Person",
        "merge-low",
        &[
            ("status", PropValue::String("active".to_string())),
            ("rank", PropValue::Int(2)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "Person",
        "merge-inactive",
        &[
            ("status", PropValue::String("inactive".to_string())),
            ("rank", PropValue::Int(0)),
        ],
        1.0,
    );

    engine.reset_query_execution_counters_for_test();
    let result = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE n.status = 'active' OR false RETURN id(n) ORDER BY n.rank",
    );
    assert_eq!(gql_u64_column(&result, 0), vec![high, low]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 3);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);
    assert_eq!(counters.public_node_query_calls, 0);
}

#[test]
fn gql_projection_need_classes_and_read_merge_hold_for_edge_scalars() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "edge-merge-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "edge-merge-to", &[], 1.0);
    let high = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("active".to_string())),
                    ("rank", PropValue::Int(1)),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let low = engine
        .upsert_edge(
            to,
            from,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("active".to_string())),
                    ("rank", PropValue::Int(2)),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            from,
            from,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("inactive".to_string())),
                    ("rank", PropValue::Int(0)),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    let lowered = lowered_gql_for_projection_test(
        "MATCH ()-[r:LIKES]->() WHERE r.status = 'active' OR false RETURN id(r) ORDER BY r.rank",
    );
    let alias_projection = gql_alias_projection_map(&lowered);
    let projection_alias = alias_projection.get("r").unwrap();
    let order_by = resolve_order_by_return_aliases(&lowered).unwrap();
    let order_exprs = order_by
        .iter()
        .map(|item| item.expr.clone())
        .collect::<Vec<_>>();
    let pre_projection = crate::gql::eval::build_runtime_projection_for_need_classes(
        &[
            crate::gql::eval::GqlRuntimeProjectionExprs {
                exprs: &lowered.residual_predicates,
                need_class: crate::row_projection::ProjectionNeedClass::Residual,
            },
            crate::gql::eval::GqlRuntimeProjectionExprs {
                exprs: &order_exprs,
                need_class: crate::row_projection::ProjectionNeedClass::Order,
            },
        ],
        &lowered.semantic,
        &alias_projection,
        false,
        false,
    )
    .unwrap();
    assert_edge_need_props(&pre_projection.plan.needs.residual, projection_alias, &["status"]);
    assert_edge_need_props(&pre_projection.plan.needs.order, projection_alias, &["rank"]);
    assert!(pre_projection.plan.needs.output.edges.is_empty());
    assert_entity_needs_do_not_request_all_properties(&pre_projection.plan.needs.residual);
    assert_entity_needs_do_not_request_all_properties(&pre_projection.plan.needs.order);

    engine.reset_query_execution_counters_for_test();
    let result = execute_gql_ok(
        &engine,
        "MATCH ()-[r:LIKES]->() WHERE r.status = 'active' OR false RETURN id(r) ORDER BY r.rank",
    );
    assert_eq!(gql_u64_column(&result, 0), vec![high, low]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_batches, 1);
    assert_eq!(counters.edge_selected_field_ids, 3);
    assert_eq!(counters.public_edge_query_calls, 0);
}

#[test]
fn gql_default_node_elements_omit_vectors_and_include_vectors_opts_in() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        },
    )
    .unwrap();
    seed_query_test_catalog(&engine);
    engine
        .upsert_node(
            "Person",
            "vector-alpha",
            UpsertNodeOptions {
                props: query_test_props(&[("name", PropValue::String("alpha".to_string()))]),
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(3, 1.5)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    engine
        .upsert_node(
            "Person",
            "vector-omega",
            UpsertNodeOptions {
                props: query_test_props(&[("name", PropValue::String("omega".to_string()))]),
                dense_vector: Some(vec![0.4, 0.5, 0.6]),
                sparse_vector: Some(vec![(7, 2.5)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let default_result = execute_gql_ok(&engine, "MATCH (n:Person) RETURN n");
    let default_node = gql_single_node(&default_result.rows[0].values[0]);
    assert!(default_node.dense_vector.is_none());
    assert!(default_node.sparse_vector.is_none());
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);

    engine.reset_query_execution_counters_for_test();
    let ordered_default = execute_gql_with_options(
        &engine,
        "MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 1",
        GqlExecutionOptions {
            allow_full_scan: false,
            ..GqlExecutionOptions::default()
        },
    );
    let ordered_node = gql_single_node(&ordered_default.rows[0].values[0]);
    assert!(ordered_node.dense_vector.is_none());
    assert!(ordered_node.sparse_vector.is_none());
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);

    engine.reset_query_execution_counters_for_test();
    let with_vectors = execute_gql_with_options(
        &engine,
        "MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 1",
        GqlExecutionOptions {
            include_vectors: true,
            ..GqlExecutionOptions::default()
        },
    );
    let node = gql_single_node(&with_vectors.rows[0].values[0]);
    assert_eq!(node.dense_vector.as_deref(), Some([0.1, 0.2, 0.3].as_slice()));
    assert_eq!(node.sparse_vector.as_deref(), Some([(3, 1.5)].as_slice()));
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.node_dense_vector_projection_reads, 1);
    assert_eq!(counters.node_sparse_vector_projection_reads, 1);
}

#[test]
fn gql_pattern_projection_batches_edge_aliases_by_need_group() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "Person", "dup-a", &[], 1.0);
    let b = insert_query_node(&engine, "Person", "dup-b", &[], 1.0);
    let c = insert_query_node(&engine, "Article", "dup-c", &[], 1.0);
    let first = engine
        .upsert_edge(
            a,
            b,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2020))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let second = engine
        .upsert_edge(
            b,
            c,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("since", PropValue::Int(2021))]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();

    engine.reset_query_execution_counters_for_test();
    let result = execute_gql_ok(
        &engine,
        "MATCH (a:Person)-[r:LIKES]->(b:Person)-[s:LIKES]->(c:Article) \
         RETURN id(r), id(s), r.since, s.since",
    );
    assert_eq!(result.rows[0].values, vec![
        GqlValue::UInt(first),
        GqlValue::UInt(second),
        GqlValue::Int(2020),
        GqlValue::Int(2021),
    ]);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.public_node_query_calls, 0);
    assert_eq!(counters.public_edge_query_calls, 0);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_selected_field_batches, 2);
    assert_eq!(counters.edge_selected_field_ids, 2);
}

#[test]
fn gql_scalar_projection_survives_flush_reopen_and_tombstone_shadowing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    seed_query_test_catalog(&engine);
    let keep = insert_query_node(
        &engine,
        "Person",
        "reopen-keep",
        &[("state", PropValue::String("old".to_string()))],
        1.0,
    );
    let drop = insert_query_node(
        &engine,
        "Person",
        "reopen-drop",
        &[("state", PropValue::String("drop".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let updated = insert_query_node(
        &engine,
        "Person",
        "reopen-keep",
        &[("state", PropValue::String("new".to_string()))],
        1.0,
    );
    assert_eq!(updated, keep);
    engine.delete_node(drop).unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let result = execute_gql_ok(
        &reopened,
        "MATCH (n:Person) WHERE n.key = 'reopen-keep' RETURN n.state ORDER BY n.state",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::String("new".to_string()));

    let all_keys = execute_gql_ok(&reopened, "MATCH (n:Person) RETURN n.key ORDER BY n.key LIMIT 10");
    assert!(!gql_string_column(&all_keys, 0).contains(&"reopen-drop".to_string()));
    reopened.close().unwrap();
}

#[test]
fn gql_edge_metadata_functions_and_dot_properties_survive_reopen_shadowing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    seed_query_test_catalog(&engine);
    let from = insert_query_node(&engine, "Person", "edge-dot-from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "edge-dot-to", &[], 1.0);
    let old_edge = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("id", PropValue::String("old-property-id".to_string())),
                    ("label", PropValue::String("old-property-label".to_string())),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let edge = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("id", PropValue::String("property-id".to_string())),
                    ("label", PropValue::String("property-label".to_string())),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    assert_ne!(edge, old_edge);
    engine.delete_edge(old_edge).unwrap();
    engine.flush().unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let result = execute_gql_ok(
        &reopened,
        "MATCH ()-[r:LIKES]->() RETURN id(r), type(r), r.id, r.label, r.from, r.to",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].values,
        vec![
            GqlValue::UInt(edge),
            GqlValue::String("LIKES".to_string()),
            GqlValue::String("property-id".to_string()),
            GqlValue::String("property-label".to_string()),
            GqlValue::UInt(from),
            GqlValue::UInt(to),
        ]
    );
    reopened.close().unwrap();
}

#[test]
fn gql_indexed_and_pattern_oracles_survive_flush_reopen_with_shadows() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    seed_query_test_catalog(&engine);
    let fixture = seed_rich_gql_graph(&engine);
    engine.flush().unwrap();
    let indexes = install_rich_gql_indexes(&engine);

    let updated_bob = insert_query_node_with_labels(
        &engine,
        &["Person", "Employee"],
        "rich-bob",
        &[
            ("status", PropValue::String("archived".to_string())),
            ("score", PropValue::Int(76)),
            ("department", PropValue::String("platform".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.5,
    );
    assert_eq!(updated_bob, fixture.bob);
    engine.delete_edge(fixture.review_edge).unwrap();
    engine.flush().unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(
        &reopened,
        indexes.employee_status,
        SecondaryIndexState::Ready,
    );
    wait_for_property_index_state(
        &reopened,
        indexes.employee_score,
        SecondaryIndexState::Ready,
    );
    wait_for_edge_property_index_state(&reopened, indexes.works_role, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&reopened, indexes.works_hours, SecondaryIndexState::Ready);

    let indexed_result = execute_gql_ok(
        &reopened,
        "MATCH (n:Person:Employee) WHERE n.status = 'focus' RETURN id(n) ORDER BY id(n)",
    );
    let mut indexed_native = reopened
        .query_node_ids(&NodeQuery {
            label_filter: Some(node_label_filter(
                &["Person", "Employee"],
                LabelMatchMode::All,
            )),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "status".to_string(),
                value: PropValue::String("focus".to_string()),
            }),
            ..NodeQuery::default()
        })
        .unwrap()
        .items;
    indexed_native.sort_unstable();
    assert_eq!(gql_u64_column(&indexed_result, 0), indexed_native);
    assert!(indexed_native.contains(&fixture.alice));
    assert!(!indexed_native.contains(&fixture.bob));
    let indexed_explain = reopened
        .explain_gql(
            "MATCH (n:Person:Employee) WHERE n.status = 'focus' RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let indexed_explain = gql_read_explain(&indexed_explain);
    assert_eq!(indexed_explain.target, GqlLoweringTarget::GraphRowQuery);
    assert!(indexed_explain.native_plan.is_none());
    assert!(indexed_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.status")));

    let lead_pattern = execute_gql_ok(
        &reopened,
        "MATCH (p:Person:Employee)-[r:WORKS_ON]->(c:Company) \
         WHERE p.status = 'focus' AND r.role = 'lead' \
         RETURN id(p), id(r), id(c) ORDER BY id(r)",
    );
    let lead_pattern_explain = reopened
        .explain_gql(
            "MATCH (p:Person:Employee)-[r:WORKS_ON]->(c:Company) \
             WHERE p.status = 'focus' AND r.role = 'lead' \
             RETURN id(p), id(r), id(c) ORDER BY id(r)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let lead_pattern_explain = gql_read_explain(&lead_pattern_explain);
    assert_eq!(
        lead_pattern_explain.target,
        GqlLoweringTarget::GraphRowQuery
    );
    assert!(lead_pattern_explain.native_plan.is_none());
    assert!(lead_pattern_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("p.status")));
    assert!(lead_pattern_explain
        .pushed_down
        .iter()
        .any(|item| item.contains("r.role")));
    let gql_lead = lead_pattern
        .rows
        .iter()
        .map(|row| match (&row.values[0], &row.values[1], &row.values[2]) {
            (GqlValue::UInt(p), GqlValue::UInt(r), GqlValue::UInt(c)) => (*p, *r, *c),
            other => panic!("expected id tuple, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        gql_lead,
        vec![
            (fixture.alice, fixture.lead_edge, fixture.acme),
            (fixture.alice, fixture.startup_edge, fixture.globex),
        ]
    );

    let deleted_pattern = execute_gql_ok(
        &reopened,
        "MATCH (p:Person:Employee)-[r:WORKS_ON]->(c:Company) \
         WHERE r.role = 'reviewer' RETURN id(r)",
    );
    assert!(deleted_pattern.rows.is_empty());
    assert!(reopened.get_edge(fixture.review_edge).unwrap().is_none());

    reopened.close().unwrap();
}
