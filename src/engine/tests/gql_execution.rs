// Public Rust GQL execution tests.

fn gql_opts() -> GqlExecutionOptions {
    GqlExecutionOptions::default()
}

fn gql_read_explain(explain: &GqlExecutionExplain) -> &GqlExplain {
    assert_eq!(explain.kind, GqlStatementKind::Query);
    assert!(explain.mutation.is_none());
    assert!(explain.schema.is_none());
    assert!(explain.index.is_none());
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

fn assert_gql_schema_result<'a>(
    result: &'a GqlExecutionResult,
    operation: &str,
) -> &'a GqlSchemaStats {
    assert_eq!(result.kind, GqlStatementKind::Schema);
    assert!(result.mutation_stats.is_none());
    assert!(result.index_stats.is_none());
    assert!(result.next_cursor.is_none());
    let stats = result
        .schema_stats
        .as_ref()
        .expect("schema result should include schema_stats");
    assert_eq!(stats.operation, operation);
    stats
}

fn gql_schema_explain(explain: &GqlExecutionExplain) -> &GqlSchemaExplain {
    assert_eq!(explain.kind, GqlStatementKind::Schema);
    assert!(explain.read.is_none());
    assert!(explain.mutation.is_none());
    assert!(explain.index.is_none());
    explain
        .schema
        .as_ref()
        .expect("schema explain should be present")
}

fn gql_map(value: &GqlValue) -> &BTreeMap<String, GqlValue> {
    match value {
        GqlValue::Map(map) => map,
        other => panic!("expected GQL map, got {other:?}"),
    }
}

fn gql_list(value: &GqlValue) -> &[GqlValue] {
    match value {
        GqlValue::List(values) => values,
        other => panic!("expected GQL list, got {other:?}"),
    }
}

fn gql_str(value: &GqlValue) -> &str {
    match value {
        GqlValue::String(value) => value,
        other => panic!("expected GQL string, got {other:?}"),
    }
}

fn gql_tagged_value(value: &GqlValue) -> (&str, &GqlValue) {
    let map = gql_map(value);
    (gql_str(&map["type"]), &map["value"])
}

fn assert_gql_tagged_uint(value: &GqlValue, expected: &str) {
    let (kind, payload) = gql_tagged_value(value);
    assert_eq!(kind, "uint");
    assert_eq!(gql_str(payload), expected);
}

fn assert_gql_tagged_bytes(value: &GqlValue, expected: &[i64]) {
    let (kind, payload) = gql_tagged_value(value);
    assert_eq!(kind, "bytes");
    let actual = gql_list(payload)
        .iter()
        .map(|value| match value {
            GqlValue::Int(value) => *value,
            other => panic!("expected byte Int, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn gql_u64_or_i64_values(result: &GqlExecutionResult, index: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|row| match &row.values[index] {
            GqlValue::Int(value) => value.to_string(),
            GqlValue::UInt(value) => value.to_string(),
            GqlValue::Float(value) => value.to_string(),
            other => panic!("expected numeric column, got {other:?}"),
        })
        .collect()
}

fn return_star_id_rows(a: u64, b: u64) -> Vec<GqlRow> {
    vec![GqlRow {
        values: vec![GqlValue::UInt(a), GqlValue::UInt(b)],
    }]
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
    assert_eq!(options.max_pipeline_rows, 65_536);
    assert_eq!(options.max_groups, 65_536);
    assert_eq!(options.max_collect_items, 65_536);
    assert_eq!(options.max_union_branches, 16);
    assert_eq!(options.max_subquery_invocations, 4_096);
    assert_eq!(options.max_subquery_depth, 2);
    assert_eq!(options.max_shortest_path_pairs, 4_096);
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
    assert_eq!(result.schema_stats, None);
    assert_eq!(result.index_stats, None);
    assert_eq!(result.columns, vec!["id"]);
    assert_eq!(gql_u64_column(&result, 0), vec![active]);
    let plan = result.plan.as_ref().expect("include_plan should return plan");
    assert_eq!(plan.kind, GqlStatementKind::Query);
    assert_eq!(plan.columns, vec!["id"]);
    assert!(plan.read.is_some());
    assert!(plan.mutation.is_none());
    assert!(plan.schema.is_none());
    assert!(plan.index.is_none());
}

#[test]
fn execute_gql_create_mutation_preserves_cursor_and_readonly_ordering() {
    let (_dir, engine) = query_test_engine();
    let source = "CREATE (n:GqlMutationNoSideEffect {elementKey: 'n'}) RETURN n";

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
    assert!(result.schema_stats.is_none());
    assert!(result.index_stats.is_none());
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
            "CREATE (n:GqlMutationIncludePlan {elementKey: 'n'}) RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let plan = planned.plan.expect("mutation include_plan should return a plan");
    assert_eq!(plan.kind, GqlStatementKind::Mutation);
    assert!(plan.schema.is_none());
    assert!(plan.index.is_none());
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
    let source = format!("CREATE (n:{label} {{elementKey: 'n'}}) RETURN n");
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
    assert!(explain.schema.is_none());
    assert!(explain.index.is_none());
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
            "CREATE (n:Person {elementKey: $key}) RETURN n",
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
                profile: true,
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
        ("MATCH (n:Person {elementKey: $key}) SET n.name = 'Ada'", "key"),
        ("CREATE (n:Person {elementKey: $key})", "key"),
        ("MATCH (n:Person {elementKey: 'a'}) SET n.name = $name", "name"),
        ("MATCH (n:Person {elementKey: 'a'}) SET n += $props", "props"),
        ("MATCH (n:Person {elementKey: 'a'}) DELETE $target", "target"),
        ("CREATE (n:Person {elementKey: 'a'}) RETURN $value", "value"),
        (
            "CREATE (n:Person {elementKey: 'a'}) RETURN n ORDER BY $order",
            "order",
        ),
        ("CREATE (n:Person {elementKey: 'a'}) RETURN n SKIP $skip", "skip"),
        ("CREATE (n:Person {elementKey: 'a'}) RETURN n LIMIT $limit", "limit"),
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &options)
            .unwrap_err();
        assert_gql_param_error(err, missing_name, "missing parameter");
    }

    let cap_err = engine
        .execute_gql(
            "CREATE (n:Person {elementKey: $key})",
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
            "CREATE (n:Person {elementKey: 'literal'})",
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
            "CREATE (n:Person:Employee {elementKey: 'gql-create-ada', name: 'Ada', age: 37, weight: 2.5, nullable: null}) RETURN n, id(n), n.name",
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
        .ensure_node_property_index("GqlCreatedIndexed", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();

    let created = engine
        .execute_gql(
            "CREATE (n:GqlCreatedIndexed {elementKey: 'n', status: 'ready', score: 7}) RETURN id(n)",
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
fn gql_merge_node_creates_matches_duplicates_and_actions_are_atomic() {
    let (_dir, engine) = query_test_engine();

    let created = engine
        .execute_gql(
            "MERGE (n:GqlMergeNode {elementKey: 'n'}) ON CREATE SET n.status = 'created' ON MATCH SET n.status = 'matched' RETURN id(n), n.status",
            &GqlParams::new(),
            &GqlExecutionOptions {
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let created_id = gql_u64_column(&created, 0)[0];
    assert_eq!(created.rows[0].values[1], GqlValue::String("created".to_string()));
    let stats = created.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.nodes_updated, 0);
    assert!(stats.db_hits >= 1);

    let matched = engine
        .execute_gql(
            "MERGE (n:GqlMergeNode {elementKey: 'n'}) ON CREATE SET n.status = 'created-again' ON MATCH SET n.status = 'matched' RETURN id(n), n.status",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(gql_u64_column(&matched, 0), vec![created_id]);
    assert_eq!(matched.rows[0].values[1], GqlValue::String("matched".to_string()));
    let stats = matched.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 0);
    assert_eq!(stats.nodes_updated, 1);

    insert_query_node(
        &engine,
        "GqlMergeCounter",
        "n",
        &[("count", PropValue::Int(1))],
        1.0,
    );
    let incremented = engine
        .execute_gql(
            "MERGE (n:GqlMergeCounter {elementKey: 'n'}) ON MATCH SET n.count = n.count + 1 RETURN n.count",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(incremented.rows[0].values[0], GqlValue::Int(2));
    let stored_counter = engine
        .get_node_by_key("GqlMergeCounter", "n")
        .unwrap()
        .unwrap();
    assert_eq!(stored_counter.props.get("count"), Some(&PropValue::Int(2)));

    insert_query_node(
        &engine,
        "GqlMergeSource",
        "a",
        &[
            ("target", PropValue::String("dup".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlMergeSource",
        "b",
        &[
            ("target", PropValue::String("dup".to_string())),
            ("rank", PropValue::Int(2)),
        ],
        1.0,
    );
    let duplicate = engine
        .execute_gql(
            "MATCH (s:GqlMergeSource) MERGE (n:GqlMergeDupNode {elementKey: s.target}) \
             ON CREATE SET n.status = 'created', n.rank = s.rank \
             ON MATCH SET n.status = 'matched', n.rank = s.rank \
             RETURN n.status, n.rank ORDER BY s.rank",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(duplicate.rows.len(), 2);
    let stats = duplicate.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.mutation_rows, 2);
    assert_eq!(stats.duplicate_targets, 3);
    assert!(stats.db_hits >= 1);
    let stored = engine
        .get_node_by_key("GqlMergeDupNode", "dup")
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.props.get("status"),
        Some(&PropValue::String("matched".to_string()))
    );
    assert_eq!(stored.props.get("rank"), Some(&PropValue::Int(2)));

    let distinct = engine
        .execute_gql(
            "MATCH (s:GqlMergeSource) MERGE (n:GqlMergeDistinctNode {elementKey: s.target}) RETURN DISTINCT n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(distinct.rows.len(), 1);
    match &distinct.rows[0].values[0] {
        GqlValue::Node(node) => assert_eq!(node.key.as_deref(), Some("dup")),
        other => panic!("expected distinct MERGE node return, got {other:?}"),
    }
    let stats = distinct.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.mutation_rows, 1);

    let local_counter = engine
        .execute_gql(
            "MATCH (s:GqlMergeSource) WITH s, s.rank AS delta \
             MERGE (n:GqlMergeLocalCounter {elementKey: s.target}) \
             ON CREATE SET n.count = delta \
             ON MATCH SET n.count = n.count + delta \
             RETURN n.count ORDER BY s.rank",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(local_counter.rows.len(), 2);
    let stored_counter = engine
        .get_node_by_key("GqlMergeLocalCounter", "dup")
        .unwrap()
        .unwrap();
    assert_eq!(stored_counter.props.get("count"), Some(&PropValue::Int(3)));

    let empty_key = engine
        .execute_gql(
            "MERGE (n:GqlMergeEmptyKey {elementKey: ''})",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(empty_key, EngineError::InvalidOperation(message) if message.contains("non-empty string"))
    );

    let bad_action = engine
        .execute_gql(
            "MERGE (n:GqlMergeBadAction {elementKey: 'n'}) ON CREATE SET n.bad = $bad",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(bad_action, EngineError::InvalidOperation(message) if message.contains("finite")));
    assert!(engine
        .get_node_by_key("GqlMergeBadAction", "n")
        .unwrap()
        .is_none());

    let bad_local_metadata = engine
        .execute_gql(
            "MERGE (n:GqlMergeBadMetadata {elementKey: 'n'}) ON CREATE SET n.source_id = id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        bad_local_metadata,
        EngineError::GqlSemantic { .. }
    ));
    assert!(engine
        .get_node_by_key("GqlMergeBadMetadata", "n")
        .unwrap()
        .is_none());

    let bad_match_metadata = engine
        .execute_gql(
            "MERGE (n:GqlMergeBadMatchMetadata {elementKey: 'n'}) ON MATCH SET n.source_id = id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(bad_match_metadata, EngineError::GqlSemantic { .. }));
    assert!(engine
        .get_node_by_key("GqlMergeBadMatchMetadata", "n")
        .unwrap()
        .is_none());
}

#[test]
fn gql_merge_node_caps_explain_indexes_and_reopen_preserve_atomicity() {
    let (dir, engine) = query_test_engine();
    let db_path = dir.path().join("db");
    for key in ["a", "b"] {
        insert_query_node(&engine, "GqlMergeCapSource", key, &[], 1.0);
    }
    let cap = engine
        .execute_gql(
            "MATCH (s:GqlMergeCapSource) MERGE (n:GqlMergeCapTarget {elementKey: elementKey(s)})",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_mutation_ops: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(cap, EngineError::InvalidOperation(message) if message.contains("max_mutation_ops")));
    for key in ["a", "b"] {
        assert!(engine
            .get_node_by_key("GqlMergeCapTarget", key)
            .unwrap()
            .is_none());
    }

    let wal_path = wal_generation_path(&db_path, 0);
    let before_wal_len = std::fs::metadata(&wal_path).map(|metadata| metadata.len()).unwrap_or(0);
    let explain = engine
        .explain_gql(
            "MERGE (n:GqlMergeExplain {elementKey: 'n'}) ON CREATE SET n.status = 'planned' RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(explain
        .mutation
        .as_ref()
        .is_some_and(|mutation| mutation.uses_transaction_snapshot));
    let after_wal_len = std::fs::metadata(&wal_path).map(|metadata| metadata.len()).unwrap_or(0);
    assert_eq!(after_wal_len, before_wal_len);
    assert_eq!(engine.get_node_label_id("GqlMergeExplain").unwrap(), None);
    assert!(engine
        .get_node_by_key("GqlMergeExplain", "n")
        .unwrap()
        .is_none());

    engine
        .ensure_node_property_index("GqlMergeIndexed", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    let inserted = execute_gql_ok(
        &engine,
        "MERGE (n:GqlMergeIndexed {elementKey: 'n'}) ON CREATE SET n.status = 'ready' RETURN id(n)",
    );
    let node_id = gql_u64_column(&inserted, 0)[0];
    execute_gql_ok(
        &engine,
        "MERGE (n:GqlMergeIndexed {elementKey: 'n'}) ON MATCH SET n.status = 'updated' RETURN n",
    );
    let updated = execute_gql_ok(
        &engine,
        "MATCH (n:GqlMergeIndexed {status: 'updated'}) RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&updated, 0), vec![node_id]);
    let ready = execute_gql_ok(
        &engine,
        "MATCH (n:GqlMergeIndexed {status: 'ready'}) RETURN id(n)",
    );
    assert!(ready.rows.is_empty());

    engine.flush().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let reopened_read = execute_gql_ok(
        &reopened,
        "MATCH (n:GqlMergeIndexed {status: 'updated'}) RETURN id(n)",
    );
    assert_eq!(gql_u64_column(&reopened_read, 0), vec![node_id]);
    reopened.close().unwrap();
}

#[test]
fn gql_merge_relationship_creates_matches_duplicates_and_skips_null_endpoints() {
    let (_dir, engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    });
    let a = insert_query_node(&engine, "GqlMergeRelNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlMergeRelNode", "b", &[], 1.0);

    let created = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL]->(b) ON CREATE SET r.status = 'created' RETURN id(r), r.status",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let edge_id = gql_u64_column(&created, 0)[0];
    assert_eq!(created.rows[0].values[1], GqlValue::String("created".to_string()));
    assert_eq!(created.mutation_stats.as_ref().unwrap().edges_created, 1);

    let matched = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL]->(b) ON MATCH SET r.status = 'matched' RETURN id(r), r.status",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(gql_u64_column(&matched, 0), vec![edge_id]);
    assert_eq!(matched.rows[0].values[1], GqlValue::String("matched".to_string()));
    assert_eq!(matched.mutation_stats.as_ref().unwrap().edges_updated, 1);

    let incremented = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL]->(b) ON MATCH SET r.visits = coalesce(r.visits, 0) + 1 RETURN r.visits",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(incremented.rows[0].values[0], GqlValue::Int(1));

    insert_query_node(&engine, "GqlMergeRelSource", "a", &[], 1.0);
    insert_query_node(&engine, "GqlMergeRelSource", "b", &[], 1.0);
    let duplicate = engine
        .execute_gql(
            "MATCH (s:GqlMergeRelSource) MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL_DUP]->(b) ON CREATE SET r.status = 'created' ON MATCH SET r.status = 'matched' \
             RETURN r.status ORDER BY elementKey(s)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(duplicate.rows.len(), 2);
    let stats = duplicate.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.edges_created, 1);
    assert_eq!(stats.mutation_rows, 2);
    assert_eq!(stats.duplicate_targets, 1);
    assert!(stats.db_hits >= 1);
    let dup_edges = engine
        .query_edges(&EdgeQuery {
            from_ids: vec![a],
            to_ids: vec![b],
            label: Some("Gql_MERGE_REL_DUP".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(dup_edges.edges.len(), 1);
    assert_eq!(
        dup_edges.edges[0].props.get("status"),
        Some(&PropValue::String("matched".to_string()))
    );

    let duplicate_counter = engine
        .execute_gql(
            "MATCH (s:GqlMergeRelSource) MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL_COUNT]->(b) ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1 \
             RETURN r.count ORDER BY elementKey(s)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(duplicate_counter.rows.len(), 2);
    let counted_edges = engine
        .query_edges(&EdgeQuery {
            from_ids: vec![a],
            to_ids: vec![b],
            label: Some("Gql_MERGE_REL_COUNT".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(counted_edges.edges.len(), 1);
    assert_eq!(
        counted_edges.edges[0].props.get("count"),
        Some(&PropValue::Int(2))
    );

    let skipped = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' OPTIONAL MATCH (a)-[:Gql_MISSING_REL]->(b) \
             MERGE (a)-[r:Gql_MERGE_NULL]->(b) RETURN r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(skipped.rows.len(), 1);
    assert_eq!(skipped.rows[0].values[0], GqlValue::Null);
    let stats = skipped.mutation_stats.as_ref().unwrap();
    assert_eq!(stats.skipped_null_targets, 1);
    assert_eq!(stats.edges_created, 0);

    let bad_local_metadata = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL_BAD_META]->(b) ON CREATE SET r.source_id = id(r)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        bad_local_metadata,
        EngineError::GqlSemantic { .. }
    ));
    let bad_edges = engine
        .query_edges(&EdgeQuery {
            from_ids: vec![a],
            to_ids: vec![b],
            label: Some("Gql_MERGE_REL_BAD_META".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert!(bad_edges.edges.is_empty());

    let bad_endpoint_metadata = engine
        .execute_gql(
            "MATCH (a:GqlMergeRelNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeRelNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_REL_BAD_FROM]->(b) ON MATCH SET r.source_from = id(startNode(r))",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        bad_endpoint_metadata,
        EngineError::GqlSemantic { .. }
    ));
    let bad_from_edges = engine
        .query_edges(&EdgeQuery {
            from_ids: vec![a],
            to_ids: vec![b],
            label: Some("Gql_MERGE_REL_BAD_FROM".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert!(bad_from_edges.edges.is_empty());
}

#[test]
fn gql_merge_relationship_rejects_without_edge_uniqueness() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlMergeNoUnique", "a", &[], 1.0);
    insert_query_node(&engine, "GqlMergeNoUnique", "b", &[], 1.0);
    let err = engine
        .execute_gql(
            "MATCH (a:GqlMergeNoUnique) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeNoUnique) WHERE elementKey(b) = 'b' MERGE (a)-[r:Gql_NO_UNIQUE]->(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(message) if message.contains("edge_uniqueness=true")));
}

#[test]
fn gql_merge_commit_conflicts_for_node_keys_and_edge_triples() {
    let (_dir, engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    });

    let worker = DatabaseEngine {
        runtime: std::sync::Arc::clone(&engine.runtime),
    };
    let (ready_rx, release_tx) = engine.set_gql_mutation_before_commit_pause();
    let node_handle = std::thread::spawn(move || {
        worker.execute_gql(
            "MERGE (n:GqlMergeNodeConflict {elementKey: 'n'}) ON CREATE SET n.status = 'worker'",
            &GqlParams::new(),
            &gql_opts(),
        )
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("node MERGE did not pause before commit");
    engine
        .upsert_node(
            "GqlMergeNodeConflict",
            "n",
            UpsertNodeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("outside".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    release_tx.send(()).unwrap();
    let node_err = node_handle.join().unwrap().unwrap_err();
    assert!(matches!(node_err, EngineError::TxnConflict { .. }));
    let stored = engine
        .get_node_by_key("GqlMergeNodeConflict", "n")
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.props.get("status"),
        Some(&PropValue::String("outside".to_string()))
    );

    let a = insert_query_node(&engine, "GqlMergeEdgeConflictNode", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlMergeEdgeConflictNode", "b", &[], 1.0);
    let worker = DatabaseEngine {
        runtime: std::sync::Arc::clone(&engine.runtime),
    };
    let (ready_rx, release_tx) = engine.set_gql_mutation_before_commit_pause();
    let edge_handle = std::thread::spawn(move || {
        worker.execute_gql(
            "MATCH (a:GqlMergeEdgeConflictNode) WHERE elementKey(a) = 'a' MATCH (b:GqlMergeEdgeConflictNode) WHERE elementKey(b) = 'b' \
             MERGE (a)-[r:Gql_MERGE_EDGE_CONFLICT]->(b) ON CREATE SET r.status = 'worker'",
            &GqlParams::new(),
            &gql_opts(),
        )
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("edge MERGE did not pause before commit");
    let outside = engine
        .upsert_edge(
            a,
            b,
            "Gql_MERGE_EDGE_CONFLICT",
            UpsertEdgeOptions {
                props: query_test_props(&[(
                    "status",
                    PropValue::String("outside".to_string()),
                )]),
                ..Default::default()
            },
        )
        .unwrap();
    release_tx.send(()).unwrap();
    let edge_err = edge_handle.join().unwrap().unwrap_err();
    assert!(matches!(edge_err, EngineError::TxnConflict { .. }));
    let edges = engine
        .query_edges(&EdgeQuery {
            from_ids: vec![a],
            to_ids: vec![b],
            label: Some("Gql_MERGE_EDGE_CONFLICT".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(edges.edges.len(), 1);
    assert_eq!(edges.edges[0].id, outside);
    assert_eq!(
        edges.edges[0].props.get("status"),
        Some(&PropValue::String("outside".to_string()))
    );
}

#[test]
fn gql_merge_read_prefix_pipelines_support_with_call_union_and_exists() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlMergePrefixSeed", "with", &[], 1.0);
    let with_prefix = engine
        .execute_gql(
            "MATCH (s:GqlMergePrefixSeed) WITH s MERGE (n:GqlMergeWithPrefix {elementKey: elementKey(s)}) RETURN elementKey(n)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&with_prefix, 0), vec!["with".to_string()]);

    insert_query_node(&engine, "GqlMergeExistsSeed", "exists", &[], 1.0);
    insert_query_node(&engine, "GqlMergeExistsMarker", "marker", &[], 1.0);
    let exists_prefix = engine
        .execute_gql(
            "MATCH (s:GqlMergeExistsSeed) WHERE EXISTS { MATCH (m:GqlMergeExistsMarker) RETURN m } \
             MERGE (n:GqlMergeExistsPrefix {elementKey: elementKey(s)}) RETURN elementKey(n)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&exists_prefix, 0), vec!["exists".to_string()]);

    insert_query_node(&engine, "GqlMergeCallA", "a", &[], 1.0);
    insert_query_node(&engine, "GqlMergeCallB", "b", &[], 1.0);
    let call_union_prefix = engine
        .execute_gql(
            "CALL { MATCH (x:GqlMergeCallA) RETURN elementKey(x) AS k UNION MATCH (x:GqlMergeCallB) RETURN elementKey(x) AS k } \
             MERGE (n:GqlMergeCallPrefix {elementKey: k}) RETURN elementKey(n) ORDER BY elementKey(n)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&call_union_prefix, 0),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[test]
fn gql_create_node_strict_duplicates_reject_before_write() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "gql-create-existing", &[], 1.0);

    let visible = engine
        .execute_gql(
            "CREATE (n:Person {elementKey: 'gql-create-existing', name: 'new'}) RETURN n",
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
            "CREATE (a:GqlCreateDup {elementKey: 'dup'}), (b:GqlCreateDup {elementKey: 'dup'}) RETURN a",
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
            "CREATE (n:GqlCreateInitialOnly {elementKey: 'final-key', name: 'new'}) SET n:GqlCreateFinalConflict RETURN n",
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
            "CREATE (a:GqlCreateFinalLeft {elementKey: 'final-dup'}), (b:GqlCreateFinalRight {elementKey: 'final-dup'}) SET a:GqlCreateFinalShared SET b:GqlCreateFinalShared RETURN a",
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
            "CREATE (n:GqlCreateRemovedOld {elementKey: 'final-free', name: 'new'}) SET n:GqlCreateFinalNew REMOVE n:GqlCreateRemovedOld RETURN id(n), labels(n)",
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
            "MATCH (s:GqlCreateSeed) CREATE (n:GqlCreateRollback {elementKey: s.target}) RETURN n",
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
            "CREATE (n:GqlPruneHiddenCreate {elementKey: 'hidden', name: 'new'}) RETURN n",
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
            "CREATE (n:GqlBadKey {elementKey: 42, name: 'bad'}) RETURN n",
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
            "CREATE (n:GqlBadWeight {elementKey: 'n', weight: 'heavy'}) RETURN n",
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
            "CREATE (n:GqlBadProp {elementKey: 'n', score: $bad}) RETURN n",
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
            "MATCH (a:Person) WHERE elementKey(a) = 'gql-create-edge-a' MATCH (b:Person) WHERE elementKey(b) = 'gql-create-edge-b' CREATE (a)-[r:Gql_CREATED {since: 2026, weight: 0.8, validFrom: 10, validTo: 20}]->(b) RETURN r, id(r), r.since",
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
            "CREATE (a:GqlChain {elementKey: 'a'})-[r:Gql_CHAIN {rank: 1}]->(b:GqlChain {elementKey: 'b'}) RETURN id(a), id(r), id(b)",
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
            "CREATE (a:GqlBadEdgeWindow {elementKey: 'a'})-[r:Gql_BAD_WINDOW {validTo: 0}]->(b:GqlBadEdgeWindow {elementKey: 'b'}) RETURN r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_valid_to, EngineError::InvalidOperation(message) if message.contains("validFrom < validTo"))
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
            "CREATE (a:GqlBadEdgeWindowMax {elementKey: 'a'})-[r:Gql_BAD_WINDOW_MAX {validFrom: 9223372036854775807}]->(b:GqlBadEdgeWindowMax {elementKey: 'b'}) RETURN r",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(bad_valid_from, EngineError::InvalidOperation(message) if message.contains("validFrom < validTo"))
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
            "CREATE (n:GqlReturnNodeMetadata {elementKey: 'n'}) RETURN createdAt(n)",
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
            "CREATE (a:GqlReturnEdgeMetadata {elementKey: 'a'})-[r:Gql_RETURN_META]->(b:GqlReturnEdgeMetadata {elementKey: 'b'}) RETURN updatedAt(r)",
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
            "MATCH (a:Person) WHERE elementKey(a) = 'gql-unique-a' MATCH (b:Person) WHERE elementKey(b) = 'gql-unique-b' CREATE (a)-[:Gql_UNIQUE]->(b)",
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
            "MATCH (a:Person) WHERE elementKey(a) = 'gql-parallel-a' MATCH (b:Person) WHERE elementKey(b) = 'gql-parallel-b' CREATE (a)-[:Gql_PARALLEL]->(b), (a)-[:Gql_PARALLEL]->(b)",
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
            "MATCH (s:GqlBatch) CREATE (n:GqlCapCreate {elementKey: elementKey(s)}) RETURN n",
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
            "MATCH (s:GqlBatch) CREATE (n:GqlCursorCapCreate {elementKey: elementKey(s)}) RETURN n",
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
            "MATCH (a:GqlOptionalRoot) WHERE elementKey(a) = 'root' OPTIONAL MATCH (a)-[r:Gql_MISSING]->(b) CREATE (b)-[:Gql_SKIP]->(c:GqlSkipped {elementKey: 'c'}) RETURN c",
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
            "MATCH (s:GqlCreateEarlyCapSource) CREATE (n:GqlCreateEarlyCap {elementKey: elementKey(s)})",
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
            "CREATE (n:GqlReturnSupportedOrder {elementKey: 'n'}) RETURN n ORDER BY elementKey(n)",
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
            "CREATE (n:GqlUnsupportedSet {elementKey: 'n'}) SET n.name = 'Ada'",
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
            "MATCH (n:GqlUnsupportedSet) WHERE elementKey(n) = 'n' DETACH DELETE n",
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
        .ensure_node_property_index("GqlSetIndexed", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
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
            "MATCH (n:GqlSetIndexed) WHERE elementKey(n) = 'n' SET n.status = 'new' RETURN n, id(n), n.status, weight(n)",
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
            "MATCH (a:GqlEdgeSetNode) WHERE elementKey(a) = 'a' MATCH (b:GqlEdgeSetNode) WHERE elementKey(b) = 'b' MATCH (a)-[r:Gql_SET_EDGE]->(b) \
             SET r.since = 2026 SET weight(r) = 2.5 SET validFrom(r) = 10 SET validTo(r) = 20 \
             RETURN id(r), r.since, weight(r), validFrom(r), validTo(r)",
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
            "MATCH (a:GqlParallelRplNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlParallelRplNode) WHERE elementKey(b) = 'b' \
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
            "MATCH (n:GqlMapMerge) WHERE elementKey(n) = 'n' SET n += $props RETURN n.keep, n.old, n.nested, weight(n)",
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
            "MATCH (n:GqlMapMerge) WHERE elementKey(n) = 'n' SET n += 1",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(non_map, EngineError::InvalidOperation(message) if message.contains("map")));
}

#[test]
fn gql_set_map_merge_formerly_reserved_keys_write_plain_properties() {
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
    let node_before = engine.get_node(a).unwrap().unwrap();

    let node_keys = [
        "id",
        "labels",
        "key",
        "created_at",
        "updated_at",
        "dense_vector",
        "sparse_vector",
    ];
    for key in node_keys {
        engine
            .execute_gql(
                "MATCH (n:GqlReservedMapMerge) WHERE elementKey(n) = 'a' SET n += $props",
                &GqlParams::from([(
                    "props".to_string(),
                    GqlParamValue::Map(BTreeMap::from([(
                        key.to_string(),
                        GqlParamValue::Int(1),
                    )])),
                )]),
                &gql_opts(),
            )
            .unwrap_or_else(|err| {
                panic!("SET += map with property key {key} should succeed, got {err:?}")
            });
    }
    let node = engine.get_node(a).unwrap().unwrap();
    // Every formerly reserved key is now a plain user property.
    for key in node_keys {
        assert_eq!(
            node.props.get(key),
            Some(&PropValue::Int(1)),
            "expected user property {key} written by SET += map"
        );
    }
    assert_eq!(
        node.props.get("status"),
        Some(&PropValue::String("old".to_string()))
    );
    // Metadata stays untouched by the property writes.
    assert_eq!(node.key, "a");
    assert_eq!(node.labels, node_before.labels);
    assert_eq!(node.weight, node_before.weight);
    assert_eq!(node.created_at, node_before.created_at);
    assert!(node.dense_vector.is_none());
    assert!(node.sparse_vector.is_none());

    // Dot reads see the property; functions still read metadata.
    let read = execute_gql_ok(
        &engine,
        "MATCH (n:GqlReservedMapMerge) WHERE elementKey(n) = 'a' \
         RETURN n.updated_at, updatedAt(n), n.key, elementKey(n)",
    );
    assert_eq!(read.rows[0].values[0], GqlValue::Int(1));
    assert!(matches!(read.rows[0].values[1], GqlValue::Int(ts) if ts == node.updated_at));
    assert_eq!(read.rows[0].values[2], GqlValue::Int(1));
    assert_eq!(read.rows[0].values[3], GqlValue::String("a".to_string()));

    let edge_before = engine.get_edge(edge_id).unwrap().unwrap();
    let edge_keys = ["id", "from", "to", "label", "type", "created_at", "updated_at"];
    for key in edge_keys {
        engine
            .execute_gql(
                "MATCH (a:GqlReservedMapMerge) WHERE elementKey(a) = 'a' \
                 MATCH (b:GqlReservedMapMerge) WHERE elementKey(b) = 'b' \
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
            .unwrap_or_else(|err| {
                panic!("SET += map with edge property key {key} should succeed, got {err:?}")
            });
    }
    let edge = engine.get_edge(edge_id).unwrap().unwrap();
    for key in edge_keys {
        assert_eq!(
            edge.props.get(key),
            Some(&PropValue::Int(1)),
            "expected user property {key} written by SET += map"
        );
    }
    assert_eq!(
        edge.props.get("status"),
        Some(&PropValue::String("old".to_string()))
    );
    // Edge identity and metadata stay untouched by the property writes.
    assert_eq!(edge.id, edge_id);
    assert_eq!(edge.from, a);
    assert_eq!(edge.to, b);
    assert_eq!(edge.label, "Gql_RESERVED_MERGE_EDGE");
    assert_eq!(edge.weight, edge_before.weight);
    assert_eq!(edge.valid_from, edge_before.valid_from);
    assert_eq!(edge.valid_to, edge_before.valid_to);
    assert_eq!(edge.created_at, edge_before.created_at);
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
            "MATCH (n:GqlRemove) WHERE elementKey(n) = 'n' REMOVE n.drop REMOVE n.missing REMOVE n:GqlRemoveExtra RETURN n.drop, labels(n)",
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
            "MATCH (n:GqlRemove) WHERE elementKey(n) = 'n' REMOVE n:GqlRemove",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(last_label, EngineError::InvalidOperation(message) if message.contains("last node label")));
    assert!(engine.get_node(node_id).unwrap().unwrap().labels.contains(&"GqlRemove".to_string()));

    let optional = engine
        .execute_gql(
            "MATCH (n:GqlRemove) WHERE elementKey(n) = 'n' OPTIONAL MATCH (n)-[r:Gql_REMOVE_MISSING]->(m) SET m.name = 'x' REMOVE m.missing",
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
            "MATCH (n:GqlDuplicateSet) WHERE elementKey(n) = 'n' SET n.name = 'first' SET n.name = 'second' RETURN n.name",
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
            "CREATE (n:GqlMixedCreate {elementKey: 'n', old: 'x'}) SET n.name = 'Ada' REMOVE n.old SET n:GqlMixedExtra RETURN n.name, n.old, labels(n)",
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
            "MATCH (n:GqlSetAtomic) WHERE elementKey(n) = 'n' SET n.status = 'new' SET n.bad = $bad",
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
            "MATCH (a:GqlSetAtomicEdgeNode) WHERE elementKey(a) = 'a' MATCH (b:GqlSetAtomicEdgeNode) WHERE elementKey(b) = 'b' MATCH (a)-[r:Gql_SET_ATOMIC_EDGE]->(b) SET validFrom(r) = 9223372036854775807",
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
    assert!(
        !engine
            .get_node_by_key("GqlSetCap", "a")
            .unwrap()
            .unwrap()
            .props
            .contains_key("flag")
    );
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
            "MATCH (n:GqlSetRevertCap) WHERE elementKey(n) = 'n' SET n.status = 'new' SET n.status = 'old'",
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
            "MATCH (n:GqlSetRevertCap) WHERE elementKey(n) = 'n' SET n.status = 'new'",
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
            "MATCH (n:GqlVectorSet) WHERE elementKey(n) = 'n' SET n:GqlVectorSetExtra SET n.status = 'ok'",
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
            "MATCH (a:GqlTransferLabel) WHERE elementKey(a) = 'shared' \
             MATCH (b:GqlTransferTarget) WHERE elementKey(b) = 'shared' \
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
            "MATCH (n:GqlConflictCandidate) WHERE elementKey(n) = 'dup' SET n:GqlConflictHeld",
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
            "MATCH (a:GqlCycleLeft) WHERE elementKey(a) = 'shared' \
             MATCH (b:GqlCycleRight) WHERE elementKey(b) = 'shared' \
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
            "MATCH (n:GqlNoopReturn) WHERE elementKey(n) = 'n' CREATE (c:GqlNoopReturnCreated {elementKey: 'c'}) SET n.missing = null RETURN n",
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
            "MATCH (n:GqlReturnOptions) WHERE elementKey(n) = 'n' SET n.status = 'new' RETURN n",
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
            "MATCH (n:GqlReturnOptions) WHERE elementKey(n) = 'n' SET n.status = 'newer' RETURN n",
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
            "MATCH (n:GqlReturnOptions) WHERE elementKey(n) = 'n' SET n.status = 'compact' RETURN n.status",
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
         RETURN elementKey(b) ORDER BY a.status, type(r)"
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
            "CREATE (n:GqlMutationProfileCreate {elementKey: 'n'})",
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
            "MATCH (s:GqlCreateReturnOpsSeed) CREATE (n:GqlCreateReturnOps {elementKey: elementKey(s), rank: s.rank}) RETURN elementKey(n) ORDER BY n.rank DESC SKIP 1 LIMIT 1",
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
            "MATCH (n:GqlSetReturnOps) SET n.touched = true RETURN elementKey(n) ORDER BY n.rank, id(n) SKIP 1 LIMIT 2",
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
             RETURN elementKey(n) ORDER BY n.rank DESC LIMIT 1",
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
            "MATCH (n:GqlSetReturnOps) SET n.limit_zero = true RETURN elementKey(n) ORDER BY n.rank LIMIT 0",
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
            "MATCH (n:GqlRemoveReturnOps) REMOVE n.drop RETURN elementKey(n) ORDER BY n.rank DESC LIMIT 2",
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
            "MATCH (n:GqlReturnCapRows) SET n.status = 'ordered' RETURN elementKey(n) ORDER BY elementKey(n)",
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
            "MATCH (n:GqlReturnCapRows) SET n.status = 'bad-order' RETURN elementKey(n) ORDER BY n",
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
fn gql_mutation_return_distinct_caps_are_precommit_atomic() {
    let (_dir, engine) = query_test_engine();
    for key in ["a", "b"] {
        insert_query_node(&engine, "GqlReturnDistinctCapSeed", key, &[], 1.0);
    }
    let options = GqlExecutionOptions {
        allow_full_scan: true,
        max_groups: 1,
        ..gql_opts()
    };

    let cap_err = engine
        .execute_gql(
            "MATCH (s:GqlReturnDistinctCapSeed) \
             CREATE (n:GqlReturnDistinctCap {elementKey: elementKey(s)}) \
             RETURN DISTINCT elementKey(n) AS key",
            &GqlParams::new(),
            &options,
        )
        .unwrap_err();
    assert!(
        cap_err.to_string().contains("max_groups"),
        "unexpected error: {cap_err:?}"
    );
    for key in ["a", "b"] {
        assert!(engine
            .get_node_by_key("GqlReturnDistinctCap", key)
            .unwrap()
            .is_none());
    }

    let same = engine
        .execute_gql(
            "MATCH (s:GqlReturnDistinctCapSeed) \
             CREATE (n:GqlReturnDistinctSame {elementKey: elementKey(s)}) \
             RETURN DISTINCT 'same' AS key",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(same.rows.len(), 1);
    assert_eq!(same.rows[0].values[0], GqlValue::String("same".to_string()));
    for key in ["a", "b"] {
        assert!(engine
            .get_node_by_key("GqlReturnDistinctSame", key)
            .unwrap()
            .is_some());
    }

    for key in ["left", "right"] {
        insert_query_node(
            &engine,
            "GqlReturnDistinctGraphSeed",
            key,
            &[("target", PropValue::String("shared".to_string()))],
            1.0,
        );
    }
    let nested_graph = engine
        .execute_gql(
            "MATCH (s:GqlReturnDistinctGraphSeed) \
             MERGE (n:GqlReturnDistinctGraph {elementKey: s.target}) \
             RETURN DISTINCT [n] AS bucket",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(nested_graph.rows.len(), 1);
    match &nested_graph.rows[0].values[0] {
        GqlValue::List(values) => assert_eq!(values.len(), 1),
        other => panic!("expected nested graph list, got {other:?}"),
    }
}

#[test]
fn gql_mutation_return_distinct_rejects_commit_assigned_metadata_before_write() {
    let (_dir, engine) = query_test_engine();
    let err = engine
        .execute_gql(
            "CREATE (n:GqlReturnDistinctMetadata {elementKey: 'n'}) RETURN DISTINCT id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::GqlSemantic { .. }),
        "unexpected error: {err:?}"
    );
    assert!(engine
        .get_node_by_key("GqlReturnDistinctMetadata", "n")
        .unwrap()
        .is_none());
}

#[test]
fn gql_mutation_return_distinct_rejects_volatile_updated_at_before_write() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(
        &engine,
        "GqlReturnDistinctUpdatedAt",
        "n",
        &[("status", PropValue::String("old".to_string()))],
        1.0,
    );

    let err = engine
        .execute_gql(
            "MATCH (n:GqlReturnDistinctUpdatedAt) WHERE elementKey(n) = 'n' \
             SET n.status = 'new' RETURN DISTINCT updatedAt(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("RETURN DISTINCT"),
        "unexpected error: {err:?}"
    );
    assert_eq!(
        engine
            .get_node(node)
            .unwrap()
            .unwrap()
            .props
            .get("status"),
        Some(&PropValue::String("old".to_string()))
    );
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
            "MATCH (n:GqlReturnFinalValidation) WHERE elementKey(n) = 'rank' \
             SET n.rank = [1] RETURN elementKey(n) ORDER BY n.rank",
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
            "MATCH (n:GqlReturnFinalValidation) WHERE elementKey(n) = 'nested' \
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
            "MATCH (n:GqlReturnFinalValidation) WHERE elementKey(n) = 'rank' \
             SET n.status = 'metadata-id-bad' RETURN updatedAt(n).inner",
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
             CREATE (n:GqlReturnCreatedMeta {elementKey: elementKey(s)}) \
             RETURN elementKey(n) ORDER BY id(n) DESC SKIP 1 LIMIT 1",
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
                 RETURN elementKey(to) ORDER BY id(endNode(r)) DESC SKIP 1 LIMIT 1"
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
             SET n.status = 'new' RETURN elementKey(n) ORDER BY updatedAt(n)",
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
         RETURN elementKey(b) ORDER BY a.status"
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
         RETURN elementKey(b) ORDER BY r.status"
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
         WHERE id(a) = {a} SET b.status = 'start-node-conflict' RETURN startNode(p)"
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
         WHERE id(a) = {a} SET b.status = 'path-helper-no-conflict' RETURN nodeIds(p)"
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
         RETURN p, a.status, b.status, length(p), nodeIds(p), edgeIds(p), r.kind, \
                startNode(p), endNode(p), nodes(p), relationships(p)"
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
                 RETURN elementKey(startNode(p)) LIMIT 0"
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
            "MATCH (n:GqlReturnPrevalidate) WHERE elementKey(n) = 'n' SET n.status = 'new' RETURN $missing",
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
            "MATCH (n:GqlReturnPrevalidate) WHERE elementKey(n) = 'n' SET n.status = 'new' RETURN relationships(n)",
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
        .ensure_edge_property_index("Gql_EDGE_INDEX", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
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
            "MATCH (a:GqlEdgeIndexNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlEdgeIndexNode) WHERE elementKey(b) = 'b' \
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
            "MATCH (a:GqlEdgeIndexNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlEdgeIndexNode) WHERE elementKey(b) = 'b' \
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
        .ensure_edge_property_index("Gql_DELETE_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
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
            "MATCH (a:GqlDeleteEdgeNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlDeleteEdgeNode) WHERE elementKey(b) = 'b' \
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
            "MATCH (a:GqlDeleteRowsNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlDeleteRowsNode) WHERE elementKey(b) = 'b' \
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
            "MATCH (n:GqlDetachNode) WHERE elementKey(n) = 'hub' DETACH DELETE n",
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
            "MATCH (a:GqlDetachDedupeNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlDetachDedupeNode) WHERE elementKey(b) = 'b' DETACH DELETE a DETACH DELETE b",
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
            "MATCH (c:GqlDetachDedupeNode) WHERE elementKey(c) = 'c' \
             MATCH (d:GqlDetachDedupeNode) WHERE elementKey(d) = 'd' \
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
            "MATCH (n:GqlDeleteOptional) WHERE elementKey(n) = 'root' \
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
            "MATCH (a:GqlDeleteWinsNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlDeleteWinsNode) WHERE elementKey(b) = 'b' \
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
            "CREATE (a:GqlCreatedEdgeDelete {elementKey: 'a'})-[r:Gql_CREATED_EDGE_DELETE]->(b:GqlCreatedEdgeDelete {elementKey: 'b'}) DELETE r",
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
            "CREATE (a:GqlCreatedDetach {elementKey: 'a'})-[r:Gql_CREATED_DETACH]->(b:GqlCreatedDetach {elementKey: 'b'}) DETACH DELETE a",
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
            "MATCH (a:GqlDeleteCapNode) WHERE elementKey(a) = 'a' \
             MATCH (b:GqlDeleteCapNode) WHERE elementKey(b) = 'b' \
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
            "MATCH (n:GqlDeleteCapNode) WHERE elementKey(n) = 'a' DETACH DELETE n",
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
            "MATCH (n:GqlDetachCapHub) WHERE elementKey(n) = 'hub' DETACH DELETE n",
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
            "MATCH (n:GqlDetachCommitCapHub) WHERE elementKey(n) = 'hub' DETACH DELETE n",
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
            "MATCH (n:GqlDeleteReject) WHERE elementKey(n) = 'n' DELETE n",
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
            "MATCH (n:GqlDeleteReject) WHERE elementKey(n) = 'n' DETACH DELETE n RETURN n",
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
            "MATCH (n:GqlDeleteReject) WHERE elementKey(n) = 'n' DETACH DELETE n",
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
            "CREATE (n:GqlReopen {elementKey: 'persisted', name: 'stored'}) RETURN id(n)",
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
            "CREATE (a:GqlEdgeReopen {elementKey: 'a'})-[r:Gql_EDGE_REOPEN {since: 7}]->(b:GqlEdgeReopen {elementKey: 'b'}) RETURN id(a), id(r), id(b)",
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
            "MATCH (n:Person {elementKey: 'explain-mutation-ada'}) SET n.name = 'Ada' RETURN n.name ORDER BY n.name SKIP 0 LIMIT 1",
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
            "MATCH (n:Person {elementKey: 'explain-mutation-ada'}) SET n.name = 'Ada' \
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
        .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap()
        .index_id;
    wait_for_property_index_state(engine, employee_status, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, employee_status, SecondaryIndexState::Ready);

    let employee_score = engine
        .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap()
        .index_id;
    wait_for_property_index_state(engine, employee_score, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, employee_score, SecondaryIndexState::Ready);

    let works_role = engine
        .ensure_edge_property_index("WORKS_ON", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("role").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap()
        .index_id;
    wait_for_edge_property_index_state(engine, works_role, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(engine, works_role, SecondaryIndexState::Ready);

    let works_hours = engine
        .ensure_edge_property_index("WORKS_ON", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("hours").to_string() }], kind: SecondaryIndexKind::Range })
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
        &format!("MATCH ()-[r:LIKES]->() WHERE id(startNode(r)) = {from}.0 AND id(endNode(r)) IN [{to}.0] RETURN id(r)"),
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
        &format!("MATCH ()-[r]->() WHERE id(startNode(r)) = {from} AND id(r) = {keep} RETURN id(r)"),
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
         RETURN p, nodeIds(p), edgeIds(p), length(p) \
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
             RETURN edgeIds(p) ORDER BY p"
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
             RETURN startNode(p), endNode(p), nodes(p), relationships(p) ORDER BY p LIMIT 1"
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
fn gql_shortest_path_executes_native_stage_and_projects_path() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "ShortestStart", "gql-sp-a", &[], 1.0);
    let mid = insert_query_node(&engine, "ShortestMid", "gql-sp-mid", &[], 1.0);
    let b = insert_query_node(&engine, "ShortestEnd", "gql-sp-b", &[], 1.0);
    engine
        .upsert_edge(a, b, "GQL_SP_OTHER", UpsertEdgeOptions::default())
        .unwrap();
    let first = engine
        .upsert_edge(a, mid, "GQL_SP", UpsertEdgeOptions::default())
        .unwrap();
    let second = engine
        .upsert_edge(mid, b, "GQL_SP", UpsertEdgeOptions::default())
        .unwrap();

    let source = format!(
        "MATCH (a:ShortestStart) WHERE id(a) = {a} \
         WITH a \
         MATCH (b:ShortestEnd) WHERE id(b) = {b} \
         WITH a, b \
         MATCH p = shortestPath((a)-[:GQL_SP*1..5]->(b)) \
         RETURN p, nodeIds(p), edgeIds(p), length(p), nodes(p), relationships(p)"
    );
    let result = execute_gql_with_options(
        &engine,
        &source,
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    let path = gql_single_path(&values[0]);
    assert_eq!(path.node_ids, vec![a, mid, b]);
    assert_eq!(path.edge_ids, vec![first, second]);
    assert_eq!(
        values[1],
        GqlValue::List(vec![GqlValue::UInt(a), GqlValue::UInt(mid), GqlValue::UInt(b)])
    );
    assert_eq!(
        values[2],
        GqlValue::List(vec![GqlValue::UInt(first), GqlValue::UInt(second)])
    );
    assert_eq!(values[3], GqlValue::UInt(2));
    assert_eq!(
        values[4],
        GqlValue::List(vec![GqlValue::UInt(a), GqlValue::UInt(mid), GqlValue::UInt(b)])
    );
    assert_eq!(
        values[5],
        GqlValue::List(vec![GqlValue::UInt(first), GqlValue::UInt(second)])
    );
    let read = gql_read_explain(result.plan.as_ref().expect("include_plan should return plan"));
    assert!(read.projection.iter().any(|line| {
        line.contains("ShortestPath")
            && line.contains("algorithm=bidirectional_bfs")
            && line.contains("distinct_pair_count=1")
            && line.contains("emitted_path_count=1")
    }));
}

#[test]
fn gql_all_shortest_paths_direction_and_min_hops() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AllShortestStart", "gql-asp-a", &[], 1.0);
    let m1 = insert_query_node(&engine, "AllShortestMid", "gql-asp-m1", &[], 1.0);
    let m2 = insert_query_node(&engine, "AllShortestMid", "gql-asp-m2", &[], 1.0);
    let b = insert_query_node(&engine, "AllShortestEnd", "gql-asp-b", &[], 1.0);
    let am1 = engine
        .upsert_edge(a, m1, "GQL_ASP", UpsertEdgeOptions::default())
        .unwrap();
    let m1b = engine
        .upsert_edge(m1, b, "GQL_ASP", UpsertEdgeOptions::default())
        .unwrap();
    let am2 = engine
        .upsert_edge(a, m2, "GQL_ASP", UpsertEdgeOptions::default())
        .unwrap();
    let m2b = engine
        .upsert_edge(m2, b, "GQL_ASP", UpsertEdgeOptions::default())
        .unwrap();

    let all = execute_gql_with_options(
        &engine,
        &format!(
            "MATCH (a:AllShortestStart) WHERE id(a) = {a} \
             WITH a \
             MATCH (b:AllShortestEnd) WHERE id(b) = {b} \
             WITH a, b \
             MATCH p = allShortestPaths((a)-[:GQL_ASP*1..3]->(b)) \
             RETURN p"
        ),
        GqlExecutionOptions {
            include_plan: true,
            max_paths_per_start: 2,
            ..gql_opts()
        },
    );
    let mut paths = all
        .rows
        .iter()
        .map(|row| {
            let path = gql_single_path(&row.values[0]);
            (path.node_ids.clone(), path.edge_ids.clone())
        })
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(
        paths,
        vec![(vec![a, m1, b], vec![am1, m1b]), (vec![a, m2, b], vec![am2, m2b])]
    );
    let read = gql_read_explain(all.plan.as_ref().expect("include_plan should return plan"));
    assert!(read.projection.iter().any(|line| {
        line.contains("ShortestPath")
            && line.contains("max_paths=2")
            && line.contains("emitted_path_count=2")
    }));

    let row_cap_err = engine
        .execute_gql(
            &format!(
                "MATCH (a:AllShortestStart) WHERE id(a) = {a} \
                 WITH a \
                 MATCH (d:AllShortestMid) \
                 WITH a \
                 MATCH (b:AllShortestEnd) WHERE id(b) = {b} \
                 WITH a, b \
                 MATCH p = allShortestPaths((a)-[:GQL_ASP*1..3]->(b)) \
                 RETURN p"
            ),
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_pipeline_rows: 3,
                max_paths_per_start: 2,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        matches!(row_cap_err, EngineError::InvalidOperation(message) if message.contains("max_pipeline_rows"))
    );

    let incoming = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:AllShortestStart) WHERE id(a) = {a} \
             WITH a \
             MATCH (m:AllShortestMid) WHERE id(m) = {m1} \
             WITH a, m \
             MATCH p = shortestPath((m)<-[:GQL_ASP*1..1]-(a)) \
             RETURN p"
        ),
    );
    let incoming_path = gql_single_path(&incoming.rows[0].values[0]);
    assert_eq!(incoming_path.node_ids, vec![m1, a]);
    assert_eq!(incoming_path.edge_ids, vec![am1]);

    let undirected = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:AllShortestStart) WHERE id(a) = {a} \
             WITH a \
             MATCH (m:AllShortestMid) WHERE id(m) = {m1} \
             WITH a, m \
             MATCH p = shortestPath((m)-[:GQL_ASP*1..1]-(a)) \
             RETURN p"
        ),
    );
    let undirected_path = gql_single_path(&undirected.rows[0].values[0]);
    assert_eq!(undirected_path.node_ids, vec![m1, a]);
    assert_eq!(undirected_path.edge_ids, vec![am1]);

    let min_filtered = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:AllShortestStart) WHERE id(a) = {a} \
             WITH a \
             MATCH (b:AllShortestEnd) WHERE id(b) = {b} \
             WITH a, b \
             MATCH p = shortestPath((a)-[:GQL_ASP*3..3]->(b)) \
             RETURN p"
        ),
    );
    assert!(min_filtered.rows.is_empty());
}

#[test]
fn gql_shortest_path_optional_cache_and_pair_cap_semantics() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "ShortestCapStart", "gql-sp-cap-a", &[], 1.0);
    let a2 = insert_query_node(&engine, "ShortestCapStart", "gql-sp-cap-a2", &[], 1.0);
    let b = insert_query_node(&engine, "ShortestCapEnd", "gql-sp-cap-b", &[], 1.0);
    let duplicate_1 = insert_query_node(&engine, "ShortestDup", "gql-sp-dup-1", &[], 1.0);
    let duplicate_2 = insert_query_node(&engine, "ShortestDup", "gql-sp-dup-2", &[], 1.0);
    let edge = engine
        .upsert_edge(a, b, "GQL_SP_CACHE", UpsertEdgeOptions::default())
        .unwrap();
    assert_ne!(duplicate_1, duplicate_2);

    let required_miss = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:ShortestCapStart) WHERE id(a) = {a2} \
             WITH a \
             MATCH (b:ShortestCapEnd) WHERE id(b) = {b} \
             WITH a, b \
             MATCH p = shortestPath((a)-[:GQL_SP_CACHE*1..2]->(b)) \
             RETURN p"
        ),
    );
    assert!(required_miss.rows.is_empty());

    let optional_miss = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (a:ShortestCapStart) WHERE id(a) = {a2} \
             WITH a \
             MATCH (b:ShortestCapEnd) WHERE id(b) = {b} \
             WITH a, b \
             OPTIONAL MATCH p = shortestPath((a)-[:GQL_SP_CACHE*1..2]->(b)) \
             RETURN id(a), p"
        ),
    );
    assert_eq!(optional_miss.rows.len(), 1);
    assert_eq!(optional_miss.rows[0].values, vec![GqlValue::UInt(a2), GqlValue::Null]);

    let cached = execute_gql_with_options(
        &engine,
        &format!(
            "MATCH (a:ShortestCapStart) WHERE id(a) = {a} \
             WITH a \
             MATCH (d:ShortestDup) \
             WITH a \
             MATCH (b:ShortestCapEnd) WHERE id(b) = {b} \
             WITH a, b \
             MATCH p = shortestPath((a)-[:GQL_SP_CACHE*1..2]->(b)) \
             RETURN p"
        ),
        GqlExecutionOptions {
            allow_full_scan: true,
            include_plan: true,
            ..gql_opts()
        },
    );
    assert_eq!(cached.rows.len(), 2);
    for row in &cached.rows {
        let path = gql_single_path(&row.values[0]);
        assert_eq!(path.node_ids, vec![a, b]);
        assert_eq!(path.edge_ids, vec![edge]);
    }
    let read = gql_read_explain(cached.plan.as_ref().expect("include_plan should return plan"));
    assert!(read.projection.iter().any(|line| {
        line.contains("ShortestPath")
            && line.contains("distinct_pair_count=1")
            && line.contains("cache_hits=1")
    }));

    let cap_err = engine
        .execute_gql(
            &format!(
                "MATCH (a:ShortestCapStart) \
                 WITH a \
                 MATCH (b:ShortestCapEnd) WHERE id(b) = {b} \
                 WITH a, b \
                 MATCH p = shortestPath((a)-[:GQL_SP_CACHE*1..2]->(b)) \
                 RETURN p"
            ),
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_shortest_path_pairs: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        matches!(cap_err, EngineError::InvalidOperation(message) if message.contains("max_shortest_path_pairs"))
    );

    let hop_cap_err = engine
        .execute_gql(
            &format!(
                "MATCH (a:ShortestCapStart) WHERE id(a) = {a} \
                 WITH a \
                 MATCH (b:ShortestCapEnd) WHERE id(b) = {b} \
                 WITH a, b \
                 MATCH p = shortestPath((a)-[:GQL_SP_CACHE*1..2]->(b)) \
                 RETURN p"
            ),
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_path_hops: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        matches!(hop_cap_err, EngineError::InvalidOperation(message) if message.contains("max_path_hops"))
    );
}

#[test]
fn gql_shortest_path_survives_flush_reopen_and_compact() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = insert_query_node(&engine, "ShortestLifecycle", "gql-sp-life-a", &[], 1.0);
    let b = insert_query_node(&engine, "ShortestLifecycle", "gql-sp-life-b", &[], 1.0);
    let c = insert_query_node(&engine, "ShortestLifecycle", "gql-sp-life-c", &[], 1.0);
    let ab = engine
        .upsert_edge(a, b, "GQL_SP_LIFE", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    let bc = engine
        .upsert_edge(b, c, "GQL_SP_LIFE", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    engine.compact().unwrap();
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let result = execute_gql_ok(
        &reopened,
        &format!(
            "MATCH (a:ShortestLifecycle) WHERE id(a) = {a} \
             WITH a \
             MATCH (c:ShortestLifecycle) WHERE id(c) = {c} \
             WITH a, c \
             MATCH p = shortestPath((a)-[:GQL_SP_LIFE*1..3]->(c)) \
             RETURN p"
        ),
    );
    let path = gql_single_path(&result.rows[0].values[0]);
    assert_eq!(path.node_ids, vec![a, b, c]);
    assert_eq!(path.edge_ids, vec![ab, bc]);
    reopened.close().unwrap();
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
         RETURN p, nodeIds(p), edgeIds(p), length(p), id(s)"
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
             RETURN edgeIds(p) ORDER BY p",
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
         RETURN id(n) AS id, elementKey(n) AS key, labels(n) AS labels, weight(n) AS weight, \
                createdAt(n) AS created_at, updatedAt(n) AS updated_at, \
                $payload AS payload, $shape AS shape \
         ORDER BY n.score ASC, elementKey(n) ASC";
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
         RETURN id(r) AS id, id(startNode(r)) AS from, id(endNode(r)) AS to, type(r) AS label, \
                r.hours AS hours, weight(r) AS weight, createdAt(r) AS created_at, \
                updatedAt(r) AS updated_at, validFrom(r) AS valid_from, validTo(r) AS valid_to \
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
         WHERE id(startNode(r)) = $from AND id(endNode(r)) IN $targets RETURN id(r) ORDER BY id(r)",
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
         RETURN id(p), id(r), id(c) ORDER BY elementKey(p), id(r)";
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
fn gql_execution_rich_expressions_in_read_surfaces_use_graph_row_semantics() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "GqlRichRead",
        "ada",
        &[
            ("status", PropValue::String("active".to_string())),
            ("name", PropValue::String("Ada".to_string())),
            ("age", PropValue::Int(37)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlRichRead",
        "bob",
        &[
            ("status", PropValue::String("active".to_string())),
            ("name", PropValue::String("Bob".to_string())),
            ("age", PropValue::Int(29)),
        ],
        1.0,
    );

    let result = execute_gql_ok(
        &engine,
        "MATCH (n:GqlRichRead) \
         WHERE n.status = 'active' AND lower(n.name) STARTS WITH 'a' \
         RETURN n.name AS name, n.age + 5 AS adjusted, \
                CASE WHEN n.age > 30 THEN upper(n.name) ELSE 'young' END AS bucket \
         ORDER BY n.age / 2 DESC",
    );
    assert_eq!(result.columns, vec!["name", "adjusted", "bucket"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::String("Ada".to_string()));
    assert_eq!(result.rows[0].values[1], GqlValue::Int(42));
    assert_eq!(result.rows[0].values[2], GqlValue::String("ADA".to_string()));
}

#[test]
fn gql_execution_rich_residual_preserves_simple_pushdown_and_narrow_needs() {
    let (_dir, engine) = query_test_engine();
    engine
        .ensure_node_property_index("GqlRichPushdown", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    insert_query_node(
        &engine,
        "GqlRichPushdown",
        "ada",
        &[
            ("status", PropValue::String("active".to_string())),
            ("name", PropValue::String("Ada".to_string())),
        ],
        1.0,
    );

    let explain = engine
        .explain_gql(
            "MATCH (n:GqlRichPushdown) \
             WHERE n.status = 'active' AND lower(n.name) STARTS WITH 'a' \
             RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let explain = gql_read_explain(&explain);
    assert!(explain
        .pushed_down
        .iter()
        .any(|item| item.contains("n.status")));
    assert!(explain
        .residual
        .iter()
        .any(|item| item.contains("STARTS WITH")));

    let lowered = lowered_gql_for_projection_test(
        "MATCH (n:GqlRichPushdown) \
         WHERE n.status = 'active' AND lower(n.name) STARTS WITH 'a' \
         RETURN id(n)",
    );
    let alias_projection = gql_alias_projection_map(&lowered);
    let projection_alias = alias_projection.get("n").unwrap();
    let residual_projection = crate::gql::eval::build_runtime_projection_for_need_class(
        &lowered.residual_predicates,
        &lowered.semantic,
        &alias_projection,
        false,
        false,
        crate::row_projection::ProjectionNeedClass::Residual,
    )
    .unwrap();
    assert_node_need_props(
        &residual_projection.plan.needs.residual,
        projection_alias,
        &["name"],
    );
    assert_entity_needs_do_not_request_all_properties(&residual_projection.plan.needs.residual);
}

#[test]
fn gql_execution_rich_mutation_set_return_and_error_prevalidation() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(
        &engine,
        "GqlRichMutation",
        "n",
        &[
            ("name", PropValue::String(" Ada ".to_string())),
            ("score", PropValue::Int(40)),
        ],
        1.0,
    );

    let result = engine
        .execute_gql(
            "MATCH (n:GqlRichMutation) WHERE elementKey(n) = 'n' \
             SET n.score = n.score + 2 SET n.slug = lower(trim(n.name)) \
             RETURN n.score + 1 AS next_score, n.slug, \
                    CASE n.slug WHEN 'ada' THEN 'ok' ELSE 'bad' END AS status",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::Int(43));
    assert_eq!(result.rows[0].values[1], GqlValue::String("ada".to_string()));
    assert_eq!(result.rows[0].values[2], GqlValue::String("ok".to_string()));

    let err = engine
        .execute_gql(
            "MATCH (n:GqlRichMutation) WHERE elementKey(n) = 'n' SET n.score = n.score / 0 RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(message) if message.contains("division by zero")));
    let stored = engine.get_node(node).unwrap().unwrap();
    assert_eq!(stored.props.get("score"), Some(&PropValue::Int(42)));

    let direct_id = engine
        .execute_gql(
            "CREATE (n:GqlRichCreatedId {elementKey: 'ok'}) RETURN id(n) AS id",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(matches!(direct_id.rows[0].values[0], GqlValue::UInt(_)));

    let err = engine
        .execute_gql(
            "CREATE (n:GqlRichCreatedIdError {elementKey: 'bad'}) \
             RETURN 1 / (id(n) - id(n)) AS unsafe",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::GqlSemantic { message, .. } if message.contains("commit-assigned created alias metadata")));
    let committed = execute_gql_ok(
        &engine,
        "MATCH (n:GqlRichCreatedIdError) RETURN id(n)",
    );
    assert!(committed.rows.is_empty());

    let err = engine
        .execute_gql(
            "CREATE (n:GqlRichCreatedOrderIdError {elementKey: 'bad'}) \
             RETURN n ORDER BY 1 / (id(n) - id(n))",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::GqlSemantic { message, .. } if message.contains("commit-assigned created alias metadata")));
    let committed = execute_gql_ok(
        &engine,
        "MATCH (n:GqlRichCreatedOrderIdError) RETURN id(n)",
    );
    assert!(committed.rows.is_empty());

    let err = engine
        .execute_gql(
            "CREATE (n:GqlRichCoalesceNan {elementKey: 'bad'}) RETURN coalesce($bad, 1)",
            &GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::InvalidOperation(message)
            if message.contains("scalar function result must be finite")
    ));
    let committed = execute_gql_ok(
        &engine,
        "MATCH (n:GqlRichCoalesceNan) RETURN id(n)",
    );
    assert!(committed.rows.is_empty());
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
                n.optional AS opt, elementKey(n) AS x, $answer",
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
            "MATCH (n:Person) RETURN n.name AS x, elementKey(n) AS x ORDER BY x",
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
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap()
        .index_id;
    let edge_index = engine
        .ensure_edge_property_index("LIKES", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
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
        .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap()
        .index_id;
    let edge_index = engine
        .ensure_edge_property_index("LIKES", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
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
        "MATCH (n:Person) WHERE elementKey(n) = $key \
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
fn gql_unsupported_features_are_rejected_by_execution_api() {
    let (_dir, engine) = query_test_engine();
    let cases = [
        (
            "CREATE INDEX node_status FOR (n:User) ON (n.status)",
            "GQL index DDL",
            "INDEX",
        ),
        ("DROP INDEX node_status", "GQL index DDL", "INDEX"),
        (
            "MATCH (n:Person)-[*]->(m) RETURN n",
            "unbounded VLP",
            "*",
        ),
        (
            "MATCH (n:Person) RETURN n UNION CREATE (m:Person {elementKey: 'm'}) RETURN m",
            "write clauses",
            "CREATE",
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

fn expected_gql_index_columns<const N: usize>(columns: [&str; N]) -> Vec<String> {
    columns.into_iter().map(str::to_string).collect()
}

fn gql_uint(value: &GqlValue) -> u64 {
    match value {
        GqlValue::UInt(value) => *value,
        other => panic!("expected GQL UInt, got {other:?}"),
    }
}

fn gql_bool(value: &GqlValue) -> bool {
    match value {
        GqlValue::Bool(value) => *value,
        other => panic!("expected GQL bool, got {other:?}"),
    }
}

fn assert_gql_index_result<'a>(
    result: &'a GqlExecutionResult,
    operation: &str,
) -> &'a GqlIndexStats {
    assert_eq!(result.kind, GqlStatementKind::Index);
    assert!(result.mutation_stats.is_none());
    assert!(result.schema_stats.is_none());
    assert!(result.next_cursor.is_none());
    assert_eq!(result.stats.rows_returned, result.rows.len());
    assert_eq!(result.stats.rows_matched, 0);
    assert_eq!(result.stats.rows_after_filter, 0);
    assert_eq!(result.stats.intermediate_bindings, 0);
    assert_eq!(result.stats.db_hits, 0);
    assert!(result.stats.warnings.is_empty());
    let stats = result
        .index_stats
        .as_ref()
        .expect("index result should include index_stats");
    assert_eq!(stats.operation, operation);
    assert!(stats.warnings.is_empty());
    stats
}

fn gql_index_field_identity(value: &GqlValue) -> Vec<(String, String)> {
    gql_list(value)
        .iter()
        .map(|field| {
            let map = gql_map(field);
            let source = gql_str(&map["source"]).to_string();
            let name = match source.as_str() {
                "property" => gql_str(&map["key"]).to_string(),
                "metadata" => gql_str(&map["field"]).to_string(),
                other => panic!("unexpected index field source {other}"),
            };
            (source, name)
        })
        .collect()
}

fn assert_gql_index_fields(value: &GqlValue, expected: &[(&str, &str)]) {
    let expected = expected
        .iter()
        .map(|(source, name)| ((*source).to_string(), (*name).to_string()))
        .collect::<Vec<_>>();
    assert_eq!(gql_index_field_identity(value), expected);
}

fn assert_gql_index_field_list_flags(values: &[GqlValue], fields_index: usize, compound_index: usize, field_count_index: usize, expected: &[(&str, &str)]) {
    assert_gql_index_fields(&values[fields_index], expected);
    assert_eq!(gql_bool(&values[compound_index]), expected.len() > 1);
    assert_eq!(gql_uint(&values[field_count_index]), expected.len() as u64);
}

fn assert_create_property_index_row(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    prop_key: &str,
    kind: &str,
) -> u64 {
    assert_eq!(
        result.columns,
        expected_gql_index_columns([
            "operation",
            "target_kind",
            "label",
            "fields",
            "kind",
            "action",
            "state",
            "index_id",
            "last_error",
            "compound",
            "field_count",
        ])
    );
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    assert_eq!(values[0], GqlValue::String("create_property_index".to_string()));
    assert_eq!(values[1], GqlValue::String(target_kind.to_string()));
    assert_eq!(values[2], GqlValue::String(label.to_string()));
    assert_gql_index_field_list_flags(values, 3, 9, 10, &[("property", prop_key)]);
    assert_eq!(values[4], GqlValue::String(kind.to_string()));
    assert_eq!(values[5], GqlValue::String("ensured".to_string()));
    assert!(
        matches!(
            &values[6],
            GqlValue::String(state) if state == "building" || state == "ready" || state == "failed"
        ),
        "unexpected index state: {:?}",
        values[6]
    );
    assert_eq!(values[8], GqlValue::Null);
    gql_uint(&values[7])
}

fn assert_create_property_index_row_fields(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    fields: &[(&str, &str)],
    kind: &str,
) -> u64 {
    assert_eq!(
        result.columns,
        expected_gql_index_columns([
            "operation",
            "target_kind",
            "label",
            "fields",
            "kind",
            "action",
            "state",
            "index_id",
            "last_error",
            "compound",
            "field_count",
        ])
    );
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    assert_eq!(values[0], GqlValue::String("create_property_index".to_string()));
    assert_eq!(values[1], GqlValue::String(target_kind.to_string()));
    assert_eq!(values[2], GqlValue::String(label.to_string()));
    assert_gql_index_field_list_flags(values, 3, 9, 10, fields);
    assert_eq!(values[4], GqlValue::String(kind.to_string()));
    assert_eq!(values[5], GqlValue::String("ensured".to_string()));
    assert!(matches!(
        &values[6],
        GqlValue::String(state) if state == "building" || state == "ready" || state == "failed"
    ));
    assert_eq!(values[8], GqlValue::Null);
    gql_uint(&values[7])
}

fn assert_drop_property_index_row(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    prop_key: &str,
    kind: &str,
    action: &str,
) {
    assert_eq!(
        result.columns,
        expected_gql_index_columns([
            "operation",
            "target_kind",
            "label",
            "fields",
            "kind",
            "action",
            "compound",
            "field_count",
        ])
    );
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    assert_eq!(values[0], GqlValue::String("drop_property_index".to_string()));
    assert_eq!(values[1], GqlValue::String(target_kind.to_string()));
    assert_eq!(values[2], GqlValue::String(label.to_string()));
    assert_gql_index_field_list_flags(values, 3, 6, 7, &[("property", prop_key)]);
    assert_eq!(values[4], GqlValue::String(kind.to_string()));
    assert_eq!(values[5], GqlValue::String(action.to_string()));
}

fn assert_drop_property_index_row_fields(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    fields: &[(&str, &str)],
    kind: &str,
    action: &str,
) {
    assert_eq!(
        result.columns,
        expected_gql_index_columns([
            "operation",
            "target_kind",
            "label",
            "fields",
            "kind",
            "action",
            "compound",
            "field_count",
        ])
    );
    assert_eq!(result.rows.len(), 1);
    let values = &result.rows[0].values;
    assert_eq!(values[0], GqlValue::String("drop_property_index".to_string()));
    assert_eq!(values[1], GqlValue::String(target_kind.to_string()));
    assert_eq!(values[2], GqlValue::String(label.to_string()));
    assert_gql_index_field_list_flags(values, 3, 6, 7, fields);
    assert_eq!(values[4], GqlValue::String(kind.to_string()));
    assert_eq!(values[5], GqlValue::String(action.to_string()));
}

fn native_node_index(
    engine: &DatabaseEngine,
    label: &str,
    prop_key: &str,
    kind: SecondaryIndexKind,
) -> Option<NodePropertyIndexInfo> {
    native_node_index_fields(engine, label, &property_index_fields(prop_key), kind)
}

fn native_node_index_fields(
    engine: &DatabaseEngine,
    label: &str,
    fields: &[SecondaryIndexField],
    kind: SecondaryIndexKind,
) -> Option<NodePropertyIndexInfo> {
    engine
        .list_node_property_indexes()
        .unwrap()
        .into_iter()
        .find(|info| info.label == label && info.fields == fields && info.kind == kind)
}

fn native_edge_index(
    engine: &DatabaseEngine,
    label: &str,
    prop_key: &str,
    kind: SecondaryIndexKind,
) -> Option<EdgePropertyIndexInfo> {
    native_edge_index_fields(engine, label, &property_index_fields(prop_key), kind)
}

fn native_edge_index_fields(
    engine: &DatabaseEngine,
    label: &str,
    fields: &[SecondaryIndexField],
    kind: SecondaryIndexKind,
) -> Option<EdgePropertyIndexInfo> {
    engine
        .list_edge_property_indexes()
        .unwrap()
        .into_iter()
        .find(|info| info.label == label && info.fields == fields && info.kind == kind)
}

fn assert_index_cursor_error(err: EngineError) {
    match err {
        EngineError::InvalidCursor { message } => {
            assert_eq!(message, "GQL index statements do not accept cursors");
        }
        other => panic!("expected GQL index cursor error, got {other:?}"),
    }
}

fn assert_index_read_only_error(err: EngineError) {
    match err {
        EngineError::InvalidOperation(message) => {
            assert_eq!(
                message,
                "GQL index management is not allowed in ReadOnly mode"
            );
        }
        other => panic!("expected GQL index ReadOnly error, got {other:?}"),
    }
}

fn gql_index_explain_payload(explain: &GqlExecutionExplain) -> &GqlIndexExplain {
    assert_eq!(explain.kind, GqlStatementKind::Index);
    assert!(explain.read.is_none());
    assert!(explain.mutation.is_none());
    assert!(explain.schema.is_none());
    assert!(explain.warnings.is_empty());
    assert!(explain.notes.iter().any(|note| {
        note.contains("side-effect-free")
            && note.contains("does not create labels")
            && note.contains("write manifests")
            && note.contains("enqueue builds")
            && note.contains("drop declarations")
            && note.contains("inspect sidecars")
            && note.contains("scan graph records")
    }));
    explain
        .index
        .as_ref()
        .expect("index explain should be present")
}

fn gql_index_test_node_query(label: &str, filter: NodeFilterExpr) -> NodeQuery {
    NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![label.to_string()],
            mode: LabelMatchMode::All,
        }),
        filter: Some(filter),
        ..Default::default()
    }
}

fn gql_index_test_edge_query(label: &str, filter: EdgeFilterExpr) -> EdgeQuery {
    EdgeQuery {
        label: Some(label.to_string()),
        filter: Some(filter),
        ..Default::default()
    }
}

fn assert_show_property_index_row(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    prop_key: &str,
    kind: &str,
    index_id: u64,
) {
    assert!(
        result.rows.iter().any(|row| {
            gql_uint(&row.values[0]) == index_id
                && row.values[1] == GqlValue::String(target_kind.to_string())
                && row.values[2] == GqlValue::String(label.to_string())
                && gql_index_field_identity(&row.values[3])
                    == vec![("property".to_string(), prop_key.to_string())]
                && row.values[4] == GqlValue::String(kind.to_string())
                && row.values[6] == GqlValue::Null
                && !gql_bool(&row.values[7])
                && gql_uint(&row.values[8]) == 1
        }),
        "SHOW PROPERTY INDEXES did not include {target_kind} {label}.{prop_key} {kind} id {index_id}: {:?}",
        result.rows
    );
}

fn assert_show_property_index_row_fields(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    fields: &[(&str, &str)],
    kind: &str,
    index_id: u64,
) {
    assert!(
        result.rows.iter().any(|row| {
            gql_uint(&row.values[0]) == index_id
                && row.values[1] == GqlValue::String(target_kind.to_string())
                && row.values[2] == GqlValue::String(label.to_string())
                && gql_index_field_identity(&row.values[3])
                    == fields
                        .iter()
                        .map(|(source, name)| ((*source).to_string(), (*name).to_string()))
                        .collect::<Vec<_>>()
                && row.values[4] == GqlValue::String(kind.to_string())
                && row.values[6] == GqlValue::Null
                && gql_bool(&row.values[7]) == (fields.len() > 1)
                && gql_uint(&row.values[8]) == fields.len() as u64
        }),
        "SHOW PROPERTY INDEXES did not include {target_kind} {label} {fields:?} {kind} id {index_id}: {:?}",
        result.rows
    );
}

fn assert_show_property_index_absent(
    result: &GqlExecutionResult,
    target_kind: &str,
    label: &str,
    prop_key: &str,
    kind: &str,
) {
    assert!(
        result.rows.iter().all(|row| {
            !(row.values[1] == GqlValue::String(target_kind.to_string())
                && row.values[2] == GqlValue::String(label.to_string())
                && gql_index_field_identity(&row.values[3])
                    == vec![("property".to_string(), prop_key.to_string())]
                && row.values[4] == GqlValue::String(kind.to_string()))
        }),
        "SHOW PROPERTY INDEXES unexpectedly included {target_kind} {label}.{prop_key} {kind}: {:?}",
        result.rows
    );
}

#[derive(Debug, PartialEq, Eq)]
struct GqlIndexSideEffectSnapshot {
    node_label_tokens: std::collections::BTreeMap<String, u32>,
    edge_label_tokens: std::collections::BTreeMap<String, u32>,
    next_node_label_id: u32,
    next_edge_label_id: u32,
    next_secondary_index_id: u64,
    node_indexes: Vec<NodePropertyIndexInfo>,
    edge_indexes: Vec<EdgePropertyIndexInfo>,
    secondary_indexes: Vec<SecondaryIndexManifestEntry>,
    pending_followups: usize,
}

fn gql_index_side_effect_snapshot(engine: &DatabaseEngine) -> GqlIndexSideEffectSnapshot {
    let manifest = engine.manifest().unwrap();
    GqlIndexSideEffectSnapshot {
        node_label_tokens: manifest.node_label_tokens,
        edge_label_tokens: manifest.edge_label_tokens,
        next_node_label_id: manifest.next_node_label_id,
        next_edge_label_id: manifest.next_edge_label_id,
        next_secondary_index_id: manifest.next_secondary_index_id,
        node_indexes: engine.list_node_property_indexes().unwrap(),
        edge_indexes: engine.list_edge_property_indexes().unwrap(),
        secondary_indexes: manifest.secondary_indexes,
        pending_followups: engine.pending_secondary_index_followup_count_for_test(),
    }
}

fn assert_gql_index_no_side_effects(
    engine: &DatabaseEngine,
    before: &GqlIndexSideEffectSnapshot,
    context: &str,
) {
    assert_eq!(
        gql_index_side_effect_snapshot(engine),
        *before,
        "{context} mutated labels, property-index declarations, manifest entries, or followups"
    );
}

fn gql_index_manifest_json(db_path: &std::path::Path) -> serde_json::Value {
    let raw = std::fs::read_to_string(db_path.join("manifest.current")).unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn assert_json_object_keys(value: &serde_json::Value, expected: &[&str]) {
    let object = value.as_object().expect("expected JSON object");
    let mut actual = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

fn assert_gql_index_manifest_secondary_index_shape(db_path: &std::path::Path, expected_len: usize) {
    let manifest = gql_index_manifest_json(db_path);
    let secondary_indexes = manifest["secondary_indexes"]
        .as_array()
        .expect("secondary_indexes should be an array");
    assert_eq!(secondary_indexes.len(), expected_len);
    for entry in secondary_indexes {
        assert_json_object_keys(
            entry,
            &["index_id", "target", "kind", "state", "last_error"],
        );
        assert!(entry["index_id"].as_u64().is_some());
        assert!(matches!(
            entry["kind"].as_str(),
            Some("Equality") | Some("Range")
        ));
        assert!(matches!(
            entry["state"].as_str(),
            Some("Building") | Some("Ready") | Some("Failed")
        ));
        assert!(entry.get("last_error").is_some());

        let target = entry["target"].as_object().expect("target should be object");
        assert_eq!(target.len(), 1);
        let (target_kind, target_payload) = target.iter().next().unwrap();
        assert!(
            matches!(
                target_kind.as_str(),
                "NodeProperty" | "EdgeProperty" | "NodeFieldIndex" | "EdgeFieldIndex"
            ),
            "unexpected secondary-index target kind: {target_kind}"
        );
        match target_kind.as_str() {
            "NodeProperty" | "EdgeProperty" => {
                assert_json_object_keys(target_payload, &["label_id", "prop_key"]);
                assert!(target_payload["label_id"].as_u64().is_some());
                assert!(target_payload["prop_key"].as_str().is_some());
            }
            "NodeFieldIndex" | "EdgeFieldIndex" => {
                assert_json_object_keys(target_payload, &["fields", "label_id"]);
                assert!(target_payload["label_id"].as_u64().is_some());
                assert!(target_payload["fields"].as_array().is_some());
            }
            other => panic!("unexpected secondary-index target kind: {other}"),
        }
    }
}

#[test]
fn gql_index_create_node_rows_idempotence_and_native_visibility() {
    let (_dir, engine) = query_test_engine();
    let source = "CREATE PROPERTY INDEX FOR (n:GqlIndexNodeCreate) ON (n.role) KIND EQUALITY";
    assert_eq!(engine.get_node_label_id("GqlIndexNodeCreate").unwrap(), None);

    let result = execute_gql_ok(&engine, source);
    let stats = assert_gql_index_result(&result, "create_property_index");
    assert_eq!(stats.indexes_ensured, 1);
    assert_eq!(stats.indexes_dropped, 0);
    assert_eq!(stats.indexes_returned, 0);
    assert!(result.stats.elapsed_us.is_none());
    assert!(stats.elapsed_us.is_none());
    let index_id =
        assert_create_property_index_row(&result, "node", "GqlIndexNodeCreate", "role", "equality");
    assert!(engine
        .get_node_label_id("GqlIndexNodeCreate")
        .unwrap()
        .is_some());
    assert_eq!(
        native_node_index(
            &engine,
            "GqlIndexNodeCreate",
            "role",
            SecondaryIndexKind::Equality
        )
        .unwrap()
        .index_id,
        index_id
    );

    let rerun = execute_gql_ok(&engine, source);
    let rerun_id =
        assert_create_property_index_row(&rerun, "node", "GqlIndexNodeCreate", "role", "equality");
    assert_eq!(rerun_id, index_id);

    let range = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNodeCreate) ON (n.score) KIND RANGE",
    );
    assert_create_property_index_row(&range, "node", "GqlIndexNodeCreate", "score", "range");
    assert!(native_node_index(
        &engine,
        "GqlIndexNodeCreate",
        "score",
        SecondaryIndexKind::Range
    )
    .is_some());
}

#[test]
fn gql_index_create_edge_rows_native_visibility_and_missing_label_tokens() {
    let (_dir, engine) = query_test_engine();
    assert_eq!(engine.get_edge_label_id("GQL_INDEX_EDGE_CREATE").unwrap(), None);

    let equality = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_EDGE_CREATE]-() ON (r.role) KIND EQUALITY",
    );
    assert_create_property_index_row(
        &equality,
        "edge",
        "GQL_INDEX_EDGE_CREATE",
        "role",
        "equality",
    );
    assert!(engine
        .get_edge_label_id("GQL_INDEX_EDGE_CREATE")
        .unwrap()
        .is_some());
    assert!(native_edge_index(
        &engine,
        "GQL_INDEX_EDGE_CREATE",
        "role",
        SecondaryIndexKind::Equality
    )
    .is_some());

    let range = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_EDGE_CREATE]-() ON (r.score) KIND RANGE",
    );
    assert_create_property_index_row(&range, "edge", "GQL_INDEX_EDGE_CREATE", "score", "range");
    assert!(native_edge_index(
        &engine,
        "GQL_INDEX_EDGE_CREATE",
        "score",
        SecondaryIndexKind::Range
    )
    .is_some());

    assert_eq!(engine.get_node_label_id("GqlIndexMissingNodeLabel").unwrap(), None);
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexMissingNodeLabel) ON (n.status) KIND EQUALITY",
    );
    assert!(engine
        .get_node_label_id("GqlIndexMissingNodeLabel")
        .unwrap()
        .is_some());
}

#[test]
fn gql_index_create_drop_show_node_and_edge_compound_fields() {
    let (_dir, engine) = query_test_engine();

    let node_fields = vec![
        SecondaryIndexField::property("tenant_id"),
        SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
    ];
    let node_create = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNodeCompound) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    let node_index_id = assert_create_property_index_row_fields(
        &node_create,
        "node",
        "GqlIndexNodeCompound",
        &[("property", "tenant_id"), ("metadata", "updatedAt")],
        "range",
    );
    let native_node = native_node_index_fields(
        &engine,
        "GqlIndexNodeCompound",
        &node_fields,
        SecondaryIndexKind::Range,
    )
    .expect("GQL-created node compound declaration should be native-visible");
    assert_eq!(native_node.index_id, node_index_id);
    assert!(native_node.compound);

    let edge_fields = vec![
        SecondaryIndexField::property("status"),
        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
    ];
    let edge_create = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_EDGE_COMPOUND]-() ON (r.status, validTo(r)) KIND RANGE",
    );
    let edge_index_id = assert_create_property_index_row_fields(
        &edge_create,
        "edge",
        "GQL_INDEX_EDGE_COMPOUND",
        &[("property", "status"), ("metadata", "validTo")],
        "range",
    );
    let native_edge = native_edge_index_fields(
        &engine,
        "GQL_INDEX_EDGE_COMPOUND",
        &edge_fields,
        SecondaryIndexKind::Range,
    )
    .expect("GQL-created edge compound declaration should be native-visible");
    assert_eq!(native_edge.index_id, edge_index_id);
    assert!(native_edge.compound);

    let show = execute_gql_ok(&engine, "SHOW PROPERTY INDEXES");
    assert_show_property_index_row_fields(
        &show,
        "node",
        "GqlIndexNodeCompound",
        &[("property", "tenant_id"), ("metadata", "updatedAt")],
        "range",
        node_index_id,
    );
    assert_show_property_index_row_fields(
        &show,
        "edge",
        "GQL_INDEX_EDGE_COMPOUND",
        &[("property", "status"), ("metadata", "validTo")],
        "range",
        edge_index_id,
    );

    let node_drop = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexNodeCompound) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    assert_drop_property_index_row_fields(
        &node_drop,
        "node",
        "GqlIndexNodeCompound",
        &[("property", "tenant_id"), ("metadata", "updatedAt")],
        "range",
        "dropped",
    );
    assert!(
        native_node_index_fields(
            &engine,
            "GqlIndexNodeCompound",
            &node_fields,
            SecondaryIndexKind::Range
        )
        .is_none()
    );

    let edge_drop = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_EDGE_COMPOUND]-() ON (r.status, validTo(r)) KIND RANGE",
    );
    assert_drop_property_index_row_fields(
        &edge_drop,
        "edge",
        "GQL_INDEX_EDGE_COMPOUND",
        &[("property", "status"), ("metadata", "validTo")],
        "range",
        "dropped",
    );
    assert!(
        native_edge_index_fields(
            &engine,
            "GQL_INDEX_EDGE_COMPOUND",
            &edge_fields,
            SecondaryIndexKind::Range
        )
        .is_none()
    );
}

#[test]
fn gql_index_drop_existing_missing_and_unknown_labels() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexDropNode) ON (n.role) KIND EQUALITY",
    );
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_DROP_EDGE]-() ON (r.role) KIND EQUALITY",
    );

    let node_drop = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexDropNode) ON (n.role) KIND EQUALITY",
    );
    let stats = assert_gql_index_result(&node_drop, "drop_property_index");
    assert_eq!(stats.indexes_ensured, 0);
    assert_eq!(stats.indexes_dropped, 1);
    assert_eq!(stats.indexes_returned, 0);
    assert_drop_property_index_row(
        &node_drop,
        "node",
        "GqlIndexDropNode",
        "role",
        "equality",
        "dropped",
    );
    assert!(native_node_index(
        &engine,
        "GqlIndexDropNode",
        "role",
        SecondaryIndexKind::Equality
    )
    .is_none());

    let edge_drop = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_DROP_EDGE]-() ON (r.role) KIND EQUALITY",
    );
    assert_drop_property_index_row(
        &edge_drop,
        "edge",
        "GQL_INDEX_DROP_EDGE",
        "role",
        "equality",
        "dropped",
    );
    assert!(native_edge_index(
        &engine,
        "GQL_INDEX_DROP_EDGE",
        "role",
        SecondaryIndexKind::Equality
    )
    .is_none());

    let missing = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexDropNode) ON (n.missing) KIND EQUALITY",
    );
    let stats = assert_gql_index_result(&missing, "drop_property_index");
    assert_eq!(stats.indexes_dropped, 0);
    assert_drop_property_index_row(
        &missing,
        "node",
        "GqlIndexDropNode",
        "missing",
        "equality",
        "not_found",
    );

    assert_eq!(engine.get_node_label_id("GqlIndexDropUnknown").unwrap(), None);
    let unknown_node = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexDropUnknown) ON (n.role) KIND EQUALITY",
    );
    assert_drop_property_index_row(
        &unknown_node,
        "node",
        "GqlIndexDropUnknown",
        "role",
        "equality",
        "not_found",
    );
    assert_eq!(engine.get_node_label_id("GqlIndexDropUnknown").unwrap(), None);

    assert_eq!(engine.get_edge_label_id("GQL_INDEX_DROP_UNKNOWN").unwrap(), None);
    let unknown_edge = execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_DROP_UNKNOWN]-() ON (r.role) KIND EQUALITY",
    );
    assert_drop_property_index_row(
        &unknown_edge,
        "edge",
        "GQL_INDEX_DROP_UNKNOWN",
        "role",
        "equality",
        "not_found",
    );
    assert_eq!(engine.get_edge_label_id("GQL_INDEX_DROP_UNKNOWN").unwrap(), None);
}

#[test]
fn gql_index_show_rows_order_filters_empty_and_stats() {
    let (_dir, engine) = query_test_engine();
    let empty = execute_gql_ok(&engine, "SHOW PROPERTY INDEXES");
    let stats = assert_gql_index_result(&empty, "show_property_indexes");
    assert_eq!(
        empty.columns,
        expected_gql_index_columns([
            "index_id",
            "target_kind",
            "label",
            "fields",
            "kind",
            "state",
            "last_error",
            "compound",
            "field_count",
        ])
    );
    assert_eq!(empty.rows.len(), 0);
    assert_eq!(stats.indexes_returned, 0);

    engine
        .ensure_edge_property_index("GQL_INDEX_SHOW_B_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("z").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    engine
        .ensure_node_property_index("GqlIndexShowBNode", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("z").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    engine
        .ensure_node_property_index("GqlIndexShowANode", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("b").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    engine
        .ensure_node_property_index("GqlIndexShowANode", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("a").to_string() }], kind: SecondaryIndexKind::Range })
        .unwrap();
    engine
        .ensure_node_property_index("GqlIndexShowANode", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("a").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    engine
        .ensure_edge_property_index("GQL_INDEX_SHOW_A_EDGE", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("a").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();

    let all = execute_gql_ok(&engine, "SHOW PROPERTY INDEXES");
    let stats = assert_gql_index_result(&all, "show_property_indexes");
    assert_eq!(stats.indexes_ensured, 0);
    assert_eq!(stats.indexes_dropped, 0);
    assert_eq!(stats.indexes_returned, 6);
    assert_eq!(
        all.rows
            .iter()
            .map(|row| {
                (
                    gql_str(&row.values[1]).to_string(),
                    gql_str(&row.values[2]).to_string(),
                    gql_index_field_identity(&row.values[3]),
                    gql_str(&row.values[4]).to_string(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                "node".to_string(),
                "GqlIndexShowANode".to_string(),
                vec![("property".to_string(), "a".to_string())],
                "equality".to_string()
            ),
            (
                "node".to_string(),
                "GqlIndexShowANode".to_string(),
                vec![("property".to_string(), "a".to_string())],
                "range".to_string()
            ),
            (
                "node".to_string(),
                "GqlIndexShowANode".to_string(),
                vec![("property".to_string(), "b".to_string())],
                "range".to_string()
            ),
            (
                "node".to_string(),
                "GqlIndexShowBNode".to_string(),
                vec![("property".to_string(), "z".to_string())],
                "range".to_string()
            ),
            (
                "edge".to_string(),
                "GQL_INDEX_SHOW_A_EDGE".to_string(),
                vec![("property".to_string(), "a".to_string())],
                "equality".to_string()
            ),
            (
                "edge".to_string(),
                "GQL_INDEX_SHOW_B_EDGE".to_string(),
                vec![("property".to_string(), "z".to_string())],
                "range".to_string()
            ),
        ]
    );
    for row in &all.rows {
        assert!(
            matches!(&row.values[5], GqlValue::String(state) if state == "building" || state == "ready" || state == "failed")
        );
        gql_uint(&row.values[0]);
        assert_eq!(row.values[6], GqlValue::Null);
        assert_eq!(gql_uint(&row.values[8]), 1);
    }

    let node_only = execute_gql_ok(&engine, "SHOW NODE PROPERTY INDEXES");
    assert_eq!(node_only.rows.len(), 4);
    assert!(node_only
        .rows
        .iter()
        .all(|row| row.values[1] == GqlValue::String("node".to_string())));
    assert_eq!(
        assert_gql_index_result(&node_only, "show_node_property_indexes").indexes_returned,
        4
    );

    let edge_only = execute_gql_ok(&engine, "SHOW EDGE PROPERTY INDEX");
    assert_eq!(edge_only.rows.len(), 2);
    assert!(edge_only
        .rows
        .iter()
        .all(|row| row.values[1] == GqlValue::String("edge".to_string())));
    assert_eq!(
        assert_gql_index_result(&edge_only, "show_edge_property_indexes").indexes_returned,
        2
    );
}

#[test]
fn gql_index_show_includes_native_compound_and_metadata_declarations_sorted() {
    let (_dir, engine) = query_test_engine();
    let node_single = engine
        .ensure_node_property_index(
            "GqlIndexShowFields",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("a")]),
        )
        .unwrap();
    let node_compound = engine
        .ensure_node_property_index(
            "GqlIndexShowFields",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("a"),
                SecondaryIndexField::property("b"),
            ]),
        )
        .unwrap();
    let node_metadata = engine
        .ensure_node_property_index(
            "GqlIndexShowFields",
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::UpdatedAt,
            )]),
        )
        .unwrap();
    let edge_compound = engine
        .ensure_edge_property_index(
            "GQL_INDEX_SHOW_FIELDS",
            SecondaryIndexSpec::range(vec![
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
            ]),
        )
        .unwrap();

    let show = execute_gql_ok(&engine, "SHOW PROPERTY INDEXES");
    assert_eq!(
        show.rows
            .iter()
            .map(|row| {
                (
                    gql_uint(&row.values[0]),
                    gql_str(&row.values[1]).to_string(),
                    gql_str(&row.values[2]).to_string(),
                    gql_index_field_identity(&row.values[3]),
                    gql_str(&row.values[4]).to_string(),
                    gql_bool(&row.values[7]),
                    gql_uint(&row.values[8]),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                node_single.index_id,
                "node".to_string(),
                "GqlIndexShowFields".to_string(),
                vec![("property".to_string(), "a".to_string())],
                "equality".to_string(),
                false,
                1,
            ),
            (
                node_compound.index_id,
                "node".to_string(),
                "GqlIndexShowFields".to_string(),
                vec![
                    ("property".to_string(), "a".to_string()),
                    ("property".to_string(), "b".to_string()),
                ],
                "range".to_string(),
                true,
                2,
            ),
            (
                node_metadata.index_id,
                "node".to_string(),
                "GqlIndexShowFields".to_string(),
                vec![("metadata".to_string(), "updatedAt".to_string())],
                "equality".to_string(),
                false,
                1,
            ),
            (
                edge_compound.index_id,
                "edge".to_string(),
                "GQL_INDEX_SHOW_FIELDS".to_string(),
                vec![
                    ("property".to_string(), "status".to_string()),
                    ("metadata".to_string(), "validTo".to_string()),
                ],
                "range".to_string(),
                true,
                2,
            ),
        ]
    );
}

#[test]
fn gql_index_max_rows_cursor_readonly_profile_and_include_plan() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexCaps) ON (n.status) KIND EQUALITY",
    );

    let max_rows = engine
        .execute_gql(
            "SHOW PROPERTY INDEXES",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 0,
                ..gql_opts()
            },
        )
        .unwrap_err();
    match max_rows {
        EngineError::InvalidOperation(message) => assert_eq!(
            message,
            "GQL index SHOW result has 1 rows, exceeding max_rows=0; index SHOW does not support cursors"
        ),
        other => panic!("expected SHOW max_rows error, got {other:?}"),
    }
    let exactly_capped = execute_gql_with_options(
        &engine,
        "SHOW PROPERTY INDEXES",
        GqlExecutionOptions {
            max_rows: 1,
            ..gql_opts()
        },
    );
    assert_eq!(exactly_capped.rows.len(), 1);

    for source in [
        "CREATE PROPERTY INDEX FOR (n:GqlIndexCursor) ON (n.status) KIND EQUALITY",
        "DROP PROPERTY INDEX FOR (n:GqlIndexCursor) ON (n.status) KIND EQUALITY",
        "SHOW PROPERTY INDEXES",
    ] {
        let err = engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    cursor: Some("not-an-index-cursor".to_string()),
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert_index_cursor_error(err);
    }

    let read_only_create = engine
        .execute_gql(
            "CREATE PROPERTY INDEX FOR (n:GqlIndexReadOnly) ON (n.status) KIND EQUALITY",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert_index_read_only_error(read_only_create);
    let read_only_drop = engine
        .execute_gql(
            "DROP PROPERTY INDEX FOR (n:GqlIndexCaps) ON (n.status) KIND EQUALITY",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert_index_read_only_error(read_only_drop);
    let read_only_show = execute_gql_with_options(
        &engine,
        "SHOW PROPERTY INDEXES",
        GqlExecutionOptions {
            mode: GqlExecutionMode::ReadOnly,
            ..gql_opts()
        },
    );
    assert_eq!(read_only_show.rows.len(), 1);

    let profiled = execute_gql_with_options(
        &engine,
        "SHOW PROPERTY INDEXES",
        GqlExecutionOptions {
            profile: true,
            ..gql_opts()
        },
    );
    assert!(profiled.stats.elapsed_us.is_some());
    assert!(profiled.index_stats.as_ref().unwrap().elapsed_us.is_some());

    let profiled_create = execute_gql_with_options(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexProfileCreate) ON (n.status) KIND EQUALITY",
        GqlExecutionOptions {
            profile: true,
            ..gql_opts()
        },
    );
    assert!(profiled_create.stats.elapsed_us.is_some());
    assert!(profiled_create
        .index_stats
        .as_ref()
        .unwrap()
        .elapsed_us
        .is_some());

    let planned_source =
        "CREATE PROPERTY INDEX FOR (n:GqlIndexPlan) ON (n.status, updatedAt(n)) KIND RANGE";
    let planned = execute_gql_with_options(
        &engine,
        planned_source,
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    let plan = planned.plan.as_ref().expect("include_plan should return plan");
    let index_plan = gql_index_explain_payload(plan);
    assert_eq!(index_plan.operation, "create_property_index");
    assert_eq!(index_plan.targets[0].target_kind, "node");
    assert_eq!(index_plan.targets[0].label.as_deref(), Some("GqlIndexPlan"));
    assert_eq!(index_plan.targets[0].fields.len(), 2);
    assert_eq!(index_plan.targets[0].fields[0].source, "property");
    assert_eq!(index_plan.targets[0].fields[0].key.as_deref(), Some("status"));
    assert_eq!(index_plan.targets[0].fields[1].source, "metadata");
    assert_eq!(
        index_plan.targets[0].fields[1].field.as_deref(),
        Some("updatedAt")
    );
    assert_eq!(index_plan.targets[0].kind.as_deref(), Some("range"));
    assert!(index_plan.targets[0].compound);
    let explained = engine
        .explain_gql(planned_source, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_eq!(plan.index, explained.index);

    let planned_show = execute_gql_with_options(
        &engine,
        "SHOW NODE PROPERTY INDEXES",
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    let show_plan = planned_show
        .plan
        .as_ref()
        .expect("SHOW include_plan should return plan");
    let show_index = gql_index_explain_payload(show_plan);
    assert_eq!(show_index.operation, "show_node_property_indexes");
    assert!(show_index.side_effect_free);
    assert_eq!(show_index.targets[0].target_kind, "node");
    assert_eq!(show_index.targets[0].action.as_deref(), Some("show"));
}

#[test]
fn gql_index_explain_booleans_readonly_and_no_side_effects() {
    let (_dir, engine) = query_test_engine();
    let create_source = "CREATE PROPERTY INDEX FOR (n:GqlIndexExplainCreate) ON (n.status) KIND EQUALITY";
    assert_eq!(engine.get_node_label_id("GqlIndexExplainCreate").unwrap(), None);
    let create = engine
        .explain_gql(create_source, &GqlParams::new(), &gql_opts())
        .unwrap();
    let create = gql_index_explain_payload(&create);
    assert_eq!(create.operation, "create_property_index");
    assert_eq!(create.targets.len(), 1);
    assert_eq!(create.targets[0].target_kind, "node");
    assert_eq!(create.targets[0].label.as_deref(), Some("GqlIndexExplainCreate"));
    assert_eq!(create.targets[0].fields.len(), 1);
    assert_eq!(create.targets[0].fields[0].source, "property");
    assert_eq!(create.targets[0].fields[0].key.as_deref(), Some("status"));
    assert_eq!(create.targets[0].fields[0].field, None);
    assert_eq!(create.targets[0].kind.as_deref(), Some("equality"));
    assert_eq!(create.targets[0].action.as_deref(), Some("ensure"));
    assert!(!create.targets[0].compound);
    assert!(create.uses_core_write_queue);
    assert!(create.publishes_manifest);
    assert!(create.creates_labels);
    assert!(create.schedules_background_build);
    assert!(!create.drops_index_data_async);
    assert!(!create.side_effect_free);
    assert_eq!(engine.get_node_label_id("GqlIndexExplainCreate").unwrap(), None);

    let drop_source =
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_EXPLAIN_DROP]-() ON (r.status) KIND RANGE";
    let drop = engine
        .explain_gql(drop_source, &GqlParams::new(), &gql_opts())
        .unwrap();
    let drop = gql_index_explain_payload(&drop);
    assert_eq!(drop.operation, "drop_property_index");
    assert_eq!(drop.targets[0].target_kind, "edge");
    assert_eq!(drop.targets[0].label.as_deref(), Some("GQL_INDEX_EXPLAIN_DROP"));
    assert_eq!(drop.targets[0].fields.len(), 1);
    assert_eq!(drop.targets[0].fields[0].source, "property");
    assert_eq!(drop.targets[0].fields[0].key.as_deref(), Some("status"));
    assert_eq!(drop.targets[0].kind.as_deref(), Some("range"));
    assert_eq!(drop.targets[0].action.as_deref(), Some("drop"));
    assert!(!drop.targets[0].compound);
    assert!(drop.uses_core_write_queue);
    assert!(drop.publishes_manifest);
    assert!(!drop.creates_labels);
    assert!(!drop.schedules_background_build);
    assert!(drop.drops_index_data_async);
    assert!(!drop.side_effect_free);
    assert_eq!(engine.get_edge_label_id("GQL_INDEX_EXPLAIN_DROP").unwrap(), None);

    let show = engine
        .explain_gql(
            "SHOW EDGE PROPERTY INDEXES",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap();
    let show = gql_index_explain_payload(&show);
    assert_eq!(show.operation, "show_edge_property_indexes");
    assert_eq!(show.targets[0].target_kind, "edge");
    assert!(show.targets[0].label.is_none());
    assert!(show.targets[0].fields.is_empty());
    assert!(show.targets[0].kind.is_none());
    assert_eq!(show.targets[0].action.as_deref(), Some("show"));
    assert!(!show.targets[0].compound);
    assert!(!show.uses_core_write_queue);
    assert!(!show.publishes_manifest);
    assert!(!show.creates_labels);
    assert!(!show.schedules_background_build);
    assert!(!show.drops_index_data_async);
    assert!(show.side_effect_free);

    for source in [create_source, drop_source] {
        let err = engine
            .explain_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    mode: GqlExecutionMode::ReadOnly,
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert_index_read_only_error(err);
    }
    let cursor_err = engine
        .explain_gql(
            "SHOW PROPERTY INDEXES",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some("not-an-index-cursor".to_string()),
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert_index_cursor_error(cursor_err);
}

#[test]
fn gql_non_index_results_and_explains_keep_index_payload_absent() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "Person", "gql-index-non-index", &[], 1.0);

    let read = execute_gql_ok(
        &engine,
        "MATCH (n:Person) WHERE elementKey(n) = 'gql-index-non-index' RETURN id(n)",
    );
    assert_eq!(read.kind, GqlStatementKind::Query);
    assert!(read.index_stats.is_none());

    let mutation = execute_gql_ok(
        &engine,
        "CREATE (n:GqlIndexNonIndexMutation {elementKey: 'created'}) RETURN id(n)",
    );
    assert_eq!(mutation.kind, GqlStatementKind::Mutation);
    assert!(mutation.index_stats.is_none());

    let schema = execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE");
    assert_eq!(schema.kind, GqlStatementKind::Schema);
    assert!(schema.index_stats.is_none());

    let read_explain = engine
        .explain_gql(
            "MATCH (n:Person) WHERE elementKey(n) = 'gql-index-non-index' RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(read_explain.index.is_none());
    let mutation_explain = engine
        .explain_gql(
            "CREATE (n:GqlIndexNonIndexExplain {elementKey: 'created'}) RETURN id(n)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert!(mutation_explain.index.is_none());
    let schema_explain = engine
        .explain_gql("SHOW CURRENT GRAPH TYPE", &GqlParams::new(), &gql_opts())
        .unwrap();
    assert!(schema_explain.index.is_none());
}

#[test]
fn gql_index_show_catalog_smoke_does_not_scan_graph_records() {
    let (_dir, engine) = query_test_engine();
    for index in 0..100 {
        engine
            .ensure_node_property_index(&format!("GqlIndexSmokeNode{index:03}"), SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine
            .ensure_edge_property_index(&format!("GQL_INDEX_SMOKE_EDGE_{index:03}"), SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
    }

    engine.reset_query_execution_counters_for_test();
    let show = execute_gql_with_options(
        &engine,
        "SHOW PROPERTY INDEXES",
        GqlExecutionOptions {
            profile: true,
            ..gql_opts()
        },
    );
    assert_eq!(show.rows.len(), 200);
    assert_eq!(show.stats.rows_returned, 200);
    assert_eq!(show.stats.db_hits, 0);
    assert_eq!(
        assert_gql_index_result(&show, "show_property_indexes").indexes_returned,
        200
    );
    assert_eq!(
        engine.query_execution_counter_snapshot_for_test(),
        QueryExecutionCounterSnapshot::default()
    );
}

#[test]
fn gql_index_lifecycle_create_persists_across_reopen_and_show() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let node = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexReopenNode) ON (n.status) KIND EQUALITY",
    );
    let node_index_id =
        assert_create_property_index_row(&node, "node", "GqlIndexReopenNode", "status", "equality");
    let edge = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_REOPEN_EDGE]-() ON (r.score) KIND RANGE",
    );
    let edge_index_id =
        assert_create_property_index_row(&edge, "edge", "GQL_INDEX_REOPEN_EDGE", "score", "range");

    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let reopened_node = native_node_index(
        &reopened,
        "GqlIndexReopenNode",
        "status",
        SecondaryIndexKind::Equality,
    )
    .unwrap();
    assert_eq!(reopened_node.index_id, node_index_id);
    assert_eq!(reopened_node.label, "GqlIndexReopenNode");
    assert_eq!(reopened_node.fields, property_index_fields("status"));
    assert_eq!(reopened_node.kind, SecondaryIndexKind::Equality);
    assert!(reopened_node.last_error.is_none());

    let reopened_edge = native_edge_index(
        &reopened,
        "GQL_INDEX_REOPEN_EDGE",
        "score",
        SecondaryIndexKind::Range,
    )
    .unwrap();
    assert_eq!(reopened_edge.index_id, edge_index_id);
    assert_eq!(reopened_edge.label, "GQL_INDEX_REOPEN_EDGE");
    assert_eq!(reopened_edge.fields, property_index_fields("score"));
    assert_eq!(reopened_edge.kind, SecondaryIndexKind::Range);
    assert!(reopened_edge.last_error.is_none());

    let show = execute_gql_ok(&reopened, "SHOW PROPERTY INDEXES");
    assert_show_property_index_row(
        &show,
        "node",
        "GqlIndexReopenNode",
        "status",
        "equality",
        node_index_id,
    );
    assert_show_property_index_row(
        &show,
        "edge",
        "GQL_INDEX_REOPEN_EDGE",
        "score",
        "range",
        edge_index_id,
    );
    reopened.close().unwrap();
}

#[test]
fn gql_index_lifecycle_flush_reopen_queries_remain_correct_while_building() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let active = insert_query_node(
        &engine,
        "GqlIndexFlushReopenNode",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlIndexFlushReopenNode",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    engine.flush().unwrap();

    let (build_ready_rx, build_release_tx) = engine.set_secondary_index_build_pause();
    let created = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexFlushReopenNode) ON (n.status) KIND EQUALITY",
    );
    let index_id = assert_create_property_index_row(
        &created,
        "node",
        "GqlIndexFlushReopenNode",
        "status",
        "equality",
    );
    build_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        native_node_index(
            &engine,
            "GqlIndexFlushReopenNode",
            "status",
            SecondaryIndexKind::Equality,
        )
        .unwrap()
        .state,
        SecondaryIndexState::Building
    );
    assert_eq!(
        engine
            .find_nodes(
                "GqlIndexFlushReopenNode",
                "status",
                &PropValue::String("active".to_string()),
            )
            .unwrap(),
        vec![active]
    );

    build_release_tx.send(()).unwrap();
    wait_for_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    wait_for_property_index_state(&reopened, index_id, SecondaryIndexState::Ready);
    assert_eq!(
        reopened
            .find_nodes(
                "GqlIndexFlushReopenNode",
                "status",
                &PropValue::String("active".to_string()),
            )
            .unwrap(),
        vec![active]
    );
    reopened.close().unwrap();
}

#[test]
fn gql_index_lifecycle_drop_persists_absence_and_fallback_queries() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let node_keep = insert_query_node(
        &engine,
        "GqlIndexDropReopenNode",
        "keep",
        &[("status", PropValue::String("keep".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlIndexDropReopenNode",
        "skip",
        &[("status", PropValue::String("skip".to_string()))],
        1.0,
    );
    let from = insert_query_node(&engine, "GqlIndexDropReopenEndpoint", "from", &[], 1.0);
    let to = insert_query_node(&engine, "GqlIndexDropReopenEndpoint", "to", &[], 1.0);
    let edge_keep = engine
        .upsert_edge(
            from,
            to,
            "GQL_INDEX_DROP_REOPEN_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(7))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            to,
            from,
            "GQL_INDEX_DROP_REOPEN_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(3))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let node_create = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexDropReopenNode) ON (n.status) KIND EQUALITY",
    );
    let node_index_id = assert_create_property_index_row(
        &node_create,
        "node",
        "GqlIndexDropReopenNode",
        "status",
        "equality",
    );
    let edge_create = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_DROP_REOPEN_EDGE]-() ON (r.score) KIND RANGE",
    );
    let edge_index_id = assert_create_property_index_row(
        &edge_create,
        "edge",
        "GQL_INDEX_DROP_REOPEN_EDGE",
        "score",
        "range",
    );
    wait_for_property_index_state(&engine, node_index_id, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&engine, edge_index_id, SecondaryIndexState::Ready);

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexDropReopenNode) ON (n.status) KIND EQUALITY",
    );
    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_DROP_REOPEN_EDGE]-() ON (r.score) KIND RANGE",
    );
    assert!(native_node_index(
        &engine,
        "GqlIndexDropReopenNode",
        "status",
        SecondaryIndexKind::Equality
    )
    .is_none());
    assert!(native_edge_index(
        &engine,
        "GQL_INDEX_DROP_REOPEN_EDGE",
        "score",
        SecondaryIndexKind::Range
    )
    .is_none());

    let node_query = gql_index_test_node_query(
        "GqlIndexDropReopenNode",
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("keep".to_string()),
        },
    );
    let edge_query = gql_index_test_edge_query(
        "GQL_INDEX_DROP_REOPEN_EDGE",
        EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(5))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(8))),
        },
    );
    assert_eq!(engine.query_node_ids(&node_query).unwrap().items, vec![node_keep]);
    assert_eq!(
        engine.query_edge_ids(&edge_query).unwrap().edge_ids,
        vec![edge_keep]
    );

    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(native_node_index(
        &reopened,
        "GqlIndexDropReopenNode",
        "status",
        SecondaryIndexKind::Equality
    )
    .is_none());
    assert!(native_edge_index(
        &reopened,
        "GQL_INDEX_DROP_REOPEN_EDGE",
        "score",
        SecondaryIndexKind::Range
    )
    .is_none());
    let show = execute_gql_ok(&reopened, "SHOW PROPERTY INDEXES");
    assert_show_property_index_absent(
        &show,
        "node",
        "GqlIndexDropReopenNode",
        "status",
        "equality",
    );
    assert_show_property_index_absent(
        &show,
        "edge",
        "GQL_INDEX_DROP_REOPEN_EDGE",
        "score",
        "range",
    );
    assert_eq!(
        reopened.query_node_ids(&node_query).unwrap().items,
        vec![node_keep]
    );
    assert_eq!(
        reopened.query_edge_ids(&edge_query).unwrap().edge_ids,
        vec![edge_keep]
    );
    reopened.close().unwrap();
}

#[test]
fn gql_index_lifecycle_retry_failed_declaration_matches_native_ensure() {
    let (_dir, engine) = query_test_engine();
    let source = "CREATE PROPERTY INDEX FOR (n:GqlIndexRetryFailedNode) ON (n.status) KIND EQUALITY";

    let created = execute_gql_ok(&engine, source);
    let index_id = assert_create_property_index_row(
        &created,
        "node",
        "GqlIndexRetryFailedNode",
        "status",
        "equality",
    );
    engine.shutdown_secondary_index_worker();
    engine
        .with_runtime_manifest_write(|manifest| {
            let entry = manifest
                .secondary_indexes
                .iter_mut()
                .find(|entry| entry.index_id == index_id)
                .unwrap();
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some("gql index forced failure".to_string());
            Ok(())
        })
        .unwrap();
    engine.rebuild_secondary_index_catalog().unwrap();
    let failed = native_node_index(
        &engine,
        "GqlIndexRetryFailedNode",
        "status",
        SecondaryIndexKind::Equality,
    )
    .unwrap();
    assert_eq!(failed.index_id, index_id);
    assert_eq!(failed.state, SecondaryIndexState::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("gql index forced failure"));

    let retried = execute_gql_ok(&engine, source);
    let retry_index_id = assert_create_property_index_row(
        &retried,
        "node",
        "GqlIndexRetryFailedNode",
        "status",
        "equality",
    );
    assert_eq!(retry_index_id, index_id);
    assert_eq!(
        retried.rows[0].values[6],
        GqlValue::String("building".to_string())
    );
    assert_eq!(retried.rows[0].values[8], GqlValue::Null);
    let native = native_node_index(
        &engine,
        "GqlIndexRetryFailedNode",
        "status",
        SecondaryIndexKind::Equality,
    )
    .unwrap();
    assert_eq!(native.index_id, index_id);
    assert_eq!(native.state, SecondaryIndexState::Building);
    assert!(native.last_error.is_none());
}

#[test]
fn gql_index_lifecycle_planner_uses_ready_declarations_and_falls_back_after_drop() {
    let (_dir, engine) = query_test_engine();

    let node_eq_keep = insert_query_node(
        &engine,
        "GqlIndexPlanNodeEq",
        "active",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlIndexPlanNodeEq",
        "inactive",
        &[("status", PropValue::String("inactive".to_string()))],
        1.0,
    );
    let node_range_keep = insert_query_node(
        &engine,
        "GqlIndexPlanNodeRange",
        "mid",
        &[("score", PropValue::Int(5))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlIndexPlanNodeRange",
        "low",
        &[("score", PropValue::Int(1))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlIndexPlanNodeRange",
        "high",
        &[("score", PropValue::Int(9))],
        1.0,
    );

    let from = insert_query_node(&engine, "GqlIndexPlanEndpoint", "from", &[], 1.0);
    let to = insert_query_node(&engine, "GqlIndexPlanEndpoint", "to", &[], 1.0);
    let other = insert_query_node(&engine, "GqlIndexPlanEndpoint", "other", &[], 1.0);
    let edge_eq_keep = engine
        .upsert_edge(
            from,
            to,
            "GQL_INDEX_PLAN_EDGE_EQ",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            from,
            other,
            "GQL_INDEX_PLAN_EDGE_EQ",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let edge_range_keep = engine
        .upsert_edge(
            to,
            other,
            "GQL_INDEX_PLAN_EDGE_RANGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(5))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            other,
            to,
            "GQL_INDEX_PLAN_EDGE_RANGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("score", PropValue::Int(10))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let node_eq_index_id = assert_create_property_index_row(
        &execute_gql_ok(
            &engine,
            "CREATE PROPERTY INDEX FOR (n:GqlIndexPlanNodeEq) ON (n.status) KIND EQUALITY",
        ),
        "node",
        "GqlIndexPlanNodeEq",
        "status",
        "equality",
    );
    let node_range_index_id = assert_create_property_index_row(
        &execute_gql_ok(
            &engine,
            "CREATE PROPERTY INDEX FOR (n:GqlIndexPlanNodeRange) ON (n.score) KIND RANGE",
        ),
        "node",
        "GqlIndexPlanNodeRange",
        "score",
        "range",
    );
    let edge_eq_index_id = assert_create_property_index_row(
        &execute_gql_ok(
            &engine,
            "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_PLAN_EDGE_EQ]-() ON (r.status) KIND EQUALITY",
        ),
        "edge",
        "GQL_INDEX_PLAN_EDGE_EQ",
        "status",
        "equality",
    );
    let edge_range_index_id = assert_create_property_index_row(
        &execute_gql_ok(
            &engine,
            "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_PLAN_EDGE_RANGE]-() ON (r.score) KIND RANGE",
        ),
        "edge",
        "GQL_INDEX_PLAN_EDGE_RANGE",
        "score",
        "range",
    );

    wait_for_property_index_state(&engine, node_eq_index_id, SecondaryIndexState::Ready);
    wait_for_property_index_state(&engine, node_range_index_id, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&engine, edge_eq_index_id, SecondaryIndexState::Ready);
    wait_for_edge_property_index_state(&engine, edge_range_index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(&engine, node_eq_index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(
        &engine,
        node_range_index_id,
        SecondaryIndexState::Ready,
    );
    wait_for_published_property_index_state(&engine, edge_eq_index_id, SecondaryIndexState::Ready);
    wait_for_published_property_index_state(
        &engine,
        edge_range_index_id,
        SecondaryIndexState::Ready,
    );

    let node_eq_query = gql_index_test_node_query(
        "GqlIndexPlanNodeEq",
        NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
    );
    let node_range_query = gql_index_test_node_query(
        "GqlIndexPlanNodeRange",
        NodeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(4))),
            upper: Some(PropertyRangeBound::Included(PropValue::Int(6))),
        },
    );
    let edge_eq_query = gql_index_test_edge_query(
        "GQL_INDEX_PLAN_EDGE_EQ",
        EdgeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        },
    );
    let edge_range_query = gql_index_test_edge_query(
        "GQL_INDEX_PLAN_EDGE_RANGE",
        EdgeFilterExpr::PropertyRange {
            key: "score".to_string(),
            lower: Some(PropertyRangeBound::Included(PropValue::Int(4))),
            upper: Some(PropertyRangeBound::Excluded(PropValue::Int(9))),
        },
    );

    let node_eq_ids = engine.query_node_ids(&node_eq_query).unwrap().items;
    assert_eq!(node_eq_ids, vec![node_eq_keep]);
    assert_plan_input_nodes(
        &engine.explain_node_query(&node_eq_query).unwrap(),
        vec![QueryPlanNode::PropertyEqualityIndex],
    );

    let node_range_ids = engine.query_node_ids(&node_range_query).unwrap().items;
    assert_eq!(node_range_ids, vec![node_range_keep]);
    assert_plan_input_nodes(
        &engine.explain_node_query(&node_range_query).unwrap(),
        vec![QueryPlanNode::PropertyRangeIndex],
    );

    let edge_eq_ids = engine.query_edge_ids(&edge_eq_query).unwrap().edge_ids;
    assert_eq!(edge_eq_ids, vec![edge_eq_keep]);
    let edge_eq_plan = engine.explain_edge_query(&edge_eq_query).unwrap();
    assert!(plan_contains_node(
        &edge_eq_plan.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));

    let edge_range_ids = engine.query_edge_ids(&edge_range_query).unwrap().edge_ids;
    assert_eq!(edge_range_ids, vec![edge_range_keep]);
    let edge_range_plan = engine.explain_edge_query(&edge_range_query).unwrap();
    assert!(plan_contains_node(
        &edge_range_plan.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexPlanNodeEq) ON (n.status) KIND EQUALITY",
    );
    assert_eq!(
        engine.query_node_ids(&node_eq_query).unwrap().items,
        node_eq_ids
    );
    let node_eq_fallback = engine.explain_node_query(&node_eq_query).unwrap();
    assert_plan_input_nodes(&node_eq_fallback, vec![QueryPlanNode::FallbackNodeLabelScan]);
    assert!(!plan_contains_node(
        &node_eq_fallback.root,
        &QueryPlanNode::PropertyEqualityIndex
    ));

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexPlanNodeRange) ON (n.score) KIND RANGE",
    );
    assert_eq!(
        engine.query_node_ids(&node_range_query).unwrap().items,
        node_range_ids
    );
    let node_range_fallback = engine.explain_node_query(&node_range_query).unwrap();
    assert_plan_input_nodes(
        &node_range_fallback,
        vec![QueryPlanNode::FallbackNodeLabelScan],
    );
    assert!(!plan_contains_node(
        &node_range_fallback.root,
        &QueryPlanNode::PropertyRangeIndex
    ));

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_PLAN_EDGE_EQ]-() ON (r.status) KIND EQUALITY",
    );
    assert_eq!(
        engine.query_edge_ids(&edge_eq_query).unwrap().edge_ids,
        edge_eq_ids
    );
    let edge_eq_fallback = engine.explain_edge_query(&edge_eq_query).unwrap();
    assert!(!plan_contains_node(
        &edge_eq_fallback.root,
        &QueryPlanNode::EdgePropertyEqualityIndex
    ));
    assert!(edge_eq_fallback
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(edge_eq_fallback
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_PLAN_EDGE_RANGE]-() ON (r.score) KIND RANGE",
    );
    assert_eq!(
        engine.query_edge_ids(&edge_range_query).unwrap().edge_ids,
        edge_range_ids
    );
    let edge_range_fallback = engine.explain_edge_query(&edge_range_query).unwrap();
    assert!(!plan_contains_node(
        &edge_range_fallback.root,
        &QueryPlanNode::EdgePropertyRangeIndex
    ));
    assert!(edge_range_fallback
        .warnings
        .contains(&QueryPlanWarning::EdgePropertyPostFilter));
    assert!(edge_range_fallback
        .warnings
        .contains(&QueryPlanWarning::VerifyOnlyFilter));
}

#[test]
fn gql_index_created_node_metadata_compound_index_is_used_by_gql_read_predicates() {
    let (_dir, engine) = query_test_engine();
    let keep = insert_query_node(
        &engine,
        "GqlIndexMetaNode",
        "keep",
        &[("tenant_id", PropValue::String("acme".to_string()))],
        1.0,
    );
    let old = insert_query_node(
        &engine,
        "GqlIndexMetaNode",
        "old",
        &[("tenant_id", PropValue::String("acme".to_string()))],
        1.0,
    );
    let other_tenant = insert_query_node(
        &engine,
        "GqlIndexMetaNode",
        "other",
        &[("tenant_id", PropValue::String("globex".to_string()))],
        1.0,
    );
    set_query_node_updated_at(&engine, keep, 2_000);
    set_query_node_updated_at(&engine, old, 500);
    set_query_node_updated_at(&engine, other_tenant, 2_000);
    engine.flush().unwrap();

    let created = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexMetaNode) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    let index_id = assert_create_property_index_row_fields(
        &created,
        "node",
        "GqlIndexMetaNode",
        &[("property", "tenant_id"), ("metadata", "updatedAt")],
        "range",
    );
    wait_for_published_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
    assert!(
        native_node_index_fields(
            &engine,
            "GqlIndexMetaNode",
            &[
                SecondaryIndexField::property("tenant_id"),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
            ],
            SecondaryIndexKind::Range,
        )
        .is_some()
    );

    let source = "MATCH (n:GqlIndexMetaNode) WHERE n.tenant_id = 'acme' AND updatedAt(n) >= 1000 RETURN id(n)";
    let result = execute_gql_with_options(
        &engine,
        source,
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(keep));
    let read = gql_read_explain(result.plan.as_ref().expect("include_plan should return plan"));
    assert!(
        read.projection
            .iter()
            .any(|line| line.contains("CompoundRangeIndex") && line.contains("NodeMetadata(UpdatedAt)")),
        "expected GQL read explain to use compound range index, got {:?}",
        read.projection
    );
}

#[test]
fn gql_index_created_edge_metadata_compound_index_is_used_by_gql_read_predicates() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "GqlIndexMetaEdgeEndpoint", "from", &[], 1.0);
    let to = insert_query_node(&engine, "GqlIndexMetaEdgeEndpoint", "to", &[], 1.0);
    let keep = engine
        .upsert_edge(
            from,
            to,
            "GQL_INDEX_META_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                valid_to: Some(i64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            from,
            to,
            "GQL_INDEX_META_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("active".to_string()))]),
                valid_to: Some(i64::MAX / 4),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(
            from,
            to,
            "GQL_INDEX_META_EDGE",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("inactive".to_string()))]),
                valid_to: Some(i64::MAX),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let created = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_META_EDGE]-() ON (r.status, validTo(r)) KIND RANGE",
    );
    let index_id = assert_create_property_index_row_fields(
        &created,
        "edge",
        "GQL_INDEX_META_EDGE",
        &[("property", "status"), ("metadata", "validTo")],
        "range",
    );
    wait_for_published_property_index_state(&engine, index_id, SecondaryIndexState::Ready);
    assert!(
        native_edge_index_fields(
            &engine,
            "GQL_INDEX_META_EDGE",
            &[
                SecondaryIndexField::property("status"),
                SecondaryIndexField::edge_meta(EdgeMetadataIndexField::ValidTo),
            ],
            SecondaryIndexKind::Range,
        )
        .is_some()
    );

    let source = "MATCH ()-[r:GQL_INDEX_META_EDGE]->() WHERE r.status = 'active' AND validTo(r) >= 4611686018427387903 RETURN id(r)";
    let result = execute_gql_with_options(
        &engine,
        source,
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::UInt(keep));
    let read = gql_read_explain(result.plan.as_ref().expect("include_plan should return plan"));
    assert!(
        read.projection
            .iter()
            .any(|line| line.contains("CompoundRangeIndex") && line.contains("EdgeMetadata(ValidTo)")),
        "expected GQL read explain to use compound edge range index, got {:?}",
        read.projection
    );
}

#[test]
fn gql_index_compound_prefix_warning_renders_locked_message() {
    let (_dir, engine) = query_test_engine();
    let node = insert_query_node(
        &engine,
        "GqlIndexPrefixWarning",
        "node",
        &[("tenant_id", PropValue::String("acme".to_string()))],
        1.0,
    );
    set_query_node_updated_at(&engine, node, 2_000);
    engine.flush().unwrap();

    let created = execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexPrefixWarning) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    let index_id = gql_uint(&created.rows[0].values[7]);
    wait_for_published_property_index_state(&engine, index_id, SecondaryIndexState::Ready);

    let warning = "compound secondary index skipped because query predicates do not constrain a left prefix of the declaration";
    let source =
        "MATCH (n:GqlIndexPrefixWarning) WHERE updatedAt(n) >= 1000 RETURN id(n)";
    let explained = engine
        .explain_gql(source, &GqlParams::new(), &gql_opts())
        .unwrap();
    let read = gql_read_explain(&explained);
    assert!(
        read.warnings.iter().any(|actual| actual == warning),
        "expected locked compound warning in explain, got {:?}",
        read.warnings
    );

    let result = execute_gql_with_options(
        &engine,
        source,
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );
    let read = gql_read_explain(result.plan.as_ref().expect("include_plan should return plan"));
    assert!(
        read.warnings.iter().any(|actual| actual == warning),
        "expected locked compound warning in include-plan read explain, got {:?}",
        read.warnings
    );
}

#[test]
fn gql_index_lifecycle_unsupported_explain_and_unknown_drop_have_no_side_effects() {
    let (_dir, engine) = query_test_engine();
    let before = gql_index_side_effect_snapshot(&engine);

    for source in [
        "CREATE INDEX gql_index_named FOR (n:GqlIndexNoSideNamed) ON (n.status)",
        "CREATE TEXT INDEX gql_index_text FOR (n:GqlIndexNoSideFamily) ON (n.status)",
        "CREATE COMPOUND INDEX",
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNoSideKind) ON (n.status) KIND TEXT",
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_NO_SIDE_DIRECTED]->() ON (r.status) KIND EQUALITY",
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNoSideDuplicate) ON (n.a, n.a) KIND EQUALITY",
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNoSideParam) ON (n.$prop) KIND EQUALITY",
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNoSideMismatch) ON (m.status) KIND EQUALITY",
    ] {
        engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        assert_gql_index_no_side_effects(&engine, &before, source);
    }

    let explain_create =
        "CREATE PROPERTY INDEX FOR (n:GqlIndexNoSideExplainCreate) ON (n.status) KIND EQUALITY";
    engine
        .explain_gql(explain_create, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_gql_index_no_side_effects(&engine, &before, explain_create);

    let explain_drop =
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_NO_SIDE_EXPLAIN_DROP]-() ON (r.status) KIND RANGE";
    engine
        .explain_gql(explain_drop, &GqlParams::new(), &gql_opts())
        .unwrap();
    assert_gql_index_no_side_effects(&engine, &before, explain_drop);

    let drop_unknown_node =
        "DROP PROPERTY INDEX FOR (n:GqlIndexNoSideUnknownDrop) ON (n.status) KIND EQUALITY";
    let dropped = execute_gql_ok(&engine, drop_unknown_node);
    assert_drop_property_index_row(
        &dropped,
        "node",
        "GqlIndexNoSideUnknownDrop",
        "status",
        "equality",
        "not_found",
    );
    assert_gql_index_no_side_effects(&engine, &before, drop_unknown_node);

    let drop_unknown_edge =
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_NO_SIDE_UNKNOWN_DROP]-() ON (r.status) KIND RANGE";
    let dropped = execute_gql_ok(&engine, drop_unknown_edge);
    assert_drop_property_index_row(
        &dropped,
        "edge",
        "GQL_INDEX_NO_SIDE_UNKNOWN_DROP",
        "status",
        "range",
        "not_found",
    );
    assert_gql_index_no_side_effects(&engine, &before, drop_unknown_edge);
}

#[test]
fn gql_index_lifecycle_manifest_shape_uses_native_secondary_index_entries() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexManifestNode) ON (n.status) KIND EQUALITY",
    );
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR ()-[r:GQL_INDEX_MANIFEST_EDGE]-() ON (r.score) KIND RANGE",
    );
    execute_gql_ok(
        &engine,
        "CREATE PROPERTY INDEX FOR (n:GqlIndexManifestNode) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    assert_gql_index_manifest_secondary_index_shape(&db_path, 3);

    let manifest = gql_index_manifest_json(&db_path);
    let raw_manifest = manifest.to_string();
    assert!(!raw_manifest.contains("ddl"));
    assert!(!raw_manifest.contains("index_name"));
    assert!(!raw_manifest.contains("provider"));
    assert!(raw_manifest.contains("NodeFieldIndex"));

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexManifestNode) ON (n.tenant_id, updatedAt(n)) KIND RANGE",
    );
    assert_gql_index_manifest_secondary_index_shape(&db_path, 2);

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR (n:GqlIndexManifestNode) ON (n.status) KIND EQUALITY",
    );
    assert_gql_index_manifest_secondary_index_shape(&db_path, 1);

    execute_gql_ok(
        &engine,
        "DROP PROPERTY INDEX FOR ()-[r:GQL_INDEX_MANIFEST_EDGE]-() ON (r.score) KIND RANGE",
    );
    assert_gql_index_manifest_secondary_index_shape(&db_path, 0);
    engine.close().unwrap();
}

#[test]
fn gql_schema_alter_add_publishes_multi_target_and_show_sees_catalog() {
    let (_dir, engine) = query_test_engine();
    engine.set_node_schema("Keep", NodeSchema::default()).unwrap();

    let result = execute_gql_with_options(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Person = {}, EDGE KNOWS = {} }",
        GqlExecutionOptions {
            include_plan: true,
            ..gql_opts()
        },
    );

    assert_eq!(result.columns, alter_add_set_columns());
    assert_eq!(result.rows.len(), 2);
    let stats = assert_gql_schema_result(&result, "alter_graph_type_add");
    assert_eq!(stats.targets_checked, 2);
    assert_eq!(stats.targets_published, 2);
    assert_eq!(stats.targets_dropped, 0);
    assert_eq!(stats.checked_records, 0);
    assert_eq!(gql_string_column(&result, 0), vec!["alter_graph_type_add"; 2]);
    assert_eq!(gql_string_column(&result, 1), vec!["node", "edge"]);
    assert_eq!(gql_string_column(&result, 2), vec!["Person", "KNOWS"]);
    assert_eq!(gql_string_column(&result, 3), vec!["published", "published"]);
    let plan = gql_schema_explain(result.plan.as_ref().expect("include_plan should exist"));
    assert_eq!(plan.operation, "alter_graph_type_add");
    assert!(plan.publishes_manifest);
    assert!(plan.uses_core_write_queue);
    assert!(!plan.side_effect_free);

    let show = execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE");
    assert_eq!(show.columns, show_columns());
    assert_gql_schema_result(&show, "show_current_graph_type");
    assert_eq!(show.rows.len(), 3);
    assert_eq!(gql_string_column(&show, 1), vec!["Keep", "Person", "KNOWS"]);
}

#[test]
fn gql_schema_alter_set_replaces_catalog_and_empty_set_publishes_summary() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Keep = {}, EDGE OLD_EDGE = {} }",
    );

    let set = execute_gql_ok(&engine, "ALTER CURRENT GRAPH TYPE SET { NODE Person = {} }");
    let stats = assert_gql_schema_result(&set, "alter_graph_type_set");
    assert_eq!(stats.targets_published, 1);
    assert_eq!(stats.targets_dropped, 2);
    assert_eq!(set.rows.len(), 1);
    assert_eq!(gql_string_column(&set, 2), vec!["Person"]);
    let show = execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE");
    assert_eq!(gql_string_column(&show, 1), vec!["Person"]);

    let empty = execute_gql_ok(&engine, "ALTER CURRENT GRAPH TYPE SET {}");
    assert_eq!(empty.columns, alter_add_set_columns());
    let stats = assert_gql_schema_result(&empty, "alter_graph_type_set");
    assert_eq!(stats.targets_published, 0);
    assert_eq!(stats.targets_dropped, 1);
    assert_eq!(
        empty.rows,
        vec![GqlRow {
            values: vec![
                GqlValue::String("alter_graph_type_set".to_string()),
                GqlValue::String("graph".to_string()),
                GqlValue::Null,
                GqlValue::String("published_empty_graph_type".to_string()),
                GqlValue::UInt(0),
                GqlValue::UInt(0),
                GqlValue::Bool(false),
                GqlValue::Bool(false),
            ],
        }]
    );
    assert!(execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE")
        .rows
        .is_empty());
}

#[test]
fn gql_schema_selected_drop_and_drop_current_return_authoritative_counts() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Person = {}, NODE Company = {}, EDGE KNOWS = {} }",
    );

    let selected = execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE DROP { NODE Person, EDGE MissingEdge, EDGE KNOWS, NODE MissingNode }",
    );
    assert_eq!(selected.columns, alter_drop_columns());
    let stats = assert_gql_schema_result(&selected, "alter_graph_type_drop");
    assert_eq!(stats.targets_dropped, 2);
    assert_eq!(gql_string_column(&selected, 1), vec!["node", "edge", "edge", "node"]);
    assert_eq!(
        gql_string_column(&selected, 2),
        vec!["Person", "MissingEdge", "KNOWS", "MissingNode"]
    );
    assert_eq!(
        gql_string_column(&selected, 3),
        vec!["dropped", "not_found", "dropped", "not_found"]
    );
    let show = execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE");
    assert_eq!(gql_string_column(&show, 1), vec!["Company"]);

    execute_gql_ok(&engine, "ALTER CURRENT GRAPH TYPE ADD { EDGE WORKS_AT = {} }");
    let dropped = execute_gql_ok(&engine, "DROP CURRENT GRAPH TYPE");
    assert_eq!(dropped.columns, drop_current_columns());
    let stats = assert_gql_schema_result(&dropped, "drop_current_graph_type");
    assert_eq!(stats.targets_dropped, 2);
    assert_eq!(
        dropped.rows[0].values,
        vec![
            GqlValue::String("drop_current_graph_type".to_string()),
            GqlValue::String("graph".to_string()),
            GqlValue::Null,
            GqlValue::String("dropped".to_string()),
            GqlValue::UInt(1),
            GqlValue::UInt(1),
        ]
    );
}

#[test]
fn gql_schema_check_rows_are_side_effect_free_and_readonly_allowed() {
    let (_dir, engine) = query_test_engine();
    engine
        .upsert_node("DryRun", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let check = engine
        .execute_gql(
            "CHECK CURRENT GRAPH TYPE ADD { NODE DryRun = { properties: { name: { required: true, nullable: false, types: ['string'] } } } } OPTIONS { max_violations: 5, scan_limit: null }",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(check.columns, check_columns());
    let stats = assert_gql_schema_result(&check, "check_graph_type_add");
    assert_eq!(stats.targets_checked, 1);
    assert_eq!(stats.targets_published, 0);
    assert_eq!(stats.violation_count, 1);
    assert_eq!(check.rows.len(), 1);
    assert_eq!(check.rows[0].values[3], GqlValue::UInt(1));
    assert_eq!(check.rows[0].values[4], GqlValue::UInt(1));
    let violations = gql_list(&check.rows[0].values[7]);
    assert_eq!(violations.len(), 1);
    let violation = gql_map(&violations[0]);
    let target = gql_map(&violation["target"]);
    assert_eq!(gql_str(&target["kind"]), "node");
    assert_gql_tagged_uint(&target["id"], "1");
    assert!(engine.get_node_schema("DryRun").unwrap().is_none());

    let no_token = engine.get_node_label_id("FutureCheckOnly").unwrap();
    assert!(no_token.is_none());
    let future = engine
        .execute_gql(
            "CHECK CURRENT GRAPH TYPE ADD { NODE FutureCheckOnly = {} }",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_gql_schema_result(&future, "check_graph_type_add");
    assert!(engine.get_node_schema("FutureCheckOnly").unwrap().is_none());
    assert!(engine.get_node_label_id("FutureCheckOnly").unwrap().is_none());

    let empty = engine
        .execute_gql(
            "CHECK CURRENT GRAPH TYPE SET {}",
            &GqlParams::new(),
            &GqlExecutionOptions {
                mode: GqlExecutionMode::ReadOnly,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(empty.columns, check_columns());
    assert_gql_schema_result(&empty, "check_graph_type_set");
    assert_eq!(
        empty.rows[0].values,
        vec![
            GqlValue::String("check_graph_type_set".to_string()),
            GqlValue::String("graph".to_string()),
            GqlValue::Null,
            GqlValue::UInt(0),
            GqlValue::UInt(0),
            GqlValue::Bool(false),
            GqlValue::Bool(false),
            GqlValue::List(Vec::new()),
        ]
    );
}

#[test]
fn gql_schema_failed_alter_and_scan_limit_publish_nothing() {
    let (_dir, engine) = query_test_engine();
    engine
        .upsert_node("Violating", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let err = engine
        .execute_gql(
            "ALTER CURRENT GRAPH TYPE ADD { NODE FutureClean = {}, NODE Violating = { properties: { name: { required: true, nullable: false, types: ['string'] } } } }",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected"));
    assert!(engine.get_node_schema("FutureClean").unwrap().is_none());
    assert!(engine.get_node_schema("Violating").unwrap().is_none());
    assert!(engine.get_node_label_id("FutureClean").unwrap().is_none());

    let err = engine
        .execute_gql(
            "ALTER CURRENT GRAPH TYPE ADD { NODE Violating = {} } OPTIONS { scan_limit: 0 }",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("scan limit"));
    assert!(engine.get_node_schema("Violating").unwrap().is_none());
}

#[test]
fn gql_schema_show_variants_and_max_rows_are_stable() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Alpha = {}, NODE Beta = {}, EDGE REL = {} }",
    );

    let current = execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE");
    assert_eq!(current.columns, show_columns());
    assert_eq!(gql_string_column(&current, 0), vec!["node", "node", "edge"]);
    assert_eq!(gql_string_column(&current, 1), vec!["Alpha", "Beta", "REL"]);
    assert!(matches!(current.rows[0].values[2], GqlValue::Map(_)));

    let nodes = execute_gql_ok(&engine, "SHOW NODE SCHEMAS");
    assert_eq!(gql_string_column(&nodes, 1), vec!["Alpha", "Beta"]);
    assert_gql_schema_result(&nodes, "show_node_schemas");
    let edges = execute_gql_ok(&engine, "SHOW EDGE SCHEMAS");
    assert_eq!(gql_string_column(&edges, 1), vec!["REL"]);
    assert_gql_schema_result(&edges, "show_edge_schemas");

    let one_node = execute_gql_ok(&engine, "SHOW NODE SCHEMA Alpha");
    assert_eq!(one_node.rows.len(), 1);
    assert_gql_schema_result(&one_node, "show_node_schema");
    let missing_node = execute_gql_ok(&engine, "SHOW NODE SCHEMA Missing");
    assert!(missing_node.rows.is_empty());
    let one_edge = execute_gql_ok(&engine, "SHOW EDGE SCHEMA REL");
    assert_eq!(one_edge.rows.len(), 1);
    assert_gql_schema_result(&one_edge, "show_edge_schema");
    let missing_edge = execute_gql_ok(&engine, "SHOW EDGE SCHEMA MissingRel");
    assert!(missing_edge.rows.is_empty());

    let err = engine
        .execute_gql(
            "SHOW CURRENT GRAPH TYPE",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(ref message) if message.contains("exceeding max_rows=2")));
    assert_eq!(execute_gql_ok(&engine, "SHOW CURRENT GRAPH TYPE").rows.len(), 3);
}

#[test]
fn gql_schema_cursor_readonly_and_explain_side_effect_rules() {
    let (_dir, engine) = query_test_engine();
    for source in [
        "ALTER CURRENT GRAPH TYPE ADD { NODE Person = {} }",
        "CHECK CURRENT GRAPH TYPE SET {}",
        "SHOW NODE SCHEMAS",
        "DROP CURRENT GRAPH TYPE",
    ] {
        let err = engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    cursor: Some("schema-cursor".to_string()),
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidCursor { .. }));
        let err = engine
            .explain_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    cursor: Some("schema-cursor".to_string()),
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidCursor { .. }));
    }

    for source in [
        "ALTER CURRENT GRAPH TYPE SET {}",
        "DROP CURRENT GRAPH TYPE",
    ] {
        let err = engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    mode: GqlExecutionMode::ReadOnly,
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, EngineError::InvalidOperation(ref message) if message.contains("ReadOnly")),
            "unexpected ReadOnly error for {source}: {err:?}"
        );
        let err = engine
            .explain_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    mode: GqlExecutionMode::ReadOnly,
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, EngineError::InvalidOperation(ref message) if message.contains("ReadOnly")),
            "unexpected ReadOnly explain error for {source}: {err:?}"
        );
    }

    for source in ["CHECK CURRENT GRAPH TYPE SET {}", "SHOW EDGE SCHEMAS"] {
        engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    mode: GqlExecutionMode::ReadOnly,
                    ..gql_opts()
                },
            )
            .unwrap();
        engine
            .explain_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    mode: GqlExecutionMode::ReadOnly,
                    ..gql_opts()
                },
            )
            .unwrap();
    }

    let explain = engine
        .explain_gql(
            "ALTER CURRENT GRAPH TYPE ADD { NODE FutureExplain = {} }",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    let schema = gql_schema_explain(&explain);
    assert_eq!(schema.operation, "alter_graph_type_add");
    assert!(schema.publishes_manifest);
    assert!(schema.uses_core_write_queue);
    assert!(!schema.side_effect_free);
    assert!(engine.get_node_schema("FutureExplain").unwrap().is_none());
    assert!(engine.get_node_label_id("FutureExplain").unwrap().is_none());
}

#[test]
fn gql_schema_publication_enforces_later_writes_through_shared_write_path() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Strict = { additional_properties: 'reject', properties: { name: { required: true, nullable: false, types: ['string'] } } } }",
    );

    let err = engine
        .execute_gql(
            "CREATE (n:Strict {elementKey: 'bad', extra: 'x'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("schema violation"));
    assert!(engine.get_node_by_key("Strict", "bad").unwrap().is_none());

    let native_err = engine
        .upsert_node("Strict", "native_bad", UpsertNodeOptions::default())
        .unwrap_err();
    assert!(native_err.to_string().contains("schema violation"));
    assert!(engine
        .get_node_by_key("Strict", "native_bad")
        .unwrap()
        .is_none());
}

#[test]
fn gql_schema_show_and_check_use_tagged_uint_and_bytes_values() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "ALTER CURRENT GRAPH TYPE ADD { NODE Tagged = { properties: { payload: { enum_values: [{ type: 'uint', value: '18446744073709551615' }, { type: 'bytes', value: [0, 1, 255] }] } } } }",
    );
    let show = execute_gql_ok(&engine, "SHOW NODE SCHEMA Tagged");
    let schema = gql_map(&show.rows[0].values[2]);
    let properties = gql_map(&schema["properties"]);
    let payload = gql_map(&properties["payload"]);
    let enum_values = gql_list(&payload["enum_values"]);
    assert_gql_tagged_uint(&enum_values[0], "18446744073709551615");
    assert_gql_tagged_bytes(&enum_values[1], &[0, 1, 255]);

    engine
        .upsert_node("NeedsName", "missing", UpsertNodeOptions::default())
        .unwrap();
    let check = execute_gql_ok(
        &engine,
        "CHECK CURRENT GRAPH TYPE ADD { NODE NeedsName = { properties: { name: { required: true, nullable: false, types: ['string'] } } } }",
    );
    let violations = gql_list(&check.rows[0].values[7]);
    let violation = gql_map(&violations[0]);
    let target = gql_map(&violation["target"]);
    assert_gql_tagged_uint(&target["id"], "1");
}

#[test]
fn gql_schema_params_are_resource_capped_before_binding() {
    let (_dir, engine) = query_test_engine();
    let schema_with_long_key = GqlParamValue::Map(BTreeMap::from([(
        "additional_properties".to_string(),
        GqlParamValue::String("allow".to_string()),
    )]));
    let err = engine
        .execute_gql(
            "CHECK CURRENT GRAPH TYPE SET { NODE Person = $schema }",
            &GqlParams::from([("schema".to_string(), schema_with_long_key)]),
            &gql_param_cap_options(64, 8, 4),
        )
        .unwrap_err();
    assert_gql_param_error(err, "schema", "exceeding max_param_bytes");

    let deeply_nested_schema = GqlParamValue::Map(BTreeMap::from([(
        "properties".to_string(),
        GqlParamValue::Map(BTreeMap::from([(
            "name".to_string(),
            GqlParamValue::Map(BTreeMap::from([(
                "types".to_string(),
                GqlParamValue::List(vec![GqlParamValue::String("string".to_string())]),
            )])),
        )])),
    )]));
    let err = engine
        .explain_gql(
            "CHECK CURRENT GRAPH TYPE SET { NODE Person = $schema }",
            &GqlParams::from([("schema".to_string(), deeply_nested_schema)]),
            &gql_param_cap_options(64, 1, 1024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "schema", "max_ast_depth");

    let options_with_long_key = GqlParamValue::Map(BTreeMap::from([(
        "max_violations".to_string(),
        GqlParamValue::Int(1),
    )]));
    let err = engine
        .execute_gql(
            "CHECK CURRENT GRAPH TYPE SET {} OPTIONS $options",
            &GqlParams::from([("options".to_string(), options_with_long_key)]),
            &gql_param_cap_options(64, 8, 4),
        )
        .unwrap_err();
    assert_gql_param_error(err, "options", "exceeding max_param_bytes");

    let deeply_nested_options = GqlParamValue::Map(BTreeMap::from([(
        "chunk_size".to_string(),
        GqlParamValue::Map(BTreeMap::from([(
            "nested".to_string(),
            GqlParamValue::Null,
        )])),
    )]));
    let err = engine
        .explain_gql(
            "CHECK CURRENT GRAPH TYPE SET {} OPTIONS $options",
            &GqlParams::from([("options".to_string(), deeply_nested_options)]),
            &gql_param_cap_options(64, 1, 1024),
        )
        .unwrap_err();
    assert_gql_param_error(err, "options", "max_ast_depth");
}

#[test]
fn gql_deferred_features_remain_rejected_after_row_ops() {
    let (_dir, engine) = query_test_engine();
    {
        let source = "MATCH (n:Person)-[*]->(m) RETURN n";
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
fn gql_read_only_exists_subqueries_execute_with_correlation_and_cache() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(
        &engine,
        "GqlSubExists",
        "a",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    let b = insert_query_node(
        &engine,
        "GqlSubExists",
        "b",
        &[("status", PropValue::String("active".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlSubExists",
        "c",
        &[("status", PropValue::String("stale".to_string()))],
        1.0,
    );
    engine
        .upsert_edge(a, b, "GQL_SUB_EXISTS_REL", UpsertEdgeOptions::default())
        .unwrap();

    let options = GqlExecutionOptions {
        allow_full_scan: true,
        include_plan: true,
        ..gql_opts()
    };
    let correlated = engine
        .execute_gql(
            "MATCH (n:GqlSubExists) \
             WHERE EXISTS { MATCH (n)-[:GQL_SUB_EXISTS_REL]->(m) RETURN m } \
             RETURN elementKey(n) AS key ORDER BY key",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(gql_string_column(&correlated, 0), vec!["a"]);
    let plan = gql_read_explain(correlated.plan.as_ref().expect("plan"));
    assert_eq!(plan.target, GqlLoweringTarget::GraphPipelineQuery);
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("exists_predicates=1")));

    let pushed_conjunct = engine
        .execute_gql(
            "MATCH (n:GqlSubExists) \
             WHERE n.status = 'active' AND EXISTS { MATCH (n)-[:GQL_SUB_EXISTS_REL]->(m) RETURN m } \
             RETURN elementKey(n) AS key ORDER BY key",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(gql_string_column(&pushed_conjunct, 0), vec!["a"]);
    let plan = gql_read_explain(pushed_conjunct.plan.as_ref().expect("plan"));
    assert!(
        plan.pushed_down.iter().any(|item| item.contains("n.status")),
        "expected subquery-free conjunct to stay pushdown-capable, got {:?}",
        plan.pushed_down
    );

    let zero_visible = engine
        .execute_gql(
            "MATCH (:GqlSubExists) \
             WHERE EXISTS { MATCH (m:GqlSubExists) RETURN m } \
             RETURN 1 AS one",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(zero_visible.rows.len(), 3);
    assert!(zero_visible
        .rows
        .iter()
        .all(|row| row.values == vec![GqlValue::Int(1)]));

    let repeated_key = engine
        .execute_gql(
            "MATCH (n:GqlSubExists) \
             WITH n.status AS status \
             WHERE EXISTS { MATCH (m:GqlSubExists) WHERE m.status = status RETURN m } \
             RETURN status ORDER BY status",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&repeated_key, 0),
        vec![
            "active".to_string(),
            "active".to_string(),
            "stale".to_string()
        ]
    );
    let plan = gql_read_explain(repeated_key.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_invocations=2")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_cache_hits=1")));

    let uncorrelated = engine
        .execute_gql(
             "MATCH (n:GqlSubExists) \
             WHERE EXISTS { MATCH (m:GqlSubExists) RETURN m } \
             RETURN elementKey(n) AS key ORDER BY key",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&uncorrelated, 0),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    let plan = gql_read_explain(uncorrelated.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_invocations=1")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_cache_hits=2")));

    insert_query_node(&engine, "GqlSubNullMarker", "marker", &[], 1.0);
    let null_key = engine
        .execute_gql(
            "MATCH (n:GqlSubExists) \
             WITH n.missing AS missing \
             WHERE EXISTS { MATCH (marker:GqlSubNullMarker) WHERE missing IS NULL RETURN marker } \
             RETURN missing",
            &GqlParams::new(),
            &options,
        )
        .unwrap();
    assert_eq!(null_key.rows.len(), 3);
    assert!(null_key
        .rows
        .iter()
        .all(|row| row.values == vec![GqlValue::Null]));
    let plan = gql_read_explain(null_key.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_invocations=1")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_cache_hits=2")));

    let epoch_a = insert_query_node(&engine, "GqlSubEpoch", "a", &[], 1.0);
    let epoch_b = insert_query_node(&engine, "GqlSubEpoch", "b", &[], 1.0);
    let epoch_c = insert_query_node(&engine, "GqlSubEpoch", "c", &[], 1.0);
    engine
        .upsert_edge(
            epoch_a,
            epoch_b,
            "GQL_SUB_EPOCH_REL",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine
        .upsert_edge(
            epoch_b,
            epoch_a,
            "GQL_SUB_EPOCH_REL",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    let epoch_source = "MATCH (n:GqlSubEpoch) \
                        WHERE EXISTS { MATCH (n)-[:GQL_SUB_EPOCH_REL]->(m) RETURN m } \
                        RETURN elementKey(n) AS key ORDER BY key";
    let first_epoch_page = engine
        .execute_gql(
            epoch_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&first_epoch_page, 0), vec!["a"]);
    let cursor = first_epoch_page
        .next_cursor
        .clone()
        .expect("first epoch page should return cursor");
    let cursor_epoch =
        graph_pipeline_decode_logical_cursor(&cursor, GraphPipelineOptions::default().max_cursor_bytes)
            .unwrap()
            .effective_at_epoch;
    engine
        .upsert_edge(
            epoch_c,
            epoch_a,
            "GQL_SUB_EPOCH_REL",
            UpsertEdgeOptions {
                valid_from: Some(cursor_epoch.saturating_add(1)),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    while now_millis() <= cursor_epoch.saturating_add(1) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let fresh_epoch_page = engine
        .execute_gql(
            epoch_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 10,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&fresh_epoch_page, 0),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    let second_epoch_page = engine
        .execute_gql(
            epoch_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 10,
                cursor: Some(cursor),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&second_epoch_page, 0), vec!["b"]);
}

#[test]
fn gql_exists_subquery_uses_physical_probe_for_simple_matches() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlSubExistsProbeOuter", "outer", &[], 1.0);
    for index in 0..8 {
        insert_query_node(
            &engine,
            "GqlSubExistsProbeInner",
            &format!("inner-{index}"),
            &[],
            1.0,
        );
    }

    let probe_options = GqlExecutionOptions {
        allow_full_scan: true,
        include_plan: true,
        max_intermediate_bindings: 1,
        ..gql_opts()
    };
    let broad_true = engine
        .execute_gql(
            "MATCH (outer:GqlSubExistsProbeOuter) \
             WHERE EXISTS { MATCH (inner:GqlSubExistsProbeInner) RETURN inner } \
             RETURN elementKey(outer)",
            &GqlParams::new(),
            &probe_options,
        )
        .unwrap();
    assert_eq!(gql_string_column(&broad_true, 0), vec!["outer"]);
    let plan = gql_read_explain(broad_true.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("physical_exists_probe=true")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_invocations=1")));

    let broad_false = engine
        .execute_gql(
            "MATCH (outer:GqlSubExistsProbeOuter) \
             WHERE EXISTS { MATCH (missing:GqlSubExistsProbeMissing) RETURN missing } \
             RETURN elementKey(outer)",
            &GqlParams::new(),
            &probe_options,
        )
        .unwrap();
    assert!(broad_false.rows.is_empty());

    for projection in [
        "inner.key AS key",
        "id(inner) AS inner_id",
        "{key: inner.key, id: id(inner)} AS payload",
        "[inner.key, id(inner)] AS payload",
    ] {
        let projected_true = engine
            .execute_gql(
                &format!(
                    "MATCH (outer:GqlSubExistsProbeOuter) \
                     WHERE EXISTS {{ MATCH (inner:GqlSubExistsProbeInner) RETURN {projection} }} \
                     RETURN elementKey(outer)"
                ),
                &GqlParams::new(),
                &probe_options,
            )
            .unwrap();
        assert_eq!(gql_string_column(&projected_true, 0), vec!["outer"]);
        let plan = gql_read_explain(projected_true.plan.as_ref().expect("plan"));
        assert!(
            plan.projection
                .iter()
                .any(|item| item.contains("physical_exists_probe=true")),
            "expected physical probe for projection {projection}, got {:?}",
            plan.projection
        );
    }

    let limit_zero = engine
        .execute_gql(
            "MATCH (outer:GqlSubExistsProbeOuter) \
             WHERE EXISTS { MATCH (inner:GqlSubExistsProbeInner) RETURN inner LIMIT 0 } \
             RETURN elementKey(outer)",
            &GqlParams::new(),
            &probe_options,
        )
        .unwrap();
    assert!(limit_zero.rows.is_empty());

    let unsafe_projection = engine
        .execute_gql(
            "MATCH (outer:GqlSubExistsProbeOuter) \
             WHERE EXISTS { MATCH (inner:GqlSubExistsProbeInner) RETURN 1 / 0 AS boom } \
             RETURN elementKey(outer)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                include_plan: true,
                max_intermediate_bindings: 64,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        unsafe_projection.to_string().contains("division by zero")
            || unsafe_projection.to_string().contains("divide by zero"),
        "unexpected unsafe projection error: {unsafe_projection:?}"
    );
}

#[test]
fn gql_exists_subquery_probe_does_not_cap_raw_edge_candidates() {
    let (_dir, engine) = query_test_engine();
    let source = insert_query_node(&engine, "GqlSubExistsProbeEdgeSource", "source", &[], 1.0);
    let miss = insert_query_node(&engine, "GqlSubExistsProbeEdgeMiss", "miss", &[], 1.0);
    let hit = insert_query_node(&engine, "GqlSubExistsProbeEdgeHit", "hit", &[], 1.0);
    engine
        .upsert_edge(
            source,
            miss,
            "GQL_SUB_EXISTS_PROBE_EDGE",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine
        .upsert_edge(
            source,
            hit,
            "GQL_SUB_EXISTS_PROBE_EDGE",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    let result = engine
        .execute_gql(
            "MATCH (source:GqlSubExistsProbeEdgeSource) \
             WHERE EXISTS { \
               MATCH (source)-[:GQL_SUB_EXISTS_PROBE_EDGE]->(target:GqlSubExistsProbeEdgeHit) \
               RETURN target \
             } \
             RETURN elementKey(source)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_intermediate_bindings: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&result, 0), vec!["source"]);
}

#[test]
fn gql_optional_match_where_exists_preserves_left_outer_semantics() {
    let (_dir, engine) = query_test_engine();
    let pass = insert_query_node(&engine, "GqlOptExistsOuter", "pass", &[], 1.0);
    let pass_match = insert_query_node(&engine, "GqlOptExistsInner", "ok", &[], 1.0);
    let fail = insert_query_node(&engine, "GqlOptExistsOuter", "fail", &[], 1.0);
    let fail_match = insert_query_node(&engine, "GqlOptExistsInner", "bad", &[], 1.0);
    let _miss = insert_query_node(&engine, "GqlOptExistsOuter", "miss", &[], 1.0);
    let partial = insert_query_node(&engine, "GqlOptExistsOuter", "partial", &[], 1.0);
    let partial_good = insert_query_node(&engine, "GqlOptExistsInner", "good", &[], 1.0);
    let partial_bad = insert_query_node(&engine, "GqlOptExistsInner", "drop", &[], 1.0);
    let marker = insert_query_node(&engine, "GqlOptExistsMarker", "marker", &[], 1.0);

    for (from, to) in [
        (pass, pass_match),
        (fail, fail_match),
        (partial, partial_good),
        (partial, partial_bad),
    ] {
        engine
            .upsert_edge(from, to, "GQL_OPT_EXISTS_REL", UpsertEdgeOptions::default())
            .unwrap();
    }
    for from in [pass_match, partial_good] {
        engine
            .upsert_edge(
                from,
                marker,
                "GQL_OPT_EXISTS_MARK",
                UpsertEdgeOptions::default(),
            )
            .unwrap();
    }

    let result = engine
        .execute_gql(
            "MATCH (n:GqlOptExistsOuter) \
             OPTIONAL MATCH (n)-[:GQL_OPT_EXISTS_REL]->(m:GqlOptExistsInner) \
             WHERE EXISTS { MATCH (m)-[:GQL_OPT_EXISTS_MARK]->(marker:GqlOptExistsMarker) RETURN marker } \
             RETURN elementKey(n) AS outer_key, elementKey(m) AS inner_key \
             ORDER BY outer_key",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let rows = result
        .rows
        .iter()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec![GqlValue::String("fail".to_string()), GqlValue::Null],
            vec![GqlValue::String("miss".to_string()), GqlValue::Null],
            vec![
                GqlValue::String("partial".to_string()),
                GqlValue::String("good".to_string())
            ],
            vec![
                GqlValue::String("pass".to_string()),
                GqlValue::String("ok".to_string())
            ],
        ]
    );
    let plan = gql_read_explain(result.plan.as_ref().expect("plan"));
    assert!(plan.projection.iter().any(|item| {
        item.contains("optional_candidate_filter=true")
            && item.contains("optional_candidate_exists_predicates=1")
    }));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("synthesized_miss_rows=1")));

    let post_optional_filter = engine
        .execute_gql(
            "MATCH (n:GqlOptExistsOuter) \
             OPTIONAL MATCH (n)-[:GQL_OPT_EXISTS_REL]->(m:GqlOptExistsInner) \
             WITH n, m \
             WHERE EXISTS { MATCH (m)-[:GQL_OPT_EXISTS_MARK]->(marker:GqlOptExistsMarker) RETURN marker } \
             RETURN elementKey(n) AS outer_key, elementKey(m) AS inner_key \
             ORDER BY outer_key",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        post_optional_filter
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                GqlValue::String("partial".to_string()),
                GqlValue::String("good".to_string())
            ],
            vec![
                GqlValue::String("pass".to_string()),
                GqlValue::String("ok".to_string())
            ],
        ]
    );
}

#[test]
fn gql_optional_match_where_exists_reuses_canonical_candidate_cache() {
    let (_dir, engine) = query_test_engine();
    let outer = insert_query_node(&engine, "GqlOptExistsCacheOuter", "outer", &[], 1.0);
    let first = insert_query_node(
        &engine,
        "GqlOptExistsCacheInner",
        "first",
        &[("bucket", PropValue::String("hit".to_string()))],
        1.0,
    );
    let second = insert_query_node(
        &engine,
        "GqlOptExistsCacheInner",
        "second",
        &[("bucket", PropValue::String("hit".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlOptExistsCacheMarker",
        "marker",
        &[("bucket", PropValue::String("hit".to_string()))],
        1.0,
    );
    for target in [first, second] {
        engine
            .upsert_edge(
                outer,
                target,
                "GQL_OPT_EXISTS_CACHE_REL",
                UpsertEdgeOptions::default(),
            )
            .unwrap();
    }

    let result = engine
        .execute_gql(
            "MATCH (n:GqlOptExistsCacheOuter) \
             WITH n, 'hit' AS bucket \
             OPTIONAL MATCH (n)-[:GQL_OPT_EXISTS_CACHE_REL]->(m:GqlOptExistsCacheInner) \
             WHERE EXISTS { \
               MATCH (marker:GqlOptExistsCacheMarker) \
               WHERE marker.bucket = bucket \
               RETURN marker \
             } \
             RETURN elementKey(m) AS inner_key \
             ORDER BY inner_key",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&result, 0),
        vec!["first".to_string(), "second".to_string()]
    );
    let plan = gql_read_explain(result.plan.as_ref().expect("plan"));
    assert!(plan.projection.iter().any(|item| {
        item.contains("optional candidate filter")
            && item.contains("subquery_invocations=1")
            && item.contains("subquery_cache_hits=1")
    }));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("optional EXISTS subquery")));
}

#[test]
fn gql_read_only_call_subqueries_inner_apply_and_cursor() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "GqlSubCall", "a", &[], 1.0);
    let b = insert_query_node(&engine, "GqlSubCall", "b", &[], 1.0);
    let c = insert_query_node(&engine, "GqlSubCall", "c", &[], 1.0);
    engine
        .upsert_edge(a, b, "GQL_SUB_CALL_REL", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, "GQL_SUB_CALL_REL", UpsertEdgeOptions::default())
        .unwrap();

    let source = "MATCH (n:GqlSubCall) \
                  CALL { MATCH (n)-[:GQL_SUB_CALL_REL]->(m) RETURN m, elementKey(m) AS friend } \
                  RETURN elementKey(n) AS source, friend, id(m) AS mid \
                  ORDER BY source, friend";
    let options = GqlExecutionOptions {
        allow_full_scan: true,
        include_plan: true,
        ..gql_opts()
    };
    let result = engine
        .execute_gql(source, &GqlParams::new(), &options)
        .unwrap();
    assert_eq!(
        gql_string_column(&result, 0),
        vec!["a".to_string(), "a".to_string()]
    );
    assert_eq!(
        gql_string_column(&result, 1),
        vec!["b".to_string(), "c".to_string()]
    );
    assert_eq!(gql_u64_column(&result, 2), vec![b, c]);
    let plan = gql_read_explain(result.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("graph pipeline stage") && item.contains("Call")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("invocations=3")));

    let no_order_source = "MATCH (n:GqlSubCall) WHERE elementKey(n) = 'a' \
                           CALL { \
                             MATCH (n)-[:GQL_SUB_CALL_REL]->(m) \
                             RETURN elementKey(m) AS friend ORDER BY friend DESC \
                           } \
                           RETURN friend";
    let no_order = engine
        .execute_gql(no_order_source, &GqlParams::new(), &options)
        .unwrap();
    assert_eq!(
        gql_string_column(&no_order, 0),
        vec!["c".to_string(), "b".to_string()]
    );
    let first_no_order = engine
        .execute_gql(
            no_order_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&first_no_order, 0), vec!["c"]);
    let no_order_cursor = first_no_order
        .next_cursor
        .expect("first no-order CALL page should return cursor");
    let second_no_order = engine
        .execute_gql(
            no_order_source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                cursor: Some(no_order_cursor),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&second_no_order, 0), vec!["b"]);

    let first = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&first, 1), vec!["b"]);
    let cursor = first.next_cursor.expect("first page should return cursor");
    let second = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 1,
                cursor: Some(cursor),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&second, 1), vec!["c"]);
    assert!(second.next_cursor.is_none());
}

#[test]
fn gql_call_subquery_does_not_truncate_inner_rows_to_outer_page_cap() {
    let (_dir, engine) = query_test_engine();
    let outer = insert_query_node(&engine, "GqlSubCallPageOuter", "outer", &[], 1.0);
    for key in ["a", "b", "c"] {
        let inner = insert_query_node(&engine, "GqlSubCallPageInner", key, &[], 1.0);
        engine
            .upsert_edge(
                outer,
                inner,
                "GQL_SUB_CALL_PAGE_REL",
                UpsertEdgeOptions::default(),
            )
            .unwrap();
    }

    let source = "MATCH (n:GqlSubCallPageOuter) WHERE elementKey(n) = 'outer' \
                  CALL { \
                    MATCH (n)-[:GQL_SUB_CALL_PAGE_REL]->(m:GqlSubCallPageInner) \
                    RETURN elementKey(m) AS friend ORDER BY friend \
                  } \
                  RETURN friend ORDER BY friend";
    let first = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 2,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&first, 0),
        vec!["a".to_string(), "b".to_string()]
    );
    let cursor = first
        .next_cursor
        .expect("CALL rows beyond the outer page cap should remain pageable");
    let second = engine
        .execute_gql(
            source,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_rows: 2,
                cursor: Some(cursor),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&second, 0), vec!["c".to_string()]);
    assert!(second.next_cursor.is_none());
}

#[test]
fn gql_call_subquery_enforces_joined_cache_materialization_cap() {
    let (_dir, engine) = query_test_engine();
    for outer_key in ["outer-a", "outer-b", "outer-c"] {
        insert_query_node(
            &engine,
            "GqlSubCallCapOuter",
            outer_key,
            &[("bucket", PropValue::String("hit".to_string()))],
            1.0,
        );
    }
    for inner_key in ["inner-a", "inner-b"] {
        insert_query_node(
            &engine,
            "GqlSubCallCapInner",
            inner_key,
            &[("bucket", PropValue::String("hit".to_string()))],
            1.0,
        );
    }

    let err = engine
        .execute_gql(
            "MATCH (n:GqlSubCallCapOuter) \
             WITH n.bucket AS bucket, elementKey(n) AS source \
             CALL { \
               MATCH (m:GqlSubCallCapInner) \
               WHERE m.bucket = bucket \
               RETURN elementKey(m) AS friend ORDER BY friend \
             } \
             RETURN source, friend \
             ORDER BY source, friend",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_pipeline_rows: 5,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("max_pipeline_rows"),
        "unexpected CALL materialization cap error: {err:?}"
    );
}

#[test]
fn gql_exists_subquery_union_branches_see_correlated_imports_and_cache() {
    let (_dir, engine) = query_test_engine();
    for (key, bucket) in [
        ("left-a", "left"),
        ("left-b", "left"),
        ("right-a", "right"),
        ("miss-a", "miss"),
    ] {
        insert_query_node(
            &engine,
            "GqlSubUnionExistsOuter",
            key,
            &[("bucket", PropValue::String(bucket.to_string()))],
            1.0,
        );
    }
    insert_query_node(
        &engine,
        "GqlSubUnionExistsLeft",
        "left-marker",
        &[("bucket", PropValue::String("left".to_string()))],
        1.0,
    );
    insert_query_node(
        &engine,
        "GqlSubUnionExistsRight",
        "right-marker",
        &[("bucket", PropValue::String("right".to_string()))],
        1.0,
    );

    let result = engine
        .execute_gql(
            "MATCH (n:GqlSubUnionExistsOuter) \
             WITH n.bucket AS bucket, elementKey(n) AS key \
             WHERE EXISTS { \
               MATCH (left:GqlSubUnionExistsLeft) WHERE left.bucket = bucket RETURN left AS hit \
               UNION \
               MATCH (right:GqlSubUnionExistsRight) WHERE right.bucket = bucket RETURN right AS hit \
             } \
             RETURN key ORDER BY key",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&result, 0),
        vec![
            "left-a".to_string(),
            "left-b".to_string(),
            "right-a".to_string()
        ]
    );
    let plan = gql_read_explain(result.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_invocations=3")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("subquery_cache_hits=1")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("internal_limit=true")));
}

#[test]
fn gql_call_subquery_union_branches_see_correlated_imports_and_dedupe() {
    let (_dir, engine) = query_test_engine();
    let one = insert_query_node(&engine, "GqlSubUnionCallOuter", "one", &[], 1.0);
    let two = insert_query_node(&engine, "GqlSubUnionCallOuter", "two", &[], 1.0);
    insert_query_node(&engine, "GqlSubUnionCallOuter", "none", &[], 1.0);
    let alpha = insert_query_node(&engine, "GqlSubUnionCallInner", "alpha", &[], 1.0);
    let beta = insert_query_node(&engine, "GqlSubUnionCallInner", "beta", &[], 1.0);
    let gamma = insert_query_node(&engine, "GqlSubUnionCallInner", "gamma", &[], 1.0);

    for (from, to, label) in [
        (one, alpha, "GQL_SUB_UNION_CALL_A"),
        (one, alpha, "GQL_SUB_UNION_CALL_B"),
        (one, beta, "GQL_SUB_UNION_CALL_B"),
        (two, gamma, "GQL_SUB_UNION_CALL_B"),
    ] {
        engine
            .upsert_edge(from, to, label, UpsertEdgeOptions::default())
            .unwrap();
    }

    let result = engine
        .execute_gql(
            "MATCH (n:GqlSubUnionCallOuter) \
             CALL { \
               MATCH (n)-[:GQL_SUB_UNION_CALL_A]->(m:GqlSubUnionCallInner) \
               RETURN elementKey(m) AS friend, m AS friend_node \
               UNION \
               MATCH (n)-[:GQL_SUB_UNION_CALL_B]->(m:GqlSubUnionCallInner) \
               RETURN elementKey(m) AS friend, m AS friend_node \
             } \
             RETURN elementKey(n) AS source, friend, id(friend_node) AS friend_id \
             ORDER BY source, friend",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                GqlValue::String("one".to_string()),
                GqlValue::String("alpha".to_string()),
                GqlValue::UInt(alpha),
            ],
            vec![
                GqlValue::String("one".to_string()),
                GqlValue::String("beta".to_string()),
                GqlValue::UInt(beta),
            ],
            vec![
                GqlValue::String("two".to_string()),
                GqlValue::String("gamma".to_string()),
                GqlValue::UInt(gamma),
            ],
        ]
    );
    let plan = gql_read_explain(result.plan.as_ref().expect("plan"));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("graph pipeline stage") && item.contains("Call")));
    assert!(plan
        .projection
        .iter()
        .any(|item| item.contains("Union")));
}

#[test]
fn gql_call_subquery_mixed_union_output_cursor_resumes() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlSubUnionMixedCursorOuter", "outer", &[], 1.0);
    let source = insert_query_node(&engine, "GqlSubUnionMixedCursor", "node", &[], 1.0);
    let target = insert_query_node(&engine, "GqlSubUnionMixedCursor", "target", &[], 1.0);
    let edge = engine
        .upsert_edge(
            source,
            target,
            "GQL_SUB_UNION_MIXED_CURSOR_REL",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    for query in [
        "MATCH (outer:GqlSubUnionMixedCursorOuter) \
         CALL { \
           MATCH (m:GqlSubUnionMixedCursor) WHERE elementKey(m) = 'node' RETURN m AS mixed \
           UNION \
           MATCH (a:GqlSubUnionMixedCursor)-[r:GQL_SUB_UNION_MIXED_CURSOR_REL]->(b:GqlSubUnionMixedCursor) RETURN r AS mixed \
         } \
         RETURN mixed ORDER BY mixed",
        "MATCH (outer:GqlSubUnionMixedCursorOuter) \
         CALL { \
           MATCH (a:GqlSubUnionMixedCursor)-[r:GQL_SUB_UNION_MIXED_CURSOR_REL]->(b:GqlSubUnionMixedCursor) RETURN r AS mixed \
           UNION \
           MATCH (m:GqlSubUnionMixedCursor) WHERE elementKey(m) = 'node' RETURN m AS mixed \
         } \
         RETURN mixed ORDER BY mixed",
    ] {
        let first = engine
            .execute_gql(
                query,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    max_rows: 1,
                    ..gql_opts()
                },
        )
        .unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0].values[0], GqlValue::UInt(source));
        let cursor = first
            .next_cursor
            .clone()
            .expect("mixed CALL UNION first page should return cursor");
        let second = engine
            .execute_gql(
                query,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    max_rows: 1,
                    cursor: Some(cursor),
                    ..gql_opts()
                },
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].values[0], GqlValue::UInt(edge));
        assert!(second.next_cursor.is_none());
    }
}

#[test]
fn gql_read_only_subqueries_reject_mutation_collision_depth_and_caps() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(&engine, "GqlSubReject", "a", &[], 1.0);
    insert_query_node(&engine, "GqlSubReject", "b", &[], 1.0);

    for source in [
        "MATCH (n:GqlSubReject) WHERE EXISTS { CREATE (m) RETURN m } RETURN n",
        "MATCH (n:GqlSubReject) CALL { CREATE (m) RETURN m } RETURN n",
        "CREATE (n:GqlSubReject {elementKey: 'x'}) CALL { MATCH (m) RETURN m } RETURN n",
    ] {
        let err = engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::GqlUnsupported { .. }
                    | EngineError::GqlParse { .. }
                    | EngineError::GqlSemantic { .. }
            ),
            "expected subquery reject for {source}, got {err:?}"
        );
    }

    let collision = engine
        .execute_gql(
            "MATCH (n:GqlSubReject) \
             CALL { MATCH (n) RETURN n } \
             RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        collision.to_string().contains("collides"),
        "unexpected collision error: {collision:?}"
    );

    let branch_local_leak = engine
        .execute_gql(
            "MATCH (n:GqlSubReject) \
             WHERE EXISTS { \
               MATCH (m:GqlSubReject) RETURN m AS item \
               UNION \
               MATCH (x:GqlSubReject) WHERE elementKey(x) = elementKey(m) RETURN x AS item \
             } \
             RETURN n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        branch_local_leak.to_string().contains("unknown variable 'm'"),
        "unexpected branch-local alias leak error: {branch_local_leak:?}"
    );

    let left = insert_query_node(&engine, "GqlSubRejectMixedUnion", "left", &[], 1.0);
    let right = insert_query_node(&engine, "GqlSubRejectMixedUnion", "right", &[], 1.0);
    engine
        .upsert_edge(
            left,
            right,
            "GQL_SUB_REJECT_MIXED_UNION_REL",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    for source in [
        "MATCH (n:GqlSubReject) \
         CALL { \
           MATCH (m:GqlSubRejectMixedUnion) RETURN m AS mixed \
           UNION \
           MATCH (a:GqlSubRejectMixedUnion)-[r:GQL_SUB_REJECT_MIXED_UNION_REL]->(b:GqlSubRejectMixedUnion) RETURN r AS mixed \
         } \
         RETURN id(mixed)",
        "MATCH (n:GqlSubReject) \
         CALL { \
           MATCH (a:GqlSubRejectMixedUnion)-[r:GQL_SUB_REJECT_MIXED_UNION_REL]->(b:GqlSubRejectMixedUnion) RETURN r AS mixed \
           UNION \
           MATCH (m:GqlSubRejectMixedUnion) RETURN m AS mixed \
         } \
         RETURN id(mixed)",
    ] {
        let mixed_union_err = engine
            .execute_gql(
                source,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    allow_full_scan: true,
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(
            mixed_union_err
                .to_string()
                .contains("expects a node or edge alias"),
            "unexpected mixed union kind error: {mixed_union_err:?}"
        );
    }

    let nested = "MATCH (n:GqlSubReject) \
                  WHERE EXISTS { \
                    MATCH (m:GqlSubReject) \
                    WHERE EXISTS { MATCH (x:GqlSubReject) RETURN x } \
                    RETURN m \
                  } \
                  RETURN elementKey(n) AS key ORDER BY key";
    let ok = engine
        .execute_gql(
            nested,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_subquery_depth: 2,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&ok, 0),
        vec!["a".to_string(), "b".to_string()]
    );
    let depth_err = engine
        .execute_gql(
            nested,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_subquery_depth: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        depth_err.to_string().contains("max_subquery_depth"),
        "unexpected depth error: {depth_err:?}"
    );

    let cap_err = engine
        .execute_gql(
            "MATCH (n:GqlSubReject) \
             WHERE EXISTS { MATCH (n) RETURN n } \
             RETURN elementKey(n)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_subquery_invocations: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        cap_err.to_string().contains("max_subquery_invocations"),
        "unexpected cap error: {cap_err:?}"
    );

    let nested_cap_err = engine
        .execute_gql(
            nested,
            &GqlParams::new(),
            &GqlExecutionOptions {
                allow_full_scan: true,
                max_subquery_depth: 2,
                max_subquery_invocations: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        nested_cap_err
            .to_string()
            .contains("max_subquery_invocations"),
        "unexpected nested cap error: {nested_cap_err:?}"
    );
}

#[test]
fn gql_with_pipeline_executes_projection_and_seeded_match_stages() {
    let (_dir, engine) = query_test_engine();
    let ada = insert_query_node(
        &engine,
        "WithPerson",
        "with-pipeline-ada",
        &[
            ("name", PropValue::String("Ada".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.0,
    );
    let bob = insert_query_node(
        &engine,
        "WithPerson",
        "with-pipeline-bob",
        &[
            ("name", PropValue::String("Bob".to_string())),
            ("rank", PropValue::Int(2)),
        ],
        1.0,
    );
    let carol = insert_query_node(
        &engine,
        "WithPerson",
        "with-pipeline-carol",
        &[
            ("name", PropValue::String("Carol".to_string())),
            ("rank", PropValue::Int(3)),
        ],
        1.0,
    );
    let ada_topic = insert_query_node(
        &engine,
        "WithTopic",
        "with-pipeline-ada-topic",
        &[("name", PropValue::String("Graph".to_string()))],
        1.0,
    );
    let bob_topic = insert_query_node(
        &engine,
        "WithTopic",
        "with-pipeline-bob-topic",
        &[("name", PropValue::String("Rust".to_string()))],
        1.0,
    );
    engine
        .upsert_edge(ada, ada_topic, "WITH_PIPELINE_REL", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(bob, bob_topic, "WITH_PIPELINE_REL", UpsertEdgeOptions::default())
        .unwrap();

    let passthrough = execute_gql_ok(
        &engine,
        "MATCH (n:WithPerson) WITH n RETURN n ORDER BY id(n)",
    );
    let passthrough_ids = passthrough
        .rows
        .iter()
        .map(|row| gql_single_node(&row.values[0]).id.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(passthrough_ids, vec![ada, bob, carol]);

    let renamed = execute_gql_ok(
        &engine,
        "MATCH (n:WithPerson) WITH n AS x RETURN id(x) AS id ORDER BY id",
    );
    assert_eq!(gql_u64_column(&renamed, 0), vec![ada, bob, carol]);

    let scalar = execute_gql_ok(
        &engine,
        "MATCH (n:WithPerson) WITH n.name AS name \
         WHERE name STARTS WITH 'A' RETURN name ORDER BY name",
    );
    assert_eq!(gql_string_column(&scalar, 0), vec!["Ada".to_string()]);

    let repeated_star = execute_gql_ok(
        &engine,
        "MATCH (n:WithPerson) WITH n WITH * RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        gql_string_column(&repeated_star, 0),
        vec!["Ada".to_string(), "Bob".to_string(), "Carol".to_string()]
    );

    let seeded_required = execute_gql_ok(
        &engine,
        "MATCH (n:WithPerson) WITH n ORDER BY n.rank SKIP 1 LIMIT 1 \
         MATCH (n)-[:WITH_PIPELINE_REL]->(m:WithTopic) \
         RETURN n.name AS person, m.name AS topic",
    );
    assert_eq!(
        seeded_required.rows[0].values,
        vec![
            GqlValue::String("Bob".to_string()),
            GqlValue::String("Rust".to_string())
        ]
    );

    let seeded_optional = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (n:WithPerson) WHERE id(n) = {carol} WITH n \
             OPTIONAL MATCH (n)-[r:WITH_PIPELINE_REL]->(m:WithTopic) \
             RETURN id(n) AS n, id(r) AS r, id(m) AS m"
        ),
    );
    assert_eq!(
        seeded_optional.rows[0].values,
        vec![GqlValue::UInt(carol), GqlValue::Null, GqlValue::Null]
    );

    let null_seeded_required = execute_gql_ok(
        &engine,
        &format!(
            "MATCH (n:WithPerson) WHERE id(n) = {carol} \
             OPTIONAL MATCH (n)-[:WITH_PIPELINE_REL]->(m:WithTopic) \
             WITH m MATCH (m)-[:WITH_PIPELINE_REL]->(x) RETURN id(x) AS x"
        ),
    );
    assert!(null_seeded_required.rows.is_empty());
}

#[test]
fn gql_with_where_filters_after_projection_row_ops() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "WithWhereBarrier",
        "with-where-top-inactive",
        &[
            ("name", PropValue::String("top-inactive".to_string())),
            ("score", PropValue::Int(100)),
            ("active", PropValue::Bool(false)),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "WithWhereBarrier",
        "with-where-second-active",
        &[
            ("name", PropValue::String("second-active".to_string())),
            ("score", PropValue::Int(90)),
            ("active", PropValue::Bool(true)),
        ],
        1.0,
    );

    let top_then_filter = execute_gql_ok(
        &engine,
        "MATCH (n:WithWhereBarrier) \
         WITH n ORDER BY n.score DESC LIMIT 1 WHERE n.active \
         RETURN n.name AS name",
    );
    assert!(top_then_filter.rows.is_empty());

    let filter_then_top = execute_gql_ok(
        &engine,
        "MATCH (n:WithWhereBarrier) WHERE n.active \
         WITH n ORDER BY n.score DESC LIMIT 1 \
         RETURN n.name AS name",
    );
    assert_eq!(
        gql_string_column(&filter_then_top, 0),
        vec!["second-active".to_string()]
    );
}

#[test]
fn gql_with_pipeline_explain_reports_native_match_and_project_stages() {
    let (_dir, engine) = query_test_engine();
    let n = insert_query_node(
        &engine,
        "WithExplain",
        "with-explain-n",
        &[("name", PropValue::String("Ada".to_string()))],
        1.0,
    );
    let result = engine
        .execute_gql(
            "MATCH (n:WithExplain) WITH n.name AS name RETURN name ORDER BY name",
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                profile: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(result.rows[0].values, vec![GqlValue::String("Ada".to_string())]);
    let plan = result.plan.as_ref().expect("include_plan should return plan");
    let read = gql_read_explain(plan);
    assert_eq!(read.target, GqlLoweringTarget::GraphPipelineQuery);
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("graph pipeline stage 0: Match")));
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("Project(With)")));
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("nested graph row plan")));
    assert_eq!(gql_string_column(&result, 0), vec!["Ada".to_string()]);

    let explain = engine
        .explain_gql(
            &format!("MATCH (n:WithExplain) WHERE id(n) = {n} WITH n RETURN id(n) AS id"),
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    let read = gql_read_explain(&explain);
    assert_eq!(read.target, GqlLoweringTarget::GraphPipelineQuery);
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("graph pipeline stage")));
}

#[test]
fn gql_distinct_projection_deduplicates_scalars_and_visible_star_rows() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "DistinctScalar",
        "int-one",
        &[
            ("score", PropValue::Int(1)),
            ("flag", PropValue::Bool(true)),
            ("name", PropValue::String("Ada".to_string())),
            ("bytes", PropValue::Bytes(vec![1, 2])),
        ],
        1.0,
    );
    insert_query_node(
        &engine,
        "DistinctScalar",
        "uint-one",
        &[("score", PropValue::UInt(1)), ("flag", PropValue::Bool(true))],
        1.0,
    );
    insert_query_node(
        &engine,
        "DistinctScalar",
        "float-one",
        &[("score", PropValue::Float(1.0)), ("flag", PropValue::Bool(false))],
        1.0,
    );
    insert_query_node(
        &engine,
        "DistinctScalar",
        "two",
        &[("score", PropValue::Int(2)), ("flag", PropValue::Bool(false))],
        1.0,
    );

    let exact_numeric = execute_gql_ok(
        &engine,
        "MATCH (n:DistinctScalar) RETURN DISTINCT n.score AS score ORDER BY score",
    );
    assert_eq!(
        gql_u64_or_i64_values(&exact_numeric, 0),
        vec!["1".to_string(), "2".to_string()]
    );

    let scalar_domains = execute_gql_ok(
        &engine,
        "MATCH (n:DistinctScalar) RETURN DISTINCT n.flag AS flag ORDER BY flag",
    );
    assert_eq!(
        scalar_domains
            .rows
            .iter()
            .map(|row| row.values[0].clone())
            .collect::<Vec<_>>(),
        vec![GqlValue::Bool(false), GqlValue::Bool(true)]
    );
    let bytes = execute_gql_ok(
        &engine,
        "MATCH (n:DistinctScalar) RETURN DISTINCT n.bytes AS bytes",
    );
    assert!(bytes
        .rows
        .iter()
        .any(|row| row.values[0] == GqlValue::Bytes(vec![1, 2])));
    assert!(bytes.rows.iter().any(|row| row.values[0] == GqlValue::Null));

    let list_key = execute_gql_ok(
        &engine,
        "MATCH (n:DistinctScalar) RETURN DISTINCT [n.score] AS bucket",
    );
    assert_eq!(list_key.rows.len(), 2);
    let map_key = execute_gql_ok(
        &engine,
        "MATCH (n:DistinctScalar) RETURN DISTINCT {score: n.score} AS bucket",
    );
    assert_eq!(map_key.rows.len(), 2);

    let a = insert_query_node(&engine, "DistinctStar", "a", &[], 1.0);
    let b = insert_query_node(&engine, "DistinctStar", "b", &[], 1.0);
    engine
        .upsert_edge(a, b, "DISTINCT_STAR_A", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "DISTINCT_STAR_B", UpsertEdgeOptions::default())
        .unwrap();

    let return_star = execute_gql_ok(&engine, "MATCH (a:DistinctStar)-[]->(b) RETURN DISTINCT *");
    assert_eq!(return_star.rows.len(), 1);
    assert_eq!(return_star.rows, return_star_id_rows(a, b));

    let with_star = execute_gql_ok(
        &engine,
        "MATCH (a:DistinctStar)-[]->(b) WITH DISTINCT * RETURN id(a), id(b)",
    );
    assert_eq!(with_star.rows, return_star_id_rows(a, b));
}

#[test]
fn gql_distinct_projection_handles_graph_identity_values_and_caps() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "DistinctGraph", "a", &[], 1.0);
    let b = insert_query_node(&engine, "DistinctGraph", "b", &[], 1.0);
    let c = insert_query_node(&engine, "DistinctGraph", "c", &[], 1.0);
    let ab = engine
        .upsert_edge(a, b, "DISTINCT_GRAPH_A", UpsertEdgeOptions::default())
        .unwrap();
    let ac = engine
        .upsert_edge(a, c, "DISTINCT_GRAPH_B", UpsertEdgeOptions::default())
        .unwrap();

    let graph_identity_options = GqlExecutionOptions {
        allow_full_scan: true,
        ..gql_opts()
    };
    let nodes = execute_gql_with_options(
        &engine,
        &format!("MATCH (a:DistinctGraph)-[]->(b) WHERE id(a) = {a} RETURN DISTINCT a"),
        graph_identity_options.clone(),
    );
    assert_eq!(nodes.rows.len(), 1);
    assert_eq!(gql_single_node(&nodes.rows[0].values[0]).id, Some(a));

    let edges = execute_gql_with_options(
        &engine,
        &format!(
            "MATCH (a:DistinctGraph)-[r]->(b) WHERE id(a) = {a} RETURN DISTINCT r ORDER BY id(r)"
        ),
        graph_identity_options.clone(),
    );
    assert_eq!(
        edges
            .rows
            .iter()
            .map(|row| gql_single_edge(&row.values[0]).id.unwrap())
            .collect::<Vec<_>>(),
        vec![ab, ac]
    );

    let paths = execute_gql_with_options(
        &engine,
        &format!("MATCH p = (a:DistinctGraph)-[]->(b) WHERE id(a) = {a} RETURN DISTINCT p ORDER BY p"),
        graph_identity_options,
    );
    assert_eq!(
        paths
            .rows
            .iter()
            .map(|row| gql_single_path(&row.values[0]).edge_ids.clone())
            .collect::<Vec<_>>(),
        vec![vec![ab], vec![ac]]
    );

    let err = engine
        .execute_gql(
            "MATCH (n:DistinctGraph) RETURN DISTINCT n",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_groups: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("max_groups"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn gql_aggregate_projection_executes_grouped_global_and_null_semantics() {
    let (_dir, engine) = query_test_engine();
    for (key, kind, score, name) in [
        ("a", "x", PropValue::Int(1), "Ada"),
        ("b", "x", PropValue::UInt(2), "Bob"),
        ("c", "y", PropValue::Float(3.5), "Cy"),
        ("d", "y", PropValue::Null, "Dee"),
        ("e", "z", PropValue::Int(-1), "Eve"),
    ] {
        insert_query_node(
            &engine,
            "AggPerson",
            key,
            &[
                ("kind", PropValue::String(kind.to_string())),
                ("score", score),
                ("name", PropValue::String(name.to_string())),
            ],
            1.0,
        );
    }

    let grouped = execute_gql_ok(
        &engine,
        "MATCH (n:AggPerson) RETURN n.kind AS k, count(*) AS c ORDER BY k",
    );
    assert_eq!(
        grouped.rows,
        vec![
            GqlRow {
                values: vec![GqlValue::String("x".to_string()), GqlValue::UInt(2)]
            },
            GqlRow {
                values: vec![GqlValue::String("y".to_string()), GqlValue::UInt(2)]
            },
            GqlRow {
                values: vec![GqlValue::String("z".to_string()), GqlValue::UInt(1)]
            },
        ]
    );

    let global = execute_gql_ok(
        &engine,
        "MATCH (n:AggPerson) RETURN count(*) AS rows, count(n.score) AS scored, sum(n.score) AS sum, avg(n.score) AS avg, min(n.score) AS min, max(n.score) AS max",
    );
    assert_eq!(global.rows.len(), 1);
    assert_eq!(global.rows[0].values[0], GqlValue::UInt(5));
    assert_eq!(global.rows[0].values[1], GqlValue::UInt(4));
    assert_eq!(global.rows[0].values[2], GqlValue::Float(5.5));
    assert_eq!(global.rows[0].values[3], GqlValue::Float(1.375));
    assert_eq!(global.rows[0].values[4], GqlValue::Int(-1));
    assert_eq!(global.rows[0].values[5], GqlValue::Float(3.5));

    let zero_global = execute_gql_ok(
        &engine,
        "MATCH (n:AggMissing) RETURN count(*) AS rows, count(n.score) AS scored, sum(n.score) AS sum, avg(n.score) AS avg, min(n.score) AS min, max(n.score) AS max, collect(n.score) AS values",
    );
    assert_eq!(
        zero_global.rows[0].values,
        vec![
            GqlValue::UInt(0),
            GqlValue::UInt(0),
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::List(Vec::new()),
        ]
    );

    let zero_grouped = execute_gql_ok(
        &engine,
        "MATCH (n:AggMissing) RETURN n.kind AS k, count(*) AS rows",
    );
    assert!(zero_grouped.rows.is_empty());

    let only_nulls = execute_gql_ok(
        &engine,
        "MATCH (n:AggPerson) WHERE n.score IS NULL RETURN count(n.score), sum(n.score), avg(n.score), min(n.score), max(n.score), collect(n.score)",
    );
    assert_eq!(
        only_nulls.rows[0].values,
        vec![
            GqlValue::UInt(0),
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::Null,
            GqlValue::List(Vec::new()),
        ]
    );
}

#[test]
fn gql_aggregate_projection_supports_distinct_collect_alias_filter_and_order() {
    let (_dir, engine) = query_test_engine();
    for (key, kind, name) in [
        ("a", "x", "Ada"),
        ("b", "x", "Bob"),
        ("c", "x", "Ada"),
        ("d", "y", "Cy"),
        ("e", "y", "Cy"),
        ("f", "z", "Zed"),
    ] {
        insert_query_node(
            &engine,
            "AggCollect",
            key,
            &[
                ("kind", PropValue::String(kind.to_string())),
                ("name", PropValue::String(name.to_string())),
            ],
            1.0,
        );
    }

    let collect = execute_gql_ok(
        &engine,
        "MATCH (n:AggCollect) RETURN collect(n.name) AS names, collect(DISTINCT n.name) AS unique_names, count(DISTINCT n.name) AS unique_count",
    );
    assert_eq!(
        collect.rows[0].values,
        vec![
            GqlValue::List(vec![
                GqlValue::String("Ada".to_string()),
                GqlValue::String("Bob".to_string()),
                GqlValue::String("Ada".to_string()),
                GqlValue::String("Cy".to_string()),
                GqlValue::String("Cy".to_string()),
                GqlValue::String("Zed".to_string()),
            ]),
            GqlValue::List(vec![
                GqlValue::String("Ada".to_string()),
                GqlValue::String("Bob".to_string()),
                GqlValue::String("Cy".to_string()),
                GqlValue::String("Zed".to_string()),
            ]),
            GqlValue::UInt(4),
        ]
    );

    let alias_filter = execute_gql_ok(
        &engine,
        "MATCH (n:AggCollect) WITH n.kind AS k, count(*) AS c WHERE c > 1 RETURN k, c ORDER BY k",
    );
    assert_eq!(
        alias_filter.rows,
        vec![
            GqlRow {
                values: vec![GqlValue::String("x".to_string()), GqlValue::UInt(3)]
            },
            GqlRow {
                values: vec![GqlValue::String("y".to_string()), GqlValue::UInt(2)]
            },
        ]
    );

    let ordered = execute_gql_ok(
        &engine,
        "MATCH (n:AggCollect) RETURN n.kind AS k, count(*) AS c ORDER BY count(*) DESC, k ASC",
    );
    assert_eq!(
        ordered
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![GqlValue::String("x".to_string()), GqlValue::UInt(3)],
            vec![GqlValue::String("y".to_string()), GqlValue::UInt(2)],
            vec![GqlValue::String("z".to_string()), GqlValue::UInt(1)],
        ]
    );

    let scalar_expr = execute_gql_ok(
        &engine,
        "MATCH (n:AggCollect) RETURN count(*) + 1 AS total",
    );
    assert_eq!(scalar_expr.rows[0].values[0], GqlValue::Int(7));

    let max_collect = engine
        .execute_gql(
            "MATCH (n:AggCollect) RETURN collect(n.name)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_collect_items: 2,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        max_collect.to_string().contains("max_collect_items"),
        "unexpected error: {max_collect:?}"
    );
}

#[test]
fn gql_aggregate_projection_enforces_numeric_domain_and_group_caps() {
    let (_dir, engine) = query_test_engine();
    insert_query_node(
        &engine,
        "AggOverflow",
        "a",
        &[("score", PropValue::Int(i64::MAX))],
        1.0,
    );
    insert_query_node(
        &engine,
        "AggOverflow",
        "b",
        &[("score", PropValue::Int(1))],
        1.0,
    );
    let overflow = engine
        .execute_gql(
            "MATCH (n:AggOverflow) RETURN sum(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(overflow.to_string().contains("overflow"));

    insert_query_node(
        &engine,
        "AggTooLargeUnsigned",
        "a",
        &[("score", PropValue::UInt(i64::MAX as u64 + 1))],
        1.0,
    );
    let unsigned = engine
        .execute_gql(
            "MATCH (n:AggTooLargeUnsigned) RETURN sum(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(unsigned.to_string().contains("unsigned value"));

    insert_query_node(
        &engine,
        "AggTooLargeUnsignedAfterFloat",
        "a",
        &[("score", PropValue::Float(1.0))],
        1.0,
    );
    insert_query_node(
        &engine,
        "AggTooLargeUnsignedAfterFloat",
        "b",
        &[("score", PropValue::UInt(i64::MAX as u64 + 1))],
        1.0,
    );
    let unsigned_after_float = engine
        .execute_gql(
            "MATCH (n:AggTooLargeUnsignedAfterFloat) RETURN sum(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(unsigned_after_float
        .to_string()
        .contains("unsigned value"));

    insert_query_node(
        &engine,
        "AggTooLargeUnsignedBeforeFloat",
        "a",
        &[("score", PropValue::UInt(i64::MAX as u64 + 1))],
        1.0,
    );
    insert_query_node(
        &engine,
        "AggTooLargeUnsignedBeforeFloat",
        "b",
        &[("score", PropValue::Float(1.0))],
        1.0,
    );
    let unsigned_before_float = engine
        .execute_gql(
            "MATCH (n:AggTooLargeUnsignedBeforeFloat) RETURN sum(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(unsigned_before_float
        .to_string()
        .contains("unsigned value"));

    insert_query_node(
        &engine,
        "AggNonFinite",
        "a",
        &[("score", PropValue::Float(f64::NAN))],
        1.0,
    );
    let non_finite = engine
        .execute_gql(
            "MATCH (n:AggNonFinite) RETURN avg(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(non_finite.to_string().contains("finite"));

    insert_query_node(
        &engine,
        "AggNonExactAvg",
        "a",
        &[("score", PropValue::UInt(9_007_199_254_740_993))],
        1.0,
    );
    let non_exact_avg = engine
        .execute_gql(
            "MATCH (n:AggNonExactAvg) RETURN avg(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        non_exact_avg.to_string().contains("exactly as float"),
        "unexpected error: {non_exact_avg:?}"
    );

    insert_query_node(
        &engine,
        "AggNonExactMixedSum",
        "a",
        &[("score", PropValue::Float(0.5))],
        1.0,
    );
    insert_query_node(
        &engine,
        "AggNonExactMixedSum",
        "b",
        &[("score", PropValue::Int(9_007_199_254_740_993))],
        1.0,
    );
    let non_exact_mixed_sum = engine
        .execute_gql(
            "MATCH (n:AggNonExactMixedSum) RETURN sum(n.score)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        non_exact_mixed_sum.to_string().contains("exactly as float"),
        "unexpected error: {non_exact_mixed_sum:?}"
    );

    insert_query_node(
        &engine,
        "AggMinMaxMixed",
        "a",
        &[("value", PropValue::Bool(true))],
        1.0,
    );
    insert_query_node(
        &engine,
        "AggMinMaxMixed",
        "b",
        &[("value", PropValue::String("x".to_string()))],
        1.0,
    );
    let mixed = engine
        .execute_gql(
            "MATCH (n:AggMinMaxMixed) RETURN min(n.value)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(mixed.to_string().contains("incompatible"));

    for (key, group) in [("a", "x"), ("b", "y")] {
        insert_query_node(
            &engine,
            "AggGroupCap",
            key,
            &[("group", PropValue::String(group.to_string()))],
            1.0,
        );
    }
    let grouped_cap = engine
        .execute_gql(
            "MATCH (n:AggGroupCap) RETURN n.group, count(*)",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_groups: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(grouped_cap.to_string().contains("max_groups"));
}

#[test]
fn gql_union_all_and_union_execute_with_branch_order_and_explain() {
    let (_dir, engine) = query_test_engine();
    for (key, side, name) in [
        ("left-a", "left", "a"),
        ("left-b", "left", "b"),
        ("right-b", "right", "b"),
        ("right-c", "right", "c"),
        ("right-d", "right", "d"),
    ] {
        insert_query_node(
            &engine,
            "GqlUnionRows",
            key,
            &[
                ("side", PropValue::String(side.to_string())),
                ("name", PropValue::String(name.to_string())),
            ],
            1.0,
        );
    }

    let all_query = "\
        MATCH (n:GqlUnionRows) WHERE n.side = 'left' RETURN n.name AS name ORDER BY name DESC \
        UNION ALL \
        MATCH (m:GqlUnionRows) WHERE m.side = 'right' RETURN m.name AS name ORDER BY name ASC SKIP 1 LIMIT 2";
    let all = engine
        .execute_gql(
            all_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&all, 0),
        vec!["b".to_string(), "a".to_string(), "c".to_string(), "d".to_string()]
    );
    let read = all.plan.as_ref().map(gql_read_explain).unwrap();
    assert!(read
        .projection
        .iter()
        .any(|entry| entry.contains("UnionAll") && entry.contains("branches=2")));
    assert!(read
        .projection
        .iter()
        .any(|entry| entry.contains("branch 1 stages: Match")));
    assert!(read
        .projection
        .iter()
        .any(|entry| entry.contains("branch 2 row op: Sort")));
    assert!(read
        .projection
        .iter()
        .any(|entry| entry.contains("branch 2 row op: Skip")));
    assert!(read
        .projection
        .iter()
        .any(|entry| entry.contains("branch 2 row op: Limit")));

    let dedupe_query = "\
        MATCH (n:GqlUnionRows) WHERE n.side = 'left' RETURN n.name AS name ORDER BY name DESC \
        UNION \
        MATCH (m:GqlUnionRows) WHERE m.side = 'right' RETURN m.name AS name ORDER BY name ASC";
    let dedupe = execute_gql_ok(&engine, dedupe_query);
    assert_eq!(
        gql_string_column(&dedupe, 0),
        vec!["b".to_string(), "a".to_string(), "c".to_string(), "d".to_string()]
    );

    let mixed_node = insert_query_node(
        &engine,
        "GqlUnionMixed",
        "node",
        &[("name", PropValue::String("node".to_string()))],
        1.0,
    );
    let mixed_node_two = insert_query_node(
        &engine,
        "GqlUnionMixed",
        "node-two",
        &[("name", PropValue::String("node-two".to_string()))],
        1.0,
    );
    let mixed = execute_gql_ok(
        &engine,
        "\
        MATCH (n:GqlUnionMixed) RETURN 'literal' AS value LIMIT 1 \
        UNION ALL \
        MATCH (m:GqlUnionMixed) RETURN m AS value",
    );
    assert_eq!(mixed.rows[0].values[0], GqlValue::String("literal".to_string()));
    assert_eq!(gql_single_node(&mixed.rows[1].values[0]).id, Some(mixed_node));

    for mixed_query in [
        "\
        MATCH (n:GqlUnionMixed) RETURN 'literal' AS value LIMIT 1 \
        UNION ALL \
        MATCH (m:GqlUnionMixed) RETURN m AS value",
        "\
        MATCH (n:GqlUnionMixed) RETURN 'literal' AS value LIMIT 1 \
        UNION \
        MATCH (m:GqlUnionMixed) RETURN m AS value",
    ] {
        let first = engine
            .execute_gql(
                mixed_query,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    max_rows: 2,
                    ..gql_opts()
                },
            )
            .unwrap();
        assert_eq!(first.rows.len(), 2);
        assert!(first.next_cursor.is_some());
        assert_eq!(gql_single_node(&first.rows[1].values[0]).id, Some(mixed_node));
        let second = engine
            .execute_gql(
                mixed_query,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    max_rows: 2,
                    cursor: first.next_cursor.clone(),
                    ..gql_opts()
                },
            )
            .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(
            gql_single_node(&second.rows[0].values[0]).id,
            Some(mixed_node_two)
        );
    }
}

#[test]
fn gql_union_caps_cursors_snapshot_and_branch_failure_are_deterministic() {
    let (_dir, engine) = query_test_engine();
    for (key, label, name) in [
        ("left-a", "GqlUnionCursorLeft", "a"),
        ("left-b", "GqlUnionCursorLeft", "b"),
        ("right-b", "GqlUnionCursorRight", "b"),
        ("right-c", "GqlUnionCursorRight", "c"),
    ] {
        insert_query_node(
            &engine,
            label,
            key,
            &[("name", PropValue::String(name.to_string()))],
            1.0,
        );
    }

    let all_query = "\
        MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name ORDER BY name \
        UNION ALL \
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS name ORDER BY name";
    let first = engine
        .execute_gql(
            all_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&first, 0),
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(first.next_cursor.is_some());
    let second = engine
        .execute_gql(
            all_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                cursor: first.next_cursor.clone(),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&second, 0),
        vec!["b".to_string(), "c".to_string()]
    );
    let union_all_cursor = first.next_cursor.clone().unwrap();
    for changed_query in [
        "\
        MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name ORDER BY name \
        UNION \
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS name ORDER BY name",
        "\
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS name ORDER BY name \
        UNION ALL \
        MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name ORDER BY name",
        "\
        MATCH (n:GqlUnionCursorLeft) RETURN n.name AS other ORDER BY other \
        UNION ALL \
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS other ORDER BY other",
        "\
        MATCH (n:GqlUnionCursorLeft) WHERE n.name <> 'z' RETURN n.name AS name ORDER BY name \
        UNION ALL \
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS name ORDER BY name",
    ] {
        let err = engine
            .execute_gql(
                changed_query,
                &GqlParams::new(),
                &GqlExecutionOptions {
                    max_rows: 2,
                    cursor: Some(union_all_cursor.clone()),
                    ..gql_opts()
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, EngineError::InvalidCursor { .. }),
            "expected invalid cursor for changed union query, got {err:?}"
        );
    }

    let param_query = "\
        MATCH (n:GqlUnionCursorLeft) WHERE n.name >= $min RETURN n.name AS name ORDER BY name \
        UNION ALL \
        MATCH (m:GqlUnionCursorRight) WHERE m.name >= $min RETURN m.name AS name ORDER BY name";
    let param_first = engine
        .execute_gql(
            param_query,
            &GqlParams::from([("min".to_string(), GqlParamValue::String("a".to_string()))]),
            &GqlExecutionOptions {
                max_rows: 2,
                ..gql_opts()
            },
        )
        .unwrap();
    let param_err = engine
        .execute_gql(
            param_query,
            &GqlParams::from([("min".to_string(), GqlParamValue::String("b".to_string()))]),
            &GqlExecutionOptions {
                max_rows: 2,
                cursor: param_first.next_cursor.clone(),
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(
        matches!(param_err, EngineError::InvalidCursor { .. }),
        "expected invalid cursor for changed union params, got {param_err:?}"
    );

    let dedupe_query = "\
        MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name ORDER BY name \
        UNION \
        MATCH (m:GqlUnionCursorRight) RETURN m.name AS name ORDER BY name";
    let dedupe_first = engine
        .execute_gql(
            dedupe_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(
        gql_string_column(&dedupe_first, 0),
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(dedupe_first.next_cursor.is_some());
    let dedupe_second = engine
        .execute_gql(
            dedupe_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 2,
                cursor: dedupe_first.next_cursor.clone(),
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(gql_string_column(&dedupe_second, 0), vec!["c".to_string()]);

    let branch_cap = engine
        .execute_gql(
            "\
            MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name \
            UNION ALL MATCH (m:GqlUnionCursorRight) RETURN m.name AS name \
            UNION ALL MATCH (x:GqlUnionCursorLeft) RETURN x.name AS name",
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_union_branches: 2,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(branch_cap.to_string().contains("max_union_branches"));

    let dedupe_cap = engine
        .execute_gql(
            dedupe_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_groups: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(dedupe_cap.to_string().contains("max_groups"));

    let failure = engine
        .execute_gql(
            "\
            MATCH (n:GqlUnionCursorLeft) RETURN n.name AS name \
            UNION ALL \
            MATCH (m:GqlUnionCursorRight) RETURN 1 / 0 AS name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(failure.to_string().contains("division by zero"));
}

#[test]
fn gql_distinct_and_aggregate_cursors_are_shape_checked() {
    let (_dir, engine) = query_test_engine();
    for (key, group) in [("a", "a"), ("b", "b"), ("c", "c")] {
        insert_query_node(
            &engine,
            "AggCursor",
            key,
            &[("group", PropValue::String(group.to_string()))],
            1.0,
        );
    }

    let distinct_query = "MATCH (n:AggCursor) RETURN DISTINCT n.group AS g ORDER BY g";
    let distinct_first = engine
        .execute_gql(
            distinct_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 1,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(distinct_first.rows[0].values[0], GqlValue::String("a".to_string()));
    let distinct_cursor = distinct_first.next_cursor.clone().unwrap();
    let distinct_second = engine
        .execute_gql(
            distinct_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(distinct_cursor.clone()),
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(distinct_second.rows[0].values[0], GqlValue::String("b".to_string()));
    assert!(distinct_first
        .plan
        .as_ref()
        .map(gql_read_explain)
        .unwrap()
        .projection
        .iter()
        .any(|item| item.contains("distinct=true")));
    let distinct_shape_err = engine
        .execute_gql(
            "MATCH (n:AggCursor) RETURN DISTINCT n.group AS g ORDER BY g DESC",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(distinct_cursor),
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(distinct_shape_err, EngineError::InvalidCursor { .. }));

    let aggregate_query =
        "MATCH (n:AggCursor) RETURN n.group AS g, count(*) AS c ORDER BY g";
    let aggregate_first = engine
        .execute_gql(
            aggregate_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                max_rows: 1,
                include_plan: true,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(aggregate_first.rows[0].values[0], GqlValue::String("a".to_string()));
    let aggregate_cursor = aggregate_first.next_cursor.clone().unwrap();
    let aggregate_second = engine
        .execute_gql(
            aggregate_query,
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(aggregate_cursor.clone()),
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap();
    assert_eq!(aggregate_second.rows[0].values[0], GqlValue::String("b".to_string()));
    let read = aggregate_first
        .plan
        .as_ref()
        .map(gql_read_explain)
        .unwrap();
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("aggregate=true")));
    assert!(read
        .projection
        .iter()
        .any(|item| item.contains("aggregate calls")));
    let aggregate_shape_err = engine
        .execute_gql(
            "MATCH (n:AggCursor) RETURN n.group AS g, count(*) AS c ORDER BY c",
            &GqlParams::new(),
            &GqlExecutionOptions {
                cursor: Some(aggregate_cursor),
                max_rows: 1,
                ..gql_opts()
            },
        )
        .unwrap_err();
    assert!(matches!(aggregate_shape_err, EngineError::InvalidCursor { .. }));
}

#[test]
fn gql_mutation_return_aggregation_is_rejected() {
    let (_dir, engine) = query_test_engine();
    let err = engine
        .execute_gql(
            "CREATE (n:GqlAggregationRejected {elementKey: 'n'}) RETURN count(*)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::GqlSemantic { .. }),
        "expected semantic rejection, got {err:?}"
    );
}

#[test]
fn gql_mutation_read_after_write_stages_remain_rejected() {
    let (_dir, engine) = query_test_engine();
    for source in [
        "CREATE (n:Person {elementKey: 'with-after-write'}) WITH n RETURN n",
        "CREATE (n:Person {elementKey: 'match-after-write'}) MATCH (n) RETURN n",
        "CREATE (n:Person {elementKey: 'call-after-write'}) CALL { MATCH (m) RETURN m } RETURN n",
    ] {
        let err = engine
            .execute_gql(source, &GqlParams::new(), &gql_opts())
            .unwrap_err();
        assert!(
            matches!(err, EngineError::GqlUnsupported { .. }),
            "expected unsupported read-after-write mutation form for {source}, got {err:?}"
        );
    }
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
            "MATCH (n:Person) RETURN n ORDER BY labels(n)",
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
            "MATCH (n:Person) RETURN labels(n) AS ls ORDER BY ls",
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
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY nodeIds(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY edgeIds(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY nodes(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN p ORDER BY relationships(p)",
        "MATCH p = (a)-[:PATH_ORDER*1..1]->(b) RETURN edgeIds(p) AS ids ORDER BY ids",
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
            "MATCH (n:Person) RETURN elementKey(n) ORDER BY $bad",
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
            "MATCH (n:Person) WHERE elementKey(n) = 'missing-order-row' RETURN elementKey(n) ORDER BY $bad",
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
            "MATCH (n:Person) RETURN elementKey(n) ORDER BY $bad",
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
            "MATCH (n:Person) WHERE elementKey(n) = 'bytes-key' RETURN id(n) ORDER BY n.payload LIMIT 0",
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
        max_pipeline_rows: 11,
        max_groups: 13,
        max_collect_items: 15,
        max_union_branches: 3,
        max_subquery_invocations: 23,
        max_subquery_depth: 2,
        max_shortest_path_pairs: 29,
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
    assert_eq!(cap_summary.max_pipeline_rows, 11);
    assert_eq!(cap_summary.max_groups, 13);
    assert_eq!(cap_summary.max_collect_items, 15);
    assert_eq!(cap_summary.max_union_branches, 3);
    assert_eq!(cap_summary.max_subquery_invocations, 23);
    assert_eq!(cap_summary.max_subquery_depth, 2);
    assert_eq!(cap_summary.max_shortest_path_pairs, 29);
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
            "MATCH (n:Person) RETURN elementKey(n) ORDER BY id(n)",
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
            "MATCH ()-[r:LIKES]->() RETURN id(startNode(r)), id(endNode(r)), type(r), validFrom(r), validTo(r)",
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
        "MATCH ()-[r:LIKES]->() RETURN id(r), type(r), id(startNode(r)), id(endNode(r))",
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
        "MATCH (n:Person) WHERE elementKey(n) = 'reopen-keep' RETURN n.state ORDER BY n.state",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], GqlValue::String("new".to_string()));

    let all_keys = execute_gql_ok(&reopened, "MATCH (n:Person) RETURN elementKey(n) ORDER BY elementKey(n) LIMIT 10");
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
        "MATCH ()-[r:LIKES]->() RETURN id(r), type(r), r.id, r.label, id(startNode(r)), id(endNode(r))",
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

#[test]
fn gql_match_pattern_map_metadata_keys_filter_metadata_not_properties() {
    let (_dir, engine) = query_test_engine();
    execute_gql_ok(
        &engine,
        "CREATE (a:MapMetaNode {elementKey: 'map-meta-a', name: 'a'})",
    );
    execute_gql_ok(
        &engine,
        "CREATE (b:MapMetaNode {elementKey: 'map-meta-b', name: 'b', marker: 'map-meta-a'})",
    );

    // elementKey in a MATCH map filters the node key, not a property named elementKey.
    let by_key = execute_gql_ok(
        &engine,
        "MATCH (n:MapMetaNode {elementKey: 'map-meta-a'}) RETURN n.name",
    );
    assert_eq!(by_key.rows.len(), 1);
    assert_eq!(by_key.rows[0].values[0], GqlValue::String("a".to_string()));

    let missing = execute_gql_ok(
        &engine,
        "MATCH (n:MapMetaNode {elementKey: 'map-meta-missing'}) RETURN n",
    );
    assert!(missing.rows.is_empty());

    // Node weight in a MATCH map filters metadata via the residual predicate path
    // (no native node weight eq filter); default node weight is 1.0.
    let by_weight = execute_gql_ok(
        &engine,
        "MATCH (n:MapMetaNode {weight: 1.0}) RETURN elementKey(n) ORDER BY elementKey(n)",
    );
    assert_eq!(by_weight.rows.len(), 2);
    assert_eq!(
        by_weight.rows[0].values[0],
        GqlValue::String("map-meta-a".to_string())
    );

    execute_gql_ok(
        &engine,
        "MATCH (a:MapMetaNode {elementKey: 'map-meta-a'}) \
         MATCH (b:MapMetaNode {elementKey: 'map-meta-b'}) \
         CREATE (a)-[:MAP_META_EDGE {weight: 2.5, validFrom: 10, tag: 'x'}]->(b)",
    );

    // Edge map metadata keys filter edge metadata through native eq filters.
    let edge_hit = execute_gql_ok(
        &engine,
        "MATCH (a:MapMetaNode)-[r:MAP_META_EDGE {weight: 2.5, validFrom: 10}]->(b:MapMetaNode) \
         RETURN validTo(r), r.tag",
    );
    assert_eq!(edge_hit.rows.len(), 1);
    assert_eq!(edge_hit.rows[0].values[0], GqlValue::Int(i64::MAX));
    assert_eq!(edge_hit.rows[0].values[1], GqlValue::String("x".to_string()));

    let edge_miss = execute_gql_ok(
        &engine,
        "MATCH (a:MapMetaNode)-[r:MAP_META_EDGE {weight: 9.0}]->(b:MapMetaNode) RETURN id(r)",
    );
    assert!(edge_miss.rows.is_empty());

    // A user property spelled like a metadata map key is still reachable via dot access.
    let by_property = execute_gql_ok(
        &engine,
        "MATCH (n:MapMetaNode) WHERE n.marker = 'map-meta-a' RETURN elementKey(n)",
    );
    assert_eq!(by_property.rows.len(), 1);
    assert_eq!(
        by_property.rows[0].values[0],
        GqlValue::String("map-meta-b".to_string())
    );
}

#[test]
fn gql_element_map_snake_case_keys_are_user_properties() {
    let (_dir, engine) = query_test_engine();

    // snake_case spellings and the old reserved name `key` are plain user properties in
    // element maps; only the exact camelCase metadata names route to metadata.
    execute_gql_ok(
        &engine,
        "CREATE (a:SnakeMapNode {elementKey: 'snake-a', updated_at: 99, key: 'custom'})",
    );
    execute_gql_ok(&engine, "CREATE (b:SnakeMapNode {elementKey: 'snake-b'})");
    execute_gql_ok(
        &engine,
        "MATCH (a:SnakeMapNode {elementKey: 'snake-a'}) \
         MATCH (b:SnakeMapNode {elementKey: 'snake-b'}) \
         CREATE (a)-[:SNAKE_MAP_EDGE {valid_from: 10, valid_to: 20, weight: 2.5}]->(b)",
    );

    let node = execute_gql_ok(
        &engine,
        "MATCH (n:SnakeMapNode {elementKey: 'snake-a'}) \
         RETURN n.updated_at, n.key, updatedAt(n)",
    );
    assert_eq!(node.rows.len(), 1);
    assert_eq!(node.rows[0].values[0], GqlValue::Int(99));
    assert_eq!(node.rows[0].values[1], GqlValue::String("custom".to_string()));
    // Real metadata is the commit timestamp, untouched by the look-alike property.
    match &node.rows[0].values[2] {
        GqlValue::Int(ts) => assert!(*ts > 99, "updatedAt(n) must be a commit timestamp"),
        other => panic!("expected Int updatedAt, got {other:?}"),
    }

    // snake_case map keys in MATCH filter the properties, not edge validity metadata.
    let edge = execute_gql_ok(
        &engine,
        "MATCH (a:SnakeMapNode)-[r:SNAKE_MAP_EDGE {valid_from: 10}]->(b:SnakeMapNode) \
         RETURN r.valid_from, r.valid_to, validFrom(r), validTo(r), weight(r)",
    );
    assert_eq!(edge.rows.len(), 1);
    assert_eq!(edge.rows[0].values[0], GqlValue::Int(10));
    assert_eq!(edge.rows[0].values[1], GqlValue::Int(20));
    // Validity metadata kept its defaults: the edge is visible now and unbounded.
    assert_eq!(edge.rows[0].values[3], GqlValue::Int(i64::MAX));
    match &edge.rows[0].values[2] {
        GqlValue::Int(from) => assert!(*from < i64::MAX),
        other => panic!("expected Int validFrom, got {other:?}"),
    }
    // camelCase `weight` in the same map DID route to metadata.
    assert_eq!(edge.rows[0].values[4], GqlValue::Float(2.5));

    let edge_props_miss = execute_gql_ok(
        &engine,
        "MATCH (a:SnakeMapNode)-[r:SNAKE_MAP_EDGE {valid_from: 11}]->(b:SnakeMapNode) \
         RETURN id(r)",
    );
    assert!(edge_props_miss.rows.is_empty());
}

#[test]
fn gql_metadata_set_items_in_merge_actions_node_set_and_endpoint_return() {
    let (_dir, engine) = gql_create_test_engine_with_options(DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    });
    execute_gql_ok(&engine, "CREATE (a:MetaSetNode {elementKey: 'meta-set-a'})");
    execute_gql_ok(&engine, "CREATE (b:MetaSetNode {elementKey: 'meta-set-b'})");

    // Node metadata SET on a matched node, read back in the same mutation RETURN.
    let node_set = execute_gql_ok(
        &engine,
        "MATCH (n:MetaSetNode {elementKey: 'meta-set-a'}) SET weight(n) = 2.5 RETURN weight(n)",
    );
    assert_eq!(node_set.rows.len(), 1);
    assert_eq!(node_set.rows[0].values[0], GqlValue::Float(2.5));

    // Metadata l-values inside ON CREATE SET hit the created-edge target arm; unset
    // validity keeps its default (validTo = i64::MAX).
    let created = execute_gql_ok(
        &engine,
        "MATCH (a:MetaSetNode {elementKey: 'meta-set-a'}) \
         MATCH (b:MetaSetNode {elementKey: 'meta-set-b'}) \
         MERGE (a)-[r:META_SET_EDGE]->(b) \
         ON CREATE SET weight(r) = 0.5, validFrom(r) = 10 \
         RETURN weight(r), validFrom(r), validTo(r)",
    );
    assert_eq!(created.rows.len(), 1);
    assert_eq!(created.rows[0].values[0], GqlValue::Float(0.5));
    assert_eq!(created.rows[0].values[1], GqlValue::Int(10));
    assert_eq!(created.rows[0].values[2], GqlValue::Int(i64::MAX));
    assert_eq!(created.mutation_stats.as_ref().unwrap().edges_created, 1);

    // Same MERGE again: ON MATCH SET metadata l-values hit the matched-edge target arm.
    let matched = execute_gql_ok(
        &engine,
        "MATCH (a:MetaSetNode {elementKey: 'meta-set-a'}) \
         MATCH (b:MetaSetNode {elementKey: 'meta-set-b'}) \
         MERGE (a)-[r:META_SET_EDGE]->(b) \
         ON MATCH SET weight(r) = 1.5, validTo(r) = 20000000000000 \
         RETURN weight(r), validFrom(r), validTo(r)",
    );
    assert_eq!(matched.rows.len(), 1);
    assert_eq!(matched.rows[0].values[0], GqlValue::Float(1.5));
    assert_eq!(matched.rows[0].values[1], GqlValue::Int(10));
    assert_eq!(matched.rows[0].values[2], GqlValue::Int(20000000000000));
    assert_eq!(matched.mutation_stats.as_ref().unwrap().edges_created, 0);

    // Node MERGE ON MATCH SET metadata l-value on the matched node.
    let node_merge = execute_gql_ok(
        &engine,
        "MERGE (n:MetaSetNode {elementKey: 'meta-set-b'}) \
         ON MATCH SET weight(n) = 3.0 RETURN weight(n)",
    );
    assert_eq!(node_merge.rows.len(), 1);
    assert_eq!(node_merge.rows[0].values[0], GqlValue::Float(3.0));

    // Mutation RETURN projects endpoint ids of a matched edge.
    let ids = execute_gql_ok(
        &engine,
        "MATCH (a:MetaSetNode {elementKey: 'meta-set-a'}) \
         MATCH (b:MetaSetNode {elementKey: 'meta-set-b'}) \
         RETURN id(a), id(b)",
    );
    let (a_id, b_id) = match (&ids.rows[0].values[0], &ids.rows[0].values[1]) {
        (GqlValue::UInt(a), GqlValue::UInt(b)) => (*a, *b),
        other => panic!("expected node ids, got {other:?}"),
    };
    let endpoints = execute_gql_ok(
        &engine,
        "MATCH (a:MetaSetNode)-[r:META_SET_EDGE]->(b:MetaSetNode) \
         SET weight(r) = 0.75 \
         RETURN id(startNode(r)), id(endNode(r)), weight(r)",
    );
    assert_eq!(endpoints.rows.len(), 1);
    assert_eq!(endpoints.rows[0].values[0], GqlValue::UInt(a_id));
    assert_eq!(endpoints.rows[0].values[1], GqlValue::UInt(b_id));
    assert_eq!(endpoints.rows[0].values[2], GqlValue::Float(0.75));
}
