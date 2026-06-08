// Schema management API and existing-data validation tests.

use crate::{
    DenseMetric, DenseVectorConfig, DenseVectorSchema, EndpointLabelSchema, HnswConfig,
    EdgeValiditySchema, GraphSchema, GraphSchemaCheckOptions, GraphSchemaDropAction,
    GraphSchemaDropTargetResult, GraphSchemaOperation, GraphSchemaOperationKind,
    GraphSchemaSetOptions, NodeLabelConstraintSchema, NumericFieldSchema, PropertySchema,
    SchemaAdditionalProperties, SchemaNumericBound, SchemaTargetKind, SchemaValueType,
    SchemaVectorPresence, SparseVectorSchema, StringFieldSchema,
};

fn schema_props(entries: &[(&str, PropertySchema)]) -> BTreeMap<String, PropertySchema> {
    entries
        .iter()
        .map(|(key, schema)| ((*key).to_string(), schema.clone()))
        .collect()
}

fn schema_value_props(entries: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn assert_schema_violation(error: EngineError) {
    assert!(
        error.to_string().contains("schema violation:"),
        "expected schema violation, got {error}"
    );
}

fn assert_schema_violation_contains(error: EngineError, needles: &[&str]) {
    let message = error.to_string();
    assert!(
        message.contains("schema violation:"),
        "expected schema violation, got {message}"
    );
    for needle in needles {
        assert!(
            message.contains(needle),
            "expected schema violation to contain {needle:?}, got {message:?}"
        );
    }
}

fn assert_invalid_operation_contains(error: EngineError, needles: &[&str]) {
    match error {
        EngineError::InvalidOperation(message) => {
            for needle in needles {
                assert!(
                    message.contains(needle),
                    "expected InvalidOperation to contain {needle:?}, got {message:?}"
                );
            }
        }
        other => panic!("expected InvalidOperation, got {other}"),
    }
}

fn required_string_property() -> PropertySchema {
    PropertySchema {
        required: true,
        nullable: false,
        types: vec![SchemaValueType::String],
        ..Default::default()
    }
}

fn required_int_property() -> PropertySchema {
    PropertySchema {
        required: true,
        nullable: false,
        types: vec![SchemaValueType::Int],
        ..Default::default()
    }
}

fn node_schema_with_required_string(key: &str) -> NodeSchema {
    NodeSchema {
        properties: schema_props(&[(key, required_string_property())]),
        ..Default::default()
    }
}

fn closed_node_schema(keys: &[&str]) -> NodeSchema {
    NodeSchema {
        additional_properties: SchemaAdditionalProperties::Reject,
        properties: keys
            .iter()
            .map(|key| ((*key).to_string(), required_string_property()))
            .collect(),
        ..Default::default()
    }
}

fn edge_schema_with_required_int(key: &str) -> EdgeSchema {
    EdgeSchema {
        properties: schema_props(&[(key, required_int_property())]),
        ..Default::default()
    }
}

fn schema_node_input(
    label: &str,
    key: &str,
    props: BTreeMap<String, PropValue>,
) -> NodeInput {
    NodeInput {
        labels: vec![label.to_string()],
        key: key.to_string(),
        props,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
    }
}

fn schema_edge_input(
    from: u64,
    to: u64,
    label: &str,
    props: BTreeMap<String, PropValue>,
) -> EdgeInput {
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

fn schema_raw_node_record(
    id: u64,
    label_id: u32,
    key: &str,
    props: BTreeMap<String, PropValue>,
) -> NodeRecord {
    schema_raw_node_record_with_labels(id, &[label_id], key, props)
}

fn schema_raw_node_record_with_labels(
    id: u64,
    label_ids: &[u32],
    key: &str,
    props: BTreeMap<String, PropValue>,
) -> NodeRecord {
    NodeRecord {
        id,
        label_ids: NodeLabelSet::from_label_ids(label_ids.iter().copied()).unwrap(),
        key: key.to_string(),
        props,
        created_at: 1000 * id as i64,
        updated_at: 1000 * id as i64 + 1,
        weight: 1.0,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: 0,
    }
}

fn schema_raw_edge_record(id: u64, from: u64, to: u64, label_id: u32) -> EdgeRecord {
    EdgeRecord {
        id,
        from,
        to,
        label_id,
        props: BTreeMap::new(),
        created_at: 1000 * id as i64,
        updated_at: 1000 * id as i64 + 1,
        weight: 1.0,
        valid_from: 0,
        valid_to: i64::MAX,
        last_write_seq: 0,
    }
}

fn endpoint_labels(all_of: &[&str], any_of: &[&str], none_of: &[&str]) -> EndpointLabelSchema {
    EndpointLabelSchema {
        all_of: all_of.iter().map(|label| (*label).to_string()).collect(),
        any_of: any_of.iter().map(|label| (*label).to_string()).collect(),
        none_of: none_of.iter().map(|label| (*label).to_string()).collect(),
    }
}

fn endpoint_edge_schema(
    from_all_of: &[&str],
    from_any_of: &[&str],
    from_none_of: &[&str],
    to_all_of: &[&str],
    to_any_of: &[&str],
    to_none_of: &[&str],
) -> EdgeSchema {
    EdgeSchema {
        from: Some(endpoint_labels(
            from_all_of,
            from_any_of,
            from_none_of,
        )),
        to: Some(endpoint_labels(to_all_of, to_any_of, to_none_of)),
        ..Default::default()
    }
}

fn graph_schema_from_targets(
    nodes: &[(&str, NodeSchema)],
    edges: &[(&str, EdgeSchema)],
) -> GraphSchema {
    GraphSchema {
        node_schemas: nodes
            .iter()
            .map(|(label, schema)| NodeSchemaInfo {
                label: (*label).to_string(),
                schema: schema.clone(),
            })
            .collect(),
        edge_schemas: edges
            .iter()
            .map(|(label, schema)| EdgeSchemaInfo {
                label: (*label).to_string(),
                schema: schema.clone(),
            })
            .collect(),
    }
}

fn schema_catalog_labels(
    engine: &DatabaseEngine,
) -> Result<(Vec<String>, Vec<String>), EngineError> {
    Ok((
        engine
            .list_node_schemas()?
            .into_iter()
            .map(|info| info.label)
            .collect(),
        engine
            .list_edge_schemas()?
            .into_iter()
            .map(|info| info.label)
            .collect(),
    ))
}

fn dense_required_schema(dimension: Option<usize>) -> NodeSchema {
    NodeSchema {
        dense_vector: Some(DenseVectorSchema {
            presence: SchemaVectorPresence::Required,
            dimension,
        }),
        ..Default::default()
    }
}

fn run_schema_set_with_queued_write(
    engine: &DatabaseEngine,
    set_call: impl FnOnce(DatabaseEngine) -> Result<(), EngineError> + Send + 'static,
) -> Result<(), EngineError> {
    let (ready_rx, release_tx) = engine.set_schema_validation_pause();
    let setter = engine.clone();
    let set_handle = std::thread::spawn(move || set_call(setter));
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("schema set did not pause during validation");

    let writer = engine.clone();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let write_handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = writer
            .upsert_node("QueueProbe", "queued-write", UpsertNodeOptions::default())
            .map(|_| ());
        done_tx.send(()).unwrap();
        result
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("queued writer did not start");
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "queued writer completed while schema set still held the core write queue"
    );

    release_tx.send(()).unwrap();
    let set_result = set_handle.join().unwrap();
    write_handle.join().unwrap().unwrap();
    set_result
}

#[test]
fn schema_management_set_graph_schema_publishes_multi_target_atomically() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let person = engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let company = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            person,
            company,
            "WORKS_AT",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();

    let result = engine
        .set_graph_schema(
            graph_schema_from_targets(
                &[("Person", node_schema_with_required_string("name"))],
                &[("WORKS_AT", edge_schema_with_required_int("since"))],
            ),
            GraphSchemaSetOptions::default(),
        )
        .unwrap();

    assert_eq!(result.operation, GraphSchemaOperationKind::Set);
    assert_eq!(result.targets_published, 2);
    assert_eq!(result.targets_dropped, 0);
    assert!(result.drop_targets.is_empty());
    assert_eq!(result.node_schemas_dropped, 0);
    assert_eq!(result.edge_schemas_dropped, 0);
    assert_eq!(result.validation.checked_records, 2);
    assert_eq!(result.validation.violation_count, 0);
    assert_eq!(result.validation.entries.len(), 2);
    assert_eq!(
        schema_catalog_labels(&engine).unwrap(),
        (
            vec!["Person".to_string()],
            vec!["WORKS_AT".to_string()]
        )
    );
}

#[test]
fn schema_management_alter_graph_schema_add_preserves_unlisted_schemas() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("ExistingNode", NodeSchema::default())
        .unwrap();
    engine
        .set_edge_schema("EXISTING_EDGE", EdgeSchema::default())
        .unwrap();

    let result = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::SetNode {
                    label: "Person".to_string(),
                    schema: NodeSchema::default(),
                },
                GraphSchemaOperation::SetEdge {
                    label: "KNOWS".to_string(),
                    schema: EdgeSchema::default(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap();

    assert_eq!(result.operation, GraphSchemaOperationKind::Add);
    assert_eq!(result.targets_published, 2);
    assert_eq!(result.targets_dropped, 0);
    assert!(result.drop_targets.is_empty());
    assert_eq!(
        schema_catalog_labels(&engine).unwrap(),
        (
            vec!["ExistingNode".to_string(), "Person".to_string()],
            vec!["EXISTING_EDGE".to_string(), "KNOWS".to_string()]
        )
    );
}

#[test]
fn schema_management_failed_bulk_add_publishes_no_schema_or_new_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node("Violating", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::SetNode {
                    label: "FutureClean".to_string(),
                    schema: NodeSchema::default(),
                },
                GraphSchemaOperation::SetNode {
                    label: "Violating".to_string(),
                    schema: node_schema_with_required_string("name"),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();

    assert!(err.to_string().contains("schema publication rejected:"));
    assert!(engine.list_node_schemas().unwrap().is_empty());
    assert_eq!(engine.get_node_label_id("FutureClean").unwrap(), None);
}

#[test]
fn schema_management_set_graph_schema_drops_unlisted_only_on_success() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine.set_node_schema("Keep", NodeSchema::default()).unwrap();
    engine
        .set_node_schema("DropMe", NodeSchema::default())
        .unwrap();
    engine
        .upsert_node("Violating", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let err = engine
        .set_graph_schema(
            graph_schema_from_targets(
                &[
                    ("Keep", NodeSchema::default()),
                    ("Violating", node_schema_with_required_string("name")),
                ],
                &[],
            ),
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected:"));
    assert!(engine.get_node_schema("DropMe").unwrap().is_some());
    assert!(engine.get_node_schema("Violating").unwrap().is_none());

    let result = engine
        .set_graph_schema(
            graph_schema_from_targets(&[("Keep", NodeSchema::default())], &[]),
            GraphSchemaSetOptions::default(),
        )
        .unwrap();
    assert_eq!(result.targets_dropped, 1);
    assert!(result.drop_targets.is_empty());
    assert_eq!(result.node_schemas_dropped, 1);
    assert_eq!(result.edge_schemas_dropped, 0);
    assert!(engine.get_node_schema("Keep").unwrap().is_some());
    assert!(engine.get_node_schema("DropMe").unwrap().is_none());
}

#[test]
fn schema_management_set_graph_schema_default_removes_all_schemas() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine.set_node_schema("Person", NodeSchema::default()).unwrap();
    engine.set_edge_schema("KNOWS", EdgeSchema::default()).unwrap();

    let result = engine
        .set_graph_schema(GraphSchema::default(), GraphSchemaSetOptions::default())
        .unwrap();

    assert_eq!(result.operation, GraphSchemaOperationKind::Set);
    assert_eq!(result.targets_published, 0);
    assert_eq!(result.targets_dropped, 2);
    assert!(result.drop_targets.is_empty());
    assert_eq!(result.node_schemas_dropped, 1);
    assert_eq!(result.edge_schemas_dropped, 1);
    assert_eq!(result.validation.checked_records, 0);
    assert_eq!(schema_catalog_labels(&engine).unwrap(), (vec![], vec![]));
}

#[test]
fn schema_management_selected_drop_treats_missing_schemas_as_not_found() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("ExistingNode", NodeSchema::default())
        .unwrap();
    engine
        .set_edge_schema("ExistingEdge", EdgeSchema::default())
        .unwrap();

    let result = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::DropNode {
                    label: "ExistingNode".to_string(),
                },
                GraphSchemaOperation::DropNode {
                    label: "MissingNode".to_string(),
                },
                GraphSchemaOperation::DropEdge {
                    label: "ExistingEdge".to_string(),
                },
                GraphSchemaOperation::DropEdge {
                    label: "MissingEdge".to_string(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap();

    assert_eq!(result.operation, GraphSchemaOperationKind::Drop);
    assert_eq!(result.targets_dropped, 2);
    assert_eq!(result.node_schemas_dropped, 1);
    assert_eq!(result.edge_schemas_dropped, 1);
    assert_eq!(
        result.drop_targets,
        vec![
            GraphSchemaDropTargetResult {
                target_kind: SchemaTargetKind::Node,
                label: "ExistingNode".to_string(),
                action: GraphSchemaDropAction::Dropped,
            },
            GraphSchemaDropTargetResult {
                target_kind: SchemaTargetKind::Node,
                label: "MissingNode".to_string(),
                action: GraphSchemaDropAction::NotFound,
            },
            GraphSchemaDropTargetResult {
                target_kind: SchemaTargetKind::Edge,
                label: "ExistingEdge".to_string(),
                action: GraphSchemaDropAction::Dropped,
            },
            GraphSchemaDropTargetResult {
                target_kind: SchemaTargetKind::Edge,
                label: "MissingEdge".to_string(),
                action: GraphSchemaDropAction::NotFound,
            },
        ]
    );
    assert_eq!(
        schema_catalog_labels(&engine).unwrap(),
        (vec![], vec![])
    );
}

#[test]
fn schema_management_drop_graph_schema_removes_all_without_scanning_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine.set_node_schema("Person", NodeSchema::default()).unwrap();
    engine.set_edge_schema("KNOWS", EdgeSchema::default()).unwrap();
    engine
        .upsert_node("Person", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let result = engine.drop_graph_schema().unwrap();

    assert_eq!(result.operation, GraphSchemaOperationKind::DropAll);
    assert_eq!(result.targets_dropped, 2);
    assert_eq!(result.node_schemas_dropped, 1);
    assert_eq!(result.edge_schemas_dropped, 1);
    assert!(result.drop_targets.is_empty());
    assert_eq!(result.validation.checked_records, 0);
    assert_eq!(schema_catalog_labels(&engine).unwrap(), (vec![], vec![]));
}

#[test]
fn schema_management_bulk_alter_rejects_empty_duplicate_and_mixed_operations() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let err = engine
        .alter_graph_schema(Vec::new(), GraphSchemaSetOptions::default())
        .unwrap_err();
    assert_invalid_operation_contains(err, &["requires at least one operation"]);

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::SetNode {
                    label: "Person".to_string(),
                    schema: NodeSchema::default(),
                },
                GraphSchemaOperation::SetNode {
                    label: "Person".to_string(),
                    schema: NodeSchema::default(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate node set target", "Person"]);

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::SetEdge {
                    label: "KNOWS".to_string(),
                    schema: EdgeSchema::default(),
                },
                GraphSchemaOperation::SetEdge {
                    label: "KNOWS".to_string(),
                    schema: EdgeSchema::default(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate edge set target", "KNOWS"]);

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::DropNode {
                    label: "Archived".to_string(),
                },
                GraphSchemaOperation::DropNode {
                    label: "Archived".to_string(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate node drop target", "Archived"]);

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::DropEdge {
                    label: "OLD_EDGE".to_string(),
                },
                GraphSchemaOperation::DropEdge {
                    label: "OLD_EDGE".to_string(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate edge drop target", "OLD_EDGE"]);

    let err = engine
        .alter_graph_schema(
            vec![
                GraphSchemaOperation::SetNode {
                    label: "Person".to_string(),
                    schema: NodeSchema::default(),
                },
                GraphSchemaOperation::DropEdge {
                    label: "OLD_EDGE".to_string(),
                },
            ],
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["cannot mix set and drop operations"]);
}

#[test]
fn schema_management_set_graph_schema_rejects_duplicate_node_and_edge_targets() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let err = engine
        .set_graph_schema(
            graph_schema_from_targets(
                &[
                    ("Person", NodeSchema::default()),
                    ("Person", NodeSchema::default()),
                ],
                &[],
            ),
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate node target", "Person"]);

    let err = engine
        .set_graph_schema(
            graph_schema_from_targets(
                &[],
                &[
                    ("KNOWS", EdgeSchema::default()),
                    ("KNOWS", EdgeSchema::default()),
                ],
            ),
            GraphSchemaSetOptions::default(),
        )
        .unwrap_err();
    assert_invalid_operation_contains(err, &["duplicate edge target", "KNOWS"]);
}

#[test]
fn schema_management_bulk_check_is_side_effect_free() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("Existing", NodeSchema::default())
        .unwrap();
    let baseline_catalog = schema_catalog_labels(&engine).unwrap();

    let add_report = engine
        .check_graph_schema_add(
            graph_schema_from_targets(&[("Future", NodeSchema::default())], &[]),
            GraphSchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(add_report.operation, GraphSchemaOperationKind::CheckAdd);
    assert_eq!(add_report.checked_records, 0);
    assert_eq!(schema_catalog_labels(&engine).unwrap(), baseline_catalog);
    assert_eq!(engine.get_node_label_id("Future").unwrap(), None);

    let set_report = engine
        .check_graph_schema_set(GraphSchema::default(), GraphSchemaCheckOptions::default())
        .unwrap();
    assert_eq!(set_report.operation, GraphSchemaOperationKind::CheckSet);
    assert_eq!(set_report.checked_records, 0);
    assert_eq!(schema_catalog_labels(&engine).unwrap(), baseline_catalog);
}

#[test]
fn schema_management_bulk_check_uses_published_snapshot_and_skips_write_queue() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let (publish_ready_rx, publish_release_tx) = engine.set_runtime_publish_pause();
    let writer = engine.clone();
    let (writer_done_tx, writer_done_rx) = std::sync::mpsc::sync_channel(1);
    let writer_handle = std::thread::spawn(move || {
        let result = writer
            .upsert_node("Person", "bob", UpsertNodeOptions::default())
            .map(|_| ());
        writer_done_tx.send(()).unwrap();
        result
    });
    publish_ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("writer did not pause at runtime publication");
    assert!(
        writer_done_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "writer completed before the test released runtime publication"
    );

    let checker = engine.clone();
    let (check_tx, check_rx) = std::sync::mpsc::sync_channel(1);
    let check_handle = std::thread::spawn(move || {
        let result = checker.check_graph_schema_add(
            graph_schema_from_targets(&[("Person", node_schema_with_required_string("name"))], &[]),
            GraphSchemaCheckOptions::default(),
        );
        check_tx.send(result).unwrap();
    });

    let report = match check_rx.recv_timeout(std::time::Duration::from_secs(5)) {
        Ok(result) => result.unwrap(),
        Err(err) => {
            publish_release_tx.send(()).unwrap();
            writer_handle.join().unwrap().unwrap();
            check_handle.join().unwrap();
            panic!("bulk CHECK did not complete while writer was paused before publish: {err}");
        }
    };
    check_handle.join().unwrap();
    assert_eq!(report.checked_records, 1);
    assert_eq!(report.violation_count, 0);
    assert!(
        writer_done_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "bulk CHECK should have completed against the old published snapshot"
    );

    publish_release_tx.send(()).unwrap();
    writer_handle.join().unwrap().unwrap();

    let report = engine
        .check_graph_schema_add(
            graph_schema_from_targets(&[("Person", node_schema_with_required_string("name"))], &[]),
            GraphSchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(report.checked_records, 2);
    assert_eq!(report.violation_count, 1);
}

#[test]
fn schema_management_check_missing_labels_create_no_persistent_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let report = engine
        .check_graph_schema_set(
            graph_schema_from_targets(
                &[(
                    "FutureNode",
                    NodeSchema {
                        label_constraints: Some(NodeLabelConstraintSchema {
                            all_of: vec!["FutureEndpoint".to_string()],
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )],
                &[(
                    "FUTURE_EDGE",
                    EdgeSchema {
                        from: Some(endpoint_labels(&["FutureFrom"], &[], &[])),
                        ..Default::default()
                    },
                )],
            ),
            GraphSchemaCheckOptions::default(),
        )
        .unwrap();

    assert_eq!(report.checked_records, 0);
    for label in ["FutureNode", "FutureEndpoint", "FutureFrom"] {
        assert_eq!(engine.get_node_label_id(label).unwrap(), None);
    }
    assert_eq!(engine.get_edge_label_id("FUTURE_EDGE").unwrap(), None);
}

#[test]
fn schema_management_bulk_endpoint_labels_validate_through_proposed_catalog() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let from = engine
        .upsert_node("BulkFromBase", "from", UpsertNodeOptions::default())
        .unwrap();
    let to = engine
        .upsert_node("BulkTo", "to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(from, to, "BULK_ENDPOINT", UpsertEdgeOptions::default())
        .unwrap();

    let schema = graph_schema_from_targets(
        &[],
        &[(
            "BULK_ENDPOINT",
            endpoint_edge_schema(&["BulkRequiredFrom"], &[], &[], &["BulkTo"], &[], &[]),
        )],
    );
    let report = engine
        .check_graph_schema_add(schema.clone(), GraphSchemaCheckOptions::default())
        .unwrap();
    assert_eq!(report.checked_records, 1);
    assert_eq!(report.violation_count, 1);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].report.checked_records, 1);
    assert_eq!(report.entries[0].report.violation_count, 1);
    let violation = &report.entries[0].report.violations[0];
    assert!(violation.path.contains("from.labels.all_of"));
    assert!(violation.message.contains("BULK_ENDPOINT"));
    assert_eq!(engine.get_node_label_id("BulkRequiredFrom").unwrap(), None);

    let err = engine
        .set_graph_schema(schema, GraphSchemaSetOptions::default())
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected:"));
    assert!(engine.get_edge_schema("BULK_ENDPOINT").unwrap().is_none());
    assert_eq!(engine.get_node_label_id("BulkRequiredFrom").unwrap(), None);
}

#[test]
fn schema_management_successful_bulk_publish_creates_needed_labels_after_validation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let result = engine
        .set_graph_schema(
            graph_schema_from_targets(
                &[(
                    "FutureNode",
                    NodeSchema {
                        label_constraints: Some(NodeLabelConstraintSchema {
                            all_of: vec!["FutureRequired".to_string()],
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )],
                &[(
                    "FUTURE_EDGE",
                    EdgeSchema {
                        from: Some(endpoint_labels(&["FutureFrom"], &[], &[])),
                        ..Default::default()
                    },
                )],
            ),
            GraphSchemaSetOptions::default(),
        )
        .unwrap();

    assert_eq!(result.targets_published, 2);
    for label in ["FutureNode", "FutureRequired", "FutureFrom"] {
        assert!(engine.get_node_label_id(label).unwrap().is_some());
    }
    assert!(engine.get_edge_label_id("FUTURE_EDGE").unwrap().is_some());
}

#[test]
fn schema_management_bulk_manifest_write_failures_publish_no_schema_or_tokens() {
    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_set_manifest_failure");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine.force_next_runtime_manifest_write_error();
        let err = engine
            .set_graph_schema(
                graph_schema_from_targets(
                    &[("BulkSetNode", NodeSchema::default())],
                    &[("BULK_SET_EDGE", EdgeSchema::default())],
                ),
                GraphSchemaSetOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("test forced runtime manifest write failure"));
        assert!(engine.list_node_schemas().unwrap().is_empty());
        assert!(engine.list_edge_schemas().unwrap().is_empty());
        assert_eq!(engine.get_node_label_id("BulkSetNode").unwrap(), None);
        assert_eq!(engine.get_edge_label_id("BULK_SET_EDGE").unwrap(), None);
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_add_manifest_failure");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine
            .set_node_schema("ExistingBulkNode", NodeSchema::default())
            .unwrap();
        engine.force_next_runtime_manifest_write_error();
        let err = engine
            .alter_graph_schema(
                vec![
                    GraphSchemaOperation::SetNode {
                        label: "BulkAddNode".to_string(),
                        schema: NodeSchema::default(),
                    },
                    GraphSchemaOperation::SetEdge {
                        label: "BULK_ADD_EDGE".to_string(),
                        schema: EdgeSchema::default(),
                    },
                ],
                GraphSchemaSetOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("test forced runtime manifest write failure"));
        assert_eq!(
            schema_catalog_labels(&engine).unwrap(),
            (vec!["ExistingBulkNode".to_string()], vec![])
        );
        assert_eq!(engine.get_node_label_id("BulkAddNode").unwrap(), None);
        assert_eq!(engine.get_edge_label_id("BULK_ADD_EDGE").unwrap(), None);
    }
}

#[test]
fn schema_management_bulk_publish_reopen_parity_after_success_and_failure() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine
            .set_graph_schema(
                graph_schema_from_targets(
                    &[("Person", NodeSchema::default())],
                    &[("KNOWS", EdgeSchema::default())],
                ),
                GraphSchemaSetOptions::default(),
            )
            .unwrap();
        engine
            .upsert_node("Violating", "missing-name", UpsertNodeOptions::default())
            .unwrap();
        let err = engine
            .alter_graph_schema(
                vec![
                    GraphSchemaOperation::SetNode {
                        label: "FutureNoToken".to_string(),
                        schema: NodeSchema::default(),
                    },
                    GraphSchemaOperation::SetNode {
                        label: "Violating".to_string(),
                        schema: node_schema_with_required_string("name"),
                    },
                ],
                GraphSchemaSetOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("schema publication rejected:"));
        assert_eq!(engine.get_node_label_id("FutureNoToken").unwrap(), None);
        engine.close().unwrap();
    }
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(engine.get_node_schema("Person").unwrap().is_some());
        assert!(engine.get_edge_schema("KNOWS").unwrap().is_some());
        assert!(engine.get_node_schema("FutureNoToken").unwrap().is_none());
        assert_eq!(engine.get_node_label_id("FutureNoToken").unwrap(), None);
        engine.close().unwrap();
    }
}

#[test]
fn schema_management_bulk_scan_limit_zero_reports_and_prevents_publish() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let report = engine
        .check_graph_schema_set(
            graph_schema_from_targets(&[("Person", NodeSchema::default())], &[]),
            GraphSchemaCheckOptions {
                scan_limit: Some(0),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(report.checked_records, 0);
    assert!(report.scan_limit_hit);

    let err = engine
        .set_graph_schema(
            graph_schema_from_targets(&[("Person", NodeSchema::default())], &[]),
            GraphSchemaSetOptions {
                scan_limit: Some(0),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("scan limit exceeded"));
    assert!(engine.get_node_schema("Person").unwrap().is_none());
}

#[test]
fn schema_management_bulk_report_counts_are_exact_for_checked_records() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node(
            "Person",
            "ok",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    let b = engine
        .upsert_node("Person", "bad", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            a,
            b,
            "KNOWS",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let report = engine
        .check_graph_schema_add(
            graph_schema_from_targets(
                &[("Person", node_schema_with_required_string("name"))],
                &[("KNOWS", edge_schema_with_required_int("since"))],
            ),
            GraphSchemaCheckOptions::default(),
        )
        .unwrap();

    assert_eq!(report.checked_records, 4);
    assert_eq!(report.violation_count, 2);
    assert_eq!(report.entries.len(), 2);
    assert_eq!(report.entries[0].report.checked_records, 2);
    assert_eq!(report.entries[0].report.violation_count, 1);
    assert_eq!(report.entries[1].report.checked_records, 2);
    assert_eq!(report.entries[1].report.violation_count, 1);
}

#[test]
fn schema_management_bulk_validation_covers_active_immutable_and_flushed_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "segment",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Segment".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_node(
            "Person",
            "immutable",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Frozen".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine.freeze_memtable().unwrap();
    engine
        .upsert_node("Person", "active", UpsertNodeOptions::default())
        .unwrap();

    let report = engine
        .check_graph_schema_set(
            graph_schema_from_targets(&[("Person", node_schema_with_required_string("name"))], &[]),
            GraphSchemaCheckOptions {
                chunk_size: 1,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(report.checked_records, 3);
    assert_eq!(report.violation_count, 1);
}

#[test]
fn schema_management_set_node_schema_succeeds_on_clean_existing_nodes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let info = engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap();
    assert_eq!(info.label, "Person");
    assert!(engine.get_node_schema("Person").unwrap().is_some());
    assert_eq!(engine.manifest().unwrap().node_schemas.len(), 1);
}

#[test]
fn schema_management_set_node_schema_rejects_violations_and_leaves_catalog_unchanged() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let err = engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected:"));
    assert!(engine.get_node_schema("Person").unwrap().is_none());
    assert!(engine.list_node_schemas().unwrap().is_empty());
    assert!(engine.manifest().unwrap().node_schemas.is_empty());

    engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
}

#[test]
fn schema_management_queued_writes_resume_after_set_exits() {
    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_success");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        run_schema_set_with_queued_write(&engine, |engine| {
            engine
                .set_node_schema("Person", NodeSchema::default())
                .map(|_| ())
        })
        .unwrap();
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_validation_failure");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine
            .upsert_node("Person", "missing-name", UpsertNodeOptions::default())
            .unwrap();
        let err = run_schema_set_with_queued_write(&engine, |engine| {
            engine
                .set_node_schema("Person", node_schema_with_required_string("name"))
                .map(|_| ())
        })
        .unwrap_err();
        assert!(err.to_string().contains("schema publication rejected:"));
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_scan_limit");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();
        let err = run_schema_set_with_queued_write(&engine, |engine| {
            engine
                .set_node_schema_with_options(
                    "Person",
                    NodeSchema::default(),
                    SchemaSetOptions {
                        max_violations: 1,
                        chunk_size: 1,
                        scan_limit: Some(1),
                    },
                )
                .map(|_| ())
        })
        .unwrap_err();
        assert!(err.to_string().contains("scan limit exceeded"));
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_manifest_failure");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine.force_next_runtime_manifest_write_error();
        let err = run_schema_set_with_queued_write(&engine, |engine| {
            engine
                .set_node_schema("Person", NodeSchema::default())
                .map(|_| ())
        })
        .unwrap_err();
        assert!(err.to_string().contains("test forced runtime manifest write failure"));
        assert!(engine.get_node_schema("Person").unwrap().is_none());
    }
}

#[test]
fn schema_management_check_node_schema_is_advisory_and_does_not_use_write_queue() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let report = engine
        .check_node_schema(
            "Future",
            node_schema_with_required_string("name"),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(report.checked_records, 0);
    assert!(engine.get_node_label_id("Future").unwrap().is_none());
    assert!(engine.list_node_schemas().unwrap().is_empty());

    let (ready_rx, release_tx) = engine.set_runtime_read_pause();
    let checker = engine.clone();
    let handle = std::thread::spawn(move || {
        checker
            .check_node_schema(
                "Person",
                node_schema_with_required_string("name"),
                SchemaCheckOptions::default(),
            )
            .unwrap()
    });
    ready_rx.recv().unwrap();
    engine
        .upsert_node("Other", "write-while-check-paused", UpsertNodeOptions::default())
        .unwrap();
    release_tx.send(()).unwrap();
    let report = handle.join().unwrap();
    assert_eq!(report.violation_count, 0);
}

#[test]
fn schema_management_clean_check_then_intervening_write_still_makes_set_reject() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();

    let report = engine
        .check_node_schema(
            "Person",
            node_schema_with_required_string("name"),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(report.violation_count, 0);

    engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let err = engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected:"));
}

#[test]
fn schema_management_existing_data_scans_use_final_state_across_flushed_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();

    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = engine
        .upsert_node(&["Person", "Archived"], "bob", UpsertNodeOptions::default())
        .unwrap();
    let company = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    let works_at = engine
        .upsert_edge(alice, company, "WORKS_AT", UpsertEdgeOptions::default())
        .unwrap();
    let old_edge = engine
        .upsert_edge(bob, company, "OLD_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(engine.remove_node_label(bob, "Person").unwrap());
    let updated_works_at = engine
        .upsert_edge(
            alice,
            company,
            "WORKS_AT",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated_works_at, works_at);
    engine.delete_edge(old_edge).unwrap();

    let node_report = engine
        .check_node_schema(
            "Person",
            node_schema_with_required_string("name"),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(node_report.checked_records, 1);
    assert_eq!(node_report.violation_count, 0);
    engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap();

    let edge_report = engine
        .check_edge_schema(
            "WORKS_AT",
            edge_schema_with_required_int("since"),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(edge_report.checked_records, 1);
    assert_eq!(edge_report.violation_count, 0);
    engine
        .set_edge_schema("WORKS_AT", edge_schema_with_required_int("since"))
        .unwrap();

    let deleted_edge_report = engine
        .check_edge_schema(
            "OLD_EDGE",
            edge_schema_with_required_int("since"),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(deleted_edge_report.checked_records, 0);
    assert_eq!(deleted_edge_report.violation_count, 0);
    engine
        .set_edge_schema("OLD_EDGE", edge_schema_with_required_int("since"))
        .unwrap();
}

#[test]
fn schema_management_report_paths_are_structured_not_string_parsed() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let tricky_key = "field expected delimiter";
    let report = engine
        .check_node_schema(
            "Person",
            node_schema_with_required_string(tricky_key),
            SchemaCheckOptions::default(),
        )
        .unwrap();
    assert_eq!(report.violation_count, 1);
    assert_eq!(
        report.violations[0].path,
        format!("properties.{tricky_key}")
    );
}

#[test]
fn schema_management_dense_vector_schema_checks_db_config_without_existing_nodes() {
    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_db");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let err = engine
            .check_node_schema(
                "VectorNode",
                dense_required_schema(None),
                SchemaCheckOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("invalid schema:"));
        assert!(err.to_string().contains("dense vector"));

        let err = engine
            .set_node_schema("VectorNode", dense_required_schema(None))
            .unwrap_err();
        assert!(err.to_string().contains("invalid schema:"));
        assert!(engine.get_node_schema("VectorNode").unwrap().is_none());
    }

    {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("schema_db");
        let engine = DatabaseEngine::open(
            &db_path,
            &DbOptions {
                dense_vector: Some(DenseVectorConfig {
                    dimension: 3,
                    metric: DenseMetric::Cosine,
                    hnsw: HnswConfig::default(),
                }),
                ..Default::default()
            },
        )
        .unwrap();

        let err = engine
            .check_node_schema(
                "VectorNode",
                dense_required_schema(Some(4)),
                SchemaCheckOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("dimension 4"));
        assert!(err.to_string().contains("DB dimension 3"));

        let err = engine
            .set_node_schema("VectorNode", dense_required_schema(Some(4)))
            .unwrap_err();
        assert!(err.to_string().contains("dimension 4"));
        assert!(engine.get_node_schema("VectorNode").unwrap().is_none());

        engine
            .set_node_schema("VectorNode", dense_required_schema(Some(3)))
            .unwrap();
        assert!(engine.get_node_schema("VectorNode").unwrap().is_some());
    }
}

#[test]
fn schema_management_endpoint_edge_scan_helper_streams_chunks() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let c = engine
        .upsert_node("Person", "c", UpsertNodeOptions::default())
        .unwrap();
    let first = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let second = engine
        .upsert_edge(a, c, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(b, c, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let (_guard, published) = engine.runtime.published_snapshot().unwrap();
    let label_id = published
        .label_catalog
        .resolve_edge_label_for_read("KNOWS")
        .unwrap()
        .unwrap();
    let mut chunks = Vec::new();
    published
        .view
        .scan_edge_ids_by_endpoints(&[a], Direction::Outgoing, Some(&[label_id]), 1, |chunk| {
            assert!(chunk.len() <= 1);
            chunks.push(chunk.to_vec());
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    let edge_ids = chunks.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(edge_ids, vec![first, second]);
}

#[test]
fn schema_management_multi_label_closed_properties_use_full_catalog_union() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            &["Person", "Employee"],
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[
                    ("name", PropValue::String("Alice".to_string())),
                    ("role", PropValue::String("Engineer".to_string())),
                ]),
                ..Default::default()
            },
        )
        .unwrap();

    engine
        .set_node_schema("Person", closed_node_schema(&["name", "role"]))
        .unwrap();
    engine
        .set_node_schema("Employee", closed_node_schema(&["role"]))
        .unwrap();
    assert_eq!(engine.list_node_schemas().unwrap().len(), 2);
}

#[test]
fn schema_management_set_edge_schema_validates_existing_edges_and_endpoints() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let person = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let company = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            person,
            company,
            "WORKS_AT",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();

    engine
        .set_edge_schema("WORKS_AT", edge_schema_with_required_int("since"))
        .unwrap();

    let endpoint_schema = EdgeSchema {
        from: Some(EndpointLabelSchema {
            all_of: vec!["Person".to_string()],
            ..Default::default()
        }),
        to: Some(EndpointLabelSchema {
            all_of: vec!["Company".to_string()],
            ..Default::default()
        }),
        ..Default::default()
    };
    engine.set_edge_schema("WORKS_AT", endpoint_schema).unwrap();

    let bad_endpoint_schema = EdgeSchema {
        to: Some(EndpointLabelSchema {
            all_of: vec!["Person".to_string()],
            ..Default::default()
        }),
        ..Default::default()
    };
    let err = engine
        .set_edge_schema("WORKS_AT", bad_endpoint_schema)
        .unwrap_err();
    assert!(err.to_string().contains("to.labels.all_of"));
}

#[test]
fn schema_management_set_edge_schema_rejects_violating_edges_and_leaves_catalog_unchanged() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let err = engine
        .set_edge_schema("KNOWS", edge_schema_with_required_int("since"))
        .unwrap_err();
    assert!(err.to_string().contains("schema publication rejected:"));
    assert!(engine.get_edge_schema("KNOWS").unwrap().is_none());
    assert!(engine.manifest().unwrap().edge_schemas.is_empty());

    engine
        .upsert_edge(a, b, "REPORTS_TO", UpsertEdgeOptions::default())
        .unwrap();
}

#[test]
fn schema_management_scan_limit_and_violation_sample_truncation_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    for key in ["a", "b", "c"] {
        engine
            .upsert_node("Person", key, UpsertNodeOptions::default())
            .unwrap();
    }

    let report = engine
        .check_node_schema(
            "Person",
            node_schema_with_required_string("name"),
            SchemaCheckOptions {
                max_violations: 1,
                chunk_size: 2,
                scan_limit: None,
            },
        )
        .unwrap();
    assert_eq!(report.checked_records, 3);
    assert_eq!(report.violation_count, 3);
    assert_eq!(report.violations.len(), 1);
    assert!(report.truncated);

    let prefix_report = engine
        .check_node_schema(
            "Person",
            NodeSchema::default(),
            SchemaCheckOptions {
                max_violations: 10,
                chunk_size: 1,
                scan_limit: Some(1),
            },
        )
        .unwrap();
    assert_eq!(prefix_report.checked_records, 1);
    assert!(prefix_report.scan_limit_hit);

    let err = engine
        .set_node_schema_with_options(
            "Person",
            NodeSchema::default(),
            SchemaSetOptions {
                max_violations: 1,
                chunk_size: 1,
                scan_limit: Some(1),
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("scan limit exceeded"));
    assert!(engine.get_node_schema("Person").unwrap().is_none());

    engine
        .upsert_node("Person", "d", UpsertNodeOptions::default())
        .unwrap();
}

#[test]
fn schema_management_drop_get_list_sorted_and_reopen_parity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        engine
            .set_node_schema("Beta", NodeSchema::default())
            .unwrap();
        engine
            .set_node_schema("Alpha", NodeSchema::default())
            .unwrap();
        engine
            .set_edge_schema("Z_EDGE", EdgeSchema::default())
            .unwrap();
        engine
            .set_edge_schema("A_EDGE", EdgeSchema::default())
            .unwrap();

        let node_labels: Vec<String> = engine
            .list_node_schemas()
            .unwrap()
            .into_iter()
            .map(|info| info.label)
            .collect();
        assert_eq!(node_labels, vec!["Alpha".to_string(), "Beta".to_string()]);
        let edge_labels: Vec<String> = engine
            .list_edge_schemas()
            .unwrap()
            .into_iter()
            .map(|info| info.label)
            .collect();
        assert_eq!(edge_labels, vec!["A_EDGE".to_string(), "Z_EDGE".to_string()]);
        assert!(engine.get_node_schema("Alpha").unwrap().is_some());
        assert!(engine.get_edge_schema("A_EDGE").unwrap().is_some());
        engine.close().unwrap();
    }
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert!(engine.get_node_schema("Alpha").unwrap().is_some());
        assert!(engine.get_edge_schema("A_EDGE").unwrap().is_some());
        assert!(engine.drop_node_schema("Alpha").unwrap());
        assert!(!engine.drop_node_schema("Alpha").unwrap());
        assert!(engine.drop_edge_schema("A_EDGE").unwrap());
        assert!(!engine.drop_edge_schema("A_EDGE").unwrap());
        engine.close().unwrap();
    }
}

#[test]
fn schema_management_manifest_metadata_replaces_and_recreates_per_spec() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    engine
        .set_node_schema("Person", NodeSchema::default())
        .unwrap();
    let first = engine.manifest().unwrap().node_schemas[0].clone();
    assert_eq!(first.schema_id, 1);
    assert_eq!(first.revision, 1);
    assert_eq!(first.created_at_ms, first.updated_at_ms);

    engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap();
    let second = engine.manifest().unwrap().node_schemas[0].clone();
    assert_eq!(second.schema_id, first.schema_id);
    assert_eq!(second.revision, 2);
    assert_eq!(second.created_at_ms, first.created_at_ms);
    assert!(second.updated_at_ms >= first.updated_at_ms);

    assert!(engine.drop_node_schema("Person").unwrap());
    engine
        .set_node_schema("Person", NodeSchema::default())
        .unwrap();
    let third = engine.manifest().unwrap().node_schemas[0].clone();
    assert_ne!(third.schema_id, first.schema_id);
    assert_eq!(third.revision, 1);
}

#[test]
fn schema_management_drop_removes_write_enforcement() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap();

    let err = engine
        .upsert_node("Person", "missing-name", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.drop_node_schema("Person").unwrap());
    assert!(engine.get_node_schema("Person").unwrap().is_none());
    engine
        .upsert_node("Person", "missing-name", UpsertNodeOptions::default())
        .unwrap();

    let a = engine
        .upsert_node("Endpoint", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Endpoint", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            a,
            b,
            "KNOWS",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .set_edge_schema("KNOWS", edge_schema_with_required_int("since"))
        .unwrap();
    let err = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.drop_edge_schema("KNOWS").unwrap());
    assert!(engine.get_edge_schema("KNOWS").unwrap().is_none());
    engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
}

#[test]
fn schema_enforcement_empty_and_unrelated_catalog_fast_paths_do_not_build_overlay() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let a = engine
        .upsert_node("Other", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Other", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "OTHER_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    assert_eq!(engine.schema_validation_overlay_build_count().unwrap(), 0);

    engine
        .set_node_schema("StrictNode", node_schema_with_required_string("name"))
        .unwrap();
    engine
        .set_edge_schema("StrictEdge", edge_schema_with_required_int("since"))
        .unwrap();

    engine
        .upsert_node("Other", "c", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "OTHER_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    assert_eq!(engine.schema_validation_overlay_build_count().unwrap(), 0);
}

#[test]
fn schema_enforcement_node_writes_are_pre_wal_atomic_and_restore_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("Person", node_schema_with_required_string("name"))
        .unwrap();

    let next_node_before = engine.next_node_id().unwrap();
    let manifest_node_before = engine.manifest().unwrap().next_node_id;
    let err = engine
        .upsert_node("Person", "missing-name", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.get_node_by_key("Person", "missing-name").unwrap().is_none());
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.manifest().unwrap().next_node_id, manifest_node_before);

    let id = engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Alice".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(id, next_node_before);

    engine
        .set_node_schema("BatchPerson", node_schema_with_required_string("name"))
        .unwrap();
    let batch_next_before = engine.next_node_id().unwrap();
    let err = engine
        .batch_upsert_nodes(vec![
            schema_node_input(
                "BatchPerson",
                "ok",
                schema_value_props(&[("name", PropValue::String("Ok".to_string()))]),
            ),
            schema_node_input("BatchPerson", "bad", BTreeMap::new()),
        ])
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine
        .get_node_by_key("BatchPerson", "ok")
        .unwrap()
        .is_none());
    assert!(engine
        .get_node_by_key("BatchPerson", "bad")
        .unwrap()
        .is_none());
    assert_eq!(engine.next_node_id().unwrap(), batch_next_before);
}

#[test]
fn schema_enforcement_node_key_weight_vector_closed_and_multilabel_rules_apply() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..Default::default()
        },
    )
    .unwrap();

    engine
        .set_node_schema(
            "KeyedNode",
            NodeSchema {
                key: Some(StringFieldSchema {
                    min_bytes: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node("KeyedNode", "ab", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_node_schema(
            "WeightedNode",
            NodeSchema {
                weight: Some(NumericFieldSchema {
                    min: Some(SchemaNumericBound {
                        value: PropValue::Float(2.0),
                        inclusive: true,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node("WeightedNode", "n", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_node_schema("DenseNode", dense_required_schema(Some(3)))
        .unwrap();
    let err = engine
        .upsert_node("DenseNode", "n", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_node_schema(
            "SparseNode",
            NodeSchema {
                sparse_vector: Some(SparseVectorSchema {
                    presence: SchemaVectorPresence::Required,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node("SparseNode", "n", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_node_schema(
            "ClosedNode",
            NodeSchema {
                additional_properties: SchemaAdditionalProperties::Reject,
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node(
            "ClosedNode",
            "n",
            UpsertNodeOptions {
                props: schema_value_props(&[("extra", PropValue::String("x".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_node_schema("MultiA", closed_node_schema(&["name"]))
        .unwrap();
    engine
        .set_node_schema("MultiB", closed_node_schema(&["role"]))
        .unwrap();
    engine
        .upsert_node(
            &["MultiA", "MultiB"],
            "ok",
            UpsertNodeOptions {
                props: schema_value_props(&[
                    ("name", PropValue::String("Ada".to_string())),
                    ("role", PropValue::String("Engineer".to_string())),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node(
            &["MultiA", "MultiB"],
            "bad",
            UpsertNodeOptions {
                props: schema_value_props(&[
                    ("name", PropValue::String("Ada".to_string())),
                    ("role", PropValue::String("Engineer".to_string())),
                    ("extra", PropValue::String("x".to_string())),
                ]),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_schema_violation(err);
}

#[test]
fn schema_enforcement_edge_rules_are_pre_wal_atomic_and_restore_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Endpoint", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Endpoint", "b", UpsertNodeOptions::default())
        .unwrap();

    engine
        .set_edge_schema("RequiresSince", edge_schema_with_required_int("since"))
        .unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();
    let manifest_edge_before = engine.manifest().unwrap().next_edge_id;
    let err = engine
        .upsert_edge(a, b, "RequiresSince", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.get_edge(next_edge_before).unwrap().is_none());
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert_eq!(engine.manifest().unwrap().next_edge_id, manifest_edge_before);
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "RequiresSince",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(edge_id, next_edge_before);

    engine
        .set_edge_schema(
            "ClosedEdge",
            EdgeSchema {
                additional_properties: SchemaAdditionalProperties::Reject,
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_edge(
            a,
            b,
            "ClosedEdge",
            UpsertEdgeOptions {
                props: schema_value_props(&[("extra", PropValue::String("x".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_edge_schema(
            "WeightedEdge",
            EdgeSchema {
                weight: Some(NumericFieldSchema {
                    min: Some(SchemaNumericBound {
                        value: PropValue::Float(2.0),
                        inclusive: true,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_edge(a, b, "WeightedEdge", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_edge_schema(
            "ValidityEdge",
            EdgeSchema {
                validity: Some(EdgeValiditySchema {
                    require_valid_from_before_valid_to: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_edge(
            a,
            b,
            "ValidityEdge",
            UpsertEdgeOptions {
                valid_from: Some(10),
                valid_to: Some(5),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_schema_violation(err);

    engine
        .set_edge_schema(
            "NoSelfLoop",
            EdgeSchema {
                allow_self_loops: false,
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_edge(a, a, "NoSelfLoop", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
}

#[test]
fn schema_enforcement_endpoint_edge_writes_validate_all_any_none_and_missing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    engine
        .set_edge_schema(
            "EndpointRules",
            endpoint_edge_schema(
                &["FromRequired"],
                &["FromChoice", "FromAlternative"],
                &["FromBlocked"],
                &["ToRequired"],
                &["ToChoice", "ToAlternative"],
                &["ToBlocked"],
            ),
        )
        .unwrap();

    let good_from = engine
        .upsert_node(
            &["FromRequired", "FromChoice"],
            "good-from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let good_to = engine
        .upsert_node(
            &["ToRequired", "ToChoice"],
            "good-to",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    engine
        .upsert_edge(good_from, good_to, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap();

    let wrong_from = engine
        .upsert_node("FromChoice", "wrong-from", UpsertNodeOptions::default())
        .unwrap();
    let err = engine
        .upsert_edge(wrong_from, good_to, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(
        err,
        &[
            "from.labels.all_of",
            "from endpoint node id",
            "labels",
            "EndpointRules",
        ],
    );

    let wrong_to = engine
        .upsert_node("ToChoice", "wrong-to", UpsertNodeOptions::default())
        .unwrap();
    let err = engine
        .upsert_edge(good_from, wrong_to, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(
        err,
        &[
            "to.labels.all_of",
            "to endpoint node id",
            "labels",
            "EndpointRules",
        ],
    );

    let blocked_from = engine
        .upsert_node(
            &["FromRequired", "FromChoice", "FromBlocked"],
            "blocked-from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let err = engine
        .upsert_edge(blocked_from, good_to, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.none_of", "from endpoint node id"]);

    let err = engine
        .upsert_edge(999_001, good_to, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(
        err,
        &["from.labels.all_of", "from endpoint node id 999001", "missing endpoint"],
    );
    let err = engine
        .upsert_edge(good_from, 999_002, "EndpointRules", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(
        err,
        &["to.labels.all_of", "to endpoint node id 999002", "missing endpoint"],
    );
}

#[test]
fn schema_enforcement_endpoint_only_schema_now_enforces_edge_writes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("WrongFrom", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("WrongTo", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema(
            "EndpointOnly",
            endpoint_edge_schema(&["RequiredFrom"], &[], &[], &["RequiredTo"], &[], &[]),
        )
        .unwrap();

    let next_edge_before = engine.next_edge_id().unwrap();
    let err = engine
        .upsert_edge(a, b, "EndpointOnly", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "EndpointOnly"]);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert!(engine.get_edge(next_edge_before).unwrap().is_none());
}

#[test]
fn schema_enforcement_same_plan_endpoint_final_labels_drive_edge_validation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "RawEndpoint",
            endpoint_edge_schema(&["RawFrom"], &[], &[], &["RawTo"], &[], &[]),
        )
        .unwrap();
    let from_label_id = engine.get_node_label_id("RawFrom").unwrap().unwrap();
    let other_label_id = engine.ensure_node_label("RawOther").unwrap();
    let edge_label_id = engine.get_edge_label_id("RawEndpoint").unwrap().unwrap();

    let to = engine
        .upsert_node("RawTo", "to", UpsertNodeOptions::default())
        .unwrap();
    let good_node = schema_raw_node_record(10_000, from_label_id, "raw-good", BTreeMap::new());
    let good_edge = schema_raw_edge_record(20_000, 10_000, to, edge_label_id);
    engine
        .write_op_batch(&[WalOp::UpsertNode(good_node), WalOp::UpsertEdge(good_edge)])
        .unwrap();
    assert!(engine.get_edge(20_000).unwrap().is_some());

    let bad_node =
        schema_raw_node_record(10_001, other_label_id, "raw-bad", BTreeMap::new());
    let bad_edge = schema_raw_edge_record(20_001, 10_001, to, edge_label_id);
    let err = engine
        .write_op_batch(&[WalOp::UpsertNode(bad_node), WalOp::UpsertEdge(bad_edge)])
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "RawEndpoint"]);
    assert!(engine.get_node(10_001).unwrap().is_none());
    assert!(engine.get_edge(20_001).unwrap().is_none());
}

#[test]
fn schema_enforcement_graph_patch_endpoint_final_state_is_validated() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();
    let from = engine
        .upsert_node("PatchBase", "from", UpsertNodeOptions::default())
        .unwrap();
    let to = engine
        .upsert_node("PatchTo", "to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema(
            "PatchEndpoint",
            endpoint_edge_schema(&["PatchFrom"], &[], &[], &["PatchTo"], &[], &[]),
        )
        .unwrap();

    engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![NodeInput {
                labels: vec!["PatchBase".to_string(), "PatchFrom".to_string()],
                key: "from".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            }],
            upsert_edges: vec![schema_edge_input(from, to, "PatchEndpoint", BTreeMap::new())],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(engine.count_edges_by_label("PatchEndpoint").unwrap(), 1);

    let invalid_from = engine
        .upsert_node("PatchBase", "invalid-from", UpsertNodeOptions::default())
        .unwrap();
    let err = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![schema_edge_input(
                invalid_from,
                to,
                "PatchEndpoint",
                BTreeMap::new(),
            )],
            ..Default::default()
        })
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "PatchEndpoint"]);
}

#[test]
fn schema_enforcement_invalidate_edge_uses_shared_edge_validation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Endpoint", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Endpoint", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "TemporalEdge",
            UpsertEdgeOptions {
                valid_from: Some(10),
                valid_to: Some(20),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .set_edge_schema(
            "TemporalEdge",
            EdgeSchema {
                validity: Some(EdgeValiditySchema {
                    require_valid_from_before_valid_to: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();

    let err = engine.invalidate_edge(edge_id, 5).unwrap_err();
    assert_schema_violation(err);
    assert_eq!(engine.get_edge(edge_id).unwrap().unwrap().valid_to, 20);
}

#[test]
fn schema_enforcement_graph_patch_validates_final_live_records_only() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();
    engine
        .set_node_schema("PatchPerson", node_schema_with_required_string("name"))
        .unwrap();

    let err = engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![schema_node_input("PatchPerson", "bad", BTreeMap::new())],
            ..Default::default()
        })
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine
        .get_node_by_key("PatchPerson", "bad")
        .unwrap()
        .is_none());

    let doomed = engine
        .upsert_node(
            "PatchPerson",
            "doomed",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Before".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![schema_node_input("PatchPerson", "doomed", BTreeMap::new())],
            delete_node_ids: vec![doomed],
            ..Default::default()
        })
        .unwrap();
    assert!(engine.get_node(doomed).unwrap().is_none());

    let a = engine
        .upsert_node("PatchEndpoint", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("PatchEndpoint", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema("PatchEdge", edge_schema_with_required_int("since"))
        .unwrap();
    let err = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![schema_edge_input(a, b, "PatchEdge", BTreeMap::new())],
            ..Default::default()
        })
        .unwrap_err();
    assert_schema_violation(err);
    assert_eq!(engine.count_edges_by_label("PatchEdge").unwrap(), 0);

    let doomed_edge = engine
        .upsert_edge(
            a,
            b,
            "PatchEdge",
            UpsertEdgeOptions {
                props: schema_value_props(&[("since", PropValue::Int(2024))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![schema_edge_input(a, b, "PatchEdge", BTreeMap::new())],
            delete_edge_ids: vec![doomed_edge],
            ..Default::default()
        })
        .unwrap();
    assert!(engine.get_edge(doomed_edge).unwrap().is_none());
}

#[test]
fn schema_enforcement_transactions_validate_final_state_and_restore_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("TxnPerson", node_schema_with_required_string("name"))
        .unwrap();

    let next_node_before = engine.next_node_id().unwrap();
    let manifest_node_before = engine.manifest().unwrap().next_node_id;
    let mut invalid_txn = engine.begin_write_txn().unwrap();
    invalid_txn
        .upsert_node("TxnPerson", "bad", UpsertNodeOptions::default())
        .unwrap();
    let err = invalid_txn.commit().unwrap_err();
    assert_schema_violation(err);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.manifest().unwrap().next_node_id, manifest_node_before);
    assert!(engine.get_node_by_key("TxnPerson", "bad").unwrap().is_none());

    let mut replacement_txn = engine.begin_write_txn().unwrap();
    replacement_txn
        .upsert_node("TxnPerson", "ok", UpsertNodeOptions::default())
        .unwrap();
    replacement_txn
        .upsert_node(
            "TxnPerson",
            "ok",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Ok".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    replacement_txn.commit().unwrap();
    assert!(engine.get_node_by_key("TxnPerson", "ok").unwrap().is_some());

    let a = engine
        .upsert_node("TxnEndpoint", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("TxnEndpoint", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema("TxnEdge", edge_schema_with_required_int("since"))
        .unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();
    let manifest_edge_before = engine.manifest().unwrap().next_edge_id;
    let mut edge_txn = engine.begin_write_txn().unwrap();
    edge_txn
        .upsert_edge(
            TxnNodeRef::Id(a),
            TxnNodeRef::Id(b),
            "TxnEdge",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    let err = edge_txn.commit().unwrap_err();
    assert_schema_violation(err);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert_eq!(engine.manifest().unwrap().next_edge_id, manifest_edge_before);
}

#[test]
fn schema_enforcement_transactions_use_final_endpoint_label_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let from = engine
        .upsert_node("TxnEndpointBase", "from", UpsertNodeOptions::default())
        .unwrap();
    let to = engine
        .upsert_node("TxnRequiredTo", "to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema(
            "TxnEndpointRules",
            endpoint_edge_schema(&["TxnRequiredFrom"], &[], &[], &["TxnRequiredTo"], &[], &[]),
        )
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.add_node_label(TxnNodeRef::Id(from), "TxnRequiredFrom")
        .unwrap();
    txn.upsert_edge(
        TxnNodeRef::Id(from),
        TxnNodeRef::Id(to),
        "TxnEndpointRules",
        UpsertEdgeOptions::default(),
    )
    .unwrap();
    txn.commit().unwrap();
    assert_eq!(engine.count_edges_by_label("TxnEndpointRules").unwrap(), 1);

    let labels_before = engine.get_node(from).unwrap().unwrap().labels;
    let mut invalid_txn = engine.begin_write_txn().unwrap();
    invalid_txn
        .remove_node_label(TxnNodeRef::Id(from), "TxnRequiredFrom")
        .unwrap();
    let err = invalid_txn.commit().unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "TxnEndpointRules"]);
    assert_eq!(engine.get_node(from).unwrap().unwrap().labels, labels_before);
}

#[test]
fn schema_enforcement_gql_mutations_route_through_shared_validator() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("GqlSchemaCreate", node_schema_with_required_string("name"))
        .unwrap();

    let next_node_before = engine.next_node_id().unwrap();
    let err = engine
        .execute_gql(
            "CREATE (n:GqlSchemaCreate {key: 'bad'}) RETURN n",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_schema_violation(err);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert!(engine
        .get_node_by_key("GqlSchemaCreate", "bad")
        .unwrap()
        .is_none());

    engine
        .upsert_node(
            "GqlSchemaSet",
            "n",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Ada".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .set_node_schema("GqlSchemaSet", node_schema_with_required_string("name"))
        .unwrap();
    let err = engine
        .execute_gql(
            "MATCH (n:GqlSchemaSet) WHERE n.key = 'n' REMOVE n.name",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_schema_violation(err);
    let node = engine
        .get_node_by_key("GqlSchemaSet", "n")
        .unwrap()
        .unwrap();
    assert!(matches!(node.props.get("name"), Some(PropValue::String(value)) if value == "Ada"));

    engine
        .upsert_node(
            "GqlSchemaSetClosed",
            "n",
            UpsertNodeOptions {
                props: schema_value_props(&[("name", PropValue::String("Ada".to_string()))]),
                ..Default::default()
            },
        )
        .unwrap();
    engine
        .set_node_schema("GqlSchemaSetClosed", closed_node_schema(&["name"]))
        .unwrap();
    let err = engine
        .execute_gql(
            "MATCH (n:GqlSchemaSetClosed) WHERE n.key = 'n' SET n.extra = 'x'",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_schema_violation(err);
    let node = engine
        .get_node_by_key("GqlSchemaSetClosed", "n")
        .unwrap()
        .unwrap();
    assert!(!node.props.contains_key("extra"));
}

#[test]
fn schema_enforcement_gql_set_remove_labels_revalidate_endpoint_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let from = engine
        .upsert_node("GqlEndpointFrom", "from", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_node("GqlEndpointTo", "to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema(
            "Gql_ENDPOINT_RULES",
            endpoint_edge_schema(
                &["GqlRequiredFrom"],
                &[],
                &["GqlForbiddenFrom"],
                &["GqlEndpointTo"],
                &[],
                &[],
            ),
        )
        .unwrap();

    engine
        .execute_gql(
            "MATCH (a:GqlEndpointFrom) WHERE a.key = 'from' \
             MATCH (b:GqlEndpointTo) WHERE b.key = 'to' \
             SET a:GqlRequiredFrom CREATE (a)-[:Gql_ENDPOINT_RULES]->(b)",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap();
    assert_eq!(engine.count_edges_by_label("Gql_ENDPOINT_RULES").unwrap(), 1);
    assert!(engine
        .get_node(from)
        .unwrap()
        .unwrap()
        .labels
        .iter()
        .any(|label| label == "GqlRequiredFrom"));

    let err = engine
        .execute_gql(
            "MATCH (a:GqlEndpointFrom) WHERE a.key = 'from' SET a:GqlForbiddenFrom",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.none_of", "Gql_ENDPOINT_RULES"]);
    assert!(!engine
        .get_node(from)
        .unwrap()
        .unwrap()
        .labels
        .iter()
        .any(|label| label == "GqlForbiddenFrom"));

    let err = engine
        .execute_gql(
            "MATCH (a:GqlEndpointFrom) WHERE a.key = 'from' REMOVE a:GqlRequiredFrom",
            &GqlParams::new(),
            &gql_opts(),
        )
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "Gql_ENDPOINT_RULES"]);
    assert!(engine
        .get_node(from)
        .unwrap()
        .unwrap()
        .labels
        .iter()
        .any(|label| label == "GqlRequiredFrom"));
}

#[test]
fn schema_enforcement_incident_edge_revalidation_handles_node_label_mutations_and_deletes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "IncidentEndpoint",
            endpoint_edge_schema(
                &["IncidentRequiredFrom"],
                &[],
                &["IncidentForbiddenFrom"],
                &["IncidentRequiredTo"],
                &[],
                &[],
            ),
        )
        .unwrap();
    let from = engine
        .upsert_node(
            &["IncidentRequiredFrom", "IncidentBase"],
            "from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let to = engine
        .upsert_node(
            &["IncidentRequiredTo", "IncidentTo"],
            "to",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let edge = engine
        .upsert_edge(from, to, "IncidentEndpoint", UpsertEdgeOptions::default())
        .unwrap();

    let next_node_before = engine.next_node_id().unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();
    let labels_before = engine.get_node(from).unwrap().unwrap().labels;
    let err = engine
        .remove_node_label(from, "IncidentRequiredFrom")
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "IncidentEndpoint"]);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert_eq!(engine.get_node(from).unwrap().unwrap().labels, labels_before);

    let err = engine.add_node_label(from, "IncidentForbiddenFrom").unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.none_of", "IncidentEndpoint"]);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert!(!engine
        .get_node(from)
        .unwrap()
        .unwrap()
        .labels
        .iter()
        .any(|label| label == "IncidentForbiddenFrom"));

    let err = engine
        .write_op(&WalOp::DeleteNode {
            id: from,
            deleted_at: 123,
        })
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "missing endpoint"]);
    assert!(engine.get_node(from).unwrap().is_some());
    assert!(engine.get_edge(edge).unwrap().is_some());

    engine.delete_node(from).unwrap();
    assert!(engine.get_node(from).unwrap().is_none());
    assert!(engine.get_edge(edge).unwrap().is_none());

    let from_skip = engine
        .upsert_node(
            &["IncidentRequiredFrom", "IncidentBase"],
            "from-skip",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let to_skip = engine
        .upsert_node(
            &["IncidentRequiredTo", "IncidentTo"],
            "to-skip",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let edge_skip = engine
        .upsert_edge(
            from_skip,
            to_skip,
            "IncidentEndpoint",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine
        .write_op_batch(&[
            WalOp::DeleteEdge {
                id: edge_skip,
                deleted_at: 200,
            },
            WalOp::DeleteNode {
                id: from_skip,
                deleted_at: 201,
            },
        ])
        .unwrap();
    assert!(engine.get_node(from_skip).unwrap().is_none());
    assert!(engine.get_edge(edge_skip).unwrap().is_none());
}

#[test]
fn schema_enforcement_same_plan_edge_rewrite_to_unconstrained_label_skips_stale_incident_edge() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "RewriteEndpoint",
            endpoint_edge_schema(&["RewriteRequiredFrom"], &[], &[], &["RewriteTo"], &[], &[]),
        )
        .unwrap();

    let from = engine
        .upsert_node(
            &["RewriteRequiredFrom", "RewriteBase"],
            "from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let to = engine
        .upsert_node("RewriteTo", "to", UpsertNodeOptions::default())
        .unwrap();
    let edge = engine
        .upsert_edge(from, to, "RewriteEndpoint", UpsertEdgeOptions::default())
        .unwrap();

    let base_label_id = engine.get_node_label_id("RewriteBase").unwrap().unwrap();
    let unconstrained_label_id = engine.ensure_edge_label("RewriteUnconstrained").unwrap();
    engine
        .write_op_batch(&[
            WalOp::UpsertNode(schema_raw_node_record(
                from,
                base_label_id,
                "from",
                BTreeMap::new(),
            )),
            WalOp::UpsertEdge(schema_raw_edge_record(
                edge,
                from,
                to,
                unconstrained_label_id,
            )),
        ])
        .unwrap();

    let from_node = engine.get_node(from).unwrap().unwrap();
    assert_eq!(from_node.labels, vec!["RewriteBase".to_string()]);
    assert_eq!(
        engine.get_edge(edge).unwrap().unwrap().label,
        "RewriteUnconstrained"
    );
}

#[test]
fn schema_enforcement_endpoint_sources_cover_active_immutable_segment_and_shadowed_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "SourceEndpoint",
            endpoint_edge_schema(&["SourceFrom"], &[], &[], &["SourceTo"], &[], &[]),
        )
        .unwrap();

    let active_from = engine
        .upsert_node(
            &["SourceFrom", "SourceBase"],
            "active-from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let active_to = engine
        .upsert_node("SourceTo", "active-to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            active_from,
            active_to,
            "SourceEndpoint",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    let imm_from = engine
        .upsert_node(
            &["SourceFrom", "SourceBase"],
            "imm-from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let imm_to = engine
        .upsert_node("SourceTo", "imm-to", UpsertNodeOptions::default())
        .unwrap();
    let imm_edge = engine
        .upsert_edge(
            imm_from,
            imm_to,
            "SourceEndpoint",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine.freeze_memtable().unwrap();
    let err = engine
        .remove_node_label(imm_from, "SourceFrom")
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "SourceEndpoint"]);
    engine.delete_edge(imm_edge).unwrap();
    assert!(engine.remove_node_label(imm_from, "SourceFrom").unwrap());

    let seg_from = engine
        .upsert_node(
            &["SourceFrom", "SourceBase"],
            "seg-from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let seg_to = engine
        .upsert_node("SourceTo", "seg-to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(
            seg_from,
            seg_to,
            "SourceEndpoint",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
    engine.flush().unwrap();
    let err = engine
        .remove_node_label(seg_from, "SourceFrom")
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "SourceEndpoint"]);

    let endpoint_from = engine
        .upsert_node("SourceFrom", "segment-endpoint-from", UpsertNodeOptions::default())
        .unwrap();
    let endpoint_to = engine
        .upsert_node("SourceTo", "segment-endpoint-to", UpsertNodeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    engine
        .upsert_edge(
            endpoint_from,
            endpoint_to,
            "SourceEndpoint",
            UpsertEdgeOptions::default(),
        )
        .unwrap();
}

#[test]
fn schema_enforcement_endpoint_incident_scan_streams_chunks_without_silent_allow() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "ChunkEndpoint",
            endpoint_edge_schema(&[], &["ChunkA", "ChunkB"], &[], &["ChunkTo"], &[], &[]),
        )
        .unwrap();
    let hub = engine
        .upsert_node(&["ChunkA", "ChunkHub"], "hub", UpsertNodeOptions::default())
        .unwrap();
    for i in 0..20 {
        let leaf = engine
            .upsert_node("ChunkTo", &format!("leaf-{i}"), UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_edge(hub, leaf, "ChunkEndpoint", UpsertEdgeOptions::default())
            .unwrap();
    }

    let chunks_before = engine.schema_validation_incident_scan_chunk_count().unwrap();
    assert!(engine.add_node_label(hub, "ChunkB").unwrap());
    let chunks_after = engine.schema_validation_incident_scan_chunk_count().unwrap();
    assert!(
        chunks_after >= chunks_before + 3,
        "expected chunked incident scan to visit at least 3 chunks, before {chunks_before}, after {chunks_after}"
    );

    assert!(engine.remove_node_label(hub, "ChunkA").unwrap());
    let err = engine.remove_node_label(hub, "ChunkB").unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.any_of", "ChunkEndpoint"]);
}

#[test]
fn schema_enforcement_endpoint_fast_paths_skip_unchanged_and_irrelevant_incident_scans() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_edge_schema(
            "FastEndpoint",
            endpoint_edge_schema(&["FastRequired"], &[], &[], &["FastTo"], &[], &[]),
        )
        .unwrap();
    let from = engine
        .upsert_node(
            &["FastRequired", "FastBase"],
            "from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let to = engine
        .upsert_node("FastTo", "to", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(from, to, "FastEndpoint", UpsertEdgeOptions::default())
        .unwrap();

    let chunks_before = engine.schema_validation_incident_scan_chunk_count().unwrap();
    engine
        .upsert_node(
            &["FastRequired", "FastBase"],
            "from",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    assert_eq!(
        engine.schema_validation_incident_scan_chunk_count().unwrap(),
        chunks_before
    );
    assert!(engine.add_node_label(from, "FastUnrelated").unwrap());
    assert_eq!(
        engine.schema_validation_incident_scan_chunk_count().unwrap(),
        chunks_before
    );
}

#[test]
fn schema_enforcement_raw_test_write_ops_are_covered_by_core_plan() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema("RawPerson", node_schema_with_required_string("name"))
        .unwrap();
    let label_id = engine.get_node_label_id("RawPerson").unwrap().unwrap();

    let err = engine
        .write_op(&WalOp::UpsertNode(schema_raw_node_record(
            100,
            label_id,
            "raw-bad",
            BTreeMap::new(),
        )))
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.get_node(100).unwrap().is_none());

    let valid = schema_raw_node_record(
        101,
        label_id,
        "raw-valid",
        schema_value_props(&[("name", PropValue::String("Valid".to_string()))]),
    );
    let invalid = schema_raw_node_record(102, label_id, "raw-batch-bad", BTreeMap::new());
    let err = engine
        .write_op_batch(&[WalOp::UpsertNode(valid), WalOp::UpsertNode(invalid)])
        .unwrap_err();
    assert_schema_violation(err);
    assert!(engine.get_node(101).unwrap().is_none());
    assert!(engine.get_node(102).unwrap().is_none());
}

#[test]
fn schema_enforcement_node_label_constraints_and_endpoint_rules_are_enforced() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .set_node_schema(
            "NeedsAlso",
            NodeSchema {
                label_constraints: Some(NodeLabelConstraintSchema {
                    all_of: vec!["Also".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_node("NeedsAlso", "bad", UpsertNodeOptions::default())
        .unwrap_err();
    assert_schema_violation(err);
    engine
        .upsert_node(&["NeedsAlso", "Also"], "ok", UpsertNodeOptions::default())
        .unwrap();

    let add_target = engine
        .upsert_node("BaseOnly", "add-target", UpsertNodeOptions::default())
        .unwrap();
    let labels_before_add = engine
        .get_node(add_target)
        .unwrap()
        .unwrap()
        .labels;
    let err = engine.add_node_label(add_target, "NeedsAlso").unwrap_err();
    assert_schema_violation(err);
    assert_eq!(
        engine
            .get_node(add_target)
            .unwrap()
            .unwrap()
            .labels,
        labels_before_add
    );

    let remove_target = engine
        .upsert_node(
            &["NeedsAlso", "Also"],
            "remove-target",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let labels_before_remove = engine
        .get_node(remove_target)
        .unwrap()
        .unwrap()
        .labels;
    let err = engine.remove_node_label(remove_target, "Also").unwrap_err();
    assert_schema_violation(err);
    assert_eq!(
        engine
            .get_node(remove_target)
            .unwrap()
            .unwrap()
            .labels,
        labels_before_remove
    );

    let a = engine
        .upsert_node("WrongFrom", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("WrongTo", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .set_edge_schema(
            "EndpointDeferred",
            EdgeSchema {
                from: Some(EndpointLabelSchema {
                    all_of: vec!["RequiredFrom".to_string()],
                    ..Default::default()
                }),
                to: Some(EndpointLabelSchema {
                    all_of: vec!["RequiredTo".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = engine
        .upsert_edge(a, b, "EndpointDeferred", UpsertEdgeOptions::default())
        .unwrap_err();
    assert_schema_violation_contains(err, &["from.labels.all_of", "EndpointDeferred"]);
}

#[test]
fn schema_enforcement_static_audit_keeps_replay_outside_validator() {
    let write = include_str!("../write.rs");
    let commit_start = write.find("fn commit_core_write_plan").unwrap();
    let commit_body = &write[commit_start..write.find("fn plan_ensure_node_label").unwrap()];
    assert!(commit_body.contains("validate_schema_for_wal_ops"));

    let engine = include_str!("../mod.rs");
    let replay_start = engine
        .find("fn replay_wal_generation_to_memtable_and_overlay")
        .unwrap();
    let replay_end = engine[replay_start..]
        .find("fn write_runtime_manifest")
        .map(|offset| replay_start + offset)
        .unwrap_or(engine.len());
    assert!(!engine[replay_start..replay_end].contains("validate_schema_for_wal_ops"));
}

#[test]
fn schema_management_invalid_options_are_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("schema_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let err = engine
        .check_node_schema(
            "Person",
            NodeSchema::default(),
            SchemaCheckOptions {
                chunk_size: 0,
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("invalid schema:"));

    let err = engine
        .set_edge_schema_with_options(
            "KNOWS",
            EdgeSchema::default(),
            SchemaSetOptions {
                scan_limit: Some(0),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("invalid schema:"));
}
