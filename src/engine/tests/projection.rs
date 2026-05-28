// Internal projection tests.

use crate::row_projection::{
    EdgeOutputProjection, EdgeProjectionField, NodeOutputProjection, NodeProjectionField,
    ProjectedNode, ProjectedValue, ProjectionColumn, PropertySelection, RowProjectionPlan,
    VectorSelection, DIRECT_EDGE_ALIAS, DIRECT_NODE_ALIAS,
};

fn direct_node_plan(columns: Vec<ProjectionColumn>) -> RowProjectionPlan {
    RowProjectionPlan::from_columns(columns).unwrap()
}

fn direct_edge_plan(columns: Vec<ProjectionColumn>) -> RowProjectionPlan {
    RowProjectionPlan::from_columns(columns).unwrap()
}

fn node_prop_column(key: &str, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::NodeProperty {
        alias: DIRECT_NODE_ALIAS.to_string(),
        key: key.to_string(),
        output_name: output_name.to_string(),
    }
}

fn node_metadata_column(field: NodeProjectionField, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::NodeMetadata {
        alias: DIRECT_NODE_ALIAS.to_string(),
        field,
        output_name: output_name.to_string(),
    }
}

fn node_alias_column(projection: NodeOutputProjection, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::NodeAlias {
        alias: DIRECT_NODE_ALIAS.to_string(),
        projection,
        output_name: output_name.to_string(),
    }
}

fn edge_prop_column(key: &str, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::EdgeProperty {
        alias: DIRECT_EDGE_ALIAS.to_string(),
        key: key.to_string(),
        output_name: output_name.to_string(),
    }
}

fn edge_metadata_column(field: EdgeProjectionField, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::EdgeMetadata {
        alias: DIRECT_EDGE_ALIAS.to_string(),
        field,
        output_name: output_name.to_string(),
    }
}

fn edge_alias_column(projection: EdgeOutputProjection, output_name: &str) -> ProjectionColumn {
    ProjectionColumn::EdgeAlias {
        alias: DIRECT_EDGE_ALIAS.to_string(),
        projection,
        output_name: output_name.to_string(),
    }
}

fn projected_props(props: &BTreeMap<String, PropValue>) -> BTreeMap<String, ProjectedValue> {
    props
        .iter()
        .map(|(key, value)| (key.clone(), ProjectedValue::from(value)))
        .collect()
}

fn projected_node(value: &ProjectedValue) -> &ProjectedNode {
    match value {
        ProjectedValue::Node(node) => node,
        other => panic!("expected projected node, got {other:?}"),
    }
}

#[test]
fn node_projection_metadata_only_from_active_memtable_resolves_labels() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node_with_labels(
        &engine,
        &["Person", "Topic"],
        "alice",
        &[],
        2.5,
    );

    let plan = direct_node_plan(vec![
        node_metadata_column(NodeProjectionField::Labels, "labels"),
        node_metadata_column(NodeProjectionField::Weight, "weight"),
        node_metadata_column(NodeProjectionField::UpdatedAt, "updated_at"),
    ]);
    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_node_id_rows(&[node_id], &plan, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(rows.columns, vec!["labels", "weight", "updated_at"]);
    assert_eq!(
        rows.rows[0].values[0],
        ProjectedValue::List(vec![
            ProjectedValue::String("Person".to_string()),
            ProjectedValue::String("Topic".to_string()),
        ])
    );
    assert_eq!(rows.rows[0].values[1], ProjectedValue::Float(2.5));
    assert!(matches!(rows.rows[0].values[2], ProjectedValue::Int(value) if value > 0));
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);
}

#[test]
fn node_projection_selected_keys_props_and_order_from_active_memtable() {
    let (_dir, engine) = query_test_engine();
    let alice = insert_query_node(
        &engine,
        "Person",
        "alice",
        &[
            ("name", PropValue::String("Alice".to_string())),
            ("rank", PropValue::Int(1)),
        ],
        1.0,
    );
    let bob = insert_query_node(
        &engine,
        "Person",
        "bob",
        &[
            ("name", PropValue::String("Bob".to_string())),
            ("rank", PropValue::Int(2)),
        ],
        1.0,
    );
    let element_projection = NodeOutputProjection {
        props: PropertySelection::Keys(vec!["name".to_string(), "missing".to_string()]),
        ..NodeOutputProjection::compact_element()
    };
    let plan = direct_node_plan(vec![
        node_prop_column("rank", "rank"),
        node_metadata_column(NodeProjectionField::Key, "key"),
        node_alias_column(element_projection, "node"),
        node_metadata_column(NodeProjectionField::Id, "id"),
    ]);

    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_node_id_rows(&[bob, alice, bob], &plan, true)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(rows.columns, vec!["rank", "key", "node", "id"]);
    assert!(rows.truncated);
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(rows.rows[0].values[0], ProjectedValue::Int(2));
    assert_eq!(
        rows.rows[0].values[1],
        ProjectedValue::String("bob".to_string())
    );
    assert_eq!(rows.rows[0].values[3], ProjectedValue::UInt(bob));
    let bob_node = projected_node(&rows.rows[0].values[2]);
    assert_eq!(bob_node.id, Some(bob));
    assert_eq!(bob_node.labels, Some(vec!["Person".to_string()]));
    assert_eq!(bob_node.key.as_deref(), Some("bob"));
    assert_eq!(
        bob_node.props.as_ref().unwrap(),
        &BTreeMap::from([(
            "name".to_string(),
            ProjectedValue::String("Bob".to_string())
        )])
    );
    assert_eq!(rows.rows[1].values[3], ProjectedValue::UInt(alice));
    assert_eq!(rows.rows[2].values[3], ProjectedValue::UInt(bob));
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 2);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);
}

#[test]
fn node_projection_segment_reopen_compaction_and_full_props_parity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    seed_query_test_catalog(&engine);

    let node_id = insert_query_node(
        &engine,
        "Person",
        "segment-node",
        &[("version", PropValue::String("segment".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let prop_plan = direct_node_plan(vec![
        node_prop_column("version", "version"),
        node_metadata_column(NodeProjectionField::Key, "key"),
    ]);
    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_node_id_rows(&[node_id], &prop_plan, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(
        rows.rows[0].values,
        vec![
            ProjectedValue::String("segment".to_string()),
            ProjectedValue::String("segment-node".to_string()),
        ]
    );
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let rows = reopened
        .project_node_id_rows(&[node_id], &prop_plan, false)
        .unwrap();
    assert_eq!(rows.rows[0].values[0], ProjectedValue::String("segment".to_string()));

    let updated = insert_query_node(
        &reopened,
        "Person",
        "segment-node",
        &[
            ("version", PropValue::String("compacted".to_string())),
            ("extra", PropValue::Bool(true)),
        ],
        3.0,
    );
    assert_eq!(updated, node_id);
    reopened.flush().unwrap();
    reopened.compact().unwrap().unwrap();

    let full_plan = direct_node_plan(vec![node_alias_column(
        NodeOutputProjection::full_without_vectors(),
        "node",
    )]);
    let rows = reopened
        .project_node_id_rows(&[node_id], &full_plan, false)
        .unwrap();
    let projected = projected_node(&rows.rows[0].values[0]);
    let hydrated = internal_node_record(&reopened, node_id).unwrap().unwrap();
    assert_eq!(projected.props.as_ref().unwrap(), &projected_props(&hydrated.props));
    assert_eq!(projected.weight, Some(3.0));
    assert_eq!(projected.created_at, Some(hydrated.created_at));
    assert_eq!(projected.updated_at, Some(hydrated.updated_at));
}

#[test]
fn node_projection_newer_updates_and_tombstones_shadow_segments() {
    let (_dir, engine) = query_test_engine();
    let node_id = insert_query_node(
        &engine,
        "Person",
        "shadowed",
        &[("state", PropValue::String("old".to_string()))],
        1.0,
    );
    engine.flush().unwrap();
    let updated = insert_query_node(
        &engine,
        "Person",
        "shadowed",
        &[("state", PropValue::String("new".to_string()))],
        1.0,
    );
    assert_eq!(updated, node_id);
    let plan = direct_node_plan(vec![node_prop_column("state", "state")]);
    let rows = engine
        .project_node_id_rows(&[node_id], &plan, false)
        .unwrap();
    assert_eq!(rows.rows[0].values[0], ProjectedValue::String("new".to_string()));

    engine.delete_node(node_id).unwrap();
    let err = engine
        .project_node_id_rows(&[node_id], &plan, false)
        .unwrap_err();
    assert!(err.to_string().contains("missing visible node") || err.to_string().contains("missing selected fields"));
}

#[test]
fn node_projection_vectors_are_omitted_by_default_and_opt_in_by_kind() {
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
            "Person",
            "vector-node",
            UpsertNodeOptions {
                props: query_test_props(&[("name", PropValue::String("vector".to_string()))]),
                dense_vector: Some(vec![0.1, 0.2, 0.3]),
                sparse_vector: Some(vec![(2, 1.5), (9, 0.25)]),
                ..UpsertNodeOptions::default()
            },
        )
        .unwrap();
    let without_vectors = direct_node_plan(vec![node_alias_column(
        NodeOutputProjection::full_without_vectors(),
        "node",
    )]);

    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_node_id_rows(&[node_id], &without_vectors, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    let node = projected_node(&rows.rows[0].values[0]);
    assert!(node.dense_vector.is_none());
    assert!(node.sparse_vector.is_none());
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);

    engine.flush().unwrap();

    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_node_id_rows(&[node_id], &without_vectors, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();
    let node = projected_node(&rows.rows[0].values[0]);
    assert!(node.dense_vector.is_none());
    assert!(node.sparse_vector.is_none());
    assert_eq!(counters.node_record_hydration_reads, 0);
    assert_eq!(counters.node_selected_field_batches, 1);
    assert_eq!(counters.node_selected_field_ids, 1);
    assert_eq!(counters.node_dense_vector_projection_reads, 0);
    assert_eq!(counters.node_sparse_vector_projection_reads, 0);

    for (selection, expected_dense, expected_sparse) in [
        (VectorSelection::Dense, true, false),
        (VectorSelection::Sparse, false, true),
        (VectorSelection::Both, true, true),
    ] {
        let plan = direct_node_plan(vec![node_alias_column(
            NodeOutputProjection {
                vectors: selection,
                ..NodeOutputProjection::compact_element()
            },
            "node",
        )]);
        engine.reset_query_execution_counters_for_test();
        let rows = engine
            .project_node_id_rows(&[node_id], &plan, false)
            .unwrap();
        let counters = engine.query_execution_counter_snapshot_for_test();
        let node = projected_node(&rows.rows[0].values[0]);
        assert_eq!(node.dense_vector.is_some(), expected_dense);
        assert_eq!(node.sparse_vector.is_some(), expected_sparse);
        assert_eq!(counters.node_record_hydration_reads, 0);
        assert_eq!(counters.node_selected_field_batches, 1);
        assert_eq!(counters.node_selected_field_ids, 1);
        assert_eq!(
            counters.node_dense_vector_projection_reads,
            if expected_dense { 1 } else { 0 }
        );
        assert_eq!(
            counters.node_sparse_vector_projection_reads,
            if expected_sparse { 1 } else { 0 }
        );
    }
}

#[test]
fn edge_projection_metadata_only_from_active_memtable_resolves_label_and_times() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "to", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                weight: 0.75,
                valid_from: Some(10),
                valid_to: Some(20),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let hydrated = internal_edge_record(&engine, edge_id).unwrap().unwrap();
    let plan = direct_edge_plan(vec![
        edge_metadata_column(EdgeProjectionField::From, "from"),
        edge_metadata_column(EdgeProjectionField::To, "to"),
        edge_metadata_column(EdgeProjectionField::Label, "label"),
        edge_metadata_column(EdgeProjectionField::CreatedAt, "created_at"),
        edge_metadata_column(EdgeProjectionField::UpdatedAt, "updated_at"),
        edge_metadata_column(EdgeProjectionField::ValidFrom, "valid_from"),
        edge_metadata_column(EdgeProjectionField::ValidTo, "valid_to"),
    ]);

    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_edge_id_rows(&[edge_id], &plan, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(rows.rows[0].values[0], ProjectedValue::UInt(from));
    assert_eq!(rows.rows[0].values[1], ProjectedValue::UInt(to));
    assert_eq!(
        rows.rows[0].values[2],
        ProjectedValue::String("LIKES".to_string())
    );
    assert_eq!(rows.rows[0].values[3], ProjectedValue::Int(hydrated.created_at));
    assert_eq!(rows.rows[0].values[4], ProjectedValue::Int(hydrated.updated_at));
    assert_eq!(rows.rows[0].values[5], ProjectedValue::Int(10));
    assert_eq!(rows.rows[0].values[6], ProjectedValue::Int(20));
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_batches, 1);
    assert_eq!(counters.edge_selected_field_ids, 1);
}

#[test]
fn edge_projection_selected_property_from_segment_uses_selected_fields_without_hydration() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "to", &[], 1.0);
    let edge_id = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[
                    ("status", PropValue::String("active".to_string())),
                    ("ignored", PropValue::String("cold".to_string())),
                ]),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    engine.flush().unwrap();

    let plan = direct_edge_plan(vec![
        edge_prop_column("status", "status"),
        edge_metadata_column(EdgeProjectionField::Id, "id"),
    ]);
    engine.reset_query_execution_counters_for_test();
    let rows = engine
        .project_edge_id_rows(&[edge_id, edge_id], &plan, false)
        .unwrap();
    let counters = engine.query_execution_counter_snapshot_for_test();

    assert_eq!(rows.rows.len(), 2);
    assert_eq!(
        rows.rows[0].values,
        vec![
            ProjectedValue::String("active".to_string()),
            ProjectedValue::UInt(edge_id),
        ]
    );
    assert_eq!(rows.rows[0], rows.rows[1]);
    assert_eq!(counters.edge_record_hydration_reads, 0);
    assert_eq!(counters.edge_record_hydration_calls, 0);
    assert_eq!(counters.edge_selected_field_batches, 1);
    assert_eq!(counters.edge_selected_field_ids, 1);
}

#[test]
fn edge_projection_segment_full_props_shadowing_tombstone_and_order() {
    let (_dir, engine) = query_test_engine();
    let from = insert_query_node(&engine, "Person", "from", &[], 1.0);
    let to = insert_query_node(&engine, "Person", "to", &[], 1.0);
    let other = insert_query_node(&engine, "Person", "other", &[], 1.0);
    let edge = engine
        .upsert_edge(
            from,
            to,
            "LIKES",
            UpsertEdgeOptions {
                props: query_test_props(&[("status", PropValue::String("segment".to_string()))]),
                valid_to: Some(100),
                ..UpsertEdgeOptions::default()
            },
        )
        .unwrap();
    let other_edge = engine
        .upsert_edge(other, to, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let segment_plan = direct_edge_plan(vec![
        edge_prop_column("status", "status"),
        edge_alias_column(EdgeOutputProjection::full(), "edge"),
        edge_metadata_column(EdgeProjectionField::Id, "id"),
    ]);
    let rows = engine
        .project_edge_id_rows(&[other_edge, edge], &segment_plan, false)
        .unwrap();
    assert_eq!(rows.columns, vec!["status", "edge", "id"]);
    assert_eq!(rows.rows[0].values[2], ProjectedValue::UInt(other_edge));
    assert_eq!(rows.rows[1].values[0], ProjectedValue::String("segment".to_string()));
    let full_edge = match &rows.rows[1].values[1] {
        ProjectedValue::Edge(edge) => edge,
        other => panic!("expected projected edge, got {other:?}"),
    };
    let hydrated = internal_edge_record(&engine, edge).unwrap().unwrap();
    assert_eq!(full_edge.props.as_ref().unwrap(), &projected_props(&hydrated.props));
    assert_eq!(full_edge.label.as_deref(), Some("LIKES"));

    engine.invalidate_edge(edge, 15).unwrap().unwrap();
    let rows = engine
        .project_edge_id_rows(
            &[edge],
            &direct_edge_plan(vec![edge_metadata_column(EdgeProjectionField::ValidTo, "valid_to")]),
            false,
        )
        .unwrap();
    assert_eq!(rows.rows[0].values[0], ProjectedValue::Int(15));

    engine.delete_edge(edge).unwrap();
    let err = engine
        .project_edge_id_rows(
            &[edge],
            &direct_edge_plan(vec![edge_prop_column("status", "status")]),
            false,
        )
        .unwrap_err();
    assert!(err.to_string().contains("missing visible edge") || err.to_string().contains("missing selected fields"));
}

#[test]
fn projection_does_not_add_public_api_symbols() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
    assert!(!lib.contains("ProjectedRows"));
    for path in [
        "src/lib.rs",
        "src/engine/mod.rs",
        "src/engine/query.rs",
        "overgraph-node/src/lib.rs",
        "overgraph-node/index.d.ts",
        "overgraph-node/query-types.d.ts",
        "overgraph-python/src/lib.rs",
        "overgraph-python/python/overgraph/__init__.pyi",
        "overgraph-python/python/overgraph/async_api.py",
    ] {
        let source = std::fs::read_to_string(root.join(path)).unwrap();
        for forbidden in [
            concat!("query_nodes", "_projected"),
            concat!("query_edges", "_projected"),
            concat!("query_pattern", "_projected"),
        ] {
            assert!(
                !source.contains(forbidden),
                "{path} unexpectedly contains {forbidden}"
            );
        }
    }
}
