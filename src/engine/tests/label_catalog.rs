#[test]
fn test_label_catalog_fresh_manifest_defaults() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let manifest = engine.manifest().unwrap();
    assert_eq!(
        manifest.label_token_schema_version,
        LABEL_TOKEN_SCHEMA_VERSION
    );
    assert!(manifest.node_label_tokens.is_empty());
    assert!(manifest.edge_label_tokens.is_empty());
    assert_eq!(manifest.next_node_label_id, 1);
    assert_eq!(manifest.next_edge_label_id, 1);
    assert!(engine.list_node_labels().unwrap().is_empty());
    assert!(engine.list_edge_labels().unwrap().is_empty());

    engine.close().unwrap();
}

#[test]
fn test_label_catalog_ensure_get_list_and_namespace_independence() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    assert_eq!(engine.ensure_edge_label("Person").unwrap(), 1);
    assert_eq!(engine.ensure_edge_label("WORKS_AT").unwrap(), 2);
    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    assert_eq!(engine.ensure_edge_label("Person").unwrap(), 1);

    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(engine.get_edge_label_id("Person").unwrap(), Some(1));
    assert_eq!(engine.get_edge_label_id("WORKS_AT").unwrap(), Some(2));
    assert_eq!(engine.get_node_label(1).unwrap().as_deref(), Some("Person"));
    assert_eq!(engine.get_edge_label(1).unwrap().as_deref(), Some("Person"));
    assert_eq!(
        engine.get_edge_label(2).unwrap().as_deref(),
        Some("WORKS_AT")
    );

    assert_eq!(
        engine.list_node_labels().unwrap(),
        vec![NodeLabelInfo {
            label: "Person".to_string(),
            label_id: 1,
        }]
    );
    assert_eq!(
        engine.list_edge_labels().unwrap(),
        vec![
            EdgeLabelInfo {
                label: "Person".to_string(),
                label_id: 1,
            },
            EdgeLabelInfo {
                label: "WORKS_AT".to_string(),
                label_id: 2,
            },
        ]
    );

    engine.close().unwrap();
}

#[test]
fn test_published_label_catalog_snapshot_rebuilds_only_for_token_creation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let initial = engine.published_label_catalog_snapshot_for_test();
    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    let after_node_label = engine.published_label_catalog_snapshot_for_test();
    assert!(!std::sync::Arc::ptr_eq(&initial, &after_node_label));

    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    let after_existing_ensure = engine.published_label_catalog_snapshot_for_test();
    assert!(std::sync::Arc::ptr_eq(
        &after_node_label,
        &after_existing_ensure
    ));

    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let after_existing_node_write = engine.published_label_catalog_snapshot_for_test();
    assert!(std::sync::Arc::ptr_eq(
        &after_existing_ensure,
        &after_existing_node_write
    ));

    assert_eq!(engine.ensure_edge_label("KNOWS").unwrap(), 1);
    let after_edge_label = engine.published_label_catalog_snapshot_for_test();
    assert!(!std::sync::Arc::ptr_eq(
        &after_existing_node_write,
        &after_edge_label
    ));

    engine
        .upsert_edge(alice, alice, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let after_existing_edge_write = engine.published_label_catalog_snapshot_for_test();
    assert!(std::sync::Arc::ptr_eq(
        &after_edge_label,
        &after_existing_edge_write
    ));

    engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    let after_first_use_node_write = engine.published_label_catalog_snapshot_for_test();
    assert!(!std::sync::Arc::ptr_eq(
        &after_existing_edge_write,
        &after_first_use_node_write
    ));
    assert_eq!(engine.get_node_label_id("Company").unwrap(), Some(2));

    engine.close().unwrap();
}

#[test]
fn test_label_catalog_name_validation() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let long_name = "x".repeat(256);
    for invalid in ["", " Person", "Person ", "bad\nname", "bad\u{0}name"] {
        assert!(matches!(
            engine.ensure_node_label(invalid),
            Err(EngineError::InvalidOperation(_))
        ));
        assert!(matches!(
            engine.ensure_edge_label(invalid),
            Err(EngineError::InvalidOperation(_))
        ));
        assert!(matches!(
            engine.get_node_label_id(invalid),
            Err(EngineError::InvalidOperation(_))
        ));
        assert!(matches!(
            engine.get_edge_label_id(invalid),
            Err(EngineError::InvalidOperation(_))
        ));
    }
    assert!(matches!(
        engine.ensure_node_label(&long_name),
        Err(EngineError::InvalidOperation(_))
    ));
    assert!(matches!(
        engine.ensure_edge_label(&long_name),
        Err(EngineError::InvalidOperation(_))
    ));
    assert!(matches!(
        engine.get_node_label_id(&long_name),
        Err(EngineError::InvalidOperation(_))
    ));
    assert!(matches!(
        engine.get_edge_label_id(&long_name),
        Err(EngineError::InvalidOperation(_))
    ));

    engine.close().unwrap();
}

#[test]
fn test_batch_upsert_inputs_create_named_label_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = engine
        .batch_upsert_nodes(vec![NodeInput {
            labels: vec!["Person".to_string()],
            key: "alice".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }])
        .unwrap()[0];
    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(
        engine.get_node(alice).unwrap().unwrap().labels.as_slice(),
        ["Person"]
    );

    let edge_id = engine
        .batch_upsert_edges(vec![EdgeInput {
            from: alice,
            to: alice,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }])
        .unwrap()[0];
    assert_eq!(engine.get_edge_label_id("KNOWS").unwrap(), Some(1));
    assert_eq!(
        engine.get_edge(edge_id).unwrap().unwrap().label,
        "KNOWS"
    );

    engine.close().unwrap();
}

#[test]
fn test_core_point_named_apis_return_hydrated_views_across_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let mut alice_props = BTreeMap::new();
    alice_props.insert("name".to_string(), PropValue::String("Alice".to_string()));
    let alice = engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                props: alice_props,
                weight: 0.7,
                ..Default::default()
            },
        )
        .unwrap();
    let bob = engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let knows = engine
        .upsert_edge(
            alice,
            bob,
            "KNOWS",
            UpsertEdgeOptions {
                weight: 2.5,
                valid_from: Some(10),
                valid_to: Some(100),
                ..Default::default()
            },
        )
        .unwrap();

    let alice_view = engine.get_node(alice).unwrap().unwrap();
    assert_eq!(alice_view.labels.as_slice(), ["Person"]);
    assert_eq!(alice_view.key, "alice");
    assert_eq!(
        alice_view.props.get("name"),
        Some(&PropValue::String("Alice".to_string()))
    );
    assert!((alice_view.weight - 0.7).abs() < f32::EPSILON);

    let knows_view = engine.get_edge(knows).unwrap().unwrap();
    assert_eq!(knows_view.label, "KNOWS");
    assert_eq!(knows_view.from, alice);
    assert_eq!(knows_view.to, bob);
    assert_eq!(knows_view.valid_from, 10);
    assert_eq!(knows_view.valid_to, 100);

    assert_eq!(
        engine
            .get_node_by_key("Person", "alice")
            .unwrap()
            .unwrap()
            .id,
        alice
    );
    assert_eq!(
        engine
            .get_edge_by_triple(alice, bob, "KNOWS")
            .unwrap()
            .unwrap()
            .id,
        knows
    );

    let nodes = engine.get_nodes(&[alice, 999, bob]).unwrap();
    assert_eq!(nodes[0].as_ref().unwrap().labels.as_slice(), ["Person"]);
    assert!(nodes[1].is_none());
    assert_eq!(nodes[2].as_ref().unwrap().key, "bob");

    let edges = engine.get_edges(&[knows, 999]).unwrap();
    assert_eq!(edges[0].as_ref().unwrap().label, "KNOWS");
    assert!(edges[1].is_none());

    let key_results = engine
        .get_nodes_by_keys(&[
            NodeKeyQuery {
                label: "Person".to_string(),
                key: "alice".to_string(),
            },
            NodeKeyQuery {
                label: "Person".to_string(),
                key: "bob".to_string(),
            },
            NodeKeyQuery {
                label: "MissingButValid".to_string(),
                key: "alice".to_string(),
            },
        ])
        .unwrap();
    assert_eq!(key_results[0].as_ref().unwrap().id, alice);
    assert_eq!(
        key_results[0].as_ref().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert_eq!(key_results[1].as_ref().unwrap().id, bob);
    assert_eq!(
        key_results[1].as_ref().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert!(key_results[2].is_none());
    assert_eq!(engine.get_node_label_id("MissingButValid").unwrap(), None);

    assert!(engine
        .get_node_by_key("MissingButValid", "alice")
        .unwrap()
        .is_none());
    assert!(engine
        .get_edge_by_triple(alice, bob, "MISSING")
        .unwrap()
        .is_none());
    assert!(matches!(
        engine.get_node_by_key(" Person", "alice"),
        Err(EngineError::InvalidOperation(_))
    ));
    assert!(matches!(
        engine.get_edge_by_triple(alice, bob, "KNOWS\n"),
        Err(EngineError::InvalidOperation(_))
    ));

    let invalidated = engine.invalidate_edge(knows, 55).unwrap().unwrap();
    assert_eq!(invalidated.label, "KNOWS");
    assert_eq!(invalidated.valid_to, 55);

    engine.flush().unwrap();
    assert_eq!(
        engine.get_node(alice).unwrap().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert_eq!(engine.get_edge(knows).unwrap().unwrap().label, "KNOWS");

    let carol = engine
        .upsert_node("Person", "carol", UpsertNodeOptions::default())
        .unwrap();
    let _mentors = engine
        .upsert_edge(alice, carol, "MENTORS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    engine.compact().unwrap();

    assert_eq!(
        engine.get_node(carol).unwrap().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert_eq!(engine.get_edge(knows).unwrap().unwrap().label, "KNOWS");
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(
        reopened.get_node(alice).unwrap().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert_eq!(
        reopened.get_edge(knows).unwrap().unwrap().label,
        "KNOWS"
    );
    reopened.close().unwrap();
}

#[test]
fn test_core_point_first_use_tokens_share_wal_batch_with_records() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(alice, bob, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let all_ops = WalReader::read_generation(&db_path, 0).unwrap();
    let ops = marker_free_wal_records(&all_ops);
    assert!(matches!(
        &ops[0].1,
        WalOp::EnsureNodeLabel { label, label_id } if label == "Person" && *label_id == 1
    ));
    assert!(matches!(
        &ops[1].1,
        WalOp::UpsertNode(node) if node.id == alice && node.label_ids.as_slice() == [1]
    ));
    assert!(matches!(
        &ops[2].1,
        WalOp::UpsertNode(node) if node.id == bob && node.label_ids.as_slice() == [1]
    ));
    assert!(matches!(
        &ops[3].1,
        WalOp::EnsureEdgeLabel { label, label_id } if label == "KNOWS" && *label_id == 1
    ));
    assert!(
        matches!(&ops[4].1, WalOp::UpsertEdge(edge) if edge.id == edge_id && edge.label_id == 1)
    );

    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert!(disk_manifest.node_label_tokens.is_empty());
    assert!(disk_manifest.edge_label_tokens.is_empty());

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(reopened.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(reopened.get_edge_label_id("KNOWS").unwrap(), Some(1));
    assert_eq!(
        reopened.get_node(alice).unwrap().unwrap().labels.as_slice(),
        ["Person"]
    );
    assert_eq!(
        reopened.get_edge(edge_id).unwrap().unwrap().label,
        "KNOWS"
    );
    reopened.close().unwrap();
}

#[test]
fn test_batch_upserts_stage_distinct_named_tokens_once_before_records() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let node_ids = engine
        .batch_upsert_nodes(vec![
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "alice".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "bob".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Company".to_string()],
                key: "acme".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            },
        ])
        .unwrap();
    assert_eq!(node_ids.len(), 3);

    let edge_ids = engine
        .batch_upsert_edges(vec![
            EdgeInput {
                from: node_ids[0],
                to: node_ids[1],
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: node_ids[0],
                to: node_ids[2],
                label: "WORKS_AT".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: node_ids[0],
                to: node_ids[1],
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                valid_from: None,
                valid_to: None,
            },
        ])
        .unwrap();
    assert_eq!(edge_ids[0], edge_ids[2]);

    let all_ops = WalReader::read_generation(&db_path, 0).unwrap();
    let ops = marker_free_wal_records(&all_ops);
    assert!(matches!(
        &ops[0].1,
        WalOp::EnsureNodeLabel { label, label_id } if label == "Person" && *label_id == 1
    ));
    assert!(matches!(
        &ops[1].1,
        WalOp::EnsureNodeLabel { label, label_id } if label == "Company" && *label_id == 2
    ));
    assert!(matches!(&ops[2].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [1]));
    assert!(matches!(&ops[3].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [1]));
    assert!(matches!(&ops[4].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [2]));
    assert!(matches!(
        &ops[5].1,
        WalOp::EnsureEdgeLabel { label, label_id } if label == "KNOWS" && *label_id == 1
    ));
    assert!(matches!(
        &ops[6].1,
        WalOp::EnsureEdgeLabel { label, label_id } if label == "WORKS_AT" && *label_id == 2
    ));
    assert!(matches!(&ops[7].1, WalOp::UpsertEdge(edge) if edge.label_id == 1));
    assert!(matches!(&ops[8].1, WalOp::UpsertEdge(edge) if edge.label_id == 2));
    assert!(matches!(&ops[9].1, WalOp::UpsertEdge(edge) if edge.label_id == 1));
    assert_eq!(
        ops.iter()
            .filter(
                |(_, op)| matches!(op, WalOp::EnsureNodeLabel { label, .. } if label == "Person")
            )
            .count(),
        1
    );
    assert_eq!(
        ops.iter()
            .filter(|(_, op)| matches!(op, WalOp::EnsureEdgeLabel { label, .. } if label == "KNOWS"))
            .count(),
        1
    );

    engine.close().unwrap();
}

#[test]
fn test_label_resolution_plan_caches_distinct_node_labels_per_request() {
    let mut manifest = default_manifest();
    manifest.node_label_tokens.insert("Person".to_string(), 7);
    manifest.next_node_label_id = 8;
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let mut plan = LabelResolutionPlan::from_catalog(&catalog);

    let label_ids = plan
        .resolve_node_label_ids_for_request(
            ["Person", "Person", "Company", "Company", "Person"],
        )
        .unwrap();

    assert_eq!(label_ids, vec![7, 7, 8, 8, 7]);
    assert_eq!(plan.node_label_resolve_calls, 2);
    assert_eq!(plan.node_labels_to_create, vec![("Company".to_string(), 8)]);
}

#[test]
fn test_node_label_set_resolution_validates_before_token_reservation() {
    let manifest = default_manifest();
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let mut plan = LabelResolutionPlan::from_catalog(&catalog);

    let err = match ValidatedNodeLabelList::new(["LeakyLabel", "LeakyLabel"]) {
        Ok(_) => panic!("duplicate labels should be rejected"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicate label"));
    assert_eq!(plan.node_label_resolve_calls, 0);
    assert!(plan.node_labels_to_create.is_empty());
    assert!(plan.new_node_label_to_id.is_empty());

    let too_many = [
        "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10", "L11",
    ];
    let err = match ValidatedNodeLabelList::new(too_many) {
        Ok(_) => panic!("too many labels should be rejected"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("at most 10 labels"));
    assert_eq!(plan.node_label_resolve_calls, 0);
    assert!(plan.node_labels_to_create.is_empty());
    assert!(plan.new_node_label_to_id.is_empty());

    let err = match ValidatedNodeLabelList::new([" LeakyLabel"]) {
        Ok(_) => panic!("invalid label should be rejected"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("leading or trailing whitespace"));
    assert_eq!(plan.node_label_resolve_calls, 0);
    assert!(plan.node_labels_to_create.is_empty());
    assert!(plan.new_node_label_to_id.is_empty());

    let labels = ValidatedNodeLabelList::new(["Person", "Company"]).unwrap();
    let label_set = plan
        .resolve_validated_node_label_set_for_write(&labels)
        .unwrap();
    assert_eq!(label_set.as_slice(), &[1, 2]);
    assert_eq!(plan.node_label_resolve_calls, 2);
    assert_eq!(
        plan.node_labels_to_create,
        vec![("Person".to_string(), 1), ("Company".to_string(), 2)]
    );
}

#[test]
fn test_node_label_set_batch_validation_happens_before_token_reservation() {
    let manifest = default_manifest();
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let plan = LabelResolutionPlan::from_catalog(&catalog);
    let requests = [vec!["WouldHaveBeenReserved"], vec!["LeakyLabel", "LeakyLabel"]];

    let mut validated_labels = Vec::with_capacity(requests.len());
    let err = requests
        .iter()
        .find_map(|labels| match ValidatedNodeLabelList::new(labels.iter().copied()) {
            Ok(labels) => {
                validated_labels.push(labels);
                None
            }
            Err(err) => Some(err),
        })
        .expect("duplicate labels should be rejected");

    assert!(err.to_string().contains("duplicate label"));
    assert_eq!(validated_labels.len(), 1);
    assert_eq!(plan.node_label_resolve_calls, 0);
    assert!(plan.node_labels_to_create.is_empty());
    assert!(plan.new_node_label_to_id.is_empty());
}

#[test]
fn test_node_label_set_resolution_is_distinct_and_deterministic() {
    let mut manifest = default_manifest();
    manifest.node_label_tokens.insert("Person".to_string(), 7);
    manifest.next_node_label_id = 8;
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let mut plan = LabelResolutionPlan::from_catalog(&catalog);
    let requests = [
        vec!["Company", "Person"],
        vec!["Team", "Company", "Person"],
    ];

    let validated_labels = requests
        .iter()
        .map(|labels| ValidatedNodeLabelList::new(labels.iter().copied()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let label_sets = plan
        .resolve_validated_node_label_sets_for_request(&validated_labels)
        .unwrap();

    assert_eq!(label_sets[0].as_slice(), &[7, 8]);
    assert_eq!(label_sets[1].as_slice(), &[7, 8, 9]);
    assert_eq!(plan.node_label_resolve_calls, 3);
    assert_eq!(
        plan.node_labels_to_create,
        vec![("Company".to_string(), 8), ("Team".to_string(), 9)]
    );
}

#[test]
fn test_resolved_node_label_filter_read_semantics_are_numeric_and_deterministic() {
    let mut manifest = default_manifest();
    manifest.node_label_tokens.insert("Person".to_string(), 7);
    manifest.node_label_tokens.insert("Company".to_string(), 3);
    manifest.next_node_label_id = 8;
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let snapshot = ReadLabelCatalogSnapshot::from_runtime(&catalog);

    let any = snapshot
        .resolve_node_label_filter_request(Some(&NodeLabelFilter {
            labels: vec![
                "Missing".to_string(),
                "Person".to_string(),
                "Company".to_string(),
            ],
            mode: LabelMatchMode::Any,
        }))
        .unwrap();
    assert_eq!(any.mode(), Some(LabelMatchMode::Any));
    assert!(!any.is_empty_constraint());
    assert_eq!(any.label_ids().unwrap().as_slice(), &[3, 7]);
    assert!(matches!(
        any,
        ResolvedNodeLabelFilter::LabelSet {
            unknown_label_count: 1,
            ..
        }
    ));

    let all = snapshot
        .resolve_node_label_filter_request(Some(&NodeLabelFilter {
            labels: vec!["Person".to_string(), "Missing".to_string()],
            mode: LabelMatchMode::All,
        }))
        .unwrap();
    assert_eq!(all.mode(), Some(LabelMatchMode::All));
    assert!(all.is_empty_constraint());
    assert!(matches!(
        all,
        ResolvedNodeLabelFilter::Empty {
            unknown_label_count: 1,
            ..
        }
    ));

    assert_eq!(
        snapshot.resolve_node_label_filter_request(None).unwrap(),
        ResolvedNodeLabelFilter::Unconstrained
    );
}

#[test]
fn test_label_resolution_plan_caches_distinct_edge_labels_per_request() {
    let mut manifest = default_manifest();
    manifest.edge_label_tokens.insert("KNOWS".to_string(), 4);
    manifest.next_edge_label_id = 5;
    let catalog = RuntimeLabelCatalog::from_manifest(&manifest).unwrap();
    let mut plan = LabelResolutionPlan::from_catalog(&catalog);

    let label_ids = plan
        .resolve_edge_label_ids_for_request(
            ["KNOWS", "KNOWS", "WORKS_AT", "WORKS_AT", "KNOWS"],
        )
        .unwrap();

    assert_eq!(label_ids, vec![4, 4, 5, 5, 4]);
    assert_eq!(plan.edge_label_resolve_calls, 2);
    assert_eq!(plan.edge_labels_to_create, vec![("WORKS_AT".to_string(), 5)]);
}

#[test]
fn test_graph_patch_stages_named_tokens_before_dependent_ops() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let result = engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "alice".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Company".to_string()],
                    key: "acme".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "bob".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
            ],
            upsert_edges: vec![
                EdgeInput {
                    from: 1,
                    to: 2,
                    label: "WORKS_AT".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                },
                EdgeInput {
                    from: 3,
                    to: 2,
                    label: "WORKS_AT".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                },
            ],
            invalidate_edges: Vec::new(),
            delete_node_ids: Vec::new(),
            delete_edge_ids: Vec::new(),
        })
        .unwrap();
    assert_eq!(result.node_ids, vec![1, 2, 3]);
    assert_eq!(result.edge_ids.len(), 2);

    let all_ops = WalReader::read_generation(&db_path, 0).unwrap();
    let ops = marker_free_wal_records(&all_ops);
    assert!(matches!(
        &ops[0].1,
        WalOp::EnsureNodeLabel { label, label_id } if label == "Person" && *label_id == 1
    ));
    assert!(matches!(
        &ops[1].1,
        WalOp::EnsureNodeLabel { label, label_id } if label == "Company" && *label_id == 2
    ));
    assert!(matches!(
        &ops[2].1,
        WalOp::EnsureEdgeLabel { label, label_id } if label == "WORKS_AT" && *label_id == 1
    ));
    assert!(matches!(&ops[3].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [1]));
    assert!(matches!(&ops[4].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [2]));
    assert!(matches!(&ops[5].1, WalOp::UpsertNode(node) if node.label_ids.as_slice() == [1]));
    assert!(matches!(&ops[6].1, WalOp::UpsertEdge(edge) if edge.label_id == 1));
    assert!(matches!(&ops[7].1, WalOp::UpsertEdge(edge) if edge.label_id == 1));
    assert_eq!(
        ops.iter()
            .filter(
                |(_, op)| matches!(op, WalOp::EnsureNodeLabel { label, .. } if label == "Person")
            )
            .count(),
        1
    );
    assert_eq!(
        ops.iter()
            .filter(|(_, op)| matches!(op, WalOp::EnsureEdgeLabel { label, .. } if label == "WORKS_AT"))
            .count(),
        1
    );

    engine.close().unwrap();
}

#[test]
fn test_graph_patch_failure_does_not_publish_staged_token() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let err = engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![NodeInput {
                labels: vec!["LeakyPatchLabel".to_string()],
                key: "bad-vector".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: Some(vec![1.0, 0.0]),
                sparse_vector: None,
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("dimension"));
    assert_eq!(engine.get_node_label_id("LeakyPatchLabel").unwrap(), None);

    engine.close().unwrap();
}

#[test]
fn test_graph_patch_invalidation_uses_staged_edge_overlay() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let mut v1 = BTreeMap::new();
    v1.insert("version".to_string(), PropValue::Int(1));
    let edge_id = engine
        .upsert_edge(
            a,
            b,
            "KNOWS",
            UpsertEdgeOptions {
                props: v1,
                weight: 1.0,
                valid_from: Some(10),
                valid_to: Some(9_999),
            },
        )
        .unwrap();

    let mut v2 = BTreeMap::new();
    v2.insert("version".to_string(), PropValue::Int(2));
    let result = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                label: "KNOWS".to_string(),
                props: v2,
                weight: 2.5,
                valid_from: Some(20),
                valid_to: Some(i64::MAX),
            }],
            invalidate_edges: vec![(edge_id, 1234)],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(result.edge_ids, vec![edge_id]);

    let edge = engine.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(edge.props.get("version"), Some(&PropValue::Int(2)));
    assert!((edge.weight - 2.5).abs() < f32::EPSILON);
    assert_eq!(edge.valid_from, 20);
    assert_eq!(edge.valid_to, 1234);

    engine.close().unwrap();
}

#[test]
fn test_graph_patch_delete_node_cascades_staged_new_edge_once() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let result = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            }],
            delete_node_ids: vec![a],
            ..Default::default()
        })
        .unwrap();
    let edge_id = result.edge_ids[0];

    assert!(engine.get_edge(edge_id).unwrap().is_none());
    assert!(engine.get_node(a).unwrap().is_none());
    let ops = WalReader::read_generation(&db_path, 0).unwrap();
    assert_eq!(
        ops.iter()
            .filter(|(_, op)| matches!(op, WalOp::DeleteEdge { id, .. } if *id == edge_id))
            .count(),
        1
    );

    engine.close().unwrap();
}

#[test]
fn test_graph_patch_delete_node_cascades_staged_existing_edge_update() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let mut props = BTreeMap::new();
    props.insert("version".to_string(), PropValue::Int(2));
    let result = engine
        .graph_patch(GraphPatch {
            upsert_edges: vec![EdgeInput {
                from: a,
                to: b,
                label: "KNOWS".to_string(),
                props,
                weight: 3.0,
                valid_from: None,
                valid_to: None,
            }],
            delete_node_ids: vec![a],
            ..Default::default()
        })
        .unwrap();

    assert_eq!(result.edge_ids, vec![edge_id]);
    assert!(engine.get_edge(edge_id).unwrap().is_none());
    assert!(engine.get_node(a).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_graph_patch_explicit_delete_and_cascade_emit_one_edge_tombstone() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    engine
        .graph_patch(GraphPatch {
            delete_edge_ids: vec![edge_id],
            delete_node_ids: vec![a],
            ..Default::default()
        })
        .unwrap();

    assert!(engine.get_edge(edge_id).unwrap().is_none());
    assert!(engine.get_node(a).unwrap().is_none());
    assert!(engine.get_node(b).unwrap().is_some());
    let ops = WalReader::read_generation(&db_path, 0).unwrap();
    assert_eq!(
        ops.iter()
            .filter(|(_, op)| matches!(op, WalOp::DeleteEdge { id, .. } if *id == edge_id))
            .count(),
        1
    );

    engine.close().unwrap();
}

#[test]
fn test_named_batch_planning_reuses_existing_ids_across_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let node_segment = engine
        .upsert_node("Person", "segment", UpsertNodeOptions::default())
        .unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let c = engine
        .upsert_node("Person", "c", UpsertNodeOptions::default())
        .unwrap();
    let d = engine
        .upsert_node("Person", "d", UpsertNodeOptions::default())
        .unwrap();
    let e = engine
        .upsert_node("Person", "e", UpsertNodeOptions::default())
        .unwrap();
    let f = engine
        .upsert_node("Person", "f", UpsertNodeOptions::default())
        .unwrap();
    let edge_segment = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let node_immutable = engine
        .upsert_node("Person", "immutable", UpsertNodeOptions::default())
        .unwrap();
    let edge_immutable = engine
        .upsert_edge(c, d, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.freeze_memtable().unwrap();

    let node_active = engine
        .upsert_node("Person", "active", UpsertNodeOptions::default())
        .unwrap();
    let edge_active = engine
        .upsert_edge(e, f, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let node_ids = engine
        .batch_upsert_nodes(vec![
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "segment".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "immutable".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "active".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                dense_vector: None,
                sparse_vector: None,
            },
        ])
        .unwrap();
    assert_eq!(node_ids, vec![node_segment, node_immutable, node_active]);

    let edge_ids = engine
        .batch_upsert_edges(vec![
            EdgeInput {
                from: a,
                to: b,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: c,
                to: d,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: e,
                to: f,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 2.0,
                valid_from: None,
                valid_to: None,
            },
        ])
        .unwrap();
    assert_eq!(edge_ids, vec![edge_segment, edge_immutable, edge_active]);

    engine.close().unwrap();
}

#[test]
fn test_named_graph_patch_reuses_existing_ids_across_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let node_segment = engine
        .upsert_node("Person", "segment", UpsertNodeOptions::default())
        .unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let c = engine
        .upsert_node("Person", "c", UpsertNodeOptions::default())
        .unwrap();
    let d = engine
        .upsert_node("Person", "d", UpsertNodeOptions::default())
        .unwrap();
    let e = engine
        .upsert_node("Person", "e", UpsertNodeOptions::default())
        .unwrap();
    let f = engine
        .upsert_node("Person", "f", UpsertNodeOptions::default())
        .unwrap();
    let edge_segment = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let node_immutable = engine
        .upsert_node("Person", "immutable", UpsertNodeOptions::default())
        .unwrap();
    let edge_immutable = engine
        .upsert_edge(c, d, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.freeze_memtable().unwrap();

    let node_active = engine
        .upsert_node("Person", "active", UpsertNodeOptions::default())
        .unwrap();
    let edge_active = engine
        .upsert_edge(e, f, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let result = engine
        .graph_patch(GraphPatch {
            upsert_nodes: vec![
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "segment".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "immutable".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "active".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
            ],
            upsert_edges: vec![
                EdgeInput {
                    from: a,
                    to: b,
                    label: "KNOWS".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    valid_from: None,
                    valid_to: None,
                },
                EdgeInput {
                    from: c,
                    to: d,
                    label: "KNOWS".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    valid_from: None,
                    valid_to: None,
                },
                EdgeInput {
                    from: e,
                    to: f,
                    label: "KNOWS".to_string(),
                    props: BTreeMap::new(),
                    weight: 2.0,
                    valid_from: None,
                    valid_to: None,
                },
            ],
            ..Default::default()
        })
        .unwrap();

    assert_eq!(result.node_ids, vec![node_segment, node_immutable, node_active]);
    assert_eq!(result.edge_ids, vec![edge_segment, edge_immutable, edge_active]);
    assert_eq!(
        engine
            .get_node_by_key("Person", "segment")
            .unwrap()
            .unwrap()
            .id,
        node_segment
    );
    assert_eq!(
        engine
            .get_node_by_key("Person", "immutable")
            .unwrap()
            .unwrap()
            .id,
        node_immutable
    );
    assert_eq!(
        engine
            .get_node_by_key("Person", "active")
            .unwrap()
            .unwrap()
            .id,
        node_active
    );
    assert_eq!(
        engine.get_edge_by_triple(a, b, "KNOWS").unwrap().unwrap().id,
        edge_segment
    );
    assert_eq!(
        engine.get_edge_by_triple(c, d, "KNOWS").unwrap().unwrap().id,
        edge_immutable
    );
    assert_eq!(
        engine.get_edge_by_triple(e, f, "KNOWS").unwrap().unwrap().id,
        edge_active
    );

    engine.close().unwrap();
}

#[test]
fn test_named_batch_edge_lookup_respects_tombstone_shadowing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let old_edge = engine
        .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();
    engine.delete_edge(old_edge).unwrap();

    let ids = engine
        .batch_upsert_edges(vec![EdgeInput {
            from: a,
            to: b,
            label: "KNOWS".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }])
        .unwrap();

    assert_ne!(ids[0], old_edge);
    assert!(engine.get_edge(old_edge).unwrap().is_none());
    assert_eq!(
        engine
            .get_edge_by_triple(a, b, "KNOWS")
            .unwrap()
            .unwrap()
            .id,
        ids[0]
    );

    engine.close().unwrap();
}

#[test]
fn test_explicit_ensure_is_wal_durable_without_foreground_manifest_write() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    assert_eq!(engine.ensure_edge_label("KNOWS").unwrap(), 1);

    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert!(disk_manifest.node_label_tokens.is_empty());
    assert!(disk_manifest.edge_label_tokens.is_empty());

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(reopened.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(reopened.get_edge_label_id("KNOWS").unwrap(), Some(1));
    reopened.close().unwrap();
}

#[test]
fn test_metadata_writes_checkpoint_existing_wal_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    assert_eq!(db.ensure_node_label("Article").unwrap(), 1);
    assert_eq!(db.ensure_edge_label("MENTIONS").unwrap(), 1);
    assert_eq!(db.ensure_node_label("Expiring").unwrap(), 2);
    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert!(disk_manifest.node_label_tokens.is_empty());
    assert!(disk_manifest.edge_label_tokens.is_empty());

    db.ensure_node_property_index("Article", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    db.ensure_edge_property_index("MENTIONS", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("rank").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap();
    db.set_prune_policy(
        "expiring",
        PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.1),
            label: Some("Expiring".to_string()),
        },
    )
    .unwrap();

    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert_eq!(disk_manifest.node_label_tokens.get("Article"), Some(&1));
    assert_eq!(disk_manifest.node_label_tokens.get("Expiring"), Some(&2));
    assert_eq!(disk_manifest.edge_label_tokens.get("MENTIONS"), Some(&1));
    assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
        entry.target
            == SecondaryIndexTarget::NodeProperty {
                label_id: 1,
                prop_key: "status".to_string(),
            }
    }));
    assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
        entry.target
            == SecondaryIndexTarget::EdgeProperty {
                label_id: 1,
                prop_key: "rank".to_string(),
            }
    }));
    assert_eq!(
        disk_manifest
            .prune_policies
            .get("expiring")
            .and_then(|policy| policy.label.as_deref()),
        Some("Expiring")
    );

    db.close().unwrap();
    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(reopened.get_node_label_id("Article").unwrap(), Some(1));
    assert_eq!(reopened.get_node_label_id("Expiring").unwrap(), Some(2));
    assert_eq!(reopened.get_edge_label_id("MENTIONS").unwrap(), Some(1));
    assert_eq!(
        reopened.list_prune_policies().unwrap()[0]
            .policy
            .label
            .as_deref(),
        Some("Expiring")
    );
    reopened.close().unwrap();
}

#[test]
fn test_flush_persists_label_tokens_before_wal_retirement() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    assert_eq!(engine.ensure_node_label("Person").unwrap(), 1);
    assert_eq!(engine.ensure_edge_label("KNOWS").unwrap(), 1);
    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(alice, alice, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert_eq!(disk_manifest.node_label_tokens.get("Person"), Some(&1));
    assert_eq!(disk_manifest.edge_label_tokens.get("KNOWS"), Some(&1));
    assert!(!wal_generation_path(&db_path, 0).exists());

    engine.close().unwrap();
}

#[test]
fn test_background_publish_does_not_checkpoint_active_group_commit_token() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::GroupCommit {
            interval_ms: 60_000,
            soft_trigger_bytes: 1 << 20,
            hard_cap_bytes: 1 << 21,
        },
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    assert_eq!(engine.ensure_node_label("DurableBeforeFlush").unwrap(), 1);
    engine
        .upsert_node("DurableBeforeFlush", "alice", UpsertNodeOptions::default())
        .unwrap();
    engine.freeze_memtable().unwrap();

    assert_eq!(engine.ensure_node_label("ActiveOnly").unwrap(), 2);
    engine.flush().unwrap();

    let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert_eq!(
        disk_manifest.node_label_tokens.get("DurableBeforeFlush"),
        Some(&1)
    );
    assert_eq!(disk_manifest.node_label_tokens.get("ActiveOnly"), None);
    assert!(
        !wal_generation_path(&db_path, 0).exists(),
        "retired WAL generation should be removable after its token is checkpointed"
    );

    engine.close().unwrap();
    let closed_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
    assert_eq!(
        closed_manifest.node_label_tokens.get("ActiveOnly"),
        Some(&2)
    );
}

#[test]
fn test_label_token_wal_replay_restores_catalog_and_records() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "KNOWS".to_string(),
                label_id: 1,
            },
            2,
        )
        .unwrap();
    writer
        .append(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(1).unwrap(),
                key: "alice".to_string(),
                props: BTreeMap::new(),
                created_at: 1,
                updated_at: 1,
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            3,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(engine.get_node_label_id("Person").unwrap(), Some(1));
    assert_eq!(engine.get_edge_label_id("KNOWS").unwrap(), Some(1));
    assert_eq!(engine.get_node(1).unwrap().unwrap().key, "alice");
    engine.close().unwrap();
}

#[test]
fn test_label_token_wal_replay_rejects_conflicting_name_or_id() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 2,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Person".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureNodeLabel {
                label: "Company".to_string(),
                label_id: 1,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "KNOWS".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "LIKES".to_string(),
                label_id: 1,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "KNOWS".to_string(),
                label_id: 1,
            },
            1,
        )
        .unwrap();
    writer
        .append(
            &WalOp::EnsureEdgeLabel {
                label: "KNOWS".to_string(),
                label_id: 2,
            },
            2,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));
}

#[test]
fn test_wal_replay_rejects_dependent_record_with_missing_token() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    write_manifest(&db_path, &default_manifest()).unwrap();

    let mut writer = WalWriter::open_generation(&db_path, 0).unwrap();
    writer
        .append(
            &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(99).unwrap(),
                key: "missing-token".to_string(),
                props: BTreeMap::new(),
                created_at: 1,
                updated_at: 1,
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            }),
            1,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(matches!(
        DatabaseEngine::open(&db_path, &DbOptions::default()),
        Err(EngineError::CorruptWal(_))
    ));
}

#[test]
fn test_public_label_and_edge_label_scans_are_read_only_and_hydrate_views() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = db
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = db
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let acme = db
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    let knows = db
        .upsert_edge(alice, bob, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let works_at = db
        .upsert_edge(alice, acme, "WORKS_AT", UpsertEdgeOptions::default())
        .unwrap();

    assert_eq!(db.nodes_by_labels("Person").unwrap(), vec![alice, bob]);
    assert_eq!(db.count_nodes_by_labels("Person").unwrap(), 2);
    let people = db.get_nodes_by_labels("Person").unwrap();
    assert_eq!(people.iter().map(|node| node.id).collect::<Vec<_>>(), vec![alice, bob]);
    assert!(people
        .iter()
        .all(|node| node.labels.as_slice() == ["Person"]));

    let page = db
        .nodes_by_labels_paged(
            "Person",
            &PageRequest {
                limit: Some(1),
                after: None,
            },
        )
        .unwrap();
    assert_eq!(page.items, vec![alice]);
    assert_eq!(page.next_cursor, Some(alice));
    let hydrated_page = db
        .get_nodes_by_labels_paged(
            "Person",
            &PageRequest {
                limit: Some(1),
                after: page.next_cursor,
            },
        )
        .unwrap();
    assert_eq!(
        hydrated_page
            .items
            .iter()
            .map(|node| (node.id, node.labels[0].as_str()))
            .collect::<Vec<_>>(),
        vec![(bob, "Person")]
    );

    assert_eq!(db.edges_by_label("KNOWS").unwrap(), vec![knows]);
    assert_eq!(db.count_edges_by_label("WORKS_AT").unwrap(), 1);
    assert_eq!(
        db.get_edges_by_label("WORKS_AT")
            .unwrap()
            .iter()
            .map(|edge| (edge.id, edge.label.as_str()))
            .collect::<Vec<_>>(),
        vec![(works_at, "WORKS_AT")]
    );
    assert_eq!(
        db.edges_by_label_paged("KNOWS", &PageRequest::default())
            .unwrap()
            .items,
        vec![knows]
    );
    assert_eq!(
        db.get_edges_by_label_paged("KNOWS", &PageRequest::default())
            .unwrap()
            .items
            .iter()
            .map(|edge| (edge.id, edge.label.as_str()))
            .collect::<Vec<_>>(),
        vec![(knows, "KNOWS")]
    );

    let node_catalog_len = db.list_node_labels().unwrap().len();
    let edge_catalog_len = db.list_edge_labels().unwrap().len();
    assert_eq!(db.nodes_by_labels("Missing").unwrap(), Vec::<u64>::new());
    assert_eq!(
        db.nodes_by_labels_paged("Missing", &PageRequest::default())
            .unwrap()
            .items,
        Vec::<u64>::new()
    );
    assert_eq!(db.get_nodes_by_labels("Missing").unwrap(), Vec::<NodeView>::new());
    assert_eq!(
        db.get_nodes_by_labels_paged("Missing", &PageRequest::default())
            .unwrap()
            .items,
        Vec::<NodeView>::new()
    );
    assert_eq!(db.count_nodes_by_labels("Missing").unwrap(), 0);
    assert_eq!(db.edges_by_label("MISSING").unwrap(), Vec::<u64>::new());
    assert_eq!(
        db.edges_by_label_paged("MISSING", &PageRequest::default())
            .unwrap()
            .items,
        Vec::<u64>::new()
    );
    assert_eq!(db.get_edges_by_label("MISSING").unwrap(), Vec::<EdgeView>::new());
    assert_eq!(
        db.get_edges_by_label_paged("MISSING", &PageRequest::default())
            .unwrap()
            .items,
        Vec::<EdgeView>::new()
    );
    assert_eq!(db.count_edges_by_label("MISSING").unwrap(), 0);
    assert_eq!(db.get_node_label_id("Missing").unwrap(), None);
    assert_eq!(db.get_edge_label_id("MISSING").unwrap(), None);
    assert_eq!(db.list_node_labels().unwrap().len(), node_catalog_len);
    assert_eq!(db.list_edge_labels().unwrap().len(), edge_catalog_len);

    assert!(db.nodes_by_labels(" Person").is_err());
    assert!(db.edges_by_label("KNOWS\n").is_err());
}

#[test]
fn test_public_label_property_and_time_queries_preserve_empty_and_validation_semantics() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut red_low = BTreeMap::new();
    red_low.insert("color".to_string(), PropValue::String("red".to_string()));
    red_low.insert("score".to_string(), PropValue::Int(10));
    let mut red_high = BTreeMap::new();
    red_high.insert("color".to_string(), PropValue::String("red".to_string()));
    red_high.insert("score".to_string(), PropValue::Int(20));
    let mut other = BTreeMap::new();
    other.insert("color".to_string(), PropValue::String("red".to_string()));
    other.insert("score".to_string(), PropValue::Int(15));

    let article_a = db
        .upsert_node(
            "Article",
            "a",
            UpsertNodeOptions {
                props: red_low,
                ..Default::default()
            },
        )
        .unwrap();
    let article_b = db
        .upsert_node(
            "Article",
            "b",
            UpsertNodeOptions {
                props: red_high,
                ..Default::default()
            },
        )
        .unwrap();
    db.upsert_node(
        "Note",
        "n",
        UpsertNodeOptions {
            props: other,
            ..Default::default()
        },
    )
    .unwrap();

    let red = PropValue::String("red".to_string());
    assert_eq!(
        db.find_nodes("Article", "color", &red).unwrap(),
        vec![article_a, article_b]
    );
    assert_eq!(
        db.find_nodes_paged(
            "Article",
            "color",
            &red,
            &PageRequest {
                limit: Some(1),
                after: None,
            },
        )
        .unwrap()
        .items,
        vec![article_a]
    );

    let lower = PropertyRangeBound::Included(PropValue::Int(10));
    let upper = PropertyRangeBound::Included(PropValue::Int(20));
    assert_eq!(
        db.find_nodes_range("Article", "score", Some(&lower), Some(&upper))
            .unwrap(),
        vec![article_a, article_b]
    );
    let range_page = db
        .find_nodes_range_paged(
            "Article",
            "score",
            Some(&lower),
            Some(&upper),
            &PropertyRangePageRequest {
                limit: Some(1),
                after: None,
            },
        )
        .unwrap();
    assert_eq!(range_page.items, vec![article_a]);
    assert!(range_page.next_cursor.is_some());

    assert_eq!(
        db.find_nodes_by_time_range("Article", i64::MIN, i64::MAX)
            .unwrap(),
        vec![article_a, article_b]
    );
    assert_eq!(
        db.find_nodes_by_time_range_paged(
            "Article",
            i64::MIN,
            i64::MAX,
            &PageRequest {
                limit: Some(1),
                after: None,
            },
        )
        .unwrap()
        .items,
        vec![article_a]
    );

    let catalog_len = db.list_node_labels().unwrap().len();
    assert_eq!(db.find_nodes("Missing", "color", &red).unwrap(), Vec::<u64>::new());
    assert_eq!(
        db.find_nodes_paged("Missing", "color", &red, &PageRequest::default())
            .unwrap()
            .items,
        Vec::<u64>::new()
    );
    assert_eq!(
        db.find_nodes_range("Missing", "score", Some(&lower), Some(&upper))
            .unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(
        db.find_nodes_range_paged(
            "Missing",
            "score",
            Some(&lower),
            Some(&upper),
            &PropertyRangePageRequest::default(),
        )
        .unwrap()
        .items,
        Vec::<u64>::new()
    );
    assert_eq!(
        db.find_nodes_by_time_range("Missing", i64::MIN, i64::MAX)
            .unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(
        db.find_nodes_by_time_range_paged(
            "Missing",
            i64::MIN,
            i64::MAX,
            &PageRequest::default(),
        )
        .unwrap()
        .items,
        Vec::<u64>::new()
    );
    assert_eq!(db.get_node_label_id("Missing").unwrap(), None);
    assert_eq!(db.list_node_labels().unwrap().len(), catalog_len);

    let mixed_upper = PropertyRangeBound::Included(PropValue::Float(1.0));
    assert_eq!(
        db.find_nodes_range("Missing", "score", Some(&lower), Some(&mixed_upper))
            .unwrap(),
        Vec::<u64>::new()
    );
    let invalid_upper = PropertyRangeBound::Included(PropValue::String("1".to_string()));
    assert!(
        db.find_nodes_range("Missing", "score", Some(&lower), Some(&invalid_upper))
            .is_err()
    );
    assert!(db.find_nodes(" Article", "color", &red).is_err());
}

#[test]
fn test_property_index_apis_use_names_and_persist_metadata() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node_info = db
            .ensure_node_property_index("Article", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(node_info.label, "Article");
        assert_eq!(node_info.fields, property_index_fields("status"));
        assert_eq!(db.get_node_label_id("Article").unwrap(), Some(1));

        let edge_info = db
            .ensure_edge_property_index("MENTIONS", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("rank").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(edge_info.label, "MENTIONS");
        assert_eq!(edge_info.fields, property_index_fields("rank"));
        assert_eq!(db.get_edge_label_id("MENTIONS").unwrap(), Some(1));

        assert_eq!(
            db.list_node_property_indexes()
                .unwrap()
                .iter()
                .map(|info| (info.label.clone(), info.fields.clone()))
                .collect::<Vec<_>>(),
            vec![("Article".to_string(), property_index_fields("status"))]
        );
        assert_eq!(
            db.list_edge_property_indexes()
                .unwrap()
                .iter()
                .map(|info| (info.label.clone(), info.fields.clone()))
                .collect::<Vec<_>>(),
            vec![("MENTIONS".to_string(), property_index_fields("rank"))]
        );
        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(db.get_node_label_id("Article").unwrap(), Some(1));
        assert_eq!(db.get_edge_label_id("MENTIONS").unwrap(), Some(1));
        assert_eq!(
            db.list_node_property_indexes()
                .unwrap()
                .iter()
                .map(|info| (info.label.clone(), info.fields.clone()))
                .collect::<Vec<_>>(),
            vec![("Article".to_string(), property_index_fields("status"))]
        );
        assert_eq!(
            db.list_edge_property_indexes()
                .unwrap()
                .iter()
                .map(|info| (info.label.clone(), info.fields.clone()))
                .collect::<Vec<_>>(),
            vec![("MENTIONS".to_string(), property_index_fields("rank"))]
        );
        db.close().unwrap();
    }
}

#[test]
fn test_field_list_property_index_contract_routes_lists_and_drops() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let single_property_spec =
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("status")]);
        let single = db
            .ensure_node_property_index("Article", single_property_spec.clone())
            .unwrap();
        assert_eq!(single.fields, property_index_fields("status"));
        assert!(!single.compound);

        let node_metadata_spec = SecondaryIndexSpec::equality(vec![SecondaryIndexField::node_meta(
            NodeMetadataIndexField::UpdatedAt,
        )]);
        let node_metadata = db
            .ensure_node_property_index("Article", node_metadata_spec.clone())
            .unwrap();
        assert_eq!(
            node_metadata.fields,
            vec![SecondaryIndexField::node_meta(
                NodeMetadataIndexField::UpdatedAt
            )]
        );
        assert!(!node_metadata.compound);
        assert_eq!(node_metadata.state, SecondaryIndexState::Building);

        let node_compound_spec = SecondaryIndexSpec::equality(vec![
            SecondaryIndexField::property("status"),
            SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
        ]);
        let node_compound = db
            .ensure_node_property_index("Article", node_compound_spec.clone())
            .unwrap();
        assert!(node_compound.compound);
        assert_eq!(node_compound.state, SecondaryIndexState::Building);
        assert_eq!(
            db.ensure_node_property_index("Article", node_compound_spec.clone())
                .unwrap()
                .index_id,
            node_compound.index_id
        );

        let edge_metadata_spec = SecondaryIndexSpec::range(vec![SecondaryIndexField::edge_meta(
            EdgeMetadataIndexField::ValidFrom,
        )]);
        let edge_metadata = db
            .ensure_edge_property_index("MENTIONS", edge_metadata_spec.clone())
            .unwrap();
        assert_eq!(
            edge_metadata.fields,
            vec![SecondaryIndexField::edge_meta(
                EdgeMetadataIndexField::ValidFrom
            )]
        );
        assert_eq!(edge_metadata.state, SecondaryIndexState::Building);

        let disk_manifest = load_manifest_readonly(&db_path).unwrap().unwrap();
        assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
            entry.target
                == SecondaryIndexTarget::NodeProperty {
                    label_id: 1,
                    prop_key: "status".to_string(),
                }
        }));
        assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
            matches!(
                &entry.target,
                SecondaryIndexTarget::NodeFieldIndex { label_id: 1, fields }
                    if fields
                        == &vec![SecondaryIndexFieldManifest::NodeMetadata {
                            field: NodeMetadataIndexFieldManifest::UpdatedAt,
                        }]
            )
        }));
        assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
            matches!(
                &entry.target,
                SecondaryIndexTarget::NodeFieldIndex { label_id: 1, fields }
                    if fields.len() == 2
            )
        }));
        assert!(disk_manifest.secondary_indexes.iter().any(|entry| {
            matches!(
                &entry.target,
                SecondaryIndexTarget::EdgeFieldIndex { label_id: 1, fields }
                    if fields
                        == &vec![SecondaryIndexFieldManifest::EdgeMetadata {
                            field: EdgeMetadataIndexFieldManifest::ValidFrom,
                        }]
            )
        }));

        let node_indexes = db.list_node_property_indexes().unwrap();
        assert!(node_indexes
            .iter()
            .any(|info| info.index_id == single.index_id && !info.compound));
        assert!(node_indexes
            .iter()
            .any(|info| info.index_id == node_metadata.index_id && !info.compound));
        assert!(node_indexes
            .iter()
            .any(|info| info.index_id == node_compound.index_id && info.compound));

        assert!(db
            .drop_node_property_index("Article", node_metadata_spec.clone())
            .unwrap());
        assert!(!db
            .drop_node_property_index("Article", node_metadata_spec)
            .unwrap());
        assert!(db
            .drop_edge_property_index("MENTIONS", edge_metadata_spec.clone())
            .unwrap());
        assert!(!db
            .drop_edge_property_index("MENTIONS", edge_metadata_spec)
            .unwrap());
        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node_indexes = db.list_node_property_indexes().unwrap();
        assert_eq!(node_indexes.len(), 2);
        assert!(node_indexes.iter().any(|info| {
            info.fields == property_index_fields("status")
                && matches!(info.kind, SecondaryIndexKind::Equality)
        }));
        assert!(node_indexes.iter().any(|info| {
            info.fields
                == vec![
                    SecondaryIndexField::property("status"),
                    SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
                ]
                && info.compound
                && matches!(info.state, SecondaryIndexState::Ready)
                && info.last_error.is_none()
        }));
        assert!(db.list_edge_property_indexes().unwrap().is_empty());
        db.close().unwrap();
    }
}

#[test]
fn test_property_index_drop_unknown_label_is_read_only() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    assert!(!db
        .drop_node_property_index("Missing", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap());
    assert!(!db
        .drop_edge_property_index("MISSING", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .unwrap());
    assert_eq!(db.get_node_label_id("Missing").unwrap(), None);
    assert_eq!(db.get_edge_label_id("MISSING").unwrap(), None);
    assert!(db
        .drop_node_property_index(" Missing", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .is_err());
    assert!(db
        .drop_edge_property_index("MISSING\n", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
        .is_err());
}

#[test]
fn test_prune_policy_apis_use_names_and_persist_metadata() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        db.set_prune_policy(
            "article-low-weight",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.25),
                label: Some("Article".to_string()),
            },
        )
        .unwrap();
        assert_eq!(db.get_node_label_id("Article").unwrap(), Some(1));

        let policies = db.list_prune_policies().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "article-low-weight");
        assert_eq!(policies[0].policy.label.as_deref(), Some("Article"));
        assert_eq!(policies[0].policy.max_weight, Some(0.25));
        db.close().unwrap();
    }

    {
        let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        assert_eq!(db.get_node_label_id("Article").unwrap(), Some(1));
        let policies = db.list_prune_policies().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "article-low-weight");
        assert_eq!(policies[0].policy.label.as_deref(), Some("Article"));
        db.close().unwrap();
    }
}

#[test]
fn test_prune_policy_validation_does_not_leak_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let err = db
        .set_prune_policy(
            "invalid-name",
            PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                label: Some(" LeakyPolicy".to_string()),
            },
        )
        .unwrap_err();
    match err {
        EngineError::InvalidOperation(message) => {
            assert!(message.contains("leading or trailing whitespace"));
        }
        other => panic!("expected invalid label error, got {other:?}"),
    }
    assert_eq!(db.get_node_label_id("LeakyPolicy").unwrap(), None);
    assert!(
        db.get_node_label_id(" LeakyPolicy")
            .unwrap_err()
            .to_string()
            .contains("leading or trailing whitespace")
    );

    let err = db
        .set_prune_policy(
            "invalid",
            PrunePolicy {
                max_age_ms: None,
                max_weight: None,
                label: Some("LeakyPolicy".to_string()),
            },
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidOperation(_)));
    assert_eq!(db.get_node_label_id("LeakyPolicy").unwrap(), None);
    assert!(db.list_prune_policies().unwrap().is_empty());
}

#[test]
fn test_open_rejects_prune_policy_with_missing_label_token() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    std::fs::create_dir_all(&db_path).unwrap();
    let mut manifest = default_manifest();
    manifest.prune_policies.insert(
        "broken".to_string(),
        PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.1),
            label: Some("Missing".to_string()),
        },
    );
    write_manifest(&db_path, &manifest).unwrap();

    match DatabaseEngine::open(&db_path, &DbOptions::default()) {
        Ok(_) => panic!("open should reject prune policy with missing label token"),
        Err(EngineError::ManifestError(message)) => {
            assert!(message.contains("prune policy references missing node label"));
        }
        Err(other) => panic!("expected manifest error, got {other:?}"),
    }
}

#[test]
fn test_prune_unknown_label_scope_creates_token_without_deleting() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("catalog_db");
    let db = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let result = db
        .prune(&PrunePolicy {
            max_age_ms: None,
            max_weight: Some(0.1),
            label: Some("MissingButValid".to_string()),
        })
        .unwrap();
    assert_eq!(result.nodes_pruned, 0);
    assert_eq!(result.edges_pruned, 0);
    assert_eq!(db.get_node_label_id("MissingButValid").unwrap(), Some(1));
}
