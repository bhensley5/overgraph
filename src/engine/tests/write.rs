// Write tests: upsert, batch, delete, adjacency verification.

use crate::secondary_index_key::{
    CompoundFieldValue, CompoundTupleContext, compound_prefix_bounds, encode_compound_tuple_prefix,
};

    // --- Upsert API tests ---

    #[test]
    fn test_upsert_node_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let id2 = engine.upsert_node("Person", "bob", UpsertNodeOptions { weight: 0.6, ..Default::default() }).unwrap();

        assert_ne!(id1, id2);
        assert_eq!(engine.node_count().unwrap(), 2);
        assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
        assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props_v1 = BTreeMap::new();
        props_v1.insert("version".to_string(), PropValue::Int(1));
        let id1 = engine.upsert_node("Person", "alice", UpsertNodeOptions { props: props_v1, weight: 0.5, ..Default::default() }).unwrap();

        let mut props_v2 = BTreeMap::new();
        props_v2.insert("version".to_string(), PropValue::Int(2));
        let id2 = engine.upsert_node("Person", "alice", UpsertNodeOptions { props: props_v2, weight: 0.9, ..Default::default() }).unwrap();

        // Same (label_id, key) → same ID, updated fields
        assert_eq!(id1, id2);
        assert_eq!(engine.node_count().unwrap(), 1);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.props.get("version"), Some(&PropValue::Int(2)));
        assert!((node.weight - 0.9).abs() < f32::EPSILON);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_accepts_default_weight() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let node_id = engine.upsert_node("Person", "alice", UpsertNodeOptions::default()).unwrap();

        let node = engine.get_node(node_id).unwrap().unwrap();
        assert!((node.weight - 1.0).abs() < f32::EPSILON);
        engine.close().unwrap();
    }

    fn node_input_with_labels(
        labels: &[&str],
        key: &str,
        props: BTreeMap<String, PropValue>,
    ) -> NodeInput {
        NodeInput {
            labels: labels.iter().map(|label| (*label).to_string()).collect(),
            key: key.to_string(),
            props,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }
    }

    fn score_props(score: i64) -> BTreeMap<String, PropValue> {
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(score));
        props
    }

    #[test]
    fn test_multi_label_upsert_replaces_and_queries_memberships() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine
            .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        engine
            .ensure_node_property_index("Employee", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();

        let id = engine
            .upsert_node(
                &["Person", "Employee"],
                "alice",
                UpsertNodeOptions {
                    props: score_props(7),
                    ..Default::default()
                },
            )
            .unwrap();
        let node = engine.get_node(id).unwrap().unwrap();
        assert_eq!(node.labels, vec!["Person".to_string(), "Employee".to_string()]);
        assert_eq!(
            engine.get_node_by_key("Person", "alice").unwrap().unwrap().id,
            id
        );
        assert_eq!(
            engine.get_node_by_key("Employee", "alice").unwrap().unwrap().id,
            id
        );
        assert_eq!(engine.nodes_by_labels("Person").unwrap(), vec![id]);
        assert_eq!(engine.nodes_by_labels("Employee").unwrap(), vec![id]);
        assert_eq!(engine.count_nodes_by_labels("Person").unwrap(), 1);
        assert_eq!(engine.count_nodes_by_labels("Employee").unwrap(), 1);
        assert_eq!(
            engine
                .get_nodes_by_keys(&[
                    NodeKeyQuery {
                        label: "Employee".to_string(),
                        key: "alice".to_string(),
                    },
                    NodeKeyQuery {
                        label: "Missing".to_string(),
                        key: "alice".to_string(),
                    },
                    NodeKeyQuery {
                        label: "Person".to_string(),
                        key: "alice".to_string(),
                    },
                ])
                .unwrap()
                .into_iter()
                .map(|node| node.map(|node| node.id))
                .collect::<Vec<_>>(),
            vec![Some(id), None, Some(id)]
        );
        assert_eq!(
            engine
                .find_nodes("Person", "score", &PropValue::Int(7))
                .unwrap(),
            vec![id]
        );
        assert_eq!(
            engine
                .find_nodes("Employee", "score", &PropValue::Int(7))
                .unwrap(),
            vec![id]
        );
        assert_eq!(
            engine
                .find_nodes_range(
                    "Employee",
                    "score",
                    Some(&PropertyRangeBound::Included(PropValue::Int(7))),
                    Some(&PropertyRangeBound::Included(PropValue::Int(7))),
                )
                .unwrap(),
            vec![id]
        );

        let updated = engine
            .upsert_node(
                &["Person"],
                "alice",
                UpsertNodeOptions {
                    props: score_props(9),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated, id);
        assert_eq!(engine.get_node(id).unwrap().unwrap().labels, vec!["Person".to_string()]);
        assert!(engine.get_node_by_key("Employee", "alice").unwrap().is_none());
        assert!(engine.nodes_by_labels("Employee").unwrap().is_empty());
        assert_eq!(engine.count_nodes_by_labels("Employee").unwrap(), 0);
        assert!(engine
            .find_nodes("Employee", "score", &PropValue::Int(7))
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .find_nodes("Person", "score", &PropValue::Int(9))
                .unwrap(),
            vec![id]
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_flexible_label_inputs_and_batch_label_vectors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let owned_label = String::from("Person");
        let id = engine
            .upsert_node(owned_label.clone(), "alice", UpsertNodeOptions::default())
            .unwrap();
        assert_eq!(
            engine
                .upsert_node(&owned_label, "alice", UpsertNodeOptions::default())
                .unwrap(),
            id
        );

        let labels = vec!["Person".to_string(), "Employee".to_string()];
        assert_eq!(
            engine
                .upsert_node(labels, "alice", UpsertNodeOptions::default())
                .unwrap(),
            id
        );
        assert_eq!(
            engine.get_node(id).unwrap().unwrap().labels,
            vec!["Person".to_string(), "Employee".to_string()]
        );

        let array_labels = ["Person".to_string(), "Reviewer".to_string()];
        let bob = engine
            .upsert_node(&array_labels, "bob", UpsertNodeOptions::default())
            .unwrap();
        assert_eq!(
            engine.get_node(bob).unwrap().unwrap().labels,
            vec!["Person".to_string(), "Reviewer".to_string()]
        );

        let batch_ids = engine
            .batch_upsert_nodes(vec![
                NodeInput {
                    labels: vec!["Person".to_string()],
                    key: "carol".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Person".to_string(), "Employee".to_string()],
                    key: "dana".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
                NodeInput {
                    labels: vec!["Person".to_string(), "Contractor".to_string()],
                    key: "erin".to_string(),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: None,
                    sparse_vector: None,
                },
            ])
            .unwrap();
        assert_eq!(
            engine.get_node(batch_ids[0]).unwrap().unwrap().labels,
            vec!["Person".to_string()]
        );
        assert_eq!(
            engine.get_node(batch_ids[1]).unwrap().unwrap().labels,
            vec!["Person".to_string(), "Employee".to_string()]
        );
        assert_eq!(
            engine.get_node(batch_ids[2]).unwrap().unwrap().labels,
            vec!["Person".to_string(), "Contractor".to_string()]
        );

        let mut txn = engine.begin_write_txn().unwrap();
        txn.upsert_node_as(
            "frank",
            vec!["Person".to_string(), "Employee".to_string()],
            "frank",
            UpsertNodeOptions::default(),
        )
        .unwrap();
        txn.commit().unwrap();

        let frank = engine.get_node_by_key("Employee", "frank").unwrap().unwrap();
        assert_eq!(
            frank.labels,
            vec!["Person".to_string(), "Employee".to_string()]
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_multi_label_conflicts_and_token_staging_are_atomic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let left = engine
            .upsert_node("ConflictA", "shared", UpsertNodeOptions::default())
            .unwrap();
        let right = engine
            .upsert_node("ConflictB", "shared", UpsertNodeOptions::default())
            .unwrap();

        let err = engine
            .upsert_node(
                &["ConflictA", "ConflictB", "ConflictNew"],
                "shared",
                UpsertNodeOptions::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("node key conflict"));
        assert_eq!(engine.get_node_label_id("ConflictNew").unwrap(), None);
        assert_eq!(
            engine.get_node_by_key("ConflictA", "shared").unwrap().unwrap().id,
            left
        );
        assert_eq!(
            engine.get_node_by_key("ConflictB", "shared").unwrap().unwrap().id,
            right
        );

        let duplicate = engine
            .upsert_node(
                &["DuplicateLabel", "DuplicateLabel"],
                "dup",
                UpsertNodeOptions::default(),
            )
            .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate label"));
        assert_eq!(engine.get_node_label_id("DuplicateLabel").unwrap(), None);

        let batch_err = engine
            .batch_upsert_nodes(vec![
                node_input_with_labels(&["BatchA"], "shared", BTreeMap::new()),
                node_input_with_labels(&["BatchB"], "shared", BTreeMap::new()),
                node_input_with_labels(&["BatchA", "BatchB", "BatchNew"], "shared", BTreeMap::new()),
            ])
            .unwrap_err();
        assert!(batch_err.to_string().contains("node key conflict"));
        assert_eq!(engine.get_node_label_id("BatchA").unwrap(), None);
        assert_eq!(engine.get_node_label_id("BatchB").unwrap(), None);
        assert_eq!(engine.get_node_label_id("BatchNew").unwrap(), None);
        assert_eq!(engine.node_count().unwrap(), 2);

        let batch_ids = engine
            .batch_upsert_nodes(vec![
                node_input_with_labels(&["BatchOkA", "BatchOkB"], "ok", BTreeMap::new()),
                node_input_with_labels(&["BatchOkB"], "other", BTreeMap::new()),
            ])
            .unwrap();
        assert_eq!(batch_ids.len(), 2);
        assert_eq!(
            engine.get_node_by_key("BatchOkA", "ok").unwrap().unwrap().id,
            batch_ids[0]
        );
        assert_eq!(
            engine.get_node_by_key("BatchOkB", "ok").unwrap().unwrap().id,
            batch_ids[0]
        );
        assert_eq!(
            engine
                .get_node_by_key("BatchOkB", "other")
                .unwrap()
                .unwrap()
                .id,
            batch_ids[1]
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_graph_patch_multi_label_conflict_rolls_back_tokens_and_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .upsert_node("PatchA", "shared", UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_node("PatchB", "shared", UpsertNodeOptions::default())
            .unwrap();
        let err = engine
            .graph_patch(GraphPatch {
                upsert_nodes: vec![node_input_with_labels(
                    &["PatchA", "PatchB", "PatchNew"],
                    "shared",
                    BTreeMap::new(),
                )],
                ..Default::default()
            })
            .unwrap_err();
        assert!(err.to_string().contains("node key conflict"));
        assert_eq!(engine.get_node_label_id("PatchNew").unwrap(), None);
        assert_eq!(engine.node_count().unwrap(), 2);
        assert!(engine.get_node_by_key("PatchNew", "shared").unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_stale_label_memberships_are_suppressed_after_flush_reopen_and_compact() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };

        let id;
        {
            let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            id = engine
                .upsert_node(&["FreshA", "FreshB"], "k", UpsertNodeOptions::default())
                .unwrap();
            engine.flush().unwrap();

            assert_eq!(
                engine
                    .upsert_node(&["FreshA"], "k", UpsertNodeOptions::default())
                    .unwrap(),
                id
            );
            assert!(engine.get_node_by_key("FreshB", "k").unwrap().is_none());
            assert!(engine.nodes_by_labels("FreshB").unwrap().is_empty());
            assert_eq!(engine.count_nodes_by_labels("FreshB").unwrap(), 0);

            engine.flush().unwrap();
            assert!(engine.get_node_by_key("FreshB", "k").unwrap().is_none());
            assert!(engine.nodes_by_labels("FreshB").unwrap().is_empty());

            let mut kept_ids = Vec::new();
            for index in 0..8 {
                let node_id = engine
                    .upsert_node(
                        &["FreshPageA", "FreshPageB"],
                        &format!("page-{index}"),
                        UpsertNodeOptions::default(),
                    )
                    .unwrap();
                if index >= 5 {
                    kept_ids.push(node_id);
                }
            }
            engine.flush().unwrap();
            for index in 0..5 {
                engine
                    .upsert_node(
                        &["FreshPageA"],
                        &format!("page-{index}"),
                        UpsertNodeOptions::default(),
                    )
                    .unwrap();
            }

            let label_page = engine
                .nodes_by_labels_paged(
                    "FreshPageB",
                    &PageRequest {
                        limit: Some(2),
                        after: None,
                    },
                )
                .unwrap();
            assert_eq!(label_page.items, kept_ids[..2].to_vec());
            let label_page_2 = engine
                .nodes_by_labels_paged(
                    "FreshPageB",
                    &PageRequest {
                        limit: Some(2),
                        after: label_page.next_cursor,
                    },
                )
                .unwrap();
            assert_eq!(label_page_2.items, kept_ids[2..].to_vec());
            assert!(label_page_2.next_cursor.is_none());

            let time_page = engine
                .find_nodes_by_time_range_paged(
                    "FreshPageB",
                    i64::MIN,
                    i64::MAX,
                    &PageRequest {
                        limit: Some(2),
                        after: None,
                    },
                )
                .unwrap();
            assert_eq!(time_page.items, kept_ids[..2].to_vec());
            let time_page_2 = engine
                .find_nodes_by_time_range_paged(
                    "FreshPageB",
                    i64::MIN,
                    i64::MAX,
                    &PageRequest {
                        limit: Some(2),
                        after: time_page.next_cursor,
                    },
                )
                .unwrap();
            assert_eq!(time_page_2.items, kept_ids[2..].to_vec());
            assert!(time_page_2.next_cursor.is_none());

            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            assert_eq!(
                engine.get_node_by_key("FreshA", "k").unwrap().unwrap().id,
                id
            );
            assert!(engine.get_node_by_key("FreshB", "k").unwrap().is_none());
            assert!(engine.nodes_by_labels("FreshB").unwrap().is_empty());
            engine.compact().unwrap();
            assert_eq!(
                engine.get_node_by_key("FreshA", "k").unwrap().unwrap().id,
                id
            );
            assert!(engine.get_node_by_key("FreshB", "k").unwrap().is_none());
            assert!(engine.nodes_by_labels("FreshB").unwrap().is_empty());
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_stale_label_memberships_are_suppressed_from_immutable_memtable() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .ensure_node_property_index("ImmFreshB", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine
            .ensure_node_property_index("ImmFreshB", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        let id = engine
            .upsert_node(
                &["ImmFreshA", "ImmFreshB"],
                "k",
                UpsertNodeOptions {
                    props: score_props(42),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        assert_eq!(
            engine
                .upsert_node(
                    &["ImmFreshA"],
                    "k",
                    UpsertNodeOptions {
                        props: score_props(43),
                        ..Default::default()
                    },
                )
                .unwrap(),
            id
        );

        assert_eq!(
            engine.get_node_by_key("ImmFreshA", "k").unwrap().unwrap().id,
            id
        );
        assert!(engine.get_node_by_key("ImmFreshB", "k").unwrap().is_none());
        assert_eq!(
            engine
                .get_nodes_by_keys(&[
                    NodeKeyQuery {
                        label: "ImmFreshB".to_string(),
                        key: "k".to_string(),
                    },
                    NodeKeyQuery {
                        label: "ImmFreshA".to_string(),
                        key: "k".to_string(),
                    },
                ])
                .unwrap()
                .into_iter()
                .map(|node| node.map(|node| node.id))
                .collect::<Vec<_>>(),
            vec![None, Some(id)]
        );
        assert!(engine.nodes_by_labels("ImmFreshB").unwrap().is_empty());
        assert_eq!(engine.count_nodes_by_labels("ImmFreshB").unwrap(), 0);
        assert!(engine
            .find_nodes("ImmFreshB", "score", &PropValue::Int(42))
            .unwrap()
            .is_empty());
        assert!(engine
            .find_nodes_range(
                "ImmFreshB",
                "score",
                Some(&PropertyRangeBound::Included(PropValue::Int(42))),
                Some(&PropertyRangeBound::Included(PropValue::Int(42))),
            )
            .unwrap()
            .is_empty());
        assert!(engine
            .find_nodes_by_time_range("ImmFreshB", i64::MIN, i64::MAX)
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_add_remove_node_label_semantics() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id = engine
            .upsert_node("PatchPerson", "alice", UpsertNodeOptions::default())
            .unwrap();
        let initial_updated_at = engine.get_node(id).unwrap().unwrap().updated_at;
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(engine.add_node_label(id, "PatchEmployee").unwrap());
        let after_add = engine.get_node(id).unwrap().unwrap();
        assert!(after_add.updated_at > initial_updated_at);
        assert!(engine
            .find_nodes_by_time_range("PatchPerson", initial_updated_at, initial_updated_at)
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .find_nodes_by_time_range("PatchPerson", after_add.updated_at, after_add.updated_at)
                .unwrap(),
            vec![id]
        );
        assert_eq!(
            engine
                .find_nodes_by_time_range(
                    "PatchEmployee",
                    after_add.updated_at,
                    after_add.updated_at,
                )
                .unwrap(),
            vec![id]
        );
        assert!(!engine.add_node_label(id, "PatchEmployee").unwrap());
        assert_eq!(
            engine
                .get_node_by_key("PatchEmployee", "alice")
                .unwrap()
                .unwrap()
                .id,
            id
        );
        assert_eq!(
            engine.get_node(id).unwrap().unwrap().labels,
            vec!["PatchPerson".to_string(), "PatchEmployee".to_string()]
        );

        engine
            .upsert_node("PatchContractor", "alice", UpsertNodeOptions::default())
            .unwrap();
        let err = engine.add_node_label(id, "PatchContractor").unwrap_err();
        assert!(err.to_string().contains("node key conflict"));

        assert!(!engine.remove_node_label(id, "MissingButValid").unwrap());
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(engine.remove_node_label(id, "PatchEmployee").unwrap());
        let after_remove = engine.get_node(id).unwrap().unwrap();
        assert!(after_remove.updated_at > after_add.updated_at);
        assert!(engine
            .find_nodes_by_time_range("PatchPerson", after_add.updated_at, after_add.updated_at)
            .unwrap()
            .is_empty());
        assert_eq!(
            engine
                .find_nodes_by_time_range(
                    "PatchPerson",
                    after_remove.updated_at,
                    after_remove.updated_at,
                )
                .unwrap(),
            vec![id]
        );
        assert!(engine
            .find_nodes_by_time_range("PatchEmployee", after_add.updated_at, after_add.updated_at)
            .unwrap()
            .is_empty());
        assert!(engine
            .get_node_by_key("PatchEmployee", "alice")
            .unwrap()
            .is_none());
        assert!(!engine.remove_node_label(id, "PatchEmployee").unwrap());
        let err = engine.remove_node_label(id, "PatchPerson").unwrap_err();
        assert!(err.to_string().contains("last node label"));

        let full_labels = [
            "Full0", "Full1", "Full2", "Full3", "Full4", "Full5", "Full6", "Full7", "Full8",
            "Full9",
        ];
        let full_id = engine
            .upsert_node(&full_labels[..], "full", UpsertNodeOptions::default())
            .unwrap();
        let label_count = engine.list_node_labels().unwrap().len();
        let err = engine.add_node_label(full_id, "FullNew").unwrap_err();
        assert!(err.to_string().contains("at most 10 labels"));
        assert_eq!(engine.get_node_label_id("FullNew").unwrap(), None);
        assert_eq!(engine.list_node_labels().unwrap().len(), label_count);

        engine.close().unwrap();
    }

    #[test]
    fn test_write_txn_multi_label_upsert_and_patch_semantics() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut txn = engine.begin_write_txn().unwrap();
        let alice = txn
            .upsert_node(&["TxnPerson", "TxnEmployee"], "alice", UpsertNodeOptions::default())
            .unwrap();
        let staged = txn.get_node(alice.clone()).unwrap().unwrap();
        assert_eq!(
            staged.labels,
            vec!["TxnPerson".to_string(), "TxnEmployee".to_string()]
        );
        assert_eq!(
            txn.get_node_by_key("TxnEmployee", "alice")
                .unwrap()
                .unwrap()
                .local,
            match alice.clone() {
                TxnNodeRef::Local(local) => Some(local),
                _ => None,
            }
        );
        assert!(txn.add_node_label(alice.clone(), "TxnManager").unwrap());
        assert!(!txn.add_node_label(alice.clone(), "TxnManager").unwrap());
        assert!(txn.remove_node_label(alice.clone(), "TxnEmployee").unwrap());
        assert!(txn
            .get_node_by_key("TxnEmployee", "alice")
            .unwrap()
            .is_none());
        let committed = txn.commit().unwrap();
        let id = committed.node_ids[0];
        assert_eq!(
            engine.get_node(id).unwrap().unwrap().labels,
            vec!["TxnPerson".to_string(), "TxnManager".to_string()]
        );
        assert_eq!(
            engine.get_node_by_key("TxnManager", "alice").unwrap().unwrap().id,
            id
        );
        assert!(engine
            .get_node_by_key("TxnEmployee", "alice")
            .unwrap()
            .is_none());

        let mut conflict_txn = engine.begin_write_txn().unwrap();
        conflict_txn
            .upsert_node("TxnConflictA", "same", UpsertNodeOptions::default())
            .unwrap();
        conflict_txn
            .upsert_node("TxnConflictB", "same", UpsertNodeOptions::default())
            .unwrap();
        assert!(matches!(
            conflict_txn.upsert_node(
                &["TxnConflictA", "TxnConflictB"],
                "same",
                UpsertNodeOptions::default(),
            ),
            Err(EngineError::InvalidOperation(_))
        ));
        conflict_txn.rollback().unwrap();
        assert_eq!(engine.get_node_label_id("TxnConflictA").unwrap(), None);
        assert_eq!(engine.get_node_label_id("TxnConflictB").unwrap(), None);

        let mut invalid_key_txn = engine.begin_write_txn().unwrap();
        let too_long_key = "k".repeat(u16::MAX as usize + 1);
        let err = invalid_key_txn
            .upsert_node("TxnInvalidKey", &too_long_key, UpsertNodeOptions::default())
            .unwrap_err();
        assert!(err.to_string().contains("node key must be at most"));
        assert_eq!(engine.get_node_label_id("TxnInvalidKey").unwrap(), None);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_with_vectors_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };

        let node_id;
        {
            let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            node_id = engine
                .upsert_node(
                    "Person",
                    "alice",
                    UpsertNodeOptions {
                        weight: 0.5,
                        dense_vector: Some(vec![0.1, 0.2, 0.3]),
                        sparse_vector: Some(vec![(9, 0.0), (4, 1.0), (2, 2.0), (4, 0.5), (2, 0.0)]),
                        ..Default::default()
                    },
                )
                .unwrap();

            let node = engine.get_node(node_id).unwrap().unwrap();
            assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
            assert_eq!(node.sparse_vector, Some(vec![(2, 2.0), (4, 1.5)]));
            engine.close().unwrap();
        }

        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let node = engine.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(node.sparse_vector, Some(vec![(2, 2.0), (4, 1.5)]));
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_dense_vector_requires_config() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let err = engine
            .upsert_node(
                "Person",
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    dense_vector: Some(vec![0.1, 0.2, 0.3]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_rejects_wrong_dense_dimension() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 2,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let err = engine
            .upsert_node(
                "Person",
                "alice",
                UpsertNodeOptions {
                    weight: 0.5,
                    dense_vector: Some(vec![0.1, 0.2, 0.3]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        engine.close().unwrap();
    }

    #[test]
    fn test_write_op_normalizes_node_vectors() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 2,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let label_id = engine.ensure_node_label("ManualVectorNode").unwrap();

        write_internal_wal_op(&engine, &WalOp::UpsertNode(NodeRecord {
                id: 1,
                label_ids: NodeLabelSet::single(label_id).unwrap(),
                key: "manual".to_string(),
                props: BTreeMap::new(),
                created_at: 100,
                updated_at: 101,
                weight: 0.5,
                dense_vector: Some(vec![0.1, 0.2]),
                sparse_vector: Some(vec![(5, 0.0), (3, 1.0), (3, 2.0)]),
                last_write_seq: 0,
            }))
            .unwrap();

        let node = engine.get_node(1).unwrap().unwrap();
        assert_eq!(node.dense_vector, Some(vec![0.1, 0.2]));
        assert_eq!(node.sparse_vector, Some(vec![(3, 3.0)]));
        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes_with_vectors_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 3,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            ..DbOptions::default()
        };

        let alice_id;
        let bob_id;
        {
            let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
            let ids = engine
                .batch_upsert_nodes(vec![
                    NodeInput {
                        labels: vec!["Person".to_string()],
                        key: "alice".to_string(),
                        props: BTreeMap::new(),
                        weight: 0.5,
                        dense_vector: Some(vec![0.1, 0.2, 0.3]),
                        sparse_vector: Some(vec![
                            (9, 0.0),
                            (4, 1.0),
                            (2, 2.0),
                            (4, 0.25),
                        ]),
                    },
                    NodeInput {
                        labels: vec!["Person".to_string()],
                        key: "bob".to_string(),
                        props: BTreeMap::new(),
                        weight: 0.7,
                        dense_vector: None,
                        sparse_vector: None,
                    },
                ])
                .unwrap();

            alice_id = ids[0];
            bob_id = ids[1];

            let alice = engine.get_node(alice_id).unwrap().unwrap();
            assert_eq!(alice.dense_vector, Some(vec![0.1, 0.2, 0.3]));
            assert_eq!(alice.sparse_vector, Some(vec![(2, 2.0), (4, 1.25)]));

            let bob = engine.get_node(bob_id).unwrap().unwrap();
            assert!(bob.dense_vector.is_none());
            assert!(bob.sparse_vector.is_none());
            engine.close().unwrap();
        }

        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();
        let alice = engine.get_node(alice_id).unwrap().unwrap();
        assert_eq!(alice.dense_vector, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(alice.sparse_vector, Some(vec![(2, 2.0), (4, 1.25)]));

        let bob = engine.get_node(bob_id).unwrap().unwrap();
        assert!(bob.dense_vector.is_none());
        assert!(bob.sparse_vector.is_none());
        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_different_labels_same_key() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let id2 = engine
            .upsert_node("Company", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();

        // Different label-key memberships produce different nodes.
        assert_ne!(id1, id2);
        assert_eq!(engine.node_count().unwrap(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_node_id_counter_monotonic() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(
                engine
                    .upsert_node("Person", &format!("node:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                    .unwrap(),
            );
        }

        // All IDs should be unique and monotonically increasing
        for i in 1..ids.len() {
            assert!(ids[i] > ids[i - 1]);
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_new() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let n1 = engine
            .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let n2 = engine.upsert_node("Person", "bob", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        let e1 = engine
            .upsert_edge(n1, n2, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();

        assert_eq!(engine.edge_count().unwrap(), 1);
        let edge = engine.get_edge(e1).unwrap().unwrap();
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_without_uniqueness_creates_duplicates() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        // Default: edge_uniqueness = false
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let e1 = engine
            .upsert_edge(1, 2, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();

        // Without uniqueness: creates separate edges
        assert_ne!(e1, e2);
        assert_eq!(engine.edge_count().unwrap(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_edge_with_uniqueness_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let e1 = engine
            .upsert_edge(1, 2, "KNOWS", UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let e2 = engine
            .upsert_edge(1, 2, "KNOWS", UpsertEdgeOptions { weight: 0.9, ..Default::default() })
            .unwrap();

        // With uniqueness: same triple → same ID, updated weight
        assert_eq!(e1, e2);
        assert_eq!(engine.edge_count().unwrap(), 1);
        assert!((engine.get_edge(e1).unwrap().unwrap().weight - 0.9).abs() < f32::EPSILON);

        // Different triple → new edge
        let e3 = engine
            .upsert_edge(1, 2, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap();
        assert_ne!(e1, e3);
        assert_eq!(engine.edge_count().unwrap(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                labels: vec!["Person".to_string()],
                key: format!("node:{}", i),
                props: BTreeMap::new(),
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();

        let ids = engine.batch_upsert_nodes(inputs).unwrap();
        assert_eq!(ids.len(), 1000);
        assert_eq!(engine.node_count().unwrap(), 1000);

        // All queryable
        for (i, &id) in ids.iter().enumerate() {
            let node = engine.get_node(id).unwrap().unwrap();
            assert_eq!(node.key, format!("node:{}", i));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_nodes_with_dedup() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        // Pre-insert a node
        let pre_id = engine
            .upsert_node("Person", "existing", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();

        // Batch with duplicate key and one that matches pre-existing
        let inputs = vec![
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "existing".into(),
                props: BTreeMap::new(),
                weight: 0.9,
                dense_vector: None,
                sparse_vector: None,
            },
            NodeInput {
                labels: vec!["Person".to_string()],
                key: "new1".into(),
                props: BTreeMap::new(),
                weight: 0.8,
                dense_vector: None,
                sparse_vector: None,
            }, // dup within batch
        ];

        let ids = engine.batch_upsert_nodes(inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // "existing" reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // "new1" appears twice → same ID
        assert_eq!(engine.node_count().unwrap(), 2); // "existing" + "new1"

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let inputs: Vec<EdgeInput> = (0..100)
            .map(|i| EdgeInput {
                from: i,
                to: i + 1,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();

        let ids = engine.batch_upsert_edges(inputs).unwrap();
        assert_eq!(ids.len(), 100);
        assert_eq!(engine.edge_count().unwrap(), 100);

        engine.close().unwrap();
    }

    #[test]
    fn test_upsert_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (id1, id2, eid);
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            id1 = engine
                .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            id2 = engine.upsert_node("Person", "bob", UpsertNodeOptions { weight: 0.6, ..Default::default() }).unwrap();
            eid = engine
                .upsert_edge(id1, id2, "KNOWS", UpsertEdgeOptions::default())
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // close() flushes to segments; verify via cross-source lookup
            assert_eq!(engine.get_nodes_by_labels("Person").unwrap().len(), 2);
            assert_eq!(engine.get_node(id1).unwrap().unwrap().key, "alice");
            assert_eq!(engine.get_node(id2).unwrap().unwrap().key, "bob");
            assert_eq!(engine.get_edge(eid).unwrap().unwrap().from, id1);

            // Upsert dedup should still work after close-flush + reopen
            let id1_again = engine
                .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.99, ..Default::default() })
                .unwrap();
            assert_eq!(id1_again, id1);

            // New allocations should not reuse old IDs
            let id3 = engine
                .upsert_node("Person", "charlie", UpsertNodeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            assert!(id3 > id2);

            engine.close().unwrap();
        }
    }

    #[test]
    fn test_upsert_node_preserves_created_at() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let id1 = engine
            .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.5, ..Default::default() })
            .unwrap();
        let created_at_v1 = engine.get_node(id1).unwrap().unwrap().created_at;

        // Small delay not needed, just upsert again. created_at must be preserved
        let id2 = engine
            .upsert_node("Person", "alice", UpsertNodeOptions { weight: 0.9, ..Default::default() })
            .unwrap();
        assert_eq!(id1, id2);

        let node = engine.get_node(id1).unwrap().unwrap();
        assert_eq!(node.created_at, created_at_v1);
        assert!(node.updated_at >= created_at_v1);

        engine.close().unwrap();
    }

    #[test]
    fn test_batch_upsert_edges_with_uniqueness() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let opts = DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        // Pre-insert an edge
        let pre_id = engine
            .upsert_edge(1, 2, "KNOWS", UpsertEdgeOptions { weight: 0.5, ..Default::default() })
            .unwrap();

        // Batch with: duplicate within batch + match against pre-existing
        let inputs = vec![
            EdgeInput {
                from: 3,
                to: 4,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 0.5,
                valid_from: None,
                valid_to: None,
            },
            EdgeInput {
                from: 1,
                to: 2,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 0.9,
                valid_from: None,
                valid_to: None,
            }, // matches pre-existing
            EdgeInput {
                from: 3,
                to: 4,
                label: "KNOWS".to_string(),
                props: BTreeMap::new(),
                weight: 0.8,
                valid_from: None,
                valid_to: None,
            }, // dup within batch
        ];

        let ids = engine.batch_upsert_edges(inputs).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[1], pre_id); // reuses pre-existing ID
        assert_eq!(ids[0], ids[2]); // within-batch dedup
        assert_eq!(engine.edge_count().unwrap(), 2); // pre-existing + one new

        engine.close().unwrap();
    }

    #[test]
    fn test_id_counters_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let last_node_id;
        let last_edge_id;
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            for i in 0..10 {
                engine
                    .upsert_node("Person", &format!("n:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                    .unwrap();
            }
            for i in 0..5 {
                engine
                    .upsert_edge(i, i + 1, "KNOWS", UpsertEdgeOptions::default())
                    .unwrap();
            }
            last_node_id = engine.next_node_id().unwrap();
            last_edge_id = engine.next_edge_id().unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.next_node_id().unwrap() >= last_node_id);
            assert!(engine.next_edge_id().unwrap() >= last_edge_id);
            engine.close().unwrap();
        }
    }

    // --- Adjacency, neighbors, delete tests ---

    #[test]
    fn test_neighbors_outgoing() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node("Person", "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(a, c, "REPORTS_TO", UpsertEdgeOptions { weight: 0.8, ..Default::default() })
            .unwrap();

        let out = engine
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert_eq!(out.len(), 2);
        let neighbor_ids: Vec<u64> = out.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_incoming() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node("Person", "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, c, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();
        engine
            .upsert_edge(b, c, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();

        let inc = engine
            .neighbors(c, &NeighborOptions { direction: Direction::Incoming, ..Default::default() })
            .unwrap();
        assert_eq!(inc.len(), 2);
        let neighbor_ids: Vec<u64> = inc.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&a));
        assert!(neighbor_ids.contains(&b));

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_label_filter() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let c = engine.upsert_node("Person", "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();

        engine
            .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
            .unwrap(); // KNOWS
        engine
            .upsert_edge(a, c, "REPORTS_TO", UpsertEdgeOptions::default())
            .unwrap(); // REPORTS_TO

        let labeled = engine
            .neighbors(a, &NeighborOptions { edge_label_filter: Some(vec!["KNOWS".to_string()]), ..Default::default() })
            .unwrap();
        assert_eq!(labeled.len(), 1);
        assert_eq!(labeled[0].node_id, b);

        engine.close().unwrap();
    }

    #[test]
    fn test_neighbors_with_limit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let hub = engine.upsert_node("Person", "hub", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        for i in 0..10 {
            let n = engine
                .upsert_node("Person", &format!("spoke:{}", i), UpsertNodeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            engine
                .upsert_edge(hub, n, "KNOWS", UpsertEdgeOptions::default())
                .unwrap();
        }

        let limited = engine
            .neighbors(hub, &NeighborOptions { limit: Some(3), ..Default::default() })
            .unwrap();
        assert_eq!(limited.len(), 3);

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_node_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        engine
            .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();

        engine.delete_node(b).unwrap();

        assert!(engine.get_node(b).unwrap().is_none());
        assert_eq!(engine.node_count().unwrap(), 1);

        // b excluded from a's neighbors (node tombstone filtering)
        let out = engine
            .neighbors(a, &NeighborOptions::default())
            .unwrap();
        assert!(out.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_edge_via_api() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
        let eid = engine
            .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
            .unwrap();

        engine.delete_edge(eid).unwrap();

        assert!(engine.get_edge(eid).unwrap().is_none());
        assert_eq!(engine.edge_count().unwrap(), 0);
        assert!(engine
            .neighbors(a, &NeighborOptions::default())
            .unwrap()
            .is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn test_delete_survives_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, eid);
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            eid = engine
                .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
                .unwrap();
            engine.delete_node(b).unwrap();
            engine.delete_edge(eid).unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            assert!(engine.get_node(b).unwrap().is_none());
            assert!(engine.get_edge(eid).unwrap().is_none());
            // close() flushes to segments; use cross-source counts
            assert_eq!(engine.get_nodes_by_labels("Person").unwrap().len(), 1);
            // Verify deleted edge not visible
            assert!(engine
                .neighbors(a, &NeighborOptions::default())
                .unwrap()
                .is_empty());
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_neighbors_survive_restart() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (a, b, c);
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            a = engine.upsert_node("Person", "a", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            b = engine.upsert_node("Person", "b", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            c = engine.upsert_node("Person", "c", UpsertNodeOptions { weight: 0.5, ..Default::default() }).unwrap();
            engine
                .upsert_edge(a, b, "KNOWS", UpsertEdgeOptions::default())
                .unwrap();
            engine
                .upsert_edge(a, c, "REPORTS_TO", UpsertEdgeOptions { weight: 0.8, ..Default::default() })
                .unwrap();
            engine
                .upsert_edge(b, c, "KNOWS", UpsertEdgeOptions { weight: 0.5, ..Default::default() })
                .unwrap();
            engine.close().unwrap();
        }

        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            // a → b, c
            let out_a = engine
                .neighbors(a, &NeighborOptions::default())
                .unwrap();
            assert_eq!(out_a.len(), 2);
            // b → c
            let out_b = engine
                .neighbors(b, &NeighborOptions::default())
                .unwrap();
            assert_eq!(out_b.len(), 1);
            assert_eq!(out_b[0].node_id, c);
            // c ← a, b
            let inc_c = engine
                .neighbors(c, &NeighborOptions { direction: Direction::Incoming, ..Default::default() })
                .unwrap();
            assert_eq!(inc_c.len(), 2);
            engine.close().unwrap();
        }
    }

    #[test]
    fn test_node_property_index_ensure_drop_list_and_reuses_range_declaration() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let eq = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(eq.state, SecondaryIndexState::Building);

        let eq_again = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(eq_again.index_id, eq.index_id);

        let range = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(range.state, SecondaryIndexState::Building);

        let indexes = engine.list_node_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].fields, property_index_fields("color"));
        assert_eq!(indexes[1].fields, property_index_fields("score"));

        let range_again = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(range_again.index_id, range.index_id);

        assert!(engine
            .drop_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap());
        assert!(!engine
            .drop_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap());

        let indexes = engine.list_node_property_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_id, range.index_id);

        engine.close().unwrap();
    }

    #[test]
    fn test_node_property_index_retry_failed_clears_error_and_preserves_id() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let created = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine.shutdown_secondary_index_worker();

        engine
            .with_runtime_manifest_write(|manifest| {
                let entry = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == created.index_id)
                    .unwrap();
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some("boom".to_string());
                Ok(())
            })
            .unwrap();
        engine.rebuild_secondary_index_catalog().unwrap();

        let retried = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(retried.index_id, created.index_id);
        assert_eq!(retried.state, SecondaryIndexState::Building);
        assert!(retried.last_error.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_ensure_node_property_index_seeds_active_and_immutable_memtables() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut frozen_props = BTreeMap::new();
        frozen_props.insert("status".to_string(), PropValue::String("active".to_string()));
        frozen_props.insert("age".to_string(), PropValue::Int(30));
        let frozen_id = engine
            .upsert_node(
                "Person",
                "frozen",
                UpsertNodeOptions {
                    props: frozen_props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let mut active_props = BTreeMap::new();
        active_props.insert("status".to_string(), PropValue::String("active".to_string()));
        active_props.insert("age".to_string(), PropValue::Int(35));
        let active_id = engine
            .upsert_node(
                "Person",
                "active",
                UpsertNodeOptions {
                    props: active_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let mut bad_props = BTreeMap::new();
        bad_props.insert("status".to_string(), PropValue::String("active".to_string()));
        bad_props.insert("age".to_string(), PropValue::String("old".to_string()));
        let bad_id = engine
            .upsert_node(
                "Person",
                "bad",
                UpsertNodeOptions {
                    props: bad_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let eq = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let range = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("age").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();

        let status_hash = hash_prop_equality_key(&PropValue::String("active".to_string()));
        let active_memtable = engine.active_memtable();
        let active_eq_state = active_memtable.secondary_eq_state();
        let active_eq_ids = active_eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(active_eq_ids.contains(&active_id));
        assert!(active_eq_ids.contains(&bad_id));

        let frozen_memtable = engine.immutable_memtable(0);
        let frozen_eq_state = frozen_memtable.secondary_eq_state();
        let frozen_eq_ids = frozen_eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(frozen_eq_ids.contains(&frozen_id));

        let active_memtable = engine.active_memtable();
        let active_range_state = active_memtable.secondary_range_state();
        let active_range = active_range_state
            .get(&range.index_id)
            .unwrap();
        let age_35 = numeric_range_sort_key_for_value(&PropValue::Int(35)).unwrap();
        assert!(active_range.contains(&(age_35, active_id)));
        assert!(!active_range.iter().any(|&(_, node_id)| node_id == bad_id));

        let frozen_memtable = engine.immutable_memtable(0);
        let frozen_range_state = frozen_memtable.secondary_range_state();
        let frozen_range = frozen_range_state
            .get(&range.index_id)
            .unwrap();
        let age_30 = numeric_range_sort_key_for_value(&PropValue::Int(30)).unwrap();
        assert!(frozen_range.contains(&(age_30, frozen_id)));

        engine.close().unwrap();
    }

    #[test]
    fn test_secondary_index_seeding_refreshes_immutable_memtable_bytes_cache() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("status".to_string(), PropValue::String("active".to_string()));
        props.insert("age".to_string(), PropValue::Int(30));
        engine
            .upsert_node(
                "Person",
                "frozen",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let before = engine.stats().unwrap().immutable_memtable_bytes;
        let info = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let after = engine.stats().unwrap().immutable_memtable_bytes;
        let actual_after: usize = (0..engine.immutable_epoch_count())
            .map(|idx| engine.immutable_memtable(idx).estimated_size())
            .sum();
        assert_eq!(after, actual_after);
        assert!(after >= before);

        engine
            .drop_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let after_drop = engine.stats().unwrap().immutable_memtable_bytes;
        let actual_after_drop: usize = (0..engine.immutable_epoch_count())
            .map(|idx| engine.immutable_memtable(idx).estimated_size())
            .sum();
        assert_eq!(after_drop, actual_after_drop);
        assert!(engine
            .list_node_property_indexes().unwrap()
            .iter()
            .all(|entry| entry.index_id != info.index_id));

        engine.close().unwrap();
    }

    // --- Edge Property Index Declaration Tests ---

    #[test]
    fn test_edge_property_index_ensure_drop_list_and_reuses_range_declaration() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let eq = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(eq.state, SecondaryIndexState::Building);

        let eq_again = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(eq_again.index_id, eq.index_id);

        let range = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(range.state, SecondaryIndexState::Building);

        let indexes = engine.list_edge_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].fields, property_index_fields("label"));
        assert_eq!(indexes[1].fields, property_index_fields("score"));

        let range_again = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(range_again.index_id, range.index_id);

        assert!(engine
            .drop_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap());
        assert!(!engine
            .drop_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap());

        let indexes = engine.list_edge_property_indexes().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].index_id, range.index_id);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_index_retry_failed_clears_error_and_preserves_id() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let created = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        engine.shutdown_secondary_index_worker();

        engine
            .with_runtime_manifest_write(|manifest| {
                let entry = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == created.index_id)
                    .unwrap();
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some("boom".to_string());
                Ok(())
            })
            .unwrap();
        engine.rebuild_secondary_index_catalog().unwrap();

        let retried = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("label").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(retried.index_id, created.index_id);
        assert_eq!(retried.state, SecondaryIndexState::Building);
        assert!(retried.last_error.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn test_ensure_edge_property_index_seeds_active_and_immutable_memtables() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node_a = engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();

        let mut frozen_props = BTreeMap::new();
        frozen_props.insert("status".to_string(), PropValue::String("active".to_string()));
        frozen_props.insert("score".to_string(), PropValue::Int(30));
        let frozen_edge_id = engine
            .upsert_edge(
                node_a,
                node_b,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props: frozen_props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let node_c = engine
            .upsert_node("Person", "c", UpsertNodeOptions::default())
            .unwrap();
        let mut active_props = BTreeMap::new();
        active_props.insert("status".to_string(), PropValue::String("active".to_string()));
        active_props.insert("score".to_string(), PropValue::Int(50));
        let active_edge_id = engine
            .upsert_edge(
                node_a,
                node_c,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props: active_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let eq = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("status").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let range = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();

        let status_hash = hash_prop_equality_key(&PropValue::String("active".to_string()));
        let active_memtable = engine.active_memtable();
        let active_eq_state = active_memtable.secondary_eq_state();
        let active_eq_ids = active_eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(active_eq_ids.contains(&active_edge_id));

        let frozen_memtable = engine.immutable_memtable(0);
        let frozen_eq_state = frozen_memtable.secondary_eq_state();
        let frozen_eq_ids = frozen_eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&status_hash)
            .unwrap();
        assert!(frozen_eq_ids.contains(&frozen_edge_id));

        let active_range_state = active_memtable.secondary_range_state();
        let active_range = active_range_state.get(&range.index_id).unwrap();
        let score_50 = numeric_range_sort_key_for_value(&PropValue::Int(50)).unwrap();
        assert!(active_range.contains(&(score_50, active_edge_id)));

        let frozen_range_state = frozen_memtable.secondary_range_state();
        let frozen_range = frozen_range_state.get(&range.index_id).unwrap();
        let score_30 = numeric_range_sort_key_for_value(&PropValue::Int(30)).unwrap();
        assert!(frozen_range.contains(&(score_30, frozen_edge_id)));

        engine.close().unwrap();
    }

    fn compound_prefix_for_entry(
        entry: &SecondaryIndexManifestEntry,
        values: &[CompoundFieldValue<'_>],
    ) -> crate::secondary_index_key::CompoundPrefixBounds {
        let context = CompoundTupleContext::from_manifest_entry(entry).unwrap();
        let prefix = encode_compound_tuple_prefix(&context, values).unwrap();
        compound_prefix_bounds(&prefix)
    }

    fn compound_string_props(key: &str, value: &str) -> BTreeMap<String, PropValue> {
        let mut props = BTreeMap::new();
        props.insert(key.to_string(), PropValue::String(value.to_string()));
        props
    }

    fn compound_node_input(label: &str, key: &str, tenant: &str) -> NodeInput {
        NodeInput {
            labels: vec![label.to_string()],
            key: key.to_string(),
            props: compound_string_props("tenant", tenant),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }
    }

    fn compound_edge_input(from: u64, to: u64, label: &str, status: &str) -> EdgeInput {
        EdgeInput {
            from,
            to,
            label: label.to_string(),
            props: compound_string_props("status", status),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        }
    }

    fn compound_edge_options(status: &str) -> UpsertEdgeOptions {
        UpsertEdgeOptions {
            props: compound_string_props("status", status),
            ..Default::default()
        }
    }

    #[test]
    fn test_compound_index_ensure_seeds_active_and_immutable_memtables_and_drop_unregisters() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut frozen_props = BTreeMap::new();
        frozen_props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        let frozen_id = engine
            .upsert_node(
                "Person",
                "frozen",
                UpsertNodeOptions {
                    props: frozen_props,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let mut active_props = BTreeMap::new();
        active_props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        let active_id = engine
            .upsert_node(
                "Person",
                "active",
                UpsertNodeOptions {
                    props: active_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let info = engine
            .ensure_node_property_index(
                "Person",
                SecondaryIndexSpec {
                    fields: vec![
                        SecondaryIndexField::property("tenant"),
                        SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
                    ],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        assert_eq!(info.state, SecondaryIndexState::Building);
        assert!(info.compound);

        let active_memtable = engine.active_memtable();
        let active_entry = active_memtable
            .secondary_index_declarations()
            .get(&info.index_id)
            .unwrap()
            .clone();
        let tenant = PropValue::String("acme".to_string());
        let prefix = compound_prefix_for_entry(
            &active_entry,
            &[CompoundFieldValue::Property(Some(&tenant))],
        );
        assert_eq!(
            active_memtable.find_node_compound_prefix_at(info.index_id, &prefix, u64::MAX),
            vec![active_id]
        );

        let frozen_memtable = engine.immutable_memtable(0);
        assert_eq!(
            frozen_memtable.find_node_compound_prefix_at(info.index_id, &prefix, u64::MAX),
            vec![frozen_id]
        );

        let left = engine
            .upsert_node("Person", "left", UpsertNodeOptions::default())
            .unwrap();
        let right = engine
            .upsert_node("Person", "right", UpsertNodeOptions::default())
            .unwrap();
        let mut edge_props = BTreeMap::new();
        edge_props.insert("status".to_string(), PropValue::String("open".to_string()));
        let edge_id = engine
            .upsert_edge(
                left,
                right,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props: edge_props,
                    valid_to: Some(500),
                    ..Default::default()
                },
            )
            .unwrap();
        let edge_info = engine
            .ensure_edge_property_index(
                "RELATES_TO",
                SecondaryIndexSpec {
                    fields: vec![
                        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::To),
                        SecondaryIndexField::property("status"),
                    ],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        assert!(edge_info.compound);
        let active_memtable = engine.active_memtable();
        let edge_entry = active_memtable
            .secondary_index_declarations()
            .get(&edge_info.index_id)
            .unwrap()
            .clone();
        let status = PropValue::String("open".to_string());
        let edge_prefix = compound_prefix_for_entry(
            &edge_entry,
            &[
                CompoundFieldValue::MetadataU64(left),
                CompoundFieldValue::MetadataU64(right),
                CompoundFieldValue::Property(Some(&status)),
            ],
        );
        assert_eq!(
            active_memtable.find_edge_compound_prefix_at(
                edge_info.index_id,
                &edge_prefix,
                u64::MAX
            ),
            vec![edge_id]
        );

        assert!(engine
            .drop_node_property_index(
                "Person",
                SecondaryIndexSpec {
                    fields: vec![
                        SecondaryIndexField::property("tenant"),
                        SecondaryIndexField::node_meta(NodeMetadataIndexField::UpdatedAt),
                    ],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap());
        assert!(!engine
            .active_memtable()
            .compound_secondary_state()
            .contains_key(&info.index_id));
        assert!(!engine
            .immutable_memtable(0)
            .compound_secondary_state()
            .contains_key(&info.index_id));

        engine.close().unwrap();
    }

    #[test]
    fn test_compound_index_failed_retry_reseeds_tuple_state_and_clears_error() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        let node_id = engine
            .upsert_node(
                "Person",
                "retry",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        let spec = SecondaryIndexSpec {
            fields: vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::Id),
            ],
            kind: SecondaryIndexKind::Equality,
        };
        let created = engine
            .ensure_node_property_index("Person", spec.clone())
            .unwrap();
        engine
            .with_core_mut(|core| {
                core.remove_secondary_index_entry_from_memtables(created.index_id)?;
                Ok(())
            })
            .unwrap();
        assert!(!engine
            .active_memtable()
            .compound_secondary_state()
            .contains_key(&created.index_id));

        engine
            .with_runtime_manifest_write(|manifest| {
                let entry = manifest
                    .secondary_indexes
                    .iter_mut()
                    .find(|entry| entry.index_id == created.index_id)
                    .unwrap();
                entry.state = SecondaryIndexState::Failed;
                entry.last_error = Some("compound secondary index unavailable: boom".to_string());
                Ok(())
            })
            .unwrap();
        engine.rebuild_secondary_index_catalog().unwrap();

        let retried = engine
            .ensure_node_property_index("Person", spec)
            .unwrap();
        assert_eq!(retried.index_id, created.index_id);
        assert_eq!(retried.state, SecondaryIndexState::Building);
        assert!(retried.last_error.is_none());

        let active_memtable = engine.active_memtable();
        let entry = active_memtable
            .secondary_index_declarations()
            .get(&retried.index_id)
            .unwrap()
            .clone();
        let tenant = PropValue::String("acme".to_string());
        let prefix = compound_prefix_for_entry(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        assert_eq!(
            active_memtable.find_node_compound_prefix_at(retried.index_id, &prefix, u64::MAX),
            vec![node_id]
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_compound_index_node_batch_graph_patch_and_txn_writes_populate_tuple_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let spec = SecondaryIndexSpec {
            fields: vec![
                SecondaryIndexField::property("tenant"),
                SecondaryIndexField::node_meta(NodeMetadataIndexField::Id),
            ],
            kind: SecondaryIndexKind::Equality,
        };
        let info = engine
            .ensure_node_property_index("Person", spec)
            .unwrap();
        let tenant = PropValue::String("acme".to_string());
        let batch_ids = engine
            .batch_upsert_nodes(vec![
                compound_node_input("Person", "batch-a", "acme"),
                compound_node_input("Person", "batch-b", "acme"),
            ])
            .unwrap();
        let patch = GraphPatch {
            upsert_nodes: vec![compound_node_input("Person", "patch-a", "acme")],
            ..Default::default()
        };
        let patch_result = engine.graph_patch(patch).unwrap();
        let mut txn = engine.begin_write_txn().unwrap();
        let mut txn_props = BTreeMap::new();
        txn_props.insert("tenant".to_string(), tenant.clone());
        txn.upsert_node(
            "Person",
            "txn-a",
            UpsertNodeOptions {
                props: txn_props,
                ..Default::default()
            },
        )
        .unwrap();
        let committed = txn.commit().unwrap();

        let active_memtable = engine.active_memtable();
        let entry = active_memtable
            .secondary_index_declarations()
            .get(&info.index_id)
            .unwrap()
            .clone();
        let prefix = compound_prefix_for_entry(&entry, &[CompoundFieldValue::Property(Some(&tenant))]);
        let mut ids = active_memtable.find_node_compound_prefix_at(info.index_id, &prefix, u64::MAX);
        ids.sort_unstable();
        let mut expected = batch_ids;
        expected.extend(patch_result.node_ids);
        expected.extend(committed.node_ids);
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert_eq!(
            active_memtable.count_node_compound_prefix_at(info.index_id, &prefix, u64::MAX),
            expected.len()
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_compound_index_edge_batch_graph_patch_and_txn_writes_populate_tuple_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let opts = DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        };
        let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

        let endpoints = engine
            .batch_upsert_nodes(vec![
                compound_node_input("Endpoint", "a", "acme"),
                compound_node_input("Endpoint", "b", "acme"),
                compound_node_input("Endpoint", "c", "acme"),
                compound_node_input("Endpoint", "d", "acme"),
                compound_node_input("Endpoint", "e", "acme"),
                compound_node_input("Endpoint", "f", "acme"),
                compound_node_input("Endpoint", "g", "acme"),
                compound_node_input("Endpoint", "h", "acme"),
            ])
            .unwrap();
        let info = engine
            .ensure_edge_property_index(
                "RELATES_TO",
                SecondaryIndexSpec {
                    fields: vec![
                        SecondaryIndexField::property("status"),
                        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                        SecondaryIndexField::edge_meta(EdgeMetadataIndexField::To),
                    ],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();

        let draft_id = engine
            .upsert_edge(
                endpoints[0],
                endpoints[1],
                "RELATES_TO",
                compound_edge_options("draft"),
            )
            .unwrap();
        let batch_ids = engine
            .batch_upsert_edges(vec![
                compound_edge_input(endpoints[2], endpoints[3], "RELATES_TO", "open"),
                compound_edge_input(endpoints[3], endpoints[4], "RELATES_TO", "open"),
            ])
            .unwrap();
        let patch_result = engine
            .graph_patch(GraphPatch {
                upsert_edges: vec![compound_edge_input(
                    endpoints[4],
                    endpoints[5],
                    "RELATES_TO",
                    "open",
                )],
                ..Default::default()
            })
            .unwrap();

        let mut txn = engine.begin_write_txn().unwrap();
        txn.upsert_edge(
            TxnNodeRef::Id(endpoints[0]),
            TxnNodeRef::Id(endpoints[1]),
            "RELATES_TO",
            compound_edge_options("open"),
        )
        .unwrap();
        let txn_left = txn
            .upsert_node("Endpoint", "txn-left", UpsertNodeOptions::default())
            .unwrap();
        let txn_right = txn
            .upsert_node("Endpoint", "txn-right", UpsertNodeOptions::default())
            .unwrap();
        txn.upsert_edge(
            txn_left,
            txn_right,
            "RELATES_TO",
            compound_edge_options("open"),
        )
        .unwrap();
        let committed = txn.commit().unwrap();

        let active_memtable = engine.active_memtable();
        let entry = active_memtable
            .secondary_index_declarations()
            .get(&info.index_id)
            .unwrap()
            .clone();
        let open = PropValue::String("open".to_string());
        let open_prefix =
            compound_prefix_for_entry(&entry, &[CompoundFieldValue::Property(Some(&open))]);
        let draft = PropValue::String("draft".to_string());
        let draft_prefix =
            compound_prefix_for_entry(&entry, &[CompoundFieldValue::Property(Some(&draft))]);

        let mut ids =
            active_memtable.find_edge_compound_prefix_at(info.index_id, &open_prefix, u64::MAX);
        ids.sort_unstable();
        let mut expected = batch_ids;
        expected.extend(patch_result.edge_ids);
        expected.push(draft_id);
        expected.extend(committed.edge_ids);
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(ids, expected);
        assert_eq!(
            active_memtable.count_edge_compound_prefix_at(info.index_id, &open_prefix, u64::MAX),
            expected.len()
        );
        assert!(
            active_memtable
                .find_edge_compound_prefix_at(info.index_id, &draft_prefix, u64::MAX)
                .is_empty(),
            "transaction update must remove the superseded draft tuple"
        );

        engine.close().unwrap();
    }

    #[test]
    fn test_single_property_declaration_stays_out_of_compound_memtable_state_and_flush_sidecars() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let mut props = BTreeMap::new();
        props.insert("tenant".to_string(), PropValue::String("acme".to_string()));
        let node_id = engine
            .upsert_node(
                "Person",
                "single",
                UpsertNodeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        let single = engine
            .ensure_node_property_index(
                "Person",
                SecondaryIndexSpec {
                    fields: vec![SecondaryIndexField::property("tenant")],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        assert!(!single.compound);
        assert!(engine
            .active_memtable()
            .secondary_eq_state()
            .get(&single.index_id)
            .is_some_and(|groups| groups
                .get(&hash_prop_equality_key(&PropValue::String("acme".to_string())))
                .is_some_and(|ids| ids.contains(&node_id))));
        assert!(!engine
            .active_memtable()
            .compound_secondary_state()
            .contains_key(&single.index_id));

        let compound = engine
            .ensure_node_property_index(
                "Person",
                SecondaryIndexSpec {
                    fields: vec![
                        SecondaryIndexField::property("tenant"),
                        SecondaryIndexField::node_meta(NodeMetadataIndexField::Id),
                    ],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        engine.flush().unwrap();
        let seg_dir = segment_dir(&db_path, engine.segments_for_test()[0].segment_id);
        assert!(!crate::segment_writer::node_compound_eq_sidecar_path(
            &seg_dir,
            single.index_id
        )
        .exists());
        assert!(crate::segment_writer::node_compound_eq_sidecar_path(
            &seg_dir,
            compound.index_id
        )
        .exists());
        assert!(crate::segment_writer::node_prop_eq_sidecar_path(&seg_dir, single.index_id).exists());

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_index_foreground_maintenance() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(
            &db_path,
            &DbOptions {
                edge_uniqueness: true,
                ..DbOptions::default()
            },
        )
        .unwrap();

        let eq = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let range = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("weight").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();

        let node_a = engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();

        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        props.insert("weight".to_string(), PropValue::Int(10));
        let edge_id = engine
            .upsert_edge(
                node_a,
                node_b,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();

        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let mem = engine.active_memtable();
        let eq_state = mem.secondary_eq_state();
        assert!(eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&red_hash)
            .unwrap()
            .contains(&edge_id));
        let range_state = mem.secondary_range_state();
        let weight_10 = numeric_range_sort_key_for_value(&PropValue::Int(10)).unwrap();
        assert!(range_state
            .get(&range.index_id)
            .unwrap()
            .contains(&(weight_10, edge_id)));

        let mut updated_props = BTreeMap::new();
        updated_props.insert("color".to_string(), PropValue::String("blue".to_string()));
        updated_props.insert("weight".to_string(), PropValue::Int(20));
        engine
            .upsert_edge(
                node_a,
                node_b,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props: updated_props,
                    ..Default::default()
                },
            )
            .unwrap();

        let blue_hash = hash_prop_equality_key(&PropValue::String("blue".to_string()));
        let mem = engine.active_memtable();
        let eq_state = mem.secondary_eq_state();
        let red_ids = eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&red_hash);
        assert!(red_ids.is_none() || !red_ids.unwrap().contains(&edge_id));
        assert!(eq_state
            .get(&eq.index_id)
            .unwrap()
            .get(&blue_hash)
            .unwrap()
            .contains(&edge_id));
        let range_state = mem.secondary_range_state();
        let weight_20 = numeric_range_sort_key_for_value(&PropValue::Int(20)).unwrap();
        assert!(range_state
            .get(&range.index_id)
            .unwrap()
            .contains(&(weight_20, edge_id)));
        assert!(!range_state
            .get(&range.index_id)
            .unwrap()
            .contains(&(weight_10, edge_id)));

        engine.delete_edge(edge_id).unwrap();
        let mem = engine.active_memtable();
        let eq_state = mem.secondary_eq_state();
        let blue_gone = eq_state
            .get(&eq.index_id)
            .and_then(|groups| groups.get(&blue_hash))
            .is_none_or(|ids| !ids.contains(&edge_id));
        assert!(blue_gone);
        let range_gone = mem
            .secondary_range_state()
            .get(&range.index_id)
            .is_none_or(|entries| !entries.contains(&(weight_20, edge_id)));
        assert!(range_gone);

        engine.close().unwrap();
    }

    #[test]
    fn test_node_and_edge_property_indexes_coexist() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(
            &db_path,
            &DbOptions {
                edge_uniqueness: true,
                ..DbOptions::default()
            },
        )
        .unwrap();

        let node_eq = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let edge_eq = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_ne!(node_eq.index_id, edge_eq.index_id);

        let node_indexes = engine.list_node_property_indexes().unwrap();
        assert_eq!(node_indexes.len(), 1);
        assert_eq!(node_indexes[0].index_id, node_eq.index_id);

        let edge_indexes = engine.list_edge_property_indexes().unwrap();
        assert_eq!(edge_indexes.len(), 1);
        assert_eq!(edge_indexes[0].index_id, edge_eq.index_id);

        let node_a = engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropValue::Int(42));
        let node_id = engine
            .upsert_node(
                "Person",
                "x",
                UpsertNodeOptions {
                    props: props.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
        let edge_id = engine
            .upsert_edge(
                node_a,
                node_b,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();

        let score_hash = hash_prop_equality_key(&PropValue::Int(42));
        let mem = engine.active_memtable();
        let eq_state = mem.secondary_eq_state();

        let node_ids = eq_state
            .get(&node_eq.index_id)
            .unwrap()
            .get(&score_hash)
            .unwrap();
        assert!(node_ids.contains(&node_id));
        assert!(!node_ids.contains(&edge_id));

        let edge_ids = eq_state
            .get(&edge_eq.index_id)
            .unwrap()
            .get(&score_hash)
            .unwrap();
        assert!(edge_ids.contains(&edge_id));
        assert!(!edge_ids.contains(&node_id));

        assert!(engine
            .drop_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap());
        let node_indexes = engine.list_node_property_indexes().unwrap();
        assert_eq!(node_indexes.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_index_shared_id_sequence() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node_idx = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("x").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let edge_idx = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("x").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(edge_idx.index_id, node_idx.index_id + 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_index_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let (color_index_id, weight_index_id) = {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
            let color_info = engine
                .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
                .unwrap();
            let weight_info = engine
                .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("weight").to_string() }], kind: SecondaryIndexKind::Range })
                .unwrap();
            let index_ids = (color_info.index_id, weight_info.index_id);
            engine.close().unwrap();
            index_ids
        };

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let color = wait_for_edge_property_index_state(
            &engine,
            color_index_id,
            SecondaryIndexState::Ready,
        );
        let weight = wait_for_edge_property_index_state(
            &engine,
            weight_index_id,
            SecondaryIndexState::Ready,
        );
        let indexes = engine.list_edge_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        assert_eq!(color.fields, property_index_fields("color"));
        assert!(matches!(color.kind, SecondaryIndexKind::Equality));
        assert_eq!(color.state, SecondaryIndexState::Ready);
        assert_eq!(weight.fields, property_index_fields("weight"));
        assert!(matches!(
            weight.kind,
            SecondaryIndexKind::Range
        ));
        assert_eq!(weight.state, SecondaryIndexState::Ready);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_background_build_writes_sidecar_and_publishes_ready() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node_a = engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        engine
            .upsert_edge(
                node_a,
                node_b,
                "RELATES_TO",
                UpsertEdgeOptions {
                    props,
                    ..Default::default()
                },
            )
            .unwrap();
        let segment_info = engine.flush().unwrap().expect("segment should flush");

        let info = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        assert_eq!(info.state, SecondaryIndexState::Building);

        let seg_dir = db_path
            .join("segments")
            .join(format!("seg_{:04}", segment_info.id));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(bytes) = std::fs::read(
                seg_dir.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME),
            ) {
                let manifest =
                    crate::segment_components::decode_manifest_envelope(&bytes).unwrap();
                let has_edge_sidecar = manifest.components.iter().any(|record| {
                    record.kind
                        == crate::segment_components::SegmentComponentKind::EdgePropertyEqualityIndex {
                            index_id: info.index_id,
                        }
                });
                if has_edge_sidecar {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "edge sidecar component record was not background-built"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let sidecar_dir = seg_dir.join("secondary_indexes");
        let sidecar_exists = std::fs::read_dir(&sidecar_dir)
            .unwrap()
            .flatten()
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("edge_prop_eq_"))
            });
        assert!(sidecar_exists);

        wait_for_edge_property_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

        engine.close().unwrap();
    }

    #[test]
    fn test_node_property_background_build_writes_targeted_planner_stats() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        for (key, color, score) in [("a", "red", 10), ("b", "red", 20), ("c", "blue", 30)] {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            props.insert("score".to_string(), PropValue::Int(score));
            engine
                .upsert_node(
                    "Person",
                    key,
                    UpsertNodeOptions {
                        props,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        engine.flush().unwrap().expect("segment should flush");
        assert!(engine.segments_for_test()[0]
            .planner_stats()
            .unwrap()
            .equality_index_stats
            .is_empty());

        let color = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let score = engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(color.state, SecondaryIndexState::Building);
        assert_eq!(score.state, SecondaryIndexState::Building);
        wait_for_property_index_state(
            &engine,
            color.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_property_index_state(
            &engine,
            score.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_published_property_index_state(
            &engine,
            color.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_published_property_index_state(
            &engine,
            score.index_id,
            SecondaryIndexState::Ready,
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let reader = engine.segments_for_test()[0].clone();
            let stats = reader.planner_stats().expect("targeted stats should load");
            let equality = stats
                .equality_index_stats
                .iter()
                .find(|stats| stats.index_id == color.index_id);
            let range = stats
                .range_index_stats
                .iter()
                .find(|stats| stats.index_id == score.index_id);
            if let (Some(equality), Some(range)) = (equality, range) {
                assert_eq!(
                    stats.build_kind,
                    crate::planner_stats::PlannerStatsBuildKind::SecondaryIndexRefresh
                );
                assert_eq!(equality.total_postings, 3);
                assert_eq!(equality.value_group_count, 2);
                assert!(equality.sidecar_present_at_build);
                assert_eq!(range.total_entries, 3);
                assert!(range.sidecar_present_at_build);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for node targeted planner stats; stats: {:?}",
                stats
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_background_build_writes_targeted_planner_stats() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let node_a = engine
            .upsert_node("Person", "a", UpsertNodeOptions::default())
            .unwrap();
        let node_b = engine
            .upsert_node("Person", "b", UpsertNodeOptions::default())
            .unwrap();
        for (color, score) in [("red", 10), ("red", 20), ("blue", 30)] {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            props.insert("score".to_string(), PropValue::Int(score));
            engine
                .upsert_edge(
                    node_a,
                    node_b,
                    "RELATES_TO",
                    UpsertEdgeOptions {
                        props,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        engine.flush().unwrap().expect("segment should flush");
        assert!(engine.segments_for_test()[0]
            .planner_stats()
            .unwrap()
            .equality_index_stats
            .is_empty());

        let color = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
            .unwrap();
        let score = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(color.state, SecondaryIndexState::Building);
        assert_eq!(score.state, SecondaryIndexState::Building);
        wait_for_edge_property_index_state(
            &engine,
            color.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_edge_property_index_state(
            &engine,
            score.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_published_property_index_state(
            &engine,
            color.index_id,
            SecondaryIndexState::Ready,
        );
        wait_for_published_property_index_state(
            &engine,
            score.index_id,
            SecondaryIndexState::Ready,
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let reader = engine.segments_for_test()[0].clone();
            let stats = reader.planner_stats().expect("targeted stats should load");
            let equality = stats
                .equality_index_stats
                .iter()
                .find(|stats| stats.index_id == color.index_id);
            let range = stats
                .range_index_stats
                .iter()
                .find(|stats| stats.index_id == score.index_id);
            if let (Some(equality), Some(range)) = (equality, range) {
                assert_eq!(
                    stats.build_kind,
                    crate::planner_stats::PlannerStatsBuildKind::SecondaryIndexRefresh
                );
                assert_eq!(equality.total_postings, 3);
                assert_eq!(equality.value_group_count, 2);
                assert!(equality.sidecar_present_at_build);
                assert_eq!(range.total_entries, 3);
                assert!(range.sidecar_present_at_build);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for edge targeted planner stats; stats: {:?}",
                stats
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        engine.close().unwrap();
    }

    #[test]
    fn test_node_edge_range_declarations_are_scoped_to_target() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        engine
            .ensure_node_property_index("Person", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();

        let edge_range = engine
            .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("score").to_string() }], kind: SecondaryIndexKind::Range })
            .unwrap();
        assert_eq!(edge_range.state, SecondaryIndexState::Building);

        engine.close().unwrap();
    }

    #[test]
    fn test_edge_property_index_wal_recovery_rebuilds_memtable_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("testdb");

        let eq_index_id;
        let range_index_id;
        let edge_id;
        {
            let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

            let eq = engine
                .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("color").to_string() }], kind: SecondaryIndexKind::Equality })
                .unwrap();
            let range = engine
                .ensure_edge_property_index("RELATES_TO", SecondaryIndexSpec { fields: vec![SecondaryIndexField::Property { key: ("weight").to_string() }], kind: SecondaryIndexKind::Range })
                .unwrap();
            eq_index_id = eq.index_id;
            range_index_id = range.index_id;

            let node_a = engine
                .upsert_node("Person", "a", UpsertNodeOptions::default())
                .unwrap();
            let node_b = engine
                .upsert_node("Person", "b", UpsertNodeOptions::default())
                .unwrap();

            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String("red".to_string()));
            props.insert("weight".to_string(), PropValue::Int(42));
            edge_id = engine
                .upsert_edge(
                    node_a,
                    node_b,
                    "RELATES_TO",
                    UpsertEdgeOptions {
                        props,
                        ..Default::default()
                    },
                )
                .unwrap();

            engine.sync().unwrap();
            // close_fast skips the memtable flush so edges remain in the WAL
            // and will be replayed into the memtable on reopen.
            engine.close_fast().unwrap();
        }

        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

        let indexes = engine.list_edge_property_indexes().unwrap();
        assert_eq!(indexes.len(), 2);

        let mem = engine.active_memtable();
        let eq_state = mem.secondary_eq_state();
        let color_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let eq_ids = eq_state
            .get(&eq_index_id)
            .expect("eq index should exist after WAL recovery")
            .get(&color_hash)
            .expect("color hash group should exist after WAL recovery");
        assert!(eq_ids.contains(&edge_id));

        let range_state = mem.secondary_range_state();
        let range_entries = range_state
            .get(&range_index_id)
            .expect("range index should exist after WAL recovery");
        let weight_42 = numeric_range_sort_key_for_value(&PropValue::Int(42)).unwrap();
        assert!(range_entries.contains(&(weight_42, edge_id)));

        engine.close().unwrap();
    }
