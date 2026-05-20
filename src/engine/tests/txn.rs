// --- Explicit write transaction tests ---

fn edge_local(edge: TxnEdgeRef) -> Option<TxnLocalRef> {
    match edge {
        TxnEdgeRef::Local(local) => Some(local),
        _ => None,
    }
}

#[test]
fn test_write_txn_staged_reads_and_rollback_leave_no_trace() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");

    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let mut txn = engine.begin_write_txn().unwrap();
        let alice = txn
            .upsert_node("Person", "alice", UpsertNodeOptions::default())
            .unwrap();

        let staged = txn.get_node(alice).unwrap().unwrap();
        assert_eq!(staged.id, None);
        assert_eq!(staged.labels, vec!["Person".to_string()]);
        assert_eq!(staged.created_at, None);
        assert_eq!(staged.updated_at, None);
        assert_eq!(staged.key, "alice");
        assert!(engine.get_node_by_key("Person", "alice").unwrap().is_none());

        txn.rollback().unwrap();
        assert!(matches!(
            txn.upsert_node("Person", "after", UpsertNodeOptions::default()),
            Err(EngineError::TxnClosed)
        ));
        assert!(engine.get_node_by_key("Person", "alice").unwrap().is_none());
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert!(reopened.get_node_by_key("Person", "alice").unwrap().is_none());
    reopened.close().unwrap();
}

#[test]
fn test_write_txn_rollback_does_not_create_named_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let left = txn
        .upsert_node("TxnRollbackNode", "left", UpsertNodeOptions::default())
        .unwrap();
    let right = txn
        .upsert_node("TxnRollbackNode", "right", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_edge(
        left,
        right,
        "TXN_ROLLBACK_EDGE",
        UpsertEdgeOptions::default(),
    )
    .unwrap();
    assert!(txn
        .get_node_by_key("TxnRollbackNode", "left")
        .unwrap()
        .is_some());

    txn.rollback().unwrap();
    assert_eq!(engine.get_node_label_id("TxnRollbackNode").unwrap(), None);
    assert_eq!(engine.get_edge_label_id("TXN_ROLLBACK_EDGE").unwrap(), None);
    assert!(engine
        .get_node_by_key("TxnRollbackNode", "left")
        .unwrap()
        .is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_unknown_read_only_lookups_do_not_create_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("KnownTxnNode", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("KnownTxnNode", "b", UpsertNodeOptions::default())
        .unwrap();

    let txn = engine.begin_write_txn().unwrap();
    assert!(txn
        .get_node_by_key("MissingTxnNode", "a")
        .unwrap()
        .is_none());
    assert!(txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "MISSING_TXN_EDGE")
        .unwrap()
        .is_none());
    assert_eq!(engine.get_node_label_id("MissingTxnNode").unwrap(), None);
    assert_eq!(engine.get_edge_label_id("MISSING_TXN_EDGE").unwrap(), None);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_lifecycle_closed_db_and_finished_txn_rules() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut rollback_txn = engine.begin_write_txn().unwrap();
    rollback_txn.rollback().unwrap();
    assert!(matches!(rollback_txn.rollback(), Err(EngineError::TxnClosed)));
    assert!(matches!(rollback_txn.commit(), Err(EngineError::TxnClosed)));

    let mut commit_txn = engine.begin_write_txn().unwrap();
    commit_txn
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    commit_txn.commit().unwrap();
    assert!(matches!(commit_txn.commit(), Err(EngineError::TxnClosed)));

    let rollback_after_close = engine.begin_write_txn().unwrap();
    engine.close().unwrap();
    assert!(matches!(
        engine.begin_write_txn(),
        Err(EngineError::DatabaseClosed)
    ));
    let mut rollback_after_close = rollback_after_close;
    rollback_after_close.rollback().unwrap();
}

#[test]
fn test_write_txn_read_own_writes_update_and_delete_views() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let alice_id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let alice_ref = txn
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    let updated = txn.get_node(alice_ref.clone()).unwrap().unwrap();
    assert_eq!(updated.id, Some(alice_id));
    assert!(updated.created_at.is_some());
    assert_eq!(updated.updated_at, None);
    assert!((updated.weight - 2.0).abs() < f32::EPSILON);
    assert_eq!(txn.get_node_by_key("Person", "alice").unwrap().unwrap().id, Some(alice_id));

    txn.delete_node(alice_ref).unwrap();
    assert!(txn.get_node(TxnNodeRef::Id(alice_id)).unwrap().is_none());
    assert!(txn.get_node_by_key("Person", "alice").unwrap().is_none());

    let staged = txn
        .upsert_node("Person", "staged", UpsertNodeOptions::default())
        .unwrap();
    assert!(txn.get_node(staged.clone()).unwrap().is_some());
    txn.delete_node(staged).unwrap();
    assert!(txn.get_node_by_key("Person", "staged").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_commit_create_and_connect_local_refs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let alice = txn
        .upsert_node_as("alice", "Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = txn
        .upsert_node_as("bob", "Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let edge = txn
        .upsert_edge_as(
            "knows",
            alice.clone(),
            bob.clone(),
            "FRIENDS_WITH",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    let staged_edge = txn.get_edge(edge.clone()).unwrap().unwrap();
    assert_eq!(staged_edge.id, None);
    assert_eq!(staged_edge.label, "FRIENDS_WITH".to_string());
    assert_eq!(staged_edge.from, alice);
    assert_eq!(staged_edge.to, bob);

    let result = txn.commit().unwrap();
    assert_eq!(result.node_ids.len(), 2);
    assert_eq!(result.edge_ids.len(), 1);
    let alice_id = result
        .local_node_ids
        .get(&TxnLocalRef::Alias("alice".into()))
        .copied()
        .unwrap();
    let bob_id = result
        .local_node_ids
        .get(&TxnLocalRef::Alias("bob".into()))
        .copied()
        .unwrap();
    let edge_id = result
        .local_edge_ids
        .get(&TxnLocalRef::Alias("knows".into()))
        .copied()
        .unwrap();

    assert_eq!(engine.get_node(alice_id).unwrap().unwrap().key, "alice");
    assert_eq!(engine.get_node(bob_id).unwrap().unwrap().key, "bob");
    let committed_edge = engine.get_edge(edge_id).unwrap().unwrap();
    assert_eq!(
        (
            committed_edge.from,
            committed_edge.to,
            committed_edge.label
        ),
        (alice_id, bob_id, "FRIENDS_WITH".to_string())
    );
    engine.close().unwrap();

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    assert_eq!(reopened.get_edge(edge_id).unwrap().unwrap().from, alice_id);
    reopened.close().unwrap();
}

#[test]
fn test_write_txn_first_use_tokens_precede_dependent_wal_ops() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let opts = DbOptions {
        wal_sync_mode: WalSyncMode::Immediate,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(&db_path, &opts).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let alice = txn
        .upsert_node("TxnWalPerson", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = txn
        .upsert_node("TxnWalPerson", "bob", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_edge(alice, bob, "TXN_WAL_KNOWS", UpsertEdgeOptions::default())
        .unwrap();
    let result = txn.commit().unwrap();
    let alice_id = result.node_ids[0];
    let edge_id = result.edge_ids[0];

    let ops = WalReader::read_generation(&db_path, 0).unwrap();
    let node_token_pos = ops
        .iter()
        .position(|(_, op)| {
            matches!(
                op,
                WalOp::EnsureNodeLabel { label, .. } if label == "TxnWalPerson"
            )
        })
        .unwrap();
    let first_node_pos = ops
        .iter()
        .position(|(_, op)| matches!(op, WalOp::UpsertNode(node) if node.id == alice_id))
        .unwrap();
    let edge_token_pos = ops
        .iter()
        .position(|(_, op)| {
            matches!(
                op,
                WalOp::EnsureEdgeLabel { label, .. } if label == "TXN_WAL_KNOWS"
            )
        })
        .unwrap();
    let edge_pos = ops
        .iter()
        .position(|(_, op)| matches!(op, WalOp::UpsertEdge(edge) if edge.id == edge_id))
        .unwrap();
    assert!(node_token_pos < first_node_pos);
    assert!(edge_token_pos < edge_pos);

    drop(engine);
    let reopened = DatabaseEngine::open(&db_path, &opts).unwrap();
    assert_eq!(
        reopened
            .get_node(alice_id)
            .unwrap()
            .unwrap()
            .labels
            .as_slice(),
        ["TxnWalPerson"]
    );
    assert_eq!(
        reopened.get_edge(edge_id).unwrap().unwrap().label,
        "TXN_WAL_KNOWS"
    );
    reopened.close().unwrap();
}

#[test]
fn test_write_txn_stage_intents_assigns_unaliased_slots_atomically() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.stage_intents(vec![
        TxnIntent::UpsertNode {
            alias: None,
            labels: vec!["Person".to_string()],
            key: "alice".into(),
            options: UpsertNodeOptions::default(),
        },
        TxnIntent::UpsertNode {
            alias: None,
            labels: vec!["Person".to_string()],
            key: "bob".into(),
            options: UpsertNodeOptions::default(),
        },
        TxnIntent::UpsertEdge {
            alias: None,
            from: TxnNodeRef::Local(TxnLocalRef::Slot(0)),
            to: TxnNodeRef::Local(TxnLocalRef::Slot(1)),
            label: "FRIENDS_WITH".to_string(),
            options: UpsertEdgeOptions::default(),
        },
    ])
    .unwrap();
    assert!(txn
        .get_edge(TxnEdgeRef::Local(TxnLocalRef::Slot(2)))
        .unwrap()
        .is_some());

    let result = txn.commit().unwrap();
    let alice = result
        .node_id(&TxnNodeRef::Local(TxnLocalRef::Slot(0)))
        .unwrap();
    let bob = result
        .node_id(&TxnNodeRef::Local(TxnLocalRef::Slot(1)))
        .unwrap();
    let edge = result
        .edge_id(&TxnEdgeRef::Local(TxnLocalRef::Slot(2)))
        .unwrap();
    let committed_edge = engine.get_edge(edge).unwrap().unwrap();
    assert_eq!((committed_edge.from, committed_edge.to), (alice, bob));

    let mut failed = engine.begin_write_txn().unwrap();
    assert!(matches!(
        failed.stage_intents(vec![TxnIntent::UpsertEdge {
            alias: None,
            from: TxnNodeRef::Local(TxnLocalRef::Slot(9)),
            to: TxnNodeRef::Local(TxnLocalRef::Slot(10)),
            label: "RELATED_TO".to_string(),
            options: UpsertEdgeOptions::default(),
        }]),
        Err(EngineError::InvalidOperation(_))
    ));
    assert_eq!(
        failed
            .upsert_node("Person", "after-failed-stage", UpsertNodeOptions::default())
            .unwrap(),
        TxnNodeRef::Local(TxnLocalRef::Slot(0))
    );
    engine.close().unwrap();
}

#[test]
fn test_write_txn_overlay_normalizes_mixed_endpoint_refs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let alice_id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob_id = engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let alice = txn
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = txn
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    let edge = txn
        .upsert_edge(alice.clone(), bob.clone(), "RELATED_TO", UpsertEdgeOptions::default())
        .unwrap();

    assert_eq!(
        txn.get_edge_by_triple(
            TxnNodeRef::Key {
                label: "Person".to_string(),
                key: "alice".into(),
            },
            TxnNodeRef::Id(bob_id),
            "RELATED_TO",
        )
        .unwrap()
        .unwrap()
        .local,
        Some(match edge.clone() {
            TxnEdgeRef::Local(local) => local,
            _ => unreachable!(),
        })
    );
    assert!(txn
        .get_edge_by_triple(TxnNodeRef::Id(alice_id), TxnNodeRef::Id(bob_id), "RELATED_TO")
        .unwrap()
        .is_some());

    txn.invalidate_edge(
        TxnEdgeRef::Triple {
            from: TxnNodeRef::Key {
                label: "Person".to_string(),
                key: "alice".into(),
            },
            to: TxnNodeRef::Key {
                label: "Person".to_string(),
                key: "bob".into(),
            },
            label: "RELATED_TO".to_string(),
        },
        123,
    )
    .unwrap();
    assert_eq!(txn.get_edge(edge.clone()).unwrap().unwrap().valid_to, Some(123));

    txn.delete_edge(TxnEdgeRef::Triple {
        from: TxnNodeRef::Id(alice_id),
        to: TxnNodeRef::Id(bob_id),
        label: "RELATED_TO".to_string(),
    })
    .unwrap();
    assert!(txn.get_edge(edge).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_multi_label_overlay_label_diff_keeps_node_identity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let node = txn
        .upsert_node(
            &["TxnOverlayA", "TxnOverlayB"],
            "same",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let peer = txn
        .upsert_node("TxnOverlayPeer", "peer", UpsertNodeOptions::default())
        .unwrap();
    let edge = txn
        .upsert_edge(
            TxnNodeRef::Key {
                label: "TxnOverlayB".to_string(),
                key: "same".to_string(),
            },
            peer.clone(),
            "TXN_OVERLAY_EDGE",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    assert!(txn.add_node_label(node.clone(), "TxnOverlayC").unwrap());
    assert!(txn.remove_node_label(node.clone(), "TxnOverlayB").unwrap());
    assert!(txn
        .get_node_by_key("TxnOverlayB", "same")
        .unwrap()
        .is_none());
    assert_eq!(
        txn.get_node_by_key("TxnOverlayC", "same")
            .unwrap()
            .unwrap()
            .local,
        match node.clone() {
            TxnNodeRef::Local(local) => Some(local),
            _ => None,
        }
    );
    assert!(txn.get_edge(edge.clone()).unwrap().is_some());
    assert!(txn
        .get_edge_by_triple(
            TxnNodeRef::Key {
                label: "TxnOverlayC".to_string(),
                key: "same".to_string(),
            },
            peer.clone(),
            "TXN_OVERLAY_EDGE",
        )
        .unwrap()
        .is_some());

    let replacement = txn
        .upsert_node(
            "TxnOverlayB",
            "same",
            UpsertNodeOptions {
                weight: 9.0,
                ..Default::default()
            },
        )
        .unwrap();
    let original = txn.get_node(node.clone()).unwrap().unwrap();
    let replacement_view = txn.get_node(replacement).unwrap().unwrap();
    assert_eq!(
        original.labels,
        vec!["TxnOverlayA".to_string(), "TxnOverlayC".to_string()]
    );
    assert_eq!(replacement_view.labels, vec!["TxnOverlayB".to_string()]);
    assert!((replacement_view.weight - 9.0).abs() < f32::EPSILON);
    assert!(txn.get_edge(edge).unwrap().is_some());
    assert!(txn
        .get_edge_by_triple(
            TxnNodeRef::Key {
                label: "TxnOverlayB".to_string(),
                key: "same".to_string(),
            },
            peer.clone(),
            "TXN_OVERLAY_EDGE",
        )
        .unwrap()
        .is_none());

    let result = txn.commit().unwrap();
    let original_id = engine
        .get_node_by_key("TxnOverlayA", "same")
        .unwrap()
        .unwrap()
        .id;
    let replacement_id = engine
        .get_node_by_key("TxnOverlayB", "same")
        .unwrap()
        .unwrap()
        .id;
    assert_ne!(original_id, replacement_id);
    assert_eq!(
        engine
            .get_node_by_key("TxnOverlayC", "same")
            .unwrap()
            .unwrap()
            .id,
        original_id
    );
    assert_eq!(
        engine
            .get_edge(result.edge_ids[0])
            .unwrap()
            .unwrap()
            .from,
        original_id
    );
    engine.close().unwrap();
}

#[test]
fn test_write_txn_multi_label_delete_cascades_staged_edge_after_label_changes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let node = txn
        .upsert_node(
            &["TxnCascadeA", "TxnCascadeB"],
            "node",
            UpsertNodeOptions::default(),
        )
        .unwrap();
    let peer = txn
        .upsert_node("TxnCascadePeer", "peer", UpsertNodeOptions::default())
        .unwrap();
    let edge = txn
        .upsert_edge(
            TxnNodeRef::Key {
                label: "TxnCascadeB".to_string(),
                key: "node".to_string(),
            },
            peer.clone(),
            "TXN_CASCADE_EDGE",
            UpsertEdgeOptions::default(),
        )
        .unwrap();

    assert!(txn.add_node_label(node.clone(), "TxnCascadeC").unwrap());
    assert!(txn.remove_node_label(node.clone(), "TxnCascadeB").unwrap());
    txn.delete_node(TxnNodeRef::Key {
        label: "TxnCascadeC".to_string(),
        key: "node".to_string(),
    })
    .unwrap();

    assert!(txn.get_edge(edge).unwrap().is_none());
    assert!(txn
        .get_edge_by_triple(
            TxnNodeRef::Key {
                label: "TxnCascadeA".to_string(),
                key: "node".to_string(),
            },
            peer,
            "TXN_CASCADE_EDGE",
        )
        .unwrap()
        .is_none());
    txn.rollback().unwrap();
    engine.close().unwrap();
}

#[test]
fn test_write_txn_overlay_latest_opinion_updates_all_locals() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let first_node = txn
        .upsert_node(
            "Person",
            "a",
            UpsertNodeOptions {
                weight: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    let second_node = txn
        .upsert_node(
            "Person",
            "a",
            UpsertNodeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    assert!((txn.get_node(first_node.clone()).unwrap().unwrap().weight - 2.0).abs() < f32::EPSILON);
    txn.delete_node(second_node.clone()).unwrap();
    assert!(txn.get_node(first_node.clone()).unwrap().is_none());
    txn.upsert_node(
        "Person",
        "a",
        UpsertNodeOptions {
            weight: 3.0,
            ..Default::default()
        },
    )
    .unwrap();
    let revived = txn.get_node(first_node).unwrap().unwrap();
    assert_eq!(revived.id, Some(a));
    assert!((revived.weight - 3.0).abs() < f32::EPSILON);

    let first_edge = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 1.0;
            options
        })
        .unwrap();
    let second_edge = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 2.0;
            options
        })
        .unwrap();
    assert!((txn.get_edge(first_edge.clone()).unwrap().unwrap().weight - 2.0).abs() < f32::EPSILON);
    txn.invalidate_edge(second_edge.clone(), 55).unwrap();
    assert_eq!(
        txn.get_edge(first_edge.clone()).unwrap().unwrap().valid_to,
        Some(55)
    );
    txn.delete_edge(second_edge).unwrap();
    assert!(txn.get_edge(first_edge).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_delete_then_reupsert_preserves_committed_identity_in_reads() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(
        &db_path,
        &DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        },
    )
    .unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let staged_a = txn
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    txn.delete_node(staged_a).unwrap();
    let revived_a = txn
        .upsert_node(
            "Person",
            "a",
            UpsertNodeOptions {
                weight: 4.0,
                ..Default::default()
            },
        )
        .unwrap();
    let revived_a_view = txn.get_node(revived_a).unwrap().unwrap();
    assert_eq!(revived_a_view.id, Some(a));
    assert!(revived_a_view.created_at.is_some());

    let staged_edge = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    txn.delete_edge(staged_edge).unwrap();
    let revived_edge = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let revived_edge_view = txn.get_edge(revived_edge).unwrap().unwrap();
    assert_eq!(revived_edge_view.id, Some(edge_id));
    assert!(revived_edge_view.created_at.is_some());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_nonunique_same_triple_edges_keep_local_identity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let first = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 1.0;
            options
        })
        .unwrap();
    let second = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 2.0;
            options
        })
        .unwrap();

    assert!((txn.get_edge(first.clone()).unwrap().unwrap().weight - 1.0).abs() < f32::EPSILON);
    let latest = txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH")
        .unwrap()
        .unwrap();
    assert_eq!(latest.local, edge_local(second.clone()));
    assert!((latest.weight - 2.0).abs() < f32::EPSILON);

    txn.delete_edge(second.clone()).unwrap();
    assert!(txn.get_edge(first.clone()).unwrap().is_some());
    assert!(txn.get_edge(second).unwrap().is_none());
    assert!(txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH")
        .unwrap()
        .is_none());

    let result = txn.commit().unwrap();
    assert_eq!(result.edge_ids.len(), 2);
    assert!(engine.get_edge(result.edge_ids[0]).unwrap().is_some());
    assert!(engine.get_edge(result.edge_ids[1]).unwrap().is_none());
    assert!(engine.get_edge_by_triple(a, b, "FRIENDS_WITH").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_nonunique_old_same_triple_delete_or_invalidate_keeps_latest() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut delete_txn = engine.begin_write_txn().unwrap();
    let first = delete_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 1.0;
            options
        })
        .unwrap();
    let second = delete_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 2.0;
            options
        })
        .unwrap();
    delete_txn.delete_edge(first.clone()).unwrap();
    assert!(delete_txn.get_edge(first).unwrap().is_none());
    let latest = delete_txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH")
        .unwrap()
        .unwrap();
    assert_eq!(latest.local, edge_local(second.clone()));
    assert!((latest.weight - 2.0).abs() < f32::EPSILON);
    let delete_result = delete_txn.commit().unwrap();
    assert!(engine.get_edge(delete_result.edge_ids[0]).unwrap().is_none());
    assert_eq!(
        engine.get_edge_by_triple(a, b, "FRIENDS_WITH").unwrap().unwrap().id,
        delete_result.edge_ids[1]
    );

    let mut invalidate_txn = engine.begin_write_txn().unwrap();
    let third = invalidate_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "COLLABORATES_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 3.0;
            options
        })
        .unwrap();
    let fourth = invalidate_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "COLLABORATES_WITH", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 4.0;
            options
        })
        .unwrap();
    invalidate_txn.invalidate_edge(third.clone(), 123).unwrap();
    assert_eq!(
        invalidate_txn
            .get_edge(third)
            .unwrap()
            .unwrap()
            .valid_to,
        Some(123)
    );
    let latest = invalidate_txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "COLLABORATES_WITH")
        .unwrap()
        .unwrap();
    assert_eq!(latest.local, edge_local(fourth));
    assert!((latest.weight - 4.0).abs() < f32::EPSILON);
    let invalidate_result = invalidate_txn.commit().unwrap();
    assert_eq!(
        engine.get_edge_by_triple(a, b, "COLLABORATES_WITH").unwrap().unwrap().id,
        invalidate_result.edge_ids[1]
    );

    let regular_first = engine
        .upsert_edge(a, b, "RELATED_TO", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 5.0;
            options
        })
        .unwrap();
    let regular_second = engine
        .upsert_edge(a, b, "RELATED_TO", {
            let mut options = UpsertEdgeOptions::default();
            options.weight = 6.0;
            options
        })
        .unwrap();
    engine.invalidate_edge(regular_first, 456).unwrap();
    assert_eq!(
        engine.get_edge_by_triple(a, b, "RELATED_TO").unwrap().unwrap().id,
        regular_second
    );
    engine.delete_edge(regular_first).unwrap();
    assert_eq!(
        engine.get_edge_by_triple(a, b, "RELATED_TO").unwrap().unwrap().id,
        regular_second
    );
    engine.close().unwrap();
}

#[test]
fn test_write_txn_delete_node_reupsert_does_not_revive_incident_edges() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let committed_edge = engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();

    let mut committed_txn = engine.begin_write_txn().unwrap();
    committed_txn.delete_node(TxnNodeRef::Id(a)).unwrap();
    committed_txn
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    assert!(committed_txn
        .get_edge(TxnEdgeRef::Id(committed_edge))
        .unwrap()
        .is_none());
    assert!(committed_txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH")
        .unwrap()
        .is_none());
    let revived = committed_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    assert!(committed_txn.get_edge(revived).unwrap().is_some());
    committed_txn.rollback().unwrap();

    let mut staged_txn = engine.begin_write_txn().unwrap();
    let staged_edge = staged_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    staged_txn.delete_node(TxnNodeRef::Id(a)).unwrap();
    staged_txn
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    assert!(staged_txn.get_edge(staged_edge).unwrap().is_none());
    let revived = staged_txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    assert!(staged_txn.get_edge(revived).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_deleted_endpoint_triple_read_returns_none() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.delete_node(TxnNodeRef::Id(a)).unwrap();
    assert!(txn
        .get_edge_by_triple(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH")
        .unwrap()
        .is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_snapshot_read_does_not_see_later_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let txn = engine.begin_write_txn().unwrap();
    engine
        .upsert_node("Person", "later", UpsertNodeOptions::default())
        .unwrap();

    assert!(txn.get_node_by_key("Person", "later").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_same_key_insert_conflict_has_no_wal_or_id_leak() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_before = engine.next_node_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_node_id().unwrap(), next_before);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_conflict_does_not_publish_staged_new_label_tokens() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    engine
        .upsert_node("TxnStable", "conflict", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let left = txn
        .upsert_node("TxnNoLeakNode", "left", UpsertNodeOptions::default())
        .unwrap();
    let right = txn
        .upsert_node("TxnNoLeakNode", "right", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_edge(left, right, "TXN_NO_LEAK_EDGE", UpsertEdgeOptions::default())
        .unwrap();
    txn.upsert_node(
        "TxnStable",
        "conflict",
        UpsertNodeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();
    engine
        .upsert_node(
            "TxnStable",
            "conflict",
            UpsertNodeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_node_before = engine.next_node_id().unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert_eq!(engine.get_node_label_id("TxnNoLeakNode").unwrap(), None);
    assert_eq!(engine.get_edge_label_id("TXN_NO_LEAK_EDGE").unwrap(), None);
    assert!(engine
        .get_node_by_key("TxnNoLeakNode", "left")
        .unwrap()
        .is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_multi_label_key_conflict_and_patch_token_atomicity() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node(
        &["TxnConflictNewA", "TxnConflictNewB"],
        "same",
        UpsertNodeOptions::default(),
    )
    .unwrap();
    engine
        .upsert_node("TxnConflictNewB", "same", UpsertNodeOptions::default())
        .unwrap();
    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.get_node_label_id("TxnConflictNewA").unwrap(), None);

    let stable = engine
        .upsert_node("TxnPatchStable", "node", UpsertNodeOptions::default())
        .unwrap();

    let mut noop_txn = engine.begin_write_txn().unwrap();
    assert!(!noop_txn
        .add_node_label(TxnNodeRef::Id(stable), "TxnPatchStable")
        .unwrap());
    assert!(!noop_txn
        .remove_node_label(TxnNodeRef::Id(stable), "TxnPatchUnknownNoToken")
        .unwrap());
    let noop_result = noop_txn.commit().unwrap();
    assert!(noop_result.node_ids.is_empty());
    assert!(noop_result.edge_ids.is_empty());
    assert_eq!(
        engine.get_node_label_id("TxnPatchUnknownNoToken").unwrap(),
        None
    );

    let mut patch_txn = engine.begin_write_txn().unwrap();
    assert!(patch_txn
        .add_node_label(TxnNodeRef::Id(stable), "TxnPatchNoLeak")
        .unwrap());
    engine
        .upsert_node(
            "TxnPatchStable",
            "node",
            UpsertNodeOptions {
                weight: 2.0,
                ..Default::default()
            },
        )
        .unwrap();
    let err = patch_txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.get_node_label_id("TxnPatchNoLeak").unwrap(), None);
    assert_eq!(
        engine.get_node(stable).unwrap().unwrap().labels,
        vec!["TxnPatchStable".to_string()]
    );

    let mut rollback_txn = engine.begin_write_txn().unwrap();
    let solo = rollback_txn
        .upsert_node("TxnRollbackSolo", "solo", UpsertNodeOptions::default())
        .unwrap();
    let err = rollback_txn.remove_node_label(solo, "TxnRollbackSolo").unwrap_err();
    assert!(err.to_string().contains("last node label"));
    rollback_txn.rollback().unwrap();
    assert_eq!(engine.get_node_label_id("TxnRollbackSolo").unwrap(), None);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_local_create_connect_conflicts_when_node_key_appears_after_begin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let alice = txn
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = txn
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_edge(alice, bob, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_node_before = engine.next_node_id().unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_later_node_conflict_after_tentative_create_does_not_leak_counters() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node("Person", "tentative", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_node("Person", "conflict", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_node("Person", "conflict", UpsertNodeOptions::default())
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_node_before = engine.next_node_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert!(engine.get_node_by_key("Person", "tentative").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_later_edge_conflict_after_tentative_create_does_not_leak_counters() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
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

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    txn.upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(c), "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, c, "COLLABORATES_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_edge_before = engine.next_edge_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    assert!(engine.get_edge_by_triple(a, b, "FRIENDS_WITH").unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_key_delete_conflict_when_target_deleted_after_begin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.delete_node(TxnNodeRef::Key {
        label: "Person".to_string(),
        key: "alice".into(),
    })
    .unwrap();
    engine.delete_node(id).unwrap();
    let seq_before = engine.engine_seq_for_test();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_local_connect_conflicts_when_edge_triple_appears_after_begin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    let a_ref = txn
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b_ref = txn
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    txn.upsert_edge(a_ref, b_ref, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    let seq_before = engine.engine_seq_for_test();
    let next_node_before = engine.next_node_id().unwrap();
    let next_edge_before = engine.next_edge_id().unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert_eq!(engine.next_node_id().unwrap(), next_node_before);
    assert_eq!(engine.next_edge_id().unwrap(), next_edge_before);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_same_node_update_conflicts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node(
        "Person",
        "alice",
        UpsertNodeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert!((engine.get_node(id).unwrap().unwrap().weight - 3.0).abs() < f32::EPSILON);
    engine.close().unwrap();
}

#[test]
fn test_write_txn_same_triple_edge_conflicts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    engine.close().unwrap();
}

#[test]
fn test_write_txn_edge_triple_delete_conflict_when_deleted_after_begin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();
    let edge_id = engine
        .upsert_edge(a, b, "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.delete_edge(TxnEdgeRef::Triple {
        from: TxnNodeRef::Id(a),
        to: TxnNodeRef::Id(b),
        label: "FRIENDS_WITH".to_string(),
    })
    .unwrap();
    engine.delete_edge(edge_id).unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    engine.close().unwrap();
}

#[test]
fn test_write_txn_delete_update_race_conflicts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let id = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let mut txn = engine.begin_write_txn().unwrap();
    txn.delete_node(TxnNodeRef::Id(id)).unwrap();
    engine
        .upsert_node(
            "Person",
            "alice",
            UpsertNodeOptions {
                weight: 4.0,
                ..Default::default()
            },
        )
        .unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert!(engine.get_node(id).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_delete_node_conflicts_on_future_incident_edge() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.delete_node(TxnNodeRef::Id(a)).unwrap();
    let edge = engine
        .upsert_edge(
            a,
            b,
            "FRIENDS_WITH",
            UpsertEdgeOptions {
                valid_from: Some(i64::MAX / 2),
                ..Default::default()
            },
        )
        .unwrap();
    let seq_before = engine.engine_seq_for_test();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, EngineError::TxnConflict(_)));
    assert_eq!(engine.engine_seq_for_test(), seq_before);
    assert!(engine.get_edge(edge).unwrap().is_some());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_deleted_endpoint_rejected_and_deleted_edge_not_resurrected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let a = engine
        .upsert_node("Person", "a", UpsertNodeOptions::default())
        .unwrap();
    let b = engine
        .upsert_node("Person", "b", UpsertNodeOptions::default())
        .unwrap();

    let mut invalid = engine.begin_write_txn().unwrap();
    invalid.delete_node(TxnNodeRef::Id(a)).unwrap();
    assert!(matches!(
        invalid.upsert_edge(
            TxnNodeRef::Id(a),
            TxnNodeRef::Id(b),
            "FRIENDS_WITH",
            UpsertEdgeOptions::default()
        ),
        Err(EngineError::InvalidOperation(_))
    ));

    let mut txn = engine.begin_write_txn().unwrap();
    let edge = txn
        .upsert_edge(TxnNodeRef::Id(a), TxnNodeRef::Id(b), "FRIENDS_WITH", UpsertEdgeOptions::default())
        .unwrap();
    txn.delete_edge(edge.clone()).unwrap();
    txn.invalidate_edge(edge, 123).unwrap();
    let result = txn.commit().unwrap();
    assert_eq!(result.edge_ids.len(), 1);
    assert!(engine.get_edge(result.edge_ids[0]).unwrap().is_none());
    engine.close().unwrap();
}

#[test]
fn test_write_txn_mixed_implicit_explicit_independent_success_after_flush() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("testdb");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let base = engine
        .upsert_node("Person", "base", UpsertNodeOptions::default())
        .unwrap();
    engine.flush().unwrap();

    let mut txn = engine.begin_write_txn().unwrap();
    txn.upsert_node(
        "Person",
        "base",
        UpsertNodeOptions {
            weight: 2.0,
            ..Default::default()
        },
    )
    .unwrap();
    let other = engine
        .upsert_node("Person", "other", UpsertNodeOptions::default())
        .unwrap();
    assert!(txn.commit().is_ok());
    assert!((engine.get_node(base).unwrap().unwrap().weight - 2.0).abs() < f32::EPSILON);
    assert!(engine.get_node(other).unwrap().is_some());
    engine.close().unwrap();
}
