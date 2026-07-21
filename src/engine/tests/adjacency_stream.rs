fn prepared_adjacency_test_hint(needed: usize) -> AdjacencyPullBoundaryHint {
    AdjacencyPullBoundaryHint {
        page_target_met: false,
        oriented_candidates_needed: needed,
    }
}

fn prepared_adjacency_test_query(filter: NormalizedEdgeFilter) -> NormalizedEdgeQuery {
    NormalizedEdgeQuery {
        label_id: None,
        ids: Vec::new(),
        from_ids: Vec::new(),
        to_ids: Vec::new(),
        endpoint_ids: Vec::new(),
        filter,
        allow_full_scan: true,
        page: PageRequest::default(),
        warnings: Vec::new(),
    }
}

fn prepared_adjacency_test_edge(
    id: u64,
    from: u64,
    to: u64,
    label_id: u32,
) -> EdgeRecord {
    EdgeRecord {
        id,
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
    }
}

fn prepared_adjacency_ready(
    view: &Arc<ReadView>,
    direction: Direction,
    labels: Option<&[u32]>,
) -> PreparedAdjacencyPullSource {
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::ValidAt { epoch_ms: 1 });
    match view
        .prepare_adjacency_pull_source(direction, labels, None, query, None)
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        PreparedAdjacencyPullPreparation::Empty => panic!("expected a prepared source"),
        PreparedAdjacencyPullPreparation::MutableActiveMemtable => {
            panic!("expected an empty active memtable")
        }
    }
}

fn prepared_adjacency_collect(
    source: &mut PreparedAdjacencyPullSource,
    scheduled: usize,
) -> (Vec<GraphRowOrientedEdge>, Vec<AdjacencyPullResult>) {
    let mut output = Vec::new();
    let mut pulls = Vec::new();
    for _ in 0..100_000 {
        let pull = source
            .pull(scheduled, prepared_adjacency_test_hint(usize::MAX))
            .unwrap();
        assert_eq!(
            pull.physical_scan_units,
            pull.cursor_seek_units
                + pull.adj_index_entries_scanned
                + pull.raw_postings_scanned
        );
        assert!(pull.physical_scan_units <= scheduled as u64);
        assert!(pull.oriented_metadata.len() <= scheduled);
        let has_more = pull.has_more;
        output.extend_from_slice(&pull.oriented_metadata);
        pulls.push(pull);
        if !has_more {
            return (output, pulls);
        }
    }
    panic!("prepared adjacency source did not terminate");
}

#[test]
fn adjacency_stream_schedule_obeys_selection_growth_and_all_ceilings() {
    assert_eq!(
        graph_row_adjacency_pull_scheduled_chunk_size(None, 11, 8_192, 8_192),
        11
    );
    assert_eq!(
        graph_row_adjacency_pull_scheduled_chunk_size(Some(11), 11, 8_192, 8_192),
        22
    );
    assert_eq!(
        graph_row_adjacency_pull_scheduled_chunk_size(Some(3_000), 11, 8_192, 8_192),
        4_096
    );
    assert_eq!(
        graph_row_adjacency_pull_scheduled_chunk_size(None, 100, 7, 8_192),
        7
    );
    assert_eq!(
        graph_row_adjacency_pull_scheduled_chunk_size(None, 100, 8_192, 0),
        1
    );
    with_graph_row_test_chunk_override(3, || {
        assert_eq!(
            graph_row_adjacency_pull_scheduled_chunk_size(None, 100, 8_192, 8_192),
            3
        );
        assert_eq!(
            graph_row_adjacency_pull_scheduled_chunk_size(None, 100, 2, 8_192),
            2
        );
    });
}

#[test]
fn adjacency_stream_newest_edge_cache_is_exact_fifo_without_hit_refresh() {
    let mut cache = AdjacencyNewestEdgeCache::prepared(3);
    assert_eq!(cache.storage_capacities(), (0, 0));
    assert_eq!(cache.retained_units(), (0, 0));
    cache.insert(1, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    cache.insert(2, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    cache.insert(3, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.retained_units(), (3, 3));
    assert!(cache.storage_capacities().0 < ADJACENCY_NEWEST_EDGE_CACHE_CAP);
    assert!(cache.storage_capacities().1 < ADJACENCY_NEWEST_EDGE_CACHE_CAP);
    assert!(cache.get(1).is_some());
    cache.insert(2, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    cache.insert(4, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    assert!(cache.get(1).is_none(), "a hit or update must not refresh FIFO order");
    assert!(cache.get(2).is_some());
    assert!(cache.get(3).is_some());
    assert!(cache.get(4).is_some());
}

#[test]
fn adjacency_stream_newest_edge_cache_retains_exact_locked_capacity() {
    let mut cache = AdjacencyNewestEdgeCache::prepared(ADJACENCY_NEWEST_EDGE_CACHE_CAP);
    for edge_id in 0..ADJACENCY_NEWEST_EDGE_CACHE_CAP as u64 {
        cache.insert(edge_id, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    }
    assert_eq!(cache.len(), ADJACENCY_NEWEST_EDGE_CACHE_CAP);
    cache.insert(
        ADJACENCY_NEWEST_EDGE_CACHE_CAP as u64,
        AdjacencyNewestEdgeOpinion::MissingOrTombstoned,
    );
    assert_eq!(cache.len(), ADJACENCY_NEWEST_EDGE_CACHE_CAP);
    assert!(cache.get(0).is_none());
    assert!(cache
        .get(ADJACENCY_NEWEST_EDGE_CACHE_CAP as u64)
        .is_some());
}

#[test]
fn adjacency_stream_segment_merge_orientation_labels_and_terminal_lookahead() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjNode", "adj-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjNode", "adj-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjNode", "adj-c", &[], 1.0);
    let label_a = engine.ensure_edge_label("ADJ_A").unwrap();
    let label_b = engine.ensure_edge_label("ADJ_B").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(101, a, b, label_a)),
    )
    .unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(102, b, c, label_b)),
    )
    .unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(103, b, b, label_a)),
    )
    .unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();

    let mut outgoing = prepared_adjacency_ready(
        &view,
        Direction::Outgoing,
        Some(&[label_b, label_a, label_b]),
    );
    assert_eq!(outgoing.cursor_count(), 1);
    let (outgoing_edges, outgoing_pulls) = prepared_adjacency_collect(&mut outgoing, 4_096);
    assert_eq!(
        outgoing_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(a, b, 101), (b, b, 103), (b, c, 102)]
    );
    assert!(outgoing_pulls.last().is_some_and(|pull| !pull.has_more));
    assert!(outgoing_pulls
        .iter()
        .all(|pull| pull.followups.is_empty()));

    let mut incoming = prepared_adjacency_ready(&view, Direction::Incoming, None);
    let (incoming_edges, _) = prepared_adjacency_collect(&mut incoming, 4_096);
    assert_eq!(
        incoming_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(b, a, 101), (b, b, 103), (c, b, 102)]
    );

    let mut both = prepared_adjacency_ready(&view, Direction::Both, None);
    assert_eq!(both.cursor_count(), 2);
    let (both_edges, both_pulls) = prepared_adjacency_collect(&mut both, 4_096);
    assert_eq!(
        both_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(a, b, 101), (b, b, 103), (b, c, 102), (b, a, 101), (c, b, 102)]
    );
    assert_eq!(
        both_pulls
            .iter()
            .map(|pull| pull.shadowed_or_stale_postings)
            .sum::<u64>(),
        0,
        "canonical incoming self-loop suppression is not stale work"
    );

    let mut final_posting = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label_b]));
    let mut saw_final_posting_pull = false;
    let mut saw_later_terminal_pull = false;
    for _ in 0..32 {
        let pull = final_posting
            .pull(1, prepared_adjacency_test_hint(usize::MAX))
            .unwrap();
        if pull.raw_postings_scanned == 1 {
            saw_final_posting_pull = true;
            assert!(pull.has_more, "final posting cannot perform free lookahead");
        } else if saw_final_posting_pull && !pull.has_more {
            saw_later_terminal_pull = true;
            assert_eq!(pull.oriented_metadata.len(), 0);
            assert_eq!(pull.physical_scan_units, 0);
            assert_eq!(pull.adj_index_entries_scanned, 0);
            break;
        }
    }
    assert!(saw_final_posting_pull && saw_later_terminal_pull);
}

#[test]
fn adjacency_stream_immutable_refill_debt_is_resumable_and_blocks_early_output() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjImm", "adj-imm-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjImm", "adj-imm-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_IMM").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(201, a, b, label)),
    )
    .unwrap();
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    assert!(view.memtable.retained_edge_record_keys_is_empty());
    assert_eq!(view.immutable_epochs.len(), 1);
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    assert_eq!(source.cursor_count(), 1);

    for _ in 0..MEMTABLE_REFILL_SEEK_UNITS {
        let pull = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
        assert!(pull.oriented_metadata.is_empty());
        assert!(pull.pending_completed_through_owner.is_none());
        assert_eq!(pull.cursor_seek_units, 1);
        assert_eq!(pull.physical_scan_units, 1);
    }
    let seeded = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
    assert!(seeded.oriented_metadata.is_empty());
    assert_eq!(seeded.adj_index_entries_scanned, 1);
    for _ in 0..MEMTABLE_REFILL_SEEK_UNITS {
        let pull = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
        assert!(pull.oriented_metadata.is_empty());
        assert_eq!(pull.cursor_seek_units, 1);
    }
    let posting = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(posting.raw_postings_scanned, 1);
    assert_eq!(posting.oriented_metadata.len(), 1);
    assert_eq!(posting.oriented_metadata[0].meta.id, 201);
    assert!(posting.partial_owner_pull);

    let (_, remaining) = prepared_adjacency_collect(&mut source, 64);
    assert!(remaining.iter().any(|pull| pull.pending_completed_through_owner == Some(a)));
    assert!(remaining.last().is_some_and(|pull| !pull.has_more));
}

#[test]
fn adjacency_stream_immutable_refill_debt_retires_in_checked_bulk() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjBulk", "adj-bulk-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjBulk", "adj-bulk-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_BULK").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(251, a, b, label)),
    )
    .unwrap();
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();

    let mut below = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    let first = below.pull(63, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(first.cursor_seek_units, 63);
    assert_eq!(first.physical_scan_units, 63);
    assert!(matches!(
        below.cursors[0].source,
        AdjacencyPhysicalSource::Immutable {
            refill_seek_debt: 1,
            ..
        }
    ));
    let residual = below.pull(1, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(residual.cursor_seek_units, 1);
    assert_eq!(residual.adj_index_entries_scanned, 0);

    let mut exact = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    let exact_pull = exact.pull(64, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(exact_pull.cursor_seek_units, 64);
    assert_eq!(exact_pull.adj_index_entries_scanned, 0);

    let mut above = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    let above_pull = above.pull(65, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(above_pull.cursor_seek_units, 64);
    assert_eq!(above_pull.adj_index_entries_scanned, 1);
    assert_eq!(above_pull.physical_scan_units, 65);
    assert!(above.initialized);
}

#[test]
fn adjacency_stream_preparation_normalization_active_exclusion_and_known_empty() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjPrep", "adj-prep-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjPrep", "adj-prep-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_PREP").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(301, a, b, label)),
    )
    .unwrap();
    let active_view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::AlwaysTrue);
    assert!(matches!(
        active_view
            .prepare_adjacency_pull_source(
                Direction::Outgoing,
                Some(&[label]),
                None,
                query.clone(),
                None,
            )
            .unwrap(),
        PreparedAdjacencyPullPreparation::MutableActiveMemtable
    ));
    assert!(matches!(
        active_view
            .prepare_adjacency_pull_source(
                Direction::Outgoing,
                Some(&[]),
                None,
                query,
                None,
            )
            .unwrap(),
        PreparedAdjacencyPullPreparation::Empty
    ));
}

#[test]
fn adjacency_stream_newest_layer_suppresses_same_id_move_relabel_and_tombstone() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjMove", "adj-move-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjMove", "adj-move-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjMove", "adj-move-c", &[], 1.0);
    let old_label = engine.ensure_edge_label("ADJ_OLD").unwrap();
    let new_label = engine.ensure_edge_label("ADJ_NEW").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(401, b, c, old_label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let mut moved = prepared_adjacency_test_edge(401, a, b, new_label);
    moved.updated_at = 2;
    moved.weight = 7.0;
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(moved)).unwrap();
    engine.flush().unwrap();
    let moved_view = engine.published_read_view_for_test();

    let mut old = prepared_adjacency_ready(&moved_view, Direction::Outgoing, Some(&[old_label]));
    let (old_edges, old_pulls) = prepared_adjacency_collect(&mut old, 4_096);
    assert!(old_edges.is_empty());
    assert!(old_pulls
        .iter()
        .map(|pull| pull.shadowed_or_stale_postings)
        .sum::<u64>()
        >= 1);

    let mut new = prepared_adjacency_ready(&moved_view, Direction::Outgoing, Some(&[new_label]));
    let (new_edges, _) = prepared_adjacency_collect(&mut new, 4_096);
    assert_eq!(
        new_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id, edge.meta.weight))
            .collect::<Vec<_>>(),
        vec![(a, b, 401, 7.0)]
    );

    write_internal_wal_op(
        &engine,
        &WalOp::DeleteEdge {
            id: 401,
            deleted_at: 3,
        },
    )
    .unwrap();
    engine.flush().unwrap();
    let tombstone_view = engine.published_read_view_for_test();
    let mut tombstoned =
        prepared_adjacency_ready(&tombstone_view, Direction::Outgoing, None);
    let (tombstoned_edges, pulls) = prepared_adjacency_collect(&mut tombstoned, 4_096);
    assert!(tombstoned_edges.is_empty());
    assert!(pulls
        .iter()
        .map(|pull| pull.shadowed_or_stale_postings)
        .sum::<u64>()
        >= 2);

    let mut resurrected = prepared_adjacency_test_edge(401, c, a, old_label);
    resurrected.updated_at = 4;
    resurrected.weight = 9.0;
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(resurrected)).unwrap();
    engine.flush().unwrap();
    let resurrected_view = engine.published_read_view_for_test();
    let mut resurrection =
        prepared_adjacency_ready(&resurrected_view, Direction::Outgoing, Some(&[old_label]));
    let (resurrected_edges, _) = prepared_adjacency_collect(&mut resurrection, 4_096);
    assert_eq!(
        resurrected_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id, edge.meta.weight))
            .collect::<Vec<_>>(),
        vec![(c, a, 401, 9.0)]
    );
}

#[test]
fn adjacency_stream_fair_initialization_waits_for_every_layer_and_direction() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjFair", "adj-fair-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjFair", "adj-fair-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjFair", "adj-fair-c", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_FAIR").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(501, b, c, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(502, a, c, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Both, Some(&[label]));
    assert_eq!(source.cursor_count(), 4);
    assert!(source
        .cursors
        .iter()
        .all(|cursor| cursor.state == AdjacencyCursorState::Unseeded));
    assert_eq!(source.newest_edge_cache.len(), 0);

    for expected_next in [1usize, 2, 3, 0] {
        let pull = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
        assert!(pull.oriented_metadata.is_empty());
        assert!(pull.pending_completed_through_owner.is_none());
        assert_eq!(pull.physical_scan_units, 1);
        assert_eq!(source.initialization_next, expected_next);
    }
    assert!(source.initialized);
    let (edges, _) = prepared_adjacency_collect(&mut source, 1);
    assert_eq!(edges.first().map(|edge| edge.logical_from), Some(a));
    assert!(edges
        .windows(2)
        .all(|pair| pair[0].logical_from <= pair[1].logical_from));
}

#[test]
fn adjacency_stream_watermark_distinguishes_completed_then_partial_owner() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjWater", "adj-water-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjWater", "adj-water-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjWater", "adj-water-c", &[], 1.0);
    let d = insert_query_node(&engine, "AdjWater", "adj-water-d", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_WATER").unwrap();
    for (edge_id, from, to) in [(601, a, b), (602, a, c), (603, a, d), (604, b, c)] {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, from, to, label)),
        )
        .unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));

    let first = source
        .pull(3, prepared_adjacency_test_hint(usize::MAX))
        .unwrap();
    assert_eq!(first.oriented_metadata.len(), 2);
    assert!(first.partial_owner_pull);
    assert!(first.pending_completed_through_owner.is_none());

    let second = source
        .pull(3, prepared_adjacency_test_hint(usize::MAX))
        .unwrap();
    assert_eq!(second.oriented_metadata.len(), 2);
    assert_eq!(second.pending_last_completed_owner, Some(a));
    assert_eq!(second.pending_completed_owner_groups, 1);
    assert_eq!(second.pending_completed_through_owner, None);
    assert!(second.partial_owner_pull);

    let mut boundary_source =
        prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    let boundary = boundary_source
        .pull(
            4_096,
            AdjacencyPullBoundaryHint {
                page_target_met: true,
                oriented_candidates_needed: 0,
            },
        )
        .unwrap();
    assert_eq!(boundary.pending_completed_through_owner, Some(a));
    assert_eq!(boundary.pending_last_completed_owner, Some(a));
    assert_eq!(boundary.pending_completed_owner_groups, 1);
    assert!(!boundary.partial_owner_pull);
    assert!(boundary.has_more);
    assert_eq!(boundary.oriented_metadata.len(), 3);
}

#[test]
fn adjacency_stream_reuses_property_temporal_and_endpoint_verification() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjVerify", "adj-verify-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjVerify", "adj-verify-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_VERIFY").unwrap();
    let mut edge = prepared_adjacency_test_edge(701, a, b, label);
    edge.props.insert("score".into(), PropValue::Int(7));
    edge.valid_from = 10;
    edge.valid_to = 20;
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::And(vec![
        NormalizedEdgeFilter::ValidAt { epoch_ms: 15 },
        NormalizedEdgeFilter::PropertyEquals {
            key: "score".into(),
            value: PropValue::Int(7),
        },
    ]));
    let mut source = match view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected verified adjacency source"),
    };
    engine.reset_query_execution_counters_for_test();
    let (verified, _) = prepared_adjacency_collect(&mut source, 4_096);
    assert_eq!(verified.iter().map(|edge| edge.meta.id).collect::<Vec<_>>(), vec![701]);
    assert!(source.endpoint_visibility_cache.retained_units() <= 2);
    let counters = engine.query_execution_counter_snapshot_for_test();
    assert_eq!(counters.adjacency_newest_opinion_resolution_calls, 1);
    assert_eq!(counters.adjacency_newest_opinion_resolution_ids, 1);
    assert_eq!(counters.edge_verifier_metadata_lookup_calls, 0);
    assert_eq!(counters.edge_verifier_metadata_lookup_ids, 0);

    let rejected_query = prepared_adjacency_test_query(NormalizedEdgeFilter::PropertyEquals {
        key: "score".into(),
        value: PropValue::Int(8),
    });
    let mut property_rejected = match view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            rejected_query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected property-rejection adjacency source"),
    };
    assert!(prepared_adjacency_collect(&mut property_rejected, 4_096)
        .0
        .is_empty());

    write_internal_wal_op(
        &engine,
        &WalOp::DeleteNode {
            id: b,
            deleted_at: 21,
        },
    )
    .unwrap();
    engine.flush().unwrap();
    let deleted_view = engine.published_read_view_for_test();
    let endpoint_query =
        prepared_adjacency_test_query(NormalizedEdgeFilter::ValidAt { epoch_ms: 15 });
    let mut deleted_endpoint = match deleted_view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            endpoint_query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected endpoint-verification adjacency source"),
    };
    let (deleted, pulls) = prepared_adjacency_collect(&mut deleted_endpoint, 4_096);
    assert!(deleted.is_empty());
    assert!(pulls
        .iter()
        .any(|pull| pull.pending_completed_through_owner == Some(a)));

    let out_of_time_query =
        prepared_adjacency_test_query(NormalizedEdgeFilter::ValidAt { epoch_ms: 25 });
    let mut out_of_time = match deleted_view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            out_of_time_query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected temporal adjacency source"),
    };
    assert!(prepared_adjacency_collect(&mut out_of_time, 4_096)
        .0
        .is_empty());
}

#[test]
fn adjacency_stream_resolved_verifier_matches_wrapper_order_and_duplicates() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjResolved", "adj-resolved-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjResolved", "adj-resolved-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_RESOLVED").unwrap();
    for (edge_id, score) in [(751, 7), (752, 8)] {
        let mut edge = prepared_adjacency_test_edge(edge_id, a, b, label);
        edge.props.insert("score".into(), PropValue::Int(score));
        edge.valid_from = 10;
        edge.valid_to = 20;
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::And(vec![
        NormalizedEdgeFilter::ValidAt { epoch_ms: 15 },
        NormalizedEdgeFilter::PropertyExists {
            key: "score".into(),
        },
    ]));
    let ids = [752, 751, 752];
    let resolved = view
        .sources()
        .find_edge_metadata(&ids)
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let mut wrapper_cache = EdgeEndpointVisibilityCache::default();
    let mut wrapper = Vec::new();
    let _ = view.verify_edge_candidate_chunk(
        &ids,
        &query,
        None,
        &mut wrapper_cache,
        &mut wrapper,
        usize::MAX,
    )
    .unwrap();
    let mut resolved_cache = EdgeEndpointVisibilityCache::default();
    let mut direct = Vec::new();
    let _ = view.verify_resolved_edge_metadata_chunk(
        &resolved,
        &query,
        None,
        &mut resolved_cache,
        &mut direct,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(direct, wrapper);
    assert_eq!(direct.iter().map(|meta| meta.id).collect::<Vec<_>>(), ids);
}

#[test]
fn adjacency_stream_post_snapshot_active_write_cannot_expand_cursor_universe() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjPinned", "adj-pinned-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjPinned", "adj-pinned-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_PINNED").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(801, a, b, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let pinned = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&pinned, Direction::Outgoing, Some(&[label]));
    assert_eq!(source.cursor_count(), 1);

    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(802, b, a, label)),
    )
    .unwrap();
    let (edges, _) = prepared_adjacency_collect(&mut source, 4_096);
    assert_eq!(edges.iter().map(|edge| edge.meta.id).collect::<Vec<_>>(), vec![801]);
    assert_eq!(source.cursor_count(), 1);
}

#[test]
fn adjacency_stream_same_id_larger_move_relabel_and_metadata_update_are_exact() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjMatrix", "adj-matrix-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjMatrix", "adj-matrix-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjMatrix", "adj-matrix-c", &[], 1.0);
    let label_a = engine.ensure_edge_label("ADJ_MATRIX_A").unwrap();
    let label_b = engine.ensure_edge_label("ADJ_MATRIX_B").unwrap();
    for edge in [
        prepared_adjacency_test_edge(901, a, b, label_a),
        prepared_adjacency_test_edge(902, a, c, label_a),
        prepared_adjacency_test_edge(903, b, c, label_a),
    ] {
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();

    let mut moved_larger = prepared_adjacency_test_edge(901, c, b, label_a);
    moved_larger.updated_at = 2;
    let mut relabeled = prepared_adjacency_test_edge(902, a, c, label_b);
    relabeled.updated_at = 2;
    let mut metadata_only = prepared_adjacency_test_edge(903, b, c, label_a);
    metadata_only.updated_at = 2;
    metadata_only.weight = 8.0;
    for edge in [moved_larger, relabeled, metadata_only] {
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();

    let mut label_a_source =
        prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label_a]));
    let (label_a_edges, pulls) = prepared_adjacency_collect(&mut label_a_source, 4_096);
    assert_eq!(
        label_a_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id, edge.meta.weight))
            .collect::<Vec<_>>(),
        vec![(b, c, 903, 8.0), (c, b, 901, 1.0)]
    );
    assert!(pulls
        .iter()
        .map(|pull| pull.shadowed_or_stale_postings)
        .sum::<u64>()
        >= 3);

    let mut label_b_source =
        prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label_b]));
    let (label_b_edges, _) = prepared_adjacency_collect(&mut label_b_source, 4_096);
    assert_eq!(
        label_b_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(a, c, 902)]
    );
}

#[test]
fn adjacency_stream_newer_immutable_wins_over_older_immutable_and_segment() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjLayers", "adj-layers-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjLayers", "adj-layers-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjLayers", "adj-layers-c", &[], 1.0);
    let old_label = engine.ensure_edge_label("ADJ_LAYERS_OLD").unwrap();
    let new_label = engine.ensure_edge_label("ADJ_LAYERS_NEW").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(1_001, b, c, old_label)),
    )
    .unwrap();
    engine.flush().unwrap();

    let mut first_immutable = prepared_adjacency_test_edge(1_001, a, b, new_label);
    first_immutable.updated_at = 2;
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(first_immutable)).unwrap();
    engine.freeze_memtable().unwrap();
    let mut newest_immutable = prepared_adjacency_test_edge(1_001, c, a, old_label);
    newest_immutable.updated_at = 3;
    write_internal_wal_op(&engine, &WalOp::UpsertEdge(newest_immutable)).unwrap();
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    assert_eq!(view.immutable_epochs.len(), 2);

    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, None);
    assert_eq!(source.cursor_count(), 3);
    let (edges, pulls) = prepared_adjacency_collect(&mut source, 4_096);
    assert_eq!(
        edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id, edge.meta.label_id))
            .collect::<Vec<_>>(),
        vec![(c, a, 1_001, old_label)]
    );
    assert!(pulls
        .iter()
        .map(|pull| pull.shadowed_or_stale_postings)
        .sum::<u64>()
        >= 2);
}

#[test]
fn adjacency_stream_inclusive_seek_is_lazy_and_reopen_equivalent() {
    let (dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjSeek", "adj-seek-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjSeek", "adj-seek-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjSeek", "adj-seek-c", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_SEEK").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(1_101, a, b, label)),
    )
    .unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(1_102, c, a, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::ValidAt { epoch_ms: 1 });
    let mut source = match view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            Some(c),
            query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected inclusive-seek source"),
    };
    assert!(source
        .cursors
        .iter()
        .all(|cursor| cursor.state == AdjacencyCursorState::Unseeded));
    assert_eq!(source.newest_edge_cache.len(), 0);
    let first = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
    assert!(first.oriented_metadata.is_empty());
    assert_eq!(first.cursor_seek_units, 1);
    let (seek_edges, _) = prepared_adjacency_collect(&mut source, 4_096);
    assert_eq!(
        seek_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(c, 1_102)]
    );

    drop(source);
    drop(view);
    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let reopened_view = reopened.published_read_view_for_test();
    let mut reopened_source =
        prepared_adjacency_ready(&reopened_view, Direction::Outgoing, Some(&[label]));
    let (reopened_edges, _) = prepared_adjacency_collect(&mut reopened_source, 4_096);
    assert_eq!(
        reopened_edges
            .iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>(),
        vec![(a, b, 1_101), (c, a, 1_102)]
    );
}

#[test]
fn adjacency_stream_enforces_4096_output_and_physical_caps_on_parallel_edges() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjCap", "adj-cap-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjCap", "adj-cap-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_CAP").unwrap();
    for edge_id in 20_000..24_097 {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, a, b, label)),
        )
        .unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));

    let seed = source.pull(1, prepared_adjacency_test_hint(1)).unwrap();
    assert!(seed.oriented_metadata.is_empty());
    assert_eq!(seed.physical_scan_units, 1);
    let capped = source
        .pull(4_096, prepared_adjacency_test_hint(usize::MAX))
        .unwrap();
    assert_eq!(capped.oriented_metadata.len(), 4_096);
    assert_eq!(capped.physical_scan_units, 4_096);
    assert_eq!(capped.raw_postings_scanned, 4_096);
    assert!(capped.partial_owner_pull);
    assert!(capped.pending_completed_through_owner.is_none());
    assert_eq!(
        capped
            .oriented_metadata
            .iter()
            .map(|edge| edge.meta.id)
            .collect::<NodeIdSet>()
            .len(),
        4_096
    );
    let terminal = source
        .pull(4_096, prepared_adjacency_test_hint(usize::MAX))
        .unwrap();
    assert_eq!(terminal.oriented_metadata.len(), 1);
    assert_eq!(terminal.pending_completed_through_owner, Some(a));
    assert!(!terminal.has_more);
}

#[test]
fn adjacency_stream_giant_immutable_owner_uses_one_bounded_scratch_and_truthful_zero() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjScratch", "adj-scratch-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjScratch", "adj-scratch-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_SCRATCH").unwrap();
    for edge_id in 30_000..30_130 {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, a, b, label)),
        )
        .unwrap();
    }
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::AlwaysFalse);
    let mut source = match view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            query,
            None,
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected giant immutable adjacency source"),
    };
    let mut pulls = 0usize;
    let mut completed = false;
    while pulls < 1_000 {
        let pull = source
            .pull(128, prepared_adjacency_test_hint(1))
            .unwrap();
        pulls += 1;
        assert!(pull.oriented_metadata.is_empty());
        assert!(pull.physical_scan_units <= 128);
        assert!(source.posting_scratch.capacity() <= 128);
        if pull.pending_completed_through_owner == Some(a) {
            completed = true;
        }
        if !pull.has_more {
            break;
        }
    }
    assert!(completed);
    assert!(pulls >= 4, "seek debt and giant owner must span pulls");
    assert!(!source.has_more());
}

#[test]
fn adjacency_stream_odd_schedule_bounds_every_reusable_batch_vector() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjOdd", "adj-odd-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjOdd", "adj-odd-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_ODD").unwrap();
    for edge_id in 40_000..40_600 {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, a, b, label)),
        )
        .unwrap();
    }
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));

    let (edges, _) = prepared_adjacency_collect(&mut source, 1_001);
    assert_eq!(edges.len(), 600);
    assert!(source.posting_scratch.capacity() <= 1_001);
    assert!(source.posting_scratch.capacity() > QUERY_VERIFY_CHUNK);
    for capacity in [
        source.raw_batch.capacity(),
        source.batch_opinions.capacity(),
        source.newest_lookup_ids.capacity(),
        source.newest_lookup_opinions.capacity(),
        source.pending_candidates.capacity(),
        source.verifier_metadata.capacity(),
        source.verified_batch.capacity(),
    ] {
        assert!(
            capacity <= QUERY_VERIFY_CHUNK,
            "scratch capacity {capacity} exceeded verification batch ceiling"
        );
        assert!(
            capacity <= 1_001,
            "scratch capacity {capacity} exceeded odd schedule"
        );
    }
}

#[test]
fn adjacency_stream_retains_and_drains_precharged_immutable_postings() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjBuffer", "adj-buffer-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjBuffer", "adj-buffer-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_BUFFER").unwrap();
    for edge_id in 45_000..45_300 {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, a, b, label)),
        )
        .unwrap();
    }
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));

    let prepaid = source.pull(129, prepared_adjacency_test_hint(1)).unwrap();
    assert_eq!(prepaid.cursor_seek_units, 128);
    assert_eq!(prepaid.adj_index_entries_scanned, 1);
    assert!(prepaid.oriented_metadata.is_empty());
    let mut refill_counters = AdjacencyPullCounters::default();
    assert!(source
        .advance_current_cursor(300, 0, &mut refill_counters)
        .unwrap());
    assert_eq!(refill_counters.raw_postings_scanned, 300);
    assert_eq!(source.posting_scratch.len(), 300);
    assert_eq!(source.posting_scratch.capacity(), 300);
    assert!(source.immutable_posting_buffer.is_some());

    for expected_unread in [200usize, 100, 0] {
        let pull = source.pull(100, prepared_adjacency_test_hint(usize::MAX)).unwrap();
        assert_eq!(pull.oriented_metadata.len(), 100);
        assert_eq!(pull.physical_scan_units, 0);
        assert_eq!(pull.raw_postings_scanned, 0);
        assert!(pull.has_more);
        assert!(pull.partial_owner_pull);
        assert!(pull.pending_completed_through_owner.is_none());
        assert_eq!(
            source
                .immutable_posting_buffer
                .map(|buffer| source.posting_scratch.len() - buffer.next_unread)
                .unwrap_or(0),
            expected_unread
        );
    }
    assert!(source.immutable_posting_buffer.is_none());
    assert!(source.posting_scratch.is_empty());
    assert!(matches!(
        source.cursors[0].source,
        AdjacencyPhysicalSource::Immutable {
            refill_seek_debt: MEMTABLE_REFILL_SEEK_UNITS,
            ..
        }
    ));
}

#[test]
fn adjacency_stream_later_buffered_verifier_error_is_terminal_without_recharge() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjBufferErr", "adj-buffer-err-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjBufferErr", "adj-buffer-err-b", &[], 1.0);
    engine.flush().unwrap();
    let label = engine.ensure_edge_label("ADJ_BUFFER_ERR").unwrap();
    for edge_id in 46_000..46_600 {
        write_internal_wal_op(
            &engine,
            &WalOp::UpsertEdge(prepared_adjacency_test_edge(edge_id, a, b, label)),
        )
        .unwrap();
    }
    engine.freeze_memtable().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    source.verifier_batches_before_failure = Some(1);

    let error = match source.pull(600, prepared_adjacency_test_hint(usize::MAX)) {
        Ok(_) => panic!("later buffered verifier batch must fail"),
        Err(error) => error,
    };
    assert!(matches!(error, EngineError::CorruptRecord(message) if message.contains("buffered adjacency verifier")));
    assert!(source.advanced);
    assert!(source.failed);
    assert!(source.posting_scratch.capacity() <= 600);
    assert!(matches!(
        source.pull(600, prepared_adjacency_test_hint(usize::MAX)),
        Err(EngineError::InvalidOperation(message))
            if message.contains("terminal after an earlier error")
    ));
}

#[test]
fn adjacency_stream_batch_opinions_survive_persistent_fifo_eviction() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjEvict", "adj-evict-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjEvict", "adj-evict-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjEvict", "adj-evict-c", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_EVICT").unwrap();
    let edge_one = prepared_adjacency_test_edge(1, a, b, label);
    let edge_four = prepared_adjacency_test_edge(4, a, c, label);
    for edge in [edge_one.clone(), edge_four.clone()] {
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    source.newest_edge_cache = AdjacencyNewestEdgeCache::prepared(3);
    source.newest_edge_cache.insert(
        1,
        AdjacencyNewestEdgeOpinion::Live {
            layer: AdjacencySourceLayer::Segment(0),
            meta: EdgeMetadataCandidate::from_edge(&edge_one),
        },
    );
    source
        .newest_edge_cache
        .insert(2, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    source
        .newest_edge_cache
        .insert(3, AdjacencyNewestEdgeOpinion::MissingOrTombstoned);
    source.prepare_batch_scratch(4);
    source.raw_batch.extend([
        AdjacencyRawPosting {
            layer: AdjacencySourceLayer::Segment(0),
            direction: Direction::Outgoing,
            owner_id: a,
            entry: crate::memtable::AdjEntry {
                edge_id: 1,
                label_id: label,
                neighbor_id: b,
                weight: 1.0,
                valid_from: 0,
                valid_to: i64::MAX,
            },
        },
        AdjacencyRawPosting {
            layer: AdjacencySourceLayer::Segment(0),
            direction: Direction::Outgoing,
            owner_id: a,
            entry: crate::memtable::AdjEntry {
                edge_id: 4,
                label_id: label,
                neighbor_id: c,
                weight: 1.0,
                valid_from: 0,
                valid_to: i64::MAX,
            },
        },
    ]);
    let mut output = Vec::new();
    let mut counters = AdjacencyPullCounters::default();
    source.flush_raw_batch(&mut output, &mut counters).unwrap();

    assert_eq!(output.iter().map(|edge| edge.meta.id).collect::<Vec<_>>(), vec![1, 4]);
    assert!(source.newest_edge_cache.get(1).is_none());
    assert!(source.raw_batch.is_empty());
    assert!(source.batch_opinions.is_empty());
}

#[test]
fn adjacency_stream_first_group_error_is_terminal_after_advance() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjErr", "adj-err-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjErr", "adj-err-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_ERR").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(50_001, a, b, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    source.cursors[0].fail_next_group_step = true;

    assert!(matches!(
        source.pull(64, prepared_adjacency_test_hint(1)),
        Err(EngineError::CorruptRecord(_))
    ));
    assert!(source.advanced);
    assert!(source.failed);
    assert!(matches!(
        source.pull(64, prepared_adjacency_test_hint(1)),
        Err(EngineError::InvalidOperation(message))
            if message.contains("terminal after an earlier error")
    ));
}

#[test]
fn adjacency_stream_future_posting_error_is_lazy_and_never_advances_a_watermark() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjLazyErr", "adj-lazy-err-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjLazyErr", "adj-lazy-err-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjLazyErr", "adj-lazy-err-c", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_LAZY_ERR").unwrap();
    for edge in [
        prepared_adjacency_test_edge(50_101, a, c, label),
        prepared_adjacency_test_edge(50_102, b, c, label),
    ] {
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let mut source = prepared_adjacency_ready(&view, Direction::Outgoing, Some(&[label]));
    source.cursors[0].fail_posting_for_owner = Some(b);

    let first = source
        .pull(
            64,
            AdjacencyPullBoundaryHint {
                page_target_met: true,
                oriented_candidates_needed: 0,
            },
        )
        .unwrap();
    assert_eq!(first.pending_completed_through_owner, Some(a));
    assert_eq!(first.pending_last_completed_owner, Some(a));
    assert_eq!(first.oriented_metadata.len(), 1);
    assert!(first.has_more);

    assert!(matches!(
        source.pull(64, prepared_adjacency_test_hint(1)),
        Err(EngineError::CorruptRecord(_))
    ));
    assert_eq!(source.current_owner, Some(b));
    assert!(source.failed);
}

#[test]
fn adjacency_stream_applies_endpoint_prune_policy_cutoffs() {
    let (_dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjPolicy", "adj-policy-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjPolicy", "adj-policy-b", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_POLICY").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(60_001, a, b, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    let view = engine.published_read_view_for_test();
    let query = prepared_adjacency_test_query(NormalizedEdgeFilter::AlwaysTrue);
    let cutoffs = PrecomputedPruneCutoffs {
        policies: vec![(None, Some(1.0), None)],
    };
    let mut source = match view
        .prepare_adjacency_pull_source(
            Direction::Outgoing,
            Some(&[label]),
            None,
            query,
            Some(cutoffs),
        )
        .unwrap()
    {
        PreparedAdjacencyPullPreparation::Ready(source) => *source,
        _ => panic!("expected policy-filtered adjacency source"),
    };
    assert!(prepared_adjacency_collect(&mut source, 4_096).0.is_empty());
}

#[test]
fn adjacency_stream_is_equivalent_before_after_compaction_and_reopen() {
    let (dir, engine) = query_test_engine();
    let a = insert_query_node(&engine, "AdjCompact", "adj-compact-a", &[], 1.0);
    let b = insert_query_node(&engine, "AdjCompact", "adj-compact-b", &[], 1.0);
    let c = insert_query_node(&engine, "AdjCompact", "adj-compact-c", &[], 1.0);
    let label = engine.ensure_edge_label("ADJ_COMPACT").unwrap();
    write_internal_wal_op(
        &engine,
        &WalOp::UpsertEdge(prepared_adjacency_test_edge(70_001, a, b, label)),
    )
    .unwrap();
    engine.flush().unwrap();
    for edge in [
        prepared_adjacency_test_edge(70_001, c, a, label),
        prepared_adjacency_test_edge(70_002, b, c, label),
    ] {
        write_internal_wal_op(&engine, &WalOp::UpsertEdge(edge)).unwrap();
    }
    engine.flush().unwrap();

    let collect = |view: &Arc<ReadView>| {
        let mut source = prepared_adjacency_ready(view, Direction::Both, Some(&[label]));
        prepared_adjacency_collect(&mut source, 4_096)
            .0
            .into_iter()
            .map(|edge| (edge.logical_from, edge.logical_to, edge.meta.id))
            .collect::<Vec<_>>()
    };
    let before = collect(&engine.published_read_view_for_test());
    engine.compact().unwrap().unwrap();
    let after = collect(&engine.published_read_view_for_test());
    assert_eq!(after, before);

    engine.close().unwrap();
    let reopened = DatabaseEngine::open(&dir.path().join("db"), &DbOptions::default()).unwrap();
    let reopened_edges = collect(&reopened.published_read_view_for_test());
    assert_eq!(reopened_edges, before);
}

#[test]
fn adjacency_stream_boundary_hint_helper_is_not_accidentally_zero() {
    assert_eq!(prepared_adjacency_test_hint(7).oriented_candidates_needed, 7);
}
