use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine,
    DbOptions,
    Direction,
    ExportOptions,
    NodeInput,
    PageRequest,
    PprOptions,
    PropValue,
    ScoringMode,
    WalSyncMode,
};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_db() -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        edge_uniqueness: true,
        ..DbOptions::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    (dir, engine)
}

fn bench_upsert_node(c: &mut Criterion) {
    c.bench_function("upsert_node", |b| {
        let (_dir, mut engine) = temp_db();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("node_{}", i);
            engine.upsert_node(1, &key, BTreeMap::new(), 1.0).unwrap();
            i += 1;
        });
    });
}

fn bench_upsert_node_with_props(c: &mut Criterion) {
    c.bench_function("upsert_node_with_props", |b| {
        let (_dir, mut engine) = temp_db();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("node_{}", i);
            let mut props = BTreeMap::new();
            props.insert("name".to_string(), PropValue::String(key.clone()));
            props.insert("score".to_string(), PropValue::Float(0.95));
            engine.upsert_node(1, &key, props, 1.0).unwrap();
            i += 1;
        });
    });
}

fn bench_upsert_edge(c: &mut Criterion) {
    c.bench_function("upsert_edge", |b| {
        let (_dir, mut engine) = temp_db();
        // Pre-create nodes
        for i in 0..1000 {
            engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let mut i = 0u64;
        b.iter(|| {
            let from = (i % 1000) + 1;
            let to = ((i + 1) % 1000) + 1;
            engine
                .upsert_edge(from, to, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            i += 1;
        });
    });
}

fn bench_get_node(c: &mut Criterion) {
    c.bench_function("get_node", |b| {
        let (_dir, mut engine) = temp_db();
        let mut ids = Vec::new();
        for i in 0..1000 {
            let id = engine
                .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            ids.push(id);
        }
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            engine.get_node(id).unwrap();
            i += 1;
        });
    });
}

fn bench_neighbors(c: &mut Criterion) {
    c.bench_function("neighbors_10_edges", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 1.0).unwrap();
        for i in 0..10 {
            let target = engine
                .upsert_node(1, &format!("t{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            engine
                .upsert_edge(hub, target, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });

    c.bench_function("neighbors_100_edges", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 1.0).unwrap();
        for i in 0..100 {
            let target = engine
                .upsert_node(1, &format!("t{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            engine
                .upsert_edge(hub, target, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
        }
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });
}

fn bench_neighbors_with_pit(c: &mut Criterion) {
    c.bench_function("neighbors_100_edges_pit", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 1.0).unwrap();
        let now = 1_000_000i64;
        for i in 0..100 {
            let target = engine
                .upsert_node(1, &format!("t{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            engine
                .upsert_edge(hub, target, 1, BTreeMap::new(), 1.0, Some(now - 10000), None)
                .unwrap();
        }
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, Some(now), None)
                .unwrap();
        });
    });
}

fn bench_find_nodes(c: &mut Criterion) {
    c.bench_function("find_nodes_1000", |b| {
        let (_dir, mut engine) = temp_db();
        for i in 0..1000 {
            let mut props = BTreeMap::new();
            let color = if i % 10 == 0 { "red" } else { "blue" };
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            engine
                .upsert_node(1, &format!("n{}", i), props, 1.0)
                .unwrap();
        }
        let val = PropValue::String("red".to_string());
        b.iter(|| {
            engine.find_nodes(1, "color", &val).unwrap();
        });
    });
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush");
    group.sample_size(20);
    group.bench_function("flush_100_nodes_20_edges", |b| {
        b.iter_batched(
            || {
                let (dir, mut engine) = temp_db();
                for i in 0..100 {
                    engine
                        .upsert_node(1, &format!("n{}", i), BTreeMap::new(), 1.0)
                        .unwrap();
                }
                for i in 0..20u64 {
                    engine
                        .upsert_edge(
                            (i % 100) + 1,
                            ((i + 1) % 100) + 1,
                            1,
                            BTreeMap::new(),
                            1.0,
                            None,
                            None,
                        )
                        .unwrap();
                }
                (dir, engine)
            },
            |(_dir, mut engine)| {
                engine.flush().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_batch_upsert_nodes(c: &mut Criterion) {
    c.bench_function("batch_upsert_100_nodes", |b| {
        let (_dir, mut engine) = temp_db();
        let mut batch_num = 0u64;
        b.iter(|| {
            let inputs: Vec<NodeInput> = (0..100)
                .map(|i| NodeInput {
                    type_id: 1,
                    key: format!("batch{}_{}", batch_num, i),
                    props: BTreeMap::new(),
                    weight: 1.0,
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
            batch_num += 1;
        });
    });
}

/// Build properties representative of typical graph nodes.
fn make_bench_props(i: u64) -> BTreeMap<String, PropValue> {
    let mut props = BTreeMap::new();
    props.insert(
        "content".to_string(),
        PropValue::String(format!(
            "Memory content for node {} with some additional context and detail to simulate real data",
            i
        )),
    );
    props.insert("confidence".to_string(), PropValue::Float(0.85 + (i as f64 % 10.0) / 100.0));
    props.insert(
        "source".to_string(),
        PropValue::String(format!("conversation_{}", i / 100)),
    );
    props
}

fn bench_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("compact");
    group.sample_size(10);

    // Clean compaction: non-overlapping segments, no tombstones.
    // Each flush produces unique node keys (s{seg}_n{i}), so IDs never overlap.
    group.bench_function("compact_clean_5x2000", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    compact_after_n_flushes: 0, // disable auto-compact for benchmark
                    ..DbOptions::default()
                };
                let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
                for seg in 0..5u64 {
                    for i in 0..2000u64 {
                        let global_i = seg * 2000 + i;
                        engine
                            .upsert_node(
                                1,
                                &format!("s{}_n{}", seg, i),
                                make_bench_props(global_i),
                                1.0,
                            )
                            .unwrap();
                    }
                    let base = seg * 2000 + 1;
                    for i in 0..400u64 {
                        engine
                            .upsert_edge(
                                base + (i % 2000),
                                base + ((i + 1) % 2000),
                                1,
                                BTreeMap::new(),
                                1.0,
                                None,
                                None,
                            )
                            .unwrap();
                    }
                    engine.flush().unwrap();
                }
                (dir, engine)
            },
            |(_dir, mut engine)| {
                engine.compact().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    // Overlapping compaction: same node keys across segments → overlapping IDs,
    // V3 planner resolves last-write-wins per node_id.
    group.bench_function("compact_overlapping_5x2000", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    compact_after_n_flushes: 0,
                    ..DbOptions::default()
                };
                let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
                for seg in 0..5u64 {
                    // Same keys each flush → updates → overlapping node IDs
                    for i in 0..2000u64 {
                        let global_i = seg * 2000 + i;
                        engine
                            .upsert_node(1, &format!("n{}", i), make_bench_props(global_i), 1.0)
                            .unwrap();
                    }
                    for i in 0..400u64 {
                        engine
                            .upsert_edge(
                                (i % 2000) + 1,
                                ((i + 1) % 2000) + 1,
                                1,
                                BTreeMap::new(),
                                1.0,
                                None,
                                None,
                            )
                            .unwrap();
                    }
                    engine.flush().unwrap();
                }
                (dir, engine)
            },
            |(_dir, mut engine)| {
                engine.compact().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    // Dirty compaction: segments with tombstones → V3 planner filters by tombstone set.
    group.bench_function("compact_dirty_5x2000", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    compact_after_n_flushes: 0,
                    ..DbOptions::default()
                };
                let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
                for seg in 0..5u64 {
                    for i in 0..2000u64 {
                        let global_i = seg * 2000 + i;
                        engine
                            .upsert_node(
                                1,
                                &format!("s{}_n{}", seg, i),
                                make_bench_props(global_i),
                                1.0,
                            )
                            .unwrap();
                    }
                    let base = seg * 2000 + 1;
                    for i in 0..400u64 {
                        engine
                            .upsert_edge(
                                base + (i % 2000),
                                base + ((i + 1) % 2000),
                                1,
                                BTreeMap::new(),
                                1.0,
                                None,
                                None,
                            )
                            .unwrap();
                    }
                    engine.flush().unwrap();
                }
                // Delete ~20% of nodes → creates tombstones
                for id in 1..=2000u64 {
                    engine.delete_node(id).unwrap();
                }
                engine.flush().unwrap();
                (dir, engine)
            },
            |(_dir, mut engine)| {
                engine.compact().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_group_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_commit");
    group.sample_size(20);

    // Single upsert: Immediate mode (~4ms/write due to fsync)
    group.bench_function("upsert_node_immediate", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            engine.upsert_node(1, &format!("imm_{}", i), BTreeMap::new(), 1.0).unwrap();
            i += 1;
        });
        engine.close().unwrap();
    });

    // Single upsert: GroupCommit mode (should be ~40-100μs/write)
    group.bench_function("upsert_node_group_commit", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            engine.upsert_node(1, &format!("gc_{}", i), BTreeMap::new(), 1.0).unwrap();
            i += 1;
        });
        engine.close().unwrap();
    });

    // Batch 100 nodes: Immediate mode
    group.bench_function("batch_100_nodes_immediate", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut batch_num = 0u64;
        b.iter(|| {
            let inputs: Vec<NodeInput> = (0..100)
                .map(|i| NodeInput {
                    type_id: 1,
                    key: format!("imm_b{}_{}", batch_num, i),
                    props: BTreeMap::new(),
                    weight: 1.0,
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
            batch_num += 1;
        });
        engine.close().unwrap();
    });

    // Batch 100 nodes: GroupCommit mode
    group.bench_function("batch_100_nodes_group_commit", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::GroupCommit {
                interval_ms: 10,
                soft_trigger_bytes: 4 * 1024 * 1024,
                hard_cap_bytes: 16 * 1024 * 1024,
            },
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut batch_num = 0u64;
        b.iter(|| {
            let inputs: Vec<NodeInput> = (0..100)
                .map(|i| NodeInput {
                    type_id: 1,
                    key: format!("gc_b{}_{}", batch_num, i),
                    props: BTreeMap::new(),
                    weight: 1.0,
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
            batch_num += 1;
        });
        engine.close().unwrap();
    });

    group.finish();
}

fn bench_advanced_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("advanced_queries");
    group.sample_size(10);

    group.bench_function("neighbors_2hop_100x10", |b| {
        let (_dir, mut engine) = temp_db();
        let root = engine.upsert_node(1, "root", BTreeMap::new(), 1.0).unwrap();
        for i in 0..100u64 {
            let mid = engine
                .upsert_node(1, &format!("mid_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            engine
                .upsert_edge(root, mid, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            for j in 0..10u64 {
                let leaf = engine
                    .upsert_node(1, &format!("leaf_{}_{}", i, j), BTreeMap::new(), 1.0)
                    .unwrap();
                engine
                    .upsert_edge(mid, leaf, 1, BTreeMap::new(), 1.0, None, None)
                    .unwrap();
            }
        }
        b.iter(|| {
            engine
                .neighbors_2hop(root, Direction::Outgoing, None, 0, None, None)
                .unwrap();
        });
    });

    group.bench_function("top_k_neighbors_weight_k20_1000", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = engine.upsert_node(1, "hub", BTreeMap::new(), 1.0).unwrap();
        for i in 0..1000u64 {
            let node = engine
                .upsert_node(1, &format!("tk_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
            let weight = 1.0 + (i % 100) as f32 / 10.0;
            engine
                .upsert_edge(hub, node, 1, BTreeMap::new(), weight, None, None)
                .unwrap();
        }
        b.iter(|| {
            engine
                .top_k_neighbors(hub, Direction::Outgoing, None, 20, ScoringMode::Weight, None)
                .unwrap();
        });
    });

    group.bench_function("find_nodes_by_time_range_10000", |b| {
        let (_dir, mut engine) = temp_db();
        let from_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 10_000;
        for i in 0..10_000u64 {
            engine
                .upsert_node(1, &format!("ts_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let to_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;
        b.iter(|| {
            engine.find_nodes_by_time_range(1, from_ms, to_ms).unwrap();
        });
    });

    group.bench_function("find_nodes_by_time_range_paged_10000_limit100", |b| {
        let (_dir, mut engine) = temp_db();
        let from_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 10_000;
        for i in 0..10_000u64 {
            engine
                .upsert_node(1, &format!("tsp_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
        }
        let to_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;
        let page = PageRequest {
            limit: Some(100),
            after: None,
        };
        b.iter(|| {
            engine
                .find_nodes_by_time_range_paged(1, from_ms, to_ms, &page)
                .unwrap();
        });
    });

    group.bench_function("personalized_pagerank_2000_nodes", |b| {
        let (_dir, mut engine) = temp_db();
        let node_ids: Vec<u64> = (0..2000u64)
            .map(|i| {
                engine
                    .upsert_node(1, &format!("ppr_{}", i), BTreeMap::new(), 1.0)
                    .unwrap()
            })
            .collect();

        for i in 0..2000usize {
            let from = node_ids[i];
            let to1 = node_ids[(i + 1) % node_ids.len()];
            let to2 = node_ids[(i + 7) % node_ids.len()];
            engine
                .upsert_edge(from, to1, 1, BTreeMap::new(), 1.0, None, None)
                .unwrap();
            engine
                .upsert_edge(from, to2, 1, BTreeMap::new(), 0.7, None, None)
                .unwrap();
        }

        let opts = PprOptions {
            max_results: Some(100),
            ..PprOptions::default()
        };
        let seed = node_ids[0];
        b.iter(|| {
            engine.personalized_pagerank(&[seed], &opts).unwrap();
        });
    });

    group.bench_function("export_adjacency_5000n_20000e", |b| {
        let (_dir, mut engine) = temp_db();
        let node_ids: Vec<u64> = (0..5000u64)
            .map(|i| {
                engine
                    .upsert_node(1, &format!("ex_{}", i), BTreeMap::new(), 1.0)
                    .unwrap()
            })
            .collect();

        for i in 0..20_000usize {
            let from = node_ids[i % node_ids.len()];
            let to = node_ids[(i * 13 + 7) % node_ids.len()];
            if from != to {
                engine
                    .upsert_edge(from, to, 1, BTreeMap::new(), 1.0, None, None)
                    .unwrap();
            }
        }

        let opts = ExportOptions::default();
        b.iter(|| {
            engine.export_adjacency(&opts).unwrap();
        });
    });

    group.finish();
}

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");
    group.sample_size(10);

    group.bench_function("open_close_wal_5000_nodes", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let mut engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                for batch in 0..10u64 {
                    let inputs: Vec<NodeInput> = (0..500u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("wal_{}", batch * 500 + i),
                            props: BTreeMap::new(),
                            weight: 1.0,
                        })
                        .collect();
                    engine.batch_upsert_nodes(&inputs).unwrap();
                }
                engine.close().unwrap();
                dir
            },
            |dir| {
                let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                engine.close().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("open_close_segment_3x2000", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let mut engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                for seg in 0..3u64 {
                    for i in 0..2000u64 {
                        engine
                            .upsert_node(
                                1,
                                &format!("seg{}_{}", seg, i),
                                BTreeMap::new(),
                                1.0,
                            )
                            .unwrap();
                    }
                    engine.flush().unwrap();
                }
                engine.close().unwrap();
                dir
            },
            |dir| {
                let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                engine.close().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_upsert_node,
    bench_upsert_node_with_props,
    bench_upsert_edge,
    bench_get_node,
    bench_neighbors,
    bench_neighbors_with_pit,
    bench_find_nodes,
    bench_flush,
    bench_batch_upsert_nodes,
    bench_compact,
    bench_group_commit,
    bench_advanced_queries,
    bench_recovery,
);
criterion_main!(benches);
