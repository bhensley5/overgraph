use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeInput, ExportOptions, NodeInput, PageRequest,
    PprOptions, PropValue, PrunePolicy, ScoringMode, WalSyncMode,
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
        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        engine.batch_upsert_nodes(&inputs).unwrap();
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
        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            engine.get_node(id).unwrap();
            i += 1;
        });
    });

    c.bench_function("get_node_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            engine.get_node(id).unwrap();
            i += 1;
        });
    });
}

/// Build a hub-and-spokes graph: one hub node with `n` outgoing edges to target nodes.
fn build_hub_graph(engine: &mut DatabaseEngine, n: usize) -> u64 {
    let mut inputs: Vec<NodeInput> = vec![NodeInput {
        type_id: 1,
        key: "hub".to_string(),
        props: BTreeMap::new(),
        weight: 1.0,
    }];
    for i in 0..n {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("t{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        });
    }
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    let hub = ids[0];
    let edges: Vec<EdgeInput> = ids[1..]
        .iter()
        .map(|&target| EdgeInput {
            from: hub,
            to: target,
            type_id: 1,
            props: BTreeMap::new(),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();
    hub
}

fn bench_neighbors(c: &mut Criterion) {
    c.bench_function("neighbors_10_edges", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 10);
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });

    c.bench_function("neighbors_10_edges_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 10);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });

    c.bench_function("neighbors_100_edges", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });

    c.bench_function("neighbors_100_edges_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, None, None)
                .unwrap();
        });
    });
}

fn bench_neighbors_with_pit(c: &mut Criterion) {
    let now = 1_000_000i64;

    let build_pit_graph = |engine: &mut DatabaseEngine| -> u64 {
        let mut inputs: Vec<NodeInput> = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        for i in 0..100 {
            inputs.push(NodeInput {
                type_id: 1,
                key: format!("t{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            });
        }
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        let hub = ids[0];
        let edges: Vec<EdgeInput> = ids[1..]
            .iter()
            .map(|&target| EdgeInput {
                from: hub,
                to: target,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: Some(now - 10000),
                valid_to: None,
            })
            .collect();
        engine.batch_upsert_edges(&edges).unwrap();
        hub
    };

    c.bench_function("neighbors_100_edges_pit", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_pit_graph(&mut engine);
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, Some(now), None)
                .unwrap();
        });
    });

    c.bench_function("neighbors_100_edges_pit_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_pit_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .neighbors(hub, Direction::Outgoing, None, usize::MAX, Some(now), None)
                .unwrap();
        });
    });
}

fn bench_find_nodes(c: &mut Criterion) {
    let build_find_graph = |engine: &mut DatabaseEngine| {
        let inputs: Vec<NodeInput> = (0..1000)
            .map(|i| {
                let color = if i % 10 == 0 { "red" } else { "blue" };
                let mut props = BTreeMap::new();
                props.insert("color".to_string(), PropValue::String(color.to_string()));
                NodeInput {
                    type_id: 1,
                    key: format!("n{}", i),
                    props,
                    weight: 1.0,
                }
            })
            .collect();
        engine.batch_upsert_nodes(&inputs).unwrap();
    };

    c.bench_function("find_nodes_1000", |b| {
        let (_dir, mut engine) = temp_db();
        build_find_graph(&mut engine);
        let val = PropValue::String("red".to_string());
        b.iter(|| {
            engine.find_nodes(1, "color", &val).unwrap();
        });
    });

    c.bench_function("find_nodes_1000_segment", |b| {
        let (_dir, mut engine) = temp_db();
        build_find_graph(&mut engine);
        engine.flush().unwrap();
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
                let node_inputs: Vec<NodeInput> = (0..100)
                    .map(|i| NodeInput {
                        type_id: 1,
                        key: format!("n{}", i),
                        props: BTreeMap::new(),
                        weight: 1.0,
                    })
                    .collect();
                let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
                let edge_inputs: Vec<EdgeInput> = (0..20)
                    .map(|i| EdgeInput {
                        from: ids[i % 100],
                        to: ids[(i + 1) % 100],
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    })
                    .collect();
                engine.batch_upsert_edges(&edge_inputs).unwrap();
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
    props.insert(
        "confidence".to_string(),
        PropValue::Float(0.85 + (i as f64 % 10.0) / 100.0),
    );
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
                    compact_after_n_flushes: 0,
                    ..DbOptions::default()
                };
                let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
                for seg in 0..5u64 {
                    let node_inputs: Vec<NodeInput> = (0..2000u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("s{}_n{}", seg, i),
                            props: make_bench_props(seg * 2000 + i),
                            weight: 1.0,
                        })
                        .collect();
                    let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
                    let edge_inputs: Vec<EdgeInput> = (0..400)
                        .map(|i| EdgeInput {
                            from: ids[i % 2000],
                            to: ids[(i + 1) % 2000],
                            type_id: 1,
                            props: BTreeMap::new(),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect();
                    engine.batch_upsert_edges(&edge_inputs).unwrap();
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

    // Same logical workload as compact_clean_5x2000, but a no-op prune policy
    // keeps the fast path ineligible so we get an apples-to-apples V3 baseline.
    group.bench_function("compact_clean_forced_v3_5x2000", |b| {
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
                    let node_inputs: Vec<NodeInput> = (0..2000u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("s{}_n{}", seg, i),
                            props: make_bench_props(seg * 2000 + i),
                            weight: 1.0,
                        })
                        .collect();
                    let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
                    let edge_inputs: Vec<EdgeInput> = (0..400)
                        .map(|i| EdgeInput {
                            from: ids[i % 2000],
                            to: ids[(i + 1) % 2000],
                            type_id: 1,
                            props: BTreeMap::new(),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect();
                    engine.batch_upsert_edges(&edge_inputs).unwrap();
                    engine.flush().unwrap();
                }
                engine
                    .set_prune_policy(
                        "noop-fast-merge-blocker",
                        PrunePolicy {
                            max_age_ms: None,
                            max_weight: Some(0.0),
                            type_id: Some(u32::MAX),
                        },
                    )
                    .unwrap();
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
                    let node_inputs: Vec<NodeInput> = (0..2000u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("n{}", i),
                            props: make_bench_props(seg * 2000 + i),
                            weight: 1.0,
                        })
                        .collect();
                    let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
                    let edge_inputs: Vec<EdgeInput> = (0..400)
                        .map(|i| EdgeInput {
                            from: ids[i % 2000],
                            to: ids[(i + 1) % 2000],
                            type_id: 1,
                            props: BTreeMap::new(),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect();
                    engine.batch_upsert_edges(&edge_inputs).unwrap();
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
                    let node_inputs: Vec<NodeInput> = (0..2000u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("s{}_n{}", seg, i),
                            props: make_bench_props(seg * 2000 + i),
                            weight: 1.0,
                        })
                        .collect();
                    let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
                    let edge_inputs: Vec<EdgeInput> = (0..400)
                        .map(|i| EdgeInput {
                            from: ids[i % 2000],
                            to: ids[(i + 1) % 2000],
                            type_id: 1,
                            props: BTreeMap::new(),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect();
                    engine.batch_upsert_edges(&edge_inputs).unwrap();
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
            engine
                .upsert_node(1, &format!("imm_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
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
            engine
                .upsert_node(1, &format!("gc_{}", i), BTreeMap::new(), 1.0)
                .unwrap();
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

/// Build 100 hubs each with 10 outgoing edges. Returns hub IDs.
fn build_multi_hub_graph(engine: &mut DatabaseEngine) -> Vec<u64> {
    let node_inputs: Vec<NodeInput> = (0..1100)
        .map(|i| NodeInput {
            type_id: 1,
            key: if i < 100 {
                format!("hub{}", i)
            } else {
                format!("t{}_{}", (i - 100) / 10, (i - 100) % 10)
            },
            props: BTreeMap::new(),
            weight: 1.0,
        })
        .collect();
    let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
    let hub_ids: Vec<u64> = ids[..100].to_vec();
    let mut edge_inputs = Vec::with_capacity(1000);
    for h in 0..100 {
        for i in 0..10 {
            edge_inputs.push(EdgeInput {
                from: ids[h],
                to: ids[100 + h * 10 + i],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
    }
    engine.batch_upsert_edges(&edge_inputs).unwrap();
    hub_ids
}

fn bench_degree(c: &mut Criterion) {
    c.bench_function("degree_fanout_100", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        b.iter(|| {
            engine.degree(hub, Direction::Outgoing, None, None).unwrap();
        });
    });

    c.bench_function("degree_fanout_100_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        engine.flush().unwrap();
        b.iter(|| {
            engine.degree(hub, Direction::Outgoing, None, None).unwrap();
        });
    });

    c.bench_function("degree_fanout_100_segment_type_filtered", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .degree(hub, Direction::Outgoing, Some(&[1]), None)
                .unwrap();
        });
    });

    c.bench_function("degree_fanout_100_segment_temporal", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_hub_graph(&mut engine, 100);
        engine.flush().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        b.iter(|| {
            engine
                .degree(hub, Direction::Outgoing, None, Some(now))
                .unwrap();
        });
    });

    c.bench_function("degrees_batch_100_nodes", |b| {
        let (_dir, mut engine) = temp_db();
        let hub_ids = build_multi_hub_graph(&mut engine);
        b.iter(|| {
            engine
                .degrees(&hub_ids, Direction::Outgoing, None, None)
                .unwrap();
        });
    });

    c.bench_function("degrees_batch_100_nodes_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub_ids = build_multi_hub_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .degrees(&hub_ids, Direction::Outgoing, None, None)
                .unwrap();
        });
    });
}

fn bench_degree_scalar_loop(c: &mut Criterion) {
    c.bench_function("degree_scalar_loop_100_nodes_10_edges", |b| {
        let (_dir, mut engine) = temp_db();
        let hub_ids = build_multi_hub_graph(&mut engine);
        b.iter(|| {
            for &hid in &hub_ids {
                engine.degree(hid, Direction::Outgoing, None, None).unwrap();
            }
        });
    });

    c.bench_function("degree_scalar_loop_100_nodes_10_edges_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub_ids = build_multi_hub_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            for &hid in &hub_ids {
                engine.degree(hid, Direction::Outgoing, None, None).unwrap();
            }
        });
    });
}

fn bench_advanced_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("advanced_queries");
    group.sample_size(10);

    let build_2hop_graph = |engine: &mut DatabaseEngine| -> u64 {
        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "root".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        for i in 0..100u64 {
            node_inputs.push(NodeInput {
                type_id: 1,
                key: format!("mid_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            });
            for j in 0..10u64 {
                node_inputs.push(NodeInput {
                    type_id: 1,
                    key: format!("leaf_{}_{}", i, j),
                    props: BTreeMap::new(),
                    weight: 1.0,
                });
            }
        }
        let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let root = ids[0];
        let mut edge_inputs = Vec::new();
        for i in 0..100usize {
            let mid = ids[1 + i * 11];
            edge_inputs.push(EdgeInput {
                from: root,
                to: mid,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for j in 0..10usize {
                let leaf = ids[1 + i * 11 + 1 + j];
                edge_inputs.push(EdgeInput {
                    from: mid,
                    to: leaf,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        root
    };

    group.bench_function("traverse_depth_2_100x10", |b| {
        let (_dir, mut engine) = temp_db();
        let root = build_2hop_graph(&mut engine);
        b.iter(|| {
            engine
                .traverse(
                    root,
                    2,
                    2,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("traverse_depth_2_100x10_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let root = build_2hop_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .traverse(
                    root,
                    2,
                    2,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    let build_layered_traversal_graph =
        |engine: &mut DatabaseEngine| -> (u64, usize, usize, usize) {
            let level1 = 24usize;
            let level2 = 4usize;
            let level3 = 4usize;

            let mut node_inputs = vec![NodeInput {
                type_id: 1,
                key: "root".to_string(),
                props: BTreeMap::new(),
                weight: 1.0,
            }];
            for i in 0..level1 {
                node_inputs.push(NodeInput {
                    type_id: 11,
                    key: format!("lvl1_{}", i),
                    props: BTreeMap::new(),
                    weight: 1.0,
                });
            }
            for i in 0..level1 {
                for j in 0..level2 {
                    node_inputs.push(NodeInput {
                        type_id: if (i + j) % 2 == 0 { 2 } else { 3 },
                        key: format!("lvl2_{}_{}", i, j),
                        props: BTreeMap::new(),
                        weight: 1.0,
                    });
                }
            }
            for i in 0..level1 {
                for j in 0..level2 {
                    for k in 0..level3 {
                        node_inputs.push(NodeInput {
                            type_id: if (i + j + k) % 2 == 0 { 2 } else { 3 },
                            key: format!("lvl3_{}_{}_{}", i, j, k),
                            props: BTreeMap::new(),
                            weight: 1.0,
                        });
                    }
                }
            }

            let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
            let root = ids[0];
            let level1_offset = 1usize;
            let level2_offset = level1_offset + level1;
            let level3_offset = level2_offset + level1 * level2;
            let mut edge_inputs = Vec::new();
            for i in 0..level1 {
                let lvl1 = ids[level1_offset + i];
                edge_inputs.push(EdgeInput {
                    from: root,
                    to: lvl1,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
                for j in 0..level2 {
                    let lvl2_idx = i * level2 + j;
                    let lvl2 = ids[level2_offset + lvl2_idx];
                    edge_inputs.push(EdgeInput {
                        from: lvl1,
                        to: lvl2,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    });
                    for k in 0..level3 {
                        let lvl3_idx = lvl2_idx * level3 + k;
                        edge_inputs.push(EdgeInput {
                            from: lvl2,
                            to: ids[level3_offset + lvl3_idx],
                            type_id: 1,
                            props: BTreeMap::new(),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        });
                    }
                }
            }
            engine.batch_upsert_edges(&edge_inputs).unwrap();
            (root, level1, level2, level3)
        };

    group.bench_function("traverse_depth_1_to_3_24x4x4", |b| {
        let (_dir, mut engine) = temp_db();
        let (root, _, _, _) = build_layered_traversal_graph(&mut engine);
        b.iter(|| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("traverse_depth_1_to_3_24x4x4_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let (root, _, _, _) = build_layered_traversal_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    let filtered_types = [2u32];

    group.bench_function("traverse_depth_1_to_3_filtered_type2_24x4x4", |b| {
        let (_dir, mut engine) = temp_db();
        let (root, _, _, _) = build_layered_traversal_graph(&mut engine);
        b.iter(|| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    Some(&filtered_types),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("traverse_depth_1_to_3_filtered_type2_24x4x4_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let (root, _, _, _) = build_layered_traversal_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .traverse(
                    root,
                    1,
                    3,
                    Direction::Outgoing,
                    None,
                    Some(&filtered_types),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    let build_topk_graph = |engine: &mut DatabaseEngine| -> u64 {
        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
        }];
        for i in 0..1000u64 {
            node_inputs.push(NodeInput {
                type_id: 1,
                key: format!("tk_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            });
        }
        let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let hub = ids[0];
        let edge_inputs: Vec<EdgeInput> = (0..1000)
            .map(|i| EdgeInput {
                from: hub,
                to: ids[1 + i],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0 + (i as u64 % 100) as f32 / 10.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        hub
    };

    group.bench_function("top_k_neighbors_weight_k20_1000", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_topk_graph(&mut engine);
        b.iter(|| {
            engine
                .top_k_neighbors(
                    hub,
                    Direction::Outgoing,
                    None,
                    20,
                    ScoringMode::Weight,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("top_k_neighbors_weight_k20_1000_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let hub = build_topk_graph(&mut engine);
        engine.flush().unwrap();
        b.iter(|| {
            engine
                .top_k_neighbors(
                    hub,
                    Direction::Outgoing,
                    None,
                    20,
                    ScoringMode::Weight,
                    None,
                )
                .unwrap();
        });
    });

    let build_time_range_graph = |engine: &mut DatabaseEngine, prefix: &str| -> (i64, i64) {
        let from_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 10_000;
        let inputs: Vec<NodeInput> = (0..10_000u64)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("{}_{}", prefix, i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        engine.batch_upsert_nodes(&inputs).unwrap();
        let to_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;
        (from_ms, to_ms)
    };

    group.bench_function("find_nodes_by_time_range_10000", |b| {
        let (_dir, mut engine) = temp_db();
        let (from_ms, to_ms) = build_time_range_graph(&mut engine, "ts");
        b.iter(|| {
            engine.find_nodes_by_time_range(1, from_ms, to_ms).unwrap();
        });
    });

    group.bench_function("find_nodes_by_time_range_10000_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let (from_ms, to_ms) = build_time_range_graph(&mut engine, "ts");
        engine.flush().unwrap();
        b.iter(|| {
            engine.find_nodes_by_time_range(1, from_ms, to_ms).unwrap();
        });
    });

    group.bench_function("find_nodes_by_time_range_paged_10000_limit100", |b| {
        let (_dir, mut engine) = temp_db();
        let (from_ms, to_ms) = build_time_range_graph(&mut engine, "tsp");
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

    group.bench_function(
        "find_nodes_by_time_range_paged_10000_limit100_segment",
        |b| {
            let (_dir, mut engine) = temp_db();
            let (from_ms, to_ms) = build_time_range_graph(&mut engine, "tsp");
            engine.flush().unwrap();
            let page = PageRequest {
                limit: Some(100),
                after: None,
            };
            b.iter(|| {
                engine
                    .find_nodes_by_time_range_paged(1, from_ms, to_ms, &page)
                    .unwrap();
            });
        },
    );

    let build_ppr_graph = |engine: &mut DatabaseEngine| -> Vec<u64> {
        let node_inputs: Vec<NodeInput> = (0..2000u64)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("ppr_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let edge_inputs: Vec<EdgeInput> = (0..2000usize)
            .flat_map(|i| {
                [
                    EdgeInput {
                        from: node_ids[i],
                        to: node_ids[(i + 1) % 2000],
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    },
                    EdgeInput {
                        from: node_ids[i],
                        to: node_ids[(i + 7) % 2000],
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 0.7,
                        valid_from: None,
                        valid_to: None,
                    },
                ]
            })
            .collect();
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        node_ids
    };

    group.bench_function("personalized_pagerank_2000_nodes", |b| {
        let (_dir, mut engine) = temp_db();
        let node_ids = build_ppr_graph(&mut engine);
        let opts = PprOptions {
            max_results: Some(100),
            ..PprOptions::default()
        };
        let seed = node_ids[0];
        b.iter(|| {
            engine.personalized_pagerank(&[seed], &opts).unwrap();
        });
    });

    group.bench_function("personalized_pagerank_2000_nodes_segment", |b| {
        let (_dir, mut engine) = temp_db();
        let node_ids = build_ppr_graph(&mut engine);
        engine.flush().unwrap();
        let opts = PprOptions {
            max_results: Some(100),
            ..PprOptions::default()
        };
        let seed = node_ids[0];
        b.iter(|| {
            engine.personalized_pagerank(&[seed], &opts).unwrap();
        });
    });

    let build_export_graph = |engine: &mut DatabaseEngine| -> Vec<u64> {
        let node_inputs: Vec<NodeInput> = (0..5000u64)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("ex_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let node_ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let edge_inputs: Vec<EdgeInput> = (0..20_000usize)
            .filter_map(|i| {
                let from = node_ids[i % 5000];
                let to = node_ids[(i * 13 + 7) % 5000];
                if from != to {
                    Some(EdgeInput {
                        from,
                        to,
                        type_id: 1,
                        props: BTreeMap::new(),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    })
                } else {
                    None
                }
            })
            .collect();
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        node_ids
    };

    group.bench_function("export_adjacency_5000n_20000e", |b| {
        let (_dir, mut engine) = temp_db();
        build_export_graph(&mut engine);
        let opts = ExportOptions::default();
        b.iter(|| {
            engine.export_adjacency(&opts).unwrap();
        });
    });

    group.bench_function("export_adjacency_5000n_20000e_segment", |b| {
        let (_dir, mut engine) = temp_db();
        build_export_graph(&mut engine);
        engine.flush().unwrap();
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
                    let inputs: Vec<NodeInput> = (0..2000u64)
                        .map(|i| NodeInput {
                            type_id: 1,
                            key: format!("seg{}_{}", seg, i),
                            props: BTreeMap::new(),
                            weight: 1.0,
                        })
                        .collect();
                    engine.batch_upsert_nodes(&inputs).unwrap();
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

/// Build a deterministic connected graph with `n` nodes and ring+offset edges.
/// Each node `i` has outgoing edges to `(i+1)%n` and `(i+7)%n`, giving avg degree ~4.
/// Edge weights are deterministic: 1.0 + (i % 10) as f32 / 10.0.
fn build_ring_graph(n: usize) -> (tempfile::TempDir, DatabaseEngine, Vec<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    };
    let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

    let inputs: Vec<NodeInput> = (0..n)
        .map(|i| NodeInput {
            type_id: 1,
            key: format!("sp_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
        })
        .collect();
    let node_ids = engine.batch_upsert_nodes(&inputs).unwrap();

    let edges: Vec<EdgeInput> = (0..n)
        .flat_map(|i| {
            let from = node_ids[i];
            let weight = 1.0 + (i % 10) as f32 / 10.0;
            vec![
                EdgeInput {
                    from,
                    to: node_ids[(i + 1) % n],
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight,
                    valid_from: None,
                    valid_to: None,
                },
                EdgeInput {
                    from,
                    to: node_ids[(i + 7) % n],
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight,
                    valid_from: None,
                    valid_to: None,
                },
            ]
        })
        .collect();
    engine.batch_upsert_edges(&edges).unwrap();

    (dir, engine, node_ids)
}

fn build_ring_graph_flushed(n: usize) -> (tempfile::TempDir, DatabaseEngine, Vec<u64>) {
    let (dir, mut engine, node_ids) = build_ring_graph(n);
    engine.flush().unwrap();
    (dir, engine, node_ids)
}

fn bench_shortest_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path");
    group.sample_size(10);

    // BFS on 10K-node graph
    group.bench_function("bfs_10k", |b| {
        let (_dir, engine, ids) = build_ring_graph(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(from, to, Direction::Outgoing, None, None, None, None, None)
                .unwrap();
        });
    });

    group.bench_function("bfs_10k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(from, to, Direction::Outgoing, None, None, None, None, None)
                .unwrap();
        });
    });

    // BFS on 100K-node graph
    group.bench_function("bfs_100k", |b| {
        let (_dir, engine, ids) = build_ring_graph(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(from, to, Direction::Outgoing, None, None, None, None, None)
                .unwrap();
        });
    });

    group.bench_function("bfs_100k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(from, to, Direction::Outgoing, None, None, None, None, None)
                .unwrap();
        });
    });

    // Dijkstra (weight_field="weight") on 10K-node graph
    group.bench_function("dijkstra_weight_10k", |b| {
        let (_dir, engine, ids) = build_ring_graph(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("dijkstra_weight_10k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("dijkstra_weight_bounded_10k", |b| {
        let (_dir, engine, ids) = build_ring_graph(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        let max_depth = Some((ids.len() / 2) as u32);
        b.iter(|| {
            engine
                .shortest_path(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    max_depth,
                    None,
                )
                .unwrap();
        });
    });

    // Dijkstra on 100K-node graph
    group.bench_function("dijkstra_weight_100k", |b| {
        let (_dir, engine, ids) = build_ring_graph(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    group.bench_function("dijkstra_weight_100k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .shortest_path(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                )
                .unwrap();
        });
    });

    // is_connected on 10K-node graph
    group.bench_function("is_connected_10k", |b| {
        let (_dir, engine, ids) = build_ring_graph(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .is_connected(from, to, Direction::Outgoing, None, None, None)
                .unwrap();
        });
    });

    group.bench_function("is_connected_10k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(10_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .is_connected(from, to, Direction::Outgoing, None, None, None)
                .unwrap();
        });
    });

    // is_connected on 100K-node graph
    group.bench_function("is_connected_100k", |b| {
        let (_dir, engine, ids) = build_ring_graph(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .is_connected(from, to, Direction::Outgoing, None, None, None)
                .unwrap();
        });
    });

    group.bench_function("is_connected_100k_segment", |b| {
        let (_dir, engine, ids) = build_ring_graph_flushed(100_000);
        let from = ids[0];
        let to = ids[ids.len() / 2];
        b.iter(|| {
            engine
                .is_connected(from, to, Direction::Outgoing, None, None, None)
                .unwrap();
        });
    });

    // all_shortest_paths on diamond-heavy graph (100 nodes, many equal-cost paths)
    group.bench_function("all_shortest_paths_diamond_100", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let inputs: Vec<NodeInput> = (0..100)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("d_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();

        let mut diamond_edges = Vec::new();
        for &l1 in &ids[1..20] {
            diamond_edges.push(EdgeInput {
                from: ids[0],
                to: l1,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for &l2 in &ids[20..40] {
                diamond_edges.push(EdgeInput {
                    from: l1,
                    to: l2,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        for &l2 in &ids[20..40] {
            diamond_edges.push(EdgeInput {
                from: l2,
                to: ids[99],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
        engine.batch_upsert_edges(&diamond_edges).unwrap();

        let from = ids[0];
        let to = ids[99];
        b.iter(|| {
            engine
                .all_shortest_paths(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(50),
                )
                .unwrap();
        });
    });

    group.bench_function("all_shortest_paths_diamond_100_segment", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let inputs: Vec<NodeInput> = (0..100)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("d_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();

        let mut diamond_edges = Vec::new();
        for &l1 in &ids[1..20] {
            diamond_edges.push(EdgeInput {
                from: ids[0],
                to: l1,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for &l2 in &ids[20..40] {
                diamond_edges.push(EdgeInput {
                    from: l1,
                    to: l2,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        for &l2 in &ids[20..40] {
            diamond_edges.push(EdgeInput {
                from: l2,
                to: ids[99],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
        engine.batch_upsert_edges(&diamond_edges).unwrap();
        engine.flush().unwrap();

        let from = ids[0];
        let to = ids[99];
        b.iter(|| {
            engine
                .all_shortest_paths(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(50),
                )
                .unwrap();
        });
    });

    group.bench_function("all_shortest_paths_weighted_diamond_100", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let inputs: Vec<NodeInput> = (0..100)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("wd_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();

        let mut diamond_edges = Vec::new();
        for &l1 in &ids[1..20] {
            diamond_edges.push(EdgeInput {
                from: ids[0],
                to: l1,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for &l2 in &ids[20..40] {
                diamond_edges.push(EdgeInput {
                    from: l1,
                    to: l2,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        for &l2 in &ids[20..40] {
            diamond_edges.push(EdgeInput {
                from: l2,
                to: ids[99],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
        engine.batch_upsert_edges(&diamond_edges).unwrap();

        let from = ids[0];
        let to = ids[99];
        b.iter(|| {
            engine
                .all_shortest_paths(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                    Some(50),
                )
                .unwrap();
        });
    });

    group.bench_function("all_shortest_paths_weighted_diamond_100_segment", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            create_if_missing: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let mut engine = DatabaseEngine::open(dir.path(), &opts).unwrap();

        let inputs: Vec<NodeInput> = (0..100)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("wd_{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();

        let mut diamond_edges = Vec::new();
        for &l1 in &ids[1..20] {
            diamond_edges.push(EdgeInput {
                from: ids[0],
                to: l1,
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
            for &l2 in &ids[20..40] {
                diamond_edges.push(EdgeInput {
                    from: l1,
                    to: l2,
                    type_id: 1,
                    props: BTreeMap::new(),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                });
            }
        }
        for &l2 in &ids[20..40] {
            diamond_edges.push(EdgeInput {
                from: l2,
                to: ids[99],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            });
        }
        engine.batch_upsert_edges(&diamond_edges).unwrap();
        engine.flush().unwrap();

        let from = ids[0];
        let to = ids[99];
        b.iter(|| {
            engine
                .all_shortest_paths(
                    from,
                    to,
                    Direction::Outgoing,
                    None,
                    Some("weight"),
                    None,
                    None,
                    None,
                    Some(50),
                )
                .unwrap();
        });
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
    bench_degree,
    bench_degree_scalar_loop,
    bench_advanced_queries,
    bench_recovery,
    bench_shortest_path,
);
criterion_main!(benches);
