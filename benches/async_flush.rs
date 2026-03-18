// CP11: Async flush benchmarks. Phase 21 readiness gate.
//
// Measures write-burst throughput and mixed read/write performance under flush
// pressure, comparing sync-flush behavior (old) vs async auto-flush (Phase 21).
//
// Design: each scenario uses `iter_batched(PerIteration)` so every measurement
// starts from a clean DB, with no cross-iteration backpressure accumulation.
//
//   sync_baseline:    threshold=0, manual periodic flush() within the burst
//   async_auto_flush: threshold=ASYNC_THRESHOLD, auto-flush freezes + enqueues
//
// The sync baseline simulates the pre-Phase-21 behavior where maybe_auto_flush
// called flush() as a synchronous barrier, blocking the writer for the entire
// segment write. The async variant freezes the memtable and returns immediately.
//
// Run:       cargo bench --bench async_flush
// Specific:  cargo bench --bench async_flush -- "sustained_writes"

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, DenseMetric, DenseVectorConfig, HnswConfig, NodeInput, PropValue,
    UpsertNodeOptions, VectorSearchMode, VectorSearchRequest,
};
use std::collections::BTreeMap;

/// Nodes per write burst. With ~300 byte nodes and 1MB threshold, this
/// crosses the flush threshold ~3 times per burst.
const BURST_SIZE: u64 = 10_000;
/// Async auto-flush threshold in bytes.
const ASYNC_THRESHOLD: usize = 1024 * 1024;
/// Approximate writes between sync flush() calls, calibrated to match
/// the async threshold crossing frequency (~300 bytes/node → ~3300 per 1MB).
const SYNC_FLUSH_INTERVAL: u64 = 3300;

fn temp_db_with_opts(opts: DbOptions) -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    (dir, engine)
}

fn sync_opts() -> DbOptions {
    DbOptions {
        create_if_missing: true,
        memtable_flush_threshold: 0, // disable auto-flush
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    }
}

fn async_opts() -> DbOptions {
    DbOptions {
        create_if_missing: true,
        memtable_flush_threshold: ASYNC_THRESHOLD,
        compact_after_n_flushes: 0,
        ..DbOptions::default()
    }
}

/// Build UpsertNodeOptions with properties to make each node ~300 bytes
/// in the memtable, producing meaningful segment sizes on flush.
fn write_opts(i: u64) -> UpsertNodeOptions {
    let mut props = BTreeMap::new();
    props.insert(
        "name".to_string(),
        PropValue::String(format!("benchmark_node_{}", i)),
    );
    props.insert(
        "category".to_string(),
        PropValue::String("perf_test_data".to_string()),
    );
    props.insert("score".to_string(), PropValue::Float(i as f64 * 0.001));
    UpsertNodeOptions {
        props,
        ..Default::default()
    }
}

// ── Vector helpers ──────────────────────────────────────────────────────────

fn simple_dense_vector(dim: usize, index: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; dim];
    let primary = index % dim;
    v[primary] = 1.0;
    v[(primary + 7) % dim] = 0.25;
    v[(index * 3 + 13) % dim] += 0.05;
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn simple_sparse_vector(index: usize, nnz: usize) -> Vec<(u32, f32)> {
    let mut dims = Vec::with_capacity(nnz);
    for j in 0..nnz {
        let d = ((index * 7 + j * 31 + 13) % 4096) as u32;
        dims.push((d, 1.0 - j as f32 * 0.08));
    }
    dims.sort_unstable_by_key(|&(d, _)| d);
    dims.dedup_by_key(|(d, _)| *d);
    dims
}

fn pre_populate_plain(engine: &mut DatabaseEngine, count: usize) -> Vec<u64> {
    let inputs: Vec<NodeInput> = (0..count)
        .map(|i| NodeInput {
            type_id: 1,
            key: format!("seed_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        })
        .collect();
    let ids = engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
    ids
}

fn pre_populate_dense(engine: &mut DatabaseEngine, count: usize, dim: usize) {
    let inputs: Vec<NodeInput> = (0..count)
        .map(|i| NodeInput {
            type_id: 1,
            key: format!("vec_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: Some(simple_dense_vector(dim, i)),
            sparse_vector: None,
        })
        .collect();
    engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
}

fn pre_populate_sparse(engine: &mut DatabaseEngine, count: usize, nnz: usize) {
    let inputs: Vec<NodeInput> = (0..count)
        .map(|i| NodeInput {
            type_id: 1,
            key: format!("svec_{}", i),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(simple_sparse_vector(i, nnz)),
        })
        .collect();
    engine.batch_upsert_nodes(&inputs).unwrap();
    engine.flush().unwrap();
}

// ── Scenario 1: Sustained writes crossing the soft threshold ────────────────
//
// Each iteration is a burst of BURST_SIZE node writes with properties.
// Sync baseline: manual flush() every SYNC_FLUSH_INTERVAL writes.
// Async: auto-flush triggers at ASYNC_THRESHOLD, returns immediately.

fn bench_sustained_writes_threshold(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_flush_sustained_writes");
    group.sample_size(30);

    group.bench_function("sync_baseline", |b| {
        b.iter_batched(
            || temp_db_with_opts(sync_opts()),
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("n{}", i), write_opts(i))
                        .unwrap();
                    if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("async_auto_flush", |b| {
        b.iter_batched(
            || temp_db_with_opts(async_opts()),
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("n{}", i), write_opts(i))
                        .unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ── Scenario 2: Sustained writes while immutable epochs are already queued ──
//
// Pre-populates 2000 nodes (triggering several freezes in async mode), then
// measures another burst of writes under that existing queue pressure.

fn bench_writes_with_queued_epochs(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_flush_queued_epochs");
    group.sample_size(30);

    group.bench_function("sync_baseline", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(sync_opts());
                for j in 0..2000u64 {
                    engine
                        .upsert_node(1, &format!("pre_{}", j), write_opts(j))
                        .unwrap();
                    if (j + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                }
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    let k = 2000 + i;
                    engine
                        .upsert_node(1, &format!("n{}", k), write_opts(k))
                        .unwrap();
                    if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("async_auto_flush", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(async_opts());
                for j in 0..2000u64 {
                    engine
                        .upsert_node(1, &format!("pre_{}", j), write_opts(j))
                        .unwrap();
                }
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    let k = 2000 + i;
                    engine
                        .upsert_node(1, &format!("n{}", k), write_opts(k))
                        .unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ── Scenario 3: Mixed writes + raw lookups under pending flush debt ─────────
//
// Pre-populates 1000 read-target nodes, then interleaves writes and reads.
// Measures combined write+read throughput under flush pressure.

fn bench_mixed_writes_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_flush_mixed_reads");
    group.sample_size(30);

    group.bench_function("sync_baseline", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(sync_opts());
                let ids = pre_populate_plain(&mut engine, 1000);
                (_dir, engine, ids)
            },
            |(_dir, mut engine, ids)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                    black_box(engine.get_node(ids[(i as usize) % ids.len()]).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("async_auto_flush", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(async_opts());
                let ids = pre_populate_plain(&mut engine, 1000);
                (_dir, engine, ids)
            },
            |(_dir, mut engine, ids)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    black_box(engine.get_node(ids[(i as usize) % ids.len()]).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ── Scenario 4: Mixed writes + dense vector search ──────────────────────────
//
// Pre-populates 1000 nodes with dim-32 dense vectors, then interleaves
// writes and vector searches under flush pressure.

fn bench_mixed_writes_dense_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_flush_mixed_dense_vector");
    group.sample_size(10);

    let dim = 32usize;
    let dense_config = DenseVectorConfig {
        dimension: dim as u32,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    };
    let request = VectorSearchRequest {
        mode: VectorSearchMode::Dense,
        dense_query: Some(simple_dense_vector(dim, 999)),
        sparse_query: None,
        k: 10,
        type_filter: None,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };

    group.bench_function("sync_baseline", |b| {
        b.iter_batched(
            || {
                let mut opts = sync_opts();
                opts.dense_vector = Some(dense_config.clone());
                let (_dir, mut engine) = temp_db_with_opts(opts);
                pre_populate_dense(&mut engine, 1000, dim);
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                    black_box(engine.vector_search(&request).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("async_auto_flush", |b| {
        b.iter_batched(
            || {
                let mut opts = async_opts();
                opts.dense_vector = Some(dense_config.clone());
                let (_dir, mut engine) = temp_db_with_opts(opts);
                pre_populate_dense(&mut engine, 1000, dim);
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    black_box(engine.vector_search(&request).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

// ── Scenario 5: Mixed writes + sparse vector search ─────────────────────────
//
// Pre-populates 1000 nodes with 8-nnz sparse vectors over 4096 dimensions,
// then interleaves writes and sparse searches under flush pressure.

fn bench_mixed_writes_sparse_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_flush_mixed_sparse_vector");
    group.sample_size(10);

    let request = VectorSearchRequest {
        mode: VectorSearchMode::Sparse,
        dense_query: None,
        sparse_query: Some(simple_sparse_vector(999, 8)),
        k: 10,
        type_filter: None,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };

    group.bench_function("sync_baseline", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(sync_opts());
                pre_populate_sparse(&mut engine, 1000, 8);
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    if (i + 1) % SYNC_FLUSH_INTERVAL == 0 {
                        engine.flush().unwrap();
                    }
                    black_box(engine.vector_search(&request).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("async_auto_flush", |b| {
        b.iter_batched(
            || {
                let (_dir, mut engine) = temp_db_with_opts(async_opts());
                pre_populate_sparse(&mut engine, 1000, 8);
                (_dir, engine)
            },
            |(_dir, mut engine)| {
                for i in 0..BURST_SIZE {
                    engine
                        .upsert_node(1, &format!("w{}", i), write_opts(i))
                        .unwrap();
                    black_box(engine.vector_search(&request).unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sustained_writes_threshold,
    bench_writes_with_queued_epochs,
    bench_mixed_writes_reads,
    bench_mixed_writes_dense_vector,
    bench_mixed_writes_sparse_vector,
);
criterion_main!(benches);
