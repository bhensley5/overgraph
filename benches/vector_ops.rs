use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use overgraph::{
    DatabaseEngine, DbOptions, DenseMetric, DenseVectorConfig, Direction, EdgeInput, FusionMode,
    HnswConfig, NodeInput, VectorSearchMode, VectorSearchRequest, VectorSearchScope,
};
use std::collections::BTreeMap;

fn temp_db_with_opts(opts: DbOptions) -> (tempfile::TempDir, DatabaseEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    (dir, engine)
}

fn dense_bench_config(dimension: u32) -> DenseVectorConfig {
    DenseVectorConfig {
        dimension,
        metric: DenseMetric::Cosine,
        hnsw: HnswConfig::default(),
    }
}

fn dense_query_request(query: Vec<f32>, k: usize) -> VectorSearchRequest {
    VectorSearchRequest {
        mode: VectorSearchMode::Dense,
        dense_query: Some(query),
        sparse_query: None,
        k,
        type_filter: None,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    }
}

fn sparse_query_request(query: Vec<(u32, f32)>, k: usize) -> VectorSearchRequest {
    VectorSearchRequest {
        mode: VectorSearchMode::Sparse,
        dense_query: None,
        sparse_query: Some(query),
        k,
        type_filter: None,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    }
}

fn sparse_query_request_with_type_filter(
    query: Vec<(u32, f32)>,
    k: usize,
    type_filter: Option<Vec<u32>>,
) -> VectorSearchRequest {
    VectorSearchRequest {
        mode: VectorSearchMode::Sparse,
        dense_query: None,
        sparse_query: Some(query),
        k,
        type_filter,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    }
}

fn patterned_dense_vector(dimension: usize, index: usize, dominant: usize) -> Vec<f32> {
    let mut dense = vec![0.0f32; dimension];
    dense[dominant % dimension] = 1.0;
    dense[(dominant + 7) % dimension] = 0.25;
    dense[(index * 5 + dominant) % dimension] += 0.05;
    dense
}

fn normalize_vector(values: &mut [f32]) {
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in values.iter_mut() {
            *value /= norm;
        }
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn clustered_dense_vector(
    dimension: usize,
    cluster: usize,
    member: usize,
    cluster_count: usize,
) -> Vec<f32> {
    let mut values = vec![0.0f32; dimension];
    let primary = (cluster * 11) % dimension;
    let secondary = (cluster * 11 + 7) % dimension;
    let tertiary = (cluster * 11 + 19) % dimension;
    let quaternary = (cluster * 11 + 31) % dimension;
    values[primary] = 1.0;
    values[secondary] = 0.45;
    values[tertiary] = 0.2;
    values[quaternary] = 0.1;

    let seed = ((cluster as u64) << 32) ^ member as u64 ^ (cluster_count as u64).rotate_left(7);
    let mut noise = splitmix64(seed);
    for value in &mut values {
        noise = splitmix64(noise);
        let jitter = ((noise >> 40) as i32 - 8_192) as f32 / 65_536.0;
        *value += jitter * 0.14;
    }
    normalize_vector(&mut values);
    values
}

fn clustered_query_vector(
    dimension: usize,
    cluster: usize,
    query_idx: usize,
    cluster_count: usize,
) -> Vec<f32> {
    let mut values = clustered_dense_vector(dimension, cluster, query_idx + 100_000, cluster_count);
    let boundary_cluster = (cluster + 1) % cluster_count;
    if query_idx % 2 == 1 {
        let boundary = clustered_dense_vector(
            dimension,
            boundary_cluster,
            query_idx + 200_000,
            cluster_count,
        );
        for (value, boundary_value) in values.iter_mut().zip(boundary.iter()) {
            *value = (*value * 0.72) + (*boundary_value * 0.28);
        }
        normalize_vector(&mut values);
    }
    values
}

fn patterned_sparse_vector(primary: u32, secondary: u32, scale: f32) -> Vec<(u32, f32)> {
    vec![(primary, scale), (secondary, scale * 0.5)]
}

fn scale_sparse_vector(values: &[(u32, f32)], scale: f32) -> Vec<(u32, f32)> {
    values
        .iter()
        .map(|&(dimension_id, weight)| (dimension_id, weight * scale))
        .collect()
}

fn unique_sparse_dimensions(seed: u64, dimension_count: u32, nnz: usize) -> Vec<u32> {
    let mut dims = Vec::with_capacity(nnz);
    let mut state = seed;
    while dims.len() < nnz {
        state = splitmix64(state);
        let dimension_id = (state % dimension_count as u64) as u32;
        if !dims.contains(&dimension_id) {
            dims.push(dimension_id);
        }
    }
    dims.sort_unstable();
    dims
}

fn clustered_sparse_vector(
    dimension_count: u32,
    cluster: usize,
    member: usize,
    cluster_count: usize,
    nnz: usize,
) -> Vec<(u32, f32)> {
    let anchor_seed = ((cluster as u64) << 32) ^ cluster_count as u64 ^ 0xA5A5_5A5A;
    let anchor_dims = unique_sparse_dimensions(anchor_seed, dimension_count, nnz.min(6));
    let noise_dims = unique_sparse_dimensions(
        ((cluster as u64) << 32) ^ member as u64 ^ 0x9E37_79B9,
        dimension_count,
        nnz.saturating_sub(anchor_dims.len()),
    );

    let mut values = Vec::with_capacity(nnz);
    for (index, dimension_id) in anchor_dims.into_iter().enumerate() {
        values.push((dimension_id, 1.2 - index as f32 * 0.12));
    }
    for (index, dimension_id) in noise_dims.into_iter().enumerate() {
        values.push((dimension_id, 0.35 - index as f32 * 0.02));
    }
    values.sort_unstable_by_key(|&(dimension_id, _)| dimension_id);
    values
}

fn clustered_sparse_query(
    dimension_count: u32,
    cluster: usize,
    query_idx: usize,
    cluster_count: usize,
    nnz: usize,
) -> Vec<(u32, f32)> {
    let mut query = clustered_sparse_vector(
        dimension_count,
        cluster,
        query_idx + 100_000,
        cluster_count,
        nnz,
    );
    if query_idx % 2 == 1 {
        let adjacent = clustered_sparse_vector(
            dimension_count,
            (cluster + 1) % cluster_count,
            query_idx + 200_000,
            cluster_count,
            nnz,
        );
        for (index, (_, weight)) in query.iter_mut().enumerate() {
            *weight *= if index < 4 { 0.72 } else { 0.88 };
        }
        for (dimension_id, weight) in adjacent.into_iter().take(4) {
            if let Some((_, existing_weight)) = query
                .iter_mut()
                .find(|(existing_dim, _)| *existing_dim == dimension_id)
            {
                *existing_weight += weight * 0.28;
            } else {
                query.push((dimension_id, weight * 0.28));
            }
        }
        query.sort_unstable_by_key(|&(dimension_id, _)| dimension_id);
    }
    query
}

fn uniform_sparse_vector(dimension_count: u32, seed: u64, nnz: usize) -> Vec<(u32, f32)> {
    let dims = unique_sparse_dimensions(seed, dimension_count, nnz);
    dims.into_iter()
        .enumerate()
        .map(|(index, dimension_id)| {
            let weight_seed = splitmix64(seed ^ ((index as u64 + 1) * 0x9E37_79B9));
            let weight = 0.2 + ((weight_seed >> 40) as f32 / 16_777_215.0) * 1.1;
            (dimension_id, weight)
        })
        .collect()
}

fn clustered_sparse_inputs(
    cluster_count: usize,
    points_per_cluster: usize,
    dimension_count: u32,
    nnz: usize,
) -> Vec<NodeInput> {
    (0..cluster_count)
        .flat_map(|cluster| {
            (0..points_per_cluster).map(move |member| NodeInput {
                type_id: 1,
                key: format!("sc{cluster}_n{member}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: Some(clustered_sparse_vector(
                    dimension_count,
                    cluster,
                    member,
                    cluster_count,
                    nnz,
                )),
            })
        })
        .collect()
}

fn uniform_sparse_inputs(count: usize, dimension_count: u32, nnz: usize) -> Vec<NodeInput> {
    (0..count)
        .map(|index| NodeInput {
            type_id: 1,
            key: format!("su{index}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(uniform_sparse_vector(
                dimension_count,
                (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                nnz,
            )),
        })
        .collect()
}

fn clustered_sparse_multisegment_inputs_a(
    count: usize,
    dimension_count: u32,
    cluster_count: usize,
    nnz: usize,
) -> Vec<NodeInput> {
    let mut inputs = Vec::with_capacity(count * 3);
    for i in 0..count {
        let shared = clustered_sparse_vector(dimension_count, 3, i, cluster_count, nnz);
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("shared_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(shared),
        });
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("stable_a_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(clustered_sparse_vector(
                dimension_count,
                7,
                i,
                cluster_count,
                nnz,
            )),
        });
        inputs.push(NodeInput {
            type_id: 2,
            key: format!("other_type_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(clustered_sparse_vector(
                dimension_count,
                3,
                i + 10_000,
                cluster_count,
                nnz,
            )),
        });
    }
    inputs
}

fn clustered_sparse_multisegment_inputs_b(
    count: usize,
    dimension_count: u32,
    cluster_count: usize,
    nnz: usize,
) -> Vec<NodeInput> {
    let mut inputs = Vec::with_capacity(count * 2);
    for i in 0..count {
        let shared = clustered_sparse_vector(dimension_count, 3, i + 50_000, cluster_count, nnz);
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("shared_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(scale_sparse_vector(&shared, 1.15)),
        });
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("stable_b_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(clustered_sparse_vector(
                dimension_count,
                11,
                i,
                cluster_count,
                nnz,
            )),
        });
    }
    inputs
}

fn clustered_sparse_overlap_segment_inputs(
    segment_index: usize,
    count: usize,
    dimension_count: u32,
    cluster_count: usize,
    nnz: usize,
) -> Vec<NodeInput> {
    let mut inputs = Vec::with_capacity(count * 2);
    for i in 0..count {
        let shared = clustered_sparse_vector(
            dimension_count,
            5,
            segment_index * 10_000 + i,
            cluster_count,
            nnz,
        );
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("shared_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(scale_sparse_vector(
                &shared,
                1.0 + segment_index as f32 * 0.08,
            )),
        });
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("stable_{segment_index}_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: Some(clustered_sparse_vector(
                dimension_count,
                9 + segment_index,
                i,
                cluster_count,
                nnz,
            )),
        });
    }
    inputs
}

fn bench_vector_non_vector_parity(c: &mut Criterion) {
    c.bench_function("get_node_segment_no_vectors_default", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let inputs: Vec<NodeInput> = (0..2000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            black_box(engine.get_node(black_box(id)).unwrap());
            i += 1;
        });
    });

    c.bench_function("get_node_segment_no_vectors_dense_config", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(32)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let inputs: Vec<NodeInput> = (0..2000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("n{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let ids = engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            black_box(engine.get_node(black_box(id)).unwrap());
            i += 1;
        });
    });

    c.bench_function("get_node_plain_in_sparse_segment_db", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let plain_inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("plain{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let sparse_inputs: Vec<NodeInput> = (0..1000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("sparse{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: Some(patterned_sparse_vector(
                    (i % 32) as u32,
                    ((i + 5) % 32) as u32,
                    1.0,
                )),
            })
            .collect();
        let plain_ids = engine.batch_upsert_nodes(&plain_inputs).unwrap();
        engine.batch_upsert_nodes(&sparse_inputs).unwrap();
        engine.flush().unwrap();

        let mut i = 0usize;
        b.iter(|| {
            let id = plain_ids[i % plain_ids.len()];
            black_box(engine.get_node(black_box(id)).unwrap());
            i += 1;
        });
    });
}

fn bench_vector_search_dense(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search_dense");
    group.sample_size(10);

    group.bench_function("single_segment_clustered_9216x64_k10_default", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let inputs: Vec<NodeInput> = (0..cluster_count)
            .flat_map(|cluster| {
                (0..points_per_cluster).map(move |member| NodeInput {
                    type_id: 1,
                    key: format!("c{cluster}_n{member}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: Some(clustered_dense_vector(64, cluster, member, cluster_count)),
                    sparse_vector: None,
                })
            })
            .collect();
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let request = dense_query_request(clustered_query_vector(64, 3, 7, cluster_count), 10);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("multisegment_clustered_filtered_k10_default", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);

        let cluster_count = 24usize;
        let points_per_cluster = 128usize;
        for segment_index in 0..3usize {
            let segment_type = if segment_index == 0 { 1 } else { 2 };
            let inputs: Vec<NodeInput> = (0..cluster_count)
                .flat_map(|cluster| {
                    (0..points_per_cluster).map(move |member| NodeInput {
                        type_id: segment_type,
                        key: format!("seg{segment_index}_c{cluster}_n{member}"),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        dense_vector: Some(clustered_dense_vector(
                            64,
                            (cluster + segment_index * 3) % cluster_count,
                            member + segment_index * 10_000,
                            cluster_count,
                        )),
                        sparse_vector: None,
                    })
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
            engine.flush().unwrap();
        }

        let mut request = dense_query_request(clustered_query_vector(64, 3, 9, cluster_count), 10);
        request.type_filter = Some(vec![1]);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("memtable_clustered_filtered_k10_default", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);

        let cluster_count = 24usize;
        let points_per_cluster = 128usize;
        for batch_index in 0..3usize {
            let batch_type = if batch_index == 0 { 1 } else { 2 };
            let inputs: Vec<NodeInput> = (0..cluster_count)
                .flat_map(|cluster| {
                    (0..points_per_cluster).map(move |member| NodeInput {
                        type_id: batch_type,
                        key: format!("mt{batch_index}_c{cluster}_n{member}"),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        dense_vector: Some(clustered_dense_vector(
                            64,
                            (cluster + batch_index * 3) % cluster_count,
                            member + batch_index * 10_000,
                            cluster_count,
                        )),
                        sparse_vector: None,
                    })
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
        }

        let mut request = dense_query_request(clustered_query_vector(64, 3, 9, cluster_count), 10);
        request.type_filter = Some(vec![1]);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("single_segment_patterned_5000x32_k10_legacy_shape", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(32)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let inputs: Vec<NodeInput> = (0..5000)
            .map(|i| NodeInput {
                type_id: 1,
                key: format!("v{}", i),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: Some(patterned_dense_vector(32, i, i % 32)),
                sparse_vector: None,
            })
            .collect();
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let mut query = vec![0.0f32; 32];
        query[3] = 1.0;
        query[10] = 0.25;
        let request = dense_query_request(query, 10);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.finish();
}

fn bench_vector_search_sparse(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search_sparse");
    group.sample_size(10);

    group.bench_function("single_segment_clustered_9216x12of4096_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let inputs = clustered_sparse_inputs(cluster_count, points_per_cluster, 4096, 12);
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let request =
            sparse_query_request(clustered_sparse_query(4096, 3, 7, cluster_count, 12), 10);
        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("single_segment_uniform_9216x12of4096_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let inputs = uniform_sparse_inputs(9_216, 4096, 12);
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let request =
            sparse_query_request(uniform_sparse_vector(4096, 0xDEAD_BEEF_CAFE_BABE, 12), 10);
        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("multisegment_clustered_filtered_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let dimension_count = 4096u32;
        let nnz = 12usize;
        let inputs_a =
            clustered_sparse_multisegment_inputs_a(1_536, dimension_count, cluster_count, nnz);
        engine.batch_upsert_nodes(&inputs_a).unwrap();
        engine.flush().unwrap();
        let inputs_b =
            clustered_sparse_multisegment_inputs_b(1_536, dimension_count, cluster_count, nnz);
        engine.batch_upsert_nodes(&inputs_b).unwrap();
        engine.flush().unwrap();

        let request = sparse_query_request_with_type_filter(
            clustered_sparse_query(dimension_count, 3, 11, cluster_count, nnz),
            10,
            Some(vec![1]),
        );
        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("memtable_clustered_filtered_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let dimension_count = 4096u32;
        let nnz = 12usize;

        let mut inputs_a =
            clustered_sparse_multisegment_inputs_a(1_536, dimension_count, cluster_count, nnz);
        let mut inputs_b =
            clustered_sparse_multisegment_inputs_b(1_536, dimension_count, cluster_count, nnz);
        for input in &mut inputs_a {
            input.key = format!("mem_a_{}", input.key);
        }
        for input in &mut inputs_b {
            input.key = format!("mem_b_{}", input.key);
        }
        engine.batch_upsert_nodes(&inputs_a).unwrap();
        engine.batch_upsert_nodes(&inputs_b).unwrap();

        let request = sparse_query_request_with_type_filter(
            clustered_sparse_query(dimension_count, 3, 11, cluster_count, nnz),
            10,
            Some(vec![1]),
        );
        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.finish();
}

fn bench_sparse_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_build");
    group.sample_size(10);

    group.bench_function("flush_single_segment_clustered_9216x12of4096", |b| {
        b.iter_batched(
            || {
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    ..DbOptions::default()
                };
                let (_dir, engine) = temp_db_with_opts(opts);
                let inputs = clustered_sparse_inputs(24, 384, 4096, 12);
                engine.batch_upsert_nodes(&inputs).unwrap();
                (_dir, engine)
            },
            |(_dir, engine)| {
                black_box(engine.flush().unwrap());
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("compact_overlap_three_segments_3x3072x12of4096", |b| {
        b.iter_batched(
            || {
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    compact_after_n_flushes: 0,
                    ..DbOptions::default()
                };
                let (_dir, engine) = temp_db_with_opts(opts);
                let cluster_count = 24usize;
                let dimension_count = 4096u32;
                let nnz = 12usize;
                for segment_index in 0..3usize {
                    let inputs = clustered_sparse_overlap_segment_inputs(
                        segment_index,
                        1_024,
                        dimension_count,
                        cluster_count,
                        nnz,
                    );
                    engine.batch_upsert_nodes(&inputs).unwrap();
                    engine.flush().unwrap();
                }
                (_dir, engine)
            },
            |(_dir, engine)| {
                black_box(engine.compact().unwrap());
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn hybrid_query_request(
    dense: Vec<f32>,
    sparse: Vec<(u32, f32)>,
    k: usize,
    fusion_mode: Option<FusionMode>,
) -> VectorSearchRequest {
    VectorSearchRequest {
        mode: VectorSearchMode::Hybrid,
        dense_query: Some(dense),
        sparse_query: Some(sparse),
        k,
        type_filter: None,
        ef_search: None,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode,
    }
}

fn clustered_hybrid_inputs(
    cluster_count: usize,
    points_per_cluster: usize,
    dense_dim: usize,
    sparse_dim_count: u32,
    nnz: usize,
) -> Vec<NodeInput> {
    (0..cluster_count)
        .flat_map(|cluster| {
            (0..points_per_cluster).map(move |member| NodeInput {
                type_id: 1,
                key: format!("h{cluster}_n{member}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: Some(clustered_dense_vector(
                    dense_dim,
                    cluster,
                    member,
                    cluster_count,
                )),
                sparse_vector: Some(clustered_sparse_vector(
                    sparse_dim_count,
                    cluster,
                    member,
                    cluster_count,
                    nnz,
                )),
            })
        })
        .collect()
}

fn bench_vector_search_hybrid(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search_hybrid");
    group.sample_size(10);

    group.bench_function("single_segment_clustered_hybrid_k10_weighted_rank", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let inputs = clustered_hybrid_inputs(cluster_count, points_per_cluster, 64, 4096, 12);
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let request = hybrid_query_request(
            clustered_query_vector(64, 3, 7, cluster_count),
            clustered_sparse_query(4096, 3, 7, cluster_count, 12),
            10,
            None,
        );

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("single_segment_clustered_hybrid_k10_reciprocal_rank", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let inputs = clustered_hybrid_inputs(cluster_count, points_per_cluster, 64, 4096, 12);
        engine.batch_upsert_nodes(&inputs).unwrap();
        engine.flush().unwrap();

        let request = hybrid_query_request(
            clustered_query_vector(64, 3, 7, cluster_count),
            clustered_sparse_query(4096, 3, 7, cluster_count, 12),
            10,
            Some(FusionMode::ReciprocalRankFusion),
        );

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("multisegment_hybrid_filtered_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 128usize;
        for segment_index in 0..3usize {
            let segment_type = if segment_index == 0 { 1 } else { 2 };
            let inputs: Vec<NodeInput> = (0..cluster_count)
                .flat_map(|cluster| {
                    (0..points_per_cluster).map(move |member| NodeInput {
                        type_id: segment_type,
                        key: format!("seg{segment_index}_h{cluster}_n{member}"),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        dense_vector: Some(clustered_dense_vector(
                            64,
                            (cluster + segment_index * 3) % cluster_count,
                            member + segment_index * 10_000,
                            cluster_count,
                        )),
                        sparse_vector: Some(clustered_sparse_vector(
                            4096,
                            (cluster + segment_index * 3) % cluster_count,
                            member + segment_index * 10_000,
                            cluster_count,
                            12,
                        )),
                    })
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
            engine.flush().unwrap();
        }

        let mut request = hybrid_query_request(
            clustered_query_vector(64, 3, 9, cluster_count),
            clustered_sparse_query(4096, 3, 9, cluster_count, 12),
            10,
            None,
        );
        request.type_filter = Some(vec![1]);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("memtable_hybrid_filtered_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            compact_after_n_flushes: 0,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 24usize;
        let points_per_cluster = 128usize;
        for batch_index in 0..3usize {
            let batch_type = if batch_index == 0 { 1 } else { 2 };
            let inputs: Vec<NodeInput> = (0..cluster_count)
                .flat_map(|cluster| {
                    (0..points_per_cluster).map(move |member| NodeInput {
                        type_id: batch_type,
                        key: format!("mt{batch_index}_h{cluster}_n{member}"),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        dense_vector: Some(clustered_dense_vector(
                            64,
                            (cluster + batch_index * 3) % cluster_count,
                            member + batch_index * 10_000,
                            cluster_count,
                        )),
                        sparse_vector: Some(clustered_sparse_vector(
                            4096,
                            (cluster + batch_index * 3) % cluster_count,
                            member + batch_index * 10_000,
                            cluster_count,
                            12,
                        )),
                    })
                })
                .collect();
            engine.batch_upsert_nodes(&inputs).unwrap();
        }

        let mut request = hybrid_query_request(
            clustered_query_vector(64, 3, 9, cluster_count),
            clustered_sparse_query(4096, 3, 9, cluster_count, 12),
            10,
            None,
        );
        request.type_filter = Some(vec![1]);

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.finish();
}

fn bench_vector_search_scoped(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search_scoped");
    group.sample_size(10);

    group.bench_function("dense_scoped_1hop_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 8usize;
        let spokes_per_cluster = 62usize;
        let total_spokes = cluster_count * spokes_per_cluster;

        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }];
        for cluster in 0..cluster_count {
            for member in 0..spokes_per_cluster {
                node_inputs.push(NodeInput {
                    type_id: 2,
                    key: format!("spoke_{cluster}_{member}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: Some(clustered_dense_vector(64, cluster, member, cluster_count)),
                    sparse_vector: None,
                });
            }
        }
        let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let hub_id = ids[0];

        let edge_inputs: Vec<EdgeInput> = (0..total_spokes)
            .map(|i| EdgeInput {
                from: hub_id,
                to: ids[1 + i],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        engine.flush().unwrap();

        let request = VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(clustered_query_vector(64, 3, 7, cluster_count)),
            sparse_query: None,
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: Some(VectorSearchScope {
                start_node_id: hub_id,
                max_depth: 1,
                direction: Direction::Outgoing,
                edge_type_filter: None,
                at_epoch: None,
            }),
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.bench_function("hybrid_scoped_1hop_k10", |b| {
        let opts = DbOptions {
            create_if_missing: true,
            edge_uniqueness: true,
            dense_vector: Some(dense_bench_config(64)),
            ..DbOptions::default()
        };
        let (_dir, engine) = temp_db_with_opts(opts);
        let cluster_count = 8usize;
        let spokes_per_cluster = 62usize;
        let total_spokes = cluster_count * spokes_per_cluster;

        let mut node_inputs = vec![NodeInput {
            type_id: 1,
            key: "hub".to_string(),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        }];
        for cluster in 0..cluster_count {
            for member in 0..spokes_per_cluster {
                node_inputs.push(NodeInput {
                    type_id: 2,
                    key: format!("spoke_{cluster}_{member}"),
                    props: BTreeMap::new(),
                    weight: 1.0,
                    dense_vector: Some(clustered_dense_vector(64, cluster, member, cluster_count)),
                    sparse_vector: Some(clustered_sparse_vector(
                        4096,
                        cluster,
                        member,
                        cluster_count,
                        12,
                    )),
                });
            }
        }
        let ids = engine.batch_upsert_nodes(&node_inputs).unwrap();
        let hub_id = ids[0];

        let edge_inputs: Vec<EdgeInput> = (0..total_spokes)
            .map(|i| EdgeInput {
                from: hub_id,
                to: ids[1 + i],
                type_id: 1,
                props: BTreeMap::new(),
                weight: 1.0,
                valid_from: None,
                valid_to: None,
            })
            .collect();
        engine.batch_upsert_edges(&edge_inputs).unwrap();
        engine.flush().unwrap();

        let request = VectorSearchRequest {
            mode: VectorSearchMode::Hybrid,
            dense_query: Some(clustered_query_vector(64, 3, 7, cluster_count)),
            sparse_query: Some(clustered_sparse_query(4096, 3, 7, cluster_count, 12)),
            k: 10,
            type_filter: None,
            ef_search: None,
            scope: Some(VectorSearchScope {
                start_node_id: hub_id,
                max_depth: 1,
                direction: Direction::Outgoing,
                edge_type_filter: None,
                at_epoch: None,
            }),
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };

        b.iter(|| {
            black_box(engine.vector_search(black_box(&request)).unwrap());
        });
    });

    group.finish();
}

fn clustered_dense_overlap_segment_inputs(
    segment_index: usize,
    count: usize,
    dimension: usize,
    cluster_count: usize,
) -> Vec<NodeInput> {
    let mut inputs = Vec::with_capacity(count * 2);
    for i in 0..count {
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("shared_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: Some(clustered_dense_vector(
                dimension,
                5,
                segment_index * 10_000 + i,
                cluster_count,
            )),
            sparse_vector: None,
        });
        inputs.push(NodeInput {
            type_id: 1,
            key: format!("stable_{segment_index}_{i}"),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: Some(clustered_dense_vector(
                dimension,
                9 + segment_index,
                i,
                cluster_count,
            )),
            sparse_vector: None,
        });
    }
    inputs
}

fn bench_dense_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("dense_build");
    group.sample_size(10);

    group.bench_function("flush_single_segment_clustered_9216x64", |b| {
        b.iter_batched(
            || {
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    dense_vector: Some(dense_bench_config(64)),
                    ..DbOptions::default()
                };
                let (_dir, engine) = temp_db_with_opts(opts);
                let cluster_count = 24usize;
                let points_per_cluster = 384usize;
                let inputs: Vec<NodeInput> = (0..cluster_count)
                    .flat_map(|cluster| {
                        (0..points_per_cluster).map(move |member| NodeInput {
                            type_id: 1,
                            key: format!("d{cluster}_n{member}"),
                            props: BTreeMap::new(),
                            weight: 1.0,
                            dense_vector: Some(clustered_dense_vector(
                                64,
                                cluster,
                                member,
                                cluster_count,
                            )),
                            sparse_vector: None,
                        })
                    })
                    .collect();
                engine.batch_upsert_nodes(&inputs).unwrap();
                (_dir, engine)
            },
            |(_dir, engine)| {
                black_box(engine.flush().unwrap());
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("compact_overlap_three_segments_3x2048x64", |b| {
        b.iter_batched(
            || {
                let opts = DbOptions {
                    create_if_missing: true,
                    edge_uniqueness: true,
                    compact_after_n_flushes: 0,
                    dense_vector: Some(dense_bench_config(64)),
                    ..DbOptions::default()
                };
                let (_dir, engine) = temp_db_with_opts(opts);
                let cluster_count = 24usize;
                for segment_index in 0..3usize {
                    let inputs = clustered_dense_overlap_segment_inputs(
                        segment_index,
                        1_024,
                        64,
                        cluster_count,
                    );
                    engine.batch_upsert_nodes(&inputs).unwrap();
                    engine.flush().unwrap();
                }
                (_dir, engine)
            },
            |(_dir, engine)| {
                black_box(engine.compact().unwrap());
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_vector_non_vector_parity,
    bench_vector_search_dense,
    bench_vector_search_sparse,
    bench_sparse_build,
    bench_vector_search_hybrid,
    bench_vector_search_scoped,
    bench_dense_build,
);
criterion_main!(benches);
