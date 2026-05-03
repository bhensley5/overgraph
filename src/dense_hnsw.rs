use crate::error::EngineError;
use crate::parallel::engine_cpu_install;
use crate::types::{DenseMetric, DenseVectorConfig, NodeIdSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, RwLock};

pub(crate) const DENSE_HNSW_META_FILENAME: &str = "dense_hnsw_meta.dat";
pub(crate) const DENSE_HNSW_GRAPH_FILENAME: &str = "dense_hnsw_graph.dat";

const DENSE_HNSW_MAGIC: [u8; 4] = *b"DHNW";
const DENSE_HNSW_VERSION: u32 = 1;
const DENSE_HNSW_HEADER_SIZE: usize = 36;
const DENSE_HNSW_POINT_META_SIZE: usize = 32;
const DENSE_VECTOR_VALUE_SIZE: usize = 4;
const MAX_HNSW_LEVEL: u8 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DenseHnswHeader {
    pub point_count: u64,
    pub entry_point: u32,
    pub max_level: u16,
    pub m: u16,
    pub ef_construction: u16,
    pub metric: DenseMetric,
    pub dimension: u32,
}

#[derive(Clone)]
struct DensePoint {
    node_id: u64,
    dense_vector_offset: u64,
    values: Vec<f32>,
    norm: f32,
}

#[derive(Clone)]
pub(crate) struct DensePointInput {
    pub node_id: u64,
    pub dense_vector_offset: u64,
    pub values: Vec<f32>,
}

#[derive(Clone, Copy)]
pub(crate) struct DenseQueryPoint {
    pub node_id: u64,
    pub dense_vector_offset: u64,
    pub level_offset: u64,
    pub max_level: u16,
}

#[derive(Clone, Copy)]
struct PointMeta {
    node_id: u64,
    dense_vector_offset: u64,
    level_offset: u64,
    max_level: u16,
}

#[derive(Clone, Copy)]
struct MinCandidate {
    dist: f32,
    point: usize,
}

impl PartialEq for MinCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.point == other.point && self.dist.to_bits() == other.dist.to_bits()
    }
}

impl Eq for MinCandidate {}

impl Ord for MinCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .dist
            .total_cmp(&self.dist)
            .then_with(|| other.point.cmp(&self.point))
    }
}

impl PartialOrd for MinCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct MaxCandidate {
    dist: f32,
    point: usize,
}

#[derive(Clone, Copy)]
struct ScoredPoint {
    point: usize,
    dist: f32,
}

struct BuildSearchScratch {
    visit_marks: Vec<u32>,
    visit_generation: u32,
    candidates: BinaryHeap<MinCandidate>,
    top: BinaryHeap<MaxCandidate>,
    results: Vec<ScoredPoint>,
    return_buffer: Vec<ScoredPoint>,
}

impl BuildSearchScratch {
    fn new(point_count: usize, ef_capacity: usize) -> Self {
        Self {
            visit_marks: vec![0; point_count],
            visit_generation: 0,
            candidates: BinaryHeap::with_capacity(ef_capacity.max(1)),
            top: BinaryHeap::with_capacity(ef_capacity.max(1)),
            results: Vec::with_capacity(ef_capacity.max(1)),
            return_buffer: Vec::with_capacity(ef_capacity.max(1)),
        }
    }

    fn begin(&mut self, ef: usize) {
        self.visit_generation = self.visit_generation.wrapping_add(1);
        if self.visit_generation == 0 {
            self.visit_marks.fill(0);
            self.visit_generation = 1;
        }
        self.candidates.clear();
        self.top.clear();
        self.results.clear();
        let reserve = ef.saturating_sub(self.candidates.capacity());
        if reserve > 0 {
            self.candidates.reserve(reserve);
        }
        let reserve = ef.saturating_sub(self.top.capacity());
        if reserve > 0 {
            self.top.reserve(reserve);
        }
        let reserve = ef.saturating_sub(self.results.capacity());
        if reserve > 0 {
            self.results.reserve(reserve);
        }
        let reserve = ef.saturating_sub(self.return_buffer.capacity());
        if reserve > 0 {
            self.return_buffer.reserve(reserve);
        }
    }

    fn mark_visited(&mut self, point: usize) -> bool {
        let mark = &mut self.visit_marks[point];
        if *mark == self.visit_generation {
            false
        } else {
            *mark = self.visit_generation;
            true
        }
    }
}

struct BuildPruneScratch {
    scored: Vec<ScoredPoint>,
    selected: Vec<ScoredPoint>,
    discarded: Vec<ScoredPoint>,
}

impl BuildPruneScratch {
    fn new(capacity: usize) -> Self {
        Self {
            scored: Vec::with_capacity(capacity),
            selected: Vec::with_capacity(capacity),
            discarded: Vec::with_capacity(capacity),
        }
    }

    fn reserve_for(&mut self, len: usize) {
        let reserve = len.saturating_sub(self.scored.capacity());
        if reserve > 0 {
            self.scored.reserve(reserve);
        }
        let reserve = len.saturating_sub(self.selected.capacity());
        if reserve > 0 {
            self.selected.reserve(reserve);
        }
        let reserve = len.saturating_sub(self.discarded.capacity());
        if reserve > 0 {
            self.discarded.reserve(reserve);
        }
    }
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.point == other.point && self.dist.to_bits() == other.dist.to_bits()
    }
}

impl Eq for MaxCandidate {}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.point.cmp(&other.point))
    }
}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn write_dense_hnsw_index_from_points(
    seg_dir: &Path,
    dense_config: Option<&DenseVectorConfig>,
    points: Vec<DensePointInput>,
) -> Result<(), EngineError> {
    let Some(config) = dense_config else {
        return Ok(());
    };
    let points: Vec<DensePoint> = points
        .into_iter()
        .map(|point| DensePoint {
            node_id: point.node_id,
            dense_vector_offset: point.dense_vector_offset,
            norm: dense_vector_norm(&point.values),
            values: point.values,
        })
        .collect();
    write_dense_hnsw_index_from_loaded_points(seg_dir, config, points)
}

fn write_dense_hnsw_index_from_loaded_points(
    seg_dir: &Path,
    config: &DenseVectorConfig,
    points: Vec<DensePoint>,
) -> Result<(), EngineError> {
    if points.is_empty() {
        return Ok(());
    }

    let built = build_hnsw(points, config)?;
    write_hnsw_files(seg_dir, config, &built)
}

pub(crate) fn validate_dense_hnsw_files(
    meta: &[u8],
    graph: &[u8],
    dense_blob_len: usize,
    dense_vector_count: usize,
    dense_config: Option<&DenseVectorConfig>,
) -> Result<Option<DenseHnswHeader>, EngineError> {
    if meta.is_empty() && graph.is_empty() {
        if dense_vector_count > 0 {
            return Err(EngineError::CorruptRecord(format!(
                "dense HNSW files are missing for {} dense vectors",
                dense_vector_count
            )));
        }
        return Ok(None);
    }
    if meta.is_empty() || graph.is_empty() {
        return Err(EngineError::CorruptRecord(
            "dense HNSW files must appear together".into(),
        ));
    }

    let header = read_header(meta)?;
    if header.point_count == 0 {
        return Err(EngineError::CorruptRecord(
            "dense HNSW metadata has zero points".into(),
        ));
    }
    if header.point_count as usize != dense_vector_count {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW point count {} does not match dense vector count {}",
            header.point_count, dense_vector_count
        )));
    }
    match dense_config {
        Some(config) => {
            if header.metric != config.metric {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW metric {:?} does not match configured metric {:?}",
                    header.metric, config.metric
                )));
            }
            if header.dimension != config.dimension {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW dimension {} does not match configured dimension {}",
                    header.dimension, config.dimension
                )));
            }
            if header.m != config.hnsw.m {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW m {} does not match configured m {}",
                    header.m, config.hnsw.m
                )));
            }
            if header.ef_construction != config.hnsw.ef_construction {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW ef_construction {} does not match configured ef_construction {}",
                    header.ef_construction, config.hnsw.ef_construction
                )));
            }
        }
        None => {
            return Err(EngineError::CorruptRecord(
                "dense HNSW files require DbOptions::dense_vector to be configured".into(),
            ));
        }
    }

    let expected_meta_len = DENSE_HNSW_HEADER_SIZE
        .checked_add(header.point_count as usize * DENSE_HNSW_POINT_META_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("dense HNSW metadata size overflow".into()))?;
    if meta.len() != expected_meta_len {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW metadata size {} does not match expected {}",
            meta.len(),
            expected_meta_len
        )));
    }

    if header.entry_point as u64 >= header.point_count {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW entry point {} out of range for {} points",
            header.entry_point, header.point_count
        )));
    }

    let dense_vector_bytes = header
        .dimension
        .checked_mul(DENSE_VECTOR_VALUE_SIZE as u32)
        .ok_or_else(|| EngineError::CorruptRecord("dense vector byte size overflow".into()))?
        as usize;

    let mut prev_node_id = None;
    let mut expected_level_offset = 0usize;
    for index in 0..header.point_count as usize {
        let point = read_point_meta(meta, index)?;
        if let Some(prev_node_id) = prev_node_id {
            if point.node_id <= prev_node_id {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW node IDs must be strictly increasing (index {} has {} after {})",
                    index, point.node_id, prev_node_id
                )));
            }
        }
        prev_node_id = Some(point.node_id);

        let dense_offset = point.dense_vector_offset as usize;
        let dense_end = dense_offset
            .checked_add(dense_vector_bytes)
            .ok_or_else(|| EngineError::CorruptRecord("dense vector range overflow".into()))?;
        if dense_end > dense_blob_len {
            return Err(EngineError::CorruptRecord(format!(
                "dense HNSW point {} vector range [{}, {}) exceeds dense blob length {}",
                index, dense_offset, dense_end, dense_blob_len
            )));
        }

        if point.level_offset as usize != expected_level_offset {
            return Err(EngineError::CorruptRecord(format!(
                "dense HNSW point {} level offset {} does not match expected {}",
                index, point.level_offset, expected_level_offset
            )));
        }
        if point.max_level > header.max_level {
            return Err(EngineError::CorruptRecord(format!(
                "dense HNSW point {} max level {} exceeds header max level {}",
                index, point.max_level, header.max_level
            )));
        }

        let mut cursor = expected_level_offset;
        for _ in 0..=point.max_level as usize {
            let neighbor_count = read_u16_at(graph, cursor)? as usize;
            cursor = cursor.checked_add(4).ok_or_else(|| {
                EngineError::CorruptRecord("dense HNSW level header overflow".into())
            })?;
            let bytes = neighbor_count.checked_mul(4).ok_or_else(|| {
                EngineError::CorruptRecord("dense HNSW neighbor bytes overflow".into())
            })?;
            let end = cursor.checked_add(bytes).ok_or_else(|| {
                EngineError::CorruptRecord("dense HNSW neighbor range overflow".into())
            })?;
            if end > graph.len() {
                return Err(EngineError::CorruptRecord(format!(
                    "dense HNSW graph range [{}, {}) exceeds graph length {}",
                    cursor,
                    end,
                    graph.len()
                )));
            }
            for neighbor_idx in 0..neighbor_count {
                let neighbor = read_u32_at(graph, cursor + neighbor_idx * 4)? as u64;
                if neighbor >= header.point_count {
                    return Err(EngineError::CorruptRecord(format!(
                        "dense HNSW point {} references out-of-range neighbor {}",
                        index, neighbor
                    )));
                }
            }
            cursor = end;
        }
        expected_level_offset = cursor;
    }

    if expected_level_offset != graph.len() {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW graph has trailing or unreferenced bytes: expected {}, got {}",
            expected_level_offset,
            graph.len()
        )));
    }

    Ok(Some(header))
}

#[cfg(test)]
pub(crate) fn search_dense_hnsw(
    meta: &[u8],
    graph: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    ef_search: usize,
    limit: usize,
) -> Result<Vec<(u64, f32)>, EngineError> {
    if meta.is_empty() || graph.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let header = read_header(meta)?;
    if query.len() != header.dimension as usize {
        return Err(EngineError::InvalidOperation(format!(
            "dense query length {} does not match configured dimension {}",
            query.len(),
            header.dimension
        )));
    }

    let point_count = header.point_count as usize;
    if point_count == 0 {
        return Ok(Vec::new());
    }

    let points = load_dense_hnsw_query_points(meta, header)?;
    search_dense_hnsw_with_points(header, &points, graph, dense_blob, query, ef_search, limit)
}

pub(crate) fn search_dense_hnsw_with_points(
    header: DenseHnswHeader,
    points: &[DenseQueryPoint],
    graph: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    ef_search: usize,
    limit: usize,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let point_count = header.point_count as usize;
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if points.len() != point_count {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW point cache length {} does not match header count {}",
            points.len(),
            point_count
        )));
    }

    let limit = limit.min(point_count);
    let ef_search = ef_search.max(limit).max(1).min(point_count);
    let query_norm = (header.metric == DenseMetric::Cosine).then(|| dense_vector_norm(query));

    // For tiny segments, exact scan is simpler and guarantees oracle parity.
    if point_count <= ef_search {
        return exact_dense_search_with_points_and_query_norm(
            points,
            dense_blob,
            query,
            header.metric,
            query_norm,
            limit,
        );
    }

    let mut entry_point = header.entry_point as usize;
    let mut entry_distance = point_distance(
        points,
        dense_blob,
        query,
        header.metric,
        query_norm,
        header.dimension as usize,
        entry_point,
    )?;

    for level in (1..=header.max_level as usize).rev() {
        loop {
            let mut improved = false;
            let (neighbors_start, neighbor_count) =
                level_neighbor_span_from_point(points[entry_point], graph, level)?;
            for neighbor_idx in 0..neighbor_count {
                let neighbor = read_u32_at(graph, neighbors_start + neighbor_idx * 4)? as usize;
                let neighbor_distance = point_distance(
                    points,
                    dense_blob,
                    query,
                    header.metric,
                    query_norm,
                    header.dimension as usize,
                    neighbor,
                )?;
                if neighbor_distance < entry_distance {
                    entry_point = neighbor;
                    entry_distance = neighbor_distance;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }

    let mut visited = vec![0u64; point_count.div_ceil(64)];
    let mut candidates = BinaryHeap::new();
    let mut top = BinaryHeap::new();

    mark_visited(&mut visited, entry_point);
    candidates.push(MinCandidate {
        dist: entry_distance,
        point: entry_point,
    });
    top.push(MaxCandidate {
        dist: entry_distance,
        point: entry_point,
    });

    while let Some(candidate) = candidates.pop() {
        let Some(farthest) = top.peek() else {
            break;
        };
        if candidate.dist > farthest.dist {
            break;
        }

        let (neighbors_start, neighbor_count) =
            level_neighbor_span_from_point(points[candidate.point], graph, 0)?;
        for neighbor_idx in 0..neighbor_count {
            let neighbor = read_u32_at(graph, neighbors_start + neighbor_idx * 4)? as usize;
            if is_visited(&visited, neighbor) {
                continue;
            }
            mark_visited(&mut visited, neighbor);

            let neighbor_distance = point_distance(
                points,
                dense_blob,
                query,
                header.metric,
                query_norm,
                header.dimension as usize,
                neighbor,
            )?;

            if top.len() < ef_search || neighbor_distance < top.peek().unwrap().dist {
                candidates.push(MinCandidate {
                    dist: neighbor_distance,
                    point: neighbor,
                });
                top.push(MaxCandidate {
                    dist: neighbor_distance,
                    point: neighbor,
                });
                if top.len() > ef_search {
                    top.pop();
                }
            }
        }
    }

    let mut ranked: Vec<(usize, f32, u64)> = top
        .into_iter()
        .map(|entry| (entry.point, entry.dist, points[entry.point].node_id))
        .collect();
    ranked.sort_unstable_by(
        |(_, left_dist, left_node_id), (_, right_dist, right_node_id)| {
            left_dist
                .total_cmp(right_dist)
                .then_with(|| left_node_id.cmp(right_node_id))
        },
    );
    ranked.truncate(limit);

    let mut hits = Vec::with_capacity(ranked.len());
    for (_, distance, node_id) in ranked {
        hits.push((node_id, score_from_distance(header.metric, distance)));
    }
    Ok(hits)
}

#[inline]
fn is_visited(visited: &[u64], point: usize) -> bool {
    let word = point / 64;
    let mask = 1u64 << (point % 64);
    (visited[word] & mask) != 0
}

#[inline]
fn mark_visited(visited: &mut [u64], point: usize) {
    let word = point / 64;
    let mask = 1u64 << (point % 64);
    visited[word] |= mask;
}

struct BuiltHnsw {
    header: DenseHnswHeader,
    point_metas: Vec<PointMeta>,
    graph: Vec<Vec<Vec<usize>>>,
}

fn build_hnsw(
    mut points: Vec<DensePoint>,
    config: &DenseVectorConfig,
) -> Result<BuiltHnsw, EngineError> {
    points.sort_unstable_by_key(|point| point.node_id);
    let point_count = points.len();
    if point_count > u32::MAX as usize {
        return Err(EngineError::InvalidOperation(
            "dense HNSW currently supports at most u32::MAX points per segment".into(),
        ));
    }

    let levels: Vec<u8> = points
        .iter()
        .map(|point| assign_level(point.node_id, config.hnsw.m))
        .collect();

    // Allocate graph with per-node RwLocks for concurrent insertion.
    let graph: Vec<Vec<RwLock<Vec<usize>>>> = levels
        .iter()
        .map(|&level| {
            (0..=level as usize)
                .map(|current_level| {
                    RwLock::new(Vec::with_capacity(max_neighbors_for_level(
                        config.hnsw.m as usize,
                        current_level,
                    )))
                })
                .collect()
        })
        .collect();

    // Synchronized entry point state — packed into a single AtomicU64 so readers
    // always see a consistent (entry_point, max_level) pair.
    #[inline]
    fn pack_entry_state(ep: usize, ml: usize) -> u64 {
        ((ep as u64) << 32) | (ml as u64)
    }
    #[inline]
    fn unpack_entry_state(state: u64) -> (usize, usize) {
        ((state >> 32) as usize, (state & 0xFFFF_FFFF) as usize)
    }
    let entry_state = AtomicU64::new(pack_entry_state(0, levels[0] as usize));
    let entry_promote_lock = Mutex::new(());

    engine_cpu_install(|| {
        use rayon::prelude::*;

        (1..point_count).into_par_iter().for_each_init(
            || {
                (
                    BuildSearchScratch::new(point_count, config.hnsw.ef_construction as usize),
                    BuildPruneScratch::new(config.hnsw.ef_construction as usize),
                )
            },
            |(search_scratch, prune_scratch), point_idx| {
                let (ep, ml) = unpack_entry_state(entry_state.load(AtomicOrdering::Relaxed));
                let point_level = levels[point_idx] as usize;

                // Greedy descent through levels above point_level.
                let mut current = ep;
                let mut current_dist =
                    point_pair_distance(config.metric, &points[point_idx], &points[current]);
                for level in ((point_level + 1)..=ml).rev() {
                    loop {
                        let mut improved = false;
                        let neighbors = graph[current][level].read().unwrap();
                        for &neighbor in neighbors.iter() {
                            let dist = point_pair_distance(
                                config.metric,
                                &points[point_idx],
                                &points[neighbor],
                            );
                            if dist < current_dist {
                                current = neighbor;
                                current_dist = dist;
                                improved = true;
                            }
                        }
                        drop(neighbors);
                        if !improved {
                            break;
                        }
                    }
                }

                // Layer-by-layer search + link installation.
                let lower_max = point_level.min(ml);
                for level in (0..=lower_max).rev() {
                    let mut selected = search_layer_locked(
                        &points,
                        &graph,
                        point_idx,
                        current,
                        level,
                        config.hnsw.ef_construction as usize,
                        config.metric,
                        search_scratch,
                    );
                    let level_m = max_neighbors_for_level(config.hnsw.m as usize, level);
                    prune_scored_neighbors_with_scratch(
                        &points,
                        &mut selected,
                        point_idx,
                        level_m,
                        config.metric,
                        &mut prune_scratch.selected,
                        &mut prune_scratch.discarded,
                    );

                    // Install outgoing links (point_idx's list). Other threads may
                    // have already pushed incoming links here, so skip duplicates.
                    {
                        let mut my_list = graph[point_idx][level].write().unwrap();
                        for candidate in &selected {
                            if !my_list.contains(&candidate.point) {
                                my_list.push(candidate.point);
                            }
                        }
                        if my_list.len() > level_m {
                            prune_neighbors_with_scratch(
                                &points,
                                &mut my_list,
                                point_idx,
                                level_m,
                                config.metric,
                                prune_scratch,
                            );
                        }
                    }

                    // Install incoming links (each neighbor, one lock at a time).
                    for candidate in &selected {
                        let neighbor = candidate.point;
                        let mut neighbor_list = graph[neighbor][level].write().unwrap();
                        if !neighbor_list.contains(&point_idx) {
                            neighbor_list.push(point_idx);
                        }
                        if neighbor_list.len() > level_m {
                            prune_neighbors_with_scratch(
                                &points,
                                &mut neighbor_list,
                                neighbor,
                                level_m,
                                config.metric,
                                prune_scratch,
                            );
                        }
                    }

                    if let Some(best) = selected.first() {
                        current = best.point;
                    }
                }

                // Entry point promotion (rare — ~log(N) times).
                let (_, current_ml) = unpack_entry_state(entry_state.load(AtomicOrdering::Relaxed));
                if point_level > current_ml {
                    let _guard = entry_promote_lock.lock().unwrap();
                    let (_, guarded_ml) =
                        unpack_entry_state(entry_state.load(AtomicOrdering::Relaxed));
                    if point_level > guarded_ml {
                        entry_state.store(
                            pack_entry_state(point_idx, point_level),
                            AtomicOrdering::Relaxed,
                        );
                    }
                }
            },
        );
    });

    // Unwrap RwLocks after parallel build is complete.
    let (final_entry_point, final_max_level) =
        unpack_entry_state(entry_state.load(AtomicOrdering::Relaxed));
    let graph: Vec<Vec<Vec<usize>>> = graph
        .into_iter()
        .map(|levels| {
            levels
                .into_iter()
                .map(|lock| lock.into_inner().unwrap())
                .collect()
        })
        .collect();

    let mut level_offset = 0u64;
    let mut point_metas = Vec::with_capacity(point_count);
    for (point, &level) in points.iter().zip(levels.iter()) {
        point_metas.push(PointMeta {
            node_id: point.node_id,
            dense_vector_offset: point.dense_vector_offset,
            level_offset,
            max_level: level as u16,
        });
        for level_neighbors in &graph[point_metas.len() - 1] {
            level_offset = level_offset
                .checked_add(4)
                .and_then(|offset| offset.checked_add(level_neighbors.len() as u64 * 4))
                .ok_or_else(|| {
                    EngineError::CorruptRecord("dense HNSW graph offset overflow".into())
                })?;
        }
    }

    Ok(BuiltHnsw {
        header: DenseHnswHeader {
            point_count: point_count as u64,
            entry_point: final_entry_point as u32,
            max_level: final_max_level as u16,
            m: config.hnsw.m,
            ef_construction: config.hnsw.ef_construction,
            metric: config.metric,
            dimension: config.dimension,
        },
        point_metas,
        graph,
    })
}

fn max_neighbors_for_level(m: usize, level: usize) -> usize {
    if level == 0 {
        m.saturating_mul(2).max(1)
    } else {
        m.max(1)
    }
}

/// Search a single layer of the HNSW graph during concurrent build.
/// Reads neighbor lists through `RwLock` read guards.
#[allow(clippy::too_many_arguments)]
fn search_layer_locked(
    points: &[DensePoint],
    graph: &[Vec<RwLock<Vec<usize>>>],
    query_idx: usize,
    entry_point: usize,
    level: usize,
    ef: usize,
    metric: DenseMetric,
    scratch: &mut BuildSearchScratch,
) -> Vec<ScoredPoint> {
    let ef = ef.max(1);
    scratch.begin(ef);

    let entry_dist = point_pair_distance(metric, &points[query_idx], &points[entry_point]);
    scratch.mark_visited(entry_point);
    scratch.candidates.push(MinCandidate {
        dist: entry_dist,
        point: entry_point,
    });
    scratch.top.push(MaxCandidate {
        dist: entry_dist,
        point: entry_point,
    });

    while let Some(candidate) = scratch.candidates.pop() {
        let Some(farthest) = scratch.top.peek() else {
            break;
        };
        if candidate.dist > farthest.dist {
            break;
        }
        let neighbors = graph[candidate.point][level].read().unwrap();
        for &neighbor in neighbors.iter() {
            if !scratch.mark_visited(neighbor) {
                continue;
            }
            let dist = point_pair_distance(metric, &points[query_idx], &points[neighbor]);
            if scratch.top.len() < ef || dist < scratch.top.peek().unwrap().dist {
                scratch.candidates.push(MinCandidate {
                    dist,
                    point: neighbor,
                });
                scratch.top.push(MaxCandidate {
                    dist,
                    point: neighbor,
                });
                if scratch.top.len() > ef {
                    scratch.top.pop();
                }
            }
        }
        drop(neighbors);
    }

    while let Some(entry) = scratch.top.pop() {
        scratch.results.push(ScoredPoint {
            point: entry.point,
            dist: entry.dist,
        });
    }
    scratch.results.sort_unstable_by(|left, right| {
        left.dist
            .total_cmp(&right.dist)
            .then_with(|| left.point.cmp(&right.point))
    });
    std::mem::swap(&mut scratch.results, &mut scratch.return_buffer);
    scratch.results.clear();
    std::mem::take(&mut scratch.return_buffer)
}

fn prune_scored_neighbors_with_scratch(
    points: &[DensePoint],
    neighbors: &mut Vec<ScoredPoint>,
    _point_idx: usize,
    m: usize,
    metric: DenseMetric,
    selected_scratch: &mut Vec<ScoredPoint>,
    discarded_scratch: &mut Vec<ScoredPoint>,
) {
    if neighbors.is_empty() || m == 0 {
        neighbors.clear();
        return;
    }

    neighbors.sort_unstable_by(|left, right| {
        left.dist
            .total_cmp(&right.dist)
            .then_with(|| left.point.cmp(&right.point))
    });
    neighbors.dedup_by(|left, right| left.point == right.point);

    if neighbors.len() <= m {
        return;
    }

    selected_scratch.clear();
    discarded_scratch.clear();
    for &candidate in neighbors.iter() {
        let mut occluded = false;
        for selected_point in selected_scratch.iter() {
            let inter_dist = point_pair_distance(
                metric,
                &points[candidate.point],
                &points[selected_point.point],
            );
            if inter_dist < candidate.dist {
                occluded = true;
                break;
            }
        }
        if occluded {
            discarded_scratch.push(candidate);
            continue;
        }
        selected_scratch.push(candidate);
        if selected_scratch.len() == m {
            break;
        }
    }

    if selected_scratch.len() < m {
        for candidate in discarded_scratch.iter().copied() {
            if selected_scratch.len() == m {
                break;
            }
            selected_scratch.push(candidate);
        }
    }

    neighbors.clear();
    neighbors.extend_from_slice(selected_scratch);
}

#[cfg(test)]
fn prune_neighbors(
    points: &[DensePoint],
    neighbors: &mut Vec<usize>,
    point_idx: usize,
    m: usize,
    metric: DenseMetric,
) {
    let mut scratch = BuildPruneScratch::new(neighbors.len().max(m));
    prune_neighbors_with_scratch(points, neighbors, point_idx, m, metric, &mut scratch);
}

fn prune_neighbors_with_scratch(
    points: &[DensePoint],
    neighbors: &mut Vec<usize>,
    point_idx: usize,
    m: usize,
    metric: DenseMetric,
    scratch: &mut BuildPruneScratch,
) {
    scratch.reserve_for(neighbors.len());
    scratch.scored.clear();
    scratch
        .scored
        .extend(neighbors.iter().copied().map(|point| ScoredPoint {
            point,
            dist: point_pair_distance(metric, &points[point_idx], &points[point]),
        }));
    prune_scored_neighbors_with_scratch(
        points,
        &mut scratch.scored,
        point_idx,
        m,
        metric,
        &mut scratch.selected,
        &mut scratch.discarded,
    );
    neighbors.clear();
    neighbors.extend(scratch.scored.iter().map(|candidate| candidate.point));
}

fn assign_level(node_id: u64, m: u16) -> u8 {
    let mut x = splitmix64(node_id);
    let threshold = (u16::MAX as u32 / m.max(2) as u32) as u16;
    let mut level = 0u8;
    while level < MAX_HNSW_LEVEL && (x as u16) <= threshold {
        level += 1;
        x = splitmix64(x ^ node_id.rotate_left(level as u32));
    }
    level
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn distance(metric: DenseMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        DenseMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut left_norm = 0.0f32;
            let mut right_norm = 0.0f32;
            for (&l, &r) in left.iter().zip(right.iter()) {
                dot += l * r;
                left_norm += l * l;
                right_norm += r * r;
            }
            let denom = left_norm.sqrt() * right_norm.sqrt();
            if denom == 0.0 {
                1.0
            } else {
                1.0 - (dot / denom)
            }
        }
        DenseMetric::Euclidean => {
            let mut sum = 0.0f32;
            for (&l, &r) in left.iter().zip(right.iter()) {
                let diff = l - r;
                sum += diff * diff;
            }
            sum
        }
        DenseMetric::DotProduct => {
            let mut dot = 0.0f32;
            for (&l, &r) in left.iter().zip(right.iter()) {
                dot += l * r;
            }
            -dot
        }
    }
}

fn dense_vector_norm(values: &[f32]) -> f32 {
    values.iter().map(|value| value * value).sum::<f32>().sqrt()
}

fn point_pair_distance(metric: DenseMetric, left: &DensePoint, right: &DensePoint) -> f32 {
    match metric {
        DenseMetric::Cosine => {
            let mut dot = 0.0f32;
            for (&l, &r) in left.values.iter().zip(right.values.iter()) {
                dot += l * r;
            }
            let denom = left.norm * right.norm;
            if denom == 0.0 {
                1.0
            } else {
                1.0 - (dot / denom)
            }
        }
        DenseMetric::Euclidean => distance(metric, &left.values, &right.values),
        DenseMetric::DotProduct => distance(metric, &left.values, &right.values),
    }
}

#[cfg(test)]
pub(crate) fn dense_score(metric: DenseMetric, left: &[f32], right: &[f32]) -> f32 {
    score_from_distance(metric, distance(metric, left, right))
}

pub(crate) fn dense_query_norm(metric: DenseMetric, query: &[f32]) -> Option<f32> {
    (metric == DenseMetric::Cosine).then(|| dense_vector_norm(query))
}

pub(crate) fn dense_score_with_query_norm(
    metric: DenseMetric,
    query: &[f32],
    query_norm: Option<f32>,
    candidate: &[f32],
) -> f32 {
    match metric {
        DenseMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut candidate_norm = 0.0f32;
            for (&query_value, &candidate_value) in query.iter().zip(candidate.iter()) {
                dot += query_value * candidate_value;
                candidate_norm += candidate_value * candidate_value;
            }
            let denom = query_norm.unwrap_or(0.0) * candidate_norm.sqrt();
            if denom == 0.0 {
                0.0
            } else {
                dot / denom
            }
        }
        DenseMetric::Euclidean => {
            let mut sum = 0.0f32;
            for (&query_value, &candidate_value) in query.iter().zip(candidate.iter()) {
                let diff = query_value - candidate_value;
                sum += diff * diff;
            }
            -sum
        }
        DenseMetric::DotProduct => {
            let mut dot = 0.0f32;
            for (&query_value, &candidate_value) in query.iter().zip(candidate.iter()) {
                dot += query_value * candidate_value;
            }
            dot
        }
    }
}

pub(crate) fn dense_score_from_bytes(
    metric: DenseMetric,
    query: &[f32],
    query_norm: Option<f32>,
    dense_blob: &[u8],
    dense_offset: usize,
    dimension: usize,
) -> Result<f32, EngineError> {
    let point_values = dense_vector_f32_slice(dense_blob, dense_offset, dimension)?;
    Ok(dense_score_with_query_norm(
        metric,
        query,
        query_norm,
        point_values,
    ))
}

fn score_from_distance(metric: DenseMetric, distance: f32) -> f32 {
    match metric {
        DenseMetric::Cosine => 1.0 - distance,
        DenseMetric::Euclidean => -distance,
        DenseMetric::DotProduct => -distance,
    }
}

#[cfg(test)]
fn exact_dense_search(
    meta: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    limit: usize,
) -> Result<Vec<(u64, f32)>, EngineError> {
    exact_dense_search_with_query_norm(
        meta,
        dense_blob,
        query,
        metric,
        (metric == DenseMetric::Cosine).then(|| dense_vector_norm(query)),
        limit,
    )
}

#[cfg(test)]
fn exact_dense_search_with_query_norm(
    meta: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    query_norm: Option<f32>,
    limit: usize,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let point_count = (meta.len() - DENSE_HNSW_HEADER_SIZE) / DENSE_HNSW_POINT_META_SIZE;
    let mut hits = Vec::with_capacity(point_count);
    let dimension = query.len();
    for point_idx in 0..point_count {
        let point = read_point_meta(meta, point_idx)?;
        let distance =
            point_distance_from_meta(point, dense_blob, query, metric, query_norm, dimension)?;
        hits.push((point.node_id, score_from_distance(metric, distance)));
    }
    hits.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

fn exact_dense_search_with_points_and_query_norm(
    points: &[DenseQueryPoint],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    query_norm: Option<f32>,
    limit: usize,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let mut hits = Vec::with_capacity(points.len());
    let dimension = query.len();
    for &point in points {
        let distance =
            point_distance_from_meta(point, dense_blob, query, metric, query_norm, dimension)?;
        hits.push((point.node_id, score_from_distance(metric, distance)));
    }
    hits.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

pub(crate) fn exact_dense_search_above_cutoff(
    meta: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    min_score: f32,
    max_node_id_on_tie: u64,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let point_count = (meta.len() - DENSE_HNSW_HEADER_SIZE) / DENSE_HNSW_POINT_META_SIZE;
    let mut hits = Vec::with_capacity(point_count.min(128));
    let dimension = query.len();
    let query_norm = (metric == DenseMetric::Cosine).then(|| dense_vector_norm(query));
    for point_idx in 0..point_count {
        let point = read_point_meta(meta, point_idx)?;
        let distance =
            point_distance_from_meta(point, dense_blob, query, metric, query_norm, dimension)?;
        let score = score_from_distance(metric, distance);
        let beats_cutoff = score > min_score
            || (score.total_cmp(&min_score) == Ordering::Equal
                && point.node_id < max_node_id_on_tie);
        if beats_cutoff {
            hits.push((point.node_id, score));
        }
    }
    hits.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    Ok(hits)
}

#[allow(clippy::too_many_arguments)] // HNSW scoped search needs all index components inline
pub(crate) fn search_dense_hnsw_scoped_with_points(
    header: DenseHnswHeader,
    points: &[DenseQueryPoint],
    graph: &[u8],
    dense_blob: &[u8],
    query: &[f32],
    ef_search: usize,
    limit: usize,
    scope_ids: &NodeIdSet,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let point_count = header.point_count as usize;
    if point_count == 0 {
        return Ok(Vec::new());
    }
    if points.len() != point_count {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW point cache length {} does not match header count {}",
            points.len(),
            point_count
        )));
    }

    let limit = limit.min(point_count);
    let ef_search = ef_search.max(limit).max(1).min(point_count);
    let query_norm = (header.metric == DenseMetric::Cosine).then(|| dense_vector_norm(query));
    let dimension = header.dimension as usize;

    // Tiny segment: exact scan filtered by scope.
    if point_count <= ef_search {
        return exact_dense_search_scoped(
            points,
            dense_blob,
            query,
            header.metric,
            query_norm,
            limit,
            scope_ids,
        );
    }

    // --- Upper-level greedy navigation (unfiltered, just finds the entry region) ---
    let mut entry_point = header.entry_point as usize;
    let mut entry_distance = point_distance(
        points,
        dense_blob,
        query,
        header.metric,
        query_norm,
        dimension,
        entry_point,
    )?;

    for level in (1..=header.max_level as usize).rev() {
        loop {
            let mut improved = false;
            let (neighbors_start, neighbor_count) =
                level_neighbor_span_from_point(points[entry_point], graph, level)?;
            for neighbor_idx in 0..neighbor_count {
                let neighbor = read_u32_at(graph, neighbors_start + neighbor_idx * 4)? as usize;
                let neighbor_distance = point_distance(
                    points,
                    dense_blob,
                    query,
                    header.metric,
                    query_norm,
                    dimension,
                    neighbor,
                )?;
                if neighbor_distance < entry_distance {
                    entry_point = neighbor;
                    entry_distance = neighbor_distance;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
    }

    // --- Base-layer ACORN-1 search ---
    let mut visited = vec![0u64; point_count.div_ceil(64)];
    let mut candidates: BinaryHeap<MinCandidate> = BinaryHeap::new();
    let mut top: BinaryHeap<MaxCandidate> = BinaryHeap::new();

    mark_visited(&mut visited, entry_point);
    candidates.push(MinCandidate {
        dist: entry_distance,
        point: entry_point,
    });
    if scope_ids.contains(&points[entry_point].node_id) {
        top.push(MaxCandidate {
            dist: entry_distance,
            point: entry_point,
        });
    }

    while let Some(candidate) = candidates.pop() {
        // Stop only when we have ef_search in-scope results and the current
        // candidate is farther than the worst in-scope result.
        if top.len() >= ef_search {
            if let Some(farthest) = top.peek() {
                if candidate.dist > farthest.dist {
                    break;
                }
            }
        }

        let (neighbors_start, neighbor_count) =
            level_neighbor_span_from_point(points[candidate.point], graph, 0)?;
        for neighbor_idx in 0..neighbor_count {
            let neighbor = read_u32_at(graph, neighbors_start + neighbor_idx * 4)? as usize;
            if is_visited(&visited, neighbor) {
                continue;
            }
            mark_visited(&mut visited, neighbor);

            let neighbor_distance = point_distance(
                points,
                dense_blob,
                query,
                header.metric,
                query_norm,
                dimension,
                neighbor,
            )?;

            if scope_ids.contains(&points[neighbor].node_id) {
                // In-scope: standard HNSW candidate + result handling.
                if top.len() < ef_search || neighbor_distance < top.peek().unwrap().dist {
                    candidates.push(MinCandidate {
                        dist: neighbor_distance,
                        point: neighbor,
                    });
                    top.push(MaxCandidate {
                        dist: neighbor_distance,
                        point: neighbor,
                    });
                    if top.len() > ef_search {
                        top.pop();
                    }
                }
            } else {
                // Out-of-scope: add to exploration queue for connectivity,
                // then ACORN two-hop. Eagerly expand this node's base-layer
                // neighbors so the search can bridge across filtered regions.
                candidates.push(MinCandidate {
                    dist: neighbor_distance,
                    point: neighbor,
                });

                let (n2_start, n2_count) =
                    level_neighbor_span_from_point(points[neighbor], graph, 0)?;
                for n2_idx in 0..n2_count {
                    let n2 = read_u32_at(graph, n2_start + n2_idx * 4)? as usize;
                    if is_visited(&visited, n2) {
                        continue;
                    }
                    mark_visited(&mut visited, n2);

                    let n2_distance = point_distance(
                        points,
                        dense_blob,
                        query,
                        header.metric,
                        query_norm,
                        dimension,
                        n2,
                    )?;

                    if scope_ids.contains(&points[n2].node_id) {
                        if top.len() < ef_search || n2_distance < top.peek().unwrap().dist {
                            candidates.push(MinCandidate {
                                dist: n2_distance,
                                point: n2,
                            });
                            top.push(MaxCandidate {
                                dist: n2_distance,
                                point: n2,
                            });
                            if top.len() > ef_search {
                                top.pop();
                            }
                        }
                    } else {
                        // Second-hop out-of-scope: still queue for exploration
                        // but no further eager expansion (one hop is enough).
                        candidates.push(MinCandidate {
                            dist: n2_distance,
                            point: n2,
                        });
                    }
                }
            }
        }
    }

    let mut ranked: Vec<(usize, f32, u64)> = top
        .into_iter()
        .map(|entry| (entry.point, entry.dist, points[entry.point].node_id))
        .collect();
    ranked.sort_unstable_by(
        |(_, left_dist, left_node_id), (_, right_dist, right_node_id)| {
            left_dist
                .total_cmp(right_dist)
                .then_with(|| left_node_id.cmp(right_node_id))
        },
    );
    ranked.truncate(limit);

    let mut hits = Vec::with_capacity(ranked.len());
    for (_, distance, node_id) in ranked {
        hits.push((node_id, score_from_distance(header.metric, distance)));
    }
    Ok(hits)
}

/// Exact brute-force dense search filtered to only in-scope node IDs.
fn exact_dense_search_scoped(
    points: &[DenseQueryPoint],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    query_norm: Option<f32>,
    limit: usize,
    scope_ids: &NodeIdSet,
) -> Result<Vec<(u64, f32)>, EngineError> {
    let dimension = query.len();
    let mut hits = Vec::with_capacity(scope_ids.len().min(points.len()));
    for &point in points {
        if !scope_ids.contains(&point.node_id) {
            continue;
        }
        let distance =
            point_distance_from_meta(point, dense_blob, query, metric, query_norm, dimension)?;
        hits.push((point.node_id, score_from_distance(metric, distance)));
    }
    hits.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

pub(crate) fn load_dense_hnsw_query_points(
    meta: &[u8],
    header: DenseHnswHeader,
) -> Result<Vec<DenseQueryPoint>, EngineError> {
    let mut points = Vec::with_capacity(header.point_count as usize);
    for point_idx in 0..header.point_count as usize {
        let point = read_point_meta(meta, point_idx)?;
        points.push(DenseQueryPoint {
            node_id: point.node_id,
            dense_vector_offset: point.dense_vector_offset,
            level_offset: point.level_offset,
            max_level: point.max_level,
        });
    }
    Ok(points)
}

fn point_distance(
    points: &[DenseQueryPoint],
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    query_norm: Option<f32>,
    dimension: usize,
    point_idx: usize,
) -> Result<f32, EngineError> {
    let point = points.get(point_idx).copied().ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "dense HNSW point index {} out of range for cache length {}",
            point_idx,
            points.len()
        ))
    })?;
    point_distance_from_meta(point, dense_blob, query, metric, query_norm, dimension)
}

fn point_distance_from_meta(
    point: impl Into<PointMeta>,
    dense_blob: &[u8],
    query: &[f32],
    metric: DenseMetric,
    query_norm: Option<f32>,
    dimension: usize,
) -> Result<f32, EngineError> {
    let point = point.into();
    let point_values =
        dense_vector_f32_slice(dense_blob, point.dense_vector_offset as usize, dimension)?;
    match metric {
        DenseMetric::Cosine => {
            let score = dense_score_with_query_norm(metric, query, query_norm, point_values);
            Ok(1.0 - score)
        }
        DenseMetric::Euclidean => Ok(distance(metric, query, point_values)),
        DenseMetric::DotProduct => Ok(distance(metric, query, point_values)),
    }
}

fn dense_vector_bytes(
    dense_blob: &[u8],
    dense_offset: usize,
    dimension: usize,
) -> Result<&[u8], EngineError> {
    let bytes = dimension
        .checked_mul(DENSE_VECTOR_VALUE_SIZE)
        .ok_or_else(|| {
            EngineError::CorruptRecord("dense HNSW vector byte count overflow".into())
        })?;
    let dense_end = dense_offset.checked_add(bytes).ok_or_else(|| {
        EngineError::CorruptRecord("dense HNSW dense vector range overflow".into())
    })?;
    dense_blob.get(dense_offset..dense_end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "dense vector range [{}, {}) exceeds dense blob length {}",
            dense_offset,
            dense_end,
            dense_blob.len()
        ))
    })
}

#[cfg(target_endian = "little")]
fn dense_vector_f32_slice(
    dense_blob: &[u8],
    dense_offset: usize,
    dimension: usize,
) -> Result<&[f32], EngineError> {
    let point_values = dense_vector_bytes(dense_blob, dense_offset, dimension)?;
    let ptr = point_values.as_ptr();
    if !(ptr as usize).is_multiple_of(std::mem::align_of::<f32>()) {
        return Err(EngineError::CorruptRecord(format!(
            "dense vector offset {} is not aligned for f32",
            dense_offset
        )));
    }
    debug_assert_eq!(point_values.len(), dimension * DENSE_VECTOR_VALUE_SIZE);
    Ok(unsafe {
        // Safety:
        // - dense blobs are written as packed little-endian f32 values
        // - offsets start at 0 and advance in 4-byte steps
        // - the validated byte span is exactly `dimension * size_of::<f32>()`
        std::slice::from_raw_parts(ptr.cast::<f32>(), dimension)
    })
}

#[cfg(not(target_endian = "little"))]
fn dense_vector_f32_slice(
    dense_blob: &[u8],
    dense_offset: usize,
    dimension: usize,
) -> Result<&[f32], EngineError> {
    let _ = (dense_blob, dense_offset, dimension);
    Err(EngineError::CorruptRecord(
        "dense vector direct f32 view requires little-endian target".into(),
    ))
}

fn level_neighbor_span_from_point(
    point: impl Into<PointMeta>,
    graph: &[u8],
    level: usize,
) -> Result<(usize, usize), EngineError> {
    let point = point.into();
    if level > point.max_level as usize {
        return Ok((0, 0));
    }

    let mut cursor = point.level_offset as usize;
    for current_level in 0..=point.max_level as usize {
        let neighbor_count = read_u16_at(graph, cursor)? as usize;
        let neighbors_start = cursor.checked_add(4).ok_or_else(|| {
            EngineError::CorruptRecord("dense HNSW neighbor header overflow".into())
        })?;
        let neighbors_end = neighbors_start
            .checked_add(neighbor_count * 4)
            .ok_or_else(|| {
                EngineError::CorruptRecord("dense HNSW neighbor range overflow".into())
            })?;
        if current_level == level {
            return Ok((neighbors_start, neighbor_count));
        }
        cursor = neighbors_end;
    }

    Ok((0, 0))
}

impl From<DenseQueryPoint> for PointMeta {
    fn from(value: DenseQueryPoint) -> Self {
        Self {
            node_id: value.node_id,
            dense_vector_offset: value.dense_vector_offset,
            level_offset: value.level_offset,
            max_level: value.max_level,
        }
    }
}

fn write_hnsw_files(
    seg_dir: &Path,
    config: &DenseVectorConfig,
    built: &BuiltHnsw,
) -> Result<(), EngineError> {
    let meta_file = File::create(seg_dir.join(DENSE_HNSW_META_FILENAME))?;
    let graph_file = File::create(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME))?;
    let mut meta_w = BufWriter::new(meta_file);
    let mut graph_w = BufWriter::new(graph_file);

    meta_w.write_all(&DENSE_HNSW_MAGIC)?;
    write_u32(&mut meta_w, DENSE_HNSW_VERSION)?;
    write_u64(&mut meta_w, built.header.point_count)?;
    write_u32(&mut meta_w, built.header.entry_point)?;
    write_u16(&mut meta_w, built.header.max_level)?;
    write_u16(&mut meta_w, built.header.m)?;
    write_u16(&mut meta_w, built.header.ef_construction)?;
    write_u8(&mut meta_w, metric_to_u8(config.metric))?;
    write_u8(&mut meta_w, 0)?;
    write_u32(&mut meta_w, config.dimension)?;
    write_u32(&mut meta_w, 0)?;

    for point in &built.point_metas {
        write_u64(&mut meta_w, point.node_id)?;
        write_u64(&mut meta_w, point.dense_vector_offset)?;
        write_u64(&mut meta_w, point.level_offset)?;
        write_u16(&mut meta_w, point.max_level)?;
        write_u16(&mut meta_w, 0)?;
        write_u32(&mut meta_w, 0)?;
    }

    for levels in &built.graph {
        for neighbors in levels {
            write_u16(&mut graph_w, neighbors.len() as u16)?;
            write_u16(&mut graph_w, 0)?;
            for &neighbor in neighbors {
                write_u32(&mut graph_w, neighbor as u32)?;
            }
        }
    }

    meta_w.flush()?;
    meta_w.get_ref().sync_all()?;
    graph_w.flush()?;
    graph_w.get_ref().sync_all()?;
    Ok(())
}

fn read_header(data: &[u8]) -> Result<DenseHnswHeader, EngineError> {
    if data.len() < DENSE_HNSW_HEADER_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW metadata too short: {} bytes",
            data.len()
        )));
    }
    if data[0..4] != DENSE_HNSW_MAGIC {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW metadata has invalid magic {:?}",
            &data[0..4]
        )));
    }
    let version = read_u32_at(data, 4)?;
    if version != DENSE_HNSW_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "dense HNSW metadata version {} is unsupported (expected {})",
            version, DENSE_HNSW_VERSION
        )));
    }
    Ok(DenseHnswHeader {
        point_count: read_u64_at(data, 8)?,
        entry_point: read_u32_at(data, 16)?,
        max_level: read_u16_at(data, 20)?,
        m: read_u16_at(data, 22)?,
        ef_construction: read_u16_at(data, 24)?,
        metric: metric_from_u8(read_u8_at(data, 26)?)?,
        dimension: read_u32_at(data, 28)?,
    })
}

fn read_point_meta(data: &[u8], index: usize) -> Result<PointMeta, EngineError> {
    let off = DENSE_HNSW_HEADER_SIZE + index * DENSE_HNSW_POINT_META_SIZE;
    Ok(PointMeta {
        node_id: read_u64_at(data, off)?,
        dense_vector_offset: read_u64_at(data, off + 8)?,
        level_offset: read_u64_at(data, off + 16)?,
        max_level: read_u16_at(data, off + 24)?,
    })
}

fn metric_to_u8(metric: DenseMetric) -> u8 {
    match metric {
        DenseMetric::Cosine => 0,
        DenseMetric::Euclidean => 1,
        DenseMetric::DotProduct => 2,
    }
}

fn metric_from_u8(value: u8) -> Result<DenseMetric, EngineError> {
    match value {
        0 => Ok(DenseMetric::Cosine),
        1 => Ok(DenseMetric::Euclidean),
        2 => Ok(DenseMetric::DotProduct),
        _ => Err(EngineError::CorruptRecord(format!(
            "invalid dense HNSW metric tag {}",
            value
        ))),
    }
}

fn write_u8(w: &mut impl Write, value: u8) -> Result<(), EngineError> {
    w.write_all(&[value])?;
    Ok(())
}

fn write_u16(w: &mut impl Write, value: u16) -> Result<(), EngineError> {
    w.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u32(w: &mut impl Write, value: u32) -> Result<(), EngineError> {
    w.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u64(w: &mut impl Write, value: u64) -> Result<(), EngineError> {
    w.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u8_at(data: &[u8], offset: usize) -> Result<u8, EngineError> {
    data.get(offset).copied().ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u8 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })
}

fn read_u16_at(data: &[u8], offset: usize) -> Result<u16, EngineError> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| EngineError::CorruptRecord("dense HNSW u16 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u16 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("dense HNSW u32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u32 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| EngineError::CorruptRecord("dense HNSW u64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u64 read at offset {} exceeds data length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_writer::{
        write_segment_without_degree_sidecar_for_test as write_segment,
        NODE_DENSE_VECTOR_BLOB_FILENAME,
    };
    use crate::types::NodeIdSet;
    use crate::types::{DenseMetric, HnswConfig, NodeRecord, WalOp, DEFAULT_DENSE_EF_SEARCH};
    use crate::{memtable::Memtable, types::DenseVectorConfig};
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::time::Instant;

    fn dense_config(dimension: u32) -> DenseVectorConfig {
        DenseVectorConfig {
            dimension,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }
    }

    fn node(id: u64, key: &str, dense: Vec<f32>) -> NodeRecord {
        NodeRecord {
            id,
            type_id: 1,
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 2,
            weight: 0.5,
            dense_vector: Some(dense),
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn normalize_vector(values: &mut [f32]) {
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm > 0.0 {
            for value in values.iter_mut() {
                *value /= norm;
            }
        }
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
        let mut values =
            clustered_dense_vector(dimension, cluster, query_idx + 100_000, cluster_count);
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

    fn random_unit_vector(dimension: usize, seed: u64) -> Vec<f32> {
        let mut values = vec![0.0f32; dimension];
        let mut state = splitmix64(seed);
        for value in &mut values {
            state = splitmix64(state);
            let centered = ((state >> 32) as u32) as f32 / u32::MAX as f32;
            *value = (centered * 2.0) - 1.0;
        }
        normalize_vector(&mut values);
        values
    }

    fn near_duplicate_dense_vector(
        dimension: usize,
        cluster: usize,
        member: usize,
        cluster_count: usize,
    ) -> Vec<f32> {
        let mut values = vec![0.0f32; dimension];
        let primary = (cluster * 13) % dimension;
        let secondary = (cluster * 13 + 5) % dimension;
        let tertiary = (cluster * 13 + 17) % dimension;
        values[primary] = 1.0;
        values[secondary] = 0.18;
        values[tertiary] = 0.08;

        let seed =
            ((cluster as u64) << 32) ^ member as u64 ^ (cluster_count as u64).rotate_left(11);
        let mut noise = splitmix64(seed);
        for value in &mut values {
            noise = splitmix64(noise);
            let jitter = ((noise >> 44) as i32 - 512) as f32 / 65_536.0;
            *value += jitter * 0.03;
        }
        normalize_vector(&mut values);
        values
    }

    fn near_duplicate_query_vector(
        dimension: usize,
        cluster: usize,
        query_idx: usize,
        cluster_count: usize,
    ) -> Vec<f32> {
        let mut values =
            near_duplicate_dense_vector(dimension, cluster, query_idx + 500_000, cluster_count);
        if query_idx % 3 == 1 {
            let adjacent = near_duplicate_dense_vector(
                dimension,
                (cluster + 1) % cluster_count,
                query_idx + 700_000,
                cluster_count,
            );
            for (value, adjacent_value) in values.iter_mut().zip(adjacent.iter()) {
                *value = (*value * 0.92) + (*adjacent_value * 0.08);
            }
            normalize_vector(&mut values);
        }
        values
    }

    fn env_usize(name: &str, default: usize) -> usize {
        env::var(name)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(default)
    }

    fn env_u16(name: &str, default: u16) -> u16 {
        env::var(name)
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(default)
    }

    struct DenseRecallBenchmarkMetrics {
        build_ms: f64,
        recall_avg: f32,
        recall_min: f32,
        recall_p50: f32,
        recall_p95: f32,
        ann_p50_us: f64,
        ann_p95_us: f64,
        exact_p50_us: f64,
        exact_p95_us: f64,
    }

    fn run_dense_hnsw_recall_benchmark(
        benchmark_name: &str,
        dataset_label: &str,
        dimension: usize,
        dataset: &[Vec<f32>],
        queries: &[Vec<f32>],
    ) -> DenseRecallBenchmarkMetrics {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let hnsw_defaults = HnswConfig::default();
        let k = env_usize("OVERGRAPH_DENSE_HNSW_K", 10);
        let ef_search = env_usize("OVERGRAPH_DENSE_HNSW_EF_SEARCH", DEFAULT_DENSE_EF_SEARCH);
        let m = env_u16("OVERGRAPH_DENSE_HNSW_M", hnsw_defaults.m);
        let ef_construction = env_u16(
            "OVERGRAPH_DENSE_HNSW_EF_CONSTRUCTION",
            hnsw_defaults.ef_construction,
        );
        let config = DenseVectorConfig {
            dimension: dimension as u32,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig { m, ef_construction },
        };

        let mt = Memtable::new();
        for (index, values) in dataset.iter().enumerate() {
            mt.apply_op(
                &WalOp::UpsertNode(node(
                    (index + 1) as u64,
                    &format!("{benchmark_name}_n{index}"),
                    values.clone(),
                )),
                0,
            );
        }

        let build_started = Instant::now();
        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();
        let build_elapsed = build_started.elapsed();

        let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
        let graph = fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap();
        let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();
        let header = read_header(&meta).unwrap();
        let points = load_dense_hnsw_query_points(&meta, header).unwrap();

        let mut recalls = Vec::with_capacity(queries.len());
        let mut ann_query_micros = Vec::with_capacity(queries.len());
        let mut exact_query_micros = Vec::with_capacity(queries.len());

        for query in queries {
            let ann_started = Instant::now();
            let ann_hits = search_dense_hnsw_with_points(
                header,
                &points,
                &graph,
                &dense_blob,
                query,
                ef_search,
                k,
            )
            .unwrap();
            ann_query_micros.push(ann_started.elapsed().as_secs_f64() * 1_000_000.0);

            let exact_started = Instant::now();
            let exact_hits =
                exact_dense_search(&meta, &dense_blob, query, DenseMetric::Cosine, k).unwrap();
            exact_query_micros.push(exact_started.elapsed().as_secs_f64() * 1_000_000.0);

            let exact_top_ids: NodeIdSet = exact_hits.iter().map(|(node_id, _)| *node_id).collect();
            let overlap = ann_hits
                .iter()
                .filter(|(node_id, _)| exact_top_ids.contains(node_id))
                .count();
            recalls.push(overlap as f32 / k as f32);
        }

        recalls.sort_unstable_by(|left, right| left.total_cmp(right));
        ann_query_micros.sort_unstable_by(|left, right| left.total_cmp(right));
        exact_query_micros.sort_unstable_by(|left, right| left.total_cmp(right));

        let metrics = DenseRecallBenchmarkMetrics {
            build_ms: build_elapsed.as_secs_f64() * 1_000.0,
            recall_avg: recalls.iter().sum::<f32>() / recalls.len() as f32,
            recall_min: recalls[0],
            recall_p50: recalls[recalls.len() / 2],
            recall_p95: recalls[((recalls.len() * 95).div_ceil(100)).saturating_sub(1)],
            ann_p50_us: ann_query_micros[ann_query_micros.len() / 2],
            ann_p95_us: ann_query_micros
                [((ann_query_micros.len() * 95).div_ceil(100)).saturating_sub(1)],
            exact_p50_us: exact_query_micros[exact_query_micros.len() / 2],
            exact_p95_us: exact_query_micros
                [((exact_query_micros.len() * 95).div_ceil(100)).saturating_sub(1)],
        };

        println!(
            "dense_hnsw_{}_recall dataset={} queries={} k={} ef_search={} m={} ef_construction={} build_ms={:.2} recall_avg={:.4} recall_min={:.4} recall_p50={:.4} recall_p95={:.4} ann_p50_us={:.2} ann_p95_us={:.2} exact_p50_us={:.2} exact_p95_us={:.2}",
            benchmark_name,
            dataset_label,
            queries.len(),
            k,
            ef_search,
            config.hnsw.m,
            config.hnsw.ef_construction,
            metrics.build_ms,
            metrics.recall_avg,
            metrics.recall_min,
            metrics.recall_p50,
            metrics.recall_p95,
            metrics.ann_p50_us,
            metrics.ann_p95_us,
            metrics.exact_p50_us,
            metrics.exact_p95_us,
        );

        metrics
    }

    fn assert_hnsw_graph_invariants(built: &BuiltHnsw, m: usize) {
        let point_count = built.header.point_count as usize;

        // Entry point has the highest level.
        let entry = built.header.entry_point as usize;
        let entry_max_level = built.point_metas[entry].max_level as usize;
        for (idx, meta) in built.point_metas.iter().enumerate() {
            assert!(
                meta.max_level as usize <= entry_max_level,
                "point {idx} has level {} > entry point level {entry_max_level}",
                meta.max_level
            );
        }

        // Structure: bounds, range, no self-loops, no duplicates.
        for (point_idx, point_levels) in built.graph.iter().enumerate() {
            for (level, neighbors) in point_levels.iter().enumerate() {
                let level_m = max_neighbors_for_level(m, level);
                assert!(
                    neighbors.len() <= level_m,
                    "point {point_idx} level {level}: {} neighbors > max {level_m}",
                    neighbors.len()
                );
                for &neighbor in neighbors {
                    assert!(
                        neighbor < point_count,
                        "point {point_idx} level {level}: neighbor {neighbor} out of range"
                    );
                    assert!(
                        neighbor != point_idx,
                        "point {point_idx} level {level}: self-loop detected"
                    );
                }
                // No duplicates.
                let mut sorted = neighbors.clone();
                sorted.sort_unstable();
                sorted.dedup();
                assert_eq!(
                    sorted.len(),
                    neighbors.len(),
                    "point {point_idx} level {level}: duplicate neighbors"
                );
            }
        }
    }

    fn valid_dense_hnsw_files() -> (DenseVectorConfig, Vec<u8>, Vec<u8>, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 8,
                ef_construction: 64,
            },
        };

        let mt = Memtable::new();
        for (index, values) in [
            vec![1.0, 0.0],
            vec![0.95, 0.05],
            vec![0.0, 1.0],
            vec![0.05, 0.95],
        ]
        .into_iter()
        .enumerate()
        {
            mt.apply_op(
                &WalOp::UpsertNode(node((index + 1) as u64, &format!("valid-n{index}"), values)),
                0,
            );
        }

        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();
        (
            config,
            fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap(),
            fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap(),
            fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap(),
        )
    }

    fn first_neighbor_offset(meta: &[u8], graph: &[u8]) -> (DenseHnswHeader, usize) {
        let header = read_header(meta).unwrap();
        for index in 0..header.point_count as usize {
            let point = read_point_meta(meta, index).unwrap();
            let mut cursor = point.level_offset as usize;
            for _ in 0..=point.max_level as usize {
                let neighbor_count = read_u16_at(graph, cursor).unwrap() as usize;
                let neighbor_base = cursor + 4;
                if neighbor_count > 0 {
                    return (header, neighbor_base);
                }
                cursor = neighbor_base + neighbor_count * 4;
            }
        }
        panic!("expected at least one neighbor entry in valid HNSW graph");
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_truncated_metadata() {
        let (config, mut meta, graph, dense_blob) = valid_dense_hnsw_files();
        meta.truncate(DENSE_HNSW_HEADER_SIZE - 1);

        match validate_dense_hnsw_files(&meta, &graph, dense_blob.len(), 4, Some(&config)) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("too short"));
            }
            other => panic!("expected truncated metadata error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_invalid_magic() {
        let (config, mut meta, graph, dense_blob) = valid_dense_hnsw_files();
        meta[0..4].copy_from_slice(b"BAD!");

        match validate_dense_hnsw_files(&meta, &graph, dense_blob.len(), 4, Some(&config)) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("invalid magic"));
            }
            other => panic!("expected invalid magic error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_invalid_version() {
        let (config, mut meta, graph, dense_blob) = valid_dense_hnsw_files();
        meta[4..8].copy_from_slice(&(DENSE_HNSW_VERSION + 1).to_le_bytes());

        match validate_dense_hnsw_files(&meta, &graph, dense_blob.len(), 4, Some(&config)) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("unsupported"));
            }
            other => panic!("expected invalid version error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_out_of_range_neighbor() {
        let (config, meta, mut graph, dense_blob) = valid_dense_hnsw_files();
        let (header, neighbor_offset) = first_neighbor_offset(&meta, &graph);
        graph[neighbor_offset..neighbor_offset + 4]
            .copy_from_slice(&(header.point_count as u32).to_le_bytes());

        match validate_dense_hnsw_files(
            &meta,
            &graph,
            dense_blob.len(),
            header.point_count as usize,
            Some(&config),
        ) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("out-of-range neighbor"));
            }
            other => panic!("expected out-of-range neighbor error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_noncontiguous_level_offset() {
        let (config, mut meta, graph, dense_blob) = valid_dense_hnsw_files();
        let level_offset_off = DENSE_HNSW_HEADER_SIZE + DENSE_HNSW_POINT_META_SIZE + 16;
        meta[level_offset_off..level_offset_off + 8].copy_from_slice(&1u64.to_le_bytes());

        match validate_dense_hnsw_files(&meta, &graph, dense_blob.len(), 4, Some(&config)) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("level offset"));
            }
            other => panic!("expected level offset error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_dense_hnsw_rejects_trailing_graph_bytes() {
        let (config, meta, mut graph, dense_blob) = valid_dense_hnsw_files();
        graph.push(0);

        match validate_dense_hnsw_files(&meta, &graph, dense_blob.len(), 4, Some(&config)) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("trailing or unreferenced bytes"));
            }
            other => panic!("expected trailing graph bytes error, got {:?}", other),
        }
    }

    #[test]
    fn test_dense_hnsw_writer_produces_high_quality_graph() {
        // Concurrent HNSW build is non-deterministic (insertion order varies),
        // so we verify quality rather than byte-identical output.
        let config = dense_config(3);

        let mt = Memtable::new();
        for index in 0..64 {
            let x = 1.0 - index as f32 * 0.01;
            let y = index as f32 * 0.01;
            let z = 0.5 + (index as f32 * 0.007).sin();
            mt.apply_op(
                &WalOp::UpsertNode(node(
                    (index + 1) as u64,
                    &format!("n{index}"),
                    vec![x, y, z],
                )),
                0,
            );
        }

        // Build twice and verify both produce valid, high-recall graphs.
        for run in 0..2 {
            let dir = tempfile::tempdir().unwrap();
            let seg_dir = dir.path().join("seg_0001");
            write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();

            let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
            let graph = fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap();
            let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();

            let mut total_recall = 0.0f32;
            let query_count = 8;
            for qi in 0..query_count {
                let query = vec![1.0 - qi as f32 * 0.08, qi as f32 * 0.08, 0.5];
                let ann_hits =
                    search_dense_hnsw(&meta, &graph, &dense_blob, &query, 32, 5).unwrap();
                let exact_hits =
                    exact_dense_search(&meta, &dense_blob, &query, DenseMetric::Cosine, 5).unwrap();
                let exact_top_ids: NodeIdSet =
                    exact_hits.iter().map(|(node_id, _)| *node_id).collect();
                let overlap = ann_hits
                    .iter()
                    .filter(|(node_id, _)| exact_top_ids.contains(node_id))
                    .count();
                total_recall += overlap as f32 / exact_hits.len() as f32;
            }
            let avg_recall = total_recall / query_count as f32;
            assert!(
                avg_recall >= 0.8,
                "run {run}: expected average recall >= 0.8, got {avg_recall}"
            );
        }

        // Also verify graph invariants via a direct build.
        let points: Vec<DensePoint> = (0..64)
            .map(|index| {
                let values = vec![
                    1.0 - index as f32 * 0.01,
                    index as f32 * 0.01,
                    0.5 + (index as f32 * 0.007).sin(),
                ];
                let norm = dense_vector_norm(&values);
                DensePoint {
                    node_id: (index + 1) as u64,
                    dense_vector_offset: 0,
                    values,
                    norm,
                }
            })
            .collect();
        let built = build_hnsw(points, &config).unwrap();
        assert_hnsw_graph_invariants(&built, config.hnsw.m as usize);
    }

    #[test]
    fn test_search_dense_hnsw_ann_sanity_against_exact_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 8,
                ef_construction: 64,
            },
        };

        let mt = Memtable::new();
        for index in 0..12 {
            mt.apply_op(
                &WalOp::UpsertNode(node(
                    (index + 1) as u64,
                    &format!("n{index}"),
                    vec![1.0 - index as f32 * 0.05, index as f32 * 0.05],
                )),
                0,
            );
        }

        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();

        let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
        let graph = fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap();
        let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();
        let query = vec![1.0, 0.0];

        let ann_hits = search_dense_hnsw(&meta, &graph, &dense_blob, &query, 8, 4).unwrap();
        let exact_hits =
            exact_dense_search(&meta, &dense_blob, &query, DenseMetric::Cosine, 8).unwrap();

        assert_eq!(ann_hits.len(), 4);
        assert_eq!(ann_hits[0].0, exact_hits[0].0);
        let exact_top_ids: NodeIdSet = exact_hits.iter().map(|(node_id, _)| *node_id).collect();
        assert!(ann_hits
            .iter()
            .all(|(node_id, _)| exact_top_ids.contains(node_id)));
    }

    #[test]
    fn test_search_dense_hnsw_recall_target_harness_on_line_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 16,
                ef_construction: 128,
            },
        };

        let mt = Memtable::new();
        for index in 0..256 {
            let x = 1.0 - index as f32 * 0.0025;
            let y = index as f32 * 0.0025;
            mt.apply_op(
                &WalOp::UpsertNode(node((index + 1) as u64, &format!("n{index}"), vec![x, y])),
                0,
            );
        }

        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();

        let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
        let graph = fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap();
        let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();
        let mut total_recall = 0.0f32;

        for index in [8usize, 24, 32, 48, 64, 96, 128, 160, 192, 208, 224, 240] {
            let query = vec![1.0 - index as f32 * 0.0025, index as f32 * 0.0025];

            let ann_hits = search_dense_hnsw(&meta, &graph, &dense_blob, &query, 64, 10).unwrap();
            let exact_hits =
                exact_dense_search(&meta, &dense_blob, &query, DenseMetric::Cosine, 10).unwrap();

            let exact_top_ids: NodeIdSet = exact_hits.iter().map(|(node_id, _)| *node_id).collect();
            let overlap = ann_hits
                .iter()
                .filter(|(node_id, _)| exact_top_ids.contains(node_id))
                .count();
            let recall = overlap as f32 / exact_hits.len() as f32;
            total_recall += recall;
        }

        // Cosine similarity on line-shaped vectors creates many near-ties around the
        // top-k boundary, so average recall is a more stable quality signal than a
        // per-query floor for this approximate search harness.
        let average_recall = total_recall / 12.0;
        assert!(
            average_recall >= 0.85,
            "expected average ANN recall >= 0.85, got {}",
            average_recall
        );
    }

    #[test]
    #[ignore = "benchmark-style dense recall harness for clustered high-dimensional data"]
    fn benchmark_dense_hnsw_recall_clustered_64d() {
        let dimension = 64usize;
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let query_count = 48usize;
        let mut dataset = Vec::with_capacity(cluster_count * points_per_cluster);
        for cluster in 0..cluster_count {
            for member in 0..points_per_cluster {
                dataset.push(clustered_dense_vector(
                    dimension,
                    cluster,
                    member,
                    cluster_count,
                ));
            }
        }
        let mut queries = Vec::with_capacity(query_count);
        for query_idx in 0..query_count {
            let cluster = query_idx % cluster_count;
            queries.push(clustered_query_vector(
                dimension,
                cluster,
                query_idx,
                cluster_count,
            ));
        }

        let metrics = run_dense_hnsw_recall_benchmark(
            "clustered",
            &format!("{}x{} clusters={}", dataset.len(), dimension, cluster_count),
            dimension,
            &dataset,
            &queries,
        );

        assert_eq!(cluster_count * points_per_cluster, 9_216);
        assert_eq!(queries.len(), query_count);
        assert!(metrics.recall_avg >= 0.0);
    }

    #[test]
    #[ignore = "benchmark-style dense recall harness for uniform random high-dimensional data"]
    fn benchmark_dense_hnsw_recall_uniform_64d() {
        let dimension = 64usize;
        let point_count = 9_216usize;
        let query_count = 48usize;
        let dataset: Vec<Vec<f32>> = (0..point_count)
            .map(|index| random_unit_vector(dimension, index as u64 + 17))
            .collect();
        let queries: Vec<Vec<f32>> = (0..query_count)
            .map(|index| random_unit_vector(dimension, 1_000_000 + index as u64))
            .collect();

        let metrics = run_dense_hnsw_recall_benchmark(
            "uniform",
            &format!("{}x{} random_unit", point_count, dimension),
            dimension,
            &dataset,
            &queries,
        );

        assert_eq!(dataset.len(), point_count);
        assert_eq!(queries.len(), query_count);
        assert!(metrics.recall_avg >= 0.0);
    }

    #[test]
    #[ignore = "benchmark-style dense recall harness for near-duplicate clustered data"]
    fn benchmark_dense_hnsw_recall_near_duplicate_64d() {
        let dimension = 64usize;
        let cluster_count = 24usize;
        let points_per_cluster = 384usize;
        let query_count = 48usize;
        let mut dataset = Vec::with_capacity(cluster_count * points_per_cluster);
        for cluster in 0..cluster_count {
            for member in 0..points_per_cluster {
                dataset.push(near_duplicate_dense_vector(
                    dimension,
                    cluster,
                    member,
                    cluster_count,
                ));
            }
        }
        let mut queries = Vec::with_capacity(query_count);
        for query_idx in 0..query_count {
            let cluster = query_idx % cluster_count;
            queries.push(near_duplicate_query_vector(
                dimension,
                cluster,
                query_idx,
                cluster_count,
            ));
        }

        let metrics = run_dense_hnsw_recall_benchmark(
            "near_duplicate",
            &format!("{}x{} clusters={}", dataset.len(), dimension, cluster_count),
            dimension,
            &dataset,
            &queries,
        );

        assert_eq!(cluster_count * points_per_cluster, 9_216);
        assert_eq!(queries.len(), query_count);
        assert!(metrics.recall_avg >= 0.0);
    }

    #[test]
    fn test_exact_dense_search_above_cutoff_respects_score_and_tiebreak() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let config = DenseVectorConfig {
            dimension: 2,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 8,
                ef_construction: 64,
            },
        };

        let mt = Memtable::new();
        mt.apply_op(
            &WalOp::UpsertNode(node(7, "winner-on-tie", vec![1.0, 0.0])),
            0,
        );
        mt.apply_op(
            &WalOp::UpsertNode(node(11, "same-score-loses-tie", vec![1.0, 0.0])),
            0,
        );
        mt.apply_op(
            &WalOp::UpsertNode(node(13, "below-cutoff", vec![0.9, 0.1])),
            0,
        );
        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();

        let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
        let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();

        let hits = exact_dense_search_above_cutoff(
            &meta,
            &dense_blob,
            &[1.0, 0.0],
            DenseMetric::Cosine,
            1.0,
            11,
        )
        .unwrap();
        let ids: Vec<u64> = hits.iter().map(|(node_id, _)| *node_id).collect();
        assert_eq!(ids, vec![7]);
    }

    #[test]
    fn test_max_neighbors_for_level_uses_double_width_on_base_layer() {
        assert_eq!(max_neighbors_for_level(16, 0), 32);
        assert_eq!(max_neighbors_for_level(16, 1), 16);
        assert_eq!(max_neighbors_for_level(1, 0), 2);
    }

    #[test]
    fn test_prune_neighbors_prefers_diverse_candidates() {
        let points = vec![
            DensePoint {
                node_id: 1,
                dense_vector_offset: 0,
                values: vec![0.0, 0.0],
                norm: dense_vector_norm(&[0.0, 0.0]),
            },
            DensePoint {
                node_id: 2,
                dense_vector_offset: 8,
                values: vec![0.1, 0.0],
                norm: dense_vector_norm(&[0.1, 0.0]),
            },
            DensePoint {
                node_id: 3,
                dense_vector_offset: 16,
                values: vec![0.11, 0.0],
                norm: dense_vector_norm(&[0.11, 0.0]),
            },
            DensePoint {
                node_id: 4,
                dense_vector_offset: 24,
                values: vec![0.0, 1.0],
                norm: dense_vector_norm(&[0.0, 1.0]),
            },
        ];
        let mut neighbors = vec![1, 2, 3];

        prune_neighbors(&points, &mut neighbors, 0, 2, DenseMetric::Euclidean);

        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains(&1));
        assert!(neighbors.contains(&3));
        assert!(!neighbors.contains(&2));
    }

    #[test]
    fn test_concurrent_hnsw_build_graph_invariants() {
        let config = DenseVectorConfig {
            dimension: 8,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig {
                m: 8,
                ef_construction: 64,
            },
        };

        let point_count = 512usize;
        let points: Vec<DensePoint> = (0..point_count)
            .map(|index| {
                let values: Vec<f32> = (0..8)
                    .map(|dim| ((index * 7 + dim * 13) as f32).sin())
                    .collect();
                let norm = dense_vector_norm(&values);
                DensePoint {
                    node_id: (index + 1) as u64,
                    dense_vector_offset: (index * 32) as u64,
                    values,
                    norm,
                }
            })
            .collect();

        let built = build_hnsw(points, &config).unwrap();
        assert_hnsw_graph_invariants(&built, config.hnsw.m as usize);

        // Verify the built graph can answer a representative query without panicking.
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_invariants");
        let mt = Memtable::new();
        for index in 0..point_count {
            let values: Vec<f32> = (0..8)
                .map(|dim| ((index * 7 + dim * 13) as f32).sin())
                .collect();
            mt.apply_op(
                &WalOp::UpsertNode(node((index + 1) as u64, &format!("inv{index}"), values)),
                0,
            );
        }
        write_segment(&seg_dir, 1, &mt, Some(&config)).unwrap();

        let meta = fs::read(seg_dir.join(DENSE_HNSW_META_FILENAME)).unwrap();
        let graph_bytes = fs::read(seg_dir.join(DENSE_HNSW_GRAPH_FILENAME)).unwrap();
        let dense_blob = fs::read(seg_dir.join(NODE_DENSE_VECTOR_BLOB_FILENAME)).unwrap();

        let query = vec![0.5f32; 8];
        let ann_hits = search_dense_hnsw(&meta, &graph_bytes, &dense_blob, &query, 32, 10).unwrap();
        assert_eq!(ann_hits.len(), 10);
    }
}
