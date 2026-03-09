// Graph operations: neighbors, traversal, top-k, subgraph, degree, shortest path, BFS, Dijkstra.
// This file is include!()'d into mod.rs — all items share the engine module scope.

/// Newtype wrapper for f64 that implements Ord (for BinaryHeap).
/// Only used with non-negative values (negative weights are rejected).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

const PATH_COST_EPSILON: f64 = 1e-10;

fn path_cost_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= PATH_COST_EPSILON
}

fn path_cost_lt(a: f64, b: f64) -> bool {
    a + PATH_COST_EPSILON < b
}

fn path_cost_le(a: f64, b: f64) -> bool {
    a <= b + PATH_COST_EPSILON
}

// Internal aliases over the public NodeIdBuildHasher for transient working sets.
// Public return types use NodeIdMap<V> directly (from types.rs).
type IdBuildHasher = NodeIdBuildHasher;
type IdMap<V> = HashMap<u64, V, IdBuildHasher>;
type IdSet = HashSet<u64, IdBuildHasher>;

#[derive(Default)]
struct SearchNeighborScratch {
    seen_edges: IdSet,
    visible_cache: IdMap<bool>,
}

#[derive(Clone, Copy)]
struct LayerBestEntry {
    hops: u32,
    exact_cost: f64,
    best_cost: f64,
    best_hops: u32,
}

/// Query-local tombstone view for traversal APIs.
///
/// Small traversals pay only for the node/edge IDs they actually touch. If the
/// search grows large, the view promotes itself to a fully materialized union
/// of tombstones for O(1) membership checks thereafter.
struct TraversalTombstoneView<'a> {
    memtable_deleted_nodes: &'a HashMap<u64, i64>,
    memtable_deleted_edges: &'a HashMap<u64, i64>,
    segments: &'a [SegmentReader],
    deleted_nodes: Option<IdSet>,
    deleted_edges: Option<IdSet>,
    node_cache: IdMap<bool>,
    edge_cache: IdMap<bool>,
    membership_checks: usize,
}

impl<'a> TraversalTombstoneView<'a> {
    const MATERIALIZE_THRESHOLD: usize = 2048;

    fn new(memtable: &'a Memtable, segments: &'a [SegmentReader]) -> Self {
        Self {
            memtable_deleted_nodes: memtable.deleted_nodes(),
            memtable_deleted_edges: memtable.deleted_edges(),
            segments,
            deleted_nodes: None,
            deleted_edges: None,
            node_cache: IdMap::default(),
            edge_cache: IdMap::default(),
            membership_checks: 0,
        }
    }

    fn is_node_deleted(&mut self, id: u64) -> bool {
        if let Some(deleted_nodes) = self.deleted_nodes.as_ref() {
            return deleted_nodes.contains(&id);
        }
        if let Some(&deleted) = self.node_cache.get(&id) {
            return deleted;
        }

        let deleted = self.memtable_deleted_nodes.contains_key(&id)
            || self.segments.iter().any(|seg| seg.is_node_deleted(id));
        self.node_cache.insert(id, deleted);
        self.membership_checks += 1;
        self.maybe_materialize();
        deleted
    }

    fn is_edge_deleted(&mut self, id: u64) -> bool {
        if let Some(deleted_edges) = self.deleted_edges.as_ref() {
            return deleted_edges.contains(&id);
        }
        if let Some(&deleted) = self.edge_cache.get(&id) {
            return deleted;
        }

        let deleted = self.memtable_deleted_edges.contains_key(&id)
            || self.segments.iter().any(|seg| seg.is_edge_deleted(id));
        self.edge_cache.insert(id, deleted);
        self.membership_checks += 1;
        self.maybe_materialize();
        deleted
    }

    fn maybe_materialize(&mut self) {
        if self.deleted_nodes.is_some()
            || self.membership_checks < Self::MATERIALIZE_THRESHOLD
            || self.segments.is_empty()
        {
            return;
        }

        let mut deleted_nodes: IdSet = self.memtable_deleted_nodes.keys().copied().collect();
        let mut deleted_edges: IdSet = self.memtable_deleted_edges.keys().copied().collect();
        for seg in self.segments {
            deleted_nodes.extend(seg.deleted_node_ids().iter().copied());
            deleted_edges.extend(seg.deleted_edge_ids().iter().copied());
        }

        self.deleted_nodes = Some(deleted_nodes);
        self.deleted_edges = Some(deleted_edges);
        self.node_cache.clear();
        self.edge_cache.clear();
    }
}

/// Disjoint-set (Union-Find) with path compression and union by rank.
///
/// Uses `IdMap` (identity-hashed HashMap) for sparse graph ID spaces.
/// Near-O(α(n)) amortized per find/union where α is the inverse Ackermann
/// function — effectively constant for all practical graph sizes.
struct UnionFind {
    parent: IdMap<u64>,
    rank: IdMap<u8>,
}

impl UnionFind {
    fn with_capacity(cap: usize) -> Self {
        Self {
            parent: IdMap::with_capacity_and_hasher(cap, IdBuildHasher::default()),
            rank: IdMap::with_hasher(IdBuildHasher::default()),
        }
    }

    fn make_set(&mut self, x: u64) {
        self.parent.insert(x, x);
    }

    /// Find with iterative path compression.
    fn find(&mut self, mut x: u64) -> u64 {
        let mut root = x;
        loop {
            let p = self.parent.get(&root).copied().unwrap_or(root);
            if p == root {
                break;
            }
            root = p;
        }
        while x != root {
            let p = self.parent.get(&x).copied().unwrap_or(x);
            self.parent.insert(x, root);
            x = p;
        }
        root
    }

    fn union(&mut self, a: u64, b: u64) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        let rank_a = self.rank.get(&ra).copied().unwrap_or(0);
        let rank_b = self.rank.get(&rb).copied().unwrap_or(0);
        match rank_a.cmp(&rank_b) {
            std::cmp::Ordering::Less => {
                self.parent.insert(ra, rb);
            }
            std::cmp::Ordering::Greater => {
                self.parent.insert(rb, ra);
            }
            std::cmp::Ordering::Equal => {
                self.parent.insert(rb, ra);
                self.rank.insert(ra, rank_a + 1);
            }
        }
    }
}

impl DatabaseEngine {
    // --- Degree cache (Phase 18a2) ---

    /// Rebuild the degree cache from scratch using the walk-based degree logic.
    /// Enumerates adjacency-bearing node IDs (nodes that appear in memtable
    /// adjacency maps or segment adjacency indexes), then computes degree
    /// stats for each using the proven-correct walk path.
    ///
    /// Uses `now_millis()` as reference time, matching the default behavior of
    /// `degree(at_epoch=None)`. The cache accelerates only the `at_epoch=None`
    /// path, so rebuild semantics must match.
    ///
    /// Called at the end of `open()` (after WAL replay + segment loading)
    /// and after compaction completes.
    pub(crate) fn rebuild_degree_cache(&mut self) -> Result<(), EngineError> {
        let now = now_millis();

        // Collect adjacency-bearing node IDs from memtable
        let mut node_ids: HashSet<u64> = HashSet::new();
        node_ids.extend(self.memtable.adj_out().keys());
        node_ids.extend(self.memtable.adj_in().keys());

        // Collect adjacency-bearing node IDs from segments
        for seg in &self.segments {
            node_ids.extend(seg.adj_node_ids()?);
        }

        // Collect tombstones once (shared across all nodes)
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        // Skip deleted nodes — their incident edges are tombstoned (cascade),
        // so walk returns (0, 0.0) and they'd be excluded anyway.
        node_ids.retain(|id| !deleted_nodes.contains(id));

        let mut cache = HashMap::with_capacity(node_ids.len());
        let mut seen_edges: HashSet<u64> = HashSet::new();
        for &nid in &node_ids {
            let (out_deg, out_wsum) = self.degree_stats_raw_walk_inner(
                nid,
                Direction::Outgoing,
                None,
                now,
                &deleted_nodes,
                &deleted_edges,
                &mut seen_edges,
            )?;
            let (in_deg, in_wsum) = self.degree_stats_raw_walk_inner(
                nid,
                Direction::Incoming,
                None,
                now,
                &deleted_nodes,
                &deleted_edges,
                &mut seen_edges,
            )?;

            // Walk outgoing edges to count self-loops and temporal edges.
            // Piggybacks on the same memtable-first / segments-newest-first /
            // seen_edges dedup pattern as degree_stats_raw_walk.
            let mut sl_count: u32 = 0;
            let mut sl_wsum: f64 = 0.0;
            let mut out_temporal: u32 = 0;
            let mut sl_temporal: u32 = 0;
            seen_edges.clear();

            let _ = self.memtable.for_each_adj_entry(
                nid,
                Direction::Outgoing,
                None,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    seen_edges.insert(edge_id);
                    if valid_to != i64::MAX || valid_from > now {
                        out_temporal += 1;
                        if neighbor_id == nid {
                            sl_temporal += 1;
                        }
                    }
                    if !is_edge_valid_at(valid_from, valid_to, now) {
                        return ControlFlow::Continue(());
                    }
                    if neighbor_id == nid {
                        sl_count += 1;
                        sl_wsum += weight as f64;
                    }
                    ControlFlow::Continue(())
                },
            );
            for seg in &self.segments {
                let _ = seg.for_each_adj_posting(
                    nid,
                    Direction::Outgoing,
                    None,
                    &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                        if !seen_edges.insert(edge_id) {
                            return ControlFlow::Continue(());
                        }
                        if deleted_edges.contains(&edge_id) {
                            return ControlFlow::Continue(());
                        }
                        if deleted_nodes.contains(&neighbor_id) {
                            return ControlFlow::Continue(());
                        }
                        if valid_to != i64::MAX || valid_from > now {
                            out_temporal += 1;
                            if neighbor_id == nid {
                                sl_temporal += 1;
                            }
                        }
                        if !is_edge_valid_at(valid_from, valid_to, now) {
                            return ControlFlow::Continue(());
                        }
                        if neighbor_id == nid {
                            sl_count += 1;
                            sl_wsum += weight as f64;
                        }
                        ControlFlow::Continue(())
                    },
                )?;
            }

            // Walk incoming edges to count temporal incoming edges.
            let mut in_temporal: u32 = 0;
            seen_edges.clear();
            let _ = self.memtable.for_each_adj_entry(
                nid,
                Direction::Incoming,
                None,
                &mut |edge_id, _neighbor_id, _weight, valid_from, valid_to| {
                    seen_edges.insert(edge_id);
                    if valid_to != i64::MAX || valid_from > now {
                        in_temporal += 1;
                    }
                    ControlFlow::Continue(())
                },
            );
            for seg in &self.segments {
                let _ = seg.for_each_adj_posting(
                    nid,
                    Direction::Incoming,
                    None,
                    &mut |edge_id, neighbor_id, _weight, valid_from, valid_to| {
                        if !seen_edges.insert(edge_id) {
                            return ControlFlow::Continue(());
                        }
                        if deleted_edges.contains(&edge_id) {
                            return ControlFlow::Continue(());
                        }
                        if deleted_nodes.contains(&neighbor_id) {
                            return ControlFlow::Continue(());
                        }
                        if valid_to != i64::MAX || valid_from > now {
                            in_temporal += 1;
                        }
                        ControlFlow::Continue(())
                    },
                )?;
            }

            // temporal_edge_count = out_temporal + in_temporal - sl_temporal
            // (self-loop temporal edges are counted in both directions but
            // should only count once, matching the self_loop_count pattern)
            let temporal_edge_count =
                out_temporal + in_temporal - sl_temporal;

            if out_deg > 0 || in_deg > 0 || temporal_edge_count > 0 {
                cache.insert(
                    nid,
                    DegreeEntry {
                        out_degree: out_deg as u32,
                        in_degree: in_deg as u32,
                        out_weight_sum: out_wsum,
                        in_weight_sum: in_wsum,
                        self_loop_count: sl_count,
                        self_loop_weight_sum: sl_wsum,
                        temporal_edge_count,
                    },
                );
            }
        }

        self.degree_cache = cache;
        Ok(())
    }

    // --- Degree counts + aggregations (Phase 18a) ---

    /// Walk-based degree stats: merge adjacency postings across memtable +
    /// segments. Deduplicates by edge_id, skips tombstoned edges/nodes,
    /// applies temporal filtering. No prune policy filtering.
    ///
    /// Used as the fallback for type-filtered, temporal, or policy-filtered
    /// queries. Also used by `rebuild_degree_cache()` to populate the cache
    /// from the known-correct walk path.
    fn degree_stats_raw_walk(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        reference_time: i64,
    ) -> Result<(u64, f64), EngineError> {
        let (deleted_nodes, deleted_edges) = self.collect_tombstones();
        let mut seen_edges = HashSet::with_capacity(32);
        self.degree_stats_raw_walk_inner(
            node_id,
            direction,
            type_filter,
            reference_time,
            &deleted_nodes,
            &deleted_edges,
            &mut seen_edges,
        )
    }

    /// Inner walk-based degree stats with pre-collected tombstones.
    /// Avoids redundant tombstone collection when called in a loop
    /// (e.g., during `rebuild_degree_cache`).
    #[allow(clippy::too_many_arguments)]
    fn degree_stats_raw_walk_inner(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        reference_time: i64,
        deleted_nodes: &HashSet<u64>,
        deleted_edges: &HashSet<u64>,
        seen_edges: &mut HashSet<u64>,
    ) -> Result<(u64, f64), EngineError> {
        let mut count: u64 = 0;
        let mut weight_sum: f64 = 0.0;

        seen_edges.clear();

        // Memtable (local tombstone filtering handled inside).
        // Shadow first: always record edge_id so segment versions can't leak.
        let _ = self.memtable.for_each_adj_entry(
            node_id,
            direction,
            type_filter,
            &mut |edge_id, _neighbor_id, weight, valid_from, valid_to| {
                seen_edges.insert(edge_id);
                if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                    return ControlFlow::Continue(());
                }
                count += 1;
                weight_sum += weight as f64;
                ControlFlow::Continue(())
            },
        );

        // Segments newest-to-oldest (segment-local + global tombstone filtering).
        // Version shadowing (seen_edges) before temporal filter — a newer invalid
        // version must still shadow older valid versions with the same edge_id.
        for seg in &self.segments {
            let _ = seg.for_each_adj_posting(
                node_id,
                direction,
                type_filter,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    if !seen_edges.insert(edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_edges.contains(&edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_nodes.contains(&neighbor_id) {
                        return ControlFlow::Continue(());
                    }
                    count += 1;
                    weight_sum += weight as f64;
                    ControlFlow::Continue(())
                },
            )?;
        }

        Ok((count, weight_sum))
    }

    /// Degree stats with prune policy filtering. When policies are active,
    /// tracks per-neighbor stats so excluded neighbors can be subtracted
    /// without materializing the full neighbor list. Temporal filtering uses
    /// `reference_time`; prune policy evaluation always uses wall-clock now.
    fn degree_stats(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        reference_time: i64,
    ) -> Result<(u64, f64), EngineError> {
        // No policies → delegate to walk path. Cache acceleration is handled
        // at the public API layer (degree/sum_edge_weights/avg_edge_weight);
        // by the time we reach here, the caller has already determined the
        // cache cannot be used (type-filtered or temporal query).
        if self.manifest.prune_policies.is_empty() {
            return self.degree_stats_raw_walk(node_id, direction, type_filter, reference_time);
        }

        // Policy path: track per-neighbor-id stats so we can subtract excluded ones.
        // This avoids materializing Vec<NeighborEntry> while still respecting policies.
        let mut neighbor_stats: HashMap<u64, (u64, f64)> = HashMap::new(); // neighbor_id → (count, weight_sum)
        let mut total_count: u64 = 0;
        let mut total_weight: f64 = 0.0;

        let accumulate = |neighbor_id: u64,
                          weight: f32,
                          stats: &mut HashMap<u64, (u64, f64)>,
                          count: &mut u64,
                          wsum: &mut f64| {
            let entry = stats.entry(neighbor_id).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += weight as f64;
            *count += 1;
            *wsum += weight as f64;
        };

        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let mut seen_edges: HashSet<u64> = HashSet::with_capacity(32);

        let _ = self.memtable.for_each_adj_entry(
            node_id,
            direction,
            type_filter,
            &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                seen_edges.insert(edge_id);
                if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                    return ControlFlow::Continue(());
                }
                accumulate(
                    neighbor_id,
                    weight,
                    &mut neighbor_stats,
                    &mut total_count,
                    &mut total_weight,
                );
                ControlFlow::Continue(())
            },
        );

        for seg in &self.segments {
            let _ = seg.for_each_adj_posting(
                node_id,
                direction,
                type_filter,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    if !seen_edges.insert(edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_edges.contains(&edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_nodes.contains(&neighbor_id) {
                        return ControlFlow::Continue(());
                    }
                    accumulate(
                        neighbor_id,
                        weight,
                        &mut neighbor_stats,
                        &mut total_count,
                        &mut total_weight,
                    );
                    ControlFlow::Continue(())
                },
            )?;
        }

        // Batch-check neighbor IDs against prune policies (single merge-walk
        // per segment, same as neighbors() policy path). Policy evaluation
        // always uses wall-clock now, not reference_time.
        let neighbor_ids: Vec<u64> = neighbor_stats.keys().copied().collect();
        let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
        if !excluded.is_empty() {
            for &nid in &excluded {
                if let Some((ec, ew)) = neighbor_stats.get(&nid) {
                    total_count -= ec;
                    total_weight -= ew;
                }
            }
        }

        Ok((total_count, total_weight))
    }

    /// Count the number of edges incident to a node (its degree).
    ///
    /// Counts adjacency postings across memtable + segments without
    /// materializing the neighbor list. Deduplicates by edge_id and
    /// skips tombstoned edges/nodes. Applies temporal filtering (edges
    /// must be valid at `at_epoch`, defaulting to now). Respects prune policies.
    ///
    /// - `direction`: `Outgoing`, `Incoming`, or `Both`.
    /// - `type_filter`: if `Some`, only count edges whose type_id is in the list.
    /// - `at_epoch`: reference time in epoch millis. `None` → now.
    ///
    /// Returns 0 for nonexistent nodes (not an error).
    pub fn degree(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<u64, EngineError> {
        // O(1) cache path: unfiltered, non-temporal, no prune policies,
        // and no temporal edges incident to this node (which could expire
        // and change degree without a mutation).
        if at_epoch.is_none()
            && type_filter.is_none()
            && self.manifest.prune_policies.is_empty()
        {
            let entry = self
                .degree_cache
                .get(&node_id)
                .copied()
                .unwrap_or(DegreeEntry::ZERO);
            if entry.temporal_edge_count == 0 {
                return Ok(match direction {
                    Direction::Outgoing => entry.out_degree as u64,
                    Direction::Incoming => entry.in_degree as u64,
                    Direction::Both => {
                        (entry.out_degree + entry.in_degree - entry.self_loop_count) as u64
                    }
                });
            }
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);
        let (count, _) = self.degree_stats(node_id, direction, type_filter, reference_time)?;
        Ok(count)
    }

    /// Sum of edge weights incident to a node.
    ///
    /// Walks adjacency postings (where weight is embedded as f32) and
    /// accumulates into f64 for precision. No edge record hydration.
    /// Applies temporal filtering (edges must be valid at `at_epoch`,
    /// defaulting to now). Respects prune policies.
    ///
    /// - `at_epoch`: reference time in epoch millis. `None` → now.
    ///
    /// Returns 0.0 for nonexistent or zero-degree nodes.
    pub fn sum_edge_weights(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<f64, EngineError> {
        // O(1) cache path
        if at_epoch.is_none()
            && type_filter.is_none()
            && self.manifest.prune_policies.is_empty()
        {
            let entry = self
                .degree_cache
                .get(&node_id)
                .copied()
                .unwrap_or(DegreeEntry::ZERO);
            if entry.temporal_edge_count == 0 {
                return Ok(match direction {
                    Direction::Outgoing => entry.out_weight_sum,
                    Direction::Incoming => entry.in_weight_sum,
                    Direction::Both => {
                        entry.out_weight_sum + entry.in_weight_sum - entry.self_loop_weight_sum
                    }
                });
            }
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);
        let (_, weight_sum) = self.degree_stats(node_id, direction, type_filter, reference_time)?;
        Ok(weight_sum)
    }

    /// Average edge weight incident to a node.
    ///
    /// Returns `None` if the node has zero edges (avoids division by zero).
    /// Uses f64 accumulator for precision. Applies temporal filtering
    /// (edges must be valid at `at_epoch`, defaulting to now). Respects
    /// prune policies.
    ///
    /// - `at_epoch`: reference time in epoch millis. `None` → now.
    pub fn avg_edge_weight(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<Option<f64>, EngineError> {
        // O(1) cache path
        if at_epoch.is_none()
            && type_filter.is_none()
            && self.manifest.prune_policies.is_empty()
        {
            let entry = self
                .degree_cache
                .get(&node_id)
                .copied()
                .unwrap_or(DegreeEntry::ZERO);
            if entry.temporal_edge_count == 0 {
                let (count, weight_sum) = match direction {
                    Direction::Outgoing => (entry.out_degree as u64, entry.out_weight_sum),
                    Direction::Incoming => (entry.in_degree as u64, entry.in_weight_sum),
                    Direction::Both => (
                        (entry.out_degree + entry.in_degree - entry.self_loop_count) as u64,
                        entry.out_weight_sum + entry.in_weight_sum - entry.self_loop_weight_sum,
                    ),
                };
                return if count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(weight_sum / count as f64))
                };
            }
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);
        let (count, weight_sum) =
            self.degree_stats(node_id, direction, type_filter, reference_time)?;
        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(weight_sum / count as f64))
        }
    }

    /// Batch degree counts for multiple nodes. Collects tombstones once, then
    /// uses a single adaptive cursor walk per segment for all node IDs.
    ///
    /// Returns a [`NodeIdMap<u64>`] mapping each queried node_id to its degree.
    /// `NodeIdMap` is a `HashMap` with identity hashing optimized for
    /// engine-generated numeric IDs. Nodes with degree 0 are omitted
    /// (consistent with `neighbors_batch`). Applies temporal filtering
    /// (edges must be valid at `at_epoch`, defaulting to now). Respects
    /// prune policies.
    ///
    /// - `at_epoch`: reference time in epoch millis. `None` → now.
    ///
    /// `node_ids` need not be sorted (sorted internally).
    pub fn degrees(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<NodeIdMap<u64>, EngineError> {
        // O(1) cache path: N cache lookups instead of batch merge-walk.
        // Per-node: use cache if temporal_edge_count == 0, else fall through
        // to walk for that individual node.
        if at_epoch.is_none()
            && type_filter.is_none()
            && self.manifest.prune_policies.is_empty()
        {
            let mut counts: NodeIdMap<u64> =
                NodeIdMap::with_capacity_and_hasher(node_ids.len(), IdBuildHasher::default());
            let mut walk_needed: Vec<u64> = Vec::new();
            for &nid in node_ids {
                let entry = self
                    .degree_cache
                    .get(&nid)
                    .copied()
                    .unwrap_or(DegreeEntry::ZERO);
                if entry.temporal_edge_count == 0 {
                    let deg = match direction {
                        Direction::Outgoing => entry.out_degree as u64,
                        Direction::Incoming => entry.in_degree as u64,
                        Direction::Both => {
                            (entry.out_degree + entry.in_degree - entry.self_loop_count) as u64
                        }
                    };
                    if deg > 0 {
                        counts.insert(nid, deg);
                    }
                } else {
                    walk_needed.push(nid);
                }
            }
            if !walk_needed.is_empty() {
                let now = now_millis();
                // Batch-walk temporal nodes via degrees_raw (single tombstone
                // collection + batched segment cursor walks). Safe because
                // we're already inside the prune_policies.is_empty() guard.
                let walked = self.degrees_raw(
                    &walk_needed, direction, type_filter, now,
                )?;
                counts.extend(walked);
            }
            return Ok(counts);
        }

        let reference_time = at_epoch.unwrap_or_else(now_millis);
        if self.manifest.prune_policies.is_empty() {
            return self.degrees_raw(node_ids, direction, type_filter, reference_time);
        }
        if node_ids.is_empty() {
            return Ok(NodeIdMap::default());
        }

        // Policy path: single walk tracking both total counts AND per-neighbor
        // counts so we can subtract excluded neighbors after one batch policy check.
        let sorted_ids: Vec<u64> = {
            let mut ids = node_ids.to_vec();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        let mut counts: NodeIdMap<u64> =
            NodeIdMap::with_capacity_and_hasher(sorted_ids.len(), IdBuildHasher::default());
        // node_id → (neighbor_id → edge_count_to_that_neighbor)
        let mut per_node_nbr: IdMap<IdMap<u64>> =
            IdMap::with_capacity_and_hasher(sorted_ids.len(), IdBuildHasher::default());
        let mut all_neighbor_ids: HashSet<u64> = HashSet::new();

        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let mut seen_edges: HashSet<(u64, u64)> = HashSet::new();

        for &nid in &sorted_ids {
            let mut count: u64 = 0;
            let mut nbr_map: IdMap<u64> = IdMap::default();
            let _ = self.memtable.for_each_adj_entry(
                nid,
                direction,
                type_filter,
                &mut |edge_id, neighbor_id, _weight, valid_from, valid_to| {
                    seen_edges.insert((nid, edge_id));
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    count += 1;
                    *nbr_map.entry(neighbor_id).or_default() += 1;
                    all_neighbor_ids.insert(neighbor_id);
                    ControlFlow::Continue(())
                },
            );
            if count > 0 {
                counts.insert(nid, count);
                per_node_nbr.insert(nid, nbr_map);
            }
        }

        for seg in &self.segments {
            let _ = seg.for_each_adj_posting_batch(
                &sorted_ids,
                direction,
                type_filter,
                &mut |queried_nid, edge_id, neighbor_id, _weight, valid_from, valid_to| {
                    if !seen_edges.insert((queried_nid, edge_id)) {
                        return ControlFlow::Continue(());
                    }
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_edges.contains(&edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_nodes.contains(&neighbor_id) {
                        return ControlFlow::Continue(());
                    }
                    *counts.entry(queried_nid).or_default() += 1;
                    *per_node_nbr
                        .entry(queried_nid)
                        .or_default()
                        .entry(neighbor_id)
                        .or_default() += 1;
                    all_neighbor_ids.insert(neighbor_id);
                    ControlFlow::Continue(())
                },
            )?;
        }

        // Single batch policy check for all unique neighbor IDs.
        // Policy evaluation always uses wall-clock now, not reference_time.
        let neighbor_ids_vec: Vec<u64> = all_neighbor_ids.into_iter().collect();
        let excluded = self.policy_excluded_node_ids(&neighbor_ids_vec)?;
        if !excluded.is_empty() {
            // P2 fix: iterate each node's (smaller) neighbor map and probe excluded
            for (&nid, nbr_map) in &per_node_nbr {
                let mut subtract = 0u64;
                for (&nbr_id, &cnt) in nbr_map {
                    if excluded.contains(&nbr_id) {
                        subtract += cnt;
                    }
                }
                if subtract > 0 {
                    if let Some(deg) = counts.get_mut(&nid) {
                        *deg = deg.saturating_sub(subtract);
                    }
                }
            }
        }

        counts.retain(|_, &mut v| v > 0);
        Ok(counts)
    }

    /// Raw batch degree counts with no prune policy filtering. Collects
    /// tombstones once, uses adaptive cursor walk per segment. Temporal
    /// filtering at `reference_time`.
    fn degrees_raw(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        reference_time: i64,
    ) -> Result<NodeIdMap<u64>, EngineError> {
        if node_ids.is_empty() {
            return Ok(NodeIdMap::default());
        }

        let sorted_ids: Vec<u64> = {
            let mut ids = node_ids.to_vec();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        let mut counts: NodeIdMap<u64> =
            NodeIdMap::with_capacity_and_hasher(sorted_ids.len(), IdBuildHasher::default());

        // Collect global tombstones once
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        // Flat (node_id, edge_id) set for cross-source dedup — single allocation
        // instead of one HashSet per node
        let mut seen_edges: HashSet<(u64, u64)> = HashSet::new();

        // Memtable pass — shadow first, then temporal filter
        for &nid in &sorted_ids {
            let mut count: u64 = 0;
            let _ = self.memtable.for_each_adj_entry(
                nid,
                direction,
                type_filter,
                &mut |edge_id, _neighbor_id, _weight, valid_from, valid_to| {
                    seen_edges.insert((nid, edge_id));
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    count += 1;
                    ControlFlow::Continue(())
                },
            );
            if count > 0 {
                counts.insert(nid, count);
            }
        }

        // Segment pass (one adaptive cursor walk per segment).
        // Version shadowing before temporal filter.
        for seg in &self.segments {
            let _ = seg.for_each_adj_posting_batch(
                &sorted_ids,
                direction,
                type_filter,
                &mut |queried_nid, edge_id, neighbor_id, _weight, valid_from, valid_to| {
                    if !seen_edges.insert((queried_nid, edge_id)) {
                        return ControlFlow::Continue(());
                    }
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_edges.contains(&edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_nodes.contains(&neighbor_id) {
                        return ControlFlow::Continue(());
                    }
                    *counts.entry(queried_nid).or_default() += 1;
                    ControlFlow::Continue(())
                },
            )?;
        }

        counts.retain(|_, &mut v| v > 0);
        Ok(counts)
    }

    // --- Shortest path algorithms (Phase 18b) ---

    /// Find the shortest path between two nodes.
    ///
    /// Algorithm auto-selected from `weight_field`:
    /// - `None` → bidirectional BFS (unweighted, hop count)
    /// - `Some("weight")` → bidirectional Dijkstra reading `NeighborEntry.weight`
    /// - `Some("<field>")` → bidirectional Dijkstra reading `edge.props[field]` as f64
    ///
    /// Returns `None` if no path exists within the given constraints.
    ///
    /// # Arguments
    /// - `direction`: edge direction for traversal
    /// - `edge_type_filter`: restrict to these edge types (None = all)
    /// - `weight_field`: weight source for Dijkstra; None = BFS
    /// - `at_epoch`: temporal snapshot (None = now)
    /// - `max_depth`: maximum hop count (None = unlimited)
    /// - `max_cost`: maximum total cost for Dijkstra (None = unlimited; ignored for BFS)
    ///
    /// # Examples
    ///
    /// ```
    /// # use overgraph::{DatabaseEngine, DbOptions, Direction};
    /// # use std::collections::BTreeMap;
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    /// let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
    /// let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
    /// let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
    /// db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    /// db.upsert_edge(b, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    ///
    /// // BFS shortest path (unweighted)
    /// let path = db.shortest_path(a, c, Direction::Outgoing, None, None, None, None, None)
    ///     .unwrap().unwrap();
    /// assert_eq!(path.nodes, vec![a, b, c]);
    /// assert_eq!(path.total_cost, 2.0); // 2 hops
    ///
    /// // Dijkstra shortest path (weighted by edge weight)
    /// let path = db.shortest_path(a, c, Direction::Outgoing, None, Some("weight"), None, None, None)
    ///     .unwrap().unwrap();
    /// assert_eq!(path.nodes, vec![a, b, c]);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn shortest_path(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
    ) -> Result<Option<ShortestPath>, EngineError> {
        let policy_cutoffs = self.query_policy_cutoffs();
        if !self.path_endpoints_visible(from, to, policy_cutoffs.as_ref())? {
            return Ok(None);
        }
        if from == to {
            return Ok(Some(ShortestPath {
                nodes: vec![from],
                edges: vec![],
                total_cost: 0.0,
            }));
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);

        match weight_field {
            None => self.bfs_shortest_path(
                from,
                to,
                direction,
                edge_type_filter,
                reference_time,
                max_depth,
                policy_cutoffs.as_ref(),
            ),
            Some(wf) => self.dijkstra_shortest_path(
                from,
                to,
                direction,
                edge_type_filter,
                wf,
                reference_time,
                max_depth,
                max_cost,
                policy_cutoffs.as_ref(),
            ),
        }
    }

    /// Check if two nodes are connected (reachable via edges).
    ///
    /// Uses bidirectional BFS with no parent tracking for minimal overhead.
    /// Returns `true` if a path exists within `max_depth` hops.
    ///
    /// # Examples
    ///
    /// ```
    /// # use overgraph::{DatabaseEngine, DbOptions, Direction};
    /// # use std::collections::BTreeMap;
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    /// let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
    /// let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
    /// let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
    /// db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    ///
    /// assert!(db.is_connected(a, b, Direction::Outgoing, None, None, None).unwrap());
    /// assert!(!db.is_connected(a, c, Direction::Outgoing, None, None, None).unwrap());
    /// ```
    pub fn is_connected(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
    ) -> Result<bool, EngineError> {
        let policy_cutoffs = self.query_policy_cutoffs();
        if !self.path_endpoints_visible(from, to, policy_cutoffs.as_ref())? {
            return Ok(false);
        }
        if from == to {
            return Ok(true);
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);
        self.bfs_is_connected(
            from,
            to,
            direction,
            edge_type_filter,
            reference_time,
            max_depth,
            policy_cutoffs.as_ref(),
        )
    }

    /// Traverse outward from `start` with deterministic BFS ordering.
    ///
    /// Results are emitted in `(depth ASC, node_id ASC)` order. `node_type_filter`
    /// applies only to emitted hits; traversal still expands through visible nodes
    /// that do not match the filter.
    #[allow(clippy::too_many_arguments)]
    pub fn traverse(
        &self,
        start: u64,
        min_depth: u32,
        max_depth: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
        limit: Option<usize>,
        cursor: Option<&TraversalCursor>,
    ) -> Result<TraversalPageResult, EngineError> {
        if min_depth > max_depth {
            return Err(EngineError::InvalidOperation(
                "min_depth must be <= max_depth".to_string(),
            ));
        }
        if let Some(lambda) = decay_lambda {
            if !lambda.is_finite() || lambda < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be finite and non-negative".to_string(),
                ));
            }
        }

        let limit = limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(TraversalPageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        let reference_time = at_epoch.unwrap_or_else(now_millis);
        let policy_cutoffs = self.query_policy_cutoffs();
        let Some(start_node) = self.get_node_raw(start)? else {
            return Ok(TraversalPageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        if policy_cutoffs
            .as_ref()
            .is_some_and(|cutoffs| cutoffs.excludes(&start_node))
        {
            return Ok(TraversalPageResult {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        let node_type_filter: Option<HashSet<u32>> =
            node_type_filter.map(|types| types.iter().copied().collect());
        let mut hits: Vec<TraversalHit> = Vec::with_capacity(limit.min(64));
        let mut last_emitted_cursor: Option<TraversalCursor> = None;
        let mut has_more = false;

        if min_depth == 0
            && Self::traversal_after_cursor(cursor, 0, start)
            && node_type_filter
                .as_ref()
                .is_none_or(|types| types.contains(&start_node.type_id))
        {
            if hits.len() < limit {
                hits.push(TraversalHit {
                    node_id: start,
                    depth: 0,
                    via_edge_id: None,
                    score: Self::traversal_score(decay_lambda, 0),
                });
                last_emitted_cursor = Some(TraversalCursor {
                    depth: 0,
                    last_node_id: start,
                });
            } else {
                has_more = true;
            }
        }

        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut blocked_hidden: IdSet = IdSet::default();
        let mut visited: IdSet = IdSet::default();
        visited.insert(start);
        let mut frontier = vec![start];
        let mut depth = 0u32;
        let mut visible_nodes = Vec::new();
        let mut emitted_nodes = Vec::new();

        'traversal: while !frontier.is_empty() && depth < max_depth {
            let next_depth = depth + 1;
            let mut layer_order: Vec<u64> = Vec::new();
            let mut layer_candidates: IdMap<(u64, u64)> = IdMap::default();

            let _ = self.expand_frontier(
                &frontier,
                direction,
                edge_type_filter,
                reference_time,
                &mut tombstones,
                &mut |source, neighbor, edge_id| {
                    if visited.contains(&neighbor)
                    {
                        return ControlFlow::Continue(());
                    }
                    match layer_candidates.entry(neighbor) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            if blocked_hidden.contains(&neighbor) {
                                return ControlFlow::Continue(());
                            }
                            layer_order.push(neighbor);
                            entry.insert((source, edge_id));
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let current = *entry.get();
                            if (source, edge_id) < current {
                                entry.insert((source, edge_id));
                            }
                        }
                    }
                    ControlFlow::Continue(())
                },
            )?;

            if layer_order.is_empty() {
                break;
            }

            self.classify_traversal_layer(
                &mut layer_order,
                node_type_filter.as_ref(),
                policy_cutoffs.as_ref(),
                &mut blocked_hidden,
                &mut visible_nodes,
                &mut emitted_nodes,
            )?;

            for &node_id in &visible_nodes {
                visited.insert(node_id);
            }

            for node_id in emitted_nodes.iter().copied().filter(|_| next_depth >= min_depth) {
                if !Self::traversal_after_cursor(cursor, next_depth, node_id) {
                    continue;
                }
                if hits.len() >= limit {
                    has_more = true;
                    break 'traversal;
                }
                let via_edge_id = layer_candidates.get(&node_id).map(|(_, edge_id)| *edge_id);
                hits.push(TraversalHit {
                    node_id,
                    depth: next_depth,
                    via_edge_id,
                    score: Self::traversal_score(decay_lambda, next_depth),
                });
                last_emitted_cursor = Some(TraversalCursor {
                    depth: next_depth,
                    last_node_id: node_id,
                });
            }

            frontier.clear();
            frontier.extend(visible_nodes.iter().copied());
            depth = next_depth;
        }

        Ok(TraversalPageResult {
            items: hits,
            next_cursor: has_more.then_some(last_emitted_cursor).flatten(),
        })
    }

    /// Reverse a direction for backward search in bidirectional algorithms.
    fn reverse_direction(direction: Direction) -> Direction {
        match direction {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Both => Direction::Both,
        }
    }

    /// Collect global tombstones once for use across BFS/Dijkstra iterations.
    fn collect_tombstones(&self) -> (HashSet<u64>, HashSet<u64>) {
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }
        (deleted_nodes, deleted_edges)
    }

    fn traversal_after_cursor(cursor: Option<&TraversalCursor>, depth: u32, node_id: u64) -> bool {
        match cursor {
            None => true,
            Some(cursor) => match depth.cmp(&cursor.depth) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => node_id > cursor.last_node_id,
            },
        }
    }

    fn traversal_score(decay_lambda: Option<f64>, depth: u32) -> Option<f64> {
        decay_lambda.map(|lambda| (-lambda * depth as f64).exp())
    }

    fn classify_traversal_layer(
        &self,
        layer_order: &mut [u64],
        node_type_filter: Option<&HashSet<u32>>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        blocked_hidden: &mut IdSet,
        visible_nodes: &mut Vec<u64>,
        emitted_nodes: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        visible_nodes.clear();
        emitted_nodes.clear();
        if layer_order.is_empty() {
            return Ok(());
        }

        layer_order.sort_unstable();
        if policy_cutoffs.is_none() && node_type_filter.is_none() {
            visible_nodes.extend(layer_order.iter().copied());
            emitted_nodes.extend(layer_order.iter().copied());
            return Ok(());
        }

        let nodes = self.get_nodes_raw(layer_order)?;
        for (&node_id, slot) in layer_order.iter().zip(nodes.iter()) {
            if let Some(node) = slot {
                if policy_cutoffs.is_some_and(|cutoffs| cutoffs.excludes(node)) {
                    blocked_hidden.insert(node_id);
                    continue;
                }
                visible_nodes.push(node_id);
                if node_type_filter.is_none_or(|types| types.contains(&node.type_id)) {
                    emitted_nodes.push(node_id);
                }
            }
        }
        Ok(())
    }

    /// Expand neighbors for a set of frontier nodes using callback-based iteration.
    /// Calls `on_neighbor` for each valid, non-tombstoned, temporally-active neighbor.
    /// Handles cross-source dedup (memtable wins over segments, newer segments win).
    fn expand_frontier<F>(
        &self,
        frontier: &[u64],
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        tombstones: &mut TraversalTombstoneView<'_>,
        on_neighbor: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, u64) -> ControlFlow<()>, // (source_node, neighbor_node, edge_id)
    {
        if frontier.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }
        // Frontier never contains duplicates (enforced by visited sets in callers),
        // but must be sorted for batch segment reads.
        let mut sorted_ids: Vec<u64> = frontier.to_vec();
        sorted_ids.sort_unstable();

        // Cross-source dedup: (queried_nid, edge_id). The key includes queried_nid
        // because the same edge can appear in multiple frontier nodes' adjacency
        // lists (e.g., both endpoints of an edge are in the frontier).
        let mut seen_edges: HashSet<(u64, u64)> = HashSet::new();

        // Memtable first (wins over segments)
        for &nid in &sorted_ids {
            if self
                .memtable
                .for_each_adj_entry(
                    nid,
                    direction,
                    edge_type_filter,
                    &mut |edge_id, neighbor_id, _weight, valid_from, valid_to| {
                        seen_edges.insert((nid, edge_id));
                        if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                            return ControlFlow::Continue(());
                        }
                        on_neighbor(nid, neighbor_id, edge_id)
                    },
                )
                .is_break()
            {
                return Ok(ControlFlow::Break(()));
            }
        }

        // Segments newest-to-oldest
        for seg in &self.segments {
            if seg
                .for_each_adj_posting_batch(
                    &sorted_ids,
                    direction,
                    edge_type_filter,
                    &mut |queried_nid, edge_id, neighbor_id, _weight, valid_from, valid_to| {
                        if !seen_edges.insert((queried_nid, edge_id)) {
                            return ControlFlow::Continue(());
                        }
                        if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                            return ControlFlow::Continue(());
                        }
                        if tombstones.is_edge_deleted(edge_id) {
                            return ControlFlow::Continue(());
                        }
                        // Skip both deleted neighbors AND deleted frontier nodes
                        if tombstones.is_node_deleted(neighbor_id)
                            || tombstones.is_node_deleted(queried_nid)
                        {
                            return ControlFlow::Continue(());
                        }
                        on_neighbor(queried_nid, neighbor_id, edge_id)
                    },
                )?
                .is_break()
            {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    /// Reconstruct path from bidirectional parent maps and a meeting node.
    /// `total_cost` is set to `edges.len()` (hop count) — callers override for weighted.
    fn reconstruct_bidir_path(
        fwd_parent: &IdMap<(u64, u64)>,
        bwd_parent: &IdMap<(u64, u64)>,
        meeting: u64,
    ) -> ShortestPath {
        // Forward half: walk from meeting back to source
        let mut fwd_nodes = vec![meeting];
        let mut fwd_edges = Vec::new();
        let mut cur = meeting;
        while let Some(&(parent, edge_id)) = fwd_parent.get(&cur) {
            fwd_nodes.push(parent);
            fwd_edges.push(edge_id);
            cur = parent;
        }
        fwd_nodes.reverse();
        fwd_edges.reverse();

        // Backward half: walk from meeting back to target
        let mut bwd_edges = Vec::new();
        cur = meeting;
        while let Some(&(parent, edge_id)) = bwd_parent.get(&cur) {
            fwd_nodes.push(parent);
            bwd_edges.push(edge_id);
            cur = parent;
        }
        fwd_edges.extend(bwd_edges);

        let total_cost = fwd_edges.len() as f64;
        ShortestPath {
            nodes: fwd_nodes,
            edges: fwd_edges,
            total_cost,
        }
    }

    fn update_layer_best(
        layer_best: &mut IdMap<Vec<LayerBestEntry>>,
        node: u64,
        hops: u32,
        cost: f64,
    ) {
        let entries = layer_best.entry(node).or_default();
        let insert_at = entries.partition_point(|entry| entry.hops < hops);
        if insert_at < entries.len() && entries[insert_at].hops == hops {
            if !path_cost_lt(cost, entries[insert_at].exact_cost) {
                return;
            }
            entries[insert_at].exact_cost = cost;
        } else {
            entries.insert(
                insert_at,
                LayerBestEntry {
                    hops,
                    exact_cost: cost,
                    best_cost: cost,
                    best_hops: hops,
                },
            );
        }

        let (mut best_cost, mut best_hops) = if insert_at > 0 {
            let prev = entries[insert_at - 1];
            (prev.best_cost, prev.best_hops)
        } else {
            (f64::INFINITY, 0)
        };

        for entry in entries.iter_mut().skip(insert_at) {
            if path_cost_lt(entry.exact_cost, best_cost)
                || (path_cost_eq(entry.exact_cost, best_cost)
                    && (!best_cost.is_finite() || entry.hops < best_hops))
            {
                best_cost = entry.exact_cost;
                best_hops = entry.hops;
            }
            entry.best_cost = best_cost;
            entry.best_hops = best_hops;
        }
    }

    fn best_layer_within(
        layer_best: &IdMap<Vec<LayerBestEntry>>,
        node: u64,
        max_hops: u32,
    ) -> Option<(u32, f64)> {
        let entries = layer_best.get(&node)?;
        let upper = entries.partition_point(|entry| entry.hops <= max_hops);
        if upper == 0 {
            return None;
        }
        let best = entries[upper - 1];
        if best.best_cost.is_finite() {
            Some((best.best_hops, best.best_cost))
        } else {
            None
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn update_layered_meeting(
        opposite_best: &IdMap<Vec<LayerBestEntry>>,
        node: u64,
        hops: u32,
        cost: f64,
        max_depth: u32,
        from_forward_side: bool,
        best_cost: &mut f64,
        best_meeting: &mut Option<(u64, u32, u32)>,
    ) {
        let Some((other_hops, other_cost)) =
            Self::best_layer_within(opposite_best, node, max_depth.saturating_sub(hops))
        else {
            return;
        };

        let total_cost = cost + other_cost;
        let total_hops = hops + other_hops;
        let should_update = path_cost_lt(total_cost, *best_cost)
            || (path_cost_eq(total_cost, *best_cost)
                && best_meeting
                    .map(|(_, best_fwd_hops, best_bwd_hops)| {
                        total_hops < best_fwd_hops + best_bwd_hops
                    })
                    .unwrap_or(true));
        if should_update {
            *best_cost = total_cost;
            *best_meeting = Some(if from_forward_side {
                (node, hops, other_hops)
            } else {
                (node, other_hops, hops)
            });
        }
    }

    fn reconstruct_bidir_layered_path(
        fwd_parent: &[IdMap<(u64, u64)>],
        bwd_parent: &[IdMap<(u64, u64)>],
        meeting: u64,
        fwd_hops: u32,
        bwd_hops: u32,
        total_cost: f64,
    ) -> ShortestPath {
        let mut nodes = vec![meeting];
        let mut edges = Vec::new();

        let mut cur_node = meeting;
        let mut cur_hops = fwd_hops;
        while cur_hops > 0 {
            let &(parent, edge_id) = fwd_parent[cur_hops as usize]
                .get(&cur_node)
                .expect("missing forward layered parent");
            nodes.push(parent);
            edges.push(edge_id);
            cur_node = parent;
            cur_hops -= 1;
        }
        nodes.reverse();
        edges.reverse();

        cur_node = meeting;
        cur_hops = bwd_hops;
        while cur_hops > 0 {
            let &(next_node, edge_id) = bwd_parent[cur_hops as usize]
                .get(&cur_node)
                .expect("missing backward layered parent");
            nodes.push(next_node);
            edges.push(edge_id);
            cur_node = next_node;
            cur_hops -= 1;
        }

        ShortestPath {
            nodes,
            edges,
            total_cost,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_weighted_dijkstra_side_up_to_cost(
        &self,
        heap: &mut BinaryHeap<Reverse<(OrdF64, u64)>>,
        dist: &mut IdMap<f64>,
        settled: &mut IdSet,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        limit_cost: f64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
    ) -> Result<(), EngineError> {
        while let Some(Reverse((OrdF64(d), node))) = heap.pop() {
            if path_cost_lt(limit_cost, d) {
                break;
            }
            if path_cost_lt(dist.get(&node).copied().unwrap_or(f64::INFINITY), d) {
                continue;
            }
            if !settled.insert(node) {
                continue;
            }

            let mut expansion_error: Option<EngineError> = None;
            let _ = self.for_each_search_neighbor(
                node,
                direction,
                edge_type_filter,
                reference_time,
                policy_cutoffs,
                tombstones,
                scratch,
                &mut |neighbor, edge_id, posting_weight| {
                    let weight = match self.extract_weight_from_parts(
                        edge_id,
                        posting_weight,
                        weight_field,
                        weight_cache,
                    ) {
                        Ok(weight) => weight,
                        Err(err) => {
                            expansion_error = Some(err);
                            return ControlFlow::Break(());
                        }
                    };
                    if weight < 0.0 || !weight.is_finite() {
                        expansion_error = Some(EngineError::InvalidOperation(
                            "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                        ));
                        return ControlFlow::Break(());
                    }
                    let new_dist = d + weight;
                    if path_cost_lt(limit_cost, new_dist) {
                        return ControlFlow::Continue(());
                    }
                    let old = dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
                    if path_cost_lt(new_dist, old) {
                        dist.insert(neighbor, new_dist);
                        heap.push(Reverse((OrdF64(new_dist), neighbor)));
                    }
                    ControlFlow::Continue(())
                },
            )?;
            if let Some(err) = expansion_error {
                return Err(err);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_weighted_layered_side_up_to_cost(
        &self,
        heap: &mut BinaryHeap<Reverse<(OrdF64, u32, u64)>>,
        dist: &mut [IdMap<f64>],
        settled: &mut [IdSet],
        layer_best: &mut IdMap<Vec<LayerBestEntry>>,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        limit_cost: f64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
    ) -> Result<(), EngineError> {
        while let Some(Reverse((OrdF64(d), hops, node))) = heap.pop() {
            if path_cost_lt(limit_cost, d) {
                break;
            }
            if path_cost_lt(
                dist[hops as usize]
                    .get(&node)
                    .copied()
                    .unwrap_or(f64::INFINITY),
                d,
            ) {
                continue;
            }
            if !settled[hops as usize].insert(node) || hops == max_depth {
                continue;
            }

            let next_hops = hops + 1;
            let mut expansion_error: Option<EngineError> = None;
            let _ = self.for_each_search_neighbor(
                node,
                direction,
                edge_type_filter,
                reference_time,
                policy_cutoffs,
                tombstones,
                scratch,
                &mut |neighbor, edge_id, posting_weight| {
                    let weight = match self.extract_weight_from_parts(
                        edge_id,
                        posting_weight,
                        weight_field,
                        weight_cache,
                    ) {
                        Ok(weight) => weight,
                        Err(err) => {
                            expansion_error = Some(err);
                            return ControlFlow::Break(());
                        }
                    };
                    if weight < 0.0 || !weight.is_finite() {
                        expansion_error = Some(EngineError::InvalidOperation(
                            "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                        ));
                        return ControlFlow::Break(());
                    }
                    let new_dist = d + weight;
                    if path_cost_lt(limit_cost, new_dist) {
                        return ControlFlow::Continue(());
                    }
                    let old = dist[next_hops as usize]
                        .get(&neighbor)
                        .copied()
                        .unwrap_or(f64::INFINITY);
                    if path_cost_lt(new_dist, old) {
                        dist[next_hops as usize].insert(neighbor, new_dist);
                        Self::update_layer_best(layer_best, neighbor, next_hops, new_dist);
                        heap.push(Reverse((OrdF64(new_dist), next_hops, neighbor)));
                    }
                    ControlFlow::Continue(())
                },
            )?;
            if let Some(err) = expansion_error {
                return Err(err);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_bfs_shortest_path_successors(
        &self,
        from: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        best_hops: u32,
        bwd_depth: &IdMap<u32>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<IdMap<Vec<(u64, u64)>>, EngineError> {
        let mut successors: IdMap<Vec<(u64, u64)>> = IdMap::default();
        if best_hops == 0 {
            return Ok(successors);
        }

        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut blocked_hidden: IdSet = IdSet::default();
        let mut frontier = vec![from];
        let mut discovered: IdSet = IdSet::default();
        discovered.insert(from);
        let mut depth = 0u32;

        while !frontier.is_empty() && depth < best_hops {
            let next_depth = depth + 1;
            let mut layer_order: Vec<u64> = Vec::new();
            let mut layer_candidates: IdSet = IdSet::default();
            let mut successor_candidates: Vec<(u64, u64, u64)> = Vec::new();

            let _ = self.expand_frontier(
                &frontier,
                direction,
                edge_type_filter,
                reference_time,
                &mut tombstones,
                &mut |source, neighbor, edge_id| {
                    let Some(&remaining_hops) = bwd_depth.get(&neighbor) else {
                        return ControlFlow::Continue(());
                    };
                    if next_depth + remaining_hops != best_hops
                        || blocked_hidden.contains(&neighbor)
                    {
                        return ControlFlow::Continue(());
                    }
                    successor_candidates.push((source, neighbor, edge_id));
                    if !discovered.contains(&neighbor) && layer_candidates.insert(neighbor) {
                        layer_order.push(neighbor);
                    }
                    ControlFlow::Continue(())
                },
            )?;

            let visible = match policy_cutoffs {
                Some(cutoffs) => Some(self.visible_node_ids(&layer_order, Some(cutoffs))?),
                None => None,
            };

            for &neighbor in &layer_order {
                if visible
                    .as_ref()
                    .map(|visible_ids| visible_ids.contains(&neighbor))
                    .unwrap_or(true)
                {
                    discovered.insert(neighbor);
                } else {
                    blocked_hidden.insert(neighbor);
                }
            }

            for (source, neighbor, edge_id) in successor_candidates {
                if visible
                    .as_ref()
                    .map(|visible_ids| visible_ids.contains(&neighbor))
                    .unwrap_or(true)
                {
                    successors
                        .entry(source)
                        .or_default()
                        .push((neighbor, edge_id));
                }
            }

            frontier = layer_order
                .into_iter()
                .filter(|neighbor| discovered.contains(neighbor))
                .collect();
            depth = next_depth;
        }

        Ok(successors)
    }

    fn cached_successors_for_node<F>(
        cache_layer: &mut IdMap<Vec<(u64, u64)>>,
        node: u64,
        build: F,
    ) -> Result<Vec<(u64, u64)>, EngineError>
    where
        F: FnOnce() -> Result<Vec<(u64, u64)>, EngineError>,
    {
        if let Some(cached) = cache_layer.get(&node) {
            return Ok(cached.clone());
        }

        let successors = build()?;
        cache_layer.insert(node, successors.clone());
        Ok(successors)
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_weighted_successors_from_node<F>(
        &self,
        node: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
        mut accept: F,
    ) -> Result<Vec<(u64, u64)>, EngineError>
    where
        F: FnMut(u64, f64) -> bool,
    {
        let mut successors: Vec<(u64, u64)> = Vec::new();
        let mut expansion_error: Option<EngineError> = None;
        let _ = self.for_each_search_neighbor(
            node,
            direction,
            edge_type_filter,
            reference_time,
            policy_cutoffs,
            tombstones,
            scratch,
            &mut |neighbor, edge_id, posting_weight| {
                let weight = match self.extract_weight_from_parts(
                    edge_id,
                    posting_weight,
                    weight_field,
                    weight_cache,
                ) {
                    Ok(weight) => weight,
                    Err(err) => {
                        expansion_error = Some(err);
                        return ControlFlow::Break(());
                    }
                };
                if weight < 0.0 || !weight.is_finite() {
                    expansion_error = Some(EngineError::InvalidOperation(
                        "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                    ));
                    return ControlFlow::Break(());
                }
                if accept(neighbor, weight) {
                    successors.push((neighbor, edge_id));
                }
                ControlFlow::Continue(())
            },
        )?;
        if let Some(err) = expansion_error {
            return Err(err);
        }
        Ok(successors)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_weighted_shortest_path_successors(
        &self,
        fwd_dist: &IdMap<f64>,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        best_cost: f64,
        bwd_dist: &IdMap<f64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<IdMap<Vec<(u64, u64)>>, EngineError> {
        let mut successors: IdMap<Vec<(u64, u64)>> = IdMap::default();
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        for (&node, &d) in fwd_dist {
            if path_cost_lt(best_cost, d) {
                continue;
            }

            let next_steps = self.collect_weighted_successors_from_node(
                node,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                policy_cutoffs,
                &mut tombstones,
                &mut scratch,
                &mut weight_cache,
                |neighbor, weight| {
                    let Some(&next_best) = fwd_dist.get(&neighbor) else {
                        return false;
                    };
                    let Some(&remaining_cost) = bwd_dist.get(&neighbor) else {
                        return false;
                    };
                    let next_cost = d + weight;
                    path_cost_eq(next_cost, next_best)
                        && !path_cost_lt(best_cost, next_cost)
                        && path_cost_eq(next_cost + remaining_cost, best_cost)
                },
            )?;
            if !next_steps.is_empty() {
                successors.insert(node, next_steps);
            }
        }

        Ok(successors)
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    fn build_weighted_bounded_shortest_path_successors(
        &self,
        fwd_dist: &[IdMap<f64>],
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        best_cost: f64,
        bwd_best: &IdMap<Vec<LayerBestEntry>>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<IdMap<Vec<(u64, u64)>>>, EngineError> {
        let layer_count = max_depth as usize + 1;
        let mut successors: Vec<IdMap<Vec<(u64, u64)>>> =
            (0..layer_count).map(|_| IdMap::default()).collect();
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        for hops in 0..max_depth {
            let next_hops = hops + 1;
            let remaining_hops = max_depth - next_hops;

            for (&node, &d) in &fwd_dist[hops as usize] {
                if path_cost_lt(best_cost, d) {
                    continue;
                }

                let next_steps = self.collect_weighted_successors_from_node(
                    node,
                    direction,
                    edge_type_filter,
                    weight_field,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut weight_cache,
                    |neighbor, weight| {
                        let Some(&next_best) = fwd_dist[next_hops as usize].get(&neighbor) else {
                            return false;
                        };
                        let Some((_other_hops, remaining_cost)) =
                            Self::best_layer_within(bwd_best, neighbor, remaining_hops)
                        else {
                            return false;
                        };
                        let next_cost = d + weight;
                        path_cost_eq(next_cost, next_best)
                            && !path_cost_lt(best_cost, next_cost)
                            && path_cost_eq(next_cost + remaining_cost, best_cost)
                    },
                )?;
                if !next_steps.is_empty() {
                    successors[hops as usize].insert(node, next_steps);
                }
            }
        }

        Ok(successors)
    }

    #[allow(clippy::too_many_arguments)]
    fn cached_bfs_shortest_path_successors(
        &self,
        cache: &mut [IdMap<Vec<(u64, u64)>>],
        node: u64,
        depth_so_far: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        best_hops: u32,
        bwd_depth: &IdMap<u32>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
    ) -> Result<Vec<(u64, u64)>, EngineError> {
        let cache_layer = &mut cache[depth_so_far as usize];
        Self::cached_successors_for_node(cache_layer, node, || {
            let mut successors: Vec<(u64, u64)> = Vec::new();
            if depth_so_far < best_hops {
                let next_depth = depth_so_far + 1;
                let _ = self.for_each_search_neighbor(
                    node,
                    direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    tombstones,
                    scratch,
                    &mut |neighbor, edge_id, _posting_weight| {
                        let Some(&remaining_hops) = bwd_depth.get(&neighbor) else {
                            return ControlFlow::Continue(());
                        };
                        if next_depth + remaining_hops == best_hops {
                            successors.push((neighbor, edge_id));
                        }
                        ControlFlow::Continue(())
                    },
                )?;
            }
            Ok(successors)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn cached_weighted_shortest_path_successors(
        &self,
        cache: &mut IdMap<Vec<(u64, u64)>>,
        node: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        best_cost: f64,
        fwd_dist: &IdMap<f64>,
        bwd_dist: &IdMap<f64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
    ) -> Result<Vec<(u64, u64)>, EngineError> {
        Self::cached_successors_for_node(cache, node, || {
            let Some(&d) = fwd_dist.get(&node) else {
                return Ok(Vec::new());
            };
            if path_cost_lt(best_cost, d) {
                return Ok(Vec::new());
            }
            self.collect_weighted_successors_from_node(
                node,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                policy_cutoffs,
                tombstones,
                scratch,
                weight_cache,
                |neighbor, weight| {
                    let Some(&next_best) = fwd_dist.get(&neighbor) else {
                        return false;
                    };
                    let Some(&remaining_cost) = bwd_dist.get(&neighbor) else {
                        return false;
                    };
                    let next_cost = d + weight;
                    path_cost_eq(next_cost, next_best)
                        && !path_cost_lt(best_cost, next_cost)
                        && path_cost_eq(next_cost + remaining_cost, best_cost)
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn cached_weighted_bounded_shortest_path_successors(
        &self,
        cache: &mut [IdMap<Vec<(u64, u64)>>],
        node: u64,
        hops_so_far: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        best_cost: f64,
        fwd_dist: &[IdMap<f64>],
        bwd_best: &IdMap<Vec<LayerBestEntry>>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
    ) -> Result<Vec<(u64, u64)>, EngineError> {
        let cache_layer = &mut cache[hops_so_far as usize];
        Self::cached_successors_for_node(cache_layer, node, || {
            let Some(&d) = fwd_dist[hops_so_far as usize].get(&node) else {
                return Ok(Vec::new());
            };
            if path_cost_lt(best_cost, d) || hops_so_far >= max_depth {
                return Ok(Vec::new());
            }

            let next_hops = hops_so_far + 1;
            let remaining_hops = max_depth - next_hops;
            self.collect_weighted_successors_from_node(
                node,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                policy_cutoffs,
                tombstones,
                scratch,
                weight_cache,
                |neighbor, weight| {
                    let Some(&next_best) = fwd_dist[next_hops as usize].get(&neighbor) else {
                        return false;
                    };
                    let Some((_other_hops, remaining_cost)) =
                        Self::best_layer_within(bwd_best, neighbor, remaining_hops)
                    else {
                        return false;
                    };
                    let next_cost = d + weight;
                    path_cost_eq(next_cost, next_best)
                        && !path_cost_lt(best_cost, next_cost)
                        && path_cost_eq(next_cost + remaining_cost, best_cost)
                },
            )
        })
    }

    /// Bidirectional BFS shortest path (unweighted).
    ///
    /// Alternates expanding the smaller frontier. Uses callback-based
    /// adjacency iteration to avoid materializing `Vec<NeighborEntry>`.
    #[allow(clippy::too_many_arguments)]
    fn bfs_shortest_path(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        max_depth: Option<u32>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Option<ShortestPath>, EngineError> {
        let bwd_direction = Self::reverse_direction(direction);
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);

        // Forward state
        let mut fwd_frontier: Vec<u64> = vec![from];
        let mut fwd_visited: IdSet = IdSet::default();
        fwd_visited.insert(from);
        let mut fwd_parent: IdMap<(u64, u64)> = IdMap::default();
        let mut fwd_depth: u32 = 0;
        let mut blocked_hidden: IdSet = IdSet::default();

        // Backward state
        let mut bwd_frontier: Vec<u64> = vec![to];
        let mut bwd_visited: IdSet = IdSet::default();
        bwd_visited.insert(to);
        let mut bwd_parent: IdMap<(u64, u64)> = IdMap::default();
        let mut bwd_depth: u32 = 0;

        let max_d = max_depth.unwrap_or(u32::MAX);

        while !fwd_frontier.is_empty() && !bwd_frontier.is_empty() {
            // Check combined depth against max_depth
            if fwd_depth + bwd_depth >= max_d {
                break;
            }

            let expand_forward = fwd_frontier.len() <= bwd_frontier.len();

            if expand_forward {
                let next_depth = fwd_depth + 1;
                let mut layer_order: Vec<u64> = Vec::new();
                let mut layer_candidates: IdMap<(u64, u64)> = IdMap::default();

                let _ = self.expand_frontier(
                    &fwd_frontier,
                    direction,
                    edge_type_filter,
                    reference_time,
                    &mut tombstones,
                    &mut |source, neighbor, edge_id| {
                        if fwd_visited.contains(&neighbor)
                            || blocked_hidden.contains(&neighbor)
                            || layer_candidates.contains_key(&neighbor)
                        {
                            return ControlFlow::Continue(());
                        }
                        layer_order.push(neighbor);
                        layer_candidates.insert(neighbor, (source, edge_id));
                        if policy_cutoffs.is_none() && bwd_visited.contains(&neighbor) {
                            return ControlFlow::Break(());
                        }
                        ControlFlow::Continue(())
                    },
                )?;

                if let Some(cutoffs) = policy_cutoffs {
                    let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                    layer_order.retain(|neighbor| {
                        if visible.contains(neighbor) {
                            true
                        } else {
                            blocked_hidden.insert(*neighbor);
                            false
                        }
                    });
                }

                let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
                let mut meeting_node: Option<u64> = None;
                for neighbor in layer_order {
                    let (source, edge_id) = layer_candidates.remove(&neighbor).unwrap();
                    fwd_visited.insert(neighbor);
                    fwd_parent.insert(neighbor, (source, edge_id));
                    if meeting_node.is_none() && bwd_visited.contains(&neighbor) {
                        meeting_node = Some(neighbor);
                    } else {
                        next_frontier.push(neighbor);
                    }
                }

                if let Some(meeting) = meeting_node {
                    return Ok(Some(Self::reconstruct_bidir_path(
                        &fwd_parent,
                        &bwd_parent,
                        meeting,
                    )));
                }
                fwd_depth = next_depth;
                fwd_frontier = next_frontier;
            } else {
                let next_depth = bwd_depth + 1;
                let mut layer_order: Vec<u64> = Vec::new();
                let mut layer_candidates: IdMap<(u64, u64)> = IdMap::default();

                let _ = self.expand_frontier(
                    &bwd_frontier,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    &mut tombstones,
                    &mut |source, neighbor, edge_id| {
                        if bwd_visited.contains(&neighbor)
                            || blocked_hidden.contains(&neighbor)
                            || layer_candidates.contains_key(&neighbor)
                        {
                            return ControlFlow::Continue(());
                        }
                        layer_order.push(neighbor);
                        layer_candidates.insert(neighbor, (source, edge_id));
                        if policy_cutoffs.is_none() && fwd_visited.contains(&neighbor) {
                            return ControlFlow::Break(());
                        }
                        ControlFlow::Continue(())
                    },
                )?;

                if let Some(cutoffs) = policy_cutoffs {
                    let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                    layer_order.retain(|neighbor| {
                        if visible.contains(neighbor) {
                            true
                        } else {
                            blocked_hidden.insert(*neighbor);
                            false
                        }
                    });
                }

                let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
                let mut meeting_node: Option<u64> = None;
                for neighbor in layer_order {
                    let (source, edge_id) = layer_candidates.remove(&neighbor).unwrap();
                    bwd_visited.insert(neighbor);
                    bwd_parent.insert(neighbor, (source, edge_id));
                    if meeting_node.is_none() && fwd_visited.contains(&neighbor) {
                        meeting_node = Some(neighbor);
                    } else {
                        next_frontier.push(neighbor);
                    }
                }

                if let Some(meeting) = meeting_node {
                    return Ok(Some(Self::reconstruct_bidir_path(
                        &fwd_parent,
                        &bwd_parent,
                        meeting,
                    )));
                }
                bwd_depth = next_depth;
                bwd_frontier = next_frontier;
            }
        }

        Ok(None)
    }

    /// Bidirectional BFS connectivity check (no parent tracking).
    #[allow(clippy::too_many_arguments)]
    fn bfs_is_connected(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        max_depth: Option<u32>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<bool, EngineError> {
        let bwd_direction = Self::reverse_direction(direction);
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);

        let mut fwd_frontier: Vec<u64> = vec![from];
        let mut fwd_visited: IdSet = IdSet::default();
        fwd_visited.insert(from);
        let mut fwd_depth: u32 = 0;
        let mut blocked_hidden: IdSet = IdSet::default();

        let mut bwd_frontier: Vec<u64> = vec![to];
        let mut bwd_visited: IdSet = IdSet::default();
        bwd_visited.insert(to);
        let mut bwd_depth: u32 = 0;

        let max_d = max_depth.unwrap_or(u32::MAX);

        while !fwd_frontier.is_empty() && !bwd_frontier.is_empty() {
            if fwd_depth + bwd_depth >= max_d {
                break;
            }

            let expand_forward = fwd_frontier.len() <= bwd_frontier.len();

            if expand_forward {
                let next_depth = fwd_depth + 1;
                let mut layer_order: Vec<u64> = Vec::new();
                let mut layer_candidates: IdSet = IdSet::default();

                let _ = self.expand_frontier(
                    &fwd_frontier,
                    direction,
                    edge_type_filter,
                    reference_time,
                    &mut tombstones,
                    &mut |_source, neighbor, _edge_id| {
                        if !fwd_visited.contains(&neighbor)
                            && !blocked_hidden.contains(&neighbor)
                            && layer_candidates.insert(neighbor)
                        {
                            layer_order.push(neighbor);
                            if policy_cutoffs.is_none() && bwd_visited.contains(&neighbor) {
                                return ControlFlow::Break(());
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;

                if let Some(cutoffs) = policy_cutoffs {
                    let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                    layer_order.retain(|neighbor| {
                        if visible.contains(neighbor) {
                            true
                        } else {
                            blocked_hidden.insert(*neighbor);
                            false
                        }
                    });
                }

                let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
                let mut found = false;
                for neighbor in layer_order {
                    fwd_visited.insert(neighbor);
                    if !found && bwd_visited.contains(&neighbor) {
                        found = true;
                    } else {
                        next_frontier.push(neighbor);
                    }
                }

                if found {
                    return Ok(true);
                }
                fwd_depth = next_depth;
                fwd_frontier = next_frontier;
            } else {
                let next_depth = bwd_depth + 1;
                let mut layer_order: Vec<u64> = Vec::new();
                let mut layer_candidates: IdSet = IdSet::default();

                let _ = self.expand_frontier(
                    &bwd_frontier,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    &mut tombstones,
                    &mut |_source, neighbor, _edge_id| {
                        if !bwd_visited.contains(&neighbor)
                            && !blocked_hidden.contains(&neighbor)
                            && layer_candidates.insert(neighbor)
                        {
                            layer_order.push(neighbor);
                            if policy_cutoffs.is_none() && fwd_visited.contains(&neighbor) {
                                return ControlFlow::Break(());
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;

                if let Some(cutoffs) = policy_cutoffs {
                    let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                    layer_order.retain(|neighbor| {
                        if visible.contains(neighbor) {
                            true
                        } else {
                            blocked_hidden.insert(*neighbor);
                            false
                        }
                    });
                }

                let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
                let mut found = false;
                for neighbor in layer_order {
                    bwd_visited.insert(neighbor);
                    if !found && fwd_visited.contains(&neighbor) {
                        found = true;
                    } else {
                        next_frontier.push(neighbor);
                    }
                }

                if found {
                    return Ok(true);
                }
                bwd_depth = next_depth;
                bwd_frontier = next_frontier;
            }
        }

        Ok(false)
    }

    /// Extract a weight value from an adjacency entry.
    ///
    /// - `"weight"` → reads the adjacency posting weight directly (zero hydration)
    /// - any other field → hydrates the edge and reads `props[field]` as f64
    fn extract_weight_from_parts(
        &self,
        edge_id: u64,
        posting_weight: f32,
        weight_field: &str,
        weight_cache: &mut IdMap<f64>,
    ) -> Result<f64, EngineError> {
        if weight_field == "weight" {
            return Ok(posting_weight as f64);
        }
        if let Some(&weight) = weight_cache.get(&edge_id) {
            return Ok(weight);
        }
        let edge = self.get_edge(edge_id)?;
        let weight = match edge {
            Some(e) => match e.props.get(weight_field) {
                Some(PropValue::Float(f)) => Ok(*f),
                Some(PropValue::Int(i)) => Ok(*i as f64),
                Some(PropValue::UInt(u)) => Ok(*u as f64),
                Some(_) => Err(EngineError::InvalidOperation(format!(
                    "weight field '{}' is not numeric on edge {}",
                    weight_field, edge_id
                ))),
                None => Err(EngineError::InvalidOperation(format!(
                    "edge {} has no property '{}'",
                    edge_id, weight_field
                ))),
            },
            None => Err(EngineError::InvalidOperation(format!(
                "edge {} not found during weight extraction",
                edge_id
            ))),
        }?;
        weight_cache.insert(edge_id, weight);
        Ok(weight)
    }

    /// Query-local visibility check for Dijkstra-style single-node expansion.
    fn node_visible_for_search(
        &self,
        node_id: u64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        visible_cache: &mut IdMap<bool>,
    ) -> Result<bool, EngineError> {
        if let Some(&visible) = visible_cache.get(&node_id) {
            return Ok(visible);
        }
        let visible = match policy_cutoffs {
            None => true,
            Some(cutoffs) => match self.get_node_raw(node_id)? {
                Some(node) => !cutoffs.excludes(&node),
                None => false,
            },
        };
        visible_cache.insert(node_id, visible);
        Ok(visible)
    }

    /// Iterate live adjacency entries for one node with no intermediate neighbor Vec.
    #[allow(clippy::too_many_arguments)]
    fn for_each_search_neighbor<F>(
        &self,
        node_id: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        on_neighbor: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32) -> ControlFlow<()>, // (neighbor_id, edge_id, posting_weight)
    {
        scratch.seen_edges.clear();
        let mut callback_error: Option<EngineError> = None;

        if self
            .memtable
            .for_each_adj_entry(
                node_id,
                direction,
                edge_type_filter,
                &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                    scratch.seen_edges.insert(edge_id);
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    match self.node_visible_for_search(
                        neighbor_id,
                        policy_cutoffs,
                        &mut scratch.visible_cache,
                    ) {
                        Ok(true) => {}
                        Ok(false) => return ControlFlow::Continue(()),
                        Err(err) => {
                            callback_error = Some(err);
                            return ControlFlow::Break(());
                        }
                    }
                    on_neighbor(neighbor_id, edge_id, weight)
                },
            )
            .is_break()
        {
            if let Some(err) = callback_error {
                return Err(err);
            }
            return Ok(ControlFlow::Break(()));
        }
        if let Some(err) = callback_error {
            return Err(err);
        }

        for seg in &self.segments {
            callback_error = None;
            if seg
                .for_each_adj_posting(
                    node_id,
                    direction,
                    edge_type_filter,
                    &mut |edge_id, neighbor_id, weight, valid_from, valid_to| {
                        if !scratch.seen_edges.insert(edge_id) {
                            return ControlFlow::Continue(());
                        }
                        if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                            return ControlFlow::Continue(());
                        }
                        if tombstones.is_edge_deleted(edge_id)
                            || tombstones.is_node_deleted(node_id)
                            || tombstones.is_node_deleted(neighbor_id)
                        {
                            return ControlFlow::Continue(());
                        }
                        match self.node_visible_for_search(
                            neighbor_id,
                            policy_cutoffs,
                            &mut scratch.visible_cache,
                        ) {
                            Ok(true) => {}
                            Ok(false) => return ControlFlow::Continue(()),
                            Err(err) => {
                                callback_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        }
                        on_neighbor(neighbor_id, edge_id, weight)
                    },
                )?
                .is_break()
            {
                if let Some(err) = callback_error {
                    return Err(err);
                }
                return Ok(ControlFlow::Break(()));
            }
            if let Some(err) = callback_error {
                return Err(err);
            }
        }

        Ok(ControlFlow::Continue(()))
    }

    /// Bidirectional Dijkstra shortest path (weighted).
    ///
    /// Uses two min-heaps expanding from `from` and `to`. Terminates when
    /// `fwd_min + bwd_min >= mu` where `mu` is the best known path cost.
    /// Lazy deletion: skip popped entries where `d > dist[v]`.
    #[allow(clippy::too_many_arguments)]
    fn dijkstra_shortest_path(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Option<ShortestPath>, EngineError> {
        if let Some(max_hops) = max_depth {
            return self.dijkstra_shortest_path_bounded(
                from,
                to,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                max_hops,
                max_cost,
                policy_cutoffs,
            );
        }

        let bwd_direction = Self::reverse_direction(direction);
        let mut fwd_heap: BinaryHeap<Reverse<(OrdF64, u64)>> = BinaryHeap::new();
        let mut fwd_dist: IdMap<f64> = IdMap::default();
        let mut fwd_parent: IdMap<(u64, u64)> = IdMap::default();
        let mut fwd_settled: IdSet = IdSet::default();
        let mut bwd_heap: BinaryHeap<Reverse<(OrdF64, u64)>> = BinaryHeap::new();
        let mut bwd_dist: IdMap<f64> = IdMap::default();
        let mut bwd_parent: IdMap<(u64, u64)> = IdMap::default();
        let mut bwd_settled: IdSet = IdSet::default();

        fwd_heap.push(Reverse((OrdF64(0.0), from)));
        bwd_heap.push(Reverse((OrdF64(0.0), to)));
        fwd_dist.insert(from, 0.0);
        bwd_dist.insert(to, 0.0);

        let mut mu = f64::INFINITY;
        let mut meeting_node: Option<u64> = None;
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        loop {
            let fwd_min = fwd_heap.peek().map(|Reverse((OrdF64(d), _))| *d);
            let bwd_min = bwd_heap.peek().map(|Reverse((OrdF64(d), _))| *d);

            match (fwd_min, bwd_min) {
                (None, None) => break,
                (Some(f), Some(b)) if path_cost_le(mu, f + b) => break,
                (Some(f), None) if path_cost_le(mu, f) => break,
                (None, Some(b)) if path_cost_le(mu, b) => break,
                _ => {}
            }

            let expand_forward = match (fwd_min, bwd_min) {
                (Some(f), Some(b)) => f <= b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };

            if expand_forward {
                let Reverse((OrdF64(d), node)) = fwd_heap.pop().unwrap();
                if path_cost_lt(fwd_dist.get(&node).copied().unwrap_or(f64::INFINITY), d) {
                    continue;
                }
                if !fwd_settled.insert(node) {
                    continue;
                }
                if let Some(&backward_cost) = bwd_dist.get(&node) {
                    let total_cost = d + backward_cost;
                    if path_cost_lt(total_cost, mu) {
                        mu = total_cost;
                        meeting_node = Some(node);
                    }
                }
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if !mu.is_infinite() && !path_cost_lt(new_dist, mu) {
                            return ControlFlow::Continue(());
                        }
                        let old = fwd_dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            fwd_dist.insert(neighbor, new_dist);
                            fwd_parent.insert(neighbor, (node, edge_id));
                            fwd_heap.push(Reverse((OrdF64(new_dist), neighbor)));
                            if let Some(&backward_cost) = bwd_dist.get(&neighbor) {
                                let total_cost = new_dist + backward_cost;
                                if path_cost_lt(total_cost, mu) {
                                    mu = total_cost;
                                    meeting_node = Some(neighbor);
                                }
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            } else {
                let Reverse((OrdF64(d), node)) = bwd_heap.pop().unwrap();
                if path_cost_lt(bwd_dist.get(&node).copied().unwrap_or(f64::INFINITY), d) {
                    continue;
                }
                if !bwd_settled.insert(node) {
                    continue;
                }
                if let Some(&forward_cost) = fwd_dist.get(&node) {
                    let total_cost = forward_cost + d;
                    if path_cost_lt(total_cost, mu) {
                        mu = total_cost;
                        meeting_node = Some(node);
                    }
                }
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if !mu.is_infinite() && !path_cost_lt(new_dist, mu) {
                            return ControlFlow::Continue(());
                        }
                        let old = bwd_dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            bwd_dist.insert(neighbor, new_dist);
                            bwd_parent.insert(neighbor, (node, edge_id));
                            bwd_heap.push(Reverse((OrdF64(new_dist), neighbor)));
                            if let Some(&forward_cost) = fwd_dist.get(&neighbor) {
                                let total_cost = forward_cost + new_dist;
                                if path_cost_lt(total_cost, mu) {
                                    mu = total_cost;
                                    meeting_node = Some(neighbor);
                                }
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            }
        }

        let Some(meeting) = meeting_node else {
            return Ok(None);
        };
        if let Some(max_total_cost) = max_cost {
            if path_cost_lt(max_total_cost, mu) {
                return Ok(None);
            }
        }

        let mut path = Self::reconstruct_bidir_path(&fwd_parent, &bwd_parent, meeting);
        path.total_cost = mu;
        Ok(Some(path))
    }

    #[allow(clippy::too_many_arguments)]
    fn dijkstra_shortest_path_bounded(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        max_cost: Option<f64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Option<ShortestPath>, EngineError> {
        let bwd_direction = Self::reverse_direction(direction);
        let layer_count = max_depth as usize + 1;
        let mut fwd_heap: BinaryHeap<Reverse<(OrdF64, u32, u64)>> = BinaryHeap::new();
        let mut bwd_heap: BinaryHeap<Reverse<(OrdF64, u32, u64)>> = BinaryHeap::new();
        let mut fwd_dist: Vec<IdMap<f64>> = (0..layer_count).map(|_| IdMap::default()).collect();
        let mut bwd_dist: Vec<IdMap<f64>> = (0..layer_count).map(|_| IdMap::default()).collect();
        let mut fwd_parent: Vec<IdMap<(u64, u64)>> =
            (0..layer_count).map(|_| IdMap::default()).collect();
        let mut bwd_parent: Vec<IdMap<(u64, u64)>> =
            (0..layer_count).map(|_| IdMap::default()).collect();
        let mut fwd_best: IdMap<Vec<LayerBestEntry>> = IdMap::default();
        let mut bwd_best: IdMap<Vec<LayerBestEntry>> = IdMap::default();

        fwd_dist[0].insert(from, 0.0);
        bwd_dist[0].insert(to, 0.0);
        Self::update_layer_best(&mut fwd_best, from, 0, 0.0);
        Self::update_layer_best(&mut bwd_best, to, 0, 0.0);
        fwd_heap.push(Reverse((OrdF64(0.0), 0, from)));
        bwd_heap.push(Reverse((OrdF64(0.0), 0, to)));

        let mut mu = f64::INFINITY;
        let mut best_meeting: Option<(u64, u32, u32)> = None;
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        loop {
            let fwd_min = fwd_heap.peek().map(|Reverse((OrdF64(d), _, _))| *d);
            let bwd_min = bwd_heap.peek().map(|Reverse((OrdF64(d), _, _))| *d);

            match (fwd_min, bwd_min) {
                (None, None) => break,
                (Some(f), Some(b)) if path_cost_le(mu, f + b) => break,
                (Some(f), None) if path_cost_le(mu, f) => break,
                (None, Some(b)) if path_cost_le(mu, b) => break,
                _ => {}
            }

            let expand_forward = match (fwd_min, bwd_min) {
                (Some(f), Some(b)) => f <= b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };

            if expand_forward {
                let Reverse((OrdF64(d), hops, node)) = fwd_heap.pop().unwrap();
                if path_cost_lt(
                    fwd_dist[hops as usize]
                        .get(&node)
                        .copied()
                        .unwrap_or(f64::INFINITY),
                    d,
                ) {
                    continue;
                }
                Self::update_layered_meeting(
                    &bwd_best,
                    node,
                    hops,
                    d,
                    max_depth,
                    true,
                    &mut mu,
                    &mut best_meeting,
                );
                if hops == max_depth {
                    continue;
                }
                let next_hops = hops + 1;
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if !mu.is_infinite() && !path_cost_lt(new_dist, mu) {
                            return ControlFlow::Continue(());
                        }
                        let old = fwd_dist[next_hops as usize]
                            .get(&neighbor)
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            fwd_dist[next_hops as usize].insert(neighbor, new_dist);
                            Self::update_layer_best(&mut fwd_best, neighbor, next_hops, new_dist);
                            fwd_parent[next_hops as usize].insert(neighbor, (node, edge_id));
                            fwd_heap.push(Reverse((OrdF64(new_dist), next_hops, neighbor)));
                            Self::update_layered_meeting(
                                &bwd_best,
                                neighbor,
                                next_hops,
                                new_dist,
                                max_depth,
                                true,
                                &mut mu,
                                &mut best_meeting,
                            );
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            } else {
                let Reverse((OrdF64(d), hops, node)) = bwd_heap.pop().unwrap();
                if path_cost_lt(
                    bwd_dist[hops as usize]
                        .get(&node)
                        .copied()
                        .unwrap_or(f64::INFINITY),
                    d,
                ) {
                    continue;
                }
                Self::update_layered_meeting(
                    &fwd_best,
                    node,
                    hops,
                    d,
                    max_depth,
                    false,
                    &mut mu,
                    &mut best_meeting,
                );
                if hops == max_depth {
                    continue;
                }
                let next_hops = hops + 1;
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if !mu.is_infinite() && !path_cost_lt(new_dist, mu) {
                            return ControlFlow::Continue(());
                        }
                        let old = bwd_dist[next_hops as usize]
                            .get(&neighbor)
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            bwd_dist[next_hops as usize].insert(neighbor, new_dist);
                            Self::update_layer_best(&mut bwd_best, neighbor, next_hops, new_dist);
                            bwd_parent[next_hops as usize].insert(neighbor, (node, edge_id));
                            bwd_heap.push(Reverse((OrdF64(new_dist), next_hops, neighbor)));
                            Self::update_layered_meeting(
                                &fwd_best,
                                neighbor,
                                next_hops,
                                new_dist,
                                max_depth,
                                false,
                                &mut mu,
                                &mut best_meeting,
                            );
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            }
        }

        let Some((meeting, fwd_hops, bwd_hops)) = best_meeting else {
            return Ok(None);
        };
        if let Some(max_total_cost) = max_cost {
            if path_cost_lt(max_total_cost, mu) {
                return Ok(None);
            }
        }

        Ok(Some(Self::reconstruct_bidir_layered_path(
            &fwd_parent,
            &bwd_parent,
            meeting,
            fwd_hops,
            bwd_hops,
            mu,
        )))
    }

    /// Enumerate all shortest paths between two nodes.
    ///
    /// Returns all paths with the same minimum cost. Uses bidirectional
    /// BFS (if `weight_field` is `None`) or bidirectional Dijkstra (if
    /// weighted), then enumerates only paths consistent with the optimal
    /// forward/backward search frontiers.
    ///
    /// # Arguments
    /// - `max_paths`: maximum number of paths to return (default 100, 0 = no limit)
    ///
    /// # Examples
    ///
    /// ```
    /// # use overgraph::{DatabaseEngine, DbOptions, Direction};
    /// # use std::collections::BTreeMap;
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let mut db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
    /// let a = db.upsert_node(1, "a", BTreeMap::new(), 1.0).unwrap();
    /// let b = db.upsert_node(1, "b", BTreeMap::new(), 1.0).unwrap();
    /// let c = db.upsert_node(1, "c", BTreeMap::new(), 1.0).unwrap();
    /// let d = db.upsert_node(1, "d", BTreeMap::new(), 1.0).unwrap();
    /// // Diamond: a->b->d and a->c->d (two equal-cost paths)
    /// db.upsert_edge(a, b, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    /// db.upsert_edge(a, c, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    /// db.upsert_edge(b, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    /// db.upsert_edge(c, d, 1, BTreeMap::new(), 1.0, None, None).unwrap();
    ///
    /// let paths = db.all_shortest_paths(
    ///     a, d, Direction::Outgoing, None, None, None, None, None, None,
    /// ).unwrap();
    /// assert_eq!(paths.len(), 2); // a->b->d and a->c->d
    /// assert!(paths.iter().all(|p| p.total_cost == 2.0));
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn all_shortest_paths(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
        max_paths: Option<usize>,
    ) -> Result<Vec<ShortestPath>, EngineError> {
        let policy_cutoffs = self.query_policy_cutoffs();
        if !self.path_endpoints_visible(from, to, policy_cutoffs.as_ref())? {
            return Ok(vec![]);
        }
        if from == to {
            return Ok(vec![ShortestPath {
                nodes: vec![from],
                edges: vec![],
                total_cost: 0.0,
            }]);
        }
        let reference_time = at_epoch.unwrap_or_else(now_millis);
        let paths_cap = max_paths.unwrap_or(100);

        match weight_field {
            None => self.bfs_all_shortest_paths(
                from,
                to,
                direction,
                edge_type_filter,
                reference_time,
                max_depth,
                paths_cap,
                policy_cutoffs.as_ref(),
            ),
            Some(wf) => self.dijkstra_all_shortest_paths(
                from,
                to,
                direction,
                edge_type_filter,
                wf,
                reference_time,
                max_depth,
                max_cost,
                paths_cap,
                policy_cutoffs.as_ref(),
            ),
        }
    }

    /// Bidirectional BFS all shortest paths.
    #[allow(clippy::too_many_arguments)]
    fn bfs_all_shortest_paths(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        max_depth: Option<u32>,
        max_paths: usize,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<ShortestPath>, EngineError> {
        let bwd_direction = Self::reverse_direction(direction);
        let max_hops = max_depth.unwrap_or(u32::MAX);
        let paths_cap = if max_paths == 0 {
            usize::MAX
        } else {
            max_paths
        };
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut blocked_hidden: IdSet = IdSet::default();
        let mut fwd_frontier: Vec<u64> = vec![from];
        let mut bwd_frontier: Vec<u64> = vec![to];
        let mut fwd_depth: IdMap<u32> = IdMap::default();
        let mut bwd_depth: IdMap<u32> = IdMap::default();
        fwd_depth.insert(from, 0);
        bwd_depth.insert(to, 0);
        let mut fwd_layer: u32 = 0;
        let mut bwd_layer: u32 = 0;
        let mut best_hops: Option<u32> = None;

        while !fwd_frontier.is_empty() && !bwd_frontier.is_empty() {
            if let Some(best) = best_hops {
                if fwd_layer + bwd_layer >= best {
                    break;
                }
            } else if fwd_layer + bwd_layer >= max_hops {
                break;
            }

            let fwd_can_expand = fwd_layer < max_hops && !fwd_frontier.is_empty();
            let bwd_can_expand = bwd_layer < max_hops && !bwd_frontier.is_empty();
            if !fwd_can_expand && !bwd_can_expand {
                break;
            }

            let expand_forward = if !bwd_can_expand {
                true
            } else if !fwd_can_expand {
                false
            } else {
                fwd_frontier.len() <= bwd_frontier.len()
            };

            let frontier = if expand_forward {
                &fwd_frontier
            } else {
                &bwd_frontier
            };
            let next_depth = if expand_forward {
                fwd_layer + 1
            } else {
                bwd_layer + 1
            };
            let mut layer_order: Vec<u64> = Vec::new();
            let mut layer_candidates: IdSet = IdSet::default();

            let _ = self.expand_frontier(
                frontier,
                if expand_forward {
                    direction
                } else {
                    bwd_direction
                },
                edge_type_filter,
                reference_time,
                &mut tombstones,
                &mut |_source, neighbor, _edge_id| {
                    let already_seen = if expand_forward {
                        fwd_depth.contains_key(&neighbor)
                    } else {
                        bwd_depth.contains_key(&neighbor)
                    };
                    if already_seen
                        || blocked_hidden.contains(&neighbor)
                        || !layer_candidates.insert(neighbor)
                    {
                        return ControlFlow::Continue(());
                    }
                    layer_order.push(neighbor);
                    ControlFlow::Continue(())
                },
            )?;

            if let Some(cutoffs) = policy_cutoffs {
                let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                layer_order.retain(|neighbor| {
                    if visible.contains(neighbor) {
                        true
                    } else {
                        blocked_hidden.insert(*neighbor);
                        false
                    }
                });
            }

            let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
            for neighbor in layer_order {
                if expand_forward {
                    fwd_depth.insert(neighbor, next_depth);
                    if let Some(&other_depth) = bwd_depth.get(&neighbor) {
                        let total_hops = next_depth + other_depth;
                        if total_hops <= max_hops
                            && best_hops.map(|best| total_hops < best).unwrap_or(true)
                        {
                            best_hops = Some(total_hops);
                        }
                    }
                } else {
                    bwd_depth.insert(neighbor, next_depth);
                    if let Some(&other_depth) = fwd_depth.get(&neighbor) {
                        let total_hops = next_depth + other_depth;
                        if total_hops <= max_hops
                            && best_hops.map(|best| total_hops < best).unwrap_or(true)
                        {
                            best_hops = Some(total_hops);
                        }
                    }
                }
                next_frontier.push(neighbor);
            }

            if expand_forward {
                fwd_frontier = next_frontier;
                fwd_layer = next_depth;
            } else {
                bwd_frontier = next_frontier;
                bwd_layer = next_depth;
            }
        }

        let Some(best_hops) = best_hops else {
            return Ok(vec![]);
        };

        while !bwd_frontier.is_empty() && bwd_layer < best_hops {
            let next_depth = bwd_layer + 1;
            let mut layer_order: Vec<u64> = Vec::new();
            let mut layer_candidates: IdSet = IdSet::default();
            let _ = self.expand_frontier(
                &bwd_frontier,
                bwd_direction,
                edge_type_filter,
                reference_time,
                &mut tombstones,
                &mut |_source, neighbor, _edge_id| {
                    if bwd_depth.contains_key(&neighbor)
                        || blocked_hidden.contains(&neighbor)
                        || !layer_candidates.insert(neighbor)
                    {
                        return ControlFlow::Continue(());
                    }
                    layer_order.push(neighbor);
                    ControlFlow::Continue(())
                },
            )?;
            if let Some(cutoffs) = policy_cutoffs {
                let visible = self.visible_node_ids(&layer_order, Some(cutoffs))?;
                layer_order.retain(|neighbor| {
                    if visible.contains(neighbor) {
                        true
                    } else {
                        blocked_hidden.insert(*neighbor);
                        false
                    }
                });
            }
            let mut next_frontier: Vec<u64> = Vec::with_capacity(layer_order.len());
            for neighbor in layer_order {
                bwd_depth.insert(neighbor, next_depth);
                next_frontier.push(neighbor);
            }
            bwd_frontier = next_frontier;
            bwd_layer = next_depth;
        }

        let mut results: Vec<ShortestPath> = Vec::new();
        let mut path_nodes: Vec<u64> = vec![from];
        let mut path_edges: Vec<u64> = Vec::new();
        if max_paths == 0 {
            let successors = self.build_bfs_shortest_path_successors(
                from,
                direction,
                edge_type_filter,
                reference_time,
                best_hops,
                &bwd_depth,
                policy_cutoffs,
            )?;
            self.enumerate_bfs_shortest_paths_dfs(
                from,
                to,
                0,
                best_hops,
                &successors,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut results,
            )?;
        } else {
            let mut lazy_scratch = SearchNeighborScratch::default();
            let mut successors_cache: Vec<IdMap<Vec<(u64, u64)>>> =
                (0..=best_hops).map(|_| IdMap::default()).collect();
            self.enumerate_bfs_shortest_paths_dfs_lazy(
                from,
                to,
                0,
                direction,
                edge_type_filter,
                reference_time,
                best_hops,
                &bwd_depth,
                policy_cutoffs,
                &mut tombstones,
                &mut lazy_scratch,
                &mut successors_cache,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut results,
            )?;
        }
        Ok(results)
    }

    /// Bidirectional Dijkstra all shortest paths.
    #[allow(clippy::too_many_arguments)]
    fn dijkstra_all_shortest_paths(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
        max_paths: usize,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<ShortestPath>, EngineError> {
        if let Some(max_hops) = max_depth {
            return self.dijkstra_all_shortest_paths_bounded(
                from,
                to,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                max_hops,
                max_cost,
                max_paths,
                policy_cutoffs,
            );
        }

        let bwd_direction = Self::reverse_direction(direction);
        let mut fwd_heap: BinaryHeap<Reverse<(OrdF64, u64)>> = BinaryHeap::new();
        let mut fwd_dist: IdMap<f64> = IdMap::default();
        let mut fwd_settled: IdSet = IdSet::default();
        let mut bwd_heap: BinaryHeap<Reverse<(OrdF64, u64)>> = BinaryHeap::new();
        let mut bwd_dist: IdMap<f64> = IdMap::default();
        let mut bwd_settled: IdSet = IdSet::default();
        fwd_heap.push(Reverse((OrdF64(0.0), from)));
        bwd_heap.push(Reverse((OrdF64(0.0), to)));
        fwd_dist.insert(from, 0.0);
        bwd_dist.insert(to, 0.0);

        let mut mu = f64::INFINITY;
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        loop {
            let fwd_min = fwd_heap.peek().map(|Reverse((OrdF64(d), _))| *d);
            let bwd_min = bwd_heap.peek().map(|Reverse((OrdF64(d), _))| *d);

            match (fwd_min, bwd_min) {
                (None, None) => break,
                (Some(f), Some(b)) if path_cost_le(mu, f + b) => break,
                (Some(f), None) if path_cost_le(mu, f) => break,
                (None, Some(b)) if path_cost_le(mu, b) => break,
                _ => {}
            }

            let expand_forward = match (fwd_min, bwd_min) {
                (Some(f), Some(b)) => f <= b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };

            if expand_forward {
                let Reverse((OrdF64(d), node)) = fwd_heap.pop().unwrap();
                if path_cost_lt(fwd_dist.get(&node).copied().unwrap_or(f64::INFINITY), d) {
                    continue;
                }
                if !fwd_settled.insert(node) {
                    continue;
                }
                if let Some(&backward_cost) = bwd_dist.get(&node) {
                    let total_cost = d + backward_cost;
                    if path_cost_lt(total_cost, mu) {
                        mu = total_cost;
                    }
                }
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if path_cost_lt(mu, new_dist) {
                            return ControlFlow::Continue(());
                        }
                        let old = fwd_dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            fwd_dist.insert(neighbor, new_dist);
                            fwd_heap.push(Reverse((OrdF64(new_dist), neighbor)));
                            if let Some(&backward_cost) = bwd_dist.get(&neighbor) {
                                let total_cost = new_dist + backward_cost;
                                if path_cost_lt(total_cost, mu) {
                                    mu = total_cost;
                                }
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            } else {
                let Reverse((OrdF64(d), node)) = bwd_heap.pop().unwrap();
                if path_cost_lt(bwd_dist.get(&node).copied().unwrap_or(f64::INFINITY), d) {
                    continue;
                }
                if !bwd_settled.insert(node) {
                    continue;
                }
                if let Some(&forward_cost) = fwd_dist.get(&node) {
                    let total_cost = forward_cost + d;
                    if path_cost_lt(total_cost, mu) {
                        mu = total_cost;
                    }
                }
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if path_cost_lt(mu, new_dist) {
                            return ControlFlow::Continue(());
                        }
                        let old = bwd_dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            bwd_dist.insert(neighbor, new_dist);
                            bwd_heap.push(Reverse((OrdF64(new_dist), neighbor)));
                            if let Some(&forward_cost) = fwd_dist.get(&neighbor) {
                                let total_cost = forward_cost + new_dist;
                                if path_cost_lt(total_cost, mu) {
                                    mu = total_cost;
                                }
                            }
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            }
        }

        if mu.is_infinite() {
            return Ok(vec![]);
        }
        if let Some(max_total_cost) = max_cost {
            if path_cost_lt(max_total_cost, mu) {
                return Ok(vec![]);
            }
        }

        self.complete_weighted_dijkstra_side_up_to_cost(
            &mut bwd_heap,
            &mut bwd_dist,
            &mut bwd_settled,
            bwd_direction,
            edge_type_filter,
            weight_field,
            reference_time,
            mu,
            policy_cutoffs,
            &mut tombstones,
            &mut scratch,
            &mut weight_cache,
        )?;
        self.complete_weighted_dijkstra_side_up_to_cost(
            &mut fwd_heap,
            &mut fwd_dist,
            &mut fwd_settled,
            direction,
            edge_type_filter,
            weight_field,
            reference_time,
            mu,
            policy_cutoffs,
            &mut tombstones,
            &mut scratch,
            &mut weight_cache,
        )?;

        let paths_cap = if max_paths == 0 {
            usize::MAX
        } else {
            max_paths
        };
        let mut results: Vec<ShortestPath> = Vec::new();
        let mut path_nodes: Vec<u64> = vec![from];
        let mut path_edges: Vec<u64> = Vec::new();
        let mut visited: IdSet = IdSet::default();
        visited.insert(from);
        if max_paths == 0 {
            let successors = self.build_weighted_shortest_path_successors(
                &fwd_dist,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                mu,
                &bwd_dist,
                policy_cutoffs,
            )?;
            self.enumerate_weighted_shortest_paths_dfs(
                from,
                to,
                mu,
                &successors,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut visited,
                &mut results,
            )?;
        } else {
            let mut successors_cache: IdMap<Vec<(u64, u64)>> = IdMap::default();
            self.enumerate_weighted_shortest_paths_dfs_lazy(
                from,
                to,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                mu,
                &fwd_dist,
                &bwd_dist,
                policy_cutoffs,
                &mut tombstones,
                &mut scratch,
                &mut weight_cache,
                &mut successors_cache,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut visited,
                &mut results,
            )?;
        }
        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn dijkstra_all_shortest_paths_bounded(
        &self,
        from: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        max_cost: Option<f64>,
        max_paths: usize,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<ShortestPath>, EngineError> {
        let bwd_direction = Self::reverse_direction(direction);
        let layer_count = max_depth as usize + 1;
        let mut fwd_heap: BinaryHeap<Reverse<(OrdF64, u32, u64)>> = BinaryHeap::new();
        let mut bwd_heap: BinaryHeap<Reverse<(OrdF64, u32, u64)>> = BinaryHeap::new();
        let mut fwd_dist: Vec<IdMap<f64>> = (0..layer_count).map(|_| IdMap::default()).collect();
        let mut bwd_dist: Vec<IdMap<f64>> = (0..layer_count).map(|_| IdMap::default()).collect();
        let mut fwd_settled: Vec<IdSet> = (0..layer_count).map(|_| IdSet::default()).collect();
        let mut bwd_settled: Vec<IdSet> = (0..layer_count).map(|_| IdSet::default()).collect();
        let mut fwd_best: IdMap<Vec<LayerBestEntry>> = IdMap::default();
        let mut bwd_best: IdMap<Vec<LayerBestEntry>> = IdMap::default();

        fwd_dist[0].insert(from, 0.0);
        bwd_dist[0].insert(to, 0.0);
        Self::update_layer_best(&mut fwd_best, from, 0, 0.0);
        Self::update_layer_best(&mut bwd_best, to, 0, 0.0);
        fwd_heap.push(Reverse((OrdF64(0.0), 0, from)));
        bwd_heap.push(Reverse((OrdF64(0.0), 0, to)));

        let mut mu = f64::INFINITY;
        let mut best_meeting: Option<(u64, u32, u32)> = None;
        let mut tombstones = TraversalTombstoneView::new(&self.memtable, &self.segments);
        let mut scratch = SearchNeighborScratch::default();
        let mut weight_cache: IdMap<f64> = IdMap::default();

        loop {
            let fwd_min = fwd_heap.peek().map(|Reverse((OrdF64(d), _, _))| *d);
            let bwd_min = bwd_heap.peek().map(|Reverse((OrdF64(d), _, _))| *d);

            match (fwd_min, bwd_min) {
                (None, None) => break,
                (Some(f), Some(b)) if path_cost_le(mu, f + b) => break,
                (Some(f), None) if path_cost_le(mu, f) => break,
                (None, Some(b)) if path_cost_le(mu, b) => break,
                _ => {}
            }

            let expand_forward = match (fwd_min, bwd_min) {
                (Some(f), Some(b)) => f <= b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };

            if expand_forward {
                let Reverse((OrdF64(d), hops, node)) = fwd_heap.pop().unwrap();
                if path_cost_lt(
                    fwd_dist[hops as usize]
                        .get(&node)
                        .copied()
                        .unwrap_or(f64::INFINITY),
                    d,
                ) {
                    continue;
                }
                if !fwd_settled[hops as usize].insert(node) {
                    continue;
                }
                Self::update_layered_meeting(
                    &bwd_best,
                    node,
                    hops,
                    d,
                    max_depth,
                    true,
                    &mut mu,
                    &mut best_meeting,
                );
                if hops == max_depth {
                    continue;
                }
                let next_hops = hops + 1;
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if path_cost_lt(mu, new_dist) {
                            return ControlFlow::Continue(());
                        }
                        let old = fwd_dist[next_hops as usize]
                            .get(&neighbor)
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            fwd_dist[next_hops as usize].insert(neighbor, new_dist);
                            Self::update_layer_best(&mut fwd_best, neighbor, next_hops, new_dist);
                            fwd_heap.push(Reverse((OrdF64(new_dist), next_hops, neighbor)));
                            Self::update_layered_meeting(
                                &bwd_best,
                                neighbor,
                                next_hops,
                                new_dist,
                                max_depth,
                                true,
                                &mut mu,
                                &mut best_meeting,
                            );
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            } else {
                let Reverse((OrdF64(d), hops, node)) = bwd_heap.pop().unwrap();
                if path_cost_lt(
                    bwd_dist[hops as usize]
                        .get(&node)
                        .copied()
                        .unwrap_or(f64::INFINITY),
                    d,
                ) {
                    continue;
                }
                if !bwd_settled[hops as usize].insert(node) {
                    continue;
                }
                Self::update_layered_meeting(
                    &fwd_best,
                    node,
                    hops,
                    d,
                    max_depth,
                    false,
                    &mut mu,
                    &mut best_meeting,
                );
                if hops == max_depth {
                    continue;
                }
                let next_hops = hops + 1;
                let mut expansion_error: Option<EngineError> = None;
                let _ = self.for_each_search_neighbor(
                    node,
                    bwd_direction,
                    edge_type_filter,
                    reference_time,
                    policy_cutoffs,
                    &mut tombstones,
                    &mut scratch,
                    &mut |neighbor, edge_id, posting_weight| {
                        let weight = match self.extract_weight_from_parts(
                            edge_id,
                            posting_weight,
                            weight_field,
                            &mut weight_cache,
                        ) {
                            Ok(weight) => weight,
                            Err(err) => {
                                expansion_error = Some(err);
                                return ControlFlow::Break(());
                            }
                        };
                        if weight < 0.0 || !weight.is_finite() {
                            expansion_error = Some(EngineError::InvalidOperation(
                                "invalid edge weight (negative, NaN, or infinite); Dijkstra requires finite non-negative weights".into(),
                            ));
                            return ControlFlow::Break(());
                        }
                        let new_dist = d + weight;
                        if let Some(max_total_cost) = max_cost {
                            if path_cost_lt(max_total_cost, new_dist) {
                                return ControlFlow::Continue(());
                            }
                        }
                        if path_cost_lt(mu, new_dist) {
                            return ControlFlow::Continue(());
                        }
                        let old = bwd_dist[next_hops as usize]
                            .get(&neighbor)
                            .copied()
                            .unwrap_or(f64::INFINITY);
                        if path_cost_lt(new_dist, old) {
                            bwd_dist[next_hops as usize].insert(neighbor, new_dist);
                            Self::update_layer_best(&mut bwd_best, neighbor, next_hops, new_dist);
                            bwd_heap.push(Reverse((OrdF64(new_dist), next_hops, neighbor)));
                            Self::update_layered_meeting(
                                &fwd_best,
                                neighbor,
                                next_hops,
                                new_dist,
                                max_depth,
                                false,
                                &mut mu,
                                &mut best_meeting,
                            );
                        }
                        ControlFlow::Continue(())
                    },
                )?;
                if let Some(err) = expansion_error {
                    return Err(err);
                }
            }
        }

        if mu.is_infinite() {
            return Ok(vec![]);
        }
        if let Some(max_total_cost) = max_cost {
            if path_cost_lt(max_total_cost, mu) {
                return Ok(vec![]);
            }
        }

        self.complete_weighted_layered_side_up_to_cost(
            &mut bwd_heap,
            &mut bwd_dist,
            &mut bwd_settled,
            &mut bwd_best,
            bwd_direction,
            edge_type_filter,
            weight_field,
            reference_time,
            max_depth,
            mu,
            policy_cutoffs,
            &mut tombstones,
            &mut scratch,
            &mut weight_cache,
        )?;
        self.complete_weighted_layered_side_up_to_cost(
            &mut fwd_heap,
            &mut fwd_dist,
            &mut fwd_settled,
            &mut fwd_best,
            direction,
            edge_type_filter,
            weight_field,
            reference_time,
            max_depth,
            mu,
            policy_cutoffs,
            &mut tombstones,
            &mut scratch,
            &mut weight_cache,
        )?;
        let paths_cap = if max_paths == 0 {
            usize::MAX
        } else {
            max_paths
        };
        let mut results: Vec<ShortestPath> = Vec::new();
        let mut path_nodes: Vec<u64> = vec![from];
        let mut path_edges: Vec<u64> = Vec::new();
        let mut visited: IdSet = IdSet::default();
        visited.insert(from);
        if max_paths == 0 {
            let successors = self.build_weighted_bounded_shortest_path_successors(
                &fwd_dist,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                max_depth,
                mu,
                &bwd_best,
                policy_cutoffs,
            )?;
            self.enumerate_weighted_bounded_shortest_paths_dfs(
                from,
                to,
                0,
                mu,
                &successors,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut visited,
                &mut results,
            )?;
        } else {
            let mut successors_cache: Vec<IdMap<Vec<(u64, u64)>>> =
                (0..=max_depth).map(|_| IdMap::default()).collect();
            self.enumerate_weighted_bounded_shortest_paths_dfs_lazy(
                from,
                to,
                0,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                max_depth,
                mu,
                &fwd_dist,
                &bwd_best,
                policy_cutoffs,
                &mut tombstones,
                &mut scratch,
                &mut weight_cache,
                &mut successors_cache,
                paths_cap,
                &mut path_nodes,
                &mut path_edges,
                &mut visited,
                &mut results,
            )?;
        }
        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_bfs_shortest_paths_dfs(
        &self,
        node: u64,
        to: u64,
        depth_so_far: u32,
        best_hops: u32,
        successors: &IdMap<Vec<(u64, u64)>>,
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        if max_paths > 0 && results.len() >= max_paths {
            return Ok(());
        }
        if node == to {
            if depth_so_far == best_hops {
                results.push(ShortestPath {
                    nodes: path_nodes.clone(),
                    edges: path_edges.clone(),
                    total_cost: best_hops as f64,
                });
            }
            return Ok(());
        }
        if depth_so_far >= best_hops {
            return Ok(());
        }

        let Some(next_steps) = successors.get(&node) else {
            return Ok(());
        };

        for &(neighbor, edge_id) in next_steps {
            path_nodes.push(neighbor);
            path_edges.push(edge_id);
            self.enumerate_bfs_shortest_paths_dfs(
                neighbor,
                to,
                depth_so_far + 1,
                best_hops,
                successors,
                max_paths,
                path_nodes,
                path_edges,
                results,
            )?;
            path_edges.pop();
            path_nodes.pop();
            if max_paths > 0 && results.len() >= max_paths {
                break;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_bfs_shortest_paths_dfs_lazy(
        &self,
        node: u64,
        to: u64,
        depth_so_far: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        reference_time: i64,
        best_hops: u32,
        bwd_depth: &IdMap<u32>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        successors_cache: &mut [IdMap<Vec<(u64, u64)>>],
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        if max_paths > 0 && results.len() >= max_paths {
            return Ok(());
        }
        if node == to {
            if depth_so_far == best_hops {
                results.push(ShortestPath {
                    nodes: path_nodes.clone(),
                    edges: path_edges.clone(),
                    total_cost: best_hops as f64,
                });
            }
            return Ok(());
        }
        if depth_so_far >= best_hops {
            return Ok(());
        }

        let next_steps = self.cached_bfs_shortest_path_successors(
            successors_cache,
            node,
            depth_so_far,
            direction,
            edge_type_filter,
            reference_time,
            best_hops,
            bwd_depth,
            policy_cutoffs,
            tombstones,
            scratch,
        )?;

        for (neighbor, edge_id) in next_steps {
            path_nodes.push(neighbor);
            path_edges.push(edge_id);
            self.enumerate_bfs_shortest_paths_dfs_lazy(
                neighbor,
                to,
                depth_so_far + 1,
                direction,
                edge_type_filter,
                reference_time,
                best_hops,
                bwd_depth,
                policy_cutoffs,
                tombstones,
                scratch,
                successors_cache,
                max_paths,
                path_nodes,
                path_edges,
                results,
            )?;
            path_edges.pop();
            path_nodes.pop();
            if max_paths > 0 && results.len() >= max_paths {
                break;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_acyclic_shortest_paths_dfs<S, NextFn, CanExpandFn, AdvanceFn>(
        &self,
        node: u64,
        to: u64,
        state: S,
        best_cost: f64,
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        visited: &mut IdSet,
        results: &mut Vec<ShortestPath>,
        can_expand: &CanExpandFn,
        advance: &AdvanceFn,
        next_steps: &mut NextFn,
    ) -> Result<(), EngineError>
    where
        S: Copy,
        NextFn: FnMut(u64, S) -> Result<Vec<(u64, u64)>, EngineError>,
        CanExpandFn: Fn(S) -> bool,
        AdvanceFn: Fn(S) -> S,
    {
        if max_paths > 0 && results.len() >= max_paths {
            return Ok(());
        }
        if node == to {
            results.push(ShortestPath {
                nodes: path_nodes.clone(),
                edges: path_edges.clone(),
                total_cost: best_cost,
            });
            return Ok(());
        }
        if !can_expand(state) {
            return Ok(());
        }

        let steps = next_steps(node, state)?;
        let next_state = advance(state);

        for (neighbor, edge_id) in steps {
            if visited.contains(&neighbor) {
                continue;
            }
            visited.insert(neighbor);
            path_nodes.push(neighbor);
            path_edges.push(edge_id);
            self.enumerate_acyclic_shortest_paths_dfs(
                neighbor, to, next_state, best_cost, max_paths, path_nodes, path_edges, visited,
                results, can_expand, advance, next_steps,
            )?;
            path_edges.pop();
            path_nodes.pop();
            visited.remove(&neighbor);
            if max_paths > 0 && results.len() >= max_paths {
                break;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_weighted_shortest_paths_dfs(
        &self,
        node: u64,
        to: u64,
        best_cost: f64,
        successors: &IdMap<Vec<(u64, u64)>>,
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        visited: &mut IdSet,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        let can_expand = |_state: ()| true;
        let advance = |_state: ()| ();
        let mut next_steps = |current_node: u64, _state: ()| {
            Ok(successors.get(&current_node).cloned().unwrap_or_default())
        };
        self.enumerate_acyclic_shortest_paths_dfs(
            node,
            to,
            (),
            best_cost,
            max_paths,
            path_nodes,
            path_edges,
            visited,
            results,
            &can_expand,
            &advance,
            &mut next_steps,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_weighted_shortest_paths_dfs_lazy(
        &self,
        node: u64,
        to: u64,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        best_cost: f64,
        fwd_dist: &IdMap<f64>,
        bwd_dist: &IdMap<f64>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
        successors_cache: &mut IdMap<Vec<(u64, u64)>>,
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        visited: &mut IdSet,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        let can_expand = |_state: ()| true;
        let advance = |_state: ()| ();
        let mut next_steps = |current_node: u64, _state: ()| {
            self.cached_weighted_shortest_path_successors(
                successors_cache,
                current_node,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                best_cost,
                fwd_dist,
                bwd_dist,
                policy_cutoffs,
                tombstones,
                scratch,
                weight_cache,
            )
        };
        self.enumerate_acyclic_shortest_paths_dfs(
            node,
            to,
            (),
            best_cost,
            max_paths,
            path_nodes,
            path_edges,
            visited,
            results,
            &can_expand,
            &advance,
            &mut next_steps,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_weighted_bounded_shortest_paths_dfs(
        &self,
        node: u64,
        to: u64,
        hops_so_far: u32,
        best_cost: f64,
        successors: &[IdMap<Vec<(u64, u64)>>],
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        visited: &mut IdSet,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        let max_hops = successors.len().saturating_sub(1) as u32;
        let can_expand = |hops: u32| hops < max_hops;
        let advance = |hops: u32| hops + 1;
        let mut next_steps = |current_node: u64, hops: u32| {
            Ok(successors[hops as usize]
                .get(&current_node)
                .cloned()
                .unwrap_or_default())
        };
        self.enumerate_acyclic_shortest_paths_dfs(
            node,
            to,
            hops_so_far,
            best_cost,
            max_paths,
            path_nodes,
            path_edges,
            visited,
            results,
            &can_expand,
            &advance,
            &mut next_steps,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enumerate_weighted_bounded_shortest_paths_dfs_lazy(
        &self,
        node: u64,
        to: u64,
        hops_so_far: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        weight_field: &str,
        reference_time: i64,
        max_depth: u32,
        best_cost: f64,
        fwd_dist: &[IdMap<f64>],
        bwd_best: &IdMap<Vec<LayerBestEntry>>,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        tombstones: &mut TraversalTombstoneView<'_>,
        scratch: &mut SearchNeighborScratch,
        weight_cache: &mut IdMap<f64>,
        successors_cache: &mut [IdMap<Vec<(u64, u64)>>],
        max_paths: usize,
        path_nodes: &mut Vec<u64>,
        path_edges: &mut Vec<u64>,
        visited: &mut IdSet,
        results: &mut Vec<ShortestPath>,
    ) -> Result<(), EngineError> {
        let can_expand = |hops: u32| hops < max_depth;
        let advance = |hops: u32| hops + 1;
        let mut next_steps = |current_node: u64, hops: u32| {
            self.cached_weighted_bounded_shortest_path_successors(
                successors_cache,
                current_node,
                hops,
                direction,
                edge_type_filter,
                weight_field,
                reference_time,
                max_depth,
                best_cost,
                fwd_dist,
                bwd_best,
                policy_cutoffs,
                tombstones,
                scratch,
                weight_cache,
            )
        };
        self.enumerate_acyclic_shortest_paths_dfs(
            node,
            to,
            hops_so_far,
            best_cost,
            max_paths,
            path_nodes,
            path_edges,
            visited,
            results,
            &can_expand,
            &advance,
            &mut next_steps,
        )
    }

    /// Raw neighbor query with no prune policy filtering. Used internally by
    /// cascade deletes (delete_node, graph_patch, prune) that must see ALL
    /// incident edges including those to policy-excluded nodes.
    ///
    /// `tombstones`: optional pre-computed (deleted_nodes, deleted_edges) sets.
    /// Pass `None` for single-call sites (collected internally). Pass
    /// `Some(...)` when calling in a loop to avoid redundant collection.
    #[allow(clippy::too_many_arguments)]
    fn neighbors_raw(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
        tombstones: Option<(&HashSet<u64>, &HashSet<u64>)>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Use pre-computed tombstones or collect them now
        let owned_tombstones;
        let (deleted_nodes, deleted_edges) = match tombstones {
            Some((dn, de)) => (dn, de),
            None => {
                owned_tombstones = self.collect_tombstones();
                (&owned_tombstones.0, &owned_tombstones.1)
            }
        };

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Start with memtable results (fetch without limit to allow for temporal filtering)
        let mut results = self.memtable.neighbors(node_id, direction, type_filter, 0);
        let mut seen_edges: HashSet<u64> = results.iter().map(|e| e.edge_id).collect();

        // Add segment results
        for seg in &self.segments {
            let seg_results = seg.neighbors(node_id, direction, type_filter, 0)?;
            for entry in seg_results {
                if !seen_edges.insert(entry.edge_id) {
                    continue;
                }
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                if deleted_nodes.contains(&entry.node_id) {
                    continue;
                }
                results.push(entry);
            }
        }

        // Temporal filtering + decay scoring using adjacency-embedded
        // valid_from/valid_to. No per-edge record lookup required.
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;

        results.retain_mut(|entry| {
            if !is_edge_valid_at(entry.valid_from, entry.valid_to, reference_time) {
                return false;
            }

            // Decay-adjusted scoring: age is measured from valid_from in the
            // validity timeline. For non-temporal edges, valid_from defaults
            // to created_at, giving natural "time since creation" decay.
            if apply_decay {
                let age_hours = (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                entry.weight *= (-lambda * age_hours).exp();
            }

            true
        });

        // Sort by decay-adjusted weight descending if decay was applied
        if apply_decay {
            results.sort_by(|a, b| {
                b.weight
                    .partial_cmp(&a.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Apply limit after all filtering and sorting
        if limit > 0 && results.len() > limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Query neighbors, merging results from memtable + segments.
    /// Memtable results come first, then segments newest-to-oldest.
    /// Deduplicates by edge_id (first-seen wins).
    ///
    /// Nodes matching any registered prune policy are excluded from results.
    /// When policies are active, fetches all results first (ignoring limit),
    /// filters, then applies limit. This prevents short results when excluded
    /// nodes consume limit slots.
    ///
    /// `at_epoch`: if `Some(t)`, filter to edges valid at time `t`. If `None`, use current time.
    /// `decay_lambda`: if `Some(λ)`, compute `score = weight * e^(-λ * age_hours)` where
    ///   `age_hours = (reference_time - valid_from) / 3_600_000` and sort descending by score.
    pub fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        // Early-out: no policies → delegate directly to raw (zero overhead)
        if self.manifest.prune_policies.is_empty() {
            return self.neighbors_raw(
                node_id,
                direction,
                type_filter,
                limit,
                at_epoch,
                decay_lambda,
                None,
            );
        }

        // Fetch ALL results (limit=0) so policy filtering doesn't cause short results
        let mut results = self.neighbors_raw(
            node_id,
            direction,
            type_filter,
            0,
            at_epoch,
            decay_lambda,
            None,
        )?;

        // Batch-fetch neighbor nodes and filter by policy (single merge-walk
        // per segment instead of N individual binary searches).
        // Dedupe endpoint IDs — many edges can point to the same neighbor.
        let neighbor_ids: Vec<u64> = {
            let mut ids: Vec<u64> = results.iter().map(|e| e.node_id).collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
        if !excluded.is_empty() {
            results.retain(|entry| !excluded.contains(&entry.node_id));
        }

        // Apply limit after policy filtering
        if limit > 0 && results.len() > limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Batch neighbor query for multiple node IDs. Single O(N+M) cursor walk
    /// per segment instead of O(M log N) individual binary searches.
    ///
    /// `node_ids` need not be sorted (sorted internally). Returns a
    /// [`NodeIdMap<Vec<NeighborEntry>>`] mapping each queried node_id to its
    /// neighbor entries. `NodeIdMap` is a `HashMap` with identity hashing
    /// optimized for engine-generated numeric IDs.
    ///
    /// Applies prune policy filtering when policies are registered.
    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<NodeIdMap<Vec<NeighborEntry>>, EngineError> {
        if self.manifest.prune_policies.is_empty() {
            return self.neighbors_batch_raw(
                node_ids,
                direction,
                type_filter,
                at_epoch,
                decay_lambda,
            );
        }

        let mut results =
            self.neighbors_batch_raw(node_ids, direction, type_filter, at_epoch, decay_lambda)?;

        // Collect unique neighbor node IDs for batch policy check (dedup avoids
        // redundant merge-walks when many queried nodes share neighbors).
        let all_neighbor_ids: Vec<u64> = {
            let mut ids: Vec<u64> = results
                .values()
                .flat_map(|entries| entries.iter().map(|e| e.node_id))
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let excluded = self.policy_excluded_node_ids(&all_neighbor_ids)?;
        if !excluded.is_empty() {
            for entries in results.values_mut() {
                entries.retain(|entry| !excluded.contains(&entry.node_id));
            }
            // Remove empty entries
            results.retain(|_, v| !v.is_empty());
        }

        Ok(results)
    }

    /// Raw batch neighbor query with no prune policy filtering.
    /// Collects tombstones once, then does a single cursor walk per segment
    /// for all node IDs.
    fn neighbors_batch_raw(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<NodeIdMap<Vec<NeighborEntry>>, EngineError> {
        if node_ids.is_empty() {
            return Ok(NodeIdMap::default());
        }

        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Sort + dedup node IDs for segment cursor walks
        let sorted_ids: Vec<u64> = {
            let mut ids = node_ids.to_vec();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        // Collect all tombstoned node and edge IDs ONCE
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Start with memtable results.
        let mut results = self
            .memtable
            .neighbors_batch(&sorted_ids, direction, type_filter);

        // Track seen edge IDs per node for dedup (memtable/newer segment wins)
        let mut seen_edges: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (&nid, entries) in &results {
            let seen = seen_edges.entry(nid).or_default();
            for e in entries {
                seen.insert(e.edge_id);
            }
        }

        // Merge segment results (newest-to-oldest, one cursor walk per segment)
        for seg in &self.segments {
            let seg_results = seg.neighbors_batch(&sorted_ids, direction, type_filter)?;
            for (nid, seg_entries) in seg_results {
                let seen = seen_edges.entry(nid).or_default();
                let node_entries = results.entry(nid).or_default();
                for entry in seg_entries {
                    if !seen.insert(entry.edge_id) {
                        continue;
                    }
                    if deleted_edges.contains(&entry.edge_id) {
                        continue;
                    }
                    if deleted_nodes.contains(&entry.node_id) {
                        continue;
                    }
                    node_entries.push(entry);
                }
            }
        }

        // Apply temporal filtering + decay scoring
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;

        for entries in results.values_mut() {
            entries.retain_mut(|entry| {
                if !is_edge_valid_at(entry.valid_from, entry.valid_to, reference_time) {
                    return false;
                }
                if apply_decay {
                    let age_hours = (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                    entry.weight *= (-lambda * age_hours).exp();
                }
                true
            });
        }

        if apply_decay {
            for entries in results.values_mut() {
                entries.sort_by(|a, b| {
                    b.weight
                        .partial_cmp(&a.weight)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        // Remove empty entries
        results.retain(|_, v| !v.is_empty());

        Ok(results)
    }

    /// Paginated version of `neighbors`. Returns a page of neighbor entries
    /// sorted by edge_id with cursor-based pagination.
    ///
    /// Three paths based on query complexity:
    /// - **Fast path** (no decay, no policies): temporal validity in skip_fn +
    ///   cursor binary-seek + early termination. O(K log N) seek + O(limit) merge.
    /// - **Policy path** (no decay, has policies): temporal in skip_fn + cursor
    ///   binary-seek (no limit), then batch policy filter + take limit. Skips
    ///   items before cursor but still processes all post-cursor items for policy
    ///   correctness.
    /// - **Decay path**: collects all temporally-valid items (no cursor; results
    ///   are re-sorted by score), applies decay scoring, sorts descending, then
    ///   policy filter + truncate to limit.
    pub fn neighbors_paged(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        page: &PageRequest,
        at_epoch: Option<i64>,
        decay_lambda: Option<f32>,
    ) -> Result<PageResult<NeighborEntry>, EngineError> {
        if let Some(l) = decay_lambda {
            if l < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Collect all tombstoned node and edge IDs across memtable + segments
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);
        let lambda = decay_lambda.unwrap_or(0.0);
        let apply_decay = lambda > 0.0;
        let has_policies = !self.manifest.prune_policies.is_empty();

        // Collect sources: memtable (unsorted) + segments (sorted by edge_id)
        let memtable_entries = self.memtable.neighbors(node_id, direction, type_filter, 0);
        let mut segment_entries: Vec<Vec<NeighborEntry>> = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            segment_entries.push(seg.neighbors(node_id, direction, type_filter, 0)?);
        }

        if apply_decay {
            // Decay path: collect all temporally-valid items, score, rank by score.
            // Cursor doesn't apply (score-sorted, not edge_id-sorted).
            let all_page = PageRequest {
                limit: None,
                after: None,
            };
            let mut all = merge_sorted_paged(
                memtable_entries,
                segment_entries,
                |e| e.edge_id,
                |e| {
                    deleted_edges.contains(&e.edge_id)
                        || deleted_nodes.contains(&e.node_id)
                        || !is_edge_valid_at(e.valid_from, e.valid_to, reference_time)
                },
                &all_page,
            );

            // Apply decay scoring
            for entry in &mut all.items {
                let age_hours = (reference_time - entry.valid_from).max(0) as f32 / 3_600_000.0;
                entry.weight *= (-lambda * age_hours).exp();
            }

            let limit = page.limit.unwrap_or(0);
            if limit > 0 && all.items.len() > limit && !has_policies {
                // O(n) partial select for top-K instead of O(n log n) full sort.
                all.items.select_nth_unstable_by(limit - 1, |a, b| {
                    b.weight
                        .partial_cmp(&a.weight)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                all.items.truncate(limit);
                all.items.sort_unstable_by(|a, b| {
                    b.weight
                        .partial_cmp(&a.weight)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                // With policies, ranking must consider all visible candidates.
                all.items.sort_unstable_by(|a, b| {
                    b.weight
                        .partial_cmp(&a.weight)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

                if has_policies {
                    let neighbor_ids: Vec<u64> = {
                        let mut ids: Vec<u64> =
                            all.items.iter().map(|e| e.node_id).collect();
                        ids.sort_unstable();
                        ids.dedup();
                        ids
                    };
                    let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
                    if !excluded.is_empty() {
                        all.items.retain(|e| !excluded.contains(&e.node_id));
                    }
                }
                if limit > 0 {
                    all.items.truncate(limit);
                }
            }
            Ok(PageResult {
                items: all.items,
                next_cursor: None,
            })
        } else if !has_policies {
            // Fast path: temporal validity in skip_fn + cursor + early termination.
            // Temporal filter always runs (reference_time = at_epoch or now),
            // matching neighbors() which always filters against reference_time.
            Ok(merge_sorted_paged(
                memtable_entries,
                segment_entries,
                |e| e.edge_id,
                |e| {
                    deleted_edges.contains(&e.edge_id)
                        || deleted_nodes.contains(&e.node_id)
                        || !is_edge_valid_at(e.valid_from, e.valid_to, reference_time)
                },
                page,
            ))
        } else {
            let limit = page.limit.unwrap_or(0);
            if limit == 0 {
                let all_page = PageRequest {
                    limit: None,
                    after: page.after,
                };
                let all = merge_sorted_paged(
                    memtable_entries,
                    segment_entries,
                    |e| e.edge_id,
                    |e| {
                        deleted_edges.contains(&e.edge_id)
                            || deleted_nodes.contains(&e.node_id)
                            || !is_edge_valid_at(e.valid_from, e.valid_to, reference_time)
                    },
                    &all_page,
                );

                let neighbor_ids: Vec<u64> = {
                    let mut ids: Vec<u64> = all.items.iter().map(|e| e.node_id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                };
                let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
                let mut items = all.items;
                if !excluded.is_empty() {
                    items.retain(|e| !excluded.contains(&e.node_id));
                }

                Ok(PageResult {
                    items,
                    next_cursor: None,
                })
            } else {
                let chunk_limit = limit.saturating_mul(4).max(limit);
                let mut collected = Vec::with_capacity(limit);
                let mut cursor = page.after;

                loop {
                    let chunk_page = PageRequest {
                        limit: Some(chunk_limit),
                        after: cursor,
                    };
                    let chunk = merge_sorted_paged(
                        memtable_entries.clone(),
                        segment_entries.clone(),
                        |e| e.edge_id,
                        |e| {
                            deleted_edges.contains(&e.edge_id)
                                || deleted_nodes.contains(&e.node_id)
                                || !is_edge_valid_at(e.valid_from, e.valid_to, reference_time)
                        },
                        &chunk_page,
                    );
                    if chunk.items.is_empty() {
                        return Ok(PageResult {
                            items: collected,
                            next_cursor: None,
                        });
                    }

                    let neighbor_ids: Vec<u64> = {
                        let mut ids: Vec<u64> = chunk.items.iter().map(|e| e.node_id).collect();
                        ids.sort_unstable();
                        ids.dedup();
                        ids
                    };
                    let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;

                    for entry in chunk.items {
                        let edge_id = entry.edge_id;
                        if !excluded.contains(&entry.node_id) {
                            collected.push(entry);
                            if collected.len() >= limit {
                                return Ok(PageResult {
                                    items: collected,
                                    next_cursor: Some(edge_id),
                                });
                            }
                        }
                        cursor = Some(edge_id);
                    }

                    if chunk.next_cursor.is_none() {
                        return Ok(PageResult {
                            items: collected,
                            next_cursor: None,
                        });
                    }
                }
            }
        }
    }

    /// Return the top-k neighbors by the given scoring mode.
    ///
    /// Uses a bounded min-heap of size k during merge for efficiency,
    /// avoids sorting the full neighbor set.
    pub fn top_k_neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        k: usize,
        scoring: ScoringMode,
        at_epoch: Option<i64>,
    ) -> Result<Vec<NeighborEntry>, EngineError> {
        if k == 0 {
            return Ok(Vec::new());
        }

        if let ScoringMode::DecayAdjusted { lambda } = &scoring {
            if *lambda < 0.0 {
                return Err(EngineError::InvalidOperation(
                    "decay_lambda must be non-negative".to_string(),
                ));
            }
        }

        // Collect all tombstoned node and edge IDs
        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let now = now_millis();
        let reference_time = at_epoch.unwrap_or(now);

        // Gather all neighbors from memtable + segments with dedup + deletion filter
        let mut all_entries = self.memtable.neighbors(node_id, direction, type_filter, 0);
        // Filter memtable results against deleted sets (M2 fix)
        all_entries
            .retain(|e| !deleted_edges.contains(&e.edge_id) && !deleted_nodes.contains(&e.node_id));
        let mut seen_edges: HashSet<u64> = all_entries.iter().map(|e| e.edge_id).collect();

        for seg in &self.segments {
            let seg_results = seg.neighbors(node_id, direction, type_filter, 0)?;
            for entry in seg_results {
                if !seen_edges.insert(entry.edge_id) {
                    continue;
                }
                if deleted_edges.contains(&entry.edge_id) {
                    continue;
                }
                if deleted_nodes.contains(&entry.node_id) {
                    continue;
                }
                all_entries.push(entry);
            }
        }

        // Policy filtering: batch-fetch neighbor nodes and exclude matches.
        // Single merge-walk per segment instead of N individual binary searches.
        {
            let neighbor_ids: Vec<u64> = {
                let mut ids: Vec<u64> = all_entries.iter().map(|e| e.node_id).collect();
                ids.sort_unstable();
                ids.dedup();
                ids
            };
            let excluded = self.policy_excluded_node_ids(&neighbor_ids)?;
            if !excluded.is_empty() {
                all_entries.retain(|entry| !excluded.contains(&entry.node_id));
            }
        }

        // Temporal filter + score computation.
        // Use f64 bits for the min-heap key to preserve i64 timestamp precision
        // for Recency scoring (f32 can't represent epoch-millis without loss).
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
        let mut scored: Vec<NeighborEntry> = Vec::new();

        for mut entry in all_entries {
            if !is_edge_valid_at(entry.valid_from, entry.valid_to, reference_time) {
                continue;
            }

            let score: f64 = match &scoring {
                ScoringMode::Weight => entry.weight as f64,
                ScoringMode::Recency => entry.valid_from as f64,
                ScoringMode::DecayAdjusted { lambda } => {
                    let age_hours = (reference_time - entry.valid_from).max(0) as f64 / 3_600_000.0;
                    (entry.weight as f64) * (-(*lambda as f64) * age_hours).exp()
                }
            };

            // Update weight to reflect the computed score (so callers see the score used)
            if matches!(scoring, ScoringMode::DecayAdjusted { .. }) {
                entry.weight = score as f32;
            }

            let idx = scored.len();
            // f64::to_bits provides total ordering for non-negative IEEE 754 values
            let score_bits = score.to_bits();

            if heap.len() < k {
                heap.push(Reverse((score_bits, idx)));
                scored.push(entry);
            } else if let Some(&Reverse((min_bits, _))) = heap.peek() {
                if score_bits > min_bits {
                    heap.pop();
                    heap.push(Reverse((score_bits, idx)));
                    scored.push(entry);
                }
            }
        }

        // Extract the top-k indices from the heap
        let mut top_indices: Vec<(u64, usize)> = heap.into_iter().map(|Reverse(x)| x).collect();
        // Sort descending by score
        top_indices.sort_by(|a, b| b.0.cmp(&a.0));

        let results: Vec<NeighborEntry> = top_indices
            .into_iter()
            .map(|(_, idx)| scored[idx].clone())
            .collect();

        Ok(results)
    }

    /// Extract a subgraph of all nodes and edges reachable within `max_depth`
    /// hops of `start_node_id`.
    ///
    /// Uses BFS with cycle detection. Edges to already-visited nodes (cross-edges
    /// and back-edges) are included in the result. The traversal respects
    /// direction and edge type filters.
    ///
    /// - `max_depth`: maximum number of hops. 0 returns just the start node.
    /// - `direction`: which edge direction to follow during traversal.
    /// - `edge_type_filter`: only traverse edges of these types. `None` = all.
    /// - `at_epoch`: temporal filter. Only include edges valid at this time.
    ///
    /// Returns an empty `Subgraph` if the start node does not exist.
    pub fn extract_subgraph(
        &self,
        start_node_id: u64,
        max_depth: u32,
        direction: Direction,
        edge_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<Subgraph, EngineError> {
        // Check start node exists
        let start_node = match self.get_node(start_node_id)? {
            Some(n) => n,
            None => {
                return Ok(Subgraph {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                })
            }
        };

        let mut visited_nodes: HashSet<u64> = HashSet::new();
        let mut collected_edge_ids: HashSet<u64> = HashSet::new();
        let mut node_records: Vec<NodeRecord> = Vec::new();
        let mut edge_records: Vec<EdgeRecord> = Vec::new();

        visited_nodes.insert(start_node_id);
        node_records.push(start_node);

        if max_depth == 0 {
            return Ok(Subgraph {
                nodes: node_records,
                edges: edge_records,
            });
        }

        // BFS level by level, batch-fetch neighbors for entire frontier
        let mut frontier: Vec<u64> = vec![start_node_id];

        for _depth in 0..max_depth {
            let mut new_edge_ids: Vec<u64> = Vec::new();
            let mut new_node_ids: Vec<u64> = Vec::new();

            // Batch adjacency: one cursor walk per segment for the whole frontier
            let all_neighbors =
                self.neighbors_batch(&frontier, direction, edge_type_filter, at_epoch, None)?;

            for &current_node in &frontier {
                let empty = Vec::new();
                let neighbors = all_neighbors.get(&current_node).unwrap_or(&empty);

                for entry in neighbors {
                    if collected_edge_ids.insert(entry.edge_id) {
                        new_edge_ids.push(entry.edge_id);
                    }
                    if visited_nodes.insert(entry.node_id) {
                        new_node_ids.push(entry.node_id);
                    }
                }
            }

            // Batch-fetch all new edges and nodes for this BFS level
            let fetched_edges = self.get_edges(&new_edge_ids)?;
            for edge in fetched_edges.into_iter().flatten() {
                edge_records.push(edge);
            }

            let fetched_nodes = self.get_nodes(&new_node_ids)?;
            let mut next_frontier: Vec<u64> = Vec::new();
            for (i, node_opt) in fetched_nodes.into_iter().enumerate() {
                if let Some(node) = node_opt {
                    node_records.push(node);
                    next_frontier.push(new_node_ids[i]);
                }
            }

            frontier = next_frontier;
            if frontier.is_empty() {
                break;
            }
        }

        Ok(Subgraph {
            nodes: node_records,
            edges: edge_records,
        })
    }

    // --- Connected Components (WCC) ---

    /// Weakly connected components over the live visible graph.
    ///
    /// WCC treats all edges as undirected: edge direction is ignored for
    /// component membership. Returns a [`NodeIdMap<u64>`] mapping every
    /// visible node ID to its deterministic component ID, where the
    /// component ID is the minimum node ID in that component.
    /// `NodeIdMap` is a `HashMap` with identity hashing optimized for
    /// engine-generated numeric IDs.
    ///
    /// - `edge_type_filter`: only consider edges of these types. `None` = all.
    /// - `node_type_filter`: only include nodes of these types. `None` = all.
    /// - `at_epoch`: reference time for temporal edge visibility. `None` → now.
    ///
    /// Isolated nodes (no visible edges after filtering) become singleton
    /// components whose component ID equals the node's own ID.
    ///
    /// # Algorithm
    ///
    /// Union-Find with path compression and union by rank over a global
    /// outgoing adjacency scan. Each edge is seen exactly once (outgoing
    /// direction), and `union(from, to)` captures undirected connectivity.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use overgraph::DatabaseEngine;
    /// # use std::path::Path;
    /// # let db = DatabaseEngine::open(Path::new("tmp"), &Default::default()).unwrap();
    /// let components = db.connected_components(None, None, None).unwrap();
    /// // components: { node_id → component_id (min node in component) }
    /// ```
    pub fn connected_components(
        &self,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<NodeIdMap<u64>, EngineError> {
        // 1. Collect all visible node IDs (policy-filtered, type-filtered).
        let node_types: Vec<u32> = {
            let mut types: HashSet<u32> =
                self.memtable.type_node_index().keys().copied().collect();
            for seg in &self.segments {
                for tid in seg.node_type_ids()? {
                    types.insert(tid);
                }
            }
            if let Some(filter) = node_type_filter {
                let allowed: HashSet<u32> = filter.iter().copied().collect();
                types.retain(|t| allowed.contains(t));
            }
            types.into_iter().collect()
        };

        // Collect all visible node IDs per type, then build the set with
        // known capacity to avoid repeated re-hashing.
        let mut per_type_ids: Vec<Vec<u64>> = Vec::with_capacity(node_types.len());
        let mut total_count: usize = 0;
        for &tid in &node_types {
            let ids = self.nodes_by_type(tid)?;
            total_count += ids.len();
            per_type_ids.push(ids);
        }
        if total_count == 0 {
            return Ok(NodeIdMap::default());
        }

        let mut node_set: IdSet =
            IdSet::with_capacity_and_hasher(total_count, IdBuildHasher::default());
        for ids in &per_type_ids {
            for &id in ids {
                node_set.insert(id);
            }
        }
        drop(per_type_ids);

        let mut node_ids: Vec<u64> = node_set.iter().copied().collect();
        node_ids.sort_unstable();

        // 2. Initialize union-find with one set per visible node.
        let mut uf = UnionFind::with_capacity(node_ids.len());
        for &id in &node_ids {
            uf.make_set(id);
        }

        // 3. Global outgoing adjacency scan — streaming union.
        //    Each directed edge appears exactly once; union(from, to)
        //    captures undirected connectivity. Unlike neighbors_batch(),
        //    this never materializes Vec<NeighborEntry> — the callback
        //    unions endpoints inline during the cursor walk.
        let reference_time = at_epoch.unwrap_or_else(now_millis);

        let mut deleted_nodes: HashSet<u64> =
            self.memtable.deleted_nodes().keys().copied().collect();
        let mut deleted_edges: HashSet<u64> =
            self.memtable.deleted_edges().keys().copied().collect();
        for seg in &self.segments {
            deleted_nodes.extend(seg.deleted_node_ids());
            deleted_edges.extend(seg.deleted_edge_ids());
        }

        let mut seen_edges: HashSet<(u64, u64)> = HashSet::new();

        // Memtable pass (per-node callback).
        for &nid in &node_ids {
            let _ = self.memtable.for_each_adj_entry(
                nid,
                Direction::Outgoing,
                edge_type_filter,
                &mut |edge_id, neighbor_id, _weight, valid_from, valid_to| {
                    seen_edges.insert((nid, edge_id));
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if node_set.contains(&neighbor_id) {
                        uf.union(nid, neighbor_id);
                    }
                    ControlFlow::Continue(())
                },
            );
        }

        // Segment passes (batch callback, one cursor walk per segment).
        for seg in &self.segments {
            let _ = seg.for_each_adj_posting_batch(
                &node_ids,
                Direction::Outgoing,
                edge_type_filter,
                &mut |queried_nid, edge_id, neighbor_id, _weight, valid_from, valid_to| {
                    if !seen_edges.insert((queried_nid, edge_id)) {
                        return ControlFlow::Continue(());
                    }
                    if !is_edge_valid_at(valid_from, valid_to, reference_time) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_edges.contains(&edge_id) {
                        return ControlFlow::Continue(());
                    }
                    if deleted_nodes.contains(&neighbor_id) {
                        return ControlFlow::Continue(());
                    }
                    if node_set.contains(&neighbor_id) {
                        uf.union(queried_nid, neighbor_id);
                    }
                    ControlFlow::Continue(())
                },
            )?;
        }

        // 5. Flatten: component_id = min(node_id) in each component.
        let mut root_to_min: IdMap<u64> =
            IdMap::with_capacity_and_hasher(node_ids.len() / 4 + 1, IdBuildHasher::default());
        for &id in &node_ids {
            let root = uf.find(id);
            root_to_min
                .entry(root)
                .and_modify(|min| {
                    if id < *min {
                        *min = id;
                    }
                })
                .or_insert(id);
        }

        // 6. Build result: node_id → component_id.
        //    Steps 5 and 6 are fused into a single pass — each node's root is
        //    already path-compressed from step 5, so the second find() is O(1).
        let mut result =
            NodeIdMap::with_capacity_and_hasher(node_ids.len(), IdBuildHasher::default());
        for &id in &node_ids {
            let root = uf.find(id);
            result.insert(id, root_to_min[&root]);
        }

        Ok(result)
    }

    /// Returns the node set of the weakly connected component containing
    /// `node_id`, sorted by node ID ascending.
    ///
    /// Uses targeted BFS from the given node, treating all edges as
    /// undirected (`Direction::Both`). Avoids full-graph computation.
    ///
    /// Returns an empty `Vec` if the node doesn't exist, is deleted, or is
    /// hidden by prune policy. Returns an empty `Vec` if the node exists but
    /// is excluded by `node_type_filter`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use overgraph::DatabaseEngine;
    /// # use std::path::Path;
    /// # let db = DatabaseEngine::open(Path::new("tmp"), &Default::default()).unwrap();
    /// let members = db.component_of(42, None, None, None).unwrap();
    /// // members: sorted Vec of node IDs in the same component as node 42
    /// ```
    pub fn component_of(
        &self,
        node_id: u64,
        edge_type_filter: Option<&[u32]>,
        node_type_filter: Option<&[u32]>,
        at_epoch: Option<i64>,
    ) -> Result<Vec<u64>, EngineError> {
        // Check if the start node exists and is visible (tombstones + policies).
        let start_node = match self.get_node(node_id)? {
            Some(n) => n,
            None => return Ok(Vec::new()),
        };

        // If node_type_filter is set, the start node must pass it.
        if let Some(filter) = node_type_filter {
            if !filter.contains(&start_node.type_id) {
                return Ok(Vec::new());
            }
        }

        let type_filter_set: Option<HashSet<u32>> =
            node_type_filter.map(|f| f.iter().copied().collect());

        let mut processed: IdSet = IdSet::with_hasher(IdBuildHasher::default());
        processed.insert(node_id);

        let mut component: Vec<u64> = vec![node_id];
        let mut frontier: Vec<u64> = vec![node_id];
        let mut candidate_ids: Vec<u64> = Vec::new();
        let mut next_frontier: Vec<u64> = Vec::new();

        while !frontier.is_empty() {
            // Batch-fetch neighbors for all frontier nodes (undirected).
            let all_neighbors = self.neighbors_batch(
                &frontier,
                Direction::Both,
                edge_type_filter,
                at_epoch,
                None,
            )?;

            // Collect newly discovered neighbor IDs.
            candidate_ids.clear();
            for entries in all_neighbors.values() {
                for entry in entries {
                    if processed.insert(entry.node_id) {
                        candidate_ids.push(entry.node_id);
                    }
                }
            }

            if candidate_ids.is_empty() {
                break;
            }

            // If node_type_filter is set, check types of discovered nodes.
            // Only nodes passing the filter join the component and frontier.
            next_frontier.clear();
            if let Some(ref type_set) = type_filter_set {
                candidate_ids.sort_unstable();
                let nodes = self.get_nodes_raw(&candidate_ids)?;
                for (&cid, slot) in candidate_ids.iter().zip(nodes.iter()) {
                    if let Some(node) = slot {
                        if type_set.contains(&node.type_id) {
                            component.push(cid);
                            next_frontier.push(cid);
                        }
                    }
                }
            } else {
                component.extend(&candidate_ids);
                next_frontier.extend(&candidate_ids);
            }

            std::mem::swap(&mut frontier, &mut next_frontier);
        }

        component.sort_unstable();
        Ok(component)
    }
}
