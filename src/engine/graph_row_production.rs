use std::cell::Cell as ProductionCell;
#[cfg(test)]
use std::cell::RefCell as ProductionRefCell;
use std::collections::{
    BTreeMap as ProductionBTreeMap, BinaryHeap as ProductionBinaryHeap,
    HashSet as ProductionHashSet, VecDeque as ProductionVecDeque,
};
use std::rc::Rc as ProductionRc;
use std::sync::Arc as ProductionArc;

type GraphRowCachedRows = ProductionArc<Vec<crate::graph_row::GraphBindingRow>>;
type GraphRowCachedVlpResult = ProductionArc<GraphRowVlpSearchResult>;

const GRAPH_ROW_CHUNK_MAX: usize = 4096;
const GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowCapKind {
    IntermediateBindings,
    Frontier,
}

thread_local! {
    static GRAPH_ROW_CAP_TRIP: ProductionCell<Option<GraphRowCapKind>> = const { ProductionCell::new(None) };
}

fn graph_row_clear_cap_trip() {
    GRAPH_ROW_CAP_TRIP.with(|trip| trip.set(None));
}

fn graph_row_take_cap_trip() -> Option<GraphRowCapKind> {
    GRAPH_ROW_CAP_TRIP.with(|trip| trip.take())
}

fn graph_row_note_cap_trip(name: &str) {
    let kind = match name {
        "max_intermediate_bindings" => Some(GraphRowCapKind::IntermediateBindings),
        "max_frontier" => Some(GraphRowCapKind::Frontier),
        _ => None,
    };
    if let Some(kind) = kind {
        GRAPH_ROW_CAP_TRIP.with(|trip| trip.set(Some(kind)));
    }
}

#[cfg(test)]
thread_local! {
    static GRAPH_ROW_TEST_CHUNK_OVERRIDE: ProductionCell<Option<usize>> = const { ProductionCell::new(None) };
    static GRAPH_ROW_TEST_EARLY_EXIT_DISABLED: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_CURSOR_SEEK_DISABLED: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_SCRATCH_TRACE_ALLOCS: ProductionCell<usize> = const { ProductionCell::new(0) };
    static GRAPH_ROW_TEST_PREPARED_PULL_FAILURE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_ADJACENCY_PREPARE_FAILURE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_SINK_FAILURE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_NODE_VERIFICATION_FAILURE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_MIN: ProductionCell<Option<usize>> = const { ProductionCell::new(None) };
    static GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_EDGE: ProductionCell<Option<u64>> = const { ProductionCell::new(None) };
    static GRAPH_ROW_TEST_ADJACENCY_POSTING_FAILURE_OWNER: ProductionCell<Option<u64>> = const { ProductionCell::new(None) };
    static GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES: ProductionRefCell<Vec<usize>> = const { ProductionRefCell::new(Vec::new()) };
    static GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE_ACTIVE: ProductionCell<bool> = const { ProductionCell::new(false) };
    static GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE: ProductionRefCell<Vec<GraphRowTestAdjacencyPullTrace>> = const { ProductionRefCell::new(Vec::new()) };
}

#[cfg(test)]
fn with_graph_row_test_adjacency_posting_failure<T>(
    owner: u64,
    run: impl FnOnce() -> T,
) -> T {
    struct Reset(Option<u64>);

    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_ADJACENCY_POSTING_FAILURE_OWNER.with(|value| value.set(self.0));
        }
    }

    let previous = GRAPH_ROW_TEST_ADJACENCY_POSTING_FAILURE_OWNER
        .with(|value| value.replace(Some(owner)));
    let reset = Reset(previous);
    let result = run();
    drop(reset);
    result
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GraphRowTestAdjacencyPullTrace {
    oriented_metadata: usize,
    physical_scan_units: u64,
    raw_postings_scanned: u64,
    pending_completed_through_owner: Option<u64>,
    pending_last_completed_owner: Option<u64>,
    has_more: bool,
    partial_owner_pull: bool,
}

fn graph_row_scratch_trace(enabled: bool) -> Option<GraphRowExplainTrace> {
    if !enabled {
        return None;
    }
    #[cfg(test)]
    GRAPH_ROW_TEST_SCRATCH_TRACE_ALLOCS.with(|count| count.set(count.get().saturating_add(1)));
    Some(GraphRowExplainTrace::default())
}

#[cfg(test)]
fn graph_row_test_scratch_trace_allocs(run: impl FnOnce()) -> usize {
    GRAPH_ROW_TEST_SCRATCH_TRACE_ALLOCS.with(|count| count.set(0));
    run();
    GRAPH_ROW_TEST_SCRATCH_TRACE_ALLOCS.with(ProductionCell::get)
}

#[cfg(test)]
fn with_graph_row_test_prepared_pull_failure<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_PREPARED_PULL_FAILURE.with(|flag| flag.set(false));
        }
    }
    GRAPH_ROW_TEST_PREPARED_PULL_FAILURE.with(|flag| flag.set(true));
    let reset = Reset;
    let result = run();
    drop(reset);
    result
}

#[cfg(test)]
fn graph_row_test_take_prepared_pull_failure() -> bool {
    GRAPH_ROW_TEST_PREPARED_PULL_FAILURE.with(|flag| flag.replace(false))
}

#[cfg(test)]
fn with_graph_row_test_adjacency_prepare_failure<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_ADJACENCY_PREPARE_FAILURE.with(|flag| flag.set(false));
        }
    }
    GRAPH_ROW_TEST_ADJACENCY_PREPARE_FAILURE.with(|flag| flag.set(true));
    let reset = Reset;
    let result = run();
    drop(reset);
    result
}

#[cfg(test)]
fn graph_row_test_take_adjacency_prepare_failure() -> bool {
    GRAPH_ROW_TEST_ADJACENCY_PREPARE_FAILURE.with(|flag| flag.replace(false))
}

#[cfg(test)]
fn with_graph_row_test_sink_failure<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_SINK_FAILURE.with(|flag| flag.set(false));
        }
    }
    GRAPH_ROW_TEST_SINK_FAILURE.with(|flag| flag.set(true));
    let reset = Reset;
    let result = run();
    drop(reset);
    result
}

#[cfg(test)]
fn graph_row_test_take_sink_failure() -> bool {
    GRAPH_ROW_TEST_SINK_FAILURE.with(|flag| flag.replace(false))
}

#[cfg(test)]
fn with_graph_row_test_node_verification_failure<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_NODE_VERIFICATION_FAILURE.with(|flag| flag.set(false));
        }
    }
    GRAPH_ROW_TEST_NODE_VERIFICATION_FAILURE.with(|flag| flag.set(true));
    let reset = Reset;
    let result = run();
    drop(reset);
    result
}

#[cfg(test)]
fn graph_row_test_take_node_verification_failure() -> bool {
    GRAPH_ROW_TEST_NODE_VERIFICATION_FAILURE.with(|flag| flag.replace(false))
}

#[cfg(test)]
fn with_graph_row_test_forced_adjacency_retry<T>(
    minimum_input: usize,
    run: impl FnOnce() -> T,
) -> (T, Vec<usize>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_MIN.with(|value| value.set(None));
            GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE.with(|value| value.set(false));
            GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow_mut().clear());
        }
    }
    GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_MIN.with(|value| value.set(Some(minimum_input)));
    GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE.with(|value| value.set(true));
    GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow_mut().clear());
    let reset = Reset;
    let result = run();
    let sizes = GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow().clone());
    drop(reset);
    (result, sizes)
}

#[cfg(test)]
fn with_graph_row_test_forced_adjacency_retry_edge<T>(
    edge_id: u64,
    run: impl FnOnce() -> T,
) -> (T, Vec<usize>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_EDGE.with(|value| value.set(None));
            GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE.with(|value| value.set(false));
            GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow_mut().clear());
        }
    }
    GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_EDGE.with(|value| value.set(Some(edge_id)));
    GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE.with(|value| value.set(true));
    GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow_mut().clear());
    let reset = Reset;
    let result = run();
    let sizes = GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES.with(|sizes| sizes.borrow().clone());
    drop(reset);
    (result, sizes)
}

#[cfg(test)]
fn with_graph_row_test_adjacency_pull_trace<T>(
    run: impl FnOnce() -> T,
) -> (T, Vec<GraphRowTestAdjacencyPullTrace>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE_ACTIVE.with(|value| value.set(false));
            GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE.with(|trace| trace.borrow_mut().clear());
        }
    }
    GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE_ACTIVE.with(|value| value.set(true));
    GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE.with(|trace| trace.borrow_mut().clear());
    let reset = Reset;
    let result = run();
    let trace = GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE.with(|trace| trace.borrow().clone());
    drop(reset);
    (result, trace)
}

fn graph_row_scheduled_chunk_size(
    previous: Option<usize>,
    selection_capacity: usize,
    proof: Option<AnchorMonotonicityProof>,
    max_intermediate_bindings: usize,
) -> usize {
    let maximum = GRAPH_ROW_CHUNK_MAX.min(max_intermediate_bindings);
    #[cfg(test)]
    if let Some(overridden) = GRAPH_ROW_TEST_CHUNK_OVERRIDE.with(ProductionCell::get) {
        return overridden.max(1).min(maximum);
    }
    match previous {
        Some(previous) => previous.saturating_mul(2).min(maximum),
        None if proof.is_some() => selection_capacity.clamp(1, maximum),
        None => maximum,
    }
}

fn graph_row_edge_pull_scheduled_chunk_size(
    direction: Direction,
    max_intermediate_bindings: usize,
    max_frontier: usize,
) -> usize {
    let base_max = GRAPH_ROW_CHUNK_MAX
        .min(max_intermediate_bindings)
        .min(max_frontier.max(1));
    debug_assert!(base_max > 0, "normalized graph-row intermediate cap must be positive");
    let direction_max = if direction == Direction::Both {
        (base_max / 2).max(1)
    } else {
        base_max
    };
    #[cfg(test)]
    if let Some(overridden) = GRAPH_ROW_TEST_CHUNK_OVERRIDE.with(ProductionCell::get) {
        return overridden.clamp(1, direction_max);
    }
    direction_max
}

fn graph_row_prepared_edge_mode_label(mode: PreparedEdgeSourceMode) -> &'static str {
    match mode {
        PreparedEdgeSourceMode::Empty => "Empty",
        PreparedEdgeSourceMode::NativeCursor => "NativeCursor",
        PreparedEdgeSourceMode::RetainedIds => "RetainedIds",
        PreparedEdgeSourceMode::StreamedSingle => "StreamedSingle",
        PreparedEdgeSourceMode::StreamedUnion => "StreamedUnion",
        PreparedEdgeSourceMode::StreamedIntersect => "StreamedIntersect",
    }
}

fn graph_row_prepared_edge_fallback_label(fallback: PreparedEdgeFallback) -> &'static str {
    match fallback {
        PreparedEdgeFallback::None => "none",
        PreparedEdgeFallback::PreparedLegalUniverse => "prepared-legal-universe",
    }
}

#[cfg(test)]
fn graph_row_test_early_exit_disabled() -> bool {
    GRAPH_ROW_TEST_EARLY_EXIT_DISABLED.with(ProductionCell::get)
}

#[cfg(not(test))]
fn graph_row_test_early_exit_disabled() -> bool {
    false
}

#[cfg(test)]
fn graph_row_test_cursor_seek_disabled() -> bool {
    GRAPH_ROW_TEST_CURSOR_SEEK_DISABLED.with(ProductionCell::get)
}

#[cfg(not(test))]
fn graph_row_test_cursor_seek_disabled() -> bool {
    false
}

fn graph_row_schedule_retry_halves<'a, T>(stack: &mut Vec<&'a [T]>, items: &'a [T]) {
    let split = items.len() / 2;
    // LIFO: push right first so the left half is always the next consumed leaf.
    stack.push(&items[split..]);
    stack.push(&items[..split]);
}

#[cfg(test)]
fn with_graph_row_test_chunk_override<T>(size: usize, run: impl FnOnce() -> T) -> T {
    struct Reset(Option<usize>);
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_CHUNK_OVERRIDE.with(|value| value.set(self.0));
        }
    }
    GRAPH_ROW_TEST_CHUNK_OVERRIDE.with(|value| {
        let reset = Reset(value.replace(Some(size)));
        let result = run();
        drop(reset);
        result
    })
}

#[cfg(test)]
fn with_graph_row_test_early_exit_disabled<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_EARLY_EXIT_DISABLED.with(|value| value.set(self.0));
        }
    }
    GRAPH_ROW_TEST_EARLY_EXIT_DISABLED.with(|value| {
        let reset = Reset(value.replace(true));
        let result = run();
        drop(reset);
        result
    })
}

#[cfg(test)]
fn with_graph_row_test_cursor_seek_disabled<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            GRAPH_ROW_TEST_CURSOR_SEEK_DISABLED.with(|value| value.set(self.0));
        }
    }
    GRAPH_ROW_TEST_CURSOR_SEEK_DISABLED.with(|value| {
        let reset = Reset(value.replace(true));
        let result = run();
        drop(reset);
        result
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct GraphRowCorrelatedOptionalCacheKey {
    piece_path: Box<[usize]>,
    dependency_key: Vec<crate::graph_row::GraphSortAtom>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct GraphRowVlpResultCacheKey {
    piece_path: Box<[usize]>,
    search_key: GraphRowVlpSearchKey,
}

enum GraphRowResultCacheJournalEntry {
    UncorrelatedOptional { piece_path: Box<[usize]>, units: usize },
    CorrelatedOptional {
        key: GraphRowCorrelatedOptionalCacheKey,
        units: usize,
    },
    VariableLength {
        key: GraphRowVlpResultCacheKey,
        units: usize,
    },
}

#[derive(Default)]
struct GraphRowCacheAttempt {
    journal: Vec<GraphRowResultCacheJournalEntry>,
}

impl GraphRowCacheAttempt {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.journal.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowResultCacheMode {
    Admitting,
    ReadOnly,
    Bypass,
}

#[derive(Default)]
struct GraphRowFixedEdgeWorkspace {
    oriented_edges: Vec<GraphRowOrientedEdge>,
    from_candidates: Vec<u64>,
    to_candidates: Vec<u64>,
    verification_misses: Vec<u64>,
    verified_ids: Vec<u64>,
    from_verified: NodeIdSet,
    to_verified: NodeIdSet,
    buckets: GraphRowEdgeCandidateBuckets,
}

impl GraphRowFixedEdgeWorkspace {
    fn clear(&mut self) {
        self.oriented_edges.clear();
        self.from_candidates.clear();
        self.to_candidates.clear();
        self.verification_misses.clear();
        self.verified_ids.clear();
        self.from_verified.clear();
        self.to_verified.clear();
        self.buckets.clear();
    }

    #[cfg(test)]
    fn retained_capacity(&self) -> usize {
        self.oriented_edges
            .capacity()
            .saturating_add(self.from_candidates.capacity())
            .saturating_add(self.to_candidates.capacity())
            .saturating_add(self.verification_misses.capacity())
            .saturating_add(self.verified_ids.capacity())
            .saturating_add(self.from_verified.capacity())
            .saturating_add(self.to_verified.capacity())
            .saturating_add(self.buckets.retained_capacity())
    }
}

struct GraphRowExecutionCaches {
    optional_physical_plans: ProductionBTreeMap<Box<[usize]>, ProductionRc<GraphRowPhysicalPlan>>,
    uncorrelated_optional_results: ProductionBTreeMap<Box<[usize]>, GraphRowCachedRows>,
    correlated_optional_results:
        ProductionBTreeMap<GraphRowCorrelatedOptionalCacheKey, GraphRowCachedRows>,
    vlp_results: ProductionBTreeMap<GraphRowVlpResultCacheKey, GraphRowCachedVlpResult>,
    max_units: usize,
    used_units: usize,
    peak_units: usize,
    no_admit: usize,
    optional_hits: usize,
    vlp_hits: usize,
    result_cache_mode: GraphRowResultCacheMode,
    node_verification_cache:
        ProductionBTreeMap<(crate::graph_row::GraphBindingSlotRef, u64), bool>,
    node_verification_fifo:
        ProductionVecDeque<(crate::graph_row::GraphBindingSlotRef, u64)>,
    node_verification_enabled: bool,
    fixed_edge_workspace: GraphRowFixedEdgeWorkspace,
    #[cfg(test)]
    reported_no_admit: usize,
}

impl GraphRowExecutionCaches {
    fn new(max_units: usize) -> Self {
        Self {
            optional_physical_plans: ProductionBTreeMap::new(),
            uncorrelated_optional_results: ProductionBTreeMap::new(),
            correlated_optional_results: ProductionBTreeMap::new(),
            vlp_results: ProductionBTreeMap::new(),
            max_units,
            used_units: 0,
            peak_units: 0,
            no_admit: 0,
            optional_hits: 0,
            vlp_hits: 0,
            result_cache_mode: GraphRowResultCacheMode::Admitting,
            node_verification_cache: ProductionBTreeMap::new(),
            node_verification_fifo: ProductionVecDeque::new(),
            node_verification_enabled: false,
            fixed_edge_workspace: GraphRowFixedEdgeWorkspace::default(),
            #[cfg(test)]
            reported_no_admit: 0,
        }
    }

    fn compatibility(max_units: usize) -> Self {
        let mut caches = Self::new(max_units);
        caches.result_cache_mode = GraphRowResultCacheMode::Bypass;
        caches
    }

    fn for_source(max_units: usize, source: &GraphRowChunkSource) -> Self {
        match source {
            GraphRowChunkSource::AnchorPull { .. } => Self::new(max_units),
            GraphRowChunkSource::EdgePull { .. } | GraphRowChunkSource::AdjacencyPull { .. } => {
                let mut caches = Self::new(max_units);
                caches.node_verification_enabled = true;
                caches
            }
            // RuntimeOnce admission measured +21-29% on FO-033 optional in CP43.2/CP43.3; keep it disabled.
            GraphRowChunkSource::RuntimeOnce { .. } => Self::compatibility(max_units),
        }
    }

    fn node_verification_get(
        &self,
        slot: crate::graph_row::GraphBindingSlotRef,
        node_id: u64,
    ) -> Option<bool> {
        self.node_verification_enabled
            .then(|| self.node_verification_cache.get(&(slot, node_id)).copied())
            .flatten()
    }

    fn node_verification_insert(
        &mut self,
        slot: crate::graph_row::GraphBindingSlotRef,
        node_id: u64,
        matches: bool,
    ) {
        if !self.node_verification_enabled
            || self.node_verification_cache.contains_key(&(slot, node_id))
        {
            return;
        }
        if self.node_verification_cache.len() == GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP {
            let oldest = self
                .node_verification_fifo
                .pop_front()
                .expect("full node-verification cache must have a FIFO head");
            let removed = self.node_verification_cache.remove(&oldest);
            debug_assert!(removed.is_some());
        }
        self.node_verification_cache.insert((slot, node_id), matches);
        self.node_verification_fifo.push_back((slot, node_id));
        debug_assert!(self.node_verification_cache.len() <= GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP);
    }

    fn begin_attempt(&self) -> GraphRowCacheAttempt {
        GraphRowCacheAttempt::default()
    }

    fn prepare_leaf(&mut self, remaining_work: bool) {
        self.result_cache_mode = if remaining_work {
            GraphRowResultCacheMode::Admitting
        } else if self.has_global_results() {
            GraphRowResultCacheMode::ReadOnly
        } else {
            GraphRowResultCacheMode::Bypass
        };
    }

    fn bypasses_execution_wide_results(&self) -> bool {
        self.result_cache_mode == GraphRowResultCacheMode::Bypass
    }

    fn can_read_global_results(&self) -> bool {
        self.result_cache_mode != GraphRowResultCacheMode::Bypass
    }

    fn has_global_results(&self) -> bool {
        debug_assert_eq!(
            self.used_units == 0,
            self.uncorrelated_optional_results.is_empty()
                && self.correlated_optional_results.is_empty()
                && self.vlp_results.is_empty(),
            "positive result-cache units must exactly track retained global results"
        );
        self.used_units > 0
    }

    fn commit_attempt(&mut self, attempt: &mut GraphRowCacheAttempt) {
        attempt.journal.clear();
    }

    fn rollback_attempt(&mut self, attempt: &mut GraphRowCacheAttempt) {
        for entry in attempt.journal.drain(..).rev() {
            let units = match entry {
                GraphRowResultCacheJournalEntry::UncorrelatedOptional { piece_path, units } => {
                    self.uncorrelated_optional_results.remove(&piece_path);
                    units
                }
                GraphRowResultCacheJournalEntry::CorrelatedOptional { key, units } => {
                    self.correlated_optional_results.remove(&key);
                    units
                }
                GraphRowResultCacheJournalEntry::VariableLength { key, units } => {
                    self.vlp_results.remove(&key);
                    units
                }
            };
            self.used_units = self
                .used_units
                .checked_sub(units)
                .expect("result-cache rollback must restore admitted units exactly");
        }
    }

    fn can_admit(&mut self, units: usize) -> bool {
        // RuntimeOnce and final AnchorPull leaves deliberately preserve the CP43.2
        // compatibility policy: policy-disabled admission is not budget pressure and
        // must not count as a no-admit event in stats or explain output.
        if self.result_cache_mode != GraphRowResultCacheMode::Admitting {
            return false;
        }
        if units > self.max_units.saturating_sub(self.used_units) {
            self.no_admit = self.no_admit.saturating_add(1);
            return false;
        }
        self.used_units += units;
        self.peak_units = self.peak_units.max(self.used_units);
        true
    }

    fn optional_units(rows: &[crate::graph_row::GraphBindingRow]) -> usize {
        rows.len().max(1)
    }

    fn vlp_units(result: &GraphRowVlpSearchResult) -> usize {
        result.paths.len().max(1)
    }

    fn admit_uncorrelated_optional(
        &mut self,
        piece_path: Box<[usize]>,
        value: GraphRowCachedRows,
        attempt: &mut GraphRowCacheAttempt,
    ) -> bool {
        if self.uncorrelated_optional_results.contains_key(&piece_path) {
            return true;
        }
        let units = Self::optional_units(value.as_slice());
        if !self.can_admit(units) {
            return false;
        }
        self.uncorrelated_optional_results
            .insert(piece_path.clone(), value);
        attempt
            .journal
            .push(GraphRowResultCacheJournalEntry::UncorrelatedOptional {
                piece_path,
                units,
            });
        true
    }

    fn admit_correlated_optional(
        &mut self,
        key: GraphRowCorrelatedOptionalCacheKey,
        value: GraphRowCachedRows,
        attempt: &mut GraphRowCacheAttempt,
    ) -> bool {
        if self.correlated_optional_results.contains_key(&key) {
            return true;
        }
        let units = Self::optional_units(value.as_slice());
        if !self.can_admit(units) {
            return false;
        }
        self.correlated_optional_results.insert(key.clone(), value);
        attempt
            .journal
            .push(GraphRowResultCacheJournalEntry::CorrelatedOptional { key, units });
        true
    }

    fn admit_vlp(
        &mut self,
        key: GraphRowVlpResultCacheKey,
        value: GraphRowCachedVlpResult,
        attempt: &mut GraphRowCacheAttempt,
    ) -> bool {
        if self.vlp_results.contains_key(&key) {
            return true;
        }
        let units = Self::vlp_units(value.as_ref());
        if !self.can_admit(units) {
            return false;
        }
        self.vlp_results.insert(key.clone(), value);
        attempt
            .journal
            .push(GraphRowResultCacheJournalEntry::VariableLength { key, units });
        true
    }

    #[cfg(test)]
    fn optional_physical_plan_count(&self) -> usize {
        self.optional_physical_plans.len()
    }
}

#[derive(Default)]
struct GraphRowLeafTransientState {
    uncorrelated_optional_results: ProductionBTreeMap<Box<[usize]>, GraphRowCachedRows>,
    correlated_optional_results:
        ProductionBTreeMap<GraphRowCorrelatedOptionalCacheKey, GraphRowCachedRows>,
    vlp_results: ProductionBTreeMap<GraphRowVlpResultCacheKey, GraphRowCachedVlpResult>,
}

struct GraphRowUniqueFollowups {
    seen: ProductionHashSet<SecondaryIndexReadFollowupKey>,
    followups: Vec<SecondaryIndexReadFollowup>,
    peak: usize,
}

impl GraphRowUniqueFollowups {
    fn new(seed: Vec<SecondaryIndexReadFollowup>) -> Self {
        let mut accumulator = Self {
            seen: ProductionHashSet::new(),
            followups: Vec::new(),
            peak: 0,
        };
        accumulator.extend(seed);
        accumulator
    }

    fn extend(&mut self, followups: Vec<SecondaryIndexReadFollowup>) {
        for followup in followups {
            if self.seen.insert(followup.dedup_key()) {
                self.followups.push(followup);
                self.peak = self.peak.max(self.followups.len());
            }
        }
    }

    fn dedup_appended(&mut self, unique_prefix_len: usize) {
        let appended = self.followups.split_off(unique_prefix_len);
        self.extend(appended);
    }

    fn drain_into(&mut self, target: &mut Vec<SecondaryIndexReadFollowup>) {
        target.append(&mut self.followups);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowSourceOrder {
    NodeIdAsc,
    LogicalFromGroupAsc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowProofBoundary {
    SourceRow,
    CompletedOwnerGroup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AnchorMonotonicityProof {
    anchor_slot: crate::graph_row::GraphBindingSlotRef,
    logical_key_ordinal: usize,
    source_order: GraphRowSourceOrder,
    boundary: GraphRowProofBoundary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowChunkEligibility {
    Eligible,
    OrderedQuery,
    AnchorNotFirstLogicalSlot,
    NonNodeInitialDriver,
    EdgeIdOrderNotLogicalPrefix,
    StageSink,
    RuntimeOnce,
}

impl GraphRowChunkEligibility {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::OrderedQuery => "ordered_query",
            Self::AnchorNotFirstLogicalSlot => "anchor_not_first_logical_slot",
            Self::NonNodeInitialDriver => "non_node_initial_driver",
            Self::EdgeIdOrderNotLogicalPrefix => "edge_id_order_not_logical_prefix",
            Self::StageSink => "stage_sink",
            Self::RuntimeOnce => "runtime_once",
        }
    }

    fn is_eligible(self) -> bool {
        self == Self::Eligible
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowRuntimeOnceReason {
    NonNodeInitialDriver,
    Stage,
    Exists,
}

impl GraphRowRuntimeOnceReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::NonNodeInitialDriver => "NonNodeInitialDriver",
            Self::Stage => "Stage",
            Self::Exists => "Exists",
        }
    }
}

enum GraphRowChunkSource {
    AnchorPull {
        alias: String,
        node_index: usize,
        anchor_slot: crate::graph_row::GraphBindingSlotRef,
    },
    EdgePull {
        edge_index: usize,
        edge_name: String,
    },
    AdjacencyPull {
        edge_index: usize,
        edge_name: String,
        anchor_slot: crate::graph_row::GraphBindingSlotRef,
    },
    RuntimeOnce {
        reason: GraphRowRuntimeOnceReason,
        initial_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
        goal: GraphRowRuntimeGoal,
    },
}

struct GraphRowRuntimeOnceExecution {
    reason: GraphRowRuntimeOnceReason,
    initial_rows: Option<Vec<crate::graph_row::GraphBindingRow>>,
    goal: GraphRowRuntimeGoal,
}

impl GraphRowChunkSource {
    fn description(&self) -> String {
        match self {
            Self::AnchorPull { alias, .. } => format!("AnchorPull{{alias={alias}}}"),
            Self::EdgePull { edge_name, .. } => format!("EdgePull{{edge={edge_name}}}"),
            Self::AdjacencyPull { edge_name, .. } => {
                format!("AdjacencyPull{{edge={edge_name}}}")
            }
            Self::RuntimeOnce { reason, .. } => {
                format!("RuntimeOnce{{reason={}}}", reason.as_str())
            }
        }
    }
}

struct GraphRowProductionSummary {
    source: String,
    source_pulls: usize,
    successful_leaves: usize,
    scheduled_first: Option<usize>,
    scheduled_last: Option<usize>,
    leaf_size_min: usize,
    leaf_size_max: usize,
    source_rows: usize,
    produced_rows: usize,
    eligibility: GraphRowChunkEligibility,
    early_exit: bool,
    cursor_seek_anchor: Option<u64>,
    heap_capacity: usize,
    cap_retries: usize,
    result_cache_units: usize,
    result_cache_max: usize,
    cache_no_admit: usize,
    optional_cache_hits: usize,
    vlp_cross_chunk_cache_hits: usize,
    adjacency: Option<GraphRowAdjacencyProductionSummary>,
    fallback: Option<&'static str>,
}

#[derive(Default)]
struct GraphRowAdjacencyProductionSummary {
    completed_owner_groups: usize,
    last_completed_owner: Option<u64>,
    physical_scan_units: u64,
    cursor_seek_units: u64,
    adj_index_entries_scanned: u64,
    raw_adjacency_postings_scanned: u64,
    shadowed_or_stale_postings: u64,
    partial_owner_pulls: usize,
}

impl GraphRowProductionSummary {
    fn new(
        source: String,
        eligibility: GraphRowChunkEligibility,
        heap_capacity: usize,
        result_cache_max: usize,
    ) -> Self {
        Self {
            source,
            source_pulls: 0,
            successful_leaves: 0,
            scheduled_first: None,
            scheduled_last: None,
            leaf_size_min: 0,
            leaf_size_max: 0,
            source_rows: 0,
            produced_rows: 0,
            eligibility,
            early_exit: false,
            cursor_seek_anchor: None,
            heap_capacity,
            cap_retries: 0,
            result_cache_units: 0,
            result_cache_max,
            cache_no_admit: 0,
            optional_cache_hits: 0,
            vlp_cross_chunk_cache_hits: 0,
            adjacency: None,
            fallback: None,
        }
    }

    fn note_scheduled_pull(&mut self, size: usize) {
        self.source_pulls = self.source_pulls.saturating_add(1);
        self.scheduled_first.get_or_insert(size);
        self.scheduled_last = Some(size);
    }

    fn note_successful_leaf(&mut self, source_rows: usize, produced_rows: usize) {
        self.successful_leaves = self.successful_leaves.saturating_add(1);
        if self.successful_leaves == 1 {
            self.leaf_size_min = source_rows;
            self.leaf_size_max = source_rows;
        } else {
            self.leaf_size_min = self.leaf_size_min.min(source_rows);
            self.leaf_size_max = self.leaf_size_max.max(source_rows);
        }
        self.produced_rows = self.produced_rows.saturating_add(produced_rows);
    }

    fn detail(&self) -> String {
        let scheduled_sizes = match (self.scheduled_first, self.scheduled_last) {
            (Some(first), Some(last)) => format!("{first}..{last}"),
            _ => "[]".to_string(),
        };
        let cursor_seek = self.cursor_seek_anchor.map_or_else(
            || "none".to_string(),
            |anchor| format!("anchor_ge:{anchor}"),
        );
        let mut detail = format!(
            "source={}; source_pulls={}; successful_leaves={}; scheduled_sizes={}; leaf_size_min={}; leaf_size_max={}; source_rows={}; produced_rows={}; early_exit_eligible={}; eligibility={}; early_exit={}; cursor_seek={}; heap_capacity={}; cap_retries={}; result_cache_units={}/{}; cache_no_admit={}; optional_cache_hits={}; vlp_cross_chunk_cache_hits={}",
            self.source,
            self.source_pulls,
            self.successful_leaves,
            scheduled_sizes,
            self.leaf_size_min,
            self.leaf_size_max,
            self.source_rows,
            self.produced_rows,
            self.eligibility.is_eligible(),
            self.eligibility.as_str(),
            self.early_exit,
            cursor_seek,
            self.heap_capacity,
            self.cap_retries,
            self.result_cache_units,
            self.result_cache_max,
            self.cache_no_admit,
            self.optional_cache_hits,
            self.vlp_cross_chunk_cache_hits,
        );
        if let Some(adjacency) = &self.adjacency {
            let last_completed_owner = adjacency.last_completed_owner.map_or_else(
                || "none".to_string(),
                |owner| owner.to_string(),
            );
            detail.push_str(&format!(
                "; source_order=logical_from_group_asc; proof_boundary=completed_owner_group; completed_owner_groups={}; last_completed_owner={last_completed_owner}; physical_scan_units={}; cursor_seek_units={}; adj_index_entries_scanned={}; raw_adjacency_postings_scanned={}; shadowed_or_stale_postings={}; partial_owner_pulls={}",
                adjacency.completed_owner_groups,
                adjacency.physical_scan_units,
                adjacency.cursor_seek_units,
                adjacency.adj_index_entries_scanned,
                adjacency.raw_adjacency_postings_scanned,
                adjacency.shadowed_or_stale_postings,
                adjacency.partial_owner_pulls,
            ));
        }
        if let Some(fallback) = self.fallback {
            detail.push_str("; fallback=");
            detail.push_str(fallback);
        }
        detail
    }
}

fn graph_row_cursor_seek_anchor(
    proof: Option<AnchorMonotonicityProof>,
    cursor_state: &GraphRowCursorState,
) -> Option<u64> {
    let proof = proof?;
    let cursor = cursor_state.decoded.as_ref()?;
    match cursor.last_logical_row_key.get(proof.logical_key_ordinal) {
        Some(crate::graph_row::GraphSortAtom::Node(id)) => Some(*id),
        _ => None,
    }
}

fn graph_row_cursor_owner_inclusive(cursor_state: &GraphRowCursorState) -> Option<u64> {
    let cursor = cursor_state.decoded.as_ref()?;
    match cursor.last_logical_row_key.first() {
        Some(crate::graph_row::GraphSortAtom::Node(id)) => Some(*id),
        _ => None,
    }
}

struct GraphRowPageProduction {
    selected: ProductionBinaryHeap<GraphRowHeapCandidate>,
    pre_page_needs: crate::row_projection::EntityProjectionNeeds,
    rows_after_filter: usize,
    rows_seen_for_page: usize,
    intermediate_peak: usize,
    frontier_peak: usize,
    paths_enumerated: usize,
    planning_ns_delta: u64,
    followups: Vec<SecondaryIndexReadFollowup>,
}

struct GraphRowPageSink<'a> {
    view: &'a ReadView,
    query: &'a NormalizedGraphRowQuery,
    cursor_state: &'a GraphRowCursorState,
    selection_capacity: usize,
    order_directions: ProductionArc<[GraphOrderDirection]>,
    selected: ProductionBinaryHeap<GraphRowHeapCandidate>,
    pre_page_needs: crate::row_projection::EntityProjectionNeeds,
    order_needs_remaining: crate::row_projection::EntityProjectionNeeds,
    rows_after_filter: usize,
    rows_seen_for_page: usize,
}

impl<'a> GraphRowPageSink<'a> {
    fn new(
        view: &'a ReadView,
        query: &'a NormalizedGraphRowQuery,
        cursor_state: &'a GraphRowCursorState,
        selection_capacity: usize,
    ) -> Self {
        let mut pre_page_needs = query.projection_needs.residual.clone();
        pre_page_needs
            .merge_from(
                &query.projection_needs.order,
                ProjectionNeedClass::Order,
            )
            .expect("normalized projection needs must merge");
        let order_needs_remaining = graph_row_remaining_output_needs(
            &query.projection_needs.order,
            &query.projection_needs.residual,
        );
        Self {
            view,
            query,
            cursor_state,
            selection_capacity,
            order_directions: graph_row_order_directions(&query.bound_order_by),
            selected: ProductionBinaryHeap::new(),
            pre_page_needs,
            order_needs_remaining,
            rows_after_filter: 0,
            rows_seen_for_page: 0,
        }
    }

    fn consume(
        &mut self,
        mut rows: Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<(), EngineError> {
        self.view.hydrate_graph_rows_for_needs(
            &mut rows,
            &self.query.binding_schema,
            &self.query.projection_needs.residual,
        )?;
        let mut survivors = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(where_expr) = self.query.bound_where.as_ref() {
                let context = crate::graph_row::BoundGraphEvalContext { row: &row };
                if !crate::graph_row::eval_bound_graph_predicate(where_expr, &context)? {
                    continue;
                }
            }
            survivors.push(row);
        }
        self.rows_after_filter = self.rows_after_filter.saturating_add(survivors.len());

        self.view.hydrate_graph_rows_for_needs(
            &mut survivors,
            &self.query.binding_schema,
            &self.order_needs_remaining,
        )?;
        for row in survivors {
            let logical_key = row.logical_sort_key(&self.query.binding_schema)?;
            let sort_key = graph_row_explicit_sort_key(self.query, &row)?;
            if let Some(cursor) = self.cursor_state.decoded.as_ref() {
                if compare_graph_final_keys_by_directions(
                    &sort_key,
                    &logical_key,
                    &cursor.last_sort_key,
                    &cursor.last_logical_row_key,
                    &self.order_directions,
                ) != std::cmp::Ordering::Greater
                {
                    continue;
                }
            }
            self.rows_seen_for_page = self.rows_seen_for_page.saturating_add(1);
            graph_row_insert_bounded_candidate(
                &mut self.selected,
                GraphRowPageCandidate {
                    sort_key,
                    logical_key,
                    row,
                },
                self.selection_capacity,
                &self.order_directions,
            );
        }
        #[cfg(test)]
        self.view.note_graph_row_page_heap_rows_peak(self.selected.len());
        Ok(())
    }

    fn page_target_met(&self) -> bool {
        self.selected.len() == self.selection_capacity
    }
}

struct GraphRowCollectAllSink {
    rows: Vec<crate::graph_row::GraphBindingRow>,
}

impl GraphRowCollectAllSink {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn consume(&mut self, mut rows: Vec<crate::graph_row::GraphBindingRow>) {
        self.rows.append(&mut rows);
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "boxing the page sink would add a heap allocation to every bounded graph-row read"
)]
enum GraphRowProductionSink<'a> {
    Page(GraphRowPageSink<'a>),
    CollectAll(GraphRowCollectAllSink),
}

impl GraphRowProductionSink<'_> {
    fn consume(
        &mut self,
        rows: Vec<crate::graph_row::GraphBindingRow>,
    ) -> Result<(), EngineError> {
        #[cfg(test)]
        if graph_row_test_take_sink_failure() {
            return Err(EngineError::InvalidOperation(
                "injected graph-row production sink failure".to_string(),
            ));
        }
        match self {
            Self::Page(sink) => sink.consume(rows),
            Self::CollectAll(sink) => {
                sink.consume(rows);
                Ok(())
            }
        }
    }

    fn page_target_met(&self) -> bool {
        match self {
            Self::Page(sink) => sink.page_target_met(),
            Self::CollectAll(_) => false,
        }
    }

    fn skips_zero_selection(&self, selection_capacity: usize) -> bool {
        matches!(self, Self::Page(_)) && selection_capacity == 0
    }
}

enum GraphRowProductionOutput {
    Page(GraphRowPageProduction),
    CollectAll(GraphRowCollectAllProduction),
}

struct GraphRowCollectAllProduction {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    intermediate_peak: usize,
    frontier_peak: usize,
    paths_enumerated: usize,
    followups: Vec<SecondaryIndexReadFollowup>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowDriverControl {
    Continue,
    StopEarly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommittedAdjacencyOwnerBoundary {
    owner_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdjacencyGroupMonotonicityProof {
    anchor_slot: crate::graph_row::GraphBindingSlotRef,
    logical_key_ordinal: usize,
}

fn graph_row_adjacency_group_monotonicity_proof(
    proof: Option<AnchorMonotonicityProof>,
) -> Option<AdjacencyGroupMonotonicityProof> {
    match proof {
        Some(AnchorMonotonicityProof {
            anchor_slot,
            logical_key_ordinal,
            source_order: GraphRowSourceOrder::LogicalFromGroupAsc,
            boundary: GraphRowProofBoundary::CompletedOwnerGroup,
        }) => Some(AdjacencyGroupMonotonicityProof {
            anchor_slot,
            logical_key_ordinal,
        }),
        _ => None,
    }
}

fn graph_row_committed_adjacency_owner_boundary(
    _proof: AdjacencyGroupMonotonicityProof,
    owner_id: Option<u64>,
) -> Option<CommittedAdjacencyOwnerBoundary> {
    owner_id.map(|owner_id| CommittedAdjacencyOwnerBoundary { owner_id })
}

fn graph_row_adjacency_pull_should_stop(
    sink: &GraphRowProductionSink<'_>,
    _proof: AdjacencyGroupMonotonicityProof,
    boundary: CommittedAdjacencyOwnerBoundary,
    has_more: bool,
) -> bool {
    let _ = boundary.owner_id;
    sink.page_target_met()
        && has_more
        && !graph_row_test_early_exit_disabled()
}

fn graph_row_edge_pull_route_is_eligible(
    query: &NormalizedGraphRowQuery,
    runtime: &GraphRowRuntimePlan,
    physical_plan: &GraphRowPhysicalPlan,
) -> bool {
    if !query.initial_bound_slots.is_empty()
        || !matches!(
            runtime.steps.first(),
            Some(GraphRowRuntimeStep::RequiredSegment(0))
        )
    {
        return false;
    }
    let GraphRowInitialDriver::Edge { edge_index, .. } = &physical_plan.initial_driver else {
        return false;
    };
    let Some(segment) = physical_plan
        .segments
        .iter()
        .find(|segment| segment.segment_index == 0)
    else {
        return false;
    };

    debug_assert_eq!(
        physical_plan
            .segments
            .first()
            .map(|segment| segment.segment_index),
        Some(0),
        "graph-row segment 0 must be stored first"
    );
    debug_assert!(
        segment.barriers_before.is_empty(),
        "edge-first graph-row segment 0 must not have preceding barriers"
    );
    debug_assert!(
        matches!(
            &segment.initial_driver,
            GraphRowInitialDriver::Edge {
                edge_index: segment_edge_index,
                ..
            } if segment_edge_index == edge_index
        ),
        "global and segment-0 graph-row edge drivers must agree"
    );
    debug_assert_eq!(
        segment.edge_order.first(),
        Some(edge_index),
        "segment-0 graph-row edge order must start with the anchored edge"
    );

    segment.barriers_before.is_empty()
        && matches!(
        &segment.initial_driver,
        GraphRowInitialDriver::Edge {
            edge_index: segment_edge_index,
            ..
        } if segment_edge_index == edge_index
    ) && segment.edge_order.first() == Some(edge_index)
}

fn graph_row_chunk_source(
    query: &NormalizedGraphRowQuery,
    runtime: &GraphRowRuntimePlan,
    physical_plan: &GraphRowPhysicalPlan,
) -> GraphRowChunkSource {
    if graph_row_edge_pull_route_is_eligible(query, runtime, physical_plan) {
        let GraphRowInitialDriver::Edge {
            edge_index,
            edge_name,
        } = &physical_plan.initial_driver
        else {
            unreachable!("eligible edge-pull route must have an edge initial driver")
        };
        return match physical_plan
            .initial_edge_production
            .expect("edge initial driver must select one production")
        {
            GraphRowEdgeAnchorProduction::EdgePull => GraphRowChunkSource::EdgePull {
                edge_index: *edge_index,
                edge_name: edge_name.clone(),
            },
            GraphRowEdgeAnchorProduction::AdjacencyPull => {
                let edge = &runtime.edges[*edge_index];
                GraphRowChunkSource::AdjacencyPull {
                    edge_index: *edge_index,
                    edge_name: edge_name.clone(),
                    anchor_slot: edge.from_slot,
                }
            }
        };
    }
    let node_driver = match &physical_plan.initial_driver {
        GraphRowInitialDriver::Node {
            node_index,
            alias,
        } => Some((*node_index, alias.clone())),
        GraphRowInitialDriver::Empty { .. } if runtime.steps.is_empty() && !runtime.nodes.is_empty() => {
            Some((0, runtime.nodes[0].alias.clone()))
        }
        GraphRowInitialDriver::Empty { .. } | GraphRowInitialDriver::Edge { .. } => None,
    };
    match node_driver {
        Some((node_index, alias)) => GraphRowChunkSource::AnchorPull {
            alias,
            node_index,
            anchor_slot: runtime.nodes[node_index].slot,
        },
        None => GraphRowChunkSource::RuntimeOnce {
            reason: GraphRowRuntimeOnceReason::NonNodeInitialDriver,
            initial_rows: None,
            goal: GraphRowRuntimeGoal::AllRows,
        },
    }
}

fn graph_row_anchor_monotonicity_proof(
    query: &NormalizedGraphRowQuery,
    source: &GraphRowChunkSource,
) -> (Option<AnchorMonotonicityProof>, GraphRowChunkEligibility) {
    let (anchor_slot, source_order, boundary) = match source {
        GraphRowChunkSource::AnchorPull { anchor_slot, .. } => (
            *anchor_slot,
            GraphRowSourceOrder::NodeIdAsc,
            GraphRowProofBoundary::SourceRow,
        ),
        GraphRowChunkSource::AdjacencyPull { anchor_slot, .. } => (
            *anchor_slot,
            GraphRowSourceOrder::LogicalFromGroupAsc,
            GraphRowProofBoundary::CompletedOwnerGroup,
        ),
        _ => {
            let eligibility = match source {
                GraphRowChunkSource::RuntimeOnce {
                    reason: GraphRowRuntimeOnceReason::Stage,
                    ..
                } => GraphRowChunkEligibility::StageSink,
                GraphRowChunkSource::RuntimeOnce {
                    reason: GraphRowRuntimeOnceReason::Exists,
                    ..
                } => GraphRowChunkEligibility::RuntimeOnce,
                GraphRowChunkSource::RuntimeOnce { .. } => {
                    GraphRowChunkEligibility::NonNodeInitialDriver
                }
                GraphRowChunkSource::EdgePull { .. } => {
                    GraphRowChunkEligibility::EdgeIdOrderNotLogicalPrefix
                }
                GraphRowChunkSource::AnchorPull { .. }
                | GraphRowChunkSource::AdjacencyPull { .. } => unreachable!(),
            };
            return (None, eligibility);
        }
    };
    if !query.bound_order_by.is_empty() {
        return (None, GraphRowChunkEligibility::OrderedQuery);
    }
    let Some(first) = query.binding_schema.slots().first() else {
        return (None, GraphRowChunkEligibility::AnchorNotFirstLogicalSlot);
    };
    let first_ref = crate::graph_row::GraphBindingSlotRef {
        kind: first.kind,
        index: first.index,
    };
    if first.kind != crate::graph_row::GraphBindingSlotKind::Node || first_ref != anchor_slot {
        return (None, GraphRowChunkEligibility::AnchorNotFirstLogicalSlot);
    }
    (
        Some(AnchorMonotonicityProof {
            anchor_slot,
            logical_key_ordinal: 0,
            source_order,
            boundary,
        }),
        GraphRowChunkEligibility::Eligible,
    )
}

fn graph_row_append_successful_trace_sample(
    target: &mut GraphRowExplainTrace,
    first_successful_trace: Option<GraphRowExplainTrace>,
    successful_leaves: usize,
    runtime_once: bool,
) {
    let Some(mut sample) = first_successful_trace else {
        return;
    };
    if successful_leaves > 1 && !runtime_once {
        for node in &mut sample.plan {
            node.detail.push_str("; first_leaf_sample=true");
        }
    }
    target.plan.append(&mut sample.plan);
    target.notes.append(&mut sample.notes);
    target.warnings.append(&mut sample.warnings);
}

struct GraphRowEdgePullAttemptSuccess {
    rows: Vec<crate::graph_row::GraphBindingRow>,
    frontier_peak: usize,
    intermediate_peak: usize,
    paths_enumerated: usize,
    scratch: Option<GraphRowExplainTrace>,
}

struct GraphRowEdgePullAttemptFailure {
    error: EngineError,
    retryable: bool,
}

impl ReadView {
    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_edge_pull_attempt(
        &self,
        query: &NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        edge_index: usize,
        read: GraphRowAnchoredEdgeSourceRead<'_>,
        trace_choice: GraphRowEdgeCandidateSourceChoice,
        source_detail: Option<String>,
        candidate_count: usize,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        unique_followups: &mut GraphRowUniqueFollowups,
        caches: &mut GraphRowExecutionCaches,
        explain_requested: bool,
    ) -> Result<GraphRowEdgePullAttemptSuccess, GraphRowEdgePullAttemptFailure> {
        graph_row_clear_cap_trip();
        let mut attempt = caches.begin_attempt();
        #[cfg(test)]
        if matches!(trace_choice, GraphRowEdgeCandidateSourceChoice::AdjacencyPullChunk) {
            if GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_TRACE_ACTIVE.with(ProductionCell::get) {
                GRAPH_ROW_TEST_ADJACENCY_ATTEMPT_SIZES
                    .with(|sizes| sizes.borrow_mut().push(candidate_count));
            }
            let force_retry = GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_MIN.with(|minimum| {
                minimum
                    .get()
                    .is_some_and(|minimum| candidate_count >= minimum)
                    .then(|| minimum.take())
                    .flatten()
                    .is_some()
            });
            let force_target_retry = GRAPH_ROW_TEST_FORCED_ADJACENCY_RETRY_EDGE.with(|target| {
                let Some(target) = target.get() else {
                    return false;
                };
                candidate_count > 1
                    && matches!(
                        &read,
                        GraphRowAnchoredEdgeSourceRead::OrientedMetadata(metadata)
                            if metadata.iter().any(|edge| edge.meta.id == target)
                    )
            });
            if force_retry || force_target_retry {
                caches.rollback_attempt(&mut attempt);
                return Err(GraphRowEdgePullAttemptFailure {
                    error: EngineError::InvalidOperation(
                        "injected adjacency retryable cap failure".into(),
                    ),
                    retryable: true,
                });
            }
        }
        let mut frontier_peak = 0usize;
        let mut intermediate_peak = 0usize;
        let mut paths_enumerated = 0usize;
        let mut scratch = graph_row_scratch_trace(explain_requested);
        if let (Some(trace), Some(source_detail)) = (scratch.as_mut(), source_detail) {
            trace.record_runtime_edge_source(
                &runtime.edges[edge_index],
                trace_choice,
                source_detail,
                candidate_count,
            );
        }
        let source_override = GraphRowAnchoredEdgeSourceOverride {
            edge_index,
            read,
            consumed: ProductionCell::new(false),
        };
        let result = self.graph_row_execute_runtime_plan_with_caches(
            query,
            runtime,
            physical_plan,
            Some(&source_override),
            None,
            GraphRowRuntimeGoal::AllRows,
            effective_at_epoch,
            policy_cutoffs,
            unique_followups,
            &mut frontier_peak,
            &mut intermediate_peak,
            &mut paths_enumerated,
            scratch.as_mut(),
            caches,
            &mut attempt,
        );
        debug_assert!(
            source_override.consumed.get(),
            "eligible edge-pull override must be consumed by its anchored edge"
        );
        match result {
            Ok(rows) => {
                graph_row_clear_cap_trip();
                caches.commit_attempt(&mut attempt);
                Ok(GraphRowEdgePullAttemptSuccess {
                    rows,
                    frontier_peak,
                    intermediate_peak,
                    paths_enumerated,
                    scratch,
                })
            }
            Err(error) => {
                let retryable = graph_row_take_cap_trip().is_some();
                caches.rollback_attempt(&mut attempt);
                Err(GraphRowEdgePullAttemptFailure { error, retryable })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn graph_row_execute_chunked<'a>(
        &'a self,
        query: &'a NormalizedGraphRowQuery,
        runtime: &GraphRowRuntimePlan,
        physical_plan: &GraphRowPhysicalPlan,
        runtime_once: Option<GraphRowRuntimeOnceExecution>,
        cursor_state: &'a GraphRowCursorState,
        selection_capacity: usize,
        collect_all: bool,
        effective_at_epoch: i64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
        mut explain_trace: Option<&mut GraphRowExplainTrace>,
    ) -> Result<GraphRowProductionOutput, EngineError> {
        let mut source = if let Some(runtime_once) = runtime_once {
            GraphRowChunkSource::RuntimeOnce {
                reason: runtime_once.reason,
                initial_rows: runtime_once.initial_rows,
                goal: runtime_once.goal,
            }
        } else {
            graph_row_chunk_source(query, runtime, physical_plan)
        };
        let (mut proof, mut eligibility) = graph_row_anchor_monotonicity_proof(query, &source);
        let runtime_once_source = matches!(source, GraphRowChunkSource::RuntimeOnce { .. });
        let heap_capacity = if collect_all { 0 } else { selection_capacity };
        let mut summary = GraphRowProductionSummary::new(
            source.description(),
            eligibility,
            heap_capacity,
            query.options.max_intermediate_bindings,
        );
        let mut sink = if collect_all {
            GraphRowProductionSink::CollectAll(GraphRowCollectAllSink::new())
        } else {
            GraphRowProductionSink::Page(GraphRowPageSink::new(
                self,
                query,
                cursor_state,
                selection_capacity,
            ))
        };
        if sink.skips_zero_selection(selection_capacity) {
            if let Some(trace) = explain_trace.as_deref_mut() {
                trace.chunk_eligibility = Some(eligibility);
                trace.chunk_cursor_seek_anchor = None;
                trace.record_plan("ChunkedRowProductionRuntime", summary.detail());
            }
            let GraphRowProductionSink::Page(sink) = sink else {
                unreachable!("only the page sink skips zero selection")
            };
            return Ok(GraphRowProductionOutput::Page(GraphRowPageProduction {
                selected: sink.selected,
                pre_page_needs: sink.pre_page_needs,
                rows_after_filter: 0,
                rows_seen_for_page: 0,
                intermediate_peak: 0,
                frontier_peak: 0,
                paths_enumerated: 0,
                planning_ns_delta: 0,
                followups: Vec::new(),
            }));
        }

        let mut cursor_seek_anchor = if graph_row_test_cursor_seek_disabled() {
            None
        } else {
            graph_row_cursor_seek_anchor(proof, cursor_state)
        };
        let mut prepared_adjacency = None;
        let mut adjacency_group_proof = graph_row_adjacency_group_monotonicity_proof(proof);
        let adjacency_identity = match &source {
            GraphRowChunkSource::AdjacencyPull {
                edge_index,
                edge_name,
                ..
            } => Some((*edge_index, edge_name.clone())),
            _ => None,
        };
        if let Some((edge_index, edge_name)) = adjacency_identity {
            if adjacency_group_proof.is_none() {
                source = GraphRowChunkSource::EdgePull {
                    edge_index,
                    edge_name,
                };
                proof = None;
                eligibility = GraphRowChunkEligibility::EdgeIdOrderNotLogicalPrefix;
                cursor_seek_anchor = None;
                summary = GraphRowProductionSummary::new(
                    source.description(),
                    eligibility,
                    heap_capacity,
                    query.options.max_intermediate_bindings,
                );
                summary.fallback = Some("adjacency_monotonicity_proof_missing");
            } else {
            let edge = &runtime.edges[edge_index];
            let planning_query = NormalizedEdgeQuery {
                label_id: None,
                ids: Vec::new(),
                from_ids: Vec::new(),
                to_ids: Vec::new(),
                endpoint_ids: Vec::new(),
                filter: edge.filter.clone(),
                allow_full_scan: query.options.allow_full_scan,
                page: PageRequest {
                    limit: Some(query.options.max_frontier.saturating_add(1).max(1)),
                    after: None,
                },
                warnings: Vec::new(),
            };
            let mut execution_query = planning_query.clone();
            execution_query.filter = graph_row_delegated_filter_with_valid_at(
                &planning_query,
                effective_at_epoch,
            )?;
            match self.prepare_adjacency_pull_source(
                edge.direction,
                edge.label_filter_ids.as_deref(),
                cursor_seek_anchor,
                execution_query,
                policy_cutoffs.cloned(),
            ) {
                Ok(PreparedAdjacencyPullPreparation::Ready(prepared)) => {
                    #[cfg(test)]
                    let prepared = {
                        let mut prepared = prepared;
                        if let Some(owner) = GRAPH_ROW_TEST_ADJACENCY_POSTING_FAILURE_OWNER
                            .with(ProductionCell::get)
                        {
                            for cursor in &mut prepared.cursors {
                                cursor.fail_posting_for_owner = Some(owner);
                            }
                        }
                        prepared
                    };
                    prepared_adjacency = Some(prepared);
                }
                Ok(PreparedAdjacencyPullPreparation::Empty) => {}
                Ok(PreparedAdjacencyPullPreparation::MutableActiveMemtable) | Err(_) => {
                    source = GraphRowChunkSource::EdgePull {
                        edge_index,
                        edge_name,
                    };
                    proof = None;
                    eligibility = GraphRowChunkEligibility::EdgeIdOrderNotLogicalPrefix;
                    cursor_seek_anchor = None;
                    summary = GraphRowProductionSummary::new(
                        source.description(),
                        eligibility,
                        heap_capacity,
                        query.options.max_intermediate_bindings,
                    );
                    summary.fallback = Some("adjacency_prepare_error");
                    adjacency_group_proof = None;
                }
            }
            }
        }

        let mut caches = GraphRowExecutionCaches::for_source(
            query.options.max_intermediate_bindings,
            &source,
        );
        let mut unique_followups = GraphRowUniqueFollowups::new(Vec::new());
        let mut successful_intermediate_peak = 0usize;
        let mut successful_frontier_peak = 0usize;
        let mut successful_paths_enumerated = 0usize;
        let mut planning_ns_delta = 0u64;
        let mut first_successful_trace = None;
        summary.cursor_seek_anchor = cursor_seek_anchor;
        let mut chunk_size = graph_row_scheduled_chunk_size(
            None,
            selection_capacity,
            proof,
            query.options.max_intermediate_bindings,
        );

        match &mut source {
            GraphRowChunkSource::AnchorPull {
                alias,
                node_index,
                anchor_slot,
            } => {
                let anchor = &runtime.nodes[*node_index];
                let mut planning_query = anchor.query.clone();
                planning_query.page.after = None;
                planning_query.page.limit = Some(
                    GRAPH_ROW_CHUNK_MAX.min(query.options.max_intermediate_bindings),
                );
                let planning_started = query.options.profile.then(std::time::Instant::now);
                let mut planned = self.plan_normalized_node_query(&planning_query)?;
                if let Some(started) = planning_started {
                    planning_ns_delta = started.elapsed().as_nanos() as u64;
                }
                if let Some(trace) = explain_trace.as_deref_mut() {
                    trace.record_node_plan(alias, "executed borrowed graph-row anchor source", &planned);
                }
                unique_followups.extend(std::mem::take(&mut planned.followups));

                let mut after = cursor_seek_anchor.and_then(|anchor| anchor.checked_sub(1));
                if cursor_seek_anchor.is_some() {
                    #[cfg(test)]
                    self.note_graph_row_cursor_anchor_seek();
                }
                let mut driver_control = GraphRowDriverControl::Continue;
                while driver_control == GraphRowDriverControl::Continue {
                    let mut pull_query = anchor.query.clone();
                    pull_query.page.after = after;
                    pull_query.page.limit = Some(chunk_size);
                    summary.note_scheduled_pull(chunk_size);
                    let (page, actual_followups) = self.query_node_page_planned_borrowed(
                        &pull_query,
                        &planned,
                        false,
                        policy_cutoffs,
                    )?;
                    unique_followups.extend(actual_followups);
                    summary.source_rows = summary.source_rows.saturating_add(page.ids.len());
                    let next = page.next_cursor;
                    if !page.ids.is_empty() {
                        let mut stack = vec![page.ids.as_slice()];
                        while let Some(ids) = stack.pop() {
                            #[cfg(test)]
                            self.note_graph_row_retry_input_rows_peak(ids.len());
                            graph_row_clear_cap_trip();
                            let remaining_work = !stack.is_empty() || next.is_some();
                            caches.prepare_leaf(remaining_work);
                            let mut attempt = caches.begin_attempt();
                            let mut attempt_frontier_peak = 0usize;
                            let mut attempt_intermediate_peak = 0usize;
                            let mut attempt_paths_enumerated = 0usize;
                            let mut scratch = graph_row_scratch_trace(explain_trace.is_some());
                            let mut initial_rows = Vec::with_capacity(ids.len());
                            for node_id in ids {
                                let mut row = query.binding_schema.empty_row();
                                row.bind_node(
                                    *anchor_slot,
                                    crate::graph_row::GraphBoundNode::id_only(*node_id),
                                )?;
                                initial_rows.push(row);
                            }
                            let result = self.graph_row_execute_runtime_plan_with_caches(
                                query,
                                runtime,
                                physical_plan,
                                None,
                                Some(initial_rows),
                                GraphRowRuntimeGoal::AllRows,
                                effective_at_epoch,
                                policy_cutoffs,
                                &mut unique_followups,
                                &mut attempt_frontier_peak,
                                &mut attempt_intermediate_peak,
                                &mut attempt_paths_enumerated,
                                scratch.as_mut(),
                                &mut caches,
                                &mut attempt,
                            );
                            match result {
                                Ok(rows) => {
                                    graph_row_clear_cap_trip();
                                    caches.commit_attempt(&mut attempt);
                                    successful_frontier_peak =
                                        successful_frontier_peak.max(attempt_frontier_peak);
                                    successful_intermediate_peak =
                                        successful_intermediate_peak.max(attempt_intermediate_peak);
                                    successful_paths_enumerated = successful_paths_enumerated
                                        .saturating_add(attempt_paths_enumerated);
                                    if first_successful_trace.is_none() {
                                        first_successful_trace = scratch;
                                    }
                                    summary.note_successful_leaf(ids.len(), rows.len());
                                    #[cfg(test)]
                                    {
                                        self.note_graph_row_chunk_executed();
                                        self.note_graph_row_successful_leaf_rows_peak(ids.len());
                                    }
                                    sink.consume(rows)?;
                                    if proof.is_some()
                                        && sink.page_target_met()
                                        && remaining_work
                                        && !graph_row_test_early_exit_disabled()
                                    {
                                        driver_control = GraphRowDriverControl::StopEarly;
                                        break;
                                    }
                                }
                                Err(error) => {
                                    let retryable = graph_row_take_cap_trip().is_some();
                                    caches.rollback_attempt(&mut attempt);
                                    if retryable && ids.len() > 1 {
                                        summary.cap_retries = summary.cap_retries.saturating_add(1);
                                        #[cfg(test)]
                                        self.note_graph_row_chunk_cap_retry();
                                        graph_row_schedule_retry_halves(&mut stack, ids);
                                    } else {
                                        return Err(error);
                                    }
                                }
                            }
                        }
                    }
                    if driver_control == GraphRowDriverControl::StopEarly {
                        summary.early_exit = true;
                        #[cfg(test)]
                        self.note_graph_row_chunk_early_exit();
                        break;
                    }
                    after = next;
                    if after.is_none() {
                        break;
                    }
                    chunk_size = graph_row_scheduled_chunk_size(
                        Some(chunk_size),
                        selection_capacity,
                        proof,
                        query.options.max_intermediate_bindings,
                    );
                }
            }
            GraphRowChunkSource::AdjacencyPull {
                edge_index,
                edge_name,
                ..
            } => {
                let edge = &runtime.edges[*edge_index];
                debug_assert!(matches!(
                    physical_plan.initial_edge_production,
                    Some(GraphRowEdgeAnchorProduction::AdjacencyPull)
                ));
                let adjacency_group_proof = adjacency_group_proof
                    .expect("AdjacencyPull execution requires a validated group-order proof");
                summary.adjacency = Some(GraphRowAdjacencyProductionSummary::default());
                if let Some(mut prepared) = prepared_adjacency.take() {
                    let mut previous_schedule = None;
                    let mut cursor_seek_noted = false;
                    loop {
                    let scheduled = graph_row_adjacency_pull_scheduled_chunk_size(
                        previous_schedule,
                        selection_capacity,
                        query.options.max_intermediate_bindings,
                        query.options.max_frontier,
                    );
                    summary.note_scheduled_pull(scheduled);
                    let page_target_met = sink.page_target_met();
                    let oriented_candidates_needed = if page_target_met {
                        0
                    } else {
                        selection_capacity
                            .checked_sub(match &sink {
                                GraphRowProductionSink::Page(page) => page.selected.len(),
                                GraphRowProductionSink::CollectAll(_) => 0,
                            })
                            .expect("page selection cannot exceed its capacity")
                    };
                    debug_assert!(page_target_met || oriented_candidates_needed > 0);
                    let mut pull = prepared.pull(
                        scheduled,
                        AdjacencyPullBoundaryHint {
                            page_target_met,
                            oriented_candidates_needed,
                        },
                    )?;
                    #[cfg(test)]
                    if GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE_ACTIVE.with(ProductionCell::get) {
                        GRAPH_ROW_TEST_ADJACENCY_PULL_TRACE.with(|trace| {
                            trace.borrow_mut().push(GraphRowTestAdjacencyPullTrace {
                                oriented_metadata: pull.oriented_metadata.len(),
                                physical_scan_units: pull.physical_scan_units,
                                raw_postings_scanned: pull.raw_postings_scanned,
                                pending_completed_through_owner: pull
                                    .pending_completed_through_owner,
                                pending_last_completed_owner: pull.pending_last_completed_owner,
                                has_more: pull.has_more,
                                partial_owner_pull: pull.partial_owner_pull,
                            });
                        });
                    }
                    unique_followups.extend(std::mem::take(&mut pull.followups));

                    if !pull.oriented_metadata.is_empty() {
                        let mut stack = vec![pull.oriented_metadata.as_slice()];
                        while let Some(metadata) = stack.pop() {
                            #[cfg(test)]
                            self.note_graph_row_retry_input_rows_peak(metadata.len());
                            let remaining_work = !stack.is_empty() || pull.has_more;
                            caches.prepare_leaf(remaining_work);
                            let source_detail = explain_trace.is_some().then(|| {
                                format!(
                                    "planned_driver=AdjacencyPull; materialized_source=AdjacencyPullOrientedMetadata{{pulled={}}}; fallback_source=none; skipped_due_to_empty_frontier=false; verified_candidates={}",
                                    metadata.len(),
                                    metadata.len(),
                                )
                            });
                            match self.graph_row_execute_edge_pull_attempt(
                                query,
                                runtime,
                                physical_plan,
                                *edge_index,
                                GraphRowAnchoredEdgeSourceRead::OrientedMetadata(metadata),
                                GraphRowEdgeCandidateSourceChoice::AdjacencyPullChunk,
                                source_detail,
                                metadata.len(),
                                effective_at_epoch,
                                policy_cutoffs,
                                &mut unique_followups,
                                &mut caches,
                                explain_trace.is_some(),
                            ) {
                                Ok(success) => {
                                    successful_frontier_peak =
                                        successful_frontier_peak.max(success.frontier_peak);
                                    successful_intermediate_peak = successful_intermediate_peak
                                        .max(success.intermediate_peak);
                                    successful_paths_enumerated = successful_paths_enumerated
                                        .saturating_add(success.paths_enumerated);
                                    if first_successful_trace.is_none() {
                                        first_successful_trace = success.scratch;
                                    }
                                    summary.note_successful_leaf(
                                        metadata.len(),
                                        success.rows.len(),
                                    );
                                    #[cfg(test)]
                                    {
                                        self.note_graph_row_chunk_executed();
                                        self.note_graph_row_successful_leaf_rows_peak(
                                            metadata.len(),
                                        );
                                    }
                                    sink.consume(success.rows)?;
                                }
                                Err(failure) => {
                                    if failure.retryable && metadata.len() > 1 {
                                        summary.cap_retries =
                                            summary.cap_retries.saturating_add(1);
                                        #[cfg(test)]
                                        self.note_graph_row_chunk_cap_retry();
                                        graph_row_schedule_retry_halves(&mut stack, metadata);
                                    } else {
                                        return Err(failure.error);
                                    }
                                }
                            }
                        }
                    }

                    summary.source_rows = summary
                        .source_rows
                        .saturating_add(pull.oriented_metadata.len());
                    let adjacency = summary
                        .adjacency
                        .as_mut()
                        .expect("adjacency source must retain adjacency summary state");
                    adjacency.completed_owner_groups = adjacency
                        .completed_owner_groups
                        .saturating_add(pull.pending_completed_owner_groups);
                    if let Some(owner) = pull.pending_last_completed_owner {
                        adjacency.last_completed_owner = Some(owner);
                    }
                    adjacency.physical_scan_units = adjacency
                        .physical_scan_units
                        .saturating_add(pull.physical_scan_units);
                    adjacency.cursor_seek_units = adjacency
                        .cursor_seek_units
                        .saturating_add(pull.cursor_seek_units);
                    adjacency.adj_index_entries_scanned = adjacency
                        .adj_index_entries_scanned
                        .saturating_add(pull.adj_index_entries_scanned);
                    adjacency.raw_adjacency_postings_scanned = adjacency
                        .raw_adjacency_postings_scanned
                        .saturating_add(pull.raw_postings_scanned);
                    adjacency.shadowed_or_stale_postings = adjacency
                        .shadowed_or_stale_postings
                        .saturating_add(pull.shadowed_or_stale_postings);
                    adjacency.partial_owner_pulls = adjacency
                        .partial_owner_pulls
                        .saturating_add(usize::from(pull.partial_owner_pull));
                    if cursor_seek_anchor.is_some()
                        && pull.cursor_seek_units > 0
                        && !cursor_seek_noted
                    {
                        #[cfg(test)]
                        self.note_graph_row_cursor_anchor_seek();
                        cursor_seek_noted = true;
                    }

                    let committed_boundary = graph_row_committed_adjacency_owner_boundary(
                        adjacency_group_proof,
                        pull.pending_completed_through_owner,
                    );
                    if let Some(boundary) = committed_boundary {
                        if graph_row_adjacency_pull_should_stop(
                            &sink,
                            adjacency_group_proof,
                            boundary,
                            pull.has_more,
                        ) {
                            summary.early_exit = true;
                            #[cfg(test)]
                            self.note_graph_row_chunk_early_exit();
                            break;
                        }
                    }
                    if !pull.has_more {
                        break;
                    }
                        previous_schedule = Some(scheduled);
                    }
                } else {
                    if let Some(trace) = explain_trace.as_deref_mut() {
                        trace.record_runtime_edge_source(
                            edge,
                            GraphRowEdgeCandidateSourceChoice::EmptyResult,
                            "planned_driver=AdjacencyPull; materialized_source=EmptyResult; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none".to_string(),
                            0,
                        );
                        trace.record_plan(
                            "GraphRowPreparedAdjacencySource",
                            format!("edge={edge_name}; source=EmptyResult"),
                        );
                    }
                }
            }
            GraphRowChunkSource::EdgePull {
                edge_index,
                edge_name,
            } => {
                #[cfg(test)]
                self.note_graph_row_edge_pull_execution();

                let edge = &runtime.edges[*edge_index];
                let scheduled = graph_row_edge_pull_scheduled_chunk_size(
                    edge.direction,
                    query.options.max_intermediate_bindings,
                    query.options.max_frontier,
                );
                let planned_driver = graph_row_source_choice_label(
                    physical_plan
                        .edge_source_choices
                        .get(*edge_index)
                        .and_then(|choice| *choice)
                        .unwrap_or_else(|| {
                            graph_row_deterministic_fallback_edge_source_choice(edge, None)
                        }),
                );

                if !edge.candidate_edge_ids.is_empty() {
                    debug_assert!(
                        edge.candidate_edge_ids
                            .windows(2)
                            .all(|pair| pair[0] < pair[1]),
                        "normalized explicit edge IDs must be sorted and unique"
                    );
                    let slice_count = edge.candidate_edge_ids.len().div_ceil(scheduled);
                    for (slice_index, original_ids) in
                        edge.candidate_edge_ids.chunks(scheduled).enumerate()
                    {
                        summary.note_scheduled_pull(scheduled);
                        summary.source_rows = summary.source_rows.saturating_add(original_ids.len());
                        let later_slice_remains = slice_index + 1 < slice_count;
                        let mut stack = vec![original_ids];
                        while let Some(ids) = stack.pop() {
                            #[cfg(test)]
                            self.note_graph_row_retry_input_rows_peak(ids.len());
                            let remaining_work = !stack.is_empty() || later_slice_remains;
                            caches.prepare_leaf(remaining_work);
                            let source_detail = explain_trace.is_some().then(|| {
                                format!(
                                    "planned_driver={planned_driver}; materialized_source=EdgePullExplicitIds{{slice={}}}; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none",
                                    ids.len()
                                )
                            });
                            match self.graph_row_execute_edge_pull_attempt(
                                query,
                                runtime,
                                physical_plan,
                                *edge_index,
                                GraphRowAnchoredEdgeSourceRead::RawIds(ids),
                                GraphRowEdgeCandidateSourceChoice::EdgePullChunk,
                                source_detail,
                                ids.len(),
                                effective_at_epoch,
                                policy_cutoffs,
                                &mut unique_followups,
                                &mut caches,
                                explain_trace.is_some(),
                            ) {
                                Ok(success) => {
                                    successful_frontier_peak =
                                        successful_frontier_peak.max(success.frontier_peak);
                                    successful_intermediate_peak = successful_intermediate_peak
                                        .max(success.intermediate_peak);
                                    successful_paths_enumerated = successful_paths_enumerated
                                        .saturating_add(success.paths_enumerated);
                                    if first_successful_trace.is_none() {
                                        first_successful_trace = success.scratch;
                                    }
                                    summary.note_successful_leaf(ids.len(), success.rows.len());
                                    #[cfg(test)]
                                    {
                                        self.note_graph_row_chunk_executed();
                                        self.note_graph_row_successful_leaf_rows_peak(ids.len());
                                    }
                                    sink.consume(success.rows)?;
                                }
                                Err(failure) => {
                                    if failure.retryable && ids.len() > 1 {
                                        summary.cap_retries =
                                            summary.cap_retries.saturating_add(1);
                                        #[cfg(test)]
                                        self.note_graph_row_chunk_cap_retry();
                                        graph_row_schedule_retry_halves(&mut stack, ids);
                                    } else {
                                        return Err(failure.error);
                                    }
                                }
                            }
                        }
                    }
                } else if edge.filter.is_always_false()
                    || edge
                        .label_filter_ids
                        .as_ref()
                        .is_some_and(|label_ids| label_ids.is_empty())
                {
                    if let Some(trace) = explain_trace.as_deref_mut() {
                        trace.record_runtime_edge_source(
                            edge,
                            GraphRowEdgeCandidateSourceChoice::EmptyResult,
                            format!(
                                "planned_driver={planned_driver}; materialized_source=EmptyResult; fallback_source=none; skipped_due_to_empty_frontier=false; subset_intersection_source_materialized=none"
                            ),
                            0,
                        );
                    }
                } else {
                    let label_branches: Vec<Option<u32>> = match edge.label_filter_ids.as_deref() {
                        Some(label_ids) => label_ids.iter().copied().map(Some).collect(),
                        None => vec![None],
                    };
                    let branch_count = label_branches.len();
                    for (branch_index, label_id) in label_branches.into_iter().enumerate() {
                        let planning_limit = query.options.max_frontier.saturating_add(1).max(1);
                        let planning_query = NormalizedEdgeQuery {
                            label_id,
                            ids: Vec::new(),
                            from_ids: Vec::new(),
                            to_ids: Vec::new(),
                            endpoint_ids: Vec::new(),
                            filter: edge.filter.clone(),
                            allow_full_scan: query.options.allow_full_scan,
                            page: PageRequest {
                                limit: Some(planning_limit),
                                after: None,
                            },
                            warnings: Vec::new(),
                        };
                        #[cfg(test)]
                        note_prepared_edge_branch_plan();
                        let planning_started =
                            query.options.profile.then(std::time::Instant::now);
                        let mut planned = match self.plan_normalized_edge_query(&planning_query) {
                            Ok(planned) => planned,
                            Err(EngineError::InvalidOperation(_)) => {
                                return Err(EngineError::InvalidOperation(
                                    "graph row required edge pattern requires an anchor or allow_full_scan=true"
                                        .to_string(),
                                ));
                            }
                            Err(error) => return Err(error),
                        };
                        if let Some(started) = planning_started {
                            planning_ns_delta = planning_ns_delta
                                .saturating_add(started.elapsed().as_nanos() as u64);
                        }
                        if let Some(trace) = explain_trace.as_deref_mut() {
                            trace.record_edge_plan(
                                edge_name,
                                "executed prepared graph-row edge anchor source",
                                label_id,
                                &planned,
                            );
                        }
                        unique_followups.extend(std::mem::take(&mut planned.followups));

                        let mut execution_query = planning_query.clone();
                        execution_query.filter = graph_row_delegated_filter_with_valid_at(
                            &planning_query,
                            effective_at_epoch,
                        )?;
                        let (mut prepared, preparation_followups) = self
                            .prepare_edge_metadata_pull_source(
                                &planning_query,
                                &execution_query,
                                planned,
                                policy_cutoffs,
                            )?;
                        unique_followups.extend(preparation_followups);
                        let prepared_mode = prepared.prepared_mode();
                        let fallback = prepared.fallback();
                        let retained_id_units = prepared.retained_id_units();
                        let prepared_mode_label =
                            graph_row_prepared_edge_mode_label(prepared_mode);
                        let fallback_label = graph_row_prepared_edge_fallback_label(fallback);
                        let label_detail = explain_trace.is_some().then(|| {
                            label_id.map_or_else(
                                || "none".to_string(),
                                |label_id| label_id.to_string(),
                            )
                        });
                        if let Some(trace) = explain_trace.as_deref_mut() {
                            let label_detail = label_detail
                                .as_deref()
                                .expect("explain label detail must be available");
                            trace.record_plan(
                                "GraphRowPreparedEdgeSource",
                                format!(
                                    "edge={edge_name}; label_id={label_detail}; prepared_source={prepared_mode_label}; fallback_source={fallback_label}; retained_id_units={retained_id_units}"
                                ),
                            );
                        }

                        loop {
                            summary.note_scheduled_pull(scheduled);
                            #[cfg(test)]
                            self.note_graph_row_delegated_edge_query();
                            #[cfg(test)]
                            if graph_row_test_take_prepared_pull_failure() {
                                return Err(EngineError::InvalidOperation(
                                    "injected graph-row prepared edge pull failure".to_string(),
                                ));
                            }
                            let pull = prepared.pull(scheduled)?;
                            unique_followups.extend(pull.followups);
                            summary.source_rows =
                                summary.source_rows.saturating_add(pull.metadata.len());
                            #[cfg(test)]
                            self.note_graph_row_delegated_verified_candidates(
                                pull.metadata.len(),
                            );
                            if !pull.metadata.is_empty() {
                                let has_more = pull.has_more;
                                let later_branch_remains = branch_index + 1 < branch_count;
                                let mut stack = vec![pull.metadata.as_slice()];
                                while let Some(metadata) = stack.pop() {
                                    #[cfg(test)]
                                    self.note_graph_row_retry_input_rows_peak(metadata.len());
                                    let remaining_work = !stack.is_empty()
                                        || has_more
                                        || later_branch_remains;
                                    caches.prepare_leaf(remaining_work);
                                    let source_detail = label_detail.as_deref().map(|label_detail| {
                                        format!(
                                            "planned_driver={planned_driver}; materialized_source=EdgePullDelegatedMetadata{{label_id={label_detail}; prepared_source={prepared_mode_label}; pulled={}}}; fallback_source={fallback_label}; skipped_due_to_empty_frontier=false; verified_candidates={}",
                                            metadata.len(),
                                            metadata.len()
                                        )
                                    });
                                    match self.graph_row_execute_edge_pull_attempt(
                                        query,
                                        runtime,
                                        physical_plan,
                                        *edge_index,
                                        GraphRowAnchoredEdgeSourceRead::DelegatedMetadata(metadata),
                                        GraphRowEdgeCandidateSourceChoice::EdgePullChunk,
                                        source_detail,
                                        metadata.len(),
                                        effective_at_epoch,
                                        policy_cutoffs,
                                        &mut unique_followups,
                                        &mut caches,
                                        explain_trace.is_some(),
                                    ) {
                                        Ok(success) => {
                                            successful_frontier_peak = successful_frontier_peak
                                                .max(success.frontier_peak);
                                            successful_intermediate_peak =
                                                successful_intermediate_peak
                                                    .max(success.intermediate_peak);
                                            successful_paths_enumerated =
                                                successful_paths_enumerated.saturating_add(
                                                    success.paths_enumerated,
                                                );
                                            if first_successful_trace.is_none() {
                                                first_successful_trace = success.scratch;
                                            }
                                            summary.note_successful_leaf(
                                                metadata.len(),
                                                success.rows.len(),
                                            );
                                            #[cfg(test)]
                                            {
                                                self.note_graph_row_chunk_executed();
                                                self.note_graph_row_successful_leaf_rows_peak(
                                                    metadata.len(),
                                                );
                                            }
                                            sink.consume(success.rows)?;
                                        }
                                        Err(failure) => {
                                            if failure.retryable && metadata.len() > 1 {
                                                summary.cap_retries =
                                                    summary.cap_retries.saturating_add(1);
                                                #[cfg(test)]
                                                self.note_graph_row_chunk_cap_retry();
                                                graph_row_schedule_retry_halves(
                                                    &mut stack,
                                                    metadata,
                                                );
                                            } else {
                                                return Err(failure.error);
                                            }
                                        }
                                    }
                                }
                            }
                            if !pull.has_more {
                                break;
                            }
                        }
                        drop(prepared);
                    }
                }
            }
            GraphRowChunkSource::RuntimeOnce {
                initial_rows,
                goal,
                ..
            } => {
                let source_row_count = initial_rows.as_ref().map_or(0, Vec::len);
                summary.source_rows = source_row_count;
                graph_row_clear_cap_trip();
                let mut attempt = caches.begin_attempt();
                let mut attempt_frontier_peak = 0usize;
                let mut attempt_intermediate_peak = 0usize;
                let mut attempt_paths_enumerated = 0usize;
                let mut scratch = graph_row_scratch_trace(explain_trace.is_some());
                let result = self.graph_row_execute_runtime_plan_with_caches(
                    query,
                    runtime,
                    physical_plan,
                    None,
                    initial_rows.take(),
                    *goal,
                    effective_at_epoch,
                    policy_cutoffs,
                    &mut unique_followups,
                    &mut attempt_frontier_peak,
                    &mut attempt_intermediate_peak,
                    &mut attempt_paths_enumerated,
                    scratch.as_mut(),
                    &mut caches,
                    &mut attempt,
                );
                match result {
                    Ok(rows) => {
                        graph_row_clear_cap_trip();
                        caches.commit_attempt(&mut attempt);
                        successful_frontier_peak = attempt_frontier_peak;
                        successful_intermediate_peak = attempt_intermediate_peak;
                        successful_paths_enumerated = attempt_paths_enumerated;
                        first_successful_trace = scratch;
                        summary.note_successful_leaf(source_row_count, rows.len());
                        #[cfg(test)]
                        {
                            self.note_graph_row_chunk_executed();
                            self.note_graph_row_successful_leaf_rows_peak(source_row_count);
                        }
                        sink.consume(rows)?;
                    }
                    Err(error) => {
                        let _ = graph_row_take_cap_trip();
                        caches.rollback_attempt(&mut attempt);
                        return Err(error);
                    }
                }
            }
        }

        summary.result_cache_units = caches.used_units;
        summary.cache_no_admit = caches.no_admit;
        summary.optional_cache_hits = caches.optional_hits;
        summary.vlp_cross_chunk_cache_hits = caches.vlp_hits;
        if let Some(trace) = explain_trace {
            trace.chunk_eligibility = Some(eligibility);
            trace.chunk_cursor_seek_anchor = cursor_seek_anchor;
            graph_row_append_successful_trace_sample(
                trace,
                first_successful_trace,
                summary.successful_leaves,
                runtime_once_source,
            );
            trace.record_plan("ChunkedRowProductionRuntime", summary.detail());
        }
        let mut followups = Vec::new();
        unique_followups.drain_into(&mut followups);
        match sink {
            GraphRowProductionSink::Page(sink) => {
                Ok(GraphRowProductionOutput::Page(GraphRowPageProduction {
                    selected: sink.selected,
                    pre_page_needs: sink.pre_page_needs,
                    rows_after_filter: sink.rows_after_filter,
                    rows_seen_for_page: sink.rows_seen_for_page,
                    intermediate_peak: successful_intermediate_peak,
                    frontier_peak: successful_frontier_peak,
                    paths_enumerated: successful_paths_enumerated,
                    planning_ns_delta,
                    followups,
                }))
            }
            GraphRowProductionSink::CollectAll(sink) => Ok(
                GraphRowProductionOutput::CollectAll(GraphRowCollectAllProduction {
                    rows: sink.rows,
                    intermediate_peak: successful_intermediate_peak,
                    frontier_peak: successful_frontier_peak,
                    paths_enumerated: successful_paths_enumerated,
                    followups,
                }),
            ),
        }
    }
}

fn graph_row_annotate_cursor_seek(
    trace: &mut GraphRowExplainTrace,
    eligibility: GraphRowChunkEligibility,
) {
    if let Some(row_op) = trace.row_ops.iter_mut().find(|row_op| row_op.kind == "CursorSeek") {
        row_op.detail.push_str(if trace.chunk_cursor_seek_anchor.is_some() {
            "; anchor_seek=applied"
        } else if eligibility.is_eligible() {
            "; anchor_seek=none"
        } else {
            "; anchor_seek=ineligible"
        });
    }
}

#[cfg(test)]
mod graph_row_production_tests {
    use super::*;

    fn edge_pull_routing_fixture() -> (
        NormalizedGraphRowQuery,
        GraphRowRuntimePlan,
        GraphRowPhysicalPlan,
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let source = engine
            .upsert_node("EdgePullRoute", "source", UpsertNodeOptions::default())
            .unwrap();
        let target = engine
            .upsert_node("EdgePullRoute", "target", UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_edge(
                source,
                target,
                "EDGE_PULL_ROUTE",
                UpsertEdgeOptions::default(),
            )
            .unwrap();
        let query = GraphRowQuery {
            nodes: vec![
                GraphNodePattern {
                    alias: "source".to_string(),
                    label_filter: None,
                    ids: Vec::new(),
                    keys: Vec::new(),
                    filter: None,
                },
                GraphNodePattern {
                    alias: "target".to_string(),
                    label_filter: None,
                    ids: Vec::new(),
                    keys: Vec::new(),
                    filter: None,
                },
            ],
            pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
                alias: Some("edge".to_string()),
                from_alias: "source".to_string(),
                to_alias: "target".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["EDGE_PULL_ROUTE".to_string()],
                filter: None,
            })],
            where_: None,
            return_items: None,
            order_by: Vec::new(),
            page: GraphPageRequest {
                skip: 0,
                limit: 10,
                cursor: None,
            },
            at_epoch: None,
            params: BTreeMap::new(),
            output: GraphOutputOptions::default(),
            options: GraphQueryOptions {
                allow_full_scan: false,
                ..GraphQueryOptions::default()
            },
        };
        let normalized = normalize_graph_row_query(&query).unwrap();
        let view = engine.published_state().view.clone();
        let runtime = view.normalize_graph_row_runtime_plan(&normalized).unwrap();
        let physical = view
            .plan_graph_row_physical(&normalized, &runtime, false, true)
            .unwrap();
        assert!(matches!(
            physical.initial_driver,
            GraphRowInitialDriver::Edge { edge_index: 0, .. }
        ));
        (normalized, runtime, physical)
    }

    #[test]
    fn edge_pull_routing_positive_activates_public_edge_pull_source() {
        let (query, runtime, mut physical) = edge_pull_routing_fixture();
        assert!(graph_row_edge_pull_route_is_eligible(
            &query, &runtime, &physical
        ));
        let source = graph_row_chunk_source(&query, &runtime, &physical);
        assert_eq!(source.description(), "EdgePull{edge=alias:edge}");
        assert!(matches!(source, GraphRowChunkSource::EdgePull { edge_index: 0, .. }));

        let GraphRowInitialDriver::Edge { edge_name, .. } = &mut physical.initial_driver else {
            unreachable!()
        };
        *edge_name = "physical-edge-name".to_string();
        physical.segments[0].initial_driver = physical.initial_driver.clone();
        let source = graph_row_chunk_source(&query, &runtime, &physical);
        assert_eq!(
            source.description(),
            "EdgePull{edge=physical-edge-name}",
            "the production summary must preserve the physical-plan driver name"
        );
    }

    #[test]
    fn edge_pull_routing_first_step_initial_input_and_missing_segment_are_ineligible() {
        let (mut query, mut runtime, mut physical) = edge_pull_routing_fixture();
        runtime.steps.insert(
            0,
            GraphRowRuntimeStep::FixedPath(GraphRowRuntimeFixedPath {
                alias: "prefix".to_string(),
                path_slot: crate::graph_row::GraphBindingSlotRef {
                    kind: crate::graph_row::GraphBindingSlotKind::Path,
                    index: 0,
                },
                node_slots: Vec::new(),
                edge_slots: Vec::new(),
            }),
        );
        assert!(physical.segments[0].barriers_before.is_empty());
        assert!(!graph_row_edge_pull_route_is_eligible(
            &query, &runtime, &physical
        ));
        runtime.steps.remove(0);

        let first_slot = &query.binding_schema.slots()[0];
        query
            .initial_bound_slots
            .push(crate::graph_row::GraphBindingSlotRef {
                kind: first_slot.kind,
                index: first_slot.index,
            });
        assert!(!graph_row_edge_pull_route_is_eligible(
            &query, &runtime, &physical
        ));
        query.initial_bound_slots.clear();

        physical.segments.clear();
        assert!(!graph_row_edge_pull_route_is_eligible(
            &query, &runtime, &physical
        ));
        physical.initial_driver = GraphRowInitialDriver::Empty {
            reason: "control".to_string(),
        };
        assert!(!graph_row_edge_pull_route_is_eligible(
            &query, &runtime, &physical
        ));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn edge_pull_routing_malformed_current_planner_invariants_panic() {
        let mut malformed = Vec::<Box<dyn FnOnce()>>::new();

        malformed.push(Box::new(|| {
            let (query, runtime, mut physical) = edge_pull_routing_fixture();
            let mut displaced = physical.segments[0].clone();
            displaced.segment_index = 1;
            physical.segments.insert(0, displaced);
            let _ = graph_row_edge_pull_route_is_eligible(&query, &runtime, &physical);
        }));
        malformed.push(Box::new(|| {
            let (query, runtime, mut physical) = edge_pull_routing_fixture();
            physical.segments[0]
                .barriers_before
                .push(GraphRowPlanBarrier {
                    kind: GraphRowPlanBarrierKind::Optional,
                    piece_index: 0,
                });
            let _ = graph_row_edge_pull_route_is_eligible(&query, &runtime, &physical);
        }));
        malformed.push(Box::new(|| {
            let (query, runtime, mut physical) = edge_pull_routing_fixture();
            physical.segments[0].initial_driver = GraphRowInitialDriver::Edge {
                edge_index: 1,
                edge_name: "wrong".to_string(),
            };
            let _ = graph_row_edge_pull_route_is_eligible(&query, &runtime, &physical);
        }));
        malformed.push(Box::new(|| {
            let (query, runtime, mut physical) = edge_pull_routing_fixture();
            physical.segments[0].edge_order[0] = 1;
            let _ = graph_row_edge_pull_route_is_eligible(&query, &runtime, &physical);
        }));

        for malformed_case in malformed {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(malformed_case)).is_err()
            );
        }
    }

    #[test]
    fn graph_row_chunk_cursor_seek_boundary_is_inclusive_and_zero_safe() {
        let proof = Some(AnchorMonotonicityProof {
            anchor_slot: crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::Node,
                index: 0,
            },
            logical_key_ordinal: 0,
            source_order: GraphRowSourceOrder::NodeIdAsc,
            boundary: GraphRowProofBoundary::SourceRow,
        });
        let cursor_state = |id| GraphRowCursorState {
            decoded: Some(GraphRowCursorPayload {
                effective_at_epoch: 1,
                original_skip: 0,
                page_sequence: 1,
                rows_emitted_after_skip: 1,
                query_fingerprint: 0,
                order_fingerprint: 0,
                output_fingerprint: 0,
                params_fingerprint: 0,
                last_sort_key: Vec::new(),
                last_logical_row_key: vec![crate::graph_row::GraphSortAtom::Node(id)],
            }),
            effective_at_epoch: 1,
            original_skip: 0,
            rows_emitted_after_skip: 1,
        };

        let positive = graph_row_cursor_seek_anchor(proof, &cursor_state(42));
        assert_eq!(positive, Some(42));
        assert_eq!(positive.and_then(|id| id.checked_sub(1)), Some(41));
        let zero = graph_row_cursor_seek_anchor(proof, &cursor_state(0));
        assert_eq!(zero, Some(0));
        assert_eq!(zero.and_then(|id| id.checked_sub(1)), None);

        let mut defensive = cursor_state(42);
        defensive.decoded.as_mut().unwrap().last_logical_row_key[0] =
            crate::graph_row::GraphSortAtom::Edge(42);
        assert_eq!(graph_row_cursor_seek_anchor(proof, &defensive), None);
    }

    fn rows(count: usize) -> GraphRowCachedRows {
        let schema = crate::graph_row::GraphBindingSchema::new();
        ProductionArc::new(
            (0..count)
                .map(|_| schema.empty_row())
                .collect(),
        )
    }

    fn vlp(paths: usize) -> GraphRowCachedVlpResult {
        ProductionArc::new(GraphRowVlpSearchResult {
            seed_count: 1,
            paths: (0..paths)
                .map(|id| GraphPath {
                    nodes: vec![id as u64],
                    edges: Vec::new(),
                })
                .collect(),
        })
    }

    #[test]
    fn graph_row_cache_exact_boundary_negative_and_no_admit_are_deterministic() {
        let mut caches = GraphRowExecutionCaches::new(3);
        let mut attempt = caches.begin_attempt();
        assert!(caches.admit_uncorrelated_optional(Box::new([0]), rows(0), &mut attempt));
        assert!(caches.admit_uncorrelated_optional(Box::new([1]), rows(2), &mut attempt));
        assert!(!caches.admit_uncorrelated_optional(Box::new([2]), rows(1), &mut attempt));
        assert_eq!((caches.used_units, caches.peak_units, caches.no_admit), (3, 3, 1));
        assert!(caches.uncorrelated_optional_results.contains_key([0].as_slice()));
        assert!(caches.uncorrelated_optional_results.contains_key([1].as_slice()));
        assert!(!caches.uncorrelated_optional_results.contains_key([2].as_slice()));
    }

    #[test]
    fn graph_row_cache_budget_zero_never_admits_or_evicts() {
        let mut caches = GraphRowExecutionCaches::new(0);
        let mut attempt = caches.begin_attempt();
        assert!(!caches.admit_uncorrelated_optional(Box::new([0]), rows(0), &mut attempt));
        assert!(!caches.admit_vlp(
            GraphRowVlpResultCacheKey {
                piece_path: Box::new([1]),
                search_key: GraphRowVlpSearchKey { bound_from: None, bound_to: None },
            },
            vlp(0),
            &mut attempt,
        ));
        assert_eq!((caches.used_units, caches.peak_units, caches.no_admit), (0, 0, 2));
        assert!(attempt.is_empty());
    }

    #[test]
    fn graph_row_cache_source_policy_admits_anchor_and_edge_pull_only() {
        let anchor = GraphRowChunkSource::AnchorPull {
            alias: "source".to_string(),
            node_index: 0,
            anchor_slot: crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::Node,
                index: 0,
            },
        };
        let mut anchor_caches = GraphRowExecutionCaches::for_source(4, &anchor);
        let mut anchor_attempt = anchor_caches.begin_attempt();
        assert!(anchor_caches.admit_uncorrelated_optional(
            Box::new([0]),
            rows(1),
            &mut anchor_attempt,
        ));
        assert_eq!(
            (anchor_caches.used_units, anchor_caches.no_admit),
            (1, 0)
        );
        anchor_caches.commit_attempt(&mut anchor_attempt);
        anchor_caches.prepare_leaf(false);
        assert_eq!(
            anchor_caches.result_cache_mode,
            GraphRowResultCacheMode::ReadOnly
        );
        assert!(anchor_caches.can_read_global_results());
        let mut read_only_attempt = anchor_caches.begin_attempt();
        assert!(!anchor_caches.admit_uncorrelated_optional(
            Box::new([1]),
            rows(1),
            &mut read_only_attempt,
        ));
        assert_eq!(
            (anchor_caches.used_units, anchor_caches.no_admit),
            (1, 0),
            "the final leaf must retain reads without counting policy-disabled admission"
        );
        assert!(read_only_attempt.is_empty());

        let mut single_leaf = GraphRowExecutionCaches::for_source(4, &anchor);
        single_leaf.prepare_leaf(false);
        assert_eq!(
            single_leaf.result_cache_mode,
            GraphRowResultCacheMode::Bypass
        );
        assert!(!single_leaf.can_read_global_results());
        let mut single_leaf_attempt = single_leaf.begin_attempt();
        assert!(!single_leaf.admit_uncorrelated_optional(
            Box::new([0]),
            rows(0),
            &mut single_leaf_attempt,
        ));
        assert_eq!((single_leaf.used_units, single_leaf.no_admit), (0, 0));
        assert!(single_leaf_attempt.is_empty());

        let edge_pull = GraphRowChunkSource::EdgePull {
            edge_index: 0,
            edge_name: "alias:edge".to_string(),
        };
        let mut edge_caches = GraphRowExecutionCaches::for_source(4, &edge_pull);
        let mut edge_attempt = edge_caches.begin_attempt();
        assert!(edge_caches.admit_uncorrelated_optional(
            Box::new([0]),
            rows(1),
            &mut edge_attempt,
        ));
        edge_caches.commit_attempt(&mut edge_attempt);
        assert_eq!((edge_caches.used_units, edge_caches.no_admit), (1, 0));
        edge_caches.prepare_leaf(false);
        assert_eq!(
            edge_caches.result_cache_mode,
            GraphRowResultCacheMode::ReadOnly
        );
        assert!(edge_caches.can_read_global_results());

        let mut single_edge_leaf = GraphRowExecutionCaches::for_source(4, &edge_pull);
        single_edge_leaf.prepare_leaf(false);
        assert_eq!(
            single_edge_leaf.result_cache_mode,
            GraphRowResultCacheMode::Bypass
        );
        assert!(!single_edge_leaf.can_read_global_results());

        let mut runtime_once = GraphRowExecutionCaches::for_source(
            4,
            &GraphRowChunkSource::RuntimeOnce {
                reason: GraphRowRuntimeOnceReason::NonNodeInitialDriver,
                initial_rows: None,
                goal: GraphRowRuntimeGoal::AllRows,
            },
        );
        let mut runtime_attempt = runtime_once.begin_attempt();
        assert!(!runtime_once.admit_uncorrelated_optional(
            Box::new([0]),
            rows(1),
            &mut runtime_attempt,
        ));
        assert!(!runtime_once.admit_vlp(
            GraphRowVlpResultCacheKey {
                piece_path: Box::new([1]),
                search_key: GraphRowVlpSearchKey {
                    bound_from: Some(1),
                    bound_to: None,
                },
            },
            vlp(1),
            &mut runtime_attempt,
        ));
        assert_eq!(
            (
                runtime_once.used_units,
                runtime_once.peak_units,
                runtime_once.no_admit,
                runtime_once.optional_hits,
                runtime_once.vlp_hits,
            ),
            (0, 0, 0, 0, 0)
        );
        assert!(runtime_attempt.is_empty());
    }

    #[test]
    fn graph_row_chunk_summary_exact_zero_one_multi_retry_runtime_once_matrix() {
        let anchor_zero = GraphRowProductionSummary::new(
            "AnchorPull{alias=n}".to_string(),
            GraphRowChunkEligibility::Eligible,
            0,
            8,
        );
        assert_eq!(
            anchor_zero.detail(),
            "source=AnchorPull{alias=n}; source_pulls=0; successful_leaves=0; \
             scheduled_sizes=[]; leaf_size_min=0; leaf_size_max=0; source_rows=0; \
             produced_rows=0; early_exit_eligible=true; eligibility=eligible; \
             early_exit=false; cursor_seek=none; heap_capacity=0; cap_retries=0; \
             result_cache_units=0/8; cache_no_admit=0; optional_cache_hits=0; \
             vlp_cross_chunk_cache_hits=0"
        );

        let edge_zero = GraphRowProductionSummary::new(
            "EdgePull{edge=alias:edge}".to_string(),
            GraphRowChunkEligibility::EdgeIdOrderNotLogicalPrefix,
            0,
            8,
        );
        assert_eq!(
            edge_zero.detail(),
            "source=EdgePull{edge=alias:edge}; source_pulls=0; successful_leaves=0; \
             scheduled_sizes=[]; leaf_size_min=0; leaf_size_max=0; source_rows=0; \
             produced_rows=0; early_exit_eligible=false; \
             eligibility=edge_id_order_not_logical_prefix; early_exit=false; \
             cursor_seek=none; heap_capacity=0; cap_retries=0; result_cache_units=0/8; \
             cache_no_admit=0; optional_cache_hits=0; vlp_cross_chunk_cache_hits=0"
        );

        let mut one = GraphRowProductionSummary::new(
            "AnchorPull{alias=n}".to_string(),
            GraphRowChunkEligibility::OrderedQuery,
            3,
            8,
        );
        one.note_scheduled_pull(2);
        one.source_rows = 2;
        one.note_successful_leaf(2, 4);
        assert_eq!(
            one.detail(),
            "source=AnchorPull{alias=n}; source_pulls=1; successful_leaves=1; \
             scheduled_sizes=2..2; leaf_size_min=2; leaf_size_max=2; source_rows=2; \
             produced_rows=4; early_exit_eligible=false; eligibility=ordered_query; \
             early_exit=false; cursor_seek=none; heap_capacity=3; cap_retries=0; \
             result_cache_units=0/8; cache_no_admit=0; optional_cache_hits=0; \
             vlp_cross_chunk_cache_hits=0"
        );

        let mut multi_retry = GraphRowProductionSummary::new(
            "AnchorPull{alias=n}".to_string(),
            GraphRowChunkEligibility::AnchorNotFirstLogicalSlot,
            5,
            8,
        );
        multi_retry.note_scheduled_pull(4);
        multi_retry.note_scheduled_pull(8);
        multi_retry.source_rows = 7;
        multi_retry.note_successful_leaf(2, 6);
        multi_retry.note_successful_leaf(2, 7);
        multi_retry.cap_retries = 1;
        multi_retry.result_cache_units = 3;
        multi_retry.cache_no_admit = 2;
        multi_retry.optional_cache_hits = 1;
        assert_eq!(
            multi_retry.detail(),
            "source=AnchorPull{alias=n}; source_pulls=2; successful_leaves=2; \
             scheduled_sizes=4..8; leaf_size_min=2; leaf_size_max=2; source_rows=7; \
             produced_rows=13; early_exit_eligible=false; \
             eligibility=anchor_not_first_logical_slot; early_exit=false; \
             cursor_seek=none; heap_capacity=5; cap_retries=1; result_cache_units=3/8; \
             cache_no_admit=2; optional_cache_hits=1; vlp_cross_chunk_cache_hits=0"
        );

        let mut runtime_once = GraphRowProductionSummary::new(
            "RuntimeOnce{reason=NonNodeInitialDriver}".to_string(),
            GraphRowChunkEligibility::NonNodeInitialDriver,
            4,
            8,
        );
        runtime_once.note_successful_leaf(0, 3);
        assert_eq!(
            runtime_once.detail(),
            "source=RuntimeOnce{reason=NonNodeInitialDriver}; source_pulls=0; \
             successful_leaves=1; scheduled_sizes=[]; leaf_size_min=0; leaf_size_max=0; \
             source_rows=0; produced_rows=3; early_exit_eligible=false; \
             eligibility=non_node_initial_driver; early_exit=false; cursor_seek=none; \
             heap_capacity=4; cap_retries=0; result_cache_units=0/8; cache_no_admit=0; \
             optional_cache_hits=0; vlp_cross_chunk_cache_hits=0"
        );
    }

    #[test]
    fn adjacency_group_stop_proof_requires_exact_order_and_boundary() {
        let anchor_slot = crate::graph_row::GraphBindingSlotRef {
            kind: crate::graph_row::GraphBindingSlotKind::Node,
            index: 0,
        };
        let exact = Some(AnchorMonotonicityProof {
            anchor_slot,
            logical_key_ordinal: 0,
            source_order: GraphRowSourceOrder::LogicalFromGroupAsc,
            boundary: GraphRowProofBoundary::CompletedOwnerGroup,
        });
        assert_eq!(
            graph_row_adjacency_group_monotonicity_proof(exact),
            Some(AdjacencyGroupMonotonicityProof {
                anchor_slot,
                logical_key_ordinal: 0,
            })
        );

        for invalid in [
            None,
            Some(AnchorMonotonicityProof {
                anchor_slot,
                logical_key_ordinal: 0,
                source_order: GraphRowSourceOrder::NodeIdAsc,
                boundary: GraphRowProofBoundary::CompletedOwnerGroup,
            }),
            Some(AnchorMonotonicityProof {
                anchor_slot,
                logical_key_ordinal: 0,
                source_order: GraphRowSourceOrder::LogicalFromGroupAsc,
                boundary: GraphRowProofBoundary::SourceRow,
            }),
        ] {
            assert_eq!(graph_row_adjacency_group_monotonicity_proof(invalid), None);
        }

        let proof = graph_row_adjacency_group_monotonicity_proof(exact).unwrap();
        assert_eq!(
            graph_row_committed_adjacency_owner_boundary(proof, None),
            None
        );
        assert_eq!(
            graph_row_committed_adjacency_owner_boundary(proof, Some(42)),
            Some(CommittedAdjacencyOwnerBoundary { owner_id: 42 })
        );
    }

    #[test]
    fn graph_row_chunk_retry_schedules_left_before_right_without_recombination() {
        let mut stack = Vec::new();
        graph_row_schedule_retry_halves(&mut stack, &[1, 2, 3, 4, 5]);
        assert_eq!(stack.pop(), Some(&[1, 2][..]));
        assert_eq!(stack.pop(), Some(&[3, 4, 5][..]));
        assert!(stack.is_empty());
    }

    #[test]
    fn graph_row_chunk_early_exit_scheduling_and_fixed_override_are_locked() {
        let proof = Some(AnchorMonotonicityProof {
            anchor_slot: crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::Node,
                index: 0,
            },
            logical_key_ordinal: 0,
            source_order: GraphRowSourceOrder::NodeIdAsc,
            boundary: GraphRowProofBoundary::SourceRow,
        });
        assert_eq!(graph_row_scheduled_chunk_size(None, 11, proof, 8_192), 11);
        assert_eq!(graph_row_scheduled_chunk_size(Some(11), 11, proof, 8_192), 22);
        assert_eq!(graph_row_scheduled_chunk_size(Some(3_000), 11, proof, 8_192), 4_096);
        assert_eq!(graph_row_scheduled_chunk_size(None, 11, None, 8_192), 4_096);
        assert_eq!(graph_row_scheduled_chunk_size(None, 11, proof, 7), 7);
        with_graph_row_test_chunk_override(3, || {
            assert_eq!(graph_row_scheduled_chunk_size(None, 11, proof, 8_192), 3);
            assert_eq!(graph_row_scheduled_chunk_size(Some(3), 11, proof, 8_192), 3);
        });
        assert_eq!(
            graph_row_edge_pull_scheduled_chunk_size(Direction::Outgoing, 100, 0),
            1
        );
        assert_eq!(
            graph_row_edge_pull_scheduled_chunk_size(Direction::Outgoing, 100, 7),
            7
        );
        assert_eq!(
            graph_row_edge_pull_scheduled_chunk_size(Direction::Both, 100, 8),
            4
        );
        assert_eq!(
            with_graph_row_test_chunk_override(usize::MAX, || {
                graph_row_edge_pull_scheduled_chunk_size(Direction::Both, 100, 8)
            }),
            4
        );
        assert_eq!(
            with_graph_row_test_chunk_override(0, || {
                graph_row_edge_pull_scheduled_chunk_size(Direction::Outgoing, 100, 8)
            }),
            1
        );
    }

    #[test]
    fn graph_row_chunk_early_exit_summary_and_test_oracle_restore_are_truthful() {
        let mut summary = GraphRowProductionSummary::new(
            "AnchorPull{alias=n}".to_string(),
            GraphRowChunkEligibility::Eligible,
            3,
            8,
        );
        summary.note_scheduled_pull(3);
        summary.source_rows = 3;
        summary.note_successful_leaf(3, 5);
        summary.early_exit = true;
        assert!(summary.detail().contains(
            "source_pulls=1; successful_leaves=1; scheduled_sizes=3..3"
        ));
        assert!(summary.detail().contains("early_exit=true; cursor_seek=none"));

        assert!(!graph_row_test_early_exit_disabled());
        with_graph_row_test_early_exit_disabled(|| {
            assert!(graph_row_test_early_exit_disabled());
        });
        assert!(!graph_row_test_early_exit_disabled());
    }

    #[test]
    fn graph_row_cache_transaction_commit_and_rollback_restore_exact_units() {
        let mut caches = GraphRowExecutionCaches::new(10);
        let mut committed = caches.begin_attempt();
        assert!(caches.admit_uncorrelated_optional(Box::new([0]), rows(2), &mut committed));
        caches.commit_attempt(&mut committed);

        let mut rolled_back = caches.begin_attempt();
        let correlated_key = GraphRowCorrelatedOptionalCacheKey {
            piece_path: Box::new([1, 0]),
            dependency_key: Vec::new(),
        };
        assert!(caches.admit_correlated_optional(correlated_key.clone(), rows(0), &mut rolled_back));
        let vlp_key = GraphRowVlpResultCacheKey {
            piece_path: Box::new([2]),
            search_key: GraphRowVlpSearchKey { bound_from: Some(1), bound_to: None },
        };
        assert!(caches.admit_vlp(vlp_key.clone(), vlp(3), &mut rolled_back));
        assert_eq!(caches.used_units, 6);
        caches.rollback_attempt(&mut rolled_back);
        assert_eq!(caches.used_units, 2);
        assert!(caches.uncorrelated_optional_results.contains_key([0].as_slice()));
        assert!(!caches.correlated_optional_results.contains_key(&correlated_key));
        assert!(!caches.vlp_results.contains_key(&vlp_key));
    }

    #[test]
    fn graph_row_cache_transaction_rollback_preserves_physical_plans() {
        let mut caches = GraphRowExecutionCaches::new(1);
        caches.optional_physical_plans.insert(
            Box::new([4, 2]),
            ProductionRc::new(GraphRowPhysicalPlan {
                initial_driver: GraphRowInitialDriver::Empty { reason: "test".into() },
                initial_edge_production: None,
                edge_order: Vec::new(),
                segments: Vec::new(),
                edge_source_choices: Vec::new(),
                alternatives: Vec::new(),
                notes: Vec::new(),
            }),
        );
        let mut attempt = caches.begin_attempt();
        assert!(caches.admit_uncorrelated_optional(Box::new([0]), rows(0), &mut attempt));
        caches.rollback_attempt(&mut attempt);
        assert_eq!(caches.optional_physical_plan_count(), 1);
    }

    #[test]
    fn graph_row_node_verification_cache_is_bounded_fifo_and_hits_do_not_refresh() {
        let mut caches = GraphRowExecutionCaches::new(0);
        caches.node_verification_enabled = true;
        let slot = crate::graph_row::GraphBindingSlotRef {
            kind: crate::graph_row::GraphBindingSlotKind::Node,
            index: 0,
        };
        for node_id in 0..GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP as u64 {
            caches.node_verification_insert(slot, node_id, node_id % 2 == 0);
        }
        assert_eq!(
            caches.node_verification_cache.len(),
            GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP
        );
        assert_eq!(caches.node_verification_get(slot, 0), Some(true));
        caches.node_verification_insert(
            slot,
            GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP as u64,
            true,
        );
        assert_eq!(caches.node_verification_get(slot, 0), None);
        assert_eq!(caches.node_verification_get(slot, 1), Some(false));
        assert_eq!(
            caches.node_verification_get(
                slot,
                GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP as u64
            ),
            Some(true)
        );
        assert_eq!(
            caches.node_verification_cache.len(),
            GRAPH_ROW_NODE_VERIFICATION_CACHE_CAP
        );
    }

    #[test]
    fn graph_row_fixed_edge_workspace_clear_retains_reusable_capacity() {
        let mut workspace = GraphRowFixedEdgeWorkspace::default();
        workspace.oriented_edges.reserve(32);
        workspace.from_candidates.extend(0..32);
        workspace.to_candidates.extend(32..64);
        workspace.verification_misses.extend(64..96);
        workspace.verified_ids.extend(96..128);
        workspace.from_verified.extend(0..16);
        workspace.to_verified.extend(16..32);
        let capacity = workspace.retained_capacity();

        workspace.clear();

        assert!(workspace.oriented_edges.is_empty());
        assert!(workspace.from_candidates.is_empty());
        assert!(workspace.to_candidates.is_empty());
        assert!(workspace.verification_misses.is_empty());
        assert!(workspace.verified_ids.is_empty());
        assert!(workspace.from_verified.is_empty());
        assert!(workspace.to_verified.is_empty());
        assert_eq!(workspace.retained_capacity(), capacity);
    }

    #[test]
    fn graph_row_followup_accumulator_retains_unique_first_seen_order() {
        let first = SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id: 1, error: None };
        let duplicate = SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id: 1, error: None };
        let second = SecondaryIndexReadFollowup::RangeSidecarFailure { index_id: 1, error: None };
        let mut accumulator = GraphRowUniqueFollowups::new(vec![first, duplicate, second]);
        accumulator.extend(vec![
            SecondaryIndexReadFollowup::RangeSidecarFailure { index_id: 1, error: None },
            SecondaryIndexReadFollowup::EqualitySidecarFailure { index_id: 2, error: None },
            SecondaryIndexReadFollowup::EqualitySidecarFailure {
                index_id: 1,
                error: Some(EngineError::InvalidOperation("first failure".into())),
            },
            SecondaryIndexReadFollowup::EqualitySidecarFailure {
                index_id: 1,
                error: Some(EngineError::InvalidOperation("second failure".into())),
            },
        ]);
        let keys = accumulator
            .followups
            .iter()
            .map(SecondaryIndexReadFollowup::dedup_key)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 5);
        assert_eq!(keys[0], SecondaryIndexReadFollowupKey::EqualityBuilding { index_id: 1 });
        assert_eq!(keys[1], SecondaryIndexReadFollowupKey::RangeBuilding { index_id: 1 });
        assert_eq!(keys[2], SecondaryIndexReadFollowupKey::EqualityBuilding { index_id: 2 });
        assert!(matches!(
            &keys[3],
            SecondaryIndexReadFollowupKey::EqualityFailed { message, .. }
                if message.contains("first failure")
        ));
        assert!(matches!(
            &keys[4],
            SecondaryIndexReadFollowupKey::EqualityFailed { message, .. }
                if message.contains("second failure")
        ));
    }
}
