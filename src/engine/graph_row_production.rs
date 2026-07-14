use std::cell::Cell as ProductionCell;
use std::collections::{
    BTreeMap as ProductionBTreeMap, BinaryHeap as ProductionBinaryHeap,
    HashSet as ProductionHashSet,
};
use std::rc::Rc as ProductionRc;
use std::sync::Arc as ProductionArc;

type GraphRowCachedRows = ProductionArc<Vec<crate::graph_row::GraphBindingRow>>;
type GraphRowCachedVlpResult = ProductionArc<GraphRowVlpSearchResult>;

const GRAPH_ROW_CHUNK_MAX: usize = 4096;

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

fn graph_row_schedule_retry_halves(
    stack: &mut Vec<Vec<u64>>,
    ids: &[u64],
) {
    let split = ids.len() / 2;
    // LIFO: push right first so the left half is always the next consumed leaf.
    stack.push(ids[split..].to_vec());
    stack.push(ids[..split].to_vec());
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
            // RuntimeOnce admission measured +21-29% on FO-033 optional in CP43.2/CP43.3; keep it disabled.
            GraphRowChunkSource::RuntimeOnce { .. } => Self::compatibility(max_units),
        }
    }

    fn begin_attempt(&self) -> GraphRowCacheAttempt {
        GraphRowCacheAttempt::default()
    }

    fn prepare_anchor_leaf(&mut self, remaining_work: bool) {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AnchorMonotonicityProof {
    anchor_slot: crate::graph_row::GraphBindingSlotRef,
    logical_key_ordinal: usize,
    source_order: GraphRowSourceOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphRowChunkEligibility {
    Eligible,
    OrderedQuery,
    AnchorNotFirstLogicalSlot,
    NonNodeInitialDriver,
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
        format!(
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
        )
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

fn graph_row_chunk_source(
    runtime: &GraphRowRuntimePlan,
    physical_plan: &GraphRowPhysicalPlan,
) -> GraphRowChunkSource {
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
    let GraphRowChunkSource::AnchorPull { anchor_slot, .. } = source else {
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
            GraphRowChunkSource::AnchorPull { .. } => unreachable!(),
        };
        return (None, eligibility);
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
    if first.kind != crate::graph_row::GraphBindingSlotKind::Node || first_ref != *anchor_slot {
        return (None, GraphRowChunkEligibility::AnchorNotFirstLogicalSlot);
    }
    (
        Some(AnchorMonotonicityProof {
            anchor_slot: *anchor_slot,
            logical_key_ordinal: 0,
            source_order: GraphRowSourceOrder::NodeIdAsc,
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

impl ReadView {
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
            graph_row_chunk_source(runtime, physical_plan)
        };
        let (proof, eligibility) = graph_row_anchor_monotonicity_proof(query, &source);
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
        let cursor_seek_anchor = if graph_row_test_cursor_seek_disabled() {
            None
        } else {
            graph_row_cursor_seek_anchor(proof, cursor_state)
        };
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
                        let mut stack = vec![page.ids];
                        while let Some(ids) = stack.pop() {
                            #[cfg(test)]
                            self.note_graph_row_retry_input_rows_peak(ids.len());
                            graph_row_clear_cap_trip();
                            let remaining_work = !stack.is_empty() || next.is_some();
                            caches.prepare_anchor_leaf(remaining_work);
                            let mut attempt = caches.begin_attempt();
                            let mut attempt_frontier_peak = 0usize;
                            let mut attempt_intermediate_peak = 0usize;
                            let mut attempt_paths_enumerated = 0usize;
                            let mut scratch = graph_row_scratch_trace(explain_trace.is_some());
                            let mut initial_rows = Vec::with_capacity(ids.len());
                            for node_id in &ids {
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
                                        graph_row_schedule_retry_halves(&mut stack, &ids);
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

    #[test]
    fn graph_row_chunk_cursor_seek_boundary_is_inclusive_and_zero_safe() {
        let proof = Some(AnchorMonotonicityProof {
            anchor_slot: crate::graph_row::GraphBindingSlotRef {
                kind: crate::graph_row::GraphBindingSlotKind::Node,
                index: 0,
            },
            logical_key_ordinal: 0,
            source_order: GraphRowSourceOrder::NodeIdAsc,
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
    fn graph_row_cache_source_policy_admits_only_for_anchor_pull() {
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
        anchor_caches.prepare_anchor_leaf(false);
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
        single_leaf.prepare_anchor_leaf(false);
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
    fn graph_row_chunk_retry_schedules_left_before_right_without_recombination() {
        let mut stack = Vec::new();
        graph_row_schedule_retry_halves(&mut stack, &[1, 2, 3, 4, 5]);
        assert_eq!(stack.pop(), Some(vec![1, 2]));
        assert_eq!(stack.pop(), Some(vec![3, 4, 5]));
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
