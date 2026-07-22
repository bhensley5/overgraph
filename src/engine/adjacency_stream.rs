const MEMTABLE_REFILL_SEEK_UNITS: u64 = u64::BITS as u64;
const ADJACENCY_NEWEST_EDGE_CACHE_CAP: usize = 65_536;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdjacencySourceLayer {
    Active,
    Immutable(usize),
    Segment(usize),
}

#[derive(Debug)]
struct AdjacencyRawPosting {
    layer: AdjacencySourceLayer,
    direction: Direction,
    owner_id: u64,
    entry: crate::memtable::AdjEntry,
}

#[derive(Clone, Copy)]
struct AdjacencyPendingCandidate {
    meta: EdgeMetadataCandidate,
    logical_from: u64,
    logical_to: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AdjacencyImmutablePostingBuffer {
    cursor_index: usize,
    owner_id: u64,
    next_unread: usize,
    refill_exhausted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AdjacencyNewestEdgeOpinion {
    MissingOrTombstoned,
    Live {
        layer: AdjacencySourceLayer,
        meta: EdgeMetadataCandidate,
    },
}

struct AdjacencyNewestEdgeCache {
    entries: NodeIdMap<AdjacencyNewestEdgeOpinion>,
    fifo: VecDeque<u64>,
    capacity: usize,
}

impl AdjacencyNewestEdgeCache {
    fn prepared(capacity: usize) -> Self {
        Self {
            entries: NodeIdMap::default(),
            fifo: VecDeque::new(),
            capacity,
        }
    }

    fn get(&self, edge_id: u64) -> Option<AdjacencyNewestEdgeOpinion> {
        self.entries.get(&edge_id).copied()
    }

    fn insert(&mut self, edge_id: u64, opinion: AdjacencyNewestEdgeOpinion) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.entries.entry(edge_id)
        {
            entry.insert(opinion);
            return;
        }
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() == self.capacity {
            let oldest = self
                .fifo
                .pop_front()
                .expect("nonempty newest-edge cache must have a FIFO key");
            self.entries.remove(&oldest);
        }
        self.entries.insert(edge_id, opinion);
        self.fifo.push_back(edge_id);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn retained_units(&self) -> (usize, usize) {
        (self.entries.len(), self.fifo.len())
    }

    #[cfg(test)]
    fn storage_capacities(&self) -> (usize, usize) {
        (self.entries.capacity(), self.fifo.capacity())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdjacencyCursorState {
    Unseeded,
    Seeking,
    Seeding,
    Head,
    Posting,
    Lookahead,
    Eof,
}

enum AdjacencyPhysicalSource {
    Immutable {
        memtable: Arc<Memtable>,
        owner_bound: std::ops::Bound<(u64, u32)>,
        posting_bound: std::ops::Bound<u64>,
        refill_seek_debt: u64,
    },
    Segment {
        cursor: crate::segment_reader::SegmentAdjacencyGroupCursor,
        inclusive_seek: bool,
        postings_remaining: usize,
    },
}

struct AdjacencyPhysicalCursor {
    layer: AdjacencySourceLayer,
    direction: Direction,
    state: AdjacencyCursorState,
    head_owner: Option<u64>,
    source: AdjacencyPhysicalSource,
    #[cfg(test)]
    fail_next_group_step: bool,
    #[cfg(test)]
    fail_posting_for_owner: Option<u64>,
}

impl AdjacencyPhysicalCursor {
    fn immutable(
        layer: AdjacencySourceLayer,
        memtable: Arc<Memtable>,
        direction: Direction,
        seek_owner: Option<u64>,
    ) -> Self {
        Self {
            layer,
            direction,
            state: AdjacencyCursorState::Unseeded,
            head_owner: None,
            source: AdjacencyPhysicalSource::Immutable {
                memtable,
                owner_bound: seek_owner.map_or(std::ops::Bound::Unbounded, |owner| {
                    std::ops::Bound::Included((owner, u32::MIN))
                }),
                posting_bound: std::ops::Bound::Unbounded,
                refill_seek_debt: MEMTABLE_REFILL_SEEK_UNITS,
            },
            #[cfg(test)]
            fail_next_group_step: false,
            #[cfg(test)]
            fail_posting_for_owner: None,
        }
    }

    fn segment(
        layer: AdjacencySourceLayer,
        cursor: crate::segment_reader::SegmentAdjacencyGroupCursor,
        direction: Direction,
        inclusive_seek: bool,
    ) -> Self {
        Self {
            layer,
            direction,
            state: AdjacencyCursorState::Unseeded,
            head_owner: None,
            source: AdjacencyPhysicalSource::Segment {
                cursor,
                inclusive_seek,
                postings_remaining: 0,
            },
            #[cfg(test)]
            fail_next_group_step: false,
            #[cfg(test)]
            fail_posting_for_owner: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct AdjacencyPullBoundaryHint {
    page_target_met: bool,
    oriented_candidates_needed: usize,
}

struct AdjacencyPullResult {
    oriented_metadata: Vec<GraphRowOrientedEdge>,
    followups: Vec<SecondaryIndexReadFollowup>,
    has_more: bool,
    pending_completed_through_owner: Option<u64>,
    pending_last_completed_owner: Option<u64>,
    pending_completed_owner_groups: usize,
    partial_owner_pull: bool,
    physical_scan_units: u64,
    cursor_seek_units: u64,
    adj_index_entries_scanned: u64,
    raw_postings_scanned: u64,
    shadowed_or_stale_postings: u64,
}

#[derive(Default)]
struct AdjacencyPullCounters {
    cursor_seek_units: u64,
    adj_index_entries_scanned: u64,
    raw_postings_scanned: u64,
    shadowed_or_stale_postings: u64,
}

impl AdjacencyPullCounters {
    fn add_seek(&mut self, count: u64) -> Result<(), EngineError> {
        self.cursor_seek_units = self.cursor_seek_units.checked_add(count).ok_or_else(|| {
            EngineError::InvalidOperation("adjacency cursor seek counter overflow".into())
        })?;
        Ok(())
    }

    fn add_index(&mut self, count: usize) -> Result<(), EngineError> {
        let count = u64::try_from(count).map_err(|_| {
            EngineError::InvalidOperation("adjacency index count does not fit in u64".into())
        })?;
        self.adj_index_entries_scanned = self
            .adj_index_entries_scanned
            .checked_add(count)
            .ok_or_else(|| {
                EngineError::InvalidOperation("adjacency index counter overflow".into())
            })?;
        Ok(())
    }

    fn add_raw(&mut self, count: usize) -> Result<(), EngineError> {
        let count = u64::try_from(count).map_err(|_| {
            EngineError::InvalidOperation("adjacency posting count does not fit in u64".into())
        })?;
        self.raw_postings_scanned = self
            .raw_postings_scanned
            .checked_add(count)
            .ok_or_else(|| {
                EngineError::InvalidOperation("adjacency posting counter overflow".into())
            })?;
        Ok(())
    }

    fn add_shadowed(&mut self) -> Result<(), EngineError> {
        self.shadowed_or_stale_postings = self
            .shadowed_or_stale_postings
            .checked_add(1)
            .ok_or_else(|| {
                EngineError::InvalidOperation("adjacency shadow counter overflow".into())
            })?;
        Ok(())
    }

    fn physical(&self) -> Result<u64, EngineError> {
        self.cursor_seek_units
            .checked_add(self.adj_index_entries_scanned)
            .and_then(|sum| sum.checked_add(self.raw_postings_scanned))
            .ok_or_else(|| {
                EngineError::InvalidOperation("adjacency physical counter overflow".into())
            })
    }
}

enum PreparedAdjacencyPullPreparation {
    Ready(Box<PreparedAdjacencyPullSource>),
    Empty,
    MutableActiveMemtable,
}

struct PreparedAdjacencyPullSource {
    read_view: ReadView,
    policy_cutoffs: Option<PrecomputedPruneCutoffs>,
    requested_direction: Direction,
    normalized_labels: Option<Box<[u32]>>,
    execution_query: NormalizedEdgeQuery,
    cursors: Vec<AdjacencyPhysicalCursor>,
    owner_heap: BinaryHeap<Reverse<(u64, usize)>>,
    owner_cursors: VecDeque<usize>,
    current_owner: Option<u64>,
    initialization_next: usize,
    initialized: bool,
    posting_scratch: Vec<crate::memtable::AdjEntry>,
    immutable_posting_buffer: Option<AdjacencyImmutablePostingBuffer>,
    raw_batch: Vec<AdjacencyRawPosting>,
    batch_opinions: Vec<Option<AdjacencyNewestEdgeOpinion>>,
    newest_lookup_ids: Vec<u64>,
    newest_lookup_opinions: Vec<AdjacencyNewestEdgeOpinion>,
    pending_candidates: Vec<AdjacencyPendingCandidate>,
    verifier_metadata: Vec<EdgeMetadataCandidate>,
    verified_batch: Vec<EdgeMetadataForQuery>,
    newest_edge_cache: AdjacencyNewestEdgeCache,
    endpoint_visibility_cache: EdgeEndpointVisibilityCache,
    #[cfg(test)]
    verifier_batches_before_failure: Option<usize>,
    advanced: bool,
    failed: bool,
}

impl ReadView {
    fn prepare_adjacency_pull_source(
        &self,
        direction: Direction,
        labels: Option<&[u32]>,
        inclusive_seek_owner: Option<u64>,
        execution_query: NormalizedEdgeQuery,
        policy_cutoffs: Option<PrecomputedPruneCutoffs>,
    ) -> Result<PreparedAdjacencyPullPreparation, EngineError> {
        #[cfg(test)]
        if graph_row_test_take_adjacency_prepare_failure() {
            return Err(EngineError::CorruptRecord(
                "injected adjacency preparation corruption".into(),
            ));
        }
        let normalized_labels = labels.map(|labels| {
            let mut normalized = labels.to_vec();
            normalized.sort_unstable();
            normalized.dedup();
            normalized.into_boxed_slice()
        });
        if normalized_labels
            .as_deref()
            .is_some_and(<[u32]>::is_empty)
        {
            return Ok(PreparedAdjacencyPullPreparation::Empty);
        }
        if !self.memtable.retained_edge_record_keys_is_empty() {
            return Ok(PreparedAdjacencyPullPreparation::MutableActiveMemtable);
        }

        let physical_directions: &[Direction] = match direction {
            Direction::Outgoing => &[Direction::Outgoing],
            Direction::Incoming => &[Direction::Incoming],
            Direction::Both => &[Direction::Outgoing, Direction::Incoming],
        };
        let mut cursors = Vec::new();
        for (immutable_index, epoch) in self.immutable_epochs.iter().enumerate() {
            for &physical_direction in physical_directions {
                if epoch
                    .memtable
                    .retained_ordered_adj_outer_group_count(physical_direction)
                    == 0
                {
                    continue;
                }
                cursors.push(AdjacencyPhysicalCursor::immutable(
                    AdjacencySourceLayer::Immutable(immutable_index),
                    Arc::clone(&epoch.memtable),
                    physical_direction,
                    inclusive_seek_owner,
                ));
            }
        }
        for (segment_index, segment) in self.segments.iter().enumerate() {
            for &physical_direction in physical_directions {
                if segment.adjacency_group_count(physical_direction)? == 0 {
                    continue;
                }
                let cursor = segment.adjacency_group_cursor(
                    physical_direction,
                    normalized_labels.as_deref(),
                    inclusive_seek_owner,
                )?;
                cursors.push(AdjacencyPhysicalCursor::segment(
                    AdjacencySourceLayer::Segment(segment_index),
                    cursor,
                    physical_direction,
                    inclusive_seek_owner.is_some(),
                ));
            }
        }

        Ok(PreparedAdjacencyPullPreparation::Ready(Box::new(
            PreparedAdjacencyPullSource {
                read_view: self.clone(),
                policy_cutoffs,
                requested_direction: direction,
                normalized_labels,
                execution_query,
                owner_heap: BinaryHeap::with_capacity(cursors.len()),
                owner_cursors: VecDeque::with_capacity(cursors.len()),
                cursors,
                current_owner: None,
                initialization_next: 0,
                initialized: false,
                posting_scratch: Vec::new(),
                immutable_posting_buffer: None,
                raw_batch: Vec::new(),
                batch_opinions: Vec::new(),
                newest_lookup_ids: Vec::new(),
                newest_lookup_opinions: Vec::new(),
                pending_candidates: Vec::new(),
                verifier_metadata: Vec::new(),
                verified_batch: Vec::new(),
                newest_edge_cache: AdjacencyNewestEdgeCache::prepared(
                    ADJACENCY_NEWEST_EDGE_CACHE_CAP,
                ),
                endpoint_visibility_cache: EdgeEndpointVisibilityCache::prepared(
                    PREPARED_EDGE_ENDPOINT_VISIBILITY_CAP,
                ),
                #[cfg(test)]
                verifier_batches_before_failure: None,
                advanced: false,
                failed: false,
            },
        )))
    }
}

impl PreparedAdjacencyPullSource {
    #[cfg(test)]
    fn cursor_count(&self) -> usize {
        self.cursors.len()
    }

    fn has_more(&self) -> bool {
        self.immutable_posting_buffer.is_some()
            || self.current_owner.is_some()
            || !self.owner_cursors.is_empty()
            || !self.owner_heap.is_empty()
            || self
                .cursors
                .iter()
                .any(|cursor| cursor.state != AdjacencyCursorState::Eof)
    }

    fn all_heads_known(&self) -> bool {
        self.cursors.iter().all(|cursor| {
            matches!(cursor.state, AdjacencyCursorState::Head | AdjacencyCursorState::Eof)
        })
    }

    fn physical_remaining(
        counters: &AdjacencyPullCounters,
        scheduled: usize,
    ) -> Result<usize, EngineError> {
        let used = usize::try_from(counters.physical()?).map_err(|_| {
            EngineError::InvalidOperation("adjacency physical count does not fit in usize".into())
        })?;
        scheduled.checked_sub(used).ok_or_else(|| {
            EngineError::InvalidOperation("adjacency physical budget exceeded".into())
        })
    }

    fn retire_immutable_refill_seek_debt(
        refill_seek_debt: &mut u64,
        physical_remaining: usize,
        counters: &mut AdjacencyPullCounters,
        advanced: &mut bool,
    ) -> Result<(), EngineError> {
        let physical_remaining = u64::try_from(physical_remaining).map_err(|_| {
            EngineError::InvalidOperation(
                "adjacency physical remainder does not fit in u64".into(),
            )
        })?;
        let retired = (*refill_seek_debt).min(physical_remaining);
        *refill_seek_debt = refill_seek_debt.checked_sub(retired).ok_or_else(|| {
            EngineError::InvalidOperation("adjacency refill seek debt underflow".into())
        })?;
        counters.add_seek(retired)?;
        if retired > 0 {
            *advanced = true;
        }
        Ok(())
    }

    fn seed_owner_heap(&mut self) {
        debug_assert!(self.owner_heap.is_empty());
        for (index, cursor) in self.cursors.iter().enumerate() {
            if cursor.state == AdjacencyCursorState::Head {
                self.owner_heap.push(Reverse((
                    cursor
                        .head_owner
                        .expect("head cursor must retain its owner"),
                    index,
                )));
            }
        }
    }

    fn initialize_one(
        &mut self,
        index: usize,
        scheduled: usize,
        counters: &mut AdjacencyPullCounters,
    ) -> Result<(), EngineError> {
        let labels = self.normalized_labels.as_deref();
        let snapshot_seq = self.read_view.snapshot_seq;
        let cursor = &mut self.cursors[index];
        match &mut cursor.source {
            AdjacencyPhysicalSource::Immutable {
                memtable,
                owner_bound,
                refill_seek_debt,
                ..
            } => {
                if cursor.state == AdjacencyCursorState::Unseeded {
                    cursor.state = AdjacencyCursorState::Seeding;
                }
                if *refill_seek_debt > 0 {
                    let remaining = Self::physical_remaining(counters, scheduled)?;
                    Self::retire_immutable_refill_seek_debt(
                        refill_seek_debt,
                        remaining,
                        counters,
                        &mut self.advanced,
                    )?;
                    if *refill_seek_debt > 0
                        || Self::physical_remaining(counters, scheduled)? == 0
                    {
                        return Ok(());
                    }
                }
                let refill = memtable.refill_ordered_adj_owner_at(
                    cursor.direction,
                    labels,
                    snapshot_seq,
                    *owner_bound,
                    1,
                );
                counters.add_index(refill.examined)?;
                self.advanced = true;
                if let Some(owner) = refill.owner_head {
                    cursor.head_owner = Some(owner);
                    cursor.state = AdjacencyCursorState::Head;
                } else if refill.exhausted {
                    cursor.head_owner = None;
                    cursor.state = AdjacencyCursorState::Eof;
                } else {
                    let resume = refill.last_group_key_examined.ok_or_else(|| {
                        EngineError::InvalidOperation(
                            "nonterminal adjacency owner refill omitted its resume key".into(),
                        )
                    })?;
                    *owner_bound = std::ops::Bound::Excluded(resume);
                    *refill_seek_debt = MEMTABLE_REFILL_SEEK_UNITS;
                }
            }
            AdjacencyPhysicalSource::Segment {
                cursor: segment_cursor,
                inclusive_seek,
                postings_remaining,
            } => {
                if cursor.state == AdjacencyCursorState::Unseeded {
                    cursor.state = if *inclusive_seek {
                        AdjacencyCursorState::Seeking
                    } else {
                        AdjacencyCursorState::Seeding
                    };
                }
                self.advanced = true;
                #[cfg(test)]
                if std::mem::take(&mut cursor.fail_next_group_step) {
                    return Err(EngineError::CorruptRecord(
                        "injected adjacency group-step corruption".into(),
                    ));
                }
                let step = segment_cursor.next_group_step()?;
                match step {
                    crate::segment_reader::SegmentAdjacencyGroupStep::SeekProbe { .. } => {
                        counters.add_seek(1)?;
                        cursor.state = AdjacencyCursorState::Seeking;
                    }
                    crate::segment_reader::SegmentAdjacencyGroupStep::FilteredGroup { .. } => {
                        counters.add_index(1)?;
                        cursor.state = AdjacencyCursorState::Seeding;
                    }
                    crate::segment_reader::SegmentAdjacencyGroupStep::Group(group) => {
                        counters.add_index(1)?;
                        cursor.head_owner = Some(group.owner_id);
                        *postings_remaining = group.posting_count;
                        cursor.state = AdjacencyCursorState::Head;
                    }
                    crate::segment_reader::SegmentAdjacencyGroupStep::Exhausted => {
                        cursor.head_owner = None;
                        cursor.state = AdjacencyCursorState::Eof;
                    }
                }
            }
        }
        Ok(())
    }

    fn initialize(
        &mut self,
        scheduled: usize,
        counters: &mut AdjacencyPullCounters,
    ) -> Result<(), EngineError> {
        if self.initialized {
            return Ok(());
        }
        if self.cursors.is_empty() {
            self.initialized = true;
            return Ok(());
        }
        while Self::physical_remaining(counters, scheduled)? > 0 && !self.all_heads_known() {
            let cursor_count = self.cursors.len();
            let mut selected = None;
            for offset in 0..cursor_count {
                let index = (self.initialization_next + offset) % cursor_count;
                if !matches!(
                    self.cursors[index].state,
                    AdjacencyCursorState::Head | AdjacencyCursorState::Eof
                ) {
                    selected = Some(index);
                    break;
                }
            }
            let Some(index) = selected else {
                break;
            };
            self.initialization_next = (index + 1) % cursor_count;
            self.initialize_one(index, scheduled, counters)?;
        }
        if self.all_heads_known() {
            self.initialized = true;
            self.seed_owner_heap();
        }
        Ok(())
    }

    fn select_next_owner(&mut self) -> Option<u64> {
        let Reverse((owner, index)) = self.owner_heap.pop()?;
        self.owner_cursors.push_back(index);
        while self
            .owner_heap
            .peek()
            .is_some_and(|Reverse((next_owner, _))| *next_owner == owner)
        {
            let Reverse((_, index)) = self.owner_heap.pop().unwrap();
            self.owner_cursors.push_back(index);
        }
        self.current_owner = Some(owner);
        for &cursor_index in &self.owner_cursors {
            let cursor = &mut self.cursors[cursor_index];
            debug_assert_eq!(cursor.head_owner, Some(owner));
            cursor.state = AdjacencyCursorState::Posting;
            if let AdjacencyPhysicalSource::Segment {
                postings_remaining,
                ..
            } = &cursor.source
            {
                if *postings_remaining == 0 {
                    cursor.state = AdjacencyCursorState::Lookahead;
                }
            }
            if let AdjacencyPhysicalSource::Immutable {
                posting_bound,
                refill_seek_debt,
                ..
            } = &mut cursor.source
            {
                *posting_bound = std::ops::Bound::Unbounded;
                *refill_seek_debt = MEMTABLE_REFILL_SEEK_UNITS;
            }
        }
        Some(owner)
    }

    fn prepare_batch_scratch(&mut self, scheduled: usize) {
        let batch_capacity = scheduled.min(QUERY_VERIFY_CHUNK);
        if self.newest_lookup_ids.capacity() < batch_capacity
            || self.newest_lookup_ids.capacity() > scheduled
        {
            self.newest_lookup_ids = Vec::with_capacity(batch_capacity);
        }
        if self.raw_batch.capacity() < batch_capacity || self.raw_batch.capacity() > scheduled {
            self.raw_batch = Vec::with_capacity(batch_capacity);
        }
        if self.batch_opinions.capacity() < batch_capacity
            || self.batch_opinions.capacity() > scheduled
        {
            self.batch_opinions = Vec::with_capacity(batch_capacity);
        }
        if self.newest_lookup_opinions.capacity() < batch_capacity
            || self.newest_lookup_opinions.capacity() > scheduled
        {
            self.newest_lookup_opinions = Vec::with_capacity(batch_capacity);
        }
        if self.pending_candidates.capacity() < batch_capacity
            || self.pending_candidates.capacity() > scheduled
        {
            self.pending_candidates = Vec::with_capacity(batch_capacity);
        }
        if self.verifier_metadata.capacity() < batch_capacity
            || self.verifier_metadata.capacity() > scheduled
        {
            self.verifier_metadata = Vec::with_capacity(batch_capacity);
        }
        if self.verified_batch.capacity() < batch_capacity
            || self.verified_batch.capacity() > scheduled
        {
            self.verified_batch = Vec::with_capacity(batch_capacity);
        }
    }

    fn resolve_newest_edge_batch(&mut self) -> Result<(), EngineError> {
        self.batch_opinions.clear();
        self.newest_lookup_ids.clear();
        for raw in &self.raw_batch {
            let cached = self.newest_edge_cache.get(raw.entry.edge_id);
            self.batch_opinions.push(cached);
            if cached.is_none() {
                self.newest_lookup_ids.push(raw.entry.edge_id);
            }
        }
        self.newest_lookup_ids.sort_unstable();
        self.newest_lookup_ids.dedup();
        if self.newest_lookup_ids.is_empty() {
            return Ok(());
        }

        #[cfg(test)]
        self.read_view
            .note_adjacency_newest_opinion_resolution(self.newest_lookup_ids.len());
        let opinions = self
            .read_view
            .sources()
            .find_edge_metadata_with_source(&self.newest_lookup_ids)?;
        self.newest_lookup_opinions.clear();
        for opinion in opinions {
            let opinion = match opinion {
                crate::source_list::EdgeMetadataSourceOpinion::MissingOrTombstoned => {
                    AdjacencyNewestEdgeOpinion::MissingOrTombstoned
                }
                crate::source_list::EdgeMetadataSourceOpinion::Live { layer, meta } => {
                    let layer = match layer {
                        crate::source_list::EdgeMetadataSourceLayer::Active => {
                            AdjacencySourceLayer::Active
                        }
                        crate::source_list::EdgeMetadataSourceLayer::Immutable(index) => {
                            AdjacencySourceLayer::Immutable(index)
                        }
                        crate::source_list::EdgeMetadataSourceLayer::Segment(index) => {
                            AdjacencySourceLayer::Segment(index)
                        }
                    };
                    AdjacencyNewestEdgeOpinion::Live { layer, meta }
                }
            };
            self.newest_lookup_opinions.push(opinion);
        }
        for (raw, slot) in self.raw_batch.iter().zip(&mut self.batch_opinions) {
            if slot.is_some() {
                continue;
            }
            let index = self
                .newest_lookup_ids
                .binary_search(&raw.entry.edge_id)
                .expect("uncached batch edge must have one resolved lookup");
            *slot = Some(self.newest_lookup_opinions[index]);
        }
        for (&edge_id, &opinion) in self
            .newest_lookup_ids
            .iter()
            .zip(&self.newest_lookup_opinions)
        {
            self.newest_edge_cache.insert(edge_id, opinion);
        }
        Ok(())
    }

    fn flush_raw_batch(
        &mut self,
        output: &mut Vec<GraphRowOrientedEdge>,
        counters: &mut AdjacencyPullCounters,
    ) -> Result<(), EngineError> {
        if self.raw_batch.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        if let Some(remaining) = &mut self.verifier_batches_before_failure {
            if *remaining == 0 {
                return Err(EngineError::CorruptRecord(
                    "injected buffered adjacency verifier corruption".into(),
                ));
            }
            *remaining -= 1;
        }
        self.resolve_newest_edge_batch()?;
        self.pending_candidates.clear();
        for (raw, opinion) in self.raw_batch.iter().zip(&self.batch_opinions) {
            let opinion = opinion.expect("batched newest-edge opinion must be materialized");
            let AdjacencyNewestEdgeOpinion::Live { layer, meta } = opinion else {
                counters.add_shadowed()?;
                continue;
            };
            if layer != raw.layer
                || meta.label_id != raw.entry.label_id
                || self
                    .normalized_labels
                    .as_deref()
                    .is_some_and(|labels| labels.binary_search(&meta.label_id).is_err())
            {
                counters.add_shadowed()?;
                continue;
            }
            if !match self.requested_direction {
                Direction::Outgoing => raw.direction == Direction::Outgoing,
                Direction::Incoming => raw.direction == Direction::Incoming,
                Direction::Both => true,
            } {
                counters.add_shadowed()?;
                continue;
            }
            let (logical_from, logical_to) = match raw.direction {
                Direction::Outgoing
                    if meta.from == raw.owner_id && meta.to == raw.entry.neighbor_id =>
                {
                    (meta.from, meta.to)
                }
                Direction::Incoming
                    if meta.to == raw.owner_id && meta.from == raw.entry.neighbor_id =>
                {
                    if self.requested_direction == Direction::Both && meta.from == meta.to {
                        continue;
                    }
                    (meta.to, meta.from)
                }
                _ => {
                    counters.add_shadowed()?;
                    continue;
                }
            };
            self.pending_candidates.push(AdjacencyPendingCandidate {
                meta,
                logical_from,
                logical_to,
            });
        }

        self.verifier_metadata.clear();
        self.verifier_metadata
            .extend(self.pending_candidates.iter().map(|candidate| candidate.meta));
        self.verifier_metadata
            .sort_unstable_by_key(|meta| meta.edge_id);
        self.verifier_metadata
            .dedup_by_key(|meta| meta.edge_id);
        self.verified_batch.clear();
        if !self.verifier_metadata.is_empty() {
            self.endpoint_visibility_cache.begin_batch();
            let _ = self.read_view.verify_resolved_edge_metadata_chunk(
                &self.verifier_metadata,
                &self.execution_query,
                self.policy_cutoffs.as_ref(),
                &mut self.endpoint_visibility_cache,
                &mut self.verified_batch,
                self.verifier_metadata.len(),
            )?;
            self.verified_batch.sort_unstable_by_key(|meta| meta.id);
        }
        for candidate in &self.pending_candidates {
            if let Ok(index) = self
                .verified_batch
                .binary_search_by_key(&candidate.meta.edge_id, |meta| meta.id)
            {
                output.push(GraphRowOrientedEdge {
                    meta: self.verified_batch[index],
                    logical_from: candidate.logical_from,
                    logical_to: candidate.logical_to,
                });
            }
        }
        self.raw_batch.clear();
        self.batch_opinions.clear();
        self.newest_lookup_ids.clear();
        self.newest_lookup_opinions.clear();
        self.pending_candidates.clear();
        self.verifier_metadata.clear();
        self.verified_batch.clear();
        Ok(())
    }

    fn finish_owner_cursor(&mut self, index: usize) {
        let cursor = &self.cursors[index];
        match cursor.state {
            AdjacencyCursorState::Head => self.owner_heap.push(Reverse((
                cursor
                    .head_owner
                    .expect("advanced head cursor must retain an owner"),
                index,
            ))),
            AdjacencyCursorState::Eof => {}
            _ => panic!("owner cursor finished before reaching a head or EOF"),
        }
        let popped = self.owner_cursors.pop_front();
        debug_assert_eq!(popped, Some(index));
    }

    fn drain_immutable_posting_buffer(
        &mut self,
        scheduled: usize,
        output_len: usize,
    ) -> Result<bool, EngineError> {
        let Some(mut buffer) = self.immutable_posting_buffer else {
            return Ok(true);
        };
        let owner = self.current_owner.ok_or_else(|| {
            EngineError::InvalidOperation(
                "buffered immutable adjacency postings lost their owner".into(),
            )
        })?;
        if owner != buffer.owner_id || self.owner_cursors.front() != Some(&buffer.cursor_index) {
            return Err(EngineError::InvalidOperation(
                "buffered immutable adjacency postings lost their owning cursor".into(),
            ));
        }
        if buffer.next_unread > self.posting_scratch.len() {
            return Err(EngineError::InvalidOperation(
                "buffered immutable adjacency posting index is out of bounds".into(),
            ));
        }

        let output_remaining = scheduled
            .saturating_sub(output_len.saturating_add(self.raw_batch.len()));
        let batch_remaining = QUERY_VERIFY_CHUNK.saturating_sub(self.raw_batch.len());
        let take = self
            .posting_scratch
            .len()
            .saturating_sub(buffer.next_unread)
            .min(output_remaining)
            .min(batch_remaining);
        let cursor = &self.cursors[buffer.cursor_index];
        let layer = cursor.layer;
        let direction = cursor.direction;
        for entry in self.posting_scratch
            [buffer.next_unread..buffer.next_unread.saturating_add(take)]
            .iter()
            .cloned()
        {
            self.raw_batch.push(AdjacencyRawPosting {
                layer,
                direction,
                owner_id: owner,
                entry,
            });
        }
        buffer.next_unread = buffer.next_unread.checked_add(take).ok_or_else(|| {
            EngineError::InvalidOperation(
                "buffered immutable adjacency posting index overflow".into(),
            )
        })?;

        if buffer.next_unread == self.posting_scratch.len() {
            self.posting_scratch.clear();
            self.immutable_posting_buffer = None;
            self.cursors[buffer.cursor_index].state = if buffer.refill_exhausted {
                AdjacencyCursorState::Lookahead
            } else {
                AdjacencyCursorState::Posting
            };
        } else {
            self.immutable_posting_buffer = Some(buffer);
        }
        Ok(take > 0 || self.immutable_posting_buffer.is_none())
    }

    fn advance_current_cursor(
        &mut self,
        scheduled: usize,
        output_len: usize,
        counters: &mut AdjacencyPullCounters,
    ) -> Result<bool, EngineError> {
        let Some(&index) = self.owner_cursors.front() else {
            return Ok(true);
        };
        let owner = self.current_owner.expect("owner cursor requires current owner");
        if self.immutable_posting_buffer.is_some() {
            return self.drain_immutable_posting_buffer(scheduled, output_len);
        }
        let labels = self.normalized_labels.as_deref();
        let snapshot_seq = self.read_view.snapshot_seq;
        let layer = self.cursors[index].layer;
        let physical_direction = self.cursors[index].direction;

        match self.cursors[index].state {
            AdjacencyCursorState::Posting => {
                if output_len.saturating_add(self.raw_batch.len()) >= scheduled {
                    return Ok(false);
                }
                #[cfg(test)]
                let fail_posting = {
                    let cursor = &mut self.cursors[index];
                    if matches!(cursor.source, AdjacencyPhysicalSource::Segment { .. })
                        && cursor.fail_posting_for_owner == Some(owner)
                    {
                        cursor.fail_posting_for_owner = None;
                        true
                    } else {
                        false
                    }
                };
                match &mut self.cursors[index].source {
                    AdjacencyPhysicalSource::Segment {
                        cursor,
                        postings_remaining,
                        ..
                    } => {
                        if Self::physical_remaining(counters, scheduled)? == 0 {
                            #[cfg(test)]
                            if fail_posting {
                                self.cursors[index].fail_posting_for_owner = Some(owner);
                            }
                            return Ok(false);
                        }
                        self.advanced = true;
                        #[cfg(test)]
                        if fail_posting {
                            return Err(EngineError::CorruptRecord(
                                "injected adjacency posting corruption".into(),
                            ));
                        }
                        let entry = cursor.next_raw_posting()?.ok_or_else(|| {
                            EngineError::CorruptRecord(
                                "adjacency group ended before its posting count".into(),
                            )
                        })?;
                        counters.add_raw(1)?;
                        *postings_remaining = postings_remaining.checked_sub(1).ok_or_else(|| {
                            EngineError::CorruptRecord(
                                "adjacency group produced more postings than declared".into(),
                            )
                        })?;
                        let raw = AdjacencyRawPosting {
                            layer,
                            direction: physical_direction,
                            owner_id: owner,
                            entry,
                        };
                        if *postings_remaining == 0 {
                            self.cursors[index].state = AdjacencyCursorState::Lookahead;
                        }
                        self.raw_batch.push(raw);
                    }
                    AdjacencyPhysicalSource::Immutable {
                        memtable,
                        owner_bound,
                        posting_bound,
                        refill_seek_debt,
                        ..
                    } => {
                        if *refill_seek_debt > 0 {
                            let remaining = Self::physical_remaining(counters, scheduled)?;
                            Self::retire_immutable_refill_seek_debt(
                                refill_seek_debt,
                                remaining,
                                counters,
                                &mut self.advanced,
                            )?;
                            if *refill_seek_debt > 0
                                || Self::physical_remaining(counters, scheduled)? == 0
                            {
                                return Ok(false);
                            }
                        }
                        let physical_remaining = Self::physical_remaining(counters, scheduled)?;
                        let refill_max = physical_remaining.min(scheduled);
                        if refill_max == 0 {
                            return Ok(false);
                        }
                        debug_assert!(self.immutable_posting_buffer.is_none());
                        debug_assert!(self.posting_scratch.is_empty());
                        if self.posting_scratch.capacity() < refill_max
                            || self.posting_scratch.capacity() > scheduled
                        {
                            self.posting_scratch = Vec::with_capacity(scheduled);
                        }
                        let scratch = std::mem::take(&mut self.posting_scratch);
                        let refill = memtable.refill_ordered_adj_postings_at(
                            physical_direction,
                            owner,
                            labels,
                            snapshot_seq,
                            *posting_bound,
                            refill_max,
                            scratch,
                        );
                        counters.add_raw(refill.examined)?;
                        self.advanced = true;
                        if let Some(last) = refill.last_edge_id_examined {
                            *posting_bound = std::ops::Bound::Excluded(last);
                        }
                        let exhausted = refill.exhausted;
                        self.posting_scratch = refill.entries;
                        if exhausted {
                            *owner_bound = std::ops::Bound::Excluded((owner, u32::MAX));
                        }
                        *refill_seek_debt = MEMTABLE_REFILL_SEEK_UNITS;
                        self.immutable_posting_buffer = Some(AdjacencyImmutablePostingBuffer {
                            cursor_index: index,
                            owner_id: owner,
                            next_unread: 0,
                            refill_exhausted: exhausted,
                        });
                    }
                }
            }
            AdjacencyCursorState::Lookahead => {
                #[cfg(test)]
                let fail_group_step = {
                    let cursor = &mut self.cursors[index];
                    std::mem::take(&mut cursor.fail_next_group_step)
                };
                match &mut self.cursors[index].source {
                    AdjacencyPhysicalSource::Segment {
                        cursor,
                        postings_remaining,
                        ..
                    } => {
                        if Self::physical_remaining(counters, scheduled)? == 0 {
                            #[cfg(test)]
                            if fail_group_step {
                                self.cursors[index].fail_next_group_step = true;
                            }
                            return Ok(false);
                        }
                    self.advanced = true;
                    #[cfg(test)]
                    if fail_group_step {
                        return Err(EngineError::CorruptRecord(
                            "injected adjacency group-step corruption".into(),
                        ));
                    }
                    let step = cursor.next_group_step()?;
                    match step {
                        crate::segment_reader::SegmentAdjacencyGroupStep::SeekProbe { .. } => {
                            counters.add_seek(1)?;
                        }
                        crate::segment_reader::SegmentAdjacencyGroupStep::FilteredGroup {
                            owner_id,
                            ..
                        } => {
                            counters.add_index(1)?;
                            if owner_id < owner {
                                return Err(EngineError::CorruptRecord(
                                    "adjacency owners are not ascending".into(),
                                ));
                            }
                        }
                        crate::segment_reader::SegmentAdjacencyGroupStep::Group(group) => {
                            counters.add_index(1)?;
                            if group.owner_id < owner {
                                return Err(EngineError::CorruptRecord(
                                    "adjacency owners are not ascending".into(),
                                ));
                            }
                            *postings_remaining = group.posting_count;
                            if group.owner_id == owner {
                                self.cursors[index].state = AdjacencyCursorState::Posting;
                            } else {
                                self.cursors[index].head_owner = Some(group.owner_id);
                                self.cursors[index].state = AdjacencyCursorState::Head;
                                self.finish_owner_cursor(index);
                            }
                        }
                        crate::segment_reader::SegmentAdjacencyGroupStep::Exhausted => {
                            self.cursors[index].head_owner = None;
                            self.cursors[index].state = AdjacencyCursorState::Eof;
                            self.finish_owner_cursor(index);
                        }
                    }
                }
                    AdjacencyPhysicalSource::Immutable {
                        memtable,
                        owner_bound,
                        refill_seek_debt,
                        ..
                    } => {
                    if *refill_seek_debt > 0 {
                        let remaining = Self::physical_remaining(counters, scheduled)?;
                        Self::retire_immutable_refill_seek_debt(
                            refill_seek_debt,
                            remaining,
                            counters,
                            &mut self.advanced,
                        )?;
                        if *refill_seek_debt > 0
                            || Self::physical_remaining(counters, scheduled)? == 0
                        {
                            return Ok(false);
                        }
                    }
                    let remaining = Self::physical_remaining(counters, scheduled)?;
                    if remaining == 0 {
                        return Ok(false);
                    }
                    let refill = memtable.refill_ordered_adj_owner_at(
                        physical_direction,
                        labels,
                        snapshot_seq,
                        *owner_bound,
                        remaining,
                    );
                    counters.add_index(refill.examined)?;
                    self.advanced = true;
                    if let Some(next_owner) = refill.owner_head {
                        if next_owner <= owner {
                            return Err(EngineError::CorruptRecord(
                                "immutable adjacency owner refill did not advance".into(),
                            ));
                        }
                        self.cursors[index].head_owner = Some(next_owner);
                        self.cursors[index].state = AdjacencyCursorState::Head;
                        self.finish_owner_cursor(index);
                    } else if refill.exhausted {
                        self.cursors[index].head_owner = None;
                        self.cursors[index].state = AdjacencyCursorState::Eof;
                        self.finish_owner_cursor(index);
                    } else {
                        let resume = refill.last_group_key_examined.ok_or_else(|| {
                            EngineError::InvalidOperation(
                                "nonterminal adjacency owner refill omitted its resume key".into(),
                            )
                        })?;
                        *owner_bound = std::ops::Bound::Excluded(resume);
                        *refill_seek_debt = MEMTABLE_REFILL_SEEK_UNITS;
                    }
                    }
                }
            }
            AdjacencyCursorState::Head | AdjacencyCursorState::Eof => {
                self.finish_owner_cursor(index);
            }
            _ => {
                return Err(EngineError::InvalidOperation(
                    "adjacency owner cursor entered an invalid production state".into(),
                ));
            }
        }
        Ok(true)
    }

    fn pull_inner(
        &mut self,
        scheduled: usize,
        hint: AdjacencyPullBoundaryHint,
    ) -> Result<AdjacencyPullResult, EngineError> {
        self.prepare_batch_scratch(scheduled);
        debug_assert!(self.raw_batch.is_empty());
        let mut counters = AdjacencyPullCounters::default();
        let mut oriented_metadata = Vec::with_capacity(scheduled);
        let mut pending_last_completed_owner = None;
        let mut pending_completed_through_owner = None;
        let mut pending_completed_owner_groups = 0usize;

        self.initialize(scheduled, &mut counters)?;
        if !self.initialized {
            let physical_scan_units = counters.physical()?;
            return Ok(AdjacencyPullResult {
                oriented_metadata,
                followups: Vec::new(),
                has_more: true,
                pending_completed_through_owner: None,
                pending_last_completed_owner: None,
                pending_completed_owner_groups: 0,
                partial_owner_pull: false,
                physical_scan_units,
                cursor_seek_units: counters.cursor_seek_units,
                adj_index_entries_scanned: counters.adj_index_entries_scanned,
                raw_postings_scanned: counters.raw_postings_scanned,
                shadowed_or_stale_postings: counters.shadowed_or_stale_postings,
            });
        }

        loop {
            if self.current_owner.is_none() {
                if oriented_metadata.len() >= scheduled
                    || Self::physical_remaining(&counters, scheduled)? == 0
                {
                    break;
                }
                if self.select_next_owner().is_none() {
                    break;
                }
            }
            let owner = self.current_owner.expect("selected owner must exist");
            let progressed =
                self.advance_current_cursor(scheduled, oriented_metadata.len(), &mut counters)?;
            let physical_remaining = Self::physical_remaining(&counters, scheduled)?;
            if self.raw_batch.len() >= QUERY_VERIFY_CHUNK
                || oriented_metadata.len().saturating_add(self.raw_batch.len()) >= scheduled
                || self.owner_cursors.is_empty()
                || !progressed
                || physical_remaining == 0
            {
                self.flush_raw_batch(&mut oriented_metadata, &mut counters)?;
            }
            if self.owner_cursors.is_empty() {
                debug_assert!(self.immutable_posting_buffer.is_none());
                self.current_owner = None;
                pending_last_completed_owner = Some(owner);
                pending_completed_through_owner = Some(owner);
                pending_completed_owner_groups = pending_completed_owner_groups
                    .checked_add(1)
                    .ok_or_else(|| {
                        EngineError::InvalidOperation(
                            "adjacency completed-owner counter overflow".into(),
                        )
                    })?;
                if hint.page_target_met
                    || oriented_metadata.len() >= hint.oriented_candidates_needed
                {
                    break;
                }
            } else {
                pending_completed_through_owner = None;
                if !progressed
                    || oriented_metadata.len() >= scheduled
                    || (physical_remaining == 0
                        && self.immutable_posting_buffer.is_none())
                {
                    break;
                }
            }
        }

        debug_assert!(self.raw_batch.is_empty());
        let physical_scan_units = counters.physical()?;
        debug_assert!(oriented_metadata.len() <= scheduled);
        debug_assert!(physical_scan_units <= scheduled as u64);
        Ok(AdjacencyPullResult {
            oriented_metadata,
            followups: Vec::new(),
            has_more: self.has_more(),
            pending_completed_through_owner,
            pending_last_completed_owner,
            pending_completed_owner_groups,
            partial_owner_pull: self.current_owner.is_some(),
            physical_scan_units,
            cursor_seek_units: counters.cursor_seek_units,
            adj_index_entries_scanned: counters.adj_index_entries_scanned,
            raw_postings_scanned: counters.raw_postings_scanned,
            shadowed_or_stale_postings: counters.shadowed_or_stale_postings,
        })
    }

    fn pull(
        &mut self,
        scheduled: usize,
        hint: AdjacencyPullBoundaryHint,
    ) -> Result<AdjacencyPullResult, EngineError> {
        if scheduled == 0 {
            return Err(EngineError::InvalidOperation(
                "adjacency pull schedule must be positive".into(),
            ));
        }
        if !hint.page_target_met && hint.oriented_candidates_needed == 0 {
            return Err(EngineError::InvalidOperation(
                "adjacency pull boundary hint requires a positive candidate need".into(),
            ));
        }
        if self.failed {
            return Err(EngineError::InvalidOperation(
                "adjacency pull source is terminal after an earlier error".into(),
            ));
        }
        let result = self.pull_inner(scheduled, hint);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

fn graph_row_adjacency_pull_scheduled_chunk_size(
    previous: Option<usize>,
    selection_capacity: usize,
    max_intermediate_bindings: usize,
    max_frontier: usize,
) -> usize {
    let ceiling = GRAPH_ROW_CHUNK_MAX
        .min(max_intermediate_bindings)
        .min(max_frontier.max(1));
    debug_assert!(ceiling > 0, "normalized graph-row intermediate cap must be positive");
    #[cfg(test)]
    if let Some(overridden) = GRAPH_ROW_TEST_CHUNK_OVERRIDE.with(ProductionCell::get) {
        return overridden.clamp(1, ceiling);
    }
    match previous {
        Some(previous) => previous.saturating_mul(2).min(ceiling),
        None => selection_capacity.clamp(1, ceiling),
    }
}
