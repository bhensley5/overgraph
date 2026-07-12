#[allow(dead_code)]
pub(crate) type NodeStreamSet = StreamSet<NodeCandidateCursor>;

#[allow(dead_code)]
pub(crate) enum NodeCandidateCursor {
    MemtableLabel {
        memtable: std::sync::Arc<Memtable>,
        label_id: u32,
        snapshot_seq: u64,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SegmentLabelPosting {
        reader: std::sync::Arc<SegmentReader>,
        posting: SegmentLabelPosting,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SegmentEqualityGroup {
        reader: std::sync::Arc<SegmentReader>,
        index_id: u64,
        offset: usize,
        id_count: usize,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SortedBuffer {
        ids: Vec<u64>,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
}

#[allow(dead_code)]
impl NodeCandidateCursor {
    pub(crate) fn memtable_label(
        memtable: std::sync::Arc<Memtable>,
        label_id: u32,
        snapshot_seq: u64,
    ) -> Self {
        Self::MemtableLabel {
            memtable,
            label_id,
            snapshot_seq,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn segment_label_posting(
        reader: std::sync::Arc<SegmentReader>,
        posting: SegmentLabelPosting,
    ) -> Self {
        Self::SegmentLabelPosting {
            reader,
            posting,
            pos: 0,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn segment_equality_group(
        reader: std::sync::Arc<SegmentReader>,
        index_id: u64,
        offset: usize,
        id_count: usize,
    ) -> Self {
        Self::SegmentEqualityGroup {
            reader,
            index_id,
            offset,
            id_count,
            pos: 0,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn sorted_buffer(ids: Vec<u64>) -> Self {
        debug_assert!(
            ids.windows(2).all(|window| window[0] < window[1]),
            "sorted buffer cursor requires strictly increasing IDs"
        );
        Self::SortedBuffer {
            ids,
            pos: 0,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        match self {
            Self::MemtableLabel {
                memtable,
                label_id,
                snapshot_seq,
                held,
                last_seek_bound,
                exhausted,
            } => {
                if let Some(head) = *held {
                    if bound <= head {
                        return Ok(Some(head));
                    }
                }
                assert_monotonic_bound(*last_seek_bound, *held, bound);
                *last_seek_bound = Some(bound);
                if *exhausted {
                    return Ok(None);
                }
                match memtable.next_visible_node_by_label_id_ge(*label_id, *snapshot_seq, bound) {
                    Some(node_id) => {
                        *held = Some(node_id);
                        Ok(Some(node_id))
                    }
                    None => {
                        *held = None;
                        *exhausted = true;
                        Ok(None)
                    }
                }
            }
            Self::SegmentLabelPosting {
                reader,
                posting,
                pos,
                held,
                last_seek_bound,
                exhausted,
            } => {
                if let Some(head) = *held {
                    if bound <= head {
                        return Ok(Some(head));
                    }
                }
                assert_monotonic_bound(*last_seek_bound, *held, bound);
                *last_seek_bound = Some(bound);
                if *exhausted {
                    return Ok(None);
                }

                let next = if held.is_some() {
                    seek_segment_label_posting_ge(reader, *posting, *pos, bound)?
                } else {
                    let next_pos = reader.node_label_posting_lower_bound_ge(posting, bound)?;
                    reader
                        .node_id_at_label_posting(*posting, next_pos)?
                        .map(|node_id| (next_pos, node_id))
                };

                match next {
                    Some((next_pos, node_id)) => {
                        *pos = next_pos;
                        *held = Some(node_id);
                        Ok(Some(node_id))
                    }
                    None => {
                        *held = None;
                        *exhausted = true;
                        Ok(None)
                    }
                }
            }
            Self::SegmentEqualityGroup {
                reader,
                index_id,
                offset,
                id_count,
                pos,
                held,
                last_seek_bound,
                exhausted,
            } => {
                if let Some(head) = *held {
                    if bound <= head {
                        return Ok(Some(head));
                    }
                }
                assert_monotonic_bound(*last_seek_bound, *held, bound);
                *last_seek_bound = Some(bound);
                if *exhausted {
                    return Ok(None);
                }
                let from_pos = if held.is_some() {
                    pos.saturating_add(1)
                } else {
                    *pos
                };
                match reader
                    .secondary_eq_posting_seek_ge(*index_id, *offset, *id_count, from_pos, bound)?
                {
                    Some((next_pos, node_id)) => {
                        *pos = next_pos;
                        *held = Some(node_id);
                        Ok(Some(node_id))
                    }
                    None => {
                        *held = None;
                        *exhausted = true;
                        Ok(None)
                    }
                }
            }
            Self::SortedBuffer {
                ids,
                pos,
                held,
                last_seek_bound,
                exhausted,
            } => {
                if let Some(head) = *held {
                    if bound <= head {
                        return Ok(Some(head));
                    }
                }
                assert_monotonic_bound(*last_seek_bound, *held, bound);
                *last_seek_bound = Some(bound);
                if *exhausted {
                    return Ok(None);
                }
                if ids.is_empty() {
                    *exhausted = true;
                    return Ok(None);
                }
                let start = if held.is_some() {
                    pos.saturating_add(1)
                } else {
                    *pos
                };
                if start >= ids.len() {
                    *held = None;
                    *exhausted = true;
                    return Ok(None);
                }
                let relative = ids[start..].partition_point(|&node_id| node_id < bound);
                let next_pos = start + relative;
                if next_pos >= ids.len() {
                    *held = None;
                    *exhausted = true;
                    return Ok(None);
                }
                *pos = next_pos;
                *held = Some(ids[next_pos]);
                Ok(*held)
            }
        }
    }
}

impl CandidateCursorSeek for NodeCandidateCursor {
    fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        NodeCandidateCursor::next_ge(self, bound)
    }
}

#[allow(dead_code)]
pub(crate) struct NodeLeafStream {
    union: NodeStreamSet,
}

#[allow(dead_code)]
impl NodeLeafStream {
    pub(crate) fn new(cursors: Vec<NodeCandidateCursor>) -> Self {
        Self {
            union: NodeStreamSet::new(cursors),
        }
    }

    pub(crate) fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        self.union.next_ge(bound)
    }

    fn into_stream_set(self) -> NodeStreamSet {
        self.union
    }

    fn into_cursors(self) -> Vec<NodeCandidateCursor> {
        self.union.into_cursors()
    }
}

#[allow(dead_code)]
fn seek_segment_label_posting_ge(
    reader: &SegmentReader,
    posting: SegmentLabelPosting,
    from_pos: usize,
    bound: u64,
) -> Result<Option<(usize, u64)>, EngineError> {
    seek_ordered_id_posting_ge(
        |index| reader.node_id_at_label_posting(posting, index),
        from_pos,
        bound,
    )
}

enum NodeStreamBuildResult {
    Ready(NodeLeafStream),
    Demoted {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
    TooBroad {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
}

impl ReadView {
    fn execute_streamed_node_driver(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        plan: &NodePhysicalPlan,
        _hydrate: bool,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<CandidateMaterializationResult, EngineError> {
        let Some((start_bound, end_bound)) = streamed_node_bounds(query) else {
            return Ok(CandidateMaterializationResult::Ready {
                ids: Vec::new(),
                followups: Vec::new(),
            });
        };

        let mut followups = Vec::new();
        let mut streams = Vec::new();
        match plan {
            NodePhysicalPlan::Intersect { inputs, .. } => {
                #[cfg(test)]
                self.note_streamed_intersect_driver();
                for input in inputs {
                    match self.build_node_leaf_stream(query, cap_context, input)? {
                        NodeStreamBuildResult::Ready(stream) => streams.push(stream.into_stream_set()),
                        NodeStreamBuildResult::Demoted {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                        }
                        NodeStreamBuildResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            return Ok(CandidateMaterializationResult::TooBroad { followups });
                        }
                    }
                }
            }
            NodePhysicalPlan::Union { inputs, .. } => {
                #[cfg(test)]
                self.note_streamed_union_driver();
                match self.build_node_union_leaf_stream(query, cap_context, inputs)? {
                    NodeStreamBuildResult::Ready(stream) => {
                        let mut union = stream.into_stream_set();
                        let ids = self.verify_streamed_node_candidates(
                            &mut union,
                            query,
                            start_bound,
                            end_bound,
                            policy_cutoffs,
                        )?;
                        return Ok(CandidateMaterializationResult::Ready { ids, followups });
                    }
                    NodeStreamBuildResult::Demoted {
                        followups: mut input_followups,
                    }
                    | NodeStreamBuildResult::TooBroad {
                        followups: mut input_followups,
                    } => {
                        followups.append(&mut input_followups);
                        return Ok(CandidateMaterializationResult::TooBroad { followups });
                    }
                }
            }
            _ => {
                #[cfg(test)]
                self.note_streamed_single_source_driver();
                match self.build_node_leaf_stream(query, cap_context, plan)? {
                    NodeStreamBuildResult::Ready(stream) => streams.push(stream.into_stream_set()),
                    NodeStreamBuildResult::Demoted {
                        followups: mut input_followups,
                    }
                    | NodeStreamBuildResult::TooBroad {
                        followups: mut input_followups,
                    } => {
                        followups.append(&mut input_followups);
                        return Ok(CandidateMaterializationResult::TooBroad { followups });
                    }
                }
            }
        }

        if streams.is_empty() {
            return Ok(CandidateMaterializationResult::TooBroad { followups });
        }

        let mut intersection = LeapfrogIntersection::new(streams);
        let ids = self.verify_streamed_node_candidates(
            &mut intersection,
            query,
            start_bound,
            end_bound,
            policy_cutoffs,
        )?;
        Ok(CandidateMaterializationResult::Ready { ids, followups })
    }

    fn build_node_leaf_stream(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        plan: &NodePhysicalPlan,
    ) -> Result<NodeStreamBuildResult, EngineError> {
        match plan {
            NodePhysicalPlan::BufferedIdSort(input) => {
                self.build_buffered_node_leaf_stream(query, cap_context, input)
            }
            NodePhysicalPlan::Source(source) => {
                self.build_node_source_stream(query, cap_context, source)
            }
            NodePhysicalPlan::Empty => Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(
                Vec::new(),
            ))),
            NodePhysicalPlan::Union { inputs, .. } => {
                self.build_node_union_leaf_stream(query, cap_context, inputs)
            }
            NodePhysicalPlan::Intersect { .. } => {
                Ok(NodeStreamBuildResult::TooBroad {
                    followups: Vec::new(),
                })
            }
        }
    }

    fn build_node_union_leaf_stream(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        inputs: &[NodePhysicalPlan],
    ) -> Result<NodeStreamBuildResult, EngineError> {
        let mut cursors = Vec::new();
        let mut followups = Vec::new();
        for input in inputs {
            let built = match input {
                NodePhysicalPlan::Union { inputs, .. } => {
                    self.build_node_union_leaf_stream(query, cap_context, inputs)?
                }
                _ => self.build_node_leaf_stream(query, cap_context, input)?,
            };
            match built {
                NodeStreamBuildResult::Ready(stream) => {
                    cursors.extend(stream.into_cursors());
                }
                NodeStreamBuildResult::Demoted {
                    followups: mut input_followups,
                }
                | NodeStreamBuildResult::TooBroad {
                    followups: mut input_followups,
                } => {
                    if input.contains_buffered_stream_input() {
                        #[cfg(test)]
                        self.note_streamed_leaf_demotion();
                    }
                    followups.append(&mut input_followups);
                    return Ok(NodeStreamBuildResult::Demoted { followups });
                }
            }
        }
        Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(cursors)))
    }

    fn build_node_source_stream(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        source: &PlannedNodeCandidateSource,
    ) -> Result<NodeStreamBuildResult, EngineError> {
        match &source.materialization {
            NodeCandidateMaterialization::Precomputed(ids) => Ok(NodeStreamBuildResult::Ready(
                NodeLeafStream::new(vec![NodeCandidateCursor::sorted_buffer(
                    ids.as_ref().clone(),
                )]),
            )),
            NodeCandidateMaterialization::KeyLookup => {
                let ids = self.key_lookup_candidate_ids(query)?;
                Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(vec![
                    NodeCandidateCursor::sorted_buffer(ids),
                ])))
            }
            NodeCandidateMaterialization::NodeLabelIndex { label_id }
            | NodeCandidateMaterialization::FallbackNodeLabelScan { label_id } => {
                self.build_label_node_stream(&[*label_id])
            }
            NodeCandidateMaterialization::NodeLabelAny { label_ids } => {
                self.build_label_node_stream(label_ids.as_slice())
            }
            NodeCandidateMaterialization::PropertyEqualityIndex {
                index_id, value, ..
            } => self.build_equality_node_stream(*index_id, value),
            NodeCandidateMaterialization::PropertyRangeIndex { .. }
            | NodeCandidateMaterialization::TimestampIndex { .. } => {
                self.build_buffered_node_leaf_stream(
                    query,
                    cap_context,
                    &NodePhysicalPlan::Source(source.clone()),
                )
            }
            NodeCandidateMaterialization::CompoundPrefixIndex { .. }
            | NodeCandidateMaterialization::CompoundRangeIndex { .. }
            | NodeCandidateMaterialization::FallbackFullNodeScan => Ok(NodeStreamBuildResult::TooBroad {
                followups: Vec::new(),
            }),
        }
    }

    fn build_label_node_stream(
        &self,
        label_ids: &[u32],
    ) -> Result<NodeStreamBuildResult, EngineError> {
        let mut cursors = Vec::new();
        for &label_id in label_ids {
            cursors.push(NodeCandidateCursor::memtable_label(
                Arc::clone(&self.memtable),
                label_id,
                self.snapshot_seq,
            ));
            for epoch in &self.immutable_epochs {
                cursors.push(NodeCandidateCursor::memtable_label(
                    Arc::clone(&epoch.memtable),
                    label_id,
                    self.snapshot_seq,
                ));
            }
            for segment in &self.segments {
                if let Some(posting) = segment.node_label_posting(label_id)? {
                    cursors.push(NodeCandidateCursor::segment_label_posting(
                        Arc::clone(segment),
                        posting,
                    ));
                }
            }
        }
        Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(cursors)))
    }

    fn build_equality_node_stream(
        &self,
        index_id: u64,
        value: &PropValue,
    ) -> Result<NodeStreamBuildResult, EngineError> {
        let mut cursors = Vec::new();
        let mut followups = Vec::new();
        let value_hashes = equality_probe_value_hashes(value);

        for segment in &self.segments {
            let coverage = self.declared_index_runtime_coverage.state(
                segment.segment_id,
                index_id,
                PlannerStatsDeclaredIndexTarget::NodeProperty,
                crate::planner_stats::PlannerStatsDeclaredIndexKind::Equality,
            );
            if coverage != crate::planner_stats::DeclaredIndexRuntimeCoverageState::Available {
                #[cfg(test)]
                self.note_streamed_leaf_demotion();
                followups.extend(materialization_followups(
                    self.equality_sidecar_failure_followup(index_id, None),
                ));
                return Ok(NodeStreamBuildResult::Demoted { followups });
            }
        }

        for value_hash in value_hashes {
            let ids = self.memtable.visible_secondary_eq_node_ids_sorted(
                index_id,
                value_hash,
                self.snapshot_seq,
            );
            if !ids.is_empty() {
                cursors.push(NodeCandidateCursor::sorted_buffer(ids));
            }
            for epoch in &self.immutable_epochs {
                let ids = epoch.memtable.visible_secondary_eq_node_ids_sorted(
                    index_id,
                    value_hash,
                    self.snapshot_seq,
                );
                if !ids.is_empty() {
                    cursors.push(NodeCandidateCursor::sorted_buffer(ids));
                }
            }
            for segment in &self.segments {
                match segment.secondary_eq_group_span_if_present(index_id, value_hash) {
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::Found(span)) => {
                        cursors.push(NodeCandidateCursor::segment_equality_group(
                            Arc::clone(segment),
                            index_id,
                            span.offset,
                            span.id_count,
                        ));
                    }
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::MissingValueGroup) => {}
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::MissingSidecar) => {
                        #[cfg(test)]
                        self.note_streamed_leaf_demotion();
                        followups.extend(materialization_followups(
                            self.equality_sidecar_failure_followup(index_id, None),
                        ));
                        return Ok(NodeStreamBuildResult::Demoted { followups });
                    }
                    Err(error) => {
                        #[cfg(test)]
                        self.note_streamed_leaf_demotion();
                        followups.extend(materialization_followups(
                            self.equality_sidecar_failure_followup(index_id, Some(error)),
                        ));
                        return Ok(NodeStreamBuildResult::Demoted { followups });
                    }
                }
            }
        }

        Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(cursors)))
    }

    fn build_buffered_node_leaf_stream(
        &self,
        query: &NormalizedNodeQuery,
        cap_context: QueryCapContext,
        plan: &NodePhysicalPlan,
    ) -> Result<NodeStreamBuildResult, EngineError> {
        let Some(source) = plan.as_source() else {
            return Ok(NodeStreamBuildResult::TooBroad {
                followups: Vec::new(),
            });
        };
        let cap = cap_context.source_cap(source.kind, query.page.limit, source.estimate);
        let mut followups = Vec::new();
        let ids = match &source.materialization {
            NodeCandidateMaterialization::PropertyRangeIndex {
                index_id,
                lower,
                upper,
            } => {
                let (ids, followup) =
                    self.ready_range_candidate_ids(*index_id, lower.as_ref(), upper.as_ref(), cap + 1)?;
                followups.extend(materialization_followups(followup));
                let Some(ids) = ids else {
                    #[cfg(test)]
                    self.note_streamed_buffered_input_overflow();
                    return Ok(NodeStreamBuildResult::Demoted { followups });
                };
                ids
            }
            NodeCandidateMaterialization::TimestampIndex {
                label_id,
                lower_ms,
                upper_ms,
            } => self.timestamp_candidate_ids(*label_id, *lower_ms, *upper_ms, cap + 1)?,
            _ => return self.build_node_leaf_stream(query, cap_context, plan),
        };

        if ids.len() > cap {
            #[cfg(test)]
            self.note_streamed_buffered_input_overflow();
            return Ok(NodeStreamBuildResult::Demoted { followups });
        }
        #[cfg(test)]
        self.note_streamed_buffered_input();
        Ok(NodeStreamBuildResult::Ready(NodeLeafStream::new(vec![
            NodeCandidateCursor::sorted_buffer(normalize_candidate_ids(ids)),
        ])))
    }

    fn verify_streamed_node_candidates(
        &self,
        stream: &mut impl CandidateCursorSeek,
        query: &NormalizedNodeQuery,
        start_bound: u64,
        end_bound: u64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<u64>, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let include_key = !query.keys.is_empty() || node_filter_needs_key(&query.filter);
        let include_created_at = node_filter_needs_created_at(&query.filter);
        let mut property_keys = Vec::new();
        collect_node_filter_property_keys(&query.filter, &mut property_keys);
        let mut verified = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut chunk = Vec::with_capacity(QUERY_VERIFY_CHUNK);
        let mut bound = start_bound;

        while bound <= end_bound && verified.len() < target {
            #[cfg(test)]
            self.note_streamed_cursor_seek();
            let Some(candidate) = stream.next_ge(bound)? else {
                break;
            };
            if candidate > end_bound {
                break;
            }
            #[cfg(test)]
            self.note_streamed_candidate_emitted();
            chunk.push(candidate);
            if chunk.len() >= QUERY_VERIFY_CHUNK {
                if self
                    .verify_node_candidate_chunk(
                        &chunk,
                        query,
                        policy_cutoffs,
                        include_key,
                        include_created_at,
                        &property_keys,
                        &mut verified,
                        target,
                    )?
                    .is_break()
                {
                    return Ok(verified);
                }
                chunk.clear();
            }
            if candidate == u64::MAX {
                break;
            }
            bound = candidate + 1;
        }

        if !chunk.is_empty() && verified.len() < target {
            let _ = self.verify_node_candidate_chunk(
                &chunk,
                query,
                policy_cutoffs,
                include_key,
                include_created_at,
                &property_keys,
                &mut verified,
                target,
            )?;
        }
        Ok(verified)
    }
}

fn streamed_node_bounds(query: &NormalizedNodeQuery) -> Option<(u64, u64)> {
    let mut start = match query.page.after {
        Some(u64::MAX) => return None,
        Some(after) => after.saturating_add(1),
        None => 0,
    };
    let mut end = u64::MAX;
    apply_node_id_range_bounds(&query.filter, &mut start, &mut end)?;
    if start > end {
        None
    } else {
        Some((start, end))
    }
}

fn apply_node_id_range_bounds(
    filter: &NormalizedNodeFilter,
    start: &mut u64,
    end: &mut u64,
) -> Option<()> {
    match filter {
        NormalizedNodeFilter::IdRange {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            if let Some(lower) = lower {
                let lower = if *lower_inclusive {
                    *lower
                } else {
                    lower.checked_add(1)?
                };
                *start = (*start).max(lower);
            }
            if let Some(upper) = upper {
                let upper = if *upper_inclusive {
                    *upper
                } else {
                    upper.checked_sub(1)?
                };
                *end = (*end).min(upper);
            }
        }
        NormalizedNodeFilter::And(children) => {
            for child in children {
                apply_node_id_range_bounds(child, start, end)?;
            }
        }
        _ => {}
    }
    Some(())
}

#[cfg(test)]
mod node_stream_tests {
    use super::*;

    fn sorted_cursor(ids: &[u64]) -> NodeCandidateCursor {
        NodeCandidateCursor::sorted_buffer(ids.to_vec())
    }

    fn leaf(ids: &[u64]) -> NodeStreamSet {
        NodeStreamSet::new(vec![sorted_cursor(ids)])
    }

    fn collect_stream_set(mut stream: NodeStreamSet) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(node_id) = stream.next_ge(bound).unwrap() {
            out.push(node_id);
            if node_id == u64::MAX {
                break;
            }
            bound = node_id + 1;
        }
        out
    }

    fn collect_intersection(mut stream: LeapfrogIntersection<NodeCandidateCursor>) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(node_id) = stream.next_ge(bound).unwrap() {
            out.push(node_id);
            if node_id == u64::MAX {
                break;
            }
            bound = node_id + 1;
        }
        out
    }

    #[test]
    fn node_stream_sorted_buffer_cursor_contract() {
        let mut empty = sorted_cursor(&[]);
        assert_eq!(empty.next_ge(0).unwrap(), None);
        assert_eq!(empty.next_ge(u64::MAX).unwrap(), None);

        let mut single = sorted_cursor(&[10]);
        assert_eq!(single.next_ge(0).unwrap(), Some(10));
        assert_eq!(single.next_ge(0).unwrap(), Some(10));
        assert_eq!(single.next_ge(10).unwrap(), Some(10));
        assert_eq!(single.next_ge(11).unwrap(), None);

        let mut cursor = sorted_cursor(&[0, 5, 10, 20, u64::MAX]);
        assert_eq!(cursor.next_ge(0).unwrap(), Some(0));
        assert_eq!(cursor.next_ge(1).unwrap(), Some(5));
        assert_eq!(cursor.next_ge(5).unwrap(), Some(5));
        assert_eq!(cursor.next_ge(6).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(20).unwrap(), Some(20));
        assert_eq!(cursor.next_ge(21).unwrap(), Some(u64::MAX));
        assert_eq!(cursor.next_ge(u64::MAX).unwrap(), Some(u64::MAX));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "next_ge bounds must be non-decreasing")]
    fn node_stream_sorted_buffer_debug_asserts_decreasing_bound_after_terminal_seek() {
        let mut cursor = sorted_cursor(&[10, 20]);
        assert_eq!(cursor.next_ge(10).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(25).unwrap(), None);
        let _ = cursor.next_ge(24);
    }

    #[test]
    fn node_stream_memtable_label_cursor_includes_bound_and_skips_invisible_slots() {
        let memtable = std::sync::Arc::new(Memtable::new());
        memtable.apply_op(&WalOp::UpsertNode(test_node(1, &[7], "a", 100)), 1);
        memtable.apply_op(&WalOp::UpsertNode(test_node(3, &[7], "b", 100)), 2);
        memtable.apply_op(&WalOp::UpsertNode(test_node(5, &[7], "c", 100)), 3);
        memtable.apply_op(&WalOp::UpsertNode(test_node(3, &[8], "b", 200)), 4);

        let mut cursor = NodeCandidateCursor::memtable_label(memtable.clone(), 7, 3);
        assert_eq!(cursor.next_ge(3).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(3).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(4).unwrap(), Some(5));

        let mut later = NodeCandidateCursor::memtable_label(memtable, 7, 4);
        assert_eq!(later.next_ge(2).unwrap(), Some(5));
    }

    #[test]
    fn node_stream_memtable_label_cursor_contract_edges() {
        let empty = std::sync::Arc::new(Memtable::new());
        let mut empty_cursor = NodeCandidateCursor::memtable_label(empty, 7, 0);
        assert_eq!(empty_cursor.next_ge(0).unwrap(), None);
        assert_eq!(empty_cursor.next_ge(u64::MAX).unwrap(), None);

        let memtable = std::sync::Arc::new(Memtable::new());
        memtable.apply_op(&WalOp::UpsertNode(test_node(u64::MAX, &[7], "max", 100)), 1);
        let mut max_cursor = NodeCandidateCursor::memtable_label(memtable, 7, 1);
        assert_eq!(max_cursor.next_ge(0).unwrap(), Some(u64::MAX));
        assert_eq!(max_cursor.next_ge(u64::MAX).unwrap(), Some(u64::MAX));
    }

    #[test]
    fn node_stream_segment_label_cursor_uses_inclusive_galloping_seek() {
        let memtable = Memtable::new();
        for id in [1, 3, 5, 8, 13, 21] {
            memtable.apply_op(
                &WalOp::UpsertNode(test_node(id, &[4], &format!("n{id}"), 1)),
                id,
            );
        }
        let (_dir, reader) = write_test_segment(&memtable, &[]);
        let reader = std::sync::Arc::new(reader);
        let posting = reader.node_label_posting(4).unwrap().unwrap();
        let mut cursor = NodeCandidateCursor::segment_label_posting(reader, posting);

        assert_eq!(cursor.next_ge(0).unwrap(), Some(1));
        assert_eq!(cursor.next_ge(1).unwrap(), Some(1));
        assert_eq!(cursor.next_ge(2).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(9).unwrap(), Some(13));
        assert_eq!(cursor.next_ge(22).unwrap(), None);
    }

    #[test]
    fn node_stream_segment_label_cursor_contract_edges_and_binary_equivalence() {
        let memtable = Memtable::new();
        for (seq, (id, labels, key)) in [
            (0, vec![4], "zero"),
            (42, vec![5], "single"),
            (u64::MAX, vec![4], "max"),
        ]
        .into_iter()
        .enumerate()
        {
            memtable.apply_op(
                &WalOp::UpsertNode(test_node(id, &labels, key, 1)),
                seq as u64 + 1,
            );
        }
        let (_dir, reader) = write_test_segment(&memtable, &[]);
        let reader = std::sync::Arc::new(reader);

        let posting = reader.node_label_posting(4).unwrap().unwrap();
        let mut cursor = NodeCandidateCursor::segment_label_posting(reader.clone(), posting);
        assert_eq!(cursor.next_ge(0).unwrap(), Some(0));
        assert_eq!(cursor.next_ge(0).unwrap(), Some(0));
        assert_eq!(cursor.next_ge(1).unwrap(), Some(u64::MAX));
        assert_eq!(cursor.next_ge(u64::MAX).unwrap(), Some(u64::MAX));

        let posting = reader.node_label_posting(5).unwrap().unwrap();
        let mut single = NodeCandidateCursor::segment_label_posting(reader.clone(), posting);
        assert_eq!(single.next_ge(0).unwrap(), Some(42));
        assert_eq!(single.next_ge(42).unwrap(), Some(42));
        assert_eq!(single.next_ge(43).unwrap(), None);

        let generated = Memtable::new();
        let mut expected_ids = Vec::new();
        let mut next_id = 0u64;
        for n in 0..128u64 {
            next_id += 1 + (n % 7);
            expected_ids.push(next_id);
            generated.apply_op(
                &WalOp::UpsertNode(test_node(next_id, &[9], &format!("g{next_id}"), 1)),
                n + 1,
            );
        }
        let (_dir, reader) = write_test_segment(&generated, &[]);
        let reader = std::sync::Arc::new(reader);
        let posting = reader.node_label_posting(9).unwrap().unwrap();
        let mut cursor = NodeCandidateCursor::segment_label_posting(reader, posting);
        let mut expected_pos = 0usize;
        let mut bound = 0u64;
        for step in 0..300u64 {
            bound = bound.saturating_add((step.wrapping_mul(17) % 11) + 1);
            let relative = expected_ids[expected_pos..].partition_point(|&node_id| node_id < bound);
            expected_pos += relative;
            let got = cursor.next_ge(bound).unwrap();
            if expected_pos >= expected_ids.len() {
                assert_eq!(got, None, "bound {bound}");
                break;
            }
            assert_eq!(got, Some(expected_ids[expected_pos]), "bound {bound}");
        }
    }

    #[test]
    fn node_stream_segment_equality_cursor_seeks_raw_group() {
        let entry = equality_entry(44, 9, "color");
        let memtable = Memtable::new();
        memtable.register_secondary_index(&entry);
        for (seq, (id, color)) in [
            (2, "red"),
            (5, "red"),
            (9, "blue"),
            (12, "red"),
            (u64::MAX, "red"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut props = BTreeMap::new();
            props.insert("color".to_string(), PropValue::String(color.to_string()));
            memtable.apply_op(
                &WalOp::UpsertNode(test_node_with_props(id, &[9], &format!("n{id}"), props)),
                seq as u64 + 1,
            );
        }
        let (_dir, reader) = write_test_segment(&memtable, std::slice::from_ref(&entry));
        let reader = std::sync::Arc::new(reader);
        let red_hash = hash_prop_equality_key(&PropValue::String("red".to_string()));
        let crate::segment_reader::SecondaryEqGroupSpanLookup::Found(span) = reader
            .secondary_eq_group_span_if_present(entry.index_id, red_hash)
            .unwrap()
        else {
            panic!("red group must exist");
        };
        let mut empty = NodeCandidateCursor::segment_equality_group(
            reader.clone(),
            entry.index_id,
            span.offset,
            0,
        );
        assert_eq!(empty.next_ge(0).unwrap(), None);

        let mut cursor = NodeCandidateCursor::segment_equality_group(
            reader,
            entry.index_id,
            span.offset,
            span.id_count,
        );

        assert_eq!(cursor.next_ge(0).unwrap(), Some(2));
        assert_eq!(cursor.next_ge(2).unwrap(), Some(2));
        assert_eq!(cursor.next_ge(3).unwrap(), Some(5));
        assert_eq!(cursor.next_ge(6).unwrap(), Some(12));
        assert_eq!(cursor.next_ge(13).unwrap(), Some(u64::MAX));
        assert_eq!(cursor.next_ge(u64::MAX).unwrap(), Some(u64::MAX));
    }

    #[test]
    fn node_stream_leaf_stream_dedups_duplicate_cursor_heads() {
        let mut stream = NodeLeafStream::new(vec![
            sorted_cursor(&[1, 3, 5, 9]),
            sorted_cursor(&[1, 2, 5, 8]),
            sorted_cursor(&[5, 7, 9]),
        ]);

        assert_eq!(stream.next_ge(0).unwrap(), Some(1));
        assert_eq!(stream.next_ge(1).unwrap(), Some(1));
        assert_eq!(stream.next_ge(2).unwrap(), Some(2));
        assert_eq!(stream.next_ge(3).unwrap(), Some(3));
        assert_eq!(stream.next_ge(4).unwrap(), Some(5));
        assert_eq!(stream.next_ge(6).unwrap(), Some(7));
        assert_eq!(stream.next_ge(8).unwrap(), Some(8));
        assert_eq!(stream.next_ge(9).unwrap(), Some(9));
        assert_eq!(stream.next_ge(10).unwrap(), None);
    }

    #[test]
    fn node_stream_union_linear_path_dedups_and_seeks_through() {
        let stream = NodeStreamSet::new(vec![
            sorted_cursor(&[1, 4, 7]),
            sorted_cursor(&[2, 4, 8]),
            sorted_cursor(&[4, 9]),
        ]);

        assert_eq!(collect_stream_set(stream), vec![1, 2, 4, 7, 8, 9]);
    }

    #[test]
    fn node_stream_union_heap_path_dedups_and_seeks_through() {
        let mut cursors = Vec::new();
        for i in 0..9 {
            cursors.push(sorted_cursor(&[i, 10, 20 + i]));
        }
        let mut stream = NodeStreamSet::new(cursors);

        assert_eq!(stream.next_ge(0).unwrap(), Some(0));
        assert_eq!(stream.next_ge(5).unwrap(), Some(5));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(11).unwrap(), Some(20));
        assert_eq!(stream.next_ge(30).unwrap(), None);
    }

    #[test]
    fn node_stream_leapfrog_handles_k_one_two_three_and_sixteen() {
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![leaf(&[1, 3, 5])])),
            vec![1, 3, 5]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 3, 5, 8]),
                leaf(&[2, 3, 8, 13]),
            ])),
            vec![3, 8]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 5, 9, 13]),
                leaf(&[3, 5, 11, 13]),
                leaf(&[5, 7, 13]),
            ])),
            vec![5, 13]
        );

        let mut many = Vec::new();
        for i in 0..16 {
            many.push(leaf(&[i, 50, 100 + i]));
        }
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(many)),
            vec![50]
        );
    }

    #[test]
    fn node_stream_leapfrog_handles_empty_one_id_head_and_tail_boundaries() {
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![leaf(&[1, 2, 3]), leaf(&[])],)),
            Vec::<u64>::new()
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[7]),
                leaf(&[1, 7, 9]),
                leaf(&[7, 10]),
            ])),
            vec![7]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 3, 9]),
                leaf(&[1, 2, 9]),
            ])),
            vec![1, 9]
        );
    }

    #[test]
    fn node_stream_leapfrog_retry_reobserves_held_head() {
        let mut stream = LeapfrogIntersection::new(vec![leaf(&[5, 10]), leaf(&[10])]);
        assert_eq!(stream.next_ge(0).unwrap(), Some(10));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(11).unwrap(), None);
    }

    fn test_node(id: u64, label_ids: &[u32], key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::from_canonical_ids(label_ids).unwrap(),
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn test_node_with_props(
        id: u64,
        label_ids: &[u32],
        key: &str,
        props: BTreeMap<String, PropValue>,
    ) -> NodeRecord {
        NodeRecord {
            props,
            ..test_node(id, label_ids, key, 1000)
        }
    }

    fn equality_entry(index_id: u64, label_id: u32, prop_key: &str) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeProperty {
                label_id,
                prop_key: prop_key.to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn write_test_segment(
        memtable: &Memtable,
        secondary_indexes: &[SecondaryIndexManifestEntry],
    ) -> (tempfile::TempDir, SegmentReader) {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let info =
            crate::segment_writer::write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
                &seg_dir,
                1,
                memtable,
                None,
                secondary_indexes,
            )
            .unwrap();
        let reader =
            SegmentReader::open_with_info(&seg_dir, &info, None, secondary_indexes).unwrap();
        (dir, reader)
    }
}
