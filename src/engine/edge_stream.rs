#[allow(dead_code)]
pub(crate) type EdgeStreamSet = StreamSet<EdgeCandidateCursor>;

#[allow(dead_code)]
pub(crate) enum EdgeCandidateCursor {
    MemtableLabel {
        memtable: Arc<Memtable>,
        label_id: u32,
        snapshot_seq: u64,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SegmentLabelPosting {
        reader: Arc<SegmentReader>,
        posting: SegmentLabelPosting,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SegmentEqualityGroup {
        reader: Arc<SegmentReader>,
        index_id: u64,
        offset: usize,
        id_count: usize,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    MemtableAdjacency {
        memtable: Arc<Memtable>,
        node_id: u64,
        outgoing: bool,
        label_filter_id: Option<u32>,
        snapshot_seq: u64,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    },
    SegmentAdjacency {
        reader: Arc<SegmentReader>,
        cursor: crate::segment_reader::SegmentAdjPostingCursor,
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
impl EdgeCandidateCursor {
    pub(crate) fn memtable_label(
        memtable: Arc<Memtable>,
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
        reader: Arc<SegmentReader>,
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
        reader: Arc<SegmentReader>,
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

    pub(crate) fn memtable_adjacency(
        memtable: Arc<Memtable>,
        node_id: u64,
        outgoing: bool,
        label_filter_id: Option<u32>,
        snapshot_seq: u64,
    ) -> Self {
        Self::MemtableAdjacency {
            memtable,
            node_id,
            outgoing,
            label_filter_id,
            snapshot_seq,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn segment_adjacency(
        reader: Arc<SegmentReader>,
        cursor: crate::segment_reader::SegmentAdjPostingCursor,
    ) -> Self {
        Self::SegmentAdjacency {
            reader,
            cursor,
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
                match memtable.next_visible_edge_by_label_id_ge(*label_id, *snapshot_seq, bound) {
                    Some(edge_id) => {
                        *held = Some(edge_id);
                        Ok(Some(edge_id))
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
                    seek_ordered_id_posting_ge(
                        |index| reader.edge_label_id_at_posting(*posting, index),
                        *pos,
                        bound,
                    )?
                } else {
                    let next_pos = reader.edge_label_posting_lower_bound_ge(posting, bound)?;
                    reader
                        .edge_label_id_at_posting(*posting, next_pos)?
                        .map(|edge_id| (next_pos, edge_id))
                };

                match next {
                    Some((next_pos, edge_id)) => {
                        *pos = next_pos;
                        *held = Some(edge_id);
                        Ok(Some(edge_id))
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
                match reader.secondary_eq_posting_seek_ge_for_target(
                    *index_id,
                    PlannerStatsDeclaredIndexTarget::EdgeProperty,
                    *offset,
                    *id_count,
                    from_pos,
                    bound,
                )? {
                    Some((next_pos, edge_id)) => {
                        *pos = next_pos;
                        *held = Some(edge_id);
                        Ok(Some(edge_id))
                    }
                    None => {
                        *held = None;
                        *exhausted = true;
                        Ok(None)
                    }
                }
            }
            Self::MemtableAdjacency {
                memtable,
                node_id,
                outgoing,
                label_filter_id,
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
                match memtable.next_visible_adj_edge_ge(
                    *node_id,
                    *outgoing,
                    *label_filter_id,
                    *snapshot_seq,
                    bound,
                ) {
                    Some(edge_id) => {
                        *held = Some(edge_id);
                        Ok(Some(edge_id))
                    }
                    None => {
                        *held = None;
                        *exhausted = true;
                        Ok(None)
                    }
                }
            }
            Self::SegmentAdjacency {
                reader,
                cursor,
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
                loop {
                    match reader.next_adj_posting_edge_id(cursor)? {
                        Some(edge_id) if edge_id >= bound => {
                            *held = Some(edge_id);
                            return Ok(Some(edge_id));
                        }
                        Some(_) => continue,
                        None => {
                            *held = None;
                            *exhausted = true;
                            return Ok(None);
                        }
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
                let relative = ids[start..].partition_point(|&edge_id| edge_id < bound);
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

impl CandidateCursorSeek for EdgeCandidateCursor {
    fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        EdgeCandidateCursor::next_ge(self, bound)
    }
}

#[allow(dead_code)]
pub(crate) struct EdgeLeafStream {
    union: EdgeStreamSet,
}

#[allow(dead_code)]
impl EdgeLeafStream {
    pub(crate) fn new(cursors: Vec<EdgeCandidateCursor>) -> Self {
        Self {
            union: EdgeStreamSet::new(cursors),
        }
    }

    pub(crate) fn sorted_buffer(ids: Vec<u64>) -> Self {
        Self::new(vec![EdgeCandidateCursor::sorted_buffer(normalize_candidate_ids(ids))])
    }

    pub(crate) fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        self.union.next_ge(bound)
    }

    fn into_stream_set(self) -> EdgeStreamSet {
        self.union
    }

    fn into_cursors(self) -> Vec<EdgeCandidateCursor> {
        self.union.into_cursors()
    }
}

#[allow(dead_code)]
enum EdgeStreamBuildResult {
    Ready(EdgeLeafStream),
    Demoted {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
    TooBroad {
        followups: Vec<SecondaryIndexReadFollowup>,
    },
}

#[allow(dead_code)]
impl ReadView {
    fn build_label_edge_stream(&self, label_id: u32) -> Result<EdgeLeafStream, EngineError> {
        let mut cursors = Vec::new();
        cursors.push(EdgeCandidateCursor::memtable_label(
            Arc::clone(&self.memtable),
            label_id,
            self.snapshot_seq,
        ));
        for epoch in &self.immutable_epochs {
            cursors.push(EdgeCandidateCursor::memtable_label(
                Arc::clone(&epoch.memtable),
                label_id,
                self.snapshot_seq,
            ));
        }
        for segment in &self.segments {
            if let Some(posting) = segment.edge_label_posting(label_id)? {
                cursors.push(EdgeCandidateCursor::segment_label_posting(
                    Arc::clone(segment),
                    posting,
                ));
            }
        }
        Ok(EdgeLeafStream::new(cursors))
    }

    fn build_equality_edge_stream(
        &self,
        index_id: u64,
        value_hashes: &[u64],
    ) -> Result<EdgeStreamBuildResult, EngineError> {
        let mut cursors = Vec::new();
        let mut followups = Vec::new();

        for segment in &self.segments {
            let coverage = self.declared_index_runtime_coverage.state(
                segment.segment_id,
                index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                crate::planner_stats::PlannerStatsDeclaredIndexKind::Equality,
            );
            if coverage != crate::planner_stats::DeclaredIndexRuntimeCoverageState::Available {
                #[cfg(test)]
                self.note_streamed_edge_leaf_demotion();
                followups.extend(materialization_followups(
                    self.equality_sidecar_failure_followup(index_id, None),
                ));
                return Ok(EdgeStreamBuildResult::Demoted { followups });
            }
        }

        for &value_hash in value_hashes {
            let ids = self.memtable.visible_secondary_eq_edge_ids_sorted(
                index_id,
                value_hash,
                self.snapshot_seq,
            );
            if !ids.is_empty() {
                cursors.push(EdgeCandidateCursor::sorted_buffer(ids));
            }
            for epoch in &self.immutable_epochs {
                let ids = epoch.memtable.visible_secondary_eq_edge_ids_sorted(
                    index_id,
                    value_hash,
                    self.snapshot_seq,
                );
                if !ids.is_empty() {
                    cursors.push(EdgeCandidateCursor::sorted_buffer(ids));
                }
            }
            for segment in &self.segments {
                match segment.secondary_eq_group_span_if_present_for_target(
                    index_id,
                    PlannerStatsDeclaredIndexTarget::EdgeProperty,
                    value_hash,
                ) {
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::Found(span)) => {
                        cursors.push(EdgeCandidateCursor::segment_equality_group(
                            Arc::clone(segment),
                            index_id,
                            span.offset,
                            span.id_count,
                        ));
                    }
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::MissingValueGroup) => {}
                    Ok(crate::segment_reader::SecondaryEqGroupSpanLookup::MissingSidecar) => {
                        #[cfg(test)]
                        self.note_streamed_edge_leaf_demotion();
                        followups.extend(materialization_followups(
                            self.equality_sidecar_failure_followup(index_id, None),
                        ));
                        return Ok(EdgeStreamBuildResult::Demoted { followups });
                    }
                    Err(error) => {
                        #[cfg(test)]
                        self.note_streamed_edge_leaf_demotion();
                        followups.extend(materialization_followups(
                            self.equality_sidecar_failure_followup(index_id, Some(error)),
                        ));
                        return Ok(EdgeStreamBuildResult::Demoted { followups });
                    }
                }
            }
        }

        Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(cursors)))
    }

    fn build_endpoint_edge_stream(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_id: Option<u32>,
    ) -> Result<EdgeLeafStream, EngineError> {
        let mut cursors = Vec::new();
        push_memtable_endpoint_cursors(
            &mut cursors,
            Arc::clone(&self.memtable),
            node_ids,
            direction,
            label_filter_id,
            self.snapshot_seq,
        );
        for epoch in &self.immutable_epochs {
            push_memtable_endpoint_cursors(
                &mut cursors,
                Arc::clone(&epoch.memtable),
                node_ids,
                direction,
                label_filter_id,
                self.snapshot_seq,
            );
        }
        for segment in &self.segments {
            let segment_cursors = match label_filter_id {
                Some(label_id) => {
                    let labels = [label_id];
                    segment.endpoint_adj_posting_cursors(node_ids, direction, Some(&labels))?
                }
                None => segment.endpoint_adj_posting_cursors(node_ids, direction, None)?,
            };
            cursors.extend(segment_cursors.into_iter().map(|cursor| {
                EdgeCandidateCursor::segment_adjacency(Arc::clone(segment), cursor)
            }));
        }
        Ok(EdgeLeafStream::new(cursors))
    }

    fn build_explicit_edge_stream(&self, ids: Vec<u64>) -> EdgeLeafStream {
        EdgeLeafStream::sorted_buffer(ids)
    }

    fn build_triple_edge_stream(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<EdgeLeafStream, EngineError> {
        Ok(EdgeLeafStream::sorted_buffer(
            self.sources().edge_ids_by_triple(from, to, label_id)?,
        ))
    }

    fn execute_streamed_edge_driver(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        plan: &EdgePhysicalPlan,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<VerifiedEdgeMetadataMaterializationResult, EngineError> {
        let Some((start_bound, end_bound)) = streamed_edge_bounds(query) else {
            return Ok(VerifiedEdgeMetadataMaterializationResult::Ready {
                metadata: Vec::new(),
                followups: Vec::new(),
            });
        };

        let mut followups = Vec::new();
        let mut streams = Vec::new();
        match plan {
            EdgePhysicalPlan::Intersect { inputs, .. } => {
                #[cfg(test)]
                self.note_streamed_edge_intersect_driver();
                for input in inputs {
                    match self.build_edge_leaf_stream(query, cap_context, input)? {
                        EdgeStreamBuildResult::Ready(stream) => {
                            streams.push(stream.into_stream_set())
                        }
                        EdgeStreamBuildResult::Demoted {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                        }
                        EdgeStreamBuildResult::TooBroad {
                            followups: mut input_followups,
                        } => {
                            followups.append(&mut input_followups);
                            return Ok(VerifiedEdgeMetadataMaterializationResult::TooBroad {
                                followups,
                            });
                        }
                    }
                }
            }
            EdgePhysicalPlan::Union { inputs, .. } => {
                #[cfg(test)]
                self.note_streamed_edge_union_driver();
                match self.build_edge_union_leaf_stream(query, cap_context, inputs)? {
                    EdgeStreamBuildResult::Ready(stream) => {
                        let mut union = stream.into_stream_set();
                        let metadata = self.verify_streamed_edge_candidates(
                            &mut union,
                            query,
                            start_bound,
                            end_bound,
                            policy_cutoffs,
                        )?;
                        return Ok(VerifiedEdgeMetadataMaterializationResult::Ready {
                            metadata,
                            followups,
                        });
                    }
                    EdgeStreamBuildResult::Demoted {
                        followups: mut input_followups,
                    }
                    | EdgeStreamBuildResult::TooBroad {
                        followups: mut input_followups,
                    } => {
                        followups.append(&mut input_followups);
                        return Ok(VerifiedEdgeMetadataMaterializationResult::TooBroad {
                            followups,
                        });
                    }
                }
            }
            _ => {
                #[cfg(test)]
                self.note_streamed_edge_single_source_driver();
                match self.build_edge_leaf_stream(query, cap_context, plan)? {
                    EdgeStreamBuildResult::Ready(stream) => streams.push(stream.into_stream_set()),
                    EdgeStreamBuildResult::Demoted {
                        followups: mut input_followups,
                    }
                    | EdgeStreamBuildResult::TooBroad {
                        followups: mut input_followups,
                    } => {
                        followups.append(&mut input_followups);
                        return Ok(VerifiedEdgeMetadataMaterializationResult::TooBroad {
                            followups,
                        });
                    }
                }
            }
        }

        if streams.is_empty() {
            return Ok(VerifiedEdgeMetadataMaterializationResult::TooBroad { followups });
        }

        let mut intersection = LeapfrogIntersection::new(streams);
        let metadata = self.verify_streamed_edge_candidates(
            &mut intersection,
            query,
            start_bound,
            end_bound,
            policy_cutoffs,
        )?;
        Ok(VerifiedEdgeMetadataMaterializationResult::Ready {
            metadata,
            followups,
        })
    }

    fn build_edge_leaf_stream(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        plan: &EdgePhysicalPlan,
    ) -> Result<EdgeStreamBuildResult, EngineError> {
        match plan {
            EdgePhysicalPlan::BufferedIdSort(input) => {
                self.build_buffered_edge_leaf_stream(query, cap_context, input)
            }
            EdgePhysicalPlan::Source(source) => {
                self.build_edge_source_stream(query, cap_context, source)
            }
            EdgePhysicalPlan::Empty => Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(
                Vec::new(),
            ))),
            EdgePhysicalPlan::Union { inputs, .. } => {
                self.build_edge_union_leaf_stream(query, cap_context, inputs)
            }
            EdgePhysicalPlan::Intersect { .. } => {
                Ok(EdgeStreamBuildResult::TooBroad {
                    followups: Vec::new(),
                })
            }
        }
    }

    fn build_edge_union_leaf_stream(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        inputs: &[EdgePhysicalPlan],
    ) -> Result<EdgeStreamBuildResult, EngineError> {
        let mut cursors = Vec::new();
        let mut followups = Vec::new();
        for input in inputs {
            let built = match input {
                EdgePhysicalPlan::Union { inputs, .. } => {
                    self.build_edge_union_leaf_stream(query, cap_context, inputs)?
                }
                _ => self.build_edge_leaf_stream(query, cap_context, input)?,
            };
            match built {
                EdgeStreamBuildResult::Ready(stream) => {
                    cursors.extend(stream.into_cursors());
                }
                EdgeStreamBuildResult::Demoted {
                    followups: mut input_followups,
                }
                | EdgeStreamBuildResult::TooBroad {
                    followups: mut input_followups,
                } => {
                    followups.append(&mut input_followups);
                    return Ok(EdgeStreamBuildResult::Demoted { followups });
                }
            }
        }
        Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(cursors)))
    }

    fn build_edge_source_stream(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        source: &PlannedEdgeCandidateSource,
    ) -> Result<EdgeStreamBuildResult, EngineError> {
        match &source.materialization {
            EdgeCandidateMaterialization::Precomputed(ids) => Ok(EdgeStreamBuildResult::Ready(
                EdgeLeafStream::new(vec![EdgeCandidateCursor::sorted_buffer(
                    ids.as_ref().clone(),
                )]),
            )),
            EdgeCandidateMaterialization::EdgeLabelIndex { label_id } => Ok(
                EdgeStreamBuildResult::Ready(self.build_label_edge_stream(*label_id)?),
            ),
            EdgeCandidateMaterialization::EdgeTripleIndex { from, to, label_id } => Ok(
                EdgeStreamBuildResult::Ready(self.build_triple_edge_stream(*from, *to, *label_id)?),
            ),
            EdgeCandidateMaterialization::EdgePropertyEqualityIndex {
                index_id,
                value_hashes,
                ..
            } => self.build_equality_edge_stream(*index_id, value_hashes),
            EdgeCandidateMaterialization::EdgeWeightIndex { .. }
            | EdgeCandidateMaterialization::EdgeUpdatedAtIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidFromIndex { .. }
            | EdgeCandidateMaterialization::EdgeValidToIndex { .. }
            | EdgeCandidateMaterialization::EdgePropertyRangeIndex { .. } => {
                self.build_buffered_edge_leaf_stream(
                    query,
                    cap_context,
                    &EdgePhysicalPlan::Source(source.clone()),
                )
            }
            EdgeCandidateMaterialization::FromEndpointAdjacency {
                node_ids,
                label_filter_ids,
                ..
            } => {
                let Some(label_filter) = endpoint_stream_label_filter(label_filter_ids.as_deref())
                else {
                    return Ok(EdgeStreamBuildResult::TooBroad {
                        followups: Vec::new(),
                    });
                };
                let EndpointStreamLabelFilter::Single(label_filter_id) = label_filter else {
                    return Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(Vec::new())));
                };
                Ok(EdgeStreamBuildResult::Ready(self.build_endpoint_edge_stream(
                    node_ids,
                    Direction::Outgoing,
                    label_filter_id,
                )?))
            }
            EdgeCandidateMaterialization::ToEndpointAdjacency {
                node_ids,
                label_filter_ids,
                ..
            } => {
                let Some(label_filter) = endpoint_stream_label_filter(label_filter_ids.as_deref())
                else {
                    return Ok(EdgeStreamBuildResult::TooBroad {
                        followups: Vec::new(),
                    });
                };
                let EndpointStreamLabelFilter::Single(label_filter_id) = label_filter else {
                    return Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(Vec::new())));
                };
                Ok(EdgeStreamBuildResult::Ready(self.build_endpoint_edge_stream(
                    node_ids,
                    Direction::Incoming,
                    label_filter_id,
                )?))
            }
            EdgeCandidateMaterialization::AnyEndpointAdjacency {
                node_ids,
                label_filter_ids,
                ..
            } => {
                let Some(label_filter) = endpoint_stream_label_filter(label_filter_ids.as_deref())
                else {
                    return Ok(EdgeStreamBuildResult::TooBroad {
                        followups: Vec::new(),
                    });
                };
                let EndpointStreamLabelFilter::Single(label_filter_id) = label_filter else {
                    return Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(Vec::new())));
                };
                Ok(EdgeStreamBuildResult::Ready(self.build_endpoint_edge_stream(
                    node_ids,
                    Direction::Both,
                    label_filter_id,
                )?))
            }
            EdgeCandidateMaterialization::CompoundPrefixIndex { .. }
            | EdgeCandidateMaterialization::CompoundRangeIndex { .. }
            | EdgeCandidateMaterialization::FallbackFullEdgeScan => Ok(EdgeStreamBuildResult::TooBroad {
                followups: Vec::new(),
            }),
        }
    }

    fn build_buffered_edge_leaf_stream(
        &self,
        query: &NormalizedEdgeQuery,
        cap_context: EdgeQueryCapContext,
        plan: &EdgePhysicalPlan,
    ) -> Result<EdgeStreamBuildResult, EngineError> {
        let Some(source) = plan.as_source() else {
            return Ok(EdgeStreamBuildResult::TooBroad {
                followups: Vec::new(),
            });
        };
        let cap = cap_context.source_cap(source.kind, query.page.limit, source.estimate);
        let mut followups = Vec::new();
        let ids = match &source.materialization {
            EdgeCandidateMaterialization::EdgePropertyRangeIndex {
                index_id,
                lower,
                upper,
                ..
            } => {
                let (ids, followup) = self.ready_edge_range_candidate_ids(
                    *index_id,
                    lower.as_ref(),
                    upper.as_ref(),
                    cap.saturating_add(1),
                )?;
                followups.extend(materialization_followups(followup));
                let Some(ids) = ids else {
                    #[cfg(test)]
                    self.note_streamed_edge_leaf_demotion();
                    return Ok(EdgeStreamBuildResult::Demoted { followups });
                };
                ids
            }
            EdgeCandidateMaterialization::EdgeWeightIndex { label_id, bounds } => self
                .sources()
                .edge_ids_by_weight_range_limited(*label_id, *bounds, cap.saturating_add(1))?,
            EdgeCandidateMaterialization::EdgeUpdatedAtIndex { label_id, bounds } => self
                .sources()
                .edge_ids_by_updated_at_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )?,
            EdgeCandidateMaterialization::EdgeValidFromIndex { label_id, bounds } => self
                .sources()
                .edge_ids_by_valid_from_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )?,
            EdgeCandidateMaterialization::EdgeValidToIndex { label_id, bounds } => self
                .sources()
                .edge_ids_by_valid_to_range_limited(
                    *label_id,
                    *bounds,
                    cap.saturating_add(1),
                )?,
            _ => return self.build_edge_leaf_stream(query, cap_context, plan),
        };

        if ids.len() > cap {
            #[cfg(test)]
            self.note_streamed_edge_buffered_input_overflow();
            return Ok(EdgeStreamBuildResult::Demoted { followups });
        }
        #[cfg(test)]
        self.note_streamed_edge_buffered_input();
        Ok(EdgeStreamBuildResult::Ready(EdgeLeafStream::new(vec![
            EdgeCandidateCursor::sorted_buffer(normalize_candidate_ids(ids)),
        ])))
    }

    fn verify_streamed_edge_candidates(
        &self,
        stream: &mut impl CandidateCursorSeek,
        query: &NormalizedEdgeQuery,
        start_bound: u64,
        end_bound: u64,
        policy_cutoffs: Option<&PrecomputedPruneCutoffs>,
    ) -> Result<Vec<EdgeMetadataForQuery>, EngineError> {
        let limit = page_limit(&query.page);
        let target = page_verify_target(limit);
        let mut verified = Vec::with_capacity(if limit > 0 { limit } else { 0 });
        let mut endpoint_cache = EdgeEndpointVisibilityCache::default();
        let mut chunk = Vec::with_capacity(QUERY_VERIFY_CHUNK);
        let mut bound = start_bound;

        while bound <= end_bound && verified.len() < target {
            #[cfg(test)]
            self.note_streamed_edge_cursor_seek();
            let Some(candidate) = stream.next_ge(bound)? else {
                break;
            };
            if candidate > end_bound {
                break;
            }
            #[cfg(test)]
            self.note_streamed_edge_candidate_emitted();
            chunk.push(candidate);
            if chunk.len() >= QUERY_VERIFY_CHUNK {
                if self
                    .verify_edge_candidate_chunk(
                        &chunk,
                        query,
                        policy_cutoffs,
                        &mut endpoint_cache,
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
            let _ = self.verify_edge_candidate_chunk(
                &chunk,
                query,
                policy_cutoffs,
                &mut endpoint_cache,
                &mut verified,
                target,
            )?;
        }
        Ok(verified)
    }
}

fn streamed_edge_bounds(query: &NormalizedEdgeQuery) -> Option<(u64, u64)> {
    let mut start = match query.page.after {
        Some(u64::MAX) => return None,
        Some(after) => after.saturating_add(1),
        None => 0,
    };
    let mut end = u64::MAX;
    apply_edge_id_range_bounds(&query.filter, &mut start, &mut end)?;
    if start > end {
        None
    } else {
        Some((start, end))
    }
}

fn apply_edge_id_range_bounds(
    filter: &NormalizedEdgeFilter,
    start: &mut u64,
    end: &mut u64,
) -> Option<()> {
    match filter {
        NormalizedEdgeFilter::IdRange {
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
        NormalizedEdgeFilter::And(children) => {
            for child in children {
                apply_edge_id_range_bounds(child, start, end)?;
            }
        }
        _ => {}
    }
    Some(())
}

enum EndpointStreamLabelFilter {
    Empty,
    Single(Option<u32>),
}

fn endpoint_stream_label_filter(label_filter_ids: Option<&[u32]>) -> Option<EndpointStreamLabelFilter> {
    match label_filter_ids {
        None => Some(EndpointStreamLabelFilter::Single(None)),
        Some([label_id]) => Some(EndpointStreamLabelFilter::Single(Some(*label_id))),
        Some([]) => Some(EndpointStreamLabelFilter::Empty),
        Some(_) => None,
    }
}

#[allow(dead_code)]
fn push_memtable_endpoint_cursors(
    cursors: &mut Vec<EdgeCandidateCursor>,
    memtable: Arc<Memtable>,
    node_ids: &[u64],
    direction: Direction,
    label_filter_id: Option<u32>,
    snapshot_seq: u64,
) {
    for &node_id in node_ids {
        match direction {
            Direction::Outgoing => cursors.push(EdgeCandidateCursor::memtable_adjacency(
                Arc::clone(&memtable),
                node_id,
                true,
                label_filter_id,
                snapshot_seq,
            )),
            Direction::Incoming => cursors.push(EdgeCandidateCursor::memtable_adjacency(
                Arc::clone(&memtable),
                node_id,
                false,
                label_filter_id,
                snapshot_seq,
            )),
            Direction::Both => {
                cursors.push(EdgeCandidateCursor::memtable_adjacency(
                    Arc::clone(&memtable),
                    node_id,
                    true,
                    label_filter_id,
                    snapshot_seq,
                ));
                cursors.push(EdgeCandidateCursor::memtable_adjacency(
                    Arc::clone(&memtable),
                    node_id,
                    false,
                    label_filter_id,
                    snapshot_seq,
                ));
            }
        }
    }
}

#[cfg(test)]
mod edge_stream_tests {
    use super::*;

    fn sorted_cursor(ids: &[u64]) -> EdgeCandidateCursor {
        EdgeCandidateCursor::sorted_buffer(ids.to_vec())
    }

    fn collect_leaf(mut stream: EdgeLeafStream) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(edge_id) = stream.next_ge(bound).unwrap() {
            out.push(edge_id);
            if edge_id == u64::MAX {
                break;
            }
            bound = edge_id + 1;
        }
        out
    }

    fn make_node(id: u64, label_id: u32, key: &str) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label_id).unwrap(),
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_edge(id: u64, from: u64, to: u64, label_id: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            label_id,
            props: BTreeMap::new(),
            created_at: 2000,
            updated_at: 2001,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    fn make_edge_with_props(
        id: u64,
        from: u64,
        to: u64,
        label_id: u32,
        props: BTreeMap<String, PropValue>,
    ) -> EdgeRecord {
        EdgeRecord {
            props,
            ..make_edge(id, from, to, label_id)
        }
    }

    fn edge_equality_entry(
        index_id: u64,
        label_id: u32,
        prop_key: &str,
    ) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::EdgeProperty {
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

    fn prop_status(value: &str) -> BTreeMap<String, PropValue> {
        let mut props = BTreeMap::new();
        props.insert("status".to_string(), PropValue::String(value.to_string()));
        props
    }

    fn assert_cursor_sequence(
        name: &str,
        mut cursor: EdgeCandidateCursor,
        expected_ids: &[u64],
        bounds: &[u64],
    ) {
        for &bound in bounds {
            let expected = expected_ids.iter().copied().find(|&edge_id| edge_id >= bound);
            assert_eq!(
                cursor.next_ge(bound).unwrap(),
                expected,
                "{name}, bound {bound}"
            );
        }
    }

    #[test]
    fn edge_stream_sorted_buffer_cursor_contract() {
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

    #[test]
    fn edge_stream_cursor_variants_cover_seek_boundary_matrix() {
        let ids = [1, 3, 5, 8];
        let bounds = [0, 1, 1, 2, 3, 4, 5, 6, 8, 9, 9, u64::MAX];
        assert_cursor_sequence("sorted terminal", sorted_cursor(&ids), &ids, &bounds);

        let max_ids = [2, u64::MAX];
        let max_bounds = [0, 2, 2, 3, u64::MAX, u64::MAX];
        assert_cursor_sequence("sorted max", sorted_cursor(&max_ids), &max_ids, &max_bounds);

        let label_memtable = Arc::new(Memtable::new());
        for (seq, id) in ids.into_iter().enumerate() {
            label_memtable.apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 33)), seq as u64 + 1);
        }
        assert_cursor_sequence(
            "memtable label terminal",
            EdgeCandidateCursor::memtable_label(Arc::clone(&label_memtable), 33, ids.len() as u64),
            &ids,
            &bounds,
        );
        assert_cursor_sequence(
            "memtable label absent",
            EdgeCandidateCursor::memtable_label(label_memtable, 34, ids.len() as u64),
            &[],
            &[0, u64::MAX],
        );

        let max_label_memtable = Arc::new(Memtable::new());
        for (seq, id) in max_ids.into_iter().enumerate() {
            max_label_memtable.apply_op(
                &WalOp::UpsertEdge(make_edge(id, 1, 2, 34)),
                seq as u64 + 1,
            );
        }
        assert_cursor_sequence(
            "memtable label max",
            EdgeCandidateCursor::memtable_label(Arc::clone(&max_label_memtable), 34, 2),
            &max_ids,
            &max_bounds,
        );

        let segment_label_memtable = Memtable::new();
        for (seq, id) in ids.into_iter().enumerate() {
            segment_label_memtable
                .apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 35)), seq as u64 + 1);
        }
        let (_label_dir, label_reader) = write_test_segment(&segment_label_memtable, &[]);
        let label_reader = Arc::new(label_reader);
        let label_posting = label_reader.edge_label_posting(35).unwrap().unwrap();
        assert_cursor_sequence(
            "segment label terminal",
            EdgeCandidateCursor::segment_label_posting(Arc::clone(&label_reader), label_posting),
            &ids,
            &bounds,
        );

        let max_segment_label_memtable = Memtable::new();
        for (seq, id) in max_ids.into_iter().enumerate() {
            max_segment_label_memtable
                .apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 36)), seq as u64 + 1);
        }
        let (_max_label_dir, max_label_reader) =
            write_test_segment(&max_segment_label_memtable, &[]);
        let max_label_reader = Arc::new(max_label_reader);
        let max_label_posting = max_label_reader.edge_label_posting(36).unwrap().unwrap();
        assert_cursor_sequence(
            "segment label max",
            EdgeCandidateCursor::segment_label_posting(max_label_reader, max_label_posting),
            &max_ids,
            &max_bounds,
        );

        let entry = edge_equality_entry(93, 37, "status");
        let equality_memtable = Memtable::new();
        equality_memtable.register_secondary_index(&entry);
        for (seq, id) in ids.into_iter().enumerate() {
            equality_memtable.apply_op(
                &WalOp::UpsertEdge(make_edge_with_props(id, 1, 2, 37, prop_status("hot"))),
                seq as u64 + 1,
            );
        }
        let (_eq_dir, eq_reader) = write_test_segment(
            &equality_memtable,
            std::slice::from_ref(&entry),
        );
        let eq_reader = Arc::new(eq_reader);
        let hot_hash = hash_prop_equality_key(&PropValue::String("hot".to_string()));
        let crate::segment_reader::SecondaryEqGroupSpanLookup::Found(eq_span) = eq_reader
            .secondary_eq_group_span_if_present_for_target(
                entry.index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                hot_hash,
            )
            .unwrap()
        else {
            panic!("hot group must exist");
        };
        assert_cursor_sequence(
            "segment equality terminal",
            EdgeCandidateCursor::segment_equality_group(
                Arc::clone(&eq_reader),
                entry.index_id,
                eq_span.offset,
                eq_span.id_count,
            ),
            &ids,
            &bounds,
        );

        let max_entry = edge_equality_entry(94, 38, "status");
        let max_equality_memtable = Memtable::new();
        max_equality_memtable.register_secondary_index(&max_entry);
        for (seq, id) in max_ids.into_iter().enumerate() {
            max_equality_memtable.apply_op(
                &WalOp::UpsertEdge(make_edge_with_props(id, 1, 2, 38, prop_status("hot"))),
                seq as u64 + 1,
            );
        }
        let (_max_eq_dir, max_eq_reader) = write_test_segment(
            &max_equality_memtable,
            std::slice::from_ref(&max_entry),
        );
        let max_eq_reader = Arc::new(max_eq_reader);
        let crate::segment_reader::SecondaryEqGroupSpanLookup::Found(max_eq_span) = max_eq_reader
            .secondary_eq_group_span_if_present_for_target(
                max_entry.index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                hot_hash,
            )
            .unwrap()
        else {
            panic!("max hot group must exist");
        };
        assert_cursor_sequence(
            "segment equality max",
            EdgeCandidateCursor::segment_equality_group(
                max_eq_reader,
                max_entry.index_id,
                max_eq_span.offset,
                max_eq_span.id_count,
            ),
            &max_ids,
            &max_bounds,
        );

        let adj_memtable = Arc::new(Memtable::new());
        adj_memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        adj_memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        for (seq, id) in ids.into_iter().enumerate() {
            adj_memtable.apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 39)), seq as u64 + 3);
        }
        assert_cursor_sequence(
            "memtable adjacency terminal",
            EdgeCandidateCursor::memtable_adjacency(
                Arc::clone(&adj_memtable),
                1,
                true,
                Some(39),
                ids.len() as u64 + 2,
            ),
            &ids,
            &bounds,
        );
        assert_cursor_sequence(
            "memtable adjacency missing node",
            EdgeCandidateCursor::memtable_adjacency(adj_memtable, 99, true, Some(39), 6),
            &[],
            &[0, u64::MAX],
        );

        let max_adj_memtable = Arc::new(Memtable::new());
        max_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        max_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        for (seq, id) in max_ids.into_iter().enumerate() {
            max_adj_memtable
                .apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 40)), seq as u64 + 3);
        }
        assert_cursor_sequence(
            "memtable adjacency max",
            EdgeCandidateCursor::memtable_adjacency(
                Arc::clone(&max_adj_memtable),
                1,
                true,
                Some(40),
                4,
            ),
            &max_ids,
            &max_bounds,
        );

        let segment_adj_memtable = Memtable::new();
        segment_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        segment_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        for (seq, id) in ids.into_iter().enumerate() {
            segment_adj_memtable
                .apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 41)), seq as u64 + 3);
        }
        let (_adj_dir, adj_reader) = write_test_segment(&segment_adj_memtable, &[]);
        let adj_reader = Arc::new(adj_reader);
        let adj_labels = [41];
        let mut adj_cursors = adj_reader
            .endpoint_adj_posting_cursors(&[1], Direction::Outgoing, Some(&adj_labels))
            .unwrap();
        assert_eq!(adj_cursors.len(), 1);
        assert_cursor_sequence(
            "segment adjacency terminal",
            EdgeCandidateCursor::segment_adjacency(adj_reader, adj_cursors.remove(0)),
            &ids,
            &bounds,
        );

        let max_segment_adj_memtable = Memtable::new();
        max_segment_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        max_segment_adj_memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        for (seq, id) in max_ids.into_iter().enumerate() {
            max_segment_adj_memtable
                .apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 42)), seq as u64 + 3);
        }
        let (_max_adj_dir, max_adj_reader) = write_test_segment(&max_segment_adj_memtable, &[]);
        let max_adj_reader = Arc::new(max_adj_reader);
        let max_adj_labels = [42];
        let mut max_adj_cursors = max_adj_reader
            .endpoint_adj_posting_cursors(&[1], Direction::Outgoing, Some(&max_adj_labels))
            .unwrap();
        assert_eq!(max_adj_cursors.len(), 1);
        assert_cursor_sequence(
            "segment adjacency max",
            EdgeCandidateCursor::segment_adjacency(max_adj_reader, max_adj_cursors.remove(0)),
            &max_ids,
            &max_bounds,
        );
    }

    #[test]
    fn edge_stream_memtable_label_cursor_includes_bound_and_skips_invisible_slots() {
        let memtable = Arc::new(Memtable::new());
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 7)), 1);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 2, 7)), 2);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(5, 1, 2, 7)), 3);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 2, 8)), 4);

        let mut cursor = EdgeCandidateCursor::memtable_label(memtable.clone(), 7, 3);
        assert_eq!(cursor.next_ge(3).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(3).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(4).unwrap(), Some(5));

        let mut later = EdgeCandidateCursor::memtable_label(memtable, 7, 4);
        assert_eq!(later.next_ge(2).unwrap(), Some(5));
    }

    #[test]
    fn edge_stream_segment_label_cursor_uses_inclusive_galloping_seek() {
        let memtable = Memtable::new();
        for (seq, id) in [1, 3, 5, 8, 13, 21].into_iter().enumerate() {
            memtable.apply_op(&WalOp::UpsertEdge(make_edge(id, 1, 2, 4)), seq as u64 + 1);
        }
        let (_dir, reader) = write_test_segment(&memtable, &[]);
        let reader = Arc::new(reader);
        let posting = reader.edge_label_posting(4).unwrap().unwrap();
        let mut cursor = EdgeCandidateCursor::segment_label_posting(reader, posting);

        assert_eq!(cursor.next_ge(0).unwrap(), Some(1));
        assert_eq!(cursor.next_ge(1).unwrap(), Some(1));
        assert_eq!(cursor.next_ge(2).unwrap(), Some(3));
        assert_eq!(cursor.next_ge(9).unwrap(), Some(13));
        assert_eq!(cursor.next_ge(22).unwrap(), None);
    }

    #[test]
    fn edge_stream_segment_equality_cursor_seeks_edge_property_group() {
        let entry = edge_equality_entry(91, 7, "status");
        let memtable = Memtable::new();
        memtable.register_secondary_index(&entry);
        for (seq, (id, status)) in [
            (2, "hot"),
            (5, "hot"),
            (9, "cold"),
            (12, "hot"),
            (u64::MAX, "hot"),
        ]
        .into_iter()
        .enumerate()
        {
            memtable.apply_op(
                &WalOp::UpsertEdge(make_edge_with_props(id, 1, 2, 7, prop_status(status))),
                seq as u64 + 1,
            );
        }
        let (_dir, reader) = write_test_segment(&memtable, std::slice::from_ref(&entry));
        let reader = Arc::new(reader);
        let hot_hash = hash_prop_equality_key(&PropValue::String("hot".to_string()));
        let crate::segment_reader::SecondaryEqGroupSpanLookup::Found(span) = reader
            .secondary_eq_group_span_if_present_for_target(
                entry.index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                hot_hash,
            )
            .unwrap()
        else {
            panic!("hot group must exist");
        };
        let mut empty =
            EdgeCandidateCursor::segment_equality_group(reader.clone(), entry.index_id, span.offset, 0);
        assert_eq!(empty.next_ge(0).unwrap(), None);

        let mut cursor = EdgeCandidateCursor::segment_equality_group(
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
    fn edge_stream_memtable_adjacency_cursor_filters_skips_and_holds() {
        let memtable = Arc::new(Memtable::new());
        memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        memtable.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 4);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(12, 1, 3, 8)), 5);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(14, 1, 2, 7)), 6);
        memtable.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 700,
            },
            7,
        );
        memtable.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 800,
            },
            8,
        );

        let mut before_delete =
            EdgeCandidateCursor::memtable_adjacency(memtable.clone(), 1, true, Some(7), 6);
        assert_eq!(before_delete.next_ge(10).unwrap(), Some(10));
        assert_eq!(before_delete.next_ge(10).unwrap(), Some(10));
        assert_eq!(before_delete.next_ge(11).unwrap(), Some(14));

        let mut after_delete =
            EdgeCandidateCursor::memtable_adjacency(memtable.clone(), 1, true, None, 8);
        assert_eq!(after_delete.next_ge(10).unwrap(), Some(14));
        assert_eq!(after_delete.next_ge(15).unwrap(), None);

        let mut label_eight = EdgeCandidateCursor::memtable_adjacency(memtable, 1, true, Some(8), 8);
        assert_eq!(label_eight.next_ge(0).unwrap(), None);
    }

    #[test]
    fn edge_stream_segment_adjacency_cursor_walks_forward_and_holds() {
        let memtable = Memtable::new();
        memtable.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")), 1);
        memtable.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")), 2);
        memtable.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")), 3);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 7)), 4);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(12, 1, 3, 7)), 5);
        memtable.apply_op(&WalOp::UpsertEdge(make_edge(14, 1, 2, 7)), 6);
        memtable.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 700,
            },
            7,
        );
        let (_dir, reader) = write_test_segment(&memtable, &[]);
        let reader = Arc::new(reader);
        let labels = [7];
        let mut cursors = reader
            .endpoint_adj_posting_cursors(&[1], Direction::Outgoing, Some(&labels))
            .unwrap();
        assert_eq!(cursors.len(), 1);
        let mut cursor = EdgeCandidateCursor::segment_adjacency(reader, cursors.remove(0));

        assert_eq!(cursor.next_ge(0).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(10).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(11).unwrap(), Some(14));
        assert_eq!(cursor.next_ge(15).unwrap(), None);
    }

    #[test]
    fn edge_stream_leaf_stream_dedups_duplicate_cursor_heads() {
        let stream = EdgeLeafStream::new(vec![
            sorted_cursor(&[1, 3, 5, 9]),
            sorted_cursor(&[1, 2, 5, 8]),
            sorted_cursor(&[5, 7, 9]),
        ]);

        assert_eq!(collect_leaf(stream), vec![1, 2, 3, 5, 7, 8, 9]);
    }

    #[test]
    fn edge_stream_leapfrog_over_edge_cursors_matches_oracle() {
        let mut stream = LeapfrogIntersection::new(vec![
            EdgeLeafStream::new(vec![sorted_cursor(&[1, 5, 9, 13])]).into_stream_set(),
            EdgeLeafStream::new(vec![sorted_cursor(&[3, 5, 11, 13])]).into_stream_set(),
            EdgeLeafStream::new(vec![sorted_cursor(&[5, 7, 13])]).into_stream_set(),
        ]);
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(edge_id) = stream.next_ge(bound).unwrap() {
            out.push(edge_id);
            bound = edge_id + 1;
        }
        assert_eq!(out, vec![5, 13]);
    }

    #[test]
    fn edge_stream_leapfrog_seeded_oracle_covers_sparse_random_inputs() {
        let mut seed = 0x41_01_e5_7a_9b_cd_ef_13u64;
        for case_index in 0..64 {
            let stream_count = 1 + (next_test_u64(&mut seed) as usize % 16);
            let shared_hit = 200 + case_index as u64;
            let mut expected: Option<Vec<u64>> = None;
            let mut streams = Vec::new();

            for stream_index in 0..stream_count {
                let mut ids = Vec::new();
                let len = next_test_u64(&mut seed) as usize % 32;
                for _ in 0..len {
                    ids.push(next_test_u64(&mut seed) % 128);
                }
                if case_index % 3 == 0 || stream_index == 0 {
                    ids.push(shared_hit);
                }
                ids.sort_unstable();
                ids.dedup();

                expected = Some(match expected {
                    Some(current) => current
                        .into_iter()
                        .filter(|id| ids.binary_search(id).is_ok())
                        .collect(),
                    None => ids.clone(),
                });
                streams.push(EdgeLeafStream::new(vec![sorted_cursor(&ids)]).into_stream_set());
            }

            let mut intersection = LeapfrogIntersection::new(streams);
            let mut got = Vec::new();
            let mut bound = 0u64;
            while let Some(edge_id) = intersection.next_ge(bound).unwrap() {
                got.push(edge_id);
                bound = edge_id + 1;
            }
            assert_eq!(
                got,
                expected.unwrap_or_default(),
                "case {case_index}, stream_count {stream_count}"
            );
        }
    }

    fn next_test_u64(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        *seed
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "next_ge bounds must be non-decreasing")]
    fn edge_stream_debug_asserts_decreasing_bound_after_terminal_seek() {
        let mut cursor = sorted_cursor(&[10, 20]);
        assert_eq!(cursor.next_ge(10).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(25).unwrap(), None);
        let _ = cursor.next_ge(24);
    }

    fn wait_for_edge_index_state(
        engine: &DatabaseEngine,
        index_id: u64,
        state: SecondaryIndexState,
    ) {
        for _ in 0..200 {
            let indexes = engine.list_edge_property_indexes().unwrap();
            if indexes
                .iter()
                .any(|info| info.index_id == index_id && info.state == state)
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("edge index {index_id} did not reach state {state:?}");
    }

    fn test_props(key: &str, value: PropValue) -> BTreeMap<String, PropValue> {
        let mut props = BTreeMap::new();
        props.insert(key.to_string(), value);
        props
    }

    #[test]
    fn edge_stream_read_view_leaf_streams_cover_visible_sources() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let a = engine
            .upsert_node("StreamNode", "a", UpsertNodeOptions::default())
            .unwrap();
        let b = engine
            .upsert_node("StreamNode", "b", UpsertNodeOptions::default())
            .unwrap();
        let label_id = engine.ensure_edge_label("StreamRel").unwrap();
        let other_label_id = engine.ensure_edge_label("OtherStreamRel").unwrap();

        let info = engine
            .ensure_edge_property_index(
                "StreamRel",
                SecondaryIndexSpec {
                    fields: vec![SecondaryIndexField::Property {
                        key: "status".to_string(),
                    }],
                    kind: SecondaryIndexKind::Equality,
                },
            )
            .unwrap();
        wait_for_edge_index_state(&engine, info.index_id, SecondaryIndexState::Ready);

        let seg_edge = engine
            .upsert_edge(
                a,
                b,
                "StreamRel",
                UpsertEdgeOptions {
                    props: test_props("status", PropValue::String("hot".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.flush().unwrap();

        let immutable_edge = engine
            .upsert_edge(
                b,
                a,
                "StreamRel",
                UpsertEdgeOptions {
                    props: test_props("status", PropValue::String("hot".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();
        engine.freeze_memtable().unwrap();

        let active_edge = engine
            .upsert_edge(
                a,
                a,
                "StreamRel",
                UpsertEdgeOptions {
                    props: test_props("status", PropValue::String("hot".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();
        let other_label_edge = engine
            .upsert_edge(
                a,
                b,
                "OtherStreamRel",
                UpsertEdgeOptions {
                    props: test_props("status", PropValue::String("hot".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();

        let view = engine.published_state().view.clone();
        assert_eq!(
            collect_leaf(view.build_label_edge_stream(label_id).unwrap()),
            vec![seg_edge, immutable_edge, active_edge]
        );

        match view
            .build_equality_edge_stream(
                info.index_id,
                &equality_probe_value_hashes(&PropValue::String("hot".to_string())),
            )
            .unwrap()
        {
            EdgeStreamBuildResult::Ready(stream) => {
                assert_eq!(collect_leaf(stream), vec![seg_edge, immutable_edge, active_edge]);
            }
            EdgeStreamBuildResult::Demoted { .. } | EdgeStreamBuildResult::TooBroad { .. } => {
                panic!("equality leaf must be streamable")
            }
        }

        assert_eq!(
            collect_leaf(
                view.build_endpoint_edge_stream(&[a], Direction::Both, Some(label_id))
                    .unwrap()
            ),
            vec![seg_edge, immutable_edge, active_edge]
        );
        assert_eq!(
            collect_leaf(
                view.build_endpoint_edge_stream(&[a], Direction::Both, Some(other_label_id))
                    .unwrap()
            ),
            vec![other_label_edge]
        );
        assert_eq!(
            collect_leaf(
                view.build_endpoint_edge_stream(&[a], Direction::Both, Some(label_id))
                    .unwrap()
            )
            .into_iter()
            .filter(|edge_id| *edge_id == active_edge)
            .count(),
            1
        );
        assert_eq!(
            collect_leaf(view.build_triple_edge_stream(a, a, label_id).unwrap()),
            vec![active_edge]
        );
        assert_eq!(
            collect_leaf(view.build_explicit_edge_stream(vec![active_edge, seg_edge, seg_edge])),
            vec![seg_edge, active_edge]
        );
    }

    #[test]
    fn edge_stream_equality_leaf_treats_missing_value_groups_as_empty_sources() {
        let entry = edge_equality_entry(92, 7, "status");
        let segment_with_hot = Memtable::new();
        segment_with_hot.register_secondary_index(&entry);
        segment_with_hot.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(10, 1, 2, 7, prop_status("hot"))),
            1,
        );
        let (_hot_dir, hot_reader) =
            write_test_segment(&segment_with_hot, std::slice::from_ref(&entry));

        let segment_without_hot = Memtable::new();
        segment_without_hot.register_secondary_index(&entry);
        segment_without_hot.apply_op(
            &WalOp::UpsertEdge(make_edge_with_props(20, 1, 2, 7, prop_status("cold"))),
            1,
        );
        let (_cold_dir, cold_reader) =
            write_test_segment(&segment_without_hot, std::slice::from_ref(&entry));

        let hot_hash = hash_prop_equality_key(&PropValue::String("hot".to_string()));
        let hot_span = hot_reader
            .secondary_eq_group_span_if_present_for_target(
                entry.index_id,
                PlannerStatsDeclaredIndexTarget::EdgeProperty,
                hot_hash,
            )
            .unwrap();
        assert!(matches!(
            hot_span,
            crate::segment_reader::SecondaryEqGroupSpanLookup::Found(_)
        ));
        assert_eq!(
            cold_reader
                .secondary_eq_group_span_if_present_for_target(
                    entry.index_id,
                    PlannerStatsDeclaredIndexTarget::EdgeProperty,
                    hot_hash,
                )
                .unwrap(),
            crate::segment_reader::SecondaryEqGroupSpanLookup::MissingValueGroup
        );

        let crate::segment_reader::SecondaryEqGroupSpanLookup::Found(span) = hot_span else {
            unreachable!();
        };
        let stream = EdgeLeafStream::new(vec![EdgeCandidateCursor::segment_equality_group(
            Arc::new(hot_reader),
            entry.index_id,
            span.offset,
            span.id_count,
        )]);
        assert_eq!(collect_leaf(stream), vec![10]);
    }
}
