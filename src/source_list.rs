//! Ordered-source visibility substrate for multi-layer lookups.
//!
//! `SourceList` encapsulates the precedence order:
//!   active memtable > immutable memtables (newest-first) > segments (newest-first)
//!
//! It provides short-circuiting point lookups, key/triple lookups, and deletion
//! checks that consult all live sources in the correct order. Engine read paths
//! delegate to `SourceList` instead of open-coding memtable + segment logic.

use crate::edge_metadata::{EdgeMetadataCandidate, RangeBoundFlags};
use crate::engine::ReadViewImmutableEpoch;
use crate::error::EngineError;
use crate::memtable::Memtable;
use crate::property_value_semantics::NumericRangeSortKey;
use crate::row_projection::{EdgeSelectedFieldNeeds, NodeSelectedFieldNeeds, PropertySelection};
use crate::segment_reader::{SegmentAdjPostingCursor, SegmentReader};
use crate::types::*;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Concrete borrowing struct over the three source layers. This is not a trait,
/// because Memtable and SegmentReader have fundamentally different APIs.
pub struct SourceList<'a> {
    pub(crate) active: &'a Memtable,
    pub(crate) immutable: &'a [ReadViewImmutableEpoch],
    pub(crate) segments: &'a [Arc<SegmentReader>],
    pub(crate) snapshot_seq: u64,
    #[cfg(test)]
    pub(crate) selected_field_read_counters: Option<&'a SelectedFieldReadCounters>,
}

pub(crate) enum LimitedEdgeIndexRead {
    Ready(Vec<u64>),
    TooBroad,
    MissingSidecar,
}

#[derive(Clone, Copy)]
struct MemtableEndpointLimit<'a> {
    direction: Direction,
    label_filter_ids: Option<&'a [u32]>,
    snapshot_seq: u64,
    limit: usize,
}

#[derive(Clone, Copy)]
enum EndpointEdgeScanLayer {
    Active,
    Immutable(usize),
    Segment(usize),
}

enum EndpointEdgeScanSource<'a> {
    Memtable(MemtableEndpointEdgeScanSource<'a>),
    Segment(SegmentEndpointEdgeScanSource<'a>),
}

enum MemtableEndpointScanDirection {
    Outgoing,
    Incoming,
}

struct MemtableEndpointScanCursor<'a> {
    memtable: &'a Memtable,
    node_id: u64,
    direction: MemtableEndpointScanDirection,
    label_filter_ids: Option<&'a [u32]>,
    snapshot_seq: u64,
    next_after: Option<u64>,
}

struct MemtableEndpointEdgeScanSource<'a> {
    cursors: Vec<MemtableEndpointScanCursor<'a>>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    last_seen: Option<u64>,
}

struct SegmentEndpointEdgeScanSource<'a> {
    segment: &'a SegmentReader,
    cursors: Vec<SegmentAdjPostingCursor>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    last_seen: Option<u64>,
}

impl<'a> EndpointEdgeScanSource<'a> {
    fn memtable(
        memtable: &'a Memtable,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'a [u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Self {
        Self::Memtable(MemtableEndpointEdgeScanSource::new(
            memtable,
            node_ids,
            direction,
            label_filter_ids,
            snapshot_seq,
            after,
        ))
    }

    fn segment(
        segment: &'a SegmentReader,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        after: Option<u64>,
    ) -> Result<Self, EngineError> {
        Ok(Self::Segment(SegmentEndpointEdgeScanSource::new(
            segment,
            node_ids,
            direction,
            label_filter_ids,
            after,
        )?))
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        match self {
            Self::Memtable(source) => Ok(source.next_id()),
            Self::Segment(source) => source.next_id(),
        }
    }
}

impl<'a> MemtableEndpointEdgeScanSource<'a> {
    fn new(
        memtable: &'a Memtable,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'a [u32]>,
        snapshot_seq: u64,
        after: Option<u64>,
    ) -> Self {
        let mut cursors = Vec::new();
        for &node_id in node_ids {
            match direction {
                Direction::Outgoing => cursors.push(MemtableEndpointScanCursor {
                    memtable,
                    node_id,
                    direction: MemtableEndpointScanDirection::Outgoing,
                    label_filter_ids,
                    snapshot_seq,
                    next_after: after,
                }),
                Direction::Incoming => cursors.push(MemtableEndpointScanCursor {
                    memtable,
                    node_id,
                    direction: MemtableEndpointScanDirection::Incoming,
                    label_filter_ids,
                    snapshot_seq,
                    next_after: after,
                }),
                Direction::Both => {
                    cursors.push(MemtableEndpointScanCursor {
                        memtable,
                        node_id,
                        direction: MemtableEndpointScanDirection::Outgoing,
                        label_filter_ids,
                        snapshot_seq,
                        next_after: after,
                    });
                    cursors.push(MemtableEndpointScanCursor {
                        memtable,
                        node_id,
                        direction: MemtableEndpointScanDirection::Incoming,
                        label_filter_ids,
                        snapshot_seq,
                        next_after: after,
                    });
                }
            }
        }

        let mut source = Self {
            cursors,
            heap: BinaryHeap::new(),
            last_seen: None,
        };
        for cursor_index in 0..source.cursors.len() {
            if let Some(edge_id) = source.next_cursor_id(cursor_index) {
                source.heap.push(Reverse((edge_id, cursor_index)));
            }
        }
        source
    }

    fn next_cursor_id(&mut self, cursor_index: usize) -> Option<u64> {
        let cursor = &mut self.cursors[cursor_index];
        let edge_id = match cursor.direction {
            MemtableEndpointScanDirection::Outgoing => {
                cursor.memtable.next_visible_edge_from_endpoint_after(
                    cursor.node_id,
                    cursor.label_filter_ids,
                    cursor.snapshot_seq,
                    cursor.next_after,
                )
            }
            MemtableEndpointScanDirection::Incoming => {
                cursor.memtable.next_visible_edge_to_endpoint_after(
                    cursor.node_id,
                    cursor.label_filter_ids,
                    cursor.snapshot_seq,
                    cursor.next_after,
                )
            }
        };
        if let Some(edge_id) = edge_id {
            cursor.next_after = Some(edge_id);
        }
        edge_id
    }

    fn next_id(&mut self) -> Option<u64> {
        while let Some(Reverse((edge_id, cursor_index))) = self.heap.pop() {
            if let Some(next_id) = self.next_cursor_id(cursor_index) {
                self.heap.push(Reverse((next_id, cursor_index)));
            }
            if self.last_seen == Some(edge_id) {
                continue;
            }
            self.last_seen = Some(edge_id);
            return Some(edge_id);
        }
        None
    }
}

impl<'a> SegmentEndpointEdgeScanSource<'a> {
    fn new(
        segment: &'a SegmentReader,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        after: Option<u64>,
    ) -> Result<Self, EngineError> {
        let mut cursors =
            segment.endpoint_adj_posting_cursors(node_ids, direction, label_filter_ids)?;
        let mut heap = BinaryHeap::new();
        for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
            while let Some(edge_id) = segment.next_adj_posting_edge_id(cursor)? {
                if after.is_none_or(|after| edge_id > after) {
                    heap.push(Reverse((edge_id, cursor_index)));
                    break;
                }
            }
        }
        Ok(Self {
            segment,
            cursors,
            heap,
            last_seen: None,
        })
    }

    fn next_id(&mut self) -> Result<Option<u64>, EngineError> {
        while let Some(Reverse((edge_id, cursor_index))) = self.heap.pop() {
            if let Some(next_id) = self
                .segment
                .next_adj_posting_edge_id(&mut self.cursors[cursor_index])?
            {
                self.heap.push(Reverse((next_id, cursor_index)));
            }
            if self.last_seen == Some(edge_id) {
                continue;
            }
            self.last_seen = Some(edge_id);
            return Ok(Some(edge_id));
        }
        Ok(None)
    }
}

impl<'a> SourceList<'a> {
    /// Find multiple nodes by ID across all sources.
    ///
    /// Preserves input order and duplicate slots while applying first-opinion-wins
    /// precedence: active > immutable (newest-first) > segments (newest-first).
    pub fn find_nodes(&self, ids: &[u64]) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let mut results = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();
        remaining = self
            .active
            .batch_get_nodes_at(&remaining, self.snapshot_seq, &mut results);

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining =
                epoch
                    .memtable
                    .batch_get_nodes_at(&remaining, self.snapshot_seq, &mut results);
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }
                remaining.retain(|&(_, id)| !seg.is_node_deleted(id));
                if remaining.is_empty() {
                    break;
                }
                seg.get_nodes_batch(&remaining, &mut results)?;
                remaining.retain(|&(index, _)| results[index].is_none());
            }
        }

        Ok(results)
    }

    /// Find multiple edges by ID across all sources.
    pub fn find_edges(&self, ids: &[u64]) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let mut results = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();
        remaining = self
            .active
            .batch_get_edges_at(&remaining, self.snapshot_seq, &mut results);

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining =
                epoch
                    .memtable
                    .batch_get_edges_at(&remaining, self.snapshot_seq, &mut results);
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }
                remaining.retain(|&(_, id)| !seg.is_edge_deleted(id));
                if remaining.is_empty() {
                    break;
                }
                seg.get_edges_batch(&remaining, &mut results)?;
                remaining.retain(|&(index, _)| results[index].is_none());
            }
        }

        Ok(results)
    }

    pub(crate) fn find_edge_metadata(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Option<EdgeMetadataCandidate>>, EngineError> {
        let mut results = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();

        remaining.retain(|&(index, id)| {
            if let Some(meta) = self.active.get_edge_metadata_at(id, self.snapshot_seq) {
                results[index] = Some(meta);
                false
            } else {
                !self.active.is_edge_deleted_at(id, self.snapshot_seq)
            }
        });

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(index, id)| {
                if let Some(meta) = epoch.memtable.get_edge_metadata_at(id, self.snapshot_seq) {
                    results[index] = Some(meta);
                    false
                } else {
                    !epoch.memtable.is_edge_deleted_at(id, self.snapshot_seq)
                }
            });
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }
                remaining.retain(|&(_, id)| !seg.is_edge_deleted(id));
                if remaining.is_empty() {
                    break;
                }
                seg.get_edge_metadata_batch(&remaining, &mut results)?;
                remaining.retain(|&(index, _)| results[index].is_none());
            }
        }

        Ok(results)
    }

    pub(crate) fn find_node_visibility_meta(
        &self,
        ids: &[u64],
    ) -> Result<Vec<NodeVisibilityState>, EngineError> {
        let mut results = vec![NodeVisibilityState::Missing; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();

        remaining = self.active.batch_get_node_visibility_meta_at(
            &remaining,
            self.snapshot_seq,
            &mut results,
        );

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining = epoch.memtable.batch_get_node_visibility_meta_at(
                &remaining,
                self.snapshot_seq,
                &mut results,
            );
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            let mut compact_lookups = Vec::new();
            let mut segment_results: Vec<Option<(NodeLabelSet, i64, f32)>> = Vec::new();
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }

                remaining.retain(|&(index, id)| {
                    if seg.is_node_deleted(id) {
                        results[index] = NodeVisibilityState::Deleted;
                        false
                    } else {
                        true
                    }
                });
                if remaining.is_empty() {
                    break;
                }

                compact_lookups.clear();
                compact_lookups.reserve(remaining.len());
                for (compact_index, &(_, id)) in remaining.iter().enumerate() {
                    compact_lookups.push((compact_index, id));
                }
                segment_results.clear();
                segment_results.resize(remaining.len(), None);

                seg.get_node_meta_batch(&compact_lookups, &mut segment_results)?;
                let mut compact_index = 0usize;
                remaining.retain(|&(index, _)| {
                    let state = segment_results[compact_index];
                    compact_index += 1;
                    if let Some((label_ids, updated_at, weight)) = state {
                        results[index] = NodeVisibilityState::Live(NodeVisibilityMeta {
                            label_ids,
                            updated_at,
                            weight,
                        });
                        false
                    } else {
                        true
                    }
                });
            }
        }

        Ok(results)
    }

    pub(crate) fn find_node_projected_fields(
        &self,
        ids: &[u64],
        needs: &NodeSelectedFieldNeeds,
    ) -> Result<Vec<Option<SelectedNodeFields>>, EngineError> {
        let mut results = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }
        #[cfg(test)]
        if let Some(counters) = self.selected_field_read_counters {
            counters.note_node_selected_field_batch(ids.len());
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();

        remaining = self.active.batch_get_node_selected_fields_at(
            &remaining,
            needs,
            self.snapshot_seq,
            &mut results,
            #[cfg(test)]
            self.selected_field_read_counters,
        );

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining = epoch.memtable.batch_get_node_selected_fields_at(
                &remaining,
                needs,
                self.snapshot_seq,
                &mut results,
                #[cfg(test)]
                self.selected_field_read_counters,
            );
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            let mut compact_lookups = Vec::new();
            let mut segment_results: Vec<Option<SelectedNodeFields>> = Vec::new();
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }

                remaining.retain(|&(_, id)| !seg.is_node_deleted(id));
                if remaining.is_empty() {
                    break;
                }

                compact_lookups.clear();
                compact_lookups.reserve(remaining.len());
                for (compact_index, &(_, id)) in remaining.iter().enumerate() {
                    compact_lookups.push((compact_index, id));
                }
                segment_results.clear();
                segment_results.resize(remaining.len(), None);

                seg.get_node_selected_fields_batch(
                    &compact_lookups,
                    needs,
                    &mut segment_results,
                    #[cfg(test)]
                    self.selected_field_read_counters,
                )?;
                let mut compact_index = 0usize;
                remaining.retain(|&(index, _)| {
                    let fields = segment_results[compact_index].take();
                    compact_index += 1;
                    if let Some(fields) = fields {
                        results[index] = Some(fields);
                        false
                    } else {
                        true
                    }
                });
            }
        }

        Ok(results)
    }

    pub(crate) fn find_edge_projected_fields(
        &self,
        ids: &[u64],
        needs: &EdgeSelectedFieldNeeds,
    ) -> Result<Vec<Option<SelectedEdgeFields>>, EngineError> {
        let mut results = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }
        #[cfg(test)]
        if let Some(counters) = self.selected_field_read_counters {
            counters.note_edge_selected_field_batch(ids.len());
        }

        let mut remaining: Vec<(usize, u64)> = ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (index, id))
            .collect();

        remaining = self.active.batch_get_edge_selected_fields_at(
            &remaining,
            needs,
            self.snapshot_seq,
            &mut results,
            #[cfg(test)]
            self.selected_field_read_counters,
        );

        for epoch in self.immutable {
            if remaining.is_empty() {
                break;
            }
            remaining = epoch.memtable.batch_get_edge_selected_fields_at(
                &remaining,
                needs,
                self.snapshot_seq,
                &mut results,
                #[cfg(test)]
                self.selected_field_read_counters,
            );
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by_key(|&(_, id)| id);
            let mut compact_lookups = Vec::new();
            let mut segment_results: Vec<Option<SelectedEdgeFields>> = Vec::new();
            for seg in self.segments {
                if remaining.is_empty() {
                    break;
                }

                remaining.retain(|&(_, id)| !seg.is_edge_deleted(id));
                if remaining.is_empty() {
                    break;
                }

                compact_lookups.clear();
                compact_lookups.reserve(remaining.len());
                for (compact_index, &(_, id)) in remaining.iter().enumerate() {
                    compact_lookups.push((compact_index, id));
                }
                segment_results.clear();
                segment_results.resize(remaining.len(), None);

                seg.get_edge_selected_fields_batch(&compact_lookups, needs, &mut segment_results)?;
                let mut compact_index = 0usize;
                remaining.retain(|&(index, _)| {
                    let fields = segment_results[compact_index].take();
                    compact_index += 1;
                    if let Some(fields) = fields {
                        results[index] = Some(fields);
                        false
                    } else {
                        true
                    }
                });
            }
        }

        Ok(results)
    }

    /// Find a node by ID across all sources. Short-circuits on the first
    /// source that has an opinion (live record or tombstone).
    pub fn find_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        if let Some(node) = self.active.get_node_at(id, self.snapshot_seq) {
            return Ok(Some(node));
        }
        if self.active.is_node_deleted_at(id, self.snapshot_seq) {
            return Ok(None);
        }

        for epoch in self.immutable {
            if let Some(node) = epoch.memtable.get_node_at(id, self.snapshot_seq) {
                return Ok(Some(node));
            }
            if epoch.memtable.is_node_deleted_at(id, self.snapshot_seq) {
                return Ok(None);
            }
        }

        for seg in self.segments {
            if seg.is_node_deleted(id) {
                return Ok(None);
            }
            if let Some(node) = seg.get_node(id)? {
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    /// Find an edge by ID across all sources. Short-circuits on the first
    /// source that has an opinion (live record or tombstone).
    pub fn find_edge(&self, id: u64) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self.active.get_edge_at(id, self.snapshot_seq) {
            return Ok(Some(edge));
        }
        if self.active.is_edge_deleted_at(id, self.snapshot_seq) {
            return Ok(None);
        }

        for epoch in self.immutable {
            if let Some(edge) = epoch.memtable.get_edge_at(id, self.snapshot_seq) {
                return Ok(Some(edge));
            }
            if epoch.memtable.is_edge_deleted_at(id, self.snapshot_seq) {
                return Ok(None);
            }
        }

        for seg in self.segments {
            if seg.is_edge_deleted(id) {
                return Ok(None);
            }
            if let Some(edge) = seg.get_edge(id)? {
                return Ok(Some(edge));
            }
        }

        Ok(None)
    }

    pub fn find_nodes_by_label_keys<'b>(
        &self,
        keys: &[(u32, &'b str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let n = keys.len();
        let mut results = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u32, &'b str)> = Vec::with_capacity(n);
        for (i, &(label_id, key)) in keys.iter().enumerate() {
            if let Some(node) = self.active.node_by_key_at(label_id, key, self.snapshot_seq) {
                results[i] = Some(node);
            } else {
                remaining.push((i, label_id, key));
            }
        }

        let mut candidates: Vec<(usize, u32, &'b str, u64)> = Vec::new();
        for (epoch_idx, epoch) in self.immutable.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, label_id, key)| {
                if let Some(node) = epoch
                    .memtable
                    .node_by_key_at(label_id, key, self.snapshot_seq)
                {
                    if self.is_node_tombstoned_above_immutable(node.id, epoch_idx) {
                        return false;
                    }
                    candidates.push((i, label_id, key, node.id));
                    return false;
                }
                true
            });
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by(|left, right| (left.1, left.2).cmp(&(right.1, right.2)));
        }

        for seg in self.segments {
            if remaining.is_empty() {
                break;
            }

            let resolved = seg.resolve_keys_to_ids(&remaining)?;
            if !resolved.is_empty() {
                let mut found = Vec::with_capacity(resolved.len());
                for (orig_idx, node_id) in resolved {
                    let (label_id, key) = keys[orig_idx];
                    candidates.push((orig_idx, label_id, key, node_id));
                    found.push(orig_idx);
                }
                found.sort_unstable();
                found.dedup();
                remaining.retain(|&(i, _, _)| found.binary_search(&i).is_err());
            }
        }

        if !candidates.is_empty() {
            let mut candidate_ids: Vec<u64> = candidates
                .iter()
                .map(|&(_, _, _, node_id)| node_id)
                .collect();
            candidate_ids.sort_unstable();
            candidate_ids.dedup();

            let visibility = self.find_node_visibility_meta(&candidate_ids)?;
            let mut candidate_labels_by_id: NodeIdMap<Vec<u32>> = NodeIdMap::default();
            for &(_, label_id, _, node_id) in &candidates {
                let labels = candidate_labels_by_id.entry(node_id).or_default();
                if !labels.contains(&label_id) {
                    labels.push(label_id);
                }
            }
            let mut visible_ids = Vec::new();
            let mut visible_positions = NodeIdMap::default();
            for (index, state) in visibility.into_iter().enumerate() {
                let NodeVisibilityState::Live(meta) = state else {
                    continue;
                };
                let node_id = candidate_ids[index];
                if candidate_labels_by_id.get(&node_id).is_some_and(|labels| {
                    labels
                        .iter()
                        .any(|&label_id| meta.label_ids.contains(label_id))
                }) {
                    visible_positions.insert(node_id, visible_ids.len());
                    visible_ids.push(node_id);
                }
            }

            if !visible_ids.is_empty() {
                let hydrated = self.find_nodes(&visible_ids)?;
                for (orig_idx, label_id, key, node_id) in candidates {
                    let Some(&position) = visible_positions.get(&node_id) else {
                        continue;
                    };
                    let Some(node) = hydrated[position].as_ref() else {
                        continue;
                    };
                    if node.label_ids.contains(label_id) && node.key == key {
                        results[orig_idx] = Some(node.clone());
                    }
                }
            }
        }

        Ok(results)
    }

    pub(crate) fn find_node_ids_by_label_keys<'b>(
        &self,
        keys: &[(u32, &'b str)],
    ) -> Result<Vec<Option<u64>>, EngineError> {
        let n = keys.len();
        let mut results = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u32, &'b str)> = Vec::with_capacity(n);
        let mut candidates: Vec<(usize, u32, &'b str, u64)> = Vec::new();
        for (i, &(label_id, key)) in keys.iter().enumerate() {
            if let Some(node_id) = self
                .active
                .node_id_by_key_at(label_id, key, self.snapshot_seq)
            {
                candidates.push((i, label_id, key, node_id));
            } else {
                remaining.push((i, label_id, key));
            }
        }

        for (epoch_idx, epoch) in self.immutable.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, label_id, key)| {
                if let Some(node_id) =
                    epoch
                        .memtable
                        .node_id_by_key_at(label_id, key, self.snapshot_seq)
                {
                    if self.is_node_tombstoned_above_immutable(node_id, epoch_idx) {
                        return false;
                    }
                    candidates.push((i, label_id, key, node_id));
                    return false;
                }
                true
            });
        }

        if !remaining.is_empty() {
            remaining.sort_unstable_by(|left, right| (left.1, left.2).cmp(&(right.1, right.2)));
        }

        for seg in self.segments {
            if remaining.is_empty() {
                break;
            }

            let resolved = seg.resolve_keys_to_ids(&remaining)?;
            if !resolved.is_empty() {
                let mut found = Vec::with_capacity(resolved.len());
                for (orig_idx, node_id) in resolved {
                    let (label_id, key) = keys[orig_idx];
                    candidates.push((orig_idx, label_id, key, node_id));
                    found.push(orig_idx);
                }
                found.sort_unstable();
                found.dedup();
                remaining.retain(|&(i, _, _)| found.binary_search(&i).is_err());
            }
        }

        if !candidates.is_empty() {
            let mut candidate_ids: Vec<u64> = candidates
                .iter()
                .map(|&(_, _, _, node_id)| node_id)
                .collect();
            candidate_ids.sort_unstable();
            candidate_ids.dedup();

            let selected = self.find_node_projected_fields(
                &candidate_ids,
                &NodeSelectedFieldNeeds {
                    key: true,
                    ..NodeSelectedFieldNeeds::default()
                },
            )?;
            let mut selected_positions = NodeIdMap::default();
            for (position, (&node_id, fields)) in
                candidate_ids.iter().zip(selected.iter()).enumerate()
            {
                if fields.is_some() {
                    selected_positions.insert(node_id, position);
                }
            }

            for (orig_idx, label_id, key, node_id) in candidates {
                let Some(&position) = selected_positions.get(&node_id) else {
                    continue;
                };
                let Some(fields) = selected[position].as_ref() else {
                    continue;
                };
                if fields.meta.label_ids.contains(label_id) && fields.key.as_deref() == Some(key) {
                    results[orig_idx] = Some(node_id);
                }
            }
        }

        Ok(results)
    }

    pub fn find_node_by_label_key(
        &self,
        label_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        Ok(self
            .find_nodes_by_label_keys(&[(label_id, key)])?
            .pop()
            .flatten())
    }

    pub fn find_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self
            .active
            .edge_by_triple_at(from, to, label_id, self.snapshot_seq)
        {
            return Ok(Some(edge));
        }

        for (i, epoch) in self.immutable.iter().enumerate() {
            if let Some(edge) =
                epoch
                    .memtable
                    .edge_by_triple_at(from, to, label_id, self.snapshot_seq)
            {
                if self.is_edge_tombstoned_above_immutable(edge.id, i) {
                    return Ok(None);
                }
                return Ok(Some(edge));
            }
        }

        for (s, seg) in self.segments.iter().enumerate() {
            if let Some(edge) = seg.edge_by_triple(from, to, label_id)? {
                if self.is_edge_tombstoned_above_segment(edge.id, s) {
                    return Ok(None);
                }
                return Ok(Some(edge));
            }
        }

        Ok(None)
    }

    pub fn find_edges_by_triples(
        &self,
        triples: &[(u64, u64, u32)],
    ) -> Result<Vec<Option<EdgeRecord>>, EngineError> {
        let n = triples.len();
        let mut results = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u64, u64, u32)> = triples
            .iter()
            .enumerate()
            .map(|(index, &(from, to, label_id))| (index, from, to, label_id))
            .collect();

        remaining =
            self.active
                .batch_edges_by_triples_at(&remaining, self.snapshot_seq, &mut results);

        for (epoch_idx, epoch) in self.immutable.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            let previous = remaining;
            remaining = epoch.memtable.batch_edges_by_triples_at(
                &previous,
                self.snapshot_seq,
                &mut results,
            );
            for &(orig_idx, _, _, _) in &previous {
                if let Some(edge) = results[orig_idx].as_ref() {
                    if self.is_edge_tombstoned_above_immutable(edge.id, epoch_idx) {
                        results[orig_idx] = None;
                    }
                }
            }
        }

        if remaining.is_empty() {
            return Ok(results);
        }

        remaining.sort_unstable_by(|left, right| {
            (left.1, left.2, left.3).cmp(&(right.1, right.2, right.3))
        });

        for (seg_idx, seg) in self.segments.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }

            let found = seg.resolve_triples_batch(&remaining, &mut results)?;
            for &orig_idx in &found {
                if let Some(edge) = results[orig_idx].as_ref() {
                    if self.is_edge_tombstoned_above_segment(edge.id, seg_idx) {
                        results[orig_idx] = None;
                    }
                }
            }

            if !found.is_empty() {
                let mut found_mask = vec![false; n];
                for &idx in &found {
                    found_mask[idx] = true;
                }
                remaining.retain(|&(idx, _, _, _)| !found_mask[idx]);
            }
        }

        Ok(results)
    }

    pub fn collect_deleted_nodes(&self) -> NodeIdSet {
        let mut deleted = self.active.collect_deleted_nodes_at(self.snapshot_seq);
        for epoch in self.immutable {
            deleted.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        }
        for seg in self.segments {
            for &id in seg.deleted_node_tombstones().keys() {
                deleted.insert(id);
            }
        }
        deleted
    }

    pub fn collect_deleted_edges(&self) -> NodeIdSet {
        let mut deleted = self.active.collect_deleted_edges_at(self.snapshot_seq);
        for epoch in self.immutable {
            deleted.extend(epoch.memtable.collect_deleted_edges_at(self.snapshot_seq));
        }
        for seg in self.segments {
            for &id in seg.deleted_edge_tombstones().keys() {
                deleted.insert(id);
            }
        }
        deleted
    }

    fn append_edge_matches(result: &mut Vec<u64>, mut matching_ids: Vec<u64>) {
        matching_ids.sort_unstable();
        matching_ids.dedup();
        result.extend(matching_ids);
    }

    fn append_edge_matches_filtered(
        result: &mut Vec<u64>,
        mut matching_ids: Vec<u64>,
        mut is_shadowed: impl FnMut(u64) -> bool,
    ) {
        matching_ids.retain(|&id| !is_shadowed(id));
        Self::append_edge_matches(result, matching_ids);
    }

    fn push_edge_match_limited(
        result: &mut Vec<u64>,
        edge_id: u64,
        limit: usize,
    ) -> ControlFlow<()> {
        if result.len() >= limit {
            return ControlFlow::Break(());
        }
        result.push(edge_id);
        if result.len() >= limit {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn finalize_edge_matches(mut result: Vec<u64>) -> Vec<u64> {
        result.sort_unstable();
        result.dedup();
        result
    }

    fn append_memtable_endpoint_matches_limited(
        result: &mut Vec<u64>,
        memtable: &Memtable,
        node_ids: &[u64],
        params: MemtableEndpointLimit<'_>,
        mut is_shadowed: impl FnMut(u64) -> bool,
    ) {
        let mut cursors = Vec::new();
        for &node_id in node_ids {
            match params.direction {
                Direction::Outgoing => cursors.push((node_id, true, None)),
                Direction::Incoming => cursors.push((node_id, false, None)),
                Direction::Both => {
                    cursors.push((node_id, true, None));
                    cursors.push((node_id, false, None));
                }
            }
        }

        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter_mut().enumerate() {
            let next = if cursor.1 {
                memtable.next_visible_edge_from_endpoint_after(
                    cursor.0,
                    params.label_filter_ids,
                    params.snapshot_seq,
                    cursor.2,
                )
            } else {
                memtable.next_visible_edge_to_endpoint_after(
                    cursor.0,
                    params.label_filter_ids,
                    params.snapshot_seq,
                    cursor.2,
                )
            };
            if let Some(edge_id) = next {
                cursor.2 = Some(edge_id);
                heap.push(Reverse((edge_id, index)));
            }
        }

        let mut last_seen = None;
        while let Some(Reverse((edge_id, cursor_index))) = heap.pop() {
            let cursor = &mut cursors[cursor_index];
            let next = if cursor.1 {
                memtable.next_visible_edge_from_endpoint_after(
                    cursor.0,
                    params.label_filter_ids,
                    params.snapshot_seq,
                    cursor.2,
                )
            } else {
                memtable.next_visible_edge_to_endpoint_after(
                    cursor.0,
                    params.label_filter_ids,
                    params.snapshot_seq,
                    cursor.2,
                )
            };
            if let Some(next_id) = next {
                cursor.2 = Some(next_id);
                heap.push(Reverse((next_id, cursor_index)));
            }

            if last_seen == Some(edge_id) {
                continue;
            }
            last_seen = Some(edge_id);
            if is_shadowed(edge_id) {
                continue;
            }
            if Self::push_edge_match_limited(result, edge_id, params.limit).is_break() {
                break;
            }
        }
    }

    fn append_segment_endpoint_matches_limited(
        result: &mut Vec<u64>,
        segment: &SegmentReader,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
        mut is_shadowed: impl FnMut(u64) -> bool,
    ) -> Result<(), EngineError> {
        if result.len() >= limit {
            return Ok(());
        }

        let mut cursors =
            segment.endpoint_adj_posting_cursors(node_ids, direction, label_filter_ids)?;
        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter_mut().enumerate() {
            if let Some(edge_id) = segment.next_adj_posting_edge_id(cursor)? {
                heap.push(Reverse((edge_id, index)));
            }
        }

        let mut last_seen = None;
        while let Some(Reverse((edge_id, cursor_index))) = heap.pop() {
            let cursor = &mut cursors[cursor_index];
            if let Some(next_id) = segment.next_adj_posting_edge_id(cursor)? {
                heap.push(Reverse((next_id, cursor_index)));
            }

            if last_seen == Some(edge_id) {
                continue;
            }
            last_seen = Some(edge_id);
            if is_shadowed(edge_id) {
                continue;
            }
            if Self::push_edge_match_limited(result, edge_id, limit).is_break() {
                break;
            }
        }

        Ok(())
    }

    fn endpoint_edge_scan_sources<'b>(
        &'b self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&'b [u32]>,
        start_after: Option<u64>,
    ) -> Result<Vec<(EndpointEdgeScanLayer, EndpointEdgeScanSource<'b>)>, EngineError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_node_ids = node_ids.to_vec();
        sorted_node_ids.sort_unstable();
        sorted_node_ids.dedup();

        let mut sources = Vec::with_capacity(1 + self.immutable.len() + self.segments.len());
        sources.push((
            EndpointEdgeScanLayer::Active,
            EndpointEdgeScanSource::memtable(
                self.active,
                &sorted_node_ids,
                direction,
                label_filter_ids,
                self.snapshot_seq,
                start_after,
            ),
        ));
        for (index, epoch) in self.immutable.iter().enumerate() {
            sources.push((
                EndpointEdgeScanLayer::Immutable(index),
                EndpointEdgeScanSource::memtable(
                    &epoch.memtable,
                    &sorted_node_ids,
                    direction,
                    label_filter_ids,
                    self.snapshot_seq,
                    start_after,
                ),
            ));
        }
        for (index, segment) in self.segments.iter().enumerate() {
            sources.push((
                EndpointEdgeScanLayer::Segment(index),
                EndpointEdgeScanSource::segment(
                    segment.as_ref(),
                    &sorted_node_ids,
                    direction,
                    label_filter_ids,
                    start_after,
                )?,
            ));
        }
        Ok(sources)
    }

    fn endpoint_scan_source_shadowed(&self, layer: EndpointEdgeScanLayer, edge_id: u64) -> bool {
        match layer {
            EndpointEdgeScanLayer::Active => false,
            EndpointEdgeScanLayer::Immutable(index) => {
                self.is_edge_shadowed_above_immutable(edge_id, index)
            }
            EndpointEdgeScanLayer::Segment(index) => {
                self.is_edge_shadowed_above_segment(edge_id, index)
            }
        }
    }

    pub(crate) fn scan_edge_ids_by_endpoints_after<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        start_after: Option<u64>,
        chunk_limit: usize,
        mut visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        let mut sources =
            self.endpoint_edge_scan_sources(node_ids, direction, label_filter_ids, start_after)?;
        let mut heap = BinaryHeap::new();
        for (source_index, (_, source)) in sources.iter_mut().enumerate() {
            if let Some(edge_id) = source.next_id()? {
                heap.push(Reverse((edge_id, source_index)));
            }
        }

        let chunk_limit = chunk_limit.max(1);
        let mut chunk = Vec::with_capacity(chunk_limit);
        let mut last_seen = None;
        while let Some(Reverse((edge_id, source_index))) = heap.pop() {
            if let Some(next_id) = sources[source_index].1.next_id()? {
                heap.push(Reverse((next_id, source_index)));
            }

            if last_seen == Some(edge_id) {
                continue;
            }
            last_seen = Some(edge_id);
            let layer = sources[source_index].0;
            if self.endpoint_scan_source_shadowed(layer, edge_id) {
                continue;
            }

            chunk.push(edge_id);
            if chunk.len() >= chunk_limit {
                if visitor(&chunk)?.is_break() {
                    return Ok(());
                }
                chunk.clear();
            }
        }

        if !chunk.is_empty() {
            let _ = visitor(&chunk)?;
        }
        Ok(())
    }

    pub(crate) fn scan_edge_ids_by_endpoints<F>(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        chunk_limit: usize,
        visitor: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(&[u64]) -> Result<ControlFlow<()>, EngineError>,
    {
        self.scan_edge_ids_by_endpoints_after(
            node_ids,
            direction,
            label_filter_ids,
            None,
            chunk_limit,
            visitor,
        )
    }

    fn optional_edge_index_or_scan(
        sidecar_result: Result<Option<ControlFlow<()>>, EngineError>,
        scan: impl FnOnce() -> Result<ControlFlow<()>, EngineError>,
    ) -> Result<ControlFlow<()>, EngineError> {
        match sidecar_result {
            Ok(Some(flow)) => Ok(flow),
            Ok(None) | Err(EngineError::CorruptRecord(_)) => scan(),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn edge_ids_by_label_id(&self, label_id: u32) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();

        Self::append_edge_matches(
            &mut result,
            self.active
                .visible_edges_by_label_id(label_id, self.snapshot_seq),
        );
        for (index, epoch) in self.immutable.iter().enumerate() {
            Self::append_edge_matches_filtered(
                &mut result,
                epoch
                    .memtable
                    .visible_edges_by_label_id(label_id, self.snapshot_seq),
                |id| self.is_edge_shadowed_above_immutable(id, index),
            );
        }
        for (index, seg) in self.segments.iter().enumerate() {
            Self::append_edge_matches_filtered(
                &mut result,
                seg.edges_by_label_id(label_id)?,
                |id| self.is_edge_shadowed_above_segment(id, index),
            );
        }

        result.sort_unstable();
        result.dedup();
        Ok(result)
    }

    pub(crate) fn edge_ids_by_triple(
        &self,
        from: u64,
        to: u64,
        label_id: u32,
    ) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();

        Self::append_edge_matches(
            &mut result,
            self.active
                .edge_ids_by_triple_at(from, to, label_id, self.snapshot_seq),
        );
        for (index, epoch) in self.immutable.iter().enumerate() {
            Self::append_edge_matches_filtered(
                &mut result,
                epoch
                    .memtable
                    .edge_ids_by_triple_at(from, to, label_id, self.snapshot_seq),
                |id| self.is_edge_shadowed_above_immutable(id, index),
            );
        }
        for (index, seg) in self.segments.iter().enumerate() {
            Self::append_edge_matches_filtered(
                &mut result,
                seg.edge_ids_by_triple(from, to, label_id)?,
                |id| self.is_edge_shadowed_above_segment(id, index),
            );
        }

        result.sort_unstable();
        result.dedup();
        Ok(result)
    }

    pub(crate) fn edge_ids_by_endpoints_limited(
        &self,
        node_ids: &[u64],
        direction: Direction,
        label_filter_ids: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_node_ids = node_ids.to_vec();
        sorted_node_ids.sort_unstable();
        sorted_node_ids.dedup();

        let mut result = Vec::new();

        Self::append_memtable_endpoint_matches_limited(
            &mut result,
            self.active,
            &sorted_node_ids,
            MemtableEndpointLimit {
                direction,
                label_filter_ids,
                snapshot_seq: self.snapshot_seq,
                limit,
            },
            |_| false,
        );
        if result.len() >= limit {
            return Ok(Self::finalize_edge_matches(result));
        }

        for (index, epoch) in self.immutable.iter().enumerate() {
            Self::append_memtable_endpoint_matches_limited(
                &mut result,
                &epoch.memtable,
                &sorted_node_ids,
                MemtableEndpointLimit {
                    direction,
                    label_filter_ids,
                    snapshot_seq: self.snapshot_seq,
                    limit,
                },
                |id| self.is_edge_shadowed_above_immutable(id, index),
            );
            if result.len() >= limit {
                return Ok(Self::finalize_edge_matches(result));
            }
        }

        for (index, seg) in self.segments.iter().enumerate() {
            Self::append_segment_endpoint_matches_limited(
                &mut result,
                seg,
                &sorted_node_ids,
                direction,
                label_filter_ids,
                limit,
                |id| self.is_edge_shadowed_above_segment(id, index),
            )?;
            if result.len() >= limit {
                return Ok(Self::finalize_edge_matches(result));
            }
        }

        result.sort_unstable();
        result.dedup();
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_weight_range(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
    ) -> Result<Vec<u64>, EngineError> {
        self.edge_ids_by_weight_range_limited(label_id, bounds, usize::MAX)
    }

    pub(crate) fn edge_ids_by_weight_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<f32>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();
        if self
            .active
            .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::weight_matches_bounds(meta.weight, bounds)
                {
                    Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break()
        {
            return Ok(Self::finalize_edge_matches(result));
        }
        for (index, epoch) in self.immutable.iter().enumerate() {
            if epoch
                .memtable
                .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                    if label_id.is_none_or(|target| meta.label_id == target)
                        && crate::edge_metadata::weight_matches_bounds(meta.weight, bounds)
                        && !self.is_edge_shadowed_above_immutable(meta.edge_id, index)
                    {
                        Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break()
            {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        for (index, seg) in self.segments.iter().enumerate() {
            let mut push = |edge_id| {
                if self.is_edge_shadowed_above_segment(edge_id, index) {
                    ControlFlow::Continue(())
                } else {
                    Self::push_edge_match_limited(&mut result, edge_id, limit)
                }
            };
            let flow = Self::optional_edge_index_or_scan(
                seg.for_each_edge_id_by_weight_range(label_id, bounds, &mut push),
                || {
                    seg.for_each_edge_metadata(|meta| {
                        if label_id.is_none_or(|target| meta.label_id == target)
                            && crate::edge_metadata::weight_matches_bounds(meta.weight, bounds)
                        {
                            push(meta.edge_id)
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                },
            )?;
            if flow.is_break() {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        Ok(Self::finalize_edge_matches(result))
    }

    #[cfg(test)]
    pub(crate) fn edge_ids_by_updated_at_range(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
    ) -> Result<Vec<u64>, EngineError> {
        self.edge_ids_by_updated_at_range_limited(label_id, bounds, usize::MAX)
    }

    pub(crate) fn edge_ids_by_updated_at_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();
        if self
            .active
            .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::i64_matches_bounds(meta.updated_at, bounds)
                {
                    Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break()
        {
            return Ok(Self::finalize_edge_matches(result));
        }
        for (index, epoch) in self.immutable.iter().enumerate() {
            if epoch
                .memtable
                .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                    if label_id.is_none_or(|target| meta.label_id == target)
                        && crate::edge_metadata::i64_matches_bounds(meta.updated_at, bounds)
                        && !self.is_edge_shadowed_above_immutable(meta.edge_id, index)
                    {
                        Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break()
            {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        for (index, seg) in self.segments.iter().enumerate() {
            let mut push = |edge_id| {
                if self.is_edge_shadowed_above_segment(edge_id, index) {
                    ControlFlow::Continue(())
                } else {
                    Self::push_edge_match_limited(&mut result, edge_id, limit)
                }
            };
            let flow = Self::optional_edge_index_or_scan(
                seg.for_each_edge_id_by_updated_at_range(label_id, bounds, &mut push),
                || {
                    seg.for_each_edge_metadata(|meta| {
                        if label_id.is_none_or(|target| meta.label_id == target)
                            && crate::edge_metadata::i64_matches_bounds(meta.updated_at, bounds)
                        {
                            push(meta.edge_id)
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                },
            )?;
            if flow.is_break() {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        Ok(Self::finalize_edge_matches(result))
    }

    pub(crate) fn edge_ids_by_valid_from_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();
        if self
            .active
            .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::i64_matches_bounds(meta.valid_from, bounds)
                {
                    Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break()
        {
            return Ok(Self::finalize_edge_matches(result));
        }
        for (index, epoch) in self.immutable.iter().enumerate() {
            if epoch
                .memtable
                .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                    if label_id.is_none_or(|target| meta.label_id == target)
                        && crate::edge_metadata::i64_matches_bounds(meta.valid_from, bounds)
                        && !self.is_edge_shadowed_above_immutable(meta.edge_id, index)
                    {
                        Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break()
            {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        for (index, seg) in self.segments.iter().enumerate() {
            let mut push = |edge_id| {
                if self.is_edge_shadowed_above_segment(edge_id, index) {
                    ControlFlow::Continue(())
                } else {
                    Self::push_edge_match_limited(&mut result, edge_id, limit)
                }
            };
            let flow = Self::optional_edge_index_or_scan(
                seg.for_each_edge_id_by_valid_from_range(label_id, bounds, &mut push),
                || {
                    seg.for_each_edge_metadata(|meta| {
                        if label_id.is_none_or(|target| meta.label_id == target)
                            && crate::edge_metadata::i64_matches_bounds(meta.valid_from, bounds)
                        {
                            push(meta.edge_id)
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                },
            )?;
            if flow.is_break() {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        Ok(Self::finalize_edge_matches(result))
    }

    pub(crate) fn edge_ids_by_valid_to_range_limited(
        &self,
        label_id: Option<u32>,
        bounds: RangeBoundFlags<i64>,
        limit: usize,
    ) -> Result<Vec<u64>, EngineError> {
        let mut result = Vec::new();
        if self
            .active
            .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                if label_id.is_none_or(|target| meta.label_id == target)
                    && crate::edge_metadata::i64_matches_bounds(meta.valid_to, bounds)
                {
                    Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break()
        {
            return Ok(Self::finalize_edge_matches(result));
        }
        for (index, epoch) in self.immutable.iter().enumerate() {
            if epoch
                .memtable
                .for_each_edge_metadata_at(self.snapshot_seq, |meta| {
                    if label_id.is_none_or(|target| meta.label_id == target)
                        && crate::edge_metadata::i64_matches_bounds(meta.valid_to, bounds)
                        && !self.is_edge_shadowed_above_immutable(meta.edge_id, index)
                    {
                        Self::push_edge_match_limited(&mut result, meta.edge_id, limit)
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break()
            {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        for (index, seg) in self.segments.iter().enumerate() {
            let mut push = |edge_id| {
                if self.is_edge_shadowed_above_segment(edge_id, index) {
                    ControlFlow::Continue(())
                } else {
                    Self::push_edge_match_limited(&mut result, edge_id, limit)
                }
            };
            let flow = Self::optional_edge_index_or_scan(
                seg.for_each_edge_id_by_valid_to_range(label_id, bounds, &mut push),
                || {
                    seg.for_each_edge_metadata(|meta| {
                        if label_id.is_none_or(|target| meta.label_id == target)
                            && crate::edge_metadata::i64_matches_bounds(meta.valid_to, bounds)
                        {
                            push(meta.edge_id)
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                },
            )?;
            if flow.is_break() {
                return Ok(Self::finalize_edge_matches(result));
            }
        }
        Ok(Self::finalize_edge_matches(result))
    }

    pub(crate) fn edge_ids_by_secondary_eq_hashes_limited_read(
        &self,
        index_id: u64,
        value_hashes: &[u64],
        limit: usize,
    ) -> Result<LimitedEdgeIndexRead, EngineError> {
        let mut result = Vec::new();
        let mut raw_remaining = limit;

        for &value_hash in value_hashes {
            if raw_remaining == 0 {
                return Ok(LimitedEdgeIndexRead::TooBroad);
            }
            let ids = self.active.find_secondary_eq_edges_by_hash_at_limited(
                index_id,
                value_hash,
                self.snapshot_seq,
                Some(raw_remaining),
            );
            raw_remaining = raw_remaining.saturating_sub(ids.len());
            Self::append_edge_matches(&mut result, ids);
            if result.len() >= limit {
                return Ok(LimitedEdgeIndexRead::Ready(Self::finalize_edge_matches(
                    result,
                )));
            }
        }

        for (index, epoch) in self.immutable.iter().enumerate() {
            for &value_hash in value_hashes {
                if raw_remaining == 0 {
                    return Ok(LimitedEdgeIndexRead::TooBroad);
                }
                let ids = epoch.memtable.find_secondary_eq_edges_by_hash_at_limited(
                    index_id,
                    value_hash,
                    self.snapshot_seq,
                    Some(raw_remaining),
                );
                raw_remaining = raw_remaining.saturating_sub(ids.len());
                Self::append_edge_matches_filtered(&mut result, ids, |id| {
                    self.is_edge_shadowed_above_immutable(id, index)
                });
                if result.len() >= limit {
                    return Ok(LimitedEdgeIndexRead::Ready(Self::finalize_edge_matches(
                        result,
                    )));
                }
            }
        }

        for (index, seg) in self.segments.iter().enumerate() {
            for &value_hash in value_hashes {
                let mut posting_offset = 0usize;
                loop {
                    if raw_remaining == 0 {
                        return Ok(LimitedEdgeIndexRead::TooBroad);
                    }
                    let raw_limit = raw_remaining.min(256);
                    let Some(chunk) = seg.edge_secondary_eq_posting_chunk_if_present(
                        index_id,
                        value_hash,
                        posting_offset,
                        raw_limit,
                    )?
                    else {
                        return Ok(LimitedEdgeIndexRead::MissingSidecar);
                    };
                    raw_remaining =
                        raw_remaining.saturating_sub(chunk.next_offset - posting_offset);
                    posting_offset = chunk.next_offset;
                    Self::append_edge_matches_filtered(&mut result, chunk.ids, |id| {
                        self.is_edge_shadowed_above_segment(id, index)
                    });
                    if result.len() >= limit {
                        return Ok(LimitedEdgeIndexRead::Ready(Self::finalize_edge_matches(
                            result,
                        )));
                    }
                    if chunk.exhausted {
                        break;
                    }
                }
            }
        }

        Ok(LimitedEdgeIndexRead::Ready(Self::finalize_edge_matches(
            result,
        )))
    }

    pub(crate) fn edge_ids_by_secondary_range_index_limited(
        &self,
        index_id: u64,
        lower: Option<(NumericRangeSortKey, bool)>,
        upper: Option<(NumericRangeSortKey, bool)>,
        limit: usize,
    ) -> Result<Option<Vec<u64>>, EngineError> {
        let mut result = Vec::new();
        let flow = self.active.for_each_visible_secondary_range_entry_at(
            index_id,
            lower,
            upper,
            None,
            self.snapshot_seq,
            &mut |(_, edge_id)| Self::push_edge_match_limited(&mut result, edge_id, limit),
        );
        if flow.is_break() {
            return Ok(Some(Self::finalize_edge_matches(result)));
        }

        for (index, epoch) in self.immutable.iter().enumerate() {
            let flow = epoch.memtable.for_each_visible_secondary_range_entry_at(
                index_id,
                lower,
                upper,
                None,
                self.snapshot_seq,
                &mut |(_, edge_id)| {
                    if self.is_edge_shadowed_above_immutable(edge_id, index) {
                        return ControlFlow::Continue(());
                    }
                    if Self::push_edge_match_limited(&mut result, edge_id, limit).is_break() {
                        return ControlFlow::Break(());
                    }
                    ControlFlow::Continue(())
                },
            );
            if flow.is_break() {
                return Ok(Some(Self::finalize_edge_matches(result)));
            }
        }

        for (index, seg) in self.segments.iter().enumerate() {
            let mut after = None;
            loop {
                if result.len() >= limit {
                    return Ok(Some(Self::finalize_edge_matches(result)));
                }
                let remaining = limit.saturating_sub(result.len()).min(256);
                let Some(entries) = seg.find_edges_by_secondary_range_index_if_present_limited(
                    index_id,
                    lower,
                    upper,
                    after,
                    Some(remaining),
                )?
                else {
                    return Ok(None);
                };
                if entries.is_empty() {
                    break;
                }
                after = entries.last().copied();
                for (_, edge_id) in entries {
                    if self.is_edge_shadowed_above_segment(edge_id, index) {
                        continue;
                    }
                    if Self::push_edge_match_limited(&mut result, edge_id, limit).is_break() {
                        return Ok(Some(Self::finalize_edge_matches(result)));
                    }
                }
            }
        }

        Ok(Some(Self::finalize_edge_matches(result)))
    }

    pub(crate) fn find_edge_properties(
        &self,
        ids: &[u64],
        prop_keys: &[String],
    ) -> Result<Vec<Option<BTreeMap<String, PropValue>>>, EngineError> {
        Ok(self
            .find_edge_projected_fields(
                ids,
                &EdgeSelectedFieldNeeds {
                    props: PropertySelection::Keys(prop_keys.to_vec()),
                    ..EdgeSelectedFieldNeeds::default()
                },
            )?
            .into_iter()
            .map(|fields| fields.map(|fields| fields.props))
            .collect())
    }

    fn is_node_tombstoned_above_immutable(&self, node_id: u64, imm_idx: usize) -> bool {
        if self.active.is_node_deleted_at(node_id, self.snapshot_seq) {
            return true;
        }
        self.immutable[..imm_idx].iter().any(|epoch| {
            epoch
                .memtable
                .is_node_deleted_at(node_id, self.snapshot_seq)
        })
    }

    fn is_edge_shadowed_above_immutable(&self, edge_id: u64, imm_idx: usize) -> bool {
        if !matches!(
            self.active
                .edge_visibility_state_at(edge_id, self.snapshot_seq),
            EdgeVisibilityState::Missing
        ) {
            return true;
        }
        self.immutable[..imm_idx].iter().any(|epoch| {
            !matches!(
                epoch
                    .memtable
                    .edge_visibility_state_at(edge_id, self.snapshot_seq),
                EdgeVisibilityState::Missing
            )
        })
    }

    fn is_edge_shadowed_above_segment(&self, edge_id: u64, seg_idx: usize) -> bool {
        if !matches!(
            self.active
                .edge_visibility_state_at(edge_id, self.snapshot_seq),
            EdgeVisibilityState::Missing
        ) {
            return true;
        }
        for epoch in self.immutable {
            if !matches!(
                epoch
                    .memtable
                    .edge_visibility_state_at(edge_id, self.snapshot_seq),
                EdgeVisibilityState::Missing
            ) {
                return true;
            }
        }
        self.segments[..seg_idx]
            .iter()
            .any(|seg| seg.has_edge(edge_id) || seg.is_edge_deleted(edge_id))
    }

    fn is_edge_tombstoned_above_immutable(&self, edge_id: u64, imm_idx: usize) -> bool {
        if self.active.is_edge_deleted_at(edge_id, self.snapshot_seq) {
            return true;
        }
        self.immutable[..imm_idx].iter().any(|epoch| {
            epoch
                .memtable
                .is_edge_deleted_at(edge_id, self.snapshot_seq)
        })
    }

    fn is_edge_tombstoned_above_segment(&self, edge_id: u64, seg_idx: usize) -> bool {
        if self.active.is_edge_deleted_at(edge_id, self.snapshot_seq) {
            return true;
        }
        for epoch in self.immutable {
            if epoch
                .memtable
                .is_edge_deleted_at(edge_id, self.snapshot_seq)
            {
                return true;
            }
        }
        self.segments[..seg_idx]
            .iter()
            .any(|seg| seg.is_edge_deleted(edge_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::degree_cache::DegreeOverlaySnapshot;
    use crate::edge_metadata::EDGE_WEIGHT_INDEX_LOGICAL_NAME;
    use crate::memtable::Memtable;
    use crate::segment_components::{
        decode_manifest_envelope, encode_manifest_envelope, SegmentComponentKind,
        SEGMENT_COMPONENT_MANIFEST_FILENAME,
    };
    use crate::segment_writer::write_segment_without_degree_sidecar_for_test;
    use crate::types::WalOp;

    fn wrap_imm(mt: Memtable) -> ReadViewImmutableEpoch {
        ReadViewImmutableEpoch {
            epoch_id: 0,
            wal_generation_id: 0,
            memtable: Arc::new(mt),
            degree_overlay: DegreeOverlaySnapshot::empty(),
            in_flight: false,
        }
    }

    fn remove_manifest_component_for_test(seg_dir: &std::path::Path, kind: SegmentComponentKind) {
        let manifest_path = seg_dir.join(SEGMENT_COMPONENT_MANIFEST_FILENAME);
        let data = std::fs::read(&manifest_path).unwrap();
        let mut manifest = decode_manifest_envelope(&data).unwrap();
        let original_len = manifest.components.len();
        manifest.components.retain(|record| record.kind != kind);
        assert_ne!(
            manifest.components.len(),
            original_len,
            "missing component {:?}",
            kind
        );
        std::fs::write(&manifest_path, encode_manifest_envelope(&manifest).unwrap()).unwrap();
    }

    fn sources_for<'a>(
        active: &'a Memtable,
        immutable: &'a [ReadViewImmutableEpoch],
        snapshot_seq: u64,
    ) -> SourceList<'a> {
        SourceList {
            active,
            immutable,
            segments: &[],
            snapshot_seq,
            selected_field_read_counters: None,
        }
    }

    fn make_node(id: u64, key: &str, label_id: u32) -> NodeRecord {
        NodeRecord {
            id,
            key: key.to_string(),
            label_ids: NodeLabelSet::single(label_id).unwrap(),
            props: Default::default(),
            created_at: 1000,
            updated_at: 1000,
            weight: 1.0,
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
            props: Default::default(),
            created_at: 1000,
            updated_at: 1000,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }

    #[test]
    fn test_find_node_active_memtable() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);

        let sources = sources_for(&mt, &[], 1);
        let node = sources.find_node(1).unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().key, "a");
        assert!(sources.find_node(999).unwrap().is_none());
    }

    #[test]
    fn test_find_node_tombstoned_in_active() {
        let mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 10,
            },
            2,
        );

        let sources = sources_for(&mt, &[], 2);
        assert!(sources.find_node(1).unwrap().is_none());
    }

    #[test]
    fn test_find_node_immutable_memtable() {
        let active = Memtable::new();
        let imm = {
            let mt = Memtable::new();
            mt.apply_op(&WalOp::UpsertNode(make_node(7, "frozen", 1)), 1);
            mt
        };
        let immutable = vec![wrap_imm(imm)];

        let sources = sources_for(&active, &immutable, 1);
        let node = sources.find_node(7).unwrap().unwrap();
        assert_eq!(node.key, "frozen");
    }

    #[test]
    fn test_find_node_by_label_key_snapshot_correct() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "alice", 1)), 1);
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2,
            },
            2,
        );
        active.apply_op(&WalOp::UpsertNode(make_node(2, "alice", 1)), 3);

        let old = sources_for(&active, &[], 1)
            .find_node_by_label_key(1, "alice")
            .unwrap()
            .unwrap();
        assert_eq!(old.id, 1);

        assert!(sources_for(&active, &[], 2)
            .find_node_by_label_key(1, "alice")
            .unwrap()
            .is_none());

        let new = sources_for(&active, &[], 3)
            .find_node_by_label_key(1, "alice")
            .unwrap()
            .unwrap();
        assert_eq!(new.id, 2);
    }

    #[test]
    fn test_find_edge_by_triple_snapshot_correct() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 2,
            },
            2,
        );
        active.apply_op(&WalOp::UpsertEdge(make_edge(2, 10, 20, 1)), 3);

        let old = sources_for(&active, &[], 1)
            .find_edge_by_triple(10, 20, 1)
            .unwrap()
            .unwrap();
        assert_eq!(old.id, 1);

        assert!(sources_for(&active, &[], 2)
            .find_edge_by_triple(10, 20, 1)
            .unwrap()
            .is_none());

        let new = sources_for(&active, &[], 3)
            .find_edge_by_triple(10, 20, 1)
            .unwrap()
            .unwrap();
        assert_eq!(new.id, 2);
    }

    #[test]
    fn test_edge_source_helpers_cover_active_and_frozen_memtables() {
        let active = Memtable::new();
        let mut active_edge = make_edge(10, 1, 2, 5);
        active_edge.weight = -0.0;
        active_edge.updated_at = 100;
        active.apply_op(&WalOp::UpsertEdge(active_edge), 2);

        let frozen = {
            let mt = Memtable::new();
            let mut edge = make_edge(20, 2, 3, 5);
            edge.weight = 0.0;
            edge.updated_at = 200;
            mt.apply_op(&WalOp::UpsertEdge(edge), 1);
            mt
        };
        let immutable = vec![wrap_imm(frozen)];
        let sources = sources_for(&active, &immutable, 2);

        assert_eq!(sources.edge_ids_by_label_id(5).unwrap(), vec![10, 20]);
        assert_eq!(
            sources
                .edge_ids_by_endpoints_limited(&[2], Direction::Both, Some(&[5]), usize::MAX)
                .unwrap(),
            vec![10, 20]
        );
        assert_eq!(
            sources
                .edge_ids_by_weight_range(
                    Some(5),
                    RangeBoundFlags::inclusive(Some(0.0), Some(0.0)),
                )
                .unwrap(),
            vec![10, 20]
        );
        assert_eq!(
            sources
                .edge_ids_by_updated_at_range(
                    Some(5),
                    RangeBoundFlags::inclusive(Some(150), Some(250)),
                )
                .unwrap(),
            vec![20]
        );
    }

    #[test]
    fn test_scan_edge_ids_by_endpoints_streams_chunks_and_skips_shadowed_sources() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(11, 1, 3, 5)), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(12, 1, 4, 5)), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(14, 1, 5, 6)), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(9, 1, 6, 5)), 2);
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 11,
                deleted_at: 3000,
            },
            3,
        );

        let frozen = {
            let mt = Memtable::new();
            mt.apply_op(&WalOp::UpsertEdge(make_edge(13, 1, 7, 5)), 2);
            mt
        };
        let immutable = vec![wrap_imm(frozen)];
        let sources = SourceList {
            active: &active,
            immutable: &immutable,
            segments: &segments,
            snapshot_seq: 3,
            selected_field_read_counters: None,
        };

        let mut chunks = Vec::new();
        sources
            .scan_edge_ids_by_endpoints(&[1], Direction::Outgoing, Some(&[5]), 2, |chunk| {
                assert!(chunk.len() <= 2);
                chunks.push(chunk.to_vec());
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();
        assert_eq!(chunks, vec![vec![9, 10], vec![12, 13]]);
    }

    #[test]
    fn test_find_edge_properties_projects_across_sources_without_full_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        let mut shadowed = make_edge(10, 1, 2, 5);
        shadowed.props.insert(
            "a".to_string(),
            PropValue::String("segment-old".to_string()),
        );
        shadowed.props.insert("b".to_string(), PropValue::Int(1));
        segment_mt.apply_op(&WalOp::UpsertEdge(shadowed), 1);
        let mut deleted = make_edge(30, 1, 3, 5);
        deleted
            .props
            .insert("a".to_string(), PropValue::String("deleted".to_string()));
        segment_mt.apply_op(&WalOp::UpsertEdge(deleted), 1);
        let mut segment_only = make_edge(40, 1, 4, 5);
        segment_only
            .props
            .insert("a".to_string(), PropValue::String("segment".to_string()));
        segment_only
            .props
            .insert("b".to_string(), PropValue::Int(40));
        segment_only
            .props
            .insert("ignored".to_string(), PropValue::Bool(true));
        segment_mt.apply_op(&WalOp::UpsertEdge(segment_only), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        let mut active_edge = make_edge(10, 1, 2, 5);
        active_edge
            .props
            .insert("a".to_string(), PropValue::String("active".to_string()));
        active_edge
            .props
            .insert("b".to_string(), PropValue::Int(10));
        active.apply_op(&WalOp::UpsertEdge(active_edge), 2);
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 30,
                deleted_at: 3,
            },
            3,
        );

        let frozen = Memtable::new();
        let mut frozen_edge = make_edge(20, 2, 3, 5);
        frozen_edge
            .props
            .insert("a".to_string(), PropValue::String("frozen".to_string()));
        frozen_edge
            .props
            .insert("b".to_string(), PropValue::Int(20));
        frozen.apply_op(&WalOp::UpsertEdge(frozen_edge), 2);
        let immutable = vec![wrap_imm(frozen)];

        let sources = SourceList {
            active: &active,
            immutable: &immutable,
            segments: &segments,
            snapshot_seq: 3,
            selected_field_read_counters: None,
        };
        let props = sources
            .find_edge_properties(
                &[10, 20, 30, 40, 999],
                &["a".to_string(), "b".to_string(), "missing".to_string()],
            )
            .unwrap();

        assert_eq!(
            props[0].as_ref().unwrap().get("a"),
            Some(&PropValue::String("active".to_string()))
        );
        assert_eq!(
            props[0].as_ref().unwrap().get("b"),
            Some(&PropValue::Int(10))
        );
        assert_eq!(
            props[1].as_ref().unwrap().get("a"),
            Some(&PropValue::String("frozen".to_string()))
        );
        assert_eq!(
            props[1].as_ref().unwrap().get("b"),
            Some(&PropValue::Int(20))
        );
        assert!(props[2].is_none());
        assert_eq!(
            props[3].as_ref().unwrap().get("a"),
            Some(&PropValue::String("segment".to_string()))
        );
        assert_eq!(
            props[3].as_ref().unwrap().get("b"),
            Some(&PropValue::Int(40))
        );
        assert!(!props[3].as_ref().unwrap().contains_key("ignored"));
        assert!(props[4].is_none());

        let fields = sources
            .find_edge_projected_fields(
                &[10, 20, 30, 40, 999],
                &EdgeSelectedFieldNeeds {
                    created_at: true,
                    props: PropertySelection::Keys(vec!["a".to_string()]),
                },
            )
            .unwrap();
        assert_eq!(fields[0].as_ref().unwrap().created_at, Some(1000));
        assert_eq!(
            fields[0].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("active".to_string()))
        );
        assert_eq!(
            fields[1].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("frozen".to_string()))
        );
        assert!(fields[2].is_none());
        assert_eq!(
            fields[3].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("segment".to_string()))
        );
        assert!(fields[4].is_none());
    }

    #[test]
    fn test_find_node_projected_fields_projects_across_sources_without_full_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        let mut shadowed = make_node(10, "segment-old", 5);
        shadowed.props.insert(
            "a".to_string(),
            PropValue::String("segment-old".to_string()),
        );
        shadowed.props.insert("b".to_string(), PropValue::Int(1));
        segment_mt.apply_op(&WalOp::UpsertNode(shadowed), 1);
        let mut deleted = make_node(30, "deleted", 5);
        deleted
            .props
            .insert("a".to_string(), PropValue::String("deleted".to_string()));
        segment_mt.apply_op(&WalOp::UpsertNode(deleted), 1);
        let mut segment_only = make_node(40, "segment", 5);
        segment_only
            .props
            .insert("a".to_string(), PropValue::String("segment".to_string()));
        segment_only
            .props
            .insert("b".to_string(), PropValue::Int(40));
        segment_only
            .props
            .insert("ignored".to_string(), PropValue::Bool(true));
        segment_mt.apply_op(&WalOp::UpsertNode(segment_only), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        let mut active_node = make_node(10, "active", 5);
        active_node
            .props
            .insert("a".to_string(), PropValue::String("active".to_string()));
        active_node
            .props
            .insert("b".to_string(), PropValue::Int(10));
        active.apply_op(&WalOp::UpsertNode(active_node), 2);
        active.apply_op(
            &WalOp::DeleteNode {
                id: 30,
                deleted_at: 3,
            },
            3,
        );

        let frozen = Memtable::new();
        let mut frozen_node = make_node(20, "frozen", 5);
        frozen_node
            .props
            .insert("a".to_string(), PropValue::String("frozen".to_string()));
        frozen_node
            .props
            .insert("b".to_string(), PropValue::Int(20));
        frozen.apply_op(&WalOp::UpsertNode(frozen_node), 2);
        let immutable = vec![wrap_imm(frozen)];

        let sources = SourceList {
            active: &active,
            immutable: &immutable,
            segments: &segments,
            snapshot_seq: 3,
            selected_field_read_counters: None,
        };
        let fields = sources
            .find_node_projected_fields(
                &[10, 20, 30, 40, 999],
                &NodeSelectedFieldNeeds {
                    key: true,
                    props: PropertySelection::Keys(vec![
                        "a".to_string(),
                        "b".to_string(),
                        "missing".to_string(),
                    ]),
                    ..NodeSelectedFieldNeeds::default()
                },
            )
            .unwrap();

        assert_eq!(fields[0].as_ref().unwrap().key.as_deref(), Some("active"));
        assert_eq!(
            fields[0].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("active".to_string()))
        );
        assert_eq!(
            fields[0].as_ref().unwrap().props.get("b"),
            Some(&PropValue::Int(10))
        );
        assert_eq!(fields[1].as_ref().unwrap().key.as_deref(), Some("frozen"));
        assert_eq!(
            fields[1].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("frozen".to_string()))
        );
        assert_eq!(
            fields[1].as_ref().unwrap().props.get("b"),
            Some(&PropValue::Int(20))
        );
        assert!(fields[2].is_none());
        assert_eq!(fields[3].as_ref().unwrap().key.as_deref(), Some("segment"));
        assert_eq!(
            fields[3].as_ref().unwrap().props.get("a"),
            Some(&PropValue::String("segment".to_string()))
        );
        assert_eq!(
            fields[3].as_ref().unwrap().props.get("b"),
            Some(&PropValue::Int(40))
        );
        assert!(!fields[3].as_ref().unwrap().props.contains_key("ignored"));
        assert!(fields[4].is_none());

        let no_key = sources
            .find_node_projected_fields(
                &[40],
                &NodeSelectedFieldNeeds {
                    props: PropertySelection::Keys(vec!["a".to_string()]),
                    ..NodeSelectedFieldNeeds::default()
                },
            )
            .unwrap();
        assert!(no_key[0].as_ref().unwrap().key.is_none());
        assert_eq!(no_key[0].as_ref().unwrap().props.len(), 1);
    }

    #[test]
    fn test_edge_endpoint_limited_early_exit_returns_sorted_deduped_ids() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(30, 1, 2, 5)), 1);
        active.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 3, 5)), 2);
        active.apply_op(&WalOp::UpsertEdge(make_edge(20, 1, 1, 5)), 3);

        let sources = sources_for(&active, &[], 3);
        assert_eq!(
            sources
                .edge_ids_by_endpoints_limited(&[1], Direction::Both, Some(&[5]), 1)
                .unwrap(),
            vec![10]
        );
    }

    #[test]
    fn test_segment_endpoint_limit_counts_only_unshadowed_unique_edges() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 1, 5)), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(20, 2, 1, 5)), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(make_edge(30, 3, 1, 5)), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(10, 9, 9, 6)), 2);
        let sources = SourceList {
            active: &active,
            immutable: &[],
            segments: &segments,
            snapshot_seq: 2,
            selected_field_read_counters: None,
        };

        assert_eq!(
            sources
                .edge_ids_by_endpoints_limited(&[1], Direction::Both, Some(&[5]), 2)
                .unwrap(),
            vec![20, 30]
        );
    }

    #[test]
    fn test_memtable_triple_source_returns_parallel_edges_from_adjacency() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(30, 1, 2, 5)), 1);
        active.apply_op(&WalOp::UpsertEdge(make_edge(10, 1, 2, 5)), 2);
        active.apply_op(&WalOp::UpsertEdge(make_edge(20, 1, 3, 5)), 3);

        let sources = sources_for(&active, &[], 3);
        assert_eq!(sources.edge_ids_by_triple(1, 2, 5).unwrap(), vec![10, 30]);
    }

    #[test]
    fn test_node_visibility_meta_resolves_live_deleted_and_missing() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "live", 7)), 1);
        active.apply_op(&WalOp::UpsertNode(make_node(2, "deleted", 7)), 2);
        active.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 3,
            },
            3,
        );

        let sources = sources_for(&active, &[], 3);
        let states = sources.find_node_visibility_meta(&[1, 2, 3]).unwrap();
        assert!(matches!(
            states[0],
            NodeVisibilityState::Live(meta) if meta.label_ids.as_slice() == [7]
        ));
        assert_eq!(states[1], NodeVisibilityState::Deleted);
        assert_eq!(states[2], NodeVisibilityState::Missing);
    }

    #[test]
    fn test_edge_source_helpers_shadow_older_segment_versions() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        let mut old = make_edge(10, 1, 2, 5);
        old.weight = 1.0;
        segment_mt.apply_op(&WalOp::UpsertEdge(old), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        let mut newer = make_edge(10, 1, 2, 6);
        newer.weight = 2.0;
        active.apply_op(&WalOp::UpsertEdge(newer), 2);
        let sources = SourceList {
            active: &active,
            immutable: &[],
            segments: &segments,
            snapshot_seq: 2,
            selected_field_read_counters: None,
        };

        assert_eq!(sources.edge_ids_by_label_id(5).unwrap(), Vec::<u64>::new());
        assert_eq!(sources.edge_ids_by_label_id(6).unwrap(), vec![10]);
        assert_eq!(
            sources
                .edge_ids_by_weight_range(None, RangeBoundFlags::inclusive(Some(1.0), Some(1.0)),)
                .unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(
            sources
                .edge_ids_by_weight_range(None, RangeBoundFlags::inclusive(Some(2.0), Some(2.0)),)
                .unwrap(),
            vec![10]
        );
    }

    #[test]
    fn test_edge_metadata_range_falls_back_when_optional_sidecar_missing() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("seg_0001");
        let segment_mt = Memtable::new();
        let mut in_range = make_edge(10, 1, 2, 5);
        in_range.weight = 1.0;
        let mut out_of_range = make_edge(20, 1, 3, 5);
        out_of_range.weight = 4.0;
        segment_mt.apply_op(&WalOp::UpsertEdge(in_range), 1);
        segment_mt.apply_op(&WalOp::UpsertEdge(out_of_range), 1);
        write_segment_without_degree_sidecar_for_test(&seg_dir, 1, &segment_mt, None).unwrap();
        assert!(!seg_dir.join(EDGE_WEIGHT_INDEX_LOGICAL_NAME).exists());
        remove_manifest_component_for_test(&seg_dir, SegmentComponentKind::EdgeWeightIndex);
        let segments = vec![Arc::new(
            SegmentReader::open_unpinned_for_test(&seg_dir, 1, None).unwrap(),
        )];

        let active = Memtable::new();
        let sources = SourceList {
            active: &active,
            immutable: &[],
            segments: &segments,
            snapshot_seq: 1,
            selected_field_read_counters: None,
        };

        assert_eq!(
            sources
                .edge_ids_by_weight_range(
                    Some(5),
                    RangeBoundFlags::inclusive(Some(0.5), Some(2.0)),
                )
                .unwrap(),
            vec![10]
        );
    }

    #[test]
    fn test_optional_edge_index_or_scan_falls_back_on_corrupt_record() {
        let mut scanned = false;
        let flow = SourceList::optional_edge_index_or_scan(
            Err(EngineError::CorruptRecord("bad optional index".into())),
            || {
                scanned = true;
                Ok(ControlFlow::Break(()))
            },
        )
        .unwrap();
        assert!(scanned);
        assert!(flow.is_break());

        let err = SourceList::optional_edge_index_or_scan(
            Err(EngineError::InvalidOperation("hard failure".into())),
            || Ok(ControlFlow::Continue(())),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
    }

    #[test]
    fn test_find_nodes_batch_uses_snapshot_visibility() {
        let active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);
        active.apply_op(&WalOp::UpsertNode(make_node(1, "a2", 1)), 2);
        active.apply_op(&WalOp::UpsertNode(make_node(2, "b", 1)), 3);

        let before = sources_for(&active, &[], 1).find_nodes(&[1, 2]).unwrap();
        assert_eq!(before[0].as_ref().unwrap().key, "a");
        assert!(before[1].is_none());

        let after = sources_for(&active, &[], 3).find_nodes(&[1, 2]).unwrap();
        assert_eq!(after[0].as_ref().unwrap().key, "a2");
        assert_eq!(after[1].as_ref().unwrap().key, "b");
    }

    #[test]
    fn test_collect_deleted_nodes_across_sources() {
        let active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 1,
            },
            1,
        );
        let imm = {
            let mt = Memtable::new();
            mt.apply_op(
                &WalOp::DeleteNode {
                    id: 2,
                    deleted_at: 2,
                },
                2,
            );
            mt
        };
        let immutable = vec![wrap_imm(imm)];
        let sources = sources_for(&active, &immutable, u64::MAX);
        let deleted = sources.collect_deleted_nodes();
        assert!(deleted.contains(&1));
        assert!(deleted.contains(&2));
    }
}
