//! Ordered-source visibility substrate for multi-layer lookups.
//!
//! `SourceList` encapsulates the precedence order:
//!   active memtable > immutable memtables (newest-first) > segments (newest-first)
//!
//! It provides short-circuiting point lookups, key/triple lookups, and deletion
//! checks that consult all live sources in the correct order. Engine read paths
//! delegate to `SourceList` instead of open-coding memtable + segment logic.

use crate::engine::ReadViewImmutableEpoch;
use crate::error::EngineError;
use crate::memtable::Memtable;
use crate::segment_reader::SegmentReader;
use crate::types::*;
use std::sync::Arc;

/// Concrete borrowing struct over the three source layers. This is not a trait,
/// because Memtable and SegmentReader have fundamentally different APIs.
pub struct SourceList<'a> {
    pub(crate) active: &'a Memtable,
    pub(crate) immutable: &'a [ReadViewImmutableEpoch],
    pub(crate) segments: &'a [Arc<SegmentReader>],
    pub(crate) snapshot_seq: u64,
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

    pub fn find_nodes_by_keys<'b>(
        &self,
        keys: &[(u32, &'b str)],
    ) -> Result<Vec<Option<NodeRecord>>, EngineError> {
        let n = keys.len();
        let mut results = vec![None; n];
        if n == 0 {
            return Ok(results);
        }

        let mut remaining: Vec<(usize, u32, &'b str)> = Vec::with_capacity(n);
        for (i, &(type_id, key)) in keys.iter().enumerate() {
            if let Some(node) = self.active.node_by_key_at(type_id, key, self.snapshot_seq) {
                results[i] = Some(node);
            } else {
                remaining.push((i, type_id, key));
            }
        }

        for (epoch_idx, epoch) in self.immutable.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            remaining.retain(|&(i, type_id, key)| {
                if let Some(node) = epoch
                    .memtable
                    .node_by_key_at(type_id, key, self.snapshot_seq)
                {
                    if self.is_node_tombstoned_above_immutable(node.id, epoch_idx) {
                        return false;
                    }
                    results[i] = Some(node);
                    return false;
                }
                true
            });
        }

        if remaining.is_empty() {
            return Ok(results);
        }

        remaining.sort_unstable_by(|left, right| (left.1, left.2).cmp(&(right.1, right.2)));

        let mut deleted_above = self.active.collect_deleted_nodes_at(self.snapshot_seq);
        for epoch in self.immutable {
            deleted_above.extend(epoch.memtable.collect_deleted_nodes_at(self.snapshot_seq));
        }

        for (seg_idx, seg) in self.segments.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }

            let found = seg.resolve_keys_batch(&remaining, &mut results)?;

            for &orig_idx in &found {
                if let Some(node) = results[orig_idx].as_ref() {
                    let tombstoned = deleted_above.contains(&node.id)
                        || self.segments[..seg_idx]
                            .iter()
                            .any(|segment| segment.is_node_deleted(node.id));
                    if tombstoned {
                        results[orig_idx] = None;
                    }
                }
            }

            if !found.is_empty() {
                let mut found_mask = vec![false; n];
                for &idx in &found {
                    found_mask[idx] = true;
                }
                remaining.retain(|&(i, _, _)| !found_mask[i]);
            }
        }

        Ok(results)
    }

    pub fn find_node_by_key(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        if let Some(node) = self.active.node_by_key_at(type_id, key, self.snapshot_seq) {
            return Ok(Some(node));
        }

        for (i, epoch) in self.immutable.iter().enumerate() {
            if let Some(node) = epoch
                .memtable
                .node_by_key_at(type_id, key, self.snapshot_seq)
            {
                if self.is_node_tombstoned_above_immutable(node.id, i) {
                    return Ok(None);
                }
                return Ok(Some(node));
            }
        }

        for (s, seg) in self.segments.iter().enumerate() {
            if let Some(node) = seg.node_by_key(type_id, key)? {
                if self.is_node_tombstoned_above_segment(node.id, s) {
                    return Ok(None);
                }
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    pub fn find_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        if let Some(edge) = self
            .active
            .edge_by_triple_at(from, to, type_id, self.snapshot_seq)
        {
            return Ok(Some(edge));
        }

        for (i, epoch) in self.immutable.iter().enumerate() {
            if let Some(edge) =
                epoch
                    .memtable
                    .edge_by_triple_at(from, to, type_id, self.snapshot_seq)
            {
                if self.is_edge_tombstoned_above_immutable(edge.id, i) {
                    return Ok(None);
                }
                return Ok(Some(edge));
            }
        }

        for (s, seg) in self.segments.iter().enumerate() {
            if let Some(edge) = seg.edge_by_triple(from, to, type_id)? {
                if self.is_edge_tombstoned_above_segment(edge.id, s) {
                    return Ok(None);
                }
                return Ok(Some(edge));
            }
        }

        Ok(None)
    }

    pub fn is_node_deleted(&self, id: u64) -> bool {
        if self.active.get_node_at(id, self.snapshot_seq).is_some() {
            return false;
        }
        if self.active.is_node_deleted_at(id, self.snapshot_seq) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.get_node_at(id, self.snapshot_seq).is_some() {
                return false;
            }
            if epoch.memtable.is_node_deleted_at(id, self.snapshot_seq) {
                return true;
            }
        }
        for seg in self.segments {
            if seg.is_node_deleted(id) {
                return true;
            }
            if seg.has_node(id) {
                return false;
            }
        }
        false
    }

    pub fn is_edge_deleted(&self, id: u64) -> bool {
        if self.active.get_edge_at(id, self.snapshot_seq).is_some() {
            return false;
        }
        if self.active.is_edge_deleted_at(id, self.snapshot_seq) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.get_edge_at(id, self.snapshot_seq).is_some() {
                return false;
            }
            if epoch.memtable.is_edge_deleted_at(id, self.snapshot_seq) {
                return true;
            }
        }
        for seg in self.segments {
            if seg.is_edge_deleted(id) {
                return true;
            }
            if seg.has_edge(id) {
                return false;
            }
        }
        false
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

    fn is_node_tombstoned_above_segment(&self, node_id: u64, seg_idx: usize) -> bool {
        if self.active.is_node_deleted_at(node_id, self.snapshot_seq) {
            return true;
        }
        for epoch in self.immutable {
            if epoch
                .memtable
                .is_node_deleted_at(node_id, self.snapshot_seq)
            {
                return true;
            }
        }
        self.segments[..seg_idx]
            .iter()
            .any(|seg| seg.is_node_deleted(node_id))
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
    use crate::memtable::Memtable;
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
        }
    }

    fn make_node(id: u64, key: &str, type_id: u32) -> NodeRecord {
        NodeRecord {
            id,
            key: key.to_string(),
            type_id,
            props: Default::default(),
            created_at: 1000,
            updated_at: 1000,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }

    fn make_edge(id: u64, from: u64, to: u64, type_id: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            type_id,
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
        assert!(sources.is_node_deleted(1));
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
    fn test_find_node_by_key_snapshot_correct() {
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
            .find_node_by_key(1, "alice")
            .unwrap()
            .unwrap();
        assert_eq!(old.id, 1);

        assert!(sources_for(&active, &[], 2)
            .find_node_by_key(1, "alice")
            .unwrap()
            .is_none());

        let new = sources_for(&active, &[], 3)
            .find_node_by_key(1, "alice")
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
