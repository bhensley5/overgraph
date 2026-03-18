//! Ordered-source visibility substrate for multi-layer lookups.
//!
//! `SourceList` encapsulates the precedence order:
//!   active memtable > immutable memtables (newest-first) > segments (newest-first)
//!
//! It provides short-circuiting point lookups, key/triple lookups, and deletion
//! checks that consult all live sources in the correct order. Engine read paths
//! delegate to `SourceList` instead of open-coding memtable + segment logic.

use crate::engine::ImmutableEpoch;
use crate::error::EngineError;
use crate::memtable::Memtable;
use crate::segment_reader::SegmentReader;
use crate::types::*;

/// Concrete borrowing struct over the three source layers. This is not a trait,
/// because Memtable and SegmentReader have fundamentally different APIs.
pub struct SourceList<'a> {
    pub(crate) active: &'a Memtable,
    pub(crate) immutable: &'a [ImmutableEpoch],
    pub(crate) segments: &'a [SegmentReader],
}

impl<'a> SourceList<'a> {
    /// Find a node by ID across all sources. Short-circuits on the first
    /// source that has an opinion (live record or tombstone).
    ///
    /// Precedence: active > immutable (newest-first) > segments (newest-first).
    pub fn find_node(&self, id: u64) -> Result<Option<NodeRecord>, EngineError> {
        // Active memtable
        if let Some(node) = self.active.get_node(id) {
            return Ok(Some(node.clone()));
        }
        if self.active.deleted_nodes().contains_key(&id) {
            return Ok(None);
        }

        // Immutable memtables (newest-first)
        for epoch in self.immutable {
            if let Some(node) = epoch.memtable.get_node(id) {
                return Ok(Some(node.clone()));
            }
            if epoch.memtable.deleted_nodes().contains_key(&id) {
                return Ok(None);
            }
        }

        // Segments (newest-first)
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
        // Active memtable
        if let Some(edge) = self.active.get_edge(id) {
            return Ok(Some(edge.clone()));
        }
        if self.active.deleted_edges().contains_key(&id) {
            return Ok(None);
        }

        // Immutable memtables (newest-first)
        for epoch in self.immutable {
            if let Some(edge) = epoch.memtable.get_edge(id) {
                return Ok(Some(edge.clone()));
            }
            if epoch.memtable.deleted_edges().contains_key(&id) {
                return Ok(None);
            }
        }

        // Segments (newest-first)
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

    /// Find a node by (type_id, key) across all sources.
    ///
    /// Walks key indexes in precedence order. When a candidate is found in a
    /// lower-precedence source, checks all higher-precedence sources for a
    /// tombstone on that node's ID before returning it.
    pub fn find_node_by_key(
        &self,
        type_id: u32,
        key: &str,
    ) -> Result<Option<NodeRecord>, EngineError> {
        // Active memtable: node_by_key checks internal tombstones, so if found
        // it's guaranteed live within this memtable.
        if let Some(node) = self.active.node_by_key(type_id, key) {
            return Ok(Some(node.clone()));
        }

        // Immutable memtables (newest-first)
        for (i, epoch) in self.immutable.iter().enumerate() {
            if let Some(node) = epoch.memtable.node_by_key(type_id, key) {
                // Check higher-precedence sources for tombstone on this node ID
                if self.is_node_tombstoned_above_immutable(node.id, i) {
                    return Ok(None);
                }
                return Ok(Some(node.clone()));
            }
        }

        // Segments (newest-first)
        for (s, seg) in self.segments.iter().enumerate() {
            if let Some(node) = seg.node_by_key(type_id, key)? {
                // Check all memtables and newer segments for tombstone
                if self.is_node_tombstoned_above_segment(node.id, s) {
                    return Ok(None);
                }
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    /// Find an edge by (from, to, type_id) across all sources.
    ///
    /// Walks triple indexes in precedence order. When a candidate is found in a
    /// lower-precedence source, checks all higher-precedence sources for a
    /// tombstone on that edge's ID before returning it.
    pub fn find_edge_by_triple(
        &self,
        from: u64,
        to: u64,
        type_id: u32,
    ) -> Result<Option<EdgeRecord>, EngineError> {
        // Active memtable
        if let Some(edge) = self.active.edge_by_triple(from, to, type_id) {
            return Ok(Some(edge.clone()));
        }

        // Immutable memtables (newest-first)
        for (i, epoch) in self.immutable.iter().enumerate() {
            if let Some(edge) = epoch.memtable.edge_by_triple(from, to, type_id) {
                if self.is_edge_tombstoned_above_immutable(edge.id, i) {
                    return Ok(None);
                }
                return Ok(Some(edge.clone()));
            }
        }

        // Segments (newest-first)
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

    /// Check if a node ID is deleted in any source.
    ///
    /// A node is considered deleted if ANY source contains a tombstone for it
    /// and no higher-precedence source has a live record for it.
    /// This is equivalent to `find_node(id)?.is_none()` but avoids the
    /// full record clone.
    ///
    /// Uses positional precedence (first opinion wins) rather than comparing
    /// `last_write_seq`. Under the single-writer model with FIFO source ordering,
    /// positional precedence produces identical results to sequence comparison.
    pub fn is_node_deleted(&self, id: u64) -> bool {
        // Walk sources in precedence order. First opinion wins.
        if self.active.nodes().contains_key(&id) {
            return false;
        }
        if self.active.deleted_nodes().contains_key(&id) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.nodes().contains_key(&id) {
                return false;
            }
            if epoch.memtable.deleted_nodes().contains_key(&id) {
                return true;
            }
        }
        for seg in self.segments {
            // Tombstone check first: prevents fallthrough to older segments
            // that might still have a stale record for this ID.
            if seg.is_node_deleted(id) {
                return true;
            }
            if seg.has_node(id) {
                return false;
            }
        }
        // Not found anywhere. Not deleted (just doesn't exist).
        false
    }

    /// Check if an edge ID is deleted in any source.
    pub fn is_edge_deleted(&self, id: u64) -> bool {
        if self.active.edges().contains_key(&id) {
            return false;
        }
        if self.active.deleted_edges().contains_key(&id) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.edges().contains_key(&id) {
                return false;
            }
            if epoch.memtable.deleted_edges().contains_key(&id) {
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

    /// Collect the union of all deleted node IDs across all sources.
    /// Used by traversal and vector search for tombstone pre-filtering.
    pub fn collect_deleted_nodes(&self) -> NodeIdSet {
        let mut deleted = NodeIdSet::default();
        for &id in self.active.deleted_nodes().keys() {
            deleted.insert(id);
        }
        for epoch in self.immutable {
            for &id in epoch.memtable.deleted_nodes().keys() {
                deleted.insert(id);
            }
        }
        for seg in self.segments {
            for &id in seg.deleted_node_tombstones().keys() {
                deleted.insert(id);
            }
        }
        deleted
    }

    /// Collect the union of all deleted edge IDs across all sources.
    pub fn collect_deleted_edges(&self) -> NodeIdSet {
        let mut deleted = NodeIdSet::default();
        for &id in self.active.deleted_edges().keys() {
            deleted.insert(id);
        }
        for epoch in self.immutable {
            for &id in epoch.memtable.deleted_edges().keys() {
                deleted.insert(id);
            }
        }
        for seg in self.segments {
            for &id in seg.deleted_edge_tombstones().keys() {
                deleted.insert(id);
            }
        }
        deleted
    }

    // --- Private helpers ---

    /// Check if a node ID is tombstoned in any source with higher precedence
    /// than immutable memtable at index `imm_idx`.
    /// Higher-precedence sources: active memtable + immutable[0..imm_idx].
    fn is_node_tombstoned_above_immutable(&self, node_id: u64, imm_idx: usize) -> bool {
        if self.active.deleted_nodes().contains_key(&node_id) {
            return true;
        }
        for epoch in &self.immutable[..imm_idx] {
            if epoch.memtable.deleted_nodes().contains_key(&node_id) {
                return true;
            }
        }
        false
    }

    /// Check if a node ID is tombstoned in any source with higher precedence
    /// than segment at index `seg_idx`.
    /// Higher-precedence sources: active + all immutable + segments[0..seg_idx].
    fn is_node_tombstoned_above_segment(&self, node_id: u64, seg_idx: usize) -> bool {
        if self.active.deleted_nodes().contains_key(&node_id) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.deleted_nodes().contains_key(&node_id) {
                return true;
            }
        }
        for seg in &self.segments[..seg_idx] {
            if seg.is_node_deleted(node_id) {
                return true;
            }
        }
        false
    }

    /// Check if an edge ID is tombstoned in any source with higher precedence
    /// than immutable memtable at index `imm_idx`.
    fn is_edge_tombstoned_above_immutable(&self, edge_id: u64, imm_idx: usize) -> bool {
        if self.active.deleted_edges().contains_key(&edge_id) {
            return true;
        }
        for epoch in &self.immutable[..imm_idx] {
            if epoch.memtable.deleted_edges().contains_key(&edge_id) {
                return true;
            }
        }
        false
    }

    /// Check if an edge ID is tombstoned in any source with higher precedence
    /// than segment at index `seg_idx`.
    fn is_edge_tombstoned_above_segment(&self, edge_id: u64, seg_idx: usize) -> bool {
        if self.active.deleted_edges().contains_key(&edge_id) {
            return true;
        }
        for epoch in self.immutable {
            if epoch.memtable.deleted_edges().contains_key(&edge_id) {
                return true;
            }
        }
        for seg in &self.segments[..seg_idx] {
            if seg.is_edge_deleted(edge_id) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::types::WalOp;
    use std::sync::Arc;

    fn wrap_imm(mt: Memtable) -> ImmutableEpoch {
        ImmutableEpoch {
            epoch_id: 0,
            wal_generation_id: 0,
            memtable: Arc::new(mt),
            in_flight: false,
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
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);

        let sources = SourceList {
            active: &mt,
            immutable: &[],
            segments: &[],
        };

        let node = sources.find_node(1).unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().key, "a");

        // Non-existent node
        assert!(sources.find_node(999).unwrap().is_none());
    }

    #[test]
    fn test_find_node_tombstoned_in_active() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);
        mt.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let sources = SourceList {
            active: &mt,
            immutable: &[],
            segments: &[],
        };

        assert!(sources.find_node(1).unwrap().is_none());
    }

    #[test]
    fn test_find_node_immutable_memtable() {
        // Node in immutable, not in active
        let active = Memtable::new();
        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let node = sources.find_node(1).unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().key, "a");
    }

    #[test]
    fn test_find_node_active_wins_over_immutable() {
        // Active has updated version, immutable has older version
        let mut active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "updated", 1)), 2);

        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertNode(make_node(1, "old", 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let node = sources.find_node(1).unwrap().unwrap();
        assert_eq!(node.key, "updated");
    }

    #[test]
    fn test_find_node_active_tombstone_shadows_immutable() {
        // Active tombstones a node that exists in immutable
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(sources.find_node(1).unwrap().is_none());
    }

    #[test]
    fn test_find_node_newer_immutable_wins() {
        let active = Memtable::new();
        let mut imm0 = Memtable::new(); // newest
        imm0.apply_op(&WalOp::UpsertNode(make_node(1, "newer", 1)), 2);
        let mut imm1 = Memtable::new(); // oldest
        imm1.apply_op(&WalOp::UpsertNode(make_node(1, "older", 1)), 1);

        let imm_slice = [wrap_imm(imm0), wrap_imm(imm1)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let node = sources.find_node(1).unwrap().unwrap();
        assert_eq!(node.key, "newer");
    }

    #[test]
    fn test_find_node_immutable_tombstone_shadows_older_immutable() {
        let active = Memtable::new();
        let mut imm0 = Memtable::new(); // newest, has tombstone
        imm0.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );
        let mut imm1 = Memtable::new(); // oldest, has live node
        imm1.apply_op(&WalOp::UpsertNode(make_node(1, "a", 1)), 1);

        let imm_slice = [wrap_imm(imm0), wrap_imm(imm1)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(sources.find_node(1).unwrap().is_none());
    }

    #[test]
    fn test_find_edge_active_memtable() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);

        let sources = SourceList {
            active: &mt,
            immutable: &[],
            segments: &[],
        };

        let edge = sources.find_edge(1).unwrap();
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().from, 10);
    }

    #[test]
    fn test_find_edge_tombstoned() {
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(sources.find_edge(1).unwrap().is_none());
    }

    #[test]
    fn test_find_node_by_key_active() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, "alice", 1)), 1);

        let sources = SourceList {
            active: &mt,
            immutable: &[],
            segments: &[],
        };

        let node = sources.find_node_by_key(1, "alice").unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, 1);

        // Wrong type_id
        assert!(sources.find_node_by_key(2, "alice").unwrap().is_none());
        // Wrong key
        assert!(sources.find_node_by_key(1, "bob").unwrap().is_none());
    }

    #[test]
    fn test_find_node_by_key_tombstoned_in_newer_source() {
        // Node with key in immutable, tombstoned by active
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertNode(make_node(1, "alice", 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(sources.find_node_by_key(1, "alice").unwrap().is_none());
    }

    #[test]
    fn test_find_edge_by_triple_active() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);

        let sources = SourceList {
            active: &mt,
            immutable: &[],
            segments: &[],
        };

        let edge = sources.find_edge_by_triple(10, 20, 1).unwrap();
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().id, 1);

        // Wrong triple
        assert!(sources.find_edge_by_triple(10, 20, 2).unwrap().is_none());
    }

    #[test]
    fn test_find_edge_by_triple_tombstoned_in_newer_source() {
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let mut imm = Memtable::new();
        imm.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(sources.find_edge_by_triple(10, 20, 1).unwrap().is_none());
    }

    #[test]
    fn test_is_node_deleted() {
        let mut active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "live", 1)), 1);
        active.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 2000,
            },
            2,
        );

        let sources = SourceList {
            active: &active,
            immutable: &[],
            segments: &[],
        };

        assert!(!sources.is_node_deleted(1)); // live
        assert!(sources.is_node_deleted(2)); // tombstoned
        assert!(!sources.is_node_deleted(3)); // doesn't exist, not "deleted"
    }

    #[test]
    fn test_is_edge_deleted() {
        let mut active = Memtable::new();
        active.apply_op(&WalOp::UpsertEdge(make_edge(1, 10, 20, 1)), 1);
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 2,
                deleted_at: 2000,
            },
            2,
        );

        let sources = SourceList {
            active: &active,
            immutable: &[],
            segments: &[],
        };

        assert!(!sources.is_edge_deleted(1));
        assert!(sources.is_edge_deleted(2));
        assert!(!sources.is_edge_deleted(3));
    }

    #[test]
    fn test_is_node_deleted_active_live_overrides_immutable_tombstone() {
        // Re-created in active after being deleted in immutable
        let mut active = Memtable::new();
        active.apply_op(&WalOp::UpsertNode(make_node(1, "revived", 1)), 3);

        let mut imm = Memtable::new();
        imm.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        assert!(!sources.is_node_deleted(1));
    }

    #[test]
    fn test_collect_deleted_nodes_across_sources() {
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 1000,
            },
            1,
        );

        let mut imm = Memtable::new();
        imm.apply_op(
            &WalOp::DeleteNode {
                id: 2,
                deleted_at: 1000,
            },
            2,
        );
        imm.apply_op(
            &WalOp::DeleteNode {
                id: 3,
                deleted_at: 1000,
            },
            3,
        );

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let deleted = sources.collect_deleted_nodes();
        assert!(deleted.contains(&1));
        assert!(deleted.contains(&2));
        assert!(deleted.contains(&3));
        assert_eq!(deleted.len(), 3);
    }

    #[test]
    fn test_collect_deleted_edges_across_sources() {
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteEdge {
                id: 10,
                deleted_at: 1000,
            },
            1,
        );

        let mut imm = Memtable::new();
        imm.apply_op(
            &WalOp::DeleteEdge {
                id: 20,
                deleted_at: 1000,
            },
            2,
        );

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let deleted = sources.collect_deleted_edges();
        assert!(deleted.contains(&10));
        assert!(deleted.contains(&20));
        assert_eq!(deleted.len(), 2);
    }

    #[test]
    fn test_collect_deleted_nodes_deduplicates() {
        // Same node ID tombstoned in both active and immutable
        let mut active = Memtable::new();
        active.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 2000,
            },
            2,
        );

        let mut imm = Memtable::new();
        imm.apply_op(
            &WalOp::DeleteNode {
                id: 1,
                deleted_at: 1000,
            },
            1,
        );

        let imm_slice = [wrap_imm(imm)];
        let sources = SourceList {
            active: &active,
            immutable: &imm_slice,
            segments: &[],
        };

        let deleted = sources.collect_deleted_nodes();
        assert!(deleted.contains(&1));
        assert_eq!(deleted.len(), 1);
    }
}
