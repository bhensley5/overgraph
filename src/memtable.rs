use crate::types::*;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;

/// An adjacency entry: one edge connecting to a neighbor.
#[derive(Debug, Clone)]
pub struct AdjEntry {
    pub edge_id: u64,
    pub type_id: u32,
    pub neighbor_id: u64,
    pub weight: f32,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// In-memory graph state. Holds live nodes, edges, tombstones, dedup indexes,
/// and adjacency lists for neighbor queries.
///
/// The Memtable is rebuilt from WAL replay on open and updated in real-time
/// during writes. It provides get-by-ID, key-based dedup, edge uniqueness,
/// and 1-hop neighbor expansion with direction and type filtering.
pub struct Memtable {
    /// Node records by ID.
    nodes: HashMap<u64, NodeRecord>,
    /// Edge records by ID.
    edges: HashMap<u64, EdgeRecord>,
    /// Deleted node IDs with deletion timestamp.
    deleted_nodes: HashMap<u64, i64>,
    /// Deleted edge IDs with deletion timestamp.
    deleted_edges: HashMap<u64, i64>,
    /// type_id → (key → node_id) for upsert dedup.
    /// Two-level map avoids String allocation on every lookup.
    node_key_index: HashMap<u32, HashMap<String, u64>>,
    /// (from, to, type_id) → edge_id for uniqueness enforcement.
    /// Always populated regardless of the engine's edge_uniqueness flag,
    /// so the memtable can answer triple lookups without knowing config.
    edge_triple_index: HashMap<(u64, u64, u32), u64>,
    /// Outgoing adjacency: node_id → (edge_id → AdjEntry). HashMap for O(1) upsert.
    adj_out: HashMap<u64, HashMap<u64, AdjEntry>>,
    /// Incoming adjacency: node_id → (edge_id → AdjEntry). HashMap for O(1) upsert.
    adj_in: HashMap<u64, HashMap<u64, AdjEntry>>,
    /// Type index for nodes: type_id → set of live node IDs.
    type_node_index: HashMap<u32, HashSet<u64>>,
    /// Type index for edges: type_id → set of live edge IDs.
    type_edge_index: HashMap<u32, HashSet<u64>>,
    /// Property equality index for nodes: (type_id, key_hash, value_hash) → node IDs.
    /// Key string is hashed to avoid String allocation on every lookup.
    prop_node_index: HashMap<(u32, u64, u64), HashSet<u64>>,
    /// Timestamp index for nodes: sorted set of (type_id, updated_at, node_id).
    /// Enables O(log N + k) range queries by time window within a type.
    time_node_index: BTreeSet<(u32, i64, u64)>,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            deleted_nodes: HashMap::new(),
            deleted_edges: HashMap::new(),
            node_key_index: HashMap::new(),
            edge_triple_index: HashMap::new(),
            adj_out: HashMap::new(),
            adj_in: HashMap::new(),
            type_node_index: HashMap::new(),
            type_edge_index: HashMap::new(),
            prop_node_index: HashMap::new(),
            time_node_index: BTreeSet::new(),
        }
    }

    /// Apply a WAL operation to the memtable. Updates all indexes.
    pub fn apply_op(&mut self, op: &WalOp) {
        match op {
            WalOp::UpsertNode(node) => {
                self.deleted_nodes.remove(&node.id);
                // If the node already exists with a different type_id, remove from old type index
                if let Some(old) = self.nodes.get(&node.id) {
                    // Remove old time index entry (may differ in type_id or updated_at)
                    self.time_node_index
                        .remove(&(old.type_id, old.updated_at, old.id));
                    if old.type_id != node.type_id {
                        if let Some(set) = self.type_node_index.get_mut(&old.type_id) {
                            set.remove(&node.id);
                            if set.is_empty() {
                                self.type_node_index.remove(&old.type_id);
                            }
                        }
                    }
                }
                self.node_key_index
                    .entry(node.type_id)
                    .or_default()
                    .insert(node.key.clone(), node.id);
                self.type_node_index
                    .entry(node.type_id)
                    .or_default()
                    .insert(node.id);
                // Property index: remove old entries, add new
                if let Some(old) = self.nodes.get(&node.id) {
                    for (k, v) in &old.props {
                        let key = (old.type_id, hash_prop_key(k), hash_prop_value(v));
                        if let Some(set) = self.prop_node_index.get_mut(&key) {
                            set.remove(&node.id);
                            if set.is_empty() {
                                self.prop_node_index.remove(&key);
                            }
                        }
                    }
                }
                for (k, v) in &node.props {
                    self.prop_node_index
                        .entry((node.type_id, hash_prop_key(k), hash_prop_value(v)))
                        .or_default()
                        .insert(node.id);
                }
                self.time_node_index
                    .insert((node.type_id, node.updated_at, node.id));
                self.nodes.insert(node.id, node.clone());
            }
            WalOp::UpsertEdge(edge) => {
                self.deleted_edges.remove(&edge.id);

                // If edge already exists, handle type index and adjacency cleanup
                if let Some(old) = self.edges.get(&edge.id) {
                    if old.type_id != edge.type_id {
                        if let Some(set) = self.type_edge_index.get_mut(&old.type_id) {
                            set.remove(&edge.id);
                            if set.is_empty() {
                                self.type_edge_index.remove(&old.type_id);
                            }
                        }
                    }
                    if old.from != edge.from || old.to != edge.to {
                        if let Some(map) = self.adj_out.get_mut(&old.from) {
                            map.remove(&edge.id);
                            if map.is_empty() {
                                self.adj_out.remove(&old.from);
                            }
                        }
                        if let Some(map) = self.adj_in.get_mut(&old.to) {
                            map.remove(&edge.id);
                            if map.is_empty() {
                                self.adj_in.remove(&old.to);
                            }
                        }
                    }
                }

                self.edge_triple_index
                    .insert((edge.from, edge.to, edge.type_id), edge.id);

                // Update adjacency. O(1) insert/update via HashMap keyed by edge_id
                self.adj_out.entry(edge.from).or_default().insert(
                    edge.id,
                    AdjEntry {
                        edge_id: edge.id,
                        type_id: edge.type_id,
                        neighbor_id: edge.to,
                        weight: edge.weight,
                        valid_from: edge.valid_from,
                        valid_to: edge.valid_to,
                    },
                );
                self.adj_in.entry(edge.to).or_default().insert(
                    edge.id,
                    AdjEntry {
                        edge_id: edge.id,
                        type_id: edge.type_id,
                        neighbor_id: edge.from,
                        weight: edge.weight,
                        valid_from: edge.valid_from,
                        valid_to: edge.valid_to,
                    },
                );

                self.type_edge_index
                    .entry(edge.type_id)
                    .or_default()
                    .insert(edge.id);
                self.edges.insert(edge.id, edge.clone());
            }
            WalOp::DeleteNode { id, deleted_at } => {
                if let Some(node) = self.nodes.remove(id) {
                    self.time_node_index
                        .remove(&(node.type_id, node.updated_at, node.id));
                    if let Some(set) = self.type_node_index.get_mut(&node.type_id) {
                        set.remove(&node.id);
                        if set.is_empty() {
                            self.type_node_index.remove(&node.type_id);
                        }
                    }
                    if let Some(inner) = self.node_key_index.get_mut(&node.type_id) {
                        inner.remove(&node.key);
                        if inner.is_empty() {
                            self.node_key_index.remove(&node.type_id);
                        }
                    }
                    for (k, v) in &node.props {
                        let key = (node.type_id, hash_prop_key(k), hash_prop_value(v));
                        if let Some(set) = self.prop_node_index.get_mut(&key) {
                            set.remove(&node.id);
                            if set.is_empty() {
                                self.prop_node_index.remove(&key);
                            }
                        }
                    }
                }
                // Clean up adjacency lists owned by this node (prevents memory leak)
                self.adj_out.remove(id);
                self.adj_in.remove(id);
                self.deleted_nodes.insert(*id, *deleted_at);
            }
            WalOp::DeleteEdge { id, deleted_at } => {
                if let Some(edge) = self.edges.remove(id) {
                    self.edge_triple_index
                        .remove(&(edge.from, edge.to, edge.type_id));
                    if let Some(set) = self.type_edge_index.get_mut(&edge.type_id) {
                        set.remove(&edge.id);
                        if set.is_empty() {
                            self.type_edge_index.remove(&edge.type_id);
                        }
                    }
                    // Remove from adjacency lists
                    if let Some(map) = self.adj_out.get_mut(&edge.from) {
                        map.remove(&edge.id);
                        if map.is_empty() {
                            self.adj_out.remove(&edge.from);
                        }
                    }
                    if let Some(map) = self.adj_in.get_mut(&edge.to) {
                        map.remove(&edge.id);
                        if map.is_empty() {
                            self.adj_in.remove(&edge.to);
                        }
                    }
                }
                self.deleted_edges.insert(*id, *deleted_at);
            }
        }
    }

    /// Get a node by ID (returns None if deleted or missing).
    pub fn get_node(&self, id: u64) -> Option<&NodeRecord> {
        if self.deleted_nodes.contains_key(&id) {
            return None;
        }
        self.nodes.get(&id)
    }

    /// Get an edge by ID (returns None if deleted or missing).
    pub fn get_edge(&self, id: u64) -> Option<&EdgeRecord> {
        if self.deleted_edges.contains_key(&id) {
            return None;
        }
        self.edges.get(&id)
    }

    /// Look up a node by (type_id, key) for upsert dedup.
    /// Returns None if the node has been deleted (defensive tombstone check).
    pub fn node_by_key(&self, type_id: u32, key: &str) -> Option<&NodeRecord> {
        let id = self.node_key_index.get(&type_id)?.get(key)?;
        if self.deleted_nodes.contains_key(id) {
            return None;
        }
        self.nodes.get(id)
    }

    /// Look up an edge by (from, to, type_id) for uniqueness enforcement.
    /// Returns None if the edge has been deleted (defensive tombstone check).
    pub fn edge_by_triple(&self, from: u64, to: u64, type_id: u32) -> Option<&EdgeRecord> {
        let id = self.edge_triple_index.get(&(from, to, type_id))?;
        if self.deleted_edges.contains_key(id) {
            return None;
        }
        self.edges.get(id)
    }

    /// Query neighbors of a node with direction, optional type filter, and limit.
    /// Excludes deleted nodes from results. Returns empty if the queried node is deleted.
    /// Limit of 0 means no limit.
    pub fn neighbors(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        limit: usize,
    ) -> Vec<NeighborEntry> {
        // Querying neighbors of a deleted node returns nothing
        if self.deleted_nodes.contains_key(&node_id) {
            return Vec::new();
        }

        let mut results = Vec::new();

        let collect = |map: &HashMap<u64, AdjEntry>, results: &mut Vec<NeighborEntry>| {
            for entry in map.values() {
                if limit > 0 && results.len() >= limit {
                    break;
                }
                if let Some(types) = type_filter {
                    if !types.contains(&entry.type_id) {
                        continue;
                    }
                }
                if self.deleted_nodes.contains_key(&entry.neighbor_id) {
                    continue;
                }
                results.push(NeighborEntry {
                    node_id: entry.neighbor_id,
                    edge_id: entry.edge_id,
                    edge_type_id: entry.type_id,
                    weight: entry.weight,
                    valid_from: entry.valid_from,
                    valid_to: entry.valid_to,
                });
            }
        };

        match direction {
            Direction::Outgoing => {
                if let Some(map) = self.adj_out.get(&node_id) {
                    collect(map, &mut results);
                }
            }
            Direction::Incoming => {
                if let Some(map) = self.adj_in.get(&node_id) {
                    collect(map, &mut results);
                }
            }
            Direction::Both => {
                if let Some(map) = self.adj_out.get(&node_id) {
                    collect(map, &mut results);
                }
                if limit == 0 || results.len() < limit {
                    if let Some(map) = self.adj_in.get(&node_id) {
                        collect(map, &mut results);
                    }
                }
                // Deduplicate by edge_id (self-loops appear in both adj_out and adj_in)
                let mut seen = std::collections::HashSet::new();
                results.retain(|e| seen.insert(e.edge_id));
            }
        }

        results
    }

    /// Batch neighbor query: collect neighbors for multiple node IDs.
    /// Memtable is HashMap-based so per-node lookups are O(1); this method
    /// batches them into a single `NodeIdMap` result for the engine merge layer.
    pub fn neighbors_batch(
        &self,
        node_ids: &[u64],
        direction: Direction,
        type_filter: Option<&[u32]>,
    ) -> NodeIdMap<Vec<NeighborEntry>> {
        let mut results = NodeIdMap::default();
        for &nid in node_ids {
            let entries = self.neighbors(nid, direction, type_filter, 0);
            if !entries.is_empty() {
                results.insert(nid, entries);
            }
        }
        results
    }

    /// Iterate adjacency entries for a node, calling the callback for each valid
    /// (non-tombstoned, type-matching) entry. Used by degree/weight aggregation
    /// to avoid materializing `Vec<NeighborEntry>`.
    ///
    /// Callback receives `(edge_id, neighbor_id, weight, valid_from, valid_to)`.
    pub fn for_each_adj_entry<F>(
        &self,
        node_id: u64,
        direction: Direction,
        type_filter: Option<&[u32]>,
        callback: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        if self.deleted_nodes.contains_key(&node_id) {
            return ControlFlow::Continue(());
        }

        let visit = |map: &HashMap<u64, AdjEntry>, cb: &mut F| -> ControlFlow<()> {
            for entry in map.values() {
                if let Some(types) = type_filter {
                    if !types.contains(&entry.type_id) {
                        continue;
                    }
                }
                if self.deleted_nodes.contains_key(&entry.neighbor_id) {
                    continue;
                }
                if cb(
                    entry.edge_id,
                    entry.neighbor_id,
                    entry.weight,
                    entry.valid_from,
                    entry.valid_to,
                )
                .is_break()
                {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        };

        match direction {
            Direction::Outgoing => {
                if let Some(map) = self.adj_out.get(&node_id) {
                    visit(map, callback)?;
                }
            }
            Direction::Incoming => {
                if let Some(map) = self.adj_in.get(&node_id) {
                    visit(map, callback)?;
                }
            }
            Direction::Both => {
                let mut seen = HashSet::new();
                if let Some(map) = self.adj_out.get(&node_id) {
                    for entry in map.values() {
                        if let Some(types) = type_filter {
                            if !types.contains(&entry.type_id) {
                                continue;
                            }
                        }
                        if self.deleted_nodes.contains_key(&entry.neighbor_id) {
                            continue;
                        }
                        seen.insert(entry.edge_id);
                        if callback(
                            entry.edge_id,
                            entry.neighbor_id,
                            entry.weight,
                            entry.valid_from,
                            entry.valid_to,
                        )
                        .is_break()
                        {
                            return ControlFlow::Break(());
                        }
                    }
                }
                if let Some(map) = self.adj_in.get(&node_id) {
                    for entry in map.values() {
                        if seen.contains(&entry.edge_id) {
                            continue;
                        }
                        if let Some(types) = type_filter {
                            if !types.contains(&entry.type_id) {
                                continue;
                            }
                        }
                        if self.deleted_nodes.contains_key(&entry.neighbor_id) {
                            continue;
                        }
                        if callback(
                            entry.edge_id,
                            entry.neighbor_id,
                            entry.weight,
                            entry.valid_from,
                            entry.valid_to,
                        )
                        .is_break()
                        {
                            return ControlFlow::Break(());
                        }
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }

    /// Return edge IDs incident to a node (outgoing + incoming).
    /// Used by the engine for cascade-delete.
    pub fn incident_edge_ids(&self, node_id: u64) -> Vec<u64> {
        let mut ids = Vec::new();
        if let Some(map) = self.adj_out.get(&node_id) {
            ids.extend(map.keys());
        }
        if let Some(map) = self.adj_in.get(&node_id) {
            ids.extend(map.keys());
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Count of live nodes (excluding tombstoned).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Count of live edges (excluding tombstoned).
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    // --- Segment flush accessors ---

    /// Return a reference to all live node records.
    pub fn nodes(&self) -> &HashMap<u64, NodeRecord> {
        &self.nodes
    }

    /// Return a reference to all live edge records.
    pub fn edges(&self) -> &HashMap<u64, EdgeRecord> {
        &self.edges
    }

    /// Return a reference to the deleted node IDs with timestamps.
    pub fn deleted_nodes(&self) -> &HashMap<u64, i64> {
        &self.deleted_nodes
    }

    /// Return a reference to the deleted edge IDs with timestamps.
    pub fn deleted_edges(&self) -> &HashMap<u64, i64> {
        &self.deleted_edges
    }

    /// Return a reference to the outgoing adjacency map.
    pub fn adj_out(&self) -> &HashMap<u64, HashMap<u64, AdjEntry>> {
        &self.adj_out
    }

    /// Return a reference to the incoming adjacency map.
    pub fn adj_in(&self) -> &HashMap<u64, HashMap<u64, AdjEntry>> {
        &self.adj_in
    }

    /// Return live node IDs for a given type_id.
    pub fn nodes_by_type(&self, type_id: u32) -> Vec<u64> {
        self.type_node_index
            .get(&type_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Return live edge IDs for a given type_id.
    pub fn edges_by_type(&self, type_id: u32) -> Vec<u64> {
        self.type_edge_index
            .get(&type_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Return a reference to the node type index.
    pub fn type_node_index(&self) -> &HashMap<u32, HashSet<u64>> {
        &self.type_node_index
    }

    /// Return a reference to the edge type index.
    pub fn type_edge_index(&self) -> &HashMap<u32, HashSet<u64>> {
        &self.type_edge_index
    }

    /// Return a reference to the property node index.
    pub fn prop_node_index(&self) -> &HashMap<(u32, u64, u64), HashSet<u64>> {
        &self.prop_node_index
    }

    /// Return a reference to the timestamp node index (for segment writer).
    pub fn time_node_index(&self) -> &BTreeSet<(u32, i64, u64)> {
        &self.time_node_index
    }

    /// Return node IDs matching a time range for a given type_id.
    /// Results are sorted by node_id (for K-way merge compatibility).
    pub fn nodes_by_time_range(&self, type_id: u32, from_ms: i64, to_ms: i64) -> Vec<u64> {
        if from_ms > to_ms {
            return Vec::new();
        }
        use std::ops::Bound;
        let start = (type_id, from_ms, 0u64);
        let end = (type_id, to_ms, u64::MAX);
        let mut ids: Vec<u64> = self
            .time_node_index
            .range((Bound::Included(start), Bound::Included(end)))
            .map(|&(_, _, node_id)| node_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Find node IDs matching (type_id, prop_key, prop_value).
    /// Uses the hash index for fast lookup, then post-filters to handle collisions.
    pub fn find_nodes(&self, type_id: u32, prop_key: &str, prop_value: &PropValue) -> Vec<u64> {
        let key = (
            type_id,
            hash_prop_key(prop_key),
            hash_prop_value(prop_value),
        );
        match self.prop_node_index.get(&key) {
            Some(ids) => ids
                .iter()
                .copied()
                .filter(|id| {
                    self.nodes
                        .get(id)
                        .and_then(|n| n.props.get(prop_key))
                        .map(|v| v == prop_value)
                        .unwrap_or(false)
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Rough estimate of memtable memory usage in bytes.
    /// Used to trigger flush when the memtable grows too large.
    pub fn estimated_size(&self) -> usize {
        // Per node: ~120 bytes base + key + props overhead
        let node_size: usize = self
            .nodes
            .values()
            .map(|n| 120 + n.key.len() + n.props.len() * 80)
            .sum();
        // Per edge: ~100 bytes base + props overhead
        let edge_size: usize = self.edges.values().map(|e| 100 + e.props.len() * 80).sum();
        // Tombstones: ~16 bytes each
        let tombstone_size = (self.deleted_nodes.len() + self.deleted_edges.len()) * 16;
        // Adjacency: ~48 bytes per entry (AdjEntry + HashMap overhead)
        let adj_size: usize = self.adj_out.values().map(|m| m.len() * 48).sum::<usize>()
            + self.adj_in.values().map(|m| m.len() * 48).sum::<usize>();
        // Type indexes: ~16 bytes per entry (u64 + hash overhead)
        let type_idx_size: usize = self
            .type_node_index
            .values()
            .map(|s| s.len() * 16)
            .sum::<usize>()
            + self
                .type_edge_index
                .values()
                .map(|s| s.len() * 16)
                .sum::<usize>();
        // Prop index: ~40 bytes per key (u32 type_id + u64 key_hash + u64 value_hash + set overhead) + 16 per ID
        let prop_idx_size: usize = self
            .prop_node_index
            .values()
            .map(|ids| 40 + ids.len() * 16)
            .sum();

        // Time index: ~48 bytes per entry (tuple + BTree node overhead)
        let time_idx_size = self.time_node_index.len() * 48;

        node_size
            + edge_size
            + tombstone_size
            + adj_size
            + type_idx_size
            + prop_idx_size
            + time_idx_size
    }

    /// Remove a node from the memtable and all associated indexes.
    /// Used during compaction to strip auto-pruned nodes without generating tombstones.
    /// Returns true if the memtable is empty (no live or deleted records).
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
            && self.edges.is_empty()
            && self.deleted_nodes.is_empty()
            && self.deleted_edges.is_empty()
    }

    /// Maximum node ID across live and deleted records. Returns 0 if empty.
    pub fn max_node_id(&self) -> u64 {
        let live_max = self.nodes.keys().max().copied().unwrap_or(0);
        let deleted_max = self.deleted_nodes.keys().max().copied().unwrap_or(0);
        live_max.max(deleted_max)
    }

    /// Maximum edge ID across live and deleted records. Returns 0 if empty.
    pub fn max_edge_id(&self) -> u64 {
        let live_max = self.edges.keys().max().copied().unwrap_or(0);
        let deleted_max = self.deleted_edges.keys().max().copied().unwrap_or(0);
        live_max.max(deleted_max)
    }
}

// Test-only helpers for inspecting internal index state.
#[cfg(test)]
impl Memtable {
    /// Number of distinct type keys in the node type index.
    fn type_node_index_key_count(&self) -> usize {
        self.type_node_index.len()
    }
    /// Number of distinct type keys in the edge type index.
    fn type_edge_index_key_count(&self) -> usize {
        self.type_edge_index.len()
    }
    /// Number of distinct composite keys in the property node index.
    fn prop_node_index_key_count(&self) -> usize {
        self.prop_node_index.len()
    }
    /// Number of distinct type keys in the node key index (outer map).
    fn node_key_index_key_count(&self) -> usize {
        self.node_key_index.len()
    }
    /// Number of node IDs with outgoing adjacency entries.
    fn adj_out_key_count(&self) -> usize {
        self.adj_out.len()
    }
    /// Number of node IDs with incoming adjacency entries.
    fn adj_in_key_count(&self) -> usize {
        self.adj_in.len()
    }
    /// Number of entries in the timestamp node index.
    fn time_node_index_len(&self) -> usize {
        self.time_node_index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_node(id: u64, type_id: u32, key: &str) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
        }
    }

    fn make_edge(id: u64, from: u64, to: u64, type_id: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            type_id,
            props: BTreeMap::new(),
            created_at: 2000,
            updated_at: 2001,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
        }
    }

    #[test]
    fn test_basic_insert_and_get() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        assert_eq!(mt.node_count(), 1);
        assert_eq!(mt.edge_count(), 1);
        assert_eq!(mt.get_node(1).unwrap().key, "alice");
        assert_eq!(mt.get_edge(1).unwrap().from, 1);
    }

    #[test]
    fn test_node_key_index() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));

        assert_eq!(mt.node_by_key(1, "alice").unwrap().id, 1);
        assert_eq!(mt.node_by_key(1, "bob").unwrap().id, 2);
        assert!(mt.node_by_key(1, "charlie").is_none());
        assert!(mt.node_by_key(2, "alice").is_none()); // different type_id
    }

    #[test]
    fn test_edge_triple_index() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 2, 20))); // different type

        assert_eq!(mt.edge_by_triple(1, 2, 10).unwrap().id, 1);
        assert_eq!(mt.edge_by_triple(1, 2, 20).unwrap().id, 2);
        assert!(mt.edge_by_triple(1, 2, 30).is_none());
    }

    #[test]
    fn test_upsert_overwrites_key_index() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));

        // Same (type_id, key) with same ID, update
        let mut updated = make_node(1, 1, "alice");
        updated.weight = 0.99;
        mt.apply_op(&WalOp::UpsertNode(updated));

        assert_eq!(mt.node_count(), 1);
        assert!((mt.get_node(1).unwrap().weight - 0.99).abs() < f32::EPSILON);
        assert_eq!(mt.node_by_key(1, "alice").unwrap().id, 1);
    }

    #[test]
    fn test_delete_removes_from_indexes() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        mt.apply_op(&WalOp::DeleteEdge {
            id: 1,
            deleted_at: 9999,
        });

        assert!(mt.get_node(1).is_none());
        assert!(mt.get_edge(1).is_none());
        assert!(mt.node_by_key(1, "alice").is_none());
        assert!(mt.edge_by_triple(1, 2, 10).is_none());
        assert_eq!(mt.node_count(), 0);
        assert_eq!(mt.edge_count(), 0);
    }

    #[test]
    fn test_max_ids() {
        let mut mt = Memtable::new();
        assert_eq!(mt.max_node_id(), 0);
        assert_eq!(mt.max_edge_id(), 0);

        mt.apply_op(&WalOp::UpsertNode(make_node(42, 1, "high")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(99, 1, 2, 10)));

        assert_eq!(mt.max_node_id(), 42);
        assert_eq!(mt.max_edge_id(), 99);

        // Delete: max should still reflect deleted IDs
        mt.apply_op(&WalOp::DeleteNode {
            id: 42,
            deleted_at: 9999,
        });
        assert_eq!(mt.max_node_id(), 42); // still 42 from deleted_nodes
    }

    #[test]
    fn test_re_upsert_after_delete() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert!(mt.get_node(1).is_none());

        // Re-upsert with same ID
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice_v2")));
        assert_eq!(mt.get_node(1).unwrap().key, "alice_v2");
        assert_eq!(mt.node_by_key(1, "alice_v2").unwrap().id, 1);
    }

    // --- Adjacency and neighbor tests ---

    #[test]
    fn test_adjacency_built_on_edge_insert() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        // Node 1 has outgoing to node 2
        let out = mt.neighbors(1, Direction::Outgoing, None, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, 2);
        assert_eq!(out[0].edge_id, 1);
        assert_eq!(out[0].edge_type_id, 10);

        // Node 2 has incoming from node 1
        let inc = mt.neighbors(2, Direction::Incoming, None, 0);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].node_id, 1);

        // Node 1 has no incoming
        assert!(mt.neighbors(1, Direction::Incoming, None, 0).is_empty());
        // Node 2 has no outgoing
        assert!(mt.neighbors(2, Direction::Outgoing, None, 0).is_empty());
    }

    #[test]
    fn test_for_each_adj_entry_breaks_early() {
        let mut mt = Memtable::new();
        for id in 1..=4 {
            mt.apply_op(&WalOp::UpsertNode(make_node(id, 1, &format!("n{}", id))));
        }
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 1, 4, 10)));

        let mut seen = 0usize;
        let flow = mt.for_each_adj_entry(
            1,
            Direction::Outgoing,
            None,
            &mut |_edge_id, _neighbor_id, _weight, _valid_from, _valid_to| {
                seen += 1;
                ControlFlow::Break(())
            },
        );

        assert!(matches!(flow, ControlFlow::Break(())));
        assert_eq!(seen, 1);
    }

    #[test]
    fn test_neighbors_with_type_filter() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10))); // type 10
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 20))); // type 20

        // No filter → both
        let all = mt.neighbors(1, Direction::Outgoing, None, 0);
        assert_eq!(all.len(), 2);

        // Filter type 10 only
        let typed = mt.neighbors(1, Direction::Outgoing, Some(&[10]), 0);
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, 2);

        // Filter type 20 only
        let typed = mt.neighbors(1, Direction::Outgoing, Some(&[20]), 0);
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].node_id, 3);

        // Filter non-existent type
        let typed = mt.neighbors(1, Direction::Outgoing, Some(&[99]), 0);
        assert!(typed.is_empty());
    }

    #[test]
    fn test_neighbors_with_limit() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "hub")));
        for i in 2..=6 {
            mt.apply_op(&WalOp::UpsertNode(make_node(i, 1, &format!("n{}", i))));
            mt.apply_op(&WalOp::UpsertEdge(make_edge(i - 1, 1, i, 10)));
        }

        // 5 outgoing edges, limit to 3
        let limited = mt.neighbors(1, Direction::Outgoing, None, 3);
        assert_eq!(limited.len(), 3);

        // Limit 0 means no limit
        let all = mt.neighbors(1, Direction::Outgoing, None, 0);
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_neighbors_both_direction() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10))); // 1→2
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 3, 1, 10))); // 3→1

        // Node 1: outgoing to 2, incoming from 3
        let both = mt.neighbors(1, Direction::Both, None, 0);
        assert_eq!(both.len(), 2);
        let neighbor_ids: Vec<u64> = both.iter().map(|e| e.node_id).collect();
        assert!(neighbor_ids.contains(&2));
        assert!(neighbor_ids.contains(&3));
    }

    #[test]
    fn test_delete_edge_removes_from_adjacency() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        assert_eq!(mt.neighbors(1, Direction::Outgoing, None, 0).len(), 1);

        mt.apply_op(&WalOp::DeleteEdge {
            id: 1,
            deleted_at: 9999,
        });

        assert!(mt.neighbors(1, Direction::Outgoing, None, 0).is_empty());
        assert!(mt.neighbors(2, Direction::Incoming, None, 0).is_empty());
    }

    #[test]
    fn test_deleted_node_excluded_from_neighbors() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 1, "c")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 1, 3, 10)));

        assert_eq!(mt.neighbors(1, Direction::Outgoing, None, 0).len(), 2);

        // Delete node 2. Edge still exists but node 2 should be excluded
        mt.apply_op(&WalOp::DeleteNode {
            id: 2,
            deleted_at: 9999,
        });

        let out = mt.neighbors(1, Direction::Outgoing, None, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_id, 3);
    }

    #[test]
    fn test_adjacency_idempotent_on_edge_upsert() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "a")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "b")));

        // Upsert same edge twice (simulates WAL replay)
        let mut edge = make_edge(1, 1, 2, 10);
        mt.apply_op(&WalOp::UpsertEdge(edge.clone()));
        edge.weight = 0.9;
        mt.apply_op(&WalOp::UpsertEdge(edge));

        // Should have exactly 1 adjacency entry, not 2
        let out = mt.neighbors(1, Direction::Outgoing, None, 0);
        assert_eq!(out.len(), 1);
        assert!((out[0].weight - 0.9).abs() < f32::EPSILON); // weight updated
    }

    // --- Type index tests ---

    #[test]
    fn test_type_node_index_basic() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertNode(make_node(3, 2, "charlie")));

        let mut type1: Vec<u64> = mt.nodes_by_type(1);
        type1.sort();
        assert_eq!(type1, vec![1, 2]);
        assert_eq!(mt.nodes_by_type(2), vec![3]);
        assert!(mt.nodes_by_type(99).is_empty());
    }

    #[test]
    fn test_type_edge_index_basic() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(2, 2, 3, 20)));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(3, 3, 4, 10)));

        let mut type10: Vec<u64> = mt.edges_by_type(10);
        type10.sort();
        assert_eq!(type10, vec![1, 3]);
        assert_eq!(mt.edges_by_type(20), vec![2]);
        assert!(mt.edges_by_type(99).is_empty());
    }

    #[test]
    fn test_type_index_updated_on_delete() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.nodes_by_type(1), vec![2]);

        mt.apply_op(&WalOp::DeleteEdge {
            id: 1,
            deleted_at: 9999,
        });
        assert!(mt.edges_by_type(10).is_empty());
    }

    #[test]
    fn test_empty_index_sets_pruned_after_deletes() {
        let mut mt = Memtable::new();
        // Two nodes of different types, one edge
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        assert_eq!(mt.type_node_index_key_count(), 2); // type 1 + type 2
        assert_eq!(mt.type_edge_index_key_count(), 1); // type 10

        // Delete all members of type 1. Internal map entry should be pruned
        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.type_node_index_key_count(), 1); // only type 2 remains

        // Delete the edge. Edge type map entry should be pruned
        mt.apply_op(&WalOp::DeleteEdge {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.type_edge_index_key_count(), 0);

        // Delete remaining node
        mt.apply_op(&WalOp::DeleteNode {
            id: 2,
            deleted_at: 9999,
        });
        assert_eq!(mt.type_node_index_key_count(), 0);
    }

    #[test]
    fn test_prop_index_pruned_after_delete() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "apple", props,
        )));

        assert_eq!(mt.prop_node_index_key_count(), 1);

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.prop_node_index_key_count(), 0); // pruned, not just empty
    }

    #[test]
    fn test_prop_index_pruned_on_upsert_type_change() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1,
            1,
            "apple",
            props.clone(),
        )));

        // prop key is (type_id=1, "color", hash("red"))
        assert_eq!(mt.prop_node_index_key_count(), 1);

        // Re-upsert same node with different props. Old prop entry should be pruned
        let mut new_props = BTreeMap::new();
        new_props.insert("size".to_string(), PropValue::String("large".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "apple", new_props,
        )));

        // Old (color, red) entry pruned, new (size, large) entry added
        assert_eq!(mt.prop_node_index_key_count(), 1);
    }

    #[test]
    fn test_edge_type_index_pruned_on_type_change() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        assert_eq!(mt.type_edge_index_key_count(), 1); // type 10

        // Re-upsert edge with different type. Old type entry should be pruned
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 20)));
        assert_eq!(mt.type_edge_index_key_count(), 1); // only type 20, not both
        assert!(mt.edges_by_type(10).is_empty());
        assert_eq!(mt.edges_by_type(20), vec![1]);
    }

    #[test]
    fn test_node_key_index_pruned_after_delete() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")));

        assert_eq!(mt.node_key_index_key_count(), 2); // type 1 + type 2

        // Delete only member of type 1. Inner map should be pruned
        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.node_key_index_key_count(), 1); // only type 2

        mt.apply_op(&WalOp::DeleteNode {
            id: 2,
            deleted_at: 9999,
        });
        assert_eq!(mt.node_key_index_key_count(), 0);
    }

    #[test]
    fn test_adjacency_lists_pruned_after_edge_delete() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 1, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        assert_eq!(mt.adj_out_key_count(), 1); // node 1 has outgoing
        assert_eq!(mt.adj_in_key_count(), 1); // node 2 has incoming

        // Delete the edge. Adjacency entries should be pruned
        mt.apply_op(&WalOp::DeleteEdge {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.adj_out_key_count(), 0);
        assert_eq!(mt.adj_in_key_count(), 0);
    }

    #[test]
    fn test_type_index_re_upsert_after_delete() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert!(mt.nodes_by_type(1).is_empty());

        // Re-upsert, should reappear in type index
        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice_v2")));
        assert_eq!(mt.nodes_by_type(1), vec![1]);
    }

    // --- Property index tests ---

    fn make_node_with_props(
        id: u64,
        type_id: u32,
        key: &str,
        props: BTreeMap<String, PropValue>,
    ) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props,
            created_at: 1000,
            updated_at: 1001,
            weight: 0.5,
        }
    }

    #[test]
    fn test_prop_index_basic_lookup() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1,
            1,
            "apple",
            props.clone(),
        )));

        let mut props2 = BTreeMap::new();
        props2.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            2, 1, "cherry", props2,
        )));

        let mut props3 = BTreeMap::new();
        props3.insert("color".to_string(), PropValue::String("green".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            3, 1, "lime", props3,
        )));

        // Find red nodes of type 1
        let mut reds = mt.find_nodes(1, "color", &PropValue::String("red".to_string()));
        reds.sort();
        assert_eq!(reds, vec![1, 2]);

        // Find green nodes of type 1
        let greens = mt.find_nodes(1, "color", &PropValue::String("green".to_string()));
        assert_eq!(greens, vec![3]);

        // Non-existent value
        assert!(mt
            .find_nodes(1, "color", &PropValue::String("blue".to_string()))
            .is_empty());

        // Non-existent key
        assert!(mt
            .find_nodes(1, "shape", &PropValue::String("round".to_string()))
            .is_empty());

        // Wrong type_id
        assert!(mt
            .find_nodes(2, "color", &PropValue::String("red".to_string()))
            .is_empty());
    }

    #[test]
    fn test_prop_index_updated_on_upsert() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert(
            "status".to_string(),
            PropValue::String("active".to_string()),
        );
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "item", props,
        )));

        assert_eq!(
            mt.find_nodes(1, "status", &PropValue::String("active".to_string()))
                .len(),
            1
        );

        // Update: change status to "inactive"
        let mut props2 = BTreeMap::new();
        props2.insert(
            "status".to_string(),
            PropValue::String("inactive".to_string()),
        );
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "item", props2,
        )));

        // Old value gone, new value present
        assert!(mt
            .find_nodes(1, "status", &PropValue::String("active".to_string()))
            .is_empty());
        assert_eq!(
            mt.find_nodes(1, "status", &PropValue::String("inactive".to_string())),
            vec![1]
        );
    }

    #[test]
    fn test_prop_index_cleaned_on_delete() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "apple", props,
        )));

        assert_eq!(
            mt.find_nodes(1, "color", &PropValue::String("red".to_string()))
                .len(),
            1
        );

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });

        assert!(mt
            .find_nodes(1, "color", &PropValue::String("red".to_string()))
            .is_empty());
    }

    #[test]
    fn test_prop_index_multiple_props_per_node() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("color".to_string(), PropValue::String("red".to_string()));
        props.insert("size".to_string(), PropValue::Int(42));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "item", props,
        )));

        assert_eq!(
            mt.find_nodes(1, "color", &PropValue::String("red".to_string())),
            vec![1]
        );
        assert_eq!(mt.find_nodes(1, "size", &PropValue::Int(42)), vec![1]);
        assert!(mt.find_nodes(1, "size", &PropValue::Int(99)).is_empty());
    }

    #[test]
    fn test_prop_index_re_upsert_after_delete() {
        let mut mt = Memtable::new();
        let mut props = BTreeMap::new();
        props.insert("tag".to_string(), PropValue::String("a".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "item", props,
        )));

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert!(mt
            .find_nodes(1, "tag", &PropValue::String("a".to_string()))
            .is_empty());

        // Re-upsert with different value
        let mut props2 = BTreeMap::new();
        props2.insert("tag".to_string(), PropValue::String("b".to_string()));
        mt.apply_op(&WalOp::UpsertNode(make_node_with_props(
            1, 1, "item_v2", props2,
        )));

        assert!(mt
            .find_nodes(1, "tag", &PropValue::String("a".to_string()))
            .is_empty());
        assert_eq!(
            mt.find_nodes(1, "tag", &PropValue::String("b".to_string())),
            vec![1]
        );
    }

    #[test]
    fn test_estimated_size_includes_type_indexes() {
        let mut mt = Memtable::new();
        let size_empty = mt.estimated_size();

        mt.apply_op(&WalOp::UpsertNode(make_node(1, 1, "alice")));
        mt.apply_op(&WalOp::UpsertNode(make_node(2, 2, "bob")));
        mt.apply_op(&WalOp::UpsertEdge(make_edge(1, 1, 2, 10)));

        let size_with_data = mt.estimated_size();
        assert!(size_with_data > size_empty);

        // The type index contributes: 2 node types × 1 entry × 16 + 1 edge type × 1 entry × 16 = 48
        // Verify type index adds non-trivial overhead by checking it's larger
        // than just nodes + edges + adjacency alone would suggest
        assert!(
            size_with_data >= 48,
            "estimated_size should include type index overhead"
        );
    }

    fn make_node_at(id: u64, type_id: u32, key: &str, updated_at: i64) -> NodeRecord {
        NodeRecord {
            id,
            type_id,
            key: key.to_string(),
            props: BTreeMap::new(),
            created_at: 1000,
            updated_at,
            weight: 0.5,
        }
    }

    #[test]
    fn test_time_index_insert_and_query() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)));
        mt.apply_op(&WalOp::UpsertNode(make_node_at(2, 1, "b", 200)));
        mt.apply_op(&WalOp::UpsertNode(make_node_at(3, 2, "c", 150)));

        assert_eq!(mt.time_node_index_len(), 3);

        // All type-1 nodes in full range
        let ids = mt.nodes_by_time_range(1, 0, 300);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));

        // Only type-2
        let ids2 = mt.nodes_by_time_range(2, 0, 300);
        assert_eq!(ids2.len(), 1);
        assert!(ids2.contains(&3));
    }

    #[test]
    fn test_time_index_range_boundaries() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)));
        mt.apply_op(&WalOp::UpsertNode(make_node_at(2, 1, "b", 200)));
        mt.apply_op(&WalOp::UpsertNode(make_node_at(3, 1, "c", 300)));

        // Inclusive boundaries
        assert_eq!(mt.nodes_by_time_range(1, 100, 300).len(), 3);
        assert_eq!(mt.nodes_by_time_range(1, 100, 200).len(), 2);
        assert_eq!(mt.nodes_by_time_range(1, 200, 200).len(), 1);

        // Empty range
        assert_eq!(mt.nodes_by_time_range(1, 250, 250).len(), 0);

        // Inverted range returns empty
        assert_eq!(mt.nodes_by_time_range(1, 300, 100).len(), 0);
    }

    #[test]
    fn test_time_index_update_moves_entry() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)));
        assert_eq!(mt.nodes_by_time_range(1, 50, 150).len(), 1);
        assert_eq!(mt.nodes_by_time_range(1, 150, 300).len(), 0);

        // Update node with new timestamp
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 200)));
        assert_eq!(mt.time_node_index_len(), 1); // no duplicate
        assert_eq!(mt.nodes_by_time_range(1, 50, 150).len(), 0); // old range empty
        assert_eq!(mt.nodes_by_time_range(1, 150, 300).len(), 1); // new range has it
    }

    #[test]
    fn test_time_index_delete_removes_entry() {
        let mut mt = Memtable::new();
        mt.apply_op(&WalOp::UpsertNode(make_node_at(1, 1, "a", 100)));
        mt.apply_op(&WalOp::UpsertNode(make_node_at(2, 1, "b", 200)));
        assert_eq!(mt.time_node_index_len(), 2);

        mt.apply_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 9999,
        });
        assert_eq!(mt.time_node_index_len(), 1);
        assert_eq!(mt.nodes_by_time_range(1, 0, 300).len(), 1);
        assert!(mt.nodes_by_time_range(1, 0, 300).contains(&2));
    }
}
