//! Candidate graph memberships. Readers never acquire the secondary/writer lock.
use super::*;
use crate::engine::participant::{AppendMap, Session, VersionWalk};
use std::cmp::Reverse;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

type AdjKey = (u64, u64, Reverse<u64>);
type GroupKey = (u64, u32, Reverse<u64>);
type LabelKey = (u32, u64, Reverse<u64>);
type TimeKey = (u32, i64, u64, Reverse<u64>);
type LogicalAdj = (u64, u64);
type LogicalLabel = (u32, u64);
type AdjExportRefill = (Vec<(u64, AdjEntry)>, VersionWalk<LogicalAdj>);
type LabelRefill = (Vec<LogicalLabel>, VersionWalk<LogicalLabel>);
const ADJ_BYTES: usize = 128;
const GROUP_BYTES: usize = 40;
const LABEL_BYTES: usize = 32;
const TIME_BYTES: usize = 48;
const BOUND_BYTES: usize = 48;
const CACHE_BYTES: usize = 32;

pub(super) fn physical_direction(direction: Direction) -> usize {
    match direction {
        Direction::Outgoing => 0,
        Direction::Incoming => 1,
        Direction::Both => panic!("physical adjacency direction required"),
    }
}

pub(super) struct GraphMaps {
    adjacency: [AppendMap<AdjKey, Option<AdjEntry>>; 2],
    groups: [AppendMap<GroupKey, bool>; 2],
    labels: [AppendMap<LabelKey, bool>; 2],
    times: AppendMap<TimeKey, bool>,
    endpoint_bounds: [AppendMap<(u64, Reverse<u64>), usize>; 2],
    label_bounds: [AppendMap<(u32, Reverse<u64>), usize>; 2],
    retained_postings: [AtomicUsize; 2],
    retained_groups: [AtomicUsize; 2],
    writer: Mutex<GraphWriter>,
    estimated: AtomicUsize,
    #[cfg(test)]
    installed: AtomicUsize,
    #[cfg(test)]
    label_refill_groups: [AtomicUsize; 2],
    #[cfg(test)]
    label_refill_calls: [AtomicUsize; 2],
}
#[derive(Default)]
struct GraphWriter {
    groups: [HashMap<(u64, u32), u64>; 2],
    endpoints: [HashMap<u64, usize>; 2],
    labels: [HashMap<u32, usize>; 2],
}
#[derive(Default)]
pub(super) struct GraphTransition {
    adj: [BTreeMap<(u64, u64), Option<AdjEntry>>; 2],
    groups: [BTreeMap<(u64, u32), bool>; 2],
    // Canonical old/new memberships are disjoint within one operation; time
    // retractions are skipped when unchanged. These actions need no coalescing.
    labels: [Vec<((u32, u64), bool)>; 2],
    times: Vec<((u32, i64, u64), bool)>,
}
#[derive(Default)]
pub(super) struct GraphUpdates {
    groups: [HashMap<(u64, u32), u64>; 2],
    endpoints: [HashMap<u64, usize>; 2],
    labels: [HashMap<u32, usize>; 2],
    retained_postings: [usize; 2],
    retained_groups: [usize; 2],
    cache_entries: usize,
}
pub(super) struct GraphPreparation<'a> {
    maps: &'a GraphMaps,
    current: MutexGuard<'a, GraphWriter>,
    updates: GraphUpdates,
    new_adj: [BTreeSet<(u64, u64)>; 2],
    new_labels: [BTreeSet<(u32, u64)>; 2],
}
impl GraphMaps {
    pub(super) fn all_adj_refill(
        &self,
        s: &Session<'_>,
        d: usize,
        seq: u64,
        bound: Bound<(u64, u64)>,
    ) -> AdjExportRefill {
        let mut entries = Vec::new();
        let walk = self.adjacency[d].walk_adjacency(
            s,
            seq,
            (bound, Bound::Unbounded),
            4096,
            |key, value| {
                if let Some(Some(entry)) = value {
                    entries.push((key.0, entry.clone()));
                }
                true
            },
        );
        (entries, walk)
    }
    pub(super) fn new() -> Self {
        Self {
            adjacency: std::array::from_fn(|_| AppendMap::new()),
            groups: std::array::from_fn(|_| AppendMap::new()),
            labels: std::array::from_fn(|_| AppendMap::new()),
            times: AppendMap::new(),
            endpoint_bounds: std::array::from_fn(|_| AppendMap::new()),
            label_bounds: std::array::from_fn(|_| AppendMap::new()),
            retained_postings: std::array::from_fn(|_| AtomicUsize::new(0)),
            retained_groups: std::array::from_fn(|_| AtomicUsize::new(0)),
            writer: Mutex::new(GraphWriter::default()),
            estimated: AtomicUsize::new(0),
            #[cfg(test)]
            installed: AtomicUsize::new(0),
            #[cfg(test)]
            label_refill_groups: std::array::from_fn(|_| AtomicUsize::new(0)),
            #[cfg(test)]
            label_refill_calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }
    pub(super) fn prepare(&self) -> GraphPreparation<'_> {
        GraphPreparation {
            maps: self,
            current: self.writer.lock().unwrap(),
            updates: GraphUpdates::default(),
            new_adj: Default::default(),
            new_labels: Default::default(),
        }
    }
    pub(super) fn install_bounds(&self, s: &Session<'_>, seq: u64, updates: &GraphUpdates) {
        let mut bytes = updates.cache_entries * CACHE_BYTES;
        for d in 0..2 {
            self.retained_postings[d].fetch_add(updates.retained_postings[d], Ordering::Relaxed);
            self.retained_groups[d].fetch_add(updates.retained_groups[d], Ordering::Relaxed);
            for (&owner, &count) in &updates.endpoints[d] {
                self.endpoint_bounds[d].insert_unique(s, (owner, Reverse(seq)), count);
                bytes += BOUND_BYTES;
            }
            for (&label, &count) in &updates.labels[d] {
                self.label_bounds[d].insert_unique(s, (label, Reverse(seq)), count);
                bytes += BOUND_BYTES;
            }
        }
        self.estimated.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(super) fn finish_install(&self, updates: GraphUpdates) {
        let mut current = self.writer.lock().unwrap();
        for d in 0..2 {
            current.groups[d].extend(updates.groups[d].iter().map(|(&k, &v)| (k, v)));
            current.endpoints[d].extend(updates.endpoints[d].iter().map(|(&k, &v)| (k, v)));
            current.labels[d].extend(updates.labels[d].iter().map(|(&k, &v)| (k, v)));
        }
    }
    pub(super) fn install(
        &self,
        s: &Session<'_>,
        seq: u64,
        t: GraphTransition,
        primary: &PrimaryMaps,
    ) -> Result<(), EngineError> {
        let mut bytes = 0;
        for (d, entries) in t.adj.into_iter().enumerate() {
            for ((owner, id), value) in entries {
                self.adjacency[d].insert_unique(s, (owner, id, Reverse(seq)), value);
                bytes += ADJ_BYTES;
                #[cfg(test)]
                self.installed.fetch_add(1, Ordering::Relaxed);
            }
            #[cfg(test)]
            primary.pause_for_test(if d == 0 { 5 } else { 6 })?;
        }
        for (d, entries) in t.groups.into_iter().enumerate() {
            for ((owner, label), value) in entries {
                self.groups[d].insert_unique(s, (owner, label, Reverse(seq)), value);
                bytes += GROUP_BYTES;
            }
        }
        for (d, entries) in t.labels.into_iter().enumerate() {
            for ((label, id), value) in entries {
                self.labels[d].insert_unique(s, (label, id, Reverse(seq)), value);
                bytes += LABEL_BYTES;
            }
        }
        for ((label, time, id), value) in t.times {
            self.times
                .insert_unique(s, (label, time, id, Reverse(seq)), value);
            bytes += TIME_BYTES;
        }
        #[cfg(test)]
        primary.pause_for_test(8)?;
        #[cfg(test)]
        primary.pause_for_test(7)?;
        #[cfg(not(test))]
        let _ = primary;
        self.estimated.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn estimated(&self) -> usize {
        self.estimated.load(Ordering::Relaxed)
    }
    pub(super) fn retained_groups(&self, d: usize) -> usize {
        self.retained_groups[d].load(Ordering::Relaxed)
    }
    pub(super) fn retained_postings(&self, d: usize) -> u64 {
        self.retained_postings[d].load(Ordering::Relaxed) as u64
    }
    pub(super) fn label_bound(&self, s: &Session<'_>, d: usize, label: u32) -> usize {
        self.label_bounds[d]
            .lower(s, Bound::Included(&(label, Reverse(u64::MAX))))
            .filter(|(key, _)| key.0 == label)
            .map_or(0, |(_, n)| *n)
    }
    pub(super) fn endpoint_bound(&self, s: &Session<'_>, d: usize, owner: u64) -> usize {
        self.endpoint_bounds[d]
            .lower(s, Bound::Included(&(owner, Reverse(u64::MAX))))
            .filter(|(key, _)| key.0 == owner)
            .map_or(0, |(_, n)| *n)
    }
    #[allow(clippy::too_many_arguments)] // Mirrors the existing bounded posting refill contract.
    pub(super) fn adj_refill(
        &self,
        s: &Session<'_>,
        d: usize,
        owner: u64,
        seq: u64,
        bound: Bound<u64>,
        labels: Option<&[u32]>,
        max: usize,
        mut entries: Vec<AdjEntry>,
    ) -> MemtableAdjacencyPostingRefill {
        entries.clear();
        let start = bound.map(|id| (owner, id));
        let start = if matches!(start, Bound::Unbounded) {
            Bound::Included((owner, 0))
        } else {
            start
        };
        let w = self.adjacency[d].walk_adjacency(
            s,
            seq,
            (start, Bound::Included((owner, u64::MAX))),
            max,
            |_, value| {
                if let Some(Some(entry)) = value {
                    if labels.is_none_or(|ls| ls.contains(&entry.label_id)) {
                        entries.push(entry.clone());
                    }
                }
                true
            },
        );
        MemtableAdjacencyPostingRefill {
            entries,
            last_edge_id_examined: w.last.map(|key| key.1),
            examined: w.examined,
            exhausted: w.exhausted,
        }
    }
    pub(super) fn owner_refill(
        &self,
        s: &Session<'_>,
        d: usize,
        seq: u64,
        bound: Bound<(u64, u32)>,
        labels: Option<&[u32]>,
        max: usize,
    ) -> MemtableAdjacencyOwnerRefill {
        let mut owner = None;
        let w = self.groups[d].walk_groups(s, seq, (bound, Bound::Unbounded), max, |key, value| {
            if value == Some(&true) && labels.is_none_or(|ls| ls.binary_search(&key.1).is_ok()) {
                owner = Some(key.0);
                false
            } else {
                true
            }
        });
        MemtableAdjacencyOwnerRefill {
            owner_head: owner,
            last_group_key_examined: w.last,
            examined: w.examined,
            exhausted: w.exhausted,
        }
    }
    pub(super) fn label_refill(
        &self,
        s: &Session<'_>,
        d: usize,
        label: Option<u32>,
        seq: u64,
        bound: Bound<(u32, u64)>,
        max: usize,
    ) -> LabelRefill {
        let mut ids = Vec::new();
        let lower = if matches!(bound, Bound::Unbounded) {
            label.map_or(Bound::Unbounded, |l| Bound::Included((l, 0)))
        } else {
            bound
        };
        let upper = label.map_or(Bound::Unbounded, |l| Bound::Included((l, u64::MAX)));
        let w = self.labels[d].walk_labels(s, seq, (lower, upper), max, |key, value| {
            if value == Some(&true) {
                ids.push(key);
            }
            true
        });
        #[cfg(test)]
        {
            self.label_refill_groups[d].fetch_add(w.examined, Ordering::Relaxed);
            self.label_refill_calls[d].fetch_add(1, Ordering::Relaxed);
        }
        (ids, w)
    }
    pub(super) fn time_refill(
        &self,
        s: &Session<'_>,
        label: u32,
        from: i64,
        to: i64,
        seq: u64,
        bound: Bound<(u32, i64, u64)>,
    ) -> (Vec<u64>, VersionWalk<(u32, i64, u64)>) {
        let mut ids = Vec::new();
        let lower = if matches!(bound, Bound::Unbounded) {
            Bound::Included((label, from, 0))
        } else {
            bound
        };
        let w = self.times.walk_times(
            s,
            seq,
            (lower, Bound::Included((label, to, u64::MAX))),
            4096,
            |key, value| {
                if value == Some(&true) {
                    ids.push(key.2);
                }
                true
            },
        );
        (ids, w)
    }
}

impl Memtable {
    #[cfg(test)]
    pub(crate) fn take_read_scan_work_for_test(&self) -> [(usize, usize); 3] {
        let m = self.primary.as_ref().expect("candidate map");
        [
            (
                m.graph.label_refill_groups[0].swap(0, Ordering::Relaxed),
                m.graph.label_refill_calls[0].swap(0, Ordering::Relaxed),
            ),
            (
                m.graph.label_refill_groups[1].swap(0, Ordering::Relaxed),
                m.graph.label_refill_calls[1].swap(0, Ordering::Relaxed),
            ),
            (m.edges.take_scan_groups_for_test(), 0),
        ]
    }
    pub(super) fn graph_session<T>(
        &self,
        read: impl FnOnce(&PrimaryMaps, &Session<'_>) -> T,
    ) -> Result<T, EngineError> {
        match self.primary_read()? {
            PrimaryRead::Candidate(m, s) => Ok(read(m, &s)),
            PrimaryRead::Legacy(_) => unreachable!("candidate graph helper on legacy memtable"),
        }
    }
    pub(super) fn graph_labels(
        &self,
        d: usize,
        label: Option<u32>,
        seq: u64,
    ) -> Result<Vec<(u32, u64)>, EngineError> {
        let mut rows = Vec::new();
        let mut bound = Bound::Unbounded;
        loop {
            let (chunk, w) =
                self.graph_session(|m, s| m.graph.label_refill(s, d, label, seq, bound, 4096))?;
            rows.extend(chunk);
            if w.exhausted {
                return Ok(rows);
            }
            bound = Bound::Excluded(w.last.expect("nonterminal label refill progresses"));
        }
    }
    pub(super) fn graph_label_count(&self, label: u32, seq: u64) -> Result<usize, EngineError> {
        let mut count = 0;
        let mut bound = Bound::Included((label, 0));
        loop {
            let walk = self.graph_session(|m, s| {
                m.graph.labels[0].walk_labels(
                    s,
                    seq,
                    (bound, Bound::Included((label, u64::MAX))),
                    4096,
                    |_, value| {
                        if value == Some(&true) {
                            count += 1;
                        }
                        true
                    },
                )
            })?;
            if walk.exhausted {
                return Ok(count);
            }
            bound = Bound::Excluded(
                walk.last
                    .expect("nonterminal label count refill progresses"),
            );
        }
    }
    pub(super) fn graph_label_next(
        &self,
        d: usize,
        label: u32,
        seq: u64,
        bound: Bound<u64>,
    ) -> Result<Option<u64>, EngineError> {
        let mut bound = bound.map(|id| (label, id));
        loop {
            let (found, walk) = self.graph_session(|m, session| {
                let mut found = None;
                let lower = if matches!(bound, Bound::Unbounded) {
                    Bound::Included((label, 0))
                } else {
                    bound
                };
                let walk = m.graph.labels[d].walk_labels(
                    session,
                    seq,
                    (lower, Bound::Included((label, u64::MAX))),
                    4096,
                    |(_, id), opinion| {
                        if opinion == Some(&true) {
                            found = Some(id);
                            false
                        } else {
                            true
                        }
                    },
                );
                (found, walk)
            })?;
            if found.is_some() || walk.exhausted {
                return Ok(found);
            }
            bound = Bound::Excluded(walk.last.expect("nonterminal scalar label seek progresses"));
        }
    }
    pub(super) fn graph_time_each<F>(
        &self,
        label: u32,
        from: i64,
        to: i64,
        seq: u64,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64) -> ControlFlow<()>,
    {
        if from > to {
            return Ok(ControlFlow::Continue(()));
        }
        let mut bound = Bound::Unbounded;
        loop {
            let (chunk, w) =
                self.graph_session(|m, s| m.graph.time_refill(s, label, from, to, seq, bound))?;
            for id in chunk {
                if callback(id).is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            if w.exhausted {
                return Ok(ControlFlow::Continue(()));
            }
            bound = Bound::Excluded(w.last.expect("nonterminal time refill progresses"));
        }
    }
    // A session spans bounded work across owners. No lease or borrowed entry reaches a caller.
    pub(super) fn graph_neighbors_one(
        &self,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        limit: usize,
        seq: u64,
        hide_neighbors: bool,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        let dirs: &[usize] = match direction {
            Direction::Outgoing => &[0],
            Direction::Incoming => &[1],
            Direction::Both => &[0, 1],
        };
        let mut rows = Vec::new();
        for &d in dirs {
            let mut bound = Bound::Included((owner, 0));
            loop {
                let walk = self.graph_session(|m, s| {
                    if m.graph_node_deleted(s, owner, seq) {
                        return VersionWalk {
                            last: None,
                            examined: 0,
                            exhausted: true,
                        };
                    }
                    m.graph.adjacency[d].walk_adjacency(
                        s,
                        seq,
                        (bound, Bound::Included((owner, u64::MAX))),
                        4096,
                        |_, value| {
                            let Some(Some(e)) = value else { return true };
                            if labels.is_some_and(|ls| !ls.contains(&e.label_id))
                                || d == 1 && direction == Direction::Both && e.neighbor_id == owner
                                || hide_neighbors && m.graph_node_deleted(s, e.neighbor_id, seq)
                            {
                                return true;
                            }
                            rows.push(NeighborRecord {
                                node_id: e.neighbor_id,
                                edge_id: e.edge_id,
                                edge_label_id: e.label_id,
                                weight: e.weight,
                                valid_from: e.valid_from,
                                valid_to: e.valid_to,
                            });
                            limit == 0 || rows.len() < limit
                        },
                    )
                })?;
                if limit > 0 && rows.len() >= limit {
                    return Ok(rows);
                }
                if walk.exhausted {
                    break;
                }
                bound =
                    Bound::Excluded(walk.last.expect("nonterminal adjacency refill progresses"));
            }
        }
        Ok(rows)
    }
    // Only numeric work occurs inside a session. Public/user callbacks still receive owned chunks.
    pub(super) fn graph_adj_stats(
        &self,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        seq: u64,
        reference_time: i64,
    ) -> Result<(u64, f64), EngineError> {
        let dirs: &[usize] = match direction {
            Direction::Outgoing => &[0],
            Direction::Incoming => &[1],
            Direction::Both => &[0, 1],
        };
        let mut count = 0;
        let mut weight = 0.0;
        for &d in dirs {
            let mut bound = Bound::Included((owner, 0));
            loop {
                let walk = self.graph_session(|m, s| {
                    if m.graph_node_deleted(s, owner, seq) {
                        return VersionWalk {
                            last: None,
                            examined: 0,
                            exhausted: true,
                        };
                    }
                    m.graph.adjacency[d].walk_adjacency(
                        s,
                        seq,
                        (bound, Bound::Included((owner, u64::MAX))),
                        4096,
                        |_, value| {
                            let Some(Some(e)) = value else { return true };
                            if labels.is_some_and(|ls| !ls.contains(&e.label_id))
                                || d == 1 && direction == Direction::Both && e.neighbor_id == owner
                                || m.graph_node_deleted(s, e.neighbor_id, seq)
                                || !(e.valid_from <= reference_time && reference_time < e.valid_to)
                            {
                                return true;
                            }
                            count += 1;
                            weight += e.weight as f64;
                            true
                        },
                    )
                })?;
                if walk.exhausted {
                    break;
                }
                bound = Bound::Excluded(walk.last.expect("nonterminal degree refill progresses"));
            }
        }
        Ok((count, weight))
    }
    pub(super) fn graph_neighbors_batch(
        &self,
        owners: &[u64],
        direction: Direction,
        labels: Option<&[u32]>,
        seq: u64,
        hide_neighbors: bool,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        let dirs: &[usize] = match direction {
            Direction::Outgoing => &[0],
            Direction::Incoming => &[1],
            Direction::Both => &[0, 1],
        };
        let mut result = NodeIdMap::default();
        let mut owner_index = 0;
        let mut direction_index = 0;
        let mut bound = Bound::Unbounded;
        while owner_index < owners.len() {
            let chunk = self.graph_session(|m, s| {
                let mut work = 0;
                let mut chunk = Vec::new();
                while owner_index < owners.len() && work < 4096 {
                    let owner = owners[owner_index];
                    if m.graph_node_deleted(s, owner, seq) {
                        owner_index += 1;
                        direction_index = 0;
                        bound = Bound::Unbounded;
                        continue;
                    }
                    let d = dirs[direction_index];
                    let r = m.graph.adj_refill(
                        s,
                        d,
                        owner,
                        seq,
                        bound,
                        labels,
                        4096 - work,
                        Vec::new(),
                    );
                    work += r.examined;
                    for entry in r.entries {
                        if (d == 1 && direction == Direction::Both && entry.neighbor_id == owner)
                            || (hide_neighbors && m.graph_node_deleted(s, entry.neighbor_id, seq))
                        {
                            continue;
                        }
                        chunk.push((
                            owner,
                            NeighborRecord {
                                node_id: entry.neighbor_id,
                                edge_id: entry.edge_id,
                                edge_label_id: entry.label_id,
                                weight: entry.weight,
                                valid_from: entry.valid_from,
                                valid_to: entry.valid_to,
                            },
                        ));
                    }
                    if r.exhausted {
                        direction_index += 1;
                        bound = Bound::Unbounded;
                        if direction_index == dirs.len() {
                            owner_index += 1;
                            direction_index = 0;
                        }
                    } else {
                        bound = Bound::Excluded(
                            r.last_edge_id_examined
                                .expect("nonterminal adjacency refill progresses"),
                        );
                    }
                }
                chunk
            })?;
            for (owner, entry) in chunk {
                result.entry(owner).or_insert_with(Vec::new).push(entry);
            }
        }
        Ok(result)
    }
    pub(super) fn graph_adj_each<F>(
        &self,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        seq: u64,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        let dirs: &[usize] = match direction {
            Direction::Outgoing => &[0],
            Direction::Incoming => &[1],
            Direction::Both => &[0, 1],
        };
        for &d in dirs {
            let mut bound = Bound::Unbounded;
            loop {
                let r = self.graph_session(|m, s| {
                    let mut r =
                        m.graph
                            .adj_refill(s, d, owner, seq, bound, labels, 4096, Vec::new());
                    if m.graph_node_deleted(s, owner, seq) {
                        r.entries.clear();
                        r.exhausted = true;
                    }
                    r.entries.retain(|e| {
                        !(d == 1 && direction == Direction::Both && e.neighbor_id == owner
                            || m.graph_node_deleted(s, e.neighbor_id, seq))
                    });
                    r
                })?;
                for e in r.entries {
                    if callback(e.edge_id, e.neighbor_id, e.weight, e.valid_from, e.valid_to)
                        .is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                if r.exhausted {
                    break;
                }
                bound = Bound::Excluded(
                    r.last_edge_id_examined
                        .expect("nonterminal adjacency refill progresses"),
                );
            }
        }
        Ok(ControlFlow::Continue(()))
    }
    #[allow(dead_code)] // No production scan caller; stop copying at the first valid opinion.
    pub(super) fn graph_endpoint_next(
        &self,
        owner: u64,
        d: usize,
        labels: Option<&[u32]>,
        seq: u64,
        mut bound: Bound<u64>,
    ) -> Result<Option<u64>, EngineError> {
        loop {
            let (found, last, exhausted) = self.graph_session(|m, session| {
                if m.graph_node_deleted(session, owner, seq) {
                    return (None, None, true);
                }
                let lower = bound.map(|id| (owner, id));
                let lower = if matches!(lower, Bound::Unbounded) {
                    Bound::Included((owner, 0))
                } else {
                    lower
                };
                let mut found = None;
                let walk = m.graph.adjacency[d].walk_adjacency(
                    session,
                    seq,
                    (lower, Bound::Included((owner, u64::MAX))),
                    4096,
                    |_, opinion| {
                        if let Some(Some(entry)) = opinion {
                            if labels.is_none_or(|ls| ls.contains(&entry.label_id))
                                && !m.graph_node_deleted(session, entry.neighbor_id, seq)
                            {
                                found = Some(entry.edge_id);
                                return false;
                            }
                        }
                        true
                    },
                );
                (found, walk.last.map(|key| key.1), walk.exhausted)
            })?;
            if found.is_some() || exhausted {
                return Ok(found);
            }
            bound = Bound::Excluded(last.expect("nonterminal scalar endpoint seek progresses"));
        }
    }
    pub(super) fn graph_endpoint_estimate(
        &self,
        owners: &[u64],
        direction: Direction,
        labels: Option<&[u32]>,
        seq: u64,
    ) -> Result<MemtableEndpointCountEstimate, EngineError> {
        self.graph_session(|m, s| {
            let mut count = 0usize;
            if !labels.is_some_and(<[u32]>::is_empty) {
                for &owner in owners {
                    if m.graph_node_deleted(s, owner, seq) {
                        continue;
                    }
                    if direction != Direction::Incoming {
                        count = count.saturating_add(m.graph.endpoint_bound(s, 0, owner));
                    }
                    if direction != Direction::Outgoing {
                        count = count.saturating_add(m.graph.endpoint_bound(s, 1, owner));
                    }
                }
            }
            MemtableEndpointCountEstimate {
                count,
                exact: count == 0,
            }
        })
    }
    pub(super) fn graph_export_time(&self) -> Result<BTreeSet<(u32, i64, u64)>, EngineError> {
        let mut rows = BTreeSet::new();
        let mut bound = Bound::Unbounded;
        loop {
            let (chunk, w) = self.graph_session(|m, s| {
                let mut chunk = Vec::new();
                let w = m.graph.times.walk_times(
                    s,
                    u64::MAX,
                    (bound, Bound::Unbounded),
                    4096,
                    |key, value| {
                        if value == Some(&true) {
                            chunk.push(key);
                        }
                        true
                    },
                );
                (chunk, w)
            })?;
            rows.extend(chunk);
            if w.exhausted {
                return Ok(rows);
            }
            bound = Bound::Excluded(w.last.expect("nonterminal time export progresses"));
        }
    }
    pub(super) fn graph_export_adj(
        &self,
        d: usize,
    ) -> Result<NodeIdMap<NodeIdMap<AdjEntry>>, EngineError> {
        let mut rows = NodeIdMap::default();
        let mut bound = Bound::Unbounded;
        loop {
            let (chunk, w) =
                self.graph_session(|m, s| m.graph.all_adj_refill(s, d, u64::MAX, bound))?;
            for (owner, entry) in chunk {
                rows.entry(owner)
                    .or_insert_with(NodeIdMap::default)
                    .insert(entry.edge_id, entry);
            }
            if w.exhausted {
                return Ok(rows);
            }
            bound = Bound::Excluded(w.last.expect("nonterminal adjacency export progresses"));
        }
    }
}
impl GraphPreparation<'_> {
    fn group(
        &mut self,
        d: usize,
        key: (u64, u32),
        add: bool,
        t: &mut GraphTransition,
    ) -> Result<(), EngineError> {
        let first = !self.current.groups[d].contains_key(&key)
            && !self.updates.groups[d].contains_key(&key);
        let current = self.updates.groups[d]
            .entry(key)
            .or_insert_with(|| self.current.groups[d].get(&key).copied().unwrap_or(0));
        let old = *current;
        *current = if add {
            old.checked_add(1)
        } else {
            old.checked_sub(1)
        }
        .ok_or_else(|| {
            EngineError::InvalidOperation(
                "candidate adjacency group count overflow/underflow".into(),
            )
        })?;
        if (old == 0) != (*current == 0) {
            t.groups[d].insert(key, *current != 0);
            if first && old == 0 && add {
                self.updates.retained_groups[d] += 1;
                self.updates.cache_entries += 1;
            }
        }
        Ok(())
    }
    fn posting(
        &mut self,
        s: &Session<'_>,
        d: usize,
        key: (u64, u64),
        value: Option<AdjEntry>,
        known: bool,
        t: &mut GraphTransition,
    ) {
        t.adj[d].insert(key, value);
        if known || self.new_adj[d].contains(&key) {
            return;
        }
        let exists = self.maps.adjacency[d]
            .lower(s, Bound::Included(&(key.0, key.1, Reverse(u64::MAX))))
            .is_some_and(|(k, _)| (k.0, k.1) == key);
        if exists {
            return;
        }
        self.new_adj[d].insert(key);
        self.updates.retained_postings[d] += 1;
        if !self.current.endpoints[d].contains_key(&key.0)
            && !self.updates.endpoints[d].contains_key(&key.0)
        {
            self.updates.cache_entries += 1;
        }
        let n = self.updates.endpoints[d]
            .entry(key.0)
            .or_insert_with(|| self.current.endpoints[d].get(&key.0).copied().unwrap_or(0));
        *n += 1;
    }
    fn label(
        &mut self,
        s: &Session<'_>,
        d: usize,
        key: (u32, u64),
        value: bool,
        known: bool,
        t: &mut GraphTransition,
    ) {
        t.labels[d].push((key, value));
        if known || self.new_labels[d].contains(&key) {
            return;
        }
        let exists = self.maps.labels[d]
            .lower(s, Bound::Included(&(key.0, key.1, Reverse(u64::MAX))))
            .is_some_and(|(k, _)| (k.0, k.1) == key);
        if exists {
            return;
        }
        self.new_labels[d].insert(key);
        if !self.current.labels[d].contains_key(&key.0)
            && !self.updates.labels[d].contains_key(&key.0)
        {
            self.updates.cache_entries += 1;
        }
        let n = self.updates.labels[d]
            .entry(key.0)
            .or_insert_with(|| self.current.labels[d].get(&key.0).copied().unwrap_or(0));
        *n += 1;
    }
    pub(super) fn node(
        &mut self,
        s: &Session<'_>,
        id: u64,
        old: Option<(NodeLabelSet, i64)>,
        local: Option<(NodeLabelSet, i64)>,
        new: Option<&NodeRecord>,
    ) -> GraphTransition {
        let mut t = GraphTransition::default();
        if let Some((labels, time)) = old {
            for &label in labels.as_slice() {
                if new.is_none_or(|n| !n.label_ids.contains(label)) {
                    self.label(
                        s,
                        0,
                        (label, id),
                        false,
                        local.is_some_and(|n| n.0.contains(label)),
                        &mut t,
                    );
                }
                if new.is_none_or(|n| !n.label_ids.contains(label) || n.updated_at != time) {
                    t.times.push(((label, time, id), false));
                }
            }
        }
        if let Some(n) = new {
            for &label in n.label_ids.as_slice() {
                if local.is_none_or(|old| !old.0.contains(label)) {
                    self.label(s, 0, (label, id), true, false, &mut t);
                }
                if local.is_none_or(|old| !old.0.contains(label) || old.1 != n.updated_at) {
                    t.times.push(((label, n.updated_at, id), true));
                }
            }
        }
        t.labels[0].sort_unstable_by_key(|(key, _)| *key);
        t.times.sort_unstable_by_key(|(key, _)| *key);
        t
    }
    pub(super) fn edge(
        &mut self,
        s: &Session<'_>,
        id: u64,
        old: Option<(u64, u64, u32)>,
        local: Option<(u64, u64, u32)>,
        new: Option<&EdgeRecord>,
    ) -> Result<GraphTransition, EngineError> {
        let mut t = GraphTransition::default();
        for d in 0..2 {
            let old_owner = old.map(|e| if d == 0 { e.0 } else { e.1 });
            let local_group = local.map(|e| (if d == 0 { e.0 } else { e.1 }, e.2));
            let new_group = new.map(|e| (if d == 0 { e.from } else { e.to }, e.label_id));
            if local_group != new_group {
                if let Some(key) = local_group {
                    self.group(d, key, false, &mut t)?;
                }
                if let Some(key) = new_group {
                    self.group(d, key, true, &mut t)?;
                }
            }
            if let Some(owner) = old_owner {
                if new_group.is_none_or(|g| g.0 != owner) {
                    self.posting(s, d, (owner, id), None, local.is_some(), &mut t);
                }
            }
            if let Some(e) = new {
                let owner = if d == 0 { e.from } else { e.to };
                let entry = AdjEntry {
                    edge_id: id,
                    neighbor_id: if d == 0 { e.to } else { e.from },
                    label_id: e.label_id,
                    weight: e.weight,
                    valid_from: e.valid_from,
                    valid_to: e.valid_to,
                };
                self.posting(
                    s,
                    d,
                    (owner, id),
                    Some(entry),
                    local_group.is_some_and(|g| g.0 == owner),
                    &mut t,
                );
            }
        }
        if let Some(old) = old {
            if new.is_none_or(|n| n.label_id != old.2) {
                self.label(s, 1, (old.2, id), false, local.is_some(), &mut t);
            }
        }
        if let Some(e) = new {
            if local.is_none_or(|old| old.2 != e.label_id) {
                self.label(s, 1, (e.label_id, id), true, false, &mut t);
            }
        }
        t.labels[1].sort_unstable_by_key(|(key, _)| *key);
        Ok(t)
    }
    pub(super) fn finish(self) -> GraphUpdates {
        self.updates
    }
}

#[cfg(test)]
impl GraphMaps {
    pub(super) fn estimated_full(&self, s: &Session<'_>) -> usize {
        let count = self.times.fold_all_for_test(s, 0, |n, _, _| n + TIME_BYTES);
        let current = self.writer.lock().unwrap();
        (0..2).fold(count, |n, d| {
            n + self.adjacency[d].fold_all_for_test(s, 0, |n, _, _| n + ADJ_BYTES)
                + self.groups[d].fold_all_for_test(s, 0, |n, _, _| n + GROUP_BYTES)
                + self.labels[d].fold_all_for_test(s, 0, |n, _, _| n + LABEL_BYTES)
                + self.endpoint_bounds[d].fold_all_for_test(s, 0, |n, _, _| n + BOUND_BYTES)
                + self.label_bounds[d].fold_all_for_test(s, 0, |n, _, _| n + BOUND_BYTES)
                + (current.groups[d].len() + current.endpoints[d].len() + current.labels[d].len())
                    * CACHE_BYTES
        })
    }
}
#[cfg(test)]
impl Memtable {
    pub(crate) fn locked_graph_slots_for_test(&self) -> [usize; 10] {
        let s = self.state.read().unwrap();
        assert!(
            s.ordered_edge_ids.is_empty(),
            "candidate ordered IDs reuse primary"
        );
        [
            s.adj_out.len(),
            s.adj_in.len(),
            s.ordered_adj_out.len(),
            s.ordered_adj_in.len(),
            s.ordered_adj_out_groups.len(),
            s.ordered_adj_in_groups.len(),
            s.label_node_index.len(),
            s.label_edge_index.len(),
            s.ordered_label_edge_index.len(),
            s.time_node_index.len(),
        ]
    }
    pub(crate) fn graph_versions_for_test(&self) -> [usize; 4] {
        self.graph_session(|m, s| {
            let g = &m.graph;
            let mut counts = [0; 4];
            for d in 0..2 {
                counts[0] += g.adjacency[d].fold_all_for_test(s, 0, |n, _, _| n + 1);
                counts[1] += g.groups[d].fold_all_for_test(s, 0, |n, _, _| n + 1);
                counts[2] += g.labels[d].fold_all_for_test(s, 0, |n, _, _| n + 1);
            }
            counts[3] = g.times.fold_all_for_test(s, 0, |n, _, _| n + 1);
            counts
        })
        .unwrap()
    }
}
#[cfg(test)]
impl Memtable {
    pub(crate) fn verify_group_counts_for_test(&self) {
        let maps = GraphMaps::new();
        let mut t = GraphTransition::default();
        {
            let mut p = maps.prepare();
            assert!(p.group(0, (1, 2), false, &mut t).is_err());
        }
        maps.writer.lock().unwrap().groups[0].insert((1, 2), u64::MAX);
        {
            let mut p = maps.prepare();
            assert!(p.group(0, (1, 2), true, &mut t).is_err());
        }
        maps.writer.lock().unwrap().groups[0].clear();
        let mut p = maps.prepare();
        p.group(0, (1, 2), true, &mut t).unwrap();
        p.group(0, (1, 2), false, &mut t).unwrap();
        p.group(0, (1, 2), true, &mut t).unwrap();
        assert_eq!(t.groups[0], BTreeMap::from([((1, 2), true)]));
        assert_eq!(p.updates.retained_groups[0], 1);
        assert_eq!(p.updates.groups[0][&(1, 2)], 1);
    }
}
