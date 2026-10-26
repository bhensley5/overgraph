//! CP45a's private real-record slice. This is not a public engine route.
//! Primary/equality/outgoing opinions share one immutable published frontier.
#![allow(dead_code)]
#[cfg(test)]
#[path = "concurrency_timing.rs"]
mod concurrency_timing;

use super::concurrency_lifecycle::{
    fail_stop, CommitFence, CommitState, Deadline, LifecycleRef, OperationPermit, Outcome,
    Pressure, QueuePosition, RuntimeOwnerPermit,
};
use super::participant::{AppendMap, ParticipantAdmission, ParticipantHandle, Session};
use super::payload::{Drain, Owned};
use super::reclamation::{
    BuildPermit, DomainHandle, Managed, OwnerPermit, ReclaimDomain, ReclamationStatsHandle,
    ResourceClass,
};
use crate::memtable::AdjEntry;
use crate::types::{
    validate_label_token_name, EdgeRecord, NodeRecord, PropValue, WalOp, WalSyncMode,
};
use crate::wal::WalWriter;
use crate::wal_sync::{sync_pending, WalSyncState};
use arc_swap::ArcSwap;
use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

fn limit(resource: &'static str, requested: usize, limit: usize, retryable: bool) -> Outcome {
    Outcome::ResourceLimit(super::concurrency_lifecycle::ResourceLimit {
        resource,
        requested,
        limit,
        retryable,
    })
}

// Ordinary local storage. It never registers, reserves memory or grants producer authority.
pub(super) struct Local<T>(T);
impl<T> Local<T> {
    fn get(&self) -> &T {
        &self.0
    }
    fn mutate<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> Result<R, Outcome> {
        Ok(f(&mut self.0))
    }
}

const SOURCE_LIMIT: usize = 32;
const QUEUE_LIMIT: usize = 8;
const QUANTUM_GROUPS: usize = 4096;
const QUANTUM_BYTES: usize = 1024 * 1024;

type PrimaryKey = (u64, Reverse<u64>);
type MembershipKey = (u32, u64, u64, Reverse<u64>);
type OutKey = (u64, u64, Reverse<u64>);

#[derive(Default)]
struct Generation {
    id: u64,
    nodes: AppendMap<PrimaryKey, Option<Owned<NodeRecord>>>,
    edges: AppendMap<PrimaryKey, Option<Owned<EdgeRecord>>>,
    equality: AppendMap<MembershipKey, bool>,
    outgoing: AppendMap<OutKey, Option<AdjEntry>>,
    degrees: AppendMap<PrimaryKey, u64>,
    tokens: AppendMap<(u8, u32, Reverse<u64>), String>,
    token_names: AppendMap<(u8, String, Reverse<u64>), u32>,
}

// Producing and passive parents are distinct registered resources. Snapshot
// handles cannot retain a producing parent or grant producer authority.
#[derive(Default)]
struct Producers {
    generations: Vec<OwnerPermit<Generation>>,
}
type Sources = Vec<Managed<Generation>>;

#[derive(Default)]
struct PublishedState {
    seq: u64,
    max_node: u64,
    max_edge: u64,
    sources: Option<Managed<Sources>>,
}

struct Published {
    state: Option<PublishedState>,
}
static CLOSED_STATE: PublishedState = PublishedState {
    seq: 0,
    max_node: 0,
    max_edge: 0,
    sources: None,
};

#[derive(Clone)]
pub(super) struct Snapshot {
    published: Arc<Published>,
    admission: ParticipantHandle,
}

impl Snapshot {
    fn state(&self) -> &PublishedState {
        self.published.state.as_ref().map_or(&CLOSED_STATE, |s| s)
    }
    pub(super) fn seq(&self) -> u64 {
        self.state().seq
    }
    fn sources(&self) -> &[Managed<Generation>] {
        self.state()
            .sources
            .as_ref()
            .map_or(&[], |s| s.get().as_slice())
    }
    fn node<'a>(&'a self, session: &'a Session<'_>, id: u64) -> Option<&'a NodeRecord> {
        let key = (id, Reverse(self.seq()));
        for source in self.sources() {
            if let Some((found, value)) = source.get().nodes.lower(session, Bound::Included(&key)) {
                if found.0 == id {
                    return value.as_deref();
                }
            }
        }
        None
    }
    fn edge<'a>(&'a self, session: &'a Session<'_>, id: u64) -> Option<&'a EdgeRecord> {
        let key = (id, Reverse(self.seq()));
        for source in self.sources() {
            if let Some((found, value)) = source.get().edges.lower(session, Bound::Included(&key)) {
                if found.0 == id {
                    return value.as_deref();
                }
            }
        }
        None
    }
    pub(super) fn copy_node(
        &self,
        id: u64,
    ) -> Result<Local<NodeOutput<Option<NodeRecord>>>, Outcome> {
        let lease = self.admission.try_acquire().map_err(|failure| {
            limit(
                "retained_participant_limit",
                failure.requested,
                failure.limit,
                failure.retryable,
            )
        })?;
        let node = self.node(&lease, id);
        let mut result = Local(NodeOutput::<Option<NodeRecord>>::default());
        if let Some(node) = node {
            result
                .mutate(|out| {
                    *out.data = Some(empty_node(node));
                    clone_node_fields(out.data.as_mut().unwrap(), node, &mut out.clone)
                })
                .unwrap()?;
        }
        drop(lease);
        Ok(result)
    }
    pub(super) fn degree(&self, owner: u64) -> Result<u64, Outcome> {
        let lease = acquire_participant(&self.admission)?;
        Ok(self.degree_in(&lease, owner))
    }
    fn degree_in(&self, lease: &Session<'_>, owner: u64) -> u64 {
        for source in self.sources() {
            if let Some((key, degree)) = source
                .get()
                .degrees
                .lower(lease, Bound::Included(&(owner, Reverse(self.seq()))))
            {
                if key.0 == owner {
                    return *degree;
                }
            }
        }
        0
    }
    pub(super) fn token(&self, namespace: u8, id: u32) -> Result<Option<String>, Outcome> {
        let lease = acquire_participant(&self.admission)?;
        Ok(self.token_in(&lease, namespace, id).map(str::to_owned))
    }
    fn token_in<'a>(&'a self, lease: &'a Session<'_>, namespace: u8, id: u32) -> Option<&'a str> {
        for source in self.sources() {
            if let Some((key, value)) = source.get().tokens.lower(
                lease,
                Bound::Included(&(namespace, id, Reverse(self.seq()))),
            ) {
                if key.0 == namespace && key.1 == id {
                    return Some(value);
                }
            }
        }
        None
    }
    fn token_id_in(&self, lease: &Session<'_>, namespace: u8, name: &str) -> Option<u32> {
        let key = (namespace, name.to_owned(), Reverse(self.seq()));
        for source in self.sources() {
            if let Some((k, id)) = source.get().token_names.lower(lease, Bound::Included(&key)) {
                if k.0 == namespace && k.1 == name {
                    return Some(*id);
                }
            }
        }
        None
    }
    pub(super) fn equality_cursor(
        &self,
        label: u32,
        value: PropValue,
    ) -> Result<MembershipCursor, Outcome> {
        let value = Owned::new(value);
        // Bound canonical equality-key work for this private index.
        let bytes = property_wire_bytes(&value)?;
        if bytes > 16 * 1024 {
            return Err(limit("equality_key_bytes", bytes, 16 * 1024, false));
        }
        let hash = super::payload::semantic_hash(&value).map_err(payload_error)?;
        Ok(MembershipCursor {
            view: self.clone(),
            kind: MembershipKind::Equality { label, hash, value },
            last: None,
            done: false,
        })
    }
    pub(super) fn outgoing_cursor(&self, owner: u64) -> MembershipCursor {
        MembershipCursor {
            view: self.clone(),
            kind: MembershipKind::Outgoing(owner),
            last: None,
            done: false,
        }
    }
    pub(super) fn node_cursor(&self) -> NodeCursor {
        NodeCursor {
            view: self.clone(),
            last: None,
            done: false,
        }
    }
}

/// Returned records and partial clone continuations use stack-safe local ownership.
pub(super) struct NodeOutput<T: super::payload::Drain> {
    data: Owned<T>,
    clone: super::payload::CloneState,
}
impl<T: super::payload::Drain + Default> Default for NodeOutput<T> {
    fn default() -> Self {
        Self {
            data: Owned::new(T::default()),
            clone: super::payload::CloneState::default(),
        }
    }
}
impl<T: super::payload::Drain> std::ops::Deref for NodeOutput<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data
    }
}
fn clone_node_fields(
    output: &mut NodeRecord,
    input: &NodeRecord,
    state: &mut super::payload::CloneState,
) -> Result<(), Outcome> {
    output.key = input.key.clone();
    output.dense_vector = input.dense_vector.clone();
    output.sparse_vector = input.sparse_vector.clone();
    clone_props(&mut output.props, &input.props, state)
}

pub(super) struct NodeCursor {
    view: Snapshot,
    last: Option<u64>,
    done: bool,
}
pub(super) struct NodeChunk {
    pub(super) rows: Local<NodeOutput<Vec<NodeRecord>>>,
    pub(super) examined: usize,
    pub(super) copied: usize,
    pub(super) done: bool,
}
impl NodeCursor {
    pub(super) fn refill(&mut self) -> Result<NodeChunk, Outcome> {
        let mut rows = Local(NodeOutput::<Vec<NodeRecord>>::default());
        rows.0.data.reserve_exact(QUANTUM_GROUPS);
        let lease = acquire_participant(&self.view.admission)?;
        let mut examined = 0;
        let mut copied = 0;
        while !self.done && examined < QUANTUM_GROUPS && copied < QUANTUM_BYTES {
            let mut next = None;
            for source in self.view.sources() {
                let key = (self.last.unwrap_or(0), Reverse(0));
                let lower = if self.last.is_some() {
                    Bound::Excluded(&key)
                } else {
                    Bound::Included(&key)
                };
                if let Some((k, _)) = source.get().nodes.lower(&lease, lower) {
                    next = Some(next.map_or(k.0, |old: u64| old.min(k.0)));
                }
            }
            let Some(id) = next else {
                self.done = true;
                break;
            };
            if let Some(node) = self.view.node(&lease, id) {
                let bytes = node_wire_bytes(node)?;
                // Leave this group unconsumed for an empty refill. An oversized
                // item must never share a chunk with rows already copied.
                if copied != 0 && bytes > QUANTUM_BYTES - copied {
                    examined += 1; // lookahead work, but do not consume
                    break;
                }
                copied += bytes;
                rows.mutate(|r| {
                    r.data.push(empty_node(node));
                    clone_node_fields(r.data.last_mut().unwrap(), node, &mut r.clone)
                })
                .unwrap()?;
            }
            self.last = Some(id);
            examined += 1;
        }
        drop(lease);
        Ok(NodeChunk {
            rows,
            examined,
            copied,
            done: self.done,
        })
    }
}

enum MembershipKind {
    Equality {
        label: u32,
        hash: u64,
        value: Owned<PropValue>,
    },
    Outgoing(u64),
}
pub(super) struct MembershipCursor {
    view: Snapshot,
    kind: MembershipKind,
    last: Option<u64>,
    done: bool,
}
pub(super) struct Chunk {
    pub(super) rows: Local<Vec<u64>>,
    pub(super) examined: usize,
    pub(super) done: bool,
}

impl MembershipCursor {
    pub(super) fn refill(&mut self) -> Result<Chunk, Outcome> {
        let mut rows = Local(Vec::<u64>::with_capacity(QUANTUM_GROUPS));
        let lease = acquire_participant(&self.view.admission)?;
        let mut examined = 0;
        while !self.done && examined < QUANTUM_GROUPS {
            // Discovery ignores S; visibility lower-bounds at S separately.
            // Excluded (id, Reverse(0)) passes EVERY version of the last group,
            // including versions concurrently installed beyond the old frontier.
            let mut next = None;
            for source in self.view.sources() {
                let generation = source.get();
                let candidate = match &self.kind {
                    MembershipKind::Equality { label, hash, .. } => {
                        let key = (*label, *hash, self.last.unwrap_or(0), Reverse(0));
                        let lower = if self.last.is_some() {
                            Bound::Excluded(&key)
                        } else {
                            Bound::Included(&key)
                        };
                        generation
                            .equality
                            .lower(&lease, lower)
                            .and_then(|(k, _)| (k.0 == *label && k.1 == *hash).then_some(k.2))
                    }
                    MembershipKind::Outgoing(owner) => {
                        let key = (*owner, self.last.unwrap_or(0), Reverse(0));
                        let lower = if self.last.is_some() {
                            Bound::Excluded(&key)
                        } else {
                            Bound::Included(&key)
                        };
                        generation
                            .outgoing
                            .lower(&lease, lower)
                            .and_then(|(k, _)| (k.0 == *owner).then_some(k.1))
                    }
                };
                if let Some(id) = candidate {
                    next = Some(next.map_or(id, |old: u64| old.min(id)));
                }
            }
            let Some(id) = next else {
                self.done = true;
                break;
            };
            self.last = Some(id);
            examined += 1;
            let mut visible = false;
            for source in self.view.sources() {
                let generation = source.get();
                match &self.kind {
                    MembershipKind::Equality { label, hash, .. } => {
                        let key = (*label, *hash, id, Reverse(self.view.seq()));
                        if let Some((k, value)) =
                            generation.equality.lower(&lease, Bound::Included(&key))
                        {
                            if k.0 == *label && k.1 == *hash && k.2 == id {
                                visible = *value;
                                break;
                            }
                        }
                    }
                    MembershipKind::Outgoing(owner) => {
                        let key = (*owner, id, Reverse(self.view.seq()));
                        if let Some((k, value)) =
                            generation.outgoing.lower(&lease, Bound::Included(&key))
                        {
                            if k.0 == *owner && k.1 == id {
                                visible = value.is_some();
                                break;
                            }
                        }
                    }
                }
            }
            if visible {
                visible = match &self.kind {
                    MembershipKind::Equality { label, value, .. } => {
                        match self.view.node(&lease, id) {
                            Some(n) if n.label_ids.contains(*label) => match n.props.get("value") {
                                Some(p) => {
                                    super::payload::semantic_eq(p, value).map_err(payload_error)?
                                }
                                None => false,
                            },
                            _ => false,
                        }
                    }
                    MembershipKind::Outgoing(owner) => {
                        self.view.edge(&lease, id).is_some_and(|e| e.from == *owner)
                    }
                };
            }
            if visible {
                rows.mutate(|r| r.push(id)).unwrap();
            }
        }
        drop(lease);
        Ok(Chunk {
            rows,
            examined,
            done: self.done,
        })
    }
}

fn payload_error(error: super::payload::PayloadError) -> Outcome {
    match error {
        super::payload::PayloadError::Resource {
            resource,
            requested,
            limit: cap,
            retryable,
        } => limit(resource, requested, cap, retryable),
        super::payload::PayloadError::Closed => Outcome::Refused("closed payload scratch"),
        super::payload::PayloadError::PersistedRoundTrip => {
            Outcome::Refused("persisted payload depth")
        }
        super::payload::PayloadError::Wire => Outcome::Refused("payload wire bound"),
    }
}

fn property_wire_bytes(value: &PropValue) -> Result<usize, Outcome> {
    let mut count = super::payload::CountSink(0);
    super::payload::write_value(value, &mut count).map_err(payload_error)?;
    Ok(count.0)
}
fn node_wire_bytes(node: &NodeRecord) -> Result<usize, Outcome> {
    let mut count = super::payload::CountSink(0);
    super::payload::write_properties_checked(&node.props, &mut count).map_err(payload_error)?;
    (37usize + node.label_ids.len() * 4)
        .checked_add(node.key.len())
        .and_then(|n| n.checked_add(count.0))
        .ok_or(Outcome::Refused("chunk wire overflow"))
}
fn equality_keys(node: &NodeRecord) -> Result<Option<u64>, Outcome> {
    node.props
        .get("value")
        .map(|v| {
            let bytes = property_wire_bytes(v)?;
            if bytes > 16 * 1024 {
                return Err(limit("equality_key_bytes", bytes, 16 * 1024, false));
            }
            super::payload::semantic_hash(v).map_err(payload_error)
        })
        .transpose()
}

struct WireLimit {
    count: usize,
    maximum: usize,
    exceeded: bool,
}
impl std::io::Write for WireLimit {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.count = self.count.saturating_add(bytes.len());
        if self.count > self.maximum {
            self.exceeded = true;
            return Err(std::io::Error::other("request frame limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn wal_op_wire_len(op: &WalOp) -> Result<usize, Outcome> {
    wal_op_wire_len_bounded(op, 0, usize::MAX)
}
fn wal_op_wire_len_bounded(op: &WalOp, prefix: usize, maximum: usize) -> Result<usize, Outcome> {
    let (fixed, props) = match op {
        WalOp::UpsertNode(n) => {
            if n.key.len() > u16::MAX as usize {
                return Err(Outcome::Refused("node key wire length"));
            }
            (
                37usize
                    .checked_add(n.label_ids.len() * 4)
                    .and_then(|nbytes| nbytes.checked_add(n.key.len()))
                    .ok_or(Outcome::Refused("node wire overflow"))?,
                Some(&n.props),
            )
        }
        WalOp::UpsertEdge(e) => (69, Some(&e.props)),
        WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => (17, None),
        WalOp::EnsureNodeLabel { label, .. } | WalOp::EnsureEdgeLabel { label, .. } => {
            if label.len() > u16::MAX as usize {
                return Err(Outcome::Refused("token wire length"));
            }
            (7 + label.len(), None)
        }
        _ => return Err(Outcome::Refused("unsupported private operation")),
    };
    let start = prefix
        .checked_add(fixed)
        .ok_or(Outcome::Refused("operation wire overflow"))?;
    if start > maximum {
        return Err(limit("max_batch_bytes", start, maximum, false));
    }
    let mut count = WireLimit {
        count: start,
        maximum,
        exceeded: false,
    };
    if let Some(props) = props {
        if let Err(error) = super::payload::write_properties_checked(props, &mut count) {
            return Err(if count.exceeded {
                limit("max_batch_bytes", count.count, maximum, false)
            } else {
                payload_error(error)
            });
        }
    }
    Ok(count.count - prefix)
}

fn check_input(options: &crate::ConcurrencyOptions, input: &[WalOp]) -> Result<(), Outcome> {
    if input.len() > options.max_batch_operations {
        return Err(limit(
            "max_batch_operations",
            input.len(),
            options.max_batch_operations,
            false,
        ));
    }
    let mut count = if input.len() > 1 { 58usize } else { 0 };
    if count > options.max_batch_bytes {
        return Err(limit(
            "max_batch_bytes",
            count,
            options.max_batch_bytes,
            false,
        ));
    }
    for op in input {
        if !matches!(
            op,
            WalOp::UpsertNode(_)
                | WalOp::UpsertEdge(_)
                | WalOp::DeleteNode { .. }
                | WalOp::DeleteEdge { .. }
                | WalOp::EnsureNodeLabel { .. }
                | WalOp::EnsureEdgeLabel { .. }
        ) {
            return Err(Outcome::Refused("unsupported private operation"));
        }
        if matches!(op, WalOp::UpsertNode(n) if n.dense_vector.is_some() || n.sparse_vector.is_some())
        {
            return Err(Outcome::Refused("vector indexes outside private slice"));
        }
        // A bounded count sink stops before cloning or traversing an over-limit suffix.
        count = count
            .checked_add(16)
            .ok_or(Outcome::Refused("batch wire overflow"))?;
        count = count
            .checked_add(wal_op_wire_len_bounded(op, count, options.max_batch_bytes)?)
            .ok_or(Outcome::Refused("batch wire overflow"))?;
    }

    Ok(())
}

fn empty_node(node: &NodeRecord) -> NodeRecord {
    NodeRecord {
        id: node.id,
        label_ids: node.label_ids,
        key: String::new(),
        props: BTreeMap::new(),
        created_at: node.created_at,
        updated_at: node.updated_at,
        weight: node.weight,
        dense_vector: None,
        sparse_vector: None,
        last_write_seq: node.last_write_seq,
    }
}
fn empty_edge(edge: &EdgeRecord) -> EdgeRecord {
    EdgeRecord {
        id: edge.id,
        from: edge.from,
        to: edge.to,
        label_id: edge.label_id,
        props: BTreeMap::new(),
        created_at: edge.created_at,
        updated_at: edge.updated_at,
        weight: edge.weight,
        valid_from: edge.valid_from,
        valid_to: edge.valid_to,
        last_write_seq: edge.last_write_seq,
    }
}
fn clone_props(
    output: &mut BTreeMap<String, PropValue>,
    input: &BTreeMap<String, PropValue>,
    state: &mut super::payload::CloneState,
) -> Result<(), Outcome> {
    for (key, value) in input {
        let target = output.entry(key.clone()).or_insert(PropValue::Null);
        state.clone_value(value).map_err(payload_error)?;
        *target = std::mem::replace(&mut *state.root, PropValue::Null);
    }
    Ok(())
}

type PreparedToken = ((u8, u32, Reverse<u64>), String);
type PreparedTokenName = ((u8, String, Reverse<u64>), u32);

#[derive(Default)]
struct Prepared {
    // Retain the preparation snapshot while local before-images are in use.
    frontier: Option<Snapshot>,
    input_reversed: bool,
    clone: super::payload::CloneState,
    input: Vec<WalOp>,
    ops: Vec<(u64, WalOp)>,
    frames: Vec<u8>,
    nodes: Vec<(PrimaryKey, Option<NodeRecord>)>,
    edges: Vec<(PrimaryKey, Option<EdgeRecord>)>,
    equality: BTreeMap<MembershipKey, bool>,
    outgoing: BTreeMap<OutKey, Option<AdjEntry>>,
    degrees: BTreeMap<PrimaryKey, u64>,
    tokens: Vec<PreparedToken>,
    token_names: Vec<PreparedTokenName>,
    declared_names: BTreeMap<(u8, String), u32>,
    publication: Option<Arc<Published>>,
    prior_nodes: BTreeMap<u64, usize>,
    prior_edges: BTreeMap<u64, usize>,
    degree_scratch: BTreeMap<u64, u64>,
    declared_tokens: BTreeMap<(u8, u32), String>,
    added_token_wire: usize,
    added_token_count: usize,
}

#[derive(Default)]
struct CanonicalInput {
    operations: Owned<Vec<WalOp>>,
}

impl Drop for Prepared {
    fn drop(&mut self) {
        self.input.drain_payload();
        self.ops.drain_payload();
        self.nodes.drain_payload();
        self.edges.drain_payload();
        drop(std::mem::take(&mut self.input));
        self.tokens.clear();
        self.token_names.clear();
        // Any original/copied backing still owned here was destroyed above.
    }
}

#[derive(Default)]
struct Core {
    path: std::path::PathBuf,
    wal: Option<WalSyncState>,
    mode: Option<WalSyncMode>,
    parents: Option<OwnerPermit<Producers>>,
    sources: Option<OwnerPermit<Sources>>,
    generation_bytes: usize,
    generation_tuples: usize,
    closed_buffers: Option<(Vec<u8>, Vec<u8>)>,
    catalog_revision: u64,
    global_token_wire: usize,
    global_token_count: usize,
    manifest_base_wire: usize,
    next_generation: u64,
    captured_generation: Option<(u64, u64)>,
    maintenance: Arc<Maintenance>,
}
struct Request {
    canonical: Option<Local<CanonicalInput>>,
    prepared: Option<Local<Prepared>>,
    transition: Option<super::durable_transition::LocalTicket>,
    deadline: Deadline,
    position: QueuePosition<QUEUE_LIMIT>,
    parked: bool,
    operation: OperationPermit,
    canceled: CancelRef,
    reply: mpsc::SyncSender<Result<Snapshot, Outcome>>,
}

struct AdoptionReservation {
    identity: (u64, u64),
    operation: OperationPermit,
    reply: mpsc::SyncSender<Result<(), Outcome>>,
}
struct AdmittedAdoption {
    custody: AdoptionReservation,
    job: CapturedJob,
    ticket: Option<super::durable_transition::LocalTicket>,
    rejection: Option<Outcome>,
}
enum AdoptionState {
    Settling,
    Reserved(AdoptionReservation),
    Ready(Box<AdmittedAdoption>),
}
#[derive(Default)]
struct AdoptionControl {
    slot: Option<AdoptionState>,
    revision: u64,
}
impl std::ops::Deref for AdoptionControl {
    type Target = Option<AdoptionState>;
    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}
impl std::ops::DerefMut for AdoptionControl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot
    }
}
#[derive(Default)]
struct Maintenance {
    active: AtomicBool,
}
struct HeavyJob {
    lane: Arc<Maintenance>,
}
impl Drop for HeavyJob {
    fn drop(&mut self) {
        self.lane.active.store(false, Ordering::Release);
    }
}
impl Maintenance {
    fn acquire(self: &Arc<Self>) -> Result<HeavyJob, Outcome> {
        self.active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| limit("heavy_maintenance", 1, 1, true))?;
        Ok(HeavyJob { lane: self.clone() })
    }
}
pub(super) struct CapturedJob {
    domain: DomainHandle,
    generation: Managed<Generation>,
    revision: u64,
    _heavy: HeavyJob,
}
impl CapturedJob {
    fn identity(&self) -> (u64, u64) {
        (self.generation.get().id, self.revision)
    }
}
pub(super) struct AdoptionDelivery {
    runtime: RuntimeRef,
    armed: bool,
    job: Option<CapturedJob>,
}
pub(super) struct AdoptionPending {
    receiver: mpsc::Receiver<Result<(), Outcome>>,
}
pub(super) struct AdoptionRefusal {
    pub(super) outcome: Outcome,
    pub(super) job: CapturedJob,
}
impl std::fmt::Debug for AdoptionRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.outcome.fmt(formatter)
    }
}
impl AdoptionPending {
    pub(super) fn wait(self) -> Result<(), Outcome> {
        self.receiver.recv().unwrap_or(Err(Outcome::Commit {
            state: CommitState::Indeterminate,
            message: "adoption reply lost".into(),
        }))
    }
}
impl AdoptionDelivery {
    pub(super) fn job_mut(&mut self) -> &mut CapturedJob {
        self.job.as_mut().expect("accepted adoption job")
    }

    pub(super) fn complete(
        mut self,
        ticket: super::durable_transition::LocalTicket,
    ) -> Result<(), Outcome> {
        let domain = self.runtime.domain()?;
        if !ticket.belongs_to(&domain) {
            return Err(Outcome::Refused("foreign adoption ticket"));
        }
        let mut slot = self.runtime.adoption.lock().unwrap();
        let Some(AdoptionState::Reserved(custody)) = slot.take() else {
            fail_stop("adoption delivery lost reserved custody");
        };
        slot.slot = Some(AdoptionState::Ready(Box::new(AdmittedAdoption {
            custody,
            job: self.job.take().expect("accepted adoption job"),
            ticket: Some(ticket),
            rejection: None,
        })));
        self.armed = false;
        drop(slot);
        self.runtime.pressure.notify();
        self.runtime.lifecycle.wake();
        Ok(())
    }
}
impl Drop for AdoptionDelivery {
    fn drop(&mut self) {
        if self.armed {
            let mut control = self.runtime.adoption.lock().unwrap();
            let Some(AdoptionState::Reserved(custody)) = control.slot.take() else {
                fail_stop("adoption cancellation lost reserved custody")
            };
            // Accepted capture settles on the coordinator, including cancellation.
            control.slot = Some(AdoptionState::Ready(Box::new(AdmittedAdoption {
                custody,
                job: self.job.take().unwrap(),
                ticket: None,
                rejection: None,
            })));
            drop(control);
            self.runtime.pressure.notify();
            self.runtime.lifecycle.wake();
        }
    }
}

enum RequestProgress {
    Complete(Result<Snapshot, Outcome>),
    Park(super::concurrency_lifecycle::ResourceLimit),
}
struct Runtime {
    lifecycle: LifecycleRef,
    domain: Mutex<Option<DomainHandle>>,
    stats: ReclamationStatsHandle,
    admission: ParticipantHandle,
    published: ArcSwap<Published>,
    sentinel: Arc<Published>,
    fence: CommitFence,
    queue: Mutex<VecDeque<Request>>,
    // Configured open initializes these fixed controls before exposing Runtime.
    pressure: Arc<Pressure<QUEUE_LIMIT>>,
    options: crate::ConcurrencyOptions,
    adoption: Mutex<AdoptionControl>,
    completion: Mutex<Option<super::completion::DomainRegistration>>,
    directory_session: Mutex<Option<super::completion::DirectorySession>>,
    closes: AtomicUsize,
    close_error: Mutex<Option<String>>,
    fast_close: AtomicU8,
    #[cfg(test)]
    hooks: Mutex<Hooks>,
}
/// The final strong handle destroys this fixed header. No Weak or raw Arc escapes.
struct RuntimeRef(Option<Arc<Runtime>>);
impl RuntimeRef {
    fn new(runtime: Runtime) -> Self {
        Self(Some(Arc::new(runtime)))
    }
}
impl Clone for RuntimeRef {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().unwrap().clone()))
    }
}
impl std::ops::Deref for RuntimeRef {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        self.0.as_ref().unwrap()
    }
}
impl Drop for RuntimeRef {
    fn drop(&mut self) {
        drop(Arc::into_inner(self.0.take().unwrap()));
    }
}
fn engine_outcome(error: crate::EngineError) -> Outcome {
    match error {
        crate::EngineError::ResourceLimit {
            resource,
            requested,
            limit,
            retryable,
        } => limit_outcome(resource, requested, limit, retryable),
        crate::EngineError::CommitOutcome { state, message } => Outcome::Commit { state, message },
        _ => Outcome::Refused("configured operation failed"),
    }
}
fn limit_outcome(
    resource: &'static str,
    requested: u64,
    capacity: u64,
    retryable: bool,
) -> Outcome {
    limit(resource, requested as usize, capacity as usize, retryable)
}
fn start_outcome(error: super::completion::StartError) -> Outcome {
    match error {
        super::completion::StartError::Inventory(error) => engine_outcome(error),
        super::completion::StartError::ConfigurationConflict => {
            Outcome::Refused("concurrency configuration conflict")
        }
        super::completion::StartError::ActiveSession => {
            Outcome::Refused("directory already active")
        }
        _ => Outcome::Refused("directory startup or native completion pending"),
    }
}

pub(super) struct Slice {
    runtime: RuntimeRef,
    owner: RuntimeOwnerPermit,
}
pub(super) struct Transaction {
    runtime: RuntimeRef,
    owner: Option<RuntimeOwnerPermit>,
    snapshot: Option<Snapshot>,
}
struct CancelState {
    state: AtomicU8,
}
/// Pending callers may outlive the closed Runtime; the last strong handle cleans up.
struct CancelRef(Option<Arc<CancelState>>);
impl CancelRef {
    fn new() -> Self {
        Self(Some(Arc::new(CancelState {
            state: AtomicU8::new(0),
        })))
    }
}
impl Clone for CancelRef {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live cancellation").clone()))
    }
}
impl std::ops::Deref for CancelRef {
    type Target = AtomicU8;
    fn deref(&self) -> &AtomicU8 {
        &self.0.as_ref().expect("live cancellation").state
    }
}
impl Drop for CancelRef {
    fn drop(&mut self) {
        let _ = Arc::into_inner(self.0.take().unwrap());
    }
}
pub(super) struct Pending {
    receiver: mpsc::Receiver<Result<Snapshot, Outcome>>,
    canceled: CancelRef,
}
impl Pending {
    pub(super) fn wait(self) -> Result<Snapshot, Outcome> {
        self.receiver.recv().unwrap_or(Err(Outcome::Commit {
            state: CommitState::Indeterminate,
            message: "reply lost".into(),
        }))
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        let _ = self
            .canceled
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
    }
}

impl Slice {
    pub(super) fn open(path: &Path, mode: WalSyncMode) -> Result<Self, Outcome> {
        Self::open_configured(path, mode, crate::ConcurrencyOptions::default())
    }
    pub(super) fn open_configured(
        path: &Path,
        mode: WalSyncMode,
        options: crate::ConcurrencyOptions,
    ) -> Result<Self, Outcome> {
        options.validate().map_err(engine_outcome)?;
        let admission_value = ParticipantAdmission::new(256)?;
        // This adapter still exercises fresh real-WAL data only; read-only
        // historical selection/replay is supplied by the reusable preflight API.
        // Refusal happens before any database file is created or normalized.
        if path.join(crate::manifest::MANIFEST_CURRENT).exists()
            || crate::wal::wal_generation_path(path, 0).exists()
        {
            return Err(Outcome::Refused("adapter requires a fresh directory"));
        }
        let pin = super::directory_identity::DirectoryPin::open(path)
            .map_err(|_| Outcome::Refused("directory identity unavailable"))?;
        if super::completion::DirectorySession::attach(pin, &options)
            .map_err(start_outcome)?
            .is_some()
        {
            return Err(Outcome::Refused(
                "adapter cannot replay an existing session",
            ));
        }
        let configured = ReclaimDomain::configured(
            &options,
            Some(
                super::directory_identity::DirectoryPin::open(path)
                    .map_err(|_| Outcome::Refused("directory identity unavailable"))?,
            ),
        )
        .map_err(engine_outcome)?;
        let super::reclamation::ConfiguredDomain {
            domain,
            registration,
        } = configured;
        let admission = ParticipantHandle::new(admission_value, domain.identity());
        let lifecycle = super::concurrency_lifecycle::Lifecycle::allocate();
        let mut core = domain
            .build::<Core>(ResourceClass::Storage)
            .map_err(Outcome::from)?;
        let mut parents = domain
            .build::<Producers>(ResourceClass::Sources)
            .map_err(Outcome::from)?;
        let mut sources = domain
            .build::<Sources>(ResourceClass::Sources)
            .map_err(Outcome::from)?;
        let generation = domain
            .build::<Generation>(ResourceClass::Generation)
            .map_err(Outcome::from)?
            .into_owner()
            .unwrap();
        sources
            .mutate(|s| {
                s.reserve_exact(SOURCE_LIMIT);
                s.push(generation.read());
            })
            .unwrap();
        parents
            .mutate(|s| {
                s.generations.reserve_exact(SOURCE_LIMIT);
                s.generations.push(generation);
            })
            .unwrap();
        core.mutate(|c| {
            c.path = path.to_path_buf();
            c.parents = Some(parents.into_owner().unwrap());
            c.sources = Some(sources.into_owner().unwrap());
            c.mode = Some(mode);
            c.next_generation = 1;
            c.manifest_base_wire =
                super::durable_transition::metadata_size(&crate::manifest::default_manifest())
                    .unwrap();
        })
        .unwrap();
        let initial = Arc::new(Published {
            state: Some(PublishedState {
                sources: Some(core.mutate(|c| c.sources.as_ref().unwrap().read()).unwrap()),
                ..PublishedState::default()
            }),
        });
        let directory_session = registration
            .bind_directory(domain.clone(), &options)
            .map_err(start_outcome)?;
        // Every substantial startup owner and mandatory future worker header is
        // now admitted and registered. Durable fresh-directory creation follows.
        crate::manifest::write_manifest(path, &crate::manifest::default_manifest()).map_err(
            |_| Outcome::Commit {
                state: CommitState::Indeterminate,
                message: "initial manifest uncertainty; files preserved".into(),
            },
        )?;
        let wal =
            WalWriter::open_generation(path, 0).map_err(|_| Outcome::Refused("WAL open failed"))?;
        core.mutate(|c| {
            c.wal = Some(WalSyncState {
                wal_writer: wal,
                buffered_bytes: 0,
                shutdown: false,
                sync_error_count: 0,
                poisoned: None,
            })
        })
        .unwrap();
        let owner = lifecycle.initial_owner();
        let stats = domain.stats();
        stats.set_generations(1, 0);
        stats
            .bind_participants(&admission)
            .map_err(engine_outcome)?;
        let runtime = RuntimeRef::new(Runtime {
            lifecycle,
            stats,
            domain: Mutex::new(Some(domain)),
            admission,
            published: ArcSwap::from(initial),
            sentinel: Arc::new(Published { state: None }),
            fence: CommitFence::new(),
            queue: Mutex::new(VecDeque::with_capacity(QUEUE_LIMIT)),
            pressure: Arc::new(Pressure::new()),
            options,
            adoption: Mutex::new(AdoptionControl::default()),
            completion: Mutex::new(Some(registration)),
            directory_session: Mutex::new(Some(directory_session)),
            closes: AtomicUsize::new(0),
            close_error: Mutex::new(None),
            fast_close: AtomicU8::new(0),
            #[cfg(test)]
            hooks: Mutex::new(Hooks::default()),
        });
        let service = runtime.clone();
        runtime
            .completion
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .spawn(
                super::completion::Role::Coordinator,
                "overgraph-slice-coordinator",
                move |_wake| coordinator(&service, core),
            )
            .map_err(start_outcome)?;
        Ok(Self { runtime, owner })
    }
    pub(super) fn snapshot(&self) -> Result<Snapshot, Outcome> {
        let _call = self.runtime.lifecycle.admit()?;
        Ok(self.runtime.snapshot())
    }
    pub(super) fn begin(&self) -> Result<Transaction, Outcome> {
        Ok(Transaction {
            runtime: self.runtime.clone(),
            owner: Some(self.owner.clone()),
            snapshot: Some(self.snapshot()?),
        })
    }
    pub(super) fn submit(&self, input: Vec<WalOp>) -> Result<Pending, Outcome> {
        submit(&self.runtime, input)
    }
    // Private durable-job protocol. A1 proves real ordering/failure retention;
    // complete source/catalog adoption remains the a3/b/c integration owners.
    pub(super) fn submit_transition(
        &self,
        mut ticket: super::durable_transition::LocalTicket,
    ) -> Result<Pending, Outcome> {
        let started = ticket
            .mutate(|ticket| ticket.preparation_started)
            .unwrap()
            .unwrap_or_else(Instant::now);
        let deadline = Deadline::from_started(started, self.runtime.options.admission_timeout_ms)?;
        self.submit_transition_with_deadline(ticket, deadline)
    }
    pub(super) fn submit_transition_with_deadline(
        &self,
        ticket: super::durable_transition::LocalTicket,
        deadline: Deadline,
    ) -> Result<Pending, Outcome> {
        let domain = self.runtime.domain()?;
        if !ticket.belongs_to(&domain) {
            return Err(Outcome::Refused("foreign transition ticket"));
        }
        enqueue(&self.runtime, None, Some(ticket), deadline)
    }
    pub(super) fn reserve_adoption(
        &self,
        job: CapturedJob,
    ) -> Result<(AdoptionDelivery, AdoptionPending), AdoptionRefusal> {
        let admission = (|| {
            let domain = self.runtime.domain()?;
            if !job.domain.same_domain(&domain) {
                return Err(Outcome::Refused("foreign admitted adoption custody"));
            }
            let mut slot = self.runtime.adoption.lock().unwrap();
            if slot.is_some() {
                return Err(Outcome::Refused("lifecycle adoption position occupied"));
            }
            let identity = job.identity();
            if identity.1 != slot.revision {
                return Err(Outcome::Refused("stale captured catalog revision"));
            }
            let operation = self.runtime.lifecycle.admit()?;
            let (reply, receiver) = mpsc::sync_channel(1);
            slot.slot = Some(AdoptionState::Reserved(AdoptionReservation {
                identity,
                operation,
                reply,
            }));
            Ok(receiver)
        })();
        let receiver = match admission {
            Ok(receiver) => receiver,
            Err(outcome) => return Err(AdoptionRefusal { outcome, job }),
        };
        Ok((
            AdoptionDelivery {
                runtime: self.runtime.clone(),
                armed: true,
                job: Some(job),
            },
            AdoptionPending { receiver },
        ))
    }
    pub(super) fn close(&self) -> Result<(), Outcome> {
        if let Some(session) = self.runtime.directory_session.lock().unwrap().as_mut() {
            session.begin_shutdown();
        }
        self.runtime.lifecycle.request_close();
        self.runtime.pressure.close();
        self.runtime.lifecycle.wait_closed();
        if self.runtime.close_error.lock().unwrap().is_some() {
            Err(Outcome::Commit {
                state: CommitState::Indeterminate,
                message: "close WAL sync failed; recovery state preserved".into(),
            })
        } else {
            Ok(())
        }
    }
    pub(super) fn stats(&self) -> ReclamationStatsHandle {
        self.runtime.stats.clone()
    }
    pub(super) fn close_fast(&self) -> Result<(), Outcome> {
        self.runtime.fast_close.store(1, Ordering::Release);
        self.close()
    }
}
impl Clone for Slice {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            owner: self.owner.clone(),
        }
    }
}
impl Transaction {
    pub(super) fn commit(&mut self, input: Vec<WalOp>) -> Result<Pending, Outcome> {
        let input = Owned::new(input);
        if self.owner.is_none() {
            return Err(Outcome::Refused("closed transaction"));
        }
        let result = submit_guarded(&self.runtime, input);
        if result.is_ok() {
            self.snapshot.take();
            self.owner.take();
        }
        result
    }
    pub(super) fn rollback(&mut self) {
        self.snapshot.take();
        self.owner.take();
    }
}
impl Runtime {
    fn domain(&self) -> Result<DomainHandle, Outcome> {
        self.domain
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or(Outcome::Refused("closed domain"))
    }
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            published: self.published.load_full(),
            admission: self.admission.clone(),
        }
    }
}
fn submit(runtime: &RuntimeRef, input: Vec<WalOp>) -> Result<Pending, Outcome> {
    submit_guarded(runtime, Owned::new(input))
}
fn submit_guarded(runtime: &RuntimeRef, input: Owned<Vec<WalOp>>) -> Result<Pending, Outcome> {
    let deadline = Deadline::new(runtime.options.admission_timeout_ms)?;
    // Allocate a ticket before traversal; hold it through preparation and retries.
    let position = {
        let _queue = runtime.queue.lock().unwrap();
        runtime.pressure.try_enter()?
    };
    let operation = runtime.lifecycle.admit()?;
    #[cfg(test)]
    hook(runtime, Boundary::InputAdmission)?;
    check_input(&runtime.options, &input)?;
    enqueue_admitted(
        runtime,
        Some(Local(CanonicalInput { operations: input })),
        None,
        deadline,
        position,
        operation,
    )
}
fn enqueue(
    runtime: &RuntimeRef,
    canonical: Option<Local<CanonicalInput>>,
    transition: Option<super::durable_transition::LocalTicket>,
    deadline: Deadline,
) -> Result<Pending, Outcome> {
    let position = {
        let _queue = runtime.queue.lock().unwrap();
        runtime.pressure.try_enter()?
    };
    let operation = runtime.lifecycle.admit()?;
    enqueue_admitted(
        runtime, canonical, transition, deadline, position, operation,
    )
}
fn enqueue_admitted(
    runtime: &RuntimeRef,
    canonical: Option<Local<CanonicalInput>>,
    transition: Option<super::durable_transition::LocalTicket>,
    deadline: Deadline,
    position: QueuePosition<QUEUE_LIMIT>,
    operation: OperationPermit,
) -> Result<Pending, Outcome> {
    if runtime.lifecycle.close_requested() {
        return Err(Outcome::Refused("closed"));
    }
    let (reply, receiver) = mpsc::sync_channel(1);
    let canceled = CancelRef::new();
    let request = Request {
        canonical,
        prepared: None,
        transition,
        deadline,
        position,
        parked: false,
        operation,
        canceled: canceled.clone(),
        reply,
    };
    let mut queue = runtime.queue.lock().unwrap();
    // Input preparation may finish out of order. The ticket order remains FIFO.
    let index = queue
        .iter()
        .position(|r| r.position.ticket() > request.position.ticket())
        .unwrap_or(queue.len());
    queue.insert(index, request);
    drop(queue);
    runtime.lifecycle.wake();
    runtime.pressure.notify();
    Ok(Pending { receiver, canceled })
}

fn restore_input(request: &mut Request) {
    let Some(mut attempt) = request.prepared.take() else {
        return;
    };
    let canonical = request.canonical.as_mut().expect("ordinary input custody");
    attempt
        .mutate(|p| {
            drop(p.frontier.take());
            let prefix = p.ops.len();
            // Remaining input is reversed by preparation; recover the original order
            // with its existing capacity, without another record/payload copy.
            if p.input_reversed {
                p.input.reverse();
            }
            for (_, op) in p.ops.drain(..) {
                p.input.push(op);
            }
            p.input.rotate_right(prefix);
            let input = &mut canonical.0.operations;
            if !input.is_empty() {
                fail_stop("canonical input recovery overwrote live input");
            }
            **input = std::mem::take(&mut p.input);
        })
        .unwrap();
    drop(attempt); // Ordinary local cleanup drains copied/partial values iteratively.
}

fn build_attempt(_runtime: &Runtime, request: &mut Request) -> Result<(), Outcome> {
    let c = &mut request.canonical.as_mut().unwrap().0;
    let mut p = Prepared::default();
    p.input = std::mem::take(&mut *c.operations);
    request.prepared = Some(Local(p));
    Ok(())
}
fn token_wire_upper(name: &str) -> Result<usize, Outcome> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(b.len())
                .ok_or_else(|| std::io::Error::other("token size overflow"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, name)
        .map_err(|_| Outcome::Refused("token metadata encoding"))?;
    count
        .0
        .checked_add(24)
        .ok_or(Outcome::Refused("token metadata overflow"))
}
fn prepare(
    runtime: &Runtime,
    core: &mut Core,
    p: &mut Prepared,
    lease: &Session<'_>,
) -> Result<(), Outcome> {
    let old = runtime.snapshot();
    p.frontier = Some(old.clone());
    let mut seq = old.seq();
    // Assign final sequence only after all earlier queued commits. Validate all
    // input before moving any record out of its local guarded input.
    for op in &p.input {
        match op {
            WalOp::UpsertNode(n) => {
                if n.id == 0 || n.id == u64::MAX {
                    return Err(Outcome::Refused("invalid/exhausted node identifier"));
                }
            }
            WalOp::UpsertEdge(e) => {
                if e.id == 0 || e.id == u64::MAX || e.from == 0 || e.to == 0 {
                    return Err(Outcome::Refused("invalid/exhausted edge identifier"));
                }
            }
            WalOp::DeleteNode { id, .. } | WalOp::DeleteEdge { id, .. } => {
                if *id == 0 || *id == u64::MAX {
                    return Err(Outcome::Refused("invalid/exhausted identifier"));
                }
            }
            WalOp::EnsureNodeLabel { label, label_id }
            | WalOp::EnsureEdgeLabel { label, label_id } => {
                validate_label_token_name(label).map_err(|_| Outcome::Refused("invalid token"))?;
                if *label_id == 0 || *label_id == u32::MAX || label.len() > 256 {
                    return Err(Outcome::Refused("invalid token"));
                }
                let namespace = u8::from(matches!(op, WalOp::EnsureEdgeLabel { .. }));
                if p.declared_tokens
                    .get(&(namespace, *label_id))
                    .map(String::as_str)
                    .or_else(|| old.token_in(lease, namespace, *label_id))
                    .is_some_and(|name| name != label)
                {
                    return Err(Outcome::Refused("token identity conflict"));
                }
                if p.declared_names
                    .get(&(namespace, label.clone()))
                    .copied()
                    .or_else(|| old.token_id_in(lease, namespace, label))
                    .is_some_and(|id| id != *label_id)
                {
                    return Err(Outcome::Refused("token name conflict"));
                }
                if !p.declared_tokens.contains_key(&(namespace, *label_id))
                    && old.token_in(lease, namespace, *label_id).is_none()
                {
                    p.added_token_wire = p
                        .added_token_wire
                        .checked_add(token_wire_upper(label)?)
                        .ok_or(Outcome::Refused("token metadata overflow"))?;
                    p.added_token_count = p
                        .added_token_count
                        .checked_add(1)
                        .ok_or(Outcome::Refused("token count overflow"))?;
                }
                p.declared_tokens
                    .insert((namespace, *label_id), label.clone());
                p.declared_names
                    .insert((namespace, label.clone()), *label_id);
            }
            _ => {}
        }
    }
    for op in &p.input {
        let known = |namespace: u8, id: u32| {
            p.declared_tokens.contains_key(&(namespace, id))
                || old.token_in(lease, namespace, id).is_some()
        };
        match op {
            WalOp::UpsertNode(n) if n.label_ids.as_slice().iter().any(|id| !known(0, *id)) => {
                return Err(Outcome::Refused("unknown node label token"))
            }
            WalOp::UpsertEdge(e) if !known(1, e.label_id) => {
                return Err(Outcome::Refused("unknown edge label token"))
            }
            _ => {}
        }
    }
    let token_wire = core
        .global_token_wire
        .checked_add(p.added_token_wire)
        .ok_or(Outcome::Refused("token metadata overflow"))?;
    let baseline = crate::manifest::default_manifest();
    // The current complete catalog's serialized size was checked at its private
    // transition; cumulative exact new token fragments are cached across freezes.
    let baseline_wire = super::durable_transition::metadata_size(&baseline)?;
    let additional = token_wire
        .checked_add(core.manifest_base_wire.saturating_sub(baseline_wire))
        .ok_or(Outcome::Refused("catalog metadata overflow"))?;
    super::durable_transition::check_future_manifest(baseline_wire, additional, SOURCE_LIMIT)?;
    seq.checked_add(p.input.len() as u64)
        .ok_or(Outcome::Refused("sequence exhausted"))?;
    p.ops.reserve_exact(p.input.len());
    let mut nodes = 0;
    let mut edges = 0;
    let mut tokens = 0;
    for op in &p.input {
        match op {
            WalOp::UpsertNode(_) | WalOp::DeleteNode { .. } => nodes += 1,
            WalOp::UpsertEdge(_) | WalOp::DeleteEdge { .. } => edges += 1,
            WalOp::EnsureNodeLabel { .. } | WalOp::EnsureEdgeLabel { .. } => tokens += 1,
            _ => {}
        }
    }
    p.nodes.reserve_exact(nodes);
    p.edges.reserve_exact(edges);
    p.tokens.reserve_exact(tokens);
    p.token_names.reserve_exact(tokens);
    // reverse once, then pop keeps every not-yet-moved record guarded.
    p.input.reverse();
    p.input_reversed = true;
    while let Some(mut op) = p.input.pop() {
        seq += 1;
        match &mut op {
            WalOp::UpsertNode(n) => n.last_write_seq = seq,
            WalOp::UpsertEdge(e) => e.last_write_seq = seq,
            _ => {}
        }
        p.ops.push((seq, op));
    }
    let mut max_node = old.state().max_node;
    let mut max_edge = old.state().max_edge;
    for (index, (seq, op)) in p.ops.iter().enumerate() {
        match op {
            WalOp::UpsertNode(n) => {
                let previous = p
                    .prior_nodes
                    .get(&n.id)
                    .and_then(|i| p.nodes[*i].1.as_ref())
                    .or_else(|| {
                        (!p.prior_nodes.contains_key(&n.id))
                            .then(|| old.node(lease, n.id))
                            .flatten()
                    });
                if let Some(before) = previous {
                    if let Some(hash) = equality_keys(before)? {
                        for label in before.label_ids.as_slice() {
                            p.equality
                                .insert((*label, hash, n.id, Reverse(*seq)), false);
                        }
                    }
                }
                if let Some(hash) = equality_keys(n)? {
                    for label in n.label_ids.as_slice() {
                        p.equality.insert((*label, hash, n.id, Reverse(*seq)), true);
                    }
                }
                p.prior_nodes.insert(n.id, p.nodes.len());
                p.nodes.push(((n.id, Reverse(*seq)), Some(empty_node(n))));
                let target = p.nodes.last_mut().unwrap().1.as_mut().unwrap();
                target.key = n.key.clone();
                clone_props(&mut target.props, &n.props, &mut p.clone)?;
                max_node = max_node.max(n.id);
            }
            WalOp::DeleteNode { id, .. } => {
                let previous = p
                    .prior_nodes
                    .get(id)
                    .and_then(|i| p.nodes[*i].1.as_ref())
                    .or_else(|| {
                        (!p.prior_nodes.contains_key(id))
                            .then(|| old.node(lease, *id))
                            .flatten()
                    });
                if let Some(before) = previous {
                    if let Some(hash) = equality_keys(before)? {
                        for label in before.label_ids.as_slice() {
                            p.equality.insert((*label, hash, *id, Reverse(*seq)), false);
                        }
                    }
                }
                p.prior_nodes.insert(*id, p.nodes.len());
                p.nodes.push(((*id, Reverse(*seq)), None));
            }
            WalOp::UpsertEdge(e) => {
                let previous = p
                    .prior_edges
                    .get(&e.id)
                    .and_then(|i| p.edges[*i].1.as_ref())
                    .or_else(|| {
                        (!p.prior_edges.contains_key(&e.id))
                            .then(|| old.edge(lease, e.id))
                            .flatten()
                    });
                if let Some(before) = previous {
                    p.outgoing.insert((before.from, e.id, Reverse(*seq)), None);
                    let count = p
                        .degree_scratch
                        .entry(before.from)
                        .or_insert_with(|| old.degree_in(lease, before.from));
                    *count = count
                        .checked_sub(1)
                        .unwrap_or_else(|| fail_stop("degree underflow"));
                    p.degrees.insert((before.from, Reverse(*seq)), *count);
                }
                p.outgoing.insert(
                    (e.from, e.id, Reverse(*seq)),
                    Some(AdjEntry {
                        edge_id: e.id,
                        label_id: e.label_id,
                        neighbor_id: e.to,
                        weight: e.weight,
                        valid_from: e.valid_from,
                        valid_to: e.valid_to,
                    }),
                );
                let count = p
                    .degree_scratch
                    .entry(e.from)
                    .or_insert_with(|| old.degree_in(lease, e.from));
                *count = count
                    .checked_add(1)
                    .ok_or(Outcome::Refused("degree overflow"))?;
                p.degrees.insert((e.from, Reverse(*seq)), *count);
                p.prior_edges.insert(e.id, p.edges.len());
                p.edges.push(((e.id, Reverse(*seq)), Some(empty_edge(e))));
                let target = p.edges.last_mut().unwrap().1.as_mut().unwrap();
                clone_props(&mut target.props, &e.props, &mut p.clone)?;
                max_edge = max_edge.max(e.id);
            }
            WalOp::DeleteEdge { id, .. } => {
                let previous = p
                    .prior_edges
                    .get(id)
                    .and_then(|i| p.edges[*i].1.as_ref())
                    .or_else(|| {
                        (!p.prior_edges.contains_key(id))
                            .then(|| old.edge(lease, *id))
                            .flatten()
                    });
                if let Some(before) = previous {
                    p.outgoing.insert((before.from, *id, Reverse(*seq)), None);
                    let count = p
                        .degree_scratch
                        .entry(before.from)
                        .or_insert_with(|| old.degree_in(lease, before.from));
                    *count = count
                        .checked_sub(1)
                        .unwrap_or_else(|| fail_stop("degree underflow"));
                    p.degrees.insert((before.from, Reverse(*seq)), *count);
                }
                p.prior_edges.insert(*id, p.edges.len());
                p.edges.push(((*id, Reverse(*seq)), None));
            }
            WalOp::EnsureNodeLabel { label, label_id } => {
                p.tokens
                    .push(((0, *label_id, Reverse(*seq)), label.clone()));
                p.token_names
                    .push(((0, label.clone(), Reverse(*seq)), *label_id));
            }
            WalOp::EnsureEdgeLabel { label, label_id } => {
                p.tokens
                    .push(((1, *label_id, Reverse(*seq)), label.clone()));
                p.token_names
                    .push(((1, label.clone(), Reverse(*seq)), *label_id));
            }
            _ => {
                let _ = index;
                return Err(Outcome::Refused("unsupported private operation"));
            }
        }
    }
    if p.ops.len() > runtime.options.max_batch_operations {
        return Err(limit(
            "max_batch_operations",
            p.ops.len(),
            runtime.options.max_batch_operations,
            false,
        ));
    }
    core.wal
        .as_mut()
        .unwrap()
        .wal_writer
        .prepare_batch_bounded(&p.ops, &mut p.frames)
        .map_err(engine_outcome)?;
    if p.frames.len() > runtime.options.max_batch_bytes {
        return Err(limit(
            "max_batch_bytes",
            p.frames.len(),
            runtime.options.max_batch_bytes,
            false,
        ));
    }
    if core
        .generation_bytes
        .checked_add(p.frames.len())
        .is_none_or(|n| n > runtime.options.generation_wal_bytes)
        || core
            .generation_tuples
            .checked_add(p.ops.len())
            .is_none_or(|n| n > runtime.options.generation_operation_limit)
    {
        return Err(Outcome::Refused("generation rotation required"));
    }
    p.publication = Some(Arc::new(Published {
        state: Some(PublishedState {
            seq,
            max_node,
            max_edge,
            sources: Some(core.sources.as_ref().unwrap().read()),
        }),
    }));
    Ok(())
}

fn acquire_participant(admission: &ParticipantHandle) -> Result<Session<'_>, Outcome> {
    admission.try_acquire().map_err(|failure| {
        limit(
            "retained_participant_limit",
            failure.requested,
            failure.limit,
            failure.retryable,
        )
    })
}
fn participant(runtime: &Runtime) -> Result<Session<'_>, Outcome> {
    acquire_participant(&runtime.admission)
}

fn publish_catalog(
    runtime: &Runtime,
    core: &mut Core,
    ticket: &mut super::durable_transition::LocalTicket,
    control: &mut AdoptionControl,
    canceled: &AtomicU8,
) -> Result<(), Outcome> {
    let revision = control
        .revision
        .checked_add(1)
        .ok_or(Outcome::Refused("catalog revision exhausted"))?;
    ticket
        .mutate(|t| {
            super::durable_transition::check_future_manifest(
                t.metadata_bytes(),
                core.global_token_wire,
                SOURCE_LIMIT,
            )
        })
        .unwrap()?;
    if canceled
        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(Outcome::Refused("canceled before acceptance"));
    }
    let result = super::durable_transition::publish(&core.path, &runtime.fence, ticket);
    if !matches!(
        result,
        Err(Outcome::Refused(_)) | Err(Outcome::ResourceLimit(_))
    ) {
        core.manifest_base_wire = ticket.mutate(|t| t.metadata_bytes()).unwrap();
        core.catalog_revision = revision;
        control.revision = revision;
    }
    result
}
fn commit(
    runtime: &Runtime,
    core: &mut BuildPermit<Core>,
    request: &mut Request,
) -> Result<Snapshot, Outcome> {
    commit_inner(runtime, core, request)
}
fn commit_inner(
    runtime: &Runtime,
    core: &mut BuildPermit<Core>,
    request: &mut Request,
) -> Result<Snapshot, Outcome> {
    if runtime.fence.is_fenced() {
        return Err(Outcome::Refused("fenced"));
    }
    if request.canceled.load(Ordering::Acquire) == 2 {
        return Err(Outcome::Refused("canceled before acceptance"));
    }
    if let Some(ticket) = request.transition.as_mut() {
        let mut control = runtime.adoption.lock().unwrap();
        if control.slot.is_some() || core.mutate(|c| c.captured_generation.is_some()).unwrap() {
            return Err(limit("lifecycle_adoption", 1, 1, true));
        }
        if ticket.mutate(|t| t.retires(0)).unwrap() {
            return Err(Outcome::Refused("active WAL retirement requires rotation"));
        }
        if !ticket
            .mutate(|t| {
                t.captured
                    .as_ref()
                    .is_some_and(|s| s.seq() == runtime.snapshot().seq())
            })
            .unwrap()
        {
            return Err(Outcome::Refused("stale source frontier ticket"));
        }
        if (request.parked || request.deadline.expires() != request.deadline.started())
            && request.deadline.expired()
        {
            return Err(limit("lifecycle_adoption", 1, 1, true));
        }
        #[cfg(test)]
        if let Some(boundary) = runtime.hooks.lock().unwrap().persistence.take() {
            crate::persistence_fault::set(boundary, false);
        }
        core.mutate(|c| publish_catalog(runtime, c, ticket, &mut control, &request.canceled))
            .unwrap()?;
        #[cfg(test)]
        if let Err(error) = hook(runtime, Boundary::BeforePublication) {
            runtime.fence.establish();
            return Err(error);
        }
        return Ok(runtime.snapshot());
    }
    let restoring = runtime
        .adoption
        .lock()
        .unwrap()
        .slot
        .as_ref()
        .is_some_and(|slot| match slot {
            AdoptionState::Ready(ready) => ready.ticket.is_none(),
            AdoptionState::Settling => true,
            _ => false,
        });
    if restoring {
        return Err(limit("lifecycle_adoption", 1, 1, true));
    }
    build_attempt(runtime, request)?;
    // A short probe obtains borrowed old-row deltas. Release it before the final
    // install session and publication.
    let probe = participant(runtime)?;
    let preparation = core
        .mutate(|c| {
            request
                .prepared
                .as_mut()
                .unwrap()
                .mutate(|p| prepare(runtime, c, p, &probe))
                .unwrap()
        })
        .unwrap();
    drop(probe);
    preparation?;
    let debt = core
        .mutate(|c| {
            request
                .prepared
                .as_mut()
                .unwrap()
                .mutate(|p| {
                    if let Some(WalSyncMode::GroupCommit { hard_cap_bytes, .. }) = c.mode {
                        let pending = c
                            .wal
                            .as_ref()
                            .unwrap()
                            .buffered_bytes
                            .checked_add(p.frames.len());
                        if pending.is_none_or(|bytes| bytes > hard_cap_bytes) {
                            return Err(limit(
                                "WAL_debt_bytes",
                                pending.unwrap_or(usize::MAX),
                                hard_cap_bytes,
                                p.frames.len() <= hard_cap_bytes,
                            ));
                        }
                    }
                    Ok(())
                })
                .unwrap()
        })
        .unwrap();
    debt?;
    // Neither Core's nor Prepared's building mutex, nor the durable gate, is
    // held during this final nonblocking admission.
    let lease = participant(runtime)?;
    let gate = runtime.fence.gate()?;
    let generation_owner = core
        .mutate(|c| c.parents.as_ref().unwrap().get().generations[0].fork())
        .unwrap();
    let prepared = request.prepared.as_mut().unwrap();
    let ready = prepared
        .mutate(|p| {
            let current = runtime.snapshot();
            if !p
                .frontier
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(&old.published, &current.published))
            {
                return Err(Outcome::Refused("stale prepared frontier"));
            }
            if p.ops.is_empty() {
                return Ok(false);
            }
            if (request.parked || request.deadline.expires() != request.deadline.started())
                && request.deadline.expired()
            {
                return Err(limit(
                    "admission_timeout_ms",
                    1,
                    runtime.options.admission_timeout_ms as usize,
                    true,
                ));
            }
            if request
                .canceled
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(Outcome::Refused("canceled before acceptance"));
            }
            Ok(true)
        })
        .unwrap();
    let result = match ready {
        Err(error) => Err(error),
        Ok(false) => Ok(runtime.snapshot()),
        Ok(true) => {
            let durable = core
                .mutate(|c| {
                    prepared
                        .mutate(|p| {
                            let wal = c.wal.as_mut().unwrap();
                            #[cfg(test)]
                            if let Some(fault) = runtime.hooks.lock().unwrap().wal.take() {
                                wal.wal_writer.inject_fault(fault);
                            }
                            // The complete future output promise is already owned.
                            c.global_token_wire = c
                                .global_token_wire
                                .checked_add(p.added_token_wire)
                                .unwrap_or_else(|| fail_stop("accepted token metadata overflow"));
                            c.global_token_count = c
                                .global_token_count
                                .checked_add(p.added_token_count)
                                .unwrap_or_else(|| fail_stop("accepted token count overflow"));
                            // Acceptance starts here. Every subsequent error is uncertainty
                            // or a committed outcome; deadlines/cancellation cannot undo it.
                            wal.buffered_bytes = wal
                                .buffered_bytes
                                .checked_add(p.frames.len())
                                .unwrap_or_else(|| fail_stop("WAL debt overflow"));
                            wal.wal_writer.append_prepared(&p.frames).map_err(|error| {
                                Outcome::Commit {
                                    state: CommitState::Indeterminate,
                                    message: error.to_string(),
                                }
                            })?;
                            if matches!(c.mode, Some(WalSyncMode::Immediate)) {
                                sync_pending(wal).map_err(|error| Outcome::Commit {
                                    state: CommitState::Indeterminate,
                                    message: error.to_string(),
                                })?;
                            }
                            Ok(())
                        })
                        .unwrap()
                })
                .unwrap();
            match durable {
                Err(error) => Err(error),
                Ok(()) => core
                    .mutate(|c| {
                        prepared
                            .mutate(|p| {
                                let generation = generation_owner.get();
                                while let Some((key, value)) = p.nodes.pop() {
                                    generation.nodes.insert_unique(
                                        &lease,
                                        key,
                                        value.map(Owned::new),
                                    );
                                }
                                while let Some((key, value)) = p.edges.pop() {
                                    generation.edges.insert_unique(
                                        &lease,
                                        key,
                                        value.map(Owned::new),
                                    );
                                }
                                #[cfg(test)]
                                hook(runtime, Boundary::Primary)?;
                                while let Some((key, value)) = p.equality.pop_first() {
                                    generation.equality.insert_unique(&lease, key, value);
                                }
                                #[cfg(test)]
                                hook(runtime, Boundary::Derived)?;
                                while let Some((key, value)) = p.outgoing.pop_first() {
                                    generation.outgoing.insert_unique(&lease, key, value);
                                }
                                while let Some((key, value)) = p.degrees.pop_first() {
                                    generation.degrees.insert_unique(&lease, key, value);
                                }
                                while let Some((key, value)) = p.tokens.pop() {
                                    generation.tokens.insert_unique(&lease, key, value);
                                }
                                while let Some((key, value)) = p.token_names.pop() {
                                    generation.token_names.insert_unique(&lease, key, value);
                                }
                                #[cfg(test)]
                                hook(runtime, Boundary::BeforePublication)?;
                                c.generation_bytes = c
                                    .generation_bytes
                                    .checked_add(p.frames.len())
                                    .unwrap_or_else(|| {
                                        fail_stop("accepted generation byte overflow")
                                    });
                                c.generation_tuples =
                                    c.generation_tuples.checked_add(p.ops.len()).unwrap_or_else(
                                        || fail_stop("accepted generation tuple overflow"),
                                    );
                                runtime.published.store(p.publication.take().unwrap());
                                #[cfg(test)]
                                hook(runtime, Boundary::AfterPublication).map_err(|_| {
                                    Outcome::Commit {
                                        state: CommitState::Committed,
                                        message: "postcommit maintenance failed".into(),
                                    }
                                })?;
                                Ok(runtime.snapshot())
                            })
                            .unwrap()
                    })
                    .unwrap(),
            }
        }
    };
    if matches!(
        result,
        Err(Outcome::Commit {
            state: CommitState::Indeterminate,
            ..
        })
    ) {
        gate.fence();
    }
    drop(generation_owner);
    drop(lease);
    drop(gate);
    result
}

fn rotate(runtime: &Runtime, core: &mut Core) -> Result<(), Outcome> {
    let previous = core.parents.as_ref().unwrap().get();
    if previous.generations.len() == SOURCE_LIMIT {
        return Err(limit(
            "source_generations",
            SOURCE_LIMIT + 1,
            SOURCE_LIMIT,
            false,
        ));
    }
    let domain = runtime.domain()?;
    let generation_id = core.next_generation;
    let next_id = generation_id
        .checked_add(1)
        .ok_or(Outcome::Refused("generation id exhausted"))?;
    let mut parents = domain
        .build::<Producers>(ResourceClass::Sources)
        .map_err(Outcome::from)?;
    let mut sources = domain
        .build::<Sources>(ResourceClass::Sources)
        .map_err(Outcome::from)?;
    let mut generation = domain
        .build::<Generation>(ResourceClass::Generation)
        .map_err(Outcome::from)?;
    generation.mutate(|g| g.id = generation_id).unwrap();
    let generation = generation.into_owner().unwrap();
    sources
        .mutate(|s| {
            s.reserve_exact(SOURCE_LIMIT);
            s.push(generation.read());
        })
        .unwrap();
    parents
        .mutate(|p| {
            p.generations.reserve_exact(SOURCE_LIMIT);
            p.generations.push(generation);
        })
        .unwrap();
    for child in &previous.generations {
        // Preserve unchanged child authority BEFORE retiring the prior parent.
        let next = child.fork();
        sources.mutate(|s| s.push(next.read())).unwrap();
        parents.mutate(|p| p.generations.push(next)).unwrap();
    }
    core.next_generation = next_id;
    core.parents = Some(parents.into_owner().unwrap());
    core.sources = Some(sources.into_owner().unwrap());
    core.generation_bytes = 0;
    core.generation_tuples = 0;
    runtime.stats.set_generations(
        1,
        core.parents
            .as_ref()
            .unwrap()
            .get()
            .generations
            .len()
            .saturating_sub(1) as u64,
    );
    Ok(())
}

fn capture_frozen(core: &mut Core, domain: &DomainHandle) -> Result<CapturedJob, Outcome> {
    if core.captured_generation.is_some() {
        return Err(Outcome::Refused("generation capture already in flight"));
    }
    let generations = &core.parents.as_ref().unwrap().get().generations;
    if generations.len() <= 1 {
        return Err(Outcome::Refused("no frozen generation"));
    }
    let generation = generations.last().unwrap().read();
    let job = CapturedJob {
        domain: domain.clone(),
        generation,
        revision: core.catalog_revision,
        _heavy: core.maintenance.acquire()?,
    };
    core.captured_generation = Some(job.identity());
    Ok(job)
}
fn settle_captured(core: &mut Core, job: CapturedJob, _restore: bool) {
    if core.captured_generation == Some(job.identity()) {
        core.captured_generation = None;
    }
    drop(job);
}
fn service_adoption(runtime: &Runtime, core: &mut BuildPermit<Core>) -> bool {
    let ready = {
        let mut control = runtime.adoption.lock().unwrap();
        if !matches!(control.slot.as_ref(), Some(AdoptionState::Ready(_))) {
            return false;
        }
        let Some(AdoptionState::Ready(ready)) = control.slot.take() else {
            unreachable!()
        };
        control.slot = Some(AdoptionState::Settling);
        ready
    };
    let AdmittedAdoption {
        custody,
        job,
        mut ticket,
        mut rejection,
    } = *ready;
    let canceled = ticket.is_none();
    let result = if let Some(outcome) = rejection.take() {
        Err(outcome)
    } else if canceled {
        Err(Outcome::Refused("adoption canceled"))
    } else {
        let ticket = ticket.as_mut().unwrap();
        let control = runtime.adoption.lock().unwrap();
        if job.identity() != custody.identity || custody.identity.1 != control.revision {
            Err(Outcome::Refused("stale captured catalog revision"))
        } else if ticket.mutate(|t| t.retires(0)).unwrap() {
            Err(Outcome::Refused("active WAL retirement requires rotation"))
        } else if !ticket
            .mutate(|t| {
                t.captured.as_ref().is_some_and(|s| {
                    let current = runtime.snapshot();
                    let frozen = core
                        .mutate(|c| {
                            c.captured_generation.is_some_and(|(history, revision)| {
                                job.identity() == (history, revision)
                            })
                        })
                        .unwrap();
                    if frozen {
                        s.sources().iter().any(|g| g.get().id == job.identity().0)
                            && current
                                .sources()
                                .iter()
                                .any(|g| g.get().id == job.identity().0)
                    } else {
                        s.seq() == current.seq()
                    }
                })
            })
            .unwrap()
        {
            Err(Outcome::Refused("stale admitted source frontier"))
        } else {
            // This typed completion's promises/outputs were accepted before job
            // execution. Its unchanged captured catalog needs no new reservation.
            if !ticket.mutate(|t| t.captured_catalog_unchanged).unwrap() {
                Err(Outcome::Refused(
                    "captured adoption changes catalog coverage",
                ))
            } else {
                core.mutate(|c| super::durable_transition::publish(&c.path, &runtime.fence, ticket))
                    .unwrap()
            }
        }
    };
    if runtime.fence.is_fenced() {
        runtime.pressure.fence();
    }
    drop(ticket);
    let restore = result.is_err()
        && !runtime.fence.is_fenced()
        && core
            .mutate(|c| {
                c.captured_generation
                    .is_some_and(|(history, revision)| job.identity() == (history, revision))
            })
            .unwrap();
    core.mutate(|c| settle_captured(c, job, restore)).unwrap();
    let _ = custody.reply.send(result);
    #[cfg(test)]
    hook(runtime, Boundary::AfterAdoption).unwrap();
    drop(custody.operation);
    runtime.adoption.lock().unwrap().slot = None;
    runtime.pressure.notify();
    runtime.lifecycle.wake();
    true
}

fn request_progress(
    runtime: &Runtime,
    core: &mut BuildPermit<Core>,
    request: &mut Request,
) -> RequestProgress {
    match commit(runtime, core, request) {
        Err(Outcome::Refused("generation rotation required")) => {
            restore_input(request);
            match core.mutate(|c| rotate(runtime, c)).unwrap() {
                Ok(()) => {
                    runtime.pressure.notify();
                    RequestProgress::Park(super::concurrency_lifecycle::ResourceLimit {
                        resource: "generation_wal_bytes",
                        requested: 0,
                        limit: runtime.options.generation_wal_bytes,
                        retryable: true,
                    })
                }
                Err(Outcome::ResourceLimit(resource)) if resource.retryable => {
                    RequestProgress::Park(resource)
                }
                Err(error) => RequestProgress::Complete(Err(error)),
            }
        }
        Err(Outcome::ResourceLimit(resource)) if resource.retryable => {
            restore_input(request);
            RequestProgress::Park(resource)
        }
        result => RequestProgress::Complete(result),
    }
}
fn settle_request(
    _runtime: &Runtime,
    request: Request,
    result: Result<Snapshot, Outcome>,
    _timeout: bool,
) {
    let _ = request.reply.send(result);
    drop(request);
}

fn coordinator(runtime: &Runtime, mut core: BuildPermit<Core>) {
    runtime.lifecycle.register_coordinator();
    let mut last_sync = Instant::now();
    let mut head: Option<Request> = None;
    let mut waiting = None;
    loop {
        #[cfg(test)]
        {
            if let Some(reply) = runtime.hooks.lock().unwrap().capture.take() {
                let domain = runtime.domain().unwrap();
                let _ = reply.send(core.mutate(|c| capture_frozen(c, &domain)).unwrap());
            }
            hook(runtime, Boundary::BeforeQueue).unwrap();
        }
        if runtime.lifecycle.close_requested() {
            runtime.lifecycle.stop_admission();
            if let Some(session) = runtime.directory_session.lock().unwrap().as_mut() {
                session.begin_shutdown();
            }
            runtime.pressure.close();
        }
        // Lifecycle completion never enters the ordinary queue, and never asks
        // for new output capacity. It runs before both retained-head retry and
        // ordinary dequeue, including at full queue or during accepted-job close.
        service_adoption(runtime, &mut core);
        if !runtime.lifecycle.close_requested() && !runtime.fence.is_fenced() {
            let force_sync = waiting.as_ref().is_some_and(
                |(resource, _): &(
                    super::concurrency_lifecycle::ResourceLimit,
                    super::concurrency_lifecycle::ReadinessEpoch,
                )| resource.resource == "WAL_debt_bytes",
            );
            let synced = core
                .mutate(|c| {
                    if let Some(WalSyncMode::GroupCommit {
                        interval_ms,
                        soft_trigger_bytes,
                        ..
                    }) = c.mode
                    {
                        let wal = c.wal.as_mut().unwrap();
                        if wal.buffered_bytes > 0
                            && (force_sync
                                || wal.buffered_bytes >= soft_trigger_bytes
                                || last_sync.elapsed() >= Duration::from_millis(interval_ms))
                        {
                            let Ok(gate) = runtime.fence.gate() else {
                                return false;
                            };
                            #[cfg(test)]
                            {
                                let mut hooks = runtime.hooks.lock().unwrap();
                                if hooks.group_failures > 0 {
                                    hooks.group_failures -= 1;
                                    wal.wal_writer
                                        .inject_fault(crate::wal::WalFault::SyncBeforeFlush);
                                }
                            }
                            if sync_pending(wal).is_err() && wal.poisoned.is_some() {
                                gate.fence();
                                #[cfg(test)]
                                if let Some(notify) =
                                    runtime.hooks.lock().unwrap().group_poison.take()
                                {
                                    notify.send(wal.buffered_bytes).unwrap();
                                }
                            }
                            last_sync = Instant::now();
                            return true;
                        }
                    }
                    false
                })
                .unwrap();
            if synced {
                runtime.pressure.notify();
            }
        }
        if runtime.fence.is_fenced() {
            runtime.pressure.fence();
        }
        if let (Some(request), Some((resource, observed))) = (head.as_ref(), waiting.take()) {
            if request.canceled.load(Ordering::Acquire) == 2 {
                settle_request(
                    runtime,
                    head.take().unwrap(),
                    Err(Outcome::Refused("canceled before acceptance")),
                    false,
                );
                continue;
            }
            match runtime
                .pressure
                .wait(observed, request.deadline, resource, false)
            {
                Ok(_) => {}
                Err(error) => {
                    let timeout = resource.retryable && request.deadline.expired();

                    settle_request(runtime, head.take().unwrap(), Err(error), timeout);
                    continue;
                }
            }
        }
        if head.is_none() {
            let mut queue = runtime.queue.lock().unwrap();
            if queue
                .front()
                .is_some_and(|r| r.position.is_head().unwrap_or(true))
            {
                head = queue.pop_front();
            }
        }
        if let Some(request) = head.as_mut() {
            let observed = runtime.pressure.observe(); // BEFORE the complete resource probe.
            match request_progress(runtime, &mut core, request) {
                RequestProgress::Complete(result) => {
                    let timeout = request.deadline.expired()
                        && (request.parked
                            || request.deadline.expires() != request.deadline.started());
                    settle_request(runtime, head.take().unwrap(), result, timeout);
                }
                RequestProgress::Park(resource) => {
                    request.parked = true;
                    #[cfg(test)]
                    hook(runtime, Boundary::Parked).unwrap();
                    waiting = Some((resource, observed));
                }
            }
            continue;
        }
        if runtime.lifecycle.close_requested()
            && runtime.adoption.lock().unwrap().is_none()
            && runtime.pressure.occupied() == 0
        {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    runtime.lifecycle.drain_independent();
    runtime.closes.fetch_add(1, Ordering::AcqRel);
    core.mutate(|c| {
        let mut wal = c.wal.take().unwrap();
        let gate = runtime.fence.shutdown_gate();
        #[cfg(test)]
        {
            let mut hooks = runtime.hooks.lock().unwrap();
            hooks.close_debt = wal.buffered_bytes;
            if let Some(fault) = hooks.close_wal.take() {
                wal.wal_writer.inject_fault(fault);
            }
        }
        if let Err(error) = wal.wal_writer.sync() {
            gate.fence();
            *runtime.close_error.lock().unwrap() = Some(error.to_string());
        }
        c.closed_buffers = Some(wal.wal_writer.close_retaining_buffers());
    })
    .unwrap();
    runtime.published.store(runtime.sentinel.clone());
    drop(core);
    runtime.stats.set_generations(0, 0);
    // Session completion is independent of retained passive snapshots/native
    // join. Closed metadata must not retain directory/Wake/unused-job roots.
    drop(runtime.directory_session.lock().unwrap().take());
    drop(runtime.completion.lock().unwrap().take());
    let domain = runtime.domain.lock().unwrap().take().unwrap();
    domain.close();
    drop(domain);
    runtime.lifecycle.complete_close();
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    InputAdmission,
    BeforeQueue,
    Primary,
    Derived,
    BeforePublication,
    AfterPublication,
    Parked,
    AfterAdoption,
}
#[cfg(test)]
#[derive(Default)]
struct Hooks {
    capture: Option<mpsc::SyncSender<Result<CapturedJob, Outcome>>>,
    pause: Option<(Boundary, mpsc::Sender<()>, mpsc::Receiver<()>)>,
    error: Option<Boundary>,
    wal: Option<crate::wal::WalFault>,
    close_wal: Option<crate::wal::WalFault>,
    close_debt: usize,
    persistence: Option<crate::persistence_fault::Boundary>,
    group_failures: u32,
    group_poison: Option<mpsc::Sender<usize>>,
}
#[cfg(test)]
fn hook(runtime: &Runtime, boundary: Boundary) -> Result<(), Outcome> {
    let pause = {
        let mut hooks = runtime.hooks.lock().unwrap();
        if hooks.error == Some(boundary) {
            hooks.error = None;
            return Err(Outcome::Commit {
                state: CommitState::Indeterminate,
                message: format!("injected {boundary:?}"),
            });
        }
        if hooks
            .pause
            .as_ref()
            .is_some_and(|(at, _, _)| *at == boundary)
        {
            hooks.pause.take()
        } else {
            None
        }
    };
    if let Some((_, ready, resume)) = pause {
        ready.send(()).unwrap();
        resume.recv().unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::participant::TEST_SERIAL;
    use super::*;
    use crate::property_value_semantics::hash_prop_equality_key;
    use crate::types::{DbOptions, NodeLabelSet};
    use crate::wal::{wal_generation_path, WalFault, WalReader};
    use tempfile::TempDir;

    // Observed excluded dependency metadata for this build and caller pattern:
    // actual snapshot root loads reach ArcSwap's process-retained debt list.

    // Positive governed regressions for the exact historical five-stage stop.
    // The frozen 011 patch preserves the prior aborting implementation verbatim.
    #[test]
    fn p45a2_payload_native_stack_stages() {
        use super::super::payload::{
            inspect, validate_persisted, write_value, CloneState, CountSink, HashSink, PayloadError,
        };
        const CHILD: &str = "OVERGRAPH_P45A2_PROPERTY_STACK_CHILD";
        const TEST: &str = "engine::append_slice::tests::p45a2_payload_native_stack_stages";
        if let Ok(mode) = std::env::var(CHILD) {
            let depth = if mode == "drop" || mode == "guardian" {
                131_072
            } else {
                65_536
            };
            std::thread::Builder::new().name("p45a2-property-stack".into()).stack_size(2 * 1024 * 1024).spawn(move || {
                let mut root = PropValue::Null;
                for _ in 0..depth { root = PropValue::Array(vec![root]); }
                let root = Owned::new(root);
                let requested_payload = depth * std::mem::size_of::<PropValue>();
                let property_wire_bytes = depth * 8 + 5;
                eprintln!("property-stack mode={mode} depth={depth} requested_payload_bytes={requested_payload} property_wire_bytes={property_wire_bytes} native_stack_bytes=2097152 entered=true");
                let domain = ReclaimDomain::new();
                let stats = domain.stats();
                match mode.as_str() {
                    "count" => {
                        let inspected = inspect(&root).unwrap();
                        assert_eq!(inspected.values, depth + 1);
                        assert_eq!(inspected.max_containers, depth);
                        assert!(inspected.values > 0);
                        assert_eq!(validate_persisted(inspected, 1), Err(PayloadError::PersistedRoundTrip));
                    }
                    "clone" => {
                        let mut cloned = domain.build::<CloneState>( ResourceClass::Staging).unwrap();
                        cloned.mutate(|state| state.clone_value(&root)).unwrap().unwrap();
                        let cloned = cloned.into_owner().unwrap();
                        assert!(super::super::payload::structural_eq(&root, &cloned.get().root).unwrap());
                        drop(cloned);
                    }
                    "encode" => {
                        let mut bytes = domain.build::<Vec<u8>>( ResourceClass::Staging).unwrap();
                        bytes.mutate(|out| {
                            out.reserve_exact(property_wire_bytes);
                            write_value(&root, out).unwrap();
                            assert_eq!(out.len(), property_wire_bytes);
                        }).unwrap();
                        let mut count = CountSink::default();
                        write_value(&root, &mut count).unwrap();
                        assert_eq!(count.0, property_wire_bytes);
                        let bytes = bytes.into_owner().unwrap();
                        let mut hash = HashSink::default();
                        write_value(&root, &mut hash).unwrap();
                        assert_eq!(hash.0, crate::types::fnv1a(bytes.get()));
                        drop(bytes);
                    }
                    "drop" => drop(root),
                    "guardian" => {
                        let mut build = domain.build::<Owned<Option<PropValue>>>( ResourceClass::Generation).unwrap();
                        build.mutate(|out| **out = Some(root.into_inner())).unwrap();
                        eprintln!("property-stack mode=guardian registered_objects={} worker=overgraph-reclaimer", stats.read().occupied);
                        drop(build.into_owner().unwrap());
                    }
                    _ => panic!("unknown property stack probe"),
                }
                domain.close();
                let deadline = Instant::now() + Duration::from_secs(10);
                while !stats.read().exited { assert!(Instant::now() < deadline, "worker did not finish"); std::thread::yield_now(); }
                assert_eq!(stats.read().occupied, 0);
                eprintln!("property-stack mode={mode} completed=true");
            }).unwrap().join().unwrap();
            return;
        }
        for mode in ["count", "clone", "encode", "drop", "guardian"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, mode)
                .env("RUST_MIN_STACK", "2097152")
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("{stderr}");
            assert!(output.status.success(), "{mode}: {stderr}");
            assert!(
                stderr.contains("entered=true") && stderr.contains("completed=true"),
                "{mode}: {stderr}"
            );
            assert!(!stderr.contains("stack overflow"), "{mode}: {stderr}");
        }
    }

    fn node(id: u64, value: i64, label: u32) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label).unwrap(),
            key: format!("n{id}"),
            props: BTreeMap::from([("value".into(), PropValue::Int(value))]),
            created_at: 10,
            updated_at: 20,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }
    fn edge(id: u64, from: u64) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to: from + 1,
            label_id: 1,
            props: BTreeMap::from([("payload".into(), PropValue::String("edge".into()))]),
            created_at: 10,
            updated_at: 20,
            weight: 0.5,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }
    fn observe(view: &Snapshot, db: &Slice) -> super::super::concurrency_proofs::TraceObservation {
        let mut observed = super::super::concurrency_proofs::TraceObservation {
            frontier: view.seq(),
            ..Default::default()
        };
        let lease = db.runtime.admission.try_acquire().unwrap();
        for id in 1..=12 {
            if let Some(n) = view.node(&lease, id) {
                observed.nodes.insert(
                    id,
                    (
                        n.last_write_seq,
                        crate::encoding::encode_wal_op(&WalOp::UpsertNode(n.clone())).unwrap(),
                    ),
                );
            }
            if let Some(e) = view.edge(&lease, id) {
                observed.edges.insert(
                    id,
                    (
                        e.last_write_seq,
                        crate::encoding::encode_wal_op(&WalOp::UpsertEdge(e.clone())).unwrap(),
                    ),
                );
            }
        }
        drop(lease);
        for label in 1..=2 {
            for value in 0..3 {
                observed
                    .equality
                    .insert((label, value), ids(view, label, value));
            }
        }
        for owner in 1..=3 {
            observed.outgoing.insert(owner, outs(view, owner));
            observed.degrees.insert(owner, view.degree(owner).unwrap());
        }
        for (namespace, id) in [(0, 1), (0, 2), (1, 1)] {
            observed
                .tokens
                .insert((namespace, id), view.token(namespace, id).unwrap().unwrap());
        }
        observed
    }

    #[test]
    fn p45a1_real_oracle_rejects_faulty_trace_observations() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let mut oracle = super::super::concurrency_proofs::TraceOracle::default();
        oracle.apply(&[
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
            WalOp::EnsureNodeLabel {
                label: "M".into(),
                label_id: 2,
            },
            WalOp::EnsureEdgeLabel {
                label: "E".into(),
                label_id: 1,
            },
        ]);
        let first = vec![
            WalOp::UpsertNode(node(7, 0, 1)),
            WalOp::UpsertEdge(edge(7, 1)),
        ];
        oracle.apply(&first);
        let before = db.submit(first).unwrap().wait().unwrap();
        let before_rows = observe(&before, &db);
        assert_eq!(oracle.check(&before_rows), Ok(()));
        let moved = vec![
            WalOp::UpsertNode(node(7, 1, 2)),
            WalOp::UpsertEdge(edge(7, 2)),
        ];
        oracle.apply(&moved);
        let after = db.submit(moved).unwrap().wait().unwrap();
        let after_rows = observe(&after, &db);
        assert_eq!(oracle.check(&after_rows), Ok(()));
        let mut premature = before_rows.clone();
        premature.frontier = after_rows.frontier;
        premature.nodes = after_rows.nodes.clone();
        premature.edges = after_rows.edges.clone();
        assert_eq!(oracle.check(&premature), Err("equality"));
        let mut missing_retraction = after_rows.clone();
        missing_retraction
            .equality
            .insert((1, 0), before_rows.equality[&(1, 0)].clone());
        assert_eq!(oracle.check(&missing_retraction), Err("equality"));
        // The same oracle also validates concrete physical catalog observations.
        let output = crate::types::SegmentInfo {
            id: 3,
            node_count: 1,
            edge_count: 1,
            segment_format_version: crate::segment_writer::SEGMENT_FORMAT_VERSION,
            segment_data_id: [81; 32],
        };
        oracle.adopt(std::slice::from_ref(&output));
        let mut accepted = after_rows;
        accepted.sources.insert(output.id, output.segment_data_id);
        assert_eq!(oracle.check(&accepted), Ok(()));
        let mut stale = accepted;
        stale.sources.insert(output.id, [82; 32]);
        assert_eq!(oracle.check(&stale), Err("stale adoption"));
        db.close().unwrap();
        drop(before);
        drop(after);
        wait_exit(&db.stats());
    }

    fn seed(db: &Slice) {
        db.submit(vec![
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
            WalOp::EnsureNodeLabel {
                label: "M".into(),
                label_id: 2,
            },
            WalOp::EnsureEdgeLabel {
                label: "E".into(),
                label_id: 1,
            },
        ])
        .unwrap()
        .wait()
        .unwrap();
    }
    fn pause(db: &Slice, at: Boundary) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        db.runtime.hooks.lock().unwrap().pause = Some((at, ready_tx, resume_rx));
        db.runtime.lifecycle.wake();
        (ready_rx, resume_tx)
    }
    fn ids(view: &Snapshot, label: u32, value: i64) -> Vec<u64> {
        let mut cursor = view.equality_cursor(label, PropValue::Int(value)).unwrap();
        let mut result = Vec::new();
        loop {
            let chunk = cursor.refill().unwrap();
            result.extend_from_slice(chunk.rows.get());
            if chunk.done {
                return result;
            }
            assert!(chunk.examined > 0);
        }
    }
    fn outs(view: &Snapshot, owner: u64) -> Vec<u64> {
        let mut cursor = view.outgoing_cursor(owner);
        let mut result = Vec::new();
        loop {
            let chunk = cursor.refill().unwrap();
            result.extend_from_slice(chunk.rows.get());
            if chunk.done {
                return result;
            }
            assert!(chunk.examined > 0);
        }
    }
    fn wait_exit(stats: &ReclamationStatsHandle) {
        let until = Instant::now() + Duration::from_secs(10);
        while !stats.read().exited {
            assert!(Instant::now() < until, "{:?}", stats.read());
            std::thread::yield_now();
        }
        assert_eq!(stats.read().occupied, 0);
        assert_eq!(stats.read().producers, 0);
    }
    #[test]
    fn p45a1_aggregate_publication_all_install_boundaries() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for at in [
            Boundary::Primary,
            Boundary::Derived,
            Boundary::BeforePublication,
        ] {
            let dir = TempDir::new().unwrap();
            let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
            seed(&db);
            let old = db
                .submit(vec![
                    WalOp::UpsertNode(node(1, 1, 1)),
                    WalOp::UpsertEdge(edge(1, 1)),
                ])
                .unwrap()
                .wait()
                .unwrap();
            let (ready, resume) = pause(&db, at);
            let request = db
                .submit(vec![
                    WalOp::UpsertNode(node(1, 2, 2)),
                    WalOp::UpsertEdge(edge(1, 2)),
                    WalOp::EnsureNodeLabel {
                        label: "Future".into(),
                        label_id: 3,
                    },
                ])
                .unwrap();
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            let during = db.snapshot().unwrap();
            assert_eq!(during.seq(), old.seq());
            for view in [&old, &during] {
                assert_eq!(ids(view, 1, 1), vec![1]);
                assert!(ids(view, 2, 2).is_empty());
                assert_eq!(outs(view, 1), vec![1]);
                assert_eq!(view.degree(1).unwrap(), 1);
                assert_eq!(view.degree(2).unwrap(), 0);
                assert_eq!(view.token(0, 3).unwrap(), None);
            }
            // The admitted writer owns its lease through install, even when all
            // remaining slots are subsequently exhausted.
            let other = ParticipantAdmission::new(256).unwrap();
            let leases: Vec<_> = (0..255).map(|_| other.try_acquire().unwrap()).collect();
            assert!(other.try_acquire().is_err());
            resume.send(()).unwrap();
            let new = request.wait().unwrap();
            drop(leases);
            assert!(ids(&new, 1, 1).is_empty());
            assert_eq!(ids(&new, 2, 2), vec![1]);
            assert!(outs(&new, 1).is_empty());
            assert_eq!(outs(&new, 2), vec![1]);
            assert_eq!(new.degree(1).unwrap(), 0);
            assert_eq!(new.degree(2).unwrap(), 1);
            assert_eq!(new.token(0, 3).unwrap().as_deref(), Some("Future"));
            let stats = db.stats();
            db.close().unwrap();
            assert!(db.runtime.published.load().state.is_none());
            assert!(!stats.read().exited);
            assert_eq!(ids(&old, 1, 1), vec![1]);
            drop(old);
            drop(during);
            drop(new);
            wait_exit(&stats);
        }
    }
    #[test]
    fn p45a1_seeded_serial_oracle_moves_tombstones_collision_and_rotation() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let options = crate::ConcurrencyOptions {
            generation_operation_limit: 128,
            max_batch_operations: 128,
            ..crate::ConcurrencyOptions::default()
        };
        let db = Slice::open_configured(dir.path(), WalSyncMode::Immediate, options).unwrap();
        seed(&db);
        let mut oracle = super::super::concurrency_proofs::TraceOracle::default();
        oracle.apply(&[
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
            WalOp::EnsureNodeLabel {
                label: "M".into(),
                label_id: 2,
            },
            WalOp::EnsureEdgeLabel {
                label: "E".into(),
                label_id: 1,
            },
        ]);
        let mut seed_value = 0x45a1_u64;
        for batch in 0..64 {
            let mut input = Vec::new();
            for _ in 0..4 {
                seed_value = seed_value
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = 1 + ((seed_value >> 32) % 12);
                let value = ((seed_value >> 20) % 3) as i64;
                let label = 1 + ((seed_value >> 16) % 2) as u32;
                let op = match seed_value % 4 {
                    0 => WalOp::DeleteNode { id, deleted_at: 30 },
                    1 => WalOp::UpsertNode(node(id, value, label)),
                    2 => WalOp::DeleteEdge { id, deleted_at: 30 },
                    _ => WalOp::UpsertEdge(edge(id, 1 + (seed_value >> 10) % 3)),
                };
                input.push(op);
            }
            oracle.apply(&input);
            let view = db.submit(input).unwrap().wait().unwrap();
            assert_eq!(
                oracle.check(&observe(&view, &db)),
                Ok(()),
                "seed=0x45a1 batch={batch}"
            );
        }
        let view = db.snapshot().unwrap();
        let lease = db.runtime.admission.try_acquire().unwrap();
        // Forced semantic-hash collision/missing-primary opinion. Verification
        // must reject it independently of the posting's hash and Present bit.
        view.sources()[0].get().equality.insert_unique(
            &lease,
            (
                1,
                hash_prop_equality_key(&PropValue::Int(99)),
                999,
                Reverse(view.seq()),
            ),
            true,
        );
        drop(lease);
        assert!(ids(&view, 1, 99).is_empty());
        drop(view);
        for batch in 0..35 {
            db.submit(
                (0..50)
                    .map(|n| WalOp::UpsertNode(node(1000 + n, batch, 1)))
                    .collect(),
            )
            .unwrap()
            .wait()
            .unwrap();
        }
        let rotated = db.snapshot().unwrap();
        assert!(rotated.sources().len() > 1);
        assert_eq!(ids(&rotated, 1, 34), (1000..1050).collect::<Vec<_>>());
        let no_op = db.submit(vec![]).unwrap().wait().unwrap();
        assert_eq!(no_op.seq(), rotated.seq());
        let stats = db.stats();
        db.close().unwrap();
        drop(rotated);
        drop(no_op);
        wait_exit(&stats);
    }
    #[test]
    fn p45a1_future_groups_finite_continuation_and_oversized_refill() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        // Keep all 4,150 future primary groups in the generation captured by
        // `old`. Reserve the complete modeled output/scratch as well as derived
        // tuples; earlier rotation would stop exercising the exact group yield.
        let options = crate::ConcurrencyOptions {
            generation_operation_limit: 20_000,
            max_batch_operations: 20_000,
            ..crate::ConcurrencyOptions::default()
        };
        let db = Slice::open_configured(dir.path(), WalSyncMode::Immediate, options).unwrap();
        seed(&db);
        let old = db.snapshot().unwrap();
        for batch in 0..83 {
            let mut ops = Vec::new();
            for item in 0..50 {
                let mut n = node(1 + batch * 50 + item, 0, 1);
                n.props.clear();
                ops.push(WalOp::UpsertNode(n));
            }
            db.submit(ops).unwrap().wait().unwrap();
        }
        let mut cursor = old.node_cursor();
        let first = cursor.refill().unwrap();
        assert_eq!(first.examined, 4096);
        assert!(!first.done);
        assert!(first.rows.get().is_empty());
        drop(first);
        let second = cursor.refill().unwrap();
        assert!(second.examined > 0);
        assert!(second.done);
        assert!(second.rows.get().is_empty());
        drop(second);
        let mut large = node(5000, 5, 1);
        large.props.insert(
            "large".into(),
            PropValue::Bytes(vec![9; QUANTUM_BYTES + 4096]),
        );
        let new = db
            .submit(vec![WalOp::UpsertNode(large)])
            .unwrap()
            .wait()
            .unwrap();
        let mut ordered = new.node_cursor();
        let mut seen_large = 0;
        loop {
            let chunk = ordered.refill().unwrap();
            for node in chunk.rows.get().iter() {
                if node.id == 5000 {
                    seen_large += 1;
                    assert!(chunk.copied > QUANTUM_BYTES);
                    assert_eq!(chunk.rows.get().len(), 1);
                }
            }
            if chunk.done {
                break;
            }
            assert!(chunk.examined > 0);
        }
        assert_eq!(seen_large, 1);
        let stats = db.stats();
        db.close().unwrap();
        drop(old);
        drop(new);
        drop(cursor);
        drop(ordered);
        wait_exit(&stats);
    }
    #[test]
    fn p45a1_node_refill_yields_before_large_item_without_consuming_it() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let mut large = node(2, 2, 1);
        large.props.insert(
            "large".into(),
            PropValue::Bytes(vec![9; QUANTUM_BYTES + 4096]),
        );
        let view = db
            .submit(vec![
                WalOp::UpsertNode(node(1, 1, 1)),
                WalOp::UpsertNode(large),
            ])
            .unwrap()
            .wait()
            .unwrap();
        let mut cursor = view.node_cursor();
        let small = cursor.refill().unwrap();
        assert_eq!(
            small.rows.get().iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![1]
        );
        assert!(small.copied < QUANTUM_BYTES);
        assert!(!small.done);
        assert_eq!(cursor.last, Some(1));
        let large = cursor.refill().unwrap();
        assert_eq!(
            large.rows.get().iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![2]
        );
        assert!(large.copied > QUANTUM_BYTES);
        assert!(!large.done);
        assert_eq!(cursor.last, Some(2));
        let end = cursor.refill().unwrap();
        assert!(end.done);
        assert!(end.rows.get().is_empty());
        db.close().unwrap();
        drop(small);
        drop(large);
        drop(end);
        drop(cursor);
        drop(view);
        wait_exit(&db.stats());
    }

    #[test]
    fn p45a1_fast_close_syncs_group_debt_and_detaches_on_terminal_error() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for fenced in [false, true] {
            for failure in [false, true] {
                let dir = TempDir::new().unwrap();
                let db = Slice::open(
                    dir.path(),
                    WalSyncMode::GroupCommit {
                        interval_ms: 1_000_000,
                        soft_trigger_bytes: 16 * 1024 * 1024,
                        hard_cap_bytes: 32 * 1024 * 1024,
                    },
                )
                .unwrap();
                seed(&db);
                let view = db
                    .submit(vec![WalOp::UpsertNode(node(1, 1, 1))])
                    .unwrap()
                    .wait()
                    .unwrap();
                if fenced {
                    db.runtime.fence.establish();
                }
                if failure {
                    db.runtime.hooks.lock().unwrap().close_wal = Some(WalFault::SyncAfterFsync);
                }
                assert_eq!(db.close_fast().is_err(), failure);
                assert!(db.runtime.hooks.lock().unwrap().close_debt > 0);
                assert!(db.runtime.published.load().state.is_none());
                assert!(db.runtime.domain().is_err());
                assert!(db.runtime.fence.is_fenced());
                let reopened = crate::engine::tests::open_after_native_shutdown(
                    dir.path(),
                    &DbOptions::default(),
                )
                .unwrap();
                assert!(reopened.get_node(1).unwrap().is_some());
                reopened.close().unwrap();
                drop(view);
                wait_exit(&db.stats());
            }
        }
    }
    #[test]
    fn p45a1_real_wal_install_error_fenced_close_modes_and_reopen() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for mode in 0..3 {
            let dir = TempDir::new().unwrap();
            let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
            seed(&db);
            let before = db.snapshot().unwrap();
            db.runtime.hooks.lock().unwrap().error = Some(Boundary::Derived);
            let result = db
                .submit(vec![
                    WalOp::UpsertNode(node(77, 7, 1)),
                    WalOp::EnsureNodeLabel {
                        label: "Durable".into(),
                        label_id: 3,
                    },
                ])
                .unwrap()
                .wait();
            assert!(matches!(
                result,
                Err(Outcome::Commit {
                    state: CommitState::Indeterminate,
                    ..
                })
            ));
            assert!(db.runtime.fence.is_fenced());
            assert_eq!(db.snapshot().unwrap().seq(), before.seq());
            assert!(matches!(
                db.submit(vec![WalOp::UpsertNode(node(88, 8, 1))])
                    .and_then(Pending::wait),
                Err(Outcome::Refused("fenced"))
            ));
            let stats = db.stats();
            let runtime = db.runtime.clone();
            match mode {
                0 => {
                    db.close().unwrap();
                }
                1 => {
                    db.close_fast().unwrap();
                }
                _ => {
                    drop(db);
                    runtime.lifecycle.wait_closed();
                }
            }
            assert!(runtime.published.load().state.is_none());
            assert_eq!(runtime.closes.load(Ordering::Acquire), 1);
            let replay = WalReader::read_generation_recoverable(dir.path(), 0).unwrap();
            assert!(replay
                .records
                .iter()
                .any(|(_, op)| matches!(op, WalOp::UpsertNode(n) if n.id == 77)));
            assert!(!replay
                .records
                .iter()
                .any(|(_, op)| matches!(op, WalOp::UpsertNode(n) if n.id == 88)));
            let reopened =
                crate::engine::tests::open_after_native_shutdown(dir.path(), &DbOptions::default())
                    .unwrap();
            assert!(reopened.get_node(77).unwrap().is_some());
            assert!(reopened.get_node(88).unwrap().is_none());
            reopened.close().unwrap();
            drop(before);
            wait_exit(&stats);
        }
    }
    #[test]
    fn p45a1_real_wal_torn_frames_sync_uncertainty_and_group_debt() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for prefix in [0, 1, 4, 8, 16, 30, 60, 100] {
            let dir = TempDir::new().unwrap();
            let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
            seed(&db);
            db.runtime.hooks.lock().unwrap().wal = Some(WalFault::AppendPrefix(prefix));
            assert!(matches!(
                db.submit(vec![
                    WalOp::UpsertNode(node(1, 1, 1)),
                    WalOp::UpsertNode(node(2, 2, 1))
                ])
                .unwrap()
                .wait(),
                Err(Outcome::Commit {
                    state: CommitState::Indeterminate,
                    ..
                })
            ));
            db.close_fast().unwrap();
            let records = WalReader::read_generation_recoverable(dir.path(), 0)
                .unwrap()
                .records;
            assert!(!records
                .iter()
                .any(|(_, op)| matches!(op, WalOp::UpsertNode(_))));
            let stats = db.stats();
            wait_exit(&stats);
        }
        for fault in [
            WalFault::SyncBeforeFlush,
            WalFault::SyncAfterFlush,
            WalFault::SyncAfterFsync,
        ] {
            let dir = TempDir::new().unwrap();
            let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
            seed(&db);
            db.runtime.hooks.lock().unwrap().wal = Some(fault);
            assert!(matches!(
                db.submit(vec![WalOp::UpsertNode(node(1, 1, 1))])
                    .unwrap()
                    .wait(),
                Err(Outcome::Commit {
                    state: CommitState::Indeterminate,
                    ..
                })
            ));
            db.close().unwrap();
            let records = WalReader::read_generation_recoverable(dir.path(), 0)
                .unwrap()
                .records;
            assert!(records
                .iter()
                .any(|(_, op)| matches!(op, WalOp::UpsertNode(n) if n.id == 1)));
            wait_exit(&db.stats());
        }
        let dir = TempDir::new().unwrap();
        let db = Slice::open(
            dir.path(),
            WalSyncMode::GroupCommit {
                interval_ms: 1_000_000,
                soft_trigger_bytes: 1 << 20,
                hard_cap_bytes: 1 << 24,
            },
        )
        .unwrap();
        let acknowledged = db
            .submit(vec![
                WalOp::EnsureNodeLabel {
                    label: "N".into(),
                    label_id: 1,
                },
                WalOp::UpsertNode(node(1, 1, 1)),
            ])
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(acknowledged.seq(), 2);
        assert_eq!(
            std::fs::metadata(wal_generation_path(dir.path(), 0))
                .unwrap()
                .len(),
            8
        );
        db.close().unwrap();
        drop(acknowledged);
        wait_exit(&db.stats());
        let mut state = WalSyncState {
            wal_writer: WalWriter::open_generation(dir.path(), 1).unwrap(),
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };
        state.buffered_bytes = state
            .wal_writer
            .append_batch(&[(1, WalOp::UpsertNode(node(2, 2, 1)))])
            .unwrap();
        let debt = state.buffered_bytes;
        for n in 1..=crate::wal_sync::MAX_SYNC_FAILURES {
            state.wal_writer.inject_fault(WalFault::SyncBeforeFlush);
            assert!(sync_pending(&mut state).is_err());
            assert_eq!(state.buffered_bytes, debt);
            assert_eq!(state.sync_error_count, n);
        }
        assert!(state.poisoned.is_some());
        sync_pending(&mut state).unwrap();
        assert_eq!(state.buffered_bytes, 0);
        assert!(state.poisoned.is_some());
    }
    #[test]
    fn p45a1_runtime_transactions_queue_full_cancellation_and_close_error() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let mut txn = db.begin().unwrap();
        let runtime = db.runtime.clone();
        drop(db);
        assert!(!runtime.lifecycle.close_requested());
        let view = txn
            .commit(vec![WalOp::UpsertNode(node(1, 1, 1))])
            .unwrap()
            .wait()
            .unwrap();
        assert!(txn.owner.is_none() && txn.snapshot.is_none());
        runtime.lifecycle.wait_closed();
        assert!(runtime.published.load().state.is_none());
        assert!(!view.sources().is_empty());
        drop(view);
        wait_exit(&runtime.stats);
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let (ready, resume) = pause(&db, Boundary::BeforeQueue);
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let canceled = db.submit(vec![WalOp::UpsertNode(node(2, 2, 1))]).unwrap();
        drop(canceled);
        let mut accepted = Vec::new();
        for id in 3..10 {
            accepted.push(db.submit(vec![WalOp::UpsertNode(node(id, 3, 1))]).unwrap());
        }
        assert!(matches!(
            db.submit(vec![]),
            Err(Outcome::ResourceLimit(
                super::super::concurrency_lifecycle::ResourceLimit {
                    resource: "request_queue",
                    retryable: true,
                    ..
                }
            ))
        ));
        let runtime = db.runtime.clone();
        let stats = db.stats();
        drop(db);
        assert!(runtime.lifecycle.close_requested());
        resume.send(()).unwrap();
        for request in accepted {
            drop(request.wait().unwrap());
        }
        runtime.lifecycle.wait_closed();
        assert_eq!(runtime.closes.load(Ordering::Acquire), 1);
        let records = WalReader::read_generation_recoverable(dir.path(), 0)
            .unwrap()
            .records;
        assert!(!records
            .iter()
            .any(|(_, op)| matches!(op, WalOp::UpsertNode(n) if n.id == 2)));
        wait_exit(&stats);
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        db.runtime.hooks.lock().unwrap().close_wal = Some(WalFault::SyncAfterFlush);
        assert!(db.close().is_err());
        assert!(db.runtime.published.load().state.is_none());
        wait_exit(&db.stats());
    }
    #[test]
    fn p45a1_preacceptance_refusal_lost_reply_and_committed_maintenance_error() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let prior = std::fs::read(wal_generation_path(dir.path(), 0)).unwrap();
        let prior_seq = db.snapshot().unwrap().seq();
        let pool = ParticipantAdmission::new(256).unwrap();
        let leases: Vec<_> = (0..256).map(|_| pool.try_acquire().unwrap()).collect();
        assert!(matches!(
            db.submit(vec![WalOp::UpsertNode(node(1, 1, 1))])
                .unwrap()
                .wait(),
            Err(Outcome::ResourceLimit(
                super::super::concurrency_lifecycle::ResourceLimit {
                    resource: "retained_participant_limit",
                    retryable: true,
                    ..
                }
            ))
        ));
        assert_eq!(
            std::fs::read(wal_generation_path(dir.path(), 0)).unwrap(),
            prior
        );
        drop(leases);
        assert_eq!(db.snapshot().unwrap().seq(), prior_seq);
        for input in [
            vec![WalOp::UpsertNode(node(0, 1, 1))],
            vec![WalOp::UpsertNode(node(9, 1, 9))],
            vec![WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 9,
            }],
            vec![WalOp::EnsureNodeLabel {
                label: "Wrong".into(),
                label_id: 1,
            }],
        ] {
            assert!(matches!(
                db.submit(input).unwrap().wait(),
                Err(Outcome::Refused(_))
            ));
        }
        assert_eq!(
            std::fs::read(wal_generation_path(dir.path(), 0)).unwrap(),
            prior
        );
        let (ready, resume) = pause(&db, Boundary::Primary);
        let lost = db.submit(vec![WalOp::UpsertNode(node(2, 2, 1))]).unwrap();
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(lost);
        resume.send(()).unwrap();
        drop(db.submit(vec![]).unwrap().wait().unwrap());
        // FIFO service completion of a second request proves the canceled reply
        // did not cancel the already accepted mutation.
        db.runtime.hooks.lock().unwrap().error = Some(Boundary::AfterPublication);
        let outcome = db
            .submit(vec![WalOp::UpsertNode(node(3, 3, 1))])
            .unwrap()
            .wait();
        assert!(matches!(
            outcome,
            Err(Outcome::Commit {
                state: CommitState::Committed,
                ..
            })
        ));
        let view = db.snapshot().unwrap();
        assert_eq!(ids(&view, 1, 2), vec![2]);
        assert_eq!(ids(&view, 1, 3), vec![3]);
        drop(view);
        db.close().unwrap();
        assert!(db.runtime.domain().is_err());
        wait_exit(&db.stats());
        let reopened =
            crate::engine::tests::open_after_native_shutdown(dir.path(), &DbOptions::default())
                .unwrap();
        assert!(reopened.get_node(1).unwrap().is_none());
        assert!(reopened.get_node(2).unwrap().is_some());
        assert!(reopened.get_node(3).unwrap().is_some());
        reopened.close().unwrap();
    }
    #[test]
    fn p45a1_wal_all_byte_truncations_and_named_kill_recovery_child() {
        const CHILD: &str = "OVERGRAPH_P45A1_WAL_KILL_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let path =
                std::path::PathBuf::from(std::env::var_os("OVERGRAPH_P45A1_CHILD_DIR").unwrap());
            let db = Slice::open(&path, WalSyncMode::Immediate).unwrap();
            let (ready, _resume) = pause(&db, Boundary::BeforePublication);
            let _pending = db
                .submit(vec![
                    WalOp::EnsureNodeLabel {
                        label: "N".into(),
                        label_id: 1,
                    },
                    WalOp::UpsertNode(node(1, 1, 1)),
                    WalOp::UpsertNode(node(2, 2, 1)),
                ])
                .unwrap();
            ready.recv().unwrap();
            std::fs::write(path.join("named-wal-ready"), b"durable-before-publication").unwrap();
            loop {
                std::thread::park();
            }
        }
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let base = TempDir::new().unwrap();
        let mut encoder = WalWriter::open_generation(base.path(), 0).unwrap();
        for count in [1, 2] {
            let ops: Vec<_> = (0..count)
                .map(|n| (n + 1, WalOp::UpsertNode(node(n + 1, n as i64, 1))))
                .collect();
            let mut frames = Vec::new();
            encoder.prepare_batch_into(&ops, &mut frames).unwrap();
            for bytes in 0..=frames.len() {
                let dir = TempDir::new().unwrap();
                let mut manifest = crate::manifest::default_manifest();
                manifest.node_label_tokens.insert("N".into(), 1);
                manifest.next_node_label_id = 2;
                crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
                let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
                writer.inject_fault(WalFault::AppendPrefix(bytes));
                assert!(writer.append_prepared(&frames).is_err());
                drop(writer);
                let replay = WalReader::read_generation_recoverable(dir.path(), 0).unwrap();
                assert_eq!(
                    replay.durable_len,
                    if bytes == frames.len() {
                        (8 + frames.len()) as u64
                    } else {
                        8
                    }
                );
                // Raw scanner records can include an unfinished Begin/op prefix;
                // the REAL engine replay owns atomic-batch visibility/filtering.
                let db = crate::engine::tests::open_after_native_shutdown(
                    dir.path(),
                    &DbOptions::default(),
                )
                .unwrap();
                let ids: Vec<_> = (1..=count)
                    .filter(|id| db.get_node(*id).unwrap().is_some())
                    .collect();
                assert_eq!(
                    ids,
                    if bytes == frames.len() {
                        (1..=count).collect::<Vec<_>>()
                    } else {
                        vec![]
                    },
                    "count={count} bytes={bytes}"
                );
                db.close().unwrap();
            }
        }
        let dir = TempDir::new().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact", "engine::append_slice::tests::p45a1_wal_all_byte_truncations_and_named_kill_recovery_child", "--nocapture"]).env(CHILD, "1").env("OVERGRAPH_P45A1_CHILD_DIR", dir.path()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.path().join("named-wal-ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before named WAL barrier: {status}");
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child missed named WAL barrier");
            }
            std::thread::yield_now();
        }
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
        let db =
            crate::engine::tests::open_after_native_shutdown(dir.path(), &DbOptions::default())
                .unwrap();
        assert!(db.get_node(1).unwrap().is_some());
        assert!(db.get_node(2).unwrap().is_some());
        db.close().unwrap();
    }
    #[test]
    fn p45a1_coordinator_manifest_uncertainty_every_close_mode_and_reopen() {
        use crate::persistence_fault::Boundary as Persist;
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for boundary in [
            Some(Persist::TmpCreate),
            Some(Persist::TmpWrite),
            Some(Persist::TmpSync),
            Some(Persist::PrevRemove),
            Some(Persist::CurrentToPrev),
            Some(Persist::TmpToCurrent),
            Some(Persist::DirSync),
            None,
        ] {
            for close_mode in 0..3 {
                let dir = TempDir::new().unwrap();
                let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
                seed(&db);
                drop(
                    db.submit(vec![WalOp::UpsertNode(node(1, 1, 1))])
                        .unwrap()
                        .wait()
                        .unwrap(),
                );
                let memtable = crate::memtable::Memtable::new();
                memtable.apply_op(&WalOp::UpsertNode(node(1, 1, 1)), 4);
                let output_dir = crate::segment_writer::segment_dir(dir.path(), 1);
                let output =
                    crate::segment_writer::write_segment_with_degree_overlay_and_secondary_indexes(
                        &output_dir,
                        1,
                        &memtable,
                        None,
                        &crate::degree_cache::DegreeOverlaySnapshot::empty(),
                        &[],
                    )
                    .unwrap();
                let captured = db.snapshot().unwrap();
                let current = crate::manifest::load_manifest_readonly(dir.path())
                    .unwrap()
                    .unwrap();
                crate::manifest::write_manifest(dir.path(), &current).unwrap();
                let mut after = current.clone();
                after.node_label_tokens =
                    BTreeMap::from([("N".into(), 1), ("M".into(), 2), ("Durable".into(), 3)]);
                after.next_node_label_id = 4;
                after.edge_label_tokens = BTreeMap::from([("E".into(), 1)]);
                after.next_edge_label_id = 2;
                after.next_node_id = 2;
                after.next_engine_seq = 5;
                after.segments = vec![output.clone()];
                let mut ticket = super::super::durable_transition::prepare(
                    &db.runtime.domain().unwrap(),
                    &current,
                    &after,
                    &[],
                    &[output],
                    &[],
                )
                .unwrap();
                ticket.mutate(|t| t.captured = Some(captured)).unwrap();
                if let Some(boundary) = boundary {
                    db.runtime.hooks.lock().unwrap().persistence = Some(boundary);
                } else {
                    db.runtime.hooks.lock().unwrap().error = Some(Boundary::BeforePublication);
                }
                assert!(matches!(
                    db.submit_transition(ticket).unwrap().wait(),
                    Err(Outcome::Commit {
                        state: CommitState::Indeterminate,
                        ..
                    })
                ));
                assert!(db.runtime.fence.is_fenced());
                assert!(wal_generation_path(dir.path(), 0).exists());
                assert!(output_dir.exists());
                assert!(matches!(
                    db.submit(vec![WalOp::UpsertNode(node(2, 2, 1))])
                        .and_then(Pending::wait),
                    Err(Outcome::Refused("fenced"))
                ));
                let runtime = db.runtime.clone();
                let stats = db.stats();
                match close_mode {
                    0 => db.close().unwrap(),
                    1 => db.close_fast().unwrap(),
                    _ => {
                        drop(db);
                        runtime.lifecycle.wait_closed();
                    }
                }
                assert!(runtime.published.load().state.is_none());
                assert!(runtime.domain().is_err());
                wait_exit(&stats);
                let selected = crate::manifest::load_manifest_readonly(dir.path())
                    .unwrap()
                    .unwrap();
                if boundary.is_none()
                    || matches!(
                        boundary,
                        Some(Persist::CurrentToPrev | Persist::TmpToCurrent | Persist::DirSync)
                    )
                {
                    assert_eq!(selected.node_label_tokens.get("Durable"), Some(&3));
                }
                let reopened = crate::engine::tests::open_after_native_shutdown(
                    dir.path(),
                    &DbOptions::default(),
                )
                .unwrap();
                assert!(reopened.get_node(1).unwrap().is_some());
                assert!(reopened.get_node(2).unwrap().is_none());
                reopened.close().unwrap();
            }
        }
    }
    #[test]
    fn p45a1_coordinator_group_poison_retains_acknowledged_debt_and_reopens() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(
            dir.path(),
            WalSyncMode::GroupCommit {
                interval_ms: 1_000_000,
                soft_trigger_bytes: 1,
                hard_cap_bytes: 1 << 24,
            },
        )
        .unwrap();
        let (poison_tx, poison_rx) = mpsc::channel();
        {
            let mut hooks = db.runtime.hooks.lock().unwrap();
            hooks.group_failures = crate::wal_sync::MAX_SYNC_FAILURES;
            hooks.group_poison = Some(poison_tx);
        }
        let acknowledged = db
            .submit(vec![
                WalOp::EnsureNodeLabel {
                    label: "N".into(),
                    label_id: 1,
                },
                WalOp::UpsertNode(node(1, 1, 1)),
            ])
            .unwrap()
            .wait()
            .unwrap();
        let debt = poison_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(debt > 0);
        assert!(db.runtime.fence.is_fenced());
        assert_eq!(acknowledged.seq(), 2);
        assert!(matches!(
            db.submit(vec![]).and_then(Pending::wait),
            Err(Outcome::Refused("fenced"))
        ));
        db.close().unwrap();
        drop(acknowledged);
        wait_exit(&db.stats());
        let reopened =
            crate::engine::tests::open_after_native_shutdown(dir.path(), &DbOptions::default())
                .unwrap();
        assert!(reopened.get_node(1).unwrap().is_some());
        reopened.close().unwrap();
    }

    #[test]
    fn p45a2_review_m1_bytes_refuse_before_wal_and_publication() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        seed(&db);
        let before = std::fs::read(wal_generation_path(dir.path(), 0)).unwrap();
        let seq = db.runtime.snapshot().seq();
        let mut value = Owned::new(PropValue::Bytes(vec![1]));
        for _ in 0..1022 {
            *value = PropValue::Array(vec![std::mem::replace(&mut *value, PropValue::Null)]);
        }
        let mut record = node(1, 1, 1);
        record.props.insert("deep".into(), value.into_inner());
        assert!(matches!(
            db.submit(vec![WalOp::UpsertNode(record)]),
            Err(Outcome::Refused("persisted payload depth"))
        ));
        assert_eq!(
            std::fs::read(wal_generation_path(dir.path(), 0)).unwrap(),
            before
        );
        assert_eq!(db.runtime.snapshot().seq(), seq);
        db.close().unwrap();
        wait_exit(&db.stats());
    }
}

#[cfg(test)]
mod practical_resource_tests {
    use super::*;
    use crate::types::NodeLabelSet;

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: "key".into(),
            props: BTreeMap::from([("value".into(), PropValue::Int(7))]),
            created_at: 0,
            updated_at: 0,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }
    fn options(ops: usize, bytes: usize, generation_ops: usize) -> crate::ConcurrencyOptions {
        crate::ConcurrencyOptions::default()
            .with_max_batch_operations(ops)
            .with_max_batch_bytes(bytes)
            .with_generation_operation_limit(generation_ops)
    }
    fn wait_drain(stats: &ReclamationStatsHandle) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !matches!(
            stats.read_full().unwrap().close_state,
            crate::ReclamationCloseState::Drained
        ) {
            assert!(
                Instant::now() < deadline,
                "shared/native drain incomplete: {:?}",
                stats.read()
            );
            std::thread::yield_now();
        }
        assert_eq!(stats.read_full().unwrap().registered_objects, 0);
    }
    fn label(db: &Slice) {
        drop(
            db.submit(vec![WalOp::EnsureNodeLabel {
                label: "Node".into(),
                label_id: 1,
            }])
            .unwrap()
            .wait()
            .unwrap(),
        );
    }

    #[test]
    fn p45a3_limits_exact_operation_wire_and_generation_rotation_without_partial_wal() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let domain = ReclaimDomain::new();
        for count in [0usize, 1, 2, 3] {
            let input: Vec<_> = (0..count)
                .map(|n| WalOp::DeleteNode {
                    id: n as u64 + 1,
                    deleted_at: 0,
                })
                .collect();
            let operations: Vec<_> = input
                .iter()
                .enumerate()
                .map(|(n, op)| (n as u64 + 1, op.clone()))
                .collect();
            let mut frames = Vec::new();
            let mut encode = Vec::new();
            crate::wal::prepare_v3_batch_bounded(&operations, &mut encode, &mut frames).unwrap();
            let cap = frames.len().max(1);
            assert!(check_input(&options(count.max(1), cap, count.max(1)), &input).is_ok());
            if !frames.is_empty() {
                assert!(
                    matches!(check_input(&options(count.max(1),cap-1,count.max(1)), &input), Err(Outcome::ResourceLimit(r)) if r.resource == "max_batch_bytes")
                );
            }
            if count > 1 {
                assert!(
                    matches!(check_input(&options(count-1,cap,count), &input), Err(Outcome::ResourceLimit(r)) if r.resource == "max_batch_operations")
                );
            }
        }
        let node_op = WalOp::UpsertNode(node(1));
        assert_eq!(
            wal_op_wire_len(&node_op).unwrap(),
            crate::encoding::encode_wal_op(&node_op).unwrap().len()
        );
        domain.close();
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open_configured(dir.path(), WalSyncMode::Immediate, options(2, 4096, 2))
            .unwrap();
        label(&db);
        let first = db
            .submit(vec![WalOp::UpsertNode(node(1))])
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(first.sources().len(), 1);
        let second = db
            .submit(vec![WalOp::UpsertNode(node(2))])
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(second.sources().len(), 2);
        assert_eq!(first.seq(), 2);
        let seq = second.seq();
        let before = std::fs::metadata(crate::wal::wal_generation_path(dir.path(), 0))
            .unwrap()
            .len();
        assert!(
            matches!(db.submit(vec![WalOp::UpsertNode(node(3)),WalOp::UpsertNode(node(4)),WalOp::UpsertNode(node(5))]),Err(Outcome::ResourceLimit(r)) if r.resource=="max_batch_operations")
        );
        assert_eq!(db.snapshot().unwrap().seq(), seq);
        assert_eq!(
            std::fs::metadata(crate::wal::wal_generation_path(dir.path(), 0))
                .unwrap()
                .len(),
            before
        );
        let stats = db.stats();
        db.close().unwrap();
        drop(first);
        drop(second);
        drop(db);
        wait_drain(&stats);
    }

    #[test]
    fn p45a3_local_copy_and_preparation_do_not_register_and_cleanup_on_owner_thread() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        label(&db);
        let view = db
            .submit(vec![WalOp::UpsertNode(node(1))])
            .unwrap()
            .wait()
            .unwrap();
        let domain = db.runtime.domain().unwrap();
        let before = domain.stats().read().occupied;
        let copied = view.copy_node(1).unwrap();
        assert_eq!(copied.get().data.as_ref().unwrap().key, "key");
        assert_eq!(domain.stats().read().occupied, before);
        drop(copied);
        let mut cursor = view.node_cursor();
        let rows = cursor.refill().unwrap();
        assert_eq!(rows.rows.get().len(), 1);
        assert_eq!(domain.stats().read().occupied, before);
        drop(rows);
        drop(cursor);
        let input = Owned::new(vec![WalOp::UpsertNode(node(2))]);
        check_input(&db.runtime.options, &input).unwrap();
        drop(input);
        assert_eq!(domain.stats().read().occupied, before);
        let stats = db.stats();
        db.close().unwrap();
        drop(view);
        drop(domain);
        drop(db);
        wait_drain(&stats);
    }

    #[test]
    fn p45a3_queue_full_refuses_before_traversal_and_releases_canceled_positions() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        let (entered, enter) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        db.runtime.hooks.lock().unwrap().pause = Some((Boundary::BeforeQueue, entered, resumed));
        db.runtime.lifecycle.wake();
        enter.recv_timeout(Duration::from_secs(5)).unwrap();
        let pending: Vec<_> = (1..=8)
            .map(|id| {
                db.submit(vec![WalOp::DeleteNode { id, deleted_at: 0 }])
                    .unwrap()
            })
            .collect();
        assert_eq!(db.runtime.pressure.occupied(), 8);
        // Unsupported and oversized input would fail validation if traversed.
        let mut invalid = node(1);
        invalid.dense_vector = Some(vec![1.0]);
        let refused = db.submit(vec![WalOp::UpsertNode(invalid)]);
        assert!(matches!(refused,Err(Outcome::ResourceLimit(r)) if r.resource=="request_queue"));
        drop(pending);
        resume.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while db.runtime.pressure.occupied() != 0 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(db.snapshot().unwrap().seq(), 0);
        let stats = db.stats();
        db.close().unwrap();
        drop(db);
        wait_drain(&stats);
    }

    #[test]
    fn p45a3_heavy_job_cancellation_and_adoption_progress_with_full_ordinary_lane() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open_configured(dir.path(), WalSyncMode::Immediate, options(1, 4096, 1))
            .unwrap();
        label(&db);
        drop(
            db.submit(vec![WalOp::UpsertNode(node(1))])
                .unwrap()
                .wait()
                .unwrap(),
        );
        let (reply, receiver) = mpsc::sync_channel(1);
        db.runtime.hooks.lock().unwrap().capture = Some(reply);
        db.runtime.lifecycle.wake();
        let job = receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(job._heavy.lane.acquire().is_err());
        let (entered, enter) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        db.runtime.hooks.lock().unwrap().pause = Some((Boundary::BeforeQueue, entered, resumed));
        db.runtime.lifecycle.wake();
        enter.recv_timeout(Duration::from_secs(5)).unwrap();
        let pending: Vec<_> = (2..10)
            .map(|id| {
                db.submit(vec![WalOp::DeleteNode { id, deleted_at: 0 }])
                    .unwrap()
            })
            .collect();
        let (delivery, completion) = db.reserve_adoption(job).unwrap();
        assert_eq!(db.runtime.pressure.occupied(), 8);
        drop(delivery);
        drop(pending);
        resume.send(()).unwrap();
        assert!(matches!(
            completion.wait(),
            Err(Outcome::Refused("adoption canceled"))
        ));
        let stats = db.stats();
        db.close().unwrap();
        drop(db);
        wait_drain(&stats);
    }
}

#[cfg(test)]
mod admission_close_regression {
    use super::*;
    #[test]
    fn p45a3_close_drains_position_that_is_still_preparing_before_enqueue() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open(dir.path(), WalSyncMode::Immediate).unwrap();
        let (entered, enter) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        db.runtime.hooks.lock().unwrap().pause = Some((Boundary::InputAdmission, entered, resumed));
        let submitting = db.clone();
        let submitter = std::thread::spawn(move || {
            submitting.submit(vec![WalOp::DeleteNode {
                id: 1,
                deleted_at: 0,
            }])
        });
        enter.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(db.runtime.pressure.occupied(), 1);
        let closing = db.clone();
        let (done, closed) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            done.send(closing.close()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !db.runtime.lifecycle.close_requested() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(closed.try_recv().is_err());
        resume.send(()).unwrap();
        let result = submitter.join().unwrap().and_then(Pending::wait);
        assert!(matches!(result, Err(Outcome::Refused("closed"))));
        closed
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        closer.join().unwrap();
        let ops = crate::wal::WalReader::new(dir.path()).read_all().unwrap();
        assert!(ops.is_empty());
        let stats = db.stats();
        drop(db);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !stats.read().exited {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod bounded_input_walk_tests {
    use super::*;
    #[test]
    fn p45a3_wire_refusal_stops_before_invalid_deep_suffix_and_leaves_wal_unchanged() {
        let _serial = super::super::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let db = Slice::open_configured(
            dir.path(),
            WalSyncMode::Immediate,
            crate::ConcurrencyOptions::default().with_max_batch_bytes(256),
        )
        .unwrap();
        let mut root = Owned::new(PropValue::Null);
        for _ in 0..4096 {
            let child = std::mem::replace(&mut *root, PropValue::Null);
            *root = PropValue::Array(vec![child]);
        }
        let node = NodeRecord {
            id: 1,
            label_ids: crate::types::NodeLabelSet::single(1).unwrap(),
            key: "key".into(),
            props: BTreeMap::from([
                ("a".into(), PropValue::String("x".repeat(512))),
                ("z".into(), root.into_inner()),
            ]),
            created_at: 0,
            updated_at: 0,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        };
        let before = std::fs::read(crate::wal::wal_generation_path(dir.path(), 0)).unwrap();
        assert!(
            matches!(db.submit(vec![WalOp::UpsertNode(node)]),Err(Outcome::ResourceLimit(r)) if r.resource=="max_batch_bytes"&&!r.retryable)
        );
        assert_eq!(db.runtime.pressure.occupied(), 0);
        assert_eq!(db.snapshot().unwrap().seq(), 0);
        assert_eq!(
            std::fs::read(crate::wal::wal_generation_path(dir.path(), 0)).unwrap(),
            before
        );
        db.close().unwrap();
    }
}
