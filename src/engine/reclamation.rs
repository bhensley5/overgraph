//! Guardian ownership: foreground release touches fixed metadata only. Resource
//! Shared DAG fanout and physical destruction belong to one detached domain worker.
#![allow(dead_code)] // Private substrate; actual locked engine wiring is CP45a.3.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use crate::error::EngineError;

mod telemetry;
use telemetry::Telemetry;

mod configured;
#[allow(unused_imports)]
pub(super) use configured::ConfiguredDomain;

#[cfg(test)]
mod scan_tests;

static DOMAIN_IDS: AtomicU64 = AtomicU64::new(0);
const FREE: u8 = 0;
const RESERVED: u8 = 1;
const LIVE: u8 = 2;
const RETIRED: u8 = 3;
const INSPECTING: u8 = 4;
const DROPPING: u8 = 5;
const CATEGORIES: usize = 6;

fn invariant(message: &str) -> ! {
    eprintln!("production-concurrency reclamation invariant: {message}");
    std::process::abort()
}

fn increment(value: &AtomicU64) -> u64 {
    value
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
        .unwrap_or_else(|_| invariant("monotonic identifier exhausted"))
        + 1
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ResourceClass {
    Generation,
    Sources,
    Catalog,
    Storage,
    Staging,
    Cache,
}

impl ResourceClass {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReclaimError {
    Closed,
    AlreadyFinished,
    Capacity,
}

impl From<ReclaimError> for super::concurrency_lifecycle::Outcome {
    fn from(error: ReclaimError) -> Self {
        use super::concurrency_lifecycle::Outcome;
        match error {
            ReclaimError::Capacity => Outcome::Refused("registry capacity allocation failed"),
            ReclaimError::Closed => Outcome::Refused("closed domain"),
            ReclaimError::AlreadyFinished => Outcome::Refused("finished construction"),
        }
    }
}

struct Status(Option<Arc<StatusInner>>);
struct StatusInner {
    identity: u64,
    #[cfg(test)]
    arc_read_live_releases: AtomicUsize,
    #[cfg(test)]
    arc_read_notifications: AtomicUsize,
    occupied: AtomicUsize,
    producers: AtomicUsize,
    exited: AtomicBool,
    completion_bound: AtomicBool,
    native_complete: AtomicBool,
    registration_id: AtomicU64,
    #[cfg(feature = "production-concurrency")]
    participants: OnceLock<super::participant::ParticipantHandle>,
    telemetry: Telemetry,
}
impl Status {
    fn new() -> Self {
        Self(Some(Arc::new(StatusInner {
            identity: increment(&DOMAIN_IDS),
            #[cfg(test)]
            arc_read_live_releases: AtomicUsize::new(0),
            #[cfg(test)]
            arc_read_notifications: AtomicUsize::new(0),
            occupied: AtomicUsize::new(0),
            producers: AtomicUsize::new(0),
            exited: AtomicBool::new(false),
            completion_bound: AtomicBool::new(false),
            native_complete: AtomicBool::new(false),
            registration_id: AtomicU64::new(0),
            #[cfg(feature = "production-concurrency")]
            participants: OnceLock::new(),
            telemetry: Telemetry::new(),
        })))
    }
}
impl Deref for Status {
    type Target = StatusInner;
    fn deref(&self) -> &StatusInner {
        self.0.as_ref().expect("live status")
    }
}
impl Clone for Status {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live status").clone()))
    }
}
impl Drop for Status {
    fn drop(&mut self) {
        drop(Arc::into_inner(
            self.0.take().expect("status dropped twice"),
        ));
    }
}

enum WorkerNotification {
    Legacy(Thread),
    Governed(super::completion::WorkerWake),
}
impl WorkerNotification {
    fn notify(&self) {
        match self {
            Self::Legacy(thread) => thread.unpark(),
            Self::Governed(wake) => wake.notify(),
        }
    }
}

/// The completion-owned notifier is shared intact in configured workers;
/// cloning Thread outside its sealed native-tail owner is forbidden there.
struct Wake(Option<Arc<WakeInner>>);
struct WakeInner {
    worker: OnceLock<WorkerNotification>,
    epoch: AtomicU64,
    retirement: AtomicU64,
    status: Status,
    closing: AtomicBool,
}

impl Wake {
    fn new() -> Self {
        Self(Some(Arc::new(WakeInner {
            worker: OnceLock::new(),
            epoch: AtomicU64::new(1),
            retirement: AtomicU64::new(0),
            status: Status::new(),
            closing: AtomicBool::new(false),
        })))
    }
    fn belongs_to(&self, other: &Self) -> bool {
        Arc::ptr_eq(
            self.0.as_ref().expect("live wake"),
            other.0.as_ref().expect("live wake"),
        )
    }
    fn notify(&self) {
        increment(&self.epoch);
        if let Some(worker) = self.worker.get() {
            worker.notify();
        }
    }
}
impl Deref for Wake {
    type Target = WakeInner;
    fn deref(&self) -> &WakeInner {
        self.0.as_ref().expect("live wake")
    }
}
impl Clone for Wake {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live wake").clone()))
    }
}
impl Drop for Wake {
    fn drop(&mut self) {
        if let Some(inner) = Arc::into_inner(self.0.take().expect("wake dropped twice")) {
            drop(inner); // Arc free, then notifier/status.
        }
    }
}

struct SlotMeta {
    state: AtomicU8,
    permits: AtomicUsize,
    incarnation: AtomicU64,
    retired_at: AtomicU64,
    failed_epoch: AtomicU64,
    failed_round: AtomicU64,
    class: AtomicUsize,
    retired_us: AtomicU64,
    eligible_us: AtomicU64,
    observation: AtomicU8,
}

trait Guardian: Send {
    // Called ONLY by the worker after extraction, outside every registry/engine
    // lock. Failure leaves exactly the same Arc in this guardian.
    fn try_destroy(&mut self, meta: &SlotMeta, status: &Status) -> bool;
}

struct TypedGuardian<T> {
    cell: Option<Arc<BuildCell<T>>>,
}

impl<T: Send + Sync + 'static> Guardian for TypedGuardian<T> {
    fn try_destroy(&mut self, meta: &SlotMeta, status: &Status) -> bool {
        let arc = self
            .cell
            .take()
            .unwrap_or_else(|| invariant("missing guardian"));
        match Arc::try_unwrap(arc) {
            Err(arc) => {
                self.cell = Some(arc);
                false
            }
            Ok(cell) => {
                status.telemetry.eligible(meta);
                meta.state.store(DROPPING, Ordering::Release);
                status.telemetry.dropping(meta);
                drop(cell); // Includes producing-parent child walk, worker only.
                true
            }
        }
    }
}

// The locked engine retains its existing Arc-backed, interior-mutable graphs.
// Guard the actual Arc allocation: dropping a wrapper/cell is not proof that a
// memtable or mapped segment has no surviving cursor references.
struct ArcGuardian<T> {
    value: Option<Arc<T>>,
}
impl<T: Send + Sync + 'static> Guardian for ArcGuardian<T> {
    fn try_destroy(&mut self, meta: &SlotMeta, status: &Status) -> bool {
        match Arc::try_unwrap(self.value.take().expect("arc guardian")) {
            Err(value) => {
                self.value = Some(value);
                false
            }
            Ok(value) => {
                status.telemetry.eligible(meta);
                meta.state.store(DROPPING, Ordering::Release);
                status.telemetry.dropping(meta);
                drop(value);
                true
            }
        }
    }
}

struct Slot {
    storage_id: Option<u64>,
    component_paths: Vec<(u64, [u8; 32], std::path::PathBuf)>,
    meta: Arc<SlotMeta>,
    category_position: usize,
    guardian: Option<Box<dyn Guardian>>,
}

struct Registry {
    slots: Vec<Slot>,
    occupied: usize,
    indexes: [Vec<usize>; CATEGORIES],
    scan_remaining: [usize; CATEGORIES],
    free: FreeSlots,
    exited: bool,
}

/// One ordinary free-slot inventory, grown with the registry.
struct FreeSlots {
    indexes: Vec<usize>,
}

impl FreeSlots {
    fn new(range: std::ops::Range<usize>) -> Self {
        let mut indexes = Vec::with_capacity(range.len());
        indexes.extend(range.rev());
        Self { indexes }
    }

    fn next(&self) -> Option<usize> {
        self.indexes.last().copied()
    }

    fn claim(&mut self, index: usize) {
        if self.indexes.pop() != Some(index) {
            invariant("changed free-slot selection");
        }
    }

    fn release(&mut self, index: usize) {
        if self.indexes.len() == self.indexes.capacity() {
            invariant("free-slot index overflow");
        }
        self.indexes.push(index);
    }
}

impl Registry {
    fn new(count: usize) -> Self {
        Self {
            slots: (0..count).map(|_| Self::empty_slot()).collect(),
            occupied: 0,
            indexes: std::array::from_fn(|_| Vec::with_capacity(count)),
            scan_remaining: [0; CATEGORIES],
            free: FreeSlots::new(0..count),
            exited: false,
        }
    }
    fn empty_slot() -> Slot {
        Slot {
            storage_id: None,
            component_paths: Vec::new(),
            meta: Arc::new(SlotMeta {
                state: AtomicU8::new(FREE),
                permits: AtomicUsize::new(0),
                incarnation: AtomicU64::new(0),
                retired_at: AtomicU64::new(0),
                failed_epoch: AtomicU64::new(0),
                failed_round: AtomicU64::new(0),
                class: AtomicUsize::new(0),
                retired_us: AtomicU64::new(0),
                eligible_us: AtomicU64::new(0),
                observation: AtomicU8::new(0),
            }),
            category_position: 0,
            guardian: None,
        }
    }
    fn grow(&mut self) -> Result<(), ReclaimError> {
        let old = self.slots.len();
        let next = old.checked_mul(2).ok_or(ReclaimError::Capacity)?;
        self.slots
            .try_reserve_exact(next - old)
            .map_err(|_| ReclaimError::Capacity)?;
        self.free
            .indexes
            .try_reserve_exact(next - self.free.indexes.len())
            .map_err(|_| ReclaimError::Capacity)?;
        for entries in &mut self.indexes {
            entries
                .try_reserve_exact(next - entries.len())
                .map_err(|_| ReclaimError::Capacity)?;
        }
        self.slots.extend((old..next).map(|_| Self::empty_slot()));
        self.free.indexes.extend((old..next).rev());
        Ok(())
    }

    fn begin_scan(&mut self) {
        self.scan_remaining = std::array::from_fn(|class| self.indexes[class].len());
    }

    fn next_scan_entry(&mut self, class: &mut usize) -> Option<usize> {
        while *class < CATEGORIES {
            let remaining = &mut self.scan_remaining[*class];
            if *remaining != 0 {
                *remaining -= 1;
                return Some(self.indexes[*class][*remaining]);
            }
            *class += 1;
        }
        None
    }

    fn scan_complete(&self) -> bool {
        self.scan_remaining.iter().all(|&remaining| remaining == 0)
    }

    fn remove_category_entry(&mut self, class: usize, index: usize) {
        let mut position = self.slots[index].category_position;
        if self.indexes[class].get(position) != Some(&index) {
            invariant("missing category index");
        }
        if position < self.scan_remaining[class] {
            let last = self.scan_remaining[class] - 1;
            // Keep the surviving uninspected entry inside the prefix. An
            // ordinary swap-remove could pull an inspected/new suffix entry in
            // and silently defer this survivor for an arbitrary number of sweeps.
            self.indexes[class].swap(position, last);
            self.slots[self.indexes[class][position]].category_position = position;
            self.slots[index].category_position = last;
            self.scan_remaining[class] = last;
            position = last;
        }
        self.indexes[class].swap_remove(position);
        if let Some(moved) = self.indexes[class].get(position).copied() {
            self.slots[moved].category_position = position;
        }
    }
}

struct DomainInner {
    registry: Mutex<Option<Registry>>,
    wake: Wake,
    #[cfg(test)]
    before_inspection: Mutex<Option<Pause>>,
    #[cfg(test)]
    before_park: Mutex<Option<Pause>>,
    #[cfg(test)]
    after_scan_chunk: Mutex<Option<Pause>>,
    #[cfg(test)]
    after_restoration: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    panic_worker: AtomicBool,
    #[cfg(test)]
    corrupt_restoration: AtomicBool,
}

struct DomainRef(Option<Arc<DomainInner>>);
impl Clone for DomainRef {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live domain").clone()))
    }
}
impl Deref for DomainRef {
    type Target = DomainInner;
    fn deref(&self) -> &DomainInner {
        self.0.as_ref().expect("live domain")
    }
}
impl Drop for DomainRef {
    fn drop(&mut self) {
        drop(Arc::into_inner(
            self.0.take().expect("domain dropped twice"),
        ));
    }
}

pub(crate) struct ReclaimDomain {
    inner: DomainRef,
}

/// No Weak escapes; this fixed handle does not own a graph destructor.
pub(super) struct DomainHandle(Option<Arc<ReclaimDomain>>);
impl DomainHandle {
    fn new(domain: ReclaimDomain) -> Self {
        Self(Some(Arc::new(domain)))
    }
    pub(super) fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(
            self.0.as_ref().expect("live domain"),
            other.0.as_ref().expect("live domain"),
        )
    }
}
impl Clone for DomainHandle {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live domain").clone()))
    }
}
impl Deref for DomainHandle {
    type Target = ReclaimDomain;
    fn deref(&self) -> &ReclaimDomain {
        self.0.as_ref().expect("live domain")
    }
}
impl Drop for DomainHandle {
    fn drop(&mut self) {
        drop(Arc::into_inner(
            self.0.take().expect("domain handle dropped twice"),
        ));
    }
}

/// Fixed session custody participates in the same worker-exit predicate as
/// accepted construction. The directory inventory serializes final close with
/// subsequent attachment; no graph root or allocator-owned buffer lives here.
pub(super) struct SessionProducer {
    wake: Wake,
}

impl Drop for SessionProducer {
    fn drop(&mut self) {
        self.wake
            .status
            .telemetry
            .closing
            .store(true, Ordering::Release);
        self.wake.closing.store(true, Ordering::Release);
        if self.wake.status.producers.fetch_sub(1, Ordering::AcqRel) == 0 {
            invariant("session producer underflow");
        }
        self.wake.notify();
    }
}

#[derive(Clone)]
pub(super) struct ReclamationStatsHandle {
    status: Status,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ReclamationStats {
    pub(super) occupied: usize,
    pub(super) producers: usize,
    pub(super) exited: bool,
}

#[cfg(test)]
impl ReclamationStatsHandle {
    pub(super) fn reset_arc_read_observer(&self) {
        self.status
            .arc_read_live_releases
            .store(0, Ordering::Relaxed);
        self.status
            .arc_read_notifications
            .store(0, Ordering::Relaxed);
    }
    pub(super) fn arc_read_observations(&self) -> (usize, usize) {
        (
            self.status.arc_read_live_releases.load(Ordering::Relaxed),
            self.status.arc_read_notifications.load(Ordering::Relaxed),
        )
    }
}
impl ReclamationStatsHandle {
    pub(super) fn read(&self) -> ReclamationStats {
        ReclamationStats {
            occupied: self.status.occupied.load(Ordering::Acquire),
            producers: self.status.producers.load(Ordering::Acquire),
            exited: self.status.exited.load(Ordering::Acquire),
        }
    }

    pub(super) fn read_full(&self) -> Result<crate::ReclamationStats, EngineError> {
        use crate::types::{ReclamationCloseState, ReclamationHistogram};
        let mut snapshot = crate::ReclamationStats {
            registered_objects: self.status.occupied.load(Ordering::Acquire) as u64,
            retired_objects: 0,
            dropping_objects: 0,
            active_generations: 0,
            frozen_generations: 0,
            leased_participants: 0,
            participant_limit: 0,
            #[cfg(feature = "production-concurrency")]
            process_participants: super::participant::process_participants(),
            #[cfg(not(feature = "production-concurrency"))]
            process_participants: 0,
            active_sessions: 0,
            draining_sessions: 0,
            close_state: if self.status.exited.load(Ordering::Acquire)
                && (!self.status.completion_bound.load(Ordering::Acquire)
                    || self.status.native_complete.load(Ordering::Acquire))
            {
                ReclamationCloseState::Drained
            } else if self.status.telemetry.closing.load(Ordering::Acquire) {
                ReclamationCloseState::Draining
            } else {
                ReclamationCloseState::Open
            },
            oldest_retirement_age_us: None,
            scan_lag_us: None,
            release_to_eligibility_us: ReclamationHistogram::empty(),
            eligibility_to_drop_us: ReclamationHistogram::empty(),
            worker_wall_us: None,
            worker_cpu_us: None,
        };
        self.status.telemetry.fill(&mut snapshot);
        #[cfg(feature = "production-concurrency")]
        if let Some(participants) = self.status.participants.get() {
            (snapshot.leased_participants, snapshot.participant_limit) = participants.snapshot();
        }
        Ok(snapshot)
    }

    #[cfg(feature = "production-concurrency")]
    pub(super) fn bind_participants(
        &self,
        participants: &super::participant::ParticipantHandle,
    ) -> Result<(), EngineError> {
        if !participants.belongs_to(&DomainIdentity(self.status.identity)) {
            return Err(EngineError::InvalidOperation(
                "foreign participant stats binding".into(),
            ));
        }
        match self.status.participants.set(participants.clone()) {
            Ok(()) => Ok(()),
            Err(rejected)
                if self
                    .status
                    .participants
                    .get()
                    .is_some_and(|existing| existing.same_admission(&rejected)) =>
            {
                Ok(())
            }
            Err(_) => Err(EngineError::InvalidOperation(
                "participant stats already bound to another admission".into(),
            )),
        }
    }

    #[cfg(feature = "production-concurrency")]
    pub(super) fn participant_handle(&self) -> Option<super::participant::ParticipantHandle> {
        self.status.participants.get().cloned()
    }

    pub(super) fn bind_native_completion(&self) {
        self.status.completion_bound.store(true, Ordering::Release);
    }
    pub(super) fn native_completed(&self) {
        self.status.native_complete.store(true, Ordering::Release);
    }

    pub(super) fn set_generations(&self, active: u64, frozen: u64) {
        self.status
            .telemetry
            .active_generations
            .store(active, Ordering::Release);
        self.status
            .telemetry
            .frozen_generations
            .store(frozen, Ordering::Release);
    }
    pub(super) fn set_participants(&self, leased: u64, limit: u64) {
        self.status
            .telemetry
            .leased_participants
            .store(leased, Ordering::Release);
        self.status
            .telemetry
            .participant_limit
            .store(limit, Ordering::Release);
    }
    pub(super) fn set_sessions(&self, active: u64, draining: u64) {
        self.status
            .telemetry
            .active_sessions
            .store(active, Ordering::Release);
        self.status
            .telemetry
            .draining_sessions
            .store(draining, Ordering::Release);
    }
    pub(super) fn shutdown_requested(&self) {
        self.status.telemetry.closing.store(true, Ordering::Release);
    }

    pub(super) fn set_active_sessions(&self, active: u64) {
        self.status
            .telemetry
            .active_sessions
            .store(active, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(super) struct DomainIdentity(u64);
impl DomainIdentity {
    pub(super) fn same_identity(&self, other: &Self) -> bool {
        self.0 == other.0
    }
    pub(super) fn belongs_to(&self, domain: &ReclaimDomain) -> bool {
        self.0 == domain.inner.wake.status.identity
    }
}
impl ReclaimDomain {
    pub(super) fn identity(&self) -> DomainIdentity {
        DomainIdentity(self.inner.wake.status.identity)
    }

    pub(super) fn inner_registration_id(&self) -> &AtomicU64 {
        &self.inner.wake.status.registration_id
    }

    pub(super) fn try_attach_session(&self) -> Result<SessionProducer, EngineError> {
        let held = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|_| invariant("registry poisoned"));
        let registry = held.as_ref().ok_or_else(|| {
            EngineError::InvalidOperation("directory domain already drained".into())
        })?;
        if registry.exited {
            return Err(EngineError::InvalidOperation(
                "directory domain exit committed".into(),
            ));
        }
        self.inner
            .wake
            .status
            .producers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .unwrap_or_else(|_| invariant("session producer count exhausted"));
        self.inner.wake.closing.store(false, Ordering::Release);
        self.inner
            .wake
            .status
            .telemetry
            .closing
            .store(false, Ordering::Release);
        Ok(SessionProducer {
            wake: self.inner.wake.clone(),
        })
    }

    pub(super) fn new() -> Self {
        let inner = DomainRef(Some(Arc::new(DomainInner {
            registry: Mutex::new(Some(Registry::new(64))),
            wake: Wake::new(),
            #[cfg(test)]
            before_inspection: Mutex::new(None),
            #[cfg(test)]
            before_park: Mutex::new(None),
            #[cfg(test)]
            after_scan_chunk: Mutex::new(None),
            #[cfg(test)]
            after_restoration: Mutex::new(None),
            #[cfg(test)]
            panic_worker: AtomicBool::new(false),
            #[cfg(test)]
            corrupt_restoration: AtomicBool::new(false),
        })));
        // Pinned Apple OnceBox initializes one 64-byte native Mutex backing.
        // Do it while unpublished, avoiding competing first-lock allocations.
        drop(inner.registry.lock().unwrap());
        let worker = inner.clone();
        super::worker::builder("overgraph-reclaimer")
            .spawn(move || {
                worker
                    .wake
                    .worker
                    .set(WorkerNotification::Legacy(thread::current()))
                    .unwrap_or_else(|_| invariant("second worker"));
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker_loop(&worker)))
                    .is_err()
                {
                    invariant("worker or physical destructor panicked");
                }
            })
            .expect("spawn domain reclaimer"); // Detached: no finalizer ever joins.
        Self { inner }
    }

    pub(super) fn stats(&self) -> ReclamationStatsHandle {
        ReclamationStatsHandle {
            status: self.inner.wake.status.clone(),
        }
    }

    pub(super) fn close(&self) {
        // Reservation holds the registry mutex from checking this flag through
        // recording Reserved/occupied. Worker tests exit under that mutex. A
        // reservation that observed false is therefore visible before exit;
        // every later reservation observes closing. Final release needs no lock.
        self.inner.wake.closing.store(true, Ordering::Release);
        self.inner
            .wake
            .status
            .telemetry
            .closing
            .store(true, Ordering::Release);
        self.inner.wake.notify();
    }

    pub(super) fn build<T: Default + Send + Sync + 'static>(
        &self,
        class: ResourceClass,
    ) -> Result<BuildPermit<T>, ReclaimError> {
        // Private construction is ordinary RAII; registration precedes shared exposure.
        let value = T::default();
        let mut reservation = self.reserve(class)?;
        let cell = Arc::new(BuildCell {
            building: Mutex::new(Some(value)),
            finished: OnceLock::new(),
        });
        let guardian = Box::new(TypedGuardian {
            cell: Some(cell.clone()),
        });
        let managed = Managed {
            cell: Some(cell),
            wake: self.inner.wake.clone(),
        };
        let owner = reservation.register(guardian, managed);
        Ok(BuildPermit { owner: Some(owner) })
    }
    pub(super) fn pin_storage(&self, owner: &OwnerPermit<()>, id: u64) -> Result<(), ReclaimError> {
        if !owner.belongs_to(self) {
            return Err(ReclaimError::Closed);
        }
        let mut held = self.inner.registry.lock().unwrap();
        let registry = held.as_mut().ok_or(ReclaimError::Closed)?;
        let slot = &mut registry.slots[owner.index];
        if slot.meta.incarnation.load(Ordering::Acquire) != owner.incarnation {
            return Err(ReclaimError::Closed);
        }
        slot.storage_id = Some(id);
        Ok(())
    }
    pub(super) fn pin_components(
        &self,
        owner: &OwnerPermit<()>,
        paths: Vec<(u64, [u8; 32], std::path::PathBuf)>,
    ) -> Result<(), ReclaimError> {
        if !owner.belongs_to(self) {
            return Err(ReclaimError::Closed);
        }
        let mut held = self.inner.registry.lock().unwrap();
        let registry = held.as_mut().ok_or(ReclaimError::Closed)?;
        let slot = &mut registry.slots[owner.index];
        if slot.meta.incarnation.load(Ordering::Acquire) != owner.incarnation {
            return Err(ReclaimError::Closed);
        }
        slot.component_paths = paths;
        Ok(())
    }
    pub(super) fn component_pins(
        &self,
    ) -> std::collections::HashSet<(u64, [u8; 32], std::path::PathBuf)> {
        let held = self.inner.registry.lock().unwrap();
        held.as_ref()
            .map_or_else(std::collections::HashSet::new, |registry| {
                registry.indexes[ResourceClass::Storage.index()]
                    .iter()
                    .flat_map(|&i| registry.slots[i].component_paths.iter().cloned())
                    .collect()
            })
    }
    pub(super) fn storage_pins(&self) -> crate::types::NodeIdSet {
        let held = self.inner.registry.lock().unwrap();
        held.as_ref()
            .map_or_else(crate::types::NodeIdSet::default, |registry| {
                registry.indexes[ResourceClass::Storage.index()]
                    .iter()
                    .filter_map(|&index| registry.slots[index].storage_id)
                    .collect()
            })
    }
    pub(super) fn guard_arc<T: Send + Sync + 'static>(
        &self,
        value: Arc<T>,
        class: ResourceClass,
    ) -> Result<OwnerPermit<()>, ReclaimError> {
        let mut reservation = self.reserve(class)?;
        let finished = OnceLock::new();
        finished.set(()).expect("fresh guard descriptor");
        let managed = Managed {
            cell: Some(Arc::new(BuildCell {
                building: Mutex::new(None),
                finished,
            })),
            wake: self.inner.wake.clone(),
        };
        Ok(reservation.register(Box::new(ArcGuardian { value: Some(value) }), managed))
    }
    fn reserve(&self, class: ResourceClass) -> Result<Reservation<'_>, ReclaimError> {
        let mut held = self.inner.registry.lock().unwrap();
        let registry = held.as_mut().ok_or(ReclaimError::Closed)?;
        if registry.exited || self.inner.wake.closing.load(Ordering::Acquire) {
            return Err(ReclaimError::Closed);
        }
        if registry.free.next().is_none() {
            registry.grow()?;
        }
        let index = registry.free.next().expect("grown free inventory");
        let meta = registry.slots[index].meta.clone();
        let incarnation = meta
            .incarnation
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or(ReclaimError::Capacity)?;
        registry.free.claim(index);
        meta.incarnation.store(incarnation, Ordering::Release);
        meta.class.store(class.index(), Ordering::Relaxed);
        meta.failed_epoch.store(0, Ordering::Relaxed);
        meta.failed_round.store(0, Ordering::Relaxed);
        meta.observation.store(0, Ordering::Relaxed);
        meta.state.store(RESERVED, Ordering::Release);
        registry.slots[index].category_position = registry.indexes[class.index()].len();
        registry.indexes[class.index()].push(index);
        registry.occupied += 1;
        self.inner
            .wake
            .status
            .occupied
            .store(registry.occupied, Ordering::Release);
        Ok(Reservation {
            domain: &self.inner,
            index,
            meta: Some(meta),
            incarnation,
            registered: false,
        })
    }
}

impl Drop for ReclaimDomain {
    fn drop(&mut self) {
        self.close();
    }
}

struct Reservation<'a> {
    domain: &'a DomainInner,
    index: usize,
    meta: Option<Arc<SlotMeta>>,
    incarnation: u64,
    registered: bool,
}

impl Reservation<'_> {
    fn register<T>(&mut self, guardian: Box<dyn Guardian>, managed: Managed<T>) -> OwnerPermit<T> {
        let mut held = self.domain.registry.lock().unwrap();
        let registry = held
            .as_mut()
            .unwrap_or_else(|| invariant("registration after registry exit"));
        let slot = &mut registry.slots[self.index];
        let meta = self.meta.as_ref().expect("reservation metadata");
        if meta.state.load(Ordering::Acquire) != RESERVED || slot.guardian.is_some() {
            invariant("invalid registration");
        }
        slot.guardian = Some(guardian);
        meta.permits.store(1, Ordering::Release);
        self.domain
            .wake
            .status
            .producers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .unwrap_or_else(|_| invariant("domain producer count exhausted"));
        meta.state.store(LIVE, Ordering::Release);
        self.registered = true;
        OwnerPermit {
            managed: Some(managed),
            meta: self.meta.take(),
            incarnation: self.incarnation,
            index: self.index,
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.registered {
            let mut held = self.domain.registry.lock().unwrap();
            let registry = held
                .as_mut()
                .unwrap_or_else(|| invariant("reservation after registry exit"));
            if self
                .meta
                .as_ref()
                .expect("reservation metadata")
                .state
                .load(Ordering::Acquire)
                != RESERVED
            {
                invariant("impossible reservation restoration");
            }
            // Complete this token's physical metadata release under the lock,
            // before publishing a free slot or allowing registry teardown.
            drop(self.meta.take());
            free_slot(registry, self.index, &self.domain.wake);
        }
    }
}

struct BuildCell<T> {
    building: Mutex<Option<T>>,
    finished: OnceLock<T>,
}

pub(super) struct Managed<T> {
    cell: Option<Arc<BuildCell<T>>>,
    wake: Wake,
}

impl<T> Managed<T> {
    fn cell(&self) -> &BuildCell<T> {
        self.cell.as_ref().expect("live managed handle")
    }
    pub(super) fn get(&self) -> &T {
        self.cell()
            .finished
            .get()
            .expect("finished managed resource")
    }
}

impl<T> Clone for Managed<T> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell.clone(),
            wake: self.wake.clone(),
        }
    }
}

impl<T> Drop for Managed<T> {
    fn drop(&mut self) {
        drop(self.cell.take());
        self.wake.notify();
    }
}

// Passive custody for the locked engine's actual Arc guardian. Live-reader
// releases cannot make a live producer reclaimable and need no kernel wake.
// Final producer retirement always wakes; retired/inspecting reader releases
// wake after dropping the payload Arc, preserving both orders of that race.
pub(super) struct ArcReadPermit {
    meta: Arc<SlotMeta>,
    wake: Wake,
    incarnation: u64,
}
impl ArcReadPermit {
    pub(super) fn belongs_to(&self, domain: &ReclaimDomain) -> bool {
        self.wake.belongs_to(&domain.inner.wake)
    }
}
impl Clone for ArcReadPermit {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            wake: self.wake.clone(),
            incarnation: self.incarnation,
        }
    }
}
impl Drop for ArcReadPermit {
    fn drop(&mut self) {
        if self.meta.incarnation.load(Ordering::Acquire) == self.incarnation
            && self.meta.state.fetch_or(0, Ordering::AcqRel) >= RETIRED
        {
            #[cfg(test)]
            self.wake
                .status
                .arc_read_notifications
                .fetch_add(1, Ordering::Relaxed);
            self.wake.notify();
        } else {
            #[cfg(test)]
            self.wake
                .status
                .arc_read_live_releases
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(super) struct OwnerPermit<T> {
    managed: Option<Managed<T>>,
    meta: Option<Arc<SlotMeta>>,
    incarnation: u64,
    index: usize,
}

impl<T> OwnerPermit<T> {
    pub(super) fn belongs_to(&self, domain: &ReclaimDomain) -> bool {
        self.managed().wake.belongs_to(&domain.inner.wake)
    }

    fn managed(&self) -> &Managed<T> {
        self.managed.as_ref().expect("live producer")
    }

    pub(super) fn arc_read_permit(&self) -> ArcReadPermit {
        ArcReadPermit {
            meta: self.meta.as_ref().expect("producer metadata").clone(),
            wake: self.managed().wake.clone(),
            incarnation: self.incarnation,
        }
    }
    pub(super) fn read(&self) -> Managed<T> {
        self.managed().clone()
    }
    pub(super) fn get(&self) -> &T {
        self.managed().get()
    }
    pub(super) fn fork(&self) -> Self {
        let meta = self.meta.as_ref().expect("producer metadata");
        if meta.incarnation.load(Ordering::Acquire) != self.incarnation
            || meta.state.load(Ordering::Acquire) != LIVE
        {
            invariant("producer resurrection or stale incarnation");
        }
        meta.permits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n == 0 {
                    None
                } else {
                    n.checked_add(1)
                }
            })
            .unwrap_or_else(|_| invariant("permit count exhausted"));
        self.managed()
            .wake
            .status
            .producers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .unwrap_or_else(|_| invariant("domain producer count exhausted"));
        Self {
            managed: Some(self.read()),
            meta: Some(meta.clone()),
            incarnation: self.incarnation,
            index: self.index,
        }
    }
}

impl<T> Drop for OwnerPermit<T> {
    fn drop(&mut self) {
        let managed = self
            .managed
            .take()
            .unwrap_or_else(|| invariant("missing producer reference"));
        let meta = self
            .meta
            .take()
            .unwrap_or_else(|| invariant("missing producer metadata"));
        if meta.incarnation.load(Ordering::Acquire) != self.incarnation {
            invariant("stale producer slot");
        }
        let old = meta.permits.fetch_sub(1, Ordering::AcqRel);
        if old == 0 {
            invariant("permit underflow");
        }
        if old == 1 {
            managed.wake.status.telemetry.retired(&meta);
            meta.retired_at
                .store(increment(&managed.wake.retirement), Ordering::Relaxed);
            meta.state
                .compare_exchange(LIVE, RETIRED, Ordering::AcqRel, Ordering::Acquire)
                .unwrap_or_else(|_| invariant("invalid final producer retirement"));
        }
        drop(meta); // No SlotMeta handle remains after final producer publication.
        managed.wake.status.producers.fetch_sub(1, Ordering::AcqRel);
        drop(managed);
    }
}

pub(super) struct BuildPermit<T> {
    owner: Option<OwnerPermit<T>>,
}

impl<T> BuildPermit<T> {
    pub(super) fn belongs_to(&self, domain: &ReclaimDomain) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.managed().wake.belongs_to(&domain.inner.wake))
    }

    pub(super) fn mutate<R>(&mut self, build: impl FnOnce(&mut T) -> R) -> Result<R, ReclaimError> {
        let cell = self.owner.as_ref().expect("build owner").managed().cell();
        if cell.finished.get().is_some() {
            return Err(ReclaimError::AlreadyFinished);
        }
        let mut value = cell.building.lock().unwrap_or_else(|e| e.into_inner());
        Ok(build(value.as_mut().expect("initialized build shell")))
    }
    pub(super) fn finish(&mut self) -> Result<(), ReclaimError> {
        let cell = self.owner.as_ref().expect("build owner").managed().cell();
        let mut building = cell.building.lock().unwrap_or_else(|e| e.into_inner());
        if cell.finished.get().is_some() {
            return Err(ReclaimError::AlreadyFinished);
        }
        let value = building
            .take()
            .unwrap_or_else(|| invariant("missing construction value"));
        if let Err(value) = cell.finished.set(value) {
            *building = Some(value); // exact ownership restored before handling
            invariant("rejected finish restoration");
        }
        Ok(())
    }
    pub(super) fn into_owner(mut self) -> Result<OwnerPermit<T>, ReclaimError> {
        self.finish()?;
        Ok(self.owner.take().expect("finished producer"))
    }
}

fn free_slot(registry: &mut Registry, index: usize, wake: &Wake) {
    let slot = &mut registry.slots[index];
    slot.storage_id = None;
    slot.component_paths.clear();
    if slot.meta.state.load(Ordering::Acquire) == FREE || slot.guardian.is_some() {
        invariant("duplicate or premature free-slot return");
    }
    let class = slot.meta.class.load(Ordering::Relaxed);
    wake.status.telemetry.freed(
        &slot.meta,
        slot.meta.state.load(Ordering::Acquire) == DROPPING,
    );
    // Cell, guardian Box and all ephemeral metadata tokens have already freed.
    slot.meta.state.store(FREE, Ordering::Release);
    registry.occupied -= 1;
    registry.remove_category_entry(class, index);
    registry.free.release(index);
    wake.status
        .occupied
        .store(registry.occupied, Ordering::Release);
    wake.notify(); // ONLY after physical destructor and guardian shell are done
}

fn worker_loop(domain: &DomainInner) {
    // A full sweep chooses oldest not-yet-inspected retirement in each category.
    // A failed pinned guardian is skipped until a new release epoch; it cannot
    // starve unpinned siblings. Each chunk checks <=256 occupied entries / 1ms,
    // one heavy drop per sweep. Unused slot capacity adds no inspection work.
    let mut scan = None;
    let mut sweep_epoch = domain.wake.epoch.load(Ordering::Acquire);
    let mut category = 0;
    let mut candidates: [Option<(u64, usize)>; CATEGORIES] = [None; CATEGORIES];
    let mut round = 1u64;
    let mut round_epoch = domain.wake.epoch.load(Ordering::Acquire);
    let mut saw_failed = false;
    let mut oldest_retired = None;
    loop {
        #[cfg(test)]
        if domain.panic_worker.swap(false, Ordering::AcqRel) {
            panic!("named unexpected worker failure");
        }
        let epoch = domain.wake.epoch.load(Ordering::Acquire);
        let start = Instant::now();
        let mut extracted = None;
        let mut retry_round = false;
        #[cfg(test)]
        let scanned_entries;
        {
            let mut held = domain.registry.lock().unwrap();
            let registry = held
                .as_mut()
                .unwrap_or_else(|| invariant("worker registry missing"));
            if domain.wake.closing.load(Ordering::Acquire)
                && registry.occupied == 0
                && domain.wake.status.producers.load(Ordering::Acquire) == 0
            {
                // Detach the variable registry/index fanout before publication of
                // exit. A surviving domain/status wrapper owns bounded metadata.
                let registry = held.take();
                drop(held);
                drop(registry); // actual registry destruction ONLY on worker
                                // A close can arrive between bounded scan chunks. Final stats
                                // must not retain an unfinished scan or miss this cleanup work.
                domain.wake.status.telemetry.finish_scan(None);
                domain.wake.status.telemetry.worked(start);
                domain.wake.status.exited.store(true, Ordering::Release);
                return;
            }
            if scan.is_none() {
                registry.begin_scan();
                scan = Some(0);
                sweep_epoch = domain.wake.epoch.load(Ordering::Acquire);
                domain.wake.status.telemetry.begin_scan();
                oldest_retired = None;
            }
            let mut checks = 0;
            while checks < 256 && start.elapsed() < Duration::from_millis(1) {
                let Some(index) = registry.next_scan_entry(scan.as_mut().unwrap()) else {
                    break;
                };
                let meta = &registry.slots[index].meta;
                if meta.state.load(Ordering::Acquire) >= RETIRED {
                    let retired = meta.retired_us.load(Ordering::Relaxed);
                    oldest_retired =
                        Some(oldest_retired.map_or(retired, |old: u64| old.min(retired)));
                }
                if meta.state.load(Ordering::Acquire) == RETIRED
                    && meta.failed_round.load(Ordering::Acquire) != 0
                {
                    saw_failed = true;
                }
                if meta.state.load(Ordering::Acquire) == RETIRED
                    && meta.failed_round.load(Ordering::Acquire) != round
                    && meta.failed_epoch.load(Ordering::Acquire) != sweep_epoch
                {
                    let order = meta.retired_at.load(Ordering::Acquire);
                    let class = meta.class.load(Ordering::Relaxed);
                    if candidates[class].is_none_or(|(best, _)| order < best) {
                        candidates[class] = Some((order, index));
                    }
                }
                checks += 1;
            }
            #[cfg(test)]
            {
                scanned_entries = checks;
            }
            if registry.scan_complete() {
                domain.wake.status.telemetry.finish_scan(oldest_retired);
                for offset in 0..CATEGORIES {
                    let class = (category + offset) % CATEGORIES;
                    if let Some((order, index)) = candidates[class].take() {
                        let slot = &mut registry.slots[index];
                        if slot.meta.state.load(Ordering::Acquire) != RETIRED
                            || slot.meta.retired_at.load(Ordering::Acquire) != order
                        {
                            continue;
                        }
                        slot.meta.state.store(INSPECTING, Ordering::Release);
                        extracted = Some((
                            index,
                            slot.meta.clone(),
                            slot.guardian
                                .take()
                                .unwrap_or_else(|| invariant("missing extracted guardian")),
                        ));
                        category = (class + 1) % CATEGORIES;
                        break;
                    }
                }
                scan = None;
                candidates = [None; CATEGORIES];
                // Unrelated clone/release wake churn cannot keep selecting the
                // same pinned heads ahead of uninspected siblings. Retry only
                // after the entire round's eligible set has had its turn.
                let completed_epoch = domain.wake.epoch.load(Ordering::Acquire);
                if extracted.is_none() && saw_failed && completed_epoch != round_epoch {
                    round = round
                        .checked_add(1)
                        .unwrap_or_else(|| invariant("reclaimer round exhausted"));
                    round_epoch = completed_epoch;
                    retry_round = true;
                }
                // A live entry may have retired after inspection, or a new/reused
                // suffix entry may have retired between chunks. Give it a fresh
                // sweep before parking; the final wake check covers later races.
                retry_round |= completed_epoch != sweep_epoch;
                saw_failed = false;
            }
        }
        if let Some((index, meta, mut guardian)) = extracted {
            #[cfg(test)]
            if let Some(pause) = domain.before_inspection.lock().unwrap().take() {
                pause.wait();
            }
            if guardian.try_destroy(&meta, &domain.wake.status) {
                drop(guardian);
                let mut held = domain.registry.lock().unwrap();
                let registry = held
                    .as_mut()
                    .unwrap_or_else(|| invariant("drop after registry exit"));
                if meta.state.load(Ordering::Acquire) != DROPPING {
                    invariant("impossible dropped-slot restoration");
                }
                drop(meta); // Complete the worker's metadata token before reuse.
                free_slot(registry, index, &domain.wake);
            } else {
                let mut held = domain.registry.lock().unwrap();
                let registry = held
                    .as_mut()
                    .unwrap_or_else(|| invariant("restoration after registry exit"));
                let slot = &mut registry.slots[index];
                #[cfg(test)]
                if domain.corrupt_restoration.swap(false, Ordering::AcqRel) {
                    meta.state.store(LIVE, Ordering::Release);
                }
                if meta.state.load(Ordering::Acquire) != INSPECTING || slot.guardian.is_some() {
                    invariant("impossible guardian restoration");
                }
                slot.guardian = Some(guardian);
                domain.wake.status.telemetry.pinned(&meta);
                // Record the epoch from BEFORE unwrap: a racing release after
                // failed unwrap must make this slot eligible for another try.
                meta.failed_epoch.store(epoch, Ordering::Release);
                meta.failed_round.store(round, Ordering::Release);
                meta.state.store(RETIRED, Ordering::Release);
                #[cfg(test)]
                if let Some(notify) = domain.after_restoration.lock().unwrap().take() {
                    notify.send(()).unwrap();
                }
            }
            domain.wake.status.telemetry.worked(start);
            thread::yield_now();
            continue;
        }
        if scan.is_some() {
            #[cfg(test)]
            if scanned_entries != 0 {
                if let Some(pause) = domain.after_scan_chunk.lock().unwrap().take() {
                    pause.wait();
                }
            }
            domain.wake.status.telemetry.worked(start);
            thread::yield_now();
            continue;
        }
        if retry_round {
            domain.wake.status.telemetry.worked(start);
            thread::yield_now();
            continue;
        }
        domain.wake.status.telemetry.worked(start);
        #[cfg(test)]
        if let Some(pause) = domain.before_park.lock().unwrap().take() {
            pause.wait();
        }
        // unpark's retained token handles release-before-park/check-vs-wait;
        // idle recheck is a safety net, not the synchronization proof.
        if domain.wake.epoch.load(Ordering::Acquire) == epoch {
            thread::park_timeout(Duration::from_millis(10));
        }
    }
}

#[cfg(test)]
struct Pause {
    ready: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}
#[cfg(test)]
impl Pause {
    fn wait(self) {
        self.ready.send(()).unwrap();
        self.resume.recv().unwrap();
    }
}

#[cfg(test)]
mod practical_lifetime_tests {
    use super::*;
    #[derive(Default)]
    struct Graph {
        data: Vec<u8>,
        notice: Option<std::sync::mpsc::Sender<String>>,
    }
    impl Drop for Graph {
        fn drop(&mut self) {
            if let Some(notice) = self.notice.take() {
                let _ = notice.send(thread::current().name().unwrap_or("unnamed").into());
            }
        }
    }
    #[test]
    fn p45a3_guardian_large_shared_pin_survives_close_and_drops_outside_registry_lock() {
        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let (notice, received) = std::sync::mpsc::channel();
        let mut build = domain.build::<Graph>(ResourceClass::Generation).unwrap();
        build
            .mutate(|g| {
                g.data.resize(8 * 1024 * 1024, 7);
                g.notice = Some(notice);
            })
            .unwrap();
        let owner = build.into_owner().unwrap();
        let pin = owner.read();
        drop(owner);
        domain.close();
        assert!(!stats.read().exited);
        assert_eq!(pin.get().data.len(), 8 * 1024 * 1024);
        drop(pin);
        assert_eq!(
            received.recv_timeout(Duration::from_secs(5)).unwrap(),
            "overgraph-reclaimer"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !stats.read().exited {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert_eq!(stats.read_full().unwrap().registered_objects, 0);
        assert_eq!(stats.read_full().unwrap().retired_objects, 0);
        assert_eq!(stats.read_full().unwrap().worker_cpu_us, None);
    }
}

#[cfg(test)]
mod shared_partial_unwind_tests {
    use super::*;
    #[derive(Default)]
    struct Partial {
        value: super::super::payload::Owned<crate::types::PropValue>,
        domain: Option<DomainRef>,
        done: Option<std::sync::mpsc::Sender<(String, bool)>>,
    }
    impl Drop for Partial {
        fn drop(&mut self) {
            if let Some(done) = self.done.take() {
                let unlocked = self.domain.as_ref().unwrap().registry.try_lock().is_ok();
                let _ = done.send((
                    thread::current().name().unwrap_or("unnamed").into(),
                    unlocked,
                ));
            }
        }
    }
    #[test]
    fn p45a3_shared_partial_unwind_retires_and_destroys_without_registry_mutex() {
        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let (done, finished) = std::sync::mpsc::channel();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut build = domain.build::<Partial>(ResourceClass::Cache).unwrap();
            build
                .mutate(|partial| {
                    partial.domain = Some(domain.inner.clone());
                    partial.done = Some(done);
                    for _ in 0..4096 {
                        let old =
                            std::mem::replace(&mut *partial.value, crate::types::PropValue::Null);
                        *partial.value = crate::types::PropValue::Array(vec![old]);
                    }
                    panic!("fixture shared construction unwind");
                })
                .unwrap();
        }));
        assert!(result.is_err());
        domain.close();
        assert_eq!(
            finished.recv_timeout(Duration::from_secs(5)).unwrap(),
            ("overgraph-reclaimer".into(), true)
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !stats.read().exited {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
    }
}
