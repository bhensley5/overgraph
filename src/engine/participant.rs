//! Private, fixed-inventory collector boundary. No library ownership escapes.
//!
//! This module is deliberately the only engine module that can name a Guard or
//! LocalHandle. Sessions lease permanent participants; snapshot retention is a
//! separate protocol. The collector never receives deferred engine garbage.
#![allow(dead_code)] // CP45a.1 proof substrate; default engine integration is a3.

use crossbeam_epoch::{Collector, Guard, LocalHandle};
use crossbeam_skiplist::base::{RefEntry, SkipList};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, TryLockError};

const PARTICIPANTS: usize = 256;
struct PoolOwner {
    value: OnceLock<ParticipantPool>,
    initializing: AtomicBool,
}

static POOL: PoolOwner = PoolOwner {
    value: OnceLock::new(),
    initializing: AtomicBool::new(false),
};

struct InitializationOwner<'a>(&'a AtomicBool);
impl Drop for InitializationOwner<'_> {
    fn drop(&mut self) {
        // Unpublished collector/handle prefix cleanup finishes in initialize()
        // before unwinding reaches this owner. No retry overlaps physical cleanup.
        self.0.store(false, Ordering::Release);
    }
}

fn invariant(message: &str) -> ! {
    eprintln!("production-concurrency invariant: {message}");
    std::process::abort()
}

struct ExclusiveParticipant(LocalHandle);

// SAFETY: exact crossbeam-epoch 0.9.18 src/internal.rs Local::register/pin/
// unpin/release_handle and Global::try_advance were audited. Local's non-atomic
// bag, guard_count, handle_count, pin_count and collector UnsafeCells are used
// only by the current slot Mutex owner. Other participants read only atomic
// entry.next/epoch and shared Global's synchronized list/queue/epoch. Handle and
// guard operations have no thread identity or TLS dependency. Mutex unlock/lock
// synchronizes successive owners on different threads. Our private Guard is
// destroyed before unlock, including error/unwind, and these permanent handles
// never finalize during process life. Empty bags and a no-unlink map prevent
// deferred closures (including non-Send payloads) from entering these Cells.
// No Sync, raw access, registration API, or Guard capability is exposed.
unsafe impl Send for ExclusiveParticipant {}

struct ParticipantPool {
    collector: Collector,
    slots: [Mutex<ExclusiveParticipant>; PARTICIPANTS],
    ticket: AtomicUsize,
    // One process inventory, reused after participant leases exit.
}

impl ParticipantPool {
    fn ready() -> &'static Self {
        POOL.value
            .get()
            .unwrap_or_else(|| invariant("participant pool used before admission"))
    }

    fn try_global() -> Result<&'static Self, super::concurrency_lifecycle::Outcome> {
        use super::concurrency_lifecycle::{Outcome, ResourceLimit};
        if let Some(pool) = POOL.value.get() {
            return Ok(pool);
        }
        // Validate the audited std layout before any collector allocation. This
        // identity is not a final JSON stack/build-context certificate.
        if POOL
            .initializing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Outcome::ResourceLimit(ResourceLimit {
                resource: "participants",
                requested: 1,
                limit: PARTICIPANTS,
                retryable: true,
            }));
        }
        let _owner = InitializationOwner(&POOL.initializing);
        // A preceding initializer may have published between the first get and
        // this CAS. Recheck while exclusively owning initialization authority.
        if let Some(pool) = POOL.value.get() {
            return Ok(pool);
        }
        let pool = Self::initialize();
        if POOL.value.set(pool).is_err() {
            invariant("second participant pool publication");
        }
        Ok(Self::ready())
    }

    fn initialize() -> Self {
        let collector = Collector::new();
        let slots = std::array::from_fn(|index| {
            initialization_test_hook(index);
            let slot = Mutex::new(ExclusiveParticipant(collector.register()));
            // Eager first lock avoids lazy backing/loser allocations after
            // exposure. This inventory lasts until process termination.
            drop(slot.lock().expect("new participant mutex"));
            slot
        });
        initialization_test_hook(PARTICIPANTS);
        Self {
            collector,
            slots,
            ticket: AtomicUsize::new(0),
        }
    }
}

pub(crate) struct ParticipantAdmission {
    limit: usize,
    leased: AtomicUsize,
    origin: Option<super::reclamation::DomainIdentity>,
}

pub(crate) struct ParticipantHandle(Option<std::sync::Arc<ParticipantAdmission>>);
impl ParticipantHandle {
    pub(super) fn new(
        mut value: ParticipantAdmission,
        origin: super::reclamation::DomainIdentity,
    ) -> Self {
        value.origin = Some(origin);
        Self(Some(std::sync::Arc::new(value)))
    }
    pub(super) fn belongs_to(&self, origin: &super::reclamation::DomainIdentity) -> bool {
        self.origin
            .as_ref()
            .is_some_and(|own| own.same_identity(origin))
    }

    pub(crate) fn same_admission(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(
            self.0.as_ref().expect("live participant admission"),
            other.0.as_ref().expect("live participant admission"),
        )
    }
}
impl Clone for ParticipantHandle {
    fn clone(&self) -> Self {
        Self(Some(
            self.0.as_ref().expect("live participant admission").clone(),
        ))
    }
}
impl std::ops::Deref for ParticipantHandle {
    type Target = ParticipantAdmission;
    fn deref(&self) -> &ParticipantAdmission {
        self.0.as_ref().expect("live participant admission")
    }
}
impl Drop for ParticipantHandle {
    fn drop(&mut self) {
        let _ = std::sync::Arc::into_inner(self.0.take().expect("participant dropped twice"));
    }
}

/// Root-free process inventory; observing statistics never initializes or
/// retains the collector. This shared modeled inventory is repeated per domain.
pub(crate) fn process_participants() -> u64 {
    if POOL.value.get().is_some() {
        PARTICIPANTS as u64
    } else {
        0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParticipantLimit {
    pub(crate) requested: usize,
    pub(crate) limit: usize,
    pub(crate) retryable: bool,
}

impl ParticipantAdmission {
    pub(crate) fn snapshot(&self) -> (u64, u64) {
        (
            self.leased.load(Ordering::Acquire) as u64,
            self.limit as u64,
        )
    }
    pub(super) fn new(limit: usize) -> Result<Self, super::concurrency_lifecycle::Outcome> {
        if !(1..=PARTICIPANTS).contains(&limit) {
            return Err(super::concurrency_lifecycle::Outcome::Refused(
                "retained_participant_limit must be in 1..=256",
            ));
        }
        ParticipantPool::try_global()?;
        Ok(Self {
            limit,
            leased: AtomicUsize::new(0),
            origin: None,
        })
    }

    // Nonblocking final admission. The owning coordinator may park/refuse BEFORE
    // WAL; it must drop this Session before parking for any other dependency.
    pub(crate) fn try_acquire(&self) -> Result<Session<'_>, ParticipantLimit> {
        let refusal = || ParticipantLimit {
            requested: 1,
            limit: self.limit,
            retryable: true,
        };
        self.leased
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .map_err(|_| refusal())?;
        let pool = ParticipantPool::ready();
        let start = pool.ticket.fetch_add(1, Ordering::Relaxed) % PARTICIPANTS;
        for offset in 0..PARTICIPANTS {
            let slot = match pool.slots[(start + offset) % PARTICIPANTS].try_lock() {
                Ok(slot) => slot,
                Err(TryLockError::WouldBlock) => continue,
                Err(TryLockError::Poisoned(_)) => invariant("participant slot poisoned"),
            };
            let guard = slot.0.pin();
            return Ok(Session {
                guard: Some(guard),
                slot: Some(slot),
                admission: self,
            });
        }
        self.leased.fetch_sub(1, Ordering::AcqRel);
        Err(refusal())
    }
}

// Fields and Drop order are intentional. No public Guard/handle accessor or
// callback taking &Guard exists. Session is !Send because its MutexGuard/Guard
// stay on the acquiring thread. Only the exclusive participant moves logically.
pub(crate) struct Session<'a> {
    guard: Option<Guard>,
    slot: Option<MutexGuard<'static, ExclusiveParticipant>>,
    admission: &'a ParticipantAdmission,
}

impl Session<'_> {
    fn guard(&self) -> &Guard {
        self.guard.as_ref().expect("live participant session")
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        drop(self.slot.take());
        self.admission.leased.fetch_sub(1, Ordering::Release);
        // Unexpected in-session panic may have interrupted a post-WAL install.
        // Never continue with a poisoned/replaced Local or partial generation.
        if std::thread::panicking() {
            invariant("participant session unwound");
        }
    }
}

pub(crate) struct AppendMap<K, V> {
    inner: SkipList<K, V>,
    #[cfg(test)]
    scan_groups: AtomicUsize,
    #[cfg(test)]
    sorted_walk_work: [AtomicUsize; 2],
}
impl<K: Ord, V> Default for AppendMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> AppendMap<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            inner: SkipList::new(ParticipantPool::ready().collector.clone()),
            #[cfg(test)]
            scan_groups: AtomicUsize::new(0),
            #[cfg(test)]
            sorted_walk_work: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn take_scan_groups_for_test(&self) -> usize {
        self.scan_groups.swap(0, Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn take_sorted_walk_work_for_test(&self) -> [usize; 2] {
        std::array::from_fn(|i| self.sorted_walk_work[i].swap(0, Ordering::Relaxed))
    }

    pub(crate) fn get<'a>(&'a self, session: &'a Session<'_>, key: &K) -> Option<&'a V> {
        self.inner
            .get(key, session.guard())
            .map(|entry| entry.value())
    }

    pub(crate) fn lower<'a>(
        &'a self,
        session: &'a Session<'_>,
        bound: Bound<&K>,
    ) -> Option<(&'a K, &'a V)> {
        self.inner
            .lower_bound(bound, session.guard())
            .map(|entry| (entry.key(), entry.value()))
    }

    #[cfg(test)]
    pub(crate) fn fold_all_for_test<T>(
        &self,
        session: &Session<'_>,
        initial: T,
        mut fold: impl FnMut(T, &K, &V) -> T,
    ) -> T {
        self.inner
            .iter(session.guard())
            .fold(initial, |sum, entry| fold(sum, entry.key(), entry.value()))
    }

    pub(crate) fn insert_unique(&self, session: &Session<'_>, key: K, value: V) {
        // Requires the existing single-coordinator insertion contract. In pinned
        // 0.1.3, an existing key returns before invoking the closure. Its later
        // CAS-loser path can consume/discard the closure's value, so consumption
        // alone does NOT detect simultaneous equal-key insertion.
        let mut value = Some(value);
        let entry = self.inner.get_or_insert_with(
            key,
            || value.take().expect("one insertion"),
            session.guard(),
        );
        let held = HeldEntry {
            entry: Some(entry),
            guard: session.guard(),
        };
        if value.is_some() {
            invariant("duplicate append full key");
        }
        drop(held);
    }
}

impl AppendMap<u64, ()> {
    /// Sparse, idempotent accelerator; its caller shares coordinator custody with
    /// primary installation. Return whether this call added the first marker.
    pub(crate) fn remember_id(&self, session: &Session<'_>, id: u64) -> bool {
        let mut inserted = false;
        let entry = self.inner.get_or_insert_with(
            id,
            || {
                inserted = true;
            },
            session.guard(),
        );
        drop(HeldEntry {
            entry: Some(entry),
            guard: session.guard(),
        });
        inserted
    }

    pub(crate) fn visit_ids(&self, session: &Session<'_>, upper: u64, mut visit: impl FnMut(u64)) {
        for entry in self.inner.iter(session.guard()) {
            let id = *entry.key();
            if id > upper {
                break;
            }
            visit(id);
        }
    }
}

// Keep this test-only import below the collector source-boundary proof prefix.
#[cfg(test)]
use crossbeam_skiplist::base::Entry;

// Preserve the adapter-level ordered-copy regression tests. Production primary
// traversal is provided exclusively by RadixDirectory.
#[cfg(test)]
impl<V> AppendMap<(u64, std::cmp::Reverse<u64>), V> {
    /// Copy at most 4,096 sorted requests, retaining dependency entries only here.
    /// Try one next entry between IDs; a sparse gap, history or future version
    /// falls back to a bound at (requested ID, S), never a linear history walk.
    /// Duplicates reuse the same opinion, including missing/removed opinions.
    /// The internal copier returns Break(false) to retry the current request in
    /// the next session, or Break(true) to consume it and yield immediately.
    pub(crate) fn copy_sorted_versions_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        ids: &[u64],
        seq: u64,
        mut copy: impl FnMut(u64, Option<&'a V>) -> std::ops::ControlFlow<bool>,
    ) -> usize {
        use std::cmp::Reverse;
        use std::ops::ControlFlow;
        let mut entry: Option<Entry<'_, '_, (u64, Reverse<u64>), V>> = None;
        let mut previous = None;
        let mut consumed = 0;
        for &id in ids.iter().take(4096) {
            if previous != Some(id) {
                if entry.as_ref().is_some_and(|found| found.key().0 < id) {
                    #[cfg(test)]
                    self.sorted_walk_work[1].fetch_add(1, Ordering::Relaxed);
                    entry = entry.take().expect("previous entry").next();
                }
                if entry
                    .as_ref()
                    .is_none_or(|found| found.key().0 != id || found.key().1 .0 > seq)
                {
                    #[cfg(test)]
                    self.sorted_walk_work[0].fetch_add(1, Ordering::Relaxed);
                    entry = self
                        .inner
                        .lower_bound(Bound::Included(&(id, Reverse(seq))), session.guard());
                }
            }
            previous = Some(id);
            let value = entry
                .as_ref()
                .filter(|found| found.key().0 == id)
                .map(|found| found.value());
            match copy(id, value) {
                ControlFlow::Continue(()) => consumed += 1,
                ControlFlow::Break(accepted) => {
                    consumed += usize::from(accepted);
                    break;
                }
            }
        }
        consumed
    }

    /// Walk logical IDs in order while retaining all library entries here.
    /// Singleton groups advance with next(); only version histories need another
    /// bound seek. The callback copies internal data, never invokes user code.
    pub(crate) fn walk_versions_at<'a>(
        &'a self,
        session: &'a Session<'_>,
        seq: u64,
        bounds: (Option<u64>, u64),
        max_groups: usize,
        mut copy: impl FnMut(u64, &'a V) -> bool,
    ) -> (Option<u64>, bool) {
        use std::cmp::Reverse;
        let (mut last, upper) = bounds;
        let start = last.map(|id| (id, Reverse(0)));
        let mut next = self.inner.lower_bound(
            start.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
            session.guard(),
        );
        for _ in 0..max_groups {
            let Some(mut entry) = next.take() else {
                return (last, true);
            };
            let id = entry.key().0;
            if id > upper {
                return (last, true);
            }
            #[cfg(test)]
            self.scan_groups.fetch_add(1, Ordering::Relaxed);
            if entry.key().1 .0 > seq {
                // Skip arbitrarily many future versions without walking them.
                next = self
                    .inner
                    .lower_bound(Bound::Included(&(id, Reverse(seq))), session.guard());
                if next.as_ref().is_none_or(|found| found.key().0 != id) {
                    last = Some(id); // Future-only groups still make progress.
                    continue;
                }
                entry = next.take().expect("same visible group");
            }
            if !copy(id, entry.value()) {
                return (last, false);
            }
            last = Some(id);
            next = entry.next();
            if next.as_ref().is_some_and(|found| found.key().0 == id) {
                next = self
                    .inner
                    .lower_bound(Bound::Excluded(&(id, Reverse(0))), session.guard());
            }
        }
        let done = next.as_ref().is_none_or(|entry| entry.key().0 > upper);
        (last, done)
    }
}

// Narrow typed tuple walks for graph families. Dependency entries and guards
// remain here; each callback receives only borrowed values within its session.
pub(crate) struct VersionWalk<K> {
    pub(crate) last: Option<K>,
    pub(crate) examined: usize,
    pub(crate) exhausted: bool,
}
macro_rules! tuple_version_walk {
    ($method:ident, $logical:ty, $version:ty, $logical_key:expr, $version_key:expr) => {
        impl<V> AppendMap<$version, V> {
            pub(crate) fn $method<'a>(
                &'a self,
                session: &'a Session<'_>,
                seq: u64,
                bounds: (Bound<$logical>, Bound<$logical>),
                max_groups: usize,
                mut copy: impl FnMut($logical, Option<&'a V>) -> bool,
            ) -> VersionWalk<$logical> {
                use std::cmp::Reverse;
                let logical = $logical_key;
                let version = $version_key;
                let start = match bounds.0 {
                    Bound::Included(key) => Bound::Included(version(key, Reverse(u64::MAX))),
                    Bound::Excluded(key) => Bound::Excluded(version(key, Reverse(0))),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let mut next = self.inner.lower_bound(start.as_ref(), session.guard());
                let mut result = VersionWalk {
                    last: None,
                    examined: 0,
                    exhausted: false,
                };
                while result.examined < max_groups.min(4096) {
                    let Some(mut entry) = next.take() else {
                        result.exhausted = true;
                        return result;
                    };
                    let key = logical(entry.key());
                    if match bounds.1 {
                        Bound::Included(end) => key > end,
                        Bound::Excluded(end) => key >= end,
                        Bound::Unbounded => false,
                    } {
                        result.exhausted = true;
                        return result;
                    }
                    result.examined += 1;
                    result.last = Some(key);
                    // Seek directly past hot future versions, including future-only groups.
                    if entry.key() < &version(key, Reverse(seq)) {
                        next = self.inner.lower_bound(
                            Bound::Included(&version(key, Reverse(seq))),
                            session.guard(),
                        );
                        if next
                            .as_ref()
                            .is_none_or(|found| logical(found.key()) != key)
                        {
                            if !copy(key, None) {
                                return result;
                            }
                            continue;
                        }
                        entry = next.take().expect("visible logical group");
                    }
                    if !copy(key, Some(entry.value())) {
                        return result;
                    }
                    next = entry.next();
                    if next
                        .as_ref()
                        .is_some_and(|found| logical(found.key()) == key)
                    {
                        next = self.inner.lower_bound(
                            Bound::Excluded(&version(key, Reverse(0))),
                            session.guard(),
                        );
                    }
                }
                result.exhausted = next.as_ref().is_none_or(|entry| {
                    let key = logical(entry.key());
                    match bounds.1 {
                        Bound::Included(end) => key > end,
                        Bound::Excluded(end) => key >= end,
                        Bound::Unbounded => false,
                    }
                });
                result
            }
        }
    };
}
tuple_version_walk!(
    walk_adjacency,
    (u64, u64),
    (u64, u64, std::cmp::Reverse<u64>),
    |k: &(u64, u64, std::cmp::Reverse<u64>)| (k.0, k.1),
    |k: (u64, u64), s| (k.0, k.1, s)
);
tuple_version_walk!(
    walk_groups,
    (u64, u32),
    (u64, u32, std::cmp::Reverse<u64>),
    |k: &(u64, u32, std::cmp::Reverse<u64>)| (k.0, k.1),
    |k: (u64, u32), s| (k.0, k.1, s)
);
tuple_version_walk!(
    walk_labels,
    (u32, u64),
    (u32, u64, std::cmp::Reverse<u64>),
    |k: &(u32, u64, std::cmp::Reverse<u64>)| (k.0, k.1),
    |k: (u32, u64), s| (k.0, k.1, s)
);
tuple_version_walk!(
    walk_times,
    (u32, i64, u64),
    (u32, i64, u64, std::cmp::Reverse<u64>),
    |k: &(u32, i64, u64, std::cmp::Reverse<u64>)| (k.0, k.1, k.2),
    |k: (u32, i64, u64), s| (k.0, k.1, k.2, s)
);

struct HeldEntry<'a, 'g, K, V> {
    entry: Option<RefEntry<'a, K, V>>,
    guard: &'g Guard,
}

impl<K, V> Drop for HeldEntry<'_, '_, K, V> {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            entry.release(self.guard);
        }
    }
}

#[cfg(not(test))]
fn initialization_test_hook(_: usize) {}

#[cfg(test)]
static INIT_FAIL_AT: AtomicUsize = AtomicUsize::new(usize::MAX);
#[cfg(test)]
static INIT_PAUSE_AT: AtomicUsize = AtomicUsize::new(usize::MAX);
#[cfg(test)]
static INIT_PAUSE_STATE: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn initialization_test_hook(index: usize) {
    if index == INIT_FAIL_AT.load(Ordering::Acquire) {
        panic!("injected participant initialization failure");
    }
    if index == INIT_PAUSE_AT.load(Ordering::Acquire) {
        INIT_PAUSE_STATE.store(1, Ordering::Release);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while INIT_PAUSE_STATE.load(Ordering::Acquire) != 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "initializer hook timeout"
            );
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
pub(crate) static TEST_SERIAL: Mutex<()> = Mutex::new(());

// Test-only allocator observation; not part of the adapter's ownership exception.
// Delegates every Layout/pointer unchanged to System. Thread-local mode measures
// caller work; isolated-child global mode follows allocation identity across all
// threads. Direct System/native allocations remain a separate source audit.
// No registry/collector access occurs inside allocation hooks.
#[cfg(test)]
pub(super) mod allocations {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    thread_local! {
        static COUNTS: Cell<Option<(usize, usize, usize, usize)>> = const { Cell::new(None) };
        static PEAK: Cell<Option<PeakObservation>> = const { Cell::new(None) };
    }
    #[derive(Clone, Copy, Default, Debug)]
    pub(in crate::engine) struct PeakObservation {
        pub(in crate::engine) live_bytes: isize,
        pub(in crate::engine) peak_bytes: usize,
        pub(in crate::engine) large_live: isize,
        pub(in crate::engine) max_request: usize,
    }
    struct ObservedSystem;
    #[global_allocator]
    static ALLOCATOR: ObservedSystem = ObservedSystem;
    fn note(size: usize, alloc: bool) {
        let _ = COUNTS.try_with(|cell| {
            if let Some((a, d, ab, db)) = cell.get() {
                cell.set(Some(if alloc {
                    (a + 1, d, ab + size, db)
                } else {
                    (a, d + 1, ab, db + size)
                }));
            }
        });
        let _ = PEAK.try_with(|cell| {
            if let Some(mut measured) = cell.get() {
                let size_signed = isize::try_from(size).unwrap();
                measured.live_bytes += if alloc { size_signed } else { -size_signed };
                measured.peak_bytes = measured.peak_bytes.max(measured.live_bytes.max(0) as usize);
                if size >= 64 * 1024 {
                    measured.large_live += if alloc { 1 } else { -1 };
                }
                if alloc {
                    measured.max_request = measured.max_request.max(size);
                }
                cell.set(Some(measured));
            }
        });
    }
    // Isolated-child observer. Metadata is fixed static storage, never allocated
    // from inside a hook. Pointer membership ignores deallocations of baseline
    // allocations and preserves conservative old/new realloc overlap.
    const TABLE_CAPACITY: usize = 16_384;
    const TOMBSTONE: usize = usize::MAX;
    static GLOBAL_ACTIVE: AtomicBool = AtomicBool::new(false);
    static GLOBAL_LOCK: AtomicBool = AtomicBool::new(false);
    static POINTERS: [AtomicUsize; TABLE_CAPACITY] =
        [const { AtomicUsize::new(0) }; TABLE_CAPACITY];
    static SIZES: [AtomicUsize; TABLE_CAPACITY] = [const { AtomicUsize::new(0) }; TABLE_CAPACITY];
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
    static ALLOCS: AtomicUsize = AtomicUsize::new(0);
    static FREES: AtomicUsize = AtomicUsize::new(0);
    static GROSS_BYTES: AtomicUsize = AtomicUsize::new(0);
    static FREED_BYTES: AtomicUsize = AtomicUsize::new(0);
    static LIVE_OBJECTS: AtomicUsize = AtomicUsize::new(0);
    static OVERFLOW: AtomicBool = AtomicBool::new(false);

    struct ObserverLock;
    impl ObserverLock {
        fn acquire() -> Self {
            while GLOBAL_LOCK
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                std::hint::spin_loop();
            }
            Self
        }
    }
    impl Drop for ObserverLock {
        fn drop(&mut self) {
            GLOBAL_LOCK.store(false, Ordering::Release);
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub(in crate::engine) struct GlobalObservation {
        pub live_bytes: usize,
        pub peak_bytes: usize,
        pub allocations: usize,
        pub deallocations: usize,
        pub gross_bytes: usize,
        pub freed_bytes: usize,
        pub live_objects: usize,
        pub overflow: bool,
    }

    fn hash(pointer: usize) -> usize {
        let mixed = (pointer >> 3) ^ (pointer >> 19);
        mixed.wrapping_mul(0x9e3779b97f4a7c15) & (TABLE_CAPACITY - 1)
    }
    // All following table/counter functions require ObserverLock. Atomic cells
    // avoid any extra UnsafeCell/Sync exception for instrumentation storage.
    fn find(pointer: usize) -> Option<usize> {
        let start = hash(pointer);
        for offset in 0..TABLE_CAPACITY {
            let i = (start + offset) & (TABLE_CAPACITY - 1);
            let value = POINTERS[i].load(Ordering::Relaxed);
            if value == pointer {
                return Some(i);
            }
            if value == 0 {
                return None;
            }
        }
        None
    }
    fn add(counter: &AtomicUsize, amount: usize) -> usize {
        match counter.load(Ordering::Relaxed).checked_add(amount) {
            Some(value) => {
                counter.store(value, Ordering::Relaxed);
                value
            }
            None => {
                OVERFLOW.store(true, Ordering::Relaxed);
                usize::MAX
            }
        }
    }
    fn allocation(pointer: usize, size: usize) {
        let start = hash(pointer);
        let mut free = None;
        for offset in 0..TABLE_CAPACITY {
            let i = (start + offset) & (TABLE_CAPACITY - 1);
            let value = POINTERS[i].load(Ordering::Relaxed);
            if value == pointer {
                OVERFLOW.store(true, Ordering::Relaxed);
                return;
            }
            if value == TOMBSTONE && free.is_none() {
                free = Some(i);
            }
            if value == 0 {
                free = Some(free.unwrap_or(i));
                break;
            }
        }
        let Some(i) = free else {
            OVERFLOW.store(true, Ordering::Relaxed);
            return;
        };
        POINTERS[i].store(pointer, Ordering::Relaxed);
        SIZES[i].store(size, Ordering::Relaxed);
        let live = add(&LIVE, size);
        PEAK_BYTES.store(
            PEAK_BYTES.load(Ordering::Relaxed).max(live),
            Ordering::Relaxed,
        );
        add(&ALLOCS, 1);
        add(&GROSS_BYTES, size);
        add(&LIVE_OBJECTS, 1);
    }
    fn deallocation(pointer: usize, size: usize) {
        let Some(i) = find(pointer) else { return };
        if SIZES[i].load(Ordering::Relaxed) != size {
            OVERFLOW.store(true, Ordering::Relaxed);
            return;
        }
        POINTERS[i].store(TOMBSTONE, Ordering::Relaxed);
        let Some(live) = LIVE.load(Ordering::Relaxed).checked_sub(size) else {
            OVERFLOW.store(true, Ordering::Relaxed);
            return;
        };
        let Some(objects) = LIVE_OBJECTS.load(Ordering::Relaxed).checked_sub(1) else {
            OVERFLOW.store(true, Ordering::Relaxed);
            return;
        };
        LIVE.store(live, Ordering::Relaxed);
        LIVE_OBJECTS.store(objects, Ordering::Relaxed);
        add(&FREES, 1);
        add(&FREED_BYTES, size);
    }
    fn global_note(pointer: *mut u8, size: usize, allocated: bool) {
        let _held = ObserverLock::acquire();
        if !GLOBAL_ACTIVE.load(Ordering::Relaxed) {
            return;
        }
        if allocated {
            allocation(pointer as usize, size);
        } else {
            deallocation(pointer as usize, size);
        }
    }
    fn snapshot_locked() -> GlobalObservation {
        GlobalObservation {
            live_bytes: LIVE.load(Ordering::Relaxed),
            peak_bytes: PEAK_BYTES.load(Ordering::Relaxed),
            allocations: ALLOCS.load(Ordering::Relaxed),
            deallocations: FREES.load(Ordering::Relaxed),
            gross_bytes: GROSS_BYTES.load(Ordering::Relaxed),
            freed_bytes: FREED_BYTES.load(Ordering::Relaxed),
            live_objects: LIVE_OBJECTS.load(Ordering::Relaxed),
            overflow: OVERFLOW.load(Ordering::Relaxed),
        }
    }
    pub(in crate::engine) fn global_start() {
        let _held = ObserverLock::acquire();
        assert!(!GLOBAL_ACTIVE.load(Ordering::Relaxed));
        for i in 0..TABLE_CAPACITY {
            POINTERS[i].store(0, Ordering::Relaxed);
            SIZES[i].store(0, Ordering::Relaxed);
        }
        for counter in [
            &LIVE,
            &PEAK_BYTES,
            &ALLOCS,
            &FREES,
            &GROSS_BYTES,
            &FREED_BYTES,
            &LIVE_OBJECTS,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        OVERFLOW.store(false, Ordering::Relaxed);
        GLOBAL_ACTIVE.store(true, Ordering::Release);
    }
    pub(in crate::engine) fn global_read() -> GlobalObservation {
        let _held = ObserverLock::acquire();
        snapshot_locked()
    }
    pub(in crate::engine) fn global_stop() -> GlobalObservation {
        let _held = ObserverLock::acquire();
        GLOBAL_ACTIVE.store(false, Ordering::Release);
        snapshot_locked()
    }

    // SAFETY: instrumentation only; System receives the original valid allocation
    // arguments, with no ownership changes, dereferences, or reentrant allocation.
    unsafe impl GlobalAlloc for ObservedSystem {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(layout) };
            if !p.is_null() {
                if GLOBAL_ACTIVE.load(Ordering::Acquire) {
                    global_note(p, layout.size(), true);
                } else {
                    note(layout.size(), true);
                }
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
            if GLOBAL_ACTIVE.load(Ordering::Acquire) {
                // Keep pointer identity serialized until the actual free has
                // finished; another thread can then reuse the address safely.
                let _held = ObserverLock::acquire();
                unsafe { System.dealloc(p, layout) }
                if GLOBAL_ACTIVE.load(Ordering::Relaxed) {
                    deallocation(p as usize, layout.size());
                }
                return;
            }
            note(layout.size(), false);
            unsafe { System.dealloc(p, layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(layout) };
            if !p.is_null() {
                if GLOBAL_ACTIVE.load(Ordering::Acquire) {
                    global_note(p, layout.size(), true);
                } else {
                    note(layout.size(), true);
                }
            }
            p
        }
        unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            if GLOBAL_ACTIVE.load(Ordering::Acquire) {
                // Serialize table membership across realloc's physical free and
                // possible address reuse by other participating threads.
                let _held = ObserverLock::acquire();
                let result = unsafe { System.realloc(p, layout, size) };
                if !result.is_null() && GLOBAL_ACTIVE.load(Ordering::Relaxed) {
                    let overlap = LIVE.load(Ordering::Relaxed).checked_add(size);
                    match overlap {
                        Some(peak) => PEAK_BYTES.store(
                            PEAK_BYTES.load(Ordering::Relaxed).max(peak),
                            Ordering::Relaxed,
                        ),
                        None => OVERFLOW.store(true, Ordering::Relaxed),
                    }
                    deallocation(p as usize, layout.size());
                    allocation(result as usize, size);
                }
                return result;
            }
            let result = unsafe { System.realloc(p, layout, size) };
            if !result.is_null() {
                // Observe the conservative old/new overlap even when System's
                // realloc grows in place. Existing gross counts stay identical.
                note(size, true);
                note(layout.size(), false);
            }
            result
        }
    }
    pub(in crate::engine) fn measure<T>(
        f: impl FnOnce() -> T,
    ) -> (T, (usize, usize, usize, usize)) {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COUNTS.with(|c| c.set(None));
            }
        }
        COUNTS.with(|c| {
            assert!(c.get().is_none());
            c.set(Some((0, 0, 0, 0)));
        });
        let reset = Reset;
        let value = f();
        let counts = COUNTS.with(|c| c.get().unwrap());
        drop(reset);
        (value, counts)
    }
    pub(in crate::engine) fn measure_peak<T>(f: impl FnOnce() -> T) -> (T, PeakObservation) {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                PEAK.with(|c| c.set(None));
            }
        }
        PEAK.with(|c| {
            assert!(c.get().is_none());
            c.set(Some(PeakObservation::default()));
        });
        let reset = Reset;
        let value = f();
        let measured = PEAK.with(|c| c.get().unwrap());
        drop(reset);
        (value, measured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn p45b1_insert_unique_rejects_sequential_duplicate() {
        const CHILD: &str = "OVERGRAPH_P45B1_DUPLICATE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let admission = ParticipantAdmission::new(1).unwrap();
            let session = admission.try_acquire().unwrap();
            let map = AppendMap::new();
            map.insert_unique(&session, 7u64, String::from("first"));
            map.insert_unique(&session, 7u64, String::from("duplicate"));
            panic!("sequential duplicate returned");
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::participant::tests::p45b1_insert_unique_rejects_sequential_duplicate",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("duplicate append full key"));
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(result.status.signal(), Some(6));
        }
    }

    #[test]
    fn p45b1_insert_unique_lazy_dependency_and_payload_custody() {
        struct Payload(Arc<AtomicUsize>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let stored = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let admission = ParticipantAdmission::new(1).unwrap();
        let session = admission.try_acquire().unwrap();
        let map = AppendMap::new();
        map.insert_unique(&session, 7u64, Payload(stored.clone()));
        assert_eq!(stored.load(Ordering::Relaxed), 0);
        // Exercise the exact pinned dependency path used by insert_unique:
        // sequential existing keys leave the proposed payload unconsumed.
        let mut value = Some(Payload(rejected.clone()));
        let entry = map
            .inner
            .get_or_insert_with(7, || value.take().unwrap(), session.guard());
        drop(HeldEntry {
            entry: Some(entry),
            guard: session.guard(),
        });
        assert!(value.is_some());
        assert!(Arc::ptr_eq(&map.get(&session, &7).unwrap().0, &stored));
        drop(value);
        assert_eq!(rejected.load(Ordering::Relaxed), 1);
        assert_eq!(stored.load(Ordering::Relaxed), 0);
        drop(session);
        assert_eq!(admission.snapshot().0, 0);
        assert_eq!(stored.load(Ordering::Relaxed), 0);
        drop(map);
        assert_eq!(stored.load(Ordering::Relaxed), 1);
        assert_eq!(rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn p45b1_ordered_walk_skips_histories_and_resumes_unconsumed_group() {
        use std::cmp::Reverse;
        let admission = ParticipantAdmission::new(1).unwrap();
        let session = admission.try_acquire().unwrap();
        let map = AppendMap::new();
        for seq in 0..1000 {
            map.insert_unique(&session, (1, Reverse(seq)), seq);
        }
        map.insert_unique(&session, (2, Reverse(900)), 900); // Future only.
        map.insert_unique(&session, (3, Reverse(0)), 30);
        map.insert_unique(&session, (4, Reverse(0)), 40);
        let mut found = Vec::new();
        let position = map.walk_versions_at(&session, 2, (None, 3), 2, |id, v| {
            found.push((id, *v));
            true
        });
        assert_eq!(found, vec![(1, 2)]);
        assert_eq!(position, (Some(2), false));
        assert_eq!(
            map.walk_versions_at(&session, 2, (position.0, 3), 4096, |_, _| false),
            (Some(2), false)
        );
        assert_eq!(
            map.walk_versions_at(&session, 2, (position.0, 3), 4096, |id, v| {
                found.push((id, *v));
                true
            }),
            (Some(3), true)
        );
        assert_eq!(found, vec![(1, 2), (3, 30)]);
    }

    #[test]
    fn targeted_v_sorted_adapter_reuses_adjacent_entries_and_bounds_sparse_work() {
        use std::cmp::Reverse;
        use std::ops::ControlFlow;
        let admission = ParticipantAdmission::new(1).unwrap();
        let session = admission.try_acquire().unwrap();
        let map = AppendMap::new();
        for id in 0..6000 {
            map.insert_unique(&session, (id, Reverse(1)), id);
        }
        let ids: Vec<_> = (0..6000).collect();
        let mut copied = Vec::new();
        let mut position = 0;
        while position < ids.len() {
            let consumed =
                map.copy_sorted_versions_at(&session, &ids[position..], 1, |id, value| {
                    copied.push((id, *value.unwrap()));
                    ControlFlow::Continue(())
                });
            assert_eq!(consumed, (ids.len() - position).min(4096));
            position += consumed;
        }
        assert_eq!(copied, ids.iter().map(|&id| (id, id)).collect::<Vec<_>>());
        assert_eq!(map.take_sorted_walk_work_for_test(), [2, 5998]);
        copied.clear();
        assert_eq!(
            map.copy_sorted_versions_at(&session, &[0, 2999, 5999, 5999], 1, |id, value| {
                copied.push((id, *value.unwrap()));
                ControlFlow::Continue(())
            }),
            4
        );
        assert_eq!(
            copied,
            vec![(0, 0), (2999, 2999), (5999, 5999), (5999, 5999)]
        );
        assert_eq!(map.take_sorted_walk_work_for_test(), [3, 2]);
        // Rejecting a nonempty refill's next item leaves exactly that request
        // for the caller's next session; an accepted oversized item can yield.
        assert_eq!(
            map.copy_sorted_versions_at(&session, &[0, 1, 2], 1, |id, _| {
                if id == 2 {
                    ControlFlow::Break(false)
                } else {
                    ControlFlow::Continue(())
                }
            }),
            2
        );
        assert_eq!(
            map.copy_sorted_versions_at(&session, &[2, 3], 1, |_, _| ControlFlow::Break(true)),
            1
        );
    }

    #[test]
    fn targeted_v_sorted_adapter_skips_long_history_and_future_chains() {
        use std::cmp::Reverse;
        use std::ops::ControlFlow;
        let admission = ParticipantAdmission::new(1).unwrap();
        let session = admission.try_acquire().unwrap();
        let map = AppendMap::new();
        for seq in 0..10000 {
            map.insert_unique(&session, (0, Reverse(seq)), seq);
            map.insert_unique(&session, (1, Reverse(seq + 10000)), seq);
        }
        map.insert_unique(&session, (2, Reverse(0)), 20);
        map.insert_unique(&session, (u64::MAX, Reverse(0)), 99);
        let mut found = Vec::new();
        assert_eq!(
            map.copy_sorted_versions_at(&session, &[0, 0, 1, 1, 2, u64::MAX], 2, |id, value| {
                found.push((id, value.copied()));
                ControlFlow::Continue(())
            }),
            6
        );
        assert_eq!(
            found,
            vec![
                (0, Some(2)),
                (0, Some(2)),
                (1, None),
                (1, None),
                (2, Some(20)),
                (u64::MAX, Some(99))
            ]
        );
        assert_eq!(map.take_sorted_walk_work_for_test(), [2, 2]);
    }

    fn isolated_child(name: &str, marker: &str) -> bool {
        if std::env::var_os(marker).is_some() {
            return false;
        }
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(marker, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child status: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        print!("{}", String::from_utf8_lossy(&output.stdout));
        true
    }

    fn await_state(state: &AtomicUsize, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while state.load(Ordering::Acquire) != expected {
            assert!(std::time::Instant::now() < deadline, "causal hook timeout");
            std::thread::yield_now();
        }
    }

    #[test]
    fn p45a2_collector_serial_initialization_failures_and_contention_child() {
        if isolated_child(
            "engine::participant::tests::p45a2_collector_serial_initialization_failures_and_contention_child",
            "OVERGRAPH_P45A2_POOL_CHILD",
        ) { return; }
        assert!(POOL.value.get().is_none());
        let (_, invalid) = allocations::measure(|| {
            assert!(ParticipantAdmission::new(0).is_err());
            assert!(ParticipantAdmission::new(257).is_err());
        });
        assert_eq!(invalid, (0, 0, 0, 0));
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        // Fixture unwind/name setup precedes the measured collector attempts.
        drop(std::panic::catch_unwind(|| panic!("fixture warmup")));
        std::panic::set_hook(old_hook);
        let _ = std::thread::current();
        for prefix in [0, 1, 128, 255, 256] {
            INIT_FAIL_AT.store(prefix, Ordering::Release);
            let old_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            allocations::global_start();
            let failure = std::panic::catch_unwind(|| ParticipantAdmission::new(1));
            let failed = failure.is_err();
            drop(failure);
            let observed = allocations::global_stop();
            // Restore diagnostics before assertions, outside the allocation window.
            std::panic::set_hook(old_hook);
            eprintln!("collector attempted prefix={prefix}: failed={failed} {observed:?}");
            assert!(failed);
            assert!(POOL.value.get().is_none());
            assert!(!POOL.initializing.load(Ordering::Acquire));
            assert!(!observed.overflow, "{observed:?}");
            assert_eq!(observed.live_bytes, 0, "{observed:?}");
            assert_eq!(observed.live_objects, 0);
            assert_eq!(observed.allocations, observed.deallocations);
            assert_eq!(observed.gross_bytes, observed.freed_bytes);
            println!("collector failed prefix={prefix}: {observed:?}");
        }
        INIT_FAIL_AT.store(usize::MAX, Ordering::Release);
        let directory = tempfile::TempDir::new().unwrap();
        let manifest_path = directory.path().join("manifest.json");
        let wal_path = directory.path().join("wal_0.log");
        let manifest_before = b"existing manifest sentinel";
        let wal_before = b"existing WAL sentinel";
        std::fs::write(&manifest_path, manifest_before).unwrap();
        std::fs::write(&wal_path, wal_before).unwrap();
        static OWNER_STATE: AtomicUsize = AtomicUsize::new(0);
        let owner = std::thread::spawn(|| {
            let _ = std::thread::current();
            OWNER_STATE.store(1, Ordering::Release);
            await_state(&OWNER_STATE, 2);
            assert!(ParticipantAdmission::new(256).is_ok());
        });
        await_state(&OWNER_STATE, 1);
        INIT_PAUSE_AT.store(128, Ordering::Release);
        allocations::global_start();
        OWNER_STATE.store(2, Ordering::Release);
        await_state(&INIT_PAUSE_STATE, 1);
        let before = allocations::global_read();
        for _ in 0..512 {
            assert!(matches!(
                ParticipantAdmission::new(256),
                Err(super::super::concurrency_lifecycle::Outcome::ResourceLimit(
                    super::super::concurrency_lifecycle::ResourceLimit {
                        resource: "participants",
                        requested: 1,
                        limit: 256,
                        retryable: true,
                    }
                ))
            ));
        }
        let open_refused = matches!(
            super::super::append_slice::Slice::open(
                directory.path(),
                crate::WalSyncMode::Immediate
            ),
            Err(super::super::concurrency_lifecycle::Outcome::ResourceLimit(
                super::super::concurrency_lifecycle::ResourceLimit {
                    resource: "participants",
                    retryable: true,
                    ..
                }
            ))
        );
        let after = allocations::global_read();
        assert_eq!(before.live_bytes, after.live_bytes);
        assert_eq!(before.peak_bytes, after.peak_bytes);
        assert_eq!(before.allocations, after.allocations);
        assert_eq!(before.deallocations, after.deallocations);
        INIT_PAUSE_STATE.store(2, Ordering::Release);
        owner.join().unwrap();
        let permanent = allocations::global_stop();
        INIT_PAUSE_AT.store(usize::MAX, Ordering::Release);
        assert!(open_refused);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal_before);
        assert!(!permanent.overflow, "{permanent:?}");
        let (_, reuse) = allocations::measure(|| {
            for _ in 0..512 {
                let admission = ParticipantAdmission::new(1).unwrap();
                drop(admission.try_acquire().unwrap());
            }
        });
        assert_eq!(reuse, (0, 0, 0, 0));
    }

    #[test]
    fn p45a2_collector_global_observer_realloc_and_explicit_overflow_child() {
        if isolated_child(
            "engine::participant::tests::p45a2_collector_global_observer_realloc_and_explicit_overflow_child",
            "OVERGRAPH_P45A2_OBSERVER_CHILD",
        ) { return; }
        // Baseline fixture storage is deliberately outside the target graph.
        let mut buffer = Vec::<u8>::with_capacity(128);
        allocations::global_start();
        buffer.reserve_exact(4096);
        let first = allocations::global_read();
        assert_eq!(first.live_bytes, buffer.capacity());
        let old = buffer.capacity();
        buffer.reserve_exact(old * 2);
        let second = allocations::global_read();
        assert_eq!(second.live_bytes, buffer.capacity());
        assert!(second.peak_bytes >= old + buffer.capacity());
        drop(buffer);
        let drained = allocations::global_stop();
        assert!(!drained.overflow, "{drained:?}");
        assert_eq!(drained.live_bytes, 0);
        assert_eq!(drained.live_objects, 0);
        assert_eq!(drained.gross_bytes, drained.freed_bytes);
        println!("observer realloc overlap: {drained:?}");

        let mut objects = Vec::with_capacity(16_385);
        allocations::global_start();
        for _ in 0..16_385 {
            objects.push(Box::new(1u8));
        }
        let overflowed = allocations::global_stop();
        assert!(overflowed.overflow); // Explicit proof failure, never a valid bound.
        assert_eq!(overflowed.live_objects, 16_384);
        drop(objects);
        println!("observer capacity failure is explicit: {overflowed:?}");
    }

    #[test]
    fn p45a2_collector_global_observer_follows_actual_worker_graph_child() {
        if isolated_child(
            "engine::participant::tests::p45a2_collector_global_observer_follows_actual_worker_graph_child",
            "OVERGRAPH_P45A2_WORKER_OBSERVER_CHILD",
        ) { return; }
        use super::super::reclamation::{ReclaimDomain, ResourceClass};
        let admission = ParticipantAdmission::new(256).unwrap();
        let _ = std::thread::current();
        allocations::global_start();
        let domain = ReclaimDomain::new();
        let mut building = domain
            .build::<AppendMap<u64, u64>>(ResourceClass::Generation)
            .unwrap();
        building
            .mutate(|map| {
                let session = admission.try_acquire().unwrap();
                for id in 0..1000 {
                    map.insert_unique(&session, id, id * 3);
                }
            })
            .unwrap();
        let owner = building.into_owner().unwrap();
        let reader = owner.read();
        drop(owner);
        domain.close();
        let held = allocations::global_read();
        assert!(held.live_bytes > 1000 * 16);
        {
            let session = admission.try_acquire().unwrap();
            assert_eq!(reader.get().get(&session, &999), Some(&2997));
        }
        drop(reader);
        drop(domain);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let observed = allocations::global_read();
            if observed.live_objects == 0 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let observed = allocations::global_stop();
                panic!("worker graph failed complete global drain: {observed:?}");
            }
            std::thread::yield_now();
        }
        let drained = allocations::global_stop();
        assert!(!drained.overflow, "{drained:?}");
        assert_eq!(drained.live_bytes, 0);
        assert_eq!(drained.gross_bytes, drained.freed_bytes);
        assert_eq!(drained.allocations, drained.deallocations);
        assert!(drained.allocations > 1000);
        println!("actual worker-owned graph: held={held:?}; drained={drained:?}");
    }

    #[test]
    fn p45a1_collector_exhaustion_reuses_exact_slots_and_domain_share() {
        if isolated_child("engine::participant::tests::p45a1_collector_exhaustion_reuses_exact_slots_and_domain_share","P45B1_POOL_EXHAUSTION_CHILD"){return;}
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let a = ParticipantAdmission::new(256).unwrap();
        let b = ParticipantAdmission::new(256).unwrap();
        let leases: Vec<_> = (0..256).map(|_| a.try_acquire().unwrap()).collect();
        assert!(b.try_acquire().err().unwrap().retryable);
        drop(leases);
        let one = Arc::new(ParticipantAdmission::new(1).unwrap());
        let lease = one.try_acquire().unwrap();
        assert!(one.try_acquire().is_err());
        drop(lease);
        assert!(one.try_acquire().is_ok());
        // Real share-one dependency: a writer must release its lease before
        // blocking for a helper that needs the same allowance. Channel order
        // makes acquisition and release causal, rather than relying on sleeps.
        let map = Arc::new(AppendMap::new());
        let (work_tx, work_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let helper_admission = one.clone();
        let helper_map = map.clone();
        let helper = std::thread::spawn(move || {
            work_rx.recv().unwrap();
            let lease = helper_admission.try_acquire().unwrap();
            assert_eq!(helper_map.get(&lease, &1), Some(&11));
            helper_map.insert_unique(&lease, 2, 22);
            drop(lease);
            done_tx.send(()).unwrap();
        });
        let writer_admission = one.clone();
        let writer_map = map.clone();
        let writer = std::thread::spawn(move || {
            let lease = writer_admission.try_acquire().unwrap();
            writer_map.insert_unique(&lease, 1, 11);
            drop(lease); // before any preacceptance dependency wait
            work_tx.send(()).unwrap();
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let lease = writer_admission.try_acquire().unwrap();
            assert_eq!(writer_map.get(&lease, &2), Some(&22));
        });
        writer.join().unwrap();
        helper.join().unwrap();
        assert_eq!(a.leased.load(Ordering::Acquire), 0);
        assert_eq!(b.leased.load(Ordering::Acquire), 0);
        assert!(ParticipantAdmission::new(0).is_err());
        assert!(ParticipantAdmission::new(257).is_err());
    }

    #[test]
    fn p45a1_collector_thread_churn_and_cross_thread_reuse() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let admission = Arc::new(ParticipantAdmission::new(1).unwrap());
        let map = Arc::new(AppendMap::new());
        {
            let lease = admission.try_acquire().unwrap();
            map.insert_unique(&lease, 1u64, 17u64);
        }
        for _ in 0..300 {
            let a = admission.clone();
            let m = map.clone();
            std::thread::spawn(move || {
                let lease = a.try_acquire().unwrap();
                assert_eq!(m.get(&lease, &1), Some(&17));
            })
            .join()
            .unwrap();
        }
        // Live-idle application threads retain no participant lease or TLS Local.
        let ready = Arc::new(Barrier::new(258));
        let release = Arc::new(Barrier::new(258));
        std::thread::scope(|scope| {
            for _ in 0..257 {
                let (a, m, r, done) = (
                    admission.clone(),
                    map.clone(),
                    ready.clone(),
                    release.clone(),
                );
                std::thread::Builder::new()
                    .stack_size(128 * 1024)
                    .spawn_scoped(scope, move || {
                        // Different domains avoid imposing share-one ordering in this
                        // simultaneous-thread test. Shared physical slots stay fixed.
                        let local = ParticipantAdmission::new(256).unwrap();
                        let lease = local.try_acquire().unwrap();
                        assert_eq!(m.get(&lease, &1), Some(&17));
                        drop(lease);
                        drop(a);
                        r.wait();
                        done.wait();
                    })
                    .unwrap();
            }
            ready.wait();
            release.wait();
        });
        assert_eq!(ParticipantPool::try_global().unwrap().slots.len(), 256);
    }

    #[test]
    fn p45a1_collector_paused_session_leaves_free_slots_usable() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let a = Arc::new(ParticipantAdmission::new(256).unwrap());
        let m = Arc::new(AppendMap::new());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let aa = a.clone();
        let mm = m.clone();
        let t = std::thread::spawn(move || {
            let s = aa.try_acquire().unwrap();
            mm.insert_unique(&s, 2, 9);
            ready_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let s = a.try_acquire().unwrap();
        assert_eq!(m.lower(&s, Bound::Included(&2)), Some((&2, &9)));
        drop(s);
        go_tx.send(()).unwrap();
        t.join().unwrap();
    }

    #[test]
    fn p45a1_collector_actual_allocations_and_zero_residual() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let a = ParticipantAdmission::new(256).unwrap();
        let map = AppendMap::new();
        let (_, insert) = allocations::measure(|| {
            let s = a.try_acquire().unwrap();
            for id in 0..1000u64 {
                map.insert_unique(&s, id, id * 3);
            }
        });
        assert_eq!(insert.0, 1000); // one real node allocation per unique full key
        assert_eq!(insert.1, 0); // no linked node/collector deferred cleanup
        let unrelated = crossbeam_epoch::pin();
        let unrelated_called = Arc::new(AtomicUsize::new(0));
        let called = unrelated_called.clone();
        unrelated.defer(move || {
            called.fetch_add(1, Ordering::Relaxed);
        });
        unrelated.flush();
        let (_, read) = allocations::measure(|| {
            for _ in 0..2048 {
                let s = a.try_acquire().unwrap();
                assert_eq!(map.get(&s, &50), Some(&150));
                assert_eq!(map.lower(&s, Bound::Excluded(&50)), Some((&51, &153)));
            }
        });
        assert_eq!(read, (0, 0, 0, 0));
        assert_eq!(unrelated_called.load(Ordering::Relaxed), 0);
        let (_, destroyed) = allocations::measure(|| drop(map));
        assert_eq!(destroyed.0, 0);
        assert_eq!(destroyed.1, 1000);
        assert_eq!(destroyed.3, insert.2);
        drop(unrelated);
    }

    #[test]
    fn p45a1_collector_guard_unwind_failstop_and_private_api_compile_check() {
        if std::env::var_os("OVERGRAPH_P45A1_GUARD_CHILD").is_some() {
            let a = ParticipantAdmission::new(1).unwrap();
            let _ = std::panic::catch_unwind(|| {
                let _lease = a.try_acquire().unwrap();
                panic!("named guard unwind");
            });
            panic!("session unwind returned instead of aborting");
        }
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let output = std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact", "engine::participant::tests::p45a1_collector_guard_unwind_failstop_and_private_api_compile_check", "--nocapture"]).env("OVERGRAPH_P45A1_GUARD_CHILD", "1").output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("participant session unwound"));
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(output.status.signal(), Some(6));
        }
        let executable = std::env::current_exe().unwrap();
        let deps = executable.parent().unwrap();
        let fingerprints = deps.parent().unwrap().join(".fingerprint");
        let read_fingerprint = |path: &std::path::Path| {
            std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        };
        let current = read_fingerprint(
            &fingerprints
                .join(executable.file_stem().unwrap())
                .join("test-lib-overgraph.json"),
        )
        .expect("running test Cargo fingerprint");
        let rlib = std::fs::read_dir(&fingerprints)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter_map(|path| {
                let library = read_fingerprint(&path.join("lib-overgraph.json"))?;
                // Concurrent default/candidate/doc builds share this directory.
                // Match the actual test's features and compiler/target identity;
                // mtime alone can choose a default library with no private modules.
                if ![
                    "features",
                    "rustc",
                    "target",
                    "path",
                    "rustflags",
                    "config",
                    "compile_kind",
                ]
                .iter()
                .all(|key| library[*key] == current[*key])
                {
                    return None;
                }
                let name = path.file_name()?.to_str()?.strip_prefix("overgraph-")?;
                let library = deps.join(format!("libovergraph-{name}.rlib"));
                library.is_file().then_some(library)
            })
            .max_by_key(|p| p.metadata().unwrap().modified().unwrap())
            .expect("library matching running test Cargo features");
        let dir = tempfile::TempDir::new().unwrap();
        for module in [
            "participant",
            "reclamation",
            "append_slice",
            "durable_transition",
        ] {
            let source = dir.path().join("escape.rs");
            std::fs::write(
                &source,
                format!("use overgraph::engine::{module}; fn main() {{}}\n"),
            )
            .unwrap();
            let result = std::process::Command::new("rustc")
                .arg("--edition=2021")
                .arg("--emit=metadata")
                .arg(&source)
                .arg("--extern")
                .arg(format!("overgraph={}", rlib.display()))
                .arg("-L")
                .arg(format!("dependency={}", deps.display()))
                .arg("-o")
                .arg(dir.path().join("escape.rmeta"))
                .output()
                .unwrap();
            assert!(!result.status.success());
            assert!(
                String::from_utf8_lossy(&result.stderr).contains("is private"),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        let pool = ParticipantPool::try_global().unwrap();
        assert_eq!(pool.slots.len(), 256);
    }
}

// Secondary walks borrow logical keys while the session is live. Only the final
// consumed continuation is copied. Refusing a row leaves it for the next refill.
type SecondaryBounds<K> = (Bound<(K, u64)>, Bound<(K, u64)>);
impl<K: Ord + Clone, V> AppendMap<(K, u64, std::cmp::Reverse<u64>), V> {
    pub(crate) fn walk_secondary_versions<'a>(
        &'a self,
        session: &'a Session<'_>,
        seq: u64,
        bounds: SecondaryBounds<K>,
        max_groups: usize,
        mut copy: impl FnMut(&K, u64, Option<&'a V>) -> bool,
    ) -> VersionWalk<(K, u64)> {
        use std::cmp::Reverse;
        let version = |key: &(K, u64), s| (key.0.clone(), key.1, Reverse(s));
        let past_upper = |key: &(K, u64, Reverse<u64>)| match &bounds.1 {
            Bound::Included(end) => key.0.cmp(&end.0).then(key.1.cmp(&end.1)).is_gt(),
            Bound::Excluded(end) => !key.0.cmp(&end.0).then(key.1.cmp(&end.1)).is_lt(),
            Bound::Unbounded => false,
        };
        let start = match &bounds.0 {
            Bound::Included(k) => Bound::Included(version(k, u64::MAX)),
            Bound::Excluded(k) => Bound::Excluded(version(k, 0)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut next = self.inner.lower_bound(start.as_ref(), session.guard());
        let mut last = None;
        let mut examined = 0;
        macro_rules! finish {
            ($done:expr) => {
                return VersionWalk {
                    last: last.as_ref().map(
                        |e: &crossbeam_skiplist::base::Entry<'_, '_, (K, u64, Reverse<u64>), V>| {
                            (e.key().0.clone(), e.key().1)
                        },
                    ),
                    examined,
                    exhausted: $done,
                }
            };
        }
        while examined < max_groups.min(4096) {
            let Some(mut entry) = next.take() else {
                finish!(true)
            };
            if past_upper(entry.key()) {
                finish!(true)
            }
            examined += 1;
            if entry.key().2 .0 > seq {
                let key = (entry.key().0.clone(), entry.key().1, Reverse(seq));
                next = self
                    .inner
                    .lower_bound(Bound::Included(&key), session.guard());
                if next
                    .as_ref()
                    .is_none_or(|e| e.key().0 != entry.key().0 || e.key().1 != entry.key().1)
                {
                    if !copy(&entry.key().0, entry.key().1, None) {
                        finish!(false)
                    }
                    last = Some(entry);
                    continue;
                }
                entry = next.take().expect("same visible secondary group");
            }
            if !copy(&entry.key().0, entry.key().1, Some(entry.value())) {
                finish!(false)
            }
            next = entry.next();
            if next
                .as_ref()
                .is_some_and(|e| e.key().0 == entry.key().0 && e.key().1 == entry.key().1)
            {
                let end = (entry.key().0.clone(), entry.key().1, Reverse(0));
                next = self
                    .inner
                    .lower_bound(Bound::Excluded(&end), session.guard());
            }
            last = Some(entry);
        }
        finish!(next.as_ref().is_none_or(|entry| past_upper(entry.key())));
    }
}
