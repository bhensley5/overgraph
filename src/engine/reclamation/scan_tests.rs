use super::*;
use std::sync::mpsc;

#[cfg(feature = "production-concurrency")]
use crate::engine::participant::{allocations, TEST_SERIAL};
#[cfg(not(feature = "production-concurrency"))]
static TEST_SERIAL: Mutex<()> = Mutex::new(());

const CLASSES: [ResourceClass; CATEGORIES] = [
    ResourceClass::Generation,
    ResourceClass::Sources,
    ResourceClass::Catalog,
    ResourceClass::Storage,
    ResourceClass::Staging,
    ResourceClass::Cache,
];

fn no_allocations<R>(work: impl FnOnce() -> R) -> R {
    #[cfg(feature = "production-concurrency")]
    {
        let (result, observed) = allocations::measure(work);
        assert_eq!(observed, (0, 0, 0, 0));
        result
    }
    #[cfg(not(feature = "production-concurrency"))]
    work()
}

// Metadata-only registry fixture, with real stable IDs, category positions and
// one free inventory. Actual custody/removal paths are exercised below too.
fn registry(capacity: usize) -> Registry {
    Registry::new(capacity)
}

fn claim(registry: &mut Registry, class: ResourceClass) -> (usize, u64) {
    let index = registry.free.next().unwrap();
    registry.free.claim(index);
    let slot = &mut registry.slots[index];
    assert_eq!(slot.meta.state.load(Ordering::Acquire), FREE);
    let incarnation = increment(&slot.meta.incarnation);
    slot.meta.class.store(class.index(), Ordering::Relaxed);
    slot.meta.state.store(RESERVED, Ordering::Release);
    slot.category_position = registry.indexes[class.index()].len();
    registry.indexes[class.index()].push(index);
    registry.occupied += 1;
    (index, incarnation)
}

fn partition(registry: &Registry) {
    let mut seen = vec![false; registry.slots.len()];
    for (class, indexes) in registry.indexes.iter().enumerate() {
        assert!(indexes.capacity() >= registry.slots.len());
        assert!(registry.scan_remaining[class] <= indexes.len());
        for (position, &index) in indexes.iter().enumerate() {
            assert!(!std::mem::replace(&mut seen[index], true));
            let slot = &registry.slots[index];
            assert_eq!(slot.meta.class.load(Ordering::Relaxed), class);
            assert_eq!(slot.category_position, position);
            assert_ne!(slot.meta.state.load(Ordering::Acquire), FREE);
        }
    }
    assert_eq!(
        seen.iter().filter(|&&entry| entry).count(),
        registry.occupied
    );
    for &index in &registry.free.indexes {
        assert!(!std::mem::replace(&mut seen[index], true));
        assert_eq!(
            registry.slots[index].meta.state.load(Ordering::Acquire),
            FREE
        );
    }
    assert!(seen.into_iter().all(|entry| entry));
    assert!(registry.free.indexes.capacity() >= registry.slots.len());
}

fn sweep(registry: &mut Registry, result: &mut Vec<(usize, u64)>, class: &mut usize) {
    while let Some(index) = registry.next_scan_entry(class) {
        result.push((
            index,
            registry.slots[index]
                .meta
                .incarnation
                .load(Ordering::Acquire),
        ));
    }
    assert!(registry.scan_complete());
}

#[test]
fn p45a3_scan_sparse_dense_work_tracks_occupancy_and_keeps_capacity() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for capacity in [2_048, 16_384] {
        for occupied in [0, 37, capacity] {
            let mut registry = registry(capacity);
            let mut expected = Vec::with_capacity(occupied);
            for n in 0..occupied {
                expected.push(claim(&mut registry, CLASSES[n % CATEGORIES]));
            }
            let mut seen = Vec::with_capacity(occupied);
            let chunks = no_allocations(|| {
                registry.begin_scan();
                let mut class = 0;
                let mut chunks = 0;
                loop {
                    let mut inspected = 0;
                    while inspected < 256 {
                        let Some(index) = registry.next_scan_entry(&mut class) else {
                            break;
                        };
                        seen.push((
                            index,
                            registry.slots[index]
                                .meta
                                .incarnation
                                .load(Ordering::Acquire),
                        ));
                        inspected += 1;
                    }
                    assert!(inspected <= 256);
                    chunks += 1;
                    if registry.scan_complete() {
                        break;
                    }
                }
                chunks
            });
            assert_eq!(chunks, occupied.div_ceil(256).max(1));
            seen.sort_unstable();
            expected.sort_unstable();
            assert_eq!(seen, expected);
            partition(&registry);
        }
    }
}

#[test]
fn p45a3_scan_prefix_suffix_removal_append_and_reuse_preserve_identities() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for class in CLASSES {
        for _ in 0..2 {
            // Six entries/two inspected: prefix first/middle/last and suffix
            // first/last. Three entries/two inspected also cover a sole prefix.
            for (count, remove) in [(6, 0), (6, 2), (6, 3), (6, 4), (6, 5), (3, 0), (1, 0)] {
                let mut registry = registry(2_048);
                let wake = Wake::new();
                let initial: Vec<_> = (0..count).map(|_| claim(&mut registry, class)).collect();
                registry.begin_scan();
                let mut cursor = 0;
                let mut seen = Vec::with_capacity(count + 2);
                for _ in 0..2.min(count) {
                    let index = registry.next_scan_entry(&mut cursor).unwrap();
                    seen.push((
                        index,
                        registry.slots[index]
                            .meta
                            .incarnation
                            .load(Ordering::Acquire),
                    ));
                }
                let removed = initial[remove];
                let already_seen = seen.contains(&removed);
                no_allocations(|| free_slot(&mut registry, removed.0, &wake));
                partition(&registry);
                let reused = no_allocations(|| claim(&mut registry, class));
                assert_eq!(reused.0, removed.0);
                assert_eq!(reused.1, removed.1 + 1);
                let added = no_allocations(|| {
                    claim(&mut registry, CLASSES[(class.index() + 1) % CATEGORIES])
                });
                no_allocations(|| sweep(&mut registry, &mut seen, &mut cursor));
                let mut expected: Vec<_> = initial
                    .into_iter()
                    .filter(|id| *id != removed || already_seen)
                    .collect();
                seen.sort_unstable();
                expected.sort_unstable();
                assert_eq!(seen, expected);
                assert!(!seen.contains(&reused) && !seen.contains(&added));
                partition(&registry);
                seen.clear();
                registry.begin_scan();
                cursor = 0;
                no_allocations(|| sweep(&mut registry, &mut seen, &mut cursor));
                assert!(seen.contains(&reused) && seen.contains(&added));
                let mut unique = seen.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), count + 1);
                partition(&registry);
            }
        }
    }
}

fn pause_pair() -> (Pause, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (ready, ready_rx) = mpsc::channel();
    let (resume, resume_rx) = mpsc::channel();
    (
        Pause {
            ready,
            resume: resume_rx,
        },
        ready_rx,
        resume,
    )
}

fn park(domain: &ReclaimDomain) -> mpsc::Sender<()> {
    let (pause, ready, resume) = pause_pair();
    *domain.inner.before_park.lock().unwrap() = Some(pause);
    domain.inner.wake.notify();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    resume
}

fn exited(stats: &ReclamationStatsHandle) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !stats.read().exited {
        assert!(Instant::now() < deadline, "{:?}", stats.read());
        thread::yield_now();
    }
    assert_eq!(stats.read().occupied, 0);
}

#[test]
fn p45a3_scan_real_rollback_scratch_release_reuse_and_partial_close() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let configured =
        ReclaimDomain::configured(&crate::ConcurrencyOptions::default(), None).unwrap();
    let domain = &configured.domain;
    let stats = domain.stats();
    let go = park(domain);
    let scratch = domain.reserve(ResourceClass::Staging).unwrap();
    let scratch_id = scratch.index;
    let reservation = domain.reserve(ResourceClass::Storage).unwrap();
    let reserved_id = reservation.index;
    let mut owners = Vec::new();
    for n in 0..600 {
        owners.push(
            domain
                .build::<()>(CLASSES[n % CATEGORIES])
                .unwrap()
                .into_owner()
                .unwrap(),
        );
    }
    let (pause, ready, resume) = pause_pair();
    *domain.inner.after_scan_chunk.lock().unwrap() = Some(pause);
    go.send(()).unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let remaining = {
        let held = domain.inner.registry.lock().unwrap();
        let registry = held.as_ref().unwrap();
        let remaining = registry.scan_remaining.iter().sum::<usize>();
        assert!((1..=256).contains(&(602 - remaining)));
        for index in [scratch_id, reserved_id] {
            let slot = &registry.slots[index];
            assert!(
                slot.category_position
                    < registry.scan_remaining[slot.meta.class.load(Ordering::Relaxed)]
            );
        }
        remaining
    };
    drop(scratch);
    drop(reservation);
    let reused = domain.reserve(ResourceClass::Staging).unwrap();
    assert_eq!(reused.index, reserved_id);
    assert_eq!(reused.incarnation, 2);
    {
        let held = domain.inner.registry.lock().unwrap();
        let registry = held.as_ref().unwrap();
        assert_eq!(registry.scan_remaining.iter().sum::<usize>(), remaining - 2);
        let slot = &registry.slots[reused.index];
        assert!(slot.category_position >= registry.scan_remaining[ResourceClass::Staging.index()]);
        partition(registry);
    }
    drop(reused);
    domain.close(); // Actual worker is paused midway through an occupied sweep.
    drop(owners);
    resume.send(()).unwrap();
    drop(configured);
    exited(&stats);
    assert_eq!(stats.read_full().unwrap().scan_lag_us, Some(0));
}

#[derive(Default)]
struct Observed {
    id: usize,
    done: Option<mpsc::Sender<(usize, String)>>,
}
impl Drop for Observed {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            done.send((self.id, thread::current().name().unwrap_or("caller").into()))
                .unwrap();
        }
    }
}

fn observed(
    domain: &ReclaimDomain,
    class: ResourceClass,
    id: usize,
    done: &mpsc::Sender<(usize, String)>,
) -> OwnerPermit<Observed> {
    let mut value = domain.build::<Observed>(class).unwrap();
    value
        .mutate(|value| {
            value.id = id;
            value.done = Some(done.clone());
        })
        .unwrap();
    value.into_owner().unwrap()
}

#[test]
fn p45a3_scan_retirement_after_inspection_rescans_before_park() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for new_suffix in [false, true] {
        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let go = park(&domain);
        let (done, dropped) = mpsc::channel();
        let mut owners: Vec<_> = (0..600)
            .map(|id| Some(observed(&domain, ResourceClass::Generation, id, &done)))
            .collect();
        let (pause, ready, resume) = pause_pair();
        *domain.inner.after_scan_chunk.lock().unwrap() = Some(pause);
        go.send(()).unwrap();
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        let expected = if new_suffix {
            // This category was empty at capture; the new retired identity is
            // outside every remaining prefix, even after the cursor reaches it.
            let value = observed(&domain, ResourceClass::Cache, 600, &done);
            {
                let held = domain.inner.registry.lock().unwrap();
                let registry = held.as_ref().unwrap();
                assert_eq!(registry.scan_remaining[ResourceClass::Cache.index()], 0);
            }
            drop(value);
            600
        } else {
            let inspected = {
                let held = domain.inner.registry.lock().unwrap();
                let registry = held.as_ref().unwrap();
                registry.indexes[0][registry.scan_remaining[0]]
            };
            // The worker cannot see this retirement in the captured prefix.
            drop(owners[inspected].take());
            inspected
        };
        let (pause, parked, unpark) = pause_pair();
        *domain.inner.before_park.lock().unwrap() = Some(pause);
        resume.send(()).unwrap();
        parked.recv_timeout(Duration::from_secs(10)).unwrap();
        let result = dropped.try_recv();
        unpark.send(()).unwrap();
        assert_eq!(result.unwrap(), (expected, "overgraph-reclaimer".into()));
        drop(owners);
        domain.close();
        exited(&stats);
    }
}

#[test]
fn p45a3_scan_multichunk_pinned_heads_and_release_churn_preserve_class_fairness() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let domain = ReclaimDomain::new();
    let stats = domain.stats();
    let go = park(&domain);
    let (done, dropped) = mpsc::channel();
    let mut pins = Vec::new();
    for (id, class) in CLASSES.into_iter().enumerate() {
        let owner = observed(&domain, class, 600 + id, &done);
        pins.push(owner.read());
        drop(owner);
    }
    let owners: Vec<_> = (0..600)
        .map(|id| observed(&domain, CLASSES[id % CATEGORIES], id, &done))
        .collect();
    drop(owners);
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        scope.spawn(|| {
            while !stop.load(Ordering::Acquire) {
                for pin in &pins {
                    drop(pin.clone());
                }
                drop(domain.reserve(ResourceClass::Staging).unwrap());
                drop(domain.reserve(ResourceClass::Cache).unwrap());
            }
        });
        go.send(()).unwrap();
        let mut seen = vec![false; 600];
        for n in 0..600 {
            let (id, worker) = dropped.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_eq!(worker, "overgraph-reclaimer");
            assert!(id < 600 && !std::mem::replace(&mut seen[id], true));
            if n < 12 {
                assert_eq!(id % CATEGORIES, n % CATEGORIES);
            }
        }
        stop.store(true, Ordering::Release);
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while stats.read().occupied != CATEGORIES {
        assert!(Instant::now() < deadline, "{:?}", stats.read());
        thread::yield_now();
    }
    drop(pins);
    domain.close();
    exited(&stats);
}

#[test]
fn p45a3_scan_geometric_growth_during_partial_sweep_preserves_prefix_and_metadata() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut registry = Registry::new(64);
    let wake = Wake::new();
    let initial: Vec<_> = (0..64)
        .map(|n| claim(&mut registry, CLASSES[n % CATEGORIES]))
        .collect();
    let meta = registry.slots[initial[0].0].meta.clone();
    registry.begin_scan();
    let mut class = 0;
    let mut seen = Vec::new();
    for _ in 0..17 {
        let index = registry.next_scan_entry(&mut class).unwrap();
        seen.push((
            index,
            registry.slots[index]
                .meta
                .incarnation
                .load(Ordering::Acquire),
        ));
    }
    registry.grow().unwrap();
    assert_eq!(registry.slots.len(), 128);
    assert!(Arc::ptr_eq(&meta, &registry.slots[initial[0].0].meta));
    let added: Vec<_> = (0..64)
        .map(|n| claim(&mut registry, CLASSES[n % CATEGORIES]))
        .collect();
    registry.grow().unwrap();
    assert_eq!(registry.slots.len(), 256);
    sweep(&mut registry, &mut seen, &mut class);
    seen.sort_unstable();
    let mut expected = initial.clone();
    expected.sort_unstable();
    assert_eq!(seen, expected);
    let removed = added[7];
    free_slot(&mut registry, removed.0, &wake);
    let reused = claim(&mut registry, ResourceClass::Cache);
    assert_eq!(reused.0, removed.0);
    assert_eq!(reused.1, removed.1 + 1);
    registry.begin_scan();
    seen.clear();
    class = 0;
    sweep(&mut registry, &mut seen, &mut class);
    assert_eq!(seen.len(), 128);
    assert!(seen.contains(&reused));
    partition(&registry);
}
