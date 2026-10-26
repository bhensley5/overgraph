//! Root-free counts and worker timing observations.
use super::*;
use crate::types::ReclamationHistogram;
const UNOBSERVED: u8 = 0;
const PINNED: u8 = 1;
const ELIGIBLE: u8 = 2;
const DROPPING: u8 = 3;
const NO_TIME: u64 = u64::MAX;
fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}
fn saturating_increment(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(amount))
    });
}
struct Histogram {
    buckets: [AtomicU64; 64],
    samples: AtomicU64,
    max_us: AtomicU64,
    observed: AtomicBool,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            samples: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            observed: AtomicBool::new(false),
        }
    }

    fn record(&self, duration_us: u64) {
        let bucket = if duration_us == 0 {
            0
        } else {
            (u64::BITS - 1 - duration_us.leading_zeros()) as usize
        };
        saturating_increment(&self.buckets[bucket], 1);
        saturating_increment(&self.samples, 1);
        self.max_us.fetch_max(duration_us, Ordering::Relaxed);
        self.observed.store(true, Ordering::Release);
    }

    fn read(&self) -> ReclamationHistogram {
        // The first observation is published only after its counters. A reader
        // that precedes that publication returns an empty sampled histogram,
        // never a positive sample count paired with an absent maximum.
        if !self.observed.load(Ordering::Acquire) {
            return ReclamationHistogram::empty();
        }
        ReclamationHistogram {
            buckets: std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
            samples: self.samples.load(Ordering::Relaxed),
            max_us: Some(self.max_us.load(Ordering::Relaxed)),
        }
    }
}

pub(super) struct Telemetry {
    started: Instant,
    retired_objects: AtomicUsize,
    dropping_objects: AtomicUsize,
    oldest_retired_us: AtomicU64,
    scan_started_us: AtomicU64,
    scanned: AtomicBool,
    scan_pending: AtomicBool,
    release_to_eligibility: Histogram,
    eligibility_to_drop: Histogram,
    worker_wall_us: AtomicU64,
    worked: AtomicBool,
    pub(super) active_generations: AtomicU64,
    pub(super) frozen_generations: AtomicU64,
    pub(super) leased_participants: AtomicU64,
    pub(super) participant_limit: AtomicU64,
    pub(super) active_sessions: AtomicU64,
    pub(super) draining_sessions: AtomicU64,
    pub(super) closing: AtomicBool,
}

impl Telemetry {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            retired_objects: AtomicUsize::new(0),
            dropping_objects: AtomicUsize::new(0),
            oldest_retired_us: AtomicU64::new(NO_TIME),
            scan_started_us: AtomicU64::new(0),
            scanned: AtomicBool::new(false),
            scan_pending: AtomicBool::new(false),
            release_to_eligibility: Histogram::new(),
            eligibility_to_drop: Histogram::new(),
            worker_wall_us: AtomicU64::new(0),
            worked: AtomicBool::new(false),
            active_generations: AtomicU64::new(0),
            frozen_generations: AtomicU64::new(0),
            leased_participants: AtomicU64::new(0),
            participant_limit: AtomicU64::new(0),
            active_sessions: AtomicU64::new(0),
            draining_sessions: AtomicU64::new(0),
            closing: AtomicBool::new(false),
        }
    }

    fn now_us(&self) -> u64 {
        micros(self.started.elapsed()).min(NO_TIME - 1)
    }

    pub(super) fn retired(&self, meta: &SlotMeta) {
        let now = self.now_us();
        meta.retired_us.store(now, Ordering::Relaxed);
        let previous = self
            .retired_objects
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(1)
            })
            .unwrap_or_else(|_| invariant("retirement count overflow"));
        if previous == 0 {
            self.oldest_retired_us.store(now, Ordering::Release);
        } else {
            self.oldest_retired_us.fetch_min(now, Ordering::AcqRel);
        }
    }

    fn observe(&self, meta: &SlotMeta, next: u8) {
        let previous = meta.observation.swap(next, Ordering::AcqRel);
        if previous == DROPPING {
            self.dropping_objects.fetch_sub(1, Ordering::AcqRel);
        }
        if next == DROPPING {
            self.dropping_objects.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(super) fn pinned(&self, meta: &SlotMeta) {
        self.observe(meta, PINNED);
    }

    pub(super) fn eligible(&self, meta: &SlotMeta) {
        let now = self.now_us();
        meta.eligible_us.store(now, Ordering::Relaxed);
        self.release_to_eligibility
            .record(now.saturating_sub(meta.retired_us.load(Ordering::Relaxed)));
        self.observe(meta, ELIGIBLE);
    }

    pub(super) fn dropping(&self, meta: &SlotMeta) {
        self.eligibility_to_drop.record(
            self.now_us()
                .saturating_sub(meta.eligible_us.load(Ordering::Relaxed)),
        );
        self.observe(meta, DROPPING);
    }

    pub(super) fn freed(&self, meta: &SlotMeta, retired: bool) {
        if retired {
            self.observe(meta, UNOBSERVED);
            self.retired_objects
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                    old.checked_sub(1)
                })
                .unwrap_or_else(|_| invariant("retirement count underflow"));
        }
        // Worker-owned complete scans refresh the oldest observation. A new
        // retirement can lower this value concurrently; snapshots are sampled.
    }

    pub(super) fn begin_scan(&self) {
        self.scan_started_us.store(self.now_us(), Ordering::Relaxed);
        self.scan_pending.store(true, Ordering::Release);
        self.scanned.store(true, Ordering::Release);
    }

    pub(super) fn finish_scan(&self, oldest: Option<u64>) {
        // Only the worker writes completed scan observations. Foreground
        // retirement remains visible through its amount and fetch_min timestamp.
        if let Some(oldest) = oldest {
            self.oldest_retired_us.store(oldest, Ordering::Release);
        }
        self.scan_pending.store(false, Ordering::Release);
    }

    pub(super) fn worked(&self, since: Instant) {
        saturating_increment(&self.worker_wall_us, micros(since.elapsed()));
        self.worked.store(true, Ordering::Release);
    }

    pub(super) fn fill(&self, snapshot: &mut crate::ReclamationStats) {
        snapshot.retired_objects = self.retired_objects.load(Ordering::Acquire) as u64;
        snapshot.dropping_objects = self.dropping_objects.load(Ordering::Acquire) as u64;
        snapshot.active_generations = self.active_generations.load(Ordering::Acquire);
        snapshot.frozen_generations = self.frozen_generations.load(Ordering::Acquire);
        snapshot.leased_participants = self.leased_participants.load(Ordering::Acquire);
        snapshot.participant_limit = self.participant_limit.load(Ordering::Acquire);
        snapshot.active_sessions = self.active_sessions.load(Ordering::Acquire);
        snapshot.draining_sessions = self.draining_sessions.load(Ordering::Acquire);
        let oldest = self.oldest_retired_us.load(Ordering::Acquire);
        snapshot.oldest_retirement_age_us = (snapshot.retired_objects != 0 && oldest != NO_TIME)
            .then(|| self.now_us().saturating_sub(oldest));
        snapshot.scan_lag_us = self.scanned.load(Ordering::Acquire).then(|| {
            if self.scan_pending.load(Ordering::Acquire) {
                self.now_us()
                    .saturating_sub(self.scan_started_us.load(Ordering::Relaxed))
            } else {
                0
            }
        });
        snapshot.release_to_eligibility_us = self.release_to_eligibility.read();
        snapshot.eligibility_to_drop_us = self.eligibility_to_drop.read();
        snapshot.worker_wall_us = self
            .worked
            .load(Ordering::Acquire)
            .then(|| self.worker_wall_us.load(Ordering::Relaxed));
        // No platform thread-CPU clock is installed. Absence is explicitly
        // unavailable, never a zero or a substitution of active wall time.
        snapshot.worker_cpu_us = None;
    }
}
