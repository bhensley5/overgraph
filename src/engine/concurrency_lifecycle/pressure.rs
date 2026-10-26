//! Bounded preacceptance custody and original-deadline resource waiting.
//!
//! The coordinator retains the same move-only position while preparing or parked.
//! Its caller must release sessions, execution custody and progress-dependent partial
//! temporary ownership before waiting, and service ready lifecycle work before retrying the head.
//! This module holds only fixed metadata; it cannot keep an engine graph alive.

use super::{fail_stop, Outcome, ResourceLimit};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

// A release in another database may restore the shared participant pool without
// notifying this database. One timed recheck bounds that delay without a global
// waiter list, a busy retry loop or another public tuning option.
const SHARED_RECHECK: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) struct Deadline {
    started: Instant,
    expires: Instant,
}

impl Deadline {
    /// Call before input measurement/construction, then move this value through
    /// every preparation and retry. Zero permits a probe but never a resource wait.
    pub(in crate::engine) fn new(timeout_ms: u64) -> Result<Self, Outcome> {
        Self::at(Instant::now(), timeout_ms)
    }

    pub(in crate::engine) fn from_started(
        started: Instant,
        timeout_ms: u64,
    ) -> Result<Self, Outcome> {
        Self::at(started, timeout_ms)
    }

    fn at(started: Instant, timeout_ms: u64) -> Result<Self, Outcome> {
        let expires = started
            .checked_add(Duration::from_millis(timeout_ms))
            .ok_or(Outcome::Refused(
                "admission timeout exceeds monotonic clock range",
            ))?;
        Ok(Self { started, expires })
    }

    pub(in crate::engine) fn started(self) -> Instant {
        self.started
    }

    pub(in crate::engine) fn expires(self) -> Instant {
        self.expires
    }

    pub(in crate::engine) fn remaining(self) -> Option<Duration> {
        self.expires
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
    }

    pub(in crate::engine) fn expired(self) -> bool {
        self.remaining().is_none()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) struct ReadinessEpoch(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum PressureWake {
    Notified,
    Recheck,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Closed,
    Fenced,
    EpochExhausted,
}

impl Stop {
    fn outcome(self) -> Outcome {
        Outcome::Refused(match self {
            Self::Closed => "closed",
            Self::Fenced => "fenced",
            Self::EpochExhausted => "pressure readiness epoch exhausted",
        })
    }
}

struct State<const N: usize> {
    positions: [Option<u64>; N],
    next_ticket: u64,
    epoch: u64,
    stop: Option<Stop>,
}

impl<const N: usize> State<N> {
    fn advance(&mut self) {
        match self.epoch.checked_add(1) {
            Some(epoch) => self.epoch = epoch,
            None => self.stop = Some(Stop::EpochExhausted),
        }
    }

    fn check_open(&self) -> Result<(), Outcome> {
        self.stop.map_or(Ok(()), |stop| Err(stop.outcome()))
    }
}

/// N is the existing private ordinary queue limit. Slots stay occupied after
/// dequeue and across parks; neither action manufactures another queue position.
/// No waiter, graph, callback or native worker is stored here.
pub(in crate::engine) struct Pressure<const N: usize> {
    state: Mutex<State<N>>,
    cv: Condvar,
}

impl<const N: usize> Pressure<N> {
    pub(in crate::engine) fn new() -> Self {
        const { assert!(N > 0) };
        Self {
            state: Mutex::new(State {
                positions: [None; N],
                next_ticket: 0,
                epoch: 0,
                stop: None,
            }),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<N>> {
        self.state
            .lock()
            .unwrap_or_else(|_| fail_stop("pressure state poisoned"))
    }

    /// Call under ordinary queue insertion authority so ticket and command order
    /// agree. The token is not Clone and dropping a canceled position frees only
    /// that position, never another request's custody.
    pub(in crate::engine) fn try_enter(self: &Arc<Self>) -> Result<QueuePosition<N>, Outcome> {
        let mut state = self.lock();
        state.check_open()?;
        let index =
            state
                .positions
                .iter()
                .position(Option::is_none)
                .ok_or(Outcome::ResourceLimit(ResourceLimit {
                    resource: "request_queue",
                    requested: 1,
                    limit: N,
                    retryable: true,
                }))?;
        let ticket = state.next_ticket.checked_add(1).ok_or(Outcome::Refused(
            "request queue position incarnation exhausted",
        ))?;
        state.next_ticket = ticket;
        state.positions[index] = Some(ticket);
        Ok(QueuePosition {
            pressure: self.clone(),
            index,
            ticket,
        })
    }

    pub(in crate::engine) fn occupied(&self) -> usize {
        self.lock().positions.iter().filter(|p| p.is_some()).count()
    }

    /// Observe BEFORE probing resources outside this mutex. A release between
    /// the failed probe and wait changes this predicate, so wait cannot miss it.
    pub(in crate::engine) fn observe(&self) -> ReadinessEpoch {
        ReadinessEpoch(self.lock().epoch)
    }

    /// Publish completion/resource readiness after publishing its actual state.
    pub(in crate::engine) fn notify(&self) {
        let mut state = self.lock();
        state.advance();
        self.cv.notify_all();
    }

    pub(in crate::engine) fn close(&self) {
        let mut state = self.lock();
        if state.stop.is_none() {
            state.stop = Some(Stop::Closed);
        }
        self.cv.notify_all();
    }

    pub(in crate::engine) fn fence(&self) {
        let mut state = self.lock();
        if state.stop != Some(Stop::EpochExhausted) {
            state.stop = Some(Stop::Fenced);
        }
        self.cv.notify_all();
    }

    /// This is preacceptance-only. The caller has already failed a complete
    /// resource probe. It must not call this after WAL acceptance, or with a
    /// participant, engine/registry lock or progress-dependent partial custody.
    /// `self_dependent` means that completion requires the caller's own retained
    /// input to be released; no timeout can make that dependency safe.
    pub(in crate::engine) fn wait(
        &self,
        observed: ReadinessEpoch,
        deadline: Deadline,
        resource: ResourceLimit,
        self_dependent: bool,
    ) -> Result<PressureWake, Outcome> {
        self.wait_with_hook(observed, deadline, resource, self_dependent, || {})
    }

    fn wait_with_hook(
        &self,
        observed: ReadinessEpoch,
        deadline: Deadline,
        resource: ResourceLimit,
        self_dependent: bool,
        before_wait: impl FnOnce(),
    ) -> Result<PressureWake, Outcome> {
        if !resource.retryable || self_dependent {
            return Err(Outcome::ResourceLimit(resource));
        }
        let mut state = self.lock();
        let mut before_wait = Some(before_wait);
        loop {
            state.check_open()?;
            let remaining = deadline
                .remaining()
                .ok_or(Outcome::ResourceLimit(resource))?;
            if state.epoch != observed.0 {
                return Ok(PressureWake::Notified);
            }
            if let Some(before_wait) = before_wait.take() {
                before_wait();
            }
            let (next, timeout) = self
                .cv
                .wait_timeout(state, remaining.min(SHARED_RECHECK))
                .unwrap_or_else(|_| fail_stop("pressure wait poisoned"));
            state = next;
            state.check_open()?;
            if deadline.expired() {
                return Err(Outcome::ResourceLimit(resource));
            }
            if state.epoch != observed.0 {
                return Ok(PressureWake::Notified);
            }
            if timeout.timed_out() {
                return Ok(PressureWake::Recheck);
            }
            // A spurious wake never replenishes time or admits another waiter.
        }
    }
}

/// The same value moves from queued to running to parked state. Release on
/// settlement, never merely because the coordinator removed the command from
/// VecDeque or parked it.
pub(in crate::engine) struct QueuePosition<const N: usize> {
    pressure: Arc<Pressure<N>>,
    index: usize,
    ticket: u64,
}

impl<const N: usize> QueuePosition<N> {
    pub(in crate::engine) fn ticket(&self) -> u64 {
        self.ticket
    }

    pub(in crate::engine) fn is_head(&self) -> Result<bool, Outcome> {
        let state = self.pressure.lock();
        state.check_open()?;
        if state.positions[self.index] != Some(self.ticket) {
            fail_stop("stale pressure queue position");
        }
        Ok(state.positions.iter().flatten().min() == Some(&self.ticket))
    }
}

impl<const N: usize> Drop for QueuePosition<N> {
    fn drop(&mut self) {
        let mut state = self.pressure.lock();
        if state.positions[self.index] != Some(self.ticket) {
            fail_stop("duplicate pressure queue position release");
        }
        state.positions[self.index] = None;
        state.advance();
        self.pressure.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn resource(retryable: bool) -> ResourceLimit {
        ResourceLimit {
            resource: "general_memory_bytes",
            requested: 37,
            limit: 29,
            retryable,
        }
    }

    fn assert_resource(result: Result<PressureWake, Outcome>, expected: ResourceLimit) {
        assert!(matches!(result, Err(Outcome::ResourceLimit(actual)) if actual == expected));
    }

    fn await_notification<const N: usize>(
        pressure: &Pressure<N>,
        observed: ReadinessEpoch,
        deadline: Deadline,
        before_wait: impl FnOnce(),
    ) -> Result<PressureWake, Outcome> {
        let mut result =
            pressure.wait_with_hook(observed, deadline, resource(true), false, before_wait);
        while matches!(result, Ok(PressureWake::Recheck)) {
            // A heavily scheduled notifier may run after the safety recheck;
            // that permitted wake does not establish or break the causal order.
            result = pressure.wait(observed, deadline, resource(true), false);
        }
        result
    }

    #[test]
    fn p45a2_pressure_original_deadline_checks_overflow_and_zero_wait() {
        let started = Instant::now();
        let deadline = Deadline::at(started, 100).unwrap();
        assert_eq!(deadline.started(), started);
        assert_eq!(deadline.expires(), started + Duration::from_millis(100));
        // Find the actual platform's final representable whole-second offset;
        // u64::MAX milliseconds alone remains representable on some Unix clocks.
        let mut low = 0u64;
        let mut high = u64::MAX;
        while low < high {
            let middle = low + (high - low) / 2 + 1;
            if started.checked_add(Duration::from_secs(middle)).is_some() {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let near_limit = started.checked_add(Duration::from_secs(low)).unwrap();
        assert!(Deadline::at(near_limit, u64::MAX).is_err());
        let pressure = Pressure::<2>::new();
        let observed = pressure.observe();
        pressure.notify();
        assert_resource(
            pressure.wait(observed, Deadline::new(0).unwrap(), resource(true), false),
            resource(true),
        );
    }

    #[test]
    fn p45a2_pressure_retained_head_preserves_capacity_and_fifo_with_holes() {
        let pressure = Arc::new(Pressure::<3>::new());
        let head = pressure.try_enter().unwrap();
        let middle = pressure.try_enter().unwrap();
        let last = pressure.try_enter().unwrap();
        assert!(head.is_head().unwrap());
        assert!(!last.is_head().unwrap());
        let parked = head; // Dequeue/repark only moves custody.
        assert_eq!(pressure.occupied(), 3);
        assert!(matches!(
            pressure.try_enter(),
            Err(Outcome::ResourceLimit(ResourceLimit {
                resource: "request_queue",
                requested: 1,
                limit: 3,
                retryable: true,
            }))
        ));
        drop(middle); // Cancellation cannot make the later request the head.
        let replacement = pressure.try_enter().unwrap();
        assert!(replacement.ticket() > last.ticket());
        assert!(!replacement.is_head().unwrap());
        drop(parked);
        assert!(last.is_head().unwrap());
        drop(last);
        assert!(replacement.is_head().unwrap());
        drop(replacement);
        assert_eq!(pressure.occupied(), 0);
    }

    #[test]
    fn p45a2_pressure_release_between_probe_and_wait_is_not_lost() {
        let pressure = Arc::new(Pressure::<2>::new());
        let deadline = Deadline::new(1000).unwrap();
        let observed = pressure.observe(); // Before the failed resource probe.
        let (failed_tx, failed_rx) = mpsc::sync_channel(0);
        let (released_tx, released_rx) = mpsc::sync_channel(0);
        let worker = pressure.clone();
        let release = std::thread::spawn(move || {
            failed_rx.recv().unwrap();
            worker.notify();
            released_tx.send(()).unwrap();
        });
        failed_tx.send(()).unwrap();
        released_rx.recv().unwrap();
        assert_eq!(
            pressure
                .wait(observed, deadline, resource(true), false)
                .unwrap(),
            PressureWake::Notified,
        );
        release.join().unwrap();
        assert_eq!(
            deadline.expires(),
            deadline.started() + Duration::from_millis(1000)
        );
    }

    #[test]
    fn p45a2_pressure_notification_races_atomic_predicate_wait_without_reset() {
        let pressure = Arc::new(Pressure::<1>::new());
        let deadline = Deadline::new(1000).unwrap();
        let (checked_tx, checked_rx) = mpsc::sync_channel(0);
        let (proceed_tx, proceed_rx) = mpsc::sync_channel(0);
        let (notify_tx, notify_rx) = mpsc::sync_channel(0);
        let notifier = pressure.clone();
        let observed = pressure.observe();
        let waiter = pressure.clone();
        let waiting = std::thread::spawn(move || {
            await_notification(&waiter, observed, deadline, || {
                // Hold the real wait path's predicate mutex while the notifier
                // begins its lock attempt. No sleep establishes this schedule.
                checked_tx.send(()).unwrap();
                proceed_rx.recv().unwrap();
            })
        });
        checked_rx.recv().unwrap();
        let release = std::thread::spawn(move || {
            notify_tx.send(()).unwrap();
            notifier.notify();
        });
        notify_rx.recv().unwrap();
        proceed_tx.send(()).unwrap();
        assert_eq!(waiting.join().unwrap().unwrap(), PressureWake::Notified);
        release.join().unwrap();
    }

    #[test]
    fn p45a2_pressure_intrinsic_self_dependency_and_terminal_refusals_are_immediate() {
        let pressure = Pressure::<1>::new();
        let deadline = Deadline::new(60_000).unwrap();
        assert_resource(
            pressure.wait(pressure.observe(), deadline, resource(false), false),
            resource(false),
        );
        assert_resource(
            pressure.wait(pressure.observe(), deadline, resource(true), true),
            resource(true),
        );
        let observed = pressure.observe();
        pressure.close();
        assert!(matches!(
            pressure.wait(observed, deadline, resource(true), false),
            Err(Outcome::Refused("closed"))
        ));
        pressure.fence();
        assert!(matches!(
            pressure.wait(observed, deadline, resource(true), false),
            Err(Outcome::Refused("fenced"))
        ));
    }

    #[test]
    fn p45a2_pressure_shared_participant_recheck_and_original_timeout() {
        let pressure = Pressure::<1>::new();
        let deadline = Deadline::new(1000).unwrap();
        let expires = deadline.expires();
        assert_eq!(
            pressure
                .wait(pressure.observe(), deadline, resource(true), false)
                .unwrap(),
            PressureWake::Recheck
        );
        // Repeated notifications cannot make an expired request runnable.
        let expired = Deadline::at(Instant::now() - Duration::from_secs(1), 1).unwrap();
        for _ in 0..3 {
            let observed = pressure.observe();
            pressure.notify();
            assert_resource(
                pressure.wait(observed, expired, resource(true), false),
                resource(true),
            );
        }
        assert_eq!(deadline.expires(), expires);
    }

    #[test]
    fn p45a2_pressure_close_and_fence_wake_real_wait_and_preserve_custody() {
        for fenced in [false, true] {
            let pressure = Arc::new(Pressure::<1>::new());
            let retained = pressure.try_enter().unwrap();
            let observed = pressure.observe();
            let deadline = Deadline::new(1000).unwrap();
            let (checked_tx, checked_rx) = mpsc::sync_channel(0);
            let (proceed_tx, proceed_rx) = mpsc::sync_channel(0);
            let waiter = pressure.clone();
            let waiting = std::thread::spawn(move || {
                await_notification(&waiter, observed, deadline, || {
                    checked_tx.send(()).unwrap();
                    proceed_rx.recv().unwrap();
                })
            });
            checked_rx.recv().unwrap();
            let terminal = pressure.clone();
            let (started_tx, started_rx) = mpsc::sync_channel(0);
            let closing = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                if fenced {
                    terminal.fence();
                } else {
                    terminal.close();
                }
            });
            started_rx.recv().unwrap();
            proceed_tx.send(()).unwrap();
            let result = waiting.join().unwrap();
            assert!(
                matches!(result, Err(Outcome::Refused(reason)) if reason == if fenced { "fenced" } else { "closed" })
            );
            closing.join().unwrap();
            assert_eq!(pressure.occupied(), 1);
            assert!(pressure.try_enter().is_err());
            drop(retained);
            assert_eq!(pressure.occupied(), 0);
        }
    }

    #[test]
    fn p45a2_pressure_ticket_and_notification_overflow_never_wrap() {
        let pressure = Arc::new(Pressure::<1>::new());
        pressure.lock().next_ticket = u64::MAX;
        assert!(matches!(
            pressure.try_enter(),
            Err(Outcome::Refused(
                "request queue position incarnation exhausted"
            ))
        ));
        assert_eq!(pressure.occupied(), 0);
        pressure.lock().epoch = u64::MAX;
        pressure.notify();
        assert!(matches!(
            pressure.try_enter(),
            Err(Outcome::Refused("pressure readiness epoch exhausted"))
        ));
    }
}
