//! Separate client, admitted service, passive data, and durable transition authority.
#![allow(dead_code)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::Thread;

mod pressure;
#[allow(unused_imports)] // The adapter and later runtime share this final protocol.
pub(super) use pressure::{Deadline, Pressure, PressureWake, QueuePosition, ReadinessEpoch};

pub(super) use crate::types::CommitOutcomeState as CommitState;

#[derive(Debug)]
pub(super) enum Outcome {
    Refused(&'static str),
    ResourceLimit(ResourceLimit),
    Commit { state: CommitState, message: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ResourceLimit {
    pub(super) resource: &'static str,
    pub(super) requested: usize,
    pub(super) limit: usize,
    pub(super) retryable: bool,
}

impl From<Outcome> for crate::error::EngineError {
    fn from(outcome: Outcome) -> Self {
        // Every supported usize is representable without loss in the public
        // u64 amount. Refuse a future wider machine at compile time rather than
        // silently truncating an accepted private outcome.
        const { assert!(usize::BITS <= u64::BITS) };
        match outcome {
            Outcome::Refused(message) => Self::InvalidOperation(message.into()),
            Outcome::ResourceLimit(limit) => Self::ResourceLimit {
                resource: limit.resource,
                requested: limit.requested as u64,
                limit: limit.limit as u64,
                retryable: limit.retryable,
            },
            Outcome::Commit { state, message } => Self::CommitOutcome { state, message },
        }
    }
}

pub(super) struct CommitFence {
    fenced: AtomicBool,
    transition: Mutex<()>,
}

impl CommitFence {
    pub(super) fn new() -> Self {
        Self {
            fenced: AtomicBool::new(false),
            transition: Mutex::new(()),
        }
    }
    pub(super) fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }
    pub(super) fn gate(&self) -> Result<DurableGate<'_>, Outcome> {
        if self.is_fenced() {
            return Err(Outcome::Refused("fenced"));
        }
        let guard = self
            .transition
            .lock()
            .unwrap_or_else(|_| fail_stop("durable gate poisoned"));
        if self.is_fenced() {
            return Err(Outcome::Refused("fenced"));
        }
        Ok(DurableGate {
            fence: self,
            _guard: guard,
        })
    }
    pub(super) fn establish(&self) {
        let _guard = self
            .transition
            .lock()
            .unwrap_or_else(|_| fail_stop("durable gate poisoned"));
        self.fenced.store(true, Ordering::Release);
    }
    // Only the drained shutdown executor uses this capability. It stops every
    // later durable publisher and serializes appropriate WAL/handle shutdown,
    // including when the ordinary publisher gate already refuses uncertainty.
    pub(super) fn shutdown_gate(&self) -> DurableGate<'_> {
        let guard = self
            .transition
            .lock()
            .unwrap_or_else(|_| fail_stop("durable shutdown gate poisoned"));
        self.fenced.store(true, Ordering::Release);
        DurableGate {
            fence: self,
            _guard: guard,
        }
    }
}

pub(super) struct DurableGate<'a> {
    fence: &'a CommitFence,
    _guard: MutexGuard<'a, ()>,
}
impl DurableGate<'_> {
    // Called on uncertainty WHILE holding the gate, never recursively locks.
    pub(super) fn fence(&self) {
        self.fence.fenced.store(true, Ordering::Release);
    }
}

pub(super) fn fail_stop(message: &str) -> ! {
    eprintln!("production-concurrency fail-stop: {message}");
    std::process::abort()
}

struct Drain {
    admitted: usize,
    accepting: bool,
    completed: bool,
}

pub(super) struct Lifecycle {
    clients: AtomicUsize,
    initial_claimed: AtomicBool,
    requested: AtomicBool,
    worker: OnceLock<Thread>,
    drain: Mutex<Drain>,
    cv: Condvar,
    #[cfg(test)]
    before_drain_wait: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

/// Sealed fixed lifecycle metadata, independent of the domain graph.
pub(super) struct LifecycleRef(Option<Arc<Lifecycle>>);
impl Clone for LifecycleRef {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live lifecycle").clone()))
    }
}
impl std::ops::Deref for LifecycleRef {
    type Target = Lifecycle;
    fn deref(&self) -> &Lifecycle {
        self.0.as_ref().expect("live lifecycle")
    }
}
impl Drop for LifecycleRef {
    fn drop(&mut self) {
        drop(Arc::into_inner(
            self.0.take().expect("lifecycle dropped twice"),
        ));
    }
}
impl LifecycleRef {
    pub(super) fn initial_owner(&self) -> RuntimeOwnerPermit {
        if self.initial_claimed.swap(true, Ordering::AcqRel) {
            fail_stop("duplicate initial owner");
        }
        RuntimeOwnerPermit {
            lifecycle: Some(self.clone()),
        }
    }
    pub(super) fn admit(&self) -> Result<OperationPermit, Outcome> {
        let mut drain = self.drain.lock().unwrap();
        if !drain.accepting || self.close_requested() {
            return Err(Outcome::Refused("closed"));
        }
        drain.admitted = drain
            .admitted
            .checked_add(1)
            .unwrap_or_else(|| fail_stop("operation count exhausted"));
        Ok(OperationPermit {
            lifecycle: Some(self.clone()),
        })
    }
}

pub(super) struct RuntimeOwnerPermit {
    lifecycle: Option<LifecycleRef>,
}
pub(super) struct OperationPermit {
    lifecycle: Option<LifecycleRef>,
}

impl Lifecycle {
    pub(super) fn allocate() -> LifecycleRef {
        LifecycleRef(Some(Arc::new(Self {
            clients: AtomicUsize::new(1),
            initial_claimed: AtomicBool::new(false),
            requested: AtomicBool::new(false),
            worker: OnceLock::new(),
            drain: Mutex::new(Drain {
                admitted: 0,
                accepting: true,
                completed: false,
            }),
            cv: Condvar::new(),
            #[cfg(test)]
            before_drain_wait: Mutex::new(None),
        })))
    }
    pub(super) fn register_coordinator(&self) {
        self.worker
            .set(std::thread::current())
            .unwrap_or_else(|_| fail_stop("second shutdown executor"));
        if self.requested.load(Ordering::Acquire) {
            self.wake();
        }
    }
    pub(super) fn wake(&self) {
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
    }
    pub(super) fn request_close(&self) {
        // Embedded pre-reserved request; independent of ordinary queue capacity.
        self.requested.store(true, Ordering::Release);
        self.wake();
    }
    pub(super) fn close_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
    pub(super) fn stop_admission(&self) {
        self.drain.lock().unwrap().accepting = false;
    }
    // Coordinator must settle ALL queued service-dependent work before calling
    // this independent-call drain. Close waiters never own OperationPermit.
    pub(super) fn has_admitted(&self) -> bool {
        self.drain.lock().unwrap().admitted != 0
    }

    pub(super) fn drain_independent(&self) {
        let mut drain = self.drain.lock().unwrap();
        drain.accepting = false;
        while drain.admitted != 0 {
            #[cfg(test)]
            if let Some((ready, go)) = self.before_drain_wait.lock().unwrap().take() {
                ready.send(()).unwrap();
                go.recv().unwrap();
            }
            drain = self.cv.wait(drain).unwrap();
        }
    }
    pub(super) fn complete_close(&self) {
        let mut drain = self.drain.lock().unwrap();
        if drain.admitted != 0 {
            fail_stop("close before operation settlement");
        }
        drain.completed = true;
        self.cv.notify_all();
    }
    pub(super) fn wait_closed(&self) {
        let mut drain = self.drain.lock().unwrap();
        while !drain.completed {
            drain = self.cv.wait(drain).unwrap();
        }
    }
}

impl Clone for RuntimeOwnerPermit {
    fn clone(&self) -> Self {
        let lifecycle = self.lifecycle.as_ref().expect("open owner");
        lifecycle
            .clients
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n == 0 {
                    None
                } else {
                    n.checked_add(1)
                }
            })
            .unwrap_or_else(|_| fail_stop("runtime owner resurrection/overflow"));
        Self {
            lifecycle: Some(lifecycle.clone()),
        }
    }
}
impl Drop for RuntimeOwnerPermit {
    fn drop(&mut self) {
        if let Some(lifecycle) = self.lifecycle.take() {
            let old = lifecycle.clients.fetch_sub(1, Ordering::AcqRel);
            if old == 0 {
                fail_stop("runtime owner underflow");
            }
            if old == 1 {
                lifecycle.request_close();
            }
        }
    }
}
impl Drop for OperationPermit {
    fn drop(&mut self) {
        if let Some(lifecycle) = self.lifecycle.take() {
            let mut drain = lifecycle.drain.lock().unwrap();
            if drain.admitted == 0 {
                fail_stop("operation underflow");
            }
            drain.admitted -= 1;
            // Same mutex as the predicate/wait: no check-vs-wait lost wake.
            if drain.admitted == 0 {
                lifecycle.cv.notify_all();
            }
            lifecycle.wake();
        }
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
mod tests {
    use super::super::participant::TEST_SERIAL;
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn p45a2_private_outcomes_preserve_public_amounts_and_accepted_state() {
        let error = crate::EngineError::from(Outcome::ResourceLimit(ResourceLimit {
            resource: "general_memory_bytes",
            requested: usize::MAX,
            limit: 17,
            retryable: false,
        }));
        assert!(matches!(error, crate::EngineError::ResourceLimit {
            resource: "general_memory_bytes", requested, limit: 17, retryable: false,
        } if requested == usize::MAX as u64));
        for state in [CommitState::Committed, CommitState::Indeterminate] {
            let message = "accepted operation retained its outcome".to_owned();
            let address = message.as_ptr();
            let error = crate::EngineError::from(Outcome::Commit { state, message });
            let crate::EngineError::CommitOutcome {
                state: actual,
                message,
            } = error
            else {
                panic!("accepted operation became rejection")
            };
            assert_eq!(actual, state);
            assert_eq!(message.as_ptr(), address); // Ownership moves, no new copy.
        }
        assert!(
            matches!(crate::EngineError::from(Outcome::Refused("closed domain")),
            crate::EngineError::InvalidOperation(message) if message == "closed domain")
        );
    }
    #[test]
    fn p45a1_lifecycle_owner_orders_task_cancellation_and_drain() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for db_first in [true, false] {
            let lifecycle = Lifecycle::allocate();
            let db = lifecycle.initial_owner();
            let txn = db.clone();
            if db_first {
                drop(db);
                assert!(!lifecycle.close_requested());
                drop(lifecycle.admit().unwrap());
                drop(txn);
            } else {
                drop(txn);
                assert!(!lifecycle.close_requested());
                drop(db);
            }
            assert!(lifecycle.close_requested());
        }
        let lifecycle = Lifecycle::allocate();
        let owner = lifecycle.initial_owner();
        let accepted = lifecycle.admit().unwrap();
        // Caller can disappear while service owns the accepted operation.
        drop(owner);
        let (settle_tx, settle_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let service = lifecycle.clone();
        let t = std::thread::spawn(move || {
            service.register_coordinator();
            service.stop_admission();
            ready_tx.send(()).unwrap();
            settle_rx.recv().unwrap();
            drop(accepted); // service completion BEFORE drain
            service.drain_independent();
            service.complete_close();
        });
        ready_rx.recv().unwrap();
        settle_tx.send(()).unwrap();
        lifecycle.wait_closed();
        t.join().unwrap();
        assert!(lifecycle.admit().is_err());
    }
    #[test]
    fn p45a1_fence_waiting_publisher_cannot_cross_uncertain_transition() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let fence = Arc::new(CommitFence::new());
        let gate = fence.gate().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let next = fence.clone();
        let t = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            next.gate().is_err()
        });
        ready_rx.recv().unwrap();
        gate.fence();
        drop(gate);
        assert!(t.join().unwrap());
        assert!(fence.is_fenced());
        // Drained close can still acquire the same transition mutex after
        // uncertainty, and holds it across its shutdown I/O without recursion.
        let shutdown = fence.shutdown_gate();
        assert!(matches!(
            fence.transition.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        shutdown.fence();
        assert!(fence.gate().is_err());
        drop(shutdown);
        assert!(fence.transition.try_lock().is_ok());

        let fresh = CommitFence::new();
        let shutdown = fresh.shutdown_gate();
        assert!(fresh.is_fenced());
        assert!(fresh.gate().is_err());
        drop(shutdown);
    }
    #[test]
    fn p45a1_lifecycle_last_read_release_between_predicate_and_wait() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let lifecycle = Lifecycle::allocate();
        let owner = lifecycle.initial_owner();
        let read = lifecycle.admit().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        *lifecycle.before_drain_wait.lock().unwrap() = Some((ready_tx, go_rx));
        drop(owner);
        let service = lifecycle.clone();
        let (closed_tx, closed_rx) = mpsc::channel();
        let coordinator = std::thread::spawn(move || {
            service.register_coordinator();
            service.stop_admission();
            service.drain_independent();
            service.complete_close();
            closed_tx.send(()).unwrap();
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (drop_tx, drop_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop_tx.send(()).unwrap();
            drop(read);
        });
        drop_rx.recv().unwrap();
        go_tx.send(()).unwrap();
        closed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        lifecycle.wait_closed();
        coordinator.join().unwrap();
        dropper.join().unwrap();
    }
}
