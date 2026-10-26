//! Narrow candidate connector bridge. Handles here own only fixed lifecycle /
//! diagnostics metadata, never a runtime, source list, domain or storage graph.
use super::*;

#[doc(hidden)]
#[derive(Clone)]
pub struct BindingStatsHandle(reclamation::ReclamationStatsHandle);
impl BindingStatsHandle {
    pub fn read(&self) -> Result<ReclamationStats, EngineError> {
        self.0.read_full()
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct BindingCloseHandle(Arc<BindingCloseState>);
struct BindingCloseState {
    lifetime: concurrency_lifecycle::LifecycleRef,
    state: Mutex<BindingCloseResult>,
}
struct BindingCloseResult {
    result: Option<Result<(), Arc<EngineError>>>,
    completed: bool,
    notification_registered: bool,
    notification: Option<Box<dyn FnOnce() + Send>>,
}
impl BindingCloseHandle {
    pub(super) fn new(lifetime: concurrency_lifecycle::LifecycleRef) -> Self {
        Self(Arc::new(BindingCloseState {
            lifetime,
            state: Mutex::new(BindingCloseResult {
                result: None,
                completed: false,
                notification_registered: false,
                notification: None,
            }),
        }))
    }
    /// Reserve the single native notification before requesting shutdown. The
    /// callback must only post delivery; it must not wait or call language code.
    pub fn register_notification(
        &self,
        notify: impl FnOnce() + Send + 'static,
    ) -> Result<(), EngineError> {
        let immediate = {
            let mut state = self.0.state.lock().unwrap();
            if state.notification_registered {
                return Err(EngineError::InvalidOperation(
                    "close notification already registered".into(),
                ));
            }
            state.notification_registered = true;
            if state.completed {
                Some(Box::new(notify) as Box<dyn FnOnce() + Send>)
            } else {
                state.notification = Some(Box::new(notify));
                None
            }
        };
        if let Some(notify) = immediate {
            notify();
        }
        Ok(())
    }
    /// Undo registration when a self-dependent request refuses before acceptance.
    pub fn cancel_notification(&self) {
        let notification = {
            let mut state = self.0.state.lock().unwrap();
            if state.completed {
                None
            } else {
                state.notification_registered = false;
                state.notification.take()
            }
        };
        drop(notification);
    }
    pub fn is_complete(&self) -> bool {
        self.0.state.lock().unwrap().completed
    }
    pub fn result(&self) -> Option<Result<(), EngineError>> {
        let state = self.0.state.lock().unwrap();
        if !state.completed {
            return None;
        }
        Some(copy_result(
            state.result.as_ref().expect("completed close result"),
        ))
    }
    pub fn wait(&self) -> Result<(), EngineError> {
        self.0.lifetime.wait_closed();
        let state = self.0.state.lock().unwrap();
        copy_result(
            state
                .result
                .as_ref()
                .expect("close result precedes completion"),
        )
    }
    pub(super) fn publish(&self, result: Result<(), EngineError>) {
        let mut state = self.0.state.lock().unwrap();
        assert!(state.result.is_none(), "duplicate native close result");
        state.result = Some(result.map_err(Arc::new));
    }
    pub(super) fn notify_completed(&self) {
        let notify = {
            let mut state = self.0.state.lock().unwrap();
            assert!(state.result.is_some());
            state.completed = true;
            state.notification.take()
        };
        // All core/lifecycle/completion locks are released. Delivery must not
        // become part of coordinator completion or native drain.
        if let Some(notify) = notify {
            notify();
        }
    }
}
fn copy_result(result: &Result<(), Arc<EngineError>>) -> Result<(), EngineError> {
    result
        .as_ref()
        .map(|_| ())
        .map_err(|error| copy_close_error(error))
}
// EngineError deliberately has no blanket Clone. Preserve its structured kind /
// amounts and diagnostic text for every observer of one accepted close outcome.
fn copy_close_error(error: &EngineError) -> EngineError {
    use EngineError::*;
    match error {
        IoError(error) => IoError(match error.raw_os_error() {
            Some(code) => std::io::Error::from_raw_os_error(code),
            None => std::io::Error::new(error.kind(), error.to_string()),
        }),
        CorruptRecord(v) => CorruptRecord(v.clone()),
        CorruptWal(v) => CorruptWal(v.clone()),
        SerializationError(v) => SerializationError(v.clone()),
        ManifestError(v) => ManifestError(v.clone()),
        DatabaseNotFound(v) => DatabaseNotFound(v.clone()),
        DatabaseClosed => DatabaseClosed,
        TxnConflict(v) => TxnConflict(v.clone()),
        TxnClosed => TxnClosed,
        InvalidOperation(v) => InvalidOperation(v.clone()),
        InvalidCursor { message } => InvalidCursor {
            message: message.clone(),
        },
        CompactionCancelled => CompactionCancelled,
        WalSyncFailed(v) => WalSyncFailed(v.clone()),
        ResourceLimit {
            resource,
            requested,
            limit,
            retryable,
        } => ResourceLimit {
            resource,
            requested: *requested,
            limit: *limit,
            retryable: *retryable,
        },
        CommitOutcome { state, message } => CommitOutcome {
            state: *state,
            message: message.clone(),
        },
        GqlParse { message, span } => GqlParse {
            message: message.clone(),
            span: span.clone(),
        },
        GqlUnsupported {
            feature,
            message,
            span,
        } => GqlUnsupported {
            feature: feature.clone(),
            message: message.clone(),
            span: span.clone(),
        },
        GqlSemantic {
            code,
            message,
            span,
        } => GqlSemantic {
            code: code.clone(),
            message: message.clone(),
            span: span.clone(),
        },
        GqlParameter {
            name,
            expected,
            message,
            span,
        } => GqlParameter {
            name: name.clone(),
            expected: expected.clone(),
            message: message.clone(),
            span: span.clone(),
        },
    }
}
impl DatabaseEngine {
    #[doc(hidden)]
    pub fn binding_stats_handle(&self) -> BindingStatsHandle {
        BindingStatsHandle(
            self.runtime
                .candidate
                .as_ref()
                .expect("candidate runtime")
                .stats
                .clone(),
        )
    }
    #[doc(hidden)]
    pub fn binding_close_handle(&self) -> BindingCloseHandle {
        self.runtime
            .candidate
            .as_ref()
            .expect("candidate runtime")
            .close_result
            .clone()
    }
    #[doc(hidden)]
    pub fn binding_request_close(&self, fast: bool) -> Result<BindingCloseHandle, EngineError> {
        self.runtime.request_candidate_close(fast, true)
    }
}
impl DbRuntime {
    pub(super) fn request_candidate_close(
        &self,
        fast: bool,
        share: bool,
    ) -> Result<BindingCloseHandle, EngineError> {
        let candidate = self.candidate.as_ref().expect("candidate runtime");
        if CandidatePreparationScope::active(self) {
            return Err(EngineError::InvalidOperation(
                "self-dependent close from input preparation".into(),
            ));
        }
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if lifecycle.mutating_barrier_thread == Some(std::thread::current().id()) {
            return Err(EngineError::InvalidOperation(
                "self-dependent close from active mutation callback".into(),
            ));
        }
        if lifecycle.closing || lifecycle.closed {
            return if share {
                Ok(candidate.close_result.clone())
            } else {
                Err(EngineError::DatabaseClosed)
            };
        }
        lifecycle.closing = true;
        self.set_open_state(RuntimeOpenState::Closing);
        candidate.close_fast.store(fast, Ordering::Release);
        candidate.lifetime.stop_admission();
        candidate.lifetime.request_close();
        self.lifecycle_cv.notify_all();
        Ok(candidate.close_result.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn p45c3_completion_preserves_error_for_every_observer_and_notifies_outside_lock() {
        let lifetime = concurrency_lifecycle::Lifecycle::allocate();
        let handle = BindingCloseHandle::new(lifetime.clone());
        let observed = handle.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        handle
            .register_notification(move || {
                tx.send(observed.result().unwrap().unwrap_err().code())
                    .unwrap()
            })
            .unwrap();
        assert!(handle.register_notification(|| {}).is_err());
        handle.publish(Err(EngineError::ResourceLimit {
            resource: "fixture",
            requested: u64::MAX,
            limit: 7,
            retryable: true,
        }));
        lifetime.complete_close();
        handle.notify_completed();
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap(),
            Some("resource_limit")
        );
        for _ in 0..3 {
            assert!(matches!(
                handle.wait(),
                Err(EngineError::ResourceLimit {
                    requested: u64::MAX,
                    limit: 7,
                    retryable: true,
                    ..
                })
            ));
        }
    }
    #[test]
    fn p45c3_late_notification_and_cancelled_registration_have_no_lost_wake() {
        let lifetime = concurrency_lifecycle::Lifecycle::allocate();
        let handle = BindingCloseHandle::new(lifetime.clone());
        handle
            .register_notification(|| panic!("cancelled callback"))
            .unwrap();
        handle.cancel_notification();
        handle.publish(Ok(()));
        lifetime.complete_close();
        handle.notify_completed();
        let observed = handle.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        handle
            .register_notification(move || {
                observed.wait().unwrap();
                tx.send(()).unwrap();
            })
            .unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    }
    #[test]
    fn p45c3_competing_close_keeps_first_mode_and_metadata_does_not_retain_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let stats = db.binding_stats_handle();
        let weak = Arc::downgrade(&db.runtime);
        let read = db.runtime.admit_operation().unwrap();
        let first = db.binding_request_close(true).unwrap();
        let second = db.binding_request_close(false).unwrap();
        assert!(db
            .runtime
            .candidate
            .as_ref()
            .unwrap()
            .close_fast
            .load(Ordering::Acquire));
        assert!(!first.is_complete());
        drop(read);
        first.wait().unwrap();
        second.wait().unwrap();
        drop(db);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while weak.upgrade().is_some() {
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        assert!(stats.read().is_ok());
    }
    #[test]
    fn p45c3_refused_self_dependent_close_preserves_open_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let completion = db.binding_close_handle();
        completion
            .register_notification(|| panic!("refused close"))
            .unwrap();
        let scope = CandidatePreparationScope::enter(&db.runtime).unwrap();
        assert!(db.binding_request_close(false).is_err());
        completion.cancel_notification();
        drop(scope);
        db.upsert_node("Item", "still-open", UpsertNodeOptions::default())
            .unwrap();
        db.close_fast().unwrap();
    }
}
