//! One real-engine transition gate, shared by WAL and manifest publishers.
use super::concurrency_lifecycle::{CommitFence, DurableGate};
use crate::error::EngineError;
use crate::types::{CommitOutcomeState, ManifestState};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

pub(super) struct ManifestLock {
    legacy: Mutex<()>,
    fence: CommitFence,
    candidate: bool,
    references: Mutex<Option<std::sync::Arc<crate::manifest::RecoveryFileReferences>>>,
    #[cfg(all(test, feature = "production-concurrency"))]
    fault: Mutex<Option<crate::persistence_fault::Boundary>>,
}
pub(super) enum ManifestGate<'a> {
    Legacy {
        _guard: MutexGuard<'a, ()>,
        references: &'a Mutex<Option<std::sync::Arc<crate::manifest::RecoveryFileReferences>>>,
    },
    Candidate {
        gate: DurableGate<'a>,
        references: &'a Mutex<Option<std::sync::Arc<crate::manifest::RecoveryFileReferences>>>,
        #[cfg(all(test, feature = "production-concurrency"))]
        fault: &'a Mutex<Option<crate::persistence_fault::Boundary>>,
    },
}
impl ManifestLock {
    pub(super) fn new(candidate: bool) -> Self {
        Self {
            legacy: Mutex::new(()),
            fence: CommitFence::new(),
            candidate,
            references: Mutex::new(None),
            #[cfg(all(test, feature = "production-concurrency"))]
            fault: Mutex::new(None),
        }
    }
    pub(super) fn gate(&self) -> Result<ManifestGate<'_>, EngineError> {
        if self.candidate {
            let gate = self.fence.gate()?;
            Ok(ManifestGate::Candidate {
                gate,
                references: &self.references,
                #[cfg(all(test, feature = "production-concurrency"))]
                fault: &self.fault,
            })
        } else {
            Ok(ManifestGate::Legacy {
                _guard: self.legacy.lock().unwrap(),
                references: &self.references,
            })
        }
    }
    #[cfg(all(test, feature = "production-concurrency"))]
    pub(super) fn inject_fault(&self, boundary: crate::persistence_fault::Boundary) {
        *self.fault.lock().unwrap() = Some(boundary);
    }
    pub(super) fn fenced(&self) -> bool {
        self.candidate && self.fence.is_fenced()
    }
    #[cfg(any(feature = "production-concurrency", test))]
    pub(super) fn establish(&self) {
        if self.candidate {
            self.fence.establish();
        }
    }
}
impl ManifestGate<'_> {
    pub(super) fn fence(&self) {
        if let Self::Candidate { gate, .. } = self {
            gate.fence();
        }
    }
    fn references_cache(
        &self,
    ) -> &Mutex<Option<std::sync::Arc<crate::manifest::RecoveryFileReferences>>> {
        match self {
            Self::Legacy { references, .. } | Self::Candidate { references, .. } => references,
        }
    }
    // Read only under durable authority. The cached immutable ID sets avoid
    // filesystem reads on each foreground publication/physical-pin retry.
    pub(super) fn recovery_references(
        &self,
        path: &Path,
    ) -> Result<std::sync::Arc<crate::manifest::RecoveryFileReferences>, EngineError> {
        let mut cache = self.references_cache().lock().unwrap();
        if let Some(refs) = cache.as_ref() {
            return Ok(refs.clone());
        }
        let refs = std::sync::Arc::new(crate::manifest::recovery_file_references(path)?);
        *cache = Some(refs.clone());
        Ok(refs)
    }
    pub(super) fn retire_unreferenced_wals(&self, path: &Path) -> Result<(), EngineError> {
        let refs = self.recovery_references(path)?;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id) = name
                .to_str()
                .and_then(|n| n.strip_prefix("wal_"))
                .and_then(|n| n.strip_suffix(".wal"))
                .and_then(|n| n.parse::<u64>().ok())
            else {
                continue;
            };
            if !refs.wal_generations.contains(&id) {
                #[cfg(all(test, feature = "production-concurrency"))]
                crate::persistence_fault::check(crate::persistence_fault::Boundary::WalRetire)?;
                crate::wal::remove_wal_generation(path, id)?;
            }
        }
        Ok(())
    }
    pub(super) fn persist(&self, path: &Path, manifest: &ManifestState) -> Result<(), EngineError> {
        self.references_cache().lock().unwrap().take();
        if matches!(self, Self::Legacy { .. }) {
            return crate::manifest::write_manifest(path, manifest);
        }
        // Serialization is private and fallible before a recovery-visible file
        // is touched. The immutable bytes are the authority for this transition.
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| EngineError::ManifestError(format!("serialize: {e}")))?;
        #[cfg(all(test, feature = "production-concurrency"))]
        if let Self::Candidate { fault, .. } = self {
            if let Some(boundary) = fault.lock().unwrap().take() {
                crate::persistence_fault::set(boundary, false);
            }
        }
        match crate::manifest::write_manifest_encoded(path, &bytes) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.fence();
                Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    message: format!("manifest publication uncertain; files preserved: {error}"),
                })
            }
        }
    }
}

// Timer waits never hold the durable gate. Publishers uniformly take gate -> WAL;
// no WAL mutex holder waits on the gate, including sync failure/fence ordering.
pub(super) fn sync_thread_loop_fenced(
    wal: std::sync::Arc<(Mutex<crate::wal_sync::WalSyncState>, std::sync::Condvar)>,
    interval: std::time::Duration,
    authority: std::sync::Arc<ManifestLock>,
) {
    let (lock, cv) = &*wal;
    loop {
        let state = lock.lock().unwrap();
        let state = cv.wait_timeout(state, interval).unwrap().0;
        if state.shutdown && state.buffered_bytes == 0 {
            break;
        }
        let shutdown = state.shutdown;
        let pending = state.buffered_bytes > 0;
        drop(state);
        if pending {
            let Ok(gate) = authority.gate() else {
                break;
            };
            let mut state = lock.lock().unwrap();
            if let Err(error) = crate::wal_sync::sync_pending(&mut state) {
                state.poisoned = Some(format!("candidate WAL sync uncertain: {error}"));
                gate.fence();
                cv.notify_all();
                break;
            }
            cv.notify_all();
        }
        if shutdown {
            break;
        }
    }
}
