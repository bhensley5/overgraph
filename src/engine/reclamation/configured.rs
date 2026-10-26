//! Configured lifetime domain, independent of numerical memory accounting.
use super::*;
use crate::engine::completion::{DomainRegistration, Role};
use crate::engine::directory_identity::DirectoryPin;
use crate::types::ConcurrencyOptions;
pub(in crate::engine) struct ConfiguredDomain {
    pub(in crate::engine) domain: DomainHandle,
    pub(in crate::engine) registration: DomainRegistration,
}
fn invalid(message: &str) -> EngineError {
    EngineError::InvalidOperation(message.into())
}
impl ReclaimDomain {
    pub(in crate::engine) fn validate_options(
        options: &ConcurrencyOptions,
    ) -> Result<(), EngineError> {
        for (name, value) in [
            ("max_batch_operations", options.max_batch_operations),
            ("max_batch_bytes", options.max_batch_bytes),
            (
                "generation_operation_limit",
                options.generation_operation_limit,
            ),
            ("generation_wal_bytes", options.generation_wal_bytes),
        ] {
            if value == 0 || value > isize::MAX as usize {
                return Err(invalid(&format!("{name} must be positive and addressable")));
            }
        }
        if options.max_batch_operations > options.generation_operation_limit
            || options.max_batch_bytes > options.generation_wal_bytes
        {
            return Err(invalid("batch limits must fit generation limits"));
        }
        if options.admission_timeout_ms == 0
            || Instant::now()
                .checked_add(Duration::from_millis(options.admission_timeout_ms))
                .is_none()
        {
            return Err(invalid(
                "admission_timeout_ms must be positive and fit the monotonic clock",
            ));
        }
        Ok(())
    }
    pub(in crate::engine) fn configured(
        options: &ConcurrencyOptions,
        identity: Option<DirectoryPin>,
    ) -> Result<ConfiguredDomain, EngineError> {
        options.validate()?;
        let registration = DomainRegistration::new_configured(identity, options)
            .map_err(|e| invalid(&format!("completion startup: {e:?}")))?;
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
        let domain = DomainHandle::new(Self { inner });
        registration.bind_domain(&domain);
        registration.bind_stats(domain.stats());
        let worker = domain.inner.clone();
        registration
            .spawn(Role::Reclaimer, "overgraph-reclaimer", move |wake| {
                worker
                    .wake
                    .worker
                    .set(WorkerNotification::Governed(wake))
                    .unwrap_or_else(|_| invariant("second configured reclaimer"));
                worker_loop(&worker);
            })
            .map_err(|e| invalid(&format!("reclaimer startup: {e:?}")))?;
        Ok(ConfiguredDomain {
            domain,
            registration,
        })
    }
}
