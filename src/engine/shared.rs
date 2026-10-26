//! Existing locked graphs with explicit producer/passive guardian custody.
//! Default builds keep exactly std::sync::Arc. No payload Arc is exposed by the
//! candidate wrapper; references/clone retain the graph and wake on release.
#[cfg(not(feature = "production-concurrency"))]
pub(crate) type SharedArc<T> = std::sync::Arc<T>;

#[cfg(feature = "production-concurrency")]
mod candidate {
    use super::super::reclamation::{ArcReadPermit, OwnerPermit, ReclaimDomain, ResourceClass};
    use crate::error::EngineError;
    use std::sync::Arc;
    pub(crate) struct SharedArc<T> {
        value: Option<Arc<T>>,
        owner: Option<OwnerPermit<()>>,
        passive: Option<ArcReadPermit>,
    }
    impl<T> SharedArc<T> {
        // Unpublished private construction/legacy fixture only. The engine
        // adopts completed graphs before installing them in any published view.
        pub(crate) fn new(value: T) -> Self {
            Self::from_arc(Arc::new(value))
        }
        fn from_arc(value: Arc<T>) -> Self {
            Self {
                value: Some(value),
                owner: None,
                passive: None,
            }
        }
        pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
            Arc::ptr_eq(left.value.as_ref().unwrap(), right.value.as_ref().unwrap())
        }
        // Explicit producer-to-producer sharing while constructing a replacement
        // binding. Ordinary Clone remains passive and cannot resurrect custody.
        pub(crate) fn fork_owner(&self) -> Self {
            let owner = self.owner.as_ref().unwrap_or_else(|| {
                super::super::concurrency_lifecycle::fail_stop(
                    "producer fork requires live authority",
                )
            });
            Self {
                value: self.value.clone(),
                owner: Some(owner.fork()),
                passive: None,
            }
        }
        pub(crate) fn is_guarded(&self) -> bool {
            self.owner.is_some() || self.passive.is_some()
        }
    }
    impl<T: Send + Sync + 'static> SharedArc<T> {
        pub(in crate::engine) fn guard_storage(
            &mut self,
            domain: &ReclaimDomain,
            id: u64,
        ) -> Result<(), EngineError> {
            self.guard(domain, ResourceClass::Storage)?;
            if let Some(owner) = &self.owner {
                domain.pin_storage(owner, id).map_err(|e| {
                    EngineError::from(super::super::concurrency_lifecycle::Outcome::from(e))
                })?;
            }
            Ok(())
        }
        pub(in crate::engine) fn pin_components(
            &self,
            domain: &ReclaimDomain,
            paths: Vec<(u64, [u8; 32], std::path::PathBuf)>,
        ) -> Result<(), EngineError> {
            if let Some(owner) = &self.owner {
                domain.pin_components(owner, paths).map_err(|e| {
                    EngineError::from(super::super::concurrency_lifecycle::Outcome::from(e))
                })?;
            }
            Ok(())
        }
        pub(crate) fn guard(
            &mut self,
            domain: &ReclaimDomain,
            class: ResourceClass,
        ) -> Result<(), EngineError> {
            if self
                .owner
                .as_ref()
                .is_some_and(|owner| !owner.belongs_to(domain))
                || self
                    .passive
                    .as_ref()
                    .is_some_and(|passive| !passive.belongs_to(domain))
            {
                return Err(EngineError::InvalidOperation(
                    "foreign guarded graph".into(),
                ));
            }
            if !self.is_guarded() {
                self.owner = Some(
                    domain
                        .guard_arc(self.value.as_ref().unwrap().clone(), class)
                        .map_err(|e| {
                            EngineError::from(super::super::concurrency_lifecycle::Outcome::from(e))
                        })?,
                );
            }
            Ok(())
        }
    }
    impl<T> Clone for SharedArc<T> {
        fn clone(&self) -> Self {
            Self {
                value: self.value.clone(),
                owner: None,
                passive: self
                    .owner
                    .as_ref()
                    .map(OwnerPermit::arc_read_permit)
                    .or_else(|| self.passive.clone()),
            }
        }
    }
    impl<T> std::ops::Deref for SharedArc<T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.value.as_ref().unwrap()
        }
    }
    impl<T> AsRef<T> for SharedArc<T> {
        fn as_ref(&self) -> &T {
            self
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for SharedArc<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&**self, f)
        }
    }
    impl<T> Drop for SharedArc<T> {
        fn drop(&mut self) {
            // Actual reference release precedes the fixed metadata wake. The
            // worker cannot miss the final passive release and strand a graph.
            drop(self.value.take());
            drop(self.owner.take());
            drop(self.passive.take());
        }
    }
}
#[cfg(feature = "production-concurrency")]
pub(crate) use candidate::SharedArc;
