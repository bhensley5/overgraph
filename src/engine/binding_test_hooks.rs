//! Installed-test boundary instrumentation, absent from ordinary artifacts.
use super::*;
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct BindingTestHandle(Arc<BindingTestState>);
#[derive(Default)]
struct BindingTestState {
    holds: Mutex<[Option<PathBuf>; 7]>,
    maintenance_error: AtomicBool,
}
impl BindingTestHandle {
    fn slot(point: &str) -> Result<usize, EngineError> {
        match point {
            "read" => Ok(0),
            "write" => Ok(1),
            "accepted" => Ok(2),
            "handoff" => Ok(3),
            "completed" => Ok(4),
            "gil" => Ok(5),
            "shutdown" => Ok(6),
            _ => Err(EngineError::InvalidOperation(
                "unknown binding test boundary".into(),
            )),
        }
    }
    pub fn arm(&self, point: &str, path: &str) -> Result<(), EngineError> {
        let slot = Self::slot(point)?;
        let path = PathBuf::from(path);
        if path.with_extension("ready").exists() || path.with_extension("release").exists() {
            return Err(EngineError::InvalidOperation(
                "binding hold files already exist".into(),
            ));
        }
        let mut holds = self.0.holds.lock().unwrap();
        if holds[slot].is_some() {
            return Err(EngineError::InvalidOperation(
                "binding hold already armed".into(),
            ));
        }
        holds[slot] = Some(path);
        Ok(())
    }
    pub fn hit(&self, point: &str) {
        let path = self.0.holds.lock().unwrap()[Self::slot(point).expect("fixed hook")].take();
        if let Some(path) = path {
            std::fs::write(path.with_extension("ready"), b"native-boundary")
                .expect("binding ready signal");
            let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while !path.with_extension("release").exists() {
                assert!(std::time::Instant::now() < until, "binding hold watchdog");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
    pub(super) fn maintenance_pending(&self) -> bool {
        self.0.maintenance_error.load(Ordering::Acquire)
    }
    pub(super) fn take_maintenance_error(&self) -> bool {
        self.0.maintenance_error.swap(false, Ordering::AcqRel)
    }
}
impl DatabaseEngine {
    #[doc(hidden)]
    pub fn binding_test_handle(&self) -> BindingTestHandle {
        self.runtime.binding_hooks.clone()
    }
    #[doc(hidden)]
    pub fn binding_test_fault(&self, kind: &str) -> Result<(), EngineError> {
        match kind {
            "indeterminate" => self
                .runtime
                .force_publication_error
                .store(true, Ordering::Release),
            "committed" => self
                .runtime
                .binding_hooks
                .0
                .maintenance_error
                .store(true, Ordering::Release),
            _ => {
                return Err(EngineError::InvalidOperation(
                    "unknown binding fault".into(),
                ))
            }
        }
        Ok(())
    }
}
