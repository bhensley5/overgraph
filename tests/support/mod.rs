use overgraph::{DatabaseEngine, DbOptions, EngineError};
use std::path::Path;

pub fn open(path: &Path, options: &DbOptions) -> Result<DatabaseEngine, EngineError> {
    #[cfg(feature = "production-concurrency")]
    {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match DatabaseEngine::open(path, options) {
                Err(EngineError::InvalidOperation(message))
                    if message == "candidate directory admission: Busy"
                        || message == "candidate directory admission: ActiveSession" =>
                {
                    if std::time::Instant::now() >= until {
                        return Err(EngineError::InvalidOperation(message));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(
                    error @ EngineError::ResourceLimit {
                        resource: "participants",
                        retryable: true,
                        ..
                    },
                ) => {
                    // Parallel first opens may race the process collector's
                    // nonblocking initialization. Retry only this preacceptance
                    // refusal, within the same original fixture deadline.
                    if std::time::Instant::now() >= until {
                        return Err(error);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }
    #[cfg(not(feature = "production-concurrency"))]
    DatabaseEngine::open(path, options)
}
