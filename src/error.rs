use std::fmt;
use std::io;

#[derive(Debug)]
pub enum EngineError {
    IoError(io::Error),
    CorruptRecord(String),
    CorruptWal(String),
    SerializationError(String),
    ManifestError(String),
    DatabaseNotFound(String),
    DatabaseClosed,
    TxnConflict(String),
    TxnClosed,
    InvalidOperation(String),
    CompactionCancelled,
    WalSyncFailed(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::IoError(e) => write!(f, "I/O error: {}", e),
            EngineError::CorruptRecord(msg) => write!(f, "corrupt record: {}", msg),
            EngineError::CorruptWal(msg) => write!(f, "corrupt WAL: {}", msg),
            EngineError::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            EngineError::ManifestError(msg) => write!(f, "manifest error: {}", msg),
            EngineError::DatabaseNotFound(msg) => write!(f, "database not found: {}", msg),
            EngineError::DatabaseClosed => write!(f, "database is closed"),
            EngineError::TxnConflict(msg) => write!(f, "transaction conflict: {}", msg),
            EngineError::TxnClosed => write!(f, "transaction is closed"),
            EngineError::InvalidOperation(msg) => write!(f, "invalid operation: {}", msg),
            EngineError::CompactionCancelled => write!(f, "compaction cancelled by callback"),
            EngineError::WalSyncFailed(msg) => write!(f, "WAL sync failed: {}", msg),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for EngineError {
    fn from(e: io::Error) -> Self {
        EngineError::IoError(e)
    }
}
