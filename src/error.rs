use std::fmt;
use std::io;

use crate::types::{GqlSemanticErrorCode, SourceSpan};

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
    InvalidCursor {
        message: String,
    },
    CompactionCancelled,
    WalSyncFailed(String),
    GqlParse {
        message: String,
        span: SourceSpan,
    },
    GqlUnsupported {
        feature: String,
        message: String,
        span: SourceSpan,
    },
    GqlSemantic {
        code: GqlSemanticErrorCode,
        message: String,
        span: SourceSpan,
    },
    GqlParameter {
        name: String,
        expected: String,
        message: String,
        span: SourceSpan,
    },
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
            EngineError::InvalidCursor { message } => write!(f, "invalid cursor: {message}"),
            EngineError::CompactionCancelled => write!(f, "compaction cancelled by callback"),
            EngineError::WalSyncFailed(msg) => write!(f, "WAL sync failed: {}", msg),
            EngineError::GqlParse { message, span } => write!(
                f,
                "GQL parse error at line {}, column {}: {}",
                span.line, span.column, message
            ),
            EngineError::GqlUnsupported {
                feature,
                message,
                span,
            } => write!(
                f,
                "unsupported GQL feature '{}' at line {}, column {}: {}",
                feature, span.line, span.column, message
            ),
            EngineError::GqlSemantic {
                code,
                message,
                span,
            } => write!(
                f,
                "GQL semantic error {:?} at line {}, column {}: {}",
                code, span.line, span.column, message
            ),
            EngineError::GqlParameter {
                name,
                expected,
                message,
                span,
            } => write!(
                f,
                "GQL parameter error for ${} at line {}, column {} (expected {}): {}",
                name, span.line, span.column, expected, message
            ),
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
