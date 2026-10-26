use std::fmt;
use std::io;

use crate::types::{CommitOutcomeState, GqlSemanticErrorCode, SourceSpan};

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
    /// Admission refused before acceptance. Intrinsic limits require changing
    /// the request or configuration; temporary pressure may be retried later.
    ResourceLimit {
        resource: &'static str,
        requested: u64,
        limit: u64,
        retryable: bool,
    },
    /// An accepted operation committed or may have committed. Retrying is not
    /// guaranteed to be safe for a non-idempotent request.
    CommitOutcome {
        state: CommitOutcomeState,
        message: String,
    },
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

impl EngineError {
    /// Stable code for additive concurrency errors. Existing variants have no
    /// code and retain their existing display text and source behavior.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::ResourceLimit { .. } => Some("resource_limit"),
            Self::CommitOutcome {
                state: CommitOutcomeState::Committed,
                ..
            } => Some("commit_committed"),
            Self::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            } => Some("commit_indeterminate"),
            _ => None,
        }
    }
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
            EngineError::ResourceLimit {
                resource,
                requested,
                limit,
                retryable,
            } => write!(
                f,
                "resource_limit: {resource} requested {requested}, limit {limit} (retryable: {retryable}); {}",
                if *retryable {
                    "retry after resource pressure clears"
                } else {
                    "retry requires changing the request or configuration"
                }
            ),
            EngineError::CommitOutcome { state, message } => write!(
                f,
                "{}: {message}",
                match state {
                    CommitOutcomeState::Committed => "commit_committed",
                    CommitOutcomeState::Indeterminate => "commit_indeterminate",
                }
            ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p45a2_resource_errors_preserve_fixed_identifiers_and_retry_guidance() {
        for resource in [
            "request_queue",
            "max_batch_operations",
            "max_batch_bytes",
            "generation_operation_limit",
            "generation_wal_bytes",
            "admission_timeout_ms",
            "heavy_maintenance",
            "allocation_capacity",
            "participants",
        ] {
            for retryable in [false, true] {
                let error = EngineError::ResourceLimit {
                    resource,
                    requested: u64::MAX,
                    limit: 19,
                    retryable,
                };
                assert_eq!(error.code(), Some("resource_limit"));
                let text = error.to_string();
                assert!(text.starts_with(&format!("resource_limit: {resource} ")));
                assert!(text.contains(&u64::MAX.to_string()));
                assert!(text.contains("limit 19"));
                assert!(text.contains(&format!("retryable: {retryable}")));
                assert!(text.contains(if retryable {
                    "pressure clears"
                } else {
                    "request or configuration"
                }));
                assert!(std::error::Error::source(&error).is_none());
            }
        }
    }

    #[test]
    fn p45a2_committed_and_indeterminate_errors_have_distinct_stable_codes() {
        for (state, code) in [
            (CommitOutcomeState::Committed, "commit_committed"),
            (CommitOutcomeState::Indeterminate, "commit_indeterminate"),
        ] {
            let error = EngineError::CommitOutcome {
                state,
                message: "failure after acceptance".into(),
            };
            assert_eq!(error.code(), Some(code));
            assert_eq!(
                error.to_string(),
                format!("{code}: failure after acceptance")
            );
            assert!(std::error::Error::source(&error).is_none());
        }
    }

    #[test]
    fn p45a2_existing_errors_keep_text_and_have_no_new_code() {
        let cases = [
            (EngineError::CorruptRecord("x".into()), "corrupt record: x"),
            (EngineError::CorruptWal("x".into()), "corrupt WAL: x"),
            (
                EngineError::SerializationError("x".into()),
                "serialization error: x",
            ),
            (EngineError::ManifestError("x".into()), "manifest error: x"),
            (
                EngineError::DatabaseNotFound("x".into()),
                "database not found: x",
            ),
            (EngineError::DatabaseClosed, "database is closed"),
            (
                EngineError::TxnConflict("x".into()),
                "transaction conflict: x",
            ),
            (EngineError::TxnClosed, "transaction is closed"),
            (
                EngineError::InvalidOperation("x".into()),
                "invalid operation: x",
            ),
            (
                EngineError::InvalidCursor {
                    message: "x".into(),
                },
                "invalid cursor: x",
            ),
            (
                EngineError::CompactionCancelled,
                "compaction cancelled by callback",
            ),
            (EngineError::WalSyncFailed("x".into()), "WAL sync failed: x"),
            (
                EngineError::GqlParse {
                    message: "x".into(),
                    span: SourceSpan::new(0, 1, 2, 3),
                },
                "GQL parse error at line 2, column 3: x",
            ),
            (
                EngineError::GqlUnsupported {
                    feature: "f".into(),
                    message: "x".into(),
                    span: SourceSpan::new(0, 1, 2, 3),
                },
                "unsupported GQL feature 'f' at line 2, column 3: x",
            ),
            (
                EngineError::GqlSemantic {
                    code: GqlSemanticErrorCode::UnknownVariable,
                    message: "x".into(),
                    span: SourceSpan::new(0, 1, 2, 3),
                },
                "GQL semantic error UnknownVariable at line 2, column 3: x",
            ),
            (
                EngineError::GqlParameter {
                    name: "p".into(),
                    expected: "integer".into(),
                    message: "x".into(),
                    span: SourceSpan::new(0, 1, 2, 3),
                },
                "GQL parameter error for $p at line 2, column 3 (expected integer): x",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(error.code(), None);
            assert_eq!(error.to_string(), text);
        }
        let error = EngineError::from(io::Error::other("x"));
        assert_eq!(error.code(), None);
        assert_eq!(error.to_string(), "I/O error: x");
        assert!(std::error::Error::source(&error).is_some());
    }
}
