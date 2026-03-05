//! # OverGraph
//!
//! An absurdly fast embedded graph database. Pure Rust, sub-microsecond reads.
//!
//! OverGraph stores typed nodes and edges with schemaless properties (MessagePack),
//! temporal validity windows, exponential decay scoring, and automatic retention
//! policies. It runs inside your process with no separate server or network calls.
//!
//! ## Quick start
//!
//! ```no_run
//! use overgraph::{DatabaseEngine, DbOptions, Direction};
//!
//! use std::path::Path;
//! let mut db = DatabaseEngine::open(Path::new("./my-db"), &DbOptions::default()).unwrap();
//! let id = db.upsert_node(1, "user:alice", Default::default(), 1.0).unwrap();
//! let neighbors = db.neighbors(id, Direction::Outgoing, None, 50, None, None).unwrap();
//! db.close().unwrap();
//! ```
//!
//! ## Storage engine
//!
//! Log-structured merge tree: WAL -> memtable -> immutable segments -> background
//! compaction. Reads never block writes. Segments are memory-mapped for zero-copy
//! access through the OS page cache.
//!
//! ## Language connectors
//!
//! Native bindings for Node.js (napi-rs) and Python (PyO3) with full API parity.

// Public API: the types and engine that library users interact with.
pub mod engine;
pub mod error;
pub mod types;

// Internal modules: accessible within the workspace (connectors, CLI binaries)
// but hidden from public documentation since they are implementation details.
#[doc(hidden)]
pub mod encoding;
#[doc(hidden)]
pub mod manifest;
#[doc(hidden)]
pub mod memtable;
#[doc(hidden)]
pub mod segment_reader;
#[doc(hidden)]
pub mod segment_writer;
#[doc(hidden)]
pub mod wal;
#[doc(hidden)]
pub mod wal_sync;

pub use engine::DatabaseEngine;
pub use error::EngineError;
pub use types::*;
