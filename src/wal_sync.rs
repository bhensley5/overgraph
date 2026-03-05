use crate::error::EngineError;
use crate::wal::WalWriter;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Maximum consecutive sync failures before the engine is poisoned.
pub const MAX_SYNC_FAILURES: u32 = 5;

/// Shared state protected by the WAL sync mutex.
pub(crate) struct WalSyncState {
    pub(crate) wal_writer: WalWriter,
    pub(crate) buffered_bytes: usize,
    pub(crate) shutdown: bool,
    /// Consecutive sync failures. Reset to 0 on success.
    pub(crate) sync_error_count: u32,
    /// If set, the engine is poisoned. Writers return this error.
    pub(crate) poisoned: Option<String>,
}

/// Background sync thread loop. Wakes on timer, soft trigger, or shutdown.
pub(crate) fn sync_thread_loop(
    wal_state: Arc<(Mutex<WalSyncState>, Condvar)>,
    interval: Duration,
) {
    let (lock, cvar) = &*wal_state;
    loop {
        let mut state = lock.lock().unwrap();

        // Wait until: timer expires, soft trigger, or shutdown
        let result = cvar.wait_timeout(state, interval).unwrap();
        state = result.0;

        if state.shutdown && state.buffered_bytes == 0 {
            break;
        }

        if state.buffered_bytes > 0 {
            match state.wal_writer.sync() {
                Ok(()) => {
                    state.buffered_bytes = 0;
                    state.sync_error_count = 0;
                    cvar.notify_all();
                }
                Err(e) => {
                    // Do NOT zero buffered_bytes on failure. Retry next tick.
                    state.sync_error_count += 1;
                    eprintln!(
                        "WAL sync error ({}/{}): {}",
                        state.sync_error_count, MAX_SYNC_FAILURES, e
                    );
                    if state.sync_error_count >= MAX_SYNC_FAILURES {
                        state.poisoned = Some(format!(
                            "WAL sync failed {} consecutive times, last error: {}",
                            state.sync_error_count, e
                        ));
                        cvar.notify_all(); // wake blocked writers so they see the error
                        break; // engine is dead, stop the sync thread
                    }
                }
            }
        }

        if state.shutdown {
            break;
        }
    }
}

/// Shared shutdown logic for both close() and Drop.
/// Signals the sync thread to stop, joins it, then does a final sync
/// to catch anything buffered after the last timer tick.
pub(crate) fn shutdown_sync_thread(
    wal_state: &Arc<(Mutex<WalSyncState>, Condvar)>,
    sync_thread: &mut Option<JoinHandle<()>>,
) -> Result<(), EngineError> {
    // Signal sync thread to stop
    {
        let (lock, cvar) = &**wal_state;
        let mut state = lock.lock().unwrap();
        state.shutdown = true;
        cvar.notify_all();
    }

    // Wait for sync thread to finish
    if let Some(handle) = sync_thread.take() {
        if handle.join().is_err() {
            eprintln!("WAL sync thread panicked during shutdown");
        }
    }

    // Final sync to catch anything buffered after last timer tick
    {
        let (lock, _) = &**wal_state;
        let mut state = lock.lock().unwrap();
        if state.buffered_bytes > 0 {
            state.wal_writer.sync()?;
            state.buffered_bytes = 0;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WalOp;
    use crate::wal::WalReader;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn make_test_node(id: u64, key: &str) -> WalOp {
        use crate::types::*;
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        WalOp::UpsertNode(NodeRecord {
            id,
            type_id: 1,
            key: key.to_string(),
            props,
            created_at: 1000 * id as i64,
            updated_at: 1000 * id as i64 + 1,
            weight: 0.5,
        })
    }

    #[test]
    fn test_truncate_and_reset() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Write some records
        writer.append(&make_test_node(1, "a")).unwrap();
        writer.append(&make_test_node(2, "b")).unwrap();
        writer.sync().unwrap();

        // Verify records exist
        let reader = WalReader::new(dir.path());
        assert_eq!(reader.read_all().unwrap().len(), 2);

        // Truncate and reset
        writer.truncate_and_reset().unwrap();

        // Verify WAL is empty (just header)
        let reader = WalReader::new(dir.path());
        assert!(reader.read_all().unwrap().is_empty());

        // Write new records after reset
        writer.append(&make_test_node(3, "c")).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            WalOp::UpsertNode(node) => assert_eq!(node.key, "c"),
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_sync_thread_basic_operation() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };

        let wal_state = Arc::new((Mutex::new(state), Condvar::new()));
        let wal_state_clone = Arc::clone(&wal_state);

        let handle = std::thread::spawn(move || {
            sync_thread_loop(wal_state_clone, Duration::from_millis(5));
        });

        // Append data under the lock
        {
            let (lock, cvar) = &*wal_state;
            let mut s = lock.lock().unwrap();
            let bytes = s.wal_writer.append(&make_test_node(1, "test")).unwrap();
            s.buffered_bytes += bytes;
            cvar.notify_all(); // soft trigger
        }

        // Wait a bit for the sync thread to process
        std::thread::sleep(Duration::from_millis(50));

        // Verify buffered_bytes was drained
        {
            let (lock, _) = &*wal_state;
            let s = lock.lock().unwrap();
            assert_eq!(s.buffered_bytes, 0);
        }

        // Shutdown
        shutdown_sync_thread(&wal_state, &mut Some(handle)).unwrap();

        // Verify data is durable
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn test_append_batch_returns_size() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        let ops = vec![
            make_test_node(1, "a"),
            make_test_node(2, "b"),
        ];
        let total = writer.append_batch(&ops).unwrap();
        // Each record has: 4 (len) + 4 (crc) + payload
        assert!(total > 0);

        // Verify both records can be read back
        writer.sync().unwrap();
        drop(writer);
        let reader = WalReader::new(dir.path());
        assert_eq!(reader.read_all().unwrap().len(), 2);
    }

    #[test]
    fn test_shutdown_with_pending_buffered_data() {
        // Verify that shutdown_sync_thread performs a final sync
        // even if the sync thread hasn't drained buffered_bytes yet.
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Append data directly (not through sync thread)
        let bytes = writer.append(&make_test_node(1, "pending")).unwrap();
        writer.flush().unwrap(); // flush to OS buffer, but NOT fsync

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: bytes,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };

        let wal_state = Arc::new((Mutex::new(state), Condvar::new()));
        let wal_state_clone = Arc::clone(&wal_state);

        // Start sync thread with a short interval
        let handle = std::thread::spawn(move || {
            sync_thread_loop(wal_state_clone, Duration::from_millis(50));
        });

        // Let the thread enter its wait before we shut down
        std::thread::sleep(Duration::from_millis(10));

        // Shut down -- the final sync in shutdown should drain remaining data
        shutdown_sync_thread(&wal_state, &mut Some(handle)).unwrap();

        // Verify buffered_bytes is zero after shutdown
        {
            let (lock, _) = &*wal_state;
            let s = lock.lock().unwrap();
            assert_eq!(s.buffered_bytes, 0, "shutdown should drain buffered data");
        }

        // Verify data is durable on disk
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn test_poisoned_state_is_visible() {
        // Verify that once poisoned is set, it persists in the shared state.
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };

        let wal_state = Arc::new((Mutex::new(state), Condvar::new()));

        // Manually set poisoned state (simulating MAX_SYNC_FAILURES reached)
        {
            let (lock, cvar) = &*wal_state;
            let mut s = lock.lock().unwrap();
            s.poisoned = Some("WAL sync failed 5 consecutive times".to_string());
            s.shutdown = true;
            cvar.notify_all();
        }

        // Verify poisoned state is readable by anyone holding the lock
        {
            let (lock, _) = &*wal_state;
            let s = lock.lock().unwrap();
            assert!(s.poisoned.is_some());
            assert!(s.poisoned.as_ref().unwrap().contains("5 consecutive"));
        }
    }

    #[test]
    fn test_multiple_sync_cycles_drain_all() {
        // Append data in three separate batches, verify all are synced.
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };

        let wal_state = Arc::new((Mutex::new(state), Condvar::new()));
        let wal_state_clone = Arc::clone(&wal_state);

        let handle = std::thread::spawn(move || {
            sync_thread_loop(wal_state_clone, Duration::from_millis(5));
        });

        // Three separate write + notify cycles
        for i in 0..3 {
            let (lock, cvar) = &*wal_state;
            let mut s = lock.lock().unwrap();
            let bytes = s.wal_writer.append(&make_test_node(i + 1, &format!("n{}", i))).unwrap();
            s.buffered_bytes += bytes;
            cvar.notify_all();
            drop(s);
            std::thread::sleep(Duration::from_millis(30));
        }

        // Verify all drained
        {
            let (lock, _) = &*wal_state;
            let s = lock.lock().unwrap();
            assert_eq!(s.buffered_bytes, 0);
            assert_eq!(s.sync_error_count, 0);
        }

        shutdown_sync_thread(&wal_state, &mut Some(handle)).unwrap();

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_sync_thread_shutdown_with_zero_buffered() {
        // Shutdown when no data has been written should be clean and immediate.
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let state = WalSyncState {
            wal_writer: writer,
            buffered_bytes: 0,
            shutdown: false,
            sync_error_count: 0,
            poisoned: None,
        };

        let wal_state = Arc::new((Mutex::new(state), Condvar::new()));
        let wal_state_clone = Arc::clone(&wal_state);

        let handle = std::thread::spawn(move || {
            sync_thread_loop(wal_state_clone, Duration::from_millis(5));
        });

        shutdown_sync_thread(&wal_state, &mut Some(handle)).unwrap();

        // WAL should exist but have only the header (no records)
        let reader = WalReader::new(dir.path());
        assert!(reader.read_all().unwrap().is_empty());
    }
}
