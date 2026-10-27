use overgraph::DatabaseEngine;

// CLI/offline fixtures own the database and must release native mappings before
// handing the directory to another process or modifying its files.
pub fn close_and_wait(db: DatabaseEngine) {
    db.close().unwrap();
    #[cfg(feature = "production-concurrency")]
    {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let stats = db.reclamation_stats().unwrap();
            if matches!(stats.close_state, overgraph::ReclamationCloseState::Drained) {
                break;
            }
            assert!(
                std::time::Instant::now() < until,
                "unfinished native close: {stats:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
