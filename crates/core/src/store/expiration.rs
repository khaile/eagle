//! Background Expiration Task
//!
//! Periodically scans and removes expired keys from the store.
//! This complements the lazy expiration (checking on read) by proactively
//! cleaning up expired entries even if they are never accessed.

// Background expiration task infrastructure for future use
#![allow(dead_code)]

use super::Store;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info};

/// Configuration for the background expiration task
#[derive(Debug, Clone)]
pub struct ExpirationConfig {
    /// Interval between cleanup runs (default: 1 second)
    pub cleanup_interval: Duration,
    /// Maximum number of keys to check per run (default: 100)
    pub max_keys_per_run: usize,
    /// Whether to enable the background task (default: true)
    pub enabled: bool,
}

impl Default for ExpirationConfig {
    fn default() -> Self {
        Self {
            cleanup_interval: Duration::from_secs(1),
            max_keys_per_run: 100,
            enabled: true,
        }
    }
}

/// Handle for controlling the background expiration task
pub struct ExpirationTask {
    /// Signal to stop the task
    shutdown: Arc<AtomicBool>,
    /// Notify handle to wake up the task early
    notify: Arc<Notify>,
    /// Join handle for the background task
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ExpirationTask {
    /// Spawn a new background expiration task
    pub fn spawn(store: Store, config: ExpirationConfig) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());

        let shutdown_clone = shutdown.clone();
        let notify_clone = notify.clone();

        let handle = if config.enabled {
            Some(tokio::spawn(async move {
                run_expiration_loop(store, config, shutdown_clone, notify_clone).await;
            }))
        } else {
            None
        };

        Self {
            shutdown,
            notify,
            handle,
        }
    }

    /// Signal the task to shut down gracefully
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Wait for the task to complete
    pub async fn wait(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Trigger an immediate cleanup cycle
    pub fn trigger_cleanup(&self) {
        self.notify.notify_one();
    }
}

impl Drop for ExpirationTask {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }
}

/// Main loop for the background expiration task
async fn run_expiration_loop(
    store: Store,
    config: ExpirationConfig,
    shutdown: Arc<AtomicBool>,
    notify: Arc<Notify>,
) {
    info!(
        "Starting background expiration task (interval: {:?}, max_keys: {})",
        config.cleanup_interval, config.max_keys_per_run
    );

    loop {
        // Wait for the cleanup interval or shutdown signal
        tokio::select! {
            _ = tokio::time::sleep(config.cleanup_interval) => {}
            _ = notify.notified() => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
            }
        }

        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Perform cleanup
        let cleaned = store.cleanup_expired();
        if cleaned > 0 {
            debug!("Background expiration cleaned up {} keys", cleaned);
        }
    }

    info!("Background expiration task stopped");
}

/// Statistics for the expiration system
#[derive(Debug, Clone, Default)]
pub struct ExpirationStats {
    /// Total number of keys expired (lazy + background)
    pub total_expired: u64,
    /// Number of keys expired via lazy expiration
    pub lazy_expired: u64,
    /// Number of keys expired via background task
    pub background_expired: u64,
    /// Number of cleanup cycles run
    pub cleanup_cycles: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_expiration_task_spawn_and_shutdown() {
        let store = Store::new().unwrap();
        let config = ExpirationConfig {
            cleanup_interval: Duration::from_millis(10),
            max_keys_per_run: 10,
            enabled: true,
        };

        let task = ExpirationTask::spawn(store, config);

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shutdown
        task.shutdown();
        task.wait().await;
    }

    #[tokio::test]
    async fn test_expiration_task_cleans_expired_keys() {
        let store = Store::new().unwrap();

        // Set a key with very short TTL
        store
            .set_with_expiry(
                b"expire_me".to_vec(),
                b"value".to_vec(),
                Some(std::time::Instant::now() + Duration::from_millis(10)),
            )
            .unwrap();

        assert!(store.exists(b"expire_me"));

        let config = ExpirationConfig {
            cleanup_interval: Duration::from_millis(5),
            max_keys_per_run: 10,
            enabled: true,
        };

        let task = ExpirationTask::spawn(store.clone(), config);

        // Wait for expiration and cleanup
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Key should be gone
        assert!(!store.exists(b"expire_me"));

        task.shutdown();
        task.wait().await;
    }

    #[tokio::test]
    async fn test_trigger_immediate_cleanup() {
        let store = Store::new().unwrap();

        // Set a key with very short TTL
        store
            .set_with_expiry(
                b"expire_me".to_vec(),
                b"value".to_vec(),
                Some(std::time::Instant::now() + Duration::from_millis(1)),
            )
            .unwrap();

        let config = ExpirationConfig {
            cleanup_interval: Duration::from_secs(60), // Long interval
            max_keys_per_run: 10,
            enabled: true,
        };

        let task = ExpirationTask::spawn(store.clone(), config);

        // Wait for key to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger immediate cleanup
        task.trigger_cleanup();

        // Give it a moment to process
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Key should be gone
        assert!(!store.exists(b"expire_me"));

        task.shutdown();
        task.wait().await;
    }
}
