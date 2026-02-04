//! Snapshot Scheduler Module
//!
//! Provides automated snapshot scheduling with configurable intervals,
//! retention policies, and compression options.

use super::{Store, StoreInner};
use anyhow::Result;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tracing::{error, info};

/// Configuration for snapshot scheduling
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Interval between automatic snapshots (None = disabled)
    pub interval: Option<Duration>,
    /// Path for snapshot files
    pub path: String,
    /// Maximum number of snapshots to keep (0 = keep all)
    pub retain: usize,
    /// Enable LZ4 compression for snapshots (requires "compression" feature)
    pub compress: bool,
    /// Whether to append commands to AOF-style log
    pub aof_enabled: bool,
    /// Path for AOF log file
    pub aof_path: String,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            interval: None,
            path: "dump.eagle".to_string(),
            retain: 0,
            compress: false,
            aof_enabled: false,
            aof_path: "appendonly.eagle".to_string(),
        }
    }
}

/// Snapshot scheduler that runs periodic background snapshots
#[derive(Debug)]
pub struct SnapshotScheduler {
    /// Configuration (kept for informational/debugging purposes)
    _config: Arc<SnapshotConfig>,
    /// Shutdown signal sender
    shutdown_tx: watch::Sender<()>,
    /// Task handle
    task: JoinHandle<()>,
}

impl SnapshotScheduler {
    /// Create a new snapshot scheduler
    pub(super) fn new(
        store_inner: Weak<StoreInner>,
        config: Arc<SnapshotConfig>,
    ) -> Result<Self, anyhow::Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let config_for_task = Arc::clone(&config);

        let task = tokio::spawn(async move {
            Self::run_scheduler(store_inner, config_for_task, shutdown_rx).await;
        });

        Ok(Self {
            _config: config,
            shutdown_tx,
            task,
        })
    }

    async fn run_scheduler(
        store_inner: Weak<StoreInner>,
        config: Arc<SnapshotConfig>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        if config.interval.is_none() {
            return;
        }

        let interval = config.interval.unwrap();
        info!(
            interval_secs = interval.as_secs(),
            "Starting snapshot scheduler"
        );

        let mut interval_timer = tokio::time::interval(interval);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    // Try to upgrade to Strong reference
                    if let Some(inner) = store_inner.upgrade() {
                        let store = Store { inner };

                        // Generate timestamped snapshot path for rotation support
                        let snapshot_path = Self::timestamped_path(&config.path);

                        if let Err(e) = store.bgsave_to_path_with_options(&snapshot_path, config.compress) {
                            error!(error = %e, "Automatic snapshot failed");
                        } else {
                            info!(path = %snapshot_path, compressed = config.compress, "Automatic snapshot completed");
                        }

                        // Rotate old snapshots if retain limit is set
                        if config.retain > 0 {
                            let _ = Self::rotate_snapshots(&config.path, config.retain);
                        }
                    } else {
                        // Store has been dropped, exit scheduler
                        info!("Store dropped, stopping snapshot scheduler");
                        return;
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Snapshot scheduler shutting down");
                    return;
                }
            }
        }
    }

    /// Generate a timestamped snapshot path from a base path
    ///
    /// Example: "dump.eagle" -> "dump_1704067200.eagle"
    fn timestamped_path(base_path: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let path = std::path::Path::new(base_path);
        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
        let extension = path.extension().unwrap_or_default().to_string_lossy();

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            if extension.is_empty() {
                return format!("{}/{}_{}", parent.display(), stem, timestamp);
            }
            return format!("{}/{}_{}.{}", parent.display(), stem, timestamp, extension);
        }

        if extension.is_empty() {
            format!("{}_{}", stem, timestamp)
        } else {
            format!("{}_{}.{}", stem, timestamp, extension)
        }
    }

    /// Rotate old snapshots, keeping only the most recent N
    ///
    /// Looks for files matching the pattern `{base_name}_{timestamp}.{extension}`
    /// in the same directory as the base path.
    fn rotate_snapshots(path: &str, retain: usize) -> Result<(), anyhow::Error> {
        use std::fs;

        let path_obj = std::path::Path::new(path);
        let parent = path_obj.parent().unwrap_or(std::path::Path::new("."));
        let base_name = path_obj.file_stem().unwrap_or_default().to_string_lossy();
        let extension = path_obj.extension().unwrap_or_default().to_string_lossy();

        // Build glob pattern including the parent directory
        let pattern = if parent.as_os_str().is_empty() || parent == std::path::Path::new(".") {
            if extension.is_empty() {
                format!("{}_*", base_name)
            } else {
                format!("{}_*.{}", base_name, extension)
            }
        } else if extension.is_empty() {
            format!("{}/{}_*", parent.display(), base_name)
        } else {
            format!("{}/{}_*.{}", parent.display(), base_name, extension)
        };

        let mut snapshots: Vec<_> = glob::glob(&pattern)?
            .filter_map(|p| p.ok())
            .filter(|p| p.is_file())
            .filter_map(|p| {
                let filename = p.file_name()?.to_str()?.to_string();
                if !filename.starts_with(&format!("{}_", base_name)) {
                    return None;
                }

                // Extract timestamp from filename like "dump_1704067200.eagle"
                let after_base = filename.strip_prefix(&format!("{}_", base_name))?;
                let timestamp_str = if extension.is_empty() {
                    after_base
                } else {
                    after_base.strip_suffix(&format!(".{}", extension))?
                };

                // Validate that timestamp is numeric
                let timestamp: u64 = timestamp_str.parse().ok()?;
                Some((timestamp, p))
            })
            .collect();

        if snapshots.is_empty() {
            return Ok(());
        }

        // Sort by timestamp (newest first)
        snapshots.sort_by(|a, b| b.0.cmp(&a.0));

        // Delete old snapshots beyond retain limit
        for (_, snapshot_path) in snapshots.iter().skip(retain) {
            if let Err(e) = fs::remove_file(snapshot_path) {
                error!(path = %snapshot_path.display(), error = %e, "Failed to remove old snapshot");
            } else {
                info!(path = %snapshot_path.display(), "Removed old snapshot");
            }
        }

        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(self) {
        let _ = self.shutdown_tx.send(());
        self.task.await.ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_timestamped_path_contains_stem_and_extension() {
        let base = "dump.eagle";
        let ts_path = SnapshotScheduler::timestamped_path(base);
        // Should contain base stem with underscore and keep extension
        assert!(ts_path.contains("dump_"), "timestamped path missing stem");
        assert!(
            ts_path.ends_with(".eagle"),
            "timestamped path missing extension"
        );
    }

    #[test]
    fn test_timestamped_path_with_parent_dir() {
        let base = "backups/dump.eagle";
        let ts_path = SnapshotScheduler::timestamped_path(base);
        // Should preserve parent directory
        assert!(
            ts_path.starts_with("backups/") || ts_path.contains("backups/"),
            "timestamped path should contain parent directory"
        );
        assert!(ts_path.contains("dump_"), "timestamped path missing stem");
        assert!(
            ts_path.ends_with(".eagle"),
            "timestamped path missing extension"
        );
    }

    #[test]
    fn test_timestamped_path_no_extension() {
        let base = "dump";
        let ts_path = SnapshotScheduler::timestamped_path(base);
        // No extension means result shouldn't contain a dot after stem
        assert!(ts_path.contains("dump_"), "timestamped path missing stem");
        // If there is no extension, ensure it doesn't end with a dot-extension
        assert!(!ts_path.contains(".eagle"), "unexpected extension present");
    }

    #[test]
    fn test_rotate_snapshots_retention() {
        // Create a unique temporary directory under the OS temp dir
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let tmp_dir = std::env::temp_dir().join(format!("eagle_snapshot_test_{}", now));
        fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        // Base path
        let base_path = tmp_dir.join("dump.eagle");
        let base_str = base_path.to_str().expect("base path invalid");

        // Create three snapshot files with numeric timestamps in their names
        let f1 = tmp_dir.join("dump_1.eagle");
        let f2 = tmp_dir.join("dump_2.eagle");
        let f3 = tmp_dir.join("dump_3.eagle");

        fs::write(&f1, b"one").expect("write f1");
        fs::write(&f2, b"two").expect("write f2");
        fs::write(&f3, b"three").expect("write f3");

        // Ensure they exist
        assert!(f1.exists());
        assert!(f2.exists());
        assert!(f3.exists());

        // Keep only the 2 most recent snapshots (timestamps 3 and 2), expect 1 to be removed
        SnapshotScheduler::rotate_snapshots(base_str, 2).expect("rotate failed");

        // The oldest (dump_1.eagle) should be removed; others should remain
        assert!(!f1.exists(), "old snapshot should have been removed");
        assert!(f2.exists(), "recent snapshot should remain");
        assert!(f3.exists(), "recent snapshot should remain");

        // Cleanup
        let _ = fs::remove_file(&f2);
        let _ = fs::remove_file(&f3);
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_rotate_snapshots_no_matches_is_ok() {
        // Create a unique temporary directory and do not create any matching snapshots
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let tmp_dir = std::env::temp_dir().join(format!("eagle_snapshot_test_nomatch_{}", now));
        fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        let base_path = tmp_dir.join("dump.eagle");
        let base_str = base_path.to_str().expect("base path invalid");

        // Should succeed even if no files match
        let res = SnapshotScheduler::rotate_snapshots(base_str, 2);
        assert!(
            res.is_ok(),
            "rotate_snapshots should return Ok when no matches"
        );

        // Cleanup
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    // Additional tests for edge cases

    #[test]
    fn test_timestamped_path_parent_no_extension() {
        let base = "snapshots/dump";
        let ts_path = SnapshotScheduler::timestamped_path(base);
        // Should preserve parent directory and include timestamped stem
        assert!(
            ts_path.starts_with("snapshots/") || ts_path.contains("snapshots/"),
            "timestamped path should contain parent directory"
        );
        assert!(ts_path.contains("dump_"), "timestamped path missing stem");
        // No extension expected
        // Ensure there is no dot after the stem portion (best-effort)
        assert!(
            !ts_path
                .split('/')
                .next_back()
                .unwrap_or_default()
                .contains('.'),
            "unexpected dot found in basename: {}",
            ts_path
        );
    }

    #[test]
    fn test_rotate_snapshots_no_extension_files() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let tmp_dir = std::env::temp_dir().join(format!("eagle_snapshot_test_noext_{}", now));
        fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        let base_path = tmp_dir.join("dump");
        let base_str = base_path.to_str().expect("base path invalid");

        let f1 = tmp_dir.join("dump_1");
        let f2 = tmp_dir.join("dump_2");
        let f3 = tmp_dir.join("dump_3");

        fs::write(&f1, b"one").expect("write f1");
        fs::write(&f2, b"two").expect("write f2");
        fs::write(&f3, b"three").expect("write f3");

        SnapshotScheduler::rotate_snapshots(base_str, 1).expect("rotate failed");

        // Only the newest (3) should remain
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());

        // Cleanup
        let _ = fs::remove_file(&f3);
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_rotate_snapshots_non_numeric_ignored() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let tmp_dir = std::env::temp_dir().join(format!("eagle_snapshot_test_nonnum_{}", now));
        fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        let base_path = tmp_dir.join("dump.eagle");
        let base_str = base_path.to_str().expect("base path invalid");

        let numeric = tmp_dir.join("dump_1000.eagle");
        let nonnum = tmp_dir.join("dump_old.eagle");

        fs::write(&numeric, b"num").expect("write numeric");
        fs::write(&nonnum, b"old").expect("write nonnum");

        // Retain 1: numeric is kept, non-numeric should remain untouched
        SnapshotScheduler::rotate_snapshots(base_str, 1).expect("rotate failed");

        assert!(numeric.exists());
        assert!(nonnum.exists());

        // Cleanup
        let _ = fs::remove_file(&numeric);
        let _ = fs::remove_file(&nonnum);
        let _ = fs::remove_dir_all(&tmp_dir);
    }
}
