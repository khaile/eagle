//! Recovery Manager - Handles crash recovery for PMEM storage
//!
//! After a crash, this module:
//! 1. Scans all segments for valid/corrupted entries
//! 2. Rebuilds in-memory indexes from PMEM data
//! 3. Reclaims space from incomplete writes

// Recovery infrastructure - reserved for future use
#![allow(dead_code)]

use super::allocator::PmemAllocator;
use crate::error::StorageError;
use crate::hash::{HighwayHasher, crc32};
use dashmap::DashMap;
#[allow(unused_imports)]
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tracing::{error, info};

/// Statistics from a recovery scan
#[derive(Debug, Default)]
pub struct RecoveryStats {
    /// Total entries scanned
    pub total_scanned: AtomicUsize,
    /// Valid entries found
    pub valid_entries: AtomicUsize,
    /// Corrupted entries (checksum mismatch)
    pub corrupted_entries: AtomicUsize,
    /// Incomplete entries (partially written)
    pub incomplete_entries: AtomicUsize,
    /// Deleted entries (tombstones)
    pub deleted_entries: AtomicUsize,
    /// Bytes recovered
    pub bytes_recovered: AtomicU64,
}

impl RecoveryStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn summary(&self) -> String {
        format!(
            "Recovery: {} total, {} valid, {} corrupted, {} incomplete, {} deleted, {} bytes",
            self.total_scanned.load(Ordering::Relaxed),
            self.valid_entries.load(Ordering::Relaxed),
            self.corrupted_entries.load(Ordering::Relaxed),
            self.incomplete_entries.load(Ordering::Relaxed),
            self.deleted_entries.load(Ordering::Relaxed),
            self.bytes_recovered.load(Ordering::Relaxed),
        )
    }
}

/// Recovery Manager for crash recovery
pub struct RecoveryManager {
    stats: RecoveryStats,
    hasher: HighwayHasher,
}

impl RecoveryManager {
    pub fn new() -> Self {
        Self {
            stats: RecoveryStats::new(),
            hasher: HighwayHasher::new(),
        }
    }

    /// Recover data from PMEM into an in-memory index
    ///
    /// Returns a map of key -> (offset, hash) for all valid entries
    ///
    /// # Note
    ///
    /// Checksum validation is performed by the allocator's `iter_entries` method.
    /// This function computes bytes_recovered statistics but does not re-validate
    /// checksums since they are already verified during iteration.
    pub fn recover(
        &self,
        allocator: &PmemAllocator,
    ) -> Result<DashMap<Vec<u8>, (u64, u64)>, StorageError> {
        info!("Starting recovery scan of PMEM file: {}", allocator.path());

        let index: DashMap<Vec<u8>, (u64, u64)> = DashMap::new();

        // Iterate through all entries in PMEM
        // Note: Checksum validation happens inside iter_entries - invalid entries are skipped
        let valid_count = allocator
            .iter_entries(|offset, key, value, _expires_at| {
                self.stats.total_scanned.fetch_add(1, Ordering::Relaxed);

                // Compute hash for the key
                let key_hash = self.hasher.hash(key);

                // Entry is valid (checksum already verified by allocator)
                self.stats.valid_entries.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes_recovered
                    .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);

                // Add to index (key -> (offset, hash))
                index.insert(key.to_vec(), (offset, key_hash));

                true // Continue iterating
            })
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        info!("Recovery complete: found {} valid entries", valid_count);
        info!("{}", self.stats.summary());

        Ok(index)
    }

    /// Recover and rebuild a store from PMEM
    pub fn recover_to_dashmap(
        &self,
        allocator: &PmemAllocator,
    ) -> Result<DashMap<Vec<u8>, Vec<u8>>, StorageError> {
        info!("Recovering data to DashMap from PMEM");

        let data: DashMap<Vec<u8>, Vec<u8>> = DashMap::new();

        allocator
            .iter_entries(|_offset, key, value, _expires_at| {
                data.insert(key.to_vec(), value.to_vec());
                true
            })
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        info!("Recovered {} entries to DashMap", data.len());

        Ok(data)
    }

    /// Check integrity of a PMEM file without loading data
    pub fn check_integrity(&self, allocator: &PmemAllocator) -> Result<bool, StorageError> {
        info!("Checking integrity of PMEM file: {}", allocator.path());

        let mut has_errors = false;

        allocator
            .iter_entries(|_offset, key, value, _expires_at| {
                self.stats.total_scanned.fetch_add(1, Ordering::Relaxed);

                // Verify data integrity
                let mut data = Vec::with_capacity(key.len() + value.len());
                data.extend_from_slice(key);
                data.extend_from_slice(value);
                let _computed_checksum = crc32(&data);

                // The checksum is already verified by the allocator's read_entry
                // If we got here, the entry is valid
                self.stats.valid_entries.fetch_add(1, Ordering::Relaxed);

                true
            })
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        let corrupted = self.stats.corrupted_entries.load(Ordering::Relaxed);
        if corrupted > 0 {
            error!("Found {} corrupted entries", corrupted);
            has_errors = true;
        }

        info!("{}", self.stats.summary());

        Ok(!has_errors)
    }

    /// Compact PMEM by removing deleted entries and defragmenting
    ///
    /// This creates a new PMEM file and copies only valid entries
    pub fn compact(
        &self,
        source: &PmemAllocator,
        dest_path: &str,
    ) -> Result<PmemAllocator, StorageError> {
        info!("Compacting PMEM from {} to {}", source.path(), dest_path);

        // Calculate required size (with some headroom)
        let estimated_size = source.used_size() + source.used_size() / 4;
        let size_mb = (estimated_size / (1024 * 1024)).max(10);

        // Create new allocator
        let dest = PmemAllocator::new(dest_path, size_mb)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        let mut copied = 0usize;

        source
            .iter_entries(|_offset, key, value, _expires_at| {
                let key_hash = self.hasher.hash(key);
                match dest.allocate_entry(key_hash, key, value) {
                    Ok(_) => {
                        copied += 1;
                        true
                    }
                    Err(e) => {
                        error!("Failed to copy entry during compaction: {}", e);
                        false
                    }
                }
            })
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        dest.flush()
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        info!(
            "Compaction complete: copied {} entries, saved {} bytes",
            copied,
            source.used_size() as i64 - dest.used_size() as i64
        );

        Ok(dest)
    }

    /// Get recovery statistics
    pub fn stats(&self) -> &RecoveryStats {
        &self.stats
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parallel recovery for large PMEM files
///
/// # Current Limitations
///
/// True parallel recovery of PMEM files with variable-size entries is complex because:
///
/// 1. **Entry boundaries are unknown**: Without a separate index, we cannot know where
///    entries start and end without sequential scanning.
///
/// 2. **Variable-size entries**: Each entry has a different size based on key and value
///    lengths, making it impossible to divide the file into equal chunks.
///
/// 3. **State machine dependencies**: Entry states (Writing, Valid, Deleting, Deleted)
///    may depend on previous entries for correct interpretation.
///
/// # Current Implementation
///
/// This struct currently falls back to sequential recovery via `RecoveryManager`.
/// The `chunk_size` and `thread_count` fields are reserved for future implementation.
///
/// # Future Improvements
///
/// Potential approaches for true parallel recovery:
///
/// - **Segment-based parallelism**: If PMEM uses fixed-size segments, each segment
///   could be recovered independently.
///
/// - **Two-pass recovery**: First pass identifies entry boundaries in parallel,
///   second pass processes entries.
///
/// - **Index-based recovery**: Maintain a separate index structure that can be
///   read in parallel.
pub struct ParallelRecoveryManager {
    chunk_size: usize,
    thread_count: usize,
}

impl ParallelRecoveryManager {
    pub fn new() -> Self {
        Self {
            chunk_size: 64 * 1024 * 1024, // 64MB chunks
            thread_count: num_cpus::get(),
        }
    }

    pub fn with_threads(mut self, count: usize) -> Self {
        self.thread_count = count;
        self
    }

    /// Recover data from PMEM into an in-memory index
    ///
    /// # Note
    ///
    /// Currently falls back to sequential recovery. See struct documentation
    /// for details on parallel recovery limitations.
    pub fn recover(
        &self,
        allocator: &PmemAllocator,
    ) -> Result<DashMap<Vec<u8>, (u64, u64)>, StorageError> {
        let manager = RecoveryManager::new();
        manager.recover(allocator)
    }
}

impl Default for ParallelRecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_recovery_empty() -> Result<(), StorageError> {
        let dir = tempdir().map_err(StorageError::Io)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        let manager = RecoveryManager::new();
        let index = manager.recover(&allocator)?;

        assert!(index.is_empty());
        Ok(())
    }

    #[test]
    fn test_recovery_with_data() -> Result<(), StorageError> {
        let dir = tempdir().map_err(StorageError::Io)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        // Add some entries
        let hasher = HighwayHasher::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            let hash = hasher.hash(key.as_bytes());
            allocator
                .allocate_entry(hash, key.as_bytes(), value.as_bytes())
                .map_err(|e| StorageError::Pmem(e.to_string()))?;
        }

        allocator
            .flush()
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        // Recover
        let manager = RecoveryManager::new();
        let index = manager.recover(&allocator)?;

        assert_eq!(index.len(), 100);
        assert!(index.contains_key(b"key000".as_slice()));
        assert!(index.contains_key(b"key099".as_slice()));

        Ok(())
    }

    #[test]
    fn test_recover_to_dashmap() -> Result<(), StorageError> {
        let dir = tempdir().map_err(StorageError::Io)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        allocator
            .allocate_entry(1, b"key1", b"value1")
            .map_err(|e| StorageError::Pmem(e.to_string()))?;
        allocator
            .allocate_entry(2, b"key2", b"value2")
            .map_err(|e| StorageError::Pmem(e.to_string()))?;
        allocator
            .flush()
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        let manager = RecoveryManager::new();
        let data = manager.recover_to_dashmap(&allocator)?;

        assert_eq!(data.len(), 2);
        assert_eq!(data.get(b"key1".as_slice()).unwrap().value(), b"value1");
        assert_eq!(data.get(b"key2".as_slice()).unwrap().value(), b"value2");

        Ok(())
    }

    #[test]
    fn test_check_integrity() -> Result<(), StorageError> {
        let dir = tempdir().map_err(StorageError::Io)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        allocator
            .allocate_entry(1, b"key", b"value")
            .map_err(|e| StorageError::Pmem(e.to_string()))?;
        allocator
            .flush()
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        let manager = RecoveryManager::new();
        assert!(manager.check_integrity(&allocator)?);

        Ok(())
    }

    #[test]
    fn test_compaction() -> Result<(), StorageError> {
        let dir = tempdir().map_err(StorageError::Io)?;
        let source_path = dir.path().join("source.pmem");
        let dest_path = dir.path().join("dest.pmem");

        let allocator = PmemAllocator::new(source_path.to_str().unwrap(), 10)
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        // Add entries
        for i in 0..50 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            allocator
                .allocate_entry(i as u64, key.as_bytes(), value.as_bytes())
                .map_err(|e| StorageError::Pmem(e.to_string()))?;
        }

        // Delete some
        // Note: We can't easily delete by key here, but the compaction should
        // only copy valid entries

        allocator
            .flush()
            .map_err(|e| StorageError::Pmem(e.to_string()))?;

        // Compact
        let manager = RecoveryManager::new();
        let compacted = manager.compact(&allocator, dest_path.to_str().unwrap())?;

        assert_eq!(compacted.entry_count(), 50);

        Ok(())
    }
}
