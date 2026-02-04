//! PMEM Allocator - Memory-mapped persistent memory allocator
//!
//! This allocator provides:
//! - Memory-mapped file access for persistence
//! - Atomic bump-pointer allocation
//! - Entry-based storage with headers
//! - Crash-consistent writes via persist barriers

// Some methods are infrastructure for future use
#![allow(dead_code)]

use super::layout::{
    EntryHeader, EntryState, HASH_ENTRY_PREFIX, HASH_ENTRY_PREFIX_LEN, MAX_KEY_SIZE,
    MAX_VALUE_SIZE, PMEM_ALIGNMENT, PMEM_MAGIC, SUPERBLOCK_SIZE, SuperBlock, align_to,
};
use super::secure_buffer::SecureAllocation;
use crate::hash::crc32;
use anyhow::Result;
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum AllocError {
    #[error("Memory allocation failed: out of space")]
    OutOfSpace,
    #[error("Memory allocation failed: size too large ({0} bytes)")]
    SizeTooLarge(usize),
    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),
    #[error("Corrupted data: checksum mismatch")]
    CorruptedData,
    #[error("Invalid PMEM file: bad magic number")]
    InvalidMagic,
    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch { expected: u32, got: u32 },
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
}

/// Result of an allocation operation
#[derive(Debug, Clone)]
pub struct Allocation {
    /// Offset in the PMEM file where data was written
    pub offset: u64,
    /// Total size allocated (including header and padding)
    pub size: usize,
}

/// Result of a recovery scan
#[derive(Debug, Clone, Default)]
pub struct RecoveryScanResult {
    /// Number of valid entries found
    pub valid_entries: usize,
    /// Number of interrupted deletes that were completed
    pub completed_deletes: usize,
    /// Number of incomplete writes that were aborted
    pub aborted_writes: usize,
}

pub type EntryData = (Vec<u8>, Vec<u8>);

/// PMEM Allocator with crash-consistent storage
pub struct PmemAllocator {
    /// Memory-mapped region
    mmap: RwLock<MmapMut>,
    /// Path to the PMEM file
    path: String,
    /// File handle (kept open for the lifetime of the allocator)
    _file: File,
    /// Total size of the PMEM region
    total_size: usize,
    /// Offset where the data region starts
    data_region_offset: u64,
    /// Size of the data region
    data_region_size: u64,
    /// Current write position (atomic for lock-free allocation)
    write_position: AtomicU64,
    /// Number of valid entries
    entry_count: AtomicU64,
}

impl PmemAllocator {
    /// Create a new PMEM allocator, initializing a fresh file
    pub fn new(path: &str, size_mb: usize) -> Result<Self, AllocError> {
        let total_size = size_mb * 1024 * 1024;

        // Ensure minimum size
        if total_size < SUPERBLOCK_SIZE + 1024 * 1024 {
            return Err(AllocError::SizeTooLarge(total_size));
        }

        info!("Creating new PMEM file at {} with size {}MB", path, size_mb);

        // Create or truncate the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(AllocError::IoError)?;

        // Set the file size
        file.set_len(total_size as u64)
            .map_err(AllocError::IoError)?;

        // Memory map the file
        let mut mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .map_err(AllocError::IoError)?
        };

        // Initialize superblock
        let superblock = SuperBlock::new(total_size);
        let sb_bytes = unsafe {
            std::slice::from_raw_parts(&superblock as *const _ as *const u8, SUPERBLOCK_SIZE)
        };
        mmap[..SUPERBLOCK_SIZE].copy_from_slice(sb_bytes);
        mmap.flush_range(0, SUPERBLOCK_SIZE)
            .map_err(AllocError::IoError)?;

        info!(
            "PMEM initialized: data_region_offset={}, data_region_size={}",
            superblock.data_region_offset, superblock.data_region_size
        );

        Ok(Self {
            mmap: RwLock::new(mmap),
            path: path.to_string(),
            _file: file,
            total_size,
            data_region_offset: superblock.data_region_offset,
            data_region_size: superblock.data_region_size,
            write_position: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
        })
    }

    /// Open an existing PMEM file and recover state
    ///
    /// This performs a recovery scan to:
    /// - Complete interrupted deletes
    /// - Abort incomplete writes
    /// - Correct the entry count
    pub fn open(path: &str) -> Result<Self, AllocError> {
        info!("Opening existing PMEM file at {}", path);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(AllocError::IoError)?;

        let metadata = file.metadata().map_err(AllocError::IoError)?;
        let total_size = metadata.len() as usize;

        let mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .map_err(AllocError::IoError)?
        };

        // Read and validate superblock
        let superblock = unsafe { &*(mmap.as_ptr() as *const SuperBlock) };

        if superblock.magic != PMEM_MAGIC {
            return Err(AllocError::InvalidMagic);
        }

        if superblock.version != super::layout::PMEM_VERSION {
            return Err(AllocError::VersionMismatch {
                expected: super::layout::PMEM_VERSION,
                got: superblock.version,
            });
        }

        let write_position = superblock.write_position.load(Ordering::Acquire);
        let entry_count = superblock.entry_count.load(Ordering::Acquire);

        info!(
            "PMEM opened: {} entries (from superblock), write_position={}",
            entry_count, write_position
        );

        let allocator = Self {
            mmap: RwLock::new(mmap),
            path: path.to_string(),
            _file: file,
            total_size,
            data_region_offset: superblock.data_region_offset,
            data_region_size: superblock.data_region_size,
            write_position: AtomicU64::new(write_position),
            entry_count: AtomicU64::new(entry_count),
        };

        // Perform recovery scan to clean up interrupted operations
        match allocator.recovery_scan() {
            Ok(result) => {
                if result.completed_deletes > 0 || result.aborted_writes > 0 {
                    info!(
                        "Recovery scan: {} valid entries, {} deletes completed, {} writes aborted",
                        result.valid_entries, result.completed_deletes, result.aborted_writes
                    );
                } else {
                    info!(
                        "Recovery scan: {} valid entries, no cleanup needed",
                        result.valid_entries
                    );
                }
            }
            Err(e) => {
                warn!("Recovery scan failed (continuing anyway): {}", e);
            }
        }

        Ok(allocator)
    }

    /// Recover from an existing file or create new
    pub fn recover(path: &str, size_mb: usize) -> Result<Self, AllocError> {
        if Path::new(path).exists() {
            match Self::open(path) {
                Ok(allocator) => {
                    info!("Successfully recovered PMEM from {}", path);
                    Ok(allocator)
                }
                Err(e) => {
                    warn!("Failed to open PMEM file, creating new: {}", e);
                    Self::new(path, size_mb)
                }
            }
        } else {
            Self::new(path, size_mb)
        }
    }

    /// Allocate space for a key-value entry and write it
    ///
    /// Returns the offset where the entry was written
    pub fn allocate_entry(
        &self,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Allocation, AllocError> {
        self.allocate_entry_with_expiry(key_hash, key, value, 0)
    }

    /// Allocate space for a key-value entry with expiration and write it
    ///
    /// `expires_at_millis` is the Unix timestamp in milliseconds when the entry expires.
    /// A value of 0 means no expiration.
    ///
    /// ## Crash Consistency
    ///
    /// This function provides crash-consistent writes using the following protocol:
    ///
    /// 1. Reserve space with atomic bump allocation
    /// 2. Write header with state=Writing (not yet valid)
    /// 3. Write key and value data
    /// 4. Persist all data to storage
    /// 5. Update state to Valid and persist
    ///
    /// If a crash occurs before step 5, the entry will be in Writing state and
    /// will be skipped during recovery. The space is "leaked" but can be reclaimed
    /// via compaction.
    ///
    /// Returns the offset where the entry was written
    pub fn allocate_entry_with_expiry(
        &self,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        expires_at_millis: u64,
    ) -> Result<Allocation, AllocError> {
        // Create entry header with state=Writing (uncommitted)
        let mut header = EntryHeader::new_with_expiry(
            key_hash,
            key.len() as u32,
            value.len() as u32,
            expires_at_millis,
        );
        header.checksum = header.calculate_checksum(key, value);
        header.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total_size = header.total_size();

        // Check if entry fits
        if total_size > self.data_region_size as usize {
            return Err(AllocError::SizeTooLarge(total_size));
        }

        // Atomic bump allocation - reserve space first
        let position = self
            .write_position
            .fetch_add(total_size as u64, Ordering::SeqCst);

        if position + total_size as u64 > self.data_region_size {
            // Rollback
            self.write_position
                .fetch_sub(total_size as u64, Ordering::SeqCst);
            return Err(AllocError::OutOfSpace);
        }

        let offset = self.data_region_offset + position;

        // Write to PMEM with crash-consistent ordering
        {
            let mut mmap = self.mmap.write().unwrap();
            let offset_usize = offset as usize;
            let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);

            // Step 1: Write header with state=Writing
            let header_bytes = unsafe {
                std::slice::from_raw_parts(&header as *const _ as *const u8, EntryHeader::SIZE)
            };
            mmap[offset_usize..offset_usize + EntryHeader::SIZE].copy_from_slice(header_bytes);

            // Step 2: Write key
            let key_offset = offset_usize + EntryHeader::SIZE;
            mmap[key_offset..key_offset + key.len()].copy_from_slice(key);

            // Step 3: Write value
            let value_offset = key_offset + key.len();
            mmap[value_offset..value_offset + value.len()].copy_from_slice(value);

            // Step 4: Persist the entire entry (header + key + value)
            // This ensures all data is durable before we mark it valid
            mmap.flush_range(offset_usize, total_size)
                .map_err(AllocError::IoError)?;

            // Step 5: Update state to Valid - this is the commit point
            // Only after this persist completes is the entry considered committed
            mmap[state_offset] = EntryState::Valid as u8;
            mmap.flush_range(state_offset, 1)
                .map_err(AllocError::IoError)?;
        }

        self.entry_count.fetch_add(1, Ordering::SeqCst);

        debug!(
            "Allocated entry at offset {} (size {}, expires_at={})",
            offset, total_size, expires_at_millis
        );

        Ok(Allocation {
            offset,
            size: total_size,
        })
    }

    /// Allocate space for a key-value entry with secure buffers
    ///
    /// Returns a SecureAllocation that will zeroize the key and value on drop.
    /// This is useful for ensuring sensitive data is not left in memory after use.
    pub fn allocate_entry_secure(
        &self,
        key_hash: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<SecureAllocation, AllocError> {
        let expires_at_millis: u64 = 0; // No expiration by default

        // Create entry header with state=Writing (uncommitted)
        let mut header = EntryHeader::new_with_expiry(
            key_hash,
            key.len() as u32,
            value.len() as u32,
            expires_at_millis,
        );
        header.checksum = header.calculate_checksum(&key, &value);
        header.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total_size = header.total_size();

        // Check if entry fits
        if total_size > self.data_region_size as usize {
            return Err(AllocError::SizeTooLarge(total_size));
        }

        // Atomic bump allocation
        let position = self
            .write_position
            .fetch_add(total_size as u64, Ordering::SeqCst);

        if position + total_size as u64 > self.data_region_size {
            self.write_position
                .fetch_sub(total_size as u64, Ordering::SeqCst);
            return Err(AllocError::OutOfSpace);
        }

        let offset = self.data_region_offset + position;

        // Write to PMEM
        {
            let mut mmap = self.mmap.write().unwrap();
            let offset_usize = offset as usize;
            let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);

            // Write header with state=Writing
            let header_bytes = unsafe {
                std::slice::from_raw_parts(&header as *const _ as *const u8, EntryHeader::SIZE)
            };
            mmap[offset_usize..offset_usize + EntryHeader::SIZE].copy_from_slice(header_bytes);

            // Write key
            let key_offset = offset_usize + EntryHeader::SIZE;
            mmap[key_offset..key_offset + key.len()].copy_from_slice(&key);

            // Write value
            let value_offset = key_offset + key.len();
            mmap[value_offset..value_offset + value.len()].copy_from_slice(&value);

            // Persist the entry
            mmap.flush_range(offset_usize, total_size)
                .map_err(AllocError::IoError)?;

            // Mark as valid
            mmap[state_offset] = EntryState::Valid as u8;
            mmap.flush_range(state_offset, 1)
                .map_err(AllocError::IoError)?;
        }

        self.entry_count.fetch_add(1, Ordering::SeqCst);

        debug!(
            "Securely allocated entry at offset {} (size {})",
            offset, total_size
        );

        Ok(SecureAllocation::new(offset, total_size, key, value))
    }

    /// Read an entry at the given offset
    pub fn read_entry(&self, offset: u64) -> Result<Option<EntryData>, AllocError> {
        if offset < self.data_region_offset
            || offset >= self.data_region_offset + self.data_region_size
        {
            return Err(AllocError::InvalidOffset(offset));
        }

        let mmap = self.mmap.read().unwrap();
        let offset_usize = offset as usize;

        // Read header
        let header = unsafe { &*(mmap[offset_usize..].as_ptr() as *const EntryHeader) };

        // Check state
        if header.get_state() != EntryState::Valid {
            return Ok(None);
        }

        // Read key and value
        let key_offset = offset_usize + EntryHeader::SIZE;
        let key = mmap[key_offset..key_offset + header.key_len as usize].to_vec();

        let value_offset = key_offset + header.key_len as usize;
        let value = mmap[value_offset..value_offset + header.value_len as usize].to_vec();

        // Verify checksum
        let expected_checksum = header.checksum;
        let actual_checksum = header.calculate_checksum(&key, &value);
        if expected_checksum != actual_checksum {
            warn!(
                "Checksum mismatch at offset {}: expected {}, got {}",
                offset, expected_checksum, actual_checksum
            );
            return Err(AllocError::CorruptedData);
        }

        Ok(Some((key, value)))
    }

    /// Mark an entry as deleted
    ///
    /// ## Crash Consistency
    ///
    /// This uses a two-phase delete:
    /// 1. Set state to Deleting (intent to delete)
    /// 2. Set state to Deleted (committed delete)
    ///
    /// If a crash occurs during step 1, recovery will see the entry as Deleting
    /// and complete the deletion. The entry_count in memory may be inconsistent
    /// with the actual count after a crash - use iter_entries to get accurate count.
    pub fn delete_entry(&self, offset: u64) -> Result<(), AllocError> {
        if offset < self.data_region_offset
            || offset >= self.data_region_offset + self.data_region_size
        {
            return Err(AllocError::InvalidOffset(offset));
        }

        let mut mmap = self.mmap.write().unwrap();
        let offset_usize = offset as usize;
        let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);

        // Read current state to verify it's valid
        let current_state = mmap[state_offset];
        if current_state != EntryState::Valid as u8 && current_state != EntryState::Deleting as u8 {
            // Entry is not valid or already deleted
            return Ok(());
        }

        // Phase 1: Mark as Deleting (intent)
        mmap[state_offset] = EntryState::Deleting as u8;
        mmap.flush_range(state_offset, 1)
            .map_err(AllocError::IoError)?;

        // Phase 2: Mark as Deleted (committed)
        mmap[state_offset] = EntryState::Deleted as u8;
        mmap.flush_range(state_offset, 1)
            .map_err(AllocError::IoError)?;

        // Note: entry_count is approximate after crash recovery
        // Accurate count requires scanning via iter_entries
        self.entry_count.fetch_sub(1, Ordering::SeqCst);

        Ok(())
    }

    /// Iterate over all valid entries
    ///
    /// The callback receives (offset, key, value, expires_at_millis).
    /// expires_at_millis is 0 if no expiration is set.
    ///
    /// Note: This only iterates over entries with state=Valid. Entries in
    /// Writing or Deleting states are skipped (they represent incomplete operations).
    pub fn iter_entries<F>(&self, mut callback: F) -> Result<usize, AllocError>
    where
        F: FnMut(u64, &[u8], &[u8], u64) -> bool,
    {
        let mmap = self.mmap.read().unwrap();
        let mut position = 0u64;
        let mut count = 0usize;
        let write_pos = self.write_position.load(Ordering::Acquire);

        while position < write_pos {
            let offset = self.data_region_offset + position;
            let offset_usize = offset as usize;

            // Bounds check: ensure we can read the header
            if offset_usize + EntryHeader::SIZE > mmap.len() {
                warn!("Truncated header at offset {}, stopping iteration", offset);
                break;
            }

            // Read header
            let header = unsafe { &*(mmap[offset_usize..].as_ptr() as *const EntryHeader) };

            // Copy packed fields to local variables to avoid unaligned access
            let key_len = header.key_len;
            let value_len = header.value_len;

            // Validate header to prevent corruption from causing issues
            if key_len > MAX_KEY_SIZE || value_len > MAX_VALUE_SIZE {
                warn!(
                    "Corrupted header at offset {}: key_len={}, value_len={}",
                    offset, key_len, value_len
                );
                // Skip to next aligned position and try to continue
                position += PMEM_ALIGNMENT as u64;
                continue;
            }

            let entry_size = header.total_size() as u64;
            if entry_size == 0 || position + entry_size > write_pos {
                warn!("Invalid entry size {} at offset {}", entry_size, offset);
                break;
            }

            if header.get_state() == EntryState::Valid {
                let key_offset = offset_usize + EntryHeader::SIZE;
                let key = &mmap[key_offset..key_offset + key_len as usize];

                let value_offset = key_offset + key_len as usize;
                let value = &mmap[value_offset..value_offset + value_len as usize];

                count += 1;
                if !callback(offset, key, value, header.expires_at) {
                    break;
                }
            }

            position += header.total_size() as u64;
        }

        Ok(count)
    }

    /// Perform recovery scan, cleaning up incomplete operations
    ///
    /// This scans all entries and:
    /// - Counts valid entries to correct entry_count
    /// - Completes interrupted deletes (Deleting -> Deleted)
    /// - Marks interrupted writes as Empty (Writing -> Empty)
    ///
    /// Call this after opening an existing PMEM file to ensure consistency.
    pub fn recovery_scan(&self) -> Result<RecoveryScanResult, AllocError> {
        let mut mmap = self.mmap.write().unwrap();
        let mut position = 0u64;
        let mut valid_count = 0usize;
        let mut completed_deletes = 0usize;
        let mut aborted_writes = 0usize;

        let write_pos = self.write_position.load(Ordering::Acquire);

        while position < write_pos {
            let offset = self.data_region_offset + position;
            let offset_usize = offset as usize;

            // Bounds check: ensure we can read the header
            if offset_usize + EntryHeader::SIZE > mmap.len() {
                warn!(
                    "Truncated header at offset {} during recovery, stopping",
                    offset
                );
                break;
            }

            // Read header
            let header = unsafe { &*(mmap[offset_usize..].as_ptr() as *const EntryHeader) };

            // Copy packed fields to local variables to avoid unaligned access
            let key_len = header.key_len;
            let value_len = header.value_len;

            // Validate header to prevent corruption from causing issues
            if key_len > MAX_KEY_SIZE || value_len > MAX_VALUE_SIZE {
                warn!(
                    "Corrupted header at offset {} during recovery: key_len={}, value_len={}",
                    offset, key_len, value_len
                );
                // Skip to next aligned position and try to continue
                position += PMEM_ALIGNMENT as u64;
                continue;
            }

            let entry_size = header.total_size() as u64;
            if entry_size == 0 || position + entry_size > write_pos {
                warn!(
                    "Invalid entry size {} at offset {} during recovery",
                    entry_size, offset
                );
                break;
            }

            let state = header.get_state();

            match state {
                EntryState::Valid => {
                    valid_count += 1;
                }
                EntryState::Deleting => {
                    // Complete the interrupted delete
                    let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);
                    mmap[state_offset] = EntryState::Deleted as u8;
                    mmap.flush_range(state_offset, 1)
                        .map_err(AllocError::IoError)?;
                    completed_deletes += 1;
                }
                EntryState::Writing => {
                    // Abort the incomplete write by marking as Empty
                    // This space is wasted but safe
                    let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);
                    mmap[state_offset] = EntryState::Empty as u8;
                    mmap.flush_range(state_offset, 1)
                        .map_err(AllocError::IoError)?;
                    aborted_writes += 1;
                }
                EntryState::Empty | EntryState::Deleted => {
                    // Nothing to do
                }
            }

            position += entry_size;
        }

        // Update entry count to match reality
        self.entry_count.store(valid_count as u64, Ordering::SeqCst);

        Ok(RecoveryScanResult {
            valid_entries: valid_count,
            completed_deletes,
            aborted_writes,
        })
    }

    /// Get total size of the PMEM region
    pub fn size(&self) -> usize {
        self.total_size
    }

    /// Get used size in the data region
    pub fn used_size(&self) -> usize {
        self.write_position.load(Ordering::Acquire) as usize
    }

    /// Get available size in the data region
    pub fn available_size(&self) -> usize {
        self.data_region_size as usize - self.used_size()
    }

    /// Get number of valid entries
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Acquire)
    }

    /// Sync the superblock with current state
    pub fn sync_superblock(&self) -> Result<(), AllocError> {
        let mut mmap = self.mmap.write().unwrap();

        // Update superblock fields
        let superblock = unsafe { &mut *(mmap.as_mut_ptr() as *mut SuperBlock) };
        superblock.write_position.store(
            self.write_position.load(Ordering::Acquire),
            Ordering::Release,
        );
        superblock
            .entry_count
            .store(self.entry_count.load(Ordering::Acquire), Ordering::Release);

        mmap.flush_range(0, SUPERBLOCK_SIZE)
            .map_err(AllocError::IoError)?;

        Ok(())
    }

    /// Force flush all pending writes
    pub fn flush(&self) -> Result<(), AllocError> {
        self.sync_superblock()?;
        let mmap = self.mmap.read().unwrap();
        mmap.flush().map_err(AllocError::IoError)
    }

    /// Clear all data (for testing)
    pub fn clear(&self) -> Result<(), AllocError> {
        self.write_position.store(0, Ordering::SeqCst);
        self.entry_count.store(0, Ordering::SeqCst);
        self.sync_superblock()
    }

    /// Get the path to the PMEM file
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Allocate a hash field entry
    ///
    /// Hash entries use a special prefix to distinguish them from regular string entries.
    /// The PMEM key format is: HASH_ENTRY_PREFIX + hash_key_len + hash_key + field
    /// The PMEM value is the field's value.
    ///
    /// Returns the offset where the entry was written
    pub fn allocate_hash_entry(
        &self,
        key_hash: u64,
        hash_key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<Allocation, AllocError> {
        use super::layout::HASH_ENTRY_PREFIX_LEN;

        // Validate hash_key length (stored as 1 byte, max 255)
        if hash_key.len() > 255 {
            return Err(AllocError::SizeTooLarge(hash_key.len()));
        }

        // Build the composite key: prefix + hash_key_len + hash_key + field
        let composite_key_len = HASH_ENTRY_PREFIX_LEN + 1 + hash_key.len() + field.len();

        // Validate sizes
        if composite_key_len > MAX_KEY_SIZE as usize {
            return Err(AllocError::SizeTooLarge(composite_key_len));
        }
        if value.len() > MAX_VALUE_SIZE as usize {
            return Err(AllocError::SizeTooLarge(value.len()));
        }

        // Create entry header
        let mut header = EntryHeader::new(key_hash, composite_key_len as u32, value.len() as u32);
        header.checksum = header.calculate_hash_entry_checksum(
            HASH_ENTRY_PREFIX,
            hash_key.len(),
            hash_key,
            field,
            value,
        );
        header.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let total_size = header.total_size();

        // Atomic bump allocation
        let position = self
            .write_position
            .fetch_add(total_size as u64, Ordering::SeqCst);

        if position + total_size as u64 > self.data_region_size {
            self.write_position
                .fetch_sub(total_size as u64, Ordering::SeqCst);
            return Err(AllocError::OutOfSpace);
        }

        let offset = self.data_region_offset + position;

        // Write to PMEM
        {
            let mut mmap = self.mmap.write().unwrap();
            let offset_usize = offset as usize;
            let state_offset = offset_usize + std::mem::offset_of!(EntryHeader, state);

            // Write header
            let header_bytes = unsafe {
                std::slice::from_raw_parts(&header as *const _ as *const u8, EntryHeader::SIZE)
            };
            mmap[offset_usize..offset_usize + EntryHeader::SIZE].copy_from_slice(header_bytes);

            // Write prefix
            let key_offset = offset_usize + EntryHeader::SIZE;
            mmap[key_offset..key_offset + HASH_ENTRY_PREFIX_LEN].copy_from_slice(HASH_ENTRY_PREFIX);

            // Write hash_key length (1 byte)
            let hash_key_len_offset = key_offset + HASH_ENTRY_PREFIX_LEN;
            mmap[hash_key_len_offset] = hash_key.len() as u8;

            // Write hash_key
            let hash_key_offset = hash_key_len_offset + 1;
            mmap[hash_key_offset..hash_key_offset + hash_key.len()].copy_from_slice(hash_key);

            // Write field
            let field_offset = hash_key_offset + hash_key.len();
            mmap[field_offset..field_offset + field.len()].copy_from_slice(field);

            // Write value
            let value_offset = field_offset + field.len();
            mmap[value_offset..value_offset + value.len()].copy_from_slice(value);

            // Persist all data
            mmap.flush_range(offset_usize, total_size)
                .map_err(AllocError::IoError)?;

            // Mark as valid
            mmap[state_offset] = EntryState::Valid as u8;
            mmap.flush_range(state_offset, 1)
                .map_err(AllocError::IoError)?;
        }

        self.entry_count.fetch_add(1, Ordering::SeqCst);

        debug!(
            "Allocated hash entry at offset {} (key={:?}, field={:?})",
            offset, hash_key, field
        );

        Ok(Allocation {
            offset,
            size: total_size,
        })
    }

    /// Iterate over hash entries only
    ///
    /// The callback receives (hash_key, field, value, expires_at_millis).
    ///
    /// Note: This only iterates over valid hash entries.
    pub fn iter_hash_entries<F>(&self, mut callback: F) -> Result<usize, AllocError>
    where
        F: FnMut(&[u8], &[u8], &[u8], u64) -> bool,
    {
        let mmap = self.mmap.read().unwrap();
        let mut position = 0u64;
        let mut count = 0usize;
        let write_pos = self.write_position.load(Ordering::Acquire);

        while position < write_pos {
            let offset = self.data_region_offset + position;
            let offset_usize = offset as usize;

            // Bounds check
            if offset_usize + EntryHeader::SIZE > mmap.len() {
                break;
            }

            // Read header
            let header = unsafe { &*(mmap[offset_usize..].as_ptr() as *const EntryHeader) };

            let key_len = header.key_len;
            let value_len = header.value_len;

            // Validate header
            if key_len > MAX_KEY_SIZE || value_len > MAX_VALUE_SIZE {
                position += PMEM_ALIGNMENT as u64;
                continue;
            }

            let entry_size = header.total_size() as u64;
            if entry_size == 0 || position + entry_size > write_pos {
                break;
            }

            if header.get_state() == EntryState::Valid {
                // Check if this is a hash entry by looking for the prefix
                let key_offset = offset_usize + EntryHeader::SIZE;

                if key_offset + HASH_ENTRY_PREFIX_LEN <= mmap.len()
                    && &mmap[key_offset..key_offset + HASH_ENTRY_PREFIX_LEN] == HASH_ENTRY_PREFIX
                {
                    // Extract hash_key and field from the composite key
                    // Format: HASH_ENTRY_PREFIX + hash_key_len (1 byte) + hash_key + field
                    let hash_key_len_offset = key_offset + HASH_ENTRY_PREFIX_LEN;

                    if hash_key_len_offset < mmap.len() {
                        let hash_key_len = mmap[hash_key_len_offset] as usize;
                        let composite_key_len = key_len as usize;
                        let field_len_isize = composite_key_len as isize
                            - HASH_ENTRY_PREFIX_LEN as isize
                            - 1
                            - hash_key_len as isize;

                        if field_len_isize >= 0
                            && hash_key_len_offset + 1 + hash_key_len + field_len_isize as usize
                                <= mmap.len()
                        {
                            let field_len = field_len_isize as usize;
                            // Read hash_key
                            let hash_key_offset = hash_key_len_offset + 1;
                            let hash_key = &mmap[hash_key_offset..hash_key_offset + hash_key_len];

                            // Read field
                            let field_offset = hash_key_offset + hash_key_len;
                            let field = &mmap[field_offset..field_offset + field_len];

                            // Read value with bounds check
                            let value_offset = field_offset + field_len;
                            let value_end = value_offset.saturating_add(value_len as usize);
                            if value_end > mmap.len() {
                                break;
                            }
                            let value = &mmap[value_offset..value_end];

                            // Verify checksum
                            let actual_checksum = {
                                let mut data = Vec::with_capacity(
                                    HASH_ENTRY_PREFIX_LEN
                                        + 1
                                        + hash_key_len
                                        + field_len
                                        + value_len as usize,
                                );
                                data.extend_from_slice(HASH_ENTRY_PREFIX);
                                data.push(hash_key_len as u8);
                                data.extend_from_slice(hash_key);
                                data.extend_from_slice(field);
                                data.extend_from_slice(value);
                                crc32(&data)
                            };
                            if header.checksum != actual_checksum {
                                warn!(
                                    "Checksum mismatch for hash entry at offset {}, skipping",
                                    offset
                                );
                                position += entry_size;
                                continue;
                            }

                            count += 1;
                            if !callback(hash_key, field, value, header.expires_at) {
                                break;
                            }
                        }
                    }
                }
            }

            position += entry_size;
        }

        Ok(count)
    }

    /// Read a hash entry at a given offset
    ///
    /// Returns (hash_key, field, value) if valid, None otherwise
    #[allow(clippy::type_complexity)]
    pub fn read_hash_entry(
        &self,
        offset: u64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>, Vec<u8>)>, AllocError> {
        if offset < self.data_region_offset
            || offset >= self.data_region_offset + self.data_region_size
        {
            return Err(AllocError::InvalidOffset(offset));
        }

        let mmap = self.mmap.read().unwrap();
        let offset_usize = offset as usize;

        // Read header
        let header = unsafe { &*(mmap[offset_usize..].as_ptr() as *const EntryHeader) };

        if header.get_state() != EntryState::Valid {
            return Ok(None);
        }

        // Check for hash prefix
        let key_offset = offset_usize + EntryHeader::SIZE;
        if key_offset + HASH_ENTRY_PREFIX_LEN > mmap.len()
            || &mmap[key_offset..key_offset + HASH_ENTRY_PREFIX_LEN] != HASH_ENTRY_PREFIX
        {
            return Ok(None); // Not a hash entry
        }

        // Read composite key: prefix + hash_key + field
        // We need to find where hash_key ends. The format is:
        // HASH_ENTRY_PREFIX | hash_key (length unknown from storage) | field
        //
        // To solve this, we store the hash_key length in the first byte after the prefix
        // Format: HASH_ENTRY_PREFIX (6 bytes) | hash_key_len (1 byte) | hash_key | field | value
        let hash_key_len_offset = key_offset + HASH_ENTRY_PREFIX_LEN;
        if hash_key_len_offset >= mmap.len() {
            return Err(AllocError::InvalidOffset(offset));
        }

        let hash_key_len = mmap[hash_key_len_offset] as usize;
        let composite_key_len = header.key_len as usize;
        let field_len_isize =
            composite_key_len as isize - HASH_ENTRY_PREFIX_LEN as isize - 1 - hash_key_len as isize;

        if field_len_isize < 0 {
            return Ok(None);
        }
        let field_len = field_len_isize as usize;

        let mmap_len = mmap.len();

        // Read hash_key with bounds check
        let hash_key_offset = hash_key_len_offset + 1;
        if hash_key_offset + hash_key_len > mmap_len {
            return Err(AllocError::CorruptedData);
        }
        let hash_key = mmap[hash_key_offset..hash_key_offset + hash_key_len].to_vec();

        // Read field with bounds check
        let field_offset = hash_key_offset + hash_key_len;
        if field_offset + field_len > mmap_len {
            return Err(AllocError::CorruptedData);
        }
        let field = mmap[field_offset..field_offset + field_len].to_vec();

        // Read value with bounds check
        let value_offset = field_offset + field_len;
        let value_len = header.value_len as usize;
        let value_end = match value_offset.checked_add(value_len) {
            Some(end) if end <= mmap_len => end,
            _ => return Err(AllocError::CorruptedData),
        };
        let value = mmap[value_offset..value_end].to_vec();

        // Verify checksum - need to recalculate with correct format
        let expected_checksum = header.checksum;
        let actual_checksum = {
            let mut data = Vec::with_capacity(composite_key_len + header.value_len as usize);
            data.extend_from_slice(HASH_ENTRY_PREFIX);
            data.push(hash_key_len as u8);
            data.extend_from_slice(&hash_key);
            data.extend_from_slice(&field);
            data.extend_from_slice(&value);
            crc32(&data)
        };

        if expected_checksum != actual_checksum {
            warn!(
                "Hash entry checksum mismatch at offset {}: expected {}, got {}",
                offset, expected_checksum, actual_checksum
            );
            return Err(AllocError::CorruptedData);
        }

        Ok(Some((hash_key, field, value)))
    }
}

impl Drop for PmemAllocator {
    fn drop(&mut self) {
        if let Err(e) = self.sync_superblock() {
            warn!("Failed to sync superblock on drop: {}", e);
        }
    }
}

// Helper function to align size to 64-byte boundary
fn align_size(size: usize) -> usize {
    align_to(size, PMEM_ALIGNMENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_pmem_allocator_new() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)?; // 10MB

        assert!(allocator.size() > 0);
        assert_eq!(allocator.used_size(), 0);
        assert_eq!(allocator.entry_count(), 0);

        Ok(())
    }

    #[test]
    fn test_allocate_and_read_entry() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)?;

        let key = b"test_key";
        let value = b"test_value";
        let key_hash = 0x12345678u64;

        // Allocate entry
        let alloc = allocator.allocate_entry(key_hash, key, value)?;
        assert!(alloc.offset > 0);
        assert_eq!(allocator.entry_count(), 1);

        // Read entry back
        let (read_key, read_value) = allocator.read_entry(alloc.offset)?.unwrap();
        assert_eq!(read_key, key);
        assert_eq!(read_value, value);

        Ok(())
    }

    #[test]
    fn test_delete_entry() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)?;

        let key = b"key";
        let value = b"value";
        let alloc = allocator.allocate_entry(0, key, value)?;

        assert_eq!(allocator.entry_count(), 1);

        // Delete entry
        allocator.delete_entry(alloc.offset)?;
        assert_eq!(allocator.entry_count(), 0);

        // Entry should not be readable
        assert!(allocator.read_entry(alloc.offset)?.is_none());

        Ok(())
    }

    #[test]
    fn test_iter_entries() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)?;

        // Add multiple entries
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            allocator.allocate_entry(i as u64, key.as_bytes(), value.as_bytes())?;
        }

        // Count entries
        let count = allocator.iter_entries(|_, _, _, _| true)?;
        assert_eq!(count, 10);

        Ok(())
    }

    #[test]
    fn test_recovery() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let path_str = path.to_str().unwrap();

        // Create and populate
        {
            let allocator = PmemAllocator::new(path_str, 10)?;
            allocator.allocate_entry(1, b"key1", b"value1")?;
            allocator.allocate_entry(2, b"key2", b"value2")?;
            allocator.flush()?;
        }

        // Reopen and verify
        {
            let allocator = PmemAllocator::open(path_str)?;
            assert_eq!(allocator.entry_count(), 2);

            let count = allocator.iter_entries(|_, key, _, _| {
                assert!(key == b"key1" || key == b"key2");
                true
            })?;
            assert_eq!(count, 2);
        }

        Ok(())
    }

    #[test]
    fn test_allocate_with_expiry() -> Result<(), AllocError> {
        let dir = tempdir().map_err(AllocError::IoError)?;
        let path = dir.path().join("test.pmem");
        let allocator = PmemAllocator::new(path.to_str().unwrap(), 10)?;

        let key = b"expiring_key";
        let value = b"some_value";
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60000; // 1 minute from now

        let alloc = allocator.allocate_entry_with_expiry(0, key, value, expires_at)?;
        assert!(alloc.offset > 0);

        // Verify expiration is stored
        let mut found_expires_at = 0u64;
        allocator.iter_entries(|_, k, _, exp| {
            if k == key {
                found_expires_at = exp;
            }
            true
        })?;

        assert_eq!(found_expires_at, expires_at);

        Ok(())
    }
}
