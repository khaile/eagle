//! PMEM Layout - Defines the memory layout for persistent memory storage
//!
//! The layout is designed for crash consistency and fast recovery:
//!
//! ```text
//! +------------------+
//! |   SuperBlock     |  (4KB) - Contains metadata and magic number
//! +------------------+
//! |   Segment Table  |  (Variable) - Directory of segments
//! +------------------+
//! |   Data Region    |  (Bulk of space) - Actual key-value entries
//! +------------------+
//! |   Free List      |  (Variable) - Tracks free blocks
//! +------------------+
//! ```

// Layout structures for future PMEM features
#![allow(dead_code)]

use crate::hash::crc32;
use std::mem::size_of;
use std::sync::atomic::AtomicU64;

/// Magic number to identify valid PMEM files
pub const PMEM_MAGIC: u64 = 0x4547414C45444221; // "EAGLEDB!"

/// Version of the PMEM layout format
pub const PMEM_VERSION: u32 = 1;

/// Alignment for all PMEM allocations (cache line size)
pub const PMEM_ALIGNMENT: usize = 64;

/// Size of the superblock
pub const SUPERBLOCK_SIZE: usize = 4096;

/// Maximum key size (64KB)
pub const MAX_KEY_SIZE: u32 = 64 * 1024;

/// Maximum value size (1MB)
pub const MAX_VALUE_SIZE: u32 = 1024 * 1024;

/// Special prefix for hash entries to distinguish them from regular string entries
/// Using a null byte prefix ensures it cannot collide with valid UTF-8 keys
pub const HASH_ENTRY_PREFIX: &[u8] = b"\x00HASH\x01";
pub const HASH_ENTRY_PREFIX_LEN: usize = 6;
/// Length byte follows the prefix to indicate hash_key length (1 byte, max 255)
/// Total key overhead: 6 (prefix) + 1 (len) = 7 bytes

#[allow(clippy::empty_line_after_doc_comments)]
/// Entry states
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    /// Slot is empty and available
    Empty = 0,
    /// Entry is being written (not yet committed)
    Writing = 1,
    /// Entry is valid and committed
    Valid = 2,
    /// Entry is being deleted
    Deleting = 3,
    /// Entry is deleted (tombstone)
    Deleted = 4,
}

impl From<u8> for EntryState {
    fn from(v: u8) -> Self {
        match v {
            0 => EntryState::Empty,
            1 => EntryState::Writing,
            2 => EntryState::Valid,
            3 => EntryState::Deleting,
            4 => EntryState::Deleted,
            _ => EntryState::Empty,
        }
    }
}

/// SuperBlock - The header of the PMEM file
///
/// This structure is stored at the beginning of the PMEM file and contains
/// essential metadata for recovery and validation.
#[repr(C)]
#[derive(Debug)]
pub struct SuperBlock {
    /// Magic number for validation
    pub magic: u64,
    /// Format version
    pub version: u32,
    /// Flags (reserved)
    pub flags: u32,
    /// Total size of the PMEM region in bytes
    pub total_size: u64,
    /// Offset to the segment table
    pub segment_table_offset: u64,
    /// Number of segments
    pub segment_count: u64,
    /// Offset to the data region
    pub data_region_offset: u64,
    /// Size of the data region
    pub data_region_size: u64,
    /// Offset to the free list
    pub free_list_offset: u64,
    /// Current write position in data region
    pub write_position: AtomicU64,
    /// Number of valid entries
    pub entry_count: AtomicU64,
    /// Checksum of the superblock (excluding this field)
    pub checksum: u32,
    /// Reserved for future use
    pub _reserved: [u8; 4012],
}

impl SuperBlock {
    /// Create a new superblock for a given total size
    pub fn new(total_size: usize) -> Self {
        let segment_table_offset = SUPERBLOCK_SIZE as u64;
        let segment_table_size = 1024 * 64; // 64KB for segment table
        let free_list_size = 1024 * 64; // 64KB for free list

        let data_region_offset = segment_table_offset + segment_table_size;
        let data_region_size = total_size as u64 - data_region_offset - free_list_size;
        let free_list_offset = data_region_offset + data_region_size;

        Self {
            magic: PMEM_MAGIC,
            version: PMEM_VERSION,
            flags: 0,
            total_size: total_size as u64,
            segment_table_offset,
            segment_count: 0,
            data_region_offset,
            data_region_size,
            free_list_offset,
            write_position: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            checksum: 0,
            _reserved: [0; 4012],
        }
    }

    /// Validate the superblock
    pub fn validate(&self) -> bool {
        self.magic == PMEM_MAGIC && self.version == PMEM_VERSION
    }

    /// Calculate and update the checksum
    pub fn update_checksum(&mut self) {
        // Calculate checksum of all fields except the checksum itself
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const _ as *const u8,
                size_of::<Self>() - 4 - 4012, // Exclude checksum and reserved
            )
        };
        self.checksum = crc32(bytes);
    }

    /// Verify the checksum
    pub fn verify_checksum(&self) -> bool {
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const _ as *const u8, size_of::<Self>() - 4 - 4012)
        };
        crc32(bytes) == self.checksum
    }
}

/// Entry Header - Metadata for each key-value entry
///
/// Each entry in the data region starts with this header.
/// The header is followed by the key bytes, then the value bytes.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct EntryHeader {
    /// Hash of the key (for quick comparison)
    pub key_hash: u64,
    /// Length of the key in bytes
    pub key_len: u32,
    /// Length of the value in bytes  
    pub value_len: u32,
    /// Entry state (atomic for lock-free operations)
    pub state: u8,
    /// Flags (reserved)
    pub flags: u8,
    /// Padding for alignment
    pub _padding: [u8; 2],
    /// CRC32 checksum of key + value
    pub checksum: u32,
    /// Timestamp (epoch millis) for creation/LRU
    pub timestamp: u64,
    /// Expiration timestamp (Unix epoch millis), 0 = no expiration
    pub expires_at: u64,
}

impl EntryHeader {
    /// Size of the header in bytes
    pub const SIZE: usize = size_of::<Self>();

    /// Create a new entry header
    pub fn new(key_hash: u64, key_len: u32, value_len: u32) -> Self {
        Self {
            key_hash,
            key_len,
            value_len,
            state: EntryState::Writing as u8,
            flags: 0,
            _padding: [0; 2],
            checksum: 0,
            timestamp: 0,
            expires_at: 0,
        }
    }

    /// Create a new entry header with expiration
    pub fn new_with_expiry(key_hash: u64, key_len: u32, value_len: u32, expires_at: u64) -> Self {
        Self {
            key_hash,
            key_len,
            value_len,
            state: EntryState::Writing as u8,
            flags: 0,
            _padding: [0; 2],
            checksum: 0,
            timestamp: 0,
            expires_at,
        }
    }

    /// Calculate the total size of this entry (header + key + value), aligned
    pub fn total_size(&self) -> usize {
        align_to(
            Self::SIZE + self.key_len as usize + self.value_len as usize,
            PMEM_ALIGNMENT,
        )
    }

    /// Calculate checksum for the entry data
    pub fn calculate_checksum(&self, key: &[u8], value: &[u8]) -> u32 {
        let mut data = Vec::with_capacity(key.len() + value.len());
        data.extend_from_slice(key);
        data.extend_from_slice(value);
        crc32(&data)
    }

    /// Get the entry state
    pub fn get_state(&self) -> EntryState {
        EntryState::from(self.state)
    }

    /// Set the entry state
    pub fn set_state(&mut self, state: EntryState) {
        self.state = state as u8;
    }

    /// Calculate checksum for hash entries with the special composite key format
    /// Format: HASH_ENTRY_PREFIX (6 bytes) + hash_key_len (1 byte) + hash_key + field
    pub fn calculate_hash_entry_checksum(
        &self,
        prefix: &[u8],
        hash_key_len: usize,
        hash_key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> u32 {
        let composite_key_len = prefix.len() + 1 + hash_key_len + field.len();
        let mut data = Vec::with_capacity(composite_key_len + value.len());
        data.extend_from_slice(prefix);
        data.push(hash_key_len as u8);
        data.extend_from_slice(hash_key);
        data.extend_from_slice(field);
        data.extend_from_slice(value);
        crc32(&data)
    }
}

/// Segment Entry - Entry in the segment table
///
/// Each segment tracks a portion of the hash space.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SegmentEntry {
    /// Offset to the segment data
    pub offset: u64,
    /// Number of entries in this segment
    pub entry_count: u32,
    /// Local depth for extendible hashing
    pub local_depth: u8,
    /// State of the segment
    pub state: u8,
    /// Padding
    pub _padding: [u8; 2],
    /// Checksum of segment metadata
    pub checksum: u32,
}

impl SegmentEntry {
    pub const SIZE: usize = size_of::<Self>();

    pub fn new(offset: u64) -> Self {
        Self {
            offset,
            entry_count: 0,
            local_depth: 0,
            state: 0,
            _padding: [0; 2],
            checksum: 0,
        }
    }
}

/// Free Block Entry - Entry in the free list
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FreeBlock {
    /// Offset of the free block
    pub offset: u64,
    /// Size of the free block
    pub size: u64,
    /// Next free block offset (linked list)
    pub next: u64,
}

impl FreeBlock {
    pub const SIZE: usize = size_of::<Self>();

    pub fn new(offset: u64, size: u64) -> Self {
        Self {
            offset,
            size,
            next: 0,
        }
    }
}

/// Bucket Header - Header for each bucket in a segment
#[repr(C)]
#[derive(Debug)]
pub struct BucketHeader {
    /// Number of entries in this bucket
    pub count: AtomicU64,
    /// Version number for optimistic concurrency
    pub version: AtomicU64,
    /// Overflow pointer (if bucket is full)
    pub overflow_offset: AtomicU64,
    /// Fingerprints for quick rejection (8 slots)
    pub fingerprints: [u8; 8],
    /// Offsets to entries (8 slots per bucket)
    pub entry_offsets: [u64; 8],
}

impl BucketHeader {
    pub const SIZE: usize = size_of::<Self>();
    pub const SLOTS_PER_BUCKET: usize = 8;

    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            version: AtomicU64::new(0),
            overflow_offset: AtomicU64::new(0),
            fingerprints: [0; 8],
            entry_offsets: [0; 8],
        }
    }

    /// Find an empty slot in the bucket
    pub fn find_empty_slot(&self) -> Option<usize> {
        (0..Self::SLOTS_PER_BUCKET).find(|&i| self.entry_offsets[i] == 0)
    }

    /// Find a slot by fingerprint
    pub fn find_by_fingerprint(&self, fp: u8) -> Vec<usize> {
        let mut matches = Vec::new();
        for i in 0..Self::SLOTS_PER_BUCKET {
            if self.fingerprints[i] == fp && self.entry_offsets[i] != 0 {
                matches.push(i);
            }
        }
        matches
    }
}

impl Default for BucketHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Align a value to the given alignment
#[inline]
pub fn align_to(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// Calculate the offset for a given entry in the data region
#[inline]
pub fn entry_offset(data_region_offset: u64, position: u64) -> u64 {
    data_region_offset + position
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superblock_size() {
        assert_eq!(size_of::<SuperBlock>(), SUPERBLOCK_SIZE);
    }

    #[test]
    fn test_superblock_validation() {
        let sb = SuperBlock::new(1024 * 1024 * 1024); // 1GB
        assert!(sb.validate());
    }

    #[test]
    fn test_entry_header() {
        let header = EntryHeader::new(0x12345678, 10, 100);
        assert_eq!(header.get_state(), EntryState::Writing);
        assert!(header.total_size() >= EntryHeader::SIZE + 10 + 100);
        // Should be aligned
        assert_eq!(header.total_size() % PMEM_ALIGNMENT, 0);
    }

    #[test]
    fn test_alignment() {
        assert_eq!(align_to(1, 64), 64);
        assert_eq!(align_to(64, 64), 64);
        assert_eq!(align_to(65, 64), 128);
        assert_eq!(align_to(127, 64), 128);
        assert_eq!(align_to(128, 64), 128);
    }

    #[test]
    fn test_entry_state_conversion() {
        assert_eq!(EntryState::from(0), EntryState::Empty);
        assert_eq!(EntryState::from(1), EntryState::Writing);
        assert_eq!(EntryState::from(2), EntryState::Valid);
        assert_eq!(EntryState::from(3), EntryState::Deleting);
        assert_eq!(EntryState::from(4), EntryState::Deleted);
        assert_eq!(EntryState::from(255), EntryState::Empty);
    }

    #[test]
    fn test_bucket_header() {
        let bucket = BucketHeader::new();
        assert_eq!(bucket.find_empty_slot(), Some(0));
        assert!(bucket.find_by_fingerprint(0x42).is_empty());
    }
}
