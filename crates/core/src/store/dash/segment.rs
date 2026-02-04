//! Segment implementation for DashStore
//!
//! A segment contains multiple buckets and represents a portion of the hash space.

#![allow(dead_code)]

use crate::hash::crc32;
use crossbeam_utils::atomic::AtomicCell;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// Number of buckets per segment
pub const BUCKETS_PER_SEGMENT: usize = 256;

/// Maximum entries per bucket
pub const ENTRIES_PER_BUCKET: usize = 8;

/// Segment header stored in PMEM
#[repr(C)]
#[derive(Debug)]
pub struct SegmentHeader {
    /// Magic number for validation
    pub magic: u64,
    /// Checksum of segment data
    pub checksum: AtomicU64,
    /// Local depth for extendible hashing
    pub local_depth: AtomicU64,
    /// Number of entries in segment
    pub entry_count: AtomicUsize,
    /// Version for optimistic concurrency
    pub version: AtomicU64,
}

impl SegmentHeader {
    pub const MAGIC: u64 = 0x5345474D454E5421; // "SEGMENT!"

    pub fn new(depth: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            checksum: AtomicU64::new(0),
            local_depth: AtomicU64::new(depth),
            entry_count: AtomicUsize::new(0),
            version: AtomicU64::new(0),
        }
    }

    pub fn validate(&self) -> bool {
        self.magic == Self::MAGIC
    }
}

/// Fixed-size entry for bucket slots
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct SlotEntry {
    /// Hash of the key (for quick comparison)
    pub hash: u64,
    /// Fingerprint (top 8 bits of hash)
    pub fingerprint: u8,
    /// Entry state
    pub state: u8,
    /// Padding
    pub _padding: [u8; 6],
    /// Offset to actual key-value data
    pub data_offset: u64,
}

impl SlotEntry {
    pub fn new(hash: u64, data_offset: u64) -> Self {
        Self {
            hash,
            fingerprint: (hash >> 56) as u8,
            state: 1, // Valid
            _padding: [0; 6],
            data_offset,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state == 0
    }

    pub fn is_valid(&self) -> bool {
        self.state == 1
    }

    pub fn matches_fingerprint(&self, fp: u8) -> bool {
        self.fingerprint == fp
    }
}

/// Variable-size entry stored separately
#[derive(Clone, Debug)]
pub struct Entry {
    pub hash: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub pmem_offset: Option<u64>,
}

impl Entry {
    pub fn new(hash: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            hash,
            key,
            value,
            pmem_offset: None,
        }
    }
}

/// Bucket containing entry slots
#[derive(Debug)]
pub struct Bucket {
    /// Entry slots
    pub slots: [AtomicCell<SlotEntry>; ENTRIES_PER_BUCKET],
    /// Actual entries (for variable-size keys/values)
    pub entries: RwLock<Vec<Entry>>,
    /// Overflow bucket pointer
    pub overflow: AtomicPtr<Bucket>,
    /// Version for optimistic reads
    pub version: AtomicU64,
}

impl Bucket {
    pub fn new() -> Self {
        Self {
            slots: Default::default(),
            entries: RwLock::new(Vec::with_capacity(ENTRIES_PER_BUCKET)),
            overflow: AtomicPtr::new(std::ptr::null_mut()),
            version: AtomicU64::new(0),
        }
    }

    /// Find an empty slot
    pub fn find_empty_slot(&self) -> Option<usize> {
        (0..ENTRIES_PER_BUCKET).find(|&i| self.slots[i].load().is_empty())
    }

    /// Find slots matching a fingerprint
    pub fn find_by_fingerprint(&self, fp: u8) -> Vec<usize> {
        let mut matches = Vec::new();
        for i in 0..ENTRIES_PER_BUCKET {
            let slot = self.slots[i].load();
            if slot.is_valid() && slot.matches_fingerprint(fp) {
                matches.push(i);
            }
        }
        matches
    }

    /// Get bucket occupancy
    pub fn occupancy(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if bucket is full
    pub fn is_full(&self) -> bool {
        self.occupancy() >= ENTRIES_PER_BUCKET
    }

    /// Increment version
    pub fn bump_version(&self) {
        self.version.fetch_add(1, Ordering::Release);
    }
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Bucket {
    fn clone(&self) -> Self {
        let new_bucket = Bucket::new();
        {
            let entries = self.entries.read();
            *new_bucket.entries.write() = entries.clone();
        }
        for i in 0..ENTRIES_PER_BUCKET {
            new_bucket.slots[i].store(self.slots[i].load());
        }
        new_bucket
    }
}

/// Segment containing buckets
pub struct Segment {
    /// Segment header
    pub header: SegmentHeader,
    /// Buckets in this segment
    pub buckets: Vec<Bucket>,
    /// Backup pointer for migration
    pub backup: AtomicPtr<Segment>,
}

impl Segment {
    pub fn new(depth: u64) -> Self {
        let buckets = (0..BUCKETS_PER_SEGMENT).map(|_| Bucket::new()).collect();

        Self {
            header: SegmentHeader::new(depth),
            buckets,
            backup: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Get bucket for a hash
    pub fn get_bucket(&self, hash: u64) -> &Bucket {
        let idx = (hash as usize) % BUCKETS_PER_SEGMENT;
        &self.buckets[idx]
    }

    /// Get mutable bucket for a hash
    pub fn get_bucket_mut(&mut self, hash: u64) -> &mut Bucket {
        let idx = (hash as usize) % BUCKETS_PER_SEGMENT;
        &mut self.buckets[idx]
    }

    /// Find a key in the segment
    pub fn find(&self, hash: u64, key: &[u8]) -> Option<Vec<u8>> {
        let bucket = self.get_bucket(hash);
        let entries = bucket.entries.read();

        for entry in entries.iter() {
            if entry.hash == hash && entry.key == key {
                return Some(entry.value.clone());
            }
        }
        None
    }

    /// Insert a key-value pair
    pub fn insert(&self, hash: u64, key: Vec<u8>, value: Vec<u8>) -> bool {
        let bucket = self.get_bucket(hash);
        let mut entries = bucket.entries.write();

        // Check for existing key
        for entry in entries.iter_mut() {
            if entry.hash == hash && entry.key == key {
                entry.value = value;
                bucket.bump_version();
                return true;
            }
        }

        // Add new entry
        if entries.len() < ENTRIES_PER_BUCKET {
            entries.push(Entry::new(hash, key, value));
            self.header.entry_count.fetch_add(1, Ordering::Relaxed);
            bucket.bump_version();
            true
        } else {
            false // Bucket full, need overflow
        }
    }

    /// Delete a key
    pub fn delete(&self, hash: u64, key: &[u8]) -> bool {
        let bucket = self.get_bucket(hash);
        let mut entries = bucket.entries.write();

        if let Some(pos) = entries.iter().position(|e| e.hash == hash && e.key == key) {
            entries.remove(pos);
            self.header.entry_count.fetch_sub(1, Ordering::Relaxed);
            bucket.bump_version();
            true
        } else {
            false
        }
    }

    /// Check if migration is in progress
    pub fn is_migrating(&self) -> bool {
        !self.backup.load(Ordering::Acquire).is_null()
    }

    /// Get local depth
    pub fn local_depth(&self) -> u64 {
        self.header.local_depth.load(Ordering::Acquire)
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.header.entry_count.load(Ordering::Relaxed)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Calculate segment checksum
    pub fn calculate_checksum(&self) -> u64 {
        let mut data = Vec::new();
        for bucket in &self.buckets {
            let entries = bucket.entries.read();
            for entry in entries.iter() {
                data.extend_from_slice(&entry.hash.to_le_bytes());
                data.extend_from_slice(&entry.key);
                data.extend_from_slice(&entry.value);
            }
        }
        crc32(&data) as u64
    }

    /// Verify segment checksum
    pub fn verify_checksum(&self) -> bool {
        let computed = self.calculate_checksum();
        let stored = self.header.checksum.load(Ordering::Acquire);
        computed == stored || stored == 0 // Allow 0 for uninitialized
    }

    /// Update stored checksum
    pub fn update_checksum(&self) {
        let checksum = self.calculate_checksum();
        self.header.checksum.store(checksum, Ordering::Release);
    }

    /// Split segment into two based on new depth
    pub fn split(&self, new_depth: u64) -> (Segment, Segment) {
        let seg0 = Segment::new(new_depth);
        let seg1 = Segment::new(new_depth);

        let split_bit = 1u64 << (new_depth - 1);

        for bucket in &self.buckets {
            let entries = bucket.entries.read();
            for entry in entries.iter() {
                let target = if entry.hash & split_bit != 0 {
                    &seg1
                } else {
                    &seg0
                };
                target.insert(entry.hash, entry.key.clone(), entry.value.clone());
            }
        }

        (seg0, seg1)
    }
}

impl Clone for Segment {
    fn clone(&self) -> Self {
        let buckets = self.buckets.to_vec();
        Self {
            header: SegmentHeader::new(self.local_depth()),
            buckets,
            backup: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_basic() {
        let segment = Segment::new(4);

        assert!(segment.insert(0x1234, b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(segment.find(0x1234, b"key1"), Some(b"value1".to_vec()));

        assert!(segment.delete(0x1234, b"key1"));
        assert_eq!(segment.find(0x1234, b"key1"), None);
    }

    #[test]
    fn test_segment_update() {
        let segment = Segment::new(4);

        segment.insert(0x1234, b"key".to_vec(), b"value1".to_vec());
        segment.insert(0x1234, b"key".to_vec(), b"value2".to_vec());

        assert_eq!(segment.find(0x1234, b"key"), Some(b"value2".to_vec()));
        assert_eq!(segment.len(), 1);
    }

    #[test]
    fn test_segment_split() {
        let segment = Segment::new(4);

        // Add entries with different bit patterns
        for i in 0..100u64 {
            let hash = i * 0x12345678;
            segment.insert(
                hash,
                format!("key{}", i).into_bytes(),
                format!("val{}", i).into_bytes(),
            );
        }

        let (seg0, seg1) = segment.split(5);

        // All entries should be distributed
        assert_eq!(seg0.len() + seg1.len(), 100);
    }

    #[test]
    fn test_bucket() {
        let bucket = Bucket::new();

        assert_eq!(bucket.find_empty_slot(), Some(0));
        assert_eq!(bucket.occupancy(), 0);
        assert!(!bucket.is_full());
    }
}
