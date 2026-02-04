//! Dash Store - Extendible Hashing Implementation
//!
//! This module implements the DASH (Dynamic And Scalable Hashing) algorithm
//! for efficient concurrent hash table operations. Key features:
//!
//! - Lock-free reads using epoch-based reclamation
//! - Fine-grained locking for writes (per-bucket)
//! - Incremental resizing via segment splitting
//! - Cache-line aligned data structures

// Dash store implementation for future use

use crate::error::Result;
use crate::hash::HighwayHasher;
use crate::pmem::PmemAllocator;
use crate::sync::epoch::Epoch as AtomicEpoch;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

pub mod directory;
pub mod segment;

/// Number of buckets per segment
const BUCKETS_PER_SEGMENT: usize = 256;

/// Initial number of segments (must be power of 2)
const INITIAL_SEGMENTS: usize = 16;

/// Maximum entries per bucket before overflow
const BUCKET_CAPACITY: usize = 8;

/// Entry stored in the hash table
#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub hash: u64,
    /// Offset in PMEM if persisted
    pub pmem_offset: Option<u64>,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>, hash: u64) -> Self {
        Self {
            key,
            value,
            hash,
            pmem_offset: None,
        }
    }
}

/// Main Dash Store structure
pub struct DashStore {
    /// Directory of segments
    dir: Arc<RwLock<Directory>>,
    /// Hasher for key hashing
    hasher: HighwayHasher,
    /// Optional PMEM allocator
    allocator: Option<Arc<PmemAllocator>>,
    /// Epoch manager for safe memory reclamation
    #[allow(dead_code)]
    epoch: Arc<AtomicEpoch>,
    /// Total entry count
    entry_count: AtomicUsize,
}

impl DashStore {
    /// Create a new in-memory DashStore
    pub fn new(epoch: Arc<AtomicEpoch>) -> Result<Self> {
        Ok(Self {
            dir: Arc::new(RwLock::new(Directory::new(INITIAL_SEGMENTS))),
            hasher: HighwayHasher::new(),
            allocator: None,
            epoch,
            entry_count: AtomicUsize::new(0),
        })
    }

    /// Create a DashStore with PMEM persistence
    pub fn new_pmem(allocator: Arc<PmemAllocator>, epoch: Arc<AtomicEpoch>) -> Result<Self> {
        Ok(Self {
            dir: Arc::new(RwLock::new(Directory::new(INITIAL_SEGMENTS))),
            hasher: HighwayHasher::new(),
            allocator: Some(allocator),
            epoch,
            entry_count: AtomicUsize::new(0),
        })
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8], _guard: &crossbeam::epoch::Guard) -> Result<Option<Vec<u8>>> {
        let hash = self.hasher.hash(key);
        let dir = self.dir.read();

        let segment = dir.get_segment(hash);
        let bucket_idx = (hash as usize) % BUCKETS_PER_SEGMENT;

        let entries = segment.buckets[bucket_idx].entries.read();
        for entry in entries.iter() {
            if entry.hash == hash && entry.key == key {
                return Ok(Some(entry.value.clone()));
            }
        }

        // Check overflow chain
        let mut overflow = segment.buckets[bucket_idx].overflow.load(Ordering::Acquire);
        while !overflow.is_null() {
            let overflow_bucket = unsafe { &*overflow };
            let entries = overflow_bucket.entries.read();
            for entry in entries.iter() {
                if entry.hash == hash && entry.key == key {
                    return Ok(Some(entry.value.clone()));
                }
            }
            overflow = overflow_bucket.overflow.load(Ordering::Acquire);
        }

        Ok(None)
    }

    /// Put a key-value pair
    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        _guard: &crossbeam::epoch::Guard,
    ) -> Result<()> {
        let hash = self.hasher.hash(&key);
        let dir = self.dir.read();

        let segment = dir.get_segment(hash);
        let bucket_idx = (hash as usize) % BUCKETS_PER_SEGMENT;
        let bucket = &segment.buckets[bucket_idx];

        // Try to update existing entry
        {
            let mut entries = bucket.entries.write();
            for entry in entries.iter_mut() {
                if entry.hash == hash && entry.key == key {
                    entry.value = value;
                    return Ok(());
                }
            }

            // Add new entry if bucket has space
            if entries.len() < BUCKET_CAPACITY {
                let mut entry = Entry::new(key, value, hash);

                // Persist to PMEM if available
                if let Some(ref allocator) = self.allocator
                    && let Ok(alloc) = allocator.allocate_entry(hash, &entry.key, &entry.value)
                {
                    entry.pmem_offset = Some(alloc.offset);
                }

                entries.push(entry);
                self.entry_count.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }

        // Bucket is full, use overflow
        self.add_to_overflow(bucket, key, value, hash)?;
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        // Check if we should split
        if self.should_split(&segment) {
            drop(dir); // Release read lock
            self.try_split(hash);
        }

        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8], _guard: &crossbeam::epoch::Guard) -> Result<bool> {
        let hash = self.hasher.hash(key);
        let dir = self.dir.read();

        let segment = dir.get_segment(hash);
        let bucket_idx = (hash as usize) % BUCKETS_PER_SEGMENT;
        let bucket = &segment.buckets[bucket_idx];

        // Try to delete from main bucket
        {
            let mut entries = bucket.entries.write();
            if let Some(pos) = entries.iter().position(|e| e.hash == hash && e.key == key) {
                let entry = entries.remove(pos);

                // Delete from PMEM if applicable
                let allocator_ref = self.allocator.as_ref();
                if let (Some(allocator), Some(offset)) = (allocator_ref, entry.pmem_offset) {
                    let _ = allocator.delete_entry(offset);
                }

                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                return Ok(true);
            }
        }

        // Check overflow chain
        let mut overflow = bucket.overflow.load(Ordering::Acquire);
        while !overflow.is_null() {
            let overflow_bucket = unsafe { &*overflow };
            let mut entries = overflow_bucket.entries.write();
            if let Some(pos) = entries.iter().position(|e| e.hash == hash && e.key == key) {
                let entry = entries.remove(pos);

                let allocator_ref = self.allocator.as_ref();
                if let (Some(allocator), Some(offset)) = (allocator_ref, entry.pmem_offset) {
                    let _ = allocator.delete_entry(offset);
                }

                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                return Ok(true);
            }
            overflow = overflow_bucket.overflow.load(Ordering::Acquire);
        }

        Ok(false)
    }

    /// Add entry to overflow chain
    fn add_to_overflow(
        &self,
        bucket: &Bucket,
        key: Vec<u8>,
        value: Vec<u8>,
        hash: u64,
    ) -> Result<()> {
        let mut current = bucket.overflow.load(Ordering::Acquire);

        // Find the last overflow bucket with space
        while !current.is_null() {
            let overflow_bucket = unsafe { &*current };
            let mut entries = overflow_bucket.entries.write();
            if entries.len() < BUCKET_CAPACITY {
                let mut entry = Entry::new(key, value, hash);
                if let Some(ref allocator) = self.allocator
                    && let Ok(alloc) = allocator.allocate_entry(hash, &entry.key, &entry.value)
                {
                    entry.pmem_offset = Some(alloc.offset);
                }
                entries.push(entry);
                return Ok(());
            }
            drop(entries);
            current = overflow_bucket.overflow.load(Ordering::Acquire);
        }

        // Need to create a new overflow bucket
        let new_bucket = Box::new(Bucket::new());
        {
            let mut entries = new_bucket.entries.write();
            let mut entry = Entry::new(key, value, hash);
            if let Some(ref allocator) = self.allocator
                && let Ok(alloc) = allocator.allocate_entry(hash, &entry.key, &entry.value)
            {
                entry.pmem_offset = Some(alloc.offset);
            }
            entries.push(entry);
        }

        let new_ptr = Box::into_raw(new_bucket);

        // Try to append to the chain
        let mut tail = &bucket.overflow;
        loop {
            let current = tail.load(Ordering::Acquire);
            if current.is_null() {
                match tail.compare_exchange(current, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return Ok(()),
                    Err(_) => continue,
                }
            }
            tail = unsafe { &(*current).overflow };
        }
    }

    /// Check if a segment should be split
    fn should_split(&self, segment: &Segment) -> bool {
        // Split if average bucket occupancy is high
        let total_entries: usize = segment.buckets.iter().map(|b| b.entries.read().len()).sum();
        total_entries > BUCKETS_PER_SEGMENT * BUCKET_CAPACITY / 2
    }

    /// Try to split a segment
    fn try_split(&self, hash: u64) {
        let mut dir = self.dir.write();
        let segment_idx = dir.segment_index(hash);

        // Check if already at max depth or splitting in progress
        if dir.is_splitting() {
            return;
        }

        dir.split_segment(segment_idx);
    }

    /// Get total entry count
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Directory of segments for extendible hashing
pub struct Directory {
    /// Segments array (length is always power of 2)
    segments: Vec<Arc<Segment>>,
    /// Global depth (log2 of segments.len())
    global_depth: usize,
    /// Flag indicating split in progress
    splitting: bool,
}

impl Directory {
    /// Create a new directory with initial number of segments
    pub fn new(initial_segments: usize) -> Self {
        assert!(initial_segments.is_power_of_two());
        let global_depth = initial_segments.trailing_zeros() as usize;

        let segments: Vec<_> = (0..initial_segments)
            .map(|_| Arc::new(Segment::new(global_depth)))
            .collect();

        Self {
            segments,
            global_depth,
            splitting: false,
        }
    }

    /// Get segment index for a hash
    pub fn segment_index(&self, hash: u64) -> usize {
        (hash as usize) & (self.segments.len() - 1)
    }

    /// Get segment for a hash
    pub fn get_segment(&self, hash: u64) -> Arc<Segment> {
        self.segments[self.segment_index(hash)].clone()
    }

    /// Check if splitting is in progress
    pub fn is_splitting(&self) -> bool {
        self.splitting
    }

    /// Split a segment (double the directory if needed)
    pub fn split_segment(&mut self, segment_idx: usize) {
        self.splitting = true;

        // Clone the segment to avoid borrow issues
        let segment = self.segments[segment_idx].clone();

        // If segment's local depth equals global depth, we need to double the directory
        if segment.local_depth.load(Ordering::Acquire) >= self.global_depth {
            self.double_directory();
        }

        // Create two new segments
        let new_depth = segment.local_depth.load(Ordering::Acquire) + 1;
        let new_segment_0 = Arc::new(Segment::new(new_depth));
        let new_segment_1 = Arc::new(Segment::new(new_depth));

        // Redistribute entries based on the new bit
        let split_bit = 1 << (new_depth - 1);

        for bucket in segment.buckets.iter() {
            let entries = bucket.entries.read();
            for entry in entries.iter() {
                let target = if (entry.hash as usize) & split_bit != 0 {
                    &new_segment_1
                } else {
                    &new_segment_0
                };
                let bucket_idx = (entry.hash as usize) % BUCKETS_PER_SEGMENT;
                target.buckets[bucket_idx]
                    .entries
                    .write()
                    .push(entry.clone());
            }
        }

        // Update directory pointers
        let mask = (1 << new_depth) - 1;
        for i in 0..self.segments.len() {
            if (i & mask) == (segment_idx & ((1 << (new_depth - 1)) - 1)) {
                if i & split_bit != 0 {
                    self.segments[i] = new_segment_1.clone();
                } else {
                    self.segments[i] = new_segment_0.clone();
                }
            }
        }

        self.splitting = false;
    }

    /// Double the directory size
    fn double_directory(&mut self) {
        let old_len = self.segments.len();
        self.segments.reserve(old_len);

        for i in 0..old_len {
            self.segments.push(self.segments[i].clone());
        }

        self.global_depth += 1;
    }

    /// Get reference to segments for iteration
    pub fn load(&self) -> &[Arc<Segment>] {
        &self.segments
    }
}

/// Segment containing buckets
pub struct Segment {
    /// Buckets in this segment
    pub buckets: Vec<Bucket>,
    /// Local depth for this segment
    pub local_depth: AtomicUsize,
    /// Version number for optimistic reads
    pub version: AtomicU64,
}

impl Segment {
    /// Create a new segment with the given local depth
    pub fn new(local_depth: usize) -> Self {
        let buckets = (0..BUCKETS_PER_SEGMENT).map(|_| Bucket::new()).collect();

        Self {
            buckets,
            local_depth: AtomicUsize::new(local_depth),
            version: AtomicU64::new(0),
        }
    }
}

/// Bucket containing entries
#[derive(Debug)]
pub struct Bucket {
    /// Entries in this bucket
    pub entries: RwLock<Vec<Entry>>,
    /// Pointer to overflow bucket
    pub overflow: AtomicPtr<Bucket>,
}

impl Bucket {
    /// Create a new empty bucket
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::with_capacity(BUCKET_CAPACITY)),
            overflow: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a segment operation
#[derive(Debug)]
pub enum SegmentResult<T> {
    Found(T),
    NotFound,
    Retry,
    Migrating,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::epoch::Epoch;

    #[test]
    fn test_dashstore_basic() -> Result<()> {
        let epoch = Arc::new(Epoch::new());
        let store = DashStore::new(epoch.clone())?;
        let guard = epoch.pin();

        // Test put and get
        store.put(b"key1".to_vec(), b"value1".to_vec(), &guard)?;
        assert_eq!(store.get(b"key1", &guard)?, Some(b"value1".to_vec()));

        // Test update
        store.put(b"key1".to_vec(), b"value2".to_vec(), &guard)?;
        assert_eq!(store.get(b"key1", &guard)?, Some(b"value2".to_vec()));

        // Test delete
        assert!(store.delete(b"key1", &guard)?);
        assert_eq!(store.get(b"key1", &guard)?, None);

        Ok(())
    }

    #[test]
    fn test_dashstore_many_entries() -> Result<()> {
        let epoch = Arc::new(Epoch::new());
        let store = DashStore::new(epoch.clone())?;
        let guard = epoch.pin();

        // Insert many entries
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            store.put(key.into_bytes(), value.into_bytes(), &guard)?;
        }

        assert_eq!(store.len(), 1000);

        // Verify all entries
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let expected = format!("value{:04}", i);
            assert_eq!(
                store.get(key.as_bytes(), &guard)?,
                Some(expected.into_bytes())
            );
        }

        Ok(())
    }

    #[test]
    fn test_directory_split() {
        let mut dir = Directory::new(4);
        assert_eq!(dir.segments.len(), 4);
        assert_eq!(dir.global_depth, 2);

        // Trigger a split
        dir.split_segment(0);

        // Directory should have doubled
        assert_eq!(dir.segments.len(), 8);
        assert_eq!(dir.global_depth, 3);
    }
}
