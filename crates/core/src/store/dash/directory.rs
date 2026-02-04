//! Directory implementation for extendible hashing
//!
//! The directory maps hash prefixes to segments. It can grow dynamically
//! by doubling when segments need to split.

#![allow(dead_code)]

use super::segment::Segment;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Initial number of directory entries (must be power of 2)
const INITIAL_SIZE: usize = 16;

/// Maximum directory size (2^24 = 16M segments)
const MAX_DEPTH: usize = 24;

/// Directory for extendible hashing
pub struct Directory {
    /// Segment pointers (length is always power of 2)
    segments: RwLock<Vec<Arc<Segment>>>,
    /// Global depth (log2 of directory size)
    global_depth: AtomicUsize,
    /// Lock for directory expansion
    resize_lock: Mutex<()>,
    /// Flag indicating resize in progress
    resizing: AtomicBool,
}

impl Directory {
    /// Create a new directory with initial size
    pub fn new() -> Self {
        Self::with_size(INITIAL_SIZE)
    }

    /// Create a directory with specified initial size
    pub fn with_size(size: usize) -> Self {
        assert!(size.is_power_of_two());
        let depth = size.trailing_zeros() as usize;

        let segments: Vec<_> = (0..size)
            .map(|_| Arc::new(Segment::new(depth as u64)))
            .collect();

        Self {
            segments: RwLock::new(segments),
            global_depth: AtomicUsize::new(depth),
            resize_lock: Mutex::new(()),
            resizing: AtomicBool::new(false),
        }
    }

    /// Get the segment index for a hash value
    #[inline]
    pub fn segment_index(&self, hash: u64) -> usize {
        let depth = self.global_depth.load(Ordering::Acquire);
        let mask = (1usize << depth) - 1;
        (hash as usize) & mask
    }

    /// Get the segment for a hash value
    pub fn get_segment(&self, hash: u64) -> Arc<Segment> {
        let segments = self.segments.read();
        let idx = self.segment_index(hash);
        segments[idx].clone()
    }

    /// Get all segments (for iteration)
    pub fn all_segments(&self) -> Vec<Arc<Segment>> {
        self.segments.read().clone()
    }

    /// Get current number of directory entries
    pub fn size(&self) -> usize {
        self.segments.read().len()
    }

    /// Get global depth
    pub fn depth(&self) -> usize {
        self.global_depth.load(Ordering::Acquire)
    }

    /// Check if resizing is in progress
    pub fn is_resizing(&self) -> bool {
        self.resizing.load(Ordering::Acquire)
    }

    /// Internal expand method (must hold resize_lock)
    fn do_expand(&self) -> bool {
        let current_depth = self.global_depth.load(Ordering::Acquire);
        if current_depth >= MAX_DEPTH {
            return false; // Cannot grow further
        }

        self.resizing.store(true, Ordering::Release);

        let mut segments = self.segments.write();
        let old_len = segments.len();

        // Double the directory by duplicating pointers
        // Clone first, then extend to avoid borrowing issues
        let cloned: Vec<_> = (0..old_len).map(|i| segments[i].clone()).collect();
        segments.extend(cloned);

        self.global_depth.fetch_add(1, Ordering::Release);
        self.resizing.store(false, Ordering::Release);

        true
    }

    /// Expand the directory by doubling its size
    pub fn expand(&self) -> bool {
        let _lock = self.resize_lock.lock();
        self.do_expand()
    }

    /// Split a segment at the given index
    pub fn split_segment(&self, segment_idx: usize) -> bool {
        let _lock = self.resize_lock.lock();

        let mut segments = self.segments.write();
        let segment = &segments[segment_idx];
        let local_depth = segment.local_depth();
        let global_depth = self.global_depth.load(Ordering::Acquire);

        // If local depth equals global depth, need to expand first
        if local_depth as usize >= global_depth {
            drop(segments);
            if !self.do_expand() {
                return false;
            }
            segments = self.segments.write();
        }

        let segment = &segments[segment_idx];
        let new_depth = segment.local_depth() + 1;

        // Split the segment
        let (seg0, seg1) = segment.split(new_depth);
        let seg0 = Arc::new(seg0);
        let seg1 = Arc::new(seg1);

        // Update directory entries
        let local_depth = new_depth - 1;
        let split_bit = 1usize << local_depth;
        let old_mask = (1usize << local_depth) - 1;
        let old_prefix = segment_idx & old_mask;

        for i in 0..segments.len() {
            if (i & old_mask) == old_prefix {
                if (i & split_bit) != 0 {
                    segments[i] = seg1.clone();
                } else {
                    segments[i] = seg0.clone();
                }
            }
        }

        true
    }

    /// Get statistics about the directory
    pub fn stats(&self) -> DirectoryStats {
        let segments = self.segments.read();
        let mut total_entries = 0;
        let mut total_buckets = 0;
        let mut unique_segments = std::collections::HashSet::new();

        for segment in segments.iter() {
            let ptr = Arc::as_ptr(segment) as usize;
            if unique_segments.insert(ptr) {
                total_entries += segment.len();
                total_buckets += super::segment::BUCKETS_PER_SEGMENT;
            }
        }

        DirectoryStats {
            directory_size: segments.len(),
            global_depth: self.global_depth.load(Ordering::Relaxed),
            unique_segments: unique_segments.len(),
            total_entries,
            total_buckets,
            load_factor: total_entries as f64 / total_buckets as f64,
        }
    }
}

impl Default for Directory {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the directory
#[derive(Debug, Clone)]
pub struct DirectoryStats {
    pub directory_size: usize,
    pub global_depth: usize,
    pub unique_segments: usize,
    pub total_entries: usize,
    pub total_buckets: usize,
    pub load_factor: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_new() {
        let dir = Directory::new();
        assert_eq!(dir.size(), INITIAL_SIZE);
        assert_eq!(dir.depth(), INITIAL_SIZE.trailing_zeros() as usize);
    }

    #[test]
    fn test_segment_index() {
        let dir = Directory::with_size(16);

        // Segment index should use lower bits
        assert_eq!(dir.segment_index(0), 0);
        assert_eq!(dir.segment_index(1), 1);
        assert_eq!(dir.segment_index(15), 15);
        assert_eq!(dir.segment_index(16), 0); // Wraps around
        assert_eq!(dir.segment_index(17), 1);
    }

    #[test]
    fn test_directory_expand() {
        let dir = Directory::with_size(4);
        assert_eq!(dir.size(), 4);
        assert_eq!(dir.depth(), 2);

        assert!(dir.expand());
        assert_eq!(dir.size(), 8);
        assert_eq!(dir.depth(), 3);
    }

    #[test]
    fn test_directory_split() {
        let dir = Directory::with_size(4);

        // Add some entries first
        {
            let segments = dir.segments.read();
            for i in 0..10 {
                let hash = i as u64;
                let segment = &segments[dir.segment_index(hash)];
                segment.insert(
                    hash,
                    format!("key{}", i).into_bytes(),
                    format!("val{}", i).into_bytes(),
                );
            }
        }

        // Split segment 0
        assert!(dir.split_segment(0));

        // Should have more segments now
        let stats = dir.stats();
        assert!(stats.unique_segments >= 2);
    }

    #[test]
    fn test_directory_stats() {
        let dir = Directory::with_size(4);

        // Add entries
        {
            let segments = dir.segments.read();
            for i in 0..100 {
                let hash = (i * 7) as u64; // Spread across segments
                let segment = &segments[dir.segment_index(hash)];
                segment.insert(
                    hash,
                    format!("k{}", i).into_bytes(),
                    format!("v{}", i).into_bytes(),
                );
            }
        }

        let stats = dir.stats();
        assert_eq!(stats.total_entries, 100);
        assert!(stats.load_factor > 0.0);
    }
}
