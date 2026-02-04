//! In-Memory Store Utilities
//!
//! Provides utilities for memory management and statistics.

// Memory tracking infrastructure for future use
#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};

/// Memory tracker for monitoring allocations
pub struct MemoryTracker {
    allocated: AtomicUsize,
    peak: AtomicUsize,
}

impl MemoryTracker {
    /// Create a new memory tracker
    pub const fn new() -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// Record an allocation
    pub fn record_alloc(&self, size: usize) {
        let current = self.allocated.fetch_add(size, Ordering::Relaxed) + size;
        // Update peak if necessary
        let mut peak = self.peak.load(Ordering::Relaxed);
        while current > peak {
            match self.peak.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    /// Record a deallocation
    pub fn record_dealloc(&self, size: usize) {
        self.allocated.fetch_sub(size, Ordering::Relaxed);
    }

    /// Get current allocated bytes
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Get peak allocated bytes
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Reset the tracker
    pub fn reset(&self) {
        self.allocated.store(0, Ordering::Relaxed);
        self.peak.store(0, Ordering::Relaxed);
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Global memory tracker instance
pub static MEMORY_TRACKER: MemoryTracker = MemoryTracker::new();

/// Estimate memory usage of a value
pub trait MemorySize {
    fn memory_size(&self) -> usize;
}

impl MemorySize for Vec<u8> {
    fn memory_size(&self) -> usize {
        std::mem::size_of::<Vec<u8>>() + self.capacity()
    }
}

impl MemorySize for String {
    fn memory_size(&self) -> usize {
        std::mem::size_of::<String>() + self.capacity()
    }
}

impl<K: MemorySize, V: MemorySize> MemorySize for (K, V) {
    fn memory_size(&self) -> usize {
        self.0.memory_size() + self.1.memory_size()
    }
}

/// Simple slab allocator for fixed-size allocations
/// Used for entry headers and small values
pub struct SlabAllocator {
    slab_size: usize,
    slabs: Vec<Box<[u8]>>,
    free_list: Vec<usize>,
    next_offset: usize,
}

impl SlabAllocator {
    /// Create a new slab allocator with the given slab size
    pub fn new(slab_size: usize) -> Self {
        Self {
            slab_size,
            slabs: Vec::new(),
            free_list: Vec::new(),
            next_offset: 0,
        }
    }

    /// Allocate a slot in the slab
    pub fn allocate(&mut self) -> Option<(usize, usize)> {
        // Try to reuse a freed slot
        if let Some(offset) = self.free_list.pop() {
            let slab_idx = offset / self.slab_size;
            let slot_offset = offset % self.slab_size;
            return Some((slab_idx, slot_offset));
        }

        // Allocate new slot
        let current_capacity: usize = self.slabs.iter().map(|s| s.len()).sum();
        if self.next_offset >= current_capacity {
            // Need a new slab
            self.slabs.push(vec![0u8; 1024 * 1024].into_boxed_slice()); // 1MB slabs
        }

        let slab_idx = self.next_offset / (1024 * 1024);
        let slot_offset = self.next_offset % (1024 * 1024);
        self.next_offset += self.slab_size;

        Some((slab_idx, slot_offset))
    }

    /// Free a slot
    pub fn free(&mut self, slab_idx: usize, slot_offset: usize) {
        let offset = slab_idx * (1024 * 1024) + slot_offset;
        self.free_list.push(offset);
    }

    /// Get a reference to the data at the given slot
    pub fn get(&self, slab_idx: usize, slot_offset: usize) -> Option<&[u8]> {
        self.slabs
            .get(slab_idx)
            .map(|slab| &slab[slot_offset..slot_offset + self.slab_size])
    }

    /// Get a mutable reference to the data at the given slot
    pub fn get_mut(&mut self, slab_idx: usize, slot_offset: usize) -> Option<&mut [u8]> {
        self.slabs
            .get_mut(slab_idx)
            .map(|slab| &mut slab[slot_offset..slot_offset + self.slab_size])
    }

    /// Get total allocated bytes
    pub fn allocated_bytes(&self) -> usize {
        self.slabs.iter().map(|s| s.len()).sum()
    }

    /// Get used bytes
    pub fn used_bytes(&self) -> usize {
        self.next_offset - (self.free_list.len() * self.slab_size)
    }
}

/// Arena allocator for batch allocations
/// Useful for building up data structures that are freed together
pub struct Arena {
    chunks: Vec<Box<[u8]>>,
    current_chunk: usize,
    current_offset: usize,
    chunk_size: usize,
}

impl Arena {
    /// Create a new arena with default chunk size (1MB)
    pub fn new() -> Self {
        Self::with_chunk_size(1024 * 1024)
    }

    /// Create a new arena with specified chunk size
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self {
            chunks: Vec::new(),
            current_chunk: 0,
            current_offset: 0,
            chunk_size,
        }
    }

    /// Allocate bytes from the arena
    pub fn allocate(&mut self, size: usize) -> &mut [u8] {
        // Ensure we have a chunk
        if self.chunks.is_empty() {
            self.chunks
                .push(vec![0u8; self.chunk_size].into_boxed_slice());
        }

        // Check if current chunk has space
        if self.current_offset + size > self.chunk_size {
            // Need a new chunk
            self.chunks
                .push(vec![0u8; self.chunk_size.max(size)].into_boxed_slice());
            self.current_chunk = self.chunks.len() - 1;
            self.current_offset = 0;
        }

        let start = self.current_offset;
        self.current_offset += size;

        &mut self.chunks[self.current_chunk][start..start + size]
    }

    /// Allocate and copy data
    pub fn alloc_copy(&mut self, data: &[u8]) -> &[u8] {
        let slot = self.allocate(data.len());
        slot.copy_from_slice(data);
        slot
    }

    /// Reset the arena (keeps allocated memory for reuse)
    pub fn reset(&mut self) {
        self.current_chunk = 0;
        self.current_offset = 0;
    }

    /// Get total allocated bytes
    pub fn allocated_bytes(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker() {
        let tracker = MemoryTracker::new();

        tracker.record_alloc(100);
        assert_eq!(tracker.allocated(), 100);
        assert_eq!(tracker.peak(), 100);

        tracker.record_alloc(50);
        assert_eq!(tracker.allocated(), 150);
        assert_eq!(tracker.peak(), 150);

        tracker.record_dealloc(100);
        assert_eq!(tracker.allocated(), 50);
        assert_eq!(tracker.peak(), 150); // Peak should not decrease
    }

    #[test]
    fn test_slab_allocator() {
        let mut slab = SlabAllocator::new(64);

        let (s1, o1) = slab.allocate().unwrap();
        let (s2, o2) = slab.allocate().unwrap();

        assert_ne!((s1, o1), (s2, o2));

        slab.free(s1, o1);
        let (s3, o3) = slab.allocate().unwrap();
        assert_eq!((s1, o1), (s3, o3)); // Should reuse freed slot
    }

    #[test]
    fn test_arena() {
        let mut arena = Arena::with_chunk_size(1024);

        let data1 = arena.allocate(100);
        assert_eq!(data1.len(), 100);

        let data2 = arena.alloc_copy(b"hello");
        assert_eq!(data2, b"hello");

        assert!(arena.allocated_bytes() >= 1024);

        arena.reset();
        // Can allocate again from the beginning
        let data3 = arena.allocate(50);
        assert_eq!(data3.len(), 50);
    }

    #[test]
    fn test_memory_size() {
        let v: Vec<u8> = vec![0u8; 100];
        assert!(v.memory_size() >= 100);

        let s = String::from("hello world");
        assert!(s.memory_size() >= 11);
    }
}
