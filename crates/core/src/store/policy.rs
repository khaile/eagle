//! Eviction Policies for the Store
//!
//! Provides various eviction strategies:
//! - None: No eviction
//! - LRU: Evict entries older than TTL
//! - Random: Evict random entries when size exceeds limit
//! - LFU: Evict least frequently used (future)

// Eviction policy infrastructure for future use
#![allow(dead_code)]

use super::EntryMeta;
use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Eviction policy types
#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    /// No eviction - store grows unbounded
    None,
    /// LRU eviction with TTL
    /// Entries older than TTL are evicted
    Lru { ttl: Duration },
    /// Random eviction when size exceeds limit
    /// Removes random entries to stay under max_size
    Random { max_size: usize },
    /// Evict when memory usage exceeds threshold
    MaxMemory { max_bytes: usize },
}

/// Policy configuration
#[derive(Debug, Clone)]
pub struct Policy {
    /// Maximum memory usage in bytes (0 = unlimited)
    pub max_memory: usize,
    /// Eviction strategy
    pub eviction: EvictionPolicy,
    /// Check interval for background eviction (in operations)
    pub check_interval: usize,
    /// Counter for operations since last check
    ops_counter: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            max_memory: 0, // Unlimited
            eviction: EvictionPolicy::None,
            check_interval: 1000,
            ops_counter: 0,
        }
    }
}

impl Policy {
    /// Create a new policy with specified settings
    pub fn new(max_memory: usize, eviction: EvictionPolicy) -> Self {
        Self {
            max_memory,
            eviction,
            check_interval: 1000,
            ops_counter: 0,
        }
    }

    /// Create a policy with LRU eviction
    pub fn lru(ttl: Duration) -> Self {
        Self {
            max_memory: 0,
            eviction: EvictionPolicy::Lru { ttl },
            check_interval: 100,
            ops_counter: 0,
        }
    }

    /// Create a policy with size-based random eviction
    pub fn max_entries(max_size: usize) -> Self {
        Self {
            max_memory: 0,
            eviction: EvictionPolicy::Random { max_size },
            check_interval: 100,
            ops_counter: 0,
        }
    }

    /// Create a policy with memory-based eviction
    pub fn max_memory(max_bytes: usize) -> Self {
        Self {
            max_memory: max_bytes,
            eviction: EvictionPolicy::MaxMemory { max_bytes },
            check_interval: 100,
            ops_counter: 0,
        }
    }

    /// Apply the policy to the store (old interface for compatibility)
    pub fn apply(&self, store: &DashMap<Vec<u8>, (Vec<u8>, Instant)>) {
        match &self.eviction {
            EvictionPolicy::None => {}
            EvictionPolicy::Lru { ttl } => {
                let now = Instant::now();
                store.retain(|_, v| now.duration_since(v.1) < *ttl);
            }
            EvictionPolicy::Random { max_size } => {
                if store.len() > *max_size {
                    let to_remove: Vec<_> = store
                        .iter()
                        .take(store.len() - *max_size)
                        .map(|entry| entry.key().clone())
                        .collect();
                    for key in to_remove {
                        store.remove(&key);
                    }
                }
            }
            EvictionPolicy::MaxMemory { .. } => {
                // Cannot estimate memory with old interface
            }
        }
    }

    /// Apply the policy to the store with EntryMeta
    pub fn apply_entries(&self, store: &DashMap<Vec<u8>, (Vec<u8>, EntryMeta)>) {
        match &self.eviction {
            EvictionPolicy::None => {}
            EvictionPolicy::Lru { ttl } => {
                let now = Instant::now();
                store.retain(|_, v| now.duration_since(v.1.timestamp) < *ttl);
            }
            EvictionPolicy::Random { max_size } => {
                self.evict_random(store, *max_size);
            }
            EvictionPolicy::MaxMemory { max_bytes } => {
                self.evict_by_memory(store, *max_bytes);
            }
        }
    }

    /// Evict random entries to stay under max_size
    fn evict_random(&self, store: &DashMap<Vec<u8>, (Vec<u8>, EntryMeta)>, max_size: usize) {
        if store.len() <= max_size {
            return;
        }

        let to_remove_count = store.len() - max_size;
        let to_remove: Vec<_> = store
            .iter()
            .take(to_remove_count)
            .map(|entry| entry.key().clone())
            .collect();

        for key in to_remove {
            store.remove(&key);
        }
    }

    /// Evict entries by memory usage
    fn evict_by_memory(&self, store: &DashMap<Vec<u8>, (Vec<u8>, EntryMeta)>, max_bytes: usize) {
        // Estimate current memory usage
        let current_size: usize = store
            .iter()
            .map(|entry| {
                entry.key().len() + entry.value().0.len() + std::mem::size_of::<EntryMeta>()
            })
            .sum();

        if current_size <= max_bytes {
            return;
        }

        // Collect entries sorted by timestamp (oldest first)
        let mut entries: Vec<_> = store
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.key().len() + entry.value().0.len(),
                    entry.value().1.timestamp,
                )
            })
            .collect();

        entries.sort_by_key(|e| e.2);

        // Evict oldest entries until under limit
        let mut freed = 0usize;
        let target_free = current_size - max_bytes;

        for (key, size, _) in entries {
            if freed >= target_free {
                break;
            }
            store.remove(&key);
            freed += size;
        }
    }

    /// Check if eviction should run based on operation count
    pub fn should_check(&mut self) -> bool {
        self.ops_counter += 1;
        if self.ops_counter >= self.check_interval {
            self.ops_counter = 0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_none() {
        let policy = Policy::default();
        let store: DashMap<Vec<u8>, (Vec<u8>, EntryMeta)> = DashMap::new();

        for i in 0..100 {
            store.insert(
                format!("key{}", i).into_bytes(),
                (format!("value{}", i).into_bytes(), EntryMeta::default()),
            );
        }

        policy.apply_entries(&store);
        assert_eq!(store.len(), 100);
    }

    #[test]
    fn test_policy_lru() {
        let policy = Policy::lru(Duration::from_millis(50));
        let store: DashMap<Vec<u8>, (Vec<u8>, EntryMeta)> = DashMap::new();

        store.insert(b"key1".to_vec(), (b"value1".to_vec(), EntryMeta::default()));

        // Entry should exist immediately
        policy.apply_entries(&store);
        assert_eq!(store.len(), 1);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(100));
        policy.apply_entries(&store);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_policy_random() {
        let policy = Policy::max_entries(10);
        let store: DashMap<Vec<u8>, (Vec<u8>, EntryMeta)> = DashMap::new();

        for i in 0..20 {
            store.insert(
                format!("key{}", i).into_bytes(),
                (format!("value{}", i).into_bytes(), EntryMeta::default()),
            );
        }

        policy.apply_entries(&store);
        assert_eq!(store.len(), 10);
    }

    #[test]
    fn test_policy_max_memory() {
        // Each entry is roughly: key(4) + value(6) + meta(~40) = ~50 bytes
        let policy = Policy::max_memory(500); // Allow ~10 entries
        let store: DashMap<Vec<u8>, (Vec<u8>, EntryMeta)> = DashMap::new();

        for i in 0..20 {
            store.insert(
                format!("k{:02}", i).into_bytes(),
                (format!("v{:04}", i).into_bytes(), EntryMeta::default()),
            );
        }

        policy.apply_entries(&store);
        assert!(store.len() < 20);
    }
}
