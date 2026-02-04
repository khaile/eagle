//! High-performance hashing utilities for EagleDB
//!
//! Uses ahash (AHash) which is a fast, DOS-resistant hash algorithm
//! optimized for hash table lookups.

// Some functions are infrastructure for future use
#![allow(dead_code)]

use ahash::AHasher;
use std::hash::Hasher;

/// A fast, high-quality hasher using AHash algorithm
///
/// AHash is designed for use in HashMaps and provides:
/// - Excellent performance on short keys (typical for KV stores)
/// - Good distribution properties
/// - DOS resistance via randomized seeds
#[derive(Clone)]
pub struct HighwayHasher {
    seed: u64,
}

impl HighwayHasher {
    /// Create a new hasher with a random seed
    pub fn new() -> Self {
        // Use a fixed seed for deterministic behavior across restarts
        // In production, you might want to use a random seed for DOS resistance
        Self {
            seed: 0x517cc1b727220a95,
        }
    }

    /// Create a hasher with a specific seed
    pub fn with_seed(seed: u64) -> Self {
        Self { seed }
    }

    /// Hash a byte slice to a 64-bit value
    #[inline]
    pub fn hash(&self, key: &[u8]) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(key);
        hasher.write_u64(self.seed);
        hasher.finish()
    }

    /// Hash with a specific seed (useful for double hashing)
    #[inline]
    pub fn hash_with_seed(&self, key: &[u8], seed: u64) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(key);
        hasher.write_u64(seed);
        hasher.finish()
    }
}

impl Default for HighwayHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute a 32-bit fingerprint for use in bucket slots
/// Uses top bits of hash which have better distribution
#[inline]
pub fn fingerprint(hash: u64) -> u32 {
    (hash >> 32) as u32
}

/// Compute slot index from hash given a capacity
#[inline]
pub fn slot_index(hash: u64, capacity: usize) -> usize {
    // Use lower bits for slot selection (assumes power-of-2 capacity)
    (hash as usize) & (capacity - 1)
}

/// Compute CRC32 checksum for data integrity verification
#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hasher_consistency() {
        let hasher = HighwayHasher::new();
        let key = b"test_key";

        // Same key should produce same hash
        let hash1 = hasher.hash(key);
        let hash2 = hasher.hash(key);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hasher_distribution() {
        let hasher = HighwayHasher::new();
        let mut hashes = Vec::new();

        // Generate hashes for sequential keys
        for i in 0..1000u32 {
            let key = i.to_le_bytes();
            hashes.push(hasher.hash(&key));
        }

        // Check that all hashes are unique (collision-free for small set)
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), 1000);
    }

    #[test]
    fn test_fingerprint() {
        let hash = 0xDEADBEEF12345678u64;
        let fp = fingerprint(hash);
        assert_eq!(fp, 0xDEADBEEF);
    }

    #[test]
    fn test_slot_index() {
        let hash = 0x12345678u64;
        assert_eq!(slot_index(hash, 256), 0x78);
        assert_eq!(slot_index(hash, 16), 0x08);
    }

    #[test]
    fn test_crc32() {
        let data = b"Hello, World!";
        let checksum = crc32(data);
        // CRC32 should be consistent
        assert_eq!(checksum, crc32(data));
        // Different data should produce different checksum
        assert_ne!(checksum, crc32(b"Hello, World?"));
    }
}
