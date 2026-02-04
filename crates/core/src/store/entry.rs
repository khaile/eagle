//! Entry Metadata Module
//!
//! Provides metadata structures for tracking entry properties like:
//! - Timestamps for access/modify tracking
//! - PMEM persistence offsets
//! - Expiration times for TTL support

use std::time::{Duration, Instant};

/// Entry metadata stored alongside values
#[derive(Clone, Debug)]
pub struct EntryMeta {
    /// When the entry was last accessed/modified
    pub timestamp: Instant,
    /// Offset in PMEM (if persisted)
    pub pmem_offset: Option<u64>,
    /// When the entry expires (None = no expiration)
    pub expires_at: Option<Instant>,
}

impl Default for EntryMeta {
    fn default() -> Self {
        Self {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at: None,
        }
    }
}

impl EntryMeta {
    /// Create metadata with an expiration time
    #[allow(dead_code)]
    pub fn with_ttl(ttl: Duration) -> Self {
        let now = Instant::now();
        Self {
            timestamp: now,
            pmem_offset: None,
            expires_at: Some(now + ttl),
        }
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Instant::now() >= exp)
    }

    /// Get remaining TTL in milliseconds (None if no expiration or expired)
    pub fn ttl_millis(&self) -> Option<u64> {
        self.expires_at.and_then(|exp| {
            let now = Instant::now();
            if now >= exp {
                None
            } else {
                Some(exp.duration_since(now).as_millis() as u64)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_entrymeta_default_and_ttl() {
        let m = EntryMeta::default();
        // Defaults should have no pmem offset and no expiration
        assert!(m.pmem_offset.is_none());
        assert!(m.expires_at.is_none());
        // ttl_millis should be None when no expiration is set
        assert!(m.ttl_millis().is_none());
        // not expired
        assert!(!m.is_expired());
    }

    #[test]
    fn test_entrymeta_with_ttl_and_is_expired() {
        // Short TTL
        let m = EntryMeta::with_ttl(Duration::from_millis(5));
        // Immediately after creation it should not be expired
        assert!(!m.is_expired());
        assert!(m.ttl_millis().is_some());

        // Sleep past the TTL to ensure it becomes expired
        std::thread::sleep(Duration::from_millis(20));
        assert!(m.is_expired());
        assert!(m.ttl_millis().is_none());
    }

    #[test]
    fn test_entrymeta_clone_and_pmem_offset() {
        let m = EntryMeta::default();
        // clone should produce independent value
        let mut m2 = m.clone();
        assert_eq!(m.pmem_offset, m2.pmem_offset);

        // assign pmem_offset on clone and verify independence
        m2.pmem_offset = Some(42);
        assert_eq!(m2.pmem_offset, Some(42));
        assert_eq!(m.pmem_offset, None);
    }
}
