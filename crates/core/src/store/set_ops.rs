#![allow(dead_code)]
//! SET Operations Module
//!
//! Provides options and result types for SET operations with
//! support for conditional setting, TTL management, and atomic operations.

use std::time::Duration;

/// Options for SET operations
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetOptions {
    /// Only set if key does not exist
    pub nx: bool,
    /// Only set if key already exists
    pub xx: bool,
    /// Return the old value
    pub get: bool,
    /// Keep existing TTL
    pub keep_ttl: bool,
    /// New expiration duration
    pub expiration: Option<Duration>,
}

/// Result of a SET operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetResult {
    /// Set was performed successfully
    Ok,
    /// Set was performed, returning old value
    OkWithOldValue(Vec<u8>),
    /// Set was not performed (condition not met)
    NotPerformed,
    /// Set was not performed, but returning old value (GET option)
    NotPerformedWithOldValue(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_setoptions_default() {
        let opts = SetOptions::default();
        assert!(!opts.nx, "nx should default to false");
        assert!(!opts.xx, "xx should default to false");
        assert!(!opts.get, "get should default to false");
        assert!(!opts.keep_ttl, "keep_ttl should default to false");
        assert!(
            opts.expiration.is_none(),
            "expiration should default to None"
        );
    }

    #[test]
    fn test_setoptions_custom_values() {
        let opts = SetOptions {
            nx: true,
            xx: false,
            get: true,
            keep_ttl: true,
            expiration: Some(Duration::from_secs(10)),
        };

        assert!(opts.nx);
        assert!(!opts.xx);
        assert!(opts.get);
        assert!(opts.keep_ttl);
        assert_eq!(opts.expiration.unwrap().as_secs(), 10);
    }

    #[test]
    fn test_setresult_variants_match_and_contents() {
        // Ok variant
        let r = SetResult::Ok;
        match r {
            SetResult::Ok => {}
            _ => panic!("Expected SetResult::Ok"),
        }

        // OkWithOldValue variant
        let old = b"old".to_vec();
        let r = SetResult::OkWithOldValue(old.clone());
        match r {
            SetResult::OkWithOldValue(v) => assert_eq!(v, old),
            _ => panic!("Expected SetResult::OkWithOldValue"),
        }

        // NotPerformed variant
        let r = SetResult::NotPerformed;
        match r {
            SetResult::NotPerformed => {}
            _ => panic!("Expected SetResult::NotPerformed"),
        }

        // NotPerformedWithOldValue variant
        let old2 = b"previous".to_vec();
        let r = SetResult::NotPerformedWithOldValue(old2.clone());
        match r {
            SetResult::NotPerformedWithOldValue(v) => assert_eq!(v, old2),
            _ => panic!("Expected SetResult::NotPerformedWithOldValue"),
        }
    }

    #[test]
    fn test_setresult_debug_format() {
        let _ = format!("{:?}", SetResult::Ok);
        let _ = format!("{:?}", SetResult::OkWithOldValue(vec![1, 2, 3]));
        let _ = format!("{:?}", SetResult::NotPerformed);
        let _ = format!("{:?}", SetResult::NotPerformedWithOldValue(Vec::new()));
    }
}
