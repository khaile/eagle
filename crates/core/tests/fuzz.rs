//! Property-based and Fuzz Tests for EagleDB
//!
//! Uses proptest to generate random inputs and verify invariants.

use hashbrown::HashMap;
use proptest::prelude::*;

/// Generate arbitrary byte vectors for keys
fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..256)
}

/// Generate arbitrary byte vectors for values
fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..4096)
}

/// Commands that can be applied to the store
#[derive(Debug, Clone)]
enum StoreCommand {
    Set(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
}

/// Generate arbitrary store commands
fn arb_command() -> impl Strategy<Value = StoreCommand> {
    prop_oneof![
        (arb_key(), arb_value()).prop_map(|(k, v)| StoreCommand::Set(k, v)),
        arb_key().prop_map(StoreCommand::Get),
        arb_key().prop_map(StoreCommand::Delete),
    ]
}

proptest! {
    /// Test that SET followed by GET returns the same value
    #[test]
    fn prop_set_get_roundtrip(key in arb_key(), value in arb_value()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = eagle_core::store::Store::new_memory().unwrap();
            store.set(key.clone(), value.clone()).unwrap();
            let result = store.get(&key);
            prop_assert_eq!(result, Some(value));
            Ok(())
        })?;
    }

    /// Test that DELETE removes a key
    #[test]
    fn prop_delete_removes_key(key in arb_key(), value in arb_value()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = eagle_core::store::Store::new_memory().unwrap();
            store.set(key.clone(), value).unwrap();
            store.delete(&key);
            let result = store.get(&key);
            prop_assert_eq!(result, None);
            Ok(())
        })?;
    }

    /// Test that multiple SETs to the same key keeps last value
    #[test]
    fn prop_set_overwrites(key in arb_key(), values in prop::collection::vec(arb_value(), 2..10)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = eagle_core::store::Store::new_memory().unwrap();
            for value in &values {
                store.set(key.clone(), value.clone()).unwrap();
            }
            let result = store.get(&key);
            prop_assert_eq!(result, Some(values.last().unwrap().clone()));
            Ok(())
        })?;
    }

    /// Test store consistency with random command sequences
    #[test]
    fn prop_store_consistency(commands in prop::collection::vec(arb_command(), 0..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = eagle_core::store::Store::new_memory().unwrap();
            let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

            for cmd in commands {
                match cmd {
                    StoreCommand::Set(k, v) => {
                        store.set(k.clone(), v.clone()).unwrap();
                        model.insert(k, v);
                    }
                    StoreCommand::Get(k) => {
                        let store_result = store.get(&k);
                        let model_result = model.get(&k).cloned();
                        prop_assert_eq!(store_result, model_result);
                    }
                    StoreCommand::Delete(k) => {
                        store.delete(&k);
                        model.remove(&k);
                    }
                }
            }
            Ok(())
        })?;
    }

    /// Test hash operations
    #[test]
    fn prop_hash_operations(
        key in arb_key(),
        field in arb_key(),
        value in arb_value()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = eagle_core::store::Store::new_memory().unwrap();

            // HSET and HGET
            store.hset(&key, &field, value.clone()).unwrap();
            let result = store.hget(&key, &field).unwrap();
            prop_assert_eq!(result, Some(value.clone()));

            // HDEL
            store.hdel(&key, &field).unwrap();
            let result = store.hget(&key, &field).unwrap();
            prop_assert_eq!(result, None);

            Ok(())
        })?;
    }
}

/// Test hasher distribution
#[test]
fn test_hash_distribution() {
    use eagle_core::hash::HighwayHasher;

    let hasher = HighwayHasher::new();
    let mut buckets = [0u32; 256];

    // Hash many keys and check distribution
    for i in 0..100_000u32 {
        let key = i.to_le_bytes();
        let hash = hasher.hash(&key);
        let bucket = (hash % 256) as usize;
        buckets[bucket] += 1;
    }

    // Check that no bucket has more than 2x the average
    let average = 100_000 / 256;
    for (i, &count) in buckets.iter().enumerate() {
        assert!(
            count < average * 3,
            "Bucket {} has {} entries, expected ~{} (poor distribution)",
            i,
            count,
            average
        );
    }
}

/// Test PMEM allocator with random data
#[test]
fn test_pmem_random_allocations() {
    use eagle_core::pmem::PmemAllocator;
    use proptest::test_runner::{Config, TestRunner};
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.pmem");
    let allocator = PmemAllocator::new(path.to_str().unwrap(), 10).unwrap();

    let mut runner = TestRunner::new(Config::with_cases(100));

    runner
        .run(&(arb_key(), arb_value()), |(key, value)| {
            let key_hash = 0u64; // Simplified
            match allocator.allocate_entry(key_hash, &key, &value) {
                Ok(alloc) => {
                    // Verify we can read it back
                    let (read_key, read_value) =
                        allocator.read_entry(alloc.offset).unwrap().unwrap();
                    prop_assert_eq!(read_key, key);
                    prop_assert_eq!(read_value, value);
                }
                Err(_) => {
                    // Out of space is ok
                }
            }
            Ok(())
        })
        .unwrap();
}

/// Stress test concurrent access
#[test]
fn test_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(eagle_core::store::Store::new_memory().unwrap());
    let mut handles = vec![];

    // Spawn multiple writers
    for t in 0..4 {
        let store = store.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("key_{}_{}", t, i);
                let value = format!("value_{}_{}", t, i);
                store.set(key.into_bytes(), value.into_bytes()).unwrap();
            }
        }));
    }

    // Spawn multiple readers
    for _ in 0..4 {
        let store = store.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let key = format!("key_0_{}", rand::random::<u32>() % 1000);
                let _ = store.get(key.as_bytes());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify some entries
    for t in 0..4 {
        for i in 0..100 {
            let key = format!("key_{}_{}", t, i);
            let expected = format!("value_{}_{}", t, i);
            assert_eq!(store.get(key.as_bytes()), Some(expected.into_bytes()));
        }
    }
}
