//! Key Scanning Command Implementations
//!
//! Implements key scanning commands for the Redis protocol:
//! - KEYS: Find keys matching a pattern
//! - SCAN: Incrementally iterate keys
//! - DBSIZE: Get the number of keys in the database
//! - RANDOMKEY: Return a random key
//! - FLUSHDB: Delete all keys

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::pattern::Pattern;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;
use std::collections::VecDeque;
use tracing::warn;

/// Threshold for warning about KEYS command on large datasets
const KEYS_WARNING_THRESHOLD: usize = 10_000;

/// Default COUNT for SCAN command
const DEFAULT_SCAN_COUNT: usize = 10;

/// KEYS command - find keys matching a glob pattern
/// WARNING: This is O(n) and should not be used in production on large datasets
#[derive(Debug)]
pub struct Keys {
    pub pattern: Bytes,
}

impl Keys {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("keys").into());
        }

        let pattern = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Keys { pattern })
    }
}

#[async_trait]
impl CommandHandler for Keys {
    fn name(&self) -> &'static str {
        "KEYS"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        // Warn if database is large
        let dbsize = store.dbsize();
        if dbsize > KEYS_WARNING_THRESHOLD {
            warn!(
                "KEYS command executed on large dataset ({} keys). Consider using SCAN instead.",
                dbsize
            );
        }

        let pattern = Pattern::new(&self.pattern);
        let keys = store.keys(&pattern);

        // Convert to RespValue array
        let result: VecDeque<RespValue> = keys
            .into_iter()
            .map(|k| RespValue::BulkString(Bytes::from(k)))
            .collect();

        Ok(RespValue::Array(result))
    }
}

/// SCAN command - incrementally iterate over keys
/// Syntax: SCAN cursor [MATCH pattern] [COUNT count]
#[derive(Debug)]
pub struct Scan {
    pub cursor: u64,
    pub pattern: Option<Bytes>,
    pub count: usize,
}

impl Scan {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(CommandError::wrong_arity("scan").into());
        }

        // Parse cursor
        let cursor = match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|_| CommandError::invalid_cursor())?;
                s.parse::<u64>()
                    .map_err(|_| CommandError::invalid_cursor())?
            }
            RespValue::Integer(n) => {
                if n < 0 {
                    return Err(CommandError::invalid_cursor().into());
                }
                n as u64
            }
            _ => return Err(CommandError::invalid_cursor().into()),
        };

        let mut pattern = None;
        let mut count = DEFAULT_SCAN_COUNT;

        // Parse optional arguments
        while !args.is_empty() {
            let option = match args.remove(0) {
                RespValue::BulkString(bytes) => bytes,
                _ => return Err(CommandError::syntax().into()),
            };

            if option.eq_ignore_ascii_case(b"MATCH") {
                if args.is_empty() {
                    return Err(CommandError::syntax().into());
                }
                pattern = Some(match args.remove(0) {
                    RespValue::BulkString(bytes) => bytes,
                    _ => return Err(CommandError::syntax().into()),
                });
            } else if option.eq_ignore_ascii_case(b"COUNT") {
                if args.is_empty() {
                    return Err(CommandError::syntax().into());
                }
                count = match args.remove(0) {
                    RespValue::BulkString(bytes) => {
                        let s =
                            std::str::from_utf8(&bytes).map_err(|_| CommandError::not_integer())?;
                        let n: usize = s.parse().map_err(|_| CommandError::not_integer())?;
                        if n == 0 {
                            return Err(CommandError::not_integer().into());
                        }
                        n
                    }
                    RespValue::Integer(n) if n > 0 => n as usize,
                    _ => return Err(CommandError::not_integer().into()),
                };
            } else if option.eq_ignore_ascii_case(b"TYPE") {
                // TYPE option is not currently supported - reject with clear error
                return Err(CommandError::generic("TYPE option is not supported").into());
            } else {
                return Err(CommandError::syntax().into());
            }
        }

        Ok(Scan {
            cursor,
            pattern,
            count,
        })
    }
}

#[async_trait]
impl CommandHandler for Scan {
    fn name(&self) -> &'static str {
        "SCAN"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let pattern = self.pattern.as_ref().map(|p| Pattern::new(p));
        let (new_cursor, keys) = store.scan(self.cursor, pattern.as_ref(), self.count);

        // Build response: [cursor, [keys...]]
        let keys_array: VecDeque<RespValue> = keys
            .into_iter()
            .map(|k| RespValue::BulkString(Bytes::from(k)))
            .collect();

        let mut result = VecDeque::new();
        result.push_back(RespValue::BulkString(Bytes::from(new_cursor.to_string())));
        result.push_back(RespValue::Array(keys_array));

        Ok(RespValue::Array(result))
    }
}

/// DBSIZE command - return the number of keys in the database
#[derive(Debug)]
pub struct DbSize;

impl DbSize {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if !args.is_empty() {
            return Err(CommandError::wrong_arity("dbsize").into());
        }
        Ok(DbSize)
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        DbSize
    }
}

impl Default for DbSize {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for DbSize {
    fn name(&self) -> &'static str {
        "DBSIZE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let size = store.dbsize();
        Ok(RespValue::Integer(size as i64))
    }
}

/// RANDOMKEY command - return a random key from the database
#[derive(Debug)]
pub struct RandomKey;

impl RandomKey {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if !args.is_empty() {
            return Err(CommandError::wrong_arity("randomkey").into());
        }
        Ok(RandomKey)
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        RandomKey
    }
}

impl Default for RandomKey {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for RandomKey {
    fn name(&self) -> &'static str {
        "RANDOMKEY"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        match store.random_key() {
            Some(key) => Ok(RespValue::BulkString(Bytes::from(key))),
            None => Ok(RespValue::Null),
        }
    }
}

/// FLUSHDB command - delete all keys in the current database
#[derive(Debug)]
pub struct FlushDb {
    #[allow(dead_code)]
    pub async_mode: bool,
}

impl FlushDb {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        let mut async_mode = false;

        // Check for ASYNC or SYNC option
        if !args.is_empty() {
            let option = match args.remove(0) {
                RespValue::BulkString(bytes) => bytes,
                _ => return Err(CommandError::syntax().into()),
            };

            if option.eq_ignore_ascii_case(b"ASYNC") {
                async_mode = true;
            } else if option.eq_ignore_ascii_case(b"SYNC") {
                async_mode = false;
            } else {
                return Err(CommandError::syntax().into());
            }
        }

        if !args.is_empty() {
            return Err(CommandError::wrong_arity("flushdb").into());
        }

        Ok(FlushDb { async_mode })
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        FlushDb { async_mode: false }
    }
}

impl Default for FlushDb {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for FlushDb {
    fn name(&self) -> &'static str {
        "FLUSHDB"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        // Note: async_mode is accepted but we always flush synchronously
        // A true async implementation would spawn a background task
        let _count = store.flush_db();
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== KEYS Tests ==========

    #[tokio::test]
    async fn test_keys_all() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.set(b"other".to_vec(), b"value3".to_vec())?;

        let keys = Keys {
            pattern: Bytes::from("*"),
        };
        let result = keys.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_keys_pattern() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.set(b"other".to_vec(), b"value3".to_vec())?;

        let keys = Keys {
            pattern: Bytes::from("key*"),
        };
        let result = keys.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_keys_empty() -> Result<()> {
        let store = Store::new_memory()?;

        let keys = Keys {
            pattern: Bytes::from("*"),
        };
        let result = keys.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert!(arr.is_empty());
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_keys_includes_hashes() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"string_key".to_vec(), b"value".to_vec())?;
        store.hset(b"hash_key", b"field", b"value".to_vec())?;

        let keys = Keys {
            pattern: Bytes::from("*"),
        };
        let result = keys.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    // ========== SCAN Tests ==========

    #[tokio::test]
    async fn test_scan_empty() -> Result<()> {
        let store = Store::new_memory()?;

        let scan = Scan {
            cursor: 0,
            pattern: None,
            count: 10,
        };
        let result = scan.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // Cursor should be "0" (iteration complete)
            if let Some(RespValue::BulkString(cursor)) = arr.front() {
                assert_eq!(cursor.as_ref(), b"0");
            }
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_basic() -> Result<()> {
        let store = Store::new_memory()?;
        for i in 0..5 {
            store.set(format!("key{}", i).into_bytes(), b"value".to_vec())?;
        }

        let scan = Scan {
            cursor: 0,
            pattern: None,
            count: 10,
        };
        let result = scan.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // Should have all 5 keys since count >= total
            if let Some(RespValue::Array(keys)) = arr.get(1) {
                assert_eq!(keys.len(), 5);
            }
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_pattern() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"user:1".to_vec(), b"value".to_vec())?;
        store.set(b"user:2".to_vec(), b"value".to_vec())?;
        store.set(b"session:1".to_vec(), b"value".to_vec())?;

        let scan = Scan {
            cursor: 0,
            pattern: Some(Bytes::from("user:*")),
            count: 10,
        };
        let result = scan.execute(&store).await?;

        if let RespValue::Array(arr) = result {
            if let Some(RespValue::Array(keys)) = arr.get(1) {
                assert_eq!(keys.len(), 2);
            }
        } else {
            panic!("Expected array result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_iteration() -> Result<()> {
        let store = Store::new_memory()?;
        for i in 0..20 {
            store.set(format!("key{:02}", i).into_bytes(), b"value".to_vec())?;
        }

        let mut all_keys = Vec::new();
        let mut cursor = 0u64;

        // Iterate through all keys
        loop {
            let scan = Scan {
                cursor,
                pattern: None,
                count: 5,
            };
            let result = scan.execute(&store).await?;

            if let RespValue::Array(arr) = result {
                if let Some(RespValue::BulkString(c)) = arr.front() {
                    cursor = std::str::from_utf8(c)?.parse()?;
                }
                if let Some(RespValue::Array(keys)) = arr.get(1) {
                    for key in keys {
                        if let RespValue::BulkString(k) = key {
                            all_keys.push(k.clone());
                        }
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        // Should have seen all 20 keys
        assert_eq!(all_keys.len(), 20);

        Ok(())
    }

    // ========== DBSIZE Tests ==========

    #[tokio::test]
    async fn test_dbsize_empty() -> Result<()> {
        let store = Store::new_memory()?;

        let dbsize = DbSize::new();
        let result = dbsize.execute(&store).await?;

        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_dbsize_with_keys() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.hset(b"hash1", b"field", b"value".to_vec())?;

        let dbsize = DbSize::new();
        let result = dbsize.execute(&store).await?;

        assert!(matches!(result, RespValue::Integer(3)));

        Ok(())
    }

    // ========== RANDOMKEY Tests ==========

    #[tokio::test]
    async fn test_randomkey_empty() -> Result<()> {
        let store = Store::new_memory()?;

        let randomkey = RandomKey::new();
        let result = randomkey.execute(&store).await?;

        assert!(matches!(result, RespValue::Null));

        Ok(())
    }

    #[tokio::test]
    async fn test_randomkey_with_keys() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;

        let randomkey = RandomKey::new();
        let result = randomkey.execute(&store).await?;

        if let RespValue::BulkString(key) = result {
            assert!(key == "key1" || key == "key2");
        } else {
            panic!("Expected bulk string result");
        }

        Ok(())
    }

    // ========== FLUSHDB Tests ==========

    #[tokio::test]
    async fn test_flushdb() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.hset(b"hash1", b"field", b"value".to_vec())?;

        assert_eq!(store.dbsize(), 3);

        let flushdb = FlushDb::new();
        let result = flushdb.execute(&store).await?;

        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));
        assert_eq!(store.dbsize(), 0);

        Ok(())
    }

    // ========== Parse Tests ==========

    #[test]
    fn test_parse_keys() -> Result<()> {
        let args = vec![RespValue::BulkString(Bytes::from("user:*"))];
        let keys = Keys::parse(args)?;
        assert_eq!(keys.pattern, Bytes::from("user:*"));
        Ok(())
    }

    #[test]
    fn test_parse_keys_wrong_args() {
        assert!(Keys::parse(vec![]).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("*")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(Keys::parse(args).is_err());
    }

    #[test]
    fn test_parse_scan() -> Result<()> {
        let args = vec![RespValue::BulkString(Bytes::from("0"))];
        let scan = Scan::parse(args)?;
        assert_eq!(scan.cursor, 0);
        assert!(scan.pattern.is_none());
        assert_eq!(scan.count, DEFAULT_SCAN_COUNT);
        Ok(())
    }

    #[test]
    fn test_parse_scan_with_options() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("123")),
            RespValue::BulkString(Bytes::from("MATCH")),
            RespValue::BulkString(Bytes::from("user:*")),
            RespValue::BulkString(Bytes::from("COUNT")),
            RespValue::BulkString(Bytes::from("100")),
        ];
        let scan = Scan::parse(args)?;
        assert_eq!(scan.cursor, 123);
        assert_eq!(scan.pattern, Some(Bytes::from("user:*")));
        assert_eq!(scan.count, 100);
        Ok(())
    }

    #[test]
    fn test_parse_scan_no_cursor() {
        assert!(Scan::parse(vec![]).is_err());
    }

    #[test]
    fn test_parse_dbsize() -> Result<()> {
        let dbsize = DbSize::parse(vec![])?;
        assert_eq!(dbsize.name(), "DBSIZE");
        Ok(())
    }

    #[test]
    fn test_parse_dbsize_wrong_args() {
        let args = vec![RespValue::BulkString(Bytes::from("extra"))];
        assert!(DbSize::parse(args).is_err());
    }

    #[test]
    fn test_parse_randomkey() -> Result<()> {
        let randomkey = RandomKey::parse(vec![])?;
        assert_eq!(randomkey.name(), "RANDOMKEY");
        Ok(())
    }

    #[test]
    fn test_parse_flushdb() -> Result<()> {
        let flushdb = FlushDb::parse(vec![])?;
        assert!(!flushdb.async_mode);

        let args = vec![RespValue::BulkString(Bytes::from("ASYNC"))];
        let flushdb = FlushDb::parse(args)?;
        assert!(flushdb.async_mode);

        let args = vec![RespValue::BulkString(Bytes::from("SYNC"))];
        let flushdb = FlushDb::parse(args)?;
        assert!(!flushdb.async_mode);

        Ok(())
    }
}
