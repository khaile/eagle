//! TTL/Expiration Command Implementations
//!
//! Implements the Redis TTL-related commands:
//! - EXPIRE, PEXPIRE - Set TTL in seconds/milliseconds
//! - EXPIREAT, PEXPIREAT - Set expiration as Unix timestamp
//! - TTL, PTTL - Get remaining TTL
//! - PERSIST - Remove expiration

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;

/// EXPIRE command - Set a key's time to live in seconds
#[derive(Debug)]
pub struct Expire {
    pub key: Bytes,
    pub seconds: u64,
}

impl Expire {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("expire").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let seconds = parse_integer(&args.remove(0))?;

        Ok(Expire { key, seconds })
    }
}

#[async_trait]
impl CommandHandler for Expire {
    fn name(&self) -> &'static str {
        "EXPIRE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.expire(&self.key, self.seconds);
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }
}

/// PEXPIRE command - Set a key's time to live in milliseconds
#[derive(Debug)]
pub struct PExpire {
    pub key: Bytes,
    pub milliseconds: u64,
}

impl PExpire {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("pexpire").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let milliseconds = parse_integer(&args.remove(0))?;

        Ok(PExpire { key, milliseconds })
    }
}

#[async_trait]
impl CommandHandler for PExpire {
    fn name(&self) -> &'static str {
        "PEXPIRE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.pexpire(&self.key, self.milliseconds);
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }
}

/// EXPIREAT command - Set the expiration as a Unix timestamp (seconds)
#[derive(Debug)]
pub struct ExpireAt {
    pub key: Bytes,
    pub timestamp: u64,
}

impl ExpireAt {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("expireat").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let timestamp = parse_integer(&args.remove(0))?;

        Ok(ExpireAt { key, timestamp })
    }
}

#[async_trait]
impl CommandHandler for ExpireAt {
    fn name(&self) -> &'static str {
        "EXPIREAT"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.expireat(&self.key, self.timestamp);
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }
}

/// PEXPIREAT command - Set the expiration as a Unix timestamp (milliseconds)
#[derive(Debug)]
pub struct PExpireAt {
    pub key: Bytes,
    pub timestamp_ms: u64,
}

impl PExpireAt {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("pexpireat").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let timestamp_ms = parse_integer(&args.remove(0))?;

        Ok(PExpireAt { key, timestamp_ms })
    }
}

#[async_trait]
impl CommandHandler for PExpireAt {
    fn name(&self) -> &'static str {
        "PEXPIREAT"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.pexpireat(&self.key, self.timestamp_ms);
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }
}

/// TTL command - Get the time to live for a key in seconds
#[derive(Debug)]
pub struct Ttl {
    pub key: Bytes,
}

impl Ttl {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("ttl").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Ttl { key })
    }
}

#[async_trait]
impl CommandHandler for Ttl {
    fn name(&self) -> &'static str {
        "TTL"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let ttl = store.ttl(&self.key);
        Ok(RespValue::Integer(ttl))
    }
}

/// PTTL command - Get the time to live for a key in milliseconds
#[derive(Debug)]
pub struct PTtl {
    pub key: Bytes,
}

impl PTtl {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("pttl").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(PTtl { key })
    }
}

#[async_trait]
impl CommandHandler for PTtl {
    fn name(&self) -> &'static str {
        "PTTL"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let pttl = store.pttl(&self.key);
        Ok(RespValue::Integer(pttl))
    }
}

/// PERSIST command - Remove the expiration from a key
#[derive(Debug)]
pub struct Persist {
    pub key: Bytes,
}

impl Persist {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("persist").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Persist { key })
    }
}

#[async_trait]
impl CommandHandler for Persist {
    fn name(&self) -> &'static str {
        "PERSIST"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.persist(&self.key);
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }
}

/// Helper function to parse an integer from a RespValue
fn parse_integer(value: &RespValue) -> Result<u64> {
    match value {
        RespValue::Integer(n) => {
            if *n < 0 {
                Err(CommandError::not_integer().into())
            } else {
                Ok(*n as u64)
            }
        }
        RespValue::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes).map_err(|_| CommandError::not_integer())?;
            s.parse::<u64>()
                .map_err(|_| CommandError::not_integer().into())
        }
        _ => Err(CommandError::not_integer().into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_expire_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        let expire = Expire {
            key: Bytes::from("mykey"),
            seconds: 10,
        };
        let result = expire.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // Check TTL is set
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 0 && ttl <= 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_expire_nonexistent_key() -> Result<()> {
        let store = Store::new_memory()?;

        let expire = Expire {
            key: Bytes::from("nonexistent"),
            seconds: 10,
        };
        let result = expire.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_pexpire_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        let pexpire = PExpire {
            key: Bytes::from("mykey"),
            milliseconds: 5000,
        };
        let result = pexpire.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // Check PTTL is set
        let pttl = store.pttl(b"mykey");
        assert!(pttl > 0 && pttl <= 5000);

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // TTL should be -1 (no expiration)
        let ttl = Ttl {
            key: Bytes::from("mykey"),
        };
        let result = ttl.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(-1)));

        // Set expiration
        store.expire(b"mykey", 100);

        // TTL should now be positive
        let result = ttl.execute(&store).await?;
        match result {
            RespValue::Integer(n) => assert!(n > 0 && n <= 100),
            _ => panic!("Expected integer"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_nonexistent_key() -> Result<()> {
        let store = Store::new_memory()?;

        let ttl = Ttl {
            key: Bytes::from("nonexistent"),
        };
        let result = ttl.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(-2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_pttl_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration in milliseconds
        store.pexpire(b"mykey", 5000);

        let pttl = PTtl {
            key: Bytes::from("mykey"),
        };
        let result = pttl.execute(&store).await?;
        match result {
            RespValue::Integer(n) => assert!(n > 0 && n <= 5000),
            _ => panic!("Expected integer"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_persist_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;
        store.expire(b"mykey", 100);

        // Verify TTL is set
        assert!(store.ttl(b"mykey") > 0);

        let persist = Persist {
            key: Bytes::from("mykey"),
        };
        let result = persist.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // TTL should now be -1 (no expiration)
        assert_eq!(store.ttl(b"mykey"), -1);

        Ok(())
    }

    #[tokio::test]
    async fn test_persist_no_ttl() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        let persist = Persist {
            key: Bytes::from("mykey"),
        };
        let result = persist.execute(&store).await?;
        // Should return 0 since key had no TTL
        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_expireat_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration 100 seconds from now
        let future_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 100;

        let expireat = ExpireAt {
            key: Bytes::from("mykey"),
            timestamp: future_timestamp,
        };
        let result = expireat.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // TTL should be around 100 seconds
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 90 && ttl <= 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_pexpireat_command() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration 5000 ms from now
        let future_timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 5000;

        let pexpireat = PExpireAt {
            key: Bytes::from("mykey"),
            timestamp_ms: future_timestamp_ms,
        };
        let result = pexpireat.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // PTTL should be around 5000 ms
        let pttl = store.pttl(b"mykey");
        assert!(pttl > 4000 && pttl <= 5000);

        Ok(())
    }

    #[tokio::test]
    async fn test_key_expires() -> Result<()> {
        let store = Store::new_memory()?;

        // Set a key with a very short TTL
        store
            .set_with_expiry(
                b"shortlived".to_vec(),
                b"value".to_vec(),
                Some(std::time::Instant::now() + Duration::from_millis(10)),
            )
            .unwrap();

        // Key should exist initially
        assert!(store.get(b"shortlived").is_some());

        // Wait for it to expire
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Key should be gone (lazy expiration)
        assert!(store.get(b"shortlived").is_none());

        Ok(())
    }

    #[test]
    fn test_parse_ttl_too_many_args() {
        let result = Ttl::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_pttl_too_many_args() {
        let result = PTtl::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_persist_too_many_args() {
        let result = Persist::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_expire_too_many_args() {
        let result = Expire::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("10")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_pexpire_too_many_args() {
        let result = PExpire::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("1000")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_expireat_too_many_args() {
        let result = ExpireAt::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("1234567890")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }

    #[test]
    fn test_parse_pexpireat_too_many_args() {
        let result = PExpireAt::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("1234567890000")),
            RespValue::BulkString(Bytes::from("extra")),
        ]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong number of arguments")
        );
    }
}
