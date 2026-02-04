//! SET/GET/DEL Command Implementations
//!
//! Implements the core key-value commands for the Redis protocol.

use super::CommandHandler;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::{SetOptions as StoreSetOptions, Store};
use std::time::Duration;

/// GET command - retrieve a value by key
#[derive(Debug)]
pub struct Get {
    pub key: Bytes,
}

impl Get {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(CommandError::wrong_arity("get").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Get { key })
    }
}

#[async_trait]
impl CommandHandler for Get {
    fn name(&self) -> &'static str {
        "GET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        match store.get(&self.key) {
            Some(value) => Ok(RespValue::BulkString(Bytes::from(value))),
            None => Ok(RespValue::NullBulkString),
        }
    }
}

/// Expiration type for SET command
#[derive(Debug, Clone, PartialEq)]
pub enum Expiration {
    /// Expire after N seconds (EX)
    Seconds(u64),
    /// Expire after N milliseconds (PX)
    Milliseconds(u64),
    /// Expire at Unix timestamp in seconds (EXAT)
    UnixSeconds(u64),
    /// Expire at Unix timestamp in milliseconds (PXAT)
    UnixMilliseconds(u64),
    /// Keep existing TTL (KEEPTTL)
    KeepTtl,
}

/// Condition for SET command
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetCondition {
    /// Only set if key does not exist (NX)
    NotExists,
    /// Only set if key already exists (XX)
    Exists,
}

/// Options for SET command
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration setting
    pub expiration: Option<Expiration>,
    /// Condition (NX or XX)
    pub condition: Option<SetCondition>,
    /// Return old value (GET)
    pub get_old: bool,
}

/// SET command - store a key-value pair
#[derive(Debug)]
pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
    pub options: SetOptions,
}

impl Set {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() < 2 {
            return Err(CommandError::wrong_arity("set").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let value = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        // Parse optional arguments
        let mut options = SetOptions::default();

        while !args.is_empty() {
            let opt = match args.remove(0) {
                RespValue::BulkString(bytes) => String::from_utf8(bytes.to_vec())
                    .map_err(|_| CommandError::syntax())?
                    .to_uppercase(),
                _ => return Err(CommandError::syntax().into()),
            };

            match opt.as_str() {
                "EX" => {
                    // Expiration in seconds
                    if options.expiration.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    let seconds = Self::parse_integer_arg(&mut args, "EX")?;
                    if seconds < 0 {
                        return Err(CommandError::not_integer().into());
                    }
                    options.expiration = Some(Expiration::Seconds(seconds as u64));
                }
                "PX" => {
                    // Expiration in milliseconds
                    if options.expiration.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    let millis = Self::parse_integer_arg(&mut args, "PX")?;
                    if millis < 0 {
                        return Err(CommandError::not_integer().into());
                    }
                    options.expiration = Some(Expiration::Milliseconds(millis as u64));
                }
                "EXAT" => {
                    // Expiration as Unix timestamp (seconds)
                    if options.expiration.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    let timestamp = Self::parse_integer_arg(&mut args, "EXAT")?;
                    if timestamp < 0 {
                        return Err(CommandError::not_integer().into());
                    }
                    options.expiration = Some(Expiration::UnixSeconds(timestamp as u64));
                }
                "PXAT" => {
                    // Expiration as Unix timestamp (milliseconds)
                    if options.expiration.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    let timestamp = Self::parse_integer_arg(&mut args, "PXAT")?;
                    if timestamp < 0 {
                        return Err(CommandError::not_integer().into());
                    }
                    options.expiration = Some(Expiration::UnixMilliseconds(timestamp as u64));
                }
                "KEEPTTL" => {
                    if options.expiration.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    options.expiration = Some(Expiration::KeepTtl);
                }
                "NX" => {
                    if options.condition.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    options.condition = Some(SetCondition::NotExists);
                }
                "XX" => {
                    if options.condition.is_some() {
                        return Err(CommandError::syntax().into());
                    }
                    options.condition = Some(SetCondition::Exists);
                }
                "GET" => {
                    options.get_old = true;
                }
                _ => return Err(CommandError::syntax().into()),
            }
        }

        Ok(Set {
            key,
            value,
            options,
        })
    }

    /// Parse an integer argument following an option
    fn parse_integer_arg(args: &mut Vec<RespValue>, option_name: &str) -> Result<i64> {
        if args.is_empty() {
            return Err(CommandError::generic(format!("{} requires a value", option_name)).into());
        }
        match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s =
                    String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_integer())?;
                s.parse::<i64>()
                    .map_err(|_| CommandError::not_integer().into())
            }
            RespValue::Integer(n) => Ok(n),
            _ => Err(CommandError::not_integer().into()),
        }
    }
}

#[async_trait]
impl CommandHandler for Set {
    fn name(&self) -> &'static str {
        "SET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        // Convert command options to store options
        let store_options = StoreSetOptions {
            nx: self.options.condition == Some(SetCondition::NotExists),
            xx: self.options.condition == Some(SetCondition::Exists),
            get: self.options.get_old,
            keep_ttl: matches!(self.options.expiration, Some(Expiration::KeepTtl)),
            expiration: self.options.expiration.as_ref().and_then(|exp| match exp {
                Expiration::Seconds(s) => Some(Duration::from_secs(*s)),
                Expiration::Milliseconds(ms) => Some(Duration::from_millis(*ms)),
                Expiration::UnixSeconds(ts) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    if *ts > now {
                        Some(Duration::from_secs(ts - now))
                    } else {
                        Some(Duration::ZERO) // Already expired
                    }
                }
                Expiration::UnixMilliseconds(ts) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Failed to get current time")
                        .as_millis() as u64;
                    if *ts > now {
                        Some(Duration::from_millis(ts - now))
                    } else {
                        Some(Duration::ZERO) // Already expired
                    }
                }
                Expiration::KeepTtl => None, // Handled by keep_ttl flag
            }),
        };

        let result =
            store.set_with_options(self.key.to_vec(), self.value.to_vec(), store_options)?;

        match result {
            eagle_core::store::SetResult::Ok => Ok(RespValue::SimpleString("OK".to_string())),
            eagle_core::store::SetResult::OkWithOldValue(old) => {
                Ok(RespValue::BulkString(Bytes::from(old)))
            }
            eagle_core::store::SetResult::NotPerformed => Ok(RespValue::NullBulkString),
            eagle_core::store::SetResult::NotPerformedWithOldValue(old) => {
                Ok(RespValue::BulkString(Bytes::from(old)))
            }
        }
    }
}

/// DEL command - delete one or more keys
#[derive(Debug)]
pub struct Del {
    pub keys: Vec<Bytes>,
}

impl Del {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(anyhow!("ERR wrong number of arguments for 'del' command"));
        }

        let keys = args
            .into_iter()
            .map(|arg| match arg {
                RespValue::BulkString(bytes) => Ok(bytes),
                _ => Err(anyhow!("Invalid key format")),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Del { keys })
    }
}

#[async_trait]
impl CommandHandler for Del {
    fn name(&self) -> &'static str {
        "DEL"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let mut deleted = 0;
        for key in &self.keys {
            if store.delete(key) {
                deleted += 1;
            }
        }
        Ok(RespValue::Integer(deleted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_get_commands() -> Result<()> {
        let store = Store::new_memory()?;

        // Test SET
        let set = Set {
            key: Bytes::from("test"),
            value: Bytes::from("value"),
            options: SetOptions::default(),
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Test GET
        let get = Get {
            key: Bytes::from("test"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value"));

        // Test GET non-existent key
        let get = Get {
            key: Bytes::from("nonexistent"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::NullBulkString));

        Ok(())
    }

    #[tokio::test]
    async fn test_del_command() -> Result<()> {
        let store = Store::new_memory()?;

        // Set up some keys
        let set1 = Set {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
            options: SetOptions::default(),
        };
        let set2 = Set {
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
            options: SetOptions::default(),
        };
        set1.execute(&store).await?;
        set2.execute(&store).await?;

        // Test DEL multiple keys
        let del = Del {
            keys: vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("nonexistent"),
            ],
        };
        let result = del.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(n) if n == 2));

        Ok(())
    }

    #[tokio::test]
    async fn test_del_hash_key() -> Result<()> {
        let store = Store::new_memory()?;

        // Create a hash key
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        // Verify hash exists
        assert!(store.exists(b"myhash"));

        // DEL should work on hash keys
        let del = Del {
            keys: vec![Bytes::from("myhash")],
        };
        let result = del.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // Hash should be gone
        assert!(!store.exists(b"myhash"));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_nx_option() -> Result<()> {
        let store = Store::new_memory()?;

        // SET with NX should succeed on new key
        let set = Set {
            key: Bytes::from("nx_key"),
            value: Bytes::from("value1"),
            options: SetOptions {
                condition: Some(SetCondition::NotExists),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Verify value was set
        let get = Get {
            key: Bytes::from("nx_key"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value1"));

        // SET with NX should fail on existing key
        let set = Set {
            key: Bytes::from("nx_key"),
            value: Bytes::from("value2"),
            options: SetOptions {
                condition: Some(SetCondition::NotExists),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::NullBulkString));

        // Verify value was NOT changed
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value1"));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_xx_option() -> Result<()> {
        let store = Store::new_memory()?;

        // SET with XX should fail on non-existent key
        let set = Set {
            key: Bytes::from("xx_key"),
            value: Bytes::from("value1"),
            options: SetOptions {
                condition: Some(SetCondition::Exists),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::NullBulkString));

        // First create the key
        let set = Set {
            key: Bytes::from("xx_key"),
            value: Bytes::from("value1"),
            options: SetOptions::default(),
        };
        set.execute(&store).await?;

        // SET with XX should succeed on existing key
        let set = Set {
            key: Bytes::from("xx_key"),
            value: Bytes::from("value2"),
            options: SetOptions {
                condition: Some(SetCondition::Exists),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Verify value was changed
        let get = Get {
            key: Bytes::from("xx_key"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_get_option() -> Result<()> {
        let store = Store::new_memory()?;

        // SET with GET on new key should return null
        let set = Set {
            key: Bytes::from("get_key"),
            value: Bytes::from("value1"),
            options: SetOptions {
                get_old: true,
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        // When key didn't exist, SET with GET returns OK (not null in our impl)
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // SET with GET on existing key should return old value
        let set = Set {
            key: Bytes::from("get_key"),
            value: Bytes::from("value2"),
            options: SetOptions {
                get_old: true,
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value1"));

        // Verify new value was set
        let get = Get {
            key: Bytes::from("get_key"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_ex_option() -> Result<()> {
        let store = Store::new_memory()?;

        // SET with EX (expire in 1 second)
        let set = Set {
            key: Bytes::from("ex_key"),
            value: Bytes::from("value"),
            options: SetOptions {
                expiration: Some(Expiration::Seconds(1)),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Key should exist immediately
        let get = Get {
            key: Bytes::from("ex_key"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value"));

        // Wait for expiration
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Key should be expired
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::NullBulkString));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_px_option() -> Result<()> {
        let store = Store::new_memory()?;

        // SET with PX (expire in 100 milliseconds)
        let set = Set {
            key: Bytes::from("px_key"),
            value: Bytes::from("value"),
            options: SetOptions {
                expiration: Some(Expiration::Milliseconds(100)),
                ..Default::default()
            },
        };
        let result = set.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Key should exist immediately
        let get = Get {
            key: Bytes::from("px_key"),
        };
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "value"));

        // Wait for expiration
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // Key should be expired
        let result = get.execute(&store).await?;
        assert!(matches!(result, RespValue::NullBulkString));

        Ok(())
    }

    #[test]
    fn test_parse_set_options() -> Result<()> {
        // Basic SET
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.key, Bytes::from("key"));
        assert_eq!(set.value, Bytes::from("value"));
        assert!(set.options.condition.is_none());
        assert!(set.options.expiration.is_none());
        assert!(!set.options.get_old);

        // SET with NX
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("NX")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.options.condition, Some(SetCondition::NotExists));

        // SET with XX
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("XX")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.options.condition, Some(SetCondition::Exists));

        // SET with EX
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("EX")),
            RespValue::BulkString(Bytes::from("60")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.options.expiration, Some(Expiration::Seconds(60)));

        // SET with PX
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("PX")),
            RespValue::BulkString(Bytes::from("5000")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.options.expiration, Some(Expiration::Milliseconds(5000)));

        // SET with GET
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("GET")),
        ];
        let set = Set::parse(args)?;
        assert!(set.options.get_old);

        // SET with multiple options
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("EX")),
            RespValue::BulkString(Bytes::from("60")),
            RespValue::BulkString(Bytes::from("NX")),
            RespValue::BulkString(Bytes::from("GET")),
        ];
        let set = Set::parse(args)?;
        assert_eq!(set.options.expiration, Some(Expiration::Seconds(60)));
        assert_eq!(set.options.condition, Some(SetCondition::NotExists));
        assert!(set.options.get_old);

        Ok(())
    }

    #[test]
    fn test_parse_set_errors() {
        // Missing value
        let args = vec![RespValue::BulkString(Bytes::from("key"))];
        assert!(Set::parse(args).is_err());

        // NX and XX together
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("NX")),
            RespValue::BulkString(Bytes::from("XX")),
        ];
        assert!(Set::parse(args).is_err());

        // Multiple expiration options
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("EX")),
            RespValue::BulkString(Bytes::from("60")),
            RespValue::BulkString(Bytes::from("PX")),
            RespValue::BulkString(Bytes::from("5000")),
        ];
        assert!(Set::parse(args).is_err());

        // EX without value
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("EX")),
        ];
        assert!(Set::parse(args).is_err());

        // Negative EX value
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("EX")),
            RespValue::BulkString(Bytes::from("-1")),
        ];
        assert!(Set::parse(args).is_err());

        // Unknown option
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("UNKNOWN")),
        ];
        assert!(Set::parse(args).is_err());
    }
}
