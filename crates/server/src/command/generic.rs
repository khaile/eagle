//! Generic Key Command Implementations
//!
//! Implements generic key commands for the Redis protocol:
//! - EXISTS: Check if keys exist
//! - TYPE: Get key type
//! - RENAME, RENAMENX: Rename keys

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;

/// EXISTS command - check if one or more keys exist
#[derive(Debug)]
pub struct Exists {
    pub keys: Vec<Bytes>,
}

impl Exists {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(CommandError::wrong_arity("exists").into());
        }

        let keys = args
            .into_iter()
            .map(|arg| match arg {
                RespValue::BulkString(bytes) => Ok(bytes),
                _ => Err(CommandError::syntax().into()),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Exists { keys })
    }
}

#[async_trait]
impl CommandHandler for Exists {
    fn name(&self) -> &'static str {
        "EXISTS"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let count: i64 = self.keys.iter().filter(|key| store.exists(key)).count() as i64;

        Ok(RespValue::Integer(count))
    }
}

/// TYPE command - get the type of a key
#[derive(Debug)]
pub struct Type {
    pub key: Bytes,
}

impl Type {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("type").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Type { key })
    }
}

#[async_trait]
impl CommandHandler for Type {
    fn name(&self) -> &'static str {
        "TYPE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let type_name = store.key_type(&self.key);
        Ok(RespValue::SimpleString(type_name))
    }
}

/// RENAME command - rename a key
#[derive(Debug)]
pub struct Rename {
    pub key: Bytes,
    pub newkey: Bytes,
}

impl Rename {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("rename").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let newkey = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Rename { key, newkey })
    }
}

#[async_trait]
impl CommandHandler for Rename {
    fn name(&self) -> &'static str {
        "RENAME"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        store.rename(&self.key, &self.newkey)?;
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

/// RENAMENX command - rename a key only if newkey does not exist
#[derive(Debug)]
pub struct RenameNx {
    pub key: Bytes,
    pub newkey: Bytes,
}

impl RenameNx {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("renamenx").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let newkey = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(RenameNx { key, newkey })
    }
}

#[async_trait]
impl CommandHandler for RenameNx {
    fn name(&self) -> &'static str {
        "RENAMENX"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let renamed = store.renamenx(&self.key, &self.newkey)?;
        Ok(RespValue::Integer(if renamed { 1 } else { 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_exists_single() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;

        let exists = Exists {
            keys: vec![Bytes::from("key1")],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        let exists = Exists {
            keys: vec![Bytes::from("nonexistent")],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_exists_multiple() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;

        let exists = Exists {
            keys: vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("key3"),
            ],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_exists_duplicate_keys() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;

        // Redis counts each key occurrence
        let exists = Exists {
            keys: vec![Bytes::from("key1"), Bytes::from("key1")],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_exists_hash() -> Result<()> {
        let store = Store::new_memory()?;
        store.hset(b"myhash", b"field", b"value".to_vec())?;

        let exists = Exists {
            keys: vec![Bytes::from("myhash")],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        // Non-existent key should still return 0
        let exists = Exists {
            keys: vec![Bytes::from("nonexistent")],
        };
        let result = exists.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_type_string() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mystring".to_vec(), b"hello".to_vec())?;

        let type_cmd = Type {
            key: Bytes::from("mystring"),
        };
        let result = type_cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "string"));

        Ok(())
    }

    #[tokio::test]
    async fn test_type_hash() -> Result<()> {
        let store = Store::new_memory()?;
        store.hset(b"myhash", b"field", b"value".to_vec())?;

        let type_cmd = Type {
            key: Bytes::from("myhash"),
        };
        let result = type_cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "hash"));

        Ok(())
    }

    #[tokio::test]
    async fn test_type_none() -> Result<()> {
        let store = Store::new_memory()?;

        let type_cmd = Type {
            key: Bytes::from("nonexistent"),
        };
        let result = type_cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "none"));

        Ok(())
    }

    #[tokio::test]
    async fn test_rename() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"oldkey".to_vec(), b"value".to_vec())?;

        let rename = Rename {
            key: Bytes::from("oldkey"),
            newkey: Bytes::from("newkey"),
        };
        let result = rename.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        // Old key should be gone
        assert!(!store.exists(b"oldkey"));
        // New key should exist with the value
        assert_eq!(store.get(b"newkey"), Some(b"value".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_rename_nonexistent() -> Result<()> {
        let store = Store::new_memory()?;

        let rename = Rename {
            key: Bytes::from("nonexistent"),
            newkey: Bytes::from("newkey"),
        };
        let result = rename.execute(&store).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_rename_overwrites_existing() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;

        let rename = Rename {
            key: Bytes::from("key1"),
            newkey: Bytes::from("key2"),
        };
        let result = rename.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        assert!(!store.exists(b"key1"));
        assert_eq!(store.get(b"key2"), Some(b"value1".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_renamenx_success() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"oldkey".to_vec(), b"value".to_vec())?;

        let renamenx = RenameNx {
            key: Bytes::from("oldkey"),
            newkey: Bytes::from("newkey"),
        };
        let result = renamenx.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        assert!(!store.exists(b"oldkey"));
        assert_eq!(store.get(b"newkey"), Some(b"value".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_renamenx_newkey_exists() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;

        let renamenx = RenameNx {
            key: Bytes::from("key1"),
            newkey: Bytes::from("key2"),
        };
        let result = renamenx.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(0)));

        // Neither key should have changed
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2"), Some(b"value2".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_renamenx_nonexistent_key() -> Result<()> {
        let store = Store::new_memory()?;

        let renamenx = RenameNx {
            key: Bytes::from("nonexistent"),
            newkey: Bytes::from("newkey"),
        };
        let result = renamenx.execute(&store).await;
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_parse_exists() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("key2")),
        ];
        let exists = Exists::parse(args)?;
        assert_eq!(exists.keys.len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_type() -> Result<()> {
        let args = vec![RespValue::BulkString(Bytes::from("mykey"))];
        let type_cmd = Type::parse(args)?;
        assert_eq!(type_cmd.key, Bytes::from("mykey"));
        Ok(())
    }

    #[test]
    fn test_parse_type_wrong_arg_count() {
        // No args
        assert!(Type::parse(vec![]).is_err());
        // Too many args
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(Type::parse(args).is_err());
    }

    #[test]
    fn test_parse_rename() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("oldkey")),
            RespValue::BulkString(Bytes::from("newkey")),
        ];
        let rename = Rename::parse(args)?;
        assert_eq!(rename.key, Bytes::from("oldkey"));
        assert_eq!(rename.newkey, Bytes::from("newkey"));
        Ok(())
    }

    #[test]
    fn test_parse_renamenx() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("oldkey")),
            RespValue::BulkString(Bytes::from("newkey")),
        ];
        let renamenx = RenameNx::parse(args)?;
        assert_eq!(renamenx.key, Bytes::from("oldkey"));
        assert_eq!(renamenx.newkey, Bytes::from("newkey"));
        Ok(())
    }

    #[test]
    fn test_parse_rename_missing_args() {
        let args = vec![RespValue::BulkString(Bytes::from("oldkey"))];
        assert!(Rename::parse(args).is_err());
    }

    #[test]
    fn test_parse_rename_too_many_args() {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("key2")),
            RespValue::BulkString(Bytes::from("key3")),
        ];
        assert!(Rename::parse(args).is_err());
    }

    #[test]
    fn test_parse_renamenx_missing_args() {
        let args = vec![RespValue::BulkString(Bytes::from("oldkey"))];
        assert!(RenameNx::parse(args).is_err());
    }

    #[test]
    fn test_parse_renamenx_too_many_args() {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("key2")),
            RespValue::BulkString(Bytes::from("key3")),
        ];
        assert!(RenameNx::parse(args).is_err());
    }
}
