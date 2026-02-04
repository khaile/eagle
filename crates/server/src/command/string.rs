//! String Command Implementations
//!
//! Implements string manipulation commands for the Redis protocol:
//! - MGET, MSET: Multi-key operations
//! - INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT: Numeric operations
//! - APPEND, STRLEN, GETRANGE, SETRANGE: String manipulation

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;

/// MGET command - get multiple keys at once
#[derive(Debug)]
pub struct Mget {
    pub keys: Vec<Bytes>,
}

impl Mget {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(CommandError::wrong_arity("mget").into());
        }

        let keys = args
            .into_iter()
            .map(|arg| match arg {
                RespValue::BulkString(bytes) => Ok(bytes),
                _ => Err(CommandError::syntax().into()),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Mget { keys })
    }
}

#[async_trait]
impl CommandHandler for Mget {
    fn name(&self) -> &'static str {
        "MGET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let values: Vec<RespValue> = self
            .keys
            .iter()
            .map(|key| match store.get(key) {
                Some(value) => RespValue::BulkString(Bytes::from(value)),
                None => RespValue::NullBulkString,
            })
            .collect();

        Ok(RespValue::Array(values.into()))
    }
}

/// MSET command - set multiple key-value pairs at once
#[derive(Debug)]
pub struct Mset {
    pub pairs: Vec<(Bytes, Bytes)>,
}

impl Mset {
    pub fn parse(args: Vec<RespValue>) -> Result<Self> {
        if args.is_empty() {
            return Err(CommandError::wrong_arity("mset").into());
        }

        if !args.len().is_multiple_of(2) {
            return Err(CommandError::wrong_arity("mset").into());
        }

        let mut pairs = Vec::with_capacity(args.len() / 2);
        let mut iter = args.into_iter();

        while let Some(key) = iter.next() {
            let key = match key {
                RespValue::BulkString(bytes) => bytes,
                _ => return Err(CommandError::syntax().into()),
            };

            let value = match iter.next() {
                Some(RespValue::BulkString(bytes)) => bytes,
                _ => return Err(CommandError::syntax().into()),
            };

            pairs.push((key, value));
        }

        Ok(Mset { pairs })
    }
}

#[async_trait]
impl CommandHandler for Mset {
    fn name(&self) -> &'static str {
        "MSET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        // Convert pairs to format expected by store.mset
        let pairs: Vec<_> = self
            .pairs
            .iter()
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
            .collect();
        store.mset(&pairs);
        Ok(RespValue::SimpleString("OK".to_string()))
    }
}

/// INCR command - increment integer value by 1
#[derive(Debug)]
pub struct Incr {
    pub key: Bytes,
}

impl Incr {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("incr").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Incr { key })
    }
}

#[async_trait]
impl CommandHandler for Incr {
    fn name(&self) -> &'static str {
        "INCR"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.incrby(&self.key, 1)?;
        Ok(RespValue::Integer(result))
    }
}

/// DECR command - decrement integer value by 1
#[derive(Debug)]
pub struct Decr {
    pub key: Bytes,
}

impl Decr {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("decr").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Decr { key })
    }
}

#[async_trait]
impl CommandHandler for Decr {
    fn name(&self) -> &'static str {
        "DECR"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.decrby(&self.key, 1)?;
        Ok(RespValue::Integer(result))
    }
}

/// INCRBY command - increment integer value by specified amount
#[derive(Debug)]
pub struct IncrBy {
    pub key: Bytes,
    pub increment: i64,
}

impl IncrBy {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("incrby").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let increment = match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s =
                    String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_integer())?;
                s.parse::<i64>().map_err(|_| CommandError::not_integer())?
            }
            RespValue::Integer(n) => n,
            _ => return Err(CommandError::not_integer().into()),
        };

        Ok(IncrBy { key, increment })
    }
}

#[async_trait]
impl CommandHandler for IncrBy {
    fn name(&self) -> &'static str {
        "INCRBY"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.incrby(&self.key, self.increment)?;
        Ok(RespValue::Integer(result))
    }
}

/// DECRBY command - decrement integer value by specified amount
#[derive(Debug)]
pub struct DecrBy {
    pub key: Bytes,
    pub decrement: i64,
}

impl DecrBy {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("decrby").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let decrement = match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s =
                    String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_integer())?;
                s.parse::<i64>().map_err(|_| CommandError::not_integer())?
            }
            RespValue::Integer(n) => n,
            _ => return Err(CommandError::not_integer().into()),
        };

        Ok(DecrBy { key, decrement })
    }
}

#[async_trait]
impl CommandHandler for DecrBy {
    fn name(&self) -> &'static str {
        "DECRBY"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.decrby(&self.key, self.decrement)?;
        Ok(RespValue::Integer(result))
    }
}

/// INCRBYFLOAT command - increment float value by specified amount
#[derive(Debug)]
pub struct IncrByFloat {
    pub key: Bytes,
    pub increment: f64,
}

impl IncrByFloat {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("incrbyfloat").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let increment = match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s = String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_float())?;
                s.parse::<f64>().map_err(|_| CommandError::not_float())?
            }
            _ => return Err(CommandError::not_float().into()),
        };

        if increment.is_nan() || increment.is_infinite() {
            return Err(CommandError::nan_or_infinity().into());
        }

        Ok(IncrByFloat { key, increment })
    }
}

#[async_trait]
impl CommandHandler for IncrByFloat {
    fn name(&self) -> &'static str {
        "INCRBYFLOAT"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.incrbyfloat(&self.key, self.increment)?;
        // Redis returns the result as a bulk string
        Ok(RespValue::BulkString(Bytes::from(result.to_string())))
    }
}

/// APPEND command - append value to existing string
#[derive(Debug)]
pub struct Append {
    pub key: Bytes,
    pub value: Bytes,
}

impl Append {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("append").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let value = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Append { key, value })
    }
}

#[async_trait]
impl CommandHandler for Append {
    fn name(&self) -> &'static str {
        "APPEND"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let new_len = store.append(&self.key, &self.value)?;
        Ok(RespValue::Integer(new_len as i64))
    }
}

/// STRLEN command - get string length
#[derive(Debug)]
pub struct StrLen {
    pub key: Bytes,
}

impl StrLen {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("strlen").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(StrLen { key })
    }
}

#[async_trait]
impl CommandHandler for StrLen {
    fn name(&self) -> &'static str {
        "STRLEN"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let len = store.strlen(&self.key);
        Ok(RespValue::Integer(len as i64))
    }
}

/// GETRANGE command - get substring of string
#[derive(Debug)]
pub struct GetRange {
    pub key: Bytes,
    pub start: i64,
    pub end: i64,
}

impl GetRange {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("getrange").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let start = Self::parse_index(&mut args)?;
        let end = Self::parse_index(&mut args)?;

        Ok(GetRange { key, start, end })
    }

    fn parse_index(args: &mut Vec<RespValue>) -> Result<i64> {
        match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s =
                    String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_integer())?;
                Ok(s.parse::<i64>().map_err(|_| CommandError::not_integer())?)
            }
            RespValue::Integer(n) => Ok(n),
            _ => Err(CommandError::not_integer().into()),
        }
    }
}

#[async_trait]
impl CommandHandler for GetRange {
    fn name(&self) -> &'static str {
        "GETRANGE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.getrange(&self.key, self.start, self.end);
        Ok(RespValue::BulkString(Bytes::from(result)))
    }
}

/// SETRANGE command - overwrite part of string at offset
#[derive(Debug)]
pub struct SetRange {
    pub key: Bytes,
    pub offset: i64,
    pub value: Bytes,
}

impl SetRange {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("setrange").into());
        }

        let key = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        let offset = match args.remove(0) {
            RespValue::BulkString(bytes) => {
                let s =
                    String::from_utf8(bytes.to_vec()).map_err(|_| CommandError::not_integer())?;
                s.parse::<i64>().map_err(|_| CommandError::not_integer())?
            }
            RespValue::Integer(n) => n,
            _ => return Err(CommandError::not_integer().into()),
        };

        if offset < 0 {
            return Err(CommandError::offset_out_of_range().into());
        }

        let value = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(SetRange { key, offset, value })
    }
}

#[async_trait]
impl CommandHandler for SetRange {
    fn name(&self) -> &'static str {
        "SETRANGE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let offset =
            usize::try_from(self.offset).map_err(|_| CommandError::offset_out_of_range())?;
        let new_len = store.setrange(&self.key, offset, &self.value)?;
        Ok(RespValue::Integer(new_len as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mget() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;

        let mget = Mget {
            keys: vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("key3"),
            ],
        };
        let result = mget.execute(&store).await?;

        match result {
            RespValue::Array(values) => {
                assert_eq!(values.len(), 3);
                let values: Vec<_> = values.into_iter().collect();
                assert!(matches!(&values[0], RespValue::BulkString(b) if b == "value1"));
                assert!(matches!(&values[1], RespValue::BulkString(b) if b == "value2"));
                assert!(matches!(&values[2], RespValue::NullBulkString));
            }
            _ => panic!("Expected Array"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mset() -> Result<()> {
        let store = Store::new_memory()?;

        let mset = Mset {
            pairs: vec![
                (Bytes::from("key1"), Bytes::from("value1")),
                (Bytes::from("key2"), Bytes::from("value2")),
            ],
        };
        let result = mset.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2"), Some(b"value2".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_incr_decr() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"counter".to_vec(), b"10".to_vec())?;

        // INCR
        let incr = Incr {
            key: Bytes::from("counter"),
        };
        let result = incr.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(11)));

        // DECR
        let decr = Decr {
            key: Bytes::from("counter"),
        };
        let result = decr.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(10)));

        Ok(())
    }

    #[tokio::test]
    async fn test_incr_nonexistent() -> Result<()> {
        let store = Store::new_memory()?;

        let incr = Incr {
            key: Bytes::from("newcounter"),
        };
        let result = incr.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(1)));

        Ok(())
    }

    #[tokio::test]
    async fn test_incrby_decrby() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"counter".to_vec(), b"10".to_vec())?;

        // INCRBY
        let incrby = IncrBy {
            key: Bytes::from("counter"),
            increment: 5,
        };
        let result = incrby.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(15)));

        // DECRBY
        let decrby = DecrBy {
            key: Bytes::from("counter"),
            decrement: 3,
        };
        let result = decrby.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(12)));

        Ok(())
    }

    #[tokio::test]
    async fn test_incrbyfloat() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"myfloat".to_vec(), b"10.5".to_vec())?;

        let incrbyfloat = IncrByFloat {
            key: Bytes::from("myfloat"),
            increment: 0.1,
        };
        let result = incrbyfloat.execute(&store).await?;

        match result {
            RespValue::BulkString(bytes) => {
                let val: f64 = String::from_utf8(bytes.to_vec())?.parse()?;
                assert!((val - 10.6).abs() < 0.0001);
            }
            _ => panic!("Expected BulkString"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_append() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello".to_vec())?;

        let append = Append {
            key: Bytes::from("mykey"),
            value: Bytes::from(" World"),
        };
        let result = append.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(11)));

        assert_eq!(store.get(b"mykey"), Some(b"Hello World".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_nonexistent() -> Result<()> {
        let store = Store::new_memory()?;

        let append = Append {
            key: Bytes::from("newkey"),
            value: Bytes::from("Hello"),
        };
        let result = append.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(5)));

        assert_eq!(store.get(b"newkey"), Some(b"Hello".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_strlen() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;

        let strlen = StrLen {
            key: Bytes::from("mykey"),
        };
        let result = strlen.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(11)));

        // Non-existent key
        let strlen = StrLen {
            key: Bytes::from("nonexistent"),
        };
        let result = strlen.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_getrange() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;

        // Positive indices
        let getrange = GetRange {
            key: Bytes::from("mykey"),
            start: 0,
            end: 4,
        };
        let result = getrange.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "Hello"));

        // Negative indices
        let getrange = GetRange {
            key: Bytes::from("mykey"),
            start: -5,
            end: -1,
        };
        let result = getrange.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "World"));

        Ok(())
    }

    #[tokio::test]
    async fn test_setrange() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;

        let setrange = SetRange {
            key: Bytes::from("mykey"),
            offset: 6,
            value: Bytes::from("Redis"),
        };
        let result = setrange.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(11)));

        assert_eq!(store.get(b"mykey"), Some(b"Hello Redis".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_setrange_extends_string() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello".to_vec())?;

        let setrange = SetRange {
            key: Bytes::from("mykey"),
            offset: 10,
            value: Bytes::from("World"),
        };
        let result = setrange.execute(&store).await?;
        assert!(matches!(result, RespValue::Integer(15)));

        let value = store.get(b"mykey").unwrap();
        assert_eq!(value.len(), 15);
        assert_eq!(&value[0..5], b"Hello");
        assert_eq!(&value[10..15], b"World");

        Ok(())
    }

    #[test]
    fn test_parse_mget() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("key2")),
        ];
        let mget = Mget::parse(args)?;
        assert_eq!(mget.keys.len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_mset() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("value1")),
            RespValue::BulkString(Bytes::from("key2")),
            RespValue::BulkString(Bytes::from("value2")),
        ];
        let mset = Mset::parse(args)?;
        assert_eq!(mset.pairs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_parse_mset_odd_args() {
        let args = vec![
            RespValue::BulkString(Bytes::from("key1")),
            RespValue::BulkString(Bytes::from("value1")),
            RespValue::BulkString(Bytes::from("key2")),
        ];
        assert!(Mset::parse(args).is_err());
    }

    #[test]
    fn test_parse_incrby() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("counter")),
            RespValue::BulkString(Bytes::from("5")),
        ];
        let incrby = IncrBy::parse(args)?;
        assert_eq!(incrby.key, Bytes::from("counter"));
        assert_eq!(incrby.increment, 5);
        Ok(())
    }

    #[test]
    fn test_parse_incrbyfloat() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("myfloat")),
            RespValue::BulkString(Bytes::from("0.5")),
        ];
        let incrbyfloat = IncrByFloat::parse(args)?;
        assert_eq!(incrbyfloat.key, Bytes::from("myfloat"));
        assert!((incrbyfloat.increment - 0.5).abs() < 0.0001);
        Ok(())
    }

    #[test]
    fn test_parse_getrange() -> Result<()> {
        let args = vec![
            RespValue::BulkString(Bytes::from("mykey")),
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("-1")),
        ];
        let getrange = GetRange::parse(args)?;
        assert_eq!(getrange.key, Bytes::from("mykey"));
        assert_eq!(getrange.start, 0);
        assert_eq!(getrange.end, -1);
        Ok(())
    }

    #[test]
    fn test_parse_setrange_negative_offset() {
        let args = vec![
            RespValue::BulkString(Bytes::from("mykey")),
            RespValue::BulkString(Bytes::from("-1")),
            RespValue::BulkString(Bytes::from("value")),
        ];
        assert!(SetRange::parse(args).is_err());
    }

    // Tests for exact argument count validation
    #[test]
    fn test_parse_incr_wrong_arg_count() {
        // No args
        assert!(Incr::parse(vec![]).is_err());
        // Too many args
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(Incr::parse(args).is_err());
    }

    #[test]
    fn test_parse_decr_wrong_arg_count() {
        assert!(Decr::parse(vec![]).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(Decr::parse(args).is_err());
    }

    #[test]
    fn test_parse_strlen_wrong_arg_count() {
        assert!(StrLen::parse(vec![]).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(StrLen::parse(args).is_err());
    }

    #[test]
    fn test_parse_incrby_wrong_arg_count() {
        // Too few
        let args = vec![RespValue::BulkString(Bytes::from("key"))];
        assert!(IncrBy::parse(args).is_err());
        // Too many
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("5")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(IncrBy::parse(args).is_err());
    }

    #[test]
    fn test_parse_decrby_wrong_arg_count() {
        let args = vec![RespValue::BulkString(Bytes::from("key"))];
        assert!(DecrBy::parse(args).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("5")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(DecrBy::parse(args).is_err());
    }

    #[test]
    fn test_parse_incrbyfloat_wrong_arg_count() {
        let args = vec![RespValue::BulkString(Bytes::from("key"))];
        assert!(IncrByFloat::parse(args).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("0.5")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(IncrByFloat::parse(args).is_err());
    }

    #[test]
    fn test_parse_append_wrong_arg_count() {
        let args = vec![RespValue::BulkString(Bytes::from("key"))];
        assert!(Append::parse(args).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(Append::parse(args).is_err());
    }

    #[test]
    fn test_parse_getrange_wrong_arg_count() {
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("0")),
        ];
        assert!(GetRange::parse(args).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("10")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(GetRange::parse(args).is_err());
    }

    #[test]
    fn test_parse_setrange_wrong_arg_count() {
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("0")),
        ];
        assert!(SetRange::parse(args).is_err());
        let args = vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("value")),
            RespValue::BulkString(Bytes::from("extra")),
        ];
        assert!(SetRange::parse(args).is_err());
    }
}
