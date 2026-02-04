use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;
use std::collections::VecDeque;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct HSet {
    key: Bytes,
    field: Bytes,
    value: Bytes,
}

impl HSet {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("hset").into());
        }

        let value = args.pop().unwrap().into_bytes()?;
        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HSet { key, field, value })
    }
}

#[async_trait]
impl CommandHandler for HSet {
    fn name(&self) -> &'static str {
        "HSET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let is_new = store.hset(&self.key, &self.field, self.value.to_vec())?;
        Ok(RespValue::Integer(if is_new { 1 } else { 0 }))
    }
}

#[derive(Debug)]
pub struct HGet {
    key: Bytes,
    field: Bytes,
}

impl HGet {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("hget").into());
        }

        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HGet { key, field })
    }
}

#[async_trait]
impl CommandHandler for HGet {
    fn name(&self) -> &'static str {
        "HGET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        match store.hget(&self.key, &self.field)? {
            Some(value) => Ok(RespValue::BulkString(value.into())),
            None => Ok(RespValue::Null),
        }
    }
}

#[derive(Debug)]
pub struct HDel {
    key: Bytes,
    field: Bytes,
}

impl HDel {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("hdel").into());
        }

        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HDel { key, field })
    }
}

#[async_trait]
impl CommandHandler for HDel {
    fn name(&self) -> &'static str {
        "HDEL"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let deleted = store.hdel(&self.key, &self.field)?;
        Ok(RespValue::Integer(if deleted { 1 } else { 0 }))
    }
}

// ========== Milestone 1.5: Complete Hash Commands ==========

#[derive(Debug)]
pub struct HGetAll {
    key: Bytes,
}

impl HGetAll {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("hgetall").into());
        }

        let key = args.pop().unwrap().into_bytes()?;
        Ok(HGetAll { key })
    }
}

#[async_trait]
impl CommandHandler for HGetAll {
    fn name(&self) -> &'static str {
        "HGETALL"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let len = store.hlen(&self.key).saturating_mul(2);
        let (tx, rx) = mpsc::channel(100);
        let store = store.clone();
        let key = self.key.clone();

        tokio::task::spawn_blocking(move || {
            let _ = store.hfor_each(&key, |field, value| {
                if tx
                    .blocking_send(Ok(RespValue::BulkString(field.to_vec().into())))
                    .is_err()
                {
                    return;
                }
                let _ = tx.blocking_send(Ok(RespValue::BulkString(value.to_vec().into())));
            });
        });
        Ok(RespValue::StreamArray(len, rx))
    }
}

#[derive(Debug)]
pub struct HMSet {
    key: Bytes,
    pairs: Vec<(Bytes, Bytes)>,
}

impl HMSet {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            return Err(CommandError::wrong_arity("hmset").into());
        }

        let key = args.remove(0).into_bytes()?;
        let mut pairs = Vec::with_capacity(args.len() / 2);

        while args.len() >= 2 {
            let field = args.remove(0).into_bytes()?;
            let value = args.remove(0).into_bytes()?;
            pairs.push((field, value));
        }

        Ok(HMSet { key, pairs })
    }
}

#[async_trait]
impl CommandHandler for HMSet {
    fn name(&self) -> &'static str {
        "HMSET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let pairs: Vec<(&[u8], Vec<u8>)> = self
            .pairs
            .iter()
            .map(|(f, v)| (f.as_ref(), v.to_vec()))
            .collect();
        store.hmset(&self.key, &pairs)?;
        Ok(RespValue::SimpleString("OK".into()))
    }
}

#[derive(Debug)]
pub struct HMGet {
    key: Bytes,
    fields: Vec<Bytes>,
}

impl HMGet {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() < 2 {
            return Err(CommandError::wrong_arity("hmget").into());
        }

        let key = args.remove(0).into_bytes()?;
        let mut fields = Vec::with_capacity(args.len());

        for arg in args {
            fields.push(arg.into_bytes()?);
        }

        Ok(HMGet { key, fields })
    }
}

#[async_trait]
impl CommandHandler for HMGet {
    fn name(&self) -> &'static str {
        "HMGET"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let field_refs: Vec<&[u8]> = self.fields.iter().map(|f| f.as_ref()).collect();
        let values = store.hmget(&self.key, &field_refs)?;

        let result: VecDeque<RespValue> = values
            .into_iter()
            .map(|v| match v {
                Some(val) => RespValue::BulkString(val.into()),
                None => RespValue::Null,
            })
            .collect();

        Ok(RespValue::Array(result))
    }
}

#[derive(Debug)]
pub struct HLen {
    key: Bytes,
}

impl HLen {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("hlen").into());
        }

        let key = args.pop().unwrap().into_bytes()?;
        Ok(HLen { key })
    }
}

#[async_trait]
impl CommandHandler for HLen {
    fn name(&self) -> &'static str {
        "HLEN"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let len = store.hlen(&self.key);
        Ok(RespValue::Integer(len as i64))
    }
}

#[derive(Debug)]
pub struct HExists {
    key: Bytes,
    field: Bytes,
}

impl HExists {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("hexists").into());
        }

        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HExists { key, field })
    }
}

#[async_trait]
impl CommandHandler for HExists {
    fn name(&self) -> &'static str {
        "HEXISTS"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let exists = store.hexists(&self.key, &self.field);
        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
    }
}

#[derive(Debug)]
pub struct HKeys {
    key: Bytes,
}

impl HKeys {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("hkeys").into());
        }

        let key = args.pop().unwrap().into_bytes()?;
        Ok(HKeys { key })
    }
}

#[async_trait]
impl CommandHandler for HKeys {
    fn name(&self) -> &'static str {
        "HKEYS"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let len = store.hlen(&self.key);
        let (tx, rx) = mpsc::channel(100);
        let store = store.clone();
        let key = self.key.clone();

        tokio::task::spawn_blocking(move || {
            let _ = store.hfor_each(&key, |field, _value| {
                let _ = tx.blocking_send(Ok(RespValue::BulkString(field.to_vec().into())));
            });
        });
        Ok(RespValue::StreamArray(len, rx))
    }
}

#[derive(Debug)]
pub struct HVals {
    key: Bytes,
}

impl HVals {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("hvals").into());
        }

        let key = args.pop().unwrap().into_bytes()?;
        Ok(HVals { key })
    }
}

#[async_trait]
impl CommandHandler for HVals {
    fn name(&self) -> &'static str {
        "HVALS"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let len = store.hlen(&self.key);
        let (tx, rx) = mpsc::channel(100);
        let store = store.clone();
        let key = self.key.clone();

        tokio::task::spawn_blocking(move || {
            let _ = store.hfor_each(&key, |_field, value| {
                let _ = tx.blocking_send(Ok(RespValue::BulkString(value.to_vec().into())));
            });
        });
        Ok(RespValue::StreamArray(len, rx))
    }
}

#[derive(Debug)]
pub struct HIncrBy {
    key: Bytes,
    field: Bytes,
    increment: i64,
}

impl HIncrBy {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("hincrby").into());
        }

        let increment_bytes = args.pop().unwrap().into_bytes()?;
        let increment_str =
            std::str::from_utf8(&increment_bytes).map_err(|_| CommandError::not_integer())?;
        let increment = increment_str
            .parse::<i64>()
            .map_err(|_| CommandError::not_integer())?;

        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HIncrBy {
            key,
            field,
            increment,
        })
    }
}

#[async_trait]
impl CommandHandler for HIncrBy {
    fn name(&self) -> &'static str {
        "HINCRBY"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.hincrby(&self.key, &self.field, self.increment)?;
        Ok(RespValue::Integer(result))
    }
}

#[derive(Debug)]
pub struct HIncrByFloat {
    key: Bytes,
    field: Bytes,
    increment: f64,
}

impl HIncrByFloat {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("hincrbyfloat").into());
        }

        let increment_bytes = args.pop().unwrap().into_bytes()?;
        let increment_str =
            std::str::from_utf8(&increment_bytes).map_err(|_| CommandError::not_float())?;
        let increment = increment_str
            .parse::<f64>()
            .map_err(|_| CommandError::not_float())?;

        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HIncrByFloat {
            key,
            field,
            increment,
        })
    }
}

#[async_trait]
impl CommandHandler for HIncrByFloat {
    fn name(&self) -> &'static str {
        "HINCRBYFLOAT"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let result = store.hincrbyfloat(&self.key, &self.field, self.increment)?;
        Ok(RespValue::BulkString(result.to_string().into()))
    }
}

#[derive(Debug)]
pub struct HSetNx {
    key: Bytes,
    field: Bytes,
    value: Bytes,
}

impl HSetNx {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 3 {
            return Err(CommandError::wrong_arity("hsetnx").into());
        }

        let value = args.pop().unwrap().into_bytes()?;
        let field = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;

        Ok(HSetNx { key, field, value })
    }
}

#[async_trait]
impl CommandHandler for HSetNx {
    fn name(&self) -> &'static str {
        "HSETNX"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let was_set = store.hsetnx(&self.key, &self.field, self.value.to_vec())?;
        Ok(RespValue::Integer(if was_set { 1 } else { 0 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eagle_core::store::Store;

    async fn collect_stream(resp: RespValue) -> RespValue {
        match resp {
            RespValue::StreamArray(_len, mut rx) => {
                let mut arr = VecDeque::new();
                while let Some(item) = rx.recv().await {
                    if let Ok(val) = item {
                        arr.push_back(val);
                    }
                }
                RespValue::Array(arr)
            }
            _ => resp,
        }
    }

    #[tokio::test]
    async fn test_hash_commands() -> Result<()> {
        let store = Store::new()?;

        // Test HSET - new field returns 1
        let hset = HSet::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("value")),
        ])?;

        assert_eq!(hset.execute(&store).await?, RespValue::Integer(1));

        // Test HSET - existing field returns 0
        let hset_update = HSet::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("new_value")),
        ])?;

        assert_eq!(hset_update.execute(&store).await?, RespValue::Integer(0));

        // Test HGET
        let hget = HGet::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("field")),
        ])?;

        assert_eq!(
            hget.execute(&store).await?,
            RespValue::BulkString(Bytes::from("new_value"))
        );

        // Test HDEL - existing field returns 1
        let hdel = HDel::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("field")),
        ])?;

        assert_eq!(hdel.execute(&store).await?, RespValue::Integer(1));

        // Verify field was deleted
        assert_eq!(hget.execute(&store).await?, RespValue::Null);

        // Test HDEL - non-existent field returns 0
        assert_eq!(hdel.execute(&store).await?, RespValue::Integer(0));

        Ok(())
    }

    #[tokio::test]
    async fn test_hdel_nonexistent_key() -> Result<()> {
        let store = Store::new()?;

        let hdel = HDel::parse(vec![
            RespValue::BulkString(Bytes::from("nonexistent")),
            RespValue::BulkString(Bytes::from("field")),
        ])?;

        // Deleting from non-existent key returns 0
        assert_eq!(hdel.execute(&store).await?, RespValue::Integer(0));

        Ok(())
    }

    #[tokio::test]
    async fn test_hget_nonexistent() -> Result<()> {
        let store = Store::new()?;

        // Non-existent key
        let hget = HGet::parse(vec![
            RespValue::BulkString(Bytes::from("nonexistent")),
            RespValue::BulkString(Bytes::from("field")),
        ])?;

        assert_eq!(hget.execute(&store).await?, RespValue::Null);

        // Existing key, non-existent field
        store.hset(b"key", b"field", b"value".to_vec())?;

        let hget2 = HGet::parse(vec![
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("other")),
        ])?;

        assert_eq!(hget2.execute(&store).await?, RespValue::Null);

        Ok(())
    }

    // ========== Milestone 1.5: Complete Hash Commands Tests ==========

    #[tokio::test]
    async fn test_hgetall() -> Result<()> {
        let store = Store::new()?;

        // Empty hash returns empty array
        let hgetall = HGetAll::parse(vec![RespValue::BulkString(Bytes::from("nonexistent"))])?;
        let result = collect_stream(hgetall.execute(&store).await?).await;
        assert!(matches!(result, RespValue::Array(arr) if arr.is_empty()));

        // Add some fields
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let hgetall = HGetAll::parse(vec![RespValue::BulkString(Bytes::from("myhash"))])?;
        let result = collect_stream(hgetall.execute(&store).await?).await;

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4); // 2 fields * 2 (field + value)
        } else {
            panic!("Expected Array response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hmset() -> Result<()> {
        let store = Store::new()?;

        let hmset = HMSet::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field1")),
            RespValue::BulkString(Bytes::from("value1")),
            RespValue::BulkString(Bytes::from("field2")),
            RespValue::BulkString(Bytes::from("value2")),
        ])?;

        assert_eq!(
            hmset.execute(&store).await?,
            RespValue::SimpleString("OK".into())
        );

        // Verify fields were set
        assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"value1".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field2")?, Some(b"value2".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_hmget() -> Result<()> {
        let store = Store::new()?;

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let hmget = HMGet::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field1")),
            RespValue::BulkString(Bytes::from("field2")),
            RespValue::BulkString(Bytes::from("field3")), // non-existent
        ])?;

        let result = hmget.execute(&store).await?;
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("value1")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("value2")));
            assert_eq!(arr[2], RespValue::Null);
        } else {
            panic!("Expected Array response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hmget_nonexistent_key() -> Result<()> {
        let store = Store::new()?;

        let hmget = HMGet::parse(vec![
            RespValue::BulkString(Bytes::from("nonexistent")),
            RespValue::BulkString(Bytes::from("field1")),
            RespValue::BulkString(Bytes::from("field2")),
        ])?;

        let result = hmget.execute(&store).await?;
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::Null);
            assert_eq!(arr[1], RespValue::Null);
        } else {
            panic!("Expected Array response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hlen() -> Result<()> {
        let store = Store::new()?;

        // Empty/non-existent hash returns 0
        let hlen = HLen::parse(vec![RespValue::BulkString(Bytes::from("nonexistent"))])?;
        assert_eq!(hlen.execute(&store).await?, RespValue::Integer(0));

        // Add some fields
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;
        store.hset(b"myhash", b"field3", b"value3".to_vec())?;

        let hlen = HLen::parse(vec![RespValue::BulkString(Bytes::from("myhash"))])?;
        assert_eq!(hlen.execute(&store).await?, RespValue::Integer(3));

        Ok(())
    }

    #[tokio::test]
    async fn test_hexists() -> Result<()> {
        let store = Store::new()?;

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;

        // Existing field returns 1
        let hexists = HExists::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field1")),
        ])?;
        assert_eq!(hexists.execute(&store).await?, RespValue::Integer(1));

        // Non-existent field returns 0
        let hexists_no = HExists::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field2")),
        ])?;
        assert_eq!(hexists_no.execute(&store).await?, RespValue::Integer(0));

        // Non-existent key returns 0
        let hexists_nokey = HExists::parse(vec![
            RespValue::BulkString(Bytes::from("nonexistent")),
            RespValue::BulkString(Bytes::from("field")),
        ])?;
        assert_eq!(hexists_nokey.execute(&store).await?, RespValue::Integer(0));

        Ok(())
    }

    #[tokio::test]
    async fn test_hkeys() -> Result<()> {
        let store = Store::new()?;

        // Empty/non-existent hash returns empty array
        let hkeys = HKeys::parse(vec![RespValue::BulkString(Bytes::from("nonexistent"))])?;
        let result = collect_stream(hkeys.execute(&store).await?).await;
        assert!(matches!(result, RespValue::Array(arr) if arr.is_empty()));

        // Add some fields
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let hkeys = HKeys::parse(vec![RespValue::BulkString(Bytes::from("myhash"))])?;
        let result = collect_stream(hkeys.execute(&store).await?).await;
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hvals() -> Result<()> {
        let store = Store::new()?;

        // Empty/non-existent hash returns empty array
        let hvals = HVals::parse(vec![RespValue::BulkString(Bytes::from("nonexistent"))])?;
        let result = collect_stream(hvals.execute(&store).await?).await;
        assert!(matches!(result, RespValue::Array(arr) if arr.is_empty()));

        // Add some fields
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let hvals = HVals::parse(vec![RespValue::BulkString(Bytes::from("myhash"))])?;
        let result = collect_stream(hvals.execute(&store).await?).await;
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hincrby() -> Result<()> {
        let store = Store::new()?;

        // Increment non-existent field creates it with value = increment
        let hincrby = HIncrBy::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("counter")),
            RespValue::BulkString(Bytes::from("5")),
        ])?;
        assert_eq!(hincrby.execute(&store).await?, RespValue::Integer(5));

        // Increment existing field
        let hincrby2 = HIncrBy::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("counter")),
            RespValue::BulkString(Bytes::from("10")),
        ])?;
        assert_eq!(hincrby2.execute(&store).await?, RespValue::Integer(15));

        // Negative increment (decrement)
        let hincrby3 = HIncrBy::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("counter")),
            RespValue::BulkString(Bytes::from("-3")),
        ])?;
        assert_eq!(hincrby3.execute(&store).await?, RespValue::Integer(12));

        Ok(())
    }

    #[tokio::test]
    async fn test_hincrby_invalid_value() -> Result<()> {
        let store = Store::new()?;

        store.hset(b"myhash", b"field", b"not a number".to_vec())?;

        let hincrby = HIncrBy::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("1")),
        ])?;

        assert!(hincrby.execute(&store).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_hincrbyfloat() -> Result<()> {
        let store = Store::new()?;

        // Increment non-existent field creates it
        let hincrbyfloat = HIncrByFloat::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("price")),
            RespValue::BulkString(Bytes::from("10.5")),
        ])?;
        let result = hincrbyfloat.execute(&store).await?;
        if let RespValue::BulkString(val) = result {
            let f: f64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
            assert!((f - 10.5).abs() < 0.001);
        } else {
            panic!("Expected BulkString response");
        }

        // Increment existing field
        let hincrbyfloat2 = HIncrByFloat::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("price")),
            RespValue::BulkString(Bytes::from("0.1")),
        ])?;
        let result = hincrbyfloat2.execute(&store).await?;
        if let RespValue::BulkString(val) = result {
            let f: f64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
            assert!((f - 10.6).abs() < 0.001);
        } else {
            panic!("Expected BulkString response");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hincrbyfloat_invalid() -> Result<()> {
        let store = Store::new()?;

        store.hset(b"myhash", b"field", b"not a number".to_vec())?;

        let hincrbyfloat = HIncrByFloat::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("1.0")),
        ])?;

        assert!(hincrbyfloat.execute(&store).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_hsetnx() -> Result<()> {
        let store = Store::new()?;

        // Set field when it doesn't exist - returns 1
        let hsetnx = HSetNx::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("value")),
        ])?;
        assert_eq!(hsetnx.execute(&store).await?, RespValue::Integer(1));

        // Try to set same field again - returns 0, value unchanged
        let hsetnx2 = HSetNx::parse(vec![
            RespValue::BulkString(Bytes::from("myhash")),
            RespValue::BulkString(Bytes::from("field")),
            RespValue::BulkString(Bytes::from("new_value")),
        ])?;
        assert_eq!(hsetnx2.execute(&store).await?, RespValue::Integer(0));

        // Verify value is still the original
        assert_eq!(store.hget(b"myhash", b"field")?, Some(b"value".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_hmset_parse_error() -> Result<()> {
        // Not enough arguments
        assert!(HMSet::parse(vec![RespValue::BulkString(Bytes::from("key"))]).is_err());

        // Odd number of field/value pairs
        assert!(
            HMSet::parse(vec![
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("field")),
            ])
            .is_err()
        );

        Ok(())
    }
}
