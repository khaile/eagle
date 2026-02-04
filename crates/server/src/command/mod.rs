use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;

use eagle_core::resp::RespValue;
use eagle_core::store::Store;

pub mod cluster;
pub mod connection;
pub mod generic;
pub mod hash;
pub mod persistence;
pub mod scan;
pub mod set_get;
pub mod string;
pub mod ttl;

#[async_trait]
pub trait CommandHandler: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    async fn execute(&self, store: &Store) -> Result<RespValue>;
}

#[derive(Debug)]
pub enum Command {
    Get(set_get::Get),
    Set(set_get::Set),
    Del(set_get::Del),
    HSet(hash::HSet),
    HGet(hash::HGet),
    HDel(hash::HDel),
    // Hash commands (Milestone 1.5)
    HGetAll(hash::HGetAll),
    HMSet(hash::HMSet),
    HMGet(hash::HMGet),
    HLen(hash::HLen),
    HExists(hash::HExists),
    HKeys(hash::HKeys),
    HVals(hash::HVals),
    HIncrBy(hash::HIncrBy),
    HIncrByFloat(hash::HIncrByFloat),
    HSetNx(hash::HSetNx),
    ClusterInfo(cluster::ClusterInfo),
    ClusterNodes(cluster::ClusterNodes),
    // TTL commands
    Expire(ttl::Expire),
    PExpire(ttl::PExpire),
    ExpireAt(ttl::ExpireAt),
    PExpireAt(ttl::PExpireAt),
    Ttl(ttl::Ttl),
    PTtl(ttl::PTtl),
    Persist(ttl::Persist),
    // String commands (Milestone 1.3)
    Mget(string::Mget),
    Mset(string::Mset),
    Incr(string::Incr),
    Decr(string::Decr),
    IncrBy(string::IncrBy),
    DecrBy(string::DecrBy),
    IncrByFloat(string::IncrByFloat),
    Append(string::Append),
    StrLen(string::StrLen),
    GetRange(string::GetRange),
    SetRange(string::SetRange),
    // Generic commands (Milestone 1.3)
    Exists(generic::Exists),
    Type(generic::Type),
    Rename(generic::Rename),
    RenameNx(generic::RenameNx),
    // Connection commands
    Ping(connection::Ping),
    Echo(connection::Echo),
    // Persistence commands (Milestone 3.2)
    Save(persistence::Save),
    Bgsave(persistence::Bgsave),
    LastSave(persistence::LastSave),
    // Scanning commands (Milestone 1.4)
    Keys(scan::Keys),
    Scan(scan::Scan),
    DbSize(scan::DbSize),
    RandomKey(scan::RandomKey),
    FlushDb(scan::FlushDb),
}

impl Command {
    pub fn is_write_resp(value: &RespValue) -> Result<bool> {
        let command = Self::command_name_from_resp(value)?;
        Ok(Self::is_write_command(command))
    }

    pub fn should_log_aof(&self, response: &RespValue) -> bool {
        match self {
            Command::Set(command) => match response {
                RespValue::SimpleString(_) => true,
                RespValue::BulkString(_) => {
                    command.options.condition != Some(set_get::SetCondition::NotExists)
                }
                _ => false,
            },
            Command::Del(_) => matches!(response, RespValue::Integer(count) if *count > 0),
            Command::HSet(_) => true,
            Command::HDel(_) => matches!(response, RespValue::Integer(count) if *count > 0),
            Command::HMSet(_) => true,
            Command::HIncrBy(_) => true,
            Command::HIncrByFloat(_) => true,
            Command::HSetNx(_) => matches!(response, RespValue::Integer(1)),
            Command::Expire(_)
            | Command::PExpire(_)
            | Command::ExpireAt(_)
            | Command::PExpireAt(_)
            | Command::Persist(_) => matches!(response, RespValue::Integer(1)),
            Command::Mset(_)
            | Command::Incr(_)
            | Command::Decr(_)
            | Command::IncrBy(_)
            | Command::DecrBy(_)
            | Command::IncrByFloat(_)
            | Command::Append(_)
            | Command::SetRange(_) => true,
            Command::Rename(_) => true,
            Command::RenameNx(_) => matches!(response, RespValue::Integer(1)),
            Command::FlushDb(_) => true,
            _ => false,
        }
    }

    pub fn from_resp(value: RespValue) -> Result<Self> {
        match value {
            RespValue::Array(items) => {
                let mut items: Vec<_> = items.into_iter().collect();
                if items.is_empty() {
                    return Err(anyhow!("Empty command"));
                }

                let command_bytes = Self::extract_command_bytes(&mut items)?;

                // Match commands using case-insensitive byte comparison (no allocation)
                if command_bytes.eq_ignore_ascii_case(b"GET") {
                    Ok(Command::Get(set_get::Get::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"SET") {
                    Ok(Command::Set(set_get::Set::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"DEL") {
                    Ok(Command::Del(set_get::Del::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HSET") {
                    Ok(Command::HSet(hash::HSet::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HGET") {
                    Ok(Command::HGet(hash::HGet::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HDEL") {
                    Ok(Command::HDel(hash::HDel::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HGETALL") {
                    Ok(Command::HGetAll(hash::HGetAll::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HMSET") {
                    Ok(Command::HMSet(hash::HMSet::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HMGET") {
                    Ok(Command::HMGet(hash::HMGet::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HLEN") {
                    Ok(Command::HLen(hash::HLen::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HEXISTS") {
                    Ok(Command::HExists(hash::HExists::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HKEYS") {
                    Ok(Command::HKeys(hash::HKeys::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HVALS") {
                    Ok(Command::HVals(hash::HVals::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HINCRBY") {
                    Ok(Command::HIncrBy(hash::HIncrBy::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HINCRBYFLOAT") {
                    Ok(Command::HIncrByFloat(hash::HIncrByFloat::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"HSETNX") {
                    Ok(Command::HSetNx(hash::HSetNx::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"INFO") {
                    // INFO and COMMAND are triggered when running `redis-cli`
                    Ok(Command::ClusterInfo(cluster::ClusterInfo::new()))
                } else if command_bytes.eq_ignore_ascii_case(b"COMMAND") {
                    let subcommand = Self::extract_command_bytes(&mut items)?;
                    if subcommand.eq_ignore_ascii_case(b"DOCS") {
                        Ok(Command::ClusterInfo(cluster::ClusterInfo::new()))
                    } else {
                        Err(anyhow!(
                            "Unknown subcommand: {}",
                            String::from_utf8_lossy(&subcommand)
                        ))
                    }
                } else if command_bytes.eq_ignore_ascii_case(b"EXPIRE") {
                    Ok(Command::Expire(ttl::Expire::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"PEXPIRE") {
                    Ok(Command::PExpire(ttl::PExpire::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"EXPIREAT") {
                    Ok(Command::ExpireAt(ttl::ExpireAt::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"PEXPIREAT") {
                    Ok(Command::PExpireAt(ttl::PExpireAt::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"TTL") {
                    Ok(Command::Ttl(ttl::Ttl::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"PTTL") {
                    Ok(Command::PTtl(ttl::PTtl::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"PERSIST") {
                    Ok(Command::Persist(ttl::Persist::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"MGET") {
                    Ok(Command::Mget(string::Mget::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"MSET") {
                    Ok(Command::Mset(string::Mset::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"INCR") {
                    Ok(Command::Incr(string::Incr::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"DECR") {
                    Ok(Command::Decr(string::Decr::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"INCRBY") {
                    Ok(Command::IncrBy(string::IncrBy::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"DECRBY") {
                    Ok(Command::DecrBy(string::DecrBy::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"INCRBYFLOAT") {
                    Ok(Command::IncrByFloat(string::IncrByFloat::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"APPEND") {
                    Ok(Command::Append(string::Append::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"STRLEN") {
                    Ok(Command::StrLen(string::StrLen::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"GETRANGE") {
                    Ok(Command::GetRange(string::GetRange::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"SETRANGE") {
                    Ok(Command::SetRange(string::SetRange::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"EXISTS") {
                    Ok(Command::Exists(generic::Exists::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"TYPE") {
                    Ok(Command::Type(generic::Type::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"RENAME") {
                    Ok(Command::Rename(generic::Rename::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"RENAMENX") {
                    Ok(Command::RenameNx(generic::RenameNx::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"PING") {
                    Ok(Command::Ping(connection::Ping::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"ECHO") {
                    Ok(Command::Echo(connection::Echo::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"SAVE") {
                    Ok(Command::Save(persistence::Save::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"BGSAVE") {
                    Ok(Command::Bgsave(persistence::Bgsave::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"LASTSAVE") {
                    Ok(Command::LastSave(persistence::LastSave::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"KEYS") {
                    Ok(Command::Keys(scan::Keys::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"SCAN") {
                    Ok(Command::Scan(scan::Scan::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"DBSIZE") {
                    Ok(Command::DbSize(scan::DbSize::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"RANDOMKEY") {
                    Ok(Command::RandomKey(scan::RandomKey::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"FLUSHDB") {
                    Ok(Command::FlushDb(scan::FlushDb::parse(items)?))
                } else if command_bytes.eq_ignore_ascii_case(b"CLUSTER") {
                    if items.is_empty() {
                        return Err(anyhow!("Missing cluster subcommand"));
                    }
                    let subcommand = Self::extract_command_bytes(&mut items)?;
                    if subcommand.eq_ignore_ascii_case(b"INFO") {
                        Ok(Command::ClusterInfo(cluster::ClusterInfo::new()))
                    } else if subcommand.eq_ignore_ascii_case(b"NODES") {
                        Ok(Command::ClusterNodes(cluster::ClusterNodes::new()))
                    } else {
                        Err(anyhow!(
                            "Unknown cluster subcommand: {}",
                            String::from_utf8_lossy(&subcommand)
                        ))
                    }
                } else {
                    Err(anyhow!(
                        "Unknown command: {}",
                        String::from_utf8_lossy(&command_bytes)
                    ))
                }
            }
            _ => Err(anyhow!("Invalid command format")),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Command::Get(_) => "GET",
            Command::Set(_) => "SET",
            Command::Del(_) => "DEL",
            Command::HSet(_) => "HSET",
            Command::HGet(_) => "HGET",
            Command::HDel(_) => "HDEL",
            // Hash commands (Milestone 1.5)
            Command::HGetAll(_) => "HGETALL",
            Command::HMSet(_) => "HMSET",
            Command::HMGet(_) => "HMGET",
            Command::HLen(_) => "HLEN",
            Command::HExists(_) => "HEXISTS",
            Command::HKeys(_) => "HKEYS",
            Command::HVals(_) => "HVALS",
            Command::HIncrBy(_) => "HINCRBY",
            Command::HIncrByFloat(_) => "HINCRBYFLOAT",
            Command::HSetNx(_) => "HSETNX",
            Command::ClusterInfo(_) => "CLUSTER INFO",
            Command::ClusterNodes(_) => "CLUSTER NODES",
            // TTL commands
            Command::Expire(_) => "EXPIRE",
            Command::PExpire(_) => "PEXPIRE",
            Command::ExpireAt(_) => "EXPIREAT",
            Command::PExpireAt(_) => "PEXPIREAT",
            Command::Ttl(_) => "TTL",
            Command::PTtl(_) => "PTTL",
            Command::Persist(_) => "PERSIST",
            // String commands (Milestone 1.3)
            Command::Mget(_) => "MGET",
            Command::Mset(_) => "MSET",
            Command::Incr(_) => "INCR",
            Command::Decr(_) => "DECR",
            Command::IncrBy(_) => "INCRBY",
            Command::DecrBy(_) => "DECRBY",
            Command::IncrByFloat(_) => "INCRBYFLOAT",
            Command::Append(_) => "APPEND",
            Command::StrLen(_) => "STRLEN",
            Command::GetRange(_) => "GETRANGE",
            Command::SetRange(_) => "SETRANGE",
            // Generic commands (Milestone 1.3)
            Command::Exists(_) => "EXISTS",
            Command::Type(_) => "TYPE",
            Command::Rename(_) => "RENAME",
            Command::RenameNx(_) => "RENAMENX",
            // Connection commands
            Command::Ping(_) => "PING",
            Command::Echo(_) => "ECHO",
            // Persistence commands (Milestone 3.2)
            Command::Save(_) => "SAVE",
            Command::Bgsave(_) => "BGSAVE",
            Command::LastSave(_) => "LASTSAVE",
            // Scanning commands (Milestone 1.4)
            Command::Keys(_) => "KEYS",
            Command::Scan(_) => "SCAN",
            Command::DbSize(_) => "DBSIZE",
            Command::RandomKey(_) => "RANDOMKEY",
            Command::FlushDb(_) => "FLUSHDB",
        }
    }

    pub async fn execute(&self, store: &Store) -> Result<RespValue> {
        match self {
            Command::Get(cmd) => cmd.execute(store).await,
            Command::Set(cmd) => cmd.execute(store).await,
            Command::Del(cmd) => cmd.execute(store).await,
            Command::HSet(cmd) => cmd.execute(store).await,
            Command::HGet(cmd) => cmd.execute(store).await,
            Command::HDel(cmd) => cmd.execute(store).await,
            // Hash commands (Milestone 1.5)
            Command::HGetAll(cmd) => cmd.execute(store).await,
            Command::HMSet(cmd) => cmd.execute(store).await,
            Command::HMGet(cmd) => cmd.execute(store).await,
            Command::HLen(cmd) => cmd.execute(store).await,
            Command::HExists(cmd) => cmd.execute(store).await,
            Command::HKeys(cmd) => cmd.execute(store).await,
            Command::HVals(cmd) => cmd.execute(store).await,
            Command::HIncrBy(cmd) => cmd.execute(store).await,
            Command::HIncrByFloat(cmd) => cmd.execute(store).await,
            Command::HSetNx(cmd) => cmd.execute(store).await,
            Command::ClusterInfo(cmd) => cmd.execute(store).await,
            Command::ClusterNodes(cmd) => cmd.execute(store).await,
            // TTL commands
            Command::Expire(cmd) => cmd.execute(store).await,
            Command::PExpire(cmd) => cmd.execute(store).await,
            Command::ExpireAt(cmd) => cmd.execute(store).await,
            Command::PExpireAt(cmd) => cmd.execute(store).await,
            Command::Ttl(cmd) => cmd.execute(store).await,
            Command::PTtl(cmd) => cmd.execute(store).await,
            Command::Persist(cmd) => cmd.execute(store).await,
            // String commands (Milestone 1.3)
            Command::Mget(cmd) => cmd.execute(store).await,
            Command::Mset(cmd) => cmd.execute(store).await,
            Command::Incr(cmd) => cmd.execute(store).await,
            Command::Decr(cmd) => cmd.execute(store).await,
            Command::IncrBy(cmd) => cmd.execute(store).await,
            Command::DecrBy(cmd) => cmd.execute(store).await,
            Command::IncrByFloat(cmd) => cmd.execute(store).await,
            Command::Append(cmd) => cmd.execute(store).await,
            Command::StrLen(cmd) => cmd.execute(store).await,
            Command::GetRange(cmd) => cmd.execute(store).await,
            Command::SetRange(cmd) => cmd.execute(store).await,
            // Generic commands (Milestone 1.3)
            Command::Exists(cmd) => cmd.execute(store).await,
            Command::Type(cmd) => cmd.execute(store).await,
            Command::Rename(cmd) => cmd.execute(store).await,
            Command::RenameNx(cmd) => cmd.execute(store).await,
            // Connection commands
            Command::Ping(cmd) => cmd.execute(store).await,
            Command::Echo(cmd) => cmd.execute(store).await,
            // Persistence commands (Milestone 3.2)
            Command::Save(cmd) => cmd.execute(store).await,
            Command::Bgsave(cmd) => cmd.execute(store).await,
            Command::LastSave(cmd) => cmd.execute(store).await,
            // Scanning commands (Milestone 1.4)
            Command::Keys(cmd) => cmd.execute(store).await,
            Command::Scan(cmd) => cmd.execute(store).await,
            Command::DbSize(cmd) => cmd.execute(store).await,
            Command::RandomKey(cmd) => cmd.execute(store).await,
            Command::FlushDb(cmd) => cmd.execute(store).await,
        }
    }

    /// Extract command bytes from the first item without String allocation
    #[inline]
    fn extract_command_bytes(items: &mut Vec<RespValue>) -> Result<Bytes> {
        match items.remove(0) {
            RespValue::BulkString(bytes) => Ok(bytes),
            _ => Err(anyhow!("Invalid command name format")),
        }
    }

    fn command_name_from_resp(value: &RespValue) -> Result<&[u8]> {
        let items = match value {
            RespValue::Array(items) => items,
            _ => return Err(anyhow!("Command must be a RESP array")),
        };

        let command_value = items.front().ok_or_else(|| anyhow!("Empty command"))?;
        match command_value {
            RespValue::BulkString(bytes) => Ok(bytes),
            RespValue::SimpleString(value) => Ok(value.as_bytes()),
            _ => Err(anyhow!("Invalid command type")),
        }
    }

    fn is_write_command(command: &[u8]) -> bool {
        const WRITE_COMMANDS: &[&[u8]] = &[
            b"SET",
            b"DEL",
            b"HSET",
            b"HDEL",
            b"HMSET",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"HSETNX",
            b"EXPIRE",
            b"PEXPIRE",
            b"EXPIREAT",
            b"PEXPIREAT",
            b"PERSIST",
            b"MSET",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"APPEND",
            b"SETRANGE",
            b"RENAME",
            b"RENAMENX",
            b"FLUSHDB",
        ];

        WRITE_COMMANDS
            .iter()
            .any(|&cmd| command.eq_ignore_ascii_case(cmd))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn test_command_parsing() -> Result<()> {
        // Test GET command
        let get_cmd = RespValue::Array(VecDeque::from([
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("key")),
        ]));
        let cmd = Command::from_resp(get_cmd)?;
        assert_eq!(cmd.name(), "GET");

        // Test SET command
        let set_cmd = RespValue::Array(VecDeque::from([
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
        ]));
        let cmd = Command::from_resp(set_cmd)?;
        assert_eq!(cmd.name(), "SET");

        Ok(())
    }

    #[tokio::test]
    async fn test_command_execution() -> Result<()> {
        let store = Store::new_memory()?;

        // Test SET command
        let set_cmd = Command::Set(set_get::Set {
            key: Bytes::from("test_key"),
            value: Bytes::from("test_value"),
            options: set_get::SetOptions::default(),
        });
        let result = set_cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(_)));

        // Test GET command
        let get_cmd = Command::Get(set_get::Get {
            key: Bytes::from("test_key"),
        });
        let result = get_cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::BulkString(_)));

        Ok(())
    }
}
