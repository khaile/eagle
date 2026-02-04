//! Connection Command Implementations
//!
//! Implements connection-related commands for the Redis protocol:
//! - PING: Test server connectivity
//! - ECHO: Echo a message back

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;

/// PING command - test if server is alive
/// Returns PONG if no argument, or the argument if provided
#[derive(Debug)]
pub struct Ping {
    pub message: Option<Bytes>,
}

impl Ping {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() > 1 {
            return Err(CommandError::wrong_arity("ping").into());
        }

        let message = if args.is_empty() {
            None
        } else {
            match args.remove(0) {
                RespValue::BulkString(bytes) => Some(bytes),
                _ => return Err(CommandError::syntax().into()),
            }
        };

        Ok(Ping { message })
    }
}

#[async_trait]
impl CommandHandler for Ping {
    fn name(&self) -> &'static str {
        "PING"
    }

    async fn execute(&self, _store: &Store) -> Result<RespValue> {
        match &self.message {
            Some(msg) => Ok(RespValue::BulkString(msg.clone())),
            None => Ok(RespValue::SimpleString("PONG".to_string())),
        }
    }
}

/// ECHO command - echo the given message
#[derive(Debug)]
pub struct Echo {
    pub message: Bytes,
}

impl Echo {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 1 {
            return Err(CommandError::wrong_arity("echo").into());
        }

        let message = match args.remove(0) {
            RespValue::BulkString(bytes) => bytes,
            _ => return Err(CommandError::syntax().into()),
        };

        Ok(Echo { message })
    }
}

#[async_trait]
impl CommandHandler for Echo {
    fn name(&self) -> &'static str {
        "ECHO"
    }

    async fn execute(&self, _store: &Store) -> Result<RespValue> {
        Ok(RespValue::BulkString(self.message.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping_no_args() -> Result<()> {
        let store = Store::new_memory()?;
        let ping = Ping { message: None };
        let result = ping.execute(&store).await?;

        assert!(matches!(result, RespValue::SimpleString(s) if s == "PONG"));
        Ok(())
    }

    #[tokio::test]
    async fn test_ping_with_message() -> Result<()> {
        let store = Store::new_memory()?;
        let ping = Ping {
            message: Some(Bytes::from("hello")),
        };
        let result = ping.execute(&store).await?;

        assert!(matches!(result, RespValue::BulkString(b) if b == "hello"));
        Ok(())
    }

    #[tokio::test]
    async fn test_echo() -> Result<()> {
        let store = Store::new_memory()?;
        let echo = Echo {
            message: Bytes::from("Hello World"),
        };
        let result = echo.execute(&store).await?;

        assert!(matches!(result, RespValue::BulkString(b) if b == "Hello World"));
        Ok(())
    }

    #[test]
    fn test_parse_ping_no_args() -> Result<()> {
        let args = vec![];
        let ping = Ping::parse(args)?;
        assert!(ping.message.is_none());
        Ok(())
    }

    #[test]
    fn test_parse_ping_with_message() -> Result<()> {
        let args = vec![RespValue::BulkString(Bytes::from("hello"))];
        let ping = Ping::parse(args)?;
        assert_eq!(ping.message, Some(Bytes::from("hello")));
        Ok(())
    }

    #[test]
    fn test_parse_ping_invalid_arg_type_errors() {
        let args = vec![RespValue::SimpleString("oops".to_string())];
        assert!(Ping::parse(args).is_err());
    }

    #[test]
    fn test_parse_ping_too_many_args_errors() {
        let args = vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ];
        assert!(Ping::parse(args).is_err());
    }

    #[test]
    fn test_parse_echo() -> Result<()> {
        let args = vec![RespValue::BulkString(Bytes::from("test"))];
        let echo = Echo::parse(args)?;
        assert_eq!(echo.message, Bytes::from("test"));
        Ok(())
    }

    #[test]
    fn test_parse_echo_no_args() {
        let args = vec![];
        assert!(Echo::parse(args).is_err());
    }

    #[test]
    fn test_parse_echo_too_many_args() {
        let args = vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ];
        assert!(Echo::parse(args).is_err());
    }
}
