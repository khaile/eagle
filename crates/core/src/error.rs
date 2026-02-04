//! Error Handling Module
//!
//! Provides standardized error types for EagleDB with Redis-compatible error messages.
//! All errors implement Display to produce Redis-compatible error strings.

// New error infrastructure - some items reserved for future command migration
#![allow(dead_code)]

use crate::pmem::allocator::AllocError;
use std::fmt;
use thiserror::Error;

/// Redis-compatible error prefixes
/// See: https://redis.io/docs/reference/protocol-spec/#resp-errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Generic error (ERR)
    Err,
    /// Wrong type error (WRONGTYPE)
    WrongType,
    /// No such key error (ERR)
    NoKey,
    /// Syntax error (ERR)
    Syntax,
    /// Out of range error (ERR)
    OutOfRange,
    /// Not an integer error (ERR)
    NotInteger,
    /// Not a float error (ERR)
    NotFloat,
    /// Overflow error (ERR)
    Overflow,
    /// Invalid arguments error (ERR)
    InvalidArgs,
    /// Unknown command error (ERR)
    UnknownCommand,
    /// Protocol error (ERR)
    Protocol,
    /// Internal server error (ERR)
    Internal,
    /// IO error (ERR)
    Io,
    /// PMEM error (ERR)
    Pmem,
    /// Configuration error (ERR)
    Config,
    /// Busy error (BUSY)
    Busy,
    /// No auth error (NOAUTH)
    NoAuth,
    /// Cluster error (CLUSTERDOWN)
    ClusterDown,
}

impl ErrorKind {
    /// Returns the Redis error prefix for this error kind
    pub fn prefix(&self) -> &'static str {
        match self {
            ErrorKind::WrongType => "WRONGTYPE",
            ErrorKind::Busy => "BUSY",
            ErrorKind::NoAuth => "NOAUTH",
            ErrorKind::ClusterDown => "CLUSTERDOWN",
            _ => "ERR",
        }
    }

    /// Returns a short identifier for metrics
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::Err => "err",
            ErrorKind::WrongType => "wrongtype",
            ErrorKind::NoKey => "nokey",
            ErrorKind::Syntax => "syntax",
            ErrorKind::OutOfRange => "outofrange",
            ErrorKind::NotInteger => "notinteger",
            ErrorKind::NotFloat => "notfloat",
            ErrorKind::Overflow => "overflow",
            ErrorKind::InvalidArgs => "invalidargs",
            ErrorKind::UnknownCommand => "unknowncommand",
            ErrorKind::Protocol => "protocol",
            ErrorKind::Internal => "internal",
            ErrorKind::Io => "io",
            ErrorKind::Pmem => "pmem",
            ErrorKind::Config => "config",
            ErrorKind::Busy => "busy",
            ErrorKind::NoAuth => "noauth",
            ErrorKind::ClusterDown => "clusterdown",
        }
    }
}

/// Main error type for EagleDB commands
///
/// This error type produces Redis-compatible error messages when displayed.
/// The format is: `PREFIX message` where PREFIX is typically "ERR" or "WRONGTYPE".
#[derive(Error, Debug, Clone)]
pub struct CommandError {
    kind: ErrorKind,
    message: String,
}

/// Error type for storage and persistence operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("PMEM error: {0}")]
    Pmem(String),

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("AOF error: {0}")]
    Aof(String),

    #[error("Key not found: {0}")]
    NotFound(String),

    #[error("Invalid type or format: {0}")]
    InvalidType(String),

    #[error("Operation overflow: {0}")]
    Overflow(String),

    #[error("Limit exceeded: {0}")]
    LimitExceeded(String),

    #[error("Resource busy: {0}")]
    Busy(String),
}

impl From<AllocError> for StorageError {
    fn from(err: AllocError) -> Self {
        Self::Pmem(err.to_string())
    }
}

impl CommandError {
    /// Create a new command error with the given kind and message
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Get the error kind
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Get the error message (without prefix)
    pub fn message(&self) -> &str {
        &self.message
    }

    // ========== Common Error Constructors ==========

    /// Wrong number of arguments for a command
    pub fn wrong_arity(command: &str) -> Self {
        Self::new(
            ErrorKind::InvalidArgs,
            format!("wrong number of arguments for '{}' command", command),
        )
    }

    /// Unknown command
    pub fn unknown_command(command: &str) -> Self {
        Self::new(
            ErrorKind::UnknownCommand,
            format!(
                "unknown command '{}', with args beginning with:",
                command.to_uppercase()
            ),
        )
    }

    /// Wrong type operation
    pub fn wrong_type() -> Self {
        Self::new(
            ErrorKind::WrongType,
            "Operation against a key holding the wrong kind of value",
        )
    }

    /// Key does not exist
    pub fn no_such_key() -> Self {
        Self::new(ErrorKind::NoKey, "no such key")
    }

    /// Value is not an integer or out of range
    pub fn not_integer() -> Self {
        Self::new(
            ErrorKind::NotInteger,
            "value is not an integer or out of range",
        )
    }

    /// Value is not a valid float
    pub fn not_float() -> Self {
        Self::new(ErrorKind::NotFloat, "value is not a valid float")
    }

    /// Hash value is not an integer
    pub fn hash_not_integer() -> Self {
        Self::new(ErrorKind::NotInteger, "hash value is not an integer")
    }

    /// Hash value is not a valid float
    pub fn hash_not_float() -> Self {
        Self::new(ErrorKind::NotFloat, "hash value is not a valid float")
    }

    /// Increment/decrement would overflow
    pub fn overflow() -> Self {
        Self::new(ErrorKind::Overflow, "increment or decrement would overflow")
    }

    /// Invalid increment value (NaN or Infinity)
    pub fn nan_or_infinity() -> Self {
        Self::new(
            ErrorKind::NotFloat,
            "increment would produce NaN or Infinity",
        )
    }

    /// Syntax error
    pub fn syntax() -> Self {
        Self::new(ErrorKind::Syntax, "syntax error")
    }

    /// Invalid cursor
    pub fn invalid_cursor() -> Self {
        Self::new(ErrorKind::Syntax, "invalid cursor")
    }

    /// Invalid integer argument
    pub fn invalid_integer() -> Self {
        Self::new(
            ErrorKind::NotInteger,
            "value is not an integer or out of range",
        )
    }

    /// Offset out of range
    pub fn offset_out_of_range() -> Self {
        Self::new(ErrorKind::OutOfRange, "offset is out of range")
    }

    /// String exceeds maximum allowed size
    pub fn string_too_large() -> Self {
        Self::new(ErrorKind::OutOfRange, "string exceeds maximum allowed size")
    }

    /// Bit offset out of range
    pub fn bit_offset_out_of_range() -> Self {
        Self::new(
            ErrorKind::OutOfRange,
            "bit offset is not an integer or out of range",
        )
    }

    /// Generic error with custom message
    pub fn generic(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Err, message)
    }

    /// Internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    /// Protocol error
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, message)
    }

    /// IO error
    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Io, message)
    }

    /// PMEM error
    pub fn pmem(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Pmem, message)
    }

    /// Configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message)
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as Redis-compatible error: "PREFIX message"
        write!(f, "{} {}", self.kind.prefix(), self.message)
    }
}

// Conversions from other error types

impl From<std::io::Error> for CommandError {
    fn from(err: std::io::Error) -> Self {
        Self::io(err.to_string())
    }
}

impl From<std::num::ParseIntError> for CommandError {
    fn from(_: std::num::ParseIntError) -> Self {
        Self::not_integer()
    }
}

impl From<std::num::ParseFloatError> for CommandError {
    fn from(_: std::num::ParseFloatError) -> Self {
        Self::not_float()
    }
}

impl From<std::str::Utf8Error> for CommandError {
    fn from(_: std::str::Utf8Error) -> Self {
        Self::protocol("invalid UTF-8 sequence")
    }
}

impl From<StorageError> for CommandError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Io(e) => Self::io(e.to_string()),
            StorageError::Corruption(msg) => Self::internal(format!("Data corruption: {}", msg)),
            StorageError::Config(msg) => Self::config(msg),
            StorageError::Pmem(msg) => Self::pmem(msg),
            StorageError::Serialization(msg) => {
                Self::internal(format!("Serialization error: {}", msg))
            }
            StorageError::Snapshot(msg) => Self::internal(format!("Snapshot error: {}", msg)),
            StorageError::Aof(msg) => Self::internal(format!("AOF error: {}", msg)),
            StorageError::NotFound(_) => Self::no_such_key(),
            StorageError::InvalidType(msg) => Self::generic(msg),
            StorageError::Overflow(msg) => Self::new(ErrorKind::Overflow, msg),
            StorageError::LimitExceeded(msg) => Self::new(ErrorKind::OutOfRange, msg),
            StorageError::Busy(msg) => Self::new(ErrorKind::Busy, msg),
        }
    }
}

/// Result type alias for command operations
pub type CommandResult<T> = std::result::Result<T, CommandError>;

// ========== Legacy Error Type (for backwards compatibility) ==========

/// Legacy error enum - kept for backwards compatibility with existing code
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("PMEM error: {0}")]
    Pmem(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for storage operations using StorageError
pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_generic() {
        let err = CommandError::wrong_arity("get");
        assert_eq!(
            err.to_string(),
            "ERR wrong number of arguments for 'get' command"
        );
    }

    #[test]
    fn test_error_display_wrongtype() {
        let err = CommandError::wrong_type();
        assert_eq!(
            err.to_string(),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn test_error_display_not_integer() {
        let err = CommandError::not_integer();
        assert_eq!(
            err.to_string(),
            "ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn test_error_display_overflow() {
        let err = CommandError::overflow();
        assert_eq!(err.to_string(), "ERR increment or decrement would overflow");
    }

    #[test]
    fn test_error_kind() {
        let err = CommandError::wrong_type();
        assert_eq!(err.kind(), ErrorKind::WrongType);
        assert_eq!(err.kind().prefix(), "WRONGTYPE");
        assert_eq!(err.kind().as_str(), "wrongtype");
    }

    #[test]
    fn test_error_from_parse_int() {
        let result: std::result::Result<i64, _> = "not_a_number".parse();
        let err: CommandError = result.unwrap_err().into();
        assert_eq!(err.kind(), ErrorKind::NotInteger);
    }

    #[test]
    fn test_error_from_parse_float() {
        let result: std::result::Result<f64, _> = "not_a_float".parse();
        let err: CommandError = result.unwrap_err().into();
        assert_eq!(err.kind(), ErrorKind::NotFloat);
    }

    #[test]
    fn test_unknown_command() {
        let err = CommandError::unknown_command("foo");
        assert!(err.to_string().contains("unknown command 'FOO'"));
    }
}
