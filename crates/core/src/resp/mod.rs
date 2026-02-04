//! Redis Serialization Protocol (RESP) implementation
use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::collections::VecDeque;

use tokio::sync::mpsc;

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(VecDeque<RespValue>),
    NullArray,
    Null,
    StreamArray(usize, mpsc::Receiver<Result<RespValue>>),
}

impl PartialEq for RespValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SimpleString(l0), Self::SimpleString(r0)) => l0 == r0,
            (Self::Error(l0), Self::Error(r0)) => l0 == r0,
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::BulkString(l0), Self::BulkString(r0)) => l0 == r0,
            (Self::NullBulkString, Self::NullBulkString) => true,
            (Self::Array(l0), Self::Array(r0)) => l0 == r0,
            (Self::NullArray, Self::NullArray) => true,
            (Self::Null, Self::Null) => true,
            // StreamArrays are never equal to anything, even themselves (can't compare streams easily)
            _ => false,
        }
    }
}

impl RespValue {
    pub fn into_bytes(self) -> Result<Bytes> {
        self.as_bulk_bytes().map(Bytes::copy_from_slice)
    }

    pub fn as_bulk_bytes(&self) -> Result<&[u8]> {
        match self {
            RespValue::BulkString(bytes) => Ok(bytes),
            _ => Err(anyhow!("Expected bulk string")),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum RespError {
    #[error("Invalid RESP format")]
    InvalidFormat,
    #[error("Incomplete input")]
    Incomplete,
    #[error("Unsupported type: {0}")]
    UnsupportedType(char),
    #[error("Nested array")]
    NestedArray,
}

pub mod encoder;
pub mod parser;
