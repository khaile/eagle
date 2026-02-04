//! RESP Encoder infrastructure
#![allow(dead_code)]

use super::{RespError, RespValue};
use bytes::{BufMut, BytesMut};

#[derive(Debug)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

pub struct Encoder {
    buffer: BytesMut,
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(1024),
        }
    }

    /// Create an encoder with a specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Clear the buffer for reuse
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Encode array header (*<len>\r\n)
    pub fn encode_array_header(&mut self, len: usize) {
        self.buffer.put_u8(b'*');
        let mut itoa_buf = itoa::Buffer::new();
        self.buffer.put_slice(itoa_buf.format(len).as_bytes());
        self.write_crlf();
    }

    /// Get the encoded bytes
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Encode a RespValue directly into the buffer
    pub fn encode_resp(&mut self, value: &RespValue) -> Result<(), RespError> {
        match value {
            RespValue::SimpleString(s) => {
                self.buffer.put_u8(b'+');
                self.buffer.put_slice(s.as_bytes());
                self.write_crlf();
            }
            RespValue::Error(e) => {
                self.buffer.put_u8(b'-');
                self.buffer.put_slice(e.as_bytes());
                self.write_crlf();
            }
            RespValue::Integer(i) => {
                self.buffer.put_u8(b':');
                // Use itoa for faster integer formatting
                let mut itoa_buf = itoa::Buffer::new();
                self.buffer.put_slice(itoa_buf.format(*i).as_bytes());
                self.write_crlf();
            }
            RespValue::BulkString(bytes) => {
                self.buffer.put_u8(b'$');
                let mut itoa_buf = itoa::Buffer::new();
                self.buffer
                    .put_slice(itoa_buf.format(bytes.len()).as_bytes());
                self.write_crlf();
                self.buffer.put_slice(bytes);
                self.write_crlf();
            }
            RespValue::NullBulkString | RespValue::Null => {
                self.buffer.put_slice(b"$-1\r\n");
            }
            RespValue::Array(items) => {
                self.buffer.put_u8(b'*');
                let mut itoa_buf = itoa::Buffer::new();
                self.buffer
                    .put_slice(itoa_buf.format(items.len()).as_bytes());
                self.write_crlf();
                for item in items {
                    self.encode_resp(item)?;
                }
            }
            RespValue::NullArray => {
                self.buffer.put_slice(b"*-1\r\n");
            }
            RespValue::StreamArray(_, _) => {
                return Err(RespError::InvalidFormat);
            }
        }
        Ok(())
    }

    pub fn encode(&mut self, value: &Value) -> Result<(), RespError> {
        match value {
            Value::SimpleString(s) => {
                self.buffer.put_u8(b'+');
                self.buffer.put_slice(s.as_bytes());
                self.write_crlf();
            }
            Value::Error(s) => {
                self.buffer.put_u8(b'-');
                self.buffer.put_slice(s.as_bytes());
                self.write_crlf();
            }
            Value::Integer(i) => {
                self.buffer.put_u8(b':');
                self.buffer.put_slice(i.to_string().as_bytes());
                self.write_crlf();
            }
            Value::BulkString(opt) => {
                self.buffer.put_u8(b'$');
                match opt {
                    Some(s) => {
                        self.buffer.put_slice(s.len().to_string().as_bytes());
                        self.write_crlf();
                        self.buffer.put_slice(s);
                    }
                    None => {
                        self.buffer.put_slice(b"-1");
                    }
                }
                self.write_crlf();
            }
            Value::Array(opt) => {
                self.buffer.put_u8(b'*');
                match opt {
                    Some(arr) => {
                        self.buffer.put_slice(arr.len().to_string().as_bytes());
                        self.write_crlf();
                        for v in arr {
                            self.encode(v)?;
                        }
                    }
                    None => {
                        self.buffer.put_slice(b"-1");
                        self.write_crlf();
                    }
                }
            }
        }
        Ok(())
    }

    fn write_crlf(&mut self) {
        self.buffer.put_slice(b"\r\n");
    }

    pub fn get_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a RespValue into a new BytesMut buffer
/// For repeated encoding, prefer using Encoder directly with encode_resp()
pub fn encode(value: &RespValue) -> Result<BytesMut, RespError> {
    let mut encoder = Encoder::new();
    encoder.encode_resp(value)?;
    Ok(encoder.buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let mut encoder = Encoder::new();

        encoder
            .encode(&Value::SimpleString("OK".to_string()))
            .unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let mut encoder = Encoder::new();

        encoder
            .encode(&Value::Error("Error message".to_string()))
            .unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b"-Error message\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let mut encoder = Encoder::new();

        encoder.encode(&Value::Integer(42)).unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let mut encoder = Encoder::new();

        encoder
            .encode(&Value::BulkString(Some(b"hello".to_vec())))
            .unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let mut encoder = Encoder::new();
        encoder.encode(&Value::BulkString(None)).unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let mut encoder = Encoder::new();

        let array = vec![
            Value::BulkString(Some(b"SET".to_vec())),
            Value::BulkString(Some(b"key".to_vec())),
            Value::BulkString(Some(b"value".to_vec())),
        ];
        encoder.encode(&Value::Array(Some(array))).unwrap();
        assert_eq!(
            encoder.get_mut().as_ref(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn test_encode_null_array() {
        let mut encoder = Encoder::new();
        encoder.encode(&Value::Array(None)).unwrap();
        assert_eq!(encoder.get_mut().as_ref(), b"*-1\r\n");
    }
}
