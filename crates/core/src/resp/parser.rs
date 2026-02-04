use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::collections::VecDeque;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

use super::RespValue;

pub struct RespParser<R> {
    reader: R,
}

impl<R: AsyncBufRead + Unpin> RespParser<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub async fn parse(&mut self) -> Result<RespValue> {
        Box::pin(async move {
            // We need to read the first byte to determine the type
            let byte = self.read_byte().await?;
            match byte {
                b'+' => self.parse_simple_string().await,
                b'-' => self.parse_error().await,
                b':' => self.parse_integer().await,
                b'$' => self.parse_bulk_string().await,
                b'*' => self.parse_array().await,
                _ => Err(anyhow!("Invalid RESP type byte: {}", byte)),
            }
        })
        .await
    }

    async fn read_byte(&mut self) -> Result<u8> {
        self.reader
            .read_u8()
            .await
            .map_err(|_| anyhow!("Connection closed"))
    }

    async fn read_line(&mut self) -> Result<String> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            return Err(anyhow!("Connection closed"));
        }

        // Remove trailing CRLF
        if line.ends_with("\r\n") {
            line.truncate(line.len() - 2);
        } else if line.ends_with('\n') {
            line.truncate(line.len() - 1);
        }

        Ok(line)
    }

    async fn parse_simple_string(&mut self) -> Result<RespValue> {
        let line = self.read_line().await?;
        Ok(RespValue::SimpleString(line))
    }

    async fn parse_error(&mut self) -> Result<RespValue> {
        let line = self.read_line().await?;
        Ok(RespValue::Error(line))
    }

    async fn parse_integer(&mut self) -> Result<RespValue> {
        let line = self.read_line().await?;
        let value = line
            .parse::<i64>()
            .map_err(|_| anyhow!("Invalid integer: {}", line))?;
        Ok(RespValue::Integer(value))
    }

    async fn parse_bulk_string(&mut self) -> Result<RespValue> {
        let line = self.read_line().await?;
        let length = line
            .parse::<i64>()
            .map_err(|_| anyhow!("Invalid bulk string length: {}", line))?;

        if length == -1 {
            return Ok(RespValue::NullBulkString);
        }

        if length < 0 {
            return Err(anyhow!("Invalid bulk string length: {}", length));
        }

        // Limit bulk string size to 512MB to prevent memory exhaustion attacks
        const MAX_BULK_SIZE: i64 = 512 * 1024 * 1024;
        if length > MAX_BULK_SIZE {
            return Err(anyhow!(
                "Bulk string length {} exceeds maximum allowed size",
                length
            ));
        }

        let length = length as usize;
        let mut data = vec![0u8; length];

        self.reader.read_exact(&mut data).await?;

        // Read and discard CRLF
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await?;
        if crlf != [b'\r', b'\n'] {
            return Err(anyhow!("Missing CRLF after bulk string"));
        }

        Ok(RespValue::BulkString(Bytes::from(data)))
    }

    async fn parse_array(&mut self) -> Result<RespValue> {
        let line = self.read_line().await?;
        let length = line
            .parse::<i64>()
            .map_err(|_| anyhow!("Invalid array length: {}", line))?;

        if length == -1 {
            return Ok(RespValue::NullArray);
        }

        if length < 0 {
            return Err(anyhow!("Invalid array length: {}", length));
        }

        // Limit array size to prevent stack overflow from deep recursion
        // and memory exhaustion from huge arrays
        const MAX_ARRAY_SIZE: i64 = 1024 * 1024;
        if length > MAX_ARRAY_SIZE {
            return Err(anyhow!(
                "Array length {} exceeds maximum allowed size",
                length
            ));
        }

        let length = length as usize;
        let mut items = VecDeque::with_capacity(length);

        for _ in 0..length {
            items.push_back(self.parse().await?);
        }

        Ok(RespValue::Array(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    async fn parse_input(input: &[u8]) -> Result<RespValue> {
        let cursor = Cursor::new(input);
        let mut parser = RespParser::new(cursor);
        parser.parse().await
    }

    #[tokio::test]
    async fn test_simple_string() -> Result<()> {
        let input = b"+OK\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));
        Ok(())
    }

    #[tokio::test]
    async fn test_error() -> Result<()> {
        let input = b"-Error message\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::Error(s) if s == "Error message"));
        Ok(())
    }

    #[tokio::test]
    async fn test_integer() -> Result<()> {
        let input = b":1234\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::Integer(n) if n == 1234));
        Ok(())
    }

    #[tokio::test]
    async fn test_bulk_string() -> Result<()> {
        let input = b"$5\r\nhello\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::BulkString(b) if b == "hello"));

        let input = b"$-1\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::NullBulkString));
        Ok(())
    }

    #[tokio::test]
    async fn test_array() -> Result<()> {
        let input = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let result = parse_input(input).await?;
        match result {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], RespValue::BulkString(b) if b == "hello"));
                assert!(matches!(&items[1], RespValue::BulkString(b) if b == "world"));
            }
            _ => panic!("Expected array"),
        }

        let input = b"*-1\r\n";
        let result = parse_input(input).await?;
        assert!(matches!(result, RespValue::NullArray));
        Ok(())
    }

    #[tokio::test]
    async fn test_bulk_string_negative_length() {
        // -1 is valid (null), but other negative values should fail
        let input = b"$-2\r\n";
        let result = parse_input(input).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid bulk string length")
        );
    }

    #[tokio::test]
    async fn test_array_negative_length() {
        // -1 is valid (null), but other negative values should fail
        let input = b"*-2\r\n";
        let result = parse_input(input).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid array length")
        );
    }

    #[tokio::test]
    async fn test_bulk_string_exceeds_max_size() {
        // Try to allocate more than 512MB
        let input = b"$600000000\r\n";
        let result = parse_input(input).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[tokio::test]
    async fn test_array_exceeds_max_size() {
        // Try to create array larger than 1M elements
        let input = b"*2000000\r\n";
        let result = parse_input(input).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }
}
