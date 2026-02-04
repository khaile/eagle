//! Append-Only File (AOF) persistence support.
//!
//! This module implements the AOF persistence mechanism which logs every
//! write operation to ensure point-in-time recovery.
//!
//! # Format
//!
//! ```text
//! +-------------------+
//! | Magic (5B)        |  "EAGLE"
//! +-------------------+
//! | Version (4B)      |  u32 little-endian
//! +-------------------+
//! | [Commands...]     |  Variable
//! +-------------------+
//! ```
//!
//! Each command is encoded as a RESP array with the command and arguments.

use crate::error::StorageError;
use crate::resp::RespValue;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    sync::Arc,
};
use tokio::sync::Mutex;

pub const AOF_MAGIC: &[u8; 5] = b"EAGLE";
pub const AOF_VERSION: u32 = 1;
pub const AOF_HEADER_LEN: usize = 9;
const ONE_MB: i64 = 1024 * 1024;

/// Synchronous RESP parser for AOF replay
struct SyncRespParser<R> {
    reader: R,
}

impl<R: BufRead> SyncRespParser<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }

    fn parse(&mut self) -> Result<Option<RespValue>, StorageError> {
        match self.read_byte() {
            Ok(byte) => match byte {
                b'+' => Ok(Some(self.parse_simple_string()?)),
                b'-' => Ok(Some(self.parse_error()?)),
                b':' => Ok(Some(self.parse_integer()?)),
                b'$' => Ok(Some(self.parse_bulk_string()?)),
                b'*' => Ok(Some(self.parse_array()?)),
                _ => Err(StorageError::Aof(format!(
                    "Invalid RESP type byte: {}",
                    byte
                ))),
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(StorageError::Io(e))
                }
            }
        }
    }

    fn read_byte(&mut self) -> std::io::Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_line(&mut self) -> Result<String, StorageError> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line).map_err(StorageError::Io)?;

        if bytes_read == 0 {
            return Err(StorageError::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }

        // Remove trailing CRLF
        if line.ends_with("\r\n") {
            line.truncate(line.len() - 2);
        } else if line.ends_with('\n') {
            line.truncate(line.len() - 1);
        }

        Ok(line)
    }

    fn parse_simple_string(&mut self) -> Result<RespValue, StorageError> {
        let line = self.read_line()?;
        Ok(RespValue::SimpleString(line))
    }

    fn parse_error(&mut self) -> Result<RespValue, StorageError> {
        let line = self.read_line()?;
        Ok(RespValue::Error(line))
    }

    fn parse_integer(&mut self) -> Result<RespValue, StorageError> {
        let line = self.read_line()?;
        let value = line
            .parse::<i64>()
            .map_err(|_| StorageError::Aof(format!("Invalid integer: {}", line)))?;
        Ok(RespValue::Integer(value))
    }

    fn parse_bulk_string(&mut self) -> Result<RespValue, StorageError> {
        let line = self.read_line()?;
        let length = line
            .parse::<i64>()
            .map_err(|_| StorageError::Aof(format!("Invalid bulk string length: {}", line)))?;

        if length == -1 {
            return Ok(RespValue::NullBulkString);
        }

        if length < 0 {
            return Err(StorageError::Aof(format!(
                "Invalid bulk string length: {}",
                length
            )));
        }

        // Safety limit: 512MB
        if length > 512 * ONE_MB {
            return Err(StorageError::Aof("Bulk string too large".to_string()));
        }

        let length = length as usize;
        let mut data = vec![0u8; length];
        self.reader
            .read_exact(&mut data)
            .map_err(StorageError::Io)?;

        // Read and discard CRLF
        let mut crlf = [0u8; 2];
        self.reader
            .read_exact(&mut crlf)
            .map_err(StorageError::Io)?;
        if crlf != [b'\r', b'\n'] {
            return Err(StorageError::Aof(
                "Missing CRLF after bulk string".to_string(),
            ));
        }

        Ok(RespValue::BulkString(Bytes::from(data)))
    }

    fn parse_array(&mut self) -> Result<RespValue, StorageError> {
        let line = self.read_line()?;
        let length = line
            .parse::<i64>()
            .map_err(|_| StorageError::Aof(format!("Invalid array length: {}", line)))?;

        if length == -1 {
            return Ok(RespValue::NullArray);
        }

        if length < 0 {
            return Err(StorageError::Aof(format!(
                "Invalid array length: {}",
                length
            )));
        }

        // Safety limit: 1M elements
        if length > ONE_MB {
            return Err(StorageError::Aof("Array too large".to_string()));
        }

        let length = length as usize;
        let mut items = VecDeque::with_capacity(length);

        for _ in 0..length {
            match self.parse()? {
                Some(item) => items.push_back(item),
                None => return Err(StorageError::Aof("Unexpected EOF in array".to_string())),
            }
        }

        Ok(RespValue::Array(items))
    }
}

/// AOF writer for logging commands
pub struct AofWriter {
    writer: BufWriter<File>,
}

impl AofWriter {
    /// Create a new AOF writer
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(StorageError::Io)?;
        let mut writer = BufWriter::new(file);

        // Write header if this is a new file
        let metadata = std::fs::metadata(path).map_err(StorageError::Io)?;
        if metadata.len() == 0 {
            writer.write_all(AOF_MAGIC).map_err(StorageError::Io)?;
            writer
                .write_u32::<LittleEndian>(AOF_VERSION)
                .map_err(StorageError::Io)?;
        }

        Ok(Self { writer })
    }

    /// Log a command to the AOF
    pub fn log_command(&mut self, command: &[u8]) -> Result<(), StorageError> {
        self.writer.write_all(command).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Flush the AOF buffer to disk
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.flush().map_err(StorageError::Io)?;
        Ok(())
    }

    /// Sync the AOF file to disk
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.writer.flush().map_err(StorageError::Io)?;
        self.writer.get_mut().sync_all().map_err(StorageError::Io)?;
        Ok(())
    }
}

/// Asynchronous AOF writer wrapper
pub struct AsyncAofWriter {
    inner: Arc<Mutex<AofWriter>>,
}

impl AsyncAofWriter {
    /// Create a new async AOF writer
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let writer = AofWriter::new(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(writer)),
        })
    }

    /// Log a command asynchronously
    pub async fn log_command(&self, command: &[u8]) -> Result<(), StorageError> {
        let mut writer = self.inner.lock().await;
        writer.log_command(command)
    }

    /// Flush the AOF buffer asynchronously
    pub async fn flush(&self) -> Result<(), StorageError> {
        let mut writer = self.inner.lock().await;
        writer.flush()
    }

    /// Sync the AOF file to disk asynchronously
    pub async fn sync(&self) -> Result<(), StorageError> {
        let mut writer = self.inner.lock().await;
        writer.sync()
    }
}

/// AOF reader for replaying commands
pub struct AofReader {
    file: File,
}

impl AofReader {
    /// Create a new AOF reader
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let file = File::open(path).map_err(StorageError::Io)?;
        Ok(Self { file })
    }

    /// Validate and consume the AOF header.
    pub fn validate_header(&mut self) -> Result<(), StorageError> {
        let mut header = [0u8; AOF_HEADER_LEN];
        self.file
            .read_exact(&mut header)
            .map_err(StorageError::Io)?;
        validate_header(&header)
    }

    /// Replay all commands in the AOF
    pub fn replay_commands<F>(&mut self, mut callback: F) -> Result<(), StorageError>
    where
        F: FnMut(RespValue) -> Result<(), StorageError>,
    {
        // Seek to start of commands (skip header)
        self.file
            .seek(SeekFrom::Start(AOF_HEADER_LEN as u64))
            .map_err(StorageError::Io)?;

        let reader = BufReader::new(&mut self.file);
        let mut parser = SyncRespParser::new(reader);

        while let Some(value) = parser.parse()? {
            callback(value)?;
        }

        Ok(())
    }
}

/// Validate a raw AOF header buffer.
pub fn validate_header(header: &[u8]) -> Result<(), StorageError> {
    if header.len() < AOF_HEADER_LEN {
        return Err(StorageError::Aof("AOF header too short".to_string()));
    }

    if &header[..AOF_MAGIC.len()] != AOF_MAGIC {
        return Err(StorageError::Aof("Invalid AOF magic header".to_string()));
    }

    let version_bytes: [u8; 4] = header[5..9]
        .try_into()
        .map_err(|_| StorageError::Aof("Invalid AOF version header".to_string()))?;
    let version = u32::from_le_bytes(version_bytes);
    if version != AOF_VERSION {
        return Err(StorageError::Aof(format!(
            "Unsupported AOF version: {}",
            version
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_aof_writer_creation() -> Result<(), StorageError> {
        let tmpfile = NamedTempFile::new().map_err(StorageError::Io)?;
        let path = tmpfile.path().to_str().unwrap();

        let writer = AofWriter::new(path)?;
        drop(writer);

        // Verify file exists and has header
        let metadata = std::fs::metadata(path).map_err(StorageError::Io)?;
        assert!(metadata.len() >= 9); // Magic + version

        Ok(())
    }

    #[test]
    fn test_aof_replay() -> Result<(), StorageError> {
        use crate::resp::RespValue;
        use bytes::Bytes;

        let tmpfile = NamedTempFile::new().map_err(StorageError::Io)?;
        let path = tmpfile.path().to_str().unwrap();

        // Write some commands
        let mut writer = AofWriter::new(path)?;
        let cmd1 = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let cmd2 = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        writer.log_command(cmd1)?;
        writer.log_command(cmd2)?;
        writer.flush()?;

        // Replay commands
        let mut reader = AofReader::new(path)?;
        reader.validate_header()?;

        let mut commands = Vec::new();
        reader.replay_commands(|cmd| {
            commands.push(cmd);
            Ok(())
        })?;

        assert_eq!(commands.len(), 2);

        // Verify first command (SET key value)
        if let RespValue::Array(args) = &commands[0] {
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], RespValue::BulkString(Bytes::from("SET")));
            assert_eq!(args[1], RespValue::BulkString(Bytes::from("key")));
            assert_eq!(args[2], RespValue::BulkString(Bytes::from("value")));
        } else {
            panic!("Expected array");
        }

        // Verify second command (GET key)
        if let RespValue::Array(args) = &commands[1] {
            assert_eq!(args.len(), 2);
            assert_eq!(args[0], RespValue::BulkString(Bytes::from("GET")));
            assert_eq!(args[1], RespValue::BulkString(Bytes::from("key")));
        } else {
            panic!("Expected array");
        }

        Ok(())
    }
}
