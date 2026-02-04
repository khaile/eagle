//! Snapshot Management
//!
//! Handles creating and loading snapshots of the database.
//!
//! # Format (Version 2 with compression support)
//!
//! ```text
//! +----------------+
//! | Magic (5B)     |  "EAGLE"
//! +----------------+
//! | Version (4B)   |  u32 little-endian
//! +----------------+
//! | Flags (1B)     |  Bit 0: compression enabled
//! +----------------+
//! | Timestamp (8B) |  u64 Unix millis
//! +----------------+
//! | [Entries...]   |  Variable (optionally compressed)
//! +----------------+
//! | EOF Marker     |  0xFF
//! +----------------+
//! | Entry Count    |  u64
//! +----------------+
//! | Checksum (4B)  |  CRC32 of all preceding data
//! +----------------+
//! ```
//!
//! When compression is enabled (flag bit 0 set), the entries section
//! is LZ4 compressed. The checksum covers the compressed data.

use crate::error::StorageError;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 5] = b"EAGLE";
const VERSION: u32 = 2; // Bumped for compression support
const VERSION_V1: u32 = 1; // Legacy version without compression

/// Snapshot format flags
pub mod flags {
    /// Bit 0: Compression enabled (LZ4)
    pub const COMPRESSION: u8 = 0b0000_0001;
}

/// Entry types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    String = 0,
    Hash = 1,
}

impl TryFrom<u8> for EntryType {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EntryType::String),
            1 => Ok(EntryType::Hash),
            _ => Err(StorageError::Snapshot(format!(
                "Invalid entry type: {}",
                value
            ))),
        }
    }
}

/// Options for creating snapshots
#[derive(Debug, Clone, Default)]
pub struct SnapshotOptions {
    /// Enable LZ4 compression (requires "compression" feature)
    pub compress: bool,
}

/// Writer for snapshots
pub struct SnapshotWriter<W: Write> {
    writer: W,
    hasher: Hasher,
    count: u64,
}

impl<W: Write> SnapshotWriter<W> {
    /// Create a new snapshot writer with default options (no compression)
    pub fn new(writer: W) -> Result<Self, StorageError> {
        Self::with_options(writer, SnapshotOptions::default())
    }

    /// Create a new snapshot writer with the specified options
    pub fn with_options(mut writer: W, options: SnapshotOptions) -> Result<Self, StorageError> {
        #[cfg(feature = "compression")]
        let flags = if options.compress {
            flags::COMPRESSION
        } else {
            0
        };

        #[cfg(not(feature = "compression"))]
        let flags = 0;

        #[cfg(not(feature = "compression"))]
        if options.compress {
            return Err(StorageError::Config(
                "Compression requested but 'compression' feature is not enabled".to_string(),
            ));
        }

        // Write header
        writer.write_all(MAGIC).map_err(StorageError::Io)?;
        writer
            .write_u32::<LittleEndian>(VERSION)
            .map_err(StorageError::Io)?;
        writer.write_u8(flags).map_err(StorageError::Io)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        writer
            .write_u64::<LittleEndian>(timestamp)
            .map_err(StorageError::Io)?;

        let mut hasher = Hasher::new();
        hasher.update(MAGIC);
        hasher.update(&VERSION.to_le_bytes());
        hasher.update(&[flags]);
        hasher.update(&timestamp.to_le_bytes());

        Ok(Self {
            writer,
            hasher,
            count: 0,
        })
    }

    /// Write string entry
    pub fn write_string(
        &mut self,
        key: &[u8],
        value: &[u8],
        expiry: Option<u64>,
    ) -> Result<(), StorageError> {
        self.write_entry_header(EntryType::String, key, expiry)?;

        // Write value
        self.writer
            .write_u32::<LittleEndian>(value.len() as u32)
            .map_err(StorageError::Io)?;
        self.writer.write_all(value).map_err(StorageError::Io)?;

        self.hasher.update(&(value.len() as u32).to_le_bytes());
        self.hasher.update(value);

        self.count += 1;
        Ok(())
    }

    /// Write hash entry
    pub fn write_hash(
        &mut self,
        key: &[u8],
        fields: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
        expiry: Option<u64>,
    ) -> Result<(), StorageError> {
        self.write_entry_header(EntryType::Hash, key, expiry)?;

        // Buffer fields to count them first?
        // Or just write a placeholder for count and seek back?
        // Since W might not be seekable (e.g. BufWriter), we should probably collect.
        // But for large hashes, collecting is bad.
        // Alternative: Use a special format for streaming fields, or just collect.
        // Given this is an in-memory DB, collecting fields of a single hash is probably fine.
        let fields_vec: Vec<_> = fields.collect();

        self.writer
            .write_u32::<LittleEndian>(fields_vec.len() as u32)
            .map_err(StorageError::Io)?;
        self.hasher.update(&(fields_vec.len() as u32).to_le_bytes());

        for (field, value) in fields_vec {
            self.writer
                .write_u32::<LittleEndian>(field.len() as u32)
                .map_err(StorageError::Io)?;
            self.writer.write_all(&field).map_err(StorageError::Io)?;
            self.hasher.update(&(field.len() as u32).to_le_bytes());
            self.hasher.update(&field);

            self.writer
                .write_u32::<LittleEndian>(value.len() as u32)
                .map_err(StorageError::Io)?;
            self.writer.write_all(&value).map_err(StorageError::Io)?;
            self.hasher.update(&(value.len() as u32).to_le_bytes());
            self.hasher.update(&value);
        }

        self.count += 1;
        Ok(())
    }

    /// Write hash entry with a known field count (streaming-friendly)
    ///
    /// This avoids collecting all fields into memory by requiring the caller
    /// to provide the field count upfront.
    pub fn write_hash_streaming<I>(
        &mut self,
        key: &[u8],
        field_count: u32,
        fields: I,
        expiry: Option<u64>,
    ) -> Result<(), StorageError>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        self.write_entry_header(EntryType::Hash, key, expiry)?;

        self.writer
            .write_u32::<LittleEndian>(field_count)
            .map_err(StorageError::Io)?;
        self.hasher.update(&field_count.to_le_bytes());

        let mut actual_count = 0u32;
        for (field, value) in fields {
            self.writer
                .write_u32::<LittleEndian>(field.len() as u32)
                .map_err(StorageError::Io)?;
            self.writer.write_all(&field).map_err(StorageError::Io)?;
            self.hasher.update(&(field.len() as u32).to_le_bytes());
            self.hasher.update(&field);

            self.writer
                .write_u32::<LittleEndian>(value.len() as u32)
                .map_err(StorageError::Io)?;
            self.writer.write_all(&value).map_err(StorageError::Io)?;
            self.hasher.update(&(value.len() as u32).to_le_bytes());
            self.hasher.update(&value);

            actual_count += 1;
        }

        if actual_count != field_count {
            return Err(StorageError::Snapshot(format!(
                "Field count mismatch: expected {}, got {}",
                field_count, actual_count
            )));
        }

        self.count += 1;
        Ok(())
    }

    fn write_entry_header(
        &mut self,
        entry_type: EntryType,
        key: &[u8],
        expiry: Option<u64>,
    ) -> Result<(), StorageError> {
        // Type
        self.writer
            .write_u8(entry_type as u8)
            .map_err(StorageError::Io)?;
        self.hasher.update(&[entry_type as u8]);

        // Key
        self.writer
            .write_u32::<LittleEndian>(key.len() as u32)
            .map_err(StorageError::Io)?;
        self.writer.write_all(key).map_err(StorageError::Io)?;
        self.hasher.update(&(key.len() as u32).to_le_bytes());
        self.hasher.update(key);

        // Expiry (u64, 0 = none) - wait, 0 is valid timestamp. Use -1 equivalent?
        // Or specific flag?
        // Let's use 0 for none, since epoch 0 is 1970 and we likely won't have keys expiring then.
        // But to be safe, let's use a flag byte? No, let's use 0 for None.
        let exp = expiry.unwrap_or(0);
        self.writer
            .write_u64::<LittleEndian>(exp)
            .map_err(StorageError::Io)?;
        self.hasher.update(&exp.to_le_bytes());

        Ok(())
    }

    pub fn finish(mut self) -> Result<W, StorageError> {
        // Write footer (checksum)
        // We didn't write entry count in header because we stream.
        // We can append entry count at end? Or just rely on EOF?
        // Let's write a special "End of Snapshot" marker entry instead of count in header.
        // Marker: Type = 255

        self.writer.write_u8(255).map_err(StorageError::Io)?; // EOF marker
        self.hasher.update(&[255]);

        // Write entry count for verification
        self.writer
            .write_u64::<LittleEndian>(self.count)
            .map_err(StorageError::Io)?;
        self.hasher.update(&self.count.to_le_bytes());

        let checksum = self.hasher.finalize();
        self.writer
            .write_u32::<LittleEndian>(checksum)
            .map_err(StorageError::Io)?;

        self.writer.flush().map_err(StorageError::Io)?;
        Ok(self.writer)
    }
}

/// Compressed snapshot writer (wraps an LZ4 encoder)
#[cfg(feature = "compression")]
pub struct CompressedSnapshotWriter<W: Write> {
    inner: SnapshotWriter<lz4_flex::frame::FrameEncoder<W>>,
}

#[cfg(feature = "compression")]
impl<W: Write> CompressedSnapshotWriter<W> {
    /// Create a new compressed snapshot writer
    pub fn new(writer: W) -> Result<Self, StorageError> {
        let encoder = lz4_flex::frame::FrameEncoder::new(writer);
        let inner = SnapshotWriter::with_options(encoder, SnapshotOptions { compress: true })?;
        Ok(Self { inner })
    }

    /// Write string entry
    pub fn write_string(
        &mut self,
        key: &[u8],
        value: &[u8],
        expiry: Option<u64>,
    ) -> Result<(), StorageError> {
        self.inner.write_string(key, value, expiry)
    }

    /// Write hash entry
    pub fn write_hash(
        &mut self,
        key: &[u8],
        fields: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
        expiry: Option<u64>,
    ) -> Result<(), StorageError> {
        self.inner.write_hash(key, fields, expiry)
    }

    /// Write hash entry with streaming (known field count)
    pub fn write_hash_streaming<I>(
        &mut self,
        key: &[u8],
        field_count: u32,
        fields: I,
        expiry: Option<u64>,
    ) -> Result<(), StorageError>
    where
        I: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        self.inner
            .write_hash_streaming(key, field_count, fields, expiry)
    }

    /// Finish writing and flush the compressed stream
    pub fn finish(self) -> Result<(), StorageError> {
        // Finish inner writer, getting back the encoder
        let encoder = self.inner.finish()?;
        // Finish the encoder to write end of frame
        encoder
            .finish()
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
        Ok(())
    }
}

/// Reader for snapshots
pub struct SnapshotReader<R: Read> {
    reader: R,
    hasher: Hasher,
    pub timestamp: u64,
    flags: u8,
}

impl<R: Read> SnapshotReader<R> {
    pub fn new(mut reader: R) -> Result<Self, StorageError> {
        let mut magic = [0u8; 5];
        reader.read_exact(&mut magic).map_err(StorageError::Io)?;
        if &magic != MAGIC {
            return Err(StorageError::Snapshot(
                "Invalid snapshot format: magic mismatch".to_string(),
            ));
        }

        let version = reader
            .read_u32::<LittleEndian>()
            .map_err(StorageError::Io)?;

        // Handle version differences
        let flags = if version >= VERSION {
            reader.read_u8().map_err(StorageError::Io)?
        } else if version == VERSION_V1 {
            0 // V1 had no flags, no compression
        } else {
            return Err(StorageError::Snapshot(format!(
                "Unsupported snapshot version: {}",
                version
            )));
        };

        let timestamp = reader
            .read_u64::<LittleEndian>()
            .map_err(StorageError::Io)?;

        let mut hasher = Hasher::new();
        hasher.update(MAGIC);
        hasher.update(&version.to_le_bytes());
        if version >= VERSION {
            hasher.update(&[flags]);
        }
        hasher.update(&timestamp.to_le_bytes());

        Ok(Self {
            reader,
            hasher,
            timestamp,
            flags,
        })
    }

    /// Returns the Unix timestamp (in milliseconds) when this snapshot was created
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns true if this snapshot is compressed
    pub fn is_compressed(&self) -> bool {
        self.flags & flags::COMPRESSION != 0
    }

    /// Iterate over entries
    /// Callback receives: (EntryType, Key, Value/Fields, Expiry)
    pub fn read_entries<F>(&mut self, mut callback: F) -> Result<u64, StorageError>
    where
        F: FnMut(EntryType, Vec<u8>, SnapshotValue, Option<u64>) -> Result<(), StorageError>,
    {
        let mut count = 0;

        loop {
            let type_byte = match self.reader.read_u8() {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Unexpected EOF before end marker
                    return Err(StorageError::Snapshot("Unexpected EOF".to_string()));
                }
                Err(e) => return Err(StorageError::Io(e)),
            };

            self.hasher.update(&[type_byte]);

            if type_byte == 255 {
                break; // EOF
            }

            let entry_type = EntryType::try_from(type_byte)?;

            // Read Key
            let key_len = self
                .reader
                .read_u32::<LittleEndian>()
                .map_err(StorageError::Io)?;
            let mut key = vec![0u8; key_len as usize];
            self.reader.read_exact(&mut key).map_err(StorageError::Io)?;
            self.hasher.update(&key_len.to_le_bytes());
            self.hasher.update(&key);

            // Read Expiry
            let expiry_raw = self
                .reader
                .read_u64::<LittleEndian>()
                .map_err(StorageError::Io)?;
            self.hasher.update(&expiry_raw.to_le_bytes());
            let expiry = if expiry_raw == 0 {
                None
            } else {
                Some(expiry_raw)
            };

            let value = match entry_type {
                EntryType::String => {
                    let val_len = self
                        .reader
                        .read_u32::<LittleEndian>()
                        .map_err(StorageError::Io)?;
                    let mut val = vec![0u8; val_len as usize];
                    self.reader.read_exact(&mut val).map_err(StorageError::Io)?;
                    self.hasher.update(&val_len.to_le_bytes());
                    self.hasher.update(&val);
                    SnapshotValue::String(val)
                }
                EntryType::Hash => {
                    let field_count = self
                        .reader
                        .read_u32::<LittleEndian>()
                        .map_err(StorageError::Io)?;
                    self.hasher.update(&field_count.to_le_bytes());

                    let mut fields = Vec::with_capacity(field_count as usize);
                    for _ in 0..field_count {
                        let f_len = self
                            .reader
                            .read_u32::<LittleEndian>()
                            .map_err(StorageError::Io)?;
                        let mut f = vec![0u8; f_len as usize];
                        self.reader.read_exact(&mut f).map_err(StorageError::Io)?;
                        self.hasher.update(&f_len.to_le_bytes());
                        self.hasher.update(&f);

                        let v_len = self
                            .reader
                            .read_u32::<LittleEndian>()
                            .map_err(StorageError::Io)?;
                        let mut v = vec![0u8; v_len as usize];
                        self.reader.read_exact(&mut v).map_err(StorageError::Io)?;
                        self.hasher.update(&v_len.to_le_bytes());
                        self.hasher.update(&v);

                        fields.push((f, v));
                    }
                    SnapshotValue::Hash(fields)
                }
            };

            callback(entry_type, key, value, expiry)?;
            count += 1;
        }

        // Read and verify footer
        let expected_count = self
            .reader
            .read_u64::<LittleEndian>()
            .map_err(StorageError::Io)?;
        self.hasher.update(&expected_count.to_le_bytes());

        if count != expected_count {
            return Err(StorageError::Snapshot(format!(
                "Entry count mismatch: read {}, expected {}",
                count, expected_count
            )));
        }

        let stored_checksum = self
            .reader
            .read_u32::<LittleEndian>()
            .map_err(StorageError::Io)?;
        let calculated_checksum = self.hasher.clone().finalize(); // clone to avoid move if we needed hasher later

        if stored_checksum != calculated_checksum {
            return Err(StorageError::Snapshot(
                "Snapshot checksum mismatch".to_string(),
            ));
        }

        Ok(count)
    }
}

/// Value types stored in snapshots
pub enum SnapshotValue {
    /// String value with its byte content
    String(Vec<u8>),
    /// Hash map containing field-value pairs
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_snapshot_roundtrip() -> Result<(), StorageError> {
        let mut buffer = Vec::new();

        // Write
        {
            let mut writer = SnapshotWriter::new(&mut buffer)?;
            writer.write_string(b"key1", b"value1", None)?;
            writer.write_string(b"key2", b"value2", Some(1234567890000))?;
            writer.write_hash(
                b"hash1",
                vec![
                    (b"field1".to_vec(), b"val1".to_vec()),
                    (b"field2".to_vec(), b"val2".to_vec()),
                ]
                .into_iter(),
                None,
            )?;
            writer.finish()?;
        }

        // Read
        {
            let cursor = Cursor::new(&buffer);
            let mut reader = SnapshotReader::new(cursor)?;

            let mut entries = Vec::new();
            reader.read_entries(|entry_type, key, value, expiry| {
                entries.push((entry_type, key, value, expiry));
                Ok(())
            })?;

            assert_eq!(entries.len(), 3);

            // Verify first entry
            assert_eq!(entries[0].0, EntryType::String);
            assert_eq!(entries[0].1, b"key1");
            if let SnapshotValue::String(v) = &entries[0].2 {
                assert_eq!(v, b"value1");
            } else {
                panic!("Expected String value");
            }
            assert_eq!(entries[0].3, None);

            // Verify second entry with expiry
            assert_eq!(entries[1].3, Some(1234567890000));

            // Verify hash entry
            assert_eq!(entries[2].0, EntryType::Hash);
            if let SnapshotValue::Hash(fields) = &entries[2].2 {
                assert_eq!(fields.len(), 2);
            } else {
                panic!("Expected Hash value");
            }
        }

        Ok(())
    }

    #[test]
    fn test_snapshot_streaming_hash() -> Result<(), StorageError> {
        let mut buffer = Vec::new();

        // Write with streaming
        {
            let mut writer = SnapshotWriter::new(&mut buffer)?;
            writer.write_hash_streaming(
                b"hash1",
                2,
                vec![
                    (b"f1".to_vec(), b"v1".to_vec()),
                    (b"f2".to_vec(), b"v2".to_vec()),
                ]
                .into_iter(),
                None,
            )?;
            writer.finish()?;
        }

        // Read
        {
            let cursor = Cursor::new(&buffer);
            let mut reader = SnapshotReader::new(cursor)?;
            let count = reader.read_entries(|_, _, value, _| {
                if let SnapshotValue::Hash(fields) = value {
                    assert_eq!(fields.len(), 2);
                }
                Ok(())
            })?;
            assert_eq!(count, 1);
        }

        Ok(())
    }

    #[test]
    fn test_snapshot_checksum_failure() {
        let mut buffer = Vec::new();

        // Write valid snapshot
        {
            let mut writer = SnapshotWriter::new(&mut buffer).unwrap();
            writer.write_string(b"key", b"value", None).unwrap();
            writer.finish().unwrap();
        }

        // Corrupt a byte
        if buffer.len() > 20 {
            buffer[20] ^= 0xFF;
        }

        // Read should fail
        let cursor = Cursor::new(&buffer);
        let reader = SnapshotReader::new(cursor);
        if let Ok(mut r) = reader {
            let result = r.read_entries(|_, _, _, _| Ok(()));
            assert!(result.is_err());
        }
    }
}
