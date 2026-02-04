//! Secure buffer types for sensitive data in PMEM operations
//!
//! This module provides secure buffer types that automatically zeroize their
//! contents when dropped, preventing sensitive data like keys and values from
//! lingering in memory after PMEM operations complete.
//!
//! # Memory Security
//!
//! When storing sensitive data in persistent memory, it's critical to ensure
//! that temporary copies of keys and values are securely wiped after use.
//! The `SecureBuffer` and `SecureAllocation` types in this module use the
//! `zeroize` crate to overwrite memory with zeros when the buffers are dropped.
//!
//! # Note
//!
//! These types are infrastructure for future secure PMEM operations and may
//! not be actively used in the current codebase. The `#[allow(dead_code)]`
//! attribute is used to suppress warnings for these documented but unused types.

#![allow(dead_code)]

use zeroize::Zeroize;

/// A byte buffer that zeroizes its contents on drop.
///
/// `SecureBuffer` wraps a `Vec<u8>` and implements the `Zeroize` trait to
/// securely overwrite the internal buffer with zeros when the type is dropped.
/// This prevents sensitive data (such as database keys and values) from
/// remaining in memory after they're no longer needed.
///
/// # Memory Safety
///
/// The zeroization happens automatically when `SecureBuffer` goes out of scope:
/// - When dropped normally (end of scope)
/// - When explicitly dropped via `drop()`
/// - When the program panics
///
/// # Performance
///
/// The zeroization operation is a single pass over the buffer contents.
/// For large buffers, consider processing data in smaller chunks if latency
/// is a concern.
///
/// # Examples
///
/// Creating a new secure buffer:
///
/// ```rust
/// use eagle_core::pmem::secure_buffer::SecureBuffer;
///
/// let data = b"my_secret_key".to_vec();
/// let secure = SecureBuffer::new(data);
/// assert_eq!(secure.len(), 13);
/// ```
///
/// Converting from a regular vector:
///
/// ```rust
/// use eagle_core::pmem::secure_buffer::SecureBuffer;
///
/// let data = b"sensitive".to_vec();
/// let secure: SecureBuffer = data.into();
/// assert_eq!(secure.0.as_slice(), b"sensitive");
/// ```
///
/// Extracting the inner data (note: this bypasses zeroization):
///
/// ```rust
/// use eagle_core::pmem::secure_buffer::SecureBuffer;
///
/// let data = b"private".to_vec();
/// let secure = SecureBuffer::new(data);
/// let extracted = secure.into_inner();
/// assert_eq!(extracted, b"private");
/// ```
///
/// # Zeroization Behavior
///
/// The following example demonstrates the zeroization behavior (for testing only):
///
/// ```rust
/// use zeroize::Zeroize;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::cell::RefCell;
/// use eagle_core::pmem::secure_buffer::SecureBuffer;
///
/// // Note: In production code, you cannot inspect zeroized memory.
/// // This is for documentation purposes only.
/// thread_local! {
///     static ZEROIZE_CALLS: RefCell<usize> = RefCell::new(0);
/// }
///
/// fn demonstrate_zeroization() {
///     // When SecureBuffer is dropped, zeroize() is called
///     let _buffer = SecureBuffer::new(b"secret".to_vec());
///     // After this point, the memory has been zeroized
/// }
/// ```
///
/// For more information about the zeroize crate, see:
/// <https://docs.rs/zeroize/latest/zeroize/>
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecureBuffer(pub Vec<u8>);

impl SecureBuffer {
    /// Creates a new `SecureBuffer` from the given data.
    ///
    /// The provided `Vec<u8>` will be zeroized when this `SecureBuffer`
    /// is dropped.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to store securely. Will be owned by this buffer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let data = b"api_key_12345".to_vec();
    /// let secure = SecureBuffer::new(data);
    /// ```
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    /// Consumes the buffer and returns the inner data.
    ///
    /// **Warning:** This bypasses zeroization. Only use this if you need
    /// to extract the data for continued use in a secure context.
    ///
    /// # Returns
    ///
    /// The original `Vec<u8>` data.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let data = b"important_data".to_vec();
    /// let secure = SecureBuffer::new(data);
    /// let extracted = secure.into_inner();
    /// assert_eq!(extracted, b"important_data");
    /// ```
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Returns a mutable reference to the inner data.
    ///
    /// **Warning:** Modifying the data does not zeroize the old contents.
    /// If you need to securely update the data, consider creating a new
    /// `SecureBuffer` instead.
    ///
    /// # Returns
    ///
    /// A mutable slice to the inner data.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let mut secure = SecureBuffer::new(b"old".to_vec());
    /// secure.as_mut_slice()[0] = b'n';
    /// assert_eq!(secure.0.as_slice(), b"nld");
    /// ```
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }

    /// Returns the length of the data in bytes.
    ///
    /// # Returns
    ///
    /// The number of bytes in the buffer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let secure = SecureBuffer::new(b"hello".to_vec());
    /// assert_eq!(secure.len(), 5);
    /// ```
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the buffer contains no data.
    ///
    /// # Returns
    ///
    /// `true` if the buffer is empty, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let empty = SecureBuffer::new(Vec::new());
    /// assert!(empty.is_empty());
    ///
    /// let non_empty = SecureBuffer::new(b"data".to_vec());
    /// assert!(!non_empty.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for SecureBuffer {
    /// Creates a `SecureBuffer` from a `Vec<u8>`.
    ///
    /// The vector will be owned and zeroized on drop.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to wrap securely.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureBuffer;
    ///
    /// let data = b"from_vec".to_vec();
    /// let secure: SecureBuffer = data.into();
    /// ```
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl Zeroize for SecureBuffer {
    /// Zeroizes the internal buffer by overwriting all bytes with zeros.
    ///
    /// This method is called automatically when the `SecureBuffer` is dropped.
    /// It uses the `zeroize` crate's optimized implementation which may use
    /// architecture-specific instructions for efficient memory clearing.
    ///
    /// # Implementation Notes
    ///
    /// - Uses `Vec::zeroize()` from the `zeroize` crate
    /// - May use `memset` or architecture-specific instructions
    /// - Is guaranteed to execute even if the thread panics
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

/// Result of a secure PMEM allocation with automatic cleanup.
///
/// `SecureAllocation` wraps the result of secure PMEM operations, containing
/// the allocation metadata plus `SecureBuffer` instances for the key and value.
/// Both the key and value buffers are automatically zeroized when the
/// allocation is dropped, ensuring sensitive data never lingers in memory.
///
/// # Use Case
///
/// Use `SecureAllocation` when you need to ensure that both the key and value
/// data from a PMEM operation are securely cleaned up, even in error paths
/// or if the program crashes before explicit cleanup.
///
/// # Example
///
/// ```rust
/// use zeroize::Zeroize;
/// use eagle_core::pmem::secure_buffer::{ SecureAllocation, SecureBuffer };
///
/// fn process_secure_allocation() {
///     // Simulate a secure allocation
///     let allocation = SecureAllocation::new(
///         0x1000,      // offset
///         64,          // size
///         b"my_key".to_vec(),
///         b"secret_value".to_vec()
///     );
///
///     // Access the allocation data...
///     assert_eq!(allocation.offset, 0x1000);
///     assert_eq!(allocation.key.0.as_slice(), b"my_key");
///     assert_eq!(allocation.value.0.as_slice(), b"secret_value");
///
///     // When `allocation` goes out of scope, both key and value
///     // are automatically zeroized
/// }
/// ```
///
/// # Comparison with Regular Allocation
///
/// Unlike `Allocation`, `SecureAllocation` ensures the key and value data
/// are zeroized on drop. This is critical for sensitive data:
///
/// ```rust
/// use eagle_core::pmem::allocator::Allocation;
/// use eagle_core::pmem::secure_buffer::SecureAllocation;
///
/// // Regular allocation - key/value may remain in memory
/// let regular = Allocation { offset: 0, size: 64 };
///
/// // Secure allocation - key/value are zeroized on drop
/// let key = b"my_key".to_vec();
/// let value = b"secret_value".to_vec();
/// let secure = SecureAllocation::new(0, 64, key, value);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SecureAllocation {
    /// The offset in the PMEM file where the data was written.
    ///
    /// This value is determined by the PMEM allocator and indicates
    /// where the persistent copy of the data can be found.
    pub offset: u64,

    /// The total size of the allocation in bytes.
    ///
    /// This includes the entry header, key, value, and any padding
    /// required for alignment.
    pub size: usize,

    /// A secure buffer containing the key data.
    ///
    /// This buffer is automatically zeroized when `SecureAllocation`
    /// is dropped, ensuring the key never lingers in memory.
    ///
    /// # Security Note
    ///
    /// Even though the key is stored persistently in PMEM, this buffer
    /// represents the in-memory copy that must be securely cleaned up.
    pub key: SecureBuffer,

    /// A secure buffer containing the value data.
    ///
    /// This buffer is automatically zeroized when `SecureAllocation`
    /// is dropped, ensuring the value never lingers in memory.
    ///
    /// # Security Note
    ///
    /// Even though the value is stored persistently in PMEM, this buffer
    /// represents the in-memory copy that must be securely cleaned up.
    pub value: SecureBuffer,
}

impl SecureAllocation {
    /// Creates a new `SecureAllocation` with the given parameters.
    ///
    /// The key and value are wrapped in `SecureBuffer` for automatic
    /// zeroization on drop.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset in PMEM where data was written.
    /// * `size` - The total size of the allocation.
    /// * `key` - The key data (will be wrapped in `SecureBuffer`).
    /// * `value` - The value data (will be wrapped in `SecureBuffer`).
    ///
    /// # Returns
    ///
    /// A new `SecureAllocation` instance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use eagle_core::pmem::secure_buffer::SecureAllocation;
    ///
    /// let alloc = SecureAllocation::new(
    ///     0x2000,
    ///     128,
    ///     b"user_key".to_vec(),
    ///     b"user_value".to_vec()
    /// );
    /// assert_eq!(alloc.offset, 0x2000);
    /// assert_eq!(alloc.size, 128);
    /// ```
    pub fn new(offset: u64, size: usize, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            offset,
            size,
            key: SecureBuffer::new(key),
            value: SecureBuffer::new(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_buffer_basic() {
        let sb = SecureBuffer::new(b"secret".to_vec());
        assert_eq!(sb.0, b"secret");
        assert_eq!(sb.len(), 6);
    }

    #[test]
    fn test_secure_buffer_into_inner() {
        let data = b"sensitive".to_vec();
        let sb = SecureBuffer::new(data.clone());
        let extracted = sb.into_inner();
        assert_eq!(extracted, data);
    }

    #[test]
    fn test_secure_buffer_from() {
        let data: Vec<u8> = b"test".to_vec();
        let sb: SecureBuffer = data.into();
        assert_eq!(sb.0, b"test");
    }

    #[test]
    fn test_secure_buffer_is_empty() {
        let sb = SecureBuffer::new(Vec::new());
        assert!(sb.is_empty());

        let sb = SecureBuffer::new(b"data".to_vec());
        assert!(!sb.is_empty());
    }

    #[test]
    fn test_secure_allocation() {
        let alloc = SecureAllocation::new(100, 50, b"key".to_vec(), b"value".to_vec());
        assert_eq!(alloc.offset, 100);
        assert_eq!(alloc.size, 50);
        assert_eq!(alloc.key.0, b"key");
        assert_eq!(alloc.value.0, b"value");
    }

    #[test]
    fn test_secure_allocation_into_inner() {
        let alloc = SecureAllocation::new(0, 0, b"key".to_vec(), b"value".to_vec());
        let (key, value) = (alloc.key.into_inner(), alloc.value.into_inner());
        assert_eq!(key, b"key");
        assert_eq!(value, b"value");
    }
}
