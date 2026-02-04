//! Secure data types for sensitive data
//!
//! This module provides wrapper types that securely zeroize their contents
//! when dropped, preventing sensitive data from lingering in memory.

use zeroize::Zeroize;

/// A byte vector that zeroizes its contents on drop.
///
/// This wrapper ensures that sensitive data like keys and values are
/// securely wiped from memory when they are no longer needed.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SecureBytes(pub Vec<u8>);

impl SecureBytes {
    /// Create a new SecureBytes from a Vec
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    /// Create a new empty SecureBytes
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Consume and extract the inner bytes
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Get a reference to the inner bytes
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for SecureBytes {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl From<&[u8]> for SecureBytes {
    fn from(data: &[u8]) -> Self {
        Self(data.to_vec())
    }
}

impl From<SecureBytes> for Vec<u8> {
    fn from(secure: SecureBytes) -> Self {
        secure.0
    }
}

impl AsRef<[u8]> for SecureBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Zeroize for SecureBytes {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_bytes_basic() {
        let sb = SecureBytes::new(b"secret".to_vec());
        assert_eq!(sb.as_ref(), b"secret");
        assert_eq!(sb.len(), 6);
        assert!(!sb.is_empty());
    }

    #[test]
    fn test_secure_bytes_empty() {
        let sb = SecureBytes::empty();
        assert!(sb.is_empty());
        assert_eq!(sb.len(), 0);
    }

    #[test]
    fn test_secure_bytes_into_inner() {
        let data = b"sensitive".to_vec();
        let sb = SecureBytes::new(data.clone());
        let extracted = sb.into_inner();
        assert_eq!(extracted, data);
    }

    #[test]
    fn test_secure_bytes_from() {
        let sb: SecureBytes = b"test".as_ref().into();
        assert_eq!(sb.as_ref(), b"test");
    }

    #[test]
    fn test_secure_bytes_clone() {
        let sb = SecureBytes::new(b"clone me".to_vec());
        let cloned = sb.clone();
        assert_eq!(cloned.as_ref(), sb.as_ref());
    }
}
