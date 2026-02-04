//! Persistent Memory (PMEM) Module
//!
//! This module provides support for persistent memory storage:
//!
//! - `allocator` - Memory-mapped file allocator with crash-consistent writes
//! - `layout` - Data structures and memory layout definitions
//! - `recovery` - Crash recovery and data integrity verification
//! - `secure_buffer` - Secure buffer types with automatic memory zeroization

pub mod allocator;
pub mod layout;
pub mod recovery;
pub mod secure_buffer;

pub use allocator::PmemAllocator;
pub use recovery::RecoveryManager;
