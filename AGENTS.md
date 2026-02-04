# AGENTS.md - EagleDB Development Guide

This document provides essential information for AI coding agents working on the EagleDB codebase.

## Project Overview

EagleDB is a high-performance Redis-compatible key-value store with persistent memory (PMEM) support. The project is written in Rust 2024 edition and uses a workspace structure.

## Workspace Structure

```bash
eagle/
├── Cargo.toml           # Workspace root (main binary)
├── crates/
│   ├── core/            # Core library: storage, RESP protocol, PMEM, sync primitives
│   └── server/          # Server: commands, networking, configuration, metrics
├── examples/            # Example applications
├── scripts/             # Build and benchmark scripts
└── docs/                # Documentation
```

## Build Commands

```bash
# Build entire workspace
cargo build --workspace

# Build with all features
cargo build --workspace --all-features

# Build release
cargo build --workspace --release

# Quick build check (faster, no codegen)
cargo check --workspace --all-features --all-targets
```

## Test Commands

```bash
# Run all tests
cargo test --workspace

# Run all tests with all features
cargo test --workspace --all-features

# Run a single test by name
cargo test --workspace test_basic_persistence

# Run tests in a specific crate
cargo test -p eagle-core
cargo test -p eagle-server

# Run tests matching a pattern
cargo test --workspace -- --test-threads=1 hash

# Run a specific integration test file
cargo test --test crash_recovery

# Run tests with output
cargo test --workspace -- --nocapture

# Using just (if installed)
just test                    # Run all tests
just tests <pattern>         # Run tests matching pattern
just nextest                 # Run with cargo-nextest
just nextests <pattern>      # Run nextest with pattern
```

## Lint Commands

```bash
# Format code
cargo fmt --all

# Check formatting (CI mode)
cargo fmt --all -- --check

# Run clippy with warnings as errors
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Combined lint check (recommended before commit)
just rust-lint   # Runs: fmt, check, clippy
```

## Benchmark Commands

```bash
# Run specific benchmark
cargo bench -p eagle-core --bench pmem_bench
cargo bench -p eagle-core --bench memory_bench
cargo bench -p eagle-core --bench redis_comparison

# Using just
just benchmark pmem_bench
```

## Run Server

```bash
# Run with default settings (port 6379)
cargo run -p eagle

# Run on custom port
cargo run -p eagle -- --port 6380

# Using just
just server  # Runs on port 6380
```

## Code Style Guidelines

### Imports

Order imports in this sequence, separated by blank lines:

1. Standard library (`std::`)
2. External crates (alphabetically)
3. Workspace crates (`eagle_core::`, `eagle_server::`)
4. Local modules (`crate::`, `super::`, `self::`)

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;

use eagle_core::error::CommandError;
use eagle_core::store::Store;

use crate::config::ServerConfig;
use super::CommandHandler;
```

### Formatting

- **Line width**: 100 characters (configured in `.taplo.toml`)
- **Indentation**: 4 spaces
- **Trailing commas**: Yes, in multi-line constructs
- Use `cargo fmt` before committing

### Types and Naming

- **Types**: `PascalCase` - `Store`, `CommandError`, `PmemAllocator`
- **Functions/methods**: `snake_case` - `new_pmem`, `set_with_options`
- **Constants**: `SCREAMING_SNAKE_CASE` - `MAX_KEY_SIZE`, `PMEM_MAGIC`
- **Lifetimes**: Short lowercase - `'a`, `'guard`
- **Type aliases**: Use for complex types - `type HashData = DashMap<Vec<u8>, RwLock<HashMap<...>>>`

### Error Handling

Use the custom error types defined in `eagle_core::error`:

```rust
use eagle_core::error::{CommandError, StorageError, Result};

// For command errors (Redis-compatible)
fn parse_args() -> Result<Self> {
    if args.len() != 2 {
        return Err(CommandError::wrong_arity("hget").into());
    }
    Ok(...)
}

// For storage errors
fn allocate() -> Result<Allocation> {
    if size > MAX_SIZE {
        return Err(StorageError::LimitExceeded("...".to_string()));
    }
    Ok(...)
}

// Use thiserror for custom error enums
#[derive(Error, Debug)]
pub enum AllocError {
    #[error("Memory allocation failed: out of space")]
    OutOfSpace,
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
}
```

### Async Code

- Use `tokio` for async runtime
- Use `async_trait` for async trait methods
- Prefer `tokio::sync` primitives in async contexts

```rust
use async_trait::async_trait;

#[async_trait]
pub trait CommandHandler: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(&self, store: &Store) -> Result<RespValue>;
}
```

### Concurrency

- Use `DashMap` for concurrent hash maps
- Use `parking_lot::RwLock` for synchronous locks (faster than std)
- Use `crossbeam` for epoch-based memory reclamation
- Use atomics with appropriate `Ordering`:

```rust
// Fast reads where ordering doesn't matter
self.counter.load(Ordering::Relaxed)

// Acquiring a lock or reading shared state
self.flag.load(Ordering::Acquire)

// Releasing a lock or publishing shared state
self.flag.store(true, Ordering::Release)
```

### Documentation

- Add module-level docs with `//!` for public modules
- Document public APIs with `///`
- Use doc comments for non-obvious invariants

```rust
//! PMEM Allocator - Memory-mapped persistent memory allocator
//!
//! This allocator provides:
//! - Memory-mapped file access for persistence
//! - Atomic bump-pointer allocation

/// Create a new store with PMEM persistence
///
/// If the PMEM file exists, data will be recovered from it.
pub fn new_pmem(path: &str, size_mb: usize) -> Result<Self> { ... }
```

### Test Patterns

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operation() {
        let store = Store::new().unwrap();
        store.set(b"key".to_vec(), b"value".to_vec()).unwrap();
        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_async_operation() -> Result<()> {
        let store = Store::new()?;
        let cmd = Command::Set(...);
        let result = cmd.execute(&store).await?;
        assert!(matches!(result, RespValue::SimpleString(_)));
        Ok(())
    }
}
```

### Feature Flags

- `compression` - Enable LZ4 compression support

```rust
#[cfg(feature = "compression")]
fn compress(data: &[u8]) -> Vec<u8> { ... }
```

### Common Patterns

**Command parsing pattern:**

```rust
impl MyCommand {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != EXPECTED {
            return Err(CommandError::wrong_arity("mycommand").into());
        }
        let value = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;
        Ok(Self { key, value })
    }
}
```

**Entry API for atomic operations:**
```rust
use dashmap::mapref::entry::Entry;

match self.data.entry(key.to_vec()) {
    Entry::Occupied(mut occupied) => {
        let entry = occupied.get_mut();
        // modify existing
    }
    Entry::Vacant(vacant) => {
        vacant.insert((value, meta));
    }
}
```

## Key Crates and Dependencies

| Crate | Purpose |
|-------|---------|
| `dashmap` | Concurrent hash map |
| `parking_lot` | Fast synchronization primitives |
| `crossbeam` | Epoch-based memory reclamation, channels |
| `tokio` | Async runtime |
| `bytes` | Efficient byte containers |
| `thiserror` | Derive Error implementations |
| `anyhow` | Flexible error handling |
| `tracing` | Structured logging |
| `memmap2` | Memory-mapped files |

## Toolchain

- **Rust**: 1.93.0 (see `rust-toolchain.toml`)
- **Edition**: 2024
- **Components**: rustfmt, clippy

## CI Checks

All PRs must pass:

1. `cargo check --workspace --all-features`
2. `cargo test --workspace --all-features`
3. `cargo fmt --all -- --check`
4. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
5. `cargo audit` (security audit)

## Common Gotchas

1. **Expired keys**: Always check `entry.1.is_expired()` before using data
2. **PMEM writes**: Use persist barriers for crash consistency
3. **Hash vs String keys**: Check both `data` and `hash_data` maps
4. **Epoch guards**: Hold guards when accessing shared data structures
5. **RESP format**: Commands must return Redis-compatible RESP values
