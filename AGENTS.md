# AGENTS.md - EagleDB Development Guide

EagleDB is a high-performance Redis-compatible key-value store with persistent memory (PMEM) support. Written in Rust 2024 edition.

## Workspace Structure

```
eagle/
├── Cargo.toml           # Workspace root
├── crates/
│   ├── core/            # Storage, RESP protocol, PMEM, sync primitives
│   └── server/          # Commands, networking, configuration, metrics
├── examples/            # Example applications
└── scripts/             # Build and benchmark scripts
```

## Commands (using just)

```bash
# Build
just build               # Build workspace
just check               # Quick check (no codegen)

# Test
just test                # Run all tests
just tests <pattern>     # Run tests matching pattern (e.g., just tests hash)
just nextest             # Run with cargo-nextest (faster)
just nextests <pattern>  # Run nextest with pattern

# Lint
just rust-lint           # Format + check + clippy (modifies files)
just lint-check          # CI mode (check only, no modifications)

# Other
just server              # Run server on port 6380
just benchmark <name>    # Run benchmark (pmem_bench, memory_bench, redis_comparison)
just clean               # Clean build artifacts
```

### Running a Single Test

```bash
# By test name
cargo test --workspace test_basic_persistence

# By pattern
just tests hash_incr

# In specific crate
cargo test -p eagle-core test_name

# With output
cargo test --workspace -- --nocapture test_name
```

## Code Style

### Imports (ordered, separated by blank lines)

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;

use eagle_core::error::CommandError;
use eagle_core::store::Store;

use crate::config::ServerConfig;
```

### Naming Conventions

- **Types**: `PascalCase` (`Store`, `CommandError`, `PmemAllocator`)
- **Functions**: `snake_case` (`new_pmem`, `set_with_options`)
- **Constants**: `SCREAMING_SNAKE_CASE` (`MAX_KEY_SIZE`, `PMEM_MAGIC`)
- **Lifetimes**: Short lowercase (`'a`, `'guard`)

### Error Handling

Use `eagle_core::error::{CommandError, StorageError, Result}`:

```rust
// Command errors (Redis-compatible)
if args.len() != 2 {
    return Err(CommandError::wrong_arity("hget").into());
}

// Storage errors
if size > MAX_SIZE {
    return Err(StorageError::LimitExceeded("...".to_string()));
}

// Custom errors with thiserror
#[derive(Error, Debug)]
pub enum AllocError {
    #[error("Out of space")]
    OutOfSpace,
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
}
```

### Async & Concurrency

```rust
// Async traits
#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn execute(&self, store: &Store) -> Result<RespValue>;
}

// Concurrent maps: DashMap
// Sync locks: parking_lot::RwLock (faster than std)
// Memory reclamation: crossbeam epoch

// Atomics ordering
counter.load(Ordering::Relaxed)   // Fast reads, no ordering
flag.load(Ordering::Acquire)      // Acquiring shared state
flag.store(true, Ordering::Release) // Publishing shared state
```

### Command Parsing Pattern

```rust
impl MyCommand {
    pub fn parse(mut args: Vec<RespValue>) -> Result<Self> {
        if args.len() != 2 {
            return Err(CommandError::wrong_arity("mycmd").into());
        }
        let value = args.pop().unwrap().into_bytes()?;
        let key = args.pop().unwrap().into_bytes()?;
        Ok(Self { key, value })
    }
}
```

### Entry API for Atomic Operations

```rust
use dashmap::mapref::entry::Entry;

match self.data.entry(key.to_vec()) {
    Entry::Occupied(mut e) => { e.get_mut().update(); }
    Entry::Vacant(e) => { e.insert((value, meta)); }
}
```

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `foundations` | Cloudflare's telemetry framework (metrics, logging) |
| `dashmap` | Concurrent hash map |
| `parking_lot` | Fast locks |
| `crossbeam` | Epoch-based reclamation |
| `tokio` | Async runtime |
| `bytes` | Byte containers |
| `thiserror` | Error derive |
| `tracing` | Structured logging |
| `memmap2` | Memory-mapped files |

## Toolchain

- **Rust**: 1.93.0 (`rust-toolchain.toml`)
- **Edition**: 2024
- **Line width**: 100 chars
- **Indentation**: 4 spaces

## CI Requirements

All PRs must pass: `just check`, `just nextest`, `just lint-check`, `cargo audit`

## Common Gotchas

1. **Expired keys**: Check `entry.1.is_expired()` before using data
2. **PMEM writes**: Use persist barriers for crash consistency
3. **Hash vs String**: Check both `data` and `hash_data` maps
4. **Epoch guards**: Hold guards when accessing shared structures
5. **RESP format**: Commands must return Redis-compatible values
