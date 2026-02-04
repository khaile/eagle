# EagleDB Milestones

This document outlines the development roadmap for EagleDB, organized by priority and category.

## Current State Summary

### Implemented Features

| Component              | Status   | Notes                                                   |
|:-----------------------|:---------|:--------------------------------------------------------|
| Core Storage (DashMap) | Complete | In-memory store with PMEM persistence                   |
| Dash Storage Engine    | Complete | Extendible hashing, segment splitting, overflow chains  |
| PMEM Allocator         | Complete | Memory-mapped files, crash-consistent writes, checksums |
| Recovery Manager       | Complete | Entry recovery, integrity checks, compaction            |
| RESP Protocol          | Complete | Parser and encoder for Redis protocol                   |
| Epoch-based GC         | Complete | Safe memory reclamation                                 |
| Cluster Mode           | Partial  | Raft-like consensus, leader election, heartbeat         |
| Metrics                | Basic    | Prometheus exporter setup                               |

### Commands Implemented

- **Key-Value**: `GET`, `SET`, `DEL`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`,
  `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`
- **Hash**: `HSET`, `HGET`, `HDEL`
- **TTL**: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
- **Generic**: `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`, `KEYS`, `SCAN`, `DBSIZE`, `RANDOMKEY`, `FLUSHDB`
- **Cluster**: `CLUSTER INFO`, `CLUSTER NODES`

### Recent Changes

#### Workspace Reorganization - COMPLETED

**Priority**: High | **Effort**: Medium | **Estimate**: 2-3 days | **Status**: Done

Split the monolithic codebase into a modular workspace:

- **eagle-core**: Core storage engine, RESP protocol, PMEM handling, and synchronization
- **eagle-server**: Network layer, command processing, metrics, and server functionality
- **Root crate**: Orchestrates components and provides main binary

**Files reorganized**:

- `src/*` → `crates/core/src/*` - Core functionality (store, resp, pmem, sync)
- `src/*` → `crates/server/src/*` - Server functionality (net, command, metrics, config)
- `tests/*` → `crates/core/tests/*` - Core tests and benchmarks
- `benches/*` → `crates/core/benches/*` - Performance benchmarks

**Acceptance Criteria**:

- [x] All code successfully moved to appropriate crates
- [x] Dependencies properly managed between crates
- [x] All tests pass in new structure
- [x] Benchmarks run correctly from new locations
- [x] Workspace builds and functions correctly

### Known TODOs

- Update remaining documentation references to old file paths
- Review and update import statements across workspace

---

## Phase 1: Redis Compatibility

Focus on achieving broader Redis command compatibility.

### Milestone 1.1: SET Command Options - COMPLETED

**Priority**: High | **Effort**: Medium | **Estimate**: 2-3 days | **Status**: Done

Implement missing SET command options:

| Option            | Description                                     | Status |
|:------------------|:------------------------------------------------|:-------|
| `EX seconds`      | Set expiration in seconds                       | Done   |
| `PX milliseconds` | Set expiration in milliseconds                  | Done   |
| `EXAT timestamp`  | Set expiration as Unix timestamp (seconds)      | Done   |
| `PXAT timestamp`  | Set expiration as Unix timestamp (milliseconds) | Done   |
| `NX`              | Only set if key does not exist                  | Done   |
| `XX`              | Only set if key already exists                  | Done   |
| `GET`             | Return the old value                            | Done   |
| `KEEPTTL`         | Retain TTL of existing key                      | Done   |

**Files modified**:

- `crates/server/src/command/set_get.rs` - Parse and handle options
- `crates/core/src/store/mod.rs` - Added `set_with_options`, TTL support in `EntryMeta`

**Acceptance Criteria**:

- [x] All SET options parse correctly
- [x] `NX`/`XX` conditional writes work
- [x] `GET` returns previous value
- [x] Unit tests for each option (9 new tests)
- [x] Integration test with redis-cli (manual testing pending)

---

### Milestone 1.2: TTL/Expiration System - COMPLETED

**Priority**: High | **Effort**: High | **Estimate**: 3-5 days | **Status**: Done

Create a key expiration system:

1. **Store TTL metadata** - Add `expires_at: Option<Instant>` to `EntryMeta` - DONE
2. **Lazy expiration** - Check TTL on read, return `None` if expired - DONE
3. **Background expiration** - Periodic task to clean expired keys - DONE
4. **PMEM persistence** - Store expiration timestamp in PMEM entries - DONE

**Commands implemented**:

| Command                   | Description                           | Status   |
|:--------------------------|:--------------------------------------|:---------|
| ---------                 | -------------                         | -------- |
| `EXPIRE key seconds`      | Set key TTL in seconds                | Done     |
| `PEXPIRE key ms`          | Set key TTL in milliseconds           | Done     |
| `EXPIREAT key timestamp`  | Set expiration as Unix timestamp      | Done     |
| `PEXPIREAT key timestamp` | Set expiration as Unix timestamp (ms) | Done     |
| `TTL key`                 | Get remaining TTL in seconds          | Done     |
| `PTTL key`                | Get remaining TTL in milliseconds     | Done     |
| `PERSIST key`             | Remove expiration from key            | Done     |

**Files created/modified**:

- `crates/core/src/store/mod.rs` - Added TTL field to `EntryMeta`, expiration methods
- `crates/core/src/store/expiration.rs` - Background expiration task
- `crates/server/src/command/ttl.rs` - TTL-related commands
- `crates/server/src/command/mod.rs` - Registered new TTL commands
- `crates/core/src/pmem/layout.rs` - Added `expires_at` field to `EntryHeader`
- `crates/core/src/pmem/allocator.rs` - Added `allocate_entry_with_expiry`, updated `iter_entries`

**Acceptance Criteria**:

- [x] Keys expire correctly after TTL
- [x] Expired keys not returned on GET
- [x] Background task cleans expired keys
- [x] TTL survives restart (PMEM persistence)
- [x] All TTL commands work correctly

---

### Milestone 1.3: Additional Core Commands - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2-3 days | **Status**: Done

**String Commands**:

| Command                          | Description                   | Status   |
|:---------------------------------|:------------------------------|:---------|
| ---------                        | -------------                 | -------- |
| `MGET key [key ...]`             | Get multiple keys             | Done     |
| `MSET key value [key value ...]` | Set multiple key-value pairs  | Done     |
| `INCR key`                       | Increment integer value by 1  | Done     |
| `DECR key`                       | Decrement integer value by 1  | Done     |
| `INCRBY key increment`           | Increment by specified amount | Done     |
| `DECRBY key decrement`           | Decrement by specified amount | Done     |
| `INCRBYFLOAT key increment`      | Increment by float            | Done     |
| `APPEND key value`               | Append value to string        | Done     |
| `STRLEN key`                     | Get string length             | Done     |
| `GETRANGE key start end`         | Get substring                 | Done     |
| `SETRANGE key offset value`      | Overwrite part of string      | Done     |

**Generic Commands**:

| Command                | Description                    | Status   |
|:-----------------------|:-------------------------------|:---------|
| ---------              | -------------                  | -------- |
| `EXISTS key [key ...]` | Check if keys exist            | Done     |
| `TYPE key`             | Get key type                   | Done     |
| `RENAME key newkey`    | Rename a key                   | Done     |
| `RENAMENX key newkey`  | Rename if newkey doesn't exist | Done     |

**Connection Commands**:

| Command          | Description       | Status   |
|:-----------------|:------------------|:---------|
| ---------        | -------------     | -------- |
| `PING [message]` | Test connectivity | Done     |
| `ECHO message`   | Echo message back | Done     |

**Files created**:

- `crates/server/src/command/string.rs` - String manipulation commands
- `crates/server/src/command/generic.rs` - Generic key commands
- `crates/server/src/command/connection.rs` - Connection commands
- `crates/server/src/command/mod.rs` - Registered new commands
- `crates/core/src/store/mod.rs` - Added store methods for new operations

**Acceptance Criteria**:

- [x] All commands parse and execute correctly
- [x] Atomic increment/decrement operations with overflow checking
- [x] Proper error handling for type mismatches
- [x] Unit tests for each command (15 store tests + 30+ command tests)

---

### Milestone 1.3.1: Security & Correctness Fixes - COMPLETED

**Priority**: High | **Effort**: Medium | **Estimate**: 1-2 days | **Status**: Done

Security hardening and race condition fixes discovered during code review.

**Race Condition Fixes**:

| Issue                   | Fix                                     | Status   |
|:------------------------|:----------------------------------------|:---------|
| -------                 | -----                                   | -------- |
| SET NX/XX TOCTOU        | Use Entry API for atomic check-and-set  | Done     |
| INCRBY/INCRBYFLOAT race | Atomic read-modify-write with Entry API | Done     |
| APPEND/SETRANGE race    | Atomic read-modify-write with Entry API | Done     |
| PMEM crash consistency  | Validate entries during recovery scan   | Done     |

**Security Hardening**:

| Issue                           | Fix                                   | Status   |
|:--------------------------------|:--------------------------------------|:---------|
| -------                         | -----                                 | -------- |
| RESP DoS via large bulk strings | 512MB limit on bulk strings           | Done     |
| RESP DoS via large arrays       | 1M element limit on arrays            | Done     |
| Connection exhaustion           | Configurable max connections (10,000) | Done     |
| Idle connection DoS             | Read timeout (300s default)           | Done     |
| PMEM corruption crash           | Bounds checking during iteration      | Done     |

**Files modified**:

- `crates/core/src/store/mod.rs` - Entry API for atomic operations
- `crates/core/src/resp/parser.rs` - Size limits
- `crates/server/src/net/mod.rs` - Connection limits
- `crates/server/src/net/connection.rs` - Read timeouts
- `crates/core/src/pmem/allocator.rs` - Bounds checking, crash consistency

---

### Milestone 1.3.2: Performance Optimizations - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 1 day | **Status**: Done

Hot path optimizations to reduce allocations and improve throughput.

**Optimizations Implemented**:

| Optimization                             | Location            | Impact                              |
|:-----------------------------------------|:--------------------|:------------------------------------|
| -------------                            | ----------          | --------                            |
| Direct RespValue encoding                | `resp/encoder.rs`   | Avoid intermediate Value conversion |
| `itoa` for integer formatting            | `resp/encoder.rs`   | Faster than `format!()`             |
| Reuse encoder buffer per connection      | `net/connection.rs` | Avoid allocation per response       |
| Zero-allocation command parsing          | `command/mod.rs`    | Byte comparison instead of String   |
| Skip eviction policy check when disabled | `store/mod.rs`      | Avoid RwLock on every write         |
| Avoid clone in INCRBY/INCRBYFLOAT        | `store/mod.rs`      | Use `str::from_utf8` directly       |

**Files modified**:

- `crates/core/src/resp/encoder.rs` - Add `encode_resp()` method, use `itoa`
- `crates/server/src/net/connection.rs` - Per-connection encoder buffer
- `crates/server/src/command/mod.rs` - Byte-level command matching
- `crates/core/src/store/mod.rs` - Fast-path eviction check, avoid clones

**Benchmarking**:

- Added `benches/redis_comparison.rs` for Redis vs EagleDB benchmarks
- Added `.github/workflows/bench.yml` for CI benchmarking

---

### Milestone 1.4: Key Scanning Commands - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2 days | **Status**: Done

| Command                                     | Description                       | Status   |
|:--------------------------------------------|:----------------------------------|:---------|
| ---------                                   | -------------                     | -------- |
| `KEYS pattern`                              | Find keys matching glob pattern   | Done     |
| `SCAN cursor [MATCH pattern] [COUNT count]` | Incrementally iterate keys        | Done     |
| `DBSIZE`                                    | Return number of keys in database | Done     |
| `RANDOMKEY`                                 | Return a random key               | Done     |
| `FLUSHDB [ASYNC\|SYNC]`                     | Delete all keys                   | Done     |

**Files created**:

- `crates/core/src/pattern.rs` - Glob pattern matching (supports `*`, `?`, `[abc]`, `[a-z]`, `[^abc]`, escape chars)
- `crates/server/src/command/scan.rs` - Scanning commands implementation

**Files modified**:

- `crates/core/src/store/mod.rs` - Added key iteration methods (`keys`, `scan`, `dbsize`, `random_key`, `flush_db`)
- `crates/server/src/command/mod.rs` - Registered new commands
- `crates/core/src/lib.rs` - Added `mod pattern`

**Acceptance Criteria**:

- [x] `KEYS` supports glob patterns (`*`, `?`, `[abc]`)
- [x] `SCAN` returns cursor for pagination
- [x] `SCAN` is safe for large datasets
- [x] Warning logged for `KEYS` on large datasets

---

### Milestone 1.5: Complete Hash Commands - COMPLETED

**Priority**: Low | **Effort**: Low | **Estimate**: 1-2 days

| Command                            | Description                  |
|:-----------------------------------|:-----------------------------|
| ---------                          | -------------                |
| `HGETALL key`                      | Get all fields and values    |
| `HMSET key field value [...]`      | Set multiple fields          |
| `HMGET key field [field ...]`      | Get multiple fields          |
| `HLEN key`                         | Get number of fields         |
| `HEXISTS key field`                | Check if field exists        |
| `HKEYS key`                        | Get all field names          |
| `HVALS key`                        | Get all values               |
| `HINCRBY key field increment`      | Increment field by integer   |
| `HINCRBYFLOAT key field increment` | Increment field by float     |
| `HSETNX key field value`           | Set field only if not exists |

**Files modified**:

- `crates/server/src/command/hash.rs` - Added all 10 hash command handlers
- `crates/server/src/command/mod.rs` - Registered new commands in Command enum and parsing
- `crates/core/src/store/mod.rs` - Added store methods: `hmset`, `hmget`, `hlen`, `hexists`, `hkeys`,
  `hvals`, `hincrby`, `hincrbyfloat`, `hsetnx`

**Acceptance Criteria**:

- [x] All hash commands work correctly
- [x] Proper handling of non-existent keys/fields
- [x] Unit tests for each command

---

## Phase 2: Production Readiness

### Milestone 2.1: Code Cleanup - COMPLETED

**Priority**: Medium | **Effort**: Low | **Estimate**: 1 day | **Status**: Done

- [x] Remove global `#![allow(dead_code)]` from src/main.rs
- [x] Remove global `#![allow(unused_imports)]` from src/main.rs
- [x] Remove global `#![allow(unused_variables)]` from src/main.rs
- [x] Add targeted `#[allow(dead_code)]` to infrastructure modules reserved for future use
- [x] Run `cargo clippy` and fix all warnings
- [x] Ensure `cargo fmt` passes

**Files modified**:

- `crates/server/src/main.rs` - Removed global allow attributes
- `crates/core/src/error.rs` - Added module-level allow for future error types
- `crates/core/src/hash/mod.rs` - Added module-level allow for helper functions
- `crates/server/src/metrics/collector.rs` - Added module-level allow for metrics infrastructure
- `crates/core/src/pmem/allocator.rs` - Added module-level allow for PMEM methods
- `crates/core/src/pmem/layout.rs` - Added module-level allow for layout structures
- `crates/core/src/pmem/recovery.rs` - Added module-level allow for recovery infrastructure
- `crates/core/src/resp/encoder.rs` - Added module-level allow for encoder infrastructure
- `crates/core/src/resp/mod.rs` - Added allow for RespError variants
- `crates/core/src/sync/epoch.rs` - Added module-level allow for epoch-based GC
- `crates/core/src/sync/rcu.rs` - Added module-level allow for RCU primitives
- `crates/core/src/store/mod.rs` - Added targeted allows for infrastructure methods
- `crates/core/src/store/dash/*.rs` - Added module-level allows for Dash store
- `crates/core/src/store/memory.rs` - Added module-level allow for memory tracking
- `crates/core/src/store/policy.rs` - Added module-level allow for eviction policies
- `crates/core/src/store/expiration.rs` - Added module-level allow for background task
- `crates/core/src/pattern.rs` - Added targeted allows for helper methods
- `crates/server/src/net/cluster.rs` - Added allow for unused field
- `crates/server/src/command/mod.rs` - Added allow for trait method
- `crates/server/src/command/scan.rs` - Added allows for constructor methods

### Milestone 2.2: Error Handling Improvements - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2 days | **Status**: Done

- [x] Standardize error types across modules
- [x] Add detailed error messages for client errors
- [x] Implement proper RESP error responses
- [x] Add error metrics/counters

**Error Infrastructure Created**:

| Component              | Description                                                | Status   |
|:-----------------------|:-----------------------------------------------------------|:---------|
| `ErrorKind` enum       | 18 Redis-compatible error categories                       | Done     |
| `CommandError` struct  | Redis-compatible error formatting                          | Done     |
| Error constructors     | Helper methods: wrong_arity, wrong_type, etc.              | Done     |
| `From` implementations | Auto conversion from std and parse errors                  | Done     |
| Error metrics          | Prometheus error tracking via `record_error_by_kind()`     | Done     |

**Commands Migrated**:

| File            | Commands                                  | Status   |
|:----------------|:------------------------------------------|:---------|
| `string.rs`     | String ops: MGET/MSET, INCR/DECR, etc.    | Done     |
| `set_get.rs`    | GET, SET                                  | Done     |
| `hash.rs`       | Hash ops: HSET/HGET, HDEL, HGETALL, etc.  | Done     |
| `ttl.rs`        | TTL ops: EXPIRE, PEXPIRE, TTL, PTTL, etc. | Done     |
| `scan.rs`       | KEYS, SCAN, DBSIZE, RANDOMKEY, FLUSHDB    | Done     |
| `generic.rs`    | EXISTS, TYPE, RENAME, RENAMENX            | Done     |
| `connection.rs` | PING, ECHO                                | Done     |

**Files modified**:

- `crates/core/src/error.rs` - New `ErrorKind`, `CommandError` infrastructure
- `crates/server/src/metrics/collector.rs` - Added `record_error_by_kind()` method
- `crates/server/src/command/string.rs` - Migrated to `CommandError`
- `crates/server/src/command/set_get.rs` - Migrated to `CommandError`
- `crates/server/src/command/hash.rs` - Migrated to `CommandError`
- `crates/server/src/command/ttl.rs` - Migrated to `CommandError`
- `crates/server/src/command/scan.rs` - Migrated to `CommandError`
- `crates/server/src/command/generic.rs` - Migrated to `CommandError`
- `crates/server/src/command/connection.rs` - Migrated to `CommandError`

**Acceptance Criteria**:

- [x] All command handlers use standardized `CommandError`
- [x] Error messages are Redis-compatible (e.g., "ERR wrong number of arguments for 'get' command")
- [x] All 310 tests pass
- [x] No clippy warnings

### Milestone 2.3: Logging and Observability - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2 days | **Status**: Done

- [x] Add structured logging throughout
- [x] Add request tracing with correlation IDs
- [x] Expand Prometheus metrics (latency histograms, error rates)
- [x] Add health check endpoint

**Structured Logging Infrastructure**:

| Component        | Description                                           | Status   |
|:-----------------|:------------------------------------------------------|:---------|
| -----------      | -------------                                         | -------- |
| `TracingConfig`  | Configurable tracing subscriber with JSON/text output | Done     |
| `RequestContext` | Request context with correlation IDs                  | Done     |
| CLI arguments    | `--log-level`, `--log-json` for logging configuration | Done     |
| Connection spans | Span-based tracing for connections and requests       | Done     |

**Prometheus Metrics Expanded**:

| Metric                           | Description                                    | Status   |
|:---------------------------------|:-----------------------------------------------|:---------|
| --------                         | -------------                                  | -------- |
| `eagle_command_duration_seconds` | Command execution latency histogram by command | Done     |
| `eagle_command_errors_total`     | Command errors by command name                 | Done     |
| `eagle_keys_total`               | Total keys gauge                               | Done     |
| `eagle_expired_keys_total`       | Expired keys counter                           | Done     |
| Metric descriptions              | Prometheus HELP text for all metrics           | Done     |

**Health Check Endpoints**:

| Endpoint      | Description                    | Status   |
|:--------------|:-------------------------------|:---------|
| ----------    | -------------                  | -------- |
| `GET /health` | Full health status (JSON)      | Done     |
| `GET /ready`  | Readiness probe for Kubernetes | Done     |
| `GET /live`   | Liveness probe for Kubernetes  | Done     |

**Files created/modified**:

- `crates/server/src/tracing_config.rs` - New tracing configuration and request context module
- `crates/server/src/net/health.rs` - New health check HTTP server
- `crates/server/src/metrics/collector.rs` - Expanded metrics with descriptions
- `crates/server/src/metrics/mod.rs` - Metric initialization with descriptions
- `crates/server/src/net/connection.rs` - Request tracing with spans
- `crates/server/src/net/mod.rs` - Connection span instrumentation
- `crates/server/src/main.rs` - CLI args for logging and health, health server startup
- `Cargo.toml` - Added tracing-subscriber features (json, env-filter)

---

## Phase 3: Persistence Features

### Milestone 3.1: Hash Data PMEM Persistence - COMPLETED

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2-3 days | **Status**: Done

Currently hash data (`HSET`, etc.) is not persisted to PMEM.

- [x] Design PMEM layout for hash entries
- [x] Persist hash data on write
- [x] Recover hash data on startup
- [x] Update crash recovery tests

**Files modified**:

- `crates/core/src/pmem/layout.rs` - Added `HASH_ENTRY_PREFIX` and checksum calculation
- `crates/core/src/pmem/allocator.rs` - Added `allocate_hash_entry` and `iter_hash_entries`
- `crates/core/src/store/mod.rs` - Updated hash operations to persist to PMEM, added recovery logic
- `crates/core/tests/crash_recovery.rs` - Added comprehensive hash persistence tests

**Acceptance Criteria**:

- [x] `HSET`, `HMSET`, `HINCRBY`, `HSETNX` persist data to PMEM
- [x] Hash data survives restart
- [x] Field updates are handled correctly (deduplication on recovery)
- [x] Corrupted hash entries are detected and skipped
- [x] All tests pass

### Milestone 3.2: Snapshots and Backups

**Priority**: Low | **Effort**: High | **Estimate**: 3-5 days | **Status**: Done

- [x] Implement `BGSAVE` command for async snapshots
- [x] `SAVE` command for synchronous snapshots
- [x] `LASTSAVE` command to get timestamp of last save
- [x] Custom EagleDB snapshot format with checksums
- [x] Point-in-time recovery via `load_snapshot()`
- [x] Snapshot scheduling with configurable interval
- [x] Automatic snapshot rotation (retain N snapshots)
- [x] CLI arguments for snapshot configuration

**Files created/modified**:

- `crates/core/src/store/snapshot.rs` - Snapshot format (already existed)
- `crates/core/src/store/mod.rs` - Added `SnapshotConfig`, `SnapshotScheduler`, snapshot methods
- `crates/server/src/command/persistence.rs` - Persistence commands (already existed)
- `crates/server/src/main.rs` - CLI arguments for snapshot configuration
- `crates/core/tests/crash_recovery.rs` - Added snapshot tests

**Acceptance Criteria**:

- [x] `SAVE` blocks until snapshot is complete
- [x] `BGSAVE` returns immediately, saves in background
- [x] `LASTSAVE` returns Unix timestamp of last successful save
- [x] Snapshots include string and hash data with TTL
- [x] Snapshots can be loaded to restore database state
- [x] Automatic snapshots can be scheduled via CLI
- [x] Old snapshots can be automatically rotated
- [x] All 321+ tests pass

---

## Phase 4: Cluster Maturity

### Milestone 4.1: Hash Slot Routing

**Priority**: Low | **Effort**: High | **Estimate**: 5-7 days

- [ ] Implement Redis Cluster hash slots (16384 slots)
- [ ] Slot assignment and migration
- [ ] `-MOVED` and `-ASK` redirections
- [ ] Cluster client compatibility

### Milestone 4.2: Full Raft Implementation

**Priority**: Low | **Effort**: Very High | **Estimate**: 2-3 weeks

- [ ] Complete log replication
- [ ] Snapshotting and log compaction
- [ ] Membership changes
- [ ] Leadership transfer

---

## Phase 5: Performance Optimization

### Milestone 5.1: NUMA Optimization

**Priority**: Low | **Effort**: High | **Estimate**: 1 week

- [ ] NUMA-aware memory allocation
- [ ] Thread-to-core pinning
- [ ] Cross-NUMA access minimization

### Milestone 5.2: Connection Pooling

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2-3 days

- [ ] Connection reuse for cluster communication
- [ ] Client connection limits
- [ ] Idle connection timeout

### Milestone 5.3: Comprehensive Benchmarking

**Priority**: Medium | **Effort**: Medium | **Estimate**: 2 days

- [ ] redis-benchmark compatibility
- [ ] Latency percentile tracking (p50, p99, p999)
- [ ] Throughput tests under various workloads
- [ ] Comparison benchmarks with Redis

---

## Implementation Priority Summary

| Phase   | Milestone                    | Priority   | Effort    | Estimate   | Status   |
|:--------|:-----------------------------|:-----------|:----------|:-----------|:---------|
| ------- | -----------                  | ---------- | --------  | ---------- | -------- |
| 1       | 1.1 SET Command Options      | High       | Medium    | 2-3 days   | Done     |
| 1       | 1.2 TTL/Expiration System    | High       | High      | 3-5 days   | Done     |
| 1       | 1.3 Additional Core Commands | Medium     | Medium    | 2-3 days   | Done     |
| 1       | 1.4 Key Scanning Commands    | Medium     | Medium    | 2 days     | Done     |
| 1       | 1.5 Complete Hash Commands   | Low        | Low       | 1-2 days   | Done     |
| 2       | 2.1 Code Cleanup             | Medium     | Low       | 1 day      | Done     |
| 2       | 2.2 Error Handling           | Medium     | Medium    | 2 days     | Done     |
| 2       | 2.3 Logging/Observability    | Medium     | Medium    | 2 days     | Done     |
| 3       | 3.1 Hash PMEM Persistence    | Medium     | Medium    | 2-3 days   | Done     |
| 3       | 3.2 Snapshots                | Low        | High      | 3-5 days   |          |
| 4       | 4.1 Hash Slot Routing        | Low        | High      | 5-7 days   |          |
| 4       | 4.2 Full Raft                | Low        | Very High | 2-3 weeks  |          |
| 5       | 5.1 NUMA Optimization        | Low        | High      | 1 week     |          |
| 5       | 5.2 Connection Pooling       | Medium     | Medium    | 2-3 days   |          |
| 5       | 5.3 Benchmarking             | Medium     | Medium    | 2 days     |          |

---

## Quick Start: Next Steps

---

## Last updated: January 2026
