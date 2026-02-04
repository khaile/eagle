# Architecture

## Overview

EagleDB is a high-performance key-value store optimized for persistent memory (PMEM).
The architecture is organized as a modular workspace with two main crates, designed for
low-latency, high-concurrency workloads with strong durability guarantees.

```bash
┌─────────────────────────────────────────────────────────────┐
│                    Workspace Structure                      │
├─────────────────────────────────────────────────────────────┤
│  eagle/ (root)                                         │
│  ├── eagle-core/    ← Core storage engine & protocols    │
│  └── eagle-server/  ← Network & server components       │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   eagle-server Crate                         │
├─────────────────────────────────────────────────────────────┤
│                      CLI (`clap`)                           │
├─────────────────────────────────────────────────────────────┤
│                   Network Layer (`net/` - `tokio`)          │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐     │
│  │ TCP Server  │  │ Connection  │  │ Cluster Server   │     │
│  │ (accept/IO) │  │ Manager     │  │ (gossip/coord)   │     │
│  └─────────────┘  └─────────────┘  └──────────────────┘     │
├─────────────────────────────────────────────────────────────┤
│                   Command Layer (`command/`)                │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐     │
│  │ KV Cmds     │  │ Hash Cmds   │  │ Cluster Cmds     │     │
│  │ GET/SET/DEL │  │ HGET/HSET   │  │ JOIN/REPL        │     │
│  └─────────────┘  └─────────────┘  └──────────────────┘     │
├─────────────────────────────────────────────────────────────┤
│                   Metrics (`metrics/`)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐     │
│  │ Prometheus  │  │ Collection  │  │ Health Endpoints │     │
│  │ Exporter    │  │ Engine      │  │ & Diagnostics    │     │
│  └─────────────┘  └─────────────┘  └──────────────────┘     │
├─────────────────────────────────────────────────────────────┤
│                   eagle-core Crate                           │
├─────────────────────────────────────────────────────────────┤
│                   RESP Protocol (`resp/`)                   │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │ Parser      │  │ Encoder     │                           │
│  │ (decode)    │  │ (encode)    │                           │
│  └─────────────┘  └─────────────┘                           │
├─────────────────────────────────────────────────────────────┤
│                   Storage Engine (`store/`)                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Dash organization │ Extendible hashing (StoreDash)  │    │
│  │ Buckets / Segments │ Indexing / Lookup             │    │
│  └─────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────┤
│                   Memory Layer (`pmem/`, `sync/`)           │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐     │
│  │ PMEM Alloc  │  │ Epoch GC    │  │ Recovery & WAL   │     │
│  │ (alloc/persist)│ (RCU/epochs)│  │ (replay/repair)  │     │
│  └─────────────┘  └─────────────┘  └──────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### Workspace Organization

EagleDB is structured as a Cargo workspace with two main crates:

- **eagle-core**: Core storage engine, RESP protocol, PMEM handling, and synchronization primitives
- **eagle-server**: Network layer, command processing, metrics, and server functionality
- **Root crate**: Orchestrates the components and provides the main binary

### CLI (`eagle-server`)

Runtime configuration, bootstrapping, and administrative commands using `clap`.

### Network Layer (`net/` - `eagle-server`)

- Accepts connections and performs async I/O using `tokio`.
- Connection lifecycle, timeouts, and request demultiplexing.
- Configurable connection limits (default 10,000) and read timeouts.
- Per-connection encoder buffer reuse for reduced allocations.
- Cluster node communication, gossipping, and membership management.
- Uses `JoinSet` for efficient connection management.

### Command Processing (`command/` - `eagle-server`)

- Maps parsed requests to handlers.
- Implements command-level validation, routing, and basic transactions.
- Zero-allocation command parsing using byte-level comparison.
- Provides handlers for:
  - Key-value: `GET`, `SET`, `DEL`, `MGET`, `MSET`
  - String operations: `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`
  - Generic: `EXISTS`, `TYPE`, `RENAME`, `RENAMENX`
  - TTL: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
  - Hash: `HGET`, `HSET`, `HDEL`
  - Connection: `PING`, `ECHO`
  - Cluster/coordination commands.

### RESP Protocol (`resp/` - `eagle-core`)

- Implements Redis-compatible RESP parsing and encoding.
- Handles protocol errors, incremental parsing, and framing.
- Optimized encoder with buffer reuse and `itoa` for fast integer formatting.
- Size limits for DoS prevention (512MB bulk strings, 1M array elements).

### Storage Engine (`store/` - `eagle-core`)

- Primary in-memory and PMEM-backed data structures.
- Dash-based organization with extendible hashing (`StoreDash`) for scalability.
- Manages buckets, directory metadata, segments, and on-disk/PMEM layout.
- Includes secure buffer handling with `zeroize` for memory security.

### PMEM Handler (`pmem/` - `eagle-core`)

- Allocation and deallocation on persistent memory.
- Ensures writes are flushed and ordered to satisfy durability guarantees.
- Provides recovery primitives and integration with WAL.
- Secure buffer management for sensitive data.

### Synchronization (`sync/` - `eagle-core`)

- Concurrency primitives: RCU (Read-Copy-Update), epoch management.
- Epoch-based garbage collection and safe reclamation for lock-free reads.
- Lightweight coordination for writers.
- Replaced nested DashMap with `RwLock<HashMap>` to eliminate deadlock risk.

### Observability (`metrics/` - `eagle-server`)

- Exposes performance counters, histograms, and tracing hooks.
- Prometheus integration for external monitoring.
- Health endpoints and diagnostics.

## Data Flow (Request -> Response)

1. Client sends bytes to the server (TCP).
2. `net/` layer (eagle-server) accepts bytes and associates them with a connection.
3. `resp/` parser (eagle-core) decodes a command from the byte stream.
4. `command/` layer (eagle-server) validates and routes the command to the correct handler.
5. `store/` engine (eagle-core) performs the needed data operation (read/write/delete).
6. `pmem/` handler (eagle-core) ensures persistence (flushes, WAL writes as needed).
7. The result is encoded by `resp/` encoder (eagle-core) and written back through `net/` (eagle-server).

## Concurrency Model

- Read-dominated workloads:
  - Use RCU for lock-free reads when possible.
  - Readers run without taking heavy locks; writers publish new versions.
- Writers:
  - Use fine-grained synchronization and epoch barriers for safe updates.
- Memory reclamation:
  - Epoch-based GC tracks active readers and delays reclamation until safe.
- NUMA-awareness:
  - Data placement and thread pinning are considered for throughput on multi-socket systems.

## Persistence & Crash Recovery

- Primary storage uses direct PMEM access for low latency persistence.
- A write-ahead-log (WAL) is used to record intent before applying non-atomic updates.
- Atomic primitives (e.g., CAS + persistent flush) are used for critical metadata updates.
- On restart, `pmem/` recovery replays WAL and reconstructs in-memory metadata from durable state.

## Cluster Architecture

- Data distribution:
  - Consistent hashing / DHT model for spreading keys across nodes.
  - Optional replication for fault tolerance.
- Cluster management:
  - Node discovery, heartbeat/health checks, and membership via cluster server.
  - Rebalancing and replication coordination handled by cluster protocol.
- Client routing:
  - Clients may contact any node; node forwards or proxies requests if necessary.

## Operational Considerations

- Monitoring:
  - Expose metrics (latency, throughput, GC stats, PMEM utilization).
- Backups & snapshots:
  - Provide snapshot capabilities integrated with PMEM and WAL checkpoints.
- Upgrades:
  - Support rolling upgrades with compatibility in on-disk/PMEM formats.

## Next Steps

- Document module-level public APIs and invariants for `store/` and `pmem/`.
- Add detailed recovery walkthrough and failure-mode tests.
- Expand cluster protocol documentation (messages, states, and flows).

## References

- PMEM programming models and best practices.
- RCU and epoch-based reclamation literature.
- Design notes for extendible hashing and dash-based storage layouts.
