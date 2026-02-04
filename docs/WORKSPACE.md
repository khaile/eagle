# Workspace Structure

This document describes the workspace organization of EagleDB after the modular reorganization.

## Overview

EagleDB is organized as a Cargo workspace to improve code organization, reduce compile times,
and enable better modularity. The workspace consists of two main crates plus a root crate
that orchestrates the components.

## Crate Organization

```text
eagle/
├── Cargo.toml              # Workspace configuration
├── justfile                # Task runner commands
├── AGENTS.md               # Development guidelines for AI assistants
├── README.md               # Project overview
├── docs/                   # Documentation
│   ├── API_REFERENCE.md    # Redis-compatible API documentation
│   ├── ARCHITECTURE.md     # System architecture overview
│   ├── MILESTONES.md       # Development roadmap
│   ├── PMEM_SETUP.md       # Persistent memory setup guide
│   └── WORKSPACE.md        # This file - workspace structure
├── crates/
│   ├── core/               # Core storage engine and protocols
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── error.rs    # Error types and handling
│   │   │   ├── hash/       # Hashing utilities
│   │   │   ├── pattern.rs  # Pattern matching for SCAN/KEYS
│   │   │   ├── pmem/       # Persistent memory management
│   │   │   ├── resp/       # Redis protocol implementation
│   │   │   ├── store/      # Storage engine
│   │   │   └── sync/       # Concurrency primitives
│   │   ├── benches/        # Performance benchmarks
│   │   └── tests/          # Core tests and integration tests
│   └── server/             # Network and server components
│       ├── Cargo.toml
│       ├── src/
│       │   ├── lib.rs
│       │   ├── main.rs     # Server entry point
│       │   ├── command/    # Command handlers
│       │   ├── config/     # Configuration management
│       │   ├── metrics/    # Prometheus metrics
│       │   ├── net/        # Network layer
│       │   └── tracing_config.rs # Logging configuration
└── scripts/                # Utility scripts
    └── README.md
```

## Crate Responsibilities

### eagle-core

The core crate contains all the fundamental components that don't depend on network or server-specific functionality:

**Modules:**

- `store/`: Storage engine with dash-based organization, TTL support, and memory management
- `resp/`: Redis protocol parser and encoder
- `pmem/`: Persistent memory allocation, layout, and recovery
- `sync/`: RCU and epoch-based synchronization primitives
- `hash/`: Hashing utilities and functions
- `pattern.rs`: Glob pattern matching for key scanning
- `error.rs`: Core error types and handling

**Dependencies:**

- tokio (async runtime)
- dashmap, hashbrown (high-performance collections)
- crossbeam (concurrency utilities)
- zeroize (secure memory handling)
- No network or server dependencies

### eagle-server

The server crate contains all network and server-specific components:

**Modules:**

- `net/`: Network layer, connection management, cluster communication
- `command/`: Command handlers for all Redis commands
- `config/`: Server and cluster configuration
- `metrics/`: Prometheus metrics collection and export
- `tracing_config.rs`: Structured logging configuration

**Dependencies:**

- eagle-core (core functionality)
- tokio (async runtime)
- clap (CLI argument parsing)
- metrics, metrics-exporter-prometheus (observability)
- serde, serde_json (configuration)

### Root Crate

The root crate provides:

- Main binary that orchestrates core and server components
- Workspace configuration and dependency management
- Development tooling integration

## Building and Testing

### Building Specific Crates

```bash
# Build entire workspace
cargo build

# Build specific crate
cargo build -p eagle-core
cargo build -p eagle-server

# Build with optimizations
cargo build --release
```

### Testing

```bash
# Test entire workspace
cargo test

# Test specific crate
cargo test -p eagle-core
cargo test -p eagle-server

# Run benchmarks (in eagle-core)
cargo bench -p eagle-core
```

## Dependencies

### Workspace Dependencies

Common dependencies are managed at the workspace level in the root `Cargo.toml`:

```toml
[workspace.dependencies]
tokio = { version = "1.49", features = ["full"] }
dashmap = "6.1"
# ... other dependencies
```

This ensures version consistency across crates and allows for easy updates.

### Internal Dependencies

- `eagle-server` depends on `eagle-core`
- `eagle-core` has no internal dependencies
- Both crates share workspace-defined external dependencies

## Development Workflow

### Adding New Features

1. Determine if the feature belongs in core or server:
   - **Core**: Storage, protocol, memory management, synchronization
   - **Server**: Network, command handling, metrics, configuration

2. Implement in the appropriate crate
3. Add tests to the same crate
4. Update workspace dependencies if needed

### Code Organization Guidelines

- Keep core functionality independent of network concerns
- Server components should be thin wrappers around core functionality
- Shared utilities go in the appropriate crate based on their primary use case
- Cross-cutting concerns (like error handling) should be in core

## Benefits of This Structure

1. **Faster Compilation**: Only rebuild what changes
2. **Clear Separation**: Core logic separate from server concerns
3. **Reusability**: Core library can be used independently
4. **Testing**: Focused testing for each component
5. **Dependency Management**: Clear dependency graph
6. **Onboarding**: Easier for new developers to understand

## Migration History

The workspace was created to address:

- Long compilation times due to monolithic structure
- Unclear separation between core and server concerns
- Difficulty in testing individual components
- Complex dependency management

This reorganization maintains all existing functionality while improving developer experience and code organization.
