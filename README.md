# EagleDB - High performance PMEM-native store

**EagleDB** is a high-performance key-value store designed specifically for persistent memory (PMEM)
systems. It combines modern architectural design with efficient memory management to deliver
exceptional performance while ensuring data durability.

## Features

- PMEM-native storage engine with zeroization support
- Redis protocol (RESP) compatibility
- High-performance concurrent operations with RCU synchronization
- Crash recovery support and snapshot management
- Cluster support for horizontal scaling
- Comprehensive metrics collection with Prometheus integration via [foundations](https://github.com/cloudflare/foundations)
- NUMA-aware design
- Memory security with secure buffer handling
- Advanced hash-based organization with DashMap optimization

## Architecture

EagleDB is organized as a modular workspace with two main crates:

### Core Library (`eagle-core`)

The core storage engine and fundamental components:

- **Storage Engine**: Dash-based organization using hashbrown::HashMap
- **PMEM Handler**: Efficient persistent memory management with secure buffer handling
- **RESP Protocol**: Redis protocol compatibility layer with full command support
- **Synchronization**: RCU and epoch-based concurrency control for lock-free reads
- **Security**: Memory zeroization and secure buffer management for sensitive data

### Server Components (`eagle-server`)

Network and server-specific functionality:

- **Network Layer**: Handles client connections, protocol management, and cluster coordination
- **Command Processor**: Implementation of various data operations including TTL, scans, and hash operations
- **Metrics**: Performance monitoring and reporting via Cloudflare's [foundations](https://github.com/cloudflare/foundations) telemetry framework with Prometheus export
- **Configuration**: Cluster and server configuration management

## Building

```bash
# Development build (entire workspace)
cargo build

# Build specific crate
cargo build -p eagle-core
cargo build -p eagle-server

# Production build with optimizations
cargo build --release

# Using just (recommended task runner)
just build
```

## Running Tests

```bash
# Run all tests (entire workspace)
cargo test --all-features

# Run tests for specific crate
cargo test -p eagle-core
cargo test -p eagle-server

# Run benchmarks
cargo bench

# Using just (recommended)
just test          # Run all tests
just tests name    # Run a specific test
just nextest       # Run tests with nextest (faster, used in CI)
just run-benches   # Run all benchmarks
```

## Development Tooling

EagleDB uses comprehensive development tooling to ensure code quality:

### Pre-commit Hooks

The project uses "prek" for pre-commit hook management:

Install and set up hooks:

```bash
cargo install prek
prek install
```

Available hooks for:

- **Code formatting**: `cargo fmt` and `taplo` for TOML
- **Linting**: `cargo clippy` with strict warnings
- **CI scripts**: Shell validation, trailing whitespace cleanup
- **Markdown linting**: Consistent documentation formatting

### Task Runner (Just)

Use `just` for common development tasks:

```bash
just --list          # Show all available commands
just rust-lint       # Format and lint code (modifies files)
just lint-check      # Check lint without modifying (CI mode)
just check           # Quick compilation check
just clean           # Clean build artifacts
just server          # Run development server on port 6380
```

### Code Quality Checks

```bash
# Format code
cargo fmt --all

# Check compilation without building
cargo check --all-features

# Run Clippy lints
cargo clippy --all-targets --all-features -- -D warnings

# Security audit
cargo audit
```

## Observability

EagleDB comes with a built-in observability stack using Prometheus and Grafana.

### Starting the Stack

Start Prometheus and Grafana using Docker Compose:

```bash
docker-compose up -d
```

This will start:

- **Grafana**: <http://localhost:3000> (default login: `admin`/`admin`)
- **Prometheus**: <http://localhost:9090>

### Running EagleDB with Metrics

Run EagleDB and expose metrics on a specific port (e.g., 9000) to match the Prometheus configuration:

```bash
PROMETHEUS_PORT=9000 cargo run

# Or using the task runner
PROMETHEUS_PORT=9000 just server
```

### Viewing Dashboards

1. Navigate to Grafana at <http://localhost:3000>.
2. Go to **Dashboards > Manage**.
3. Select **EagleDB Dashboard** to view real-time metrics including:
   - Operations per second (GET/SET/DEL)
   - Latency (p99)
   - Memory and PMEM usage
   - Active connections

## Documentation

See the `docs/` directory for detailed documentation:

- [API Reference](docs/API_REFERENCE.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Workspace Structure](docs/WORKSPACE.md)
- [PMEM Setup](docs/PMEM_SETUP.md)
- [Development Guidelines](AGENTS.md) - For developers and AI assistants

## Examples

Check the `examples/` directory for usage examples:

- Simple server setup
- Cluster client implementation

## License

This project is licensed under the MIT License.
