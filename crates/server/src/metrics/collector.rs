//! Metrics collection infrastructure for EagleDB
//!
//! Provides comprehensive observability through:
//! - Operation counters and latency histograms
//! - Error tracking by kind and command
//! - Memory and connection metrics
//! - Prometheus integration via foundations

#![allow(dead_code)]

use eagle_core::error::ErrorKind;
use foundations::telemetry::metrics::{Counter, Gauge, HistogramBuilder, TimeHistogram, metrics};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// Label types for metrics
pub mod labels {
    use serde::Serialize;

    #[derive(Clone, Eq, Hash, PartialEq, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum Operation {
        Get,
        Set,
        Del,
    }
}

/// EagleDB metrics definitions using foundations #[metrics] macro
#[metrics]
pub mod eagle {
    /// Total number of operations by type (get, set, del, etc.)
    pub fn ops_total(operation: super::labels::Operation) -> Counter;

    /// Total number of commands executed by command name
    pub fn commands_total(command: &'static str) -> Counter;

    /// Operation duration in seconds by operation type
    #[ctor = HistogramBuilder {
        buckets: &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    }]
    pub fn operation_duration_seconds(operation: super::labels::Operation) -> TimeHistogram;

    /// Command execution duration in seconds by command name
    #[ctor = HistogramBuilder {
        buckets: &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    }]
    pub fn command_duration_seconds(command: &'static str) -> TimeHistogram;

    /// Total number of errors by error kind
    pub fn errors_total(kind: &'static str) -> Counter;

    /// Total number of command errors by command name
    pub fn command_errors_total(command: &'static str) -> Counter;

    /// Number of currently active client connections
    pub fn active_connections() -> Gauge;

    /// Total number of connections accepted since server start
    pub fn connections_total() -> Counter;

    /// Memory used in bytes
    pub fn memory_used_bytes() -> Gauge;

    /// Memory available in bytes
    pub fn memory_available_bytes() -> Gauge;

    /// PMEM used in bytes
    pub fn pmem_used_bytes() -> Gauge;

    /// PMEM available in bytes
    pub fn pmem_available_bytes() -> Gauge;

    /// Total number of keys in the database
    pub fn keys_total() -> Gauge;

    /// Total number of expired keys removed
    pub fn expired_keys_total() -> Counter;
}

#[derive(Clone)]
pub struct EagleMetrics;

impl EagleMetrics {
    pub fn new() -> Self {
        Self
    }

    // Operation tracking
    pub fn record_get(&self, latency: Duration) {
        eagle::ops_total(labels::Operation::Get).inc();
        eagle::operation_duration_seconds(labels::Operation::Get)
            .observe(latency.as_nanos() as u64);
    }

    pub fn record_set(&self, latency: Duration) {
        eagle::ops_total(labels::Operation::Set).inc();
        eagle::operation_duration_seconds(labels::Operation::Set)
            .observe(latency.as_nanos() as u64);
    }

    pub fn record_del(&self, latency: Duration) {
        eagle::ops_total(labels::Operation::Del).inc();
        eagle::operation_duration_seconds(labels::Operation::Del)
            .observe(latency.as_nanos() as u64);
    }

    // Error tracking
    pub fn record_error(&self) {
        eagle::errors_total("unknown").inc();
    }

    /// Record an error with its specific kind for detailed metrics
    pub fn record_error_by_kind(&self, kind: ErrorKind) {
        eagle::errors_total(kind.as_str()).inc();
    }

    // Memory tracking
    pub fn update_memory_usage(&self, used: u64, available: u64) {
        eagle::memory_used_bytes().set(used);
        eagle::memory_available_bytes().set(available);
    }

    // PMEM tracking
    pub fn update_pmem_usage(&self, used: u64, available: u64) {
        eagle::pmem_used_bytes().set(used);
        eagle::pmem_available_bytes().set(available);
    }

    // Connection tracking
    pub fn record_new_connection(&self) {
        eagle::active_connections().inc();
        eagle::connections_total().inc();
    }

    pub fn record_connection_closed(&self) {
        eagle::active_connections().dec();
    }

    // Command tracking - uses static strings for common commands
    pub fn record_command(&self, command: &'static str) {
        eagle::commands_total(command).inc();
    }

    /// Record command execution with latency
    pub fn record_command_latency(&self, command: &'static str, latency: Duration) {
        eagle::commands_total(command).inc();
        eagle::command_duration_seconds(command).observe(latency.as_nanos() as u64);
    }

    /// Record a command error
    pub fn record_command_error(&self, command: &'static str) {
        eagle::command_errors_total(command).inc();
        eagle::errors_total("command").inc();
    }

    /// Update the total key count gauge
    pub fn update_key_count(&self, count: u64) {
        eagle::keys_total().set(count);
    }

    /// Record expired keys being removed
    pub fn record_expired_keys(&self, count: u64) {
        for _ in 0..count {
            eagle::expired_keys_total().inc();
        }
    }
}

impl Default for EagleMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Legacy MetricsCollector for backwards compatibility
/// This wraps EagleMetrics and adds local atomic counters for
/// programmatic access to metrics values
#[derive(Clone)]
pub struct MetricsCollector {
    inner: EagleMetrics,

    // Operation counters (for programmatic access)
    get_ops: Arc<AtomicU64>,
    set_ops: Arc<AtomicU64>,
    del_ops: Arc<AtomicU64>,

    // Latency tracking (in microseconds)
    get_latency: Arc<AtomicU64>,
    set_latency: Arc<AtomicU64>,
    del_latency: Arc<AtomicU64>,

    // Error counters
    errors: Arc<AtomicU64>,

    // Memory metrics
    memory_used: Arc<AtomicU64>,
    memory_available: Arc<AtomicU64>,

    // PMEM metrics
    pmem_used: Arc<AtomicU64>,
    pmem_available: Arc<AtomicU64>,

    // Connection metrics
    active_connections: Arc<AtomicU64>,
    total_connections: Arc<AtomicU64>,

    // Command statistics
    command_stats: Arc<RwLock<HashMap<String, u64>>>,

    // Start time for uptime calculation
    start_time: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            inner: EagleMetrics::new(),
            get_ops: Arc::new(AtomicU64::default()),
            set_ops: Arc::new(AtomicU64::default()),
            del_ops: Arc::new(AtomicU64::default()),
            get_latency: Arc::new(AtomicU64::default()),
            set_latency: Arc::new(AtomicU64::default()),
            del_latency: Arc::new(AtomicU64::default()),
            errors: Arc::new(AtomicU64::default()),
            memory_used: Arc::new(AtomicU64::default()),
            memory_available: Arc::new(AtomicU64::default()),
            pmem_used: Arc::new(AtomicU64::default()),
            pmem_available: Arc::new(AtomicU64::default()),
            active_connections: Arc::new(AtomicU64::default()),
            total_connections: Arc::new(AtomicU64::default()),
            command_stats: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    // Operation tracking
    pub fn record_get(&self, latency: Duration) {
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.get_latency
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.inner.record_get(latency);
    }

    pub fn record_set(&self, latency: Duration) {
        self.set_ops.fetch_add(1, Ordering::Relaxed);
        self.set_latency
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.inner.record_set(latency);
    }

    pub fn record_del(&self, latency: Duration) {
        self.del_ops.fetch_add(1, Ordering::Relaxed);
        self.del_latency
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.inner.record_del(latency);
    }

    // Error tracking
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.inner.record_error();
    }

    pub fn record_error_by_kind(&self, kind: ErrorKind) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.inner.record_error_by_kind(kind);
    }

    // Memory tracking
    pub fn update_memory_usage(&self, used: u64, available: u64) {
        self.memory_used.store(used, Ordering::Relaxed);
        self.memory_available.store(available, Ordering::Relaxed);
        self.inner.update_memory_usage(used, available);
    }

    // PMEM tracking
    pub fn update_pmem_usage(&self, used: u64, available: u64) {
        self.pmem_used.store(used, Ordering::Relaxed);
        self.pmem_available.store(available, Ordering::Relaxed);
        self.inner.update_pmem_usage(used, available);
    }

    // Connection tracking
    pub fn record_new_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.inner.record_new_connection();
    }

    pub fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.inner.record_connection_closed();
    }

    // Command tracking (stores stats locally, doesn't forward to foundations for dynamic strings)
    pub fn record_command(&self, command: &str) {
        let mut stats = self.command_stats.write();
        *stats.entry(command.to_string()).or_default() += 1;
        // Note: foundations metrics require static strings, so we skip the prometheus recording
        // for dynamic command names. Use record_command_static for prometheus.
    }

    /// Record command with static string (for prometheus metrics)
    pub fn record_command_static(&self, command: &'static str, latency: Duration) {
        let mut stats = self.command_stats.write();
        *stats.entry(command.to_string()).or_default() += 1;
        self.inner.record_command_latency(command, latency);
    }

    pub fn record_command_latency(&self, command: &str, _latency: Duration) {
        let mut stats = self.command_stats.write();
        *stats.entry(command.to_string()).or_default() += 1;
        // Note: foundations requires static strings for labels
    }

    pub fn record_command_error(&self, _command: &str) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        // Note: foundations requires static strings for labels
    }

    pub fn update_key_count(&self, count: u64) {
        self.inner.update_key_count(count);
    }

    pub fn record_expired_keys(&self, count: u64) {
        self.inner.record_expired_keys(count);
    }

    // Metrics retrieval
    pub fn get_operation_metrics(&self) -> OperationMetrics {
        OperationMetrics {
            get_ops: self.get_ops.load(Ordering::Relaxed),
            set_ops: self.set_ops.load(Ordering::Relaxed),
            del_ops: self.del_ops.load(Ordering::Relaxed),
            get_latency_avg: self.calculate_average_latency(
                self.get_latency.load(Ordering::Relaxed),
                self.get_ops.load(Ordering::Relaxed),
            ),
            set_latency_avg: self.calculate_average_latency(
                self.set_latency.load(Ordering::Relaxed),
                self.set_ops.load(Ordering::Relaxed),
            ),
            del_latency_avg: self.calculate_average_latency(
                self.del_latency.load(Ordering::Relaxed),
                self.del_ops.load(Ordering::Relaxed),
            ),
        }
    }

    pub fn get_memory_metrics(&self) -> MemoryMetrics {
        MemoryMetrics {
            memory_used: self.memory_used.load(Ordering::Relaxed),
            memory_available: self.memory_available.load(Ordering::Relaxed),
            pmem_used: self.pmem_used.load(Ordering::Relaxed),
            pmem_available: self.pmem_available.load(Ordering::Relaxed),
        }
    }

    pub fn get_connection_metrics(&self) -> ConnectionMetrics {
        ConnectionMetrics {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
        }
    }

    pub fn get_command_stats(&self) -> HashMap<String, u64> {
        self.command_stats.read().clone()
    }

    pub fn get_uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn get_error_count(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn calculate_average_latency(&self, total_latency: u64, total_ops: u64) -> f64 {
        if total_ops == 0 {
            0.0
        } else {
            total_latency as f64 / total_ops as f64
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct OperationMetrics {
    pub get_ops: u64,
    pub set_ops: u64,
    pub del_ops: u64,
    pub get_latency_avg: f64,
    pub set_latency_avg: f64,
    pub del_latency_avg: f64,
}

#[derive(Debug)]
pub struct MemoryMetrics {
    pub memory_used: u64,
    pub memory_available: u64,
    pub pmem_used: u64,
    pub pmem_available: u64,
}

#[derive(Debug)]
pub struct ConnectionMetrics {
    pub active_connections: u64,
    pub total_connections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_operation_tracking() {
        let collector = MetricsCollector::new();
        let latency = Duration::from_micros(100);

        collector.record_get(latency);
        collector.record_set(latency);
        collector.record_del(latency);

        let metrics = collector.get_operation_metrics();
        assert_eq!(metrics.get_ops, 1);
        assert_eq!(metrics.set_ops, 1);
        assert_eq!(metrics.del_ops, 1);
        assert_eq!(metrics.get_latency_avg, 100.0);
    }

    #[test]
    fn test_memory_tracking() {
        let collector = MetricsCollector::new();
        collector.update_memory_usage(1000, 2000);
        collector.update_pmem_usage(3000, 4000);

        let metrics = collector.get_memory_metrics();
        assert_eq!(metrics.memory_used, 1000);
        assert_eq!(metrics.memory_available, 2000);
        assert_eq!(metrics.pmem_used, 3000);
        assert_eq!(metrics.pmem_available, 4000);
    }

    #[test]
    fn test_connection_tracking() {
        let collector = MetricsCollector::new();
        collector.record_new_connection();
        collector.record_new_connection();
        collector.record_connection_closed();

        let metrics = collector.get_connection_metrics();
        assert_eq!(metrics.active_connections, 1);
        assert_eq!(metrics.total_connections, 2);
    }

    #[test]
    fn test_command_tracking() {
        let collector = MetricsCollector::new();
        collector.record_command("GET");
        collector.record_command("SET");
        collector.record_command("GET");

        let stats = collector.get_command_stats();
        assert_eq!(stats.get("GET"), Some(&2));
        assert_eq!(stats.get("SET"), Some(&1));
    }

    #[test]
    fn test_uptime() {
        let collector = MetricsCollector::new();
        thread::sleep(Duration::from_millis(10));
        assert!(collector.get_uptime().as_millis() >= 10);
    }
}
