//! Metrics collection infrastructure for EagleDB
//!
//! Provides comprehensive observability through:
//! - Operation counters and latency histograms
//! - Error tracking by kind and command
//! - Memory and connection metrics
//! - Prometheus integration

#![allow(dead_code)]

use eagle_core::error::ErrorKind;
use hashbrown::HashMap;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use parking_lot::RwLock;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

/// Initialize metric descriptions for Prometheus
/// This should be called once at startup
pub fn describe_metrics() {
    // Operation counters
    describe_counter!(
        "eagle_ops_total",
        "Total number of operations by type (get, set, del, etc.)"
    );
    describe_counter!(
        "eagle_commands_total",
        "Total number of commands executed by command name"
    );

    // Latency histograms
    describe_histogram!(
        "eagle_operation_duration_seconds",
        "Operation duration in seconds by operation type"
    );
    describe_histogram!(
        "eagle_command_duration_seconds",
        "Command execution duration in seconds by command name"
    );

    // Error metrics
    describe_counter!("eagle_errors_total", "Total number of errors by error kind");
    describe_counter!(
        "eagle_command_errors_total",
        "Total number of command errors by command name"
    );

    // Connection metrics
    describe_gauge!(
        "eagle_active_connections",
        "Number of currently active client connections"
    );
    describe_counter!(
        "eagle_connections_total",
        "Total number of connections accepted since server start"
    );

    // Memory metrics
    describe_gauge!("eagle_memory_used_bytes", "Memory used in bytes");
    describe_gauge!("eagle_memory_available_bytes", "Memory available in bytes");
    describe_gauge!("eagle_pmem_used_bytes", "PMEM used in bytes");
    describe_gauge!("eagle_pmem_available_bytes", "PMEM available in bytes");

    // Key/database metrics
    describe_gauge!("eagle_keys_total", "Total number of keys in the database");
    describe_gauge!(
        "eagle_expired_keys_total",
        "Total number of expired keys removed"
    );
}

#[derive(Clone)]
pub struct MetricsCollector {
    // Operation counters
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

        counter!("eagle_ops_total", "operation" => "get").increment(1);
        histogram!("eagle_operation_duration_seconds", "operation" => "get")
            .record(latency.as_secs_f64());
    }

    pub fn record_set(&self, latency: Duration) {
        self.set_ops.fetch_add(1, Ordering::Relaxed);
        self.set_latency
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);

        counter!("eagle_ops_total", "operation" => "set").increment(1);
        histogram!("eagle_operation_duration_seconds", "operation" => "set")
            .record(latency.as_secs_f64());
    }

    pub fn record_del(&self, latency: Duration) {
        self.del_ops.fetch_add(1, Ordering::Relaxed);
        self.del_latency
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);

        counter!("eagle_ops_total", "operation" => "del").increment(1);
        histogram!("eagle_operation_duration_seconds", "operation" => "del")
            .record(latency.as_secs_f64());
    }

    // Error tracking
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        counter!("eagle_errors_total").increment(1);
    }

    /// Record an error with its specific kind for detailed metrics
    pub fn record_error_by_kind(&self, kind: ErrorKind) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        counter!("eagle_errors_total", "kind" => kind.as_str().to_string()).increment(1);
    }

    // Memory tracking
    pub fn update_memory_usage(&self, used: u64, available: u64) {
        self.memory_used.store(used, Ordering::Relaxed);
        self.memory_available.store(available, Ordering::Relaxed);

        gauge!("eagle_memory_used_bytes").set(used as f64);
        gauge!("eagle_memory_available_bytes").set(available as f64);
    }

    // PMEM tracking
    pub fn update_pmem_usage(&self, used: u64, available: u64) {
        self.pmem_used.store(used, Ordering::Relaxed);
        self.pmem_available.store(available, Ordering::Relaxed);

        gauge!("eagle_pmem_used_bytes").set(used as f64);
        gauge!("eagle_pmem_available_bytes").set(available as f64);
    }

    // Connection tracking
    pub fn record_new_connection(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);

        gauge!("eagle_active_connections").increment(1.0);
        counter!("eagle_connections_total").increment(1);
    }

    pub fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        gauge!("eagle_active_connections").decrement(1.0);
    }

    // Command tracking
    pub fn record_command(&self, command: &str) {
        let mut stats = self.command_stats.write();
        *stats.entry(command.to_string()).or_default() += 1;

        counter!("eagle_commands_total", "command" => command.to_string()).increment(1);
    }

    /// Record command execution with latency
    pub fn record_command_latency(&self, command: &str, latency: Duration) {
        let mut stats = self.command_stats.write();
        *stats.entry(command.to_string()).or_default() += 1;

        counter!("eagle_commands_total", "command" => command.to_string()).increment(1);
        histogram!("eagle_command_duration_seconds", "command" => command.to_string())
            .record(latency.as_secs_f64());
    }

    /// Record a command error
    pub fn record_command_error(&self, command: &str) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        counter!("eagle_command_errors_total", "command" => command.to_string()).increment(1);
        counter!("eagle_errors_total", "kind" => "command").increment(1);
    }

    /// Update the total key count gauge
    pub fn update_key_count(&self, count: u64) {
        gauge!("eagle_keys_total").set(count as f64);
    }

    /// Record expired keys being removed
    pub fn record_expired_keys(&self, count: u64) {
        counter!("eagle_expired_keys_total").increment(count);
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

    // Helper functions
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
