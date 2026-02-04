//! Tracing and observability infrastructure for EagleDB
//!
//! This module provides:
//! - Request context with correlation IDs
//! - Structured logging configuration
//! - Request span creation utilities

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{Level, Span};
use uuid::Uuid;

/// Global request counter for correlation ID generation
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Request context containing correlation ID and metadata
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Unique correlation ID for tracing requests across components
    pub correlation_id: String,
    /// Client address (if available)
    pub client_addr: Option<String>,
    /// Command being executed (if known)
    pub command: Option<String>,
    /// Request sequence number within connection
    pub request_seq: u64,
}

impl RequestContext {
    /// Create a new request context with a unique correlation ID
    pub fn new() -> Self {
        let (correlation_id, request_seq) = generate_correlation_id();
        Self {
            correlation_id,
            client_addr: None,
            command: None,
            request_seq,
        }
    }

    /// Create a request context for a specific client
    pub fn for_client(client_addr: &str) -> Self {
        let (correlation_id, request_seq) = generate_correlation_id();
        Self {
            correlation_id,
            client_addr: Some(client_addr.to_string()),
            command: None,
            request_seq,
        }
    }

    /// Set the command being executed
    pub fn with_command(mut self, command: &str) -> Self {
        self.command = Some(command.to_uppercase());
        self
    }

    /// Create a tracing span for this request
    pub fn span(&self) -> Span {
        if let Some(ref cmd) = self.command {
            tracing::info_span!(
                "request",
                correlation_id = %self.correlation_id,
                client = self.client_addr.as_deref().unwrap_or("unknown"),
                command = %cmd,
                seq = self.request_seq
            )
        } else {
            tracing::info_span!(
                "request",
                correlation_id = %self.correlation_id,
                client = self.client_addr.as_deref().unwrap_or("unknown"),
                seq = self.request_seq
            )
        }
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a unique correlation ID and return it with the sequence number
///
/// Uses a compact format: first 8 chars of UUID v7 (time-ordered) for sortability
/// plus a monotonic counter for uniqueness within the same timestamp.
/// Returns both the correlation ID and the counter value to avoid race conditions.
fn generate_correlation_id() -> (String, u64) {
    let uuid = Uuid::now_v7();
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let correlation_id = format!("{}-{:06x}", &uuid.to_string()[..8], counter & 0xFFFFFF);
    (correlation_id, counter)
}

/// Tracing subscriber configuration
pub struct TracingConfig {
    /// Log level (default: INFO)
    pub level: Level,
    /// Enable JSON output format
    pub json_format: bool,
    /// Include file and line numbers in logs
    pub include_location: bool,
    /// Include thread IDs in logs
    pub include_thread_ids: bool,
    /// Include target module in logs
    pub include_target: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            level: Level::INFO,
            json_format: false,
            include_location: true,
            include_thread_ids: true,
            include_target: false,
        }
    }
}

impl TracingConfig {
    /// Create a production configuration with JSON output
    pub fn production() -> Self {
        Self {
            level: Level::INFO,
            json_format: true,
            include_location: true,
            include_thread_ids: true,
            include_target: true,
        }
    }

    /// Create a development configuration with human-readable output
    pub fn development() -> Self {
        Self {
            level: Level::DEBUG,
            json_format: false,
            include_location: true,
            include_thread_ids: false,
            include_target: false,
        }
    }

    /// Initialize the global tracing subscriber with this configuration
    pub fn init(self) {
        use tracing_subscriber::EnvFilter;
        use tracing_subscriber::fmt;
        use tracing_subscriber::prelude::*;

        // Allow RUST_LOG to override the default level
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(self.level.to_string()));

        if self.json_format {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .json()
                    .with_file(self.include_location)
                    .with_line_number(self.include_location)
                    .with_thread_ids(self.include_thread_ids)
                    .with_target(self.include_target),
            );
            subscriber.init();
        } else {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .with_file(self.include_location)
                    .with_line_number(self.include_location)
                    .with_thread_ids(self.include_thread_ids)
                    .with_target(self.include_target),
            );
            subscriber.init();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_id_generation() {
        let (id1, seq1) = generate_correlation_id();
        let (id2, seq2) = generate_correlation_id();

        // IDs should be unique
        assert_ne!(id1, id2);

        // Sequence numbers should increment
        assert!(seq2 > seq1);

        // IDs should have the expected format (8 chars + hyphen + 6 hex chars)
        assert!(id1.len() >= 15);
        assert!(id1.contains('-'));
    }

    #[test]
    fn test_request_context_creation() {
        let ctx = RequestContext::new();
        assert!(!ctx.correlation_id.is_empty());
        assert!(ctx.client_addr.is_none());
        assert!(ctx.command.is_none());
    }

    #[test]
    fn test_request_context_for_client() {
        let ctx = RequestContext::for_client("127.0.0.1:12345");
        assert_eq!(ctx.client_addr, Some("127.0.0.1:12345".to_string()));
    }

    #[test]
    fn test_request_context_with_command() {
        let ctx = RequestContext::new().with_command("get");
        assert_eq!(ctx.command, Some("GET".to_string()));
    }

    #[test]
    fn test_request_sequence_increments() {
        let ctx1 = RequestContext::new();
        let ctx2 = RequestContext::new();
        assert!(ctx2.request_seq > ctx1.request_seq);
    }
}
