//! Health check infrastructure for EagleDB
//!
//! Provides health and readiness endpoints for container orchestrators
//! like Kubernetes, as well as load balancers.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::sync::watch;
use tracing::info;

use crate::metrics::collector::MetricsCollector;
use eagle_core::store::Store;

pub const DEFAULT_HEALTH_PORT: u16 = 9001;

/// Health check server configuration
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Address to bind the health check server
    pub address: SocketAddr,
}

impl Default for HealthConfig {
    fn default() -> Self {
        let address = format!("{}:{}", crate::DEFAULT_HOST, DEFAULT_HEALTH_PORT);
        Self {
            address: address.parse().unwrap(),
        }
    }
}

/// Health status of the server
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    /// Overall health status
    pub status: String,
    /// Server uptime in seconds
    pub uptime_seconds: u64,
    /// Number of active connections
    pub active_connections: u64,
    /// Total keys in the store
    pub total_keys: usize,
    /// Whether the store is operational
    pub store_healthy: bool,
    /// Server version
    pub version: &'static str,
}

impl HealthStatus {
    /// Check if the server is healthy
    pub fn is_healthy(&self) -> bool {
        self.status == "healthy" && self.store_healthy
    }
}

/// Shared state for the health check server
#[derive(Clone)]
struct HealthServerState {
    store: Arc<Store>,
    metrics: Arc<MetricsCollector>,
    start_time: Instant,
}

impl HealthServerState {
    /// Get the current health status
    fn health_status(&self) -> HealthStatus {
        let store_healthy = self.check_store_health();
        let conn_metrics = self.metrics.get_connection_metrics();

        HealthStatus {
            status: if store_healthy {
                "healthy"
            } else {
                "unhealthy"
            }
            .to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            active_connections: conn_metrics.active_connections,
            total_keys: self.store.dbsize(),
            store_healthy,
            version: env!("CARGO_PKG_VERSION"),
        }
    }

    /// Check if the store is operational
    fn check_store_health(&self) -> bool {
        // Simple health check: verify we can read from the store
        // A more comprehensive check could verify PMEM status, etc.
        // Using dbsize() as a non-invasive check that the store is responsive
        let _ = self.store.dbsize();
        true
    }
}

async fn health_handler(State(state): State<Arc<HealthServerState>>) -> impl IntoResponse {
    let status = state.health_status();
    let code = if status.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (code, Json(status))
}

async fn ready_handler(State(state): State<Arc<HealthServerState>>) -> impl IntoResponse {
    // Readiness: checks the store is operational.
    let status = state.health_status();
    let code = if status.store_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (code, "OK")
}

async fn live_handler() -> impl IntoResponse {
    // Liveness: always OK if the server process is running.
    (StatusCode::OK, "OK")
}

fn router(state: Arc<HealthServerState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/live", get(live_handler))
        .with_state(state)
}

/// Health check server
pub struct HealthServer {
    state: Arc<HealthServerState>,
    config: HealthConfig,
}

impl HealthServer {
    pub fn new(store: Arc<Store>, metrics: Arc<MetricsCollector>) -> Self {
        Self::with_config(store, metrics, HealthConfig::default())
    }

    pub fn with_config(
        store: Arc<Store>,
        metrics: Arc<MetricsCollector>,
        config: HealthConfig,
    ) -> Self {
        Self {
            state: Arc::new(HealthServerState {
                store,
                metrics,
                start_time: Instant::now(),
            }),
            config,
        }
    }

    /// Get the current health status
    pub fn health_status(&self) -> HealthStatus {
        self.state.health_status()
    }

    /// Run the health check HTTP server
    pub async fn run(&self) -> anyhow::Result<()> {
        self.run_with_shutdown(tokio::sync::watch::channel(()).1)
            .await
    }

    /// Run the health check HTTP server with shutdown signal
    pub async fn run_with_shutdown(
        &self,
        mut shutdown_rx: watch::Receiver<()>,
    ) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(self.config.address).await?;
        info!(
            address = %self.config.address,
            "Health check server listening"
        );

        let shutdown = async move {
            let _ = shutdown_rx.changed().await;
            info!("Health check server shutting down");
        };

        axum::serve(listener, router(Arc::clone(&self.state)))
            .with_graceful_shutdown(shutdown)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_serialization() {
        let status = HealthStatus {
            status: "healthy".to_string(),
            uptime_seconds: 3600,
            active_connections: 10,
            total_keys: 1000,
            store_healthy: true,
            version: "0.1.0",
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("healthy"));
        assert!(json.contains("3600"));
    }

    #[test]
    fn test_health_status_is_healthy() {
        let healthy = HealthStatus {
            status: "healthy".to_string(),
            uptime_seconds: 0,
            active_connections: 0,
            total_keys: 0,
            store_healthy: true,
            version: "0.1.0",
        };
        assert!(healthy.is_healthy());

        let unhealthy = HealthStatus {
            status: "unhealthy".to_string(),
            uptime_seconds: 0,
            active_connections: 0,
            total_keys: 0,
            store_healthy: false,
            version: "0.1.0",
        };
        assert!(!unhealthy.is_healthy());
    }
}
