pub mod collector;

use std::env;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

pub use collector::describe_metrics;

pub fn init_metrics() -> Result<()> {
    let host = env::var("PROMETHEUS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PROMETHEUS_PORT").unwrap_or_else(|_| "9000".to_string());
    let addr: SocketAddr = format!("{}:{}", host, port).parse().with_context(|| {
        format!(
            "failed to parse PROMETHEUS_HOST='{}' and PROMETHEUS_PORT='{}' into a socket address",
            host, port
        )
    })?;

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    // Register metric descriptions for better Prometheus labels
    describe_metrics();

    info!(
        address = %addr,
        "Prometheus metrics endpoint started"
    );

    Ok(())
}
