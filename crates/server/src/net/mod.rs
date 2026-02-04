use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{Instrument, error, info, info_span, warn};

pub mod cluster;
pub mod connection;
pub mod health;
pub mod protocol;

use crate::metrics::collector::MetricsCollector;
use eagle_core::store::Store;

/// Default maximum number of concurrent connections
const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Default read timeout in seconds
const DEFAULT_READ_TIMEOUT_SECS: u64 = 30;

/// Server configuration
#[derive(Clone)]
pub struct ServerConfig {
    /// Maximum number of concurrent connections (default: 10,000)
    pub max_connections: usize,
    /// Read timeout in seconds (default: 30)
    pub read_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            read_timeout_secs: DEFAULT_READ_TIMEOUT_SECS,
        }
    }
}

pub struct Server {
    store: Store,
    metrics: Arc<MetricsCollector>,
    config: ServerConfig,
    /// Semaphore to limit concurrent connections
    connection_semaphore: Arc<Semaphore>,
}

impl Server {
    pub fn new(store: Store, metrics: Arc<MetricsCollector>) -> Self {
        Self::with_config(store, metrics, ServerConfig::default())
    }

    pub fn with_config(store: Store, metrics: Arc<MetricsCollector>, config: ServerConfig) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));
        Self {
            store,
            metrics,
            config,
            connection_semaphore,
        }
    }

    #[allow(dead_code)]
    pub async fn run(&self, addr: &str) -> Result<(), anyhow::Error> {
        self.run_with_shutdown(addr, tokio::sync::watch::channel(()).1)
            .await
    }

    pub async fn run_with_shutdown(
        &self,
        addr: &str,
        mut shutdown_rx: watch::Receiver<()>,
    ) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind(addr).await?;
        info!(
            address = %addr,
            max_connections = self.config.max_connections,
            read_timeout_secs = self.config.read_timeout_secs,
            "Server listening"
        );

        let mut connection_tasks = JoinSet::new();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, peer_addr)) => {
                            // Try to acquire a connection permit
                            let permit = match self.connection_semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    warn!(
                                        peer = %peer_addr,
                                        max_connections = self.config.max_connections,
                                        "Connection limit reached, rejecting connection"
                                    );
                                    self.metrics.record_error();
                                    continue;
                                }
                            };

                            info!(peer = %peer_addr, "Accepted new connection");
                            self.metrics.record_new_connection();

                            let mut conn = connection::Connection::new(
                                socket,
                                peer_addr,
                                self.store.clone(),
                                self.metrics.clone(),
                                self.config.read_timeout_secs,
                            );

                            let conn_span = info_span!("conn_task", peer = %peer_addr);

                            connection_tasks.spawn(
                                async move {
                                    let _permit = permit;
                                    if let Err(e) = conn.handle().await {
                                        error!(error = %e, "Connection handler error");
                                    }
                                }
                                .instrument(conn_span),
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("Shutting down server");
                    break;
                }
            }
        }

        // Wait for all connection tasks to complete
        info!(
            "Waiting for {} connection tasks to complete",
            connection_tasks.len()
        );
        while let Some(join_result) = connection_tasks.join_next().await {
            if let Err(e) = join_result {
                error!(error = %e, "Connection task panicked");
            }
        }
        info!("All connection tasks completed");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_server_connection() -> Result<(), anyhow::Error> {
        let store = Store::new_memory()?;
        let metrics = Arc::new(MetricsCollector::new());
        let server = Server::new(store, metrics);
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);

        // Start server in background
        tokio::spawn(async move {
            if let Err(e) = server.run(&addr.to_string()).await {
                error!("Server error: {}", e);
            }
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test connection
        let _client = TcpStream::connect(addr).await?;
        Ok(())
    }
}
