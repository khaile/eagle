use anyhow::Result;
use clap::Parser;
use command::Command;
use eagle_core::error::StorageError;
use eagle_core::store;
use eagle_core::store::aof::AofReader;
use std::path::Path;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

pub mod command;
pub mod config;
pub mod metrics;
pub mod net;
pub mod tracing_config;

use crate::tracing_config::TracingConfig;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 6379;
const DEFAULT_CLUSTER_PORT: u16 = 6380;
const DEFAULT_PMEM_SIZE_MB: usize = 1024;
const DEFAULT_SNAPSHOT_INTERVAL_SECS: u64 = 3600; // 1 hour

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'H', long, default_value = DEFAULT_HOST)]
    host: String,

    #[arg(short = 'P', long, default_value_t = DEFAULT_PORT)]
    port: u16,

    #[arg(long)]
    pmem_path: Option<String>,

    #[arg(long, default_value_t = DEFAULT_PMEM_SIZE_MB)]
    pmem_size_mb: usize,

    // New cluster mode flag
    #[arg(long)]
    cluster_mode: bool,

    // Cluster configuration path
    #[arg(long)]
    cluster_config: Option<String>,

    #[arg(long, default_value_t = DEFAULT_CLUSTER_PORT)]
    cluster_port: u16,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Use JSON format for logs (recommended for production)
    #[arg(long)]
    log_json: bool,

    /// Health check server port (set to 0 to disable)
    #[arg(long, default_value_t = net::health::DEFAULT_HEALTH_PORT)]
    health_port: u16,

    /// Snapshot interval in seconds (0 to disable automatic snapshots)
    #[arg(long, default_value_t = DEFAULT_SNAPSHOT_INTERVAL_SECS)]
    snapshot_interval_secs: u64,

    /// Path for snapshot files
    #[arg(long, default_value = "dump.eagle")]
    snapshot_path: String,

    /// Maximum number of snapshots to retain (0 to keep all)
    #[arg(long, default_value_t = 0)]
    snapshot_retain: usize,

    /// Enable append-only file logging
    #[arg(long, default_value_t = false)]
    aof_enabled: bool,

    /// Path to append-only file
    #[arg(long, default_value = "appendonly.eagle")]
    aof_path: String,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();

    // Initialize structured logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => tracing::Level::TRACE,
        "debug" => tracing::Level::DEBUG,
        "info" => tracing::Level::INFO,
        "warn" => tracing::Level::WARN,
        "error" => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };

    TracingConfig {
        level: log_level,
        json_format: args.log_json,
        ..TracingConfig::default()
    }
    .init();

    // Log startup configuration before moving values
    let pmem_enabled = args.pmem_path.is_some();
    let snapshot_interval = if args.snapshot_interval_secs > 0 {
        Some(std::time::Duration::from_secs(args.snapshot_interval_secs))
    } else {
        None
    };

    // Initialize storage
    let store = if let Some(pmem_path) = args.pmem_path {
        if snapshot_interval.is_some() {
            let snapshot_config = store::SnapshotConfig {
                interval: snapshot_interval,
                path: args.snapshot_path.clone(),
                retain: args.snapshot_retain,
                compress: false,
                aof_enabled: args.aof_enabled,
                aof_path: args.aof_path.clone(),
            };
            Arc::new(store::Store::new_pmem_with_snapshot(
                &pmem_path,
                args.pmem_size_mb,
                snapshot_config,
            )?)
        } else {
            // Note: If AOF enabled but no snapshot interval, we still might want AOF writer.
            // But Store::new_pmem doesn't take config.
            // For now assuming AOF requires snapshot config or manual setup.
            // Let's assume new_pmem doesn't support AOF unless we use new_pmem_with_snapshot.
            // If user wants AOF with PMEM but no periodic snapshots, they should set interval=0 (which isn't supported by config option type Option<Duration>)
            // Actually, SnapshotConfig defaults interval to None.
            // So we can use new_pmem_with_snapshot with interval=None if AOF is enabled.
            if args.aof_enabled {
                let snapshot_config = store::SnapshotConfig {
                    interval: None,
                    path: args.snapshot_path.clone(),
                    retain: args.snapshot_retain,
                    compress: false,
                    aof_enabled: true,
                    aof_path: args.aof_path.clone(),
                };
                Arc::new(store::Store::new_pmem_with_snapshot(
                    &pmem_path,
                    args.pmem_size_mb,
                    snapshot_config,
                )?)
            } else {
                Arc::new(store::Store::new_pmem(&pmem_path, args.pmem_size_mb)?)
            }
        }
    } else if snapshot_interval.is_some() || args.aof_enabled {
        let snapshot_config = store::SnapshotConfig {
            interval: snapshot_interval,
            path: args.snapshot_path.clone(),
            retain: args.snapshot_retain,
            compress: false,
            aof_enabled: args.aof_enabled,
            aof_path: args.aof_path.clone(),
        };
        Arc::new(store::Store::new_with_snapshot(snapshot_config)?)
    } else {
        Arc::new(store::Store::new()?)
    };

    // AOF Recovery or Snapshot Loading
    if !pmem_enabled {
        if args.aof_enabled && Path::new(&args.aof_path).exists() {
            info!(path = %args.aof_path, "Replaying AOF file");
            let mut reader = AofReader::new(&args.aof_path)?;
            reader.validate_header()?;

            let mut count = 0;
            let start = std::time::Instant::now();

            // Replay commands
            reader.replay_commands(|resp| {
                // Parse command from RESP
                let cmd = Command::from_resp(resp)
                    .map_err(|e| StorageError::Aof(format!("Failed to parse command: {}", e)))?;

                // Execute command synchronously (since we are in initialization)
                // We need to use block_on because execute is async
                let store = store.clone();
                let result = tokio::runtime::Handle::current()
                    .block_on(async move { cmd.execute(&store).await });

                if let Err(e) = result {
                    error!(error = %e, "Failed to execute replayed command");
                }

                count += 1;
                Ok(())
            })?;

            info!(
                count = count,
                duration_ms = start.elapsed().as_millis(),
                "AOF replay completed"
            );
        } else if Path::new(&args.snapshot_path).exists() {
            info!(path = %args.snapshot_path, "Loading existing snapshot");
            match store.load_snapshot(&args.snapshot_path) {
                Ok(count) => {
                    info!("Successfully loaded {} entries from snapshot", count);
                }
                Err(e) => {
                    error!(error = %e, "Failed to load snapshot, starting with empty database");
                }
            }
        }
    } else if args.aof_enabled && Path::new(&args.aof_path).exists() {
        info!("PMEM enabled: Skipping AOF replay to avoid duplication");
    }

    // Initialize Prometheus metrics exporter
    metrics::init_metrics()?;

    // Create shared metrics collector for all components
    let metrics_collector = Arc::new(metrics::collector::MetricsCollector::new());

    // Start servers
    info!(
        host = %args.host,
        port = args.port,
        pmem_enabled = pmem_enabled,
        cluster_mode = args.cluster_mode,
        "Starting EagleDB server"
    );

    // Start cluster server if in cluster mode
    if args.cluster_mode {
        let cluster_config = if args.cluster_mode {
            let config_path = args
                .cluster_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Cluster config path required in cluster mode"))?;
            Some(config::cluster::ClusterConfig::load(config_path)?)
        } else {
            None
        };

        let cluster_addr = format!("{}:{}", args.host, args.cluster_port);
        let addr = cluster_addr.parse()?;
        let store_clone = store.clone();
        tokio::spawn(async move {
            if let Err(e) =
                net::cluster::start_cluster_server(addr, store_clone, cluster_config.unwrap()).await
            {
                error!("Cluster server error: {}", e);
            }
        });
    }

    // Start main server
    let addr = format!("{}:{}", args.host, args.port);
    let server = net::Server::new(store.as_ref().clone(), Arc::clone(&metrics_collector));

    // Create a channel for shutdown signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    // Run health check server in background with shutdown signal
    if args.health_port > 0 {
        let health_addr: std::net::SocketAddr =
            format!("{}:{}", args.host, args.health_port).parse()?;
        let health_config = net::health::HealthConfig {
            address: health_addr,
        };
        let health_server = net::health::HealthServer::with_config(
            store.clone(),
            Arc::clone(&metrics_collector),
            health_config,
        );

        let health_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let _ = health_server.run_with_shutdown(health_shutdown_rx).await;
        });
    }

    // Create a clone for signal handler
    let store_for_signal = store.clone();
    let shutdown_tx_for_signal = shutdown_tx.clone();

    // Spawn signal handler with timeout
    tokio::spawn(async move {
        // Wait for shutdown signal (Ctrl+C)
        #[cfg(unix)]
        let signal_result = signal::ctrl_c().await;

        #[cfg(windows)]
        let signal_result = {
            use signal::windows::SignalKind;
            let ctrl_c = signal::windows::ctrl_c()
                .map_err(|e| anyhow::anyhow!("Failed to create Ctrl+C handler: {}", e))?;
            let ctrl_break = signal::windows::ctrl_break()
                .map_err(|e| anyhow::anyhow!("Failed to create Ctrl+Break handler: {}", e))?;
            tokio::select! {
                _ = ctrl_c.recv() => Ok(()),
                _ = ctrl_break.recv() => Ok(()),
            }
        };

        #[cfg(not(any(unix, windows)))]
        let signal_result = {
            // Fallback for other platforms
            tracing::warn!("Signal handling not implemented for this platform");
            std::future::pending().await
        };

        if let Err(e) = signal_result {
            error!("Failed to listen for shutdown signal: {}", e);
            return;
        }

        info!("Received shutdown signal, initiating graceful shutdown");

        // Stop snapshot scheduler gracefully (no timeout)
        if store_for_signal.has_snapshot_scheduler()
            && let Some(scheduler) = store_for_signal.take_snapshot_scheduler()
        {
            info!("Stopping snapshot scheduler");
            scheduler.stop().await;
            info!("Snapshot scheduler stopped successfully");
        }

        // Send shutdown signal to servers
        if let Err(e) = shutdown_tx_for_signal.send(()) {
            error!("Failed to send shutdown signal to servers: {}", e);
        } else {
            info!("Shutdown signal sent to all servers");
        }
    });

    // Run main server with graceful shutdown (no timeout)
    let server_shutdown_rx = shutdown_rx.clone();

    // Wait for server to shut down gracefully when signal is received
    let server_result = server.run_with_shutdown(&addr, server_shutdown_rx).await;

    // Handle main server result
    if let Err(e) = server_result {
        error!(error = %e, "Server error during shutdown");
        return Err(e);
    }

    info!("Server shutdown complete");
    Ok(())
}
