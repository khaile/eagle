pub mod collector;

use anyhow::Result;
use foundations::telemetry::metrics::collect;
use foundations::telemetry::settings::MetricsSettings;

pub use collector::{EagleMetrics, MetricsCollector};

/// Initialize metrics (no-op for foundations - metrics are auto-registered)
/// The telemetry server should be started separately if needed
pub fn init_metrics() -> Result<()> {
    // foundations metrics are automatically registered when first accessed
    // No explicit initialization needed
    Ok(())
}

/// Collect all metrics in Prometheus text format
pub fn collect_metrics() -> String {
    let settings = MetricsSettings::default();
    collect(&settings).unwrap_or_default()
}

/// Collect all metrics including optional ones
pub fn collect_all_metrics() -> String {
    let settings = MetricsSettings {
        report_optional: true,
        ..Default::default()
    };
    collect(&settings).unwrap_or_default()
}
