use config::GarbageCollectorConfig;
use opentelemetry_config::init_otel_tracing;

mod config;
mod opentelemetry_config;

pub async fn garbage_collector_service_entrypoint() {
    // Parse configuration. Configuration includes sysdb connection details, and
    // otel details.
    let config = GarbageCollectorConfig::load();
    // Enable OTEL tracing.
    init_otel_tracing(&config.service_name, &config.otel_endpoint);

    // Start a background task to periodically check for garbage.
    todo!()
}
