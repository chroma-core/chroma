use chroma_config::Configurable;
use chroma_system::{Dispatcher, System};
use config::GarbageCollectorConfig;
use garbage_collector_component::GarbageCollector;
use opentelemetry_config::init_otel_tracing;

mod config;
mod garbage_collector_component;
mod opentelemetry_config;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn garbage_collector_service_entrypoint() {
    // Parse configuration. Configuration includes sysdb connection details, and
    // gc run details amongst others.
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => GarbageCollectorConfig::load_from_path(&config_path),
        Err(_) => GarbageCollectorConfig::load(),
    };
    // Enable OTEL tracing.
    init_otel_tracing(&config.service_name, &config.otel_endpoint);

    // Setup the dispatcher and the pool of workers.
    let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config)
        .await
        .expect("Failed to create dispatcher from config");

    let system = System::new();
    let dispatcher_handle = system.start_component(dispatcher);

    // Start a background task to periodically check for garbage.
    // Garbage collector is a component that gets notified every
    // gc_interval_mins to check for garbage.
    let mut garbage_collector_component = GarbageCollector::try_from_config(&config)
        .await
        .expect("Failed to create garbage collector component");
    garbage_collector_component.set_dispatcher(dispatcher_handle);
    garbage_collector_component.set_system(system.clone());

    let _ = system.start_component(garbage_collector_component);
}
