use chroma_config::Configurable;
use chroma_memberlist::memberlist_provider::CustomResourceMemberlistProvider;
use chroma_memberlist::memberlist_provider::MemberlistProvider;
use chroma_system::{Dispatcher, System};
use chroma_tracing::{
    init_global_filter_layer, init_otel_layer, init_panic_tracing_hook, init_stdout_layer,
    init_tracing,
};
use config::GarbageCollectorConfig;
use garbage_collector_component::GarbageCollector;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};

mod config;
mod garbage_collector_component;
pub mod garbage_collector_orchestrator;
#[cfg(test)]
pub(crate) mod helper;
pub mod operators;
pub mod types;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn garbage_collector_service_entrypoint() -> Result<(), Box<dyn std::error::Error>> {
    debug!("Loading configuration from environment");
    // Parse configuration. Configuration includes sysdb connection details, and
    // gc run details amongst others.
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => {
            info!("Found config path: {}", config_path);
            GarbageCollectorConfig::load_from_path(&config_path)
        }
        Err(_) => {
            info!("No config path found, using default");
            GarbageCollectorConfig::load()
        }
    };

    let tracing_layers = vec![
        init_global_filter_layer(),
        init_otel_layer(&config.service_name, &config.otel_endpoint),
        init_stdout_layer(),
    ];
    init_tracing(tracing_layers);
    init_panic_tracing_hook();

    info!("Loaded configuration successfully: {:#?}", config);

    let registry = chroma_config::registry::Registry::new();
    let system = System::new();

    // Setup the dispatcher and the pool of workers.
    let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
        .await
        .expect("Failed to create dispatcher from config");
    let dispatcher_handle = system.start_component(dispatcher);

    let mut memberlist =
        CustomResourceMemberlistProvider::try_from_config(&config.memberlist_provider, &registry)
            .await?;

    // Start a background task to periodically check for garbage.
    // Garbage collector is a component that gets notified every
    // gc_interval_mins to check for garbage.
    let mut garbage_collector_component = GarbageCollector::try_from_config(&config, &registry)
        .await
        .map_err(|e| {
            error!("Failed to create garbage collector component: {:?}", e);
            e
        })?;

    garbage_collector_component.set_dispatcher(dispatcher_handle);
    garbage_collector_component.set_system(system.clone());

    let garbage_collector_handle = system.start_component(garbage_collector_component);
    memberlist.subscribe(garbage_collector_handle.receiver());
    let _memberlist_handle = system.start_component(memberlist);

    // Keep the service running and handle shutdown signals
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    info!("Service running, waiting for signals");
    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM signal");
                break;
            }
            _ = sigint.recv() => {
                info!("Received SIGINT signal");
                break;
            }
            _ = sleep(Duration::from_secs(1)) => {
                // Keep the service running
                continue;
            }
        }
    }

    // Give some time for any in-progress garbage collection to complete
    info!("Starting graceful shutdown, waiting for in-progress tasks");
    sleep(Duration::from_secs(5)).await;
    info!("Shutting down garbage collector service");
    Ok(())
}
