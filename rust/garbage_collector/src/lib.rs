use chroma_config::Configurable;
use chroma_memberlist::memberlist_provider::CustomResourceMemberlistProvider;
use chroma_memberlist::memberlist_provider::MemberlistProvider;
use chroma_system::{Dispatcher, System};
use chroma_tracing::{
    init_global_filter_layer, init_otel_layer, init_panic_tracing_hook, init_stdout_layer,
    init_tracing,
};
use chroma_types::chroma_proto::garbage_collector_server::GarbageCollectorServer;
use config::GarbageCollectorConfig;
use garbage_collector_component::GarbageCollector;
use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;
use tracing::{debug, error, info};

mod config;
mod construct_version_graph_orchestrator;
mod garbage_collector_component;
pub mod garbage_collector_orchestrator;
pub mod garbage_collector_orchestrator_v2;

#[cfg(test)]
pub(crate) mod helper;
pub mod operators;
pub mod types;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

// This is a placeholder service so that we can expose a health service
struct GarbageCollectorService {}

impl chroma_types::chroma_proto::garbage_collector_server::GarbageCollector
    for GarbageCollectorService
{
}

pub async fn garbage_collector_service_entrypoint() -> Result<(), Box<dyn std::error::Error>> {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_not_serving::<GarbageCollectorServer<GarbageCollectorService>>()
        .await;

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

    let addr = format!("[::]:{}", config.port)
        .parse()
        .expect("Invalid address format");
    let server_join_handle = tokio::spawn(async move {
        let server = Server::builder().add_service(health_service);
        server
            .serve_with_shutdown(addr, async {
                match signal(SignalKind::terminate()) {
                    Ok(mut sigterm) => {
                        sigterm.recv().await;
                        tracing::info!("Received SIGTERM, shutting down gRPC server");
                    }
                    Err(err) => {
                        tracing::error!("Failed to create SIGTERM handler: {err}")
                    }
                }
            })
            .await
            .expect("Failed to start gRPC server");
    });

    let registry = chroma_config::registry::Registry::new();
    let system = System::new();

    // Setup the dispatcher and the pool of workers.
    let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
        .await
        .expect("Failed to create dispatcher from config");
    let mut dispatcher_handle = system.start_component(dispatcher);

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

    garbage_collector_component.set_dispatcher(dispatcher_handle.clone());
    garbage_collector_component.set_system(system.clone());

    let mut garbage_collector_handle = system.start_component(garbage_collector_component);
    memberlist.subscribe(garbage_collector_handle.receiver());
    let mut memberlist_handle = system.start_component(memberlist);

    health_reporter
        .set_serving::<GarbageCollectorServer<GarbageCollectorService>>()
        .await;

    // Keep the service running and handle shutdown signals
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    info!("Service running, waiting for signals");
    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM signal");
        }
        _ = sigint.recv() => {
            info!("Received SIGINT signal");
        }
    }
    info!("Starting graceful shutdown, waiting for in-progress tasks");
    // NOTE: We should first stop the garbage collector. The garbage collector will finish the remaining jobs before shutdown.
    // We cannot directly shutdown the dispatcher and system because that will fail remaining jobs.
    memberlist_handle.stop();
    memberlist_handle
        .join()
        .await
        .expect("Memberlist should be stoppable");
    garbage_collector_handle.stop();
    garbage_collector_handle
        .join()
        .await
        .expect("Garbage collector should be stoppable");
    dispatcher_handle.stop();
    dispatcher_handle
        .join()
        .await
        .expect("Dispatcher should be stoppable");
    system.stop().await;
    system.join().await;
    let _ = server_join_handle.await;

    info!("Shutting down garbage collector service");
    Ok(())
}
