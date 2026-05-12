use crate::config::RootConfig;
use crate::fn_consumer::fn_consumer_manager::FnConsumerManager;
use crate::fn_consumer::fn_consumer_server::FnConsumerServer;
use crate::fn_consumer::orchestrator::NoopSink;
use crate::work_queue::work_queue_client::WorkQueueClient;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use std::sync::Arc;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn fn_consumer_service_entrypoint() {
    // Load configuration from CONFIG_PATH if set, otherwise use default
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => {
            eprintln!("loading from {config_path}");
            RootConfig::load_from_path(&config_path)
        }
        Err(_) => {
            eprintln!("loading from default path");
            RootConfig::load()
        }
    };

    let service_config = config.fn_consumer_service.clone();
    let registry = Registry::new();

    chroma_tracing::init_otel_tracing(
        &service_config.service_name,
        &service_config.otel_filters,
        &service_config.otel_endpoint,
    );

    let system = chroma_system::System::new();

    // Create dispatcher
    let dispatcher =
        match chroma_system::Dispatcher::try_from_config(&service_config.dispatcher, &registry)
            .await
        {
            Ok(dispatcher) => dispatcher,
            Err(err) => {
                eprintln!("Failed to create dispatcher: {:?}", err);
                return;
            }
        };
    let dispatcher_handle = system.start_component(dispatcher);

    // Connect to the work queue
    let work_queue_client =
        match WorkQueueClient::new(service_config.fn_consumer.work_queue_endpoint.clone()).await {
            Ok(client) => client,
            Err(err) => {
                eprintln!("Failed to connect to work queue: {:?}", err);
                return;
            }
        };

    // Build and start the manager
    let sink = Arc::new(NoopSink);
    let mut manager = FnConsumerManager::new(
        service_config.fn_consumer.clone(),
        service_config.my_member_id.clone(),
        system.clone(),
        sink,
        work_queue_client.clone(),
    );
    manager.set_dispatcher(dispatcher_handle);
    let manager_handle = system.start_component(manager);

    // Build the gRPC server (proxies FinishWork to the work queue)
    let server = FnConsumerServer::new(manager_handle, work_queue_client).into_service();
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let addr = format!("0.0.0.0:{}", service_config.my_port)
        .parse()
        .expect("valid address");
    println!("fn-consumer service starting on {}", addr);

    tonic::transport::Server::builder()
        .add_service(server)
        .add_service(health_service)
        .serve(addr)
        .await
        .expect("Failed to start fn-consumer service");
}
