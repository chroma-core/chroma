use crate::config::RootConfig;
use crate::work_queue::work_queue_server::WorkQueueServer;
use crate::work_queue::WorkQueueManager;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_storage::Storage;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

pub async fn service_entrypoint() {
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

    let service_config = config.work_queue_service.clone();
    let work_queue_config = config.work_queue.clone();
    let registry = Registry::new();

    // Initialize tracing
    chroma_tracing::init_otel_tracing(
        &service_config.service_name,
        &service_config.otel_filters,
        &service_config.otel_endpoint,
    );

    let system = chroma_system::System::new();

    // Create storage
    let storage = match Storage::try_from_config(&service_config.storage, &registry).await {
        Ok(storage) => storage,
        Err(err) => {
            println!("Failed to create storage: {:?}", err);
            return;
        }
    };

    // Create and start work queue manager
    let work_queue_manager = WorkQueueManager::new(storage, work_queue_config.clone());
    let work_queue_handle = system.start_component(work_queue_manager);

    // Create and start gRPC server
    let work_queue_server = WorkQueueServer::new(work_queue_handle.clone());
    let server = work_queue_server.into_service();
    let port = service_config.my_port;

    // Create health service for readiness probe
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let server_join_handle = tokio::spawn(async move {
        // Set service as serving for health checks
        health_reporter.set_serving::<chroma_types::chroma_proto::work_queue_service_server::WorkQueueServiceServer<WorkQueueServer>>().await;

        let addr = format!("0.0.0.0:{}", port).parse().unwrap();
        println!("Work queue gRPC server listening on {}", addr);
        tonic::transport::Server::builder()
            .add_service(server)
            .add_service(health_service)
            .serve(addr)
            .await
            .unwrap();
    });

    // Set up signal handlers
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    select! {
        _ = sigterm.recv() => {
            println!("Received SIGTERM, shutting down work queue service");
        },
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down work queue service");
        },
        res = server_join_handle => {
            match res {
                Ok(_) => println!("Server task ended unexpectedly"),
                Err(e) => println!("Server task error: {:?}", e),
            }
        }
    };

    // Shutdown procedure
    match tokio::time::timeout(std::time::Duration::from_secs(30), async {
        work_queue_handle.stop();
        system.stop().await;
        system.join().await;
    })
    .await
    {
        Ok(_) => println!("Clean shutdown completed"),
        Err(_) => {
            println!("Shutdown timeout, forcing exit");
        }
    };

    println!("Work queue service stopped");
}
