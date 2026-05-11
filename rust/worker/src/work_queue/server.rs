use crate::config::RootConfig;
use crate::work_queue::work_queue_manager::WorkQueueManager;
use crate::work_queue::work_queue_server::WorkQueueServer;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_storage::Storage;
use chroma_sysdb::SysDb;

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

    // Create sysdb
    let sysdb = match SysDb::try_from_config(&(service_config.sysdb, None), &registry).await {
        Ok(sysdb) => sysdb,
        Err(err) => {
            eprintln!("Failed to create sysdb: {:?}", err);
            return;
        }
    };

    // Create and start work queue manager
    let work_queue_manager =
        WorkQueueManager::new(storage, work_queue_config.clone(), sysdb.clone());
    let work_queue_handle = system.start_component(work_queue_manager);

    // Create and start gRPC server
    let work_queue_server = WorkQueueServer::new(work_queue_handle.clone(), sysdb);
    let server = work_queue_server.into_service();
    let port = service_config.my_port;

    // Create health service for readiness probe
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let addr = format!("0.0.0.0:{}", port).parse().unwrap();

    println!("Work queue service starting on {}", addr);

    // Start server (this blocks forever)
    tonic::transport::Server::builder()
        .add_service(server)
        .add_service(health_service)
        .serve(addr)
        .await
        .expect("Failed to start work queue service");
}
