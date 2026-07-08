use crate::config::RootConfig;
use crate::fn_consumer::fn_consumer_manager::FnConsumerManager;
use crate::work_queue::work_queue_client::WorkQueueClient;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_index::{hnsw_provider::HnswIndexProvider, usearch::USearchIndexProvider};
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use tonic::transport::Server;

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
                tracing::error!("Failed to create dispatcher: {:?}", err);
                return;
            }
        };
    let dispatcher_handle = system.start_component(dispatcher);

    // Create log service
    let log = match Log::try_from_config(&(service_config.log.clone(), system.clone()), &registry)
        .await
    {
        Ok(log) => log,
        Err(err) => {
            tracing::error!("Failed to create log service: {:?}", err);
            return;
        }
    };

    // Create sysdb
    let sysdb = match SysDb::try_from_config(&(service_config.sysdb.clone(), None), &registry).await
    {
        Ok(sysdb) => sysdb,
        Err(err) => {
            tracing::error!("Failed to create sysdb: {:?}", err);
            return;
        }
    };

    // Create storage
    let storage = match Storage::try_from_config(&service_config.storage, &registry).await {
        Ok(storage) => storage,
        Err(err) => {
            tracing::error!("Failed to create storage: {:?}", err);
            return;
        }
    };

    // Create blockfile provider
    let blockfile_provider = match BlockfileProvider::try_from_config(
        &(service_config.blockfile_provider.clone(), storage.clone()),
        &registry,
    )
    .await
    {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("Failed to create blockfile provider: {:?}", err);
            return;
        }
    };

    // Create hnsw provider
    let hnsw_provider = match HnswIndexProvider::try_from_config(
        &(service_config.hnsw_provider.clone(), storage.clone()),
        &registry,
    )
    .await
    {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("Failed to create hnsw provider: {:?}", err);
            return;
        }
    };

    // Create usearch provider for spann
    let usearch_cache = match chroma_cache::from_config(
        &service_config.spann_provider.usearch_provider.cache_config,
    )
    .await
    {
        Ok(cache) => cache,
        Err(err) => {
            tracing::error!("Failed to create usearch cache: {:?}", err);
            return;
        }
    };
    let usearch_provider = USearchIndexProvider::new(storage.clone(), usearch_cache);

    // Create spann provider
    let spann_provider = match SpannProvider::try_from_config(
        &(
            hnsw_provider.clone(),
            blockfile_provider.clone(),
            service_config.spann_provider.clone(),
            usearch_provider,
        ),
        &registry,
    )
    .await
    {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("Failed to create spann provider: {:?}", err);
            return;
        }
    };

    // Connect to the work queue
    let work_queue_client =
        match WorkQueueClient::try_from_config(&service_config.fn_consumer.work_queue).await {
            Ok(client) => {
                tracing::info!(
                    "WorkQueue client initialized for {}:{}",
                    service_config.fn_consumer.work_queue.host,
                    service_config.fn_consumer.work_queue.port
                );
                client
            }
            Err(err) => {
                tracing::error!("Failed to initialize WorkQueue client: {:?}", err);
                return;
            }
        };

    // Build and start the manager
    let mut manager = FnConsumerManager::new(
        service_config.fn_consumer.clone(),
        config.compaction_service.compactor.clone(),
        service_config.my_member_id.clone(),
        system.clone(),
        work_queue_client.clone(),
        log,
        sysdb,
        blockfile_provider,
        hnsw_provider,
        spann_provider,
    );
    manager.set_dispatcher(dispatcher_handle);
    let _manager_handle = system.start_component(manager);

    // Create health service for readiness probe
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let addr = format!("0.0.0.0:{}", service_config.my_port)
        .parse()
        .unwrap();

    println!("fn-consumer service starting on {}", addr);

    // Start server (this blocks forever)
    Server::builder()
        .add_service(health_service)
        .serve(addr)
        .await
        .expect("Failed to start fn-consumer service");
}
