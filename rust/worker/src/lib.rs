mod assignment;
mod blockstore;
mod compactor;
mod config;
pub mod distance;
mod errors;
mod execution;
mod index;
mod log;
mod memberlist;
mod segment;
mod server;
mod storage;
mod sysdb;
mod system;
mod tracing;
mod types;
mod utils;
use config::Configurable;
use memberlist::MemberlistProvider;
use rand::Rng;
use tokio::io::AsyncReadExt;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

mod chroma_proto {
    tonic::include_proto!("chroma");
}

pub async fn s3_test_entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => config::RootConfig::load_from_path(&config_path),
        Err(_) => config::RootConfig::load(),
    };

    let mb = 500;
    let n_requests = 10;
    let n_max = 10;

    let storage = storage::from_config(&config.query_service.storage)
        .await
        .expect("Failed to create storage");

    let file_size_bytes = mb * 1024 * 1024; // 500MB
    let file_name_prefix = "test_file";
    let random_suffix: u32 = rand::thread_rng().gen();
    let full_key = format!("{}_{}", file_name_prefix, random_suffix);

    // Generate a random byte array of size file_size_bytes
    let mut rng = rand::thread_rng();
    let byte = rng.gen::<u8>();
    let mut file_data: Vec<u8> = vec![byte; file_size_bytes];
    println!("Generated random file data of size: {}", file_size_bytes);

    // Write the file to the storage
    storage.put_bytes(&full_key, file_data).await.unwrap();
    println!("Wrote file to storage with key: {}", full_key);

    // Read the file from the storage
    let start_req_time = std::time::Instant::now();
    let mut read_file_data = storage.get(&full_key).await.unwrap();
    let get_time = std::time::Instant::now();
    println!(
        "Initial get time from storage with key: {} in {:?} seconds",
        full_key,
        get_time.duration_since(start_req_time).as_secs_f64()
    );
    let mut read_buffer: Vec<u8> = Vec::new();
    read_file_data.read_to_end(&mut read_buffer).await.unwrap();
    let end_req_time = std::time::Instant::now();
    let req_time = end_req_time - start_req_time;
    println!(
        "Read file from storage with key: {} in {:?} seconds",
        full_key,
        req_time.as_secs_f64()
    );

    storage.get_parallel(n_requests, n_max, &full_key).await;
}

pub async fn query_service_entrypoint() {
    // Check if the config path is set in the env var
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => config::RootConfig::load_from_path(&config_path),
        Err(_) => config::RootConfig::load(),
    };

    let config = config.query_service;

    crate::tracing::opentelemetry_config::init_otel_tracing(
        &config.service_name,
        &config.otel_endpoint,
    );

    let system: system::System = system::System::new();
    let dispatcher =
        match execution::dispatcher::Dispatcher::try_from_config(&config.dispatcher).await {
            Ok(dispatcher) => dispatcher,
            Err(err) => {
                println!("Failed to create dispatcher component: {:?}", err);
                return;
            }
        };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut worker_server = match server::WorkerServer::try_from_config(&config).await {
        Ok(worker_server) => worker_server,
        Err(err) => {
            println!("Failed to create worker server component: {:?}", err);
            return;
        }
    };
    worker_server.set_system(system.clone());
    worker_server.set_dispatcher(dispatcher_handle.clone());

    let server_join_handle = tokio::spawn(async move {
        let _ = crate::server::WorkerServer::run(worker_server).await;
    });

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sigterm) => sigterm,
        Err(e) => {
            println!("Failed to create signal handler: {:?}", e);
            return;
        }
    };

    println!("Waiting for SIGTERM to stop the server");
    select! {
        // Kubernetes will send SIGTERM to stop the pod gracefully
        // TODO: add more signal handling
        _ = sigterm.recv() => {
            dispatcher_handle.stop();
            dispatcher_handle.join().await;
            system.stop().await;
            system.join().await;
        },
    };
    println!("Server stopped");
}

pub async fn compaction_service_entrypoint() {
    // Check if the config path is set in the env var
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => config::RootConfig::load_from_path(&config_path),
        Err(_) => config::RootConfig::load(),
    };

    let config = config.compaction_service;

    crate::tracing::opentelemetry_config::init_otel_tracing(
        &config.service_name,
        &config.otel_endpoint,
    );

    let system: system::System = system::System::new();

    let mut memberlist = match memberlist::CustomResourceMemberlistProvider::try_from_config(
        &config.memberlist_provider,
    )
    .await
    {
        Ok(memberlist) => memberlist,
        Err(err) => {
            println!("Failed to create memberlist component: {:?}", err);
            return;
        }
    };

    let dispatcher =
        match execution::dispatcher::Dispatcher::try_from_config(&config.dispatcher).await {
            Ok(dispatcher) => dispatcher,
            Err(err) => {
                println!("Failed to create dispatcher component: {:?}", err);
                return;
            }
        };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut compaction_manager =
        match crate::compactor::CompactionManager::try_from_config(&config).await {
            Ok(compaction_manager) => compaction_manager,
            Err(err) => {
                println!("Failed to create compaction manager component: {:?}", err);
                return;
            }
        };
    compaction_manager.set_dispatcher(dispatcher_handle.clone());
    compaction_manager.set_system(system.clone());

    let mut compaction_manager_handle = system.start_component(compaction_manager);
    memberlist.subscribe(compaction_manager_handle.receiver());

    let mut memberlist_handle = system.start_component(memberlist);

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sigterm) => sigterm,
        Err(e) => {
            println!("Failed to create signal handler: {:?}", e);
            return;
        }
    };
    println!("Waiting for SIGTERM to stop the server");
    select! {
        // Kubernetes will send SIGTERM to stop the pod gracefully
        // TODO: add more signal handling
        _ = sigterm.recv() => {
            memberlist_handle.stop();
            memberlist_handle.join().await;
            dispatcher_handle.stop();
            dispatcher_handle.join().await;
            compaction_manager_handle.stop();
            compaction_manager_handle.join().await;
            system.stop().await;
            system.join().await;
        },
    };
    println!("Server stopped");
}
