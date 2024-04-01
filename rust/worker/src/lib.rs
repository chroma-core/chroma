mod assignment;
mod blockstore;
mod compactor;
mod config;
mod distance;
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
mod types;

use config::Configurable;

mod chroma_proto {
    tonic::include_proto!("chroma");
}

pub async fn query_service_entrypoint() {
    let config = config::RootConfig::load();
    let system: system::System = system::System::new();
    let dispatcher = match execution::dispatcher::Dispatcher::try_from_config(&config.worker).await
    {
        Ok(dispatcher) => dispatcher,
        Err(err) => {
            println!("Failed to create dispatcher component: {:?}", err);
            return;
        }
    };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut worker_server = match server::WorkerServer::try_from_config(&config.worker).await {
        Ok(worker_server) => worker_server,
        Err(err) => {
            println!("Failed to create worker server component: {:?}", err);
            return;
        }
    };
    worker_server.set_system(system);
    worker_server.set_dispatcher(dispatcher_handle.receiver());

    let server_join_handle = tokio::spawn(async move {
        crate::server::WorkerServer::run(worker_server).await;
    });

    let _ = tokio::join!(server_join_handle, dispatcher_handle.join());
}

pub async fn compaction_service_entrypoint() {
    let config = config::RootConfig::load();
    let system: system::System = system::System::new();
    let dispatcher = match execution::dispatcher::Dispatcher::try_from_config(&config.worker).await
    {
        Ok(dispatcher) => dispatcher,
        Err(err) => {
            println!("Failed to create dispatcher component: {:?}", err);
            return;
        }
    };
    let mut dispatcher_handle = system.start_component(dispatcher);
    let mut compaction_manager =
        match crate::compactor::CompactionManager::try_from_config(&config.worker).await {
            Ok(compaction_manager) => compaction_manager,
            Err(err) => {
                println!("Failed to create compaction manager component: {:?}", err);
                return;
            }
        };
    compaction_manager.set_dispatcher(dispatcher_handle.receiver());
    compaction_manager.set_system(system.clone());
    let mut compaction_manager_handle = system.start_component(compaction_manager);
    tokio::join!(compaction_manager_handle.join(), dispatcher_handle.join());
}
