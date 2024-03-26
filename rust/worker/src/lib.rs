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

pub async fn worker_entrypoint() {
    let config = config::RootConfig::load();
    // Create all the core components and start them
    // TODO: This should be handled by an Application struct and we can push the config into it
    // for now we expose the config to pub and inject it into the components

    // The two root components are ingest, and the gRPC server
    let mut system: system::System = system::System::new();

    let mut memberlist =
        match memberlist::CustomResourceMemberlistProvider::try_from_config(&config.worker).await {
            Ok(memberlist) => memberlist,
            Err(err) => {
                println!("Failed to create memberlist component: {:?}", err);
                return;
            }
        };

    let mut worker_server = match server::WorkerServer::try_from_config(&config.worker).await {
        Ok(worker_server) => worker_server,
        Err(err) => {
            println!("Failed to create worker server component: {:?}", err);
            return;
        }
    };

    // Boot the system
    // memberlist -> (This is broken for now until we have compaction manager) NUM_THREADS x segment_ingestor -> segment_manager
    // server <- segment_manager

    // memberlist.subscribe(recv);
    let mut memberlist_handle = system.start_component(memberlist);

    let server_join_handle = tokio::spawn(async move {
        crate::server::WorkerServer::run(worker_server).await;
    });

    // Join on all handles
    let _ = tokio::join!(memberlist_handle.join(), server_join_handle,);
}
