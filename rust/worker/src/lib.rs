mod assignment;
mod blockstore;
mod compactor;
mod config;
mod errors;
mod execution;
mod index;
mod ingest;
mod log;
mod memberlist;
mod segment;
mod server;
mod storage;
mod sysdb;
mod system;
mod types;

use crate::sysdb::sysdb::SysDb;
use config::Configurable;
use memberlist::MemberlistProvider;

mod chroma_proto {
    tonic::include_proto!("chroma");
}

pub async fn query_service_entrypoint() {
    let config = config::RootConfig::load();
    let system: system::System = system::System::new();
    let segment_manager = match segment::SegmentManager::try_from_config(&config.worker).await {
        Ok(segment_manager) => segment_manager,
        Err(err) => {
            println!("Failed to create segment manager component: {:?}", err);
            return;
        }
    };
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
    worker_server.set_segment_manager(segment_manager.clone());
    worker_server.set_dispatcher(dispatcher_handle.receiver());

    let server_join_handle = tokio::spawn(async move {
        crate::server::WorkerServer::run(worker_server).await;
    });

    let _ = tokio::join!(server_join_handle, dispatcher_handle.join());
}

pub async fn worker_entrypoint() {
    let config = config::RootConfig::load();
    // Create all the core components and start them
    // TODO: This should be handled by an Application struct and we can push the config into it
    // for now we expose the config to pub and inject it into the components

    // The two root components are ingest, and the gRPC server
    let mut system: system::System = system::System::new();

    let mut ingest = match ingest::Ingest::try_from_config(&config.worker).await {
        Ok(ingest) => ingest,
        Err(err) => {
            println!("Failed to create ingest component: {:?}", err);
            return;
        }
    };

    let mut memberlist =
        match memberlist::CustomResourceMemberlistProvider::try_from_config(&config.worker).await {
            Ok(memberlist) => memberlist,
            Err(err) => {
                println!("Failed to create memberlist component: {:?}", err);
                return;
            }
        };

    let mut scheduler = ingest::RoundRobinScheduler::new();

    let segment_manager = match segment::SegmentManager::try_from_config(&config.worker).await {
        Ok(segment_manager) => segment_manager,
        Err(err) => {
            println!("Failed to create segment manager component: {:?}", err);
            return;
        }
    };

    let mut segment_ingestor_receivers =
        Vec::with_capacity(config.worker.num_indexing_threads as usize);
    for _ in 0..config.worker.num_indexing_threads {
        let segment_ingestor = segment::SegmentIngestor::new(segment_manager.clone());
        let segment_ingestor_handle = system.start_component(segment_ingestor);
        let recv = segment_ingestor_handle.receiver();
        segment_ingestor_receivers.push(recv);
    }

    let mut worker_server = match server::WorkerServer::try_from_config(&config.worker).await {
        Ok(worker_server) => worker_server,
        Err(err) => {
            println!("Failed to create worker server component: {:?}", err);
            return;
        }
    };
    worker_server.set_segment_manager(segment_manager.clone());

    // Boot the system
    // memberlist -> ingest -> scheduler -> NUM_THREADS x segment_ingestor -> segment_manager
    // server <- segment_manager

    for recv in segment_ingestor_receivers {
        scheduler.subscribe(recv);
    }

    let mut scheduler_handler = system.start_component(scheduler);
    ingest.subscribe(scheduler_handler.receiver());

    let mut ingest_handle = system.start_component(ingest);
    let recv = ingest_handle.receiver();
    memberlist.subscribe(recv);
    let mut memberlist_handle = system.start_component(memberlist);

    let server_join_handle = tokio::spawn(async move {
        crate::server::WorkerServer::run(worker_server).await;
    });

    // Join on all handles
    let _ = tokio::join!(
        ingest_handle.join(),
        memberlist_handle.join(),
        scheduler_handler.join(),
        server_join_handle,
    );
}
