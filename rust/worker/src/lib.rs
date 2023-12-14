mod assignment;
mod config;
mod errors;
mod index;
mod ingest;
mod memberlist;
mod system;
mod types;

use config::Configurable;
use memberlist::MemberlistProvider;

mod chroma_proto {
    tonic::include_proto!("chroma");
}

pub async fn worker_entrypoint() {
    let config = config::RootConfig::load();
    // Create all the core components and start them
    // TODO: This should be handled by an Application struct and we can push the config into it
    // for now we expose the config to pub and inject it into the components

    // The two root components are ingest, and the gRPC server

    let ingest = match ingest::Ingest::try_from_config(&config.worker).await {
        Ok(ingest) => ingest,
        Err(err) => {
            println!("Failed to create ingest component: {:?}", err);
            return;
        }
    };

    let memberlist =
        match memberlist::CustomResourceMemberlistProvider::try_from_config(&config.worker).await {
            Ok(memberlist) => memberlist,
            Err(err) => {
                println!("Failed to create memberlist component: {:?}", err);
                return;
            }
        };

    // Boot the system
    let mut system = system::System::new();
    let (mut ingest_handle, ingest_sender) = system.start_component(ingest);
    memberlist.subscribe(ingest_sender);
    let (mut memberlist_handle, _) = system.start_component(memberlist);
    // Join on all handles
    let _ = tokio::join!(ingest_handle.join(), memberlist_handle.join());
}
