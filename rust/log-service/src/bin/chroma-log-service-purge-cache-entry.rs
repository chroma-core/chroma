use tonic::transport::Channel;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{
    purge_from_cache_request::EntryToEvict, FragmentToEvict, PurgeFromCacheRequest,
};

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 && args.len() != 5 {
        eprintln!(
            "USAGE: chroma-log-service-purge-cache-entry HOST DATABASE_NAME TYPE COLLECTION_UUID [FRAGMENT_PATH]"
        );
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let database_name = args[1].clone();
    let req = match args[2].as_str() {
        "cursor" => {
            if args.len() != 4 {
                eprintln!("purge cache entry takes no fragment path");
                std::process::exit(13);
            }
            PurgeFromCacheRequest {
                entry_to_evict: Some(EntryToEvict::CursorForCollectionId(args[3].clone())),
                database_name,
            }
        }
        "manifest" => {
            if args.len() != 4 {
                eprintln!("purge cache entry takes no fragment path");
                std::process::exit(13);
            }
            PurgeFromCacheRequest {
                entry_to_evict: Some(EntryToEvict::ManifestForCollectionId(args[3].clone())),
                database_name,
            }
        }
        "fragment" => {
            if args.len() != 5 {
                eprintln!("purge cache entry takes a fragment path");
                std::process::exit(13);
            }
            PurgeFromCacheRequest {
                entry_to_evict: Some(EntryToEvict::Fragment(FragmentToEvict {
                    collection_id: args[3].clone(),
                    fragment_path: args[4].clone(),
                })),
                database_name,
            }
        }
        _ => {
            eprintln!("unknown type: {}", args[2]);
            std::process::exit(13);
        }
    };
    let mut client = LogServiceClient::new(logservice);
    let _state = client
        .purge_from_cache(req)
        .await
        .expect("could not purge from cache");
}
