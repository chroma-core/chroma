use tonic::transport::Channel;
use uuid::Uuid;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::PurgeDirtyForCollectionRequest;
use chroma_types::CollectionUuid;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("USAGE: chroma-purge-dirty-log [HOST] [COLLECTION-UUID]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let collection_id = Uuid::parse_str(&args[1])
        .map(CollectionUuid)
        .expect("Failed to parse collection_id");
    let mut client = LogServiceClient::new(logservice);
    let _resp = client
        .purge_dirty_for_collection(PurgeDirtyForCollectionRequest {
            collection_ids: vec![collection_id.to_string()],
        })
        .await
        .expect("purge-dirty-log request should succeed");
}
