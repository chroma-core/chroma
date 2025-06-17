use tonic::transport::Channel;
use uuid::Uuid;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::{CheckCollectionsRequest, UpdateCollectionLogOffsetRequest};
use chroma_types::CollectionUuid;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        eprintln!("USAGE: chroma-migrate-log [LOG-HOST] [SYSDB-HOST] [COLLECTION-UUID]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let mut log_client = LogServiceClient::new(logservice);
    let sysdbservice = Channel::from_shared(args[1].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to sysdb service");
    let mut sysdb_client = SysDbClient::new(sysdbservice);
    let collection_id = Uuid::parse_str(&args[2])
        .map(CollectionUuid)
        .expect("Failed to parse collection_id");
    let collection_info = sysdb_client
        .check_collections(CheckCollectionsRequest {
            collection_ids: vec![args[2].clone()],
        })
        .await
        .expect("could not fetch collection info")
        .into_inner();
    eprintln!("{collection_info:?}");
    if collection_info.deleted.len() != 1 || collection_info.log_position.len() != 1 {
        eprintln!("got abnormal/non-length-1 results");
        std::process::exit(13);
    }
    if collection_info.deleted[0] {
        eprintln!("cowardly refusing to do anything with a deleted collection");
        std::process::exit(13);
    }
    let _resp = log_client
        .update_collection_log_offset(UpdateCollectionLogOffsetRequest {
            collection_id: collection_id.to_string(),
            log_offset: collection_info.log_position[0],
        })
        .await
        .expect("migrate log request should succeed");
}
