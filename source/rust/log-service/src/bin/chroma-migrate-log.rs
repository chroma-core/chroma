use tonic::transport::Channel;
use uuid::Uuid;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::MigrateLogRequest;
use chroma_types::CollectionUuid;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("USAGE: chroma-migrate-log [HOST] [COLLECTION-UUID]");
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
        .migrate_log(MigrateLogRequest {
            collection_id: collection_id.to_string(),
        })
        .await
        .expect("migrate log request should succeed");
}
