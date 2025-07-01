use tonic::transport::Channel;
use uuid::Uuid;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::scrub_log_request::LogToScrub;
use chroma_types::chroma_proto::ScrubLogRequest;
use chroma_types::CollectionUuid;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("USAGE: chroma-scrub-log [HOST] [LOG]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .timeout(std::time::Duration::from_secs(600))
        .connect()
        .await
        .expect("could not connect to log service");
    let log_to_scrub = if let Ok(collection_id) = Uuid::parse_str(&args[1]).map(CollectionUuid) {
        LogToScrub::CollectionId(collection_id.to_string())
    } else {
        LogToScrub::DirtyLog(args[1].clone())
    };
    let mut client = LogServiceClient::new(logservice);
    let resp = client
        // Approximately $1 to scrub
        .scrub_log(ScrubLogRequest {
            log_to_scrub: Some(log_to_scrub),
            max_bytes_to_read: 10_000_000_000,
            max_files_to_read: 100_000,
        })
        .await
        .expect("chroma-scrub-log request should succeed");
    let resp = resp.into_inner();
    eprintln!("{resp:#?}");
}
