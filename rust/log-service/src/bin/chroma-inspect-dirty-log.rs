use tonic::transport::Channel;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::InspectDirtyLogRequest;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 {
        eprintln!("USAGE: chroma-inspect-dirty-log [HOST]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let mut client = LogServiceClient::new(logservice);
    let dirty = client
        .inspect_dirty_log(InspectDirtyLogRequest {})
        .await
        .expect("could not inspect dirty log");
    let dirty = dirty.into_inner();
    for line in dirty.markers {
        println!("{line}");
    }
}
