use tonic::transport::Channel;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::InspectLogStateRequest;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("USAGE: chroma-inspect-log-state [HOST] [COLLECTION_UUID]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let mut client = LogServiceClient::new(logservice);
    let state = client
        .inspect_log_state(InspectLogStateRequest {
            collection_id: args[1].clone(),
        })
        .await
        .expect("could not inspect log state");
    let state = state.into_inner();
    println!("{}", state.debug);
}
