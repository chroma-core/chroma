use tonic::transport::Channel;

use chroma_types::chroma_proto::garbage_collector_client::GarbageCollectorClient;
use chroma_types::chroma_proto::KickoffGarbageCollectionRequest;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 1 {
        eprintln!("USAGE: garbage_collector_manual_kickoff [HOST] [COLLECTION_UUID ...]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let mut client = GarbageCollectorClient::new(logservice);
    for arg in args.into_iter().skip(1) {
        let _state = client
            .kickoff_garbage_collection(KickoffGarbageCollectionRequest {
                collection_id: arg.clone(),
            })
            .await
            .expect("could not inspect log state");
        println!("{arg}");
    }
}
