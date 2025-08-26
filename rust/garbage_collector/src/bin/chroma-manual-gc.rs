use tonic::transport::Channel;

use chroma_types::chroma_proto::garbage_collector_client::GarbageCollectorClient;
use chroma_types::chroma_proto::KickoffGarbageCollectionRequest;

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("USAGE: chroma-manual-gc [HOST] [COLLECTION_UUID]");
        std::process::exit(13);
    }
    let gcservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to gc service");
    let mut client = GarbageCollectorClient::new(gcservice);
    client
        .kickoff_garbage_collection(KickoffGarbageCollectionRequest {
            collection_id: args[1].clone(),
        })
        .await
        .expect("could not kickoff gc");
}
