use tonic::transport::Channel;

use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{PullLogsRequest, ScoutLogsRequest};

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 && args.len() != 4 {
        eprintln!("USAGE: chroma-inspect-log-state [HOST] [COLLECTION_UUID]");
        std::process::exit(13);
    }
    let logservice = Channel::from_shared(args[0].clone())
        .expect("could not create channel")
        .connect()
        .await
        .expect("could not connect to log service");
    let mut client = LogServiceClient::new(logservice);
    if args.len() == 4 {
        let start: u64 = args[2].parse().unwrap();
        let limit: u64 = args[3].parse().unwrap();
        let batch_size: u32 = limit.saturating_sub(start).try_into().unwrap();
        let i = start;
        println!("Fetching [{start}:{})", start + batch_size as u64);
        let pulled = client
            .pull_logs(PullLogsRequest {
                collection_id: args[1].clone(),
                start_from_offset: start as i64,
                batch_size: batch_size as i32,
                end_timestamp: i64::MAX,
            })
            .await
            .expect("could not pull logs");
        let pulled = pulled.into_inner();
        for (j, record) in pulled.records.into_iter().enumerate() {
            println!(
                "{} {} {} {}",
                i as usize + j,
                record.log_offset,
                record.record.as_ref().map(|r| r.operation).unwrap_or(4),
                record
                    .record
                    .as_ref()
                    .map(|r| r.id.as_str())
                    .unwrap_or("<NONE>")
            );
        }
    } else {
        let scouted = client
            .scout_logs(ScoutLogsRequest {
                collection_id: args[1].clone(),
            })
            .await
            .expect("could not inspect log state");
        let scouted = scouted.into_inner();
        println!("Scouted {scouted:?}");
        for i in (scouted.first_uncompacted_record_offset..=scouted.first_uninserted_record_offset)
            .step_by(100)
        {
            let batch_size: i32 =
                (std::cmp::min(i + 100, scouted.first_uninserted_record_offset) - i) as i32;
            println!("Fetching [{i}:{})", i + batch_size as i64);
            let pulled = client
                .pull_logs(PullLogsRequest {
                    collection_id: args[1].clone(),
                    start_from_offset: i,
                    batch_size,
                    end_timestamp: i64::MAX,
                })
                .await
                .expect("could not pull logs");
            let pulled = pulled.into_inner();
            for (j, record) in pulled.records.into_iter().enumerate() {
                println!(
                    "{} {} {} {}",
                    i as usize + j,
                    record.log_offset,
                    record.record.as_ref().map(|r| r.operation).unwrap_or(4),
                    record
                        .record
                        .as_ref()
                        .map(|r| r.id.as_str())
                        .unwrap_or("<NONE>")
                );
            }
        }
    }
}
