//! Start a workload on the chroma-load server.

use clap::Parser;

use chroma_load::rest::StartRequest;
use chroma_load::{humanize_expires, Workload};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    name: String,
    #[arg(long)]
    expires: String,
    #[arg(long)]
    data_set: String,
    #[arg(long)]
    workload: String,
    #[arg(long)]
    throughput: f64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();
    let req = StartRequest {
        name: args.name,
        expires: humanize_expires(&args.expires).unwrap_or(args.expires),
        data_set: args.data_set,
        workload: Workload::ByName(args.workload),
        throughput: args.throughput,
    };
    match client
        .post(format!("{}/start", args.host))
        .json(&req)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                println!("Started workload on {}", args.host);
            } else {
                eprintln!(
                    "Failed to start workload on {}: {}",
                    args.host,
                    resp.status()
                );
            }
        }
        Err(e) => eprintln!("Failed to start workload on {}: {}", args.host, e),
    }
}
