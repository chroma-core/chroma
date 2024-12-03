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
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&req)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                let uuid = match resp.text().await {
                    Ok(uuid) => uuid,
                    Err(err) => {
                        eprintln!("Failed to start workload on {}: {}", args.host, err);
                        return;
                    }
                };
                println!(
                    "Started workload on {}:\n{}",
                    args.host,
                    // SAFETY(rescrv):  serde_json::to_string_pretty should always convert to JSON
                    // when it just parses as JSON.
                    uuid,
                );
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
