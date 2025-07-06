//! Stop a single workload on the chroma-load server.
//!
//! If you are looking to stop traffic for a SEV, see chroma-load-inhibit.

use clap::Parser;
use uuid::Uuid;

use chroma_load::rest::StopRequest;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    uuid: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();
    let req = StopRequest {
        uuid: Uuid::parse_str(&args.uuid).unwrap(),
    };
    match client
        .post(format!("{}/stop", args.host))
        .json(&req)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                println!("Stopped workload on {}", args.host);
            } else {
                eprintln!(
                    "Failed to stop workload on {}: {}",
                    args.host,
                    resp.status()
                );
            }
        }
        Err(e) => eprintln!("Failed to stop workload on {}: {}", args.host, e),
    }
}
