//! Stop all workloads on the chroma-load server.
//!
//! If you are looking to stop traffic for a SEV, see chroma-load-inhibit.

use clap::Parser;

use chroma_load::rest::StopRequest;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    host: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();
    match client
        .get(format!("{}/", args.host))
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
    {
        Ok(resp) => {
            let resp = resp.error_for_status().expect("Failed to get status");
            let resp = resp
                .json::<chroma_load::rest::Status>()
                .await
                .expect("Failed to parse status");
            for workload in resp.running {
                let req = StopRequest {
                    uuid: workload.uuid,
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
        }
        Err(e) => {
            eprintln!("Failed to get status: {}", e);
        }
    }
}
