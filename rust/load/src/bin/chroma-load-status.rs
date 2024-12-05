//! Inspect chroma-load

use clap::Parser;

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
        .get(&args.host)
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                let status = match resp.json::<chroma_load::rest::Status>().await {
                    Ok(status) => status,
                    Err(e) => {
                        eprintln!("Failed to fetch workload status on {}: {}", args.host, e);
                        return;
                    }
                };
                if status.inhibited {
                    println!("inhibited");
                } else {
                    for running in status.running {
                        println!(
                            "{} {} {} {} {}",
                            running.uuid,
                            running.expires,
                            running.name,
                            running.data_set,
                            // SAFETY(rescrv):  WorkloadSummary always converts to JSON.
                            serde_json::to_string(&running.workload).unwrap()
                        );
                    }
                }
            } else {
                eprintln!(
                    "Failed to get workload status on {}: {}",
                    args.host,
                    resp.status()
                );
            }
        }
        Err(e) => eprintln!("Failed to get workload status on {}: {}", args.host, e),
    }
}
