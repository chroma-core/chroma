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
                let status = resp.json::<chroma_load::rest::Status>().await;
                println!("{:#?}", status);
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
