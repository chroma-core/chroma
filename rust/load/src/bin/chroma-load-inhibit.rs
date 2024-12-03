//! Inhibit chroma-load on every host provided on the command line.

#[tokio::main]
async fn main() {
    for host in std::env::args().skip(1) {
        let client = reqwest::Client::new();
        match client.post(format!("{}/inhibit", host)).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    println!("Inhibited load on {}", host);
                } else {
                    eprintln!("Failed to inhibit load on {}: {}", host, resp.status());
                }
            }
            Err(e) => eprintln!("Failed to inhibit load on {}: {}", host, e),
        }
    }
}
