//! Example: Run a local Chroma server from Rust
//!
//! Run with: cargo run -p chroma --features server --example run_server

use chroma::server::{ChromaServer, FrontendServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the server
    let mut config = FrontendServerConfig::single_node_default();
    config.port = 8000;
    config.listen_address = "127.0.0.1".to_string();
    config.persist_path = "./chroma_data".to_string();

    println!("Starting Chroma server on http://127.0.0.1:8000 ...");
    println!("Data will be stored in: ./chroma_data");

    // Start the server (this will block until ready)
    let server = ChromaServer::with_config(config).await?;

    println!("Server is ready at: {}", server.endpoint());
    println!("Press Ctrl+C to stop");

    // Keep running until interrupted
    tokio::signal::ctrl_c().await?;

    println!("\nShutting down...");
    drop(server);

    Ok(())
}
