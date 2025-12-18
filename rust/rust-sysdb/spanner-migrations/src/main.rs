//! Spanner migration CLI binary (stub).
//! TODO: Implement actual migration logic in follow-up PR.

mod bootstrap;
mod config;

use chroma_config::spanner::SpannerConfig;
use config::RootConfig;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("rust-sysdb-migration stub - migrations not yet implemented");

    let config = match RootConfig::load() {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    match &config.spanner {
        SpannerConfig::Emulator(emulator) => {
            // Bootstrap emulator (create instance/database if needed)
            if let Err(e) = bootstrap::bootstrap_emulator(emulator).await {
                tracing::error!("Failed to bootstrap emulator: {}", e);
                std::process::exit(1);
            }
            tracing::info!("Emulator bootstrapped at {}", emulator.grpc_endpoint());
        }
    }

    tracing::info!("Migration stub complete");
}
