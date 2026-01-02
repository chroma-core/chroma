//! Spanner migration CLI binary.

mod bootstrap;
mod config;
mod migrations;
mod runner;

use chroma_config::spanner::SpannerConfig;
use chroma_tracing::{init_global_filter_layer, init_otel_layer, init_stdout_layer, init_tracing};
use config::{MigrationMode, RootConfig};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use google_cloud_spanner::client::{Client, ClientConfig};
use migrations::MIGRATION_DIRS;
use runner::MigrationRunner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check for --generate-sum flag, don't need configs for this
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--generate-sum") {
        println!("Generating migrations.sum files...");
        for dir in MIGRATION_DIRS.iter() {
            let manifest_content = dir.generate_manifest()?;
            let manifest_path = "migrations/migrations.sum";
            std::fs::write(manifest_path, manifest_content)?;
            println!("  Wrote {}", manifest_path);
        }
        println!("Done!");
        return Ok(());
    }

    let config = match RootConfig::load() {
        Ok(config) => config,
        Err(e) => {
            panic!("Failed to load configuration: {}", e);
        }
    };

    // Initialize tracing with OTLP export for Tilt
    let tracing_layers = vec![
        init_global_filter_layer(&config.otel_filters),
        init_otel_layer(&config.service_name, &config.otel_endpoint),
        init_stdout_layer(),
    ];
    init_tracing(tracing_layers);

    let runner = match &config.spanner {
        SpannerConfig::Emulator(emulator) => {
            // Bootstrap emulator (create instance/database if needed)
            if let Err(e) = bootstrap::bootstrap_emulator(emulator).await {
                panic!("Failed to bootstrap emulator: {}", e);
            }

            let client_config = ClientConfig {
                environment: Environment::Emulator(emulator.grpc_endpoint()),
                ..Default::default()
            };
            let admin_client_config = AdminClientConfig {
                environment: Environment::Emulator(emulator.grpc_endpoint()),
            };

            tracing::info!(
                "Connecting to Spanner database {} in emulator",
                emulator.database_path()
            );

            let client = Client::new(&emulator.database_path(), client_config).await?;
            let admin_client = AdminClient::new(admin_client_config).await?;

            tracing::info!(
                "Connected to Spanner database {} in emulator",
                emulator.database_path()
            );

            MigrationRunner::new(client, admin_client, emulator.database_path())
        }
        SpannerConfig::Gcp(gcp) => {
            let client_config = ClientConfig::default().with_auth().await?;
            let admin_client_config = AdminClientConfig::default().with_auth().await?;

            tracing::info!(
                "Connecting to Spanner database {} in gcp",
                gcp.database_path()
            );

            let client = Client::new(&gcp.database_path(), client_config).await?;
            let admin_client = AdminClient::new(admin_client_config).await?;

            tracing::info!(
                "Connected to Spanner database {} in gcp",
                gcp.database_path()
            );

            MigrationRunner::new(client, admin_client, gcp.database_path())
        }
    };

    match config.migration_mode {
        MigrationMode::Apply => {
            tracing::info!("Initializing migrations table...");
            runner.initialize_migrations_table().await?;

            tracing::info!("Applying migrations...");
            runner.apply_all_migrations().await?;

            tracing::info!("Migrations applied successfully!");
        }
        MigrationMode::Validate => {
            tracing::info!("Validating migrations...");
            runner.validate_all_migrations().await?;

            tracing::info!("All migrations are applied!");
        }
    }

    Ok(())
}
