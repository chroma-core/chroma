//! Spanner migration CLI binary.

mod bootstrap;
mod config;
mod migrations;
mod runner;

use chroma_config::spanner::SpannerConfig;
use chroma_tracing::{init_global_filter_layer, init_otel_layer, init_stdout_layer, init_tracing};
use clap::{Parser, Subcommand};
use config::{MigrationMode, RootConfig};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use google_cloud_spanner::client::{Client, ClientConfig};
use migrations::MIGRATION_DIRS;
use runner::MigrationRunner;

/// Spanner migration CLI for managing database migrations.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Filter migrations by slug (e.g., "spanner_sysdb" or "spanner_logdb").
    #[arg(long, global = true)]
    slug: Option<String>,

    /// Root directory for outputting migrations (used with generate-sum command).
    #[arg(long, global = true)]
    root: Option<String>,
}

/// Available commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Generate migrations.sum files from the embedded migrations.
    GenerateSum,
    /// Apply migrations to the database (default behavior if no command is specified).
    Apply,
    /// Validate that all migrations have been applied.
    Validate,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Handle generate-sum command without requiring configs.
    if matches!(args.command, Some(Command::GenerateSum)) {
        let root = args.root.as_deref().unwrap_or(".");
        println!("Generating migrations.sum files...");
        for dir in MIGRATION_DIRS.iter() {
            let manifest_content = dir.generate_manifest()?;
            let manifest_path = std::path::Path::new(root)
                .join(dir.folder_name())
                .join(dir.manifest_filename());
            if let Some(parent) = manifest_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&manifest_path, manifest_content)?;
            println!("  Wrote {}", manifest_path.display());
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

    // Determine the migration mode: CLI command takes precedence over config.
    let mode = match args.command {
        Some(Command::Apply) => MigrationMode::Apply,
        Some(Command::Validate) => MigrationMode::Validate,
        Some(Command::GenerateSum) => unreachable!("GenerateSum handled earlier"),
        None => config.migration_mode,
    };

    let slug = args.slug.as_deref();

    if let Some(slug_val) = slug {
        let slug_exists = MIGRATION_DIRS
            .iter()
            .any(|d| d.migration_slug() == slug_val);
        if !slug_exists {
            let known_slugs: Vec<&str> =
                MIGRATION_DIRS.iter().map(|d| d.migration_slug()).collect();
            return Err(format!(
                "Unknown migration slug '{}'. Available slugs are: {}",
                slug_val,
                known_slugs.join(", ")
            )
            .into());
        }
    }

    match mode {
        MigrationMode::Apply => {
            tracing::info!("Initializing migrations table...");
            runner.initialize_migrations_table().await?;

            tracing::info!("Applying migrations...");
            runner.apply_all_migrations(slug).await?;

            tracing::info!("Migrations applied successfully!");
        }
        MigrationMode::Validate => {
            tracing::info!("Validating migrations...");
            runner.validate_all_migrations(slug).await?;

            tracing::info!("All migrations are applied!");
        }
    }

    Ok(())
}
