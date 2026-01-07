//! Spanner migration CLI binary.

use chroma_tracing::{init_global_filter_layer, init_otel_layer, init_stdout_layer, init_tracing};
use clap::{Parser, Subcommand};
use spanner_migrations::{run_migrations, MigrationMode, RootConfig, MIGRATION_DIRS};

/// Validates that the provided slug exists in MIGRATION_DIRS.
fn validate_slug(slug: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(slug_val) = slug {
        if !MIGRATION_DIRS
            .iter()
            .any(|d| d.migration_slug() == slug_val)
        {
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
    Ok(())
}

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
        let slug = args.slug.as_deref();

        validate_slug(slug)?;

        println!("Generating migrations.sum files...");
        for dir in MIGRATION_DIRS.iter() {
            if let Some(slug_val) = slug {
                if dir.migration_slug() != slug_val {
                    continue;
                }
            }
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

    // Determine the migration mode: CLI command takes precedence over config.
    let mode = match args.command {
        Some(Command::Apply) => MigrationMode::Apply,
        Some(Command::Validate) => MigrationMode::Validate,
        Some(Command::GenerateSum) => unreachable!("GenerateSum handled earlier"),
        None => config.migration_mode,
    };

    run_migrations(&config.spanner, Some("spanner_sysdb"), mode).await?;
    if let Some(logdb_spanner) = &config.logdb_spanner {
        run_migrations(logdb_spanner, Some("spanner_logdb"), mode).await?;
    }

    Ok(())
}
