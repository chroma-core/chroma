//! Spanner migration library.
//!
//! This library provides functionality for running Spanner schema migrations.

mod bootstrap;
mod config;
mod migrations;
mod runner;

pub use config::MigrationConfig;
pub use config::MigrationMode;
pub use config::RootConfig;
pub use config::TopologySpannerConfig;
pub use migrations::MigrationDir;
pub use migrations::MIGRATION_DIRS;

use std::time::Duration;

use chroma_config::spanner::{SpannerChannelConfig, SpannerConfig, SpannerSessionPoolConfig};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;
use google_cloud_spanner::client::{ChannelConfig, Client, ClientConfig};
use google_cloud_spanner::session::SessionConfig;
use runner::MigrationRunner;
use thiserror::Error;

/// Converts a SpannerSessionPoolConfig to the library's SessionConfig.
fn to_session_config(cfg: &SpannerSessionPoolConfig) -> SessionConfig {
    let mut config = SessionConfig::default();
    config.session_get_timeout = Duration::from_secs(cfg.session_get_timeout_secs);
    config.max_opened = cfg.max_opened;
    config.min_opened = cfg.min_opened;
    config
}

/// Converts a SpannerChannelConfig to the library's ChannelConfig.
fn to_channel_config(cfg: &SpannerChannelConfig) -> ChannelConfig {
    ChannelConfig {
        num_channels: cfg.num_channels,
        connect_timeout: Duration::from_secs(cfg.connect_timeout_secs),
        timeout: Duration::from_secs(cfg.timeout_secs),
    }
}

/// Errors that can occur during migration execution.
#[derive(Error, Debug)]
pub enum RunMigrationsError {
    /// Failed to bootstrap the emulator.
    #[error("Failed to bootstrap emulator: {0}")]
    BootstrapEmulatorError(String),
    /// Failed to create client configuration with authentication.
    #[error("Failed to create client config: {0}")]
    ClientConfigError(String),
    /// Failed to create Spanner client.
    #[error("Failed to create client: {0}")]
    CreateClientError(String),
    /// Failed to create admin client.
    #[error("Failed to create admin client: {0}")]
    CreateAdminClientError(String),
    /// Unknown migration slug provided.
    #[error("Unknown migration slug '{slug}'. Available slugs are: {available}")]
    UnknownSlug {
        /// The slug that was provided.
        slug: String,
        /// The available slugs.
        available: String,
    },
    /// Failed to initialize migrations table.
    #[error("Failed to initialize migrations table: {0}")]
    InitializeMigrationsTableError(String),
    /// Failed to apply migrations.
    #[error("Failed to apply migrations: {0}")]
    ApplyMigrationsError(String),
    /// Failed to validate migrations.
    #[error("Failed to validate migrations: {0}")]
    ValidateMigrationsError(String),
}

/// Validates that the provided slug exists in MIGRATION_DIRS.
fn validate_slug(slug: Option<&str>) -> Result<(), RunMigrationsError> {
    if let Some(slug_val) = slug {
        if !MIGRATION_DIRS
            .iter()
            .any(|d| d.migration_slug() == slug_val)
        {
            let known_slugs: Vec<&str> =
                MIGRATION_DIRS.iter().map(|d| d.migration_slug()).collect();
            return Err(RunMigrationsError::UnknownSlug {
                slug: slug_val.to_string(),
                available: known_slugs.join(", "),
            });
        }
    }
    Ok(())
}

/// Runs migrations against a Spanner database.
///
/// This function connects to the Spanner database specified in the configuration,
/// optionally bootstraps the emulator if using one, and applies or validates migrations
/// based on the specified mode.
///
/// # Arguments
///
/// * `spanner_config` - The Spanner configuration specifying how to connect to the database.
/// * `slug` - An optional filter to only run migrations for a specific migration directory.
/// * `mode` - The migration mode (Apply or Validate).
/// * `topology_name` - An optional topology name for variable substitution in migrations.
///
/// # Errors
///
/// Returns an error if the connection fails, the slug is invalid, or migration operations fail.
pub async fn run_migrations(
    spanner_config: &SpannerConfig,
    slug: Option<&str>,
    mode: MigrationMode,
    topology_name: Option<&str>,
) -> Result<(), RunMigrationsError> {
    validate_slug(slug)?;

    let session_config = to_session_config(spanner_config.session_pool());
    let (database_path, client_config, admin_client_config) = match spanner_config {
        SpannerConfig::Emulator(emulator) => {
            if let Err(e) = bootstrap::bootstrap_emulator(emulator).await {
                return Err(RunMigrationsError::BootstrapEmulatorError(e.to_string()));
            }

            let channel_config = to_channel_config(spanner_config.channel());
            let client_config = ClientConfig {
                environment: Environment::Emulator(emulator.grpc_endpoint()),
                session_config,
                channel_config,
                ..Default::default()
            };
            let admin_client_config = AdminClientConfig {
                environment: Environment::Emulator(emulator.grpc_endpoint()),
            };

            tracing::info!(
                "Connecting to Spanner database {} in emulator",
                emulator.database_path()
            );
            (emulator.database_path(), client_config, admin_client_config)
        }
        SpannerConfig::Gcp(gcp) => {
            let channel_config = to_channel_config(spanner_config.channel());
            let mut client_config = ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| RunMigrationsError::ClientConfigError(e.to_string()))?;
            client_config.session_config = session_config;
            client_config.channel_config = channel_config;
            let admin_client_config = AdminClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| RunMigrationsError::ClientConfigError(e.to_string()))?;

            tracing::info!(
                "Connecting to Spanner database {} in gcp",
                gcp.database_path()
            );

            (gcp.database_path(), client_config, admin_client_config)
        }
    };

    let client = Client::new(&database_path, client_config)
        .await
        .map_err(|e| RunMigrationsError::CreateClientError(e.to_string()))?;
    let admin_client = AdminClient::new(admin_client_config)
        .await
        .map_err(|e| RunMigrationsError::CreateAdminClientError(e.to_string()))?;

    let mut runner = MigrationRunner::new(client, admin_client, database_path);
    if let Some(topology) = topology_name {
        runner = runner.with_topology(topology.to_string());
    }

    match mode {
        MigrationMode::Apply => {
            tracing::info!("Initializing migrations table...");
            runner
                .initialize_migrations_table()
                .await
                .map_err(|e| RunMigrationsError::InitializeMigrationsTableError(e.to_string()))?;

            tracing::info!("Applying migrations...");
            runner
                .apply_all_migrations(slug)
                .await
                .map_err(|e| RunMigrationsError::ApplyMigrationsError(e.to_string()))?;

            tracing::info!("Migrations applied successfully!");
        }
        MigrationMode::Validate => {
            tracing::info!("Validating migrations...");
            runner
                .validate_all_migrations(slug)
                .await
                .map_err(|e| RunMigrationsError::ValidateMigrationsError(e.to_string()))?;

            tracing::info!("All migrations are applied!");
        }
    }

    Ok(())
}
