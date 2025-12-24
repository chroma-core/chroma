//! Configuration for Spanner migrations.

pub use chroma_config::spanner::SpannerConfig;
use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;
use std::default::Default;
use std::env;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";
// spanner-migration is in the chroma2 namespace on tilt
const DEFAULT_CONFIG_PATH: &str = "./chroma_config2.yaml";

#[derive(Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationMode {
    Apply,
    #[default]
    Validate,
}

/// Migration-specific configuration
#[derive(Deserialize)]
pub struct MigrationConfig {
    pub spanner: SpannerConfig,
    #[serde(default)]
    pub migration_mode: MigrationMode,
    #[serde(default = "MigrationConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "MigrationConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default)]
    pub otel_filters: Vec<OtelFilter>,
}

pub use chroma_tracing::OtelFilter;

impl MigrationConfig {
    fn default_service_name() -> String {
        "rust-sysdb-migration".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }
}

/// Root config wrapper to extract rust-sysdb-migration section
#[derive(Deserialize)]
pub struct RootConfig {
    #[serde(rename = "rust-sysdb-migration")]
    pub rust_sysdb_migration: MigrationConfig,
}

impl RootConfig {
    pub fn load() -> Result<MigrationConfig, Box<dyn std::error::Error>> {
        let path =
            env::var(CONFIG_PATH_ENV_VAR).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        tracing::info!("Loading config from: {}", path);

        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            std::fs::read_to_string(&path)
                .expect("should be able to open and read config to string")
        );

        let f = figment::Figment::from(Yaml::file(&path))
            .merge(Env::prefixed("CHROMA_").map(|k| k.as_str().replace("__", ".").into()));

        let root: RootConfig = f.extract()?;
        Ok(root.rust_sysdb_migration)
    }
}
