//! Configuration for Spanner migrations.

use chroma_config::spanner::SpannerConfig;
use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;
use std::env;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";
// spanner-migration is in the chroma2 namespace on tilt
const DEFAULT_CONFIG_PATH: &str = "./chroma_config2.yaml";

/// Migration-specific configuration
#[derive(Deserialize)]
pub struct MigrationConfig {
    #[serde(default)]
    pub spanner: SpannerConfig,
    #[serde(default)]
    pub _migration_mode: MigrationMode,
}

#[derive(Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum MigrationMode {
    Apply,
    #[default]
    Validate,
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
