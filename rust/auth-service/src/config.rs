//! Configuration shared by the server and tenant installer.

use std::path::Path;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;

/// A single tenant's credentials, loaded from a Secret volume.
/// Deliberately does not implement `Debug` to avoid exposing credentials.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub tenant: String,
    pub database: String,
    pub api_keys: Vec<String>,
    pub data_plane_api_key: String,
}

impl Config {
    /// Read and validate configuration without including secrets in errors.
    pub fn load(path: &Path) -> Result<Self> {
        let input = std::fs::read_to_string(path).context("Cannot read auth configuration")?;
        Self::parse(&input)
    }

    /// Parse TOML, rejecting empty credentials and invalid tenant/database names.
    pub fn parse(input: &str) -> Result<Self> {
        // TOML errors can include source lines containing credentials.
        let config: Self = toml::from_str(input)
            .map_err(|_| anyhow::anyhow!("Invalid auth configuration TOML or field types"))?;
        for name in [&config.tenant, &config.database] {
            ensure!(
                name.len() >= 3 && name.chars().all(|c| c.is_ascii_alphanumeric() || "_-".contains(c)),
                "Tenant and database must be at least three characters and contain only letters, digits, underscores, or hyphens"
            );
        }
        let valid_key = |key: &str| !key.is_empty() && key.bytes().all(|b| (33..=126).contains(&b));
        ensure!(
            valid_key(&config.data_plane_api_key),
            "Invalid data-plane credential"
        );
        ensure!(
            !config.api_keys.is_empty() && config.api_keys.iter().all(|k| valid_key(k)),
            "At least one nonempty printable API key is required"
        );
        ensure!(
            !config.api_keys.contains(&config.data_plane_api_key),
            "Data-plane credential must be distinct from tenant API keys"
        );
        Ok(config)
    }
}
