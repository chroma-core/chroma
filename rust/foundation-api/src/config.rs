use frontend_core::config::{load_yaml_with_env, BaseServerConfig};
use serde::{Deserialize, Serialize};

/// Top-level config for the foundation-api HTTP server.
///
/// Embeds `BaseServerConfig` (port, listen address, payload size, circuit
/// breaker, scorecard, OTEL, CORS) flat at the top level so existing
/// `CHROMA_*` env-var bindings work without nesting. Foundation-specific
/// fields will land here as handler tickets bring them in.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FoundationApiConfig {
    #[serde(flatten)]
    pub base: BaseServerConfig,
}

impl Default for FoundationApiConfig {
    fn default() -> Self {
        Self {
            base: BaseServerConfig::default(),
        }
    }
}

impl FoundationApiConfig {
    pub fn load_from_path(path: &str) -> Self {
        load_yaml_with_env(path)
    }
}
