use chroma_tracing::{OtelFilter, OtelFilterLevel};
use figment::providers::{Env, Format, Yaml};
use mdac::CircuitBreakerConfig;
use serde::{Deserialize, Serialize};

/// A rule for the scorecard rate limiter. Generic across servers.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ScorecardRule {
    pub patterns: Vec<String>,
    pub score: u32,
}

fn default_otel_service_name() -> String {
    "chromadb".to_string()
}

fn default_otel_filters() -> Vec<OtelFilter> {
    vec![OtelFilter {
        crate_name: "chroma_frontend".to_string(),
        filter_level: OtelFilterLevel::Trace,
    }]
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    pub endpoint: String,
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
    #[serde(default = "default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}

fn default_port() -> u16 {
    8000
}

fn default_listen_address() -> String {
    "0.0.0.0".to_string()
}

fn default_max_payload_size_bytes() -> usize {
    40 * 1024 * 1024 // 40 MB
}

/// Common server scaffolding configuration shared across binaries that embed
/// the frontend-core HTTP server (e.g. chroma-frontend, foundation-api).
///
/// Binaries can either embed this directly via `#[serde(flatten)]` on their
/// own top-level config, or compose only the fields they need.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct BaseServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default = "default_max_payload_size_bytes")]
    pub max_payload_size_bytes: usize,
    #[serde(default = "CircuitBreakerConfig::default")]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub scorecard_enabled: bool,
    #[serde(default)]
    pub scorecard: Vec<ScorecardRule>,
    pub open_telemetry: Option<OpenTelemetryConfig>,
    #[serde(default)]
    pub stdout_tracing: bool,
    #[serde(default)]
    pub cors_allow_origins: Option<Vec<String>>,
}

impl Default for BaseServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            listen_address: default_listen_address(),
            max_payload_size_bytes: default_max_payload_size_bytes(),
            circuit_breaker: CircuitBreakerConfig::default(),
            scorecard_enabled: false,
            scorecard: Vec::new(),
            open_telemetry: None,
            stdout_tracing: false,
            cors_allow_origins: None,
        }
    }
}

/// Load a config of any deserializable shape from a YAML file at `path`,
/// overlaying environment variables prefixed with `CHROMA_` (with `__` mapped
/// to `.` for nesting).
pub fn load_yaml_with_env<T>(path: &str) -> T
where
    T: serde::de::DeserializeOwned,
{
    // SAFETY(rescrv): If we cannot read the config, we panic anyway.
    eprintln!(
        "==========\n{}\n==========\n",
        std::fs::read_to_string(path).unwrap()
    );
    // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
    // Excluding our own environment variables, which are prefixed with CHROMA_.
    let mut f = figment::Figment::from(
        Env::prefixed("CHROMA_").map(|k| k.as_str().replace("__", ".").into()),
    );
    if std::path::Path::new(path).exists() {
        f = figment::Figment::from(Yaml::file(path)).merge(f);
    }
    match f.extract() {
        Ok(config) => config,
        Err(e) => panic!("Error loading config: {}", e),
    }
}
