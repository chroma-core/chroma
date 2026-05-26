use chroma_sysdb::SysDbConfig;
use frontend_core::config::{load_yaml_with_env, BaseServerConfig};
use serde::{Deserialize, Serialize};

/// Top-level config for the foundation-api HTTP server.
///
/// Embeds `BaseServerConfig` (port, listen address, payload size, circuit
/// breaker, scorecard, OTEL, CORS) flat at the top level so existing
/// `CHROMA_*` env-var bindings work without nesting. Foundation-specific
/// fields will land here as handler tickets bring them in.
#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct FoundationApiConfig {
    #[serde(flatten)]
    pub base: BaseServerConfig,
    #[serde(default)]
    pub sysdb: SysDbConfig,
    #[serde(default)]
    pub foundation: FoundationConfig,
}

/// Names of the database and collections that `POST /api/foundation/init`
/// ensures. Overridable via env vars (e.g. `CHROMA_FOUNDATION__DATABASE_NAME`)
/// so deployments and tests can point at non-default workspaces without a
/// code change.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FoundationConfig {
    #[serde(default = "FoundationConfig::default_database_name")]
    pub database_name: String,
    // TODO(hammadb): collection identities should move onto Chroma collection
    // metadata rather than living as a deployment-side config field.
    #[serde(default = "FoundationConfig::default_wiki_collection")]
    pub wiki_collection: String,
    #[serde(default = "FoundationConfig::default_wiki_revisions_collection")]
    pub wiki_revisions_collection: String,
    /// Source collections (one per ingest source) that `/init` ensures.
    /// These receive the chunk-sibling grouping flag so the attached
    /// function observes the per-job end-of-job marker after all of a
    /// job's chunk records (ADR 0001 §6 in chroma-core/foundation).
    #[serde(default = "FoundationConfig::default_source_collections")]
    pub source_collections: Vec<String>,
}

impl FoundationConfig {
    fn default_database_name() -> String {
        "FOUNDATION".to_string()
    }
    fn default_wiki_collection() -> String {
        "wiki".to_string()
    }
    fn default_wiki_revisions_collection() -> String {
        "wiki_revisions".to_string()
    }
    fn default_source_collections() -> Vec<String> {
        vec!["slack".to_string(), "notion".to_string()]
    }
}

impl Default for FoundationConfig {
    fn default() -> Self {
        Self {
            database_name: Self::default_database_name(),
            wiki_collection: Self::default_wiki_collection(),
            wiki_revisions_collection: Self::default_wiki_revisions_collection(),
            source_collections: Self::default_source_collections(),
        }
    }
}

impl FoundationApiConfig {
    pub fn load_from_path(path: &str) -> Self {
        load_yaml_with_env(path)
    }
}
