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

/// Names of the database and collections that `POST /api/init`
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
    /// Base name for the per-user file-uploads collection. The actual
    /// collection name is `{base}_{user_id}`, making it private to the
    /// authenticated user rather than shared tenant-wide.
    #[serde(default = "FoundationConfig::default_file_uploads_collection")]
    pub file_uploads_collection: String,
    /// Base name for the per-user coding-agent session collection.
    /// Like file uploads, the real name is `{base}_{user_id}`.
    #[serde(default = "FoundationConfig::default_agent_sessions_collection")]
    pub agent_sessions_collection: String,
    /// Source collections (one per ingest source) that `/init` ensures.
    /// These receive the chunk-sibling grouping flag so the attached
    /// function observes the per-job end-of-job marker after all of a
    /// job's chunk records (ADR 0001 §6 in chroma-core/foundation).
    #[serde(default = "FoundationConfig::default_source_collections")]
    pub source_collections: Vec<String>,
    /// Server-registered function attached to each source collection
    /// (its output is the wiki collection). Default mirrors the POC.
    #[serde(default = "FoundationConfig::default_function_name")]
    pub function_name: String,
    /// Modal endpoint the attached function POSTs to. Threaded into the
    /// attach `params` as `endpoint_url`. Required — there is intentionally
    /// no default, so a deploy can't silently fall back to a hardcoded
    /// endpoint; `/init` errors if it is unset (absent in config -> `None`).
    #[serde(default)]
    pub function_endpoint_url: Option<String>,
    /// How many new source-collection records accumulate before the
    /// attached function is invoked. Matches the chroma frontend default.
    #[serde(default = "FoundationConfig::default_min_records_for_invocation")]
    pub min_records_for_invocation: u64,
    /// Modal `/ask` endpoint URL that `POST /api/ask` reverse-proxies to.
    /// Required at call time (no silent default): the `/api/ask` handler
    /// errors if it is unset. Override via
    /// `CHROMA_FOUNDATION__ASK_ENDPOINT_URL`.
    #[serde(default)]
    pub ask_endpoint_url: Option<String>,
    /// Request timeout applied to the outbound `/ask` HTTP client. Set
    /// high enough to tolerate Modal cold starts.
    #[serde(default = "FoundationConfig::default_ask_timeout_secs")]
    pub ask_timeout_secs: u64,
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
    fn default_file_uploads_collection() -> String {
        "file_uploads".to_string()
    }
    fn default_agent_sessions_collection() -> String {
        "agent_sessions".to_string()
    }
    fn default_source_collections() -> Vec<String> {
        vec!["slack".to_string(), "notion".to_string()]
    }
    fn default_function_name() -> String {
        "http_generate".to_string()
    }
    fn default_min_records_for_invocation() -> u64 {
        100
    }
    fn default_ask_timeout_secs() -> u64 {
        120
    }
}

impl Default for FoundationConfig {
    fn default() -> Self {
        Self {
            database_name: Self::default_database_name(),
            wiki_collection: Self::default_wiki_collection(),
            wiki_revisions_collection: Self::default_wiki_revisions_collection(),
            file_uploads_collection: Self::default_file_uploads_collection(),
            agent_sessions_collection: Self::default_agent_sessions_collection(),
            source_collections: Self::default_source_collections(),
            function_name: Self::default_function_name(),
            function_endpoint_url: None,
            min_records_for_invocation: Self::default_min_records_for_invocation(),
            ask_endpoint_url: None,
            ask_timeout_secs: Self::default_ask_timeout_secs(),
        }
    }
}

impl FoundationApiConfig {
    pub fn load_from_path(path: &str) -> Self {
        load_yaml_with_env(path)
    }
}
