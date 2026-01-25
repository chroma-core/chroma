use figment::providers::{Format, Yaml};

#[derive(serde::Deserialize, serde::Serialize)]
pub struct RootConfig {
    pub frontend: chroma_frontend::config::FrontendServerConfig,
    #[serde(default)]
    pub log_service: chroma_log_service::LogServerConfig,
    #[serde(default)]
    pub query_service: worker::config::QueryServiceConfig,
    #[serde(default)]
    pub compaction_service: worker::config::CompactionServiceConfig,
}

fn main() {
    for arg in std::env::args().skip(1) {
        let f = figment::Figment::from(Yaml::file(&arg));
        let rc: RootConfig = match f.extract() {
            Ok(config) => config,
            Err(e) => panic!("cannot load config {arg:?}: {e}"),
        };
        _ = rc;
    }
}
