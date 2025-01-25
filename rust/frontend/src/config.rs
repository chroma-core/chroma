use chroma_sysdb::SysDbConfig;
use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct FrontEndConfig {
    pub(super) sysdb: SysDbConfig,
}

const DEFAULT_CONFIG_PATH: &str = "./frontend_config.yaml";

impl FrontEndConfig {
    pub(super) fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub(super) fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(
            Env::prefixed("CHROMA_").map(|k| k.as_str().replace("__", ".").into()),
        );
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use chroma_sysdb::SysDbConfig::Grpc;
    use crate::config::FrontEndConfig;

    #[test]
    fn test_load_config() {
        let config = FrontEndConfig::load();
        let Grpc(sysdb_config) = config.sysdb;
        assert_eq!(sysdb_config.host, "sysdb.chroma");
        assert_eq!(sysdb_config.port, 50051);
        assert_eq!(sysdb_config.connect_timeout_ms, 60000);
        assert_eq!(sysdb_config.request_timeout_ms, 60000);
    }
}
