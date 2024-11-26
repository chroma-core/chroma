use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

#[derive(Deserialize)]
/// Root config for chroma-load service.  Can be part of a larger config file.
pub struct RootConfig {
    pub load_service: LoadServiceConfig,
}

impl RootConfig {
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    // NOTE:  Copied from ../worker/src/config.rs.
    pub fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        // f = f.join(Serialized::default(
        //     "worker.num_indexing_threads",
        //     num_cpus::get(),
        // ));
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[derive(Deserialize)]
pub struct LoadServiceConfig {
    pub service_name: String,
    pub otel_endpoint: String,
    pub port: u16,
}
