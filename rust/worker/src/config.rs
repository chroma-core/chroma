use figment::providers::{Env, Format, Serialized, Yaml};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "chroma_config.yaml";
const ENV_PREFIX: &str = "CHROMA_";

#[derive(Deserialize)]
/// # Description
/// The RootConfig for all chroma services this is a YAML file that
/// is shared between all services, and secondarily, fields can be
/// populated from environment variables. The environment variables
/// are prefixed with CHROMA_ and are uppercase. Values in the envionment
/// variables take precedence over values in the YAML file.
/// By default, it is read from the current working directory,
/// with the filename chroma_config.yaml.
struct RootConfig {
    // The root config object wraps the worker config object so that
    // we can share the same config file between multiple services.
    worker: WorkerConfig,
}

impl RootConfig {
    /// # Description
    /// Load the config from the default location.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The default location is the current working directory, with the filename chroma_config.yaml.
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    pub fn load() -> Self {
        return Self::load_from_path(DEFAULT_CONFIG_PATH);
    }

    /// # Description
    /// Load the config from a specific location.
    /// # Arguments
    /// - path: The path to the config file.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    pub fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "num_indexing_threads" => k.into(),
            k if k == "my_ip" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        f = f.join(Serialized::default(
            "worker.num_indexing_threads",
            num_cpus::get(),
        ));
        let res = f.extract();
        match res {
            Ok(config) => return config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[derive(Deserialize)]
/// # Description
/// The primary config for the worker service.
/// ## Description of parameters
/// - my_ip: The IP address of the worker service. Used for memberlist assignment. Must be provided
/// - num_indexing_threads: The number of indexing threads to use. If not provided, defaults to the number of cores on the machine.
/// # Notes
/// In order to set the enviroment variables, you must prefix them with CHROMA_WORKER__<FIELD_NAME>.
/// For example, to set my_ip, you would set CHROMA_WORKER__MY_IP.
struct WorkerConfig {
    my_ip: String,
    num_indexing_threads: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;

    #[test]
    fn test_config_from_default_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                    num_indexing_threads: 4
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, 4);
            Ok(())
        });
    }

    #[test]
    fn test_config_from_specific_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "random_path.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                    num_indexing_threads: 4
                "#,
            );
            let config = RootConfig::load_from_path("random_path.yaml");
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, 4);
            Ok(())
        });
    }

    #[test]
    #[should_panic]
    fn test_config_missing_required_field() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    num_indexing_threads: 4
                "#,
            );
            let _ = RootConfig::load();
            Ok(())
        });
    }

    #[test]
    fn test_missing_default_field() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, num_cpus::get() as u32);
            Ok(())
        });
    }

    #[test]
    fn test_config_with_env_override() {
        Jail::expect_with(|jail| {
            let _ = jail.set_env("CHROMA_WORKER__MY_IP", "192.0.0.1");
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, num_cpus::get() as u32);
            Ok(())
        });
    }
}
