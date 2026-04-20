//! Configuration for Spanner migrations.

pub use chroma_config::spanner::SpannerConfig;
use chroma_types::Topology;
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};
use std::default::Default;
use std::env;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";
// spanner-migration is in the chroma2 namespace on tilt
const DEFAULT_CONFIG_PATH: &str = "../worker/chroma_config2.yaml";

#[derive(Copy, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationMode {
    Apply,
    #[default]
    Validate,
}

/// Topology-specific configuration
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TopologySpannerConfig {
    pub sysdb_spanner: SpannerConfig,
    // NOTE(rescrv):  Until we plumb everything we will just say that there are two spanner
    // configs.
    pub logdb_spanner: SpannerConfig,
}

/// Migration-specific configuration
#[derive(Deserialize)]
pub struct MigrationConfig {
    #[serde(default)]
    pub migration_mode: MigrationMode,
    #[serde(default = "MigrationConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "MigrationConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default)]
    pub otel_filters: Vec<OtelFilter>,
    #[serde(default)]
    pub topologies: Vec<Topology<TopologySpannerConfig>>,
}

pub use chroma_tracing::OtelFilter;

impl MigrationConfig {
    fn default_service_name() -> String {
        "rust-sysdb-migration".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn from_service_topologies(
        sysdb_service: CompatSysDbServiceConfig,
        log_service: CompatLogServiceConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let log_topologies = log_service.regions_and_topologies.ok_or(
            "log_service.regions_and_topologies is required when rust-sysdb-migration is absent",
        )?;

        let mut topologies =
            Vec::with_capacity(sysdb_service.regions_and_topologies.topologies.len());
        for sysdb_topology in sysdb_service.regions_and_topologies.topologies {
            let logdb_topology = log_topologies
                .topologies
                .iter()
                .find(|topology| topology.name == sysdb_topology.name)
                .ok_or_else(|| {
                    format!(
                        "Topology '{}' exists in sysdb_service but not log_service",
                        sysdb_topology.name
                    )
                })?;

            if sysdb_topology.regions() != logdb_topology.regions() {
                return Err(format!(
                    "Topology '{}' has different regions in sysdb_service and log_service",
                    sysdb_topology.name
                )
                .into());
            }

            topologies.push(Topology::new(
                sysdb_topology.name.clone(),
                sysdb_topology.regions().to_vec(),
                TopologySpannerConfig {
                    sysdb_spanner: sysdb_topology.config.spanner,
                    logdb_spanner: logdb_topology.config.spanner.clone(),
                },
            ));
        }

        Ok(Self {
            migration_mode: MigrationMode::default(),
            service_name: Self::default_service_name(),
            otel_endpoint: sysdb_service.otel_endpoint,
            otel_filters: sysdb_service.otel_filters,
            topologies,
        })
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
struct IgnoredRegionConfig {}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct CompatTopologySpannerConfig {
    spanner: SpannerConfig,
}

#[derive(Deserialize)]
struct CompatSysDbServiceConfig {
    #[serde(default = "MigrationConfig::default_otel_endpoint")]
    otel_endpoint: String,
    #[serde(default)]
    otel_filters: Vec<OtelFilter>,
    regions_and_topologies: chroma_types::MultiCloudMultiRegionConfiguration<
        IgnoredRegionConfig,
        CompatTopologySpannerConfig,
    >,
}

#[derive(Deserialize)]
struct CompatLogServiceConfig {
    #[serde(default)]
    regions_and_topologies: Option<
        chroma_types::MultiCloudMultiRegionConfiguration<
            IgnoredRegionConfig,
            CompatTopologySpannerConfig,
        >,
    >,
}

/// Root config wrapper that can extract either the dedicated migration section
/// or derive it from the standard sysdb/log service topology config.
#[derive(Deserialize)]
pub struct RootConfig {
    #[serde(rename = "rust-sysdb-migration")]
    #[serde(default)]
    pub rust_sysdb_migration: Option<MigrationConfig>,
    #[serde(default)]
    sysdb_service: Option<CompatSysDbServiceConfig>,
    #[serde(default)]
    log_service: Option<CompatLogServiceConfig>,
}

impl RootConfig {
    fn into_migration_config(self) -> Result<MigrationConfig, Box<dyn std::error::Error>> {
        if let Some(config) = self.rust_sysdb_migration {
            return Ok(config);
        }

        let sysdb_service = self
            .sysdb_service
            .ok_or("Missing rust-sysdb-migration and sysdb_service sections in configuration")?;
        let log_service = self
            .log_service
            .ok_or("Missing rust-sysdb-migration and log_service sections in configuration")?;
        MigrationConfig::from_service_topologies(sysdb_service, log_service)
    }

    pub fn load() -> Result<MigrationConfig, Box<dyn std::error::Error>> {
        let path =
            env::var(CONFIG_PATH_ENV_VAR).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        println!("Loading config from: {}", path);

        let config_content = std::fs::read_to_string(&path)
            .expect("should be able to open and read config to string");

        // Check if topologies section exists in the raw content
        if config_content.contains("topologies:") {
            println!("Found 'topologies:' section in config file");
        } else {
            println!("No 'topologies:' section found in config file");
        }

        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            config_content
        );

        let f = figment::Figment::from(Yaml::file(&path))
            .merge(Env::prefixed("CHROMA_").map(|k| k.as_str().replace("__", ".").into()));

        let root: RootConfig = f.extract()?;
        let config = root.into_migration_config()?;
        println!("Parsed RootConfig");
        println!(
            "Found {} topologies in migration configuration",
            config.topologies.len()
        );
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_config::spanner::{SpannerConfig, SpannerEmulatorConfig};
    use chroma_types::{
        MultiCloudMultiRegionConfiguration, ProviderRegion, RegionName, Topology, TopologyName,
    };

    fn emulator(database: &str) -> SpannerConfig {
        let mut emulator = SpannerEmulatorConfig::default();
        emulator.database = database.to_string();
        SpannerConfig::Emulator(emulator)
    }

    fn region(name: &str) -> ProviderRegion<IgnoredRegionConfig> {
        ProviderRegion::new(
            RegionName::new(name).unwrap(),
            "tilt",
            name,
            IgnoredRegionConfig::default(),
        )
    }

    fn sysdb_service(topology_name: &str, database: &str) -> CompatSysDbServiceConfig {
        CompatSysDbServiceConfig {
            otel_endpoint: "http://otel.example:4317".to_string(),
            otel_filters: vec![],
            regions_and_topologies: MultiCloudMultiRegionConfiguration::new(
                RegionName::new("tilt-config-1").unwrap(),
                vec![region("tilt-config-1"), region("tilt-config-2")],
                vec![Topology::new(
                    TopologyName::new(topology_name).unwrap(),
                    vec![
                        RegionName::new("tilt-config-1").unwrap(),
                        RegionName::new("tilt-config-2").unwrap(),
                    ],
                    CompatTopologySpannerConfig {
                        spanner: emulator(database),
                    },
                )],
            )
            .unwrap(),
        }
    }

    fn log_service(topology_name: &str, database: &str) -> CompatLogServiceConfig {
        CompatLogServiceConfig {
            regions_and_topologies: Some(
                MultiCloudMultiRegionConfiguration::new(
                    RegionName::new("tilt-config-1").unwrap(),
                    vec![region("tilt-config-1"), region("tilt-config-2")],
                    vec![Topology::new(
                        TopologyName::new(topology_name).unwrap(),
                        vec![
                            RegionName::new("tilt-config-1").unwrap(),
                            RegionName::new("tilt-config-2").unwrap(),
                        ],
                        CompatTopologySpannerConfig {
                            spanner: emulator(database),
                        },
                    )],
                )
                .unwrap(),
            ),
        }
    }

    #[test]
    fn derives_migration_config_from_service_topologies() {
        let root = RootConfig {
            rust_sysdb_migration: None,
            sysdb_service: Some(sysdb_service("tilt-spanning", "local-sysdb-database")),
            log_service: Some(log_service("tilt-spanning", "local-logdb-database")),
        };

        let config = root.into_migration_config().unwrap();

        assert_eq!(config.service_name, "rust-sysdb-migration");
        assert_eq!(config.otel_endpoint, "http://otel.example:4317");
        assert_eq!(config.topologies.len(), 1);
        assert_eq!(config.topologies[0].name.to_string(), "tilt-spanning");
        assert_eq!(
            config.topologies[0].config.sysdb_spanner.database_path(),
            emulator("local-sysdb-database").database_path()
        );
        assert_eq!(
            config.topologies[0].config.logdb_spanner.database_path(),
            emulator("local-logdb-database").database_path()
        );
    }

    #[test]
    fn explicit_migration_section_takes_precedence() {
        let root = RootConfig {
            rust_sysdb_migration: Some(MigrationConfig {
                migration_mode: MigrationMode::Apply,
                service_name: "explicit".to_string(),
                otel_endpoint: "http://explicit:4317".to_string(),
                otel_filters: vec![],
                topologies: vec![],
            }),
            sysdb_service: Some(sysdb_service("tilt-spanning", "local-sysdb-database")),
            log_service: Some(log_service("tilt-spanning", "local-logdb-database")),
        };

        let config = root.into_migration_config().unwrap();

        assert_eq!(config.service_name, "explicit");
        assert!(matches!(config.migration_mode, MigrationMode::Apply));
        assert!(config.topologies.is_empty());
    }
}
