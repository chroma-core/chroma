use std::time::Duration;

use super::{distributed::DistributedExecutor, local::LocalExecutor, Executor};
use crate::executor::distributed::ClientSelectionConfig;
use async_trait::async_trait;
use backon::ExponentialBuilder;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_memberlist::client_manager::Tier;
use chroma_system::System;
use chroma_types::Collection;
use serde::{Deserialize, Serialize};

// 32 MB
fn default_max_query_service_response_size_bytes() -> usize {
    1024 * 1024 * 32
}

fn default_query_service_port() -> u16 {
    50051
}

/// Configuration for the distributed executor.
/// # Fields
/// - `connections_per_node` - The number of connections to maintain per node
/// - `replication_factor` - The target replication factor for the request
/// - `connect_timeout_ms` - The timeout for connecting to a node
/// - `request_timeout_ms` - The timeout for the request
/// - `assignment` - The assignment policy to use for routing requests
/// - `memberlist_provider` - The memberlist provider to use for getting the list of nodes
/// - `port` - The port the query service listens on. Defaults to 50051.
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct DistributedExecutorConfig {
    pub connections_per_node: usize,
    pub replication_factor: usize,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    #[serde(default = "RetryConfig::default")]
    pub retry: RetryConfig,
    pub assignment: chroma_config::assignment::config::AssignmentPolicyConfig,
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    #[serde(default = "default_max_query_service_response_size_bytes")]
    pub max_query_service_response_size_bytes: usize,
    #[serde(default = "ClientSelectionConfig::default")]
    pub client_selection: ClientSelectionConfig,
    #[serde(default = "default_query_service_port")]
    pub port: u16,
    /// Tier configuration with capacities and routing rules
    #[serde(default)]
    pub tiers: TiersConfig,
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct LocalExecutorConfig {}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum ExecutorConfig {
    #[serde(alias = "distributed")]
    Distributed(DistributedExecutorConfig),
    #[serde(alias = "local")]
    Local(LocalExecutorConfig),
}

#[async_trait]
impl Configurable<(ExecutorConfig, System)> for Executor {
    async fn try_from_config(
        (config, system): &(ExecutorConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            ExecutorConfig::Distributed(distributed_config) => {
                let distributed_executor = DistributedExecutor::try_from_config(
                    &(distributed_config.clone(), system.clone()),
                    registry,
                )
                .await?;
                Ok(Executor::Distributed(distributed_executor))
            }
            // TODO(hammadb): WE cannot use this since we cannot inject the sysdb into the local executor
            // use ::new() instead for now
            ExecutorConfig::Local(local_config) => {
                let local_executor = LocalExecutor::try_from_config(local_config, registry).await?;
                Ok(Executor::Local(local_executor))
            }
        }
    }
}

//////////////////////// Tiers Config ////////////////////////

/// Pattern for matching requests to tiers
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TierPattern {
    /// Match specific tenant ID
    TenantId(String),
    /// Match specific database ID
    DatabaseId(String),
    /// Match specific collection ID
    CollectionId(String),
    /// Match collection size range [min, max) - max is exclusive
    CollectionSize { min: u64, max: u64 },
}

impl TierPattern {
    fn matches(&self, collection: &Collection) -> bool {
        match self {
            TierPattern::TenantId(id) => &collection.tenant == id,
            TierPattern::DatabaseId(id) => collection.database_id.to_string() == *id,
            TierPattern::CollectionId(id) => collection.collection_id.to_string() == *id,
            TierPattern::CollectionSize { min, max } => {
                let size = collection.total_records_post_compaction;
                size >= *min && size < *max
            }
        }
    }
}

/// Configuration for tier-based routing.
/// Each entry specifies a tier's capacity and routing patterns.
/// Tiers are evaluated in order; the first matching tier wins.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct TiersConfig(#[serde(default)] pub Vec<TierEntry>);

/// A single tier entry with its capacity and routing patterns
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TierEntry {
    pub tier: usize,
    pub capacity: usize,
    #[serde(default)]
    pub patterns: Vec<TierPattern>,
}

impl TiersConfig {
    /// Get capacities ordered by tier number for ClientAssigner.
    /// Returns a Vec where index i contains the capacity for tier i.
    pub fn capacities(&self) -> Vec<usize> {
        if self.0.is_empty() {
            return vec![];
        }

        let max_tier = self.0.iter().map(|t| t.tier).max().unwrap_or(0);
        let mut capacities = vec![0; max_tier + 1];
        for entry in &self.0 {
            capacities[entry.tier] = entry.capacity;
        }
        capacities
    }

    /// Resolve the tier for a given collection based on configured patterns.
    /// Returns the first matching tier, or Tier::default() if no patterns match.
    pub fn resolve_tier(&self, collection: &Collection) -> Tier {
        for entry in &self.0 {
            if entry.patterns.iter().all(|p| p.matches(collection)) {
                return Tier::new(entry.tier);
            }
        }
        Tier::default()
    }
}

//////////////////////// Retry Config ////////////////////////
/// Configuration for the retry policy.
/// # Fields
/// - `factor` - The factor to multiply the delay by
/// - `min_delay_ms` - The minimum delay in milliseconds
/// - `max_delay_ms` - The maximum delay in milliseconds
/// - `max_attempts` - The maximum number of attempts
#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct RetryConfig {
    pub factor: f32,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_attempts: usize,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            factor: 2.0,
            min_delay_ms: 100,
            max_delay_ms: 5000,
            max_attempts: 5,
            jitter: true,
        }
    }
}

impl From<&RetryConfig> for ExponentialBuilder {
    fn from(config: &RetryConfig) -> Self {
        let b = ExponentialBuilder::default()
            .with_factor(config.factor)
            .with_min_delay(Duration::from_millis(config.min_delay_ms))
            .with_max_delay(Duration::from_millis(config.max_delay_ms))
            .with_max_times(config.max_attempts);
        if config.jitter {
            b.with_jitter()
        } else {
            b
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::{CollectionUuid, DatabaseUuid};

    fn test_collection(
        tenant: &str,
        database_id: &str,
        collection_id: &str,
        size: u64,
    ) -> Collection {
        Collection {
            tenant: tenant.to_string(),
            database_id: database_id.parse().unwrap_or(DatabaseUuid::new()),
            collection_id: collection_id.parse().unwrap_or(CollectionUuid::new()),
            total_records_post_compaction: size,
            ..Default::default()
        }
    }

    // ==================== TiersConfig::capacities() tests ====================

    #[test]
    fn test_capacities_empty() {
        let config = TiersConfig::default();
        assert_eq!(config.capacities(), Vec::<usize>::new());
    }

    #[test]
    fn test_capacities_single_tier() {
        let config = TiersConfig(vec![TierEntry {
            tier: 0,
            capacity: 4,
            patterns: vec![],
        }]);
        assert_eq!(config.capacities(), vec![4]);
    }

    #[test]
    fn test_capacities_multiple_tiers() {
        let config = TiersConfig(vec![
            TierEntry {
                tier: 0,
                capacity: 4,
                patterns: vec![],
            },
            TierEntry {
                tier: 1,
                capacity: 5,
                patterns: vec![],
            },
            TierEntry {
                tier: 2,
                capacity: 3,
                patterns: vec![],
            },
        ]);
        assert_eq!(config.capacities(), vec![4, 5, 3]);
    }

    #[test]
    fn test_capacities_non_sequential_tiers() {
        // Tier 0 and tier 2, but no tier 1 - should fill with 0
        let config = TiersConfig(vec![
            TierEntry {
                tier: 0,
                capacity: 4,
                patterns: vec![],
            },
            TierEntry {
                tier: 2,
                capacity: 6,
                patterns: vec![],
            },
        ]);
        assert_eq!(config.capacities(), vec![4, 0, 6]);
    }

    // ==================== TiersConfig::resolve_tier() tests ====================

    #[test]
    fn test_resolve_tier_empty_config() {
        let config = TiersConfig::default();
        let collection = test_collection("tenant1", "", "", 100);
        assert_eq!(config.resolve_tier(&collection), Tier::default());
    }

    #[test]
    fn test_resolve_tier_tenant_match() {
        let config = TiersConfig(vec![TierEntry {
            tier: 0,
            capacity: 4,
            patterns: vec![TierPattern::TenantId("premium".to_string())],
        }]);

        let premium = test_collection("premium", "", "", 0);
        let regular = test_collection("regular", "", "", 0);

        assert_eq!(config.resolve_tier(&premium), Tier::new(0));
        assert_eq!(config.resolve_tier(&regular), Tier::default());
    }

    #[test]
    fn test_resolve_tier_collection_size_match() {
        let config = TiersConfig(vec![
            TierEntry {
                tier: 0,
                capacity: 4,
                patterns: vec![TierPattern::CollectionSize { min: 0, max: 1000 }],
            },
            TierEntry {
                tier: 1,
                capacity: 5,
                patterns: vec![TierPattern::CollectionSize {
                    min: 1000,
                    max: 10000,
                }],
            },
        ]);

        let small = test_collection("t", "", "", 500);
        let medium = test_collection("t", "", "", 5000);
        let large = test_collection("t", "", "", 50000);

        assert_eq!(config.resolve_tier(&small), Tier::new(0));
        assert_eq!(config.resolve_tier(&medium), Tier::new(1));
        assert_eq!(config.resolve_tier(&large), Tier::default()); // No match
    }

    #[test]
    fn test_resolve_tier_first_match_wins() {
        let config = TiersConfig(vec![
            TierEntry {
                tier: 0,
                capacity: 4,
                patterns: vec![TierPattern::TenantId("vip".to_string())],
            },
            TierEntry {
                tier: 1,
                capacity: 5,
                patterns: vec![TierPattern::TenantId("vip".to_string())], // Same pattern, different tier
            },
        ]);

        let collection = test_collection("vip", "", "", 0);
        // First match wins - should be tier 0, not tier 1
        assert_eq!(config.resolve_tier(&collection), Tier::new(0));
    }

    #[test]
    fn test_resolve_tier_empty_patterns_catch_all() {
        let config = TiersConfig(vec![
            TierEntry {
                tier: 0,
                capacity: 4,
                patterns: vec![TierPattern::TenantId("premium".to_string())],
            },
            TierEntry {
                tier: 1,
                capacity: 5,
                patterns: vec![], // Empty patterns = catch-all
            },
        ]);

        let premium = test_collection("premium", "", "", 0);
        let anyone = test_collection("anyone", "", "", 0);

        assert_eq!(config.resolve_tier(&premium), Tier::new(0));
        assert_eq!(config.resolve_tier(&anyone), Tier::new(1)); // Caught by empty patterns
    }

    #[test]
    fn test_resolve_tier_multiple_patterns_all_must_match() {
        let config = TiersConfig(vec![TierEntry {
            tier: 0,
            capacity: 4,
            patterns: vec![
                TierPattern::TenantId("premium".to_string()),
                TierPattern::CollectionSize { min: 0, max: 1000 },
            ],
        }]);

        let premium_small = test_collection("premium", "", "", 500);
        let premium_large = test_collection("premium", "", "", 5000);
        let regular_small = test_collection("regular", "", "", 500);

        assert_eq!(config.resolve_tier(&premium_small), Tier::new(0)); // Both match
        assert_eq!(config.resolve_tier(&premium_large), Tier::default()); // Size doesn't match
        assert_eq!(config.resolve_tier(&regular_small), Tier::default()); // Tenant doesn't match
    }

    // ==================== TierPattern::matches() boundary tests ====================

    #[test]
    fn test_collection_size_boundaries() {
        let pattern = TierPattern::CollectionSize { min: 100, max: 200 };

        let below = test_collection("t", "", "", 99);
        let at_min = test_collection("t", "", "", 100);
        let middle = test_collection("t", "", "", 150);
        let at_max_minus_one = test_collection("t", "", "", 199);
        let at_max = test_collection("t", "", "", 200);
        let above = test_collection("t", "", "", 201);

        assert!(!pattern.matches(&below));
        assert!(pattern.matches(&at_min)); // min is inclusive
        assert!(pattern.matches(&middle));
        assert!(pattern.matches(&at_max_minus_one));
        assert!(!pattern.matches(&at_max)); // max is exclusive
        assert!(!pattern.matches(&above));
    }
}
