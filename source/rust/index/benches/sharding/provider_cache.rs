//! Optional plain-provider cache targets, not process-memory limits.
use anyhow::{ensure, Context, Result};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::FoyerCacheConfig;
use chroma_storage::{local::LocalStorage, Storage};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProviderCachePolicy {
    pub block_capacity_units: usize,
    pub root_capacity_entries: usize,
    pub shards: usize,
    pub clear_after_operation: bool,
}
impl Default for ProviderCachePolicy {
    fn default() -> Self {
        Self {
            block_capacity_units: 256,
            root_capacity_entries: 4096,
            shards: 64,
            clear_after_operation: false,
        }
    }
}
impl ProviderCachePolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.shards.is_power_of_two() && self.shards <= 64,
            "cache shards must be a power of two within 1 through 64"
        );
        ensure!(
            self.block_capacity_units >= self.shards && self.root_capacity_entries >= self.shards,
            "cache capacities must cover all shards"
        );
        ensure!(
            self.block_capacity_units % self.shards == 0
                && self.root_capacity_entries % self.shards == 0,
            "cache capacities must divide exactly across shards"
        );
        Ok(())
    }
    pub fn blocks(&self) -> FoyerCacheConfig {
        FoyerCacheConfig {
            name: "shard_blocks".into(),
            capacity: self.block_capacity_units,
            shards: self.shards,
            ..Default::default()
        }
    }
    pub fn roots(&self) -> FoyerCacheConfig {
        FoyerCacheConfig {
            name: "shard_roots".into(),
            capacity: self.root_capacity_entries,
            shards: self.shards,
            ..Default::default()
        }
    }
}
pub async fn provider(dir: &Path, policy: ProviderCachePolicy) -> Result<BlockfileProvider> {
    policy.validate()?;
    let data = dir.join("data");
    std::fs::create_dir_all(&data)?;
    Ok(BlockfileProvider::ArrowBlockfileProvider(
        ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(data.to_str().context("non-UTF8 path")?)),
            3 * 1024 * 1024,
            policy
                .blocks()
                .build_memory_persistent()
                .await
                .map_err(|e| anyhow::anyhow!("block cache: {e}"))?,
            policy
                .roots()
                .build_memory_persistent()
                .await
                .map_err(|e| anyhow::anyhow!("root cache: {e}"))?,
            16,
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_cache::{Cache, Weighted};
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Value(usize);
    impl Weighted for Value {
        fn weight(&self) -> usize {
            self.0
        }
    }
    #[tokio::test]
    async fn oversized_entry_is_retained_and_clear_does_not_revoke_clones() {
        let p = ProviderCachePolicy {
            block_capacity_units: 1,
            root_capacity_entries: 1,
            shards: 1,
            clear_after_operation: false,
        };
        let cache = p
            .blocks()
            .build_memory_persistent::<u32, Value>()
            .await
            .unwrap();
        cache.insert(1, Value(3)).await;
        let pinned = cache.get(&1).await.unwrap().unwrap();
        assert_eq!(pinned.weight(), 3); // Existing Foyer admits above the soft target.
        cache.clear().await.unwrap();
        assert!(cache.get(&1).await.unwrap().is_none());
        assert_eq!(pinned.weight(), 3); // Existing readers/clones remain independent.
        cache.insert(2, Value(1)).await;
        cache.insert(3, Value(1)).await;
        assert!(cache.get(&2).await.unwrap().is_none());
        assert!(cache.get(&3).await.unwrap().is_some());
    }
    #[test]
    fn policy_schema_and_shard_targets_are_exact() {
        assert_eq!(ProviderCachePolicy::default().blocks().capacity, 256);
        assert_eq!(ProviderCachePolicy::default().roots().capacity, 4096);
        for text in [
            r#"{"block_capacity_units":true,"root_capacity_entries":1,"shards":1,"clear_after_operation":false}"#,
            r#"{"block_capacity_units":1,"root_capacity_entries":1,"shards":1,"clear_after_operation":false,"unknown":1}"#,
        ] {
            assert!(serde_json::from_str::<ProviderCachePolicy>(text).is_err());
        }
        for p in [
            ProviderCachePolicy {
                shards: 0,
                ..Default::default()
            },
            ProviderCachePolicy {
                shards: 3,
                ..Default::default()
            },
            ProviderCachePolicy {
                block_capacity_units: 1,
                ..Default::default()
            },
            ProviderCachePolicy {
                block_capacity_units: 65,
                ..Default::default()
            },
        ] {
            assert!(p.validate().is_err())
        }
    }
}
