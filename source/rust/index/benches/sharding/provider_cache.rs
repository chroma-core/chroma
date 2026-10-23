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
    provider_with_block_target(dir, policy, 3 * 1024 * 1024).await
}

/// Benchmark-only split target; oversized singleton values remain possible.
pub async fn provider_with_block_target(dir: &Path, policy: ProviderCachePolicy, block_target: usize) -> Result<BlockfileProvider> {
    provider_inner(dir,policy,block_target,None).await
}
/// Same provider/cache defaults with opt-in encoded local-read diagnostics.
pub async fn observed_provider(dir:&Path, observation:std::sync::Arc<std::sync::Mutex<chroma_storage::local::LocalReadObservation>>)->Result<BlockfileProvider> {
    provider_inner(dir,ProviderCachePolicy::default(),3*1024*1024,Some(observation)).await
}
async fn provider_inner(dir:&Path,policy:ProviderCachePolicy,block_target:usize,observation:Option<std::sync::Arc<std::sync::Mutex<chroma_storage::local::LocalReadObservation>>>)->Result<BlockfileProvider> {
    ensure!(block_target > 0 && block_target <= 3 * 1024 * 1024, "invalid benchmark block split target");
    policy.validate()?;
    let data = dir.join("data");
    std::fs::create_dir_all(&data)?;
    let local=LocalStorage::new(data.to_str().context("non-UTF8 path")?);
    let local=if let Some(observation)=observation {local.with_read_observation(observation)}else{local};
    Ok(BlockfileProvider::ArrowBlockfileProvider(
        ArrowBlockfileProvider::new(
            Storage::Local(local),
            block_target,
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
    /// Mechanism test only: deliberately colliding keys reproduce shard-local
    /// eviction with global headroom. This does not reconstruct a prior process's
    /// randomized UUID bucket assignments or prove the remote eviction cause.
    #[tokio::test]
    async fn shard_fragmentation_repeats_obtains_despite_global_headroom() {
        use std::hash::{Hash,Hasher};
        #[derive(Clone,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
        struct CollisionKey(u32);
        impl Hash for CollisionKey {fn hash<H:Hasher>(&self,state:&mut H){0u8.hash(state);}}
        #[derive(Clone,serde::Serialize,serde::Deserialize)]
        struct CollisionValue(u32);
        impl Weighted for CollisionValue {fn weight(&self)->usize {2}}
        let mut totals=Vec::new();
        for shards in [64,1] {
            let p=ProviderCachePolicy{shards,..Default::default()};p.validate().unwrap();
            let cache=p.blocks().build_memory_persistent::<CollisionKey,CollisionValue>().await.unwrap();
            let keys:Vec<_>=(0..4).map(CollisionKey).collect();
            for key in &keys {cache.insert(key.clone(),CollisionValue(key.0)).await;}
            let mut misses=0;let mut returned=Vec::new();
            for _ in 0..8 {for key in &keys {
                let value=match cache.obtain(key.clone()).await.unwrap(){Some(v)=>v,None=>{misses+=1;let v=CollisionValue(key.0);cache.insert(key.clone(),v.clone()).await;v}};
                assert_eq!(value.0,key.0);returned.push(value.0);
            }}
            assert_eq!(returned,(0..8).flat_map(|_|0..4).collect::<Vec<_>>());
            if shards==64 {assert!(misses>0,"same-shard demand8 exceeds localcapacity4");}else{assert_eq!(misses,0,"demand8 fits sharedcapacity256");}
            println!("CACHE_FRAGMENTATION capacity256 shards{shards} localcapacity{} demand8 repeated32 misses{misses}",256/shards);totals.push(misses);
        }
        assert!(totals[0]>totals[1]);
    }

}
