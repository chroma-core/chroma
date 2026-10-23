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

/// Benchmark-only decoded payload charging. The existing cache remains soft.
#[derive(Clone,Copy,Debug,Deserialize,Serialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct ByteChargePolicy{pub target_bytes:usize,pub quantum_bytes:usize}
impl ByteChargePolicy{
 pub fn validate(&self,policy:ProviderCachePolicy)->Result<usize>{policy.validate()?;ensure!(self.quantum_bytes.is_power_of_two()&&self.quantum_bytes<=1024*1024,"byte cache quantum");ensure!(self.target_bytes>0&&self.target_bytes%self.quantum_bytes==0,"byte cache target/quantum");let units=self.target_bytes/self.quantum_bytes;ensure!(units>=policy.shards&&units%policy.shards==0,"byte cache target/shards");Ok(units)}
 fn charge(&self,bytes:usize)->usize{bytes.div_ceil(self.quantum_bytes).max(1)}
}
static NEXT_BYTE_CACHE_INSTANCE:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);
#[derive(Debug,Default)]pub struct ByteChargeObservation{inserts:std::sync::atomic::AtomicU64,insert_payload:std::sync::atomic::AtomicU64,insert_units:std::sync::atomic::AtomicU64,requests:std::sync::atomic::AtomicU64,hits:std::sync::atomic::AtomicU64,peak_block:std::sync::atomic::AtomicU64,weight_calls:std::sync::atomic::AtomicU64,applied_units:std::sync::atomic::AtomicU64,constructed_units:std::sync::atomic::AtomicU64,constructed_shards:std::sync::atomic::AtomicU64,instance:std::sync::atomic::AtomicU64,clears:std::sync::atomic::AtomicU64,removes:std::sync::atomic::AtomicU64,live_generations:std::sync::atomic::AtomicU64,peak_generations:std::sync::atomic::AtomicU64,replacements:std::sync::atomic::AtomicU64}
impl ByteChargeObservation{pub fn report(&self,policy:ByteChargePolicy,shards:usize)->serde_json::Value{use std::sync::atomic::Ordering::Relaxed;serde_json::json!({"policy":"benchmark-decoded-block-byte-charge-v1","target_bytes":policy.target_bytes,"quantum_bytes":policy.quantum_bytes,"capacity_units":policy.target_bytes/policy.quantum_bytes,"shards":shards,"per_shard_target_charge_bytes":policy.target_bytes/shards,"cache_instance_id":self.instance.load(Relaxed),"constructed_capacity_units":self.constructed_units.load(Relaxed),"constructed_shards":self.constructed_shards.load(Relaxed),"live_cache_generations":self.live_generations.load(Relaxed),"peak_live_cache_generations":self.peak_generations.load(Relaxed),"cache_generation_replacements":self.replacements.load(Relaxed),"cache_clear_calls":self.clears.load(Relaxed),"cache_remove_calls":self.removes.load(Relaxed),"weighter_applications":self.weight_calls.load(Relaxed),"applied_charge_units":self.applied_units.load(Relaxed),"insertion_calls":self.inserts.load(Relaxed),"cumulative_inserted_decoded_payload_bytes":self.insert_payload.load(Relaxed),"cumulative_inserted_charge_units":self.insert_units.load(Relaxed),"peak_single_inserted_decoded_payload_bytes":self.peak_block.load(Relaxed),"cache_requests":self.requests.load(Relaxed),"cache_hits":self.hits.load(Relaxed),"resident_decoded_payload_bytes":serde_json::Value::Null,"resident_charge_bytes":serde_json::Value::Null,"scope":"Same Foyer memory cache and shard policy with ceil(decoded Block::get_size / quantum) charge, minimum one unit. Target is soft; oversized entries/shard imbalance can exceed it. Insert totals are cumulative traffic, not occupancy. Exact resident payload/charge is unavailable through the existing cache interface; no UUID mirror is retained. Block-cache clear atomically replaces the same-config memory cache to avoid stale usage in the locked Foyer clear implementation. No pointer lock spans awaited cache operations. One replacement builds at a time; retained generations are at most one current plus one constructing/retiring clear plus one per overlapping operation snapshot (no retained generation list). This is a concurrency-dependent bound, not an intrinsic one-cache byte ceiling. Returned value clones remain valid. The separate root cache keeps legacy weighting/clear behavior; provider.clear is not a full root-cache capacity repair. Resident charge across generations depends on concurrent operations; this is not a hard RSS cap. Reader clones/pins, provider root indexes, encoded/storage buffers and allocator are separate."})}}
#[derive(Clone,Debug)]struct ChargedBlock<V>{block:V,units:usize,observation:std::sync::Arc<ByteChargeObservation>}
impl<V> chroma_cache::Weighted for ChargedBlock<V>{fn weight(&self)->usize{self.observation.weight_calls.fetch_add(1,std::sync::atomic::Ordering::Relaxed);self.observation.applied_units.fetch_add(self.units as u64,std::sync::atomic::Ordering::Relaxed);self.units}}
#[derive(Debug)]struct Generation<V:Clone+Send+Sync+std::fmt::Debug+'static>{cache:Box<dyn chroma_cache::Cache<uuid::Uuid,ChargedBlock<V>>>,observation:std::sync::Arc<ByteChargeObservation>}
impl<V:Clone+Send+Sync+std::fmt::Debug+'static> Drop for Generation<V>{fn drop(&mut self){self.observation.live_generations.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);}}
#[derive(Debug)]struct ByteBlockCache<V:Clone+Send+Sync+std::fmt::Debug+'static>{inner:std::sync::RwLock<std::sync::Arc<Generation<V>>>,clear_gate:tokio::sync::Mutex<()>,config:FoyerCacheConfig,policy:ByteChargePolicy,observation:std::sync::Arc<ByteChargeObservation>}
impl<V:Clone+Send+Sync+std::fmt::Debug+'static> ByteBlockCache<V>{
 async fn generation(config:&FoyerCacheConfig,observation:std::sync::Arc<ByteChargeObservation>)->Result<std::sync::Arc<Generation<V>>>{let cache=config.build_memory().await.map_err(|e|anyhow::anyhow!("byte block cache: {e}"))?;let live=observation.live_generations.fetch_add(1,std::sync::atomic::Ordering::Relaxed)+1;observation.peak_generations.fetch_max(live,std::sync::atomic::Ordering::Relaxed);Ok(std::sync::Arc::new(Generation{cache,observation}))}
 async fn new(policy:ProviderCachePolicy,charge:ByteChargePolicy,observation:std::sync::Arc<ByteChargeObservation>)->Result<Self>{let mut config=policy.blocks();config.capacity=charge.validate(policy)?;observation.instance.store(NEXT_BYTE_CACHE_INSTANCE.fetch_add(1,std::sync::atomic::Ordering::Relaxed),std::sync::atomic::Ordering::Relaxed);observation.constructed_units.store(config.capacity as u64,std::sync::atomic::Ordering::Relaxed);observation.constructed_shards.store(config.shards as u64,std::sync::atomic::Ordering::Relaxed);let inner=Self::generation(&config,observation.clone()).await?;Ok(Self{inner:std::sync::RwLock::new(inner),clear_gate:tokio::sync::Mutex::new(()),config,policy:charge,observation})}
 fn snapshot(&self)->std::sync::Arc<Generation<V>>{self.inner.read().unwrap().clone()}
}
#[async_trait::async_trait]
impl<V:Clone+Send+Sync+std::fmt::Debug+chroma_cache::Weighted+chroma_cache::foyer::Code+'static> chroma_cache::Cache<uuid::Uuid,V> for ByteBlockCache<V>{
 async fn insert(&self,key:uuid::Uuid,value:V){use std::sync::atomic::Ordering::Relaxed;let bytes=chroma_cache::foyer::Code::estimated_size(&value);let units=self.policy.charge(bytes);self.observation.inserts.fetch_add(1,Relaxed);self.observation.insert_payload.fetch_add(bytes as u64,Relaxed);self.observation.insert_units.fetch_add(units as u64,Relaxed);self.observation.peak_block.fetch_max(bytes as u64,Relaxed);self.snapshot().cache.insert(key,ChargedBlock{block:value,units,observation:self.observation.clone()}).await;}
 async fn get(&self,key:&uuid::Uuid)->std::result::Result<Option<V>,chroma_cache::CacheError>{use std::sync::atomic::Ordering::Relaxed;self.observation.requests.fetch_add(1,Relaxed);let result=self.snapshot().cache.get(key).await?;if result.is_some(){self.observation.hits.fetch_add(1,Relaxed);}Ok(result.map(|v|v.block))}
 async fn obtain(&self,key:uuid::Uuid)->std::result::Result<Option<V>,chroma_cache::CacheError>{use std::sync::atomic::Ordering::Relaxed;self.observation.requests.fetch_add(1,Relaxed);let result=self.snapshot().cache.obtain(key).await?;if result.is_some(){self.observation.hits.fetch_add(1,Relaxed);}Ok(result.map(|v|v.block))}
 async fn remove(&self,key:&uuid::Uuid){self.observation.removes.fetch_add(1,std::sync::atomic::Ordering::Relaxed);self.snapshot().cache.remove(key).await;}
 // The pointer swap is clear's linearization point. Calls that already own
 // a generation may finish there; they never insert into the new generation.
 // The short pointer lock is released before every awaited cache operation.
 async fn clear(&self)->std::result::Result<(),chroma_cache::CacheError>{let _serial_clear=self.clear_gate.lock().await;let next=Self::generation(&self.config,self.observation.clone()).await.map_err(|e|chroma_cache::CacheError::InvalidCacheConfig(e.to_string()))?;let old={let mut current=self.inner.write().unwrap();std::mem::replace(&mut *current,next)};self.observation.clears.fetch_add(1,std::sync::atomic::Ordering::Relaxed);self.observation.replacements.fetch_add(1,std::sync::atomic::Ordering::Relaxed);drop(old);Ok(())}
}
// Only the memory backend is constructed; the adapter does not add a disk tier.
#[async_trait::async_trait]impl<V:Clone+Send+Sync+std::fmt::Debug+chroma_cache::Weighted+chroma_cache::foyer::Code+'static> chroma_cache::PersistentCache<uuid::Uuid,V> for ByteBlockCache<V>{}
pub async fn byte_charged_provider(dir:&Path,policy:ProviderCachePolicy,charge:ByteChargePolicy)->Result<(BlockfileProvider,std::sync::Arc<ByteChargeObservation>)>{charge.validate(policy)?;let observation=std::sync::Arc::new(ByteChargeObservation::default());let cache=ByteBlockCache::new(policy,charge,observation.clone()).await?;let data=dir.join("data");std::fs::create_dir_all(&data)?;let local=LocalStorage::new(data.to_str().context("non-UTF8 path")?);let roots=policy.roots().build_memory_persistent().await.map_err(|e|anyhow::anyhow!("root cache: {e}"))?;Ok((BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(local),3*1024*1024,Box::new(cache),roots,16)),observation))}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_cache::{Cache, Weighted};
    #[derive(Clone,Debug,serde::Serialize,serde::Deserialize)]struct Payload(Vec<u8>);
    impl chroma_cache::Weighted for Payload{fn weight(&self)->usize{self.0.len().div_ceil(1024*1024).max(1)}}
    fn payload_block(bytes:usize,fill:u8)->(uuid::Uuid,Payload){(uuid::Uuid::new_v4(),Payload(vec![fill;bytes]))}
    #[tokio::test]async fn provider_byte_charge_small_large_eviction_and_immutable_tape(){
        use chroma_cache::{Cache,Weighted};let policy=ProviderCachePolicy{block_capacity_units:8,root_capacity_entries:16,shards:1,clear_after_operation:false};let charge=ByteChargePolicy{target_bytes:32768,quantum_bytes:4096};assert_eq!(charge.validate(policy).unwrap(),8);let small=payload_block(1024,1);let large=payload_block(2*1024*1024,2);assert_eq!(small.1.weight(),1);assert_eq!(large.1.weight(),2);assert_eq!(charge.charge(chroma_cache::foyer::Code::estimated_size(&small.1)),1);assert!(charge.charge(chroma_cache::foyer::Code::estimated_size(&large.1))>=512);
        assert!(ByteChargePolicy{target_bytes:32767,..charge}.validate(policy).is_err());assert!(ByteChargePolicy{quantum_bytes:0,..charge}.validate(policy).is_err());assert!(ByteChargePolicy{target_bytes:4096,..charge}.validate(ProviderCachePolicy{shards:2,..policy}).is_err());assert_eq!(charge.validate(ProviderCachePolicy{shards:2,..policy}).unwrap()/2,4);let observed=std::sync::Arc::new(ByteChargeObservation::default());let cache=ByteBlockCache::new(policy,charge,observed.clone()).await.unwrap();cache.insert(large.0,large.1.clone()).await;let oversized=cache.get(&large.0).await.unwrap();assert!(oversized.is_some(),"existing Foyer soft target may retain a single oversized value");cache.clear().await.unwrap();let blocks:Vec<_>=(0..8u8).map(|i|payload_block(12288,i)).collect();for block in &blocks{cache.insert(block.0,block.1.clone()).await;}let mut misses=0;let mut hits=0;for block in &blocks{if let Some(value)=cache.get(&block.0).await.unwrap(){hits+=1;assert_eq!(value.0,block.1.0);}else{misses+=1;}}assert!(misses>0&&hits>0,"bounded cache must evict part of this fixed tape");let pinned=cache.get(&blocks[7].0).await.unwrap();cache.clear().await.unwrap();assert!(cache.get(&blocks[7].0).await.unwrap().is_none());if let Some(pinned)=pinned{assert_eq!(pinned.0,blocks[7].1.0,"clearing cache cannot mutate an old clone");}println!("BYTE_CACHE_FIXED {}",serde_json::json!({"hits":hits,"misses":misses,"report":observed.report(charge,1)}));
        let dir=tempfile::tempdir().unwrap();let byte_policy=ByteChargePolicy{target_bytes:262144,quantum_bytes:4096};let(provider,_writer_obs)=byte_charged_provider(dir.path(),policy,byte_policy).await.unwrap();let direct=ByteBlockCache::new(policy,byte_policy,std::sync::Arc::new(ByteChargeObservation::default())).await.unwrap();let direct_rows:Vec<_>=(0..12).map(|i|payload_block(8440,i)).collect();for(id,value)in &direct_rows{direct.insert(*id,value.clone()).await;}let mut retained=0;for(id,value)in &direct_rows{if let Some(got)=direct.get(id).await.unwrap(){assert_eq!(got.0,value.0);retained+=1;}}println!("BYTE_DIRECT_FIT retained={retained}");assert_eq!(retained,12,"36charge units fit64unit single-shard cache");for _cycle in 0..3{direct.clear().await.unwrap();for(id,value)in &direct_rows{direct.insert(*id,value.clone()).await;}let mut n=0;for(id,_)in &direct_rows{n+=usize::from(direct.get(id).await.unwrap().is_some());}assert_eq!(n,12,"fresh generation restores complete charge capacity");}let retired=direct.snapshot();let old_clone=direct.get(&direct_rows[0].0).await.unwrap().unwrap();direct.clear().await.unwrap();assert_eq!(direct.observation.live_generations.load(std::sync::atomic::Ordering::Relaxed),2);let late=payload_block(8440,99);retired.cache.insert(late.0,ChargedBlock{block:late.1.clone(),units:3,observation:direct.observation.clone()}).await;assert!(direct.get(&late.0).await.unwrap().is_none(),"write completing in retired generation cannot contaminate current cache");assert!(retired.cache.get(&late.0).await.unwrap().is_some());assert_eq!(old_clone.0,direct_rows[0].1.0);drop(retired);assert_eq!(direct.observation.live_generations.load(std::sync::atomic::Ordering::Relaxed),1);println!("BYTE_CLEAR_GENERATIONS {}",direct.observation.report(byte_policy,1));let mut legacy_config=policy.blocks();legacy_config.capacity=64;let legacy_inner=legacy_config.build_memory::<uuid::Uuid,ChargedBlock<Payload>>().await.unwrap();for(id,value)in &direct_rows{legacy_inner.insert(*id,ChargedBlock{block:value.clone(),units:3,observation:observed.clone()}).await;}legacy_inner.clear().await.unwrap();for(id,value)in &direct_rows{legacy_inner.insert(*id,ChargedBlock{block:value.clone(),units:3,observation:observed.clone()}).await;}let mut after_legacy_clear=0;for(id,_)in &direct_rows{after_legacy_clear+=usize::from(legacy_inner.get(id).await.unwrap().is_some());}assert!(after_legacy_clear<12,"locked Foyer clear preserves stale charge; this is the adverse dependency witness");println!("FOYER_CLEAR_ADVERSE retained_after_refill={after_legacy_clear} expected_capacity_rows=12");let mut roots=Vec::new();let mut payloads=Vec::new();for id in 0..12u32{let writer=provider.write::<u32,Vec<f32>>(chroma_blockstore::BlockfileWriterOptions::new(String::new()).ordered_mutations().max_block_size_bytes(16*1024)).await.unwrap();let mut value=vec![id as f32;2048];value[0]=-0.;writer.set("vector",id,value.clone()).await.unwrap();let flush=writer.commit::<u32,Vec<f32>>().await.unwrap();roots.push(flush.id());flush.flush::<u32,Vec<f32>>().await.unwrap();payloads.push(value);}let mut evidence=Vec::new();for touches in [1,2]{let legacy=provider_with_block_target(dir.path(),policy,3*1024*1024).await.unwrap();let(byte_provider,obs)=byte_charged_provider(dir.path(),policy,byte_policy).await.unwrap();for(name,p)in [("legacy-minimum-MiB-units",&legacy),("byte-charge",&byte_provider)]{let mut decodes=[0u64;2];let mut physical_keys=Vec::new();for pass in 0..2{for(id,root)in roots.iter().enumerate(){for _repeat in 0..touches{let reader=p.read::<u32,&[f32]>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(*root,String::new())).await.unwrap();reader.start_loaded_block_observation();let value=reader.get("vector",id as u32).await.unwrap().unwrap();assert_eq!(value.iter().map(|x|x.to_bits()).collect::<Vec<_>>(),payloads[id].iter().map(|x|x.to_bits()).collect::<Vec<_>>());let observation=serde_json::to_value(reader.loaded_block_observation().unwrap()).unwrap();assert_eq!(observation["access_overflow"],false);physical_keys.push(serde_json::json!({"pass":pass,"repeat":_repeat,"root":root,"logical_key":id,"physical_block_ids":observation["block_access"].as_object().unwrap().keys().collect::<Vec<_>>()}));for block in observation["block_access"].as_object().unwrap().values(){decodes[pass]+=block["decode_attempts"].as_u64().unwrap();}reader.clear_loaded_blocks();drop(reader);}}}println!("BYTE_TAPE_DIAGNOSTIC {}",serde_json::json!({"name":name,"touches_per_root":touches,"physical_keys":physical_keys,"decodes":decodes,"report":obs.report(byte_policy,1)}));assert_eq!(decodes[0],12);if name=="byte-charge"{assert_eq!(decodes[1],0,"fresh64unit cache retains36unit request set");}else{assert!(decodes[1]>0);}evidence.push(serde_json::json!({"policy":name,"touches_per_root":touches,"decode_attempts_by_pass":decodes}));}}
        let old=roots[0];let writer=provider.write::<u32,Vec<f32>>(chroma_blockstore::BlockfileWriterOptions::new(String::new()).ordered_mutations().fork(old).max_block_size_bytes(16*1024)).await.unwrap();let second=vec![4.;2048];writer.set("vector",0,second.clone()).await.unwrap();let flush=writer.commit::<u32,Vec<f32>>().await.unwrap();let new=flush.id();flush.flush::<u32,Vec<f32>>().await.unwrap();provider.clear().await.unwrap();for root in [old,new,old,new]{let reader=provider.read::<u32,&[f32]>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(root,String::new())).await.unwrap();let value=reader.get("vector",0).await.unwrap().unwrap();let expected=if root==old{&payloads[0]}else{&second};assert_eq!(value.iter().map(|x|x.to_bits()).collect::<Vec<_>>(),expected.iter().map(|x|x.to_bits()).collect::<Vec<_>>());reader.clear_loaded_blocks();drop(reader);}println!("BYTE_CACHE_IMMUTABLE {}",serde_json::json!({"tape":evidence,"scope":"12 independent immutable16KiB-target roots,8192FPbytes each; one shard isolates charge arithmetic,not64-shard imbalance or speed. Both one-touch and double-touch tapes are retained; every request drops reader pins. No retained-pass gain is required or claimed. The locked Foyer default is LRU; a prior S3FIFO hypothesis is retracted."}));

    }
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
