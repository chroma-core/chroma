use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) enum EvictionConfig {
    Lru,
}

#[derive(Deserialize)]
pub(crate) struct LogCacheConfig {
    pub capacity: usize,
    pub shard_num: usize,
    pub eviction: EvictionConfig,
}
