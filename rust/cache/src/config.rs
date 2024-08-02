use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub enum CacheConfig {
    // case-insensitive
    #[serde(alias = "unbounded")]
    Unbounded(UnboundedCacheConfig),
    #[serde(alias = "lru")]
    Lru(LruConfig),
    #[serde(alias = "lfu")]
    Lfu(LfuConfig),
}

#[derive(Deserialize, Debug, Clone)]
pub struct UnboundedCacheConfig {}

#[derive(Deserialize, Debug, Clone)]
pub struct LruConfig {
    pub capacity: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LfuConfig {
    pub capacity: usize,
}
