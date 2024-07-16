use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub(crate) enum CacheConfig {
    // case-insensitive
    #[serde(alias = "unbounded")]
    Unbounded(UnboundedCacheConfig),
    #[serde(alias = "lru")]
    Lru(LruConfig),
    #[serde(alias = "lfu")]
    Lfu(LfuConfig),
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct UnboundedCacheConfig {}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct LruConfig {
    pub(crate) capacity: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct LfuConfig {
    pub(crate) capacity: usize,
}
