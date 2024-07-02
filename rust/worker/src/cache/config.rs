use crate::cache::cache::Cache;
use crate::errors::ChromaError;
use core::hash::Hash;
use serde::Deserialize;

#[derive(Deserialize, Debug)]

pub(crate) enum CacheConfig {
    // case-insensitive
    #[serde(alias = "unbounded")]
    Unbounded(UnboundedCacheConfig),
    #[serde(alias = "lru")]
    Lru(LruConfig),
    #[serde(alias = "lfu")]
    Lfu(LfuConfig),
}

#[derive(Deserialize, Debug)]
pub(crate) struct UnboundedCacheConfig {}

#[derive(Deserialize, Debug)]
pub(crate) struct LruConfig {
    pub(crate) capacity: usize,
}

#[derive(Deserialize, Debug)]
pub(crate) struct LfuConfig {
    pub(crate) capacity: usize,
}
