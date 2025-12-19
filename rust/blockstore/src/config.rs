use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum BlockfileProviderConfig {
    #[serde(alias = "arrow")]
    Arrow(Box<super::arrow::config::ArrowBlockfileProviderConfig>),
    #[serde(alias = "memory")]
    Memory,
    #[serde(alias = "dashmap")]
    DashMap,
}

impl Default for BlockfileProviderConfig {
    fn default() -> Self {
        BlockfileProviderConfig::Arrow(Box::default())
    }
}
