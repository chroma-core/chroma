use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockfileProviderConfig {
    Arrow(Box<super::arrow::config::ArrowBlockfileProviderConfig>),
    Memory,
}

impl Default for BlockfileProviderConfig {
    fn default() -> Self {
        BlockfileProviderConfig::Arrow(Box::default())
    }
}
