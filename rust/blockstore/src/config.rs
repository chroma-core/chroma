use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub enum BlockfileProviderConfig {
    Arrow(Box<super::arrow::config::ArrowBlockfileProviderConfig>),
    Memory,
}
