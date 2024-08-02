use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub enum BlockfileProviderConfig {
    Arrow(super::arrow::config::ArrowBlockfileProviderConfig),
    Memory,
}
