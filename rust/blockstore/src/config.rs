use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub(crate) enum BlockfileProviderConfig {
    Arrow(super::arrow::config::ArrowBlockfileProviderConfig),
    Memory,
}
