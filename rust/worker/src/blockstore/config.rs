pub(crate) enum BlockfileProviderConfig {
    Arrow(super::arrow::config::ArrowBlockfileProviderConfig),
    Memory,
}
