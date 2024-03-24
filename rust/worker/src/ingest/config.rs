use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct IngestConfig {
    pub(crate) queue_size: usize,
}
