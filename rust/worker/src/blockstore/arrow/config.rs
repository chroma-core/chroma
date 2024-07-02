use serde::Deserialize;

#[cfg(test)]
// A small block size for testing, so that triggering splits etc is easier
pub(crate) const TEST_MAX_BLOCK_SIZE_BYTES: usize = 16384;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ArrowBlockfileProviderConfig {
    // Note: This provider has two dependent components that
    // are both internal to the arrow blockfile provider.
    // The BlockManager and the SparseIndexManager.
    // We could have a BlockManagerConfig and a SparseIndexManagerConfig
    // but the only configuration that is needed is the max_block_size_bytes
    // so for now we just hoid this configuration in the ArrowBlockfileProviderConfig.
    pub(crate) max_block_size_bytes: usize,
}
