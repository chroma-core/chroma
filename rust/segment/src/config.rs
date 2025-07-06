use serde::Deserialize;

/// The configuration for the custom resource memberlist provider.
/// # Fields
/// - storage_path: The path to use for temporary storage in the segment manager, if needed.
#[derive(Deserialize)]
pub(crate) struct SegmentManagerConfig {
    #[allow(dead_code)]
    pub(crate) storage_path: String,
}
