// GC will use it to rename a S3 file to a new name.
pub(crate) const RENAMED_FILE_PREFIX: &str = "gc/renamed/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CleanupMode {
    /// Only list files that would be affected without making changes
    #[default]
    DryRun,
    /// Move files to a deletion directory instead of removing them
    Rename,
    /// Permanently delete files
    Delete,
}
