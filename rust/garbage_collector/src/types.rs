#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    /// Only list files that would be affected without making changes
    ListOnly,
    /// Move files to a deletion directory instead of removing them
    Rename,
    /// Permanently delete files
    Delete,
}
