use std::fmt;

pub(crate) const RENAMED_FILE_PREFIX: &str = "gc/renamed/";
pub(crate) const DELETE_LIST_FILE_PREFIX: &str = "gc/delete-list/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    /// Only list files that would be affected without making changes
    ListOnly,
    /// Move files to a deletion directory instead of removing them
    Rename,
    /// Permanently delete files
    Delete,
}
