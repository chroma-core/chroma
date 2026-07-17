/// Bound the byte length of every generated Chroma record key.
pub const KEY_MAX_BYTES: usize = 128;

/// Bound the byte length of every generated Chroma record document.
pub const VALUE_MAX_BYTES: usize = 16 * 1024;

/// Bound a chunkset base so `/metadata` and `/chunks/{index}` suffixes fit.
pub const CHUNKSET_BASE_MAX_BYTES: usize = 115;

/// Width of a base36-encoded UUID trajectory id.
pub(crate) const TID_WIDTH: usize = 25;
/// Width of a base36-encoded trajectory entry index.
pub(crate) const ENTRY_INDEX_WIDTH: usize = 6;
/// Width of a base36-encoded per-entry call index.
pub(crate) const CALL_INDEX_WIDTH: usize = 4;
/// Width of a base36-encoded chunk index within a chunkset.
pub(crate) const CHUNK_INDEX_WIDTH: usize = 5;
/// Width of a base36-encoded SHA-256 item id.
pub(crate) const ITEM_ID_WIDTH: usize = 50;
