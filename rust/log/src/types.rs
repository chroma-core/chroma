use chroma_types::CollectionUuid;

/// CollectionInfo is a struct that contains information about a collection for the
/// compacting process.
/// Fields:
/// - collection_id: the id of the collection that needs to be compacted
/// - first_log_offset: the offset of the first log entry in the collection that needs to be compacted
/// - first_log_ts: the timestamp of the first log entry in the collection that needs to be compacted
#[derive(Debug)]
pub struct CollectionInfo {
    pub collection_id: CollectionUuid,
    pub first_log_offset: i64,
    pub first_log_ts: i64,
}
