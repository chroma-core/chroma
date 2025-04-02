use chroma_types::CollectionUuid;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: CollectionUuid,
    pub(crate) tenant_id: String,
    pub(crate) offset: i64,
    pub(crate) collection_version: i32,
    pub(crate) collection_logical_size_bytes: u64,
    pub(crate) rebuild: bool,
}

#[derive(Clone, Debug)]
pub struct ScheduledCompactMessage {}

#[derive(Clone, Debug)]
pub struct OneOffCompactMessage {
    pub collection_ids: Vec<CollectionUuid>,
}

#[derive(Clone, Debug)]
pub struct RebuildMessage {
    pub collection_ids: Vec<CollectionUuid>,
}
