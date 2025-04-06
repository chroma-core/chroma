use chroma_types::CollectionUuid;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: CollectionUuid,
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
