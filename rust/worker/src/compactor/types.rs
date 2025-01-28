use chroma_log::log::CollectionRecord;
use chroma_types::{CollectionUuid, Segment};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: CollectionUuid,
    pub(crate) tenant_id: String,
    pub(crate) offset: i64,
    pub(crate) collection_version: i32,
    pub(crate) segments: Vec<Segment>,
}

#[derive(Clone, Debug)]
pub struct ScheduledCompactionMessage {}

#[derive(Clone, Debug)]
pub struct OneOffCompactionMessage {
    pub collection_ids: Vec<CollectionUuid>,
}

#[derive(Clone, Debug)]
pub(crate) struct CollectionAndSegments {
    pub collection: CollectionRecord,
    pub segments: Vec<Segment>,
}
