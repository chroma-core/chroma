use chroma_types::CollectionUuid;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: CollectionUuid,
    pub(crate) tenant_id: String,
    pub(crate) offset: i64,
    pub(crate) collection_version: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct ScheduleMessage {}
