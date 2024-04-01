#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: String,
    pub(crate) tenant_id: String,
    pub(crate) offset: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct ScheduleMessage {}
