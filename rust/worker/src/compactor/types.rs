use chroma_types::{CollectionUuid, DatabaseName, JobId};
use tokio::sync::oneshot;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CompactionJob {
    pub(crate) collection_id: CollectionUuid,
    pub(crate) database_name: DatabaseName,
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

#[derive(Debug)]
pub struct ListDeadJobsMessage {
    pub response_tx: oneshot::Sender<Vec<JobId>>,
}
