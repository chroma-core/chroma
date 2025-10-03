use uuid::Uuid;

use crate::{Error, HeapScheduler, Schedule, Triggerable};

/// A dummy scheduler implementation for testing purposes.
///
/// This scheduler always reports that items are not done and have no scheduled times.
pub struct DummyScheduler;

#[async_trait::async_trait]
impl HeapScheduler for DummyScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
        Ok(vec![false; items.len()])
    }

    async fn get_schedules(&self, ids: &[Uuid]) -> Result<Vec<Option<Schedule>>, Error> {
        Ok(vec![None; ids.len()])
    }
}
