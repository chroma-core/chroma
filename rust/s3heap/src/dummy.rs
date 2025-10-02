use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{Error, HeapScheduler, Triggerable};

/// A dummy scheduler implementation for testing purposes.
///
/// This scheduler always reports that items are not done and have no scheduled times.
pub struct DummyScheduler;

#[async_trait::async_trait]
impl HeapScheduler for DummyScheduler {
    async fn are_done(&self, items: &[(Triggerable, Uuid)]) -> Result<Vec<bool>, Error> {
        Ok(vec![false; items.len()])
    }

    async fn next_times_and_nonces(
        &self,
        items: &[Triggerable],
    ) -> Result<Vec<Option<(DateTime<Utc>, Uuid)>>, Error> {
        Ok(vec![None; items.len()])
    }
}
