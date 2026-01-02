use std::sync::Arc;

use setsum::Setsum;

use chroma_storage::{admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage};

use crate::interfaces::FragmentConsumer;
use crate::{fragment_path, Error, Fragment, FragmentSeqNo, LogPosition, LogReaderOptions};

pub struct S3FragmentPuller {
    storage: Arc<Storage>,
    prefix: String,
}

impl S3FragmentPuller {
    pub fn new(_: LogReaderOptions, storage: Arc<Storage>, prefix: String) -> Self {
        Self { storage, prefix }
    }
}

#[async_trait::async_trait]
impl FragmentConsumer for S3FragmentPuller {
    type FragmentPointer = (FragmentSeqNo, LogPosition);

    async fn read_raw_bytes(&self, path: &str, _: LogPosition) -> Result<Arc<Vec<u8>>, Error> {
        let path = fragment_path(&self.prefix, path);
        let parquet = self
            .storage
            .get(&path, GetOptions::new(StorageRequestPriority::P0))
            .await
            .map_err(Arc::new)?;
        Ok(parquet)
    }

    async fn read_parquet(
        &self,
        path: &str,
        _: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64), Error> {
        super::read_parquet(&self.storage, &self.prefix, path, None).await
    }

    async fn read_fragment(&self, path: &str, _: LogPosition) -> Result<Option<Fragment>, Error> {
        super::read_fragment(&self.storage, &self.prefix, path, None).await
    }
}
