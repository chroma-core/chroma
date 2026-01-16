use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use setsum::Setsum;

use chroma_storage::{ETag, Storage};

use crate::interfaces::{repl::StorageWrapper, FragmentConsumer};
use crate::{
    CursorStore, CursorStoreOptions, Error, Fragment, FragmentSeqNo, LogPosition, LogReaderOptions,
};

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

    async fn read_raw_bytes(&self, path: &str) -> Result<(Arc<Vec<u8>>, Option<ETag>), Error> {
        let sw = vec![StorageWrapper {
            region: "local".to_string(),
            storage: Storage::clone(&*self.storage),
            prefix: self.prefix.clone(),
            counter: Arc::new(AtomicU64::new(0)),
        }];
        Ok(crate::interfaces::read_raw_bytes(path, &sw)
            .await
            .map_err(Arc::new)?)
    }

    async fn read_parquet(
        &self,
        path: &str,
        _: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        super::read_parquet(&self.storage, &self.prefix, path, None).await
    }

    async fn read_fragment(&self, path: &str, _: LogPosition) -> Result<Option<Fragment>, Error> {
        super::read_fragment(&self.storage, &self.prefix, path, None).await
    }

    async fn cursors(&self, options: CursorStoreOptions) -> CursorStore {
        CursorStore::new(
            options,
            Arc::clone(&self.storage),
            self.prefix.clone(),
            "fragment_puller".to_string(),
        )
    }
}
