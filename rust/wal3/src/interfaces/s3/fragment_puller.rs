use std::sync::Arc;

use setsum::Setsum;

use chroma_storage::{Storage, StorageError};

use crate::interfaces::FragmentConsumer;
use crate::{Error, Fragment, LogPosition, LogReaderOptions};

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
    async fn read_bytes(&self, path: &str) -> Result<Arc<Vec<u8>>, Error> {
        match super::read_bytes(&self.storage, &self.prefix, path).await? {
            Some(bytes) => Ok(bytes),
            None => Err(Arc::new(StorageError::NotFound {
                path: path.into(),
                source: Arc::new(std::io::Error::other("file not found")),
            })
            .into()),
        }
    }

    async fn parse_parquet(
        &self,
        parquet: &[u8],
        _starting_log_position: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        // NOTE(rescrv):  S3FragmentPuller deals with absolutes; we therefore do not pass an
        // offset.
        super::parse_parquet(parquet, None).await
    }

    async fn parse_parquet_fast(
        &self,
        parquet: &[u8],
        _starting_log_position: LogPosition,
    ) -> Result<(Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        // NOTE(rescrv):  S3FragmentPuller deals with absolutes; we therefore do not pass an
        // offset.
        super::parse_parquet_fast(parquet, None).await
    }

    async fn read_fragment(&self, path: &str, _: LogPosition) -> Result<Option<Fragment>, Error> {
        super::read_fragment(&self.storage, &self.prefix, path, None).await
    }
}
