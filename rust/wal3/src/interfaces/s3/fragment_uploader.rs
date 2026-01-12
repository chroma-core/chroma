use std::sync::Arc;

use setsum::Setsum;

use chroma_storage::Storage;
use chroma_types::Cmek;

use crate::interfaces::FragmentUploader;
use crate::{Error, FragmentSeqNo, LogPosition, LogWriterOptions, MarkDirty};

/// Uploads fragments to S3 storage.
pub struct S3FragmentUploader {
    pub(super) options: LogWriterOptions,
    pub(super) storage: Arc<Storage>,
    pub(super) prefix: String,
    pub(super) mark_dirty: Arc<dyn MarkDirty>,
}

impl S3FragmentUploader {
    /// Creates a new S3FragmentUploader.
    pub fn new(
        options: LogWriterOptions,
        storage: Arc<Storage>,
        prefix: String,
        mark_dirty: Arc<dyn MarkDirty>,
    ) -> Self {
        Self {
            options,
            storage,
            prefix,
            mark_dirty,
        }
    }
}

#[async_trait::async_trait]
impl FragmentUploader<(FragmentSeqNo, LogPosition)> for S3FragmentUploader {
    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &(FragmentSeqNo, LogPosition),
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<(String, Setsum, usize), Error> {
        let messages_len = messages.len();
        let fut1 = crate::interfaces::batch_manager::upload_parquet(
            &self.options,
            &self.storage,
            &self.prefix,
            pointer.0.into(),
            Some(pointer.1),
            messages,
            cmek,
            epoch_micros,
        );
        let fut2 = async {
            match self.mark_dirty.mark_dirty(pointer.1, messages_len).await {
                Ok(_) | Err(Error::LogContentionDurable) => Ok(()),
                Err(err) => Err(err),
            }
        };
        let (res1, res2) = futures::future::join(fut1, fut2).await;
        // Prioritize upload error if it exists, as that's the primary operation.
        if let Err(e) = &res1 {
            return Err(e.clone());
        }
        res2?;
        res1
    }
}
