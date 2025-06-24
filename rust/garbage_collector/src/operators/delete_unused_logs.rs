use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use futures::future::join_all;
use thiserror::Error;
use wal3::{GarbageCollectionOptions, LogWriter, LogWriterOptions};

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsOperator {
    storage: Arc<Storage>,
}

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsInput {
    collections_to_destroy: Vec<CollectionUuid>,
    collections_to_garbage_collect: Vec<CollectionUuid>,
}

pub type DeleteUnusedLogsOutput = ();

#[derive(Debug, Error)]
pub enum DeleteUnusedLogsError {
    #[error("No log service found")]
    NoLogServiceFound,
}

impl ChromaError for DeleteUnusedLogsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<DeleteUnusedLogsInput, DeleteUnusedLogsOutput> for DeleteUnusedLogsOperator {
    type Error = DeleteUnusedLogsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &DeleteUnusedLogsInput,
    ) -> Result<DeleteUnusedLogsOutput, DeleteUnusedLogsError> {
        if !input.collections_to_garbage_collect.is_empty() {
            let mut log_gc_futures = Vec::with_capacity(input.collections_to_garbage_collect.len());
            for collection_id in &input.collections_to_garbage_collect {
                let storage_clone = self.storage.clone();
                log_gc_futures.push(async move {
                    let writer = match LogWriter::open(
                        LogWriterOptions::default(),
                        storage_clone,
                        &collection_id.storage_prefix_for_log(),
                        "garbage collection service",
                        (),
                    )
                    .await
                    {
                        Ok(log_writer) => log_writer,
                        Err(wal3::Error::UninitializedLog) => return,
                        Err(err) => {
                            tracing::error!("Unable to initialize log writer for collection [{collection_id}]: {err}");
                            return
                        }
                    };
                    if let Err(err) = writer.garbage_collect(&GarbageCollectionOptions::default()).await {
                        tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                    }
                });
            }
            join_all(log_gc_futures).await;
        }
        if !input.collections_to_destroy.is_empty() {
            let mut log_destroy_futures = Vec::with_capacity(input.collections_to_destroy.len());
            for collection_id in &input.collections_to_destroy {
                let storage_clone = self.storage.clone();
                log_destroy_futures.push(async move {
                    if let Err(err) =
                        wal3::destroy(storage_clone, &collection_id.storage_prefix_for_log()).await
                    {
                        tracing::error!(
                            "Unable to destroy log for collection [{collection_id}]: {err:?}"
                        );
                    }
                })
            }
            join_all(log_destroy_futures).await;
        }

        // NOTE: This is a hack to get the dirty log writers for all available log services
        // It tries to open the dirty log manifest sequentially until the manifest is not found
        // It assumes that all dirty log paths looks like `dirty-rust-log-service-<replica_id>`
        let mut dirty_log_writers = Vec::new();
        let mut replica_id = 0;
        loop {
            let log_service_name = format!("rust-log-service-{replica_id}");
            match LogWriter::open(
                LogWriterOptions::default(),
                self.storage.clone(),
                &log_service_name,
                "garbage collection service",
                (),
            )
            .await
            {
                Ok(log_writer) => dirty_log_writers.push(log_writer),
                Err(wal3::Error::UninitializedLog) => break,
                Err(err) => {
                    tracing::error!("Unable to open dirty log [{log_service_name}]: {err}");
                    break;
                }
            };
            replica_id += 1;
        }
        if dirty_log_writers.is_empty() {
            tracing::error!("Unable to find any dirty log manifest. Skipping dirty log GC");
            return Err(DeleteUnusedLogsError::NoLogServiceFound);
        }
        join_all(dirty_log_writers.into_iter().map(|writer| async move {
            if let Err(err) = writer
                .garbage_collect(&GarbageCollectionOptions::default())
                .await
            {
                tracing::error!("Unable to garbage collect dirty log: {err}");
            }
        }))
        .await;
        Ok(())
    }
}
