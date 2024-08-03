use super::{GetError, Storage};
use crate::errors::{ChromaError, ErrorCodes};
use futures::{future::Shared, FutureExt, StreamExt};
use parking_lot::Mutex;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use thiserror::Error;
use tracing::{Instrument, Span};

#[derive(Clone)]
pub(crate) struct NetworkAdmissionControl {
    storage: Storage,
    outstanding_requests: Arc<
        Mutex<
            HashMap<
                String,
                Shared<
                    Pin<
                        Box<
                            dyn Future<Output = Result<(), Box<NetworkAdmissionControlError>>>
                                + Send
                                + 'static,
                        >,
                    >,
                >,
            >,
        >,
    >,
}

#[derive(Error, Debug, Clone)]
pub(crate) enum NetworkAdmissionControlError {
    #[error("Error performing a get call from storage {0}")]
    StorageGetError(#[from] GetError),
    #[error("IO Error")]
    IOError,
    #[error("Error deserializing to block")]
    DeserializationError,
}

impl ChromaError for NetworkAdmissionControlError {
    fn code(&self) -> ErrorCodes {
        match self {
            NetworkAdmissionControlError::StorageGetError(e) => e.code(),
            NetworkAdmissionControlError::IOError => ErrorCodes::Internal,
            NetworkAdmissionControlError::DeserializationError => ErrorCodes::Internal,
        }
    }
}

impl NetworkAdmissionControl {
    pub(crate) fn new(storage: Storage) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn read_from_storage<F, R>(
        storage: Storage,
        key: String,
        f: F,
    ) -> Result<(), Box<NetworkAdmissionControlError>>
    where
        R: Future<Output = Result<(), Box<NetworkAdmissionControlError>>> + Send + 'static,
        F: (FnOnce(Vec<u8>) -> R) + Send + 'static,
    {
        let stream = storage
            .get(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "Storage get"))
            .await;
        match stream {
            Ok(mut bytes) => {
                let read_block_span =
                    tracing::trace_span!(parent: Span::current(), "Read bytes to end");
                let buf = read_block_span
                    .in_scope(|| async {
                        let mut buf: Vec<u8> = Vec::new();
                        while let Some(res) = bytes.next().await {
                            match res {
                                Ok(chunk) => {
                                    buf.extend(chunk);
                                }
                                Err(e) => {
                                    tracing::error!("Error reading from storage: {}", e);
                                    return Err(Box::new(
                                        NetworkAdmissionControlError::StorageGetError(e),
                                    ));
                                }
                            }
                        }
                        Ok(Some(buf))
                    })
                    .await?;
                let buf = match buf {
                    Some(buf) => buf,
                    None => {
                        // Buffer is empty. Nothing interesting to do.
                        return Ok(());
                    }
                };
                tracing::info!("Read {:?} bytes from s3", buf.len());
                return f(buf).await;
            }
            Err(e) => {
                tracing::error!("Error reading from storage: {}", e);
                return Err(Box::new(NetworkAdmissionControlError::StorageGetError(e)));
            }
        }
    }

    pub(crate) async fn get<F, R>(
        &self,
        key: String,
        f: F,
    ) -> Result<(), Box<NetworkAdmissionControlError>>
    where
        R: Future<Output = Result<(), Box<NetworkAdmissionControlError>>> + Send + 'static,
        F: (FnOnce(Vec<u8>) -> R) + Send + 'static,
    {
        let future_to_await;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).map(|fut| fut.clone());
            future_to_await = match maybe_inflight {
                Some(fut) => fut,
                None => {
                    let get_storage_future = NetworkAdmissionControl::read_from_storage(
                        self.storage.clone(),
                        key.clone(),
                        f,
                    )
                    .boxed()
                    .shared();
                    requests.insert(key.clone(), get_storage_future.clone());
                    get_storage_future
                }
            };
        }
        let res = future_to_await.await;
        {
            let mut requests = self.outstanding_requests.lock();
            requests.remove(&key);
        }
        res
    }
}
