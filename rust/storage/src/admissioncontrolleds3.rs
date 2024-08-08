use crate::{
    config::{CountBasedPolicyConfig, StorageAdmissionConfig},
    s3::{S3GetError, S3PutError, S3Storage},
};

use super::{GetError, Storage};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use futures::{future::Shared, FutureExt, StreamExt, TryFutureExt};
use parking_lot::Mutex;
use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{Instrument, Span};

// Wrapper over s3 storage that provides proxy features such as
// request coalescing, rate limiting, etc.
#[derive(Clone)]
pub struct AdmissionControlledS3Storage {
    storage: S3Storage,
    outstanding_requests: Arc<
        Mutex<
            HashMap<
                String,
                Shared<
                    Pin<
                        Box<
                            dyn Future<Output = Result<Vec<u8>, AdmissionControlledS3StorageError>>
                                + Send
                                + 'static,
                        >,
                    >,
                >,
            >,
        >,
    >,
    rate_limiter: Arc<RateLimitPolicy>,
}

#[derive(Error, Debug, Clone)]
pub enum AdmissionControlledS3StorageError {
    #[error("Error performing a get call from storage {0}")]
    S3GetError(#[from] S3GetError),
}

impl ChromaError for AdmissionControlledS3StorageError {
    fn code(&self) -> ErrorCodes {
        match self {
            AdmissionControlledS3StorageError::S3GetError(e) => e.code(),
        }
    }
}

impl AdmissionControlledS3Storage {
    pub fn new_with_default_policy(storage: S3Storage) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(15))),
        }
    }

    pub fn new(storage: S3Storage, policy: RateLimitPolicy) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(policy),
        }
    }

    async fn read_from_storage(
        storage: S3Storage,
        key: String,
    ) -> Result<Vec<u8>, AdmissionControlledS3StorageError> {
        let stream = storage
            .get_stream(&key)
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
                                Err(err) => {
                                    tracing::error!("Error reading from storage: {}", err);
                                    match err {
                                        GetError::S3Error(e) => {
                                            return Err(
                                                AdmissionControlledS3StorageError::S3GetError(e),
                                            );
                                        }
                                        GetError::NoSuchKey(e) => {
                                            return Err(
                                                AdmissionControlledS3StorageError::S3GetError(
                                                    S3GetError::NoSuchKey(e),
                                                ),
                                            );
                                        }
                                        GetError::LocalError(_) => unreachable!(),
                                    }
                                }
                            }
                        }
                        tracing::info!("Read {:?} bytes from s3", buf.len());
                        Ok(Some(buf))
                    })
                    .await?;
                match buf {
                    Some(buf) => Ok(buf),
                    None => {
                        // Buffer is empty. Nothing interesting to do.
                        Ok(vec![])
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error reading from storage: {}", e);
                return Err(AdmissionControlledS3StorageError::S3GetError(e));
            }
        }
    }

    async fn enter(&self) -> SemaphorePermit<'_> {
        match &*self.rate_limiter {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                return policy.acquire().await;
            }
        }
    }

    async fn exit(&self, permit: SemaphorePermit<'_>) {
        match &*self.rate_limiter {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                policy.drop(permit).await;
            }
        }
    }

    pub async fn get(&self, key: String) -> Result<Vec<u8>, AdmissionControlledS3StorageError> {
        let permit = self.enter().await;
        let future_to_await;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).map(|fut| fut.clone());
            future_to_await = match maybe_inflight {
                Some(fut) => fut,
                None => {
                    let get_storage_future = AdmissionControlledS3Storage::read_from_storage(
                        self.storage.clone(),
                        key.clone(),
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
        // Release permit.
        self.exit(permit).await;
        res
    }

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), S3PutError> {
        self.storage.put_file(key, path).await
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), S3PutError> {
        self.storage.put_bytes(key, bytes).await
    }
}

// Prefer enum dispatch over dyn since there could
// only be a handful of these policies.
#[derive(Debug)]
enum RateLimitPolicy {
    CountBasedPolicy(CountBasedPolicy),
}

#[derive(Debug)]
struct CountBasedPolicy {
    max_allowed_outstanding: usize,
    remaining_tokens: Semaphore,
}

impl CountBasedPolicy {
    fn new(max_allowed_outstanding: usize) -> Self {
        Self {
            max_allowed_outstanding,
            remaining_tokens: Semaphore::new(max_allowed_outstanding),
        }
    }
    async fn acquire(&self) -> SemaphorePermit<'_> {
        let token_res = self.remaining_tokens.acquire().await;
        match token_res {
            Ok(token) => {
                return token;
            }
            Err(e) => panic!("AcquireToken Failed {}", e),
        }
    }
    async fn drop(&self, permit: SemaphorePermit<'_>) {
        drop(permit);
    }
}

pub async fn from_config(
    config: &StorageAdmissionConfig,
    storage: S3Storage,
) -> Result<AdmissionControlledS3Storage, Box<dyn ChromaError>> {
    match &config {
        StorageAdmissionConfig::CountBasedPolicy(policy) => Ok(AdmissionControlledS3Storage::new(
            storage,
            RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::try_from_config(policy).await?),
        )),
    }
}

#[async_trait]
impl Configurable<CountBasedPolicyConfig> for CountBasedPolicy {
    async fn try_from_config(
        config: &CountBasedPolicyConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Self::new(config.max_concurrent_requests))
    }
}
