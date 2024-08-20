use crate::{
    config::{CountBasedPolicyConfig, RateLimitingConfig, StorageConfig},
    s3::{S3GetError, S3PutError, S3Storage, StorageConfigError},
    stream::ByteStreamItem,
};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use futures::{future::Shared, FutureExt, Stream};
use parking_lot::Mutex;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{Instrument, Span};

/// Wrapper over s3 storage that provides proxy features such as
/// request coalescing, rate limiting, etc.
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
                            dyn Future<
                                    Output = Result<
                                        Arc<Vec<u8>>,
                                        AdmissionControlledS3StorageError,
                                    >,
                                > + Send
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
    #[error("Error performing a get call from s3 storage {0}")]
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

    // TODO: Remove this once the upstream consumers switch to non-streaming APIs.
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> Result<
        Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>,
        AdmissionControlledS3StorageError,
    > {
        match self
            .storage
            .get_stream(key)
            .instrument(tracing::trace_span!(parent: Span::current(), "Storage get"))
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => {
                tracing::error!("Error reading from storage: {}", e);
                return Err(AdmissionControlledS3StorageError::S3GetError(e));
            }
        }
    }

    async fn read_from_storage(
        storage: S3Storage,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        let bytes_res = storage
            .get(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "S3 get"))
            .await;
        match bytes_res {
            Ok(bytes) => {
                return Ok(bytes);
            }
            Err(e) => {
                tracing::error!("Error reading from s3: {}", e);
                return Err(AdmissionControlledS3StorageError::S3GetError(e));
            }
        }
    }

    pub async fn get(
        &self,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3. We rely on synchronization on the cache
        // by the upstream consumer to make sure that this works correctly.
        let future_to_await;
        let is_dupe: bool;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).map(|fut| fut.clone());
            (future_to_await, is_dupe) = match maybe_inflight {
                Some(fut) => (fut, true),
                None => {
                    let get_storage_future = AdmissionControlledS3Storage::read_from_storage(
                        self.storage.clone(),
                        key.clone(),
                    )
                    .boxed()
                    .shared();
                    requests.insert(key.clone(), get_storage_future.clone());
                    (get_storage_future, false)
                }
            };
        }

        // Acquire permit.
        let permit: SemaphorePermit<'_>;
        if is_dupe {
            permit = self.rate_limiter.enter().await;
        }

        let res = future_to_await.await;
        {
            let mut requests = self.outstanding_requests.lock();
            requests.remove(&key);
        }

        res
        // Permit gets dropped here since it is RAII.
    }

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), S3PutError> {
        self.storage.put_file(key, path).await
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), S3PutError> {
        self.storage.put_bytes(key, bytes).await
    }
}

#[async_trait]
impl Configurable<StorageConfig> for AdmissionControlledS3Storage {
    async fn try_from_config(config: &StorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::AdmissionControlledS3(nacconfig) => {
                let s3_storage =
                    S3Storage::try_from_config(&StorageConfig::S3(nacconfig.s3_config.clone()))
                        .await?;
                let policy =
                    RateLimitPolicy::try_from_config(&nacconfig.rate_limiting_policy).await?;
                return Ok(Self::new(s3_storage, policy));
            }
            _ => {
                return Err(Box::new(StorageConfigError::InvalidStorageConfig));
            }
        }
    }
}

// Prefer enum dispatch over dyn since there could
// only be a handful of these policies.
#[derive(Debug)]
enum RateLimitPolicy {
    CountBasedPolicy(CountBasedPolicy),
}

impl RateLimitPolicy {
    async fn enter(&self) -> SemaphorePermit<'_> {
        match self {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                return policy.acquire().await;
            }
        }
    }
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
}

#[async_trait]
impl Configurable<RateLimitingConfig> for RateLimitPolicy {
    async fn try_from_config(config: &RateLimitingConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            RateLimitingConfig::CountBasedPolicy(count_policy) => {
                return Ok(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(
                    count_policy.max_concurrent_requests,
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{admissioncontrolleds3::AdmissionControlledS3Storage, s3::S3Storage};

    fn get_s3_client() -> aws_sdk_s3::Client {
        // Set up credentials assuming minio is running locally
        let cred = aws_sdk_s3::config::Credentials::new(
            "minio",
            "minio123",
            None,
            None,
            "loaded-from-env",
        );

        // Set up s3 client
        let config = aws_sdk_s3::config::Builder::new()
            .endpoint_url("http://127.0.0.1:9000".to_string())
            .credentials_provider(cred)
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .force_path_style(true)
            .build();

        aws_sdk_s3::Client::from_conf(config)
    }

    #[tokio::test]
    #[cfg(CHROMA_KUBERNETES_INTEGRATION)]
    async fn test_put_get_key() {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: "test".to_string(),
            client,
            upload_part_size_bytes: 1024 * 1024 * 8,
        };
        storage.create_bucket().await.unwrap();
        let admission_controlled_storage =
            AdmissionControlledS3Storage::new_with_default_policy(storage);

        let test_data = "test data";
        admission_controlled_storage
            .put_bytes("test", test_data.as_bytes().to_vec())
            .await
            .unwrap();

        let buf = admission_controlled_storage
            .get("test".to_string())
            .await
            .unwrap();

        let buf = String::from_utf8(Arc::unwrap_or_clone(buf)).unwrap();
        assert_eq!(buf, test_data);
    }
}
