use crate::StorageConfigError;
use crate::{
    config::{RateLimitingConfig, StorageConfig},
    s3::{S3GetError, S3PutError, S3Storage},
};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use futures::{future::Shared, stream, FutureExt, StreamExt};
use parking_lot::Mutex;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    sync::{Semaphore, SemaphorePermit},
};
use tracing::{Instrument, Span};

/// Wrapper over s3 storage that provides proxy features such as
/// request coalescing, rate limiting, etc.
#[derive(Clone)]
pub struct AdmissionControlledS3Storage {
    storage: S3Storage,
    #[allow(clippy::type_complexity)]
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
            rate_limiter: Arc::new(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(2))),
        }
    }

    pub fn new(storage: S3Storage, policy: RateLimitPolicy) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(policy),
        }
    }

    async fn parallel_fetch(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        let (content_length, ranges) = match storage.get_key_ranges(&key).await {
            Ok(ranges) => ranges,
            Err(e) => {
                tracing::error!("Error heading s3: {}", e);
                return Err(AdmissionControlledS3StorageError::S3GetError(e));
            }
        };

        // .buffer_unordered() below will hang if the range is empty (https://github.com/rust-lang/futures-rs/issues/2740), so we short-circuit here
        if content_length == 0 {
            return Ok(Arc::new(Vec::new()));
        }

        let part_size = storage.download_part_size_bytes;
        tracing::info!(
            "[AdmissionControlledS3][Parallel fetch] Content length: {}, key ranges: {:?}",
            content_length,
            ranges
        );
        let mut output_buffer: Vec<u8> = vec![0; content_length as usize];
        let mut output_slices = output_buffer.chunks_mut(part_size).collect::<Vec<_>>();
        let range_and_output_slices = ranges.iter().zip(output_slices.drain(..));
        let mut futures = Vec::new();
        let num_parts = range_and_output_slices.len();
        for (range, output_slice) in range_and_output_slices {
            let rate_limiter_clone = rate_limiter.clone();
            let storage_clone = storage.clone();
            let key_clone = key.clone();
            let fut = async move {
                // Acquire permit.
                let token = rate_limiter_clone.enter().await;
                let range_str = format!("bytes={}-{}", range.0, range.1);
                storage_clone
                    .fetch_range(key_clone, range_str)
                    .then(|res| async move {
                        let _token = token;
                        match res {
                            Ok(output) => {
                                let body = output.body;
                                let mut reader = body.into_async_read();
                                match reader.read_exact(output_slice).await {
                                    Ok(_) => Ok(()),
                                    Err(e) => {
                                        tracing::error!("Error reading from s3: {}", e);
                                        Err(AdmissionControlledS3StorageError::S3GetError(
                                            S3GetError::ByteStreamError(e.to_string()),
                                        ))
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error reading from s3: {}", e);
                                Err(AdmissionControlledS3StorageError::S3GetError(e))
                            }
                        }
                        // _token gets dropped due to RAII and we've released the permit.
                    })
                    .await
            };
            futures.push(fut);
        }
        // Await all futures and return the result.
        let _ = stream::iter(futures)
            .buffer_unordered(num_parts)
            .collect::<Vec<_>>()
            .await;
        Ok(Arc::new(output_buffer))
    }

    async fn read_from_storage(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        // Acquire permit.
        let _permit = rate_limiter.enter().await;
        let bytes_res = storage
            .get(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "S3 get"))
            .await;
        match bytes_res {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                tracing::error!("Error reading from s3: {}", e);
                Err(AdmissionControlledS3StorageError::S3GetError(e))
            }
        }
        // Permit gets dropped here due to RAII.
    }

    pub async fn get_parallel(
        &self,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3.
        let future_to_await;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).cloned();
            future_to_await = match maybe_inflight {
                Some(fut) => {
                    tracing::info!("[AdmissionControlledS3] Found inflight request to s3 for key: {}. Deduping", key);
                    fut
                }
                None => {
                    let get_parallel_storage_future = AdmissionControlledS3Storage::parallel_fetch(
                        self.storage.clone(),
                        self.rate_limiter.clone(),
                        key.clone(),
                    )
                    .boxed()
                    .shared();
                    requests.insert(key.clone(), get_parallel_storage_future.clone());
                    get_parallel_storage_future
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

    pub async fn get(
        &self,
        key: String,
    ) -> Result<Arc<Vec<u8>>, AdmissionControlledS3StorageError> {
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3.
        let future_to_await;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).cloned();
            future_to_await = match maybe_inflight {
                Some(fut) => fut,
                None => {
                    let get_storage_future = AdmissionControlledS3Storage::read_from_storage(
                        self.storage.clone(),
                        self.rate_limiter.clone(),
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
        res
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
pub enum RateLimitPolicy {
    CountBasedPolicy(CountBasedPolicy),
}

impl RateLimitPolicy {
    async fn enter(&self) -> SemaphorePermit<'_> {
        match self {
            RateLimitPolicy::CountBasedPolicy(policy) => policy.acquire().await,
        }
    }
}

#[derive(Debug)]
pub struct CountBasedPolicy {
    remaining_tokens: Semaphore,
}

impl CountBasedPolicy {
    fn new(max_allowed_outstanding: usize) -> Self {
        Self {
            remaining_tokens: Semaphore::new(max_allowed_outstanding),
        }
    }
    async fn acquire(&self) -> SemaphorePermit<'_> {
        let token_res = self.remaining_tokens.acquire().await;
        match token_res {
            Ok(token) => token,
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

    use rand::{distributions::Alphanumeric, Rng};

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

    async fn test_multipart_get_for_size(value_size: usize) {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: format!("test-{}", rand::thread_rng().gen::<u64>()),
            client,
            upload_part_size_bytes: 1024 * 1024 * 8,
            download_part_size_bytes: 1024 * 1024 * 8,
        };
        storage.create_bucket().await.unwrap();
        let admission_controlled_storage =
            AdmissionControlledS3Storage::new_with_default_policy(storage);

        // Randomly generate a 16 byte utf8 string.
        let test_data_key: String = rand::thread_rng()
            .sample_iter(Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        // Randomly generate data of size equaling value_size.
        let test_data_value_string: String = rand::thread_rng()
            .sample_iter(Alphanumeric)
            .take(value_size)
            .map(char::from)
            .collect();
        admission_controlled_storage
            .put_bytes(
                test_data_key.as_str(),
                test_data_value_string.as_bytes().to_vec(),
            )
            .await
            .unwrap();
        println!(
            "Wrote key {} with value of size {}",
            test_data_key,
            test_data_value_string.len()
        );

        // Parallel fetch.
        let buf = admission_controlled_storage
            .get_parallel(test_data_key.to_string())
            .await
            .unwrap();

        let buf = String::from_utf8(Arc::unwrap_or_clone(buf)).unwrap();
        assert_eq!(buf, test_data_value_string);
    }

    #[tokio::test]
    // Naming this "test_k8s_integration_" means that the Tilt stack is required. See rust/worker/README.md.
    async fn test_k8s_integration_put_get_key() {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: format!("test-{}", rand::thread_rng().gen::<u64>()),
            client,
            upload_part_size_bytes: 1024 * 1024 * 8,
            download_part_size_bytes: 1024 * 1024 * 8,
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

    #[tokio::test]
    // Naming this "test_k8s_integration_" means that the Tilt stack is required. See rust/worker/README.md.
    async fn test_k8s_integration_multipart_get() {
        // At 8 MB.
        test_multipart_get_for_size(1024 * 1024 * 8).await;
        // At < 8 MB.
        test_multipart_get_for_size(1024 * 1024 * 7).await;
        // At > 8 MB.
        test_multipart_get_for_size(1024 * 1024 * 10).await;
        // Greater than NAC limit i.e. > 2*8 MB = 16 MB.
        test_multipart_get_for_size(1024 * 1024 * 18).await;
    }

    #[tokio::test]
    async fn test_k8s_integration_empty_file() {
        test_multipart_get_for_size(0).await;
    }
}
