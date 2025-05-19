use crate::{
    config::{RateLimitingConfig, StorageConfig},
    s3::S3Storage,
    GetOptions,
};
use crate::{ETag, PutOptions, StorageConfigError};
use async_trait::async_trait;
use aws_sdk_s3::primitives::{ByteStream, Length};
use bytes::Bytes;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use futures::future::BoxFuture;
use futures::{future::Shared, stream, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};
use std::{ops::Range, sync::atomic::AtomicUsize};
use tokio::{
    io::AsyncReadExt,
    select,
    sync::{Semaphore, SemaphorePermit, TryAcquireError},
};
use tracing::{Instrument, Span};

use crate::StorageError;

/// Wrapper over s3 storage that provides proxy features such as
/// request coalescing, rate limiting, etc.
/// For reads, it will coalesce requests for the same key and rate limit
/// the number of concurrent requests.
/// For writes, it will rate limit the number of concurrent requests.
#[derive(Clone)]
pub struct AdmissionControlledS3Storage {
    storage: S3Storage,
    #[allow(clippy::type_complexity)]
    outstanding_read_requests: Arc<tokio::sync::Mutex<HashMap<String, InflightRequest>>>,
    rate_limiter: Arc<RateLimitPolicy>,
}

#[derive(Debug, Clone)]
struct InflightRequest {
    priority: Arc<AtomicUsize>,
    notify_channel: Option<tokio::sync::mpsc::Sender<()>>,
    #[allow(clippy::type_complexity)]
    future: Shared<
        Pin<
            Box<
                dyn Future<Output = Result<(Arc<Vec<u8>>, Option<ETag>), StorageError>>
                    + Send
                    + 'static,
            >,
        >,
    >,
}

impl InflightRequest {
    // Not thread safe.
    async fn update_priority(&self, priority: StorageRequestPriority) {
        // It is ok to not do Compare And Swap here since the caller obtains a mutex before
        // performing this operation so at any point there will only be one writer
        // for this AtomicUsize.
        if let Some(channel) = &self.notify_channel {
            let curr_pri = self.priority.load(std::sync::atomic::Ordering::SeqCst);
            if priority.as_usize() < curr_pri {
                self.priority
                    .store(priority.as_usize(), std::sync::atomic::Ordering::SeqCst);
                // Ignore send errors since it can happen that the receiver is dropped
                // and the task is busy reading the data from s3.
                let _ = channel.send(()).await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, Eq, PartialEq, Default)]
pub enum StorageRequestPriority {
    #[default]
    P0 = 0,
    P1 = 1,
}

impl StorageRequestPriority {
    pub fn lowest() -> Self {
        StorageRequestPriority::P1
    }

    pub fn as_usize(self) -> usize {
        self as usize
    }
}

impl From<usize> for StorageRequestPriority {
    fn from(value: usize) -> Self {
        match value {
            0 => StorageRequestPriority::P0,
            1 => StorageRequestPriority::P1,
            _ => {
                tracing::warn!(
                    "Invalid StorageRequestPriority value: {}. Defaulting to lowest priority.",
                    value
                );
                StorageRequestPriority::lowest()
            }
        }
    }
}

impl AdmissionControlledS3Storage {
    pub fn new_with_default_policy(storage: S3Storage) -> Self {
        Self {
            storage,
            outstanding_read_requests: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(
                2,
                &vec![1.0],
            ))),
        }
    }

    pub fn new(storage: S3Storage, policy: RateLimitPolicy) -> Self {
        Self {
            storage,
            outstanding_read_requests: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(policy),
        }
    }

    async fn parallel_fetch(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
        priority: Arc<AtomicUsize>,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let (content_length, ranges, e_tag) = storage.get_key_ranges(&key).await?;

        // .buffer_unordered() below will hang if the range is empty (https://github.com/rust-lang/futures-rs/issues/2740), so we short-circuit here
        if content_length == 0 {
            return Ok((Arc::new(Vec::new()), e_tag));
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
            let priority = priority.clone();
            let fut = async move {
                // Acquire permit.
                let token = rate_limiter_clone.enter(priority, None).await;
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
                                        Err(StorageError::Generic {
                                            source: Arc::new(e),
                                        })
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error reading from s3: {}", e);
                                Err(StorageError::Generic {
                                    source: Arc::new(e),
                                })
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
        Ok((Arc::new(output_buffer), e_tag))
    }

    async fn read_from_storage(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
        priority: Arc<AtomicUsize>,
        channel_receiver: Option<tokio::sync::mpsc::Receiver<()>>,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        // Acquire permit.
        let _permit = rate_limiter.enter(priority, channel_receiver).await;
        storage
            .get_with_e_tag(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "S3 get"))
            .await
        // Permit gets dropped here due to RAII.
    }

    pub async fn get_parallel(
        &self,
        key: String,
        options: GetOptions,
    ) -> Result<Arc<Vec<u8>>, StorageError> {
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3.
        let future_to_await;
        {
            let mut requests = self.outstanding_read_requests.lock().await;
            let maybe_inflight = requests.get(&key).cloned();
            future_to_await = match maybe_inflight {
                Some(fut) => {
                    tracing::trace!("[AdmissionControlledS3] Found inflight request to s3 for key: {:?}. Deduping", key);
                    fut.update_priority(options.priority).await;
                    fut.future
                }
                None => {
                    let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
                    let get_parallel_storage_future = AdmissionControlledS3Storage::parallel_fetch(
                        self.storage.clone(),
                        self.rate_limiter.clone(),
                        key.clone(),
                        atomic_priority.clone(),
                    )
                    .boxed()
                    .shared();
                    requests.insert(
                        key.clone(),
                        InflightRequest {
                            priority: atomic_priority,
                            future: get_parallel_storage_future.clone(),
                            notify_channel: None,
                        },
                    );
                    get_parallel_storage_future
                }
            };
        }

        let res = future_to_await.await;
        {
            let mut requests = self.outstanding_read_requests.lock().await;
            requests.remove(&key);
        }
        Ok(res?.0)
    }

    pub async fn get(&self, key: &str, options: GetOptions) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag(key, options)
            .await
            .map(|(bytes, _e_tag)| bytes)
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
        options: GetOptions,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        if options.requires_strong_consistency {
            return self.strongly_consistent_get_with_e_tag(key, options).await;
        }
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3.
        let future_to_await;
        {
            let mut requests = self.outstanding_read_requests.lock().await;
            let maybe_inflight = requests.get(key).cloned();
            future_to_await = match maybe_inflight {
                Some(fut) => {
                    // Update the priority if the new request has higher priority.
                    fut.update_priority(options.priority).await;
                    fut.future
                }
                None => {
                    let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
                    let (tx, rx) = tokio::sync::mpsc::channel(100);
                    let get_storage_future = AdmissionControlledS3Storage::read_from_storage(
                        self.storage.clone(),
                        self.rate_limiter.clone(),
                        key.to_string(),
                        atomic_priority.clone(),
                        Some(rx),
                    )
                    .boxed()
                    .shared();
                    requests.insert(
                        key.to_string(),
                        InflightRequest {
                            priority: atomic_priority,
                            future: get_storage_future.clone(),
                            notify_channel: Some(tx),
                        },
                    );
                    get_storage_future
                }
            };
        }

        let res = future_to_await.await;
        {
            let mut requests = self.outstanding_read_requests.lock().await;
            requests.remove(key);
        }
        res
    }

    pub async fn strongly_consistent_get_with_e_tag(
        &self,
        key: &str,
        options: GetOptions,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
        AdmissionControlledS3Storage::read_from_storage(
            self.storage.clone(),
            self.rate_limiter.clone(),
            key.to_string(),
            atomic_priority,
            None,
        )
        .await
    }

    async fn oneshot_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
        // Acquire permit.
        let _permit = self.rate_limiter.enter(atomic_priority, None).await;
        self.storage
            .oneshot_upload(key, total_size_bytes, create_bytestream_fn, options)
            .await
        // Permit gets dropped due to RAII.
    }

    async fn multipart_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
        let (part_count, size_of_last_part, upload_id) = self
            .storage
            .prepare_multipart_upload(key, total_size_bytes)
            .await?;
        let mut upload_parts = Vec::new();
        for part_index in 0..part_count {
            // Acquire token.
            let _permit = self.rate_limiter.enter(atomic_priority.clone(), None).await;
            let completed_part = self
                .storage
                .upload_part(
                    key,
                    &upload_id,
                    part_count,
                    part_index,
                    size_of_last_part,
                    &create_bytestream_fn,
                )
                .await?;
            upload_parts.push(completed_part);
            // Permit gets dropped due to RAII.
        }

        self.storage
            .finish_multipart_upload(key, &upload_id, upload_parts, options)
            .await
    }

    async fn put_object(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        if self.storage.is_oneshot_upload(total_size_bytes) {
            return self
                .oneshot_upload(key, total_size_bytes, create_bytestream_fn, options)
                .await;
        }

        self.multipart_upload(key, total_size_bytes, create_bytestream_fn, options)
            .await?;
        Ok(None)
    }

    pub async fn put_file(
        &self,
        key: &str,
        path: &str,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let file_size = tokio::fs::metadata(path)
            .await
            .map_err(|err| StorageError::Generic {
                source: Arc::new(err),
            })?
            .len();

        let path = path.to_string();

        self.put_object(
            key,
            file_size as usize,
            move |range| {
                let path = path.clone();

                async move {
                    ByteStream::read_from()
                        .path(path)
                        .offset(range.start as u64)
                        .length(Length::Exact(range.len() as u64))
                        .build()
                        .await
                        .map_err(|err| StorageError::Generic {
                            source: Arc::new(err),
                        })
                }
                .boxed()
            },
            options,
        )
        .await
    }

    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let bytes = Arc::new(Bytes::from(bytes));

        self.put_object(
            key,
            bytes.len(),
            move |range| {
                let bytes = bytes.clone();
                async move { Ok(ByteStream::from(bytes.slice(range))) }.boxed()
            },
            options,
        )
        .await
    }
}

#[async_trait]
impl Configurable<StorageConfig> for AdmissionControlledS3Storage {
    async fn try_from_config(
        config: &StorageConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::AdmissionControlledS3(nacconfig) => {
                let s3_storage = S3Storage::try_from_config(
                    &StorageConfig::S3(nacconfig.s3_config.clone()),
                    registry,
                )
                .await?;
                let policy =
                    RateLimitPolicy::try_from_config(&nacconfig.rate_limiting_policy, registry)
                        .await?;
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
    async fn enter(
        &self,
        priority: Arc<AtomicUsize>,
        channel_receiver: Option<tokio::sync::mpsc::Receiver<()>>,
    ) -> SemaphorePermit<'_> {
        match self {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                policy.acquire(priority, channel_receiver).await
            }
        }
    }
}

#[derive(Debug)]
pub struct CountBasedPolicy {
    remaining_tokens: Vec<Semaphore>,
}

impl CountBasedPolicy {
    fn new(max_allowed_outstanding: usize, bandwidth_allocation: &Vec<f32>) -> Self {
        let mut remaining_tokens = Vec::with_capacity(bandwidth_allocation.len());
        for allocation in bandwidth_allocation {
            remaining_tokens.push(Semaphore::new(
                (max_allowed_outstanding as f32 * allocation).ceil() as usize,
            ));
        }
        Self { remaining_tokens }
    }

    async fn acquire(
        &self,
        priority: Arc<AtomicUsize>,
        mut channel_receiver: Option<tokio::sync::mpsc::Receiver<()>>,
    ) -> SemaphorePermit<'_> {
        loop {
            let current_priority = priority.load(Ordering::SeqCst);
            let current_priority: StorageRequestPriority = current_priority.into();

            // Try acquiring permits at current and lower priorities
            for pri in current_priority.as_usize()
                ..=StorageRequestPriority::lowest()
                    .as_usize()
                    .min(self.remaining_tokens.len() - 1)
            {
                match self.remaining_tokens[pri].try_acquire() {
                    Ok(token) => {
                        return token;
                    }
                    Err(TryAcquireError::NoPermits) => continue,
                    Err(e) => panic!("Unexpected semaphore error: {}", e),
                }
            }

            match &mut channel_receiver {
                Some(rx) => {
                    select! {
                        did_recv = rx.recv() => {
                            // Reevaluate priority if we got a notification.
                            match did_recv {
                                Some(_) => {
                                    // If we got a notification, continue to acquire.
                                    continue;
                                }
                                None => {
                                    // If the channel was closed, break out of the loop.
                                    channel_receiver = None;
                                    continue;
                                }
                            }
                        }
                        token = self.remaining_tokens[current_priority.as_usize()].acquire() => {
                            match token {
                                Ok(token) => {
                                    // If we got a token, return it.
                                    return token;
                                },
                                Err(e) => {
                                    // If we got an error, log it and continue.
                                    tracing::error!("Error acquiring semaphore token: {}", e);
                                    panic!("Error acquiring semaphore token: {}", e);
                                }
                            }
                        }
                    }
                }
                None => {
                    let token = self.remaining_tokens[current_priority.as_usize()]
                        .acquire()
                        .await;
                    match token {
                        Ok(token) => {
                            // If we got a token, return it.
                            return token;
                        }
                        Err(e) => {
                            // If we got an error, log it and continue.
                            tracing::error!("Error acquiring semaphore token: {}", e);
                            panic!("Error acquiring semaphore token: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Configurable<RateLimitingConfig> for RateLimitPolicy {
    async fn try_from_config(
        config: &RateLimitingConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            RateLimitingConfig::CountBasedPolicy(count_policy) => {
                return Ok(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(
                    count_policy.max_concurrent_requests,
                    &count_policy.bandwidth_allocation,
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::{distributions::Alphanumeric, Rng};

    use crate::{admissioncontrolleds3::AdmissionControlledS3Storage, s3::S3Storage, GetOptions};

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
                &test_data_key,
                test_data_value_string.as_bytes().to_vec(),
                crate::PutOptions::default(),
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
            .get_parallel(test_data_key, GetOptions::default())
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
            .put_bytes(
                "test",
                test_data.as_bytes().to_vec(),
                crate::PutOptions::default(),
            )
            .await
            .unwrap();

        let buf = admission_controlled_storage
            .get("test", GetOptions::default())
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
