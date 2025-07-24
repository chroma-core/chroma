use crate::StorageError;
use crate::{
    config::{RateLimitingConfig, StorageConfig},
    s3::S3Storage,
    GetOptions,
};
use crate::{DeleteOptions, ETag, PutOptions, StorageConfigError};
use async_trait::async_trait;
use aws_sdk_s3::primitives::{ByteStream, Length};
use bytes::Bytes;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_tracing::util::Stopwatch;
use futures::future::BoxFuture;
use futures::{stream, FutureExt, StreamExt};
use opentelemetry::{global, metrics::Counter, KeyValue};
use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};
use std::{ops::Range, sync::atomic::AtomicUsize};
use tokio::{
    io::AsyncReadExt,
    select,
    sync::{Semaphore, SemaphorePermit, TryAcquireError},
};

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
    metrics: AdmissionControlledS3StorageMetrics,
}

////// Metrics //////
#[derive(Debug, Clone)]
struct AdmissionControlledS3StorageMetrics {
    pub nac_dedup_count: opentelemetry::metrics::Counter<u64>,
    pub nac_lock_wait_duration_us: opentelemetry::metrics::Histogram<u64>,
    pub outstanding_read_requests: Arc<AtomicUsize>,
    pub read_requests_waiting_for_token: Arc<AtomicUsize>,
    pub hostname_attribute: [KeyValue; 1],
    pub nac_outstanding_read_requests: opentelemetry::metrics::Histogram<u64>,
    pub nac_read_requests_waiting_for_token: opentelemetry::metrics::Histogram<u64>,
    pub nac_priority_increase_sent: opentelemetry::metrics::Counter<u64>,
}

impl Default for AdmissionControlledS3StorageMetrics {
    fn default() -> Self {
        let meter = global::meter("chroma.storage.admission_control");
        Self {
            outstanding_read_requests: Arc::new(AtomicUsize::new(0)),
            read_requests_waiting_for_token: Arc::new(AtomicUsize::new(0)),
            hostname_attribute: [KeyValue::new(
                "hostname",
                std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
            )],
            nac_dedup_count: meter
                .u64_counter("nac_dedup_count")
                .with_description("Number of deduplicated requests")
                .build(),
            nac_lock_wait_duration_us: meter
                .u64_histogram("nac_lock_wait_duration_us")
                .with_description("Duration of waiting for the lock in microseconds")
                .with_unit("us")
                .build(),
            nac_outstanding_read_requests: meter
                .u64_histogram("nac_outstanding_requests")
                .with_description("Number of outstanding requests in the admission control system")
                .build(),
            nac_read_requests_waiting_for_token: meter
                .u64_histogram("nac_read_requests_waiting_for_token")
                .with_description(
                    "Number of requests in the admission control system waiting for a token",
                )
                .build(),
            nac_priority_increase_sent: meter
                .u64_counter("nac_priority_increase_sent")
                .with_description("Number of times increase of priority was sent")
                .build(),
        }
    }
}

////// Inflight Request Management //////
struct InflightRequest {
    priority: Arc<AtomicUsize>,
    priority_upgrade_channel: Option<tokio::sync::mpsc::Sender<()>>,
    #[allow(clippy::type_complexity)]
    senders: Vec<
        tokio::sync::oneshot::Sender<
            Result<(Arc<dyn Any + Send + Sync>, Option<ETag>), StorageError>,
        >,
    >,
}

impl InflightRequest {
    // Not thread safe.
    async fn update_priority(
        &self,
        priority: StorageRequestPriority,
        update_priority_counter: Counter<u64>,
        hostname: &[KeyValue],
    ) {
        // It is ok to not do Compare And Swap here since the caller obtains a mutex before
        // performing this operation so at any point there will only be one writer
        // for this AtomicUsize.
        if let Some(channel) = &self.priority_upgrade_channel {
            let curr_pri = self.priority.load(std::sync::atomic::Ordering::SeqCst);
            if priority.as_usize() < curr_pri {
                self.priority
                    .store(priority.as_usize(), std::sync::atomic::Ordering::SeqCst);
                update_priority_counter.add(1, hostname);
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

////// AdmissionControlledS3Storage //////

impl AdmissionControlledS3Storage {
    pub fn new_with_default_policy(storage: S3Storage) -> Self {
        Self {
            storage,
            outstanding_read_requests: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(
                2,
                &vec![1.0],
            ))),
            metrics: AdmissionControlledS3StorageMetrics::default(),
        }
    }

    pub fn new(storage: S3Storage, policy: RateLimitPolicy) -> Self {
        Self {
            storage,
            outstanding_read_requests: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(policy),
            metrics: AdmissionControlledS3StorageMetrics::default(),
        }
    }

    async fn parallel_fetch(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
        priority: Arc<AtomicUsize>,
        outstanding_read_request_counter: Arc<AtomicUsize>,
        outstanding_read_request_metric: opentelemetry::metrics::Histogram<u64>,
        hostname_attribute: [KeyValue; 1],
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let (content_length, ranges, e_tag) = storage.get_key_ranges(&key).await?;

        // .buffer_unordered() below will hang if the range is empty (https://github.com/rust-lang/futures-rs/issues/2740), so we short-circuit here
        if content_length == 0 {
            return Ok((Arc::new(Vec::new()), e_tag));
        }

        let part_size = storage.download_part_size_bytes;
        tracing::debug!(
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
            let outstanding_read_request_counter = outstanding_read_request_counter.clone();
            let outstanding_read_request_metric = outstanding_read_request_metric.clone();
            let hostname_attr_clone = hostname_attribute.clone();
            let fut = async move {
                // Acquire permit.
                let token = rate_limiter_clone.enter(priority, None).await;
                let range_str = format!("bytes={}-{}", range.0, range.1);
                outstanding_read_request_metric.record(
                    outstanding_read_request_counter.load(Ordering::Relaxed) as u64,
                    &hostname_attr_clone,
                );
                outstanding_read_request_counter.fetch_add(1, Ordering::Relaxed);
                let res = storage_clone
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
                    .await;
                outstanding_read_request_counter.fetch_sub(1, Ordering::Relaxed);
                res
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

    #[allow(clippy::too_many_arguments)]
    async fn read_from_storage(
        storage: S3Storage,
        rate_limiter: Arc<RateLimitPolicy>,
        key: String,
        priority: Arc<AtomicUsize>,
        channel_receiver: Option<tokio::sync::mpsc::Receiver<()>>,
        outstanding_read_request_counter: Arc<AtomicUsize>,
        outstanding_read_request_metric: opentelemetry::metrics::Histogram<u64>,
        hostname_attribute: [KeyValue; 1],
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        outstanding_read_request_metric.record(
            outstanding_read_request_counter.load(Ordering::Relaxed) as u64,
            &hostname_attribute,
        );
        outstanding_read_request_counter.fetch_add(1, Ordering::Relaxed);
        // Acquire permit.
        let _permit = rate_limiter.enter(priority, channel_receiver).await;
        let res = storage.get_with_e_tag(&key).await;
        outstanding_read_request_counter.fetch_sub(1, Ordering::Relaxed);
        res
        // Permit gets dropped here due to RAII.
    }

    pub async fn get_parallel(
        &self,
        key: String,
        options: GetOptions,
    ) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag_internal::<_, _, _>(&key, options, true, Self::no_op_closure)
            .await
            .map(|(bytes, _e_tag)| bytes)
    }

    pub async fn get(&self, key: &str, options: GetOptions) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag(key, options)
            .await
            .map(|(bytes, _e_tag)| bytes)
    }

    pub async fn fetch<FetchReturn, FetchFn, FetchFut>(
        &self,
        key: &str,
        options: GetOptions,
        fetch_fn: FetchFn,
    ) -> Result<(FetchReturn, Option<ETag>), StorageError>
    where
        FetchFn: FnOnce(Result<Arc<Vec<u8>>, StorageError>) -> FetchFut,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        self.get_with_e_tag_internal(key, options, false, fetch_fn)
            .await
    }

    pub async fn get_with_e_tag(
        &self,
        key: &str,
        options: GetOptions,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        self.get_with_e_tag_internal::<_, _, _>(key, options, false, Self::no_op_closure)
            .await
    }

    async fn no_op_closure(
        r: Result<Arc<Vec<u8>>, StorageError>,
    ) -> Result<Arc<Vec<u8>>, StorageError> {
        r
    }

    async fn dispatch_fetch<FetchReturn, FetchFn, FetchFut>(
        fetch_fn: FetchFn,
        input: Result<(Arc<Vec<u8>>, Option<ETag>), StorageError>,
    ) -> Result<(FetchReturn, Option<ETag>), StorageError>
    where
        FetchFn: FnOnce(Result<Arc<Vec<u8>>, StorageError>) -> FetchFut,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        match input {
            Ok((bytes, e_tag)) => {
                let ret = fetch_fn(Ok(bytes)).await;
                ret.map(|r| (r, e_tag))
            }
            Err(e) => {
                let ret = fetch_fn(Err(e)).await;
                ret.map(|r| (r, None))
            }
        }
    }

    async fn get_with_e_tag_internal<FetchReturn, FetchFn, FetchFut>(
        &self,
        key: &str,
        options: GetOptions,
        // TODO: remove is_parallel and move it into GetOptions, refactor all callers
        // to use the new options instead of special methods.
        is_parallel: bool,
        fetch_fn: FetchFn,
    ) -> Result<(FetchReturn, Option<ETag>), StorageError>
    where
        FetchFn: FnOnce(Result<Arc<Vec<u8>>, StorageError>) -> FetchFut,
        FetchFut: Future<Output = Result<FetchReturn, StorageError>> + Send + 'static,
        FetchReturn: Clone + Any + Sync + Send,
    {
        self.metrics.nac_outstanding_read_requests.record(
            self.metrics
                .outstanding_read_requests
                .load(Ordering::Relaxed) as u64,
            &self.metrics.hostname_attribute,
        );
        self.metrics
            .outstanding_read_requests
            .fetch_add(1, Ordering::Relaxed);

        if options.requires_strong_consistency {
            let res = self.strongly_consistent_get_with_e_tag(key, options).await;
            self.metrics
                .outstanding_read_requests
                .fetch_sub(1, Ordering::Relaxed);
            return Self::dispatch_fetch(fetch_fn, res).await;
        }
        // If there is a duplicate request and the original request finishes
        // before we look it up in the map below then we will end up with another
        // request to S3.
        let any_res;
        {
            let _stopwatch = Stopwatch::new(
                &self.metrics.nac_lock_wait_duration_us,
                &self.metrics.hostname_attribute,
                chroma_tracing::util::StopWatchUnit::Micros,
            );
            let mut requests = self.outstanding_read_requests.lock().await;
            any_res = match requests.get_mut(key) {
                Some(inflight_req) => {
                    self.metrics
                        .nac_dedup_count
                        .add(1, &self.metrics.hostname_attribute);
                    // Update the priority if the new request has higher priority.
                    inflight_req
                        .update_priority(
                            options.priority,
                            self.metrics.nac_priority_increase_sent.clone(),
                            &self.metrics.hostname_attribute,
                        )
                        .await;
                    let (output_tx, output_rx) = tokio::sync::oneshot::channel();
                    // Add the new sender to the existing request.
                    inflight_req.senders.push(output_tx);
                    drop(requests);
                    // TODO: don't unwrap here
                    output_rx
                        .await
                        .expect("Output channel should not be closed")?
                }
                None => {
                    let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
                    let (priority_tx, priority_rx) = tokio::sync::mpsc::channel(100);
                    requests.insert(
                        key.to_string(),
                        InflightRequest {
                            priority: atomic_priority.clone(),
                            priority_upgrade_channel: Some(priority_tx),
                            // The driving task is not a waiter
                            senders: vec![],
                        },
                    );
                    drop(requests);
                    let res = match is_parallel {
                        true => {
                            AdmissionControlledS3Storage::parallel_fetch(
                                self.storage.clone(),
                                self.rate_limiter.clone(),
                                key.to_string(),
                                atomic_priority,
                                self.metrics.read_requests_waiting_for_token.clone(),
                                self.metrics.nac_read_requests_waiting_for_token.clone(),
                                self.metrics.hostname_attribute.clone(),
                            )
                            .await
                        }
                        false => {
                            AdmissionControlledS3Storage::read_from_storage(
                                self.storage.clone(),
                                self.rate_limiter.clone(),
                                key.to_string(),
                                atomic_priority,
                                Some(priority_rx),
                                self.metrics.read_requests_waiting_for_token.clone(),
                                self.metrics.nac_read_requests_waiting_for_token.clone(),
                                self.metrics.hostname_attribute.clone(),
                            )
                            .await
                        }
                    };
                    let fetched = Self::dispatch_fetch(fetch_fn, res)
                        .await
                        .map(|(r, e_tag)| (Arc::new(r) as Arc<dyn Any + Send + Sync>, e_tag));
                    let mut requests = self.outstanding_read_requests.lock().await;
                    // SAFETY(hammadb): We just created this entry above, so it must exist.
                    let mut inflight = requests.remove(key).unwrap();
                    drop(requests);
                    for output_tx in inflight.senders.drain(..) {
                        // TODO: don't unwrap here
                        output_tx.send(fetched.clone()).unwrap();
                    }
                    fetched?
                }
            };
        }

        self.metrics
            .outstanding_read_requests
            .fetch_sub(1, Ordering::Relaxed);

        Ok((
            any_res
                .0
                .downcast::<FetchReturn>()
                .expect("Impossible state: downcast failed")
                .as_ref()
                .clone(),
            any_res.1,
        ))
    }

    async fn strongly_consistent_get_with_e_tag(
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
            self.metrics.read_requests_waiting_for_token.clone(),
            self.metrics.nac_read_requests_waiting_for_token.clone(),
            self.metrics.hostname_attribute.clone(),
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

    pub async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        // Akin to a HEAD request; no AC.
        self.storage.copy(src_key, dst_key).await
    }

    pub async fn list_prefix(
        &self,
        prefix: &str,
        options: GetOptions,
    ) -> Result<Vec<String>, StorageError> {
        let atomic_priority = Arc::new(AtomicUsize::new(options.priority.as_usize()));
        let _permit = self.rate_limiter.enter(atomic_priority, None).await;
        self.storage.list_prefix(prefix).await
    }

    pub async fn delete(&self, key: &str, options: DeleteOptions) -> Result<(), StorageError> {
        self.storage.delete(key, options).await
    }

    pub async fn delete_many<S: AsRef<str> + std::fmt::Debug, I: IntoIterator<Item = S>>(
        &self,
        keys: I,
    ) -> Result<crate::s3::DeletedObjects, StorageError> {
        self.storage.delete_many(keys).await
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
pub struct CountBasedPolicyMetrics {
    // The delay in milliseconds before a request is allowed to proceed.
    pub nac_delay_secs: opentelemetry::metrics::Histogram<u64>,
    pub nac_priority_increase_received: opentelemetry::metrics::Counter<u64>,
    pub nac_receive_channel_closed_count: opentelemetry::metrics::Counter<u64>,
    pub hostname_attribute: [KeyValue; 1],
    pub nac_available_permits: opentelemetry::metrics::Histogram<u64>,
}

impl Default for CountBasedPolicyMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma.storage.admission_control");
        Self {
            nac_delay_secs: meter
                .u64_histogram("nac_delay_secs")
                .with_description("The delay in seconds before a request is allowed to proceed.")
                .with_unit("secs")
                .build(),
            nac_priority_increase_received: meter
                .u64_counter("nac_priority_increase_received")
                .with_description("Number of times priority was increased for a request.")
                .build(),
            nac_receive_channel_closed_count: meter
                .u64_counter("nac_receive_channel_closed_count")
                .with_description("Number of times the receive channel was closed.")
                .build(),
            nac_available_permits: meter
                .u64_histogram("nac_available_permits")
                .with_description("Number of available permits in the semaphore.")
                .build(),
            hostname_attribute: [KeyValue::new(
                "hostname",
                std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
            )],
        }
    }
}

#[derive(Debug)]
pub struct CountBasedPolicy {
    remaining_tokens: Vec<Semaphore>,
    metrics: CountBasedPolicyMetrics,
}

impl CountBasedPolicy {
    fn new(max_allowed_outstanding: usize, bandwidth_allocation: &Vec<f32>) -> Self {
        let mut remaining_tokens = Vec::with_capacity(bandwidth_allocation.len());
        for allocation in bandwidth_allocation {
            remaining_tokens.push(Semaphore::new(
                (max_allowed_outstanding as f32 * allocation).ceil() as usize,
            ));
        }
        Self {
            remaining_tokens,
            metrics: CountBasedPolicyMetrics::default(),
        }
    }

    async fn acquire(
        &self,
        priority: Arc<AtomicUsize>,
        mut channel_receiver: Option<tokio::sync::mpsc::Receiver<()>>,
    ) -> SemaphorePermit<'_> {
        let priority_and_hostname_attr = [
            KeyValue::new("priority", priority.load(Ordering::Relaxed).to_string()),
            self.metrics.hostname_attribute[0].clone(),
        ];
        self.metrics.nac_available_permits.record(
            self.remaining_tokens[priority.load(Ordering::Relaxed)].available_permits() as u64,
            &priority_and_hostname_attr,
        );
        let _stopwatch = Stopwatch::new(
            &self.metrics.nac_delay_secs,
            &priority_and_hostname_attr,
            chroma_tracing::util::StopWatchUnit::Seconds,
        );
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
                                    self.metrics.nac_priority_increase_received.add(1, &self.metrics.hostname_attribute);
                                    // If we got a notification, continue to acquire.
                                    continue;
                                }
                                None => {
                                    self.metrics.nac_receive_channel_closed_count.add(1, &self.metrics.hostname_attribute);
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
            metrics: Default::default(),
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
            metrics: Default::default(),
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
    async fn test_k8s_dedupe_requests() {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: format!("test-{}", rand::thread_rng().gen::<u64>()),
            client,
            upload_part_size_bytes: 1024 * 1024 * 8,
            download_part_size_bytes: 1024 * 1024 * 8,
            metrics: Default::default(),
        };
        storage.create_bucket().await.unwrap();
        let admission_controlled_storage =
            AdmissionControlledS3Storage::new_with_default_policy(storage);

        let test_data_key: String = rand::thread_rng()
            .sample_iter(Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let test_data_value_string = "test data".to_string();
        admission_controlled_storage
            .put_bytes(
                &test_data_key,
                test_data_value_string.as_bytes().to_vec(),
                crate::PutOptions::default(),
            )
            .await
            .unwrap();

        const N_REQUESTS: usize = 100;
        let mut futures = Vec::new();
        for _ in 0..N_REQUESTS {
            let storage_clone = admission_controlled_storage.clone();
            let key_clone = test_data_key.clone();
            let test_data_value_string_clone = test_data_value_string.clone();
            let fut = async move {
                let buf = storage_clone
                    .get(key_clone.as_str(), GetOptions::default())
                    .await
                    .unwrap();
                let buf = String::from_utf8(Arc::unwrap_or_clone(buf)).unwrap();
                assert_eq!(buf, test_data_value_string_clone);
            };
            futures.push(fut);
        }
        // Await all futures and return the result.
        let _ = futures::future::join_all(futures).await;
    }

    #[tokio::test]
    async fn test_k8s_integration_empty_file() {
        test_multipart_get_for_size(0).await;
    }
}
