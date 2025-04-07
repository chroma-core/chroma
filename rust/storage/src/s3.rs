// Presents an interface to a storage backend such as s3 or local disk.
// The interface is a simple key-value store, which maps to s3 well.
// For now the interface fetches a file and stores it at a specific
// location on disk. This is not ideal for s3, but it is a start.

// Ideally we would support streaming the file from s3 to the index
// but the current implementation of hnswlib makes this complicated.
// Once we move to our own implementation of hnswlib we can support
// streaming from s3.

use super::config::StorageConfig;
use super::stream::ByteStreamItem;
use super::stream::S3ByteStream;
use super::PutOptions;
use super::StorageConfigError;
use crate::{ETag, StorageError};
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfigBuilder;
use aws_sdk_s3;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_smithy_types::byte_stream::Length;
use bytes::Bytes;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use rand::Rng;
use std::clone::Clone;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tracing::Instrument;

#[derive(Clone)]
pub struct S3Storage {
    pub(super) bucket: String,
    pub(super) client: aws_sdk_s3::Client,
    pub(super) upload_part_size_bytes: usize,
    pub(super) download_part_size_bytes: usize,
}

impl S3Storage {
    fn new(
        bucket: &str,
        client: aws_sdk_s3::Client,
        upload_part_size_bytes: usize,
        download_part_size_bytes: usize,
    ) -> S3Storage {
        S3Storage {
            bucket: bucket.to_string(),
            client,
            upload_part_size_bytes,
            download_part_size_bytes,
        }
    }

    pub(super) async fn create_bucket(&self) -> Result<(), String> {
        // Creates a public bucket with default settings in the region.
        // This should only be used for testing and in production
        // the bucket should be provisioned ahead of time.
        let res = self
            .client
            .create_bucket()
            .bucket(self.bucket.clone())
            .send()
            .await;
        match res {
            Ok(_) => {
                tracing::info!("created bucket {}", self.bucket);
                Ok(())
            }
            Err(e) => match e {
                SdkError::ServiceError(err) => match err.into_err() {
                    CreateBucketError::BucketAlreadyExists(msg) => {
                        tracing::error!("bucket already exists: {}", msg);
                        Ok(())
                    }
                    CreateBucketError::BucketAlreadyOwnedByYou(msg) => {
                        tracing::error!("bucket already owned by you: {}", msg);
                        Ok(())
                    }
                    e => {
                        tracing::error!("Error creating bucket: {}", e.to_string());
                        Err::<(), String>(e.to_string())
                    }
                },
                _ => {
                    tracing::error!("Error creating bucket: {}", e);
                    Err::<(), String>(e.to_string())
                }
            },
        }
    }

    #[tracing::instrument(skip(self))]
    #[allow(clippy::type_complexity)]
    async fn get_stream_and_e_tag(
        &self,
        key: &str,
    ) -> Result<
        (
            Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>,
            Option<ETag>,
        ),
        StorageError,
    > {
        let res = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;
        match res {
            Ok(res) => {
                let byte_stream = res.body;
                Ok((
                    Box::new(S3ByteStream::new(byte_stream)),
                    res.e_tag.map(ETag),
                ))
            }
            Err(e) => {
                match e {
                    SdkError::ServiceError(err) => {
                        let inner = err.into_err();
                        match &inner {
                            aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                                Err(StorageError::NotFound {
                                    path: key.to_string(),
                                    source: Arc::new(inner),
                                })
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::InvalidObjectState(msg) => {
                                tracing::error!("invalid object state: {}", msg);
                                Err(StorageError::Generic {
                                    source: Arc::new(inner),
                                })
                            }
                            _ => {
                                tracing::error!("error: {}", inner.to_string());
                                Err(StorageError::Generic {
                                    source: Arc::new(inner),
                                })
                            }
                        }
                    }
                    _ => Err(StorageError::Generic {
                        source: Arc::new(e),
                    }),
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    #[allow(clippy::type_complexity)]
    pub(super) async fn get_key_ranges(
        &self,
        key: &str,
    ) -> Result<(i64, Vec<(i64, i64)>, Option<ETag>), StorageError> {
        let part_size = self.download_part_size_bytes as i64;
        let head_res = self
            .client
            .head_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;
        let (content_length, e_tag) = match head_res {
            Ok(res) => match res.content_length {
                Some(len) => (len, res.e_tag),
                None => {
                    return Err(StorageError::Message {
                        message: "No content length".to_string(),
                    })
                }
            },
            Err(e) => {
                return Err(StorageError::Generic {
                    source: Arc::new(e),
                })
            }
        };
        // Round up.
        let num_parts = (content_length as f64 / part_size as f64).ceil() as i64;
        let mut ranges = Vec::new();
        for i in 0..num_parts {
            let start = i * part_size;
            let end = if i == num_parts - 1 {
                content_length - 1
            } else {
                (i + 1) * part_size - 1
            };
            ranges.push((start, end));
        }
        Ok((content_length, ranges, e_tag.map(ETag)))
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn fetch_range(
        &self,
        key: String,
        range_str: String,
    ) -> Result<GetObjectOutput, StorageError> {
        let res = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(&key)
            .range(range_str)
            .send()
            .await;
        match res {
            Ok(output) => Ok(output),
            Err(e) => {
                tracing::error!("Error fetching range: {:?}", e);
                match e {
                    SdkError::ServiceError(err) => {
                        let inner = err.into_err();
                        match &inner {
                            aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                                Err(StorageError::NotFound {
                                    path: key.to_string(),
                                    source: Arc::new(inner),
                                })
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::InvalidObjectState(_) => {
                                Err(StorageError::Generic {
                                    source: Arc::new(inner),
                                })
                            }
                            _ => {
                                Err(StorageError::Generic {
                                    source: Arc::new(inner),
                                })
                            }
                        }
                    }
                    _ => Err(StorageError::Generic {
                        source: Arc::new(e),
                    }),
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn get_parallel(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let (content_length, ranges, e_tag) = self.get_key_ranges(key).await?;

        // .buffer_unordered() below will hang if the range is empty (https://github.com/rust-lang/futures-rs/issues/2740), so we short-circuit here
        if content_length == 0 {
            return Ok((Arc::new(Vec::new()), None));
        }

        let part_size = self.download_part_size_bytes;
        let mut output_buffer: Vec<u8> = vec![0; content_length as usize];
        let mut output_slices = output_buffer.chunks_mut(part_size).collect::<Vec<_>>();
        let range_and_output_slices = ranges.iter().zip(output_slices.drain(..));
        let mut get_futures = Vec::new();
        let num_parts = range_and_output_slices.len();
        for (range, output_slice) in range_and_output_slices {
            let range_str = format!("bytes={}-{}", range.0, range.1);
            let fut = self
                .fetch_range(key.to_string(), range_str)
                .then(|res| async move {
                    match res {
                        Ok(res) => {
                            let body = res.body;
                            let mut reader = body.into_async_read();
                            match reader.read_exact(output_slice).await {
                                Ok(_) => Ok(()),
                                Err(e) => {
                                    tracing::error!("Error reading range: {:?}", e);
                                    Err(StorageError::Generic {
                                        source: Arc::new(e),
                                    })
                                }
                            }
                        }
                        Err(e) => Err(e),
                    }
                });
            get_futures.push(fut);
        }
        // Await all futures and return the result.
        let _ = stream::iter(get_futures)
            .buffer_unordered(num_parts)
            .collect::<Vec<_>>()
            .await;
        Ok((Arc::new(output_buffer), e_tag))
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        self.get_with_e_tag(key).await.map(|(buf, _)| buf)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_with_e_tag(
        &self,
        key: &str,
    ) -> Result<(Arc<Vec<u8>>, Option<ETag>), StorageError> {
        let (mut stream, e_tag) = self
            .get_stream_and_e_tag(key)
            .instrument(tracing::trace_span!("S3 get stream"))
            .await?;
        let read_block_span = tracing::trace_span!("S3 read bytes to end");
        let buf = read_block_span
            .in_scope(|| async {
                let mut buf: Vec<u8> = Vec::new();
                while let Some(res) = stream.next().await {
                    match res {
                        Ok(chunk) => {
                            buf.extend(chunk);
                        }
                        Err(e) => {
                            tracing::error!("Error reading from S3: {}", e);
                            return Err(e);
                        }
                    }
                }
                Ok(Some(buf))
            })
            .await?;
        match buf {
            Some(buf) => Ok((Arc::new(buf), e_tag)),
            None => {
                // Buffer is empty. Nothing interesting to do.
                Ok((Arc::new(vec![]), None))
            }
        }
    }

    pub(super) fn is_oneshot_upload(&self, total_size_bytes: usize) -> bool {
        total_size_bytes < self.upload_part_size_bytes
    }

    #[tracing::instrument(skip(self, bytes))]
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

    #[tracing::instrument(skip(self))]
    pub async fn put_file(&self, key: &str, path: &str) -> Result<Option<ETag>, StorageError> {
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
            PutOptions::default(),
        )
        .await
    }

    #[tracing::instrument(skip(self, create_bytestream_fn))]
    async fn put_object(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        if self.is_oneshot_upload(total_size_bytes) {
            return self
                .oneshot_upload(key, total_size_bytes, create_bytestream_fn, options)
                .await;
        }
        self.multipart_upload(key, total_size_bytes, create_bytestream_fn, options)
            .await
    }

    #[tracing::instrument(skip(self, create_bytestream_fn))]
    pub(super) async fn oneshot_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(create_bytestream_fn(0..total_size_bytes).await?);
        let req = match options.if_not_exists {
            true => req.if_none_match('*'),
            false => req,
        };

        let req = match options.if_match {
            Some(e_tag) => req.if_match(e_tag.0),
            None => req,
        };

        let resp = req.send().await.map_err(|err| {
            let err = err.into_service_error();
            if err.meta().code() == Some("PreconditionFailed") {
                StorageError::Precondition {
                    path: key.to_string(),
                    source: Arc::new(err),
                }
            } else {
                StorageError::Generic {
                    source: Arc::new(err),
                }
            }
        })?;
        Ok(resp.e_tag.map(ETag))
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn prepare_multipart_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
    ) -> Result<(usize, usize, String), StorageError> {
        let mut part_count = (total_size_bytes / self.upload_part_size_bytes) + 1;
        let mut size_of_last_part = total_size_bytes % self.upload_part_size_bytes;
        if size_of_last_part == 0 {
            size_of_last_part = self.upload_part_size_bytes;
            part_count -= 1;
        }

        let upload_id = match self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StorageError::Generic {
                source: Arc::new(err.into_service_error()),
            })?
            .upload_id
        {
            Some(upload_id) => upload_id,
            None => {
                return Err(StorageError::Message {
                    message: "Multipart upload creation response missing upload ID".to_string(),
                });
            }
        };

        Ok((part_count, size_of_last_part, upload_id))
    }

    #[tracing::instrument(skip(self, create_bytestream_fn))]
    pub(super) async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_count: usize,
        part_index: usize,
        size_of_last_part: usize,
        create_bytestream_fn: &impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, StorageError>>,
    ) -> Result<CompletedPart, StorageError> {
        let this_part = if part_count - 1 == part_index {
            size_of_last_part
        } else {
            self.upload_part_size_bytes
        };
        let part_number = part_index as i32 + 1; // Part numbers start at 1
        let offset = part_index * self.upload_part_size_bytes;
        let length = this_part;

        let stream = create_bytestream_fn(offset..(offset + length)).await?;

        let upload_part_res = self
            .client
            .upload_part()
            .key(key)
            .bucket(&self.bucket)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await
            .map_err(|err| StorageError::Generic {
                source: Arc::new(err.into_service_error()),
            })?;

        Ok(CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build())
    }

    #[tracing::instrument(skip(self, upload_parts))]
    pub(super) async fn finish_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        upload_parts: Vec<CompletedPart>,
        options: PutOptions,
    ) -> Result<Option<ETag>, StorageError> {
        let complete_req = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(upload_parts))
                    .build(),
            )
            .upload_id(upload_id);

        let complete_req = match options.if_not_exists {
            true => complete_req.if_none_match('*'),
            false => complete_req,
        };

        let complete_req = match options.if_match {
            Some(e_tag) => complete_req.if_match(e_tag.0),
            None => complete_req,
        };

        let resp = complete_req
            .send()
            .await
            .map_err(|err| StorageError::Generic {
                source: Arc::new(err.into_service_error()),
            })?;
        Ok(resp.e_tag.map(ETag))
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
        let (part_count, size_of_last_part, upload_id) =
            self.prepare_multipart_upload(key, total_size_bytes).await?;

        let mut upload_parts = Vec::new();
        for part_index in 0..part_count {
            let completed_part = self
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
        }

        self.finish_multipart_upload(key, &upload_id, upload_parts, options)
            .await
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        tracing::debug!(key = %key, "Deleting object from S3");

        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => {
                tracing::debug!(key = %key, "Successfully deleted object from S3");
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, key = %key, "Failed to delete object from S3");
                Err(StorageError::Generic {
                    source: Arc::new(e.into_service_error()),
                })
            }
        }
    }

    pub async fn rename(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        tracing::info!(src = %src_key, dst = %dst_key, "Renaming object in S3");

        // S3 doesn't have a native rename operation, so we need to copy and delete
        match self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(format!("{}/{}", self.bucket, src_key))
            .key(dst_key)
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!(src = %src_key, dst = %dst_key, "Successfully copied object");
                // After successful copy, delete the original
                match self.delete(src_key).await {
                    Ok(_) => {
                        tracing::info!(src = %src_key, dst = %dst_key, "Successfully renamed object");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!(error = %e, src = %src_key, "Failed to delete source object after copy");
                        Err(e)
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, src = %src_key, dst = %dst_key, "Failed to copy object");
                Err(StorageError::Generic {
                    source: Arc::new(e.into_service_error()),
                })
            }
        }
    }
}

#[async_trait]
impl Configurable<StorageConfig> for S3Storage {
    async fn try_from_config(
        config: &StorageConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::S3(s3_config) => {
                let client = match &s3_config.credentials {
                    super::config::S3CredentialsConfig::Minio => {
                        // Set up credentials assuming minio is running locally
                        let cred = aws_sdk_s3::config::Credentials::new(
                            "minio",
                            "minio123",
                            None,
                            None,
                            "loaded-from-env",
                        );

                        let timeout_config_builder = TimeoutConfigBuilder::default()
                            .connect_timeout(Duration::from_millis(s3_config.connect_timeout_ms))
                            .read_timeout(Duration::from_millis(s3_config.request_timeout_ms));
                        let retry_config = RetryConfig::standard();

                        // Set up s3 client
                        let config = aws_sdk_s3::config::Builder::new()
                            .endpoint_url("http://minio.chroma:9000".to_string())
                            .credentials_provider(cred)
                            .behavior_version_latest()
                            .region(aws_sdk_s3::config::Region::new("us-east-1"))
                            .force_path_style(true)
                            .timeout_config(timeout_config_builder.build())
                            .retry_config(retry_config)
                            .build();
                        aws_sdk_s3::Client::from_conf(config)
                    }
                    super::config::S3CredentialsConfig::AWS => {
                        let config = aws_config::load_from_env().await;
                        let timeout_config_builder = TimeoutConfigBuilder::default()
                            .connect_timeout(Duration::from_millis(s3_config.connect_timeout_ms))
                            .read_timeout(Duration::from_millis(s3_config.request_timeout_ms));
                        let retry_config = RetryConfig::standard();
                        let config = config
                            .to_builder()
                            .timeout_config(timeout_config_builder.build())
                            .retry_config(retry_config)
                            .build();
                        aws_sdk_s3::Client::new(&config)
                    }
                };
                let storage = S3Storage::new(
                    &s3_config.bucket,
                    client,
                    s3_config.upload_part_size_bytes,
                    s3_config.download_part_size_bytes,
                );
                // for minio we create the bucket since it is only used for testing

                if let super::config::S3CredentialsConfig::Minio = &s3_config.credentials {
                    let res = storage.create_bucket().await;
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(Box::new(StorageConfigError::FailedToCreateBucket(e)));
                        }
                    }
                }
                Ok(storage)
            }
            _ => Err(Box::new(StorageConfigError::InvalidStorageConfig)),
        }
    }
}

pub async fn s3_client_for_test_with_new_bucket() -> crate::Storage {
    // Set up credentials assuming minio is running locally
    let cred =
        aws_sdk_s3::config::Credentials::new("minio", "minio123", None, None, "loaded-from-env");

    // Set up s3 client
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url("http://127.0.0.1:9000".to_string())
        .credentials_provider(cred)
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .force_path_style(true)
        .build();

    let storage = S3Storage::new(
        &format!("test-{}", rand::thread_rng().gen::<u64>()),
        aws_sdk_s3::Client::from_conf(config),
        1024 * 1024 * 8,
        1024 * 1024 * 8,
    );
    eprintln!("Creating bucket {}", storage.bucket);
    storage.create_bucket().await.unwrap();
    crate::Storage::S3(storage)
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;
    use rand::{distributions::Alphanumeric, Rng, SeedableRng};
    use std::io::Write;
    use tempfile::NamedTempFile;

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

        let test_data = "test data";
        storage
            .put_bytes("test", test_data.as_bytes().to_vec(), PutOptions::default())
            .await
            .unwrap();

        let buf = storage.get("test").await.unwrap();
        let buf = String::from_utf8(buf.to_vec()).unwrap();
        assert_eq!(buf, test_data);
    }

    async fn setup_with_bucket(
        upload_part_size_bytes: usize,
        download_part_size_bytes: usize,
    ) -> S3Storage {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: format!("test-{}", rand::thread_rng().gen::<u64>()),
            client,
            upload_part_size_bytes,
            download_part_size_bytes,
        };
        storage.create_bucket().await.unwrap();
        storage
    }

    async fn test_put_file(
        file_size: usize,
        upload_part_size_bytes: usize,
        download_part_size_bytes: usize,
    ) {
        let storage = setup_with_bucket(upload_part_size_bytes, download_part_size_bytes).await;

        let mut temp_file = NamedTempFile::new().unwrap();

        let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(0);
        let mut remaining_file_size = file_size;

        while remaining_file_size > 0 {
            let chunk_size = std::cmp::min(remaining_file_size, 4096);
            let mut chunk = vec![0u8; chunk_size];
            rng.try_fill(&mut chunk[..]).unwrap();
            temp_file.write_all(&chunk).unwrap();
            remaining_file_size -= chunk_size;
        }

        storage
            .put_file("test", temp_file.path().to_str().unwrap())
            .await
            .unwrap();

        let buf = storage.get("test").await.unwrap();
        let file_contents = std::fs::read(temp_file.path()).unwrap();
        assert_eq!(buf, file_contents.into());
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
        storage
            .put_bytes(
                test_data_key.as_str(),
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
        let (buf, _e_tag) = storage.get_parallel(&test_data_key).await.unwrap();

        let buf = String::from_utf8(Arc::unwrap_or_clone(buf)).unwrap();
        assert_eq!(buf, test_data_value_string);
    }

    #[tokio::test]
    // Naming this "test_k8s_integration_" means that the Tilt stack is required. See rust/worker/README.md.
    async fn test_k8s_integration_put_file_scenarios() {
        let test_upload_part_size_bytes = 1024 * 1024 * 8; // 8MB
        let test_download_part_size_bytes = 1024 * 1024 * 8; // 8MB

        // Under part size
        test_put_file(
            1024,
            test_upload_part_size_bytes,
            test_download_part_size_bytes,
        )
        .await;
        // At part size
        test_put_file(
            test_upload_part_size_bytes,
            test_upload_part_size_bytes,
            test_download_part_size_bytes,
        )
        .await;
        // Over part size
        test_put_file(
            (test_upload_part_size_bytes as f64 * 2.5) as usize,
            test_upload_part_size_bytes,
            test_download_part_size_bytes,
        )
        .await;
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
    async fn test_k8s_integration_if_not_exist() {
        let storage = setup_with_bucket(1024 * 1024 * 8, 1024 * 1024 * 8).await;
        storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: true,
                    if_match: None,
                },
            )
            .await
            .unwrap();

        let err = storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: true,
                    if_match: None,
                },
            )
            .await
            .unwrap_err();
        eprintln!("{:?}", err);
        assert!(matches!(
            &err,
            StorageError::Precondition { path: _, source: _ }
        ));
        if let StorageError::Precondition { path, source: _ } = err {
            assert_eq!("test", path)
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_e_tag_succeed() {
        let storage = setup_with_bucket(1024 * 1024 * 8, 1024 * 1024 * 8).await;
        storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: true,
                    if_match: None,
                },
            )
            .await
            .unwrap();
        let (_, e_tag) = storage.get_with_e_tag("test").await.unwrap();
        assert!(e_tag.is_some());

        storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: false,
                    if_match: e_tag,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_k8s_integration_e_tag_fail() {
        let storage = setup_with_bucket(1024 * 1024 * 8, 1024 * 1024 * 8).await;
        storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: true,
                    if_match: None,
                },
            )
            .await
            .unwrap();

        let err = storage
            .oneshot_upload(
                "test",
                0,
                |_| Box::pin(ready(Ok(ByteStream::from(Bytes::new())))) as _,
                PutOptions {
                    if_not_exists: false,
                    if_match: Some(ETag("e_tag".to_string())),
                },
            )
            .await
            .unwrap_err();
        eprintln!("{:?}", err);
        assert!(matches!(
            &err,
            StorageError::Precondition { path: _, source: _ }
        ));
        if let StorageError::Precondition { path, source: _ } = err {
            assert_eq!("test", path)
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_empty_file() {
        test_multipart_get_for_size(0).await;
    }

    #[test]
    fn test_put_options_default() {
        let default = PutOptions::default();

        assert!(!default.if_not_exists);
        assert_eq!(default.if_match, None);
    }
}
