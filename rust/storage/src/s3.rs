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
use crate::GetError;
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfigBuilder;
use aws_sdk_s3;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_smithy_types::byte_stream::Length;
use bytes::Bytes;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_error::ErrorCodes;
use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use std::clone::Clone;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tracing::Instrument;
use tracing::Span;

#[derive(Clone)]
pub struct S3Storage {
    pub(super) bucket: String,
    pub(super) client: aws_sdk_s3::Client,
    pub(super) upload_part_size_bytes: usize,
    pub(super) download_part_size_bytes: usize,
}

#[derive(Error, Debug)]
pub enum S3PutError {
    #[error("S3 PUT error: {0}")]
    S3PutError(String),
    #[error("S3 Dispatch failure error")]
    S3DispatchFailure,
}

impl ChromaError for S3PutError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug, Clone)]
pub enum S3GetError {
    #[error("S3 GET error: {0}")]
    S3GetError(String),
    #[error("No such key: {0}")]
    NoSuchKey(String),
    #[error("ByteStream error: {0}")]
    ByteStreamError(String),
}

impl ChromaError for S3GetError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl S3Storage {
    fn new(
        bucket: &str,
        client: aws_sdk_s3::Client,
        upload_part_size_bytes: usize,
        download_part_size_bytes: usize,
    ) -> S3Storage {
        return S3Storage {
            bucket: bucket.to_string(),
            client,
            upload_part_size_bytes,
            download_part_size_bytes,
        };
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
                return Ok(());
            }
            Err(e) => match e {
                SdkError::ServiceError(err) => match err.into_err() {
                    CreateBucketError::BucketAlreadyExists(msg) => {
                        tracing::error!("bucket already exists: {}", msg);
                        return Ok(());
                    }
                    CreateBucketError::BucketAlreadyOwnedByYou(msg) => {
                        tracing::error!("bucket already owned by you: {}", msg);
                        return Ok(());
                    }
                    e => {
                        tracing::error!("Error creating bucket: {}", e.to_string());
                        return Err::<(), String>(e.to_string());
                    }
                },
                _ => {
                    tracing::error!("Error creating bucket: {}", e);
                    return Err::<(), String>(e.to_string());
                }
            },
        }
    }

    pub(super) async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = ByteStreamItem> + Unpin + Send>, S3GetError> {
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
                return Ok(Box::new(S3ByteStream::new(byte_stream)));
            }
            Err(e) => {
                tracing::error!("error: {}", e);
                match e {
                    SdkError::ServiceError(err) => {
                        let inner = err.into_err();
                        match inner {
                            aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(msg) => {
                                 tracing::error!("no such key: {}", msg);
                                return Err(S3GetError::NoSuchKey(msg.to_string()));
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::InvalidObjectState(msg) => {
                                 tracing::error!("invalid object state: {}", msg);
                                return Err(S3GetError::S3GetError(msg.to_string()));
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::Unhandled(_) =>  {
                                 tracing::error!("unhandled error");
                                return Err(S3GetError::S3GetError("unhandled error".to_string()));
                            }
                            _ => {
                                 tracing::error!("error: {}", inner.to_string());
                                return Err(S3GetError::S3GetError(inner.to_string()));
                            }
                        };
                    }
                    _ => {}
                }
                return Err(S3GetError::S3GetError(e.to_string()));
            }
        }
    }

    pub(super) async fn get_key_ranges(
        &self,
        key: &str,
    ) -> Result<(i64, Vec<(i64, i64)>), S3GetError> {
        let part_size = self.download_part_size_bytes as i64;
        let head_res = self
            .client
            .head_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;
        let content_length = match head_res {
            Ok(res) => match res.content_length {
                Some(len) => len,
                None => {
                    return Err(S3GetError::S3GetError("No content length".to_string()));
                }
            },
            Err(e) => {
                return Err(S3GetError::S3GetError(e.to_string()));
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
        Ok((content_length, ranges))
    }

    pub(super) async fn fetch_range(
        &self,
        key: String,
        range_str: String,
    ) -> Result<GetObjectOutput, S3GetError> {
        let res = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
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
                        match inner {
                            aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(msg) => {
                                return Err(S3GetError::NoSuchKey(msg.to_string()));
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::InvalidObjectState(msg) => {
                                return Err(S3GetError::S3GetError(msg.to_string()));
                            }
                            aws_sdk_s3::operation::get_object::GetObjectError::Unhandled(_) => {
                                return Err(S3GetError::S3GetError("unhandled error".to_string()));
                            }
                            _ => {
                                return Err(S3GetError::S3GetError(inner.to_string()));
                            }
                        };
                    }
                    _ => {
                        return Err(S3GetError::S3GetError(e.to_string()));
                    }
                }
            }
        }
    }

    pub(super) async fn get_parallel(&self, key: &str) -> Result<Arc<Vec<u8>>, S3GetError> {
        let (content_length, ranges) = self.get_key_ranges(key).await?;
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
                                    Err(S3GetError::ByteStreamError(e.to_string()))
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
        Ok(Arc::new(output_buffer))
    }

    pub async fn get(&self, key: &str) -> Result<Arc<Vec<u8>>, S3GetError> {
        let mut stream = self
            .get_stream(key)
            .instrument(tracing::trace_span!(parent: Span::current(), "S3 get stream"))
            .await?;
        let read_block_span = tracing::trace_span!(parent: Span::current(), "S3 read bytes to end");
        let buf = read_block_span
            .in_scope(|| async {
                let mut buf: Vec<u8> = Vec::new();
                while let Some(res) = stream.next().await {
                    match res {
                        Ok(chunk) => {
                            buf.extend(chunk);
                        }
                        Err(err) => {
                            tracing::error!("Error reading from S3: {}", err);
                            match err {
                                GetError::S3Error(e) => {
                                    return Err(e);
                                }
                                GetError::NoSuchKey(e) => {
                                    return Err(S3GetError::NoSuchKey(e));
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
            Some(buf) => Ok(Arc::new(buf)),
            None => {
                // Buffer is empty. Nothing interesting to do.
                Ok(Arc::new(vec![]))
            }
        }
    }

    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), S3PutError> {
        let bytes = Arc::new(Bytes::from(bytes));

        self.put_object(key, bytes.len(), move |range| {
            let bytes = bytes.clone();
            async move { Ok(ByteStream::from(bytes.slice(range))) }.boxed()
        })
        .await
    }

    pub async fn put_file(&self, key: &str, path: &str) -> Result<(), S3PutError> {
        let file_size = tokio::fs::metadata(path)
            .await
            .map_err(|err| S3PutError::S3PutError(err.to_string()))?
            .len();

        let path = path.to_string();

        self.put_object(key, file_size as usize, move |range| {
            let path = path.clone();

            async move {
                ByteStream::read_from()
                    .path(path)
                    .offset(range.start as u64)
                    .length(Length::Exact(range.len() as u64))
                    .build()
                    .await
                    .map_err(|err| S3PutError::S3PutError(err.to_string()))
            }
            .boxed()
        })
        .await
    }

    async fn put_object(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, S3PutError>>,
    ) -> Result<(), S3PutError> {
        if total_size_bytes < self.upload_part_size_bytes {
            return self
                .oneshot_upload(key, total_size_bytes, create_bytestream_fn)
                .await;
        }

        self.multipart_upload(key, total_size_bytes, create_bytestream_fn)
            .await
    }

    async fn oneshot_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, S3PutError>>,
    ) -> Result<(), S3PutError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(create_bytestream_fn(0..total_size_bytes).await?)
            .send()
            .await
            .map_err(|err| S3PutError::S3PutError(err.to_string()))?;

        Ok(())
    }

    async fn multipart_upload(
        &self,
        key: &str,
        total_size_bytes: usize,
        create_bytestream_fn: impl Fn(
            Range<usize>,
        ) -> BoxFuture<'static, Result<ByteStream, S3PutError>>,
    ) -> Result<(), S3PutError> {
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
            .map_err(|err| S3PutError::S3PutError(err.to_string()))?
            .upload_id
        {
            Some(upload_id) => upload_id,
            None => {
                return Err(S3PutError::S3PutError(
                    "Multipart upload creation response missing upload ID".to_string(),
                ));
            }
        };

        let mut upload_parts = Vec::new();
        for part_index in 0..part_count {
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
                .upload_id(&upload_id)
                .body(stream)
                .part_number(part_number)
                .send()
                .await
                .map_err(|err| S3PutError::S3PutError(err.to_string()))?;

            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_res.e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(upload_parts))
                    .build(),
            )
            .upload_id(&upload_id)
            .send()
            .await
            .map_err(|err| S3PutError::S3PutError(err.to_string()))?;

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum StorageConfigError {
    #[error("Invalid storage config")]
    InvalidStorageConfig,
    #[error("Failed to create bucket: {0}")]
    FailedToCreateBucket(String),
}

impl ChromaError for StorageConfigError {
    fn code(&self) -> ErrorCodes {
        match self {
            StorageConfigError::InvalidStorageConfig => ErrorCodes::InvalidArgument,
            StorageConfigError::FailedToCreateBucket(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Configurable<StorageConfig> for S3Storage {
    async fn try_from_config(config: &StorageConfig) -> Result<Self, Box<dyn ChromaError>> {
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
                            .endpoint_url(s3_config.endpoint.clone())
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
                match &s3_config.credentials {
                    super::config::S3CredentialsConfig::Minio => {
                        let res = storage.create_bucket().await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                return Err(Box::new(StorageConfigError::FailedToCreateBucket(e)));
                            }
                        }
                    }
                    _ => {}
                }

                return Ok(storage);
            }
            _ => {
                return Err(Box::new(StorageConfigError::InvalidStorageConfig));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use rand::{Rng, SeedableRng};
    use std::io::Write;
    use tempfile::{tempdir, NamedTempFile};

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
            download_part_size_bytes: 1024 * 1024 * 8,
        };
        storage.create_bucket().await.unwrap();

        let test_data = "test data";
        storage
            .put_bytes("test", test_data.as_bytes().to_vec())
            .await
            .unwrap();

        let mut stream = storage.get("test").await.unwrap();

        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => {
                    buf.extend_from_slice(&data);
                }
                Err(e) => {
                    panic!("Error reading stream: {}", e);
                }
            }
        }

        let buf = String::from_utf8(buf).unwrap();
        assert_eq!(buf, test_data);
    }

    async fn test_put_file(
        file_size: usize,
        upload_part_size_bytes: usize,
        download_part_size_bytes: usize,
    ) {
        let client = get_s3_client();

        let storage = S3Storage {
            bucket: "test".to_string(),
            client,
            upload_part_size_bytes,
            download_part_size_bytes,
        };
        storage.create_bucket().await.unwrap();

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
            .put_file("test", &temp_file.path().to_str().unwrap())
            .await
            .unwrap();

        let mut stream = storage.get_stream("test").await.unwrap();

        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => {
                    buf.extend_from_slice(&data);
                }
                Err(e) => {
                    panic!("Error reading stream: {}", e);
                }
            }
        }

        let file_contents = std::fs::read(temp_file.path()).unwrap();
        assert_eq!(buf, file_contents);
    }

    #[tokio::test]
    #[cfg(CHROMA_KUBERNETES_INTEGRATION)]
    async fn test_put_file_scenarios() {
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
            test_upload_part_size_bytes as usize,
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
}
