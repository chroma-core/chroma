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
use crate::config::Configurable;
use crate::errors::ChromaError;
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfigBuilder;
use aws_sdk_s3;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::primitives::ByteStream;
use futures::Stream;
use std::clone::Clone;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone)]
pub(crate) struct S3Storage {
    bucket: String,
    client: aws_sdk_s3::Client,
}

#[derive(Error, Debug)]
pub enum S3PutError {
    #[error("S3 PUT error: {0}")]
    S3PutError(String),
    #[error("S3 Dispatch failure error")]
    S3DispatchFailure,
}

impl ChromaError for S3PutError {
    fn code(&self) -> crate::errors::ErrorCodes {
        crate::errors::ErrorCodes::Internal
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
    fn code(&self) -> crate::errors::ErrorCodes {
        crate::errors::ErrorCodes::Internal
    }
}

impl S3Storage {
    fn new(bucket: &str, client: aws_sdk_s3::Client) -> S3Storage {
        return S3Storage {
            bucket: bucket.to_string(),
            client: client,
        };
    }

    async fn create_bucket(&self) -> Result<(), String> {
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

    pub(crate) async fn get(
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

    pub(crate) async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), S3PutError> {
        let bytestream = ByteStream::from(bytes);
        self.put_bytestream(key, bytestream).await
    }

    pub(crate) async fn put_file(&self, key: &str, path: &str) -> Result<(), S3PutError> {
        let bytestream = ByteStream::from_path(path).await;
        match bytestream {
            Ok(bytestream) => return self.put_bytestream(key, bytestream).await,
            Err(e) => {
                return Err(S3PutError::S3PutError(e.to_string()));
            }
        }
    }

    async fn put_bytestream(&self, key: &str, bytestream: ByteStream) -> Result<(), S3PutError> {
        let res = self
            .client
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(bytestream)
            .send()
            .await;
        match res {
            Ok(_) => {
                tracing::info!("put object {} to bucket {}", key, self.bucket);
                return Ok(());
            }
            Err(e) => match e {
                SdkError::ServiceError(err) => {
                    let inner_err = err.into_err();
                    let err_string = format!(
                        "S3 service error with code: {:?} and message: {:?}",
                        inner_err.code(),
                        inner_err.message()
                    );
                    tracing::error!("{}", err_string);
                    return Err(S3PutError::S3PutError(err_string));
                }
                SdkError::DispatchFailure(e) => {
                    tracing::error!("S3 Dispatch failure error {:?}", e);
                    return Err(S3PutError::S3DispatchFailure);
                }
                _ => {
                    tracing::error!("S3 Put Error: {}", e);
                    return Err(S3PutError::S3PutError(e.to_string()));
                }
            },
        }
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
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            StorageConfigError::InvalidStorageConfig => crate::errors::ErrorCodes::InvalidArgument,
            StorageConfigError::FailedToCreateBucket(_) => crate::errors::ErrorCodes::Internal,
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
                        aws_sdk_s3::Client::new(&config)
                    }
                };
                let storage = S3Storage::new(&s3_config.bucket, client);
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
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    #[cfg(CHROMA_KUBERNETES_INTEGRATION)]
    async fn test_get() {
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
        let client = aws_sdk_s3::Client::from_conf(config);

        let storage = S3Storage {
            bucket: "test".to_string(),
            client,
        };
        storage.create_bucket().await.unwrap();

        // Write some data to a test file, put it in s3, get it back and verify its contents
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();

        let test_data = "test data";
        let test_file_in = format!("{}/test_file_in", persist_path);
        std::fs::write(&test_file_in, test_data).unwrap();
        storage.put_file("test", &test_file_in).await.unwrap();
        let mut bytes = storage.get("test").await.unwrap();

        let mut buf = String::new();
        bytes.read_to_string(&mut buf).await.unwrap();
        assert_eq!(buf, test_data);
    }
}
