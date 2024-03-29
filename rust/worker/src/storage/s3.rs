// Presents an interface to a storage backend such as s3 or local disk.
// The interface is a simple key-value store, which maps to s3 well.
// For now the interface fetches a file and stores it at a specific
// location on disk. This is not ideal for s3, but it is a start.

// Ideally we would support streaming the file from s3 to the index
// but the current implementation of hnswlib makes this complicated.
// Once we move to our own implementation of hnswlib we can support
// streaming from s3.

use super::{config::StorageConfig, Storage};
use crate::config::{Configurable, QueryServiceConfig};
use crate::errors::ChromaError;
use async_trait::async_trait;
use aws_sdk_s3;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_smithy_types::byte_stream::ByteStream;
use std::clone::Clone;
use std::io::Write;

#[derive(Clone)]
struct S3Storage {
    bucket: String,
    client: aws_sdk_s3::Client,
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
                println!("created bucket {}", self.bucket);
                return Ok(());
            }
            Err(e) => match e {
                SdkError::ServiceError(err) => match err.into_err() {
                    CreateBucketError::BucketAlreadyExists(msg) => {
                        println!("bucket already exists: {}", msg);
                        return Ok(());
                    }
                    CreateBucketError::BucketAlreadyOwnedByYou(msg) => {
                        println!("bucket already owned by you: {}", msg);
                        return Ok(());
                    }
                    e => {
                        println!("error: {}", e.to_string());
                        return Err::<(), String>(e.to_string());
                    }
                },
                _ => {
                    println!("error: {}", e);
                    return Err::<(), String>(e.to_string());
                }
            },
        }
    }
}

#[async_trait]
impl Configurable<StorageConfig> for S3Storage {
    async fn try_from_config(config: &StorageConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            StorageConfig::S3(s3_config) => {
                let config = aws_config::load_from_env().await;
                let client = aws_sdk_s3::Client::new(&config);

                let storage = S3Storage::new(&s3_config.bucket, client);
                return Ok(storage);
            }
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn get(&self, key: &str, path: &str) -> Result<(), String> {
        let mut file = std::fs::File::create(path);
        let res = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(key)
            .send()
            .await;
        match res {
            Ok(mut res) => {
                match file {
                    Ok(mut file) => {
                        while let bytes = res.body.next().await {
                            match bytes {
                                Some(bytes) => match bytes {
                                    Ok(bytes) => {
                                        file.write_all(&bytes).unwrap();
                                    }
                                    Err(e) => {
                                        println!("error: {}", e);
                                        return Err::<(), String>(e.to_string());
                                    }
                                },
                                None => {
                                    // Stream is done
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        return Err::<(), String>(e.to_string());
                    }
                }
                return Ok(());
            }
            Err(e) => {
                println!("error: {}", e);
                return Err::<(), String>(e.to_string());
            }
        }
    }

    async fn put(&self, key: &str, path: &str) -> Result<(), String> {
        // Puts from a file on disk to s3.
        let bytestream = ByteStream::from_path(path).await;
        match bytestream {
            Ok(bytestream) => {
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
                        println!("put object {} to bucket {}", key, self.bucket);
                        return Ok(());
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        return Err::<(), String>(e.to_string());
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
                return Err::<(), String>(e.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
            client: client,
        };
        storage.create_bucket().await.unwrap();

        // Write some data to a test file, put it in s3, get it back and verify its contents
        let tmp_dir = tempdir().unwrap();
        let persist_path = tmp_dir.path().to_str().unwrap().to_string();

        let test_data = "test data";
        let test_file_in = format!("{}/test_file_in", persist_path);
        let test_file_out = format!("{}/test_file_out", persist_path);
        std::fs::write(&test_file_in, test_data).unwrap();
        storage.put("test", &test_file_in).await.unwrap();
        storage.get("test", &test_file_out).await.unwrap();

        let contents = std::fs::read_to_string(test_file_out).unwrap();
        assert_eq!(contents, test_data);
    }
}
