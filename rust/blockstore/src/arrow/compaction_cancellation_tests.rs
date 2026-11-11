use crate::arrow::config::BlockManagerConfig;
use crate::arrow::provider::ArrowBlockfileProvider;
use crate::arrow::provider::BlockfileReaderOptions;
use crate::{BlockfileWriter, BlockfileWriterOptions};
use chroma_cache::new_cache_for_test;
use chroma_config::{registry::Registry, Configurable};
use chroma_storage::config::{S3CredentialsConfig, S3StorageConfig, StorageConfig};
use chroma_storage::Storage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

struct BlockedUploadServer {
    endpoint: String,
    requests: mpsc::UnboundedReceiver<String>,
    task: JoinHandle<()>,
}

impl Drop for BlockedUploadServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl BlockedUploadServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let requests_tx = requests_tx.clone();
                connections.spawn(async move {
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        assert!(headers.len() < 65536);
                        headers.push(socket.read_u8().await.unwrap());
                    }
                    let headers = String::from_utf8(headers).unwrap();
                    let request = headers.lines().next().unwrap();
                    if request.starts_with("PUT ") {
                        // Receipt of the HTTP request proves the real storage upload
                        // is underway. Withhold its response until the client cancels.
                        requests_tx.send(request.to_string()).unwrap();
                        let mut body = Vec::new();
                        let _ = socket.read_to_end(&mut body).await;
                    } else {
                        assert!(request.starts_with("GET ") || request.starts_with("HEAD "));
                        socket
                            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .await
                            .unwrap();
                    }
                });
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
}

#[tokio::test]
async fn test_compaction_cancelled_upload_cannot_publish() {
    let mut server = BlockedUploadServer::start().await;
    let storage = Box::pin(Storage::try_from_config(
        &StorageConfig::S3(S3StorageConfig {
            bucket: "compaction-cancellation".to_string(),
            credentials: S3CredentialsConfig::Explicit {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
                custom_endpoint: Some(server.endpoint.clone()),
                region: "us-east-1".to_string(),
            },
            request_retry_count: 0,
            ..S3StorageConfig::default()
        }),
        &Registry::new(),
    ))
    .await
    .unwrap();
    let provider = ArrowBlockfileProvider::new(
        storage,
        1024,
        new_cache_for_test(),
        new_cache_for_test(),
        BlockManagerConfig::default_num_concurrent_block_flushes(),
        BlockManagerConfig::default_max_concurrent_block_loads(),
    )
    .with_compaction_memory_options(false, true);
    let BlockfileWriter::ArrowOrderedBlockfileWriter(writer) = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(String::new()).ordered_mutations())
        .await
        .unwrap()
    else {
        panic!("expected ordered Arrow writer")
    };
    let id = writer.id();
    let active_writer = writer.clone();
    let task = tokio::spawn(async move {
        for key in 0..10000 {
            Box::pin(active_writer.set("a", key, key as f32))
                .await
                .unwrap();
        }
    });
    let request = tokio::time::timeout(std::time::Duration::from_secs(10), server.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(request.starts_with("PUT /compaction-cancellation/block/"));
    assert!(!task.is_finished(), "upload must still be awaiting storage");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let timeout = std::time::Duration::from_secs(5);
    assert!(
        tokio::time::timeout(timeout, Box::pin(writer.set("a", 10001, 1.0)))
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        tokio::time::timeout(timeout, Box::pin(writer.commit::<u32, f32>()))
            .await
            .unwrap()
            .is_err()
    );
    assert!(tokio::time::timeout(
        timeout,
        provider.read::<u32, f32>(BlockfileReaderOptions::new(id, String::new()))
    )
    .await
    .unwrap()
    .is_err());
    while let Ok(request) = server.requests.try_recv() {
        assert!(request.starts_with("PUT /compaction-cancellation/block/"));
    }
}
