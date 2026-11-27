//! Exercise the production S3 client and wal3 refresh path over HTTP, without Tilt.
//! This controls responses and counts wire requests; it does not emulate all of S3.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chroma_config::{registry::Registry, Configurable};
use chroma_storage::config::{S3CredentialsConfig, S3StorageConfig, StorageConfig};
use chroma_storage::{ETag, Storage, StorageError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use wal3::interfaces::{FragmentManagerFactory, ManifestManagerFactory};
use wal3::{
    create_s3_factories, Error, LogReader, LogReaderOptions, LogWriterOptions, Manifest,
    ManifestAndWitness, ManifestRefresh, ManifestWitness,
};

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    if_none_match: Option<String>,
    sdk_request: Option<String>,
}

enum Response {
    Object,
    Error(u16, &'static str),
    MissingEtag,
    InvalidJson,
    TruncatedBody,
    Disconnect,
    Stall,
}

struct State {
    manifest: Manifest,
    etag: String,
    responses: VecDeque<Response>,
    requests: Vec<Request>,
}

struct Endpoint {
    url: String,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}

impl Endpoint {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            manifest: Manifest::new_empty("first-writer"),
            etag: "\"first\"".to_string(),
            responses: VecDeque::new(),
            requests: Vec::new(),
        }));
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        connections.spawn(serve(socket, Arc::clone(&shared)));
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });
        Self { url, state, task }
    }

    fn publish(&self) -> ManifestAndWitness {
        let mut state = self.state.lock().unwrap();
        state.manifest = Manifest::new_empty("second-writer");
        state.etag = "\"second\"".to_string();
        ManifestAndWitness {
            manifest: state.manifest.clone(),
            witness: ManifestWitness::ETag(ETag(state.etag.clone())),
        }
    }

    fn enqueue(&self, responses: impl IntoIterator<Item = Response>) {
        self.state.lock().unwrap().responses.extend(responses);
    }

    fn requests(&self) -> Vec<Request> {
        self.state.lock().unwrap().requests.clone()
    }

    fn assert_conditional_gets(&self, count: usize, etag: &str) {
        let requests = self.requests();
        assert_eq!(requests.len(), count, "wire requests: {requests:?}");
        for request in requests {
            assert_eq!(request.method, "GET");
            assert_eq!(
                request.path.split('?').next().unwrap(),
                "/test-bucket/log/manifest/MANIFEST"
            );
            assert_eq!(request.if_none_match.as_deref(), Some(etag));
        }
        assert!(self.state.lock().unwrap().responses.is_empty());
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // Dropping the task also drops its JoinSet, cancelling stalled connections.
        self.task.abort();
    }
}

async fn serve(mut socket: TcpStream, state: Arc<Mutex<State>>) {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let Ok(byte) = socket.read_u8().await else {
            return;
        };
        header.push(byte);
        assert!(
            header.len() < 32 * 1024,
            "unexpectedly large request header"
        );
    }
    let header = String::from_utf8(header).unwrap();
    let mut lines = header.lines();
    let mut first = lines.next().unwrap().split_whitespace();
    let request = Request {
        method: first.next().unwrap().to_string(),
        path: first.next().unwrap().to_string(),
        if_none_match: lines
            .clone()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| {
                key.eq_ignore_ascii_case("if-none-match")
                    .then(|| value.trim().to_string())
            }),
        sdk_request: lines
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| {
                key.eq_ignore_ascii_case("amz-sdk-request")
                    .then(|| value.trim().to_string())
            }),
    };
    let (response, manifest, etag) = {
        let mut state = state.lock().unwrap();
        state.requests.push(request.clone());
        (
            state.responses.pop_front().unwrap_or(Response::Object),
            serde_json::to_vec(&state.manifest).unwrap(),
            state.etag.clone(),
        )
    };
    let (status, body, response_etag, extra_length) = match response {
        Response::Object if request.if_none_match.as_deref() == Some(&etag) => {
            (304, Vec::new(), Some(etag), 0)
        }
        Response::Object => (200, manifest, Some(etag), 0),
        Response::Error(status, code) => (
            status,
            format!("<Error><Code>{code}</Code><Message>injected failure</Message></Error>")
                .into_bytes(),
            None,
            0,
        ),
        Response::MissingEtag => (200, manifest, None, 0),
        Response::InvalidJson => (200, b"not JSON".to_vec(), Some(etag), 0),
        Response::TruncatedBody => (200, b"{\"writer\":".to_vec(), Some(etag), 100),
        Response::Disconnect => return,
        Response::Stall => std::future::pending().await,
    };
    let mut header = format!(
        "HTTP/1.1 {status} test\r\nContent-Length: {}\r\nConnection: close\r\nx-amz-request-id: test\r\n",
        body.len() + extra_length,
    );
    if let Some(etag) = response_etag {
        header.push_str(&format!("ETag: {etag}\r\n"));
    }
    header.push_str("\r\n");
    if socket.write_all(header.as_bytes()).await.is_ok() && request.method != "HEAD" {
        let _ = socket.write_all(&body).await;
    }
}

struct Fixture {
    endpoint: Endpoint,
    reader: LogReader,
    cached: ManifestAndWitness,
}

impl Fixture {
    async fn new(sdk_attempts: u32) -> Self {
        let endpoint = Endpoint::start().await;
        let config = StorageConfig::S3(S3StorageConfig {
            bucket: "test-bucket".to_string(),
            credentials: S3CredentialsConfig::Explicit {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
                custom_endpoint: Some(endpoint.url.clone()),
                region: "us-east-1".to_string(),
            },
            request_retry_count: sdk_attempts,
            read_timeout_ms: 500,
            request_timeout_ms: 5_000,
            ..S3StorageConfig::default()
        });
        let storage = Arc::new(
            Storage::try_from_config(&config, &Registry::new())
                .await
                .unwrap(),
        );
        let (fragments, manifests) = create_s3_factories(
            LogWriterOptions::default(),
            LogReaderOptions::default(),
            storage,
            "log".to_string(),
            "reader".to_string(),
            Arc::new(()),
            Arc::new(()),
        );
        let reader = LogReader::new(
            LogReaderOptions::default(),
            fragments.make_consumer().await.unwrap(),
            manifests.make_consumer().await.unwrap(),
        );
        let cached = reader.manifest_and_witness().await.unwrap().unwrap();
        endpoint.state.lock().unwrap().requests.clear();
        Self {
            endpoint,
            reader,
            cached,
        }
    }

    async fn refresh(&self) -> Result<ManifestRefresh, Error> {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.reader.refresh_manifest(&self.cached),
        )
        .await
        .expect("manifest refresh must terminate")
    }

    // The public reader operations used before conditional refresh was added.
    async fn head_then_get(&self) -> Result<ManifestRefresh, Error> {
        if self.reader.verify(&self.cached).await? {
            Ok(ManifestRefresh::Unchanged)
        } else {
            Ok(match self.reader.manifest_and_witness().await? {
                Some(manifest) => ManifestRefresh::Changed(Box::new(manifest)),
                None => ManifestRefresh::Missing,
            })
        }
    }
}

#[tokio::test]
async fn changed_manifest_refresh_uses_one_request() {
    let baseline = Fixture::new(1).await;
    let expected = baseline.endpoint.publish();
    assert_eq!(
        baseline.head_then_get().await.unwrap(),
        ManifestRefresh::Changed(Box::new(expected.clone()))
    );
    let baseline_requests = baseline.endpoint.requests();
    assert_eq!(
        baseline_requests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        ["HEAD", "GET"]
    );
    assert!(baseline_requests.iter().all(|r| r.if_none_match.is_none()));

    let patched = Fixture::new(1).await;
    assert_eq!(patched.endpoint.publish(), expected);
    assert_eq!(
        patched.refresh().await.unwrap(),
        ManifestRefresh::Changed(Box::new(expected))
    );
    patched.endpoint.assert_conditional_gets(1, "\"first\"");
    println!(
        "changed manifest: HEAD-then-GET = {} requests; conditional refresh = {} request",
        baseline_requests.len(),
        patched.endpoint.requests().len()
    );
}

#[tokio::test]
async fn unchanged_manifest_is_one_request_in_both_implementations() {
    let baseline = Fixture::new(1).await;
    assert_eq!(
        baseline.head_then_get().await.unwrap(),
        ManifestRefresh::Unchanged
    );
    assert_eq!(baseline.endpoint.requests().len(), 1);
    assert_eq!(baseline.endpoint.requests()[0].method, "HEAD");

    let patched = Fixture::new(1).await;
    assert_eq!(patched.refresh().await.unwrap(), ManifestRefresh::Unchanged);
    patched.endpoint.assert_conditional_gets(1, "\"first\"");
}

#[tokio::test]
async fn changed_response_witness_can_validate_the_next_refresh() {
    let fixture = Fixture::new(1).await;
    fixture.endpoint.publish();
    let ManifestRefresh::Changed(updated) = fixture.refresh().await.unwrap() else {
        panic!("expected changed manifest");
    };
    assert_eq!(
        fixture.reader.refresh_manifest(&updated).await.unwrap(),
        ManifestRefresh::Unchanged
    );
    let requests = fixture.endpoint.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].if_none_match.as_deref(), Some("\"first\""));
    assert_eq!(requests[1].if_none_match.as_deref(), Some("\"second\""));
}

#[tokio::test]
async fn missing_manifest_is_not_reported_as_unchanged() {
    let fixture = Fixture::new(1).await;
    fixture
        .endpoint
        .enqueue([Response::Error(404, "NoSuchKey")]);
    assert_eq!(fixture.refresh().await.unwrap(), ManifestRefresh::Missing);
    fixture.endpoint.assert_conditional_gets(1, "\"first\"");
}

#[tokio::test]
async fn persistent_service_errors_propagate_after_bounded_retries() {
    for (status, code) in [
        (403, "AccessDenied"),
        (412, "PreconditionFailed"),
        (500, "InternalError"),
        (503, "SlowDown"),
    ] {
        let fixture = Fixture::new(1).await;
        // Disable SDK retries to check wal3's initial attempt plus three retries.
        fixture
            .endpoint
            .enqueue((0..4).map(|_| Response::Error(status, code)));
        let Error::StorageError(error) = fixture.refresh().await.unwrap_err() else {
            panic!("expected storage error for {code}");
        };
        match status {
            403 => assert!(matches!(*error, StorageError::PermissionDenied { .. })),
            503 => assert!(matches!(*error, StorageError::Backoff)),
            _ => assert!(matches!(*error, StorageError::Generic { .. })),
        }
        fixture.endpoint.assert_conditional_gets(4, "\"first\"");
    }
}

#[tokio::test]
async fn transient_failures_retry_the_original_witness_and_return_fresh_state() {
    for failure in [
        Response::Error(500, "InternalError"),
        Response::Error(503, "SlowDown"),
        Response::TruncatedBody,
        Response::Disconnect,
        Response::Stall,
    ] {
        let fixture = Fixture::new(1).await;
        let expected = fixture.endpoint.publish();
        fixture.endpoint.enqueue([failure]);
        assert_eq!(
            fixture.refresh().await.unwrap(),
            ManifestRefresh::Changed(Box::new(expected))
        );
        fixture.endpoint.assert_conditional_gets(2, "\"first\"");
    }
}

#[tokio::test]
async fn sdk_retry_preserves_the_conditional_header() {
    let fixture = Fixture::new(2).await;
    let expected = fixture.endpoint.publish();
    fixture.endpoint.enqueue([Response::Error(503, "SlowDown")]);
    assert_eq!(
        fixture.refresh().await.unwrap(),
        ManifestRefresh::Changed(Box::new(expected))
    );
    fixture.endpoint.assert_conditional_gets(2, "\"first\"");
    let requests = fixture.endpoint.requests();
    assert!(requests[0]
        .sdk_request
        .as_deref()
        .unwrap()
        .contains("attempt=1"));
    assert!(requests[1]
        .sdk_request
        .as_deref()
        .unwrap()
        .contains("attempt=2"));
}

#[tokio::test]
async fn malformed_successes_fail_instead_of_reusing_cached_state() {
    for response in [Response::MissingEtag, Response::InvalidJson] {
        let fixture = Fixture::new(1).await;
        fixture.endpoint.publish();
        fixture.endpoint.enqueue([response]);
        assert!(matches!(
            fixture.refresh().await,
            Err(Error::CorruptManifest(_))
        ));
        fixture.endpoint.assert_conditional_gets(1, "\"first\"");
    }
}
