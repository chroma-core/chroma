//! An embeddable Axum harness for named, process-local token buckets.

use std::{
    collections::HashMap, future::Future, io, net::SocketAddr, path::Path, sync::Arc,
    time::Duration,
};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
pub use chroma_tracing::OpenTelemetryConfig;
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use mdac::TokenBucket;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

/// Startup configuration. Rate and capacity are required rather than silently defaulted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// TCP address to bind; defaults to loopback port 8001.
    #[serde(default = "default_listen_address")]
    pub listen_address: SocketAddr,
    /// Fixed set of named bucket definitions.
    pub buckets: HashMap<String, BucketConfig>,
    /// OTLP exporter configuration, matching the frontend's `open_telemetry` settings.
    #[serde(default)]
    pub open_telemetry: Option<OpenTelemetryConfig>,
    /// Enable stdout tracing when OTEL is not configured, as in the frontend.
    #[serde(default)]
    pub stdout_tracing: bool,
}

/// Initialize the frontend's shared OTEL/stdout tracing setup.
/// Call once inside the Tokio runtime, before serving requests.
pub fn init_otel_tracing(config: &Config) {
    chroma_tracing::init_server_otel_tracing(config.open_telemetry.as_ref(), config.stdout_tracing);
}

/// Capacity and refill rate for one configured bucket.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    /// Maximum burst allowance in tokens.
    pub capacity: u32,
    /// Nanoseconds required to replenish one token.
    pub interval_ns: u64,
}

fn default_listen_address() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8001))
}

impl Config {
    /// Read optional YAML configuration, then apply `MDAC_` environment overrides.
    pub fn load(path: Option<&Path>) -> Result<Self, Box<figment::Error>> {
        let mut config = Figment::new();
        if let Some(path) = path {
            config = config.merge(Yaml::file(path));
        }
        config
            .merge(Env::prefixed("MDAC_"))
            .extract()
            .map_err(Box::new)
    }

    /// Validate all rates and construct every configured bucket with a full allowance.
    pub fn buckets(&self) -> io::Result<Arc<TokenBuckets>> {
        for (name, config) in &self.buckets {
            if config.capacity == 0
                || config.interval_ns == 0
                || config
                    .interval_ns
                    .checked_mul(u64::from(config.capacity))
                    .is_none()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("bucket {name:?}: capacity and interval_ns must be positive and their product must fit in u64"),
                ));
            }
        }
        Ok(Arc::new(TokenBuckets {
            buckets: self
                .buckets
                .iter()
                .map(|(name, config)| {
                    (
                        name.clone(),
                        TokenBucket::new(config.capacity, Duration::from_nanos(config.interval_ns)),
                    )
                })
                .collect(),
        }))
    }
}

/// A fixed set of buckets constructed at startup from configured definitions.
///
/// Each bucket uses its own capacity and refill interval. Names match exactly, including case
/// and whitespace. The map is immutable; requests update only the selected bucket's atomic state.
#[derive(Debug)]
pub struct TokenBuckets {
    buckets: HashMap<String, TokenBucket>,
}

impl TokenBuckets {
    /// Refund and drain a configured bucket.
    /// Returns `None` for unknown names without applying a refund.
    pub fn put_back_and_drain(&self, name: &str, excess: u32, need: u32) -> Option<bool> {
        self.buckets
            .get(name)
            .map(|bucket| bucket.put_back_and_drain(excess, need))
    }
}

/// Refund unused tokens, then request a new allowance. All fields are required.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRequest {
    /// Exact configured name identifying the bucket.
    pub name: String,
    /// Tokens to return, capped at the bucket's capacity before draining.
    pub excess: u32,
    /// Tokens to consume, all or nothing.
    pub need: u32,
}

/// Refund and drain either one bucket or a sequence of buckets in request order.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum UpdatesRequest {
    Single(UpdateRequest),
    Multiple(Vec<UpdateRequest>),
}

/// For configured names, the refund is applied regardless of whether the drain was admitted.
#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateResponse {
    pub admitted: bool,
}

/// The outcome of one update in an array, including its individual HTTP status.
#[derive(Debug, Deserialize, Serialize)]
pub struct BatchUpdateResponse {
    pub admitted: bool,
    pub status: u16,
}

/// Response shape follows whether the request contained an object or an array.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum UpdatesResponse {
    Single(UpdateResponse),
    Multiple(Vec<BatchUpdateResponse>),
}

/// Build embeddable routes around a registry. Router clones share the same named buckets.
pub fn router(buckets: Arc<TokenBuckets>) -> Router {
    let app = Router::new()
        .route("/api/v1/healthcheck", get(|| async { StatusCode::OK }))
        .route("/api/v1/token-bucket/put-back-and-drain", post(update))
        .with_state(buckets)
        .layer(DefaultBodyLimit::max(1024));
    chroma_tracing::add_tracing_middleware(app)
}

fn apply_update(buckets: &TokenBuckets, request: UpdateRequest) -> (StatusCode, UpdateResponse) {
    let Some(admitted) = buckets.put_back_and_drain(&request.name, request.excess, request.need)
    else {
        return (StatusCode::NOT_FOUND, UpdateResponse { admitted: false });
    };
    let status = if admitted {
        StatusCode::OK
    } else {
        StatusCode::TOO_MANY_REQUESTS
    };
    (status, UpdateResponse { admitted })
}

async fn update(
    State(buckets): State<Arc<TokenBuckets>>,
    Json(request): Json<UpdatesRequest>,
) -> (StatusCode, Json<UpdatesResponse>) {
    match request {
        UpdatesRequest::Single(request) => {
            let (status, response) = apply_update(&buckets, request);
            (status, Json(UpdatesResponse::Single(response)))
        }
        UpdatesRequest::Multiple(requests) => {
            let responses = requests
                .into_iter()
                .map(|request| {
                    let (status, response) = apply_update(&buckets, request);
                    BatchUpdateResponse {
                        admitted: response.admitted,
                        status: status.as_u16(),
                    }
                })
                .collect();
            (StatusCode::OK, Json(UpdatesResponse::Multiple(responses)))
        }
    }
}

/// Serve configured buckets and finish in-flight requests on graceful shutdown.
pub async fn serve(
    listener: TcpListener,
    buckets: Arc<TokenBuckets>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    axum::serve(listener, router(buckets))
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(names: &[&str], capacity: u32, interval_ns: u64) -> Config {
        Config {
            listen_address: default_listen_address(),
            open_telemetry: None,
            stdout_tracing: false,
            buckets: names
                .iter()
                .map(|name| {
                    (
                        (*name).to_owned(),
                        BucketConfig {
                            capacity,
                            interval_ns,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn concurrent_requests_share_one_named_bucket() {
        let buckets = config(&["new"], 5, 3_600_000_000_000).buckets().unwrap();
        let barrier = std::sync::Barrier::new(16);
        std::thread::scope(|scope| {
            let requests: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        usize::from(buckets.put_back_and_drain("new", 0, 1).unwrap())
                    })
                })
                .collect();
            let admitted: usize = requests
                .into_iter()
                .map(|request| request.join().unwrap())
                .sum();
            assert_eq!(admitted, 5);
        });
        assert_eq!(buckets.buckets.len(), 1);
    }

    #[test]
    fn sample_configuration_builds_a_bucket() {
        let config: Config = Figment::from(Yaml::string(include_str!("../sample_config.yaml")))
            .extract()
            .unwrap();
        assert_eq!(config.listen_address, default_listen_address());
        let otel = config.open_telemetry.as_ref().unwrap();
        assert_eq!(otel.endpoint, "http://localhost:4317");
        assert_eq!(otel.service_name, "mdac-service");
        assert_eq!(otel.filters.len(), 2);
        assert_eq!(config.buckets["tenant-a/reads"].capacity, 10);
        assert_eq!(config.buckets["tenant-b/reads"].capacity, 1000);
        assert_eq!(config.buckets["tenant-a/reads"].interval_ns, 100_000_000);
        assert_eq!(config.buckets["tenant-b/reads"].interval_ns, 10_000_000);
        let buckets = config.buckets().unwrap();
        assert_eq!(
            buckets.put_back_and_drain("tenant-a/reads", 0, 11),
            Some(false)
        );
        assert_eq!(
            buckets.put_back_and_drain("tenant-b/reads", 0, 1000),
            Some(true)
        );
    }

    #[test]
    fn telemetry_configuration_supports_defaults_and_custom_filters() {
        let config: Config = Figment::from(Yaml::string(
            "buckets: {}\nopen_telemetry: {endpoint: 'http://collector:4317'}",
        ))
        .extract()
        .unwrap();
        let otel = config.open_telemetry.unwrap();
        assert_eq!(otel.service_name, "chromadb");
        assert_eq!(otel.filters[0].crate_name, "chroma_frontend");
        assert_eq!(otel.filters[0].filter_level.to_string(), "trace");

        let config: Config = Figment::from(Yaml::string(
            "buckets: {}\nopen_telemetry:\n  endpoint: 'http://collector:4317'\n  service_name: limiter\n  filters: [{crate_name: mdac_service, filter_level: info}]",
        )).extract().unwrap();
        let otel = config.open_telemetry.unwrap();
        assert_eq!(otel.service_name, "limiter");
        assert_eq!(otel.filters.len(), 1);
        assert_eq!(otel.filters[0].filter_level.to_string(), "info");
    }

    #[test]
    fn configured_buckets_remain_independent_and_unknown_names_create_no_state() {
        let mut config = config(&["low"], 1, 3_600_000_000_000);
        config.buckets.insert(
            "high".into(),
            BucketConfig {
                capacity: 1000,
                interval_ns: 1_800_000_000_000,
            },
        );
        let buckets = config.buckets().unwrap();
        assert_eq!(buckets.buckets.len(), 2);
        for _ in 0..2 {
            assert_eq!(buckets.put_back_and_drain("low", 0, 2), Some(false));
            assert_eq!(buckets.put_back_and_drain("high", 0, 1000), Some(true));
            assert_eq!(buckets.put_back_and_drain("high", 0, 1), Some(false));
            assert_eq!(buckets.put_back_and_drain("high", 1000, 0), Some(true));
            assert_eq!(buckets.buckets.len(), 2);
        }
        assert_eq!(buckets.put_back_and_drain("unknown", 1000, 0), None);
        assert_eq!(buckets.buckets.len(), 2);
    }

    #[test]
    fn configuration_requires_explicit_rate_and_rejects_typos() {
        for yaml in [
            "listen_address: 127.0.0.1:8001",
            "buckets: {test: {capacity: 10}}",
            "buckets: {test: {interval_ns: 100}}",
            "buckets: {test: {capacity: 10, interval_ns: 100, capcity: 20}}",
        ] {
            assert!(Figment::from(Yaml::string(yaml))
                .extract::<Config>()
                .is_err());
        }
    }

    #[test]
    fn invalid_rates_are_startup_errors() {
        for (capacity, interval_ns) in [(0, 1), (1, 0), (2, u64::MAX)] {
            let config = config(&["invalid"], capacity, interval_ns);
            assert_eq!(
                config.buckets().unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
