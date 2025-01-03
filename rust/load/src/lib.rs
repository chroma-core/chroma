//! chroma-load is the load generator for Chroma.
//!
//! This library conceptually separates the notion of a workload from the notion of a data set.
//! Data sets map onto collections in Chroma, but there can be many data sets per collection.
//! Effectively, a data set is a way to specify what it means to get, query, or upsert.
//!
//! Workloads specify a way to manipulate a data set.  They specify data-agnostic ways to get,
//! query, or upsert.  The workload type is compositional and recursive, so workloads can specify
//! blends of other workloads.
//!
//! The load harness provides a way to start and stop (workload, data set) pairs.  The nature of
//! the types means any workload can run against any data set (though the results may not be
//! meaningful except to be some form of load).

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::header::{HeaderMap, ACCEPT};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chromadb::client::{ChromaAuthMethod, ChromaClientOptions, ChromaTokenHeader};
use chromadb::ChromaClient;
use guacamole::combinators::*;
use guacamole::{Guacamole, Zipf};
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use opentelemetry::{Key, KeyValue, Value};
use tower_http::trace::TraceLayer;
use tracing::Instrument;
use uuid::Uuid;

pub mod bit_difference;
pub mod config;
pub mod data_sets;
pub mod opentelemetry_config;
pub mod rest;
pub mod words;
pub mod workloads;

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

/////////////////////////////////////////////// Error //////////////////////////////////////////////

/// Errors that can occur in the load service.
// TODO(rescrv):  Implement ChromaError.
#[derive(Debug)]
pub enum Error {
    /// The requested resource was not found.
    NotFound(String),
    /// The request was invalid.
    InvalidRequest(String),
    /// An internal error occurred.
    InternalError(String),
    /// A request to chroma failed.
    FailWorkload(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::NotFound(msg) => write!(f, "not found: {}", msg),
            Error::InvalidRequest(msg) => write!(f, "invalid request: {}", msg),
            Error::InternalError(msg) => write!(f, "internal error: {}", msg),
            Error::FailWorkload(msg) => write!(f, "workload failed: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        let (status, body) = match self {
            Error::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Error::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Error::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            Error::FailWorkload(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        axum::http::Response::builder()
            .status(status)
            .body((body.trim().to_string() + "\n").into())
            .expect("response and status are always valid")
    }
}

////////////////////////////////////////////// Metrics /////////////////////////////////////////////

#[derive(Debug)]
pub struct Metrics {
    /// The number of times an individual workload step was inhibited.  It will not be tracked as
    /// inactive or stepped.
    inhibited: Counter<u64>,
    /// The number of times an individual workload step was inactive.  It will not be tracked as
    /// inhibited or stepped.
    inactive: Counter<u64>,
    /// The number of times an individual workload was stepped.
    step: Counter<u64>,
    /// The number of times a workload issued a get against a data set.
    get: Counter<u64>,
    /// The number of times a workload issued a query against a data set.
    query: Counter<u64>,
    /// The number of times a workload issued an upsert against a data set.
    upsert: Counter<u64>,
}

////////////////////////////////////////////// client //////////////////////////////////////////////

/// Instantiate a new Chroma client.  This will use the CHROMA_HOST environment variable (or
/// http://localhost:8000 when unset) as the argument to [client_for_url].
pub async fn client() -> ChromaClient {
    let url = std::env::var("CHROMA_HOST").unwrap_or_else(|_| "http://localhost:8000".into());
    client_for_url(url).await
}

/// Create a new Chroma client for the given URL.  This will use the CHROMA_TOKEN environment
/// variable if set, or no authentication if unset.
pub async fn client_for_url(url: String) -> ChromaClient {
    if let Ok(auth) = std::env::var("CHROMA_TOKEN") {
        ChromaClient::new(ChromaClientOptions {
            url: Some(url),
            auth: ChromaAuthMethod::TokenAuth {
                token: auth,
                header: ChromaTokenHeader::XChromaToken,
            },
            database: "hf-tiny-stories".to_string(),
            connections: 4,
        })
        .await
        .unwrap()
    } else {
        ChromaClient::new(ChromaClientOptions {
            url: Some(url),
            auth: ChromaAuthMethod::None,
            database: "hf-tiny-stories".to_string(),
            connections: 4,
        })
        .await
        .unwrap()
    }
}

////////////////////////////////////////////// DataSet /////////////////////////////////////////////

/// A data set is an abstraction over a Chroma collection.  It is designed to allow callers to use
/// get/query/upsert without worrying about the semantics of a particular data set.  A valid
/// [GetQuery], [QueryQuery], or [UpsertQuery] should work for any data set or return an explicit
/// error.
#[async_trait::async_trait]
pub trait DataSet: std::fmt::Debug + Send + Sync {
    /// A human-readable name for the data set.  This will be used for starting workloads to pair
    /// them to a data set.
    fn name(&self) -> String;

    /// A human-readable description of the data set.  This will be used in the status endpoint.
    fn description(&self) -> String;

    /// A JSON representation of the data set.  This will be used in the status endpoint when
    /// requesting JSON.
    fn json(&self) -> serde_json::Value;

    /// Get documents from the data set.
    ///
    /// The semantics of this call is that it should loosely translate to a non-vector query,
    /// whatever that means for the implementor of the data set.
    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Query documents from the data set.
    ///
    /// The semantics of this call correspond to a vector query, whatever that means for the
    /// implementor of the data set.
    async fn query(
        &self,
        client: &ChromaClient,
        vq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Upsert documents into the data set.
    ///
    /// The semantics of this call correspond to writing documents into the data set, whatever that
    /// means for the implementor of the data set.
    async fn upsert(
        &self,
        client: &ChromaClient,
        uq: UpsertQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

/////////////////////////////////////////// Distribution ///////////////////////////////////////////

/// Distribution size and shape.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Distribution {
    /// Draw a constant value.
    Constant(usize),
    /// Draw from an exponential distribution with the given average.
    Exponential(f64),
    /// Draw from a uniform distribution between min and max.
    Uniform(usize, usize),
    /// Draw from a Zipf distribution with the given number of elements and theta (<1.0).
    Zipf(u64, f64),
}

impl Distribution {
    /// Given Guacamole, generate a sample from the distribution.
    pub fn sample(&self, guac: &mut Guacamole) -> usize {
        match self {
            Distribution::Constant(n) => *n,
            Distribution::Exponential(rate) => poisson(*rate)(guac).ceil() as usize,
            Distribution::Uniform(min, max) => uniform(*min, *max)(guac),
            Distribution::Zipf(n, alpha) => {
                let z = Zipf::from_alpha(*n, *alpha);
                z.next(guac) as usize
            }
        }
    }
}

impl Eq for Distribution {}

impl PartialEq for Distribution {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Distribution::Constant(a), Distribution::Constant(b)) => a == b,
            (Distribution::Exponential(a), Distribution::Exponential(b)) => a.total_cmp(b).is_eq(),
            (Distribution::Uniform(a, b), Distribution::Uniform(c, d)) => a == c && b == d,
            (Distribution::Zipf(a, b), Distribution::Zipf(c, d)) => {
                a == c && b.total_cmp(d).is_eq()
            }
            _ => false,
        }
    }
}

/////////////////////////////////////////////// Skew ///////////////////////////////////////////////

/// Distribution shape, without size.
#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Skew {
    /// A uniform skew introduces no bias in the selection.
    #[serde(rename = "uniform")]
    Uniform,
    /// A Zipf skew is skewed according to theta.  Theta=0.0 is uniform, theta=1.0-\epsilon is very
    /// skewed.  Try 0.9 and add nines for skew.
    #[serde(rename = "zipf")]
    Zipf { theta: f64 },
}

impl Eq for Skew {}

impl PartialEq for Skew {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Skew::Uniform, Skew::Uniform) => true,
            (Skew::Zipf { theta: a }, Skew::Zipf { theta: b }) => a.total_cmp(b).is_eq(),
            _ => false,
        }
    }
}

///////////////////////////////////////// TinyStoriesMixin /////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum TinyStoriesMixin {
    #[serde(rename = "numeric")]
    Numeric { ratio_selected: f64 },
}

impl TinyStoriesMixin {
    pub fn to_json(&self, guac: &mut Guacamole) -> serde_json::Value {
        match self {
            Self::Numeric { ratio_selected } => {
                let field: &'static str = match uniform(0u8, 5u8)(guac) {
                    0 => "i1",
                    1 => "i2",
                    2 => "i3",
                    3 => "f1",
                    4 => "f2",
                    5 => "f3",
                    _ => unreachable!(),
                };
                let mut center = uniform(0, 1_000_000)(guac);
                let window = (1e6 * ratio_selected) as usize;
                if window / 2 > center {
                    center = window / 2
                }
                let min = center - window / 2;
                let max = center + window / 2;
                serde_json::json!({"$and": [{field: {"$gte": min}}, {field: {"$lt": max}}]})
            }
        }
    }
}

//////////////////////////////////////////// WhereMixin ////////////////////////////////////////////

/// A metadata query specifies a metadata filter in Chroma.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum WhereMixin {
    /// A raw metadata query simply copies the provided filter spec.
    #[serde(rename = "query")]
    Constant(serde_json::Value),
    /// Search for a word from the provided set of words with skew.
    #[serde(rename = "fts")]
    FullTextSearch(Skew),
    /// The tiny stories workload.  The way these collections were setup, there are three fields
    /// each of integer, float, and string.  The integer fields are named i1, i2, and i3.  The
    /// float fields are named f1, f2, and f3.  The string fields are named s1, s2, and s3.
    ///
    /// This mixin selects one of these 6 numeric fields at random and picks a metadata range query
    /// to perform on it that will return data according to the mixin.
    #[serde(rename = "tiny-stories")]
    TinyStories(TinyStoriesMixin),
    /// A constant operator with different comparison.
    /// A mix of metadata queries selects one of the queries at random.
    #[serde(rename = "select")]
    Select(Vec<(f64, WhereMixin)>),
}

impl WhereMixin {
    /// Convert the metadata query into a JSON value suitable for use in a Chroma query.
    pub fn to_json(&self, guac: &mut Guacamole) -> serde_json::Value {
        match self {
            Self::Constant(query) => query.clone(),
            Self::FullTextSearch(skew) => {
                const WORDS: &[&str] = words::FEW_WORDS;
                let word = match skew {
                    Skew::Uniform => WORDS[uniform(0, WORDS.len() as u64)(guac) as usize],
                    Skew::Zipf { theta } => {
                        let z = Zipf::from_alpha(WORDS.len() as u64, *theta);
                        WORDS[z.next(guac) as usize]
                    }
                };
                serde_json::json!({"$contains": word.to_string()})
            }
            Self::TinyStories(mixin) => mixin.to_json(guac),
            Self::Select(select) => {
                let scale: f64 = any(guac);
                let mut total = scale * select.iter().map(|(p, _)| *p).sum::<f64>();
                for (p, mixin) in select {
                    if *p < 0.0 {
                        return serde_json::Value::Null;
                    }
                    if *p >= total {
                        return mixin.to_json(guac);
                    }
                    total -= *p;
                }
                serde_json::Value::Null
            }
        }
    }
}

impl Eq for WhereMixin {}

///////////////////////////////////////////// GetQuery /////////////////////////////////////////////

/// A get query specifies a get operation in Chroma.
///
/// This roughly corresponds to a skew in popularity of a key (note that it's not a distribution
/// because the distribution requires a size and that comes when bound to the workload).
///
/// The limit specifies a distribution of request sizes.  (note that it's a distribution and not a
/// skew because we specify the size as part of the query spec).
///
/// Then there are metadata and document filters, which are optional.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GetQuery {
    pub skew: Skew,
    pub limit: Distribution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WhereMixin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<WhereMixin>,
}

//////////////////////////////////////////// QueryQuery ////////////////////////////////////////////

/// A query query specifies a vector query operation in Chroma.
///
/// This roughly corresponds to a skew in popularity of a vector (note that it's not a distribution
/// because the distribution requires a size and that comes when bound to the workload).
///
/// The limit specifies a distribution of request sizes.  (note that it's a distribution and not a
/// skew because we specify the size as part of the query spec).
///
/// Then there are metadata and document filters, which are optional.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct QueryQuery {
    pub skew: Skew,
    pub limit: Distribution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WhereMixin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<WhereMixin>,
}

//////////////////////////////////////////// KeySelector ///////////////////////////////////////////

/// A means of selecting a key for upsert.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
pub enum KeySelector {
    /// Select a key by index.  If the index is out of bounds, the behavior is defined to wrap.
    #[serde(rename = "index")]
    Index(usize),
    /// Select a key by skew.  The skew is used to select a key from the distribution of keys the
    /// data set has available for upsert.
    #[serde(rename = "random")]
    Random(Skew),
}

//////////////////////////////////////////// UpsertQuery ///////////////////////////////////////////

/// An upsert query specifies an upsert operation in Chroma.
///
/// The batch will be selected using the provided key.  The batch size is the number of documents
/// to upsert in a single operation.  The associativity is the ratio is data set defined, but
/// generally means that denser operations will take place with higher values.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UpsertQuery {
    /// Select the document ID to upsert.
    pub key: KeySelector,
    /// The number of documents to upsert in a single operation.
    pub batch_size: usize,
    /// The associativity of the upsert operation.  Implementation-defined meaning.
    pub associativity: f64,
}

/////////////////////////////////////////// WorkloadState //////////////////////////////////////////

/// The state of a workload.
#[derive(Clone)]
pub struct WorkloadState {
    seq_no: u64,
    guac: Guacamole,
}

///////////////////////////////////////////// Workload /////////////////////////////////////////////

/// A workload is a description of a set of operations to perform against a data set.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Workload {
    /// No Operatioon; do nothing.
    #[serde(rename = "nop")]
    Nop,
    /// Resolve the workload by name.
    #[serde(rename = "by_name")]
    ByName(String),
    /// Get documents from the data set according to the query.
    #[serde(rename = "get")]
    Get(GetQuery),
    /// Query documents from the data set according to the query.
    #[serde(rename = "query")]
    Query(QueryQuery),
    /// A hybrid workload is a blend of other workloads.  The blend is specified as a list of other
    /// valid workload.  The probabilities are normalized to 1.0 before selection.
    #[serde(rename = "hybrid")]
    Hybrid(Vec<(f64, Workload)>),
    /// Delay the workload until after the specified time.
    #[serde(rename = "delay")]
    Delay {
        after: chrono::DateTime<chrono::FixedOffset>,
        wrap: Box<Workload>,
    },
    /// Load the data set.  Will repeatedly load until the time expires.
    #[serde(rename = "load")]
    Load,
    /// Randomly upsert a document.
    #[serde(rename = "random")]
    RandomUpsert(KeySelector),
}

impl Workload {
    /// A human-readable description of the workload.
    pub fn description(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    /// Resolve named workload references to the actual workloads they reference.
    pub fn resolve_by_name(&mut self, workloads: &HashMap<String, Workload>) -> Result<(), Error> {
        match self {
            Workload::Nop => {}
            Workload::ByName(name) => {
                if let Some(workload) = workloads.get(name) {
                    *self = workload.clone();
                } else {
                    return Err(Error::NotFound(format!("workload not found: {name}")));
                }
            }
            Workload::Get(_) => {}
            Workload::Query(_) => {}
            Workload::Hybrid(hybrid) => {
                for (_, workload) in hybrid {
                    workload.resolve_by_name(workloads)?;
                }
            }
            Workload::Delay { after: _, wrap } => wrap.resolve_by_name(workloads)?,
            Workload::Load => {}
            Workload::RandomUpsert(_) => {}
        }
        Ok(())
    }

    /// Do one operation of the workload against the data set.
    pub async fn step(
        &self,
        client: &ChromaClient,
        metrics: &Metrics,
        data_set: &dyn DataSet,
        state: &mut WorkloadState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Workload::Nop => {
                tracing::info!("nop");
                Ok(())
            }
            Workload::ByName(_) => {
                tracing::error!("cannot step by name; by_name should be resolved");
                Err(Box::new(Error::InternalError(
                    "cannot step by name".to_string(),
                )))
            }
            Workload::Get(get) => {
                metrics.get.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(data_set.name()),
                    )],
                );
                data_set
                    .get(client, get.clone(), &mut state.guac)
                    .instrument(tracing::info_span!("get"))
                    .await
            }
            Workload::Query(query) => {
                metrics.query.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(data_set.name()),
                    )],
                );
                data_set
                    .query(client, query.clone(), &mut state.guac)
                    .instrument(tracing::info_span!("query"))
                    .await
            }
            Workload::Hybrid(hybrid) => {
                let scale: f64 = any(&mut state.guac);
                let mut total = scale
                    * hybrid
                        .iter()
                        .filter_map(|(p, w)| if w.is_active() { Some(*p) } else { None })
                        .sum::<f64>();
                for (p, workload) in hybrid {
                    if *p < 0.0 {
                        return Err(Box::new(Error::InvalidRequest(
                            "hybrid probabilities must be positive".to_string(),
                        )));
                    }
                    if workload.is_active() {
                        if *p >= total {
                            return Box::pin(workload.step(client, metrics, data_set, state)).await;
                        }
                        total -= *p;
                    }
                }
                Err(Box::new(Error::InternalError(
                    "miscalculation of total hybrid probabilities".to_string(),
                )))
            }
            Workload::Delay { after: _, wrap } => {
                Box::pin(wrap.step(client, metrics, data_set, state)).await
            }
            Workload::Load => {
                metrics.upsert.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(data_set.name()),
                    )],
                );
                data_set
                    .upsert(
                        client,
                        UpsertQuery {
                            key: KeySelector::Index(state.seq_no as usize),
                            batch_size: 100,
                            // Associativity is the ratio of documents in a cluster to documents
                            // written by the workload.  It is ignored for load.
                            associativity: 0.0,
                        },
                        &mut state.guac,
                    )
                    .instrument(tracing::info_span!("load"))
                    .await
            }
            Workload::RandomUpsert(key) => {
                metrics.upsert.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(data_set.name()),
                    )],
                );
                data_set
                    .upsert(
                        client,
                        UpsertQuery {
                            key: key.clone(),
                            batch_size: 100,
                            // Associativity is the ratio of documents in a cluster to documents
                            // written by the workload.  It is ignored for load.
                            associativity: 0.0,
                        },
                        &mut state.guac,
                    )
                    .instrument(tracing::info_span!("load"))
                    .await
            }
        }
    }

    /// True if the workload is active, which means it may interact with Chroma.
    pub fn is_active(&self) -> bool {
        match self {
            Workload::Nop => true,
            Workload::ByName(_) => true,
            Workload::Get(_) => true,
            Workload::Query(_) => true,
            Workload::Hybrid(hybrid) => hybrid.iter().any(|(_, w)| w.is_active()),
            Workload::Delay { after, wrap } => chrono::Utc::now() >= *after && wrap.is_active(),
            Workload::Load => true,
            Workload::RandomUpsert(_) => true,
        }
    }
}

impl Eq for Workload {}

impl PartialEq for Workload {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Workload::Nop, Workload::Nop) => true,
            (Workload::ByName(a), Workload::ByName(b)) => a == b,
            (Workload::Get(a), Workload::Get(b)) => a == b,
            (Workload::Query(a), Workload::Query(b)) => a == b,
            (Workload::Hybrid(a), Workload::Hybrid(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(a, b)| a.0.total_cmp(&b.0).is_eq() && a.1 == b.1)
            }
            (
                Workload::Delay {
                    after: a,
                    wrap: a_wrap,
                },
                Workload::Delay {
                    after: b,
                    wrap: b_wrap,
                },
            ) => a == b && a_wrap == b_wrap,
            (Workload::Load, Workload::Load) => true,
            (Workload::RandomUpsert(a), Workload::RandomUpsert(b)) => a == b,
            _ => false,
        }
    }
}

//////////////////////////////////////////// Throughput ////////////////////////////////////////////

/// A throughput specification.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Throughput {
    /// Target a constant throughput.
    #[serde(rename = "constant")]
    Constant(f64),
    /// Operate in a sinusoidal fashion, oscillating between min and max throughput over
    /// periodicity seconds.
    #[serde(rename = "sinusoidal")]
    Sinusoidal {
        /// Trough throughput.
        min: f64,
        /// Peak throughput.
        max: f64,
        /// Periodicity in seconds.
        periodicity: usize,
    },
    /// Sawtooth throughput, increasing linearly from min to max throughput over periodicity
    #[serde(rename = "sawtooth")]
    Sawtooth {
        /// Starting throughput.
        min: f64,
        /// Ending throughput.
        max: f64,
        /// Periodicity in seconds.
        periodicity: usize,
    },
}

impl std::fmt::Display for Throughput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Throughput::Constant(throughput) => write!(f, "constant: {}", throughput),
            Throughput::Sinusoidal {
                min,
                max,
                periodicity,
            } => {
                write!(f, "sinusoidal: {} to {} over {}s", min, max, periodicity)
            }
            Throughput::Sawtooth {
                min,
                max,
                periodicity,
            } => {
                write!(f, "sawtooth: {} to {} over {}s", min, max, periodicity)
            }
        }
    }
}

impl Eq for Throughput {}

impl PartialEq for Throughput {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Throughput::Constant(a), Throughput::Constant(b)) => a == b,
            (
                Throughput::Sinusoidal {
                    min: amin,
                    max: amax,
                    periodicity: aperiodicity,
                },
                Throughput::Sinusoidal {
                    min: bmin,
                    max: bmax,
                    periodicity: bperiodicity,
                },
            ) => {
                amin.total_cmp(bmin).is_eq()
                    && amax.total_cmp(bmax).is_eq()
                    && aperiodicity == bperiodicity
            }
            (
                Throughput::Sawtooth {
                    min: amin,
                    max: amax,
                    periodicity: aperiodicity,
                },
                Throughput::Sawtooth {
                    min: bmin,
                    max: bmax,
                    periodicity: bperiodicity,
                },
            ) => {
                amin.total_cmp(bmin).is_eq()
                    && amax.total_cmp(bmax).is_eq()
                    && aperiodicity == bperiodicity
            }
            _ => false,
        }
    }
}

////////////////////////////////////////// RunningWorkload /////////////////////////////////////////

/// A running workload is a workload that has been bound to a data set at a given throughput.  It
/// is assigned a name, uuid, and expiration time.
#[derive(Clone, Debug)]
pub struct RunningWorkload {
    uuid: Uuid,
    name: String,
    workload: Workload,
    data_set: Arc<dyn DataSet>,
    expires: chrono::DateTime<chrono::FixedOffset>,
    throughput: Throughput,
}

impl RunningWorkload {
    /// A human-readable description of the running workload.
    pub fn description(&self) -> String {
        format!("{}/{}", self.uuid, self.data_set.name())
    }
}

impl From<WorkloadSummary> for Option<RunningWorkload> {
    fn from(s: WorkloadSummary) -> Self {
        if let Some(data_set) = data_sets::from_json(&s.data_set) {
            Some(RunningWorkload {
                uuid: s.uuid,
                name: s.name,
                workload: s.workload,
                data_set,
                expires: s.expires,
                throughput: s.throughput,
            })
        } else {
            None
        }
    }
}

impl Eq for RunningWorkload {}

impl PartialEq for RunningWorkload {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
            && self.name == other.name
            && self.workload == other.workload
            && self.data_set.json() == other.data_set.json()
            && self.expires == other.expires
            && self.throughput == other.throughput
    }
}

////////////////////////////////////////// WorkloadSummary /////////////////////////////////////////

/// A summary of a workload.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct WorkloadSummary {
    /// The UUID of the workload.
    pub uuid: Uuid,
    /// The name of the workload.
    pub name: String,
    /// The workload itself.
    pub workload: Workload,
    /// The data set the workload is bound to.
    pub data_set: serde_json::Value,
    /// The expiration time of the workload.
    pub expires: chrono::DateTime<chrono::FixedOffset>,
    /// The throughput of the workload.
    pub throughput: Throughput,
}

impl From<RunningWorkload> for WorkloadSummary {
    fn from(r: RunningWorkload) -> Self {
        WorkloadSummary {
            uuid: r.uuid,
            name: r.name,
            workload: r.workload,
            data_set: r.data_set.json(),
            expires: r.expires,
            throughput: r.throughput,
        }
    }
}

//////////////////////////////////////////// SavedState ////////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SavedState {
    inhibited: bool,
    running: Vec<WorkloadSummary>,
}

//////////////////////////////////////////// LoadHarness ///////////////////////////////////////////

/// A load harness is a collection of running workloads.
#[derive(Debug)]
pub struct LoadHarness {
    running: Vec<RunningWorkload>,
}

impl LoadHarness {
    /// The status of the load harness.
    pub fn status(&self) -> Vec<RunningWorkload> {
        self.running.clone()
    }

    /// Start a workload on the load harness.
    pub fn start(
        &mut self,
        name: String,
        workload: Workload,
        data_set: &Arc<dyn DataSet>,
        expires: chrono::DateTime<chrono::FixedOffset>,
        throughput: Throughput,
    ) -> Uuid {
        let uuid = Uuid::new_v4();
        let data_set = Arc::clone(data_set);
        self.running.push(RunningWorkload {
            uuid,
            name,
            workload,
            data_set,
            expires,
            throughput,
        });
        uuid
    }

    /// Stop a workload on the load harness.
    pub fn stop(&mut self, uuid: Uuid) -> bool {
        let old_sz = self.running.len();
        self.running.retain(|w| w.uuid != uuid);
        let new_sz = self.running.len();
        old_sz > new_sz
    }
}

//////////////////////////////////////////// LoadService ///////////////////////////////////////////

/// The load service is a collection of data sets and workloads that can be started and stopped.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct LoadService {
    metrics: Metrics,
    inhibit: Arc<AtomicBool>,
    harness: Mutex<LoadHarness>,
    running: Mutex<HashMap<Uuid, (Arc<AtomicBool>, tokio::task::JoinHandle<()>)>>,
    data_sets: Vec<Arc<dyn DataSet>>,
    workloads: HashMap<String, Workload>,
    persistent_path: Option<String>,
}

impl LoadService {
    /// Create a new load service from the given data sets and workloads.
    pub fn new(data_sets: Vec<Arc<dyn DataSet>>, workloads: HashMap<String, Workload>) -> Self {
        let meter = global::meter("chroma");
        let inhibited = meter.u64_counter("inhibited").build();
        let inactive = meter.u64_counter("inactive").build();
        let step = meter.u64_counter("step").build();
        let get = meter.u64_counter("get").build();
        let query = meter.u64_counter("query").build();
        let upsert = meter.u64_counter("upsert").build();
        let metrics = Metrics {
            inhibited,
            inactive,
            step,
            get,
            query,
            upsert,
        };
        LoadService {
            metrics,
            inhibit: Arc::new(AtomicBool::new(false)),
            harness: Mutex::new(LoadHarness { running: vec![] }),
            running: Mutex::new(HashMap::default()),
            data_sets,
            workloads,
            persistent_path: None,
        }
    }

    /// Set the persistent path and load its contents.
    pub fn set_persistent_path_and_load(
        &mut self,
        persistent_path: Option<String>,
    ) -> Result<(), Error> {
        if let Some(persistent_path) = persistent_path {
            self.persistent_path = Some(persistent_path);
            self.load_persistent()?;
        }
        Ok(())
    }

    /// Inhibit the load service.  This stops all activity in perpetuity until a call to uninhibit.
    /// Even subsequent calls to start will not do anything until a call to uninhibit.
    pub fn inhibit(&self) -> Result<(), Error> {
        self.inhibit
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.save_persistent()?;
        Ok(())
    }

    /// Uninhibit the load service.  This allows activity to resume.
    pub fn uninhibit(&self) -> Result<(), Error> {
        self.inhibit
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.save_persistent()?;
        Ok(())
    }

    /// Check if the load service is inhibited.
    pub fn is_inhibited(&self) -> bool {
        self.inhibit.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the data sets in the load service.
    pub fn data_sets(&self) -> &[Arc<dyn DataSet>] {
        &self.data_sets
    }

    /// Get the workloads in the load service.
    pub fn workloads(&self) -> &HashMap<String, Workload> {
        &self.workloads
    }

    /// Get the status of the load service.
    pub fn status(&self) -> Vec<WorkloadSummary> {
        let running = {
            // SAFETY(rescrv): Mutex poisoning.
            let harness = self.harness.lock().unwrap();
            harness.status()
        };
        running.into_iter().map(WorkloadSummary::from).collect()
    }

    /// Start a workload on the load service.
    pub fn start(
        &self,
        name: String,
        data_set: String,
        mut workload: Workload,
        expires: chrono::DateTime<chrono::FixedOffset>,
        throughput: Throughput,
    ) -> Result<Uuid, Error> {
        let Some(data_set) = self.data_sets().iter().find(|ds| ds.name() == data_set) else {
            return Err(Error::NotFound("data set not found".to_string()));
        };
        workload.resolve_by_name(self.workloads())?;
        let res = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut harness = self.harness.lock().unwrap();
            Ok(harness.start(name, workload.clone(), data_set, expires, throughput))
        };
        self.save_persistent()?;
        res
    }

    /// Stop a workload on the load service.
    pub fn stop(&self, uuid: Uuid) -> Result<(), Error> {
        // SAFETY(rescrv): Mutex poisoning.
        let mut harness = self.harness.lock().unwrap();
        if harness.stop(uuid) {
            drop(harness);
            self.save_persistent()?;
            Ok(())
        } else {
            Err(Error::NotFound("uuid not found".to_string()))
        }
    }

    /// Run the load service in perpetuity.
    pub async fn run(self: &Arc<Self>) -> ! {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _ = tx.send(tokio::task::spawn(async {}));
        let _reclaimer = tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                task.await.unwrap();
            }
        });
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let declared = {
                // SAFETY(rescrv): Mutex poisoning.
                let mut harness = self.harness.lock().unwrap();
                let now = chrono::Utc::now();
                for running in std::mem::take(&mut harness.running) {
                    if running.expires > now {
                        harness.running.push(running);
                    } else {
                        tracing::info!("workload expired: {}", running.description());
                    }
                }
                harness.running.clone()
            };
            // SAFETY(rescrv): Mutex poisoning.
            let mut running = self.running.lock().unwrap();
            let keys = running.keys().copied().collect::<Vec<_>>();
            for workload in keys {
                if !declared.iter().any(|w| w.uuid == workload) {
                    if let Some((done, task)) = running.remove(&workload) {
                        done.store(true, std::sync::atomic::Ordering::Relaxed);
                        // NOTE(rescrv):  Literally nothing to be done.  We've given up ownership
                        // of task by send, and if it fails we cannot cleanup.  Literally just log.
                        if let Err(err) = tx.send(task) {
                            tracing::error!("failed to send task to reclaimer: {err:?}");
                        }
                    }
                }
            }
            for declared in declared {
                if let Entry::Vacant(entry) = running.entry(declared.uuid) {
                    tracing::info!("spawning workload {}", declared.uuid);
                    let root = tracing::info_span!(parent: None, "workload");
                    let this = Arc::clone(self);
                    let done = Arc::new(AtomicBool::new(false));
                    let done_p = Arc::clone(&done);
                    let inhibit = Arc::clone(&self.inhibit);
                    let task = tokio::task::spawn(async move {
                        let _enter = root.enter();
                        this.run_one_workload(done, inhibit, declared)
                            .instrument(tracing::info_span!("run one workload"))
                            .await
                    });
                    entry.insert((done_p, task));
                }
            }
        }
    }

    async fn run_one_workload(
        self: Arc<Self>,
        done: Arc<AtomicBool>,
        inhibit: Arc<AtomicBool>,
        spec: RunningWorkload,
    ) {
        let client = Arc::new(client().await);
        let mut guac = Guacamole::new(spec.expires.timestamp_millis() as u64);
        let mut next_op = Instant::now();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
        let _ = tx
            .send(tokio::spawn(async move { Ok::<(), Error>(()) }))
            .await;
        let reaper = tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                if let Err(err) = task.await.unwrap() {
                    tracing::error!("workload task failed: {err:?}");
                }
            }
        });
        let mut seq_no = 0u64;
        let start = Instant::now();
        while !done.load(std::sync::atomic::Ordering::Relaxed) {
            seq_no += 1;
            let throughput = match spec.throughput {
                Throughput::Constant(throughput) => throughput,
                Throughput::Sinusoidal {
                    min,
                    max,
                    periodicity,
                } => {
                    let elapsed = start.elapsed().as_secs_f64();
                    let period = periodicity as f64;
                    let phase = (elapsed / period).fract();
                    min + 0.5 * (max - min) * (1.0 + phase.sin())
                }
                Throughput::Sawtooth {
                    min,
                    max,
                    periodicity,
                } => {
                    let elapsed = start.elapsed().as_secs_f64();
                    let period = periodicity as f64;
                    let phase = (elapsed / period).fract();
                    min + (max - min) * phase
                }
            };
            let delay = interarrival_duration(throughput)(&mut guac);
            next_op += delay;
            let now = Instant::now();
            if next_op > now {
                tokio::time::sleep(next_op - now).await;
            }
            if inhibit.load(std::sync::atomic::Ordering::Relaxed) {
                tracing::info!("inhibited");
                self.metrics.inhibited.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(spec.data_set.name()),
                    )],
                );
            } else if !spec.workload.is_active() {
                tracing::debug!("workload inactive");
                self.metrics.inactive.add(
                    1,
                    &[KeyValue::new(
                        Key::from_static_str("data_set"),
                        Value::from(spec.data_set.name()),
                    )],
                );
            } else {
                let workload = spec.workload.clone();
                let this = Arc::clone(&self);
                let client = Arc::clone(&client);
                let data_set = Arc::clone(&spec.data_set);
                let guac = Guacamole::new(any(&mut guac));
                let mut state = WorkloadState { seq_no, guac };
                let fut = async move {
                    this.metrics.step.add(
                        1,
                        &[KeyValue::new(
                            Key::from_static_str("data_set"),
                            Value::from(data_set.name()),
                        )],
                    );
                    workload
                        .step(&client, &this.metrics, &*data_set, &mut state)
                        .await
                        .map_err(|err| {
                            tracing::error!("workload failed: {err:?}");
                            Error::FailWorkload(err.to_string())
                        })
                };
                tx.send(tokio::spawn(fut)).await.unwrap();
            }
        }
        drop(tx);
        reaper.await.unwrap();
    }

    fn load_persistent(&self) -> Result<(), Error> {
        if let Some(persistent_path) = self.persistent_path.as_ref() {
            // SAFETY(rescrv): Mutex poisoning.
            let mut harness = self.harness.lock().unwrap();
            harness.running.clear();
            let saved_state_json = match std::fs::read_to_string(persistent_path) {
                Ok(saved_state_json) => saved_state_json,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        return Ok(());
                    } else {
                        return Err(Error::InternalError(err.to_string()));
                    }
                }
            };
            let saved_state: SavedState = serde_json::from_str(&saved_state_json)
                .map_err(|err| Error::InternalError(err.to_string()))?;
            self.inhibit
                .store(saved_state.inhibited, std::sync::atomic::Ordering::Relaxed);
            for workload in saved_state.running {
                if let Some(running) = <Option<RunningWorkload>>::from(workload) {
                    harness.running.push(running);
                }
            }
        }
        Ok(())
    }

    fn save_persistent(&self) -> Result<(), Error> {
        if let Some(persistent_path) = self.persistent_path.as_ref() {
            // SAFETY(rescrv): Mutex poisoning.
            let harness = self.harness.lock().unwrap();
            let saved_state = SavedState {
                inhibited: self.is_inhibited(),
                running: harness
                    .running
                    .iter()
                    .cloned()
                    .map(WorkloadSummary::from)
                    .collect(),
            };
            let saved_state_json = serde_json::to_string_pretty(&saved_state)
                .map_err(|err| Error::InternalError(err.to_string()))?;
            std::fs::write(persistent_path, saved_state_json.as_bytes())
                .map_err(|err| Error::InternalError(err.to_string()))?;
        }
        Ok(())
    }
}

impl Default for LoadService {
    fn default() -> Self {
        let data_sets = data_sets::all_data_sets();
        let workloads = workloads::all_workloads();
        Self::new(data_sets, workloads)
    }
}

//////////////////////////////////////////// entrypoint ////////////////////////////////////////////

#[derive(Clone, Debug)]
struct AppState {
    load: Arc<LoadService>,
}

async fn readme(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    match headers.get(ACCEPT).map(|x| x.as_bytes()) {
        Some(b"application/json") => {
            let running = state.load.status();
            let data_sets = state
                .load
                .data_sets()
                .iter()
                .map(|x| rest::Description::from(&**x))
                .collect::<Vec<_>>();
            let workloads = state
                .load
                .workloads()
                .iter()
                // SAFETY(rescrv): x.1 is always serializable to JSON.
                .map(|x| serde_json::to_value(x.1).unwrap())
                .collect::<Vec<_>>();
            Json(serde_json::json! {{
                "running": running,
                "inhibited": state.load.is_inhibited(),
                "data_sets": data_sets,
                "workloads": workloads,
            }})
            .into_response()
        }
        Some(b"*/*") | Some(b"text/plain") | None => {
            let mut output = r#"chroma-load
===========

This is a load generator service for Chroma.  This API is intended to be self-documenting.

It consists of endpoints, data sets, and workloads.  An endpoint is an exposed HTTP endpoint for
controlling chroma-load.  A workload specifies a mix of operations to perform.  A data set specifies
how to perform those operations.

# Endpoints
GET /           this document, available in "Accept: text/plain" and "Accept: application/json".
POST /start     start a job, requires a JSON body with the following fields:
                - name: the name of the job; this is a human-readable identifier and can duplicate
                - data_set: the name of the data set to use; see / for a list of data sets.
                - workload: the name of the workload to use; see / for a list of workloads.
                - expires: the time at which the job should expire in rfc3339 format.
                - throughput: the target throughput in ops/sec; may be floating point.
POST /stop      stop a job, requires a JSON body with the following fields:
                - uuid: the UUID of the job to stop.
POST /inhibit   inhibit load generation.
POST /uninhibit stop inhibiting load generation.

# Data Sets
        "#
            .to_string();
            for data_set in state.load.data_sets().iter() {
                output.push_str(&format!("\n## {}\n", data_set.name()));
                output.push_str(data_set.description().trim());
                output.push('\n');
            }
            if state.load.data_sets().is_empty() {
                output.push_str("\nNo data sets are available.\n");
            }

            output.push_str("\n# Workloads\n");
            for (name, workload) in state.load.workloads().iter() {
                output.push_str(&format!("\n## {}\n", name));
                output.push_str(workload.description().trim());
                output.push('\n');
            }
            if state.load.workloads().is_empty() {
                output.push_str("\nNo workloads are available.\n");
            }
            output.push_str("\n# At a Glance\n");
            if state.load.is_inhibited() {
                output.push_str("\nLoad generation is inhibited.\n");
            } else {
                for running in state.load.status() {
                    output.push_str(&format!("\n## {}\n", running.name));
                    output.push_str(&format!("Workload: {:?}\n", running.workload));
                    output.push_str(&format!("Data Set: {}\n", running.data_set));
                    output.push_str(&format!("Expires: {}\n", running.expires));
                    output.push_str(&format!("Throughput: {}\n", running.throughput));
                }
                if state.load.status().is_empty() {
                    output.push_str("\nNo workloads are running.\n");
                }
            }
            output.into_response()
        }
        Some(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn start(
    State(state): State<AppState>,
    Json(req): Json<rest::StartRequest>,
) -> Result<String, Error> {
    let expires = chrono::DateTime::parse_from_rfc3339(&req.expires)
        .map_err(|err| Error::InvalidRequest(format!("could not parse rfc3339: {err:?}")))?;
    let uuid = state.load.start(
        req.name,
        req.data_set,
        req.workload,
        expires,
        req.throughput,
    )?;
    Ok(uuid.to_string() + "\n")
}

async fn stop(
    State(state): State<AppState>,
    Json(req): Json<rest::StopRequest>,
) -> Result<String, Error> {
    state.load.stop(req.uuid)?;
    Ok("stopped\n".to_string())
}

async fn inhibit(State(state): State<AppState>) -> Result<String, Error> {
    state.load.inhibit()?;
    Ok("inhibited\n".to_string())
}

async fn uninhibit(State(state): State<AppState>) -> Result<String, Error> {
    state.load.uninhibit()?;
    Ok("uninhibited\n".to_string())
}

pub async fn entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => config::RootConfig::load_from_path(&config_path),
        Err(_) => config::RootConfig::load(),
    };

    let config = config.load_service;

    opentelemetry_config::init_otel_tracing(&config.service_name, &config.otel_endpoint);
    let mut load = LoadService::default();
    if let Err(err) = load.set_persistent_path_and_load(config.persistent_state_path.clone()) {
        tracing::warn!("failed to load persistent state: {:?}", err);
    }
    let load = Arc::new(load);
    let state = AppState {
        load: Arc::clone(&load),
    };
    let app = Router::new()
        .route("/", get(readme))
        .route("/start", post(start))
        .route("/stop", post(stop))
        .route("/inhibit", post(inhibit))
        .route("/uninhibit", post(uninhibit))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
                // Log the matched route's path (with placeholders not filled in).
                // Use request.uri() or OriginalUri if you want the real path.
                let matched_path = request
                    .extensions()
                    .get::<MatchedPath>()
                    .map(MatchedPath::as_str);
                tracing::info_span!(
                    "http_request",
                    method = ?request.method(),
                    matched_path,
                )
            }),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.port))
        .await
        .unwrap();
    let runner = tokio::task::spawn(async move { load.run().await });
    axum::serve(listener, app).await.unwrap();
    runner.abort();
}

pub fn humanize_expires(expires: &str) -> Option<String> {
    if let Ok(expires) = chrono::DateTime::parse_from_rfc3339(expires) {
        Some(expires.to_rfc3339())
    } else if let Some(duration) = expires.strip_suffix("s") {
        let expires = chrono::Utc::now() + chrono::Duration::seconds(duration.trim().parse().ok()?);
        Some(expires.to_rfc3339())
    } else if let Some(duration) = expires.strip_suffix("min") {
        let expires = chrono::Utc::now()
            + chrono::Duration::seconds(duration.trim().parse::<i64>().ok()? * 60i64);
        Some(expires.to_rfc3339())
    } else {
        Some(expires.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn end_to_end() {
        let load = LoadService::default();
        load.start(
            "foo".to_string(),
            "nop".to_string(),
            Workload::ByName("get-no-filter".to_string()),
            (chrono::Utc::now() + chrono::Duration::seconds(10)).into(),
            Throughput::Constant(1.0),
        )
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }

    #[test]
    fn workload_json() {
        let json = r#"{
  "hybrid": [
    [
      1.0,
      "nop"
    ],
    [
      1.0,
      {
        "by_name": "foo"
      }
    ],
    [
      1.0,
      {
        "get": {
          "skew": {
            "zipf": {
              "theta": 0.999
            }
          },
          "limit": {
            "Constant": 10
          }
        }
      }
    ],
    [
      1.0,
      {
        "query": {
          "skew": {
            "zipf": {
              "theta": 0.999
            }
          },
          "limit": {
            "Constant": 10
          }
        }
      }
    ],
    [
      1.0,
      {
        "delay": {
          "after": "2021-01-01T00:00:00Z",
          "wrap": "nop"
        }
      }
    ]
  ]
}"#;
        let workload = Workload::Hybrid(vec![
            (1.0, Workload::Nop),
            (1.0, Workload::ByName("foo".to_string())),
            (
                1.0,
                Workload::Get(GetQuery {
                    skew: Skew::Zipf { theta: 0.999 },
                    limit: Distribution::Constant(10),
                    document: None,
                    metadata: None,
                }),
            ),
            (
                1.0,
                Workload::Query(QueryQuery {
                    skew: Skew::Zipf { theta: 0.999 },
                    limit: Distribution::Constant(10),
                    document: None,
                    metadata: None,
                }),
            ),
            (
                1.0,
                Workload::Delay {
                    after: chrono::DateTime::parse_from_rfc3339("2021-01-01T00:00:00+00:00")
                        .unwrap(),
                    wrap: Box::new(Workload::Nop),
                },
            ),
        ]);
        assert_eq!(json, serde_json::to_string_pretty(&workload).unwrap());
    }

    #[test]
    fn workload_save_restore() {
        const TEST_PATH: &str = "workload_save_restore.test.json";
        std::fs::remove_file(TEST_PATH).ok();
        // First verse.
        let mut load = LoadService::default();
        load.set_persistent_path_and_load(Some(TEST_PATH.to_string()))
            .unwrap();
        load.start(
            "foo".to_string(),
            "nop".to_string(),
            Workload::ByName("get-no-filter".to_string()),
            (chrono::Utc::now() + chrono::Duration::seconds(10)).into(),
            Throughput::Constant(1.0),
        )
        .unwrap();
        let expected = {
            // SAFETY(rescrv):  Mutex poisoning.
            let harness = load.harness.lock().unwrap();
            assert_eq!(1, harness.running.len());
            harness.running[0].clone()
        };
        drop(load);
        println!("expected: {:?}", expected);
        // Second verse.
        let mut load = LoadService::default();
        {
            // SAFETY(rescrv):  Mutex poisoning.
            let harness = load.harness.lock().unwrap();
            assert!(harness.running.is_empty());
        }
        load.set_persistent_path_and_load(Some(TEST_PATH.to_string()))
            .unwrap();
        {
            // SAFETY(rescrv):  Mutex poisoning.
            let harness = load.harness.lock().unwrap();
            assert_eq!(1, harness.running.len());
            assert_eq!(expected, harness.running[0]);
        }
        std::fs::remove_file(TEST_PATH).ok();
    }
}
