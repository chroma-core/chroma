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
use chromadb::v2::client::{ChromaAuthMethod, ChromaClientOptions, ChromaTokenHeader};
use chromadb::v2::ChromaClient;
use guacamole::combinators::*;
use guacamole::{Guacamole, Zipf};
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

#[derive(Debug)]
pub enum Error {
    NotFound(String),
    InvalidRequest(String),
    InternalError(String),
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

////////////////////////////////////////////// client //////////////////////////////////////////////

pub fn client() -> ChromaClient {
    let url = std::env::var("CHROMA_HOST").unwrap_or_else(|_| "http://localhost:8000".into());
    client_for_url(url)
}

pub fn client_for_url(url: String) -> ChromaClient {
    if let Ok(auth) = std::env::var("CHROMA_TOKEN") {
        ChromaClient::new(ChromaClientOptions {
            url,
            auth: ChromaAuthMethod::TokenAuth {
                token: auth,
                header: ChromaTokenHeader::XChromaToken,
            },
            database: Some("hf-tiny-stories".to_string()),
        })
    } else {
        ChromaClient::new(ChromaClientOptions {
            url,
            auth: ChromaAuthMethod::None,
            database: Some("hf-tiny-stories".to_string()),
        })
    }
}

////////////////////////////////////////////// DataSet /////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DataSet: std::fmt::Debug + Send + Sync {
    fn name(&self) -> String;
    fn description(&self) -> String;
    fn json(&self) -> serde_json::Value;

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn query(
        &self,
        client: &ChromaClient,
        vq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>>;

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
    Constant(usize),
    Exponential(f64),
    Uniform(usize, usize),
    Zipf(u64, f64),
}

impl Distribution {
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

/////////////////////////////////////////////// Skew ///////////////////////////////////////////////

/// Distribution shape, without size.
#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Skew {
    #[serde(rename = "uniform")]
    Uniform,
    #[serde(rename = "zipf")]
    Zipf { theta: f64 },
}

/////////////////////////////////////////// MetadataQuery //////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum MetadataQuery {
    #[serde(rename = "raw")]
    Raw(serde_json::Value),
}

impl MetadataQuery {
    pub fn into_where_metadata(self, _: &mut Guacamole) -> serde_json::Value {
        match self {
            MetadataQuery::Raw(json) => json,
        }
    }
}

/////////////////////////////////////////// DocumentQuery //////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum DocumentQuery {
    #[serde(rename = "raw")]
    Raw(serde_json::Value),
}

impl DocumentQuery {
    pub fn into_where_document(self, _: &mut Guacamole) -> serde_json::Value {
        match self {
            DocumentQuery::Raw(json) => json,
        }
    }
}

///////////////////////////////////////////// GetQuery /////////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetQuery {
    pub skew: Skew,
    pub limit: Distribution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<DocumentQuery>,
}

//////////////////////////////////////////// QueryQuery ////////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryQuery {
    pub skew: Skew,
    pub limit: Distribution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<DocumentQuery>,
}

//////////////////////////////////////////// KeySelector ///////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
pub enum KeySelector {
    #[serde(rename = "index")]
    Index(usize),
    #[serde(rename = "random")]
    Random(Skew),
}

//////////////////////////////////////////// UpsertQuery ///////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UpsertQuery {
    pub key: KeySelector,
    pub batch_size: usize,
    pub associativity: f64,
}

/////////////////////////////////////////// WorkloadState //////////////////////////////////////////

#[derive(Clone)]
pub struct WorkloadState {
    seq_no: u64,
    guac: Guacamole,
}

///////////////////////////////////////////// Workload /////////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Workload {
    #[serde(rename = "nop")]
    Nop,
    #[serde(rename = "by_name")]
    ByName(String),
    #[serde(rename = "get")]
    Get(GetQuery),
    #[serde(rename = "query")]
    Query(QueryQuery),
    #[serde(rename = "hybrid")]
    Hybrid(Vec<(f64, Workload)>),
    #[serde(rename = "delay")]
    Delay {
        after: chrono::DateTime<chrono::FixedOffset>,
        wrap: Box<Workload>,
    },
    #[serde(rename = "load")]
    Load,
    #[serde(rename = "random")]
    RandomUpsert(KeySelector),
}

impl Workload {
    pub fn description(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    pub fn resolve_by_name(&mut self, workloads: &HashMap<String, Workload>) -> Result<(), Error> {
        match self {
            Workload::Nop => {}
            Workload::ByName(name) => {
                if let Some(workload) = workloads.get(name) {
                    *self = workload.clone();
                } else {
                    return Err(Error::InvalidRequest(format!("workload not found: {name}")));
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

    pub async fn step(
        &self,
        client: &ChromaClient,
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
                data_set
                    .get(client, get.clone(), &mut state.guac)
                    .instrument(tracing::info_span!("get"))
                    .await
            }
            Workload::Query(query) => {
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
                            return Box::pin(workload.step(client, data_set, state)).await;
                        }
                        total -= *p;
                    }
                }
                Err(Box::new(Error::InternalError(
                    "miscalculation of total hybrid probabilities".to_string(),
                )))
            }
            Workload::Delay { after: _, wrap } => {
                Box::pin(wrap.step(client, data_set, state)).await
            }
            Workload::Load => {
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

//////////////////////////////////////////// Throughput ////////////////////////////////////////////

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

////////////////////////////////////////// RunningWorkload /////////////////////////////////////////

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
    pub fn description(&self) -> String {
        format!("{}/{}", self.uuid, self.data_set.name())
    }
}

////////////////////////////////////////// WorkloadSummary /////////////////////////////////////////

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct WorkloadSummary {
    pub uuid: Uuid,
    pub name: String,
    pub workload: serde_json::Value,
    pub data_set: serde_json::Value,
    pub expires: String,
    pub throughput: Throughput,
}

//////////////////////////////////////////// LoadHarness ///////////////////////////////////////////

#[derive(Debug)]
pub struct LoadHarness {
    running: Vec<RunningWorkload>,
}

impl LoadHarness {
    pub fn status(&self) -> Vec<RunningWorkload> {
        self.running.clone()
    }

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

    pub fn stop(&mut self, uuid: Uuid) -> bool {
        let old_sz = self.running.len();
        self.running.retain(|w| w.uuid != uuid);
        let new_sz = self.running.len();
        old_sz > new_sz
    }
}

//////////////////////////////////////////// LoadService ///////////////////////////////////////////

#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct LoadService {
    inhibit: Arc<AtomicBool>,
    harness: Mutex<LoadHarness>,
    running: Mutex<HashMap<Uuid, (Arc<AtomicBool>, tokio::task::JoinHandle<()>)>>,
    data_sets: Vec<Arc<dyn DataSet>>,
    workloads: HashMap<String, Workload>,
}

impl LoadService {
    pub fn new(data_sets: Vec<Arc<dyn DataSet>>, workloads: HashMap<String, Workload>) -> Self {
        LoadService {
            inhibit: Arc::new(AtomicBool::new(false)),
            harness: Mutex::new(LoadHarness { running: vec![] }),
            running: Mutex::new(HashMap::default()),
            data_sets,
            workloads,
        }
    }

    pub fn inhibit(&self) {
        self.inhibit
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn uninhibit(&self) {
        self.inhibit
            .store(false, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_inhibited(&self) -> bool {
        self.inhibit.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn data_sets(&self) -> &[Arc<dyn DataSet>] {
        &self.data_sets
    }

    pub fn workloads(&self) -> &HashMap<String, Workload> {
        &self.workloads
    }

    pub fn status(&self) -> Vec<WorkloadSummary> {
        let running = {
            // SAFETY(rescrv): Mutex poisoning.
            let harness = self.harness.lock().unwrap();
            harness.status()
        };
        running
            .into_iter()
            .map(|r| WorkloadSummary {
                uuid: r.uuid,
                name: r.name,
                workload: serde_json::to_value(r.workload).unwrap(),
                data_set: r.data_set.json(),
                expires: r.expires.to_rfc3339(),
                throughput: r.throughput,
            })
            .collect()
    }

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
        // SAFETY(rescrv):  Mutex poisoning.
        let mut harness = self.harness.lock().unwrap();
        Ok(harness.start(name, workload.clone(), data_set, expires, throughput))
    }

    pub fn stop(&self, uuid: Uuid) -> Result<(), Error> {
        // SAFETY(rescrv): Mutex poisoning.
        let mut harness = self.harness.lock().unwrap();
        if harness.stop(uuid) {
            Ok(())
        } else {
            Err(Error::NotFound("uuid not found".to_string()))
        }
    }

    pub async fn run(&self) -> ! {
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
                    let done = Arc::new(AtomicBool::new(false));
                    let done_p = Arc::clone(&done);
                    let inhibit = Arc::clone(&self.inhibit);
                    let task = tokio::task::spawn(async move {
                        let _enter = root.enter();
                        Self::run_one_workload(done, inhibit, declared)
                            .instrument(tracing::info_span!("run one workload"))
                            .await
                    });
                    entry.insert((done_p, task));
                }
            }
        }
    }

    async fn run_one_workload(
        done: Arc<AtomicBool>,
        inhibit: Arc<AtomicBool>,
        spec: RunningWorkload,
    ) {
        let client = Arc::new(client());
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
            } else if !spec.workload.is_active() {
                tracing::debug!("workload inactive");
            } else {
                let workload = spec.workload.clone();
                let client = Arc::clone(&client);
                let data_set = Arc::clone(&spec.data_set);
                let guac = Guacamole::new(any(&mut guac));
                let mut state = WorkloadState { seq_no, guac };
                let fut = async move {
                    workload
                        .step(&client, &*data_set, &mut state)
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
                    output.push_str(&format!("Workload: {}\n", running.workload));
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
    state.load.inhibit();
    Ok("inhibited\n".to_string())
}

async fn uninhibit(State(state): State<AppState>) -> Result<String, Error> {
    state.load.uninhibit();
    Ok("uninhibited\n".to_string())
}

pub async fn entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => config::RootConfig::load_from_path(&config_path),
        Err(_) => config::RootConfig::load(),
    };

    let config = config.load_service;

    opentelemetry_config::init_otel_tracing(&config.service_name, &config.otel_endpoint);
    let load = Arc::new(LoadService::default());
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
                    some_other_field = tracing::field::Empty,
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
}
