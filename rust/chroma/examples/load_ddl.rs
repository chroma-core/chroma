//! DDL Load Generator Example
//!
//! A paced load generator for Chroma DDL operations.  Each cycle runs the selected `--ops`
//! sequence in order and records a separate latency histogram for each operation.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example load_ddl -- --duration 60 --pace-qps 1
//! cargo run --example load_ddl -- --ops list_collections,get_collections,list_databases
//! cargo run --example load_ddl -- --endpoint http://localhost:8000 --ops all
//! ```
//!
//! # Environment Variables
//!
//! For cloud endpoints, `CHROMA_API_KEY` is required. `CHROMA_TENANT` and `CHROMA_DATABASE`
//! are optional and follow the same resolution rules as the other load generator examples.

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use biometrics::{Counter, Sensor};
use chroma::bench::{create_client, spawn_pacing_task};
use chroma::{ChromaCollection, ChromaHttpClient};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time;

const DEFAULT_ENDPOINT: &str = "https://api.devchroma.com:443";
const DEFAULT_DURATION_SECS: u64 = 60;
const DEFAULT_TASKS: usize = 1;
const DEFAULT_PACE_QPS: u64 = 1;
const DEFAULT_MAX_OUTSTANDING_OPS: usize = 4;
const DEFAULT_LIST_LIMIT: usize = 100;
const DEFAULT_COLLECTION_PREFIX: &str = "load_ddl";
const DEFAULT_OPS: &str = "list_collections,get_collection,create_collection,list_databases";
const METRICS_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Parser, Debug)]
#[command(name = "load_ddl")]
#[command(about = "Generate paced Chroma DDL load")]
struct Args {
    /// Chroma endpoint to target.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    endpoint: String,

    /// Duration to run the load generator in seconds.
    #[arg(short, long, default_value_t = DEFAULT_DURATION_SECS)]
    duration: u64,

    /// Number of concurrent worker tasks.
    #[arg(short, long, default_value_t = DEFAULT_TASKS)]
    tasks: usize,

    /// Target cycles per second. Each cycle runs the selected `--ops` sequence once.
    #[arg(long, default_value_t = DEFAULT_PACE_QPS)]
    pace_qps: u64,

    /// Maximum number of outstanding operation cycles.
    #[arg(long, default_value_t = DEFAULT_MAX_OUTSTANDING_OPS)]
    max_outstanding_ops: usize,

    /// Maximum number of collections to fetch from each list_collections call.
    #[arg(long, default_value_t = DEFAULT_LIST_LIMIT)]
    list_limit: usize,

    /// Prefix for collections created by create_collection.
    #[arg(long, default_value = DEFAULT_COLLECTION_PREFIX)]
    collection_prefix: String,

    /// Comma-separated operations to run. Supported values: all, list_collections,
    /// get_collection/get_collections, create_collection, list_databases.
    #[arg(long, default_value = DEFAULT_OPS)]
    ops: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DdlOp {
    ListCollections,
    GetCollection,
    CreateCollection,
    ListDatabases,
}

impl DdlOp {
    const ALL: [DdlOp; 4] = [
        DdlOp::ListCollections,
        DdlOp::GetCollection,
        DdlOp::CreateCollection,
        DdlOp::ListDatabases,
    ];

    fn name(self) -> &'static str {
        match self {
            DdlOp::ListCollections => "list_collections",
            DdlOp::GetCollection => "get_collection",
            DdlOp::CreateCollection => "create_collection",
            DdlOp::ListDatabases => "list_databases",
        }
    }
}

fn push_unique(ops: &mut Vec<DdlOp>, op: DdlOp) {
    if !ops.contains(&op) {
        ops.push(op);
    }
}

fn parse_ops(value: &str) -> Result<Vec<DdlOp>, String> {
    let mut ops = Vec::new();

    for raw in value.split(',') {
        let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
        match normalized.as_str() {
            "" => return Err("empty operation in --ops".to_string()),
            "all" | "*" => {
                for op in DdlOp::ALL {
                    push_unique(&mut ops, op);
                }
            }
            "etc" => {
                for op in DdlOp::ALL {
                    push_unique(&mut ops, op);
                }
            }
            "list" | "list_collection" | "list_collections" => {
                push_unique(&mut ops, DdlOp::ListCollections);
            }
            "get"
            | "get_collection"
            | "get_collections"
            | "get_random_collection"
            | "get_random_collections" => {
                push_unique(&mut ops, DdlOp::GetCollection);
            }
            "create" | "create_collection" | "create_collections" => {
                push_unique(&mut ops, DdlOp::CreateCollection);
            }
            "databases" | "list_database" | "list_databases" => {
                push_unique(&mut ops, DdlOp::ListDatabases);
            }
            _ => {
                return Err(format!(
                    "unknown operation {raw:?}; supported operations are all, list_collections, \
                     get_collection/get_collections, create_collection, list_databases"
                ));
            }
        }
    }

    if ops.is_empty() {
        Err("--ops must include at least one operation".to_string())
    } else {
        Ok(ops)
    }
}

fn format_ops(ops: &[DdlOp]) -> String {
    ops.iter().map(|op| op.name()).collect::<Vec<_>>().join(",")
}

struct RunConfig {
    duration: Duration,
    ops: Arc<[DdlOp]>,
    list_limit: usize,
    collection_prefix: String,
    run_id: u64,
}

#[derive(Debug, Default)]
struct WorkerSummary {
    cycles: u64,
}

struct CollectionNamePool {
    names: Mutex<Vec<String>>,
}

impl CollectionNamePool {
    fn new() -> Self {
        Self {
            names: Mutex::new(Vec::new()),
        }
    }

    async fn replace_if_non_empty(&self, names: Vec<String>) {
        if names.is_empty() {
            return;
        }
        *self.names.lock().await = names;
    }

    async fn push(&self, name: String) {
        let mut names = self.names.lock().await;
        if !names.contains(&name) {
            names.push(name);
        }
    }

    async fn random_name(&self, rng: &mut StdRng) -> Option<String> {
        let names = self.names.lock().await;
        if names.is_empty() {
            None
        } else {
            Some(names[rng.gen_range(0..names.len())].clone())
        }
    }

    async fn len(&self) -> usize {
        self.names.lock().await.len()
    }
}

static LOAD_DDL_LIST_COLLECTIONS_ATTEMPTS: Counter =
    Counter::new("load_ddl.list_collections_attempts");
static LOAD_DDL_LIST_COLLECTIONS_SUCCESS: Counter =
    Counter::new("load_ddl.list_collections_successes");
static LOAD_DDL_LIST_COLLECTIONS_FAILURES: Counter =
    Counter::new("load_ddl.list_collections_failures");
static LOAD_DDL_LIST_COLLECTIONS_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_DDL_LIST_COLLECTIONS_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_ddl.list_collections_latency_ms",
    &LOAD_DDL_LIST_COLLECTIONS_LATENCY,
);

static LOAD_DDL_GET_COLLECTION_ATTEMPTS: Counter = Counter::new("load_ddl.get_collection_attempts");
static LOAD_DDL_GET_COLLECTION_SUCCESS: Counter = Counter::new("load_ddl.get_collection_successes");
static LOAD_DDL_GET_COLLECTION_FAILURES: Counter = Counter::new("load_ddl.get_collection_failures");
static LOAD_DDL_GET_COLLECTION_SKIPPED: Counter = Counter::new("load_ddl.get_collection_skipped");
static LOAD_DDL_GET_COLLECTION_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_DDL_GET_COLLECTION_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_ddl.get_collection_latency_ms",
    &LOAD_DDL_GET_COLLECTION_LATENCY,
);

static LOAD_DDL_CREATE_COLLECTION_ATTEMPTS: Counter =
    Counter::new("load_ddl.create_collection_attempts");
static LOAD_DDL_CREATE_COLLECTION_SUCCESS: Counter =
    Counter::new("load_ddl.create_collection_successes");
static LOAD_DDL_CREATE_COLLECTION_FAILURES: Counter =
    Counter::new("load_ddl.create_collection_failures");
static LOAD_DDL_CREATE_COLLECTION_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_DDL_CREATE_COLLECTION_LATENCY_SENSOR: biometrics::Histogram =
    biometrics::Histogram::new(
        "load_ddl.create_collection_latency_ms",
        &LOAD_DDL_CREATE_COLLECTION_LATENCY,
    );

static LOAD_DDL_LIST_DATABASES_ATTEMPTS: Counter = Counter::new("load_ddl.list_databases_attempts");
static LOAD_DDL_LIST_DATABASES_SUCCESS: Counter = Counter::new("load_ddl.list_databases_successes");
static LOAD_DDL_LIST_DATABASES_FAILURES: Counter = Counter::new("load_ddl.list_databases_failures");
static LOAD_DDL_LIST_DATABASES_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_DDL_LIST_DATABASES_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_ddl.list_databases_latency_ms",
    &LOAD_DDL_LIST_DATABASES_LATENCY,
);

#[derive(Clone, Copy)]
struct OpMetrics {
    attempts: &'static Counter,
    successes: &'static Counter,
    failures: &'static Counter,
    latency: &'static biometrics::Histogram,
}

fn metrics_for(op: DdlOp) -> OpMetrics {
    match op {
        DdlOp::ListCollections => OpMetrics {
            attempts: &LOAD_DDL_LIST_COLLECTIONS_ATTEMPTS,
            successes: &LOAD_DDL_LIST_COLLECTIONS_SUCCESS,
            failures: &LOAD_DDL_LIST_COLLECTIONS_FAILURES,
            latency: &LOAD_DDL_LIST_COLLECTIONS_LATENCY_SENSOR,
        },
        DdlOp::GetCollection => OpMetrics {
            attempts: &LOAD_DDL_GET_COLLECTION_ATTEMPTS,
            successes: &LOAD_DDL_GET_COLLECTION_SUCCESS,
            failures: &LOAD_DDL_GET_COLLECTION_FAILURES,
            latency: &LOAD_DDL_GET_COLLECTION_LATENCY_SENSOR,
        },
        DdlOp::CreateCollection => OpMetrics {
            attempts: &LOAD_DDL_CREATE_COLLECTION_ATTEMPTS,
            successes: &LOAD_DDL_CREATE_COLLECTION_SUCCESS,
            failures: &LOAD_DDL_CREATE_COLLECTION_FAILURES,
            latency: &LOAD_DDL_CREATE_COLLECTION_LATENCY_SENSOR,
        },
        DdlOp::ListDatabases => OpMetrics {
            attempts: &LOAD_DDL_LIST_DATABASES_ATTEMPTS,
            successes: &LOAD_DDL_LIST_DATABASES_SUCCESS,
            failures: &LOAD_DDL_LIST_DATABASES_FAILURES,
            latency: &LOAD_DDL_LIST_DATABASES_LATENCY_SENSOR,
        },
    }
}

fn latency_histogram_for(op: DdlOp) -> &'static sig_fig_histogram::LockFreeHistogram<450> {
    match op {
        DdlOp::ListCollections => &LOAD_DDL_LIST_COLLECTIONS_LATENCY,
        DdlOp::GetCollection => &LOAD_DDL_GET_COLLECTION_LATENCY,
        DdlOp::CreateCollection => &LOAD_DDL_CREATE_COLLECTION_LATENCY,
        DdlOp::ListDatabases => &LOAD_DDL_LIST_DATABASES_LATENCY,
    }
}

struct DdlMetricsEmitter {
    handle: tokio::task::JoinHandle<()>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl DdlMetricsEmitter {
    async fn finish(mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let _ = self.handle.await;
    }
}

fn register_op_metrics(collector: &biometrics::Collector, op: DdlOp) {
    let metrics = metrics_for(op);
    collector.register_counter(metrics.attempts);
    collector.register_counter(metrics.successes);
    collector.register_counter(metrics.failures);
    collector.register_histogram(metrics.latency);
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock moved backward")
        .as_millis()
        .try_into()
        .expect("timestamp exceeds supported range")
}

fn start_metrics_emitter() -> DdlMetricsEmitter {
    let collector = biometrics::Collector::new();
    for op in DdlOp::ALL {
        register_op_metrics(&collector, op);
    }
    collector.register_counter(&LOAD_DDL_GET_COLLECTION_SKIPPED);

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let mut emitter = biometrics_prometheus::Emitter::new(biometrics_prometheus::Options {
            segment_size: 64 * 1024 * 1024,
            flush_interval: METRICS_FLUSH_INTERVAL,
            prefix: utf8path::Path::new("load_ddl."),
        });
        let mut interval = time::interval(METRICS_FLUSH_INTERVAL);
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = collector.emit(&mut emitter, now_millis());
                }
                _ = &mut stop_rx => {
                    let _ = collector.emit(&mut emitter, now_millis());
                    break;
                }
            }
        }
    });

    DdlMetricsEmitter {
        handle,
        stop_tx: Some(stop_tx),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let ops = parse_ops(&args.ops).map_err(std::io::Error::other)?;
    validate_args(&args)?;

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock moved backward")
        .as_nanos() as u64;
    let client = create_client(&args.endpoint)?;
    let pool = Arc::new(CollectionNamePool::new());
    seed_collection_pool(&client, Arc::clone(&pool), &args, &ops, run_id).await?;

    println!("=== Chroma DDL Load Generator ===");
    println!("Endpoint: {}", args.endpoint);
    println!("Duration: {} seconds", args.duration);
    println!("Tasks: {}", args.tasks);
    println!("Pace: {} cycles/s", args.pace_qps);
    println!("Max outstanding cycles: {}", args.max_outstanding_ops);
    println!("List limit: {}", args.list_limit);
    println!("Ops: {}", format_ops(&ops));
    println!("Collection prefix: {}", args.collection_prefix);
    println!(
        "Initial get_collection pool: {} collections",
        pool.len().await
    );
    println!();

    let config = Arc::new(RunConfig {
        duration: Duration::from_secs(args.duration),
        ops: ops.into(),
        list_limit: args.list_limit,
        collection_prefix: args.collection_prefix.clone(),
        run_id,
    });
    let metrics_emitter = start_metrics_emitter();
    let summaries = run_ddl_load(
        client,
        Arc::clone(&pool),
        config,
        args.tasks,
        args.pace_qps,
        args.max_outstanding_ops,
    )
    .await;
    metrics_emitter.finish().await;
    let summaries = summaries?;

    print_summary(&summaries, pool.len().await);
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    if args.duration == 0 {
        return Err(std::io::Error::other("--duration must be greater than 0").into());
    }
    if args.tasks == 0 {
        return Err(std::io::Error::other("--tasks must be greater than 0").into());
    }
    if args.pace_qps == 0 {
        return Err(std::io::Error::other("--pace-qps must be greater than 0").into());
    }
    if args.max_outstanding_ops == 0 {
        return Err(std::io::Error::other("--max-outstanding-ops must be greater than 0").into());
    }
    if args.list_limit == 0 {
        return Err(std::io::Error::other("--list-limit must be greater than 0").into());
    }
    if args.collection_prefix.trim().is_empty() {
        return Err(std::io::Error::other("--collection-prefix must not be empty").into());
    }
    Ok(())
}

async fn seed_collection_pool(
    client: &ChromaHttpClient,
    pool: Arc<CollectionNamePool>,
    args: &Args,
    ops: &[DdlOp],
    run_id: u64,
) -> Result<(), Box<dyn Error>> {
    if !ops.contains(&DdlOp::GetCollection) {
        return Ok(());
    }

    let collections = client.list_collections(args.list_limit, None).await?;
    let names = collection_names(&collections);
    if names.is_empty() {
        let name = bootstrap_collection_name(&args.collection_prefix, run_id);
        let collection = client.get_or_create_collection(&name, None, None).await?;
        println!(
            "Bootstrapped collection {} for random get_collection load",
            collection.name()
        );
        pool.push(collection.name().to_string()).await;
    } else {
        pool.replace_if_non_empty(names).await;
    }

    Ok(())
}

async fn run_ddl_load(
    client: ChromaHttpClient,
    pool: Arc<CollectionNamePool>,
    config: Arc<RunConfig>,
    tasks: usize,
    pace_qps: u64,
    max_outstanding_ops: usize,
) -> Result<Vec<WorkerSummary>, Box<dyn Error>> {
    let start_time = Instant::now();
    let (ticket_tx, ticket_rx) = mpsc::channel::<()>(1024);
    let pacing_rx = Arc::new(Mutex::new(ticket_rx));
    let pacing_handle = spawn_pacing_task(start_time, config.duration, pace_qps, ticket_tx);
    let semaphore = Arc::new(Semaphore::new(max_outstanding_ops));
    let mut handles = Vec::with_capacity(tasks);

    for task_id in 0..tasks {
        handles.push(tokio::spawn(run_ddl_worker(
            client.clone(),
            Arc::clone(&pool),
            Arc::clone(&config),
            start_time,
            Arc::clone(&pacing_rx),
            Arc::clone(&semaphore),
            task_id,
        )));
    }

    let mut summaries = Vec::with_capacity(tasks);
    for handle in handles {
        match handle.await {
            Ok(summary) => summaries.push(summary),
            Err(err) => eprintln!("DDL worker panicked: {err}"),
        }
    }
    pacing_handle.abort();

    Ok(summaries)
}

#[allow(clippy::too_many_arguments)]
async fn run_ddl_worker(
    client: ChromaHttpClient,
    pool: Arc<CollectionNamePool>,
    config: Arc<RunConfig>,
    start_time: Instant,
    pacing_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    semaphore: Arc<Semaphore>,
    task_id: usize,
) -> WorkerSummary {
    let mut summary = WorkerSummary::default();
    let mut rng = StdRng::seed_from_u64(config.run_id.wrapping_add(task_id as u64 * 1000));
    let mut create_sequence = 0u64;

    while start_time.elapsed() < config.duration {
        let remaining = config.duration.saturating_sub(start_time.elapsed());
        if remaining.is_zero() {
            break;
        }

        let ticket = time::timeout(remaining, async {
            let mut rx = pacing_rx.lock().await;
            rx.recv().await
        })
        .await;
        match ticket {
            Ok(Some(())) => {}
            _ => break,
        }

        let remaining = config.duration.saturating_sub(start_time.elapsed());
        if remaining.is_zero() {
            break;
        }
        let permit = match time::timeout(remaining, Arc::clone(&semaphore).acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            _ => break,
        };

        for op in config.ops.iter().copied() {
            if let Err(err) = run_ddl_op(
                op,
                &client,
                Arc::clone(&pool),
                &config,
                task_id,
                &mut create_sequence,
                &mut rng,
            )
            .await
            {
                eprintln!("[task {task_id}] {} error: {err}", op.name());
            }
        }

        drop(permit);
        summary.cycles += 1;
    }

    summary
}

#[allow(clippy::too_many_arguments)]
async fn run_ddl_op(
    op: DdlOp,
    client: &ChromaHttpClient,
    pool: Arc<CollectionNamePool>,
    config: &RunConfig,
    task_id: usize,
    create_sequence: &mut u64,
    rng: &mut StdRng,
) -> Result<(), chroma::client::ChromaHttpClientError> {
    match op {
        DdlOp::ListCollections => {
            let collections =
                record_operation(op, client.list_collections(config.list_limit, None)).await?;
            pool.replace_if_non_empty(collection_names(&collections))
                .await;
            Ok(())
        }
        DdlOp::GetCollection => {
            let Some(name) = pool.random_name(rng).await else {
                LOAD_DDL_GET_COLLECTION_SKIPPED.click();
                return Ok(());
            };
            let _collection = record_operation(op, client.get_collection(&name)).await?;
            Ok(())
        }
        DdlOp::CreateCollection => {
            *create_sequence += 1;
            let name = collection_name(
                &config.collection_prefix,
                config.run_id,
                task_id,
                *create_sequence,
            );
            let collection =
                record_operation(op, client.create_collection(name.as_str(), None, None)).await?;
            pool.push(collection.name().to_string()).await;
            Ok(())
        }
        DdlOp::ListDatabases => {
            let _databases = record_operation(op, client.list_databases()).await?;
            Ok(())
        }
    }
}

async fn record_operation<T, Fut>(
    op: DdlOp,
    future: Fut,
) -> Result<T, chroma::client::ChromaHttpClientError>
where
    Fut: Future<Output = Result<T, chroma::client::ChromaHttpClientError>>,
{
    let metrics = metrics_for(op);
    metrics.attempts.click();

    let start = Instant::now();
    let result = future.await;
    metrics
        .latency
        .observe(start.elapsed().as_secs_f64() * 1000.);
    match &result {
        Ok(_) => metrics.successes.click(),
        Err(_) => metrics.failures.click(),
    }

    result
}

fn collection_names(collections: &[ChromaCollection]) -> Vec<String> {
    collections
        .iter()
        .map(|collection| collection.name().to_string())
        .collect()
}

fn collection_name(prefix: &str, run_id: u64, task_id: usize, sequence: u64) -> String {
    format!("{prefix}_{run_id:016x}_{task_id:04}_{sequence:08}")
}

fn bootstrap_collection_name(prefix: &str, run_id: u64) -> String {
    format!("{prefix}_{run_id:016x}_bootstrap")
}

struct LatencySummary {
    count: u64,
    min: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
    avg: f64,
}

fn summarize_latency(
    histogram: &sig_fig_histogram::LockFreeHistogram<450>,
) -> Option<LatencySummary> {
    let histogram = histogram.to_histogram();
    let buckets: Vec<(f64, u64)> = histogram.iter().collect();
    let count: u64 = buckets.iter().map(|(_, count)| *count).sum();
    if count == 0 {
        return None;
    }

    let min = buckets
        .iter()
        .find(|(_, bucket_count)| *bucket_count > 0)
        .map(|(bucket, _)| *bucket)
        .unwrap_or(0.0);
    let max = buckets
        .iter()
        .rev()
        .find(|(_, bucket_count)| *bucket_count > 0)
        .map(|(bucket, _)| *bucket)
        .unwrap_or(0.0);
    let sum: f64 = buckets
        .iter()
        .map(|(bucket, bucket_count)| *bucket * *bucket_count as f64)
        .sum();

    Some(LatencySummary {
        count,
        min,
        p50: percentile(&buckets, count, 0.50),
        p90: percentile(&buckets, count, 0.90),
        p99: percentile(&buckets, count, 0.99),
        max,
        avg: sum / count as f64,
    })
}

fn percentile(buckets: &[(f64, u64)], count: u64, percentile: f64) -> f64 {
    let target = ((count as f64 * percentile).ceil() as u64).max(1);
    let mut cumulative = 0u64;
    for (bucket, bucket_count) in buckets {
        cumulative += *bucket_count;
        if cumulative >= target {
            return *bucket;
        }
    }
    buckets
        .iter()
        .rev()
        .find(|(_, bucket_count)| *bucket_count > 0)
        .map(|(bucket, _)| *bucket)
        .unwrap_or(0.0)
}

fn print_summary(summaries: &[WorkerSummary], pool_size: usize) {
    let total_cycles: u64 = summaries.iter().map(|summary| summary.cycles).sum();

    println!("\n=== DDL Load Complete ===");
    println!("Total cycles: {}", total_cycles);
    println!("Random get_collection pool: {} collections", pool_size);
    println!(
        "Skipped get_collection operations: {}",
        LOAD_DDL_GET_COLLECTION_SKIPPED.read()
    );
    println!();

    for op in DdlOp::ALL {
        let metrics = metrics_for(op);
        println!("{}", op.name());
        println!(
            "  attempts/success/failures: {}/{}/{}",
            metrics.attempts.read(),
            metrics.successes.read(),
            metrics.failures.read()
        );
        match summarize_latency(latency_histogram_for(op)) {
            Some(summary) => {
                println!(
                    "  latency_ms count={} min={:.3} avg={:.3} p50={:.3} p90={:.3} p99={:.3} max={:.3}",
                    summary.count,
                    summary.min,
                    summary.avg,
                    summary.p50,
                    summary.p90,
                    summary.p99,
                    summary.max
                );
            }
            None => println!("  latency_ms count=0"),
        }
    }
}
