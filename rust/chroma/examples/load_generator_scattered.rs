//! Load Generator Example
//!
//! A load generator for Chroma that creates concurrent upsert and search operations across
//! multiple collections on two different Chroma endpoints.
//!
//! # Features
//!
//! - Dual endpoint support (api.trychroma.com and europe-west1.gcp.devchroma.com)
//! - Configurable number of collections, read/write tasks, batch size, and duration
//! - Uniform collection selection by default, with optional Zipf skew via `--zipf`
//! - Gaussian Mixture Model (GMM) for realistic embedding generation
//! - Concurrent search traffic with independent pace and outstanding-op controls
//!
//! # Usage
//!
//! ```bash
//! cargo run --example load_generator_scattered -- --collections 10 --duration 600 --tasks 4 --batch-size 100
//! cargo run --example load_generator_scattered -- --collections 10 --zipf 0.8
//! cargo run --example load_generator_scattered -- --collections 10 --read-pace-qps 200 --read-tasks 8
//! ```
//!
//! # Environment Variables
//!
//! The following environment variables must be set:
//! - `CHROMA_API_KEY` - API key for Chroma Cloud authentication
//! - `CHROMA_TENANT` - Tenant ID (optional, will be auto-resolved)
//! - `CHROMA_DATABASE` - Database name (optional, will be auto-resolved)

use std::error::Error;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use biometrics::{Counter, Sensor};
use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt};
use guacamole::{FromGuacamole, Guacamole, Zipf};
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;

use chroma::bench::{
    boxed_collection_selector, collection_cache_file_path, prepare_dual_collections,
    print_load_generator_header, run_load_generator, run_load_generator_dry_run, spawn_pacing_task,
    start_load_metrics_emitter, CollectionSelector, CommonLoadArgs, DualLoadEndpoints,
    GaussianMixtureModel, LoadMetricRefs,
};
use chroma::types::{Key, QueryVector, RankExpr, SearchPayload};
use chroma::ChromaCollection;

/// Load generator for Chroma that creates concurrent upsert operations.
#[derive(Parser, Debug)]
#[command(name = "load_generator")]
#[command(about = "Generate load against Chroma endpoints")]
struct Args {
    /// Number of collections to create and write to.
    #[arg(short, long, default_value_t = 10)]
    collections: usize,

    /// Duration to run the load generator in seconds.
    #[arg(short, long, default_value_t = 600)]
    duration: u64,

    /// Number of concurrent tasks.
    #[arg(short, long, default_value_t = 4)]
    tasks: usize,

    /// Batch size for upsert operations.
    #[arg(short, long, default_value_t = 100)]
    batch_size: usize,

    /// Target request pace in queries per second.
    #[arg(long, default_value_t = 100)]
    pace_qps: u64,

    /// Maximum number of outstanding operations per collection.
    #[arg(long, default_value_t = 10)]
    max_outstanding_ops: usize,

    /// Maximum total number of outstanding operations per backend.
    /// Defaults to the per-collection limit when omitted.
    #[arg(long)]
    global_max_outstanding_ops: Option<usize>,

    /// Number of concurrent read tasks. Defaults to `--tasks`.
    #[arg(long)]
    read_tasks: Option<usize>,

    /// Target read request pace in queries per second. Set to 0 to disable reads.
    /// Defaults to `--pace-qps`.
    #[arg(long)]
    read_pace_qps: Option<u64>,

    /// Maximum number of outstanding read operations per collection.
    /// Defaults to `--max-outstanding-ops`.
    #[arg(long)]
    read_max_outstanding_ops: Option<usize>,

    /// Maximum total number of outstanding read operations per backend.
    /// Defaults to the per-collection read limit when omitted.
    #[arg(long)]
    read_global_max_outstanding_ops: Option<usize>,

    /// Number of results requested by each read query.
    #[arg(long, default_value_t = 10)]
    read_limit: u32,

    /// Zipf skew for collection selection over `(0, 1)`. Omit for uniform selection.
    #[arg(long, value_name = "SKEW", value_parser = parse_zipf_param)]
    zipf: Option<f64>,

    /// Target local backends on ports 8000 and 8001 instead of cloud endpoints.
    #[arg(long, default_value_t = false)]
    local: bool,

    /// Print each selected write collection and a final rank-ordered histogram without
    /// contacting any backend.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn parse_zipf_param(value: &str) -> Result<f64, String> {
    let skew: f64 = value
        .parse()
        .map_err(|err| format!("invalid Zipf skew {value:?}: {err}"))?;
    if (0.0..1.0).contains(&skew) {
        Ok(skew)
    } else {
        Err("Zipf skew must be between 0 and 1 (exclusive)".to_string())
    }
}

fn build_collection_selector(task_seed: u64, zipf: Option<Zipf>) -> Box<dyn CollectionSelector> {
    if let Some(zipf) = zipf {
        let mut collection_rng = Guacamole::new(task_seed);
        boxed_collection_selector(move |num_collections, _rng| {
            let idx = zipf.next(&mut collection_rng) as usize;
            idx.saturating_sub(1) % num_collections
        })
    } else {
        let mut collection_rng = Guacamole::new(task_seed);
        boxed_collection_selector(move |num_collections, _rng| {
            usize::from_guacamole(&mut (), &mut collection_rng) % num_collections
        })
    }
}

/// Generates a deterministic collection name from the index.
fn collection_name(index: usize) -> String {
    format!("loadgen_collection2_{:06}", index)
}

/// Returns the path to the collection cache file.
fn cache_file_path(num_collections: usize) -> String {
    collection_cache_file_path("loadgen_collections2", num_collections)
}

#[derive(Debug, Clone, Copy)]
struct ReadLoadArgs {
    duration_secs: u64,
    tasks: usize,
    pace_qps: u64,
    max_outstanding_ops: usize,
    global_max_outstanding_ops: usize,
    limit: u32,
}

impl ReadLoadArgs {
    fn enabled(self) -> bool {
        self.tasks > 0 && self.pace_qps > 0
    }
}

static LOAD_SCATTERED_UPSERT_ATTEMPTS: Counter =
    Counter::new("load_generator.scattered.upsert_attempts");
static LOAD_SCATTERED_UPSERT_SUCCESS: Counter =
    Counter::new("load_generator.scattered.upsert_successes");
static LOAD_SCATTERED_UPSERT_FAILURES: Counter =
    Counter::new("load_generator.scattered.upsert_failures");
static LOAD_SCATTERED_UPSERT_DROPPED: Counter =
    Counter::new("load_generator.scattered.upsert_dropped");

static LOAD_SCATTERED_UPSERT_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_SCATTERED_UPSERT_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.scattered.upsert_latency_ms",
    &LOAD_SCATTERED_UPSERT_LATENCY,
);

static LOAD_SCATTERED_SUCCESS_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.scattered.upsert_success_latency_ms",
    &LOAD_SCATTERED_SUCCESS_LATENCY,
);

static LOAD_SCATTERED_DDL_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_SCATTERED_DDL_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.scattered.collection_ddl_latency_ms",
    &LOAD_SCATTERED_DDL_LATENCY,
);

static LOAD_SCATTERED_SEARCH_ATTEMPTS: Counter =
    Counter::new("load_generator.scattered.search_attempts");
static LOAD_SCATTERED_SEARCH_SUCCESS: Counter =
    Counter::new("load_generator.scattered.search_successes");
static LOAD_SCATTERED_SEARCH_FAILURES: Counter =
    Counter::new("load_generator.scattered.search_failures");
static LOAD_SCATTERED_SEARCH_DROPPED: Counter =
    Counter::new("load_generator.scattered.search_dropped");

static LOAD_SCATTERED_SEARCH_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_SCATTERED_SEARCH_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.scattered.search_latency_ms",
    &LOAD_SCATTERED_SEARCH_LATENCY,
);

static LOAD_SCATTERED_SEARCH_SUCCESS_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_SCATTERED_SEARCH_SUCCESS_LATENCY_SENSOR: biometrics::Histogram =
    biometrics::Histogram::new(
        "load_generator.scattered.search_success_latency_ms",
        &LOAD_SCATTERED_SEARCH_SUCCESS_LATENCY,
    );

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let zipf = args.zipf;
    if args.read_limit == 0 {
        return Err(std::io::Error::other("--read-limit must be greater than 0").into());
    }
    let common_args = CommonLoadArgs {
        duration_secs: args.duration,
        tasks: args.tasks,
        batch_size: args.batch_size,
        pace_qps: args.pace_qps,
        max_outstanding_ops: args.max_outstanding_ops,
        global_max_outstanding_ops: args
            .global_max_outstanding_ops
            .unwrap_or(args.max_outstanding_ops),
    };
    let read_args = ReadLoadArgs {
        duration_secs: args.duration,
        tasks: args.read_tasks.unwrap_or(args.tasks),
        pace_qps: args.read_pace_qps.unwrap_or(args.pace_qps),
        max_outstanding_ops: args
            .read_max_outstanding_ops
            .unwrap_or(args.max_outstanding_ops),
        global_max_outstanding_ops: args.read_global_max_outstanding_ops.unwrap_or_else(|| {
            args.read_max_outstanding_ops
                .unwrap_or(args.max_outstanding_ops)
        }),
        limit: args.read_limit,
    };

    print_load_generator_header(&format!("Collections: {}", args.collections), &common_args);
    match zipf {
        Some(skew) => println!("Collection selection: Zipf skew {skew}"),
        None => println!("Collection selection: uniform"),
    }
    if args.dry_run {
        println!("Concurrent reads: dry-run skips reads");
    } else if read_args.enabled() {
        println!(
            "Concurrent reads: {} tasks @ {} qps, limit {}, max outstanding {}/{}",
            read_args.tasks,
            read_args.pace_qps,
            read_args.limit,
            read_args.max_outstanding_ops,
            read_args.global_max_outstanding_ops
        );
    } else {
        println!("Concurrent reads: disabled");
    }
    println!();

    if args.dry_run {
        println!(
            "Dry run: skipping backend warmup and reads; printing the Zipf-rank summary on termination.\n"
        );
        let collection_names: Vec<String> = (0..args.collections).map(collection_name).collect();
        let zipf = zipf.map(|skew| Zipf::from_param(collection_names.len() as u64, skew));
        run_load_generator_dry_run(
            &common_args,
            collection_names,
            |task_id, _collection_count| {
                build_collection_selector(task_id as u64 * 1000, zipf.clone())
            },
            |task_id, _collection_count| {
                build_collection_selector((task_id as u64 + 500) * 1000, zipf.clone())
            },
        )
        .await?;
        return Ok(());
    }

    println!(
        "Creating/getting {} collections on both endpoints...",
        args.collections
    );

    let metrics = LoadMetricRefs {
        upsert_attempts: &LOAD_SCATTERED_UPSERT_ATTEMPTS,
        upsert_success: &LOAD_SCATTERED_UPSERT_SUCCESS,
        upsert_failures: &LOAD_SCATTERED_UPSERT_FAILURES,
        upsert_dropped: &LOAD_SCATTERED_UPSERT_DROPPED,
        ddl_latency: &LOAD_SCATTERED_DDL_LATENCY_SENSOR,
        upsert_latency: &LOAD_SCATTERED_UPSERT_LATENCY_SENSOR,
        success_latency: &LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR,
        search_attempts: Some(&LOAD_SCATTERED_SEARCH_ATTEMPTS),
        search_success: Some(&LOAD_SCATTERED_SEARCH_SUCCESS),
        search_failures: Some(&LOAD_SCATTERED_SEARCH_FAILURES),
        search_dropped: Some(&LOAD_SCATTERED_SEARCH_DROPPED),
        search_latency: Some(&LOAD_SCATTERED_SEARCH_LATENCY_SENSOR),
        search_success_latency: Some(&LOAD_SCATTERED_SEARCH_SUCCESS_LATENCY_SENSOR),
    };
    let metrics_emitter = start_load_metrics_emitter("load_generator_scattered.", &metrics);

    let result = async {
        let collection_names: Vec<String> = (0..args.collections).map(collection_name).collect();
        let cache_path = cache_file_path(args.collections);
        let (collections_us, collections_eu) = prepare_dual_collections(
            if args.local {
                DualLoadEndpoints::LOCAL
            } else {
                DualLoadEndpoints::CLOUD
            },
            &cache_path,
            &collection_names,
            args.max_outstanding_ops,
            ("US", "EU"),
            &LOAD_SCATTERED_DDL_LATENCY_SENSOR,
        )
        .await?;
        println!(
            "  Ready {} US collections and {} EU collections",
            collections_us.len(),
            collections_eu.len()
        );
        let zipf = zipf.map(|skew| Zipf::from_param(collections_us.len() as u64, skew));

        println!("Collections ready. Starting load generation...\n");

        let write_workload = run_load_generator(
            &common_args,
            metrics,
            collections_us.clone(),
            collections_eu.clone(),
            |task_id, _collection_count| {
                build_collection_selector(task_id as u64 * 1000, zipf.clone())
            },
            |task_id, _collection_count| {
                build_collection_selector((task_id as u64 + 500) * 1000, zipf.clone())
            },
        );

        if read_args.enabled() {
            let read_workload = run_search_load_generator(
                read_args,
                collections_us,
                collections_eu,
                |task_id, _collection_count| {
                    build_collection_selector((task_id as u64 + 1_000) * 1000, zipf.clone())
                },
                |task_id, _collection_count| {
                    build_collection_selector((task_id as u64 + 1_500) * 1000, zipf.clone())
                },
            );
            tokio::try_join!(write_workload, read_workload)?;
            Ok(())
        } else {
            write_workload.await
        }
    }
    .await;
    metrics_emitter.finish().await;
    result?;

    Ok(())
}

struct ReadBackendStats {
    total_reads: AtomicU64,
}

impl ReadBackendStats {
    fn new() -> Self {
        Self {
            total_reads: AtomicU64::new(0),
        }
    }

    fn record_read(&self) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn reads(&self) -> u64 {
        self.total_reads.load(Ordering::Relaxed)
    }
}

struct ReadWorkerContext {
    gmm: Arc<GaussianMixtureModel>,
    stats: Arc<ReadBackendStats>,
    limit: u32,
    start_time: Instant,
    duration: Duration,
    pacing_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

#[derive(Debug, Clone, Copy)]
struct ReadOpSample {
    latency_ms: f64,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReadWorkerSummary {
    attempts: u64,
    successes: u64,
    failures: u64,
    dropped: u64,
}

fn build_search_payload(query: Vec<f32>, limit: u32) -> SearchPayload {
    SearchPayload::default()
        .rank(RankExpr::Knn {
            query: QueryVector::Dense(query),
            key: Key::Embedding,
            limit,
            default: None,
            return_rank: false,
        })
        .limit(Some(limit), 0)
        .select([Key::Score])
}

fn spawn_read_backend_workers<SelFactory>(
    handles: &mut Vec<JoinHandle<ReadWorkerSummary>>,
    endpoint_label: &str,
    collections: Arc<[ChromaCollection]>,
    collection_semaphores: Arc<[Arc<Semaphore>]>,
    backend_semaphore: Arc<Semaphore>,
    args: ReadLoadArgs,
    seed_base: u64,
    start_time: Instant,
    pacing_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    gmm: Arc<GaussianMixtureModel>,
    stats: Arc<ReadBackendStats>,
    selector_factory: &mut SelFactory,
) where
    SelFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector>,
{
    if collections.is_empty() {
        return;
    }

    for task_id in 0..args.tasks {
        let collection_count = collections.len();
        let mut collection_selector = selector_factory(task_id, collection_count);
        let ctx = ReadWorkerContext {
            gmm: Arc::clone(&gmm),
            stats: Arc::clone(&stats),
            limit: args.limit,
            start_time,
            duration: Duration::from_secs(args.duration_secs),
            pacing_rx: Arc::clone(&pacing_rx),
        };
        let task_label = format!("{}_read_task{}", endpoint_label, task_id);
        let failure_label = task_label.clone();

        let handle = tokio::spawn(run_search_worker(
            Arc::clone(&collections),
            Arc::clone(&collection_semaphores),
            Arc::clone(&backend_semaphore),
            ctx,
            task_id as u64 * 1000 + seed_base,
            move |num_collections, rng| collection_selector.select(num_collections, rng),
            move |sample: ReadOpSample| {
                LOAD_SCATTERED_SEARCH_ATTEMPTS.click();
                LOAD_SCATTERED_SEARCH_SUCCESS.click();
                LOAD_SCATTERED_SEARCH_LATENCY_SENSOR.observe(sample.latency_ms);
                LOAD_SCATTERED_SEARCH_SUCCESS_LATENCY_SENSOR.observe(sample.latency_ms);
            },
            move |_attempt, err| {
                LOAD_SCATTERED_SEARCH_ATTEMPTS.click();
                LOAD_SCATTERED_SEARCH_FAILURES.click();
                eprintln!("[{}] Search error: {}", failure_label, err);
            },
            move |_dropped| {
                LOAD_SCATTERED_SEARCH_DROPPED.click();
            },
        ));
        handles.push(handle);
    }
}

fn spawn_read_progress_reporter(
    start_time: Instant,
    duration: Duration,
    stats_us: Arc<ReadBackendStats>,
    stats_eu: Arc<ReadBackendStats>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_us_reads = 0u64;
        let mut last_eu_reads = 0u64;
        let report_interval = Duration::from_secs(10);

        while start_time.elapsed() < duration {
            tokio::time::sleep(report_interval).await;

            let us_reads = stats_us.reads();
            let eu_reads = stats_eu.reads();
            let elapsed = start_time.elapsed().as_secs_f64();

            let us_reads_delta = us_reads - last_us_reads;
            let eu_reads_delta = eu_reads - last_eu_reads;
            let interval_secs = report_interval.as_secs_f64();

            println!(
                "[{:.0}s] US reads: {} | Rate: {:.1} reads/s",
                elapsed,
                us_reads,
                us_reads_delta as f64 / interval_secs
            );
            println!(
                "[{:.0}s] EU reads: {} | Rate: {:.1} reads/s",
                elapsed,
                eu_reads,
                eu_reads_delta as f64 / interval_secs
            );
            println!(
                "[{:.0}s] Total reads: {} | Rate: {:.1} reads/s",
                elapsed,
                us_reads + eu_reads,
                (us_reads_delta + eu_reads_delta) as f64 / interval_secs
            );
            println!();

            last_us_reads = us_reads;
            last_eu_reads = eu_reads;
        }
    })
}

fn print_dual_backend_read_summary(
    elapsed: Duration,
    stats_us: &ReadBackendStats,
    stats_eu: &ReadBackendStats,
) {
    let elapsed_secs = elapsed.as_secs_f64();
    let us_reads = stats_us.reads();
    let eu_reads = stats_eu.reads();

    println!("\n=== Concurrent Read Load Complete ===");
    println!("Duration: {:.1} seconds", elapsed_secs);
    println!();
    println!("US Backend:");
    println!("  Total reads: {}", us_reads);
    println!(
        "  Average rate: {:.1} reads/s",
        us_reads as f64 / elapsed_secs
    );
    println!();
    println!("EU Backend:");
    println!("  Total reads: {}", eu_reads);
    println!(
        "  Average rate: {:.1} reads/s",
        eu_reads as f64 / elapsed_secs
    );
    println!();
    println!("Combined:");
    println!("  Total reads: {}", us_reads + eu_reads);
    println!(
        "  Average rate: {:.1} reads/s",
        (us_reads + eu_reads) as f64 / elapsed_secs
    );
    println!(
        "  Read attempts/success/failures/dropped: {}/{}/{}/{}",
        LOAD_SCATTERED_SEARCH_ATTEMPTS.read(),
        LOAD_SCATTERED_SEARCH_SUCCESS.read(),
        LOAD_SCATTERED_SEARCH_FAILURES.read(),
        LOAD_SCATTERED_SEARCH_DROPPED.read(),
    );
}

async fn run_search_load_generator<UsSelectorFactory, EuSelectorFactory>(
    args: ReadLoadArgs,
    collections_us: Vec<ChromaCollection>,
    collections_eu: Vec<ChromaCollection>,
    mut us_selector_factory: UsSelectorFactory,
    mut eu_selector_factory: EuSelectorFactory,
) -> Result<(), Box<dyn Error>>
where
    UsSelectorFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector> + Send,
    EuSelectorFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector> + Send,
{
    if !args.enabled() {
        return Ok(());
    }

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);
    let max_outstanding_ops = args.max_outstanding_ops.max(1);
    let global_max_outstanding_ops = args.global_max_outstanding_ops.max(1);
    let gmm = Arc::new(GaussianMixtureModel::new(84));
    let stats_us = Arc::new(ReadBackendStats::new());
    let stats_eu = Arc::new(ReadBackendStats::new());

    let semaphores_us: Arc<[Arc<Semaphore>]> = (0..collections_us.len())
        .map(|_| Arc::new(Semaphore::new(max_outstanding_ops)))
        .collect::<Vec<_>>()
        .into();
    let semaphores_eu: Arc<[Arc<Semaphore>]> = (0..collections_eu.len())
        .map(|_| Arc::new(Semaphore::new(max_outstanding_ops)))
        .collect::<Vec<_>>()
        .into();
    let collections_us: Arc<[ChromaCollection]> = collections_us.into();
    let collections_eu: Arc<[ChromaCollection]> = collections_eu.into();
    let backend_semaphore_us = Arc::new(Semaphore::new(global_max_outstanding_ops));
    let backend_semaphore_eu = Arc::new(Semaphore::new(global_max_outstanding_ops));

    let (ticket_tx, ticket_rx) = mpsc::channel::<()>(1024);
    let pacing_rx = Arc::new(Mutex::new(ticket_rx));
    let pacing_handle = spawn_pacing_task(start_time, duration, args.pace_qps, ticket_tx);
    let mut handles = Vec::with_capacity(args.tasks.saturating_mul(2));

    spawn_read_backend_workers(
        &mut handles,
        "us",
        collections_us,
        semaphores_us,
        backend_semaphore_us,
        args,
        1_000_000,
        start_time,
        Arc::clone(&pacing_rx),
        Arc::clone(&gmm),
        Arc::clone(&stats_us),
        &mut us_selector_factory,
    );

    spawn_read_backend_workers(
        &mut handles,
        "eu",
        collections_eu,
        semaphores_eu,
        backend_semaphore_eu,
        args,
        1_500_000,
        start_time,
        Arc::clone(&pacing_rx),
        Arc::clone(&gmm),
        Arc::clone(&stats_eu),
        &mut eu_selector_factory,
    );

    let report_handle = spawn_read_progress_reporter(
        start_time,
        duration,
        Arc::clone(&stats_us),
        Arc::clone(&stats_eu),
    );

    for handle in handles {
        if let Err(err) = handle.await {
            eprintln!("read worker panicked: {err}");
        }
    }

    report_handle.abort();
    pacing_handle.abort();

    print_dual_backend_read_summary(start_time.elapsed(), &stats_us, &stats_eu);

    Ok(())
}

async fn run_search_worker<F, OnSuccess, OnFailure>(
    collections: Arc<[ChromaCollection]>,
    collection_semaphores: Arc<[Arc<Semaphore>]>,
    backend_semaphore: Arc<Semaphore>,
    ctx: ReadWorkerContext,
    seed: u64,
    mut select_collection: F,
    mut on_success: OnSuccess,
    mut on_failure: OnFailure,
    mut on_drop: impl FnMut(u64) + Send,
) -> ReadWorkerSummary
where
    F: FnMut(usize, &mut StdRng) -> usize + Send,
    OnSuccess: FnMut(ReadOpSample) + Send,
    OnFailure: FnMut(u64, String) + Send,
{
    enum ReadWorkerEvent {
        Success { latency_ms: f64 },
        Failure { attempt: u64, error: String },
    }

    let num_collections = collections.len();
    if num_collections == 0 {
        return ReadWorkerSummary::default();
    }

    let mut in_flight: FuturesUnordered<
        Pin<Box<dyn futures_util::Future<Output = ReadWorkerEvent> + Send>>,
    > = FuturesUnordered::new();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut summary = ReadWorkerSummary::default();

    let mut handle_completed = |event: ReadWorkerEvent, summary: &mut ReadWorkerSummary| match event
    {
        ReadWorkerEvent::Success { latency_ms } => {
            summary.successes += 1;
            on_success(ReadOpSample { latency_ms });
            ctx.stats.record_read();
        }
        ReadWorkerEvent::Failure { attempt, error } => {
            summary.failures += 1;
            on_failure(attempt, error);
        }
    };

    while ctx.start_time.elapsed() < ctx.duration {
        let remaining = ctx.duration.saturating_sub(ctx.start_time.elapsed());
        if remaining.is_zero() {
            break;
        }

        let ticket = tokio::select! {
            maybe_completed = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(completed) = maybe_completed {
                    handle_completed(completed, &mut summary);
                }
                continue;
            }
            ticket = time::timeout(remaining, async {
                let mut rx = ctx.pacing_rx.lock().await;
                rx.recv().await
            }) => ticket,
        };

        match ticket {
            Ok(Some(())) => {}
            _ => break,
        }

        let idx = select_collection(num_collections, &mut rng) % num_collections;
        let collection = collections[idx].clone();
        let semaphore = Arc::clone(&collection_semaphores[idx]);

        let backend_permit = match Arc::clone(&backend_semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                summary.dropped += 1;
                on_drop(summary.dropped);
                continue;
            }
            Err(tokio::sync::TryAcquireError::Closed) => break,
        };

        let permit = match semaphore.try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                drop(backend_permit);
                summary.dropped += 1;
                on_drop(summary.dropped);
                continue;
            }
            Err(tokio::sync::TryAcquireError::Closed) => break,
        };

        let query = ctx
            .gmm
            .generate_batch(&mut rng, 1)
            .into_iter()
            .next()
            .expect("one query embedding should be generated");
        let search = build_search_payload(query, ctx.limit);
        summary.attempts += 1;
        let attempt = summary.attempts;

        in_flight.push(Box::pin(async move {
            let op_start = Instant::now();
            let result = collection.search(vec![search]).await;
            drop(permit);
            drop(backend_permit);
            match result {
                Ok(_response) => ReadWorkerEvent::Success {
                    latency_ms: op_start.elapsed().as_secs_f64() * 1000.,
                },
                Err(err) => ReadWorkerEvent::Failure {
                    attempt,
                    error: err.to_string(),
                },
            }
        }));
    }

    while let Some(completed) = in_flight.next().await {
        handle_completed(completed, &mut summary);
    }

    summary
}
