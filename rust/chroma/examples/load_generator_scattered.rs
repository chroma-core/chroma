//! Load Generator Example
//!
//! A load generator for Chroma that creates concurrent upsert operations across multiple
//! collections on two different Chroma endpoints.
//!
//! # Features
//!
//! - Dual endpoint support (api.trychroma.com and europe-west1.gcp.devchroma.com)
//! - Configurable number of collections, tasks, batch size, and duration
//! - Random collection selection within each task to avoid concurrency hotspots
//! - Gaussian Mixture Model (GMM) for realistic embedding generation
//!
//! # Usage
//!
//! ```bash
//! cargo run --example load_generator_scattered -- --collections 10 --duration 600 --tasks 4 --batch-size 100
//! ```
//!
//! # Environment Variables
//!
//! The following environment variables must be set:
//! - `CHROMA_API_KEY` - API key for Chroma Cloud authentication
//! - `CHROMA_TENANT` - Tenant ID (optional, will be auto-resolved)
//! - `CHROMA_DATABASE` - Database name (optional, will be auto-resolved)

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use biometrics::Sensor;

use biometrics::{Collector, Counter};
use chroma::ChromaCollection;
use clap::Parser;
use guacamole::combinators::*;
use guacamole::{Guacamole, Zipf};
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use utf8path::Path as Utf8Path;

use chroma::bench::{
    collection_cache_file_path, create_client, get_or_create_collections_with_cache,
    run_load_worker, spawn_pacing_task, BackendStats, GaussianMixtureModel, LoadOpSample,
    WorkerContext,
};

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
}

/// Generates a deterministic collection name from the index.
fn collection_name(index: usize) -> String {
    format!("loadgen_collection2_{:06}", index)
}

/// Returns the path to the collection cache file.
fn cache_file_path(num_collections: usize) -> String {
    collection_cache_file_path("loadgen_collections2", num_collections)
}

/// Maximum number of retry attempts for collection creation.
const MAX_COLLECTION_RETRIES: u32 = 3;
static LOAD_SCATTERED_UPSERT_ATTEMPTS: Counter =
    Counter::new("load_generator.scattered.upsert_attempts");
static LOAD_SCATTERED_UPSERT_SUCCESS: Counter =
    Counter::new("load_generator.scattered.upsert_successes");
static LOAD_SCATTERED_UPSERT_FAILURES: Counter =
    Counter::new("load_generator.scattered.upsert_failures");

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

fn spawn_metrics_emitter() -> (tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
    let collector = Collector::new();
    collector.register_counter(&LOAD_SCATTERED_UPSERT_ATTEMPTS);
    collector.register_counter(&LOAD_SCATTERED_UPSERT_SUCCESS);
    collector.register_counter(&LOAD_SCATTERED_UPSERT_FAILURES);
    collector.register_histogram(&LOAD_SCATTERED_DDL_LATENCY_SENSOR);
    collector.register_histogram(&LOAD_SCATTERED_UPSERT_LATENCY_SENSOR);
    collector.register_histogram(&LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR);
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let mut emitter = biometrics_prometheus::Emitter::new(biometrics_prometheus::Options {
            segment_size: 64 * 1024 * 1024 * 1024,
            flush_interval: Duration::from_secs(3600),
            prefix: Utf8Path::new("load_generator_scattered."),
        });
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("system clock moved backward")
                        .as_millis()
                        .try_into()
                        .expect("timestamp exceeds supported range");
                    let _ = collector.emit(&mut emitter, now);
                }
                _ = &mut stop_rx => {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("system clock moved backward")
                        .as_millis()
                        .try_into()
                        .expect("timestamp exceeds supported range");
                    let _ = collector.emit(&mut emitter, now);
                    break;
                }
            }
        }
    });

    (handle, stop_tx)
}

/// Runs a worker task that performs upserts with random collection selection.
async fn run_worker(
    collections: Vec<ChromaCollection>,
    collection_semaphores: Vec<Arc<Semaphore>>,
    ctx: WorkerContext,
    seed: u64,
    id_prefix: String,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut collection_rng = Guacamole::new(seed);
    let mut record_counter: u64 = 0;
    let zipf = Zipf::from_param(collections.len() as u64, 0.8);
    let mut collection_idx = map(
        move |guac| zipf.next(guac),
        |value| (value as usize).saturating_sub(1),
    );

    while ctx.start_time.elapsed() < ctx.duration {
        let remaining = ctx.duration.saturating_sub(ctx.start_time.elapsed());
        if remaining.is_zero() {
            break;
        }

        let ticket = tokio::time::timeout(remaining, async {
            let mut rx = ctx.pacing_rx.lock().await;
            rx.recv().await
        })
        .await;

        match ticket {
            Ok(Some(())) => {}
            _ => break,
        }

        // Random collection selection to avoid concurrency hotspots
        let idx = collection_idx(&mut collection_rng) % collections.len();
        let collection = &collections[idx];
        let semaphore = &collection_semaphores[idx];

        // Acquire permit to limit outstanding ops per collection
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };

        // Generate batch
        let embeddings = ctx.gmm.generate_batch(&mut rng, ctx.batch_size);
        let ids: Vec<String> = (0..ctx.batch_size)
            .map(|i| {
                record_counter += 1;
                format!("{}_{}", id_prefix, record_counter + i as u64)
            })
            .collect();

        // Perform upsert
        let op_start = Instant::now();
        LOAD_SCATTERED_UPSERT_ATTEMPTS.click();
        match collection.upsert(ids, embeddings, None, None, None).await {
            Ok(_) => {
                let latency_ms = op_start.elapsed().as_secs_f64() * 1000.;
                LOAD_SCATTERED_UPSERT_SUCCESS.click();
                LOAD_SCATTERED_UPSERT_LATENCY_SENSOR.observe(latency_ms);
                LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR.observe(latency_ms);
                ctx.stats.record_upsert(ctx.batch_size as u64);
            }
            Err(e) => {
                LOAD_SCATTERED_UPSERT_FAILURES.click();
                eprintln!("[{}] Upsert error: {}", id_prefix, e);
            }
        }

        // Permit is dropped here, releasing the semaphore slot
        drop(permit);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("=== Chroma Load Generator ===");
    println!("Collections: {}", args.collections);
    println!("Duration: {} seconds", args.duration);
    println!("Tasks: {}", args.tasks);
    println!("Batch size: {}", args.batch_size);
    println!("Pace: {} qps", args.pace_qps.max(1));
    println!(
        "Max outstanding ops per collection: {}",
        args.max_outstanding_ops
    );
    println!();

    // Create clients for both endpoints
    let client_us = create_client("https://api.devchroma.com:443")?;
    let client_eu = create_client("https://europe-west1.gcp.devchroma.com:443")?;

    println!(
        "Creating/getting {} collections on both endpoints...",
        args.collections
    );

    let collection_names: Vec<String> = (0..args.collections).map(collection_name).collect();
    let cache_path = cache_file_path(args.collections);
    let collections_us = get_or_create_collections_with_cache(
        &client_us,
        "us",
        &cache_path,
        &collection_names,
        MAX_COLLECTION_RETRIES,
        args.max_outstanding_ops,
        "US",
        |_collection_name, elapsed| {
            LOAD_SCATTERED_DDL_LATENCY_SENSOR.observe(elapsed.as_secs_f64() * 1000.);
        },
    )
    .await?;
    let collections_eu = get_or_create_collections_with_cache(
        &client_eu,
        "eu",
        &cache_path,
        &collection_names,
        MAX_COLLECTION_RETRIES,
        args.max_outstanding_ops,
        "EU",
        |_collection_name, elapsed| {
            LOAD_SCATTERED_DDL_LATENCY_SENSOR.observe(elapsed.as_secs_f64() * 1000.);
        },
    )
    .await?;
    println!(
        "  Ready {} US collections and {} EU collections",
        collections_us.len(),
        collections_eu.len()
    );

    println!("Collections ready. Starting load generation...\n");

    // Create per-collection semaphores to limit outstanding operations
    let semaphores_us: Vec<Arc<Semaphore>> = (0..args.collections)
        .map(|_| Arc::new(Semaphore::new(args.max_outstanding_ops)))
        .collect();
    let semaphores_eu: Vec<Arc<Semaphore>> = (0..args.collections)
        .map(|_| Arc::new(Semaphore::new(args.max_outstanding_ops)))
        .collect();

    // Shared state
    let gmm = Arc::new(GaussianMixtureModel::new(42));
    let stats_us = Arc::new(BackendStats::new());
    let stats_eu = Arc::new(BackendStats::new());

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let pace_qps = args.pace_qps.max(1);

    let (ticket_tx, ticket_rx) = mpsc::channel::<()>(1024);
    let pacing_rx = Arc::new(Mutex::new(ticket_rx));
    let (metrics_handle, stop_metrics) = spawn_metrics_emitter();

    let pacing_handle = spawn_pacing_task(start_time, duration, pace_qps, ticket_tx);

    // Spawn worker tasks
    let mut handles = Vec::new();

    for task_id in 0..args.tasks {
        // US endpoint task
        let ctx = WorkerContext {
            gmm: Arc::clone(&gmm),
            stats: Arc::clone(&stats_us),
            batch_size: args.batch_size,
            start_time,
            duration,
            pacing_rx: Arc::clone(&pacing_rx),
        };
        let mut collection_rng = Guacamole::new(task_id as u64 * 1000);
        let mut zipf = Zipf::from_param(collections_us.len() as u64, 0.8);
        let mut collection_idx = map(
            move |guac| zipf.next(guac),
            |value| (value as usize).saturating_sub(1),
        );
        let us_collection_count = collections_us.len();
        let us_task_label = format!("us_task{}", task_id);
        let handle = tokio::spawn(run_load_worker(
            collections_us.clone(),
            semaphores_us.clone(),
            ctx,
            task_id as u64 * 1000,
            us_task_label.clone(),
            move |_num_collections, rng| {
                let _ = rng;
                collection_idx(&mut collection_rng) % us_collection_count
            },
            move |sample: LoadOpSample| {
                LOAD_SCATTERED_UPSERT_ATTEMPTS.click();
                LOAD_SCATTERED_UPSERT_SUCCESS.click();
                LOAD_SCATTERED_UPSERT_LATENCY_SENSOR.observe(sample.latency_ms);
                LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR.observe(sample.latency_ms);
            },
            move |_attempt, err| {
                LOAD_SCATTERED_UPSERT_ATTEMPTS.click();
                LOAD_SCATTERED_UPSERT_FAILURES.click();
                eprintln!("[{}] Upsert error: {}", us_task_label, err);
            },
        ));
        handles.push(handle);

        // EU endpoint task
        let ctx = WorkerContext {
            gmm: Arc::clone(&gmm),
            stats: Arc::clone(&stats_eu),
            batch_size: args.batch_size,
            start_time,
            duration,
            pacing_rx: Arc::clone(&pacing_rx),
        };
        let mut collection_rng = Guacamole::new((task_id as u64 + 500) * 1000);
        let mut zipf = Zipf::from_param(collections_eu.len() as u64, 0.8);
        let mut collection_idx = map(
            move |guac| zipf.next(guac),
            |value| (value as usize).saturating_sub(1),
        );
        let eu_collection_count = collections_eu.len();
        let eu_task_label = format!("eu_task{}", task_id);
        let handle = tokio::spawn(run_load_worker(
            collections_eu.clone(),
            semaphores_eu.clone(),
            ctx,
            (task_id as u64 + 500) * 1000,
            eu_task_label.clone(),
            move |_num_collections, rng| {
                let _ = rng;
                collection_idx(&mut collection_rng) % eu_collection_count
            },
            move |sample: LoadOpSample| {
                LOAD_SCATTERED_UPSERT_ATTEMPTS.click();
                LOAD_SCATTERED_UPSERT_SUCCESS.click();
                LOAD_SCATTERED_UPSERT_LATENCY_SENSOR.observe(sample.latency_ms);
                LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR.observe(sample.latency_ms);
            },
            move |_attempt, err| {
                LOAD_SCATTERED_UPSERT_ATTEMPTS.click();
                LOAD_SCATTERED_UPSERT_FAILURES.click();
                eprintln!("[{}] Upsert error: {}", eu_task_label, err);
            },
        ));
        handles.push(handle);
    }

    // Progress reporting task
    let stats_us_report = Arc::clone(&stats_us);
    let stats_eu_report = Arc::clone(&stats_eu);
    let report_handle = tokio::spawn(async move {
        let mut last_us_upserts = 0u64;
        let mut last_us_records = 0u64;
        let mut last_eu_upserts = 0u64;
        let mut last_eu_records = 0u64;
        let report_interval = Duration::from_secs(10);

        while start_time.elapsed() < duration {
            tokio::time::sleep(report_interval).await;

            let us_upserts = stats_us_report.upserts();
            let us_records = stats_us_report.records();
            let eu_upserts = stats_eu_report.upserts();
            let eu_records = stats_eu_report.records();
            let elapsed = start_time.elapsed().as_secs_f64();

            let us_upserts_delta = us_upserts - last_us_upserts;
            let us_records_delta = us_records - last_us_records;
            let eu_upserts_delta = eu_upserts - last_eu_upserts;
            let eu_records_delta = eu_records - last_eu_records;

            let interval_secs = report_interval.as_secs_f64();

            println!(
                "[{:.0}s] US: {} upserts, {} records | Rate: {:.1} upserts/s, {:.1} records/s",
                elapsed,
                us_upserts,
                us_records,
                us_upserts_delta as f64 / interval_secs,
                us_records_delta as f64 / interval_secs
            );
            println!(
                "[{:.0}s] EU: {} upserts, {} records | Rate: {:.1} upserts/s, {:.1} records/s",
                elapsed,
                eu_upserts,
                eu_records,
                eu_upserts_delta as f64 / interval_secs,
                eu_records_delta as f64 / interval_secs
            );
            println!(
                "[{:.0}s] Total: {} upserts, {} records | Rate: {:.1} upserts/s, {:.1} records/s",
                elapsed,
                us_upserts + eu_upserts,
                us_records + eu_records,
                (us_upserts_delta + eu_upserts_delta) as f64 / interval_secs,
                (us_records_delta + eu_records_delta) as f64 / interval_secs
            );
            println!();

            last_us_upserts = us_upserts;
            last_us_records = us_records;
            last_eu_upserts = eu_upserts;
            last_eu_records = eu_records;
        }
    });

    // Wait for all tasks to complete
    for handle in handles {
        match handle.await {
            Ok(_summary) => {}
            Err(err) => eprintln!("load worker panicked: {}", err),
        }
    }
    let _ = stop_metrics.send(());
    let _ = metrics_handle.await;
    report_handle.abort();
    pacing_handle.abort();

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    let us_upserts = stats_us.upserts();
    let us_records = stats_us.records();
    let eu_upserts = stats_eu.upserts();
    let eu_records = stats_eu.records();

    println!("\n=== Load Generation Complete ===");
    println!("Duration: {:.1} seconds", elapsed_secs);
    println!();
    println!("US Backend:");
    println!("  Total upserts: {}", us_upserts);
    println!("  Total records: {}", us_records);
    println!(
        "  Average rate: {:.1} upserts/s, {:.1} records/s",
        us_upserts as f64 / elapsed_secs,
        us_records as f64 / elapsed_secs
    );
    println!();
    println!("EU Backend:");
    println!("  Total upserts: {}", eu_upserts);
    println!("  Total records: {}", eu_records);
    println!(
        "  Average rate: {:.1} upserts/s, {:.1} records/s",
        eu_upserts as f64 / elapsed_secs,
        eu_records as f64 / elapsed_secs
    );
    println!();
    println!("Combined:");
    println!("  Total upserts: {}", us_upserts + eu_upserts);
    println!("  Total records: {}", us_records + eu_records);
    println!(
        "  Average rate: {:.1} upserts/s, {:.1} records/s",
        (us_upserts + eu_upserts) as f64 / elapsed_secs,
        (us_records + eu_records) as f64 / elapsed_secs
    );
    println!(
        "  Upsert attempts/success/failures: {}/{}/{}",
        LOAD_SCATTERED_UPSERT_ATTEMPTS.read(),
        LOAD_SCATTERED_UPSERT_SUCCESS.read(),
        LOAD_SCATTERED_UPSERT_FAILURES.read(),
    );

    Ok(())
}
