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

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use biometrics::Sensor;

use biometrics::{Collector, Counter};
use chroma::{ChromaCollection, ChromaHttpClient};
use clap::Parser;
use futures_util::future::join_all;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use utf8path::Path as Utf8Path;

use chroma::bench::{
    create_client, get_or_create_collection_with_retry, BackendStats, GaussianMixtureModel,
    WorkerContext,
};

/// Print warmup progress every N completed collections.
const WARMUP_PROGRESS_INTERVAL: usize = 10;
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
    format!("loadgen_collections2_{}.json", num_collections)
}

/// Cached collection data for dehydration/rehydration.
#[derive(Serialize, Deserialize)]
struct CollectionCache {
    us_collections: Vec<serde_json::Value>,
    eu_collections: Vec<serde_json::Value>,
}

/// Attempts to load collections from the cache file.
async fn load_collections_from_cache(
    client_us: &ChromaHttpClient,
    client_eu: &ChromaHttpClient,
    num_collections: usize,
) -> Option<(Vec<ChromaCollection>, Vec<ChromaCollection>)> {
    let cache_path = cache_file_path(num_collections);
    let path = Path::new(&cache_path);

    if !path.exists() {
        return None;
    }

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to read cache file: {}", e);
            return None;
        }
    };

    let cache: CollectionCache = match serde_json::from_str(&content) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to parse cache file: {}", e);
            return None;
        }
    };

    if cache.us_collections.len() != num_collections
        || cache.eu_collections.len() != num_collections
    {
        eprintln!(
            "Cache has wrong number of collections (expected {}, got US:{}, EU:{})",
            num_collections,
            cache.us_collections.len(),
            cache.eu_collections.len()
        );
        return None;
    }

    let mut us_collections = Vec::with_capacity(num_collections);
    for dehydrated in cache.us_collections {
        match client_us.rehydrate_collection(dehydrated).await {
            Ok(c) => us_collections.push(c),
            Err(e) => {
                eprintln!("Failed to rehydrate US collection: {}", e);
                return None;
            }
        }
    }

    let mut eu_collections = Vec::with_capacity(num_collections);
    for dehydrated in cache.eu_collections {
        match client_eu.rehydrate_collection(dehydrated).await {
            Ok(c) => eu_collections.push(c),
            Err(e) => {
                eprintln!("Failed to rehydrate EU collection: {}", e);
                return None;
            }
        }
    }

    Some((us_collections, eu_collections))
}

/// Creates collections on a client concurrently and logs warmup progress as they complete.
async fn create_collections_with_progress(
    client: &ChromaHttpClient,
    collection_names: Vec<String>,
    max_outstanding_ops: usize,
    label: &'static str,
) -> Result<Vec<ChromaCollection>, chroma::client::ChromaHttpClientError> {
    let total_collections = collection_names.len();
    let limiter = Arc::new(Semaphore::new(max_outstanding_ops));
    let (progress_tx, mut progress_rx) = mpsc::channel::<()>(max_outstanding_ops.max(1));

    let progress_handle = tokio::spawn(async move {
        let mut completed = 0usize;

        while let Some(()) = progress_rx.recv().await {
            completed += 1;
            if completed.is_multiple_of(WARMUP_PROGRESS_INTERVAL) || completed == total_collections
            {
                let pct = if total_collections == 0 {
                    100.0
                } else {
                    (completed as f64 / total_collections as f64) * 100.0
                };
                println!(
                    "{} warmup progress: {}/{} collections ({:.0}%)",
                    label, completed, total_collections, pct
                );
            }
        }
    });

    let mut futures = Vec::with_capacity(total_collections);
    for name in collection_names {
        let client = client.clone();
        let limiter = Arc::clone(&limiter);
        let tx = progress_tx.clone();
        futures.push(async move {
            let _permit = limiter.acquire().await.unwrap();
            let result =
                get_or_create_collection_with_retry(&client, &name, MAX_COLLECTION_RETRIES).await;
            let _ = tx.send(()).await;
            result
        });
    }
    drop(progress_tx);

    let results: Vec<Result<ChromaCollection, chroma::client::ChromaHttpClientError>> =
        join_all(futures).await;
    let collections = results.into_iter().collect();
    progress_handle
        .await
        .expect("warmup progress reporter should not panic");

    collections
}

/// Saves collections to the cache file.
async fn save_collections_to_cache(
    us_collections: &[ChromaCollection],
    eu_collections: &[ChromaCollection],
    num_collections: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut us_dehydrated = Vec::with_capacity(us_collections.len());
    for collection in us_collections {
        us_dehydrated.push(collection.dehydrate().await?);
    }

    let mut eu_dehydrated = Vec::with_capacity(eu_collections.len());
    for collection in eu_collections {
        eu_dehydrated.push(collection.dehydrate().await?);
    }

    let cache = CollectionCache {
        us_collections: us_dehydrated,
        eu_collections: eu_dehydrated,
    };

    let content = serde_json::to_string_pretty(&cache)?;
    let cache_path = cache_file_path(num_collections);
    std::fs::write(&cache_path, content)?;

    println!("  Saved collections to cache: {}", cache_path);
    Ok(())
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

fn spawn_metrics_emitter() -> (tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
    let collector = Collector::new();
    collector.register_counter(&LOAD_SCATTERED_UPSERT_ATTEMPTS);
    collector.register_counter(&LOAD_SCATTERED_UPSERT_SUCCESS);
    collector.register_counter(&LOAD_SCATTERED_UPSERT_FAILURES);
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
    let mut record_counter: u64 = 0;
    let num_collections = collections.len();

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
        let collection_idx = rng.gen_range(0..num_collections);
        let collection = &collections[collection_idx];
        let semaphore = &collection_semaphores[collection_idx];

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

    // Try to load collections from cache first
    let (collections_us, collections_eu) = if let Some((us, eu)) =
        load_collections_from_cache(&client_us, &client_eu, args.collections).await
    {
        println!(
            "  Loaded {} US collections and {} EU collections from cache",
            us.len(),
            eu.len()
        );
        (us, eu)
    } else {
        // Create or get collections on both endpoints concurrently with retry logic
        let collection_names: Vec<String> = (0..args.collections).map(collection_name).collect();
        let (collections_us, collections_eu) = tokio::join!(
            create_collections_with_progress(
                &client_us,
                collection_names.clone(),
                args.max_outstanding_ops,
                "US",
            ),
            create_collections_with_progress(
                &client_eu,
                collection_names,
                args.max_outstanding_ops,
                "EU",
            )
        );
        let collections_us = collections_us?;
        let collections_eu = collections_eu?;

        println!(
            "  Created {} US collections and {} EU collections",
            collections_us.len(),
            collections_eu.len()
        );

        // Save to cache for next run
        if let Err(e) =
            save_collections_to_cache(&collections_us, &collections_eu, args.collections).await
        {
            eprintln!("  Warning: Failed to save collections to cache: {}", e);
        }

        (collections_us, collections_eu)
    };

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
    let ticket_interval = Duration::from_secs_f64(1.0 / pace_qps as f64);

    let (ticket_tx, ticket_rx) = mpsc::channel::<()>(1024);
    let pacing_rx = Arc::new(Mutex::new(ticket_rx));
    let (metrics_handle, stop_metrics) = spawn_metrics_emitter();

    let pacing_handle = {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(ticket_interval);
            while start_time.elapsed() < duration {
                interval.tick().await;
                let _ = ticket_tx.try_send(());
            }
        })
    };

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
        let handle = tokio::spawn(run_worker(
            collections_us.clone(),
            semaphores_us.clone(),
            ctx,
            task_id as u64 * 1000,
            format!("us_task{}", task_id),
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
        let handle = tokio::spawn(run_worker(
            collections_eu.clone(),
            semaphores_eu.clone(),
            ctx,
            (task_id as u64 + 500) * 1000,
            format!("eu_task{}", task_id),
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
        let _ = handle.await;
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
