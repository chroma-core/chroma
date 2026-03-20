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
use std::time::Duration;

use biometrics::Counter;
use clap::Parser;
use guacamole::combinators::map;
use guacamole::{Guacamole, Zipf};

use chroma::bench::{
    boxed_collection_selector, collection_cache_file_path, create_client,
    get_or_create_collections_with_cache, run_dual_load_generator, BackendStats,
    GaussianMixtureModel, LoadMetricRefs,
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

    // Shared state
    let gmm = Arc::new(GaussianMixtureModel::new(42));
    let stats_us = Arc::new(BackendStats::new());
    let stats_eu = Arc::new(BackendStats::new());

    let metrics = LoadMetricRefs {
        upsert_attempts: &LOAD_SCATTERED_UPSERT_ATTEMPTS,
        upsert_success: &LOAD_SCATTERED_UPSERT_SUCCESS,
        upsert_failures: &LOAD_SCATTERED_UPSERT_FAILURES,
        ddl_latency: &LOAD_SCATTERED_DDL_LATENCY_SENSOR,
        upsert_latency: &LOAD_SCATTERED_UPSERT_LATENCY_SENSOR,
        success_latency: &LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR,
    };

    run_dual_load_generator(
        Duration::from_secs(args.duration),
        args.pace_qps,
        args.tasks,
        args.batch_size,
        args.max_outstanding_ops,
        "load_generator_scattered.",
        metrics,
        collections_us,
        collections_eu,
        gmm,
        stats_us,
        stats_eu,
        |task_id, collection_count| {
            let mut collection_rng = Guacamole::new(task_id as u64 * 1000);
            let zipf = Zipf::from_param(collection_count as u64, 0.8);
            let mut collection_idx = map(
                move |guac| zipf.next(guac),
                |value| (value as usize).saturating_sub(1),
            );
            boxed_collection_selector(move |num_collections, _rng| {
                let _ = _rng;
                collection_idx(&mut collection_rng) % num_collections
            })
        },
        |task_id, collection_count| {
            let mut collection_rng = Guacamole::new((task_id as u64 + 500) * 1000);
            let zipf = Zipf::from_param(collection_count as u64, 0.8);
            let mut collection_idx = map(
                move |guac| zipf.next(guac),
                |value| (value as usize).saturating_sub(1),
            );
            boxed_collection_selector(move |num_collections, _rng| {
                let _ = _rng;
                collection_idx(&mut collection_rng) % num_collections
            })
        },
    )
    .await?;

    Ok(())
}
