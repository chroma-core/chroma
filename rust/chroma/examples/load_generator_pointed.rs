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
//! cargo run --example load_generator_pointed -- --duration 600 --tasks 4 --batch-size 100
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
use biometrics::Sensor;
use chroma::bench::{
    collection_cache_file_path, create_client, get_or_create_collections_with_cache,
    run_dual_load_generator, BackendStats, CollectionSelector, GaussianMixtureModel,
    LoadMetricRefs,
};
use clap::Parser;

/// Load generator for Chroma that creates concurrent upsert operations.
#[derive(Parser, Debug)]
#[command(name = "load_generator")]
#[command(about = "Generate load against Chroma endpoints")]
struct Args {
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
fn collection_name() -> String {
    "loadgen_collection_pointed".to_string()
}

/// Returns the path to the collection cache file.
fn cache_file_path() -> String {
    collection_cache_file_path("loadgen_collection_pointed", 1)
}

/// Maximum number of retry attempts for collection creation.
const MAX_COLLECTION_RETRIES: u32 = 3;
static LOAD_POINTED_UPSERT_ATTEMPTS: Counter =
    Counter::new("load_generator.pointed.upsert_attempts");
static LOAD_POINTED_UPSERT_SUCCESS: Counter =
    Counter::new("load_generator.pointed.upsert_successes");
static LOAD_POINTED_UPSERT_FAILURES: Counter =
    Counter::new("load_generator.pointed.upsert_failures");

static LOAD_POINTED_UPSERT_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_POINTED_UPSERT_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.pointed.upsert_latency_ms",
    &LOAD_POINTED_UPSERT_LATENCY,
);

static LOAD_POINTED_SUCCESS_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_POINTED_SUCCESS_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.pointed.upsert_success_latency_ms",
    &LOAD_POINTED_SUCCESS_LATENCY,
);

static LOAD_POINTED_DDL_LATENCY: sig_fig_histogram::LockFreeHistogram<450> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static LOAD_POINTED_DDL_LATENCY_SENSOR: biometrics::Histogram = biometrics::Histogram::new(
    "load_generator.pointed.collection_ddl_latency_ms",
    &LOAD_POINTED_DDL_LATENCY,
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("=== Chroma Load Generator ===");
    println!("Collection: {}", collection_name());
    println!("Duration: {} seconds", args.duration);
    println!("Tasks: {}", args.tasks);
    println!("Batch size: {}", args.batch_size);
    println!("Pace: {} qps", args.pace_qps.max(1));
    println!(
        "Max outstanding ops per collection: {}",
        args.max_outstanding_ops
    );
    println!();

    // Create clients for both endpoints.
    let client_us = create_client("https://api.devchroma.com:443")?;
    let client_eu = create_client("https://europe-west1.gcp.devchroma.com:443")?;

    println!("Creating/getting collection on both endpoints...");

    // Load the collections for both endpoints.
    let collection_name = collection_name();
    let collection_names = vec![collection_name.clone()];
    let cache_path = cache_file_path();

    let collection_us = get_or_create_collections_with_cache(
        &client_us,
        "us",
        &cache_path,
        &collection_names,
        MAX_COLLECTION_RETRIES,
        args.max_outstanding_ops,
        "US endpoint",
        |_collection_name, elapsed| {
            LOAD_POINTED_DDL_LATENCY_SENSOR.observe(elapsed.as_secs_f64() * 1000.);
        },
    )
    .await?;
    let collection_eu = get_or_create_collections_with_cache(
        &client_eu,
        "eu",
        &cache_path,
        &collection_names,
        MAX_COLLECTION_RETRIES,
        args.max_outstanding_ops,
        "EU endpoint",
        |_collection_name, elapsed| {
            LOAD_POINTED_DDL_LATENCY_SENSOR.observe(elapsed.as_secs_f64() * 1000.);
        },
    )
    .await?;
    let collection_us = collection_us.into_iter().next().ok_or_else(|| {
        std::io::Error::other("failed to load US pointed load generator collection")
    })?;
    let collection_eu = collection_eu.into_iter().next().ok_or_else(|| {
        std::io::Error::other("failed to load EU pointed load generator collection")
    })?;
    println!("Collections ready. Starting load generation...\n");

    // Shared state
    let gmm = Arc::new(GaussianMixtureModel::new(42));
    let stats_us = Arc::new(BackendStats::new());
    let stats_eu = Arc::new(BackendStats::new());

    let metrics = LoadMetricRefs {
        upsert_attempts: &LOAD_POINTED_UPSERT_ATTEMPTS,
        upsert_success: &LOAD_POINTED_UPSERT_SUCCESS,
        upsert_failures: &LOAD_POINTED_UPSERT_FAILURES,
        ddl_latency: &LOAD_POINTED_DDL_LATENCY_SENSOR,
        upsert_latency: &LOAD_POINTED_UPSERT_LATENCY_SENSOR,
        success_latency: &LOAD_POINTED_SUCCESS_LATENCY_SENSOR,
    };

    run_dual_load_generator(
        Duration::from_secs(args.duration),
        args.pace_qps,
        args.tasks,
        args.batch_size,
        args.max_outstanding_ops,
        "load_generator_pointed.",
        metrics,
        vec![collection_us.clone()],
        vec![collection_eu.clone()],
        gmm,
        stats_us,
        stats_eu,
        |_task_id, _collection_count| {
            Box::new(|_num_collections, _rng| 0usize) as CollectionSelector
        },
        |_task_id, _collection_count| {
            Box::new(|_num_collections, _rng| 0usize) as CollectionSelector
        },
    )
    .await?;

    Ok(())
}
