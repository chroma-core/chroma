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

use biometrics::Counter;
use clap::Parser;
use guacamole::combinators::map;
use guacamole::{Guacamole, Zipf};

use chroma::bench::{
    boxed_collection_selector, collection_cache_file_path, prepare_dual_collections,
    print_load_generator_header, run_load_generator, CommonLoadArgs, DualLoadEndpoints,
    LoadMetricRefs,
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

    /// Target local backends on ports 8000 and 8001 instead of cloud endpoints.
    #[arg(long, default_value_t = false)]
    local: bool,
}

/// Generates a deterministic collection name from the index.
fn collection_name(index: usize) -> String {
    format!("loadgen_collection2_{:06}", index)
}

/// Returns the path to the collection cache file.
fn cache_file_path(num_collections: usize) -> String {
    collection_cache_file_path("loadgen_collections2", num_collections)
}

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
    let common_args = CommonLoadArgs {
        duration_secs: args.duration,
        tasks: args.tasks,
        batch_size: args.batch_size,
        pace_qps: args.pace_qps,
        max_outstanding_ops: args.max_outstanding_ops,
    };

    print_load_generator_header(
        &format!("Collections: {}", args.collections),
        &common_args,
    );

    println!(
        "Creating/getting {} collections on both endpoints...",
        args.collections
    );

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

    println!("Collections ready. Starting load generation...\n");

    let metrics = LoadMetricRefs {
        upsert_attempts: &LOAD_SCATTERED_UPSERT_ATTEMPTS,
        upsert_success: &LOAD_SCATTERED_UPSERT_SUCCESS,
        upsert_failures: &LOAD_SCATTERED_UPSERT_FAILURES,
        ddl_latency: &LOAD_SCATTERED_DDL_LATENCY_SENSOR,
        upsert_latency: &LOAD_SCATTERED_UPSERT_LATENCY_SENSOR,
        success_latency: &LOAD_SCATTERED_SUCCESS_LATENCY_SENSOR,
    };

    run_load_generator(
        &common_args,
        "load_generator_scattered.",
        metrics,
        collections_us,
        collections_eu,
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
