//! Load Generator Example
//!
//! A load generator for Chroma that creates concurrent upsert operations across multiple
//! collections on two different Chroma endpoints.
//!
//! # Features
//!
//! - Dual endpoint support (api.trychroma.com and europe-west1.gcp.devchroma.com)
//! - Configurable number of collections, tasks, batch size, and duration
//! - Single shared collection target for hotspot-oriented load
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

use biometrics::Counter;
use chroma::bench::{
    boxed_collection_selector, collection_cache_file_path, prepare_dual_collections,
    print_load_generator_header, run_load_generator, start_load_metrics_emitter, CommonLoadArgs,
    DualLoadEndpoints, LoadMetricRefs,
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

    /// Maximum total number of outstanding operations per backend.
    /// Defaults to the per-collection limit when omitted.
    #[arg(long)]
    global_max_outstanding_ops: Option<usize>,

    /// Zipf skew for collection selection over `(0, 1)`. Ignored by this single-collection load.
    #[arg(long, value_name = "SKEW", value_parser = parse_zipf_param)]
    zipf: Option<f64>,

    /// Target local backends on ports 8000 and 8001 instead of cloud endpoints.
    #[arg(long, default_value_t = false)]
    local: bool,
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

/// Generates a deterministic collection name from the index.
fn collection_name() -> String {
    "loadgen_collection_pointed".to_string()
}

/// Returns the path to the collection cache file.
fn cache_file_path() -> String {
    collection_cache_file_path("loadgen_collection_pointed", 1)
}

static LOAD_POINTED_UPSERT_ATTEMPTS: Counter =
    Counter::new("load_generator.pointed.upsert_attempts");
static LOAD_POINTED_UPSERT_SUCCESS: Counter =
    Counter::new("load_generator.pointed.upsert_successes");
static LOAD_POINTED_UPSERT_FAILURES: Counter =
    Counter::new("load_generator.pointed.upsert_failures");
static LOAD_POINTED_UPSERT_DROPPED: Counter = Counter::new("load_generator.pointed.upsert_dropped");

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

    print_load_generator_header(&format!("Collection: {}", collection_name()), &common_args);
    if let Some(skew) = args.zipf {
        println!(
            "Collection selection: Zipf skew {skew} requested, but this load targets one collection"
        );
        println!();
    }

    println!("Creating/getting collection on both endpoints...");

    let metrics = LoadMetricRefs {
        upsert_attempts: &LOAD_POINTED_UPSERT_ATTEMPTS,
        upsert_success: &LOAD_POINTED_UPSERT_SUCCESS,
        upsert_failures: &LOAD_POINTED_UPSERT_FAILURES,
        upsert_dropped: &LOAD_POINTED_UPSERT_DROPPED,
        ddl_latency: &LOAD_POINTED_DDL_LATENCY_SENSOR,
        upsert_latency: &LOAD_POINTED_UPSERT_LATENCY_SENSOR,
        success_latency: &LOAD_POINTED_SUCCESS_LATENCY_SENSOR,
    };
    let metrics_emitter = start_load_metrics_emitter("load_generator_pointed.", &metrics);

    let result = async {
        // Load the collections for both endpoints.
        let collection_name = collection_name();
        let collection_names = vec![collection_name.clone()];
        let cache_path = cache_file_path();

        let (collection_us, collection_eu) = prepare_dual_collections(
            if args.local {
                DualLoadEndpoints::LOCAL
            } else {
                DualLoadEndpoints::CLOUD
            },
            &cache_path,
            &collection_names,
            args.max_outstanding_ops,
            ("US endpoint", "EU endpoint"),
            &LOAD_POINTED_DDL_LATENCY_SENSOR,
        )
        .await?;
        let collection_us = collection_us.into_iter().next().ok_or_else(|| {
            std::io::Error::other("failed to load US pointed load generator collection")
        })?;
        let collection_eu = collection_eu.into_iter().next().ok_or_else(|| {
            std::io::Error::other("failed to load EU pointed load generator collection")
        })?;
        println!("Collections ready. Starting load generation...\n");

        run_load_generator(
            &common_args,
            metrics,
            vec![collection_us.clone()],
            vec![collection_eu.clone()],
            |_task_id, _collection_count| {
                boxed_collection_selector(|_num_collections, _rng| 0usize)
            },
            |_task_id, _collection_count| {
                boxed_collection_selector(|_num_collections, _rng| 0usize)
            },
        )
        .await
    }
    .await;
    metrics_emitter.finish().await;
    result?;

    Ok(())
}
