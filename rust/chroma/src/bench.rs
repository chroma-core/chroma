//! Shared helpers for example load generators.

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::ChromaHttpClientError;
use crate::{ChromaCollection, ChromaHttpClient, ChromaHttpClientOptions};
use biometrics::Sensor;
use futures_util::future::join_all;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;

/// Embedding dimensionality shared by load generator examples.
pub const EMBEDDING_DIM: usize = 1536;

/// Default number of GMM clusters shared by load generator examples.
pub const NUM_CLUSTERS: usize = 1000;

/// JSONL cache progress logging interval during collection bootstrap.
const WARMUP_PROGRESS_INTERVAL: usize = 10;

#[derive(Debug, Serialize, Deserialize)]
struct CachedCollection {
    endpoint: String,
    name: String,
    dehydrated: serde_json::Value,
}

/// Returns the cache file path for collection metadata.
pub fn collection_cache_file_path(prefix: &str, num_collections: usize) -> String {
    format!("{prefix}_{num_collections}.jsonl")
}

fn read_collection_cache_records(
    cache_path: &Path,
) -> HashMap<String, HashMap<String, serde_json::Value>> {
    let mut cache = HashMap::<String, HashMap<String, serde_json::Value>>::new();
    let file = match File::open(cache_path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return cache,
        Err(err) => {
            eprintln!(
                "Failed to read collection cache {}: {}",
                cache_path.display(),
                err
            );
            return cache;
        }
    };

    let reader = BufReader::new(file);
    for (line_number, raw_line) in reader.lines().enumerate() {
        let line_number = line_number + 1;
        let line = match raw_line {
            Ok(line) => line,
            Err(err) => {
                eprintln!(
                    "Failed to read line {line_number} from collection cache {}: {}",
                    cache_path.display(),
                    err
                );
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match serde_json::from_str::<CachedCollection>(line) {
            Ok(record) => {
                cache
                    .entry(record.endpoint)
                    .or_default()
                    .insert(record.name, record.dehydrated);
            }
            Err(err) => {
                eprintln!(
                    "Failed to parse line {line_number} from collection cache {}: {}",
                    cache_path.display(),
                    err
                );
            }
        }
    }

    cache
}

fn write_collection_cache_records(
    cache_path: &Path,
    cache: &HashMap<String, HashMap<String, serde_json::Value>>,
) -> io::Result<()> {
    let tmp_path = cache_path.with_extension("tmp");
    let mut file = BufWriter::new(File::create(&tmp_path)?);
    let mut endpoints: Vec<&str> = cache.keys().map(String::as_str).collect();
    endpoints.sort_unstable();

    for endpoint in endpoints {
        let mut names: Vec<&str> = cache
            .get(endpoint)
            .map(|collections| collections.keys().map(String::as_str).collect())
            .unwrap_or_default();
        names.sort_unstable();

        for name in names {
            let dehydrated = cache
                .get(endpoint)
                .and_then(|collections| collections.get(name))
                .ok_or_else(|| io::Error::other("cache mutation during write"))?;
            let record = CachedCollection {
                endpoint: endpoint.to_string(),
                name: name.to_string(),
                dehydrated: dehydrated.clone(),
            };
            let line = serde_json::to_string(&record).map_err(io::Error::other)?;
            writeln!(file, "{}", line)?;
        }
    }

    file.flush()?;
    let file = file.into_inner()?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, cache_path)?;

    Ok(())
}

fn append_collection_cache_record(
    cache_path: &Path,
    endpoint_label: &str,
    collection_name: &str,
    dehydrated: &serde_json::Value,
) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(cache_path)?;
    let record = CachedCollection {
        endpoint: endpoint_label.to_string(),
        name: collection_name.to_string(),
        dehydrated: dehydrated.clone(),
    };
    let line = serde_json::to_string(&record).map_err(io::Error::other)?;
    writeln!(file, "{}", line)?;
    file.sync_all()?;

    Ok(())
}

/// Loads cached collections for the endpoint, rehydrating when possible and creating the rest.
///
/// Any collection found in the cache is rehydrated. Collections that are missing from the cache
/// or fail to rehydrate are created with a retry loop.
#[allow(clippy::too_many_arguments)]
pub async fn get_or_create_collections_with_cache(
    client: &ChromaHttpClient,
    endpoint_label: &str,
    cache_path: impl AsRef<Path>,
    collection_names: &[String],
    max_retries: u32,
    max_outstanding_ops: usize,
    progress_label: &'static str,
    mut on_ddl_latency: impl FnMut(&str, Duration) + Send,
) -> Result<Vec<ChromaCollection>, ChromaHttpClientError> {
    let cache_path = cache_path.as_ref();
    let mut cache = read_collection_cache_records(cache_path);
    let mut endpoint_cache = cache.remove(endpoint_label).unwrap_or_default();

    let mut pending: Vec<(usize, String)> = Vec::new();
    let mut collections: Vec<Option<ChromaCollection>> = vec![None; collection_names.len()];

    for (idx, name) in collection_names.iter().enumerate() {
        match endpoint_cache.get(name) {
            Some(dehydrated) => match client.rehydrate_collection(dehydrated.clone()).await {
                Ok(collection) => collections[idx] = Some(collection),
                Err(err) => {
                    eprintln!(
                        "Failed to rehydrate collection '{}' for '{}': {}",
                        name, endpoint_label, err
                    );
                    pending.push((idx, name.clone()));
                }
            },
            None => pending.push((idx, name.clone())),
        }
    }

    if !pending.is_empty() {
        let pending_names: Vec<String> = pending.iter().map(|(_, name)| name.clone()).collect();
        let created = create_collections_with_progress(
            client,
            &pending_names,
            max_outstanding_ops,
            max_retries,
            progress_label,
            &mut on_ddl_latency,
        )
        .await?;
        for ((idx, _), collection) in pending.into_iter().zip(created.into_iter()) {
            collections[idx] = Some(collection);
        }
    }

    let collections = collections
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            ChromaHttpClientError::ApiError(
                "Failed to prepare collections from cache or creation path".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;

    for collection in &collections {
        match collection.dehydrate().await {
            Ok(dehydrated) => {
                let collection_name = collection.name().to_string();
                let needs_append = endpoint_cache
                    .get(&collection_name)
                    .is_none_or(|cached| cached != &dehydrated);
                endpoint_cache.insert(collection_name.clone(), dehydrated.clone());

                if needs_append {
                    if let Err(err) = append_collection_cache_record(
                        cache_path,
                        endpoint_label,
                        &collection_name,
                        &dehydrated,
                    ) {
                        eprintln!(
                            "Warning: Failed to append collection cache {}: {}",
                            cache_path.display(),
                            err
                        );
                    }
                }
            }
            Err(err) => {
                eprintln!(
                    "Failed to dehydrate collection '{}' for cache '{}': {}",
                    collection.name(),
                    endpoint_label,
                    err
                );
            }
        }
    }

    cache.insert(endpoint_label.to_string(), endpoint_cache);
    if let Err(err) = write_collection_cache_records(cache_path, &cache) {
        eprintln!(
            "Warning: Failed to save collection cache {}: {}",
            cache_path.display(),
            err
        );
    }

    Ok(collections)
}

/// Creates collections on a client concurrently and logs warmup progress as they complete.
async fn create_collections_with_progress(
    client: &ChromaHttpClient,
    collection_names: &[String],
    max_outstanding_ops: usize,
    max_retries: u32,
    label: &'static str,
    on_ddl_latency: &mut (impl FnMut(&str, Duration) + Send),
) -> Result<Vec<ChromaCollection>, ChromaHttpClientError> {
    let total_collections = collection_names.len();
    let limiter = Arc::new(tokio::sync::Semaphore::new(max_outstanding_ops));
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
            let result = get_or_create_collection_with_retry(&client, name, max_retries).await;
            let _ = tx.send(()).await;
            result
        });
    }
    drop(progress_tx);

    let results: Vec<Result<(ChromaCollection, Duration), ChromaHttpClientError>> =
        join_all(futures).await;

    let mut collections = Vec::with_capacity(total_collections);
    for (name, result) in collection_names.iter().zip(results.into_iter()) {
        let (collection, elapsed) = result?;
        on_ddl_latency(name.as_str(), elapsed);
        collections.push(collection);
    }
    progress_handle
        .await
        .expect("warmup progress reporter should not panic");

    Ok(collections)
}

/// Gaussian Mixture Model for generating realistic embeddings.
pub struct GaussianMixtureModel {
    centroids: Vec<Vec<f32>>,
    std_devs: Vec<f32>,
}

impl GaussianMixtureModel {
    /// Creates a new GMM with deterministic random state from `seed`.
    pub fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        let centroids: Vec<Vec<f32>> = (0..NUM_CLUSTERS)
            .map(|_| {
                (0..EMBEDDING_DIM)
                    .map(|_| rng.gen_range(-1.0..1.0))
                    .collect()
            })
            .collect();

        let std_devs: Vec<f32> = (0..NUM_CLUSTERS)
            .map(|_| rng.gen_range(0.01..0.1))
            .collect();

        Self {
            centroids,
            std_devs,
        }
    }

    /// Generates one embedding batch of `batch_size`.
    pub fn generate_batch(&self, rng: &mut StdRng, batch_size: usize) -> Vec<Vec<f32>> {
        (0..batch_size)
            .map(|_| {
                let cluster_idx = rng.gen_range(0..NUM_CLUSTERS);
                let centroid = &self.centroids[cluster_idx];
                let std_dev = self.std_devs[cluster_idx];

                centroid
                    .iter()
                    .map(|&c| {
                        let u1: f32 = rng.gen_range(0.0001..1.0);
                        let u2: f32 = rng.gen_range(0.0..1.0);
                        let z = (-2.0_f32 * u1.ln()).sqrt()
                            * (2.0_f32 * std::f32::consts::PI * u2).cos();
                        c + z * std_dev
                    })
                    .collect()
            })
            .collect()
    }
}

/// Shared upsert counters for one backend.
pub struct BackendStats {
    total_upserts: AtomicU64,
    total_records: AtomicU64,
}

impl Default for BackendStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendStats {
    /// Creates a new `BackendStats` counter block.
    pub fn new() -> Self {
        Self {
            total_upserts: AtomicU64::new(0),
            total_records: AtomicU64::new(0),
        }
    }

    /// Records a successful upsert completion.
    pub fn record_upsert(&self, batch_size: u64) {
        self.total_upserts.fetch_add(1, Ordering::Relaxed);
        self.total_records.fetch_add(batch_size, Ordering::Relaxed);
    }

    /// Returns the total upsert count.
    pub fn upserts(&self) -> u64 {
        self.total_upserts.load(Ordering::Relaxed)
    }

    /// Returns the total number of records inserted.
    pub fn records(&self) -> u64 {
        self.total_records.load(Ordering::Relaxed)
    }
}

/// Shared context used by load-generator workers.
pub struct WorkerContext {
    /// GMM used to generate embeddings.
    pub gmm: Arc<GaussianMixtureModel>,
    /// Backend counters for this worker group.
    pub stats: Arc<BackendStats>,
    /// Batch size for each upsert operation.
    pub batch_size: usize,
    /// Start timestamp for the run.
    pub start_time: Instant,
    /// Total run duration.
    pub duration: Duration,
    /// Pace limiter receiver.
    pub pacing_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

/// Per-operation sample emitted by shared load worker.
#[derive(Debug, Clone, Copy)]
pub struct LoadOpSample {
    /// Observed latency in milliseconds.
    pub latency_ms: f64,
    /// Number of vectors in the upsert batch.
    pub batch_size: usize,
}

/// Aggregate worker statistics returned after load worker completion.
#[derive(Debug, Default, Clone, Copy)]
pub struct LoadWorkerSummary {
    /// Upsert calls attempted by the worker.
    pub attempts: u64,
    /// Upsert calls that succeeded.
    pub successes: u64,
    /// Upsert calls that failed.
    pub failures: u64,
    /// Total records inserted successfully by the worker.
    pub records: u64,
}

/// Selects which collection each worker should write to for a given operation.
pub trait CollectionSelector: Send {
    /// Choose the target collection index for the next operation.
    fn select(&mut self, num_collections: usize, rng: &mut StdRng) -> usize;
}

impl<F> CollectionSelector for F
where
    F: FnMut(usize, &mut StdRng) -> usize + Send,
{
    fn select(&mut self, num_collections: usize, rng: &mut StdRng) -> usize {
        self(num_collections, rng)
    }
}

/// Boxes a collection selector closure for use with shared load-generator helpers.
pub fn boxed_collection_selector<F>(selector: F) -> Box<dyn CollectionSelector>
where
    F: FnMut(usize, &mut StdRng) -> usize + Send + 'static,
{
    Box::new(selector)
}

/// A collection of shared load-generator metrics for a specific example.
pub struct LoadMetricRefs {
    /// Total upsert attempts.
    pub upsert_attempts: &'static biometrics::Counter,
    /// Successful upserts.
    pub upsert_success: &'static biometrics::Counter,
    /// Failed upserts.
    pub upsert_failures: &'static biometrics::Counter,
    /// DDL latency histogram.
    pub ddl_latency: &'static biometrics::Histogram,
    /// Upsert latency histogram.
    pub upsert_latency: &'static biometrics::Histogram,
    /// Success-path latency histogram.
    pub success_latency: &'static biometrics::Histogram,
}

/// Spawns the shared metrics emitter loop used by load generators.
pub fn spawn_load_metrics_emitter(
    options: biometrics_prometheus::Options,
    upsert_attempts: &'static biometrics::Counter,
    upsert_success: &'static biometrics::Counter,
    upsert_failures: &'static biometrics::Counter,
    ddl_latency: &'static biometrics::Histogram,
    upsert_latency: &'static biometrics::Histogram,
    success_latency: &'static biometrics::Histogram,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let collector = biometrics::Collector::new();
    collector.register_counter(upsert_attempts);
    collector.register_counter(upsert_success);
    collector.register_counter(upsert_failures);
    collector.register_histogram(ddl_latency);
    collector.register_histogram(upsert_latency);
    collector.register_histogram(success_latency);
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let mut emitter = biometrics_prometheus::Emitter::new(options);
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system clock moved backward")
                        .as_millis()
                        .try_into()
                        .expect("timestamp exceeds supported range");
                    let _ = collector.emit(&mut emitter, now);
                }
                _ = &mut stop_rx => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
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

#[allow(clippy::too_many_arguments)]
fn spawn_backend_workers<SelFactory>(
    handles: &mut Vec<tokio::task::JoinHandle<LoadWorkerSummary>>,
    endpoint_label: &str,
    collections: Vec<ChromaCollection>,
    collection_semaphores: Vec<Arc<Semaphore>>,
    task_count: usize,
    seed_base: u64,
    batch_size: usize,
    start_time: Instant,
    duration: Duration,
    pacing_rx: Arc<Mutex<mpsc::Receiver<()>>>,
    gmm: Arc<GaussianMixtureModel>,
    stats: Arc<BackendStats>,
    metrics: &LoadMetricRefs,
    selector_factory: &mut SelFactory,
) where
    SelFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector>,
{
    if collections.is_empty() {
        return;
    }

    for task_id in 0..task_count {
        let collection_count = collections.len();
        let mut collection_selector = selector_factory(task_id, collection_count);
        let ctx = WorkerContext {
            gmm: Arc::clone(&gmm),
            stats: Arc::clone(&stats),
            batch_size,
            start_time,
            duration,
            pacing_rx: Arc::clone(&pacing_rx),
        };
        let task_label = format!("{}_task{}", endpoint_label, task_id);
        let upsert_attempts = metrics.upsert_attempts;
        let upsert_success = metrics.upsert_success;
        let upsert_failures = metrics.upsert_failures;
        let upsert_latency = metrics.upsert_latency;
        let success_latency = metrics.success_latency;

        let handle = tokio::spawn(run_load_worker(
            collections.clone(),
            collection_semaphores.clone(),
            ctx,
            task_id as u64 * 1000 + seed_base,
            task_label.clone(),
            move |num_collections, rng| collection_selector.select(num_collections, rng),
            move |sample: LoadOpSample| {
                upsert_attempts.click();
                upsert_success.click();
                upsert_latency.observe(sample.latency_ms);
                success_latency.observe(sample.latency_ms);
            },
            move |_attempt, err| {
                upsert_attempts.click();
                upsert_failures.click();
                eprintln!("[{}] Upsert error: {}", task_label, err);
            },
        ));
        handles.push(handle);
    }
}

/// Spawns load-reporting progress task for dual-backend load generators.
pub fn spawn_load_progress_reporter(
    start_time: Instant,
    duration: Duration,
    stats_us: std::sync::Arc<BackendStats>,
    stats_eu: std::sync::Arc<BackendStats>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_us_upserts = 0u64;
        let mut last_us_records = 0u64;
        let mut last_eu_upserts = 0u64;
        let mut last_eu_records = 0u64;
        let report_interval = Duration::from_secs(10);

        while start_time.elapsed() < duration {
            tokio::time::sleep(report_interval).await;

            let us_upserts = stats_us.upserts();
            let us_records = stats_us.records();
            let eu_upserts = stats_eu.upserts();
            let eu_records = stats_eu.records();
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
    })
}

/// Prints the final combined load generation summary for dual-backend runs.
pub fn print_dual_backend_load_summary(
    elapsed: Duration,
    stats_us: &BackendStats,
    stats_eu: &BackendStats,
    upsert_attempts: &biometrics::Counter,
    upsert_success: &biometrics::Counter,
    upsert_failures: &biometrics::Counter,
) {
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
        upsert_attempts.read(),
        upsert_success.read(),
        upsert_failures.read(),
    );
}

/// Runs the dual-backend load generation workload shared by load generator examples.
#[allow(clippy::too_many_arguments)]
pub async fn run_dual_load_generator<UsSelectorFactory, EuSelectorFactory>(
    duration: Duration,
    pace_qps: u64,
    task_count: usize,
    batch_size: usize,
    max_outstanding_ops: usize,
    metrics_prefix: &'static str,
    metrics: LoadMetricRefs,
    collections_us: Vec<ChromaCollection>,
    collections_eu: Vec<ChromaCollection>,
    gmm: Arc<GaussianMixtureModel>,
    stats_us: Arc<BackendStats>,
    stats_eu: Arc<BackendStats>,
    mut us_selector_factory: UsSelectorFactory,
    mut eu_selector_factory: EuSelectorFactory,
) -> Result<(), Box<dyn Error>>
where
    UsSelectorFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector> + Send,
    EuSelectorFactory: FnMut(usize, usize) -> Box<dyn CollectionSelector> + Send,
{
    let start_time = Instant::now();

    let semaphores_us: Vec<Arc<Semaphore>> = (0..collections_us.len())
        .map(|_| Arc::new(Semaphore::new(max_outstanding_ops)))
        .collect();
    let semaphores_eu: Vec<Arc<Semaphore>> = (0..collections_eu.len())
        .map(|_| Arc::new(Semaphore::new(max_outstanding_ops)))
        .collect();

    let (ticket_tx, ticket_rx) = mpsc::channel::<()>(1024);
    let pacing_rx = Arc::new(Mutex::new(ticket_rx));

    let (metrics_handle, stop_metrics) = spawn_load_metrics_emitter(
        biometrics_prometheus::Options {
            segment_size: 64 * 1024 * 1024 * 1024,
            flush_interval: Duration::from_secs(3600),
            prefix: utf8path::Path::new(metrics_prefix),
        },
        metrics.upsert_attempts,
        metrics.upsert_success,
        metrics.upsert_failures,
        metrics.ddl_latency,
        metrics.upsert_latency,
        metrics.success_latency,
    );

    let pacing_handle = spawn_pacing_task(start_time, duration, pace_qps.max(1), ticket_tx);
    let mut handles = Vec::with_capacity(task_count.saturating_mul(2));

    spawn_backend_workers(
        &mut handles,
        "us",
        collections_us,
        semaphores_us,
        task_count,
        0,
        batch_size,
        start_time,
        duration,
        Arc::clone(&pacing_rx),
        Arc::clone(&gmm),
        Arc::clone(&stats_us),
        &metrics,
        &mut us_selector_factory,
    );

    spawn_backend_workers(
        &mut handles,
        "eu",
        collections_eu,
        semaphores_eu,
        task_count,
        500 * 1000,
        batch_size,
        start_time,
        duration,
        Arc::clone(&pacing_rx),
        Arc::clone(&gmm),
        Arc::clone(&stats_eu),
        &metrics,
        &mut eu_selector_factory,
    );

    let report_handle = spawn_load_progress_reporter(
        start_time,
        duration,
        Arc::clone(&stats_us),
        Arc::clone(&stats_eu),
    );

    for handle in handles {
        if let Err(err) = handle.await {
            eprintln!("load worker panicked: {err}");
        }
    }

    let _ = stop_metrics.send(());
    let _ = metrics_handle.await;
    report_handle.abort();
    pacing_handle.abort();

    print_dual_backend_load_summary(
        start_time.elapsed(),
        &stats_us,
        &stats_eu,
        metrics.upsert_attempts,
        metrics.upsert_success,
        metrics.upsert_failures,
    );

    Ok(())
}

/// Spawn a pacing task that emits one token per target QPS until run duration elapses.
pub fn spawn_pacing_task(
    start_time: Instant,
    duration: Duration,
    pace_qps: u64,
    ticket_tx: mpsc::Sender<()>,
) -> JoinHandle<()> {
    let pace_qps = pace_qps.max(1);
    let ticket_interval = Duration::from_secs_f64(1.0 / pace_qps as f64);

    tokio::spawn(async move {
        let mut interval = time::interval(ticket_interval);
        while start_time.elapsed() < duration {
            interval.tick().await;
            if ticket_tx.send(()).await.is_err() {
                break;
            }
        }
    })
}

/// Shared load worker that drives concurrent upserts and emits per-op samples.
#[allow(clippy::too_many_arguments)]
pub async fn run_load_worker<F, OnSuccess, OnFailure>(
    collections: Vec<ChromaCollection>,
    collection_semaphores: Vec<Arc<Semaphore>>,
    ctx: WorkerContext,
    seed: u64,
    id_prefix: String,
    mut select_collection: F,
    mut on_success: OnSuccess,
    mut on_failure: OnFailure,
) -> LoadWorkerSummary
where
    F: FnMut(usize, &mut StdRng) -> usize + Send,
    OnSuccess: FnMut(LoadOpSample) + Send,
    OnFailure: FnMut(u64, String) + Send,
{
    let num_collections = collections.len();
    if num_collections == 0 {
        return LoadWorkerSummary::default();
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let mut record_counter: u64 = 0;
    let mut summary = LoadWorkerSummary::default();

    while ctx.start_time.elapsed() < ctx.duration {
        let remaining = ctx.duration.saturating_sub(ctx.start_time.elapsed());
        if remaining.is_zero() {
            break;
        }

        let ticket = time::timeout(remaining, async {
            let mut rx = ctx.pacing_rx.lock().await;
            rx.recv().await
        })
        .await;

        match ticket {
            Ok(Some(())) => {}
            _ => break,
        }

        let idx = select_collection(num_collections, &mut rng) % num_collections;
        let collection = &collections[idx];
        let semaphore = &collection_semaphores[idx];

        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };

        let embeddings = ctx.gmm.generate_batch(&mut rng, ctx.batch_size);
        let ids: Vec<String> = (0..ctx.batch_size)
            .map(|i| {
                record_counter += 1;
                format!("{}_{}", id_prefix, record_counter + i as u64)
            })
            .collect();

        let op_start = Instant::now();
        summary.attempts += 1;
        match collection.upsert(ids, embeddings, None, None, None).await {
            Ok(_response) => {
                let latency_ms = op_start.elapsed().as_secs_f64() * 1000.;
                summary.successes += 1;
                summary.records += ctx.batch_size as u64;
                on_success(LoadOpSample {
                    latency_ms,
                    batch_size: ctx.batch_size,
                });
                ctx.stats.record_upsert(ctx.batch_size as u64);
            }
            Err(err) => {
                summary.failures += 1;
                on_failure(summary.attempts, err.to_string());
            }
        }

        drop(permit);
    }

    summary
}

/// Creates a cloud client and overrides the endpoint.
pub fn create_client(endpoint: &str) -> Result<ChromaHttpClient, Box<dyn Error>> {
    let mut options = ChromaHttpClientOptions::from_cloud_env()?;
    options.endpoint = endpoint.parse()?;
    Ok(ChromaHttpClient::new(options))
}

/// Retries `get_or_create_collection` with exponential-ish backoff.
pub async fn get_or_create_collection_with_retry(
    client: &ChromaHttpClient,
    name: &str,
    max_retries: u32,
) -> Result<(ChromaCollection, Duration), ChromaHttpClientError> {
    let start = Instant::now();
    let mut last_error = None;
    for attempt in 1..=max_retries {
        match client.get_or_create_collection(name, None, None).await {
            Ok(collection) => return Ok((collection, start.elapsed())),
            Err(e) => {
                eprintln!(
                    "  Attempt {}/{} failed for collection '{}': {}",
                    attempt, max_retries, name, e
                );
                last_error = Some(e);
                if attempt < max_retries {
                    tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}
