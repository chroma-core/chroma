//! Shared helpers for example load generators.

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::ChromaHttpClientError;
use crate::{ChromaCollection, ChromaHttpClient, ChromaHttpClientOptions};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex};

/// Embedding dimensionality shared by load generator examples.
pub const EMBEDDING_DIM: usize = 1536;

/// Default number of GMM clusters shared by load generator examples.
pub const NUM_CLUSTERS: usize = 1000;

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
) -> Result<ChromaCollection, ChromaHttpClientError> {
    let mut last_error = None;
    for attempt in 1..=max_retries {
        match client.get_or_create_collection(name, None, None).await {
            Ok(collection) => return Ok(collection),
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
