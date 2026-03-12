//! Benchmark for a flat centroid index under a realistic SPANN write workload.
//!
//! Implements Option B from the central index alternatives design: linear scan
//! over centroid vectors with optional 1-bit RaBitQ quantization and full-
//! precision reranking. Targets the case where the quantized index fits in L3
//! cache (~100K centroids at 4096 dims, ~480K at 1024 dims).
//!
//! Phase 1: Build flat index from N centroid vectors.
//! Phase 2: Simulate adding 1M data vectors. The centroid index sees:
//!   - navigate (search) ~3.05x per data vector
//!   - spawn (add)        ~1.14% of data vectors
//!   - drop (remove)      ~0.57% of data vectors
//! Phase 3: Scale sweep — latency & recall across index sizes with reranking.

#[allow(dead_code)]
mod datasets;

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::{Mutex, RwLock};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use simsimd::SpatialSimilarity;

use datasets::{format_count, Dataset, DatasetType, MetricType};

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "flat_centroid_profile")]
#[command(about = "Benchmark flat centroid index under SPANN workload")]
#[command(trailing_var_arg = true)]
struct Args {
    /// Dataset to use
    #[arg(long, default_value = "db-pedia")]
    dataset: DatasetType,

    /// Distance metric
    #[arg(long, default_value = "l2")]
    metric: MetricType,

    /// Quantization bit-width for centroid codes (1 only). Omit for full precision f32.
    #[arg(long)]
    centroid_bits: Option<u8>,

    /// Number of initial centroid vectors to bootstrap
    #[arg(long, default_value = "5700")]
    initial_centroids: usize,

    /// Number of simulated data vector adds (drives navigate/spawn/drop)
    #[arg(long, default_value = "1000000")]
    data_vectors: usize,

    /// Number of threads for the SPANN simulation
    #[arg(long, default_value = "32")]
    threads: usize,

    /// Number of queries for recall evaluation
    #[arg(long, default_value = "100")]
    num_queries: usize,

    /// Rerank factor for quantized mode: fetch rerank_factor*k candidates,
    /// rerank with full precision, return top k. Ignored for full precision.
    #[arg(long, default_value = "1")]
    rerank_factor: usize,

    /// Use code-to-code distance (quantize the query to 1-bit, then hamming)
    /// instead of the default quantized-query path (4-bit query quantization
    /// with AND+popcount). Only applies when --centroid-bits is set.
    #[arg(long, default_value = "true")]
    code_to_code: bool,

    /// Extra arguments (ignored, for compatibility with cargo bench)
    #[arg(hide = true, allow_hyphen_values = true)]
    _extra: Vec<String>,
}
// example:
// cargo bench -p chroma-index --bench flat_centroid_profile -- --dataset db-pedia --centroid-bits 1 --initial-centroids 5700 --threads 32 --data-vectors 10000

// =============================================================================
// Load profile ratios (from SPANN CP1 @ 1M data vectors)
// =============================================================================

const NAVIGATES_PER_ADD: f64 = 3.05;
const SPAWN_RATE: f64 = 0.0114;
const DROP_RATE: f64 = 0.0057;
const NPROBE: usize = 64;

// =============================================================================
// Stats tracking
// =============================================================================

#[derive(Default, Clone)]
struct MethodStats {
    calls: u64,
    total: Duration,
}

impl MethodStats {
    fn record(&mut self, elapsed: Duration) {
        self.calls += 1;
        self.total += elapsed;
    }

    fn merge(&mut self, other: &MethodStats) {
        self.calls += other.calls;
        self.total += other.total;
    }

    fn avg_nanos(&self) -> u64 {
        if self.calls == 0 {
            0
        } else {
            self.total.as_nanos() as u64 / self.calls
        }
    }
}

#[derive(Default, Clone)]
struct PhaseStats {
    navigate: MethodStats,
    spawn: MethodStats,
    drop_op: MethodStats,
    wall: Duration,
}

impl PhaseStats {
    fn merge(&mut self, other: &PhaseStats) {
        self.navigate.merge(&other.navigate);
        self.spawn.merge(&other.spawn);
        self.drop_op.merge(&other.drop_op);
    }
}

// =============================================================================
// Formatting helpers
// =============================================================================

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 0.000_001 {
        format!("{:.0}ns", secs * 1_000_000_000.0)
    } else if secs < 0.001 {
        format!("{:.1}\u{00b5}s", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.2}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.1}m", secs / 60.0)
    }
}

fn format_nanos(nanos: u64) -> String {
    format_duration(Duration::from_nanos(nanos))
}

// =============================================================================
// Distance helpers
// =============================================================================

fn compute_distance(a: &[f32], b: &[f32], df: &DistanceFunction) -> f32 {
    match df {
        DistanceFunction::Euclidean => f32::sqeuclidean(a, b).unwrap_or(f64::MAX) as f32,
        DistanceFunction::InnerProduct => {
            let ip = f32::inner(a, b).unwrap_or(0.0) as f32;
            1.0 - ip
        }
        DistanceFunction::Cosine => f32::cosine(a, b).unwrap_or(f64::MAX) as f32,
    }
}

// =============================================================================
// Flat centroid index
// =============================================================================

#[derive(Serialize, Deserialize)]
struct FlatIndexData {
    keys: Vec<u32>,
    vectors: Vec<f32>,
    codes: Option<Vec<u8>>,
    quantization_center: Option<Vec<f32>>,
}

#[derive(Clone, Copy, PartialEq)]
struct OrdF32(f32);
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

struct FlatCentroidIndex {
    dim: usize,
    distance_fn: DistanceFunction,
    code_size: usize,

    keys: RwLock<Vec<u32>>,
    /// Contiguous f32 storage: vectors[i*dim..(i+1)*dim] is vector i.
    vectors: RwLock<Vec<f32>>,
    /// 1-bit RaBitQ codes (contiguous, code_size bytes per entry). None for full precision.
    codes: RwLock<Option<Vec<u8>>>,
    quantization_center: Option<Vec<f32>>,
    /// Tombstone set for soft deletes.
    tombstones: RwLock<HashSet<u32>>,
}

impl FlatCentroidIndex {
    fn build(
        flat_vectors: &[f32],
        keys: &[u32],
        n: usize,
        dim: usize,
        distance_fn: DistanceFunction,
        centroid_bits: Option<u8>,
    ) -> Self {
        let quantization_center = centroid_bits.map(|_| {
            let mut mean = vec![0.0f32; dim];
            for i in 0..n {
                let v = &flat_vectors[i * dim..(i + 1) * dim];
                for (m, &val) in mean.iter_mut().zip(v) {
                    *m += val;
                }
            }
            let scale = 1.0 / n as f32;
            for m in mean.iter_mut() {
                *m *= scale;
            }
            mean
        });

        let codes = quantization_center.as_ref().map(|center| {
            let cs = Code::<1>::size(dim);
            let mut all_codes = Vec::with_capacity(n * cs);
            for i in 0..n {
                let v = &flat_vectors[i * dim..(i + 1) * dim];
                let code = Code::<1>::quantize(v, center);
                all_codes.extend_from_slice(code.as_ref());
            }
            all_codes
        });

        let code_size = if centroid_bits.is_some() {
            Code::<1>::size(dim)
        } else {
            0
        };

        Self {
            dim,
            distance_fn,
            code_size,
            keys: RwLock::new(keys.to_vec()),
            vectors: RwLock::new(flat_vectors.to_vec()),
            codes: RwLock::new(codes),
            quantization_center,
            tombstones: RwLock::new(HashSet::new()),
        }
    }

    fn from_data(data: FlatIndexData, dim: usize, distance_fn: DistanceFunction) -> Self {
        let code_size = if data.codes.is_some() {
            Code::<1>::size(dim)
        } else {
            0
        };
        Self {
            dim,
            distance_fn,
            code_size,
            keys: RwLock::new(data.keys),
            vectors: RwLock::new(data.vectors),
            codes: RwLock::new(data.codes),
            quantization_center: data.quantization_center,
            tombstones: RwLock::new(HashSet::new()),
        }
    }

    fn to_data(&self) -> FlatIndexData {
        FlatIndexData {
            keys: self.keys.read().clone(),
            vectors: self.vectors.read().clone(),
            codes: self.codes.read().clone(),
            quantization_center: self.quantization_center.clone(),
        }
    }

    fn len(&self) -> usize {
        self.keys.read().len() - self.tombstones.read().len()
    }

    /// Full-precision flat scan: compute exact distance to every centroid, return top-k.
    fn search_f32(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let keys = self.keys.read();
        let vectors = self.vectors.read();
        let tombstones = self.tombstones.read();
        let n = keys.len();

        let mut heap: BinaryHeap<(OrdF32, u32)> = BinaryHeap::with_capacity(k + 1);
        for i in 0..n {
            let key = keys[i];
            if tombstones.contains(&key) {
                continue;
            }
            let v = &vectors[i * dim..(i + 1) * dim];
            let dist = compute_distance(query, v, df);
            if heap.len() < k {
                heap.push((OrdF32(dist), key));
            } else if dist < heap.peek().unwrap().0 .0 {
                heap.pop();
                heap.push((OrdF32(dist), key));
            }
        }
        let mut result: Vec<(u32, f32)> = heap.into_iter().map(|(d, k)| (k, d.0)).collect();
        result.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        result
    }

    /// 1-bit quantized scan with 4 bit query: approximate distance via RaBitQ, return top-k.
    fn search_quantized(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let cs = self.code_size;
        let center = self.quantization_center.as_ref().unwrap();
        let keys = self.keys.read();
        let codes_guard = self.codes.read();
        let codes_buf = codes_guard.as_ref().unwrap();
        let tombstones = self.tombstones.read();
        let n = keys.len();

        let r_q: Vec<f32> = query.iter().zip(center.iter()).map(|(q, c)| q - c).collect();
        let c_norm = center.iter().map(|c| c * c).sum::<f32>().sqrt();
        let c_dot_q: f32 = center.iter().zip(query.iter()).map(|(c, q)| c * q).sum();
        let q_norm = query.iter().map(|q| q * q).sum::<f32>().sqrt();
        let padded_bytes = Code::<1>::packed_len(dim);
        let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);

        let mut heap: BinaryHeap<(OrdF32, u32)> = BinaryHeap::with_capacity(k + 1);
        for i in 0..n {
            let key = keys[i];
            if tombstones.contains(&key) {
                continue;
            }
            let code_slice = &codes_buf[i * cs..(i + 1) * cs];
            let code = Code::<1, &[u8]>::new(code_slice);
            let dist = code.distance_quantized_query(df, &qq);
            if heap.len() < k {
                heap.push((OrdF32(dist), key));
            } else if dist < heap.peek().unwrap().0 .0 {
                heap.pop();
                heap.push((OrdF32(dist), key));
            }
        }
        let mut result: Vec<(u32, f32)> = heap.into_iter().map(|(d, k)| (k, d.0)).collect();
        result.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        result
    }

    /// Quantized scan with full-precision reranking.
    /// Fetches `fetch_k` candidates via quantized scan, then reranks with exact distances.
    fn search_quantized_rerank(&self, query: &[f32], k: usize, fetch_k: usize) -> Vec<(u32, f32)> {
        let candidates = self.search_quantized(query, fetch_k);
        let dim = self.dim;
        let df = &self.distance_fn;
        let keys_guard = self.keys.read();
        let vectors = self.vectors.read();

        let key_to_idx: HashMap<u32, usize> = keys_guard
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i))
            .collect();

        let mut scored: Vec<(u32, f32)> = candidates
            .iter()
            .filter_map(|&(key, _)| {
                key_to_idx.get(&key).map(|&idx| {
                    let v = &vectors[idx * dim..(idx + 1) * dim];
                    let d = compute_distance(query, v, df);
                    (key, d)
                })
            })
            .collect();
        scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        scored.truncate(k);
        scored
    }

    /// 1-bit code-to-code scan: quantize the query to a 1-bit code, then
    /// compute distance via hamming (Code::distance_code).
    fn search_code_to_code(&self, query: &[f32], k: usize) -> Vec<(u32, f32)> {
        let dim = self.dim;
        let df = &self.distance_fn;
        let cs = self.code_size;
        let center = self.quantization_center.as_ref().unwrap();
        let keys = self.keys.read();
        let codes_guard = self.codes.read();
        let codes_buf = codes_guard.as_ref().unwrap();
        let tombstones = self.tombstones.read();
        let n = keys.len();

        let c_norm = center.iter().map(|c| c * c).sum::<f32>().sqrt();
        let query_code = Code::<1>::quantize(query, center);

        let mut heap: BinaryHeap<(OrdF32, u32)> = BinaryHeap::with_capacity(k + 1);
        for i in 0..n {
            let key = keys[i];
            if tombstones.contains(&key) {
                continue;
            }
            let code_slice = &codes_buf[i * cs..(i + 1) * cs];
            let code = Code::<1, &[u8]>::new(code_slice);
            let dist = query_code.distance_code(&code, df, c_norm, dim);
            if heap.len() < k {
                heap.push((OrdF32(dist), key));
            } else if dist < heap.peek().unwrap().0 .0 {
                heap.pop();
                heap.push((OrdF32(dist), key));
            }
        }
        let mut result: Vec<(u32, f32)> = heap.into_iter().map(|(d, k)| (k, d.0)).collect();
        result.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        result
    }

    fn search_code_to_code_rerank(&self, query: &[f32], k: usize, fetch_k: usize) -> Vec<(u32, f32)> {
        let candidates = self.search_code_to_code(query, fetch_k);
        let dim = self.dim;
        let df = &self.distance_fn;
        let keys_guard = self.keys.read();
        let vectors = self.vectors.read();

        let key_to_idx: HashMap<u32, usize> = keys_guard
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i))
            .collect();

        let mut scored: Vec<(u32, f32)> = candidates
            .iter()
            .filter_map(|&(key, _)| {
                key_to_idx.get(&key).map(|&idx| {
                    let v = &vectors[idx * dim..(idx + 1) * dim];
                    let d = compute_distance(query, v, df);
                    (key, d)
                })
            })
            .collect();
        scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        scored.truncate(k);
        scored
    }

    fn search(&self, query: &[f32], k: usize, code_to_code: bool) -> Vec<(u32, f32)> {
        if self.quantization_center.is_some() {
            if code_to_code {
                self.search_code_to_code(query, k)
            } else {
                self.search_quantized(query, k)
            }
        } else {
            self.search_f32(query, k)
        }
    }

    fn search_with_rerank(&self, query: &[f32], k: usize, rerank_factor: usize, code_to_code: bool) -> Vec<(u32, f32)> {
        if self.quantization_center.is_some() && rerank_factor > 1 {
            if code_to_code {
                self.search_code_to_code_rerank(query, k, k * rerank_factor)
            } else {
                self.search_quantized_rerank(query, k, k * rerank_factor)
            }
        } else {
            self.search(query, k, code_to_code)
        }
    }

    fn add(&self, key: u32, vector: &[f32]) {
        let mut keys = self.keys.write();
        let mut vectors = self.vectors.write();
        keys.push(key);
        vectors.extend_from_slice(vector);

        if let Some(center) = &self.quantization_center {
            let code = Code::<1>::quantize(vector, center);
            let mut codes_guard = self.codes.write();
            codes_guard.as_mut().unwrap().extend_from_slice(code.as_ref());
        }

        // Un-tombstone if re-added
        self.tombstones.write().remove(&key);
    }

    fn remove(&self, key: u32) {
        self.tombstones.write().insert(key);
    }

    fn memory_bytes(&self) -> usize {
        let keys = self.keys.read();
        let vectors = self.vectors.read();
        let codes_guard = self.codes.read();
        let n = keys.len();
        let key_bytes = n * std::mem::size_of::<u32>();
        let vec_bytes = vectors.len() * std::mem::size_of::<f32>();
        let code_bytes = codes_guard.as_ref().map_or(0, |c| c.len());
        let center_bytes = self
            .quantization_center
            .as_ref()
            .map_or(0, |c| c.len() * std::mem::size_of::<f32>());
        key_bytes + vec_bytes + code_bytes + center_bytes
    }
}

// =============================================================================
// Dataset loading
// =============================================================================

fn load_vectors(args: &Args) -> (Vec<Vec<f32>>, usize, DistanceFunction) {
    let distance_fn = args.metric.to_distance_function();

    let total_needed = args.initial_centroids
        + (args.data_vectors as f64 * SPAWN_RATE) as usize
        + args.num_queries
        + 1024;

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let dataset: Box<dyn Dataset> = rt.block_on(async {
        match args.dataset {
            DatasetType::DbPedia => Box::new(
                datasets::dbpedia::DbPedia::load()
                    .await
                    .expect("Failed to load DBPedia dataset"),
            ) as Box<dyn Dataset>,
            DatasetType::Arxiv => Box::new(
                datasets::arxiv::Arxiv::load()
                    .await
                    .expect("Failed to load Arxiv dataset"),
            ),
            DatasetType::Sec => Box::new(
                datasets::sec::Sec::load()
                    .await
                    .expect("Failed to load SEC dataset"),
            ),
            DatasetType::MsMarco => Box::new(
                datasets::msmarco::MsMarco::load()
                    .await
                    .expect("Failed to load MS MARCO dataset"),
            ),
            DatasetType::WikipediaEn => Box::new(
                datasets::wikipedia::Wikipedia::load()
                    .await
                    .expect("Failed to load Wikipedia dataset"),
            ),
            DatasetType::Synthetic => todo!("Synthetic dataset not supported"),
        }
    });

    let dim = dataset.dimension();
    let load_count = total_needed.min(dataset.data_len());
    println!(
        "Loading {} vectors from {} (dim={})...",
        format_count(load_count),
        dataset.name(),
        dim
    );
    let pairs = dataset
        .load_range(0, load_count)
        .expect("Failed to load dataset");
    let vectors: Vec<Vec<f32>> = pairs.into_iter().map(|(_, v)| v.to_vec()).collect();
    (vectors, dim, distance_fn)
}

// =============================================================================
// Brute-force ground truth
// =============================================================================

fn brute_force_knn(
    query: &[f32],
    corpus: &[Vec<f32>],
    corpus_keys: &[u32],
    k: usize,
    distance_fn: &DistanceFunction,
) -> Vec<u32> {
    let mut dists: Vec<(u32, f32)> = corpus_keys
        .iter()
        .zip(corpus.iter())
        .map(|(&key, vec)| {
            let d = compute_distance(query, vec, distance_fn);
            (key, d)
        })
        .collect();
    dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    dists.into_iter().take(k).map(|(k, _)| k).collect()
}

// =============================================================================
// Main benchmark
// =============================================================================

fn main() {
    let args = Args::parse();
    let centroid_bits = args.centroid_bits;
    let initial_centroids = args.initial_centroids;
    let data_vectors = args.data_vectors;
    let num_threads = args.threads;
    let num_queries = args.num_queries;
    let rerank_factor = args.rerank_factor;
    let code_to_code = args.code_to_code;

    if let Some(bits) = centroid_bits {
        assert_eq!(
            bits, 1,
            "Only 1-bit quantization is supported for flat index"
        );
    }

    let (all_vectors, dim, distance_fn) = load_vectors(&args);

    let bits_label = match centroid_bits {
        Some(b) => format!("{}", b),
        None => "f32".to_string(),
    };

    let quantized_index_bytes = if centroid_bits.is_some() {
        let code_size = Code::<1>::size(dim);
        initial_centroids * code_size
    } else {
        initial_centroids * dim * std::mem::size_of::<f32>()
    };

    println!("\n=== Flat Centroid Index Profile ===");
    println!(
        "Dim: {} | Metric: {:?} | Centroid bits: {} | Threads: {}",
        dim, args.metric, bits_label, num_threads
    );
    println!(
        "Initial centroids: {} | Data vectors: {} | Queries: {}",
        format_count(initial_centroids),
        format_count(data_vectors),
        num_queries
    );
    if centroid_bits.is_some() {
        println!(
            "Rerank factor: {}x | Distance: {}",
            rerank_factor,
            if code_to_code { "code-to-code (hamming)" } else { "quantized-query (AND+popcount)" }
        );
    }
    println!(
        "Scan footprint: {:.1} MB ({})",
        quantized_index_bytes as f64 / (1024.0 * 1024.0),
        if centroid_bits.is_some() {
            "quantized codes"
        } else {
            "full precision"
        }
    );
    println!(
        "Load profile per data vector: {:.2} navigates, {:.4} spawns, {:.4} drops",
        NAVIGATES_PER_ADD, SPAWN_RATE, DROP_RATE
    );

    // =========================================================================
    // Phase 1: Build flat index (with disk cache)
    // =========================================================================
    let n = initial_centroids.min(all_vectors.len());

    let cache_dir = PathBuf::from("target/flat_cache");
    let cache_file = cache_dir.join(format!(
        "flat_{:?}_{}_{}_{:?}.bin",
        args.dataset, initial_centroids, bits_label, args.metric,
    ));

    let index = if cache_file.exists() {
        println!(
            "\n--- Phase 1: Loading cached flat index from {} ---",
            cache_file.display()
        );
        let load_start = Instant::now();
        let data = std::fs::read(&cache_file).expect("Failed to read cache file");
        let index_data: FlatIndexData =
            bincode::deserialize(&data).expect("Failed to deserialize flat index");
        let idx = FlatCentroidIndex::from_data(index_data, dim, distance_fn.clone());
        println!(
            "Loaded {} centroids in {}",
            format_count(idx.len()),
            format_duration(load_start.elapsed()),
        );
        idx
    } else {
        println!(
            "\n--- Phase 1: Build flat index ({} centroids) ---",
            format_count(initial_centroids)
        );

        let mut flat_vectors = Vec::with_capacity(n * dim);
        for v in &all_vectors[..n] {
            flat_vectors.extend_from_slice(v);
        }
        let keys: Vec<u32> = (0..n as u32).collect();

        let build_start = Instant::now();
        let idx = FlatCentroidIndex::build(
            &flat_vectors,
            &keys,
            n,
            dim,
            distance_fn.clone(),
            centroid_bits,
        );
        let build_time = build_start.elapsed();

        println!(
            "Built flat index with {} centroids in {} ({:.0} vec/s)",
            format_count(n),
            format_duration(build_time),
            n as f64 / build_time.as_secs_f64()
        );
        println!(
            "Memory: {:.1} MB",
            idx.memory_bytes() as f64 / (1024.0 * 1024.0)
        );

        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
        let encoded =
            bincode::serialize(&idx.to_data()).expect("Failed to serialize flat index");
        std::fs::write(&cache_file, &encoded).expect("Failed to write cache file");
        let cache_size_mb = encoded.len() as f64 / (1024.0 * 1024.0);
        println!(
            "Cached flat index to {} ({:.1} MB)",
            cache_file.display(),
            cache_size_mb
        );

        idx
    };
    println!("Index size: {}", index.len());

    // =========================================================================
    // Phase 2: Simulated SPANN workload (multi-threaded)
    // =========================================================================
    println!(
        "\n--- Phase 2: SPANN workload ({} data vectors, {} threads) ---",
        format_count(data_vectors),
        num_threads
    );

    let next_key = AtomicU32::new(initial_centroids as u32);
    let live_entries: Mutex<Vec<(u32, usize)>> =
        Mutex::new((0..initial_centroids).map(|i| (i as u32, i)).collect());

    let total_navigates = (data_vectors as f64 * NAVIGATES_PER_ADD) as u64;
    let total_spawns = (data_vectors as f64 * SPAWN_RATE) as u64;
    let total_drops = (data_vectors as f64 * DROP_RATE) as u64;
    let total_ops = total_navigates + total_spawns + total_drops;

    let progress = ProgressBar::new(total_ops);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[SPANN sim] {wide_bar} {pos}/{len} [{elapsed_precise}<{eta_precise}]")
            .unwrap(),
    );

    let nav_per_add = NAVIGATES_PER_ADD.floor() as usize;
    let nav_frac = NAVIGATES_PER_ADD - nav_per_add as f64;
    let vec_pool_start = initial_centroids;
    let vec_pool_size = all_vectors.len() - vec_pool_start;

    let phase2_start = Instant::now();

    let chunk_size = (data_vectors + num_threads - 1) / num_threads;
    let thread_stats: Vec<PhaseStats> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let index = &index;
                let all_vectors = &all_vectors;
                let next_key = &next_key;
                let live_entries = &live_entries;
                let progress = &progress;
                s.spawn(move || {
                    let mut local_stats = PhaseStats::default();
                    let mut rng = StdRng::seed_from_u64(123 + thread_id as u64);
                    let start = thread_id * chunk_size;
                    let end = (start + chunk_size).min(data_vectors);

                    for i in start..end {
                        let pool_idx = i % vec_pool_size;
                        let query_vec = &all_vectors[vec_pool_start + pool_idx];

                        let mut n_nav = nav_per_add;
                        if rng.gen::<f64>() < nav_frac {
                            n_nav += 1;
                        }
                        for _ in 0..n_nav {
                            let t = Instant::now();
                            let _ = index.search_with_rerank(query_vec, NPROBE, rerank_factor, code_to_code);
                            local_stats.navigate.record(t.elapsed());
                            progress.inc(1);
                        }

                        if rng.gen::<f64>() < SPAWN_RATE {
                            let spawn_idx = (i + 1) % vec_pool_size;
                            let vec_index = vec_pool_start + spawn_idx;
                            let spawn_vec = &all_vectors[vec_index];
                            let key = next_key.fetch_add(1, Ordering::Relaxed);

                            let t = Instant::now();
                            index.add(key, spawn_vec);
                            local_stats.spawn.record(t.elapsed());
                            live_entries.lock().push((key, vec_index));
                            progress.inc(1);
                        }

                        if rng.gen::<f64>() < DROP_RATE {
                            let entry = {
                                let mut entries = live_entries.lock();
                                if entries.len() > 100 {
                                    let idx = rng.gen_range(0..entries.len());
                                    Some(entries.swap_remove(idx))
                                } else {
                                    None
                                }
                            };
                            if let Some((key, _)) = entry {
                                let t = Instant::now();
                                index.remove(key);
                                local_stats.drop_op.record(t.elapsed());
                                progress.inc(1);
                            }
                        }
                    }

                    local_stats
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    progress.finish_and_clear();

    let mut stats = PhaseStats::default();
    for ts in &thread_stats {
        stats.merge(ts);
    }
    stats.wall = phase2_start.elapsed();

    let _final_live_entries = live_entries.into_inner();

    println!(
        "Completed in {} | Index size: {}",
        format_duration(stats.wall),
        index.len()
    );

    println!("\n=== Phase 2: Task Counts ===");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop"
    );
    println!("|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        format_count(stats.navigate.calls as usize),
        format_count(stats.spawn.calls as usize),
        format_count(stats.drop_op.calls as usize),
    );

    println!("\n=== Phase 2: Task Total Time ===");
    println!(
        "| {:>10} | {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop", "wall"
    );
    println!("|------------|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} | {:>10} |",
        format_duration(stats.navigate.total),
        format_duration(stats.spawn.total),
        format_duration(stats.drop_op.total),
        format_duration(stats.wall),
    );

    println!("\n=== Phase 2: Task Avg Time ===");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        "navigate", "spawn", "drop"
    );
    println!("|------------|------------|------------|");
    println!(
        "| {:>10} | {:>10} | {:>10} |",
        format_nanos(stats.navigate.avg_nanos()),
        format_nanos(stats.spawn.avg_nanos()),
        format_nanos(stats.drop_op.avg_nanos()),
    );

    // =========================================================================
    // Phase 3: Scale sweep — latency & recall across index sizes
    // =========================================================================
    let scale_steps: Vec<usize> = {
        let mut steps = Vec::new();
        let mut s = 1_000usize;
        while s <= initial_centroids {
            steps.push(s);
            s *= 10;
        }
        if steps.last() != Some(&initial_centroids) && initial_centroids > 0 {
            steps.push(initial_centroids);
        }
        steps.retain(|&s| s <= all_vectors.len());
        steps
    };

    let scale_k: usize = 100;

    if scale_steps.len() > 1 {
        println!(
            "\n--- Phase 3: Scale sweep ({} sizes, {} queries, k={}) ---",
            scale_steps.len(),
            num_queries,
            scale_k
        );
        println!(
            "Scales: {}",
            scale_steps
                .iter()
                .map(|s| format_count(*s))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Query vectors come from beyond the largest corpus.
        let scale_query_start = *scale_steps.last().unwrap();
        let scale_query_vecs: Vec<&Vec<f32>> = all_vectors[scale_query_start..]
            .iter()
            .take(num_queries)
            .collect();

        if scale_query_vecs.len() < num_queries {
            println!(
                "Warning: only {} query vectors available (requested {})",
                scale_query_vecs.len(),
                num_queries
            );
        }

        if centroid_bits.is_some() {
            // Quantized mode: table with rerank factors per scale
            let rerank_factors: &[usize] = &[1, 2, 4, 8, 16];

            println!(
                "\n| {:>10} | {:>8} | {:>7} | {:>11} | {:>12} | {:>10} | {:>10} | {:>10} | {:>10} |",
                "Centroids", "Scan MB", "Rerank", "Recall@10", "Recall@100", "Scan", "Rerank", "Total", "Wall"
            );
            println!(
                "|------------|----------|---------|-------------|--------------|------------|------------|------------|------------|"
            );

            for &scale in &scale_steps {
                let mut flat_vecs = Vec::with_capacity(scale * dim);
                for v in &all_vectors[..scale] {
                    flat_vecs.extend_from_slice(v);
                }
                let keys: Vec<u32> = (0..scale as u32).collect();
                let idx = FlatCentroidIndex::build(
                    &flat_vecs,
                    &keys,
                    scale,
                    dim,
                    distance_fn.clone(),
                    centroid_bits,
                );

                let corpus_vecs_scale: Vec<Vec<f32>> =
                    all_vectors[..scale].iter().cloned().collect();
                let corpus_keys_scale: Vec<u32> = (0..scale as u32).collect();

                let gt_scale: Vec<Vec<u32>> = scale_query_vecs
                    .iter()
                    .map(|q| {
                        brute_force_knn(q, &corpus_vecs_scale, &corpus_keys_scale, scale_k, &distance_fn)
                    })
                    .collect();

                let scan_mb = (scale * Code::<1>::size(dim)) as f64 / (1024.0 * 1024.0);

                for &factor in rerank_factors {
                    let fetch_k = factor * scale_k;
                    let nq = scale_query_vecs.len();
                    let chunk = (nq + num_threads - 1) / num_threads;

                    let wall_start = Instant::now();
                    let per_thread: Vec<(f64, f64, Duration, Duration)> = std::thread::scope(|s| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|tid| {
                                let idx = &idx;
                                let scale_query_vecs = &scale_query_vecs;
                                let gt_scale = &gt_scale;
                                let distance_fn = &distance_fn;
                                s.spawn(move || {
                                    let start = tid * chunk;
                                    let end = (start + chunk).min(nq);
                                    let mut r10 = 0.0f64;
                                    let mut rk_acc = 0.0f64;
                                    let mut s_lat = Duration::ZERO;
                                    let mut rr_lat = Duration::ZERO;

                                    for qi in start..end {
                                        let q = scale_query_vecs[qi];
                                        let t_scan = Instant::now();
                                        let candidates = if code_to_code {
                                            idx.search_code_to_code(q, fetch_k)
                                        } else {
                                            idx.search_quantized(q, fetch_k)
                                        };
                                        s_lat += t_scan.elapsed();

                                        let result = if factor == 1 {
                                            candidates
                                        } else {
                                            let t_rr = Instant::now();
                                            let keys_guard = idx.keys.read();
                                            let vectors = idx.vectors.read();
                                            let key_to_idx: HashMap<u32, usize> = keys_guard
                                                .iter()
                                                .enumerate()
                                                .map(|(i, &k)| (k, i))
                                                .collect();
                                            let mut scored: Vec<(u32, f32)> = candidates
                                                .iter()
                                                .filter_map(|&(key, _)| {
                                                    key_to_idx.get(&key).map(|&vi| {
                                                        let v = &vectors[vi * dim..(vi + 1) * dim];
                                                        let d = compute_distance(q, v, distance_fn);
                                                        (key, d)
                                                    })
                                                })
                                                .collect();
                                            scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                                            scored.truncate(scale_k);
                                            rr_lat += t_rr.elapsed();
                                            scored
                                        };

                                        let predicted: HashSet<u32> =
                                            result.iter().map(|&(key, _)| key).collect();
                                        let gt = &gt_scale[qi];
                                        let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                                        let gt_k: HashSet<u32> = gt.iter().take(scale_k).copied().collect();
                                        r10 += predicted.intersection(&gt_10).count() as f64
                                            / gt_10.len().max(1) as f64;
                                        rk_acc += predicted.intersection(&gt_k).count() as f64
                                            / gt_k.len().max(1) as f64;
                                    }
                                    (r10, rk_acc, s_lat, rr_lat)
                                })
                            })
                            .collect();
                        handles.into_iter().map(|h| h.join().unwrap()).collect()
                    });

                    let wall = wall_start.elapsed();

                    let (r10, rk, scan_lat, rerank_lat) = per_thread.iter().fold(
                        (0.0f64, 0.0f64, Duration::ZERO, Duration::ZERO),
                        |(r10, rk, sl, rl), &(r, k, s, re)| (r10 + r, rk + k, sl + s, rl + re),
                    );

                    let nq = nq as u32;
                    println!(
                        "| {:>10} | {:>7.1} | {:>5}x | {:>10.2}% | {:>11.2}% | {:>10} | {:>10} | {:>10} | {:>10} |",
                        format_count(scale),
                        scan_mb,
                        factor,
                        r10 / nq as f64 * 100.0,
                        rk / nq as f64 * 100.0,
                        format_duration(scan_lat / nq),
                        format_duration(rerank_lat / nq),
                        format_duration((scan_lat + rerank_lat) / nq),
                        format_duration(wall),
                    );
                }
            }

            // Full-precision scan baseline at each scale
            println!(
                "\n| {:>10} | {:>8} | {:>11} | {:>12} | {:>10} | {:>10} |",
                "Centroids", "FP MB", "Recall@10", "Recall@100", "FP Avg lat", "Wall"
            );
            println!(
                "|------------|----------|-------------|--------------|------------|------------|"
            );

            for &scale in &scale_steps {
                let mut flat_vecs = Vec::with_capacity(scale * dim);
                for v in &all_vectors[..scale] {
                    flat_vecs.extend_from_slice(v);
                }
                let keys: Vec<u32> = (0..scale as u32).collect();
                let idx = FlatCentroidIndex::build(
                    &flat_vecs,
                    &keys,
                    scale,
                    dim,
                    distance_fn.clone(),
                    centroid_bits,
                );

                let corpus_vecs_scale: Vec<Vec<f32>> =
                    all_vectors[..scale].iter().cloned().collect();
                let corpus_keys_scale: Vec<u32> = (0..scale as u32).collect();

                let gt_scale: Vec<Vec<u32>> = scale_query_vecs
                    .iter()
                    .map(|q| {
                        brute_force_knn(q, &corpus_vecs_scale, &corpus_keys_scale, scale_k, &distance_fn)
                    })
                    .collect();

                let fp_mb = (scale * dim * 4) as f64 / (1024.0 * 1024.0);
                let nq = scale_query_vecs.len();
                let chunk = (nq + num_threads - 1) / num_threads;

                let wall_start = Instant::now();
                let per_thread: Vec<(f64, f64, Duration)> = std::thread::scope(|s| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let idx = &idx;
                            let scale_query_vecs = &scale_query_vecs;
                            let gt_scale = &gt_scale;
                            s.spawn(move || {
                                let start = tid * chunk;
                                let end = (start + chunk).min(nq);
                                let mut r10 = 0.0f64;
                                let mut rk_acc = 0.0f64;
                                let mut lat = Duration::ZERO;

                                for qi in start..end {
                                    let q = scale_query_vecs[qi];
                                    let t = Instant::now();
                                    let result = idx.search_f32(q, scale_k);
                                    lat += t.elapsed();

                                    let predicted: HashSet<u32> = result.iter().map(|&(key, _)| key).collect();
                                    let gt = &gt_scale[qi];
                                    let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                                    let gt_k: HashSet<u32> = gt.iter().take(scale_k).copied().collect();
                                    r10 += predicted.intersection(&gt_10).count() as f64
                                        / gt_10.len().max(1) as f64;
                                    rk_acc += predicted.intersection(&gt_k).count() as f64
                                        / gt_k.len().max(1) as f64;
                                }
                                (r10, rk_acc, lat)
                            })
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });
                let wall = wall_start.elapsed();

                let (r10, rk, lat) = per_thread.iter().fold(
                    (0.0f64, 0.0f64, Duration::ZERO),
                    |(r10, rk, l), &(r, k, la)| (r10 + r, rk + k, l + la),
                );

                println!(
                    "| {:>10} | {:>7.1} | {:>10.2}% | {:>11.2}% | {:>10} | {:>10} |",
                    format_count(scale),
                    fp_mb,
                    r10 / nq as f64 * 100.0,
                    rk / nq as f64 * 100.0,
                    format_duration(lat / nq as u32),
                    format_duration(wall),
                );
            }
        } else {
            // Full precision mode: simple latency table
            println!(
                "\n| {:>10} | {:>8} | {:>11} | {:>12} | {:>10} | {:>10} |",
                "Centroids", "FP MB", "Recall@10", "Recall@100", "Avg lat", "Wall"
            );
            println!(
                "|------------|----------|-------------|--------------|------------|------------|"
            );

            for &scale in &scale_steps {
                let mut flat_vecs = Vec::with_capacity(scale * dim);
                for v in &all_vectors[..scale] {
                    flat_vecs.extend_from_slice(v);
                }
                let keys: Vec<u32> = (0..scale as u32).collect();
                let idx = FlatCentroidIndex::build(
                    &flat_vecs,
                    &keys,
                    scale,
                    dim,
                    distance_fn.clone(),
                    None,
                );

                let corpus_vecs_scale: Vec<Vec<f32>> =
                    all_vectors[..scale].iter().cloned().collect();
                let corpus_keys_scale: Vec<u32> = (0..scale as u32).collect();

                let gt_scale: Vec<Vec<u32>> = scale_query_vecs
                    .iter()
                    .map(|q| {
                        brute_force_knn(q, &corpus_vecs_scale, &corpus_keys_scale, scale_k, &distance_fn)
                    })
                    .collect();

                let fp_mb = (scale * dim * 4) as f64 / (1024.0 * 1024.0);
                let nq = scale_query_vecs.len();
                let chunk = (nq + num_threads - 1) / num_threads;

                let wall_start = Instant::now();
                let per_thread: Vec<(f64, f64, Duration)> = std::thread::scope(|s| {
                    let handles: Vec<_> = (0..num_threads)
                        .map(|tid| {
                            let idx = &idx;
                            let scale_query_vecs = &scale_query_vecs;
                            let gt_scale = &gt_scale;
                            s.spawn(move || {
                                let start = tid * chunk;
                                let end = (start + chunk).min(nq);
                                let mut r10 = 0.0f64;
                                let mut rk_acc = 0.0f64;
                                let mut lat = Duration::ZERO;

                                for qi in start..end {
                                    let q = scale_query_vecs[qi];
                                    let t = Instant::now();
                                    let result = idx.search(q, scale_k, false, None);
                                    lat += t.elapsed();

                                    let predicted: HashSet<u32> = result.iter().map(|&(key, _)| key).collect();
                                    let gt = &gt_scale[qi];
                                    let gt_10: HashSet<u32> = gt.iter().take(10).copied().collect();
                                    let gt_k: HashSet<u32> = gt.iter().take(scale_k).copied().collect();
                                    r10 += predicted.intersection(&gt_10).count() as f64
                                        / gt_10.len().max(1) as f64;
                                    rk_acc += predicted.intersection(&gt_k).count() as f64
                                        / gt_k.len().max(1) as f64;
                                }
                                (r10, rk_acc, lat)
                            })
                        })
                        .collect();
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });
                let wall = wall_start.elapsed();

                let (r10, rk, lat) = per_thread.iter().fold(
                    (0.0f64, 0.0f64, Duration::ZERO),
                    |(r10, rk, l), &(r, k, la)| (r10 + r, rk + k, l + la),
                );

                println!(
                    "| {:>10} | {:>7.1} | {:>10.2}% | {:>11.2}% | {:>10} | {:>10} |",
                    format_count(scale),
                    fp_mb,
                    r10 / nq as f64 * 100.0,
                    rk / nq as f64 * 100.0,
                    format_duration(lat / nq as u32),
                    format_duration(wall),
                );
            }
        }
    }

    println!("\n=== Legend ===");
    println!("Phase 2 columns:");
    println!(
        "  navigate  - flat scan the centroid index (nprobe={}, rerank={}x)",
        NPROBE,
        if centroid_bits.is_some() { rerank_factor } else { 1 }
    );
    println!("  spawn     - append a new centroid to the flat index (from cluster split)");
    println!("  drop      - tombstone a centroid in the flat index (from cluster split/merge)");
    println!("  wall      - wall-clock time for the full SPANN simulation phase");
    println!("Phase 3 columns:");
    println!("  Centroids - number of centroid vectors in the flat index");
    if centroid_bits.is_some() {
        println!("  Scan MB   - memory footprint of the quantized code array");
        println!("  Rerank    - rerank multiplier (fetch Rerank*k candidates, rerank with f32)");
    }
    println!("  FP MB     - memory footprint of the full-precision f32 vector array");
    println!("  Recall@10 - fraction of true top-10 neighbors found");
    println!("  Recall@100- fraction of true top-100 neighbors found");
    if centroid_bits.is_some() {
        println!("  Scan      - average quantized scan time per query");
        println!("  Rerank    - average full-precision rerank time per query (0 at 1x)");
        println!("  Total     - Scan + Rerank (= end-to-end query latency)");
    } else {
        println!("  Avg lat   - average single-query search latency");
    }
    println!("  Wall      - wall-clock time for all queries at this scale/rerank combo");
}
